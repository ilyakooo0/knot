//! `textDocument/completion` and `completionItem/resolve` handlers, plus all
//! the local helpers (dot-completion field resolution, monad-aware ranking,
//! atomic-context filtering, import-path completion).

use std::collections::{HashMap, HashSet};
use std::path::Path;

use lsp_types::*;

use knot::ast::{self, DeclKind, Module, Span, TypeKind};
use knot_compiler::infer::MonadKind;

use crate::analysis::get_or_parse_file_shared;
use crate::builtins::ATOMIC_DISALLOWED_BUILTINS;
use crate::shared::{
    collect_lambda_param_names, extract_record_fields, find_enclosing_atomic_expr,
    find_field_refinement, format_route_constructor_hover, predicate_to_source,
    resolve_var_to_source, scan_knot_files_in_roots,
};
use crate::state::{
    builtins as state_builtins, DocumentState, ServerState, SnippetContext, KEYWORDS, SNIPPETS,
};
use crate::type_format::format_type_kind;
use crate::utils::{
    offset_to_position, position_to_offset, recurse_expr, uri_to_path,
};

// ── Completion ──────────────────────────────────────────────────────

pub(crate) fn handle_completion(
    state: &ServerState,
    params: &CompletionParams,
) -> Option<CompletionResponse> {
    let uri = &params.text_document_position.text_document.uri;
    let doc = state.documents.get(uri)?;
    let pos = params.text_document_position.position;

    // Detect trigger context
    let offset = position_to_offset(&doc.source, pos);
    let trigger_char = if offset > 0 {
        doc.source.as_bytes().get(offset - 1).copied()
    } else {
        None
    };

    // Atomic-block context: when the cursor is inside `atomic { ... }`, the
    // type checker forbids any IO effects (console/fs/network/clock/random).
    // Drop those builtins and any user functions that perform IO from the
    // completion list so the user can't type them.
    let in_atomic = find_enclosing_atomic_expr(&doc.module, &doc.source, offset).is_some();

    // Route-declaration context: a `route Foo where ...` block contains only
    // HTTP method keywords, path literals, and field-of-type entries. None of
    // the runtime builtins, lambdas, or do-block scaffolding belongs here.
    // Returning a tightly-scoped completion list keeps the user from typing
    // expression-level snippets that would never parse.
    if find_enclosing_route_decl_span(&doc.module, offset).is_some() {
        return Some(CompletionResponse::Array(route_completions(doc)));
    }

    let mut items = Vec::new();

    // Context-aware: after `*` only suggest source/view names
    if trigger_char == Some(b'*') {
        for decl in &doc.module.decls {
            if let DeclKind::Source { name, .. } | DeclKind::View { name, .. } = &decl.node {
                let detail = doc.type_info.get(name.as_str()).cloned();
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::VARIABLE),
                    detail,
                    ..Default::default()
                });
            }
        }
        return Some(CompletionResponse::Array(items));
    }

    // Context-aware: after `&` only suggest derived names
    if trigger_char == Some(b'&') {
        for decl in &doc.module.decls {
            if let DeclKind::Derived { name, .. } = &decl.node {
                let detail = doc.type_info.get(name.as_str()).cloned();
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::VARIABLE),
                    detail,
                    ..Default::default()
                });
            }
        }
        return Some(CompletionResponse::Array(items));
    }

    // Context-aware: after `/` in an import line, suggest file paths
    if trigger_char == Some(b'/') {
        let line_start = doc.source[..offset].rfind('\n').map(|p| p + 1).unwrap_or(0);
        let line_text = &doc.source[line_start..offset];
        if line_text.trim_start().starts_with("import ") {
            if let Some(source_path) = uri_to_path(uri) {
                if let Some(base_dir) = source_path.parent() {
                    let partial = line_text.trim_start().strip_prefix("import ").unwrap_or("");
                    items.extend(complete_import_path(base_dir, partial));
                }
            }
            return Some(CompletionResponse::Array(items));
        }
    }

    // Context-aware: after `.` suggest record field names from known types
    if trigger_char == Some(b'.') {
        // Try to find the expression before the dot and its type
        let expr_end = offset - 1; // position of the `.`
        let fields = resolve_dot_fields(doc, expr_end);
        if !fields.is_empty() {
            // If the receiver before the dot is a `Var` bound from a `*source`,
            // attach the source's field refinement (when present) as completion
            // detail/documentation so the user sees the predicate without
            // having to open the source declaration.
            let receiver_var = receiver_ident_before_dot(&doc.source, expr_end);
            let owner_source = receiver_var
                .as_deref()
                .and_then(|name| resolve_var_to_source(&doc.module, name));
            for name in fields {
                let mut item = CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::FIELD),
                    ..Default::default()
                };
                if let Some(src) = owner_source.as_deref() {
                    if let Some((type_label, predicate)) =
                        find_field_refinement(&doc.source_refinements, src, &name)
                    {
                        let pred_src = predicate_to_source(predicate, &doc.source);
                        item.detail = Some(format!("refined `{type_label}` — {pred_src}"));
                        item.documentation = Some(Documentation::MarkupContent(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!(
                                "Field `{name}` of `*{src}` is refined; values must satisfy `{pred_src}`."
                            ),
                        }));
                    }
                }
                items.push(item);
            }
            return Some(CompletionResponse::Array(items));
        }

        // Fallback: all known field names from all types
        let mut all_fields = HashSet::new();
        for decl in &doc.module.decls {
            match &decl.node {
                DeclKind::TypeAlias { ty, .. } => {
                    if let TypeKind::Record { fields: fs, .. } = &ty.node {
                        for f in fs {
                            all_fields.insert(f.name.clone());
                        }
                    }
                }
                DeclKind::Source { ty, .. } => {
                    if let TypeKind::Record { fields: fs, .. } = &ty.node {
                        for f in fs {
                            all_fields.insert(f.name.clone());
                        }
                    }
                }
                DeclKind::Data { constructors, .. } => {
                    for ctor in constructors {
                        for f in &ctor.fields {
                            all_fields.insert(f.name.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        for name in all_fields {
            items.push(CompletionItem {
                label: name,
                kind: Some(CompletionItemKind::FIELD),
                ..Default::default()
            });
        }
        return Some(CompletionResponse::Array(items));
    }

    // General completion: keywords + snippets + declarations + builtins

    // Context detection: if cursor is in a type annotation position (after `:` or `[`),
    // only suggest types and type constructors
    let in_type_context = {
        let before = &doc.source[..offset];
        let trimmed = before.trim_end();
        trimmed.ends_with(':') || trimmed.ends_with('[')
            || trimmed.ends_with("->")
    };

    if in_type_context {
        // Only suggest types: data types, type aliases, built-in types
        for decl in &doc.module.decls {
            match &decl.node {
                DeclKind::Data { name, .. } => {
                    items.push(CompletionItem {
                        label: name.clone(),
                        kind: Some(CompletionItemKind::STRUCT),
                        detail: doc.details.get(name).cloned(),
                        ..Default::default()
                    });
                }
                DeclKind::TypeAlias { name, ty, .. } => {
                    // Refined-type aliases get their predicate inline as the
                    // detail string so the user sees what the value must
                    // satisfy without having to navigate to the declaration.
                    let (detail, doc_md) = match &ty.node {
                        TypeKind::Refined { base, predicate } => {
                            let base_str = format_type_kind(&base.node);
                            let pred_src = doc
                                .source
                                .get(predicate.span.start..predicate.span.end)
                                .map(|s| s.trim().to_string())
                                .unwrap_or_else(|| "…".into());
                            (
                                Some(format!("refined {base_str} where {pred_src}")),
                                Some(format!(
                                    "Refined type. Values of `{name}` must satisfy `{pred_src}`."
                                )),
                            )
                        }
                        _ => (doc.details.get(name).cloned(), None),
                    };
                    let documentation = doc_md.map(|s| {
                        Documentation::MarkupContent(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: s,
                        })
                    });
                    items.push(CompletionItem {
                        label: name.clone(),
                        kind: Some(CompletionItemKind::STRUCT),
                        detail,
                        documentation,
                        ..Default::default()
                    });
                }
                DeclKind::Trait { name, .. } => {
                    items.push(CompletionItem {
                        label: name.clone(),
                        kind: Some(CompletionItemKind::INTERFACE),
                        ..Default::default()
                    });
                }
                _ => {}
            }
        }
        for ty in &["Int", "Float", "Text", "Bool", "IO", "Maybe", "Result"] {
            items.push(CompletionItem {
                label: ty.to_string(),
                kind: Some(CompletionItemKind::STRUCT),
                ..Default::default()
            });
        }
        return Some(CompletionResponse::Array(items));
    }

    // Keywords
    for kw in KEYWORDS {
        items.push(CompletionItem {
            label: kw.to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        });
    }

    // Snippet completions for common patterns. Context-filtered: a `route`
    // snippet only surfaces at top-level positions, a `let` snippet only
    // inside a do-block, etc. Keeps the completion list scoped to what the
    // cursor can actually parse.
    let snippet_ctx = detect_snippet_context(doc, offset, in_atomic);
    for (label, detail, snippet, ctx) in SNIPPETS {
        if !snippet_context_matches(*ctx, snippet_ctx) {
            continue;
        }
        items.push(CompletionItem {
            label: label.to_string(),
            kind: Some(CompletionItemKind::SNIPPET),
            detail: Some(detail.to_string()),
            insert_text: Some(snippet.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        });
    }

    // Declarations from current document with type details
    for decl in &doc.module.decls {
        match &decl.node {
            DeclKind::Data {
                name, constructors, ..
            } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::STRUCT),
                    detail: doc.details.get(name).cloned(),
                    ..Default::default()
                });
                for ctor in constructors {
                    let snippet = build_constructor_snippet(&ctor.name, &ctor.fields);
                    items.push(CompletionItem {
                        label: ctor.name.clone(),
                        kind: Some(CompletionItemKind::ENUM_MEMBER),
                        detail: doc.details.get(&ctor.name).cloned(),
                        insert_text: snippet,
                        insert_text_format: Some(InsertTextFormat::SNIPPET),
                        ..Default::default()
                    });
                }
            }
            DeclKind::TypeAlias { name, .. } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::STRUCT),
                    detail: doc.details.get(name).cloned(),
                    ..Default::default()
                });
            }
            DeclKind::Source { name, .. } | DeclKind::View { name, .. } => {
                items.push(CompletionItem {
                    label: format!("*{name}"),
                    kind: Some(CompletionItemKind::VARIABLE),
                    insert_text: Some(format!("*{name}")),
                    detail: doc.type_info.get(name.as_str()).cloned(),
                    ..Default::default()
                });
            }
            DeclKind::Derived { name, .. } => {
                items.push(CompletionItem {
                    label: format!("&{name}"),
                    kind: Some(CompletionItemKind::VARIABLE),
                    insert_text: Some(format!("&{name}")),
                    detail: doc.type_info.get(name.as_str()).cloned(),
                    ..Default::default()
                });
            }
            DeclKind::Fun { name, .. } => {
                let snippet = build_function_call_snippet(&doc.module, name);
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::FUNCTION),
                    detail: doc.type_info.get(name.as_str()).cloned(),
                    insert_text: snippet,
                    insert_text_format: Some(InsertTextFormat::SNIPPET),
                    ..Default::default()
                });
            }
            DeclKind::Trait { name, .. } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::INTERFACE),
                    detail: doc.details.get(name).cloned(),
                    ..Default::default()
                });
            }
            _ => {}
        }
    }

    // Built-in functions with type info. Synthesize a snippet from the
    // arity recorded in `type_info` so users get tab stops on call.
    for name in state_builtins() {
        let detail = doc.type_info.get(name).cloned();
        let snippet = detail
            .as_deref()
            .and_then(|ty| build_builtin_call_snippet(name, ty));
        items.push(CompletionItem {
            label: name.to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail,
            insert_text: snippet,
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        });
    }

    // Auto-import completions: scan workspace for symbols not in current document.
    // Uses the parsed-import cache (populated lazily as imports are resolved
    // for any open file) plus a one-shot disk read for files we haven't parsed
    // yet. Modules are not re-parsed across completion requests within a single
    // analyze cycle.
    {
        // Cap auto-import suggestions so a workspace with thousands of exported
        // symbols can't generate a giant payload on every keystroke. The user
        // will type-filter further; clients also typically truncate before
        // rendering.
        const MAX_AUTO_IMPORT_ITEMS: usize = 500;

        let source_path = uri_to_path(uri);
        let existing_imports: HashSet<String> = doc.module.imports.iter().map(|i| i.path.clone()).collect();
        let local_names: HashSet<&str> = doc.definitions.keys().map(|s| s.as_str()).collect();

        // De-dupe by name across files: if two workspace files both export
        // `parse`, prefer the one with the lexicographically-shortest path.
        let mut seen_names: HashSet<String> = HashSet::new();
        let mut auto_imports_added: usize = 0;

        let files = scan_knot_files_in_roots(
            &state.workspace_roots,
            state.workspace_root.as_deref(),
        );
        'files: for file_path in files {
            if auto_imports_added >= MAX_AUTO_IMPORT_ITEMS {
                break;
            }
            let canonical = match file_path.canonicalize() {
                Ok(p) => p,
                Err(_) => continue,
            };
            // Skip current file
            if source_path.as_ref().and_then(|p| p.canonicalize().ok()) == Some(canonical.clone()) {
                continue;
            }
            // Compute the import path relative to the current file
            let import_path = match source_path.as_ref().and_then(|p| p.parent()) {
                Some(base) => {
                    match canonical.strip_prefix(base) {
                        Ok(rel) => rel.with_extension("").to_string_lossy().to_string(),
                        Err(_) => continue,
                    }
                }
                None => continue,
            };
            // Skip already imported files
            if existing_imports.contains(&import_path) {
                continue;
            }

            // Reuse the cached parsed module if available (populated by
            // resolve_import_navigation when other files have imported it),
            // and populate the cache if not — auto-import completion is
            // typically the first request that touches new workspace files.
            let module = match get_or_parse_file_shared(&canonical, &state.import_cache)
            {
                Some((m, _)) => m,
                None => continue,
            };

            for decl in &module.decls {
                // Only suggest exported names (or all top-level if `export`
                // isn't being used in this file)
                let (name, kind) = match &decl.node {
                    DeclKind::Fun { name, .. } => (name.clone(), CompletionItemKind::FUNCTION),
                    DeclKind::Data { name, .. } => (name.clone(), CompletionItemKind::STRUCT),
                    DeclKind::TypeAlias { name, .. } => (name.clone(), CompletionItemKind::STRUCT),
                    DeclKind::Trait { name, .. } => (name.clone(), CompletionItemKind::INTERFACE),
                    _ => continue,
                };
                // Skip names already defined locally or already suggested
                if local_names.contains(name.as_str()) || seen_names.contains(&name) {
                    continue;
                }
                seen_names.insert(name.clone());

                // Defer `additional_text_edits` computation to resolve —
                // produce a marker payload via `data` instead. The resolve
                // handler computes the import-line edit only for the item
                // the user actually selects, avoiding O(workspace_files ×
                // decls) edit construction per keystroke.
                let data = serde_json::json!({
                    "kind": "auto_import",
                    "name": name,
                    "import_path": import_path,
                    "source_uri": uri.to_string(),
                });

                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(kind),
                    detail: Some(format!("auto-import from {import_path}")),
                    sort_text: Some(format!("zz_{name}")), // sort after local items
                    data: Some(data),
                    ..Default::default()
                });
                auto_imports_added += 1;
                if auto_imports_added >= MAX_AUTO_IMPORT_ITEMS {
                    break 'files;
                }
            }
        }
    }

    // Monad-aware ranking: inside a do-block, items whose type sits in the
    // contextual monad sort first. The HKT unifier resolves the monad even when
    // the source is a partial expression (e.g. mid-typing inside a `<-` bind),
    // so the bias kicks in continuously as the user types.
    if let Some(do_span) = find_enclosing_do_span(&doc.module, offset) {
        if let Some(monad) = monad_for_do_span(&doc.monad_info, do_span) {
            for item in items.iter_mut() {
                let label = item.label.trim_start_matches(['*', '&']);
                if let Some(ty) = doc.type_info.get(label) {
                    if type_matches_monad(ty, &monad) {
                        // Prefix the existing sort_text (or label fallback) so
                        // matching items rank ahead of everything else but keep
                        // their relative order from the original list.
                        let base = item
                            .sort_text
                            .clone()
                            .unwrap_or_else(|| item.label.clone());
                        item.sort_text = Some(format!("aaa_{base}"));
                    }
                }
            }
        }
    }

    // Type-aware ranking: when the cursor sits at an argument position in a
    // function call we know the expected parameter type. Push candidates
    // whose return type matches that expectation. This is the same signal
    // signature-help uses to highlight the active parameter — reusing the
    // detection here keeps the two features consistent.
    if let Some((func_name, active_param)) =
        crate::shared::find_enclosing_application(&doc.module, &doc.source, offset)
    {
        // Resolve the function's type. Globals first, then locals (let-bound
        // lambdas, do-block binds). Mirrors signature_help's lookup order.
        let func_ty = doc
            .type_info
            .get(func_name.as_str())
            .cloned()
            .or_else(|| lookup_local_binding_type(doc, &func_name, offset));
        if let Some(ty) = func_ty {
            let params = crate::shared::parse_function_params(&ty);
            // The last entry is the return type; everything before it is a
            // parameter slot. Skip ranking when the active position is past
            // the last param (likely typing past the end of an arity-n call).
            if !params.is_empty() && active_param + 1 < params.len() {
                let expected = params[active_param].as_str();
                let arity_remaining = params.len().saturating_sub(active_param + 1);
                rank_by_type_alignment(&mut items, doc, expected, arity_remaining);
            }
        }
    }

    if in_atomic {
        items.retain(|item| !is_disallowed_in_atomic(&item.label, doc, state));
    }

    // Final ordering pass: assign a stable category prefix to every item that
    // doesn't already have a higher-priority sort_text. The categories layer
    // beneath the contextual `aaa_`/`aab_` prefixes set above so type/monad
    // hits stay first; among the rest, locals beat builtins beat keywords beat
    // snippets beat auto-imports. Without this the editor falls back to
    // alphabetical-by-label, which buries the "you almost certainly want
    // `helper`" candidate behind 80 builtins starting with `a-h`.
    apply_default_category_ranking(&mut items, doc);

    Some(CompletionResponse::Array(items))
}

/// Bump the `sort_text` of items whose return type matches `expected` and
/// whose remaining arity matches what the call site needs. We do not replace
/// existing higher-priority `aaa_` (monad) prefixes — those are stronger
/// signals — but we do upgrade plain items that have only a default prefix.
fn rank_by_type_alignment(
    items: &mut [CompletionItem],
    doc: &DocumentState,
    expected: &str,
    arity_remaining: usize,
) {
    let expected_norm = expected.trim();
    for item in items.iter_mut() {
        // Items with a stronger ranking (`aaa_*` from monad) keep theirs.
        if item
            .sort_text
            .as_deref()
            .map_or(false, |s| s.starts_with("aaa_"))
        {
            continue;
        }
        let label = item.label.trim_start_matches(['*', '&']);
        let Some(ty) = doc.type_info.get(label).or_else(|| item.detail.as_ref())
        else {
            continue;
        };
        let candidate_params = crate::shared::parse_function_params(ty);
        // For non-function items, the type itself is the "return". For
        // function items, we compare the final arrow segment.
        let candidate_ret = if candidate_params.is_empty() {
            ty.as_str()
        } else {
            candidate_params.last().map(String::as_str).unwrap_or("")
        };
        // Type alignment is approximate: we compare the principal type names
        // (after stripping IO/list wrappers) so `[Int]` matches `[Int]` and
        // `Person` matches `Person`. Better than nothing for ranking; not
        // strict enough to mislead.
        let aligns = type_strings_align(candidate_ret.trim(), expected_norm);
        // Arity alignment: a candidate's residual arity (params after partial
        // application from this call site) must not exceed the slot's needs.
        // Concretely, if the user is filling slot `n` of an `n+k` call, only
        // values (arity 0) or single-arg curried functions fit. We give a
        // smaller bump for arity-mismatched candidates so they don't outrank
        // a perfect match.
        let candidate_arity = candidate_params.len().saturating_sub(1);
        let arity_ok = candidate_arity == 0 || candidate_arity <= arity_remaining;
        if aligns {
            let base = item
                .sort_text
                .clone()
                .unwrap_or_else(|| item.label.clone());
            let prefix = if arity_ok { "aab_" } else { "aac_" };
            item.sort_text = Some(format!("{prefix}{base}"));
        }
    }
}

/// Approximate type-string equivalence, robust to whitespace and the common
/// IO/relation wrappers. Both inputs are formatted-type strings drawn from
/// `type_info`, so we don't have a structured type to compare against.
fn type_strings_align(a: &str, b: &str) -> bool {
    let strip = |s: &str| -> String {
        let t = s.trim();
        // Drop a leading IO effect annotation: `IO {fs} Text` → `Text`.
        let t = if let Some(rest) = t.strip_prefix("IO ") {
            if rest.starts_with('{') {
                if let Some(close) = rest.find('}') {
                    rest[close + 1..].trim().to_string()
                } else {
                    rest.to_string()
                }
            } else {
                rest.trim().to_string()
            }
        } else {
            t.to_string()
        };
        // Collapse runs of whitespace so `[ Int ]` matches `[Int]`.
        t.split_whitespace().collect::<Vec<_>>().join(" ")
    };
    let a_n = strip(a);
    let b_n = strip(b);
    if a_n == b_n {
        return true;
    }
    // Loose match on the principal type name — good for ranking when generic
    // params differ but the head matches.
    let head = |s: &str| {
        s.split(|c: char| c.is_whitespace() || c == '<' || c == '(' || c == '[')
            .next()
            .unwrap_or("")
            .to_string()
    };
    let a_head = head(&a_n);
    let b_head = head(&b_n);
    !a_head.is_empty() && a_head == b_head
}

/// Look up the inferred type of a locally-bound name visible at `offset`.
/// Mirrors signature_help::lookup_local_binding_type — duplicated rather than
/// shared so the two callers don't pay for an extra import in the common
/// path.
fn lookup_local_binding_type(doc: &DocumentState, name: &str, offset: usize) -> Option<String> {
    let mut best: Option<(Span, String)> = None;
    for (span, ty) in &doc.local_type_info {
        if span.end > offset {
            continue;
        }
        if span.end > doc.source.len() || span.start > span.end {
            continue;
        }
        if &doc.source[span.start..span.end] != name {
            continue;
        }
        match &best {
            None => best = Some((*span, ty.clone())),
            Some((cur, _)) if span.start > cur.start => best = Some((*span, ty.clone())),
            _ => {}
        }
    }
    best.map(|(_, ty)| ty)
}

/// Apply a stable category prefix to every item that doesn't already have a
/// higher-priority bias. Categories rank as:
///   `b_local`        — declarations from this document
///   `c_keyword`      — language keywords
///   `d_snippet`      — snippet templates
///   `e_builtin`      — stdlib functions and types
///   `f_other`        — everything else (auto-import etc. already use `zz_`)
/// Within a category items keep their original alphabetical order (the
/// editor sorts on `sort_text`, falling back to `label`).
fn apply_default_category_ranking(items: &mut [CompletionItem], doc: &DocumentState) {
    let local_names: std::collections::HashSet<&str> =
        doc.definitions.keys().map(String::as_str).collect();
    for item in items.iter_mut() {
        // Skip items that already carry a contextual prefix — those bumps are
        // signals about *this* completion request, stronger than category.
        if let Some(s) = item.sort_text.as_deref() {
            if s.starts_with("aaa_") || s.starts_with("aab_") || s.starts_with("aac_") {
                continue;
            }
            // Auto-import is already explicitly demoted to `zz_`; respect that.
            if s.starts_with("zz_") {
                continue;
            }
        }
        let label = item.label.trim_start_matches(['*', '&']);
        let prefix = match item.kind {
            Some(CompletionItemKind::SNIPPET) => "d_snippet_",
            Some(CompletionItemKind::KEYWORD) => "c_keyword_",
            _ if local_names.contains(label) => "b_local_",
            _ if knot_compiler::builtins::ALL_BUILTINS
                .iter()
                .copied()
                .flatten()
                .any(|n| *n == label) =>
            {
                "e_builtin_"
            }
            _ => "f_other_",
        };
        item.sort_text = Some(format!("{prefix}{label}"));
    }
}

/// True if a completion candidate would be rejected by the effect checker
/// inside an `atomic` block. Mirrors the rule in `effects.rs`: any builtin
/// from `ATOMIC_DISALLOWED_BUILTINS`, plus any user function whose inferred
/// effect set contains console/network/fs/clock/random — including imported
/// functions that show up in the *current* doc (effect_sets is populated
/// after import resolution) and auto-import candidates that show up in any
/// open document's effect_sets.
fn is_disallowed_in_atomic(label: &str, doc: &DocumentState, state: &ServerState) -> bool {
    let bare = label.trim_start_matches(['*', '&']);
    if ATOMIC_DISALLOWED_BUILTINS.contains(&bare) {
        return true;
    }
    if let Some(eff) = doc.effect_sets.get(bare) {
        return eff.has_io();
    }
    // Auto-import items don't have entries in this doc's effect_sets yet
    // (the import isn't applied), but if any open doc declares the same
    // name with IO effects, it's almost certainly the same definition.
    for other in state.documents.values() {
        if let Some(eff) = other.effect_sets.get(bare) {
            if eff.has_io() {
                return true;
            }
        }
    }
    false
}

/// Return the span of a `route` or `route Foo = ...` declaration that
/// encloses `offset`, or `None` if the cursor is outside any route block.
fn find_enclosing_route_decl_span(module: &Module, offset: usize) -> Option<Span> {
    for decl in &module.decls {
        let in_span = decl.span.start <= offset && offset <= decl.span.end;
        if !in_span {
            continue;
        }
        if matches!(
            &decl.node,
            DeclKind::Route { .. } | DeclKind::RouteComposite { .. }
        ) {
            return Some(decl.span);
        }
    }
    None
}

/// Build the completion list for a position inside a `route … where` block.
/// Surfaces HTTP method keywords, the soft `headers` keyword, and types that
/// are valid in field-of-type positions; everything else (functions, builtins,
/// snippets) would be a parse error inside a route declaration.
fn route_completions(doc: &DocumentState) -> Vec<CompletionItem> {
    let mut items = Vec::new();

    for method in ["GET", "POST", "PUT", "DELETE", "PATCH"] {
        items.push(CompletionItem {
            label: method.to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("HTTP method".into()),
            ..Default::default()
        });
    }

    items.push(CompletionItem {
        label: "headers".to_string(),
        kind: Some(CompletionItemKind::KEYWORD),
        detail: Some("typed request/response headers block".into()),
        ..Default::default()
    });

    for ty in &["Int", "Float", "Text", "Bool", "Bytes", "Maybe", "Result"] {
        items.push(CompletionItem {
            label: ty.to_string(),
            kind: Some(CompletionItemKind::STRUCT),
            ..Default::default()
        });
    }

    for decl in &doc.module.decls {
        match &decl.node {
            DeclKind::Data { name, .. } | DeclKind::TypeAlias { name, .. } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::STRUCT),
                    detail: doc.details.get(name).cloned(),
                    ..Default::default()
                });
            }
            _ => {}
        }
    }

    items
}

/// Find the smallest `do { ... }` whose span encloses `offset`. Walks every
/// declaration body — do-blocks can nest arbitrarily inside lambdas, case arms,
/// record fields, etc.
fn find_enclosing_do_span(module: &Module, offset: usize) -> Option<Span> {
    fn walk(expr: &ast::Expr, offset: usize, best: &mut Option<Span>) {
        if expr.span.start > offset || offset > expr.span.end {
            return;
        }
        if let ast::ExprKind::Do(_) = &expr.node {
            let size = expr.span.end - expr.span.start;
            if best.map_or(true, |b| size < b.end - b.start) {
                *best = Some(expr.span);
            }
        }
        recurse_expr(expr, |e| walk(e, offset, best));
    }
    let mut best: Option<Span> = None;
    for decl in &module.decls {
        if decl.span.start > offset || offset > decl.span.end {
            continue;
        }
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => walk(body, offset, &mut best),
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk(body, offset, &mut best);
                    }
                }
            }
            _ => {}
        }
    }
    best
}

/// Classify the cursor's surrounding context for snippet filtering. The
/// returned variant is what the cursor actually *is* — `snippet_context_matches`
/// then checks whether each snippet's declared context covers it.
///
/// Detection precedence (innermost wins): atomic block → do-block → expression
/// inside any decl → top level. The `in_atomic` flag is computed by the caller
/// so we don't redo the AST walk; everything else is a quick offset check.
fn detect_snippet_context(doc: &DocumentState, offset: usize, in_atomic: bool) -> SnippetContext {
    if in_atomic {
        return SnippetContext::AtomicBlock;
    }
    if find_enclosing_do_span(&doc.module, offset).is_some() {
        return SnippetContext::DoBlock;
    }
    // Inside any declaration body? Treat as expression position. Decls that
    // don't have a body (data, type, source, trait header, etc.) leave the
    // cursor at top-level even when textually overlapping their span — but
    // the body-bearing decls are what matter for expression-position snippets.
    for decl in &doc.module.decls {
        let inside = decl.span.start <= offset && offset <= decl.span.end;
        if !inside {
            continue;
        }
        match &decl.node {
            ast::DeclKind::Fun { body: Some(body), .. }
            | ast::DeclKind::View { body, .. }
            | ast::DeclKind::Derived { body, .. } => {
                if body.span.start <= offset && offset <= body.span.end {
                    return SnippetContext::Expression;
                }
            }
            ast::DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        if body.span.start <= offset && offset <= body.span.end {
                            return SnippetContext::Expression;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    SnippetContext::TopLevel
}

/// Whether a snippet declared with context `decl` is appropriate at a cursor
/// whose context is `cursor`. Most declared contexts are concrete; `Any` is
/// always allowed; `Expression` covers do-blocks and atomic blocks too (they
/// are expression positions). Atomic-specific snippets aren't gated separately
/// here — atomic also matches `Expression` since all our atomic snippets are
/// expression-shaped.
fn snippet_context_matches(decl: SnippetContext, cursor: SnippetContext) -> bool {
    match (decl, cursor) {
        (SnippetContext::Any, _) => true,
        (d, c) if d == c => true,
        // Expression-position snippets are also OK in do-blocks and atomic
        // blocks — those are still expression positions, just with extra
        // semantic constraints handled by other filters.
        (SnippetContext::Expression, SnippetContext::DoBlock) => true,
        (SnippetContext::Expression, SnippetContext::AtomicBlock) => true,
        // do-block snippets can fire in atomic blocks too.
        (SnippetContext::DoBlock, SnippetContext::AtomicBlock) => true,
        _ => false,
    }
}

/// Return the resolved monad kind for the do-block at `do_span`, if any. The
/// type checker registers monad-vars with spans tied to the desugared
/// `__bind`/`__yield`/`__empty` callsites, which sit inside the original
/// do-block, so any `monad_info` entry whose span is contained in `do_span`
/// is a valid sample of that block's monad.
fn monad_for_do_span(
    monad_info: &HashMap<Span, MonadKind>,
    do_span: Span,
) -> Option<MonadKind> {
    monad_info
        .iter()
        .find(|(s, _)| s.start >= do_span.start && s.end <= do_span.end)
        .map(|(_, k)| k.clone())
}

/// True if the rendered type of a completion candidate is a value in the
/// requested monad. The match is structural-by-string (we only have the
/// formatted type text in `type_info`); good enough for ranking, not for
/// type checking. For function types like `a -> Maybe a`, also looks at the
/// return type so constructors that produce monad values rank up too.
fn type_matches_monad(ty: &str, monad: &MonadKind) -> bool {
    let t = ty.trim();
    if monad_head_matches(t, monad) {
        return true;
    }
    // Function type: walk past the arrows and look at the final return position.
    // Constructors are typed `field -> ParentType`, so a Maybe-returning
    // constructor like `Just : a -> Maybe a` wouldn't match by leading prefix
    // but should still rank into a Maybe context.
    let params = crate::shared::parse_function_params(t);
    if params.len() > 1 {
        if let Some(ret) = params.last() {
            return monad_head_matches(ret.trim(), monad);
        }
    }
    false
}

/// Helper: does the head of a non-function type string sit in the requested
/// monad? Pure-string match — we don't have AST-level info at this point.
fn monad_head_matches(t: &str, monad: &MonadKind) -> bool {
    match monad {
        MonadKind::Relation => {
            t.starts_with('[') || t.contains(" [") || t.contains("IO ")
        }
        MonadKind::IO => t.starts_with("IO ") || t.starts_with("IO{") || t == "IO",
        MonadKind::Adt(name) => {
            let prefix_eq = t == name;
            let prefix_app = t
                .split_once(|c: char| c.is_whitespace() || c == '<' || c == '(')
                .map(|(head, _)| head == name)
                .unwrap_or(false);
            prefix_eq || prefix_app
        }
    }
}

/// Return the bare identifier immediately preceding the dot at `dot_pos`, if
/// the receiver is a simple variable. Used to attach refinement metadata to
/// dot-completion items when the variable was bound from a `*source`.
fn receiver_ident_before_dot(source: &str, dot_pos: usize) -> Option<String> {
    let bytes = source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut end = dot_pos;
    while end > 0 && bytes[end - 1] == b' ' {
        end -= 1;
    }
    let ident_end = end;
    while end > 0 && is_ident(bytes[end - 1]) {
        end -= 1;
    }
    if end == ident_end {
        return None;
    }
    let name = &source[end..ident_end];
    // Reject leading sigils — those land us in source/derived ref territory,
    // which is handled separately.
    if name.starts_with('*') || name.starts_with('&') {
        return None;
    }
    Some(name.to_string())
}

/// Try to resolve field names for dot completion by finding the type of the
/// expression before the dot.
fn resolve_dot_fields(doc: &DocumentState, dot_pos: usize) -> Vec<String> {
    let bytes = doc.source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    // Find the identifier immediately before the dot
    let mut end = dot_pos;
    while end > 0 && bytes[end - 1] == b' ' {
        end -= 1;
    }
    let ident_end = end;
    while end > 0 && is_ident(bytes[end - 1]) {
        end -= 1;
    }
    if end == ident_end {
        return Vec::new();
    }

    let var_name = &doc.source[end..ident_end];

    // Look up the variable's type
    let type_str = find_type_for_name(doc, var_name, end);
    let type_str = match type_str {
        Some(t) => t,
        None => return Vec::new(),
    };

    // Parse fields from the type string
    extract_fields_from_type_str(&type_str, &doc.module)
}

/// Find the type string for a name, checking local bindings first, then globals.
fn find_type_for_name(doc: &DocumentState, name: &str, offset: usize) -> Option<String> {
    // Check local type info: find a binding whose span covers this identifier
    // Use the full identifier range [offset..ident_end) for more precise matching
    let ident_end = offset + name.len();
    for (span, ty) in &doc.local_type_info {
        if span.start <= offset && ident_end <= span.end {
            return Some(ty.clone());
        }
    }
    // Check if any reference at this offset points to a local binding with a known type
    for (usage_span, def_span) in &doc.references {
        if usage_span.start <= offset && offset < usage_span.end {
            if let Some(ty) = doc.local_type_info.get(def_span) {
                return Some(ty.clone());
            }
        }
    }
    // Check global type info
    doc.type_info.get(name).cloned()
}

/// Extract field names from a type string like `{name: Text, age: Int}` or a named type.
fn extract_fields_from_type_str(type_str: &str, module: &Module) -> Vec<String> {
    let type_str = type_str.trim();

    // Direct record type: `{name: Text, age: Int}`
    if type_str.starts_with('{') && type_str.ends_with('}') {
        return extract_record_fields(type_str);
    }

    // Relation type: `[{name: Text}]` or `[Person]` — extract inner type
    if type_str.starts_with('[') && type_str.ends_with(']') {
        let inner = &type_str[1..type_str.len() - 1];
        return extract_fields_from_type_str(inner, module);
    }

    // IO type: `IO {...} [T]` or `IO {...} {fields}` — skip to the value type
    if type_str.starts_with("IO ") {
        let rest = &type_str[3..];
        // Skip the effect set `{...}`
        if rest.starts_with('{') {
            if let Some(close) = rest.find('}') {
                let value_type = rest[close + 1..].trim();
                return extract_fields_from_type_str(value_type, module);
            }
        }
    }

    // Maybe type: `Maybe T` — unwrap to inner type
    if type_str.starts_with("Maybe ") {
        let inner = type_str[6..].trim();
        return extract_fields_from_type_str(inner, module);
    }

    // Named type: look up in the module's declarations
    for decl in &module.decls {
        match &decl.node {
            DeclKind::TypeAlias { name, ty, .. } if name == type_str => {
                match &ty.node {
                    TypeKind::Record { fields, .. } => {
                        return fields.iter().map(|f| f.name.clone()).collect();
                    }
                    // Follow alias to another named type
                    TypeKind::Named(target) => {
                        return extract_fields_from_type_str(target, module);
                    }
                    _ => {}
                }
            }
            DeclKind::Source { name, ty, .. } if name == type_str => {
                if let TypeKind::Record { fields, .. } = &ty.node {
                    return fields.iter().map(|f| f.name.clone()).collect();
                }
            }
            // Data type with a single constructor — expose its fields
            DeclKind::Data { name, constructors, .. } if name == type_str => {
                if constructors.len() == 1 {
                    return constructors[0].fields.iter().map(|f| f.name.clone()).collect();
                }
            }
            _ => {}
        }
    }

    Vec::new()
}

// ── Completion Resolve ───────────────────────────────────────────────

pub(crate) fn handle_resolve_completion_item(
    state: &ServerState,
    mut item: CompletionItem,
) -> CompletionItem {
    // Strip the relation/derived prefix so lookups succeed for `*todos`/`&seniors`.
    let label = item.label.trim_start_matches(['*', '&']).to_string();

    // If this is an auto-import marker (added by the workspace scan in
    // `handle_completion`), resolve the actual TextEdit lazily here.
    if let Some(data) = item.data.clone() {
        if data.get("kind").and_then(|v| v.as_str()) == Some("auto_import") {
            let import_path = data.get("import_path").and_then(|v| v.as_str()).unwrap_or("");
            let source_uri = data.get("source_uri").and_then(|v| v.as_str()).unwrap_or("");
            if !import_path.is_empty() && !source_uri.is_empty() {
                if let Ok(uri) = source_uri.parse::<Uri>() {
                    if let Some(doc) = state.documents.get(&uri) {
                        let import_insert_pos = if let Some(last_import) = doc.module.imports.last() {
                            let end = offset_to_position(&doc.source, last_import.span.end);
                            Position::new(end.line + 1, 0)
                        } else {
                            Position::new(0, 0)
                        };
                        let import_line = if doc.module.imports.is_empty() {
                            format!("import {import_path}\n\n")
                        } else {
                            format!("import {import_path}\n")
                        };
                        item.additional_text_edits = Some(vec![TextEdit {
                            range: Range {
                                start: import_insert_pos,
                                end: import_insert_pos,
                            },
                            new_text: import_line,
                        }]);
                    }
                }
            }
        }
    }

    // Aggregate enrichment across all open documents — workspace-symbol-style
    // labels can come from any file, and effect/doc/type info may live in
    // different files (e.g. trait declared in A, impl in B).
    let mut detail: Option<String> = item.detail.clone();
    let mut doc_md: Option<String> = None;
    let mut sections: Vec<String> = Vec::new();

    let push_unique = |sections: &mut Vec<String>, s: String| {
        if !sections.contains(&s) {
            sections.push(s);
        }
    };

    for doc in state.documents.values() {
        if detail.is_none() {
            if let Some(ty) = doc.type_info.get(label.as_str()) {
                detail = Some(ty.clone());
            }
        }
        if doc_md.is_none() {
            if let Some(d) = doc.doc_comments.get(label.as_str()) {
                doc_md = Some(d.clone());
            }
        }
        if let Some(eff) = doc.effect_info.get(label.as_str()) {
            push_unique(&mut sections, format!("*Effects:* `{eff}`"));
        }
        if let Some(predicate) = doc.refined_types.get(label.as_str()) {
            let pred_src = predicate_to_source(predicate, &doc.source);
            push_unique(
                &mut sections,
                format!("*Refinement:* values of `{label}` must satisfy `{pred_src}`"),
            );
        }
        // Route constructor preview: show method + path so the user can pick
        // the right ADT variant when constructing routed requests.
        if let Some(summary) = format_route_constructor_hover(&doc.module, &label) {
            push_unique(&mut sections, summary);
        }
        // Trait method default body: when a method is declared with a default
        // body, render the source so it's visible in the completion expansion.
        if let Some(default_src) = trait_method_default_source(&doc.module, &doc.source, &label)
        {
            push_unique(
                &mut sections,
                format!("*Default impl:*\n```knot\n{default_src}\n```"),
            );
        }
        // Data constructor list: hovering over a type name shouldn't require
        // a separate trip — show the constructors inline.
        if let Some(ctors) = data_constructor_summary(&doc.module, &label) {
            push_unique(&mut sections, ctors);
        }
        // Trait dispatch: when the user is completing a method declared by a
        // trait, list the types that supply it. The companion code-lens shows
        // the same info inline in the source — completion resolve makes it
        // discoverable from the picker too.
        if let Some(dispatch) = trait_method_dispatch_summary(&doc.module, &label) {
            push_unique(&mut sections, dispatch);
        }
        // Function-level trait constraints: surface the `Display a => …`
        // requirements so the user notices that calling this function brings
        // in trait dispatch.
        if let Some(constraints) = function_constraint_summary(&doc.module, &label) {
            push_unique(&mut sections, constraints);
        }
    }

    item.detail = detail;

    let mut combined = doc_md.unwrap_or_default();
    for section in sections {
        if combined.is_empty() {
            combined = section;
        } else {
            combined.push_str("\n\n---\n\n");
            combined.push_str(&section);
        }
    }
    if !combined.is_empty() {
        item.documentation = Some(Documentation::MarkupContent(MarkupContent {
            kind: MarkupKind::Markdown,
            value: combined,
        }));
    }

    item
}

/// If `name` resolves to a trait method declared in `module`, render a list
/// of impls in the same module that supply the method. Returns `None` when
/// the name isn't a trait method or no impls exist locally.
fn trait_method_dispatch_summary(module: &Module, name: &str) -> Option<String> {
    let mut owning_trait: Option<String> = None;
    for decl in &module.decls {
        if let DeclKind::Trait { name: tn, items, .. } = &decl.node {
            for item in items {
                if let ast::TraitItem::Method { name: mn, .. } = item {
                    if mn == name {
                        owning_trait = Some(tn.clone());
                    }
                }
            }
        }
    }
    let trait_name = owning_trait?;
    let mut providing: Vec<String> = Vec::new();
    for decl in &module.decls {
        if let DeclKind::Impl { trait_name: tn, args, items, .. } = &decl.node {
            if tn != &trait_name {
                continue;
            }
            let provides = items.iter().any(|i| {
                matches!(i, ast::ImplItem::Method { name: n, .. } if n == name)
            });
            if !provides {
                continue;
            }
            let label = args
                .iter()
                .map(|a| format_type_kind(&a.node))
                .collect::<Vec<_>>()
                .join(" ");
            providing.push(label);
        }
    }
    if providing.is_empty() {
        return None;
    }
    Some(format!(
        "*Method of `{trait_name}`* — impls: {}",
        providing
            .iter()
            .map(|t| format!("`{t}`"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// If `name` resolves to a function with declared trait constraints in
/// `module`, render the constraint list. Returns `None` when no such
/// function exists or it has no constraints.
fn function_constraint_summary(module: &Module, name: &str) -> Option<String> {
    for decl in &module.decls {
        if let DeclKind::Fun { name: n, ty: Some(scheme), .. } = &decl.node {
            if n == name && !scheme.constraints.is_empty() {
                let cs: Vec<String> = scheme
                    .constraints
                    .iter()
                    .map(|c| {
                        let args: Vec<String> = c
                            .args
                            .iter()
                            .map(|t| format_type_kind(&t.node))
                            .collect();
                        format!("`{} {}`", c.trait_name, args.join(" "))
                    })
                    .collect();
                return Some(format!("*Constraints:* {}", cs.join(", ")));
            }
        }
    }
    None
}

/// If `name` resolves to a trait method with a default body in `module`,
/// return the default-body source slice for completion preview.
fn trait_method_default_source(module: &Module, source: &str, name: &str) -> Option<String> {
    for decl in &module.decls {
        if let DeclKind::Trait { items, .. } = &decl.node {
            for item in items {
                if let ast::TraitItem::Method {
                    name: m,
                    default_body: Some(body),
                    ..
                } = item
                {
                    if m == name {
                        let s = body.span;
                        if s.start < s.end && s.end <= source.len() {
                            return Some(source[s.start..s.end].to_string());
                        }
                    }
                }
            }
        }
    }
    None
}

/// If `name` resolves to a data type, render its constructors as a markdown
/// bullet list. Returns None if `name` is not a top-level data type.
fn data_constructor_summary(module: &Module, name: &str) -> Option<String> {
    for decl in &module.decls {
        if let DeclKind::Data {
            name: dn,
            constructors,
            ..
        } = &decl.node
        {
            if dn != name {
                continue;
            }
            if constructors.is_empty() {
                return None;
            }
            let mut out = String::from("*Constructors:*");
            for ctor in constructors {
                if ctor.fields.is_empty() {
                    out.push_str(&format!("\n- `{}`", ctor.name));
                } else {
                    let fs: Vec<String> = ctor
                        .fields
                        .iter()
                        .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
                        .collect();
                    out.push_str(&format!("\n- `{} {{{}}}`", ctor.name, fs.join(", ")));
                }
            }
            return Some(out);
        }
    }
    None
}

// ── Snippet builders ────────────────────────────────────────────────

/// Build a snippet expansion for a constructor call. With fields, expands to
/// `Ctor {field1: $1, field2: $2}` so users can tab through. Without fields,
/// returns `None` so the plain label is used.
fn build_constructor_snippet(name: &str, fields: &[ast::Field<ast::Type>]) -> Option<String> {
    if fields.is_empty() {
        return None;
    }
    let placeholders: Vec<String> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| format!("{}: ${{{}:{}}}", f.name, i + 1, f.name))
        .collect();
    Some(format!("{name} {{{}}}", placeholders.join(", ")))
}

/// Build a snippet expansion for a function call. Walks the function's lambda
/// chain to recover parameter names; if none are found (e.g. eta-reduced
/// function defined as a non-lambda value), returns `None`.
fn build_function_call_snippet(module: &ast::Module, name: &str) -> Option<String> {
    let params = module.decls.iter().find_map(|decl| match &decl.node {
        DeclKind::Fun {
            name: n,
            body: Some(body),
            ..
        } if n == name => Some(collect_lambda_param_names(body)),
        _ => None,
    })?;
    if params.is_empty() {
        return None;
    }
    let placeholders: Vec<String> = params
        .iter()
        .enumerate()
        .map(|(i, p)| format!("${{{}:{}}}", i + 1, p))
        .collect();
    Some(format!("{name} {}", placeholders.join(" ")))
}

/// Build a snippet expansion for a built-in call. We don't have parameter
/// names for builtins, so we emit `arg1`, `arg2`, … placeholders sized to
/// the type's arrow arity. Returns `None` for nullary builtins.
fn build_builtin_call_snippet(name: &str, ty: &str) -> Option<String> {
    let arity = arrow_arity(ty);
    if arity == 0 {
        return None;
    }
    let placeholders: Vec<String> = (0..arity)
        .map(|i| format!("${{{}:arg{}}}", i + 1, i + 1))
        .collect();
    Some(format!("{name} {}", placeholders.join(" ")))
}

/// Count top-level `->` arrows in a type string. Mirrors the cheap split done
/// by `parse_function_params`, but doesn't allocate the per-param strings.
fn arrow_arity(ty: &str) -> usize {
    let mut depth = 0i32;
    let mut count = 0usize;
    let bytes = ty.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' | b'<' => depth += 1,
            b')' | b']' | b'}' | b'>' => depth -= 1,
            b'-' if depth == 0 && i + 1 < bytes.len() && bytes[i + 1] == b'>' => {
                count += 1;
                i += 2;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    count
}

// ── Import Path Completion ──────────────────────────────────────────

fn complete_import_path(base_dir: &Path, partial: &str) -> Vec<CompletionItem> {
    let mut items = Vec::new();

    // Resolve the directory from the partial path
    let (search_dir, prefix) = if let Some(last_slash) = partial.rfind('/') {
        let dir_part = &partial[..last_slash];
        let file_part = &partial[last_slash + 1..];
        (base_dir.join(dir_part), file_part)
    } else {
        (base_dir.to_path_buf(), partial)
    };

    let entries = match std::fs::read_dir(&search_dir) {
        Ok(e) => e,
        Err(_) => return items,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        // Skip hidden files/dirs
        if name.starts_with('.') || name == "target" || name == "node_modules" {
            continue;
        }

        if path.is_dir() {
            if name.to_lowercase().starts_with(&prefix.to_lowercase()) {
                items.push(CompletionItem {
                    label: format!("{name}/"),
                    kind: Some(CompletionItemKind::FOLDER),
                    insert_text: Some(format!("{name}/")),
                    command: Some(Command {
                        title: "Trigger completion".into(),
                        command: "editor.action.triggerSuggest".into(),
                        arguments: None,
                    }),
                    ..Default::default()
                });
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("knot") {
            let stem = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
            if stem.to_lowercase().starts_with(&prefix.to_lowercase()) {
                items.push(CompletionItem {
                    label: stem.clone(),
                    kind: Some(CompletionItemKind::MODULE),
                    detail: Some("module".into()),
                    ..Default::default()
                });
            }
        }
    }

    items
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestWorkspace;
    use crate::utils::offset_to_position;

    fn comp_params(uri: &Uri, position: Position, trigger: Option<&str>) -> CompletionParams {
        CompletionParams {
            text_document_position: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
            context: Some(CompletionContext {
                trigger_kind: if trigger.is_some() {
                    CompletionTriggerKind::TRIGGER_CHARACTER
                } else {
                    CompletionTriggerKind::INVOKED
                },
                trigger_character: trigger.map(String::from),
            }),
        }
    }

    fn item_labels(resp: CompletionResponse) -> Vec<String> {
        match resp {
            CompletionResponse::Array(items) => items.into_iter().map(|i| i.label).collect(),
            CompletionResponse::List(list) => list.items.into_iter().map(|i| i.label).collect(),
        }
    }

    #[test]
    fn completion_after_star_yields_source_names() {
        let mut ws = TestWorkspace::new();
        // Position the cursor immediately after the `*` literal — the
        // completion handler reads the byte at offset-1 to detect the
        // trigger context.
        let uri = ws.open(
            "main",
            r#"type P = {n: Text}
*people : [P]
*pets : [P]
get = \_ -> *
"#,
        );
        let doc = ws.doc(&uri);
        let star = doc.source.find("\\_ -> *").expect("trigger position");
        let after_star = star + "\\_ -> *".len();
        let pos = offset_to_position(&doc.source, after_star);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, Some("*")))
            .expect("completion returns");
        let labels = item_labels(resp);
        assert!(labels.contains(&"people".to_string()), "labels: {labels:?}");
        assert!(labels.contains(&"pets".to_string()), "labels: {labels:?}");
    }

    #[test]
    fn completion_inside_route_decl_offers_methods_and_types_only() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"type Greeting = {message: Text}
route Hello where
  GET /hi -> Greeting
"#,
        );
        let doc = ws.doc(&uri);
        // Cursor on the second line of the route block — inside a route decl
        // but on a "fresh" line where the user is typing a new entry.
        let off = doc.source.find("GET /hi").expect("route entry") + 3;
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, None))
            .expect("completion returns");
        let labels = item_labels(resp);

        // HTTP methods and the local type alias should be present.
        assert!(labels.contains(&"GET".to_string()), "labels: {labels:?}");
        assert!(labels.contains(&"POST".to_string()), "labels: {labels:?}");
        assert!(
            labels.contains(&"Greeting".to_string()),
            "labels: {labels:?}"
        );
        // Expression-level builtins like `println` make no sense in a route
        // declaration and would be a parse error if accepted.
        assert!(
            !labels.contains(&"println".to_string()),
            "println leaked into route-decl completion; labels: {labels:?}"
        );
    }

    #[test]
    fn completion_dot_includes_field_refinement_detail() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"*scores : [{name: Text, score: Int where \x -> x >= 0}]

main = do
  s <- *scores
  yield s.score
"#,
        );
        let doc = ws.doc(&uri);
        // Position cursor right after the `.` (between the dot and `score`).
        let dot = doc.source.rfind("s.score").expect("dot site") + 2;
        let pos = offset_to_position(&doc.source, dot);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, Some(".")))
            .expect("completion returns");
        let items: Vec<CompletionItem> = match resp {
            CompletionResponse::Array(items) => items,
            CompletionResponse::List(list) => list.items,
        };
        let score = items
            .iter()
            .find(|i| i.label == "score")
            .expect("score field offered");
        let detail = score
            .detail
            .as_deref()
            .expect("refined field has a detail string");
        assert!(
            detail.contains(">= 0") || detail.contains(">=0"),
            "expected refinement predicate in detail; got: {detail:?}"
        );
    }

    #[test]
    fn completion_filters_io_builtins_in_atomic_block() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"type P = {n: Text}
*people : [P]
main = atomic do
  *people = [{n: "A"}]
  yield {}
"#,
        );
        let doc = ws.doc(&uri);
        // Cursor inside the atomic body, on the line after `*people = ...`
        let inside = doc.source.find("[{n:").expect("atomic body");
        let pos = offset_to_position(&doc.source, inside);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, None))
            .expect("completion returns");
        let labels = item_labels(resp);
        // `println` is in ATOMIC_DISALLOWED_BUILTINS — must not appear.
        assert!(
            !labels.contains(&"println".to_string()),
            "println leaked into atomic-context completion; labels: {labels:?}"
        );
    }
}
