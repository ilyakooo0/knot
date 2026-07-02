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
use crate::utils::{offset_to_position, position_to_offset, recurse_expr, uri_to_path};

// ── Completion ──────────────────────────────────────────────────────

/// Replace-range for a relation completion (`*name`/`&name`) at `offset`: the
/// typed token INCLUDING any leading source/derived-ref sigil. Emitting items
/// with `insert_text: "*name"` + a `text_edit` over this range replaces the
/// sigil rather than inserting after it, avoiding `**name`/`&&name` in clients
/// that fold the trigger char into the word start.
fn sigil_replace_range(latest_source: &str, offset: usize) -> Range {
    let bytes = latest_source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';
    let mut start = offset.min(bytes.len());
    while start > 0 && is_ident(bytes[start - 1]) {
        start -= 1;
    }
    if start > 0 && (bytes[start - 1] == b'*' || bytes[start - 1] == b'&') {
        // Only absorb the `*`/`&` into the edit range when it's an actual
        // source/derived-ref sigil (atom position) — NOT when it's a binary
        // operator (`n * m`, `a && b`). Mirroring `trigger_is_operator`: the
        // byte before the sigil being an expression-ending byte (or another
        // `&`) means operator position. Absorbing an operator byte would
        // corrupt the buffer — `n*` accepting `*people` → `n*people`
        // (`n * people`), `flag &&` accepting `&active` → `flag &&active`
        // (the derived-ref sigil silently lost).
        let before_sigil = (start - 1)
            .checked_sub(1)
            .and_then(|i| bytes.get(i))
            .copied();
        let sigil_is_operator = before_sigil.is_some_and(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'_' | b')' | b']' | b'}' | b'"' | b'&')
        });
        if !sigil_is_operator {
            start -= 1;
        }
    }
    // Extend the end past the caret over the rest of the identifier, so a
    // mid-token completion (`*us|ers`) replaces the whole token rather than
    // appending the suffix and producing `*usersers`.
    let mut end = offset.min(bytes.len());
    while end < bytes.len() && is_ident(bytes[end]) {
        end += 1;
    }
    Range {
        start: offset_to_position(latest_source, start),
        end: offset_to_position(latest_source, end),
    }
}

pub(crate) fn handle_completion(
    state: &ServerState,
    params: &CompletionParams,
) -> Option<CompletionResponse> {
    let uri = &params.text_document_position.text_document.uri;
    let doc = state.documents.get(uri)?;
    let pos = params.text_document_position.position;

    // Clients fire completion immediately on a keystroke, while `doc` lags
    // behind by the analysis debounce window. Resolve the cursor and all
    // textual context against the freshest text we have, and trust the
    // client-reported trigger character over inspecting bytes that may not
    // contain the just-typed character yet. Analysis-derived data (module,
    // types) stays best-effort against the older text.
    let latest_source: &str = state
        .pending_sources
        .get(uri)
        .map(|p| p.source.as_str())
        .unwrap_or(&doc.source);
    let offset = position_to_offset(latest_source, pos);
    // AST-based context checks (atomic/route) walk `doc.module`, whose spans
    // index into `doc.source` (the last-analyzed text), not `latest_source`.
    // Resolving the cursor against the analyzed source keeps the offset in the
    // same byte space as those spans when the buffer is mid-debounce.
    let analyzed_offset = position_to_offset(&doc.source, pos);
    let trigger_char = params
        .context
        .as_ref()
        .and_then(|c| c.trigger_character.as_deref())
        .and_then(|s| s.as_bytes().first().copied())
        .or_else(|| {
            if offset > 0 {
                latest_source.as_bytes().get(offset - 1).copied()
            } else {
                None
            }
        });

    // Atomic-block context: when the cursor is inside `atomic { ... }`, the
    // type checker forbids any IO effects (console/fs/network/clock/random).
    // Drop those builtins and any user functions that perform IO from the
    // completion list so the user can't type them.
    let in_atomic = find_enclosing_atomic_expr(&doc.module, &doc.source, analyzed_offset).is_some();

    // Route-declaration context: a `route Foo where ...` block contains only
    // HTTP method keywords, path literals, and field-of-type entries. None of
    // the runtime builtins, lambdas, or do-block scaffolding belongs here.
    // Returning a tightly-scoped completion list keeps the user from typing
    // expression-level snippets that would never parse.
    //
    // Exception: `rateLimit <expr>` clauses hold ordinary EXPRESSIONS
    // (`{key: \inp ctx -> ..., limit: ...}`), so a cursor inside one needs
    // the normal expression completions, not the method/type gate.
    if find_enclosing_route_decl_span(&doc.module, analyzed_offset).is_some()
        && !offset_in_route_rate_limit(&doc.module, analyzed_offset)
    {
        return Some(CompletionResponse::Array(route_completions(doc)));
    }

    let mut items = Vec::new();

    // `*` and `&` are trigger characters but also binary operators (`a * b`,
    // `a && b`). Treat them as source/derived-ref prefixes only in *atom*
    // position — not immediately preceded by an expression-ending byte
    // (identifier char, digit, closing bracket, string quote) or, for `&`,
    // another `&` (the `&&` operator). This mirrors the parser's adjacency
    // rule and stops `x && ` / `a * ` from collapsing the completion popup.
    let trigger_is_operator = offset
        .checked_sub(2)
        .and_then(|i| latest_source.as_bytes().get(i))
        .copied()
        .is_some_and(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'_' | b')' | b']' | b'}' | b'"' | b'&')
        });

    // Context-aware: after `*` only suggest source/view names
    if trigger_char == Some(b'*') {
        // No relation completion inside strings or comments — the trigger
        // character fires on any typed `*` regardless of context.
        if inside_string_or_comment(latest_source, offset.saturating_sub(1)) {
            return None;
        }
        // Only a `*` in atom position is a source-ref prefix; a `*` used as
        // multiplication falls through to general completion (see above).
        if !trigger_is_operator {
            // Mirror the general path's sigil handling (label/insert_text/
            // text_edit over the sigil range) so accepting an item right after
            // the typed `*` replaces the sigil instead of producing `**name`.
            let edit_range = sigil_replace_range(latest_source, offset);
            for decl in &doc.module.decls {
                if let DeclKind::Source { name, .. } | DeclKind::View { name, .. } = &decl.node {
                    let detail = doc.type_info.get(name.as_str()).cloned();
                    items.push(CompletionItem {
                        label: format!("*{name}"),
                        kind: Some(CompletionItemKind::VARIABLE),
                        insert_text: Some(format!("*{name}")),
                        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                            range: edit_range,
                            new_text: format!("*{name}"),
                        })),
                        detail,
                        ..Default::default()
                    });
                }
            }
            return Some(CompletionResponse::Array(items));
        }
    }

    // Context-aware: after `&` only suggest derived names
    if trigger_char == Some(b'&') {
        if inside_string_or_comment(latest_source, offset.saturating_sub(1)) {
            return None;
        }
        // `&&` (or `&` after an expression) is the boolean operator, not a
        // derived-ref prefix — fall through to general completion.
        if !trigger_is_operator {
            // Mirror the general path's sigil handling so accepting an item
            // right after the typed `&` replaces the sigil instead of `&&name`.
            let edit_range = sigil_replace_range(latest_source, offset);
            for decl in &doc.module.decls {
                if let DeclKind::Derived { name, .. } = &decl.node {
                    let detail = doc.type_info.get(name.as_str()).cloned();
                    items.push(CompletionItem {
                        label: format!("&{name}"),
                        kind: Some(CompletionItemKind::VARIABLE),
                        insert_text: Some(format!("&{name}")),
                        text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                            range: edit_range,
                            new_text: format!("&{name}"),
                        })),
                        detail,
                        ..Default::default()
                    });
                }
            }
            return Some(CompletionResponse::Array(items));
        }
    }

    // Context-aware: after `/` in an import line, suggest file paths
    if trigger_char == Some(b'/') {
        // Don't hijack `/` typed inside a string literal or comment — the
        // sibling `*`/`&`/`.` triggers all guard against this, and otherwise a
        // comment like `-- see import a/b` whose trimmed text begins with
        // `import ` would suppress all other completions with path items.
        if inside_string_or_comment(latest_source, offset.saturating_sub(1)) {
            return None;
        }
        let line_start = latest_source[..offset].rfind('\n').map(|p| p + 1).unwrap_or(0);
        let line_text = &latest_source[line_start..offset];
        if line_text.trim_start().starts_with("import ") {
            if let Some(source_path) = uri_to_path(uri)
                && let Some(base_dir) = source_path.parent() {
                    let partial = line_text.trim_start().strip_prefix("import ").unwrap_or("");
                    items.extend(complete_import_path(base_dir, partial));
                }
            return Some(CompletionResponse::Array(items));
        }
    }

    // Context-aware: after `.` suggest record field names from known types
    if trigger_char == Some(b'.') {
        let expr_end = offset.saturating_sub(1); // position of the `.`
        // Context guards: the trigger fires on every typed `.`, including
        // the decimal point of a float literal (`3.`), dots inside string
        // literals, and dots in `--` comments. None of those are field
        // accesses — and the all-known-fields fallback below would happily
        // offer fields there.
        if inside_string_or_comment(latest_source, expr_end)
            || dot_receiver_is_numeric(latest_source, expr_end)
        {
            return None;
        }
        // Try to find the expression before the dot and its type. The receiver
        // name comes from `latest_source` (what the user sees); the type lookup
        // uses the analyzed-source offset so it matches `doc.source` span space.
        let fields = resolve_dot_fields(doc, latest_source, expr_end, analyzed_offset);
        if !fields.is_empty() {
            // If the receiver before the dot is a `Var` bound from a `*source`,
            // attach the source's field refinement (when present) as completion
            // detail/documentation so the user sees the predicate without
            // having to open the source declaration.
            let receiver_var = receiver_ident_before_dot(latest_source, expr_end);
            let owner_source = receiver_var
                .as_deref()
                .and_then(|name| resolve_var_to_source(&doc.module, name));
            for name in fields {
                let mut item = CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::FIELD),
                    ..Default::default()
                };
                if let Some(src) = owner_source.as_deref()
                    && let Some((type_label, predicate)) =
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

    // Context detection: if the cursor is in a type annotation position,
    // only suggest types and type constructors. The trigger characters
    // (`:`, `[`, `->`) are each ambiguous in Knot — `\x -> `, case-arm
    // `->`, record-literal `{name: `, and list literals `[` are all
    // expression positions — so a token scanner over the current
    // declaration decides (see `cursor_in_type_context` for the rules).
    let in_type_context = {
        let before = &latest_source[..offset.min(latest_source.len())];
        let trimmed = before.trim_end();
        (trimmed.ends_with(':') || trimmed.ends_with('[') || trimmed.ends_with("->"))
            && cursor_in_type_context(before)
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
    // `detect_snippet_context` walks `doc.module` spans (indexed against
    // `doc.source`), so resolve the cursor in that same byte space.
    let snippet_ctx = detect_snippet_context(doc, analyzed_offset, in_atomic);
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

    // Replace-range for relation completions (`*name`/`&name`): the typed
    // token INCLUDING any leading sigil. With only a bare `insert_text`,
    // clients insert after the word start — and `*`/`&` aren't word chars,
    // so accepting `*name` after a typed `*` produced `**name`. A text_edit
    // that spans the sigil replaces it instead.
    let sigil_edit_range = sigil_replace_range(latest_source, offset);

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
                    text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                        range: sigil_edit_range,
                        new_text: format!("*{name}"),
                    })),
                    detail: doc.type_info.get(name.as_str()).cloned(),
                    ..Default::default()
                });
            }
            DeclKind::Derived { name, .. } => {
                items.push(CompletionItem {
                    label: format!("&{name}"),
                    kind: Some(CompletionItemKind::VARIABLE),
                    insert_text: Some(format!("&{name}")),
                    text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                        range: sigil_edit_range,
                        new_text: format!("&{name}"),
                    })),
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

    // Imported symbols (selective or wildcard imports): they're in scope, so
    // they belong in the list alongside local declarations. Local decls of
    // the same name shadow the import and were already pushed above.
    for name in doc.import_defs.keys() {
        if doc.definitions.contains_key(name) {
            continue;
        }
        let kind = if name.chars().next().is_some_and(|c| c.is_uppercase()) {
            CompletionItemKind::STRUCT
        } else {
            CompletionItemKind::FUNCTION
        };
        let detail = doc.type_info.get(name.as_str()).cloned().or_else(|| {
            doc.import_origins
                .get(name)
                .map(|origin| format!("imported from {origin}"))
        });
        items.push(CompletionItem {
            label: name.clone(),
            kind: Some(kind),
            detail,
            ..Default::default()
        });
    }

    // Built-in functions with type info. Synthesize a snippet from the
    // arity recorded in `type_info` so users get tab stops on call.
    // A user declaration (or import) of the same name shadows the builtin —
    // offering both produced duplicate items with divergent snippets.
    for name in state_builtins() {
        if doc.definitions.contains_key(name) || doc.import_defs.contains_key(name) {
            continue;
        }
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
        let current_canonical = source_path.as_ref().and_then(|p| p.canonicalize().ok());
        let existing_imports: HashSet<String> = doc.module.imports.iter().map(|i| i.path.clone()).collect();
        // Names already in scope — local declarations AND symbols brought in
        // by existing imports. Suggesting an auto-import for a name the file
        // can already see would add a redundant (or conflicting) import line.
        let local_names: HashSet<&str> = doc
            .definitions
            .keys()
            .map(|s| s.as_str())
            .chain(doc.import_defs.keys().map(|s| s.as_str()))
            .collect();

        // De-dupe by name across files: if two workspace files both export
        // `parse`, prefer the one whose path sorts first. `scan_knot_files_*`
        // pushes files in `read_dir` order, which is filesystem-dependent, so
        // sort first — otherwise which file's import target wins (and the
        // suggested import line) is nondeterministic across runs and machines.
        let mut seen_names: HashSet<String> = HashSet::new();
        let mut auto_imports_added: usize = 0;

        let mut files = scan_knot_files_in_roots(
            &state.workspace_roots,
            state.workspace_root.as_deref(),
        );
        files.sort();
        'files: for file_path in files {
            if auto_imports_added >= MAX_AUTO_IMPORT_ITEMS {
                break;
            }
            let canonical = match file_path.canonicalize() {
                Ok(p) => p,
                Err(_) => continue,
            };
            // Skip current file
            if current_canonical.as_ref() == Some(&canonical) {
                continue;
            }
            // Compute the import path relative to the current file. Use the
            // same `./`-prefixed form the parser stores for imports
            // (`./helpers`, `../shared/x`) — a bare `helpers` would never
            // match `existing_imports` (so already-imported files keep being
            // offered) and produces a parse error when accepted.
            let import_path = match current_canonical.as_ref() {
                Some(cur) => {
                    match crate::code_action::relative_import_path(cur, &canonical) {
                        Some(rel) => rel,
                        None => continue,
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
    // `doc.module` and `doc.monad_info` spans index `doc.source`, so use the
    // analyzed-source offset (not the mid-debounce `offset`).
    if let Some(do_span) = find_enclosing_do_span(&doc.module, analyzed_offset)
        && let Some(monad) = monad_for_do_span(&doc.monad_info, do_span, analyzed_offset) {
            for item in items.iter_mut() {
                let label = item.label.trim_start_matches(['*', '&']);
                if let Some(ty) = doc.type_info.get(label)
                    && type_matches_monad(ty, &monad) {
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

    // Type-aware ranking: when the cursor sits at an argument position in a
    // function call we know the expected parameter type. Push candidates
    // whose return type matches that expectation. This is the same signal
    // signature-help uses to highlight the active parameter — reusing the
    // detection here keeps the two features consistent.
    // `find_enclosing_application` and `lookup_local_binding_type` both walk
    // spans indexed against `doc.source`, so use `analyzed_offset`.
    if let Some((func_name, active_param)) =
        crate::shared::find_enclosing_application(&doc.module, &doc.source, analyzed_offset)
    {
        // Resolve the function's type. Globals first, then locals (let-bound
        // lambdas, do-block binds). Mirrors signature_help's lookup order.
        let func_ty = doc
            .type_info
            .get(func_name.as_str())
            .cloned()
            .or_else(|| lookup_local_binding_type(doc, &func_name, analyzed_offset));
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
            .is_some_and(|s| s.starts_with("aaa_"))
        {
            continue;
        }
        let label = item.label.trim_start_matches(['*', '&']);
        let Some(ty) = doc.type_info.get(label).or(item.detail.as_ref())
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
        // Char-boundary-safe: a stale span could land mid-multibyte-char, and
        // a raw slice there would panic. Mirrors the hardened signature_help
        // twin.
        if crate::utils::safe_slice(&doc.source, *span) != name {
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
    // Imported names rank with locals: both are already in scope.
    let local_names: std::collections::HashSet<&str> = doc
        .definitions
        .keys()
        .map(String::as_str)
        .chain(doc.import_defs.keys().map(String::as_str))
        .collect();
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
        if let Some(eff) = other.effect_sets.get(bare)
            && eff.has_io() {
                return true;
            }
    }
    false
}

/// Return the span of a `route` or `route Foo = ...` declaration that
/// encloses `offset`, or `None` if the cursor is outside any route block.
fn find_enclosing_route_decl_span(module: &Module, offset: usize) -> Option<Span> {
    for decl in &module.decls {
        let in_span = decl.span.start <= offset && offset < decl.span.end;
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

/// True when `offset` sits inside the expression of a `rateLimit <expr>`
/// clause of some route entry. Those expressions are ordinary value-level
/// code and must receive normal expression completions.
fn offset_in_route_rate_limit(module: &Module, offset: usize) -> bool {
    for decl in &module.decls {
        if let DeclKind::Route { entries, .. } = &decl.node {
            for entry in entries {
                if let Some(rl) = &entry.rate_limit
                    && rl.span.start <= offset && offset < rl.span.end {
                        return true;
                    }
            }
        }
    }
    false
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
            if best.is_none_or(|b| size < b.end - b.start) {
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
        let inside = decl.span.start <= offset && offset < decl.span.end;
        if !inside {
            continue;
        }
        match &decl.node {
            ast::DeclKind::Fun { body: Some(body), .. }
            | ast::DeclKind::View { body, .. }
            | ast::DeclKind::Derived { body, .. }
                if body.span.start <= offset && offset < body.span.end => {
                    return SnippetContext::Expression;
                }
            ast::DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item
                        && body.span.start <= offset && offset < body.span.end {
                            return SnippetContext::Expression;
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

/// Return the resolved monad kind for the do-block at `do_span`. The type
/// checker registers monad-vars with spans tied to the desugared
/// `__bind`/`__yield`/`__empty` callsites, which sit inside the original
/// do-block — but NESTED do-blocks contribute entries inside `do_span` too,
/// so an arbitrary `.find()` over the HashMap sampled them
/// nondeterministically. Prefer the INNERMOST entry containing the cursor
/// (that's the block the user is typing in); when no entry covers the
/// cursor, fall back to the first contained entry in deterministic
/// `(start, end)` order.
fn monad_for_do_span(
    monad_info: &HashMap<Span, MonadKind>,
    do_span: Span,
    offset: usize,
) -> Option<MonadKind> {
    let mut contained: Vec<(&Span, &MonadKind)> = monad_info
        .iter()
        .filter(|(s, _)| s.start >= do_span.start && s.end <= do_span.end)
        .collect();
    if let Some((_, kind)) = contained
        .iter()
        .filter(|(s, _)| s.start <= offset && offset <= s.end)
        .min_by_key(|(s, _)| (s.end - s.start, s.start, s.end))
    {
        return Some((*kind).clone());
    }
    contained.sort_by_key(|(s, _)| (s.start, s.end));
    contained.first().map(|(_, k)| (*k).clone())
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
    if params.len() > 1
        && let Some(ret) = params.last() {
            return monad_head_matches(ret.trim(), monad);
        }
    false
}

/// Helper: does the head of a non-function type string sit in the requested
/// monad? Pure-string match — we don't have AST-level info at this point.
fn monad_head_matches(t: &str, monad: &MonadKind) -> bool {
    match monad {
        MonadKind::Relation => {
            // A relation-monad value is a list `[T]` or an IO-wrapped list
            // (relation reads are typed `IO {} [T]`). Inspect the parsed
            // head — substring scans over the full type string would rank
            // functions that merely *take* IO/list parameters as matches.
            if t.starts_with('[') {
                return true;
            }
            if t.starts_with("IO") {
                let parsed = crate::parsed_type::ParsedType::parse(t);
                return matches!(
                    parsed.strip_io(),
                    crate::parsed_type::ParsedType::Relation(_)
                );
            }
            false
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

/// Decide whether a cursor placed immediately after `before` sits in a *type*
/// position. Scans the current top-level declaration (from the last line that
/// starts at column 0) with a small state machine. Rules implemented:
///
/// - `:` enters type position when it is an annotation colon — at the top
///   level of a declaration (`name : …`), inside parens/brackets (postfix
///   annotation `(x : Int)`), or inside a record TYPE / constructor field
///   block (a `{` that itself opened in type position). A `:` inside a
///   record LITERAL (`p = {name: …}`, where the `{` opened in expression
///   position) is the field VALUE position, not a type.
/// - A plain `=` (not `==`/`=>`/`<=`/`>=`/`!=`) leaves type position — the
///   value follows — except in `type X = …` / `data X = …` declarations
///   where the RHS is type-level. The constraint arrow `=>` keeps the
///   current position.
/// - `\` starts a lambda: the next `->` is the lambda's arrow and leaves
///   type position (the body is an expression). Any other `->` keeps the
///   current position, so signature arrows stay type-level while a case
///   arm's `->` (already expression-level after `of`) stays
///   expression-level.
/// - The expression keywords `case`/`of`/`if`/`then`/`else`/`do`/`let`/
///   `yield`/`where` (refinement predicates and serve handlers are
///   expressions) leave type position.
/// - `(`/`[`/`{` push the current position and `)`/`]`/`}` restore it, so
///   `[`/`{` contents inherit the opener's position (element type vs. list
///   literal, record type vs. record literal).
/// - String literals and `--` line comments are skipped.
fn cursor_in_type_context(before: &str) -> bool {
    // Start of the current top-level declaration: the last line whose first
    // character is non-whitespace (Knot decls start at column 0).
    let bytes_all = before.as_bytes();
    let mut decl_start = 0;
    let mut line_start = 0;
    for (i, &c) in bytes_all.iter().enumerate() {
        if c == b'\n' {
            line_start = i + 1;
        } else if i == line_start && c != b' ' && c != b'\t' && c != b'\r' {
            decl_start = line_start;
        }
    }
    let text = &before[decl_start..];
    let head = text.trim_start();
    // In `type`/`data` declarations the RHS of `=` is type-level.
    let eq_introduces_type = head.starts_with("type ") || head.starts_with("data ");

    let is_ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'\'';
    let mut ty = false; // current position is type-level?
    let mut lambda_arrows = 0usize; // `\`s whose `->` hasn't been seen yet
    let mut stack: Vec<(u8, bool)> = Vec::new(); // (bracket, ty at open)
    let mut word_start: Option<usize> = None;
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if is_ident(c) {
            if word_start.is_none() {
                word_start = Some(i);
            }
            i += 1;
            continue;
        }
        if let Some(ws) = word_start.take()
            && matches!(
                &text[ws..i],
                "case" | "of" | "if" | "then" | "else" | "do" | "let" | "yield" | "where"
            ) {
                ty = false;
            }
        match c {
            b'"' => {
                // Skip string literal (with escapes).
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < b.len() && b[i + 1] == b'>' => {
                if lambda_arrows > 0 {
                    lambda_arrows -= 1;
                    ty = false;
                }
                i += 2;
                continue;
            }
            b'-' if i + 1 < b.len() && b[i + 1] == b'-' => {
                // Line comment: skip to end of line.
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'=' if i + 1 < b.len() && b[i + 1] == b'>' => {
                // Constraint arrow: stay in the current position.
                i += 2;
                continue;
            }
            b'=' if i + 1 < b.len() && b[i + 1] == b'=' => {
                i += 2;
                continue;
            }
            b'<' if i + 1 < b.len() && (b[i + 1] == b'=' || b[i + 1] == b'-') => {
                // `<=` comparison / `<-` do-bind.
                i += 2;
                continue;
            }
            b'>' if i + 1 < b.len() && b[i + 1] == b'=' => {
                i += 2;
                continue;
            }
            b'!' if i + 1 < b.len() && b[i + 1] == b'=' => {
                i += 2;
                continue;
            }
            b'=' => {
                ty = eq_introduces_type;
                lambda_arrows = 0;
            }
            b'\\' => {
                lambda_arrows += 1;
                ty = false;
            }
            b'(' | b'[' | b'{' => stack.push((c, ty)),
            b')' | b']' | b'}' => ty = stack.pop().map(|(_, t)| t).unwrap_or(false),
            b':' => {
                ty = match stack.last() {
                    // Inside a record: type only when the record itself is
                    // a record type (constructor fields, annotations).
                    Some((b'{', opened_in_ty)) => *opened_in_ty,
                    // Top level, parens, lists: annotation colon.
                    _ => true,
                };
            }
            _ => {}
        }
        i += 1;
    }
    ty
}

/// Return the bare identifier immediately preceding the dot at `dot_pos`, if
/// the receiver is a simple variable. Used to attach refinement metadata to
/// dot-completion items when the variable was bound from a `*source`.
fn receiver_ident_before_dot(source: &str, dot_pos: usize) -> Option<String> {
    let bytes = source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';
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

/// True when `offset` sits inside a string literal or after a `--` line
/// comment opener. Line-local scan (Knot has no multi-line strings or block
/// comments), tracking `\"` escapes. Used by the trigger-character branches
/// so a `.`/`*`/`&` typed inside a string or comment doesn't pop completion.
fn inside_string_or_comment(source: &str, offset: usize) -> bool {
    // `offset` is usually `cursor.saturating_sub(1)`. When the char ending at
    // the cursor is multibyte (e.g. `é` in a mid-debounce `pending_sources`
    // buffer that the client-reported trigger char disagrees with), that `-1`
    // can land on a UTF-8 continuation byte, so snap down to a char boundary
    // before slicing — otherwise `source[..offset]` panics and crashes the
    // completion request.
    let mut offset = offset.min(source.len());
    while offset > 0 && !source.is_char_boundary(offset) {
        offset -= 1;
    }
    let line_start = source[..offset].rfind('\n').map(|p| p + 1).unwrap_or(0);
    let bytes = source.as_bytes();
    let mut in_str = false;
    let mut i = line_start;
    while i < offset {
        let c = bytes[i];
        if in_str {
            if c == b'\\' {
                i += 2;
                continue;
            }
            if c == b'"' {
                in_str = false;
            }
        } else if c == b'"' {
            in_str = true;
        } else if c == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            return true;
        }
        i += 1;
    }
    in_str
}

/// True when the token immediately before the `.` at `dot_pos` is a numeric
/// literal — i.e. the dot is (part of) a float like `3.`, not a field
/// access. Identifiers can't start with a digit, so a backward ident-char
/// scan whose first char is a digit means "number".
fn dot_receiver_is_numeric(source: &str, dot_pos: usize) -> bool {
    let bytes = source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';
    let mut end = dot_pos.min(bytes.len());
    while end > 0 && bytes[end - 1] == b' ' {
        end -= 1;
    }
    let tok_end = end;
    while end > 0 && is_ident(bytes[end - 1]) {
        end -= 1;
    }
    end < tok_end && bytes[end].is_ascii_digit()
}

/// Try to resolve field names for dot completion by finding the type of the
/// expression before the dot. `source` is the freshest text the client has
/// sent (it may be newer than `doc.source`); `dot_pos` is an offset into it.
/// Scan backwards from `dot_pos` over trailing spaces, then over an
/// identifier, returning its `[start, end)` byte range in `source`.
fn ident_range_before_dot(source: &str, dot_pos: usize) -> Option<(usize, usize)> {
    let bytes = source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';
    let mut end = dot_pos.min(bytes.len());
    while end > 0 && bytes[end - 1] == b' ' {
        end -= 1;
    }
    let ident_end = end;
    while end > 0 && is_ident(bytes[end - 1]) {
        end -= 1;
    }
    if end == ident_end {
        None
    } else {
        Some((end, ident_end))
    }
}

/// `latest_dot_pos` indexes `latest_source` (the freshest, possibly
/// not-yet-analyzed buffer) — used to read the receiver name the user sees.
/// `analyzed_dot_pos` indexes `doc.source` (the last-analyzed text) and is used
/// for the type lookup, because `find_type_for_name` compares against and
/// slices `doc.source`-space spans. Feeding it a `latest_source` offset (the
/// previous bug) made the local-binding lookup miss whenever the two buffers
/// differed in length before the cursor mid-debounce, yielding no/wrong fields.
fn resolve_dot_fields(
    doc: &DocumentState,
    latest_source: &str,
    latest_dot_pos: usize,
    analyzed_dot_pos: usize,
) -> Vec<String> {
    let (start, ident_end) = match ident_range_before_dot(latest_source, latest_dot_pos) {
        Some(r) => r,
        None => return Vec::new(),
    };
    let var_name = &latest_source[start..ident_end];

    // Locate the same receiver in `doc.source` so the type lookup offset lives
    // in the span space `find_type_for_name` expects. `analyzed_dot_pos` is the
    // cursor mapped into `doc.source`: when the analyzed buffer lacks the dot
    // (mid-debounce) it clamps to just past the receiver, but when the dot is
    // already analyzed it sits just past the dot — so try that position and
    // then one byte earlier. Fall back to the latest-source offset if neither
    // finds a receiver (the name guard inside the lookup keeps a stale match
    // safe).
    let lookup_offset = ident_range_before_dot(&doc.source, analyzed_dot_pos)
        .or_else(|| ident_range_before_dot(&doc.source, analyzed_dot_pos.saturating_sub(1)))
        .map(|(s, _)| s)
        .unwrap_or(start);

    // Look up the variable's type
    let type_str = match find_type_for_name(doc, var_name, lookup_offset) {
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
            // Guard that this binding's span actually spells `name`: without
            // it, any unrelated binding whose (possibly larger) span merely
            // contains the receiver position would hand back the wrong type,
            // so `recv.` would offer fields of a different binding's type.
            // Mirrors `lookup_local_binding_type`.
            if span.end <= doc.source.len()
                && span.start <= span.end
                && &doc.source[span.start..span.end] == name
            {
                return Some(ty.clone());
            }
        }
    }
    // Check if any reference at this offset points to a local binding with a known type
    for (usage_span, def_span) in &doc.references {
        if usage_span.start <= offset && offset < usage_span.end
            && let Some(ty) = doc.local_type_info.get(def_span) {
                // Guard that the pointed-at definition is actually named `name`,
                // mirroring the `local_type_info` branch above. Without it a
                // stale or mismatched reference span covering the receiver
                // offset hands back an unrelated binding's type, offering the
                // wrong fields after `.`.
                if def_span.end <= doc.source.len()
                    && def_span.start <= def_span.end
                    && &doc.source[def_span.start..def_span.end] == name
                {
                    return Some(ty.clone());
                }
            }
    }
    // Check global type info
    doc.type_info.get(name).cloned()
}

/// Extract field names from a type string like `{name: Text, age: Int}` or a named type.
fn extract_fields_from_type_str(type_str: &str, module: &Module) -> Vec<String> {
    let mut visited = HashSet::new();
    extract_fields_from_type_str_inner(type_str, module, &mut visited)
}

/// Recursive worker for `extract_fields_from_type_str`. Carries a visited
/// set of alias names so cyclic type aliases (`type A = B` ⏎ `type B = A`)
/// terminate instead of overflowing the stack.
fn extract_fields_from_type_str_inner(
    type_str: &str,
    module: &Module,
    visited: &mut HashSet<String>,
) -> Vec<String> {
    let type_str = type_str.trim();

    // Direct record type: `{name: Text, age: Int}`
    if type_str.starts_with('{') && type_str.ends_with('}') {
        return extract_record_fields(type_str);
    }

    // Relation type: `[{name: Text}]` or `[Person]` — extract inner type
    if type_str.starts_with('[') && type_str.ends_with(']') {
        let inner = &type_str[1..type_str.len() - 1];
        return extract_fields_from_type_str_inner(inner, module, visited);
    }

    // IO type: `IO {...} [T]` or `IO {...} {fields}` — skip to the value type
    if let Some(rest) = type_str.strip_prefix("IO ") {
        // Skip the effect set `{...}` when present.
        if rest.starts_with('{') {
            if let Some(close) = rest.find('}') {
                let value_type = rest[close + 1..].trim();
                return extract_fields_from_type_str_inner(value_type, module, visited);
            }
        } else {
            // No effect row rendered (`IO Person`, `IO [Person]`): the value
            // type is the whole remainder. Without this, field completion on
            // such receivers offered nothing. Mirrors `shared.rs`.
            return extract_fields_from_type_str_inner(rest.trim(), module, visited);
        }
    }

    // Maybe type: `Maybe T` — unwrap to inner type
    if let Some(rest) = type_str.strip_prefix("Maybe ") {
        let inner = rest.trim();
        return extract_fields_from_type_str_inner(inner, module, visited);
    }

    // Named type: look up in the module's declarations. Cycle guard: a name
    // we've already followed resolves to nothing rather than recursing.
    if !visited.insert(type_str.to_string()) {
        return Vec::new();
    }
    for decl in &module.decls {
        match &decl.node {
            DeclKind::TypeAlias { name, ty, .. } if name == type_str => {
                match &ty.node {
                    TypeKind::Record { fields, .. } => {
                        return fields.iter().map(|f| f.name.clone()).collect();
                    }
                    // Follow alias to another named type
                    TypeKind::Named(target) => {
                        return extract_fields_from_type_str_inner(target, module, visited);
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
            DeclKind::Data { name, constructors, .. } if name == type_str
                && constructors.len() == 1 => {
                    return constructors[0].fields.iter().map(|f| f.name.clone()).collect();
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
    if let Some(data) = item.data.clone()
        && data.get("kind").and_then(|v| v.as_str()) == Some("auto_import") {
            let import_path = data.get("import_path").and_then(|v| v.as_str()).unwrap_or("");
            let source_uri = data.get("source_uri").and_then(|v| v.as_str()).unwrap_or("");
            if !import_path.is_empty() && !source_uri.is_empty()
                && let Ok(uri) = source_uri.parse::<Uri>()
                    && let Some(doc) = state.documents.get(&uri) {
                        // The insert position is computed against the analyzed
                        // text; if the buffer has newer pending edits the
                        // position could land mid-edit and corrupt the file.
                        // Skip the lazy import edit in that window — the item
                        // still completes, just without auto-import.
                        let is_stale = state
                            .pending_sources
                            .get(&uri)
                            .is_some_and(|p| p.source != doc.source);
                        if !is_stale {
                            // Shared with the code-action quickfix path:
                            // anchors to the byte offset after the last
                            // import's newline (or EOF with a leading `\n`
                            // when the file lacks a trailing newline), instead
                            // of a possibly-nonexistent `line + 1` position
                            // that clients clamp into the middle of the last
                            // line.
                            let (import_insert_pos, import_line) =
                                crate::code_action::import_insert_position_and_text(
                                    doc,
                                    import_path,
                                );
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
        if detail.is_none()
            && let Some(ty) = doc.type_info.get(label.as_str()) {
                detail = Some(ty.clone());
            }
        if doc_md.is_none()
            && let Some(d) = doc.doc_comments.get(label.as_str()) {
                doc_md = Some(d.clone());
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
                if let ast::TraitItem::Method { name: mn, .. } = item
                    && mn == name {
                        owning_trait = Some(tn.clone());
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
        if let DeclKind::Fun { name: n, ty: Some(scheme), .. } = &decl.node
            && n == name && !scheme.constraints.is_empty() {
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
                    && m == name {
                        let s = body.span;
                        if s.start < s.end && s.end <= source.len() {
                            return Some(source[s.start..s.end].to_string());
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
            // Skip `->` at ANY depth (counting only depth-0 ones) — the `>`
            // of a nested arrow must never reach the bracket logic below, or
            // it would decrement depth and corrupt the count (e.g.
            // `(a -> Bool) -> [a] -> [a]` would compute arity 0).
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'>' => {
                if depth == 0 {
                    count += 1;
                }
                i += 2;
                continue;
            }
            // Same for the constraint arrow `=>` (`Ord a => a -> a -> a`).
            b'=' if i + 1 < bytes.len() && bytes[i + 1] == b'>' => {
                i += 2;
                continue;
            }
            b'(' | b'[' | b'{' | b'<' => depth += 1,
            b')' | b']' | b'}' | b'>' => depth -= 1,
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

    #[test]
    fn inside_string_or_comment_survives_continuation_byte_offset() {
        // `café` is 5 bytes; `é` occupies bytes 3..5. A client-reported `.`
        // trigger against a mid-debounce buffer ending in `é` passes
        // `cursor.saturating_sub(1)` == 4, a UTF-8 continuation byte. The
        // slice must not panic — regression for the completion-request crash.
        let src = "café";
        assert!(!inside_string_or_comment(src, 4));
        // And a non-boundary offset inside a string literal still reports true.
        let src2 = "\"café";
        assert!(inside_string_or_comment(src2, 5));
    }

    fn item_labels(resp: CompletionResponse) -> Vec<String> {
        match resp {
            CompletionResponse::Array(items) => items.into_iter().map(|i| i.label).collect(),
            CompletionResponse::List(list) => list.items.into_iter().map(|i| i.label).collect(),
        }
    }

    fn auto_import_item(uri: &Uri, import_path: &str) -> CompletionItem {
        CompletionItem {
            label: "helper".into(),
            data: Some(serde_json::json!({
                "kind": "auto_import",
                "import_path": import_path,
                "source_uri": uri.as_str(),
            })),
            ..Default::default()
        }
    }

    /// Regression: completion derived its trigger character from the last
    /// *analyzed* text. During the analysis debounce window the just-typed
    /// `.` isn't in that text yet, so dot completion silently degraded to
    /// the generic identifier list. The handler must trust the
    /// client-reported trigger character and resolve the cursor against the
    /// pending (freshest) text.
    #[test]
    fn completion_uses_client_trigger_and_pending_text() {
        use crate::state::PendingSource;
        let mut ws = TestWorkspace::new();
        let analyzed = "*users : [{name: Text, age: Int}]\nmain = do\n  u <- *users\n  yield u";
        let uri = ws.open("main", analyzed);
        // The editor buffer is ahead: the user just typed `.` after the final
        // `u`. The analyzed text doesn't contain the dot yet.
        let pending = format!("{analyzed}.");
        ws.state.pending_sources.insert(
            uri.clone(),
            PendingSource {
                source: pending.clone(),
                version: Some(2),
            },
        );
        let dot_off = pending.len();
        let pos = offset_to_position(&pending, dot_off);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, Some(".")))
            .expect("completion response");
        let labels = item_labels(resp);
        assert!(
            labels.iter().any(|l| l == "name"),
            "dot completion must surface field names; got: {labels:?}"
        );
        assert!(
            !labels.iter().any(|l| l == "do"),
            "dot completion must not fall back to the generic keyword list; got: {labels:?}"
        );
    }

    /// The lazy auto-import edit is computed against the analyzed text; when
    /// newer text is pending the insert position may no longer be valid, so
    /// the resolve path must skip the edit rather than risk corrupting the
    /// buffer.
    #[test]
    fn auto_import_resolve_skips_edit_when_doc_is_stale() {
        use crate::state::PendingSource;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "import ./a\nmain = helper 1\n");
        ws.state.pending_sources.insert(
            uri.clone(),
            PendingSource {
                source: "import ./a\nimport ./b\nmain = helper 1\n".into(),
                version: Some(2),
            },
        );
        let resolved =
            handle_resolve_completion_item(&ws.state, auto_import_item(&uri, "./helpers"));
        assert!(
            resolved.additional_text_edits.is_none(),
            "stale doc must not produce auto-import edits; got: {:?}",
            resolved.additional_text_edits
        );
    }

    /// Regression: the resolve path inserted the auto-import line at
    /// `Position::new(last_import_end.line + 1, 0)`. When the last import is
    /// the final line with no trailing newline, that position doesn't exist
    /// and clients clamp it to end-of-document, gluing the text onto the
    /// previous import (`import ./aimport foo`). The insert must land at EOF
    /// with a leading newline instead.
    #[test]
    fn auto_import_resolve_no_trailing_newline_at_eof() {
        let mut ws = TestWorkspace::new();
        let source = "import ./a"; // final line, no trailing newline
        let uri = ws.open("main", source);
        let resolved =
            handle_resolve_completion_item(&ws.state, auto_import_item(&uri, "./helpers"));
        let edits = resolved
            .additional_text_edits
            .expect("auto-import edit resolved");
        assert_eq!(edits.len(), 1);
        let edit = &edits[0];
        // Insert position must exist in the document (clamping target = EOF).
        let eof = offset_to_position(source, source.len());
        assert_eq!(
            edit.range.start, eof,
            "insert position must be end-of-document, not a nonexistent line"
        );
        assert_eq!(
            edit.new_text, "\nimport ./helpers\n",
            "a leading newline must separate the new import from the last line"
        );
        // Applying the edit yields well-formed imports, not glued text.
        let mut applied = source.to_string();
        applied.insert_str(source.len(), &edit.new_text);
        assert!(applied.contains("import ./a\nimport ./helpers\n"), "got: {applied:?}");
    }

    /// The common case (imports followed by more lines) still inserts right
    /// after the last import line.
    #[test]
    fn auto_import_resolve_inserts_after_last_import_line() {
        let mut ws = TestWorkspace::new();
        let source = "import ./a\n\nmain = 1\n";
        let uri = ws.open("main", source);
        let resolved =
            handle_resolve_completion_item(&ws.state, auto_import_item(&uri, "./helpers"));
        let edits = resolved
            .additional_text_edits
            .expect("auto-import edit resolved");
        assert_eq!(edits[0].range.start, Position::new(1, 0));
        assert_eq!(edits[0].new_text, "import ./helpers\n");
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
        // Labels carry the sigil (matching the general completion path) and the
        // items replace the typed `*` via a text_edit so accepting one yields
        // `*people`, not `**people`.
        assert!(labels.contains(&"*people".to_string()), "labels: {labels:?}");
        assert!(labels.contains(&"*pets".to_string()), "labels: {labels:?}");
    }

    #[test]
    fn completion_after_double_amp_operator_does_not_collapse() {
        // Typing the second `&` of `&&` fires a `&` trigger, but the popup
        // must NOT collapse to derived-relation names only — `flag` (a param)
        // should still be offered.
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"&active = []
check = \flag -> flag &&
"#,
        );
        let doc = ws.doc(&uri);
        let off = doc.source.find("flag &&").expect("trigger") + "flag &&".len();
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, Some("&")))
            .expect("completion returns");
        let labels = item_labels(resp);
        // Falls through to general completion instead of collapsing to the
        // derived-only list: general items like builtins are present, and the
        // set is not just the single derived name `active`.
        assert!(labels.contains(&"count".to_string()), "labels: {labels:?}");
        assert!(labels.len() > 1, "should not collapse to one item: {labels:?}");
    }

    #[test]
    fn completion_after_mul_operator_does_not_collapse() {
        // `n *` (mul, `*` adjacent to nothing but preceded by an expression):
        // the `n*` form must fall through, not force source-name completion.
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"*people : [{n: Int}]
calc = \n -> n*
"#,
        );
        let doc = ws.doc(&uri);
        let off = doc.source.find("n*").expect("trigger") + "n*".len();
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, Some("*")))
            .expect("completion returns");
        // Falls through to general completion rather than the source-only list:
        // general items are present (a `*` in operator position is not a
        // source-ref prefix).
        let labels = item_labels(resp);
        assert!(labels.contains(&"count".to_string()), "labels: {labels:?}");
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

// Regression tests for the 2026-06 LSP bug-fix batch (completion group).
#[cfg(test)]
mod regress_fixes_tests {
    use super::*;

    #[test]
    fn fuzz_helpers_no_panic() {
        // Directly probe byte-offset helpers with every offset including
        // non-char-boundaries and out-of-range, on multibyte strings.
        let strings = [
            "café.field", "3.5", "é.", ".x", "x'.", "  foo  .", "\"a.b\"",
            "-- a.b", "IO {fs} café", "usér.ok", "𝟛.5", "a\r\nb.", "",
            "\u{200b}.x", "n😀.", "  ", "\t\tx.",
        ];
        for s in strings {
            for off in 0..=s.len() {
                let _ = super::inside_string_or_comment(s, off);
                let _ = super::dot_receiver_is_numeric(s, off);
                let _ = super::receiver_ident_before_dot(s, off);
                let _ = super::ident_range_before_dot(s, off);
                let idx = off.min(s.len());
                if s.is_char_boundary(idx) {
                    let _ = super::cursor_in_type_context(&s[..idx]);
                }
            }
            let _ = super::arrow_arity(s);
            let _ = super::type_matches_monad(s, &MonadKind::Relation);
            let _ = super::type_matches_monad(s, &MonadKind::IO);
            let _ = super::type_matches_monad(s, &MonadKind::Adt("Maybe".into()));
            let _ = super::monad_head_matches(s, &MonadKind::Relation);
        }
    }

    #[test]
    fn fuzz_no_panic() {
        use crate::test_support::TestWorkspace;
        use crate::utils::offset_to_position;
        let sources = [
            "*usérs : [{náme: Text, age: Int}]\nmain = do\n  u <- *usérs\n  yield u",
            "type P = {café: Text}\nx = 3.5\nmain = x.",
            "-- see notés.\nmain = 1",
            "main = println \"a*b—é\"\nfoo : Int",
            "type X = {n: Text}\nf = \\x -> atomic do\n  yield {}\n",
            "route Hello where\n  GET /hí -> Text\n",
            "import ./café\nmain = 1\n",
            "x = \"\\\"é\nfoo :",
            "🎉 = 1\nmain = 🎉.\n",
            "f : {name: \ng = {a: 1}.",
            "*t : [{x: Int}]\nmain = *té\n",
            "&d = do yield {}\nmain = &dé\n",
            "s = \"café\nmain = s.",
            "Ünïcöde = 1\nmain = Ünïcöde",
            "a = 1\r\nb = 2\r\nmain = a.\r\n",
        ];
        let valid = [
            "*people : [{name: Text, age: Int}]\nmain = do\n  p <- *people\n  yield p.name\n",
            "type Person = {name: Text, age: Int}\ngreet = \\p -> p.name\n",
            "*t : [{x: Int}]\nf = \\r -> r.x\nmain = f\n",
            "u = {a: 1, b: 2}\nmain = u.a\n",
        ];
        let triggers = [None, Some("."), Some("*"), Some("&"), Some("/"), Some(":")];
        for src in sources.iter().copied().chain(valid.iter().copied()) {
            let mut ws = TestWorkspace::new();
            let uri = ws.open("main", src);
            // Exercise mid-debounce / latest_source paths: pending source may
            // be longer OR shorter than the analyzed text, shifting offsets.
            let shorter = {
                let mut idx = src.len().saturating_sub(2);
                while idx > 0 && !src.is_char_boundary(idx) { idx -= 1; }
                if idx > 0 { Some(src[..idx].to_string()) } else { None }
            };
            for pend in [
                None,
                Some(format!("{src}.")),
                Some(format!("{src}é")),
                Some(format!("{src}é.")),
                shorter,
            ] {
                if let Some(p) = pend {
                    ws.state.pending_sources.insert(
                        uri.clone(),
                        crate::state::PendingSource { source: p, version: Some(2) },
                    );
                } else {
                    ws.state.pending_sources.remove(&uri);
                }
                let latest = ws
                    .state
                    .pending_sources
                    .get(&uri)
                    .map(|p| p.source.clone())
                    .unwrap_or_else(|| src.to_string());
                for off in 0..=latest.len() {
                    if !latest.is_char_boundary(off) {
                        continue;
                    }
                    let pos = offset_to_position(&latest, off);
                    for trig in triggers {
                        let _ = handle_completion(&ws.state, &comp_params(&uri, pos, trig));
                    }
                    // Also try positions one past line ends etc via raw pos.
                    let big = Position::new(pos.line, pos.character + 3);
                    for trig in triggers {
                        let _ = handle_completion(&ws.state, &comp_params(&uri, big, trig));
                    }
                }
            }
        }
    }

    /// Item 8: `->` must be skipped at any depth so its `>` never reaches
    /// the bracket-depth logic.
    #[test]
    fn arrow_arity_handles_parenthesized_function_params() {
        assert_eq!(arrow_arity("(a -> Bool) -> [a] -> [a]"), 2);
        assert_eq!(arrow_arity("(a -> b) -> [a] -> [b]"), 2);
        assert_eq!(arrow_arity("(b -> a -> b) -> b -> [a] -> b"), 3);
        assert_eq!(arrow_arity("Int -> Text"), 1);
        assert_eq!(arrow_arity("Int"), 0);
        // Constraint arrows don't corrupt the count either.
        assert_eq!(arrow_arity("Ord a => a -> a -> a"), 2);
        // Units and effect rows unaffected.
        assert_eq!(arrow_arity("Int<Ms> -> IO {clock} {}"), 1);
    }

    /// Item 9: type-context detection must not fire after lambda arrows,
    /// case-arm arrows, record-literal field colons, or list literals.
    #[test]
    fn type_context_rules() {
        // Type positions.
        assert!(cursor_in_type_context("f : "));
        assert!(cursor_in_type_context("f : Int -> "));
        assert!(cursor_in_type_context("f : ["));
        assert!(cursor_in_type_context("f : {name: "));
        assert!(cursor_in_type_context("type X = "));
        assert!(cursor_in_type_context("data Shape = Circle {radius: "));
        assert!(cursor_in_type_context("g = (x : "));
        assert!(cursor_in_type_context("f : Display a => a -> "));
        assert!(cursor_in_type_context("source users : [{name: "));
        // Expression positions.
        assert!(!cursor_in_type_context("f = \\x -> "));
        assert!(!cursor_in_type_context("f = case x of\n  Red {} -> "));
        assert!(!cursor_in_type_context("p = {name: "));
        assert!(!cursor_in_type_context("xs = ["));
        assert!(!cursor_in_type_context("f : Int -> Int = \\x -> "));
        // A signature arrow followed by a lambda body arrow: the lambda wins.
        assert!(!cursor_in_type_context("nums = do\n  n <- *numbers\n  let y = "));
    }

    /// Item 10: relation-monad ranking must inspect the head of the type,
    /// not substring-scan the whole string.
    #[test]
    fn monad_head_matches_relation_inspects_head() {
        let rel = MonadKind::Relation;
        assert!(monad_head_matches("[Int]", &rel));
        assert!(monad_head_matches("IO {} [Int]", &rel));
        assert!(monad_head_matches("IO {r *users} [{name: Text}]", &rel));
        // Not relations: plain IO, and types merely mentioning IO / lists.
        assert!(!monad_head_matches("IO {} Int", &rel));
        assert!(!monad_head_matches("Int -> IO {} Int", &rel));
        assert!(!monad_head_matches("Map Text [Int]", &rel));
        assert!(!monad_head_matches("Text", &rel));
    }

    fn parse_module_src(src: &str) -> Module {
        let lexer = knot::lexer::Lexer::new(src);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(src.to_string(), tokens);
        parser.parse_module().0
    }

    /// Cyclic type aliases must not overflow the stack during
    /// dot-completion field extraction.
    #[test]
    fn extract_fields_terminates_on_cyclic_type_aliases() {
        // Previously: infinite mutual recursion A → B → A → … killing the
        // whole LSP process. Now: terminates with no fields.
        let module = parse_module_src("type A = B\ntype B = A\nx = 1\n");
        assert!(extract_fields_from_type_str("A", &module).is_empty());
        assert!(extract_fields_from_type_str("B", &module).is_empty());
        // Self-cycle too.
        let module2 = parse_module_src("type C = C\n");
        assert!(extract_fields_from_type_str("C", &module2).is_empty());
    }

    /// Non-cyclic alias chains still resolve through the guard.
    #[test]
    fn extract_fields_follows_acyclic_alias_chain() {
        let module = parse_module_src("type Person = {name: Text}\ntype Alias = Person\n");
        assert_eq!(extract_fields_from_type_str("Alias", &module), vec!["name"]);
    }

    /// Monad-aware ranking must sample the do-block deterministically:
    /// the INNERMOST monad_info span containing the cursor wins; without a
    /// containing span, the first contained entry in (start, end) order.
    #[test]
    fn monad_for_do_span_prefers_innermost_containing_cursor() {
        let mut info: HashMap<Span, MonadKind> = HashMap::new();
        let do_span = Span::new(0, 100);
        // Outer block's callsite entry.
        info.insert(Span::new(5, 90), MonadKind::Relation);
        // Nested do-block's entry.
        info.insert(Span::new(40, 60), MonadKind::IO);
        // Cursor inside the nested block → IO.
        assert!(matches!(
            monad_for_do_span(&info, do_span, 50),
            Some(MonadKind::IO)
        ));
        // Cursor inside the outer block only → Relation.
        assert!(matches!(
            monad_for_do_span(&info, do_span, 10),
            Some(MonadKind::Relation)
        ));
        // Cursor outside both → deterministic fallback (first by start).
        assert!(matches!(
            monad_for_do_span(&info, do_span, 95),
            Some(MonadKind::Relation)
        ));
    }

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

    fn resp_items(resp: CompletionResponse) -> Vec<CompletionItem> {
        match resp {
            CompletionResponse::Array(items) => items,
            CompletionResponse::List(list) => list.items,
        }
    }

    /// Bug 15: a user declaration shadows the same-named builtin — offering
    /// both produced two `map` items with divergent snippets.
    #[test]
    fn builtin_shadowed_by_user_declaration_is_not_duplicated() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "map = \\x -> x\nmain = map 1\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("map 1").expect("usage");
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, None))
            .expect("completion returns");
        let items = resp_items(resp);
        let map_items: Vec<&CompletionItem> = items
            .iter()
            .filter(|i| i.label == "map" && i.kind != Some(CompletionItemKind::SNIPPET))
            .collect();
        assert_eq!(
            map_items.len(),
            1,
            "user `map` must shadow the builtin; got: {map_items:?}"
        );
    }

    /// Bug 12: imported symbols appear in normal completion, and the
    /// auto-import path must not re-suggest names already in scope.
    #[test]
    fn imported_symbols_complete_without_auto_import_duplicate() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        std::fs::write(tw.root.join("lib.knot"), "helper = \\x -> x\n").unwrap();
        let uri = tw.write_and_open("main.knot", "import ./lib\n\nmain = helper 1\n");
        let doc = tw.workspace.doc(&uri);
        let off = doc.source.find("helper 1").expect("usage");
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_completion(&tw.workspace.state, &comp_params(&uri, pos, None))
            .expect("completion returns");
        let items = resp_items(resp);
        assert!(
            items
                .iter()
                .any(|i| i.label == "helper" && i.data.is_none()),
            "imported `helper` must surface as a plain completion item"
        );
        let dup_auto_import = items.iter().any(|i| {
            i.label == "helper"
                && i.data
                    .as_ref()
                    .and_then(|d| d.get("kind"))
                    .and_then(|k| k.as_str())
                    == Some("auto_import")
        });
        assert!(
            !dup_auto_import,
            "auto-import must not re-suggest a symbol already imported"
        );
    }

    /// Bug 14: the `.` trigger inside a float literal is a decimal point —
    /// no field completion (the all-known-fields fallback used to fire).
    #[test]
    fn dot_trigger_suppressed_inside_float_literal() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "type P = {name: Text}\nx = 3.5\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("3.").expect("float") + 2;
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, Some(".")));
        assert!(resp.is_none(), "no field completion inside a float: {resp:?}");
    }

    /// Bug 14: `.` typed inside a `--` comment must not pop completion.
    #[test]
    fn dot_trigger_suppressed_inside_comment() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "-- see notes.\nmain = 1\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("notes.").expect("comment dot") + "notes.".len();
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, Some(".")));
        assert!(resp.is_none(), "no completion inside a comment: {resp:?}");
    }

    /// Bug 14: `*` typed inside a string literal must not pop relation
    /// completion.
    #[test]
    fn star_trigger_suppressed_inside_string() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "*todos : [{t: Text}]\nmain = println \"a*b\"\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("a*").expect("string star") + 2;
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, Some("*")));
        assert!(resp.is_none(), "no relation completion inside a string: {resp:?}");
    }

    /// Bug 16: relation items must carry a text_edit that REPLACES the
    /// typed token including its sigil — bare insert_text after a typed `*`
    /// produced `**name`.
    #[test]
    fn relation_completion_text_edit_replaces_typed_sigil() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "*todos : [{t: Text}]\nmain = *to\n");
        let doc = ws.doc(&uri);
        // Target the `*to` in `main = *to` (end-of-token), not the line-1
        // `*todos` declaration that `find` would match first.
        let sigil_off = doc.source.rfind("*to").expect("typed prefix");
        let cursor = sigil_off + 3;
        let pos = offset_to_position(&doc.source, cursor);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, None))
            .expect("completion returns");
        let items = resp_items(resp);
        let item = items
            .iter()
            .find(|i| i.label == "*todos")
            .expect("relation item offered");
        let Some(CompletionTextEdit::Edit(edit)) = &item.text_edit else {
            panic!("relation item must carry a replacing text_edit: {item:?}");
        };
        assert_eq!(
            edit.range.start,
            offset_to_position(&doc.source, sigil_off),
            "edit must start AT the sigil so it gets replaced"
        );
        assert_eq!(edit.range.end, pos);
        assert_eq!(edit.new_text, "*todos");
    }

    /// Bug 2: completing mid-token must replace the WHOLE token, not just up
    /// to the caret — otherwise accepting `*todos` at `*to|dos` left the `dos`
    /// suffix behind, producing `*todosdos`.
    #[test]
    fn relation_completion_text_edit_replaces_whole_token_midword() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "*todos : [{t: Text}]\nmain = *todos\n");
        let doc = ws.doc(&uri);
        // Caret in the middle of the `*todos` usage on line 2: `*to|dos`.
        let usage = doc.source.rfind("*todos").expect("usage");
        let caret = usage + 3;
        let pos = offset_to_position(&doc.source, caret);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, None))
            .expect("completion returns");
        let item = resp_items(resp)
            .into_iter()
            .find(|i| i.label == "*todos")
            .expect("relation item offered");
        let Some(CompletionTextEdit::Edit(edit)) = &item.text_edit else {
            panic!("relation item must carry a replacing text_edit: {item:?}");
        };
        assert_eq!(
            edit.range.start,
            offset_to_position(&doc.source, usage),
            "edit starts at the sigil"
        );
        assert_eq!(
            edit.range.end,
            offset_to_position(&doc.source, usage + "*todos".len()),
            "edit must extend past the caret to the end of the token"
        );
        assert_eq!(edit.new_text, "*todos");
    }

    /// Bug 4 consequence: completion stops offering IO builtins inside
    /// atomic blocks of PARAMETERIZED functions (the lambda-skip in the
    /// atomic walker made `in_atomic` always false there).
    #[test]
    fn completion_filters_io_builtins_in_atomic_block_of_parameterized_fn() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "type P = {n: Text}\n*people : [P]\nf = \\x -> atomic do\n  *people = [{n: \"A\"}]\n  yield {}\n",
        );
        let doc = ws.doc(&uri);
        let inside = doc.source.find("[{n:").expect("atomic body");
        let pos = offset_to_position(&doc.source, inside);
        let resp = handle_completion(&ws.state, &comp_params(&uri, pos, None))
            .expect("completion returns");
        let labels: Vec<String> = resp_items(resp).into_iter().map(|i| i.label).collect();
        assert!(
            !labels.contains(&"println".to_string()),
            "println leaked into atomic completion of a parameterized fn"
        );
    }

    /// `rateLimit <expr>` clauses inside route declarations hold ordinary
    /// expressions — the route-block completion gate must not swallow them.
    #[test]
    fn rate_limit_expression_gets_normal_expression_completions() {
        use crate::test_support::TestWorkspace;
        let mut ws = TestWorkspace::new();
        let src = "route Api where\n  GET /things -> Text rateLimit {key: \\i -> \\c -> Nothing, limit: {requests: 10, window: 1000}} = GetThings\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        // Sanity: the parser recorded the rateLimit expression.
        let has_rl = doc.module.decls.iter().any(|d| {
            matches!(&d.node, DeclKind::Route { entries, .. }
                if entries.iter().any(|e| e.rate_limit.is_some()))
        });
        assert!(has_rl, "parser should record the rateLimit clause");
        let off = doc.source.find("\\i ->").expect("rateLimit lambda");
        assert!(
            offset_in_route_rate_limit(&doc.module, off),
            "cursor inside the rateLimit expression must be detected"
        );
        let pos = crate::utils::offset_to_position(&doc.source, off);
        let params = CompletionParams {
            text_document_position: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position: pos,
            },
            context: None,
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        };
        let resp = handle_completion(&ws.state, &params).expect("completion response");
        let items = match resp {
            CompletionResponse::Array(items) => items,
            CompletionResponse::List(l) => l.items,
        };
        // Expression completions include builtins like `show`; the gated
        // route list contains only methods/types/headers.
        assert!(
            items.iter().any(|i| i.label == "show"),
            "expected expression completions inside rateLimit; got {} items: {:?}",
            items.len(),
            items.iter().map(|i| &i.label).take(20).collect::<Vec<_>>()
        );
    }
}
