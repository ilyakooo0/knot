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
    format_route_constructor_hover, predicate_to_source, scan_knot_files,
};
use crate::state::{
    builtins as state_builtins, DocumentState, ServerState, KEYWORDS, SNIPPETS,
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
            for name in fields {
                items.push(CompletionItem {
                    label: name,
                    kind: Some(CompletionItemKind::FIELD),
                    ..Default::default()
                });
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
                DeclKind::Data { name, .. } | DeclKind::TypeAlias { name, .. } => {
                    items.push(CompletionItem {
                        label: name.clone(),
                        kind: Some(CompletionItemKind::STRUCT),
                        detail: doc.details.get(name).cloned(),
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

    // Snippet completions for common patterns
    for (label, detail, snippet) in SNIPPETS {
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
    if let Some(root) = &state.workspace_root {
        let source_path = uri_to_path(uri);
        let existing_imports: HashSet<String> = doc.module.imports.iter().map(|i| i.path.clone()).collect();
        let local_names: HashSet<&str> = doc.definitions.keys().map(|s| s.as_str()).collect();

        // De-dupe by name across files: if two workspace files both export
        // `parse`, prefer the one with the lexicographically-shortest path.
        let mut seen_names: HashSet<String> = HashSet::new();

        if let Ok(files) = scan_knot_files(root) {
            for file_path in files {
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

                    // Compute where to insert the import line
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

                    let additional_edits = vec![TextEdit {
                        range: Range {
                            start: import_insert_pos,
                            end: import_insert_pos,
                        },
                        new_text: import_line,
                    }];

                    items.push(CompletionItem {
                        label: name.clone(),
                        kind: Some(kind),
                        detail: Some(format!("auto-import from {import_path}")),
                        additional_text_edits: Some(additional_edits),
                        sort_text: Some(format!("zz_{name}")), // sort after local items
                        ..Default::default()
                    });
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

    if in_atomic {
        items.retain(|item| !is_disallowed_in_atomic(&item.label, doc));
    }

    Some(CompletionResponse::Array(items))
}

/// True if a completion candidate would be rejected by the effect checker
/// inside an `atomic` block. Mirrors the rule in `effects.rs`: any builtin
/// from `ATOMIC_DISALLOWED_BUILTINS`, plus any user function whose inferred
/// effect set contains console/network/fs/clock/random.
fn is_disallowed_in_atomic(label: &str, doc: &DocumentState) -> bool {
    if ATOMIC_DISALLOWED_BUILTINS.contains(&label) {
        return true;
    }
    if let Some(eff) = doc.effect_sets.get(label) {
        return eff.has_io();
    }
    false
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
/// type checking.
fn type_matches_monad(ty: &str, monad: &MonadKind) -> bool {
    let t = ty.trim();
    match monad {
        MonadKind::Relation => {
            // Direct relation `[T]`, or the IO-wrapped variant `IO {} [T]`
            // returned by `*src` / `&derived` / `set` / etc.
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
    fn completion_filters_io_builtins_in_atomic_block() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"type P = {n: Text}
*people : [P]
main = atomic do
  set *people = [{n: "A"}]
  yield {}
"#,
        );
        let doc = ws.doc(&uri);
        // Cursor inside the atomic body, on the line after `set *people = ...`
        let inside = doc.source.find("set *people").expect("set");
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
