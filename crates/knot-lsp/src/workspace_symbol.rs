//! `workspace/symbol` handler. Returns a flat list of symbol matches across
//! the workspace, with caching to avoid re-parsing closed files.

use std::collections::HashSet;
use std::path::PathBuf;

use lsp_types::*;

use knot::ast::{DeclKind, Module};

use crate::analysis::get_or_parse_file_shared;
use crate::shared::scan_knot_files_in_roots;
use crate::state::{content_hash, ServerState, WorkspaceSymbolEntry};
use crate::type_format::format_type_kind;
use crate::utils::{path_to_uri, span_to_range, uri_to_path};

// ── Workspace Symbols ───────────────────────────────────────────────

/// Build the cacheable list of symbol entries for a parsed module. Path-keyed,
/// so the same vector can be reused across queries until the file's content
/// hash changes. Returns entries with absolute file URIs already resolved.
pub(crate) fn build_workspace_symbol_entries(
    module: &Module,
    source: &str,
    uri: &Uri,
) -> Vec<WorkspaceSymbolEntry> {
    let mut out = Vec::new();
    for decl in &module.decls {
        let (name, kind, container) = match &decl.node {
            DeclKind::Data { name, .. } => (name.clone(), SymbolKind::STRUCT, None),
            DeclKind::TypeAlias { name, ty, .. } => {
                // Refined types: surface `where` predicate in the container so
                // the workspace symbol picker shows it inline.
                let container = match &ty.node {
                    knot::ast::TypeKind::Refined { predicate, .. } => source
                        .get(predicate.span.start..predicate.span.end)
                        .map(|s| format!("refined where {}", s.trim())),
                    _ => None,
                };
                (name.clone(), SymbolKind::TYPE_PARAMETER, container)
            }
            DeclKind::Source { name, .. } => (format!("*{name}"), SymbolKind::VARIABLE, None),
            DeclKind::View { name, .. } => (format!("*{name}"), SymbolKind::VARIABLE, None),
            DeclKind::Derived { name, .. } => (format!("&{name}"), SymbolKind::VARIABLE, None),
            DeclKind::Fun { name, .. } => (name.clone(), SymbolKind::FUNCTION, None),
            DeclKind::Trait { name, .. } => (name.clone(), SymbolKind::INTERFACE, None),
            DeclKind::Impl {
                trait_name, args, ..
            } => {
                let args_str = args
                    .iter()
                    .map(|a| format_type_kind(&a.node))
                    .collect::<Vec<_>>()
                    .join(" ");
                (
                    format!("impl {trait_name} {args_str}"),
                    SymbolKind::OBJECT,
                    None,
                )
            }
            DeclKind::Route { name, .. } | DeclKind::RouteComposite { name, .. } => {
                (format!("route {name}"), SymbolKind::MODULE, None)
            }
            _ => continue,
        };
        out.push(WorkspaceSymbolEntry {
            name,
            kind,
            uri: uri.clone(),
            range: span_to_range(decl.span, source),
            container,
        });
    }
    out
}

#[allow(deprecated)]
pub(crate) fn handle_workspace_symbol(
    state: &mut ServerState,
    params: &WorkspaceSymbolParams,
) -> Option<Vec<SymbolInformation>> {
    let query = params.query.to_lowercase();
    let mut symbols: Vec<SymbolInformation> = Vec::new();
    let mut seen_paths: HashSet<PathBuf> = HashSet::new();

    let push_matching = |entries: &[WorkspaceSymbolEntry],
                         query: &str,
                         out: &mut Vec<SymbolInformation>| {
        for e in entries {
            if !query.is_empty() && !e.name.to_lowercase().contains(query) {
                continue;
            }
            out.push(SymbolInformation {
                name: e.name.clone(),
                kind: e.kind,
                tags: None,
                deprecated: None,
                location: Location {
                    uri: e.uri.clone(),
                    range: e.range,
                },
                container_name: e.container.clone(),
            });
        }
    };

    // Phase 1: collect from open documents. Always recompute (the user may be
    // mid-edit), and refresh the cache for that path so that the next time
    // the file is closed we have a fresh entry. We deliberately do NOT stamp
    // an mtime here — the buffer in the editor may be ahead of the on-disk
    // file, so any cached mtime would falsely match a stale on-disk version.
    let open_entries: Vec<(PathBuf, u64, Vec<WorkspaceSymbolEntry>)> = state
        .documents
        .iter()
        .filter_map(|(uri, doc)| {
            let path = uri_to_path(uri)?;
            let canonical = path.canonicalize().ok()?;
            seen_paths.insert(canonical.clone());
            let entries = build_workspace_symbol_entries(&doc.module, &doc.source, uri);
            push_matching(&entries, &query, &mut symbols);
            Some((canonical, content_hash(&doc.source), entries))
        })
        .collect();
    if let Ok(mut cache) = state.workspace_symbol_cache.lock() {
        for (path, hash, entries) in open_entries {
            cache.by_path.insert(path, (None, hash, entries));
        }
    }

    // Phase 2: closed workspace files. Use the cache when the on-disk hash
    // matches; otherwise re-parse and update the cache.
    {
        let entries = scan_knot_files_in_roots(
            &state.workspace_roots,
            state.workspace_root.as_deref(),
        );
        if !entries.is_empty() {
            // Keep only paths that still exist on disk to avoid the cache
            // ballooning over time.
            let on_disk: HashSet<PathBuf> = entries
                .iter()
                .filter_map(|p| p.canonicalize().ok())
                .collect();
            if let Ok(mut cache) = state.workspace_symbol_cache.lock() {
                cache.by_path.retain(|path, _| on_disk.contains(path));
            }

            for path in entries {
                let canonical = match path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if seen_paths.contains(&canonical) {
                    continue;
                }

                // Mtime fast path: if the on-disk mtime matches what we cached
                // last time, the file content can't have changed, so skip the
                // read+hash entirely.
                let on_disk_mtime = std::fs::metadata(&canonical)
                    .ok()
                    .and_then(|m| m.modified().ok());
                if let Some(disk_mtime) = on_disk_mtime {
                    let cached_hit = state
                        .workspace_symbol_cache
                        .lock()
                        .ok()
                        .and_then(|c| match c.by_path.get(&canonical) {
                            Some((Some(cached_mtime), _, cached_entries))
                                if *cached_mtime == disk_mtime =>
                            {
                                Some(cached_entries.clone())
                            }
                            _ => None,
                        });
                    if let Some(entries) = cached_hit {
                        push_matching(&entries, &query, &mut symbols);
                        continue;
                    }
                }

                // Mtime moved or wasn't recorded — read+hash. A content match
                // refreshes the mtime so the fast path applies next time.
                let source = match std::fs::read_to_string(&canonical) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let hash = content_hash(&source);

                let cached_hit_by_hash = {
                    let mut cache = match state.workspace_symbol_cache.lock() {
                        Ok(c) => c,
                        Err(_) => continue,
                    };
                    match cache.by_path.get_mut(&canonical) {
                        Some((mtime_slot, cached_hash, cached_entries)) if *cached_hash == hash => {
                            if let Some(disk_mtime) = on_disk_mtime {
                                *mtime_slot = Some(disk_mtime);
                            }
                            Some(cached_entries.clone())
                        }
                        _ => None,
                    }
                };
                if let Some(entries) = cached_hit_by_hash {
                    push_matching(&entries, &query, &mut symbols);
                    continue;
                }

                // Stale or missing — reparse and refresh the cache.
                let (module, _) = match get_or_parse_file_shared(&canonical, &state.import_cache) {
                    Some(v) => v,
                    None => continue,
                };
                let uri = match path_to_uri(&canonical) {
                    Some(u) => u,
                    None => continue,
                };
                let entries = build_workspace_symbol_entries(&module, &source, &uri);
                push_matching(&entries, &query, &mut symbols);
                if let Ok(mut cache) = state.workspace_symbol_cache.lock() {
                    cache
                        .by_path
                        .insert(canonical, (on_disk_mtime, hash, entries));
                }
            }
        }
    }

    if symbols.is_empty() {
        None
    } else {
        Some(symbols)
    }
}
