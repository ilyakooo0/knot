// ── Module declarations ─────────────────────────────────────────────

mod analysis;
mod builtins;
mod call_hierarchy;
mod code_action;
mod code_lens;
mod completion;
mod defs;
mod diagnostics;
mod document_highlight;
mod document_link;
mod document_symbol;
mod folding;
mod formatting;
mod goto;
mod hover;
mod incremental;
mod inlay_hints;
mod legend;
mod linked_editing;
mod parsed_type;
mod references;
mod rename;
mod selection_range;
mod semantic_tokens;
mod shared;
mod signature_help;
mod state;
mod type_hierarchy;
#[cfg(test)]
mod test_support;
mod type_format;
mod utils;
mod workspace_diagnostics;
mod workspace_symbol;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam_channel::select;
use lsp_server::{Connection, Message, Notification, Request, RequestId};
use lsp_types::notification::Notification as _;
use lsp_types::request::Request as _;
use lsp_types::*;

use crate::analysis::{analysis_worker, panic_message};
use crate::call_hierarchy::{
    handle_call_hierarchy_incoming, handle_call_hierarchy_outgoing, handle_call_hierarchy_prepare,
};
use crate::code_action::handle_code_action;
use crate::code_lens::handle_code_lens;
use crate::completion::{handle_completion, handle_resolve_completion_item};
use crate::document_highlight::handle_document_highlight;
use crate::document_link::handle_document_link;
use crate::document_symbol::handle_document_symbol;
use crate::folding::handle_folding_range;
use crate::formatting::{handle_formatting, handle_on_type_formatting, handle_range_formatting};
use crate::goto::{
    handle_goto_definition, handle_goto_implementation, handle_goto_type_definition,
};
use crate::hover::handle_hover;
use crate::inlay_hints::handle_inlay_hint;
use crate::legend::semantic_token_legend;
use crate::linked_editing::handle_linked_editing_range;
use crate::references::handle_references;
use crate::rename::{handle_prepare_rename, handle_rename};
use crate::selection_range::handle_selection_range;
use crate::semantic_tokens::{
    handle_semantic_tokens_full, handle_semantic_tokens_full_delta, handle_semantic_tokens_range,
};
use crate::signature_help::handle_signature_help;
use crate::state::{
    send_internal_error, send_response, AnalysisResult, AnalysisTask, PendingSource, ServerConfig,
    ServerState, WorkspaceSymbolCache,
};
use crate::type_hierarchy::{
    handle_prepare_type_hierarchy, handle_type_hierarchy_subtypes,
    handle_type_hierarchy_supertypes,
};
use crate::utils::{offset_to_position, position_to_offset, uri_to_path};
use crate::workspace_diagnostics::{
    handle_document_diagnostics, handle_workspace_diagnostics, prune_stale_workspace_diag_cache,
};
use crate::workspace_symbol::handle_workspace_symbol;

// ── Entry point ─────────────────────────────────────────────────────

fn main() {
    eprintln!("knot-lsp starting...");

    let (connection, io_threads) = Connection::stdio();

    let mut server_capabilities = serde_json::to_value(ServerCapabilities {
        text_document_sync: Some(TextDocumentSyncCapability::Options(
            TextDocumentSyncOptions {
                open_close: Some(true),
                change: Some(TextDocumentSyncKind::INCREMENTAL),
                // Save events drive the didSave robustness backstop in the
                // notification handler. We don't request `include_text` —
                // the buffer the editor saved already matches the source we
                // last analyzed (didSave fires after didChange), so re-reading
                // it would be wasted bandwidth.
                save: Some(TextDocumentSyncSaveOptions::Supported(true)),
                ..Default::default()
            },
        )),
        document_symbol_provider: Some(OneOf::Left(true)),
        definition_provider: Some(OneOf::Left(true)),
        type_definition_provider: Some(TypeDefinitionProviderCapability::Simple(true)),
        implementation_provider: Some(ImplementationProviderCapability::Simple(true)),
        hover_provider: Some(HoverProviderCapability::Simple(true)),
        completion_provider: Some(CompletionOptions {
            trigger_characters: Some(vec![".".into(), "*".into(), "&".into(), "/".into()]),
            resolve_provider: Some(true),
            ..Default::default()
        }),
        references_provider: Some(OneOf::Left(true)),
        rename_provider: Some(OneOf::Right(RenameOptions {
            prepare_provider: Some(true),
            work_done_progress_options: Default::default(),
        })),
        inlay_hint_provider: Some(OneOf::Left(true)),
        signature_help_provider: Some(SignatureHelpOptions {
            trigger_characters: Some(vec![" ".into(), "(".into()]),
            // Comma triggers re-evaluation: when the user moves between
            // arguments the active-parameter index needs to update without
            // re-typing a space. The space-after-arg path is already covered
            // by `trigger_characters`, but commas in `f a, b` would otherwise
            // leave the active parameter stuck on `a`.
            retrigger_characters: Some(vec![",".into(), " ".into()]),
            work_done_progress_options: Default::default(),
        }),
        code_lens_provider: Some(CodeLensOptions {
            resolve_provider: Some(false),
        }),
        semantic_tokens_provider: Some(
            SemanticTokensServerCapabilities::SemanticTokensOptions(SemanticTokensOptions {
                legend: semantic_token_legend(),
                // Advertise range, full, and full/delta — the delta path
                // lets editors re-fetch only changed tokens after edits.
                full: Some(SemanticTokensFullOptions::Delta { delta: Some(true) }),
                range: Some(true),
                work_done_progress_options: Default::default(),
            }),
        ),
        folding_range_provider: Some(FoldingRangeProviderCapability::Simple(true)),
        selection_range_provider: Some(SelectionRangeProviderCapability::Simple(true)),
        call_hierarchy_provider: Some(CallHierarchyServerCapability::Simple(true)),
        code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
        document_formatting_provider: Some(OneOf::Left(true)),
        document_range_formatting_provider: Some(OneOf::Left(true)),
        document_highlight_provider: Some(OneOf::Left(true)),
        document_link_provider: Some(DocumentLinkOptions {
            resolve_provider: Some(false),
            work_done_progress_options: Default::default(),
        }),
        document_on_type_formatting_provider: Some(DocumentOnTypeFormattingOptions {
            first_trigger_character: "\n".into(),
            more_trigger_character: None,
        }),
        workspace_symbol_provider: Some(OneOf::Left(true)),
        linked_editing_range_provider: Some(
            LinkedEditingRangeServerCapabilities::Simple(true),
        ),
        diagnostic_provider: Some(DiagnosticServerCapabilities::Options(DiagnosticOptions {
            identifier: Some("knot".into()),
            inter_file_dependencies: true,
            workspace_diagnostics: true,
            work_done_progress_options: Default::default(),
        })),
        // Advertise multi-folder support and lifecycle notifications. Without
        // this, editors send us only the first folder's URI in initialize and
        // never report subsequent folder add/remove events.
        workspace: Some(WorkspaceServerCapabilities {
            workspace_folders: Some(WorkspaceFoldersServerCapabilities {
                supported: Some(true),
                change_notifications: Some(OneOf::Left(true)),
            }),
            // willRename surfaces upcoming file moves *before* the rename
            // happens, so we can return a WorkspaceEdit that updates every
            // `import` line referencing the old path. Filter to `.knot` files
            // so the editor doesn't bother us for unrelated renames.
            file_operations: Some(WorkspaceFileOperationsServerCapabilities {
                will_rename: Some(FileOperationRegistrationOptions {
                    filters: vec![FileOperationFilter {
                        scheme: Some("file".into()),
                        pattern: FileOperationPattern {
                            glob: "**/*.knot".into(),
                            matches: Some(FileOperationPatternKind::File),
                            options: None,
                        },
                    }],
                }),
                ..Default::default()
            }),
        }),
        ..Default::default()
    })
    .expect("server capabilities are static and always serialize cleanly");

    // Advertise typeHierarchyProvider directly in the JSON — lsp-types 0.97
    // doesn't yet model this field on `ServerCapabilities`, but the wire
    // protocol accepts it. The handlers route `textDocument/prepareTypeHierarchy`
    // and the supertype/subtype follow-ups manually.
    if let Some(obj) = server_capabilities.as_object_mut() {
        obj.insert(
            "typeHierarchyProvider".into(),
            serde_json::Value::Bool(true),
        );
    }

    let init_params = match connection.initialize(server_capabilities) {
        Ok(params) => params,
        Err(e) => {
            eprintln!("Initialize error: {e}");
            return;
        }
    };

    eprintln!("knot-lsp initialized");

    let parsed_init = serde_json::from_value::<lsp_types::InitializeParams>(init_params).ok();
    let workspace_roots: Vec<PathBuf> = parsed_init
        .as_ref()
        .and_then(|p| p.workspace_folders.as_ref())
        .map(|folders| {
            folders
                .iter()
                .filter_map(|f| uri_to_path(&f.uri))
                .collect()
        })
        .unwrap_or_default();
    let workspace_root = workspace_roots.first().cloned();
    let mut config = ServerConfig::default();
    if let Some(opts) = parsed_init
        .as_ref()
        .and_then(|p| p.initialization_options.as_ref())
    {
        config.merge_from_json(opts);
    }
    let client_supports_diagnostic_refresh = parsed_init
        .as_ref()
        .and_then(|p| p.capabilities.workspace.as_ref())
        .and_then(|w| w.diagnostic.as_ref())
        .and_then(|d| d.refresh_support)
        .unwrap_or(false);

    // Spawn the analysis worker. It owns no per-request state of its own —
    // the import cache is shared (Arc<Mutex>) so the main thread can read it
    // for auto-import completion suggestions without a round trip.
    let import_cache = Arc::new(Mutex::new(HashMap::new()));
    let inference_cache = Arc::new(Mutex::new(HashMap::new()));
    let (analysis_tx, analysis_rx) = crossbeam_channel::unbounded::<AnalysisTask>();
    let (results_tx, results_rx) = crossbeam_channel::unbounded::<AnalysisResult>();
    let worker_import_cache = Arc::clone(&import_cache);
    let worker_inference_cache = Arc::clone(&inference_cache);
    let worker = thread::Builder::new()
        .name("knot-lsp-analysis".into())
        .spawn(move || {
            analysis_worker(
                analysis_rx,
                results_tx,
                worker_import_cache,
                worker_inference_cache,
            )
        })
        .expect("failed to spawn analysis worker");

    let mut state = ServerState {
        documents: HashMap::new(),
        workspace_root,
        workspace_roots,
        config,
        import_cache,
        workspace_diag_cache: HashMap::new(),
        workspace_diag_clock: 0,
        workspace_symbol_cache: Arc::new(Mutex::new(WorkspaceSymbolCache::default())),
        pending_sources: HashMap::new(),
        analysis_tx,
        reverse_imports: HashMap::new(),
        inference_cache,
        semantic_token_cache: HashMap::new(),
        semantic_token_counter: 0,
        published_lsp_diagnostics: HashMap::new(),
        client_supports_diagnostic_refresh,
        diagnostic_refresh_counter: 0,
        workspace_diag_reported: HashSet::new(),
    };

    // Register for file watcher notifications (.knot files). Build the
    // request defensively: if any payload fails to serialize (this should
    // never happen for these static structs, but handling it costs nothing),
    // skip the registration rather than panicking.
    if let Some(register_request) = build_file_watcher_registration() {
        let _ = connection.sender.send(Message::Request(register_request));
    }

    // Pre-warm the workspace-symbol cache in the background. The first
    // `workspace/symbol` query then sees a populated cache instead of having
    // to walk the entire workspace from scratch. Runs at lower priority than
    // the analysis worker. Wrapped in catch_unwind so a malformed file on
    // disk that trips a parser/inference panic doesn't silently kill the
    // indexer thread — the analysis worker has the same boundary for the
    // same reason.
    {
        let cache_handle = Arc::clone(&state.workspace_symbol_cache);
        let import_cache_handle = Arc::clone(&state.import_cache);
        let roots = state.workspace_roots.clone();
        let legacy_root = state.workspace_root.clone();
        thread::Builder::new()
            .name("knot-lsp-workspace-indexer".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    prewarm_workspace_symbol_cache(
                        cache_handle,
                        import_cache_handle,
                        &roots,
                        legacy_root.as_deref(),
                    );
                }));
                if let Err(payload) = result {
                    let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
                        (*s).to_string()
                    } else if let Some(s) = payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "panic with non-string payload".to_string()
                    };
                    eprintln!("knot-lsp: workspace indexer panicked: {msg}");
                }
            })
            .ok();
    }

    'outer: loop {
        select! {
            recv(connection.receiver) -> msg => {
                let msg = match msg {
                    Ok(m) => m,
                    Err(_) => break 'outer,
                };
                match msg {
                    Message::Request(req) => {
                        if connection.handle_shutdown(&req).unwrap_or(false) {
                            break 'outer;
                        }
                        let id = req.id.clone();
                        let method = req.method.clone();
                        // A panic in any handler must not bring the server
                        // down — reply with an error so the client unblocks.
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            handle_request(&mut state, &connection, req);
                        }));
                        if let Err(payload) = result {
                            let msg = panic_message(&payload);
                            eprintln!("knot-lsp: handler `{method}` panicked: {msg}");
                            send_internal_error(&connection, id, &method, &msg);
                        }
                    }
                    Message::Notification(not) => {
                        let method = not.method.clone();
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            handle_notification(&mut state, &connection, not);
                        }));
                        if let Err(payload) = result {
                            let msg = panic_message(&payload);
                            eprintln!("knot-lsp: notification `{method}` panicked: {msg}");
                        }
                    }
                    Message::Response(_) => {}
                }
            }
            recv(results_rx) -> result => {
                let result = match result {
                    Ok(r) => r,
                    Err(_) => break 'outer,
                };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    apply_analysis_result(&mut state, &connection, result);
                }));
                if let Err(payload) = result {
                    let msg = panic_message(&payload);
                    eprintln!("knot-lsp: applying analysis result panicked: {msg}");
                }
            }
        }
    }

    // Dropping `state` (and thus `analysis_tx`) closes the channel, prompting
    // the worker to exit on its next blocking `recv`.
    drop(state);
    let _ = worker.join();
    if let Err(e) = io_threads.join() {
        eprintln!("knot-lsp: stdio thread join failed: {e:?}");
    }
}

/// Drop cache entries for paths that no longer fall under any workspace root.
/// Called when folders are removed via `workspace/didChangeWorkspaceFolders`.
/// We don't touch entries belonging to currently-open documents — the editor
/// can keep editing a file even after its containing folder was removed, and
/// we want analysis results to keep flowing.
fn prune_caches_outside_roots(state: &mut ServerState) {
    let roots = state.workspace_roots.clone();
    let open_paths: HashSet<PathBuf> = state
        .documents
        .keys()
        .filter_map(|u| uri_to_path(u))
        .filter_map(|p| p.canonicalize().ok())
        .collect();
    let in_scope = |p: &Path| -> bool {
        if open_paths.contains(p) {
            return true;
        }
        roots.iter().any(|r| p.starts_with(r))
    };

    state
        .workspace_diag_cache
        .retain(|p, _| in_scope(p));
    state
        .reverse_imports
        .retain(|p, _| in_scope(p));
    for importers in state.reverse_imports.values_mut() {
        importers.retain(|p| in_scope(p));
    }
    if let Ok(mut cache) = state.import_cache.lock() {
        cache.retain(|p, _| in_scope(p));
    }
    if let Ok(mut sym) = state.workspace_symbol_cache.lock() {
        sym.by_path.retain(|p, _| in_scope(p));
    }
    // Also drop the "previously reported" tracking set so a re-added folder
    // doesn't get a one-shot empty-diagnostic flush for files that no longer
    // exist in this session.
    state
        .workspace_diag_reported
        .retain(|uri| match uri_to_path(uri).and_then(|p| p.canonicalize().ok()) {
            Some(p) => in_scope(&p),
            None => false,
        });
}

/// Walk every `.knot` file under the given workspace roots and populate the
/// shared workspace-symbol cache. Each file is read+parsed only if the cache
/// doesn't already hold a fresh entry (mtime match). The first `workspace/symbol`
/// query after init then runs against a hot cache.
fn prewarm_workspace_symbol_cache(
    cache: Arc<Mutex<WorkspaceSymbolCache>>,
    import_cache: Arc<Mutex<HashMap<PathBuf, (u64, knot::ast::Module, String)>>>,
    roots: &[PathBuf],
    legacy_root: Option<&Path>,
) {
    use crate::shared::scan_knot_files_in_roots;
    use crate::state::content_hash;
    use crate::utils::path_to_uri;
    use crate::workspace_symbol::build_workspace_symbol_entries;

    for path in scan_knot_files_in_roots(roots, legacy_root) {
        let canonical = match path.canonicalize() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let on_disk_mtime = std::fs::metadata(&canonical)
            .ok()
            .and_then(|m| m.modified().ok());
        // Mtime fast-path: skip if the cache already has a fresh entry.
        let already_fresh = cache.lock().ok().is_some_and(|c| {
            matches!(
                (c.by_path.get(&canonical), on_disk_mtime),
                (Some((Some(cached), _, _)), Some(disk)) if *cached == disk
            )
        });
        if already_fresh {
            continue;
        }
        let source = match std::fs::read_to_string(&canonical) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let hash = content_hash(&source);
        let module = match crate::analysis::get_or_parse_file_shared(&canonical, &import_cache) {
            Some((m, _)) => m,
            None => continue,
        };
        let uri = match path_to_uri(&canonical) {
            Some(u) => u,
            None => continue,
        };
        let entries = build_workspace_symbol_entries(&module, &source, &uri);
        if let Ok(mut c) = cache.lock() {
            c.by_path.insert(canonical, (on_disk_mtime, hash, entries));
        }
    }
}

/// `workspace/willRenameFiles` — return a `WorkspaceEdit` that updates every
/// `import` line referencing the moved file. Runs synchronously before the
/// editor performs the rename; if we miss any importer, the user gets a
/// diagnostic on the next analysis cycle.
fn handle_will_rename_files(
    state: &ServerState,
    params: &RenameFilesParams,
) -> Option<WorkspaceEdit> {
    use std::collections::HashMap as Map;
    let mut changes: Map<Uri, Vec<TextEdit>> = Map::new();

    for rename in &params.files {
        let old_uri: Uri = match rename.old_uri.parse() {
            Ok(u) => u,
            Err(_) => continue,
        };
        let new_uri: Uri = match rename.new_uri.parse() {
            Ok(u) => u,
            Err(_) => continue,
        };
        let old_path = match uri_to_path(&old_uri).and_then(|p| p.canonicalize().ok()) {
            Some(p) => p,
            None => continue,
        };
        let new_path = match uri_to_path(&new_uri) {
            Some(p) => p,
            None => continue,
        };

        for (importer_uri, doc) in &state.documents {
            let importer_path = match uri_to_path(importer_uri) {
                Some(p) => p,
                None => continue,
            };
            let importer_dir = match importer_path.parent() {
                Some(p) => p,
                None => continue,
            };
            for imp in &doc.module.imports {
                let resolved = importer_dir.join(format!("{}.knot", imp.path));
                let canonical = match resolved.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if canonical != old_path {
                    continue;
                }
                // `relative_import_path` takes the importer file (not its dir)
                // and the destination, normalizes separators to `/`, and adds
                // a `./` prefix for same-directory paths.
                let new_rel = match crate::code_action::relative_import_path(
                    &importer_path,
                    &new_path,
                ) {
                    Some(s) => s,
                    None => continue,
                };
                // The import statement's span covers `import path` — replace
                // just the path portion. Compute it by finding the first
                // non-whitespace after `import`.
                let span = imp.span;
                let span_text = match doc.source.get(span.start..span.end) {
                    Some(s) => s,
                    None => continue,
                };
                let path_offset_in_span = span_text
                    .find("import")
                    .map(|i| i + "import".len())
                    .unwrap_or(0);
                let path_start = span.start
                    + path_offset_in_span
                    + span_text[path_offset_in_span..]
                        .chars()
                        .take_while(|c| c.is_whitespace())
                        .map(|c| c.len_utf8())
                        .sum::<usize>();
                let path_start_pos = offset_to_position(&doc.source, path_start);
                let path_end_pos = offset_to_position(&doc.source, span.end);
                changes
                    .entry(importer_uri.clone())
                    .or_default()
                    .push(TextEdit {
                        range: Range {
                            start: path_start_pos,
                            end: path_end_pos,
                        },
                        new_text: new_rel,
                    });
            }
        }
    }

    if changes.is_empty() {
        return None;
    }
    Some(WorkspaceEdit {
        changes: Some(changes),
        ..Default::default()
    })
}

/// Construct the `client/registerCapability` request that asks the editor to
/// notify us about changes to `.knot` files. Returns `None` if the static
/// payloads fail to serialize (defensive — should never happen).
fn build_file_watcher_registration() -> Option<Request> {
    let watcher_options = serde_json::to_value(DidChangeWatchedFilesRegistrationOptions {
        watchers: vec![FileSystemWatcher {
            glob_pattern: GlobPattern::String("**/*.knot".into()),
            kind: Some(WatchKind::Create | WatchKind::Delete | WatchKind::Change),
        }],
    })
    .ok()?;
    let registration = Registration {
        id: "knot-file-watcher".into(),
        method: "workspace/didChangeWatchedFiles".into(),
        register_options: Some(watcher_options),
    };
    let params = serde_json::to_value(RegistrationParams {
        registrations: vec![registration],
    })
    .ok()?;
    Some(Request::new(
        RequestId::from("register-file-watcher".to_string()),
        "client/registerCapability".into(),
        params,
    ))
}

/// Apply a finished analysis: replace the document state and publish diagnostics.
fn apply_analysis_result(state: &mut ServerState, conn: &Connection, result: AnalysisResult) {
    // Document closed while analysis was in flight: drop the result rather
    // than resurrecting a removed document.
    let tracked = state.documents.contains_key(&result.uri)
        || state.pending_sources.contains_key(&result.uri);
    if !tracked {
        return;
    }

    // If a newer edit was applied while analysis was running, drop the result.
    // The newer edit will already have queued a fresh task.
    if let Some(pending) = state.pending_sources.get(&result.uri) {
        if pending.source != result.doc.source {
            return;
        }
        state.pending_sources.remove(&result.uri);
    }

    // Compute the LSP-shaped diagnostics from the freshly analyzed doc *before*
    // moving the doc into `state.documents`. We need these for the publish
    // call after the insert (the publish helper no longer holds a doc
    // reference, so the borrow checker is happy).
    let lsp_diags: Vec<lsp_types::Diagnostic> = result
        .doc
        .knot_diagnostics
        .iter()
        .filter_map(|d| crate::diagnostics::to_lsp_diagnostic(d, &result.doc.source, &result.uri))
        .collect();

    // Update the reverse-import graph for cross-file diagnostics. Each
    // analyzed module knows which files it imported (`imported_files`); we
    // invert that map so a later edit to an imported file can re-queue every
    // open consumer for re-analysis.
    if let Some(this_path) = uri_to_path(&result.uri).and_then(|p| p.canonicalize().ok()) {
        // Drop any prior incoming edges from this importer — a removed
        // `import X` statement should stop pulling X back in for re-checks.
        for importers in state.reverse_imports.values_mut() {
            importers.remove(&this_path);
        }
        for imported in result.doc.imported_files.keys() {
            state
                .reverse_imports
                .entry(imported.clone())
                .or_default()
                .insert(this_path.clone());
        }

        // Selective dependent re-analysis: when a file changes, only
        // re-queue downstream files whose `import_defs` actually reference
        // a *signature-changed* decl name. Body-only edits to a typed
        // function don't move its outward-facing type, so dependents of
        // that name don't need a fresh inference pass — the broader
        // `changed_decl_names` set is used in-file for telemetry only.
        // Without this filter, every keystroke on a popular utility module
        // re-analyzes its entire dependency closure even when the user is
        // just editing a function body that no consumer depends on directly.
        let changed: HashSet<&str> = result
            .doc
            .signature_changed_decl_names
            .iter()
            .map(|s| s.as_str())
            .collect();
        if !changed.is_empty() {
            requeue_dependents_for_changed_decls(state, &result.uri, &this_path, &changed);
        }
    }

    // Update `state.documents` *before* publishing or sending the diagnostic
    // refresh. Pull-mode clients (JetBrains) react to the refresh by
    // immediately re-pulling via `textDocument/diagnostic`; that handler
    // reads `state.documents.knot_diagnostics`. If we sent the refresh
    // before this insert, the client would re-pull and get the stale prior
    // doc — which is exactly the bug this is meant to fix.
    let uri = result.uri.clone();
    let version = result.version;
    state.documents.insert(result.uri, result.doc);

    let published = publish_diagnostics_dedup(state, conn, &uri, lsp_diags, version);
    if published {
        // Pull-mode clients (notably JetBrains) ignore the publish above and
        // only refresh diagnostics when the server explicitly invalidates
        // their cache. Without this, a fix that clears a diagnostic stays
        // visible in the gutter until the user triggers another pull.
        request_workspace_diagnostic_refresh(state, conn);
    }
}

/// Re-queue analysis for open documents whose imports reference any of the
/// changed decl names. Walks the reverse-import graph transitively so the
/// downstream chain (A imports B imports C; C changes) reaches the right set
/// of consumers.
fn requeue_dependents_for_changed_decls(
    state: &mut ServerState,
    source_uri: &Uri,
    changed_path: &Path,
    changed_names: &HashSet<&str>,
) {
    // Transitive set of importers via BFS over reverse_imports.
    let mut to_visit = vec![changed_path.to_path_buf()];
    let mut visited: HashSet<PathBuf> = HashSet::new();
    let mut affected: HashSet<PathBuf> = HashSet::new();
    while let Some(p) = to_visit.pop() {
        if !visited.insert(p.clone()) {
            continue;
        }
        if let Some(importers) = state.reverse_imports.get(&p) {
            for imp in importers {
                if affected.insert(imp.clone()) {
                    to_visit.push(imp.clone());
                }
            }
        }
    }

    let dependents: Vec<(Uri, PathBuf, String)> = state
        .documents
        .iter()
        .filter(|(other_uri, _)| *other_uri != source_uri)
        .filter_map(|(uri, doc)| {
            let path = uri_to_path(uri).and_then(|p| p.canonicalize().ok())?;
            if !affected.contains(&path) {
                return None;
            }
            // Only re-queue if the dependent imports at least one of the
            // changed names from `changed_path`. Two-level filter: first
            // we narrow to importers (via `affected`), then we narrow to
            // importers that actually use one of the changed names.
            let uses_changed = doc.import_defs.iter().any(|(n, (origin, _))| {
                origin == changed_path && changed_names.contains(n.as_str())
            });
            if !uses_changed {
                return None;
            }
            let src = state
                .pending_sources
                .get(uri)
                .map(|p| p.source.clone())
                .unwrap_or_else(|| doc.source.clone());
            Some((uri.clone(), path, src))
        })
        .collect();

    if dependents.is_empty() {
        return;
    }

    if let Ok(mut cache) = state.inference_cache.lock() {
        let dep_paths: HashSet<&PathBuf> = dependents.iter().map(|(_, p, _)| p).collect();
        cache.retain(|(p, _), _| !dep_paths.contains(p));
    }

    for (dep_uri, _, dep_source) in dependents {
        queue_analysis(state, dep_uri, dep_source, None);
    }
}

// ── Request dispatch ────────────────────────────────────────────────

fn handle_request(state: &mut ServerState, conn: &Connection, req: Request) {
    let id = req.id.clone();

    if let Some(params) = cast_request::<request::DocumentSymbolRequest>(&req) {
        let result = handle_document_symbol(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::GotoDefinition>(&req) {
        let result = handle_goto_definition(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::GotoTypeDefinition>(&req) {
        let result = handle_goto_type_definition(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::GotoImplementation>(&req) {
        let result = handle_goto_implementation(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::HoverRequest>(&req) {
        let result = handle_hover(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::Completion>(&req) {
        let result = handle_completion(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::References>(&req) {
        let result = handle_references(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::PrepareRenameRequest>(&req) {
        let result = handle_prepare_rename(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::Rename>(&req) {
        let result = handle_rename(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::InlayHintRequest>(&req) {
        let result = handle_inlay_hint(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::SignatureHelpRequest>(&req) {
        let result = handle_signature_help(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::CodeLensRequest>(&req) {
        let result = handle_code_lens(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::SemanticTokensFullRequest>(&req) {
        let result = handle_semantic_tokens_full(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::SemanticTokensFullDeltaRequest>(&req) {
        let result = handle_semantic_tokens_full_delta(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::SemanticTokensRangeRequest>(&req) {
        let result = handle_semantic_tokens_range(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::FoldingRangeRequest>(&req) {
        let result = handle_folding_range(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::SelectionRangeRequest>(&req) {
        let result = handle_selection_range(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::Formatting>(&req) {
        let result = handle_formatting(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::RangeFormatting>(&req) {
        let result = handle_range_formatting(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::OnTypeFormatting>(&req) {
        let result = handle_on_type_formatting(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::DocumentHighlightRequest>(&req) {
        let result = handle_document_highlight(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::DocumentLinkRequest>(&req) {
        let result = handle_document_link(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::CodeActionRequest>(&req) {
        let result = handle_code_action(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::WorkspaceSymbolRequest>(&req) {
        let result = handle_workspace_symbol(state, &params);
        send_response(conn, id, result);
        // Keep workspace_symbol_cache from growing unbounded — pruning happens
        // inside the handler via the on-disk scan.
    } else if let Some(params) = cast_request::<request::CallHierarchyPrepare>(&req) {
        let result = handle_call_hierarchy_prepare(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::CallHierarchyIncomingCalls>(&req) {
        let result = handle_call_hierarchy_incoming(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::CallHierarchyOutgoingCalls>(&req) {
        let result = handle_call_hierarchy_outgoing(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::ResolveCompletionItem>(&req) {
        let result = handle_resolve_completion_item(state, params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::LinkedEditingRange>(&req) {
        let result = handle_linked_editing_range(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::DocumentDiagnosticRequest>(&req) {
        let result = handle_document_diagnostics(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::WorkspaceDiagnosticRequest>(&req) {
        let result = handle_workspace_diagnostics(state, &params);
        send_response(conn, id, result);
        // Periodically prune the workspace diagnostics cache to avoid
        // unbounded growth when files are deleted from disk.
        prune_stale_workspace_diag_cache(state);
    } else if let Some(params) = cast_request::<request::WillRenameFiles>(&req) {
        let result = handle_will_rename_files(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::TypeHierarchyPrepare>(&req) {
        let result = handle_prepare_type_hierarchy(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::TypeHierarchySupertypes>(&req) {
        let result = handle_type_hierarchy_supertypes(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::TypeHierarchySubtypes>(&req) {
        let result = handle_type_hierarchy_subtypes(state, &params);
        send_response(conn, id, result);
    }
}

fn cast_request<R: request::Request>(req: &Request) -> Option<R::Params> {
    if req.method == R::METHOD {
        serde_json::from_value(req.params.clone()).ok()
    } else {
        None
    }
}

// ── Notification dispatch ───────────────────────────────────────────

fn handle_notification(state: &mut ServerState, conn: &Connection, not: Notification) {
    /// Decode notification params or log+drop the message. A malformed payload
    /// from a misbehaving client must not bring down the server.
    fn decode<T: serde::de::DeserializeOwned>(method: &str, value: serde_json::Value) -> Option<T> {
        match serde_json::from_value(value) {
            Ok(v) => Some(v),
            Err(e) => {
                eprintln!("knot-lsp: malformed `{method}` payload: {e}");
                None
            }
        }
    }

    if not.method == notification::DidOpenTextDocument::METHOD {
        let Some(params) = decode::<DidOpenTextDocumentParams>(&not.method, not.params) else {
            return;
        };
        let uri = params.text_document.uri.clone();
        let version = Some(params.text_document.version);
        // Park the source as pending until analysis catches up. This lets
        // subsequent didChange edits stack on top of the freshest text even
        // before the worker has produced its first AST.
        state.pending_sources.insert(
            uri.clone(),
            PendingSource {
                source: params.text_document.text.clone(),
                version,
            },
        );
        queue_analysis(state, uri, params.text_document.text, version);
    } else if not.method == notification::DidChangeTextDocument::METHOD {
        let Some(params) =
            decode::<DidChangeTextDocumentParams>(&not.method, not.params)
        else {
            return;
        };
        let uri = params.text_document.uri.clone();
        let version = Some(params.text_document.version);
        // Apply edits to the freshest source we know about: the pending one
        // (if mid-debounce) or the last analyzed one. Falling back to the
        // analyzed source could lose interleaved edits, so prefer pending.
        let mut source = state
            .pending_sources
            .get(&uri)
            .map(|p| p.source.clone())
            .or_else(|| state.documents.get(&uri).map(|d| d.source.clone()))
            .unwrap_or_default();

        // Rebase any cached LSP diagnostics through the incoming edits so
        // their ranges keep tracking the document while the analysis worker
        // catches up. Without this, the editor renders stale line/character
        // positions from the last analysis run, and the diagnostics appear
        // to drift after every keystroke. We shift in byte-offset space (the
        // diagnostics' line/character ranges convert to byte offsets in the
        // *current* source, then move with each edit), and finally render
        // them back to LSP positions against the post-edit source.
        let cached_diags = state.published_lsp_diagnostics.get(&uri).cloned();
        // Convert each diagnostic's `Range` (and any related-info ranges that
        // happen to live in this same file) to byte offsets up front. After
        // each edit the offsets are adjusted in place; ranges that overlap
        // an edit are tagged with `usize::MAX` and dropped at the end.
        let mut diag_byte_ranges: Option<Vec<(usize, usize)>> = cached_diags
            .as_ref()
            .map(|ds| {
                ds.iter()
                    .map(|d| (
                        position_to_offset(&source, d.range.start),
                        position_to_offset(&source, d.range.end),
                    ))
                    .collect()
            });

        for change in params.content_changes {
            if let Some(range) = change.range {
                let start = position_to_offset(&source, range.start);
                let end = position_to_offset(&source, range.end);
                let new_len = change.text.len();
                if let Some(diag_ranges) = diag_byte_ranges.as_mut() {
                    shift_byte_ranges_for_edit(diag_ranges, start, end, new_len);
                }
                source.replace_range(start..end, &change.text);
            } else {
                source = change.text;
                // Full replace invalidates every cached range — the document
                // structure no longer relates to the prior analysis output.
                if let Some(diag_ranges) = diag_byte_ranges.as_mut() {
                    for r in diag_ranges {
                        *r = (usize::MAX, usize::MAX);
                    }
                }
            }
        }

        // Republish rebased diagnostics with the new version. The editor sees
        // up-to-date positions immediately; the next analysis push will
        // either confirm them or replace them with fresh ones.
        if let (Some(cached), Some(byte_ranges)) = (cached_diags, diag_byte_ranges.as_ref()) {
            let rebased: Vec<lsp_types::Diagnostic> = cached
                .iter()
                .zip(byte_ranges.iter())
                .filter_map(|(d, &(s, e))| {
                    if s == usize::MAX || e > source.len() {
                        return None;
                    }
                    let mut shifted = d.clone();
                    shifted.range = lsp_types::Range {
                        start: offset_to_position(&source, s),
                        end: offset_to_position(&source, e),
                    };
                    Some(shifted)
                })
                .collect();
            // Skip the LSP roundtrip when nothing actually moved — first
            // didChange after a publish typically happens before any cached
            // diagnostic's position drifted.
            if rebased != cached {
                let params = lsp_types::PublishDiagnosticsParams::new(
                    uri.clone(),
                    rebased.clone(),
                    version,
                );
                let not = Notification::new(
                    lsp_types::notification::PublishDiagnostics::METHOD.into(),
                    params,
                );
                if let Err(e) = conn.sender.send(Message::Notification(not)) {
                    eprintln!("knot-lsp: failed to publish rebased diagnostics: {e}");
                }
                state
                    .published_lsp_diagnostics
                    .insert(uri.clone(), rebased);
                // Don't send a refresh here: pull-mode clients would re-pull
                // and see the still-stale `state.documents.knot_diagnostics`
                // (analysis hasn't caught up yet). The post-analysis refresh
                // in `apply_analysis_result` handles the update once
                // `state.documents` reflects the new source.
            }
        }

        // No-op edits (e.g. format-on-save with no changes) shouldn't queue
        // redundant work. The pending check covers an edit that was already
        // queued; the analyzed check covers a steady state.
        let already_pending = state
            .pending_sources
            .get(&uri)
            .map(|p| p.source == source)
            .unwrap_or(false);
        let unchanged = state
            .documents
            .get(&uri)
            .map(|d| d.source == source)
            .unwrap_or(false);
        if already_pending {
            // Just refresh the version so the result-routing check in
            // `apply_analysis_result` keeps working.
            if let Some(p) = state.pending_sources.get_mut(&uri) {
                p.version = version;
            }
            return;
        }
        if unchanged {
            state.pending_sources.remove(&uri);
            return;
        }

        state.pending_sources.insert(
            uri.clone(),
            PendingSource {
                source: source.clone(),
                version,
            },
        );

        // The current file's edits can invalidate cached diagnostics for any
        // unopened file that imports it (directly or transitively). The open
        // file itself is served from `state.documents`, not the workspace
        // diag cache, so we only need to evict the importers.
        if let Some(this_path) = uri_to_path(&uri).and_then(|p| p.canonicalize().ok()) {
            let mut changed = HashSet::new();
            changed.insert(this_path);
            invalidate_workspace_diag_cache_for(state, &changed);
        }

        queue_analysis(state, uri.clone(), source, version);

        // Dependents are no longer re-queued eagerly here — `apply_analysis_result`
        // handles them once the changed file's analysis completes, filtered by
        // the per-decl change set so unrelated dependents stay quiet.
    } else if not.method == notification::DidChangeWatchedFiles::METHOD {
        let Some(params) =
            decode::<DidChangeWatchedFilesParams>(&not.method, not.params)
        else {
            return;
        };
        let changed_paths: HashSet<PathBuf> = params
            .changes
            .iter()
            .filter_map(|c| uri_to_path(&c.uri))
            .filter_map(|p| p.canonicalize().ok())
            .collect();

        if !changed_paths.is_empty() {
            let dependents: Vec<(Uri, PathBuf, String)> = state
                .documents
                .iter()
                .filter(|(_, doc)| {
                    doc.imported_files
                        .keys()
                        .any(|p| changed_paths.contains(p))
                })
                .filter_map(|(uri, doc)| {
                    let path = uri_to_path(uri).and_then(|p| p.canonicalize().ok())?;
                    let src = state
                        .pending_sources
                        .get(uri)
                        .map(|p| p.source.clone())
                        .unwrap_or_else(|| doc.source.clone());
                    Some((uri.clone(), path, src))
                })
                .collect();

            // Evict cached snapshots for affected dependents and the
            // changed paths themselves — the inference for a file whose
            // imports just changed on disk is no longer valid.
            if let Ok(mut cache) = state.inference_cache.lock() {
                let affected: HashSet<&PathBuf> = changed_paths
                    .iter()
                    .chain(dependents.iter().map(|(_, p, _)| p))
                    .collect();
                cache.retain(|(p, _), _| !affected.contains(p));
            }

            // Same logic applied to the workspace-diagnostic cache: any
            // unopened-file diagnostics that referenced the changed file's
            // exports are stale now. Without eager invalidation, the next
            // workspace-diag request would replay last run's diagnostics.
            invalidate_workspace_diag_cache_for(state, &changed_paths);

            for (dep_uri, _, dep_source) in dependents {
                queue_analysis(state, dep_uri, dep_source, None);
            }
        }
    } else if not.method == notification::DidSaveTextDocument::METHOD {
        // Save is a robustness backstop: re-render whatever diagnostics we
        // currently believe are correct and bypass the dedup, so any prior
        // dropped/coalesced publish (or an out-of-sync editor) gets a fresh
        // copy. Doesn't queue analysis — the source is already what we just
        // analyzed, save events carry no new content (we don't opt into
        // `includeText`).
        let Some(params) =
            decode::<DidSaveTextDocumentParams>(&not.method, not.params)
        else {
            return;
        };
        let uri = params.text_document.uri;
        if let Some(doc) = state.documents.get(&uri) {
            let lsp_diags: Vec<Diagnostic> = doc
                .knot_diagnostics
                .iter()
                .filter_map(|d| crate::diagnostics::to_lsp_diagnostic(d, &doc.source, &uri))
                .collect();
            // Force-publish: clear the dedup cache first so the publish always
            // goes out, then update the cache to the just-sent list.
            state.published_lsp_diagnostics.remove(&uri);
            publish_diagnostics_dedup(state, conn, &uri, lsp_diags, None);
            request_workspace_diagnostic_refresh(state, conn);
        }
    } else if not.method == notification::DidCloseTextDocument::METHOD {
        let Some(params) =
            decode::<DidCloseTextDocumentParams>(&not.method, not.params)
        else {
            return;
        };
        state.documents.remove(&params.text_document.uri);
        state.pending_sources.remove(&params.text_document.uri);
        state.semantic_token_cache.remove(&params.text_document.uri);
        // Drop the diagnostic-dedup entry too: otherwise a reopen whose first
        // analysis produced the same list as the last pre-close run would
        // short-circuit republishing, leaving the editor with the empty
        // diagnostics we just sent below.
        state
            .published_lsp_diagnostics
            .remove(&params.text_document.uri);
        let diags = PublishDiagnosticsParams::new(params.text_document.uri, vec![], None);
        let not = Notification::new(notification::PublishDiagnostics::METHOD.into(), diags);
        if let Err(e) = conn.sender.send(Message::Notification(not)) {
            eprintln!("knot-lsp: failed to publish empty diagnostics on close: {e}");
        }
    } else if not.method == notification::DidChangeConfiguration::METHOD {
        // Apply runtime config changes (tab size, inlay-hint toggles, cache
        // bounds). Refresh inlay hints since their visibility may have flipped.
        let Some(params) =
            decode::<DidChangeConfigurationParams>(&not.method, not.params)
        else {
            return;
        };
        state.config.merge_from_json(&params.settings);
        // Best-effort hint-refresh request. The client may not honor it, but
        // sending it is the standard way to invalidate stale hints.
        let refresh = Request::new(
            RequestId::from("inlay-hint-refresh".to_string()),
            "workspace/inlayHint/refresh".into(),
            serde_json::Value::Null,
        );
        let _ = conn.sender.send(Message::Request(refresh));
    } else if not.method == notification::DidChangeWorkspaceFolders::METHOD {
        let Some(params) =
            decode::<DidChangeWorkspaceFoldersParams>(&not.method, not.params)
        else {
            return;
        };
        // Apply added/removed folders. We rebuild `workspace_roots` from the
        // delta so the order roughly mirrors editor presentation order.
        let removed: HashSet<PathBuf> = params
            .event
            .removed
            .iter()
            .filter_map(|f| uri_to_path(&f.uri))
            .collect();
        state.workspace_roots.retain(|p| !removed.contains(p));
        for added in &params.event.added {
            if let Some(path) = uri_to_path(&added.uri) {
                if !state.workspace_roots.contains(&path) {
                    state.workspace_roots.push(path);
                }
            }
        }
        state.workspace_root = state.workspace_roots.first().cloned();

        // Drop cached state for files that fall outside the surviving roots.
        // Without this, a removed folder's `import_cache` / `reverse_imports` /
        // `workspace_diag_cache` / `workspace_symbol_cache` entries persist for
        // the rest of the session, surfacing stale errors on reopened folders
        // that share filenames and bloating memory in long-lived sessions.
        if !removed.is_empty() {
            prune_caches_outside_roots(state);
        }
    }
}

/// Publish diagnostics, but skip the LSP roundtrip when the diagnostic set is
/// identical to the last publish for this URI. Whitespace-only and
/// comment-only edits go through the fingerprint cache, producing the same
/// `knot_diagnostics` output verbatim — there's no need to re-render the
/// editor's gutter for those.
///
/// Returns `true` when a publish was actually sent. The caller uses this to
/// decide whether to follow up with `workspace/diagnostic/refresh` for
/// pull-mode clients that ignore `publishDiagnostics`.
///
/// Takes `lsp_diags` by value rather than borrowing the source `DocumentState`
/// so the caller can move the doc into `state.documents` *before* invoking
/// publish — that ordering matters for pull-mode clients (see callers).
fn publish_diagnostics_dedup(
    state: &mut ServerState,
    conn: &Connection,
    uri: &Uri,
    lsp_diags: Vec<lsp_types::Diagnostic>,
    version: Option<i32>,
) -> bool {
    // Direct equality against the cached list. We previously kept a separate
    // 64-bit hash to dedup, but a hash collision (rare but not impossible)
    // would silently swallow a needed publish, leaving stale diagnostics in
    // the gutter — and since we already store the full list for the rebase
    // path, the hash bought no memory savings. Equality on a typically-tiny
    // `Vec<Diagnostic>` is cheap.
    if state.published_lsp_diagnostics.get(uri) == Some(&lsp_diags) {
        return false;
    }
    state
        .published_lsp_diagnostics
        .insert(uri.clone(), lsp_diags.clone());
    let params = lsp_types::PublishDiagnosticsParams::new(uri.clone(), lsp_diags, version);
    let not = Notification::new(
        lsp_types::notification::PublishDiagnostics::METHOD.into(),
        params,
    );
    if let Err(e) = conn.sender.send(Message::Notification(not)) {
        eprintln!("knot-lsp: failed to publish diagnostics: {e}");
    }
    true
}

/// Ask a pull-mode client (e.g. JetBrains) to re-pull diagnostics. Pull-mode
/// clients ignore `publishDiagnostics`, so a fix that clears a diagnostic
/// would otherwise stay visible until the user triggered another pull (open
/// a file, edit again, etc.). Push-mode clients ignore the refresh request,
/// so it's safe to send unconditionally when the capability is advertised.
///
/// Counter is monotonic to keep request ids unique across calls.
fn request_workspace_diagnostic_refresh(state: &mut ServerState, conn: &Connection) {
    if !state.client_supports_diagnostic_refresh {
        return;
    }
    state.diagnostic_refresh_counter = state.diagnostic_refresh_counter.wrapping_add(1);
    let req = Request::new(
        RequestId::from(format!("knot-diag-refresh-{}", state.diagnostic_refresh_counter)),
        lsp_types::request::WorkspaceDiagnosticRefresh::METHOD.into(),
        serde_json::Value::Null,
    );
    if let Err(e) = conn.sender.send(Message::Request(req)) {
        eprintln!("knot-lsp: failed to send workspace/diagnostic/refresh: {e}");
    }
}

/// Shift the cached diagnostic byte-ranges through a single edit. The edit
/// replaces `[edit_start, edit_end)` with `new_len` bytes of new text. For
/// each cached range:
///
/// - Fully before the edit (`r.end <= edit_start`): unchanged.
/// - Fully after the edit (`r.start >= edit_end`): shifted by `new_len -
///   (edit_end - edit_start)`.
/// - Overlapping the edit: marked with `usize::MAX` so the caller drops it.
///   The diagnostic's content is no longer aligned with valid bytes — better
///   to hide it until the next analysis pass.
fn shift_byte_ranges_for_edit(
    ranges: &mut [(usize, usize)],
    edit_start: usize,
    edit_end: usize,
    new_len: usize,
) {
    let removed = edit_end - edit_start;
    // Compute the shift in signed terms so insertions and deletions both work.
    // Sentinel ranges (usize::MAX) propagate untouched.
    let new_len_i = new_len as isize;
    let removed_i = removed as isize;
    for r in ranges {
        if r.0 == usize::MAX {
            continue;
        }
        if r.1 <= edit_start {
            // Range fully before the edit: nothing to shift.
            continue;
        }
        if r.0 >= edit_end {
            // Range fully after the edit: shift both endpoints.
            let delta = new_len_i - removed_i;
            r.0 = (r.0 as isize + delta).max(0) as usize;
            r.1 = (r.1 as isize + delta).max(0) as usize;
            continue;
        }
        // Overlap — invalidate. The diagnostic's anchored content has been
        // partially replaced, so any shifted position would be misleading.
        *r = (usize::MAX, usize::MAX);
    }
}

/// Send an analysis task to the worker. Errors here are unrecoverable — the
/// worker has died — so we eprintln and continue (other features still work
/// against the last good analysis).
fn queue_analysis(state: &ServerState, uri: Uri, source: String, version: Option<i32>) {
    if let Err(e) = state.analysis_tx.send(AnalysisTask { uri, source, version }) {
        eprintln!("knot-lsp: analysis worker channel closed: {e}");
    }
}

/// Eagerly evict workspace-diagnostic cache entries for `changed` and every
/// file that transitively imports any of them. Without this, the cache can
/// hand stale diagnostics to the editor between a file edit and the next
/// pull-mode `workspace/diagnostic` request — the lazy `prune_stale_…` pass
/// only runs on workspace-diag requests, so cross-file errors caused by an
/// upstream edit linger until the user happens to ask for them again.
fn invalidate_workspace_diag_cache_for(state: &mut ServerState, changed: &HashSet<PathBuf>) {
    if changed.is_empty() {
        return;
    }
    // Transitive closure over the reverse-import graph. We start the BFS from
    // every changed path and pull in any file that imports them, directly or
    // through a chain of imports.
    let mut affected: HashSet<PathBuf> = changed.iter().cloned().collect();
    let mut frontier: Vec<PathBuf> = changed.iter().cloned().collect();
    while let Some(p) = frontier.pop() {
        if let Some(importers) = state.reverse_imports.get(&p) {
            for imp in importers {
                if affected.insert(imp.clone()) {
                    frontier.push(imp.clone());
                }
            }
        }
    }
    state
        .workspace_diag_cache
        .retain(|path, _| !affected.contains(path));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Insertion before every cached range pushes them all forward by the
    /// inserted byte count.
    #[test]
    fn shift_byte_ranges_insertion_before() {
        let mut ranges = vec![(10, 14), (20, 25)];
        // Insert 3 bytes at offset 5 (replace 0 bytes).
        shift_byte_ranges_for_edit(&mut ranges, 5, 5, 3);
        assert_eq!(ranges, vec![(13, 17), (23, 28)]);
    }

    /// An edit ending at exactly a range's start should not shift it (the
    /// range still begins at its same byte content; new text was inserted
    /// strictly before).
    #[test]
    fn shift_byte_ranges_insertion_at_start_boundary_shifts() {
        let mut ranges = vec![(10, 14)];
        shift_byte_ranges_for_edit(&mut ranges, 10, 10, 4);
        // Range starts AT edit_end, so it shifts.
        assert_eq!(ranges, vec![(14, 18)]);
    }

    /// Deletion before a range pulls it back.
    #[test]
    fn shift_byte_ranges_deletion_before() {
        let mut ranges = vec![(20, 25)];
        // Delete 3 bytes (replace [5, 8) with empty).
        shift_byte_ranges_for_edit(&mut ranges, 5, 8, 0);
        assert_eq!(ranges, vec![(17, 22)]);
    }

    /// Edits fully after a range leave it alone.
    #[test]
    fn shift_byte_ranges_edit_after_range_leaves_it_alone() {
        let mut ranges = vec![(2, 5)];
        shift_byte_ranges_for_edit(&mut ranges, 10, 12, 5);
        assert_eq!(ranges, vec![(2, 5)]);
    }

    /// Edits overlapping a range invalidate it.
    #[test]
    fn shift_byte_ranges_overlap_invalidates() {
        let mut ranges = vec![(10, 20), (30, 40)];
        // Replace [15, 35) — overlaps both ranges.
        shift_byte_ranges_for_edit(&mut ranges, 15, 35, 5);
        assert_eq!(ranges, vec![(usize::MAX, usize::MAX), (usize::MAX, usize::MAX)]);
    }

    /// A previously invalidated range stays invalid through subsequent edits.
    #[test]
    fn shift_byte_ranges_invalidated_stays_invalidated() {
        let mut ranges = vec![(usize::MAX, usize::MAX), (50, 60)];
        shift_byte_ranges_for_edit(&mut ranges, 0, 0, 10);
        assert_eq!(
            ranges,
            vec![(usize::MAX, usize::MAX), (60, 70)],
            "sentinel must not be shifted; live range moves by +10"
        );
    }

    // ── apply_analysis_result + publish_diagnostics_dedup tests ────────
    //
    // These use a `Connection::memory()` pair so we can inspect what the
    // server actually sends over the wire. The worker thread isn't spawned —
    // we drive `analyze_document` synchronously and feed the result directly
    // into `apply_analysis_result`, which is the same path the worker would
    // hit after a `select!` dispatch.

    use crate::analysis::analyze_document;
    use crate::state::{ServerConfig, ServerState, WorkspaceSymbolCache};
    use lsp_server::Connection;
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};

    fn make_state() -> ServerState {
        let (analysis_tx, _rx) = crossbeam_channel::unbounded();
        ServerState {
            documents: HashMap::new(),
            workspace_root: None,
            workspace_roots: Vec::new(),
            config: ServerConfig::default(),
            import_cache: Arc::new(Mutex::new(HashMap::new())),
            workspace_diag_cache: HashMap::new(),
            workspace_diag_clock: 0,
            workspace_symbol_cache: Arc::new(Mutex::new(WorkspaceSymbolCache::default())),
            pending_sources: HashMap::new(),
            analysis_tx,
            reverse_imports: HashMap::new(),
            inference_cache: Arc::new(Mutex::new(HashMap::new())),
            semantic_token_cache: HashMap::new(),
            semantic_token_counter: 0,
            published_lsp_diagnostics: HashMap::new(),
            client_supports_diagnostic_refresh: false,
            diagnostic_refresh_counter: 0,
            workspace_diag_reported: HashSet::new(),
        }
    }

    /// Drain all queued publishDiagnostics notifications from the client side
    /// of a memory connection. Returns one entry per notification: (uri,
    /// version, diagnostic count, list of message prefixes). Non-publish
    /// messages (capability registrations etc.) are ignored.
    fn drain_publishes(client: &Connection) -> Vec<(String, Option<i32>, usize, Vec<String>)> {
        let mut out = Vec::new();
        while let Ok(msg) = client.receiver.try_recv() {
            if let Message::Notification(n) = msg {
                if n.method == lsp_types::notification::PublishDiagnostics::METHOD {
                    if let Ok(p) = serde_json::from_value::<
                        lsp_types::PublishDiagnosticsParams,
                    >(n.params)
                    {
                        let msgs: Vec<String> = p
                            .diagnostics
                            .iter()
                            .map(|d| d.message.chars().take(60).collect())
                            .collect();
                        out.push((p.uri.as_str().to_string(), p.version, p.diagnostics.len(), msgs));
                    }
                }
            }
        }
        out
    }

    /// One-shot helper: analyze `source`, build an `AnalysisResult`, set
    /// up `pending_sources` so the apply path doesn't early-return, and
    /// invoke `apply_analysis_result` against the given server connection.
    fn apply_analysis_in(
        state: &mut ServerState,
        conn: &Connection,
        uri: &Uri,
        source: &str,
        version: Option<i32>,
    ) {
        let mut import_cache_local = match state.import_cache.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        };
        let mut inf_cache_local = match state.inference_cache.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        };
        let doc = analyze_document(uri, source, &mut import_cache_local, &mut inf_cache_local);
        state.pending_sources.insert(
            uri.clone(),
            crate::state::PendingSource {
                source: source.to_string(),
                version,
            },
        );
        let result = AnalysisResult {
            uri: uri.clone(),
            version,
            doc,
        };
        apply_analysis_result(state, conn, result);
    }

    const BAD_SOURCE: &str = r#"type Msg = {id: Int, text: Text}

*messages : [Msg]

removeWhere = \xs pred -> do
  m <- xs
  where not (pred m)
  yield m

main = do
  mssgs <- *messages
  replace *messages = removeWhere mssgs (\m -> m.text == "spam")
  yield {}
"#;

    const GOOD_SOURCE: &str = r#"type Msg = {id: Int, text: Text}

*messages : [Msg]

removeWhere = \xs pred -> do
  m <- xs
  where not (pred m)
  yield m

main = do
  mssgs <- *messages
  *messages = removeWhere mssgs (\m -> m.text == "spam")
  yield {}
"#;

    /// Bad source → exactly one publishDiagnostics with the "is unnecessary"
    /// error. Locks down the basic publish-on-error path.
    #[test]
    fn apply_analysis_result_publishes_error_diags() {
        let (server, client) = Connection::memory();
        let mut state = make_state();
        let uri = Uri::from_str("file:///test/repro.knot").unwrap();
        apply_analysis_in(&mut state, &server, &uri, BAD_SOURCE, Some(1));
        let pubs = drain_publishes(&client);
        assert_eq!(pubs.len(), 1, "expected one publish; got: {:?}", pubs);
        assert_eq!(pubs[0].2, 1, "expected one diagnostic");
        assert!(
            pubs[0].3[0].contains("is unnecessary"),
            "expected `is unnecessary` message; got {:?}",
            pubs[0].3
        );
    }

    /// Bad → good must clear the diagnostic with an empty publish. This is
    /// the path the user reported as broken; the test pins it.
    #[test]
    fn apply_analysis_result_clears_diags_on_fix() {
        let (server, client) = Connection::memory();
        let mut state = make_state();
        let uri = Uri::from_str("file:///test/repro.knot").unwrap();
        apply_analysis_in(&mut state, &server, &uri, BAD_SOURCE, Some(1));
        apply_analysis_in(&mut state, &server, &uri, GOOD_SOURCE, Some(2));
        let pubs = drain_publishes(&client);
        assert_eq!(pubs.len(), 2, "expected two publishes; got: {:?}", pubs);
        assert_eq!(pubs[1].2, 0, "expected the fix-publish to be empty");
        assert_eq!(pubs[1].1, Some(2), "expected v2 on the fix-publish");
    }

    /// Re-applying the same analysis output (e.g. a whitespace edit that
    /// went through the fingerprint cache) must NOT publish again. Verifies
    /// the dedup short-circuit using direct `Vec<Diagnostic>` equality.
    #[test]
    fn publish_dedup_skips_identical_diags() {
        let (server, client) = Connection::memory();
        let mut state = make_state();
        let uri = Uri::from_str("file:///test/repro.knot").unwrap();
        apply_analysis_in(&mut state, &server, &uri, BAD_SOURCE, Some(1));
        apply_analysis_in(&mut state, &server, &uri, BAD_SOURCE, Some(2));
        let pubs = drain_publishes(&client);
        assert_eq!(
            pubs.len(),
            1,
            "second apply with identical diags must dedup; got: {:?}",
            pubs
        );
    }

    /// Removing a workspace folder must drop cache entries for files under
    /// the removed root — both the workspace diagnostic cache and the
    /// reverse-imports graph. Open files are kept regardless of root.
    #[test]
    fn prune_caches_drops_paths_outside_remaining_roots() {
        let mut state = make_state();
        // Set up two "roots": /a (kept) and /b (removed).
        let kept_root = PathBuf::from("/tmp/knot-prune-test/a");
        let removed_root = PathBuf::from("/tmp/knot-prune-test/b");
        state.workspace_roots = vec![kept_root.clone()];

        let kept_path = kept_root.join("kept.knot");
        let dropped_path = removed_root.join("dropped.knot");

        state.workspace_diag_cache.insert(
            kept_path.clone(),
            (0, Vec::new(), 0),
        );
        state.workspace_diag_cache.insert(
            dropped_path.clone(),
            (0, Vec::new(), 0),
        );
        state
            .reverse_imports
            .entry(kept_path.clone())
            .or_default()
            .insert(dropped_path.clone());
        state
            .reverse_imports
            .entry(dropped_path.clone())
            .or_default()
            .insert(kept_path.clone());

        prune_caches_outside_roots(&mut state);

        assert!(state.workspace_diag_cache.contains_key(&kept_path));
        assert!(!state.workspace_diag_cache.contains_key(&dropped_path));
        assert!(state.reverse_imports.contains_key(&kept_path));
        assert!(!state.reverse_imports.contains_key(&dropped_path));
        // The remaining edge set on `kept_path` must also drop the stale
        // pointer to a now-out-of-scope importer.
        let importers = state.reverse_imports.get(&kept_path).unwrap();
        assert!(!importers.contains(&dropped_path));
    }

    /// Closing a document publishes empty diagnostics and drops the cache,
    /// so a subsequent reopen with the same content actually re-publishes.
    #[test]
    fn close_drops_cache_so_reopen_republishes() {
        let (server, client) = Connection::memory();
        let mut state = make_state();
        let uri = Uri::from_str("file:///test/repro.knot").unwrap();
        apply_analysis_in(&mut state, &server, &uri, BAD_SOURCE, Some(1));
        // Simulate didClose without going through the full notification
        // dispatcher: we only need the state-cleanup half here.
        state.documents.remove(&uri);
        state.pending_sources.remove(&uri);
        state.published_lsp_diagnostics.remove(&uri);
        // Re-applying the same source must republish (cache was dropped).
        apply_analysis_in(&mut state, &server, &uri, BAD_SOURCE, Some(2));
        let pubs = drain_publishes(&client);
        assert_eq!(
            pubs.len(),
            2,
            "expected publish-on-open + publish-after-reopen; got: {:?}",
            pubs
        );
    }
}

