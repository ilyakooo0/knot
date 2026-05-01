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
use lsp_types::*;

use crate::analysis::{analysis_worker, publish_diagnostics};
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
    send_response, AnalysisResult, AnalysisTask, PendingSource, ServerConfig, ServerState,
    WorkspaceSymbolCache,
};
use crate::utils::{position_to_offset, uri_to_path};
use crate::workspace_diagnostics::{handle_workspace_diagnostics, prune_stale_workspace_diag_cache};
use crate::workspace_symbol::handle_workspace_symbol;

// ── Entry point ─────────────────────────────────────────────────────

fn main() {
    eprintln!("knot-lsp starting...");

    let (connection, io_threads) = Connection::stdio();

    let server_capabilities = serde_json::to_value(ServerCapabilities {
        text_document_sync: Some(TextDocumentSyncCapability::Kind(
            TextDocumentSyncKind::INCREMENTAL,
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
            retrigger_characters: None,
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
            file_operations: None,
        }),
        ..Default::default()
    })
    .expect("server capabilities are static and always serialize cleanly");

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
        workspace_symbol_cache: WorkspaceSymbolCache::default(),
        pending_sources: HashMap::new(),
        analysis_tx,
        reverse_imports: HashMap::new(),
        inference_cache,
        semantic_token_cache: HashMap::new(),
        semantic_token_counter: 0,
    };

    // Register for file watcher notifications (.knot files). Build the
    // request defensively: if any payload fails to serialize (this should
    // never happen for these static structs, but handling it costs nothing),
    // skip the registration rather than panicking.
    if let Some(register_request) = build_file_watcher_registration() {
        let _ = connection.sender.send(Message::Request(register_request));
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
                        handle_request(&mut state, &connection, req);
                    }
                    Message::Notification(not) => {
                        handle_notification(&mut state, &connection, not);
                    }
                    Message::Response(_) => {}
                }
            }
            recv(results_rx) -> result => {
                let result = match result {
                    Ok(r) => r,
                    Err(_) => break 'outer,
                };
                apply_analysis_result(&mut state, &connection, result);
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

    publish_diagnostics(conn, &result.uri, &result.doc, result.version);

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
        // a changed decl name. Without this, every keystroke on a popular
        // utility module re-analyzes its entire dependency closure even
        // when the user is just editing a function body that no consumer
        // depends on directly.
        let changed: HashSet<&str> = result
            .doc
            .changed_decl_names
            .iter()
            .map(|s| s.as_str())
            .collect();
        if !changed.is_empty() {
            requeue_dependents_for_changed_decls(state, &result.uri, &this_path, &changed);
        }
    }

    state.documents.insert(result.uri, result.doc);
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
    } else if let Some(params) = cast_request::<request::WorkspaceDiagnosticRequest>(&req) {
        let result = handle_workspace_diagnostics(state, &params);
        send_response(conn, id, result);
        // Periodically prune the workspace diagnostics cache to avoid
        // unbounded growth when files are deleted from disk.
        prune_stale_workspace_diag_cache(state);
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
        for change in params.content_changes {
            if let Some(range) = change.range {
                let start = position_to_offset(&source, range.start);
                let end = position_to_offset(&source, range.end);
                source.replace_range(start..end, &change.text);
            } else {
                source = change.text;
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

            for (dep_uri, _, dep_source) in dependents {
                queue_analysis(state, dep_uri, dep_source, None);
            }
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

