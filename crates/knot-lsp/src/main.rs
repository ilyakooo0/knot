// `lsp_types::Uri` (fluent-uri backed) carries interior mutability but is only
// ever used immutably as a map/set key throughout the server, so the lint fires
// as a false positive across every `HashMap<Uri, _>`/`HashSet<Uri>`.
#![allow(clippy::mutable_key_type)]

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
use crate::document_symbol::handle_document_symbol;
use crate::folding::handle_folding_range;
use crate::formatting::{handle_formatting, handle_on_type_formatting, handle_range_formatting};
use crate::goto::{handle_goto_definition, handle_goto_type_definition};
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
    send_internal_error, send_invalid_params, send_method_not_found, send_response,
    AnalysisResult, AnalysisTask, PendingSource, ServerConfig, ServerState, WorkspaceSymbolCache,
};
use crate::utils::{offset_to_position, position_to_offset, uri_to_path};
use crate::workspace_diagnostics::{
    handle_document_diagnostics, handle_workspace_diagnostics, prune_stale_workspace_diag_cache,
};
use crate::workspace_symbol::handle_workspace_symbol;

/// One diagnostic's primary byte range plus the byte ranges of its related
/// information entries that live in the same file. Used by the
/// `didChange` handler to rebase cached diagnostics across in-flight edits.
type DiagByteRanges = Vec<(usize, usize, Vec<(usize, usize)>)>;

// ── Entry point ─────────────────────────────────────────────────────

fn main() {
    eprintln!("knot-lsp starting...");

    let (connection, io_threads) = Connection::stdio();

    let server_capabilities = serde_json::to_value(ServerCapabilities {
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
            ..Default::default()
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
                // Canonicalize so `prune_caches_outside_roots`'s
                // `p.starts_with(root)` check works on symlinked setups where
                // cache keys (canonicalized) would otherwise never match
                // non-canonical roots. Fall back to the raw path if
                // canonicalization fails (e.g. the folder doesn't exist yet).
                .map(|p| p.canonicalize().unwrap_or(p))
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
    // Bounded so a misbehaving client can't grow the queue without limit.
    // Tasks carry a clone of the file source; coalescing in the worker means
    // dropping on overflow is safe (the next didChange supersedes anyway).
    let (analysis_tx, analysis_rx) =
        crossbeam_channel::bounded::<AnalysisTask>(state::ANALYSIS_QUEUE_CAPACITY);
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
        dropped_analysis_retry: HashMap::new(),
        analysis_tx,
        inference_cache,
        semantic_token_cache: HashMap::new(),
        semantic_token_counter: 0,
        published_lsp_diagnostics: HashMap::new(),
        client_supports_diagnostic_refresh,
        diagnostic_refresh_counter: 0,
        document_versions: HashMap::new(),
        workspace_diag_reported: HashSet::new(),
    };

    // Register for file watcher notifications (.knot files). Build the
    // request defensively: if any payload fails to serialize (this should
    // never happen for these static structs, but handling it costs nothing),
    // skip the registration rather than panicking. A send failure here is
    // also non-fatal — the editor just won't push file-change events — but
    // log it so we don't silently lose cross-file invalidation if the
    // connection has gone bad.
    if let Some(register_request) = build_file_watcher_registration()
        && let Err(e) = connection.sender.send(Message::Request(register_request)) {
            eprintln!("knot-lsp: failed to register file watcher: {e}");
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
        // Spawn failures are rare (resource exhaustion) but worth logging:
        // without the prewarm thread, the first `workspace/symbol` query
        // walks the workspace from cold instead of finding a populated
        // cache — degraded UX, but not fatal.
        let spawn_result = thread::Builder::new()
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
            });
        if let Err(e) = spawn_result {
            eprintln!("knot-lsp: failed to spawn workspace indexer thread: {e}");
        }
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
        .filter_map(uri_to_path)
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
    import_cache: Arc<Mutex<crate::state::ImportCache>>,
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
            c.insert_capped(canonical, (on_disk_mtime, hash, entries));
        }
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
    //
    // Version to publish with. Defaults to the task's own version, but when a
    // pending entry carries the *same source* as this result, a no-op
    // `didChange` may have bumped the pending version past the task's without
    // queueing fresh work (see the `already_pending` branch in `didChange`).
    // In that case the pending version is the one the client's buffer is at, so
    // publish against it — otherwise version-checking clients (Helix) discard a
    // notification carrying the stale task version and the dedup cache then
    // suppresses every identical re-analysis, stranding the diagnostics.
    let mut publish_version = result.version;
    match state.pending_sources.get(&result.uri) {
        Some(pending) => {
            if pending.source != result.doc.source {
                return;
            }
            publish_version = pending.version;
            state.pending_sources.remove(&result.uri);
        }
        None => {
            // No pending edit means the live editor buffer matches the
            // last *analyzed* source. A result carrying different source
            // text is stale — e.g. the user edited (queueing a task) and
            // then undid back to the analyzed text before the worker
            // finished: the undo's didChange takes the `unchanged` early
            // return, which removes the pending entry without queueing a
            // fresh task. Applying the in-flight result here would make
            // `documents[uri].source` diverge from the editor buffer, so
            // every subsequent didChange range (computed by the client
            // against its own buffer) would be applied to the wrong text —
            // persistent corruption. Drop it instead.
            if let Some(current) = state.documents.get(&result.uri)
                && current.source != result.doc.source {
                    return;
                }
        }
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
    // Honor the `warnUnusedImports` config knob at the publish boundary (the
    // analysis pipeline emits the warnings unconditionally).
    let lsp_diags = crate::diagnostics::filter_unused_warnings(
        lsp_diags,
        state.config.warn_unused_imports,
    );

    // Update `state.documents` *before* publishing or sending the diagnostic
    // refresh. Pull-mode clients (JetBrains) react to the refresh by
    // immediately re-pulling via `textDocument/diagnostic`; that handler
    // reads `state.documents.knot_diagnostics`. If we sent the refresh
    // before this insert, the client would re-pull and get the stale prior
    // doc — which is exactly the bug this is meant to fix.
    let uri = result.uri.clone();
    let version = publish_version;
    // Snapshot whether the cached doc diagnostics actually moved. Pull-mode
    // clients sit on whatever they last pulled until we send a refresh, so
    // any change to `state.documents.knot_diagnostics` must trigger one —
    // even when the push-publish dedups out below. The dedup commonly
    // skips a fix-clearing publish because `didChange` already pre-rebased
    // `published_lsp_diagnostics` to the cleared list (which pull-mode
    // clients never saw), so the post-analysis refresh is the only signal
    // that gets a JetBrains gutter to drop the stale squiggle.
    let doc_diags_changed = match state.documents.get(&uri) {
        Some(prev) => prev.knot_diagnostics != result.doc.knot_diagnostics,
        None => !result.doc.knot_diagnostics.is_empty(),
    };
    state.documents.insert(result.uri, result.doc);
    // Track the version the stored analysis corresponds to — the
    // workspace-diagnostics pull handler reports it for open docs (the LSP
    // spec reserves `version: null` for files that are NOT open).
    if let Some(v) = version {
        state.document_versions.insert(uri.clone(), v);
    }

    let published = publish_diagnostics_dedup(state, conn, &uri, lsp_diags, version);
    if published || doc_diags_changed {
        // Pull-mode clients (notably JetBrains) ignore the publish above and
        // only refresh diagnostics when the server explicitly invalidates
        // their cache. Without this, a fix that clears a diagnostic stays
        // visible in the gutter until the user triggers another pull.
        request_workspace_diagnostic_refresh(state, conn);
    }

    // The worker just drained (at least) one task — re-queue anything that
    // was dropped on channel overflow so no file stays permanently stale.
    retry_dropped_analysis(state);
}

// ── Request dispatch ────────────────────────────────────────────────

/// Tristate result of attempting to decode a `Request` as a specific LSP
/// request type. Splitting "method matches but params don't deserialize" out
/// from "method doesn't match this handler" lets the dispatcher reply with
/// `InvalidParams` (-32602) instead of letting the request fall through to
/// the `MethodNotFound` (-32601) fallback or — worse, before the fallback
/// existed — be silently dropped while the client hangs on a response that
/// never comes.
enum Cast<T> {
    /// Method and params both matched; ready to invoke the handler.
    Matched(T),
    /// Method matched but the params payload failed to deserialize.
    Malformed(serde_json::Error),
    /// Different method — try the next handler.
    Other,
}

fn cast_request<R: request::Request>(req: &Request) -> Cast<R::Params> {
    if req.method != R::METHOD {
        return Cast::Other;
    }
    match serde_json::from_value(req.params.clone()) {
        Ok(params) => Cast::Matched(params),
        Err(e) => Cast::Malformed(e),
    }
}

/// Dispatch a single request to its handler.
///
/// Each `try_handle!` arm runs `cast_request` for one LSP request type:
/// - On `Matched`, it evaluates the handler expression, sends the response,
///   and returns from the enclosing function.
/// - On `Malformed`, it logs and replies with `InvalidParams`, then returns.
/// - On `Other`, it falls through so the next arm can try its method.
///
/// The closure-style `|p| ...` syntax keeps each call site to a single line
/// while letting the body capture `state` mutably; the borrow ends when the
/// macro expansion returns from the enclosing function (Matched/Malformed)
/// or releases at the next statement (Other).
macro_rules! try_handle {
    ($req:expr, $conn:expr, $req_ty:ty, |$params:ident| $body:expr) => {
        match cast_request::<$req_ty>($req) {
            Cast::Matched($params) => {
                let __result = $body;
                send_response($conn, $req.id.clone(), __result);
                return;
            }
            Cast::Malformed(__e) => {
                eprintln!(
                    "knot-lsp: malformed `{}` params: {}",
                    <$req_ty as request::Request>::METHOD,
                    __e
                );
                send_invalid_params(
                    $conn,
                    $req.id.clone(),
                    <$req_ty as request::Request>::METHOD,
                    &__e.to_string(),
                );
                return;
            }
            Cast::Other => {}
        }
    };
}

fn handle_request(state: &mut ServerState, conn: &Connection, req: Request) {
    try_handle!(&req, conn, request::DocumentSymbolRequest, |p| handle_document_symbol(state, &p));
    try_handle!(&req, conn, request::GotoDefinition, |p| handle_goto_definition(state, &p));
    try_handle!(&req, conn, request::GotoTypeDefinition, |p| handle_goto_type_definition(state, &p));
    try_handle!(&req, conn, request::HoverRequest, |p| handle_hover(state, &p));
    try_handle!(&req, conn, request::Completion, |p| handle_completion(state, &p));
    try_handle!(&req, conn, request::References, |p| handle_references(state, &p));
    try_handle!(&req, conn, request::PrepareRenameRequest, |p| handle_prepare_rename(state, &p));
    try_handle!(&req, conn, request::Rename, |p| handle_rename(state, &p));
    try_handle!(&req, conn, request::InlayHintRequest, |p| handle_inlay_hint(state, &p));
    try_handle!(&req, conn, request::SignatureHelpRequest, |p| handle_signature_help(state, &p));
    try_handle!(&req, conn, request::CodeLensRequest, |p| handle_code_lens(state, &p));
    try_handle!(&req, conn, request::SemanticTokensFullRequest, |p| handle_semantic_tokens_full(state, &p));
    try_handle!(&req, conn, request::SemanticTokensFullDeltaRequest, |p| handle_semantic_tokens_full_delta(state, &p));
    try_handle!(&req, conn, request::SemanticTokensRangeRequest, |p| handle_semantic_tokens_range(state, &p));
    try_handle!(&req, conn, request::FoldingRangeRequest, |p| handle_folding_range(state, &p));
    try_handle!(&req, conn, request::SelectionRangeRequest, |p| handle_selection_range(state, &p));
    try_handle!(&req, conn, request::Formatting, |p| handle_formatting(state, &p));
    try_handle!(&req, conn, request::RangeFormatting, |p| handle_range_formatting(state, &p));
    try_handle!(&req, conn, request::OnTypeFormatting, |p| handle_on_type_formatting(state, &p));
    try_handle!(&req, conn, request::DocumentHighlightRequest, |p| handle_document_highlight(state, &p));
    try_handle!(&req, conn, request::CodeActionRequest, |p| handle_code_action(state, &p));
    // Keep workspace_symbol_cache from growing unbounded — pruning happens
    // inside the handler via the on-disk scan.
    try_handle!(&req, conn, request::WorkspaceSymbolRequest, |p| handle_workspace_symbol(state, &p));
    try_handle!(&req, conn, request::CallHierarchyPrepare, |p| handle_call_hierarchy_prepare(state, &p));
    try_handle!(&req, conn, request::CallHierarchyIncomingCalls, |p| handle_call_hierarchy_incoming(state, &p));
    try_handle!(&req, conn, request::CallHierarchyOutgoingCalls, |p| handle_call_hierarchy_outgoing(state, &p));
    try_handle!(&req, conn, request::ResolveCompletionItem, |p| handle_resolve_completion_item(state, p));
    try_handle!(&req, conn, request::LinkedEditingRange, |p| handle_linked_editing_range(state, &p));
    try_handle!(&req, conn, request::DocumentDiagnosticRequest, |p| handle_document_diagnostics(state, &p));
    // Workspace-diagnostic results piggyback a cache prune so deleted files
    // don't leave stale entries; bundle that side-effect into the handler
    // call here rather than splitting it out across the macro.
    match cast_request::<request::WorkspaceDiagnosticRequest>(&req) {
        Cast::Matched(params) => {
            let result = handle_workspace_diagnostics(state, &params);
            send_response(conn, req.id.clone(), result);
            prune_stale_workspace_diag_cache(state);
            return;
        }
        Cast::Malformed(e) => {
            eprintln!(
                "knot-lsp: malformed `{}` params: {e}",
                <request::WorkspaceDiagnosticRequest as request::Request>::METHOD
            );
            send_invalid_params(
                conn,
                req.id.clone(),
                <request::WorkspaceDiagnosticRequest as request::Request>::METHOD,
                &e.to_string(),
            );
            return;
        }
        Cast::Other => {}
    }
    // Fallback: every known method is handled above, so reaching here means
    // the client sent something we don't implement. Replying with
    // `MethodNotFound` (-32601) is mandatory — without a response the client
    // would block waiting for the request id forever.
    eprintln!("knot-lsp: unhandled request method `{}`", req.method);
    send_method_not_found(conn, req.id, &req.method);
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
        crate::state::enforce_uri_cache_cap(
            &mut state.pending_sources,
            &state.documents,
            crate::state::MAX_PENDING_SOURCES,
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
        // Convert each diagnostic's primary `Range` AND any `related_information`
        // ranges that live in this same file (all related-info entries emitted
        // by `diagnostics.rs` point at the diagnostic's own URI) to byte
        // offsets up front. After each edit the offsets are adjusted in place;
        // ranges that overlap an edit are tagged with `usize::MAX` and dropped
        // at the end. Rebuilding the rebased `Diagnostic` rewrites both the
        // primary range and the related-info locations, so "click to navigate"
        // targets stay correct during the debounce window before the next full
        // analysis.
        let mut diag_byte_ranges: Option<DiagByteRanges> =
            cached_diags
                .as_ref()
                .map(|ds| {
                    ds.iter()
                        .map(|d| {
                            let primary = (
                                position_to_offset(&source, d.range.start),
                                position_to_offset(&source, d.range.end),
                            );
                            let related = d
                                .related_information
                                .as_ref()
                                .map(|ris| {
                                    ris.iter()
                                        .filter(|ri| ri.location.uri == uri)
                                        .map(|ri| {
                                            (
                                                position_to_offset(&source, ri.location.range.start),
                                                position_to_offset(&source, ri.location.range.end),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default();
                            (primary.0, primary.1, related)
                        })
                        .collect()
                });

        for change in params.content_changes {
            if let Some(range) = change.range {
                let a = position_to_offset(&source, range.start);
                let b = position_to_offset(&source, range.end);
                // The LSP spec does not guarantee `range.start <= range.end`;
                // a buggy client can send an inverted range. Normalize it so
                // `replace_range`/`shift_byte_ranges_for_edit` don't panic on
                // `start > end` (which would unwind the whole didChange handler
                // under catch_unwind, silently desyncing the document forever).
                let (start, end) = if a <= b { (a, b) } else { (b, a) };
                let new_len = change.text.len();
                if let Some(diag_ranges) = diag_byte_ranges.as_mut() {
                    for (ps, pe, related) in diag_ranges {
                        // Shift the primary and all related-info ranges in one
                        // pass so they move consistently with the edit.
                        let mut all: Vec<(usize, usize)> =
                            std::iter::once((*ps, *pe)).chain(related.iter().copied()).collect();
                        shift_byte_ranges_for_edit(&mut all, start, end, new_len);
                        *ps = all[0].0;
                        *pe = all[0].1;
                        for (i, r) in related.iter_mut().enumerate() {
                            *r = all[i + 1];
                        }
                    }
                }
                source.replace_range(start..end, &change.text);
            } else {
                source = change.text;
                // Full replace invalidates every cached range — the document
                // structure no longer relates to the prior analysis output.
                if let Some(diag_ranges) = diag_byte_ranges.as_mut() {
                    for (ps, pe, related) in diag_ranges {
                        *ps = usize::MAX;
                        *pe = usize::MAX;
                        for r in related {
                            *r = (usize::MAX, usize::MAX);
                        }
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
                .filter_map(|(d, &(ps, pe, ref related))| {
                    if ps == usize::MAX || pe > source.len() {
                        return None;
                    }
                    let mut shifted = d.clone();
                    shifted.range = lsp_types::Range {
                        start: offset_to_position(&source, ps),
                        end: offset_to_position(&source, pe),
                    };
                    // Rebase related-info locations that live in this file.
                    // Entries pointing at other URIs (none are emitted today,
                    // but the guard keeps the rebase correct if that changes)
                    // are preserved verbatim — their target file's positions
                    // are unaffected by an edit in *this* file.
                    if let Some(orig_related) = d.related_information.as_ref() {
                        let mut shifted_related: Vec<lsp_types::DiagnosticRelatedInformation> =
                            Vec::with_capacity(orig_related.len());
                        let mut rel_iter = related.iter();
                        for ri in orig_related {
                            if ri.location.uri != uri {
                                shifted_related.push(ri.clone());
                            } else if let Some(&(rs, re)) = rel_iter.next()
                                && rs != usize::MAX && re <= source.len()
                            {
                                shifted_related.push(lsp_types::DiagnosticRelatedInformation {
                                    location: lsp_types::Location {
                                        uri: uri.clone(),
                                        range: lsp_types::Range {
                                            start: offset_to_position(&source, rs),
                                            end: offset_to_position(&source, re),
                                        },
                                    },
                                    message: ri.message.clone(),
                                });
                                // else: overlapped an edit — drop this related entry.
                            }
                        }
                        shifted.related_information = if shifted_related.is_empty() {
                            None
                        } else {
                            Some(shifted_related)
                        };
                    }
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
                crate::state::enforce_uri_cache_cap(
                    &mut state.published_lsp_diagnostics,
                    &state.documents,
                    crate::state::MAX_PUBLISHED_DIAGNOSTICS,
                );
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
        crate::state::enforce_uri_cache_cap(
            &mut state.pending_sources,
            &state.documents,
            crate::state::MAX_PENDING_SOURCES,
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
        // Track deletions separately: a deleted path needs its entries flushed
        // from every off-disk cache (parsed AST, reverse-import edges,
        // workspace symbol index) so the next request doesn't read stale data
        // for a file that no longer exists. Other change kinds — Created,
        // Changed — only invalidate inference; the parse cache is keyed by
        // content hash and naturally re-populates.
        let mut deleted_paths: HashSet<PathBuf> = HashSet::new();
        for c in &params.changes {
            if matches!(c.typ, FileChangeType::DELETED)
                && let Some(p) = uri_to_path(&c.uri) {
                    // `canonicalize` fails on paths the OS no longer knows
                    // about, so fall back to the lexical path. Either form is
                    // acceptable here — the cache keys are canonical paths
                    // captured when the file was first analyzed, and we'll
                    // try both shapes when evicting.
                    deleted_paths.insert(p.canonicalize().unwrap_or(p));
                }
        }
        let changed_paths: HashSet<PathBuf> = params
            .changes
            .iter()
            .filter_map(|c| uri_to_path(&c.uri))
            .filter_map(|p| p.canonicalize().ok())
            .collect();

        if !changed_paths.is_empty() || !deleted_paths.is_empty() {
            // Evict cached inference snapshots for the changed paths — the
            // on-disk bytes moved, so any prior snapshot is stale.
            if let Ok(mut cache) = state.inference_cache.lock() {
                let affected: HashSet<&PathBuf> = changed_paths
                    .iter()
                    .chain(deleted_paths.iter())
                    .collect();
                cache.retain(|(p, _), _| !affected.contains(p));
            }

            // Any unopened-file diagnostics for the changed file are stale
            // now. Without eager invalidation, the next workspace-diag
            // request would replay last run's diagnostics.
            let mut diag_invalidate = changed_paths.clone();
            diag_invalidate.extend(deleted_paths.iter().cloned());
            invalidate_workspace_diag_cache_for(state, &diag_invalidate);

            // Hard-evict deleted files from every cache that's keyed by a
            // disk path. Without this, the parsed AST and the
            // workspace-symbol entries for a removed file outlive the file
            // itself for the rest of the session — slow leaks plus the risk
            // of handlers handing back data anchored to bytes that no longer
            // exist on disk.
            if !deleted_paths.is_empty() {
                if let Ok(mut cache) = state.import_cache.lock() {
                    cache.retain(|p, _| !deleted_paths.contains(p));
                }
                if let Ok(mut sym) = state.workspace_symbol_cache.lock() {
                    sym.by_path.retain(|p, _| !deleted_paths.contains(p));
                }
                state
                    .workspace_diag_cache
                    .retain(|p, _| !deleted_paths.contains(p));
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
        // When a pending (newer) source exists, the analyzed `doc.source` is
        // stale relative to the editor buffer: republishing diagnostics
        // rendered against it would clobber the rebased positions didChange
        // already pushed. Skip the backstop — the in-flight analysis will
        // publish fresh diagnostics for the pending text shortly.
        if state.pending_sources.contains_key(&uri) {
            return;
        }
        if let Some(doc) = state.documents.get(&uri) {
            let lsp_diags: Vec<Diagnostic> = doc
                .knot_diagnostics
                .iter()
                .filter_map(|d| crate::diagnostics::to_lsp_diagnostic(d, &doc.source, &uri))
                .collect();
            let lsp_diags = crate::diagnostics::filter_unused_warnings(
                lsp_diags,
                state.config.warn_unused_imports,
            );
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
        state.document_versions.remove(&params.text_document.uri);
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
            .filter_map(|f| uri_to_path(&f.uri).and_then(|p| p.canonicalize().ok().or(Some(p))))
            .collect();
        state.workspace_roots.retain(|p| !removed.contains(p));
        for added in &params.event.added {
            if let Some(path) = uri_to_path(&added.uri) {
                // Canonicalize added roots to match cache-key canonicalization,
                // and do so *before* the dedup check: existing roots are stored
                // canonicalized, so a symlinked spelling of an already-present
                // root would slip past a raw-path comparison and get pushed
                // again, causing duplicate workspace scans.
                let path = path.canonicalize().unwrap_or(path);
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
    crate::state::enforce_uri_cache_cap(
        &mut state.published_lsp_diagnostics,
        &state.documents,
        crate::state::MAX_PUBLISHED_DIAGNOSTICS,
    );
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

/// Send an analysis task to the worker. The channel is bounded
/// (`ANALYSIS_QUEUE_CAPACITY`); we use `try_send` so a runaway client can't
/// block the main event loop on a full queue. Two distinct failure modes:
///
/// - `Full`: the worker is behind. The task is parked in
///   `dropped_analysis_retry` and re-queued after the next analysis
///   completes (the worker has freed at least one slot by then). Without
///   the retry, the file stayed permanently stale: `pending_sources` keeps
///   the new text, so any older in-flight result for the URI is discarded
///   by the `pending.source != result.doc.source` guard and diagnostics/
///   hover freeze until the next edit.
/// - `Disconnected`: the worker thread has died. Other features still work
///   against the last good analysis, so log and continue rather than crash.
///
fn queue_analysis(state: &mut ServerState, uri: Uri, source: String, version: Option<i32>) {
    use crossbeam_channel::TrySendError;
    // Anything parked from an earlier overflow is superseded by the fresher
    // task we're about to enqueue (and re-parked below if this one is
    // dropped too).
    state.dropped_analysis_retry.remove(&uri);
    match state.analysis_tx.try_send(AnalysisTask { uri, source, version }) {
        Ok(()) => {}
        Err(TrySendError::Full(task)) => {
            eprintln!(
                "knot-lsp: analysis queue full ({} tasks); will re-queue `{}` after the next analysis completes",
                state::ANALYSIS_QUEUE_CAPACITY,
                task.uri.as_str()
            );
            state
                .dropped_analysis_retry
                .insert(task.uri, (task.source, task.version));
            crate::state::enforce_uri_cache_cap(
                &mut state.dropped_analysis_retry,
                &state.documents,
                crate::state::MAX_PENDING_SOURCES,
            );
        }
        Err(TrySendError::Disconnected(_)) => {
            eprintln!("knot-lsp: analysis worker channel closed");
        }
    }
}

/// Re-queue analysis tasks that were dropped on channel overflow. Called
/// after each completed analysis (the worker has drained at least one slot,
/// so there's room again). Prefers the freshest `pending_sources` text over
/// the snapshot captured at drop time. If the queue fills again mid-drain,
/// the remaining entries are parked back for the next completion.
fn retry_dropped_analysis(state: &mut ServerState) {
    if state.dropped_analysis_retry.is_empty() {
        return;
    }
    let entries: Vec<(Uri, (String, Option<i32>))> =
        state.dropped_analysis_retry.drain().collect();
    for (uri, (source, version)) in entries {
        let (src, ver) = match state.pending_sources.get(&uri) {
            Some(p) => (p.source.clone(), p.version),
            None => (source, version),
        };
        queue_analysis(state, uri, src, ver);
    }
}

/// Eagerly evict workspace-diagnostic cache entries for `changed`. Without
/// this, the cache can hand stale diagnostics to the editor between a file
/// edit and the next pull-mode `workspace/diagnostic` request — the lazy
/// `prune_stale_…` pass only runs on workspace-diag requests.
fn invalidate_workspace_diag_cache_for(state: &mut ServerState, changed: &HashSet<PathBuf>) {
    if changed.is_empty() {
        return;
    }
    state
        .workspace_diag_cache
        .retain(|path, _| !changed.contains(path));
}



