//! Pull-model diagnostic handlers — both `textDocument/diagnostic` (single
//! document) and `workspace/diagnostic` (whole workspace).
//!
//! The single-document handler is a thin wrapper over the cached `DocumentState`:
//! analysis already runs on every change, so the pull request just maps the
//! cached `knot::diagnostic::Diagnostic` list into LSP form. For files we
//! haven't analyzed yet (the editor pulls before our `didOpen` analysis
//! finishes) we run the full pipeline synchronously, mirroring the
//! workspace handler's unopened-file path.
//!
//! The workspace handler additionally reports diagnostics for unopened files
//! by running the full pipeline (lex → parse → type infer → effect infer →
//! stratify → SQL lint) on each `.knot` file in the workspace.

use std::collections::HashSet;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use lsp_types::*;

use knot::ast::Module;
use knot::diagnostic;

use crate::analysis::get_or_parse_file_shared;
use crate::diagnostics::to_lsp_diagnostic;
use crate::shared::scan_knot_files_in_roots;
use crate::state::{content_hash, ServerState};
use crate::utils::{path_to_uri, uri_to_path};

// ── Document Diagnostics (Pull Model, per-doc) ──────────────────────

/// `textDocument/diagnostic` — pull-model diagnostics for a single document.
/// Returns the cached analysis output when the document is open; falls back
/// to a synchronous analysis pass for unopened files (the editor can request
/// diagnostics on a file it has not opened yet — typically when navigating
/// to a workspace symbol or via a code-lens follow-up).
pub(crate) fn handle_document_diagnostics(
    state: &mut ServerState,
    params: &DocumentDiagnosticParams,
) -> DocumentDiagnosticReportResult {
    let uri = &params.text_document.uri;

    let items: Vec<Diagnostic> = if let Some(doc) = state.documents.get(uri) {
        // Hot path: analysis has already run on this file — reuse the cached
        // diagnostics. This is the common case — the editor pulls right after
        // a didChange that already produced a result.
        doc.knot_diagnostics
            .iter()
            .filter_map(|d| to_lsp_diagnostic(d, &doc.source, uri))
            .collect()
    } else if let Some(path) = uri_to_path(uri).and_then(|p| p.canonicalize().ok()) {
        // Cold path: file isn't open in the editor. Honor the pull anyway —
        // editors send `textDocument/diagnostic` for files surfaced via
        // workspace-symbol or goto-definition that haven't been opened yet.
        // Reuse the cached unopened-file diagnostics when available; otherwise
        // run the full pipeline and seed the cache.
        let source = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(_) => {
                return DocumentDiagnosticReportResult::Report(
                    DocumentDiagnosticReport::Full(empty_full_report()),
                );
            }
        };
        let hash = content_hash(&source);
        // Pull the cached result without holding the immutable borrow across
        // the cache-update calls below — we only need to know whether the
        // cached hash matches and, if so, what diagnostics to return.
        let cached_match = state
            .workspace_diag_cache
            .get(&path)
            .filter(|(cached_h, _, _, _)| *cached_h == hash)
            .map(|(_, diags, _, _)| diags.clone());
        if let Some(diags) = cached_match {
            state.workspace_diag_clock = state.workspace_diag_clock.wrapping_add(1);
            let access = state.workspace_diag_clock;
            // Refresh the recorded mtime to the current disk mtime: we just
            // verified content matches, so future prune/Phase-A passes can
            // take the mtime fast-path even if `jj`/`git` touched the file
            // without changing its bytes.
            let disk_mtime = current_mtime(&path);
            if let Some(entry) = state.workspace_diag_cache.get_mut(&path) {
                entry.2 = access;
                if disk_mtime.is_some() {
                    entry.3 = disk_mtime;
                }
            }
            diags
        } else {
            analyze_and_cache(state, &path, &source, hash, uri)
        }
    } else {
        Vec::new()
    };

    DocumentDiagnosticReportResult::Report(DocumentDiagnosticReport::Full(
        RelatedFullDocumentDiagnosticReport {
            related_documents: None,
            full_document_diagnostic_report: FullDocumentDiagnosticReport {
                result_id: None,
                items,
            },
        },
    ))
}

fn empty_full_report() -> RelatedFullDocumentDiagnosticReport {
    RelatedFullDocumentDiagnosticReport {
        related_documents: None,
        full_document_diagnostic_report: FullDocumentDiagnosticReport {
            result_id: None,
            items: Vec::new(),
        },
    }
}

/// Analyze an unopened file synchronously and seed the workspace-diag cache
/// with its result. Pulled out so the cache-hit and cache-miss paths share
/// the cache-write code.
fn analyze_and_cache(
    state: &mut ServerState,
    path: &Path,
    source: &str,
    hash: u64,
    uri: &Uri,
) -> Vec<Diagnostic> {
    let module = match get_or_parse_file_shared(path, &state.import_cache) {
        Some((m, _)) => m,
        None => return Vec::new(),
    };
    let diags = analyze_unopened_file(&module, source, path, uri);
    state.workspace_diag_clock = state.workspace_diag_clock.wrapping_add(1);
    let access = state.workspace_diag_clock;
    let mtime = current_mtime(path);
    state
        .workspace_diag_cache
        .insert(path.to_path_buf(), (hash, diags.clone(), access, mtime));
    // Per-document pulls that never fire a `workspace/diagnostic` would
    // otherwise let the cache grow unbounded — full pruning runs only after
    // workspace pulls. The cap-only eviction here skips the disk-read
    // invalidation step (too expensive on every textDocument/diagnostic) and
    // just trims the oldest entries when over budget.
    enforce_workspace_diag_cap(state);
    diags
}

/// LRU cap eviction without disk-read invalidation. Cheap enough to call on
/// every per-document diagnostic pull. Invariant kept: cache size ≤ cap after
/// return. The full `prune_stale_workspace_diag_cache` is still the
/// authoritative cleanup — it additionally re-validates content against disk.
fn enforce_workspace_diag_cap(state: &mut ServerState) {
    let cap = state.config.max_workspace_diag_cache;
    if state.workspace_diag_cache.len() <= cap {
        return;
    }
    let mut by_age: Vec<(PathBuf, u64)> = state
        .workspace_diag_cache
        .iter()
        .map(|(p, (_, _, access, _))| (p.clone(), *access))
        .collect();
    by_age.sort_by_key(|(_, a)| *a);
    let to_drop = state.workspace_diag_cache.len().saturating_sub(cap);
    for (p, _) in by_age.into_iter().take(to_drop) {
        state.workspace_diag_cache.remove(&p);
    }
}

/// Read the on-disk modification time for `path`. Returns `None` on any I/O
/// error (file gone, permission denied) or on filesystems that don't expose
/// mtime; callers fall back to the slower hash-based verification.
fn current_mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

// ── Workspace Diagnostics (Pull Model) ──────────────────────────────

pub(crate) fn handle_workspace_diagnostics(
    state: &mut ServerState,
    _params: &WorkspaceDiagnosticParams,
) -> WorkspaceDiagnosticReportResult {
    let mut items = Vec::new();

    // Snapshot last pull's reported-with-errors set, then rebuild it as we go.
    // Files that were in the prior set but now have empty diagnostics must be
    // re-emitted with an empty list — clients treat absent URIs in a workspace
    // report as unchanged, so a fix that clears errors stays visible until the
    // server explicitly emits the empty list.
    let prev_reported: HashSet<Uri> =
        std::mem::take(&mut state.workspace_diag_reported);
    let mut now_reported: HashSet<Uri> = HashSet::new();

    for (uri, doc) in &state.documents {
        let lsp_diags: Vec<Diagnostic> = doc
            .knot_diagnostics
            .iter()
            .filter_map(|d| to_lsp_diagnostic(d, &doc.source, uri))
            .collect();

        if !lsp_diags.is_empty() {
            now_reported.insert(uri.clone());
        }
        if !lsp_diags.is_empty() || prev_reported.contains(uri) {
            items.push(WorkspaceDocumentDiagnosticReport::Full(
                WorkspaceFullDocumentDiagnosticReport {
                    uri: uri.clone(),
                    version: None,
                    full_document_diagnostic_report: FullDocumentDiagnosticReport {
                        result_id: None,
                        items: lsp_diags,
                    },
                },
            ));
        }
    }

    // Also scan workspace files not currently open. We run the full pipeline
    // (lex → parse → type infer → effect infer → stratify → SQL lint) so
    // cross-file errors surface even when a file isn't open in the editor.
    //
    // Analysis is parallelized across all CPUs using `std::thread::scope` —
    // each unopened-file pipeline is independent (no shared mutable state
    // beyond the import cache, which is mutex-protected). Speeds up the
    // first workspace-diagnostics call on cold caches by roughly the number
    // of cores.
    {
        let open_paths: HashSet<PathBuf> = state
            .documents
            .keys()
            .filter_map(|u| uri_to_path(u))
            .filter_map(|p| p.canonicalize().ok())
            .collect();

        let files = scan_knot_files_in_roots(
            &state.workspace_roots,
            state.workspace_root.as_deref(),
        );
        if !files.is_empty() {
            // Phase A: cheaply collect the work list — paths to analyze, their
            // current source/module, content hash, and any cached diagnostics.
            // Cached entries skip the parallel pass entirely.
            struct WorkItem {
                canonical: PathBuf,
                file_uri: Uri,
                hash: u64,
                module: Module,
                source: String,
            }
            let mut to_analyze: Vec<WorkItem> = Vec::new();
            let mut cached_results: Vec<(Uri, Vec<Diagnostic>, PathBuf)> = Vec::new();
            // mtime hits we should refresh after the read+hash fallback
            // confirmed content was unchanged (jj/git checkout case).
            let mut mtime_refreshes: Vec<(PathBuf, SystemTime)> = Vec::new();
            for file_path in files {
                let canonical = match file_path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if open_paths.contains(&canonical) {
                    continue;
                }
                let file_uri = match path_to_uri(&canonical) {
                    Some(u) => u,
                    None => continue,
                };

                // Mtime fast path: if the on-disk mtime matches the mtime
                // recorded with the cache entry, the bytes can't have
                // changed since we last verified them — reuse the cached
                // diagnostics without reading or hashing the file. Saves
                // O(workspace_size) disk reads per workspace pull.
                let disk_mtime = current_mtime(&canonical);
                if let Some(dm) = disk_mtime {
                    if let Some((_, cached, _, Some(cached_mtime))) =
                        state.workspace_diag_cache.get(&canonical)
                    {
                        if *cached_mtime == dm {
                            cached_results.push((
                                file_uri,
                                cached.clone(),
                                canonical.clone(),
                            ));
                            continue;
                        }
                    }
                }

                // Mtime missing or moved — fall through to read+hash.
                let (module, source) =
                    match get_or_parse_file_shared(&canonical, &state.import_cache) {
                        Some(v) => v,
                        None => continue,
                    };
                let hash = content_hash(&source);
                if let Some((cached_h, cached, _, _)) =
                    state.workspace_diag_cache.get(&canonical)
                {
                    if *cached_h == hash {
                        // Content is unchanged but the mtime moved (typical
                        // after `jj` / `git` checkouts). Schedule a mtime
                        // refresh so the fast path applies on the next pull.
                        if let Some(dm) = disk_mtime {
                            mtime_refreshes.push((canonical.clone(), dm));
                        }
                        cached_results.push((file_uri, cached.clone(), canonical.clone()));
                        continue;
                    }
                }
                to_analyze.push(WorkItem {
                    canonical,
                    file_uri,
                    hash,
                    module,
                    source,
                });
            }
            for (path, mtime) in mtime_refreshes {
                if let Some(entry) = state.workspace_diag_cache.get_mut(&path) {
                    entry.3 = Some(mtime);
                }
            }

            // Phase B: parallel analysis. `analyze_unopened_file` allocates its
            // own type/effect/stratify/sql-lint state per call, so the only
            // shared resource is the import cache (already Arc<Mutex<>>). We
            // batch into chunks roughly proportional to core count to keep
            // dispatch overhead small.
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            let chunk_size = ((to_analyze.len() + cores - 1) / cores).max(1);

            // Move into per-chunk Vec<WorkItem> so each worker owns its slice.
            let mut chunks: Vec<Vec<WorkItem>> = Vec::new();
            let mut buf: Vec<WorkItem> = Vec::with_capacity(chunk_size);
            for w in to_analyze {
                buf.push(w);
                if buf.len() >= chunk_size {
                    chunks.push(std::mem::take(&mut buf));
                    buf.reserve(chunk_size);
                }
            }
            if !buf.is_empty() {
                chunks.push(buf);
            }

            let mut analysis_results: Vec<(PathBuf, Uri, u64, Vec<Diagnostic>)> = Vec::new();
            std::thread::scope(|s| {
                let handles: Vec<_> = chunks
                    .into_iter()
                    .map(|chunk| {
                        s.spawn(move || {
                            let mut out = Vec::with_capacity(chunk.len());
                            for w in chunk {
                                let diags = analyze_unopened_file(
                                    &w.module,
                                    &w.source,
                                    &w.canonical,
                                    &w.file_uri,
                                );
                                out.push((w.canonical, w.file_uri, w.hash, diags));
                            }
                            out
                        })
                    })
                    .collect();
                for h in handles {
                    if let Ok(part) = h.join() {
                        analysis_results.extend(part);
                    }
                }
            });

            // Phase C: serialize cache writes and report assembly. Cheap.
            for (canonical, file_uri, hash, lsp_diags) in analysis_results {
                state.workspace_diag_clock = state.workspace_diag_clock.wrapping_add(1);
                let access = state.workspace_diag_clock;
                let mtime = current_mtime(&canonical);
                state
                    .workspace_diag_cache
                    .insert(canonical, (hash, lsp_diags.clone(), access, mtime));
                if !lsp_diags.is_empty() {
                    now_reported.insert(file_uri.clone());
                }
                if !lsp_diags.is_empty() || prev_reported.contains(&file_uri) {
                    items.push(WorkspaceDocumentDiagnosticReport::Full(
                        WorkspaceFullDocumentDiagnosticReport {
                            uri: file_uri,
                            version: None,
                            full_document_diagnostic_report: FullDocumentDiagnosticReport {
                                result_id: None,
                                items: lsp_diags,
                            },
                        },
                    ));
                }
            }
            for (file_uri, lsp_diags, canonical) in cached_results {
                // Bump LRU access counter on hit so frequently-touched files
                // stay resident through cap-based eviction.
                state.workspace_diag_clock = state.workspace_diag_clock.wrapping_add(1);
                let access = state.workspace_diag_clock;
                if let Some(entry) = state.workspace_diag_cache.get_mut(&canonical) {
                    entry.2 = access;
                }
                if !lsp_diags.is_empty() {
                    now_reported.insert(file_uri.clone());
                }
                if !lsp_diags.is_empty() || prev_reported.contains(&file_uri) {
                    items.push(WorkspaceDocumentDiagnosticReport::Full(
                        WorkspaceFullDocumentDiagnosticReport {
                            uri: file_uri,
                            version: None,
                            full_document_diagnostic_report: FullDocumentDiagnosticReport {
                                result_id: None,
                                items: lsp_diags,
                            },
                        },
                    ));
                }
            }
        }
    }

    state.workspace_diag_reported = now_reported;

    // Belt-and-suspenders: a workspace pull can mass-insert cache entries
    // (one per `.knot` file in the workspace), so cap inline rather than
    // relying on the caller to call `prune_stale_workspace_diag_cache`
    // afterwards. The prune still runs in the main loop and is the
    // authoritative invalidation pass; this just guarantees the bound is
    // re-established before we hand control back, even if a future refactor
    // changes how prune is wired up.
    enforce_workspace_diag_cap(state);

    WorkspaceDiagnosticReportResult::Report(WorkspaceDiagnosticReport { items })
}

/// Drop cache entries for files whose content has changed (hash mismatch),
/// that no longer exist, or whose transitive imports have changed since the
/// entry was cached. Then enforce the configured cap by evicting the
/// least-recently-used remaining entries.
pub(crate) fn prune_stale_workspace_diag_cache(state: &mut ServerState) {
    // Compute the set of files whose disk content has changed since cached.
    // For the common case (file unchanged on disk since the cache entry was
    // written) the on-disk mtime still matches the recorded mtime and we
    // skip the read+hash entirely — keeps prune O(workspace_size) stat
    // calls instead of O(workspace_size) reads on a clean workspace.
    let mut changed: HashSet<PathBuf> = HashSet::new();
    let mut mtime_refreshes: Vec<(PathBuf, SystemTime)> = Vec::new();
    for (path, (cached_h, _, _, cached_mtime)) in &state.workspace_diag_cache {
        let disk_mtime = current_mtime(path);
        if let (Some(cm), Some(dm)) = (cached_mtime, disk_mtime) {
            if *cm == dm {
                continue;
            }
        }
        // Mtime missing or moved — verify by hash. A content match here means
        // the bytes were untouched but mtime was bumped (`jj`/`git` checkout):
        // refresh the recorded mtime so the fast path applies next time.
        match std::fs::read_to_string(path) {
            Ok(s) if content_hash(&s) == *cached_h => {
                if let Some(dm) = disk_mtime {
                    mtime_refreshes.push((path.clone(), dm));
                }
            }
            _ => {
                changed.insert(path.clone());
            }
        }
    }
    for (p, mtime) in mtime_refreshes {
        if let Some(entry) = state.workspace_diag_cache.get_mut(&p) {
            entry.3 = Some(mtime);
        }
    }

    // Propagate invalidation along the reverse-imports graph: any file that
    // imports a changed file (transitively) must also be evicted, because its
    // cached diagnostics may have referenced types/effects from the now-stale
    // import.
    let mut affected = changed.clone();
    let mut frontier: Vec<PathBuf> = changed.into_iter().collect();
    while let Some(p) = frontier.pop() {
        if let Some(importers) = state.reverse_imports.get(&p) {
            for imp in importers {
                if affected.insert(imp.clone()) {
                    frontier.push(imp.clone());
                }
            }
        }
    }

    state.workspace_diag_cache.retain(|path, _| !affected.contains(path));

    enforce_workspace_diag_cap(state);
}

/// Run the full analysis pipeline on an unopened workspace file and return its
/// LSP diagnostics. Reuses the parsed module from the import cache so we don't
/// pay the lex+parse cost twice (the caller already paid it via
/// `get_or_parse_file_shared`).
///
/// Panics inside the compiler pipeline (parser, infer, effects, stratify,
/// sql_lint) on malformed input would otherwise kill the worker thread silently
/// — `std::thread::scope` swallows the join error in our caller, leaving the
/// file with no diagnostics and the user with no signal that anything broke.
/// Catch them here, log the message, and emit a synthetic diagnostic so the
/// failure surfaces in the gutter.
fn analyze_unopened_file(
    module: &Module,
    source: &str,
    path: &Path,
    uri: &Uri,
) -> Vec<Diagnostic> {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        analyze_unopened_file_inner(module, source, path, uri)
    }));
    match result {
        Ok(diags) => diags,
        Err(payload) => {
            let msg = panic_payload_message(&payload);
            eprintln!(
                "knot-lsp: workspace analysis panicked for {}: {msg}",
                uri.as_str()
            );
            let raw = diagnostic::Diagnostic::error(format!(
                "internal LSP error during workspace analysis: {msg}"
            ))
            .label(knot::ast::Span::new(0, 0), "analysis aborted here")
            .note("this is a bug in the language server; other files are unaffected");
            to_lsp_diagnostic(&raw, source, uri).into_iter().collect()
        }
    }
}

fn panic_payload_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic with non-string payload".to_string()
    }
}

fn analyze_unopened_file_inner(
    module: &Module,
    source: &str,
    path: &Path,
    uri: &Uri,
) -> Vec<Diagnostic> {
    let mut all_diags = Vec::new();

    // Re-lex to surface lexer diagnostics (the cache only stored the parsed AST).
    let lexer = knot::lexer::Lexer::new(source);
    let (_, lex_diags) = lexer.tokenize();
    all_diags.extend(lex_diags);

    // Re-parse to capture parse diagnostics (the cache discards them too).
    let lexer2 = knot::lexer::Lexer::new(source);
    let (tokens, _) = lexer2.tokenize();
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (_, parse_diags) = parser.parse_module();
    all_diags.extend(parse_diags);

    let has_parse_errors = all_diags
        .iter()
        .any(|d| matches!(d.severity, diagnostic::Severity::Error));

    if !has_parse_errors {
        let mut analysis_module = module.clone();

        let _ = knot_compiler::modules::resolve_imports(&mut analysis_module, path);
        knot_compiler::base::inject_prelude(&mut analysis_module);
        knot_compiler::desugar::desugar(&mut analysis_module);

        let (infer_diags, _, _, _, _, _, _, _) = knot_compiler::infer::check(&analysis_module);
        all_diags.extend(infer_diags);

        let (effect_diags, _) = knot_compiler::effects::check_with_effects(&analysis_module);
        all_diags.extend(effect_diags);

        all_diags.extend(knot_compiler::stratify::check(&analysis_module));

        let type_env = knot_compiler::types::TypeEnv::from_module(&analysis_module);
        all_diags.extend(knot_compiler::sql_lint::check(&analysis_module, &type_env));
    }

    all_diags
        .iter()
        .filter_map(|d| to_lsp_diagnostic(d, source, uri))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestWorkspace;

    /// Seed `workspace_diag_cache` with `count` synthetic entries with strictly
    /// increasing access timestamps. Returns paths in insertion order, so
    /// callers can assert about which entries survive eviction.
    fn seed_cache(state: &mut ServerState, count: usize) -> Vec<PathBuf> {
        let mut paths = Vec::with_capacity(count);
        for i in 0..count {
            let p = PathBuf::from(format!("/tmp/knot-lsp-cap-test/file_{i}.knot"));
            state.workspace_diag_clock = state.workspace_diag_clock.wrapping_add(1);
            let access = state.workspace_diag_clock;
            state
                .workspace_diag_cache
                .insert(p.clone(), (i as u64, Vec::new(), access, None));
            paths.push(p);
        }
        paths
    }

    #[test]
    fn enforce_cap_evicts_oldest_entries_when_over_budget() {
        let mut ws = TestWorkspace::new();
        ws.state.config.max_workspace_diag_cache = 3;

        let paths = seed_cache(&mut ws.state, 5);
        assert_eq!(ws.state.workspace_diag_cache.len(), 5);

        enforce_workspace_diag_cap(&mut ws.state);

        assert_eq!(ws.state.workspace_diag_cache.len(), 3);
        // Oldest two entries (paths[0], paths[1]) should be gone.
        assert!(!ws.state.workspace_diag_cache.contains_key(&paths[0]));
        assert!(!ws.state.workspace_diag_cache.contains_key(&paths[1]));
        assert!(ws.state.workspace_diag_cache.contains_key(&paths[2]));
        assert!(ws.state.workspace_diag_cache.contains_key(&paths[3]));
        assert!(ws.state.workspace_diag_cache.contains_key(&paths[4]));
    }

    #[test]
    fn enforce_cap_no_op_when_under_budget() {
        let mut ws = TestWorkspace::new();
        ws.state.config.max_workspace_diag_cache = 10;

        let paths = seed_cache(&mut ws.state, 4);
        let before_clock = ws.state.workspace_diag_clock;

        enforce_workspace_diag_cap(&mut ws.state);

        assert_eq!(ws.state.workspace_diag_cache.len(), 4);
        for p in &paths {
            assert!(ws.state.workspace_diag_cache.contains_key(p));
        }
        // The cap-only path must not bump the access clock — only
        // insert/hit paths do.
        assert_eq!(ws.state.workspace_diag_clock, before_clock);
    }

    #[test]
    fn prune_skips_disk_read_when_mtime_matches() {
        // The mtime fast-path is the whole reason we threaded mtime through
        // the cache value. Set a real on-disk file, write a cache entry
        // anchored to its current mtime but with a *deliberately wrong*
        // content hash, and confirm prune leaves the entry in place — proof
        // that no disk read happened (a read+hash would have flagged the
        // hash mismatch and evicted).
        let dir = std::env::temp_dir().join("knot-lsp-prune-mtime");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hot.knot");
        std::fs::write(&path, "actual content\n").expect("seed file");
        let mtime = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .expect("mtime available on this fs");

        let mut ws = TestWorkspace::new();
        ws.state.config.max_workspace_diag_cache = 16;
        ws.state.workspace_diag_clock = 1;
        // Hash 0xDEAD doesn't match the real content's hash — if prune
        // reads the file, it'll evict on hash mismatch.
        ws.state
            .workspace_diag_cache
            .insert(path.clone(), (0xDEAD, Vec::new(), 1, Some(mtime)));

        prune_stale_workspace_diag_cache(&mut ws.state);

        assert!(
            ws.state.workspace_diag_cache.contains_key(&path),
            "mtime fast-path should have skipped the disk read entirely"
        );

        // Flip the cached mtime to a value that won't match disk; now prune
        // *must* read+hash and find the mismatch, so the entry is dropped.
        let bogus_mtime = mtime - std::time::Duration::from_secs(3600);
        ws.state
            .workspace_diag_cache
            .insert(path.clone(), (0xDEAD, Vec::new(), 1, Some(bogus_mtime)));

        prune_stale_workspace_diag_cache(&mut ws.state);

        assert!(
            !ws.state.workspace_diag_cache.contains_key(&path),
            "mtime miss should fall through to read+hash and evict on mismatch"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prune_refreshes_mtime_when_content_unchanged() {
        // The bookkeeping for the jj/git checkout case: mtime moves but
        // bytes don't. After the read+hash confirms content unchanged, the
        // recorded mtime should be refreshed so the fast path applies on
        // the next pull.
        let dir = std::env::temp_dir().join("knot-lsp-prune-mtime-refresh");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("touched.knot");
        std::fs::write(&path, "stable content\n").expect("seed file");
        let real_hash = content_hash("stable content\n");
        let real_mtime = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .expect("mtime available on this fs");

        let mut ws = TestWorkspace::new();
        ws.state.config.max_workspace_diag_cache = 16;
        ws.state.workspace_diag_clock = 1;
        let stale_mtime = real_mtime - std::time::Duration::from_secs(3600);
        ws.state
            .workspace_diag_cache
            .insert(path.clone(), (real_hash, Vec::new(), 1, Some(stale_mtime)));

        prune_stale_workspace_diag_cache(&mut ws.state);

        let entry = ws
            .state
            .workspace_diag_cache
            .get(&path)
            .expect("entry should survive — content matched cached hash");
        assert_eq!(
            entry.3,
            Some(real_mtime),
            "mtime should be refreshed to disk's current mtime"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn enforce_cap_respects_recency_after_access_bump() {
        let mut ws = TestWorkspace::new();
        ws.state.config.max_workspace_diag_cache = 2;

        let paths = seed_cache(&mut ws.state, 3);
        // Touch paths[0] so it's now the most recently used. Without this
        // bump it would be evicted first; with it, paths[1] should go.
        ws.state.workspace_diag_clock = ws.state.workspace_diag_clock.wrapping_add(1);
        let access = ws.state.workspace_diag_clock;
        if let Some(entry) = ws.state.workspace_diag_cache.get_mut(&paths[0]) {
            entry.2 = access;
        }

        enforce_workspace_diag_cap(&mut ws.state);

        assert_eq!(ws.state.workspace_diag_cache.len(), 2);
        assert!(ws.state.workspace_diag_cache.contains_key(&paths[0]));
        assert!(!ws.state.workspace_diag_cache.contains_key(&paths[1]));
        assert!(ws.state.workspace_diag_cache.contains_key(&paths[2]));
    }
}
