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
use std::path::{Path, PathBuf};

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
            .filter(|(cached_h, _, _)| *cached_h == hash)
            .map(|(_, diags, _)| diags.clone());
        if let Some(diags) = cached_match {
            state.workspace_diag_clock = state.workspace_diag_clock.wrapping_add(1);
            let access = state.workspace_diag_clock;
            if let Some(entry) = state.workspace_diag_cache.get_mut(&path) {
                entry.2 = access;
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
    state
        .workspace_diag_cache
        .insert(path.to_path_buf(), (hash, diags.clone(), access));
    diags
}

// ── Workspace Diagnostics (Pull Model) ──────────────────────────────

pub(crate) fn handle_workspace_diagnostics(
    state: &mut ServerState,
    _params: &WorkspaceDiagnosticParams,
) -> WorkspaceDiagnosticReportResult {
    let mut items = Vec::new();

    for (uri, doc) in &state.documents {
        let lsp_diags: Vec<Diagnostic> = doc
            .knot_diagnostics
            .iter()
            .filter_map(|d| to_lsp_diagnostic(d, &doc.source, uri))
            .collect();

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
            for file_path in files {
                let canonical = match file_path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if open_paths.contains(&canonical) {
                    continue;
                }
                let (module, source) =
                    match get_or_parse_file_shared(&canonical, &state.import_cache) {
                        Some(v) => v,
                        None => continue,
                    };
                let file_uri = match path_to_uri(&canonical) {
                    Some(u) => u,
                    None => continue,
                };
                let hash = content_hash(&source);
                if let Some((cached_h, cached, _)) =
                    state.workspace_diag_cache.get(&canonical)
                {
                    if *cached_h == hash {
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
                state
                    .workspace_diag_cache
                    .insert(canonical, (hash, lsp_diags.clone(), access));
                if !lsp_diags.is_empty() {
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

    WorkspaceDiagnosticReportResult::Report(WorkspaceDiagnosticReport { items })
}

/// Drop cache entries for files whose content has changed (hash mismatch),
/// that no longer exist, or whose transitive imports have changed since the
/// entry was cached. Then enforce the configured cap by evicting the
/// least-recently-used remaining entries.
pub(crate) fn prune_stale_workspace_diag_cache(state: &mut ServerState) {
    // Compute the set of files whose disk content has changed since cached.
    let mut changed: HashSet<PathBuf> = HashSet::new();
    for (path, (cached_h, _, _)) in &state.workspace_diag_cache {
        match std::fs::read_to_string(path) {
            Ok(s) if content_hash(&s) == *cached_h => {}
            _ => {
                changed.insert(path.clone());
            }
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

    // Cap-based LRU eviction. The configured `max_workspace_diag_cache` is the
    // soft ceiling — we sort by the per-entry access counter and drop the
    // oldest entries until the size is within bounds. O(n log n) on the cache
    // size, which is bounded; called once per workspace-diagnostics request.
    let cap = state.config.max_workspace_diag_cache;
    if state.workspace_diag_cache.len() > cap {
        let mut by_age: Vec<(PathBuf, u64)> = state
            .workspace_diag_cache
            .iter()
            .map(|(p, (_, _, access))| (p.clone(), *access))
            .collect();
        by_age.sort_by_key(|(_, a)| *a); // ascending: oldest first
        let to_drop = state.workspace_diag_cache.len().saturating_sub(cap);
        for (p, _) in by_age.into_iter().take(to_drop) {
            state.workspace_diag_cache.remove(&p);
        }
    }
}

/// Run the full analysis pipeline on an unopened workspace file and return its
/// LSP diagnostics. Reuses the parsed module from the import cache so we don't
/// pay the lex+parse cost twice (the caller already paid it via
/// `get_or_parse_file_shared`).
fn analyze_unopened_file(
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

        let (infer_diags, _, _, _, _, _, _) = knot_compiler::infer::check(&analysis_module);
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
