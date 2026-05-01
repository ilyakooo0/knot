//! Document analysis pipeline. Lex → parse → import resolution → desugar →
//! type/effect/stratification check, plus the off-disk file caching that backs
//! cross-file features.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use lsp_server::{Connection, Message, Notification};
use lsp_types::notification::Notification as _;
use lsp_types::{notification, Diagnostic, PublishDiagnosticsParams, Uri};

use knot::ast::{self, DeclKind, Module, Span};
use knot::diagnostic;
use knot_compiler::effects::EffectSet;
use knot_compiler::infer::MonadKind;

use crate::defs::{build_details, resolve_definitions};
use crate::diagnostics::to_lsp_diagnostic;
use crate::state::{
    content_hash, AnalysisResult, AnalysisTask, DocumentState, InferenceCache, InferenceSnapshot,
    ANALYSIS_DEBOUNCE, ANALYSIS_MAX_WAIT,
};
use crate::utils::{
    collect_keyword_operator_positions, extract_doc_comments, find_word_in_source, uri_to_path,
};

/// Soft cap on cached inference snapshots — undo/redo and rapid file
/// switching are well-served by even a small cache; we don't need to retain
/// the whole edit history.
const MAX_INFERENCE_CACHE_ENTRIES: usize = 128;

// ── Analysis worker ─────────────────────────────────────────────────

pub fn analysis_worker(
    rx: Receiver<AnalysisTask>,
    tx: Sender<AnalysisResult>,
    import_cache: Arc<Mutex<HashMap<PathBuf, (u64, Module, String)>>>,
    inference_cache: Arc<Mutex<InferenceCache>>,
) {
    loop {
        let first = match rx.recv() {
            Ok(t) => t,
            Err(_) => return,
        };

        // Coalesce: keep the latest task per URI within a debounce window.
        let mut pending: HashMap<Uri, AnalysisTask> = HashMap::new();
        pending.insert(first.uri.clone(), first);

        let batch_start = Instant::now();
        loop {
            let elapsed = batch_start.elapsed();
            if elapsed >= ANALYSIS_MAX_WAIT {
                break;
            }
            let timeout = ANALYSIS_DEBOUNCE.min(ANALYSIS_MAX_WAIT - elapsed);
            match rx.recv_timeout(timeout) {
                Ok(task) => {
                    pending.insert(task.uri.clone(), task);
                }
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }

        for (_, task) in pending {
            // Clone the caches out of the shared mutexes so the heavy analysis
            // pipeline (lex+parse+infer+effects+stratify+sql_lint, ~100ms) runs
            // without blocking the main thread. Cache writes are merged back
            // afterwards. This trades a transient double-allocation for UI
            // responsiveness — completion, workspace symbol, rename etc. all
            // need read access to `import_cache` and would otherwise queue
            // behind the worker on every keystroke.
            let mut cache_local = match import_cache.lock() {
                Ok(g) => g.clone(),
                Err(poison) => poison.into_inner().clone(),
            };
            let mut inf_cache_local = match inference_cache.lock() {
                Ok(g) => g.clone(),
                Err(poison) => poison.into_inner().clone(),
            };

            let doc = analyze_document(
                &task.uri,
                &task.source,
                &mut cache_local,
                &mut inf_cache_local,
            );

            // Merge any new entries back. We don't overwrite entries that the
            // main thread or another worker iteration may have produced
            // concurrently with a fresher hash — only insert if the key is
            // missing or our hash matches.
            if let Ok(mut shared) = import_cache.lock() {
                for (k, v) in cache_local.into_iter() {
                    shared.entry(k).or_insert(v);
                }
            }
            if let Ok(mut shared) = inference_cache.lock() {
                *shared = inf_cache_local;
            }

            if tx
                .send(AnalysisResult {
                    uri: task.uri,
                    version: task.version,
                    doc,
                })
                .is_err()
            {
                return;
            }
        }
    }
}

// ── Document analysis ───────────────────────────────────────────────

pub fn analyze_document(
    uri: &Uri,
    source: &str,
    import_cache: &mut HashMap<PathBuf, (u64, Module, String)>,
    inference_cache: &mut InferenceCache,
) -> DocumentState {
    let mut all_diags = Vec::new();
    let mut type_info = HashMap::new();
    let mut local_type_info = HashMap::new();
    let mut effect_info = HashMap::new();
    let mut effect_sets: HashMap<String, EffectSet> = HashMap::new();
    let mut refined_types: HashMap<String, ast::Expr> = HashMap::new();
    let mut refine_targets: HashMap<Span, String> = HashMap::new();
    let mut source_refinements: HashMap<String, Vec<(Option<String>, String, ast::Expr)>> =
        HashMap::new();
    let mut monad_info: HashMap<Span, MonadKind> = HashMap::new();
    let unit_info: HashMap<Span, String> = HashMap::new();

    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, lex_diags) = lexer.tokenize();
    all_diags.extend(lex_diags);

    let keyword_tokens = collect_keyword_operator_positions(&tokens);

    let canonical_path = uri_to_path(uri).and_then(|p| p.canonicalize().ok());
    let src_hash = content_hash(source);
    let (module, parse_diags) = match canonical_path.as_ref() {
        Some(p) => match import_cache.get(p) {
            Some((cached_hash, cached_module, _)) if *cached_hash == src_hash => {
                let parser = knot::parser::Parser::new(source.to_string(), tokens);
                let (_reparsed, parse_d) = parser.parse_module();
                (cached_module.clone(), parse_d)
            }
            _ => {
                let parser = knot::parser::Parser::new(source.to_string(), tokens);
                let (m, parse_d) = parser.parse_module();
                import_cache.insert(p.clone(), (src_hash, m.clone(), source.to_string()));
                (m, parse_d)
            }
        },
        None => {
            let parser = knot::parser::Parser::new(source.to_string(), tokens);
            parser.parse_module()
        }
    };
    all_diags.extend(parse_diags);

    let (definitions, references, literal_types) = resolve_definitions(&module, source);
    let details = build_details(&module);
    let doc_comments = extract_doc_comments(source, &module);

    let (imported_files, import_defs, import_origins) = if let Some(path) = uri_to_path(uri) {
        resolve_import_navigation(&module.imports, &path, import_cache)
    } else {
        (HashMap::new(), HashMap::new(), HashMap::new())
    };

    let has_parse_errors = all_diags
        .iter()
        .any(|d| matches!(d.severity, diagnostic::Severity::Error));

    if !has_parse_errors {
        // Inference-snapshot fast path: if we've already analyzed this exact
        // (canonical_path, content_hash) pair, replay the cached diagnostics
        // and inferred-type maps instead of re-running the full pipeline.
        // This is a meaningful win for undo/redo, format-on-save round trips,
        // and rapid file switches between unchanged sources. True
        // per-declaration incrementalism (only re-checking the changed decl
        // and its dependents) requires restructuring `infer.rs` and is
        // tracked separately.
        let cache_key = canonical_path
            .as_ref()
            .map(|p| (p.clone(), src_hash));
        if let Some(key) = &cache_key {
            if let Some(snap) = inference_cache.get(key) {
                all_diags.extend(snap.diagnostics.iter().cloned());
                type_info = snap.type_info.clone();
                local_type_info = snap.local_type_info.clone();
                effect_info = snap.effect_info.clone();
                effect_sets = snap.effect_sets.clone();
                refined_types = snap.refined_types.clone();
                refine_targets = snap.refine_targets.clone();
                source_refinements = snap.source_refinements.clone();
                monad_info = snap.monad_info.clone();

                return DocumentState {
                    source: source.to_string(),
                    module,
                    references,
                    definitions,
                    details,
                    type_info,
                    local_type_info,
                    literal_types,
                    effect_info,
                    effect_sets,
                    knot_diagnostics: all_diags,
                    imported_files,
                    import_defs,
                    import_origins,
                    doc_comments,
                    keyword_tokens,
                    refined_types,
                    refine_targets,
                    source_refinements,
                    monad_info,
                    unit_info,
                };
            }
        }

        let mut analysis_module = module.clone();

        if let Some(path) = uri_to_path(uri) {
            let _ = knot_compiler::modules::resolve_imports(&mut analysis_module, &path);
        }

        knot_compiler::base::inject_prelude(&mut analysis_module);
        knot_compiler::desugar::desugar(&mut analysis_module);

        // Capture the start of inference diagnostics so the cached snapshot
        // only contains what's contributed by this run (lex + parse diags
        // are repopulated on every call from fresh state).
        let pre_inference_len = all_diags.len();

        let (
            infer_diags,
            mi,
            inferred_types,
            local_types,
            rt,
            refined_type_info,
            _from_json,
        ) = knot_compiler::infer::check(&analysis_module);
        all_diags.extend(infer_diags);
        type_info = inferred_types;
        local_type_info = local_types;
        refined_types = refined_type_info;
        refine_targets = rt;
        monad_info = mi;

        let (effect_diags, effects) =
            knot_compiler::effects::check_with_effects(&analysis_module);
        all_diags.extend(effect_diags);
        for (name, eff) in &effects {
            if !eff.is_pure() {
                effect_info.insert(name.clone(), format!("{eff}"));
            }
            effect_sets.insert(name.clone(), eff.clone());
        }

        all_diags.extend(knot_compiler::stratify::check(&analysis_module));

        let type_env = knot_compiler::types::TypeEnv::from_module(&analysis_module);
        source_refinements = type_env.source_refinements.clone();

        all_diags.extend(knot_compiler::sql_lint::check(&analysis_module, &type_env));

        // Stash the inferred outputs for the next analysis of this same
        // (path, content_hash) pair. Eviction is bounded — if we're at the
        // soft cap, drop a single arbitrary entry; that's good enough for a
        // fast-path cache where stale entries are equivalent to a miss.
        if let Some(key) = cache_key {
            if inference_cache.len() >= MAX_INFERENCE_CACHE_ENTRIES {
                if let Some(victim) = inference_cache.keys().next().cloned() {
                    inference_cache.remove(&victim);
                }
            }
            let snapshot = InferenceSnapshot {
                diagnostics: all_diags[pre_inference_len..].to_vec(),
                type_info: type_info.clone(),
                local_type_info: local_type_info.clone(),
                effect_info: effect_info.clone(),
                effect_sets: effect_sets.clone(),
                refined_types: refined_types.clone(),
                refine_targets: refine_targets.clone(),
                source_refinements: source_refinements.clone(),
                monad_info: monad_info.clone(),
            };
            inference_cache.insert(key, snapshot);
        }
    }

    DocumentState {
        source: source.to_string(),
        module,
        references,
        definitions,
        details,
        type_info,
        local_type_info,
        literal_types,
        effect_info,
        effect_sets,
        knot_diagnostics: all_diags,
        imported_files,
        import_defs,
        import_origins,
        doc_comments,
        keyword_tokens,
        refined_types,
        refine_targets,
        source_refinements,
        monad_info,
        unit_info,
    }
}

// ── File caching ────────────────────────────────────────────────────

/// Read, lex, and parse a `.knot` file off disk, reusing the content-hash-keyed
/// cache when possible. Returns `None` only if the file is missing or unreadable.
///
/// This is the single funnel through which every off-disk read in the LSP flows
/// (rename across unopened files, workspace symbol search, workspace diagnostics,
/// auto-import completion, import resolution).
pub fn get_or_parse_file(
    path: &Path,
    cache: &mut HashMap<PathBuf, (u64, Module, String)>,
) -> Option<(Module, String)> {
    let source = std::fs::read_to_string(path).ok()?;
    let hash = content_hash(&source);
    if let Some((cached_hash, cached_module, cached_source)) = cache.get(path) {
        if *cached_hash == hash {
            return Some((cached_module.clone(), cached_source.clone()));
        }
    }
    let lexer = knot::lexer::Lexer::new(&source);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(source.clone(), tokens);
    let (module, _) = parser.parse_module();
    cache.insert(path.to_path_buf(), (hash, module.clone(), source.clone()));
    Some((module, source))
}

/// Like `get_or_parse_file`, but operates against the `Arc<Mutex<…>>` cache held
/// by `ServerState`. Locks the mutex briefly per call.
pub fn get_or_parse_file_shared(
    path: &Path,
    cache: &Arc<Mutex<HashMap<PathBuf, (u64, Module, String)>>>,
) -> Option<(Module, String)> {
    let mut guard = cache.lock().ok()?;
    get_or_parse_file(path, &mut guard)
}

// ── Import navigation ───────────────────────────────────────────────

/// Resolve imported files for cross-file navigation.
pub fn resolve_import_navigation(
    imports: &[ast::Import],
    source_path: &Path,
    import_cache: &mut HashMap<PathBuf, (u64, Module, String)>,
) -> (
    HashMap<PathBuf, String>,
    HashMap<String, (PathBuf, Span)>,
    HashMap<String, String>,
) {
    let mut imported_files = HashMap::new();
    let mut import_defs = HashMap::new();
    let mut import_origins = HashMap::new();

    let base_dir = source_path.parent().unwrap_or(Path::new("."));

    for imp in imports {
        let rel_path = PathBuf::from(&imp.path).with_extension("knot");
        let full_path = base_dir.join(&rel_path);

        let canonical = match full_path.canonicalize() {
            Ok(p) => p,
            Err(_) => continue,
        };

        let (module, source) = match get_or_parse_file(&canonical, import_cache) {
            Some(v) => v,
            None => continue,
        };

        for decl in &module.decls {
            match &decl.node {
                DeclKind::Data {
                    name, constructors, ..
                } => {
                    import_defs.insert(name.clone(), (canonical.clone(), decl.span));
                    import_origins.insert(name.clone(), imp.path.clone());
                    for ctor in constructors {
                        let ctor_span = find_word_in_source(
                            &source,
                            &ctor.name,
                            decl.span.start,
                            decl.span.end,
                        )
                        .unwrap_or(decl.span);
                        import_defs.insert(ctor.name.clone(), (canonical.clone(), ctor_span));
                        import_origins.insert(ctor.name.clone(), imp.path.clone());
                    }
                }
                DeclKind::TypeAlias { name, .. }
                | DeclKind::Source { name, .. }
                | DeclKind::View { name, .. }
                | DeclKind::Derived { name, .. }
                | DeclKind::Fun { name, .. }
                | DeclKind::Trait { name, .. }
                | DeclKind::Route { name, .. }
                | DeclKind::RouteComposite { name, .. } => {
                    import_defs.insert(name.clone(), (canonical.clone(), decl.span));
                    import_origins.insert(name.clone(), imp.path.clone());
                }
                DeclKind::Impl { items, .. } => {
                    for item in items {
                        if let ast::ImplItem::Method { name, .. } = item {
                            import_defs.insert(name.clone(), (canonical.clone(), decl.span));
                            import_origins.insert(name.clone(), imp.path.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        imported_files.insert(canonical, source);
    }

    (imported_files, import_defs, import_origins)
}

// ── Diagnostic publishing ───────────────────────────────────────────

pub fn publish_diagnostics(
    conn: &Connection,
    uri: &Uri,
    doc: &DocumentState,
    version: Option<i32>,
) {
    let lsp_diags: Vec<Diagnostic> = doc
        .knot_diagnostics
        .iter()
        .filter_map(|d| to_lsp_diagnostic(d, &doc.source, uri))
        .collect();

    let params = PublishDiagnosticsParams::new(uri.clone(), lsp_diags, version);
    let not = Notification::new(notification::PublishDiagnostics::METHOD.into(), params);
    if let Err(e) = conn.sender.send(Message::Notification(not)) {
        eprintln!("knot-lsp: failed to publish diagnostics: {e}");
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{Receiver, Sender};
    use std::thread;
    use std::time::Duration;

    fn fake_uri(s: &str) -> Uri {
        s.parse().unwrap()
    }

    /// Spin up the analysis worker and feed it tasks. Used to verify the
    /// debounce/coalesce behavior without going through stdio.
    fn spawn_worker() -> (Sender<AnalysisTask>, Receiver<AnalysisResult>, thread::JoinHandle<()>) {
        let (tx, rx) = crossbeam_channel::unbounded::<AnalysisTask>();
        let (rtx, rrx) = crossbeam_channel::unbounded::<AnalysisResult>();
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let inf_cache = Arc::new(Mutex::new(HashMap::new()));
        let handle = thread::spawn(move || analysis_worker(rx, rtx, cache, inf_cache));
        (tx, rrx, handle)
    }

    #[test]
    fn worker_returns_result_for_single_task() {
        let (tx, rx, handle) = spawn_worker();
        let uri = fake_uri("file:///tmp/a.knot");
        tx.send(AnalysisTask {
            uri: uri.clone(),
            source: "x = 1".into(),
            version: Some(1),
        })
        .unwrap();

        let result = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker should produce a result");
        assert_eq!(result.uri, uri);
        assert_eq!(result.version, Some(1));
        assert_eq!(result.doc.source, "x = 1");

        drop(tx);
        handle.join().unwrap();
    }

    #[test]
    fn worker_coalesces_rapid_edits_to_latest() {
        let (tx, rx, handle) = spawn_worker();
        let uri = fake_uri("file:///tmp/b.knot");
        // Three rapid edits within the debounce window — only the latest
        // should turn into a result.
        for (i, src) in ["x = 1", "x = 2", "x = 3"].iter().enumerate() {
            tx.send(AnalysisTask {
                uri: uri.clone(),
                source: (*src).into(),
                version: Some(i as i32 + 1),
            })
            .unwrap();
        }

        let result = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker should produce a result");
        assert_eq!(result.doc.source, "x = 3");
        assert_eq!(result.version, Some(3));

        // No follow-up result should arrive — the earlier two were dropped.
        assert!(rx.recv_timeout(Duration::from_millis(300)).is_err());

        drop(tx);
        handle.join().unwrap();
    }

    #[test]
    fn worker_keeps_distinct_uris_separate() {
        let (tx, rx, handle) = spawn_worker();
        let a = fake_uri("file:///tmp/a.knot");
        let b = fake_uri("file:///tmp/b.knot");
        tx.send(AnalysisTask {
            uri: a.clone(),
            source: "x = 1".into(),
            version: Some(1),
        })
        .unwrap();
        tx.send(AnalysisTask {
            uri: b.clone(),
            source: "y = 2".into(),
            version: Some(1),
        })
        .unwrap();

        let mut got = Vec::new();
        for _ in 0..2 {
            let r = rx
                .recv_timeout(Duration::from_secs(5))
                .expect("worker should produce two results");
            got.push((r.uri, r.doc.source));
        }
        got.sort_by(|x, y| x.0.as_str().cmp(y.0.as_str()));
        assert_eq!(got[0], (a, "x = 1".into()));
        assert_eq!(got[1], (b, "y = 2".into()));

        drop(tx);
        handle.join().unwrap();
    }
}
