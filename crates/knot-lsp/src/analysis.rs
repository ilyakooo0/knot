//! Document analysis pipeline. Lex → parse → import resolution → desugar →
//! type/effect/stratification check, plus the off-disk file caching that backs
//! cross-file features.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use lsp_types::Uri;

use knot::ast::{self, DeclKind, Module, Span};
use knot::diagnostic;
use knot_compiler::effects::EffectSet;
use knot_compiler::infer::MonadKind;

use crate::defs::{build_details, resolve_definitions};
use crate::incremental::ModuleFingerprint;
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

/// Monotonic clock for LRU eviction. Bumped on every cache insert and hit.
/// `wrapping_add` so the counter never panics on overflow — at one bump
/// per microsecond the wrap is hundreds of thousands of years away, but
/// it's free safety against pathological clients.
fn next_access_clock(cache: &InferenceCache) -> u64 {
    cache
        .values()
        .map(|s| s.access_clock)
        .max()
        .unwrap_or(0)
        .wrapping_add(1)
}

/// Extract a `<unit>` annotation from a formatted local type string. Returns
/// `None` for dimensionless types or types without unit info.
fn extract_unit_from_local_type(ty: &str) -> Option<String> {
    let parsed = crate::parsed_type::ParsedType::parse(ty);
    let value = match &parsed {
        crate::parsed_type::ParsedType::Function(_, ret) => ret.strip_io(),
        other => other.strip_io(),
    };
    value.unit().map(|s| s.to_string())
}

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
                for (k, v) in inf_cache_local.into_iter() {
                    shared.entry(k).or_insert(v);
                }
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
    let mut unit_info: HashMap<Span, String> = HashMap::new();
    let mut changed_decl_names: Vec<String> = Vec::new();
    let mut signature_changed_decl_names: Vec<String> = Vec::new();
    let mut dirty_decl_closure: std::collections::HashSet<String> = std::collections::HashSet::new();

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
        // Inference-snapshot fast path. Two tiers:
        //
        // (1) **Content-hash hit.** Same source bytes as a prior analysis —
        //     undo/redo, format-on-save round trips, rapid file switching.
        //
        // (2) **Fingerprint hit.** Source bytes differ but the AST shape is
        //     identical (whitespace, comment, doc-comment edits). The
        //     `ModuleFingerprint` ignores spans and trivia, so a structurally
        //     equal fingerprint guarantees the inferencer would produce the
        //     same `(type_info, effect_info, ...)` output. We reuse it.
        //
        // True per-declaration incrementalism — re-checking only the changed
        // decls and their transitive dependents — also depends on the
        // fingerprint groundwork built here, but additionally requires
        // changes to `infer.rs::pre_register` so cached schemes can replace
        // a body-checking pass for "clean" decls. That's a separate effort.
        let cache_key = canonical_path
            .as_ref()
            .map(|p| (p.clone(), src_hash));
        let new_fingerprint = ModuleFingerprint::from_module(&module);

        // Compute the per-decl diff between this run and the most recent
        // snapshot for the same path. Used downstream by `apply_analysis_result`
        // to skip re-queuing dependents that don't actually import any of the
        // changed names — a real win when a user edits one decl in a file
        // with many importers.
        if let Some(key) = &cache_key {
            if let Some(latest) = inference_cache
                .iter()
                .filter(|(k, _)| k.0 == key.0)
                .max_by_key(|(_, s)| s.access_clock)
            {
                let dirty = new_fingerprint.changed_decls(&latest.1.fingerprint);
                // Compute the transitive in-file dirty closure too. Stored
                // on `DocumentState` for selective re-check downstream of
                // the inference pass once `infer.rs::check` learns to skip
                // clean decls. Cheap to compute (linear in decl count).
                dirty_decl_closure = new_fingerprint.dirty_closure(&dirty);
                changed_decl_names = dirty.into_iter().collect();
                changed_decl_names.sort();

                // The cross-file dependent re-queue (handled by
                // `apply_analysis_result::requeue_dependents_for_changed_decls`)
                // only needs to fire when an externally-visible signature
                // moved. Body-only changes to a *typed* function don't shift
                // its declared type, so its dependents can sit tight.
                let sig_dirty = new_fingerprint
                    .signature_changed_decls(&latest.1.fingerprint);
                signature_changed_decl_names = sig_dirty.into_iter().collect();
                signature_changed_decl_names.sort();
                if std::env::var("KNOT_LSP_TRACE_DIRTY").is_ok() && !changed_decl_names.is_empty() {
                    eprintln!(
                        "knot-lsp: {} dirty decls in {}: {}",
                        changed_decl_names.len(),
                        key.0.display(),
                        changed_decl_names
                            .iter()
                            .take(8)
                            .map(String::as_str)
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
        }
        if let Some(key) = &cache_key {
            // Tier 1: exact content hash match.
            // Use a two-step lookup so we can bump the access clock on the
            // mutable map without confusing the borrow checker.
            if inference_cache.contains_key(key) {
                let new_clock = next_access_clock(inference_cache);
                let snap = inference_cache.get_mut(key).unwrap();
                snap.access_clock = new_clock;
                let snap_ref = &*snap;
                return reuse_snapshot(
                    snap_ref,
                    source,
                    module,
                    references,
                    definitions,
                    details,
                    literal_types,
                    imported_files,
                    import_defs,
                    import_origins,
                    doc_comments,
                    keyword_tokens,
                    unit_info,
                    all_diags,
                );
            }
            // Tier 2: structural fingerprint match. Search the per-path
            // entries — the cache is small enough that a linear scan over
            // its keys is cheap (bounded by MAX_INFERENCE_CACHE_ENTRIES).
            // Two-step again so we can bump the clock without aliasing.
            let matching_key = inference_cache.iter().find_map(|(k, snap)| {
                if k.0 == key.0 && new_fingerprint.structurally_equal(&snap.fingerprint) {
                    Some(k.clone())
                } else {
                    None
                }
            });
            if let Some(matched) = matching_key {
                let new_clock = next_access_clock(inference_cache);
                let snap = inference_cache.get_mut(&matched).unwrap();
                snap.access_clock = new_clock;
                let snap_ref = &*snap;
                return reuse_snapshot(
                    snap_ref,
                    source,
                    module,
                    references,
                    definitions,
                    details,
                    literal_types,
                    imported_files,
                    import_defs,
                    import_origins,
                    doc_comments,
                    keyword_tokens,
                    unit_info,
                    all_diags,
                );
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

        // Populate `unit_info` from the formatted local type strings — every
        // binding whose type carries a `<unit>` annotation gets an entry. The
        // inlay-hint handler reads this directly to surface unit annotations
        // on bindings without re-parsing each type string per request.
        for (span, ty) in &local_type_info {
            if let Some(unit) = extract_unit_from_local_type(ty) {
                unit_info.insert(*span, unit);
            }
        }

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
        // soft cap, drop the least-recently-accessed entry. LRU keeps
        // hot files (the ones the user is actively editing or jumping
        // between) resident through long sessions; previously the random
        // eviction could discard a fresh snapshot before its first hit.
        if let Some(key) = cache_key {
            if inference_cache.len() >= MAX_INFERENCE_CACHE_ENTRIES {
                if let Some((victim, _)) = inference_cache
                    .iter()
                    .min_by_key(|(_, s)| s.access_clock)
                    .map(|(k, s)| (k.clone(), s.access_clock))
                {
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
                fingerprint: new_fingerprint.clone(),
                access_clock: next_access_clock(inference_cache),
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
        changed_decl_names,
        signature_changed_decl_names,
        dirty_decl_closure,
    }
}

/// Compose a `DocumentState` from a cached inference snapshot. Pulled out
/// of `analyze_document` so the two cache-hit tiers (content hash and
/// structural fingerprint) can share the result-assembly path verbatim.
#[allow(clippy::too_many_arguments)]
fn reuse_snapshot(
    snap: &InferenceSnapshot,
    source: &str,
    module: Module,
    references: Vec<(Span, Span)>,
    definitions: HashMap<String, Span>,
    details: HashMap<String, String>,
    literal_types: Vec<(Span, String)>,
    imported_files: HashMap<PathBuf, String>,
    import_defs: HashMap<String, (PathBuf, Span)>,
    import_origins: HashMap<String, String>,
    doc_comments: HashMap<String, String>,
    keyword_tokens: Vec<(Span, u32)>,
    unit_info: HashMap<Span, String>,
    mut all_diags: Vec<diagnostic::Diagnostic>,
) -> DocumentState {
    all_diags.extend(snap.diagnostics.iter().cloned());
    DocumentState {
        source: source.to_string(),
        module,
        references,
        definitions,
        details,
        type_info: snap.type_info.clone(),
        local_type_info: snap.local_type_info.clone(),
        literal_types,
        effect_info: snap.effect_info.clone(),
        effect_sets: snap.effect_sets.clone(),
        knot_diagnostics: all_diags,
        imported_files,
        import_defs,
        import_origins,
        doc_comments,
        keyword_tokens,
        refined_types: snap.refined_types.clone(),
        refine_targets: snap.refine_targets.clone(),
        source_refinements: snap.source_refinements.clone(),
        monad_info: snap.monad_info.clone(),
        unit_info,
        // Cache hit: the parsed AST is identical (or structurally equal)
        // to a prior run, so by definition no decls changed since then.
        changed_decl_names: Vec::new(),
        signature_changed_decl_names: Vec::new(),
        dirty_decl_closure: std::collections::HashSet::new(),
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
                | DeclKind::Route { name, .. }
                | DeclKind::RouteComposite { name, .. } => {
                    import_defs.insert(name.clone(), (canonical.clone(), decl.span));
                    import_origins.insert(name.clone(), imp.path.clone());
                }
                DeclKind::Trait { name, items, .. } => {
                    import_defs.insert(name.clone(), (canonical.clone(), decl.span));
                    import_origins.insert(name.clone(), imp.path.clone());
                    // Trait methods are jumped-to from call sites that reach
                    // for the *signature*. `goto_implementation` covers the
                    // impl side. Insert these *before* impl methods so impls
                    // overwrite only when no trait declared the name — that
                    // way we always land on the canonical signature when one
                    // exists.
                    for item in items {
                        if let ast::TraitItem::Method {
                            name: m_name,
                            name_span,
                            ..
                        } = item
                        {
                            import_defs.insert(
                                m_name.clone(),
                                (canonical.clone(), *name_span),
                            );
                            import_origins.insert(m_name.clone(), imp.path.clone());
                        }
                    }
                }
                DeclKind::Impl { items, .. } => {
                    for item in items {
                        if let ast::ImplItem::Method { name, name_span, .. } = item {
                            // Don't overwrite an existing trait-method
                            // declaration — the trait signature is the
                            // better goto-definition target. `goto_impl`
                            // walks the workspace separately for impls.
                            import_defs
                                .entry(name.clone())
                                .or_insert((canonical.clone(), *name_span));
                            import_origins
                                .entry(name.clone())
                                .or_insert(imp.path.clone());
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

// Diagnostic publishing now lives in `main.rs::publish_diagnostics_dedup` so
// it can hash against the per-URI `published_diag_hashes` cache and skip
// redundant LSP roundtrips. The legacy `publish_diagnostics` helper here was
// removed when that move happened.

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

    #[test]
    fn lru_eviction_drops_least_recently_used() {
        // Synthesize a tiny cache and verify the eviction picks the entry
        // with the smallest access_clock. We bypass `analyze_document` for
        // this test because it's about pure cache mechanics — synthesizing
        // 128 real source files would be slow without testing more.
        use crate::state::InferenceSnapshot;

        let mut cache: InferenceCache = HashMap::new();
        for i in 0..MAX_INFERENCE_CACHE_ENTRIES + 5 {
            let key = (PathBuf::from(format!("/tmp/x{i}.knot")), i as u64);
            cache.insert(
                key,
                InferenceSnapshot {
                    diagnostics: Vec::new(),
                    type_info: HashMap::new(),
                    local_type_info: HashMap::new(),
                    effect_info: HashMap::new(),
                    effect_sets: HashMap::new(),
                    refined_types: HashMap::new(),
                    refine_targets: HashMap::new(),
                    source_refinements: HashMap::new(),
                    monad_info: HashMap::new(),
                    fingerprint: ModuleFingerprint {
                        decl_hashes: HashMap::new(),
                        decl_signature_hashes: HashMap::new(),
                        decl_deps: HashMap::new(),
                        structure_hash: 0,
                    },
                    access_clock: i as u64,
                },
            );
        }
        // Apply the same eviction the analyzer applies after MAX is hit.
        while cache.len() > MAX_INFERENCE_CACHE_ENTRIES {
            let victim = cache
                .iter()
                .min_by_key(|(_, s)| s.access_clock)
                .map(|(k, _)| k.clone())
                .unwrap();
            cache.remove(&victim);
        }
        // The smallest access_clocks (0, 1, 2, 3, 4) should have been
        // evicted, leaving 5..132.
        let surviving: Vec<u64> = cache.values().map(|s| s.access_clock).collect();
        let min_surviving = surviving.iter().min().copied().unwrap_or_default();
        assert!(
            min_surviving >= 5,
            "expected oldest entries evicted; min surviving access_clock is {min_surviving}"
        );
    }

    #[test]
    fn fingerprint_cache_reuses_snapshot_for_whitespace_only_edit() {
        // The inference cache is keyed on a canonical (resolved-on-disk)
        // path. Use a real temp file so `canonicalize()` succeeds.
        let dir = std::env::temp_dir().join(format!(
            "knot-lsp-fp-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("fp.knot");
        std::fs::write(&path, "").unwrap();
        let canonical = path.canonicalize().unwrap();
        let uri = fake_uri(&format!("file://{}", canonical.display()));

        let mut import_cache: HashMap<PathBuf, (u64, Module, String)> = HashMap::new();
        let mut inference_cache: InferenceCache = HashMap::new();

        let v1 = "id = \\x -> x\nmain = id 1\n";
        std::fs::write(&path, v1).unwrap();
        let doc1 = analyze_document(&uri, v1, &mut import_cache, &mut inference_cache);
        let entries_after_v1 = inference_cache.len();
        assert!(entries_after_v1 >= 1, "v1 should populate the cache");
        assert!(
            doc1.type_info.contains_key("id"),
            "v1 type_info should contain id, got: {:?}",
            doc1.type_info.keys().collect::<Vec<_>>()
        );

        // Same shape, different whitespace and a comment — content hash
        // differs, but the fingerprint matches. The pipeline should reuse
        // the existing snapshot rather than insert a new one.
        let v2 = "-- a comment\nid    =   \\x -> x\n\n\nmain = id 1\n";
        std::fs::write(&path, v2).unwrap();
        let doc = analyze_document(&uri, v2, &mut import_cache, &mut inference_cache);
        // Reused snapshot means inferred type info is still populated even
        // though no fresh inference ran (and no new entry was inserted).
        assert!(
            doc.type_info.contains_key("id"),
            "v2 type_info should contain id (fingerprint reuse), got: {:?}",
            doc.type_info.keys().collect::<Vec<_>>()
        );
        assert!(doc.type_info.contains_key("main"));
        assert_eq!(
            inference_cache.len(),
            entries_after_v1,
            "fingerprint cache hit should not insert a new entry"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn changed_decl_names_populated_on_body_edit() {
        let dir = std::env::temp_dir().join(format!(
            "knot-lsp-changed-decls-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.knot");
        let canonical = {
            std::fs::write(&path, "").unwrap();
            path.canonicalize().unwrap()
        };
        let uri = fake_uri(&format!("file://{}", canonical.display()));

        let mut import_cache: HashMap<PathBuf, (u64, Module, String)> = HashMap::new();
        let mut inference_cache: InferenceCache = HashMap::new();

        let v1 = "double = \\x -> x * 2\nfoo = \\y -> y\n";
        std::fs::write(&path, v1).unwrap();
        let doc1 = analyze_document(&uri, v1, &mut import_cache, &mut inference_cache);
        // First analysis: no prior snapshot → empty changed set.
        assert!(
            doc1.changed_decl_names.is_empty(),
            "first analysis should have no changes; got: {:?}",
            doc1.changed_decl_names
        );

        // Edit only `double`'s body. `foo` should not appear in the change
        // set; `double` should.
        let v2 = "double = \\x -> x * 3\nfoo = \\y -> y\n";
        std::fs::write(&path, v2).unwrap();
        let doc2 = analyze_document(&uri, v2, &mut import_cache, &mut inference_cache);
        assert!(
            doc2.changed_decl_names.contains(&"double".to_string()),
            "double should be in change set; got: {:?}",
            doc2.changed_decl_names
        );
        assert!(
            !doc2.changed_decl_names.contains(&"foo".to_string()),
            "foo should NOT be in change set; got: {:?}",
            doc2.changed_decl_names
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
