//! Document analysis pipeline. Lex → parse → import resolution → desugar →
//! type/effect/stratification check, plus the off-disk file caching that backs
//! cross-file features.

use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use lsp_types::Uri;

use knot::ast::{self, Span};
use knot::diagnostic::{self, Diagnostic};
use knot_compiler::effects::EffectSet;
use knot_compiler::infer::MonadKind;

use crate::defs::{build_details, resolve_definitions};
use crate::incremental::ModuleFingerprint;
use crate::state::{
    content_hash, AnalysisResult, AnalysisTask, DocumentState, ImportCache, ImportCacheEntry,
    InferenceCache, InferenceSnapshot, ANALYSIS_DEBOUNCE, ANALYSIS_MAX_WAIT,
};
use crate::utils::{
    collect_keyword_operator_positions, extract_doc_comments, uri_to_path,
};

/// Soft cap on cached inference snapshots — undo/redo and rapid file
/// switching are well-served by even a small cache; we don't need to retain
/// the whole edit history.
const MAX_INFERENCE_CACHE_ENTRIES: usize = 128;

/// Soft cap on cached parsed files. Each entry holds the AST plus the source
/// text, so the per-entry footprint is much larger than an inference snapshot;
/// we keep the cap a bit higher than `MAX_INFERENCE_CACHE_ENTRIES` because
/// import-resolution touches transitively-imported files that aren't open in
/// the editor and would otherwise re-parse on every cross-file query.
const MAX_IMPORT_CACHE_ENTRIES: usize = 256;

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

/// Same monotonic-clock helper, for `ImportCache`. Separate from
/// `next_access_clock` because the cache types are unrelated, and inlining
/// `.values().map(...).max()` everywhere obscures intent.
fn next_import_clock(cache: &ImportCache) -> u64 {
    cache
        .values()
        .map(|e| e.access_clock)
        .max()
        .unwrap_or(0)
        .wrapping_add(1)
}

/// LRU eviction for `ImportCache`. Drops the least-recently-touched entries
/// until the cache is back under `MAX_IMPORT_CACHE_ENTRIES`. Called from
/// every insertion site (`analyze_document`, `get_or_parse_file`,
/// `get_or_parse_file_shared`) so growth stays bounded regardless of which
/// path inserts; without this the cache grew linearly with unique files
/// touched across the entire session (workspace symbol search alone visits
/// every `.knot` file in the workspace once).
fn enforce_import_cache_cap(cache: &mut ImportCache) {
    while cache.len() > MAX_IMPORT_CACHE_ENTRIES {
        let victim = cache
            .iter()
            .min_by_key(|(_, e)| e.access_clock)
            .map(|(k, _)| k.clone());
        match victim {
            Some(k) => {
                cache.remove(&k);
            }
            None => break,
        }
    }
}

/// LRU eviction for `InferenceCache`. Drops the least-recently-accessed
/// entries until the cache is back under `MAX_INFERENCE_CACHE_ENTRIES`.
/// The in-place insertion path (`analyze_document`) evicts before it inserts,
/// but merge-back in `analysis_worker` only inserts into the shared cache and
/// never evicts, so the shared map grew unbounded across a long session (each
/// task then clones the whole map, driving RSS and per-keystroke latency up).
/// Mirrors `enforce_import_cache_cap` so growth stays bounded regardless of
/// which path inserts.
fn enforce_inference_cache_cap(cache: &mut InferenceCache) {
    while cache.len() > MAX_INFERENCE_CACHE_ENTRIES {
        let victim = cache
            .iter()
            .min_by_key(|(_, s)| s.access_clock)
            .map(|(k, _)| k.clone());
        match victim {
            Some(k) => {
                cache.remove(&k);
            }
            None => break,
        }
    }
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
    import_cache: Arc<Mutex<ImportCache>>,
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

            // Snapshot the import-cache hash for each key so we can detect
            // which entries the worker actually mutated during analysis. The
            // shared cache may be touched by request handlers (hover, rename,
            // completion) while we're running, so blanket-overwriting at merge
            // time would clobber their fresher entries. Inference-cache keys
            // include the content hash, so its entries are content-addressed
            // and never need overwrite-vs-skip resolution.
            let import_cache_before: HashMap<PathBuf, u64> = cache_local
                .iter()
                .map(|(k, e)| (k.clone(), e.content_hash))
                .collect();

            // Snapshot the inference-cache keys present *before* this task
            // runs. At merge time only keys absent from this snapshot — i.e.
            // entries the worker actually computed fresh during THIS task —
            // are written back. Blanket-merging the whole clone (the old
            // `or_insert` loop) resurrected entries the main thread
            // deliberately evicted while we were running (see
            // `requeue_dependents_for_changed_decls`), handing re-queued
            // dependents exact-key stale cache hits.
            let inference_keys_before: std::collections::HashSet<(PathBuf, u64)> =
                inf_cache_local.keys().cloned().collect();

            // Run the compiler pipeline behind a panic boundary. If any
            // stage (parser, inference, effect check, stratify, sql_lint)
            // panics on malformed input, recover and emit a synthetic
            // diagnostic instead of letting the worker thread die — a
            // dead worker silently stops processing all subsequent edits
            // for the entire session.
            let task_uri = task.uri.clone();
            let task_source = task.source.clone();
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                analyze_document(
                    &task_uri,
                    &task_source,
                    &mut cache_local,
                    &mut inf_cache_local,
                )
            }));

            let (doc, merge_caches) = match result {
                Ok(doc) => (doc, true),
                Err(payload) => {
                    let msg = panic_message(&payload);
                    eprintln!(
                        "knot-lsp: analysis panicked for {}: {msg}",
                        task.uri.as_str()
                    );
                    // Discard locally-mutated caches: a panic mid-analysis
                    // could have left them in an inconsistent state, and
                    // skipping the merge keeps the shared caches at their
                    // last-known-good contents.
                    (panic_recovery_state(&task.source, &msg), false)
                }
            };

            // Merge entries back: for the import cache, overwrite only the
            // keys whose hash changed during analysis (the file we just
            // re-parsed and any imports re-read off disk). The previous
            // `or_insert` left a stale (hash, module) for the edited file
            // in the shared cache, so dependents importing it kept seeing
            // the pre-edit AST until the editor was restarted.
            if merge_caches {
                if let Ok(mut shared) = import_cache.lock() {
                    for (k, v) in cache_local.into_iter() {
                        let before_hash = import_cache_before.get(&k).copied();
                        if before_hash != Some(v.content_hash) {
                            shared.insert(k, v);
                        }
                    }
                    // Worker-side eviction operates on the local clone, so the
                    // shared cache can drift over the cap when handlers
                    // (rename, completion, workspace symbol) insert in
                    // parallel between merges. Re-enforce the cap on the
                    // shared cache here so growth stays bounded across
                    // sessions regardless of which path inserts.
                    enforce_import_cache_cap(&mut shared);
                }
                if let Ok(mut shared) = inference_cache.lock() {
                    for (k, v) in inf_cache_local.into_iter() {
                        // Only insert keys this task computed fresh. Keys
                        // that were already in the pre-task clone must not
                        // be re-inserted: the main thread may have evicted
                        // them mid-flight to force re-inference of a
                        // dependent, and resurrecting them would serve that
                        // dependent a stale snapshot. (Entries are content-
                        // addressed, so a fresh key can't conflict with a
                        // newer shared value.)
                        if !inference_keys_before.contains(&k) {
                            shared.entry(k).or_insert(v);
                        }
                    }
                    // Merge-back only inserts, so the shared inference cache
                    // could drift over the cap the same way the import cache
                    // did. Re-enforce the LRU cap here so growth stays bounded
                    // across sessions regardless of which path inserts.
                    enforce_inference_cache_cap(&mut shared);
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

/// Best-effort extraction of a panic payload's message. `panic!` payloads are
/// usually `&'static str` or `String`; anything else falls back to a generic
/// label so the diagnostic still surfaces the failure.
pub fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic with non-string payload".to_string()
    }
}

/// Synthesize a minimal `DocumentState` after the analysis pipeline panicked.
/// Carries a single error diagnostic at the start of the file so the editor
/// surfaces a visible failure instead of going silent. All cross-cutting
/// data (definitions, references, type info, etc.) is empty — features that
/// depend on them degrade gracefully via existing `None` / empty-map paths.
fn panic_recovery_state(source: &str, message: &str) -> DocumentState {
    let span = Span::new(0, 0);
    let diag = Diagnostic::error(format!(
        "internal LSP error during analysis: {message}"
    ))
    .label(span, "analysis aborted here")
    .note("this is a bug in the language server; other files are unaffected");

    DocumentState {
        source: source.to_string(),
        module: knot::ast::Spanned {
            node: knot::ast::ExprKind::Record(Vec::new()),
            span: Span::new(0, 0),
        },
        references: Vec::new(),
        definitions: HashMap::new(),
        details: HashMap::new(),
        type_info: HashMap::new(),
        local_type_info: HashMap::new(),
        local_type_info_sorted: Vec::new(),
        literal_types: Vec::new(),
        effect_info: HashMap::new(),
        effect_sets: HashMap::new(),
        knot_diagnostics: vec![diag],
        doc_comments: HashMap::new(),
        keyword_tokens: Vec::new(),
        refined_types: HashMap::new(),
        refine_targets: HashMap::new(),
        source_refinements: HashMap::new(),
        monad_info: HashMap::new(),
        unit_info: HashMap::new(),
        changed_decl_names: Vec::new(),
        dirty_decl_closure: std::collections::HashSet::new(),
    }
}

// ── Document analysis ───────────────────────────────────────────────

pub fn analyze_document(
    uri: &Uri,
    source: &str,
    import_cache: &mut ImportCache,
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
    let mut dirty_decl_closure: std::collections::HashSet<String> = std::collections::HashSet::new();

    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, lex_diags) = lexer.tokenize();
    all_diags.extend(lex_diags);

    let keyword_tokens = collect_keyword_operator_positions(&tokens);

    let canonical_path = uri_to_path(uri).and_then(|p| p.canonicalize().ok());
    let src_hash = content_hash(source);
    let (module, parse_diags) = match canonical_path.as_ref() {
        Some(p) => {
            // Bump access_clock on hits so the LRU eviction below preserves
            // entries the user is actively touching, even when the content
            // hasn't changed (re-analysis for the same buffer is the common
            // case, e.g. `didChange` arriving back in the worker).
            let new_clock = next_import_clock(import_cache);
            let cache_hit = import_cache
                .get_mut(p)
                .filter(|e| e.content_hash == src_hash)
                .map(|e| {
                    e.access_clock = new_clock;
                    e.module.clone()
                });
            match cache_hit {
                Some(cached_module) => {
                    let parser = knot::parser::Parser::new(source.to_string(), tokens);
                    let (_reparsed, parse_d) = parser.parse_file_expr();
                    (cached_module, parse_d)
                }
                None => {
                    let parser = knot::parser::Parser::new(source.to_string(), tokens);
                    let (m, parse_d) = parser.parse_file_expr();
                    import_cache.insert(
                        p.clone(),
                        ImportCacheEntry {
                            content_hash: src_hash,
                            module: m.clone(),
                            source: source.to_string(),
                            access_clock: new_clock,
                        },
                    );
                    enforce_import_cache_cap(import_cache);
                    (m, parse_d)
                }
            }
        }
        None => {
            let parser = knot::parser::Parser::new(source.to_string(), tokens);
            parser.parse_file_expr()
        }
    };
    all_diags.extend(parse_diags);

    let (definitions, references, literal_types) = resolve_definitions(&module, source);
    let details = build_details(&module);
    let doc_comments = extract_doc_comments(source, &module);

    let has_parse_errors = all_diags
        .iter()
        .any(|d| matches!(d.severity, diagnostic::Severity::Error));

    if !has_parse_errors {
        // Inference-snapshot fast path. Content-hash exact match only:
        // same source bytes as a prior analysis (undo/redo, format-on-save
        // round trips, rapid file switching) reuse the cached inference
        // output verbatim.
        //
        // A second tier keyed on `ModuleFingerprint::structurally_equal`
        // (same AST shape, different bytes — i.e. whitespace/comment edits)
        // used to fire here too. It was unsound: the snapshot's span-keyed
        // data (`local_type_info`, `monad_info`, `refine_targets`, diagnostic
        // labels, plus spans nested inside refined-type predicate `Expr`s)
        // pointed into the *old* source. Reusing it after a whitespace edit
        // surfaced diagnostics anchored to byte offsets that no longer held
        // the offending tokens — squiggles drifted, hover/inlay hints
        // resolved against the wrong characters, and warnings persisted
        // after the underlying issue moved off the position they pointed
        // at. Until we add a span remapper that pairs old/new spans via a
        // parallel AST walk, structural reuse runs the inference pipeline
        // fresh.
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
        if let Some(key) = &cache_key
            && let Some(latest) = inference_cache
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
        if let Some(key) = &cache_key {
            // Exact content-hash match: reuse the cached snapshot verbatim.
            // Same source bytes ⇒ same spans, so every Span key in the
            // cached HashMaps still indexes the right characters.
            let new_clock = next_access_clock(inference_cache);
            if let Some(snap) = inference_cache.get_mut(key) {
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
                    doc_comments,
                    keyword_tokens,
                    all_diags,
                    changed_decl_names,
                    dirty_decl_closure,
                );
            }
        }

        let mut analysis_module = module.clone();

        knot_compiler::base::inject_prelude(&mut analysis_module);
        knot_compiler::desugar::desugar(&mut analysis_module);

        // Capture the start of inference diagnostics so the cached snapshot
        // only contains what's contributed by this run (lex + parse diags
        // are repopulated on every call from fresh state).
        let pre_inference_len = all_diags.len();

        // Inference/effects/stratify/sql_lint run on `analysis_module` — the
        // prelude-injected module. Binding spans and diagnostic labels recorded
        // for prelude decls are byte offsets into OTHER sources; treating them
        // as offsets into this document produces ghost inlay hints, wrong hover
        // types, and — for diagnostics — phantom errors that relocate to 0:0
        // when mapped against this file's source. Keep only entries anchored
        // inside one of the *user's* own decl spans (`module` is the
        // pre-injection parse of this file). Mirrors `workspace_diagnostics`'
        // `anchored_in_importer` filter.
        let user_decl_spans: Vec<Span> = crate::utils::top_fields(&module).iter().map(|d| d.value.span).collect();
        let in_user_decl = |s: &Span| {
            user_decl_spans
                .iter()
                .any(|d| d.start <= s.start && s.end <= d.end)
        };
        let anchored_in_user = |d: &Diagnostic| -> bool {
            d.labels.iter().any(|l| in_user_decl(&l.span))
        };

        let (
            infer_diags,
            mi,
            inferred_types,
            local_types,
            rt,
            refined_type_info,
            _from_json,
            _elem_pushdown,
            _show_units,
            _sum_floats,
            _relation_fields,
            _with_fields,
            _type_args,
            _implicit_refs,
            _implicit_dict_args,
        ) = knot_compiler::infer::check(&mut analysis_module);
        all_diags.extend(infer_diags.into_iter().filter(anchored_in_user));
        type_info = inferred_types;
        local_type_info = local_types;
        refined_types = refined_type_info;
        refine_targets = rt;
        monad_info = mi;

        local_type_info.retain(|s, _| in_user_decl(s));
        refine_targets.retain(|s, _| in_user_decl(s));
        monad_info.retain(|s, _| in_user_decl(s));

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
        all_diags.extend(effect_diags.into_iter().filter(anchored_in_user));
        for (name, eff) in &effects {
            if !eff.is_pure() {
                effect_info.insert(name.clone(), format!("{eff}"));
            }
            effect_sets.insert(name.clone(), eff.clone());
        }

        all_diags.extend(
            knot_compiler::stratify::check(&analysis_module)
                .into_iter()
                .filter(anchored_in_user),
        );

        // Unused-definition warnings: run on the user's pre-prelude decls so
        // we don't flag prelude/imported names. Cached as part of
        // `all_diags[pre_inference_len..]` along with the other inference
        // diagnostics.
        all_diags.extend(knot_compiler::unused::check(&module));

        all_diags.extend(
            knot_compiler::types::check_reserved_field_names(&analysis_module)
                .into_iter()
                .filter(anchored_in_user),
        );

        let type_env = knot_compiler::types::TypeEnv::from_program(&analysis_module);
        source_refinements = type_env.source_refinements.clone();

        all_diags.extend(
            knot_compiler::sql_lint::check(&analysis_module, &type_env)
                .into_iter()
                .filter(anchored_in_user),
        );

        // Stash the inferred outputs for the next analysis of this same
        // (path, content_hash) pair. Eviction is bounded — if we're at the
        // soft cap, drop the least-recently-accessed entry. LRU keeps
        // hot files (the ones the user is actively editing or jumping
        // between) resident through long sessions; previously the random
        // eviction could discard a fresh snapshot before its first hit.
        if let Some(key) = cache_key {
            if inference_cache.len() >= MAX_INFERENCE_CACHE_ENTRIES
                && let Some((victim, _)) = inference_cache
                    .iter()
                    .min_by_key(|(_, s)| s.access_clock)
                    .map(|(k, s)| (k.clone(), s.access_clock))
                {
                    inference_cache.remove(&victim);
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
                unit_info: unit_info.clone(),
                fingerprint: new_fingerprint.clone(),
                access_clock: next_access_clock(inference_cache),
            };
            inference_cache.insert(key, snapshot);
        }
    }

    let local_type_info_sorted = sort_local_type_info(&local_type_info);
    DocumentState {
        source: source.to_string(),
        module,
        references,
        definitions,
        details,
        type_info,
        local_type_info,
        local_type_info_sorted,
        literal_types,
        effect_info,
        effect_sets,
        knot_diagnostics: all_diags,
        doc_comments,
        keyword_tokens,
        refined_types,
        refine_targets,
        source_refinements,
        monad_info,
        unit_info,
        changed_decl_names,
        dirty_decl_closure,
    }
}

/// Build a `(Span, type)` list sorted by `span.start`. Consumers (notably the
/// inlay-hint handler) binary-search this to clip to the visible byte range
/// instead of linear-scanning every binding on every cursor move.
fn sort_local_type_info(map: &HashMap<Span, String>) -> Vec<(Span, String)> {
    let mut sorted: Vec<(Span, String)> = map.iter().map(|(s, t)| (*s, t.clone())).collect();
    sorted.sort_by_key(|(s, _)| s.start);
    sorted
}

/// Compose a `DocumentState` from a cached inference snapshot. Pulled out
/// of `analyze_document` so the two cache-hit tiers (content hash and
/// structural fingerprint) can share the result-assembly path verbatim.
///
/// `changed_decl_names`/`signature_changed_decl_names`/`dirty_decl_closure`
/// reflect the diff against the *most recent* snapshot for the same path,
/// not against the snapshot we're reusing — when the user reverts a file
/// (A v1 → A v2 → A v1), the cache-hit on v1 still represents a "change"
/// from the perspective of dependents that were re-checked against v2's
/// signatures. Without these fields propagating, `apply_analysis_result`
/// would skip re-queuing dependents and any errors they picked up from v2
/// would linger after the revert.
#[allow(clippy::too_many_arguments)]
fn reuse_snapshot(
    snap: &InferenceSnapshot,
    source: &str,
    module: ast::Expr,
    references: Vec<(Span, Span)>,
    definitions: HashMap<String, Span>,
    details: HashMap<String, String>,
    literal_types: Vec<(Span, String)>,
    doc_comments: HashMap<String, String>,
    keyword_tokens: Vec<(Span, u32)>,
    mut all_diags: Vec<diagnostic::Diagnostic>,
    changed_decl_names: Vec<String>,
    dirty_decl_closure: std::collections::HashSet<String>,
) -> DocumentState {
    all_diags.extend(snap.diagnostics.iter().cloned());
    let local_type_info_sorted = sort_local_type_info(&snap.local_type_info);
    DocumentState {
        source: source.to_string(),
        module,
        references,
        definitions,
        details,
        type_info: snap.type_info.clone(),
        local_type_info: snap.local_type_info.clone(),
        local_type_info_sorted,
        literal_types,
        effect_info: snap.effect_info.clone(),
        effect_sets: snap.effect_sets.clone(),
        knot_diagnostics: all_diags,
        doc_comments,
        keyword_tokens,
        refined_types: snap.refined_types.clone(),
        refine_targets: snap.refine_targets.clone(),
        source_refinements: snap.source_refinements.clone(),
        monad_info: snap.monad_info.clone(),
        unit_info: snap.unit_info.clone(),
        changed_decl_names,
        dirty_decl_closure,
    }
}

// ── File caching ────────────────────────────────────────────────────

/// Like `get_or_parse_file`, but operates against the `Arc<Mutex<…>>` cache held
/// by `ServerState`. Reads the file and (on cache miss) parses it *outside* the
/// lock so concurrent callers don't serialize on disk-IO + parse work for
/// unrelated files. The lock is held only for the cheap hash-key lookup and
/// final insert.
pub fn get_or_parse_file_shared(
    path: &Path,
    cache: &Arc<Mutex<ImportCache>>,
) -> Option<(ast::Expr, String)> {
    let source = std::fs::read_to_string(path).ok()?;
    let hash = content_hash(&source);
    {
        let mut guard = cache.lock().ok()?;
        let new_clock = next_import_clock(&guard);
        if let Some(entry) = guard.get_mut(path)
            && entry.content_hash == hash {
                entry.access_clock = new_clock;
                return Some((entry.module.clone(), entry.source.clone()));
            }
    }
    let lexer = knot::lexer::Lexer::new(&source);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(source.clone(), tokens);
    let (module, _) = parser.parse_file_expr();
    if let Ok(mut guard) = cache.lock() {
        let new_clock = next_import_clock(&guard);
        guard.insert(
            path.to_path_buf(),
            ImportCacheEntry {
                content_hash: hash,
                module: module.clone(),
                source: source.clone(),
                access_clock: new_clock,
            },
        );
        enforce_import_cache_cap(&mut guard);
    }
    Some((module, source))
}

