//! Shared LSP state: per-document analysis output, server-wide bookkeeping,
//! analysis-task plumbing, and grab-bag string constants for completion.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use crossbeam_channel::Sender;
use lsp_server::Connection;
use lsp_types::{Diagnostic, SemanticToken, Uri};

use knot::ast::{self, Module, Span};
use knot::diagnostic;
use knot_compiler::effects::EffectSet;
use knot_compiler::infer::MonadKind;

use crate::incremental::ModuleFingerprint;

// ── Inference snapshot cache ────────────────────────────────────────

/// A frozen snapshot of the type-/effect-inference output for a particular
/// source content. Cached by `(path, content_hash)` so re-analyzing the same
/// source bytes (undo/redo, file watcher round-trip, rapid file switches)
/// can skip the expensive HM + effect + stratify + sql_lint pipeline.
///
/// True per-declaration incrementalism — where a single edit to one
/// declaration only re-checks that decl plus its transitive dependents —
/// requires restructuring `infer.rs::pre_register` and `infer_declarations`
/// to use a dependency-aware work queue. That's a separate, larger project;
/// this snapshot cache covers the common "no real change" cases for now.
#[derive(Clone)]
pub struct InferenceSnapshot {
    pub diagnostics: Vec<diagnostic::Diagnostic>,
    pub type_info: HashMap<String, String>,
    pub local_type_info: HashMap<Span, String>,
    pub effect_info: HashMap<String, String>,
    pub effect_sets: HashMap<String, EffectSet>,
    pub refined_types: HashMap<String, ast::Expr>,
    pub refine_targets: HashMap<Span, String>,
    pub source_refinements: HashMap<String, Vec<(Option<String>, String, ast::Expr)>>,
    pub monad_info: HashMap<Span, MonadKind>,
    /// Per-decl AST hash + dependency graph for the snapshot's source.
    /// When a fresh edit produces a structurally-equal fingerprint
    /// (whitespace/comment-only changes), the snapshot can be reused even
    /// though the raw content hash differs.
    pub fingerprint: ModuleFingerprint,
    /// LRU access counter — bumped on each cache hit. The eviction policy
    /// in `analyze_document` drops the entry with the lowest counter when
    /// the cache reaches its bound, so frequently-touched files stay
    /// resident through long editing sessions.
    pub access_clock: u64,
}

pub type InferenceCache = HashMap<(PathBuf, u64), InferenceSnapshot>;

// The structural-fingerprint lookup currently linear-scans `InferenceCache`,
// which is fine while `MAX_INFERENCE_CACHE_ENTRIES` stays at 128. A keyed
// secondary index `HashMap<(PathBuf, structure_hash), content_hash>` would
// turn that into O(1), but at this size it's pure overhead — the index also
// has to be maintained on every insert/evict. Wire one up if profiling ever
// shows the linear scan is a bottleneck.

// ── Per-document analysis state ─────────────────────────────────────

pub struct DocumentState {
    pub source: String,
    pub module: Module,
    /// Span-based references: (usage_span → definition_span).
    pub references: Vec<(Span, Span)>,
    /// Fallback name-based definitions for names not covered by AST walk.
    pub definitions: HashMap<String, Span>,
    pub details: HashMap<String, String>,
    pub type_info: HashMap<String, String>,
    /// Span-based type info for local bindings (let, bind, lambda params, case patterns).
    pub local_type_info: HashMap<Span, String>,
    /// Span-based type info for literal expressions.
    pub literal_types: Vec<(Span, String)>,
    /// Per-declaration effect info (formatted strings).
    pub effect_info: HashMap<String, String>,
    /// Per-declaration effect sets (structured form). Keys mirror `effect_info`
    /// but the value is the underlying `EffectSet` so callers can do set
    /// operations (e.g. atomic-context filtering).
    pub effect_sets: HashMap<String, EffectSet>,
    pub knot_diagnostics: Vec<diagnostic::Diagnostic>,
    /// Imported files: canonical path → source text
    pub imported_files: HashMap<PathBuf, String>,
    /// Definitions from imported files: name → (canonical path, span in that file)
    pub import_defs: HashMap<String, (PathBuf, Span)>,
    /// Which import path each name originated from (for scoped cross-file matching).
    pub import_origins: HashMap<String, String>,
    /// Doc comments for declarations: name → comment text.
    pub doc_comments: HashMap<String, String>,
    /// Keyword/operator token positions for semantic highlighting.
    pub keyword_tokens: Vec<(Span, u32)>,
    /// Refined-type-alias predicates: type-name → predicate expression. Includes
    /// every named refined type alias declared (or imported) in this module.
    pub refined_types: HashMap<String, ast::Expr>,
    /// `refine expr` target types: span-of-refine-expr → target refined-type name.
    pub refine_targets: HashMap<Span, String>,
    /// Per-source field refinements: source-name → [(field-name, refined-type-name, predicate-expr)].
    /// `field-name = None` denotes a whole-element refinement.
    pub source_refinements: HashMap<String, Vec<(Option<String>, String, ast::Expr)>>,
    /// Monad context resolved for each `do` block: span-of-do → kind (Relation/IO/Adt).
    /// Drives monad-aware completion ranking inside do-blocks.
    pub monad_info: HashMap<Span, MonadKind>,
    /// Inferred per-binding units (Float/Int) for inlay-hint display. Keyed by the
    /// binding-site span; value is a normalized unit string like `M/S^2`.
    /// Populated by `analyze_document` from the formatted local-type strings;
    /// the inlay-hint handler reads this directly to avoid re-parsing the type
    /// string on every request.
    pub unit_info: HashMap<Span, String>,
    /// Top-level decl names whose AST shape changed between this analysis
    /// and the previous one for the same file. Empty when no prior snapshot
    /// exists or the fingerprints matched (typical first analysis or
    /// whitespace-only edits). Used for the in-file dirty closure (telemetry
    /// + future selective re-inference). Production code currently routes
    /// dependent re-queue through `signature_changed_decl_names` (a strict
    /// subset that excludes body-only changes to typed decls); this broader
    /// field stays populated for tests and the planned in-file selective
    /// inference pass.
    #[allow(dead_code)]
    pub changed_decl_names: Vec<String>,
    /// Strict subset of `changed_decl_names` containing only those decls
    /// whose externally-visible signature moved. A typed `Fun` whose body
    /// changed but whose signature is intact lands in `changed_decl_names`
    /// but NOT here — its dependents needn't be re-analyzed because the
    /// outward type is unchanged. Drives `apply_analysis_result`'s
    /// cross-file dependent re-queue.
    pub signature_changed_decl_names: Vec<String>,
    /// Transitive in-file closure of `changed_decl_names` — every decl whose
    /// inferred type or effects could conceivably have shifted since the
    /// previous analysis, accounting for the per-decl reverse-dependency
    /// graph. Populated by `analyze_document` from the fingerprint, and
    /// consumed by the inlay-hint handler (gated on `KNOT_LSP_TRACE_DIRTY`)
    /// to surface a "♻" hint on freshly re-checked decls. Once `infer.rs`
    /// learns to skip clean decls, this same set becomes the input for the
    /// selective inference pass.
    pub dirty_decl_closure: std::collections::HashSet<String>,
}

// ── Server-wide state ───────────────────────────────────────────────

pub struct ServerState {
    pub documents: HashMap<Uri, DocumentState>,
    /// First (primary) workspace folder. Kept for handlers that only need a
    /// single root — most workspace scans do, since `.knot` files are
    /// typically rooted at one folder per project.
    pub workspace_root: Option<PathBuf>,
    /// All workspace folders the editor handed us (LSP supports multi-root).
    /// Handlers that walk the whole workspace (workspace symbol, auto-import
    /// completion, workspace diagnostics) should iterate this list so users
    /// with multi-root workspaces don't lose visibility into other roots.
    pub workspace_roots: Vec<PathBuf>,
    /// Editor-supplied configuration (didChangeConfiguration payload).
    /// Read on every request so live changes take effect without a restart.
    pub config: ServerConfig,
    /// Cached parsed files: canonical path → (content hash, parsed module, source text).
    /// Keyed by content hash rather than mtime — mtime is unreliable across
    /// `jj`/`git` checkouts that touch timestamps without changing content.
    /// Shared with the analysis worker thread; populated lazily by every
    /// caller that reads a `.knot` file (imports, rename, workspace symbol,
    /// workspace diagnostics, completion).
    pub import_cache: Arc<Mutex<HashMap<PathBuf, (u64, Module, String)>>>,
    /// Cached LSP diagnostics for unopened workspace files, keyed by content
    /// hash. The third tuple field is a monotonic access counter used for LRU
    /// eviction in `prune_stale_workspace_diag_cache`. Bumped on every cache
    /// hit so frequently-queried files survive cap-based pruning.
    pub workspace_diag_cache: HashMap<PathBuf, (u64, Vec<Diagnostic>, u64)>,
    /// Monotonic counter incremented on every cache access. Provides a
    /// total order for LRU eviction without paying for `Instant::now()` on
    /// hot paths.
    pub workspace_diag_clock: u64,
    /// Cached workspace symbol index, rebuilt incrementally from file watcher
    /// notifications and on-demand. Avoids walking the disk on every
    /// `workspace/symbol` query. Wrapped in `Arc<Mutex>` so a background
    /// indexing thread can pre-warm the cache at startup without contending
    /// with the main thread.
    pub workspace_symbol_cache: Arc<Mutex<WorkspaceSymbolCache>>,
    /// Edited but not-yet-analyzed sources. When present, this is the source
    /// the next analysis run will see. Subsequent didChange edits stack on top
    /// of this rather than the (stale) analyzed source.
    pub pending_sources: HashMap<Uri, PendingSource>,
    /// Sender side of the analysis-task channel. Cloned per outgoing task.
    pub analysis_tx: Sender<AnalysisTask>,
    /// Reverse-import graph: importer → set of imported files. Built from the
    /// `imported_files` of every doc + the on-disk modules in `import_cache`.
    /// Used by cross-file diagnostics to re-check downstream consumers when
    /// a file changes. The map stores absolute canonical paths for both keys
    /// and values so it works uniformly for open and unopened files.
    pub reverse_imports: HashMap<PathBuf, std::collections::HashSet<PathBuf>>,
    /// Cached inference snapshots keyed by (canonical path, content hash).
    /// Skips re-running the type/effect/stratify/sql_lint pipeline when the
    /// source bytes match a previous successful analysis. Bounded eviction
    /// happens lazily inside the worker — see `analysis::analyze_document`.
    /// Held by the main thread purely to keep the cache alive; the analysis
    /// worker holds its own `Arc` clone and is the only thing that mutates it.
    #[allow(dead_code)]
    pub inference_cache: Arc<Mutex<InferenceCache>>,
    /// Last semantic-tokens response per URI: `result_id → tokens`. Drives
    /// `textDocument/semanticTokens/full/delta` so editors can re-fetch
    /// changes instead of the whole file's tokens on every edit. Pruned on
    /// document close.
    pub semantic_token_cache: HashMap<Uri, (String, Vec<SemanticToken>)>,
    /// Monotonic counter feeding `semantic_token_cache` result-ids. Each
    /// request bumps this; the resulting string is used as the next
    /// `result_id` field.
    pub semantic_token_counter: u64,
    /// Hash of the most recently published diagnostics list per URI. Used to
    /// short-circuit `publish_diagnostics` when an edit produced an identical
    /// diagnostic set — common for whitespace/comment edits where the
    /// fingerprint cache already reused the snapshot. Avoids gratuitous LSP
    /// traffic and editor re-renders. Pruned on document close.
    pub published_diag_hashes: HashMap<Uri, u64>,
    /// Last LSP diagnostics published per URI, kept verbatim so didChange can
    /// rebase their `Range` fields through pending edits and republish them
    /// against the new document version. Without this, diagnostics' positions
    /// drift while the analysis worker catches up — the editor still sees the
    /// old line/character ranges even though the document has moved on.
    /// Cleared on document close alongside `published_diag_hashes`.
    pub published_lsp_diagnostics: HashMap<Uri, Vec<Diagnostic>>,
    /// Whether the client supports `workspace/diagnostic/refresh`. Pull-mode
    /// clients (notably JetBrains) ignore `publishDiagnostics` and only update
    /// when the server explicitly invalidates their cache via this request;
    /// without it, fixed errors keep showing in the gutter until the next
    /// user-initiated pull. Set from `clientCapabilities.workspace.diagnostic.
    /// refreshSupport` at initialize time.
    pub client_supports_diagnostic_refresh: bool,
    /// Monotonic id source for outgoing `workspace/diagnostic/refresh` requests.
    /// Each call bumps this so request ids stay unique across the session.
    pub diagnostic_refresh_counter: u64,
}

/// Symbol entry stored in the workspace symbol cache.
#[derive(Clone)]
pub struct WorkspaceSymbolEntry {
    pub name: String,
    pub kind: lsp_types::SymbolKind,
    pub uri: Uri,
    pub range: lsp_types::Range,
    pub container: Option<String>,
}

/// In-memory workspace symbol index: `path → (mtime, content_hash, [entries])`.
/// On a `workspace/symbol` query we first compare the file's `mtime` against the
/// cached value — if it matches, we reuse the entries without reading or
/// hashing the file. If the mtime moved, we read the file and fall back to the
/// content hash check (mtime can change without the bytes changing, e.g. across
/// `jj`/`git` checkouts), only re-parsing on a real content difference.
#[derive(Default)]
pub struct WorkspaceSymbolCache {
    pub by_path: HashMap<PathBuf, (Option<SystemTime>, u64, Vec<WorkspaceSymbolEntry>)>,
}

pub struct PendingSource {
    pub source: String,
    /// Latest LSP version observed for this URI (`None` for didOpen).
    pub version: Option<i32>,
}

/// Work item handed to the analysis worker.
pub struct AnalysisTask {
    pub uri: Uri,
    pub source: String,
    pub version: Option<i32>,
}

/// Output from the analysis worker.
pub struct AnalysisResult {
    pub uri: Uri,
    pub version: Option<i32>,
    pub doc: DocumentState,
}

// ── Worker constants ────────────────────────────────────────────────

/// Quiet period after the most recent task before processing begins.
pub const ANALYSIS_DEBOUNCE: Duration = Duration::from_millis(150);
/// Hard upper bound on how long a task can sit in the debounce queue. Prevents
/// continuous typing from indefinitely starving the analysis pass.
pub const ANALYSIS_MAX_WAIT: Duration = Duration::from_millis(500);

// ── Configuration ───────────────────────────────────────────────────

/// Editor-side configurable knobs. Populated from `initializationOptions`
/// at startup and refreshed when the editor sends `workspace/didChangeConfiguration`.
/// All fields have sensible defaults so missing payloads degrade gracefully.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Tab width used by the formatter when normalizing indentation.
    pub tab_size: usize,
    /// Whether to expand tabs to spaces (true) or keep them (false). Mirrors
    /// the LSP `formattingOptions.insertSpaces` field.
    pub insert_spaces: bool,
    /// Whether unused-import warnings should fire. Some teams prefer to
    /// disable this in libraries where re-exports look unused.
    pub warn_unused_imports: bool,
    /// Whether the inlay-hint pass should emit parameter-name hints. Hints
    /// are noisy in tight loops, so we let users opt out.
    pub inlay_parameter_names: bool,
    /// Whether the inlay-hint pass should emit type hints (binding sites,
    /// inferred function types).
    pub inlay_types: bool,
    /// Maximum entries kept in the workspace diagnostic cache. Old entries
    /// past this watermark are evicted by an LRU policy.
    pub max_workspace_diag_cache: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            tab_size: 2,
            insert_spaces: true,
            warn_unused_imports: true,
            inlay_parameter_names: true,
            inlay_types: true,
            max_workspace_diag_cache: 256,
        }
    }
}

impl ServerConfig {
    /// Update fields from a JSON `settings` payload. Unknown keys are ignored
    /// so future config additions don't break older clients. Keys live under
    /// the `knot` namespace per LSP convention.
    pub fn merge_from_json(&mut self, value: &serde_json::Value) {
        let knot = value.get("knot").unwrap_or(value);
        if let Some(n) = knot.get("tabSize").and_then(|v| v.as_u64()) {
            self.tab_size = (n as usize).max(1);
        }
        if let Some(b) = knot.get("insertSpaces").and_then(|v| v.as_bool()) {
            self.insert_spaces = b;
        }
        if let Some(b) = knot.get("warnUnusedImports").and_then(|v| v.as_bool()) {
            self.warn_unused_imports = b;
        }
        if let Some(b) = knot.get("inlayParameterNames").and_then(|v| v.as_bool()) {
            self.inlay_parameter_names = b;
        }
        if let Some(b) = knot.get("inlayTypes").and_then(|v| v.as_bool()) {
            self.inlay_types = b;
        }
        if let Some(n) = knot
            .get("maxWorkspaceDiagCache")
            .and_then(|v| v.as_u64())
        {
            self.max_workspace_diag_cache = (n as usize).max(8);
        }
    }
}

// ── Completion / token grab-bag constants ───────────────────────────

pub const KEYWORDS: &[&str] = &[
    "import", "data", "type", "trait", "impl", "route", "migrate", "where", "do", "yield",
    "if", "then", "else", "case", "of", "let", "in", "not", "replace", "atomic", "deriving", "with",
    "export",
];

/// Context tag for a snippet — used by `handle_completion` to filter snippets
/// to ones that make sense at the cursor. `Any` shows everywhere; `TopLevel`
/// only shows at the start of a line outside any decl; `RouteBlock` only
/// inside a `route Foo where` body, etc. This keeps the completion list
/// focused: typing inside an expression doesn't surface a `route` snippet.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum SnippetContext {
    /// Always show.
    Any,
    /// Only at module top level (start of line, no enclosing expression).
    TopLevel,
    /// Inside a function/view/derived body — i.e. expression position.
    Expression,
    /// Inside a `do { ... }` block (any monad).
    DoBlock,
    /// Inside an `atomic { ... }` block — IO is forbidden; only DB ops.
    AtomicBlock,
    /// Inside a `route Foo where` body — already gated by route_completions.
    /// Listed for completeness; not currently filtered against.
    RouteBlock,
}

pub const SNIPPETS: &[(&str, &str, &str, SnippetContext)] = &[
    // ── Expression-position snippets ───────────────────────────────────
    (
        "do",
        "do block",
        "do\n  ${1:x} <- ${2:expr}\n  yield {$3}",
        SnippetContext::Expression,
    ),
    (
        "do-io",
        "IO do block",
        "do\n  ${1:x} <- ${2:readLine}\n  println $1",
        SnippetContext::Expression,
    ),
    (
        "do-where",
        "do block with filter",
        "do\n  ${1:x} <- *${2:source}\n  where ${3:cond}\n  yield $1",
        SnippetContext::Expression,
    ),
    (
        "do-let",
        "do block with let binding",
        "do\n  ${1:x} <- ${2:expr}\n  let ${3:y} = ${4:body}\n  yield $3",
        SnippetContext::Expression,
    ),
    (
        "do-group",
        "do block grouped by key",
        "do\n  ${1:x} <- *${2:source}\n  groupBy {${3:key}: $1.$3}\n  yield {$3: $1.$3, count: count $1}",
        SnippetContext::Expression,
    ),
    (
        "case",
        "case expression",
        "case ${1:expr} of\n  ${2:pattern} -> ${3:body}",
        SnippetContext::Expression,
    ),
    (
        "case-result",
        "case for Result",
        "case ${1:expr} of\n  Ok {value: ${2:x}} -> ${3:body}\n  Err {error: ${4:e}} -> ${5:body}",
        SnippetContext::Expression,
    ),
    (
        "case-maybe",
        "case for Maybe",
        "case ${1:expr} of\n  Just {value: ${2:x}} -> ${3:body}\n  Nothing {} -> ${4:body}",
        SnippetContext::Expression,
    ),
    (
        "case-bool",
        "case for Bool",
        "case ${1:expr} of\n  True {} -> ${2:body}\n  False {} -> ${3:body}",
        SnippetContext::Expression,
    ),
    (
        "lambda",
        "lambda expression",
        "\\\\${1:x} -> ${2:body}",
        SnippetContext::Expression,
    ),
    (
        "if",
        "if expression",
        "if ${1:cond}\n  then ${2:a}\n  else ${3:b}",
        SnippetContext::Expression,
    ),
    (
        "let",
        "let binding (in do block)",
        "let ${1:x} = ${2:expr}",
        SnippetContext::DoBlock,
    ),
    (
        "atomic",
        "atomic block",
        "atomic do\n  ${1:x} <- *${2:source}\n  ${3:body}",
        SnippetContext::Expression,
    ),
    (
        "refine",
        "refine expression",
        "case refine ${1:expr} of\n  Ok {value: ${2:x}} -> ${3:body}\n  Err {error: ${4:e}} -> ${5:body}",
        SnippetContext::Expression,
    ),
    (
        "fold",
        "fold over a relation",
        "fold (\\\\${1:acc} ${2:x} -> ${3:body}) ${4:init} ${5:rel}",
        SnippetContext::Expression,
    ),
    (
        "filter",
        "filter a relation",
        "filter (\\\\${1:x} -> ${2:cond}) ${3:rel}",
        SnippetContext::Expression,
    ),
    (
        "map",
        "map a relation",
        "map (\\\\${1:x} -> ${2:body}) ${3:rel}",
        SnippetContext::Expression,
    ),
    // ── Top-level declaration snippets ─────────────────────────────────
    (
        "data",
        "data declaration",
        "data ${1:Name} = ${2:Ctor} {${3:field}: ${4:Type}}",
        SnippetContext::TopLevel,
    ),
    (
        "data-deriving",
        "data declaration with deriving",
        "data ${1:Name} = ${2:Ctor} {${3:field}: ${4:Type}} deriving (${5:Eq, Show})",
        SnippetContext::TopLevel,
    ),
    (
        "data-enum",
        "data declaration (enum-style)",
        "data ${1:Name} = ${2:A} | ${3:B} | ${4:C}",
        SnippetContext::TopLevel,
    ),
    (
        "type",
        "type alias",
        "type ${1:Name} = ${2:Type}",
        SnippetContext::TopLevel,
    ),
    (
        "type-refined",
        "refined type alias",
        "type ${1:Name} = ${2:Int} where \\\\${3:x} -> ${4:cond}",
        SnippetContext::TopLevel,
    ),
    (
        "source",
        "source declaration",
        "*${1:name} : [${2:Type}]",
        SnippetContext::TopLevel,
    ),
    (
        "source-history",
        "source with history",
        "*${1:name} : [${2:Type}] with history",
        SnippetContext::TopLevel,
    ),
    (
        "view",
        "view declaration",
        "*${1:name} = do\n  ${2:x} <- *${3:source}\n  yield ${4:x}",
        SnippetContext::TopLevel,
    ),
    (
        "derived",
        "derived relation",
        "&${1:name} = do\n  ${2:x} <- *${3:source}\n  yield ${4:x}",
        SnippetContext::TopLevel,
    ),
    (
        "trait",
        "trait declaration",
        "trait ${1:Name} ${2:a} where\n  ${3:method} : ${4:Type}",
        SnippetContext::TopLevel,
    ),
    (
        "impl",
        "impl block",
        "impl ${1:Trait} ${2:Type} where\n  ${3:method} ${4:x} = ${5:body}",
        SnippetContext::TopLevel,
    ),
    (
        "fun",
        "function with type signature",
        "${1:name} : ${2:Type}\n$1 ${3:x} = ${4:body}",
        SnippetContext::TopLevel,
    ),
    (
        "fun-io",
        "IO function with effects",
        "${1:name} : ${2:Args} -> IO {${3:console}} ${4:Result}\n$1 ${5:x} = do\n  ${6:body}",
        SnippetContext::TopLevel,
    ),
    (
        "route",
        "route declaration",
        "route ${1:Name} where\n  GET \"${2:/path}\" -> ${3:Response}",
        SnippetContext::TopLevel,
    ),
    (
        "route-post",
        "POST route with body",
        "route ${1:Name} where\n  POST \"${2:/path}\"\n    body: {${3:field}: ${4:Type}}\n    -> ${5:Response}",
        SnippetContext::TopLevel,
    ),
    (
        "route-composite",
        "composite route",
        "route ${1:Name} = ${2:RouteA} | ${3:RouteB}",
        SnippetContext::TopLevel,
    ),
    (
        "migrate",
        "migration",
        "migrate \"${1:name}\" do\n  ${2:body}",
        SnippetContext::TopLevel,
    ),
    (
        "import",
        "import declaration",
        "import ${1:./path}",
        SnippetContext::TopLevel,
    ),
    (
        "unit",
        "unit of measure",
        "unit ${1:Name}",
        SnippetContext::TopLevel,
    ),
    (
        "unit-derived",
        "derived unit",
        "unit ${1:N} = ${2:Kg} * ${3:M} / ${4:S}^2",
        SnippetContext::TopLevel,
    ),
    (
        "subset",
        "subset constraint",
        "*${1:a}.${2:field} <= *${3:b}.${4:key}",
        SnippetContext::TopLevel,
    ),
];

/// Iterator over every user-callable builtin name. Drawn from the centralized
/// `knot_compiler::builtins` tables so completion lists stay in sync with the
/// effect inferencer, codegen, and atomic-context filter. Intentionally
/// excludes the `__bind`/`__yield`/`__empty` desugar internals (they are
/// callable in source code but not user-facing).
pub fn builtins() -> impl Iterator<Item = &'static str> {
    knot_compiler::builtins::ALL_BUILTINS
        .iter()
        .copied()
        .flatten()
        .copied()
        .filter(|n| !n.starts_with("__"))
}

// ── Hashing utility ─────────────────────────────────────────────────

/// Compute a fast content hash of a source file. Used to key the import cache
/// stably across `jj`/`git` checkouts that touch mtimes without changing content.
pub fn content_hash(s: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

// ── Connection helper ───────────────────────────────────────────────

/// Send a JSON-encoded LSP response. Logs but doesn't propagate JSON failures
/// — those should never happen for `lsp_types`-defined results, but we don't
/// want a malformed message to take down the server.
pub fn send_response<T: serde::Serialize>(
    conn: &Connection,
    id: lsp_server::RequestId,
    result: T,
) {
    let value = match serde_json::to_value(result) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("knot-lsp: failed to encode response: {e}");
            return;
        }
    };
    let resp = lsp_server::Response::new_ok(id, value);
    if let Err(e) = conn.sender.send(lsp_server::Message::Response(resp)) {
        eprintln!("knot-lsp: failed to send response: {e}");
    }
}
