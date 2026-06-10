//! `textDocument/prepareRename` and `textDocument/rename` handlers.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use lsp_types::*;

use knot::ast::{self, DeclKind, Module, Span};

use crate::analysis::get_or_parse_file_shared;
use crate::defs::resolve_definitions;
use crate::shared::scan_knot_files_in_roots;
use crate::state::{builtins, DocumentState, ServerState, KEYWORDS};
use crate::utils::{
    find_word_in_source, path_to_uri, position_to_offset, recurse_expr, safe_slice,
    span_to_range, uri_to_path, word_at_position,
};

// ── Rename ──────────────────────────────────────────────────────────

pub(crate) fn handle_prepare_rename(
    state: &ServerState,
    params: &TextDocumentPositionParams,
) -> Option<PrepareRenameResponse> {
    let doc = state.documents.get(&params.text_document.uri)?;
    // Mirror `handle_rename`'s staleness guard: ranges computed from the
    // last-analyzed source don't line up with newer pending editor text.
    if state
        .pending_sources
        .get(&params.text_document.uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let pos = params.position;
    let offset = position_to_offset(&doc.source, pos);

    // Check if cursor is on a renameable symbol
    let word = word_at_position(&doc.source, pos)?;

    // Reject keywords up front. `word_at_position` returns None for non-ident
    // chars, so the cursor lands on something that *parses* as an identifier;
    // if that identifier is a reserved keyword, no rename is meaningful.
    if KEYWORDS.iter().any(|kw| *kw == word) {
        return None;
    }

    // Must be on a known definition, a reference to one, or a record field
    // position. Field positions are determined by AST shape — we accept them
    // here so the editor offers the rename action; the actual rewrite is
    // handled in `handle_rename` via `collect_field_rename_sites`.
    let is_ref = doc
        .references
        .iter()
        .any(|(usage, _)| usage.start <= offset && offset < usage.end);
    let is_def = doc.definitions.values().any(|span| span.start <= offset && offset < span.end);
    let is_field = is_at_record_field(&doc.module, &doc.source, offset);
    let is_imported = doc.import_defs.contains_key(word);

    if !is_ref && !is_def && !is_field && !is_imported {
        return None;
    }

    // Reject builtins that aren't shadowed by a user definition — renaming a
    // stdlib symbol like `println` would only edit local references and leave
    // the binding broken. We keep the rename if a user-declared symbol with
    // the same name shadows the builtin, since that's the canonical owner.
    if builtins().any(|b| b == word) && !is_def && !is_imported {
        return None;
    }

    // Return the word range
    let word_offset = position_to_offset(&doc.source, pos);
    let bytes = doc.source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let start = (0..word_offset)
        .rev()
        .find(|&i| !is_ident(bytes[i]))
        .map(|i| i + 1)
        .unwrap_or(0);
    let end = (word_offset..bytes.len())
        .find(|&i| !is_ident(bytes[i]))
        .unwrap_or(bytes.len());

    let range = span_to_range(Span::new(start, end), &doc.source);
    Some(PrepareRenameResponse::RangeWithPlaceholder {
        range,
        placeholder: word.to_string(),
    })
}

/// Validate that `name` is a syntactically valid Knot identifier:
/// starts with letter or underscore, contains only ident chars, and isn't a
/// reserved keyword. Used by `handle_rename` to reject malformed renames
/// before scanning the workspace.
fn is_valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let first = match chars.next() {
        Some(c) => c,
        None => return false,
    };
    if !(first.is_alphabetic() || first == '_') {
        return false;
    }
    if !chars.all(|c| c.is_alphanumeric() || c == '_') {
        return false;
    }
    !KEYWORDS.iter().any(|kw| *kw == name)
}

pub(crate) fn handle_rename(
    state: &ServerState,
    params: &RenameParams,
) -> Option<WorkspaceEdit> {
    let uri = &params.text_document_position.text_document.uri;
    let pos = params.text_document_position.position;
    let doc = state.documents.get(uri)?;
    // Staleness guard: when the editor holds newer text than the last
    // analyzed source, every span we'd compute here indexes into the *old*
    // bytes — applying those edits to the new buffer corrupts it. Bail and
    // let the client retry once analysis catches up.
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let offset = position_to_offset(&doc.source, pos);
    let new_name = &params.new_name;
    let old_name = word_at_position(&doc.source, pos)?.to_string();

    // Reject malformed new names — keywords, empty strings, names starting
    // with digits. The LSP spec lets us return null when a rename would
    // produce an invalid result.
    if !is_valid_identifier(new_name) || old_name == *new_name {
        return None;
    }

    // Field rename branch: when the cursor sits on a record field name (not a
    // symbol), the cross-file owner machinery doesn't apply — there's no
    // "owning declaration" the way there is for top-level names. We rewrite
    // every matching field-name position in the same file. We don't attempt
    // cross-file field rename: doing it correctly requires record-type
    // resolution at every site, which the LSP doesn't have plumbed through.
    if is_at_record_field(&doc.module, &doc.source, offset) {
        let sites = collect_field_rename_sites(&doc.module, &doc.source, &old_name);
        if !sites.is_empty() {
            let mut edits: Vec<TextEdit> = sites
                .into_iter()
                .map(|s| TextEdit {
                    range: span_to_range(s, &doc.source),
                    new_text: new_name.clone(),
                })
                .collect();
            // Deterministic order (stable across runs) so test expectations
            // can match a known sequence.
            edits.sort_by_key(|e| (e.range.start.line, e.range.start.character));
            let mut changes: HashMap<Uri, Vec<TextEdit>> = HashMap::new();
            changes.insert(uri.clone(), edits);
            return Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            });
        }
    }

    // Phase 1: identify the canonical owner — the file + span where the
    // symbol's definition lives. If the cursor is on an imported symbol,
    // the owner is that imported file's decl, not this file.
    let owner = resolve_canonical_owner(state, uri, doc, offset, &old_name)?;

    let mut changes: HashMap<Uri, Vec<TextEdit>> = HashMap::new();

    // Phase 2: visit every file in the workspace and emit edits if it
    // either owns the symbol or imports it from the owner. Open files use
    // the cached `DocumentState`; closed files are read off disk.
    let scanned = scan_workspace_files(state, &owner, &old_name, new_name, &mut changes);

    // Phase 3: scan unopened workspace files for references via on-disk
    // parse. We narrow by the reverse-import graph when possible — most
    // files don't reach the owner, and skipping them avoids per-file
    // parse cost.
    scan_disk_files(state, &owner, &old_name, new_name, &scanned, &mut changes);

    // Defensive de-duplication: the owner-file path can discover the same
    // name-token span through both the declaration edit and a reference
    // whose target equals it. Clients reject workspace edits with
    // overlapping ranges, so collapse exact duplicates here.
    for edits in changes.values_mut() {
        edits.sort_by_key(|e| {
            (
                e.range.start.line,
                e.range.start.character,
                e.range.end.line,
                e.range.end.character,
            )
        });
        edits.dedup_by(|a, b| a.range == b.range);
    }

    Some(WorkspaceEdit {
        changes: Some(changes),
        ..Default::default()
    })
}

/// The canonical owner of a symbol — the file path and span where the symbol
/// was originally declared. Cross-file rename uses this as the source of
/// truth: every other file's references must point back to this same
/// `(path, span)` pair to be considered the same symbol.
struct CanonicalOwner {
    canonical_path: PathBuf,
    /// Span of the *whole* declaration in the owning file.
    decl_span: Span,
    /// Span of just the symbol's name token within the declaration.
    name_span: Span,
    /// Whether the resolved definition is a top-level declaration of the
    /// owner file (and therefore visible to importers). Local bindings —
    /// lambda params, do-binds, let-binds, case patterns — set this to
    /// `false`, restricting the rename strictly to the owner file.
    is_top_level: bool,
}

fn resolve_canonical_owner(
    state: &ServerState,
    uri: &Uri,
    doc: &DocumentState,
    offset: usize,
    name: &str,
) -> Option<CanonicalOwner> {
    // Case A: the cursor is on a local def or usage that resolves locally.
    let local_def = doc
        .references
        .iter()
        .find(|(usage, _)| usage.start <= offset && offset < usage.end)
        .map(|(_, def)| *def)
        .or_else(|| {
            doc.definitions
                .values()
                .find(|span| span.start <= offset && offset < span.end)
                .copied()
        });
    if let Some(decl_span) = local_def {
        let canonical_path = uri_to_path(uri)
            .and_then(|p| p.canonicalize().ok())
            .unwrap_or_else(|| PathBuf::from(uri.as_str()));
        let name_span = name_span_within(&doc.source, decl_span, name).unwrap_or(decl_span);
        // Top-level definitions are registered (by name) in `doc.definitions`
        // with their name-token span; if the resolved span matches one of
        // those, the symbol is an export. Otherwise it's a local binding and
        // the rename must not leak into importers.
        let is_top_level = doc.definitions.values().any(|s| *s == decl_span);
        return Some(CanonicalOwner {
            canonical_path,
            decl_span,
            name_span,
            is_top_level,
        });
    }

    // Case B: the cursor is on a usage of an imported symbol. The doc's
    // `import_defs` map records `(path, span)` for each imported name.
    if let Some((other_path, decl_span)) = doc.import_defs.get(name) {
        let other_source = doc
            .imported_files
            .get(other_path)
            .cloned()
            .or_else(|| {
                let cache = state.import_cache.lock().ok()?;
                cache.get(other_path).map(|e| e.source.clone())
            })
            .unwrap_or_default();
        let name_span = name_span_within(&other_source, *decl_span, name).unwrap_or(*decl_span);
        return Some(CanonicalOwner {
            canonical_path: other_path.clone(),
            decl_span: *decl_span,
            name_span,
            is_top_level: true,
        });
    }
    None
}

/// Locate the symbol-name token within a declaration's span, falling back to
/// the whole span if the name isn't recoverable (defensive — most decls
/// repeat the name as their first token).
fn name_span_within(source: &str, decl_span: Span, name: &str) -> Option<Span> {
    let text = safe_slice(source, decl_span);
    let bytes = text.as_bytes();
    let needle = name.as_bytes();
    if needle.is_empty() {
        return None;
    }
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let left_ok = i == 0 || !is_ident(bytes[i - 1]);
            let right_ok = i + needle.len() >= bytes.len() || !is_ident(bytes[i + needle.len()]);
            if left_ok && right_ok {
                return Some(Span::new(decl_span.start + i, decl_span.start + i + needle.len()));
            }
        }
        i += 1;
    }
    None
}

/// Replacement text for a rename edit at `span`, expanding record puns. A
/// bare identifier sitting directly between `{`/`,`/`|` and `,`/`}` is a pun
/// — `{name}` in a pattern binds a variable *and* selects a field; in an
/// expression it reads a variable *and* names a field. Renaming the variable
/// must not change which field is matched/built, so the pun expands to
/// `name: newName` instead of being rewritten in place.
fn pun_aware_new_text(source: &str, span: Span, old_name: &str, new_name: &str) -> String {
    let before = source
        .get(..span.start)
        .and_then(|s| s.trim_end().chars().next_back());
    let after = source
        .get(span.end..)
        .and_then(|s| s.trim_start().chars().next());
    let opens = matches!(before, Some('{') | Some(',') | Some('|'));
    let closes = matches!(after, Some(',') | Some('}'));
    if opens && closes {
        format!("{old_name}: {new_name}")
    } else {
        new_name.to_string()
    }
}

/// Resolve a URI to a stable canonical path, falling back to the URI-as-path
/// when canonicalize fails (e.g. synthetic test URIs that don't hit disk).
/// The fallback must mirror `resolve_canonical_owner`'s fallback so equality
/// checks line up across both call sites.
fn canonical_for_uri(uri: &Uri) -> Option<PathBuf> {
    let path = uri_to_path(uri)?;
    Some(path.canonicalize().unwrap_or_else(|_| PathBuf::from(uri.as_str())))
}

/// Walk every open document. Emit edits when the doc owns the symbol or
/// imports it from the canonical owner. Returns the set of canonical paths
/// already handled so the disk-scan phase can skip them.
fn scan_workspace_files(
    state: &ServerState,
    owner: &CanonicalOwner,
    old_name: &str,
    new_name: &str,
    changes: &mut HashMap<Uri, Vec<TextEdit>>,
) -> HashSet<PathBuf> {
    let mut scanned = HashSet::new();
    for (other_uri, other_doc) in &state.documents {
        let other_path = canonical_for_uri(other_uri);
        let is_owner = other_path.as_ref() == Some(&owner.canonical_path);
        // Span comparison must be containment-tolerant: when the rename
        // starts in the owner file (Case A), `owner.decl_span` is the
        // name-token span, while `import_defs` stores whole-declaration
        // spans. Exact equality would silently skip open importers and
        // push them onto the disk-scan path, which computes edits against
        // the on-disk bytes instead of their (possibly unsaved) buffers.
        let imports_owner = owner.is_top_level
            && other_doc
                .import_defs
                .get(old_name)
                .map(|(p, span)| {
                    *p == owner.canonical_path
                        && (*span == owner.decl_span
                            || (span.start <= owner.decl_span.start
                                && owner.decl_span.end <= span.end)
                            || (owner.decl_span.start <= span.start
                                && span.end <= owner.decl_span.end))
                })
                .unwrap_or(false);
        if !is_owner && !imports_owner {
            continue;
        }
        emit_edits_for_open_doc(other_uri, other_doc, owner, old_name, new_name, is_owner, changes);
        if let Some(p) = other_path {
            scanned.insert(p);
        }
    }
    scanned
}

/// Apply rename edits to a single open document. The owner-file branch uses
/// the AST-derived `references` to find usages; the importer-file branch
/// walks the AST directly because imported-symbol uses don't appear in
/// `references` (which only resolves local decls).
fn emit_edits_for_open_doc(
    uri: &Uri,
    doc: &DocumentState,
    owner: &CanonicalOwner,
    old_name: &str,
    new_name: &str,
    is_owner: bool,
    changes: &mut HashMap<Uri, Vec<TextEdit>>,
) {
    if is_owner {
        // Rename the declaration itself.
        let name_span = name_span_within(&doc.source, owner.decl_span, old_name)
            .unwrap_or(owner.name_span);
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(name_span, &doc.source),
            new_text: pun_aware_new_text(&doc.source, name_span, old_name, new_name),
        });
        // Rename every local usage that resolves to the canonical decl.
        //
        // `owner.decl_span` can be either the name-token span (rename started
        // in this file — Case A) or the *whole-declaration* span (rename
        // started at an importer's call site — Case B resolves via
        // `import_defs`, which stores whole-decl spans). The open doc's
        // `references` always target name-token spans, so compare against the
        // resolved name token too; otherwise the owner's internal usages keep
        // the old name and the rename breaks the code.
        for (usage_span, target_span) in &doc.references {
            if *target_span == owner.decl_span || *target_span == name_span {
                changes.entry(uri.clone()).or_default().push(TextEdit {
                    range: span_to_range(*usage_span, &doc.source),
                    new_text: pun_aware_new_text(&doc.source, *usage_span, old_name, new_name),
                });
            }
        }
    } else {
        // Importer file. If the file declares its own top-level symbol with
        // the same name, every local reference resolves to that declaration
        // — not to the import — so renaming the owner's export must leave
        // this file untouched.
        if module_defines_name(&doc.module, old_name) {
            return;
        }
        // Walk the AST to find every Var/Constructor/source-
        // ref/derived-ref site that names the symbol, and rewrite each.
        let mut sites: Vec<Span> = Vec::new();
        for decl in &doc.module.decls {
            collect_name_uses_in_decl(decl, old_name, &mut sites);
        }
        // Selective import items: `import foo {bar, baz}` — if the rename
        // targets `bar`, the import line itself needs updating. These sit
        // inside braces but are NOT record puns, so they must bypass the
        // pun expansion below.
        let mut import_sites: Vec<Span> = Vec::new();
        for imp in &doc.module.imports {
            if let Some(items) = &imp.items {
                for item in items {
                    if item.name == old_name {
                        import_sites.push(item.span);
                    }
                }
            }
        }
        sites.sort_by_key(|s| s.start);
        sites.dedup_by_key(|s| s.start);
        for span in sites {
            changes.entry(uri.clone()).or_default().push(TextEdit {
                range: span_to_range(span, &doc.source),
                new_text: pun_aware_new_text(&doc.source, span, old_name, new_name),
            });
        }
        for span in import_sites {
            changes.entry(uri.clone()).or_default().push(TextEdit {
                range: span_to_range(span, &doc.source),
                new_text: new_name.to_string(),
            });
        }
    }
}

/// Whether `module` declares `name` at top level (function, type alias,
/// source, view, derived, data type or its constructors, trait or its
/// methods, route). Used to decide that an importer's local references
/// resolve to its own declaration rather than the imported symbol.
pub(crate) fn module_defines_name(module: &Module, name: &str) -> bool {
    module.decls.iter().any(|d| match &d.node {
        DeclKind::Fun { name: n, .. }
        | DeclKind::TypeAlias { name: n, .. }
        | DeclKind::Source { name: n, .. }
        | DeclKind::View { name: n, .. }
        | DeclKind::Derived { name: n, .. }
        | DeclKind::Route { name: n, .. }
        | DeclKind::RouteComposite { name: n, .. } => n == name,
        DeclKind::Data {
            name: n,
            constructors,
            ..
        } => n == name || constructors.iter().any(|c| c.name == name),
        DeclKind::Trait { name: n, items, .. } => {
            n == name
                || items.iter().any(|item| {
                    matches!(item, ast::TraitItem::Method { name: m, .. } if m == name)
                })
        }
        _ => false,
    })
}

/// True if binding `pat` introduces a local variable called `name` —
/// shadowing any imported symbol of the same name within the pattern's scope.
fn pat_binds_name(pat: &ast::Pat, name: &str) -> bool {
    match &pat.node {
        ast::PatKind::Var(n) => n == name,
        ast::PatKind::Wildcard | ast::PatKind::Lit(_) => false,
        ast::PatKind::Constructor { payload, .. } => pat_binds_name(payload, name),
        ast::PatKind::Record(fields) => fields.iter().any(|f| match &f.pattern {
            Some(p) => pat_binds_name(p, name),
            // Punned `{name}` binds a variable called `name`.
            None => f.name == name,
        }),
        ast::PatKind::List(pats) => pats.iter().any(|p| pat_binds_name(p, name)),
        ast::PatKind::Cons { head, tail } => {
            pat_binds_name(head, name) || pat_binds_name(tail, name)
        }
    }
}

/// Walk `decl` and collect every span where `name` appears as a value-level
/// reference (Var / Constructor / SourceRef / DerivedRef). This is the
/// importer-file rename oracle: the inferencer doesn't track cross-file
/// references in `doc.references`, so we walk the AST directly.
///
/// Scope-aware: a local binder (lambda param, do-bind, do-let, case pattern,
/// `let … in`) with the same name shadows the imported symbol, so `Var`
/// occurrences underneath that binder refer to the local and are skipped.
/// Constructor / SourceRef / DerivedRef occurrences live in namespaces value
/// binders can't shadow and are always collected.
pub(crate) fn collect_name_uses_in_decl(decl: &ast::Decl, name: &str, out: &mut Vec<Span>) {
    // Collect constructor-pattern name tokens (`Ctor pat <- …`, `case … of
    // Ctor …`) — these reference the renamed symbol when it's a constructor.
    fn walk_pat_ctors(pat: &ast::Pat, name: &str, out: &mut Vec<Span>) {
        match &pat.node {
            ast::PatKind::Constructor { name: n, payload } => {
                if n == name {
                    // Constructor names lead the pattern span; `n.len()` is
                    // bytes, matching the byte-indexed span representation.
                    out.push(Span::new(pat.span.start, pat.span.start + n.len()));
                }
                walk_pat_ctors(payload, name, out);
            }
            ast::PatKind::Record(fields) => {
                for f in fields {
                    if let Some(p) = &f.pattern {
                        walk_pat_ctors(p, name, out);
                    }
                }
            }
            ast::PatKind::List(pats) => {
                for p in pats {
                    walk_pat_ctors(p, name, out);
                }
            }
            ast::PatKind::Cons { head, tail } => {
                walk_pat_ctors(head, name, out);
                walk_pat_ctors(tail, name, out);
            }
            _ => {}
        }
    }
    fn walk_expr(expr: &ast::Expr, name: &str, shadowed: bool, out: &mut Vec<Span>) {
        match &expr.node {
            ast::ExprKind::Var(n) => {
                if !shadowed && n == name {
                    out.push(expr.span);
                }
                return;
            }
            ast::ExprKind::Constructor(n)
            | ast::ExprKind::SourceRef(n)
            | ast::ExprKind::DerivedRef(n) => {
                if n == name {
                    out.push(expr.span);
                }
                return;
            }
            ast::ExprKind::Lambda { params, body } => {
                for p in params {
                    walk_pat_ctors(p, name, out);
                }
                let sh = shadowed || params.iter().any(|p| pat_binds_name(p, name));
                walk_expr(body, name, sh, out);
                return;
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                walk_expr(scrutinee, name, shadowed, out);
                for arm in arms {
                    walk_pat_ctors(&arm.pat, name, out);
                    let sh = shadowed || pat_binds_name(&arm.pat, name);
                    walk_expr(&arm.body, name, sh, out);
                }
                return;
            }
            ast::ExprKind::Do(stmts) => {
                let mut sh = shadowed;
                for stmt in stmts {
                    match &stmt.node {
                        ast::StmtKind::Bind { pat, expr }
                        | ast::StmtKind::Let { pat, expr } => {
                            // The RHS is evaluated before the pattern binds,
                            // so it sees the pre-bind shadow status.
                            walk_expr(expr, name, sh, out);
                            walk_pat_ctors(pat, name, out);
                            if pat_binds_name(pat, name) {
                                sh = true;
                            }
                        }
                        ast::StmtKind::Where { cond } | ast::StmtKind::Expr(cond) => {
                            walk_expr(cond, name, sh, out);
                        }
                        ast::StmtKind::GroupBy { key } => walk_expr(key, name, sh, out),
                    }
                }
                return;
            }
            _ => {}
        }
        recurse_expr(expr, |e| walk_expr(e, name, shadowed, out));
    }
    match &decl.node {
        DeclKind::Fun { body: Some(body), .. }
        | DeclKind::View { body, .. }
        | DeclKind::Derived { body, .. } => walk_expr(body, name, false, out),
        DeclKind::Impl { items, .. } => {
            for item in items {
                if let ast::ImplItem::Method { params, body, .. } = item {
                    for p in params {
                        walk_pat_ctors(p, name, out);
                    }
                    let sh = params.iter().any(|p| pat_binds_name(p, name));
                    walk_expr(body, name, sh, out);
                }
            }
        }
        DeclKind::Trait { items, .. } => {
            for item in items {
                if let ast::TraitItem::Method {
                    default_body: Some(body),
                    default_params,
                    ..
                } = item
                {
                    for p in default_params {
                        walk_pat_ctors(p, name, out);
                    }
                    let sh = default_params.iter().any(|p| pat_binds_name(p, name));
                    walk_expr(body, name, sh, out);
                }
            }
        }
        DeclKind::Migrate { using_fn, .. } => {
            walk_expr(using_fn, name, false, out);
        }
        _ => {}
    }
}

fn scan_disk_files(
    state: &ServerState,
    owner: &CanonicalOwner,
    old_name: &str,
    new_name: &str,
    already_scanned: &HashSet<PathBuf>,
    changes: &mut HashMap<Uri, Vec<TextEdit>>,
) {
    // A local binding (lambda param, do-bind, let, case pattern) is invisible
    // outside its scope in the owner file — which is necessarily open, since
    // the rename started there. Touching any other file would rewrite
    // unrelated same-named identifiers.
    if !owner.is_top_level {
        return;
    }
    // Narrow to "files that could plausibly reference the owner" using the
    // reverse-import graph. The graph lists every file that imports the
    // owner directly or transitively, which is all we need — if a file
    // doesn't import the owner, it can't see the symbol.
    let candidate_paths = transitive_importers(state, &owner.canonical_path);
    // The owner itself is always a candidate (the rename starts there too).
    let mut to_scan: Vec<PathBuf> = candidate_paths.into_iter().collect();
    to_scan.push(owner.canonical_path.clone());
    // Fall back to a full workspace scan when the reverse-import graph is
    // empty (e.g. before any document opens populate it). This keeps the
    // rename correct in fresh sessions, at the cost of a one-time scan.
    if to_scan.len() <= 1 {
        let all = scan_knot_files_in_roots(
            &state.workspace_roots,
            state.workspace_root.as_deref(),
        );
        for f in all {
            if let Ok(c) = f.canonicalize() {
                if !already_scanned.contains(&c) {
                    to_scan.push(c);
                }
            }
        }
    }

    for path in to_scan {
        if already_scanned.contains(&path) {
            continue;
        }
        let (module, file_source) =
            match get_or_parse_file_shared(&path, &state.import_cache) {
                Some(v) => v,
                None => continue,
            };
        // Quick rejection — if the source bytes don't even mention the name
        // anywhere, no edits are needed and we can skip the heavier walk.
        if !file_source.contains(old_name) {
            continue;
        }
        let Some(file_uri) = path_to_uri(&path) else {
            continue;
        };
        let is_owner = path == owner.canonical_path;
        if is_owner {
            apply_owner_disk_edits(&file_uri, &module, &file_source, owner, old_name, new_name, changes);
        } else if file_imports_owner(&module, &path, &owner.canonical_path, old_name) {
            apply_importer_disk_edits(&file_uri, &module, &file_source, old_name, new_name, changes);
        }
    }
}

/// Apply rename edits to the owner file when it isn't currently open. We
/// re-parse the disk copy to recover refs/defs.
fn apply_owner_disk_edits(
    uri: &Uri,
    module: &Module,
    source: &str,
    owner: &CanonicalOwner,
    old_name: &str,
    new_name: &str,
    changes: &mut HashMap<Uri, Vec<TextEdit>>,
) {
    let (defs, refs, _) = resolve_definitions(module, source);
    if let Some(decl_span) = defs.get(old_name) {
        let name_span = name_span_within(source, *decl_span, old_name).unwrap_or(*decl_span);
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(name_span, source),
            new_text: pun_aware_new_text(source, name_span, old_name, new_name),
        });
        for (usage_span, target_span) in &refs {
            if target_span == decl_span {
                changes.entry(uri.clone()).or_default().push(TextEdit {
                    range: span_to_range(*usage_span, source),
                    new_text: pun_aware_new_text(source, *usage_span, old_name, new_name),
                });
            }
        }
    }
    let _ = owner;
}

fn apply_importer_disk_edits(
    uri: &Uri,
    module: &Module,
    source: &str,
    old_name: &str,
    new_name: &str,
    changes: &mut HashMap<Uri, Vec<TextEdit>>,
) {
    // Same shadowing discipline as the open-doc importer path: a file that
    // declares its own top-level `old_name` resolves references locally,
    // so the imported symbol's rename must not rewrite anything here.
    if module_defines_name(module, old_name) {
        return;
    }
    let mut sites: Vec<Span> = Vec::new();
    for decl in &module.decls {
        collect_name_uses_in_decl(decl, old_name, &mut sites);
    }
    // Import items live inside braces but are not record puns — plain
    // replacement, no pun expansion.
    let mut import_sites: Vec<Span> = Vec::new();
    for imp in &module.imports {
        if let Some(items) = &imp.items {
            for item in items {
                if item.name == old_name {
                    import_sites.push(item.span);
                }
            }
        }
    }
    sites.sort_by_key(|s| s.start);
    sites.dedup_by_key(|s| s.start);
    for span in sites {
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(span, source),
            new_text: pun_aware_new_text(source, span, old_name, new_name),
        });
    }
    for span in import_sites {
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(span, source),
            new_text: new_name.to_string(),
        });
    }
}

/// Quick check: does `module` import `owner_path` and does that import surface
/// `old_name`? Used to filter disk files before doing any AST walking.
pub(crate) fn file_imports_owner(
    module: &Module,
    file_path: &Path,
    owner_path: &Path,
    old_name: &str,
) -> bool {
    let base_dir = file_path.parent().unwrap_or(Path::new("."));
    for imp in &module.imports {
        let rel = PathBuf::from(&imp.path).with_extension("knot");
        let abs = base_dir.join(&rel);
        let canonical = match abs.canonicalize() {
            Ok(p) => p,
            Err(_) => continue,
        };
        if canonical == owner_path {
            // Selective imports: must list the name explicitly.
            if let Some(items) = &imp.items {
                if items.iter().any(|i| i.name == old_name) {
                    return true;
                }
            } else {
                // Wildcard import — always brings in everything.
                return true;
            }
        }
    }
    false
}

/// True if `offset` lands on a record field name. Field names live in:
/// record-literal field bindings (`{name: x}`), record patterns (`{name: y}`),
/// field accesses (`p.name`), record-type fields (`type T = {name: Text}`),
/// and data-constructor fields (`Circle {radius: Float}`). The AST stores
/// field names as bare strings (no per-name span), so we recover spans by
/// scanning the field's containing source range.
fn is_at_record_field(module: &ast::Module, source: &str, offset: usize) -> bool {
    field_position_at(module, source, offset).is_some()
}

/// Find the field name (and its span) at `offset`. Returns `None` if the
/// cursor isn't on a field-name token.
fn field_position_at(
    module: &ast::Module,
    source: &str,
    offset: usize,
) -> Option<(String, Span)> {
    let mut found: Option<(String, Span)> = None;
    for decl in &module.decls {
        if decl.span.start > offset || offset > decl.span.end {
            continue;
        }
        field_sites_in_decl(decl, source, &mut |name, span| {
            if found.is_none() && span.start <= offset && offset < span.end {
                found = Some((name.to_string(), span));
            }
        });
        if found.is_some() {
            return found;
        }
    }
    None
}

/// Walk the module and collect every span where `name` appears as a record-
/// field token. Used by the field-rename path to build the edit list. Casts
/// a wider net than strict type-aware rename, so users may need to review
/// edits in a codebase where the same field name appears across multiple
/// record types.
fn collect_field_rename_sites(
    module: &ast::Module,
    source: &str,
    name: &str,
) -> Vec<Span> {
    let mut out: Vec<Span> = Vec::new();
    for decl in &module.decls {
        field_sites_in_decl(decl, source, &mut |n, span| {
            if n == name {
                out.push(span);
            }
        });
    }
    // Dedupe in case the AST walk visits the same span via two branches.
    out.sort_by_key(|s| (s.start, s.end));
    out.dedup_by_key(|s| (s.start, s.end));
    out
}

// ── Field-name site enumeration ─────────────────────────────────────
//
// The AST stores field names as bare strings without their own spans, so
// the token position has to be recovered from source text. Searching the
// whole containing span for the first whole-word match is wrong — a value
// subexpression holding an identifier with the same name hijacks the
// location (`{a: count, count: 2}` resolves `count` to the *variable* in
// `a: count`). Mirroring `linked_editing.rs`, each field name is searched
// only in its syntactic field-name position: the slice between the previous
// field's value (or the container's opening token) and this field's value.

/// Invoke `f(name, span)` for every record-field-name token in `decl`.
fn field_sites_in_decl<F: FnMut(&str, Span)>(decl: &ast::Decl, source: &str, f: &mut F) {
    match &decl.node {
        DeclKind::Fun {
            body: Some(body), ..
        }
        | DeclKind::View { body, .. }
        | DeclKind::Derived { body, .. } => field_sites_in_expr(body, source, f),
        DeclKind::Impl { items, .. } => {
            for item in items {
                if let ast::ImplItem::Method { params, body, .. } = item {
                    for p in params {
                        field_sites_in_pat(p, source, f);
                    }
                    field_sites_in_expr(body, source, f);
                }
            }
        }
        DeclKind::Data { constructors, .. } => {
            // Constructor fields appear sequentially in source order, so a
            // single running cursor across all constructors keeps each
            // field-name search confined to its own slot.
            let mut search_start = decl.span.start;
            for ctor in constructors {
                for fld in &ctor.fields {
                    if let Some(span) = find_word_in_source(
                        source,
                        &fld.name,
                        search_start,
                        fld.value.span.start,
                    ) {
                        f(&fld.name, span);
                    }
                    field_sites_in_type(&fld.value, source, f);
                    search_start = fld.value.span.end;
                }
            }
        }
        DeclKind::TypeAlias { ty, .. } | DeclKind::Source { ty, .. } => {
            field_sites_in_type(ty, source, f);
        }
        _ => {}
    }
}

fn field_sites_in_expr<F: FnMut(&str, Span)>(expr: &ast::Expr, source: &str, f: &mut F) {
    match &expr.node {
        ast::ExprKind::Record(fields) => {
            let mut search_start = expr.span.start;
            for fld in fields {
                // Punned fields (`{name}`) have their value span on the very
                // token that names the field; the between-fields window is
                // empty and no site is reported. That's deliberate — the
                // token doubles as a variable reference and is handled by
                // the symbol-rename path instead.
                if let Some(span) =
                    find_word_in_source(source, &fld.name, search_start, fld.value.span.start)
                {
                    f(&fld.name, span);
                }
                search_start = fld.value.span.end;
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            let mut search_start = base.span.end;
            for fld in fields {
                if let Some(span) =
                    find_word_in_source(source, &fld.name, search_start, fld.value.span.start)
                {
                    f(&fld.name, span);
                }
                search_start = fld.value.span.end;
            }
        }
        ast::ExprKind::FieldAccess { field, expr: rec } => {
            // The field token is the suffix of the access expression.
            if expr.span.end >= field.len() {
                let start = expr.span.end - field.len();
                if start >= rec.span.end
                    && source.get(start..expr.span.end) == Some(field.as_str())
                {
                    f(field, Span::new(start, expr.span.end));
                }
            }
        }
        ast::ExprKind::Case { arms, .. } => {
            for arm in arms {
                field_sites_in_pat(&arm.pat, source, f);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, .. } | ast::StmtKind::Let { pat, .. } => {
                        field_sites_in_pat(pat, source, f);
                    }
                    _ => {}
                }
            }
        }
        ast::ExprKind::Lambda { params, .. } => {
            for p in params {
                field_sites_in_pat(p, source, f);
            }
        }
        _ => {}
    }
    recurse_expr(expr, |e| field_sites_in_expr(e, source, f));
}

fn field_sites_in_pat<F: FnMut(&str, Span)>(pat: &ast::Pat, source: &str, f: &mut F) {
    match &pat.node {
        ast::PatKind::Record(fields) => {
            let mut search_start = pat.span.start;
            for fp in fields {
                match &fp.pattern {
                    Some(inner) => {
                        if let Some(span) = find_word_in_source(
                            source,
                            &fp.name,
                            search_start,
                            inner.span.start,
                        ) {
                            f(&fp.name, span);
                        }
                        field_sites_in_pat(inner, source, f);
                        search_start = inner.span.end;
                    }
                    None => {
                        // Punned `{name}` pattern: the token both names the
                        // field and binds the variable. Renaming it would
                        // silently rebind every use in the body, so it is
                        // not reported as a field site; the symbol-rename
                        // path owns it. Still advance the cursor past it.
                        if let Some(span) = find_word_in_source(
                            source,
                            &fp.name,
                            search_start,
                            pat.span.end,
                        ) {
                            search_start = span.end;
                        }
                    }
                }
            }
        }
        ast::PatKind::Constructor { payload, .. } => field_sites_in_pat(payload, source, f),
        ast::PatKind::List(pats) => {
            for p in pats {
                field_sites_in_pat(p, source, f);
            }
        }
        ast::PatKind::Cons { head, tail } => {
            field_sites_in_pat(head, source, f);
            field_sites_in_pat(tail, source, f);
        }
        _ => {}
    }
}

fn field_sites_in_type<F: FnMut(&str, Span)>(ty: &ast::Type, source: &str, f: &mut F) {
    match &ty.node {
        ast::TypeKind::Record { fields, .. } => {
            let mut search_start = ty.span.start;
            for fld in fields {
                if let Some(span) =
                    find_word_in_source(source, &fld.name, search_start, fld.value.span.start)
                {
                    f(&fld.name, span);
                }
                field_sites_in_type(&fld.value, source, f);
                search_start = fld.value.span.end;
            }
        }
        ast::TypeKind::Variant { constructors, .. } => {
            let mut search_start = ty.span.start;
            for ctor in constructors {
                for fld in &ctor.fields {
                    if let Some(span) = find_word_in_source(
                        source,
                        &fld.name,
                        search_start,
                        fld.value.span.start,
                    ) {
                        f(&fld.name, span);
                    }
                    field_sites_in_type(&fld.value, source, f);
                    search_start = fld.value.span.end;
                }
            }
        }
        ast::TypeKind::Relation(inner) => field_sites_in_type(inner, source, f),
        ast::TypeKind::App { func, arg } => {
            field_sites_in_type(func, source, f);
            field_sites_in_type(arg, source, f);
        }
        ast::TypeKind::Function { param, result } => {
            field_sites_in_type(param, source, f);
            field_sites_in_type(result, source, f);
        }
        ast::TypeKind::Effectful { ty: inner, .. }
        | ast::TypeKind::IO { ty: inner, .. } => field_sites_in_type(inner, source, f),
        ast::TypeKind::UnitAnnotated { base, .. } => field_sites_in_type(base, source, f),
        ast::TypeKind::Refined { base, .. } => field_sites_in_type(base, source, f),
        ast::TypeKind::Forall { ty: inner, .. } => field_sites_in_type(inner, source, f),
        _ => {}
    }
}

/// BFS over `state.reverse_imports` to collect every file that transitively
/// imports `owner`. The graph is keyed by canonical paths.
fn transitive_importers(state: &ServerState, owner: &Path) -> HashSet<PathBuf> {
    let mut out: HashSet<PathBuf> = HashSet::new();
    let mut frontier: Vec<PathBuf> = vec![owner.to_path_buf()];
    while let Some(p) = frontier.pop() {
        if let Some(importers) = state.reverse_imports.get(&p) {
            for imp in importers {
                if out.insert(imp.clone()) {
                    frontier.push(imp.clone());
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestWorkspace;
    use crate::utils::offset_to_position;

    fn rename_params(uri: &Uri, position: Position, new_name: &str) -> RenameParams {
        RenameParams {
            text_document_position: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position,
            },
            new_name: new_name.to_string(),
            work_done_progress_params: Default::default(),
        }
    }

    fn prepare_params(uri: &Uri, position: Position) -> TextDocumentPositionParams {
        TextDocumentPositionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            position,
        }
    }

    #[test]
    fn prepare_rename_rejects_unshadowed_builtin() {
        // Cursor on `println` — a stdlib symbol with no local declaration.
        // Renaming it would leave the binding broken, so prepare_rename
        // should bail out before the editor offers the action.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "main = println \"hi\"\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("println").expect("builtin call");
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_prepare_rename(&ws.state, &prepare_params(&uri, pos));
        assert!(resp.is_none(), "rename should bail on unshadowed builtin: {resp:?}");
    }

    #[test]
    fn rename_rejects_invalid_new_name() {
        // Renaming to a Knot keyword would produce a syntax error in every
        // edited file. Reject before the workspace scan rather than commit
        // and let the editor flag downstream parse failures.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "double = \\x -> x * 2\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("double").expect("def");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "do"));
        assert!(edit.is_none(), "rename to keyword should be rejected: {edit:?}");
    }

    #[test]
    fn prepare_rename_accepts_known_symbol() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "id = \\x -> x\nmain = id 5\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("id =").expect("id def");
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_prepare_rename(&ws.state, &prepare_params(&uri, pos))
            .expect("prepare rename accepts");
        match resp {
            PrepareRenameResponse::RangeWithPlaceholder { placeholder, .. } => {
                assert_eq!(placeholder, "id");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn prepare_rename_rejects_keyword_position() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "id = \\x -> x\n");
        let doc = ws.doc(&uri);
        // Cursor on the lambda backslash — not a renameable symbol.
        let off = doc.source.find('\\').expect("lambda");
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_prepare_rename(&ws.state, &prepare_params(&uri, pos));
        assert!(resp.is_none(), "unexpected accept: {resp:?}");
    }

    #[test]
    fn rename_propagates_across_imported_files() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        // Owner file declares `parse`. Consumer imports it.
        let owner_uri = tw.write_and_open("owner.knot", "parse = \\x -> x\n");
        let consumer_uri = tw.write_and_open(
            "consumer.knot",
            "import ./owner\n\nmain = parse 5\n",
        );
        // Cursor on `parse` at the consumer's call site.
        let consumer_doc = tw.workspace.doc(&consumer_uri);
        let off = consumer_doc.source.find("parse 5").expect("call site");
        let pos = offset_to_position(&consumer_doc.source, off);
        let edit = handle_rename(&tw.workspace.state, &rename_params(&consumer_uri, pos, "parsed"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        // Both files should receive edits.
        assert!(
            changes.contains_key(&owner_uri),
            "owner file missed edit; got: {changes:?}"
        );
        assert!(
            changes.contains_key(&consumer_uri),
            "consumer file missed edit; got: {changes:?}"
        );
        for (_, edits) in &changes {
            assert!(
                edits.iter().all(|e| e.new_text == "parsed"),
                "all edits should rewrite to `parsed`; got: {edits:?}"
            );
        }
    }

    #[test]
    fn rename_does_not_touch_unrelated_local_with_same_name() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        // Two files: owner declares `parse`, unrelated file has its own
        // local `parse`. Renaming owner.parse should leave the unrelated
        // file alone.
        let owner_uri = tw.write_and_open("owner.knot", "parse = \\x -> x\n");
        let unrelated_uri = tw.write_and_open(
            "unrelated.knot",
            "parse = \\y -> y\nmain = parse 1\n",
        );
        let owner_doc = tw.workspace.doc(&owner_uri);
        let off = owner_doc.source.find("parse =").expect("def");
        let pos = offset_to_position(&owner_doc.source, off);
        let edit = handle_rename(&tw.workspace.state, &rename_params(&owner_uri, pos, "parsed"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        assert!(
            changes.contains_key(&owner_uri),
            "owner file should be edited"
        );
        assert!(
            !changes.contains_key(&unrelated_uri),
            "unrelated file with same-name local was renamed; got: {changes:?}"
        );
    }

    /// Apply a list of `TextEdit`s to `source` (positions resolved against
    /// `source`). Edits are applied back-to-front so earlier offsets stay
    /// valid.
    fn apply_edits(source: &str, edits: &[TextEdit]) -> String {
        let mut spans: Vec<(usize, usize, &str)> = edits
            .iter()
            .map(|e| {
                (
                    position_to_offset(source, e.range.start),
                    position_to_offset(source, e.range.end),
                    e.new_text.as_str(),
                )
            })
            .collect();
        spans.sort_by_key(|(s, _, _)| std::cmp::Reverse(*s));
        let mut out = source.to_string();
        for (start, end, text) in spans {
            out.replace_range(start..end, text);
        }
        out
    }

    #[test]
    fn rename_constructor_preserves_pattern_payload() {
        // Regression: constructor-pattern references used to span the whole
        // pattern (`Circle c`), so renaming the constructor deleted the
        // payload binder.
        let mut ws = TestWorkspace::new();
        let src = "data Shape = Circle {radius: Int} | Square {side: Int}\n\
                   area = \\s -> case s of\n  Circle c -> c.radius\n  Square q -> q.side\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("Circle").expect("ctor def");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "Round"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("owner file edited");
        let out = apply_edits(&doc.source, edits);
        assert!(
            out.contains("Round c -> c.radius"),
            "payload binder must survive constructor rename; got:\n{out}"
        );
        assert!(
            out.contains("data Shape = Round {radius: Int}"),
            "data declaration should be renamed; got:\n{out}"
        );
    }

    #[test]
    fn rename_payload_var_does_not_touch_constructor() {
        // Regression: the whole-pattern reference span made a payload-variable
        // cursor resolve to the constructor's definition, corrupting the
        // `data` declaration.
        let mut ws = TestWorkspace::new();
        let src = "data Shape = Circle {radius: Int} | Square {side: Int}\n\
                   area = \\s -> case s of\n  Circle c -> c.radius\n  Square q -> q.side\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("c.radius").expect("payload usage");
        let pos = offset_to_position(&doc.source, off);
        if let Some(edit) = handle_rename(&ws.state, &rename_params(&uri, pos, "circ")) {
            let changes = edit.changes.expect("changes present");
            let edits = changes.get(&uri).expect("owner file edited");
            let out = apply_edits(&doc.source, edits);
            assert!(
                out.contains("data Shape = Circle {radius: Int}"),
                "data declaration must not be touched by payload-var rename; got:\n{out}"
            );
            assert!(
                out.contains("Circle circ -> circ.radius"),
                "binder and usage should be renamed together; got:\n{out}"
            );
        }
    }

    #[test]
    fn rename_punned_pattern_binder_expands_pun() {
        // Regression: renaming the binder of a punned record pattern `{name}`
        // used to rewrite the pun token itself, silently changing which field
        // is matched. The pun must expand to `name: newName`.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "getName = \\{name} -> name\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("> name").expect("body usage") + 2;
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "fullName"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("owner file edited");
        let out = apply_edits(&doc.source, edits);
        assert_eq!(
            out, "getName = \\{name: fullName} -> fullName\n",
            "pun must expand so the matched field is preserved"
        );
    }

    #[test]
    fn rename_through_expression_pun_expands_pun() {
        // Expression puns have the same hazard: `{name}` builds `{name: name}`,
        // so renaming the variable must expand the pun to keep the field name.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "mk = \\name -> {name}\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("{name}").expect("expr pun") + 1;
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "label"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("owner file edited");
        let out = apply_edits(&doc.source, edits);
        assert_eq!(
            out, "mk = \\label -> {name: label}\n",
            "expression pun must expand so the built record keeps its field"
        );
    }

    #[test]
    fn field_rename_targets_field_token_not_variable() {
        // `count` appears both as a lambda-bound variable (value position in
        // `a: count`) and as a field name (`count: 2`). Renaming the *field*
        // must touch only the field token — the old first-whole-word scan
        // rewrote the variable and left the field untouched.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "f = \\count -> {a: count, count: 2}\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("count: 2").expect("field position");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "total"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        assert_eq!(edits.len(), 1, "exactly the field token; got: {edits:?}");
        let new_src = apply_edits(&doc.source, edits);
        assert_eq!(new_src, "f = \\count -> {a: count, total: 2}\n");
    }

    #[test]
    fn field_rename_rewrites_same_named_fields_in_separate_records() {
        // Two records in one expression, each with a field `n`. The old
        // implementation found the same (first) span twice and deduped to a
        // single wrong edit.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "g = [{n: 1}, {n: 2}]\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("n: 1").expect("first field");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "m"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        assert_eq!(edits.len(), 2, "one edit per record; got: {edits:?}");
        let new_src = apply_edits(&doc.source, edits);
        assert_eq!(new_src, "g = [{m: 1}, {m: 2}]\n");
    }

    #[test]
    fn rename_from_importer_rewrites_owner_internal_usages() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        // The owner uses `parse` internally; the rename starts at the
        // consumer's call site. The owner's declaration AND its internal
        // usage must both be rewritten — previously only the declaration
        // token changed because the whole-decl span from `import_defs`
        // never matched the owner doc's name-token reference targets.
        let owner_uri = tw.write_and_open("owner.knot", "parse = \\x -> x\nmain = parse 5\n");
        let consumer_uri =
            tw.write_and_open("consumer.knot", "import ./owner\n\nrun = parse 1\n");
        let consumer_doc = tw.workspace.doc(&consumer_uri);
        let off = consumer_doc.source.find("parse 1").expect("call site");
        let pos = offset_to_position(&consumer_doc.source, off);
        let edit = handle_rename(
            &tw.workspace.state,
            &rename_params(&consumer_uri, pos, "parsed"),
        )
        .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let owner_doc = tw.workspace.doc(&owner_uri);
        let owner_edits = changes.get(&owner_uri).expect("owner file edited");
        let new_owner = apply_edits(&owner_doc.source, owner_edits);
        assert_eq!(
            new_owner, "parsed = \\x -> x\nmain = parsed 5\n",
            "owner declaration AND internal usage must be renamed; edits: {owner_edits:?}"
        );
        let consumer_edits = changes.get(&consumer_uri).expect("consumer file edited");
        let new_consumer = apply_edits(&consumer_doc.source, consumer_edits);
        assert_eq!(new_consumer, "import ./owner\n\nrun = parsed 1\n");
    }

    #[test]
    fn rename_skips_shadowed_locals_in_importer() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        let owner_uri = tw.write_and_open("owner.knot", "parse = \\x -> x\n");
        let consumer_uri = tw.write_and_open(
            "consumer.knot",
            "import ./owner\n\nuse1 = parse 2\nshadow = \\parse -> parse 1\n",
        );
        let owner_doc = tw.workspace.doc(&owner_uri);
        let off = owner_doc.source.find("parse =").expect("def");
        let pos = offset_to_position(&owner_doc.source, off);
        let edit = handle_rename(
            &tw.workspace.state,
            &rename_params(&owner_uri, pos, "parsed"),
        )
        .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let consumer_doc = tw.workspace.doc(&consumer_uri);
        let consumer_edits = changes.get(&consumer_uri).expect("consumer file edited");
        let new_consumer = apply_edits(&consumer_doc.source, consumer_edits);
        assert_eq!(
            new_consumer,
            "import ./owner\n\nuse1 = parsed 2\nshadow = \\parse -> parse 1\n",
            "the lambda-bound `parse` and its scoped use refer to the local — \
             they must not be rewritten; edits: {consumer_edits:?}"
        );
    }

    #[test]
    fn renaming_local_binding_does_not_touch_unopened_importers() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        let owner_uri = tw.write_and_open("owner.knot", "f = \\count -> count + 1\n");
        // An unopened file on disk that imports the owner and mentions the
        // same identifier. A *local* rename in the owner must never reach it.
        let other_path = tw.root.join("other.knot");
        std::fs::write(&other_path, "import ./owner\ng = \\x -> count\n").unwrap();

        let owner_doc = tw.workspace.doc(&owner_uri);
        // Start the rename at the local *usage* so it resolves to the lambda
        // binder.
        let off = owner_doc.source.find("count + 1").expect("usage");
        let pos = offset_to_position(&owner_doc.source, off);
        let edit = handle_rename(
            &tw.workspace.state,
            &rename_params(&owner_uri, pos, "total"),
        )
        .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        assert_eq!(
            changes.len(),
            1,
            "local rename must stay in the owner file; got: {changes:?}"
        );
        let owner_edits = changes.get(&owner_uri).expect("owner file edited");
        let new_owner = apply_edits(&owner_doc.source, owner_edits);
        assert_eq!(new_owner, "f = \\total -> total + 1\n");
    }

    #[test]
    fn rename_bails_when_pending_text_is_newer() {
        use crate::state::PendingSource;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "double = \\x -> x * 2\nmain = double 1\n");
        let doc_source = ws.doc(&uri).source.clone();
        let off = doc_source.find("double =").expect("def");
        let pos = offset_to_position(&doc_source, off);
        // Simulate an edit that hasn't been analyzed yet: spans computed from
        // the analyzed source would corrupt the editor's newer buffer.
        ws.state.pending_sources.insert(
            uri.clone(),
            PendingSource {
                source: "-- new line\ndouble = \\x -> x * 2\nmain = double 1\n".into(),
                version: Some(2),
            },
        );
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "doubled"));
        assert!(
            edit.is_none(),
            "rename against stale analysis must bail; got: {edit:?}"
        );
    }

    #[test]
    fn rename_emits_edits_for_decl_and_usages() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "double = \\x -> x * 2\nmain = println (show (double 21))\n",
        );
        let doc = ws.doc(&uri);
        let off = doc.source.find("double =").expect("def");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "doubled"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        // Decl + one usage = 2 edits at minimum.
        assert!(edits.len() >= 2, "got: {edits:?}");
        assert!(edits.iter().all(|e| e.new_text == "doubled"));
    }
}
