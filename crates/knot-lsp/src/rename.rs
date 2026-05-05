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
    path_to_uri, position_to_offset, recurse_expr, safe_slice, span_to_range, uri_to_path,
    word_at_position,
};

// ── Rename ──────────────────────────────────────────────────────────

pub(crate) fn handle_prepare_rename(
    state: &ServerState,
    params: &TextDocumentPositionParams,
) -> Option<PrepareRenameResponse> {
    let doc = state.documents.get(&params.text_document.uri)?;
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
        return Some(CanonicalOwner {
            canonical_path,
            decl_span,
            name_span,
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
        let imports_owner = other_doc
            .import_defs
            .get(old_name)
            .map(|(p, span)| *p == owner.canonical_path && *span == owner.decl_span)
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
            new_text: new_name.to_string(),
        });
        // Rename every local usage that resolves to the canonical decl.
        for (usage_span, target_span) in &doc.references {
            if *target_span == owner.decl_span {
                changes.entry(uri.clone()).or_default().push(TextEdit {
                    range: span_to_range(*usage_span, &doc.source),
                    new_text: new_name.to_string(),
                });
            }
        }
    } else {
        // Importer file. Walk the AST to find every Var/Constructor/source-
        // ref/derived-ref site that names the symbol, and rewrite each.
        let mut sites: Vec<Span> = Vec::new();
        for decl in &doc.module.decls {
            collect_name_uses_in_decl(decl, old_name, &mut sites);
        }
        // Selective import items: `import foo {bar, baz}` — if the rename
        // targets `bar`, the import line itself needs updating.
        for imp in &doc.module.imports {
            if let Some(items) = &imp.items {
                for item in items {
                    if item.name == old_name {
                        sites.push(item.span);
                    }
                }
            }
        }
        sites.sort_by_key(|s| s.start);
        sites.dedup_by_key(|s| s.start);
        for span in sites {
            changes.entry(uri.clone()).or_default().push(TextEdit {
                range: span_to_range(span, &doc.source),
                new_text: new_name.to_string(),
            });
        }
    }
}

/// Walk `decl` and collect every span where `name` appears as a value-level
/// reference (Var / Constructor / SourceRef / DerivedRef). This is the
/// importer-file rename oracle: the inferencer doesn't track cross-file
/// references in `doc.references`, so we walk the AST directly.
fn collect_name_uses_in_decl(decl: &ast::Decl, name: &str, out: &mut Vec<Span>) {
    fn walk_expr(expr: &ast::Expr, name: &str, out: &mut Vec<Span>) {
        match &expr.node {
            ast::ExprKind::Var(n)
            | ast::ExprKind::Constructor(n)
            | ast::ExprKind::SourceRef(n)
            | ast::ExprKind::DerivedRef(n) => {
                if n == name {
                    out.push(expr.span);
                }
            }
            _ => {}
        }
        recurse_expr(expr, |e| walk_expr(e, name, out));
    }
    fn walk_pat(pat: &ast::Pat, name: &str, out: &mut Vec<Span>) {
        if let ast::PatKind::Constructor { name: n, payload } = &pat.node {
            if n == name {
                // Constructor names lead the pattern span; `n.len()` is bytes,
                // matching the byte-indexed span representation.
                out.push(Span::new(pat.span.start, pat.span.start + n.len()));
            }
            walk_pat(payload, name, out);
        }
        if let ast::PatKind::Record(fields) = &pat.node {
            for f in fields {
                if let Some(p) = &f.pattern {
                    walk_pat(p, name, out);
                }
            }
        }
        if let ast::PatKind::List(pats) = &pat.node {
            for p in pats {
                walk_pat(p, name, out);
            }
        }
    }
    fn walk_stmt(stmt: &ast::Stmt, name: &str, out: &mut Vec<Span>) {
        match &stmt.node {
            ast::StmtKind::Bind { pat, expr } | ast::StmtKind::Let { pat, expr } => {
                walk_pat(pat, name, out);
                walk_expr(expr, name, out);
            }
            ast::StmtKind::Where { cond } | ast::StmtKind::Expr(cond) => {
                walk_expr(cond, name, out);
            }
            ast::StmtKind::GroupBy { key } => walk_expr(key, name, out),
        }
    }
    fn walk_pat_recursive_with_arms(expr: &ast::Expr, name: &str, out: &mut Vec<Span>) {
        if let ast::ExprKind::Case { scrutinee: _, arms } = &expr.node {
            for arm in arms {
                walk_pat(&arm.pat, name, out);
            }
        }
        if let ast::ExprKind::Do(stmts) = &expr.node {
            for stmt in stmts {
                walk_stmt(stmt, name, out);
            }
        }
        recurse_expr(expr, |e| walk_pat_recursive_with_arms(e, name, out));
    }
    fn visit_body(body: &ast::Expr, name: &str, out: &mut Vec<Span>) {
        walk_expr(body, name, out);
        walk_pat_recursive_with_arms(body, name, out);
    }
    match &decl.node {
        DeclKind::Fun { body: Some(body), .. }
        | DeclKind::View { body, .. }
        | DeclKind::Derived { body, .. } => visit_body(body, name, out),
        DeclKind::Impl { items, .. } => {
            for item in items {
                if let ast::ImplItem::Method { params, body, .. } = item {
                    for p in params {
                        walk_pat(p, name, out);
                    }
                    visit_body(body, name, out);
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
                        walk_pat(p, name, out);
                    }
                    visit_body(body, name, out);
                }
            }
        }
        DeclKind::Migrate { using_fn, .. } => {
            walk_expr(using_fn, name, out);
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
            new_text: new_name.to_string(),
        });
        for (usage_span, target_span) in &refs {
            if target_span == decl_span {
                changes.entry(uri.clone()).or_default().push(TextEdit {
                    range: span_to_range(*usage_span, source),
                    new_text: new_name.to_string(),
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
    let mut sites: Vec<Span> = Vec::new();
    for decl in &module.decls {
        collect_name_uses_in_decl(decl, old_name, &mut sites);
    }
    for imp in &module.imports {
        if let Some(items) = &imp.items {
            for item in items {
                if item.name == old_name {
                    sites.push(item.span);
                }
            }
        }
    }
    sites.sort_by_key(|s| s.start);
    sites.dedup_by_key(|s| s.start);
    for span in sites {
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(span, source),
            new_text: new_name.to_string(),
        });
    }
    let _ = source;
}

/// Quick check: does `module` import `owner_path` and does that import surface
/// `old_name`? Used to filter disk files before doing any AST walking.
fn file_imports_owner(
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
        scan_decl_for_field(decl, source, offset, &mut found);
        if found.is_some() {
            return found;
        }
    }
    None
}

fn scan_decl_for_field(
    decl: &ast::Decl,
    source: &str,
    offset: usize,
    found: &mut Option<(String, Span)>,
) {
    match &decl.node {
        DeclKind::Fun {
            body: Some(body), ..
        } => scan_expr_for_field(body, source, offset, found),
        DeclKind::View { body, .. } | DeclKind::Derived { body, .. } => {
            scan_expr_for_field(body, source, offset, found)
        }
        DeclKind::Impl { items, .. } => {
            for item in items {
                if let ast::ImplItem::Method { body, .. } = item {
                    scan_expr_for_field(body, source, offset, found);
                    if found.is_some() {
                        return;
                    }
                }
            }
        }
        DeclKind::Data { constructors, .. } => {
            for ctor in constructors {
                for f in &ctor.fields {
                    if let Some(span) =
                        find_field_name_span(source, decl.span, &f.name, offset)
                    {
                        *found = Some((f.name.clone(), span));
                        return;
                    }
                }
            }
        }
        DeclKind::TypeAlias { ty, .. } => {
            scan_type_for_field(ty, source, offset, found);
        }
        DeclKind::Source { ty, .. } => {
            scan_type_for_field(ty, source, offset, found);
        }
        _ => {}
    }
}

fn scan_expr_for_field(
    expr: &ast::Expr,
    source: &str,
    offset: usize,
    found: &mut Option<(String, Span)>,
) {
    if found.is_some() {
        return;
    }
    if expr.span.start > offset || offset > expr.span.end {
        return;
    }
    match &expr.node {
        ast::ExprKind::Record(fields) => {
            for f in fields {
                if let Some(span) = find_field_name_span(source, expr.span, &f.name, offset)
                {
                    *found = Some((f.name.clone(), span));
                    return;
                }
            }
        }
        ast::ExprKind::RecordUpdate { fields, .. } => {
            for f in fields {
                if let Some(span) = find_field_name_span(source, expr.span, &f.name, offset)
                {
                    *found = Some((f.name.clone(), span));
                    return;
                }
            }
        }
        ast::ExprKind::FieldAccess { field, expr: rec } => {
            // Only the field-name suffix is renameable; the receiver is
            // its own thing. Search for the field token after the receiver.
            let after_recv = rec.span.end.min(expr.span.end);
            if let Some(span) = find_field_name_span(
                source,
                Span::new(after_recv, expr.span.end),
                field,
                offset,
            ) {
                *found = Some((field.clone(), span));
                return;
            }
        }
        ast::ExprKind::Case { arms, .. } => {
            for arm in arms {
                scan_pat_for_field(&arm.pat, source, offset, found);
                if found.is_some() {
                    return;
                }
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, .. } | ast::StmtKind::Let { pat, .. } => {
                        scan_pat_for_field(pat, source, offset, found);
                        if found.is_some() {
                            return;
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    crate::utils::recurse_expr(expr, |e| scan_expr_for_field(e, source, offset, found));
}

fn scan_pat_for_field(
    pat: &ast::Pat,
    source: &str,
    offset: usize,
    found: &mut Option<(String, Span)>,
) {
    if found.is_some() {
        return;
    }
    if pat.span.start > offset || offset > pat.span.end {
        return;
    }
    if let ast::PatKind::Record(fields) = &pat.node {
        for f in fields {
            if let Some(span) = find_field_name_span(source, pat.span, &f.name, offset) {
                *found = Some((f.name.clone(), span));
                return;
            }
            if let Some(inner) = &f.pattern {
                scan_pat_for_field(inner, source, offset, found);
                if found.is_some() {
                    return;
                }
            }
        }
    }
    if let ast::PatKind::Constructor { payload, .. } = &pat.node {
        scan_pat_for_field(payload, source, offset, found);
    }
    if let ast::PatKind::List(pats) = &pat.node {
        for p in pats {
            scan_pat_for_field(p, source, offset, found);
            if found.is_some() {
                return;
            }
        }
    }
}

fn scan_type_for_field(
    ty: &ast::Type,
    source: &str,
    offset: usize,
    found: &mut Option<(String, Span)>,
) {
    if found.is_some() {
        return;
    }
    if ty.span.start > offset || offset > ty.span.end {
        return;
    }
    if let ast::TypeKind::Record { fields, .. } = &ty.node {
        for f in fields {
            if let Some(span) = find_field_name_span(source, ty.span, &f.name, offset) {
                *found = Some((f.name.clone(), span));
                return;
            }
            scan_type_for_field(&f.value, source, offset, found);
            if found.is_some() {
                return;
            }
        }
    }
}

/// Locate `name` as a whole-word token within the source slice covering
/// `outer`, picking the occurrence whose span contains `offset`. Returns the
/// absolute span of the match, or `None` if `offset` doesn't land on a
/// whole-word `name` inside `outer`.
fn find_field_name_span(
    source: &str,
    outer: Span,
    name: &str,
    offset: usize,
) -> Option<Span> {
    if name.is_empty() || outer.end > source.len() || outer.start >= outer.end {
        return None;
    }
    let text = &source[outer.start..outer.end];
    let bytes = text.as_bytes();
    let needle = name.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let left_ok = i == 0 || !is_ident(bytes[i - 1]);
            let right_ok = i + needle.len() >= bytes.len() || !is_ident(bytes[i + needle.len()]);
            if left_ok && right_ok {
                let abs_start = outer.start + i;
                let abs_end = abs_start + needle.len();
                if abs_start <= offset && offset < abs_end {
                    return Some(Span::new(abs_start, abs_end));
                }
            }
        }
        i += 1;
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
        collect_field_sites_in_decl(decl, source, name, &mut out);
    }
    // Dedupe in case the AST walk visits the same span via two branches.
    out.sort_by_key(|s| (s.start, s.end));
    out.dedup_by_key(|s| (s.start, s.end));
    out
}

fn collect_field_sites_in_decl(
    decl: &ast::Decl,
    source: &str,
    name: &str,
    out: &mut Vec<Span>,
) {
    match &decl.node {
        DeclKind::Fun {
            body: Some(body), ..
        } => collect_field_sites_in_expr(body, source, name, out),
        DeclKind::View { body, .. } | DeclKind::Derived { body, .. } => {
            collect_field_sites_in_expr(body, source, name, out);
        }
        DeclKind::Impl { items, .. } => {
            for item in items {
                if let ast::ImplItem::Method { body, .. } = item {
                    collect_field_sites_in_expr(body, source, name, out);
                }
            }
        }
        DeclKind::Data { constructors, .. } => {
            for ctor in constructors {
                for f in &ctor.fields {
                    if f.name == name {
                        if let Some(span) = find_first_field_name_span(source, decl.span, name) {
                            out.push(span);
                        }
                    }
                }
            }
        }
        DeclKind::TypeAlias { ty, .. } | DeclKind::Source { ty, .. } => {
            collect_field_sites_in_type(ty, source, name, out);
        }
        _ => {}
    }
}

fn collect_field_sites_in_expr(
    expr: &ast::Expr,
    source: &str,
    name: &str,
    out: &mut Vec<Span>,
) {
    match &expr.node {
        ast::ExprKind::Record(fields) | ast::ExprKind::RecordUpdate { fields, .. } => {
            for f in fields {
                if f.name == name {
                    if let Some(span) = find_first_field_name_span(source, expr.span, name) {
                        out.push(span);
                    }
                }
            }
        }
        ast::ExprKind::FieldAccess { field, expr: rec } => {
            if field == name {
                let after_recv = rec.span.end.min(expr.span.end);
                if let Some(span) =
                    find_first_field_name_span(source, Span::new(after_recv, expr.span.end), name)
                {
                    out.push(span);
                }
            }
        }
        ast::ExprKind::Case { arms, .. } => {
            for arm in arms {
                collect_field_sites_in_pat(&arm.pat, source, name, out);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, .. } | ast::StmtKind::Let { pat, .. } => {
                        collect_field_sites_in_pat(pat, source, name, out);
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    crate::utils::recurse_expr(expr, |e| collect_field_sites_in_expr(e, source, name, out));
}

fn collect_field_sites_in_pat(
    pat: &ast::Pat,
    source: &str,
    name: &str,
    out: &mut Vec<Span>,
) {
    if let ast::PatKind::Record(fields) = &pat.node {
        for f in fields {
            if f.name == name {
                if let Some(span) = find_first_field_name_span(source, pat.span, name) {
                    out.push(span);
                }
            }
            if let Some(inner) = &f.pattern {
                collect_field_sites_in_pat(inner, source, name, out);
            }
        }
    }
    if let ast::PatKind::Constructor { payload, .. } = &pat.node {
        collect_field_sites_in_pat(payload, source, name, out);
    }
    if let ast::PatKind::List(pats) = &pat.node {
        for p in pats {
            collect_field_sites_in_pat(p, source, name, out);
        }
    }
}

fn collect_field_sites_in_type(
    ty: &ast::Type,
    source: &str,
    name: &str,
    out: &mut Vec<Span>,
) {
    if let ast::TypeKind::Record { fields, .. } = &ty.node {
        for f in fields {
            if f.name == name {
                if let Some(span) = find_first_field_name_span(source, ty.span, name) {
                    out.push(span);
                }
            }
            collect_field_sites_in_type(&f.value, source, name, out);
        }
    }
}

/// Locate the first whole-word occurrence of `name` inside `outer`, returning
/// its absolute span. Used when we know a field with that name appears within
/// a span and want to find its source position.
fn find_first_field_name_span(source: &str, outer: Span, name: &str) -> Option<Span> {
    if name.is_empty() || outer.end > source.len() || outer.start >= outer.end {
        return None;
    }
    let text = &source[outer.start..outer.end];
    let bytes = text.as_bytes();
    let needle = name.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let left_ok = i == 0 || !is_ident(bytes[i - 1]);
            let right_ok = i + needle.len() >= bytes.len() || !is_ident(bytes[i + needle.len()]);
            if left_ok && right_ok {
                let abs_start = outer.start + i;
                return Some(Span::new(abs_start, abs_start + needle.len()));
            }
        }
        i += 1;
    }
    None
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
