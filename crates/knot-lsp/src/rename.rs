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
    find_word_after_eq, find_word_in_source, find_word_last_in_source, ident_lookup_offset, path_to_uri,
    position_to_offset, recurse_expr,
    safe_slice, span_to_range, uri_to_path, word_at_position,
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
    let offset = ident_lookup_offset(&doc.source, position_to_offset(&doc.source, pos));

    // Check if cursor is on a renameable symbol
    let word = word_at_position(&doc.source, pos)?;

    // Reject keywords up front. `word_at_position` returns None for non-ident
    // chars, so the cursor lands on something that *parses* as an identifier;
    // if that identifier is a reserved keyword, no rename is meaningful.
    if KEYWORDS.contains(&word) {
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
    // The shadowing exemption is by NAME (a top-level definition anywhere in
    // the file) or by position (a resolved reference to a local/top-level
    // def) — not by whether the cursor happens to sit on the definition
    // token, otherwise F2 on a *usage* of a user symbol that shadows a
    // builtin is refused even though `handle_rename` would succeed.
    if builtins().any(|b| b == word)
        && !is_ref
        && !is_field
        && !doc.definitions.contains_key(word)
        && !is_imported
    {
        return None;
    }

    // Return the word range
    let word_offset = position_to_offset(&doc.source, pos);
    let bytes = doc.source.as_bytes();
    // `'` continues identifiers in the lexer (`x'`), so it's part of the
    // rename range too — otherwise renaming `x'` edits only `x` and leaves
    // a stray prime behind.
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';
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
/// starts with an ASCII letter or underscore, continues with ASCII
/// alphanumerics, `_`, or `'`, and isn't a reserved keyword. This matches
/// the lexer's `is_ident_continue` rules exactly — the lexer is ASCII-only,
/// so a Unicode-letter name like `naïve` would lex as garbage and corrupt
/// every edited file. Used by `handle_rename` to reject malformed renames
/// before scanning the workspace.
fn is_valid_identifier(name: &str) -> bool {
    let bytes = name.as_bytes();
    let first = match bytes.first() {
        Some(b) => *b,
        None => return false,
    };
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return false;
    }
    if !bytes[1..]
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'\'')
    {
        return false;
    }
    !KEYWORDS.contains(&name)
}

/// Whether `name` is uppercase-initial — i.e. lexes as a constructor/type
/// name rather than a variable. Used to reject renames that would move an
/// identifier across lexical namespaces.
fn starts_uppercase(name: &str) -> bool {
    name.chars().next().map(|c| c.is_uppercase()).unwrap_or(false)
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
    let offset = ident_lookup_offset(&doc.source, position_to_offset(&doc.source, pos));
    let new_name = &params.new_name;
    let old_name = word_at_position(&doc.source, pos)?.to_string();

    // Reject malformed new names — keywords, empty strings, names starting
    // with digits. The LSP spec lets us return null when a rename would
    // produce an invalid result.
    if !is_valid_identifier(new_name) || old_name == *new_name {
        return None;
    }

    // Reject case-class changes. Knot's lexer assigns identifiers to
    // namespaces by their first character: uppercase-initial names lex as
    // constructors/types, lowercase-initial (or `_`) as variables. Renaming
    // `Circle` to `round` would re-lex every occurrence as a variable and
    // break parsing, so refuse the rename up front (the LSP rename protocol
    // has no error channel here beyond returning null).
    if starts_uppercase(&old_name) != starts_uppercase(new_name) {
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
    // overlapping ranges, so collapse not just exact duplicates but any
    // edit that overlaps an already-kept one — pun expansion (`name: newName`
    // over the bare token span) and the decl-edit-vs-reference paths can
    // produce nested/overlapping ranges that exact-equality dedup misses.
    for edits in changes.values_mut() {
        edits.sort_by(|a, b| {
            let ka = (a.range.start.line, a.range.start.character);
            let kb = (b.range.start.line, b.range.start.character);
            ka.cmp(&kb)
                // Tie-break so the dedup below is deterministic AND keeps the
                // most complete replacement at a shared start: a punned field
                // rewrite (`name: newName`) must win over a bare `newName`,
                // else the pun expansion is silently dropped. Longest range
                // first (so it's the one kept), then lexicographic for a total
                // order.
                .then_with(|| {
                    let ea = (a.range.end.line, a.range.end.character);
                    let eb = (b.range.end.line, b.range.end.character);
                    eb.cmp(&ea)
                })
                .then_with(|| b.new_text.len().cmp(&a.new_text.len()))
                .then_with(|| a.new_text.cmp(&b.new_text))
        });
        // Keep an edit only if it starts at or after the end of the last edit
        // we kept; otherwise it overlaps and would be rejected by the client.
        let mut kept: Vec<TextEdit> = Vec::with_capacity(edits.len());
        for e in edits.drain(..) {
            if let Some(last) = kept.last() {
                let last_end = (last.range.end.line, last.range.end.character);
                let e_start = (e.range.start.line, e.range.start.character);
                if e_start < last_end {
                    continue;
                }
            }
            kept.push(e);
        }
        *edits = kept;
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
    // Pick the *innermost* (smallest) covering usage span, matching
    // `references::find_local_def`, `goto`, and `document_highlight`. Using
    // first-in-walk-order here instead would resolve a different (outer)
    // symbol than `prepare_rename`/find-references advertise when two recorded
    // references overlap (e.g. a relation sigil span overlapping a nested
    // binder), renaming the wrong binding.
    let local_def = doc
        .references
        .iter()
        .filter(|(usage, _)| usage.start <= offset && offset < usage.end)
        .min_by_key(|(usage, _)| usage.end - usage.start)
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
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';
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

/// Replacement text for a rename edit at `span`, expanding record puns.
/// `{name}` in a pattern binds a variable *and* selects a field; in an
/// expression it reads a variable *and* names a field. Renaming the variable
/// must not change which field is matched/built, so the pun expands to
/// `name: newName` instead of being rewritten in place. Pun detection is
/// AST-driven (is the span actually a record-literal/pattern pun token?) —
/// a textual neighbor check misfires on list elements like `[a, x, b]`.
fn pun_aware_new_text(
    module: &Module,
    source: &str,
    span: Span,
    old_name: &str,
    new_name: &str,
) -> String {
    if span_is_record_pun(module, source, span) {
        format!("{old_name}: {new_name}")
    } else {
        new_name.to_string()
    }
}

/// True when `span` is exactly the token of a punned record field — either
/// an expression pun (`{name}` building `{name: name}`) or a pattern pun
/// (`{name}` matching field `name` and binding a variable). Explicit
/// `{name: name}` fields are NOT puns: their field-name token sits before
/// the value, so the field-name search window is non-empty.
fn span_is_record_pun(module: &Module, source: &str, span: Span) -> bool {
    fn pun_in_pat(pat: &ast::Pat, source: &str, span: Span) -> bool {
        match &pat.node {
            ast::PatKind::Record(fields) => {
                let mut search_start = pat.span.start;
                for f in fields {
                    match &f.pattern {
                        Some(p) => {
                            if pun_in_pat(p, source, span) {
                                return true;
                            }
                            search_start = p.span.end;
                        }
                        None => {
                            // Punned field: the token both names the field
                            // and binds the variable.
                            if let Some(s) = find_word_in_source(
                                source,
                                &f.name,
                                search_start,
                                pat.span.end,
                            ) {
                                if s == span {
                                    return true;
                                }
                                search_start = s.end;
                            }
                        }
                    }
                }
                false
            }
            ast::PatKind::Constructor { payload, .. } => pun_in_pat(payload, source, span),
            ast::PatKind::List(pats) => pats.iter().any(|p| pun_in_pat(p, source, span)),
            ast::PatKind::Cons { head, tail } => {
                pun_in_pat(head, source, span) || pun_in_pat(tail, source, span)
            }
            _ => false,
        }
    }
    fn pun_field_in_fields(
        fields: &[ast::Field<ast::Expr>],
        mut search_start: usize,
        source: &str,
        span: Span,
    ) -> bool {
        for f in fields {
            // A pun field's value span IS the field-name token; an explicit
            // field has its name token (in the window before the value).
            let named = find_word_in_source(source, &f.name, search_start, f.value.span.start)
                .is_some();
            if !named
                && f.value.span == span
                && matches!(&f.value.node, ast::ExprKind::Var(n) if *n == f.name)
            {
                return true;
            }
            search_start = f.value.span.end;
        }
        false
    }
    fn pun_in_expr(expr: &ast::Expr, source: &str, span: Span, found: &mut bool) {
        if *found {
            return;
        }
        match &expr.node {
            ast::ExprKind::Record(fields)
                if pun_field_in_fields(fields, expr.span.start, source, span) => {
                    *found = true;
                    return;
                }
            ast::ExprKind::RecordUpdate { base, fields }
                if pun_field_in_fields(fields, base.span.end, source, span) => {
                    *found = true;
                    return;
                }
            ast::ExprKind::Lambda { params, .. }
                if params.iter().any(|p| pun_in_pat(p, source, span)) => {
                    *found = true;
                    return;
                }
            ast::ExprKind::Case { arms, .. }
                if arms.iter().any(|a| pun_in_pat(&a.pat, source, span)) => {
                    *found = true;
                    return;
                }
            ast::ExprKind::Do(stmts) => {
                for stmt in stmts {
                    if let ast::StmtKind::Bind { pat, .. } | ast::StmtKind::Let { pat, .. } =
                        &stmt.node
                        && pun_in_pat(pat, source, span) {
                            *found = true;
                            return;
                        }
                }
            }
            _ => {}
        }
        recurse_expr(expr, |e| pun_in_expr(e, source, span, found));
    }

    let mut found = false;
    for decl in &module.decls {
        if decl.span.start > span.start || span.end > decl.span.end {
            continue;
        }
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => pun_in_expr(body, source, span, &mut found),
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { params, body, .. } = item {
                        if params.iter().any(|p| pun_in_pat(p, source, span)) {
                            found = true;
                        }
                        pun_in_expr(body, source, span, &mut found);
                    }
                }
            }
            DeclKind::Trait { items, .. } => {
                for item in items {
                    if let ast::TraitItem::Method {
                        default_params,
                        default_body,
                        ..
                    } = item
                    {
                        if default_params.iter().any(|p| pun_in_pat(p, source, span)) {
                            found = true;
                        }
                        if let Some(body) = default_body {
                            pun_in_expr(body, source, span, &mut found);
                        }
                    }
                }
            }
            DeclKind::Migrate { using_fn, .. } => pun_in_expr(using_fn, source, span, &mut found),
            _ => {}
        }
        if found {
            return true;
        }
    }
    false
}

/// Narrow a reference span to its editable name token. `SourceRef` /
/// `DerivedRef` expression spans include the leading `*`/`&` sigil (the
/// parser builds them from the sigil token's start), and identifiers can
/// never begin with those bytes — so a rename edit must skip the sigil or
/// it gets deleted along with the old name. Reference *display* (find-
/// references, highlight) keeps the full span; only edits are narrowed.
fn edit_span(source: &str, span: Span) -> Span {
    match source.as_bytes().get(span.start) {
        Some(b'*') | Some(b'&') => Span::new(span.start + 1, span.end),
        _ => span,
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
            && (other_doc
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
                .unwrap_or(false)
                // Lenient fallback mirroring the disk-scan import check
                // (`file_imports_owner`). When `import_defs` span-containment
                // misses an open importer, it must still be handled here —
                // against its live (possibly unsaved) buffer — rather than
                // falling through to the disk scan, which computes edits from
                // on-disk bytes and corrupts the unsaved buffer's ranges.
                || other_path.as_ref().is_some_and(|p| {
                    file_imports_owner(
                        &other_doc.module,
                        p,
                        &owner.canonical_path,
                        old_name,
                    )
                }));
        if is_owner || imports_owner {
            emit_edits_for_open_doc(
                other_uri, other_doc, owner, old_name, new_name, is_owner, changes,
            );
        }
        // Always mark open documents as scanned, even when they neither own
        // nor import the symbol. The disk phase must never recompute an open
        // document's edits from on-disk bytes — those can differ from the
        // editor's unsaved buffer, producing edits at the wrong offsets.
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
            new_text: pun_aware_new_text(&doc.module, &doc.source, name_span, old_name, new_name),
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
                // `SourceRef`/`DerivedRef` usage spans include the `*`/`&`
                // sigil — the edit must only replace the name.
                let span = edit_span(&doc.source, *usage_span);
                changes.entry(uri.clone()).or_default().push(TextEdit {
                    range: span_to_range(span, &doc.source),
                    new_text: pun_aware_new_text(&doc.module, &doc.source, span, old_name, new_name),
                });
            }
        }
        // Impl-method definition tokens don't appear in `doc.references`
        // (defs.rs never links them to the trait method), so a same-file
        // trait-method rename would leave `impl … method =` stale. When this
        // module declares `old_name` as a trait method, rename every impl
        // method of that name here too. (Other files are handled by the
        // importer path via `collect_name_uses_in_decl`.)
        let renames_trait_method = doc.module.decls.iter().any(|d| {
            matches!(&d.node, DeclKind::Trait { items, .. }
                if items.iter().any(|it| matches!(
                    it, ast::TraitItem::Method { name: m, .. } if m == old_name)))
        });
        if renames_trait_method {
            for decl in &doc.module.decls {
                if let DeclKind::Impl { items, .. } = &decl.node {
                    for item in items {
                        if let ast::ImplItem::Method { name: m, name_span, .. } = item
                            && m == old_name
                        {
                            changes.entry(uri.clone()).or_default().push(TextEdit {
                                range: span_to_range(*name_span, &doc.source),
                                new_text: new_name.to_string(),
                            });
                        }
                    }
                }
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
        // If this file also imports `old_name` from a module other than the
        // owner, its body references are ambiguous; leave them untouched
        // rather than corrupt the other module's references.
        if let Some(file_path) = uri_to_path(uri)
            && imports_name_from_other_module(
                &doc.module,
                &file_path,
                &owner.canonical_path,
                old_name,
            ) {
                return;
            }
        // Walk the AST to find every Var/Constructor/source-
        // ref/derived-ref site that names the symbol, and rewrite each.
        let mut sites: Vec<Span> = Vec::new();
        for decl in &doc.module.decls {
            collect_name_uses_in_decl(decl, old_name, &doc.source, &mut sites);
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
            let span = edit_span(&doc.source, span);
            changes.entry(uri.clone()).or_default().push(TextEdit {
                range: span_to_range(span, &doc.source),
                new_text: pun_aware_new_text(&doc.module, &doc.source, span, old_name, new_name),
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
/// reference (Var / Constructor / SourceRef / DerivedRef), a type-level
/// reference (`Named` types in annotations, aliases, source/data decls,
/// routes), or an impl method-name token. This is the importer-file
/// rename oracle: the inferencer doesn't track cross-file references in
/// `doc.references`, so we walk the AST directly — mirroring what
/// `doc.references` covers for owner files.
///
/// Scope-aware: a local binder (lambda param, do-bind, do-let, case pattern,
/// `let … in`) with the same name shadows the imported symbol, so `Var`
/// occurrences underneath that binder refer to the local and are skipped.
/// Constructor / SourceRef / DerivedRef / type occurrences live in
/// namespaces value binders can't shadow and are always collected.
pub(crate) fn collect_name_uses_in_decl(
    decl: &ast::Decl,
    name: &str,
    source: &str,
    out: &mut Vec<Span>,
) {
    // Collect constructor-pattern name tokens (`Ctor pat <- …`, `case … of
    // Ctor …`) — these reference the renamed symbol when it's a constructor.
    fn walk_pat_ctors(pat: &ast::Pat, name: &str, source: &str, out: &mut Vec<Span>) {
        match &pat.node {
            ast::PatKind::Constructor { name: n, payload } => {
                if n == name {
                    // The constructor name does NOT always lead the pattern
                    // span: a parenthesized pattern (`(Circle c)`, the normal
                    // form for destructuring in a lambda/case) rewrites the
                    // span to start at `(`. Locate the actual name token via
                    // word search rather than assuming `start + n.len()`,
                    // which would otherwise corrupt the source on rename.
                    if let Some(span) =
                        find_word_in_source(source, n, pat.span.start, pat.span.end)
                    {
                        out.push(span);
                    } else if safe_slice(source, pat.span) == name {
                        out.push(pat.span);
                    }
                }
                walk_pat_ctors(payload, name, source, out);
            }
            ast::PatKind::Record(fields) => {
                for f in fields {
                    if let Some(p) = &f.pattern {
                        walk_pat_ctors(p, name, source, out);
                    }
                }
            }
            ast::PatKind::List(pats) => {
                for p in pats {
                    walk_pat_ctors(p, name, source, out);
                }
            }
            ast::PatKind::Cons { head, tail } => {
                walk_pat_ctors(head, name, source, out);
                walk_pat_ctors(tail, name, source, out);
            }
            _ => {}
        }
    }
    // Type-level references: `Named` nodes matching `name`. The recorded
    // span is just the name token (recovered via word search inside the
    // type node's span), so edits don't clobber surrounding syntax.
    fn walk_type(ty: &ast::Type, name: &str, source: &str, out: &mut Vec<Span>) {
        match &ty.node {
            ast::TypeKind::Named(n) => {
                if n == name {
                    if let Some(span) =
                        find_word_in_source(source, name, ty.span.start, ty.span.end)
                    {
                        out.push(span);
                    } else if safe_slice(source, ty.span) == name {
                        out.push(ty.span);
                    }
                }
            }
            ast::TypeKind::Var(_) | ast::TypeKind::Hole => {}
            ast::TypeKind::App { func, arg } => {
                walk_type(func, name, source, out);
                walk_type(arg, name, source, out);
            }
            ast::TypeKind::Record { fields, .. } => {
                for f in fields {
                    walk_type(&f.value, name, source, out);
                }
            }
            ast::TypeKind::Relation(inner) => walk_type(inner, name, source, out),
            ast::TypeKind::Function { param, result } => {
                walk_type(param, name, source, out);
                walk_type(result, name, source, out);
            }
            ast::TypeKind::Variant { constructors, .. } => {
                for ctor in constructors {
                    for f in &ctor.fields {
                        walk_type(&f.value, name, source, out);
                    }
                }
            }
            ast::TypeKind::Effectful { ty: inner, .. }
            | ast::TypeKind::IO { ty: inner, .. } => walk_type(inner, name, source, out),
            ast::TypeKind::UnitAnnotated { base, .. } => walk_type(base, name, source, out),
            ast::TypeKind::Refined { base, predicate } => {
                walk_type(base, name, source, out);
                walk_expr(predicate, name, source, false, out);
            }
            ast::TypeKind::Forall { ty: inner, .. } => walk_type(inner, name, source, out),
        }
    }
    fn walk_scheme(scheme: &ast::TypeScheme, name: &str, source: &str, out: &mut Vec<Span>) {
        walk_type(&scheme.ty, name, source, out);
        for c in &scheme.constraints {
            for arg in &c.args {
                walk_type(arg, name, source, out);
            }
        }
    }
    fn walk_expr(expr: &ast::Expr, name: &str, source: &str, shadowed: bool, out: &mut Vec<Span>) {
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
                    walk_pat_ctors(p, name, source, out);
                }
                let sh = shadowed || params.iter().any(|p| pat_binds_name(p, name));
                walk_expr(body, name, source, sh, out);
                return;
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                walk_expr(scrutinee, name, source, shadowed, out);
                for arm in arms {
                    walk_pat_ctors(&arm.pat, name, source, out);
                    let sh = shadowed || pat_binds_name(&arm.pat, name);
                    walk_expr(&arm.body, name, source, sh, out);
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
                            walk_expr(expr, name, source, sh, out);
                            walk_pat_ctors(pat, name, source, out);
                            if pat_binds_name(pat, name) {
                                sh = true;
                            }
                        }
                        ast::StmtKind::Where { cond } | ast::StmtKind::Expr(cond) => {
                            walk_expr(cond, name, source, sh, out);
                        }
                        ast::StmtKind::GroupBy { key } => walk_expr(key, name, source, sh, out),
                    }
                }
                return;
            }
            ast::ExprKind::Annot { expr: inner, ty } => {
                // Type annotations reference type names — `(x : Shape)` must
                // be rewritten when `Shape` is renamed.
                walk_type(ty, name, source, out);
                walk_expr(inner, name, source, shadowed, out);
                return;
            }
            ast::ExprKind::Serve { api, api_span, handlers } => {
                if api == name {
                    out.push(*api_span);
                }
                for h in handlers {
                    // Endpoint names reference route endpoint constructors.
                    if h.endpoint == name {
                        out.push(h.endpoint_span);
                    }
                    walk_expr(&h.body, name, source, shadowed, out);
                }
                return;
            }
            _ => {}
        }
        recurse_expr(expr, |e| walk_expr(e, name, source, shadowed, out));
    }
    // Recover rename sites for a textually-ordered sequence of *spanless*
    // trait-name tokens (an `impl`'s trait, a supertrait, or a `Trait a =>`
    // constraint) within `[from, to)`. Each name is located in source order
    // with a moving cursor (searching for its own text) so distinct trait
    // names in one clause each anchor on their own token; a token equal to the
    // renamed `name` is recorded. Without this, renaming a trait leaves
    // `impl Show …` and `Show a =>` bounds stale and corrupts the source.
    fn push_trait_name_refs<'b>(
        names: impl Iterator<Item = &'b str>,
        name: &str,
        source: &str,
        from: usize,
        to: usize,
        out: &mut Vec<Span>,
    ) {
        let mut cursor = from;
        for tn in names {
            if let Some(span) = find_word_in_source(source, tn, cursor, to) {
                cursor = span.end;
                if tn == name {
                    out.push(span);
                }
            }
        }
    }
    match &decl.node {
        DeclKind::Fun { ty, body, .. } => {
            if let Some(scheme) = ty {
                push_trait_name_refs(
                    scheme.constraints.iter().map(|c| c.trait_name.as_str()),
                    name,
                    source,
                    decl.span.start,
                    scheme.ty.span.start,
                    out,
                );
                walk_scheme(scheme, name, source, out);
            }
            if let Some(body) = body {
                walk_expr(body, name, source, false, out);
            }
        }
        DeclKind::View { ty, body, .. } | DeclKind::Derived { ty, body, .. } => {
            if let Some(scheme) = ty {
                push_trait_name_refs(
                    scheme.constraints.iter().map(|c| c.trait_name.as_str()),
                    name,
                    source,
                    decl.span.start,
                    scheme.ty.span.start,
                    out,
                );
                walk_scheme(scheme, name, source, out);
            }
            walk_expr(body, name, source, false, out);
        }
        DeclKind::Source { ty, .. } | DeclKind::TypeAlias { ty, .. } => {
            walk_type(ty, name, source, out);
        }
        DeclKind::Data { constructors, .. } => {
            for ctor in constructors {
                for f in &ctor.fields {
                    walk_type(&f.value, name, source, out);
                }
            }
        }
        DeclKind::Impl { trait_name, args, constraints, items, .. } => {
            // Impl head: `impl (Constraint =>)* TraitName args*`. All trait-name
            // tokens precede the first arg, in constraint-then-head order.
            let head_end = args.first().map(|a| a.span.start).unwrap_or(decl.span.end);
            push_trait_name_refs(
                constraints
                    .iter()
                    .map(|c| c.trait_name.as_str())
                    .chain(std::iter::once(trait_name.as_str())),
                name,
                source,
                decl.span.start,
                head_end,
                out,
            );
            for arg in args {
                walk_type(arg, name, source, out);
            }
            for c in constraints {
                for arg in &c.args {
                    walk_type(arg, name, source, out);
                }
            }
            for item in items {
                match item {
                    ast::ImplItem::Method { name: m, name_span, params, body } => {
                        // The method-name token references the trait's
                        // method declaration.
                        if m == name {
                            out.push(*name_span);
                        }
                        for p in params {
                            walk_pat_ctors(p, name, source, out);
                        }
                        let sh = params.iter().any(|p| pat_binds_name(p, name));
                        walk_expr(body, name, source, sh, out);
                    }
                    ast::ImplItem::AssociatedType { args, ty, .. } => {
                        for a in args {
                            walk_type(a, name, source, out);
                        }
                        walk_type(ty, name, source, out);
                    }
                }
            }
        }
        DeclKind::Trait { items, supertraits, .. } => {
            // `trait T a : Super1, Super2` — each supertrait name references
            // that trait and must be renamed with it.
            push_trait_name_refs(
                supertraits.iter().map(|c| c.trait_name.as_str()),
                name,
                source,
                decl.span.start,
                decl.span.end,
                out,
            );
            for c in supertraits {
                for arg in &c.args {
                    walk_type(arg, name, source, out);
                }
            }
            for item in items {
                if let ast::TraitItem::Method {
                    name_span,
                    ty,
                    default_body,
                    default_params,
                    ..
                } = item
                {
                    // A method's own `Trait a =>` constraint trait names
                    // (`eq : Show a => Bool`) are spanless; `walk_scheme` covers
                    // the type and constraint args but not the trait names, so
                    // recover them between the method name and its type (mirrors
                    // the `Fun`/supertrait handling).
                    push_trait_name_refs(
                        ty.constraints.iter().map(|c| c.trait_name.as_str()),
                        name,
                        source,
                        name_span.end,
                        ty.ty.span.start,
                        out,
                    );
                    walk_scheme(ty, name, source, out);
                    if let Some(body) = default_body {
                        for p in default_params {
                            walk_pat_ctors(p, name, source, out);
                        }
                        let sh = default_params.iter().any(|p| pat_binds_name(p, name));
                        walk_expr(body, name, source, sh, out);
                    }
                }
            }
        }
        DeclKind::Migrate { relation, from_ty, to_ty, using_fn, .. } => {
            // `migrate *rel from … to …` — the migrated relation references its
            // source declaration; rename it too so the migrate doesn't dangle.
            // `relation` is the bare name (no `*`), recovered from the source;
            // the `*` sigil is a word boundary for the search.
            if relation == name
                && let Some(span) =
                    find_word_in_source(source, name, decl.span.start, decl.span.end)
            {
                out.push(span);
            }
            walk_type(from_ty, name, source, out);
            walk_type(to_ty, name, source, out);
            walk_expr(using_fn, name, source, false, out);
        }
        DeclKind::Route { entries, .. } => {
            let mut ctor_cursor = decl.span.start;
            for entry in entries {
                for f in entry
                    .body_fields
                    .iter()
                    .chain(&entry.query_params)
                    .chain(&entry.request_headers)
                    .chain(&entry.response_headers)
                {
                    walk_type(&f.value, name, source, out);
                }
                if let Some(resp) = &entry.response_ty {
                    walk_type(resp, name, source, out);
                }
                for seg in &entry.path {
                    if let ast::PathSegment::Param { ty, .. } = seg {
                        walk_type(ty, name, source, out);
                    }
                }
                // The `rateLimit <expr>` clause references user names (e.g.
                // `rateLimit {key: keyByIp, ...}`). Walk it so renaming a
                // function/constructor used inside it updates those sites too —
                // otherwise the rename leaves stale names and breaks the source.
                if let Some(rl) = &entry.rate_limit {
                    walk_expr(rl, name, source, false, out);
                }
                // The endpoint constructor (`… -> Response = GetUsers`) is a
                // definition referenced by `serve API where GetUsers = …` and
                // `fetch url (GetUsers {…})`. It's spanless in the AST, so
                // recover its `= Ctor` token by scanning the decl source;
                // without this a rename updates the serve/fetch sites but
                // leaves the route declaration dangling.
                if entry.constructor == name
                    && let Some(span) =
                        find_word_after_eq(source, name, ctor_cursor, decl.span.end)
                {
                    ctor_cursor = span.end;
                    out.push(span);
                }
            }
        }
        DeclKind::RouteComposite { components, .. } => {
            // `route Api = A | B` — each component references another route by
            // name. Components are spanless in the AST, so recover each token
            // span by scanning the decl source. A moving cursor lets repeated
            // component names each get their own span.
            let mut cursor = decl.span.start;
            for comp in components {
                if comp == name
                    && let Some(span) =
                        find_word_in_source(source, name, cursor, decl.span.end)
                    {
                        cursor = span.end;
                        out.push(span);
                    }
            }
        }
        DeclKind::SubsetConstraint { sub, sup } => {
            // `*orders.customer <= *people.name` references source relations by
            // name. `RelationPath` is spanless, so recover each relation-name
            // occurrence by scanning the decl source with a moving cursor (both
            // sides may name the same relation, e.g. `*users <= *users.email`).
            // The `*` sigil is a word boundary for the search. Without this, a
            // source rename leaves the constraint dangling and broken.
            let mut cursor = decl.span.start;
            for rel in [&sub.relation, &sup.relation] {
                if rel.as_str() == name
                    && let Some(span) =
                        find_word_in_source(source, name, cursor, decl.span.end)
                {
                    cursor = span.end;
                    out.push(span);
                }
            }
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
    // Start from "files known to reference the owner" via the reverse-import
    // graph (cheap, already in memory).
    let candidate_paths = transitive_importers(state, &owner.canonical_path);
    // The owner itself is always a candidate (the rename starts there too).
    let mut to_scan: Vec<PathBuf> = candidate_paths.into_iter().collect();
    to_scan.push(owner.canonical_path.clone());
    // The reverse-import graph only has edges for files that have been
    // ANALYZED — i.e. open documents. An unopened importer that was never
    // analyzed is invisible to `transitive_importers`, so gating the disk
    // sweep on the graph being empty silently skipped such files whenever
    // ANY importer happened to be open. Always sweep the workspace for
    // files not already covered by the graph; the cheap
    // `contains(old_name)` rejection below keeps the per-file cost low.
    let known: HashSet<PathBuf> = to_scan.iter().cloned().collect();
    let all = scan_knot_files_in_roots(
        &state.workspace_roots,
        state.workspace_root.as_deref(),
    );
    for f in all {
        let c = f.canonicalize().unwrap_or(f);
        if !known.contains(&c) && !already_scanned.contains(&c) {
            to_scan.push(c);
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
            apply_importer_disk_edits(
                &file_uri,
                &module,
                &file_source,
                &path,
                &owner.canonical_path,
                old_name,
                new_name,
                changes,
            );
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
            new_text: pun_aware_new_text(module, source, name_span, old_name, new_name),
        });
        for (usage_span, target_span) in &refs {
            if target_span == decl_span {
                // Skip the `*`/`&` sigil on relation references.
                let span = edit_span(source, *usage_span);
                changes.entry(uri.clone()).or_default().push(TextEdit {
                    range: span_to_range(span, source),
                    new_text: pun_aware_new_text(module, source, span, old_name, new_name),
                });
            }
        }
    }
    // Impl-method definition tokens are not linked to the trait method in
    // `resolve_definitions`, so — exactly as the open-doc owner path does at
    // the top of `build_changes` — when this (disk) owner module declares
    // `old_name` as a trait method, rename every `impl … <old_name> =` method
    // token too. Without this, renaming a trait method from an importer's call
    // site left every impl in an *unopened* owner file stale, producing broken
    // code.
    let renames_trait_method = module.decls.iter().any(|d| {
        matches!(&d.node, DeclKind::Trait { items, .. }
            if items.iter().any(|it| matches!(
                it, ast::TraitItem::Method { name: m, .. } if m == old_name)))
    });
    if renames_trait_method {
        for decl in &module.decls {
            if let DeclKind::Impl { items, .. } = &decl.node {
                for item in items {
                    if let ast::ImplItem::Method { name: m, name_span, .. } = item
                        && m == old_name
                    {
                        changes.entry(uri.clone()).or_default().push(TextEdit {
                            range: span_to_range(*name_span, source),
                            new_text: new_name.to_string(),
                        });
                    }
                }
            }
        }
    }
    let _ = owner;
}

// Each argument carries distinct rename context (uri, module, source, paths,
// old/new name, output map); bundling them would not clarify the call sites.
#[allow(clippy::too_many_arguments)]
fn apply_importer_disk_edits(
    uri: &Uri,
    module: &Module,
    source: &str,
    file_path: &Path,
    owner_path: &Path,
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
    // If the file also imports `old_name` from a different module, its body
    // references are ambiguous — leave them untouched rather than corrupt the
    // other module's references.
    if imports_name_from_other_module(module, file_path, owner_path, old_name) {
        return;
    }
    let mut sites: Vec<Span> = Vec::new();
    for decl in &module.decls {
        collect_name_uses_in_decl(decl, old_name, source, &mut sites);
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
        let span = edit_span(source, span);
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(span, source),
            new_text: pun_aware_new_text(module, source, span, old_name, new_name),
        });
    }
    for span in import_sites {
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(span, source),
            new_text: new_name.to_string(),
        });
    }
}

/// True if `module` brings `name` into scope via an import whose target is a
/// module *other than* the owner. When that happens, in-body references to
/// `name` are ambiguous — they may resolve to the other module's export — so a
/// rename of the owner's symbol must not rewrite them; doing so would silently
/// break the reference to the other module. Name-only resolution can't tell
/// the two apart, so we conservatively skip rewriting body references in such
/// files (an unresolvable import that surfaces the name is also treated as a
/// potential other source).
pub(crate) fn imports_name_from_other_module(
    module: &Module,
    file_path: &Path,
    owner_path: &Path,
    name: &str,
) -> bool {
    let base_dir = file_path.parent().unwrap_or(Path::new("."));
    for imp in &module.imports {
        // Does this import surface `name`? Wildcards surface everything;
        // selective imports must list it explicitly.
        let surfaces = match &imp.items {
            Some(items) => items.iter().any(|i| i.name == name),
            None => true,
        };
        if !surfaces {
            continue;
        }
        let rel = PathBuf::from(&imp.path).with_extension("knot");
        let abs = base_dir.join(&rel);
        match abs.canonicalize() {
            Ok(p) if p == owner_path => {} // the owner import itself — expected
            Ok(_) => return true,          // a different module also exports `name`
            Err(_) => return true,         // can't prove it's the owner — be safe
        }
    }
    false
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
pub(crate) fn is_at_record_field(module: &ast::Module, source: &str, offset: usize) -> bool {
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
        DeclKind::Fun { ty, body, .. } => {
            // The type signature can carry record types whose field names must
            // rename in lockstep with the body (`mkPerson : Text -> {name:
            // Text}`); every other decl arm walks its type via
            // `field_sites_in_type`, so the Fun arm must too — otherwise a
            // field rename leaves the signature stale and corrupts the source.
            // Signature-only decls (`body: None`) still need their `ty` walked.
            if let Some(scheme) = ty {
                field_sites_in_type(&scheme.ty, source, f);
            }
            if let Some(body) = body {
                field_sites_in_expr(body, source, f);
            }
        }
        DeclKind::View { ty, body, .. } | DeclKind::Derived { ty, body, .. } => {
            if let Some(scheme) = ty {
                field_sites_in_type(&scheme.ty, source, f);
            }
            field_sites_in_expr(body, source, f);
        }
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
            // field-name search confined to its own slot. The search starts
            // after the `=` — the header (`data Pair a = …`) contains type
            // parameter tokens that can collide with field names (renaming
            // field `a` in `data Pair a = Pair {a: Int}` must not match the
            // type parameter `a`).
            let decl_text = safe_slice(source, decl.span);
            let mut search_start = decl_text
                .find('=')
                .map(|i| decl.span.start + i + 1)
                .unwrap_or(decl.span.start);
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
        DeclKind::Trait { items, .. } => {
            for item in items {
                if let ast::TraitItem::Method {
                    ty,
                    default_params,
                    default_body,
                    ..
                } = item
                {
                    field_sites_in_type(&ty.ty, source, f);
                    for p in default_params {
                        field_sites_in_pat(p, source, f);
                    }
                    if let Some(body) = default_body {
                        field_sites_in_expr(body, source, f);
                    }
                }
            }
        }
        DeclKind::Migrate {
            from_ty,
            to_ty,
            using_fn,
            ..
        } => {
            field_sites_in_type(from_ty, source, f);
            field_sites_in_type(to_ty, source, f);
            field_sites_in_expr(using_fn, source, f);
        }
        DeclKind::Route { entries, .. } => {
            // Route field names are declared inline (`body {userId: Int}`,
            // `?{q: Text}`, `headers {auth: Text}`, and the response record
            // type) before their type. They appear in source order: body, path
            // params, query, request headers, response type, response headers.
            // A single running cursor confines each name search to its own slot
            // (mirroring the `Data`/`Record` walks).
            for entry in entries {
                let mut cursor = decl.span.start;
                let field_list = |flds: &[ast::Field<ast::Type>],
                                  cursor: &mut usize,
                                  f: &mut F| {
                    for fld in flds {
                        // Use the *last* match before the type: the field name
                        // declaration sits immediately before `fld.value`, so a
                        // same-named path literal earlier in the window (the
                        // body cursor starts at the route keyword, before the
                        // path in source) is correctly skipped.
                        if let Some(span) = find_word_last_in_source(
                            source,
                            &fld.name,
                            *cursor,
                            fld.value.span.start,
                        ) {
                            f(&fld.name, span);
                        }
                        field_sites_in_type(&fld.value, source, f);
                        *cursor = fld.value.span.end;
                    }
                };
                field_list(&entry.body_fields, &mut cursor, f);
                // The path appears before the body in source but is processed
                // after it here, so the body-advanced `cursor` points past the
                // path. Use a dedicated monotonic cursor for the param-name
                // search (the param name `{userId: Int}` is between the
                // previous segment's end and the type's start). Without this,
                // renaming a field that is also a route path param left the
                // path-param declaration site stale, breaking the route.
                let mut path_name_cursor = decl.span.start;
                for seg in &entry.path {
                    if let ast::PathSegment::Param { name, ty } = seg {
                        // Same reasoning as body fields: the param name `{id:
                        // Int}` is the last occurrence before its type.
                        if let Some(span) =
                            find_word_last_in_source(source, name, path_name_cursor, ty.span.start)
                        {
                            f(name, span);
                        }
                        field_sites_in_type(ty, source, f);
                        path_name_cursor = ty.span.end;
                        cursor = ty.span.end;
                    }
                }
                field_list(&entry.query_params, &mut cursor, f);
                field_list(&entry.request_headers, &mut cursor, f);
                if let Some(resp) = &entry.response_ty {
                    field_sites_in_type(resp, source, f);
                    cursor = resp.span.end;
                }
                field_list(&entry.response_headers, &mut cursor, f);
                // Record-field occurrences inside the `rateLimit` expression
                // (e.g. `key: \input ctx -> input.userId`) must be renamed too.
                if let Some(rl) = &entry.rate_limit {
                    field_sites_in_expr(rl, source, f);
                }
            }
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
            // The field token sits between the receiver's end and the access
            // expression's end. It is NOT reliably the exact suffix of the
            // access span: a parenthesized access (`(r.total)`) widens the
            // node span to cover the closing paren(s), so a fixed suffix
            // offset would slice `otal)` instead of `total`. Locate the token
            // by whole-word search in the receiver→access window instead.
            if let Some(span) = find_word_in_source(source, field, rec.span.end, expr.span.end) {
                f(field, span);
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
        ast::ExprKind::Annot { ty, .. } => {
            // Inline type annotations (`(x : {name: Text})`) carry record field
            // names that must rename alongside their value occurrences.
            // `recurse_expr` descends only into the annotation's inner
            // expression, never its type, so walk the type here.
            field_sites_in_type(ty, source, f);
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
    fn rename_route_updates_composite_component() {
        // Regression: `route Api = AApi | BApi` component references were never
        // renamed — `collect_name_uses_in_decl` had no `RouteComposite` arm and
        // `defs.rs` didn't record component references. Renaming `AApi` must
        // update the composite component token too.
        let mut ws = TestWorkspace::new();
        let src = "route AApi where\n  /a\n    GET /one -> Int = GetOne\nroute BApi where\n  /b\n    GET /two -> Int = GetTwo\nroute Api = AApi | BApi\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("route AApi").expect("route def") + "route ".len();
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "RenamedApi"))
            .expect("rename produces edits");
        let mut changes = edit.changes.expect("changes present");
        let edits = changes.remove(&uri).expect("file has edits");

        // Locate the composite's `AApi` component token (after `=`).
        let composite_off = doc.source.find("Api = AApi").expect("composite line");
        let comp_off = doc.source[composite_off..].find("AApi").unwrap() + composite_off;
        let comp_pos = offset_to_position(&doc.source, comp_off);
        assert!(
            edits
                .iter()
                .any(|e| e.range.start == comp_pos && e.new_text.contains("RenamedApi")),
            "composite component `AApi` must be renamed; edits: {edits:?}"
        );
    }

    #[test]
    fn rename_field_updates_route_path_param() {
        // Regression: route path-param names (`/{owner: Text}`) were never
        // collected as field-rename sites — the param walk only visited the
        // param's *type*. Renaming a field also used as a path param left the
        // path-param declaration stale, breaking the route. Renaming the
        // `owner` field must also update the `/{owner: Text}` token.
        let mut ws = TestWorkspace::new();
        let src = "type Todo = {title: Text, owner: Text}\n\
                   *todos : [Todo]\n\
                   route TodoApi where\n  /todos\n    GET /{owner: Text} -> [Todo] = GetTodos\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        // Initiate the rename from the `owner` field in the `Todo` record.
        let field_off = doc.source.find("owner").expect("owner field");
        let pos = offset_to_position(&doc.source, field_off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "holder"))
            .expect("rename produces edits");
        let mut changes = edit.changes.expect("changes present");
        let edits = changes.remove(&uri).expect("file has edits");

        // Locate the path-param `owner` token (after `/{`).
        let param_off = doc.source.find("/{owner").expect("path param") + "/{".len();
        let param_pos = offset_to_position(&doc.source, param_off);
        assert!(
            edits
                .iter()
                .any(|e| e.range.start == param_pos && e.new_text.contains("holder")),
            "path-param `owner` must be renamed; edits: {edits:?}"
        );
    }

    #[test]
    fn rename_trait_updates_impl_head() {
        // Regression: `impl Show Foo`'s trait-name token was never collected —
        // `collect_name_uses_in_decl`'s Impl arm dropped `trait_name`. Renaming
        // the trait left `impl Show` stale, breaking the file. Renaming `Show`
        // must also update the `impl Show` occurrence.
        let mut ws = TestWorkspace::new();
        let src = "trait Show a where\n  present : a -> Text\ndata Foo = Foo {}\nimpl Show Foo where\n  present = \\x -> \"foo\"\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("Show").expect("trait def");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "Display"))
            .expect("rename produces edits");
        let mut changes = edit.changes.expect("changes present");
        let edits = changes.remove(&uri).expect("file has edits");

        let impl_off = doc.source.find("impl Show").expect("impl head") + "impl ".len();
        let impl_pos = offset_to_position(&doc.source, impl_off);
        assert!(
            edits
                .iter()
                .any(|e| e.range.start == impl_pos && e.new_text.contains("Display")),
            "trait name in `impl Show` must be renamed; edits: {edits:?}"
        );
    }

    #[test]
    fn rename_trait_method_updates_impl_method_same_file() {
        // Regression: a same-file trait-method rename left the `impl … method =`
        // definition stale. Impl-method name tokens aren't in `doc.references`
        // (defs.rs never links them to the trait method), and the collector that
        // does know them ran only on importer files — so the owner file's impl
        // was skipped, producing a trait method with no matching impl.
        let mut ws = TestWorkspace::new();
        let src = "trait Show a where\n  present : a -> Text\ndata Foo = Foo {}\nimpl Show Foo where\n  present = \\x -> \"foo\"\nmain = present (Foo {})\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        // Initiate the rename from the trait-method declaration.
        let off = doc.source.find("present").expect("trait method decl");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "render"))
            .expect("rename produces edits");
        let mut changes = edit.changes.expect("changes present");
        let edits = changes.remove(&uri).expect("file has edits");

        // The impl method definition token must be renamed too.
        let impl_off = doc.source.find("present = ").expect("impl method def");
        let impl_pos = offset_to_position(&doc.source, impl_off);
        assert!(
            edits
                .iter()
                .any(|e| e.range.start == impl_pos && e.new_text.contains("render")),
            "impl method `present =` must be renamed; edits: {edits:?}"
        );
    }

    #[test]
    fn rename_trait_updates_constraint() {
        // A `Show a =>` bound's trait name must rename with the trait too.
        let mut ws = TestWorkspace::new();
        let src = "trait Show a where\n  present : a -> Text\nfmtAll : Show a => a -> Text\nfmtAll = \\x -> present x\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("Show").expect("trait def");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "Display"))
            .expect("rename produces edits");
        let mut changes = edit.changes.expect("changes present");
        let edits = changes.remove(&uri).expect("file has edits");

        let bound_off = doc.source.find("Show a =>").expect("constraint");
        let bound_pos = offset_to_position(&doc.source, bound_off);
        assert!(
            edits
                .iter()
                .any(|e| e.range.start == bound_pos && e.new_text.contains("Display")),
            "trait name in `Show a =>` constraint must be renamed; edits: {edits:?}"
        );
    }

    #[test]
    fn rename_source_updates_migrate_relation() {
        // Regression: `migrate *users …`'s relation token was never collected —
        // the Migrate arm dropped `relation`. Renaming the source left the
        // migrate dangling. Renaming `users` must also update `migrate *users`.
        let mut ws = TestWorkspace::new();
        let src = "*users : [{v: Int}]\nf = \\x -> x\nmigrate *users from Int to Int using f\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("users").expect("source def");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "accounts"))
            .expect("rename produces edits");
        let mut changes = edit.changes.expect("changes present");
        let edits = changes.remove(&uri).expect("file has edits");

        let migrate_off = doc.source.find("migrate *users").expect("migrate") + "migrate *".len();
        let migrate_pos = offset_to_position(&doc.source, migrate_off);
        assert!(
            edits
                .iter()
                .any(|e| e.range.start == migrate_pos && e.new_text.contains("accounts")),
            "migrated relation `*users` must be renamed; edits: {edits:?}"
        );
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
    fn prepare_rename_accepts_field_named_like_builtin() {
        // A record field named like a stdlib symbol (`count`, `map`, …) lives
        // in a separate namespace from the builtin, and `handle_rename`
        // renames it correctly. The builtin-rejection guard must exempt field
        // positions, otherwise the editor never offers F2 on the field.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "g = {count: 2}\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("count").expect("field");
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_prepare_rename(&ws.state, &prepare_params(&uri, pos))
            .expect("prepare rename accepts builtin-named field");
        match resp {
            PrepareRenameResponse::RangeWithPlaceholder { placeholder, .. } => {
                assert_eq!(placeholder, "count");
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
        for edits in changes.values() {
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
    fn field_rename_updates_function_signature_type() {
        // Regression: `field_sites_in_decl`'s Fun arm walked only the body and
        // dropped the `ty` scheme, so renaming a record field left the
        // signature's `{name: Text}` stale — a type mismatch that corrupts the
        // source. The signature occurrence must rename in lockstep.
        let mut ws = TestWorkspace::new();
        let src = "mkPerson : Text -> {name: Text}\nmkPerson = \\n -> {name: n}\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("name: n").expect("body field");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "fullName"))
            .expect("rename emits edit");
        let edits = edit.changes.expect("changes").get(&uri).expect("edits").clone();
        assert_eq!(
            edits.len(),
            2,
            "both signature and body field tokens; got: {edits:?}"
        );
        let new_src = apply_edits(&doc.source, &edits);
        assert_eq!(
            new_src,
            "mkPerson : Text -> {fullName: Text}\nmkPerson = \\n -> {fullName: n}\n"
        );
    }

    #[test]
    fn field_rename_updates_inline_annotation_type() {
        // Regression: `field_sites_in_expr` had no `Annot` arm and
        // `recurse_expr` descends only into an annotation's inner expression,
        // never its type — so the field in `(r : {name: Text})` was never
        // collected, leaving the annotation stale after a field rename.
        let mut ws = TestWorkspace::new();
        let src = "f = \\r -> (r : {name: Text})\ng = {name: 1}\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("name: 1").expect("record field");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "label"))
            .expect("rename emits edit");
        let edits = edit.changes.expect("changes").get(&uri).expect("edits").clone();
        let new_src = apply_edits(&doc.source, &edits);
        assert_eq!(
            new_src,
            "f = \\r -> (r : {label: Text})\ng = {label: 1}\n",
            "annotation field must rename too; got edits: {edits:?}"
        );
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

/// Regression tests for the rename/references/highlight fix batch (sigil
/// preservation, AST-driven pun detection, builtin-shadowing prepare,
/// data-field search windows, local binder resolution, case-class guard,
/// references origin discipline, linked-editing recursion).
#[cfg(test)]
mod regress_rename_fixes_tests {
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

    /// Apply `TextEdit`s to `source`, back-to-front so offsets stay valid.
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

    // ── Finding 3: relation sigils survive rename ───────────────────

    #[test]
    fn rename_source_relation_keeps_star_sigil() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "*todos : [{title: Text}]\nallTodos = *todos\n",
        );
        let doc = ws.doc(&uri);
        // Cursor on the usage's name (after the sigil).
        let off = doc.source.find("= *todos").expect("usage") + 3;
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "items"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        let out = apply_edits(&doc.source, edits);
        assert_eq!(
            out, "*items : [{title: Text}]\nallTodos = *items\n",
            "the `*` sigil must survive the rename; edits: {edits:?}"
        );
    }

    #[test]
    fn rename_derived_relation_keeps_ampersand_sigil() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "*todos : [{title: Text}]\n&open = *todos\nmain = &open\n",
        );
        let doc = ws.doc(&uri);
        let off = doc.source.find("&open\n").expect("usage") + 1;
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "pending"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        let out = apply_edits(&doc.source, edits);
        assert_eq!(
            out,
            "*todos : [{title: Text}]\n&pending = *todos\nmain = &pending\n",
            "the `&` sigil must survive the rename; edits: {edits:?}"
        );
    }

    // ── Finding 4: pun detection must not misfire on list elements ──

    #[test]
    fn rename_list_element_is_not_treated_as_pun() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "f = \\x -> [1, x, 2]\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find(", x,").expect("list element") + 2;
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "y"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        let out = apply_edits(&doc.source, edits);
        assert_eq!(
            out, "f = \\y -> [1, y, 2]\n",
            "a list element between commas is not a record pun; edits: {edits:?}"
        );
    }

    #[test]
    fn rename_real_expression_pun_still_expands() {
        // The AST-driven check must keep the correct pun expansion.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "mk = \\name -> {name}\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("{name}").expect("expr pun") + 1;
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "label"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        let out = apply_edits(&doc.source, edits);
        assert_eq!(out, "mk = \\label -> {name: label}\n");
    }

    #[test]
    fn rename_explicit_same_named_field_value_is_not_a_pun() {
        // `{name: name}` written out explicitly: the value var renames in
        // place; expanding it again would corrupt the record.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "mk = \\name -> {name: name}\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("name}").expect("value var");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "label"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        let out = apply_edits(&doc.source, edits);
        assert_eq!(out, "mk = \\label -> {name: label}\n");
    }

    // ── Finding 5: prepare_rename on usages of builtin-shadowing symbols ──

    #[test]
    fn prepare_rename_accepts_usage_of_user_symbol_shadowing_builtin() {
        // `count` is a stdlib builtin, but this file declares its own.
        // F2 on a *usage* (not the definition token) must be accepted —
        // handle_rename would succeed, so prepare must not refuse.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "count = \\x -> x\nmain = count 1\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("count 1").expect("usage");
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_prepare_rename(&ws.state, &prepare_params(&uri, pos))
            .expect("prepare accepts usage of shadowing user symbol");
        match resp {
            PrepareRenameResponse::RangeWithPlaceholder { placeholder, .. } => {
                assert_eq!(placeholder, "count");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn prepare_rename_still_rejects_unshadowed_builtin_usage() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "main = count [1]\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("count").expect("builtin usage");
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_prepare_rename(&ws.state, &prepare_params(&uri, pos));
        assert!(resp.is_none(), "unshadowed builtin must be refused: {resp:?}");
    }

    // ── Finding 9: data-decl field rename skips the type-parameter header ──

    #[test]
    fn data_field_rename_does_not_hit_type_parameter() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "data Pair a = Pair {a: Int, b: a}\nmk = Pair {a: 1, b: 2}\n",
        );
        let doc = ws.doc(&uri);
        // Cursor on the FIELD `a` inside the constructor record.
        let off = doc.source.find("{a: Int").expect("field a") + 1;
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "first"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        let out = apply_edits(&doc.source, edits);
        assert_eq!(
            out,
            "data Pair a = Pair {first: Int, b: a}\nmk = Pair {first: 1, b: 2}\n",
            "the type parameter `a` (header and field type) must be untouched; edits: {edits:?}"
        );
    }

    // ── Finding 11: case-class changes are rejected ─────────────────

    #[test]
    fn rename_rejects_constructor_to_lowercase() {
        let mut ws = TestWorkspace::new();
        let src = "data Shape = Circle {radius: Int}\n\
                   area = \\s -> case s of\n  Circle c -> c.radius\n  _ -> 0\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("Circle").expect("ctor");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "round"));
        assert!(
            edit.is_none(),
            "lowercase-initial name for a constructor would re-lex as a variable: {edit:?}"
        );
    }

    #[test]
    fn rename_rejects_function_to_uppercase() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "double = \\x -> x * 2\nmain = double 1\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("double =").expect("def");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "Double"));
        assert!(
            edit.is_none(),
            "uppercase-initial name for a variable would re-lex as a constructor: {edit:?}"
        );
    }

    // ── Finding 13: local binders resolve from their definition token ──

    #[test]
    fn rename_local_binder_from_its_definition_token() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "f = \\total -> total + 1\n");
        let doc = ws.doc(&uri);
        // Cursor ON the binder token itself.
        let off = doc.source.find("\\total").expect("binder") + 1;
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "amount"))
            .expect("rename resolves the binder from its definition token");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        let out = apply_edits(&doc.source, edits);
        assert_eq!(out, "f = \\amount -> amount + 1\n");
    }

    #[test]
    fn references_resolve_from_local_binder_token() {
        use crate::references::handle_references;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "f = \\total -> total + 1\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("\\total").expect("binder") + 1;
        let pos = offset_to_position(&doc.source, off);
        let params = ReferenceParams {
            text_document_position: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position: pos,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
            context: ReferenceContext {
                include_declaration: false,
            },
        };
        let locs = handle_references(&ws.state, &params)
            .expect("references resolve from the binder token");
        // At least the body usage `total + 1`.
        let usage_off = doc.source.find("total + 1").expect("usage");
        let usage_pos = offset_to_position(&doc.source, usage_off);
        assert!(
            locs.iter().any(|l| l.range.start == usage_pos),
            "body usage must be reported; got: {locs:?}"
        );
    }

    #[test]
    fn document_highlight_resolves_from_local_binder_token() {
        use crate::document_highlight::handle_document_highlight;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "f = \\total -> total + 1\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("\\total").expect("binder") + 1;
        let pos = offset_to_position(&doc.source, off);
        let params = DocumentHighlightParams {
            text_document_position_params: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position: pos,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        };
        let highlights = handle_document_highlight(&ws.state, &params)
            .expect("highlight resolves from the binder token");
        assert_eq!(
            highlights.len(),
            2,
            "binder (write) + one usage (read); got: {highlights:?}"
        );
    }

    // ── Finding 7: no name-keyed retargeting in references ──────────

    #[test]
    fn references_on_field_does_not_retarget_same_named_top_level() {
        use crate::references::handle_references;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "count = \\x -> x\ng = {count: 2}\n");
        let doc = ws.doc(&uri);
        // Cursor on the record FIELD named `count`.
        let off = doc.source.find("{count").expect("field") + 1;
        let pos = offset_to_position(&doc.source, off);
        let params = ReferenceParams {
            text_document_position: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position: pos,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
            context: ReferenceContext {
                include_declaration: true,
            },
        };
        let locs = handle_references(&ws.state, &params);
        assert!(
            locs.is_none(),
            "a field token must not resolve to the unrelated top-level symbol: {locs:?}"
        );
    }

    // ── Finding 2: caret immediately after an identifier ────────────

    #[test]
    fn prepare_rename_accepts_caret_after_identifier() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "double = \\x -> x * 2\nmain = double 1\n");
        let doc = ws.doc(&uri);
        // Caret right AFTER the last char of `double` at the call site.
        let off = doc.source.find("double 1").expect("usage") + "double".len();
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_prepare_rename(&ws.state, &prepare_params(&uri, pos))
            .expect("caret-after-word must resolve the identifier");
        match resp {
            PrepareRenameResponse::RangeWithPlaceholder { placeholder, .. } => {
                assert_eq!(placeholder, "double");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    // ── Finding 12: linked editing recursion through UnaryOp ────────

    #[test]
    fn linked_editing_finds_fields_under_unary_op() {
        use crate::linked_editing::handle_linked_editing_range;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "g = \\p -> {amt: -p.amt}\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("p.amt").expect("access") + 2;
        let pos = offset_to_position(&doc.source, off);
        let params = LinkedEditingRangeParams {
            text_document_position_params: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position: pos,
            },
            work_done_progress_params: Default::default(),
        };
        let resp = handle_linked_editing_range(&ws.state, &params)
            .expect("field under unary negation must be linked");
        assert_eq!(resp.ranges.len(), 2, "field name + access; got: {:?}", resp.ranges);
    }

    // ── Finding 6: local declaration outranks a same-named import ──

    #[test]
    fn references_prefer_local_definition_over_import() {
        use crate::references::handle_references;
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        let owner_uri = tw.write_and_open("owner.knot", "parse = \\x -> x\nmain = parse 9\n");
        let consumer_uri = tw.write_and_open(
            "consumer.knot",
            "import ./owner\n\nparse = \\y -> y\nrun = parse 1\n",
        );
        let consumer_doc = tw.workspace.doc(&consumer_uri);
        let off = consumer_doc.source.find("parse 1").expect("local usage");
        let pos = offset_to_position(&consumer_doc.source, off);
        let params = ReferenceParams {
            text_document_position: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier {
                    uri: consumer_uri.clone(),
                },
                position: pos,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
            context: ReferenceContext {
                include_declaration: true,
            },
        };
        let locs = handle_references(&tw.workspace.state, &params)
            .expect("references found");
        assert!(
            locs.iter().all(|l| l.uri == consumer_uri),
            "the local `parse` must not merge the imported module's references; got: {locs:?}"
        );
        let _ = owner_uri;
    }

    // ── Finding 10: importer-side rename rewrites type annotations ──

    #[test]
    fn rename_type_updates_importer_annotations() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        let owner_uri = tw.write_and_open("owner.knot", "type Shape = {radius: Int}\n");
        let consumer_uri = tw.write_and_open(
            "consumer.knot",
            "import ./owner\n\nf : Shape -> Int\nf = \\s -> 1\n",
        );
        let owner_doc = tw.workspace.doc(&owner_uri);
        let off = owner_doc.source.find("Shape").expect("type def");
        let pos = offset_to_position(&owner_doc.source, off);
        let edit = handle_rename(
            &tw.workspace.state,
            &rename_params(&owner_uri, pos, "Form"),
        )
        .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let consumer_doc = tw.workspace.doc(&consumer_uri);
        let consumer_edits = changes
            .get(&consumer_uri)
            .expect("consumer annotation must be edited");
        let out = apply_edits(&consumer_doc.source, consumer_edits);
        assert_eq!(
            out, "import ./owner\n\nf : Form -> Int\nf = \\s -> 1\n",
            "the importer's type annotation must be rewritten; edits: {consumer_edits:?}"
        );
    }

    // ── Body-line definition token of typed functions ────────────────
    //
    // The parser merges `f : T` ⏎ `f = body` into ONE DeclKind::Fun. Rename
    // used to edit only the FIRST whole-word occurrence (the signature line)
    // plus references, leaving the body-line `f` behind — silent corruption.

    #[test]
    fn rename_typed_function_edits_both_definition_lines() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "double : Int -> Int\ndouble = \\x -> x * 2\nmain = double 2\n",
        );
        let doc = ws.doc(&uri);
        let off = doc.source.find("double").expect("sig line");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "triple"))
            .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let edits = changes.get(&uri).expect("edits for main");
        let out = apply_edits(&doc.source, edits);
        assert_eq!(
            out,
            "triple : Int -> Int\ntriple = \\x -> x * 2\nmain = triple 2\n",
            "the body-line definition token must be renamed too; edits: {edits:?}"
        );
    }

    #[test]
    fn rename_typed_function_initiated_from_body_line_token() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "double : Int -> Int\ndouble = \\x -> x * 2\nmain = double 2\n",
        );
        let doc = ws.doc(&uri);
        // Cursor on the BODY-line `double` (second occurrence).
        let off = doc.source.find("double =").expect("body line");
        let pos = offset_to_position(&doc.source, off);
        let prep = handle_prepare_rename(&ws.state, &prepare_params(&uri, pos));
        assert!(prep.is_some(), "prepare must accept the body-line token");
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "triple"))
            .expect("rename emits edit");
        let edits = edit.changes.expect("changes").remove(&uri).expect("edits");
        let out = apply_edits(&doc.source, &edits);
        assert_eq!(
            out,
            "triple : Int -> Int\ntriple = \\x -> x * 2\nmain = triple 2\n"
        );
    }

    #[test]
    fn rename_typed_function_in_unopened_disk_file() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        // Owner exists ONLY on disk — never opened — and has a separate
        // type signature, exercising `apply_owner_disk_edits`.
        let owner_src = "parse : Int -> Int\nparse = \\x -> x\n";
        std::fs::write(tw.root.join("owner.knot"), owner_src).expect("write owner");
        let consumer_uri = tw.write_and_open(
            "consumer.knot",
            "import ./owner\n\nmain = parse 5\n",
        );
        let consumer_doc = tw.workspace.doc(&consumer_uri);
        let off = consumer_doc.source.find("parse 5").expect("call site");
        let pos = offset_to_position(&consumer_doc.source, off);
        let edit = handle_rename(
            &tw.workspace.state,
            &rename_params(&consumer_uri, pos, "parsed"),
        )
        .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let owner_uri_entry = changes
            .iter()
            .find(|(u, _)| u.as_str().contains("owner.knot"))
            .expect("owner file must be edited");
        let out = apply_edits(owner_src, owner_uri_entry.1);
        assert_eq!(
            out, "parsed : Int -> Int\nparsed = \\x -> x\n",
            "disk-path rename must edit BOTH definition lines; edits: {:?}",
            owner_uri_entry.1
        );
    }

    // ── Primed identifiers (`x'`) ────────────────────────────────────

    #[test]
    fn rename_primed_identifier_covers_whole_token() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "f = \\x' -> x' + 1\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("x' +").expect("usage");
        let pos = offset_to_position(&doc.source, off);
        let prep = handle_prepare_rename(&ws.state, &prepare_params(&uri, pos))
            .expect("prepare accepts primed identifier");
        match prep {
            PrepareRenameResponse::RangeWithPlaceholder { placeholder, .. } => {
                assert_eq!(placeholder, "x'", "placeholder must include the prime");
            }
            other => panic!("unexpected response: {other:?}"),
        }
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "y'"))
            .expect("rename emits edit");
        let edits = edit.changes.expect("changes").remove(&uri).expect("edits");
        let out = apply_edits(&doc.source, &edits);
        assert_eq!(
            out, "f = \\y' -> y' + 1\n",
            "renaming `x'` must not leave stray primes; edits: {edits:?}"
        );
    }

    // ── New-name validation must match the (ASCII-only) lexer ────────

    #[test]
    fn rename_rejects_non_ascii_new_name() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "double = \\x -> x * 2\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("double").expect("def");
        let pos = offset_to_position(&doc.source, off);
        // `naïve` passes Unicode is_alphabetic but the lexer is ASCII-only —
        // accepting it would corrupt every edited file.
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "naïve"));
        assert!(edit.is_none(), "non-ASCII name must be rejected: {edit:?}");
    }

    #[test]
    fn rename_rejects_all_lexer_keywords() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "double = \\x -> x * 2\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("double").expect("def");
        let pos = offset_to_position(&doc.source, off);
        // Previously missing from KEYWORDS: serve/unit/refine/forall/true/false.
        for kw in ["serve", "unit", "refine", "forall", "true", "false"] {
            let edit = handle_rename(&ws.state, &rename_params(&uri, pos, kw));
            assert!(edit.is_none(), "rename to keyword `{kw}` must be rejected");
        }
    }

    // ── Cross-file rename must reach unopened importers even when the
    // reverse-import graph is non-empty (it only has edges for OPEN docs) ──

    #[test]
    fn rename_reaches_unopened_importer_when_another_importer_is_open() {
        use crate::test_support::TempWorkspace;
        let mut tw = TempWorkspace::new();
        let owner_uri = tw.write_and_open("owner.knot", "parse = \\x -> x\n");
        let _open_consumer =
            tw.write_and_open("consumer1.knot", "import ./owner\n\nuse1 = parse 1\n");
        // A second importer exists ONLY on disk — never opened, never analyzed.
        let c2_src = "import ./owner\n\nuse2 = parse 2\n";
        std::fs::write(tw.root.join("consumer2.knot"), c2_src).unwrap();
        // Simulate the reverse-import graph the real server builds from open
        // docs: owner ← consumer1 (and ONLY consumer1 — consumer2 was never
        // analyzed, so it has no edge). The old code skipped the workspace
        // sweep whenever this graph was non-empty, silently missing
        // consumer2.
        let owner_path = tw.root.join("owner.knot").canonicalize().unwrap();
        let c1_path = tw.root.join("consumer1.knot").canonicalize().unwrap();
        tw.workspace
            .state
            .reverse_imports
            .entry(owner_path)
            .or_default()
            .insert(c1_path);

        let owner_doc = tw.workspace.doc(&owner_uri);
        let off = owner_doc.source.find("parse").expect("def");
        let pos = offset_to_position(&owner_doc.source, off);
        let edit = handle_rename(
            &tw.workspace.state,
            &rename_params(&owner_uri, pos, "parsed"),
        )
        .expect("rename emits edit");
        let changes = edit.changes.expect("changes present");
        let c2_entry = changes
            .iter()
            .find(|(u, _)| u.as_str().contains("consumer2.knot"))
            .expect("UNOPENED importer must receive edits");
        let out = apply_edits(c2_src, c2_entry.1);
        assert_eq!(out, "import ./owner\n\nuse2 = parsed 2\n");
        assert!(
            changes.keys().any(|u| u.as_str().contains("consumer1.knot")),
            "open importer must still be edited; got {:?}",
            changes.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn rename_accepts_primed_new_name() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "double = \\x -> x * 2\nmain = double 1\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("double").expect("def");
        let pos = offset_to_position(&doc.source, off);
        // `'` is a legal identifier-continue char in the lexer.
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "double'"));
        assert!(edit.is_some(), "primed new name must be accepted");
    }

    #[test]
    fn rename_route_endpoint_constructor_updates_route_decl_and_serve() {
        // Renaming the endpoint constructor from its `serve` handler must also
        // rewrite the route declaration's `= GetUsers` site (spanless in the
        // AST) — otherwise the route dangles and serve exhaustiveness breaks.
        let mut ws = TestWorkspace::new();
        let src = r#"type Resp = {ok: Bool}
route Api where
  GET /users -> Resp = GetUsers

srv = serve Api where GetUsers = \req -> Ok {value: {ok: true}}
"#;
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        // Start the rename from the `serve` handler's endpoint token.
        let off = doc.source.find("GetUsers = \\req").expect("serve endpoint");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "ListUsers"))
            .expect("rename emits edit");
        let edits = edit.changes.expect("changes").remove(&uri).expect("edits");
        let out = apply_edits(&doc.source, &edits);
        assert!(
            out.contains("-> Resp = ListUsers"),
            "route decl endpoint constructor must be renamed; got:\n{out}"
        );
        assert!(
            out.contains("serve Api where ListUsers ="),
            "serve handler endpoint must be renamed; got:\n{out}"
        );
        assert!(
            !out.contains("GetUsers"),
            "no stale GetUsers should remain; got:\n{out}"
        );
    }

    #[test]
    fn rename_source_updates_subset_constraint_refs() {
        // Renaming a source relation must rewrite its occurrences inside a
        // `*sub <= *sup` subset constraint (spanless `RelationPath`), including
        // when the same relation appears on both sides of a uniqueness
        // constraint. Otherwise the constraint dangles and breaks the source.
        let mut ws = TestWorkspace::new();
        let src = "*people : [{name: Text, email: Text}]\n\
                   *orders : [{customer: Text}]\n\
                   *orders.customer <= *people.name\n\
                   *people <= *people.email\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        // Rename `people` starting from its declaration.
        let off = doc.source.find("people").expect("source decl");
        let pos = offset_to_position(&doc.source, off);
        let edit = handle_rename(&ws.state, &rename_params(&uri, pos, "humans"))
            .expect("rename emits edit");
        let edits = edit.changes.expect("changes").remove(&uri).expect("edits");
        let out = apply_edits(&doc.source, &edits);
        assert!(
            out.contains("*orders.customer <= *humans.name"),
            "referential-integrity constraint must be renamed; got:\n{out}"
        );
        assert!(
            out.contains("*humans <= *humans.email"),
            "both sides of the uniqueness constraint must be renamed; got:\n{out}"
        );
        assert!(
            !out.contains("people"),
            "no stale `people` should remain; got:\n{out}"
        );
    }
}
