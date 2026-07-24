//! `textDocument/prepareRename` and `textDocument/rename` handlers.

use std::collections::HashMap;

use lsp_types::*;

use knot::ast::{self, Span};

use crate::state::{builtins, DocumentState, ServerState, KEYWORDS};
use crate::utils::{
    find_word_in_source, find_word_last_in_source, ident_lookup_offset,
    position_to_offset, recurse_expr,
    safe_slice, span_to_range, top_fields, word_at_position,
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

    if !is_ref && !is_def && !is_field {
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

    // Identify the symbol's definition in this file. Rename is confined to
    // the current document — the language no longer has imports, so a symbol
    // is only ever visible within its own file.
    let (decl_span, owner_name_span) = resolve_local_owner(doc, offset, &old_name)?;

    let mut changes: HashMap<Uri, Vec<TextEdit>> = HashMap::new();

    // Rename the declaration itself.
    let name_span =
        name_span_within(&doc.source, decl_span, &old_name).unwrap_or(owner_name_span);
    changes.entry(uri.clone()).or_default().push(TextEdit {
        range: span_to_range(name_span, &doc.source),
        new_text: pun_aware_new_text(&doc.module, &doc.source, name_span, &old_name, new_name),
    });
    // Rename every local usage that resolves to this definition.
    for (usage_span, target_span) in &doc.references {
        if *target_span == decl_span || *target_span == name_span {
            // `SourceRef`/`DerivedRef` usage spans include the `*`/`&`
            // sigil — the edit must only replace the name.
            let span = edit_span(&doc.source, *usage_span);
            changes.entry(uri.clone()).or_default().push(TextEdit {
                range: span_to_range(span, &doc.source),
                new_text: pun_aware_new_text(&doc.module, &doc.source, span, &old_name, new_name),
            });
        }
    }

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

    // Capture/shadowing guard (B71): if renaming into (or out of) a scope that
    // already binds `new_name` would silently change name resolution, don't
    // hand the client a set of edits it will apply blind. Flag the whole edit
    // for user confirmation with an explanatory annotation instead. The check
    // runs against the originating document — the file the cursor is in, where
    // local-binding renames (the B71 case) are always confined.
    let mut conflicts: Vec<Span> = Vec::new();
    collect_shadowed_names(&doc.module, &old_name, new_name, &mut conflicts);
    let has_edits = changes.values().any(|edits| !edits.is_empty());
    if has_edits && !conflicts.is_empty() {
        return Some(workspace_edit_with_conflict_warning(changes, &old_name, new_name));
    }

    Some(WorkspaceEdit {
        changes: Some(changes),
        ..Default::default()
    })
}

/// Wrap computed rename `changes` in a `WorkspaceEdit` that flags every edit
/// for user confirmation with a name-capture warning. A bare `changes` map
/// cannot reference a change annotation, so we mirror the edits into
/// `document_changes` as `AnnotatedTextEdit`s pointing at a single
/// `needs_confirmation` annotation. `changes` is retained too: clients that
/// don't advertise `documentChanges`/`changeAnnotationSupport` fall back to it
/// and apply the rename exactly as before (just unwarned), while capable
/// clients surface the confirmation prompt before touching the buffer.
fn workspace_edit_with_conflict_warning(
    changes: HashMap<Uri, Vec<TextEdit>>,
    old_name: &str,
    new_name: &str,
) -> WorkspaceEdit {
    const ANNOTATION_ID: &str = "knot.rename.capture";
    let document_changes = DocumentChanges::Edits(
        changes
            .iter()
            .map(|(uri, edits)| TextDocumentEdit {
                text_document: OptionalVersionedTextDocumentIdentifier {
                    uri: uri.clone(),
                    version: None,
                },
                edits: edits
                    .iter()
                    .cloned()
                    .map(|text_edit| {
                        OneOf::Right(AnnotatedTextEdit {
                            text_edit,
                            annotation_id: ANNOTATION_ID.to_string(),
                        })
                    })
                    .collect(),
            })
            .collect(),
    );
    let mut change_annotations = HashMap::new();
    change_annotations.insert(
        ANNOTATION_ID.to_string(),
        ChangeAnnotation {
            label: format!("Rename '{old_name}' → '{new_name}' shadows an existing binding"),
            needs_confirmation: Some(true),
            description: Some(format!(
                "'{new_name}' is already bound in a scope where '{old_name}' is used. \
                 Applying this rename will capture or shadow that binding and may change \
                 the program's meaning."
            )),
        },
    );
    WorkspaceEdit {
        changes: Some(changes),
        document_changes: Some(document_changes),
        change_annotations: Some(change_annotations),
    }
}

/// Resolve the local definition of the symbol at `offset` in this document.
/// Returns `(decl_span, name_span)` — the whole declaration span and the
/// symbol's name-token span within it. Rename is confined to the current
/// file, so we never look beyond `doc`.
fn resolve_local_owner(
    doc: &DocumentState,
    offset: usize,
    name: &str,
) -> Option<(Span, Span)> {
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
        let name_span = name_span_within(&doc.source, decl_span, name).unwrap_or(decl_span);
        return Some((decl_span, name_span));
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
    module: &ast::Expr,
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
///
/// `Field<Expr>` and `RecordField` both expose a field name + value expr;
/// this trait lets the pun detector work over either.
trait PunField {
    fn field_name(&self) -> &str;
    fn field_value(&self) -> &ast::Expr;
}

impl PunField for ast::Field<ast::Expr> {
    fn field_name(&self) -> &str {
        &self.name
    }
    fn field_value(&self) -> &ast::Expr {
        &self.value
    }
}

impl PunField for ast::RecordField {
    fn field_name(&self) -> &str {
        &self.name
    }
    fn field_value(&self) -> &ast::Expr {
        &self.value
    }
}

fn span_is_record_pun(module: &ast::Expr, source: &str, span: Span) -> bool {
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
    fn pun_field_in_fields<F: PunField>(
        fields: &[F],
        mut search_start: usize,
        source: &str,
        span: Span,
    ) -> bool {
        for f in fields {
            let (name, value) = (f.field_name(), f.field_value());
            // A pun field's value span IS the field-name token; an explicit
            // field has its name token (in the window before the value).
            let named = find_word_in_source(source, name, search_start, value.span.start)
                .is_some();
            if !named
                && value.span == span
                && matches!(&value.node, ast::ExprKind::Var(n) if n == name)
            {
                return true;
            }
            search_start = value.span.end;
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
                    if let ast::StmtKind::Bind { pat, .. } = &stmt.node
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
    for decl in top_fields(module) {
        let dspan = decl.value.span;
        if dspan.start > span.start || span.end > dspan.end {
            continue;
        }
        match &decl.value.node {
            ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => {
                pun_in_expr(body, source, span, &mut found)
            }
            _ => {
                // A named function field.
                pun_in_expr(&decl.value, source, span, &mut found)
            }
        }
    }
    found
}

/// Narrow a reference span to its editable name token. `SourceRef` /
/// `DerivedRef` expression spans include the leading `*`/`&` sigil (the
/// parser builds them from the sigil token's start), and identifiers can
/// never begin with those bytes — so a rename edit must skip the sigil or
/// it gets deleted along with the old name. Reference *display* (find-
/// references, highlight) keeps the full span; only edits are narrowed.
fn edit_span(source: &str, span: Span) -> Span {
    let bytes = source.as_bytes();
    // Skip any leading noise before the identifier: a `(` folded into the span
    // (the parser rewrites a parenthesized expression's span to start at `(`),
    // the `*`/`&` relation sigils, and ASCII whitespace. This handles nested
    // forms like `(*users)` / `((*users))` / `(&d)` whose SourceRef/DerivedRef
    // span begins at `(`, not the sigil — narrowing only `span.start` would
    // otherwise replace the whole `(*users)` and drop the sigil and parens.
    let mut start = span.start;
    while start < span.end {
        match bytes.get(start) {
            Some(b'(') | Some(b'*') | Some(b'&') => start += 1,
            Some(b) if b.is_ascii_whitespace() => start += 1,
            _ => break,
        }
    }
    // Take the contiguous identifier run (matches the lexer's
    // `is_ident_continue`: ASCII alphanumeric, `_`, and a trailing `'` prime),
    // so a trailing `)` — or anything else the folded span swept up — is left
    // in place.
    let mut end = start;
    while end < span.end {
        match bytes.get(end) {
            Some(b) if b.is_ascii_alphanumeric() || *b == b'_' || *b == b'\'' => end += 1,
            _ => break,
        }
    }
    // Defensive: if no identifier was found (unexpected shape), fall back to the
    // original span rather than emit an empty/inverted edit.
    if end > start {
        Span::new(start, end)
    } else {
        span
    }
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
        ast::PatKind::Annot { pat, .. } => pat_binds_name(pat, name),
    }
}

/// Detect lexical name-capture / shadowing conflicts that renaming `old_name`
/// to `new_name` would introduce within `module`, pushing the span of each
/// offending site into `out`. A rename is a conflict when it silently changes
/// which binding a name resolves to. Three shapes are caught:
///
///   * a use of `old_name` whose nearest enclosing binder-of-interest is a
///     `new_name` binder — after the rename the use resolves to that inner
///     binder instead of the renamed one (the reported B71 capture:
///     `f = \x -> \y -> x + y`, renaming `x` to `y` yields
///     `\y -> \y -> y + y`);
///   * a use of `new_name` whose nearest enclosing binder-of-interest is an
///     `old_name` binder — the renamed binder would capture that use
///     (`f = \y -> \x -> y + x`, renaming `x` to `y`);
///   * a single scope that binds both names — the rename collapses them into a
///     duplicate binder (`\x y -> …`, renaming `x` to `y`).
///
/// The walk is lexical and scope-precise: it tracks a stack of the enclosing
/// binder scopes (lambda params, `case` arms, `do` binds/lets, impl and trait
/// method params) and, for each use, inspects only the *nearest* relevant
/// binder — so an inner rebinding of the same name correctly shields outer
/// scopes from a false positive. It never reports a rename that leaves name
/// resolution unchanged, and never misses a genuine capture. An empty `out`
/// means the rename is capture-free.
pub(crate) fn collect_shadowed_names(
    module: &ast::Expr,
    old_name: &str,
    new_name: &str,
    out: &mut Vec<Span>,
) {
    // The binder stack runs outer→inner; `stack.last()` is the innermost
    // scope. Each frame records whether it binds the old and/or new name.
    // `nearest_is_new`/`nearest_is_old` scan inward-out and stop at the first
    // frame binding either name — that frame is the one a use resolves to.
    fn nearest_is_new(stack: &[(bool, bool)]) -> bool {
        for &(binds_old, binds_new) in stack.iter().rev() {
            if binds_new {
                return true;
            }
            if binds_old {
                return false;
            }
        }
        false
    }
    fn nearest_is_old(stack: &[(bool, bool)]) -> bool {
        for &(binds_old, binds_new) in stack.iter().rev() {
            if binds_old {
                return true;
            }
            if binds_new {
                return false;
            }
        }
        false
    }
    fn walk(
        expr: &ast::Expr,
        old_name: &str,
        new_name: &str,
        stack: &mut Vec<(bool, bool)>,
        out: &mut Vec<Span>,
    ) {
        match &expr.node {
            ast::ExprKind::Var(n) => {
                // A renamed `old_name` use captured by an inner `new_name`
                // binder, or an existing `new_name` use the renamed binder
                // would capture — both silently change resolution.
                if (n == old_name && nearest_is_new(stack))
                    || (n == new_name && nearest_is_old(stack))
                {
                    out.push(expr.span);
                }
                return;
            }
            ast::ExprKind::Lambda { params, body, .. } => {
                let binds_old = params.iter().any(|p| pat_binds_name(p, old_name));
                let binds_new = params.iter().any(|p| pat_binds_name(p, new_name));
                // Siblings collapsing into one name is a duplicate binder even
                // with no body use (`\x y -> …` → `\y y -> …`).
                if binds_old && binds_new {
                    out.push(expr.span);
                }
                stack.push((binds_old, binds_new));
                walk(body, old_name, new_name, stack, out);
                stack.pop();
                return;
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                walk(scrutinee, old_name, new_name, stack, out);
                for arm in arms {
                    let binds_old = pat_binds_name(&arm.pat, old_name);
                    let binds_new = pat_binds_name(&arm.pat, new_name);
                    if binds_old && binds_new {
                        out.push(arm.pat.span);
                    }
                    stack.push((binds_old, binds_new));
                    walk(&arm.body, old_name, new_name, stack, out);
                    stack.pop();
                }
                return;
            }
            ast::ExprKind::Do(stmts) => {
                // A `do` bind/let extends the scope for *subsequent* statements
                // only; its own RHS sees the pre-bind scope. Model each bind as
                // its own frame pushed in statement order, so a later bind is
                // "inner" to an earlier one and shadows it.
                let mut pushed = 0usize;
                for stmt in stmts {
                    match &stmt.node {
                        ast::StmtKind::Bind { pat, expr } => {
                            walk(expr, old_name, new_name, stack, out);
                            let binds_old = pat_binds_name(pat, old_name);
                            let binds_new = pat_binds_name(pat, new_name);
                            if binds_old && binds_new {
                                out.push(pat.span);
                            }
                            stack.push((binds_old, binds_new));
                            pushed += 1;
                        }
                        ast::StmtKind::Where { cond } | ast::StmtKind::Expr(cond) => {
                            walk(cond, old_name, new_name, stack, out);
                        }
                        ast::StmtKind::GroupBy { key } => {
                            walk(key, old_name, new_name, stack, out);
                        }
                    }
                }
                for _ in 0..pushed {
                    stack.pop();
                }
                return;
            }
            _ => {}
        }
        // Non-scope nodes: descend without touching the binder stack.
        recurse_expr(expr, |child| walk(child, old_name, new_name, stack, out));
    }

    for decl in top_fields(module) {
        match &decl.value.node {
            ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => {
                walk(body, old_name, new_name, &mut Vec::new(), out);
            }
            _ => {
                // A named function field.
                walk(&decl.value, old_name, new_name, &mut Vec::new(), out);
            }
        }
    }
}

/// True if `offset` lands on a record field name. Field names live in:
/// record-literal field bindings (`{name: x}`), record patterns (`{name: y}`),
/// field accesses (`p.name`), record-type fields (`type T = {name: Text}`),
/// and data-constructor fields (`Circle {radius: Float}`). The AST stores
/// field names as bare strings (no per-name span), so we recover spans by
/// scanning the field's containing source range.
pub(crate) fn is_at_record_field(module: &ast::Expr, source: &str, offset: usize) -> bool {
    field_position_at(module, source, offset).is_some()
}

/// Find the field name (and its span) at `offset`. Returns `None` if the
/// cursor isn't on a field-name token.
fn field_position_at(
    module: &ast::Expr,
    source: &str,
    offset: usize,
) -> Option<(String, Span)> {
    let mut found: Option<(String, Span)> = None;
    for decl in top_fields(module) {
        if decl.value.span.start > offset || offset >= decl.value.span.end {
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
    module: &ast::Expr,
    source: &str,
    name: &str,
) -> Vec<Span> {
    let mut out: Vec<Span> = Vec::new();
    for decl in top_fields(module) {
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
fn field_sites_in_decl<F: FnMut(&str, Span)>(decl: &ast::RecordField, source: &str, f: &mut F) {
    let dspan = decl.value.span;
    // The field's own signature can carry record types whose field names must
    // rename in lockstep with the body (`mkPerson : Text -> {name: Text}`).
    if let Some(scheme) = &decl.sig {
        field_sites_in_type(&scheme.ty, source, f);
    }
    match &decl.value.node {
        ast::ExprKind::ViewDecl { ty, body, .. } | ast::ExprKind::DerivedDecl { ty, body, .. } => {
            if let Some(scheme) = ty {
                field_sites_in_type(&scheme.ty, source, f);
            }
            field_sites_in_expr(body, source, f);
        }
        ast::ExprKind::DataCtor { constructors, .. } => {
            // Constructor fields appear sequentially in source order, so a
            // single running cursor across all constructors keeps each
            // field-name search confined to its own slot. The search starts
            // after the `=` — the header (`data Pair a = …`) contains type
            // parameter tokens that can collide with field names (renaming
            // field `a` in `data Pair a = Pair {a: Int}` must not match the
            // type parameter `a`).
            let decl_text = safe_slice(source, dspan);
            let mut search_start = decl_text
                .find('=')
                .map(|i| dspan.start + i + 1)
                .unwrap_or(dspan.start);
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
        ast::ExprKind::TypeCtor { name: _, ty, .. } | ast::ExprKind::SourceDecl { ty, .. } => {
            field_sites_in_type(ty, source, f);
        }
        ast::ExprKind::RouteDecl { entries, .. } => {
            // Route field names are declared inline (`body {userId: Int}`,
            // `?{q: Text}`, `headers {auth: Text}`, and the response record
            // type) before their type. They appear in source order: body, path
            // params, query, request headers, response type, response headers.
            // A single running cursor confines each name search to its own slot
            // (mirroring the `Data`/`Record` walks).
            for entry in entries {
                let mut cursor = dspan.start;
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
                let mut path_name_cursor = dspan.start;
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
        ast::ExprKind::SourceDecl { .. } | ast::ExprKind::RouteCompositeDecl { .. }
        | ast::ExprKind::SubsetConstraint { .. } => {}
        _ => {
            // A named function field: walk its body.
            field_sites_in_expr(&decl.value, source, f);
        }
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
                    ast::StmtKind::Bind { pat, .. } => {
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

