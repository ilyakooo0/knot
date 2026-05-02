//! `textDocument/inlayHint` handler. Surfaces inferred types, effects,
//! parameter names, monad context, and unit annotations as inline hints.

use lsp_types::*;

use knot::ast::{self, DeclKind, Span};
use knot_compiler::infer::MonadKind;

use crate::shared::{extract_param_names, flatten_app_chain, parse_function_params};
use crate::state::{DocumentState, ServerState};
use crate::utils::{
    offset_to_position, position_to_offset, recurse_expr, safe_slice,
};

// ── Inlay Hints ─────────────────────────────────────────────────────

pub(crate) fn handle_inlay_hint(
    state: &ServerState,
    params: &InlayHintParams,
) -> Option<Vec<InlayHint>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let mut hints = Vec::new();

    let range_start = position_to_offset(&doc.source, params.range.start);
    let range_end = position_to_offset(&doc.source, params.range.end);

    // Show inferred types for unannotated function declarations.
    // For annotated functions, show only the inferred *effects* if they exist
    // and aren't already in the type signature.
    for decl in &doc.module.decls {
        // Only show hints within the visible range
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }

        match &decl.node {
            DeclKind::Fun { name, ty: None, .. } => {
                if let Some(inferred) = doc.type_info.get(name) {
                    let decl_text = safe_slice(&doc.source, decl.span);
                    let name_end = decl_text.find(|c: char| !c.is_alphanumeric() && c != '_')
                        .unwrap_or(decl_text.len());
                    let hint_offset = decl.span.start + name_end;
                    let hint_pos = offset_to_position(&doc.source, hint_offset);
                    // Text edit emits the signature as a separate statement above the
                    // function, so anchor it at the declaration start, not at the hint.
                    let edit_pos = offset_to_position(&doc.source, decl.span.start);
                    // Effects (including reads/writes) live inside the IO row of
                    // the rendered type — no extra prefix is needed.
                    let full_sig = inferred.clone();
                    hints.push(InlayHint {
                        position: hint_pos,
                        label: InlayHintLabel::String(format!(": {full_sig}")),
                        kind: Some(InlayHintKind::TYPE),
                        text_edits: Some(vec![TextEdit {
                            range: Range { start: edit_pos, end: edit_pos },
                            new_text: format!("{name} : {full_sig}\n"),
                        }]),
                        tooltip: doc.effect_info.get(name).map(|effects| {
                            InlayHintTooltip::String(format!("Effects: {effects}"))
                        }),
                        padding_left: Some(true),
                        padding_right: Some(true),
                        data: None,
                    });
                }
            }
            DeclKind::Fun { name, ty: Some(_), .. } => {
                // Annotated function: show the inferred *effects* as a hint at
                // the function body's start, only when the type doesn't already
                // declare them. Helps with effect-row polymorphism debugging.
                if let Some(effects) = doc.effect_info.get(name) {
                    let inferred_ty = doc.type_info.get(name);
                    let needs_hint = inferred_ty
                        .map(|ty| !type_str_mentions_effects(ty, effects))
                        .unwrap_or(true);
                    if needs_hint {
                        let hint_offset = name_end_offset(&doc.source, decl.span, name);
                        let hint_pos = offset_to_position(&doc.source, hint_offset);
                        hints.push(InlayHint {
                            position: hint_pos,
                            label: InlayHintLabel::String(format!("-- effects: {effects}")),
                            kind: None,
                            text_edits: None,
                            tooltip: None,
                            padding_left: Some(true),
                            padding_right: None,
                            data: None,
                        });
                    }
                }
            }
            DeclKind::View { name, ty: None, .. } | DeclKind::Derived { name, ty: None, .. } => {
                if let Some(inferred) = doc.type_info.get(name) {
                    let decl_text = safe_slice(&doc.source, decl.span);
                    let name_end = decl_text.find('=').unwrap_or(decl_text.len());
                    let hint_offset = decl.span.start + name_end;
                    let hint_pos = offset_to_position(&doc.source, hint_offset);
                    let full_sig = inferred.clone();
                    hints.push(InlayHint {
                        position: hint_pos,
                        label: InlayHintLabel::String(format!(": {full_sig}")),
                        kind: Some(InlayHintKind::TYPE),
                        text_edits: None,
                        tooltip: doc.effect_info.get(name).map(|e| {
                            InlayHintTooltip::String(format!("Effects: {e}"))
                        }),
                        padding_left: Some(true),
                        padding_right: Some(true),
                        data: None,
                    });
                }
            }
            _ => {}
        }
    }

    // Show inferred types for local bindings (let/bind in do blocks). Reads
    // `unit_info` (populated during analysis) instead of re-parsing each type
    // string per request — this makes the hint cheap when the file has many
    // unit-annotated bindings.
    for (span, ty) in &doc.local_type_info {
        if span.end < range_start || span.start > range_end {
            continue;
        }
        let hint_pos = offset_to_position(&doc.source, span.end);
        let unit_tooltip = doc
            .unit_info
            .get(span)
            .map(|u| InlayHintTooltip::String(format!("Inferred unit: `{u}`")));
        hints.push(InlayHint {
            position: hint_pos,
            label: InlayHintLabel::String(format!(": {ty}")),
            kind: Some(InlayHintKind::TYPE),
            text_edits: None,
            tooltip: unit_tooltip,
            padding_left: Some(true),
            padding_right: None,
            data: None,
        });
    }

    // Show inferred unit hints on numeric literals whose enclosing binding has
    // a unit-annotated type. The literals themselves don't carry explicit unit
    // syntax, so the user otherwise has to mentally trace the type — the hint
    // shows e.g. `<M>` after `42` in `let distance : Float<M> = 42.0`.
    add_unit_literal_hints(doc, range_start, range_end, &mut hints);

    // Show parameter-name hints at named function call sites. The hint shows
    // `name:` before each argument so multi-arg calls don't require jumping to
    // the definition to know which argument is which.
    add_parameter_name_hints(doc, range_start, range_end, &mut hints);

    // Show the resolved monad kind at the start of each `do` block. Helps when
    // the same `do` syntax can desugar to `[]`, `Maybe`, `Result`, or `IO`
    // depending on context.
    add_monad_context_hints(doc, range_start, range_end, &mut hints);

    // Show per-field type hints for record-destructure patterns in case arms,
    // do-binds, and lambda params. The whole-pattern hint (above) shows the
    // record type; this loop adds `: T` after each individual field name so
    // users can see the field types without expanding mentally.
    add_record_pattern_field_hints(doc, range_start, range_end, &mut hints);

    // Closing-label hints — for blocks that span many lines, show a hint at the
    // closing token indicating what's ending. Helps when the opener is far
    // off-screen.
    add_closing_label_hints(doc, range_start, range_end, &mut hints);

    // Trait-constraint hints at call sites of constrained functions. The
    // inferencer doesn't memoize per-call-site substitutions, so we surface
    // the *declared* constraints — useful for spotting "this call brings in
    // an Eq/Ord/Display requirement" without jumping to the definition.
    add_constraint_hints(doc, range_start, range_end, &mut hints);

    // Per-decl re-check telemetry — gated on KNOT_LSP_TRACE_DIRTY since this
    // information is mostly useful when investigating incremental-inference
    // performance, not as everyday UI. Surfaces a "♻" hint at the start of
    // every decl in `dirty_decl_closure` so the developer can see exactly
    // which decls were re-analyzed after an edit.
    if std::env::var("KNOT_LSP_TRACE_DIRTY").is_ok() && !doc.dirty_decl_closure.is_empty() {
        add_dirty_decl_telemetry(doc, range_start, range_end, &mut hints);
    }

    Some(hints)
}

/// Emit a "♻ re-checked" hint at the start of every decl whose name appears
/// in `dirty_decl_closure`. Helps surface incremental-inference activity for
/// developers debugging the per-decl re-check path.
fn add_dirty_decl_telemetry(
    doc: &DocumentState,
    range_start: usize,
    range_end: usize,
    hints: &mut Vec<InlayHint>,
) {
    for decl in &doc.module.decls {
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }
        let name = match &decl.node {
            DeclKind::Fun { name, .. }
            | DeclKind::Data { name, .. }
            | DeclKind::TypeAlias { name, .. }
            | DeclKind::Trait { name, .. }
            | DeclKind::View { name, .. }
            | DeclKind::Derived { name, .. }
            | DeclKind::Source { name, .. } => name.clone(),
            _ => continue,
        };
        if !doc.dirty_decl_closure.contains(&name) {
            continue;
        }
        hints.push(InlayHint {
            position: offset_to_position(&doc.source, decl.span.start),
            label: InlayHintLabel::String("♻ ".into()),
            kind: None,
            text_edits: None,
            tooltip: Some(InlayHintTooltip::String(format!(
                "Re-analyzed: `{name}` is in this edit's dirty closure"
            ))),
            padding_left: None,
            padding_right: Some(true),
            data: None,
        });
    }
}

/// Emit `// end <kind>` style hints at the close of long `do` blocks, lambdas,
/// and case expressions. Only shown when the block spans more than a threshold
/// of lines so we don't pollute short bodies with redundant labels.
fn add_closing_label_hints(
    doc: &DocumentState,
    range_start: usize,
    range_end: usize,
    hints: &mut Vec<InlayHint>,
) {
    const MIN_LINES: u32 = 6;

    fn count_lines(source: &str, span: Span) -> u32 {
        let mut lines = 0u32;
        if span.end <= source.len() {
            for b in source.as_bytes()[span.start..span.end].iter() {
                if *b == b'\n' {
                    lines += 1;
                }
            }
        }
        lines
    }

    fn collect(
        expr: &ast::Expr,
        source: &str,
        out: &mut Vec<(Span, String)>,
    ) {
        match &expr.node {
            ast::ExprKind::Do(_) => {
                if count_lines(source, expr.span) >= MIN_LINES {
                    out.push((expr.span, "end do".into()));
                }
                recurse_expr(expr, |e| collect(e, source, out));
            }
            ast::ExprKind::Case { .. } => {
                if count_lines(source, expr.span) >= MIN_LINES {
                    out.push((expr.span, "end case".into()));
                }
                recurse_expr(expr, |e| collect(e, source, out));
            }
            ast::ExprKind::Lambda { .. } => {
                if count_lines(source, expr.span) >= MIN_LINES {
                    out.push((expr.span, "end λ".into()));
                }
                recurse_expr(expr, |e| collect(e, source, out));
            }
            ast::ExprKind::Atomic { .. } => {
                if count_lines(source, expr.span) >= MIN_LINES {
                    out.push((expr.span, "end atomic".into()));
                }
                recurse_expr(expr, |e| collect(e, source, out));
            }
            _ => recurse_expr(expr, |e| collect(e, source, out)),
        }
    }

    let mut spans: Vec<(Span, String)> = Vec::new();
    for decl in &doc.module.decls {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => collect(body, &doc.source, &mut spans),
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        collect(body, &doc.source, &mut spans);
                    }
                }
            }
            _ => {}
        }
    }

    for (span, label) in spans {
        if span.end < range_start || span.start > range_end {
            continue;
        }
        if span.end == 0 || span.end > doc.source.len() {
            continue;
        }
        let pos = offset_to_position(&doc.source, span.end);
        hints.push(InlayHint {
            position: pos,
            label: InlayHintLabel::String(format!("// {label}")),
            kind: None,
            text_edits: None,
            tooltip: None,
            padding_left: Some(true),
            padding_right: None,
            data: None,
        });
    }
}

/// Walk record-destructure patterns and emit a `: <field-type>` hint at each
/// field-name occurrence inside the pattern. The whole-pattern hint already
/// shows the parent record's type; this complements that by exposing the
/// field types for users who care about a single field.
fn add_record_pattern_field_hints(
    doc: &DocumentState,
    range_start: usize,
    range_end: usize,
    hints: &mut Vec<InlayHint>,
) {
    /// Find each pattern that destructures a record. Tracks both the span
    /// and an optional constructor name for ADT cases like `Person {name}`.
    fn walk_pat_for_records(pat: &ast::Pat, out: &mut Vec<(Span, Option<String>)>) {
        match &pat.node {
            ast::PatKind::Record(_) => out.push((pat.span, None)),
            ast::PatKind::Constructor { name, payload } => {
                if matches!(&payload.node, ast::PatKind::Record(_)) {
                    out.push((pat.span, Some(name.clone())));
                }
                walk_pat_for_records(payload, out);
            }
            ast::PatKind::List(pats) => {
                for p in pats {
                    walk_pat_for_records(p, out);
                }
            }
            _ => {}
        }
    }
    fn walk_expr(expr: &ast::Expr, out: &mut Vec<(Span, Option<String>)>) {
        match &expr.node {
            ast::ExprKind::Lambda { params, body } => {
                for p in params {
                    walk_pat_for_records(p, out);
                }
                walk_expr(body, out);
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                walk_expr(scrutinee, out);
                for arm in arms {
                    walk_pat_for_records(&arm.pat, out);
                    walk_expr(&arm.body, out);
                }
            }
            ast::ExprKind::Do(stmts) => {
                for stmt in stmts {
                    match &stmt.node {
                        ast::StmtKind::Bind { pat, expr }
                        | ast::StmtKind::Let { pat, expr } => {
                            walk_pat_for_records(pat, out);
                            walk_expr(expr, out);
                        }
                        ast::StmtKind::Where { cond } | ast::StmtKind::Expr(cond) => {
                            walk_expr(cond, out);
                        }
                        ast::StmtKind::GroupBy { key } => walk_expr(key, out),
                    }
                }
            }
            _ => recurse_expr(expr, |e| walk_expr(e, out)),
        }
    }

    let mut record_pats: Vec<(Span, Option<String>)> = Vec::new();
    for decl in &doc.module.decls {
        match &decl.node {
            ast::DeclKind::Fun { body: Some(body), .. }
            | ast::DeclKind::View { body, .. }
            | ast::DeclKind::Derived { body, .. } => walk_expr(body, &mut record_pats),
            ast::DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { params, body, .. } = item {
                        for p in params {
                            walk_pat_for_records(p, &mut record_pats);
                        }
                        walk_expr(body, &mut record_pats);
                    }
                }
            }
            _ => {}
        }
    }

    for (span, ctor_opt) in record_pats {
        if span.end < range_start || span.start > range_end {
            continue;
        }
        // Resolve the field set. Prefer the AST-driven constructor lookup —
        // the inferencer's local_type_info often doesn't carry an entry at
        // the constructor pattern's outer span, but the data decl always
        // does.
        let mut fields_str: Vec<(String, String)> = Vec::new();
        let mut tooltip_source = String::from("destructured record");
        if let Some(ctor_name) = ctor_opt.as_deref() {
            for d in &doc.module.decls {
                if let ast::DeclKind::Data { constructors, name: data_name, .. } = &d.node {
                    for c in constructors {
                        if c.name == ctor_name {
                            tooltip_source = format!("{ctor_name} (constructor of {data_name})");
                            for f in &c.fields {
                                fields_str.push((
                                    f.name.clone(),
                                    crate::type_format::format_type_kind(&f.value.node),
                                ));
                            }
                        }
                    }
                }
            }
        }
        if fields_str.is_empty() {
            // Fall back to local_type_info — useful for plain Record
            // destructures (no constructor wrapper).
            let parent_ty = match doc.local_type_info.get(&span) {
                Some(t) => t.clone(),
                None => continue,
            };
            let parsed = crate::parsed_type::ParsedType::parse(&parent_ty);
            let stripped = parsed.strip_io();
            if let Some(fs) = stripped.record_fields() {
                for (n, t) in fs {
                    fields_str.push((n.clone(), t.render()));
                }
                tooltip_source = parent_ty.clone();
            } else if let Some(fs) =
                extract_variant_ctor_fields(stripped, &doc.source, span)
            {
                for (n, t) in fs {
                    fields_str.push((n, t.render()));
                }
                tooltip_source = parent_ty.clone();
            } else {
                continue;
            }
        }
        if fields_str.is_empty() {
            continue;
        }
        // Slice once; we need the pattern source to find each field's
        // position via word-boundary scan.
        let pat_text = match doc.source.get(span.start..span.end) {
            Some(s) => s,
            None => continue,
        };
        for (field_name, ty_str) in fields_str {
            // The field name appears as a whole-word identifier inside the
            // pattern. Search for it with simple boundary checks.
            if let Some(rel_pos) = find_word_boundary(pat_text, &field_name) {
                let abs_end = span.start + rel_pos + field_name.len();
                if abs_end > span.end {
                    continue;
                }
                let hint_pos = offset_to_position(&doc.source, abs_end);
                hints.push(InlayHint {
                    position: hint_pos,
                    label: InlayHintLabel::String(format!(": {ty_str}")),
                    kind: Some(InlayHintKind::TYPE),
                    text_edits: None,
                    tooltip: Some(InlayHintTooltip::String(format!(
                        "Field `{field_name}` destructured from `{tooltip_source}`"
                    ))),
                    padding_left: Some(true),
                    padding_right: None,
                    data: None,
                });
            }
        }
    }
}

/// When the pattern's parent type is a Variant (typical for ADT constructor
/// patterns), pick the constructor whose name appears at the start of the
/// pattern source, then return its record-shaped payload fields. Returns
/// `None` if no constructor matches or the payload isn't a record.
fn extract_variant_ctor_fields(
    parsed: &crate::parsed_type::ParsedType,
    source: &str,
    span: Span,
) -> Option<Vec<(String, crate::parsed_type::ParsedType)>> {
    use crate::parsed_type::ParsedType;
    let ctors = match parsed {
        ParsedType::Variant(cs, _) => cs,
        _ => return None,
    };
    let pat_text = source.get(span.start..span.end)?;
    // Pattern source looks like `Person {name, age}` — pull the first
    // identifier token as the constructor name.
    let bytes = pat_text.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let mut j = i;
    while j < bytes.len()
        && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_')
    {
        j += 1;
    }
    let ctor_name = pat_text.get(i..j)?;
    for (name, payload) in ctors {
        if name == ctor_name {
            if let Some(p) = payload {
                if let Some(fields) = p.record_fields() {
                    return Some(fields.to_vec());
                }
            }
        }
    }
    None
}

/// Locate `word` as a whole-word match in `text`. Returns the byte offset of
/// its first occurrence, or `None`. Avoids matching `name` inside `nameish`.
fn find_word_boundary(text: &str, word: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let needle = word.as_bytes();
    if needle.is_empty() || bytes.len() < needle.len() {
        return None;
    }
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let left_ok = i == 0 || !is_ident(bytes[i - 1]);
            let right_ok = i + needle.len() >= bytes.len() || !is_ident(bytes[i + needle.len()]);
            if left_ok && right_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Extract the unit annotation `<...>` from a formatted type string.
/// Returns the unit text without the angle brackets, or `None` if the type
/// has no unit annotation. Skips trivial dimensionless `<1>` annotations.
fn extract_unit_from_type_str(ty: &str) -> Option<String> {
    let parsed = crate::parsed_type::ParsedType::parse(ty);
    // Look at the function's return type if it's a function; otherwise the
    // whole type. Unit-annotated parameters aren't surfaced here because the
    // hint is anchored to the binding's overall type.
    let value = match &parsed {
        crate::parsed_type::ParsedType::Function(_, ret) => ret.strip_io(),
        other => other.strip_io(),
    };
    value.unit().map(|s| s.to_string())
}

/// Walk every binding-with-unit and emit hints on numeric literals inside the
/// binding's defining expression.
fn add_unit_literal_hints(
    doc: &DocumentState,
    range_start: usize,
    range_end: usize,
    hints: &mut Vec<InlayHint>,
) {
    fn collect_literals_in_expr(expr: &ast::Expr, out: &mut Vec<Span>) {
        if matches!(
            &expr.node,
            ast::ExprKind::Lit(ast::Literal::Int(_)) | ast::ExprKind::Lit(ast::Literal::Float(_))
        ) {
            out.push(expr.span);
        }
        recurse_expr(expr, |e| collect_literals_in_expr(e, out));
    }

    fn collect_literals_in_decl(decl: &ast::Decl, out: &mut Vec<(Span, ast::Expr)>) {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                walk_for_unit_bindings(body, out);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk_for_unit_bindings(body, out);
                    }
                }
            }
            _ => {}
        }
    }

    fn walk_for_unit_bindings(expr: &ast::Expr, out: &mut Vec<(Span, ast::Expr)>) {
        if let ast::ExprKind::Do(stmts) = &expr.node {
            for stmt in stmts {
                if let ast::StmtKind::Let { pat, expr: rhs } | ast::StmtKind::Bind { pat, expr: rhs } =
                    &stmt.node
                {
                    out.push((pat.span, rhs.clone()));
                    walk_for_unit_bindings(rhs, out);
                }
            }
        }
        recurse_expr(expr, |e| walk_for_unit_bindings(e, out));
    }

    let mut bindings_with_rhs: Vec<(Span, ast::Expr)> = Vec::new();
    for decl in &doc.module.decls {
        collect_literals_in_decl(decl, &mut bindings_with_rhs);
    }

    for (binding_span, rhs) in bindings_with_rhs {
        let ty = match doc.local_type_info.get(&binding_span) {
            Some(t) => t,
            None => continue,
        };
        let unit = match extract_unit_from_type_str(ty) {
            Some(u) => u,
            None => continue,
        };
        let mut literals = Vec::new();
        collect_literals_in_expr(&rhs, &mut literals);
        for span in literals {
            if span.end < range_start || span.start > range_end {
                continue;
            }
            hints.push(InlayHint {
                position: offset_to_position(&doc.source, span.end),
                label: InlayHintLabel::String(format!("<{unit}>")),
                kind: Some(InlayHintKind::TYPE),
                text_edits: None,
                tooltip: Some(InlayHintTooltip::String(format!(
                    "Inferred unit `{unit}` from enclosing binding"
                ))),
                padding_left: None,
                padding_right: None,
                data: None,
            });
        }
    }
}

/// Find the byte offset just after the function name within its declaration span.
fn name_end_offset(source: &str, decl_span: Span, _name: &str) -> usize {
    let decl_text = safe_slice(source, decl_span);
    let name_end = decl_text
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(decl_text.len());
    decl_span.start + name_end
}

/// Heuristic: does the rendered type string already mention all of the given
/// effects? Used to suppress redundant effect inlay hints.
fn type_str_mentions_effects(ty: &str, effects: &str) -> bool {
    // The effects string looks like `{console, reads *foo}` — pull the inner
    // tokens and check that each appears in the type string.
    let inner = effects.trim_start_matches('{').trim_end_matches('}');
    if inner.is_empty() {
        return true;
    }
    inner.split(',').all(|tok| ty.contains(tok.trim()))
}

/// Walk the AST looking for App expressions whose callee resolves to a named
/// function with known parameter names. Emit a `name:` hint at the start of
/// each argument expression. Hints are suppressed when the argument is a bare
/// reference whose name already matches the parameter (e.g. `f(name)` →
/// no `name: name` redundant hint), and when the argument occupies the same
/// span as the parameter name itself.
fn add_parameter_name_hints(
    doc: &DocumentState,
    range_start: usize,
    range_end: usize,
    hints: &mut Vec<InlayHint>,
) {
    fn walk_apps(
        expr: &ast::Expr,
        doc: &DocumentState,
        range_start: usize,
        range_end: usize,
        hints: &mut Vec<InlayHint>,
    ) {
        // When we hit an App chain, flatten it and emit hints for the whole
        // chain — but recurse only into the args (not the head), so we don't
        // re-process inner Apps from the same chain.
        if matches!(expr.node, ast::ExprKind::App { .. }) {
            let (callee, args) = flatten_app_chain(expr);
            if let ast::ExprKind::Var(name) = &callee.node {
                emit_arg_hints(doc, name, &args, range_start, range_end, hints);
            }
            for arg in args {
                walk_apps(arg, doc, range_start, range_end, hints);
            }
            return;
        }
        recurse_expr(expr, |e| walk_apps(e, doc, range_start, range_end, hints));
    }

    fn walk_decl(
        decl: &ast::Decl,
        doc: &DocumentState,
        range_start: usize,
        range_end: usize,
        hints: &mut Vec<InlayHint>,
    ) {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                walk_apps(body, doc, range_start, range_end, hints);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk_apps(body, doc, range_start, range_end, hints);
                    }
                }
            }
            _ => {}
        }
    }

    for decl in &doc.module.decls {
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }
        walk_decl(decl, doc, range_start, range_end, hints);
    }
}

/// Emit one parameter-name hint per positional argument when the callee's
/// names are known.
fn emit_arg_hints(
    doc: &DocumentState,
    func_name: &str,
    args: &[&ast::Expr],
    range_start: usize,
    range_end: usize,
    hints: &mut Vec<InlayHint>,
) {
    let param_names = extract_param_names(&doc.module, func_name);
    if param_names.is_empty() {
        return;
    }
    // Limit to single-arg calls being silent (no value), and skip hints when
    // the call is a postfix pipe (`x |> f`) — those are handled syntactically.
    // Also skip when arity ≤ 1, since a single argument's role is unambiguous.
    if param_names.len() <= 1 || args.len() <= 1 {
        return;
    }
    for (i, arg) in args.iter().enumerate() {
        let name = match param_names.get(i) {
            Some(n) => n,
            None => break,
        };
        // Suppress hint for bare-name args that already match the parameter
        // name — `transfer(amount, from, to)` doesn't need `amount: amount`.
        if let ast::ExprKind::Var(arg_name) = &arg.node {
            if arg_name == name {
                continue;
            }
        }
        // Don't hint trivial/anonymous parameter names (`_`, single letters
        // synthesized by the fallback). Single-letter ASCII params from real
        // code (`\x -> ...`) are kept — the hint is still useful there.
        if name == "_" {
            continue;
        }
        // Only hint for arguments visible in the requested range.
        if arg.span.end < range_start || arg.span.start > range_end {
            continue;
        }
        hints.push(InlayHint {
            position: offset_to_position(&doc.source, arg.span.start),
            label: InlayHintLabel::String(format!("{name}:")),
            kind: Some(InlayHintKind::PARAMETER),
            text_edits: None,
            tooltip: function_param_tooltip(doc, func_name, i, name),
            padding_left: None,
            padding_right: Some(true),
            data: None,
        });
    }
}

/// Build a tooltip with the parameter's type and a snippet of the function's
/// signature. Falls back to `None` if no signature is known.
fn function_param_tooltip(
    doc: &DocumentState,
    func_name: &str,
    index: usize,
    param_name: &str,
) -> Option<InlayHintTooltip> {
    let ty = doc.type_info.get(func_name)?;
    let params = parse_function_params(ty);
    let param_ty = params.get(index)?;
    Some(InlayHintTooltip::String(format!(
        "{param_name} : {param_ty}\n\n`{func_name} : {ty}`"
    )))
}

/// Walk the AST collecting `do` block spans whose monad has been resolved.
/// Emit a leading hint at the block's `do` keyword describing the kind.
fn add_monad_context_hints(
    doc: &DocumentState,
    range_start: usize,
    range_end: usize,
    hints: &mut Vec<InlayHint>,
) {
    fn walk(
        expr: &ast::Expr,
        doc: &DocumentState,
        range_start: usize,
        range_end: usize,
        hints: &mut Vec<InlayHint>,
    ) {
        if let ast::ExprKind::Do(_) = &expr.node {
            if expr.span.start >= range_start && expr.span.start <= range_end {
                if let Some(monad) = doc.monad_info.get(&expr.span) {
                    let label = match monad {
                        MonadKind::Relation => "[Relation]".to_string(),
                        MonadKind::IO => "[IO]".to_string(),
                        MonadKind::Adt(name) => format!("[{name}]"),
                    };
                    let pos = offset_to_position(&doc.source, expr.span.start);
                    // Anchor the hint just past the `do` keyword. We trust the
                    // span starts at `do` — emit at start, then let the editor
                    // render with padding_right.
                    let do_end = expr.span.start + 2; // length of "do"
                    let do_pos = if do_end <= doc.source.len() {
                        offset_to_position(&doc.source, do_end)
                    } else {
                        pos
                    };
                    hints.push(InlayHint {
                        position: do_pos,
                        label: InlayHintLabel::String(label),
                        kind: None,
                        text_edits: None,
                        tooltip: Some(InlayHintTooltip::String(monad_tooltip(monad))),
                        padding_left: Some(true),
                        padding_right: Some(true),
                        data: None,
                    });
                }
            }
        }
        recurse_expr(expr, |e| walk(e, doc, range_start, range_end, hints));
    }

    fn walk_decl(
        decl: &ast::Decl,
        doc: &DocumentState,
        range_start: usize,
        range_end: usize,
        hints: &mut Vec<InlayHint>,
    ) {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                walk(body, doc, range_start, range_end, hints);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk(body, doc, range_start, range_end, hints);
                    }
                }
            }
            _ => {}
        }
    }

    for decl in &doc.module.decls {
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }
        walk_decl(decl, doc, range_start, range_end, hints);
    }
}

/// Walk the AST collecting App-chain head positions whose callee resolves to
/// a function with declared trait constraints. Emits a small `[Trait a, …]`
/// hint immediately after the callee name so the user sees what trait
/// dispatch the call is bringing in.
fn add_constraint_hints(
    doc: &DocumentState,
    range_start: usize,
    range_end: usize,
    hints: &mut Vec<InlayHint>,
) {
    use crate::shared::flatten_app_chain;
    use crate::type_format::format_type_kind;

    fn walk(
        expr: &ast::Expr,
        doc: &DocumentState,
        range_start: usize,
        range_end: usize,
        hints: &mut Vec<InlayHint>,
    ) {
        if matches!(expr.node, ast::ExprKind::App { .. }) {
            let (callee, args) = flatten_app_chain(expr);
            if let ast::ExprKind::Var(name) = &callee.node {
                if callee.span.start >= range_start && callee.span.end <= range_end {
                    if let Some(constraints) = constraints_for_callee(&doc.module, name) {
                        if !constraints.is_empty() {
                            let label = format!("[{}]", constraints.join(", "));
                            hints.push(InlayHint {
                                position: offset_to_position(&doc.source, callee.span.end),
                                label: InlayHintLabel::String(label),
                                kind: None,
                                text_edits: None,
                                tooltip: Some(InlayHintTooltip::String(format!(
                                    "Call site brings in trait constraints from `{name}`'s declaration"
                                ))),
                                padding_left: Some(true),
                                padding_right: None,
                                data: None,
                            });
                        }
                    }
                }
            }
            for arg in args {
                walk(arg, doc, range_start, range_end, hints);
            }
            return;
        }
        recurse_expr(expr, |e| walk(e, doc, range_start, range_end, hints));
    }

    fn constraints_for_callee(module: &knot::ast::Module, name: &str) -> Option<Vec<String>> {
        for decl in &module.decls {
            match &decl.node {
                DeclKind::Fun {
                    name: n,
                    ty: Some(scheme),
                    ..
                } if n == name => {
                    let cs: Vec<String> = scheme
                        .constraints
                        .iter()
                        .map(|c| {
                            let args: Vec<String> = c
                                .args
                                .iter()
                                .map(|t| format_type_kind(&t.node))
                                .collect();
                            format!("{} {}", c.trait_name, args.join(" "))
                        })
                        .collect();
                    return Some(cs);
                }
                DeclKind::Trait { items, .. } => {
                    for item in items {
                        if let ast::TraitItem::Method {
                            name: n, ty, ..
                        } = item
                        {
                            if n == name {
                                let cs: Vec<String> = ty
                                    .constraints
                                    .iter()
                                    .map(|c| {
                                        let args: Vec<String> = c
                                            .args
                                            .iter()
                                            .map(|t| format_type_kind(&t.node))
                                            .collect();
                                        format!("{} {}", c.trait_name, args.join(" "))
                                    })
                                    .collect();
                                return Some(cs);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn walk_decl(
        decl: &ast::Decl,
        doc: &DocumentState,
        range_start: usize,
        range_end: usize,
        hints: &mut Vec<InlayHint>,
    ) {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                walk(body, doc, range_start, range_end, hints);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk(body, doc, range_start, range_end, hints);
                    }
                }
            }
            _ => {}
        }
    }

    for decl in &doc.module.decls {
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }
        walk_decl(decl, doc, range_start, range_end, hints);
    }
}

fn monad_tooltip(monad: &MonadKind) -> String {
    match monad {
        MonadKind::Relation => {
            "Relation comprehension. `<-` iterates rows, `where` filters, \
             `yield` collects, `groupBy` aggregates."
                .into()
        }
        MonadKind::IO => "IO action sequencing. Each statement is an effectful \
                          action; the final yield/expression is the result."
            .into(),
        MonadKind::Adt(name) => {
            format!("`{name}` monad. Bind dispatches via the `Monad {name}` impl.")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestWorkspace;

    fn hint_params(uri: &Uri, range: Range) -> InlayHintParams {
        InlayHintParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            range,
            work_done_progress_params: Default::default(),
        }
    }

    #[test]
    fn inlay_hint_shows_inferred_type_for_unannotated_fun() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "id = \\x -> x\n");
        let range = ws.whole_file_range(&uri);
        let hints = handle_inlay_hint(&ws.state, &hint_params(&uri, range)).unwrap_or_default();
        let labels: Vec<String> = hints
            .iter()
            .map(|h| match &h.label {
                InlayHintLabel::String(s) => s.clone(),
                _ => String::new(),
            })
            .collect();
        // Expect at least one type-annotation hint (": Type").
        assert!(
            labels.iter().any(|l| l.starts_with(":")),
            "expected `:T` hint; got: {labels:?}"
        );
    }

    #[test]
    fn inlay_hint_emits_per_field_types_for_record_destructure() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"data Person = Person {name: Text, age: Int}

show1 = \p -> case p of
  Person {name, age} -> name
"#,
        );
        let range = ws.whole_file_range(&uri);
        let hints = handle_inlay_hint(&ws.state, &hint_params(&uri, range)).unwrap_or_default();
        let labels: Vec<String> = hints
            .iter()
            .map(|h| match &h.label {
                InlayHintLabel::String(s) => s.clone(),
                _ => String::new(),
            })
            .collect();
        // Expect per-field hints for `name` and `age` derived from the parent
        // record's type. They render as `: Text` / `: Int`.
        assert!(
            labels.iter().any(|l| l == ": Text"),
            "expected `: Text` hint for destructured `name`; got: {labels:?}"
        );
        assert!(
            labels.iter().any(|l| l == ": Int"),
            "expected `: Int` hint for destructured `age`; got: {labels:?}"
        );
    }

    #[test]
    fn inlay_hint_emits_monad_context_for_maybe_do_block() {
        // Maybe is desugared (has Monad/Applicative/Alternative impls), so the
        // inferencer populates monad_info for its do blocks. IO and pure
        // sequential do blocks aren't desugared, so they don't get monad_info
        // entries today — the inlay hint correctly hides itself in that case.
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"safe = \x -> do
  v <- Just {value: x}
  yield v.value
"#,
        );
        let range = ws.whole_file_range(&uri);
        let hints = handle_inlay_hint(&ws.state, &hint_params(&uri, range)).unwrap_or_default();
        let labels: Vec<String> = hints
            .iter()
            .map(|h| match &h.label {
                InlayHintLabel::String(s) => s.clone(),
                _ => String::new(),
            })
            .collect();
        // Monad-kind hint shows up as `[Maybe]` or similar.
        let has_monad_hint = labels
            .iter()
            .any(|l| l.starts_with('[') && l.ends_with(']') && !l.contains(':'));
        assert!(
            has_monad_hint,
            "expected `[Monad]` hint; got: {labels:?}"
        );
    }
}
