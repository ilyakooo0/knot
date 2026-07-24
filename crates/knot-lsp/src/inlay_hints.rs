//! `textDocument/inlayHint` handler. Surfaces inferred types, effects,
//! parameter names, monad context, and unit annotations as inline hints.

use lsp_types::*;

use knot::ast::{self, Span};
use knot_compiler::infer::MonadKind;

use crate::shared::{extract_param_names, flatten_app_chain, parse_function_params};
use crate::state::{DocumentState, ServerState};
use crate::utils::{
    offset_to_position, position_to_offset, recurse_expr, safe_slice, top_fields,
};

// ── Inlay Hints ─────────────────────────────────────────────────────

pub(crate) fn handle_inlay_hint(
    state: &ServerState,
    params: &InlayHintParams,
) -> Option<Vec<InlayHint>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    // Staleness guard: during the analysis debounce window the editor buffer
    // is newer than the analyzed source, so range positions from the live
    // buffer would resolve against the wrong bytes — hints would land on the
    // wrong tokens. Bail; the client re-requests once analysis catches up.
    if state
        .pending_sources
        .get(&params.text_document.uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let mut hints = Vec::new();

    let range_start = position_to_offset(&doc.source, params.range.start);
    let range_end = position_to_offset(&doc.source, params.range.end);

    // Config gating. Two user-facing knobs cover all hint categories:
    //   `inlayTypes` — everything that surfaces inferred TYPE-ish info:
    //     decl signature hints, local-binding type hints, record-pattern
    //     field types, unit-literal hints, effect rows, monad context,
    //     and trait-constraint hints (effects/monads/constraints are part
    //     of a decl's type in Knot, so this is the closest flag).
    //   `inlayParameterNames` — call-site parameter-name hints, plus the
    //     closing-block labels (both are "reading aid" annotations rather
    //     than type info; parameter names is the closest flag).
    // The dirty-decl telemetry stays gated on KNOT_LSP_TRACE_DIRTY only.
    let show_types = state.config.inlay_types;
    let show_param_names = state.config.inlay_parameter_names;

    // Show inferred types for unannotated function declarations.
    // For annotated functions, show only the inferred *effects* if they exist
    // and aren't already in the type signature.
    //
    // `decls` is in source order (parser pushes them sequentially) so once the
    // start exceeds the visible range we can stop — the linear scan is bounded
    // by the visible-region size, not by the file's total decl count.
    for decl in top_fields(&doc.module) {
        if !show_types {
            break;
        }
        let dspan = decl.value.span;
        if dspan.start > range_end {
            break;
        }
        if dspan.end < range_start {
            continue;
        }

        // Classify the field: marker vs named-function, and whether it has a sig.
        let (fname, fsig, is_relation_marker) = match &decl.value.node {
            ast::ExprKind::ViewDecl { name, ty, .. } | ast::ExprKind::DerivedDecl { name, ty, .. } => {
                (name.as_str(), ty.as_ref(), true)
            }
            ast::ExprKind::SourceDecl { .. } | ast::ExprKind::DataCtor { .. }
            | ast::ExprKind::TypeCtor { .. } | ast::ExprKind::RouteDecl { .. }
            | ast::ExprKind::RouteCompositeDecl { .. } | ast::ExprKind::SubsetConstraint { .. } => continue,
            _ => (decl.name.as_str(), decl.sig.as_ref(), false),
        };
        match (fname, fsig, is_relation_marker) {
            (name, None, marker) => {
                if let Some(inferred) = doc.type_info.get(name) {
                    let decl_text = safe_slice(&doc.source, dspan);
                    // View/derived markers begin with a `*`/`&` sigil that
                    // `dspan.start` points at; skip it before scanning for the
                    // end of the name so the hint doesn't land on the sigil.
                    let sigil_len = if marker {
                        decl_text
                            .chars()
                            .next()
                            .filter(|c| *c == '*' || *c == '&')
                            .map_or(0, char::len_utf8)
                    } else {
                        0
                    };
                    // `'` continues identifiers in the lexer (`x'` is one
                    // token), so the hint anchor must skip it too — otherwise
                    // `x' = 1` renders as `x : Int' = 1`.
                    let name_end = decl_text[sigil_len..]
                        .find(|c: char| !c.is_alphanumeric() && c != '_' && c != '\'')
                        .map(|p| sigil_len + p)
                        .unwrap_or(decl_text.len());
                    let hint_offset = dspan.start + name_end;
                    let hint_pos = offset_to_position(&doc.source, hint_offset);
                    // Text edit emits the signature as a separate statement above the
                    // function, so anchor it at the declaration start, not at the hint.
                    let edit_pos = offset_to_position(&doc.source, dspan.start);
                    // Merge per-decl effect-checker findings into the IO row of
                    // the rendered type, in case HM inference dropped them.
                    let full_sig = match doc.effect_sets.get(name) {
                        Some(eff) => crate::shared::render_signature_with_effects(inferred, eff),
                        None => inferred.clone(),
                    };
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
            (name, Some(scheme), false) => {
                // Annotated function: show the inferred *effects* as a hint at
                // the function body's start, only when the type doesn't already
                // declare them. Helps with effect-row polymorphism debugging.
                if let Some(effects) = doc.effect_info.get(name) {
                    let inferred_ty = doc.type_info.get(name);
                    let needs_hint = inferred_ty
                        .map(|ty| !type_str_mentions_effects(ty, effects))
                        .unwrap_or(true);
                    if needs_hint {
                        // Anchor at the END of the signature line. Anchoring
                        // right after the name would visually split `name`
                        // and `:` on annotated declarations. Use the end of the
                        // *type signature* line, not the first `\n` in the whole
                        // declaration — on a multi-line signature the latter
                        // lands mid-type, where the `--` hint reads as commenting
                        // out the continuation.
                        let span_end = dspan.end.min(doc.source.len());
                        let sig_end = scheme.ty.span.end.min(span_end);
                        // `sig_end`/`span_end` are clamped to `len` but a stale
                        // or mid-token span endpoint can land mid-multibyte-char;
                        // a direct slice would panic, so use `get` and fall back.
                        let hint_offset = doc.source
                            .get(sig_end..span_end)
                            .and_then(|s| s.find('\n').map(|p| sig_end + p))
                            .unwrap_or(span_end);
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
            _ => {}
        }
    }

    // Show inferred types for local bindings (let/bind in do blocks). Reads
    // `unit_info` (populated during analysis) instead of re-parsing each type
    // string per request — this makes the hint cheap when the file has many
    // unit-annotated bindings.
    //
    // `local_type_info_sorted` is the analysis output's `local_type_info`
    // sorted by `span.start`. Binary-search for the first entry whose start
    // exceeds the visible range and clip — bindings past that point are
    // certainly off-screen, so we avoid scanning the whole map on every
    // cursor move. Bindings *before* the visible range may still overlap
    // (an outer let around the cursor's expression starts well before its
    // end), so the lower bound is enforced per-iteration with the existing
    // `span.end < range_start` check.
    let upper = if show_types {
        doc.local_type_info_sorted
            .partition_point(|(s, _)| s.start <= range_end)
    } else {
        0
    };
    for (span, ty) in &doc.local_type_info_sorted[..upper] {
        if span.end < range_start {
            continue;
        }
        // Only annotate simple identifier bindings. A punned record sub-pattern
        // (`case p of Pt {x, y} -> …`) records its whole `{x, y}` span in the
        // type-info table (hover uses it, but there's no per-field span); a
        // `: Int` rendered after the `}` is misplaced, so skip spans whose
        // source slice isn't a bare identifier.
        if !is_bare_identifier(&doc.source, span.start, span.end) {
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

    if show_types {
        // Show inferred unit hints on numeric literals whose enclosing binding has
        // a unit-annotated type. The literals themselves don't carry explicit unit
        // syntax, so the user otherwise has to mentally trace the type — the hint
        // shows e.g. `<M>` after `42` in `let distance : Float M = 42.0`.
        add_unit_literal_hints(doc, range_start, range_end, &mut hints);
    }

    if show_param_names {
        // Show parameter-name hints at named function call sites. The hint shows
        // `name:` before each argument so multi-arg calls don't require jumping to
        // the definition to know which argument is which.
        add_parameter_name_hints(doc, range_start, range_end, &mut hints);
    }

    if show_types {
        // Show the resolved monad kind at the start of each `do` block. Helps when
        // the same `do` syntax can desugar to `[]`, `Maybe`, `Result`, or `IO`
        // depending on context.
        add_monad_context_hints(doc, range_start, range_end, &mut hints);

        // Show per-field type hints for record-destructure patterns in case arms,
        // do-binds, and lambda params. The whole-pattern hint (above) shows the
        // record type; this loop adds `: T` after each individual field name so
        // users can see the field types without expanding mentally.
        add_record_pattern_field_hints(doc, range_start, range_end, &mut hints);
    }

    if show_param_names {
        // Closing-label hints — for blocks that span many lines, show a hint at the
        // closing token indicating what's ending. Helps when the opener is far
        // off-screen.
        add_closing_label_hints(doc, range_start, range_end, &mut hints);
    }

    if show_types {
        // Trait-constraint hints at call sites of constrained functions. The
        // inferencer doesn't memoize per-call-site substitutions, so we surface
        // the *declared* constraints — useful for spotting "this call brings in
        // an Eq/Ord/Display requirement" without jumping to the definition.
        add_constraint_hints(doc, range_start, range_end, &mut hints);
    }

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

/// True when `source[start..end]` is a single bare identifier (an alphabetic
/// or `_` first char, then identifier chars / `'`). Used to keep inlay type
/// hints on simple variable bindings and off destructuring sub-patterns whose
/// recorded span covers punctuation (`{x, y}`).
fn is_bare_identifier(source: &str, start: usize, end: usize) -> bool {
    let slice = match source.get(start..end) {
        Some(s) if !s.is_empty() => s,
        _ => return false,
    };
    let mut chars = slice.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '\'')
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
    for decl in top_fields(&doc.module) {
        if decl.value.span.end < range_start || decl.value.span.start > range_end {
            continue;
        }
        let name = match &decl.value.node {
            ast::ExprKind::SubsetConstraint { .. } => continue,
            _ => decl.name.clone(),
        };
        if !doc.dirty_decl_closure.contains(&name) {
            continue;
        }
        hints.push(InlayHint {
            position: offset_to_position(&doc.source, decl.value.span.start),
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
        // Guard `start <= end` too: an inverted span would make the slice
        // index panic (`slice index starts at N but ends at M`).
        if span.start <= span.end && span.end <= source.len() {
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
    for decl in top_fields(&doc.module) {
        match &decl.value.node {
            ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => {
                collect(body, &doc.source, &mut spans)
            }
            ast::ExprKind::SourceDecl { .. } | ast::ExprKind::DataCtor { .. }
            | ast::ExprKind::TypeCtor { .. } | ast::ExprKind::RouteDecl { .. }
            | ast::ExprKind::RouteCompositeDecl { .. } | ast::ExprKind::SubsetConstraint { .. } => {}
            _ => collect(&decl.value, &doc.source, &mut spans),
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
    /// Field-name token positions parsed structurally from a record pattern.
    /// The AST stores field names as bare strings, so each token is recovered
    /// from source text — but confined to its syntactic slot: the window
    /// between the previous field's sub-pattern (or the record opener) and
    /// this field's sub-pattern. A whole-pattern first-occurrence search
    /// would anchor field `b`'s hint on the BINDER `b` of an earlier field
    /// (`P {a: b, b: c}`).
    fn record_field_name_spans(
        fields: &[ast::FieldPat],
        window: Span,
        source: &str,
    ) -> Vec<(String, Span)> {
        let mut out = Vec::new();
        let mut search_start = window.start;
        for f in fields {
            match &f.pattern {
                Some(p) => {
                    if let Some(s) = crate::utils::find_word_in_source(
                        source,
                        &f.name,
                        search_start,
                        p.span.start,
                    ) {
                        out.push((f.name.clone(), s));
                    }
                    search_start = p.span.end;
                }
                None => {
                    // Pun: the token both names the field and binds the var.
                    if let Some(s) = crate::utils::find_word_in_source(
                        source,
                        &f.name,
                        search_start,
                        window.end,
                    ) {
                        out.push((f.name.clone(), s));
                        search_start = s.end;
                    }
                }
            }
        }
        out
    }

    /// A record-destructuring pattern: its span, an optional constructor
    /// name (for ADT cases like `Person {name}`), and the structurally-parsed
    /// field-name token spans.
    type RecordPat = (Span, Option<String>, Vec<(String, Span)>);

    /// Find each pattern that destructures a record. Tracks the span, an
    /// optional constructor name for ADT cases like `Person {name}`, and the
    /// structurally-parsed field-name token spans.
    fn walk_pat_for_records(pat: &ast::Pat, source: &str, out: &mut Vec<RecordPat>) {
        match &pat.node {
            ast::PatKind::Record(fields) => {
                out.push((
                    pat.span,
                    None,
                    record_field_name_spans(fields, pat.span, source),
                ));
                // Recurse into field sub-patterns so nested record
                // destructures (`{addr: {city}}`) get hints too.
                for f in fields {
                    if let Some(p) = &f.pattern {
                        walk_pat_for_records(p, source, out);
                    }
                }
            }
            ast::PatKind::Constructor { name, payload, .. } => {
                // A constructor-record pattern is collected ONCE, as the
                // constructor entry; recursing into the payload Record would
                // push the same record again and duplicate every per-field
                // hint. Only the payload's field SUB-patterns are recursed.
                if let ast::PatKind::Record(fields) = &payload.node {
                    out.push((
                        pat.span,
                        Some(name.clone()),
                        record_field_name_spans(fields, payload.span, source),
                    ));
                    for f in fields {
                        if let Some(p) = &f.pattern {
                            walk_pat_for_records(p, source, out);
                        }
                    }
                } else {
                    walk_pat_for_records(payload, source, out);
                }
            }
            ast::PatKind::List(pats) => {
                for p in pats {
                    walk_pat_for_records(p, source, out);
                }
            }
            ast::PatKind::Cons { head, tail } => {
                walk_pat_for_records(head, source, out);
                walk_pat_for_records(tail, source, out);
            }
            _ => {}
        }
    }
    fn walk_expr(expr: &ast::Expr, source: &str, out: &mut Vec<RecordPat>) {
        match &expr.node {
            ast::ExprKind::Lambda { params, body, .. } => {
                for p in params {
                    walk_pat_for_records(p, source, out);
                }
                walk_expr(body, source, out);
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                walk_expr(scrutinee, source, out);
                for arm in arms {
                    walk_pat_for_records(&arm.pat, source, out);
                    walk_expr(&arm.body, source, out);
                }
            }
            ast::ExprKind::Do(stmts) => {
                for stmt in stmts {
                    match &stmt.node {
                        ast::StmtKind::Bind { pat, expr } => {
                            walk_pat_for_records(pat, source, out);
                            walk_expr(expr, source, out);
                        }
                        ast::StmtKind::Where { cond } | ast::StmtKind::Expr(cond) => {
                            walk_expr(cond, source, out);
                        }
                        ast::StmtKind::GroupBy { key } => walk_expr(key, source, out),
                    }
                }
            }
            _ => recurse_expr(expr, |e| walk_expr(e, source, out)),
        }
    }

    let mut record_pats: Vec<RecordPat> = Vec::new();
    for decl in top_fields(&doc.module) {
        match &decl.value.node {
            ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => {
                walk_expr(body, &doc.source, &mut record_pats)
            }
            ast::ExprKind::SourceDecl { .. } | ast::ExprKind::DataCtor { .. }
            | ast::ExprKind::TypeCtor { .. } | ast::ExprKind::RouteDecl { .. }
            | ast::ExprKind::RouteCompositeDecl { .. } | ast::ExprKind::SubsetConstraint { .. } => {}
            _ => walk_expr(&decl.value, &doc.source, &mut record_pats),
        }
    }

    for (span, ctor_opt, field_name_spans) in record_pats {
        if span.end < range_start || span.start > range_end {
            continue;
        }
        // Resolve the field set. Prefer the AST-driven constructor lookup —
        // the inferencer's local_type_info often doesn't carry an entry at
        // the constructor pattern's outer span, but the data decl always
        // does. Take the FIRST data type declaring a constructor with this
        // name and stop — scanning on would accumulate fields from every ADT
        // that happens to share the constructor name.
        let mut fields_str: Vec<(String, String)> = Vec::new();
        let mut tooltip_source = String::from("destructured record");
        if let Some(ctor_name) = ctor_opt.as_deref() {
            'ctor_lookup: for d in top_fields(&doc.module) {
                if let ast::ExprKind::DataCtor { constructors, name: data_name, .. } = &d.value.node {
                    for c in constructors {
                        if c.name == ctor_name {
                            tooltip_source = format!("{ctor_name} (constructor of {data_name})");
                            for f in &c.fields {
                                fields_str.push((
                                    f.name.clone(),
                                    crate::type_format::format_type_kind(&f.value.node),
                                ));
                            }
                            break 'ctor_lookup;
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
        for (field_name, ty_str) in fields_str {
            // Anchor each hint on the field-NAME token position parsed
            // structurally from the pattern — not on the first same-named
            // token, which could be an earlier field's binder.
            if let Some((_, name_span)) = field_name_spans
                .iter()
                .find(|(n, _)| *n == field_name)
            {
                let abs_end = name_span.end;
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
        && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] == b'\'')
    {
        j += 1;
    }
    let ctor_name = pat_text.get(i..j)?;
    for (name, payload) in ctors {
        if name == ctor_name
            && let Some(p) = payload
                && let Some(fields) = p.record_fields() {
                    return Some(fields.to_vec());
                }
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

/// Walk every binding-with-unit and emit a hint on the binding's literal.
///
/// Attribution is deliberately conservative: the hint fires ONLY when the
/// binding's RHS is exactly one bare numeric literal (`let d = 42.0` with an
/// inferred `Float M`). Anything compound is skipped, because the binding's
/// unit doesn't necessarily belong to each literal inside it:
/// - `base * 2.0` — the `2.0` is dimensionless (unit algebra composes via
///   `*`), so stamping the binding's `<M>` on it is wrong;
/// - `5 seconds` — the time-word sugar desugars to `5 * 1000` where the
///   synthesized `1000` literal's span covers the word `seconds`, so the
///   old walk hinted `<Ms>` after the word.
///
/// When in doubt, no hint.
fn add_unit_literal_hints(
    doc: &DocumentState,
    range_start: usize,
    range_end: usize,
    hints: &mut Vec<InlayHint>,
) {
    /// The RHS's span iff it is a single bare numeric literal (no explicit
    /// unit annotation, no surrounding expression).
    fn bare_literal_span(expr: &ast::Expr) -> Option<Span> {
        match &expr.node {
            ast::ExprKind::Lit(ast::Literal::Int(_))
            | ast::ExprKind::Lit(ast::Literal::Float(_)) => Some(expr.span),
            _ => None,
        }
    }

    fn collect_literals_in_decl(decl: &ast::RecordField, out: &mut Vec<(Span, ast::Expr)>) {
        match &decl.value.node {
            ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => {
                walk_for_unit_bindings(body, out);
            }
            ast::ExprKind::SourceDecl { .. } | ast::ExprKind::DataCtor { .. }
            | ast::ExprKind::TypeCtor { .. } | ast::ExprKind::RouteDecl { .. }
            | ast::ExprKind::RouteCompositeDecl { .. } | ast::ExprKind::SubsetConstraint { .. } => {}
            _ => {
                walk_for_unit_bindings(&decl.value, out);
            }
        }
    }

    fn walk_for_unit_bindings(expr: &ast::Expr, out: &mut Vec<(Span, ast::Expr)>) {
        // Handle Do blocks entirely here and return — falling through to
        // `recurse_expr` afterwards would visit every binding RHS a second
        // time (its Do arm also yields Bind/Let RHS), duplicating unit hints
        // (and multiplying them with nesting).
        if let ast::ExprKind::Do(stmts) = &expr.node {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, expr: rhs } => {
                        out.push((pat.span, rhs.clone()));
                        walk_for_unit_bindings(rhs, out);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        walk_for_unit_bindings(e, out);
                    }
                    ast::StmtKind::GroupBy { key } => walk_for_unit_bindings(key, out),
                }
            }
            return;
        }
        recurse_expr(expr, |e| walk_for_unit_bindings(e, out));
    }

    let mut bindings_with_rhs: Vec<(Span, ast::Expr)> = Vec::new();
    for decl in top_fields(&doc.module) {
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
        let span = match bare_literal_span(&rhs) {
            Some(s) => s,
            None => continue,
        };
        if span.end < range_start || span.start > range_end {
            continue;
        }
        // Belt-and-suspenders: only hint when the span's source text really
        // is a numeric literal, and the source doesn't already spell a unit
        // (`<…>`) or a time word right after it — synthesized/desugared
        // literals carry spans pointing at non-numeric tokens.
        let text = safe_slice(&doc.source, span);
        let is_numeric = !text.is_empty()
            && text
                .chars()
                .all(|c| c.is_ascii_digit() || c == '.' || c == '_');
        if !is_numeric {
            continue;
        }
        if doc
            .source
            .get(span.end.min(doc.source.len())..)
            .unwrap_or("")
            .trim_start()
            .starts_with('<')
        {
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

/// Heuristic: does the rendered type string already mention all of the given
/// effects? Used to suppress redundant effect inlay hints.
fn type_str_mentions_effects(ty: &str, effects: &str) -> bool {
    // The effects string looks like `{console, r *foo}` — pull the inner
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
        shadowed: &std::collections::HashSet<String>,
        range_start: usize,
        range_end: usize,
        hints: &mut Vec<InlayHint>,
    ) {
        // When we hit an App chain, flatten it and emit hints for the whole
        // chain, then recurse into the head and the args. The head is non-App
        // (flatten goes to the bottom of the chain), so recursing into it
        // doesn't re-process inner Apps — but it does reach hints inside
        // non-Var heads like `(if c then f else g) a b` or lambda/case heads.
        if matches!(expr.node, ast::ExprKind::App { .. }) {
            let (callee, args) = flatten_app_chain(expr);
            if let ast::ExprKind::Var(name) = &callee.node {
                // Param names are resolved by NAME against top-level decls.
                // When a local binder in this declaration shadows that name
                // (`\add v -> add v 1`), the top-level decl's param names
                // don't apply — suppress conservatively.
                if !shadowed.contains(name.as_str()) {
                    emit_arg_hints(doc, name, &args, range_start, range_end, hints);
                }
            }
            walk_apps(callee, doc, shadowed, range_start, range_end, hints);
            for arg in args {
                walk_apps(arg, doc, shadowed, range_start, range_end, hints);
            }
            return;
        }
        recurse_expr(expr, |e| {
            walk_apps(e, doc, shadowed, range_start, range_end, hints)
        });
    }

    fn walk_decl(
        decl: &ast::RecordField,
        doc: &DocumentState,
        range_start: usize,
        range_end: usize,
        hints: &mut Vec<InlayHint>,
    ) {
        let body = match &decl.value.node {
            ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => body,
            ast::ExprKind::SourceDecl { .. } | ast::ExprKind::DataCtor { .. }
            | ast::ExprKind::TypeCtor { .. } | ast::ExprKind::RouteDecl { .. }
            | ast::ExprKind::RouteCompositeDecl { .. } | ast::ExprKind::SubsetConstraint { .. } => return,
            _ => &decl.value,
        };
        let mut shadowed = std::collections::HashSet::new();
        collect_binder_names(body, &mut shadowed);
        walk_apps(body, doc, &shadowed, range_start, range_end, hints);
    }

    for decl in top_fields(&doc.module) {
        if decl.value.span.end < range_start || decl.value.span.start > range_end {
            continue;
        }
        walk_decl(decl, doc, range_start, range_end, hints);
    }
}

/// Collect every name bound by a local binder (lambda params, case-arm
/// patterns, do-block bind/let patterns) anywhere inside `expr`. Used to
/// conservatively suppress parameter-name hints whose callee name is
/// shadowed somewhere in the declaration — name-based top-level resolution
/// can't tell which binding a shadowed call site refers to.
fn collect_binder_names(expr: &ast::Expr, out: &mut std::collections::HashSet<String>) {
    match &expr.node {
        ast::ExprKind::Lambda { params, .. } => {
            for p in params {
                collect_pat_binder_names(&p.node, out);
            }
        }
        ast::ExprKind::Case { arms, .. } => {
            for arm in arms {
                collect_pat_binder_names(&arm.pat.node, out);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                if let ast::StmtKind::Bind { pat, .. } = &stmt.node {
                    collect_pat_binder_names(&pat.node, out);
                }
            }
        }
        _ => {}
    }
    recurse_expr(expr, |e| collect_binder_names(e, out));
}

/// Names bound by a single pattern.
fn collect_pat_binder_names(pat: &ast::PatKind, out: &mut std::collections::HashSet<String>) {
    match pat {
        ast::PatKind::Var(name) => {
            out.insert(name.clone());
        }
        ast::PatKind::Record(fields) => {
            for f in fields {
                match &f.pattern {
                    Some(p) => collect_pat_binder_names(&p.node, out),
                    // Shorthand `{name}` binds the field name itself.
                    None => {
                        out.insert(f.name.clone());
                    }
                }
            }
        }
        ast::PatKind::Constructor { payload, .. } => {
            collect_pat_binder_names(&payload.node, out);
        }
        ast::PatKind::List(pats) => {
            for p in pats {
                collect_pat_binder_names(&p.node, out);
            }
        }
        ast::PatKind::Cons { head, tail } => {
            collect_pat_binder_names(&head.node, out);
            collect_pat_binder_names(&tail.node, out);
        }
        _ => {}
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
        if let ast::ExprKind::Var(arg_name) = &arg.node
            && arg_name == name {
                continue;
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

/// Find the byte offset of the `do` keyword within `[start, end)` of `source`.
///
/// A `Do` expression's span does not always begin at `do`: a parenthesized do
/// used as an argument (`f (do ...)`) keeps the inner `Do` node but widens its
/// span to include the surrounding parens, so `span.start` points at `(`.
/// Anchoring a hint at a fixed `span.start + 2` would therefore land
/// mid-keyword. This scans forward from `start` for the first standalone `do`
/// token — bounded by non-identifier characters so it never matches the tail of
/// an identifier like `weirdo` or a longer keyword-like word — and returns its
/// offset, or `None` if there is no such token before `end`.
fn find_do_keyword(source: &str, start: usize, end: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let end = end.min(bytes.len());
    // Identifier-continue chars per the lexer: ASCII alphanumerics, `_`, `'`.
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';
    let mut i = start;
    while i + 2 <= end {
        if bytes[i] == b'd' && bytes[i + 1] == b'o' {
            let prev_ok = i == 0 || !is_ident(bytes[i - 1]);
            let next_ok = i + 2 >= bytes.len() || !is_ident(bytes[i + 2]);
            if prev_ok && next_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
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
        if let ast::ExprKind::Do(_) = &expr.node
            && expr.span.start >= range_start && expr.span.start < range_end
                && let Some(monad) = doc.monad_info.get(&expr.span) {
                    let label = match monad {
                        MonadKind::Relation => "[Relation]".to_string(),
                        MonadKind::IO => "[IO]".to_string(),
                        MonadKind::Adt(name) => format!("[{name}]"),
                    };
                    let pos = offset_to_position(&doc.source, expr.span.start);
                    // Anchor the hint just past the `do` keyword. The span does
                    // NOT always begin at `do`: a parenthesized do used as an
                    // argument (`f (do ...)`) keeps the inner `Do` node but
                    // widens its span to include the surrounding parens, so
                    // `span.start` points at `(`. A blind `+ 2` would then land
                    // mid-keyword (between `d` and `o`). Scan forward from the
                    // span start for the actual `do` token and anchor after it;
                    // fall back to the span start if none is found.
                    let do_pos = match find_do_keyword(&doc.source, expr.span.start, expr.span.end)
                    {
                        Some(do_start) => offset_to_position(&doc.source, do_start + 2),
                        None => pos,
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
        recurse_expr(expr, |e| walk(e, doc, range_start, range_end, hints));
    }

    fn walk_decl(
        decl: &ast::RecordField,
        doc: &DocumentState,
        range_start: usize,
        range_end: usize,
        hints: &mut Vec<InlayHint>,
    ) {
        let body = match &decl.value.node {
            ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => body,
            ast::ExprKind::SourceDecl { .. } | ast::ExprKind::DataCtor { .. }
            | ast::ExprKind::TypeCtor { .. } | ast::ExprKind::RouteDecl { .. }
            | ast::ExprKind::RouteCompositeDecl { .. } | ast::ExprKind::SubsetConstraint { .. } => return,
            _ => &decl.value,
        };
        walk(body, doc, range_start, range_end, hints);
    }

    for decl in top_fields(&doc.module) {
        if decl.value.span.end < range_start || decl.value.span.start > range_end {
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
            if let ast::ExprKind::Var(name) = &callee.node
                && callee.span.start >= range_start && callee.span.end <= range_end
                    && let Some(constraints) = constraints_for_callee(&doc.module, name)
                        && !constraints.is_empty() {
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
            // Recurse into the head too — it can be a non-Var expression
            // (`(if c then f else g) a b`, lambda/case heads) containing
            // further call chains. The head is non-App, so no re-processing.
            walk(callee, doc, range_start, range_end, hints);
            for arg in args {
                walk(arg, doc, range_start, range_end, hints);
            }
            return;
        }
        recurse_expr(expr, |e| walk(e, doc, range_start, range_end, hints));
    }

    fn constraints_for_callee(program: &knot::ast::Expr, name: &str) -> Option<Vec<String>> {
        for decl in top_fields(program) {
            match (&decl.name, &decl.sig) {
                (n, Some(scheme)) if n == name => {
                    let cs: Vec<String> = scheme
                        .constraints
                        .iter()
                        .map(|c| match c {
                            knot::ast::Constraint::Trait { trait_name, args } => {
                                let args: Vec<String> = args
                                    .iter()
                                    .map(|t| format_type_kind(&t.node))
                                    .collect();
                                format!("{} {}", trait_name, args.join(" "))
                            }
                            knot::ast::Constraint::ImplicitField { field, ty } => {
                                format!("(^ {} : {})", field, format_type_kind(&ty.node))
                            }
                        })
                        .collect();
                    return Some(cs);
                }
                _ => {}
            }
        }
        None
    }

    fn walk_decl(
        decl: &ast::RecordField,
        doc: &DocumentState,
        range_start: usize,
        range_end: usize,
        hints: &mut Vec<InlayHint>,
    ) {
        let body = match &decl.value.node {
            ast::ExprKind::ViewDecl { body, .. } | ast::ExprKind::DerivedDecl { body, .. } => body,
            ast::ExprKind::SourceDecl { .. } | ast::ExprKind::DataCtor { .. }
            | ast::ExprKind::TypeCtor { .. } | ast::ExprKind::RouteDecl { .. }
            | ast::ExprKind::RouteCompositeDecl { .. } | ast::ExprKind::SubsetConstraint { .. } => return,
            _ => &decl.value,
        };
        walk(body, doc, range_start, range_end, hints);
    }

    for decl in top_fields(&doc.module) {
        if decl.value.span.end < range_start || decl.value.span.start > range_end {
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


