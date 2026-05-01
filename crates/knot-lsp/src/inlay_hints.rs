//! `textDocument/inlayHint` handler. Surfaces inferred types, effects, and
//! unit annotations as inline-decoration hints.

use lsp_types::*;

use knot::ast::{self, DeclKind, Span};

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
                    hints.push(InlayHint {
                        position: hint_pos,
                        label: InlayHintLabel::String(format!(": {inferred}")),
                        kind: Some(InlayHintKind::TYPE),
                        text_edits: Some(vec![TextEdit {
                            range: Range { start: hint_pos, end: hint_pos },
                            new_text: format!("{name} : {inferred}\n"),
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
                    hints.push(InlayHint {
                        position: hint_pos,
                        label: InlayHintLabel::String(format!(": {inferred}")),
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

    // Show inferred types for local bindings (let/bind in do blocks)
    for (span, ty) in &doc.local_type_info {
        if span.end < range_start || span.start > range_end {
            continue;
        }
        let hint_pos = offset_to_position(&doc.source, span.end);
        let unit_tooltip = extract_unit_from_type_str(ty)
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

    Some(hints)
}

/// Extract the unit annotation `<...>` from a formatted type string.
/// Returns the unit text without the angle brackets, or `None` if the type
/// has no unit annotation. Skips trivial dimensionless `<1>` annotations.
fn extract_unit_from_type_str(ty: &str) -> Option<String> {
    // Find the first `<` that follows `Int` or `Float`. Bail if there's no
    // such pattern; that's how non-unit types like `Maybe<T>` are excluded.
    let lt = ty.find('<')?;
    let prefix = ty[..lt].trim_end();
    if !prefix.ends_with("Int") && !prefix.ends_with("Float") {
        return None;
    }
    // Find the matching `>` honoring nesting. Units are flat (no nesting in
    // practice) but compose like `M*S^2`; tracking depth keeps us safe if
    // someone constructs a parenthesized unit later.
    let mut depth = 0i32;
    let bytes = ty.as_bytes();
    let mut close = None;
    for (i, &b) in bytes[lt..].iter().enumerate() {
        match b {
            b'<' => depth += 1,
            b'>' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(lt + i);
                    break;
                }
            }
            _ => {}
        }
    }
    let close = close?;
    let inner = ty[lt + 1..close].trim();
    if inner.is_empty() || inner == "1" {
        return None;
    }
    Some(inner.to_string())
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
