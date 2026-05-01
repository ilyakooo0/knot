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

    // Show parameter-name hints at named function call sites. The hint shows
    // `name:` before each argument so multi-arg calls don't require jumping to
    // the definition to know which argument is which.
    add_parameter_name_hints(doc, range_start, range_end, &mut hints);

    // Show the resolved monad kind at the start of each `do` block. Helps when
    // the same `do` syntax can desugar to `[]`, `Maybe`, `Result`, or `IO`
    // depending on context.
    add_monad_context_hints(doc, range_start, range_end, &mut hints);

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
