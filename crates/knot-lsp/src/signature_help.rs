//! `textDocument/signatureHelp` handler.

use lsp_types::*;

use knot::ast::Span;

use crate::shared::{
    extract_param_names, find_enclosing_application, parse_function_params,
};
use crate::state::{DocumentState, ServerState};
use crate::utils::position_to_offset;

/// UTF-16 code-unit length of a string — LSP measures label offsets in UTF-16.
fn utf16_len(s: &str) -> u32 {
    s.chars().map(|c| c.len_utf16() as u32).sum()
}

// ── Signature Help (paren-aware) ────────────────────────────────────

pub(crate) fn handle_signature_help(
    state: &ServerState,
    params: &SignatureHelpParams,
) -> Option<SignatureHelp> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    // Staleness guard (mirrors hover / inlay-hint): during the analysis
    // debounce window the editor buffer is newer than the analyzed source,
    // so the cursor position would resolve against the wrong bytes — the
    // wrong parameter would be highlighted. Bail; the client re-requests
    // once analysis catches up.
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let offset = position_to_offset(&doc.source, pos);

    // Strategy: find the innermost App chain in the AST that contains the cursor,
    // then determine which argument position the cursor is in.
    let (func_name, active_param) = find_enclosing_application(&doc.module, &doc.source, offset)?;

    // Look up the function type. Try the global type table first (covers
    // top-level decls, builtins, and trait/impl methods), then fall back to
    // the local-binding table (covers let-bound lambdas and do-block binds).
    // The fallback matches by name + by the binding span — any local binding
    // whose name matches and whose span lies before the call site is a
    // candidate.
    // An in-scope local binding shadows a same-named top-level decl, so it must
    // win over the global table. Bound the local search to the enclosing
    // declaration (a binding ending before the call site but in a *different*
    // top-level decl is out of scope and must not shadow). Only fall back to
    // the global table when no shadowing local is in scope.
    let enclosing = doc
        .module
        .decls
        .iter()
        .map(|d| d.span)
        .find(|s| s.start <= offset && offset <= s.end);
    let local = lookup_local_binding_type(doc, &func_name, offset, enclosing);
    let type_str_owned: String;
    let type_str: &str = if let Some(local) = local {
        type_str_owned = local;
        type_str_owned.as_str()
    } else if let Some(global) = doc.type_info.get(func_name.as_str()) {
        global.as_str()
    } else {
        return None;
    };

    // Parse arrow-separated parameters from the type string
    let param_types = parse_function_params(type_str);
    if param_types.is_empty() {
        return None;
    }

    // Try to extract parameter names from the function definition. Falls back
    // to a synthesized list (`a`, `b`, ...) when the function isn't a
    // top-level decl with an inferable param list (e.g. an inline lambda).
    //
    // A point-free / partially-eta-reduced definition (`addBoth = \x -> plus x`
    // for `addBoth : Int -> Int -> Int`) yields FEWER names than the type's
    // arity. The remaining slots must still get synthesized names, otherwise
    // they render as a bare `Int` label whose offsets collide with an earlier
    // identically-typed parameter — the editor would then highlight the wrong
    // parameter. So pad up to the full arity, picking synthesized letters that
    // don't clash with names already present.
    let arity = param_types.len().saturating_sub(1);
    let mut param_names = extract_param_names(&doc.module, &func_name);
    // Drop any names beyond the arity (an over-long lambda chain would
    // otherwise attach a name to the return-type slot).
    param_names.truncate(arity);
    if param_names.len() < arity {
        let mut existing: std::collections::HashSet<String> =
            param_names.iter().cloned().collect();
        for i in param_names.len()..arity {
            // Walk `a`, `b`, … `z`, `a1`, `b1`, … skipping any that collide.
            let mut k = i;
            let mut name = synth_param_name(k);
            while existing.contains(&name) {
                k += 26;
                name = synth_param_name(k);
            }
            // Record the synthesized name so a later slot can't reuse it.
            existing.insert(name.clone());
            param_names.push(name);
        }
    }

    // Build parameter labels: "name: Type" if we have a name, else just "Type"
    // The label must be a substring of the signature label so the editor can
    // highlight the active parameter.
    let signature_label = build_signature_label(&func_name, &param_types, &param_names, type_str);

    // Running byte cursor into `signature_label`: each parameter label is
    // searched *after* the previous one's match, so a param whose label is a
    // substring of an earlier one (e.g. `x: Int` inside `x: Int -> ...`)
    // highlights its own occurrence rather than the first textual hit.
    let mut search_from = 0usize;
    let param_infos: Vec<ParameterInformation> = param_types
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let name = param_names.get(i);
            let label_text = match name {
                Some(n) => format!("{n}: {ty}"),
                None => ty.clone(),
            };
            // Locate the label substring in the signature for highlighting.
            // `ParameterLabel::LabelOffsets` are UTF-16 code-unit offsets per
            // the LSP spec, NOT byte offsets — convert so labels containing
            // multibyte characters highlight the correct span.
            let label = match signature_label[search_from..].find(&label_text) {
                Some(rel) => {
                    let start_byte = search_from + rel;
                    let end_byte = start_byte + label_text.len();
                    search_from = end_byte;
                    ParameterLabel::LabelOffsets([
                        utf16_len(&signature_label[..start_byte]),
                        utf16_len(&signature_label[..end_byte]),
                    ])
                }
                None => ParameterLabel::Simple(label_text.clone()),
            };
            ParameterInformation {
                label,
                documentation: param_doc(doc, &func_name, i, name.map(String::as_str)),
            }
        })
        .collect();

    // `param_infos` includes the trailing return type as its last entry, so
    // clamp to len-2 — over-applied calls must highlight the last *parameter*,
    // not the return type (mirrors hover's guard).
    let active = (active_param as u32).min(param_infos.len().saturating_sub(2) as u32);

    // Function-level documentation: doc comment + effects
    let mut doc_parts: Vec<String> = Vec::new();
    if let Some(comment) = doc.doc_comments.get(&func_name) {
        doc_parts.push(comment.clone());
    }
    if let Some(effects) = doc.effect_info.get(&func_name) {
        doc_parts.push(format!("**Effects:** `{effects}`"));
    }
    let doc_value = if doc_parts.is_empty() {
        None
    } else {
        Some(Documentation::MarkupContent(MarkupContent {
            kind: MarkupKind::Markdown,
            value: doc_parts.join("\n\n"),
        }))
    };

    let signature = SignatureInformation {
        label: signature_label,
        documentation: doc_value,
        parameters: Some(param_infos),
        active_parameter: Some(active),
    };

    Some(SignatureHelp {
        signatures: vec![signature],
        active_signature: Some(0),
        active_parameter: Some(active),
    })
}

/// Look up the inferred type of a locally-bound name visible at `call_offset`.
///
/// Walks `local_type_info` for any binding whose declared name matches and
/// whose binding span sits *before* the call site. When several such bindings
/// exist (e.g. shadowing in nested scopes), returns the one with the latest
/// (closest, in source order) binding span — that's the binding the parser
/// would resolve at the call site.
fn lookup_local_binding_type(
    doc: &DocumentState,
    func_name: &str,
    call_offset: usize,
    enclosing: Option<Span>,
) -> Option<String> {
    let mut best: Option<(Span, String)> = None;
    for (span, ty) in &doc.local_type_info {
        if span.end > call_offset {
            continue;
        }
        if span.end > doc.source.len() || span.start > span.end {
            continue;
        }
        // Restrict to bindings inside the enclosing declaration; a local in a
        // sibling decl that merely precedes the call is not in scope and must
        // not be treated as a shadowing binding.
        if let Some(enc) = enclosing
            && (span.start < enc.start || span.end > enc.end) {
                continue;
            }
        // Char-boundary-safe: a stale span could land mid-multibyte-char.
        let name = crate::utils::safe_slice(&doc.source, *span);
        if name != func_name {
            continue;
        }
        match &best {
            None => best = Some((*span, ty.clone())),
            Some((cur, _)) if span.start > cur.start => {
                best = Some((*span, ty.clone()));
            }
            _ => {}
        }
    }
    best.map(|(_, ty)| ty)
}

/// Synthesize a positional parameter name: `0→a`, `25→z`, `26→a1`, `27→b1`, …
fn synth_param_name(i: usize) -> String {
    let letter = (b'a' + (i % 26) as u8) as char;
    let group = i / 26;
    if group == 0 {
        letter.to_string()
    } else {
        format!("{letter}{group}")
    }
}

/// Build a signature label like `func : a: T1 -> b: T2 -> Result`.
/// Falls back to the type string if no parameter names are known.
fn build_signature_label(
    func_name: &str,
    param_types: &[String],
    param_names: &[String],
    return_str: &str,
) -> String {
    if param_names.is_empty() {
        return format!("{func_name} : {return_str}");
    }
    // Compute the return type: the suffix of `return_str` after the param types.
    // We render arguments as `name: Type -> ...` and append the return type.
    let mut parts: Vec<String> = Vec::new();
    for (i, ty) in param_types.iter().enumerate() {
        if let Some(name) = param_names.get(i) {
            parts.push(format!("{name}: {ty}"));
        } else {
            parts.push(ty.clone());
        }
    }
    // Last entry of param_types is the return type — but parse_function_params
    // splits all arrow-separated parts including the return type. Keep the
    // final part as-is (no name).
    format!("{func_name} : {}", parts.join(" -> "))
}

/// Look up documentation for a single parameter.
/// Falls back to the function's doc comment if it mentions the parameter name.
fn param_doc(
    doc: &DocumentState,
    func_name: &str,
    _index: usize,
    name: Option<&str>,
) -> Option<Documentation> {
    let name = name?;
    // Look for a `param_name: ...` line in the function's doc comment
    let comment = doc.doc_comments.get(func_name)?;
    for line in comment.lines() {
        let trimmed = line.trim();
        // Match formats: `name: description`, `@param name description`, `- name: description`
        let candidate = trimmed
            .strip_prefix(&format!("{name}: "))
            .or_else(|| trimmed.strip_prefix(&format!("- {name}: ")))
            .or_else(|| trimmed.strip_prefix(&format!("@param {name} ")))
            .or_else(|| trimmed.strip_prefix(&format!("@param {name}: ")));
        if let Some(desc) = candidate {
            return Some(Documentation::String(desc.to_string()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestWorkspace;
    use crate::utils::offset_to_position;

    fn sig_params(uri: &Uri, position: Position) -> SignatureHelpParams {
        SignatureHelpParams {
            text_document_position_params: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position,
            },
            work_done_progress_params: Default::default(),
            context: None,
        }
    }

    fn probe(src: &str, needle: &str, after: usize) -> Option<SignatureHelp> {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find(needle).expect("needle") + after;
        let pos = offset_to_position(&doc.source, off);
        handle_signature_help(&ws.state, &sig_params(&uri, pos))
    }

    fn param_label_span(p: &ParameterInformation) -> Option<(u32, u32)> {
        match &p.label {
            ParameterLabel::LabelOffsets([s, e]) => Some((*s, *e)),
            ParameterLabel::Simple(_) => None,
        }
    }

    #[test]
    fn synth_param_name_avoids_overflow_and_repeats() {
        assert_eq!(synth_param_name(0), "a");
        assert_eq!(synth_param_name(25), "z");
        assert_eq!(synth_param_name(26), "a1");
        assert_eq!(synth_param_name(27), "b1");
    }

    /// Regression: a point-free / partially-eta-reduced definition supplies
    /// fewer lambda param names than the type's arity. The unnamed trailing
    /// parameters must still get distinct `name: Type` labels — otherwise
    /// their label-offset highlight collides with an earlier parameter of the
    /// same type, and the editor highlights the WRONG parameter when the
    /// cursor sits on a later argument.
    #[test]
    fn point_free_params_get_distinct_highlight_spans() {
        // `addBoth` has arity 2 in its type but only one lambda binder (`x`).
        let src = "addBoth : Int -> Int -> Int\naddBoth = \\x -> plus x\nmain = addBoth 1 2\n";
        // Cursor on the SECOND argument (the `2`), active parameter index 1.
        let h = probe(src, "addBoth 1 2", 10).expect("sig help");
        let sig = &h.signatures[0];
        assert_eq!(h.active_parameter, Some(1), "cursor is on the 2nd arg");

        let params = sig.parameters.as_ref().expect("parameters");
        // The two parameter slots must point at DIFFERENT, non-overlapping
        // spans within the label — the bug produced identical/overlapping spans.
        let s0 = param_label_span(&params[0]).expect("param0 offsets");
        let s1 = param_label_span(&params[1]).expect("param1 offsets");
        assert_ne!(
            s0, s1,
            "the two params must highlight different spans; label={:?}",
            sig.label
        );
        // The active parameter's span must reference the *second* `Int` in the
        // label, i.e. start at or after the first parameter's span ends.
        assert!(
            s1.0 >= s0.1,
            "2nd param span {s1:?} must come after 1st param span {s0:?}; label={:?}",
            sig.label
        );
        // And the substring the active span points at must read `<name>: Int`.
        let active = h.active_parameter.unwrap() as usize;
        let (start, end) = param_label_span(&params[active]).unwrap();
        let slice = &sig.label[start as usize..end as usize];
        assert!(
            slice.ends_with("Int") && slice.contains(':'),
            "active param label slice should be a `name: Int`, got {slice:?} in {:?}",
            sig.label
        );
    }
}
