//! `textDocument/signatureHelp` handler.

use lsp_types::*;

use knot::ast::{self, DeclKind, Module, Span};

use crate::shared::{find_enclosing_application, parse_function_params};
use crate::state::{DocumentState, ServerState};
use crate::utils::position_to_offset;

// ── Signature Help (paren-aware) ────────────────────────────────────

pub(crate) fn handle_signature_help(
    state: &ServerState,
    params: &SignatureHelpParams,
) -> Option<SignatureHelp> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
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
    let type_str_owned: String;
    let type_str: &str = if let Some(global) = doc.type_info.get(func_name.as_str()) {
        global.as_str()
    } else if let Some(local) = lookup_local_binding_type(doc, &func_name, offset) {
        type_str_owned = local;
        type_str_owned.as_str()
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
    let mut param_names = extract_param_names(&doc.module, &func_name);
    if param_names.is_empty() && param_types.len() > 1 {
        // For arrow types `T1 -> T2 -> ... -> R`, the last entry is the
        // return type — name positional params for the rest.
        let arity = param_types.len() - 1;
        param_names = (0..arity)
            .map(|i| ((b'a' + (i as u8 % 26)) as char).to_string())
            .collect();
    }

    // Build parameter labels: "name: Type" if we have a name, else just "Type"
    // The label must be a substring of the signature label so the editor can
    // highlight the active parameter.
    let signature_label = build_signature_label(&func_name, &param_types, &param_names, type_str);

    let param_infos: Vec<ParameterInformation> = param_types
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let name = param_names.get(i);
            let label_text = match name {
                Some(n) => format!("{n}: {ty}"),
                None => ty.clone(),
            };
            // Locate the label substring in the signature for proper highlighting
            let label = match signature_label.find(&label_text) {
                Some(start) => ParameterLabel::LabelOffsets([
                    start as u32,
                    (start + label_text.len()) as u32,
                ]),
                None => ParameterLabel::Simple(label_text.clone()),
            };
            ParameterInformation {
                label,
                documentation: param_doc(doc, &func_name, i, name.map(String::as_str)),
            }
        })
        .collect();

    let active = (active_param as u32).min(param_infos.len().saturating_sub(1) as u32);

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
) -> Option<String> {
    let mut best: Option<(Span, String)> = None;
    for (span, ty) in &doc.local_type_info {
        if span.end > call_offset {
            continue;
        }
        if span.end > doc.source.len() || span.start > span.end {
            continue;
        }
        let name = &doc.source[span.start..span.end];
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

/// Extract parameter names from a function declaration's body.
/// Returns an empty Vec if the function isn't directly a lambda chain.
fn extract_param_names(module: &Module, func_name: &str) -> Vec<String> {
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun {
                name,
                body: Some(body),
                ..
            } if name == func_name => {
                return collect_lambda_param_names(body);
            }
            DeclKind::Trait { items, .. } => {
                for item in items {
                    if let ast::TraitItem::Method {
                        name,
                        default_params,
                        ..
                    } = item
                    {
                        if name == func_name {
                            return default_params
                                .iter()
                                .map(|p| pat_to_simple_name(&p.node))
                                .collect();
                        }
                    }
                }
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { name, params, .. } = item {
                        if name == func_name {
                            return params
                                .iter()
                                .map(|p| pat_to_simple_name(&p.node))
                                .collect();
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Vec::new()
}

/// Walk a chain of nested lambdas (`\a -> \b -> body`) and collect param names.
fn collect_lambda_param_names(expr: &ast::Expr) -> Vec<String> {
    let mut names = Vec::new();
    let mut cur = expr;
    loop {
        match &cur.node {
            ast::ExprKind::Lambda { params, body } => {
                for p in params {
                    names.push(pat_to_simple_name(&p.node));
                }
                cur = body;
            }
            _ => break,
        }
    }
    names
}

/// Render a pattern as a simple name for parameter display.
/// `x` → "x", `{name, age}` → "{name, age}", `_` → "_".
fn pat_to_simple_name(pat: &ast::PatKind) -> String {
    match pat {
        ast::PatKind::Var(name) => name.clone(),
        ast::PatKind::Wildcard => "_".into(),
        ast::PatKind::Record(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| match &f.pattern {
                    None => f.name.clone(),
                    Some(p) => format!("{}: {}", f.name, pat_to_simple_name(&p.node)),
                })
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        ast::PatKind::Constructor { name, payload } => {
            format!("{name} {}", pat_to_simple_name(&payload.node))
        }
        ast::PatKind::List(_) => "[..]".into(),
        ast::PatKind::Lit(_) => "_".into(),
    }
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
