//! `textDocument/hover` handler. Renders type, effect, refinement, route, and
//! schema info for the symbol under the cursor.

use lsp_types::*;

use knot::ast::{DeclKind, TypeKind};

use crate::shared::{
    constraints_for_type_var, extract_record_fields, find_enclosing_application,
    find_enclosing_type_scheme, find_field_access_at_offset, find_field_refinement,
    format_route_constructor_hover, parse_function_params, predicate_to_source,
    resolve_var_to_source, ReceiverKind,
};
use crate::state::ServerState;
use crate::type_format::format_type_kind;
use crate::utils::{
    position_to_offset, safe_slice, span_to_range, word_at_position, word_span_at_offset,
};

// ── Hover ───────────────────────────────────────────────────────────

pub(crate) fn handle_hover(state: &ServerState, params: &HoverParams) -> Option<Hover> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;

    let offset = position_to_offset(&doc.source, pos);

    // Try literal types first (span-based, works for strings/floats/etc.)
    if let Some((span, ty)) = doc
        .literal_types
        .iter()
        .find(|(span, _)| span.start <= offset && offset < span.end)
    {
        let source_text = safe_slice(&doc.source, *span);
        let detail = format!("{source_text} : {ty}");
        return Some(Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::Markdown,
                value: format!("```knot\n{detail}\n```"),
            }),
            range: Some(span_to_range(*span, &doc.source)),
        });
    }

    let word = word_at_position(&doc.source, pos)?;
    let word_span = word_span_at_offset(&doc.source, offset);

    // Try local binding types (let, bind, lambda params, case patterns).
    // Check if cursor is on a binding site or on a usage that references one.
    let local_type = doc
        .local_type_info
        .iter()
        .find(|(span, _)| span.start <= offset && offset < span.end)
        .map(|(_, ty)| ty.clone())
        .or_else(|| {
            // Cursor is on a usage — find the definition span and look up its type
            let (_, def_span) = doc
                .references
                .iter()
                .find(|(usage, _)| usage.start <= offset && offset < usage.end)?;
            doc.local_type_info.get(def_span).cloned()
        });

    // Build hover detail
    // Track the raw type string so we can later scan it for refined-type names
    // and surface their predicates inline. `None` means we showed details only
    // (no inferred type was available), in which case there's nothing to scan.
    let mut type_for_refinement_scan: Option<String> = None;
    let detail_opt = if let Some(ty) = local_type {
        type_for_refinement_scan = Some(ty.clone());
        Some(format!("{word} : {ty}"))
    } else if let Some(d) = doc.details.get(word) {
        let base = if let Some(inferred) = doc.type_info.get(word) {
            type_for_refinement_scan = Some(inferred.clone());
            if !d.contains(':') {
                format!("{d} : {inferred}")
            } else {
                d.clone()
            }
        } else {
            d.clone()
        };
        Some(if let Some(effects) = doc.effect_info.get(word) {
            format!("{base}\n{effects}")
        } else {
            base
        })
    } else if let Some(inferred) = doc.type_info.get(word) {
        type_for_refinement_scan = Some(inferred.clone());
        let base = format!("{word} : {inferred}");
        Some(if let Some(effects) = doc.effect_info.get(word) {
            format!("{base}\n{effects}")
        } else {
            base
        })
    } else {
        None
    };

    // The hover handler historically returned None when no symbol info was
    // available. With field-access and type-variable enrichment, we fall
    // through and render an informational hover for those cases too.
    let field_at_cursor = find_field_access_at_offset(&doc.module, offset);
    let enclosing_scheme = find_enclosing_type_scheme(&doc.module, offset);
    let type_var_constraints: Vec<&knot::ast::Constraint> = enclosing_scheme
        .as_ref()
        .filter(|_| {
            word.chars()
                .next()
                .map(|c| c.is_ascii_lowercase())
                .unwrap_or(false)
        })
        .map(|(scheme, _)| constraints_for_type_var(scheme, word))
        .unwrap_or_default();
    if detail_opt.is_none() && field_at_cursor.is_none() && type_var_constraints.is_empty() {
        return None;
    }
    let mut value = match &detail_opt {
        Some(detail) => format!("```knot\n{detail}\n```"),
        None => String::new(),
    };

    // At a call site, show the full signature with the active argument highlighted
    if let Some((func_name, active_param)) =
        find_enclosing_application(&doc.module, &doc.source, offset)
    {
        if func_name == word {
            if let Some(type_str) = doc.type_info.get(func_name.as_str()) {
                let params_list = parse_function_params(type_str);
                if params_list.len() > 1 {
                    let highlighted: Vec<String> = params_list
                        .iter()
                        .enumerate()
                        .map(|(i, p)| {
                            if i == active_param && i < params_list.len() - 1 {
                                format!("**{p}**")
                            } else {
                                p.clone()
                            }
                        })
                        .collect();
                    value.push_str(&format!(
                        "\n\n*Signature:* `{} : {}`",
                        func_name,
                        highlighted.join(" → ")
                    ));
                }
            }
        }
    }

    // For source/view/derived refs, show the relation schema
    for decl in &doc.module.decls {
        match &decl.node {
            DeclKind::Source { name, ty, history } if name == word => {
                let hist = if *history { " (with history)" } else { "" };
                let schema = format_schema_from_type(&ty.node);
                if !schema.is_empty() {
                    value.push_str(&format!("\n\n**Schema:**{hist}\n{schema}"));
                }
                break;
            }
            DeclKind::View { name, .. } if name == word => {
                if let Some(inferred) = doc.type_info.get(word) {
                    let schema = format_schema_from_type_str(inferred);
                    if !schema.is_empty() {
                        value.push_str(&format!("\n\n**View schema:**\n{schema}"));
                    }
                }
                break;
            }
            DeclKind::Derived { name, .. } if name == word => {
                if let Some(inferred) = doc.type_info.get(word) {
                    let schema = format_schema_from_type_str(inferred);
                    if !schema.is_empty() {
                        value.push_str(&format!("\n\n**Derived schema:**\n{schema}"));
                    }
                }
                break;
            }
            _ => {}
        }
    }

    // Routes: if the word names a route constructor, render the resolved URL
    // with typed path parameters and any declared body/query/headers.
    if let Some(route_summary) = format_route_constructor_hover(&doc.module, word) {
        value.push_str("\n\n---\n\n");
        value.push_str(&route_summary);
    }

    // Refined types: if the word names a refined type alias, show its predicate.
    if let Some(predicate) = doc.refined_types.get(word) {
        let pred_src = predicate_to_source(predicate, &doc.source);
        value.push_str(&format!(
            "\n\n**Refined type:** values of `{word}` must satisfy `{pred_src}`"
        ));
    } else if let Some(type_str) = type_for_refinement_scan.as_deref() {
        // Inline refinements: when the inferred type *contains* refined type
        // names (e.g. `x : Nat`, `f : Nat -> Nat`), surface each refinement
        // so the user knows what predicate the value must satisfy. Skip when
        // hovering on the refined type's own declaration (handled above).
        let mut mentioned: Vec<&String> = doc
            .refined_types
            .keys()
            .filter(|name| name.as_str() != word && type_contains_name(type_str, name))
            .collect();
        if !mentioned.is_empty() {
            mentioned.sort();
            value.push_str("\n\n**Refinements in this type:**");
            for name in mentioned {
                if let Some(predicate) = doc.refined_types.get(name) {
                    let pred_src = predicate_to_source(predicate, &doc.source);
                    value.push_str(&format!("\n- `{name}` — values must satisfy `{pred_src}`"));
                }
            }
        }
    }

    // If the cursor is inside a `refine expr` form, show its inferred target type
    // and the predicate it'll be checked against.
    if let Some((_, target_name)) = doc
        .refine_targets
        .iter()
        .find(|(span, _)| span.start <= offset && offset < span.end)
    {
        let detail = if let Some(predicate) = doc.refined_types.get(target_name) {
            let pred_src = predicate_to_source(predicate, &doc.source);
            format!(
                "\n\n**`refine` target:** `{target_name}` — predicate `{pred_src}` is checked at runtime; result is `Result RefinementError {target_name}`"
            )
        } else {
            format!("\n\n**`refine` target:** `{target_name}`")
        };
        value.push_str(&detail);
    }

    // Sources whose schema declares refined fields: list the refinements so the
    // user knows which fields will be validated on `set`.
    if let Some(refinements) = doc.source_refinements.get(word) {
        if !refinements.is_empty() {
            value.push_str("\n\n**Refinements (validated on write):**");
            for (field, type_name, predicate) in refinements {
                let pred_src = predicate_to_source(predicate, &doc.source);
                let label = match field {
                    Some(f) => format!("`{f}: {type_name}`"),
                    None => format!("(whole element) `{type_name}`"),
                };
                value.push_str(&format!("\n- {label} — `{pred_src}`"));
            }
        }
    }

    // Trait-constraint hover: if the cursor lands on a generic type parameter
    // inside a function's type signature, list the trait constraints that
    // mention that variable. Lets users see at a glance why `a` is required to
    // be `Display a` without scrolling to the constraint list.
    if !type_var_constraints.is_empty() {
        let decl_name = enclosing_scheme.map(|(_, n)| n).unwrap_or("");
        let rendered: Vec<String> = type_var_constraints
            .iter()
            .map(|c| {
                let args: Vec<String> = c
                    .args
                    .iter()
                    .map(|t| format_type_kind(&t.node))
                    .collect();
                format!("`{} {}`", c.trait_name, args.join(" "))
            })
            .collect();
        if !value.is_empty() {
            value.push_str("\n\n");
        }
        value.push_str(&format!(
            "**Generic parameter `{word}`** of `{decl_name}` — must satisfy: {}",
            rendered.join(", ")
        ));
    }

    // Field-access hover: when the cursor is on a record field name (e.g. the
    // `age` in `p.age`), surface the source-declared refinement for that field.
    // The refinement metadata is keyed by (source-name, field-name); we trace
    // the receiver back to a `Bind`/`Let` from a `*source` to find which source
    // owns the field.
    if let Some(field_at) = &field_at_cursor {
        let owner_source = match &field_at.receiver {
            ReceiverKind::Var(name) => resolve_var_to_source(&doc.module, name),
            ReceiverKind::SourceRef(name) | ReceiverKind::DerivedRef(name) => Some(name.clone()),
            ReceiverKind::Other => None,
        };
        if let Some(source_name) = owner_source.as_deref() {
            if let Some((type_label, predicate)) =
                find_field_refinement(&doc.source_refinements, source_name, &field_at.field_name)
            {
                let pred_src = predicate_to_source(predicate, &doc.source);
                if !value.is_empty() {
                    value.push_str("\n\n");
                }
                value.push_str(&format!(
                    "**Field refinement:** `{}.{}` must satisfy `{}` (refined `{}`)",
                    source_name, field_at.field_name, pred_src, type_label
                ));
            }
        }
    }

    // Include doc comments if available
    if let Some(doc_comment) = doc.doc_comments.get(word) {
        value.push_str("\n\n---\n\n");
        value.push_str(doc_comment);
    }

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value,
        }),
        range: word_span.map(|s| span_to_range(s, &doc.source)),
    })
}

/// Whole-word search for `name` inside a rendered type string. Type strings
/// run together identifiers with non-identifier punctuation (`->`, `,`, `(`,
/// `[`, `<`, `{`, whitespace), so a substring scan that respects identifier
/// boundaries is enough to spot `Nat` in `Nat -> Nat` without falsely
/// matching `Nation`.
fn type_contains_name(haystack: &str, name: &str) -> bool {
    let bytes = haystack.as_bytes();
    let needle = name.as_bytes();
    if needle.is_empty() || bytes.len() < needle.len() {
        return false;
    }
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let left_ok = i == 0 || !is_ident(bytes[i - 1]);
            let right_ok = i + needle.len() >= bytes.len() || !is_ident(bytes[i + needle.len()]);
            if left_ok && right_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Format a TypeKind as a markdown schema table for hover display.
fn format_schema_from_type(ty: &TypeKind) -> String {
    match ty {
        TypeKind::Record { fields, .. } => {
            let mut lines = Vec::new();
            lines.push("| Field | Type |".to_string());
            lines.push("|-------|------|".to_string());
            for f in fields {
                lines.push(format!(
                    "| `{}` | `{}` |",
                    f.name,
                    format_type_kind(&f.value.node)
                ));
            }
            lines.join("\n")
        }
        _ => String::new(),
    }
}

/// Format a type string like `[{name: Text, age: Int}]` as a schema table.
fn format_schema_from_type_str(type_str: &str) -> String {
    let s = type_str.trim();
    // Unwrap IO wrapper
    let s = if s.starts_with("IO ") {
        let rest = &s[3..];
        if rest.starts_with('{') {
            if let Some(close) = rest.find('}') {
                rest[close + 1..].trim()
            } else {
                rest
            }
        } else {
            rest
        }
    } else {
        s
    };
    // Unwrap relation brackets
    let s = if s.starts_with('[') && s.ends_with(']') {
        &s[1..s.len() - 1]
    } else {
        s
    };
    // Parse record fields
    if s.starts_with('{') && s.ends_with('}') {
        let fields = extract_record_fields(s);
        let inner = &s[1..s.len() - 1];
        if fields.is_empty() {
            return String::new();
        }
        let mut lines = Vec::new();
        lines.push("| Field | Type |".to_string());
        lines.push("|-------|------|".to_string());
        // Parse field:type pairs from inner
        let mut depth = 0i32;
        let mut current = String::new();
        for ch in inner.chars() {
            match ch {
                '{' | '[' | '(' | '<' => {
                    depth += 1;
                    current.push(ch);
                }
                '}' | ']' | ')' | '>' => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 => {
                    if let Some((name, ty)) = current.trim().split_once(':') {
                        lines.push(format!("| `{}` | `{}` |", name.trim(), ty.trim()));
                    }
                    current.clear();
                }
                '|' if depth == 0 => break,
                _ => current.push(ch),
            }
        }
        if let Some((name, ty)) = current.trim().split_once(':') {
            lines.push(format!("| `{}` | `{}` |", name.trim(), ty.trim()));
        }
        lines.join("\n")
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestWorkspace;
    use crate::utils::offset_to_position;

    fn hover_params(uri: &Uri, position: Position) -> HoverParams {
        HoverParams {
            text_document_position_params: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position,
            },
            work_done_progress_params: Default::default(),
        }
    }

    fn hover_text(hover: Hover) -> String {
        match hover.contents {
            HoverContents::Scalar(MarkedString::String(s)) => s,
            HoverContents::Scalar(MarkedString::LanguageString(ls)) => ls.value,
            HoverContents::Markup(m) => m.value,
            HoverContents::Array(items) => items
                .into_iter()
                .map(|i| match i {
                    MarkedString::String(s) => s,
                    MarkedString::LanguageString(ls) => ls.value,
                })
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }

    #[test]
    fn hover_shows_inferred_type_for_function() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "id = \\x -> x\nmain = println (show (id 42))\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("id =").expect("def");
        let pos = offset_to_position(&doc.source, off);
        let hover = handle_hover(&ws.state, &hover_params(&uri, pos)).expect("hover");
        let text = hover_text(hover);
        assert!(
            text.contains("id"),
            "hover should mention symbol; got: {text}"
        );
    }

    #[test]
    fn hover_surfaces_refined_type_predicates_inline() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"type Nat = Int where \x -> x >= 0
double : Nat -> Nat
double = \n -> n + n
"#,
        );
        let doc = ws.doc(&uri);
        // Hover on the function name `double`. Its type contains `Nat`
        // (a refined alias), so the hover should explain the predicate.
        let off = doc.source.find("double :").expect("definition");
        let pos = offset_to_position(&doc.source, off);
        let hover = handle_hover(&ws.state, &hover_params(&uri, pos)).expect("hover");
        let text = hover_text(hover);
        assert!(
            text.contains("Refinements in this type"),
            "hover should call out embedded refined types; got:\n{text}"
        );
        assert!(
            text.contains(">= 0") || text.contains(">=0"),
            "hover should include the predicate text; got:\n{text}"
        );
    }

    #[test]
    fn hover_does_not_repeat_refinement_when_hovering_alias_itself() {
        // When the user hovers on the refined-type alias name `Nat`, the
        // existing handler renders the predicate via the "Refined type:"
        // section. The new inline scan should not fire on the same name
        // and produce a duplicate "Refinements in this type" block.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "type Nat = Int where \\x -> x >= 0\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("Nat").expect("alias");
        let pos = offset_to_position(&doc.source, off);
        let hover = handle_hover(&ws.state, &hover_params(&uri, pos)).expect("hover");
        let text = hover_text(hover);
        assert!(
            !text.contains("Refinements in this type"),
            "alias hover duplicated refinement section; got:\n{text}"
        );
    }

    #[test]
    fn hover_surfaces_field_refinement_for_source_field() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"*scores : [{name: Text, score: Int where \x -> x >= 0}]

main = do
  s <- *scores
  yield s.score
"#,
        );
        let doc = ws.doc(&uri);
        let off = doc.source.rfind("score\n").expect("field use");
        let pos = offset_to_position(&doc.source, off + 2);
        let hover = handle_hover(&ws.state, &hover_params(&uri, pos)).expect("hover");
        let text = hover_text(hover);
        assert!(
            text.contains("Field refinement"),
            "expected field-refinement section; got:\n{text}"
        );
        assert!(
            text.contains(">= 0") || text.contains(">=0"),
            "expected predicate text; got:\n{text}"
        );
    }

    #[test]
    fn hover_shows_trait_constraints_for_generic_param() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"trait Display a where
  display : a -> Text

show2 : Display a => a -> Text
show2 = \x -> display x
"#,
        );
        let doc = ws.doc(&uri);
        // Locate the `a` after `=>` in show2's signature, not the one inside
        // the trait body.
        let sig_start = doc.source.find("show2 :").expect("sig start");
        let arrow = doc.source[sig_start..]
            .find("=> ")
            .map(|p| sig_start + p + 3)
            .expect("arrow site");
        let pos = offset_to_position(&doc.source, arrow);
        let hover = handle_hover(&ws.state, &hover_params(&uri, pos))
            .expect("hover at type-var position");
        let text = hover_text(hover);
        assert!(
            text.contains("Generic parameter") && text.contains("Display"),
            "expected generic-param section with Display constraint; got:\n{text}"
        );
    }

    #[test]
    fn hover_returns_none_for_blank_position() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "main = println \"hi\"\n");
        // Position past end of line — no symbol there.
        let pos = Position::new(5, 5);
        let resp = handle_hover(&ws.state, &hover_params(&uri, pos));
        assert!(resp.is_none());
    }
}

