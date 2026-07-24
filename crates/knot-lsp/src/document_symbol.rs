//! `textDocument/documentSymbol` handler — builds the hierarchical symbol tree
//! displayed in the editor's outline view.

use lsp_types::*;

use knot::ast::{self, ExprKind};
use crate::utils::top_fields;

use crate::state::{DocumentState, ServerState};
use crate::type_format::{format_type_kind, format_type_scheme};
use crate::utils::{find_word_after_eq, find_word_in_source, span_to_range};

// ── Document symbols (hierarchical) ─────────────────────────────────

pub(crate) fn handle_document_symbol(
    state: &ServerState,
    params: &DocumentSymbolParams,
) -> Option<DocumentSymbolResponse> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let symbols = build_symbols(doc);
    Some(DocumentSymbolResponse::Nested(symbols))
}

/// Build a hint string combining declared/inferred type and effects.
/// Falls back to declared type, then inferred type, then effect string alone.
fn detail_for(doc: &DocumentState, name: &str, declared: Option<String>) -> Option<String> {
    let mut parts = Vec::new();
    let ty = declared.or_else(|| doc.type_info.get(name).cloned());
    if let Some(t) = ty {
        parts.push(t);
    }
    if let Some(eff) = doc.effect_info.get(name)
        && !eff.trim_start_matches('{').trim_end_matches('}').trim().is_empty() {
            parts.push(eff.clone());
        }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

#[allow(deprecated)]
fn build_symbols(doc: &DocumentState) -> Vec<DocumentSymbol> {
    let module = &doc.module;
    let source = doc.source.as_str();
    let mut symbols = Vec::new();

    for decl in top_fields(module) {
        let dspan = decl.value.span;
        let range = span_to_range(dspan, source);
        let selection_range = range;

        match &decl.value.node {
            ExprKind::DataCtor {
                name, constructors, ..
            } => {
                // Start the search after the `=` so a self-named constructor
                // (`data Circle = Circle {…}`) anchors on the constructor token,
                // not the type name before the `=`. Advance past each hit so a
                // name reused in an earlier constructor's field types can't
                // steal a later constructor's span. Mirrors semantic_tokens.rs.
                let mut search_from = source
                    .get(dspan.start..dspan.end.min(source.len()))
                    .and_then(|t| t.find('='))
                    .map(|p| dspan.start + p + 1)
                    .unwrap_or(dspan.start);
                let children: Vec<DocumentSymbol> = constructors
                    .iter()
                    .filter_map(|ctor| {
                        // Bound the name search to the window before this
                        // constructor's first field type, so a later ctor whose
                        // name reappears as a field type of an earlier ctor
                        // (`data T = A {x: B} | B {…}`) isn't matched at that
                        // field-type occurrence. The name precedes its fields.
                        let search_end = ctor
                            .fields
                            .first()
                            .map(|f| f.value.span.start)
                            .unwrap_or(dspan.end);
                        let ctor_span = find_word_in_source(source, &ctor.name, search_from, search_end)?;
                        // Advance past this ctor's last field type (or its name,
                        // if nullary) so its field types can't be matched as the
                        // next ctor's name.
                        search_from = ctor
                            .fields
                            .last()
                            .map(|f| f.value.span.end)
                            .unwrap_or(ctor_span.end);
                        let ctor_range = span_to_range(ctor_span, source);
                        Some(DocumentSymbol {
                            name: ctor.name.clone(),
                            detail: if ctor.fields.is_empty() {
                                None
                            } else {
                                let fs: Vec<String> = ctor
                                    .fields
                                    .iter()
                                    .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
                                    .collect();
                                Some(format!("{{{}}}", fs.join(", ")))
                            },
                            kind: SymbolKind::ENUM_MEMBER,
                            tags: None,
                            deprecated: None,
                            range: ctor_range,
                            selection_range: ctor_range,
                            children: None,
                        })
                    })
                    .collect();
                let kind = if constructors.len() > 1 {
                    SymbolKind::ENUM
                } else {
                    SymbolKind::STRUCT
                };
                symbols.push(DocumentSymbol {
                    name: name.clone(),
                    detail: Some(format!(
                        "{} ctor{}",
                        constructors.len(),
                        if constructors.len() == 1 { "" } else { "s" }
                    )),
                    kind,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: if children.is_empty() {
                        None
                    } else {
                        Some(children)
                    },
                });
            }
            ExprKind::TypeCtor { name, ty, .. } => {
                // Refined type aliases (`type Nat = Int 1 where \x -> ...`) carry
                // their predicate in the AST. Surface it in the outline so the
                // user can scan for refined types without opening each one.
                let detail = match &ty.node {
                    ast::TypeKind::Refined { base, predicate } => {
                        let base_str = format_type_kind(&base.node);
                        let pred_src = predicate
                            .span
                            .start
                            .checked_add(0)
                            .and_then(|_| {
                                doc.source.get(predicate.span.start..predicate.span.end)
                            })
                            .map(|s| s.trim().to_string())
                            .unwrap_or_else(|| "…".into());
                        Some(format!("refined {base_str} where {pred_src}"))
                    }
                    _ => Some(format_type_kind(&ty.node)),
                };
                symbols.push(DocumentSymbol {
                    name: name.clone(),
                    detail,
                    kind: SymbolKind::TYPE_PARAMETER,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            ExprKind::SourceDecl { name, ty, .. } => {
                symbols.push(DocumentSymbol {
                    name: format!("*{name}"),
                    detail: Some(format_type_kind(&ty.node)),
                    kind: SymbolKind::VARIABLE,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            ExprKind::ViewDecl { name, ty, .. } => {
                let declared = ty.as_ref().map(format_type_scheme);
                symbols.push(DocumentSymbol {
                    name: format!("*{name}"),
                    detail: detail_for(doc, name, declared).or_else(|| Some("view".into())),
                    kind: SymbolKind::VARIABLE,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            ExprKind::DerivedDecl { name, ty, .. } => {
                let declared = ty.as_ref().map(format_type_scheme);
                symbols.push(DocumentSymbol {
                    name: format!("&{name}"),
                    detail: detail_for(doc, name, declared).or_else(|| Some("derived".into())),
                    kind: SymbolKind::VARIABLE,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            ExprKind::SubsetConstraint { .. } => {}
            ExprKind::RouteDecl { name, entries } => {
                // Advance the search cursor past each matched constructor so an
                // earlier entry's name appearing in a later entry's text can't
                // steal its span (mirrors the Data-constructor loop above).
                let mut search_from = dspan.start;
                let children: Vec<DocumentSymbol> = entries
                    .iter()
                    .map(|e| {
                        let path_str: String = e
                            .path
                            .iter()
                            .map(|seg| match seg {
                                ast::PathSegment::Literal(s) => format!("/{s}"),
                                ast::PathSegment::Param { name, .. } => format!("/{{{name}}}"),
                            })
                            .collect();
                        let method = match e.method {
                            ast::HttpMethod::Get => "GET",
                            ast::HttpMethod::Post => "POST",
                            ast::HttpMethod::Put => "PUT",
                            ast::HttpMethod::Delete => "DELETE",
                            ast::HttpMethod::Patch => "PATCH",
                        };
                        // The endpoint constructor is defined after `=`
                        // (`… -> Response = Ctor`); anchor on that token so an
                        // identically-named response type or path segment
                        // earlier in the entry isn't picked instead. Mirrors
                        // `defs.rs`'s `find_word_after_eq` lookup.
                        let name_range = match find_word_after_eq(
                            source,
                            &e.constructor,
                            search_from,
                            dspan.end,
                        ) {
                            Some(s) => {
                                search_from = s.end;
                                span_to_range(s, source)
                            }
                            None => range,
                        };
                        DocumentSymbol {
                            name: e.constructor.clone(),
                            detail: Some(format!("{method} {path_str}")),
                            kind: SymbolKind::ENUM_MEMBER,
                            tags: None,
                            deprecated: None,
                            range: name_range,
                            selection_range: name_range,
                            children: None,
                        }
                    })
                    .collect();
                symbols.push(DocumentSymbol {
                    name: format!("route {name}"),
                    detail: None,
                    kind: SymbolKind::MODULE,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: if children.is_empty() {
                        None
                    } else {
                        Some(children)
                    },
                });
            }
            ExprKind::RouteCompositeDecl { name, .. } => {
                symbols.push(DocumentSymbol {
                    name: format!("route {name}"),
                    detail: Some("composite".into()),
                    kind: SymbolKind::MODULE,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            _ => {
                // A named function field.
                let name = &decl.name;
                let declared = decl.sig.as_ref().map(format_type_scheme);
                symbols.push(DocumentSymbol {
                    name: name.clone(),
                    detail: detail_for(doc, name, declared),
                    kind: SymbolKind::FUNCTION,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
        }
    }

    symbols
}


