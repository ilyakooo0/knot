//! `textDocument/documentSymbol` handler — builds the hierarchical symbol tree
//! displayed in the editor's outline view.

use lsp_types::*;

use knot::ast::{self, DeclKind};

use crate::state::{DocumentState, ServerState};
use crate::type_format::{format_type_kind, format_type_scheme};
use crate::utils::{find_word_in_source, span_to_range};

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
    if let Some(eff) = doc.effect_info.get(name) {
        if !eff.trim_start_matches('{').trim_end_matches('}').trim().is_empty() {
            parts.push(eff.clone());
        }
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

    for decl in &module.decls {
        let range = span_to_range(decl.span, source);
        let selection_range = range;

        match &decl.node {
            DeclKind::Data {
                name, constructors, ..
            } => {
                let children: Vec<DocumentSymbol> = constructors
                    .iter()
                    .filter_map(|ctor| {
                        let ctor_span = find_word_in_source(source, &ctor.name, decl.span.start, decl.span.end)?;
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
            DeclKind::TypeAlias { name, .. } => {
                symbols.push(DocumentSymbol {
                    name: name.clone(),
                    detail: None,
                    kind: SymbolKind::TYPE_PARAMETER,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            DeclKind::Source { name, ty, history, .. } => {
                let mut detail_parts = vec![format_type_kind(&ty.node)];
                if *history {
                    detail_parts.push("with history".into());
                }
                symbols.push(DocumentSymbol {
                    name: format!("*{name}"),
                    detail: Some(detail_parts.join(" ")),
                    kind: SymbolKind::VARIABLE,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            DeclKind::View { name, ty, .. } => {
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
            DeclKind::Derived { name, ty, .. } => {
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
            DeclKind::Fun { name, ty, .. } => {
                let declared = ty.as_ref().map(format_type_scheme);
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
            DeclKind::Trait { name, items, .. } => {
                let children: Vec<DocumentSymbol> = items
                    .iter()
                    .filter_map(|item| {
                        if let ast::TraitItem::Method { name: method_name, ty, .. } = item {
                            Some(DocumentSymbol {
                                name: method_name.clone(),
                                detail: Some(format_type_scheme(ty)),
                                kind: SymbolKind::METHOD,
                                tags: None,
                                deprecated: None,
                                range,
                                selection_range: range,
                                children: None,
                            })
                        } else {
                            None
                        }
                    })
                    .collect();
                symbols.push(DocumentSymbol {
                    name: name.clone(),
                    detail: None,
                    kind: SymbolKind::INTERFACE,
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
            DeclKind::Impl {
                trait_name,
                args,
                items,
                ..
            } => {
                let args_str = args
                    .iter()
                    .map(|a| format_type_kind(&a.node))
                    .collect::<Vec<_>>()
                    .join(" ");
                let children: Vec<DocumentSymbol> = items
                    .iter()
                    .filter_map(|item| {
                        if let ast::ImplItem::Method { name, .. } = item {
                            Some(DocumentSymbol {
                                name: name.clone(),
                                detail: None,
                                kind: SymbolKind::METHOD,
                                tags: None,
                                deprecated: None,
                                range,
                                selection_range: range,
                                children: None,
                            })
                        } else {
                            None
                        }
                    })
                    .collect();
                symbols.push(DocumentSymbol {
                    name: format!("impl {trait_name} {args_str}"),
                    detail: None,
                    kind: SymbolKind::OBJECT,
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
            DeclKind::Route { name, entries, .. } => {
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
                        DocumentSymbol {
                            name: e.constructor.clone(),
                            detail: Some(format!("{method} {path_str}")),
                            kind: SymbolKind::ENUM_MEMBER,
                            tags: None,
                            deprecated: None,
                            range,
                            selection_range: range,
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
            DeclKind::RouteComposite { name, .. } => {
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
            DeclKind::Migrate { relation, .. } => {
                symbols.push(DocumentSymbol {
                    name: format!("migrate *{relation}"),
                    detail: None,
                    kind: SymbolKind::EVENT,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            DeclKind::SubsetConstraint { .. } => {}
            DeclKind::UnitDecl { .. } => {}
        }
    }

    symbols
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestWorkspace;

    fn ds_params(uri: &Uri) -> DocumentSymbolParams {
        DocumentSymbolParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        }
    }

    fn names_in(symbols: &[DocumentSymbol]) -> Vec<String> {
        symbols.iter().map(|s| s.name.clone()).collect()
    }

    #[test]
    fn document_symbol_lists_top_level_decls() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"type Person = {name: Text, age: Int}
*people : [Person]
greet = \name -> "hi " ++ name
main = println "hello"
"#,
        );
        let resp =
            handle_document_symbol(&ws.state, &ds_params(&uri)).expect("symbols returned");
        let nested = match resp {
            DocumentSymbolResponse::Nested(s) => s,
            _ => panic!("expected nested response"),
        };
        let names = names_in(&nested);
        assert!(names.iter().any(|n| n == "Person"), "names: {names:?}");
        assert!(names.iter().any(|n| n.contains("people")), "names: {names:?}");
        assert!(names.iter().any(|n| n == "greet"), "names: {names:?}");
        assert!(names.iter().any(|n| n == "main"), "names: {names:?}");
    }

    #[test]
    fn document_symbol_distinguishes_kinds() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"data Color = Red {} | Blue {}
*items : [{name: Text}]
main = println "ok"
"#,
        );
        let resp =
            handle_document_symbol(&ws.state, &ds_params(&uri)).expect("symbols returned");
        let nested = match resp {
            DocumentSymbolResponse::Nested(s) => s,
            _ => panic!("expected nested"),
        };
        let color = nested.iter().find(|s| s.name == "Color").expect("Color present");
        assert_eq!(color.kind, SymbolKind::ENUM);
        // Constructors should be nested children of the data decl.
        let children = color.children.as_ref().expect("constructors");
        let child_names: Vec<_> = children.iter().map(|c| c.name.clone()).collect();
        assert!(child_names.contains(&"Red".to_string()));
        assert!(child_names.contains(&"Blue".to_string()));
    }
}
