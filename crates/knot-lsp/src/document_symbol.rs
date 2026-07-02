//! `textDocument/documentSymbol` handler — builds the hierarchical symbol tree
//! displayed in the editor's outline view.

use lsp_types::*;

use knot::ast::{self, DeclKind};

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

    for decl in &module.decls {
        let range = span_to_range(decl.span, source);
        let selection_range = range;

        match &decl.node {
            DeclKind::Data {
                name, constructors, ..
            } => {
                // Start the search after the `=` so a self-named constructor
                // (`data Circle = Circle {…}`) anchors on the constructor token,
                // not the type name before the `=`. Advance past each hit so a
                // name reused in an earlier constructor's field types can't
                // steal a later constructor's span. Mirrors semantic_tokens.rs.
                let mut search_from = source
                    .get(decl.span.start..decl.span.end.min(source.len()))
                    .and_then(|t| t.find('='))
                    .map(|p| decl.span.start + p + 1)
                    .unwrap_or(decl.span.start);
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
                            .unwrap_or(decl.span.end);
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
            DeclKind::TypeAlias { name, ty, .. } => {
                // Refined type aliases (`type Nat = Int where \x -> ...`) carry
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
            DeclKind::Source { name, ty, .. } => {
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
                        if let ast::TraitItem::Method { name: method_name, name_span, ty, .. } = item {
                            // Anchor the child on its own name token (carried in
                            // the AST) so "go to symbol" lands on the method, and
                            // give each child its own range instead of the whole
                            // trait — otherwise every sibling reports an identical
                            // range covering the entire trait.
                            let name_range = span_to_range(*name_span, source);
                            Some(DocumentSymbol {
                                name: method_name.clone(),
                                detail: Some(format_type_scheme(ty)),
                                kind: SymbolKind::METHOD,
                                tags: None,
                                deprecated: None,
                                range: name_range,
                                selection_range: name_range,
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
                        if let ast::ImplItem::Method { name, name_span, body, .. } = item {
                            // Use the AST's name span for selection, and span the
                            // method's full extent (name through body) for range,
                            // so each child reports its own region instead of the
                            // whole impl block.
                            let name_range = span_to_range(*name_span, source);
                            let full = ast::Span::new(
                                name_span.start,
                                body.span.end.max(name_span.end),
                            );
                            let full_range = span_to_range(full, source);
                            Some(DocumentSymbol {
                                name: name.clone(),
                                detail: None,
                                kind: SymbolKind::METHOD,
                                tags: None,
                                deprecated: None,
                                range: full_range,
                                selection_range: name_range,
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
                // Advance the search cursor past each matched constructor so an
                // earlier entry's name appearing in a later entry's text can't
                // steal its span (mirrors the Data-constructor loop above).
                let mut search_from = decl.span.start;
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
                            decl.span.end,
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
    fn document_symbol_includes_type_detail_for_fun() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"add : Int -> Int -> Int
add = \x y -> x + y
"#,
        );
        let resp =
            handle_document_symbol(&ws.state, &ds_params(&uri)).expect("symbols returned");
        let nested = match resp {
            DocumentSymbolResponse::Nested(s) => s,
            _ => panic!("expected nested"),
        };
        let add = nested.iter().find(|s| s.name == "add").expect("add present");
        let detail = add.detail.as_deref().expect("detail set");
        assert!(detail.contains("Int"), "detail should include type; got: {detail}");
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
