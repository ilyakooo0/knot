//! `textDocument/documentSymbol` handler — builds the hierarchical symbol tree
//! displayed in the editor's outline view.

use lsp_types::*;

use knot::ast::{self, DeclKind, Module};

use crate::state::ServerState;
use crate::type_format::{format_type_kind, format_type_scheme};
use crate::utils::{find_word_in_source, span_to_range};

// ── Document symbols (hierarchical) ─────────────────────────────────

pub(crate) fn handle_document_symbol(
    state: &ServerState,
    params: &DocumentSymbolParams,
) -> Option<DocumentSymbolResponse> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let symbols = build_symbols(&doc.module, &doc.source);
    Some(DocumentSymbolResponse::Nested(symbols))
}

#[allow(deprecated)]
fn build_symbols(module: &Module, source: &str) -> Vec<DocumentSymbol> {
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
                symbols.push(DocumentSymbol {
                    name: name.clone(),
                    detail: None,
                    kind: SymbolKind::STRUCT,
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
            DeclKind::Source { name, .. } => {
                symbols.push(DocumentSymbol {
                    name: format!("*{name}"),
                    detail: None,
                    kind: SymbolKind::VARIABLE,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            DeclKind::View { name, .. } => {
                symbols.push(DocumentSymbol {
                    name: format!("*{name}"),
                    detail: Some("view".into()),
                    kind: SymbolKind::VARIABLE,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            DeclKind::Derived { name, .. } => {
                symbols.push(DocumentSymbol {
                    name: format!("&{name}"),
                    detail: Some("derived".into()),
                    kind: SymbolKind::VARIABLE,
                    tags: None,
                    deprecated: None,
                    range,
                    selection_range,
                    children: None,
                });
            }
            DeclKind::Fun { name, .. } => {
                symbols.push(DocumentSymbol {
                    name: name.clone(),
                    detail: None,
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
