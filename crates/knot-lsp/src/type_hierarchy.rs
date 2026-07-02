//! `textDocument/prepareTypeHierarchy` and the supertype/subtype follow-up
//! requests. Models the Knot trait/impl/data graph as an LSP type hierarchy
//! so the editor can walk:
//!   - trait → all impls (subtypes)
//!   - trait → supertraits (supertypes)
//!   - data type → all traits it implements (supertypes)
//!   - constructor → parent data type (supertypes)

use lsp_types::*;

use knot::ast::DeclKind;

use crate::state::ServerState;
use crate::utils::{
    position_to_offset, span_to_range, uri_to_path, word_at_position,
};

pub(crate) fn handle_prepare_type_hierarchy(
    state: &ServerState,
    params: &TypeHierarchyPrepareParams,
) -> Option<Vec<TypeHierarchyItem>> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    // Staleness guard: during the analysis debounce window the editor buffer
    // is newer than the analyzed source, so positions from the live buffer
    // would resolve against the wrong bytes.
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let word = word_at_position(&doc.source, pos)?.to_string();

    let mut items = Vec::new();

    for (decl_uri, decl_doc) in &state.documents {
        for decl in &decl_doc.module.decls {
            match &decl.node {
                DeclKind::Trait { name, .. } if name == &word => {
                    items.push(TypeHierarchyItem {
                        name: format!("trait {name}"),
                        kind: SymbolKind::INTERFACE,
                        tags: None,
                        detail: Some("trait".into()),
                        uri: decl_uri.clone(),
                        range: span_to_range(decl.span, &decl_doc.source),
                        selection_range: span_to_range(decl.span, &decl_doc.source),
                        data: Some(serde_json::json!({"kind": "trait", "name": name})),
                    });
                }
                DeclKind::Data { name, .. } if name == &word => {
                    items.push(TypeHierarchyItem {
                        name: format!("data {name}"),
                        kind: SymbolKind::CLASS,
                        tags: None,
                        detail: Some("data type".into()),
                        uri: decl_uri.clone(),
                        range: span_to_range(decl.span, &decl_doc.source),
                        selection_range: span_to_range(decl.span, &decl_doc.source),
                        data: Some(serde_json::json!({"kind": "data", "name": name})),
                    });
                }
                DeclKind::TypeAlias { name, .. } if name == &word => {
                    items.push(TypeHierarchyItem {
                        name: format!("type {name}"),
                        kind: SymbolKind::CLASS,
                        tags: None,
                        detail: Some("type alias".into()),
                        uri: decl_uri.clone(),
                        range: span_to_range(decl.span, &decl_doc.source),
                        selection_range: span_to_range(decl.span, &decl_doc.source),
                        data: Some(serde_json::json!({"kind": "alias", "name": name})),
                    });
                }
                DeclKind::Data {
                    name: data_name,
                    constructors,
                    ..
                } => {
                    if let Some(c) = constructors.iter().find(|c| c.name == word) {
                        items.push(TypeHierarchyItem {
                            name: format!("{} of {}", c.name, data_name),
                            kind: SymbolKind::CONSTRUCTOR,
                            tags: None,
                            detail: Some(format!("constructor of {data_name}")),
                            uri: decl_uri.clone(),
                            range: span_to_range(decl.span, &decl_doc.source),
                            selection_range: span_to_range(decl.span, &decl_doc.source),
                            data: Some(serde_json::json!({
                                "kind": "ctor",
                                "name": c.name,
                                "parent": data_name,
                            })),
                        });
                    }
                }
                _ => {}
            }
        }
    }

    if items.is_empty() {
        None
    } else {
        Some(items)
    }
}

pub(crate) fn handle_type_hierarchy_supertypes(
    state: &ServerState,
    params: &TypeHierarchySupertypesParams,
) -> Option<Vec<TypeHierarchyItem>> {
    let data = params.item.data.as_ref()?;
    let kind = data.get("kind")?.as_str()?;
    let name = data.get("name")?.as_str()?;
    let mut out = Vec::new();
    match kind {
        "trait" => {
            // Supertypes of a trait = its supertraits.
            for (decl_uri, doc) in &state.documents {
                for decl in &doc.module.decls {
                    if let DeclKind::Trait {
                        name: tn,
                        supertraits,
                        ..
                    } = &decl.node
                        && tn == name {
                            for c in supertraits {
                                push_trait_item(&c.trait_name, state, &mut out);
                            }
                        }
                }
                let _ = decl_uri;
            }
        }
        "data" | "alias" => {
            // Supertypes of a data type = traits it implements.
            for doc in state.documents.values() {
                for decl in &doc.module.decls {
                    if let DeclKind::Impl {
                        trait_name, args, ..
                    } = &decl.node
                    {
                        // Compare `name` only against the *head* type
                        // constructor of each impl arg. A whole-token scan over
                        // the rendered arg over-matches: for `impl Container
                        // [Widget]` it would split `[Widget]` and report
                        // Container as a supertype of `Widget`, when it is the
                        // *list* type that implements Container, not `Widget`.
                        // The head of `Result e` is `Result`; of `[a]` the list
                        // constructor — never the nested element type.
                        let matches_head = args
                            .iter()
                            .any(|t| head_type_name(&t.node).as_deref() == Some(name));
                        if matches_head {
                            push_trait_item(trait_name, state, &mut out);
                        }
                    }
                }
            }
        }
        "ctor" => {
            // Supertype of a constructor = its parent data type.
            if let Some(parent) = data.get("parent").and_then(|v| v.as_str()) {
                push_data_item(parent, state, &mut out);
            }
        }
        _ => {}
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

pub(crate) fn handle_type_hierarchy_subtypes(
    state: &ServerState,
    params: &TypeHierarchySubtypesParams,
) -> Option<Vec<TypeHierarchyItem>> {
    let data = params.item.data.as_ref()?;
    let kind = data.get("kind")?.as_str()?;
    let name = data.get("name")?.as_str()?;
    let mut out = Vec::new();
    match kind {
        "trait" => {
            // Subtypes of a trait = types that implement it.
            for (decl_uri, doc) in &state.documents {
                for decl in &doc.module.decls {
                    if let DeclKind::Impl {
                        trait_name, args, ..
                    } = &decl.node
                        && trait_name == name {
                            let arg_names: Vec<String> = args
                                .iter()
                                .map(|t| crate::type_format::format_type_kind(&t.node))
                                .collect();
                            let label = arg_names.join(" ");
                            out.push(TypeHierarchyItem {
                                name: format!("impl {} {}", name, label),
                                kind: SymbolKind::CLASS,
                                tags: None,
                                detail: Some(uri_to_path(decl_uri)
                                    .map(|p| p.display().to_string())
                                    .unwrap_or_default()),
                                uri: decl_uri.clone(),
                                range: span_to_range(decl.span, &doc.source),
                                selection_range: span_to_range(decl.span, &doc.source),
                                data: Some(serde_json::json!({
                                    "kind": "impl",
                                    "trait": name,
                                    "args": label,
                                })),
                            });
                        }
                }
            }
        }
        "data" => {
            // Subtypes of a data type = its constructors.
            for (decl_uri, doc) in &state.documents {
                for decl in &doc.module.decls {
                    if let DeclKind::Data {
                        name: dn,
                        constructors,
                        ..
                    } = &decl.node
                        && dn == name {
                            for c in constructors {
                                out.push(TypeHierarchyItem {
                                    name: c.name.clone(),
                                    kind: SymbolKind::CONSTRUCTOR,
                                    tags: None,
                                    detail: Some(format!("constructor of {dn}")),
                                    uri: decl_uri.clone(),
                                    range: span_to_range(decl.span, &doc.source),
                                    selection_range: span_to_range(decl.span, &doc.source),
                                    data: Some(serde_json::json!({
                                        "kind": "ctor",
                                        "name": c.name,
                                        "parent": dn,
                                    })),
                                });
                            }
                        }
                }
            }
        }
        _ => {}
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

/// The head type constructor of a type expression: `Foo` for `Foo`, `Result`
/// for `Result e a`, the list constructor for `[a]`. Used to decide whether an
/// impl head is *about* a given data type, without matching type arguments
/// nested inside a compound head (which would wrongly attribute the impl to the
/// element/argument type).
fn head_type_name(tk: &knot::ast::TypeKind) -> Option<String> {
    use knot::ast::TypeKind;
    match tk {
        TypeKind::Named(n) => Some(n.clone()),
        TypeKind::App { func, .. } => head_type_name(&func.node),
        TypeKind::Relation(_) => Some("[]".to_string()),
        TypeKind::UnitAnnotated { base, .. } => head_type_name(&base.node),
        TypeKind::Refined { base, .. } => head_type_name(&base.node),
        _ => None,
    }
}

fn push_trait_item(name: &str, state: &ServerState, out: &mut Vec<TypeHierarchyItem>) {
    for (decl_uri, doc) in &state.documents {
        for decl in &doc.module.decls {
            if let DeclKind::Trait { name: tn, .. } = &decl.node
                && tn == name {
                    out.push(TypeHierarchyItem {
                        name: format!("trait {tn}"),
                        kind: SymbolKind::INTERFACE,
                        tags: None,
                        detail: Some("trait".into()),
                        uri: decl_uri.clone(),
                        range: span_to_range(decl.span, &doc.source),
                        selection_range: span_to_range(decl.span, &doc.source),
                        data: Some(serde_json::json!({"kind": "trait", "name": tn})),
                    });
                    return;
                }
        }
    }
}

fn push_data_item(name: &str, state: &ServerState, out: &mut Vec<TypeHierarchyItem>) {
    for (decl_uri, doc) in &state.documents {
        for decl in &doc.module.decls {
            if let DeclKind::Data { name: dn, .. } = &decl.node
                && dn == name {
                    out.push(TypeHierarchyItem {
                        name: format!("data {dn}"),
                        kind: SymbolKind::CLASS,
                        tags: None,
                        detail: Some("data type".into()),
                        uri: decl_uri.clone(),
                        range: span_to_range(decl.span, &doc.source),
                        selection_range: span_to_range(decl.span, &doc.source),
                        data: Some(serde_json::json!({"kind": "data", "name": dn})),
                    });
                    return;
                }
        }
    }
}

// `position_to_offset` is needed by callers that want to anchor follow-up
// requests at the original cursor; not used here directly but kept as a
// future hook for symbol-vs-cursor disambiguation.
#[allow(dead_code)]
fn _ensure_position_to_offset(s: &str, p: Position) -> usize {
    position_to_offset(s, p)
}
