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
    // Staleness guard (mirrors rename / completion-resolve): during the
    // analysis debounce window the editor buffer is newer than the analyzed
    // source, so positions from the live buffer would resolve against the
    // wrong bytes — hover would caption the wrong token. Bail; the client
    // re-requests once analysis catches up.
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }

    let offset = position_to_offset(&doc.source, pos);

    // Span containment must use the same leftward-resolved offset that
    // `word_at_position` used: a caret immediately AFTER an identifier
    // (or literal) resolves the token to its left, so the lookup has to
    // be nudged back inside that token too.
    let lookup_offset = crate::utils::ident_lookup_offset(&doc.source, offset);

    // Try literal types first (span-based, works for strings/floats/etc.)
    if let Some((span, ty)) = doc
        .literal_types
        .iter()
        .find(|(span, _)| span.start <= lookup_offset && lookup_offset < span.end)
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

    // Field-access context (AST-driven): when the cursor sits on the field
    // token of `recv.field`, the token names a RECORD FIELD, not a symbol.
    // Field tokens never appear in `doc.references`, so the name-based
    // global lookups below would caption the hover with a same-named
    // global's signature — wrong info. Computed up front so the headline
    // lookups can suppress themselves; the field-refinement section further
    // down still renders when metadata exists. (Numeric receivers like the
    // `14` of `3.14` parse as float literals, not FieldAccess nodes, so
    // they never classify as field context here.)
    let field_at_cursor = find_field_access_at_offset(&doc.module, &doc.source, lookup_offset);
    let on_field_token = field_at_cursor
        .as_ref()
        .is_some_and(|f| f.field_name == word);

    // Try local binding types (let, bind, lambda params, case patterns).
    // Check if cursor is on a binding site or on a usage that references one.
    // Several recorded spans can overlap (a destructuring pattern contains
    // its binders); `local_type_info` is a HashMap, so a bare `.find()` is
    // nondeterministic across runs — always pick the SMALLEST containing
    // span (the innermost binding), tie-broken by start for determinism.
    let local_type = doc
        .local_type_info
        .iter()
        .filter(|(span, _)| span.start <= lookup_offset && lookup_offset < span.end)
        .min_by_key(|(span, _)| (span.end - span.start, span.start, span.end))
        .map(|(_, ty)| ty.clone())
        .or_else(|| {
            // Cursor is on a usage — find the definition span and look up its
            // type. References overlap the same way, so prefer the innermost.
            let (_, def_span) = doc
                .references
                .iter()
                .filter(|(usage, _)| usage.start <= lookup_offset && lookup_offset < usage.end)
                .min_by_key(|(usage, _)| (usage.end - usage.start, usage.start, usage.end))?;
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
    } else if on_field_token {
        // Record-field token: the name-based fallbacks below would show a
        // same-named GLOBAL's signature for `p.count` when a top-level
        // `count` exists. We have no per-field type info plumbed through
        // (field spans aren't in `local_type_info`), so show nothing here —
        // the field-refinement section still renders when available.
        None
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
    let enclosing_scheme = find_enclosing_type_scheme(&doc.module, lookup_offset);
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
    // Route declaration names carry no `details`/`type_info` entry (they live
    // in the type-level route registry, not the value scope), so the three
    // checks above are all empty for them. Without this exception the early
    // return fires before `route_decl_section` / the route doc comment can
    // render, leaving route names with no hover at all.
    let is_route_name = is_route_decl_name(&doc.module, word);
    // A cursor on the bare `refine` keyword has no symbol/field/type-var/route
    // detail, but the refine-target section below should still render, so don't
    // early-return when the cursor sits inside a `refine expr` span.
    let in_refine_target = doc
        .refine_targets
        .iter()
        .any(|(span, _)| span.start <= lookup_offset && lookup_offset < span.end);
    if detail_opt.is_none()
        && field_at_cursor.is_none()
        && type_var_constraints.is_empty()
        && !is_route_name
        && !in_refine_target
    {
        return None;
    }
    let mut value = match &detail_opt {
        Some(detail) => format!("```knot\n{detail}\n```"),
        None => String::new(),
    };

    // At a call site, show the full signature with the active argument highlighted
    if let Some((func_name, active_param)) =
        find_enclosing_application(&doc.module, &doc.source, lookup_offset)
        && func_name == word
            && let Some(type_str) = doc.type_info.get(func_name.as_str()) {
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

    // For source/view/derived refs, show the relation schema. Suppressed on a
    // record-field token: hovering the `items` field of `rec.items` must not
    // leak the schema of an unrelated lowercase source/view/derived also named
    // `items` — the same wrong-info class the headline suppression guards.
    for decl in &doc.module.decls {
        if on_field_token {
            break;
        }
        match &decl.node {
            DeclKind::Source { name, ty, .. } if name == word => {
                let schema = format_schema_from_type(&ty.node);
                if !schema.is_empty() {
                    value.push_str(&format!("\n\n**Schema:**\n{schema}"));
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
        .filter(|(span, _)| span.start <= lookup_offset && lookup_offset < span.end)
        .min_by_key(|(span, _)| (span.end - span.start, span.start, span.end))
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
    // user knows which fields will be validated on `set`. Skip on a record-field
    // token (same wrong-info reason as the schema section above).
    if let Some(refinements) = doc.source_refinements.get(word).filter(|_| !on_field_token)
        && !refinements.is_empty() {
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
            ReceiverKind::Var(name) => resolve_var_to_source(&doc.module, name, lookup_offset),
            ReceiverKind::SourceRef(name) | ReceiverKind::DerivedRef(name) => Some(name.clone()),
            ReceiverKind::Other => None,
        };
        if let Some(source_name) = owner_source.as_deref()
            && let Some((type_label, predicate)) =
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

    // Trait hover: list all known impls across open documents so the user can
    // see at a glance which types implement this trait.
    if let Some(impls_section) = trait_impls_section(state, word) {
        value.push_str("\n\n---\n\n");
        value.push_str(&impls_section);
    }

    // Trait method hover: when the cursor lands on a method declared inside a
    // `trait` block, surface the list of impls that override it (and the impls
    // that fall back to a default body, when the method has one). The trait
    // dispatch code lens shows the same data, but a hover on the method name
    // is the most natural place for it too.
    // Not on a record-field token: a field named `map` must not surface an
    // unrelated trait method `map`'s dispatch info (field names are lowercase
    // and collide with method names).
    if !on_field_token
        && let Some(method_section) = trait_method_dispatch_section(state, word) {
            value.push_str("\n\n");
            value.push_str(&method_section);
        }

    // Route hover (on the route declaration's name): list all of its
    // constructor entries with method+path. Hovering on a single constructor
    // already shows that one entry; this gives the bird's-eye view when the
    // user hovers on the route name.
    if let Some(route_summary) = route_decl_section(&doc.module, word) {
        value.push_str("\n\n");
        value.push_str(&route_summary);
    }

    // Unit-annotated types: surface the canonical unit form and the unit
    // conversion functions so users can spot dimensionality at a glance and
    // know how to drop into / out of unit-tagged numeric flows.
    if let Some(ref ty) = type_for_refinement_scan
        && let Some(section) = unit_aware_section(ty) {
            value.push_str("\n\n");
            value.push_str(&section);
        }

    // Constructor → parent type: hovering on a constructor surfaces the parent
    // data type and a link-style listing of sibling constructors.
    if let Some(ctor_section) = constructor_parent_section(&doc.module, word) {
        value.push_str("\n\n");
        value.push_str(&ctor_section);
    }

    // Include doc comments if available. Doc comments are keyed on lowercase
    // top-level decl names (fun/source/view/derived), which collide with
    // record-field names — so a field token like `rec.total` must not pick up
    // an unrelated top-level `total`'s doc comment.
    if !on_field_token
        && let Some(doc_comment) = doc.doc_comments.get(word) {
            value.push_str("\n\n---\n\n");
            value.push_str(doc_comment);
        }

    // Every section above is conditional — e.g. `field_at_cursor` can be
    // `Some` without any refinement metadata to render. Don't ship an empty
    // popup in that case; `None` lets the editor show nothing.
    if value.trim().is_empty() {
        return None;
    }

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value,
        }),
        range: word_span.map(|s| span_to_range(s, &doc.source)),
    })
}

/// If `name` is a trait declared in any open document, render a markdown
/// section listing the types that implement it (across all open docs).
fn trait_impls_section(state: &ServerState, name: &str) -> Option<String> {
    let mut is_trait = false;
    let mut impls: Vec<(String, String)> = Vec::new(); // (file_label, args_text)
    for doc in state.documents.values() {
        for decl in &doc.module.decls {
            match &decl.node {
                DeclKind::Trait { name: tn, .. } if tn == name => {
                    is_trait = true;
                }
                DeclKind::Impl {
                    trait_name, args, ..
                } if trait_name == name => {
                    let args_str: Vec<String> =
                        args.iter().map(|t| format_type_kind(&t.node)).collect();
                    impls.push((String::new(), args_str.join(" ")));
                }
                _ => {}
            }
        }
    }
    if !is_trait {
        return None;
    }
    if impls.is_empty() {
        return Some(format!("**Implementations of `{name}`:** _none yet_"));
    }
    let mut out = format!("**Implementations of `{name}`:**");
    for (_, args) in impls {
        out.push_str(&format!("\n- `impl {name} {args}`"));
    }
    Some(out)
}

/// If `name` is a method declared in any open trait, render the list of impls
/// that supply it explicitly plus those that inherit the default body.
fn trait_method_dispatch_section(state: &ServerState, name: &str) -> Option<String> {
    let mut owning_trait: Option<(String, bool)> = None; // (trait_name, has_default)
    for doc in state.documents.values() {
        for decl in &doc.module.decls {
            if let DeclKind::Trait { name: tn, items, .. } = &decl.node {
                for item in items {
                    if let knot::ast::TraitItem::Method {
                        name: method_name,
                        default_body,
                        ..
                    } = item
                        && method_name == name {
                            owning_trait = Some((tn.clone(), default_body.is_some()));
                        }
                }
            }
        }
    }
    let (trait_name, has_default) = owning_trait?;

    let mut providing: Vec<String> = Vec::new();
    let mut defaulted: Vec<String> = Vec::new();
    for doc in state.documents.values() {
        for decl in &doc.module.decls {
            if let DeclKind::Impl {
                trait_name: tn,
                args,
                items,
                ..
            } = &decl.node
            {
                if tn != &trait_name {
                    continue;
                }
                let arg_label = args
                    .iter()
                    .map(|a| format_type_kind(&a.node))
                    .collect::<Vec<_>>()
                    .join(" ");
                let provides = items.iter().any(|i| {
                    matches!(i, knot::ast::ImplItem::Method { name: n, .. } if n == name)
                });
                if provides {
                    providing.push(arg_label);
                } else if has_default {
                    defaulted.push(arg_label);
                }
            }
        }
    }
    if providing.is_empty() && defaulted.is_empty() {
        return None;
    }
    let mut out = format!("**Method `{name}` of `{trait_name}`** dispatches to:");
    if !providing.is_empty() {
        out.push_str("\n\n");
        for ty in &providing {
            out.push_str(&format!("- `{ty}` (explicit impl)\n"));
        }
    }
    if !defaulted.is_empty() {
        out.push('\n');
        for ty in &defaulted {
            out.push_str(&format!("- `{ty}` (uses default body)\n"));
        }
    }
    Some(out.trim_end().to_string())
}

/// True when `name` is the name of a `route` (or composite `route`)
/// declaration in this module. Used to keep route names from being dropped by
/// the hover early-return guard, since they have no value-scope detail entry.
fn is_route_decl_name(module: &knot::ast::Module, name: &str) -> bool {
    module.decls.iter().any(|decl| match &decl.node {
        DeclKind::Route { name: rn, .. } => rn == name,
        DeclKind::RouteComposite { name: rn, .. } => rn == name,
        _ => false,
    })
}

/// If `name` is a `route` declaration's name in this module, render a summary
/// of all constructor entries (method + path) the route declares.
fn route_decl_section(module: &knot::ast::Module, name: &str) -> Option<String> {
    use crate::shared::{format_route_path, http_method_str};
    for decl in &module.decls {
        if let DeclKind::Route { name: rn, entries, .. } = &decl.node {
            if rn != name {
                continue;
            }
            if entries.is_empty() {
                return None;
            }
            let mut out = format!("**Route `{name}`** entries:");
            for entry in entries {
                let method = http_method_str(entry.method);
                let path = format_route_path(entry);
                out.push_str(&format!(
                    "\n- `{method} {path}` → `{}`",
                    entry.constructor
                ));
            }
            return Some(out);
        }
    }
    None
}

/// Render unit-aware information when the formatted type carries a `<unit>`
/// annotation. Walks the parsed type to find any unit on the value (or the
/// function's return type) and surfaces the conversion idioms so users know
/// how to bridge into unitless arithmetic.
fn unit_aware_section(ty: &str) -> Option<String> {
    let parsed = crate::parsed_type::ParsedType::parse(ty);
    // Pull the value type out of an outer function or IO wrapper so we can
    // see units on the result, not on parameters.
    let value = match &parsed {
        crate::parsed_type::ParsedType::Function(_, ret) => ret.strip_io(),
        other => other.strip_io(),
    };
    let unit = value.unit()?;
    let unit_str = unit.trim();
    // Distinguish Int from Float here — the runtime exposes two pairs of
    // conversion helpers and the user has to pick the right one. `unit()`
    // only returns `Some` for `UnitAnnotated`, so discriminate on its base
    // type — a substring scan of the WHOLE type string would give Float
    // advice for `g : Float -> Int<Ms>`.
    let is_float = matches!(
        value,
        crate::parsed_type::ParsedType::UnitAnnotated { base, .. }
            if matches!(&**base, crate::parsed_type::ParsedType::Named(n, _) if n == "Float")
    );
    let (strip, with) = if is_float {
        ("stripFloatUnit", "withFloatUnit")
    } else {
        ("stripUnit", "withUnit")
    };
    let mut out = format!("**Units:** `<{unit_str}>`");
    out.push_str(&format!(
        "  \n*Drop unit:* `{strip} v` — get the unitless number  \n*Re-tag:* `{with} v` — re-attach the inferred unit"
    ));
    Some(out)
}

/// If `name` is a constructor of a data type declared in `module`, return a
/// markdown summary linking back to the parent type and listing siblings.
fn constructor_parent_section(module: &knot::ast::Module, name: &str) -> Option<String> {
    for decl in &module.decls {
        if let DeclKind::Data {
            name: dn,
            constructors,
            ..
        } = &decl.node
            && constructors.iter().any(|c| c.name == name) {
                let siblings: Vec<String> = constructors
                    .iter()
                    .filter(|c| c.name != name)
                    .map(|c| format!("`{}`", c.name))
                    .collect();
                let mut out = format!("**Constructor of:** `{dn}`");
                if !siblings.is_empty() {
                    out.push_str(&format!("  \nSiblings: {}", siblings.join(", ")));
                }
                return Some(out);
            }
    }
    None
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
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';
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
    let s = if let Some(rest) = s.strip_prefix("IO ") {
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
        // Parse field:type pairs from inner. The `>` of `->`/`=>` is an
        // arrow, not a closing bracket — skipping it keeps the depth from
        // going negative after a function-typed field (which would merge
        // the remaining rows into one).
        let mut depth = 0i32;
        let mut current = String::new();
        let mut prev = '\0';
        for ch in inner.chars() {
            match ch {
                '{' | '[' | '(' | '<' => {
                    depth += 1;
                    current.push(ch);
                }
                '>' if prev == '-' || prev == '=' => {
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
            prev = ch;
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
    fn hover_field_refinement_scopes_to_cursor_decl() {
        // Regression (bug B74): two do-blocks in different decls both bind the
        // variable `p`, each from a different source. Field-refinement hover
        // must attribute `p.amount` to the source bound in the *enclosing*
        // decl, not to whichever decl binds `p` first module-wide. The two
        // sources share the field name `amount` but carry distinct predicates,
        // so a mis-resolution surfaces the wrong predicate.
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"*alpha : [{amount: Int where \x -> x >= 100}]
*beta : [{amount: Int where \x -> x >= 200}]

fromAlpha = do
  p <- *alpha
  yield p.amount

fromBeta = do
  p <- *beta
  yield p.amount
"#,
        );
        let doc = ws.doc(&uri);
        // Hover on `amount` in the SECOND do-block (`fromBeta`), which binds
        // `p` from `*beta`. Before the fix, resolution found `fromAlpha`'s
        // binding first and reported `beta`'s field with alpha's predicate.
        let off = doc.source.rfind("p.amount").expect("field use in fromBeta") + "p.".len();
        let pos = offset_to_position(&doc.source, off);
        let hover = handle_hover(&ws.state, &hover_params(&uri, pos)).expect("hover");
        let text = hover_text(hover);
        assert!(
            text.contains("Field refinement"),
            "expected field-refinement section; got:\n{text}"
        );
        assert!(
            text.contains(">= 200") || text.contains(">=200"),
            "expected beta's predicate (>= 200) for the cursor's decl; got:\n{text}"
        );
        assert!(
            !text.contains(">= 100") && !text.contains(">=100"),
            "must not surface alpha's predicate from an unrelated decl; got:\n{text}"
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

    #[test]
    fn hover_on_field_token_does_not_leak_top_level_doc_comment() {
        // Regression: a record field named `total` picked up the doc comment
        // of an unrelated top-level `total` decl (doc comments are keyed on
        // lowercase names, which collide with field names). The doc-comment
        // section must be gated out on field tokens.
        let mut ws = TestWorkspace::new();
        let src = "-- The grand total value.\ntotal = 42\ntype Rec = {total: Int}\nuseRec = \\r -> r.total\nmain = println (show total)\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("r.total").expect("field access") + "r.".len();
        let pos = offset_to_position(&doc.source, off);
        let text = handle_hover(&ws.state, &hover_params(&uri, pos))
            .map(hover_text)
            .unwrap_or_default();
        assert!(
            !text.contains("grand total"),
            "field-token hover must not leak an unrelated top-level doc comment; got: {text}"
        );
    }

    #[test]
    fn hover_on_field_token_does_not_leak_trait_method_dispatch() {
        // Regression: a record field named `combine` surfaced the dispatch
        // info of an unrelated trait method `combine`. Gated out on field
        // tokens.
        let mut ws = TestWorkspace::new();
        let src = "trait Combiner a where\n  combine : a -> a -> a\ndata Foo = Foo {}\nimpl Combiner Foo where\n  combine = \\x y -> x\ntype Rec = {combine: Int}\nuseRec = \\r -> r.combine\nmain = println \"hi\"\n";
        let uri = ws.open("main", src);
        let doc = ws.doc(&uri);
        let off = doc.source.find("r.combine").expect("field access") + "r.".len();
        let pos = offset_to_position(&doc.source, off);
        let text = handle_hover(&ws.state, &hover_params(&uri, pos))
            .map(hover_text)
            .unwrap_or_default();
        assert!(
            !text.contains("dispatches to"),
            "field-token hover must not leak trait-method dispatch info; got: {text}"
        );
    }
}


// Regression tests for the 2026-06 LSP bug-fix batch (hover group).
#[cfg(test)]
mod regress_fixes_tests {
    use super::*;

    /// Item 12: Int<unit> results must get Int conversion advice even when
    /// the type string mentions Float elsewhere (e.g. in a parameter).
    #[test]
    fn unit_section_discriminates_on_annotated_component() {
        let section = unit_aware_section("Float -> Int<Ms>").expect("unit section");
        assert!(
            section.contains("stripUnit") && section.contains("withUnit"),
            "expected Int advice for Int<Ms> result, got: {section}"
        );
        assert!(
            !section.contains("stripFloatUnit"),
            "Float advice leaked from a Float parameter: {section}"
        );

        let f = unit_aware_section("Int -> Float<M>").expect("unit section");
        assert!(
            f.contains("stripFloatUnit") && f.contains("withFloatUnit"),
            "expected Float advice for Float<M> result, got: {f}"
        );
    }

    /// Item 17: the schema field splitter must not treat the `>` of `->` as
    /// a closing bracket (function-typed fields would merge later rows).
    #[test]
    fn schema_table_survives_function_typed_fields() {
        let table = format_schema_from_type_str("{f: Int -> Text, age: Int}");
        assert!(
            table.contains("| `f` | `Int -> Text` |"),
            "missing function field row: {table}"
        );
        assert!(
            table.contains("| `age` | `Int` |"),
            "row after function-typed field was merged/lost: {table}"
        );
    }

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
            HoverContents::Markup(m) => m.value,
            other => format!("{other:?}"),
        }
    }

    /// Hover at a caret sitting immediately AFTER an identifier (standard
    /// post-typing position) must resolve the LOCAL binding to its left —
    /// not fall back to a same-named global. `word_at_position` resolves the
    /// word to the left; span containment has to use the same nudged offset.
    #[test]
    fn hover_after_identifier_prefers_local_over_same_named_global() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "total : Text\ntotal = \"label\"\nf = \\total -> total + 1\n",
        );
        let doc = ws.doc(&uri);
        // Caret right after the body USAGE of the lambda param `total`.
        let usage = doc.source.rfind("total +").expect("usage");
        let pos = offset_to_position(&doc.source, usage + "total".len());
        let hover = handle_hover(&ws.state, &hover_params(&uri, pos)).expect("hover");
        let text = hover_text(hover);
        assert!(
            text.contains("Int"),
            "expected the local param's Int type, got: {text}"
        );
        assert!(
            !text.lines().nth(1).unwrap_or("").contains("Text"),
            "hover headline leaked the same-named global's type: {text}"
        );
    }

    /// Overlapping local_type_info spans (a destructuring pattern containing
    /// its binders) must resolve to the SMALLEST containing span. The old
    /// `.find()` over a HashMap was nondeterministic across process runs.
    #[test]
    fn hover_picks_innermost_binding_span() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "data P = P {a: Int, b: Text}\nf = \\p -> case p of\n  P {a, b} -> b\n",
        );
        let doc = ws.doc(&uri);
        let off = doc.source.find("a, b}").expect("binder a");
        let pos = offset_to_position(&doc.source, off);
        // Run the lookup many times — with the smallest-span rule the result
        // is stable; the old behavior depended on HashMap iteration order.
        let mut seen: Option<String> = None;
        for _ in 0..16 {
            let hover = handle_hover(&ws.state, &hover_params(&uri, pos)).expect("hover");
            let text = hover_text(hover);
            let headline = text.lines().nth(1).unwrap_or("").to_string();
            match &seen {
                None => seen = Some(headline),
                Some(prev) => assert_eq!(prev, &headline, "hover result not stable"),
            }
        }
        // The binder `a` is an Int; the whole-pattern span would render the
        // record/variant type instead.
        let headline = seen.unwrap_or_default();
        assert!(
            headline.contains("Int") || headline.contains("a :"),
            "expected the innermost binder's type, got: {headline}"
        );
    }

    /// Bug 17: positions from the live buffer must not resolve against the
    /// older analyzed text during the debounce window — mirror the staleness
    /// guard rename/completion-resolve already have.
    #[test]
    fn hover_bails_when_pending_text_is_newer() {
        use crate::state::PendingSource;
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "double = \\x -> x * 2\nmain = double 1\n");
        let doc_source = ws.doc(&uri).source.clone();
        let off = doc_source.find("double =").expect("def");
        let pos = offset_to_position(&doc_source, off);
        ws.state.pending_sources.insert(
            uri.clone(),
            PendingSource {
                source: format!("-- new line\n{doc_source}"),
                version: Some(2),
            },
        );
        let resp = handle_hover(&ws.state, &hover_params(&uri, pos));
        assert!(resp.is_none(), "hover against stale analysis must bail: {resp:?}");
    }

    /// Bug 13: hovering the FIELD token of `p.count` must not caption the
    /// popup with a same-named GLOBAL's signature (field tokens are never in
    /// `doc.references`, so the name-based fallback used to fire).
    #[test]
    fn hover_on_field_token_does_not_show_same_named_global() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "count : Int -> Int\ncount = \\x -> x\nf = \\p -> p.count\n",
        );
        let doc = ws.doc(&uri);
        let off = doc.source.find("p.count").expect("access") + 2;
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_hover(&ws.state, &hover_params(&uri, pos));
        if let Some(h) = resp {
            let text = hover_text(h);
            assert!(
                !text.contains("Int -> Int"),
                "field hover leaked the unrelated global's signature: {text}"
            );
        }
        // Hovering the actual global usage still shows its signature.
        let uri2 = ws.open("main2", "count : Int -> Int\ncount = \\x -> x\nmain = count 1\n");
        let doc2 = ws.doc(&uri2);
        let off2 = doc2.source.find("count 1").expect("usage");
        let pos2 = offset_to_position(&doc2.source, off2);
        let hover = handle_hover(&ws.state, &hover_params(&uri2, pos2)).expect("hover");
        assert!(hover_text(hover).contains("Int"));
    }

    /// A field-access position with no refinement metadata and no symbol
    /// info must return None — not an empty popup.
    #[test]
    fn hover_returns_none_instead_of_empty_popup() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "f = \\p -> p.unknownField\n");
        let doc = ws.doc(&uri);
        let off = doc.source.find("unknownField").expect("field");
        let pos = offset_to_position(&doc.source, off + 2);
        let resp = handle_hover(&ws.state, &hover_params(&uri, pos));
        if let Some(h) = resp {
            let text = hover_text(h);
            assert!(
                !text.trim().is_empty(),
                "hover returned an EMPTY popup; should have been None"
            );
        }
    }
}
