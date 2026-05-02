//! `textDocument/codeLens` handler. Surfaces reference counts, lineage info,
//! route URLs, and impl counts.

use std::collections::HashMap;

use lsp_types::*;

use knot::ast::DeclKind;

use crate::shared::{format_route_path, http_method_str, plural, route_is_listened};
use crate::state::ServerState;
use crate::utils::span_to_range;

// ── Code Lens ───────────────────────────────────────────────────────

pub(crate) fn handle_code_lens(
    state: &ServerState,
    params: &CodeLensParams,
) -> Option<Vec<CodeLens>> {
    let uri = &params.text_document.uri;
    let doc = state.documents.get(uri)?;
    let mut lenses = Vec::new();

    // Lineage: for each relation (source/view/derived), find the consumers and
    // producers using the per-decl effect sets.
    //   readers[name] → list of (consumer_name, consumer_kind)
    //   writers[name] → list of writer decl names
    // Built once per request; small enough that O(n × m) is fine.
    let mut readers: HashMap<&str, Vec<(&str, &str)>> = HashMap::new();
    let mut writers: HashMap<&str, Vec<&str>> = HashMap::new();
    for d in &doc.module.decls {
        let (name, kind) = match &d.node {
            DeclKind::Fun { name, .. } => (name.as_str(), "fn"),
            DeclKind::View { name, .. } => (name.as_str(), "view"),
            DeclKind::Derived { name, .. } => (name.as_str(), "derived"),
            _ => continue,
        };
        if let Some(eff) = doc.effect_sets.get(name) {
            for r in &eff.reads {
                readers.entry(r.as_str()).or_default().push((name, kind));
            }
            for w in &eff.writes {
                writers.entry(w.as_str()).or_default().push(name);
            }
        }
    }

    for decl in &doc.module.decls {
        match &decl.node {
            DeclKind::Fun { .. }
            | DeclKind::Source { .. }
            | DeclKind::View { .. }
            | DeclKind::Derived { .. }
            | DeclKind::Data { .. }
            | DeclKind::Trait { .. }
            | DeclKind::Route { .. } => {}
            _ => continue,
        }

        // Collect reference locations for this declaration
        let ref_locations: Vec<Location> = doc
            .references
            .iter()
            .filter(|(_, def)| *def == decl.span)
            .map(|(usage, _)| Location {
                uri: uri.clone(),
                range: span_to_range(*usage, &doc.source),
            })
            .collect();
        let ref_count = ref_locations.len();

        let range = span_to_range(decl.span, &doc.source);
        let title = if ref_count == 1 {
            "1 reference".to_string()
        } else {
            format!("{ref_count} references")
        };

        lenses.push(CodeLens {
            range: Range {
                start: range.start,
                end: range.start,
            },
            command: Some(Command {
                title,
                command: "editor.action.showReferences".to_string(),
                arguments: Some(vec![
                    serde_json::to_value(uri.as_str()).unwrap(),
                    serde_json::to_value(range.start).unwrap(),
                    serde_json::to_value(&ref_locations).unwrap(),
                ]),
            }),
            data: None,
        });

        // Effect summary lens: surface inferred IO/relation effects inline so
        // the user sees at a glance whether a function reads/writes relations
        // or performs IO. Effects are central to knot's semantics — a `set`
        // hidden behind two helper layers is easy to miss without this.
        // Suppress the lens for pure-by-construction decl kinds (sources,
        // data, traits) where the effect summary would be noise.
        if matches!(
            &decl.node,
            DeclKind::Fun { .. } | DeclKind::View { .. } | DeclKind::Derived { .. }
        ) {
            let name = match &decl.node {
                DeclKind::Fun { name, .. }
                | DeclKind::View { name, .. }
                | DeclKind::Derived { name, .. } => name.as_str(),
                _ => "",
            };
            if let Some(effects) = doc.effect_info.get(name) {
                lenses.push(CodeLens {
                    range: Range {
                        start: range.start,
                        end: range.start,
                    },
                    command: Some(Command {
                        title: format!("effects: {effects}"),
                        command: String::new(),
                        arguments: None,
                    }),
                    data: None,
                });
            }
        }

        // Lineage lens: source declarations show their consumers; views/derived
        // show their producers. The lens command is informational (no nav target),
        // so we use a no-op command name and put the summary in the title.
        match &decl.node {
            DeclKind::Source { name, .. } => {
                let mut view_count = 0;
                let mut derived_count = 0;
                let mut fn_count = 0;
                if let Some(consumers) = readers.get(name.as_str()) {
                    for (_, kind) in consumers {
                        match *kind {
                            "view" => view_count += 1,
                            "derived" => derived_count += 1,
                            "fn" => fn_count += 1,
                            _ => {}
                        }
                    }
                }
                let writer_count = writers.get(name.as_str()).map_or(0, |v| v.len());
                let mut parts = Vec::new();
                if view_count > 0 {
                    parts.push(format!("{view_count} view{}", plural(view_count)));
                }
                if derived_count > 0 {
                    parts.push(format!(
                        "{derived_count} derived"
                    ));
                }
                if fn_count > 0 {
                    parts.push(format!("{fn_count} fn{}", plural(fn_count)));
                }
                if writer_count > 0 {
                    parts.push(format!(
                        "written by {writer_count} decl{}",
                        plural(writer_count)
                    ));
                }
                if !parts.is_empty() {
                    let title = format!("feeds: {}", parts.join(", "));
                    lenses.push(CodeLens {
                        range: Range {
                            start: range.start,
                            end: range.start,
                        },
                        command: Some(Command {
                            title,
                            command: String::new(),
                            arguments: None,
                        }),
                        data: None,
                    });
                }
            }
            DeclKind::Derived { name, .. } | DeclKind::View { name, .. } => {
                if let Some(eff) = doc.effect_sets.get(name) {
                    let mut deps: Vec<String> = Vec::new();
                    for r in &eff.reads {
                        deps.push(format!("*{r}"));
                    }
                    if !deps.is_empty() {
                        let title = format!("depends on: {}", deps.join(", "));
                        lenses.push(CodeLens {
                            range: Range {
                                start: range.start,
                                end: range.start,
                            },
                            command: Some(Command {
                                title,
                                command: String::new(),
                                arguments: None,
                            }),
                            data: None,
                        });
                    }
                }
            }
            DeclKind::Route { name, entries } => {
                // Per-entry URL preview lens, anchored at the route header. Each
                // entry's constructor is also separately hoverable for the same
                // info; this lens makes the URL space visible at a glance.
                for entry in entries {
                    let method = http_method_str(entry.method);
                    let path = format_route_path(entry);
                    lenses.push(CodeLens {
                        range: Range {
                            start: range.start,
                            end: range.start,
                        },
                        command: Some(Command {
                            title: format!("{method} {path} → {}", entry.constructor),
                            command: String::new(),
                            arguments: None,
                        }),
                        data: None,
                    });
                }
                // Dead-route lint: this route is never composed into a `listen`
                // call within the current document. Surface it as a lens so the
                // user can see at a glance.
                if !route_is_listened(&doc.module, name) {
                    lenses.push(CodeLens {
                        range: Range {
                            start: range.start,
                            end: range.start,
                        },
                        command: Some(Command {
                            title: "⚠ no `listen` handler references this route".to_string(),
                            command: String::new(),
                            arguments: None,
                        }),
                        data: None,
                    });
                }
            }
            _ => {}
        }

        // For traits: show implementations with clickable lens
        if let DeclKind::Trait { name, .. } = &decl.node {
            let impl_locations: Vec<Location> = doc
                .module
                .decls
                .iter()
                .filter(|d| matches!(&d.node, DeclKind::Impl { trait_name, .. } if trait_name == name))
                .map(|d| Location {
                    uri: uri.clone(),
                    range: span_to_range(d.span, &doc.source),
                })
                .collect();
            let impl_count = impl_locations.len();
            if impl_count > 0 {
                let title = if impl_count == 1 {
                    "1 implementation".to_string()
                } else {
                    format!("{impl_count} implementations")
                };
                lenses.push(CodeLens {
                    range: Range {
                        start: range.start,
                        end: range.start,
                    },
                    command: Some(Command {
                        title,
                        command: "editor.action.showReferences".to_string(),
                        arguments: Some(vec![
                            serde_json::to_value(uri.as_str()).unwrap(),
                            serde_json::to_value(range.start).unwrap(),
                            serde_json::to_value(&impl_locations).unwrap(),
                        ]),
                    }),
                    data: None,
                });
            }
        }
    }

    Some(lenses)
}
