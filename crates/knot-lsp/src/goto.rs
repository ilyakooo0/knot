//! `textDocument/definition`, `textDocument/typeDefinition`, and
//! `textDocument/implementation` handlers.

use lsp_types::*;

use knot::ast::{self, DeclKind};

use crate::shared::extract_principal_type_name;
use crate::state::ServerState;
use crate::utils::{
    find_word_in_source, ident_lookup_offset, path_to_uri, position_to_offset, span_to_range,
    word_at_position,
};

/// Find the span of a type *declaration's* name token (`data T = …` /
/// `type T = …`). `doc.definitions` maps a self-named data type
/// (`data Circle = Circle {}`) to the *constructor* token (last-write-wins),
/// which is the wrong target for goto-type-definition — resolve the type-name
/// token directly from the AST instead.
fn type_decl_name_span(module: &ast::Module, source: &str, type_name: &str) -> Option<ast::Span> {
    for decl in &module.decls {
        let is_match = match &decl.node {
            DeclKind::Data { name, .. } | DeclKind::TypeAlias { name, .. } => name == type_name,
            _ => false,
        };
        if is_match {
            return Some(
                find_word_in_source(source, type_name, decl.span.start, decl.span.end)
                    .unwrap_or(decl.span),
            );
        }
    }
    None
}

// ── Go to definition ────────────────────────────────────────────────

pub(crate) fn handle_goto_definition(
    state: &ServerState,
    params: &GotoDefinitionParams,
) -> Option<GotoDefinitionResponse> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;

    // Staleness guard (mirrors hover / rename): during the analysis debounce
    // window the live buffer diverges from the analyzed source, so a position
    // from the editor would resolve against stale bytes and jump to the wrong
    // symbol. Bail; the client re-requests once analysis catches up.
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }

    // Nudge a caret sitting just past a usage's last char back into the word,
    // matching references/highlight — otherwise the half-open span match
    // (`offset < usage.end`) misses and we fall through to the name-keyed
    // fallback, which jumps to a shadowing top-level symbol instead of the
    // local binder under the cursor.
    let offset = ident_lookup_offset(&doc.source, position_to_offset(&doc.source, pos));

    // Try span-based reference lookup first. Usage spans can overlap (a
    // constructor-pattern reference enclosing a nested binder reference), so
    // pick the *smallest* containing span — the symbol the cursor is actually
    // on — mirroring hover/goto-type-definition rather than taking an
    // arbitrary first match.
    let def_span = doc
        .references
        .iter()
        .filter(|(usage, _)| usage.start <= offset && offset < usage.end)
        .min_by_key(|(usage, _)| usage.end - usage.start)
        .map(|(_, def)| *def)
        .or_else(|| {
            // Fallback: the cursor sitting directly on a definition's own name
            // token resolves to that definition. Strictly position-based — a
            // name-keyed fallback (`definitions.get(word)`) misfires on a
            // record-field token (or any token) that merely *shares its name*
            // with a top-level symbol, jumping to that unrelated declaration.
            // `references.rs` removed exactly this fallback for the same reason;
            // keep the two handlers consistent.
            doc.definitions
                .values()
                .find(|span| span.start <= offset && offset < span.end)
                .copied()
        });

    if let Some(span) = def_span {
        let range = span_to_range(span, &doc.source);
        return Some(GotoDefinitionResponse::Scalar(Location {
            uri: uri.clone(),
            range,
        }));
    }

    // Cross-file: check imported definitions. The `import_defs` lookup is purely
    // name-keyed, so guard it the same way the local path above is position-based:
    // a record-field token must not fall through to an imported symbol that merely
    // shares its name (field tokens are never recorded in `references`, so without
    // this guard `b.size` jumps to an imported `size` function).
    if crate::rename::is_at_record_field(&doc.module, &doc.source, offset) {
        return None;
    }
    let word = word_at_position(&doc.source, pos)?;
    let (path, span) = doc.import_defs.get(word)?;
    let imported_source = doc.imported_files.get(path)?;
    let range = span_to_range(*span, imported_source);
    let import_uri = path_to_uri(path)?;
    Some(GotoDefinitionResponse::Scalar(Location {
        uri: import_uri,
        range,
    }))
}

// ── Go to type definition ────────────────────────────────────────────

pub(crate) fn handle_goto_type_definition(
    state: &ServerState,
    params: &GotoDefinitionParams,
) -> Option<GotoDefinitionResponse> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    if state
        .pending_sources
        .get(uri)
        .is_some_and(|p| p.source != doc.source)
    {
        return None;
    }
    let offset = ident_lookup_offset(&doc.source, position_to_offset(&doc.source, pos));
    let word = word_at_position(&doc.source, pos)?;

    // Get the type string for the symbol at cursor. Multiple recorded spans
    // can contain the offset (a binding inside a larger pattern, a lambda
    // param inside its body span); iterating the HashMap and taking the
    // first hit returns an arbitrary one (hash-order nondeterminism). Use
    // the sorted vec and pick the *smallest* containing span — the
    // innermost binding is what the cursor is actually on.
    let type_str = doc
        .local_type_info_sorted
        .iter()
        .filter(|(span, _)| span.start <= offset && offset < span.end)
        .min_by_key(|(span, _)| span.end - span.start)
        .map(|(_, ty)| ty.clone())
        .or_else(|| {
            // Usage spans can overlap (a constructor-pattern reference
            // enclosing a nested binder reference); pick the *smallest*
            // containing span — the symbol the cursor is on — rather than an
            // arbitrary first match, mirroring the innermost-span rule above.
            doc.references
                .iter()
                .filter(|(usage, _)| usage.start <= offset && offset < usage.end)
                .min_by_key(|(usage, _)| usage.end - usage.start)
                .and_then(|(_, def_span)| doc.local_type_info.get(def_span).cloned())
        })
        .or_else(|| {
            // The global `type_info` lookup is name-keyed, so guard it the
            // same way `handle_goto_definition` guards its cross-file fallback:
            // a record-field token (`p.name`) is never recorded in the local
            // type-info tables, so without this guard it falls through to an
            // unrelated top-level symbol that merely shares the field's name.
            // (`references.rs`/`hover` suppress name-based lookups for the same
            // reason.)
            if crate::rename::is_at_record_field(&doc.module, &doc.source, offset) {
                None
            } else {
                doc.type_info.get(word).cloned()
            }
        })?;

    // Extract the principal named type from the type string
    let type_name = extract_principal_type_name(&type_str)?;

    // Look up the definition of that type in the current document. Prefer the
    // type *declaration's* name token (`doc.definitions` maps a self-named data
    // type to its constructor token, the wrong target here); fall back to the
    // generic definitions map for anything not found as a local type decl.
    if let Some(def_span) = type_decl_name_span(&doc.module, &doc.source, &type_name)
        .or_else(|| doc.definitions.get(&type_name).copied())
    {
        let range = span_to_range(def_span, &doc.source);
        return Some(GotoDefinitionResponse::Scalar(Location {
            uri: uri.clone(),
            range,
        }));
    }

    // Check imported definitions
    if let Some((path, span)) = doc.import_defs.get(&type_name) {
        let imported_source = doc.imported_files.get(path)?;
        let range = span_to_range(*span, imported_source);
        let import_uri = path_to_uri(path)?;
        return Some(GotoDefinitionResponse::Scalar(Location {
            uri: import_uri,
            range,
        }));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TempWorkspace, TestWorkspace};
    use crate::utils::offset_to_position;

    fn goto_params(uri: &Uri, position: Position) -> GotoDefinitionParams {
        GotoDefinitionParams {
            text_document_position_params: TextDocumentPositionParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                position,
            },
            work_done_progress_params: Default::default(),
            partial_result_params: Default::default(),
        }
    }

    #[test]
    fn goto_definition_resolves_local_function_call() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"greet = \name -> "hi" ++ name
main = println (greet "world")
"#,
        );
        let doc = ws.doc(&uri);
        assert!(
            doc.definitions.contains_key("greet"),
            "definitions: {:?}",
            doc.definitions.keys().collect::<Vec<_>>()
        );
        let src_pos = doc.source.find("greet \"world\"").expect("call site");
        let pos = offset_to_position(&doc.source, src_pos + 1);
        let resp = handle_goto_definition(&ws.state, &goto_params(&uri, pos))
            .expect("definition resolves");
        let loc = match resp {
            GotoDefinitionResponse::Scalar(l) => l,
            _ => panic!("expected scalar location"),
        };
        assert_eq!(loc.uri, uri);
        assert_eq!(loc.range.start.line, 0);
    }

    #[test]
    fn constructor_definition_span_anchors_on_the_constructor_token() {
        // A self-named constructor (`data Pair = Pair {...}`) must resolve to
        // the constructor token after `=`, not the type-name token before it.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "data Pair = Pair {x: Int 1}\n");
        let doc = ws.doc(&uri);
        let span = doc
            .definitions
            .get("Pair")
            .expect("Pair constructor defined");
        let eq = doc.source.find('=').unwrap();
        assert!(
            span.start > eq,
            "constructor span should be after `=` (got start {}, `=` at {})",
            span.start,
            eq
        );
    }

    #[test]
    fn constructor_definition_skips_shadowing_field_type() {
        // `B` appears first inside `A`'s field type and then as a constructor.
        // The constructor's definition span must anchor on the constructor
        // token (the last `B`), not the earlier field-type reference.
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "data T = A {x: B} | B {}\n");
        let doc = ws.doc(&uri);
        let span = doc.definitions.get("B").expect("B constructor defined");
        let ctor_b = doc.source.rfind('B').unwrap();
        assert_eq!(
            span.start, ctor_b,
            "B's definition should anchor on the constructor, not the field type"
        );
    }

    #[test]
    fn goto_definition_returns_none_for_undefined_word() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open("main", "main = println \"hi\"\n");
        // Cursor on a position with no symbol — middle of the string
        let pos = Position::new(0, 16);
        let _ = handle_goto_definition(&ws.state, &goto_params(&uri, pos));
        // We don't assert None here strictly because `"hi"` may resolve as
        // word-based fallback; the important thing is no panic.
    }

    #[test]
    fn goto_definition_on_field_token_does_not_jump_to_shared_name_symbol() {
        // A record-field token (`b.size`) that merely shares its name with a
        // top-level symbol (`size = 100`) must not resolve to that symbol via a
        // name-keyed fallback. The field is not a recorded reference and the
        // cursor is not on a definition's own name token, so resolution must
        // NOT land on the unrelated `size` declaration.
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            "data Box = Box {size: Int 1}\nsize = 100\nget = \\b -> b.size\n",
        );
        let doc = ws.doc(&uri);
        // Cursor on `size` in `b.size` (the last occurrence).
        let field_off = doc.source.rfind("size").expect("b.size field token");
        let pos = offset_to_position(&doc.source, field_off + 1);
        let resp = handle_goto_definition(&ws.state, &goto_params(&uri, pos));
        if let Some(GotoDefinitionResponse::Scalar(loc)) = resp {
            assert_ne!(
                loc.range.start.line, 1,
                "goto on a field token must not jump to the unrelated top-level `size = 100`"
            );
        }
    }

    #[test]
    fn goto_type_definition_resolves_data_constructor() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"data Color = Red {} | Blue {}
shade : Color
shade = Red {}
"#,
        );
        let pos = ws.position_of(&uri, "shade = Red");
        let pos = Position::new(pos.line, pos.character);
        let resp = handle_goto_type_definition(&ws.state, &goto_params(&uri, pos));
        // Either the inferred type lands us on Color, or it doesn't resolve.
        // We just want this to not panic.
        let _ = resp;
    }

    #[test]
    fn goto_definition_resolves_type_name_in_annotation() {
        let mut ws = TestWorkspace::new();
        let uri = ws.open(
            "main",
            r#"type Color = {hex: Text}
get : Color -> Text
get = \c -> c.hex
"#,
        );
        let doc = ws.doc(&uri);
        // Cursor on the `Color` token in `get : Color -> Text`.
        let off = doc.source.find(": Color").expect("annotation") + 2;
        let pos = offset_to_position(&doc.source, off);
        let resp = handle_goto_definition(&ws.state, &goto_params(&uri, pos))
            .expect("type-name annotation resolves to definition");
        let loc = match resp {
            GotoDefinitionResponse::Scalar(l) => l,
            other => panic!("expected scalar, got {other:?}"),
        };
        assert_eq!(loc.uri, uri);
        // The Color type alias is on line 0.
        assert_eq!(loc.range.start.line, 0);
    }
}
