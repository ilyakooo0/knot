//! Regression tests for the sync/state/navigation bug-fix batch:
//! - stale analysis results racing an undo (apply_analysis_result guard)
//! - willRenameFiles edits computed against pending (live) source
//! - trait-bound changes included in signature fingerprints
//! - code-lens reference counts / call-hierarchy prepare comparing against
//!   name-token definition spans
//! - type-hierarchy supertype whole-token matching
//! - goto-type-definition innermost-span selection
//! - document-symbol name-token selection ranges for trait/impl/route children
//! - workspace-diagnostic clearing reports for deleted files

#![cfg(test)]

use std::collections::{HashMap, HashSet};

use lsp_types::*;

use knot::ast::Span;

use crate::analysis::analyze_document;
use crate::state::{AnalysisResult, PendingSource};
use crate::test_support::{TempWorkspace, TestWorkspace};

// ── Finding 1: undo-to-analyzed-text race ───────────────────────────

fn analyze_standalone(uri: &Uri, source: &str) -> crate::state::DocumentState {
    let mut import_cache = HashMap::new();
    let mut inference_cache = HashMap::new();
    analyze_document(uri, source, &mut import_cache, &mut inference_cache)
}

#[test]
fn stale_analysis_result_is_dropped_when_no_pending_entry() {
    // Doc analyzed at S0; an S1 result lands with no pending entry (the
    // editor undid back to S0, whose didChange removed the pending entry via
    // the `unchanged` early return). The S1 result must be dropped — applying
    // it would desync the server text from the editor buffer.
    let s0 = "main = println \"v0\"\n";
    let s1 = "main = println \"v1 edited\"\n";
    let mut ws = TestWorkspace::new();
    let uri = ws.open("main", s0);
    assert!(ws.state.pending_sources.is_empty());

    let stale_doc = analyze_standalone(&uri, s1);
    let (conn, _client) = lsp_server::Connection::memory();
    crate::apply_analysis_result(
        &mut ws.state,
        &conn,
        AnalysisResult {
            uri: uri.clone(),
            version: Some(2),
            doc: stale_doc,
        },
    );

    assert_eq!(
        ws.state.documents.get(&uri).unwrap().source,
        s0,
        "stale S1 result must not replace the S0 document"
    );
}

#[test]
fn matching_pending_analysis_result_is_applied() {
    // Sanity: the normal flow (pending source == result source) still applies.
    let s0 = "main = println \"v0\"\n";
    let s1 = "main = println \"v1 edited\"\n";
    let mut ws = TestWorkspace::new();
    let uri = ws.open("main", s0);
    ws.state.pending_sources.insert(
        uri.clone(),
        PendingSource {
            source: s1.to_string(),
            version: Some(2),
        },
    );

    let fresh_doc = analyze_standalone(&uri, s1);
    let (conn, _client) = lsp_server::Connection::memory();
    crate::apply_analysis_result(
        &mut ws.state,
        &conn,
        AnalysisResult {
            uri: uri.clone(),
            version: Some(2),
            doc: fresh_doc,
        },
    );

    assert_eq!(ws.state.documents.get(&uri).unwrap().source, s1);
    assert!(
        !ws.state.pending_sources.contains_key(&uri),
        "pending entry consumed on apply"
    );
}

// ── Finding 5: willRenameFiles uses pending (live) source ───────────

#[test]
fn will_rename_files_computes_edits_against_pending_source() {
    let mut tmp = TempWorkspace::new();
    let target_uri = tmp.write_and_open("target.knot", "helper = \\x -> x\n");
    let importer_uri =
        tmp.write_and_open("importer.knot", "import ./target\nmain = helper 1\n");

    // Live buffer has a comment line above the import: the import statement
    // now lives on line 1, not line 0 as in the last-analyzed source.
    tmp.workspace.state.pending_sources.insert(
        importer_uri.clone(),
        PendingSource {
            source: "-- moved\nimport ./target\nmain = helper 1\n".to_string(),
            version: Some(2),
        },
    );

    let canonical_root = tmp.root.canonicalize().expect("root exists");
    let new_uri = format!("file://{}", canonical_root.join("moved.knot").display());
    let params = RenameFilesParams {
        files: vec![FileRename {
            old_uri: target_uri.as_str().to_string(),
            new_uri,
        }],
    };
    let edit = crate::handle_will_rename_files(&tmp.workspace.state, &params)
        .expect("rewrite edit produced");
    let changes = edit.changes.expect("changes map");
    let edits = changes.get(&importer_uri).expect("importer has edits");
    assert_eq!(edits.len(), 1);
    assert_eq!(
        edits[0].range.start.line, 1,
        "edit range must target the import's position in the PENDING source"
    );
    assert_eq!(edits[0].new_text, "./moved");
}

// ── Finding 3: trait bounds participate in signature fingerprint ─────

#[test]
fn signature_fingerprint_detects_trait_bound_change() {
    fn parse_module(src: &str) -> knot::ast::Module {
        let lex = knot::lexer::Lexer::new(src);
        let (tokens, _) = lex.tokenize();
        let parser = knot::parser::Parser::new(src.to_string(), tokens);
        let (m, _) = parser.parse_module();
        m
    }
    let a = parse_module("render : a -> Text\nrender = \\x -> show x\n");
    let b = parse_module("render : Display a => a -> Text\nrender = \\x -> show x\n");
    let fa = crate::incremental::ModuleFingerprint::from_module(&a);
    let fb = crate::incremental::ModuleFingerprint::from_module(&b);
    let sig_changed = fb.signature_changed_decls(&fa);
    assert!(
        sig_changed.contains("render"),
        "adding a trait bound must change the signature hash; got: {sig_changed:?}"
    );
}

// ── Finding 6: code-lens reference counts ────────────────────────────

#[test]
fn code_lens_counts_references_to_used_function() {
    let mut ws = TestWorkspace::new();
    let uri = ws.open(
        "main",
        "greet = \\name -> \"hi \" ++ name\nmain = println (greet \"world\")\n",
    );
    let params = CodeLensParams {
        text_document: TextDocumentIdentifier { uri: uri.clone() },
        work_done_progress_params: Default::default(),
        partial_result_params: Default::default(),
    };
    let lenses =
        crate::code_lens::handle_code_lens(&ws.state, &params).expect("lenses returned");
    // The reference lens for `greet` (declared on line 0) must report the
    // call site in `main`.
    let greet_ref_lens = lenses
        .iter()
        .find(|l| {
            l.range.start.line == 0
                && l.command
                    .as_ref()
                    .map(|c| c.command == "editor.action.showReferences")
                    .unwrap_or(false)
        })
        .expect("reference lens for greet");
    let title = &greet_ref_lens.command.as_ref().unwrap().title;
    assert_eq!(
        title, "1 reference",
        "lens must count the call site (was '0 references' before fix)"
    );
}

// ── Finding 7: call-hierarchy prepare from a call site ───────────────

#[test]
fn call_hierarchy_prepare_works_from_call_site() {
    let mut ws = TestWorkspace::new();
    let uri = ws.open(
        "main",
        "greet = \\name -> \"hi \" ++ name\nmain = println (greet \"world\")\n",
    );
    let doc = ws.doc(&uri);
    let call_offset = doc.source.find("greet \"world\"").expect("call site") + 1;
    let pos = crate::utils::offset_to_position(&doc.source, call_offset);
    let params = CallHierarchyPrepareParams {
        text_document_position_params: TextDocumentPositionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            position: pos,
        },
        work_done_progress_params: Default::default(),
    };
    let items = crate::call_hierarchy::handle_call_hierarchy_prepare(&ws.state, &params)
        .expect("prepare resolves from a call site");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].name, "greet");
}

#[test]
fn call_hierarchy_incoming_excludes_self_declaration_token() {
    // A function written with a separate type-signature line parses into a
    // single `DeclKind::Fun` whose span covers both lines; the body-line name
    // token (`inc =`) is recorded as a self-reference to the canonical
    // definition span. Incoming-calls must not treat that declaration token as
    // a call site, i.e. `inc` must not appear to call itself.
    let mut ws = TestWorkspace::new();
    let uri = ws.open(
        "main",
        "inc : Int -> Int\ninc = \\x -> x + 1\nmain = println (show (inc 41))\n",
    );
    let doc = ws.doc(&uri);
    let inc_def = *doc.definitions.get("inc").expect("inc defined");
    let params = CallHierarchyIncomingCallsParams {
        item: CallHierarchyItem {
            name: "inc".to_string(),
            kind: SymbolKind::FUNCTION,
            tags: None,
            detail: None,
            uri: uri.clone(),
            range: crate::utils::span_to_range(inc_def, &doc.source),
            selection_range: crate::utils::span_to_range(inc_def, &doc.source),
            data: None,
        },
        work_done_progress_params: Default::default(),
        partial_result_params: Default::default(),
    };
    let incoming = crate::call_hierarchy::handle_call_hierarchy_incoming(&ws.state, &params)
        .expect("inc has an incoming call from main");
    let callers: Vec<&str> = incoming.iter().map(|c| c.from.name.as_str()).collect();
    assert!(
        !callers.contains(&"inc"),
        "inc must not appear as its own caller (declaration token leaked as a call site): {callers:?}"
    );
    assert!(
        callers.contains(&"main"),
        "the genuine caller `main` must be present: {callers:?}"
    );
}

// ── Finding 8: type-hierarchy whole-token supertype match ────────────

fn th_supertypes_params(uri: &Uri, kind: &str, name: &str) -> TypeHierarchySupertypesParams {
    TypeHierarchySupertypesParams {
        item: TypeHierarchyItem {
            name: name.to_string(),
            kind: SymbolKind::CLASS,
            tags: None,
            detail: None,
            uri: uri.clone(),
            range: Range::default(),
            selection_range: Range::default(),
            data: Some(serde_json::json!({"kind": kind, "name": name})),
        },
        work_done_progress_params: Default::default(),
        partial_result_params: Default::default(),
    }
}

#[test]
fn type_hierarchy_supertypes_require_whole_token_match() {
    let mut ws = TestWorkspace::new();
    let uri = ws.open(
        "main",
        r#"data Id = MkId {v: Int}
data UserId = MkUserId {v: Int}
trait Display a where
  display : a -> Text
impl Display UserId where
  display x = "user"
"#,
    );

    // `UserId` implements Display — supertypes must include the trait.
    let user_id = crate::type_hierarchy::handle_type_hierarchy_supertypes(
        &ws.state,
        &th_supertypes_params(&uri, "data", "UserId"),
    )
    .expect("UserId has Display as supertype");
    assert!(
        user_id.iter().any(|i| i.name.contains("Display")),
        "got: {:?}",
        user_id.iter().map(|i| &i.name).collect::<Vec<_>>()
    );

    // `Id` does NOT implement Display; the substring match
    // ("UserId".contains("Id")) used to report it anyway.
    let id = crate::type_hierarchy::handle_type_hierarchy_supertypes(
        &ws.state,
        &th_supertypes_params(&uri, "data", "Id"),
    );
    assert!(
        id.is_none(),
        "Id must not inherit Display via substring match; got: {:?}",
        id.map(|v| v.iter().map(|i| i.name.clone()).collect::<Vec<_>>())
    );
}

// ── Finding 9: goto type definition picks the innermost span ─────────

#[test]
fn goto_type_definition_selects_innermost_containing_span() {
    let mut ws = TestWorkspace::new();
    let uri = ws.open(
        "main",
        r#"type Inner = {x: Int}
type Outer = {y: Int}
check = \v -> v
"#,
    );
    // Synthesize overlapping local-type spans around the lambda param `v`:
    // a wide span typed `Outer` and a narrow (innermost) span typed `Inner`.
    // The handler must use the sorted list and pick the smallest span.
    let v_offset = {
        let doc = ws.doc(&uri);
        doc.source.find("\\v ->").expect("lambda") + 1
    };
    {
        let doc = ws.state.documents.get_mut(&uri).unwrap();
        let wide = Span::new(v_offset.saturating_sub(5), v_offset + 6);
        let narrow = Span::new(v_offset, v_offset + 1);
        doc.local_type_info_sorted = vec![
            (wide, "Outer".to_string()),
            (narrow, "Inner".to_string()),
        ];
        doc.local_type_info.clear();
        doc.local_type_info.insert(wide, "Outer".to_string());
        doc.local_type_info.insert(narrow, "Inner".to_string());
    }
    let doc = ws.doc(&uri);
    let pos = crate::utils::offset_to_position(&doc.source, v_offset);
    let params = GotoDefinitionParams {
        text_document_position_params: TextDocumentPositionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            position: pos,
        },
        work_done_progress_params: Default::default(),
        partial_result_params: Default::default(),
    };
    let resp = crate::goto::handle_goto_type_definition(&ws.state, &params)
        .expect("type definition resolves");
    let loc = match resp {
        GotoDefinitionResponse::Scalar(l) => l,
        other => panic!("expected scalar, got {other:?}"),
    };
    // `Inner` is declared on line 0; `Outer` on line 1. The innermost span's
    // type must win regardless of HashMap iteration order.
    assert_eq!(loc.range.start.line, 0, "must resolve to Inner (line 0)");
}

// ── Finding 10: document-symbol selection ranges on name tokens ──────

#[test]
fn document_symbol_children_select_name_tokens() {
    let mut ws = TestWorkspace::new();
    let uri = ws.open(
        "main",
        r#"trait Display a where
  display : a -> Text
impl Display Int where
  display x = "int"
route Api where
  /things
    GET /count -> Int = GetCount
"#,
    );
    let params = DocumentSymbolParams {
        text_document: TextDocumentIdentifier { uri: uri.clone() },
        work_done_progress_params: Default::default(),
        partial_result_params: Default::default(),
    };
    let resp = crate::document_symbol::handle_document_symbol(&ws.state, &params)
        .expect("symbols returned");
    let nested = match resp {
        DocumentSymbolResponse::Nested(s) => s,
        _ => panic!("expected nested"),
    };

    let trait_sym = nested
        .iter()
        .find(|s| s.name == "Display")
        .expect("trait symbol");
    let method = trait_sym
        .children
        .as_ref()
        .and_then(|c| c.iter().find(|m| m.name == "display"))
        .expect("trait method child");
    assert_eq!(
        method.selection_range.start.line, 1,
        "trait method selection range must sit on its name token (line 1), \
         not the whole trait decl"
    );
    assert_ne!(
        method.selection_range, trait_sym.range,
        "selection range must be narrower than the parent decl span"
    );

    let impl_sym = nested
        .iter()
        .find(|s| s.name.starts_with("impl Display"))
        .expect("impl symbol");
    if let Some(children) = impl_sym.children.as_ref() {
        let m = children.iter().find(|m| m.name == "display").expect("impl method");
        assert_ne!(
            m.selection_range, impl_sym.range,
            "impl method selection range must be its name token"
        );
        assert_eq!(m.selection_range.start.line, 3);
    }

    if let Some(route_sym) = nested.iter().find(|s| s.name == "route Api") {
        if let Some(children) = route_sym.children.as_ref() {
            let entry = children
                .iter()
                .find(|c| c.name == "GetCount")
                .expect("route entry child");
            assert_ne!(
                entry.selection_range, route_sym.range,
                "route entry selection range must be its constructor token"
            );
            assert_eq!(entry.selection_range.start.line, 6);
        }
    }
}

// ── Finding 11: workspace diagnostics clear deleted files ────────────

fn ws_diag_params() -> WorkspaceDiagnosticParams {
    WorkspaceDiagnosticParams {
        identifier: None,
        previous_result_ids: Vec::new(),
        work_done_progress_params: Default::default(),
        partial_result_params: Default::default(),
    }
}

fn report_uris(result: &WorkspaceDiagnosticReportResult) -> Vec<(String, usize)> {
    match result {
        WorkspaceDiagnosticReportResult::Report(r) => r
            .items
            .iter()
            .map(|item| match item {
                WorkspaceDocumentDiagnosticReport::Full(f) => (
                    f.uri.as_str().to_string(),
                    f.full_document_diagnostic_report.items.len(),
                ),
                WorkspaceDocumentDiagnosticReport::Unchanged(u) => {
                    (u.uri.as_str().to_string(), usize::MAX)
                }
            })
            .collect(),
        _ => Vec::new(),
    }
}

#[test]
fn workspace_diagnostics_emit_clearing_report_for_deleted_file() {
    let mut tmp = TempWorkspace::new();
    // Unopened file with a guaranteed parse error.
    let path = tmp.root.join("broken.knot");
    std::fs::write(&path, "main = (1 +\n").expect("write broken file");

    // Pull 1: the broken file is reported with diagnostics.
    let r1 = crate::workspace_diagnostics::handle_workspace_diagnostics(
        &mut tmp.workspace.state,
        &ws_diag_params(),
    );
    let uris1 = report_uris(&r1);
    let broken1 = uris1
        .iter()
        .find(|(u, _)| u.ends_with("broken.knot"))
        .expect("broken file reported on first pull");
    assert!(broken1.1 > 0, "first pull must carry diagnostics");
    assert!(
        tmp.workspace
            .state
            .workspace_diag_reported
            .iter()
            .any(|u| u.as_str().ends_with("broken.knot")),
        "reported-set tracks the erroring file"
    );

    // Delete the file, then pull again: a clearing (empty-items) report must
    // go out exactly once so pull-mode clients drop the stale errors.
    std::fs::remove_file(&path).expect("delete broken file");
    let r2 = crate::workspace_diagnostics::handle_workspace_diagnostics(
        &mut tmp.workspace.state,
        &ws_diag_params(),
    );
    let uris2 = report_uris(&r2);
    let broken2 = uris2
        .iter()
        .find(|(u, _)| u.ends_with("broken.knot"))
        .expect("deleted file must get a clearing report");
    assert_eq!(broken2.1, 0, "clearing report must have empty items");
    assert!(
        !tmp.workspace
            .state
            .workspace_diag_reported
            .iter()
            .any(|u| u.as_str().ends_with("broken.knot")),
        "cleared file must leave the reported set"
    );

    // Pull 3: gone for good — no further reports for the deleted file.
    let r3 = crate::workspace_diagnostics::handle_workspace_diagnostics(
        &mut tmp.workspace.state,
        &ws_diag_params(),
    );
    let uris3 = report_uris(&r3);
    assert!(
        !uris3.iter().any(|(u, _)| u.ends_with("broken.knot")),
        "no repeated clearing reports; got: {uris3:?}"
    );
}

// ── Finding 2 companion: evicted inference keys stay evicted ──────────
// The worker-loop merge itself is exercised end-to-end by the LSP worker
// thread; here we lock in the data-shape assumption the fix relies on —
// inference-cache keys are (path, content_hash) pairs, so a fresh
// computation for changed content never collides with an evicted key.

#[test]
fn inference_cache_keys_are_content_addressed() {
    let a = crate::state::content_hash("source v1");
    let b = crate::state::content_hash("source v2");
    assert_ne!(a, b, "different content must hash to different cache keys");
    let mut keys: HashSet<(std::path::PathBuf, u64)> = HashSet::new();
    keys.insert((std::path::PathBuf::from("/x.knot"), a));
    assert!(!keys.contains(&(std::path::PathBuf::from("/x.knot"), b)));
}

#[test]
fn call_hierarchy_prepare_works_from_end_of_identifier() {
    // Cursor placed exactly AT the end of the identifier (the standard
    // post-typing caret position). Without ident_lookup_offset nudging,
    // the half-open `offset < usage.end` check failed and prepare
    // returned None.
    let mut ws = TestWorkspace::new();
    let uri = ws.open(
        "main",
        "greet = \\name -> \"hi \" ++ name\nmain = println (greet \"world\")\n",
    );
    let doc = ws.doc(&uri);
    let call_start = doc.source.find("greet \"world\"").expect("call site");
    // Position cursor right after the last char of "greet"
    let call_offset = call_start + "greet".len();
    let pos = crate::utils::offset_to_position(&doc.source, call_offset);
    let params = CallHierarchyPrepareParams {
        text_document_position_params: TextDocumentPositionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            position: pos,
        },
        work_done_progress_params: Default::default(),
    };
    let items = crate::call_hierarchy::handle_call_hierarchy_prepare(&ws.state, &params)
        .expect("prepare must resolve when cursor is at end of identifier");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].name, "greet");
}
