use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use lsp_server::{Connection, Message, Notification, Request, RequestId, Response};
use lsp_types::notification::Notification as _;
use lsp_types::*;

use knot::ast::{self, DeclKind, Module, Span, TypeKind, TypeScheme};
use knot::diagnostic;

// ── Types ───────────────────────────────────────────────────────────

struct DocumentState {
    source: String,
    module: Module,
    /// Span-based references: (usage_span → definition_span).
    references: Vec<(Span, Span)>,
    /// Fallback name-based definitions for names not covered by AST walk.
    definitions: HashMap<String, Span>,
    details: HashMap<String, String>,
    type_info: HashMap<String, String>,
    /// Span-based type info for local bindings (let, bind, lambda params, case patterns).
    local_type_info: HashMap<Span, String>,
    /// Span-based type info for literal expressions.
    literal_types: Vec<(Span, String)>,
    /// Per-declaration effect info (formatted strings).
    effect_info: HashMap<String, String>,
    knot_diagnostics: Vec<diagnostic::Diagnostic>,
    /// Imported files: canonical path → source text
    imported_files: HashMap<PathBuf, String>,
    /// Definitions from imported files: name → (canonical path, span in that file)
    import_defs: HashMap<String, (PathBuf, Span)>,
    /// Which import path each name originated from (for scoped cross-file matching).
    import_origins: HashMap<String, String>,
    /// Doc comments for declarations: name → comment text.
    doc_comments: HashMap<String, String>,
    /// Keyword/operator token positions for semantic highlighting.
    keyword_tokens: Vec<(Span, u32)>,
}

struct ServerState {
    documents: HashMap<Uri, DocumentState>,
    workspace_root: Option<PathBuf>,
    /// Cached parsed imports: canonical path → (mtime, module, source text).
    import_cache: HashMap<PathBuf, (SystemTime, Module, String)>,
    /// Cached LSP diagnostics for unopened workspace files, keyed by mtime.
    /// Refreshed lazily; stale entries are dropped when mtime advances.
    workspace_diag_cache: HashMap<PathBuf, (SystemTime, Vec<Diagnostic>)>,
}

// ── Semantic token legend ───────────────────────────────────────────

const TOK_NAMESPACE: u32 = 0;
const TOK_TYPE: u32 = 1;
const TOK_STRUCT: u32 = 2;
const TOK_ENUM_MEMBER: u32 = 3;
const TOK_PARAMETER: u32 = 4;
const TOK_VARIABLE: u32 = 5;
const TOK_PROPERTY: u32 = 6;
const TOK_FUNCTION: u32 = 7;
const TOK_KEYWORD: u32 = 8;
const TOK_STRING: u32 = 9;
const TOK_NUMBER: u32 = 10;
const TOK_OPERATOR: u32 = 11;

const MOD_DECLARATION: u32 = 0b0001;
const MOD_READONLY: u32 = 0b0010;
/// Effectful operation (IO, network, fs, clock, random, console).
/// Maps to `async` since it's the closest standard token modifier — many
/// editor themes already color async calls distinctively.
const MOD_EFFECTFUL: u32 = 0b0100;
/// Mutation: writes to a relation (`set *r = ...`, `full set *r = ...`).
const MOD_MUTATION: u32 = 0b1000;

fn semantic_token_legend() -> SemanticTokensLegend {
    SemanticTokensLegend {
        token_types: vec![
            SemanticTokenType::NAMESPACE,    // 0
            SemanticTokenType::TYPE,         // 1
            SemanticTokenType::STRUCT,       // 2
            SemanticTokenType::ENUM_MEMBER,  // 3
            SemanticTokenType::PARAMETER,    // 4
            SemanticTokenType::VARIABLE,     // 5
            SemanticTokenType::PROPERTY,     // 6
            SemanticTokenType::FUNCTION,     // 7
            SemanticTokenType::KEYWORD,      // 8
            SemanticTokenType::STRING,       // 9
            SemanticTokenType::NUMBER,       // 10
            SemanticTokenType::OPERATOR,     // 11
        ],
        token_modifiers: vec![
            SemanticTokenModifier::DECLARATION,  // bit 0
            SemanticTokenModifier::READONLY,     // bit 1
            SemanticTokenModifier::ASYNC,        // bit 2 — used for effectful calls
            SemanticTokenModifier::MODIFICATION, // bit 3 — used for relation mutations
        ],
    }
}

/// Set of builtin function names that perform IO effects.
/// Kept in sync with the builtin tables in `effects.rs`.
const EFFECTFUL_BUILTINS: &[&str] = &[
    // console
    "println", "putLine", "print", "readLine",
    "logInfo", "logWarn", "logError", "logDebug",
    // clock
    "now", "sleep",
    // random
    "randomInt", "randomFloat",
    "generateKeyPair", "generateSigningKeyPair", "encrypt",
    // network
    "listen", "fetch", "fetchWith",
    // fs
    "readFile", "writeFile", "appendFile",
    "fileExists", "removeFile", "listDir",
    // concurrency
    "fork", "retry",
];

// ── Entry point ─────────────────────────────────────────────────────

fn main() {
    eprintln!("knot-lsp starting...");

    let (connection, io_threads) = Connection::stdio();

    let server_capabilities = serde_json::to_value(ServerCapabilities {
        text_document_sync: Some(TextDocumentSyncCapability::Kind(
            TextDocumentSyncKind::INCREMENTAL,
        )),
        document_symbol_provider: Some(OneOf::Left(true)),
        definition_provider: Some(OneOf::Left(true)),
        type_definition_provider: Some(TypeDefinitionProviderCapability::Simple(true)),
        implementation_provider: Some(ImplementationProviderCapability::Simple(true)),
        hover_provider: Some(HoverProviderCapability::Simple(true)),
        completion_provider: Some(CompletionOptions {
            trigger_characters: Some(vec![".".into(), "*".into(), "&".into(), "/".into()]),
            resolve_provider: Some(true),
            ..Default::default()
        }),
        references_provider: Some(OneOf::Left(true)),
        rename_provider: Some(OneOf::Right(RenameOptions {
            prepare_provider: Some(true),
            work_done_progress_options: Default::default(),
        })),
        inlay_hint_provider: Some(OneOf::Left(true)),
        signature_help_provider: Some(SignatureHelpOptions {
            trigger_characters: Some(vec![" ".into(), "(".into()]),
            retrigger_characters: None,
            work_done_progress_options: Default::default(),
        }),
        code_lens_provider: Some(CodeLensOptions {
            resolve_provider: Some(false),
        }),
        semantic_tokens_provider: Some(
            SemanticTokensServerCapabilities::SemanticTokensOptions(SemanticTokensOptions {
                legend: semantic_token_legend(),
                full: Some(SemanticTokensFullOptions::Bool(true)),
                range: None,
                work_done_progress_options: Default::default(),
            }),
        ),
        folding_range_provider: Some(FoldingRangeProviderCapability::Simple(true)),
        selection_range_provider: Some(SelectionRangeProviderCapability::Simple(true)),
        call_hierarchy_provider: Some(CallHierarchyServerCapability::Simple(true)),
        code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
        document_formatting_provider: Some(OneOf::Left(true)),
        document_range_formatting_provider: Some(OneOf::Left(true)),
        document_highlight_provider: Some(OneOf::Left(true)),
        document_link_provider: Some(DocumentLinkOptions {
            resolve_provider: Some(false),
            work_done_progress_options: Default::default(),
        }),
        document_on_type_formatting_provider: Some(DocumentOnTypeFormattingOptions {
            first_trigger_character: "\n".into(),
            more_trigger_character: None,
        }),
        workspace_symbol_provider: Some(OneOf::Left(true)),
        linked_editing_range_provider: Some(
            LinkedEditingRangeServerCapabilities::Simple(true),
        ),
        diagnostic_provider: Some(DiagnosticServerCapabilities::Options(DiagnosticOptions {
            identifier: Some("knot".into()),
            inter_file_dependencies: true,
            workspace_diagnostics: true,
            work_done_progress_options: Default::default(),
        })),
        ..Default::default()
    })
    .unwrap();

    let init_params = match connection.initialize(server_capabilities) {
        Ok(params) => params,
        Err(e) => {
            eprintln!("Initialize error: {e}");
            return;
        }
    };

    eprintln!("knot-lsp initialized");

    let workspace_root = serde_json::from_value::<lsp_types::InitializeParams>(init_params)
        .ok()
        .and_then(|p| {
            p.workspace_folders
                .and_then(|folders| folders.into_iter().next().map(|f| f.uri))
        })
        .and_then(|u| uri_to_path(&u));

    let mut state = ServerState {
        documents: HashMap::new(),
        workspace_root,
        import_cache: HashMap::new(),
        workspace_diag_cache: HashMap::new(),
    };

    // Register for file watcher notifications (.knot files)
    let registration = Registration {
        id: "knot-file-watcher".into(),
        method: "workspace/didChangeWatchedFiles".into(),
        register_options: Some(
            serde_json::to_value(DidChangeWatchedFilesRegistrationOptions {
                watchers: vec![FileSystemWatcher {
                    glob_pattern: GlobPattern::String("**/*.knot".into()),
                    kind: Some(WatchKind::Create | WatchKind::Delete | WatchKind::Change),
                }],
            })
            .unwrap(),
        ),
    };
    let _ = connection.sender.send(Message::Request(Request::new(
        RequestId::from("register-file-watcher".to_string()),
        "client/registerCapability".into(),
        serde_json::to_value(RegistrationParams {
            registrations: vec![registration],
        })
        .unwrap(),
    )));

    for msg in &connection.receiver {
        match msg {
            Message::Request(req) => {
                if connection.handle_shutdown(&req).unwrap_or(false) {
                    return;
                }
                handle_request(&mut state, &connection, req);
            }
            Message::Notification(not) => {
                handle_notification(&mut state, &connection, not);
            }
            Message::Response(_) => {}
        }
    }

    io_threads.join().unwrap();
}

// ── Request dispatch ────────────────────────────────────────────────

fn handle_request(state: &mut ServerState, conn: &Connection, req: Request) {
    let id = req.id.clone();

    if let Some(params) = cast_request::<request::DocumentSymbolRequest>(&req) {
        let result = handle_document_symbol(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::GotoDefinition>(&req) {
        let result = handle_goto_definition(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::GotoTypeDefinition>(&req) {
        let result = handle_goto_type_definition(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::GotoImplementation>(&req) {
        let result = handle_goto_implementation(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::HoverRequest>(&req) {
        let result = handle_hover(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::Completion>(&req) {
        let result = handle_completion(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::References>(&req) {
        let result = handle_references(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::PrepareRenameRequest>(&req) {
        let result = handle_prepare_rename(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::Rename>(&req) {
        let result = handle_rename(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::InlayHintRequest>(&req) {
        let result = handle_inlay_hint(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::SignatureHelpRequest>(&req) {
        let result = handle_signature_help(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::CodeLensRequest>(&req) {
        let result = handle_code_lens(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::SemanticTokensFullRequest>(&req) {
        let result = handle_semantic_tokens_full(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::FoldingRangeRequest>(&req) {
        let result = handle_folding_range(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::SelectionRangeRequest>(&req) {
        let result = handle_selection_range(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::Formatting>(&req) {
        let result = handle_formatting(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::RangeFormatting>(&req) {
        let result = handle_range_formatting(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::OnTypeFormatting>(&req) {
        let result = handle_on_type_formatting(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::DocumentHighlightRequest>(&req) {
        let result = handle_document_highlight(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::DocumentLinkRequest>(&req) {
        let result = handle_document_link(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::CodeActionRequest>(&req) {
        let result = handle_code_action(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::WorkspaceSymbolRequest>(&req) {
        let result = handle_workspace_symbol(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::CallHierarchyPrepare>(&req) {
        let result = handle_call_hierarchy_prepare(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::CallHierarchyIncomingCalls>(&req) {
        let result = handle_call_hierarchy_incoming(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::CallHierarchyOutgoingCalls>(&req) {
        let result = handle_call_hierarchy_outgoing(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::ResolveCompletionItem>(&req) {
        let result = handle_resolve_completion_item(state, params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::LinkedEditingRange>(&req) {
        let result = handle_linked_editing_range(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::WorkspaceDiagnosticRequest>(&req) {
        let result = handle_workspace_diagnostics(state, &params);
        send_response(conn, id, result);
        // Periodically prune the workspace diagnostics cache to avoid
        // unbounded growth when files are deleted from disk.
        prune_stale_workspace_diag_cache(state);
    }
}

fn cast_request<R: request::Request>(req: &Request) -> Option<R::Params> {
    if req.method == R::METHOD {
        serde_json::from_value(req.params.clone()).ok()
    } else {
        None
    }
}

fn send_response<T: serde::Serialize>(conn: &Connection, id: RequestId, result: T) {
    let resp = Response::new_ok(id, serde_json::to_value(result).unwrap());
    conn.sender.send(Message::Response(resp)).unwrap();
}

// ── Notification dispatch ───────────────────────────────────────────

fn handle_notification(state: &mut ServerState, conn: &Connection, not: Notification) {
    if not.method == notification::DidOpenTextDocument::METHOD {
        let params: DidOpenTextDocumentParams = serde_json::from_value(not.params).unwrap();
        let uri = params.text_document.uri.clone();
        let doc = analyze_document(&uri, &params.text_document.text, &mut state.import_cache);
        publish_diagnostics(conn, &uri, &doc);
        state.documents.insert(uri, doc);
    } else if not.method == notification::DidChangeTextDocument::METHOD {
        let params: DidChangeTextDocumentParams = serde_json::from_value(not.params).unwrap();
        let uri = params.text_document.uri.clone();
        // Get current source or start empty
        let mut source = state
            .documents
            .get(&uri)
            .map(|d| d.source.clone())
            .unwrap_or_default();
        // Apply incremental edits (or full replacement if range is None)
        for change in params.content_changes {
            if let Some(range) = change.range {
                let start = position_to_offset(&source, range.start);
                let end = position_to_offset(&source, range.end);
                source.replace_range(start..end, &change.text);
            } else {
                // Full document replacement (fallback)
                source = change.text;
            }
        }
        // Skip re-analysis if the source matches what we already analyzed.
        // Triggered when a no-op edit fires (e.g. format-on-save with no changes).
        let unchanged = state
            .documents
            .get(&uri)
            .map(|d| d.source == source)
            .unwrap_or(false);
        if !unchanged {
            let doc = analyze_document(&uri, &source, &mut state.import_cache);
            publish_diagnostics(conn, &uri, &doc);
            state.documents.insert(uri.clone(), doc);
        }

        // Re-analyze open documents that import from the changed file
        if let Some(changed_path) = uri_to_path(&uri).and_then(|p| p.canonicalize().ok()) {
            let uris_to_refresh: Vec<(Uri, String)> = state
                .documents
                .iter()
                .filter(|(other_uri, other_doc)| {
                    **other_uri != uri
                        && other_doc
                            .imported_files
                            .keys()
                            .any(|p| *p == changed_path)
                })
                .map(|(u, d)| (u.clone(), d.source.clone()))
                .collect();
            for (refresh_uri, refresh_source) in uris_to_refresh {
                let doc =
                    analyze_document(&refresh_uri, &refresh_source, &mut state.import_cache);
                publish_diagnostics(conn, &refresh_uri, &doc);
                state.documents.insert(refresh_uri, doc);
            }
        }
    } else if not.method == notification::DidChangeWatchedFiles::METHOD {
        let params: DidChangeWatchedFilesParams = serde_json::from_value(not.params).unwrap();
        // When .knot files change on disk, re-analyze open documents that might
        // import them (their cross-file navigation data may be stale)
        let changed_paths: HashSet<PathBuf> = params
            .changes
            .iter()
            .filter_map(|c| uri_to_path(&c.uri))
            .filter_map(|p| p.canonicalize().ok())
            .collect();

        if !changed_paths.is_empty() {
            // Re-analyze all open documents whose imports overlap with changed files
            let uris_to_refresh: Vec<(Uri, String)> = state
                .documents
                .iter()
                .filter(|(_, doc)| {
                    doc.imported_files
                        .keys()
                        .any(|p| changed_paths.contains(p))
                })
                .map(|(uri, doc)| (uri.clone(), doc.source.clone()))
                .collect();

            for (uri, source) in uris_to_refresh {
                let doc = analyze_document(&uri, &source, &mut state.import_cache);
                publish_diagnostics(conn, &uri, &doc);
                state.documents.insert(uri, doc);
            }
        }
    } else if not.method == notification::DidCloseTextDocument::METHOD {
        let params: DidCloseTextDocumentParams = serde_json::from_value(not.params).unwrap();
        state.documents.remove(&params.text_document.uri);
        // Clear diagnostics
        let diags = PublishDiagnosticsParams::new(params.text_document.uri, vec![], None);
        let not = Notification::new(notification::PublishDiagnostics::METHOD.into(), diags);
        conn.sender.send(Message::Notification(not)).unwrap();
    }
}

// ── Document analysis ───────────────────────────────────────────────

fn analyze_document(
    uri: &Uri,
    source: &str,
    import_cache: &mut HashMap<PathBuf, (SystemTime, Module, String)>,
) -> DocumentState {
    let mut all_diags = Vec::new();
    let mut type_info = HashMap::new();
    let mut local_type_info = HashMap::new();
    let mut effect_info = HashMap::new();

    // Lex
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, lex_diags) = lexer.tokenize();
    all_diags.extend(lex_diags);

    // Collect keyword/operator positions for semantic tokens before parser consumes them
    let keyword_tokens = collect_keyword_operator_positions(&tokens);

    // Parse
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    all_diags.extend(parse_diags);

    // Build navigation data from original AST
    let (definitions, references, literal_types) = resolve_definitions(&module, source);
    let details = build_details(&module);

    // Extract doc comments from source
    let doc_comments = extract_doc_comments(source, &module);

    // Resolve import navigation (cross-file definitions)
    let (imported_files, import_defs, import_origins) = if let Some(path) = uri_to_path(uri) {
        resolve_import_navigation(&module.imports, &path, import_cache)
    } else {
        (HashMap::new(), HashMap::new(), HashMap::new())
    };

    // Run deeper analysis if no parse errors
    let has_parse_errors = all_diags
        .iter()
        .any(|d| matches!(d.severity, diagnostic::Severity::Error));

    if !has_parse_errors {
        let mut analysis_module = module.clone();

        // Resolve imports
        if let Some(path) = uri_to_path(uri) {
            let _ = knot_compiler::modules::resolve_imports(&mut analysis_module, &path);
        }

        // Inject prelude + desugar
        knot_compiler::base::inject_prelude(&mut analysis_module);
        knot_compiler::desugar::desugar(&mut analysis_module);

        // Type inference
        let (infer_diags, _monad_info, inferred_types, local_types, _refine_targets, _refined_types, _from_json) =
            knot_compiler::infer::check(&analysis_module);
        all_diags.extend(infer_diags);
        type_info = inferred_types;
        local_type_info = local_types;

        // Effect inference
        let (effect_diags, effects) =
            knot_compiler::effects::check_with_effects(&analysis_module);
        all_diags.extend(effect_diags);
        for (name, eff) in &effects {
            if !eff.is_pure() {
                effect_info.insert(name.clone(), format!("{eff}"));
            }
        }

        // Stratification
        all_diags.extend(knot_compiler::stratify::check(&analysis_module));

        // SQL optimization lint
        let type_env = knot_compiler::types::TypeEnv::from_module(&analysis_module);
        all_diags.extend(knot_compiler::sql_lint::check(&analysis_module, &type_env));
    }

    DocumentState {
        source: source.to_string(),
        module,
        references,
        definitions,
        details,
        type_info,
        local_type_info,
        literal_types,
        effect_info,
        knot_diagnostics: all_diags,
        imported_files,
        import_defs,
        import_origins,
        doc_comments,
        keyword_tokens,
    }
}

/// Resolve imported files for cross-file navigation.
fn resolve_import_navigation(
    imports: &[ast::Import],
    source_path: &Path,
    import_cache: &mut HashMap<PathBuf, (SystemTime, Module, String)>,
) -> (HashMap<PathBuf, String>, HashMap<String, (PathBuf, Span)>, HashMap<String, String>) {
    let mut imported_files = HashMap::new();
    let mut import_defs = HashMap::new();
    let mut import_origins = HashMap::new();

    let base_dir = source_path.parent().unwrap_or(Path::new("."));

    for imp in imports {
        let rel_path = PathBuf::from(&imp.path).with_extension("knot");
        let full_path = base_dir.join(&rel_path);

        let canonical = match full_path.canonicalize() {
            Ok(p) => p,
            Err(_) => continue,
        };

        // Check import cache: reuse if mtime hasn't changed
        let current_mtime = std::fs::metadata(&canonical)
            .and_then(|m| m.modified())
            .ok();

        let (module, source) = if let Some(mtime) = current_mtime {
            if let Some((cached_mtime, cached_module, cached_source)) =
                import_cache.get(&canonical)
            {
                if *cached_mtime == mtime {
                    (cached_module.clone(), cached_source.clone())
                } else {
                    let source = match std::fs::read_to_string(&canonical) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    let lexer = knot::lexer::Lexer::new(&source);
                    let (tokens, _) = lexer.tokenize();
                    let parser = knot::parser::Parser::new(source.clone(), tokens);
                    let (module, _) = parser.parse_module();
                    import_cache
                        .insert(canonical.clone(), (mtime, module.clone(), source.clone()));
                    (module, source)
                }
            } else {
                let source = match std::fs::read_to_string(&canonical) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let lexer = knot::lexer::Lexer::new(&source);
                let (tokens, _) = lexer.tokenize();
                let parser = knot::parser::Parser::new(source.clone(), tokens);
                let (module, _) = parser.parse_module();
                import_cache
                    .insert(canonical.clone(), (mtime, module.clone(), source.clone()));
                (module, source)
            }
        } else {
            let source = match std::fs::read_to_string(&canonical) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let lexer = knot::lexer::Lexer::new(&source);
            let (tokens, _) = lexer.tokenize();
            let parser = knot::parser::Parser::new(source.clone(), tokens);
            let (module, _) = parser.parse_module();
            (module, source)
        };

        // Register definitions from this file
        for decl in &module.decls {
            match &decl.node {
                DeclKind::Data {
                    name, constructors, ..
                } => {
                    import_defs.insert(name.clone(), (canonical.clone(), decl.span));
                    import_origins.insert(name.clone(), imp.path.clone());
                    for ctor in constructors {
                        // Find constructor span within the data decl
                        let ctor_span =
                            find_word_in_source(&source, &ctor.name, decl.span.start, decl.span.end)
                                .unwrap_or(decl.span);
                        import_defs.insert(ctor.name.clone(), (canonical.clone(), ctor_span));
                        import_origins.insert(ctor.name.clone(), imp.path.clone());
                    }
                }
                DeclKind::TypeAlias { name, .. }
                | DeclKind::Source { name, .. }
                | DeclKind::View { name, .. }
                | DeclKind::Derived { name, .. }
                | DeclKind::Fun { name, .. }
                | DeclKind::Trait { name, .. }
                | DeclKind::Route { name, .. }
                | DeclKind::RouteComposite { name, .. } => {
                    import_defs.insert(name.clone(), (canonical.clone(), decl.span));
                    import_origins.insert(name.clone(), imp.path.clone());
                }
                DeclKind::Impl { items, .. } => {
                    for item in items {
                        if let ast::ImplItem::Method { name, .. } = item {
                            import_defs.insert(name.clone(), (canonical.clone(), decl.span));
                            import_origins.insert(name.clone(), imp.path.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        imported_files.insert(canonical, source);
    }

    (imported_files, import_defs, import_origins)
}

fn publish_diagnostics(conn: &Connection, uri: &Uri, doc: &DocumentState) {
    let lsp_diags: Vec<Diagnostic> = doc
        .knot_diagnostics
        .iter()
        .filter_map(|d| to_lsp_diagnostic(d, &doc.source, uri))
        .collect();

    let params = PublishDiagnosticsParams::new(uri.clone(), lsp_diags, None);
    let not = Notification::new(
        notification::PublishDiagnostics::METHOD.into(),
        params,
    );
    conn.sender.send(Message::Notification(not)).unwrap();
}

// ── Document symbols (hierarchical) ─────────────────────────────────

fn handle_document_symbol(
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

// ── Go to definition ────────────────────────────────────────────────

fn handle_goto_definition(
    state: &ServerState,
    params: &GotoDefinitionParams,
) -> Option<GotoDefinitionResponse> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;

    let offset = position_to_offset(&doc.source, pos);

    // Try span-based reference lookup first
    let def_span = doc
        .references
        .iter()
        .find(|(usage, _)| usage.start <= offset && offset < usage.end)
        .map(|(_, def)| *def)
        .or_else(|| {
            // Fallback: name-based lookup
            let word = word_at_position(&doc.source, pos)?;
            doc.definitions.get(word).copied()
        });

    if let Some(span) = def_span {
        let range = span_to_range(span, &doc.source);
        return Some(GotoDefinitionResponse::Scalar(Location {
            uri: uri.clone(),
            range,
        }));
    }

    // Cross-file: check imported definitions
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

fn handle_goto_type_definition(
    state: &ServerState,
    params: &GotoDefinitionParams,
) -> Option<GotoDefinitionResponse> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);
    let word = word_at_position(&doc.source, pos)?;

    // Get the type string for the symbol at cursor
    let type_str = doc
        .local_type_info
        .iter()
        .find(|(span, _)| span.start <= offset && offset < span.end)
        .map(|(_, ty)| ty.clone())
        .or_else(|| {
            doc.references
                .iter()
                .find(|(usage, _)| usage.start <= offset && offset < usage.end)
                .and_then(|(_, def_span)| doc.local_type_info.get(def_span).cloned())
        })
        .or_else(|| doc.type_info.get(word).cloned())?;

    // Extract the principal named type from the type string
    let type_name = extract_principal_type_name(&type_str)?;

    // Look up the definition of that type in the current document
    if let Some(def_span) = doc.definitions.get(&type_name) {
        let range = span_to_range(*def_span, &doc.source);
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

// ── Go to implementation ─────────────────────────────────────────────

/// Resolve `textDocument/implementation`:
/// - On a trait name: jump to all `impl Trait ...` blocks across the workspace.
/// - On a trait method name: jump to each impl's version of that method.
/// - On a type name with traits implemented for it: list all impls.
fn handle_goto_implementation(
    state: &ServerState,
    params: &GotoDefinitionParams,
) -> Option<GotoDefinitionResponse> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let word = word_at_position(&doc.source, pos)?;

    let mut locations: Vec<Location> = Vec::new();

    // Helper: collect impls from a parsed module that target a given trait or
    // contain a method of the given name.
    let collect_from_module =
        |module: &Module,
         module_uri: &Uri,
         module_source: &str,
         word: &str,
         locs: &mut Vec<Location>| {
            // Determine what kind of symbol the cursor is on:
            // - trait name: collect every `impl <word> ...` block
            // - method name: for each impl block, find that method and add its span
            let is_trait_name = module.decls.iter().any(|d| {
                matches!(&d.node, DeclKind::Trait { name, .. } if name == word)
            });
            let is_method_name = module.decls.iter().any(|d| {
                if let DeclKind::Trait { items, .. } = &d.node {
                    items.iter().any(|i| {
                        matches!(i, ast::TraitItem::Method { name, .. } if name == word)
                    })
                } else {
                    false
                }
            });
            for decl in &module.decls {
                if let DeclKind::Impl {
                    trait_name, items, ..
                } = &decl.node
                {
                    if is_trait_name && trait_name == word {
                        locs.push(Location {
                            uri: module_uri.clone(),
                            range: span_to_range(decl.span, module_source),
                        });
                    } else if is_method_name {
                        for item in items {
                            if let ast::ImplItem::Method { name, body, .. } = item {
                                if name == word {
                                    // Use the body span as the navigation target
                                    // (keeps the method declaration in view).
                                    locs.push(Location {
                                        uri: module_uri.clone(),
                                        range: span_to_range(body.span, module_source),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        };

    // Phase 1: search the current document
    collect_from_module(&doc.module, uri, &doc.source, word, &mut locations);

    // Phase 2: search all open documents
    for (other_uri, other_doc) in &state.documents {
        if other_uri == uri {
            continue;
        }
        collect_from_module(&other_doc.module, other_uri, &other_doc.source, word, &mut locations);
    }

    // Phase 3: search workspace files not currently open
    if let Some(root) = &state.workspace_root {
        let open_paths: HashSet<PathBuf> = state
            .documents
            .keys()
            .filter_map(|u| uri_to_path(u))
            .filter_map(|p| p.canonicalize().ok())
            .collect();

        if let Ok(files) = scan_knot_files(root) {
            for path in files {
                let canonical = match path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if open_paths.contains(&canonical) {
                    continue;
                }
                let source = match std::fs::read_to_string(&canonical) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                if !source.contains(word) {
                    continue;
                }
                let file_uri = match path_to_uri(&canonical) {
                    Some(u) => u,
                    None => continue,
                };
                let lexer = knot::lexer::Lexer::new(&source);
                let (tokens, _) = lexer.tokenize();
                let parser = knot::parser::Parser::new(source.clone(), tokens);
                let (module, _) = parser.parse_module();
                collect_from_module(&module, &file_uri, &source, word, &mut locations);
            }
        }
    }

    if locations.is_empty() {
        None
    } else if locations.len() == 1 {
        Some(GotoDefinitionResponse::Scalar(locations.into_iter().next().unwrap()))
    } else {
        Some(GotoDefinitionResponse::Array(locations))
    }
}

/// Extract the principal named type from a type string.
/// E.g., "[Person]" -> "Person", "Maybe Text" -> "Maybe",
/// "Int -> Text" -> None (functions have no single type def),
/// "{name: Text}" -> None (anonymous records).
fn extract_principal_type_name(type_str: &str) -> Option<String> {
    let s = type_str.trim();

    // Strip relation brackets: [T] -> T
    if s.starts_with('[') && s.ends_with(']') {
        return extract_principal_type_name(&s[1..s.len() - 1]);
    }

    // Strip IO wrapper: IO {effects} T -> T
    if s.starts_with("IO ") {
        let rest = &s[3..];
        if rest.starts_with('{') {
            if let Some(close) = rest.find('}') {
                return extract_principal_type_name(rest[close + 1..].trim());
            }
        }
        return extract_principal_type_name(rest);
    }

    // Anonymous record — no named type
    if s.starts_with('{') {
        return None;
    }

    // Variant type — no single named type
    if s.starts_with('<') {
        return None;
    }

    // Function type — no single named type
    if s.contains(" -> ") {
        return None;
    }

    // Named type (possibly with params): "Person", "Maybe Text", "Result Text Int"
    // Take the first word as the type name
    let name = s.split_whitespace().next()?;

    // Must start with uppercase to be a concrete type name
    if name.chars().next()?.is_uppercase() {
        Some(name.to_string())
    } else {
        None
    }
}

// ── Hover ───────────────────────────────────────────────────────────

fn handle_hover(state: &ServerState, params: &HoverParams) -> Option<Hover> {
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
        let source_text = &doc.source[span.start..span.end];
        let detail = format!("{source_text} : {ty}");
        return Some(Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::Markdown,
                value: format!("```knot\n{detail}\n```"),
            }),
            range: None,
        });
    }

    let word = word_at_position(&doc.source, pos)?;

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
    let detail = if let Some(ty) = local_type {
        format!("{word} : {ty}")
    } else if let Some(d) = doc.details.get(word) {
        // If we have an inferred type and the AST detail has no type annotation,
        // enhance with the inferred type
        let base = if let Some(inferred) = doc.type_info.get(word) {
            if !d.contains(':') {
                format!("{d} : {inferred}")
            } else {
                d.clone()
            }
        } else {
            d.clone()
        };
        // Append effect info if available
        if let Some(effects) = doc.effect_info.get(word) {
            format!("{base}\n{effects}")
        } else {
            base
        }
    } else if let Some(inferred) = doc.type_info.get(word) {
        let base = format!("{word} : {inferred}");
        if let Some(effects) = doc.effect_info.get(word) {
            format!("{base}\n{effects}")
        } else {
            base
        }
    } else {
        return None;
    };

    let mut value = format!("```knot\n{detail}\n```");

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
        range: None,
    })
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

// ── Completion ──────────────────────────────────────────────────────

fn handle_completion(
    state: &ServerState,
    params: &CompletionParams,
) -> Option<CompletionResponse> {
    let uri = &params.text_document_position.text_document.uri;
    let doc = state.documents.get(uri)?;
    let pos = params.text_document_position.position;

    // Detect trigger context
    let offset = position_to_offset(&doc.source, pos);
    let trigger_char = if offset > 0 {
        doc.source.as_bytes().get(offset - 1).copied()
    } else {
        None
    };

    let mut items = Vec::new();

    // Context-aware: after `*` only suggest source/view names
    if trigger_char == Some(b'*') {
        for decl in &doc.module.decls {
            if let DeclKind::Source { name, .. } | DeclKind::View { name, .. } = &decl.node {
                let detail = doc.type_info.get(name.as_str()).cloned();
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::VARIABLE),
                    detail,
                    ..Default::default()
                });
            }
        }
        return Some(CompletionResponse::Array(items));
    }

    // Context-aware: after `&` only suggest derived names
    if trigger_char == Some(b'&') {
        for decl in &doc.module.decls {
            if let DeclKind::Derived { name, .. } = &decl.node {
                let detail = doc.type_info.get(name.as_str()).cloned();
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::VARIABLE),
                    detail,
                    ..Default::default()
                });
            }
        }
        return Some(CompletionResponse::Array(items));
    }

    // Context-aware: after `/` in an import line, suggest file paths
    if trigger_char == Some(b'/') {
        let line_start = doc.source[..offset].rfind('\n').map(|p| p + 1).unwrap_or(0);
        let line_text = &doc.source[line_start..offset];
        if line_text.trim_start().starts_with("import ") {
            if let Some(source_path) = uri_to_path(uri) {
                if let Some(base_dir) = source_path.parent() {
                    let partial = line_text.trim_start().strip_prefix("import ").unwrap_or("");
                    items.extend(complete_import_path(base_dir, partial));
                }
            }
            return Some(CompletionResponse::Array(items));
        }
    }

    // Context-aware: after `.` suggest record field names from known types
    if trigger_char == Some(b'.') {
        // Try to find the expression before the dot and its type
        let expr_end = offset - 1; // position of the `.`
        let fields = resolve_dot_fields(doc, expr_end);
        if !fields.is_empty() {
            for name in fields {
                items.push(CompletionItem {
                    label: name,
                    kind: Some(CompletionItemKind::FIELD),
                    ..Default::default()
                });
            }
            return Some(CompletionResponse::Array(items));
        }

        // Fallback: all known field names from all types
        let mut all_fields = HashSet::new();
        for decl in &doc.module.decls {
            match &decl.node {
                DeclKind::TypeAlias { ty, .. } => {
                    if let TypeKind::Record { fields: fs, .. } = &ty.node {
                        for f in fs {
                            all_fields.insert(f.name.clone());
                        }
                    }
                }
                DeclKind::Source { ty, .. } => {
                    if let TypeKind::Record { fields: fs, .. } = &ty.node {
                        for f in fs {
                            all_fields.insert(f.name.clone());
                        }
                    }
                }
                DeclKind::Data { constructors, .. } => {
                    for ctor in constructors {
                        for f in &ctor.fields {
                            all_fields.insert(f.name.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        for name in all_fields {
            items.push(CompletionItem {
                label: name,
                kind: Some(CompletionItemKind::FIELD),
                ..Default::default()
            });
        }
        return Some(CompletionResponse::Array(items));
    }

    // General completion: keywords + snippets + declarations + builtins

    // Context detection: if cursor is in a type annotation position (after `:` or `[`),
    // only suggest types and type constructors
    let in_type_context = {
        let before = &doc.source[..offset];
        let trimmed = before.trim_end();
        trimmed.ends_with(':') || trimmed.ends_with('[')
            || trimmed.ends_with("->")
    };

    if in_type_context {
        // Only suggest types: data types, type aliases, built-in types
        for decl in &doc.module.decls {
            match &decl.node {
                DeclKind::Data { name, .. } | DeclKind::TypeAlias { name, .. } => {
                    items.push(CompletionItem {
                        label: name.clone(),
                        kind: Some(CompletionItemKind::STRUCT),
                        detail: doc.details.get(name).cloned(),
                        ..Default::default()
                    });
                }
                DeclKind::Trait { name, .. } => {
                    items.push(CompletionItem {
                        label: name.clone(),
                        kind: Some(CompletionItemKind::INTERFACE),
                        ..Default::default()
                    });
                }
                _ => {}
            }
        }
        for ty in &["Int", "Float", "Text", "Bool", "IO", "Maybe", "Result"] {
            items.push(CompletionItem {
                label: ty.to_string(),
                kind: Some(CompletionItemKind::STRUCT),
                ..Default::default()
            });
        }
        return Some(CompletionResponse::Array(items));
    }

    // Keywords
    for kw in KEYWORDS {
        items.push(CompletionItem {
            label: kw.to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        });
    }

    // Snippet completions for common patterns
    for (label, detail, snippet) in SNIPPETS {
        items.push(CompletionItem {
            label: label.to_string(),
            kind: Some(CompletionItemKind::SNIPPET),
            detail: Some(detail.to_string()),
            insert_text: Some(snippet.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        });
    }

    // Declarations from current document with type details
    for decl in &doc.module.decls {
        match &decl.node {
            DeclKind::Data {
                name, constructors, ..
            } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::STRUCT),
                    detail: doc.details.get(name).cloned(),
                    ..Default::default()
                });
                for ctor in constructors {
                    items.push(CompletionItem {
                        label: ctor.name.clone(),
                        kind: Some(CompletionItemKind::ENUM_MEMBER),
                        detail: doc.details.get(&ctor.name).cloned(),
                        ..Default::default()
                    });
                }
            }
            DeclKind::TypeAlias { name, .. } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::STRUCT),
                    detail: doc.details.get(name).cloned(),
                    ..Default::default()
                });
            }
            DeclKind::Source { name, .. } | DeclKind::View { name, .. } => {
                items.push(CompletionItem {
                    label: format!("*{name}"),
                    kind: Some(CompletionItemKind::VARIABLE),
                    insert_text: Some(format!("*{name}")),
                    detail: doc.type_info.get(name.as_str()).cloned(),
                    ..Default::default()
                });
            }
            DeclKind::Derived { name, .. } => {
                items.push(CompletionItem {
                    label: format!("&{name}"),
                    kind: Some(CompletionItemKind::VARIABLE),
                    insert_text: Some(format!("&{name}")),
                    detail: doc.type_info.get(name.as_str()).cloned(),
                    ..Default::default()
                });
            }
            DeclKind::Fun { name, .. } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::FUNCTION),
                    detail: doc.type_info.get(name.as_str()).cloned(),
                    ..Default::default()
                });
            }
            DeclKind::Trait { name, .. } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::INTERFACE),
                    detail: doc.details.get(name).cloned(),
                    ..Default::default()
                });
            }
            _ => {}
        }
    }

    // Built-in functions with type info
    for name in BUILTINS {
        items.push(CompletionItem {
            label: name.to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: doc.type_info.get(*name).cloned(),
            ..Default::default()
        });
    }

    // Auto-import completions: scan workspace for symbols not in current document.
    // Uses the parsed-import cache (populated lazily as imports are resolved
    // for any open file) plus a one-shot disk read for files we haven't parsed
    // yet. Modules are not re-parsed across completion requests within a single
    // analyze cycle.
    if let Some(root) = &state.workspace_root {
        let source_path = uri_to_path(uri);
        let existing_imports: HashSet<String> = doc.module.imports.iter().map(|i| i.path.clone()).collect();
        let local_names: HashSet<&str> = doc.definitions.keys().map(|s| s.as_str()).collect();

        // De-dupe by name across files: if two workspace files both export
        // `parse`, prefer the one with the lexicographically-shortest path.
        let mut seen_names: HashSet<String> = HashSet::new();

        if let Ok(files) = scan_knot_files(root) {
            for file_path in files {
                let canonical = match file_path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                // Skip current file
                if source_path.as_ref().and_then(|p| p.canonicalize().ok()) == Some(canonical.clone()) {
                    continue;
                }
                // Compute the import path relative to the current file
                let import_path = match source_path.as_ref().and_then(|p| p.parent()) {
                    Some(base) => {
                        match canonical.strip_prefix(base) {
                            Ok(rel) => rel.with_extension("").to_string_lossy().to_string(),
                            Err(_) => continue,
                        }
                    }
                    None => continue,
                };
                // Skip already imported files
                if existing_imports.contains(&import_path) {
                    continue;
                }

                // Reuse the cached parsed module if available (populated by
                // resolve_import_navigation when other files have imported it).
                let module = if let Some((_, cached_mod, _)) = state.import_cache.get(&canonical) {
                    cached_mod.clone()
                } else {
                    let source_text = match std::fs::read_to_string(&canonical) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    let lexer = knot::lexer::Lexer::new(&source_text);
                    let (tokens, _) = lexer.tokenize();
                    let parser = knot::parser::Parser::new(source_text, tokens);
                    let (m, _) = parser.parse_module();
                    m
                };

                for decl in &module.decls {
                    // Only suggest exported names (or all top-level if `export`
                    // isn't being used in this file)
                    let (name, kind) = match &decl.node {
                        DeclKind::Fun { name, .. } => (name.clone(), CompletionItemKind::FUNCTION),
                        DeclKind::Data { name, .. } => (name.clone(), CompletionItemKind::STRUCT),
                        DeclKind::TypeAlias { name, .. } => (name.clone(), CompletionItemKind::STRUCT),
                        DeclKind::Trait { name, .. } => (name.clone(), CompletionItemKind::INTERFACE),
                        _ => continue,
                    };
                    // Skip names already defined locally or already suggested
                    if local_names.contains(name.as_str()) || seen_names.contains(&name) {
                        continue;
                    }
                    seen_names.insert(name.clone());

                    // Compute where to insert the import line
                    let import_insert_pos = if let Some(last_import) = doc.module.imports.last() {
                        let end = offset_to_position(&doc.source, last_import.span.end);
                        Position::new(end.line + 1, 0)
                    } else {
                        Position::new(0, 0)
                    };
                    let import_line = if doc.module.imports.is_empty() {
                        format!("import {import_path}\n\n")
                    } else {
                        format!("import {import_path}\n")
                    };

                    let additional_edits = vec![TextEdit {
                        range: Range {
                            start: import_insert_pos,
                            end: import_insert_pos,
                        },
                        new_text: import_line,
                    }];

                    items.push(CompletionItem {
                        label: name.clone(),
                        kind: Some(kind),
                        detail: Some(format!("auto-import from {import_path}")),
                        additional_text_edits: Some(additional_edits),
                        sort_text: Some(format!("zz_{name}")), // sort after local items
                        ..Default::default()
                    });
                }
            }
        }
    }

    Some(CompletionResponse::Array(items))
}

/// Try to resolve field names for dot completion by finding the type of the
/// expression before the dot.
fn resolve_dot_fields(doc: &DocumentState, dot_pos: usize) -> Vec<String> {
    let bytes = doc.source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    // Find the identifier immediately before the dot
    let mut end = dot_pos;
    while end > 0 && bytes[end - 1] == b' ' {
        end -= 1;
    }
    let ident_end = end;
    while end > 0 && is_ident(bytes[end - 1]) {
        end -= 1;
    }
    if end == ident_end {
        return Vec::new();
    }

    let var_name = &doc.source[end..ident_end];

    // Look up the variable's type
    let type_str = find_type_for_name(doc, var_name, end);
    let type_str = match type_str {
        Some(t) => t,
        None => return Vec::new(),
    };

    // Parse fields from the type string
    extract_fields_from_type_str(&type_str, &doc.module)
}

/// Find the type string for a name, checking local bindings first, then globals.
fn find_type_for_name(doc: &DocumentState, name: &str, offset: usize) -> Option<String> {
    // Check local type info: find a binding whose span covers this identifier
    // Use the full identifier range [offset..ident_end) for more precise matching
    let ident_end = offset + name.len();
    for (span, ty) in &doc.local_type_info {
        if span.start <= offset && ident_end <= span.end {
            return Some(ty.clone());
        }
    }
    // Check if any reference at this offset points to a local binding with a known type
    for (usage_span, def_span) in &doc.references {
        if usage_span.start <= offset && offset < usage_span.end {
            if let Some(ty) = doc.local_type_info.get(def_span) {
                return Some(ty.clone());
            }
        }
    }
    // Check global type info
    doc.type_info.get(name).cloned()
}

/// Extract field names from a type string like `{name: Text, age: Int}` or a named type.
fn extract_fields_from_type_str(type_str: &str, module: &Module) -> Vec<String> {
    let type_str = type_str.trim();

    // Direct record type: `{name: Text, age: Int}`
    if type_str.starts_with('{') && type_str.ends_with('}') {
        return extract_record_fields(type_str);
    }

    // Relation type: `[{name: Text}]` or `[Person]` — extract inner type
    if type_str.starts_with('[') && type_str.ends_with(']') {
        let inner = &type_str[1..type_str.len() - 1];
        return extract_fields_from_type_str(inner, module);
    }

    // IO type: `IO {...} [T]` or `IO {...} {fields}` — skip to the value type
    if type_str.starts_with("IO ") {
        let rest = &type_str[3..];
        // Skip the effect set `{...}`
        if rest.starts_with('{') {
            if let Some(close) = rest.find('}') {
                let value_type = rest[close + 1..].trim();
                return extract_fields_from_type_str(value_type, module);
            }
        }
    }

    // Maybe type: `Maybe T` — unwrap to inner type
    if type_str.starts_with("Maybe ") {
        let inner = type_str[6..].trim();
        return extract_fields_from_type_str(inner, module);
    }

    // Named type: look up in the module's declarations
    for decl in &module.decls {
        match &decl.node {
            DeclKind::TypeAlias { name, ty, .. } if name == type_str => {
                match &ty.node {
                    TypeKind::Record { fields, .. } => {
                        return fields.iter().map(|f| f.name.clone()).collect();
                    }
                    // Follow alias to another named type
                    TypeKind::Named(target) => {
                        return extract_fields_from_type_str(target, module);
                    }
                    _ => {}
                }
            }
            DeclKind::Source { name, ty, .. } if name == type_str => {
                if let TypeKind::Record { fields, .. } = &ty.node {
                    return fields.iter().map(|f| f.name.clone()).collect();
                }
            }
            // Data type with a single constructor — expose its fields
            DeclKind::Data { name, constructors, .. } if name == type_str => {
                if constructors.len() == 1 {
                    return constructors[0].fields.iter().map(|f| f.name.clone()).collect();
                }
            }
            _ => {}
        }
    }

    Vec::new()
}

/// Parse field names from a record type string like `{name: Text, age: Int}`.
fn extract_record_fields(type_str: &str) -> Vec<String> {
    let inner = &type_str[1..type_str.len() - 1]; // strip { }
    let mut fields = Vec::new();
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
                if let Some(name) = extract_field_name(&current) {
                    fields.push(name);
                }
                current.clear();
            }
            '|' if depth == 0 => {
                // Row variable — stop
                if let Some(name) = extract_field_name(&current) {
                    fields.push(name);
                }
                break;
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        if let Some(name) = extract_field_name(&current) {
            fields.push(name);
        }
    }
    fields
}

fn extract_field_name(field_str: &str) -> Option<String> {
    let trimmed = field_str.trim();
    let colon = trimmed.find(':')?;
    Some(trimmed[..colon].trim().to_string())
}

// ── Find References ─────────────────────────────────────────────────

fn handle_references(
    state: &ServerState,
    params: &ReferenceParams,
) -> Option<Vec<Location>> {
    let uri = &params.text_document_position.text_document.uri;
    let pos = params.text_document_position.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);

    // Find the symbol name and definition span in current document
    let word = word_at_position(&doc.source, pos)?;
    let def_span = doc
        .references
        .iter()
        .find(|(usage, _)| usage.start <= offset && offset < usage.end)
        .map(|(_, def)| *def)
        .or_else(|| doc.definitions.get(word).copied())
        .or_else(|| {
            doc.definitions.values().find(|span| span.start <= offset && offset < span.end).copied()
        })?;

    let symbol_name = word.to_string();
    let mut locations = Vec::new();

    // Include declaration if requested
    if params.context.include_declaration {
        locations.push(Location {
            uri: uri.clone(),
            range: span_to_range(def_span, &doc.source),
        });
    }

    // Find all usages in current document
    for (usage_span, target_span) in &doc.references {
        if *target_span == def_span {
            locations.push(Location {
                uri: uri.clone(),
                range: span_to_range(*usage_span, &doc.source),
            });
        }
    }

    // Determine the canonical file that defines this symbol (for scoped matching)
    let defining_file = doc.import_origins.get(&symbol_name);
    let _current_file = uri_to_path(uri).and_then(|p| p.canonicalize().ok());

    // Cross-file: search all other open documents for references to the same name
    for (other_uri, other_doc) in &state.documents {
        if other_uri == uri {
            continue;
        }
        // Scope check: if the symbol is imported, only match in documents that import
        // from the same origin, or that define the symbol themselves
        let _other_file = uri_to_path(other_uri).and_then(|p| p.canonicalize().ok());
        let is_defining_file = defining_file.is_some()
            && other_doc.import_defs.get(&symbol_name)
                .map(|(path, _)| Some(path.clone()) == doc.import_defs.get(&symbol_name).map(|(p, _)| p.clone()))
                .unwrap_or(false);
        let is_local_def = other_doc.definitions.contains_key(&symbol_name);
        let shares_origin = defining_file.is_none() // locally defined — match by name
            || is_defining_file
            || is_local_def
            || other_doc.import_origins.get(&symbol_name) == defining_file;

        if !shares_origin {
            continue;
        }

        for (usage_span, target_span) in &other_doc.references {
            let target_name = &other_doc.source[target_span.start..target_span.end.min(other_doc.source.len())];
            if other_doc.definitions.get(&symbol_name) == Some(target_span) {
                locations.push(Location {
                    uri: other_uri.clone(),
                    range: span_to_range(*usage_span, &other_doc.source),
                });
            } else if target_name == symbol_name {
                locations.push(Location {
                    uri: other_uri.clone(),
                    range: span_to_range(*usage_span, &other_doc.source),
                });
            }
        }
        // Also check if the other doc has a definition of this name (for include_declaration)
        if params.context.include_declaration {
            if let Some(other_def) = other_doc.definitions.get(&symbol_name) {
                locations.push(Location {
                    uri: other_uri.clone(),
                    range: span_to_range(*other_def, &other_doc.source),
                });
            }
        }
    }

    if locations.is_empty() {
        None
    } else {
        Some(locations)
    }
}

// ── Rename ──────────────────────────────────────────────────────────

fn handle_prepare_rename(
    state: &ServerState,
    params: &TextDocumentPositionParams,
) -> Option<PrepareRenameResponse> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let pos = params.position;
    let offset = position_to_offset(&doc.source, pos);

    // Check if cursor is on a renameable symbol
    let word = word_at_position(&doc.source, pos)?;

    // Must be on a known definition or a reference to one
    let is_ref = doc
        .references
        .iter()
        .any(|(usage, _)| usage.start <= offset && offset < usage.end);
    let is_def = doc.definitions.values().any(|span| span.start <= offset && offset < span.end);

    if !is_ref && !is_def {
        return None;
    }

    // Return the word range
    let word_offset = position_to_offset(&doc.source, pos);
    let bytes = doc.source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let start = (0..word_offset)
        .rev()
        .find(|&i| !is_ident(bytes[i]))
        .map(|i| i + 1)
        .unwrap_or(0);
    let end = (word_offset..bytes.len())
        .find(|&i| !is_ident(bytes[i]))
        .unwrap_or(bytes.len());

    let range = span_to_range(Span::new(start, end), &doc.source);
    Some(PrepareRenameResponse::RangeWithPlaceholder {
        range,
        placeholder: word.to_string(),
    })
}

fn handle_rename(
    state: &ServerState,
    params: &RenameParams,
) -> Option<WorkspaceEdit> {
    let uri = &params.text_document_position.text_document.uri;
    let pos = params.text_document_position.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);
    let new_name = &params.new_name;

    // Find the definition span
    let def_span = doc
        .references
        .iter()
        .find(|(usage, _)| usage.start <= offset && offset < usage.end)
        .map(|(_, def)| *def)
        .or_else(|| {
            doc.definitions.values().find(|span| span.start <= offset && offset < span.end).copied()
        })?;

    let old_name = word_at_position(&doc.source, pos)?;
    let symbol_name = old_name.to_string();

    let mut changes: HashMap<Uri, Vec<TextEdit>> = HashMap::new();

    // Rename at definition site in current doc
    let def_text = &doc.source[def_span.start..def_span.end];
    if let Some(name_start) = def_text.find(old_name) {
        let name_span = Span::new(def_span.start + name_start, def_span.start + name_start + old_name.len());
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(name_span, &doc.source),
            new_text: new_name.clone(),
        });
    } else {
        changes.entry(uri.clone()).or_default().push(TextEdit {
            range: span_to_range(def_span, &doc.source),
            new_text: new_name.clone(),
        });
    }

    // Rename all usage sites in current doc
    for (usage_span, target_span) in &doc.references {
        if *target_span == def_span {
            changes.entry(uri.clone()).or_default().push(TextEdit {
                range: span_to_range(*usage_span, &doc.source),
                new_text: new_name.clone(),
            });
        }
    }

    // Cross-file: rename in all other open documents
    for (other_uri, other_doc) in &state.documents {
        if other_uri == uri {
            continue;
        }
        // Rename definition if it exists in this doc
        if let Some(other_def) = other_doc.definitions.get(&symbol_name) {
            let def_text = &other_doc.source[other_def.start..other_def.end.min(other_doc.source.len())];
            if let Some(name_start) = def_text.find(old_name) {
                let name_span = Span::new(other_def.start + name_start, other_def.start + name_start + old_name.len());
                changes.entry(other_uri.clone()).or_default().push(TextEdit {
                    range: span_to_range(name_span, &other_doc.source),
                    new_text: new_name.clone(),
                });
            }
        }
        // Rename usages
        for (usage_span, target_span) in &other_doc.references {
            let target_name = &other_doc.source[target_span.start..target_span.end.min(other_doc.source.len())];
            if other_doc.definitions.get(&symbol_name) == Some(target_span) || target_name == symbol_name {
                changes.entry(other_uri.clone()).or_default().push(TextEdit {
                    range: span_to_range(*usage_span, &other_doc.source),
                    new_text: new_name.clone(),
                });
            }
        }
    }

    // Workspace-wide: scan .knot files not currently open that may import this symbol
    if let Some(root) = &state.workspace_root {
        let open_paths: HashSet<PathBuf> = state
            .documents
            .keys()
            .filter_map(|u| uri_to_path(u))
            .filter_map(|p| p.canonicalize().ok())
            .collect();

        if let Ok(files) = scan_knot_files(root) {
            for file_path in files {
                let canonical = match file_path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if open_paths.contains(&canonical) {
                    continue;
                }
                let file_source = match std::fs::read_to_string(&canonical) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                // Quick check: does the file contain the symbol name at all?
                if !file_source.contains(old_name) {
                    continue;
                }
                let file_uri = match path_to_uri(&canonical) {
                    Some(u) => u,
                    None => continue,
                };
                let lexer = knot::lexer::Lexer::new(&file_source);
                let (tokens, _) = lexer.tokenize();
                let parser = knot::parser::Parser::new(file_source.clone(), tokens);
                let (module, _) = parser.parse_module();
                let (defs, refs, _) = resolve_definitions(&module, &file_source);

                // Rename at definition sites
                if let Some(def_span) = defs.get(&symbol_name) {
                    let def_text =
                        &file_source[def_span.start..def_span.end.min(file_source.len())];
                    if let Some(name_start) = def_text.find(old_name) {
                        let name_span = Span::new(
                            def_span.start + name_start,
                            def_span.start + name_start + old_name.len(),
                        );
                        changes
                            .entry(file_uri.clone())
                            .or_default()
                            .push(TextEdit {
                                range: span_to_range(name_span, &file_source),
                                new_text: new_name.clone(),
                            });
                    }
                }
                // Rename at usage sites
                for (usage_span, target_span) in &refs {
                    if defs.get(&symbol_name) == Some(target_span) {
                        changes
                            .entry(file_uri.clone())
                            .or_default()
                            .push(TextEdit {
                                range: span_to_range(*usage_span, &file_source),
                                new_text: new_name.clone(),
                            });
                    }
                }
            }
        }
    }

    Some(WorkspaceEdit {
        changes: Some(changes),
        ..Default::default()
    })
}

// ── Inlay Hints ─────────────────────────────────────────────────────

fn handle_inlay_hint(
    state: &ServerState,
    params: &InlayHintParams,
) -> Option<Vec<InlayHint>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let mut hints = Vec::new();

    let range_start = position_to_offset(&doc.source, params.range.start);
    let range_end = position_to_offset(&doc.source, params.range.end);

    // Show inferred types for unannotated function declarations.
    // For annotated functions, show only the inferred *effects* if they exist
    // and aren't already in the type signature.
    for decl in &doc.module.decls {
        // Only show hints within the visible range
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }

        match &decl.node {
            DeclKind::Fun { name, ty: None, .. } => {
                if let Some(inferred) = doc.type_info.get(name) {
                    let decl_text = &doc.source[decl.span.start..decl.span.end.min(doc.source.len())];
                    let name_end = decl_text.find(|c: char| !c.is_alphanumeric() && c != '_')
                        .unwrap_or(name.len());
                    let hint_offset = decl.span.start + name_end;
                    let hint_pos = offset_to_position(&doc.source, hint_offset);
                    hints.push(InlayHint {
                        position: hint_pos,
                        label: InlayHintLabel::String(format!(": {inferred}")),
                        kind: Some(InlayHintKind::TYPE),
                        text_edits: Some(vec![TextEdit {
                            range: Range { start: hint_pos, end: hint_pos },
                            new_text: format!("{name} : {inferred}\n"),
                        }]),
                        tooltip: doc.effect_info.get(name).map(|effects| {
                            InlayHintTooltip::String(format!("Effects: {effects}"))
                        }),
                        padding_left: Some(true),
                        padding_right: Some(true),
                        data: None,
                    });
                }
            }
            DeclKind::Fun { name, ty: Some(_), .. } => {
                // Annotated function: show the inferred *effects* as a hint at
                // the function body's start, only when the type doesn't already
                // declare them. Helps with effect-row polymorphism debugging.
                if let Some(effects) = doc.effect_info.get(name) {
                    let inferred_ty = doc.type_info.get(name);
                    let needs_hint = inferred_ty
                        .map(|ty| !type_str_mentions_effects(ty, effects))
                        .unwrap_or(true);
                    if needs_hint {
                        let hint_offset = name_end_offset(&doc.source, decl.span, name);
                        let hint_pos = offset_to_position(&doc.source, hint_offset);
                        hints.push(InlayHint {
                            position: hint_pos,
                            label: InlayHintLabel::String(format!("-- effects: {effects}")),
                            kind: None,
                            text_edits: None,
                            tooltip: None,
                            padding_left: Some(true),
                            padding_right: None,
                            data: None,
                        });
                    }
                }
            }
            DeclKind::View { name, ty: None, .. } | DeclKind::Derived { name, ty: None, .. } => {
                if let Some(inferred) = doc.type_info.get(name) {
                    let decl_text = &doc.source[decl.span.start..decl.span.end.min(doc.source.len())];
                    let name_end = decl_text.find('=').unwrap_or(name.len() + 1);
                    let hint_offset = decl.span.start + name_end;
                    let hint_pos = offset_to_position(&doc.source, hint_offset);
                    hints.push(InlayHint {
                        position: hint_pos,
                        label: InlayHintLabel::String(format!(": {inferred}")),
                        kind: Some(InlayHintKind::TYPE),
                        text_edits: None,
                        tooltip: doc.effect_info.get(name).map(|e| {
                            InlayHintTooltip::String(format!("Effects: {e}"))
                        }),
                        padding_left: Some(true),
                        padding_right: Some(true),
                        data: None,
                    });
                }
            }
            _ => {}
        }
    }

    // Show inferred types for local bindings (let/bind in do blocks)
    for (span, ty) in &doc.local_type_info {
        if span.end < range_start || span.start > range_end {
            continue;
        }
        let hint_pos = offset_to_position(&doc.source, span.end);
        hints.push(InlayHint {
            position: hint_pos,
            label: InlayHintLabel::String(format!(": {ty}")),
            kind: Some(InlayHintKind::TYPE),
            text_edits: None,
            tooltip: None,
            padding_left: Some(true),
            padding_right: None,
            data: None,
        });
    }

    Some(hints)
}

/// Find the byte offset just after the function name within its declaration span.
fn name_end_offset(source: &str, decl_span: Span, name: &str) -> usize {
    let decl_text = &source[decl_span.start..decl_span.end.min(source.len())];
    let name_end = decl_text
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(name.len());
    decl_span.start + name_end
}

/// Heuristic: does the rendered type string already mention all of the given
/// effects? Used to suppress redundant effect inlay hints.
fn type_str_mentions_effects(ty: &str, effects: &str) -> bool {
    // The effects string looks like `{console, reads *foo}` — pull the inner
    // tokens and check that each appears in the type string.
    let inner = effects.trim_start_matches('{').trim_end_matches('}');
    if inner.is_empty() {
        return true;
    }
    inner.split(',').all(|tok| ty.contains(tok.trim()))
}

// ── Signature Help (paren-aware) ────────────────────────────────────

fn handle_signature_help(
    state: &ServerState,
    params: &SignatureHelpParams,
) -> Option<SignatureHelp> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);

    // Strategy: find the innermost App chain in the AST that contains the cursor,
    // then determine which argument position the cursor is in.
    let (func_name, active_param) = find_enclosing_application(&doc.module, &doc.source, offset)?;

    // Look up the function type
    let type_str = doc.type_info.get(func_name.as_str())?;

    // Parse arrow-separated parameters from the type string
    let param_types = parse_function_params(type_str);
    if param_types.is_empty() {
        return None;
    }

    // Try to extract parameter names from the function definition
    let param_names = extract_param_names(&doc.module, &func_name);

    // Build parameter labels: "name: Type" if we have a name, else just "Type"
    // The label must be a substring of the signature label so the editor can
    // highlight the active parameter.
    let signature_label = build_signature_label(&func_name, &param_types, &param_names, type_str);

    let param_infos: Vec<ParameterInformation> = param_types
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let name = param_names.get(i);
            let label_text = match name {
                Some(n) => format!("{n}: {ty}"),
                None => ty.clone(),
            };
            // Locate the label substring in the signature for proper highlighting
            let label = match signature_label.find(&label_text) {
                Some(start) => ParameterLabel::LabelOffsets([
                    start as u32,
                    (start + label_text.len()) as u32,
                ]),
                None => ParameterLabel::Simple(label_text.clone()),
            };
            ParameterInformation {
                label,
                documentation: param_doc(doc, &func_name, i, name.map(String::as_str)),
            }
        })
        .collect();

    let active = (active_param as u32).min(param_infos.len().saturating_sub(1) as u32);

    // Function-level documentation: doc comment + effects
    let mut doc_parts: Vec<String> = Vec::new();
    if let Some(comment) = doc.doc_comments.get(&func_name) {
        doc_parts.push(comment.clone());
    }
    if let Some(effects) = doc.effect_info.get(&func_name) {
        doc_parts.push(format!("**Effects:** `{effects}`"));
    }
    let doc_value = if doc_parts.is_empty() {
        None
    } else {
        Some(Documentation::MarkupContent(MarkupContent {
            kind: MarkupKind::Markdown,
            value: doc_parts.join("\n\n"),
        }))
    };

    let signature = SignatureInformation {
        label: signature_label,
        documentation: doc_value,
        parameters: Some(param_infos),
        active_parameter: Some(active),
    };

    Some(SignatureHelp {
        signatures: vec![signature],
        active_signature: Some(0),
        active_parameter: Some(active),
    })
}

/// Build a signature label like `func : a: T1 -> b: T2 -> Result`.
/// Falls back to the type string if no parameter names are known.
fn build_signature_label(
    func_name: &str,
    param_types: &[String],
    param_names: &[String],
    return_str: &str,
) -> String {
    if param_names.is_empty() {
        return format!("{func_name} : {return_str}");
    }
    // Compute the return type: the suffix of `return_str` after the param types.
    // We render arguments as `name: Type -> ...` and append the return type.
    let mut parts: Vec<String> = Vec::new();
    for (i, ty) in param_types.iter().enumerate() {
        if let Some(name) = param_names.get(i) {
            parts.push(format!("{name}: {ty}"));
        } else {
            parts.push(ty.clone());
        }
    }
    // Last entry of param_types is the return type — but parse_function_params
    // splits all arrow-separated parts including the return type. Keep the
    // final part as-is (no name).
    format!("{func_name} : {}", parts.join(" -> "))
}

/// Extract parameter names from a function declaration's body.
/// Returns an empty Vec if the function isn't directly a lambda chain.
fn extract_param_names(module: &Module, func_name: &str) -> Vec<String> {
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun {
                name,
                body: Some(body),
                ..
            } if name == func_name => {
                return collect_lambda_param_names(body);
            }
            DeclKind::Trait { items, .. } => {
                for item in items {
                    if let ast::TraitItem::Method {
                        name,
                        default_params,
                        ..
                    } = item
                    {
                        if name == func_name {
                            return default_params
                                .iter()
                                .map(|p| pat_to_simple_name(&p.node))
                                .collect();
                        }
                    }
                }
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { name, params, .. } = item {
                        if name == func_name {
                            return params
                                .iter()
                                .map(|p| pat_to_simple_name(&p.node))
                                .collect();
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Vec::new()
}

/// Walk a chain of nested lambdas (`\a -> \b -> body`) and collect param names.
fn collect_lambda_param_names(expr: &ast::Expr) -> Vec<String> {
    let mut names = Vec::new();
    let mut cur = expr;
    loop {
        match &cur.node {
            ast::ExprKind::Lambda { params, body } => {
                for p in params {
                    names.push(pat_to_simple_name(&p.node));
                }
                cur = body;
            }
            _ => break,
        }
    }
    names
}

/// Render a pattern as a simple name for parameter display.
/// `x` → "x", `{name, age}` → "{name, age}", `_` → "_".
fn pat_to_simple_name(pat: &ast::PatKind) -> String {
    match pat {
        ast::PatKind::Var(name) => name.clone(),
        ast::PatKind::Wildcard => "_".into(),
        ast::PatKind::Record(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| match &f.pattern {
                    None => f.name.clone(),
                    Some(p) => format!("{}: {}", f.name, pat_to_simple_name(&p.node)),
                })
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        ast::PatKind::Constructor { name, payload } => {
            format!("{name} {}", pat_to_simple_name(&payload.node))
        }
        ast::PatKind::List(_) => "[..]".into(),
        ast::PatKind::Lit(_) => "_".into(),
    }
}

/// Look up documentation for a single parameter.
/// Falls back to the function's doc comment if it mentions the parameter name.
fn param_doc(
    doc: &DocumentState,
    func_name: &str,
    _index: usize,
    name: Option<&str>,
) -> Option<Documentation> {
    let name = name?;
    // Look for a `param_name: ...` line in the function's doc comment
    let comment = doc.doc_comments.get(func_name)?;
    for line in comment.lines() {
        let trimmed = line.trim();
        // Match formats: `name: description`, `@param name description`, `- name: description`
        let candidate = trimmed
            .strip_prefix(&format!("{name}: "))
            .or_else(|| trimmed.strip_prefix(&format!("- {name}: ")))
            .or_else(|| trimmed.strip_prefix(&format!("@param {name} ")))
            .or_else(|| trimmed.strip_prefix(&format!("@param {name}: ")));
        if let Some(desc) = candidate {
            return Some(Documentation::String(desc.to_string()));
        }
    }
    None
}

/// Walk the AST to find the innermost function application chain containing the cursor.
/// Returns (function_name, active_parameter_index).
fn find_enclosing_application(module: &Module, source: &str, offset: usize) -> Option<(String, usize)> {
    let mut best: Option<(String, usize, usize)> = None; // (name, param_idx, span_size)

    for decl in &module.decls {
        if decl.span.start > offset || offset > decl.span.end {
            continue;
        }
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                find_app_in_expr(body, source, offset, &mut best);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        find_app_in_expr(body, source, offset, &mut best);
                    }
                }
            }
            DeclKind::Trait { items, .. } => {
                for item in items {
                    if let ast::TraitItem::Method { default_body: Some(body), .. } = item {
                        find_app_in_expr(body, source, offset, &mut best);
                    }
                }
            }
            _ => {}
        }
    }

    best.map(|(name, idx, _)| (name, idx))
}

fn find_app_in_expr(
    expr: &ast::Expr,
    source: &str,
    offset: usize,
    best: &mut Option<(String, usize, usize)>,
) {
    if expr.span.start > offset || offset > expr.span.end {
        return;
    }

    // Check if this is an App chain
    if let ast::ExprKind::App { .. } = &expr.node {
        // Flatten the App spine: f a b c is App(App(App(f, a), b), c)
        let mut args = Vec::new();
        let mut cur = expr;
        while let ast::ExprKind::App { func, arg } = &cur.node {
            args.push(arg.as_ref());
            cur = func.as_ref();
        }
        args.reverse();

        // cur is now the function at the head
        let func_name = match &cur.node {
            ast::ExprKind::Var(name) => Some(name.clone()),
            ast::ExprKind::Constructor(name) => Some(name.clone()),
            _ => None,
        };

        if let Some(name) = func_name {
            // Determine which argument the cursor is in
            let mut param_idx = args.len(); // default: past the last arg (next param)
            for (i, arg) in args.iter().enumerate() {
                if offset <= arg.span.start {
                    param_idx = i;
                    break;
                }
                if offset >= arg.span.start && offset <= arg.span.end {
                    param_idx = i;
                    break;
                }
            }

            let span_size = expr.span.end - expr.span.start;
            // Prefer the smallest (innermost) enclosing application
            if best.as_ref().map_or(true, |b| span_size <= b.2) {
                *best = Some((name, param_idx, span_size));
            }
        }
    }

    // Recurse into sub-expressions
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            find_app_in_expr(func, source, offset, best);
            find_app_in_expr(arg, source, offset, best);
        }
        ast::ExprKind::Lambda { body, .. } => {
            find_app_in_expr(body, source, offset, best);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            find_app_in_expr(lhs, source, offset, best);
            find_app_in_expr(rhs, source, offset, best);
        }
        ast::ExprKind::UnaryOp { operand, .. } => {
            find_app_in_expr(operand, source, offset, best);
        }
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            find_app_in_expr(cond, source, offset, best);
            find_app_in_expr(then_branch, source, offset, best);
            find_app_in_expr(else_branch, source, offset, best);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            find_app_in_expr(scrutinee, source, offset, best);
            for arm in arms {
                find_app_in_expr(&arm.body, source, offset, best);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => {
                        find_app_in_expr(expr, source, offset, best);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        find_app_in_expr(e, source, offset, best);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        find_app_in_expr(key, source, offset, best);
                    }
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => find_app_in_expr(e, source, offset, best),
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
            find_app_in_expr(target, source, offset, best);
            find_app_in_expr(value, source, offset, best);
        }
        ast::ExprKind::Record(fields) => {
            for f in fields {
                find_app_in_expr(&f.value, source, offset, best);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            find_app_in_expr(base, source, offset, best);
            for f in fields {
                find_app_in_expr(&f.value, source, offset, best);
            }
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                find_app_in_expr(e, source, offset, best);
            }
        }
        ast::ExprKind::FieldAccess { expr, .. } => {
            find_app_in_expr(expr, source, offset, best);
        }
        ast::ExprKind::At { relation, time } => {
            find_app_in_expr(relation, source, offset, best);
            find_app_in_expr(time, source, offset, best);
        }
        _ => {}
    }
}

/// Parse a Knot type string like "Int -> Text -> Bool" into parameter types.
fn parse_function_params(type_str: &str) -> Vec<String> {
    let mut params = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();

    let chars: Vec<char> = type_str.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '(' | '[' | '{' | '<' => {
                depth += 1;
                current.push(chars[i]);
            }
            ')' | ']' | '}' | '>' => {
                depth -= 1;
                current.push(chars[i]);
            }
            '-' if depth == 0 && i + 1 < chars.len() && chars[i + 1] == '>' => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    params.push(trimmed);
                }
                current.clear();
                i += 2; // skip "->"
                continue;
            }
            _ => {
                current.push(chars[i]);
            }
        }
        i += 1;
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        params.push(trimmed);
    }

    params
}

// ── Code Lens ───────────────────────────────────────────────────────

fn handle_code_lens(
    state: &ServerState,
    params: &CodeLensParams,
) -> Option<Vec<CodeLens>> {
    let uri = &params.text_document.uri;
    let doc = state.documents.get(uri)?;
    let mut lenses = Vec::new();

    for decl in &doc.module.decls {
        match &decl.node {
            DeclKind::Fun { .. }
            | DeclKind::Source { .. }
            | DeclKind::View { .. }
            | DeclKind::Derived { .. }
            | DeclKind::Data { .. }
            | DeclKind::Trait { .. } => {}
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

// ── Semantic Tokens ─────────────────────────────────────────────────

fn handle_semantic_tokens_full(
    state: &ServerState,
    params: &SemanticTokensParams,
) -> Option<SemanticTokensResult> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let mut raw_tokens = Vec::new();
    let mut collector = TokenCollector {
        tokens: &mut raw_tokens,
        source: &doc.source,
    };

    for decl in &doc.module.decls {
        collector.visit_decl(decl);
    }

    // Add keyword and operator tokens from lexer
    let ast_token_starts: HashSet<usize> = raw_tokens.iter().map(|t| t.start).collect();
    for (span, tok_type) in &doc.keyword_tokens {
        // Only add if no AST-based token already covers this position
        if !ast_token_starts.contains(&span.start) {
            raw_tokens.push(RawToken {
                start: span.start,
                length: span.end - span.start,
                token_type: *tok_type,
                modifiers: 0,
            });
        }
    }

    raw_tokens.sort_by_key(|t| (t.start, t.length));

    // Delta encode
    let encoded = delta_encode_tokens(&raw_tokens, &doc.source);

    Some(SemanticTokensResult::Tokens(SemanticTokens {
        result_id: None,
        data: encoded,
    }))
}

struct RawToken {
    start: usize,
    length: usize,
    token_type: u32,
    modifiers: u32,
}

struct TokenCollector<'a> {
    tokens: &'a mut Vec<RawToken>,
    source: &'a str,
}

impl<'a> TokenCollector<'a> {
    fn add(&mut self, span: Span, token_type: u32, modifiers: u32) {
        if span.start < span.end && span.end <= self.source.len() {
            let text = &self.source[span.start..span.end];
            if !text.contains('\n') {
                self.tokens.push(RawToken {
                    start: span.start,
                    length: span.end - span.start,
                    token_type,
                    modifiers,
                });
            } else {
                // Split multi-line tokens into per-line tokens
                let mut offset = span.start;
                for line in text.split('\n') {
                    if !line.is_empty() {
                        self.tokens.push(RawToken {
                            start: offset,
                            length: line.len(),
                            token_type,
                            modifiers,
                        });
                    }
                    offset += line.len() + 1; // +1 for the '\n'
                }
            }
        }
    }

    fn visit_decl(&mut self, decl: &ast::Decl) {
        match &decl.node {
            DeclKind::Fun { name, body, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.start + name.len() + 20) {
                    self.add(s, TOK_FUNCTION, MOD_DECLARATION);
                }
                if let Some(body) = body {
                    self.visit_expr(body);
                }
            }
            DeclKind::Data {
                name, constructors, ..
            } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_STRUCT, MOD_DECLARATION);
                }
                for ctor in constructors {
                    if let Some(s) = find_word_in_source(self.source, &ctor.name, decl.span.start, decl.span.end) {
                        self.add(s, TOK_ENUM_MEMBER, MOD_DECLARATION);
                    }
                }
            }
            DeclKind::Source { name, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_NAMESPACE, MOD_DECLARATION);
                }
            }
            DeclKind::View { name, body, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_NAMESPACE, MOD_DECLARATION);
                }
                self.visit_expr(body);
            }
            DeclKind::Derived { name, body, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_NAMESPACE, MOD_DECLARATION | MOD_READONLY);
                }
                self.visit_expr(body);
            }
            DeclKind::Trait { name, items, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_TYPE, MOD_DECLARATION);
                }
                for item in items {
                    if let ast::TraitItem::Method {
                        default_params,
                        default_body: Some(body),
                        ..
                    } = item
                    {
                        for p in default_params {
                            self.visit_pat(p, true);
                        }
                        self.visit_expr(body);
                    }
                }
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { params, body, .. } = item {
                        for p in params {
                            self.visit_pat(p, true);
                        }
                        self.visit_expr(body);
                    }
                }
            }
            DeclKind::Migrate { using_fn, .. } => {
                self.visit_expr(using_fn);
            }
            DeclKind::UnitDecl { name, .. } => {
                if let Some(s) = find_word_in_source(self.source, name, decl.span.start, decl.span.end) {
                    self.add(s, TOK_TYPE, MOD_DECLARATION);
                }
            }
            _ => {}
        }
    }

    fn visit_expr(&mut self, expr: &ast::Expr) {
        match &expr.node {
            ast::ExprKind::Var(name) => {
                let modifier = if EFFECTFUL_BUILTINS.contains(&name.as_str()) {
                    MOD_EFFECTFUL
                } else {
                    0
                };
                self.add(expr.span, TOK_VARIABLE, modifier);
            }
            ast::ExprKind::Constructor(_) => {
                self.add(expr.span, TOK_ENUM_MEMBER, 0);
            }
            ast::ExprKind::SourceRef(_) => {
                self.add(expr.span, TOK_NAMESPACE, 0);
            }
            ast::ExprKind::DerivedRef(_) => {
                self.add(expr.span, TOK_NAMESPACE, MOD_READONLY);
            }
            ast::ExprKind::FieldAccess { expr: inner, field } => {
                self.visit_expr(inner);
                // Field name span: the part after the `.`
                let field_start = expr.span.end - field.len();
                if field_start < expr.span.end {
                    self.add(Span::new(field_start, expr.span.end), TOK_PROPERTY, 0);
                }
            }
            ast::ExprKind::Lit(ast::Literal::Int(_) | ast::Literal::Float(_)) => {
                self.add(expr.span, TOK_NUMBER, 0);
            }
            ast::ExprKind::Lit(ast::Literal::Text(_)) => {
                self.add(expr.span, TOK_STRING, 0);
            }
            ast::ExprKind::Lambda { params, body } => {
                for p in params {
                    self.visit_pat(p, true);
                }
                self.visit_expr(body);
            }
            ast::ExprKind::App { func, arg } => {
                self.visit_expr(func);
                self.visit_expr(arg);
            }
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                self.visit_expr(lhs);
                self.visit_expr(rhs);
            }
            ast::ExprKind::UnaryOp { operand, .. } => self.visit_expr(operand),
            ast::ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } => {
                self.visit_expr(cond);
                self.visit_expr(then_branch);
                self.visit_expr(else_branch);
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                self.visit_expr(scrutinee);
                for arm in arms {
                    self.visit_pat(&arm.pat, false);
                    self.visit_expr(&arm.body);
                }
            }
            ast::ExprKind::Do(stmts) => {
                for stmt in stmts {
                    match &stmt.node {
                        ast::StmtKind::Bind { pat, expr } => {
                            self.visit_expr(expr);
                            self.visit_pat(pat, false);
                        }
                        ast::StmtKind::Let { pat, expr } => {
                            self.visit_expr(expr);
                            self.visit_pat(pat, false);
                        }
                        ast::StmtKind::Where { cond } => self.visit_expr(cond),
                        ast::StmtKind::GroupBy { key } => self.visit_expr(key),
                        ast::StmtKind::Expr(e) => self.visit_expr(e),
                    }
                }
            }
            ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => self.visit_expr(e),
            ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
                // Highlight mutation targets distinctly. We re-emit the target
                // span with a MUTATION modifier overlaying whatever inner type
                // visit_expr would assign.
                if let ast::ExprKind::SourceRef(_) | ast::ExprKind::DerivedRef(_) = &target.node {
                    self.add(target.span, TOK_NAMESPACE, MOD_MUTATION);
                } else {
                    self.visit_expr(target);
                }
                self.visit_expr(value);
            }
            ast::ExprKind::At { relation, time } => {
                self.visit_expr(relation);
                self.visit_expr(time);
            }
            ast::ExprKind::Record(fields) => {
                for f in fields {
                    self.visit_expr(&f.value);
                }
            }
            ast::ExprKind::RecordUpdate { base, fields } => {
                self.visit_expr(base);
                for f in fields {
                    self.visit_expr(&f.value);
                }
            }
            ast::ExprKind::List(elems) => {
                for e in elems {
                    self.visit_expr(e);
                }
            }
            ast::ExprKind::UnitLit { value, .. } => {
                self.visit_expr(value);
            }
            ast::ExprKind::Annot { expr: inner, .. } => {
                self.visit_expr(inner);
            }
            _ => {}
        }
    }

    fn visit_pat(&mut self, pat: &ast::Pat, is_param: bool) {
        match &pat.node {
            ast::PatKind::Var(_) => {
                let tok = if is_param { TOK_PARAMETER } else { TOK_VARIABLE };
                self.add(pat.span, tok, MOD_DECLARATION);
            }
            ast::PatKind::Constructor { payload, .. } => {
                // Visit payload (the constructor name itself is part of pat.span)
                self.visit_pat(payload, false);
            }
            ast::PatKind::Record(fields) => {
                for f in fields {
                    if let Some(p) = &f.pattern {
                        self.visit_pat(p, false);
                    }
                }
            }
            ast::PatKind::List(pats) => {
                for p in pats {
                    self.visit_pat(p, false);
                }
            }
            _ => {}
        }
    }
}

fn delta_encode_tokens(tokens: &[RawToken], source: &str) -> Vec<SemanticToken> {
    let mut result = Vec::new();
    let mut prev_line = 0u32;
    let mut prev_char = 0u32;

    for token in tokens {
        let pos = offset_to_position(source, token.start);
        let delta_line = pos.line - prev_line;
        let delta_start = if delta_line == 0 {
            pos.character - prev_char
        } else {
            pos.character
        };

        result.push(SemanticToken {
            delta_line,
            delta_start,
            length: token.length as u32,
            token_type: token.token_type,
            token_modifiers_bitset: token.modifiers,
        });

        prev_line = pos.line;
        prev_char = pos.character;
    }

    result
}

// ── Folding Ranges ──────────────────────────────────────────────────

fn handle_folding_range(
    state: &ServerState,
    params: &FoldingRangeParams,
) -> Option<Vec<FoldingRange>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let mut ranges = Vec::new();

    for decl in &doc.module.decls {
        let range = span_to_range(decl.span, &doc.source);
        if range.end.line > range.start.line {
            ranges.push(FoldingRange {
                start_line: range.start.line,
                start_character: Some(range.start.character),
                end_line: range.end.line,
                end_character: Some(range.end.character),
                kind: Some(FoldingRangeKind::Region),
                ..Default::default()
            });
        }

        // Fold sub-expressions within declarations
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                collect_folding_ranges_expr(body, &doc.source, &mut ranges);
            }
            DeclKind::Fun { body: None, .. } => {}
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        collect_folding_ranges_expr(body, &doc.source, &mut ranges);
                    }
                }
            }
            DeclKind::Trait { items, .. } => {
                for item in items {
                    if let ast::TraitItem::Method {
                        default_body: Some(body),
                        ..
                    } = item
                    {
                        collect_folding_ranges_expr(body, &doc.source, &mut ranges);
                    }
                }
            }
            _ => {}
        }
    }

    // Fold imports if there are multiple
    if doc.module.imports.len() > 1 {
        let first = &doc.module.imports[0];
        let last = &doc.module.imports[doc.module.imports.len() - 1];
        let start = span_to_range(first.span, &doc.source);
        let end = span_to_range(last.span, &doc.source);
        if end.end.line > start.start.line {
            ranges.push(FoldingRange {
                start_line: start.start.line,
                start_character: None,
                end_line: end.end.line,
                end_character: None,
                kind: Some(FoldingRangeKind::Imports),
                ..Default::default()
            });
        }
    }

    Some(ranges)
}

fn collect_folding_ranges_expr(expr: &ast::Expr, source: &str, ranges: &mut Vec<FoldingRange>) {
    let range = span_to_range(expr.span, source);

    match &expr.node {
        ast::ExprKind::Do(_) | ast::ExprKind::Case { .. } => {
            if range.end.line > range.start.line {
                ranges.push(FoldingRange {
                    start_line: range.start.line,
                    start_character: Some(range.start.character),
                    end_line: range.end.line,
                    end_character: Some(range.end.character),
                    kind: Some(FoldingRangeKind::Region),
                    ..Default::default()
                });
            }
        }
        ast::ExprKind::If {
            then_branch,
            else_branch,
            ..
        } => {
            let then_range = span_to_range(then_branch.span, source);
            if then_range.end.line > then_range.start.line {
                ranges.push(FoldingRange {
                    start_line: then_range.start.line,
                    start_character: Some(then_range.start.character),
                    end_line: then_range.end.line,
                    end_character: Some(then_range.end.character),
                    kind: Some(FoldingRangeKind::Region),
                    ..Default::default()
                });
            }
            let else_range = span_to_range(else_branch.span, source);
            if else_range.end.line > else_range.start.line {
                ranges.push(FoldingRange {
                    start_line: else_range.start.line,
                    start_character: Some(else_range.start.character),
                    end_line: else_range.end.line,
                    end_character: Some(else_range.end.character),
                    kind: Some(FoldingRangeKind::Region),
                    ..Default::default()
                });
            }
        }
        _ => {}
    }

    // Recurse into sub-expressions
    match &expr.node {
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => {
                        collect_folding_ranges_expr(expr, source, ranges);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        collect_folding_ranges_expr(e, source, ranges);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_folding_ranges_expr(key, source, ranges);
                    }
                }
            }
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_folding_ranges_expr(scrutinee, source, ranges);
            for arm in arms {
                collect_folding_ranges_expr(&arm.body, source, ranges);
            }
        }
        ast::ExprKind::Lambda { body, .. } => {
            collect_folding_ranges_expr(body, source, ranges);
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            collect_folding_ranges_expr(cond, source, ranges);
            collect_folding_ranges_expr(then_branch, source, ranges);
            collect_folding_ranges_expr(else_branch, source, ranges);
        }
        ast::ExprKind::App { func, arg } => {
            collect_folding_ranges_expr(func, source, ranges);
            collect_folding_ranges_expr(arg, source, ranges);
        }
        _ => {}
    }
}

// ── Selection Range ─────────────────────────────────────────────────

fn handle_selection_range(
    state: &ServerState,
    params: &SelectionRangeParams,
) -> Option<Vec<SelectionRange>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let mut results = Vec::new();

    for pos in &params.positions {
        let offset = position_to_offset(&doc.source, *pos);
        let selection = build_selection_range(&doc.module, &doc.source, offset);
        results.push(selection);
    }

    Some(results)
}

fn build_selection_range(module: &Module, source: &str, offset: usize) -> SelectionRange {
    // Collect all AST spans that contain the offset, from largest to smallest
    let mut spans = Vec::new();

    for decl in &module.decls {
        if decl.span.start <= offset && offset < decl.span.end {
            spans.push(decl.span);
            match &decl.node {
                DeclKind::Fun { body: Some(body), .. }
                | DeclKind::View { body, .. }
                | DeclKind::Derived { body, .. } => {
                    collect_containing_spans(body, offset, &mut spans);
                }
                DeclKind::Fun { body: None, .. } => {}
                DeclKind::Impl { items, .. } => {
                    for item in items {
                        if let ast::ImplItem::Method { body, .. } = item {
                            collect_containing_spans(body, offset, &mut spans);
                        }
                    }
                }
                DeclKind::Trait { items, .. } => {
                    for item in items {
                        if let ast::TraitItem::Method {
                            default_body: Some(body),
                            ..
                        } = item
                        {
                            collect_containing_spans(body, offset, &mut spans);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // Sort by size (largest first) and deduplicate
    spans.sort_by(|a, b| {
        let a_size = a.end - a.start;
        let b_size = b.end - b.start;
        b_size.cmp(&a_size)
    });
    spans.dedup();

    // Build linked list from largest to smallest
    let mut selection = SelectionRange {
        range: Range {
            start: Position::new(0, 0),
            end: offset_to_position(source, source.len()),
        },
        parent: None,
    };

    for span in &spans {
        selection = SelectionRange {
            range: span_to_range(*span, source),
            parent: Some(Box::new(selection)),
        };
    }

    selection
}

fn collect_containing_spans(expr: &ast::Expr, offset: usize, spans: &mut Vec<Span>) {
    if expr.span.start > offset || offset >= expr.span.end {
        return;
    }
    spans.push(expr.span);

    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            collect_containing_spans(func, offset, spans);
            collect_containing_spans(arg, offset, spans);
        }
        ast::ExprKind::Lambda { body, .. } => {
            collect_containing_spans(body, offset, spans);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            collect_containing_spans(lhs, offset, spans);
            collect_containing_spans(rhs, offset, spans);
        }
        ast::ExprKind::UnaryOp { operand, .. } => {
            collect_containing_spans(operand, offset, spans);
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            collect_containing_spans(cond, offset, spans);
            collect_containing_spans(then_branch, offset, spans);
            collect_containing_spans(else_branch, offset, spans);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_containing_spans(scrutinee, offset, spans);
            for arm in arms {
                collect_containing_spans(&arm.body, offset, spans);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => {
                        collect_containing_spans(expr, offset, spans);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        collect_containing_spans(e, offset, spans);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_containing_spans(key, offset, spans);
                    }
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => {
            collect_containing_spans(e, offset, spans);
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
            collect_containing_spans(target, offset, spans);
            collect_containing_spans(value, offset, spans);
        }
        ast::ExprKind::At { relation, time } => {
            collect_containing_spans(relation, offset, spans);
            collect_containing_spans(time, offset, spans);
        }
        ast::ExprKind::FieldAccess { expr, .. } => {
            collect_containing_spans(expr, offset, spans);
        }
        ast::ExprKind::Record(fields) => {
            for f in fields {
                collect_containing_spans(&f.value, offset, spans);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_containing_spans(base, offset, spans);
            for f in fields {
                collect_containing_spans(&f.value, offset, spans);
            }
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                collect_containing_spans(e, offset, spans);
            }
        }
        _ => {}
    }
}

// ── Document Formatting ─────────────────────────────────────────────

fn handle_formatting(
    state: &ServerState,
    params: &DocumentFormattingParams,
) -> Option<Vec<TextEdit>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let source = &doc.source;

    // Formatter:
    // 1. Convert tabs to spaces (2 spaces per tab)
    // 2. Trim trailing whitespace from all lines
    // 3. Normalize blank lines between top-level declarations (exactly one blank line)
    // 4. Collapse consecutive blank lines inside blocks to at most one
    // 5. Ensure trailing newline
    // 6. Normalize imports (single blank line after import block)

    // Convert tabs to spaces first
    let source = &source.replace('\t', "  ");
    let lines: Vec<&str> = source.split('\n').collect();
    let mut result_lines: Vec<String> = Vec::with_capacity(lines.len());

    // Compute line ranges for each top-level declaration
    let mut decl_line_ranges: Vec<(u32, u32)> = Vec::new();
    for decl in &doc.module.decls {
        let start = offset_to_position(source, decl.span.start);
        let end = offset_to_position(source, decl.span.end);
        decl_line_ranges.push((start.line, end.line));
    }
    // Also track import line ranges
    let mut import_line_ranges: Vec<(u32, u32)> = Vec::new();
    for imp in &doc.module.imports {
        let start = offset_to_position(source, imp.span.start);
        let end = offset_to_position(source, imp.span.end);
        import_line_ranges.push((start.line, end.line));
    }

    // Merge all block ranges (imports + declarations) sorted by start line
    let mut block_ranges: Vec<(u32, u32)> = Vec::new();
    block_ranges.extend_from_slice(&import_line_ranges);
    block_ranges.extend_from_slice(&decl_line_ranges);
    block_ranges.sort_by_key(|r| r.0);

    let mut i = 0;
    while i < lines.len() {
        let line_num = i as u32;

        // Check if this line is between two top-level blocks (a gap line)
        let in_block = block_ranges
            .iter()
            .any(|(start, end)| line_num >= *start && line_num <= *end);
        let prev_block_end = block_ranges
            .iter()
            .filter(|(_, end)| *end < line_num)
            .max_by_key(|(_, end)| *end);
        let next_block_start = block_ranges
            .iter()
            .filter(|(start, _)| *start > line_num)
            .min_by_key(|(start, _)| *start);

        if !in_block && lines[i].trim().is_empty() {
            // We're in a gap between blocks — check if this is part of
            // a run of blank lines that should be collapsed to exactly one
            let gap_start = i;
            while i < lines.len() && lines[i].trim().is_empty() {
                i += 1;
            }
            // Only emit a blank line if there are blocks on both sides
            if prev_block_end.is_some() && next_block_start.is_some() {
                result_lines.push(String::new());
            } else if prev_block_end.is_some() {
                // Trailing blank lines at end — skip (trailing newline added later)
            } else {
                // Leading blank lines — preserve one at most
                if gap_start == 0 {
                    // skip leading blank lines
                } else {
                    result_lines.push(String::new());
                }
            }
            continue;
        }

        // Collapse consecutive blank lines inside blocks to at most one
        if lines[i].trim().is_empty() && in_block {
            let mut blank_count = 0;
            while i < lines.len() && lines[i].trim().is_empty() {
                blank_count += 1;
                i += 1;
            }
            if blank_count > 0 {
                result_lines.push(String::new());
            }
            continue;
        }

        // Trim trailing whitespace
        result_lines.push(lines[i].trim_end().to_string());
        i += 1;
    }

    // Ensure trailing newline
    if result_lines.last().map_or(true, |l| !l.is_empty()) {
        result_lines.push(String::new());
    }

    let formatted = result_lines.join("\n");

    // Only return edits if something changed
    if formatted == *source {
        return None;
    }

    // Replace entire document
    let last_line = lines.len().saturating_sub(1) as u32;
    let last_col = lines.last().map_or(0, |l| l.len()) as u32;
    Some(vec![TextEdit {
        range: Range {
            start: Position::new(0, 0),
            end: Position::new(last_line, last_col),
        },
        new_text: formatted,
    }])
}

// ── Code Actions ────────────────────────────────────────────────────

fn handle_code_action(
    state: &ServerState,
    params: &CodeActionParams,
) -> Option<CodeActionResponse> {
    let uri = &params.text_document.uri;
    let doc = state.documents.get(uri)?;
    let mut actions = Vec::new();

    let range_start = position_to_offset(&doc.source, params.range.start);
    let range_end = position_to_offset(&doc.source, params.range.end);

    for decl in &doc.module.decls {
        // Only consider declarations overlapping the cursor range
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }

        // Action: Add type annotation to unannotated functions
        if let DeclKind::Fun { name, ty: None, .. } = &decl.node {
            if let Some(inferred) = doc.type_info.get(name) {
                let insert_pos = offset_to_position(&doc.source, decl.span.start);

                let mut changes = HashMap::new();
                changes.insert(
                    uri.clone(),
                    vec![TextEdit {
                        range: Range {
                            start: insert_pos,
                            end: insert_pos,
                        },
                        new_text: format!("{name} : {inferred}\n"),
                    }],
                );

                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: format!("Add type annotation: {inferred}"),
                    kind: Some(CodeActionKind::QUICKFIX),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    ..Default::default()
                }));
            }
        }

        // Action: Add type annotation to unannotated views/derived
        match &decl.node {
            DeclKind::View { name, ty: None, .. } | DeclKind::Derived { name, ty: None, .. } => {
                if let Some(inferred) = doc.type_info.get(name) {
                    let decl_text =
                        &doc.source[decl.span.start..decl.span.end.min(doc.source.len())];
                    if let Some(eq_pos) = decl_text.find('=') {
                        let insert_offset = decl.span.start + eq_pos;
                        let insert_pos = offset_to_position(&doc.source, insert_offset);

                        let mut changes = HashMap::new();
                        changes.insert(
                            uri.clone(),
                            vec![TextEdit {
                                range: Range {
                                    start: insert_pos,
                                    end: insert_pos,
                                },
                                new_text: format!(": {inferred} "),
                            }],
                        );

                        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                            title: format!("Add type annotation: {inferred}"),
                            kind: Some(CodeActionKind::QUICKFIX),
                            edit: Some(WorkspaceEdit {
                                changes: Some(changes),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }));
                    }
                }
            }
            _ => {}
        }

        // Action: Add missing trait methods to impl blocks
        if let DeclKind::Impl {
            trait_name, items, ..
        } = &decl.node
        {
            // Find the trait declaration to know which methods are required.
            // We need the full TraitItem (not just the name) so we can look up
            // each method's type signature for param-count and default body.
            let trait_items: Vec<&ast::TraitItem> = doc
                .module
                .decls
                .iter()
                .filter_map(|d| {
                    if let DeclKind::Trait {
                        name,
                        items: trait_items,
                        ..
                    } = &d.node
                    {
                        if name == trait_name {
                            return Some(trait_items);
                        }
                    }
                    None
                })
                .flatten()
                .filter(|item| {
                    matches!(
                        item,
                        ast::TraitItem::Method {
                            default_body: None,
                            ..
                        }
                    )
                })
                .collect();

            let impl_methods: HashSet<&str> = items
                .iter()
                .filter_map(|item| {
                    if let ast::ImplItem::Method { name, .. } = item {
                        Some(name.as_str())
                    } else {
                        None
                    }
                })
                .collect();

            let missing: Vec<&&ast::TraitItem> = trait_items
                .iter()
                .filter(|item| {
                    if let ast::TraitItem::Method { name, .. } = item {
                        !impl_methods.contains(name.as_str())
                    } else {
                        false
                    }
                })
                .collect();

            if !missing.is_empty() {
                let insert_pos = offset_to_position(&doc.source, decl.span.end);
                let stubs: String = missing
                    .iter()
                    .map(|item| build_trait_method_stub(item))
                    .collect();

                let mut changes = HashMap::new();
                changes.insert(
                    uri.clone(),
                    vec![TextEdit {
                        range: Range {
                            start: insert_pos,
                            end: insert_pos,
                        },
                        new_text: stubs,
                    }],
                );

                let missing_names: Vec<String> = missing
                    .iter()
                    .filter_map(|item| {
                        if let ast::TraitItem::Method { name, .. } = item {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: format!("Add missing methods: {}", missing_names.join(", ")),
                    kind: Some(CodeActionKind::QUICKFIX),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    ..Default::default()
                }));
            }
        }
    }

    // Diagnostic-attached quick fixes: suggest similar names for unknown identifiers
    let lsp_diags = &params.context.diagnostics;
    for diag in lsp_diags {
        let diag_offset = position_to_offset(&doc.source, diag.range.start);
        let msg = &diag.message;

        // Effect-related quick fixes
        if msg.contains("IO effects are not allowed inside atomic")
            || msg.contains("atomic block must interact with relations")
        {
            // Find the enclosing `atomic` expression in the AST and offer to
            // unwrap it (replace `atomic expr` with `expr`).
            if let Some((atomic_span, inner_text)) =
                find_enclosing_atomic_expr(&doc.module, &doc.source, diag_offset)
            {
                let mut changes = HashMap::new();
                changes.insert(
                    uri.clone(),
                    vec![TextEdit {
                        range: span_to_range(atomic_span, &doc.source),
                        new_text: inner_text,
                    }],
                );
                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: "Remove `atomic` wrapper".to_string(),
                    kind: Some(CodeActionKind::QUICKFIX),
                    diagnostics: Some(vec![diag.clone()]),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    ..Default::default()
                }));
            }

            // Additionally, if the diagnostic is "IO in atomic", suggest
            // wrapping the offending IO call in `fork` (fire-and-forget) so it
            // runs outside the transaction.
            if msg.contains("IO effects are not allowed inside atomic") {
                if let Some(call_span) = find_io_call_in_range(&doc, diag_offset) {
                    let inner_text =
                        doc.source[call_span.start..call_span.end.min(doc.source.len())]
                            .to_string();
                    let mut changes = HashMap::new();
                    changes.insert(
                        uri.clone(),
                        vec![TextEdit {
                            range: span_to_range(call_span, &doc.source),
                            new_text: format!("fork ({inner_text})"),
                        }],
                    );
                    actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                        title: "Wrap IO in `fork`".to_string(),
                        kind: Some(CodeActionKind::QUICKFIX),
                        diagnostics: Some(vec![diag.clone()]),
                        edit: Some(WorkspaceEdit {
                            changes: Some(changes),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }));
                }
            }
        }

        // Quick fix for "inferred effects exceed declared effects"
        if msg.contains("inferred effects exceed declared effects") {
            // Extract the inferred-effects line from the diagnostic message
            if let Some(inferred) = extract_effect_set_from_message(msg, "inferred effects:") {
                // Find the declaration whose span overlaps this diagnostic
                if let Some((decl, fun_name)) = doc
                    .module
                    .decls
                    .iter()
                    .find_map(|d| match &d.node {
                        DeclKind::Fun {
                            name, ty: Some(_), ..
                        } if d.span.start <= diag_offset && diag_offset < d.span.end => {
                            Some((d, name.clone()))
                        }
                        _ => None,
                    })
                {
                    if let Some(edit) = build_effect_widen_edit(decl, &doc.source, &inferred) {
                        let mut changes = HashMap::new();
                        changes.insert(uri.clone(), vec![edit]);
                        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                            title: format!("Widen declared effects to: {inferred}"),
                            kind: Some(CodeActionKind::QUICKFIX),
                            diagnostics: Some(vec![diag.clone()]),
                            edit: Some(WorkspaceEdit {
                                changes: Some(changes),
                                ..Default::default()
                            }),
                            is_preferred: Some(true),
                            ..Default::default()
                        }));
                        let _ = fun_name; // for diagnostics in future
                    }
                }
            }
        }

        // Pattern: "Unknown variable/type/constructor" → suggest similar names
        if msg.contains("nknown") || msg.contains("ndefined") || msg.contains("not found") || msg.contains("unresolved") {
            // Extract the unknown name from the diagnostic range
            let unknown_name = word_at_position(&doc.source, diag.range.start)
                .unwrap_or("");
            if !unknown_name.is_empty() {
                // Find similar names using edit distance
                let mut candidates: Vec<(&str, usize)> = Vec::new();
                for name in doc.definitions.keys() {
                    let dist = edit_distance(unknown_name, name);
                    if dist <= 2 && dist > 0 {
                        candidates.push((name, dist));
                    }
                }
                // Also check builtins
                for name in BUILTINS {
                    let dist = edit_distance(unknown_name, name);
                    if dist <= 2 && dist > 0 {
                        candidates.push((name, dist));
                    }
                }
                candidates.sort_by_key(|(_, d)| *d);

                for (suggestion, _) in candidates.iter().take(3) {
                    let mut changes = HashMap::new();
                    changes.insert(
                        uri.clone(),
                        vec![TextEdit {
                            range: diag.range,
                            new_text: suggestion.to_string(),
                        }],
                    );
                    actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                        title: format!("Did you mean `{suggestion}`?"),
                        kind: Some(CodeActionKind::QUICKFIX),
                        diagnostics: Some(vec![diag.clone()]),
                        edit: Some(WorkspaceEdit {
                            changes: Some(changes),
                            ..Default::default()
                        }),
                        is_preferred: Some(candidates.first().map_or(false, |(s, _)| *s == *suggestion)),
                        ..Default::default()
                    }));
                }
            }
        }
    }

    // Action: Fill case arms — check if cursor is inside a case expression
    for decl in &doc.module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                find_case_actions(body, doc, uri, range_start, range_end, &mut actions);
            }
            DeclKind::Fun { body: None, .. } => {}
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        find_case_actions(body, doc, uri, range_start, range_end, &mut actions);
                    }
                }
            }
            _ => {}
        }
    }

    // Action: Extract variable — if a non-trivial expression is selected, offer to extract it
    if range_start != range_end {
        let selected_text = &doc.source[range_start..range_end.min(doc.source.len())];
        let trimmed = selected_text.trim();
        // Only offer for non-trivial selections (not just a name or empty)
        if !trimmed.is_empty()
            && trimmed.len() > 1
            && !trimmed.chars().all(|c| c.is_alphanumeric() || c == '_')
        {
            // Find the line where the selection starts to determine indentation
            let line_start = doc.source[..range_start]
                .rfind('\n')
                .map(|p| p + 1)
                .unwrap_or(0);
            let current_line = &doc.source[line_start..];
            let indent = current_line.len() - current_line.trim_start().len();
            let indent_str = " ".repeat(indent);

            let mut changes = HashMap::new();
            changes.insert(
                uri.clone(),
                vec![
                    // Insert let binding before the current line
                    TextEdit {
                        range: Range {
                            start: offset_to_position(&doc.source, line_start),
                            end: offset_to_position(&doc.source, line_start),
                        },
                        new_text: format!("{indent_str}let extracted = {trimmed}\n"),
                    },
                    // Replace the selected expression with the variable name
                    TextEdit {
                        range: params.range,
                        new_text: "extracted".to_string(),
                    },
                ],
            );

            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                title: "Extract to let binding".to_string(),
                kind: Some(CodeActionKind::REFACTOR_EXTRACT),
                edit: Some(WorkspaceEdit {
                    changes: Some(changes),
                    ..Default::default()
                }),
                ..Default::default()
            }));

            // Extract function: wrap selected expression in a named function
            let mut fn_changes = HashMap::new();
            // Find free variables in the selected text that are bound in scope
            let free_vars = find_free_vars_in_selection(doc, range_start, range_end);
            let params_str = if free_vars.is_empty() {
                String::new()
            } else {
                format!(" {}", free_vars.join(" "))
            };
            let call_args = if free_vars.is_empty() {
                String::new()
            } else {
                format!(" {}", free_vars.join(" "))
            };

            // Find the enclosing top-level declaration to place the function before it
            let fn_insert_offset = doc
                .module
                .decls
                .iter()
                .find(|d| d.span.start <= range_start && range_end <= d.span.end)
                .map(|d| d.span.start)
                .unwrap_or(0);
            let fn_insert_pos = offset_to_position(&doc.source, fn_insert_offset);

            fn_changes.insert(
                uri.clone(),
                vec![
                    // Insert new function before the enclosing declaration
                    TextEdit {
                        range: Range {
                            start: fn_insert_pos,
                            end: fn_insert_pos,
                        },
                        new_text: format!(
                            "extracted{params_str} = {trimmed}\n\n"
                        ),
                    },
                    // Replace the selected expression with a call
                    TextEdit {
                        range: params.range,
                        new_text: format!("extracted{call_args}"),
                    },
                ],
            );

            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                title: "Extract to function".to_string(),
                kind: Some(CodeActionKind::REFACTOR_EXTRACT),
                edit: Some(WorkspaceEdit {
                    changes: Some(fn_changes),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }
    }

    // Action: Inline variable — if cursor is on a let binding's name, offer to inline it
    for decl in &doc.module.decls {
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                find_inline_actions(body, doc, uri, range_start, &mut actions);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        find_inline_actions(body, doc, uri, range_start, &mut actions);
                    }
                }
            }
            _ => {}
        }
    }

    // Action: Convert lambda to named function
    for decl in &doc.module.decls {
        if decl.span.end < range_start || decl.span.start > range_end {
            continue;
        }
        if let DeclKind::Fun { name, body: Some(body), ty: None, .. } = &decl.node {
            // Check if the body is a lambda — offer to convert to direct function params
            if let ast::ExprKind::Lambda { params: lam_params, body: lam_body } = &body.node {
                let param_names: Vec<String> = lam_params
                    .iter()
                    .map(|p| pat_to_string(p, &doc.source))
                    .collect();
                let body_text = &doc.source[lam_body.span.start..lam_body.span.end.min(doc.source.len())];

                let mut changes = HashMap::new();
                changes.insert(
                    uri.clone(),
                    vec![TextEdit {
                        range: span_to_range(decl.span, &doc.source),
                        new_text: format!(
                            "{name} {} = {body_text}",
                            param_names.join(" ")
                        ),
                    }],
                );

                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: "Convert lambda to function parameters".to_string(),
                    kind: Some(CodeActionKind::REFACTOR_REWRITE),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    ..Default::default()
                }));
            }
        }
    }

    // Action: Organize imports — remove unused, sort, deduplicate
    if !doc.module.imports.is_empty() {
        // Collect names referenced in the document to detect unused imports.
        let referenced = collect_referenced_names(&doc.module);

        // For each import, check whether any of its top-level names are referenced.
        // We need to parse each imported file to know what it exports.
        let unused_imports: HashSet<String> = doc
            .module
            .imports
            .iter()
            .filter(|imp| !import_is_used(imp, doc, &referenced))
            .map(|imp| imp.path.clone())
            .collect();

        let original_paths: Vec<String> =
            doc.module.imports.iter().map(|i| i.path.clone()).collect();

        let mut kept_paths: Vec<String> = original_paths
            .iter()
            .filter(|p| !unused_imports.contains(p.as_str()))
            .cloned()
            .collect();
        kept_paths.sort();
        kept_paths.dedup();

        // Only emit the action if something would change
        let changed = kept_paths != original_paths;
        if changed {
            let first_import = &doc.module.imports[0];
            let last_import = doc.module.imports.last().unwrap();
            let import_range = Range {
                start: offset_to_position(&doc.source, first_import.span.start),
                end: offset_to_position(&doc.source, last_import.span.end),
            };

            let new_text = if kept_paths.is_empty() {
                String::new()
            } else {
                kept_paths
                    .iter()
                    .map(|p| format!("import {p}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            };

            let mut changes = HashMap::new();
            changes.insert(uri.clone(), vec![TextEdit { range: import_range, new_text }]);

            let title = if !unused_imports.is_empty() {
                format!(
                    "Organize imports (remove {} unused)",
                    unused_imports.len()
                )
            } else {
                "Organize imports".to_string()
            };

            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                title,
                kind: Some(CodeActionKind::SOURCE_ORGANIZE_IMPORTS),
                edit: Some(WorkspaceEdit {
                    changes: Some(changes),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }

        // Also offer per-import "Remove unused import" actions for each unused
        // import (single-shot, simpler than the bulk organize action).
        for imp in &doc.module.imports {
            if !unused_imports.contains(&imp.path) {
                continue;
            }
            // Compute the line range to remove (include trailing newline)
            let line_start = doc.source[..imp.span.start]
                .rfind('\n')
                .map(|p| p + 1)
                .unwrap_or(imp.span.start);
            let line_end = doc.source[imp.span.end..]
                .find('\n')
                .map(|p| imp.span.end + p + 1)
                .unwrap_or(imp.span.end);
            let mut changes = HashMap::new();
            changes.insert(
                uri.clone(),
                vec![TextEdit {
                    range: Range {
                        start: offset_to_position(&doc.source, line_start),
                        end: offset_to_position(&doc.source, line_end),
                    },
                    new_text: String::new(),
                }],
            );
            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                title: format!("Remove unused import `{}`", imp.path),
                kind: Some(CodeActionKind::QUICKFIX),
                edit: Some(WorkspaceEdit {
                    changes: Some(changes),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }
    }

    if actions.is_empty() {
        None
    } else {
        Some(actions)
    }
}

/// Find case expressions at the cursor and offer to fill missing arms.
fn find_case_actions(
    expr: &ast::Expr,
    doc: &DocumentState,
    uri: &Uri,
    range_start: usize,
    range_end: usize,
    actions: &mut Vec<CodeActionOrCommand>,
) {
    if expr.span.end < range_start || expr.span.start > range_end {
        return;
    }

    if let ast::ExprKind::Case { scrutinee, arms } = &expr.node {
        // Try to find the ADT type of the scrutinee
        let scrutinee_type = match &scrutinee.node {
            ast::ExprKind::Var(name) => doc
                .local_type_info
                .iter()
                .find(|(span, _)| {
                    let src = &doc.source[span.start..span.end.min(doc.source.len())];
                    src == name
                })
                .map(|(_, ty)| ty.clone())
                .or_else(|| doc.type_info.get(name).cloned()),
            _ => None,
        };

        if let Some(type_str) = scrutinee_type {
            // Extract the principal type name (handles parametrized types like
            // `Maybe Int`, `Result Text Person`, `[Shape]`, `IO {} Maybe`)
            let type_name = extract_principal_type_name(&type_str);

            if let Some(type_name) = type_name {
                // Find the data declaration for this type
                for decl in &doc.module.decls {
                    if let DeclKind::Data {
                        name, constructors, ..
                    } = &decl.node
                    {
                        if *name != type_name {
                            continue;
                        }
                        let existing: HashSet<String> = arms
                            .iter()
                            .filter_map(|arm| match &arm.pat.node {
                                ast::PatKind::Constructor { name, .. } => Some(name.clone()),
                                _ => None,
                            })
                            .collect();

                        let missing: Vec<&ast::ConstructorDef> = constructors
                            .iter()
                            .filter(|c| !existing.contains(&c.name))
                            .collect();

                        if missing.is_empty() {
                            continue;
                        }

                        // Determine indentation from the existing arms or the case
                        // expression itself, so generated arms align nicely.
                        let arm_indent = arm_indentation(expr, arms, &doc.source);
                        // Default body: the first bound variable, or `todo` if
                        // the constructor is nullary. `todo` is intentionally an
                        // undefined identifier so the user sees a clear error.
                        let new_arms: String = missing
                            .iter()
                            .map(|c| build_case_arm(c, &arm_indent))
                            .collect();

                        let insert_pos = offset_to_position(&doc.source, expr.span.end);
                        let mut changes = HashMap::new();
                        changes.insert(
                            uri.clone(),
                            vec![TextEdit {
                                range: Range {
                                    start: insert_pos,
                                    end: insert_pos,
                                },
                                new_text: new_arms,
                            }],
                        );

                        let names: Vec<&str> =
                            missing.iter().map(|c| c.name.as_str()).collect();
                        actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                            title: format!("Add missing case arms: {}", names.join(", ")),
                            kind: Some(CodeActionKind::QUICKFIX),
                            edit: Some(WorkspaceEdit {
                                changes: Some(changes),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }));
                        break;
                    }
                }
            }
        }
    }

    // Recurse into sub-expressions
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            find_case_actions(func, doc, uri, range_start, range_end, actions);
            find_case_actions(arg, doc, uri, range_start, range_end, actions);
        }
        ast::ExprKind::Lambda { body, .. } => {
            find_case_actions(body, doc, uri, range_start, range_end, actions);
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => {
                        find_case_actions(expr, doc, uri, range_start, range_end, actions);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        find_case_actions(e, doc, uri, range_start, range_end, actions);
                    }
                    _ => {}
                }
            }
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            find_case_actions(cond, doc, uri, range_start, range_end, actions);
            find_case_actions(then_branch, doc, uri, range_start, range_end, actions);
            find_case_actions(else_branch, doc, uri, range_start, range_end, actions);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            find_case_actions(scrutinee, doc, uri, range_start, range_end, actions);
            for arm in arms {
                find_case_actions(&arm.body, doc, uri, range_start, range_end, actions);
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => {
            find_case_actions(e, doc, uri, range_start, range_end, actions);
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
            find_case_actions(target, doc, uri, range_start, range_end, actions);
            find_case_actions(value, doc, uri, range_start, range_end, actions);
        }
        _ => {}
    }
}

/// Collect every identifier name that appears in expressions, types, or
/// patterns in the module. Used to detect unused imports.
fn collect_referenced_names(module: &Module) -> HashSet<String> {
    let mut names = HashSet::new();
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body),
                ty,
                ..
            } => {
                collect_names_in_expr(body, &mut names);
                if let Some(scheme) = ty {
                    collect_names_in_type(&scheme.ty, &mut names);
                }
            }
            DeclKind::View { body, ty, .. } | DeclKind::Derived { body, ty, .. } => {
                collect_names_in_expr(body, &mut names);
                if let Some(scheme) = ty {
                    collect_names_in_type(&scheme.ty, &mut names);
                }
            }
            DeclKind::Source { ty, .. } => {
                collect_names_in_type(ty, &mut names);
            }
            DeclKind::TypeAlias { ty, .. } => {
                collect_names_in_type(ty, &mut names);
            }
            DeclKind::Data { constructors, .. } => {
                for ctor in constructors {
                    for f in &ctor.fields {
                        collect_names_in_type(&f.value, &mut names);
                    }
                }
            }
            DeclKind::Trait {
                items, supertraits, ..
            } => {
                for sup in supertraits {
                    names.insert(sup.trait_name.clone());
                }
                for item in items {
                    if let ast::TraitItem::Method {
                        ty,
                        default_body: Some(b),
                        ..
                    } = item
                    {
                        collect_names_in_type(&ty.ty, &mut names);
                        collect_names_in_expr(b, &mut names);
                    } else if let ast::TraitItem::Method { ty, .. } = item {
                        collect_names_in_type(&ty.ty, &mut names);
                    }
                }
            }
            DeclKind::Impl {
                trait_name,
                args,
                items,
                constraints,
                ..
            } => {
                names.insert(trait_name.clone());
                for c in constraints {
                    names.insert(c.trait_name.clone());
                }
                for arg in args {
                    collect_names_in_type(arg, &mut names);
                }
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        collect_names_in_expr(body, &mut names);
                    }
                }
            }
            DeclKind::Migrate {
                using_fn,
                from_ty,
                to_ty,
                ..
            } => {
                collect_names_in_expr(using_fn, &mut names);
                collect_names_in_type(from_ty, &mut names);
                collect_names_in_type(to_ty, &mut names);
            }
            DeclKind::Route { entries, .. } => {
                for e in entries {
                    for seg in &e.path {
                        if let ast::PathSegment::Param { ty, .. } = seg {
                            collect_names_in_type(ty, &mut names);
                        }
                    }
                    for f in &e.body_fields {
                        collect_names_in_type(&f.value, &mut names);
                    }
                    for f in &e.query_params {
                        collect_names_in_type(&f.value, &mut names);
                    }
                    for f in &e.request_headers {
                        collect_names_in_type(&f.value, &mut names);
                    }
                    if let Some(t) = &e.response_ty {
                        collect_names_in_type(t, &mut names);
                    }
                    for f in &e.response_headers {
                        collect_names_in_type(&f.value, &mut names);
                    }
                }
            }
            _ => {}
        }
    }
    names
}

fn collect_names_in_expr(expr: &ast::Expr, out: &mut HashSet<String>) {
    match &expr.node {
        ast::ExprKind::Var(n)
        | ast::ExprKind::Constructor(n)
        | ast::ExprKind::SourceRef(n)
        | ast::ExprKind::DerivedRef(n) => {
            out.insert(n.clone());
        }
        ast::ExprKind::Lambda { body, .. } => collect_names_in_expr(body, out),
        ast::ExprKind::App { func, arg } => {
            collect_names_in_expr(func, out);
            collect_names_in_expr(arg, out);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            collect_names_in_expr(lhs, out);
            collect_names_in_expr(rhs, out);
        }
        ast::ExprKind::UnaryOp { operand, .. } => collect_names_in_expr(operand, out),
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            collect_names_in_expr(cond, out);
            collect_names_in_expr(then_branch, out);
            collect_names_in_expr(else_branch, out);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_names_in_expr(scrutinee, out);
            for arm in arms {
                collect_names_in_pat(&arm.pat, out);
                collect_names_in_expr(&arm.body, out);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { pat, expr } | ast::StmtKind::Let { pat, expr } => {
                        collect_names_in_pat(pat, out);
                        collect_names_in_expr(expr, out);
                    }
                    ast::StmtKind::Where { cond } => collect_names_in_expr(cond, out),
                    ast::StmtKind::GroupBy { key } => collect_names_in_expr(key, out),
                    ast::StmtKind::Expr(e) => collect_names_in_expr(e, out),
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => collect_names_in_expr(e, out),
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
            collect_names_in_expr(target, out);
            collect_names_in_expr(value, out);
        }
        ast::ExprKind::Record(fields) => {
            for f in fields {
                collect_names_in_expr(&f.value, out);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_names_in_expr(base, out);
            for f in fields {
                collect_names_in_expr(&f.value, out);
            }
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                collect_names_in_expr(e, out);
            }
        }
        ast::ExprKind::FieldAccess { expr, .. } => collect_names_in_expr(expr, out),
        ast::ExprKind::At { relation, time } => {
            collect_names_in_expr(relation, out);
            collect_names_in_expr(time, out);
        }
        ast::ExprKind::UnitLit { value, .. } => collect_names_in_expr(value, out),
        ast::ExprKind::Annot { expr, ty } => {
            collect_names_in_expr(expr, out);
            collect_names_in_type(ty, out);
        }
        ast::ExprKind::Lit(_) => {}
    }
}

fn collect_names_in_pat(pat: &ast::Pat, out: &mut HashSet<String>) {
    match &pat.node {
        ast::PatKind::Constructor { name, payload } => {
            out.insert(name.clone());
            collect_names_in_pat(payload, out);
        }
        ast::PatKind::Record(fields) => {
            for f in fields {
                if let Some(p) = &f.pattern {
                    collect_names_in_pat(p, out);
                }
            }
        }
        ast::PatKind::List(pats) => {
            for p in pats {
                collect_names_in_pat(p, out);
            }
        }
        _ => {}
    }
}

fn collect_names_in_type(ty: &ast::Type, out: &mut HashSet<String>) {
    match &ty.node {
        TypeKind::Named(n) => {
            out.insert(n.clone());
        }
        TypeKind::App { func, arg } => {
            collect_names_in_type(func, out);
            collect_names_in_type(arg, out);
        }
        TypeKind::Record { fields, .. } => {
            for f in fields {
                collect_names_in_type(&f.value, out);
            }
        }
        TypeKind::Relation(inner) => collect_names_in_type(inner, out),
        TypeKind::Function { param, result } => {
            collect_names_in_type(param, out);
            collect_names_in_type(result, out);
        }
        TypeKind::Variant { constructors, .. } => {
            for c in constructors {
                for f in &c.fields {
                    collect_names_in_type(&f.value, out);
                }
            }
        }
        TypeKind::Effectful { ty, .. } => collect_names_in_type(ty, out),
        TypeKind::IO { ty, .. } => collect_names_in_type(ty, out),
        TypeKind::UnitAnnotated { base, .. } => collect_names_in_type(base, out),
        TypeKind::Refined { base, .. } => collect_names_in_type(base, out),
        TypeKind::Forall { ty, .. } => collect_names_in_type(ty, out),
        TypeKind::Var(_) | TypeKind::Hole => {}
    }
}

/// Decide whether an import is used by checking whether any of its top-level
/// definitions appear in the document's referenced-names set. If we can't parse
/// the imported file, conservatively treat the import as used.
fn import_is_used(
    imp: &ast::Import,
    doc: &DocumentState,
    referenced: &HashSet<String>,
) -> bool {
    // Fast path: selective imports list the names directly
    if let Some(items) = &imp.items {
        return items.iter().any(|i| referenced.contains(&i.name));
    }

    // Otherwise scan the import's exported declarations from the cache
    for (name, origin_path) in &doc.import_origins {
        if origin_path == &imp.path && referenced.contains(name) {
            return true;
        }
    }
    // Also check direct names from import_defs (in case origins aren't tracked)
    for (name, (path, _)) in &doc.import_defs {
        // Reconstruct the "origin" from path: this is best-effort, prefer origins
        let origin = doc.import_origins.get(name);
        if origin == Some(&imp.path) && referenced.contains(name) {
            return true;
        }
        let _ = path;
    }
    false
}

/// Find the enclosing `atomic expr` and return `(atomic_span, inner_source_text)`
/// so we can replace `atomic e` with `e`. Returns None if no atomic wraps the
/// given offset.
fn find_enclosing_atomic_expr(
    module: &Module,
    source: &str,
    offset: usize,
) -> Option<(Span, String)> {
    fn walk(expr: &ast::Expr, source: &str, offset: usize, best: &mut Option<(Span, String)>) {
        if expr.span.start > offset || offset > expr.span.end {
            return;
        }
        if let ast::ExprKind::Atomic(inner) = &expr.node {
            let inner_text =
                source[inner.span.start..inner.span.end.min(source.len())].to_string();
            // Track the smallest enclosing atomic
            let size = expr.span.end - expr.span.start;
            if best
                .as_ref()
                .map_or(true, |b: &(Span, String)| size < b.0.end - b.0.start)
            {
                *best = Some((expr.span, inner_text));
            }
        }
        // Recurse
        match &expr.node {
            ast::ExprKind::App { func, arg } => {
                walk(func, source, offset, best);
                walk(arg, source, offset, best);
            }
            ast::ExprKind::Lambda { body, .. } => walk(body, source, offset, best),
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                walk(lhs, source, offset, best);
                walk(rhs, source, offset, best);
            }
            ast::ExprKind::UnaryOp { operand, .. } => walk(operand, source, offset, best),
            ast::ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } => {
                walk(cond, source, offset, best);
                walk(then_branch, source, offset, best);
                walk(else_branch, source, offset, best);
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                walk(scrutinee, source, offset, best);
                for arm in arms {
                    walk(&arm.body, source, offset, best);
                }
            }
            ast::ExprKind::Do(stmts) => {
                for stmt in stmts {
                    match &stmt.node {
                        ast::StmtKind::Bind { expr, .. }
                        | ast::StmtKind::Let { expr, .. }
                        | ast::StmtKind::Expr(expr)
                        | ast::StmtKind::Where { cond: expr } => walk(expr, source, offset, best),
                        ast::StmtKind::GroupBy { key } => walk(key, source, offset, best),
                    }
                }
            }
            ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => walk(e, source, offset, best),
            ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
                walk(target, source, offset, best);
                walk(value, source, offset, best);
            }
            ast::ExprKind::Record(fields) => {
                for f in fields {
                    walk(&f.value, source, offset, best);
                }
            }
            ast::ExprKind::RecordUpdate { base, fields } => {
                walk(base, source, offset, best);
                for f in fields {
                    walk(&f.value, source, offset, best);
                }
            }
            ast::ExprKind::List(elems) => {
                for e in elems {
                    walk(e, source, offset, best);
                }
            }
            ast::ExprKind::FieldAccess { expr, .. } => walk(expr, source, offset, best),
            ast::ExprKind::At { relation, time } => {
                walk(relation, source, offset, best);
                walk(time, source, offset, best);
            }
            ast::ExprKind::Annot { expr, .. } => walk(expr, source, offset, best),
            ast::ExprKind::UnitLit { value, .. } => walk(value, source, offset, best),
            _ => {}
        }
    }

    let mut best: Option<(Span, String)> = None;
    for decl in &module.decls {
        if decl.span.start > offset || offset > decl.span.end {
            continue;
        }
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => walk(body, source, offset, &mut best),
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk(body, source, offset, &mut best);
                    }
                }
            }
            _ => {}
        }
    }
    best
}

/// Locate an effectful builtin call at or near the given offset, for `fork`-wrap suggestions.
fn find_io_call_in_range(doc: &DocumentState, offset: usize) -> Option<Span> {
    // Scan literal/reference info: find a Var span that names an effectful builtin
    // and whose containing AppChain encloses the offset.
    for decl in &doc.module.decls {
        if decl.span.start > offset || offset > decl.span.end {
            continue;
        }
        let body_opt: Option<&ast::Expr> = match &decl.node {
            DeclKind::Fun { body: Some(b), .. }
            | DeclKind::View { body: b, .. }
            | DeclKind::Derived { body: b, .. } => Some(b),
            _ => None,
        };
        if let Some(body) = body_opt {
            if let Some(span) = find_io_call(body, offset) {
                return Some(span);
            }
        }
        if let DeclKind::Impl { items, .. } = &decl.node {
            for item in items {
                if let ast::ImplItem::Method { body, .. } = item {
                    if let Some(span) = find_io_call(body, offset) {
                        return Some(span);
                    }
                }
            }
        }
    }
    None
}

fn find_io_call(expr: &ast::Expr, offset: usize) -> Option<Span> {
    if expr.span.start > offset || offset > expr.span.end {
        return None;
    }
    // If this expression is an App whose head is an effectful builtin, return
    // the entire App's span.
    if let ast::ExprKind::App { .. } = &expr.node {
        let mut head = expr;
        while let ast::ExprKind::App { func, .. } = &head.node {
            head = func;
        }
        if let ast::ExprKind::Var(name) = &head.node {
            if EFFECTFUL_BUILTINS.contains(&name.as_str()) {
                return Some(expr.span);
            }
        }
    }
    // Recurse, keeping the smallest match
    let mut best: Option<Span> = None;
    let consider = |s: Span, best: &mut Option<Span>| {
        if best
            .as_ref()
            .map_or(true, |b| s.end - s.start < b.end - b.start)
        {
            *best = Some(s);
        }
    };
    let recur = |e: &ast::Expr, best: &mut Option<Span>| {
        if let Some(s) = find_io_call(e, offset) {
            consider(s, best);
        }
    };
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            recur(func, &mut best);
            recur(arg, &mut best);
        }
        ast::ExprKind::Lambda { body, .. } => recur(body, &mut best),
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            recur(lhs, &mut best);
            recur(rhs, &mut best);
        }
        ast::ExprKind::UnaryOp { operand, .. } => recur(operand, &mut best),
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            recur(cond, &mut best);
            recur(then_branch, &mut best);
            recur(else_branch, &mut best);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            recur(scrutinee, &mut best);
            for arm in arms {
                recur(&arm.body, &mut best);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. }
                    | ast::StmtKind::Let { expr, .. }
                    | ast::StmtKind::Expr(expr)
                    | ast::StmtKind::Where { cond: expr } => recur(expr, &mut best),
                    ast::StmtKind::GroupBy { key } => recur(key, &mut best),
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => recur(e, &mut best),
        _ => {}
    }
    best
}

/// Pull a `{...}` block out of an effects diagnostic note like
/// `inferred effects: {console, reads *foo}`.
fn extract_effect_set_from_message(msg: &str, prefix: &str) -> Option<String> {
    let start = msg.find(prefix)? + prefix.len();
    let rest = msg[start..].trim_start();
    let open = rest.find('{')?;
    let close = rest[open..].find('}')?;
    Some(rest[open..=open + close].to_string())
}

/// Build a TextEdit that widens a function's declared effects to a target set.
/// Looks for the `: ... -> ...` signature in the source and rewrites the head.
fn build_effect_widen_edit(decl: &ast::Decl, source: &str, target_effects: &str) -> Option<TextEdit> {
    // The strategy: find the type annotation signature line that looks like
    // `name : ...` within the declaration span and replace the existing IO
    // effect set or insert one if none exists. We do a minimal textual rewrite
    // rather than re-rendering the whole type, to preserve user formatting.
    let decl_text = source.get(decl.span.start..decl.span.end.min(source.len()))?;
    // Find `: ` after the function name to locate the start of the type signature
    let colon = decl_text.find(": ")?;
    let after_colon = &decl_text[colon + 2..];
    // Find an existing IO effect set: `IO {...}`
    let abs_after_colon = decl.span.start + colon + 2;
    if let Some(io_pos) = after_colon.find("IO {") {
        let abs_io = abs_after_colon + io_pos;
        // Find the matching `}`
        let depth_start = abs_io + 3; // position of `{`
        let bytes = source.as_bytes();
        let mut depth = 0i32;
        for i in depth_start..source.len() {
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        // Replace `{...}` with target effects (which already
                        // includes braces).
                        return Some(TextEdit {
                            range: Range {
                                start: offset_to_position(source, depth_start),
                                end: offset_to_position(source, i + 1),
                            },
                            new_text: target_effects.to_string(),
                        });
                    }
                }
                _ => {}
            }
        }
    }
    // No existing IO effects: insert IO before the result type. We just append
    // a comment hint at the end of the signature line so the user can review.
    None
}

/// Build a trait method stub `name p1 p2 = todo` from a trait method declaration.
/// Counts arrows in the type signature to determine arity, then synthesizes
/// fresh `a`, `b`, `c`... parameter names.
fn build_trait_method_stub(item: &ast::TraitItem) -> String {
    let (name, arity) = match item {
        ast::TraitItem::Method { name, ty, .. } => {
            let arity = count_function_arity(&ty.ty);
            (name.clone(), arity)
        }
        _ => return String::new(),
    };
    let params: Vec<String> = (0..arity)
        .map(|i| {
            // Generate a, b, c, ... aa, ab, ...
            let mut s = String::new();
            let mut n = i;
            loop {
                s.insert(0, (b'a' + (n % 26) as u8) as char);
                n = n / 26;
                if n == 0 {
                    break;
                }
                n -= 1;
            }
            s
        })
        .collect();
    let params_str = if params.is_empty() {
        String::new()
    } else {
        format!(" {}", params.join(" "))
    };
    format!("\n  {name}{params_str} = todo")
}

/// Count the arity of a function type by walking the arrow spine.
/// `Int -> Text -> Bool` → 2.
fn count_function_arity(ty: &ast::Type) -> usize {
    let mut count = 0;
    let mut cur = ty;
    loop {
        match &cur.node {
            ast::TypeKind::Function { result, .. } => {
                count += 1;
                cur = result;
            }
            // Look through Forall, IO, and Effectful wrappers
            ast::TypeKind::Forall { ty: inner, .. } => cur = inner,
            ast::TypeKind::IO { ty: inner, .. } => cur = inner,
            ast::TypeKind::Effectful { ty: inner, .. } => cur = inner,
            _ => break,
        }
    }
    count
}

/// Compute the indentation prefix for a new case arm, matching the existing arms
/// or falling back to a default indent relative to the case expression.
fn arm_indentation(case_expr: &ast::Expr, arms: &[ast::CaseArm], source: &str) -> String {
    // Prefer the indentation of an existing arm
    if let Some(arm) = arms.first() {
        let line_start = source[..arm.pat.span.start]
            .rfind('\n')
            .map(|p| p + 1)
            .unwrap_or(0);
        let prefix = &source[line_start..arm.pat.span.start];
        if prefix.chars().all(char::is_whitespace) {
            return format!("\n{prefix}");
        }
    }
    // Fall back: case expression's column + 2
    let line_start = source[..case_expr.span.start]
        .rfind('\n')
        .map(|p| p + 1)
        .unwrap_or(0);
    let case_col = case_expr.span.start - line_start;
    format!("\n{}", " ".repeat(case_col + 2))
}

/// Build a single case arm string for the given constructor.
/// Bodies use bound-field references when available (e.g. `Just {value} -> value`
/// for return-the-value-as-is) or an undefined `todo` placeholder otherwise,
/// which produces a clear "unknown variable" error rather than a parse error.
fn build_case_arm(c: &ast::ConstructorDef, indent: &str) -> String {
    if c.fields.is_empty() {
        format!("{indent}{} {{}} -> todo", c.name)
    } else {
        let field_names: Vec<&str> = c.fields.iter().map(|f| f.name.as_str()).collect();
        // Default body: the first bound field name (often the right type for
        // identity-style mappings). User can edit; `todo` is the safe fallback.
        let body = field_names[0];
        format!(
            "{indent}{} {{{}}} -> {body}",
            c.name,
            field_names.join(", ")
        )
    }
}

/// Find free variables in a selection that are bound in surrounding scope.
fn find_free_vars_in_selection(
    doc: &DocumentState,
    start: usize,
    end: usize,
) -> Vec<String> {
    let mut free_vars = Vec::new();
    let mut seen = HashSet::new();

    // Check all references that start within the selection range
    for (usage_span, _def_span) in &doc.references {
        if usage_span.start >= start && usage_span.end <= end {
            let name = &doc.source[usage_span.start..usage_span.end.min(doc.source.len())];
            // Only include if it looks like a lowercase variable (not a constructor/type)
            if !name.is_empty()
                && name.chars().next().map_or(false, |c| c.is_lowercase())
                && !seen.contains(name)
            {
                // Check it's a local binding, not a top-level definition
                if doc.local_type_info.keys().any(|span| {
                    span.start < start
                        && doc.source.get(span.start..span.end.min(doc.source.len())) == Some(name)
                }) {
                    seen.insert(name.to_string());
                    free_vars.push(name.to_string());
                }
            }
        }
    }

    free_vars
}

/// Find inline variable opportunities in do-block let bindings.
fn find_inline_actions(
    expr: &ast::Expr,
    doc: &DocumentState,
    uri: &Uri,
    cursor_offset: usize,
    actions: &mut Vec<CodeActionOrCommand>,
) {
    if expr.span.end < cursor_offset || expr.span.start > cursor_offset {
        return;
    }

    if let ast::ExprKind::Do(stmts) = &expr.node {
        for stmt in stmts {
            if let ast::StmtKind::Let { pat, expr: value_expr } = &stmt.node {
                // Check if cursor is on the let binding
                if stmt.span.start <= cursor_offset && cursor_offset <= stmt.span.end {
                    if let ast::PatKind::Var(var_name) = &pat.node {
                        let value_text = &doc.source
                            [value_expr.span.start..value_expr.span.end.min(doc.source.len())];

                        // Count usages of this variable in subsequent statements
                        let use_count = doc
                            .references
                            .iter()
                            .filter(|(usage, def)| {
                                *def == pat.span
                                    && usage.start > stmt.span.end
                                    && usage.start < expr.span.end
                            })
                            .count();

                        if use_count > 0 {
                            // Build edits: remove the let line, replace all usages with the value
                            let mut edits = Vec::new();

                            // Remove the let statement (including the newline)
                            let let_line_start = doc.source[..stmt.span.start]
                                .rfind('\n')
                                .map(|p| p + 1)
                                .unwrap_or(stmt.span.start);
                            let let_line_end = doc.source[stmt.span.end..]
                                .find('\n')
                                .map(|p| stmt.span.end + p + 1)
                                .unwrap_or(stmt.span.end);

                            edits.push(TextEdit {
                                range: Range {
                                    start: offset_to_position(&doc.source, let_line_start),
                                    end: offset_to_position(&doc.source, let_line_end),
                                },
                                new_text: String::new(),
                            });

                            // Replace each usage with the value (parenthesized if complex)
                            let replacement = if value_text.contains(' ') && use_count > 0 {
                                format!("({value_text})")
                            } else {
                                value_text.to_string()
                            };

                            for (usage_span, def_span) in &doc.references {
                                if *def_span == pat.span
                                    && usage_span.start > stmt.span.end
                                    && usage_span.start < expr.span.end
                                {
                                    edits.push(TextEdit {
                                        range: span_to_range(*usage_span, &doc.source),
                                        new_text: replacement.clone(),
                                    });
                                }
                            }

                            let mut changes = HashMap::new();
                            changes.insert(uri.clone(), edits);

                            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                                title: format!("Inline `{var_name}`"),
                                kind: Some(CodeActionKind::REFACTOR_INLINE),
                                edit: Some(WorkspaceEdit {
                                    changes: Some(changes),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }));
                        }
                    }
                }
            }
        }

        // Recurse into statements
        for stmt in stmts {
            match &stmt.node {
                ast::StmtKind::Bind { expr: e, .. }
                | ast::StmtKind::Let { expr: e, .. }
                | ast::StmtKind::Expr(e)
                | ast::StmtKind::Where { cond: e } => {
                    find_inline_actions(e, doc, uri, cursor_offset, actions);
                }
                ast::StmtKind::GroupBy { key } => {
                    find_inline_actions(key, doc, uri, cursor_offset, actions);
                }
            }
        }
    }

    // Recurse into other expression types
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            find_inline_actions(func, doc, uri, cursor_offset, actions);
            find_inline_actions(arg, doc, uri, cursor_offset, actions);
        }
        ast::ExprKind::Lambda { body, .. } => {
            find_inline_actions(body, doc, uri, cursor_offset, actions);
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            find_inline_actions(cond, doc, uri, cursor_offset, actions);
            find_inline_actions(then_branch, doc, uri, cursor_offset, actions);
            find_inline_actions(else_branch, doc, uri, cursor_offset, actions);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            find_inline_actions(scrutinee, doc, uri, cursor_offset, actions);
            for arm in arms {
                find_inline_actions(&arm.body, doc, uri, cursor_offset, actions);
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => {
            find_inline_actions(e, doc, uri, cursor_offset, actions);
        }
        _ => {}
    }
}

/// Convert a pattern AST node to a source string representation.
fn pat_to_string(pat: &ast::Pat, source: &str) -> String {
    source[pat.span.start..pat.span.end.min(source.len())].to_string()
}

// ── Call Hierarchy ───────────────────────────────────────────────────

fn handle_call_hierarchy_prepare(
    state: &ServerState,
    params: &CallHierarchyPrepareParams,
) -> Option<Vec<CallHierarchyItem>> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);
    let word = word_at_position(&doc.source, pos)?;

    // Find the declaration containing this name
    for decl in &doc.module.decls {
        let name = match &decl.node {
            DeclKind::Fun { name, .. } => name,
            DeclKind::Source { name, .. }
            | DeclKind::View { name, .. }
            | DeclKind::Derived { name, .. } => name,
            DeclKind::Data { name, .. } | DeclKind::Trait { name, .. } => name,
            _ => continue,
        };
        if name != word {
            continue;
        }
        // Check if cursor is on or references this declaration
        let on_def = decl.span.start <= offset && offset < decl.span.end;
        let on_ref = doc.references.iter().any(|(usage, def)| {
            usage.start <= offset && offset < usage.end && *def == decl.span
        });
        if !on_def && !on_ref {
            continue;
        }

        let range = span_to_range(decl.span, &doc.source);
        let selection_range = find_word_in_source(&doc.source, name, decl.span.start, decl.span.end)
            .map(|s| span_to_range(s, &doc.source))
            .unwrap_or(range);

        let kind = match &decl.node {
            DeclKind::Fun { .. } => SymbolKind::FUNCTION,
            DeclKind::Data { .. } => SymbolKind::STRUCT,
            DeclKind::Trait { .. } => SymbolKind::INTERFACE,
            _ => SymbolKind::VARIABLE,
        };

        return Some(vec![CallHierarchyItem {
            name: name.clone(),
            kind,
            tags: None,
            detail: doc.type_info.get(name).cloned(),
            uri: uri.clone(),
            range,
            selection_range,
            data: None,
        }]);
    }

    None
}

fn handle_call_hierarchy_incoming(
    state: &ServerState,
    params: &CallHierarchyIncomingCallsParams,
) -> Option<Vec<CallHierarchyIncomingCall>> {
    let target_name = &params.item.name;
    let target_uri = &params.item.uri;
    let doc = state.documents.get(target_uri)?;

    // Find all declarations that reference the target name
    let target_def = doc.definitions.get(target_name)?;
    let mut calls: HashMap<String, (ast::Span, Vec<Span>)> = HashMap::new(); // caller_name -> (decl_span, [call_site_spans])

    for decl in &doc.module.decls {
        let caller_name = match &decl.node {
            DeclKind::Fun { name, .. } => name.clone(),
            DeclKind::View { name, .. } => name.clone(),
            DeclKind::Derived { name, .. } => name.clone(),
            _ => continue,
        };
        // Collect call sites within this declaration that point to target_def
        let call_sites: Vec<Span> = doc
            .references
            .iter()
            .filter(|(usage, def)| {
                *def == *target_def
                    && usage.start >= decl.span.start
                    && usage.end <= decl.span.end
            })
            .map(|(usage, _)| *usage)
            .collect();

        if !call_sites.is_empty() {
            calls.insert(caller_name, (decl.span, call_sites));
        }
    }

    let mut result = Vec::new();
    for (name, (decl_span, sites)) in &calls {
        let range = span_to_range(*decl_span, &doc.source);
        let selection_range = find_word_in_source(&doc.source, name, decl_span.start, decl_span.end)
            .map(|s| span_to_range(s, &doc.source))
            .unwrap_or(range);

        let kind = doc
            .module
            .decls
            .iter()
            .find(|d| d.span == *decl_span)
            .map(|d| match &d.node {
                DeclKind::Fun { .. } => SymbolKind::FUNCTION,
                DeclKind::Data { .. } => SymbolKind::STRUCT,
                _ => SymbolKind::VARIABLE,
            })
            .unwrap_or(SymbolKind::FUNCTION);

        result.push(CallHierarchyIncomingCall {
            from: CallHierarchyItem {
                name: name.clone(),
                kind,
                tags: None,
                detail: doc.type_info.get(name).cloned(),
                uri: target_uri.clone(),
                range,
                selection_range,
                data: None,
            },
            from_ranges: sites.iter().map(|s| span_to_range(*s, &doc.source)).collect(),
        });
    }

    if result.is_empty() { None } else { Some(result) }
}

fn handle_call_hierarchy_outgoing(
    state: &ServerState,
    params: &CallHierarchyOutgoingCallsParams,
) -> Option<Vec<CallHierarchyOutgoingCall>> {
    let source_name = &params.item.name;
    let source_uri = &params.item.uri;
    let doc = state.documents.get(source_uri)?;

    // Find the declaration for the source item
    let source_decl = doc
        .module
        .decls
        .iter()
        .find(|d| match &d.node {
            DeclKind::Fun { name, .. }
            | DeclKind::View { name, .. }
            | DeclKind::Derived { name, .. } => name == source_name,
            _ => false,
        })?;

    // Collect all references within this declaration that point to other declarations
    let mut outgoing: HashMap<String, (Span, Vec<Span>)> = HashMap::new(); // callee_name -> (def_span, [call_site_spans])

    for (usage_span, def_span) in &doc.references {
        if usage_span.start < source_decl.span.start || usage_span.end > source_decl.span.end {
            continue;
        }
        // Find the callee name from the definition
        if let Some((name, _)) = doc.definitions.iter().find(|(_, s)| *s == def_span) {
            if name == source_name {
                continue; // Skip self-references
            }
            outgoing
                .entry(name.clone())
                .or_insert_with(|| (*def_span, Vec::new()))
                .1
                .push(*usage_span);
        }
    }

    let mut result = Vec::new();
    for (name, (def_span, sites)) in &outgoing {
        let range = span_to_range(*def_span, &doc.source);
        let selection_range = find_word_in_source(&doc.source, name, def_span.start, def_span.end)
            .map(|s| span_to_range(s, &doc.source))
            .unwrap_or(range);

        let kind = doc
            .module
            .decls
            .iter()
            .find(|d| d.span == *def_span)
            .map(|d| match &d.node {
                DeclKind::Fun { .. } => SymbolKind::FUNCTION,
                DeclKind::Data { .. } => SymbolKind::STRUCT,
                DeclKind::Trait { .. } => SymbolKind::INTERFACE,
                _ => SymbolKind::VARIABLE,
            })
            .unwrap_or(SymbolKind::FUNCTION);

        result.push(CallHierarchyOutgoingCall {
            to: CallHierarchyItem {
                name: name.clone(),
                kind,
                tags: None,
                detail: doc.type_info.get(name).cloned(),
                uri: source_uri.clone(),
                range,
                selection_range,
                data: None,
            },
            from_ranges: sites.iter().map(|s| span_to_range(*s, &doc.source)).collect(),
        });
    }

    if result.is_empty() { None } else { Some(result) }
}

// ── Workspace Symbols ───────────────────────────────────────────────

#[allow(deprecated)]
fn handle_workspace_symbol(
    state: &ServerState,
    params: &WorkspaceSymbolParams,
) -> Option<Vec<SymbolInformation>> {
    let query = params.query.to_lowercase();
    let mut symbols = Vec::new();
    let mut seen_paths: HashSet<PathBuf> = HashSet::new();

    // Helper closure to extract symbols from a module
    let collect_symbols =
        |module: &Module, source: &str, uri: &Uri, query: &str, symbols: &mut Vec<SymbolInformation>| {
            for decl in &module.decls {
                let (name, kind) = match &decl.node {
                    DeclKind::Data { name, .. } => (name.clone(), SymbolKind::STRUCT),
                    DeclKind::TypeAlias { name, .. } => (name.clone(), SymbolKind::TYPE_PARAMETER),
                    DeclKind::Source { name, .. } => (format!("*{name}"), SymbolKind::VARIABLE),
                    DeclKind::View { name, .. } => (format!("*{name}"), SymbolKind::VARIABLE),
                    DeclKind::Derived { name, .. } => (format!("&{name}"), SymbolKind::VARIABLE),
                    DeclKind::Fun { name, .. } => (name.clone(), SymbolKind::FUNCTION),
                    DeclKind::Trait { name, .. } => (name.clone(), SymbolKind::INTERFACE),
                    DeclKind::Impl {
                        trait_name, args, ..
                    } => {
                        let args_str = args
                            .iter()
                            .map(|a| format_type_kind(&a.node))
                            .collect::<Vec<_>>()
                            .join(" ");
                        (
                            format!("impl {trait_name} {args_str}"),
                            SymbolKind::OBJECT,
                        )
                    }
                    DeclKind::Route { name, .. } | DeclKind::RouteComposite { name, .. } => {
                        (format!("route {name}"), SymbolKind::MODULE)
                    }
                    _ => continue,
                };

                if !query.is_empty() && !name.to_lowercase().contains(query) {
                    continue;
                }

                let range = span_to_range(decl.span, source);
                symbols.push(SymbolInformation {
                    name,
                    kind,
                    tags: None,
                    deprecated: None,
                    location: Location {
                        uri: uri.clone(),
                        range,
                    },
                    container_name: None,
                });
            }
        };

    // Phase 1: collect from open documents
    for (uri, doc) in &state.documents {
        if let Some(path) = uri_to_path(uri) {
            if let Ok(canonical) = path.canonicalize() {
                seen_paths.insert(canonical);
            }
        }
        collect_symbols(&doc.module, &doc.source, uri, &query, &mut symbols);
    }

    // Phase 2: scan workspace for .knot files not already open
    if let Some(root) = &state.workspace_root {
        if let Ok(entries) = scan_knot_files(root) {
            for path in entries {
                let canonical = match path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if seen_paths.contains(&canonical) {
                    continue;
                }
                let source = match std::fs::read_to_string(&canonical) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let uri = match path_to_uri(&canonical) {
                    Some(u) => u,
                    None => continue,
                };
                let lexer = knot::lexer::Lexer::new(&source);
                let (tokens, _) = lexer.tokenize();
                let parser = knot::parser::Parser::new(source.clone(), tokens);
                let (module, _) = parser.parse_module();
                collect_symbols(&module, &source, &uri, &query, &mut symbols);
            }
        }
    }

    if symbols.is_empty() {
        None
    } else {
        Some(symbols)
    }
}

/// Recursively find all .knot files under a directory.
fn scan_knot_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    scan_knot_files_recursive(dir, &mut files)?;
    Ok(files)
}

fn scan_knot_files_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            // Skip hidden dirs and common non-source dirs
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.starts_with('.') && name != "target" && name != "node_modules" {
                scan_knot_files_recursive(&path, files)?;
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("knot") {
            files.push(path);
        }
    }
    Ok(())
}

// ── Document Highlights ─────────────────────────────────────────────

fn handle_document_highlight(
    state: &ServerState,
    params: &DocumentHighlightParams,
) -> Option<Vec<DocumentHighlight>> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);

    // Find the definition span for the symbol at cursor
    let def_span = doc
        .references
        .iter()
        .find(|(usage, _)| usage.start <= offset && offset < usage.end)
        .map(|(_, def)| *def)
        .or_else(|| {
            doc.definitions
                .values()
                .find(|span| span.start <= offset && offset < span.end)
                .copied()
        })?;

    let mut highlights = Vec::new();

    // Highlight the definition itself
    highlights.push(DocumentHighlight {
        range: span_to_range(def_span, &doc.source),
        kind: Some(DocumentHighlightKind::WRITE),
    });

    // Highlight all usages
    for (usage_span, target_span) in &doc.references {
        if *target_span == def_span {
            highlights.push(DocumentHighlight {
                range: span_to_range(*usage_span, &doc.source),
                kind: Some(DocumentHighlightKind::READ),
            });
        }
    }

    if highlights.is_empty() {
        None
    } else {
        Some(highlights)
    }
}

// ── Document Links ──────────────────────────────────────────────────

fn handle_document_link(
    state: &ServerState,
    params: &DocumentLinkParams,
) -> Option<Vec<DocumentLink>> {
    let uri = &params.text_document.uri;
    let doc = state.documents.get(uri)?;
    let source_path = uri_to_path(uri)?;
    let base_dir = source_path.parent().unwrap_or(Path::new("."));

    let mut links = Vec::new();

    for imp in &doc.module.imports {
        let rel_path = PathBuf::from(&imp.path).with_extension("knot");
        let full_path = base_dir.join(&rel_path);
        let canonical = match full_path.canonicalize() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let target_uri = match path_to_uri(&canonical) {
            Some(u) => u,
            None => continue,
        };

        // The link range covers the import path string within the import span.
        // Find the path string in the source text of this import.
        let import_text = &doc.source[imp.span.start..imp.span.end.min(doc.source.len())];
        if let Some(path_start) = import_text.find(&imp.path) {
            let abs_start = imp.span.start + path_start;
            let abs_end = abs_start + imp.path.len();
            links.push(DocumentLink {
                range: span_to_range(Span::new(abs_start, abs_end), &doc.source),
                target: Some(target_uri),
                tooltip: Some(format!("{}", canonical.display())),
                data: None,
            });
        }
    }

    if links.is_empty() {
        None
    } else {
        Some(links)
    }
}

// ── Range Formatting ────────────────────────────────────────────────

fn handle_range_formatting(
    state: &ServerState,
    params: &DocumentRangeFormattingParams,
) -> Option<Vec<TextEdit>> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let source = &doc.source;
    let tab_size = params.options.tab_size as usize;
    let use_spaces = params.options.insert_spaces;

    let start_line = params.range.start.line as usize;
    let end_line = params.range.end.line as usize;

    let lines: Vec<&str> = source.split('\n').collect();
    let mut edits = Vec::new();

    let mut prev_was_blank = false;
    for i in start_line..=end_line.min(lines.len().saturating_sub(1)) {
        let line = lines[i];

        // Convert tabs to spaces
        if use_spaces && line.contains('\t') {
            let indent_str = " ".repeat(tab_size);
            let new_line = line.replace('\t', &indent_str);
            let trimmed = new_line.trim_end();
            edits.push(TextEdit {
                range: Range {
                    start: Position::new(i as u32, 0),
                    end: Position::new(i as u32, line.len() as u32),
                },
                new_text: trimmed.to_string(),
            });
            prev_was_blank = trimmed.is_empty();
            continue;
        }

        // Collapse consecutive blank lines to at most one
        if line.trim().is_empty() {
            if prev_was_blank {
                edits.push(TextEdit {
                    range: Range {
                        start: Position::new(i as u32, 0),
                        end: Position::new((i + 1).min(lines.len()) as u32, 0),
                    },
                    new_text: String::new(),
                });
                continue;
            }
            prev_was_blank = true;
        } else {
            prev_was_blank = false;
        }

        // Trim trailing whitespace
        let trimmed = line.trim_end();
        if trimmed.len() != line.len() {
            edits.push(TextEdit {
                range: Range {
                    start: Position::new(i as u32, trimmed.len() as u32),
                    end: Position::new(i as u32, line.len() as u32),
                },
                new_text: String::new(),
            });
        }
    }

    if edits.is_empty() {
        None
    } else {
        Some(edits)
    }
}

// ── On-Type Formatting ──────────────────────────────────────────────

fn handle_on_type_formatting(
    state: &ServerState,
    params: &DocumentOnTypeFormattingParams,
) -> Option<Vec<TextEdit>> {
    let doc = state.documents.get(&params.text_document_position.text_document.uri)?;
    let source = &doc.source;
    let pos = params.text_document_position.position;

    // We triggered on '\n' — look at the previous line to decide indentation
    if pos.line == 0 {
        return None;
    }

    let prev_line_idx = (pos.line - 1) as usize;
    let lines: Vec<&str> = source.split('\n').collect();
    if prev_line_idx >= lines.len() {
        return None;
    }

    let prev_line = lines[prev_line_idx];
    let prev_trimmed = prev_line.trim();

    // Measure the previous line's indentation
    let prev_indent = prev_line.len() - prev_line.trim_start().len();

    // Keywords that should increase indent on the next line
    let should_indent = prev_trimmed == "do"
        || prev_trimmed.ends_with(" do")
        || prev_trimmed.ends_with(" of")
        || prev_trimmed == "where"
        || prev_trimmed.ends_with(" where")
        || prev_trimmed.ends_with(" then")
        || prev_trimmed.ends_with(" else")
        || prev_trimmed.ends_with("->")
        || prev_trimmed.ends_with('=')
        || (prev_trimmed.starts_with("impl ") && !prev_trimmed.contains('='));

    if !should_indent {
        return None;
    }

    let new_indent = prev_indent + 2;
    let current_line_idx = pos.line as usize;

    // Only add indent if the current line is empty or has less indentation
    if current_line_idx < lines.len() {
        let current_line = lines[current_line_idx];
        let current_indent = current_line.len() - current_line.trim_start().len();
        if current_indent >= new_indent && !current_line.trim().is_empty() {
            return None;
        }
    }

    let indent_str = " ".repeat(new_indent);
    Some(vec![TextEdit {
        range: Range {
            start: Position::new(pos.line, 0),
            end: Position::new(pos.line, pos.character),
        },
        new_text: indent_str,
    }])
}

// ── Completion Resolve ───────────────────────────────────────────────

fn handle_resolve_completion_item(
    state: &ServerState,
    mut item: CompletionItem,
) -> CompletionItem {
    // Enrich the completion item with documentation and type details
    let label = &item.label;

    // Search all open documents for matching definitions
    for doc in state.documents.values() {
        // Check doc comments
        if let Some(doc_comment) = doc.doc_comments.get(label.as_str()) {
            item.documentation = Some(Documentation::MarkupContent(MarkupContent {
                kind: MarkupKind::Markdown,
                value: doc_comment.clone(),
            }));
        }
        // Enrich detail with type info if not already present
        if item.detail.is_none() {
            if let Some(ty) = doc.type_info.get(label.as_str()) {
                item.detail = Some(ty.clone());
            }
        }
        // Add effect info as part of documentation
        if let Some(effects) = doc.effect_info.get(label.as_str()) {
            let existing = item
                .documentation
                .as_ref()
                .map(|d| match d {
                    Documentation::String(s) => s.clone(),
                    Documentation::MarkupContent(m) => m.value.clone(),
                })
                .unwrap_or_default();
            let combined = if existing.is_empty() {
                format!("*Effects:* `{effects}`")
            } else {
                format!("{existing}\n\n---\n\n*Effects:* `{effects}`")
            };
            item.documentation = Some(Documentation::MarkupContent(MarkupContent {
                kind: MarkupKind::Markdown,
                value: combined,
            }));
        }

        if item.documentation.is_some() || item.detail.is_some() {
            break;
        }
    }

    item
}

// ── Import Path Completion ──────────────────────────────────────────

fn complete_import_path(base_dir: &Path, partial: &str) -> Vec<CompletionItem> {
    let mut items = Vec::new();

    // Resolve the directory from the partial path
    let (search_dir, prefix) = if let Some(last_slash) = partial.rfind('/') {
        let dir_part = &partial[..last_slash];
        let file_part = &partial[last_slash + 1..];
        (base_dir.join(dir_part), file_part)
    } else {
        (base_dir.to_path_buf(), partial)
    };

    let entries = match std::fs::read_dir(&search_dir) {
        Ok(e) => e,
        Err(_) => return items,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        // Skip hidden files/dirs
        if name.starts_with('.') || name == "target" || name == "node_modules" {
            continue;
        }

        if path.is_dir() {
            if name.to_lowercase().starts_with(&prefix.to_lowercase()) {
                items.push(CompletionItem {
                    label: format!("{name}/"),
                    kind: Some(CompletionItemKind::FOLDER),
                    insert_text: Some(format!("{name}/")),
                    command: Some(Command {
                        title: "Trigger completion".into(),
                        command: "editor.action.triggerSuggest".into(),
                        arguments: None,
                    }),
                    ..Default::default()
                });
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("knot") {
            let stem = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
            if stem.to_lowercase().starts_with(&prefix.to_lowercase()) {
                items.push(CompletionItem {
                    label: stem.clone(),
                    kind: Some(CompletionItemKind::MODULE),
                    detail: Some("module".into()),
                    ..Default::default()
                });
            }
        }
    }

    items
}

// ── Linked Editing Range ────────────────────────────────────────────

fn handle_linked_editing_range(
    state: &ServerState,
    params: &LinkedEditingRangeParams,
) -> Option<LinkedEditingRanges> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);
    let word = word_at_position(&doc.source, pos)?;

    // Check if cursor is on a record field name (either in a record expression,
    // pattern, or type declaration) — link all occurrences of the same field
    // within the same declaration scope
    let mut linked_ranges = Vec::new();

    // Find the enclosing declaration
    for decl in &doc.module.decls {
        if decl.span.start > offset || offset > decl.span.end {
            continue;
        }

        // Collect all field name positions within this declaration
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                collect_field_name_spans(body, word, &doc.source, &mut linked_ranges);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        collect_field_name_spans(body, word, &doc.source, &mut linked_ranges);
                    }
                }
            }
            _ => {}
        }
    }

    if linked_ranges.len() <= 1 {
        return None;
    }

    Some(LinkedEditingRanges {
        ranges: linked_ranges,
        word_pattern: None,
    })
}

fn collect_field_name_spans(
    expr: &ast::Expr,
    field_name: &str,
    source: &str,
    ranges: &mut Vec<Range>,
) {
    match &expr.node {
        ast::ExprKind::Record(fields) => {
            for f in fields {
                if f.name == field_name {
                    // Find the field name span within the record expression
                    if let Some(span) =
                        find_word_in_source(source, field_name, expr.span.start, expr.span.end)
                    {
                        ranges.push(span_to_range(span, source));
                    }
                }
                collect_field_name_spans(&f.value, field_name, source, ranges);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            collect_field_name_spans(base, field_name, source, ranges);
            for f in fields {
                if f.name == field_name {
                    if let Some(span) =
                        find_word_in_source(source, field_name, expr.span.start, expr.span.end)
                    {
                        ranges.push(span_to_range(span, source));
                    }
                }
                collect_field_name_spans(&f.value, field_name, source, ranges);
            }
        }
        ast::ExprKind::FieldAccess {
            expr: inner, field, ..
        } => {
            if field == field_name {
                let field_start = expr.span.end - field.len();
                ranges.push(span_to_range(
                    Span::new(field_start, expr.span.end),
                    source,
                ));
            }
            collect_field_name_spans(inner, field_name, source, ranges);
        }
        ast::ExprKind::App { func, arg } => {
            collect_field_name_spans(func, field_name, source, ranges);
            collect_field_name_spans(arg, field_name, source, ranges);
        }
        ast::ExprKind::Lambda { body, .. } => {
            collect_field_name_spans(body, field_name, source, ranges);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            collect_field_name_spans(lhs, field_name, source, ranges);
            collect_field_name_spans(rhs, field_name, source, ranges);
        }
        ast::ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            collect_field_name_spans(cond, field_name, source, ranges);
            collect_field_name_spans(then_branch, field_name, source, ranges);
            collect_field_name_spans(else_branch, field_name, source, ranges);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            collect_field_name_spans(scrutinee, field_name, source, ranges);
            for arm in arms {
                collect_field_name_spans(&arm.body, field_name, source, ranges);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => {
                        collect_field_name_spans(expr, field_name, source, ranges);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        collect_field_name_spans(e, field_name, source, ranges);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        collect_field_name_spans(key, field_name, source, ranges);
                    }
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => collect_field_name_spans(e, field_name, source, ranges),
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
            collect_field_name_spans(target, field_name, source, ranges);
            collect_field_name_spans(value, field_name, source, ranges);
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                collect_field_name_spans(e, field_name, source, ranges);
            }
        }
        _ => {}
    }
}

// ── Workspace Diagnostics (Pull Model) ──────────────────────────────

fn handle_workspace_diagnostics(
    state: &mut ServerState,
    _params: &WorkspaceDiagnosticParams,
) -> WorkspaceDiagnosticReportResult {
    let mut items = Vec::new();

    for (uri, doc) in &state.documents {
        let lsp_diags: Vec<Diagnostic> = doc
            .knot_diagnostics
            .iter()
            .filter_map(|d| to_lsp_diagnostic(d, &doc.source, uri))
            .collect();

        items.push(WorkspaceDocumentDiagnosticReport::Full(
            WorkspaceFullDocumentDiagnosticReport {
                uri: uri.clone(),
                version: None,
                full_document_diagnostic_report: FullDocumentDiagnosticReport {
                    result_id: None,
                    items: lsp_diags,
                },
            },
        ));
    }

    // Also scan workspace files not currently open. We run the full pipeline
    // (lex → parse → type infer → effect infer → stratify → SQL lint) so
    // cross-file errors surface even when a file isn't open in the editor.
    if let Some(root) = &state.workspace_root {
        let open_paths: HashSet<PathBuf> = state
            .documents
            .keys()
            .filter_map(|u| uri_to_path(u))
            .filter_map(|p| p.canonicalize().ok())
            .collect();

        if let Ok(files) = scan_knot_files(root) {
            for file_path in files {
                let canonical = match file_path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if open_paths.contains(&canonical) {
                    continue;
                }
                let source = match std::fs::read_to_string(&canonical) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let file_uri = match path_to_uri(&canonical) {
                    Some(u) => u,
                    None => continue,
                };

                // Try the mtime cache first to avoid redundant analysis runs
                // across requests.
                let mtime = std::fs::metadata(&canonical)
                    .and_then(|m| m.modified())
                    .ok();

                let lsp_diags = match mtime {
                    Some(t) => match state.workspace_diag_cache.get(&canonical) {
                        Some((cached_t, cached)) if *cached_t == t => cached.clone(),
                        _ => {
                            let diags =
                                analyze_unopened_file(&source, &canonical, &file_uri);
                            state
                                .workspace_diag_cache
                                .insert(canonical.clone(), (t, diags.clone()));
                            diags
                        }
                    },
                    None => analyze_unopened_file(&source, &canonical, &file_uri),
                };

                if !lsp_diags.is_empty() {
                    items.push(WorkspaceDocumentDiagnosticReport::Full(
                        WorkspaceFullDocumentDiagnosticReport {
                            uri: file_uri,
                            version: None,
                            full_document_diagnostic_report: FullDocumentDiagnosticReport {
                                result_id: None,
                                items: lsp_diags,
                            },
                        },
                    ));
                }
            }
        }
    }

    WorkspaceDiagnosticReportResult::Report(WorkspaceDiagnosticReport { items })
}

/// Drop cache entries for files that no longer exist or whose mtime has advanced
/// past the cached value. Cheap O(n) over the cache; called after each
/// workspace-diagnostics request.
fn prune_stale_workspace_diag_cache(state: &mut ServerState) {
    state.workspace_diag_cache.retain(|path, (cached_t, _)| {
        match std::fs::metadata(path).and_then(|m| m.modified()) {
            Ok(t) => t == *cached_t,
            Err(_) => false, // file deleted or inaccessible
        }
    });
}

/// Run the full analysis pipeline on an unopened workspace file and return its
/// LSP diagnostics. Identical to `analyze_document` but stripped of all the
/// ancillary metadata (definitions, references, etc.) that this caller doesn't
/// need.
fn analyze_unopened_file(source: &str, path: &Path, uri: &Uri) -> Vec<Diagnostic> {
    let mut all_diags = Vec::new();

    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, lex_diags) = lexer.tokenize();
    all_diags.extend(lex_diags);

    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    all_diags.extend(parse_diags);

    let has_parse_errors = all_diags
        .iter()
        .any(|d| matches!(d.severity, diagnostic::Severity::Error));

    if !has_parse_errors {
        let mut analysis_module = module.clone();

        let _ = knot_compiler::modules::resolve_imports(&mut analysis_module, path);
        knot_compiler::base::inject_prelude(&mut analysis_module);
        knot_compiler::desugar::desugar(&mut analysis_module);

        let (infer_diags, _, _, _, _, _, _) = knot_compiler::infer::check(&analysis_module);
        all_diags.extend(infer_diags);

        let (effect_diags, _) = knot_compiler::effects::check_with_effects(&analysis_module);
        all_diags.extend(effect_diags);

        all_diags.extend(knot_compiler::stratify::check(&analysis_module));

        let type_env = knot_compiler::types::TypeEnv::from_module(&analysis_module);
        all_diags.extend(knot_compiler::sql_lint::check(&analysis_module, &type_env));
    }

    all_diags
        .iter()
        .filter_map(|d| to_lsp_diagnostic(d, source, uri))
        .collect()
}

// ── Constants ───────────────────────────────────────────────────────

const KEYWORDS: &[&str] = &[
    "import", "data", "type", "trait", "impl", "route", "migrate", "where", "do", "yield", "set",
    "if", "then", "else", "case", "of", "let", "in", "not", "full", "atomic", "deriving", "with",
    "export",
];

const SNIPPETS: &[(&str, &str, &str)] = &[
    ("do", "do block", "do\n  ${1:x} <- ${2:expr}\n  yield {$3}"),
    ("case", "case expression", "case ${1:expr} of\n  ${2:pattern} -> ${3:body}"),
    ("lambda", "lambda expression", "\\\\${1:x} -> ${2:body}"),
    ("if", "if expression", "if ${1:cond}\n  then ${2:a}\n  else ${3:b}"),
    ("data", "data declaration", "data ${1:Name} = ${2:Ctor} {${3:field}: ${4:Type}}"),
    ("source", "source declaration", "*${1:name} : [${2:Type}]"),
    ("trait", "trait declaration", "trait ${1:Name} ${2:a} where\n  ${3:method} : ${4:Type}"),
    ("impl", "impl block", "impl ${1:Trait} ${2:Type} where\n  ${3:method} ${4:x} = ${5:body}"),
];

const BUILTINS: &[&str] = &[
    "println", "print", "show", "union", "count", "now", "filter", "match", "map", "fold",
    "single", "diff", "inter", "sum", "avg", "toUpper", "toLower", "take", "drop", "length",
    "trim", "contains", "reverse", "chars", "id", "toJson", "parseJson", "readFile", "writeFile",
    "appendFile", "fileExists", "removeFile", "listDir",
];

// ── Definition resolution ────────────────────────────────────────────

/// Resolve definitions: returns (name_map, span_references, literal_types).
fn resolve_definitions(
    module: &Module,
    source: &str,
) -> (HashMap<String, Span>, Vec<(Span, Span)>, Vec<(Span, String)>) {
    let mut resolver = DefResolver {
        scopes: vec![HashMap::new()],
        refs: Vec::new(),
        literals: Vec::new(),
    };

    // Phase 1: register all top-level declarations
    for decl in &module.decls {
        // Use just the name span (not the whole declaration) so document
        // highlights only cover the identifier, not the entire body.
        let name_span = |name: &str| {
            find_word_in_source(source, name, decl.span.start, decl.span.end)
                .unwrap_or(decl.span)
        };
        match &decl.node {
            DeclKind::Data {
                name, constructors, ..
            } => {
                resolver.define(name, name_span(name));
                for ctor in constructors {
                    resolver.define(&ctor.name, name_span(&ctor.name));
                }
            }
            DeclKind::TypeAlias { name, .. } => {
                resolver.define(name, name_span(name));
            }
            DeclKind::Source { name, .. } | DeclKind::View { name, .. } => {
                resolver.define(name, name_span(name));
            }
            DeclKind::Derived { name, .. } => {
                resolver.define(name, name_span(name));
            }
            DeclKind::Fun { name, .. } => {
                resolver.define(name, name_span(name));
            }
            DeclKind::Trait { name, items, .. } => {
                resolver.define(name, name_span(name));
                for item in items {
                    if let ast::TraitItem::Method { name, .. } = item {
                        resolver.define(name, name_span(name));
                    }
                }
            }
            DeclKind::Route { name, .. } | DeclKind::RouteComposite { name, .. } => {
                resolver.define(name, name_span(name));
            }
            _ => {}
        }
    }

    // Phase 2: walk declaration bodies to resolve references
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                resolver.resolve_expr(body);
            }
            DeclKind::Fun { body: None, .. } => {}
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { params, body, .. } = item {
                        resolver.push_scope();
                        for p in params {
                            resolver.define_pat(p);
                        }
                        resolver.resolve_expr(body);
                        resolver.pop_scope();
                    }
                }
            }
            DeclKind::Trait { items, .. } => {
                for item in items {
                    if let ast::TraitItem::Method {
                        default_params,
                        default_body: Some(body),
                        ..
                    } = item
                    {
                        resolver.push_scope();
                        for p in default_params {
                            resolver.define_pat(p);
                        }
                        resolver.resolve_expr(body);
                        resolver.pop_scope();
                    }
                }
            }
            DeclKind::Migrate { using_fn, .. } => {
                resolver.resolve_expr(using_fn);
            }
            _ => {}
        }
    }

    // Build the fallback name map from global scope
    let name_map = resolver.scopes[0].clone();
    (name_map, resolver.refs, resolver.literals)
}

struct DefResolver {
    scopes: Vec<HashMap<String, Span>>,
    refs: Vec<(Span, Span)>,
    literals: Vec<(Span, String)>,
}

impl DefResolver {
    fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    fn pop_scope(&mut self) {
        self.scopes.pop();
    }

    fn define(&mut self, name: &str, span: Span) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name.to_string(), span);
        }
    }

    fn lookup(&self, name: &str) -> Option<Span> {
        for scope in self.scopes.iter().rev() {
            if let Some(span) = scope.get(name) {
                return Some(*span);
            }
        }
        None
    }

    fn add_ref(&mut self, usage: Span, name: &str) {
        if let Some(def) = self.lookup(name) {
            self.refs.push((usage, def));
        }
    }

    fn define_pat(&mut self, pat: &ast::Pat) {
        match &pat.node {
            ast::PatKind::Var(name) => self.define(name, pat.span),
            ast::PatKind::Constructor { name, payload } => {
                // The constructor name is a reference
                self.add_ref(pat.span, name);
                self.define_pat(payload);
            }
            ast::PatKind::Record(fields) => {
                for f in fields {
                    if let Some(p) = &f.pattern {
                        self.define_pat(p);
                    } else {
                        // Punned: `{name}` introduces `name`
                        self.define(&f.name, pat.span);
                    }
                }
            }
            ast::PatKind::List(pats) => {
                for p in pats {
                    self.define_pat(p);
                }
            }
            ast::PatKind::Wildcard | ast::PatKind::Lit(_) => {}
        }
    }

    fn resolve_expr(&mut self, expr: &ast::Expr) {
        match &expr.node {
            ast::ExprKind::Var(name) => self.add_ref(expr.span, name),
            ast::ExprKind::Constructor(name) => self.add_ref(expr.span, name),
            ast::ExprKind::SourceRef(name) => self.add_ref(expr.span, name),
            ast::ExprKind::DerivedRef(name) => self.add_ref(expr.span, name),

            ast::ExprKind::Lambda { params, body } => {
                self.push_scope();
                for p in params {
                    self.define_pat(p);
                }
                self.resolve_expr(body);
                self.pop_scope();
            }

            ast::ExprKind::Do(stmts) => {
                self.push_scope();
                for stmt in stmts {
                    match &stmt.node {
                        ast::StmtKind::Bind { pat, expr } => {
                            self.resolve_expr(expr);
                            self.define_pat(pat);
                        }
                        ast::StmtKind::Let { pat, expr } => {
                            self.resolve_expr(expr);
                            self.define_pat(pat);
                        }
                        ast::StmtKind::Where { cond } => self.resolve_expr(cond),
                        ast::StmtKind::GroupBy { key } => self.resolve_expr(key),
                        ast::StmtKind::Expr(e) => self.resolve_expr(e),
                    }
                }
                self.pop_scope();
            }

            ast::ExprKind::Case { scrutinee, arms } => {
                self.resolve_expr(scrutinee);
                for arm in arms {
                    self.push_scope();
                    self.define_pat(&arm.pat);
                    self.resolve_expr(&arm.body);
                    self.pop_scope();
                }
            }

            ast::ExprKind::App { func, arg } => {
                self.resolve_expr(func);
                self.resolve_expr(arg);
            }
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                self.resolve_expr(lhs);
                self.resolve_expr(rhs);
            }
            ast::ExprKind::UnaryOp { operand, .. } => self.resolve_expr(operand),
            ast::ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } => {
                self.resolve_expr(cond);
                self.resolve_expr(then_branch);
                self.resolve_expr(else_branch);
            }
            ast::ExprKind::Atomic(e) => self.resolve_expr(e),
            ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
                self.resolve_expr(target);
                self.resolve_expr(value);
            }
            ast::ExprKind::At { relation, time } => {
                self.resolve_expr(relation);
                self.resolve_expr(time);
            }
            ast::ExprKind::Record(fields) => {
                for f in fields {
                    self.resolve_expr(&f.value);
                }
            }
            ast::ExprKind::RecordUpdate { base, fields } => {
                self.resolve_expr(base);
                for f in fields {
                    self.resolve_expr(&f.value);
                }
            }
            ast::ExprKind::FieldAccess { expr, .. } => self.resolve_expr(expr),
            ast::ExprKind::List(elems) => {
                for e in elems {
                    self.resolve_expr(e);
                }
            }
            ast::ExprKind::Lit(lit) => {
                let ty = match lit {
                    ast::Literal::Int(_) => "Int",
                    ast::Literal::Float(_) => "Float",
                    ast::Literal::Text(_) => "Text",
                    ast::Literal::Bool(_) => "Bool",
                    ast::Literal::Bytes(_) => "Bytes",
                };
                self.literals.push((expr.span, ty.to_string()));
            }
            ast::ExprKind::UnitLit { value, .. } => self.resolve_expr(value),
            ast::ExprKind::Annot { expr: inner, .. } => self.resolve_expr(inner),
            ast::ExprKind::Refine(inner) => self.resolve_expr(inner),
        }
    }
}

fn build_details(module: &Module) -> HashMap<String, String> {
    let mut details = HashMap::new();

    for decl in &module.decls {
        match &decl.node {
            DeclKind::Data {
                name,
                params,
                constructors,
                ..
            } => {
                let params_str = if params.is_empty() {
                    String::new()
                } else {
                    format!(" {}", params.join(" "))
                };
                let ctors: Vec<String> = constructors
                    .iter()
                    .map(|c| {
                        if c.fields.is_empty() {
                            c.name.clone()
                        } else {
                            let fields: Vec<String> = c
                                .fields
                                .iter()
                                .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
                                .collect();
                            format!("{} {{{}}}", c.name, fields.join(", "))
                        }
                    })
                    .collect();
                let detail = format!("data {name}{params_str} = {}", ctors.join(" | "));
                details.insert(name.clone(), detail.clone());
                // Also add constructors
                for ctor in constructors {
                    let fields: Vec<String> = ctor
                        .fields
                        .iter()
                        .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
                        .collect();
                    let ctor_detail = if fields.is_empty() {
                        format!("{} — constructor of {name}", ctor.name)
                    } else {
                        format!("{} {{{}}} — constructor of {name}", ctor.name, fields.join(", "))
                    };
                    details.insert(ctor.name.clone(), ctor_detail);
                }
            }
            DeclKind::TypeAlias { name, params, ty } => {
                let params_str = if params.is_empty() {
                    String::new()
                } else {
                    format!(" {}", params.join(" "))
                };
                details.insert(
                    name.clone(),
                    format!("type {name}{params_str} = {}", format_type_kind(&ty.node)),
                );
            }
            DeclKind::Source { name, ty, history } => {
                let hist = if *history { " with history" } else { "" };
                details.insert(
                    name.clone(),
                    format!("*{name} : [{}]{hist}", format_type_kind(&ty.node)),
                );
            }
            DeclKind::View { name, ty, .. } => {
                let ty_str = ty
                    .as_ref()
                    .map(|t| format!(" : {}", format_type_scheme(t)))
                    .unwrap_or_default();
                details.insert(name.clone(), format!("*{name}{ty_str} (view)"));
            }
            DeclKind::Derived { name, ty, .. } => {
                let ty_str = ty
                    .as_ref()
                    .map(|t| format!(" : {}", format_type_scheme(t)))
                    .unwrap_or_default();
                details.insert(name.clone(), format!("&{name}{ty_str} (derived)"));
            }
            DeclKind::Fun { name, ty, .. } => {
                let ty_str = ty
                    .as_ref()
                    .map(|t| format!(" : {}", format_type_scheme(t)))
                    .unwrap_or_default();
                details.insert(name.clone(), format!("{name}{ty_str}"));
            }
            DeclKind::Trait { name, params, .. } => {
                let params_str = params
                    .iter()
                    .map(|p| {
                        if let Some(kind) = &p.kind {
                            format!("({} : {})", p.name, format_type_kind(&kind.node))
                        } else {
                            p.name.clone()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                details.insert(name.clone(), format!("trait {name} {params_str}"));
            }
            _ => {}
        }
    }

    details
}

// ── Type formatting ─────────────────────────────────────────────────

fn format_type_scheme(ts: &TypeScheme) -> String {
    let mut s = String::new();
    for c in &ts.constraints {
        let args: Vec<String> = c.args.iter().map(|a| format_type_kind(&a.node)).collect();
        s.push_str(&format!("{} {} => ", c.trait_name, args.join(" ")));
    }
    s.push_str(&format_type_kind(&ts.ty.node));
    s
}

fn format_type_kind(ty: &TypeKind) -> String {
    match ty {
        TypeKind::Named(n) => n.clone(),
        TypeKind::Var(n) => n.clone(),
        TypeKind::App { func, arg } => {
            format!("{} {}", format_type_kind(&func.node), format_type_kind(&arg.node))
        }
        TypeKind::Record { fields, rest } => {
            let fs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
                .collect();
            match rest {
                Some(r) => format!("{{{} | {r}}}", fs.join(", ")),
                None => format!("{{{}}}", fs.join(", ")),
            }
        }
        TypeKind::Relation(inner) => format!("[{}]", format_type_kind(&inner.node)),
        TypeKind::Function { param, result } => {
            let p = format_type_kind(&param.node);
            let r = format_type_kind(&result.node);
            if matches!(param.node, TypeKind::Function { .. }) {
                format!("({p}) -> {r}")
            } else {
                format!("{p} -> {r}")
            }
        }
        TypeKind::Variant { constructors, rest } => {
            let cs: Vec<String> = constructors
                .iter()
                .map(|c| {
                    if c.fields.is_empty() {
                        c.name.clone()
                    } else {
                        let fs: Vec<String> = c
                            .fields
                            .iter()
                            .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
                            .collect();
                        format!("{} {{{}}}", c.name, fs.join(", "))
                    }
                })
                .collect();
            match rest {
                Some(r) => format!("<{} | {r}>", cs.join(" | ")),
                None => format!("<{}>", cs.join(" | ")),
            }
        }
        TypeKind::Effectful { effects, ty } => {
            let effs: Vec<String> = effects.iter().map(format_effect).collect();
            format!("{{{}}} {}", effs.join(", "), format_type_kind(&ty.node))
        }
        TypeKind::IO { effects, rest, ty } => {
            if effects.is_empty() && rest.is_none() {
                format!("IO {}", format_type_kind(&ty.node))
            } else {
                let mut parts: Vec<String> =
                    effects.iter().map(format_effect).collect();
                if let Some(name) = rest {
                    parts.push(format!("| {}", name));
                }
                format!("IO {{{}}} {}", parts.join(", "), format_type_kind(&ty.node))
            }
        }
        TypeKind::Hole => "_".into(),
        TypeKind::UnitAnnotated { base, unit } => {
            format!("{}<{}>", format_type_kind(&base.node), format_unit_expr(unit))
        }
        TypeKind::Refined { base, predicate } => {
            format!("{} where {}", format_type_kind(&base.node), format_expr_brief(&predicate.node))
        }
        TypeKind::Forall { vars, ty } => {
            format!("forall {}. {}", vars.join(" "), format_type_kind(&ty.node))
        }
    }
}

/// Brief structural rendering of an expression for display in type hovers.
fn format_expr_brief(expr: &ast::ExprKind) -> String {
    match expr {
        ast::ExprKind::Var(name) => name.clone(),
        ast::ExprKind::Lit(ast::Literal::Int(n)) => n.to_string(),
        ast::ExprKind::Lit(ast::Literal::Float(f)) => f.to_string(),
        ast::ExprKind::Lit(ast::Literal::Text(s)) => format!("\"{}\"", s),
        ast::ExprKind::Lit(ast::Literal::Bool(b)) => if *b { "true" } else { "false" }.into(),
        ast::ExprKind::Lambda { params, body } => {
            let ps: Vec<String> = params.iter().map(|p| format_pat_brief(&p.node)).collect();
            format!("\\{} -> {}", ps.join(" "), format_expr_brief(&body.node))
        }
        ast::ExprKind::App { func, arg } => {
            let f = format_expr_brief(&func.node);
            let a = format_expr_brief(&arg.node);
            if matches!(arg.node, ast::ExprKind::App { .. } | ast::ExprKind::BinOp { .. }) {
                format!("{f} ({a})")
            } else {
                format!("{f} {a}")
            }
        }
        ast::ExprKind::BinOp { op, lhs, rhs } => {
            let op_str = match op {
                ast::BinOp::Add => "+", ast::BinOp::Sub => "-",
                ast::BinOp::Mul => "*", ast::BinOp::Div => "/",
                ast::BinOp::Eq => "==", ast::BinOp::Neq => "!=",
                ast::BinOp::Lt => "<", ast::BinOp::Gt => ">",
                ast::BinOp::Le => "<=", ast::BinOp::Ge => ">=",
                ast::BinOp::And => "&&", ast::BinOp::Or => "||",
                ast::BinOp::Concat => "++", ast::BinOp::Pipe => "|>",
            };
            format!("{} {} {}", format_expr_brief(&lhs.node), op_str, format_expr_brief(&rhs.node))
        }
        ast::ExprKind::UnaryOp { op, operand } => {
            let op_str = match op {
                ast::UnaryOp::Neg => "-",
                ast::UnaryOp::Not => "not ",
            };
            format!("{}{}", op_str, format_expr_brief(&operand.node))
        }
        ast::ExprKind::FieldAccess { expr, field } => {
            format!("{}.{}", format_expr_brief(&expr.node), field)
        }
        _ => "...".into(),
    }
}

fn format_pat_brief(pat: &ast::PatKind) -> String {
    match pat {
        ast::PatKind::Var(name) => name.clone(),
        ast::PatKind::Wildcard => "_".into(),
        _ => "...".into(),
    }
}

fn format_unit_expr(u: &ast::UnitExpr) -> String {
    match u {
        ast::UnitExpr::Dimensionless => "1".into(),
        ast::UnitExpr::Named(n) => n.clone(),
        ast::UnitExpr::Mul(a, b) => format!("{}*{}", format_unit_expr(a), format_unit_expr(b)),
        ast::UnitExpr::Div(a, b) => format!("{}/{}", format_unit_expr(a), format_unit_expr(b)),
        ast::UnitExpr::Pow(base, exp) => format!("{}^{}", format_unit_expr(base), exp),
    }
}

fn format_effect(eff: &ast::Effect) -> String {
    match eff {
        ast::Effect::Reads(r) => format!("reads *{r}"),
        ast::Effect::Writes(r) => format!("writes *{r}"),
        ast::Effect::Console => "console".into(),
        ast::Effect::Network => "network".into(),
        ast::Effect::Fs => "fs".into(),
        ast::Effect::Clock => "clock".into(),
        ast::Effect::Random => "random".into(),
    }
}

// ── Diagnostic conversion ───────────────────────────────────────────

fn to_lsp_diagnostic(diag: &diagnostic::Diagnostic, source: &str, uri: &Uri) -> Option<Diagnostic> {
    let severity = match diag.severity {
        diagnostic::Severity::Error => DiagnosticSeverity::ERROR,
        diagnostic::Severity::Warning => DiagnosticSeverity::WARNING,
        diagnostic::Severity::Info => DiagnosticSeverity::INFORMATION,
    };

    // Use the first valid label's span for the diagnostic range,
    // or fall back to the start of the file.
    let range = diag
        .labels
        .iter()
        .find(|l| l.span.start < source.len() && l.span.end <= source.len())
        .map(|l| span_to_range(l.span, source))
        .unwrap_or(Range {
            start: Position::new(0, 0),
            end: Position::new(0, 0),
        });

    // Build message with label messages and notes
    let mut message = diag.message.clone();
    for label in &diag.labels {
        if !label.message.is_empty() && label.message != diag.message {
            message.push_str(&format!("\n  {}", label.message));
        }
    }
    for note in &diag.notes {
        message.push_str(&format!("\nnote: {note}"));
    }

    // Build related information from additional labels (with real URI)
    let related: Vec<DiagnosticRelatedInformation> = diag
        .labels
        .iter()
        .skip(1)
        .filter(|l| l.span.start < source.len() && l.span.end <= source.len())
        .map(|l| DiagnosticRelatedInformation {
            location: Location {
                uri: uri.clone(),
                range: span_to_range(l.span, source),
            },
            message: l.message.clone(),
        })
        .collect();

    // Assign structured error codes based on diagnostic message patterns
    let code = error_code_for_diagnostic(&diag.message);

    // Assign diagnostic tags: Unnecessary for unused warnings, Deprecated for deprecation
    let msg_lower = diag.message.to_lowercase();
    let mut tags = Vec::new();
    if msg_lower.contains("unused") || msg_lower.contains("never used") {
        tags.push(DiagnosticTag::UNNECESSARY);
    }
    if msg_lower.contains("deprecated") {
        tags.push(DiagnosticTag::DEPRECATED);
    }

    Some(Diagnostic {
        range,
        severity: Some(severity),
        code: code.map(NumberOrString::String),
        source: Some("knot".into()),
        message,
        related_information: if related.is_empty() {
            None
        } else {
            Some(related)
        },
        tags: if tags.is_empty() { None } else { Some(tags) },
        ..Default::default()
    })
}

// ── Position utilities ──────────────────────────────────────────────

fn span_to_range(span: Span, source: &str) -> Range {
    Range {
        start: offset_to_position(source, span.start),
        end: offset_to_position(source, span.end),
    }
}

fn offset_to_position(source: &str, offset: usize) -> Position {
    let (line, col) = diagnostic::line_col(source, offset);
    // line_col returns 1-based line, 0-based col; LSP uses 0-based for both
    Position::new((line - 1) as u32, col as u32)
}

fn position_to_offset(source: &str, pos: Position) -> usize {
    let mut offset = 0;
    for (i, line) in source.split('\n').enumerate() {
        if i == pos.line as usize {
            return offset + (pos.character as usize).min(line.len());
        }
        offset += line.len() + 1;
    }
    source.len()
}

fn word_at_position<'a>(source: &'a str, pos: Position) -> Option<&'a str> {
    let offset = position_to_offset(source, pos);
    let bytes = source.as_bytes();
    if offset >= bytes.len() {
        return None;
    }

    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    if !is_ident(bytes[offset]) {
        return None;
    }

    let start = (0..offset)
        .rev()
        .find(|&i| !is_ident(bytes[i]))
        .map(|i| i + 1)
        .unwrap_or(0);

    let end = (offset..bytes.len())
        .find(|&i| !is_ident(bytes[i]))
        .unwrap_or(bytes.len());

    Some(&source[start..end])
}

fn uri_to_path(uri: &Uri) -> Option<PathBuf> {
    let s = uri.as_str();
    s.strip_prefix("file://").map(PathBuf::from)
}

fn path_to_uri(path: &Path) -> Option<Uri> {
    let s = format!("file://{}", path.display());
    s.parse::<Uri>().ok()
}

/// Compute Levenshtein edit distance between two strings.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut dp = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for i in 0..=a.len() {
        dp[i][0] = i;
    }
    for j in 0..=b.len() {
        dp[0][j] = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[a.len()][b.len()]
}

/// Find a whole-word occurrence of `name` in `source[start..end]`.
fn find_word_in_source(source: &str, name: &str, start: usize, end: usize) -> Option<Span> {
    let end = end.min(source.len());
    let text = source.get(start..end)?;
    let bytes = source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    let mut search_start = 0;
    while let Some(pos) = text[search_start..].find(name) {
        let abs_pos = start + search_start + pos;
        let abs_end = abs_pos + name.len();

        // Check word boundaries
        let left_ok = abs_pos == 0 || !is_ident(bytes[abs_pos - 1]);
        let right_ok = abs_end >= bytes.len() || !is_ident(bytes[abs_end]);

        if left_ok && right_ok {
            return Some(Span::new(abs_pos, abs_end));
        }
        search_start += pos + 1;
    }
    None
}

// ── Doc comment extraction ──────────────────────────────────────────

/// Extract doc comments (lines starting with `-- `) above each declaration.
fn extract_doc_comments(source: &str, module: &Module) -> HashMap<String, String> {
    let mut comments = HashMap::new();
    let lines: Vec<&str> = source.split('\n').collect();

    for decl in &module.decls {
        let name = match &decl.node {
            DeclKind::Fun { name, .. }
            | DeclKind::Data { name, .. }
            | DeclKind::TypeAlias { name, .. }
            | DeclKind::Source { name, .. }
            | DeclKind::View { name, .. }
            | DeclKind::Derived { name, .. }
            | DeclKind::Trait { name, .. }
            | DeclKind::Route { name, .. }
            | DeclKind::RouteComposite { name, .. } => name.clone(),
            _ => continue,
        };

        let decl_line = offset_to_position(source, decl.span.start).line as usize;
        if decl_line == 0 {
            continue;
        }

        // Collect consecutive comment lines above the declaration
        let mut comment_lines = Vec::new();
        let mut line_idx = decl_line;
        while line_idx > 0 {
            line_idx -= 1;
            let line = lines.get(line_idx).map(|l| l.trim()).unwrap_or("");
            if let Some(text) = line.strip_prefix("-- ") {
                comment_lines.push(text.to_string());
            } else if line == "--" {
                comment_lines.push(String::new());
            } else {
                break;
            }
        }

        if !comment_lines.is_empty() {
            comment_lines.reverse();
            comments.insert(name, comment_lines.join("\n"));
        }
    }

    comments
}

// ── Keyword/operator semantic token collection ──────────────────────

/// Collect keyword and operator token positions from the lexer token stream.
fn collect_keyword_operator_positions(tokens: &[knot::lexer::Token]) -> Vec<(Span, u32)> {
    use knot::lexer::TokenKind;
    let mut positions = Vec::new();
    for token in tokens {
        let tok_type = match &token.kind {
            // Keywords
            TokenKind::Import
            | TokenKind::Data
            | TokenKind::Type
            | TokenKind::Trait
            | TokenKind::Impl
            | TokenKind::Route
            | TokenKind::Migrate
            | TokenKind::Where
            | TokenKind::Do
            | TokenKind::Set
            | TokenKind::If
            | TokenKind::Then
            | TokenKind::Else
            | TokenKind::Case
            | TokenKind::Of
            | TokenKind::Let
            | TokenKind::In
            | TokenKind::Not
            | TokenKind::Full
            | TokenKind::Atomic
            | TokenKind::Deriving
            | TokenKind::With
            | TokenKind::Export
            | TokenKind::Unit
            | TokenKind::Refine => Some(TOK_KEYWORD),
            // Operators
            TokenKind::Plus
            | TokenKind::Minus
            | TokenKind::Star
            | TokenKind::Slash
            | TokenKind::EqEq
            | TokenKind::BangEq
            | TokenKind::Lt
            | TokenKind::Gt
            | TokenKind::Le
            | TokenKind::Ge
            | TokenKind::PlusPlus
            | TokenKind::AndAnd
            | TokenKind::OrOr
            | TokenKind::PipeGt
            | TokenKind::Caret
            | TokenKind::Arrow
            | TokenKind::FatArrow
            | TokenKind::LArrow => Some(TOK_OPERATOR),
            _ => None,
        };
        if let Some(tt) = tok_type {
            positions.push((token.span, tt));
        }
    }
    positions
}

// ── Diagnostic error codes ──────────────────────────────────────────

/// Map diagnostic messages to structured error codes.
fn error_code_for_diagnostic(message: &str) -> Option<String> {
    let msg = message.to_lowercase();
    if msg.contains("type mismatch") || msg.contains("cannot unify") {
        Some("E001".into())
    } else if msg.contains("undefined") || msg.contains("unknown") || msg.contains("not found") {
        Some("E002".into())
    } else if msg.contains("missing") && msg.contains("field") {
        Some("E003".into())
    } else if msg.contains("exhaustive") || msg.contains("missing case") {
        Some("E004".into())
    } else if msg.contains("occurs check") || msg.contains("infinite type") {
        Some("E005".into())
    } else if msg.contains("duplicate") {
        Some("E006".into())
    } else if msg.contains("import") {
        Some("E007".into())
    } else if msg.contains("effect") || msg.contains("purity") {
        Some("E008".into())
    } else if msg.contains("stratif") {
        Some("E009".into())
    } else if msg.contains("trait") && (msg.contains("impl") || msg.contains("instance")) {
        Some("E010".into())
    } else if msg.contains("unused") {
        Some("W001".into())
    } else if msg.contains("shadow") {
        Some("W002".into())
    } else if msg.contains("runtime") && msg.contains("sql") {
        Some("I001".into())
    } else {
        None
    }
}
