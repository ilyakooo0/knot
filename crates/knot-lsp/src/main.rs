use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

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
}

struct ServerState {
    documents: HashMap<Uri, DocumentState>,
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
#[allow(dead_code)]
const TOK_KEYWORD: u32 = 8;
const TOK_STRING: u32 = 9;
const TOK_NUMBER: u32 = 10;
#[allow(dead_code)]
const TOK_OPERATOR: u32 = 11;

const MOD_DECLARATION: u32 = 0b01;
const MOD_READONLY: u32 = 0b10;

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
            SemanticTokenModifier::DECLARATION, // bit 0
            SemanticTokenModifier::READONLY,    // bit 1
        ],
    }
}

// ── Entry point ─────────────────────────────────────────────────────

fn main() {
    eprintln!("knot-lsp starting...");

    let (connection, io_threads) = Connection::stdio();

    let server_capabilities = serde_json::to_value(ServerCapabilities {
        text_document_sync: Some(TextDocumentSyncCapability::Kind(
            TextDocumentSyncKind::FULL,
        )),
        document_symbol_provider: Some(OneOf::Left(true)),
        definition_provider: Some(OneOf::Left(true)),
        hover_provider: Some(HoverProviderCapability::Simple(true)),
        completion_provider: Some(CompletionOptions {
            trigger_characters: Some(vec![".".into(), "*".into(), "&".into()]),
            ..Default::default()
        }),
        references_provider: Some(OneOf::Left(true)),
        rename_provider: Some(OneOf::Right(RenameOptions {
            prepare_provider: Some(true),
            work_done_progress_options: Default::default(),
        })),
        inlay_hint_provider: Some(OneOf::Left(true)),
        signature_help_provider: Some(SignatureHelpOptions {
            trigger_characters: Some(vec![" ".into()]),
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
        code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
        workspace_symbol_provider: Some(OneOf::Left(true)),
        ..Default::default()
    })
    .unwrap();

    let _init_params = match connection.initialize(server_capabilities) {
        Ok(params) => params,
        Err(e) => {
            eprintln!("Initialize error: {e}");
            return;
        }
    };

    eprintln!("knot-lsp initialized");

    let mut state = ServerState {
        documents: HashMap::new(),
    };

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
    } else if let Some(params) = cast_request::<request::CodeActionRequest>(&req) {
        let result = handle_code_action(state, &params);
        send_response(conn, id, result);
    } else if let Some(params) = cast_request::<request::WorkspaceSymbolRequest>(&req) {
        let result = handle_workspace_symbol(state, &params);
        send_response(conn, id, result);
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
        let doc = analyze_document(&uri, &params.text_document.text);
        publish_diagnostics(conn, &uri, &doc);
        state.documents.insert(uri, doc);
    } else if not.method == notification::DidChangeTextDocument::METHOD {
        let params: DidChangeTextDocumentParams = serde_json::from_value(not.params).unwrap();
        let uri = params.text_document.uri.clone();
        if let Some(change) = params.content_changes.into_iter().last() {
            let doc = analyze_document(&uri, &change.text);
            publish_diagnostics(conn, &uri, &doc);
            state.documents.insert(uri, doc);
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

fn analyze_document(uri: &Uri, source: &str) -> DocumentState {
    let mut all_diags = Vec::new();
    let mut type_info = HashMap::new();
    let mut local_type_info = HashMap::new();
    let mut effect_info = HashMap::new();

    // Lex
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, lex_diags) = lexer.tokenize();
    all_diags.extend(lex_diags);

    // Parse
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    all_diags.extend(parse_diags);

    // Build navigation data from original AST
    let (definitions, references, literal_types) = resolve_definitions(&module, source);
    let details = build_details(&module);

    // Resolve import navigation (cross-file definitions)
    let (imported_files, import_defs) = if let Some(path) = uri_to_path(uri) {
        resolve_import_navigation(&module.imports, &path)
    } else {
        (HashMap::new(), HashMap::new())
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
        let (infer_diags, _monad_info, inferred_types, local_types) =
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
    }
}

/// Resolve imported files for cross-file navigation.
fn resolve_import_navigation(
    imports: &[ast::Import],
    source_path: &Path,
) -> (HashMap<PathBuf, String>, HashMap<String, (PathBuf, Span)>) {
    let mut imported_files = HashMap::new();
    let mut import_defs = HashMap::new();

    let base_dir = source_path.parent().unwrap_or(Path::new("."));

    for imp in imports {
        let rel_path = PathBuf::from(&imp.path).with_extension("knot");
        let full_path = base_dir.join(&rel_path);

        let canonical = match full_path.canonicalize() {
            Ok(p) => p,
            Err(_) => continue,
        };

        let source = match std::fs::read_to_string(&canonical) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let lexer = knot::lexer::Lexer::new(&source);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(source.clone(), tokens);
        let (module, _) = parser.parse_module();

        // Register definitions from this file
        for decl in &module.decls {
            match &decl.node {
                DeclKind::Data {
                    name, constructors, ..
                } => {
                    import_defs.insert(name.clone(), (canonical.clone(), decl.span));
                    for ctor in constructors {
                        // Find constructor span within the data decl
                        let ctor_span =
                            find_word_in_source(&source, &ctor.name, decl.span.start, decl.span.end)
                                .unwrap_or(decl.span);
                        import_defs.insert(ctor.name.clone(), (canonical.clone(), ctor_span));
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
                }
                DeclKind::Impl { items, .. } => {
                    for item in items {
                        if let ast::ImplItem::Method { name, .. } = item {
                            import_defs.insert(name.clone(), (canonical.clone(), decl.span));
                        }
                    }
                }
                _ => {}
            }
        }

        imported_files.insert(canonical, source);
    }

    (imported_files, import_defs)
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

    Some(Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: format!("```knot\n{detail}\n```"),
        }),
        range: None,
    })
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

    // General completion: keywords + declarations + builtins

    // Keywords
    for kw in KEYWORDS {
        items.push(CompletionItem {
            label: kw.to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
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
    // Check local type info (span-based: find a binding whose span matches this name)
    for (span, ty) in &doc.local_type_info {
        if span.start <= offset && offset < span.end {
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

    // Relation type: `[{name: Text}]` — extract inner type
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

    // Named type: look up in the module's type aliases and source declarations
    for decl in &module.decls {
        match &decl.node {
            DeclKind::TypeAlias { name, ty, .. } if name == type_str => {
                if let TypeKind::Record { fields, .. } = &ty.node {
                    return fields.iter().map(|f| f.name.clone()).collect();
                }
            }
            DeclKind::Source { name, ty, .. } if name == type_str => {
                if let TypeKind::Record { fields, .. } = &ty.node {
                    return fields.iter().map(|f| f.name.clone()).collect();
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

    // Find the definition span for the symbol at cursor
    let def_span = doc
        .references
        .iter()
        .find(|(usage, _)| usage.start <= offset && offset < usage.end)
        .map(|(_, def)| *def)
        .or_else(|| {
            // Cursor might be on a definition site itself
            let word = word_at_position(&doc.source, pos)?;
            doc.definitions.get(word).copied()
        })
        // Also check: cursor might be directly on a definition span
        .or_else(|| {
            doc.definitions.values().find(|span| span.start <= offset && offset < span.end).copied()
        })?;

    let mut locations = Vec::new();

    // Include declaration if requested
    if params.context.include_declaration {
        locations.push(Location {
            uri: uri.clone(),
            range: span_to_range(def_span, &doc.source),
        });
    }

    // Find all usages that point to this definition
    for (usage_span, target_span) in &doc.references {
        if *target_span == def_span {
            locations.push(Location {
                uri: uri.clone(),
                range: span_to_range(*usage_span, &doc.source),
            });
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

    let mut edits = Vec::new();

    // Rename at definition site
    let def_range = span_to_range(def_span, &doc.source);
    // Find the word within the definition span to rename precisely
    let old_name = word_at_position(&doc.source, pos)?;
    let def_text = &doc.source[def_span.start..def_span.end];
    if let Some(name_start) = def_text.find(old_name) {
        let name_span = Span::new(def_span.start + name_start, def_span.start + name_start + old_name.len());
        edits.push(TextEdit {
            range: span_to_range(name_span, &doc.source),
            new_text: new_name.clone(),
        });
    } else {
        edits.push(TextEdit {
            range: def_range,
            new_text: new_name.clone(),
        });
    }

    // Rename all usage sites
    for (usage_span, target_span) in &doc.references {
        if *target_span == def_span {
            edits.push(TextEdit {
                range: span_to_range(*usage_span, &doc.source),
                new_text: new_name.clone(),
            });
        }
    }

    let mut changes = HashMap::new();
    changes.insert(uri.clone(), edits);

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

    // Show inferred types for unannotated function declarations
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
                        text_edits: None,
                        tooltip: None,
                        padding_left: Some(true),
                        padding_right: Some(true),
                        data: None,
                    });
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
                        tooltip: None,
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

// ── Signature Help (paren-aware) ────────────────────────────────────

fn handle_signature_help(
    state: &ServerState,
    params: &SignatureHelpParams,
) -> Option<SignatureHelp> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;
    let offset = position_to_offset(&doc.source, pos);

    let bytes = doc.source.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    // Check if cursor is on an identifier (user is typing a word)
    let on_word = offset > 0 && offset <= bytes.len() && is_ident(bytes[offset - 1]);

    // Collect argument tokens going backwards from cursor.
    // Each "token" is either a bare identifier or a balanced paren/bracket group.
    let mut i = offset;
    let mut tokens: Vec<String> = Vec::new();

    loop {
        // Skip whitespace/newlines
        while i > 0 && matches!(bytes[i - 1], b' ' | b'\t' | b'\r' | b'\n') {
            i -= 1;
        }
        if i == 0 {
            break;
        }

        if is_ident(bytes[i - 1]) {
            let end = i;
            while i > 0 && is_ident(bytes[i - 1]) {
                i -= 1;
            }
            let word = doc.source[i..end].to_string();
            // Stop at keywords that can't be arguments
            if matches!(
                word.as_str(),
                "let" | "in" | "if" | "then" | "else" | "do" | "where" | "case" | "of"
                    | "yield" | "set" | "import" | "type" | "data" | "trait" | "impl"
                    | "route" | "migrate" | "atomic" | "full" | "export"
            ) {
                break;
            }
            tokens.push(word);
        } else if matches!(bytes[i - 1], b')' | b']' | b'}') {
            let close = bytes[i - 1];
            let open = match close {
                b')' => b'(',
                b']' => b'[',
                _ => b'{',
            };
            i -= 1;
            let mut depth = 1i32;
            while i > 0 && depth > 0 {
                i -= 1;
                if bytes[i] == close {
                    depth += 1;
                } else if bytes[i] == open {
                    depth -= 1;
                }
            }
            tokens.push("<group>".to_string());
        } else {
            break; // Operator, `=`, `<-`, etc. — stop
        }
    }

    if tokens.is_empty() {
        return None;
    }

    let func_name = tokens.last()?;
    let num_args = tokens.len() - 1; // excluding function name
    let active_param = if on_word {
        num_args.saturating_sub(1) as u32
    } else {
        num_args as u32
    };

    // Look up the function type
    let type_str = doc.type_info.get(func_name.as_str())?;

    // Parse arrow-separated parameters from the type string
    let params_list = parse_function_params(type_str);
    if params_list.is_empty() {
        return None;
    }

    let param_infos: Vec<ParameterInformation> = params_list
        .iter()
        .map(|p| ParameterInformation {
            label: ParameterLabel::Simple(p.clone()),
            documentation: None,
        })
        .collect();

    let signature = SignatureInformation {
        label: format!("{func_name} : {type_str}"),
        documentation: None,
        parameters: Some(param_infos),
        active_parameter: Some(active_param),
    };

    Some(SignatureHelp {
        signatures: vec![signature],
        active_signature: Some(0),
        active_parameter: Some(active_param),
    })
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
    let doc = state.documents.get(&params.text_document.uri)?;
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

        // Count references to this declaration
        let ref_count = doc
            .references
            .iter()
            .filter(|(_, def)| *def == decl.span)
            .count();

        let range = span_to_range(decl.span, &doc.source);
        let title = if ref_count == 1 {
            "1 reference".to_string()
        } else {
            format!("{ref_count} references")
        };

        lenses.push(CodeLens {
            range: Range {
                start: range.start,
                end: range.start, // code lens goes on a single line
            },
            command: Some(Command {
                title,
                command: String::new(),
                arguments: None,
            }),
            data: None,
        });

        // For traits: show number of implementations
        if let DeclKind::Trait { name, .. } = &decl.node {
            let impl_count = doc
                .module
                .decls
                .iter()
                .filter(|d| matches!(&d.node, DeclKind::Impl { trait_name, .. } if trait_name == name))
                .count();
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
                        command: String::new(),
                        arguments: None,
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
            // Skip multi-line tokens for now (semantic tokens should be single-line)
            let text = &self.source[span.start..span.end];
            if !text.contains('\n') {
                self.tokens.push(RawToken {
                    start: span.start,
                    length: span.end - span.start,
                    token_type,
                    modifiers,
                });
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
            _ => {}
        }
    }

    fn visit_expr(&mut self, expr: &ast::Expr) {
        match &expr.node {
            ast::ExprKind::Var(_) => {
                self.add(expr.span, TOK_VARIABLE, 0);
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
            ast::ExprKind::Atomic(e) => self.visit_expr(e),
            ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
                self.visit_expr(target);
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
        ast::ExprKind::Atomic(e) => {
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
                let decl_text = &doc.source[decl.span.start..decl.span.end.min(doc.source.len())];
                let name_end = decl_text
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(name.len());
                let insert_offset = decl.span.start + name_end;
                let insert_pos = offset_to_position(&doc.source, insert_offset);

                let mut changes = HashMap::new();
                changes.insert(
                    uri.clone(),
                    vec![TextEdit {
                        range: Range {
                            start: insert_pos,
                            end: insert_pos,
                        },
                        new_text: format!(" : {inferred}"),
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
            // Find the trait declaration to know which methods are required
            let trait_methods: Vec<&str> = doc
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
                .filter_map(|item| {
                    if let ast::TraitItem::Method {
                        name,
                        default_body: None,
                        ..
                    } = item
                    {
                        Some(name.as_str())
                    } else {
                        None
                    }
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

            let missing: Vec<&&str> = trait_methods
                .iter()
                .filter(|m| !impl_methods.contains(**m))
                .collect();

            if !missing.is_empty() {
                let insert_pos = offset_to_position(&doc.source, decl.span.end);
                let stubs: String = missing
                    .iter()
                    .map(|m| format!("\n  {m} x = x"))
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

                let missing_names = missing.iter().map(|m| **m).collect::<Vec<_>>().join(", ");
                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: format!("Add missing methods: {missing_names}"),
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

        if let Some(type_name) = scrutinee_type {
            let type_name = type_name.trim().to_string();
            // Find the data declaration for this type
            for decl in &doc.module.decls {
                if let DeclKind::Data {
                    name, constructors, ..
                } = &decl.node
                {
                    if *name == type_name {
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

                        if !missing.is_empty() {
                            let insert_pos = offset_to_position(&doc.source, expr.span.end);
                            let new_arms: String = missing
                                .iter()
                                .map(|c| {
                                    if c.fields.is_empty() {
                                        format!("\n  {} {{}} -> _", c.name)
                                    } else {
                                        let field_names: Vec<&str> =
                                            c.fields.iter().map(|f| f.name.as_str()).collect();
                                        format!(
                                            "\n  {} {{{}}} -> _",
                                            c.name,
                                            field_names.join(", ")
                                        )
                                    }
                                })
                                .collect();

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
                        }
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
        ast::ExprKind::Atomic(e) => {
            find_case_actions(e, doc, uri, range_start, range_end, actions);
        }
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
            find_case_actions(target, doc, uri, range_start, range_end, actions);
            find_case_actions(value, doc, uri, range_start, range_end, actions);
        }
        _ => {}
    }
}

// ── Workspace Symbols ───────────────────────────────────────────────

#[allow(deprecated)]
fn handle_workspace_symbol(
    state: &ServerState,
    params: &WorkspaceSymbolParams,
) -> Option<Vec<SymbolInformation>> {
    let query = params.query.to_lowercase();
    let mut symbols = Vec::new();

    for (uri, doc) in &state.documents {
        for decl in &doc.module.decls {
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

            // Filter by query
            if !query.is_empty() && !name.to_lowercase().contains(&query) {
                continue;
            }

            let range = span_to_range(decl.span, &doc.source);

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
    }

    if symbols.is_empty() {
        None
    } else {
        Some(symbols)
    }
}

// ── Constants ───────────────────────────────────────────────────────

const KEYWORDS: &[&str] = &[
    "import", "data", "type", "trait", "impl", "route", "migrate", "where", "do", "yield", "set",
    "if", "then", "else", "case", "of", "let", "in", "not", "full", "atomic", "deriving", "with",
    "export",
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
        match &decl.node {
            DeclKind::Data {
                name, constructors, ..
            } => {
                resolver.define(name, decl.span);
                for ctor in constructors {
                    // Point constructor to its own span within the data declaration
                    let ctor_span =
                        find_word_in_source(source, &ctor.name, decl.span.start, decl.span.end)
                            .unwrap_or(decl.span);
                    resolver.define(&ctor.name, ctor_span);
                }
            }
            DeclKind::TypeAlias { name, .. } => {
                resolver.define(name, decl.span);
            }
            DeclKind::Source { name, .. } | DeclKind::View { name, .. } => {
                resolver.define(name, decl.span);
            }
            DeclKind::Derived { name, .. } => {
                resolver.define(name, decl.span);
            }
            DeclKind::Fun { name, .. } => {
                resolver.define(name, decl.span);
            }
            DeclKind::Trait { name, items, .. } => {
                resolver.define(name, decl.span);
                for item in items {
                    if let ast::TraitItem::Method { name, .. } = item {
                        resolver.define(name, decl.span);
                    }
                }
            }
            DeclKind::Route { name, .. } | DeclKind::RouteComposite { name, .. } => {
                resolver.define(name, decl.span);
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
        TypeKind::IO { effects, ty } => {
            if effects.is_empty() {
                format!("IO {}", format_type_kind(&ty.node))
            } else {
                format!("IO {{{}}} {}", effects.join(", "), format_type_kind(&ty.node))
            }
        }
        TypeKind::Hole => "_".into(),
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

    Some(Diagnostic {
        range,
        severity: Some(severity),
        source: Some("knot".into()),
        message,
        related_information: if related.is_empty() {
            None
        } else {
            Some(related)
        },
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
