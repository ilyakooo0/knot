use std::collections::HashMap;
use std::path::PathBuf;

use lsp_server::{Connection, Message, Notification, Request, RequestId, Response};
use lsp_types::notification::Notification as _;
use lsp_types::*;

use knot::ast::{self, DeclKind, Module, Span, TypeKind, TypeScheme};
use knot::diagnostic;

// ── Types ───────────────────────────────────────────────────────────

struct DocumentState {
    source: String,
    module: Module,
    definitions: HashMap<String, Span>,
    details: HashMap<String, String>,
    type_info: HashMap<String, String>,
    knot_diagnostics: Vec<diagnostic::Diagnostic>,
}

struct ServerState {
    documents: HashMap<Uri, DocumentState>,
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

    // Lex
    let lexer = knot::lexer::Lexer::new(source);
    let (tokens, lex_diags) = lexer.tokenize();
    all_diags.extend(lex_diags);

    // Parse
    let parser = knot::parser::Parser::new(source.to_string(), tokens);
    let (module, parse_diags) = parser.parse_module();
    all_diags.extend(parse_diags);

    // Build navigation data from original AST
    let definitions = build_definitions(&module);
    let details = build_details(&module);

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
        let (infer_diags, _monad_info, inferred_types) =
            knot_compiler::infer::check(&analysis_module);
        all_diags.extend(infer_diags);
        type_info = inferred_types;

        // Effect inference
        all_diags.extend(knot_compiler::effects::check(&analysis_module));

        // Stratification
        all_diags.extend(knot_compiler::stratify::check(&analysis_module));
    }

    DocumentState {
        source: source.to_string(),
        module,
        definitions,
        details,
        type_info,
        knot_diagnostics: all_diags,
    }
}

fn publish_diagnostics(conn: &Connection, uri: &Uri, doc: &DocumentState) {
    let lsp_diags: Vec<Diagnostic> = doc
        .knot_diagnostics
        .iter()
        .filter_map(|d| to_lsp_diagnostic(d, &doc.source))
        .collect();

    let params = PublishDiagnosticsParams::new(uri.clone(), lsp_diags, None);
    let not = Notification::new(
        notification::PublishDiagnostics::METHOD.into(),
        params,
    );
    conn.sender.send(Message::Notification(not)).unwrap();
}

// ── Document symbols ────────────────────────────────────────────────

fn handle_document_symbol(
    state: &ServerState,
    params: &DocumentSymbolParams,
) -> Option<DocumentSymbolResponse> {
    let doc = state.documents.get(&params.text_document.uri)?;
    let symbols = build_symbols(&doc.module, &doc.source);
    Some(DocumentSymbolResponse::Flat(symbols))
}

fn build_symbols(module: &Module, source: &str) -> Vec<SymbolInformation> {
    let mut symbols = Vec::new();

    for decl in &module.decls {
        let (name, kind) = match &decl.node {
            DeclKind::Data { name, .. } => (name.clone(), SymbolKind::STRUCT),
            DeclKind::TypeAlias { name, .. } => (name.clone(), SymbolKind::TYPE_PARAMETER),
            DeclKind::Source { name, .. } => (format!("*{name}"), SymbolKind::VARIABLE),
            DeclKind::View { name, .. } => (format!("*{name}"), SymbolKind::VARIABLE),
            DeclKind::Derived { name, .. } => (format!("&{name}"), SymbolKind::VARIABLE),
            DeclKind::Fun { name, .. } => (name.clone(), SymbolKind::FUNCTION),
            DeclKind::Trait { name, .. } => (name.clone(), SymbolKind::INTERFACE),
            DeclKind::Impl { trait_name, args, .. } => {
                let args_str = args
                    .iter()
                    .map(|a| format_type_kind(&a.node))
                    .collect::<Vec<_>>()
                    .join(" ");
                (format!("impl {trait_name} {args_str}"), SymbolKind::OBJECT)
            }
            DeclKind::Route { name, .. } => (format!("route {name}"), SymbolKind::MODULE),
            DeclKind::RouteComposite { name, .. } => (format!("route {name}"), SymbolKind::MODULE),
            DeclKind::Migrate { relation, .. } => {
                (format!("migrate *{relation}"), SymbolKind::EVENT)
            }
            DeclKind::SubsetConstraint { .. } => continue,
        };

        let range = span_to_range(decl.span, source);

        #[allow(deprecated)]
        symbols.push(SymbolInformation {
            name,
            kind,
            tags: None,
            deprecated: None,
            location: Location {
                uri: "file:///".parse::<Uri>().unwrap(), // placeholder, overridden by client
                range,
            },
            container_name: None,
        });
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

    let word = word_at_position(&doc.source, pos)?;
    let span = doc.definitions.get(word)?;

    let range = span_to_range(*span, &doc.source);
    Some(GotoDefinitionResponse::Scalar(Location {
        uri: uri.clone(),
        range,
    }))
}

// ── Hover ───────────────────────────────────────────────────────────

fn handle_hover(state: &ServerState, params: &HoverParams) -> Option<Hover> {
    let uri = &params.text_document_position_params.text_document.uri;
    let pos = params.text_document_position_params.position;
    let doc = state.documents.get(uri)?;

    let word = word_at_position(&doc.source, pos)?;

    // Prefer AST-level details (richer for data/type decls), fall back to inferred types
    let detail = if let Some(d) = doc.details.get(word) {
        // If we have an inferred type and the AST detail has no type annotation,
        // enhance with the inferred type
        if let Some(inferred) = doc.type_info.get(word) {
            if !d.contains(':') {
                format!("{d} : {inferred}")
            } else {
                d.clone()
            }
        } else {
            d.clone()
        }
    } else if let Some(inferred) = doc.type_info.get(word) {
        // No AST detail but have inferred type (e.g. builtins, prelude functions)
        format!("{word} : {inferred}")
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

    let mut items = Vec::new();

    // Keywords
    for kw in KEYWORDS {
        items.push(CompletionItem {
            label: kw.to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        });
    }

    // Declarations from current document
    for decl in &doc.module.decls {
        match &decl.node {
            DeclKind::Data {
                name, constructors, ..
            } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::STRUCT),
                    ..Default::default()
                });
                for ctor in constructors {
                    items.push(CompletionItem {
                        label: ctor.name.clone(),
                        kind: Some(CompletionItemKind::ENUM_MEMBER),
                        ..Default::default()
                    });
                }
            }
            DeclKind::TypeAlias { name, .. } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::STRUCT),
                    ..Default::default()
                });
            }
            DeclKind::Source { name, .. } | DeclKind::View { name, .. } => {
                items.push(CompletionItem {
                    label: format!("*{name}"),
                    kind: Some(CompletionItemKind::VARIABLE),
                    insert_text: Some(format!("*{name}")),
                    ..Default::default()
                });
            }
            DeclKind::Derived { name, .. } => {
                items.push(CompletionItem {
                    label: format!("&{name}"),
                    kind: Some(CompletionItemKind::VARIABLE),
                    insert_text: Some(format!("&{name}")),
                    ..Default::default()
                });
            }
            DeclKind::Fun { name, .. } => {
                items.push(CompletionItem {
                    label: name.clone(),
                    kind: Some(CompletionItemKind::FUNCTION),
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

    // Built-in functions
    for name in BUILTINS {
        items.push(CompletionItem {
            label: name.to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            ..Default::default()
        });
    }

    Some(CompletionResponse::Array(items))
}

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

// ── Build navigation data ───────────────────────────────────────────

fn build_definitions(module: &Module) -> HashMap<String, Span> {
    let mut defs = HashMap::new();

    for decl in &module.decls {
        match &decl.node {
            DeclKind::Data {
                name, constructors, ..
            } => {
                defs.insert(name.clone(), decl.span);
                for ctor in constructors {
                    defs.insert(ctor.name.clone(), decl.span);
                }
            }
            DeclKind::TypeAlias { name, .. } => {
                defs.insert(name.clone(), decl.span);
            }
            DeclKind::Source { name, .. } | DeclKind::View { name, .. } => {
                defs.insert(name.clone(), decl.span);
            }
            DeclKind::Derived { name, .. } => {
                defs.insert(name.clone(), decl.span);
            }
            DeclKind::Fun { name, .. } => {
                defs.insert(name.clone(), decl.span);
            }
            DeclKind::Trait { name, items, .. } => {
                defs.insert(name.clone(), decl.span);
                for item in items {
                    if let ast::TraitItem::Method { name, .. } = item {
                        defs.insert(name.clone(), decl.span);
                    }
                }
            }
            DeclKind::Route { name, .. } | DeclKind::RouteComposite { name, .. } => {
                defs.insert(name.clone(), decl.span);
            }
            _ => {}
        }
    }

    defs
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

fn to_lsp_diagnostic(diag: &diagnostic::Diagnostic, source: &str) -> Option<Diagnostic> {
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

    // Build related information from additional labels
    let related: Vec<DiagnosticRelatedInformation> = diag
        .labels
        .iter()
        .skip(1)
        .filter(|l| l.span.start < source.len() && l.span.end <= source.len())
        .map(|l| DiagnosticRelatedInformation {
            location: Location {
                uri: "file:///".parse::<Uri>().unwrap(),
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
