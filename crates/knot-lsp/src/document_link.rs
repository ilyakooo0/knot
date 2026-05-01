//! `textDocument/documentLink` handler. Resolves `import path` lines to
//! navigable links pointing at the target .knot file.

use std::path::{Path, PathBuf};

use lsp_types::*;

use knot::ast::Span;

use crate::state::ServerState;
use crate::utils::{path_to_uri, safe_slice, span_to_range, uri_to_path};

// ── Document Links ──────────────────────────────────────────────────

pub(crate) fn handle_document_link(
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
        let import_text = safe_slice(&doc.source, imp.span);
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
