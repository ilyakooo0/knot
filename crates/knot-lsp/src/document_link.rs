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
        // `with_extension("knot")` replaces an existing extension if any —
        // covers both `./foo` (compiler-conventional) and `./foo.knot`
        // (sometimes written by hand). The extension always ends up exactly
        // once, no matter what the user typed.
        let rel_path = PathBuf::from(&imp.path).with_extension("knot");
        let full_path = base_dir.join(&rel_path);

        // Prefer the canonical path when the file exists — it deduplicates
        // symlinks and handles `..`/`.` segments. Fall back to the resolved
        // (but not canonicalized) path when the target doesn't exist yet:
        // an editor can still navigate to the location, which lets the user
        // create the missing file via the link rather than retyping the
        // path. Compiler error reporting will still fail loudly later.
        let (resolved, exists) = match full_path.canonicalize() {
            Ok(p) => (p, true),
            Err(_) => (full_path.clone(), false),
        };
        let Some(target_uri) = path_to_uri(&resolved) else {
            continue;
        };

        // The link range covers the import path string within the import span.
        // Find the path string in the source text of this import.
        let import_text = safe_slice(&doc.source, imp.span);
        if let Some(path_start) = import_text.find(&imp.path) {
            let abs_start = imp.span.start + path_start;
            let abs_end = abs_start + imp.path.len();
            let tooltip = if exists {
                format!("{}", resolved.display())
            } else {
                format!("{} (not found — click to create)", resolved.display())
            };
            links.push(DocumentLink {
                range: span_to_range(Span::new(abs_start, abs_end), &doc.source),
                target: Some(target_uri),
                tooltip: Some(tooltip),
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
