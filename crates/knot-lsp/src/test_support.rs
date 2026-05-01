//! In-process test harness for LSP handler scenarios. Builds a `ServerState`
//! with real analyzed documents so handlers can be invoked without spawning
//! a worker thread or talking over stdio. Intended for use only from
//! `#[cfg(test)]` blocks in feature modules.

#![cfg(test)]
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use lsp_types::{Position, Range, Uri};

use crate::analysis::analyze_document;
use crate::state::{
    DocumentState, ServerState, WorkspaceSymbolCache,
};
use crate::utils::offset_to_position;

/// Builder + container for a synthetic LSP workspace. Holds a `ServerState`
/// with one or more documents pre-analyzed; tests open files, look up
/// positions by string match, and invoke handlers directly.
pub(crate) struct TestWorkspace {
    pub state: ServerState,
    /// Track URIs by their assigned name so tests can refer to them stably.
    uris: HashMap<String, Uri>,
}

impl TestWorkspace {
    pub fn new() -> Self {
        // The analysis tx side is never used in tests — the worker isn't
        // running. We still need a real Sender to satisfy ServerState's type.
        let (analysis_tx, _analysis_rx) = crossbeam_channel::unbounded();
        let state = ServerState {
            documents: HashMap::new(),
            workspace_root: None,
            import_cache: Arc::new(Mutex::new(HashMap::new())),
            workspace_diag_cache: HashMap::new(),
            workspace_symbol_cache: WorkspaceSymbolCache::default(),
            pending_sources: HashMap::new(),
            analysis_tx,
            reverse_imports: HashMap::new(),
            inference_cache: Arc::new(Mutex::new(HashMap::new())),
        };
        TestWorkspace {
            state,
            uris: HashMap::new(),
        }
    }

    /// Open a file in the synthetic workspace and run the analysis pipeline
    /// synchronously (no worker thread). Returns the URI assigned to the file.
    /// `name` is a label like `"main"` — it becomes part of the URI.
    pub fn open(&mut self, name: &str, source: &str) -> Uri {
        let uri = Uri::from_str(&format!("file:///test/{name}.knot")).expect("valid uri");
        let mut import_cache = HashMap::new();
        let mut inference_cache = HashMap::new();
        let doc = analyze_document(&uri, source, &mut import_cache, &mut inference_cache);
        // Merge import cache results back into shared state so cross-file
        // navigation tests can find imported modules.
        if let Ok(mut shared) = self.state.import_cache.lock() {
            for (k, v) in import_cache {
                shared.insert(k, v);
            }
        }
        self.state.documents.insert(uri.clone(), doc);
        self.uris.insert(name.to_string(), uri.clone());
        uri
    }

    /// Open multiple files in dependency order. The last entry's analysis
    /// will see the prior entries' definitions (assuming `import` statements
    /// are present and resolvable on disk — for cross-file tests, prefer
    /// `open_with_real_files`).
    pub fn open_many(&mut self, files: &[(&str, &str)]) -> Vec<Uri> {
        files.iter().map(|(n, s)| self.open(n, s)).collect()
    }

    /// Look up the URI assigned to a previously-opened file by name.
    pub fn uri(&self, name: &str) -> &Uri {
        self.uris.get(name).expect("unknown file name")
    }

    /// Borrow an analyzed document.
    pub fn doc(&self, uri: &Uri) -> &DocumentState {
        self.state.documents.get(uri).expect("doc not opened")
    }

    /// Find the position of the *first* occurrence of `needle` in the file's
    /// source. Returns the start of the match. Tests use this to anchor
    /// positions to known source patterns rather than hard-coding line/col.
    pub fn position_of(&self, uri: &Uri, needle: &str) -> Position {
        let doc = self.doc(uri);
        let offset = doc
            .source
            .find(needle)
            .unwrap_or_else(|| panic!("needle {needle:?} not found in {}", uri.as_str()));
        offset_to_position(&doc.source, offset)
    }

    /// Like `position_of` but returns the position of the *last* character
    /// of the match — useful for positioning the cursor right after a word.
    pub fn position_after(&self, uri: &Uri, needle: &str) -> Position {
        let doc = self.doc(uri);
        let offset = doc
            .source
            .find(needle)
            .unwrap_or_else(|| panic!("needle {needle:?} not found in {}", uri.as_str()));
        offset_to_position(&doc.source, offset + needle.len())
    }

    /// Range covering the entire content of a file.
    pub fn whole_file_range(&self, uri: &Uri) -> Range {
        let doc = self.doc(uri);
        Range {
            start: Position::new(0, 0),
            end: offset_to_position(&doc.source, doc.source.len()),
        }
    }
}

/// Convenience: build a temp directory, write files into it, and return a
/// workspace whose `workspace_root` points at the tempdir. Used by tests
/// that need real on-disk imports to resolve.
pub(crate) struct TempWorkspace {
    pub root: PathBuf,
    pub workspace: TestWorkspace,
}

impl TempWorkspace {
    pub fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "knot-lsp-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&root).expect("create tempdir");
        let mut workspace = TestWorkspace::new();
        workspace.state.workspace_root = Some(root.clone());
        TempWorkspace { root, workspace }
    }

    /// Write a file to disk and analyze it through the LSP. URI uses the
    /// real on-disk path so cross-file imports resolve correctly.
    pub fn write_and_open(&mut self, rel_path: &str, source: &str) -> Uri {
        let path = self.root.join(rel_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent dir");
        }
        std::fs::write(&path, source).expect("write file");
        let canonical = path.canonicalize().unwrap_or(path);
        let uri = Uri::from_str(&format!("file://{}", canonical.display())).expect("valid uri");
        let mut import_cache = HashMap::new();
        let mut inference_cache = HashMap::new();
        let doc = analyze_document(&uri, source, &mut import_cache, &mut inference_cache);
        if let Ok(mut shared) = self.workspace.state.import_cache.lock() {
            for (k, v) in import_cache {
                shared.insert(k, v);
            }
        }
        self.workspace
            .state
            .documents
            .insert(uri.clone(), doc);
        self.workspace
            .uris
            .insert(rel_path.to_string(), uri.clone());
        uri
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
