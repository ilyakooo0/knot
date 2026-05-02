//! Per-declaration change detection — the foundation for an incremental
//! type-checking pipeline. Hashes each decl's AST shape and computes a
//! dependency graph so analysis can identify which decls actually changed
//! between edits and which downstream decls a change may impact.
//!
//! This module is the LSP-side scaffolding. True per-decl selective
//! re-inference also requires changes to `infer.rs` so the inferencer can
//! reuse cached schemes for clean decls and only re-check dirty ones — see
//! `state.rs::InferenceSnapshot` for the overall design notes.
//!
//! Even without selective re-inference, the change detector lights up two
//! immediate wins:
//!
//! 1. **Comment- and whitespace-only edits.** When the AST shape is
//!    structurally identical to the cached previous run, the inference
//!    snapshot can be reused — the existing content-hash cache misses on
//!    these edits because the source bytes differ.
//!
//! 2. **Telemetry.** Knowing how often each decl changes (and how often
//!    edits cascade to dependents) tells us where the future selective
//!    re-inference will pay off most.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use knot::ast::{self, DeclKind, Module};

/// Per-declaration analysis fingerprint. Stored alongside the inference
/// snapshot so the next edit can compute a delta cheaply.
#[derive(Clone, Debug)]
pub struct ModuleFingerprint {
    /// Map from decl name (or synthetic key for unnamed decls like impls)
    /// to a hash of that decl's AST. Spans are excluded from the hash —
    /// only structural content matters, so reformatting a decl that doesn't
    /// change tokens is a "clean" delta.
    pub decl_hashes: HashMap<String, u64>,
    /// Dependency graph: `decl_name → set of top-level names it references`.
    /// References include calls to other functions, type uses, and
    /// constructor uses. Used to compute the transitive dirty set when a
    /// decl's type signature might have changed.
    ///
    /// Consumed by `dirty_closure`, which feeds `DocumentState::dirty_decl_closure`.
    /// Inlay hints surface this set under `KNOT_LSP_TRACE_DIRTY` so
    /// developers can watch the per-edit dirty closure live.
    pub decl_deps: HashMap<String, HashSet<String>>,
    /// Hash of the module's overall import set + decl signature shapes
    /// (names + declared types, ignoring bodies). When this changes, even
    /// "clean-bodied" decls might need re-checking because the trait
    /// resolution / import graph shifted.
    pub structure_hash: u64,
}

impl ModuleFingerprint {
    /// Compute the fingerprint of a freshly-parsed module.
    pub fn from_module(module: &Module) -> Self {
        let mut decl_hashes = HashMap::new();
        let mut decl_deps = HashMap::new();
        for (i, decl) in module.decls.iter().enumerate() {
            let key = decl_key(decl, i);
            decl_hashes.insert(key.clone(), hash_decl(decl));
            decl_deps.insert(key, collect_decl_deps(decl));
        }
        let structure_hash = hash_structure(module);
        ModuleFingerprint {
            decl_hashes,
            decl_deps,
            structure_hash,
        }
    }

    /// Compute the set of decl keys that changed between `prev` and `self`.
    /// A decl is considered changed if its hash differs, was added, or was
    /// removed. Drives `apply_analysis_result`'s selective dependent
    /// re-queue: only files that import a *changed* name are re-analyzed.
    pub fn changed_decls(&self, prev: &ModuleFingerprint) -> HashSet<String> {
        let mut changed: HashSet<String> = HashSet::new();
        for (k, h) in &self.decl_hashes {
            match prev.decl_hashes.get(k) {
                Some(prev_h) if prev_h == h => {}
                _ => {
                    changed.insert(k.clone());
                }
            }
        }
        for k in prev.decl_hashes.keys() {
            if !self.decl_hashes.contains_key(k) {
                changed.insert(k.clone());
            }
        }
        changed
    }

    /// Transitively expand `seed` to include every decl that depends on a
    /// decl already in the seed set. Conservative: when a decl's type might
    /// change, every decl referencing it is re-checked. Output flows through
    /// `DocumentState::dirty_decl_closure` to the inlay-hint telemetry path
    /// and (eventually) the selective-inference entry point.
    pub fn dirty_closure(&self, seed: &HashSet<String>) -> HashSet<String> {
        // Reverse-deps: who references whom.
        let mut reverse: HashMap<&str, Vec<&str>> = HashMap::new();
        for (decl, deps) in &self.decl_deps {
            for d in deps {
                reverse.entry(d.as_str()).or_default().push(decl.as_str());
            }
        }
        let mut frontier: Vec<String> = seed.iter().cloned().collect();
        let mut closure = seed.clone();
        while let Some(name) = frontier.pop() {
            if let Some(consumers) = reverse.get(name.as_str()) {
                for c in consumers {
                    if closure.insert((*c).to_string()) {
                        frontier.push((*c).to_string());
                    }
                }
            }
        }
        closure
    }

    /// Returns true when `prev` and `self` are structurally identical
    /// (same decl set, same hashes, same module-level shape). When this
    /// holds, comment- and whitespace-only edits can reuse the cached
    /// inference output even if the raw content hash differs.
    pub fn structurally_equal(&self, prev: &ModuleFingerprint) -> bool {
        self.structure_hash == prev.structure_hash
            && self.decl_hashes.len() == prev.decl_hashes.len()
            && self
                .decl_hashes
                .iter()
                .all(|(k, h)| prev.decl_hashes.get(k) == Some(h))
    }
}

/// Build a stable key for a decl. Named decls use their name; impls and
/// other unnamed decls fall back to a positional key combined with their
/// shape so reordering doesn't alias.
fn decl_key(decl: &ast::Decl, index: usize) -> String {
    match &decl.node {
        DeclKind::Fun { name, .. }
        | DeclKind::Source { name, .. }
        | DeclKind::View { name, .. }
        | DeclKind::Derived { name, .. }
        | DeclKind::Data { name, .. }
        | DeclKind::TypeAlias { name, .. }
        | DeclKind::Trait { name, .. }
        | DeclKind::Route { name, .. } => name.clone(),
        DeclKind::Impl { trait_name, args, .. } => {
            // Impls aren't named — combine trait + first-arg shape so two
            // distinct impls don't collide.
            let arg_shape: String = args
                .iter()
                .map(|a| format!("{:?}", a.node))
                .collect::<Vec<_>>()
                .join("/");
            format!("__impl[{trait_name}]({arg_shape})#{index}")
        }
        DeclKind::Migrate { .. } => format!("__migrate#{index}"),
        DeclKind::SubsetConstraint { .. } => format!("__subset#{index}"),
        DeclKind::UnitDecl { name, .. } => format!("__unit:{name}"),
        DeclKind::RouteComposite { name, .. } => format!("__route_comp:{name}"),
    }
}

/// Hash a declaration ignoring its source spans. We rely on `Debug`-printed
/// form as a stable structural representation; this keeps the impl simple
/// and avoids hand-writing a deep-walk hasher for every node kind. The
/// hash is keyed off the AST shape only — formatting and span shifts in
/// otherwise-identical decls produce the same hash.
fn hash_decl(decl: &ast::Decl) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    // Debug format includes spans, so strip them from the rendered string
    // before hashing. Spans look like `Span { start: 12, end: 34 }`.
    let raw = format!("{:?}", decl.node);
    let stripped = strip_spans(&raw);
    stripped.hash(&mut h);
    h.finish()
}

/// Hash module-level structure: imports plus the *signature* of each decl
/// (no bodies). When this changes, the import graph or trait surface
/// shifted and even "clean-bodied" decls may need re-checking.
fn hash_structure(module: &Module) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for imp in &module.imports {
        imp.path.hash(&mut h);
    }
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { name, ty, .. } => {
                ("fun", name).hash(&mut h);
                if let Some(ts) = ty {
                    strip_spans(&format!("{:?}", ts.ty.node)).hash(&mut h);
                }
            }
            DeclKind::Source { name, ty, history, .. } => {
                ("source", name, history).hash(&mut h);
                strip_spans(&format!("{:?}", ty.node)).hash(&mut h);
            }
            DeclKind::View { name, ty, .. } | DeclKind::Derived { name, ty, .. } => {
                ("vd", name).hash(&mut h);
                if let Some(ts) = ty {
                    strip_spans(&format!("{:?}", ts.ty.node)).hash(&mut h);
                }
            }
            DeclKind::Data {
                name,
                constructors,
                ..
            } => {
                ("data", name).hash(&mut h);
                for c in constructors {
                    c.name.hash(&mut h);
                    for f in &c.fields {
                        f.name.hash(&mut h);
                        strip_spans(&format!("{:?}", f.value.node)).hash(&mut h);
                    }
                }
            }
            DeclKind::TypeAlias { name, .. } => {
                ("alias", name).hash(&mut h);
                strip_spans(&format!("{:?}", decl.node)).hash(&mut h);
            }
            DeclKind::Trait { name, .. } => {
                ("trait", name).hash(&mut h);
                strip_spans(&format!("{:?}", decl.node)).hash(&mut h);
            }
            DeclKind::Impl { trait_name, args, .. } => {
                ("impl", trait_name).hash(&mut h);
                for a in args {
                    strip_spans(&format!("{:?}", a.node)).hash(&mut h);
                }
            }
            DeclKind::Route { name, .. } => {
                ("route", name).hash(&mut h);
                strip_spans(&format!("{:?}", decl.node)).hash(&mut h);
            }
            other => {
                strip_spans(&format!("{:?}", other)).hash(&mut h);
            }
        }
    }
    h.finish()
}

/// Strip `Span { start: NN, end: NN }` substrings from a Debug rendering.
/// Spans depend on byte offsets that shift with formatting changes; we want
/// hashes that survive whitespace edits.
fn strip_spans(s: &str) -> String {
    // Cheap state machine: walk the string and skip the span markers.
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i..].starts_with(b"Span {") {
            // Skip until the matching `}`. Spans don't contain nested `}`,
            // so a simple seek is enough.
            if let Some(end) = bytes[i..].iter().position(|&b| b == b'}') {
                i += end + 1;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Collect every top-level name a declaration's body references. Used to
/// build the reverse-dependency graph for selective re-inference.
fn collect_decl_deps(decl: &ast::Decl) -> HashSet<String> {
    let mut deps = HashSet::new();
    match &decl.node {
        DeclKind::Fun {
            body: Some(body), ..
        } => collect_expr_names(body, &mut deps),
        DeclKind::View { body, .. } | DeclKind::Derived { body, .. } => {
            collect_expr_names(body, &mut deps);
        }
        DeclKind::Impl { items, .. } => {
            for item in items {
                if let ast::ImplItem::Method { body, .. } = item {
                    collect_expr_names(body, &mut deps);
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
                    collect_expr_names(body, &mut deps);
                }
            }
        }
        _ => {}
    }
    deps
}

fn collect_expr_names(expr: &ast::Expr, out: &mut HashSet<String>) {
    match &expr.node {
        ast::ExprKind::Var(name) => {
            out.insert(name.clone());
        }
        ast::ExprKind::SourceRef(name) | ast::ExprKind::DerivedRef(name) => {
            out.insert(name.clone());
        }
        ast::ExprKind::Constructor(name) => {
            out.insert(name.clone());
        }
        _ => {}
    }
    crate::utils::recurse_expr(expr, |e| collect_expr_names(e, out));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_module(src: &str) -> Module {
        let lex = knot::lexer::Lexer::new(src);
        let (tokens, _) = lex.tokenize();
        let parser = knot::parser::Parser::new(src.to_string(), tokens);
        let (m, _) = parser.parse_module();
        m
    }

    #[test]
    fn fingerprint_invariant_to_whitespace_changes() {
        let a = parse_module("foo = \\x -> x\nbar = \\y -> y\n");
        let b = parse_module("foo = \\x -> x\n\n\nbar   =    \\y -> y\n");
        let fa = ModuleFingerprint::from_module(&a);
        let fb = ModuleFingerprint::from_module(&b);
        assert!(
            fa.structurally_equal(&fb),
            "whitespace-only edit should preserve fingerprint"
        );
    }

    #[test]
    fn fingerprint_detects_body_change() {
        let a = parse_module("double = \\x -> x * 2\n");
        let b = parse_module("double = \\x -> x * 3\n");
        let fa = ModuleFingerprint::from_module(&a);
        let fb = ModuleFingerprint::from_module(&b);
        let changed = fb.changed_decls(&fa);
        assert!(changed.contains("double"), "got: {changed:?}");
    }

    #[test]
    fn fingerprint_dirty_closure_propagates_through_deps() {
        let m = parse_module(
            r#"helper = \x -> x + 1
caller = \y -> helper y
top = \z -> caller z
"#,
        );
        let fp = ModuleFingerprint::from_module(&m);
        let mut seed = HashSet::new();
        seed.insert("helper".to_string());
        let closure = fp.dirty_closure(&seed);
        assert!(closure.contains("helper"));
        assert!(closure.contains("caller"));
        assert!(closure.contains("top"), "closure: {closure:?}");
    }

    #[test]
    fn fingerprint_unrelated_decls_are_not_dirty() {
        let m = parse_module(
            r#"a = \x -> x
b = \y -> y
"#,
        );
        let fp = ModuleFingerprint::from_module(&m);
        let mut seed = HashSet::new();
        seed.insert("a".to_string());
        let closure = fp.dirty_closure(&seed);
        assert!(closure.contains("a"));
        assert!(!closure.contains("b"), "closure: {closure:?}");
    }

    #[test]
    fn fingerprint_invariant_to_doc_comment_changes() {
        let a = parse_module("foo = \\x -> x\n");
        let b = parse_module("-- a doc comment\nfoo = \\x -> x\n");
        let fa = ModuleFingerprint::from_module(&a);
        let fb = ModuleFingerprint::from_module(&b);
        assert!(
            fa.structurally_equal(&fb),
            "comment-only edit should preserve fingerprint"
        );
    }

    #[test]
    fn fingerprint_detects_added_decl() {
        let a = parse_module("foo = \\x -> x\n");
        let b = parse_module("foo = \\x -> x\nbar = \\y -> y\n");
        let fa = ModuleFingerprint::from_module(&a);
        let fb = ModuleFingerprint::from_module(&b);
        assert!(!fa.structurally_equal(&fb));
        let changed = fb.changed_decls(&fa);
        assert!(changed.contains("bar"));
    }
}
