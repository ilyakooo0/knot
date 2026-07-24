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

use knot::ast::{self, Expr, ExprKind};
use std::collections::hash_map::DefaultHasher;

/// Per-declaration analysis fingerprint. Stored alongside the inference
/// snapshot so the next edit can compute a delta cheaply.
#[derive(Clone, Debug)]
pub struct ModuleFingerprint {
    /// Map from decl name (or synthetic key for unnamed decls like impls)
    /// to a hash of that decl's AST. Spans are excluded from the hash —
    /// only structural content matters, so reformatting a decl that doesn't
    /// change tokens is a "clean" delta.
    pub decl_hashes: HashMap<String, u64>,
    /// Per-decl signature hash. For `Fun` decls with an explicit type
    /// annotation this hashes only the signature line — body changes don't
    /// move the value. For Fun decls *without* a signature, the hash also
    /// reflects the body, since the inferred type could shift. For other
    /// decl kinds (Source/View/Derived/Data/Trait/Impl) the signature hash
    /// covers the externally-visible shape only.
    ///
    /// `signature_changed_decls` uses this to compute a narrower change set
    /// than `changed_decls`: dependents only need to be re-checked when an
    /// upstream decl's *externally-visible* type/shape changed, not when a
    /// signed body got rewritten internally. Reduces cascade re-analysis on
    /// the common "edit a function body" workflow.
    pub decl_signature_hashes: HashMap<String, u64>,
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
    ///
    /// Kept around (alongside `structurally_equal`) for the planned
    /// fingerprint-cache reuse path: it lets a whitespace/comment-only edit
    /// skip re-inference once a span remapper exists. Today the caller
    /// (`analyze_document`) only consumes the per-decl `decl_hashes` and
    /// `decl_signature_hashes`.
    #[allow(dead_code)]
    pub structure_hash: u64,
}

impl ModuleFingerprint {
    /// Compute the fingerprint of a freshly-parsed program.
    pub fn from_module(program: &Expr) -> Self {
        let mut decl_hashes = HashMap::new();
        let mut decl_signature_hashes = HashMap::new();
        let mut decl_deps = HashMap::new();
        for (i, field) in top_fields(program).iter().enumerate() {
            let key = field_key(field, i);
            decl_hashes.insert(key.clone(), hash_field(field));
            decl_signature_hashes.insert(key.clone(), hash_field_signature(field));
            decl_deps.insert(key, collect_field_deps(field));
        }
        let structure_hash = hash_structure(program);
        ModuleFingerprint {
            decl_hashes,
            decl_signature_hashes,
            decl_deps,
            structure_hash,
        }
    }

    /// Decls whose externally-visible signature changed between `prev` and
    /// `self`. Strict subset of `changed_decls`: a function whose explicit
    /// type signature is unchanged but whose body got rewritten falls in
    /// `changed_decls` but NOT here. Drives the cross-file dependent
    /// re-queue: dependents of `f` only need re-analysis when `f`'s
    /// outward-facing type or shape moves.
    pub fn signature_changed_decls(&self, prev: &ModuleFingerprint) -> HashSet<String> {
        let mut changed: HashSet<String> = HashSet::new();
        for (k, h) in &self.decl_signature_hashes {
            match prev.decl_signature_hashes.get(k) {
                Some(prev_h) if prev_h == h => {}
                _ => {
                    changed.insert(k.clone());
                }
            }
        }
        for k in prev.decl_signature_hashes.keys() {
            if !self.decl_signature_hashes.contains_key(k) {
                changed.insert(k.clone());
            }
        }
        changed
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
    /// inference output *if the consumer remaps spans* — see the comment
    /// in `analyze_document` about why the bare reuse path was disabled.
    #[allow(dead_code)]
    pub fn structurally_equal(&self, prev: &ModuleFingerprint) -> bool {
        self.structure_hash == prev.structure_hash
            && self.decl_hashes.len() == prev.decl_hashes.len()
            && self
                .decl_hashes
                .iter()
                .all(|(k, h)| prev.decl_hashes.get(k) == Some(h))
    }
}

/// The top-level record fields of the program — one per declaration.
/// The file is a single expression; declarations are the fields of its
/// top-level record literal (or `with` record). Returns `&RecordField`s so
/// positional indexing (for synthetic keys) is stable across a single pass.
fn top_fields(program: &Expr) -> Vec<&ast::RecordField> {
    match &program.node {
        ExprKind::Record(fields) => fields.iter().collect(),
        ExprKind::With { record, .. } => match &record.node {
            ExprKind::Record(fields) => fields.iter().collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Build a stable key for a declaration field. Named decls use their name;
/// marker decls that lack a meaningful name fall back to a positional key
/// combined with their shape so reordering doesn't alias.
fn field_key(field: &ast::RecordField, index: usize) -> String {
    match &field.value.node {
        ExprKind::SubsetConstraint { .. } => format!("__subset#{index}"),
        ExprKind::RouteCompositeDecl { name, .. } => format!("__route_comp:{name}"),
        _ => field.name.clone(),
    }
}

/// Hash a declaration field ignoring its source spans. We rely on
/// `Debug`-printed form as a stable structural representation; this keeps
/// the impl simple and avoids hand-writing a deep-walk hasher for every node
/// kind. The hash is keyed off the AST shape only — formatting and span
/// shifts in otherwise-identical decls produce the same hash.
fn hash_field(field: &ast::RecordField) -> u64 {
    let mut h = DefaultHasher::new();
    field.name.hash(&mut h);
    // Debug format includes spans, so strip them from the rendered string
    // before hashing. Spans look like `Span { start: 12, end: 34 }`.
    let raw = format!("{:?}", field.value.node);
    let stripped = strip_spans(&raw);
    stripped.hash(&mut h);
    if let Some(sig) = &field.sig {
        strip_spans(&format!("{:?}", sig.ty.node)).hash(&mut h);
    }
    h.finish()
}

/// Hash only the externally-visible signature of a declaration field. Used
/// by `signature_changed_decls` to detect whether a downstream consumer
/// needs re-checking. The rule: if a Knot user observes only the `name :
/// Type` line of a decl, what changes here?
///
/// - Field with an explicit signature: hash the name + signature only —
///   body is internal to this decl.
/// - Field without signature: must include the body (its inferred type
///   depends on the body, and dependents see the inferred type).
/// - Source: hash the type annotation.
/// - Data/Route/etc.: shape is the signature — hash the whole marker.
fn hash_field_signature(field: &ast::RecordField) -> u64 {
    let mut h = DefaultHasher::new();
    match &field.value.node {
        ExprKind::SourceDecl { name, ty, .. } => {
            ("source_sig", name).hash(&mut h);
            strip_spans(&format!("{:?}", ty.node)).hash(&mut h);
        }
        ExprKind::ViewDecl { name, ty, body }
        | ExprKind::DerivedDecl { name, ty, body } => {
            ("vd_sig", name).hash(&mut h);
            match ty {
                Some(ts) => {
                    strip_spans(&format!("{:?}", ts.ty.node)).hash(&mut h);
                    strip_spans(&format!("{:?}", ts.constraints)).hash(&mut h);
                }
                None => {
                    "untyped".hash(&mut h);
                    strip_spans(&format!("{:?}", body.node)).hash(&mut h);
                }
            }
        }
        // Data / route / etc.: shape *is* the signature — full hash.
        ExprKind::DataCtor { .. } | ExprKind::TypeCtor { .. }
        | ExprKind::RouteDecl { .. } | ExprKind::RouteCompositeDecl { .. }
        | ExprKind::SubsetConstraint { .. } => return hash_field(field),
        _ => {
            // A named function field: hash the sig when present, else the body.
            ("fun_sig", &field.name).hash(&mut h);
            match &field.sig {
                Some(ts) => {
                    strip_spans(&format!("{:?}", ts.ty.node)).hash(&mut h);
                    strip_spans(&format!("{:?}", ts.constraints)).hash(&mut h);
                }
                None => {
                    "untyped".hash(&mut h);
                    strip_spans(&format!("{:?}", field.value.node)).hash(&mut h);
                }
            }
        }
    }
    h.finish()
}

/// Hash program-level structure: the *signature* of each decl (no bodies).
/// When this changes, the trait surface shifted and even "clean-bodied"
/// decls may need re-checking.
fn hash_structure(program: &Expr) -> u64 {
    let mut h = DefaultHasher::new();
    for field in top_fields(program) {
        match &field.value.node {
            ExprKind::SourceDecl { name, ty, .. } => {
                ("source", name).hash(&mut h);
                strip_spans(&format!("{:?}", ty.node)).hash(&mut h);
            }
            ExprKind::ViewDecl { name, ty, .. } | ExprKind::DerivedDecl { name, ty, .. } => {
                ("vd", name).hash(&mut h);
                if let Some(ts) = ty {
                    strip_spans(&format!("{:?}", ts.ty.node)).hash(&mut h);
                }
            }
            ExprKind::DataCtor { name, constructors, .. } => {
                ("data", name).hash(&mut h);
                for c in constructors {
                    c.name.hash(&mut h);
                    for f in &c.fields {
                        f.name.hash(&mut h);
                        strip_spans(&format!("{:?}", f.value.node)).hash(&mut h);
                    }
                }
            }
            ExprKind::TypeCtor { name, .. } => {
                ("alias", name).hash(&mut h);
                strip_spans(&format!("{:?}", field.value.node)).hash(&mut h);
            }
            ExprKind::RouteDecl { name, .. } => {
                ("route", name).hash(&mut h);
                strip_spans(&format!("{:?}", field.value.node)).hash(&mut h);
            }
            other => {
                ("fun", &field.name).hash(&mut h);
                if let Some(ts) = &field.sig {
                    strip_spans(&format!("{:?}", ts.ty.node)).hash(&mut h);
                } else {
                    strip_spans(&format!("{:?}", other)).hash(&mut h);
                }
            }
        }
    }
    h.finish()
}

/// Collect every top-level name a declaration field references, including
/// type-level dependencies (type annotations, source types, constraints,
/// data constructors). Used to build the reverse-dependency graph for
/// selective re-inference.
fn collect_field_deps(field: &ast::RecordField) -> HashSet<String> {
    let mut deps = HashSet::new();
    // Signature types are deps for any field that carries one.
    if let Some(ts) = &field.sig {
        collect_type_names(&ts.ty, &mut deps);
        for c in &ts.constraints {
            for arg in c.types() {
                collect_type_names(arg, &mut deps);
            }
        }
    }
    match &field.value.node {
        ExprKind::SourceDecl { ty, .. } => {
            collect_type_names(ty, &mut deps);
        }
        ExprKind::ViewDecl { ty, body, .. } | ExprKind::DerivedDecl { ty, body, .. } => {
            if let Some(ts) = ty {
                collect_type_names(&ts.ty, &mut deps);
                for c in &ts.constraints {
                    for arg in c.types() {
                        collect_type_names(arg, &mut deps);
                    }
                }
            }
            collect_expr_names(body, &mut deps);
        }
        ExprKind::TypeCtor { ty, .. } => {
            collect_type_names(ty, &mut deps);
        }
        ExprKind::DataCtor { constructors, .. } => {
            for ctor in constructors {
                for f in &ctor.fields {
                    collect_type_names(&f.value, &mut deps);
                }
            }
        }
        ExprKind::RouteDecl { name, entries } => {
            deps.insert(name.clone());
            for entry in entries {
                for seg in &entry.path {
                    if let ast::PathSegment::Param { ty, .. } = seg {
                        collect_type_names(ty, &mut deps);
                    }
                }
                for f in &entry.body_fields {
                    collect_type_names(&f.value, &mut deps);
                }
                for f in &entry.query_params {
                    collect_type_names(&f.value, &mut deps);
                }
                for f in &entry.request_headers {
                    collect_type_names(&f.value, &mut deps);
                }
                for f in &entry.response_headers {
                    collect_type_names(&f.value, &mut deps);
                }
                if let Some(ty) = &entry.response_ty {
                    collect_type_names(ty, &mut deps);
                }
                if let Some(rate_limit) = &entry.rate_limit {
                    collect_expr_names(rate_limit, &mut deps);
                }
            }
        }
        ExprKind::RouteCompositeDecl { name, components } => {
            deps.insert(name.clone());
            for comp in components {
                deps.insert(comp.clone());
            }
        }
        ExprKind::SubsetConstraint { sub, sup } => {
            deps.insert(sub.relation.clone());
            if let Some(f) = &sub.field {
                deps.insert(f.clone());
            }
            deps.insert(sup.relation.clone());
            if let Some(f) = &sup.field {
                deps.insert(f.clone());
            }
        }
        _ => {
            // A plain value/function field: body deps.
            collect_expr_names(&field.value, &mut deps);
        }
    }
    deps
}

fn utf8_char_len(first: u8) -> usize {
    match first {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        _ => 1, // stray continuation byte: advance one to stay live
    }
}


fn span_marker_len(b: &[u8]) -> Option<usize> {
    let lit_at = |i: usize, lit: &[u8]| -> Option<usize> {
        if b.len() >= i + lit.len() && b[i..i + lit.len()] == *lit {
            Some(i + lit.len())
        } else {
            None
        }
    };
    let digits_at = |i: usize| -> Option<usize> {
        let mut j = i;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        if j > i { Some(j) } else { None }
    };
    let i = lit_at(0, b"Span { start: ".as_slice())?;
    let i = digits_at(i)?;
    let i = lit_at(i, b", end: ".as_slice())?;
    let i = digits_at(i)?;
    lit_at(i, b" }".as_slice())
}


fn strip_spans(s: &str) -> String {
    // Walk the string, dropping only *complete* span markers. Matching the
    // full `Span { start: N, end: N }` shape (rather than a bare `Span {`
    // prefix + seek-to-`}`) is required for correctness: a string/int literal
    // whose Debug rendering contains the text `Span {` has no `}` of its own,
    // so a naive seek would swallow the following real span's `}` and delete
    // the part of the AST that actually changed — making two different decl
    // versions hash equal and leaving dependents with stale diagnostics.
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    // Track whether we're inside a string-literal's Debug rendering. A user
    // string whose contents happen to spell `Span { start: N, end: N }` must
    // NOT be scrubbed — otherwise edits confined to those digits would hash
    // identically, a false negative in change detection. Derived `Debug`
    // always quotes string contents and escapes inner quotes as `\"`, so a
    // simple in-string flag (with backslash escape handling) separates real
    // span markers (rendered outside quotes) from look-alike string contents.
    let mut in_string = false;
    while i < bytes.len() {
        if in_string {
            // Inside a string: copy verbatim, honoring `\`-escapes (so an
            // escaped `\"` does not prematurely close the string), and exit
            // on the closing unescaped quote.
            let step = utf8_char_len(bytes[i]);
            let end = (i + step).min(bytes.len());
            out.push_str(&s[i..end]);
            if bytes[i] == b'\\' {
                // Copy the escaped char too, so it can't be misread as a quote.
                if end < bytes.len() {
                    let step2 = utf8_char_len(bytes[end]);
                    let end2 = (end + step2).min(bytes.len());
                    out.push_str(&s[end..end2]);
                    i = end2;
                    continue;
                }
            } else if bytes[i] == b'"' {
                in_string = false;
            }
            i = end;
            continue;
        }
        if let Some(len) = span_marker_len(&bytes[i..]) {
            i += len;
            continue;
        }
        // Copy one whole UTF-8 character verbatim. `i` is always on a char
        // boundary: it starts at 0 and advances either by a full char width
        // here or past an all-ASCII span marker above.
        let step = utf8_char_len(bytes[i]);
        let end = (i + step).min(bytes.len());
        out.push_str(&s[i..end]);
        if bytes[i] == b'"' {
            in_string = true;
        }
        i = end;
    }
    out
}


/// Collect named type references from a type AST node (or a slice of them).
fn collect_type_names(ty: &ast::Type, out: &mut HashSet<String>) {
    match &ty.node {
        ast::TypeKind::Named(name) => {
            out.insert(name.clone());
        }
        ast::TypeKind::Var(_) | ast::TypeKind::Hole => {}
        ast::TypeKind::App { func, arg } => {
            collect_type_names(func, out);
            collect_type_names(arg, out);
        }
        ast::TypeKind::Record { fields, .. } => {
            for f in fields {
                collect_type_names(&f.value, out);
            }
        }
        ast::TypeKind::Relation(inner) => collect_type_names(inner, out),
        ast::TypeKind::Function { param, result } => {
            collect_type_names(param, out);
            collect_type_names(result, out);
        }
        ast::TypeKind::Variant { constructors, .. } => {
            for ctor in constructors {
                for field in &ctor.fields {
                    collect_type_names(&field.value, out);
                }
            }
        }
        ast::TypeKind::Effectful { ty, .. } => collect_type_names(ty, out),
        ast::TypeKind::IO { ty, .. } => collect_type_names(ty, out),
        ast::TypeKind::Unit(_) => {},
        ast::TypeKind::UnitAnnotated { base, .. } => collect_type_names(base, out),
        ast::TypeKind::Refined { base, .. } => collect_type_names(base, out),
        ast::TypeKind::Forall { ty, .. } => collect_type_names(ty, out),
    }
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
        ast::ExprKind::Serve { api, .. } => {
            out.insert(api.clone());
        }
        _ => {}
    }
    crate::utils::recurse_expr(expr, |e| collect_expr_names(e, out));
}

