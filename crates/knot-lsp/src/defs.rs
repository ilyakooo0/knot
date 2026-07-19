//! Definition resolution: scope-aware AST walk that produces (1) span-keyed
//! references for goto/find-references, (2) name-keyed top-level definitions
//! as a fallback, and (3) literal-type info for hover.
//!
//! Also lives here: `build_details`, which formats per-declaration "summary"
//! strings used as completion details and hover headlines.

use std::collections::{HashMap, HashSet};

use knot::ast::{self, DeclKind, Module, Span, Type, TypeKind};

use crate::type_format::{format_type_kind, format_type_scheme};
use crate::utils::{find_word_after_eq, find_word_in_source};

/// Given a byte offset just after a constructor's name token, return the
/// offset just past that constructor's brace-balanced `{…}` field block, or
/// `from` unchanged if no field block precedes `end`. Constructors in Knot
/// always carry a `{…}` block (even nullary ones are written `C {}`), so this
/// lets the constructor search advance past field types — keeping a type name
/// reused in one constructor's fields from being mistaken for a later
/// constructor's definition token.
fn advance_past_field_block(source: &str, from: usize, end: usize) -> usize {
    let bytes = source.as_bytes();
    let end = end.min(bytes.len());
    let mut i = from;
    // Find the opening brace of the field block. Bail if we reach the next
    // constructor delimiter first (defensive — should not happen given the
    // grammar always emits `{…}`).
    while i < end && bytes[i] != b'{' {
        if bytes[i] == b'|' {
            return from;
        }
        i += 1;
    }
    if i >= end {
        return from;
    }
    let mut depth = 0usize;
    while i < end {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return i + 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    from
}

/// Canonical definition span of trait method `method` as declared by the trait
/// named `trait_name` in this module, if any. Mirrors the scope-0 registration
/// in `resolve_definitions` (the `Trait` arm), which anchors a method on its
/// *first* `TraitItem::Method` entry — the signature token for a defaulted
/// method. Used to link an `impl`'s method token to the trait method it
/// implements. Resolves precisely through `trait_name` rather than the
/// name-keyed scope map, which can't disambiguate two traits that declare a
/// method of the same name.
fn trait_method_def_span(module: &Module, trait_name: &str, method: &str) -> Option<Span> {
    module.decls.iter().find_map(|d| match &d.node {
        DeclKind::Trait { name, items, .. } if name == trait_name => {
            items.iter().find_map(|it| match it {
                ast::TraitItem::Method { name: m, name_span, .. } if m == method => Some(*name_span),
                _ => None,
            })
        }
        _ => None,
    })
}

/// Definition resolution result: name → def span, (use span, def span)
/// references, and (literal span, type name) pairs.
type Definitions = (HashMap<String, Span>, Vec<(Span, Span)>, Vec<(Span, String)>);

/// Resolve definitions: returns (name_map, span_references, literal_types).
pub fn resolve_definitions(module: &Module, source: &str) -> Definitions {
    let mut resolver = DefResolver {
        source,
        scopes: vec![HashMap::new()],
        refs: Vec::new(),
        literals: Vec::new(),
    };

    // Phase 1: register all top-level declarations
    for decl in &module.decls {
        let name_span = |name: &str| {
            find_word_in_source(source, name, decl.span.start, decl.span.end)
                .unwrap_or(decl.span)
        };
        match &decl.node {
            DeclKind::Data {
                name, constructors, ..
            } => {
                resolver.define(name, name_span(name));
                // Start the search after the `=` so a self-named constructor
                // (`data Circle = Circle {…}`) anchors on the constructor
                // token, not the type name before the `=`. Advance past each
                // hit so a name reused in an earlier constructor's field types
                // can't steal a later constructor's span. Mirrors
                // document_symbol.rs / semantic_tokens.rs.
                let mut search_from = source
                    .get(decl.span.start..decl.span.end.min(source.len()))
                    .and_then(|t| t.find('='))
                    .map(|p| decl.span.start + p + 1)
                    .unwrap_or(decl.span.start);
                for ctor in constructors {
                    let ctor_span =
                        find_word_in_source(source, &ctor.name, search_from, decl.span.end)
                            .unwrap_or(decl.span);
                    resolver.define(&ctor.name, ctor_span);
                    // Skip past this constructor's `{…}` field block so a type
                    // name reused inside the fields (`data T = A {x: B} | B {}`)
                    // can't be mistaken for the next constructor's token.
                    search_from =
                        advance_past_field_block(source, ctor_span.end, decl.span.end);
                }
            }
            DeclKind::TypeAlias { name, .. } => {
                resolver.define(name, name_span(name));
            }
            DeclKind::Source { name, .. } | DeclKind::View { name, .. } => {
                let span = name_span(name);
                resolver.define(name, span);
                resolver.register_extra_definition_tokens(decl.span, name, span);
            }
            DeclKind::Derived { name, .. } => {
                let span = name_span(name);
                resolver.define(name, span);
                resolver.register_extra_definition_tokens(decl.span, name, span);
            }
            DeclKind::Fun { name, .. } => {
                let span = name_span(name);
                resolver.define(name, span);
                // The parser merges a separate type signature and the body
                // line (`f : T` ⏎ `f = body`) into ONE `DeclKind::Fun`
                // spanning both lines. `name_span` finds only the FIRST
                // whole-word occurrence (the signature line), so the
                // body-line definition token would otherwise be invisible to
                // rename/references/highlight — register every additional
                // line-start occurrence as a self-reference to the canonical
                // definition span.
                resolver.register_extra_definition_tokens(decl.span, name, span);
            }
            DeclKind::Trait { name, items, .. } => {
                resolver.define(name, name_span(name));
                // Methods live after `where`; searching from the trait header
                // (`name_span` starts at `decl.span.start`) lets a method name
                // collide with the trait name, a supertrait, or a type
                // parameter (`trait T a where  a : a -> Int` anchors method `a`
                // on the header's `a`). Start the search past `where` so each
                // method resolves to its own signature token. Mirrors the Data
                // arm's `=`-anchored search above. Use a whole-word search for
                // the keyword so a trait/supertrait/param name that merely
                // *contains* the substring `where` (e.g. `Nowhere`) doesn't
                // anchor the search before the real keyword.
                // Each method carries an authoritative `name_span` pointing at
                // its own signature token (see `ast::TraitItem::Method`). Use it
                // directly — a non-advancing text search anchored past `where`
                // mis-resolves a method to an *earlier* method's default-body
                // reference of the same name (e.g. `eq` calling `neq` before
                // `neq`'s own signature appears). Mirrors document_symbol.rs.
                // A defaulted method is parsed as TWO `TraitItem::Method`
                // entries — the signature (`foo : T`, `default_body: None`) and
                // the default body (`foo = …`, `default_body: Some`), each with
                // its own `name_span`. Registering both via `define` keeps only
                // the last (the body), leaving the signature token invisible to
                // rename/references/highlight and corrupting the trait on
                // rename. Define the first occurrence of each method name as
                // canonical and cross-link any later same-name token as a
                // self-reference (mirrors the `Fun` arm — but per-method, so we
                // don't capture a *reference* from another method's default body
                // of the same name, e.g. `eq` calling `neq`).
                let mut method_defs: HashSet<&str> = HashSet::new();
                for item in items {
                    if let ast::TraitItem::Method { name, name_span, .. } = item {
                        if method_defs.insert(name.as_str()) {
                            resolver.define(name, *name_span);
                        } else {
                            resolver.add_ref(*name_span, name);
                        }
                    }
                }
            }
            DeclKind::Route { name, entries, .. } => {
                resolver.define(name, name_span(name));
                // Each endpoint's constructor (`… -> Response = GetUsers`) is a
                // definition referenced from `serve`/`fetch`. It's spanless, so
                // recover its `= Ctor` token and register it so goto and
                // find-references from those sites reach the route declaration.
                let mut cursor = decl.span.start;
                for entry in entries {
                    if let Some(span) =
                        find_word_after_eq(source, &entry.constructor, cursor, decl.span.end)
                    {
                        cursor = span.end;
                        resolver.define(&entry.constructor, span);
                    }
                }
            }
            DeclKind::RouteComposite { name, .. } => {
                resolver.define(name, name_span(name));
            }
            _ => {}
        }
    }

    // Phase 2: walk declaration bodies to resolve references
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { body, ty, .. } => {
                if let Some(scheme) = ty {
                    // `Show a =>` constraint trait names precede the type; they
                    // are spanless, so recover them before walking the type.
                    resolver.resolve_trait_names(
                        scheme.constraints.iter().map(|c| c.trait_name.as_str()),
                        decl.span.start,
                        scheme.ty.span.start,
                    );
                    resolver.resolve_type(&scheme.ty, source);
                    for c in &scheme.constraints {
                        for arg in &c.args {
                            resolver.resolve_type(arg, source);
                        }
                    }
                }
                if let Some(body) = body {
                    resolver.resolve_expr(body);
                }
            }
            DeclKind::View { body, ty, .. } | DeclKind::Derived { body, ty, .. } => {
                if let Some(scheme) = ty {
                    resolver.resolve_trait_names(
                        scheme.constraints.iter().map(|c| c.trait_name.as_str()),
                        decl.span.start,
                        scheme.ty.span.start,
                    );
                    resolver.resolve_type(&scheme.ty, source);
                    for c in &scheme.constraints {
                        for arg in &c.args {
                            resolver.resolve_type(arg, source);
                        }
                    }
                }
                resolver.resolve_expr(body);
            }
            DeclKind::Source { ty, .. } => {
                resolver.resolve_type(ty, source);
            }
            DeclKind::TypeAlias { ty, .. } => {
                resolver.resolve_type(ty, source);
            }
            DeclKind::Data { constructors, .. } => {
                for ctor in constructors {
                    for f in &ctor.fields {
                        resolver.resolve_type(&f.value, source);
                    }
                }
            }
            DeclKind::Impl { trait_name, args, items, constraints, .. } => {
                // The impl head is `impl (Constraint =>)* TraitName args*`, so
                // every trait-name token (the constraints' trait names then the
                // impl's own) precedes the first arg. Recover them in order so
                // goto/find-references/rename reach `impl Show …` and any
                // `Show a =>` bound — otherwise renaming the trait leaves them
                // stale.
                let head_end = args.first().map(|a| a.span.start).unwrap_or(decl.span.end);
                resolver.resolve_trait_names(
                    constraints
                        .iter()
                        .map(|c| c.trait_name.as_str())
                        .chain(std::iter::once(trait_name.as_str())),
                    decl.span.start,
                    head_end,
                );
                for arg in args {
                    resolver.resolve_type(arg, source);
                }
                for c in constraints {
                    for arg in &c.args {
                        resolver.resolve_type(arg, source);
                    }
                }
                for item in items {
                    match item {
                        ast::ImplItem::Method { name, name_span, params, body } => {
                            // Link the method's definition token to the trait
                            // method it implements, so find-references/goto from
                            // the trait method reach every impl in this file
                            // (rename.rs already does this out-of-band; without
                            // the link, references.rs' local path — which relies
                            // solely on these refs — misses impl definitions).
                            // Resolve precisely through this impl's `trait_name`
                            // rather than the ambiguous scope-0 name map.
                            if let Some(def_span) =
                                trait_method_def_span(module, trait_name, name)
                            {
                                resolver.refs.push((*name_span, def_span));
                            }
                            resolver.push_scope();
                            for p in params {
                                resolver.define_pat(p);
                            }
                            resolver.resolve_expr(body);
                            resolver.pop_scope();
                        }
                        ast::ImplItem::AssociatedType { args, ty, .. } => {
                            // `type Item [a] = SomeType` — the argument and
                            // definition types name real types; resolve them so
                            // goto/find-references reach those definitions
                            // (mirrors the rename walker).
                            for arg in args {
                                resolver.resolve_type(arg, source);
                            }
                            resolver.resolve_type(ty, source);
                        }
                    }
                }
            }
            DeclKind::Trait { items, supertraits, .. } => {
                // `trait T a : Super1, Super2` — each supertrait's trait name is
                // a reference to that trait's definition.
                resolver.resolve_trait_names(
                    supertraits.iter().map(|c| c.trait_name.as_str()),
                    decl.span.start,
                    decl.span.end,
                );
                for c in supertraits {
                    for arg in &c.args {
                        resolver.resolve_type(arg, source);
                    }
                }
                for item in items {
                    if let ast::TraitItem::Method {
                        name_span,
                        default_params,
                        default_body,
                        ty,
                        ..
                    } = item
                    {
                        // A method's own `Trait a =>` constraint trait names
                        // (`eq : Show a => Bool`) are spanless — recover them
                        // between the method name and its type, so goto/rename
                        // reach the constrained trait (mirrors the `Fun` arm).
                        resolver.resolve_trait_names(
                            ty.constraints.iter().map(|c| c.trait_name.as_str()),
                            name_span.end,
                            ty.ty.span.start,
                        );
                        resolver.resolve_type(&ty.ty, source);
                        for c in &ty.constraints {
                            for arg in &c.args {
                                resolver.resolve_type(arg, source);
                            }
                        }
                        if let Some(body) = default_body {
                            resolver.push_scope();
                            for p in default_params {
                                resolver.define_pat(p);
                            }
                            resolver.resolve_expr(body);
                            resolver.pop_scope();
                        }
                    }
                }
            }
            DeclKind::Migrate { relation, from_ty, to_ty, using_fn, .. } => {
                // `migrate *rel from … to … using …` — the migrated relation is
                // a reference to its source declaration. `relation` is spanless
                // (the bare name, no `*`), so recover its token from the decl
                // source; the `*` sigil is a word boundary for the search.
                if let Some(span) =
                    find_word_in_source(source, relation, decl.span.start, decl.span.end)
                {
                    resolver.add_ref(span, relation);
                }
                resolver.resolve_type(from_ty, source);
                resolver.resolve_type(to_ty, source);
                resolver.resolve_expr(using_fn);
            }
            DeclKind::Route { entries, .. } => {
                for entry in entries {
                    for f in &entry.body_fields {
                        resolver.resolve_type(&f.value, source);
                    }
                    for f in &entry.query_params {
                        resolver.resolve_type(&f.value, source);
                    }
                    for f in &entry.request_headers {
                        resolver.resolve_type(&f.value, source);
                    }
                    for f in &entry.response_headers {
                        resolver.resolve_type(&f.value, source);
                    }
                    if let Some(resp) = &entry.response_ty {
                        resolver.resolve_type(resp, source);
                    }
                    for seg in &entry.path {
                        if let ast::PathSegment::Param { ty, .. } = seg {
                            resolver.resolve_type(ty, source);
                        }
                    }
                    // The `rateLimit <expr>` clause is a real user-edited
                    // expression (e.g. `{key: keyByIp, ...}`); resolve names
                    // used inside it so goto/find-references reach them.
                    if let Some(rl) = &entry.rate_limit {
                        resolver.resolve_expr(rl);
                    }
                }
            }
            DeclKind::RouteComposite { components, .. } => {
                // `route Api = A | B` — each component names another route.
                // Register each as a reference so goto/rename/highlight reach
                // the composed routes. Start after `=` so the composite's own
                // name token isn't mistaken for a component, and advance the
                // cursor so repeated names each resolve to their own span.
                let mut search_from = source
                    .get(decl.span.start..decl.span.end.min(source.len()))
                    .and_then(|t| t.find('='))
                    .map(|p| decl.span.start + p + 1)
                    .unwrap_or(decl.span.start);
                for comp in components {
                    if let Some(span) =
                        find_word_in_source(source, comp, search_from, decl.span.end)
                    {
                        search_from = span.end;
                        resolver.add_ref(span, comp);
                    }
                }
            }
            DeclKind::SubsetConstraint { sub, sup } => {
                // `*orders.customer <= *people.name` references source
                // relations by name. `RelationPath` is spanless, so recover
                // each relation-name token from the decl source (the `*` sigil
                // is a word boundary) and register it so goto/find-references/
                // rename reach the referenced sources. A moving cursor lets
                // both sides — including `*users <= *users.email` — resolve.
                let mut search_from = decl.span.start;
                for rel in [&sub.relation, &sup.relation] {
                    if let Some(span) =
                        find_word_in_source(source, rel, search_from, decl.span.end)
                    {
                        search_from = span.end;
                        resolver.add_ref(span, rel);
                    }
                }
            }
        }
    }

    let name_map = resolver.scopes[0].clone();
    (name_map, resolver.refs, resolver.literals)
}

struct DefResolver<'a> {
    source: &'a str,
    scopes: Vec<HashMap<String, Span>>,
    refs: Vec<(Span, Span)>,
    literals: Vec<(Span, String)>,
}

impl<'a> DefResolver<'a> {
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

    /// Register goto/find-references for a textually-ordered sequence of
    /// *spanless* trait-name tokens occurring within `[from, to)` — the trait
    /// applied by an `impl`, a supertrait, or a `Trait a =>` constraint. Each
    /// name is located in source order with a moving cursor (searching for the
    /// token's own text, not a shared needle) so distinct trait names in one
    /// clause each anchor on their own token. `add_ref` no-ops for names not
    /// defined in this module, so an out-of-module trait simply records nothing.
    fn resolve_trait_names<'b>(
        &mut self,
        names: impl Iterator<Item = &'b str>,
        from: usize,
        to: usize,
    ) {
        let mut cursor = from;
        for tn in names {
            if let Some(span) = find_word_in_source(self.source, tn, cursor, to) {
                cursor = span.end;
                self.add_ref(span, tn);
            }
        }
    }

    /// Register every *additional* definition-name token of a top-level
    /// declaration as a self-reference to its canonical (first) name span.
    ///
    /// Multi-line declarations repeat their name at the start of each
    /// defining line — most importantly `f : T` ⏎ `f = body`, which the
    /// parser merges into a single decl. Top-level decls start at column 0,
    /// so any line inside the decl span that begins with the name (after an
    /// optional `*`/`&` relation sigil) is a definition token, never a body
    /// expression (body lines are layout-indented).
    fn register_extra_definition_tokens(&mut self, decl_span: Span, name: &str, primary: Span) {
        let end = decl_span.end.min(self.source.len());
        let start = decl_span.start.min(end);
        let text = &self.source[start..end];
        let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'\'';
        let mut rel = 0usize;
        for line in text.split('\n') {
            // Optional relation sigil before the name (views/derived).
            let cand = if line.starts_with('*') || line.starts_with('&') {
                1
            } else {
                0
            };
            let rest = &line[cand.min(line.len())..];
            if rest.starts_with(name) {
                let tok_end = cand + name.len();
                let boundary_ok = line
                    .as_bytes()
                    .get(tok_end)
                    .is_none_or(|b| !is_ident(*b));
                if boundary_ok {
                    let span = Span::new(start + rel + cand, start + rel + tok_end);
                    if span != primary {
                        self.refs.push((span, primary));
                    }
                }
            }
            rel += line.len() + 1;
        }
    }

    /// Walk a type expression, registering goto-references for each named
    /// type. The recorded usage span is just the name (not the surrounding
    /// type construction), so the goto-on-cursor lookup matches identifier
    /// boundaries the way users expect.
    fn resolve_type(&mut self, ty: &Type, source: &str) {
        match &ty.node {
            TypeKind::Named(name) => {
                let span = find_word_in_source(source, name, ty.span.start, ty.span.end)
                    .unwrap_or(ty.span);
                self.add_ref(span, name);
            }
            TypeKind::Var(_) | TypeKind::Hole => {}
            TypeKind::App { func, arg } => {
                self.resolve_type(func, source);
                self.resolve_type(arg, source);
            }
            TypeKind::Record { fields, .. } => {
                for f in fields {
                    self.resolve_type(&f.value, source);
                }
            }
            TypeKind::Relation(inner) => self.resolve_type(inner, source),
            TypeKind::Function { param, result } => {
                self.resolve_type(param, source);
                self.resolve_type(result, source);
            }
            TypeKind::Variant { constructors, .. } => {
                for ctor in constructors {
                    for f in &ctor.fields {
                        self.resolve_type(&f.value, source);
                    }
                }
            }
            TypeKind::Effectful { ty, .. } => self.resolve_type(ty, source),
            TypeKind::IO { ty, .. } => self.resolve_type(ty, source),
            TypeKind::Unit(_) => {},
            TypeKind::UnitAnnotated { base, .. } => self.resolve_type(base, source),
            TypeKind::Refined { base, predicate } => {
                self.resolve_type(base, source);
                self.resolve_expr(predicate);
            }
            TypeKind::Forall { ty, .. } => self.resolve_type(ty, source),
        }
    }

    fn define_pat(&mut self, pat: &ast::Pat) {
        match &pat.node {
            ast::PatKind::Var(name) => {
                self.define(name, pat.span);
                // Record the binder token itself as a self-reference so
                // position-based resolution (rename/references/highlight)
                // finds the local symbol when the cursor sits on the binder
                // — local binders have no entry in the top-level name map.
                self.refs.push((pat.span, pat.span));
            }
            ast::PatKind::Constructor { name, payload } => {
                // The reference must cover only the constructor name, not the
                // payload — rename replaces usage spans verbatim, so a
                // whole-pattern span would delete the payload binder. The name
                // does NOT always lead the pattern span: a parenthesized
                // pattern (`(Circle c)`, the normal form for destructuring in
                // a lambda/case) rewrites the span to start at `(`, so
                // `start + name.len()` would cover `(Circl` instead of the
                // name. Locate the actual token via word search, falling back
                // to the leading-name form only for unparenthesized patterns.
                // (Mirrors `rename.rs::walk_pat_ctors`.)
                let name_span = find_word_in_source(self.source, name, pat.span.start, pat.span.end)
                    .unwrap_or_else(|| Span::new(pat.span.start, pat.span.start + name.len()));
                self.add_ref(name_span, name);
                self.define_pat(payload);
            }
            ast::PatKind::Record(fields) => {
                // Fields appear sequentially in source order; a running
                // cursor keeps each pun-token search confined to its slot.
                let mut search_start = pat.span.start;
                for f in fields {
                    if let Some(p) = &f.pattern {
                        self.define_pat(p);
                        search_start = p.span.end;
                    } else {
                        let span = find_word_in_source(
                            self.source,
                            &f.name,
                            search_start,
                            pat.span.end,
                        )
                        .unwrap_or(pat.span);
                        self.define(&f.name, span);
                        // Self-reference for the pun binder token (see
                        // `PatKind::Var` above).
                        self.refs.push((span, span));
                        search_start = span.end;
                    }
                }
            }
            ast::PatKind::List(pats) => {
                for p in pats {
                    self.define_pat(p);
                }
            }
            ast::PatKind::Cons { head, tail } => {
                self.define_pat(head);
                self.define_pat(tail);
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

            ast::ExprKind::With { record, body } => {
                self.resolve_expr(record);
                self.resolve_expr(body);
            }

            ast::ExprKind::Do(stmts) => {
                self.push_scope();
                for stmt in stmts {
                    match &stmt.node {
                        ast::StmtKind::Bind { pat, expr } => {
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
            ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
                self.resolve_expr(target);
                self.resolve_expr(value);
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
            ast::ExprKind::TimeUnitLit { value, .. } => self.resolve_expr(value),
            ast::ExprKind::Annot { expr: inner, ty } => {
                self.resolve_type(ty, self.source);
                self.resolve_expr(inner);
            }
            ast::ExprKind::Refine(inner) => self.resolve_expr(inner),
            ast::ExprKind::Serve { api, api_span, handlers } => {
                self.add_ref(*api_span, api);
                for h in handlers {
                    // Each endpoint token references the route endpoint
                    // constructor it handles. Mirror the rename walker so
                    // goto/find-references/highlight see it too (a no-op until
                    // endpoint constructors are registered as definitions, but
                    // keeps the two walkers symmetric so navigation and rename
                    // never diverge on these tokens).
                    self.add_ref(h.endpoint_span, &h.endpoint);
                    self.resolve_expr(&h.body);
                }
            }
        }
    }
}

pub fn build_details(module: &Module) -> HashMap<String, String> {
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
            DeclKind::Source { name, ty } => {
                details.insert(
                    name.clone(),
                    format!("*{name} : [{}]", format_type_kind(&ty.node)),
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
            DeclKind::Impl { trait_name, args, .. } => {
                let args_str = args.iter().map(|a| format_type_kind(&a.node)).collect::<Vec<_>>().join(" ");
                details.insert(format!("{trait_name}@{args_str}"), format!("impl {trait_name} {args_str}"));
            }
            DeclKind::Route { name, .. } => {
                details.insert(name.clone(), format!("route {name}"));
            }
            DeclKind::RouteComposite { name, components, .. } => {
                details.insert(name.clone(), format!("route {name} = {}", components.join(" | ")));
            }
            _ => {}
        }
    }

    details
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(source: &str) -> Module {
        let lexer = knot::lexer::Lexer::new(source);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(source.to_string(), tokens);
        let (module, _) = parser.parse_module();
        module
    }

    #[test]
    fn trait_method_def_anchors_on_signature_not_header() {
        // A trait method whose name collides with a token in the trait
        // header (here the type parameter `a`) must resolve to its own
        // signature line, not the header occurrence — otherwise goto/rename
        // on calls of the method jump to / edit the type parameter.
        let source = "trait T a where\n  a : a -> Int 1\n";
        let module = parse(source);
        let (defs, _, _) = resolve_definitions(&module, source);
        let span = defs.get("a").expect("method `a` is defined");
        // The header `a` is on line 0; the method signature is on line 1.
        assert!(
            span.start > source.find('\n').unwrap(),
            "method def should anchor after the header line, got {span:?}"
        );
        assert_eq!(&source[span.start..span.end], "a");
    }

    /// True if some recorded reference's usage span is exactly `text` and
    /// points at the definition of `def_name`.
    fn has_ref_to(defs: &Definitions, source: &str, text: &str, def_name: &str) -> bool {
        let (name_map, refs, _) = defs;
        let Some(def) = name_map.get(def_name) else {
            return false;
        };
        refs.iter().any(|(usage, d)| {
            d == def && source.get(usage.start..usage.end) == Some(text)
        })
    }

    #[test]
    fn impl_trait_name_is_recorded_as_reference() {
        // `impl Show Foo` must record its `Show` token as a reference to the
        // trait definition, so goto/find-references reach it.
        let source =
            "trait Show a where\n  present : a -> Text\ndata Foo = Foo {}\nimpl Show Foo where\n  present = \\x -> \"foo\"\n";
        let module = parse(source);
        let defs = resolve_definitions(&module, source);
        assert!(
            has_ref_to(&defs, source, "Show", "Show"),
            "the `impl Show` trait token must be a reference to trait `Show`"
        );
    }

    #[test]
    fn constraint_trait_name_is_recorded_as_reference() {
        let source =
            "trait Show a where\n  present : a -> Text\nfmtAll : Show a => a -> Text\nfmtAll = \\x -> present x\n";
        let module = parse(source);
        let defs = resolve_definitions(&module, source);
        assert!(
            has_ref_to(&defs, source, "Show", "Show"),
            "the `Show a =>` constraint token must be a reference to trait `Show`"
        );
    }

    #[test]
    fn migrate_relation_is_recorded_as_reference() {
        let source = "*users : [{v: Int 1}]\nf = \\x -> x\nmigrate *users from Int to Int using f\n";
        let module = parse(source);
        let defs = resolve_definitions(&module, source);
        assert!(
            has_ref_to(&defs, source, "users", "users"),
            "the migrated `*users` relation token must be a reference to source `users`"
        );
    }

    #[test]
    fn trait_method_anchor_ignores_where_substring_in_header() {
        // The trait name "Nowhere" contains the substring "where". Anchoring
        // the method search on the first *substring* `where` would land inside
        // the name, before the real keyword, and resolve method `a` to the
        // header type parameter `a`. A whole-word keyword search must skip it.
        let source = "trait Nowhere a where\n  a : a -> Int 1\n";
        let module = parse(source);
        let (defs, _, _) = resolve_definitions(&module, source);
        let span = defs.get("a").expect("method `a` is defined");
        assert!(
            span.start > source.find('\n').unwrap(),
            "method def should anchor after the header line, got {span:?}"
        );
        assert_eq!(&source[span.start..span.end], "a");
    }
}
