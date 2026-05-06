//! Definition resolution: scope-aware AST walk that produces (1) span-keyed
//! references for goto/find-references, (2) name-keyed top-level definitions
//! as a fallback, and (3) literal-type info for hover.
//!
//! Also lives here: `build_details`, which formats per-declaration "summary"
//! strings used as completion details and hover headlines.

use std::collections::HashMap;

use knot::ast::{self, DeclKind, Module, Span, Type, TypeKind};

use crate::type_format::{format_type_kind, format_type_scheme};
use crate::utils::find_word_in_source;

/// Resolve definitions: returns (name_map, span_references, literal_types).
pub fn resolve_definitions(
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
            DeclKind::Fun { body, ty, .. } => {
                if let Some(scheme) = ty {
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
            DeclKind::Impl { args, items, constraints, .. } => {
                for arg in args {
                    resolver.resolve_type(arg, source);
                }
                for c in constraints {
                    for arg in &c.args {
                        resolver.resolve_type(arg, source);
                    }
                }
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
            DeclKind::Trait { items, supertraits, .. } => {
                for c in supertraits {
                    for arg in &c.args {
                        resolver.resolve_type(arg, source);
                    }
                }
                for item in items {
                    if let ast::TraitItem::Method {
                        default_params,
                        default_body,
                        ty,
                        ..
                    } = item
                    {
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
            DeclKind::Migrate { from_ty, to_ty, using_fn, .. } => {
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
                }
            }
            _ => {}
        }
    }

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
            ast::PatKind::Var(name) => self.define(name, pat.span),
            ast::PatKind::Constructor { name, payload } => {
                self.add_ref(pat.span, name);
                self.define_pat(payload);
            }
            ast::PatKind::Record(fields) => {
                for f in fields {
                    if let Some(p) = &f.pattern {
                        self.define_pat(p);
                    } else {
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
            ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
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
            ast::ExprKind::Serve { api, api_span, handlers } => {
                self.add_ref(*api_span, api);
                for h in handlers {
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
