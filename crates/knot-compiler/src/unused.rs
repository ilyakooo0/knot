//! Warns about unused top-level definitions.
//!
//! Walks the user's parsed module and emits warnings for top-level
//! `Fun`, `Source`, `View`, `Derived`, `TypeAlias`, and `Data` declarations
//! whose names are never referenced from any other declaration.
//!
//! Exempt from warnings:
//! - The `main` function (program entry point).
//! - Names beginning with `_` (intentionally-unused convention).
//! - Signature-only `Fun` decls (no body — interface stubs).
//! - Trait, Impl, Route, RouteComposite, Migrate, SubsetConstraint
//!   (skipped because their "use" is implicit in the runtime, not a name reference).
//!
//! Operate on the user's parsed `&[Decl]` slice — call this *before* prelude
//! injection / import resolution / desugaring so that only user code is in
//! scope. Imports and prelude can never reference user-defined names anyway.

use crate::decl_view::{decl_views, DeclView, DeclViewKind};
use knot::ast::{CaseArm, ConstructorDef, Constraint, Effect, Expr, ExprKind, Pat, PatKind, PathSegment, RouteEntry, Stmt, StmtKind, Type, TypeKind, TypeScheme};
use knot::diagnostic::Diagnostic;
use std::collections::HashSet;

/// Per-declaration record of every name the declaration's body refers to.
#[derive(Default, Debug)]
struct Refs {
    /// Lowercase identifiers in expression position: `Var(name)`.
    values: HashSet<String>,
    /// Type names in type position: `TypeKind::Named(name)`.
    types: HashSet<String>,
    /// Constructor references (`ExprKind::Constructor` or `PatKind::Constructor`).
    ctors: HashSet<String>,
    /// `*name` references: `SourceRef`, `Set` targets, `ReplaceSet` targets,
    /// `At` relations, effect-row `r`/`w`, and subset relation paths.
    sources: HashSet<String>,
    /// `&name` references.
    deriveds: HashSet<String>,
}

/// Check the user's declarations for unused names.
pub fn check(program: &Expr) -> Vec<Diagnostic> {
    let decls = decl_views(program);
    let per_decl: Vec<Refs> = decls.iter().map(refs_of_decl).collect();
    let mut diags = Vec::new();

    for (i, decl) in decls.iter().enumerate() {
        let name = decl.name;
        let dspan = decl.span;
        match decl.kind {
            DeclViewKind::Fun { body, .. } => {
                if name == "main" || starts_with_underscore(name) {
                    continue;
                }
                // Signature-only declarations are interface stubs, not
                // unused values — skip them.
                if body.is_none() {
                    continue;
                }
                if !referenced_by_others(&per_decl, i, name, |r| &r.values) {
                    diags.push(make_warning("value", name, dspan));
                }
            }
            DeclViewKind::TypeAlias { .. } => {
                if starts_with_underscore(name) {
                    continue;
                }
                if !referenced_by_others(&per_decl, i, name, |r| &r.types) {
                    diags.push(make_warning("type alias", name, dspan));
                }
            }
            DeclViewKind::Data { ctors, .. } => {
                if starts_with_underscore(name) {
                    continue;
                }
                let type_used = referenced_by_others(&per_decl, i, name, |r| &r.types);
                let any_ctor_used = ctors
                    .iter()
                    .any(|c| referenced_by_others(&per_decl, i, &c.name, |r| &r.ctors));
                if !type_used && !any_ctor_used {
                    diags.push(make_warning("data type", name, dspan));
                }
            }
            DeclViewKind::Source { .. } => {
                if starts_with_underscore(name) {
                    continue;
                }
                if !referenced_by_others(&per_decl, i, name, |r| &r.sources) {
                    diags.push(make_warning("source", name, dspan));
                }
            }
            DeclViewKind::View { .. } => {
                if starts_with_underscore(name) {
                    continue;
                }
                // Views are queried with `*name` like sources.
                if !referenced_by_others(&per_decl, i, name, |r| &r.sources) {
                    diags.push(make_warning("view", name, dspan));
                }
            }
            DeclViewKind::Derived { .. } => {
                if starts_with_underscore(name) {
                    continue;
                }
                if !referenced_by_others(&per_decl, i, name, |r| &r.deriveds) {
                    diags.push(make_warning("derived relation", name, dspan));
                }
            }
            // Skip everything else: routes (top-level API surface),
            // migrations, and subset constraints.
            _ => {}
        }
    }

    diags
}

fn starts_with_underscore(name: &str) -> bool {
    name.starts_with('_')
}

fn referenced_by_others<F>(refs: &[Refs], my_idx: usize, name: &str, get: F) -> bool
where
    F: Fn(&Refs) -> &HashSet<String>,
{
    refs.iter()
        .enumerate()
        .any(|(j, r)| j != my_idx && get(r).contains(name))
}

fn make_warning(kind: &str, name: &str, span: knot::ast::Span) -> Diagnostic {
    Diagnostic::warning(format!("unused {}: `{}`", kind, name))
        .label(span, "defined here but never used")
        .note("prefix the name with `_` to silence this warning, or remove it".to_string())
}

// ── Per-declaration reference collection ─────────────────────────────

fn refs_of_decl(decl: &DeclView) -> Refs {
    let mut r = Refs::default();
    walk_decl(decl, &mut r);
    r
}

fn walk_decl(decl: &DeclView, r: &mut Refs) {
    match decl.kind {
        DeclViewKind::Data { ctors, .. } => {
            for ctor in ctors {
                walk_ctor_def(ctor, r);
            }
        }
        DeclViewKind::TypeAlias { ty, .. } => walk_type(ty, r),
        DeclViewKind::Source { ty, .. } => walk_type(ty, r),
        DeclViewKind::View { ty, body, .. } => {
            if let Some(scheme) = ty {
                walk_scheme(scheme, r);
            }
            if let Some(b) = body {
                walk_expr(b, r);
            }
        }
        DeclViewKind::Derived { ty, body, .. } => {
            if let Some(scheme) = ty {
                walk_scheme(scheme, r);
            }
            if let Some(b) = body {
                walk_expr(b, r);
            }
        }
        DeclViewKind::Fun { ty, body, .. } => {
            if let Some(scheme) = ty {
                walk_scheme(scheme, r);
            }
            if let Some(b) = body {
                walk_expr(b, r);
            }
        }
        DeclViewKind::Route { entries } => {
            for e in entries {
                walk_route_entry(e, r);
            }
        }
        DeclViewKind::RouteComposite { .. } => {}
        DeclViewKind::Subset { sub, sup } => {
            r.sources.insert(sub.relation.clone());
            r.sources.insert(sup.relation.clone());
        }
    }
}

fn walk_ctor_def(c: &ConstructorDef, r: &mut Refs) {
    for f in &c.fields {
        walk_type(&f.value, r);
    }
}

fn walk_constraint(c: &Constraint, r: &mut Refs) {
    match c {
        Constraint::Trait { args, .. } => {
            for arg in args {
                walk_type(arg, r);
            }
        }
        Constraint::ImplicitField { ty, .. } => walk_type(ty, r),
    }
}

fn walk_scheme(s: &TypeScheme, r: &mut Refs) {
    for c in &s.constraints {
        walk_constraint(c, r);
    }
    walk_type(&s.ty, r);
}

fn walk_route_entry(e: &RouteEntry, r: &mut Refs) {
    for seg in &e.path {
        if let PathSegment::Param { ty, .. } = seg {
            walk_type(ty, r);
        }
    }
    for f in &e.body_fields {
        walk_type(&f.value, r);
    }
    for f in &e.query_params {
        walk_type(&f.value, r);
    }
    for f in &e.request_headers {
        walk_type(&f.value, r);
    }
    if let Some(t) = &e.response_ty {
        walk_type(t, r);
    }
    for f in &e.response_headers {
        walk_type(&f.value, r);
    }
    if let Some(expr) = &e.rate_limit {
        walk_expr(expr, r);
    }
}

// ── Expression walker ────────────────────────────────────────────────

fn walk_expr(e: &Expr, r: &mut Refs) {
    match &e.node {
        ExprKind::Lit(_) => {}
        // `^x` reads a field of an in-scope record, but which record is only
        // known after type inference (recorded in `Infer::implicit_refs`);
        // unused-analysis runs on the AST without that map, so it can't name
        // the root binding. Treat as using nothing (no leaf refs).
        ExprKind::ImplicitRef(_) => {}
        ExprKind::TypeCtor { .. } | ExprKind::DataCtor { .. } | ExprKind::SourceDecl { .. } | ExprKind::SubsetConstraint { .. } => {}
        ExprKind::RouteDecl { .. } | ExprKind::RouteCompositeDecl { .. } => {}
        ExprKind::ViewDecl { body, .. } | ExprKind::DerivedDecl { body, .. } => walk_expr(body, r),
        ExprKind::Var(name) => {
            r.values.insert(name.clone());
        }
        ExprKind::Constructor(name) => {
            r.ctors.insert(name.clone());
        }
        ExprKind::SourceRef(name) => {
            r.sources.insert(name.clone());
        }
        ExprKind::DerivedRef(name) => {
            r.deriveds.insert(name.clone());
        }
        ExprKind::Record(fields) => {
            for f in fields {
                walk_expr(&f.value, r);
            }
        }
        ExprKind::RecordUpdate { base, fields } => {
            walk_expr(base, r);
            for f in fields {
                walk_expr(&f.value, r);
            }
        }
        ExprKind::FieldAccess { expr, field } => {
            // Qualified constructor `Color.Red`: the base is the data type and
            // `field` is the constructor. Count the ctor as used so the data
            // type isn't flagged unused. (`Constructor("Color")` itself is also
            // walked, recording the type-name reference.)
            if let ExprKind::Constructor(_) = &expr.node {
                r.ctors.insert(field.clone());
            }
            walk_expr(expr, r);
        }
        ExprKind::List(items) => {
            for it in items {
                walk_expr(it, r);
            }
        }
        ExprKind::Lambda { params, body, .. } => {
            for p in params {
                walk_pat(p, r);
            }
            walk_expr(body, r);
        }
        ExprKind::App { func, arg } => {
            walk_expr(func, r);
            walk_expr(arg, r);
        }
        ExprKind::With { record, body } => {
            walk_expr(record, r);
            walk_expr(body, r);
        }
        ExprKind::BinOp { lhs, rhs, .. } => {
            walk_expr(lhs, r);
            walk_expr(rhs, r);
        }
        ExprKind::UnaryOp { operand, .. } => walk_expr(operand, r),
        ExprKind::If { cond, then_branch, else_branch } => {
            walk_expr(cond, r);
            walk_expr(then_branch, r);
            walk_expr(else_branch, r);
        }
        ExprKind::Case { scrutinee, arms } => {
            walk_expr(scrutinee, r);
            for arm in arms {
                walk_case_arm(arm, r);
            }
        }
        ExprKind::Do(stmts) => {
            for s in stmts {
                walk_stmt(s, r);
            }
        }
        ExprKind::Set { target, value } => {
            walk_expr(target, r);
            walk_expr(value, r);
        }
        ExprKind::ReplaceSet { target, value } => {
            walk_expr(target, r);
            walk_expr(value, r);
        }
        ExprKind::Atomic(inner) => walk_expr(inner, r),
        ExprKind::TimeUnitLit { value, .. } => walk_expr(value, r),
        ExprKind::Annot { expr, ty } => {
            walk_expr(expr, r);
            walk_type(ty, r);
        }
        ExprKind::Refine(inner) => walk_expr(inner, r),
        ExprKind::Serve { handlers, .. } => {
            // The `api` name refers to a route; routes aren't tracked as
            // unused-warn candidates so we don't need to record it. Each
            // handler endpoint is a route-generated constructor — also
            // skipped. Walk handler bodies to collect refs.
            for h in handlers {
                walk_expr(&h.body, r);
            }
        }
    }
}

fn walk_case_arm(arm: &CaseArm, r: &mut Refs) {
    walk_pat(&arm.pat, r);
    walk_expr(&arm.body, r);
}

fn walk_stmt(s: &Stmt, r: &mut Refs) {
    match &s.node {
        StmtKind::Bind { pat, expr } => {
            walk_pat(pat, r);
            walk_expr(expr, r);
        }
        StmtKind::Where { cond } => walk_expr(cond, r),
        StmtKind::GroupBy { key } => walk_expr(key, r),
        StmtKind::Expr(e) => walk_expr(e, r),
    }
}

fn walk_pat(p: &Pat, r: &mut Refs) {
    match &p.node {
        PatKind::Var(_) | PatKind::Wildcard | PatKind::Lit(_) => {}
        PatKind::Constructor { name, payload, .. } => {
            r.ctors.insert(name.clone());
            walk_pat(payload, r);
        }
        PatKind::Record(fields) => {
            for fp in fields {
                if let Some(inner) = &fp.pattern {
                    walk_pat(inner, r);
                }
            }
        }
        PatKind::List(items) => {
            for it in items {
                walk_pat(it, r);
            }
        }
        PatKind::Cons { head, tail } => {
            walk_pat(head, r);
            walk_pat(tail, r);
        }
        PatKind::Annot { pat, .. } => walk_pat(pat, r),
    }
}

fn walk_type(t: &Type, r: &mut Refs) {
    match &t.node {
        TypeKind::Named(name) => {
            r.types.insert(name.clone());
        }
        TypeKind::Var(_) | TypeKind::Hole => {}
        TypeKind::App { func, arg } => {
            walk_type(func, r);
            walk_type(arg, r);
        }
        TypeKind::Record { fields, .. } => {
            for f in fields {
                walk_type(&f.value, r);
            }
        }
        TypeKind::Relation(inner) => walk_type(inner, r),
        TypeKind::Function { param, result } => {
            walk_type(param, r);
            walk_type(result, r);
        }
        TypeKind::Variant { constructors, .. } => {
            for c in constructors {
                walk_ctor_def(c, r);
            }
        }
        TypeKind::Effectful { effects, ty } => {
            for e in effects {
                walk_effect(e, r);
            }
            walk_type(ty, r);
        }
        TypeKind::IO { effects, ty, .. } => {
            for e in effects {
                walk_effect(e, r);
            }
            walk_type(ty, r);
        }
        TypeKind::UnitAnnotated { base, .. } => walk_type(base, r),
        TypeKind::Unit(_) => {}
        TypeKind::Refined { base, predicate } => {
            walk_type(base, r);
            walk_expr(predicate, r);
        }
        TypeKind::Forall { ty, .. } => walk_type(ty, r),
    }
}

fn walk_effect(e: &Effect, r: &mut Refs) {
    match e {
        Effect::Reads(name) | Effect::Writes(name) => {
            r.sources.insert(name.clone());
        }
        Effect::Console | Effect::Network | Effect::Fs | Effect::Clock | Effect::Random => {}
    }
}

// ── Tests ────────────────────────────────────────────────────────────


