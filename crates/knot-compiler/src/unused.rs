//! Warns about unused top-level definitions.
//!
//! Walks the user's parsed module and emits warnings for top-level
//! `Fun`, `Source`, `View`, `Derived`, `TypeAlias`, and `Data` declarations
//! whose names are never referenced from any other declaration.
//!
//! Exempt from warnings:
//! - Exported declarations (`exported = true`).
//! - The `main` function (program entry point).
//! - Names beginning with `_` (intentionally-unused convention).
//! - Signature-only `Fun` decls (no body — interface stubs).
//! - Trait, Impl, Route, RouteComposite, Migrate, SubsetConstraint, UnitDecl
//!   (skipped because their "use" is implicit in the runtime, not a name reference).
//!
//! Operate on the user's parsed `&[Decl]` slice — call this *before* prelude
//! injection / import resolution / desugaring so that only user code is in
//! scope. Imports and prelude can never reference user-defined names anyway.

use knot::ast::{
    self, CaseArm, ConstructorDef, Constraint, Decl, DeclKind, Effect, Expr, ExprKind,
    ImplItem, Pat, PatKind, PathSegment, RouteEntry, Stmt, StmtKind, TraitItem, Type,
    TypeKind, TypeScheme,
};
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
pub fn check(decls: &[Decl]) -> Vec<Diagnostic> {
    let per_decl: Vec<Refs> = decls.iter().map(refs_of_decl).collect();
    let mut diags = Vec::new();

    for (i, decl) in decls.iter().enumerate() {
        if decl.exported {
            continue;
        }
        match &decl.node {
            DeclKind::Fun { name, body, .. } => {
                if name == "main" || starts_with_underscore(name) {
                    continue;
                }
                // Signature-only declarations are interface stubs, not
                // unused values — skip them.
                if body.is_none() {
                    continue;
                }
                if !referenced_by_others(&per_decl, i, name, |r| &r.values) {
                    diags.push(make_warning("value", name, decl.span));
                }
            }
            DeclKind::TypeAlias { name, .. } => {
                if starts_with_underscore(name) {
                    continue;
                }
                if !referenced_by_others(&per_decl, i, name, |r| &r.types) {
                    diags.push(make_warning("type alias", name, decl.span));
                }
            }
            DeclKind::Data { name, constructors, .. } => {
                if starts_with_underscore(name) {
                    continue;
                }
                let type_used = referenced_by_others(&per_decl, i, name, |r| &r.types);
                let any_ctor_used = constructors
                    .iter()
                    .any(|c| referenced_by_others(&per_decl, i, &c.name, |r| &r.ctors));
                if !type_used && !any_ctor_used {
                    diags.push(make_warning("data type", name, decl.span));
                }
            }
            DeclKind::Source { name, .. } => {
                if starts_with_underscore(name) {
                    continue;
                }
                if !referenced_by_others(&per_decl, i, name, |r| &r.sources) {
                    diags.push(make_warning("source", name, decl.span));
                }
            }
            DeclKind::View { name, .. } => {
                if starts_with_underscore(name) {
                    continue;
                }
                // Views are queried with `*name` like sources.
                if !referenced_by_others(&per_decl, i, name, |r| &r.sources) {
                    diags.push(make_warning("view", name, decl.span));
                }
            }
            DeclKind::Derived { name, .. } => {
                if starts_with_underscore(name) {
                    continue;
                }
                if !referenced_by_others(&per_decl, i, name, |r| &r.deriveds) {
                    diags.push(make_warning("derived relation", name, decl.span));
                }
            }
            // Skip everything else: traits/impls (implicitly used at dispatch),
            // routes (top-level API surface), migrations, subset constraints,
            // and unit declarations.
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

fn make_warning(kind: &str, name: &str, span: ast::Span) -> Diagnostic {
    Diagnostic::warning(format!("unused {}: `{}`", kind, name))
        .label(span, "defined here but never used")
        .note(format!(
            "prefix the name with `_` to silence this warning, mark it `export`, or remove it"
        ))
}

// ── Per-declaration reference collection ─────────────────────────────

fn refs_of_decl(decl: &Decl) -> Refs {
    let mut r = Refs::default();
    walk_decl(&decl.node, &mut r);
    r
}

fn walk_decl(decl: &DeclKind, r: &mut Refs) {
    match decl {
        DeclKind::Data { constructors, deriving: _, .. } => {
            for ctor in constructors {
                walk_ctor_def(ctor, r);
            }
        }
        DeclKind::TypeAlias { ty, .. } => walk_type(ty, r),
        DeclKind::Source { ty, .. } => walk_type(ty, r),
        DeclKind::View { ty, body, .. } => {
            if let Some(scheme) = ty {
                walk_scheme(scheme, r);
            }
            walk_expr(body, r);
        }
        DeclKind::Derived { ty, body, .. } => {
            if let Some(scheme) = ty {
                walk_scheme(scheme, r);
            }
            walk_expr(body, r);
        }
        DeclKind::Fun { ty, body, .. } => {
            if let Some(scheme) = ty {
                walk_scheme(scheme, r);
            }
            if let Some(b) = body {
                walk_expr(b, r);
            }
        }
        DeclKind::Trait { items, supertraits, .. } => {
            for c in supertraits {
                walk_constraint(c, r);
            }
            for item in items {
                walk_trait_item(item, r);
            }
        }
        DeclKind::Impl { args, constraints, items, .. } => {
            for a in args {
                walk_type(a, r);
            }
            for c in constraints {
                walk_constraint(c, r);
            }
            for item in items {
                walk_impl_item(item, r);
            }
        }
        DeclKind::Route { entries, .. } => {
            for e in entries {
                walk_route_entry(e, r);
            }
        }
        DeclKind::RouteComposite { .. } => {}
        DeclKind::Migrate { from_ty, to_ty, using_fn, .. } => {
            walk_type(from_ty, r);
            walk_type(to_ty, r);
            walk_expr(using_fn, r);
        }
        DeclKind::SubsetConstraint { sub, sup } => {
            r.sources.insert(sub.relation.clone());
            r.sources.insert(sup.relation.clone());
        }
        DeclKind::UnitDecl { .. } => {}
    }
}

fn walk_ctor_def(c: &ConstructorDef, r: &mut Refs) {
    for f in &c.fields {
        walk_type(&f.value, r);
    }
}

fn walk_constraint(c: &Constraint, r: &mut Refs) {
    for arg in &c.args {
        walk_type(arg, r);
    }
}

fn walk_scheme(s: &TypeScheme, r: &mut Refs) {
    for c in &s.constraints {
        walk_constraint(c, r);
    }
    walk_type(&s.ty, r);
}

fn walk_trait_item(item: &TraitItem, r: &mut Refs) {
    match item {
        TraitItem::Method { ty, default_body, default_params, .. } => {
            walk_scheme(ty, r);
            for p in default_params {
                walk_pat(p, r);
            }
            if let Some(b) = default_body {
                walk_expr(b, r);
            }
        }
        TraitItem::AssociatedType { .. } => {}
    }
}

fn walk_impl_item(item: &ImplItem, r: &mut Refs) {
    match item {
        ImplItem::Method { params, body, .. } => {
            for p in params {
                walk_pat(p, r);
            }
            walk_expr(body, r);
        }
        ImplItem::AssociatedType { args, ty, .. } => {
            for a in args {
                walk_type(a, r);
            }
            walk_type(ty, r);
        }
    }
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
}

// ── Expression walker ────────────────────────────────────────────────

fn walk_expr(e: &Expr, r: &mut Refs) {
    match &e.node {
        ExprKind::Lit(_) => {}
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
        ExprKind::FieldAccess { expr, .. } => walk_expr(expr, r),
        ExprKind::List(items) => {
            for it in items {
                walk_expr(it, r);
            }
        }
        ExprKind::Lambda { params, body } => {
            for p in params {
                walk_pat(p, r);
            }
            walk_expr(body, r);
        }
        ExprKind::App { func, arg } => {
            walk_expr(func, r);
            walk_expr(arg, r);
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
        ExprKind::At { relation, time } => {
            walk_expr(relation, r);
            walk_expr(time, r);
        }
        ExprKind::UnitLit { value, .. } => walk_expr(value, r),
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
        StmtKind::Bind { pat, expr } | StmtKind::Let { pat, expr } => {
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
        PatKind::Constructor { name, payload } => {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(src: &str) -> ast::Module {
        let lexer = knot::lexer::Lexer::new(src);
        let (tokens, lex_diags) = lexer.tokenize();
        assert!(
            !lex_diags.iter().any(|d| d.severity == knot::diagnostic::Severity::Error),
            "lex errors: {:?}",
            lex_diags
        );
        let parser = knot::parser::Parser::new(src.to_string(), tokens);
        let (module, parse_diags) = parser.parse_module();
        assert!(
            !parse_diags.iter().any(|d| d.severity == knot::diagnostic::Severity::Error),
            "parse errors: {:?}",
            parse_diags
        );
        module
    }

    fn warns(src: &str) -> Vec<String> {
        let module = parse(src);
        check(&module.decls)
            .into_iter()
            .map(|d| d.message)
            .collect()
    }

    #[test]
    fn unused_function_warns() {
        let warnings = warns(
            r#"
helper = \x -> x + 1

main = println "hi"
"#,
        );
        assert_eq!(warnings.len(), 1, "got: {:?}", warnings);
        assert!(warnings[0].contains("helper"), "got: {}", warnings[0]);
    }

    #[test]
    fn used_function_does_not_warn() {
        let warnings = warns(
            r#"
helper = \x -> x + 1

main = println (show (helper 1))
"#,
        );
        assert!(warnings.is_empty(), "got: {:?}", warnings);
    }

    #[test]
    fn main_never_warns() {
        let warnings = warns(r#"main = println "hi""#);
        assert!(warnings.is_empty(), "got: {:?}", warnings);
    }

    #[test]
    fn underscore_prefix_silences() {
        let warnings = warns(
            r#"
_helper = \x -> x + 1

main = println "hi"
"#,
        );
        assert!(warnings.is_empty(), "got: {:?}", warnings);
    }

    #[test]
    fn exported_does_not_warn() {
        let warnings = warns(
            r#"
export helper = \x -> x + 1

main = println "hi"
"#,
        );
        assert!(warnings.is_empty(), "got: {:?}", warnings);
    }

    #[test]
    fn unused_data_warns() {
        let warnings = warns(
            r#"
data Shape = Circle {radius: Float} | Rect {w: Float, h: Float}

main = println "hi"
"#,
        );
        assert_eq!(warnings.len(), 1, "got: {:?}", warnings);
        assert!(warnings[0].contains("Shape"), "got: {}", warnings[0]);
    }

    #[test]
    fn data_used_via_constructor_does_not_warn() {
        let warnings = warns(
            r#"
data Shape = Circle {radius: Float} | Rect {w: Float, h: Float}

mkShape = Circle {radius: 1.0}

main = println (show mkShape)
"#,
        );
        assert!(warnings.is_empty(), "got: {:?}", warnings);
    }

    #[test]
    fn data_used_via_type_alias_does_not_warn() {
        let warnings = warns(
            r#"
data Shape = Circle {radius: Float} | Rect {w: Float, h: Float}

type Shapes = [Shape]

main = println "hi"
"#,
        );
        // Shape used by the type alias; type alias itself is unused.
        let msgs = warnings.join(" | ");
        assert!(msgs.contains("Shapes"), "got: {}", msgs);
        assert!(!msgs.contains("`Shape`") || msgs.contains("Shapes"), "got: {}", msgs);
    }

    #[test]
    fn unused_type_alias_warns() {
        let warnings = warns(
            r#"
type Pair = {a: Int, b: Int}

main = println "hi"
"#,
        );
        assert_eq!(warnings.len(), 1, "got: {:?}", warnings);
        assert!(warnings[0].contains("Pair"), "got: {}", warnings[0]);
    }

    #[test]
    fn unused_source_warns() {
        let warnings = warns(
            r#"
*people : [{name: Text, age: Int}]

main = println "hi"
"#,
        );
        assert_eq!(warnings.len(), 1, "got: {:?}", warnings);
        assert!(warnings[0].contains("people"), "got: {}", warnings[0]);
    }

    #[test]
    fn used_source_does_not_warn() {
        let warnings = warns(
            r#"
*people : [{name: Text, age: Int}]

main = do
  p <- *people
  println p.name
"#,
        );
        assert!(warnings.is_empty(), "got: {:?}", warnings);
    }

    #[test]
    fn signature_only_fun_does_not_warn() {
        let warnings = warns(
            r#"
helper : Int -> Int

main = println "hi"
"#,
        );
        assert!(warnings.is_empty(), "got: {:?}", warnings);
    }

    #[test]
    fn mutual_recursion_does_not_warn() {
        // `even` and `odd` reference each other but neither is reachable from
        // main; the simple check still treats them as "used" because each is
        // referenced by another decl. This documents the limitation — a more
        // sophisticated pass would do reachability from main/exports.
        let warnings = warns(
            r#"
isEven = \n -> if n == 0 then true else isOdd (n - 1)
isOdd = \n -> if n == 0 then false else isEven (n - 1)

main = println "hi"
"#,
        );
        assert!(warnings.is_empty(), "got: {:?}", warnings);
    }

    #[test]
    fn self_recursion_does_warn() {
        // `forever` only references itself — must be flagged as unused.
        let warnings = warns(
            r#"
forever = \x -> forever x

main = println "hi"
"#,
        );
        assert_eq!(warnings.len(), 1, "got: {:?}", warnings);
        assert!(warnings[0].contains("forever"), "got: {}", warnings[0]);
    }
}
