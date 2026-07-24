//! A unified read-only view over the "declarations" of a Knot program.
//!
//! A `.knot` file is a single expression (usually a `with`-record). The
//! declarations that used to be top-level `Decl` nodes — functions, data
//! types, type aliases, sources, views, derived relations, routes — now appear
//! as *markers* inside record literals and `with` bindings. This module walks
//! the expression once and yields a uniform [`DeclView`] for each, so the
//! compiler passes (effects, codegen, …) can iterate "the declarations"
//! without knowing where in the expression they live.
//!
//! Names are the qualified record path when a declaration is nested
//! (`db.*todos`), or the bare field name at the top level.

use knot::ast;

/// The shape-specific payload of a [`DeclView`].
#[derive(Clone, Copy)]
pub enum DeclViewKind<'a> {
    /// `data Name = …`
    Data {
        params: &'a [ast::Name],
        ctors: &'a [ast::ConstructorDef],
    },
    /// `type Name = …`
    TypeAlias {
        params: &'a [ast::Name],
        ty: &'a ast::Type,
    },
    /// `*name : [T]` — a persisted source relation.
    Source { ty: &'a ast::Type },
    /// `*name = body` — a view.
    View {
        ty: Option<&'a ast::TypeScheme>,
        body: Option<&'a ast::Expr>,
    },
    /// `&name = body` — a derived relation.
    Derived {
        ty: Option<&'a ast::TypeScheme>,
        body: Option<&'a ast::Expr>,
    },
    /// A named function: a record field with a lambda body and/or a signature.
    Fun {
        ty: Option<&'a ast::TypeScheme>,
        body: Option<&'a ast::Expr>,
    },
    /// `route Name where …`
    Route { entries: &'a [ast::RouteEntry] },
    /// `route Name = A | B`
    RouteComposite { components: &'a [String] },
    /// `*a <= *b` — a subset constraint.
    Subset {
        sub: &'a ast::RelationPath,
        sup: &'a ast::RelationPath,
    },
}

/// A single declaration discovered in the program.
#[derive(Clone, Copy)]
pub struct DeclView<'a> {
    pub name: &'a str,
    pub kind: DeclViewKind<'a>,
    /// The span of the marker expression (best-effort source location).
    pub span: ast::Span,
}

impl<'a> DeclView<'a> {
    pub fn body(&self) -> Option<&'a ast::Expr> {
        match self.kind {
            DeclViewKind::View { body, .. }
            | DeclViewKind::Derived { body, .. }
            | DeclViewKind::Fun { body, .. } => body,
            _ => None,
        }
    }
    pub fn ty(&self) -> Option<&'a ast::TypeScheme> {
        match self.kind {
            DeclViewKind::View { ty, .. }
            | DeclViewKind::Derived { ty, .. }
            | DeclViewKind::Fun { ty, .. } => ty,
            _ => None,
        }
    }
}

/// Collect every declaration in the program.
pub fn decl_views(program: &ast::Expr) -> Vec<DeclView<'_>> {
    let mut out = Vec::new();
    collect(program, &mut out);
    out
}

fn collect<'a>(e: &'a ast::Expr, out: &mut Vec<DeclView<'a>>) {
    use ast::ExprKind::*;
    match &e.node {
        Record(fields) => {
            for fl in fields {
                match &fl.value.node {
                    DataCtor { params, constructors, .. } => out.push(DeclView {
                        name: fl.name.as_str(),
                        span: fl.value.span,
                        kind: DeclViewKind::Data {
                            params,
                            ctors: constructors,
                        },
                    }),
                    TypeCtor { params, ty, .. } => out.push(DeclView {
                        name: fl.name.as_str(),
                        span: fl.value.span,
                        kind: DeclViewKind::TypeAlias { params, ty },
                    }),
                    SourceDecl { ty, .. } => out.push(DeclView {
                        name: fl.name.as_str(),
                        span: fl.value.span,
                        kind: DeclViewKind::Source { ty },
                    }),
                    ViewDecl { ty, body, .. } => out.push(DeclView {
                        name: fl.name.as_str(),
                        span: fl.value.span,
                        kind: DeclViewKind::View {
                            ty: ty.as_ref(),
                            body: Some(body),
                        },
                    }),
                    DerivedDecl { ty, body, .. } => out.push(DeclView {
                        name: fl.name.as_str(),
                        span: fl.value.span,
                        kind: DeclViewKind::Derived {
                            ty: ty.as_ref(),
                            body: Some(body),
                        },
                    }),
                    RouteDecl { entries, .. } => out.push(DeclView {
                        name: fl.name.as_str(),
                        span: fl.value.span,
                        kind: DeclViewKind::Route { entries },
                    }),
                    RouteCompositeDecl { components, .. } => out.push(DeclView {
                        name: fl.name.as_str(),
                        span: fl.value.span,
                        kind: DeclViewKind::RouteComposite { components },
                    }),
                    SubsetConstraint { sub, sup } => out.push(DeclView {
                        name: fl.name.as_str(),
                        span: fl.value.span,
                        kind: DeclViewKind::Subset { sub, sup },
                    }),
                    _ => {
                        // A named function: a record field with a lambda body
                        // and/or a signature.
                        let is_lambda = matches!(fl.value.node, Lambda { .. });
                        if is_lambda || fl.sig.is_some() {
                            out.push(DeclView {
                                name: fl.name.as_str(),
                                span: fl.value.span,
                                kind: DeclViewKind::Fun {
                                    ty: fl.sig.as_ref(),
                                    body: Some(&fl.value),
                                },
                            });
                        }
                    }
                }
                // Recurse into every field value to find nested records.
                collect(&fl.value, out);
            }
        }
        With { record, body } => {
            collect(record, out);
            collect(body, out);
        }
        _ => recurse(e, out),
    }
}

/// Read-only recursion into all sub-expressions for non-record nodes.
fn recurse<'a>(e: &'a ast::Expr, out: &mut Vec<DeclView<'a>>) {
    use ast::ExprKind::*;
    match &e.node {
        App { func, arg } => {
            collect(func, out);
            collect(arg, out);
        }
        Lambda { body, .. } => collect(body, out),
        BinOp { lhs, rhs, .. } => {
            collect(lhs, out);
            collect(rhs, out);
        }
        UnaryOp { operand, .. } => collect(operand, out),
        If {
            cond,
            then_branch,
            else_branch,
        } => {
            collect(cond, out);
            collect(then_branch, out);
            collect(else_branch, out);
        }
        Case { scrutinee, arms } => {
            collect(scrutinee, out);
            for arm in arms {
                collect(&arm.body, out);
            }
        }
        Do(stmts) => {
            for s in stmts {
                match &s.node {
                    ast::StmtKind::Bind { expr, .. } => collect(expr, out),
                    ast::StmtKind::Where { cond } => collect(cond, out),
                    ast::StmtKind::GroupBy { key } => collect(key, out),
                    ast::StmtKind::Expr(x) => collect(x, out),
                }
            }
        }
        Set { target, value } | ReplaceSet { target, value } => {
            collect(target, out);
            collect(value, out);
        }
        Atomic(x) | Refine(x) => collect(x, out),
        TimeUnitLit { value, .. } => collect(value, out),
        RecordUpdate { base, fields } => {
            collect(base, out);
            for fl in fields {
                collect(&fl.value, out);
            }
        }
        List(items) => {
            for it in items {
                collect(it, out);
            }
        }
        FieldAccess { expr, .. } | Annot { expr, .. } => collect(expr, out),
        Serve { handlers, .. } => {
            for h in handlers {
                collect(&h.body, out);
            }
        }
        ViewDecl { body, .. } | DerivedDecl { body, .. } => collect(body, out),
        _ => {}
    }
}
