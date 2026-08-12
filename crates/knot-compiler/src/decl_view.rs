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
    Source {
        ty: &'a ast::Type,
        migrations: &'a [ast::SourceMigration],
    },
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
    // The user's declaration record is the record of the INNERMOST `with` in
    // the `with {prelude} <user program>` chain (the user's own `with`, when
    // theirs is one). Only TOP-LEVEL fields are declarations — nested record
    // literals are plain values, not decls (recursing into a field's value
    // would turn `{name "Alice"}` into a spurious `name` decl and collide
    // nested `rec {max …}` field-functions).
    let mut cur = program;
    loop {
        match &cur.node {
            ast::ExprKind::With { record, body, .. }
                if matches!(body.node, ast::ExprKind::With { .. }) =>
            {
                cur = body;
            }
            ast::ExprKind::With { record, .. } => {
                collect(record, &mut out);
                break;
            }
            _ => {
                collect(cur, &mut out);
                break;
            }
        }
    }
    out
}

fn collect<'a>(e: &'a ast::Expr, out: &mut Vec<DeclView<'a>>) {
    use ast::ExprKind::*;
    if let Record(fields) = &e.node {
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
                SourceDecl { ty, migrations, .. } => out.push(DeclView {
                    name: fl.name.as_str(),
                    span: fl.value.span,
                    kind: DeclViewKind::Source { ty, migrations },
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
                    // A named value/function: any record field whose value
                    // is not a declaration marker. This covers lambdas
                    // (functions), signatures, AND plain value bindings
                    // like `nums [1, 2, 3]` — all become top-level named
                    // declarations, exactly as the Phase-1 lowering turned
                    // `with {f v} body` fields into decls.
                    //
                    // A signature-only field (`name : Type` with no `=`)
                    // is emitted by the parser as `sig: Some` + an
                    // empty-record placeholder value. That is a required
                    // CLI constant, NOT a `{}` binding: surface it as
                    // `Fun { body: None }` so codegen registers a startup
                    // `--name=value` lookup instead of checking the empty
                    // record against the sig (which fails to type-check).
                    let is_required_const = fl.sig.is_some()
                        && matches!(&fl.value.node, Record(fs) if fs.is_empty());
                    out.push(DeclView {
                        name: fl.name.as_str(),
                        span: fl.value.span,
                        kind: DeclViewKind::Fun {
                            ty: fl.sig.as_ref(),
                            body: if is_required_const {
                                None
                            } else {
                                Some(&fl.value)
                            },
                        },
                    });
                }
            }
        }
    }
}
