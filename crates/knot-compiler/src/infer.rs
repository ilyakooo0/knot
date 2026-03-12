//! Hindley-Milner type inference for the Knot language.
//!
//! Infers and checks types for all declarations. Reports type errors as
//! diagnostics. The runtime uses uniform pointer representation, so this
//! pass is purely for error detection — it does not affect code generation.

use knot::ast;
use knot::ast::Span;
use knot::diagnostic::Diagnostic;
use std::collections::{BTreeMap, HashMap, HashSet};

// ── Internal type representation ──────────────────────────────────

type TyVar = u32;

/// Internal type representation for unification-based inference.
#[derive(Debug, Clone)]
enum Ty {
    /// Unification variable.
    Var(TyVar),
    /// Primitives.
    Int,
    Float,
    Text,
    Bool,
    /// Function type.
    Fun(Box<Ty>, Box<Ty>),
    /// Record with named fields and optional row variable (open record).
    Record(BTreeMap<String, Ty>, Option<TyVar>),
    /// Relation (set) type: [T].
    Relation(Box<Ty>),
    /// Named algebraic data type with optional type arguments.
    Con(String, Vec<Ty>),
    /// Error sentinel — suppresses cascading errors.
    Error,
}

impl Ty {
    fn unit() -> Ty {
        Ty::Record(BTreeMap::new(), None)
    }
}

/// Polymorphic type scheme: ∀ vars. ty
#[derive(Debug, Clone)]
struct Scheme {
    vars: Vec<TyVar>,
    ty: Ty,
}

impl Scheme {
    fn mono(ty: Ty) -> Self {
        Scheme { vars: vec![], ty }
    }
}

// ── Constructor and data type metadata ────────────────────────────

#[derive(Debug, Clone)]
struct CtorInfo {
    data_type: String,
    data_params: Vec<String>,
    fields: Vec<(String, ast::Type)>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct DataInfo {
    params: Vec<String>,
    ctors: Vec<(String, Vec<(String, ast::Type)>)>,
}

// ── Inference engine ──────────────────────────────────────────────

struct Infer {
    next_var: TyVar,
    subst: HashMap<TyVar, Ty>,

    /// Scoped variable environment (functions, let-bindings, params).
    scopes: Vec<HashMap<String, Scheme>>,

    /// Constructor metadata: ctor_name → info.
    constructors: HashMap<String, CtorInfo>,

    /// Data type definitions: type_name → info.
    data_types: HashMap<String, DataInfo>,

    /// Source/view relation types: name → full type (always Relation(...)).
    source_types: HashMap<String, Ty>,

    /// Derived relation types: name → full type.
    derived_types: HashMap<String, Ty>,

    /// Names that are views (for lenient set checking).
    view_names: HashSet<String>,

    /// Associated type names (from trait declarations).
    assoc_type_names: HashSet<String>,

    /// Type aliases: name → resolved Ty.
    aliases: HashMap<String, Ty>,

    /// Mapping from annotation type-variable names to TyVars (per-declaration).
    annotation_vars: HashMap<String, TyVar>,

    /// Accumulated type errors.
    errors: Vec<(String, Span)>,
}

// ── Core operations ───────────────────────────────────────────────

impl Infer {
    fn new() -> Self {
        Self {
            next_var: 0,
            subst: HashMap::new(),
            scopes: vec![HashMap::new()],
            constructors: HashMap::new(),
            data_types: HashMap::new(),
            source_types: HashMap::new(),
            derived_types: HashMap::new(),
            view_names: HashSet::new(),
            assoc_type_names: HashSet::new(),
            aliases: HashMap::new(),
            annotation_vars: HashMap::new(),
            errors: Vec::new(),
        }
    }

    fn fresh(&mut self) -> Ty {
        Ty::Var(self.fresh_var())
    }

    fn fresh_var(&mut self) -> TyVar {
        let v = self.next_var;
        self.next_var += 1;
        v
    }

    fn error(&mut self, msg: String, span: Span) {
        self.errors.push((msg, span));
    }

    // ── Substitution application ─────────────────────────────────

    fn apply(&self, ty: &Ty) -> Ty {
        match ty {
            Ty::Var(v) => match self.subst.get(v) {
                Some(resolved) => self.apply(resolved),
                None => ty.clone(),
            },
            Ty::Fun(p, r) => {
                Ty::Fun(Box::new(self.apply(p)), Box::new(self.apply(r)))
            }
            Ty::Record(fields, row) => {
                let mut applied: BTreeMap<String, Ty> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), self.apply(v)))
                    .collect();
                if let Some(rv) = row {
                    let resolved = self.apply(&Ty::Var(*rv));
                    match resolved {
                        Ty::Record(extra, rest) => {
                            for (k, v) in extra {
                                applied.entry(k).or_insert(v);
                            }
                            Ty::Record(applied, rest)
                        }
                        Ty::Var(rv2) => Ty::Record(applied, Some(rv2)),
                        _ => Ty::Record(applied, None),
                    }
                } else {
                    Ty::Record(applied, None)
                }
            }
            Ty::Relation(inner) => {
                Ty::Relation(Box::new(self.apply(inner)))
            }
            Ty::Con(name, args) => Ty::Con(
                name.clone(),
                args.iter().map(|a| self.apply(a)).collect(),
            ),
            _ => ty.clone(),
        }
    }

    // ── Occurs check ─────────────────────────────────────────────

    fn occurs_in(&self, var: TyVar, ty: &Ty) -> bool {
        match ty {
            Ty::Var(v) => {
                if *v == var {
                    return true;
                }
                match self.subst.get(v) {
                    Some(resolved) => self.occurs_in(var, resolved),
                    None => false,
                }
            }
            Ty::Fun(p, r) => {
                self.occurs_in(var, p) || self.occurs_in(var, r)
            }
            Ty::Record(fields, row) => {
                if fields.values().any(|v| self.occurs_in(var, v)) {
                    return true;
                }
                if let Some(rv) = row {
                    if *rv == var {
                        return true;
                    }
                    if let Some(resolved) = self.subst.get(rv) {
                        return self.occurs_in(var, resolved);
                    }
                }
                false
            }
            Ty::Relation(inner) => self.occurs_in(var, inner),
            Ty::Con(_, args) => args.iter().any(|a| self.occurs_in(var, a)),
            _ => false,
        }
    }

    // ── Unification ──────────────────────────────────────────────

    fn unify(&mut self, t1: &Ty, t2: &Ty, span: Span) {
        let t1 = self.apply(t1);
        let t2 = self.apply(t2);

        match (&t1, &t2) {
            (Ty::Error, _) | (_, Ty::Error) => {}
            (Ty::Var(a), Ty::Var(b)) if a == b => {}
            (Ty::Var(v), _) => {
                let v = *v;
                if self.occurs_in(v, &t2) {
                    self.error("infinite type".into(), span);
                } else {
                    self.subst.insert(v, t2);
                }
            }
            (_, Ty::Var(v)) => {
                let v = *v;
                if self.occurs_in(v, &t1) {
                    self.error("infinite type".into(), span);
                } else {
                    self.subst.insert(v, t1);
                }
            }
            (Ty::Int, Ty::Int)
            | (Ty::Float, Ty::Float)
            | (Ty::Text, Ty::Text)
            | (Ty::Bool, Ty::Bool) => {}
            (Ty::Fun(p1, r1), Ty::Fun(p2, r2)) => {
                let (p1, p2) = (p1.clone(), p2.clone());
                let (r1, r2) = (r1.clone(), r2.clone());
                self.unify(&p1, &p2, span);
                self.unify(&r1, &r2, span);
            }
            (Ty::Relation(a), Ty::Relation(b)) => {
                let (a, b) = (a.clone(), b.clone());
                self.unify(&a, &b, span);
            }
            (Ty::Con(n1, a1), Ty::Con(n2, a2))
                if n1 == n2 && a1.len() == a2.len() =>
            {
                let pairs: Vec<_> = a1
                    .iter()
                    .zip(a2.iter())
                    .map(|(a, b)| (a.clone(), b.clone()))
                    .collect();
                for (a, b) in &pairs {
                    self.unify(a, b, span);
                }
            }
            (Ty::Record(f1, r1), Ty::Record(f2, r2)) => {
                let (f1, r1) = (f1.clone(), *r1);
                let (f2, r2) = (f2.clone(), *r2);
                self.unify_records(&f1, r1, &f2, r2, span);
            }
            _ => {
                let d1 = self.display_ty(&t1);
                let d2 = self.display_ty(&t2);
                self.error(
                    format!("type mismatch: expected {}, found {}", d1, d2),
                    span,
                );
            }
        }
    }

    fn unify_records(
        &mut self,
        f1: &BTreeMap<String, Ty>,
        r1: Option<TyVar>,
        f2: &BTreeMap<String, Ty>,
        r2: Option<TyVar>,
        span: Span,
    ) {
        let keys1: HashSet<&String> = f1.keys().collect();
        let keys2: HashSet<&String> = f2.keys().collect();

        // Unify common fields
        for key in keys1.intersection(&keys2) {
            let t1 = f1[*key].clone();
            let t2 = f2[*key].clone();
            self.unify(&t1, &t2, span);
        }

        let only1: BTreeMap<String, Ty> = f1
            .iter()
            .filter(|(k, _)| !keys2.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let only2: BTreeMap<String, Ty> = f2
            .iter()
            .filter(|(k, _)| !keys1.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        match (r1, r2) {
            (None, None) => {
                if !only1.is_empty() || !only2.is_empty() {
                    let extras: Vec<_> =
                        only1.keys().chain(only2.keys()).cloned().collect();
                    self.error(
                        format!(
                            "record fields don't match: extra fields {{{}}}",
                            extras.join(", ")
                        ),
                        span,
                    );
                }
            }
            (Some(rv), None) => {
                if !only1.is_empty() {
                    let names: Vec<_> = only1.keys().cloned().collect();
                    self.error(
                        format!(
                            "record has unexpected fields: {{{}}}",
                            names.join(", ")
                        ),
                        span,
                    );
                }
                self.subst.insert(rv, Ty::Record(only2, None));
            }
            (None, Some(rv)) => {
                if !only2.is_empty() {
                    let names: Vec<_> = only2.keys().cloned().collect();
                    self.error(
                        format!(
                            "record has unexpected fields: {{{}}}",
                            names.join(", ")
                        ),
                        span,
                    );
                }
                self.subst.insert(rv, Ty::Record(only1, None));
            }
            (Some(rv1), Some(rv2)) => {
                if rv1 == rv2 {
                    if !only1.is_empty() || !only2.is_empty() {
                        self.error(
                            "record fields don't match".into(),
                            span,
                        );
                    }
                } else {
                    let fresh = self.fresh_var();
                    self.subst
                        .insert(rv1, Ty::Record(only2, Some(fresh)));
                    self.subst
                        .insert(rv2, Ty::Record(only1, Some(fresh)));
                }
            }
        }
    }

    // ── Scheme operations ────────────────────────────────────────

    fn instantiate(&mut self, scheme: &Scheme) -> Ty {
        if scheme.vars.is_empty() {
            return scheme.ty.clone();
        }
        let mapping: HashMap<TyVar, Ty> = scheme
            .vars
            .iter()
            .map(|v| (*v, self.fresh()))
            .collect();
        self.subst_ty(&scheme.ty, &mapping)
    }

    /// Substitute type variables according to a mapping (for instantiation).
    fn subst_ty(&self, ty: &Ty, mapping: &HashMap<TyVar, Ty>) -> Ty {
        match ty {
            Ty::Var(v) => {
                if let Some(replacement) = mapping.get(v) {
                    replacement.clone()
                } else if let Some(resolved) = self.subst.get(v) {
                    self.subst_ty(resolved, mapping)
                } else {
                    ty.clone()
                }
            }
            Ty::Fun(p, r) => Ty::Fun(
                Box::new(self.subst_ty(p, mapping)),
                Box::new(self.subst_ty(r, mapping)),
            ),
            Ty::Record(fields, row) => {
                let new_fields: BTreeMap<_, _> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), self.subst_ty(v, mapping)))
                    .collect();
                let new_row = row.and_then(|rv| {
                    if let Some(replacement) = mapping.get(&rv) {
                        match replacement {
                            Ty::Var(new_rv) => Some(*new_rv),
                            _ => None,
                        }
                    } else {
                        Some(rv)
                    }
                });
                Ty::Record(new_fields, new_row)
            }
            Ty::Relation(inner) => {
                Ty::Relation(Box::new(self.subst_ty(inner, mapping)))
            }
            Ty::Con(name, args) => Ty::Con(
                name.clone(),
                args.iter().map(|a| self.subst_ty(a, mapping)).collect(),
            ),
            _ => ty.clone(),
        }
    }

    fn generalize(&self, ty: &Ty) -> Scheme {
        let applied = self.apply(ty);
        let env_fv = self.free_vars_in_env();
        let ty_fv = self.free_vars(&applied);
        let gen_vars: Vec<TyVar> =
            ty_fv.difference(&env_fv).copied().collect();
        Scheme {
            vars: gen_vars,
            ty: applied,
        }
    }

    fn free_vars(&self, ty: &Ty) -> HashSet<TyVar> {
        let mut s = HashSet::new();
        self.collect_free_vars(ty, &mut s);
        s
    }

    fn collect_free_vars(&self, ty: &Ty, out: &mut HashSet<TyVar>) {
        match ty {
            Ty::Var(v) => match self.subst.get(v) {
                Some(resolved) => self.collect_free_vars(resolved, out),
                None => {
                    out.insert(*v);
                }
            },
            Ty::Fun(p, r) => {
                self.collect_free_vars(p, out);
                self.collect_free_vars(r, out);
            }
            Ty::Record(fields, row) => {
                for v in fields.values() {
                    self.collect_free_vars(v, out);
                }
                if let Some(rv) = row {
                    match self.subst.get(rv) {
                        Some(resolved) => {
                            self.collect_free_vars(resolved, out)
                        }
                        None => {
                            out.insert(*rv);
                        }
                    }
                }
            }
            Ty::Relation(inner) => self.collect_free_vars(inner, out),
            Ty::Con(_, args) => {
                for a in args {
                    self.collect_free_vars(a, out);
                }
            }
            _ => {}
        }
    }

    fn free_vars_in_env(&self) -> HashSet<TyVar> {
        let mut s = HashSet::new();
        for scope in &self.scopes {
            for scheme in scope.values() {
                let mut fv = self.free_vars(&scheme.ty);
                for v in &scheme.vars {
                    fv.remove(v);
                }
                s.extend(fv);
            }
        }
        for ty in self.source_types.values() {
            self.collect_free_vars(ty, &mut s);
        }
        for ty in self.derived_types.values() {
            self.collect_free_vars(ty, &mut s);
        }
        s
    }

    // ── Environment ──────────────────────────────────────────────

    fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    fn pop_scope(&mut self) {
        self.scopes.pop();
    }

    fn bind(&mut self, name: &str, scheme: Scheme) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name.to_string(), scheme);
        }
    }

    fn bind_top(&mut self, name: &str, scheme: Scheme) {
        if let Some(scope) = self.scopes.first_mut() {
            scope.insert(name.to_string(), scheme);
        }
    }

    fn lookup(&self, name: &str) -> Option<&Scheme> {
        for scope in self.scopes.iter().rev() {
            if let Some(scheme) = scope.get(name) {
                return Some(scheme);
            }
        }
        None
    }

    fn lookup_instantiate(&mut self, name: &str) -> Option<Ty> {
        let scheme = self.lookup(name)?.clone();
        Some(self.instantiate(&scheme))
    }

    // ── AST type → Ty ────────────────────────────────────────────

    fn ast_type_to_ty(&mut self, ty: &ast::Type) -> Ty {
        match &ty.node {
            ast::TypeKind::Named(name) => match name.as_str() {
                "Int" => Ty::Int,
                "Float" => Ty::Float,
                "Text" => Ty::Text,
                "Bool" => Ty::Bool,
                _ => {
                    if let Some(aliased) = self.aliases.get(name).cloned() {
                        aliased
                    } else {
                        Ty::Con(name.clone(), vec![])
                    }
                }
            },
            ast::TypeKind::Var(name) => {
                let var = self.annotation_var(name);
                Ty::Var(var)
            }
            ast::TypeKind::Record { fields, rest } => {
                if fields.is_empty() && rest.is_none() {
                    return Ty::unit();
                }
                let field_tys: BTreeMap<String, Ty> = fields
                    .iter()
                    .map(|f| (f.name.clone(), self.ast_type_to_ty(&f.value)))
                    .collect();
                let row_var =
                    rest.as_ref().map(|name| self.annotation_var(name));
                Ty::Record(field_tys, row_var)
            }
            ast::TypeKind::Relation(inner) => {
                Ty::Relation(Box::new(self.ast_type_to_ty(inner)))
            }
            ast::TypeKind::Function { param, result } => Ty::Fun(
                Box::new(self.ast_type_to_ty(param)),
                Box::new(self.ast_type_to_ty(result)),
            ),
            ast::TypeKind::App { func, arg } => {
                if let ast::TypeKind::Named(name) = &func.node {
                    // Associated type applications (e.g. Elem c) are
                    // type-level computations; treat as fresh variables
                    // since HM can't resolve them.
                    if self.assoc_type_names.contains(name) {
                        return self.fresh();
                    }
                    let arg_ty = self.ast_type_to_ty(arg);
                    Ty::Con(name.clone(), vec![arg_ty])
                } else {
                    Ty::Error
                }
            }
            ast::TypeKind::Variant { .. } => Ty::Error,
            ast::TypeKind::Effectful { ty, .. } => self.ast_type_to_ty(ty),
        }
    }

    fn annotation_var(&mut self, name: &str) -> TyVar {
        if let Some(&var) = self.annotation_vars.get(name) {
            var
        } else {
            let var = self.fresh_var();
            self.annotation_vars.insert(name.to_string(), var);
            var
        }
    }

    // ── Type display ─────────────────────────────────────────────

    fn display_ty(&self, ty: &Ty) -> String {
        self.display_ty_inner(ty, false)
    }

    fn display_ty_inner(&self, ty: &Ty, in_fun: bool) -> String {
        match ty {
            Ty::Var(v) => match self.subst.get(v) {
                Some(resolved) => self.display_ty(resolved),
                None => {
                    let idx = *v as usize;
                    if idx < 26 {
                        format!("{}", (b'a' + idx as u8) as char)
                    } else {
                        format!("t{}", v)
                    }
                }
            },
            Ty::Int => "Int".into(),
            Ty::Float => "Float".into(),
            Ty::Text => "Text".into(),
            Ty::Bool => "Bool".into(),
            Ty::Fun(p, r) => {
                let s = format!(
                    "{} -> {}",
                    self.display_ty_inner(p, true),
                    self.display_ty_inner(r, false)
                );
                if in_fun {
                    format!("({})", s)
                } else {
                    s
                }
            }
            Ty::Record(fields, row) => {
                if fields.is_empty() && row.is_none() {
                    return "{}".into();
                }
                let mut parts: Vec<String> = fields
                    .iter()
                    .map(|(n, t)| {
                        format!("{}: {}", n, self.display_ty(t))
                    })
                    .collect();
                if let Some(rv) = row {
                    match self.subst.get(rv) {
                        Some(resolved) => {
                            parts.push(format!(
                                "| {}",
                                self.display_ty(resolved)
                            ));
                        }
                        None => {
                            let idx = *rv as usize;
                            let name = if idx < 26 {
                                format!("{}", (b'a' + idx as u8) as char)
                            } else {
                                format!("r{}", rv)
                            };
                            parts.push(format!("| {}", name));
                        }
                    }
                }
                format!("{{{}}}", parts.join(", "))
            }
            Ty::Relation(inner) => {
                format!("[{}]", self.display_ty(inner))
            }
            Ty::Con(name, args) => {
                if args.is_empty() {
                    name.clone()
                } else {
                    let args_str: Vec<String> =
                        args.iter().map(|a| self.display_ty(a)).collect();
                    format!("{} {}", name, args_str.join(" "))
                }
            }
            Ty::Error => "<error>".into(),
        }
    }

    // ── Constructor instantiation ────────────────────────────────

    /// Returns (data_type, field_record_type) with fresh vars for params.
    fn instantiate_ctor(
        &mut self,
        name: &str,
        _span: Span,
    ) -> Option<(Ty, Ty)> {
        let info = self.constructors.get(name)?.clone();

        self.annotation_vars.clear();
        let param_tys: Vec<Ty> = info
            .data_params
            .iter()
            .map(|p| {
                let v = self.fresh_var();
                self.annotation_vars.insert(p.clone(), v);
                Ty::Var(v)
            })
            .collect();

        let field_tys: BTreeMap<String, Ty> = info
            .fields
            .iter()
            .map(|(name, ty)| (name.clone(), self.ast_type_to_ty(ty)))
            .collect();

        let data_ty = Ty::Con(info.data_type.clone(), param_tys);
        let record_ty = Ty::Record(field_tys, None);

        Some((data_ty, record_ty))
    }

    // ── Expression inference ─────────────────────────────────────

    fn infer_expr(&mut self, expr: &ast::Expr) -> Ty {
        match &expr.node {
            ast::ExprKind::Lit(lit) => self.literal_type(lit),

            ast::ExprKind::Var(name) => {
                if let Some(ty) = self.lookup_instantiate(name) {
                    ty
                } else {
                    self.error(
                        format!("undefined variable '{}'", name),
                        expr.span,
                    );
                    Ty::Error
                }
            }

            ast::ExprKind::Constructor(name) => {
                if let Some((data_ty, record_ty)) =
                    self.instantiate_ctor(name, expr.span)
                {
                    Ty::Fun(Box::new(record_ty), Box::new(data_ty))
                } else {
                    self.error(
                        format!("unknown constructor '{}'", name),
                        expr.span,
                    );
                    Ty::Error
                }
            }

            ast::ExprKind::SourceRef(name) => {
                if let Some(ty) = self.source_types.get(name).cloned() {
                    ty
                } else {
                    self.error(
                        format!("unknown source relation '*{}'", name),
                        expr.span,
                    );
                    Ty::Error
                }
            }

            ast::ExprKind::DerivedRef(name) => {
                if let Some(ty) = self.derived_types.get(name).cloned() {
                    ty
                } else {
                    self.error(
                        format!("unknown derived relation '&{}'", name),
                        expr.span,
                    );
                    Ty::Error
                }
            }

            ast::ExprKind::Record(fields) => {
                if fields.is_empty() {
                    return Ty::unit();
                }
                let field_tys: BTreeMap<String, Ty> = fields
                    .iter()
                    .map(|f| {
                        (f.name.clone(), self.infer_expr(&f.value))
                    })
                    .collect();
                Ty::Record(field_tys, None)
            }

            ast::ExprKind::RecordUpdate { base, fields } => {
                let base_ty = self.infer_expr(base);
                let mut update_fields = BTreeMap::new();
                for field in fields {
                    let val_ty = self.infer_expr(&field.value);
                    update_fields.insert(field.name.clone(), val_ty);
                }
                let rv = self.fresh_var();
                let constraint = Ty::Record(update_fields, Some(rv));
                self.unify(&base_ty, &constraint, expr.span);
                base_ty
            }

            ast::ExprKind::FieldAccess { expr: e, field } => {
                let expr_ty = self.infer_expr(e);
                let field_ty = self.fresh();
                let rv = self.fresh_var();
                let constraint = Ty::Record(
                    BTreeMap::from([(field.clone(), field_ty.clone())]),
                    Some(rv),
                );
                self.unify(&expr_ty, &constraint, e.span);
                field_ty
            }

            ast::ExprKind::List(elems) => {
                let elem_ty = self.fresh();
                for e in elems {
                    let t = self.infer_expr(e);
                    self.unify(&elem_ty, &t, e.span);
                }
                Ty::Relation(Box::new(elem_ty))
            }

            ast::ExprKind::Lambda { params, body } => {
                self.push_scope();
                let mut param_types = Vec::new();
                for param in params {
                    let t = self.fresh();
                    self.check_pattern(param, &t);
                    param_types.push(t);
                }
                let body_ty = self.infer_expr(body);
                self.pop_scope();

                let mut result = body_ty;
                for pt in param_types.into_iter().rev() {
                    result = Ty::Fun(Box::new(pt), Box::new(result));
                }
                result
            }

            ast::ExprKind::App { func, arg } => {
                let func_ty = self.infer_expr(func);
                let arg_ty = self.infer_expr(arg);
                let result_ty = self.fresh();
                let expected = Ty::Fun(
                    Box::new(arg_ty),
                    Box::new(result_ty.clone()),
                );
                self.unify(&func_ty, &expected, expr.span);
                result_ty
            }

            ast::ExprKind::BinOp { op, lhs, rhs } => {
                self.infer_binop(*op, lhs, rhs, expr.span)
            }

            ast::ExprKind::UnaryOp { op, operand } => {
                let operand_ty = self.infer_expr(operand);
                match op {
                    ast::UnaryOp::Neg => {
                        // numeric negation — result same type as operand
                        operand_ty
                    }
                    ast::UnaryOp::Not => {
                        self.unify(
                            &operand_ty,
                            &Ty::Bool,
                            operand.span,
                        );
                        Ty::Bool
                    }
                }
            }

            ast::ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } => {
                let cond_ty = self.infer_expr(cond);
                self.unify(&cond_ty, &Ty::Bool, cond.span);
                let then_ty = self.infer_expr(then_branch);
                let else_ty = self.infer_expr(else_branch);
                self.unify(&then_ty, &else_ty, expr.span);
                then_ty
            }

            ast::ExprKind::Case { scrutinee, arms } => {
                let scrut_ty = self.infer_expr(scrutinee);
                let result_ty = self.fresh();

                for arm in arms {
                    self.push_scope();
                    self.check_pattern(&arm.pat, &scrut_ty);
                    let body_ty = self.infer_expr(&arm.body);
                    self.unify(&result_ty, &body_ty, arm.body.span);
                    self.pop_scope();
                }

                self.check_exhaustiveness(&scrut_ty, arms, expr.span);

                result_ty
            }

            ast::ExprKind::Do(stmts) => self.infer_do(stmts, expr.span),

            ast::ExprKind::Yield(inner) => {
                let inner_ty = self.infer_expr(inner);
                Ty::Relation(Box::new(inner_ty))
            }

            ast::ExprKind::Set { target, value } => {
                let is_view = matches!(&target.node,
                    ast::ExprKind::SourceRef(n) if self.view_names.contains(n));
                if is_view {
                    // View writes have constant columns auto-filled by
                    // codegen — skip type-checking the value expression
                    // since its type intentionally differs from the view's
                    // read type.
                } else {
                    let target_ty = self.infer_expr(target);
                    let value_ty = self.infer_expr(value);
                    self.unify(&target_ty, &value_ty, expr.span);
                }
                Ty::unit()
            }

            ast::ExprKind::FullSet { target, value } => {
                let is_view = matches!(&target.node,
                    ast::ExprKind::SourceRef(n) if self.view_names.contains(n));
                if is_view {
                    // Same as Set: skip view write checking.
                } else {
                    let target_ty = self.infer_expr(target);
                    let value_ty = self.infer_expr(value);
                    self.unify(&target_ty, &value_ty, expr.span);
                }
                Ty::unit()
            }

            ast::ExprKind::Atomic(inner) => self.infer_expr(inner),

            ast::ExprKind::At { relation, time } => {
                let rel_ty = self.infer_expr(relation);
                let time_ty = self.infer_expr(time);
                self.unify(&time_ty, &Ty::Int, time.span);
                rel_ty
            }
        }
    }

    fn infer_binop(
        &mut self,
        op: ast::BinOp,
        lhs: &ast::Expr,
        rhs: &ast::Expr,
        span: Span,
    ) -> Ty {
        let lhs_ty = self.infer_expr(lhs);
        let rhs_ty = self.infer_expr(rhs);

        match op {
            // Arithmetic: both same type, result same type
            ast::BinOp::Add
            | ast::BinOp::Sub
            | ast::BinOp::Mul
            | ast::BinOp::Div => {
                self.unify(&lhs_ty, &rhs_ty, span);
                lhs_ty
            }
            // Comparison: both same type, result Bool
            ast::BinOp::Eq
            | ast::BinOp::Neq
            | ast::BinOp::Lt
            | ast::BinOp::Gt
            | ast::BinOp::Le
            | ast::BinOp::Ge => {
                self.unify(&lhs_ty, &rhs_ty, span);
                Ty::Bool
            }
            // Boolean: both Bool, result Bool
            ast::BinOp::And | ast::BinOp::Or => {
                self.unify(&lhs_ty, &Ty::Bool, lhs.span);
                self.unify(&rhs_ty, &Ty::Bool, rhs.span);
                Ty::Bool
            }
            // Concat: both Text, result Text
            ast::BinOp::Concat => {
                self.unify(&lhs_ty, &Ty::Text, lhs.span);
                self.unify(&rhs_ty, &Ty::Text, rhs.span);
                Ty::Text
            }
            // Pipe: a |> f  =  f a
            ast::BinOp::Pipe => {
                let result_ty = self.fresh();
                let fun_ty = Ty::Fun(
                    Box::new(lhs_ty),
                    Box::new(result_ty.clone()),
                );
                self.unify(&rhs_ty, &fun_ty, span);
                result_ty
            }
        }
    }

    fn literal_type(&self, lit: &ast::Literal) -> Ty {
        match lit {
            ast::Literal::Int(_) => Ty::Int,
            ast::Literal::Float(_) => Ty::Float,
            ast::Literal::Text(_) => Ty::Text,
        }
    }

    // ── Pattern checking ─────────────────────────────────────────

    fn check_pattern(&mut self, pat: &ast::Pat, expected: &Ty) {
        match &pat.node {
            ast::PatKind::Var(name) => {
                self.bind(name, Scheme::mono(expected.clone()));
            }
            ast::PatKind::Wildcard => {}
            ast::PatKind::Constructor { name, payload } => {
                if let Some((data_ty, record_ty)) =
                    self.instantiate_ctor(name, pat.span)
                {
                    self.unify(expected, &data_ty, pat.span);
                    self.check_pattern(payload, &record_ty);
                } else {
                    self.error(
                        format!(
                            "unknown constructor '{}' in pattern",
                            name
                        ),
                        pat.span,
                    );
                }
            }
            ast::PatKind::Record(field_pats) => {
                let mut field_types = BTreeMap::new();
                for fp in field_pats {
                    let ft = self.fresh();
                    field_types.insert(fp.name.clone(), ft.clone());
                    if let Some(p) = &fp.pattern {
                        self.check_pattern(p, &ft);
                    } else {
                        // Punned: {name} → bind variable 'name' to field type
                        self.bind(&fp.name, Scheme::mono(ft));
                    }
                }
                let row_var = self.fresh_var();
                let record_ty = Ty::Record(field_types, Some(row_var));
                self.unify(expected, &record_ty, pat.span);
            }
            ast::PatKind::Lit(lit) => {
                let lit_ty = self.literal_type(lit);
                self.unify(expected, &lit_ty, pat.span);
            }
            ast::PatKind::List(pats) => {
                let elem_ty = self.fresh();
                for p in pats {
                    self.check_pattern(p, &elem_ty);
                }
                let list_ty = Ty::Relation(Box::new(elem_ty));
                self.unify(expected, &list_ty, pat.span);
            }
        }
    }

    // ── Exhaustiveness checking ────────────────────────────────

    /// Check that a case expression covers all constructors of the
    /// scrutinee's type.  Emits an error listing missing patterns when
    /// the match is non-exhaustive.
    fn check_exhaustiveness(
        &mut self,
        scrut_ty: &Ty,
        arms: &[ast::CaseArm],
        span: Span,
    ) {
        // Resolve the scrutinee type through substitution.
        let resolved = self.apply(scrut_ty);

        // Only check ADTs — primitives (Int, Text, etc.) have infinite
        // domains and can't be exhaustively matched by constructors.
        let type_name = match &resolved {
            Ty::Con(name, _) => name.clone(),
            _ => return,
        };

        let data_info = match self.data_types.get(&type_name) {
            Some(info) => info.clone(),
            None => return,
        };

        // If any arm has an unconditional catch-all pattern (wildcard or
        // variable) at the top level, the match is trivially exhaustive.
        let has_catchall = arms.iter().any(|arm| {
            matches!(
                &arm.pat.node,
                ast::PatKind::Wildcard | ast::PatKind::Var(_)
            )
        });
        if has_catchall {
            return;
        }

        // Collect which constructors are covered by the arms.
        let covered: HashSet<&str> = arms
            .iter()
            .filter_map(|arm| match &arm.pat.node {
                ast::PatKind::Constructor { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect();

        let all_ctors: Vec<&str> =
            data_info.ctors.iter().map(|(n, _)| n.as_str()).collect();

        let missing: Vec<&str> = all_ctors
            .iter()
            .copied()
            .filter(|c| !covered.contains(c))
            .collect();

        if missing.is_empty() {
            return;
        }

        let missing_list = missing.join(", ");
        self.error(
            format!(
                "non-exhaustive pattern match — missing: {}",
                missing_list,
            ),
            span,
        );
    }

    // ── Do-block inference ───────────────────────────────────────

    fn infer_do(&mut self, stmts: &[ast::Stmt], _span: Span) -> Ty {
        self.push_scope();
        let mut yield_ty: Option<Ty> = None;

        for stmt in stmts {
            match &stmt.node {
                ast::StmtKind::Bind { pat, expr } => {
                    let expr_ty = self.infer_expr(expr);
                    let resolved = self.apply(&expr_ty);
                    let is_ctor_pat =
                        matches!(&pat.node, ast::PatKind::Constructor { .. });

                    if is_ctor_pat
                        && !matches!(&resolved, Ty::Relation(_) | Ty::Var(_))
                    {
                        // Value pattern match: `Constructor pat <- value_expr`
                        // Filters the enclosing iteration (skip if no match)
                        self.check_pattern(pat, &expr_ty);
                    } else {
                        // Normal relation bind
                        let elem_ty = self.fresh();
                        self.unify(
                            &expr_ty,
                            &Ty::Relation(Box::new(elem_ty.clone())),
                            expr.span,
                        );
                        self.check_pattern(pat, &elem_ty);
                    }
                }
                ast::StmtKind::Let { pat, expr } => {
                    let expr_ty = self.infer_expr(expr);
                    self.check_pattern(pat, &expr_ty);
                }
                ast::StmtKind::Where { cond } => {
                    let cond_ty = self.infer_expr(cond);
                    self.unify(&cond_ty, &Ty::Bool, cond.span);
                }
                ast::StmtKind::Expr(expr) => {
                    if let ast::ExprKind::Yield(inner) = &expr.node {
                        let inner_ty = self.infer_expr(inner);
                        if let Some(ref yt) = yield_ty {
                            let yt = yt.clone();
                            self.unify(&yt, &inner_ty, expr.span);
                        } else {
                            yield_ty = Some(inner_ty);
                        }
                    } else {
                        let _ = self.infer_expr(expr);
                    }
                }
            }
        }

        self.pop_scope();

        match yield_ty {
            Some(ty) => Ty::Relation(Box::new(ty)),
            None => Ty::Relation(Box::new(Ty::unit())),
        }
    }

    // ── Declaration collection (phase 1) ─────────────────────────

    fn collect_types(&mut self, module: &ast::Module) {
        // First pass: type aliases
        for decl in &module.decls {
            if let ast::DeclKind::TypeAlias { name, params, ty } =
                &decl.node
            {
                if params.is_empty() {
                    self.annotation_vars.clear();
                    let resolved = self.ast_type_to_ty(ty);
                    self.aliases.insert(name.clone(), resolved);
                }
            }
        }

        // Second pass: data types and constructors
        for decl in &module.decls {
            if let ast::DeclKind::Data {
                name,
                params,
                constructors: ctors,
                ..
            } = &decl.node
            {
                let mut ctor_list = Vec::new();
                for ctor in ctors {
                    let fields: Vec<(String, ast::Type)> = ctor
                        .fields
                        .iter()
                        .map(|f| (f.name.clone(), f.value.clone()))
                        .collect();
                    self.constructors.insert(
                        ctor.name.clone(),
                        CtorInfo {
                            data_type: name.clone(),
                            data_params: params.clone(),
                            fields: fields.clone(),
                        },
                    );
                    ctor_list.push((ctor.name.clone(), fields));
                }

                // For single-variant data types, also register as alias
                if ctors.len() == 1 {
                    self.annotation_vars.clear();
                    for p in params {
                        let v = self.fresh_var();
                        self.annotation_vars.insert(p.clone(), v);
                    }
                    let field_tys: BTreeMap<String, Ty> = ctors[0]
                        .fields
                        .iter()
                        .map(|f| {
                            (
                                f.name.clone(),
                                self.ast_type_to_ty(&f.value),
                            )
                        })
                        .collect();
                    if params.is_empty() {
                        self.aliases.insert(
                            name.clone(),
                            Ty::Record(field_tys, None),
                        );
                    }
                }

                self.data_types.insert(
                    name.clone(),
                    DataInfo {
                        params: params.clone(),
                        ctors: ctor_list,
                    },
                );
            }
        }

        // Third pass: collect associated type names from traits
        for decl in &module.decls {
            if let ast::DeclKind::Trait { items, .. } = &decl.node {
                for item in items {
                    if let ast::TraitItem::AssociatedType { name, .. } =
                        item
                    {
                        self.assoc_type_names.insert(name.clone());
                    }
                }
            }
        }
    }

    // ── Source/view collection (phase 2) ──────────────────────────

    fn collect_sources(&mut self, module: &ast::Module) {
        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Source { name, ty, .. } => {
                    self.annotation_vars.clear();
                    let resolved = self.ast_type_to_ty(ty);
                    self.source_types.insert(name.clone(), resolved);
                }
                ast::DeclKind::View { name, .. } => {
                    let elem = self.fresh();
                    self.source_types.insert(
                        name.clone(),
                        Ty::Relation(Box::new(elem)),
                    );
                    self.view_names.insert(name.clone());
                }
                ast::DeclKind::Derived { name, .. } => {
                    let ty = self.fresh();
                    self.derived_types.insert(name.clone(), ty);
                }
                _ => {}
            }
        }
    }

    // ── Pre-registration (phase 3) ───────────────────────────────

    fn pre_register(&mut self, module: &ast::Module) {
        // Register built-in functions
        self.register_builtins();

        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Fun { name, ty, .. } => {
                    if let Some(scheme) = ty {
                        self.annotation_vars.clear();
                        let ty = self.ast_type_to_ty(&scheme.ty);
                        let vars: Vec<TyVar> =
                            self.annotation_vars.values().copied().collect();
                        self.bind_top(name, Scheme { vars, ty });
                    } else {
                        let var = self.fresh();
                        self.bind_top(name, Scheme::mono(var));
                    }
                }
                ast::DeclKind::Trait { items, params, .. } => {
                    self.register_trait_methods(params, items);
                }
                _ => {}
            }
        }
    }

    fn register_builtins(&mut self) {
        // println : ∀a. a -> {}
        let a = self.fresh_var();
        self.bind_top(
            "println",
            Scheme {
                vars: vec![a],
                ty: Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::unit())),
            },
        );

        // print : ∀a. a -> {}
        let a = self.fresh_var();
        self.bind_top(
            "print",
            Scheme {
                vars: vec![a],
                ty: Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::unit())),
            },
        );

        // show : ∀a. a -> Text
        let a = self.fresh_var();
        self.bind_top(
            "show",
            Scheme {
                vars: vec![a],
                ty: Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::Text)),
            },
        );

        // union : ∀a. [a] -> [a] -> [a]
        let a = self.fresh_var();
        self.bind_top(
            "union",
            Scheme {
                vars: vec![a],
                ty: Ty::Fun(
                    Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    Box::new(Ty::Fun(
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                        Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    )),
                ),
            },
        );

        // count : ∀a. [a] -> Int
        let a = self.fresh_var();
        self.bind_top(
            "count",
            Scheme {
                vars: vec![a],
                ty: Ty::Fun(
                    Box::new(Ty::Relation(Box::new(Ty::Var(a)))),
                    Box::new(Ty::Int),
                ),
            },
        );

        // putLine : ∀a. a -> {} (alias for println)
        let a = self.fresh_var();
        self.bind_top(
            "putLine",
            Scheme {
                vars: vec![a],
                ty: Ty::Fun(Box::new(Ty::Var(a)), Box::new(Ty::unit())),
            },
        );

        // now : Int (current time in milliseconds since epoch)
        self.bind_top(
            "now",
            Scheme::mono(Ty::Int),
        );
    }

    fn register_trait_methods(
        &mut self,
        params: &[ast::TraitParam],
        items: &[ast::TraitItem],
    ) {
        for item in items {
            if let ast::TraitItem::Method { name, ty, .. } = item {
                // Skip default-body entries with placeholder types
                if let ast::TypeKind::Named(n) = &ty.ty.node {
                    if n == "_" {
                        continue;
                    }
                }
                self.annotation_vars.clear();
                // Register trait params as annotation vars
                for p in params {
                    self.annotation_var(&p.name);
                }
                let method_ty = self.ast_type_to_ty(&ty.ty);
                let vars: Vec<TyVar> =
                    self.annotation_vars.values().copied().collect();
                self.bind_top(
                    name,
                    Scheme {
                        vars,
                        ty: method_ty,
                    },
                );
            }
        }
    }

    // ── Declaration inference (phase 4) ──────────────────────────

    fn infer_declarations(&mut self, module: &ast::Module) {
        for decl in &module.decls {
            match &decl.node {
                ast::DeclKind::Fun { name, body, .. } => {
                    let expected =
                        self.lookup_instantiate(name).unwrap_or_else(|| {
                            self.fresh()
                        });
                    let inferred = self.infer_expr(body);
                    self.unify(&expected, &inferred, body.span);

                    // Remove the old monomorphic binding before
                    // generalizing, so its free variables don't block
                    // quantification.
                    if let Some(scope) = self.scopes.first_mut() {
                        scope.remove(name.as_str());
                    }
                    let applied = self.apply(&inferred);
                    let scheme = self.generalize(&applied);
                    self.bind_top(name, scheme);
                }
                ast::DeclKind::View { name, body, .. } => {
                    let expected =
                        self.source_types.get(name).cloned().unwrap_or_else(
                            || self.fresh(),
                        );
                    let inferred = self.infer_expr(body);
                    self.unify(&expected, &inferred, body.span);
                }
                ast::DeclKind::Derived { name, body, .. } => {
                    let expected = self
                        .derived_types
                        .get(name)
                        .cloned()
                        .unwrap_or_else(|| self.fresh());
                    let inferred = self.infer_expr(body);
                    self.unify(&expected, &inferred, body.span);
                }
                ast::DeclKind::Impl {
                    trait_name,
                    items,
                    ..
                } => {
                    self.check_impl_items(trait_name, items);
                }
                _ => {}
            }
        }
    }

    fn check_impl_items(
        &mut self,
        _trait_name: &str,
        items: &[ast::ImplItem],
    ) {
        for item in items {
            if let ast::ImplItem::Method {
                name, params, body, ..
            } = item
            {
                // Type-check each impl method body
                self.push_scope();
                let mut param_types = Vec::new();
                for param in params {
                    let t = self.fresh();
                    self.check_pattern(param, &t);
                    param_types.push(t);
                }
                let _ = self.infer_expr(body);
                self.pop_scope();

                // If there's a known trait method type, we could unify
                // but for now just check the body is well-typed
                let _ = name;
            }
        }
    }

    // ── Error conversion ─────────────────────────────────────────

    fn to_diagnostics(&self) -> Vec<Diagnostic> {
        self.errors
            .iter()
            .map(|(msg, span)| {
                Diagnostic::error(msg.clone()).label(*span, msg.clone())
            })
            .collect()
    }
}

// ── Public API ────────────────────────────────────────────────────

/// Run type inference on a parsed module. Returns diagnostics for any
/// type errors found. An empty result means the program is well-typed.
pub fn check(module: &ast::Module) -> Vec<Diagnostic> {
    let mut infer = Infer::new();

    // Phase 1: Collect type aliases, data types, constructors
    infer.collect_types(module);

    // Phase 2: Register source/view/derived relation types
    infer.collect_sources(module);

    // Phase 3: Pre-register top-level names (builtins, functions, trait methods)
    infer.pre_register(module);

    // Phase 4: Infer all declaration bodies
    infer.infer_declarations(module);

    infer.to_diagnostics()
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(src: &str) -> ast::Module {
        let lexer = knot::lexer::Lexer::new(src);
        let (tokens, _) = lexer.tokenize();
        let parser = knot::parser::Parser::new(src.to_string(), tokens);
        let (module, _) = parser.parse_module();
        module
    }

    fn check_src(src: &str) -> Vec<Diagnostic> {
        check(&parse(src))
    }

    fn has_error(diags: &[Diagnostic], needle: &str) -> bool {
        diags.iter().any(|d| d.message.contains(needle))
    }

    #[test]
    fn literal_arithmetic() {
        assert!(check_src("main = 1 + 2").is_empty());
    }

    #[test]
    fn arithmetic_type_mismatch() {
        let diags = check_src("main = 1 + \"hello\"");
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn boolean_ops_require_bool() {
        let diags = check_src("main = 1 && 2");
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn concat_requires_text() {
        assert!(check_src("main = \"a\" ++ \"b\"").is_empty());
        let diags = check_src("main = 1 ++ 2");
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn if_branches_must_agree() {
        assert!(check_src(
            "main = if 1 == 1 then 42 else 0"
        )
        .is_empty());
        let diags = check_src(
            "main = if 1 == 1 then 42 else \"hello\"",
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn field_access_on_record() {
        assert!(check_src("main = {name: \"Alice\"}.name").is_empty());
    }

    #[test]
    fn field_access_nonexistent() {
        let diags = check_src("main = {name: \"Alice\"}.age");
        assert!(has_error(&diags, "unexpected fields"));
    }

    #[test]
    fn lambda_inference() {
        assert!(check_src("f = \\x -> x + 1\nmain = f 42").is_empty());
    }

    #[test]
    fn let_polymorphism() {
        // id should work with both Int and Text
        assert!(check_src(
            "id = \\x -> x\nmain = do\n  println (id 42)\n  println (id \"hello\")\n  yield {}"
        ).is_empty());
    }

    #[test]
    fn recursive_function() {
        assert!(check_src(
            "fac = \\n -> if n == 0 then 1 else n * fac (n - 1)\nmain = fac 5"
        ).is_empty());
    }

    #[test]
    fn row_polymorphism() {
        // getName should work on any record with a name field
        assert!(check_src(
            "getName = \\r -> r.name\nmain = do\n  let x = getName {name: \"A\", age: 1}\n  let y = getName {name: \"B\", email: \"b\"}\n  yield {}"
        ).is_empty());
    }

    #[test]
    fn data_type_constructor() {
        assert!(check_src(
            "data Shape = Circle {radius: Int} | Rect {w: Int, h: Int}\nmain = Circle {radius: 5}"
        ).is_empty());
    }

    #[test]
    fn case_expression() {
        assert!(check_src(
            "data Shape = Circle {r: Int} | Rect {w: Int, h: Int}\nf = \\s -> case s of\n  Circle {r} -> r\n  Rect {w, h} -> w * h\nmain = f (Circle {r: 5})"
        ).is_empty());
    }

    #[test]
    fn case_branch_type_mismatch() {
        let diags = check_src(
            "data AB = A {} | B {}\nf = \\x -> case x of\n  A {} -> 42\n  B {} -> \"hello\"\nmain = f (A {})"
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn do_block_bind() {
        assert!(check_src(
            "*people : [{name: Text, age: Int}]\nmain = do\n  p <- *people\n  yield p.name"
        ).is_empty());
    }

    #[test]
    fn do_block_where_bool() {
        let diags = check_src(
            "*people : [{name: Text}]\nmain = do\n  p <- *people\n  where p.name\n  yield p"
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn undefined_variable() {
        let diags = check_src("main = undefined_var");
        assert!(has_error(&diags, "undefined variable"));
    }

    #[test]
    fn pipe_operator() {
        assert!(check_src(
            "inc = \\x -> x + 1\nmain = 5 |> inc"
        ).is_empty());
    }

    #[test]
    fn set_type_matches_source() {
        assert!(check_src(
            "*nums : [Int]\nmain = set *nums = [1, 2, 3]"
        ).is_empty());
    }

    #[test]
    fn set_type_mismatch() {
        let diags = check_src(
            "*nums : [Int]\nmain = set *nums = [\"a\", \"b\"]"
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    #[test]
    fn union_builtin() {
        assert!(check_src(
            "main = union [1, 2] [3, 4]"
        ).is_empty());
    }

    #[test]
    fn count_builtin() {
        assert!(check_src("main = count [1, 2, 3]").is_empty());
    }

    #[test]
    fn trait_method() {
        assert!(check_src(
            "trait Show a where\n  show_ : a -> Text\nimpl Show Int where\n  show_ n = \"int\"\nmain = show_ 42"
        ).is_empty());
    }

    #[test]
    fn record_update() {
        assert!(check_src(
            "main = {name: \"Alice\", age: 30} |> \\r -> {r | age: r.age + 1}"
        ).is_empty());
    }

    #[test]
    fn higher_order_function() {
        assert!(check_src(
            "apply = \\f x -> f x\nmain = apply (\\x -> x + 1) 5"
        ).is_empty());
    }

    #[test]
    fn now_builtin() {
        // now should type as Int
        assert!(check_src("main = now + 1000").is_empty());
    }

    #[test]
    fn temporal_at_expression() {
        // @(timestamp) on a source should preserve the relation type
        assert!(check_src(
            "*people : [{name: Text, age: Int}]\nmain = *people @(now)"
        ).is_empty());
    }

    #[test]
    fn temporal_at_requires_int_time() {
        // time expression must be Int, not Text
        let diags = check_src(
            "*people : [{name: Text}]\nmain = *people @(\"yesterday\")"
        );
        assert!(has_error(&diags, "type mismatch"));
    }

    // ── Exhaustiveness checking ─────────────────────────────────

    #[test]
    fn exhaustive_case_all_constructors() {
        // Covering all constructors is fine.
        assert!(check_src(
            "data Shape = Circle {r: Int} | Rect {w: Int, h: Int}\n\
             f = \\s -> case s of\n  Circle {r} -> r\n  Rect {w, h} -> w * h\n\
             main = f (Circle {r: 5})"
        ).is_empty());
    }

    #[test]
    fn exhaustive_case_wildcard() {
        // A wildcard catch-all makes any match exhaustive.
        assert!(check_src(
            "data Shape = Circle {r: Int} | Rect {w: Int, h: Int}\n\
             f = \\s -> case s of\n  Circle {r} -> r\n  _ -> 0\n\
             main = f (Circle {r: 5})"
        ).is_empty());
    }

    #[test]
    fn exhaustive_case_var_catchall() {
        // A variable catch-all also makes it exhaustive.
        assert!(check_src(
            "data Shape = Circle {r: Int} | Rect {w: Int, h: Int}\n\
             f = \\s -> case s of\n  Circle {r} -> r\n  other -> 0\n\
             main = f (Circle {r: 5})"
        ).is_empty());
    }

    #[test]
    fn non_exhaustive_case_missing_constructor() {
        // Missing Rect — should produce an error.
        let diags = check_src(
            "data Shape = Circle {r: Int} | Rect {w: Int, h: Int}\n\
             f = \\s -> case s of\n  Circle {r} -> r\n\
             main = f (Circle {r: 5})"
        );
        assert!(has_error(&diags, "non-exhaustive"));
        assert!(has_error(&diags, "Rect"));
    }

    #[test]
    fn non_exhaustive_case_missing_multiple() {
        // Missing two out of three constructors.
        let diags = check_src(
            "data Color = Red {} | Green {} | Blue {}\n\
             f = \\c -> case c of\n  Red {} -> 1\n\
             main = f (Red {})"
        );
        assert!(has_error(&diags, "non-exhaustive"));
        assert!(has_error(&diags, "Green"));
        assert!(has_error(&diags, "Blue"));
    }

    #[test]
    fn exhaustive_case_single_constructor() {
        // Data type with one constructor — a single arm is exhaustive.
        assert!(check_src(
            "data Wrapper = Wrap {val: Int}\n\
             f = \\w -> case w of\n  Wrap {val} -> val\n\
             main = f (Wrap {val: 42})"
        ).is_empty());
    }

    #[test]
    fn case_on_primitive_skips_exhaustiveness() {
        // Matching on Int — no exhaustiveness check (infinite domain).
        assert!(check_src(
            "f = \\n -> case n of\n  0 -> 1\n  1 -> 2\n\
             main = f 0"
        ).is_empty());
    }
}
