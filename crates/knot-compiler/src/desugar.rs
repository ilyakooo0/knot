//! Do-block desugaring pass.
//!
//! Transforms "pure comprehension" do blocks into nested calls to
//! `__bind`, `yield`, and `[]` (empty). A pure comprehension is a do
//! block whose statements are all Bind, Where, Let, or a final Yield —
//! no bare side-effecting expressions like `set` or `println`.
//!
//! Desugaring rules (processing right-to-left):
//!
//! ```text
//! [yield e]                         =>  Yield(e)
//! [x <- e, ...rest]                 =>  App(App(__bind, \x -> desugar(rest)), e)
//! [Ctor pat <- e, ...rest]          =>  App(App(__bind, \__m -> case __m of
//!                                           Ctor pat -> desugar(rest)
//!                                           _        -> []), e)
//! [where cond, ...rest]             =>  App(App(__bind, \_ -> desugar(rest)),
//!                                           if cond then yield {} else [])
//! [let pat = e, ...rest]            =>  (\pat -> desugar(rest)) e
//! ```
//!
//! Do blocks that are direct values of Set/ReplaceSet are NOT desugared
//! (to preserve SQL optimization patterns in codegen).

use knot::ast::*;
use std::collections::HashSet;

/// Desugar the file's expression in place. Transforms pure-comprehension do
/// blocks into nested bind/yield/empty expressions.
///
/// Runs on a grown stack: `desugar_stmts` recurses once per do-block
/// statement to build the bind chain, and the walkers then descend it.
pub fn desugar(expr: &mut Expr) {
    crate::stack::grow(|| desugar_inner(expr))
}

fn desugar_inner(expr: &mut Expr) {
    let io_fns = detect_io_functions(expr);
    let no_source_vars = HashSet::new();
    // Rewrite `rec.*name` field access on a static source-record
    // (`db = { *todos : [Todo], … }`) to a plain `SourceRef(name)`. The record
    // value is erased to unit at runtime and every downstream source-read
    // path (do-block binds, `count`, SQL pushdown, STM tracking) keys on
    // `SourceRef` — routing through them unchanged is far less invasive than
    // teaching each to recognise `FieldAccess` on a source-record.
    rewrite_record_source_refs(expr);
    desugar_expr(expr, &io_fns, &no_source_vars);
}

/// Map a `with`-bound record-variable name to the relations its record
/// literal declares via `*name` / `&name` fields, tagged by kind.
fn collect_record_source_fields(
    expr: &Expr,
) -> std::collections::HashMap<String, Vec<(String, RecordRelKind)>> {
    let mut out = std::collections::HashMap::new();
    collect_source_fields_in_expr(expr, &mut out);
    out
}

fn collect_source_fields_in_expr(
    expr: &Expr,
    out: &mut std::collections::HashMap<String, Vec<(String, RecordRelKind)>>,
) {
    if let ExprKind::With { record, body } = &expr.node
        && let ExprKind::Record(fields) = &record.node
    {
        let names: Vec<(String, RecordRelKind)> = fields
            .iter()
            .filter_map(|f| match &f.value.node {
                ExprKind::SourceDecl { name, .. } => {
                    Some((name.clone(), RecordRelKind::Source))
                }
                ExprKind::ViewDecl { name, .. } => Some((name.clone(), RecordRelKind::Source)),
                ExprKind::DerivedDecl { name, .. } => {
                    Some((name.clone(), RecordRelKind::Derived))
                }
                _ => None,
            })
            .collect();
        if !names.is_empty() {
            // The record variable is the With record itself; but a With has no
            // variable name — its fields are scoped directly over the body.
            // Field-path source refs arise on NAMED record bindings
            // (`db = {…}; db.*todos`), handled below.
            let _ = names;
        }
        collect_source_fields_in_expr(record, out);
        collect_source_fields_in_expr(body, out);
        return;
    }
    walk_expr_children_read(expr, &mut |child| collect_source_fields_in_expr(child, out));
}

/// Read-only variant of `walk_expr_children`.
fn walk_expr_children_read(expr: &Expr, f: &mut impl FnMut(&Expr)) {
    match &expr.node {
        ExprKind::App { func, arg } => {
            f(func);
            f(arg);
        }
        ExprKind::Lambda { body, .. } => f(body),
        ExprKind::Record(fields) => {
            for field in fields {
                f(&field.value);
            }
        }
        ExprKind::RecordUpdate { base, fields } => {
            f(base);
            for field in fields {
                f(&field.value);
            }
        }
        ExprKind::FieldAccess { expr: e, .. } => f(e),
        ExprKind::List(elems) => {
            for e in elems {
                f(e);
            }
        }
        ExprKind::With { record, body } => {
            f(record);
            f(body);
        }
        ExprKind::BinOp { lhs, rhs, .. } => {
            f(lhs);
            f(rhs);
        }
        ExprKind::UnaryOp { operand, .. } => f(operand),
        ExprKind::If { cond, then_branch, else_branch } => {
            f(cond);
            f(then_branch);
            f(else_branch);
        }
        ExprKind::Case { scrutinee, arms } => {
            f(scrutinee);
            for arm in arms {
                f(&arm.body);
            }
        }
        ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    StmtKind::Bind { expr: e, .. } => f(e),
                    StmtKind::Where { cond } => f(cond),
                    StmtKind::GroupBy { key } => f(key),
                    StmtKind::Expr(e) => f(e),
                }
            }
        }
        ExprKind::Set { target, value } | ExprKind::ReplaceSet { target, value } => {
            f(target);
            f(value);
        }
        ExprKind::Atomic(e) | ExprKind::Refine(e) => f(e),
        ExprKind::TimeUnitLit { value, .. } => f(value),
        ExprKind::Annot { expr: e, .. } => f(e),
        ExprKind::Serve { handlers, .. } => {
            for h in handlers {
                f(&h.body);
            }
        }
        ExprKind::ViewDecl { body, .. } => f(body),
        _ => {}
    }
}

/// Whether a record-embedded relation field is a source/view (`*name`, read+write
/// via `SourceRef`) or a derived relation (`&name`, read-only via `DerivedRef`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum RecordRelKind {
    Source,
    Derived,
}

/// Rewrite `rec.*name` → `SourceRef(name)` / `rec.&name` → `DerivedRef(name)`
/// when `rec` is a static source-record that declares that relation.
fn rewrite_record_source_refs(expr: &mut Expr) {
    let map = collect_record_source_fields(expr);
    if map.is_empty() {
        return;
    }
    rewrite_source_refs_in_expr(expr, &map);
}

fn rewrite_source_refs_in_expr(
    expr: &mut Expr,
    map: &std::collections::HashMap<String, Vec<(String, RecordRelKind)>>,
) {
    // Rewrite this node first (top-down so a rewritten ref isn't descended into).
    if let ExprKind::FieldAccess { expr: base, field } = &expr.node
        && let ExprKind::Var(rec) = &base.node
        && let Some(names) = map.get(rec)
    {
        let stripped = field
            .strip_prefix('*')
            .map(|n| (n, RecordRelKind::Source))
            .or_else(|| field.strip_prefix('&').map(|n| (n, RecordRelKind::Derived)));
        if let Some((bare, kind)) = stripped
            && names.iter().any(|(n, k)| n == bare && *k == kind)
        {
            expr.node = match kind {
                RecordRelKind::Source => ExprKind::SourceRef(bare.to_string()),
                RecordRelKind::Derived => ExprKind::DerivedRef(bare.to_string()),
            };
            return;
        }
    }
    walk_expr_children(expr, &mut |child| rewrite_source_refs_in_expr(child, map));
}

/// IO-producing function names, split by how they were detected.
///
/// `base` mirrors codegen's `detect_io_functions` exactly (DeclKind::Fun
/// bodies plus impl/trait-default method bodies + IO builtins fixpoint):
/// names in this set are also recognized by codegen's `is_io_do_block`, so
/// excluding a do-block because of them routes it to the dedicated
/// `compile_io_do` path.
///
/// `all` additionally contains trait-method names whose declared signature
/// returns `IO ...` (in any trait — the signature is the most reliable
/// signal since impls must conform to it, and the impl body may not
/// syntactically reveal IO). Codegen cannot see these, so do-blocks whose
/// IO-ness comes only from `all − base` must be handled by the
/// `__bind`/IO monadic path, not by exclusion (see `is_pure_comprehension`).
///
/// The trait scan is deliberately name-based and conservative in the
/// "flag as IO" direction: a pure function sharing a name with an IO trait
/// method would also be treated as IO, which mirrors the treatment already
/// applied to IO builtins (a lambda param named `println` is likewise
/// treated as IO today). The opposite direction (missing a genuinely IO
/// method) desugars an IO do-block as a pure comprehension, which fails to
/// type-check.
pub(crate) struct IoFns {
    base: HashSet<String>,
    all: HashSet<String>,
}

/// Detect user functions whose bodies (transitively) produce IO values.
/// Uses fixed-point iteration to handle transitive IO (e.g., genToken calls randomInt).
/// Collect named function bindings (a `with`-record field whose value is a
/// lambda, possibly with a signature) so IO detection can see calls between
/// them. Walks every `with {record}` in the expression.
fn collect_fun_bodies<'a>(
    expr: &'a Expr,
    fun_bodies: &mut Vec<(&'a str, &'a Expr)>,
    fun_sig_io: &mut HashSet<String>,
) {
    if let ExprKind::With { record, body } = &expr.node {
        if let ExprKind::Record(fields) = &record.node {
            for f in fields {
                if matches!(f.value.node, ExprKind::Lambda { .. }) {
                    fun_bodies.push((f.name.as_str(), &f.value));
                    if let Some(ts) = &f.sig
                        && type_returns_io(&ts.ty)
                    {
                        fun_sig_io.insert(f.name.clone());
                    }
                }
                // Recurse into field values (nested `with`s).
                collect_fun_bodies(&f.value, fun_bodies, fun_sig_io);
            }
        } else {
            collect_fun_bodies(record, fun_bodies, fun_sig_io);
        }
        collect_fun_bodies(body, fun_bodies, fun_sig_io);
        return;
    }
    // Generic recursion for non-With nodes (avoids the shared read-walker,
    // whose closure can't capture the invariant `&mut Vec<(&'a …)>`).
    match &expr.node {
        ExprKind::App { func, arg } => {
            collect_fun_bodies(func, fun_bodies, fun_sig_io);
            collect_fun_bodies(arg, fun_bodies, fun_sig_io);
        }
        ExprKind::Lambda { body, .. } => collect_fun_bodies(body, fun_bodies, fun_sig_io),
        ExprKind::BinOp { lhs, rhs, .. } => {
            collect_fun_bodies(lhs, fun_bodies, fun_sig_io);
            collect_fun_bodies(rhs, fun_bodies, fun_sig_io);
        }
        ExprKind::UnaryOp { operand, .. } => {
            collect_fun_bodies(operand, fun_bodies, fun_sig_io)
        }
        ExprKind::If { cond, then_branch, else_branch } => {
            collect_fun_bodies(cond, fun_bodies, fun_sig_io);
            collect_fun_bodies(then_branch, fun_bodies, fun_sig_io);
            collect_fun_bodies(else_branch, fun_bodies, fun_sig_io);
        }
        ExprKind::Case { scrutinee, arms } => {
            collect_fun_bodies(scrutinee, fun_bodies, fun_sig_io);
            for arm in arms {
                collect_fun_bodies(&arm.body, fun_bodies, fun_sig_io);
            }
        }
        ExprKind::Do(stmts) => {
            for s in stmts {
                match &s.node {
                    StmtKind::Bind { expr: e, .. } => {
                        collect_fun_bodies(e, fun_bodies, fun_sig_io)
                    }
                    StmtKind::Where { cond } => {
                        collect_fun_bodies(cond, fun_bodies, fun_sig_io)
                    }
                    StmtKind::GroupBy { key } => {
                        collect_fun_bodies(key, fun_bodies, fun_sig_io)
                    }
                    StmtKind::Expr(e) => collect_fun_bodies(e, fun_bodies, fun_sig_io),
                }
            }
        }
        ExprKind::Set { target, value } | ExprKind::ReplaceSet { target, value } => {
            collect_fun_bodies(target, fun_bodies, fun_sig_io);
            collect_fun_bodies(value, fun_bodies, fun_sig_io);
        }
        ExprKind::Atomic(e) | ExprKind::Refine(e) => {
            collect_fun_bodies(e, fun_bodies, fun_sig_io)
        }
        ExprKind::TimeUnitLit { value, .. } => {
            collect_fun_bodies(value, fun_bodies, fun_sig_io)
        }
        ExprKind::Record(fields) => {
            for fl in fields {
                collect_fun_bodies(&fl.value, fun_bodies, fun_sig_io);
            }
        }
        ExprKind::RecordUpdate { base, fields } => {
            collect_fun_bodies(base, fun_bodies, fun_sig_io);
            for fl in fields {
                collect_fun_bodies(&fl.value, fun_bodies, fun_sig_io);
            }
        }
        ExprKind::List(items) => {
            for it in items {
                collect_fun_bodies(it, fun_bodies, fun_sig_io);
            }
        }
        ExprKind::FieldAccess { expr: e, .. } | ExprKind::Annot { expr: e, .. } => {
            collect_fun_bodies(e, fun_bodies, fun_sig_io)
        }
        ExprKind::Serve { handlers, .. } => {
            for h in handlers {
                collect_fun_bodies(&h.body, fun_bodies, fun_sig_io);
            }
        }
        ExprKind::ViewDecl { body, .. } | ExprKind::DerivedDecl { body, .. } => {
            collect_fun_bodies(body, fun_bodies, fun_sig_io)
        }
        _ => {}
    }
}

fn detect_io_functions(expr: &Expr) -> IoFns {
    let io_builtins: HashSet<&str> = crate::builtins::EFFECTFUL_BUILTINS
        .iter()
        .filter(|n| **n != "retry")
        .copied()
        .collect();

    let mut fun_bodies: Vec<(&str, &Expr)> = Vec::new();
    let mut fun_sig_io: HashSet<String> = HashSet::new();
    collect_fun_bodies(expr, &mut fun_bodies, &mut fun_sig_io);

    fn fixpoint(
        bodies: &[(&str, &Expr)],
        io_builtins: &HashSet<&str>,
        io_fns: &mut HashSet<String>,
    ) {
        loop {
            let mut changed = false;
            for (name, body) in bodies {
                if io_fns.contains(*name) {
                    continue;
                }
                if expr_contains_io(body, io_builtins, io_fns) {
                    io_fns.insert(name.to_string());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    // Base set: Fun bodies — mirrors codegen's detect_io_functions (which
    // scans the same body kinds), so exclusion driven by this set always
    // lines up with codegen's is_io_do_block routing to compile_io_do.
    let all_bodies = fun_bodies;
    let mut base = HashSet::new();
    base.extend(fun_sig_io.clone());
    fixpoint(&all_bodies, &io_builtins, &mut base);

    // Full set: additionally seeded with IO-signature functions, then
    // re-fixpointed so functions calling them are recognized too.
    let mut all = base.clone();
    all.extend(fun_sig_io);
    fixpoint(&all_bodies, &io_builtins, &mut all);

    IoFns { base, all }
}

/// Whether a declared type's final return type is `IO ...` (walking through
/// curried function arrows). Used to flag trait methods as IO-returning from
/// their signatures alone.
fn type_returns_io(ty: &Type) -> bool {
    match &ty.node {
        TypeKind::Function { result, .. } => type_returns_io(result),
        TypeKind::IO { .. } => true,
        _ => false,
    }
}

/// Check if an expression contains IO calls (builtins or known IO user functions).
fn expr_contains_io(expr: &Expr, builtins: &HashSet<&str>, io_fns: &HashSet<String>) -> bool {
    match &expr.node {
        ExprKind::Var(name) => builtins.contains(name.as_str()) || io_fns.contains(name.as_str()),
        ExprKind::SourceRef(_) | ExprKind::DerivedRef(_) => true,
        ExprKind::Set { .. } | ExprKind::ReplaceSet { .. } => true,
        ExprKind::Atomic(_) => true,
        ExprKind::TimeUnitLit { value, .. } => expr_contains_io(value, builtins, io_fns),
        ExprKind::Annot { expr, .. } => expr_contains_io(expr, builtins, io_fns),
        ExprKind::Refine(inner) => expr_contains_io(inner, builtins, io_fns),
        ExprKind::App { func, arg } => {
            expr_contains_io(func, builtins, io_fns)
                || expr_contains_io(arg, builtins, io_fns)
        }
        ExprKind::With { record, body } => {
            expr_contains_io(record, builtins, io_fns)
                || expr_contains_io(body, builtins, io_fns)
        }
        ExprKind::BinOp { lhs, rhs, .. } => {
            expr_contains_io(lhs, builtins, io_fns)
                || expr_contains_io(rhs, builtins, io_fns)
        }
        ExprKind::UnaryOp { operand, .. } => {
            expr_contains_io(operand, builtins, io_fns)
        }
        ExprKind::Do(stmts) => {
            stmts.iter().any(|s| match &s.node {
                StmtKind::Bind { expr, .. } => expr_contains_io(expr, builtins, io_fns),
                StmtKind::Expr(expr) => expr_contains_io(expr, builtins, io_fns),
                StmtKind::Where { cond } => expr_contains_io(cond, builtins, io_fns),
                StmtKind::GroupBy { key } => expr_contains_io(key, builtins, io_fns),
            })
        }
        ExprKind::Lambda { body, .. } => expr_contains_io(body, builtins, io_fns),
        ExprKind::If { cond, then_branch, else_branch, .. } => {
            expr_contains_io(cond, builtins, io_fns)
                || expr_contains_io(then_branch, builtins, io_fns)
                || expr_contains_io(else_branch, builtins, io_fns)
        }
        ExprKind::Case { scrutinee, arms, .. } => {
            expr_contains_io(scrutinee, builtins, io_fns)
                || arms.iter().any(|arm| expr_contains_io(&arm.body, builtins, io_fns))
        }
        // Records, lists, field access are data constructors/accessors —
        // they don't produce IO even if they contain IO values as
        // subexpressions. Must match codegen's expr_contains_io to avoid
        // incorrectly preventing desugaring of pure comprehension do-blocks.
        ExprKind::Record(_)
        | ExprKind::RecordUpdate { .. }
        | ExprKind::FieldAccess { .. }
        | ExprKind::List(_) => false,
        _ => {
            if let Some(inner) = expr.node.as_yield_arg() {
                expr_contains_io(inner, builtins, io_fns)
            } else {
                false
            }
        }
    }
}



/// Name of the hidden dictionary parameter introduced for a `^field`
/// constraint: `__dict_<field>`.
fn dict_param_name(field: &str) -> Name {
    format!("__dict_{field}")
}

/// Elaborate a function's `^`-field signature constraints into hidden leading
/// dictionary parameters, rewriting each body occurrence of `^field` to
/// `__dict_<field>.field` and prepending `{field : F} ->` to the declared
/// type. Shared by top-level funs and record-field funs. Returns the implicit
/// constraints in declared order.
fn elaborate_implicit_dicts(body: &mut Expr, ty: &mut Option<TypeScheme>) -> Vec<(Name, Type)> {
    let implicit: Vec<(Name, Type)> = ty
        .as_ref()
        .map(|ts| {
            ts.constraints
                .iter()
                .filter_map(|c| match c {
                    Constraint::ImplicitField { field, ty } => {
                        Some((field.clone(), ty.clone()))
                    }
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    if implicit.is_empty() {
        return implicit;
    }
    let span = body.span;
    for (field, _) in &implicit {
        rewrite_implicit_refs(body, field);
    }
    for (field, _) in implicit.iter().rev() {
        let dict = dict_param_name(field);
        let placeholder = Spanned::new(ExprKind::Lit(Literal::Bool(false)), span);
        let old_body = std::mem::replace(body, placeholder);
        *body = Spanned::new(
            ExprKind::Lambda {
                params: vec![Spanned::new(PatKind::Var(dict), span)],
                ty_params: vec![],
                body: Box::new(old_body),
            },
            span,
        );
    }
    // Elaborate the declared type: prepend `{field : F} ->` for each
    // implicit-field constraint (innermost constraint last, so the first
    // declared constraint is the outermost/first param).
    if let Some(ts) = ty {
        for (field, fty) in implicit.iter().rev() {
            let dict_ty = Spanned::new(
                TypeKind::Record {
                    fields: vec![Field {
                        name: field.clone(),
                        value: fty.clone(),
                    }],
                    rest: None,
                },
                fty.span,
            );
            let old_span = ts.ty.span;
            let old = std::mem::replace(&mut ts.ty, dict_ty.clone());
            ts.ty = Spanned::new(
                TypeKind::Function {
                    param: Box::new(dict_ty),
                    result: Box::new(old),
                },
                old_span,
            );
        }
    }
    implicit
}

/// Rewrite every `^field` implicit projection in `expr` to an explicit
/// dictionary projection `__dict_<field>.field`, so the constrained function's
/// body reads its operations off the hidden dictionary parameter.
fn rewrite_implicit_refs(expr: &mut Expr, field: &str) {
    if let ExprKind::ImplicitRef(name) = &expr.node
        && name == field
    {
        let span = expr.span;
        expr.node = ExprKind::FieldAccess {
            expr: Box::new(Spanned::new(
                ExprKind::Var(dict_param_name(field)),
                span,
            )),
            field: field.to_string(),
        };
        return;
    }
    walk_expr_children(expr, &mut |child| rewrite_implicit_refs(child, field));
}

/// Recurse over all direct child expressions of `expr`.
fn walk_expr_children(expr: &mut Expr, f: &mut impl FnMut(&mut Expr)) {
    match &mut expr.node {
        ExprKind::App { func, arg } => {
            f(func);
            f(arg);
        }
        ExprKind::Lambda { body, .. } => f(body),
        ExprKind::Record(fields) => {
            for field in fields {
                f(&mut field.value);
            }
        }
        ExprKind::RecordUpdate { base, fields } => {
            f(base);
            for field in fields {
                f(&mut field.value);
            }
        }
        ExprKind::FieldAccess { expr: e, .. } => f(e),
        ExprKind::List(elems) => {
            for e in elems {
                f(e);
            }
        }
        ExprKind::With { record, body } => {
            f(record);
            f(body);
        }
        ExprKind::BinOp { lhs, rhs, .. } => {
            f(lhs);
            f(rhs);
        }
        ExprKind::UnaryOp { operand, .. } => f(operand),
        ExprKind::If { cond, then_branch, else_branch } => {
            f(cond);
            f(then_branch);
            f(else_branch);
        }
        ExprKind::Case { scrutinee, arms } => {
            f(scrutinee);
            for arm in arms {
                f(&mut arm.body);
            }
        }
        ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &mut stmt.node {
                    StmtKind::Bind { expr: e, .. } => f(e),
                    StmtKind::Where { cond } => f(cond),
                    StmtKind::GroupBy { key } => f(key),
                    StmtKind::Expr(e) => f(e),
                }
            }
        }
        ExprKind::Set { target, value } | ExprKind::ReplaceSet { target, value } => {
            f(target);
            f(value);
        }
        ExprKind::Atomic(e) | ExprKind::Refine(e) => f(e),
        ExprKind::TimeUnitLit { value, .. } => f(value),
        ExprKind::Annot { expr: e, .. } => f(e),
        ExprKind::Serve { handlers, .. } => {
            for h in handlers {
                f(&mut h.body);
            }
        }
        ExprKind::ViewDecl { body, .. } => f(body),
        _ => {}
    }
}


/// Recursively desugar expressions. The `Do` nodes that qualify as
/// pure comprehensions are replaced with nested App/Lambda/Yield nodes.
///
/// `source_vars` tracks variables bound from `*source` reads in enclosing
/// do-blocks (mirroring codegen's `source_var_binds`), so the SQL-compilable
/// check only treats `x <- someVar` as a relation read when `someVar` is
/// provably source-bound — a bind over a Maybe/Result/lambda-param variable
/// must desugar through `__bind` instead.
fn desugar_expr(expr: &mut Expr, io_fns: &IoFns, source_vars: &HashSet<String>) {
    // First, recurse into sub-expressions (bottom-up).
    // We handle Set/ReplaceSet specially to avoid desugaring their value do blocks.
    match &mut expr.node {
        ExprKind::Set { target, value } | ExprKind::ReplaceSet { target, value } => {
            desugar_expr(target, io_fns, source_vars);
            // Don't desugar the top-level do block of a set value,
            // but DO recurse into its sub-expressions.
            // Unwrap Annot/Refine wrappers to find the Do block
            // (e.g. `set *rel = (do { ... } : [T])`).
            let inner = unwrap_wrappers_mut(value);
            if let ExprKind::Do(stmts) = &mut inner.node {
                desugar_do_stmts(stmts, io_fns, source_vars);
            } else {
                desugar_expr(value, io_fns, source_vars);
            }
            return; // Don't fall through to the Do check below
        }
        _ => recurse_into_children(expr, io_fns, source_vars),
    }

    // Now check if this expression is a desugaring-eligible Do block.
    // Check eligibility with immutable borrows first to avoid borrow conflicts.
    let (sql_compilable, pure_comp) = if let ExprKind::Do(stmts) = &expr.node {
        (
            is_sql_compilable(stmts, source_vars),
            is_pure_comprehension(stmts, io_fns),
        )
    } else {
        (false, false)
    };

    if sql_compilable {
        // SQL-compilable do-blocks are preserved for codegen to compile
        // to a single SQL query. Sub-expressions were already desugared by
        // recurse_into_children above.
        return;
    }
    if pure_comp
        && let ExprKind::Do(stmts) = &expr.node {
            let span = expr.span;
            let desugared = desugar_stmts(stmts, span);
            *expr = desugared;
        }
}

/// Recurse into all child expressions of a node (except Do blocks handled
/// by the caller).
fn recurse_into_children(expr: &mut Expr, io_fns: &IoFns, source_vars: &HashSet<String>) {
    match &mut expr.node {
        ExprKind::Lit(_) | ExprKind::Var(_) | ExprKind::Constructor(_)
        | ExprKind::SourceRef(_) | ExprKind::DerivedRef(_) | ExprKind::ImplicitRef(_) => {}
        ExprKind::TypeCtor { .. } | ExprKind::DataCtor { .. } | ExprKind::SourceDecl { .. } => {}
        ExprKind::SubsetConstraint { .. } => {}
        ExprKind::RouteDecl { .. } | ExprKind::RouteCompositeDecl { .. } => {}
        ExprKind::ViewDecl { body, .. } | ExprKind::DerivedDecl { body, .. } => {
            desugar_expr(body, io_fns, source_vars)
        }

        ExprKind::Record(fields) => {
            for f in fields {
                desugar_expr(&mut f.value, io_fns, source_vars);
            }
        }
        ExprKind::RecordUpdate { base, fields } => {
            desugar_expr(base, io_fns, source_vars);
            for f in fields {
                desugar_expr(&mut f.value, io_fns, source_vars);
            }
        }
        ExprKind::FieldAccess { expr: e, .. } => desugar_expr(e, io_fns, source_vars),
        ExprKind::List(elems) => {
            for e in elems {
                desugar_expr(e, io_fns, source_vars);
            }
        }
        ExprKind::Lambda { params, body, .. } => {
            // Lambda params shadow any same-named source-bound variables
            // from the enclosing scope.
            let mut bound: Vec<String> = Vec::new();
            for p in params.iter() {
                pat_bound_names(p, &mut bound);
            }
            if bound.iter().any(|n| source_vars.contains(n)) {
                let mut inner_vars = source_vars.clone();
                for n in &bound {
                    inner_vars.remove(n);
                }
                desugar_expr(body, io_fns, &inner_vars);
            } else {
                desugar_expr(body, io_fns, source_vars);
            }
        }
        ExprKind::With { record, body } => {
            desugar_expr(record, io_fns, source_vars);
            desugar_expr(body, io_fns, source_vars);
        }
        ExprKind::App { func, arg } => {
            // Preserve do-block arguments to sortBy/takeRelation so codegen
            // can compile them to SQL ORDER BY + LIMIT.
            let protect_do = if let ExprKind::App { func: inner_f, .. } = &func.node {
                matches!(&inner_f.node, ExprKind::Var(name) if name == "sortBy")
            } else {
                false
            };
            desugar_expr(func, io_fns, source_vars);
            if protect_do {
                if let ExprKind::Do(stmts) = &mut arg.node {
                    desugar_do_stmts(stmts, io_fns, source_vars);
                } else {
                    desugar_expr(arg, io_fns, source_vars);
                }
            } else {
                desugar_expr(arg, io_fns, source_vars);
            }
        }
        ExprKind::BinOp { lhs, rhs, .. } => {
            desugar_expr(lhs, io_fns, source_vars);
            desugar_expr(rhs, io_fns, source_vars);
        }
        ExprKind::UnaryOp { operand, .. } => desugar_expr(operand, io_fns, source_vars),
        ExprKind::If {
            cond,
            then_branch,
            else_branch,
        } => {
            desugar_expr(cond, io_fns, source_vars);
            desugar_expr(then_branch, io_fns, source_vars);
            desugar_expr(else_branch, io_fns, source_vars);
        }
        ExprKind::Case { scrutinee, arms } => {
            desugar_expr(scrutinee, io_fns, source_vars);
            for arm in arms {
                // Case-arm pattern binders shadow source-bound variables.
                let mut bound: Vec<String> = Vec::new();
                pat_bound_names(&arm.pat, &mut bound);
                if bound.iter().any(|n| source_vars.contains(n)) {
                    let mut inner_vars = source_vars.clone();
                    for n in &bound {
                        inner_vars.remove(n);
                    }
                    desugar_expr(&mut arm.body, io_fns, &inner_vars);
                } else {
                    desugar_expr(&mut arm.body, io_fns, source_vars);
                }
            }
        }
        ExprKind::Do(stmts) => {
            desugar_do_stmts(stmts, io_fns, source_vars);
        }
        ExprKind::Set { target, value } | ExprKind::ReplaceSet { target, value } => {
            desugar_expr(target, io_fns, source_vars);
            desugar_expr(value, io_fns, source_vars);
        }
        ExprKind::Atomic(inner) => desugar_expr(inner, io_fns, source_vars),
        ExprKind::TimeUnitLit { value, .. } => desugar_expr(value, io_fns, source_vars),
        ExprKind::Annot { expr, .. } => desugar_expr(expr, io_fns, source_vars),
        ExprKind::Refine(inner) => desugar_expr(inner, io_fns, source_vars),
        ExprKind::Serve { handlers, .. } => {
            for h in handlers {
                desugar_expr(&mut h.body, io_fns, source_vars);
            }
        }
    }
}

/// Unwrap Annot/Refine wrappers to find the innermost expression.
/// Used to protect Do blocks inside Set values and View bodies from
/// desugaring when they're wrapped in type annotations.
fn unwrap_wrappers_mut(expr: &mut Expr) -> &mut Expr {
    if matches!(
        &expr.node,
        ExprKind::Annot { .. }
            | ExprKind::TimeUnitLit { .. }
            | ExprKind::Refine(_)
    ) {
        let inner = match &mut expr.node {
            ExprKind::Annot { expr: inner, .. }
            | ExprKind::TimeUnitLit { value: inner, .. } => inner.as_mut(),
            ExprKind::Refine(inner) => inner.as_mut(),
            _ => unreachable!(),
        };
        unwrap_wrappers_mut(inner)
    } else {
        expr
    }
}

fn desugar_stmt(stmt: &mut Stmt, io_fns: &IoFns, source_vars: &HashSet<String>) {
    match &mut stmt.node {
        StmtKind::Bind { expr, .. } => desugar_expr(expr, io_fns, source_vars),
        StmtKind::Where { cond } => desugar_expr(cond, io_fns, source_vars),
        StmtKind::GroupBy { key } => desugar_expr(key, io_fns, source_vars),
        StmtKind::Expr(e) => desugar_expr(e, io_fns, source_vars),
    }
}

/// Desugar the statements of a do-block whose top-level structure is being
/// preserved, threading the source-bound variable context: statements are
/// processed in order, and each `x <- *source` bind extends the set (while
/// any other binding of `x` shadows/removes it), mirroring codegen's
/// `source_var_binds` bookkeeping.
fn desugar_do_stmts(stmts: &mut [Stmt], io_fns: &IoFns, source_vars: &HashSet<String>) {
    let mut local = source_vars.clone();
    for stmt in stmts.iter_mut() {
        desugar_stmt(stmt, io_fns, &local);
        match &stmt.node {
            StmtKind::Bind { pat, expr } => {
                let mut bound: Vec<String> = Vec::new();
                pat_bound_names(pat, &mut bound);
                let is_source_read = matches!(&expr.node, ExprKind::SourceRef(_));
                if let (PatKind::Var(name), true) = (&pat.node, is_source_read) {
                    local.insert(name.clone());
                } else {
                    for n in &bound {
                        local.remove(n);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Collect every variable name bound by a pattern.
fn pat_bound_names(pat: &Pat, out: &mut Vec<String>) {
    match &pat.node {
        PatKind::Var(name) => out.push(name.clone()),
        PatKind::Wildcard | PatKind::Lit(_) => {}
        PatKind::Constructor { payload, .. } => pat_bound_names(payload, out),
        PatKind::Record(fields) => {
            for f in fields {
                match &f.pattern {
                    Some(p) => pat_bound_names(p, out),
                    // Punned: `{name}` binds `name`.
                    None => out.push(f.name.clone()),
                }
            }
        }
        PatKind::List(pats) => {
            for p in pats {
                pat_bound_names(p, out);
            }
        }
        PatKind::Cons { head, tail } => {
            pat_bound_names(head, out);
            pat_bound_names(tail, out);
        }
        PatKind::Annot { pat, .. } => pat_bound_names(pat, out),
    }
}

// ── Eligibility check ────────────────────────────────────────────

/// A do block is SQL-compilable if:
/// 1. All non-final stmts are Bind(Var, SourceRef) or Where
/// 2. All Where conditions use only field accesses, literals, variables,
///    comparison operators, and boolean connectives
/// 3. The final stmt is Yield(Record) where each field is a field access
///    on a bound variable, or Yield(Var(bound_var)) for single-table
/// 4. At least one Bind
///
/// This is a purely syntactic check. The codegen does additional validation
/// (schema shape, views, etc.) and falls back to loop-based compilation
/// if the SQL path is not viable.
///
/// `source_vars` is the set of variables bound from `*source` reads in
/// enclosing do-blocks. A `x <- someVar` bind only counts as a relation
/// read (SQL-compilable) when `someVar` is in that set — codegen resolves
/// exactly those via `source_var_binds`. Binds over arbitrary variables
/// (lambda params, Maybe/Result values, ...) must NOT be preserved as raw
/// Do nodes: they desugar through `__bind` so non-relation monads work.
fn is_sql_compilable(stmts: &[Stmt], source_vars: &HashSet<String>) -> bool {
    if stmts.len() < 2 {
        return false;
    }

    let mut bind_vars: std::collections::HashSet<&str> = std::collections::HashSet::new();

    for stmt in &stmts[..stmts.len() - 1] {
        match &stmt.node {
            StmtKind::Bind { pat, expr } => {
                if let PatKind::Var(name) = &pat.node {
                    // Accept SourceRef (direct source read) and Var when it is
                    // provably a source-bound variable from an enclosing
                    // do-block (codegen resolves these via source_var_binds).
                    let ok = match &expr.node {
                        ExprKind::SourceRef(_) => true,
                        ExprKind::Var(v) => source_vars.contains(v),
                        _ => false,
                    };
                    if ok {
                        bind_vars.insert(name.as_str());
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            StmtKind::Where { cond } => {
                if !is_sql_where_expr(cond, &bind_vars) {
                    return false;
                }
            }
            _ => return false,
        }
    }

    if bind_vars.is_empty() {
        return false;
    }

    // Final statement must be yield of a record of field accesses or a bound var
    match &stmts.last().unwrap().node {
        StmtKind::Expr(e) => {
            if let Some(inner) = e.node.as_yield_arg() {
                match &inner.node {
                    ExprKind::Record(fields) => {
                        !fields.is_empty()
                            && fields.iter().all(|f| is_bound_field_access(&f.value, &bind_vars))
                    }
                    ExprKind::Var(name) => bind_vars.contains(name.as_str()),
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

fn is_sql_where_expr(expr: &Expr, bind_vars: &std::collections::HashSet<&str>) -> bool {
    match &expr.node {
        ExprKind::BinOp { op, lhs, rhs } => match op {
            BinOp::And | BinOp::Or => {
                is_sql_where_expr(lhs, bind_vars) && is_sql_where_expr(rhs, bind_vars)
            }
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                let l_ok = is_sql_atom(lhs);
                let r_ok = is_sql_atom(rhs);
                let l_bound = is_bound_field_access(lhs, bind_vars);
                let r_bound = is_bound_field_access(rhs, bind_vars);
                l_ok && r_ok && (l_bound || r_bound)
            }
            _ => false,
        },
        ExprKind::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => is_sql_where_expr(operand, bind_vars),
        // `contains needle haystack` and `elem element list` are SQL-compilable
        // (codegen emits INSTR/IN for them). Accept when at least one argument
        // is a bound field access, matching the codegen pattern.
        ExprKind::App { func, arg } => {
            if let ExprKind::App { func: inner, arg: first_arg } = &func.node
                && let ExprKind::Var(name) = &inner.node
                    && (name == "contains" || name == "elem") {
                        let a_bound = is_bound_field_access(first_arg, bind_vars)
                            || is_sql_atom(first_arg);
                        let b_bound = is_bound_field_access(arg, bind_vars)
                            || is_sql_atom(arg);
                        return a_bound && b_bound
                            && (is_bound_field_access(first_arg, bind_vars)
                                || is_bound_field_access(arg, bind_vars));
                    }
            false
        }
        _ => false,
    }
}

fn is_sql_atom(expr: &Expr) -> bool {
    matches!(
        &expr.node,
        ExprKind::FieldAccess { expr, .. } if matches!(&expr.node, ExprKind::Var(_))
    ) || matches!(&expr.node, ExprKind::Lit(_) | ExprKind::Var(_))
}

fn is_bound_field_access(expr: &Expr, bind_vars: &std::collections::HashSet<&str>) -> bool {
    if let ExprKind::FieldAccess { expr, .. } = &expr.node
        && let ExprKind::Var(name) = &expr.node {
            return bind_vars.contains(name.as_str());
        }
    false
}

/// A do block is a "pure comprehension" if:
/// 1. It contains at least one Bind or Where statement
/// 2. All non-final statements are Bind, Where, or Let
/// 3. The final statement is Expr(Yield(..))
fn is_pure_comprehension(stmts: &[Stmt], io_fns: &IoFns) -> bool {
    if stmts.is_empty() {
        return false;
    }

    // Need at least one Bind/Where (relational comprehension) or multiple
    // statements (monadic sequencing). A single bare expression doesn't need
    // desugaring — EXCEPT a lone `yield e`: left undesugared it falls through
    // to the relational `compile_do` path, which wraps the value in a
    // singleton relation (`[e]`) even when the do-block's monad is IO/Maybe/…
    // (e.g. an `IO`-typed handler body `do { yield Ok {...} }` would return
    // `[value]` and be serialized as a relation). Routing it through `__yield`
    // lets it dispatch on the resolved monad (`knot_io_pure` for IO, `Just`
    // for Maybe, singleton for `[]`), which is correct in every monad.
    let has_bind_or_where = stmts.iter().any(|s| {
        matches!(
            &s.node,
            StmtKind::Bind { .. } | StmtKind::Where { .. }
        )
    });
    if !has_bind_or_where && stmts.len() < 2 {
        let lone_yield = matches!(
            stmts.first().map(|s| &s.node),
            Some(StmtKind::Expr(e)) if e.node.as_yield_arg().is_some()
        );
        if !lone_yield {
            return false;
        }
    }

    // GroupBy requires loop-based codegen — not eligible for desugaring
    if stmts.iter().any(|s| matches!(&s.node, StmtKind::GroupBy { .. })) {
        return false;
    }

    // IO do blocks use a dedicated codegen path (compile_io_do) that handles
    // running IO actions and iterating over resulting relations. Desugaring
    // would use IO's monadic bind (sequencing) instead, which is wrong when
    // the intent is to iterate over relation elements.
    let io_base = &io_fns.base;
    let io_all = &io_fns.all;
    if stmts.iter().any(|s| match &s.node {
        StmtKind::Bind { expr, .. } | StmtKind::Expr(expr) => expr_is_io(expr, io_base),
        StmtKind::Where { cond } => expr_is_io(cond, io_base),
        _ => false,
    }) {
        return false;
    }

    // Trait-method IO is invisible to codegen's `is_io_do_block` (it scans
    // Fun bodies only), so excluding such a block would route it to the
    // relational `compile_do` path which discards bare IO values. The
    // desugared `__bind`/IO chain is the only path that actually runs
    // trait-method IO — so desugar when that chain is well-typed as a pure
    // IO chain (every bind binds from an IO expression, no `where` guards —
    // IO has no Alternative instance), and exclude otherwise (e.g. a bind
    // from a relation/Maybe would force a different monad and make the
    // desugared chain ill-typed).
    let has_trait_only_io = stmts.iter().any(|s| match &s.node {
        StmtKind::Bind { expr, .. } | StmtKind::Expr(expr) => {
            expr_is_io(expr, io_all)
        }
        StmtKind::Where { cond } => expr_is_io(cond, io_all),
        _ => false,
    });
    if has_trait_only_io {
        if stmts.iter().any(|s| matches!(&s.node, StmtKind::Where { .. })) {
            return false;
        }
        // The desugared IO chain requires every `Bind` source and every
        // non-final bare `Expr` to be IO (they become `__bind` arguments).
        // `Let` expressions are applied directly (`(\x -> rest) expr`), not
        // through `__bind`, so they don't need to be IO. The final `Expr`
        // is wrapped in `__yield` (Applicative.pure), so it doesn't need to
        // be IO either. Only `Bind`/non-final `Expr` must be IO for the
        // chain to be well-typed in the IO monad.
        if stmts.iter().enumerate().any(|(i, s)| {
            let is_last = i + 1 == stmts.len();
            match &s.node {
                StmtKind::Bind { expr, .. } => !expr_is_io(expr, io_all),
                StmtKind::Expr(expr) if !is_last => !expr_is_io(expr, io_all),
                _ => false,
            }
        }) {
            return false;
        }
    }

    // Refutable bind patterns (`Circle c <- xs`, `5 <- xs`, `[] <- xs`,
    // `Cons h t <- xs`, or a record with a refutable subpattern) must not be
    // desugared. Two reasons: constructor binds may be value pattern matches
    // rather than monadic binds (the desugarer can't tell syntactically), and
    // any refutable pattern needs *filtering* semantics — a non-matching
    // element is skipped, not a hard failure. The desugared `__bind (\pat ->
    // …)` lambda has no skip target, so a refutable pattern compiles to a trap
    // (aborting the process on the first non-matching element). Direct codegen
    // (`compile_do` / `bind_do_pattern`) handles both cases correctly, so
    // leave these do-blocks un-desugared.
    if stmts.iter().any(|s| matches!(
        &s.node,
        StmtKind::Bind { pat, .. } if pat_is_refutable(&pat.node)
    )) {
        return false;
    }

    // Non-final statements must be Bind/Where/Let or a bare Expr (sequenced
    // monadically as `_ <- e`). Final statement must be a bare Expr — either
    // an explicit `yield e` or any other expression. `desugar_stmts` wraps the
    // final in `__yield` (Applicative.pure) either way, so a plain value `a`
    // becomes the monadic result `m a`.
    for stmt in &stmts[..stmts.len() - 1] {
        match &stmt.node {
            StmtKind::Bind { .. } | StmtKind::Where { .. } | StmtKind::Expr(_) => {}
            _ => return false,
        }
    }
    matches!(stmts.last().unwrap().node, StmtKind::Expr(_))
}

/// Whether a bind pattern can fail to match a given value. Irrefutable
/// patterns (`x`, `_`, and records whose subpatterns are all irrefutable)
/// always bind; everything else (constructor, literal, list, cons) is
/// refutable and needs the skip/filter semantics of direct do-codegen.
fn pat_is_refutable(pat: &PatKind) -> bool {
    match pat {
        PatKind::Var(_) | PatKind::Wildcard => false,
        PatKind::Constructor { .. }
        | PatKind::Lit(_)
        | PatKind::List(_)
        | PatKind::Cons { .. } => true,
        PatKind::Record(fields) => fields
            .iter()
            .any(|f| f.pattern.as_ref().is_some_and(|p| pat_is_refutable(&p.node))),
        PatKind::Annot { pat, .. } => pat_is_refutable(&pat.node),
    }
}

/// Check if an expression contains an IO-returning builtin or user-defined IO function.
/// Recurses into nested expressions to catch IO buried inside if/case/lambda/etc.
fn expr_is_io(expr: &Expr, io_fns: &HashSet<String>) -> bool {
    match &expr.node {
        ExprKind::App { func, arg } => {
            expr_is_io(func, io_fns)
                || expr_is_io(arg, io_fns)
                || applied_lambda_body_is_io(func, io_fns)
                // A higher-order function applied to an IO-bodied lambda
                // argument (e.g. `forEach xs (\i -> println i)`) produces IO
                // when the lambda is called. codegen's `expr_is_io` recurses
                // into every lambda body, so desugar must agree here or the
                // do-block gets misclassified as a pure comprehension and
                // rewritten to `__bind`/`__yield` for the wrong monad.
                || lambda_chain_body_is_io(arg, io_fns)
        }
        ExprKind::Var(name) => {
            // `retry` is in EFFECTFUL_BUILTINS but isn't an IO-producing
            // expression (it's the STM primitive, typed `∀a. a`); excluding
            // it here matches the filter applied in `expr_contains_io`.
            (crate::builtins::EFFECTFUL_BUILTINS.contains(&name.as_str())
                && name.as_str() != "retry")
                || io_fns.contains(name.as_str())
        }
        ExprKind::SourceRef(_) | ExprKind::DerivedRef(_) => true,
        ExprKind::Set { .. } | ExprKind::ReplaceSet { .. } => true,
        ExprKind::Atomic(_) => true,
        ExprKind::BinOp { lhs, rhs, .. } => {
            expr_is_io(lhs, io_fns) || expr_is_io(rhs, io_fns)
        }
        ExprKind::UnaryOp { operand, .. } => expr_is_io(operand, io_fns),
        ExprKind::TimeUnitLit { value, .. } => expr_is_io(value, io_fns),
        ExprKind::Annot { expr, .. } => expr_is_io(expr, io_fns),
        ExprKind::Refine(inner) => expr_is_io(inner, io_fns),
        ExprKind::If { cond, then_branch, else_branch, .. } => {
            expr_is_io(cond, io_fns)
                || expr_is_io(then_branch, io_fns)
                || expr_is_io(else_branch, io_fns)
        }
        ExprKind::Case { scrutinee, arms, .. } => {
            expr_is_io(scrutinee, io_fns)
                || arms.iter().any(|arm| expr_is_io(&arm.body, io_fns))
        }
        // Recurse into the lambda body to mirror codegen's `expr_is_io`
        // (codegen.rs: `Lambda { body, .. } => self.expr_is_io(body)`). The
        // two classifiers MUST agree: if they diverge on a bare IO-bodied
        // lambda used as a do-statement, desugar rewrites the block to
        // `__bind`/`__yield` (wrong monad) while codegen routes it to the IO
        // path. Keeping them identical is what prevents that misclassification
        // — the App-arm `applied_lambda_body_is_io`/`lambda_chain_body_is_io`
        // helpers become redundant but stay as a belt-and-braces guard.
        ExprKind::Lambda { body, .. } => expr_is_io(body, io_fns),
        ExprKind::Do(stmts) => {
            stmts.iter().any(|s| match &s.node {
                StmtKind::Bind { expr, .. } => expr_is_io(expr, io_fns),
                StmtKind::Expr(expr) => expr_is_io(expr, io_fns),
                StmtKind::Where { cond } => expr_is_io(cond, io_fns),
                StmtKind::GroupBy { key } => expr_is_io(key, io_fns),
            })
        }
        // Records, lists, field access are data constructors/accessors —
        // they don't produce IO even if they contain IO values as
        // subexpressions. Only direct IO-producing expressions (calls to
        // IO functions, relation ops, etc.) should flag a do-block as IO.
        _ => false,
    }
}

/// Whether the expression is a lambda (possibly curried or wrapped in
/// annotations/refinements) whose body performs IO. Used to classify
/// do-local `let f = \y -> println ...` bindings: applications of `f`
/// are IO expressions even though the lambda value itself is not.
fn lambda_chain_body_is_io(expr: &Expr, io_fns: &HashSet<String>) -> bool {
    match &expr.node {
        ExprKind::Lambda { body, .. } => match &body.node {
            // Curried lambda: keep peeling to the innermost body.
            ExprKind::Lambda { .. } => lambda_chain_body_is_io(body, io_fns),
            _ => expr_is_io(body, io_fns),
        },
        ExprKind::TimeUnitLit { value, .. }
        | ExprKind::Annot { expr: value, .. }
        | ExprKind::Refine(value) => lambda_chain_body_is_io(value, io_fns),
        _ => false,
    }
}

/// Whether the function position of an application is a lambda (possibly
/// curried or wrapped in annotations) whose body performs IO. An *applied*
/// lambda runs its body immediately, so IO inside the body makes the whole
/// application an IO expression — mirroring codegen's `expr_is_io`, which
/// recurses into lambda bodies. Bare lambda VALUES (not applied) remain
/// non-IO; only the `App { func: Lambda, .. }` shape reaches here.
fn applied_lambda_body_is_io(func: &Expr, io_fns: &HashSet<String>) -> bool {
    match &func.node {
        ExprKind::Lambda { body, .. } => expr_is_io(body, io_fns),
        // Curried application: `(\a b -> body) x y` — keep peeling.
        ExprKind::App { func, .. } => applied_lambda_body_is_io(func, io_fns),
        ExprKind::TimeUnitLit { value, .. }
        | ExprKind::Annot { expr: value, .. }
        | ExprKind::Refine(value) => applied_lambda_body_is_io(value, io_fns),
        _ => false,
    }
}

// ── Core desugaring ──────────────────────────────────────────────

/// Counter for generating unique temporary variable names.
static DESUGAR_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn fresh_var() -> String {
    let n = DESUGAR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("__ds{}", n)
}

/// Globally unique spans for synthesized `__bind`/`__yield`/`__empty` Var
/// nodes. `monad_info` is keyed by these spans (type inference records the
/// resolved monad per helper Var; codegen dispatches on it), and real file
/// offsets COLLIDE with merged decls — the prelude is merged with unshifted
/// spans, so two do-blocks at identical byte offsets could otherwise share
/// one monad slot (and a `Maybe` comprehension could get compiled with
/// Relation binds). Spans are
/// allocated above any plausible real file offset so they never alias a
/// user expression. Diagnostics still anchor on the surrounding App/do
/// spans, which keep their real locations.
const SYNTH_SPAN_BASE: usize = 1 << 31;

/// Synthesized span → original do-block span. Consumers that key on the
/// *do-block's* real span (the LSP's monad inlay hints read
/// `monad_info[do_span]`) still need an entry there, so type inference
/// aliases each resolved monad back to the origin span via this table.
/// Keys are globally unique (the atomic counter), so concurrent
/// compilations in one process can share the table safely; stale entries
/// from other modules are never looked up.
static SYNTH_SPAN_ORIGINS: std::sync::Mutex<Option<std::collections::HashMap<usize, Span>>> =
    std::sync::Mutex::new(None);

/// Soft cap on retained synth-span→origin entries. The map is consulted only
/// by the *same* compile's inference, so entries from finished compiles are
/// dead weight; without bounding, the long-running LSP (which re-desugars on
/// every keystroke) grows the map without limit. See `fresh_monad_span`.
const MAX_SYNTH_SPANS: usize = 1 << 16;

fn fresh_monad_span(origin: Span) -> Span {
    let n = DESUGAR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        as usize;
    let span = Span::new(SYNTH_SPAN_BASE + n, SYNTH_SPAN_BASE + n + 1);
    let mut guard = SYNTH_SPAN_ORIGINS
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let map = guard.get_or_insert_with(std::collections::HashMap::new);
    map.insert(span.start, origin);
    // Reclaim memory in long-running processes. Keys are the monotonic
    // `DESUGAR_COUNTER`, so any compile still in flight holds the *highest*
    // keys; dropping the lowest keys when we run far over capacity evicts only
    // entries from long-finished compiles, never a live one. Amortized O(1):
    // eviction runs once per ~`MAX_SYNTH_SPANS` inserts.
    if map.len() > 2 * MAX_SYNTH_SPANS {
        let mut keys: Vec<usize> = map.keys().copied().collect();
        keys.sort_unstable();
        let cutoff = keys[keys.len() - MAX_SYNTH_SPANS];
        map.retain(|k, _| *k >= cutoff);
    }
    span
}

/// The original do-block span a synthesized monad span was created for.
pub(crate) fn synth_span_origin(span: Span) -> Option<Span> {
    if span.start < SYNTH_SPAN_BASE {
        return None;
    }
    SYNTH_SPAN_ORIGINS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|m| m.get(&span.start).copied())
}

fn spanned<T>(node: T, span: Span) -> Spanned<T> {
    Spanned::new(node, span)
}

/// Desugar a list of statements into a single expression.
fn desugar_stmts(stmts: &[Stmt], span: Span) -> Expr {
    assert!(!stmts.is_empty());

    // Base case: single statement — the do-block's final result.
    if stmts.len() == 1 {
        return match &stmts[0].node {
            StmtKind::Expr(e) => {
                // An explicit `yield e` is always `Applicative.pure e`.
                //
                // A final *bare* expression is ambiguous and only the type
                // checker can settle it: `do { x <- act; show x }` wants
                // `pure (show x)` (a plain `Text` value), while
                // `do { action x; loop rest }` wants `loop rest` itself — it
                // is already an action in the block's monad. Wrapping both in
                // `__yield` types the second as `m (m a)`, which is what broke
                // every IO do-block (the prelude's own `forEach` included).
                // Emit a `__result` marker instead: `infer` rewrites it to
                // `__yield e` or to a bare `e` once it knows both the block's
                // monad and `e`'s type (see `resolve_result_markers`).
                match e.node.as_yield_arg() {
                    Some(inner) => mk_yield(inner.clone(), span),
                    None => mk_result(e.clone(), span),
                }
            }
            // Shouldn't happen for valid pure comprehensions (last must be Expr)
            _ => mk_empty(span),
        };
    }

    let rest = desugar_stmts(&stmts[1..], span);

    match &stmts[0].node {
        StmtKind::Bind { pat, expr } => {
            if let PatKind::Constructor { .. } = &pat.node {
                // Constructor pattern: case dispatch with empty fallback
                desugar_ctor_bind(pat, expr, &rest, span)
            } else {
                // Normal bind: App(App(__bind, \pat -> rest), expr)
                mk_bind(
                    spanned(
                        ExprKind::Lambda {
                            params: vec![pat.clone()],
                            ty_params: vec![],
                            body: Box::new(rest),
                        },
                        span,
                    ),
                    expr.clone(),
                    span,
                )
            }
        }

        StmtKind::Where { cond } => {
            // App(App(__bind, \_ -> rest), if cond then __yield({}) else __empty)
            let guard = spanned(
                ExprKind::If {
                    cond: Box::new(cond.clone()),
                    then_branch: Box::new(mk_yield(
                        spanned(ExprKind::Record(vec![]), span),
                        span,
                    )),
                    else_branch: Box::new(mk_empty(span)),
                },
                span,
            );
            mk_bind(
                spanned(
                    ExprKind::Lambda {
                        params: vec![spanned(PatKind::Wildcard, span)],
                        ty_params: vec![],
                        body: Box::new(rest),
                    },
                    span,
                ),
                guard,
                span,
            )
        }

        StmtKind::GroupBy { .. } => {
            // GroupBy blocks are not desugared (filtered by is_pure_comprehension)
            unreachable!("groupBy should not appear in desugared do blocks")
        }

        StmtKind::Expr(e) => {
            // Bare expression in non-final position: monadic sequencing.
            // `e; rest` => `__bind (\_ -> rest) e` (run e, discard result, then rest).
            // A non-final `yield x` must be routed through `mk_yield` (like the
            // final-statement base case) so its helper Var gets a collision-free
            // synthesized span. Cloning the raw `yield` keeps its real file
            // offset, which `monad_info`/`compile_monadic_yield` key on and which
            // collides across merged files — aliasing another do-block's monad.
            let action = match e.node.as_yield_arg() {
                Some(inner) => mk_yield(inner.clone(), span),
                None => e.clone(),
            };
            mk_bind(
                spanned(
                    ExprKind::Lambda {
                        params: vec![spanned(PatKind::Wildcard, span)],
                        ty_params: vec![],
                        body: Box::new(rest),
                    },
                    span,
                ),
                action,
                span,
            )
        }
    }
}

/// Desugar a constructor pattern bind:
/// `Ctor pat <- expr; rest` =>
/// `__bind (\__tmp -> case __tmp of { Ctor pat -> rest; _ -> [] }) expr`
fn desugar_ctor_bind(pat: &Pat, expr: &Expr, rest: &Expr, span: Span) -> Expr {
    let tmp = fresh_var();
    let tmp_var = spanned(ExprKind::Var(tmp.clone()), span);

    let case_expr = spanned(
        ExprKind::Case {
            scrutinee: Box::new(tmp_var),
            arms: vec![
                CaseArm {
                    pat: pat.clone(),
                    body: rest.clone(),
                },
                CaseArm {
                    pat: spanned(PatKind::Wildcard, span),
                    body: mk_empty(span),
                },
            ],
        },
        span,
    );

    mk_bind(
        spanned(
            ExprKind::Lambda {
                params: vec![spanned(PatKind::Var(tmp), span)],
                ty_params: vec![],
                body: Box::new(case_expr),
            },
            span,
        ),
        expr.clone(),
        span,
    )
}

/// Build `App(Var("__yield"), inner)` — monadic yield for generic do-blocks.
/// The helper Var gets a unique synthesized span (see `fresh_monad_span`).
fn mk_yield(inner: Expr, span: Span) -> Expr {
    spanned(
        ExprKind::App {
            func: Box::new(spanned(
                ExprKind::Var("__yield".into()),
                fresh_monad_span(span),
            )),
            arg: Box::new(inner),
        },
        span,
    )
}

/// Build `App(Var("__result"), inner)` — the *unresolved* result of a
/// do-block whose final statement is a bare expression. Type inference
/// replaces every one of these with either `App(Var("__yield"), inner)` (when
/// `inner` is a plain value, so the block's result is `pure inner`) or with
/// `inner` alone (when `inner` is already an action in the block's monad).
/// The helper Var gets a unique synthesized span (see `fresh_monad_span`) so
/// the `__yield` rewrite lands on a span `monad_info` already carries.
fn mk_result(inner: Expr, span: Span) -> Expr {
    spanned(
        ExprKind::App {
            func: Box::new(spanned(
                ExprKind::Var(RESULT_MARKER.into()),
                fresh_monad_span(span),
            )),
            arg: Box::new(inner),
        },
        span,
    )
}

/// Name of the synthesized marker `mk_result` emits. Never written by users
/// (leading underscores are not valid in Knot identifiers) and never survives
/// inference — `infer::check` rewrites every occurrence away.
pub(crate) const RESULT_MARKER: &str = "__result";

/// Build `Var("__empty")` with a unique synthesized span (see
/// `fresh_monad_span`).
fn mk_empty(span: Span) -> Expr {
    spanned(ExprKind::Var("__empty".into()), fresh_monad_span(span))
}

/// Build `App(App(Var("__bind"), func), collection)`
/// The helper Var gets a unique synthesized span (see `fresh_monad_span`).
fn mk_bind(func: Expr, collection: Expr, span: Span) -> Expr {
    spanned(
        ExprKind::App {
            func: Box::new(spanned(
                ExprKind::App {
                    func: Box::new(spanned(
                        ExprKind::Var("__bind".into()),
                        fresh_monad_span(span),
                    )),
                    arg: Box::new(func),
                },
                span,
            )),
            arg: Box::new(collection),
        },
        span,
    )
}

// ── Tests ─────────────────────────────────────────────────────────


