//! Standard prelude: ordinary polymorphic functions injected into every module.
//!
//! The user-facing trait/typeclass system has been removed; the prelude is now
//! a small set of plain functions. Builtin operator semantics (`+`, `<`, `++`,
//! unary `-`, `==`) are enforced intrinsically by the type checker and code
//! generator, and monadic do-blocks dispatch structurally, so no trait
//! declarations are needed here.

use knot::ast;

/// Byte offset added to every parsed prelude span so prelude spans can never
/// collide with user-file spans (bug B39). Chosen far above any plausible real
/// file size and above `desugar::SYNTH_SPAN_BASE` (1 << 31) so it also clears
/// the synthesized monad-span range.
pub(crate) const PRELUDE_SPAN_OFFSET: usize = 1 << 40;

/// Knot source for the standard prelude.
const PRELUDE_SOURCE: &str = r#"
data Ordering = LT {} | EQ {} | GT {}

min : a -> a -> a
min = \a b -> if a < b then a else b

max : a -> a -> a
max = \a b -> if a > b then a else b

when : Bool -> IO {| e} {} -> IO {| e} {}
when = \cond action -> if cond then action else yield {}

unless : Bool -> IO {| e} {} -> IO {| e} {}
unless = \cond action -> if cond then yield {} else action
"#;

/// Parse the prelude source and prepend its declarations to the user's module.
/// User declarations with the same name shadow the prelude versions.
pub fn inject_prelude(module: &mut ast::Module) {
    let lexer = knot::lexer::Lexer::new(PRELUDE_SOURCE);
    let (tokens, lex_diags) = lexer.tokenize();
    assert!(
        !lex_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error),
        "prelude failed to lex: {:?}",
        lex_diags
    );
    let parser = knot::parser::Parser::new(PRELUDE_SOURCE.to_string(), tokens);
    let (parsed, parse_diags) = parser.parse_module();
    assert!(
        !parse_diags
            .iter()
            .any(|d| d.severity == knot::diagnostic::Severity::Error),
        "prelude failed to parse: {:?}",
        parse_diags
    );

    let user_names: std::collections::HashSet<&str> = module
        .decls
        .iter()
        .filter_map(|d| match &d.node {
            ast::DeclKind::Fun { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();

    let mut filtered: Vec<ast::Decl> = parsed
        .decls
        .into_iter()
        .filter(|d| match &d.node {
            ast::DeclKind::Fun { name, .. } => !user_names.contains(name.as_str()),
            _ => true,
        })
        .collect();

    for decl in &mut filtered {
        shift_decl_spans(decl, PRELUDE_SPAN_OFFSET);
    }

    // Prepend prelude declarations before user declarations
    let mut all_decls = filtered;
    all_decls.append(&mut module.decls);
    module.decls = all_decls;
}

// ── Prelude span shifting (bug B39) ──────────────────────────────────
//
// Add `offset` to every declaration/expression/statement/pattern span in a
// prelude decl so prelude spans can never alias user-file spans in
// `monad_info` (and the other span-keyed inference maps). Type spans are left
// alone — they never key `monad_info`. Mirrors the AST shape walked by
// `unused::walk_decl`; keep the two in sync when the AST grows a node.
//
// "Every span" includes the standalone `name_span`/`api_span` fields, not just
// the `Spanned` wrappers: inference keys a punned record field's binder on
// `FieldPat::name_span`, so leaving it unshifted leaks a raw PRELUDE_SOURCE
// offset into `local_type_info` — a span the LSP cannot tell apart from a
// user span, since its provenance filter can only compare byte ranges. It
// then anchors an inlay hint at that offset in the user's file.

fn shift_decl_spans(decl: &mut ast::Decl, offset: usize) {
    use ast::DeclKind::*;
    decl.span.start += offset;
    decl.span.end += offset;
    match &mut decl.node {
        Fun { body, .. } => {
            if let Some(b) = body {
                shift_expr_spans(b, offset);
            }
        }
        View { body, .. } | Derived { body, .. } => shift_expr_spans(body, offset),
        Route { entries, .. } => {
            for e in entries {
                if let Some(expr) = &mut e.rate_limit {
                    shift_expr_spans(expr, offset);
                }
            }
        }
        // No embedded expressions to shift.
        Data { .. } | TypeAlias { .. } | Source { .. } | RouteComposite { .. }
        | SubsetConstraint { .. } => {}
    }
}

fn shift_expr_spans(e: &mut ast::Expr, offset: usize) {
    use ast::ExprKind::*;
    e.span.start += offset;
    e.span.end += offset;
    match &mut e.node {
        App { func, arg } => {
            shift_expr_spans(func, offset);
            shift_expr_spans(arg, offset);
        }
        With { record, body } => {
            shift_expr_spans(record, offset);
            shift_expr_spans(body, offset);
        }
        Lambda { params, body, .. } => {
            for p in params {
                shift_pat_spans(p, offset);
            }
            shift_expr_spans(body, offset);
        }
        BinOp { lhs, rhs, .. } => {
            shift_expr_spans(lhs, offset);
            shift_expr_spans(rhs, offset);
        }
        UnaryOp { operand, .. } => shift_expr_spans(operand, offset),
        If { cond, then_branch, else_branch } => {
            shift_expr_spans(cond, offset);
            shift_expr_spans(then_branch, offset);
            shift_expr_spans(else_branch, offset);
        }
        Case { scrutinee, arms } => {
            shift_expr_spans(scrutinee, offset);
            for arm in arms {
                shift_pat_spans(&mut arm.pat, offset);
                shift_expr_spans(&mut arm.body, offset);
            }
        }
        Do(stmts) => {
            for s in stmts {
                shift_stmt_spans(s, offset);
            }
        }
        Set { target, value } | ReplaceSet { target, value } => {
            shift_expr_spans(target, offset);
            shift_expr_spans(value, offset);
        }
        Atomic(inner) | Refine(inner) => shift_expr_spans(inner, offset),
        TimeUnitLit { value, .. } => shift_expr_spans(value, offset),
        Record(fields) => {
            for fl in fields {
                shift_expr_spans(&mut fl.value, offset);
            }
        }
        RecordUpdate { base, fields } => {
            shift_expr_spans(base, offset);
            for fl in fields {
                shift_expr_spans(&mut fl.value, offset);
            }
        }
        List(items) => {
            for it in items {
                shift_expr_spans(it, offset);
            }
        }
        FieldAccess { expr, .. } | Annot { expr, .. } => shift_expr_spans(expr, offset),
        Serve { api_span, handlers, .. } => {
            api_span.start += offset;
            api_span.end += offset;
            for h in handlers {
                h.endpoint_span.start += offset;
                h.endpoint_span.end += offset;
                shift_expr_spans(&mut h.body, offset);
            }
        }
        Lit(_) | Var(_) | Constructor(_) | SourceRef(_) | DerivedRef(_) | ImplicitRef(_) => {}
        TypeCtor { .. } | DataCtor { .. } | SourceDecl { .. } | SubsetConstraint { .. } => {}
        RouteDecl { .. } | RouteCompositeDecl { .. } => {}
        ViewDecl { body, .. } | DerivedDecl { body, .. } => shift_expr_spans(body, offset),
    }
}

fn shift_stmt_spans(s: &mut ast::Stmt, offset: usize) {
    use ast::StmtKind::*;
    s.span.start += offset;
    s.span.end += offset;
    match &mut s.node {
        Bind { pat, expr } => {
            shift_pat_spans(pat, offset);
            shift_expr_spans(expr, offset);
        }
        Where { cond } => shift_expr_spans(cond, offset),
        GroupBy { key } => shift_expr_spans(key, offset),
        Expr(e) => shift_expr_spans(e, offset),
    }
}

fn shift_pat_spans(p: &mut ast::Pat, offset: usize) {
    use ast::PatKind::*;
    p.span.start += offset;
    p.span.end += offset;
    match &mut p.node {
        Var(_) | Wildcard | Lit(_) => {}
        Constructor { payload, .. } => shift_pat_spans(payload, offset),
        Record(fields) => {
            for fp in fields {
                // The field-name token's own span. For a punned field
                // (`{value}`) this IS the binder's span, and inference records
                // it in `binding_types` — so it must be shifted like any other
                // binder span, or it escapes as a raw prelude offset.
                fp.name_span.start += offset;
                fp.name_span.end += offset;
                if let Some(inner) = &mut fp.pattern {
                    shift_pat_spans(inner, offset);
                }
            }
        }
        List(items) => {
            for it in items {
                shift_pat_spans(it, offset);
            }
        }
        Cons { head, tail } => {
            shift_pat_spans(head, offset);
            shift_pat_spans(tail, offset);
        }
        Annot { pat, .. } => shift_pat_spans(pat, offset),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use knot::ast::Span;

    /// Smallest `span.start` anywhere in an expression subtree.
    fn min_expr_span(e: &ast::Expr) -> usize {
        use ast::ExprKind::*;
        let children: Vec<&ast::Expr> = match &e.node {
            App { func, arg } => vec![func, arg],
            Lambda { body, .. } => vec![body],
            If { cond, then_branch, else_branch } => vec![cond, then_branch, else_branch],
            _ => vec![],
        };
        children
            .iter()
            .map(|c| min_expr_span(c))
            .fold(e.span.start, usize::min)
    }

    /// Bug B39: the prelude is parsed from its own string constant, so its byte
    /// offsets start at 0 and alias user-file offsets. `monad_info` (infer.rs)
    /// is keyed by raw `Span`, and prelude constructs feed it real spans — the
    /// bare `yield {}` in when/unless/forEach is a `yield` monad-var keyed by
    /// its offset. When a user expression lands at the same offset,
    /// last-write-wins on `monad_info.insert` re-dispatches one side's `yield`
    /// through the other's Applicative. Shifting every prelude span past
    /// `PRELUDE_SPAN_OFFSET` guarantees they can never collide with user spans.
    #[test]
    fn prelude_spans_shifted_out_of_user_range() {
        let mut module = ast::Module { imports: vec![], decls: vec![] };
        inject_prelude(&mut module);

        // Every decl came from the prelude (the user module was empty), so all
        // of their spans — down to the nested `yield` expressions that feed
        // `monad_info` — must be lifted past the offset.
        assert!(!module.decls.is_empty(), "prelude should inject decls");
        let mut saw_when_yield = false;
        for decl in &module.decls {
            assert!(
                decl.span.start >= PRELUDE_SPAN_OFFSET,
                "prelude decl span {} not shifted past PRELUDE_SPAN_OFFSET",
                decl.span.start,
            );
            // Reach into `when`'s body and confirm the bare `yield {}` span —
            // the exact node that collides in `monad_info` — is shifted too.
            if let ast::DeclKind::Fun { name, body: Some(body), .. } = &decl.node {
                if name == "when" {
                    assert!(
                        min_expr_span(body) >= PRELUDE_SPAN_OFFSET,
                        "when body has an unshifted span",
                    );
                    saw_when_yield = true;
                }
            }
        }
        assert!(saw_when_yield, "prelude should define `when` with a body");

        // The offset must also clear the synthesized monad-span range
        // (`desugar::SYNTH_SPAN_BASE` = 1 << 31) so a prelude span can't alias a
        // desugared `__bind`/`__yield` helper span either.
        // (Clippy: assertions_on_constants — use a const check instead.)
        const _: () = {
            assert!(PRELUDE_SPAN_OFFSET > (1usize << 31));
        };
    }

    /// Collect EVERY value-level span in a decl — including the standalone
    /// `name_span`/`api_span`/`endpoint_span` fields, which are easy to forget
    /// because they aren't `Spanned` wrappers. Type spans are excluded: the
    /// shifter deliberately leaves them alone (they never key a span map).
    ///
    /// Deliberately written as an independent walk rather than reusing the
    /// shifter's traversal, so a field the shifter forgets is still visited
    /// here and the assertion below catches it.
    fn collect_value_spans(decl: &ast::Decl, out: &mut Vec<(&'static str, Span)>) {
        fn pat(p: &ast::Pat, out: &mut Vec<(&'static str, Span)>) {
            out.push(("pat", p.span));
            match &p.node {
                ast::PatKind::Var(_) | ast::PatKind::Wildcard | ast::PatKind::Lit(_) => {}
                ast::PatKind::Constructor { payload, .. } => pat(payload, out),
                ast::PatKind::Record(fields) => {
                    for fp in fields {
                        out.push(("FieldPat::name_span", fp.name_span));
                        if let Some(inner) = &fp.pattern {
                            pat(inner, out);
                        }
                    }
                }
                ast::PatKind::List(items) => items.iter().for_each(|i| pat(i, out)),
                ast::PatKind::Cons { head, tail } => {
                    pat(head, out);
                    pat(tail, out);
                }
                ast::PatKind::Annot { pat: inner, .. } => pat(inner, out),
            }
        }
        fn expr(e: &ast::Expr, out: &mut Vec<(&'static str, Span)>) {
            out.push(("expr", e.span));
            if let ast::ExprKind::Serve { api_span, handlers, .. } = &e.node {
                out.push(("Serve::api_span", *api_span));
                for h in handlers {
                    out.push(("ServeHandler::endpoint_span", h.endpoint_span));
                }
            }
            if let ast::ExprKind::Lambda { params, .. } = &e.node {
                params.iter().for_each(|p| pat(p, out));
            }
            if let ast::ExprKind::Case { arms, .. } = &e.node {
                arms.iter().for_each(|a| pat(&a.pat, out));
            }
            if let ast::ExprKind::Do(stmts) = &e.node {
                for s in stmts {
                    out.push(("stmt", s.span));
                    if let ast::StmtKind::Bind { pat: p, .. } = &s.node {
                        pat(p, out);
                    }
                }
            }
            recurse(e, |c| expr(c, out));
        }

        out.push(("decl", decl.span));
        match &decl.node {
            ast::DeclKind::Fun { body: Some(b), .. } => expr(b, out),
            ast::DeclKind::View { body, .. } | ast::DeclKind::Derived { body, .. } => {
                expr(body, out)
            }
            ast::DeclKind::Route { entries, .. } => {
                for e in entries {
                    if let Some(rl) = &e.rate_limit {
                        expr(rl, out);
                    }
                }
            }
            _ => {}
        }
    }

    /// Re-export the shared sub-expression walk for `collect_value_spans`.
    fn recurse<F: FnMut(&ast::Expr)>(e: &ast::Expr, mut f: F) {
        use ast::ExprKind::*;
        match &e.node {
            App { func, arg } => {
                f(func);
                f(arg);
            }
            With { record, body } => {
                f(record);
                f(body);
            }
            Lambda { body, .. } => f(body),
            BinOp { lhs, rhs, .. } => {
                f(lhs);
                f(rhs);
            }
            UnaryOp { operand, .. } => f(operand),
            If { cond, then_branch, else_branch } => {
                f(cond);
                f(then_branch);
                f(else_branch);
            }
            Case { scrutinee, arms } => {
                f(scrutinee);
                arms.iter().for_each(|a| f(&a.body));
            }
            Do(stmts) => {
                for s in stmts {
                    match &s.node {
                        ast::StmtKind::Bind { expr, .. }
                        | ast::StmtKind::Expr(expr)
                        | ast::StmtKind::Where { cond: expr } => f(expr),
                        ast::StmtKind::GroupBy { key } => f(key),
                    }
                }
            }
            Set { target, value } | ReplaceSet { target, value } => {
                f(target);
                f(value);
            }
            Atomic(i) | Refine(i) => f(i),
            Record(fields) => fields.iter().for_each(|fl| f(&fl.value)),
            RecordUpdate { base, fields } => {
                f(base);
                fields.iter().for_each(|fl| f(&fl.value));
            }
            List(items) => items.iter().for_each(f),
            FieldAccess { expr, .. } | Annot { expr, .. } => f(expr),
            TimeUnitLit { value, .. } => f(value),
            Serve { handlers, .. } => handlers.iter().for_each(|h| f(&h.body)),
            ViewDecl { body, .. } | DerivedDecl { body, .. } => f(body),
            Lit(_) | Var(_) | Constructor(_) | SourceRef(_) | DerivedRef(_) | ImplicitRef(_) => {}
            TypeCtor { .. } | DataCtor { .. } | SourceDecl { .. } | SubsetConstraint { .. } => {}
        RouteDecl { .. } | RouteCompositeDecl { .. } => {}
        }
    }

    /// GitHub issue #4 — "wrong inlay hint position sometimes".
    ///
    /// `prelude_spans_shifted_out_of_user_range` (above) only spot-checks decl
    /// spans and `when`'s body, so it missed `FieldPat::name_span`: the prelude's
    /// Maybe impls destructure `Just {value}`, and for a PUNNED field that span
    /// is the binder's span, which inference records in `binding_types`. Left
    /// unshifted it escaped as a raw PRELUDE_SOURCE offset (1303/1640/1850) and
    /// the LSP — whose provenance filter can only compare byte ranges — anchored
    /// an inlay hint there, mid-token, in whatever user file straddled it.
    ///
    /// Assert exhaustively instead of by spot-check: NO value-level span in any
    /// prelude decl may remain in user-file range.
    #[test]
    fn every_prelude_value_span_is_shifted() {
        let mut module = ast::Module { imports: vec![], decls: vec![] };
        inject_prelude(&mut module);
        assert!(!module.decls.is_empty(), "prelude should inject decls");

        let mut spans = Vec::new();
        for decl in &module.decls {
            collect_value_spans(decl, &mut spans);
        }

        let leaked: Vec<_> = spans
            .iter()
            .filter(|(_, s)| s.start < PRELUDE_SPAN_OFFSET || s.end < PRELUDE_SPAN_OFFSET)
            .collect();
        assert!(
            leaked.is_empty(),
            "prelude spans left in user-file range (they will alias user offsets \
             and misplace inlay hints): {leaked:?}",
        );
    }
}
