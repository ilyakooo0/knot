//! Helpers shared across multiple LSP feature modules. Anything that's used by
//! exactly one feature lives in that feature's module; this file is for the
//! cross-feature utilities (route formatting, atomic-context detection, etc.).

use std::path::{Path, PathBuf};

use knot::ast::{self, DeclKind, Module, Span};
use knot_compiler::effects::EffectSet;

use crate::type_format::format_type_kind;
use crate::utils::{recurse_expr, safe_slice};

// ── Signature rendering ─────────────────────────────────────────────

/// Render a type signature for "Add type annotation" suggestions, using the
/// per-decl `effect_sets` analysis to populate the IO effect row when the
/// inferred type has one. Effects belong inside the `IO { … }` row, not as a
/// prefix on the function — this helper only adjusts the row's contents.
///
/// HM inference and the effect-checker are separate passes. In some cases
/// (forward references through annotated callers), HM closes a function's IO
/// row to `{}` and silently drops body-side effects. The effect-checker is
/// precise per declaration, so when it disagrees with the rendered type's IO
/// row, prefer the effect-checker's view.
///
/// Falls back to `inferred` unchanged when:
/// - the type contains no `IO { … }` row (the function isn't IO),
/// - the effect set is pure (nothing to add),
/// - the rendered IO row already contains every effect the set knows about.
pub(crate) fn render_signature_with_effects(inferred: &str, effects: &EffectSet) -> String {
    if effects.is_pure() {
        return inferred.to_string();
    }
    let Some((row_start, row_end)) = find_outermost_io_row(inferred) else {
        return inferred.to_string();
    };
    let existing_row = &inferred[row_start..row_end];
    let merged = merge_effects_into_row(existing_row, effects);
    if merged == existing_row {
        return inferred.to_string();
    }
    format!("{}{}{}", &inferred[..row_start], merged, &inferred[row_end..])
}

/// Find the byte range of the contents (between `{` and `}`) of the *last*
/// `IO { … }` row in the rendered type — the result-position one for a
/// function type. Returns `None` when no IO row is present.
fn find_outermost_io_row(ty: &str) -> Option<(usize, usize)> {
    let bytes = ty.as_bytes();
    let mut last: Option<(usize, usize)> = None;
    let mut search_from = 0;
    while let Some(io_pos) = ty[search_from..].find("IO {") {
        let row_start = search_from + io_pos + 4;
        let mut depth: i32 = 1;
        let mut i = row_start;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => depth -= 1,
                _ => {}
            }
            if depth == 0 {
                break;
            }
            i += 1;
        }
        if depth != 0 {
            return None;
        }
        last = Some((row_start, i));
        search_from = i + 1;
    }
    last
}

/// Merge any effects from `effects` that aren't already named in
/// `existing_row` into the row, preserving the row's existing tokens (and any
/// trailing row variable like `| r`). Returns the new row contents (no braces).
fn merge_effects_into_row(existing_row: &str, effects: &EffectSet) -> String {
    let trimmed = existing_row.trim();
    let (effects_part, row_var): (&str, Option<String>) = match trimmed.split_once('|') {
        Some((before, tail)) => (before, Some(format!("| {}", tail.trim()))),
        None => (trimmed, None),
    };
    let existing_effects: Vec<String> = effects_part
        .split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .map(String::from)
        .collect();

    let mut have: std::collections::BTreeSet<String> =
        existing_effects.iter().cloned().collect();
    let mut additions: Vec<String> = Vec::new();
    let read_write: std::collections::BTreeSet<&String> =
        effects.reads.intersection(&effects.writes).collect();
    for name in &effects.reads {
        if read_write.contains(name) {
            continue;
        }
        let s = format!("r *{name}");
        if have.insert(s.clone()) {
            additions.push(s);
        }
    }
    for name in &effects.writes {
        if read_write.contains(name) {
            continue;
        }
        let s = format!("w *{name}");
        if have.insert(s.clone()) {
            additions.push(s);
        }
    }
    for name in &read_write {
        let s = format!("rw *{name}");
        if have.insert(s.clone()) {
            additions.push(s);
        }
    }
    for (flag, name) in [
        (effects.console, "console"),
        (effects.network, "network"),
        (effects.fs, "fs"),
        (effects.clock, "clock"),
        (effects.random, "random"),
    ] {
        if flag {
            let s = name.to_string();
            if have.insert(s.clone()) {
                additions.push(s);
            }
        }
    }

    if additions.is_empty() {
        return existing_row.to_string();
    }

    let mut parts = existing_effects;
    parts.extend(additions);
    let body = parts.join(", ");
    match row_var {
        Some(rv) => format!("{body} {rv}"),
        None => body,
    }
}

#[cfg(test)]
mod sig_tests {
    use super::*;

    fn effects(reads: &[&str], writes: &[&str]) -> EffectSet {
        let mut e = EffectSet::empty();
        for r in reads {
            e.reads.insert((*r).to_string());
        }
        for w in writes {
            e.writes.insert((*w).to_string());
        }
        e
    }

    #[test]
    fn passthrough_when_pure() {
        let s = render_signature_with_effects("Int -> Int", &EffectSet::empty());
        assert_eq!(s, "Int -> Int");
    }

    #[test]
    fn passthrough_when_no_io_row() {
        // No IO row in the type → don't invent one, even if the effect set
        // claims effects (shouldn't happen in practice, but be safe).
        let s = render_signature_with_effects("Int -> Int", &effects(&["foo"], &[]));
        assert_eq!(s, "Int -> Int");
    }

    #[test]
    fn fills_empty_io_row_with_relation_effects() {
        let s = render_signature_with_effects(
            "Timestamp -> IO {} Bool",
            &effects(&["globalRateCount"], &["globalRateCount"]),
        );
        assert_eq!(
            s,
            "Timestamp -> IO {rw *globalRateCount} Bool"
        );
    }

    #[test]
    fn appends_missing_effects_to_existing_row() {
        let mut e = effects(&[], &[]);
        e.console = true;
        let s = render_signature_with_effects("Text -> IO {fs} {}", &e);
        assert_eq!(s, "Text -> IO {fs, console} {}");
    }

    #[test]
    fn no_change_when_io_row_already_complete() {
        let mut e = EffectSet::empty();
        e.fs = true;
        let s = render_signature_with_effects("Text -> IO {fs} Text", &e);
        assert_eq!(s, "Text -> IO {fs} Text");
    }

    #[test]
    fn modifies_only_outermost_io_row() {
        // Inner IO (callback type) must stay untouched; only the result-position
        // IO row (the function's own return) gets effects added.
        let mut e = EffectSet::empty();
        e.console = true;
        let s = render_signature_with_effects(
            "(a -> IO {fs} b) -> IO {} a",
            &e,
        );
        assert_eq!(s, "(a -> IO {fs} b) -> IO {console} a");
    }

    #[test]
    fn preserves_row_variable_tail() {
        let mut e = EffectSet::empty();
        e.console = true;
        let s = render_signature_with_effects("Int -> IO {fs | r} Int", &e);
        assert_eq!(s, "Int -> IO {fs, console | r} Int");
    }
}

// ── Type-string parsing ─────────────────────────────────────────────

/// Extract the principal named type from a type string.
/// E.g., "[Person]" -> "Person", "Maybe Text" -> "Maybe",
/// "Int -> Text" -> None (functions have no single type def),
/// "{name: Text}" -> None (anonymous records).
pub(crate) fn extract_principal_type_name(type_str: &str) -> Option<String> {
    let s = type_str.trim();

    // Strip relation brackets: [T] -> T
    if s.starts_with('[') && s.ends_with(']') {
        return extract_principal_type_name(&s[1..s.len() - 1]);
    }

    // Strip IO wrapper: IO {effects} T -> T
    if s.starts_with("IO ") {
        let rest = &s[3..];
        if rest.starts_with('{') {
            if let Some(close) = rest.find('}') {
                return extract_principal_type_name(rest[close + 1..].trim());
            }
        }
        return extract_principal_type_name(rest);
    }

    // Anonymous record — no named type
    if s.starts_with('{') {
        return None;
    }

    // Variant type — no single named type
    if s.starts_with('<') {
        return None;
    }

    // Function type — no single named type
    if s.contains(" -> ") {
        return None;
    }

    // Named type (possibly with params): "Person", "Maybe Text", "Result Text Int"
    // Take the first word as the type name
    let name = s.split_whitespace().next()?;

    // Must start with uppercase to be a concrete type name
    if name.chars().next()?.is_uppercase() {
        Some(name.to_string())
    } else {
        None
    }
}

// ── Workspace file scanning ─────────────────────────────────────────

/// Recursively find all .knot files under a directory.
pub(crate) fn scan_knot_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    scan_knot_files_recursive(dir, &mut files)?;
    Ok(files)
}

/// Recursively find all .knot files under any of the given roots. Used by
/// workspace-wide handlers (auto-import, workspace symbol, workspace
/// diagnostics) that should see every folder the editor surfaced, not just
/// the first. Falls back to `legacy_root` when `roots` is empty so single-root
/// callers stay correct.
pub(crate) fn scan_knot_files_in_roots(
    roots: &[PathBuf],
    legacy_root: Option<&Path>,
) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let dirs: Vec<&Path> = if roots.is_empty() {
        legacy_root.into_iter().collect()
    } else {
        roots.iter().map(|p| p.as_path()).collect()
    };
    for dir in dirs {
        if let Ok(files) = scan_knot_files(dir) {
            for f in files {
                if let Ok(canonical) = f.canonicalize() {
                    if seen.insert(canonical.clone()) {
                        out.push(canonical);
                    }
                } else if seen.insert(f.clone()) {
                    out.push(f);
                }
            }
        }
    }
    out
}

pub(crate) fn scan_knot_files_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            // Skip hidden dirs and common non-source dirs
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.starts_with('.') && name != "target" && name != "node_modules" {
                scan_knot_files_recursive(&path, files)?;
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("knot") {
            files.push(path);
        }
    }
    Ok(())
}

// ── Route formatting ────────────────────────────────────────────────

/// English plural suffix for counts. `1 view`, `2 views`.
pub(crate) fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// Format a Knot HTTP method as the literal HTTP verb.
pub(crate) fn http_method_str(m: ast::HttpMethod) -> &'static str {
    match m {
        ast::HttpMethod::Get => "GET",
        ast::HttpMethod::Post => "POST",
        ast::HttpMethod::Put => "PUT",
        ast::HttpMethod::Delete => "DELETE",
        ast::HttpMethod::Patch => "PATCH",
    }
}

/// Render a route entry's path with typed `{name: Type}` placeholders.
pub(crate) fn format_route_path(entry: &ast::RouteEntry) -> String {
    let mut out = String::new();
    for seg in &entry.path {
        match seg {
            ast::PathSegment::Literal(s) => {
                out.push('/');
                out.push_str(s);
            }
            ast::PathSegment::Param { name, ty } => {
                out.push('/');
                out.push('{');
                out.push_str(name);
                out.push_str(": ");
                out.push_str(&format_type_kind(&ty.node));
                out.push('}');
            }
        }
    }
    if out.is_empty() {
        "/".to_string()
    } else {
        out
    }
}

/// Returns true if any decl in the module composes the given route into a
/// `listen port handler` call. Used by the dead-route lint.
pub(crate) fn route_is_listened(module: &Module, route_name: &str) -> bool {
    fn walk(expr: &ast::Expr, route_name: &str, found: &mut bool) {
        if *found {
            return;
        }
        if let ast::ExprKind::App { func, arg } = &expr.node {
            // Detect `listen port handler` where one argument references the route.
            // The handler's body typically destructures the route ADT, so any reference
            // to the route name (constructor case-match) inside a `listen` call is
            // a strong signal that the route is wired in.
            if app_callee_is(func, "listen") && expr_references_name(arg, route_name) {
                *found = true;
                return;
            }
        }
        recurse_expr(expr, |e| walk(e, route_name, found));
    }
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun {
                body: Some(body), ..
            }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                let mut found = false;
                walk(body, route_name, &mut found);
                if found {
                    return true;
                }
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        let mut found = false;
                        walk(body, route_name, &mut found);
                        if found {
                            return true;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    false
}

/// True if `expr` is application chain whose head is `Var(name)` (e.g.
/// `name`, `name x`, `name x y`).
pub(crate) fn app_callee_is(expr: &ast::Expr, name: &str) -> bool {
    match &expr.node {
        ast::ExprKind::Var(n) => n == name,
        ast::ExprKind::App { func, .. } => app_callee_is(func, name),
        _ => false,
    }
}

/// True if any sub-expression of `expr` references `name` as a `Var` or `Constructor`.
/// Used to decide whether a `listen` call's argument is wired to a given route.
pub(crate) fn expr_references_name(expr: &ast::Expr, name: &str) -> bool {
    let mut found = false;
    fn walk(expr: &ast::Expr, name: &str, found: &mut bool) {
        if *found {
            return;
        }
        match &expr.node {
            ast::ExprKind::Var(n) | ast::ExprKind::Constructor(n) if n == name => {
                *found = true;
            }
            _ => recurse_expr(expr, |e| walk(e, name, found)),
        }
    }
    walk(expr, name, &mut found);
    found
}

// ── Application-chain lookup (signature help / hover) ───────────────

/// Walk the AST to find the innermost function application chain containing the cursor.
/// Returns (function_name, active_parameter_index).
pub(crate) fn find_enclosing_application(
    module: &Module,
    source: &str,
    offset: usize,
) -> Option<(String, usize)> {
    let mut best: Option<(String, usize, usize)> = None; // (name, param_idx, span_size)

    for decl in &module.decls {
        if decl.span.start > offset || offset > decl.span.end {
            continue;
        }
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                find_app_in_expr(body, source, offset, &mut best);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        find_app_in_expr(body, source, offset, &mut best);
                    }
                }
            }
            DeclKind::Trait { items, .. } => {
                for item in items {
                    if let ast::TraitItem::Method { default_body: Some(body), .. } = item {
                        find_app_in_expr(body, source, offset, &mut best);
                    }
                }
            }
            _ => {}
        }
    }

    best.map(|(name, idx, _)| (name, idx))
}

pub(crate) fn find_app_in_expr(
    expr: &ast::Expr,
    source: &str,
    offset: usize,
    best: &mut Option<(String, usize, usize)>,
) {
    if expr.span.start > offset || offset > expr.span.end {
        return;
    }

    // Check if this is an App chain
    if let ast::ExprKind::App { .. } = &expr.node {
        // Flatten the App spine: f a b c is App(App(App(f, a), b), c)
        let mut args = Vec::new();
        let mut cur = expr;
        while let ast::ExprKind::App { func, arg } = &cur.node {
            args.push(arg.as_ref());
            cur = func.as_ref();
        }
        args.reverse();

        // cur is now the function at the head
        let func_name = match &cur.node {
            ast::ExprKind::Var(name) => Some(name.clone()),
            ast::ExprKind::Constructor(name) => Some(name.clone()),
            _ => None,
        };

        if let Some(name) = func_name {
            // Determine which argument the cursor is in
            let mut param_idx = args.len(); // default: past the last arg (next param)
            for (i, arg) in args.iter().enumerate() {
                if offset <= arg.span.start {
                    param_idx = i;
                    break;
                }
                if offset >= arg.span.start && offset <= arg.span.end {
                    param_idx = i;
                    break;
                }
            }

            let span_size = expr.span.end - expr.span.start;
            // Prefer the smallest (innermost) enclosing application
            if best.as_ref().map_or(true, |b| span_size <= b.2) {
                *best = Some((name, param_idx, span_size));
            }
        }
    }

    // Recurse into sub-expressions
    match &expr.node {
        ast::ExprKind::App { func, arg } => {
            find_app_in_expr(func, source, offset, best);
            find_app_in_expr(arg, source, offset, best);
        }
        ast::ExprKind::Lambda { body, .. } => {
            find_app_in_expr(body, source, offset, best);
        }
        ast::ExprKind::BinOp { lhs, rhs, .. } => {
            find_app_in_expr(lhs, source, offset, best);
            find_app_in_expr(rhs, source, offset, best);
        }
        ast::ExprKind::UnaryOp { operand, .. } => {
            find_app_in_expr(operand, source, offset, best);
        }
        ast::ExprKind::If { cond, then_branch, else_branch } => {
            find_app_in_expr(cond, source, offset, best);
            find_app_in_expr(then_branch, source, offset, best);
            find_app_in_expr(else_branch, source, offset, best);
        }
        ast::ExprKind::Case { scrutinee, arms } => {
            find_app_in_expr(scrutinee, source, offset, best);
            for arm in arms {
                find_app_in_expr(&arm.body, source, offset, best);
            }
        }
        ast::ExprKind::Do(stmts) => {
            for stmt in stmts {
                match &stmt.node {
                    ast::StmtKind::Bind { expr, .. } | ast::StmtKind::Let { expr, .. } => {
                        find_app_in_expr(expr, source, offset, best);
                    }
                    ast::StmtKind::Expr(e) | ast::StmtKind::Where { cond: e } => {
                        find_app_in_expr(e, source, offset, best);
                    }
                    ast::StmtKind::GroupBy { key } => {
                        find_app_in_expr(key, source, offset, best);
                    }
                }
            }
        }
        ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => find_app_in_expr(e, source, offset, best),
        ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
            find_app_in_expr(target, source, offset, best);
            find_app_in_expr(value, source, offset, best);
        }
        ast::ExprKind::Record(fields) => {
            for f in fields {
                find_app_in_expr(&f.value, source, offset, best);
            }
        }
        ast::ExprKind::RecordUpdate { base, fields } => {
            find_app_in_expr(base, source, offset, best);
            for f in fields {
                find_app_in_expr(&f.value, source, offset, best);
            }
        }
        ast::ExprKind::List(elems) => {
            for e in elems {
                find_app_in_expr(e, source, offset, best);
            }
        }
        ast::ExprKind::FieldAccess { expr, .. } => {
            find_app_in_expr(expr, source, offset, best);
        }
        ast::ExprKind::At { relation, time } => {
            find_app_in_expr(relation, source, offset, best);
            find_app_in_expr(time, source, offset, best);
        }
        _ => {}
    }
}

/// Split a Knot type string like `Int -> Text -> Bool` into the rendered
/// parameter types (and the trailing return). Backed by `ParsedType` so
/// effect rows (`IO {fx} a`), record nesting, and constraint prefixes
/// (`Display a => …`) don't fool the splitter.
pub(crate) fn parse_function_params(type_str: &str) -> Vec<String> {
    let parsed = crate::parsed_type::ParsedType::parse(type_str);
    match parsed {
        crate::parsed_type::ParsedType::Function(params, ret) => {
            let mut out: Vec<String> = params.iter().map(|p| p.render()).collect();
            out.push(ret.render());
            out
        }
        // Constraint prefix gets stripped during parsing, but if the parser
        // bailed entirely fall back to returning the whole string as a
        // single non-parametric "type" rather than producing wrong arity.
        _ => Vec::new(),
    }
}

// ── Atomic-context detection (completion / code action) ─────────────

/// Find the enclosing `atomic expr` and return `(atomic_span, inner_source_text)`
/// so we can replace `atomic e` with `e`. Returns None if no atomic wraps the
/// given offset.
pub(crate) fn find_enclosing_atomic_expr(
    module: &Module,
    source: &str,
    offset: usize,
) -> Option<(Span, String)> {
    fn walk(expr: &ast::Expr, source: &str, offset: usize, best: &mut Option<(Span, String)>) {
        if expr.span.start > offset || offset > expr.span.end {
            return;
        }
        if let ast::ExprKind::Atomic(inner) = &expr.node {
            let inner_text = safe_slice(source, inner.span).to_string();
            // Track the smallest enclosing atomic
            let size = expr.span.end - expr.span.start;
            if best
                .as_ref()
                .map_or(true, |b: &(Span, String)| size < b.0.end - b.0.start)
            {
                *best = Some((expr.span, inner_text));
            }
        }
        // Recurse
        match &expr.node {
            ast::ExprKind::App { func, arg } => {
                walk(func, source, offset, best);
                walk(arg, source, offset, best);
            }
            // Don't recurse into lambda bodies: a lambda is a deferred
            // computation that runs when (and where) it's eventually called,
            // not in the atomic context that lexically encloses its
            // definition. `fork (\_ -> println ...)` inside `atomic` should
            // not flag `println` as atomic-disallowed.
            ast::ExprKind::Lambda { .. } => {}
            ast::ExprKind::BinOp { lhs, rhs, .. } => {
                walk(lhs, source, offset, best);
                walk(rhs, source, offset, best);
            }
            ast::ExprKind::UnaryOp { operand, .. } => walk(operand, source, offset, best),
            ast::ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } => {
                walk(cond, source, offset, best);
                walk(then_branch, source, offset, best);
                walk(else_branch, source, offset, best);
            }
            ast::ExprKind::Case { scrutinee, arms } => {
                walk(scrutinee, source, offset, best);
                for arm in arms {
                    walk(&arm.body, source, offset, best);
                }
            }
            ast::ExprKind::Do(stmts) => {
                for stmt in stmts {
                    match &stmt.node {
                        ast::StmtKind::Bind { expr, .. }
                        | ast::StmtKind::Let { expr, .. }
                        | ast::StmtKind::Expr(expr)
                        | ast::StmtKind::Where { cond: expr } => walk(expr, source, offset, best),
                        ast::StmtKind::GroupBy { key } => walk(key, source, offset, best),
                    }
                }
            }
            ast::ExprKind::Atomic(e) | ast::ExprKind::Refine(e) => walk(e, source, offset, best),
            ast::ExprKind::Set { target, value } | ast::ExprKind::ReplaceSet { target, value } => {
                walk(target, source, offset, best);
                walk(value, source, offset, best);
            }
            ast::ExprKind::Record(fields) => {
                for f in fields {
                    walk(&f.value, source, offset, best);
                }
            }
            ast::ExprKind::RecordUpdate { base, fields } => {
                walk(base, source, offset, best);
                for f in fields {
                    walk(&f.value, source, offset, best);
                }
            }
            ast::ExprKind::List(elems) => {
                for e in elems {
                    walk(e, source, offset, best);
                }
            }
            ast::ExprKind::FieldAccess { expr, .. } => walk(expr, source, offset, best),
            ast::ExprKind::At { relation, time } => {
                walk(relation, source, offset, best);
                walk(time, source, offset, best);
            }
            ast::ExprKind::Annot { expr, .. } => walk(expr, source, offset, best),
            ast::ExprKind::UnitLit { value, .. } => walk(value, source, offset, best),
            _ => {}
        }
    }

    let mut best: Option<(Span, String)> = None;
    for decl in &module.decls {
        if decl.span.start > offset || offset > decl.span.end {
            continue;
        }
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => walk(body, source, offset, &mut best),
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk(body, source, offset, &mut best);
                    }
                }
            }
            _ => {}
        }
    }
    best
}

// ── Hover-only helpers used by completion-resolve too ───────────────

/// Render a predicate expression as its source text. Falls back to a placeholder
/// when the span is empty or out of bounds (defensive: predicates always have
/// real spans, but the LSP is also fed by the import cache, which can outlive
/// edits to the source).
pub(crate) fn predicate_to_source(expr: &ast::Expr, source: &str) -> String {
    let span = expr.span;
    if span.start < span.end && span.end <= source.len() {
        source[span.start..span.end].to_string()
    } else {
        "<predicate>".to_string()
    }
}

/// Find a route entry by its constructor name and render a hover summary
/// (method + path + body/query/headers/response). Returns `None` if no route
/// declares this constructor.
pub(crate) fn format_route_constructor_hover(module: &Module, name: &str) -> Option<String> {
    for decl in &module.decls {
        if let DeclKind::Route { entries, .. } = &decl.node {
            for entry in entries {
                if entry.constructor == name {
                    return Some(render_route_entry(entry));
                }
            }
        }
    }
    None
}

pub(crate) fn render_route_entry(entry: &ast::RouteEntry) -> String {
    let method = http_method_str(entry.method);
    let path = format_route_path(entry);
    let mut out = format!("**Route:** `{method} {path}`");

    if !entry.body_fields.is_empty() {
        let fields: Vec<String> = entry
            .body_fields
            .iter()
            .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
            .collect();
        out.push_str(&format!("\n\n**Body:** `{{{}}}`", fields.join(", ")));
    }
    if !entry.query_params.is_empty() {
        let fields: Vec<String> = entry
            .query_params
            .iter()
            .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
            .collect();
        out.push_str(&format!("\n\n**Query:** `{{{}}}`", fields.join(", ")));
    }
    if !entry.request_headers.is_empty() {
        let fields: Vec<String> = entry
            .request_headers
            .iter()
            .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
            .collect();
        out.push_str(&format!("\n\n**Request headers:** `{{{}}}`", fields.join(", ")));
    }
    if let Some(resp) = &entry.response_ty {
        out.push_str(&format!(
            "\n\n**Response:** `{}`",
            format_type_kind(&resp.node)
        ));
    }
    if !entry.response_headers.is_empty() {
        let fields: Vec<String> = entry
            .response_headers
            .iter()
            .map(|f| format!("{}: {}", f.name, format_type_kind(&f.value.node)))
            .collect();
        out.push_str(&format!(
            "\n\n**Response headers:** `{{{}}}`",
            fields.join(", ")
        ));
    }
    out
}

// ── Record-type parsing (used by hover and completion) ──────────────

/// Parse field names from a record type string like `{name: Text, age: Int}`.
/// Now backed by `ParsedType` so nested records, function-typed fields, and
/// row variables don't trip the field splitter.
pub(crate) fn extract_record_fields(type_str: &str) -> Vec<String> {
    let parsed = crate::parsed_type::ParsedType::parse(type_str);
    parsed
        .record_fields()
        .map(|fs| fs.iter().map(|(n, _)| n.clone()).collect())
        .unwrap_or_default()
}

// ── Function parameter introspection ────────────────────────────────

/// Extract parameter names from a function declaration's body.
/// Returns an empty Vec if the function isn't directly a lambda chain or
/// trait/impl method. Used by signature_help and parameter-name inlay hints.
///
/// For trait methods, prefers the trait declaration's `default_params` (the
/// names the trait author chose, which match the user's mental model of the
/// API). When the trait method has no default body, falls back to scanning
/// impl methods — those carry meaningful names that are still better than
/// the synthesized `a`/`b`/`c` fallback.
pub(crate) fn extract_param_names(module: &Module, func_name: &str) -> Vec<String> {
    let mut from_impl: Option<Vec<String>> = None;
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun {
                name,
                body: Some(body),
                ..
            } if name == func_name => {
                return collect_lambda_param_names(body);
            }
            DeclKind::Trait { items, .. } => {
                for item in items {
                    if let ast::TraitItem::Method {
                        name,
                        default_params,
                        ..
                    } = item
                    {
                        if name == func_name && !default_params.is_empty() {
                            return default_params
                                .iter()
                                .map(|p| pat_to_simple_name(&p.node))
                                .collect();
                        }
                    }
                }
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { name, params, .. } = item {
                        if name == func_name && from_impl.is_none() {
                            from_impl = Some(
                                params
                                    .iter()
                                    .map(|p| pat_to_simple_name(&p.node))
                                    .collect(),
                            );
                        }
                    }
                }
            }
            _ => {}
        }
    }
    from_impl.unwrap_or_default()
}

/// Walk a chain of nested lambdas (`\a -> \b -> body`) and collect param names.
pub(crate) fn collect_lambda_param_names(expr: &ast::Expr) -> Vec<String> {
    let mut names = Vec::new();
    let mut cur = expr;
    loop {
        match &cur.node {
            ast::ExprKind::Lambda { params, body } => {
                for p in params {
                    names.push(pat_to_simple_name(&p.node));
                }
                cur = body;
            }
            _ => break,
        }
    }
    names
}

/// Render a pattern as a simple name for parameter display.
/// `x` → "x", `{name, age}` → "{name, age}", `_` → "_".
pub(crate) fn pat_to_simple_name(pat: &ast::PatKind) -> String {
    match pat {
        ast::PatKind::Var(name) => name.clone(),
        ast::PatKind::Wildcard => "_".into(),
        ast::PatKind::Record(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| match &f.pattern {
                    None => f.name.clone(),
                    Some(p) => format!("{}: {}", f.name, pat_to_simple_name(&p.node)),
                })
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        ast::PatKind::Constructor { name, payload } => match &payload.node {
            ast::PatKind::Record(fields) if fields.is_empty() => name.clone(),
            other => format!("{name} {}", pat_to_simple_name(other)),
        },
        ast::PatKind::List(pats) => {
            let parts: Vec<String> = pats.iter().map(|p| pat_to_simple_name(&p.node)).collect();
            format!("[{}]", parts.join(", "))
        }
        ast::PatKind::Lit(_) => "_".into(),
    }
}

/// Flatten an `App(App(App(f, x), y), z)` chain into `(callee_expr, [x, y, z])`.
/// Returns the args in source order. The callee is whatever sits at the
/// bottom of the chain — typically a `Var` for named function calls.
pub(crate) fn flatten_app_chain<'a>(expr: &'a ast::Expr) -> (&'a ast::Expr, Vec<&'a ast::Expr>) {
    let mut args: Vec<&ast::Expr> = Vec::new();
    let mut current = expr;
    while let ast::ExprKind::App { func, arg } = &current.node {
        args.push(arg.as_ref());
        current = func.as_ref();
    }
    args.reverse();
    (current, args)
}

// ── Field-access introspection ──────────────────────────────────────

/// What the cursor pointed to when the user hovered on a field-access's
/// trailing field-name token. We extract the receiver as a small enum rather
/// than a borrowed reference so the result can outlive the AST walk closure
/// (the recursive `recurse_expr` callback can't carry borrowed AST references
/// in its mutable accumulator).
#[derive(Debug, Clone)]
pub(crate) struct FieldAccessAt {
    pub field_name: String,
    pub receiver: ReceiverKind,
}

#[derive(Debug, Clone)]
pub(crate) enum ReceiverKind {
    /// `someVar.field` — most common case.
    Var(String),
    /// `*src.field` or `&derived.field`.
    SourceRef(String),
    DerivedRef(String),
    /// Anything else; refinement lookup is not supported for these.
    Other,
}

/// If a `FieldAccess { expr, field }` node has its field-name token under the
/// cursor, return the field name and a coarse classification of the receiver.
/// The field-name span is the trailing `field.len()` bytes of the FieldAccess's
/// overall span (mirrors `semantic_tokens::visit_expr`).
pub(crate) fn find_field_access_at_offset(
    module: &Module,
    offset: usize,
) -> Option<FieldAccessAt> {
    fn classify_receiver(expr: &ast::Expr) -> ReceiverKind {
        match &expr.node {
            ast::ExprKind::Var(n) => ReceiverKind::Var(n.clone()),
            ast::ExprKind::SourceRef(n) => ReceiverKind::SourceRef(n.clone()),
            ast::ExprKind::DerivedRef(n) => ReceiverKind::DerivedRef(n.clone()),
            _ => ReceiverKind::Other,
        }
    }
    fn walk(expr: &ast::Expr, offset: usize, best: &mut Option<FieldAccessAt>) {
        if let ast::ExprKind::FieldAccess { expr: receiver, field } = &expr.node {
            let field_start = expr.span.end.saturating_sub(field.len());
            if field_start <= offset && offset < expr.span.end {
                *best = Some(FieldAccessAt {
                    field_name: field.clone(),
                    receiver: classify_receiver(receiver),
                });
            }
        }
        recurse_expr(expr, |e| walk(e, offset, best));
    }
    let mut best = None;
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => walk(body, offset, &mut best),
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk(body, offset, &mut best);
                    }
                }
            }
            _ => {}
        }
    }
    best
}

/// Walk the module looking for a `Bind`/`Let` statement that introduces
/// `var_name` from a `*source` (or `&derived`) reference, and return the
/// source name. Handles direct binds: `p <- *people`, `let p = *people`, and
/// constructor-pattern binds like `Just p <- *people`. Also peels App chains
/// and pipe operators so `p <- filter f *people` and `p <- *src |> filter f`
/// resolve to `people` / `src`.
///
/// First match wins. In typical knot code, top-level decls bind a given name
/// at most once, so this is correct in practice; shadowed bindings won't be
/// distinguished.
pub(crate) fn resolve_var_to_source(module: &Module, var_name: &str) -> Option<String> {
    fn pat_binds_var(pat: &ast::Pat, name: &str) -> bool {
        match &pat.node {
            ast::PatKind::Var(n) => n == name,
            ast::PatKind::Constructor { payload, .. } => pat_binds_var(payload, name),
            ast::PatKind::Record(fields) => fields.iter().any(|f| {
                if f.name == name && f.pattern.is_none() {
                    true
                } else {
                    f.pattern.as_ref().is_some_and(|p| pat_binds_var(p, name))
                }
            }),
            ast::PatKind::List(pats) => pats.iter().any(|p| pat_binds_var(p, name)),
            _ => false,
        }
    }

    fn rhs_source_name(rhs: &ast::Expr) -> Option<String> {
        match &rhs.node {
            ast::ExprKind::SourceRef(n) | ast::ExprKind::DerivedRef(n) => Some(n.clone()),
            // `filter f *src`, `take n *src`, etc. — peel App chains to find
            // an underlying source ref.
            ast::ExprKind::App { func, arg } => {
                rhs_source_name(arg).or_else(|| rhs_source_name(func))
            }
            // Pipe: `*src |> filter f` puts the source on the LHS of pipe,
            // which desugars to `filter f *src`. Walk both sides.
            ast::ExprKind::BinOp { op: ast::BinOp::Pipe, lhs, rhs } => {
                rhs_source_name(lhs).or_else(|| rhs_source_name(rhs))
            }
            _ => None,
        }
    }

    fn walk(expr: &ast::Expr, var_name: &str, found: &mut Option<String>) {
        if found.is_some() {
            return;
        }
        if let ast::ExprKind::Do(stmts) = &expr.node {
            for stmt in stmts {
                if let ast::StmtKind::Bind { pat, expr: rhs }
                | ast::StmtKind::Let { pat, expr: rhs } = &stmt.node
                {
                    if pat_binds_var(pat, var_name) {
                        if let Some(name) = rhs_source_name(rhs) {
                            *found = Some(name);
                            return;
                        }
                    }
                    walk(rhs, var_name, found);
                    if found.is_some() {
                        return;
                    }
                }
                if let ast::StmtKind::Where { cond } | ast::StmtKind::Expr(cond) = &stmt.node {
                    walk(cond, var_name, found);
                }
                if let ast::StmtKind::GroupBy { key } = &stmt.node {
                    walk(key, var_name, found);
                }
            }
            return;
        }
        recurse_expr(expr, |e| walk(e, var_name, found));
    }

    let mut found = None;
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { body: Some(body), .. }
            | DeclKind::View { body, .. }
            | DeclKind::Derived { body, .. } => {
                walk(body, var_name, &mut found);
            }
            DeclKind::Impl { items, .. } => {
                for item in items {
                    if let ast::ImplItem::Method { body, .. } = item {
                        walk(body, var_name, &mut found);
                    }
                }
            }
            _ => {}
        }
        if found.is_some() {
            break;
        }
    }
    found
}

/// Look up a per-field refinement by source name + field name. Returns the
/// refined-type label and the predicate expression for hover/completion to
/// render.
pub(crate) fn find_field_refinement<'a>(
    source_refinements: &'a std::collections::HashMap<
        String,
        Vec<(Option<String>, String, ast::Expr)>,
    >,
    source_name: &str,
    field_name: &str,
) -> Option<(&'a str, &'a ast::Expr)> {
    let entries = source_refinements.get(source_name)?;
    for (field, type_label, predicate) in entries {
        if field.as_deref() == Some(field_name) {
            return Some((type_label.as_str(), predicate));
        }
    }
    None
}

// ── Type-scheme cursor lookup ───────────────────────────────────────

/// Walk a `Type` AST node and return true if any of its sub-spans contain
/// `offset`. Used to test whether a cursor is *inside* a type expression.
fn type_contains_offset(ty: &ast::Type, offset: usize) -> bool {
    if ty.span.start <= offset && offset < ty.span.end {
        return true;
    }
    match &ty.node {
        ast::TypeKind::App { func, arg } => {
            type_contains_offset(func, offset) || type_contains_offset(arg, offset)
        }
        ast::TypeKind::Record { fields, .. } => {
            fields.iter().any(|f| type_contains_offset(&f.value, offset))
        }
        ast::TypeKind::Relation(inner) => type_contains_offset(inner, offset),
        ast::TypeKind::Function { param, result } => {
            type_contains_offset(param, offset) || type_contains_offset(result, offset)
        }
        ast::TypeKind::Variant { constructors, .. } => constructors
            .iter()
            .any(|c| c.fields.iter().any(|f| type_contains_offset(&f.value, offset))),
        ast::TypeKind::Effectful { ty, .. } => type_contains_offset(ty, offset),
        ast::TypeKind::IO { ty, .. } => type_contains_offset(ty, offset),
        ast::TypeKind::UnitAnnotated { base, .. } => type_contains_offset(base, offset),
        ast::TypeKind::Refined { base, .. } => type_contains_offset(base, offset),
        ast::TypeKind::Forall { ty, .. } => type_contains_offset(ty, offset),
        _ => false,
    }
}

fn scheme_contains_offset(scheme: &ast::TypeScheme, offset: usize) -> bool {
    if type_contains_offset(&scheme.ty, offset) {
        return true;
    }
    scheme
        .constraints
        .iter()
        .any(|c| c.args.iter().any(|t| type_contains_offset(t, offset)))
}

/// If the cursor is inside a function/view/derived/trait-method's type
/// signature, return the `TypeScheme` plus the decl name. Used by hover to
/// surface trait constraints that mention a generic parameter under the
/// cursor.
pub(crate) fn find_enclosing_type_scheme<'a>(
    module: &'a Module,
    offset: usize,
) -> Option<(&'a ast::TypeScheme, &'a str)> {
    for decl in &module.decls {
        match &decl.node {
            DeclKind::Fun { name, ty: Some(scheme), .. }
            | DeclKind::View { name, ty: Some(scheme), .. }
            | DeclKind::Derived { name, ty: Some(scheme), .. } => {
                if scheme_contains_offset(scheme, offset) {
                    return Some((scheme, name.as_str()));
                }
            }
            DeclKind::Trait { items, .. } => {
                for item in items {
                    if let ast::TraitItem::Method { name, ty, .. } = item {
                        if scheme_contains_offset(ty, offset) {
                            return Some((ty, name.as_str()));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    None
}

/// Constraints from `scheme` that mention the given type variable. Used by
/// hover to render `Display a, Show a` when the cursor is on `a` in a
/// `Display a => Show a => [a] -> Text` signature.
pub(crate) fn constraints_for_type_var<'a>(
    scheme: &'a ast::TypeScheme,
    var_name: &str,
) -> Vec<&'a ast::Constraint> {
    scheme
        .constraints
        .iter()
        .filter(|c| {
            c.args.iter().any(|t| type_mentions_var(t, var_name))
        })
        .collect()
}

fn type_mentions_var(ty: &ast::Type, var: &str) -> bool {
    match &ty.node {
        ast::TypeKind::Var(n) => n == var,
        ast::TypeKind::Named(_) | ast::TypeKind::Hole => false,
        ast::TypeKind::App { func, arg } => {
            type_mentions_var(func, var) || type_mentions_var(arg, var)
        }
        ast::TypeKind::Record { fields, .. } => {
            fields.iter().any(|f| type_mentions_var(&f.value, var))
        }
        ast::TypeKind::Relation(inner) => type_mentions_var(inner, var),
        ast::TypeKind::Function { param, result } => {
            type_mentions_var(param, var) || type_mentions_var(result, var)
        }
        ast::TypeKind::Variant { constructors, .. } => constructors
            .iter()
            .any(|c| c.fields.iter().any(|f| type_mentions_var(&f.value, var))),
        ast::TypeKind::Effectful { ty, .. }
        | ast::TypeKind::IO { ty, .. }
        | ast::TypeKind::UnitAnnotated { base: ty, .. }
        | ast::TypeKind::Refined { base: ty, .. } => type_mentions_var(ty, var),
        ast::TypeKind::Forall { vars, ty } => {
            !vars.iter().any(|n| n == var) && type_mentions_var(ty, var)
        }
    }
}

