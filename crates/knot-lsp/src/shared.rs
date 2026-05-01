//! Helpers shared across multiple LSP feature modules. Anything that's used by
//! exactly one feature lives in that feature's module; this file is for the
//! cross-feature utilities (route formatting, atomic-context detection, etc.).

use std::path::{Path, PathBuf};

use knot::ast::{self, DeclKind, Module, Span};

use crate::state::DocumentState;
use crate::type_format::format_type_kind;
use crate::utils::{recurse_expr, safe_slice};

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
        ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
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

/// Parse a Knot type string like "Int -> Text -> Bool" into parameter types.
pub(crate) fn parse_function_params(type_str: &str) -> Vec<String> {
    let mut params = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();

    let chars: Vec<char> = type_str.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '(' | '[' | '{' | '<' => {
                depth += 1;
                current.push(chars[i]);
            }
            ')' | ']' | '}' | '>' => {
                depth -= 1;
                current.push(chars[i]);
            }
            '-' if depth == 0 && i + 1 < chars.len() && chars[i + 1] == '>' => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    params.push(trimmed);
                }
                current.clear();
                i += 2; // skip "->"
                continue;
            }
            _ => {
                current.push(chars[i]);
            }
        }
        i += 1;
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        params.push(trimmed);
    }

    params
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
            ast::ExprKind::Set { target, value } | ast::ExprKind::FullSet { target, value } => {
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
pub(crate) fn extract_record_fields(type_str: &str) -> Vec<String> {
    let inner = &type_str[1..type_str.len() - 1]; // strip { }
    let mut fields = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();

    for ch in inner.chars() {
        match ch {
            '{' | '[' | '(' | '<' => {
                depth += 1;
                current.push(ch);
            }
            '}' | ']' | ')' | '>' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                if let Some(name) = extract_field_name(&current) {
                    fields.push(name);
                }
                current.clear();
            }
            '|' if depth == 0 => {
                // Row variable — stop
                if let Some(name) = extract_field_name(&current) {
                    fields.push(name);
                }
                break;
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        if let Some(name) = extract_field_name(&current) {
            fields.push(name);
        }
    }
    fields
}

pub(crate) fn extract_field_name(field_str: &str) -> Option<String> {
    let trimmed = field_str.trim();
    let colon = trimmed.find(':')?;
    Some(trimmed[..colon].trim().to_string())
}

// Re-export DocumentState use convenience for callers
#[allow(dead_code)]
fn _unused(_: &DocumentState) {}
