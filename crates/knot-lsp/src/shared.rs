//! Helpers shared across multiple LSP feature modules. Anything that's used by
//! exactly one feature lives in that feature's module; this file is for the
//! cross-feature utilities (route formatting, atomic-context detection, etc.).

use std::path::{Path, PathBuf};

use knot::ast::{self, Expr, ExprKind, Span};
use knot_compiler::effects::EffectSet;

use crate::type_format::format_type_kind;
use crate::utils::{find_word_in_source, recurse_expr, safe_slice, top_fields};

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

/// Find the byte range of the contents (between `{` and `}`) of the
/// *outermost* (top-level result position) `IO { … }` row in the rendered
/// type. Only `IO {` occurrences at nesting depth 0 count — taking the
/// textually-last row regardless of nesting picks the wrong one for curried
/// IO-returning results (`Int -> IO {} (Int -> IO {fs} Text)`, where the
/// outermost row is the FIRST). Among depth-0 rows, the last along the arrow
/// spine (`IO {a} Int -> IO {b} Text`) is the result row. Returns `None`
/// when no top-level IO row is present.
pub(crate) fn find_outermost_io_row(ty: &str) -> Option<(usize, usize)> {
    let bytes = ty.as_bytes();
    let mut last: Option<(usize, usize)> = None;
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b'I' if depth == 0 && ty[i..].starts_with("IO {") => {
                // Whole-word check: don't match the tail of an identifier.
                let left_ok = i == 0
                    || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
                if left_ok {
                    let row_start = i + 4;
                    let mut row_depth: i32 = 1;
                    let mut j = row_start;
                    while j < bytes.len() {
                        match bytes[j] {
                            b'{' => row_depth += 1,
                            b'}' => {
                                row_depth -= 1;
                                if row_depth == 0 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                        j += 1;
                    }
                    if row_depth != 0 {
                        return None;
                    }
                    last = Some((row_start, j));
                    i = j + 1;
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
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
    if let Some(rest) = s.strip_prefix("IO ") {
        if rest.starts_with('{')
            && let Some(close) = rest.find('}') {
                return extract_principal_type_name(rest[close + 1..].trim());
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

/// Maximum directory depth walked when scanning for `.knot` files. Real
/// workspaces don't nest this deeply; the cap exists to terminate symlink
/// loops that slip past the symlink filter (e.g. via bind mounts) and
/// pathological generated trees.
const MAX_SCAN_DEPTH: usize = 32;
/// Hard ceiling on `.knot` files collected per scan. Workspace-wide handlers
/// run on every keystroke (workspace/symbol, auto-import); a runaway scan
/// — say, the user pointed the LSP at `$HOME` — would otherwise eat memory
/// and stall. Well above any realistic project size.
const MAX_SCAN_FILES: usize = 50_000;

/// Recursively find all .knot files under a directory.
pub(crate) fn scan_knot_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    scan_knot_files_recursive(dir, &mut files, 0)?;
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

pub(crate) fn scan_knot_files_recursive(
    dir: &Path,
    files: &mut Vec<PathBuf>,
    depth: usize,
) -> std::io::Result<()> {
    if depth >= MAX_SCAN_DEPTH || files.len() >= MAX_SCAN_FILES {
        return Ok(());
    }
    // Per-directory and per-entry IO errors are skipped, not propagated: one
    // unreadable subdirectory (permissions, racing deletion) must not abort
    // the whole workspace scan — callers like the workspace-diagnostics
    // handler would otherwise drop a partially-scanned root and mass-clear
    // previously-reported diagnostics.
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        // Use the dirent's metadata, not Path::is_dir, so we don't follow
        // symlinks — a self-referential symlink chain (`a -> b`, `b -> a`)
        // would otherwise recurse until the depth cap, doing pointless IO.
        let file_type = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !name.starts_with('.') && name != "target" && name != "node_modules" {
                scan_knot_files_recursive(&path, files, depth + 1)?;
                if files.len() >= MAX_SCAN_FILES {
                    return Ok(());
                }
            }
        } else if file_type.is_file()
            && path.extension().and_then(|e| e.to_str()) == Some("knot")
        {
            files.push(path);
            if files.len() >= MAX_SCAN_FILES {
                return Ok(());
            }
        }
    }
    Ok(())
}

// ── AST walker depth cap ────────────────────────────────────────────

/// Recursion ceiling for the AST walkers in this module. Pathological
/// left-deep expressions (a 200k-term `1+1+…` chain parses into a BinOp
/// spine 200k nodes deep) would otherwise overflow the stack — and these
/// walkers run on every keystroke (completion's atomic-context check) and on
/// unopened workspace files. 10k frames is far beyond any human-written
/// nesting while staying comfortably inside the default 8 MiB stack. Bailing
/// out simply stops descending — features degrade (e.g. no signature help
/// at nesting level 10_001) but the process survives.
///
/// Residual risk: recursive walkers in other feature modules (semantic
/// tokens, folding, document symbols, …) and in the compiler crates still
/// recurse unboundedly over the same ASTs; a complete fix needs either a
/// shared iterative walker or running analysis on a dedicated thread with a
/// large stack (`std::thread::Builder::stack_size`).
pub(crate) const MAX_WALK_DEPTH: usize = 10_000;

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

/// Returns true if the given route is wired into a server. Used by the
/// dead-route lint.
///
/// Two signals count as "listened":
/// 1. Any `serve <Api> where …` expression in the module whose api name is
///    this route (or a composite route that includes it). This is the
///    canonical wiring — `api = serve Api where …; main = listen 8080 api`
///    stores the route as `ExprKind::Serve`'s plain `api: Name`, which is
///    invisible to `expr_references_name`, so the lens used to fire on
///    every working route. We deliberately do NOT require that the serve
///    value is itself reachable from a `listen` call: a `serve` that's
///    never listened is far rarer than the canonical pattern, and the
///    lens's job is catching truly dead routes — a sound approximation
///    that under-warns beats one that warns on every wired route.
/// 2. A `listen port handler` call whose argument textually references the
///    route name (the pre-`serve` wiring style).
pub(crate) fn route_is_listened(program: &Expr, route_name: &str) -> bool {
    route_is_listened_inner(program, route_name, &mut std::collections::HashSet::new())
}

fn route_is_listened_inner(
    program: &Expr,
    route_name: &str,
    visiting: &mut std::collections::HashSet<String>,
) -> bool {
    // Cycle guard for composite-route recursion (`route A = B`, `route B = A`
    // is malformed but must not hang the lens).
    if !visiting.insert(route_name.to_string()) {
        return false;
    }
    fn walk(expr: &ast::Expr, route_name: &str, found: &mut bool, depth: usize) {
        if *found || depth > MAX_WALK_DEPTH {
            return;
        }
        // `serve Api where …` — the api is a plain Name on the Serve node.
        if let ast::ExprKind::Serve { api, .. } = &expr.node
            && api == route_name {
                *found = true;
                return;
            }
        if let ast::ExprKind::App { func, arg } = &expr.node {
            // Detect `listen port handler` where one argument references the route.
            // The handler's body typically destructures the route ADT, so any reference
            // to the route name (constructor case-match) inside a `listen` call is
            // a strong signal that the route is wired in.
            if (app_callee_is(func, "listen") || app_callee_is(func, "listenOn"))
                && expr_references_name(arg, route_name)
            {
                *found = true;
                return;
            }
        }
        recurse_expr(expr, |e| walk(e, route_name, found, depth + 1));
    }
    for decl in top_fields(program) {
        match &decl.value.node {
            ExprKind::ViewDecl { body, .. } | ExprKind::DerivedDecl { body, .. } => {
                let mut found = false;
                walk(body, route_name, &mut found, 0);
                if found {
                    return true;
                }
            }
            // A composite `route Api = A | B` that is itself listened/served
            // wires in every component route.
            ExprKind::RouteCompositeDecl { name, components }
                if components.iter().any(|c| c == route_name)
                    && route_is_listened_inner(program, name, visiting)
                => {
                    return true;
                }
            _ => {
                // A named function field: walk its body.
                let mut found = false;
                walk(&decl.value, route_name, &mut found, 0);
                if found {
                    return true;
                }
            }
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
    fn walk(expr: &ast::Expr, name: &str, found: &mut bool, depth: usize) {
        if *found || depth > MAX_WALK_DEPTH {
            return;
        }
        match &expr.node {
            ast::ExprKind::Var(n) | ast::ExprKind::Constructor(n) if n == name => {
                *found = true;
            }
            _ => recurse_expr(expr, |e| walk(e, name, found, depth + 1)),
        }
    }
    walk(expr, name, &mut found, 0);
    found
}

// ── Application-chain lookup (signature help / hover) ───────────────

/// Walk the AST to find the innermost function application chain containing the cursor.
/// Returns (function_name, active_parameter_index).
pub(crate) fn find_enclosing_application(
    program: &Expr,
    source: &str,
    offset: usize,
) -> Option<(String, usize)> {
    let mut best: Option<(String, usize, usize)> = None; // (name, param_idx, span_size)

    for decl in top_fields(program) {
        if decl.value.span.start > offset || offset >= decl.value.span.end {
            continue;
        }
        match &decl.value.node {
            ExprKind::ViewDecl { body, .. } | ExprKind::DerivedDecl { body, .. } => {
                find_app_in_expr(body, source, offset, &mut best);
            }
            // Route `rateLimit` expressions can contain applications that
            // need signature help.
            ExprKind::RouteDecl { entries, .. } => {
                for entry in entries {
                    if let Some(rl) = &entry.rate_limit {
                        find_app_in_expr(rl, source, offset, &mut best);
                    }
                }
            }
            _ => {
                // A named function field.
                find_app_in_expr(&decl.value, source, offset, &mut best);
            }
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
    find_app_in_expr_at(expr, source, offset, best, 0)
}

// `source` is threaded through recursion to mirror `find_app_in_expr`'s
// signature (it feeds recursive calls); not otherwise read in this body.
#[allow(clippy::only_used_in_recursion)]
fn find_app_in_expr_at(
    expr: &ast::Expr,
    source: &str,
    offset: usize,
    best: &mut Option<(String, usize, usize)>,
    depth: usize,
) {
    if depth > MAX_WALK_DEPTH {
        return;
    }
    if expr.span.start > offset || offset >= expr.span.end {
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
                if offset >= arg.span.start && offset < arg.span.end {
                    param_idx = i;
                    break;
                }
            }

            let span_size = expr.span.end - expr.span.start;
            // Prefer the smallest (innermost) enclosing application
            if best.as_ref().is_none_or(|b| span_size <= b.2) {
                *best = Some((name, param_idx, span_size));
            }
        }
    }

    // Recurse into sub-expressions. `recurse_expr` covers every non-leaf
    // ExprKind — including `Serve` handler bodies, which the old manual
    // match here omitted (signature help was dead inside serve handlers).
    recurse_expr(expr, |e| find_app_in_expr_at(e, source, offset, best, depth + 1));
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
    program: &Expr,
    source: &str,
    offset: usize,
) -> Option<(Span, String)> {
    fn walk(
        expr: &ast::Expr,
        source: &str,
        offset: usize,
        best: &mut Option<(Span, String)>,
        depth: usize,
    ) {
        if depth > MAX_WALK_DEPTH {
            return;
        }
        if expr.span.start > offset || offset >= expr.span.end {
            return;
        }
        if let ast::ExprKind::Atomic(inner) = &expr.node {
            let inner_text = safe_slice(source, inner.span).to_string();
            // Track the smallest enclosing atomic
            let size = expr.span.end - expr.span.start;
            if best
                .as_ref()
                .is_none_or(|b: &(Span, String)| size < b.0.end - b.0.start)
            {
                *best = Some((expr.span, inner_text));
            }
        }
        // Descend into ALL children via `recurse_expr` — including Lambda
        // bodies and Serve handler bodies. The walk used to skip lambdas,
        // which made atomic detection blind inside every parameterized
        // function (the decl body of `f = \x -> atomic do …` is a Lambda, so
        // the walk stopped before reaching the Atomic). Any Atomic that
        // lexically encloses `offset` records itself BEFORE this descent, so
        // descending into lambdas only ADDS detection of atomics nested
        // inside them — it can't misattribute an enclosing one.
        recurse_expr(expr, |e| walk(e, source, offset, best, depth + 1));
    }

    let mut best: Option<(Span, String)> = None;
    for decl in top_fields(program) {
        if decl.value.span.start > offset || offset >= decl.value.span.end {
            continue;
        }
        match &decl.value.node {
            ExprKind::ViewDecl { body, .. } | ExprKind::DerivedDecl { body, .. } => {
                walk(body, source, offset, &mut best, 0)
            }
            // Route `rateLimit` expressions are user-edited code.
            ExprKind::RouteDecl { entries, .. } => {
                for entry in entries {
                    if let Some(rl) = &entry.rate_limit {
                        walk(rl, source, offset, &mut best, 0);
                    }
                }
            }
            _ => {
                // A named function field.
                walk(&decl.value, source, offset, &mut best, 0);
            }
        }
    }
    best
}

// ── Hover-only helpers used by completion-resolve too ───────────────

/// Render a predicate expression as its source text.
///
/// Caution: refined types can be *imported* — inference runs on the
/// post-import-inlining module, so the predicate's spans may index into the
/// imported file's bytes, not the current document's. Blindly slicing
/// `source` then displays arbitrary text from the wrong file. We only trust
/// the slice when it plausibly is the predicate (refinement predicates are
/// always lambdas, so the text must start with `\`); otherwise we fall back
/// to pretty-printing the AST, which is always correct.
pub(crate) fn predicate_to_source(expr: &ast::Expr, source: &str) -> String {
    let span = expr.span;
    if span.start < span.end && span.end <= source.len()
        && let Some(text) = source.get(span.start..span.end)
            && slice_matches_predicate(text, expr) {
                return text.to_string();
            }
    render_predicate_expr(expr)
}

/// Sanity-check that a source slice plausibly *is* the given predicate
/// expression: it must start with the lambda's `\` and the first parameter
/// token must follow. A span pointing into the wrong file's bytes virtually
/// never passes both checks.
fn slice_matches_predicate(text: &str, expr: &ast::Expr) -> bool {
    let ast::ExprKind::Lambda { params, .. } = &expr.node else {
        return false;
    };
    let trimmed = text.trim_start();
    let Some(rest) = trimmed.strip_prefix('\\') else {
        return false;
    };
    match params.first().map(|p| &p.node) {
        Some(ast::PatKind::Var(n)) => rest.trim_start().starts_with(n.as_str()),
        Some(ast::PatKind::Wildcard) => rest.trim_start().starts_with('_'),
        Some(ast::PatKind::Record(_)) => rest.trim_start().starts_with('{'),
        _ => true,
    }
}

/// Minimal AST pretty-printer for refinement predicates. Covers the shapes
/// predicates actually take (lambdas over comparisons, field accesses,
/// arithmetic, boolean connectives); anything more exotic degrades to a
/// `<predicate>` placeholder rather than arbitrary text.
fn render_predicate_expr(expr: &ast::Expr) -> String {
    fn pat(p: &ast::Pat) -> String {
        match &p.node {
            ast::PatKind::Var(n) => n.clone(),
            ast::PatKind::Wildcard => "_".to_string(),
            ast::PatKind::Record(fields) => {
                let inner: Vec<String> = fields
                    .iter()
                    .map(|f| match &f.pattern {
                        Some(inner) => format!("{}: {}", f.name, pat(inner)),
                        None => f.name.clone(),
                    })
                    .collect();
                format!("{{{}}}", inner.join(", "))
            }
            ast::PatKind::Constructor { name, payload, .. } => {
                format!("{name} {}", pat(payload))
            }
            ast::PatKind::List(pats) => {
                format!("[{}]", pats.iter().map(|p| pat(p)).collect::<Vec<_>>().join(", "))
            }
            ast::PatKind::Cons { head, tail } => {
                format!("Cons {} {}", pat(head), pat(tail))
            }
            _ => "_".to_string(),
        }
    }
    fn bin_op(op: ast::BinOp) -> &'static str {
        match op {
            ast::BinOp::Add => "+",
            ast::BinOp::Sub => "-",
            ast::BinOp::Mul => "*",
            ast::BinOp::Div => "/",
            ast::BinOp::Mod => "%",
            ast::BinOp::Eq => "==",
            ast::BinOp::Neq => "!=",
            ast::BinOp::Lt => "<",
            ast::BinOp::Gt => ">",
            ast::BinOp::Le => "<=",
            ast::BinOp::Ge => ">=",
            ast::BinOp::And => "&&",
            ast::BinOp::Or => "||",
            ast::BinOp::Concat => "++",
            ast::BinOp::Pipe => "|>",
        }
    }
    /// Render `expr`; `nested` adds parens around compound results so the
    /// output stays readable without a precedence table.
    fn go(expr: &ast::Expr, nested: bool) -> Option<String> {
        let s = match &expr.node {
            ast::ExprKind::Lit(lit) => match lit {
                ast::Literal::Int(s) => s.clone(),
                ast::Literal::Float(f) => format!("{f}"),
                ast::Literal::Text(t) => format!("{t:?}"),
                ast::Literal::Bool(b) => b.to_string(),
                ast::Literal::Bytes(_) => return None,
            },
            ast::ExprKind::Var(n) | ast::ExprKind::Constructor(n) => n.clone(),
            ast::ExprKind::SourceRef(n) => format!("*{n}"),
            ast::ExprKind::DerivedRef(n) => format!("&{n}"),
            ast::ExprKind::FieldAccess { expr: recv, field } => {
                format!("{}.{field}", go(recv, true)?)
            }
            ast::ExprKind::Lambda { params, body, .. } => {
                let ps: Vec<String> = params.iter().map(pat).collect();
                let rendered = format!("\\{} -> {}", ps.join(" "), go(body, false)?);
                return Some(if nested {
                    format!("({rendered})")
                } else {
                    rendered
                });
            }
            ast::ExprKind::App { func, arg } => {
                let rendered = format!("{} {}", go(func, true)?, go(arg, true)?);
                return Some(if nested {
                    format!("({rendered})")
                } else {
                    rendered
                });
            }
            ast::ExprKind::BinOp { op, lhs, rhs } => {
                let rendered =
                    format!("{} {} {}", go(lhs, true)?, bin_op(*op), go(rhs, true)?);
                return Some(if nested {
                    format!("({rendered})")
                } else {
                    rendered
                });
            }
            ast::ExprKind::UnaryOp { op, operand } => match op {
                ast::UnaryOp::Neg => format!("-{}", go(operand, true)?),
                ast::UnaryOp::Not => format!("not {}", go(operand, true)?),
            },
            ast::ExprKind::If {
                cond,
                then_branch,
                else_branch,
            } => {
                let rendered = format!(
                    "if {} then {} else {}",
                    go(cond, false)?,
                    go(then_branch, false)?,
                    go(else_branch, false)?
                );
                return Some(if nested {
                    format!("({rendered})")
                } else {
                    rendered
                });
            }
            _ => return None,
        };
        Some(s)
    }
    go(expr, false).unwrap_or_else(|| "<predicate>".to_string())
}

/// Find a route entry by its constructor name and render a hover summary
/// (method + path + body/query/headers/response). Returns `None` if no route
/// declares this constructor.
pub(crate) fn format_route_constructor_hover(program: &Expr, name: &str) -> Option<String> {
    for decl in top_fields(program) {
        if let ExprKind::RouteDecl { entries, .. } = &decl.value.node {
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
/// Returns an empty Vec if the function isn't directly a lambda chain.
/// Used by signature_help and parameter-name inlay hints.
pub(crate) fn extract_param_names(program: &Expr, func_name: &str) -> Vec<String> {
    for decl in top_fields(program) {
        if decl.name == func_name
            && matches!(decl.value.node, ExprKind::Lambda { .. })
        {
            return collect_lambda_param_names(&decl.value);
        }
    }
    Vec::new()
}

/// Walk a chain of nested lambdas (`\a -> \b -> body`) and collect param names.
pub(crate) fn collect_lambda_param_names(expr: &ast::Expr) -> Vec<String> {
    let mut names = Vec::new();
    let mut cur = expr;
    while let ast::ExprKind::Lambda { params, body, .. } = &cur.node {
        for p in params {
            names.push(pat_to_simple_name(&p.node));
        }
        cur = body;
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
        ast::PatKind::Constructor { name, payload, .. } => match &payload.node {
            ast::PatKind::Record(fields) if fields.is_empty() => name.clone(),
            other => format!("{name} {}", pat_to_simple_name(other)),
        },
        ast::PatKind::List(pats) => {
            let parts: Vec<String> = pats.iter().map(|p| pat_to_simple_name(&p.node)).collect();
            format!("[{}]", parts.join(", "))
        }
        ast::PatKind::Cons { head, tail } => {
            format!("Cons {} {}", pat_to_simple_name(&head.node), pat_to_simple_name(&tail.node))
        }
        ast::PatKind::Annot { pat, .. } => pat_to_simple_name(&pat.node),
        ast::PatKind::Lit(_) => "_".into(),
    }
}

/// Flatten an `App(App(App(f, x), y), z)` chain into `(callee_expr, [x, y, z])`.
/// Returns the args in source order. The callee is whatever sits at the
/// bottom of the chain — typically a `Var` for named function calls.
pub(crate) fn flatten_app_chain(expr: &ast::Expr) -> (&ast::Expr, Vec<&ast::Expr>) {
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
/// The field-name token is located by whole-word search in the window between
/// the receiver's end and the access node's end — NOT as a fixed `field.len()`
/// suffix of the access span, because a parenthesized access (`(r.total)`)
/// widens the node span to cover the trailing paren(s), which would shift the
/// suffix offset off the real token.
pub(crate) fn find_field_access_at_offset(
    program: &Expr,
    source: &str,
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
    fn walk(
        expr: &ast::Expr,
        source: &str,
        offset: usize,
        best: &mut Option<FieldAccessAt>,
        depth: usize,
    ) {
        if depth > MAX_WALK_DEPTH {
            return;
        }
        if let ast::ExprKind::FieldAccess { expr: receiver, field } = &expr.node
            && let Some(tok) =
                find_word_in_source(source, field, receiver.span.end, expr.span.end)
            && tok.start <= offset
            && offset < tok.end
        {
            *best = Some(FieldAccessAt {
                field_name: field.clone(),
                receiver: classify_receiver(receiver),
            });
        }
        recurse_expr(expr, |e| walk(e, source, offset, best, depth + 1));
    }
    let mut best = None;
    for decl in top_fields(program) {
        match &decl.value.node {
            ExprKind::ViewDecl { body, .. } | ExprKind::DerivedDecl { body, .. } => {
                walk(body, source, offset, &mut best, 0)
            }
            // The `rateLimit <expr>` clause on a route entry is user-edited
            // code (`{key: \input ctx -> …}`) that dereferences fields.
            ExprKind::RouteDecl { entries, .. } => {
                for entry in entries {
                    if let Some(rl) = &entry.rate_limit {
                        walk(rl, source, offset, &mut best, 0);
                    }
                }
            }
            _ => {
                // A named function field.
                walk(&decl.value, source, offset, &mut best, 0);
            }
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
/// Only the declaration that encloses `cursor_offset` is searched. The same
/// variable name (e.g. `p`) is routinely bound in unrelated do-blocks across
/// different decls; without this scoping the first module-wide binding would
/// win and callers would attribute the field to the wrong source relation
/// (bug B74). Within the enclosing decl, first match wins — shadowed bindings
/// inside a single decl are not distinguished.
pub(crate) fn resolve_var_to_source(
    program: &Expr,
    var_name: &str,
    cursor_offset: usize,
) -> Option<String> {
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
            ast::PatKind::Cons { head, tail } => {
                pat_binds_var(head, name) || pat_binds_var(tail, name)
            }
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

    fn walk(expr: &ast::Expr, var_name: &str, found: &mut Option<String>, depth: usize) {
        if found.is_some() || depth > MAX_WALK_DEPTH {
            return;
        }
        if let ast::ExprKind::Do(stmts) = &expr.node {
            for stmt in stmts {
                if let ast::StmtKind::Bind { pat, expr: rhs } = &stmt.node
                {
                    if pat_binds_var(pat, var_name)
                        && let Some(name) = rhs_source_name(rhs) {
                            *found = Some(name);
                            return;
                        }
                    walk(rhs, var_name, found, depth + 1);
                    if found.is_some() {
                        return;
                    }
                }
                if let ast::StmtKind::Where { cond } | ast::StmtKind::Expr(cond) = &stmt.node {
                    walk(cond, var_name, found, depth + 1);
                }
                if let ast::StmtKind::GroupBy { key } = &stmt.node {
                    walk(key, var_name, found, depth + 1);
                }
            }
            return;
        }
        recurse_expr(expr, |e| walk(e, var_name, found, depth + 1));
    }

    let mut found = None;
    for decl in top_fields(program) {
        // Scope resolution to the decl under the cursor. Top-level decls don't
        // nest, so at most one span contains the offset; any others may bind
        // the same name from a different source and must be ignored (bug B74).
        let dspan = decl.value.span;
        if !(dspan.start <= cursor_offset && cursor_offset < dspan.end) {
            continue;
        }
        match &decl.value.node {
            ExprKind::ViewDecl { body, .. } | ExprKind::DerivedDecl { body, .. } => {
                walk(body, var_name, &mut found, 0);
            }
            _ => {
                // A named function field.
                walk(&decl.value, var_name, &mut found, 0);
            }
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
/// Refinement predicates per source: source name → `(field?, type label,
/// predicate)` entries (field is `None` for whole-element refinements).
pub(crate) type SourceRefinements =
    std::collections::HashMap<String, Vec<(Option<String>, String, ast::Expr)>>;

pub(crate) fn find_field_refinement<'a>(
    source_refinements: &'a SourceRefinements,
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
        .any(|c| c.types().iter().any(|t| type_contains_offset(t, offset)))
}

/// If the cursor is inside a function/view/derived's type
/// signature, return the `TypeScheme` plus the decl name.
pub(crate) fn find_enclosing_type_scheme(
    program: &Expr,
    offset: usize,
) -> Option<(&ast::TypeScheme, &str)> {
    for decl in top_fields(program) {
        // The field's own signature.
        if let Some(scheme) = &decl.sig
            && scheme_contains_offset(scheme, offset)
        {
            return Some((scheme, decl.name.as_str()));
        }
        // View/Derived marker type annotations.
        let marker_ty = match &decl.value.node {
            ExprKind::ViewDecl { name: _, ty, .. } | ExprKind::DerivedDecl { name: _, ty, .. } => {
                ty.as_ref()
            }
            _ => None,
        };
        if let Some(scheme) = marker_ty
            && scheme_contains_offset(scheme, offset)
        {
            return Some((scheme, decl.name.as_str()));
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
        .filter(|c| match c {
            ast::Constraint::Trait { args, .. } => {
                args.iter().any(|t| type_mentions_var(t, var_name))
            }
            ast::Constraint::ImplicitField { ty, .. } => type_mentions_var(ty, var_name),
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
        ast::TypeKind::Unit(_) => false,
        ast::TypeKind::Forall { vars, ty } => {
            !vars.iter().any(|n| n == var) && type_mentions_var(ty, var)
        }
    }
}


// ── Tests ───────────────────────────────────────────────────────────



// Regression tests for the walker/scan fix batch (atomic-in-lambda, serve
// awareness, depth caps, resilient workspace scanning).


// Regression tests for the 2026-06 LSP bug-fix batch (shared helpers).

