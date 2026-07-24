//! Convert Knot compiler diagnostics into LSP diagnostics, including error
//! codes, related-information links, and unused/deprecated tags.

use lsp_types::{
    CodeDescription, Diagnostic, DiagnosticRelatedInformation, DiagnosticSeverity, DiagnosticTag,
    Location, NumberOrString, Position, Range, Uri,
};

use knot::diagnostic;

use crate::utils::span_to_range;

pub fn to_lsp_diagnostic(
    diag: &diagnostic::Diagnostic,
    source: &str,
    uri: &Uri,
) -> Option<Diagnostic> {
    let severity = match diag.severity {
        diagnostic::Severity::Error => DiagnosticSeverity::ERROR,
        diagnostic::Severity::Warning => DiagnosticSeverity::WARNING,
        diagnostic::Severity::Info => DiagnosticSeverity::INFORMATION,
    };

    // Index of the label that anchors the primary diagnostic range: the first
    // label whose span is in-bounds for `source`. Tracked (rather than just
    // mapped) so `related_information` below can exclude *this* label instead
    // of blindly skipping index 0 — otherwise, when the first label is
    // out-of-bounds and a later one supplies the range, that same later label
    // would also be emitted as related info (a duplicate anchored at the
    // primary range).
    let primary_label_idx = diag
        .labels
        .iter()
        .position(|l| l.span.start <= source.len() && l.span.end <= source.len());
    let range = primary_label_idx
        .map(|i| span_to_range(diag.labels[i].span, source))
        .unwrap_or(Range {
            start: Position::new(0, 0),
            end: Position::new(0, 0),
        });

    let mut message = diag.message.clone();
    for label in &diag.labels {
        if !label.message.is_empty() && label.message != diag.message {
            message.push_str(&format!("\n  {}", label.message));
        }
    }
    for note in &diag.notes {
        message.push_str(&format!("\nnote: {note}"));
    }

    let related: Vec<DiagnosticRelatedInformation> = diag
        .labels
        .iter()
        .enumerate()
        // Exclude exactly the label that became the primary range (not just
        // index 0): when the first label was out-of-bounds, the primary range
        // came from a later label, and that one must not also appear here.
        .filter(|(i, _)| Some(*i) != primary_label_idx)
        .map(|(_, l)| l)
        .filter(|l| l.span.start <= source.len() && l.span.end <= source.len())
        .map(|l| DiagnosticRelatedInformation {
            location: Location {
                uri: uri.clone(),
                range: span_to_range(l.span, source),
            },
            message: l.message.clone(),
        })
        .collect();

    let code = error_code_for_diagnostic(&diag.message);
    if let Some(desc) = code.as_deref().and_then(description_for_code) {
        message.push_str(&format!("\n[{}] {}", code.as_deref().unwrap_or(""), desc));
    }

    truncate_message(&mut message);
    let code_description = code.as_deref().and_then(doc_url_for_code).map(|href| {
        CodeDescription {
            href: href.parse().ok().unwrap_or_else(|| {
                "https://example.invalid/".parse().expect("static URI parses")
            }),
        }
    });

    let msg_lower = diag.message.to_lowercase();
    let mut tags = Vec::new();
    if msg_lower.contains("unused") || msg_lower.contains("never used") {
        tags.push(DiagnosticTag::UNNECESSARY);
    }
    if msg_lower.contains("deprecated") {
        tags.push(DiagnosticTag::DEPRECATED);
    }

    Some(Diagnostic {
        range,
        severity: Some(severity),
        code: code.map(NumberOrString::String),
        code_description,
        source: Some("knot".into()),
        message,
        related_information: if related.is_empty() {
            None
        } else {
            Some(related)
        },
        tags: if tags.is_empty() { None } else { Some(tags) },
        ..Default::default()
    })
}

/// Drop unused-declaration warnings (produced by
/// `knot_compiler::unused::check`, mapped to code `W001`) when the
/// `warnUnusedImports` config flag is off. The warnings are produced
/// unconditionally by the analysis pipeline — the worker doesn't see config,
/// and the snapshot/workspace caches store the full list — so gating happens
/// at every emission boundary (publish + pull handlers). That way a live
/// config change takes effect without re-analysis or cache invalidation.
pub(crate) fn filter_unused_warnings(
    items: Vec<Diagnostic>,
    warn_unused: bool,
) -> Vec<Diagnostic> {
    if warn_unused {
        return items;
    }
    items
        .into_iter()
        .filter(|d| d.code != Some(NumberOrString::String("W001".into())))
        .collect()
}

/// Hard cap on a single diagnostic message's serialized size. Compiler
/// diagnostics with a long chain of notes (e.g. trait-resolution failures
/// dragging in dozens of candidate impls) can blow past 100KB on the wire,
/// and some clients buffer the whole list in memory. Truncating to a few
/// kilobytes keeps editor responsiveness while still showing the bulk of
/// the explanation.
const MAX_DIAGNOSTIC_MESSAGE_BYTES: usize = 8 * 1024;

fn truncate_message(message: &mut String) {
    if message.len() <= MAX_DIAGNOSTIC_MESSAGE_BYTES {
        return;
    }
    // Truncate on a UTF-8 char boundary so the resulting String stays valid.
    // Walk backwards from the cap until we find one — at most 3 bytes back
    // for any valid UTF-8 sequence.
    let mut cut = MAX_DIAGNOSTIC_MESSAGE_BYTES;
    while cut > 0 && !message.is_char_boundary(cut) {
        cut -= 1;
    }
    message.truncate(cut);
    message.push_str("\n… (diagnostic truncated)");
}

/// Map diagnostic messages to structured error codes.
pub fn error_code_for_diagnostic(message: &str) -> Option<String> {
    let msg = message.to_lowercase();
    if msg.contains("type mismatch") || msg.contains("cannot unify") {
        Some("E001".into())
    } else if msg.contains("undefined") || msg.contains("unknown") || msg.contains("not found") {
        Some("E002".into())
    } else if msg.contains("missing") && msg.contains("field") {
        Some("E003".into())
    } else if msg.contains("exhaustive") || msg.contains("missing case") {
        Some("E004".into())
    } else if msg.contains("occurs check") || msg.contains("infinite type") {
        Some("E005".into())
    } else if msg.contains("duplicate") {
        Some("E006".into())
    } else if msg.contains("import") {
        Some("E007".into())
    } else if msg.contains("effect") || msg.contains("purity") {
        Some("E008".into())
    } else if msg.contains("stratif") {
        Some("E009".into())
    } else if msg.contains("trait") && (msg.contains("impl") || msg.contains("instance")) {
        Some("E010".into())
    } else if msg.contains("refine") || msg.contains("predicate") {
        Some("E011".into())
    } else if msg.contains("atomic") {
        Some("E012".into())
    } else if msg.contains("unit") && (msg.contains("mismatch") || msg.contains("conflict")) {
        Some("E013".into())
    } else if msg.contains("unused") {
        Some("W001".into())
    } else if msg.contains("shadow") {
        Some("W002".into())
    } else if msg.contains("runtime") && msg.contains("sql") {
        Some("I001".into())
    } else {
        None
    }
}

/// Human-readable description for a Knot error code.
pub fn description_for_code(code: &str) -> Option<&'static str> {
    Some(match code {
        "E001" => "Type mismatch — two expressions have incompatible types.",
        "E002" => "Reference to an undefined name (variable, function, type, or relation).",
        "E003" => "A record literal or pattern is missing a required field.",
        "E004" => "A `case` expression is not exhaustive — some constructor patterns are unmatched.",
        "E005" => "Occurs check failure — a type variable would have to contain itself.",
        "E006" => "Duplicate declaration — the same name is defined twice.",
        "E007" => "Import error — the module path does not resolve or has a cycle.",
        "E008" => "Effect mismatch — actual effects exceed the annotated effect set.",
        "E009" => "Stratification error — recursion crosses a negation boundary.",
        "E010" => "Trait/impl error — a required trait implementation is missing or invalid.",
        "E011" => "Refinement predicate failed — value does not satisfy the refined type.",
        "E012" => "Atomic-block restriction — only DB interactions allowed inside `atomic`.",
        "E013" => "Unit-of-measure mismatch — operands have incompatible units.",
        "W001" => "Unused declaration — defined but never referenced.",
        "W002" => "Shadowed binding — a name reuses an existing binding in the same scope.",
        "I001" => "Informational — runtime SQL note.",
        _ => return None,
    })
}

/// Map an error code to a documentation URL (placeholder host —
/// users can configure their own docs server).
fn doc_url_for_code(code: &str) -> Option<String> {
    Some(format!("https://knot-lang.org/errors/{code}"))
}


