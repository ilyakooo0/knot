//! Error reporting infrastructure for the Knot compiler.

use std::fmt::Write;

use crate::ast::Span;

// ── Types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

#[derive(Debug, Clone)]
pub struct Label {
    pub span: Span,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub severity: Severity,
    pub message: String,
    pub labels: Vec<Label>,
    pub notes: Vec<String>,
}

// ── Builder ──────────────────────────────────────────────────────────

impl Diagnostic {
    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            message: msg.into(),
            labels: Vec::new(),
            notes: Vec::new(),
        }
    }

    pub fn warning(msg: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            message: msg.into(),
            labels: Vec::new(),
            notes: Vec::new(),
        }
    }

    pub fn label(mut self, span: Span, msg: impl Into<String>) -> Self {
        self.labels.push(Label { span, message: msg.into() });
        self
    }

    pub fn note(mut self, msg: impl Into<String>) -> Self {
        self.notes.push(msg.into());
        self
    }
}

// ── Source helpers ────────────────────────────────────────────────────

/// Returns `(line, col)` for a byte offset. Line is 1-based, column is 0-based.
pub fn line_col(source: &str, byte_offset: usize) -> (usize, usize) {
    let offset = byte_offset.min(source.len());
    let before = &source[..offset];
    let line = before.chars().filter(|&c| c == '\n').count() + 1;
    let col = match before.rfind('\n') {
        Some(nl) => offset - nl - 1,
        None => offset,
    };
    (line, col)
}

/// Returns the content of a 1-based line number. Returns `""` if out of bounds.
pub fn get_line(source: &str, line: usize) -> &str {
    if line == 0 {
        return "";
    }
    source.split('\n').nth(line - 1).unwrap_or("")
}

// ── ANSI color helpers ───────────────────────────────────────────────

fn use_color() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    std::io::IsTerminal::is_terminal(&std::io::stderr())
}

struct Colors {
    red: &'static str,
    yellow: &'static str,
    cyan: &'static str,
    blue: &'static str,
    bold: &'static str,
    reset: &'static str,
}

const COLORS_ON: Colors = Colors {
    red: "\x1b[31m",
    yellow: "\x1b[33m",
    cyan: "\x1b[36m",
    blue: "\x1b[34m",
    bold: "\x1b[1m",
    reset: "\x1b[0m",
};

const COLORS_OFF: Colors = Colors {
    red: "",
    yellow: "",
    cyan: "",
    blue: "",
    bold: "",
    reset: "",
};

fn colors() -> &'static Colors {
    if use_color() { &COLORS_ON } else { &COLORS_OFF }
}

// ── Rendering ────────────────────────────────────────────────────────

impl Diagnostic {
    pub fn render(&self, source: &str, filename: &str) -> String {
        let mut out = String::new();
        let c = colors();

        // Header: "error: message" or "warning: message"
        let (sev, sev_color) = match self.severity {
            Severity::Error => ("error", c.red),
            Severity::Warning => ("warning", c.yellow),
        };
        let _ = writeln!(
            out,
            "{sev_color}{bold}{sev}{reset}: {bold}{msg}{reset}",
            bold = c.bold,
            reset = c.reset,
            msg = self.message,
        );

        // Group labels by line number so we only show each source line once.
        let mut by_line: std::collections::BTreeMap<usize, Vec<&Label>> =
            std::collections::BTreeMap::new();
        for label in &self.labels {
            let (line, _) = line_col(source, label.span.start);
            by_line.entry(line).or_default().push(label);
        }

        // Compute gutter width from the largest line number we'll display.
        let max_line = by_line.keys().last().copied().unwrap_or(1);
        let gutter = max_line.to_string().len();

        for (&line_no, labels) in &by_line {
            let src_line = get_line(source, line_no);

            // Location arrow for the first label in this group.
            let (_, first_col) = line_col(source, labels[0].span.start);
            let _ = writeln!(
                out,
                "{blue}{:>gutter$}--> {reset}{filename}:{line_no}:{col}",
                "",
                col = first_col + 1,
                blue = c.blue,
                reset = c.reset,
            );
            let _ = writeln!(out, "{blue}{:>gutter$} |{reset}", "", blue = c.blue, reset = c.reset);

            // Source line.
            let _ = writeln!(
                out,
                "{blue}{line_no:>gutter$} |{reset} {src_line}",
                blue = c.blue,
                reset = c.reset,
            );

            // Underline + message for each label on this line.
            let ul_color = match self.severity {
                Severity::Error => c.red,
                Severity::Warning => c.yellow,
            };
            for label in labels {
                let (_, col) = line_col(source, label.span.start);
                let span_len = (label.span.end - label.span.start).max(1);
                let _ = writeln!(
                    out,
                    "{blue}{:>gutter$} |{reset} {:>col$}{ul_color}{bold}{carets}{reset} {ul_color}{msg}{reset}",
                    "",
                    "",
                    carets = "^".repeat(span_len),
                    msg = label.message,
                    blue = c.blue,
                    reset = c.reset,
                    ul_color = ul_color,
                    bold = c.bold,
                );
            }

            let _ = writeln!(out, "{blue}{:>gutter$} |{reset}", "", blue = c.blue, reset = c.reset);
        }

        // Notes.
        for note in &self.notes {
            let _ = writeln!(
                out,
                "{blue}{:>gutter$} = {cyan}help:{reset} {note}",
                "",
                blue = c.blue,
                cyan = c.cyan,
                reset = c.reset,
            );
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_col_basic() {
        let src = "aaa\nbbb\nccc";
        assert_eq!(line_col(src, 0), (1, 0));
        assert_eq!(line_col(src, 3), (1, 3));
        assert_eq!(line_col(src, 4), (2, 0));
        assert_eq!(line_col(src, 8), (3, 0));
        // Past end clamps.
        assert_eq!(line_col(src, 999), (3, 3));
    }

    #[test]
    fn line_col_empty() {
        assert_eq!(line_col("", 0), (1, 0));
        assert_eq!(line_col("", 5), (1, 0));
    }

    #[test]
    fn get_line_basic() {
        let src = "alpha\nbeta\ngamma";
        assert_eq!(get_line(src, 1), "alpha");
        assert_eq!(get_line(src, 3), "gamma");
        assert_eq!(get_line(src, 0), "");
        assert_eq!(get_line(src, 99), "");
    }

    #[test]
    fn render_snapshot() {
        // Force NO_COLOR so test output is deterministic.
        unsafe { std::env::set_var("NO_COLOR", "1") };
        let src = "let x = 1\n  if x > 0\n    42";
        let diag = Diagnostic::error("expected `then` after condition")
            .label(Span::new(12, 17), "expected `then` after this")
            .note("add `then` before the consequent");
        let rendered = diag.render(src, "input");
        assert!(rendered.contains("error: expected `then` after condition"));
        assert!(rendered.contains("--> input:2:3"));
        assert!(rendered.contains("^^^^^"));
        assert!(rendered.contains("= help: add `then`"));
    }
}
