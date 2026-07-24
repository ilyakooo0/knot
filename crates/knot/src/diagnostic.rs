//! Error reporting infrastructure for the Knot compiler.

use crate::ast::Span;

// ── Types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Label {
    pub span: Span,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn info(msg: impl Into<String>) -> Self {
        Self {
            severity: Severity::Info,
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

// ── Source helpers (still used by LSP and other consumers) ───────────

/// Returns `(line, col)` for a byte offset. Line is 1-based, column is 0-based (in characters, not bytes).
pub fn line_col(source: &str, byte_offset: usize) -> (usize, usize) {
    let offset = byte_offset.min(source.len());
    let before = &source.as_bytes()[..offset];
    // Count line breaks treating `\n`, lone `\r`, and `\r\n` each as a single
    // break, matching the lexer's layout handling and the parser's column
    // bookkeeping (a `\r`-only or Windows source must not collapse to line 1).
    let mut line = 1;
    let mut line_start = 0;
    let mut i = 0;
    while i < before.len() {
        match before[i] {
            b'\n' => {
                line += 1;
                i += 1;
                line_start = i;
            }
            b'\r' => {
                line += 1;
                // `\r\n` is one break, not two.  Consult the full source
                // bytes (not the truncated `before`) when peeking ahead,
                // because `i+1` may point past `offset` — e.g. when
                // `offset` lands on the `\n` of a `\r\n`, `before` ends
                // at the `\r` and `before.get(i+1)` returns `None`,
                // wrongly treating the `\r` as a lone CR.
                let full = source.as_bytes();
                i += if full.get(i + 1) == Some(&b'\n') { 2 } else { 1 };
                line_start = i;
            }
            _ => i += 1,
        }
    }
    // Clamp to a valid char boundary in case offset lands mid-character.
    let mut safe_offset = offset;
    while safe_offset > line_start && !source.is_char_boundary(safe_offset) {
        safe_offset -= 1;
    }
    let col = source[line_start..safe_offset].chars().count();
    (line, col)
}

/// Returns the content of a 1-based line number. Returns `""` if out of bounds.
pub fn get_line(source: &str, line: usize) -> &str {
    if line == 0 {
        return "";
    }
    let bytes = source.as_bytes();
    let mut current_line = 1;
    let mut start = 0;
    let mut i = 0;
    // Advance to the start of the requested line, treating `\n`, lone `\r`,
    // and `\r\n` each as one line break.
    while current_line < line {
        if i >= bytes.len() {
            return "";
        }
        match bytes[i] {
            b'\n' => {
                current_line += 1;
                i += 1;
                start = i;
            }
            b'\r' => {
                current_line += 1;
                i += if bytes.get(i + 1) == Some(&b'\n') { 2 } else { 1 };
                start = i;
            }
            _ => i += 1,
        }
    }
    // End the line at the next break of any kind.
    let mut end = start;
    while end < bytes.len() && bytes[end] != b'\n' && bytes[end] != b'\r' {
        end += 1;
    }
    &source[start..end]
}

// ── Rendering (ariadne) ─────────────────────────────────────────────

impl Diagnostic {
    pub fn render(&self, source: &str, filename: &str) -> String {
        use ariadne::{CharSet, Config, ColorGenerator, Label as ALabel, Report, ReportKind, Source};

        let kind = match self.severity {
            Severity::Error => ReportKind::Error,
            Severity::Warning => ReportKind::Warning,
            Severity::Info => ReportKind::Advice,
        };

        // Knot stores byte-offset spans, but ariadne 0.6 expects character
        // offsets (per its `Span` trait docs). Convert once per label so
        // diagnostics line up correctly when the source contains non-ASCII
        // (e.g. `→`, `—`) above the error.
        let byte_to_char = |byte_offset: usize| -> usize {
            let clamped = byte_offset.min(source.len());
            let mut safe = clamped;
            while safe > 0 && !source.is_char_boundary(safe) {
                safe -= 1;
            }
            source[..safe].chars().count()
        };

        let header_offset = self
            .labels
            .first()
            .map_or(0, |l| byte_to_char(l.span.start));
        let fname = filename.to_string();

        let mut colors = ColorGenerator::new();

        let mut builder = Report::build(kind, (fname.clone(), header_offset..header_offset))
            .with_message(&self.message)
            .with_config(Config::default().with_char_set(CharSet::Unicode));

        for label in &self.labels {
            let color = colors.next();
            let start = byte_to_char(label.span.start);
            let end = byte_to_char(label.span.end);
            builder = builder.with_label(
                ALabel::new((fname.clone(), start..end))
                    .with_message(&label.message)
                    .with_color(color),
            );
        }

        for note in &self.notes {
            builder = builder.with_help(note);
        }

        let report = builder.finish();

        let mut buf = Vec::new();
        report.write(
            (filename.to_string(), Source::from(source)),
            &mut buf,
        ).expect("write to Vec cannot fail");

        String::from_utf8(buf)
            .expect("ariadne output is always UTF-8")
    }
}


