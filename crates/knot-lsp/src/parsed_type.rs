//! Structured representation of type strings produced by the inferencer.
//!
//! The inferencer hands the LSP type info as pre-formatted strings (see
//! `infer::TypeInfo`). Multiple LSP features need to introspect those —
//! signature help splits on `->`, dot-completion finds record fields,
//! inlay hints extract `<unit>` annotations. Doing each query as a fresh
//! ad-hoc parse turned out brittle (effect rows shaped like `IO {a, b} T`
//! confuse a naive arrow-splitter; nested generics confuse a naive
//! brace-counter). This module parses once into `ParsedType`, and exposes
//! structural accessors the features can call.
//!
//! It deliberately uses a permissive parser: any string that doesn't match
//! a known shape becomes `ParsedType::Unknown(raw)`. The LSP is not the
//! place to enforce well-formedness — that's the inferencer's job.

#![allow(dead_code)]

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedType {
    Named(String, Vec<ParsedType>),
    Var(String),
    /// Function type as a curried sequence: each entry is one arrow. A
    /// 3-arg function `A -> B -> C -> D` parses as
    /// `Function([A, B, C], D)` so consumers can ask "how many params"
    /// without re-walking nested arrows.
    Function(Vec<ParsedType>, Box<ParsedType>),
    Record(Vec<(String, ParsedType)>, Option<String>),
    Variant(Vec<(String, Option<ParsedType>)>, Option<String>),
    Relation(Box<ParsedType>),
    Io {
        effects: Vec<String>,
        rest: Option<String>,
        ty: Box<ParsedType>,
    },
    /// Numeric type with unit annotation. `base` is `Int` or `Float`.
    UnitAnnotated {
        base: Box<ParsedType>,
        unit: String,
    },
    /// `T where <predicate>` — predicate kept as raw text.
    Refined {
        base: Box<ParsedType>,
        predicate: String,
    },
    Forall(Vec<String>, Box<ParsedType>),
    /// Fallback when we can't parse.
    Unknown(String),
}

impl ParsedType {
    /// Parse a formatted type string. Returns `Unknown(raw)` on
    /// unparseable input — never panics, never returns `None`.
    pub fn parse(input: &str) -> Self {
        let mut parser = Parser::new(input);
        parser.parse_top().unwrap_or_else(|| ParsedType::Unknown(input.to_string()))
    }

    /// Number of curried parameters. `Int -> Text -> Bool` returns `2`.
    /// Non-function types return `0`.
    pub fn arity(&self) -> usize {
        match self {
            ParsedType::Function(params, _) => params.len(),
            _ => 0,
        }
    }

    /// Curried parameter types, or empty for non-functions.
    pub fn params(&self) -> &[ParsedType] {
        match self {
            ParsedType::Function(p, _) => p,
            _ => &[],
        }
    }

    /// Final return type for a function, or `self` for non-functions.
    pub fn result(&self) -> &ParsedType {
        match self {
            ParsedType::Function(_, r) => r,
            other => other,
        }
    }

    /// Strip the IO wrapper if present so consumers can introspect the
    /// underlying value type. Recursive — `IO {} (IO {} T)` peels both.
    pub fn strip_io(&self) -> &ParsedType {
        match self {
            ParsedType::Io { ty, .. } => ty.strip_io(),
            other => other,
        }
    }

    /// If this is (or wraps) a record type, return its fields.
    pub fn record_fields(&self) -> Option<&[(String, ParsedType)]> {
        match self.strip_io() {
            ParsedType::Record(fs, _) => Some(fs),
            ParsedType::Relation(inner) => inner.record_fields(),
            ParsedType::UnitAnnotated { base, .. } => base.record_fields(),
            ParsedType::Refined { base, .. } => base.record_fields(),
            _ => None,
        }
    }

    /// If this is `Float<unit>` or `Int<unit>`, return the unit text.
    /// Skips trivial dimensionless `<1>`.
    pub fn unit(&self) -> Option<&str> {
        match self {
            ParsedType::UnitAnnotated { unit, .. } if unit != "1" => Some(unit),
            _ => None,
        }
    }

    /// Effects associated with this type's IO wrapper, if any.
    pub fn effects(&self) -> Option<&[String]> {
        match self {
            ParsedType::Io { effects, .. } => Some(effects),
            _ => None,
        }
    }

    /// Re-render. Round-trips clean for any well-formed input.
    pub fn render(&self) -> String {
        match self {
            ParsedType::Named(name, args) => {
                if args.is_empty() {
                    name.clone()
                } else {
                    let parts: Vec<String> = args.iter().map(|a| a.render_atomic()).collect();
                    format!("{name} {}", parts.join(" "))
                }
            }
            ParsedType::Var(name) => name.clone(),
            ParsedType::Function(params, ret) => {
                let mut parts: Vec<String> = params.iter().map(|p| p.render_atomic()).collect();
                parts.push(ret.render());
                parts.join(" -> ")
            }
            ParsedType::Record(fields, rest) => {
                let fs: Vec<String> = fields
                    .iter()
                    .map(|(n, t)| format!("{n}: {}", t.render()))
                    .collect();
                match rest {
                    Some(r) => format!("{{{} | {r}}}", fs.join(", ")),
                    None => format!("{{{}}}", fs.join(", ")),
                }
            }
            ParsedType::Variant(ctors, rest) => {
                let cs: Vec<String> = ctors
                    .iter()
                    .map(|(n, t)| match t {
                        Some(t) => format!("{n} {}", t.render()),
                        None => n.clone(),
                    })
                    .collect();
                match rest {
                    Some(r) => format!("<{} | {r}>", cs.join(" | ")),
                    None => format!("<{}>", cs.join(" | ")),
                }
            }
            ParsedType::Relation(t) => format!("[{}]", t.render()),
            ParsedType::Io { effects, rest, ty } => {
                let mut parts: Vec<String> = effects.clone();
                if let Some(r) = rest {
                    parts.push(format!("| {r}"));
                }
                format!("IO {{{}}} {}", parts.join(", "), ty.render_atomic())
            }
            ParsedType::UnitAnnotated { base, unit } => {
                format!("{}<{unit}>", base.render_atomic())
            }
            ParsedType::Refined { base, predicate } => {
                format!("{} where {predicate}", base.render())
            }
            ParsedType::Forall(vars, ty) => {
                format!("forall {}. {}", vars.join(" "), ty.render())
            }
            ParsedType::Unknown(s) => s.clone(),
        }
    }

    /// Render with surrounding parens when needed for unambiguous nesting.
    fn render_atomic(&self) -> String {
        match self {
            ParsedType::Function(_, _) => format!("({})", self.render()),
            other => other.render(),
        }
    }
}

// ── Parser ──────────────────────────────────────────────────────────

struct Parser<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str) -> Self {
        Self { src, pos: 0 }
    }

    fn parse_top(&mut self) -> Option<ParsedType> {
        self.skip_ws();
        // Try `forall a b. T` first.
        if self.eat_keyword("forall") {
            let mut vars = Vec::new();
            self.skip_ws();
            while let Some(name) = self.peek_ident() {
                if name == "." {
                    break;
                }
                vars.push(self.consume_ident()?);
                self.skip_ws();
            }
            if self.eat_char('.') {
                self.skip_ws();
                let body = self.parse_top()?;
                return Some(ParsedType::Forall(vars, Box::new(body)));
            }
        }
        // Constraint prefix `Display a => T` — drop it for now (the LSP
        // doesn't need to introspect constraints structurally yet).
        let snapshot = self.pos;
        if self.try_skip_constraints() {
            self.skip_ws();
            // Successful constraint skip — parse the body.
            return self.parse_function();
        }
        self.pos = snapshot;
        self.parse_function()
    }

    fn try_skip_constraints(&mut self) -> bool {
        // A constraint sequence ends with ` => `. If we don't find that
        // before the next `->` (or EOF), treat the whole thing as a type.
        let mut depth = 0i32;
        for (offset, b) in self.src[self.pos..].as_bytes().iter().enumerate() {
            let i = self.pos + offset;
            match *b {
                b'(' | b'[' | b'{' | b'<' => depth += 1,
                b')' | b']' | b'}' | b'>' => depth -= 1,
                b'=' if depth == 0
                    && i + 1 < self.src.len()
                    && self.src.as_bytes()[i + 1] == b'>' =>
                {
                    self.pos = i + 2;
                    return true;
                }
                b'-' if depth == 0
                    && i + 1 < self.src.len()
                    && self.src.as_bytes()[i + 1] == b'>' =>
                {
                    return false;
                }
                _ => {}
            }
        }
        false
    }

    fn parse_function(&mut self) -> Option<ParsedType> {
        let first = self.parse_refined()?;
        let mut params = Vec::new();
        let mut current = first;
        loop {
            self.skip_ws();
            if !self.eat_arrow() {
                break;
            }
            params.push(current);
            self.skip_ws();
            current = self.parse_refined()?;
        }
        if params.is_empty() {
            Some(current)
        } else {
            Some(ParsedType::Function(params, Box::new(current)))
        }
    }

    fn parse_refined(&mut self) -> Option<ParsedType> {
        let base = self.parse_app()?;
        self.skip_ws();
        if self.eat_keyword("where") {
            // Take the rest of the input verbatim as the predicate. Refined
            // types only appear at the outermost form when the inferrer
            // formats them, so this is safe.
            let pred = self.src[self.pos..].trim().to_string();
            self.pos = self.src.len();
            return Some(ParsedType::Refined {
                base: Box::new(base),
                predicate: pred,
            });
        }
        Some(base)
    }

    fn parse_app(&mut self) -> Option<ParsedType> {
        let head = self.parse_atom()?;
        // Application binds tighter than ->. After the head, any number
        // of atoms are arguments.
        let mut args = Vec::new();
        loop {
            self.skip_ws();
            if self.is_app_terminator() {
                break;
            }
            // An atom must start with one of these. Anything else (->, ,, },
            // etc.) terminates the app.
            let saved = self.pos;
            if let Some(arg) = self.parse_atom() {
                args.push(arg);
            } else {
                self.pos = saved;
                break;
            }
        }
        if args.is_empty() {
            Some(head)
        } else if let ParsedType::Named(name, mut existing) = head {
            existing.extend(args);
            Some(ParsedType::Named(name, existing))
        } else if let ParsedType::Io { effects, rest, ty } = head {
            // `IO {fx} T` parses with T as a separate atom; absorb a
            // trailing arg if `ty` is the empty/unknown placeholder.
            if matches!(*ty, ParsedType::Unknown(ref s) if s.is_empty()) {
                // `args` is guaranteed non-empty here (we returned early on
                // `args.is_empty()` above), but we still split with a fallback
                // rather than `.unwrap()` so a future refactor doesn't turn a
                // misuse into a server-killing panic.
                let mut iter = args.into_iter();
                let h = match iter.next() {
                    Some(h) => h,
                    None => return Some(ParsedType::Io { effects, rest, ty }),
                };
                let mut tail: Vec<ParsedType> = iter.collect();
                let value_ty = if tail.is_empty() {
                    Box::new(h)
                } else if let ParsedType::Named(n, mut a) = h {
                    a.append(&mut tail);
                    Box::new(ParsedType::Named(n, a))
                } else {
                    Box::new(h)
                };
                Some(ParsedType::Io {
                    effects,
                    rest,
                    ty: value_ty,
                })
            } else {
                Some(ParsedType::Io { effects, rest, ty })
            }
        } else {
            // Atom-with-args where head isn't named — uncommon; fall back.
            Some(head)
        }
    }

    fn parse_atom(&mut self) -> Option<ParsedType> {
        self.skip_ws();
        let c = self.peek()?;
        match c {
            '(' => self.parse_paren(),
            '[' => self.parse_relation(),
            '{' => self.parse_record(),
            '<' => self.parse_variant(),
            _ if c.is_alphabetic() || c == '_' => {
                let name = self.consume_ident()?;
                if name == "IO" {
                    return self.parse_io_tail();
                }
                let mut node = if first_uppercase(&name) || name == "_" {
                    ParsedType::Named(name, Vec::new())
                } else {
                    ParsedType::Var(name)
                };
                // `<unit>` only applies to numeric primitives.
                self.skip_ws();
                if self.peek() == Some('<') && type_takes_unit(&node) {
                    let saved = self.pos;
                    if let Some(unit) = self.parse_unit_braces() {
                        node = ParsedType::UnitAnnotated {
                            base: Box::new(node),
                            unit,
                        };
                    } else {
                        self.pos = saved;
                    }
                }
                Some(node)
            }
            _ => None,
        }
    }

    fn parse_paren(&mut self) -> Option<ParsedType> {
        self.eat_char('(');
        self.skip_ws();
        let inner = self.parse_function()?;
        self.skip_ws();
        self.eat_char(')');
        Some(inner)
    }

    fn parse_relation(&mut self) -> Option<ParsedType> {
        self.eat_char('[');
        self.skip_ws();
        if self.peek() == Some(']') {
            self.eat_char(']');
            // Bare `[]` is the empty list type constructor at value level;
            // not common as a type, but represent it as a variable.
            return Some(ParsedType::Named("[]".into(), Vec::new()));
        }
        let inner = self.parse_function()?;
        self.skip_ws();
        self.eat_char(']');
        Some(ParsedType::Relation(Box::new(inner)))
    }

    fn parse_record(&mut self) -> Option<ParsedType> {
        self.eat_char('{');
        let mut fields = Vec::new();
        let mut rest: Option<String> = None;
        loop {
            self.skip_ws();
            if self.peek() == Some('}') {
                self.eat_char('}');
                break;
            }
            if self.peek() == Some('|') {
                self.eat_char('|');
                self.skip_ws();
                rest = self.consume_ident();
                self.skip_ws();
                self.eat_char('}');
                break;
            }
            let name = self.consume_ident()?;
            self.skip_ws();
            if !self.eat_char(':') {
                return None;
            }
            self.skip_ws();
            let ty = self.parse_function()?;
            fields.push((name, ty));
            self.skip_ws();
            if self.peek() == Some(',') {
                self.eat_char(',');
            }
        }
        Some(ParsedType::Record(fields, rest))
    }

    fn parse_variant(&mut self) -> Option<ParsedType> {
        self.eat_char('<');
        let mut ctors: Vec<(String, Option<ParsedType>)> = Vec::new();
        let mut rest: Option<String> = None;
        loop {
            self.skip_ws();
            if self.peek() == Some('>') {
                self.eat_char('>');
                break;
            }
            // Row-tail variable. Open variants render as `<C1 {} | C2 {} | a>`
            // — the trailing `|` from the previous ctor is already consumed by
            // the time we reach this iteration, so the tail looks like a bare
            // ident. Constructors are always uppercase, so a lowercase ident
            // here is unambiguously the row variable.
            if let Some(name) = self.peek_ident() {
                if first_lowercase(&name) {
                    rest = self.consume_ident();
                    self.skip_ws();
                    self.eat_char('>');
                    break;
                }
            }
            let name = self.consume_ident()?;
            self.skip_ws();
            // Optional payload — anything up to `|` or `>`.
            let payload = if self.peek() == Some('|') || self.peek() == Some('>') {
                None
            } else {
                Some(self.parse_function()?)
            };
            ctors.push((name, payload));
            self.skip_ws();
            if self.peek() == Some('|') {
                self.eat_char('|');
            }
        }
        Some(ParsedType::Variant(ctors, rest))
    }

    fn parse_io_tail(&mut self) -> Option<ParsedType> {
        self.skip_ws();
        let (effects, rest) = if self.peek() == Some('{') {
            self.parse_effects_braces()?
        } else {
            (Vec::new(), None)
        };
        // The value type follows. parse_app has special handling to glue
        // the next atom on; emit a placeholder so it can detect us.
        Some(ParsedType::Io {
            effects,
            rest,
            ty: Box::new(ParsedType::Unknown(String::new())),
        })
    }

    fn parse_effects_braces(&mut self) -> Option<(Vec<String>, Option<String>)> {
        self.eat_char('{');
        let mut effects = Vec::new();
        let mut rest: Option<String> = None;
        loop {
            self.skip_ws();
            if self.peek() == Some('}') {
                self.eat_char('}');
                break;
            }
            if self.peek() == Some('|') {
                self.eat_char('|');
                self.skip_ws();
                rest = self.consume_ident();
                self.skip_ws();
                self.eat_char('}');
                break;
            }
            // Effects can have spaces (e.g. `reads *foo`). Read until comma,
            // pipe, or close-brace, respecting balanced delimiters.
            let start = self.pos;
            let mut depth = 0i32;
            while let Some(c) = self.peek() {
                match c {
                    '{' | '[' | '(' | '<' => depth += 1,
                    '}' | ']' | ')' | '>' if depth > 0 => depth -= 1,
                    '}' | '|' if depth == 0 => break,
                    ',' if depth == 0 => break,
                    _ => {}
                }
                self.advance();
            }
            let eff = self.src[start..self.pos].trim();
            if !eff.is_empty() {
                effects.push(eff.to_string());
            }
            self.skip_ws();
            if self.peek() == Some(',') {
                self.eat_char(',');
            }
        }
        Some((effects, rest))
    }

    fn parse_unit_braces(&mut self) -> Option<String> {
        self.eat_char('<');
        let start = self.pos;
        let mut depth = 1i32;
        while let Some(c) = self.peek() {
            match c {
                '<' => depth += 1,
                '>' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            self.advance();
        }
        let unit = self.src[start..self.pos].trim().to_string();
        if self.eat_char('>') {
            Some(unit)
        } else {
            None
        }
    }

    fn is_app_terminator(&self) -> bool {
        match self.peek() {
            None => true,
            Some(c) => matches!(c, '-' | ',' | '}' | ']' | ')' | '>' | '|'),
        }
    }

    fn peek(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }

    fn advance(&mut self) {
        if let Some(c) = self.peek() {
            self.pos += c.len_utf8();
        }
    }

    fn eat_char(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn eat_arrow(&mut self) -> bool {
        if self.src[self.pos..].starts_with("->") {
            self.pos += 2;
            true
        } else {
            false
        }
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn eat_keyword(&mut self, kw: &str) -> bool {
        let saved = self.pos;
        let rest = &self.src[self.pos..];
        if rest.starts_with(kw) {
            let next = rest[kw.len()..].chars().next();
            if next.map_or(true, |c| !is_ident_cont(c)) {
                self.pos += kw.len();
                return true;
            }
        }
        self.pos = saved;
        false
    }

    fn consume_ident(&mut self) -> Option<String> {
        let start = self.pos;
        let first = self.peek()?;
        if !(first.is_alphabetic() || first == '_') {
            return None;
        }
        self.advance();
        while let Some(c) = self.peek() {
            if is_ident_cont(c) {
                self.advance();
            } else {
                break;
            }
        }
        Some(self.src[start..self.pos].to_string())
    }

    fn peek_ident(&self) -> Option<String> {
        let mut i = self.pos;
        let bytes = self.src.as_bytes();
        let first = self.src[i..].chars().next()?;
        if !(first.is_alphabetic() || first == '_') {
            return None;
        }
        i += first.len_utf8();
        while i < self.src.len() {
            let c = self.src[i..].chars().next().unwrap();
            if is_ident_cont(c) {
                i += c.len_utf8();
            } else {
                break;
            }
        }
        let _ = bytes;
        Some(self.src[self.pos..i].to_string())
    }
}

fn is_ident_cont(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn first_uppercase(s: &str) -> bool {
    s.chars().next().map_or(false, |c| c.is_uppercase())
}

fn first_lowercase(s: &str) -> bool {
    s.chars().next().map_or(false, |c| c.is_lowercase())
}

fn type_takes_unit(t: &ParsedType) -> bool {
    matches!(
        t,
        ParsedType::Named(name, _) if name == "Int" || name == "Float"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> ParsedType {
        ParsedType::parse(s)
    }

    #[test]
    fn parse_named_and_var() {
        match p("Int") {
            ParsedType::Named(n, a) => {
                assert_eq!(n, "Int");
                assert!(a.is_empty());
            }
            other => panic!("{other:?}"),
        }
        match p("a") {
            ParsedType::Var(n) => assert_eq!(n, "a"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parse_function_arity() {
        let t = p("Int -> Text -> Bool");
        assert_eq!(t.arity(), 2);
        assert_eq!(t.params().len(), 2);
        match t.result() {
            ParsedType::Named(n, _) => assert_eq!(n, "Bool"),
            _ => panic!(),
        }
    }

    #[test]
    fn parse_relation_record() {
        let t = p("[{name: Text, age: Int}]");
        let fields = t.record_fields().expect("should have fields");
        let names: Vec<&str> = fields.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["name", "age"]);
    }

    #[test]
    fn parse_io_with_effects_strips() {
        let t = p("IO {console} Text");
        assert!(matches!(t, ParsedType::Io { .. }));
        match t.strip_io() {
            ParsedType::Named(n, _) => assert_eq!(n, "Text"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parse_unit_annotated() {
        let t = p("Float<M>");
        assert_eq!(t.unit(), Some("M"));
    }

    #[test]
    fn parse_dimensionless_unit_skipped() {
        let t = p("Int<1>");
        assert_eq!(t.unit(), None);
    }

    #[test]
    fn parse_function_arity_with_effect_set() {
        // Effect-set braces should not be mistaken for record/arrow tokens.
        let t = p("Int -> IO {console} Text");
        assert_eq!(t.arity(), 1);
    }

    #[test]
    fn parse_constraint_prefix_dropped() {
        let t = p("Display a => a -> Text");
        assert_eq!(t.arity(), 1);
    }

    #[test]
    fn parse_unknown_falls_through() {
        let t = p("");
        assert!(matches!(t, ParsedType::Unknown(_)));
    }

    #[test]
    fn render_round_trip_named() {
        let s = "Maybe Text";
        let t = p(s);
        assert_eq!(t.render(), s);
    }

    #[test]
    fn render_round_trip_function() {
        let s = "Int -> Text -> Bool";
        let t = p(s);
        assert_eq!(t.render(), s);
    }

    #[test]
    fn parse_variant_row_tail_is_not_a_constructor() {
        // The inferencer renders open variants as `<C1 {} | C2 {} | a>`.
        // The trailing lowercase ident is the row variable, not a ctor.
        let t = p("<Some Int | None | r>");
        match t {
            ParsedType::Variant(ctors, rest) => {
                let names: Vec<&str> = ctors.iter().map(|(n, _)| n.as_str()).collect();
                assert_eq!(names, vec!["Some", "None"]);
                assert_eq!(rest.as_deref(), Some("r"));
            }
            other => panic!("expected Variant, got {other:?}"),
        }
    }
}
