//! TUI database explorer for compiled Knot programs.
//!
//! Launched via `<program> db` — reads `_knot_schema` to discover relations
//! and displays their contents in Knot syntax.

use std::io::{self, stdout};

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Cell, Clear, HighlightSpacing, List, ListItem, ListState, Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState, Table, TableState, Wrap},
};
use rusqlite::Connection;

// ── Schema types (mirrors lib.rs internal types) ─────────────────

#[derive(Clone, Debug)]
struct RelationInfo {
    name: String,
    schema: String,
    row_count: usize,
}

#[derive(Clone, Debug)]
enum SchemaKind {
    Record(Vec<(String, String)>), // (field_name, type)
    Adt(Vec<AdtCtor>),
    Unit,
}

#[derive(Clone, Debug)]
struct AdtCtor {
    name: String,
    fields: Vec<(String, String)>,
}

// ── Schema parsing ───────────────────────────────────────────────

fn parse_schema_kind(schema: &str) -> SchemaKind {
    if schema.is_empty() {
        return SchemaKind::Unit;
    }
    if schema.starts_with('#') {
        let body = &schema[1..];
        let mut ctors = Vec::new();
        for ctor_part in split_respecting_brackets(body, '|') {
            let mut parts = ctor_part.splitn(2, ':');
            let name = parts.next().unwrap().to_string();
            let fields = if let Some(field_spec) = parts.next() {
                split_respecting_brackets(field_spec, ';')
                    .into_iter()
                    .map(|f| {
                        let mut fp = f.splitn(2, '=');
                        let fname = fp.next().unwrap().to_string();
                        let fty = fp.next().unwrap_or("text").to_string();
                        (fname, fty)
                    })
                    .collect()
            } else {
                Vec::new()
            };
            ctors.push(AdtCtor { name, fields });
        }
        SchemaKind::Adt(ctors)
    } else {
        let fields: Vec<(String, String)> = split_respecting_brackets(schema, ',')
            .into_iter()
            .filter_map(|part| {
                let colon = part.find(':')?;
                let name = part[..colon].to_string();
                let ty = part[colon + 1..].to_string();
                Some((name, ty))
            })
            .collect();
        SchemaKind::Record(fields)
    }
}

fn split_respecting_brackets(s: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth = depth.saturating_sub(1),
            c if c == sep && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

// ── Data loading ─────────────────────────────────────────────────

fn load_relations(conn: &Connection) -> Vec<RelationInfo> {
    // Check if _knot_schema exists
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='_knot_schema'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(false);

    if !exists {
        return Vec::new();
    }

    let mut stmt = conn
        .prepare("SELECT name, schema FROM _knot_schema ORDER BY name")
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            let name: String = row.get(0)?;
            let schema: String = row.get(1)?;
            Ok((name, schema))
        })
        .unwrap();

    let mut relations = Vec::new();
    for row in rows {
        let (name, schema) = row.unwrap();
        let table_name = format!("_knot_{}", name);
        let quoted_table = crate::quote_ident(&table_name);
        let count: usize = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {}", quoted_table),
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        relations.push(RelationInfo {
            name,
            schema,
            row_count: count,
        });
    }
    relations
}

/// Column headers for a relation
fn column_headers(schema: &SchemaKind) -> Vec<String> {
    match schema {
        SchemaKind::Unit => vec!["value".to_string()],
        SchemaKind::Record(fields) => fields.iter().map(|(n, _)| n.clone()).collect(),
        SchemaKind::Adt(_) => vec!["value".to_string()],
    }
}

/// Load rows from a relation as displayable strings
fn load_rows(conn: &Connection, rel: &RelationInfo) -> Vec<Vec<String>> {
    let schema = parse_schema_kind(&rel.schema);
    let table_name = format!("_knot_{}", rel.name);

    let quoted_table = crate::quote_ident(&table_name);

    match &schema {
        SchemaKind::Unit => {
            let count: usize = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {}", quoted_table),
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            (0..count).map(|_| vec!["{}".to_string()]).collect()
        }
        SchemaKind::Record(fields) => {
            let col_names: Vec<String> = fields
                .iter()
                .filter(|(_, ty)| !ty.starts_with('['))
                .map(|(n, _)| crate::quote_ident(n))
                .collect();

            if col_names.is_empty() {
                return Vec::new();
            }

            let sql = format!("SELECT {} FROM {}", col_names.join(", "), quoted_table);
            let mut stmt = match conn.prepare(&sql) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };

            let col_count = col_names.len();
            let field_types: Vec<&str> = fields
                .iter()
                .filter(|(_, ty)| !ty.starts_with('['))
                .map(|(_, ty)| ty.as_str())
                .collect();

            let rows = stmt
                .query_map([], |row| {
                    let mut cells = Vec::with_capacity(col_count);
                    for i in 0..col_count {
                        let cell = format_cell(row, i, field_types[i]);
                        cells.push(cell);
                    }
                    Ok(cells)
                })
                .unwrap();

            rows.filter_map(|r| r.ok()).collect()
        }
        SchemaKind::Adt(ctors) => {
            // Wide table: _tag + all constructor fields
            let mut all_fields: Vec<(String, String)> = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for ctor in ctors {
                for (fname, fty) in &ctor.fields {
                    if seen.insert(fname.clone()) {
                        all_fields.push((fname.clone(), fty.clone()));
                    }
                }
            }

            let mut select_cols = vec![crate::quote_ident("_tag")];
            for (fname, _) in &all_fields {
                select_cols.push(crate::quote_ident(fname));
            }

            let sql = format!("SELECT {} FROM {}", select_cols.join(", "), quoted_table);
            let mut stmt = match conn.prepare(&sql) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };

            let rows = stmt
                .query_map([], |row| {
                    let tag: String = row.get(0).unwrap_or_default();

                    // Find matching constructor
                    let ctor = ctors.iter().find(|c| c.name == tag);

                    let value = if let Some(ctor) = ctor {
                        if ctor.fields.is_empty() {
                            format!("{} {{}}", tag)
                        } else {
                            let field_strs: Vec<String> = ctor
                                .fields
                                .iter()
                                .map(|(fname, fty)| {
                                    // Find column index in all_fields
                                    let col_idx = all_fields
                                        .iter()
                                        .position(|(n, _)| n == fname)
                                        .unwrap();
                                    let cell = format_cell(row, col_idx + 1, fty);
                                    format!("{}: {}", fname, cell)
                                })
                                .collect();
                            format!("{} {{{}}}", tag, field_strs.join(", "))
                        }
                    } else {
                        format!("{} {{...}}", tag)
                    };

                    Ok(vec![value])
                })
                .unwrap();

            rows.filter_map(|r| r.ok()).collect()
        }
    }
}

fn format_cell(row: &rusqlite::Row, idx: usize, ty: &str) -> String {
    match ty {
        "int" => {
            // Could be stored as INTEGER or TEXT (for big ints)
            match row.get_ref(idx) {
                Ok(rusqlite::types::ValueRef::Integer(n)) => n.to_string(),
                Ok(rusqlite::types::ValueRef::Text(t)) => {
                    String::from_utf8_lossy(t).to_string()
                }
                Ok(rusqlite::types::ValueRef::Null) => "null".to_string(),
                _ => "?".to_string(),
            }
        }
        "float" => {
            match row.get_ref(idx) {
                Ok(rusqlite::types::ValueRef::Real(f)) => {
                    if f.is_nan() || f.is_infinite() {
                        format!("{}", f)
                    } else if f == (f as i64) as f64 {
                        format!("{:.1}", f)
                    } else {
                        f.to_string()
                    }
                }
                Ok(rusqlite::types::ValueRef::Null) => "null".to_string(),
                _ => "?".to_string(),
            }
        }
        "bool" => {
            match row.get::<_, Option<i32>>(idx) {
                Ok(Some(1)) => "True".to_string(),
                Ok(Some(0)) => "False".to_string(),
                Ok(None) => "null".to_string(),
                _ => "?".to_string(),
            }
        }
        "text" | "tag" => {
            match row.get::<_, Option<String>>(idx) {
                Ok(Some(s)) => {
                    if ty == "tag" {
                        format!("{} {{}}", s)
                    } else {
                        format!("\"{}\"", s)
                    }
                }
                Ok(None) => "null".to_string(),
                Err(_) => "?".to_string(),
            }
        }
        "bytes" => {
            match row.get::<_, Option<Vec<u8>>>(idx) {
                Ok(Some(b)) => {
                    let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                    format!("0x{}", hex)
                }
                Ok(None) => "null".to_string(),
                Err(_) => "?".to_string(),
            }
        }
        _ => {
            // Nested relation or unknown - show raw
            match row.get::<_, Option<String>>(idx) {
                Ok(Some(s)) => s,
                Ok(None) => "null".to_string(),
                Err(_) => "?".to_string(),
            }
        }
    }
}

// ── App state ────────────────────────────────────────────────────

enum Focus {
    Relations,
    Data,
}

struct App {
    relations: Vec<RelationInfo>,
    parsed_schemas: Vec<SchemaKind>,
    relation_state: ListState,
    data_rows: Vec<Vec<String>>,
    data_state: TableState,
    data_scroll: ScrollbarState,
    focus: Focus,
    detail_view: Option<String>, // expanded row view
}

impl App {
    fn new(conn: &Connection) -> Self {
        let relations = load_relations(conn);
        let parsed_schemas: Vec<SchemaKind> = relations
            .iter()
            .map(|r| parse_schema_kind(&r.schema))
            .collect();

        let mut app = App {
            relations,
            parsed_schemas,
            relation_state: ListState::default(),
            data_rows: Vec::new(),
            data_state: TableState::default(),
            data_scroll: ScrollbarState::default(),
            focus: Focus::Relations,
            detail_view: None,
        };

        if !app.relations.is_empty() {
            app.relation_state.select(Some(0));
        }

        app
    }

    fn selected_relation(&self) -> Option<&RelationInfo> {
        self.relation_state.selected().and_then(|i| self.relations.get(i))
    }

    fn selected_schema(&self) -> Option<&SchemaKind> {
        self.relation_state.selected().and_then(|i| self.parsed_schemas.get(i))
    }

    fn load_data(&mut self, conn: &Connection) {
        if let Some(rel) = self.selected_relation().cloned() {
            self.data_rows = load_rows(conn, &rel);
            self.data_state = TableState::default();
            if !self.data_rows.is_empty() {
                self.data_state.select(Some(0));
            }
            self.data_scroll = ScrollbarState::new(self.data_rows.len());
        } else {
            self.data_rows.clear();
            self.data_state = TableState::default();
            self.data_scroll = ScrollbarState::default();
        }
    }

    fn expand_selected_row(&mut self) {
        let Some(schema) = self.selected_schema().cloned() else { return };
        let Some(row_idx) = self.data_state.selected() else { return };
        let Some(row) = self.data_rows.get(row_idx) else { return };

        let text = match &schema {
            SchemaKind::Unit => "{}".to_string(),
            SchemaKind::Record(fields) => {
                let field_strs: Vec<String> = fields
                    .iter()
                    .filter(|(_, ty)| !ty.starts_with('['))
                    .zip(row.iter())
                    .map(|((name, _), val)| format!("  {}: {}", name, val))
                    .collect();
                format!("{{\n{}\n}}", field_strs.join(",\n"))
            }
            SchemaKind::Adt(_) => row.first().cloned().unwrap_or_default(),
        };

        self.detail_view = Some(text);
    }
}

// ── TUI rendering ────────────────────────────────────────────────

fn ui(frame: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(28), Constraint::Min(40)])
        .split(frame.area());

    // Left pane: relation list
    let items: Vec<ListItem> = app
        .relations
        .iter()
        .map(|r| {
            let line = Line::from(vec![
                Span::styled("*", Style::default().fg(Color::DarkGray)),
                Span::raw(format!("{}", r.name)),
                Span::styled(format!(" ({})", r.row_count), Style::default().fg(Color::DarkGray)),
            ]);
            ListItem::new(line)
        })
        .collect();

    let relations_block = Block::default()
        .title(" Relations ")
        .borders(Borders::ALL)
        .border_style(match app.focus {
            Focus::Relations => Style::default().fg(Color::Cyan),
            _ => Style::default().fg(Color::DarkGray),
        });

    let list = List::new(items)
        .block(relations_block)
        .highlight_style(Style::default().bg(Color::DarkGray).fg(Color::White).bold())
        .highlight_symbol(" › ");

    frame.render_stateful_widget(list, chunks[0], &mut app.relation_state);

    // Right pane: data table
    let (title, headers, widths) = if let Some(rel) = app.selected_relation() {
        let schema = parse_schema_kind(&rel.schema);
        let hdrs = column_headers(&schema);
        let w: Vec<Constraint> = hdrs
            .iter()
            .map(|h| {
                // Calculate width from header + data
                let max_data = app
                    .data_rows
                    .iter()
                    .filter_map(|row| row.get(hdrs.iter().position(|x| x == h)?))
                    .map(|s| s.len())
                    .max()
                    .unwrap_or(0);
                let w = h.len().max(max_data).min(40) + 2;
                Constraint::Length(w as u16)
            })
            .collect();
        (
            format!(" *{} ({} rows) ", rel.name, rel.row_count),
            hdrs,
            w,
        )
    } else {
        (" No relation selected ".to_string(), Vec::new(), Vec::new())
    };

    let data_block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(match app.focus {
            Focus::Data => Style::default().fg(Color::Cyan),
            _ => Style::default().fg(Color::DarkGray),
        });

    if headers.is_empty() {
        frame.render_widget(data_block, chunks[1]);
    } else {
        let header_cells: Vec<Cell> = headers
            .iter()
            .map(|h| Cell::from(h.as_str()).style(Style::default().fg(Color::Yellow).bold()))
            .collect();
        let header = Row::new(header_cells).bottom_margin(1);

        let rows: Vec<Row> = app
            .data_rows
            .iter()
            .map(|row| {
                let cells: Vec<Cell> = row.iter().map(|c| Cell::from(c.as_str())).collect();
                Row::new(cells)
            })
            .collect();

        let table = Table::new(rows, &widths)
            .header(header)
            .block(data_block)
            .row_highlight_style(Style::default().bg(Color::DarkGray))
            .highlight_spacing(HighlightSpacing::Always);

        frame.render_stateful_widget(table, chunks[1], &mut app.data_state);

        // Scrollbar
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(None)
            .end_symbol(None);
        let scrollbar_area = chunks[1].inner(Margin { vertical: 1, horizontal: 0 });
        app.data_scroll = app
            .data_scroll
            .position(app.data_state.selected().unwrap_or(0));
        frame.render_stateful_widget(scrollbar, scrollbar_area, &mut app.data_scroll);
    }

    // Detail overlay
    if let Some(ref detail) = app.detail_view {
        let area = centered_rect(60, 60, frame.area());
        frame.render_widget(Clear, area);
        let block = Block::default()
            .title(" Row Detail ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));
        let paragraph = Paragraph::new(detail.as_str())
            .block(block)
            .wrap(Wrap { trim: false });
        frame.render_widget(paragraph, area);
    }

    // Help bar
    let help_area = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(frame.area())[1];

    let help = Line::from(vec![
        Span::styled(" ↑↓ ", Style::default().fg(Color::Cyan).bold()),
        Span::raw("navigate  "),
        Span::styled("←→/Tab ", Style::default().fg(Color::Cyan).bold()),
        Span::raw("switch pane  "),
        Span::styled("Enter ", Style::default().fg(Color::Cyan).bold()),
        Span::raw("expand  "),
        Span::styled("q/Esc ", Style::default().fg(Color::Cyan).bold()),
        Span::raw("quit"),
    ]);
    frame.render_widget(Paragraph::new(help), help_area);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

// ── Entry point ──────────────────────────────────────────────────

pub fn run_db_explorer(db_path: &str) -> io::Result<()> {
    let conn = Connection::open(db_path).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("Failed to open database: {}", e))
    })?;

    let mut app = App::new(&conn);
    app.load_data(&conn);

    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

    let result = (|| -> io::Result<()> {
        loop {
            terminal.draw(|f| ui(f, &mut app))?;

            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }

                // Detail view dismissal
                if app.detail_view.is_some() {
                    match key.code {
                        KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter => {
                            app.detail_view = None;
                        }
                        _ => {}
                    }
                    continue;
                }

                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Tab | KeyCode::Right | KeyCode::Left => {
                        app.focus = match app.focus {
                            Focus::Relations => Focus::Data,
                            Focus::Data => Focus::Relations,
                        };
                    }
                    KeyCode::Up => match app.focus {
                        Focus::Relations => {
                            let i = app.relation_state.selected().unwrap_or(0);
                            if i > 0 {
                                app.relation_state.select(Some(i - 1));
                                app.load_data(&conn);
                            }
                        }
                        Focus::Data => {
                            let i = app.data_state.selected().unwrap_or(0);
                            if i > 0 {
                                app.data_state.select(Some(i - 1));
                            }
                        }
                    },
                    KeyCode::Down => match app.focus {
                        Focus::Relations => {
                            let i = app.relation_state.selected().unwrap_or(0);
                            if i + 1 < app.relations.len() {
                                app.relation_state.select(Some(i + 1));
                                app.load_data(&conn);
                            }
                        }
                        Focus::Data => {
                            let i = app.data_state.selected().unwrap_or(0);
                            if i + 1 < app.data_rows.len() {
                                app.data_state.select(Some(i + 1));
                            }
                        }
                    },
                    KeyCode::Enter => {
                        if matches!(app.focus, Focus::Data) {
                            app.expand_selected_row();
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    })();

    // Always restore terminal, even if the loop returned an error
    let _ = disable_raw_mode();
    let _ = stdout().execute(LeaveAlternateScreen);
    result
}
