//! Knot runtime library.
//!
//! Provides C-ABI functions for value management, relation operations,
//! and SQLite-backed persistence. This crate is compiled as a static
//! library and linked into every compiled Knot program.

use num_bigint::BigInt;
use num_traits::ToPrimitive;
use num_traits::Zero;
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};

// ── Arena allocator ──────────────────────────────────────────────

/// A mark-and-reset arena for `Value` allocations.
///
/// All `*mut Value` pointers are heap-allocated via `Box` and tracked here.
/// `mark()` snapshots the current position; `reset_to(mark)` drops and frees
/// every allocation made after that mark. Dropping a `Value` frees its owned
/// data (String, Vec, BigInt) but does NOT recurse into child `*mut Value`
/// pointers — those are independently tracked in the arena.
struct Arena {
    ptrs: Vec<*mut Value>,
}

impl Arena {
    fn new() -> Self {
        Arena { ptrs: Vec::new() }
    }

    fn alloc(&mut self, v: Value) -> *mut Value {
        let ptr = Box::into_raw(Box::new(v));
        self.ptrs.push(ptr);
        ptr
    }

    fn mark(&self) -> usize {
        self.ptrs.len()
    }

    fn reset_to(&mut self, mark: usize) {
        for ptr in self.ptrs.drain(mark..) {
            unsafe { drop(Box::from_raw(ptr)); }
        }
    }
}

thread_local! {
    static ARENA: RefCell<Arena> = RefCell::new(Arena::new());
}

// ── Debug mode ───────────────────────────────────────────────────

static DEBUG: AtomicBool = AtomicBool::new(false);

fn debug_enabled() -> bool {
    DEBUG.load(Ordering::Relaxed)
}

fn debug_sql(sql: &str) {
    if debug_enabled() {
        eprintln!("[SQL] {}", sql);
    }
}

fn debug_sql_params(sql: &str, params: &[rusqlite::types::Value]) {
    if debug_enabled() {
        if params.is_empty() {
            eprintln!("[SQL] {}", sql);
        } else {
            eprintln!("[SQL] {} -- params: {:?}", sql, params);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_debug_init() {
    for arg in std::env::args() {
        if arg == "--debug" {
            DEBUG.store(true, Ordering::Relaxed);
            return;
        }
    }
}

// ── Value representation ──────────────────────────────────────────

/// Runtime representation of all Knot values.
///
/// Every Knot expression evaluates to a heap-allocated `Value`.
/// The Cranelift-generated code works exclusively with `*mut Value` pointers.
pub enum Value {
    Int(BigInt),
    Float(f64),
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
    Unit,
    Record(Vec<RecordField>),
    Relation(Vec<*mut Value>),
    Constructor(String, *mut Value),
    /// (fn_ptr, env, source) — fn_ptr has signature: extern "C" fn(db, env, arg) -> *mut Value
    Function(*const u8, *mut Value, String),
}

pub struct RecordField {
    pub name: String,
    pub value: *mut Value,
}

/// SQLite database handle.
pub struct KnotDb {
    pub conn: Connection,
    /// Nesting depth for `atomic` savepoints.
    atomic_depth: std::cell::Cell<usize>,
    /// Tracks which indexes have been created this session to avoid redundant DDL.
    indexed: RefCell<HashSet<String>>,
}

impl KnotDb {
    /// Create an index on `column` of `table` if one hasn't been created yet.
    fn ensure_index(&self, table: &str, column: &str) {
        let key = format!("{}:{}", table, column);
        if self.indexed.borrow().contains(&key) {
            return;
        }
        let idx_name = format!("_knot_auto_{}_{}", table, column);
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&idx_name),
            quote_ident(table),
            quote_ident(column)
        );
        debug_sql(&sql);
        let _ = self.conn.execute_batch(&sql);
        self.indexed.borrow_mut().insert(key);
    }

    /// Ensure indexes on all columns referenced in a WHERE clause.
    /// Column names in generated SQL are always double-quoted identifiers.
    fn ensure_indexes_for_where(&self, table: &str, where_clause: &str) {
        for col in extract_where_columns(where_clause) {
            self.ensure_index(table, &col);
        }
    }
}

/// Extract column names from a generated SQL WHERE clause.
/// Columns are always double-quoted identifiers (e.g. `"age"`, `"name"`).
fn extract_where_columns(sql: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' {
            let mut col = String::new();
            loop {
                match chars.next() {
                    Some('"') => {
                        if chars.peek() == Some(&'"') {
                            col.push('"');
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    Some(ch) => col.push(ch),
                    None => break,
                }
            }
            if seen.insert(col.clone()) {
                columns.push(col);
            }
        }
    }
    columns
}

// ── Helpers ───────────────────────────────────────────────────────

fn alloc(v: Value) -> *mut Value {
    ARENA.with(|a| a.borrow_mut().alloc(v))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_arena_mark() -> usize {
    ARENA.with(|a| a.borrow().mark())
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_arena_reset_to(mark: usize) {
    ARENA.with(|a| a.borrow_mut().reset_to(mark));
}

unsafe fn as_ref<'a>(v: *mut Value) -> &'a Value {
    unsafe { &*v }
}

unsafe fn str_from_raw(ptr: *const u8, len: usize) -> &'static str {
    unsafe { std::str::from_utf8_unchecked(slice::from_raw_parts(ptr, len)) }
}

/// Runtime error for missing trait implementations.
#[unsafe(no_mangle)]
pub extern "C" fn knot_trait_no_impl(
    method_ptr: *const u8,
    method_len: usize,
    value: *mut Value,
) -> *mut Value {
    let method = unsafe { str_from_raw(method_ptr, method_len) };
    panic!(
        "knot runtime: no implementation of '{}' for type {}",
        method,
        brief_value(value)
    );
}

fn type_name(v: *mut Value) -> &'static str {
    if v.is_null() {
        return "null";
    }
    match unsafe { as_ref(v) } {
        Value::Int(_) => "Int",
        Value::Float(_) => "Float",
        Value::Text(_) => "Text",
        Value::Bool(_) => "Bool",
        Value::Bytes(_) => "Bytes",
        Value::Unit => "Unit",
        Value::Record(_) => "Record",
        Value::Relation(_) => "Relation",
        Value::Constructor(_, _) => "Constructor",
        Value::Function(_, _, _) => "Function",
    }
}

fn brief_value(v: *mut Value) -> String {
    if v.is_null() {
        return "null".to_string();
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => format!("Int({})", n),
        Value::Float(n) => format!("Float({})", n),
        Value::Text(s) => {
            if s.len() > 30 {
                format!("Text(\"{}...\")", &s[..27])
            } else {
                format!("Text(\"{}\")", s)
            }
        }
        Value::Bool(b) => format!("Bool({})", b),
        Value::Bytes(b) => format!("Bytes({} bytes)", b.len()),
        Value::Unit => "Unit".to_string(),
        Value::Record(fields) => {
            let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
            format!("Record({{{}}})", names.join(", "))
        }
        Value::Relation(rows) => format!("Relation({} rows)", rows.len()),
        Value::Constructor(tag, _) => format!("Constructor({})", tag),
        Value::Function(_, _, src) => format!("Function({})", src),
    }
}

/// Escape a SQL identifier by wrapping it in double quotes and doubling
/// any internal `"` characters, per the SQL standard.
fn quote_ident(name: &str) -> String {
    if name.contains('"') {
        let mut s = String::with_capacity(name.len() + 2);
        s.push('"');
        for ch in name.chars() {
            if ch == '"' { s.push('"'); }
            s.push(ch);
        }
        s.push('"');
        s
    } else {
        let mut s = String::with_capacity(name.len() + 2);
        s.push('"');
        s.push_str(name);
        s.push('"');
        s
    }
}

// ── Value constructors ────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_int(n: i64) -> *mut Value {
    alloc(Value::Int(BigInt::from(n)))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_int_from_str(ptr: *const u8, len: usize) -> *mut Value {
    let s = unsafe { str_from_raw(ptr, len) };
    let n = s.parse::<BigInt>().unwrap_or_else(|_| BigInt::ZERO);
    alloc(Value::Int(n))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_float(n: f64) -> *mut Value {
    alloc(Value::Float(n))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_text(ptr: *const u8, len: usize) -> *mut Value {
    let s = unsafe { str_from_raw(ptr, len) };
    alloc(Value::Text(s.to_string()))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_bool(b: i32) -> *mut Value {
    alloc(Value::Bool(b != 0))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_unit() -> *mut Value {
    alloc(Value::Unit)
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_function(
    fn_ptr: *const u8,
    env: *mut Value,
    src_ptr: *const u8,
    src_len: usize,
) -> *mut Value {
    let source = unsafe { str_from_raw(src_ptr, src_len) }.to_string();
    alloc(Value::Function(fn_ptr, env, source))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_constructor(
    tag_ptr: *const u8,
    tag_len: usize,
    payload: *mut Value,
) -> *mut Value {
    let tag = unsafe { str_from_raw(tag_ptr, tag_len) }.to_string();
    alloc(Value::Constructor(tag, payload))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_bytes(ptr: *const u8, len: usize) -> *mut Value {
    let bytes = if ptr.is_null() || len == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(ptr, len) }.to_vec()
    };
    alloc(Value::Bytes(bytes))
}

// ── Value accessors ───────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_get_int(v: *mut Value) -> i64 {
    match unsafe { as_ref(v) } {
        Value::Int(n) => n.to_i64().expect("knot runtime: Int too large for i64"),
        _ => panic!("knot runtime: expected Int, got {}", brief_value(v)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_get_float(v: *mut Value) -> f64 {
    match unsafe { as_ref(v) } {
        Value::Float(n) => *n,
        _ => panic!("knot runtime: expected Float, got {}", brief_value(v)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_get_bool(v: *mut Value) -> i32 {
    match unsafe { as_ref(v) } {
        Value::Bool(b) => *b as i32,
        _ => panic!("knot runtime: expected Bool, got {}", brief_value(v)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_get_tag(v: *mut Value) -> i32 {
    if v.is_null() {
        return 9; // Nullable none (null pointer)
    }
    match unsafe { as_ref(v) } {
        Value::Int(_) => 0,
        Value::Float(_) => 1,
        Value::Text(_) => 2,
        Value::Bool(_) => 3,
        Value::Unit => 4,
        Value::Record(_) => 5,
        Value::Relation(_) => 6,
        Value::Constructor(_, _) => 7,
        Value::Function(_, _, _) => 8,
        Value::Bytes(_) => 10,
    }
}

// ── Record operations ─────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_record_empty(capacity: usize) -> *mut Value {
    alloc(Value::Record(Vec::with_capacity(capacity)))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_record_set_field(
    record: *mut Value,
    key_ptr: *const u8,
    key_len: usize,
    value: *mut Value,
) {
    let name = unsafe { str_from_raw(key_ptr, key_len) }.to_string();
    let rec = unsafe { &mut *record };
    match rec {
        Value::Record(fields) => {
            // Maintain sorted order by field name for O(log n) lookup
            match fields.binary_search_by(|f| f.name.as_str().cmp(&name)) {
                Ok(idx) => fields[idx].value = value,
                Err(idx) => fields.insert(idx, RecordField { name, value }),
            }
        }
        _ => panic!("knot runtime: expected Record in set_field, got {}", type_name(record)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_record_field(
    record: *mut Value,
    key_ptr: *const u8,
    key_len: usize,
) -> *mut Value {
    if record.is_null() {
        let name = unsafe { str_from_raw(key_ptr, key_len) };
        panic!("knot runtime: field '{}' access on null (nullable none variant)", name);
    }
    let name = unsafe { str_from_raw(key_ptr, key_len) };
    match unsafe { as_ref(record) } {
        Value::Record(fields) => {
            // Binary search for O(log n) lookup (fields are kept sorted)
            if let Ok(idx) = fields.binary_search_by(|f| f.name.as_str().cmp(name)) {
                return fields[idx].value;
            }
            // Fallback: linear scan for records not built via set_field
            for field in fields {
                if field.name == name {
                    return field.value;
                }
            }
            let available: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
            panic!(
                "knot runtime: field '{}' not found in record\n  available fields: {}",
                name,
                if available.is_empty() { "(none)".to_string() } else { available.join(", ") }
            )
        }
        Value::Constructor(_, payload) => {
            // Delegate to the payload (which should be a record)
            knot_record_field(*payload, key_ptr, key_len)
        }
        Value::Relation(rows) => {
            // After groupBy, field access on a group relation delegates to first element.
            // All elements in a group share the key fields, so this is well-defined.
            if rows.is_empty() {
                let name = unsafe { str_from_raw(key_ptr, key_len) };
                panic!("knot runtime: field '{}' access on empty relation group", name);
            }
            knot_record_field(rows[0], key_ptr, key_len)
        }
        _ => panic!(
            "knot runtime: expected Record in field access, got {}",
            brief_value(record)
        ),
    }
}

// ── SQLite-backed temp tables for relation operations ─────────────

thread_local! {
    static TEMP_COUNTER: std::cell::Cell<u64> = std::cell::Cell::new(0);
}

fn next_temp_name() -> String {
    TEMP_COUNTER.with(|c| {
        let n = c.get();
        c.set(n + 1);
        format!("_knot_tmp_{}", n)
    })
}

/// Schema for SQLite temp tables, inferred from relation values at runtime.
enum TempSchema {
    /// Records: named columns with SQL types
    Record(Vec<(String, ColType)>),
    /// Scalars (Int, Float, Text, Bool, Bytes): single `_val` column
    Scalar(ColType),
    /// ADT constructors: `_tag TEXT` + nullable fields from all constructors
    Adt {
        constructors: Vec<(String, Vec<(String, ColType)>)>,
        all_fields: Vec<(String, ColType)>,
    },
    /// Unit values
    Unit,
}

/// Infer the SQL column type from a runtime Value.
fn infer_col_type(v: *mut Value) -> Option<ColType> {
    if v.is_null() {
        return Some(ColType::Text);
    }
    match unsafe { as_ref(v) } {
        Value::Int(_) => Some(ColType::Int),
        Value::Float(_) => Some(ColType::Float),
        Value::Text(_) => Some(ColType::Text),
        Value::Bool(_) => Some(ColType::Bool),
        Value::Bytes(_) => Some(ColType::Bytes),
        Value::Unit => None,
        Value::Constructor(_, _) => Some(ColType::Tag),
        _ => None,
    }
}

/// Infer a TempSchema from a non-empty slice of values.
/// Returns None if the values contain unsupported types (Function, nested Relation).
fn infer_temp_schema(rows: &[*mut Value]) -> Option<TempSchema> {
    if rows.is_empty() {
        return Some(TempSchema::Unit);
    }
    let first = rows[0];
    if first.is_null() {
        return Some(TempSchema::Scalar(ColType::Text));
    }
    match unsafe { as_ref(first) } {
        Value::Record(fields) => {
            let mut cols = Vec::with_capacity(fields.len());
            for f in fields {
                match unsafe { as_ref(f.value) } {
                    Value::Relation(_) | Value::Function(_, _, _) => return None,
                    _ => {}
                }
                let ty = infer_col_type(f.value)?;
                cols.push((f.name.clone(), ty));
            }
            Some(TempSchema::Record(cols))
        }
        Value::Constructor(_, _) => {
            // Scan all rows to collect all constructor variants
            let mut ctors: Vec<(String, Vec<(String, ColType)>)> = Vec::new();
            let mut seen_tags: HashSet<&str> = HashSet::new();
            let mut seen_field_names: HashSet<String> = HashSet::new();
            let mut all_fields: Vec<(String, ColType)> = Vec::new();

            for row in rows {
                if row.is_null() { continue; }
                match unsafe { as_ref(*row) } {
                    Value::Constructor(tag, payload) => {
                        if !seen_tags.insert(tag.as_str()) {
                            continue;
                        }
                        let ctor_fields = match unsafe { as_ref(*payload) } {
                            Value::Unit => Vec::new(),
                            Value::Record(fields) => {
                                let mut cf = Vec::new();
                                for f in fields {
                                    let ty = infer_col_type(f.value)?;
                                    cf.push((f.name.clone(), ty));
                                    if seen_field_names.insert(f.name.clone()) {
                                        all_fields.push((f.name.clone(), ty));
                                    }
                                }
                                cf
                            }
                            _ => return None,
                        };
                        ctors.push((tag.clone(), ctor_fields));
                    }
                    _ => return None,
                }
            }
            Some(TempSchema::Adt { constructors: ctors, all_fields })
        }
        Value::Unit => Some(TempSchema::Unit),
        Value::Int(_) => Some(TempSchema::Scalar(ColType::Int)),
        Value::Float(_) => Some(TempSchema::Scalar(ColType::Float)),
        Value::Text(_) => Some(TempSchema::Scalar(ColType::Text)),
        Value::Bool(_) => Some(TempSchema::Scalar(ColType::Bool)),
        Value::Bytes(_) => Some(TempSchema::Scalar(ColType::Bytes)),
        _ => None,
    }
}

/// Create a temp table with the given schema.
fn create_temp_table(conn: &Connection, name: &str, schema: &TempSchema) {
    let table = quote_ident(name);
    let col_defs = match schema {
        TempSchema::Record(cols) => {
            if cols.is_empty() {
                "\"_dummy\" INTEGER DEFAULT 0".to_string()
            } else {
                cols.iter()
                    .map(|(name, ty)| format!("{} {}", quote_ident(name), sql_type(*ty)))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        }
        TempSchema::Scalar(ty) => format!("\"_val\" {}", sql_type(*ty)),
        TempSchema::Adt { all_fields, .. } => {
            let mut defs = vec!["\"_tag\" TEXT NOT NULL".to_string()];
            for (name, ty) in all_fields {
                defs.push(format!("{} {}", quote_ident(name), sql_type(*ty)));
            }
            defs.join(", ")
        }
        TempSchema::Unit => "\"_dummy\" INTEGER DEFAULT 0".to_string(),
    };
    let sql = format!("CREATE TEMP TABLE {} ({});", table, col_defs);
    debug_sql(&sql);
    conn.execute_batch(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: failed to create temp table: {}", e));
}

/// Build an INSERT SQL statement for a temp table.
fn temp_insert_sql(name: &str, schema: &TempSchema) -> String {
    let table = quote_ident(name);
    let (col_names, n_cols) = match schema {
        TempSchema::Record(cols) => {
            if cols.is_empty() {
                ("\"_dummy\"".to_string(), 1)
            } else {
                let names: Vec<String> = cols.iter().map(|(n, _)| quote_ident(n)).collect();
                let n = names.len();
                (names.join(", "), n)
            }
        }
        TempSchema::Scalar(_) => ("\"_val\"".to_string(), 1),
        TempSchema::Adt { all_fields, .. } => {
            let mut names = vec!["\"_tag\"".to_string()];
            for (n, _) in all_fields {
                names.push(quote_ident(n));
            }
            let n = names.len();
            (names.join(", "), n)
        }
        TempSchema::Unit => ("\"_dummy\"".to_string(), 1),
    };
    let placeholders: Vec<String> = (1..=n_cols).map(|i| format!("?{}", i)).collect();
    format!("INSERT INTO {} ({}) VALUES ({});", table, col_names, placeholders.join(", "))
}

/// Convert a Value to SQL params for temp table insertion.
fn temp_row_to_params(v: *mut Value, schema: &TempSchema) -> Vec<rusqlite::types::Value> {
    match schema {
        TempSchema::Record(cols) => {
            if cols.is_empty() {
                return vec![rusqlite::types::Value::Integer(0)];
            }
            let fields = match unsafe { as_ref(v) } {
                Value::Record(fields) => fields,
                _ => panic!("knot runtime: expected Record for temp table insert, got {}", type_name(v)),
            };
            cols.iter()
                .map(|(name, ty)| {
                    let field = fields.iter().find(|f| f.name == *name);
                    match field {
                        Some(f) => value_to_sqlite(f.value, *ty),
                        None => rusqlite::types::Value::Null,
                    }
                })
                .collect()
        }
        TempSchema::Scalar(ty) => vec![value_to_sqlite(v, *ty)],
        TempSchema::Adt { all_fields, constructors } => {
            match unsafe { as_ref(v) } {
                Value::Constructor(tag, payload) => {
                    let mut params = vec![rusqlite::types::Value::Text(tag.clone())];
                    let ctor = constructors.iter().find(|(t, _)| t == tag);
                    for (fname, fty) in all_fields {
                        let has_field = ctor.map_or(false, |(_, fields)| {
                            fields.iter().any(|(n, _)| n == fname)
                        });
                        if has_field {
                            match unsafe { as_ref(*payload) } {
                                Value::Record(fields) => {
                                    let field = fields.iter().find(|f| f.name == *fname);
                                    params.push(match field {
                                        Some(f) => value_to_sqlite(f.value, *fty),
                                        None => rusqlite::types::Value::Null,
                                    });
                                }
                                _ => params.push(rusqlite::types::Value::Null),
                            }
                        } else {
                            params.push(rusqlite::types::Value::Null);
                        }
                    }
                    params
                }
                _ => panic!("knot runtime: expected Constructor for ADT temp table"),
            }
        }
        TempSchema::Unit => vec![rusqlite::types::Value::Integer(0)],
    }
}

/// Read a single row from a query result and convert to a Value using TempSchema.
fn read_temp_row(row: &rusqlite::Row, schema: &TempSchema) -> *mut Value {
    match schema {
        TempSchema::Record(cols) => {
            if cols.is_empty() {
                return knot_record_empty(0);
            }
            let record = knot_record_empty(cols.len());
            for (i, (name, ty)) in cols.iter().enumerate() {
                let val = read_sql_column(row, i, *ty);
                let name_bytes = name.as_bytes();
                knot_record_set_field(record, name_bytes.as_ptr(), name_bytes.len(), val);
            }
            record
        }
        TempSchema::Scalar(ty) => read_sql_column(row, 0, *ty),
        TempSchema::Adt { constructors, all_fields } => {
            let tag: String = row.get(0).unwrap();
            let ctor = constructors.iter().find(|(t, _)| t == &tag);
            let payload = if let Some((_, fields)) = ctor {
                if fields.is_empty() {
                    alloc(Value::Unit)
                } else {
                    let field_idx: HashMap<&str, usize> = all_fields.iter().enumerate()
                        .map(|(i, (n, _))| (n.as_str(), i)).collect();
                    let record = knot_record_empty(fields.len());
                    for (fname, fty) in fields {
                        let col_idx = field_idx[fname.as_str()];
                        let val = read_sql_column(row, col_idx + 1, *fty);
                        let name_bytes = fname.as_bytes();
                        knot_record_set_field(record, name_bytes.as_ptr(), name_bytes.len(), val);
                    }
                    record
                }
            } else {
                alloc(Value::Unit)
            };
            alloc(Value::Constructor(tag, payload))
        }
        TempSchema::Unit => alloc(Value::Unit),
    }
}

/// Read rows from an arbitrary SQL query using a TempSchema.
fn read_query_rows(conn: &Connection, sql: &str, schema: &TempSchema) -> Vec<*mut Value> {
    debug_sql(sql);
    let mut stmt = conn
        .prepare_cached(sql)
        .unwrap_or_else(|e| panic!("knot runtime: temp query error: {}\n  SQL: {}", e, sql));
    let mut result_rows = stmt
        .query([])
        .unwrap_or_else(|e| panic!("knot runtime: temp query exec error: {}\n  SQL: {}", e, sql));

    let mut rows: Vec<*mut Value> = Vec::new();
    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: temp query fetch error: {}", e))
    {
        rows.push(read_temp_row(row, schema));
    }
    rows
}

/// Drop a temp table.
fn drop_temp_table(conn: &Connection, name: &str) {
    let sql = format!("DROP TABLE IF EXISTS {};", quote_ident(name));
    debug_sql(&sql);
    let _ = conn.execute_batch(&sql);
}

/// Materialize a relation into a temp table and return the table name.
fn materialize_relation(conn: &Connection, rows: &[*mut Value], schema: &TempSchema) -> String {
    let name = next_temp_name();
    create_temp_table(conn, &name, schema);
    if !rows.is_empty() {
        let ins_sql = temp_insert_sql(&name, schema);
        debug_sql(&ins_sql);
        let mut stmt = conn
            .prepare_cached(&ins_sql)
            .unwrap_or_else(|e| panic!("knot runtime: temp insert prepare error: {}", e));
        for row in rows {
            let params = temp_row_to_params(*row, schema);
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
            stmt.execute(param_refs.as_slice())
                .unwrap_or_else(|e| panic!("knot runtime: temp table insert error: {}", e));
        }
    }
    name
}

/// In-memory dedup fallback for relations that can't be stored in SQL.
fn in_memory_dedup(rows: Vec<*mut Value>) -> Vec<*mut Value> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    let mut buf = Vec::new();
    for row in rows {
        buf.clear();
        value_to_hash_bytes(row, &mut buf);
        if !seen.contains(buf.as_slice()) {
            seen.insert(std::mem::take(&mut buf));
            result.push(row);
        }
    }
    result
}

/// Perform a set operation (UNION/EXCEPT/INTERSECT) using SQLite.
fn sql_set_op(
    conn: &Connection,
    a: &[*mut Value],
    b: &[*mut Value],
    op: &str,
) -> Option<Vec<*mut Value>> {
    let sample = if !a.is_empty() { a } else { b };
    if sample.is_empty() {
        return Some(Vec::new());
    }
    let schema = infer_temp_schema(sample)?;

    let t1 = materialize_relation(conn, a, &schema);
    let t2 = materialize_relation(conn, b, &schema);

    let sql = format!(
        "SELECT * FROM {} {} SELECT * FROM {}",
        quote_ident(&t1),
        op,
        quote_ident(&t2)
    );
    let result = read_query_rows(conn, &sql, &schema);

    drop_temp_table(conn, &t1);
    drop_temp_table(conn, &t2);

    Some(result)
}

/// Dedup a list of values using a SQL temp table with SELECT DISTINCT.
fn sql_dedup(conn: &Connection, rows: &[*mut Value]) -> Option<Vec<*mut Value>> {
    if rows.is_empty() {
        return Some(Vec::new());
    }
    let schema = infer_temp_schema(rows)?;
    let tmp = materialize_relation(conn, rows, &schema);
    let sql = format!("SELECT DISTINCT * FROM {}", quote_ident(&tmp));
    let result = read_query_rows(conn, &sql, &schema);
    drop_temp_table(conn, &tmp);
    Some(result)
}

/// Check if two relations are equal using SQL EXCEPT (symmetric difference).
fn sql_relations_equal(conn: &Connection, a: &[*mut Value], b: &[*mut Value]) -> Option<bool> {
    if a.len() != b.len() {
        return Some(false);
    }
    if a.is_empty() {
        return Some(true);
    }
    let schema = infer_temp_schema(a)?;

    let t1 = materialize_relation(conn, a, &schema);
    let t2 = materialize_relation(conn, b, &schema);

    // Check symmetric difference: (a EXCEPT b) UNION ALL (b EXCEPT a) should be empty
    let sql = format!(
        "SELECT 1 FROM (SELECT * FROM {} EXCEPT SELECT * FROM {} UNION ALL SELECT * FROM {} EXCEPT SELECT * FROM {}) LIMIT 1",
        quote_ident(&t1), quote_ident(&t2), quote_ident(&t2), quote_ident(&t1)
    );
    debug_sql(&sql);
    let has_diff = conn
        .prepare_cached(&sql)
        .and_then(|mut s| s.query_row([], |_| Ok(true)))
        .unwrap_or(false);

    drop_temp_table(conn, &t1);
    drop_temp_table(conn, &t2);

    Some(!has_diff)
}

// ── Relation operations ───────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_empty() -> *mut Value {
    alloc(Value::Relation(Vec::new()))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_singleton(v: *mut Value) -> *mut Value {
    alloc(Value::Relation(vec![v]))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_push(rel: *mut Value, row: *mut Value) {
    let r = unsafe { &mut *rel };
    match r {
        Value::Relation(rows) => rows.push(row),
        _ => panic!("knot runtime: expected Relation in push, got {}", type_name(rel)),
    }
}

/// If the value is already a relation, return it as-is.
/// Otherwise, wrap it in a singleton relation.
/// Null (nullable none) wraps as a singleton containing null.
#[unsafe(no_mangle)]
pub extern "C" fn knot_ensure_relation(v: *mut Value) -> *mut Value {
    if v.is_null() {
        return alloc(Value::Relation(vec![v]));
    }
    match unsafe { as_ref(v) } {
        Value::Relation(_) => v,
        _ => alloc(Value::Relation(vec![v])),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_len(rel: *mut Value) -> usize {
    match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows.len(),
        _ => panic!("knot runtime: expected Relation in len, got {}", type_name(rel)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_get(rel: *mut Value, index: usize) -> *mut Value {
    match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows[index],
        _ => panic!("knot runtime: expected Relation in get, got {}", type_name(rel)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_union(
    db: *mut c_void,
    a: *mut Value,
    b: *mut Value,
) -> *mut Value {
    let rows_a = match unsafe { as_ref(a) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: expected Relation in union, got {}", type_name(a)),
    };
    let rows_b = match unsafe { as_ref(b) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: expected Relation in union, got {}", type_name(b)),
    };

    if rows_a.is_empty() { return b; }
    if rows_b.is_empty() { return a; }

    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_set_op(&db_ref.conn, rows_a, rows_b, "UNION") {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory hash-based dedup
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    let mut buf = Vec::new();
    for &row in rows_a.iter().chain(rows_b.iter()) {
        buf.clear();
        value_to_hash_bytes(row, &mut buf);
        if !seen.contains(buf.as_slice()) {
            seen.insert(buf.clone());
            result.push(row);
        }
    }
    alloc(Value::Relation(result))
}

/// Monadic bind for relations: iterate `rel`, call `func` on each element,
/// union all resulting relations into one.
/// Signature: (db, func, rel) -> rel
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_bind(
    db: *mut c_void,
    func: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: expected Relation in bind, got {}",
            type_name(rel)
        ),
    };

    if rows.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    // Collect all sub-relation rows
    let mut all_rows: Vec<*mut Value> = Vec::new();
    for &row in rows {
        let sub = knot_value_call(db, func, row);
        match unsafe { as_ref(sub) } {
            Value::Relation(sub_rows) => {
                all_rows.extend_from_slice(sub_rows);
            }
            _ => panic!(
                "knot runtime: bind function must return a Relation, got {}",
                type_name(sub)
            ),
        }
    }

    if all_rows.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    // Dedup via SQLite
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_dedup(&db_ref.conn, &all_rows) {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory dedup
    alloc(Value::Relation(in_memory_dedup(all_rows)))
}

/// Group a relation by key columns using SQLite ORDER BY for efficient grouping.
/// Inserts key columns + row indices into a temp table, sorts via ORDER BY,
/// then groups consecutive rows with matching keys in O(n).
/// Signature: (db, rel, schema_ptr, schema_len, key_cols_ptr, key_cols_len) -> [[row]]
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_group_by(
    db: *mut c_void,
    rel: *mut Value,
    schema_ptr: *const u8,
    schema_len: usize,
    key_cols_ptr: *const u8,
    key_cols_len: usize,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: expected Relation in group_by, got {}",
            type_name(rel)
        ),
    };

    if rows.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let schema_str = unsafe { str_from_raw(schema_ptr, schema_len) };
    let key_cols_str = unsafe { str_from_raw(key_cols_ptr, key_cols_len) };
    let schema = parse_record_schema(schema_str);
    let key_col_names: Vec<&str> = key_cols_str.split(',').collect();

    // Find key column specs in the schema via HashMap lookup
    let col_map: HashMap<&str, &ColumnSpec> = schema.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let key_specs: Vec<&ColumnSpec> = key_col_names
        .iter()
        .map(|kc| {
            *col_map.get(kc).unwrap_or_else(|| {
                panic!(
                    "knot runtime: key column '{}' not found in schema",
                    kc
                )
            })
        })
        .collect();

    let temp_name = "_knot_tmp_groupby";
    let temp = quote_ident(temp_name);

    // Drop any leftover temp table
    let _ = db_ref
        .conn
        .execute_batch(&format!("DROP TABLE IF EXISTS {};", temp));

    // Create temp table: _idx INTEGER + key columns only
    let mut col_defs = vec!["_idx INTEGER".to_string()];
    for ks in &key_specs {
        col_defs.push(format!("{} {}", quote_ident(&ks.name), sql_type(ks.ty)));
    }
    let create_sql = format!(
        "CREATE TEMP TABLE {} ({});",
        temp,
        col_defs.join(", ")
    );
    debug_sql(&create_sql);
    db_ref
        .conn
        .execute_batch(&create_sql)
        .expect("knot runtime: failed to create groupby temp table");

    // Insert row indices + key column values
    let mut insert_col_names = vec!["\"_idx\"".to_string()];
    for ks in &key_specs {
        insert_col_names.push(quote_ident(&ks.name));
    }
    let placeholders: Vec<String> = (1..=insert_col_names.len())
        .map(|i| format!("?{}", i))
        .collect();
    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({});",
        temp,
        insert_col_names.join(", "),
        placeholders.join(", ")
    );
    debug_sql(&insert_sql);

    {
        let mut insert_stmt = db_ref
            .conn
            .prepare_cached(&insert_sql)
            .expect("knot runtime: failed to prepare groupby insert");

        for (idx, row_ptr) in rows.iter().enumerate() {
            let fields = match unsafe { as_ref(*row_ptr) } {
                Value::Record(fields) => fields,
                _ => panic!("knot runtime: groupby rows must be Records"),
            };

            let mut params: Vec<rusqlite::types::Value> =
                vec![rusqlite::types::Value::Integer(idx as i64)];
            for ks in &key_specs {
                let value = fields.iter().find(|f| f.name == ks.name)
                    .unwrap_or_else(|| {
                        panic!(
                            "knot runtime: missing field '{}' in record",
                            ks.name
                        )
                    });
                params.push(value_to_sqlite(value.value, ks.ty));
            }

            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
            insert_stmt
                .execute(param_refs.as_slice())
                .expect("knot runtime: groupby insert error");
        }
    } // insert_stmt dropped here

    // SELECT with ORDER BY key columns; group consecutive rows
    let order_cols: Vec<String> = key_specs
        .iter()
        .map(|ks| quote_ident(&ks.name))
        .collect();
    let select_sql = format!(
        "SELECT {} FROM {} ORDER BY {}",
        insert_col_names.join(", "),
        temp,
        order_cols.join(", ")
    );
    debug_sql(&select_sql);

    let groups = {
        let mut select_stmt = db_ref
            .conn
            .prepare_cached(&select_sql)
            .expect("knot runtime: failed to prepare groupby select");
        let mut result_rows = select_stmt
            .query([])
            .expect("knot runtime: groupby select error");

        let mut groups: Vec<Vec<*mut Value>> = Vec::new();
        let mut current_group: Vec<*mut Value> = Vec::new();
        let mut prev_keys: Option<Vec<rusqlite::types::Value>> = None;

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: groupby fetch error: {}", e))
        {
            let idx: i64 = row.get(0).unwrap();

            // Extract key values for comparison
            let keys: Vec<rusqlite::types::Value> = (1..=key_specs.len())
                .map(|i| row.get(i).unwrap())
                .collect();

            // Detect group boundary
            if let Some(ref prev) = prev_keys {
                if keys != *prev {
                    groups.push(std::mem::take(&mut current_group));
                }
            }

            current_group.push(rows[idx as usize]);
            prev_keys = Some(keys);
        }

        if !current_group.is_empty() {
            groups.push(current_group);
        }

        groups
    }; // select_stmt + result_rows dropped here

    // Clean up temp table
    let _ = db_ref
        .conn
        .execute_batch(&format!("DROP TABLE IF EXISTS {};", temp));

    // Convert to a relation of relations
    let result: Vec<*mut Value> = groups
        .into_iter()
        .map(|rows| alloc(Value::Relation(rows)))
        .collect();

    alloc(Value::Relation(result))
}

// ── Value equality ────────────────────────────────────────────────

/// Recursively serialize a Value to bytes for hash-based set comparison.
fn value_to_hash_bytes(v: *mut Value, buf: &mut Vec<u8>) {
    if v.is_null() {
        buf.push(0xFF);
        return;
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => {
            buf.push(0);
            let bytes = n.to_signed_bytes_le();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&bytes);
        }
        Value::Float(f) => {
            buf.push(1);
            buf.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(2);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Bool(b) => {
            buf.push(3);
            buf.push(*b as u8);
        }
        Value::Bytes(b) => {
            buf.push(4);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Unit => {
            buf.push(5);
        }
        Value::Record(fields) => {
            buf.push(6);
            buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for field in fields {
                buf.extend_from_slice(&(field.name.len() as u32).to_le_bytes());
                buf.extend_from_slice(field.name.as_bytes());
                value_to_hash_bytes(field.value, buf);
            }
        }
        Value::Constructor(tag, payload) => {
            buf.push(7);
            buf.extend_from_slice(&(tag.len() as u32).to_le_bytes());
            buf.extend_from_slice(tag.as_bytes());
            value_to_hash_bytes(*payload, buf);
        }
        Value::Relation(rows) => {
            buf.push(8);
            buf.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            // Sort serialized rows for order-independent comparison
            let mut row_bytes: Vec<Vec<u8>> = rows
                .iter()
                .map(|r| {
                    let mut rb = Vec::new();
                    value_to_hash_bytes(*r, &mut rb);
                    rb
                })
                .collect();
            row_bytes.sort_unstable();
            for rb in &row_bytes {
                buf.extend_from_slice(&(rb.len() as u32).to_le_bytes());
                buf.extend_from_slice(rb);
            }
        }
        Value::Function(_, _, src) => {
            buf.push(9);
            buf.extend_from_slice(src.as_bytes());
        }
    }
}



fn values_equal(a: *mut Value, b: *mut Value) -> bool {
    if a == b {
        return true;
    }
    // Nullable encoding: null represents the "none" variant
    if a.is_null() || b.is_null() {
        return false; // a == b already handled both-null
    }
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x == y,
        (Value::Text(x), Value::Text(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Bytes(x), Value::Bytes(y)) => x == y,
        (Value::Unit, Value::Unit) => true,
        (Value::Record(fa), Value::Record(fb)) => {
            if fa.len() != fb.len() {
                return false;
            }
            // Fields are sorted by name — linear comparison
            fa.iter().zip(fb.iter()).all(|(a, b)| {
                a.name == b.name && values_equal(a.value, b.value)
            })
        }
        (Value::Constructor(ta, pa), Value::Constructor(tb, pb)) => {
            ta == tb && values_equal(*pa, *pb)
        }
        (Value::Relation(ra), Value::Relation(rb)) => {
            if ra.len() != rb.len() {
                return false;
            }
            let set_a: HashSet<Vec<u8>> = ra.iter().map(|r| {
                let mut buf = Vec::new();
                value_to_hash_bytes(*r, &mut buf);
                buf
            }).collect();
            let mut buf = Vec::new();
            rb.iter().all(|r| {
                buf.clear();
                value_to_hash_bytes(*r, &mut buf);
                set_a.contains(buf.as_slice())
            })
        }
        _ => false,
    }
}

// ── Binary operations ─────────────────────────────────────────────

fn bigint_to_f64(n: &BigInt) -> f64 {
    n.to_f64().unwrap_or(if n.sign() == num_bigint::Sign::Minus { f64::NEG_INFINITY } else { f64::INFINITY })
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_add(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => alloc(Value::Int(x + y)),
        (Value::Float(x), Value::Float(y)) => alloc(Value::Float(x + y)),
        (Value::Int(x), Value::Float(y)) => alloc(Value::Float(bigint_to_f64(x) + y)),
        (Value::Float(x), Value::Int(y)) => alloc(Value::Float(x + bigint_to_f64(y))),
        _ => panic!("knot runtime: cannot add {} + {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_sub(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => alloc(Value::Int(x - y)),
        (Value::Float(x), Value::Float(y)) => alloc(Value::Float(x - y)),
        (Value::Int(x), Value::Float(y)) => alloc(Value::Float(bigint_to_f64(x) - y)),
        (Value::Float(x), Value::Int(y)) => alloc(Value::Float(x - bigint_to_f64(y))),
        _ => panic!("knot runtime: cannot subtract {} - {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_mul(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => alloc(Value::Int(x * y)),
        (Value::Float(x), Value::Float(y)) => alloc(Value::Float(x * y)),
        (Value::Int(x), Value::Float(y)) => alloc(Value::Float(bigint_to_f64(x) * y)),
        (Value::Float(x), Value::Int(y)) => alloc(Value::Float(x * bigint_to_f64(y))),
        _ => panic!("knot runtime: cannot multiply {} * {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_div(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => {
            if y.is_zero() {
                panic!("knot runtime: division by zero");
            }
            alloc(Value::Int(x / y))
        }
        (Value::Float(x), Value::Float(y)) => alloc(Value::Float(x / y)),
        (Value::Int(x), Value::Float(y)) => alloc(Value::Float(bigint_to_f64(x) / y)),
        (Value::Float(x), Value::Int(y)) => alloc(Value::Float(x / bigint_to_f64(y))),
        _ => panic!("knot runtime: cannot divide {} / {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_eq(a: *mut Value, b: *mut Value) -> *mut Value {
    alloc(Value::Bool(values_equal(a, b)))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_neq(a: *mut Value, b: *mut Value) -> *mut Value {
    alloc(Value::Bool(!values_equal(a, b)))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_lt(a: *mut Value, b: *mut Value) -> *mut Value {
    let result = match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => x < y,
        (Value::Float(x), Value::Float(y)) => x < y,
        (Value::Int(x), Value::Float(y)) => bigint_to_f64(x) < *y,
        (Value::Float(x), Value::Int(y)) => *x < bigint_to_f64(y),
        (Value::Text(x), Value::Text(y)) => x < y,
        _ => panic!("knot runtime: cannot compare {} < {}", type_name(a), type_name(b)),
    };
    alloc(Value::Bool(result))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_gt(a: *mut Value, b: *mut Value) -> *mut Value {
    let result = match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => x > y,
        (Value::Float(x), Value::Float(y)) => x > y,
        (Value::Int(x), Value::Float(y)) => bigint_to_f64(x) > *y,
        (Value::Float(x), Value::Int(y)) => *x > bigint_to_f64(y),
        (Value::Text(x), Value::Text(y)) => x > y,
        _ => panic!("knot runtime: cannot compare {} > {}", type_name(a), type_name(b)),
    };
    alloc(Value::Bool(result))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_le(a: *mut Value, b: *mut Value) -> *mut Value {
    let result = match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => x <= y,
        (Value::Float(x), Value::Float(y)) => x <= y,
        (Value::Int(x), Value::Float(y)) => bigint_to_f64(x) <= *y,
        (Value::Float(x), Value::Int(y)) => *x <= bigint_to_f64(y),
        (Value::Text(x), Value::Text(y)) => x <= y,
        _ => panic!("knot runtime: cannot compare {} <= {}", type_name(a), type_name(b)),
    };
    alloc(Value::Bool(result))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_ge(a: *mut Value, b: *mut Value) -> *mut Value {
    let result = match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => x >= y,
        (Value::Float(x), Value::Float(y)) => x >= y,
        (Value::Int(x), Value::Float(y)) => bigint_to_f64(x) >= *y,
        (Value::Float(x), Value::Int(y)) => *x >= bigint_to_f64(y),
        (Value::Text(x), Value::Text(y)) => x >= y,
        _ => panic!("knot runtime: cannot compare {} >= {}", type_name(a), type_name(b)),
    };
    alloc(Value::Bool(result))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_and(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Bool(x), Value::Bool(y)) => alloc(Value::Bool(*x && *y)),
        _ => panic!("knot runtime: && requires Bool operands, got {} && {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_or(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Bool(x), Value::Bool(y)) => alloc(Value::Bool(*x || *y)),
        _ => panic!("knot runtime: || requires Bool operands, got {} || {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_concat(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Text(x), Value::Text(y)) => {
            let mut s = x.clone();
            s.push_str(y);
            alloc(Value::Text(s))
        }
        (Value::Relation(rows_a), Value::Relation(rows_b)) => {
            // ++ on relations is union (in-memory hash-based dedup)
            let mut seen = HashSet::new();
            let mut result = Vec::new();
            let mut buf = Vec::new();
            for &row in rows_a.iter().chain(rows_b.iter()) {
                buf.clear();
                value_to_hash_bytes(row, &mut buf);
                if !seen.contains(buf.as_slice()) {
                    seen.insert(std::mem::take(&mut buf));
                    result.push(row);
                }
            }
            alloc(Value::Relation(result))
        }
        _ => panic!("knot runtime: ++ requires Text or Relation operands, got {} ++ {}", type_name(a), type_name(b)),
    }
}

// ── Comparison (returns Ordering ADT) ─────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_compare(a: *mut Value, b: *mut Value) -> *mut Value {
    let ordering = match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Int(x), Value::Float(y)) => {
            bigint_to_f64(x).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Float(x), Value::Int(y)) => {
            x.partial_cmp(&bigint_to_f64(y)).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        _ => panic!(
            "knot runtime: cannot compare {} with {}",
            type_name(a),
            type_name(b)
        ),
    };
    let tag = match ordering {
        std::cmp::Ordering::Less => "LT",
        std::cmp::Ordering::Equal => "EQ",
        std::cmp::Ordering::Greater => "GT",
    };
    alloc(Value::Constructor(
        tag.to_string(),
        alloc(Value::Unit),
    ))
}

// ── Unary operations ──────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_negate(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Int(n) => alloc(Value::Int(-n)),
        Value::Float(n) => alloc(Value::Float(-n)),
        _ => panic!("knot runtime: cannot negate {}", type_name(v)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_not(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Bool(b) => alloc(Value::Bool(!b)),
        _ => panic!("knot runtime: 'not' requires Bool, got {}", type_name(v)),
    }
}

// ── Function calls ────────────────────────────────────────────────

/// Call a function value: fn_ptr(db, env, arg) -> result
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_call(
    db: *mut c_void,
    func: *mut Value,
    arg: *mut Value,
) -> *mut Value {
    match unsafe { as_ref(func) } {
        Value::Function(fn_ptr, env, _) => {
            let f: extern "C" fn(*mut c_void, *mut Value, *mut Value) -> *mut Value =
                unsafe { std::mem::transmute(*fn_ptr) };
            f(db, *env, arg)
        }
        _ => panic!("knot runtime: cannot call {}, expected Function", brief_value(func)),
    }
}

// ── Printing ──────────────────────────────────────────────────────

fn format_value(v: *mut Value) -> String {
    if v.is_null() {
        return "null".to_string();
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => n.to_string(),
        Value::Float(n) => {
            if *n == (*n as i64) as f64 {
                format!("{:.1}", n)
            } else {
                n.to_string()
            }
        }
        Value::Text(s) => format!("\"{}\"", s),
        Value::Bytes(b) => {
            let mut hex = String::with_capacity(b.len() * 2 + 3);
            hex.push_str("b\"");
            for byte in b {
                use std::fmt::Write;
                let _ = write!(hex, "{:02x}", byte);
            }
            hex.push('"');
            hex
        }
        Value::Bool(b) => {
            if *b {
                "True {}".to_string()
            } else {
                "False {}".to_string()
            }
        }
        Value::Unit => "{}".to_string(),
        Value::Record(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name, format_value(f.value)))
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
        Value::Relation(rows) => {
            let inner: Vec<String> = rows.iter().map(|r| format_value(*r)).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Constructor(tag, payload) => {
            let p = format_value(*payload);
            if p == "{}" {
                format!("{} {{}}", tag)
            } else {
                format!("{} {}", tag, p)
            }
        }
        Value::Function(_, _, src) => src.clone(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_read_line() -> *mut Value {
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .expect("knot runtime: failed to read from stdin");
    // Strip trailing newline
    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }
    alloc(Value::Text(line))
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_print(v: *mut Value) -> *mut Value {
    print!("{}", format_value(v));
    alloc(Value::Unit)
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_println(v: *mut Value) -> *mut Value {
    println!("{}", format_value(v));
    alloc(Value::Unit)
}

/// Convert a value to its text representation (returned as a Value::Text).
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_show(v: *mut Value) -> *mut Value {
    fn show_inner(v: *mut Value) -> String {
        if v.is_null() {
            return "null".to_string();
        }
        match unsafe { as_ref(v) } {
            Value::Int(n) => n.to_string(),
            Value::Float(n) => {
                if *n == (*n as i64) as f64 {
                    format!("{:.1}", n)
                } else {
                    n.to_string()
                }
            }
            Value::Text(s) => s.clone(),
            Value::Bytes(b) => {
                let mut hex = String::with_capacity(b.len() * 2);
                for byte in b {
                    use std::fmt::Write;
                    let _ = write!(hex, "{:02x}", byte);
                }
                hex
            }
            Value::Bool(b) => {
                if *b { "True".to_string() } else { "False".to_string() }
            }
            Value::Unit => "{}".to_string(),
            Value::Record(fields) => {
                let inner: Vec<String> = fields
                    .iter()
                    .map(|f| format!("{}: {}", f.name, show_inner(f.value)))
                    .collect();
                format!("{{{}}}", inner.join(", "))
            }
            Value::Relation(rows) => {
                let inner: Vec<String> = rows.iter().map(|r| show_inner(*r)).collect();
                format!("[{}]", inner.join(", "))
            }
            Value::Constructor(tag, payload) => {
                let p = show_inner(*payload);
                if p == "{}" {
                    format!("{} {{}}", tag)
                } else {
                    format!("{} {}", tag, p)
                }
            }
            Value::Function(_, _, src) => src.clone(),
        }
    }
    alloc(Value::Text(show_inner(v)))
}

// ── Standard library: relation operations ─────────────────────────


/// filter(pred, rel) — keep rows where pred returns true
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_filter(
    db: *mut c_void,
    pred: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: filter expected Relation, got {}",
            type_name(rel)
        ),
    };
    let mut result: Vec<*mut Value> = Vec::new();
    for &row in rows {
        let v = knot_value_call(db, pred, row);
        match unsafe { as_ref(v) } {
            Value::Bool(true) => result.push(row),
            Value::Bool(false) => {}
            _ => panic!("knot runtime: filter predicate must return Bool"),
        }
    }
    alloc(Value::Relation(result))
}

/// match(ctor, rel) — filter relation to rows matching a constructor tag, extract payloads
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_match(
    ctor: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let tag = match unsafe { as_ref(ctor) } {
        Value::Constructor(t, _) => t.as_str(),
        _ => panic!(
            "knot runtime: match expected Constructor, got {}",
            type_name(ctor)
        ),
    };
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: match expected Relation, got {}",
            type_name(rel)
        ),
    };
    let mut result: Vec<*mut Value> = Vec::new();
    for &row in rows {
        match unsafe { as_ref(row) } {
            Value::Constructor(t, payload) if t == tag => {
                result.push(*payload);
            }
            _ => {}
        }
    }
    alloc(Value::Relation(result))
}

/// map(f, rel) — apply f to each row, collect results (deduplicating)
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_map(
    db: *mut c_void,
    func: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: map expected Relation, got {}",
            type_name(rel)
        ),
    };

    if rows.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    // Apply function to all rows
    let mapped: Vec<*mut Value> = rows.iter().map(|&r| knot_value_call(db, func, r)).collect();

    // Dedup via SQLite
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_dedup(&db_ref.conn, &mapped) {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory dedup
    alloc(Value::Relation(in_memory_dedup(mapped)))
}

/// ap(fs, xs) — applicative apply: apply each function in fs to each value in xs
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_ap(
    db: *mut c_void,
    fs: *mut Value,
    xs: *mut Value,
) -> *mut Value {
    let funcs = match unsafe { as_ref(fs) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: ap expected Relation of functions, got {}",
            type_name(fs)
        ),
    };
    let vals = match unsafe { as_ref(xs) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: ap expected Relation of values, got {}",
            type_name(xs)
        ),
    };

    if funcs.is_empty() || vals.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    // Apply all function-value pairs
    let mut all: Vec<*mut Value> = Vec::with_capacity(funcs.len() * vals.len());
    for &f in funcs {
        for &x in vals {
            all.push(knot_value_call(db, f, x));
        }
    }

    // Dedup via SQLite
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_dedup(&db_ref.conn, &all) {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory dedup
    alloc(Value::Relation(in_memory_dedup(all)))
}

/// fold(f, init, rel) — left fold over a relation
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_fold(
    db: *mut c_void,
    func: *mut Value,
    init: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: fold expected Relation, got {}",
            type_name(rel)
        ),
    };
    let mut acc = init;
    for &row in rows {
        // func is curried: func(acc) returns a function, then that function(row) returns new acc
        let partial = knot_value_call(db, func, acc);
        acc = knot_value_call(db, partial, row);
    }
    acc
}

/// single(rel) — extract the single element from a one-element relation.
/// Returns `Just {value: x}` for a singleton, `Nothing {}` otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_single(rel: *mut Value) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: single expected Relation, got {}",
            type_name(rel)
        ),
    };
    if rows.len() == 1 {
        let record = alloc(Value::Record(vec![RecordField { name: "value".into(), value: rows[0] }]));
        alloc(Value::Constructor("Just".into(), record))
    } else {
        alloc(Value::Constructor("Nothing".into(), alloc(Value::Unit)))
    }
}

// ── Standard library: derived relation operations ────────────────

/// diff(a, b) — rows in a but not in b
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_diff(
    db: *mut c_void,
    a: *mut Value,
    b: *mut Value,
) -> *mut Value {
    let rows_a = match unsafe { as_ref(a) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: diff expected Relation, got {}", type_name(a)),
    };
    let rows_b = match unsafe { as_ref(b) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: diff expected Relation, got {}", type_name(b)),
    };

    if rows_a.is_empty() || rows_b.is_empty() { return a; }

    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_set_op(&db_ref.conn, rows_a, rows_b, "EXCEPT") {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory — hash-based O(n)
    let set_b: HashSet<Vec<u8>> = rows_b.iter().map(|r| {
        let mut buf = Vec::new();
        value_to_hash_bytes(*r, &mut buf);
        buf
    }).collect();
    let mut buf = Vec::new();
    let result: Vec<*mut Value> = rows_a
        .iter()
        .copied()
        .filter(|r| {
            buf.clear();
            value_to_hash_bytes(*r, &mut buf);
            !set_b.contains(buf.as_slice())
        })
        .collect();
    alloc(Value::Relation(result))
}

/// inter(a, b) — rows in both a and b
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_inter(
    db: *mut c_void,
    a: *mut Value,
    b: *mut Value,
) -> *mut Value {
    let rows_a = match unsafe { as_ref(a) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: inter expected Relation, got {}",
            type_name(a)
        ),
    };
    let rows_b = match unsafe { as_ref(b) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: inter expected Relation, got {}",
            type_name(b)
        ),
    };

    if rows_a.is_empty() || rows_b.is_empty() {
        return alloc(Value::Relation(Vec::new()));
    }

    let db_ref = unsafe { &*(db as *mut KnotDb) };
    if let Some(result) = sql_set_op(&db_ref.conn, rows_a, rows_b, "INTERSECT") {
        return alloc(Value::Relation(result));
    }

    // Fallback: in-memory — hash-based O(n)
    let set_b: HashSet<Vec<u8>> = rows_b.iter().map(|r| {
        let mut buf = Vec::new();
        value_to_hash_bytes(*r, &mut buf);
        buf
    }).collect();
    let mut buf = Vec::new();
    let result: Vec<*mut Value> = rows_a
        .iter()
        .copied()
        .filter(|r| {
            buf.clear();
            value_to_hash_bytes(*r, &mut buf);
            set_b.contains(buf.as_slice())
        })
        .collect();
    alloc(Value::Relation(result))
}

/// sum(f, rel) — sum of f(x) for each x in rel
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_sum(
    db: *mut c_void,
    f: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: sum expected Relation, got {}",
            type_name(rel)
        ),
    };
    let mut acc = alloc(Value::Int(BigInt::ZERO));
    for &row in rows {
        let val = knot_value_call(db, f, row);
        acc = knot_value_add(acc, val);
    }
    acc
}

/// avg(f, rel) — average of f(x) for each x in rel (returns Float)
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_avg(
    db: *mut c_void,
    f: *mut Value,
    rel: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: avg expected Relation, got {}",
            type_name(rel)
        ),
    };
    if rows.is_empty() {
        return alloc(Value::Float(0.0));
    }
    let mut total = 0.0f64;
    let count = rows.len();
    for &row in rows {
        let val = knot_value_call(db, f, row);
        match unsafe { as_ref(val) } {
            Value::Int(n) => total += bigint_to_f64(n),
            Value::Float(n) => total += n,
            _ => panic!(
                "knot runtime: avg projection must return Int or Float, got {}",
                type_name(val)
            ),
        }
    }
    alloc(Value::Float(total / count as f64))
}

// ── Standard library: text operations ─────────────────────────────

/// toUpper(text) — convert text to uppercase
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_to_upper(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => alloc(Value::Text(s.to_uppercase())),
        _ => panic!("knot runtime: toUpper expected Text, got {}", type_name(v)),
    }
}

/// toLower(text) — convert text to lowercase
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_to_lower(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => alloc(Value::Text(s.to_lowercase())),
        _ => panic!("knot runtime: toLower expected Text, got {}", type_name(v)),
    }
}

/// take(n, text) — first n characters
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_take(n: *mut Value, text: *mut Value) -> *mut Value {
    let n = match unsafe { as_ref(n) } {
        Value::Int(n) => n.to_usize().expect("knot runtime: take index out of range"),
        _ => panic!("knot runtime: take expected Int as first arg, got {}", type_name(n)),
    };
    match unsafe { as_ref(text) } {
        Value::Text(s) => {
            let result: String = s.chars().take(n).collect();
            alloc(Value::Text(result))
        }
        _ => panic!("knot runtime: take expected Text as second arg, got {}", type_name(text)),
    }
}

/// drop(n, text) — skip first n characters
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_drop(n: *mut Value, text: *mut Value) -> *mut Value {
    let n = match unsafe { as_ref(n) } {
        Value::Int(n) => n.to_usize().expect("knot runtime: drop index out of range"),
        _ => panic!("knot runtime: drop expected Int as first arg, got {}", type_name(n)),
    };
    match unsafe { as_ref(text) } {
        Value::Text(s) => {
            let result: String = s.chars().skip(n).collect();
            alloc(Value::Text(result))
        }
        _ => panic!("knot runtime: drop expected Text as second arg, got {}", type_name(text)),
    }
}

/// length(text) — character count of a text value
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_length(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => knot_value_int(s.chars().count() as i64),
        _ => panic!("knot runtime: length expected Text, got {}", type_name(v)),
    }
}

/// trim(text) — strip leading and trailing whitespace
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_trim(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => alloc(Value::Text(s.trim().to_string())),
        _ => panic!("knot runtime: trim expected Text, got {}", type_name(v)),
    }
}

/// contains(needle, haystack) — check if text contains a substring
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_contains(needle: *mut Value, haystack: *mut Value) -> *mut Value {
    let needle = match unsafe { as_ref(needle) } {
        Value::Text(s) => s.as_str(),
        _ => panic!("knot runtime: contains expected Text as first arg"),
    };
    match unsafe { as_ref(haystack) } {
        Value::Text(s) => alloc(Value::Bool(s.contains(needle))),
        _ => panic!("knot runtime: contains expected Text as second arg"),
    }
}

/// reverse(text) — reverse a text value
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_reverse(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => alloc(Value::Text(s.chars().rev().collect())),
        _ => panic!("knot runtime: reverse expected Text, got {}", type_name(v)),
    }
}

/// chars(text) — convert text to a relation of single characters
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_chars(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => {
            let rows: Vec<*mut Value> = s
                .chars()
                .map(|c| alloc(Value::Text(c.to_string())))
                .collect();
            alloc(Value::Relation(rows))
        }
        _ => panic!("knot runtime: chars expected Text, got {}", type_name(v)),
    }
}

// ── Standard library: bytes operations ─────────────────────────

/// bytesLength(bytes) — byte count
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_length(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Bytes(b) => knot_value_int(b.len() as i64),
        _ => panic!("knot runtime: bytesLength expected Bytes, got {}", type_name(v)),
    }
}

/// bytesConcat(a, b) — concatenate two byte strings
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_concat(a: *mut Value, b: *mut Value) -> *mut Value {
    let a_bytes = match unsafe { as_ref(a) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: bytesConcat expected Bytes as first arg, got {}", type_name(a)),
    };
    let b_bytes = match unsafe { as_ref(b) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: bytesConcat expected Bytes as second arg, got {}", type_name(b)),
    };
    let mut result = a_bytes.clone();
    result.extend_from_slice(b_bytes);
    alloc(Value::Bytes(result))
}

/// bytesSlice(start, len, bytes) — extract a sub-range of bytes
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_slice(
    _db: *mut c_void,
    start: *mut Value,
    len: *mut Value,
    bytes: *mut Value,
) -> *mut Value {
    let start = match unsafe { as_ref(start) } {
        Value::Int(n) => n.to_usize().expect("knot runtime: bytesSlice start out of range"),
        _ => panic!("knot runtime: bytesSlice expected Int as first arg"),
    };
    let len = match unsafe { as_ref(len) } {
        Value::Int(n) => n.to_usize().expect("knot runtime: bytesSlice len out of range"),
        _ => panic!("knot runtime: bytesSlice expected Int as second arg"),
    };
    match unsafe { as_ref(bytes) } {
        Value::Bytes(b) => {
            let end = (start + len).min(b.len());
            let s = start.min(b.len());
            alloc(Value::Bytes(b[s..end].to_vec()))
        }
        _ => panic!("knot runtime: bytesSlice expected Bytes as third arg, got {}", type_name(bytes)),
    }
}

/// textToBytes(text) — encode text as UTF-8 bytes
#[unsafe(no_mangle)]
pub extern "C" fn knot_text_to_bytes(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => alloc(Value::Bytes(s.as_bytes().to_vec())),
        _ => panic!("knot runtime: textToBytes expected Text, got {}", type_name(v)),
    }
}

/// bytesToText(bytes) — decode UTF-8 bytes to text
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_to_text(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Bytes(b) => {
            let s = String::from_utf8(b.clone())
                .unwrap_or_else(|e| panic!("knot runtime: bytesToText: invalid UTF-8: {}", e));
            alloc(Value::Text(s))
        }
        _ => panic!("knot runtime: bytesToText expected Bytes, got {}", type_name(v)),
    }
}

/// bytesToHex(bytes) — encode bytes as hex string
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_to_hex(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Bytes(b) => {
            let mut hex = String::with_capacity(b.len() * 2);
            for byte in b {
                use std::fmt::Write;
                let _ = write!(hex, "{:02x}", byte);
            }
            alloc(Value::Text(hex))
        }
        _ => panic!("knot runtime: bytesToHex expected Bytes, got {}", type_name(v)),
    }
}

/// bytesFromHex(text) — decode hex string to bytes
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_from_hex(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => {
            let s = s.trim();
            if s.len() % 2 != 0 {
                panic!("knot runtime: bytesFromHex: odd-length hex string");
            }
            let bytes: Vec<u8> = (0..s.len())
                .step_by(2)
                .map(|i| {
                    u8::from_str_radix(&s[i..i + 2], 16)
                        .unwrap_or_else(|_| panic!("knot runtime: bytesFromHex: invalid hex at position {}", i))
                })
                .collect();
            alloc(Value::Bytes(bytes))
        }
        _ => panic!("knot runtime: bytesFromHex expected Text, got {}", type_name(v)),
    }
}

/// bytesGet(index, bytes) — get byte at index as Int (0-255)
#[unsafe(no_mangle)]
pub extern "C" fn knot_bytes_get(index: *mut Value, bytes: *mut Value) -> *mut Value {
    let i = match unsafe { as_ref(index) } {
        Value::Int(n) => n.to_usize().expect("knot runtime: bytesGet index out of range"),
        _ => panic!("knot runtime: bytesGet expected Int as first arg"),
    };
    match unsafe { as_ref(bytes) } {
        Value::Bytes(b) => {
            if i >= b.len() {
                panic!("knot runtime: bytesGet index {} out of bounds (length {})", i, b.len());
            }
            knot_value_int(b[i] as i64)
        }
        _ => panic!("knot runtime: bytesGet expected Bytes as second arg, got {}", type_name(bytes)),
    }
}

// ── Standard library: JSON operations ─────────────────────────

/// toJson(value) — convert any Knot value to its JSON text representation
#[unsafe(no_mangle)]
pub extern "C" fn knot_json_encode(v: *mut Value) -> *mut Value {
    alloc(Value::Text(value_to_json(v)))
}

/// parseJson(text) — parse a JSON string into a Knot value
///
/// Mapping:
///   JSON object  → Record
///   JSON array   → Relation
///   JSON string  → Text
///   JSON number  → Int (if no decimal point) or Float
///   JSON boolean → Bool
///   JSON null    → Unit
#[unsafe(no_mangle)]
pub extern "C" fn knot_json_decode(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Text(s) => {
            let s = s.trim();
            let (val, _) = parse_json_value(s);
            val
        }
        _ => panic!("knot runtime: parseJson expected Text, got {}", type_name(v)),
    }
}

/// Recursive JSON parser. Returns (parsed_value, rest_of_input).
fn parse_json_value(s: &str) -> (*mut Value, &str) {
    let s = s.trim();
    if s.is_empty() {
        return (alloc(Value::Unit), s);
    }
    match s.as_bytes()[0] {
        b'"' => parse_json_string(s),
        b'{' => parse_json_obj(s),
        b'[' => parse_json_array(s),
        b't' if s.starts_with("true") => (alloc(Value::Bool(true)), &s[4..]),
        b'f' if s.starts_with("false") => (alloc(Value::Bool(false)), &s[5..]),
        b'n' if s.starts_with("null") => (alloc(Value::Unit), &s[4..]),
        _ => parse_json_number(s),
    }
}

fn parse_json_string(s: &str) -> (*mut Value, &str) {
    debug_assert!(s.starts_with('"'));
    let mut chars = s[1..].chars();
    let mut result = String::new();
    loop {
        match chars.next() {
            None => break,
            Some('"') => {
                let rest = chars.as_str();
                return (alloc(Value::Text(result)), rest);
            }
            Some('\\') => match chars.next() {
                Some('"') => result.push('"'),
                Some('\\') => result.push('\\'),
                Some('/') => result.push('/'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('u') => {
                    let hex: String = chars.by_ref().take(4).collect();
                    if let Ok(cp) = u32::from_str_radix(&hex, 16) {
                        if let Some(c) = char::from_u32(cp) {
                            result.push(c);
                        }
                    }
                }
                Some(c) => {
                    result.push('\\');
                    result.push(c);
                }
                None => break,
            },
            Some(c) => result.push(c),
        }
    }
    (alloc(Value::Text(result)), "")
}

fn parse_json_number(s: &str) -> (*mut Value, &str) {
    let mut end = 0;
    let mut is_float = false;
    let bytes = s.as_bytes();
    if end < bytes.len() && (bytes[end] == b'-' || bytes[end] == b'+') {
        end += 1;
    }
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end < bytes.len() && bytes[end] == b'.' {
        is_float = true;
        end += 1;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
    }
    if end < bytes.len() && (bytes[end] == b'e' || bytes[end] == b'E') {
        is_float = true;
        end += 1;
        if end < bytes.len() && (bytes[end] == b'-' || bytes[end] == b'+') {
            end += 1;
        }
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
    }
    let num_str = &s[..end];
    let rest = &s[end..];
    if is_float {
        let n: f64 = num_str.parse().unwrap_or(0.0);
        (alloc(Value::Float(n)), rest)
    } else {
        let n: BigInt = num_str.parse().unwrap_or(BigInt::ZERO);
        (alloc(Value::Int(n)), rest)
    }
}

fn parse_json_obj(s: &str) -> (*mut Value, &str) {
    debug_assert!(s.starts_with('{'));
    let mut rest = s[1..].trim();
    let mut fields: Vec<RecordField> = Vec::new();
    if rest.starts_with('}') {
        return (alloc(Value::Unit), &rest[1..]);
    }
    loop {
        // Parse key (must be a string)
        if !rest.starts_with('"') {
            break;
        }
        let (key_val, after_key) = parse_json_string(rest);
        let key = match unsafe { as_ref(key_val) } {
            Value::Text(s) => s.clone(),
            _ => unreachable!(),
        };
        rest = after_key.trim();
        // Skip colon
        if rest.starts_with(':') {
            rest = rest[1..].trim();
        }
        // Parse value
        let (val, after_val) = parse_json_value(rest);
        rest = after_val.trim();
        fields.push(RecordField { name: key, value: val });
        // Skip comma or end
        if rest.starts_with(',') {
            rest = rest[1..].trim();
        } else if rest.starts_with('}') {
            rest = &rest[1..];
            break;
        } else {
            break;
        }
    }
    (alloc(Value::Record(fields)), rest)
}

fn parse_json_array(s: &str) -> (*mut Value, &str) {
    debug_assert!(s.starts_with('['));
    let mut rest = s[1..].trim();
    let mut items: Vec<*mut Value> = Vec::new();
    if rest.starts_with(']') {
        return (alloc(Value::Relation(items)), &rest[1..]);
    }
    loop {
        let (val, after_val) = parse_json_value(rest);
        rest = after_val.trim();
        items.push(val);
        if rest.starts_with(',') {
            rest = rest[1..].trim();
        } else if rest.starts_with(']') {
            rest = &rest[1..];
            break;
        } else {
            break;
        }
    }
    (alloc(Value::Relation(items)), rest)
}

// ── Standard library: utility operations ──────────────────────

/// id(x) — identity function, returns its argument unchanged
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_id(v: *mut Value) -> *mut Value {
    v
}

/// not(bool) — boolean negation (function form of !)
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_not_fn(v: *mut Value) -> *mut Value {
    knot_value_not(v)
}

// ── Standard library: file system operations ──────────────────

/// readFile(path) — read entire file contents as Text
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_read_file(path: *mut Value) -> *mut Value {
    match unsafe { as_ref(path) } {
        Value::Text(p) => match std::fs::read_to_string(p) {
            Ok(contents) => alloc(Value::Text(contents)),
            Err(e) => panic!("knot runtime: readFile failed for {:?}: {}", p, e),
        },
        _ => panic!(
            "knot runtime: readFile expected Text, got {}",
            type_name(path)
        ),
    }
}

/// writeFile(path, contents) — write Text to a file (creates or overwrites)
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_write_file(path: *mut Value, contents: *mut Value) -> *mut Value {
    let p = match unsafe { as_ref(path) } {
        Value::Text(s) => s.as_str(),
        _ => panic!(
            "knot runtime: writeFile expected Text as first arg, got {}",
            type_name(path)
        ),
    };
    let c = match unsafe { as_ref(contents) } {
        Value::Text(s) => s.as_str(),
        _ => panic!(
            "knot runtime: writeFile expected Text as second arg, got {}",
            type_name(contents)
        ),
    };
    match std::fs::write(p, c) {
        Ok(()) => alloc(Value::Unit),
        Err(e) => panic!("knot runtime: writeFile failed for {:?}: {}", p, e),
    }
}

/// appendFile(path, contents) — append Text to a file
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_append_file(path: *mut Value, contents: *mut Value) -> *mut Value {
    use std::io::Write;
    let p = match unsafe { as_ref(path) } {
        Value::Text(s) => s.as_str(),
        _ => panic!(
            "knot runtime: appendFile expected Text as first arg, got {}",
            type_name(path)
        ),
    };
    let c = match unsafe { as_ref(contents) } {
        Value::Text(s) => s.as_str(),
        _ => panic!(
            "knot runtime: appendFile expected Text as second arg, got {}",
            type_name(contents)
        ),
    };
    match std::fs::OpenOptions::new().create(true).append(true).open(p) {
        Ok(mut f) => {
            f.write_all(c.as_bytes())
                .unwrap_or_else(|e| panic!("knot runtime: appendFile write failed for {:?}: {}", p, e));
            alloc(Value::Unit)
        }
        Err(e) => panic!("knot runtime: appendFile failed for {:?}: {}", p, e),
    }
}

/// fileExists(path) — check whether a file exists
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_file_exists(path: *mut Value) -> *mut Value {
    match unsafe { as_ref(path) } {
        Value::Text(p) => alloc(Value::Bool(std::path::Path::new(p).exists())),
        _ => panic!(
            "knot runtime: fileExists expected Text, got {}",
            type_name(path)
        ),
    }
}

/// removeFile(path) — delete a file
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_remove_file(path: *mut Value) -> *mut Value {
    match unsafe { as_ref(path) } {
        Value::Text(p) => match std::fs::remove_file(p) {
            Ok(()) => alloc(Value::Unit),
            Err(e) => panic!("knot runtime: removeFile failed for {:?}: {}", p, e),
        },
        _ => panic!(
            "knot runtime: removeFile expected Text, got {}",
            type_name(path)
        ),
    }
}

/// listDir(path) — list directory entries as a relation of Text
#[unsafe(no_mangle)]
pub extern "C" fn knot_fs_list_dir(path: *mut Value) -> *mut Value {
    match unsafe { as_ref(path) } {
        Value::Text(p) => {
            let entries: Vec<*mut Value> = match std::fs::read_dir(p) {
                Ok(rd) => rd
                    .filter_map(|entry| entry.ok())
                    .map(|entry| alloc(Value::Text(entry.file_name().to_string_lossy().into_owned())))
                    .collect(),
                Err(e) => panic!("knot runtime: listDir failed for {:?}: {}", p, e),
            };
            alloc(Value::Relation(entries))
        }
        _ => panic!(
            "knot runtime: listDir expected Text, got {}",
            type_name(path)
        ),
    }
}

// ── Database operations ───────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_db_open(path_ptr: *const u8, path_len: usize) -> *mut c_void {
    let path = unsafe { str_from_raw(path_ptr, path_len) };
    let conn = Connection::open(path).expect("knot runtime: failed to open database");
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
        .expect("knot runtime: failed to set pragmas");
    let db = Box::new(KnotDb {
        conn,
        atomic_depth: std::cell::Cell::new(0),
        indexed: RefCell::new(HashSet::new()),
    });
    Box::into_raw(db) as *mut c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_db_close(db: *mut c_void) {
    if !db.is_null() {
        let _ = unsafe { Box::from_raw(db as *mut KnotDb) };
    }
}

/// Execute a SQL statement (e.g., CREATE TABLE).
#[unsafe(no_mangle)]
pub extern "C" fn knot_db_exec(db: *mut c_void, sql_ptr: *const u8, sql_len: usize) {
    let db = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };
    debug_sql(sql);
    db.conn
        .execute_batch(sql)
        .unwrap_or_else(|e| panic!("knot runtime: SQL error: {}\n  SQL: {}", e, sql));
}

// ── Schema tracking ──────────────────────────────────────────────

/// Create the schema metadata table that tracks each source's column layout.
#[unsafe(no_mangle)]
pub extern "C" fn knot_schema_init(db: *mut c_void) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql =
        "CREATE TABLE IF NOT EXISTS _knot_schema (name TEXT PRIMARY KEY, schema TEXT NOT NULL);";
    debug_sql(sql);
    db_ref
        .conn
        .execute_batch(sql)
        .expect("knot runtime: failed to create schema tracking table");
}

/// Apply a migration to a source relation.
///
/// Checks the stored schema in `_knot_schema`:
/// - If stored == new_schema: already migrated, skip.
/// - If stored == old_schema: read old rows, transform each via `migrate_fn`,
///   drop & recreate the table, insert transformed rows, update stored schema.
/// - If no stored schema: new table, skip.
/// - Otherwise: error (unexpected schema).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_migrate(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    old_schema_ptr: *const u8,
    old_schema_len: usize,
    new_schema_ptr: *const u8,
    new_schema_len: usize,
    migrate_fn: *mut Value,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let old_schema = unsafe { str_from_raw(old_schema_ptr, old_schema_len) };
    let new_schema = unsafe { str_from_raw(new_schema_ptr, new_schema_len) };

    // Check stored schema
    let stored: Option<String> = db_ref
        .conn
        .query_row(
            "SELECT schema FROM _knot_schema WHERE name = ?1;",
            rusqlite::params![name],
            |row| row.get(0),
        )
        .ok();

    match &stored {
        Some(s) if s == new_schema => return,
        Some(s) if s == old_schema => {}
        Some(s) => panic!(
            "knot runtime: source '{}' has schema '{}', expected '{}' (pre-migration) or '{}' (post-migration).\n\
             Check your migrate block.",
            name, s, old_schema, new_schema
        ),
        None => return,
    }

    eprintln!("Migrating source '{}'...", name);

    // 1. Read all rows using old schema
    let old_data = knot_source_read(db, name_ptr, name_len, old_schema_ptr, old_schema_len);
    let old_rows = match unsafe { as_ref(old_data) } {
        Value::Relation(rows) => rows.clone(),
        _ => panic!("knot runtime: expected relation during migration"),
    };

    // 2. Transform each row through the migration function
    let mut new_rows: Vec<*mut Value> = Vec::with_capacity(old_rows.len());
    for row in &old_rows {
        let new_row = knot_value_call(db, migrate_fn, *row);
        new_rows.push(new_row);
    }

    // 3. Drop old table + index and recreate with new schema (in a transaction)
    let table = quote_ident(&format!("_knot_{}", name));
    let new_cols = parse_schema(new_schema);
    let col_defs: Vec<String> = new_cols
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.name), sql_type(c.ty)))
        .collect();
    let col_names: Vec<String> = new_cols.iter().map(|c| quote_ident(&c.name)).collect();

    db_ref
        .conn
        .execute_batch("BEGIN IMMEDIATE;")
        .expect("knot runtime: failed to begin migration transaction");

    let drop_sql = format!("DROP TABLE IF EXISTS {};", table);
    debug_sql(&drop_sql);
    db_ref
        .conn
        .execute_batch(&drop_sql)
        .expect("knot runtime: failed to drop table during migration");

    let create_sql = format!("CREATE TABLE {} ({});", table, col_defs.join(", "));
    debug_sql(&create_sql);
    db_ref
        .conn
        .execute_batch(&create_sql)
        .expect("knot runtime: failed to create table during migration");

    if !new_cols.is_empty() {
        let idx_sql = format!(
            "CREATE UNIQUE INDEX {} ON {} ({});",
            quote_ident(&format!("_knot_{}_unique", name)),
            table,
            col_names.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = db_ref.conn.execute_batch(&idx_sql);
    }

    // 4. Insert transformed rows
    if !new_cols.is_empty() && !new_rows.is_empty() {
        let placeholders: Vec<String> = new_cols
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", i + 1))
            .collect();
        let insert_sql = format!(
            "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
            table,
            col_names.join(", "),
            placeholders.join(", ")
        );
        debug_sql(&insert_sql);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&insert_sql)
            .expect("knot runtime: failed to prepare insert during migration");

        for row_ptr in &new_rows {
            let row_ref = unsafe { as_ref(*row_ptr) };
            let fields = match row_ref {
                Value::Record(f) => f,
                _ => panic!("knot runtime: migration function must return a record"),
            };
            let params: Vec<rusqlite::types::Value> = new_cols
                .iter()
                .map(|c| {
                    let val = fields
                        .iter()
                        .find(|f| f.name == c.name)
                        .map(|f| f.value)
                        .unwrap_or_else(|| {
                            panic!(
                                "knot runtime: migration result missing field '{}'",
                                c.name
                            )
                        });
                    value_to_sql_param(val)
                })
                .collect();
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
            stmt.execute(param_refs.as_slice())
                .expect("knot runtime: failed to insert row during migration");
        }
    }

    // 5. Update stored schema
    db_ref
        .conn
        .execute(
            "INSERT OR REPLACE INTO _knot_schema (name, schema) VALUES (?1, ?2);",
            rusqlite::params![name, new_schema],
        )
        .expect("knot runtime: failed to update schema after migration");

    db_ref
        .conn
        .execute_batch("COMMIT;")
        .expect("knot runtime: failed to commit migration");

    eprintln!("Migrated source '{}': {} rows", name, old_rows.len());
}

// ── Source operations ─────────────────────────────────────────────

/// Schema descriptor format: "col1:type1,col2:type2,..."
/// Types: "int", "float", "text", "bool", "tag"
/// Nested relations: "col:[inner_schema]"
/// ADT schema format: "#Ctor1:f1=t1;f2=t2|Ctor2|Ctor3:f3=t3"
struct ColumnSpec {
    name: String,
    ty: ColType,
}

#[derive(Clone, Copy)]
enum ColType {
    Int,
    Float,
    Text,
    Bool,
    Bytes,
    /// Stored as TEXT, reconstructed as Constructor on read
    Tag,
}

/// A nested relation field stored in a child table.
struct NestedField {
    name: String,
    /// Scalar columns in the child table
    columns: Vec<ColumnSpec>,
    /// Further nested relations within this child
    nested: Vec<NestedField>,
}

/// Parsed record schema with both scalar columns and nested relation fields.
struct RecordSchema {
    /// Scalar (non-relation) columns stored directly in this table
    columns: Vec<ColumnSpec>,
    /// Nested relation fields stored in child tables
    nested: Vec<NestedField>,
}

/// ADT constructor schema: constructor name and its fields
struct CtorSpec {
    name: String,
    fields: Vec<ColumnSpec>,
}

/// Parsed ADT schema for direct ADT relations
struct AdtSpec {
    constructors: Vec<CtorSpec>,
    /// Union of all fields across all constructors (for wide table columns)
    all_fields: Vec<ColumnSpec>,
}

/// Determine if a schema descriptor is an ADT schema (starts with '#')
fn is_adt_schema(spec: &str) -> bool {
    spec.starts_with('#')
}

/// Parse an ADT schema descriptor: "#Ctor1:f1=t1;f2=t2|Ctor2|Ctor3:f3=t3"
fn parse_adt_schema(spec: &str) -> AdtSpec {
    let body = &spec[1..]; // strip '#'
    let mut constructors = Vec::new();
    let mut all_field_names: HashSet<String> = HashSet::new();
    let mut all_fields: Vec<ColumnSpec> = Vec::new();

    for ctor_part in body.split('|') {
        let mut parts = ctor_part.splitn(2, ':');
        let name = parts.next().unwrap().to_string();
        let fields: Vec<ColumnSpec> = if let Some(field_spec) = parts.next() {
            field_spec
                .split(';')
                .map(|f| {
                    let mut fp = f.splitn(2, '=');
                    let fname = fp.next().unwrap().to_string();
                    let fty = match fp.next().unwrap_or("text") {
                        "int" => ColType::Int,
                        "float" => ColType::Float,
                        "text" => ColType::Text,
                        "bool" => ColType::Bool,
                        "bytes" => ColType::Bytes,
                        "tag" => ColType::Tag,
                        other => panic!("knot runtime: unknown ADT field type '{}'", other),
                    };
                    ColumnSpec {
                        name: fname,
                        ty: fty,
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        // Add unique fields to the all_fields list
        for f in &fields {
            if all_field_names.insert(f.name.clone()) {
                all_fields.push(ColumnSpec {
                    name: f.name.clone(),
                    ty: f.ty,
                });
            }
        }

        constructors.push(CtorSpec { name, fields });
    }

    AdtSpec {
        constructors,
        all_fields,
    }
}

/// Split a string by `sep` while respecting `[...]` bracket nesting.
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
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

fn parse_col_type(s: &str) -> ColType {
    match s {
        "int" => ColType::Int,
        "float" => ColType::Float,
        "text" => ColType::Text,
        "bool" => ColType::Bool,
        "bytes" => ColType::Bytes,
        "tag" => ColType::Tag,
        other => panic!("knot runtime: unknown column type '{}'", other),
    }
}

fn parse_record_schema(spec: &str) -> RecordSchema {
    if spec.is_empty() {
        return RecordSchema { columns: Vec::new(), nested: Vec::new() };
    }
    let mut columns = Vec::new();
    let mut nested = Vec::new();
    for part in split_respecting_brackets(spec, ',') {
        // Find the first ':' (field name separator)
        let colon = part.find(':').unwrap_or_else(|| {
            panic!("knot runtime: malformed schema field '{}'", part)
        });
        let name = part[..colon].to_string();
        let type_str = &part[colon + 1..];
        if type_str.starts_with('[') && type_str.ends_with(']') {
            // Nested relation: parse child schema recursively
            let inner = &type_str[1..type_str.len() - 1];
            let child = parse_record_schema(inner);
            nested.push(NestedField {
                name,
                columns: child.columns,
                nested: child.nested,
            });
        } else {
            columns.push(ColumnSpec { name, ty: parse_col_type(type_str) });
        }
    }
    RecordSchema { columns, nested }
}

/// Backward-compatible: parse a flat schema (no nested fields) into Vec<ColumnSpec>.
fn parse_schema(spec: &str) -> Vec<ColumnSpec> {
    parse_record_schema(spec).columns
}

fn sql_type(ty: ColType) -> &'static str {
    match ty {
        ColType::Int => "BLOB",
        ColType::Float => "REAL",
        ColType::Text => "TEXT",
        ColType::Bool => "INTEGER",
        ColType::Bytes => "BLOB",
        ColType::Tag => "TEXT",
    }
}

/// Read a column value from a SQLite row, returning null pointer for SQL NULL.
fn read_sql_column(row: &rusqlite::Row, i: usize, ty: ColType) -> *mut Value {
    if matches!(row.get_ref(i).unwrap(), ValueRef::Null) {
        return std::ptr::null_mut();
    }
    match ty {
        ColType::Int => {
            match row.get_ref(i).unwrap() {
                ValueRef::Integer(n) => alloc(Value::Int(BigInt::from(n))),
                ValueRef::Blob(b) => {
                    let s = std::str::from_utf8(b).expect("knot runtime: invalid UTF-8 in bigint blob");
                    let n: BigInt = s.parse().expect("knot runtime: invalid bigint in column");
                    alloc(Value::Int(n))
                }
                ValueRef::Text(s) => {
                    let s = std::str::from_utf8(s).expect("knot runtime: invalid UTF-8 in int column");
                    let n: BigInt = s.parse().expect("knot runtime: invalid bigint in column");
                    alloc(Value::Int(n))
                }
                other => panic!("knot runtime: unexpected SQLite type for Int column: {:?}", other),
            }
        }
        ColType::Float => knot_value_float(row.get::<_, f64>(i).unwrap()),
        ColType::Text => {
            let s: String = row.get(i).unwrap();
            alloc(Value::Text(s))
        }
        ColType::Bool => knot_value_bool(row.get::<_, i32>(i).unwrap()),
        ColType::Bytes => {
            let b: Vec<u8> = row.get(i).unwrap();
            alloc(Value::Bytes(b))
        }
        ColType::Tag => {
            // Read TEXT but reconstruct as a Constructor with Unit payload
            let tag: String = row.get(i).unwrap();
            alloc(Value::Constructor(tag, alloc(Value::Unit)))
        }
    }
}

/// Create a record table and any child tables for nested relation fields.
/// Tables with nested children get `_id INTEGER PRIMARY KEY AUTOINCREMENT`.
fn init_record_table(conn: &rusqlite::Connection, table_name: &str, schema: &RecordSchema) {
    let table = quote_ident(table_name);
    let has_children = !schema.nested.is_empty();

    let mut col_defs: Vec<String> = Vec::new();
    let mut unique_cols: Vec<String> = Vec::new();

    if has_children {
        col_defs.push("_id INTEGER PRIMARY KEY AUTOINCREMENT".to_string());
    }

    for c in &schema.columns {
        col_defs.push(format!("{} {}", quote_ident(&c.name), sql_type(c.ty)));
        unique_cols.push(quote_ident(&c.name));
    }

    if col_defs.is_empty() {
        col_defs.push("_dummy INTEGER DEFAULT 0".to_string());
    }

    let sql = format!("CREATE TABLE IF NOT EXISTS {} ({});", table, col_defs.join(", "));
    debug_sql(&sql);
    conn.execute_batch(&sql).unwrap_or_else(|e| {
        panic!("knot runtime: failed to create table '{}': {}", table_name, e)
    });

    if !unique_cols.is_empty() {
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&format!("{}_unique", table_name)),
            table,
            unique_cols.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = conn.execute_batch(&idx_sql);
    }

    // Recursively create child tables
    for nf in &schema.nested {
        init_child_table(conn, table_name, nf);
    }
}

/// Create a child table for a nested relation field, recursing for deeper nesting.
fn init_child_table(conn: &rusqlite::Connection, parent_table: &str, nf: &NestedField) {
    let child_table_name = format!("{}__{}", parent_table, nf.name);
    let child_table = quote_ident(&child_table_name);
    let has_children = !nf.nested.is_empty();

    let mut col_defs = vec!["_parent_id INTEGER NOT NULL".to_string()];
    let mut unique_cols = vec![quote_ident("_parent_id")];

    if has_children {
        col_defs.push("_id INTEGER PRIMARY KEY AUTOINCREMENT".to_string());
    }

    for c in &nf.columns {
        col_defs.push(format!("{} {}", quote_ident(&c.name), sql_type(c.ty)));
        unique_cols.push(quote_ident(&c.name));
    }

    let sql = format!("CREATE TABLE IF NOT EXISTS {} ({});", child_table, col_defs.join(", "));
    debug_sql(&sql);
    conn.execute_batch(&sql).unwrap_or_else(|e| {
        panic!("knot runtime: failed to create child table '{}': {}", child_table_name, e)
    });

    // Unique index: (_parent_id, scalar_cols) for set semantics within each parent row
    if unique_cols.len() > 1 {
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&format!("{}_unique", child_table_name)),
            child_table,
            unique_cols.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = conn.execute_batch(&idx_sql);
    }

    // Recurse for deeper nesting
    for grandchild in &nf.nested {
        init_child_table(conn, &child_table_name, grandchild);
    }
}

/// Try to auto-apply a safe schema change (e.g. adding ADT constructors).
/// Returns true if the change was applied, false if it's a breaking change.
fn auto_apply_schema_change(
    conn: &Connection,
    name: &str,
    stored: &str,
    compiled: &str,
) -> bool {
    let table = format!("_knot_{}", name);
    let stored_is_adt = is_adt_schema(stored);
    let compiled_is_adt = is_adt_schema(compiled);

    if stored_is_adt != compiled_is_adt {
        return false;
    }

    if stored_is_adt {
        auto_apply_adt_change(conn, &table, name, stored, compiled)
    } else {
        auto_apply_record_change(conn, &table, name, stored, compiled)
    }
}

fn auto_apply_adt_change(
    conn: &Connection,
    table: &str,
    name: &str,
    stored: &str,
    compiled: &str,
) -> bool {
    let old_adt = parse_adt_schema(stored);
    let new_adt = parse_adt_schema(compiled);

    // Every old constructor must exist in new with identical fields
    for old_ctor in &old_adt.constructors {
        match new_adt.constructors.iter().find(|c| c.name == old_ctor.name) {
            Some(new_ctor) => {
                if old_ctor.fields.len() != new_ctor.fields.len() {
                    return false;
                }
                for (of, nf) in old_ctor.fields.iter().zip(&new_ctor.fields) {
                    if of.name != nf.name || std::mem::discriminant(&of.ty) != std::mem::discriminant(&nf.ty) {
                        return false;
                    }
                }
            }
            None => return false,
        }
    }

    // Add new columns for new constructor fields
    let old_field_names: HashSet<&str> = old_adt.all_fields.iter().map(|f| f.name.as_str()).collect();
    for f in &new_adt.all_fields {
        if !old_field_names.contains(f.name.as_str()) {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN {} {};",
                quote_ident(table),
                quote_ident(&f.name),
                sql_type(f.ty)
            );
            debug_sql(&sql);
            if conn.execute_batch(&sql).is_err() {
                return false;
            }
        }
    }

    // Drop and recreate unique index with full column set
    let drop_idx = format!(
        "DROP INDEX IF EXISTS {};",
        quote_ident(&format!("{}_unique", table))
    );
    debug_sql(&drop_idx);
    let _ = conn.execute_batch(&drop_idx);

    let coalesced: Vec<String> = std::iter::once(quote_ident("_tag"))
        .chain(new_adt.all_fields.iter().map(|f| {
            let col = quote_ident(&f.name);
            match f.ty {
                ColType::Int | ColType::Bool => format!("COALESCE({}, -9223372036854775808)", col),
                ColType::Float => format!("COALESCE({}, -1.7976931348623157e+308)", col),
                _ => format!("COALESCE({}, X'00')", col),
            }
        }))
        .collect();
    let idx_sql = format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
        quote_ident(&format!("{}_unique", table)),
        quote_ident(table),
        coalesced.join(", ")
    );
    debug_sql(&idx_sql);
    let _ = conn.execute_batch(&idx_sql);

    // Update stored schema
    let _ = conn.execute(
        "INSERT OR REPLACE INTO _knot_schema (name, schema) VALUES (?1, ?2);",
        rusqlite::params![name, compiled],
    );

    true
}

fn auto_apply_record_change(
    conn: &Connection,
    table: &str,
    name: &str,
    stored: &str,
    compiled: &str,
) -> bool {
    let old_rec = parse_record_schema(stored);
    let new_rec = parse_record_schema(compiled);

    // Every old column must exist in new with same type
    for old_col in &old_rec.columns {
        match new_rec.columns.iter().find(|c| c.name == old_col.name) {
            Some(new_col) => {
                if std::mem::discriminant(&old_col.ty) != std::mem::discriminant(&new_col.ty) {
                    return false;
                }
            }
            None => return false,
        }
    }

    // Any removed nested fields → breaking
    for old_nf in &old_rec.nested {
        if !new_rec.nested.iter().any(|n| n.name == old_nf.name) {
            return false;
        }
    }

    // Add new columns
    let old_col_names: HashSet<&str> = old_rec.columns.iter().map(|c| c.name.as_str()).collect();
    for c in &new_rec.columns {
        if !old_col_names.contains(c.name.as_str()) {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN {} {};",
                quote_ident(table),
                quote_ident(&c.name),
                sql_type(c.ty)
            );
            debug_sql(&sql);
            if conn.execute_batch(&sql).is_err() {
                return false;
            }
        }
    }

    // Drop and recreate unique index with full column set
    let drop_idx = format!(
        "DROP INDEX IF EXISTS {};",
        quote_ident(&format!("{}_unique", table))
    );
    debug_sql(&drop_idx);
    let _ = conn.execute_batch(&drop_idx);

    let unique_cols: Vec<String> = new_rec.columns.iter().map(|c| quote_ident(&c.name)).collect();
    if !unique_cols.is_empty() {
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&format!("{}_unique", table)),
            quote_ident(table),
            unique_cols.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = conn.execute_batch(&idx_sql);
    }

    // Initialize any new child tables for nested relations
    let old_nested_names: HashSet<&str> = old_rec.nested.iter().map(|n| n.name.as_str()).collect();
    for nf in &new_rec.nested {
        if !old_nested_names.contains(nf.name.as_str()) {
            init_child_table(conn, table, nf);
        }
    }

    // Update stored schema
    let _ = conn.execute(
        "INSERT OR REPLACE INTO _knot_schema (name, schema) VALUES (?1, ?2);",
        rusqlite::params![name, compiled],
    );

    true
}

/// Initialize a source table. Creates it if it doesn't exist.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_init(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let table = quote_ident(&format!("_knot_{}", name));

    if is_adt_schema(schema) {
        // ADT schema: create wide table with _tag + all constructor fields
        let adt = parse_adt_schema(schema);
        let mut col_defs = vec![format!("{} TEXT NOT NULL", quote_ident("_tag"))];
        let mut col_names = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            col_defs.push(format!("{} {}", quote_ident(&f.name), sql_type(f.ty)));
            col_names.push(quote_ident(&f.name));
        }

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({});",
            table,
            col_defs.join(", ")
        );
        debug_sql(&sql);
        db_ref.conn.execute_batch(&sql).unwrap_or_else(|e| {
            panic!("knot runtime: failed to create table '{}': {}", name, e)
        });

        // Unique index using COALESCE to treat NULLs as equal
        let coalesced: Vec<String> = std::iter::once(quote_ident("_tag"))
            .chain(adt.all_fields.iter().map(|f| {
                let col = quote_ident(&f.name);
                let default = match f.ty {
                    ColType::Int | ColType::Bool => "COALESCE(".to_string() + &col + ", -9223372036854775808)",
                    ColType::Float => "COALESCE(".to_string() + &col + ", -1.7976931348623157e+308)",
                    _ => "COALESCE(".to_string() + &col + ", X'00')",
                };
                default
            }))
            .collect();
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&format!("_knot_{}_unique", name)),
            table,
            coalesced.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = db_ref.conn.execute_batch(&idx_sql);

        // Auto-index _tag for efficient pattern matching (WHERE _tag = ?)
        db_ref.ensure_index(&format!("_knot_{}", name), "_tag");
    } else {
        // Regular record schema (may include nested relations)
        let rec = parse_record_schema(schema);
        init_record_table(&db_ref.conn, &format!("_knot_{}", name), &rec);
    }

    // Check stored schema against compiled schema
    let stored: Option<String> = db_ref
        .conn
        .query_row(
            "SELECT schema FROM _knot_schema WHERE name = ?1;",
            rusqlite::params![name],
            |row| row.get(0),
        )
        .ok();

    if let Some(ref stored_schema) = stored {
        if stored_schema != schema {
            if !auto_apply_schema_change(&db_ref.conn, name, stored_schema, schema) {
                panic!(
                    "knot runtime: schema mismatch for source '*{}'.\n\
                     Stored:   {}\n\
                     Compiled: {}\n\
                     Add a `migrate *{} from {{...}} to {{...}} using (\\old -> ...)` block to your source.",
                    name, stored_schema, schema, name
                );
            }
        }
    }

    // Record current schema
    db_ref
        .conn
        .execute(
            "INSERT OR REPLACE INTO _knot_schema (name, schema) VALUES (?1, ?2);",
            rusqlite::params![name, schema],
        )
        .expect("knot runtime: failed to record schema");
}

/// Read all rows from a source relation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_read(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let table = quote_ident(&format!("_knot_{}", name));

    if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);
        // Build field name → index map for O(1) lookups
        let field_idx: HashMap<&str, usize> = adt.all_fields.iter().enumerate()
            .map(|(i, f)| (f.name.as_str(), i)).collect();
        // SELECT _tag + all fields from the wide table
        let mut select_cols = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            select_cols.push(quote_ident(&f.name));
        }
        let sql = format!("SELECT {} FROM {}", select_cols.join(", "), table);
        debug_sql(&sql);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&sql)
            .unwrap_or_else(|e| panic!("knot runtime: query error: {}", e));
        let mut rows: Vec<*mut Value> = Vec::new();
        let mut result_rows = stmt
            .query([])
            .unwrap_or_else(|e| panic!("knot runtime: query exec error: {}", e));

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: row fetch error: {}", e))
        {
            let tag: String = row.get(0).unwrap();
            // Find the constructor spec for this tag
            let ctor = adt.constructors.iter().find(|c| c.name == tag);
            let payload = if let Some(ctor) = ctor {
                if ctor.fields.is_empty() {
                    alloc(Value::Unit)
                } else {
                    // Build a record from the constructor's specific fields
                    let record = knot_record_empty(ctor.fields.len());
                    for field in &ctor.fields {
                        let col_idx = field_idx[field.name.as_str()];
                        let val = read_sql_column(row, col_idx + 1, field.ty); // +1 for _tag
                        let fname = field.name.as_bytes();
                        knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                    }
                    record
                }
            } else {
                // Unknown constructor: include all non-NULL fields
                let record = knot_record_empty(adt.all_fields.len());
                let mut has_fields = false;
                for (i, field) in adt.all_fields.iter().enumerate() {
                    if !matches!(row.get_ref(i + 1).unwrap(), ValueRef::Null) {
                        let val = read_sql_column(row, i + 1, field.ty);
                        let fname = field.name.as_bytes();
                        knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                        has_fields = true;
                    }
                }
                if has_fields {
                    record
                } else {
                    alloc(Value::Unit)
                }
            };
            rows.push(alloc(Value::Constructor(tag, payload)));
        }
        alloc(Value::Relation(rows))
    } else {
        let rec = parse_record_schema(schema);
        read_record_table(&db_ref.conn, &format!("_knot_{}", name), &rec)
    }
}

/// Execute an arbitrary SQL query that returns COUNT(*), with bind parameters.
/// Returns a boxed Int value.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_query_count(
    db: *mut c_void,
    sql_ptr: *const u8,
    sql_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: query_count params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    debug_sql_params(sql, &sql_params);

    let count: i64 = db_ref
        .conn
        .query_row(sql, param_refs.as_slice(), |row| row.get(0))
        .unwrap_or_else(|e| panic!("knot runtime: query_count error: {}\n  SQL: {}", e, sql));
    alloc(Value::Int(BigInt::from(count)))
}

/// Execute a SQL aggregate query returning a float (e.g. AVG).
/// Returns a boxed Float value.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_query_float(
    db: *mut c_void,
    sql_ptr: *const u8,
    sql_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: query_float params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    debug_sql_params(sql, &sql_params);

    let result: f64 = db_ref
        .conn
        .query_row(sql, param_refs.as_slice(), |row| row.get(0))
        .unwrap_or_else(|e| panic!("knot runtime: query_float error: {}\n  SQL: {}", e, sql));
    alloc(Value::Float(result))
}

/// Count rows in a source relation via SQL COUNT(*).
/// Returns a boxed Int value.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_count(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let table = quote_ident(&format!("_knot_{}", name));
    let sql = format!("SELECT COUNT(*) FROM {}", table);
    debug_sql(&sql);
    let count: i64 = db_ref
        .conn
        .query_row(&sql, [], |row| row.get(0))
        .unwrap_or_else(|e| panic!("knot runtime: count error: {}", e));
    alloc(Value::Int(BigInt::from(count)))
}

/// Read rows from a source relation with a WHERE clause.
/// Params is a Relation of bind parameter values (?1, ?2, ...).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_read_where(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    where_ptr: *const u8,
    where_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let where_clause = unsafe { str_from_raw(where_ptr, where_len) };

    let table_name = format!("_knot_{}", name);
    let table = quote_ident(&table_name);

    // Auto-index columns used in the WHERE clause
    db_ref.ensure_indexes_for_where(&table_name, where_clause);

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: read_where params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);
        let field_idx: HashMap<&str, usize> = adt.all_fields.iter().enumerate()
            .map(|(i, f)| (f.name.as_str(), i)).collect();
        let mut select_cols = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            select_cols.push(quote_ident(&f.name));
        }
        let sql = format!(
            "SELECT {} FROM {} WHERE {}",
            select_cols.join(", "),
            table,
            where_clause
        );
        debug_sql_params(&sql, &sql_params);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&sql)
            .unwrap_or_else(|e| panic!("knot runtime: read_where query error: {}", e));
        let mut rows: Vec<*mut Value> = Vec::new();
        let mut result_rows = stmt
            .query(param_refs.as_slice())
            .unwrap_or_else(|e| panic!("knot runtime: read_where query exec error: {}", e));

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: read_where row fetch error: {}", e))
        {
            let tag: String = row.get(0).unwrap();
            let ctor = adt.constructors.iter().find(|c| c.name == tag);
            let payload = if let Some(ctor) = ctor {
                if ctor.fields.is_empty() {
                    alloc(Value::Unit)
                } else {
                    let record = knot_record_empty(ctor.fields.len());
                    for field in &ctor.fields {
                        let col_idx = field_idx[field.name.as_str()];
                        let val = read_sql_column(row, col_idx + 1, field.ty);
                        let fname = field.name.as_bytes();
                        knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                    }
                    record
                }
            } else {
                let record = knot_record_empty(adt.all_fields.len());
                let mut has_fields = false;
                for (i, field) in adt.all_fields.iter().enumerate() {
                    if !matches!(row.get_ref(i + 1).unwrap(), ValueRef::Null) {
                        let val = read_sql_column(row, i + 1, field.ty);
                        let fname = field.name.as_bytes();
                        knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                        has_fields = true;
                    }
                }
                if has_fields { record } else { alloc(Value::Unit) }
            };
            rows.push(alloc(Value::Constructor(tag, payload)));
        }
        alloc(Value::Relation(rows))
    } else {
        let rec = parse_record_schema(schema);
        let table_q = quote_ident(&table_name);
        let has_children = !rec.nested.is_empty();

        let mut select_cols: Vec<String> = Vec::new();
        if has_children {
            select_cols.push(quote_ident("_id"));
        }
        for c in &rec.columns {
            select_cols.push(quote_ident(&c.name));
        }

        let sql = format!(
            "SELECT {} FROM {} WHERE {}",
            if select_cols.is_empty() { "1".to_string() } else { select_cols.join(", ") },
            table_q,
            where_clause
        );
        debug_sql_params(&sql, &sql_params);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&sql)
            .unwrap_or_else(|e| panic!("knot runtime: read_where query error: {}", e));
        let mut rows: Vec<*mut Value> = Vec::new();
        let mut result_rows = stmt
            .query(param_refs.as_slice())
            .unwrap_or_else(|e| panic!("knot runtime: read_where query exec error: {}", e));

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: read_where row fetch error: {}", e))
        {
            let total_fields = rec.columns.len() + rec.nested.len();
            let record = knot_record_empty(total_fields);
            let col_offset = if has_children { 1 } else { 0 };

            for (i, col) in rec.columns.iter().enumerate() {
                let val = read_sql_column(row, i + col_offset, col.ty);
                let cname = col.name.as_bytes();
                knot_record_set_field(record, cname.as_ptr(), cname.len(), val);
            }

            if has_children {
                let parent_id: i64 = row.get(0).unwrap();
                for nf in &rec.nested {
                    let child_table_name = format!("{}__{}", table_name, nf.name);
                    let val = read_child_table(&db_ref.conn, &child_table_name, nf, parent_id);
                    let fname = nf.name.as_bytes();
                    knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
                }
            }

            rows.push(record);
        }
        alloc(Value::Relation(rows))
    }
}

/// Execute an arbitrary SQL SELECT and return results as a relation of records.
/// Used by the compiler for full SQL query compilation of do-blocks.
///
/// `sql` is the complete SELECT statement (with `?` placeholders).
/// `result_schema` is a record schema descriptor for constructing result records
/// (e.g., `"name:text,dept:text,budget:int"`).
/// `params` is a Relation of parameter values to bind to `?` placeholders.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_query(
    db: *mut c_void,
    sql_ptr: *const u8,
    sql_len: usize,
    result_schema_ptr: *const u8,
    result_schema_len: usize,
    params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sql = unsafe { str_from_raw(sql_ptr, sql_len) };
    let result_schema = unsafe { str_from_raw(result_schema_ptr, result_schema_len) };

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: source_query params must be a Relation, got {}",
            type_name(params)
        ),
    };
    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    debug_sql_params(sql, &sql_params);

    let rec = parse_record_schema(result_schema);

    let mut stmt = db_ref
        .conn
        .prepare_cached(sql)
        .unwrap_or_else(|e| panic!("knot runtime: source_query error: {}\n  SQL: {}", e, sql));
    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query(param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: source_query exec error: {}\n  SQL: {}", e, sql));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: source_query row fetch error: {}", e))
    {
        let record = knot_record_empty(rec.columns.len());
        for (i, col) in rec.columns.iter().enumerate() {
            let val = read_sql_column(row, i, col.ty);
            let cname = col.name.as_bytes();
            knot_record_set_field(record, cname.as_ptr(), cname.len(), val);
        }
        rows.push(record);
    }
    alloc(Value::Relation(rows))
}

/// Read rows from a source ADT relation matching a specific constructor tag.
/// Executes `SELECT <ctor_fields> FROM table WHERE _tag = ?` at the SQL level.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_match(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    tag_ptr: *const u8,
    tag_len: usize,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let tag = unsafe { str_from_raw(tag_ptr, tag_len) };

    let table = quote_ident(&format!("_knot_{}", name));
    let adt = parse_adt_schema(schema);

    let ctor = adt
        .constructors
        .iter()
        .find(|c| c.name == tag)
        .unwrap_or_else(|| panic!("knot runtime: match: unknown constructor '{}'", tag));

    if ctor.fields.is_empty() {
        // Nullary constructor: count matching rows, return that many Unit values
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE {} = ?1",
            table,
            quote_ident("_tag")
        );
        debug_sql(&sql);
        let count: i64 = db_ref
            .conn
            .query_row(&sql, rusqlite::params![tag], |row| row.get(0))
            .unwrap();
        let mut rows = Vec::with_capacity(count as usize);
        for _ in 0..count {
            rows.push(alloc(Value::Unit));
        }
        alloc(Value::Relation(rows))
    } else {
        let select_cols: Vec<String> =
            ctor.fields.iter().map(|f| quote_ident(&f.name)).collect();
        let sql = format!(
            "SELECT {} FROM {} WHERE {} = ?1",
            select_cols.join(", "),
            table,
            quote_ident("_tag")
        );
        debug_sql(&sql);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&sql)
            .unwrap_or_else(|e| panic!("knot runtime: match query error: {}", e));
        let mut rows: Vec<*mut Value> = Vec::new();
        let mut result_rows = stmt
            .query(rusqlite::params![tag])
            .unwrap_or_else(|e| panic!("knot runtime: match query exec error: {}", e));

        while let Some(row) = result_rows
            .next()
            .unwrap_or_else(|e| panic!("knot runtime: match row fetch error: {}", e))
        {
            let record = knot_record_empty(ctor.fields.len());
            for (i, field) in ctor.fields.iter().enumerate() {
                let val = read_sql_column(row, i, field.ty);
                let fname = field.name.as_bytes();
                knot_record_set_field(record, fname.as_ptr(), fname.len(), val);
            }
            rows.push(record);
        }
        alloc(Value::Relation(rows))
    }
}

/// Read all rows from a record table, including nested relation fields from child tables.
fn read_record_table(
    conn: &rusqlite::Connection,
    table_name: &str,
    schema: &RecordSchema,
) -> *mut Value {
    let table = quote_ident(table_name);
    let has_children = !schema.nested.is_empty();

    // Build SELECT: _id (if has children) + scalar columns
    let mut select_cols: Vec<String> = Vec::new();
    if has_children {
        select_cols.push(quote_ident("_id"));
    }
    for c in &schema.columns {
        select_cols.push(quote_ident(&c.name));
    }

    let sql = format!(
        "SELECT {} FROM {}",
        if select_cols.is_empty() { "1".to_string() } else { select_cols.join(", ") },
        table
    );
    debug_sql(&sql);

    let mut stmt = conn
        .prepare_cached(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: query error: {}", e));
    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query([])
        .unwrap_or_else(|e| panic!("knot runtime: query exec error: {}", e));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: row fetch error: {}", e))
    {
        let total_fields = schema.columns.len() + schema.nested.len();
        let record = knot_record_empty(total_fields);
        let col_offset = if has_children { 1 } else { 0 }; // skip _id column

        // Read scalar columns
        for (i, col) in schema.columns.iter().enumerate() {
            let val = read_sql_column(row, i + col_offset, col.ty);
            let name = col.name.as_bytes();
            knot_record_set_field(record, name.as_ptr(), name.len(), val);
        }

        // Read nested relation fields from child tables
        if has_children {
            let parent_id: i64 = row.get(0).unwrap();
            for nf in &schema.nested {
                let child_table_name = format!("{}__{}", table_name, nf.name);
                let val = read_child_table(conn, &child_table_name, nf, parent_id);
                let name = nf.name.as_bytes();
                knot_record_set_field(record, name.as_ptr(), name.len(), val);
            }
        }

        rows.push(record);
    }

    alloc(Value::Relation(rows))
}

/// Read child rows for a nested relation field, filtered by parent_id.
fn read_child_table(
    conn: &rusqlite::Connection,
    table_name: &str,
    nf: &NestedField,
    parent_id: i64,
) -> *mut Value {
    let table = quote_ident(table_name);
    let has_children = !nf.nested.is_empty();

    let mut select_cols: Vec<String> = Vec::new();
    if has_children {
        select_cols.push(quote_ident("_id"));
    }
    for c in &nf.columns {
        select_cols.push(quote_ident(&c.name));
    }

    let sql = format!(
        "SELECT {} FROM {} WHERE _parent_id = ?1",
        if select_cols.is_empty() { "1".to_string() } else { select_cols.join(", ") },
        table
    );
    debug_sql(&sql);

    let mut stmt = conn
        .prepare_cached(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: child query error: {}", e));
    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query(rusqlite::params![parent_id])
        .unwrap_or_else(|e| panic!("knot runtime: child query exec error: {}", e));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: child row fetch error: {}", e))
    {
        let total_fields = nf.columns.len() + nf.nested.len();
        let record = knot_record_empty(total_fields);
        let col_offset = if has_children { 1 } else { 0 };

        for (i, col) in nf.columns.iter().enumerate() {
            let val = read_sql_column(row, i + col_offset, col.ty);
            let name = col.name.as_bytes();
            knot_record_set_field(record, name.as_ptr(), name.len(), val);
        }

        if has_children {
            let child_id: i64 = row.get(0).unwrap();
            for grandchild in &nf.nested {
                let gc_table = format!("{}__{}", table_name, grandchild.name);
                let val = read_child_table(conn, &gc_table, grandchild, child_id);
                let name = grandchild.name.as_bytes();
                knot_record_set_field(record, name.as_ptr(), name.len(), val);
            }
        }

        rows.push(record);
    }

    alloc(Value::Relation(rows))
}

/// Serialize a Constructor value into SQL params for an ADT wide table.
/// Returns params for [_tag, field1, field2, ...] columns.
fn adt_row_to_params(
    row_ptr: *mut Value,
    adt: &AdtSpec,
) -> Vec<rusqlite::types::Value> {
    let row = unsafe { as_ref(row_ptr) };
    match row {
        Value::Constructor(tag, payload) => {
            let mut params = Vec::with_capacity(1 + adt.all_fields.len());
            // First column: _tag
            params.push(rusqlite::types::Value::Text(tag.clone()));

            // Find which fields belong to this constructor
            let ctor = adt.constructors.iter().find(|c| c.name == *tag);
            let ctor_field_names: HashSet<&str> = ctor
                .map(|c| c.fields.iter().map(|f| f.name.as_str()).collect())
                .unwrap_or_default();

            // For each field in the wide table
            for field in &adt.all_fields {
                if ctor_field_names.contains(field.name.as_str()) {
                    // This field belongs to this constructor — extract from payload
                    let payload_ref = unsafe { as_ref(*payload) };
                    match payload_ref {
                        Value::Record(fields) => {
                            let val = fields
                                .iter()
                                .find(|f| f.name == field.name)
                                .map(|f| value_to_sqlite(f.value, field.ty))
                                .unwrap_or(rusqlite::types::Value::Null);
                            params.push(val);
                        }
                        _ => params.push(rusqlite::types::Value::Null),
                    }
                } else {
                    // Field doesn't belong to this constructor — NULL
                    params.push(rusqlite::types::Value::Null);
                }
            }
            params
        }
        _ => panic!(
            "knot runtime: ADT source rows must be Constructors, got {}",
            type_name(row_ptr)
        ),
    }
}

/// Delete all rows from a record table and its child tables (children first).
fn delete_record_table(conn: &rusqlite::Connection, table_name: &str, schema: &RecordSchema) {
    // Delete children first
    for nf in &schema.nested {
        delete_child_table(conn, table_name, nf);
    }
    let sql = format!("DELETE FROM {};", quote_ident(table_name));
    debug_sql(&sql);
    conn.execute_batch(&sql).expect("knot runtime: failed to delete rows");
}

fn delete_child_table(conn: &rusqlite::Connection, parent_table: &str, nf: &NestedField) {
    let child_table = format!("{}__{}", parent_table, nf.name);
    // Recurse to delete grandchildren first
    for grandchild in &nf.nested {
        delete_child_table(conn, &child_table, grandchild);
    }
    let sql = format!("DELETE FROM {};", quote_ident(&child_table));
    debug_sql(&sql);
    conn.execute_batch(&sql).expect("knot runtime: failed to delete child rows");
}

/// Insert rows into a record table and its child tables.
fn write_record_rows(
    conn: &rusqlite::Connection,
    table_name: &str,
    schema: &RecordSchema,
    rows: &[*mut Value],
) {
    if rows.is_empty() {
        return;
    }

    let table = quote_ident(table_name);
    let has_children = !schema.nested.is_empty();

    // Build INSERT for scalar columns only
    let col_names: Vec<String> = schema.columns.iter().map(|c| quote_ident(&c.name)).collect();
    if col_names.is_empty() && !has_children {
        return;
    }

    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("?{}", i)).collect();

    // For tables with children, we need the _id back
    let insert_sql = if has_children && !col_names.is_empty() {
        format!(
            "INSERT INTO {} ({}) VALUES ({});",
            table, col_names.join(", "), placeholders.join(", ")
        )
    } else if has_children {
        // No scalar columns, just get an _id
        format!("INSERT INTO {} DEFAULT VALUES;", table)
    } else {
        format!(
            "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
            table, col_names.join(", "), placeholders.join(", ")
        )
    };
    debug_sql(&insert_sql);

    let mut stmt = conn.prepare_cached(&insert_sql).expect("knot runtime: failed to prepare insert");

    for row_ptr in rows {
        let row = unsafe { as_ref(*row_ptr) };
        let fields = match row {
            Value::Record(fields) => fields,
            _ => panic!("knot runtime: relation rows must be Records, got {}", type_name(*row_ptr)),
        };

        // Build field lookup map for O(1) access
        let field_map: HashMap<&str, *mut Value> = fields.iter().map(|f| (f.name.as_str(), f.value)).collect();

        // Build scalar params
        let params: Vec<rusqlite::types::Value> = schema.columns
            .iter()
            .map(|col| {
                let value = field_map.get(col.name.as_str())
                    .unwrap_or_else(|| panic!("knot runtime: missing field '{}' in record", col.name));
                value_to_sqlite(*value, col.ty)
            })
            .collect();

        if !params.is_empty() {
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
            stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
                panic!("knot runtime: insert error: {}", e)
            });
        } else {
            stmt.execute([]).unwrap_or_else(|e| {
                panic!("knot runtime: insert error: {}", e)
            });
        }

        // Write nested relation fields to child tables
        if has_children {
            let parent_id = conn.last_insert_rowid();
            for nf in &schema.nested {
                let child_table = format!("{}__{}", table_name, nf.name);
                let child_val = field_map.get(nf.name.as_str())
                    .copied()
                    .unwrap_or(std::ptr::null_mut());
                if !child_val.is_null() {
                    if let Value::Relation(child_rows) = unsafe { as_ref(child_val) } {
                        write_child_rows(conn, &child_table, nf, parent_id, child_rows);
                    }
                }
            }
        }
    }
}

/// Insert rows into a child table for a nested relation field.
fn write_child_rows(
    conn: &rusqlite::Connection,
    table_name: &str,
    nf: &NestedField,
    parent_id: i64,
    rows: &[*mut Value],
) {
    if rows.is_empty() {
        return;
    }

    let table = quote_ident(table_name);
    let has_children = !nf.nested.is_empty();

    let mut col_names = vec![quote_ident("_parent_id")];
    for c in &nf.columns {
        col_names.push(quote_ident(&c.name));
    }
    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("?{}", i)).collect();

    let insert_sql = format!(
        "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
        table, col_names.join(", "), placeholders.join(", ")
    );
    debug_sql(&insert_sql);

    let mut stmt = conn.prepare_cached(&insert_sql).expect("knot runtime: failed to prepare child insert");

    for row_ptr in rows {
        let row = unsafe { as_ref(*row_ptr) };
        let fields = match row {
            Value::Record(fields) => fields,
            _ => panic!("knot runtime: child rows must be Records"),
        };

        let field_map: HashMap<&str, *mut Value> = fields.iter().map(|f| (f.name.as_str(), f.value)).collect();

        let mut params: Vec<rusqlite::types::Value> = vec![
            rusqlite::types::Value::Integer(parent_id),
        ];
        for col in &nf.columns {
            let value = field_map.get(col.name.as_str())
                .unwrap_or_else(|| panic!("knot runtime: missing field '{}' in child record", col.name));
            params.push(value_to_sqlite(*value, col.ty));
        }

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
        stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
            panic!("knot runtime: child insert error: {}", e)
        });

        // Recurse for deeper nesting
        if has_children {
            let child_id = conn.last_insert_rowid();
            for grandchild in &nf.nested {
                let gc_table = format!("{}__{}", table_name, grandchild.name);
                let gc_val = field_map.get(grandchild.name.as_str())
                    .copied()
                    .unwrap_or(std::ptr::null_mut());
                if !gc_val.is_null() {
                    if let Value::Relation(gc_rows) = unsafe { as_ref(gc_val) } {
                        write_child_rows(conn, &gc_table, grandchild, child_id, gc_rows);
                    }
                }
            }
        }
    }
}

/// Write a relation to a source (replaces all rows).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_write(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    relation: *mut Value,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: source_write expects a Relation, got {}", type_name(relation)),
    };

    // Delete all existing rows and insert new ones in a transaction
    db_ref
        .conn
        .execute_batch("BEGIN;")
        .expect("knot runtime: failed to begin transaction");

    let table_name = format!("_knot_{}", name);

    if is_adt_schema(schema) {
        let table = quote_ident(&table_name);
        let delete_sql = format!("DELETE FROM {};", table);
        debug_sql(&delete_sql);
        db_ref.conn.execute_batch(&delete_sql)
            .expect("knot runtime: failed to delete rows");

        let adt = parse_adt_schema(schema);
        if !rows.is_empty() {
            let mut col_names = vec![quote_ident("_tag")];
            for f in &adt.all_fields {
                col_names.push(quote_ident(&f.name));
            }
            let placeholders: Vec<String> = (1..=col_names.len())
                .map(|i| format!("?{}", i))
                .collect();
            let insert_sql = format!(
                "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
                table,
                col_names.join(", "),
                placeholders.join(", ")
            );
            debug_sql(&insert_sql);

            let mut stmt = db_ref
                .conn
                .prepare_cached(&insert_sql)
                .expect("knot runtime: failed to prepare insert");

            for row_ptr in rows {
                let params = adt_row_to_params(*row_ptr, &adt);
                let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                    params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
                    panic!("knot runtime: insert error: {}", e)
                });
            }
        }
    } else {
        let rec = parse_record_schema(schema);
        // Delete child tables first (deepest first), then parent
        delete_record_table(&db_ref.conn, &table_name, &rec);
        // Insert all rows
        write_record_rows(&db_ref.conn, &table_name, &rec, rows);
    }

    db_ref
        .conn
        .execute_batch("COMMIT;")
        .expect("knot runtime: failed to commit transaction");
}

/// Append rows to a source relation (INSERT only, no DELETE).
/// Used when the compiler detects `set *rel = union *rel <new_rows>`.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_append(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    relation: *mut Value,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: source_append expects a Relation, got {}",
            type_name(relation)
        ),
    };

    if rows.is_empty() {
        return;
    }

    let table = quote_ident(&format!("_knot_{}", name));

    db_ref
        .conn
        .execute_batch("BEGIN;")
        .expect("knot runtime: failed to begin transaction");

    if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);
        let mut col_names = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            col_names.push(quote_ident(&f.name));
        }
        let placeholders: Vec<String> = (1..=col_names.len())
            .map(|i| format!("?{}", i))
            .collect();
        let insert_sql = format!(
            "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
            table,
            col_names.join(", "),
            placeholders.join(", ")
        );
        debug_sql(&insert_sql);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&insert_sql)
            .expect("knot runtime: failed to prepare insert");

        for row_ptr in rows {
            let params = adt_row_to_params(*row_ptr, &adt);
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
            stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
                panic!("knot runtime: insert error: {}", e)
            });
        }
    } else {
        let rec = parse_record_schema(schema);
        write_record_rows(&db_ref.conn, &format!("_knot_{}", name), &rec, rows);
    }

    db_ref
        .conn
        .execute_batch("COMMIT;")
        .expect("knot runtime: failed to commit transaction");
}

/// Diff-based write: compute minimal INSERT/DELETE against the existing table.
/// Used when the value expression reads from the same source relation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_diff_write(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    relation: *mut Value,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };

    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: source_diff_write expects a Relation, got {}",
            type_name(relation)
        ),
    };

    let table = quote_ident(&format!("_knot_{}", name));
    let temp = quote_ident(&format!("_knot_{}_new", name));

    db_ref
        .conn
        .execute_batch("BEGIN;")
        .expect("knot runtime: failed to begin transaction");

    if is_adt_schema(schema) {
        let adt = parse_adt_schema(schema);

        // 1. Create temp table with ADT columns
        let mut col_defs = vec![format!("{} TEXT NOT NULL", quote_ident("_tag"))];
        let mut col_names = vec![quote_ident("_tag")];
        for f in &adt.all_fields {
            col_defs.push(format!("{} {}", quote_ident(&f.name), sql_type(f.ty)));
            col_names.push(quote_ident(&f.name));
        }
        let create_temp = format!(
            "CREATE TEMP TABLE {} ({});",
            temp,
            col_defs.join(", ")
        );
        debug_sql(&create_temp);
        db_ref
            .conn
            .execute_batch(&create_temp)
            .expect("knot runtime: failed to create temp table");

        // 2. Insert new rows into temp
        if !rows.is_empty() {
            let placeholders: Vec<String> = (1..=col_names.len())
                .map(|i| format!("?{}", i))
                .collect();
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({});",
                temp,
                col_names.join(", "),
                placeholders.join(", ")
            );
            debug_sql(&insert_sql);
            let mut stmt = db_ref
                .conn
                .prepare_cached(&insert_sql)
                .expect("knot runtime: failed to prepare temp insert");

            for row_ptr in rows {
                let params = adt_row_to_params(*row_ptr, &adt);
                let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                    params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
                    panic!("knot runtime: temp insert error: {}", e)
                });
            }
        }

        // 3. DELETE rows from main not in temp (use COALESCE for NULL comparison)
        let match_conds: Vec<String> = col_names
            .iter()
            .map(|c| {
                format!(
                    "COALESCE({t}.{c}, '') = COALESCE({m}.{c}, '')",
                    t = temp,
                    m = table,
                    c = c
                )
            })
            .collect();
        let delete_sql = format!(
            "DELETE FROM {} WHERE NOT EXISTS (SELECT 1 FROM {} WHERE {});",
            table,
            temp,
            match_conds.join(" AND ")
        );
        debug_sql(&delete_sql);
        db_ref
            .conn
            .execute_batch(&delete_sql)
            .expect("knot runtime: failed to delete removed rows");

        // 4. INSERT rows from temp not in main
        let insert_new_sql = format!(
            "INSERT OR IGNORE INTO {} ({}) SELECT {} FROM {};",
            table,
            col_names.join(", "),
            col_names.join(", "),
            temp
        );
        debug_sql(&insert_new_sql);
        db_ref
            .conn
            .execute_batch(&insert_new_sql)
            .expect("knot runtime: failed to insert new rows");
    } else {
        let cols = parse_schema(schema);

        // 1. Create temp table with same schema
        let col_defs: Vec<String> = cols
            .iter()
            .map(|c| format!("{} {}", quote_ident(&c.name), sql_type(c.ty)))
            .collect();
        let create_temp = format!(
            "CREATE TEMP TABLE {} ({});",
            temp,
            col_defs.join(", ")
        );
        debug_sql(&create_temp);
        db_ref
            .conn
            .execute_batch(&create_temp)
            .expect("knot runtime: failed to create temp table");

        // 2. Insert all new rows into temp
        if !cols.is_empty() && !rows.is_empty() {
            let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
            let placeholders: Vec<String> = cols
                .iter()
                .enumerate()
                .map(|(i, _)| format!("?{}", i + 1))
                .collect();
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({});",
                temp,
                col_names.join(", "),
                placeholders.join(", ")
            );
            debug_sql(&insert_sql);

            let mut stmt = db_ref
                .conn
                .prepare_cached(&insert_sql)
                .expect("knot runtime: failed to prepare temp insert");

            for row_ptr in rows {
                let row = unsafe { as_ref(*row_ptr) };
                match row {
                    Value::Record(fields) => {
                        let params: Vec<rusqlite::types::Value> = cols
                            .iter()
                            .map(|col| {
                                let field = fields
                                    .iter()
                                    .find(|f| f.name == col.name)
                                    .unwrap_or_else(|| {
                                        panic!(
                                            "knot runtime: missing field '{}' in record",
                                            col.name
                                        )
                                    });
                                value_to_sqlite(field.value, col.ty)
                            })
                            .collect();
                        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                            params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();
                        stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
                            panic!("knot runtime: temp insert error: {}", e)
                        });
                    }
                    _ => panic!(
                        "knot runtime: relation rows must be Records, got {}",
                        type_name(*row_ptr)
                    ),
                }
            }
        }

        // 3. DELETE rows from main that are not in temp
        if !cols.is_empty() {
            let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
            let match_conds: Vec<String> = cols
                .iter()
                .map(|c| {
                    format!(
                        "{t}.{c} = {m}.{c}",
                        t = temp,
                        m = table,
                        c = quote_ident(&c.name)
                    )
                })
                .collect();
            let delete_sql = format!(
                "DELETE FROM {} WHERE NOT EXISTS (SELECT 1 FROM {} WHERE {});",
                table,
                temp,
                match_conds.join(" AND ")
            );
            debug_sql(&delete_sql);
            db_ref
                .conn
                .execute_batch(&delete_sql)
                .expect("knot runtime: failed to delete removed rows");

            // 4. INSERT rows from temp that are not in main
            let insert_new_sql = format!(
                "INSERT OR IGNORE INTO {} ({}) SELECT {} FROM {};",
                table,
                col_names.join(", "),
                col_names.join(", "),
                temp
            );
            debug_sql(&insert_new_sql);
            db_ref
                .conn
                .execute_batch(&insert_new_sql)
                .expect("knot runtime: failed to insert new rows");
        }
    }

    // 5. Drop temp table
    let drop_temp = format!("DROP TABLE {};", temp);
    debug_sql(&drop_temp);
    db_ref
        .conn
        .execute_batch(&drop_temp)
        .expect("knot runtime: failed to drop temp table");


    db_ref
        .conn
        .execute_batch("COMMIT;")
        .expect("knot runtime: failed to commit transaction");
}

/// DELETE rows that don't match a WHERE condition.
/// Used for `set *rel = do { t <- *rel; where cond; yield t }`.
/// The where_clause is the *keep* condition; rows NOT matching are deleted.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_delete_where(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    where_ptr: *const u8,
    where_len: usize,
    params: *mut Value,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let where_clause = unsafe { str_from_raw(where_ptr, where_len) };

    // Auto-index columns used in the WHERE clause
    let table = format!("_knot_{}", name);
    db_ref.ensure_indexes_for_where(&table, where_clause);

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: delete_where params must be a Relation, got {}",
            type_name(params)
        ),
    };

    let sql = format!(
        "DELETE FROM {} WHERE NOT ({});",
        quote_ident(&table),
        where_clause
    );

    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    debug_sql_params(&sql, &sql_params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    db_ref
        .conn
        .execute(&sql, param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: delete_where error: {}\n  SQL: {}", e, sql));
}

/// UPDATE rows matching a WHERE condition with new field values.
/// Used for `set *rel = do { t <- *rel; yield (if cond then {t | ...} else t) }`.
/// Params relation contains SET values first, then WHERE values.
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_update_where(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    set_clause_ptr: *const u8,
    set_clause_len: usize,
    where_ptr: *const u8,
    where_len: usize,
    params: *mut Value,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let set_clause = unsafe { str_from_raw(set_clause_ptr, set_clause_len) };
    let where_clause = unsafe { str_from_raw(where_ptr, where_len) };

    // Auto-index columns used in the WHERE clause
    let table = format!("_knot_{}", name);
    db_ref.ensure_indexes_for_where(&table, where_clause);

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: update_where params must be a Relation, got {}",
            type_name(params)
        ),
    };

    let sql = format!(
        "UPDATE OR REPLACE {} SET {} WHERE {};",
        quote_ident(&table),
        set_clause,
        where_clause
    );

    let sql_params: Vec<rusqlite::types::Value> =
        param_values.iter().map(|v| value_to_sql_param(*v)).collect();
    debug_sql_params(&sql, &sql_params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        sql_params.iter().map(|p| p as &dyn rusqlite::types::ToSql).collect();

    db_ref
        .conn
        .execute(&sql, param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: update_where error: {}\n  SQL: {}", e, sql));
}

fn value_to_sql_param(v: *mut Value) -> rusqlite::types::Value {
    if v.is_null() {
        return rusqlite::types::Value::Null;
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => match n.to_i64() {
            Some(i) => rusqlite::types::Value::Integer(i),
            None => rusqlite::types::Value::Blob(n.to_string().into_bytes()),
        },
        Value::Float(n) => rusqlite::types::Value::Real(*n),
        Value::Text(s) => rusqlite::types::Value::Text(s.clone()),
        Value::Bool(b) => rusqlite::types::Value::Integer(*b as i64),
        Value::Bytes(b) => rusqlite::types::Value::Blob(b.clone()),
        Value::Constructor(tag, _) => rusqlite::types::Value::Text(tag.clone()),
        Value::Relation(_) | Value::Record(_) => {
            rusqlite::types::Value::Text(value_to_json(v))
        }
        _ => panic!(
            "knot runtime: cannot use {} as SQL parameter",
            brief_value(v)
        ),
    }
}

fn value_to_sqlite(v: *mut Value, ty: ColType) -> rusqlite::types::Value {
    if v.is_null() {
        return rusqlite::types::Value::Null;
    }
    match (unsafe { as_ref(v) }, ty) {
        (Value::Int(n), _) => match n.to_i64() {
            Some(i) => rusqlite::types::Value::Integer(i),
            None => rusqlite::types::Value::Blob(n.to_string().into_bytes()),
        },
        (Value::Float(n), _) => rusqlite::types::Value::Real(*n),
        (Value::Text(s), _) => rusqlite::types::Value::Text(s.clone()),
        (Value::Bool(b), _) => rusqlite::types::Value::Integer(*b as i64),
        (Value::Bytes(b), _) => rusqlite::types::Value::Blob(b.clone()),
        (Value::Constructor(tag, _), ColType::Tag) => {
            rusqlite::types::Value::Text(tag.clone())
        }
        (Value::Constructor(tag, _), _) => rusqlite::types::Value::Text(tag.clone()),
        _ => panic!("knot runtime: cannot convert {} to SQL", brief_value(v)),
    }
}

// ── Temporal queries (history tracking) ───────────────────────────

/// Return current time as milliseconds since Unix epoch.
#[unsafe(no_mangle)]
pub extern "C" fn knot_now() -> *mut Value {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    knot_value_int(ms)
}

// ── Random number generation ─────────────────────────────────────

/// Return a random integer in [0, bound).
#[unsafe(no_mangle)]
pub extern "C" fn knot_random_int(bound: *mut Value) -> *mut Value {
    let n = match unsafe { as_ref(bound) } {
        Value::Int(i) => i.to_u64().expect("knot runtime: randomInt bound must be positive"),
        _ => panic!(
            "knot runtime: randomInt expected Int, got {}",
            type_name(bound)
        ),
    };
    assert!(n > 0, "knot runtime: randomInt bound must be > 0");
    // Use getrandom for platform-independent cryptographic randomness
    let mut buf = [0u8; 8];
    getrandom::fill(&mut buf).expect("knot runtime: failed to get random bytes");
    let raw = u64::from_le_bytes(buf);
    let result = (raw % n) as i64;
    knot_value_int(result)
}

/// Return a random Float in [0.0, 1.0).
#[unsafe(no_mangle)]
pub extern "C" fn knot_random_float() -> *mut Value {
    let mut buf = [0u8; 8];
    getrandom::fill(&mut buf).expect("knot runtime: failed to get random bytes");
    let raw = u64::from_le_bytes(buf);
    // Divide by u64::MAX+1 to get [0.0, 1.0)
    let result = (raw as f64) / ((u64::MAX as f64) + 1.0);
    alloc(Value::Float(result))
}

/// Initialize a history table for a source with `with history`.
/// Creates `_knot_{name}_history` with the same columns plus `_knot_valid_from`
/// and `_knot_valid_to` timestamp columns.
#[unsafe(no_mangle)]
pub extern "C" fn knot_history_init(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let cols = parse_schema(schema);

    let history_table = quote_ident(&format!("_knot_{}_history", name));
    let mut col_defs: Vec<String> = cols
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.name), sql_type(c.ty)))
        .collect();
    col_defs.push("\"_knot_valid_from\" INTEGER NOT NULL".to_string());
    col_defs.push("\"_knot_valid_to\" INTEGER".to_string());

    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({});",
        history_table,
        col_defs.join(", ")
    );
    debug_sql(&sql);
    db_ref.conn.execute_batch(&sql).unwrap_or_else(|e| {
        panic!(
            "knot runtime: failed to create history table for '{}': {}",
            name, e
        )
    });

    // Index on valid_from/valid_to for efficient temporal queries
    let idx_sql = format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} (\"_knot_valid_from\", \"_knot_valid_to\");",
        quote_ident(&format!("_knot_{}_history_time", name)),
        history_table
    );
    debug_sql(&idx_sql);
    let _ = db_ref.conn.execute_batch(&idx_sql);
}

/// Snapshot the current state of a source into its history table.
/// Called before each write to a history-enabled source.
/// Closes out any open history rows (valid_to IS NULL) and inserts
/// the current state with valid_from = now and valid_to = NULL.
#[unsafe(no_mangle)]
pub extern "C" fn knot_history_snapshot(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let cols = parse_schema(schema);

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let table = quote_ident(&format!("_knot_{}", name));
    let history_table = quote_ident(&format!("_knot_{}_history", name));

    // Close out currently-open history rows
    let close_sql = format!(
        "UPDATE {} SET \"_knot_valid_to\" = ?1 WHERE \"_knot_valid_to\" IS NULL;",
        history_table
    );
    debug_sql(&close_sql);
    db_ref
        .conn
        .execute(&close_sql, rusqlite::params![now_ms])
        .unwrap_or_else(|e| {
            panic!(
                "knot runtime: failed to close history rows for '{}': {}",
                name, e
            )
        });

    // Insert current state as new open rows
    if !cols.is_empty() {
        let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
        let insert_sql = format!(
            "INSERT INTO {} ({}, \"_knot_valid_from\", \"_knot_valid_to\") SELECT {}, ?1, NULL FROM {};",
            history_table,
            col_names.join(", "),
            col_names.join(", "),
            table
        );
        debug_sql(&insert_sql);
        db_ref
            .conn
            .execute(&insert_sql, rusqlite::params![now_ms])
            .unwrap_or_else(|e| {
                panic!(
                    "knot runtime: failed to snapshot history for '{}': {}",
                    name, e
                )
            });
    }
}

/// Read a source relation at a specific point in time.
/// Returns the rows that were valid at the given timestamp (milliseconds since epoch).
#[unsafe(no_mangle)]
pub extern "C" fn knot_source_read_at(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    timestamp: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let cols = parse_schema(schema);

    let ts = match unsafe { as_ref(timestamp) } {
        Value::Int(n) => n.to_i64().expect("knot runtime: timestamp too large for i64"),
        _ => panic!(
            "knot runtime: temporal query timestamp must be Int, got {}",
            type_name(timestamp)
        ),
    };

    let history_table = quote_ident(&format!("_knot_{}_history", name));
    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();

    let sql = format!(
        "SELECT {} FROM {} WHERE \"_knot_valid_from\" <= ?1 AND (\"_knot_valid_to\" IS NULL OR \"_knot_valid_to\" > ?1)",
        if col_names.is_empty() {
            "1".to_string()
        } else {
            col_names.join(", ")
        },
        history_table
    );

    debug_sql(&sql);
    let mut stmt = db_ref
        .conn
        .prepare_cached(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: temporal query error: {}", e));

    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query(rusqlite::params![ts])
        .unwrap_or_else(|e| panic!("knot runtime: temporal query exec error: {}", e));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: temporal row fetch error: {}", e))
    {
        let record = knot_record_empty(cols.len());
        for (i, col) in cols.iter().enumerate() {
            let val = read_sql_column(row, i, col.ty);
            let field_name = col.name.as_bytes();
            knot_record_set_field(record, field_name.as_ptr(), field_name.len(), val);
        }
        rows.push(record);
    }

    alloc(Value::Relation(rows))
}

// ── Subset constraints ────────────────────────────────────────────

/// Register a subset constraint. Called at program startup.
/// Empty field strings mean "no field" (whole relation).
#[unsafe(no_mangle)]
pub extern "C" fn knot_constraint_register(
    db: *mut c_void,
    sub_rel_ptr: *const u8,
    sub_rel_len: usize,
    sub_field_ptr: *const u8,
    sub_field_len: usize,
    sup_rel_ptr: *const u8,
    sup_rel_len: usize,
    sup_field_ptr: *const u8,
    sup_field_len: usize,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let sub_rel = unsafe { str_from_raw(sub_rel_ptr, sub_rel_len) }.to_string();
    let sub_field_str = unsafe { str_from_raw(sub_field_ptr, sub_field_len) };
    let sub_field = if sub_field_str.is_empty() {
        None
    } else {
        Some(sub_field_str.to_string())
    };
    let sup_rel = unsafe { str_from_raw(sup_rel_ptr, sup_rel_len) }.to_string();
    let sup_field_str = unsafe { str_from_raw(sup_field_ptr, sup_field_len) };
    let sup_field = if sup_field_str.is_empty() {
        None
    } else {
        Some(sup_field_str.to_string())
    };

    // Enforce constraint via SQL indexes and triggers
    match (&sub_field, &sup_field) {
        // Uniqueness: *rel <= *rel.field — index + trigger
        (None, Some(sf)) if sub_rel == sup_rel => {
            let table = quote_ident(&format!("_knot_{}", sub_rel));
            let col = quote_ident(sf);

            // Index for efficient lookups
            let idx_sql = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                quote_ident(&format!("_knot_{}_idx_{}", sub_rel, sf)),
                table,
                col,
            );
            debug_sql(&idx_sql);
            let _ = db_ref.conn.execute_batch(&idx_sql);

            // Trigger: reject INSERT if value already exists
            let msg = format!(
                "uniqueness constraint violated: *{} <= *{}.{}",
                sub_rel, sup_rel, sf
            );
            let trigger_sql = format!(
                "CREATE TRIGGER IF NOT EXISTS {trg} \
                 BEFORE INSERT ON {table} \
                 FOR EACH ROW \
                 WHEN EXISTS (SELECT 1 FROM {table} WHERE {col} = NEW.{col}) \
                 BEGIN SELECT RAISE(ABORT, '{msg}'); END;",
                trg = quote_ident(&format!("_knot_uniq_{}_{}_ins", sub_rel, sf)),
                table = table,
                col = col,
                msg = msg,
            );
            debug_sql(&trigger_sql);
            db_ref.conn.execute_batch(&trigger_sql)
                .expect("knot runtime: failed to create uniqueness trigger");
        }
        // Referential integrity: *sub.sf <= *sup.spf — indexes + triggers
        (Some(sf), Some(spf)) => {
            // Indexes for efficient lookups
            let sub_idx = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                quote_ident(&format!("_knot_{}_idx_{}", sub_rel, sf)),
                quote_ident(&format!("_knot_{}", sub_rel)),
                quote_ident(sf),
            );
            debug_sql(&sub_idx);
            let _ = db_ref.conn.execute_batch(&sub_idx);

            let sup_idx = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({});",
                quote_ident(&format!("_knot_{}_idx_{}", sup_rel, spf)),
                quote_ident(&format!("_knot_{}", sup_rel)),
                quote_ident(spf),
            );
            debug_sql(&sup_idx);
            let _ = db_ref.conn.execute_batch(&sup_idx);

            let sub_table = quote_ident(&format!("_knot_{}", sub_rel));
            let sup_table = quote_ident(&format!("_knot_{}", sup_rel));
            let sub_col = quote_ident(sf);
            let sup_col = quote_ident(spf);
            let msg = format!(
                "subset constraint violated: *{}.{} <= *{}.{}",
                sub_rel, sf, sup_rel, spf
            );

            // Trigger: reject INSERT into sub if value doesn't exist in sup
            let insert_trigger = format!(
                "CREATE TRIGGER IF NOT EXISTS {trg} \
                 BEFORE INSERT ON {sub_table} \
                 FOR EACH ROW \
                 WHEN NOT EXISTS (SELECT 1 FROM {sup_table} WHERE {sup_col} = NEW.{sub_col}) \
                 BEGIN SELECT RAISE(ABORT, '{msg}'); END;",
                trg = quote_ident(&format!("_knot_fk_{}_{}_ins", sub_rel, sf)),
                sub_table = sub_table,
                sup_table = sup_table,
                sub_col = sub_col,
                sup_col = sup_col,
                msg = msg,
            );
            debug_sql(&insert_trigger);
            db_ref.conn.execute_batch(&insert_trigger)
                .expect("knot runtime: failed to create insert trigger");
        }
        _ => {}
    }
}

// ── Atomic (transactions) ─────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_atomic_begin(db: *mut c_void) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let depth = db_ref.atomic_depth.get() + 1;
    db_ref.atomic_depth.set(depth);
    db_ref
        .conn
        .execute_batch(&format!("SAVEPOINT knot_atomic_{depth};"))
        .expect("knot runtime: failed to begin atomic");
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_atomic_commit(db: *mut c_void) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let depth = db_ref.atomic_depth.get();
    db_ref
        .conn
        .execute_batch(&format!("RELEASE SAVEPOINT knot_atomic_{depth};"))
        .expect("knot runtime: failed to commit atomic");
    db_ref.atomic_depth.set(depth - 1);
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_atomic_rollback(db: *mut c_void) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let depth = db_ref.atomic_depth.get();
    db_ref
        .conn
        .execute_batch(&format!("ROLLBACK TO SAVEPOINT knot_atomic_{depth};"))
        .expect("knot runtime: failed to rollback atomic");
    db_ref.atomic_depth.set(depth - 1);
}

// ── Record update ─────────────────────────────────────────────────

/// Create a new record by copying `base` and overriding fields.
/// This implements `{base | field1: val1, field2: val2}`.
#[unsafe(no_mangle)]
pub extern "C" fn knot_record_update(base: *mut Value) -> *mut Value {
    match unsafe { as_ref(base) } {
        Value::Record(fields) => {
            let new_fields: Vec<RecordField> = fields
                .iter()
                .map(|f| RecordField {
                    name: f.name.clone(),
                    value: f.value,
                })
                .collect();
            alloc(Value::Record(new_fields))
        }
        _ => panic!("knot runtime: record update requires a Record base, got {}", type_name(base)),
    }
}

// ── View operations ──────────────────────────────────────────────

/// Read through a view: SELECT only view columns WHERE constant columns match.
/// `view_schema` contains only the columns visible in the view (source columns).
/// `filter_where` is the WHERE clause for constant column filtering.
/// `filter_params` is a flat relation of values for the WHERE placeholders.
#[unsafe(no_mangle)]
pub extern "C" fn knot_view_read(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    filter_ptr: *const u8,
    filter_len: usize,
    filter_params: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let view_schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let filter_where = unsafe { str_from_raw(filter_ptr, filter_len) };
    let cols = parse_schema(view_schema);

    let filter_values = match unsafe { as_ref(filter_params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: view_read filter_params must be Relation, got {}",
            type_name(filter_params)
        ),
    };

    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
    let sql = if filter_where.is_empty() {
        format!(
            "SELECT {} FROM {}",
            if col_names.is_empty() {
                "1".to_string()
            } else {
                col_names.join(", ")
            },
            quote_ident(&format!("_knot_{}", name))
        )
    } else {
        format!(
            "SELECT {} FROM {} WHERE {}",
            if col_names.is_empty() {
                "1".to_string()
            } else {
                col_names.join(", ")
            },
            quote_ident(&format!("_knot_{}", name)),
            filter_where
        )
    };

    let sql_params: Vec<rusqlite::types::Value> = filter_values
        .iter()
        .map(|v| value_to_sql_param(*v))
        .collect();
    debug_sql_params(&sql, &sql_params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = sql_params
        .iter()
        .map(|p| p as &dyn rusqlite::types::ToSql)
        .collect();

    let mut stmt = db_ref
        .conn
        .prepare_cached(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: view_read query error: {}", e));

    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query(param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: view_read exec error: {}", e));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: view_read fetch error: {}", e))
    {
        let record = knot_record_empty(cols.len());
        for (i, col) in cols.iter().enumerate() {
            let val = read_sql_column(row, i, col.ty);
            let name_bytes = col.name.as_bytes();
            knot_record_set_field(record, name_bytes.as_ptr(), name_bytes.len(), val);
        }
        rows.push(record);
    }

    alloc(Value::Relation(rows))
}

/// Read a view at a specific point in time, combining temporal and view filtering.
/// Queries the underlying source's history table with both temporal and constant column filters.
#[unsafe(no_mangle)]
pub extern "C" fn knot_view_read_at(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    filter_ptr: *const u8,
    filter_len: usize,
    filter_params: *mut Value,
    timestamp: *mut Value,
) -> *mut Value {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let view_schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let filter_where = unsafe { str_from_raw(filter_ptr, filter_len) };
    let cols = parse_schema(view_schema);

    let ts = match unsafe { as_ref(timestamp) } {
        Value::Int(n) => n.to_i64().expect("knot runtime: timestamp too large for i64"),
        _ => panic!(
            "knot runtime: temporal query timestamp must be Int, got {}",
            type_name(timestamp)
        ),
    };

    let filter_values = match unsafe { as_ref(filter_params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: view_read_at filter_params must be Relation, got {}",
            type_name(filter_params)
        ),
    };

    let history_table = quote_ident(&format!("_knot_{}_history", name));
    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();

    // Temporal condition uses the first parameter slot
    let temporal_cond =
        "\"_knot_valid_from\" <= ?1 AND (\"_knot_valid_to\" IS NULL OR \"_knot_valid_to\" > ?1)";

    // View filter params are offset by 1 (timestamp takes ?1)
    let view_filter = if filter_where.is_empty() {
        String::new()
    } else {
        // Rewrite ?1, ?2, ... to ?2, ?3, ... to account for timestamp param
        let mut rewritten = filter_where.to_string();
        // Replace from highest to lowest to avoid ?1 -> ?2 then ?2 -> ?3
        for i in (1..=filter_values.len()).rev() {
            rewritten = rewritten.replace(
                &format!("?{}", i),
                &format!("?{}", i + 1),
            );
        }
        format!(" AND {}", rewritten)
    };

    let sql = format!(
        "SELECT {} FROM {} WHERE {}{}",
        if col_names.is_empty() {
            "1".to_string()
        } else {
            col_names.join(", ")
        },
        history_table,
        temporal_cond,
        view_filter,
    );

    // Build params: timestamp first, then view filter values
    let mut sql_params: Vec<rusqlite::types::Value> = Vec::new();
    sql_params.push(rusqlite::types::Value::Integer(ts));
    for v in filter_values.iter() {
        sql_params.push(value_to_sql_param(*v));
    }

    debug_sql_params(&sql, &sql_params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = sql_params
        .iter()
        .map(|p| p as &dyn rusqlite::types::ToSql)
        .collect();

    let mut stmt = db_ref
        .conn
        .prepare_cached(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: view_read_at query error: {}", e));

    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query(param_refs.as_slice())
        .unwrap_or_else(|e| panic!("knot runtime: view_read_at exec error: {}", e));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: view_read_at fetch error: {}", e))
    {
        let record = knot_record_empty(cols.len());
        for (i, col) in cols.iter().enumerate() {
            let val = read_sql_column(row, i, col.ty);
            let name_bytes = col.name.as_bytes();
            knot_record_set_field(record, name_bytes.as_ptr(), name_bytes.len(), val);
        }
        rows.push(record);
    }

    alloc(Value::Relation(rows))
}

/// Add fields from `extra_fields` record to each row in `relation`.
/// Returns a new relation with augmented rows.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_add_fields(
    relation: *mut Value,
    extra_fields: *mut Value,
) -> *mut Value {
    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: relation_add_fields expects Relation, got {}",
            type_name(relation)
        ),
    };
    let extra = match unsafe { as_ref(extra_fields) } {
        Value::Record(fields) => fields,
        _ => panic!(
            "knot runtime: relation_add_fields extra must be Record, got {}",
            type_name(extra_fields)
        ),
    };

    let new_rows: Vec<*mut Value> = rows
        .iter()
        .map(|row_ptr| {
            let updated = knot_record_update(*row_ptr);
            for field in extra {
                let name_bytes = field.name.as_bytes();
                knot_record_set_field(
                    updated,
                    name_bytes.as_ptr(),
                    name_bytes.len(),
                    field.value,
                );
            }
            updated
        })
        .collect();

    alloc(Value::Relation(new_rows))
}

/// Write through a view: delete rows matching filter, insert new rows.
/// `filter_params` is a flat relation of values for the WHERE clause placeholders.
/// `new_relation` has ALL columns (including constants that were added back).
#[unsafe(no_mangle)]
pub extern "C" fn knot_view_write(
    db: *mut c_void,
    name_ptr: *const u8,
    name_len: usize,
    schema_ptr: *const u8,
    schema_len: usize,
    filter_ptr: *const u8,
    filter_len: usize,
    filter_params: *mut Value,
    new_relation: *mut Value,
) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let name = unsafe { str_from_raw(name_ptr, name_len) };
    let schema = unsafe { str_from_raw(schema_ptr, schema_len) };
    let filter_where = unsafe { str_from_raw(filter_ptr, filter_len) };
    let cols = parse_schema(schema);

    let filter_values = match unsafe { as_ref(filter_params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: view_write filter_params must be Relation, got {}",
            type_name(filter_params)
        ),
    };

    let rows = match unsafe { as_ref(new_relation) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: view_write new_relation must be Relation, got {}",
            type_name(new_relation)
        ),
    };

    let table = quote_ident(&format!("_knot_{}", name));

    db_ref
        .conn
        .execute_batch("BEGIN;")
        .expect("knot runtime: view_write begin failed");

    // 1. Delete rows matching the view's constant filter
    let delete_sql = format!("DELETE FROM {} WHERE {};", table, filter_where);
    let sql_params: Vec<rusqlite::types::Value> = filter_values
        .iter()
        .map(|v| value_to_sql_param(*v))
        .collect();
    debug_sql_params(&delete_sql, &sql_params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = sql_params
        .iter()
        .map(|p| p as &dyn rusqlite::types::ToSql)
        .collect();
    db_ref
        .conn
        .execute(&delete_sql, param_refs.as_slice())
        .unwrap_or_else(|e| {
            panic!(
                "knot runtime: view_write delete error: {}\n  SQL: {}",
                e, delete_sql
            )
        });

    // 2. Insert new rows
    if !cols.is_empty() && !rows.is_empty() {
        let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
        let placeholders: Vec<String> = cols
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", i + 1))
            .collect();
        let insert_sql = format!(
            "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
            table,
            col_names.join(", "),
            placeholders.join(", ")
        );
        debug_sql(&insert_sql);

        let mut stmt = db_ref
            .conn
            .prepare_cached(&insert_sql)
            .expect("knot runtime: view_write prepare insert failed");

        for row_ptr in rows {
            let row = unsafe { as_ref(*row_ptr) };
            match row {
                Value::Record(fields) => {
                    let params: Vec<rusqlite::types::Value> = cols
                        .iter()
                        .map(|col| {
                            let field = fields
                                .iter()
                                .find(|f| f.name == col.name)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "knot runtime: missing field '{}' in record",
                                        col.name
                                    )
                                });
                            value_to_sqlite(field.value, col.ty)
                        })
                        .collect();
                    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
                        .iter()
                        .map(|p| p as &dyn rusqlite::types::ToSql)
                        .collect();
                    stmt.execute(param_refs.as_slice()).unwrap_or_else(|e| {
                        panic!("knot runtime: view_write insert error: {}", e)
                    });
                }
                _ => panic!(
                    "knot runtime: rows must be Records, got {}",
                    type_name(*row_ptr)
                ),
            }
        }
    }

    db_ref
        .conn
        .execute_batch("COMMIT;")
        .expect("knot runtime: view_write commit failed");
}

// ── Pipe (|>) support ─────────────────────────────────────────────

/// Apply a function value to an argument: `arg |> func`
#[unsafe(no_mangle)]
pub extern "C" fn knot_value_pipe(
    db: *mut c_void,
    arg: *mut Value,
    func: *mut Value,
) -> *mut Value {
    knot_value_call(db, func, arg)
}

// ── Constructor matching ──────────────────────────────────────────

/// Check if a value is a constructor with the given tag.
#[unsafe(no_mangle)]
pub extern "C" fn knot_constructor_matches(
    v: *mut Value,
    tag_ptr: *const u8,
    tag_len: usize,
) -> i32 {
    if v.is_null() {
        return 0; // Null (nullable none) never matches a constructor tag
    }
    let tag = unsafe { str_from_raw(tag_ptr, tag_len) };
    match unsafe { as_ref(v) } {
        Value::Constructor(t, _) => (t == tag) as i32,
        _ => 0,
    }
}

/// Get the payload of a constructor value.
/// For nullable-encoded types, the value IS the payload (or null for none).
#[unsafe(no_mangle)]
pub extern "C" fn knot_constructor_payload(v: *mut Value) -> *mut Value {
    if v.is_null() {
        return v; // Nullable none: return null
    }
    match unsafe { as_ref(v) } {
        Value::Constructor(_, payload) => *payload,
        _ => panic!("knot runtime: expected Constructor, got {}", type_name(v)),
    }
}

// ── Recursive derived relations (fixpoint iteration) ──────────────

/// Iterates a body function to a fixed point for recursive derived relations.
/// `body` is a raw function pointer: `extern "C" fn(db, current) -> new_result`.
/// Starts with `initial` and calls body repeatedly until the result stabilizes.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_fixpoint(
    db: *mut c_void,
    body: *const u8,
    initial: *mut Value,
) -> *mut Value {
    let body_fn: extern "C" fn(*mut c_void, *mut Value) -> *mut Value =
        unsafe { std::mem::transmute(body) };
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    let mut current = initial;
    for _ in 0..10_000 {
        let next = body_fn(db, current);

        // Try SQL-based equality check (O(n log n) via EXCEPT)
        let equal = match (unsafe { as_ref(current) }, unsafe { as_ref(next) }) {
            (Value::Relation(curr_rows), Value::Relation(next_rows)) => {
                sql_relations_equal(&db_ref.conn, curr_rows, next_rows)
                    .unwrap_or_else(|| values_equal(current, next))
            }
            _ => values_equal(current, next),
        };

        if equal {
            return next;
        }
        current = next;
    }
    panic!("knot runtime: recursive derived relation did not converge after 10000 iterations");
}

// ── HTTP server (routes) ──────────────────────────────────────────

enum PathPart {
    Literal(String),
    Param(String, String), // (name, type)
}

struct RouteTableEntry {
    method: String,
    path_parts: Vec<PathPart>,
    constructor: String,
    body_fields: Vec<(String, String)>,
    query_fields: Vec<(String, String)>,
    response_type: String,
}

struct RouteTable {
    entries: Vec<RouteTableEntry>,
}

fn parse_descriptor(desc: &str) -> Vec<(String, String)> {
    if desc.is_empty() {
        return Vec::new();
    }
    desc.split(',')
        .map(|part| {
            let mut split = part.splitn(2, ':');
            let name = split.next().unwrap_or("").to_string();
            let ty = split.next().unwrap_or("text").to_string();
            (name, ty)
        })
        .collect()
}

fn parse_path_pattern(path: &str) -> Vec<PathPart> {
    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|seg| {
            if seg.starts_with('{') && seg.ends_with('}') {
                let inner = &seg[1..seg.len() - 1];
                let mut split = inner.splitn(2, ':');
                let name = split.next().unwrap_or("").to_string();
                let ty = split.next().unwrap_or("text").to_string();
                PathPart::Param(name, ty)
            } else {
                PathPart::Literal(seg.to_string())
            }
        })
        .collect()
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_route_table_new() -> *mut c_void {
    let table = Box::new(RouteTable {
        entries: Vec::new(),
    });
    Box::into_raw(table) as *mut c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_route_table_add(
    table: *mut c_void,
    method_ptr: *const u8,
    method_len: usize,
    path_ptr: *const u8,
    path_len: usize,
    ctor_ptr: *const u8,
    ctor_len: usize,
    body_desc_ptr: *const u8,
    body_desc_len: usize,
    query_desc_ptr: *const u8,
    query_desc_len: usize,
    resp_ptr: *const u8,
    resp_len: usize,
) {
    let table = unsafe { &mut *(table as *mut RouteTable) };
    let method = unsafe { str_from_raw(method_ptr, method_len) }.to_string();
    let path = unsafe { str_from_raw(path_ptr, path_len) };
    let ctor = unsafe { str_from_raw(ctor_ptr, ctor_len) }.to_string();
    let body_desc = unsafe { str_from_raw(body_desc_ptr, body_desc_len) };
    let query_desc = unsafe { str_from_raw(query_desc_ptr, query_desc_len) };
    let resp = unsafe { str_from_raw(resp_ptr, resp_len) }.to_string();

    table.entries.push(RouteTableEntry {
        method,
        path_parts: parse_path_pattern(path),
        constructor: ctor,
        body_fields: parse_descriptor(body_desc),
        query_fields: parse_descriptor(query_desc),
        response_type: resp,
    });
}

fn match_route<'a>(
    entries: &'a [RouteTableEntry],
    method: &str,
    path_segments: &[&str],
) -> Option<(&'a RouteTableEntry, Vec<(String, String)>)> {
    for entry in entries {
        if !entry.method.eq_ignore_ascii_case(method) {
            continue;
        }
        if entry.path_parts.len() != path_segments.len() {
            continue;
        }
        let mut params = Vec::new();
        let mut matched = true;
        for (part, seg) in entry.path_parts.iter().zip(path_segments.iter()) {
            match part {
                PathPart::Literal(lit) => {
                    if lit != seg {
                        matched = false;
                        break;
                    }
                }
                PathPart::Param(name, _ty) => {
                    params.push((name.clone(), seg.to_string()));
                }
            }
        }
        if matched {
            return Some((entry, params));
        }
    }
    None
}

fn parse_query_string(qs: &str) -> HashMap<String, String> {
    if qs.is_empty() {
        return HashMap::new();
    }
    qs.split('&')
        .filter_map(|pair| {
            let mut split = pair.splitn(2, '=');
            let key = split.next()?;
            let val = split.next().unwrap_or("");
            Some((
                url_decode(key),
                url_decode(val),
            ))
        })
        .collect()
}

fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let h = chars.next().unwrap_or(b'0');
            let l = chars.next().unwrap_or(b'0');
            let val = hex_val(h) * 16 + hex_val(l);
            result.push(val as char);
        } else if b == b'+' {
            result.push(' ');
        } else {
            result.push(b as char);
        }
    }
    result
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

/// Minimal JSON object parser. Handles {"key": value, ...} with string, number, bool, null values.
fn parse_json_object(s: &str) -> HashMap<String, String> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return HashMap::new();
    }
    let inner = &s[1..s.len() - 1];
    let mut result = HashMap::new();
    let mut rest = inner.trim();
    while !rest.is_empty() {
        // Parse key
        let key;
        if rest.starts_with('"') {
            let end = rest[1..].find('"').map(|i| i + 1).unwrap_or(rest.len());
            key = rest[1..end].to_string();
            rest = rest[end + 1..].trim();
        } else {
            break;
        }
        // Skip colon
        if rest.starts_with(':') {
            rest = rest[1..].trim();
        } else {
            break;
        }
        // Parse value
        if rest.starts_with('"') {
            let end = rest[1..].find('"').map(|i| i + 1).unwrap_or(rest.len());
            let val = rest[1..end].to_string();
            rest = rest[end + 1..].trim();
            result.insert(key, val);
        } else {
            // number, bool, null — read until comma or end
            let end = rest.find(',').unwrap_or(rest.len());
            let val = rest[..end].trim().to_string();
            rest = rest[end..].trim_start();
            result.insert(key, val);
        }
        // Skip comma
        if rest.starts_with(',') {
            rest = rest[1..].trim();
        }
    }
    result
}

fn string_to_value(s: &str, ty: &str) -> *mut Value {
    match ty {
        "int" => {
            let n: BigInt = s.parse().unwrap_or(BigInt::ZERO);
            alloc(Value::Int(n))
        }
        "float" => {
            let n: f64 = s.parse().unwrap_or(0.0);
            alloc(Value::Float(n))
        }
        "bool" => {
            let b = s == "true" || s == "True";
            alloc(Value::Bool(b))
        }
        _ => alloc(Value::Text(s.to_string())),
    }
}

const BASE64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(BASE64_CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(BASE64_CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(BASE64_CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(BASE64_CHARS[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn value_to_json(v: *mut Value) -> String {
    if v.is_null() {
        return "null".to_string();
    }
    match unsafe { as_ref(v) } {
        Value::Int(n) => n.to_string(),
        Value::Float(n) => n.to_string(),
        Value::Text(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Bytes(b) => {
            format!("\"{}\"", base64_encode(b))
        }
        Value::Unit => "{}".to_string(),
        Value::Record(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| format!("\"{}\":{}", f.name, value_to_json(f.value)))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        Value::Relation(rows) => {
            let parts: Vec<String> = rows.iter().map(|r| value_to_json(*r)).collect();
            format!("[{}]", parts.join(","))
        }
        Value::Constructor(tag, payload) => {
            let p = value_to_json(*payload);
            format!("{{\"tag\":\"{}\",\"value\":{}}}", tag, p)
        }
        Value::Function(_, _, src) => format!("\"<function: {}>\"", src),
    }
}

/// Identity function used as the `respond` field in route constructors.
/// At runtime, respond just passes through the value unchanged — the type system
/// uses it to check that each handler branch returns the declared response type.
extern "C" fn respond_identity(
    _db: *mut c_void,
    _env: *mut Value,
    arg: *mut Value,
) -> *mut Value {
    arg
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_http_listen(
    db: *mut c_void,
    port_val: *mut Value,
    route_table: *mut c_void,
    handler: *mut Value,
) -> *mut Value {
    let port = match unsafe { as_ref(port_val) } {
        Value::Int(n) => n.to_u16().expect("knot runtime: port number out of range"),
        _ => panic!("knot runtime: listen expects Int port, got {}", type_name(port_val)),
    };
    let table = unsafe { &*(route_table as *mut RouteTable) };
    let addr = format!("0.0.0.0:{}", port);
    let server = tiny_http::Server::http(&addr)
        .unwrap_or_else(|e| panic!("knot runtime: failed to start HTTP server on {}: {}", addr, e));
    eprintln!("Knot HTTP server listening on http://0.0.0.0:{}", port);

    // Mark the arena so that per-request allocations can be freed.
    // Everything allocated before this point (handler, env, etc.) survives.
    let arena_mark = knot_arena_mark();

    loop {
        knot_arena_reset_to(arena_mark);

        let mut request = match server.recv() {
            Ok(req) => req,
            Err(e) => {
                eprintln!("knot runtime: error receiving request: {}", e);
                continue;
            }
        };

        let method = request.method().as_str().to_string();
        let url = request.url().to_string();
        let (path, query_string) = match url.split_once('?') {
            Some((p, q)) => (p, q),
            None => (url.as_str(), ""),
        };
        let path_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        match match_route(&table.entries, &method, &path_segments) {
            Some((entry, path_params)) => {
                // Build record from path params, query params, and body
                let mut fields: Vec<RecordField> = Vec::new();

                // Path params
                for (name, val) in &path_params {
                    let ty = entry
                        .path_parts
                        .iter()
                        .find_map(|p| match p {
                            PathPart::Param(n, t) if n == name => Some(t.as_str()),
                            _ => None,
                        })
                        .unwrap_or("text");
                    fields.push(RecordField {
                        name: name.clone(),
                        value: string_to_value(val, ty),
                    });
                }

                // Query params
                let qs = parse_query_string(query_string);
                for (qname, qty) in &entry.query_fields {
                    let val = qs
                        .get(qname)
                        .map(|v| v.as_str())
                        .unwrap_or("");
                    fields.push(RecordField {
                        name: qname.clone(),
                        value: string_to_value(val, qty),
                    });
                }

                // Body fields (JSON) — flat, same level as path/query params
                if !entry.body_fields.is_empty() {
                    let mut body_bytes = Vec::new();
                    request
                        .as_reader()
                        .read_to_end(&mut body_bytes)
                        .unwrap_or(0);
                    let body_str = String::from_utf8_lossy(&body_bytes);
                    let json_fields = parse_json_object(&body_str);
                    for (bname, bty) in &entry.body_fields {
                        let val = json_fields
                            .get(bname)
                            .map(|v| v.as_str())
                            .unwrap_or("");
                        fields.push(RecordField {
                            name: bname.clone(),
                            value: string_to_value(val, bty),
                        });
                    }
                }

                // Add `respond` field — identity function at runtime
                // (the type system uses it for per-branch response type checking)
                fields.push(RecordField {
                    name: "respond".to_string(),
                    value: alloc(Value::Function(
                        respond_identity as *const u8,
                        std::ptr::null_mut(),
                        "respond".to_string(),
                    )),
                });

                let record = alloc(Value::Record(fields));
                // Wrap in constructor
                let tag = entry.constructor.clone();
                let ctor_val = alloc(Value::Constructor(tag, record));

                // Call handler
                let result = knot_value_call(db, handler, ctor_val);
                let json = value_to_json(result);

                let response = tiny_http::Response::from_string(&json)
                    .with_header(
                        "Content-Type: application/json"
                            .parse::<tiny_http::Header>()
                            .unwrap(),
                    );
                let _ = request.respond(response);
            }
            None => {
                let response = tiny_http::Response::from_string("{\"error\":\"not found\"}")
                    .with_status_code(404)
                    .with_header(
                        "Content-Type: application/json"
                            .parse::<tiny_http::Header>()
                            .unwrap(),
                    );
                let _ = request.respond(response);
            }
        }
    }
}

// ── OpenAPI spec generation ──────────────────────────────────────

use std::sync::Mutex;

struct SendPtr(*mut c_void);
unsafe impl Send for SendPtr {}

static API_REGISTRY: Mutex<Vec<(String, SendPtr)>> = Mutex::new(Vec::new());

#[unsafe(no_mangle)]
pub extern "C" fn knot_api_register(
    name_ptr: *const u8,
    name_len: usize,
    table: *mut c_void,
) {
    let name = unsafe { str_from_raw(name_ptr, name_len) }.to_string();
    API_REGISTRY.lock().unwrap().push((name, SendPtr(table)));
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_api_handle(argc: i32, argv: *const *const u8) -> i32 {
    if argc < 2 {
        return 0;
    }
    let args: Vec<String> = (0..argc as usize)
        .map(|i| unsafe {
            let ptr = *argv.add(i);
            let mut len = 0;
            while *ptr.add(len) != 0 {
                len += 1;
            }
            String::from_utf8_lossy(std::slice::from_raw_parts(ptr, len)).to_string()
        })
        .collect();

    if args.get(1).map(|s| s.as_str()) != Some("api") {
        return 0;
    }

    let registry = API_REGISTRY.lock().unwrap();

    if argc < 3 {
        eprintln!("Usage: <program> api <RouteName>");
        eprintln!();
        eprintln!("Available routes:");
        for (name, _) in registry.iter() {
            eprintln!("  {}", name);
        }
        std::process::exit(1);
    }

    let route_name = &args[2];

    for (name, SendPtr(table_ptr)) in registry.iter() {
        if name == route_name {
            let table = unsafe { &*(*table_ptr as *const RouteTable) };
            let spec = generate_openapi(name, table);
            println!("{}", spec);
            return 1;
        }
    }

    eprintln!("Unknown route: {}", route_name);
    eprintln!();
    eprintln!("Available routes:");
    for (name, _) in registry.iter() {
        eprintln!("  {}", name);
    }
    std::process::exit(1);
}

fn generate_openapi(name: &str, table: &RouteTable) -> String {
    let mut out = String::new();
    out.push_str("{\n");
    out.push_str("  \"openapi\": \"3.0.3\",\n");
    out.push_str(&format!(
        "  \"info\": {{ \"title\": \"{}\", \"version\": \"1.0.0\" }},\n",
        json_escape(name)
    ));
    out.push_str("  \"paths\": {\n");

    // Group entries by path
    let mut path_map: HashMap<String, Vec<&RouteTableEntry>> = HashMap::new();
    for entry in &table.entries {
        let path_str = openapi_path(&entry.path_parts);
        path_map.entry(path_str).or_default().push(entry);
    }
    let paths: Vec<(String, Vec<&RouteTableEntry>)> = path_map.into_iter().collect();

    for (i, (path, entries)) in paths.iter().enumerate() {
        out.push_str(&format!("    \"{}\": {{\n", json_escape(path)));
        for (j, entry) in entries.iter().enumerate() {
            let method = entry.method.to_lowercase();
            out.push_str(&format!("      \"{}\": {{\n", method));
            out.push_str(&format!(
                "        \"operationId\": \"{}\",\n",
                json_escape(&entry.constructor)
            ));

            // Collect parameters (path + query)
            let mut params = Vec::new();
            for part in &entry.path_parts {
                if let PathPart::Param(pname, pty) = part {
                    params.push(format!(
                        "{{ \"name\": \"{}\", \"in\": \"path\", \"required\": true, \"schema\": {} }}",
                        json_escape(pname),
                        type_to_openapi_schema(pty)
                    ));
                }
            }
            for (qname, qty) in &entry.query_fields {
                params.push(format!(
                    "{{ \"name\": \"{}\", \"in\": \"query\", \"required\": false, \"schema\": {} }}",
                    json_escape(qname),
                    type_to_openapi_schema(qty)
                ));
            }

            let has_body = !entry.body_fields.is_empty();
            let has_response = !entry.response_type.is_empty();

            if !params.is_empty() {
                out.push_str("        \"parameters\": [\n");
                for (k, param) in params.iter().enumerate() {
                    out.push_str(&format!("          {}", param));
                    if k + 1 < params.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                out.push_str("        ]");
                if has_body || has_response {
                    out.push(',');
                }
                out.push('\n');
            }

            // Request body
            if has_body {
                out.push_str("        \"requestBody\": {\n");
                out.push_str("          \"required\": true,\n");
                out.push_str("          \"content\": {\n");
                out.push_str("            \"application/json\": {\n");
                out.push_str("              \"schema\": {\n");
                out.push_str("                \"type\": \"object\",\n");
                out.push_str("                \"properties\": {\n");
                for (k, (fname, fty)) in entry.body_fields.iter().enumerate() {
                    out.push_str(&format!(
                        "                  \"{}\": {}",
                        json_escape(fname),
                        type_to_openapi_schema(fty)
                    ));
                    if k + 1 < entry.body_fields.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                out.push_str("                },\n");
                out.push_str("                \"required\": [");
                for (k, (fname, _)) in entry.body_fields.iter().enumerate() {
                    out.push_str(&format!("\"{}\"", json_escape(fname)));
                    if k + 1 < entry.body_fields.len() {
                        out.push_str(", ");
                    }
                }
                out.push_str("]\n");
                out.push_str("              }\n");
                out.push_str("            }\n");
                out.push_str("          }\n");
                out.push_str("        }");
                if has_response {
                    out.push(',');
                }
                out.push('\n');
            }

            // Response
            out.push_str("        \"responses\": {\n");
            out.push_str("          \"200\": {\n");
            out.push_str("            \"description\": \"Successful response\"");
            if has_response {
                out.push_str(",\n");
                out.push_str("            \"content\": {\n");
                out.push_str("              \"application/json\": {\n");
                out.push_str(&format!(
                    "                \"schema\": {}\n",
                    response_type_to_schema(&entry.response_type)
                ));
                out.push_str("              }\n");
                out.push_str("            }\n");
            } else {
                out.push('\n');
            }
            out.push_str("          }\n");
            out.push_str("        }\n");

            out.push_str("      }");
            if j + 1 < entries.len() {
                out.push(',');
            }
            out.push('\n');
        }
        out.push_str("    }");
        if i + 1 < paths.len() {
            out.push(',');
        }
        out.push('\n');
    }

    out.push_str("  }\n");
    out.push_str("}\n");
    out
}

fn openapi_path(parts: &[PathPart]) -> String {
    if parts.is_empty() {
        return "/".to_string();
    }
    let mut s = String::new();
    for part in parts {
        s.push('/');
        match part {
            PathPart::Literal(lit) => s.push_str(lit),
            PathPart::Param(name, _) => {
                s.push('{');
                s.push_str(name);
                s.push('}');
            }
        }
    }
    s
}

fn type_to_openapi_schema(ty: &str) -> &'static str {
    match ty {
        "int" => "{ \"type\": \"integer\" }",
        "float" => "{ \"type\": \"number\" }",
        "bool" => "{ \"type\": \"boolean\" }",
        "text" => "{ \"type\": \"string\" }",
        _ => "{ \"type\": \"string\" }",
    }
}

/// Parse a response type descriptor and produce an OpenAPI schema JSON string.
///
/// Descriptor format:
/// - `int` / `float` / `text` / `bool` — primitives
/// - `[<inner>]` — array of inner type
/// - `{name:type,name:type}` — object
/// - Anything else — treated as string
fn response_type_to_schema(desc: &str) -> String {
    let desc = desc.trim();
    if desc.is_empty() {
        return "{}".to_string();
    }
    match desc {
        "int" => "{ \"type\": \"integer\" }".to_string(),
        "float" => "{ \"type\": \"number\" }".to_string(),
        "bool" => "{ \"type\": \"boolean\" }".to_string(),
        "text" => "{ \"type\": \"string\" }".to_string(),
        "unit" => "{ \"type\": \"object\" }".to_string(),
        _ if desc.starts_with('[') && desc.ends_with(']') => {
            let inner = &desc[1..desc.len() - 1];
            format!(
                "{{ \"type\": \"array\", \"items\": {} }}",
                response_type_to_schema(inner)
            )
        }
        _ if desc.starts_with('{') && desc.ends_with('}') => {
            let inner = &desc[1..desc.len() - 1];
            let fields = parse_response_fields(inner);
            let mut s = String::new();
            s.push_str("{ \"type\": \"object\", \"properties\": { ");
            for (i, (fname, fty)) in fields.iter().enumerate() {
                s.push_str(&format!(
                    "\"{}\": {}",
                    json_escape(fname),
                    response_type_to_schema(fty)
                ));
                if i + 1 < fields.len() {
                    s.push_str(", ");
                }
            }
            s.push_str(" } }");
            s
        }
        _ => "{ \"type\": \"string\" }".to_string(),
    }
}

/// Parse comma-separated `name:type` fields, respecting nested brackets/braces.
fn parse_response_fields(s: &str) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    let bytes = s.as_bytes();
    for i in 0..bytes.len() {
        match bytes[i] {
            b'[' | b'{' => depth += 1,
            b']' | b'}' => depth -= 1,
            b',' if depth == 0 => {
                let part = s[start..i].trim();
                if let Some((name, ty)) = part.split_once(':') {
                    fields.push((name.trim().to_string(), ty.trim().to_string()));
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let part = s[start..].trim();
    if let Some((name, ty)) = part.split_once(':') {
        fields.push((name.trim().to_string(), ty.trim().to_string()));
    }
    fields
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

// ── Hash index for equi-join optimization ──────────────────────────

struct HashIndex {
    map: HashMap<Vec<u8>, Vec<*mut Value>>,
}

/// Serialize a Value to compact bytes for use as a hash key.
fn serialize_value_for_hash(v: *mut Value) -> Vec<u8> {
    let mut buf = Vec::new();
    match unsafe { as_ref(v) } {
        Value::Int(n) => {
            buf.push(0);
            buf.extend_from_slice(n.to_string().as_bytes());
        }
        Value::Float(f) => {
            buf.push(1);
            buf.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(2);
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Bool(b) => {
            buf.push(3);
            buf.push(*b as u8);
        }
        Value::Unit => {
            buf.push(4);
        }
        Value::Constructor(tag, _) => {
            buf.push(5);
            buf.extend_from_slice(tag.as_bytes());
        }
        _ => {
            // Fallback: use the show representation
            buf.push(6);
            buf.extend_from_slice(brief_value(v).as_bytes());
        }
    }
    buf
}

/// Build a hash index over a relation on a given field.
/// Returns an opaque pointer to a heap-allocated HashIndex.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_build_index(
    rel: *mut Value,
    field_ptr: *const u8,
    field_len: usize,
) -> *mut c_void {
    let field_name = unsafe { str_from_raw(field_ptr, field_len) };
    let rows = match unsafe { as_ref(rel) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: build_index expected Relation, got {}", type_name(rel)),
    };

    let mut map: HashMap<Vec<u8>, Vec<*mut Value>> = HashMap::new();
    for &row in rows {
        let key_val = knot_record_field(row, field_ptr, field_len);
        let key = serialize_value_for_hash(key_val);
        map.entry(key).or_default().push(row);
    }

    if debug_enabled() {
        eprintln!(
            "[OPT] hash index on .{}: {} keys from {} rows",
            field_name,
            map.len(),
            rows.len()
        );
    }

    Box::into_raw(Box::new(HashIndex { map })) as *mut c_void
}

/// Look up matching rows in a hash index by key value.
/// Returns a Relation of matching rows (empty if no match).
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_index_lookup(
    index: *mut c_void,
    key: *mut Value,
) -> *mut Value {
    let idx = unsafe { &*(index as *mut HashIndex) };
    let hash_key = serialize_value_for_hash(key);
    match idx.map.get(&hash_key) {
        Some(rows) => alloc(Value::Relation(rows.clone())),
        None => alloc(Value::Relation(Vec::new())),
    }
}

/// Free a hash index.
#[unsafe(no_mangle)]
pub extern "C" fn knot_relation_index_free(index: *mut c_void) {
    unsafe {
        drop(Box::from_raw(index as *mut HashIndex));
    }
}

// ── Elliptic curve cryptography ──────────────────────────────────

/// Generate an X25519 key pair for encryption.
/// Returns Record {privateKey: Bytes, publicKey: Bytes}.
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_generate_key_pair() -> *mut Value {
    let mut secret_bytes = [0u8; 32];
    getrandom::fill(&mut secret_bytes).expect("knot runtime: failed to generate random bytes");
    let secret = x25519_dalek::StaticSecret::from(secret_bytes);
    let public = x25519_dalek::PublicKey::from(&secret);

    let record = knot_record_empty(2);
    let k = b"privateKey";
    knot_record_set_field(record, k.as_ptr(), k.len(), alloc(Value::Bytes(secret_bytes.to_vec())));
    let k = b"publicKey";
    knot_record_set_field(record, k.as_ptr(), k.len(), alloc(Value::Bytes(public.as_bytes().to_vec())));
    record
}

/// Generate an Ed25519 key pair for signing.
/// Returns Record {privateKey: Bytes, publicKey: Bytes}.
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_generate_signing_key_pair() -> *mut Value {
    let mut secret_bytes = [0u8; 32];
    getrandom::fill(&mut secret_bytes).expect("knot runtime: failed to generate random bytes");
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
    let verifying_key = signing_key.verifying_key();

    let record = knot_record_empty(2);
    let k = b"privateKey";
    knot_record_set_field(record, k.as_ptr(), k.len(), alloc(Value::Bytes(signing_key.to_bytes().to_vec())));
    let k = b"publicKey";
    knot_record_set_field(record, k.as_ptr(), k.len(), alloc(Value::Bytes(verifying_key.to_bytes().to_vec())));
    record
}

/// Sealed-box encryption: X25519 ECDH + ChaCha20-Poly1305.
/// Takes (publicKey: Bytes, plaintext: Bytes), returns ciphertext Bytes.
/// Format: [ephemeral_pub: 32][nonce: 12][encrypted + tag: len+16]
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_encrypt(public_key: *mut Value, plaintext: *mut Value) -> *mut Value {
    use chacha20poly1305::{ChaCha20Poly1305, KeyInit};
    use chacha20poly1305::aead::Aead;

    let pub_bytes = match unsafe { as_ref(public_key) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: encrypt expected Bytes for publicKey, got {}", type_name(public_key)),
    };
    let plain = match unsafe { as_ref(plaintext) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: encrypt expected Bytes for plaintext, got {}", type_name(plaintext)),
    };

    let recipient_pub: [u8; 32] = pub_bytes.as_slice().try_into()
        .expect("knot runtime: encrypt publicKey must be 32 bytes");
    let recipient_public = x25519_dalek::PublicKey::from(recipient_pub);

    // Generate ephemeral key pair
    let mut eph_secret_bytes = [0u8; 32];
    getrandom::fill(&mut eph_secret_bytes).expect("knot runtime: failed to generate random bytes");
    let eph_secret = x25519_dalek::StaticSecret::from(eph_secret_bytes);
    let eph_public = x25519_dalek::PublicKey::from(&eph_secret);

    // ECDH shared secret
    let shared = eph_secret.diffie_hellman(&recipient_public);
    let key = chacha20poly1305::Key::from_slice(shared.as_bytes());
    let cipher = ChaCha20Poly1305::new(key);

    // Random nonce
    let mut nonce_bytes = [0u8; 12];
    getrandom::fill(&mut nonce_bytes).expect("knot runtime: failed to generate nonce");
    let nonce = chacha20poly1305::Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher.encrypt(nonce, plain.as_slice())
        .expect("knot runtime: encryption failed");

    // Pack: ephemeral_public (32) + nonce (12) + ciphertext
    let mut result = Vec::with_capacity(32 + 12 + ciphertext.len());
    result.extend_from_slice(eph_public.as_bytes());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    alloc(Value::Bytes(result))
}

/// Sealed-box decryption: reverse of encrypt.
/// Takes (privateKey: Bytes, ciphertext: Bytes), returns plaintext Bytes.
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_decrypt(private_key: *mut Value, ciphertext: *mut Value) -> *mut Value {
    use chacha20poly1305::{ChaCha20Poly1305, KeyInit};
    use chacha20poly1305::aead::Aead;

    let priv_bytes = match unsafe { as_ref(private_key) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: decrypt expected Bytes for privateKey, got {}", type_name(private_key)),
    };
    let ct = match unsafe { as_ref(ciphertext) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: decrypt expected Bytes for ciphertext, got {}", type_name(ciphertext)),
    };

    if ct.len() < 32 + 12 + 16 {
        panic!("knot runtime: decrypt ciphertext too short (need at least 60 bytes, got {})", ct.len());
    }

    let secret_bytes: [u8; 32] = priv_bytes.as_slice().try_into()
        .expect("knot runtime: decrypt privateKey must be 32 bytes");
    let secret = x25519_dalek::StaticSecret::from(secret_bytes);

    // Unpack
    let eph_pub_bytes: [u8; 32] = ct[..32].try_into().unwrap();
    let nonce_bytes: [u8; 12] = ct[32..44].try_into().unwrap();
    let encrypted = &ct[44..];

    let eph_public = x25519_dalek::PublicKey::from(eph_pub_bytes);
    let shared = secret.diffie_hellman(&eph_public);
    let key = chacha20poly1305::Key::from_slice(shared.as_bytes());
    let cipher = ChaCha20Poly1305::new(key);
    let nonce = chacha20poly1305::Nonce::from_slice(&nonce_bytes);

    let plaintext = cipher.decrypt(nonce, encrypted)
        .expect("knot runtime: decryption failed (invalid key or corrupted ciphertext)");
    alloc(Value::Bytes(plaintext))
}

/// Ed25519 signing. Takes (privateKey: Bytes, message: Bytes), returns signature Bytes.
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_sign(private_key: *mut Value, message: *mut Value) -> *mut Value {
    use ed25519_dalek::Signer;

    let priv_bytes = match unsafe { as_ref(private_key) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: sign expected Bytes for privateKey, got {}", type_name(private_key)),
    };
    let msg = match unsafe { as_ref(message) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: sign expected Bytes for message, got {}", type_name(message)),
    };

    let secret_bytes: [u8; 32] = priv_bytes.as_slice().try_into()
        .expect("knot runtime: sign privateKey must be 32 bytes");
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
    let signature = signing_key.sign(msg);
    alloc(Value::Bytes(signature.to_bytes().to_vec()))
}

/// Ed25519 verification. Takes (db, publicKey: Bytes, message: Bytes, signature: Bytes), returns Bool.
#[unsafe(no_mangle)]
pub extern "C" fn knot_crypto_verify(
    _db: *mut c_void,
    public_key: *mut Value,
    message: *mut Value,
    signature: *mut Value,
) -> *mut Value {
    use ed25519_dalek::Verifier;

    let pub_bytes = match unsafe { as_ref(public_key) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: verify expected Bytes for publicKey, got {}", type_name(public_key)),
    };
    let msg = match unsafe { as_ref(message) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: verify expected Bytes for message, got {}", type_name(message)),
    };
    let sig_bytes = match unsafe { as_ref(signature) } {
        Value::Bytes(b) => b,
        _ => panic!("knot runtime: verify expected Bytes for signature, got {}", type_name(signature)),
    };

    let pub_arr: [u8; 32] = pub_bytes.as_slice().try_into()
        .expect("knot runtime: verify publicKey must be 32 bytes");
    let sig_arr: [u8; 64] = sig_bytes.as_slice().try_into()
        .expect("knot runtime: verify signature must be 64 bytes");

    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&pub_arr)
        .expect("knot runtime: verify invalid public key");
    let signature = ed25519_dalek::Signature::from_bytes(&sig_arr);

    let valid = verifying_key.verify(msg, &signature).is_ok();
    alloc(Value::Bool(valid))
}
