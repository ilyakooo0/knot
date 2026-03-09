//! Knot runtime library.
//!
//! Provides C-ABI functions for value management, relation operations,
//! and SQLite-backed persistence. This crate is compiled as a static
//! library and linked into every compiled Knot program.

use rusqlite::Connection;
use std::ffi::c_void;
use std::slice;

// ── Value representation ──────────────────────────────────────────

/// Runtime representation of all Knot values.
///
/// Every Knot expression evaluates to a heap-allocated `Value`.
/// The Cranelift-generated code works exclusively with `*mut Value` pointers.
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Unit,
    Record(Vec<RecordField>),
    Relation(Vec<*mut Value>),
    Constructor(String, *mut Value),
    /// (fn_ptr, env) — fn_ptr has signature: extern "C" fn(db, env, arg) -> *mut Value
    Function(*const u8, *mut Value),
}

pub struct RecordField {
    pub name: String,
    pub value: *mut Value,
}

/// SQLite database handle.
pub struct KnotDb {
    pub conn: Connection,
}

// ── Helpers ───────────────────────────────────────────────────────

fn alloc(v: Value) -> *mut Value {
    Box::into_raw(Box::new(v))
}

unsafe fn as_ref<'a>(v: *mut Value) -> &'a Value {
    unsafe { &*v }
}

unsafe fn str_from_raw(ptr: *const u8, len: usize) -> &'static str {
    unsafe { std::str::from_utf8_unchecked(slice::from_raw_parts(ptr, len)) }
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
        Value::Unit => "Unit",
        Value::Record(_) => "Record",
        Value::Relation(_) => "Relation",
        Value::Constructor(_, _) => "Constructor",
        Value::Function(_, _) => "Function",
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
        Value::Unit => "Unit".to_string(),
        Value::Record(fields) => {
            let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
            format!("Record({{{}}})", names.join(", "))
        }
        Value::Relation(rows) => format!("Relation({} rows)", rows.len()),
        Value::Constructor(tag, _) => format!("Constructor({})", tag),
        Value::Function(_, _) => "Function".to_string(),
    }
}

// ── Value constructors ────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_int(n: i64) -> *mut Value {
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
pub extern "C" fn knot_value_function(fn_ptr: *const u8, env: *mut Value) -> *mut Value {
    alloc(Value::Function(fn_ptr, env))
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

// ── Value accessors ───────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_get_int(v: *mut Value) -> i64 {
    match unsafe { as_ref(v) } {
        Value::Int(n) => *n,
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
    match unsafe { as_ref(v) } {
        Value::Int(_) => 0,
        Value::Float(_) => 1,
        Value::Text(_) => 2,
        Value::Bool(_) => 3,
        Value::Unit => 4,
        Value::Record(_) => 5,
        Value::Relation(_) => 6,
        Value::Constructor(_, _) => 7,
        Value::Function(_, _) => 8,
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
            // Update existing field or add new one
            if let Some(field) = fields.iter_mut().find(|f| f.name == name) {
                field.value = value;
            } else {
                fields.push(RecordField { name, value });
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
    let name = unsafe { str_from_raw(key_ptr, key_len) };
    match unsafe { as_ref(record) } {
        Value::Record(fields) => {
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
        _ => panic!(
            "knot runtime: expected Record in field access, got {}",
            brief_value(record)
        ),
    }
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
pub extern "C" fn knot_relation_union(a: *mut Value, b: *mut Value) -> *mut Value {
    let rows_a = match unsafe { as_ref(a) } {
        Value::Relation(rows) => rows.clone(),
        _ => panic!("knot runtime: expected Relation in union, got {}", type_name(a)),
    };
    let rows_b = match unsafe { as_ref(b) } {
        Value::Relation(rows) => rows.clone(),
        _ => panic!("knot runtime: expected Relation in union, got {}", type_name(b)),
    };
    let mut result = rows_a;
    for row in rows_b {
        if !result.iter().any(|existing| values_equal(*existing, row)) {
            result.push(row);
        }
    }
    alloc(Value::Relation(result))
}

// ── Value equality ────────────────────────────────────────────────

fn values_equal(a: *mut Value, b: *mut Value) -> bool {
    if a == b {
        return true;
    }
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x == y,
        (Value::Text(x), Value::Text(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Unit, Value::Unit) => true,
        (Value::Record(fa), Value::Record(fb)) => {
            if fa.len() != fb.len() {
                return false;
            }
            fa.iter().all(|field_a| {
                fb.iter()
                    .any(|field_b| field_a.name == field_b.name && values_equal(field_a.value, field_b.value))
            })
        }
        (Value::Constructor(ta, pa), Value::Constructor(tb, pb)) => {
            ta == tb && values_equal(*pa, *pb)
        }
        (Value::Relation(ra), Value::Relation(rb)) => {
            if ra.len() != rb.len() {
                return false;
            }
            ra.iter()
                .all(|row_a| rb.iter().any(|row_b| values_equal(*row_a, *row_b)))
                && rb
                    .iter()
                    .all(|row_b| ra.iter().any(|row_a| values_equal(*row_a, *row_b)))
        }
        _ => false,
    }
}

// ── Binary operations ─────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_add(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => alloc(Value::Int(x + y)),
        (Value::Float(x), Value::Float(y)) => alloc(Value::Float(x + y)),
        (Value::Int(x), Value::Float(y)) => alloc(Value::Float(*x as f64 + y)),
        (Value::Float(x), Value::Int(y)) => alloc(Value::Float(x + *y as f64)),
        _ => panic!("knot runtime: cannot add {} + {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_sub(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => alloc(Value::Int(x - y)),
        (Value::Float(x), Value::Float(y)) => alloc(Value::Float(x - y)),
        (Value::Int(x), Value::Float(y)) => alloc(Value::Float(*x as f64 - y)),
        (Value::Float(x), Value::Int(y)) => alloc(Value::Float(x - *y as f64)),
        _ => panic!("knot runtime: cannot subtract {} - {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_mul(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => alloc(Value::Int(x * y)),
        (Value::Float(x), Value::Float(y)) => alloc(Value::Float(x * y)),
        (Value::Int(x), Value::Float(y)) => alloc(Value::Float(*x as f64 * y)),
        (Value::Float(x), Value::Int(y)) => alloc(Value::Float(x * *y as f64)),
        _ => panic!("knot runtime: cannot multiply {} * {}", type_name(a), type_name(b)),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_value_div(a: *mut Value, b: *mut Value) -> *mut Value {
    match (unsafe { as_ref(a) }, unsafe { as_ref(b) }) {
        (Value::Int(x), Value::Int(y)) => {
            if *y == 0 {
                panic!("knot runtime: division by zero");
            }
            alloc(Value::Int(x / y))
        }
        (Value::Float(x), Value::Float(y)) => alloc(Value::Float(x / y)),
        (Value::Int(x), Value::Float(y)) => alloc(Value::Float(*x as f64 / y)),
        (Value::Float(x), Value::Int(y)) => alloc(Value::Float(x / *y as f64)),
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
        (Value::Int(x), Value::Float(y)) => (*x as f64) < *y,
        (Value::Float(x), Value::Int(y)) => *x < (*y as f64),
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
        (Value::Int(x), Value::Float(y)) => (*x as f64) > *y,
        (Value::Float(x), Value::Int(y)) => *x > (*y as f64),
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
        (Value::Int(x), Value::Float(y)) => (*x as f64) <= *y,
        (Value::Float(x), Value::Int(y)) => *x <= (*y as f64),
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
        (Value::Int(x), Value::Float(y)) => (*x as f64) >= *y,
        (Value::Float(x), Value::Int(y)) => *x >= (*y as f64),
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
        (Value::Relation(_x), Value::Relation(_y)) => {
            // ++ on relations is union
            knot_relation_union(a, b)
        }
        _ => panic!("knot runtime: ++ requires Text or Relation operands, got {} ++ {}", type_name(a), type_name(b)),
    }
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
        Value::Function(fn_ptr, env) => {
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
        Value::Function(_, _) => "<function>".to_string(),
    }
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

// ── Database operations ───────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_db_open(path_ptr: *const u8, path_len: usize) -> *mut c_void {
    let path = unsafe { str_from_raw(path_ptr, path_len) };
    let conn = Connection::open(path).expect("knot runtime: failed to open database");
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
        .expect("knot runtime: failed to set pragmas");
    let db = Box::new(KnotDb { conn });
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
    db.conn
        .execute_batch(sql)
        .unwrap_or_else(|e| panic!("knot runtime: SQL error: {}\n  SQL: {}", e, sql));
}

// ── Source operations ─────────────────────────────────────────────

/// Schema descriptor format: "col1:type1,col2:type2,..."
/// Types: "int", "float", "text", "bool"
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
}

fn parse_schema(spec: &str) -> Vec<ColumnSpec> {
    if spec.is_empty() {
        return Vec::new();
    }
    spec.split(',')
        .map(|part| {
            let mut parts = part.splitn(2, ':');
            let name = parts.next().unwrap().to_string();
            let ty = match parts.next().unwrap_or("text") {
                "int" => ColType::Int,
                "float" => ColType::Float,
                "text" => ColType::Text,
                "bool" => ColType::Bool,
                other => panic!("knot runtime: unknown column type '{}'", other),
            };
            ColumnSpec { name, ty }
        })
        .collect()
}

fn sql_type(ty: ColType) -> &'static str {
    match ty {
        ColType::Int => "INTEGER",
        ColType::Float => "REAL",
        ColType::Text => "TEXT",
        ColType::Bool => "INTEGER",
    }
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
    let cols = parse_schema(schema);

    let col_defs: Vec<String> = cols
        .iter()
        .map(|c| format!("\"{}\" {}", c.name, sql_type(c.ty)))
        .collect();
    let col_names: Vec<String> = cols.iter().map(|c| format!("\"{}\"", c.name)).collect();

    let sql = format!(
        "CREATE TABLE IF NOT EXISTS \"_knot_{}\" ({});",
        name,
        col_defs.join(", ")
    );
    db_ref.conn.execute_batch(&sql).unwrap_or_else(|e| {
        panic!("knot runtime: failed to create table '{}': {}", name, e)
    });

    // Create unique index for set semantics (ignore if already exists)
    if !cols.is_empty() {
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS \"_knot_{}_unique\" ON \"_knot_{}\" ({});",
            name,
            name,
            col_names.join(", ")
        );
        let _ = db_ref.conn.execute_batch(&idx_sql);
    }
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
    let cols = parse_schema(schema);

    let col_names: Vec<String> = cols.iter().map(|c| format!("\"{}\"", c.name)).collect();
    let sql = format!(
        "SELECT {} FROM \"_knot_{}\"",
        if col_names.is_empty() {
            "1".to_string()
        } else {
            col_names.join(", ")
        },
        name
    );

    let mut stmt = db_ref
        .conn
        .prepare(&sql)
        .unwrap_or_else(|e| panic!("knot runtime: query error: {}", e));

    let mut rows: Vec<*mut Value> = Vec::new();
    let mut result_rows = stmt
        .query([])
        .unwrap_or_else(|e| panic!("knot runtime: query exec error: {}", e));

    while let Some(row) = result_rows
        .next()
        .unwrap_or_else(|e| panic!("knot runtime: row fetch error: {}", e))
    {
        let record = knot_record_empty(cols.len());
        for (i, col) in cols.iter().enumerate() {
            let val = match col.ty {
                ColType::Int => knot_value_int(row.get::<_, i64>(i).unwrap()),
                ColType::Float => knot_value_float(row.get::<_, f64>(i).unwrap()),
                ColType::Text => {
                    let s: String = row.get(i).unwrap();
                    let v = alloc(Value::Text(s));
                    v
                }
                ColType::Bool => knot_value_bool(row.get::<_, i32>(i).unwrap()),
            };
            // Set the field
            let name = col.name.as_bytes();
            knot_record_set_field(record, name.as_ptr(), name.len(), val);
        }
        rows.push(record);
    }

    alloc(Value::Relation(rows))
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
    let cols = parse_schema(schema);

    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => panic!("knot runtime: source_write expects a Relation, got {}", type_name(relation)),
    };

    // Delete all existing rows and insert new ones in a transaction
    db_ref
        .conn
        .execute_batch("BEGIN;")
        .expect("knot runtime: failed to begin transaction");

    let delete_sql = format!("DELETE FROM \"_knot_{}\";", name);
    db_ref
        .conn
        .execute_batch(&delete_sql)
        .expect("knot runtime: failed to delete rows");

    if !cols.is_empty() && !rows.is_empty() {
        let col_names: Vec<String> = cols.iter().map(|c| format!("\"{}\"", c.name)).collect();
        let placeholders: Vec<String> = cols.iter().enumerate().map(|(i, _)| format!("?{}", i + 1)).collect();
        let insert_sql = format!(
            "INSERT OR IGNORE INTO \"_knot_{}\" ({}) VALUES ({});",
            name,
            col_names.join(", "),
            placeholders.join(", ")
        );

        let mut stmt = db_ref
            .conn
            .prepare(&insert_sql)
            .expect("knot runtime: failed to prepare insert");

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
                        panic!("knot runtime: insert error: {}", e)
                    });
                }
                _ => panic!("knot runtime: relation rows must be Records, got {}", type_name(*row_ptr)),
            }
        }
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
    let cols = parse_schema(schema);

    let rows = match unsafe { as_ref(relation) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: source_append expects a Relation, got {}",
            type_name(relation)
        ),
    };

    if cols.is_empty() || rows.is_empty() {
        return;
    }

    db_ref
        .conn
        .execute_batch("BEGIN;")
        .expect("knot runtime: failed to begin transaction");

    let col_names: Vec<String> = cols.iter().map(|c| format!("\"{}\"", c.name)).collect();
    let placeholders: Vec<String> = cols
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect();
    let insert_sql = format!(
        "INSERT OR IGNORE INTO \"_knot_{}\" ({}) VALUES ({});",
        name,
        col_names.join(", "),
        placeholders.join(", ")
    );

    let mut stmt = db_ref
        .conn
        .prepare(&insert_sql)
        .expect("knot runtime: failed to prepare insert");

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
                    panic!("knot runtime: insert error: {}", e)
                });
            }
            _ => panic!(
                "knot runtime: relation rows must be Records, got {}",
                type_name(*row_ptr)
            ),
        }
    }

    db_ref
        .conn
        .execute_batch("COMMIT;")
        .expect("knot runtime: failed to commit transaction");
}

fn value_to_sqlite(v: *mut Value, ty: ColType) -> rusqlite::types::Value {
    match (unsafe { as_ref(v) }, ty) {
        (Value::Int(n), _) => rusqlite::types::Value::Integer(*n),
        (Value::Float(n), _) => rusqlite::types::Value::Real(*n),
        (Value::Text(s), _) => rusqlite::types::Value::Text(s.clone()),
        (Value::Bool(b), _) => rusqlite::types::Value::Integer(*b as i64),
        _ => panic!("knot runtime: cannot convert {} to SQL", brief_value(v)),
    }
}

// ── Atomic (transactions) ─────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn knot_atomic_begin(db: *mut c_void) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    db_ref
        .conn
        .execute_batch("SAVEPOINT knot_atomic;")
        .expect("knot runtime: failed to begin atomic");
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_atomic_commit(db: *mut c_void) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    db_ref
        .conn
        .execute_batch("RELEASE SAVEPOINT knot_atomic;")
        .expect("knot runtime: failed to commit atomic");
}

#[unsafe(no_mangle)]
pub extern "C" fn knot_atomic_rollback(db: *mut c_void) {
    let db_ref = unsafe { &*(db as *mut KnotDb) };
    db_ref
        .conn
        .execute_batch("ROLLBACK TO SAVEPOINT knot_atomic;")
        .expect("knot runtime: failed to rollback atomic");
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
    let tag = unsafe { str_from_raw(tag_ptr, tag_len) };
    match unsafe { as_ref(v) } {
        Value::Constructor(t, _) => (t == tag) as i32,
        _ => 0,
    }
}

/// Get the payload of a constructor value.
#[unsafe(no_mangle)]
pub extern "C" fn knot_constructor_payload(v: *mut Value) -> *mut Value {
    match unsafe { as_ref(v) } {
        Value::Constructor(_, payload) => *payload,
        _ => panic!("knot runtime: expected Constructor, got {}", type_name(v)),
    }
}
