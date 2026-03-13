//! Knot runtime library.
//!
//! Provides C-ABI functions for value management, relation operations,
//! and SQLite-backed persistence. This crate is compiled as a static
//! library and linked into every compiled Knot program.

use rusqlite::Connection;
use std::ffi::c_void;
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};

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
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
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
    format!("\"{}\"", name.replace('"', "\"\""))
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
        Value::Function(_, _, _) => 8,
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

/// If the value is already a relation, return it as-is.
/// Otherwise, wrap it in a singleton relation.
#[unsafe(no_mangle)]
pub extern "C" fn knot_ensure_relation(v: *mut Value) -> *mut Value {
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
            .prepare(&insert_sql)
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

    let table = quote_ident(&format!("_knot_{}", name));
    let col_defs: Vec<String> = cols
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.name), sql_type(c.ty)))
        .collect();
    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();

    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({});",
        table,
        col_defs.join(", ")
    );
    debug_sql(&sql);
    db_ref.conn.execute_batch(&sql).unwrap_or_else(|e| {
        panic!("knot runtime: failed to create table '{}': {}", name, e)
    });

    // Create unique index for set semantics (ignore if already exists)
    if !cols.is_empty() {
        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});",
            quote_ident(&format!("_knot_{}_unique", name)),
            table,
            col_names.join(", ")
        );
        debug_sql(&idx_sql);
        let _ = db_ref.conn.execute_batch(&idx_sql);
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
            panic!(
                "knot runtime: schema mismatch for source '*{}'.\n\
                 Stored:   {}\n\
                 Compiled: {}\n\
                 Add a `migrate *{} from {{...}} to {{...}} using (\\old -> ...)` block to your source.",
                name, stored_schema, schema, name
            );
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
    let cols = parse_schema(schema);

    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
    let sql = format!(
        "SELECT {} FROM {}",
        if col_names.is_empty() {
            "1".to_string()
        } else {
            col_names.join(", ")
        },
        quote_ident(&format!("_knot_{}", name))
    );

    debug_sql(&sql);
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

    let table = quote_ident(&format!("_knot_{}", name));
    let delete_sql = format!("DELETE FROM {};", table);
    debug_sql(&delete_sql);
    db_ref
        .conn
        .execute_batch(&delete_sql)
        .expect("knot runtime: failed to delete rows");

    if !cols.is_empty() && !rows.is_empty() {
        let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
        let placeholders: Vec<String> = cols.iter().enumerate().map(|(i, _)| format!("?{}", i + 1)).collect();
        let insert_sql = format!(
            "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
            table,
            col_names.join(", "),
            placeholders.join(", ")
        );
        debug_sql(&insert_sql);

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

    let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
    let placeholders: Vec<String> = cols
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect();
    let insert_sql = format!(
        "INSERT OR IGNORE INTO {} ({}) VALUES ({});",
        quote_ident(&format!("_knot_{}", name)),
        col_names.join(", "),
        placeholders.join(", ")
    );
    debug_sql(&insert_sql);

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
    let cols = parse_schema(schema);

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
            .prepare(&insert_sql)
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

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: delete_where params must be a Relation, got {}",
            type_name(params)
        ),
    };

    let sql = format!(
        "DELETE FROM {} WHERE NOT ({});",
        quote_ident(&format!("_knot_{}", name)),
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

    let param_values = match unsafe { as_ref(params) } {
        Value::Relation(rows) => rows,
        _ => panic!(
            "knot runtime: update_where params must be a Relation, got {}",
            type_name(params)
        ),
    };

    let sql = format!(
        "UPDATE OR REPLACE {} SET {} WHERE {};",
        quote_ident(&format!("_knot_{}", name)),
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
    match unsafe { as_ref(v) } {
        Value::Int(n) => rusqlite::types::Value::Integer(*n),
        Value::Float(n) => rusqlite::types::Value::Real(*n),
        Value::Text(s) => rusqlite::types::Value::Text(s.clone()),
        Value::Bool(b) => rusqlite::types::Value::Integer(*b as i64),
        Value::Constructor(tag, _) => rusqlite::types::Value::Text(tag.clone()),
        _ => panic!(
            "knot runtime: cannot use {} as SQL parameter",
            brief_value(v)
        ),
    }
}

fn value_to_sqlite(v: *mut Value, ty: ColType) -> rusqlite::types::Value {
    match (unsafe { as_ref(v) }, ty) {
        (Value::Int(n), _) => rusqlite::types::Value::Integer(*n),
        (Value::Float(n), _) => rusqlite::types::Value::Real(*n),
        (Value::Text(s), _) => rusqlite::types::Value::Text(s.clone()),
        (Value::Bool(b), _) => rusqlite::types::Value::Integer(*b as i64),
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
        Value::Int(n) => *n,
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
        .prepare(&sql)
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
            let val = match col.ty {
                ColType::Int => knot_value_int(row.get::<_, i64>(i).unwrap()),
                ColType::Float => knot_value_float(row.get::<_, f64>(i).unwrap()),
                ColType::Text => {
                    let s: String = row.get(i).unwrap();
                    alloc(Value::Text(s))
                }
                ColType::Bool => knot_value_bool(row.get::<_, i32>(i).unwrap()),
            };
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
        .prepare(&sql)
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
            let val = match col.ty {
                ColType::Int => knot_value_int(row.get::<_, i64>(i).unwrap()),
                ColType::Float => knot_value_float(row.get::<_, f64>(i).unwrap()),
                ColType::Text => {
                    let s: String = row.get(i).unwrap();
                    alloc(Value::Text(s))
                }
                ColType::Bool => knot_value_bool(row.get::<_, i32>(i).unwrap()),
            };
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
            .prepare(&insert_sql)
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
