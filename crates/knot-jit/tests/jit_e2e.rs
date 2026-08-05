//! End-to-end JIT verification: compile a knot source string in-process and
//! read back the produced Value. The test binary links knot-runtime, so its
//! `knot_*` symbols are resolvable by the JIT'd code via dlsym(RTLD_DEFAULT).

use knot_jit::compile_and_run;

// Generated `main` calls `knot_compile_rt_init` (from knot-compile-rt) before
// running the program. Referencing it here keeps the symbol linked into the
// test binary and in its dynamic table, so the JIT'd code resolves it via
// dlsym(RTLD_DEFAULT) — and registering it exercises the same init path a real
// compiled program takes.
fn init_compile_rt() {
    unsafe { knot_compile_rt::knot_compile_rt_init() }
}

fn db() -> *mut std::ffi::c_void {
    // Open a throwaway in-memory db for the compiled snippet. knot_db_open
    // takes (path_ptr, path_len); ":memory:" gives an isolated store.
    static PATH: &[u8] = b":memory:";
    knot_runtime::knot_db_open(PATH.as_ptr(), PATH.len())
}

#[test]
fn jit_compiles_and_runs_pure_int() {
    init_compile_rt();
    let db = db();
    let out = compile_and_run("base.println (base.show (40 + 2))", db)
        .expect("JIT compile failed");
    assert!(!out.value.is_null(), "JIT produced a null Value");
}

#[test]
fn jit_compiles_and_runs_closure() {
    init_compile_rt();
    let db = db();
    let out = compile_and_run("base.println (base.show ((\\x -> x * 2 + 1) 10))", db)
        .expect("JIT compile failed");
    assert!(!out.value.is_null());
}

#[test]
fn jit_rejects_type_error() {
    init_compile_rt();
    let db = db();
    let res = compile_and_run("1 + \"not an int\"", db);
    assert!(res.is_err(), "type error should fail compilation");
}
