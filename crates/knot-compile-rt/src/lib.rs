//! The `compile` builtin's JIT glue, linked into compiled knot programs as a
//! separate staticlib archive (alongside libknot_runtime.a). knot-runtime
//! cannot depend on the compiler directly (knot-compiler's build.rs builds
//! knot-runtime — a dep here would recurse), so this crate bridges: it depends
//! on knot-jit (→ knot-compiler) and knot-runtime, exposes the compile
//! implementation as a `#[no_mangle]` extern, and registers it into
//! knot-runtime's `COMPILE_IMPL` slot at program startup.

use std::ffi::c_void;

/// The compile implementation registered into knot-runtime. Matches the
/// `CompileImpl` signature in knot-runtime: takes the program source and a db
/// handle, writes the inferred type into the out-params, returns the forced
/// value (or null on any compile error).
///
/// `out_ty` is a freshly `malloc`'d, NUL-free buffer of length `*out_ty_len`
/// (caller frees with `libc::free`), or null when the type is unknown.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn knot_compile_impl(
    src_ptr: *const u8,
    src_len: usize,
    db: *mut c_void,
    out_ty_ptr: *mut *mut u8,
    out_ty_len: *mut usize,
) -> *mut knot_runtime::Value {
    // Null out-params up front so a compile error leaves them well-defined.
    unsafe {
        if !out_ty_ptr.is_null() {
            *out_ty_ptr = std::ptr::null_mut();
        }
        if !out_ty_len.is_null() {
            *out_ty_len = 0;
        }
    }

    let src_bytes = unsafe { std::slice::from_raw_parts(src_ptr, src_len) };
    let Ok(source) = std::str::from_utf8(src_bytes) else {
        return std::ptr::null_mut();
    };

    match knot_jit::compile_and_run(source, db) {
        Ok(cv) => {
            // Hand the inferred type back as a malloc'd buffer (C ABI — the
            // runtime frees it). Only when we actually have one.
            if let Some(ty) = cv.ty
                && !out_ty_ptr.is_null()
                && !out_ty_len.is_null()
            {
                let buf = unsafe { libc::malloc(ty.len()) as *mut u8 };
                if !buf.is_null() {
                    unsafe {
                        std::ptr::copy_nonoverlapping(ty.as_ptr(), buf, ty.len());
                        *out_ty_ptr = buf;
                        *out_ty_len = ty.len();
                    }
                } else {
                    unsafe { libc::free(buf as *mut c_void) };
                }
            }
            // knot-jit treats the value as opaque *mut c_void; here it's a
            // knot-runtime Value again.
            cv.value as *mut knot_runtime::Value
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Install `knot_compile_impl` into knot-runtime's `COMPILE_IMPL` slot. Called
/// once from the generated program's `main` before user code runs.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn knot_compile_rt_init() {
    knot_runtime::knot_register_compile_impl(knot_compile_impl as *mut c_void);
}
