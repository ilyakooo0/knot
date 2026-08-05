//! Invokes the system linker to combine a Cranelift-generated object file
//! with the knot runtime static library into an executable.

use std::path::Path;
use std::process::Command;

pub fn link(
    object_path: &Path,
    runtime_path: &Path,
    compile_rt_path: &Path,
    output_path: &Path,
) -> Result<(), String> {
    let mut cmd = Command::new("cc");
    // Link order: the program object first, then the JIT compile-runtime
    // (knot-compile-rt, which provides `knot_compile_impl`/`knot_compile_rt_init`
    // and pulls in the compiler+cranelift+z3), then the knot runtime that
    // satisfies its undefined `knot_*` references. Archives are scanned
    // left-to-right, so a provider must come after its users.
    cmd.arg("-o")
        .arg(output_path)
        .arg(object_path)
        .arg(compile_rt_path)
        .arg(runtime_path);

    // Export the program's `knot_*` runtime symbols into the dynamic symbol
    // table so code JIT-compiled at runtime by the `compile` builtin can
    // resolve them against the running process (dlsym RTLD_DEFAULT). Without
    // this the linker keeps them local and JIT symbol resolution fails.
    if cfg!(target_os = "macos") {
        cmd.arg("-Wl,-export_dynamic");
    } else {
        cmd.arg("-Wl,--export-dynamic");
    }

    // On macOS, link system libraries needed by the Rust runtime
    if cfg!(target_os = "macos") {
        cmd.arg("-lSystem").arg("-lresolv").arg("-liconv");
        // knot-compile-rt embeds Z3 (C++), needing the C++ stdlib.
        cmd.arg("-lc++");
    } else if cfg!(target_os = "linux") {
        cmd.arg("-lpthread").arg("-ldl").arg("-lm");
        // knot-compile-rt embeds Z3 (C++, `__cxa_*`).
        cmd.arg("-lstdc++");
    } else {
        return Err(format!(
            "unsupported target OS for linking: {}; only macOS and Linux are supported",
            std::env::consts::OS
        ));
    }

    let output = cmd
        .output()
        .map_err(|e| format!("failed to run linker (cc): {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("linker failed:\n{}", stderr));
    }

    Ok(())
}
