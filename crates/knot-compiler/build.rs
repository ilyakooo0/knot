use std::path::PathBuf;

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_path = PathBuf::from(&out_dir);

    // Find libknot_runtime.a by walking up from OUT_DIR to the target profile dir
    let runtime_path = out_path
        .ancestors()
        .map(|p| p.join("libknot_runtime.a"))
        .find(|p| p.exists());

    // Copy the runtime into OUT_DIR so include_bytes! can find it with a stable path
    let dest = out_path.join("libknot_runtime.a");
    if let Some(src) = runtime_path {
        std::fs::copy(&src, &dest).expect("failed to copy libknot_runtime.a to OUT_DIR");
        println!("cargo:rustc-cfg=has_embedded_runtime");
    }

    println!("cargo:rustc-check-cfg=cfg(has_embedded_runtime)");
    println!("cargo:rerun-if-changed=../knot-runtime/src/lib.rs");
}
