use std::path::PathBuf;

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_path = PathBuf::from(&out_dir);
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let workspace_root = PathBuf::from(&manifest_dir).join("../..");

    // Find libknot_runtime.a — try multiple strategies:
    let runtime_path = None
        // 1. Explicit env var override
        .or_else(|| {
            std::env::var("KNOT_RUNTIME_LIB")
                .ok()
                .map(PathBuf::from)
                .filter(|p| p.exists())
        })
        // 2. Walk up from OUT_DIR (works in normal `cargo build` within workspace)
        .or_else(|| {
            out_path
                .ancestors()
                .map(|p| p.join("libknot_runtime.a"))
                .find(|p| p.exists())
        })
        // 3. Check workspace target directory (works during `cargo install --path`)
        .or_else(|| {
            ["release", "debug"]
                .iter()
                .map(|profile| workspace_root.join("target").join(profile).join("libknot_runtime.a"))
                .find(|p| p.exists())
        });

    // Copy the runtime into OUT_DIR so include_bytes! can find it with a stable path
    let dest = out_path.join("libknot_runtime.a");
    if let Some(src) = runtime_path {
        std::fs::copy(&src, &dest).expect("failed to copy libknot_runtime.a to OUT_DIR");
        println!("cargo:rustc-cfg=has_embedded_runtime");
    }

    println!("cargo:rustc-check-cfg=cfg(has_embedded_runtime)");
    println!("cargo:rerun-if-changed=../knot-runtime/src/lib.rs");
}
