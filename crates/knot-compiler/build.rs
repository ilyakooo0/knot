use std::path::{Path, PathBuf};

fn is_valid_lib(p: &Path) -> bool {
    p.exists() && std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false)
}

/// Check that `lib` is at least as new as the runtime source file.
fn is_fresh_lib(lib: &Path, workspace_root: &Path) -> bool {
    if !is_valid_lib(lib) {
        return false;
    }
    let src = workspace_root.join("crates/knot-runtime/src/lib.rs");
    let src_mtime = std::fs::metadata(&src).and_then(|m| m.modified()).ok();
    let lib_mtime = std::fs::metadata(lib).and_then(|m| m.modified()).ok();
    match (src_mtime, lib_mtime) {
        (Some(s), Some(l)) => l >= s,
        _ => true, // can't compare, assume valid
    }
}

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_path = PathBuf::from(&out_dir);
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let workspace_root = PathBuf::from(&manifest_dir).join("../..");

    // Remove stale dest file so it doesn't interfere with the search
    let dest = out_path.join("libknot_runtime.a");
    let _ = std::fs::remove_file(&dest);

    // Find libknot_runtime.a — try multiple strategies:
    let runtime_path = None
        // 1. Explicit env var override
        .or_else(|| {
            std::env::var("KNOT_RUNTIME_LIB")
                .ok()
                .map(PathBuf::from)
                .filter(|p| is_valid_lib(p))
        })
        // 2. Walk up from OUT_DIR (works in normal `cargo build` within workspace)
        .or_else(|| {
            out_path
                .ancestors()
                .map(|p| p.join("libknot_runtime.a"))
                .find(|p| is_fresh_lib(p, &workspace_root))
        })
        // 3. Check workspace target directory, respecting CARGO_TARGET_DIR
        .or_else(|| {
            let target_dir = std::env::var("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| workspace_root.join("target"));
            ["release", "debug"]
                .iter()
                .map(|profile| target_dir.join(profile).join("libknot_runtime.a"))
                .find(|p| is_fresh_lib(p, &workspace_root))
        })
        // 4. Build the runtime ourselves (needed for `cargo install` where the
        //    staticlib isn't produced as a dependency artifact). Uses a separate
        //    target-dir inside OUT_DIR to avoid cargo lock contention.
        .or_else(|| try_build_runtime(&workspace_root, &out_path));

    // Copy the runtime into OUT_DIR so include_bytes! can find it with a stable path
    if let Some(src) = runtime_path {
        std::fs::copy(&src, &dest).expect("failed to copy libknot_runtime.a to OUT_DIR");
        println!("cargo:rustc-cfg=has_embedded_runtime");
    }

    println!("cargo:rustc-check-cfg=cfg(has_embedded_runtime)");
    println!("cargo:rerun-if-changed=../knot-runtime/src/lib.rs");
}

fn try_build_runtime(workspace_root: &Path, out_path: &Path) -> Option<PathBuf> {
    let runtime_manifest = workspace_root.join("crates/knot-runtime/Cargo.toml");
    if !runtime_manifest.exists() {
        return None;
    }

    let target_dir = out_path.join("rt-build");
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "release".into());
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());

    let mut cmd = std::process::Command::new(cargo);
    cmd.arg("build")
        .arg("-p")
        .arg("knot-runtime")
        .arg("--target-dir")
        .arg(&target_dir)
        .current_dir(workspace_root);
    if profile == "release" {
        cmd.arg("--release");
    }

    eprintln!("build.rs: building knot-runtime staticlib...");
    if cmd.status().map(|s| s.success()).unwrap_or(false) {
        let lib_path = target_dir
            .join(if profile == "release" { "release" } else { "debug" })
            .join("libknot_runtime.a");
        if is_valid_lib(&lib_path) {
            return Some(lib_path);
        }
    }

    None
}
