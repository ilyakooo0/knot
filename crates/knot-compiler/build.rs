use std::path::{Path, PathBuf};

fn is_valid_lib(p: &Path) -> bool {
    p.exists() && std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false)
}

/// Max mtime across all runtime source files: every `*.rs` under
/// `crates/knot-runtime/src` (recursively — the crate has lib.rs, log.rs,
/// tui.rs and may gain more) plus the runtime crate's Cargo.toml.
fn runtime_src_mtime(workspace_root: &Path) -> Option<std::time::SystemTime> {
    fn consider(p: &Path, newest: &mut Option<std::time::SystemTime>) {
        if let Ok(m) = std::fs::metadata(p).and_then(|m| m.modified())
            && newest.map(|n| m > n).unwrap_or(true) {
                *newest = Some(m);
            }
    }
    fn walk(dir: &Path, newest: &mut Option<std::time::SystemTime>) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for entry in rd.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    walk(&p, newest);
                } else if p.extension().map(|e| e == "rs").unwrap_or(false) {
                    consider(&p, newest);
                }
            }
        }
    }
    let mut newest = None;
    walk(&workspace_root.join("crates/knot-runtime/src"), &mut newest);
    consider(&workspace_root.join("crates/knot-runtime/Cargo.toml"), &mut newest);
    newest
}

/// Check that `lib` is at least as new as every runtime source file.
fn is_fresh_lib(lib: &Path, workspace_root: &Path) -> bool {
    if !is_valid_lib(lib) {
        return false;
    }
    let src_mtime = runtime_src_mtime(workspace_root);
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

    // Find libknot_runtime.a — try multiple strategies. NOTE: this build
    // script deliberately does NOT spawn a nested `cargo build` — doing so
    // deadlocks on the package-cache lock whenever the outer build holds it
    // (the common case). Staleness is instead handled at LINK time by
    // `rebuild_runtime_at_exe` in main.rs, which runs as its own process and
    // can safely invoke cargo. Here we only locate an existing archive.
    let runtime_path = None
        // 1. Explicit env var override
        .or_else(|| {
            std::env::var("KNOT_RUNTIME_LIB")
                .ok()
                .map(PathBuf::from)
                .filter(|p| is_valid_lib(p))
        })
        // 2. Walk up from OUT_DIR (works in normal `cargo build` within
        //    workspace), but stop at the cargo target directory boundary. Going
        //    past it would let a stray libknot_runtime.a in an unrelated
        //    ancestor (e.g. $HOME) be embedded on mtime alone.
        .or_else(|| {
            let target_dir = std::env::var("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| workspace_root.join("target"));
            // Canonicalize so the `..`-laden workspace_root path compares
            // correctly against cargo's canonical OUT_DIR ancestors.
            let boundary = std::fs::canonicalize(&target_dir).unwrap_or(target_dir.clone());
            out_path
                .ancestors()
                .take_while(|p| p.starts_with(&boundary))
                .map(|p| p.join("libknot_runtime.a"))
                .chain(
                    ["release", "debug"]
                        .iter()
                        .map(|profile| target_dir.join(profile).join("libknot_runtime.a")),
                )
                .find(|p| is_fresh_lib(p, &workspace_root))
        });

    // Copy the runtime into OUT_DIR so include_bytes! can find it with a stable path
    if let Some(src) = runtime_path {
        std::fs::copy(&src, &dest).expect("failed to copy libknot_runtime.a to OUT_DIR");
        println!("cargo:rustc-cfg=has_embedded_runtime");
        // Stamp the content hash of the archive we're embedding. At link time
        // the compiler hashes any on-disk candidate and compares — a match
        // means byte-identical bits (fresh), regardless of mtimes or which
        // archive produced them.
        let bytes = std::fs::read(&dest).expect("failed to read embedded runtime for hashing");
        let hash = blake3::hash(&bytes).to_hex();
        println!("cargo:rustc-env=KNOT_EMBEDDED_RUNTIME_HASH={hash}");
    }

    println!("cargo:rustc-check-cfg=cfg(has_embedded_runtime)");

    // ── JIT compile-runtime archive (knot-compile-rt) ──
    // Always linked into compiled programs so `base.compile` works. Found in
    // the target dir (the `knot` binary depends on knot-compile-rt, so cargo
    // builds it first); embedded so the compiler binary stays self-contained.
    // No self-spawned build here (unlike the runtime fallback) — knot-compile-rt
    // is always a normal dependency of the binary that drives linking.
    {
        let crt_dest = out_path.join("libknot_compile_rt.a");
        let _ = std::fs::remove_file(&crt_dest);
        let target_dir = std::env::var("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| workspace_root.join("target"));
        let crt_src = out_path
            .ancestors()
            .take_while(|p| p.starts_with(&target_dir))
            .map(|p| p.join("libknot_compile_rt.a"))
            .find(|p| is_valid_lib(p))
            .or_else(|| {
                ["release", "debug"]
                    .iter()
                    .map(|prof| target_dir.join(prof).join("libknot_compile_rt.a"))
                    .find(|p| is_valid_lib(p))
            });
        if let Some(src) = crt_src {
            std::fs::copy(&src, &crt_dest)
                .expect("failed to copy libknot_compile_rt.a to OUT_DIR");
            println!("cargo:rustc-cfg=has_embedded_compile_rt");
        }
    }
    println!("cargo:rustc-check-cfg=cfg(has_embedded_compile_rt)");
    println!("cargo:rerun-if-changed=../knot-compile-rt/src");
    println!("cargo:rerun-if-changed=../knot-compile-rt/Cargo.toml");
    // Watch the whole runtime source tree (cargo tracks directories
    // recursively), not just lib.rs — log.rs/tui.rs edits must also
    // refresh the embedded runtime.
    println!("cargo:rerun-if-changed=../knot-runtime/src");
    println!("cargo:rerun-if-changed=../knot-runtime/Cargo.toml");

    // A-experiment: whole-archive link the knot-runtime staticlib into the
    // knot binary and export its knot_* symbols into the dynamic table so the
    // JIT (running in-process for compile-time predicate eval) can resolve
    // them via dlsym(RTLD_DEFAULT). The runtime is found the same way as the
    // embedded archive above; this only ADDS link directives.
    // --allow-multiple-definition: the staticlib bundles compiler_builtins
    // objects that the binary already links from rustc's rlib — whole-archive
    // would otherwise double-define them. The first (rustc) definition wins.
    {
        let target_dir = std::env::var("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| workspace_root.join("target"));
        let rt = out_path
            .ancestors()
            .take_while(|p| p.starts_with(&target_dir))
            .map(|p| p.join("libknot_runtime.a"))
            .find(|p| is_valid_lib(p))
            .or_else(|| {
                ["release", "debug"]
                    .iter()
                    .map(|prof| target_dir.join(prof).join("libknot_runtime.a"))
                    .find(|p| is_valid_lib(p))
            });
        if let Some(rt) = rt {
            if cfg!(target_os = "macos") {
                println!("cargo:rustc-link-arg=-Wl,-force_load,{}", rt.display());
                println!("cargo:rustc-link-arg=-Wl,-export_dynamic");
            } else {
                println!("cargo:rustc-link-arg=-Wl,--allow-multiple-definition");
                println!(
                    "cargo:rustc-link-arg=-Wl,--whole-archive,{},--no-whole-archive",
                    rt.display()
                );
                println!("cargo:rustc-link-arg=-Wl,--export-dynamic");
            }
        }
    }
}
