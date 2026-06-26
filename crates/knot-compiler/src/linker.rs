//! Invokes the system linker to combine a Cranelift-generated object file
//! with the knot runtime static library into an executable.

use std::path::Path;
use std::process::Command;

pub fn link(
    object_path: &Path,
    runtime_path: &Path,
    output_path: &Path,
) -> Result<(), String> {
    let mut cmd = Command::new("cc");
    cmd.arg("-o")
        .arg(output_path)
        .arg(object_path)
        .arg(runtime_path);

    // On macOS, link system libraries needed by the Rust runtime
    if cfg!(target_os = "macos") {
        cmd.arg("-lSystem").arg("-lresolv").arg("-liconv");
    } else if cfg!(target_os = "linux") {
        cmd.arg("-lpthread").arg("-ldl").arg("-lm");
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
