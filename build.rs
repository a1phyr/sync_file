use std::env;
use std::process::Command;

fn builds_for_wasi() -> bool {
    env::var_os("CARGO_CFG_TARGET_OS").map_or(false, |target| target == "wasi")
}

fn uses_nightly() -> bool {
    (|| {
        let rustc = env::var_os("RUSTC")?;
        let out = Command::new(rustc).arg("--version").output().ok()?;

        Some(out.status.success() && String::from_utf8(out.stdout).ok()?.contains("nightly"))
    })()
    .unwrap_or(false)
}

fn main() {
    if builds_for_wasi() && uses_nightly() {
        println!("cargo:rustc-cfg=wasi_ext");
    }
}
