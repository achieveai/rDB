//! PR #156 review, #127: `scripts/local-rdb.sh sigstop N` and `sigcont N` refuse on Windows
//! (MSYS), which has no SIGSTOP, and point to `suspend N` / `resume N`. Each row runs the
//! script with a `uname` shim first on `PATH` that prints `MINGW64_NT-10.0`, against a run
//! folder that does not exist, so no signal is ever sent. The refusal must come before the run
//! lookup: without the guard the script still exits nonzero, saying the run folder is gone, so
//! the rows check the words, not only the exit code.

use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the repo root")
}

/// `local-rdb.sh <verb> 2 --run <missing>` with `uname` shimmed to MSYS: its exit code and
/// stderr.
fn lr_on_msys(verb: &str) -> (Option<i32>, String) {
    let dir = config_testkit::fs::temp_dir();
    let shims = dir.path().join("shims");
    std::fs::create_dir_all(&shims).expect("shim folder");
    let uname = shims.join("uname");
    std::fs::write(&uname, "#!/bin/sh\necho MINGW64_NT-10.0\n").expect("uname shim");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&uname, std::fs::Permissions::from_mode(0o755))
            .expect("shim is executable");
    }
    let missing = dir.path().join("no-such-run");
    assert!(!missing.exists());
    let path = std::env::join_paths(std::iter::once(shims).chain(std::env::split_paths(
        &std::env::var_os("PATH").expect("PATH is set"),
    )))
    .expect("a PATH");
    let script = repo_root().join("scripts/local-rdb.sh");
    let out = Command::new("bash")
        .arg(&script)
        .arg(verb)
        .arg("2")
        .arg("--run")
        .arg(&missing)
        .env("PATH", path)
        .output()
        .expect("bash runs the script");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn refuses_on_msys(verb: &str) {
    let (code, stderr) = lr_on_msys(verb);
    assert_eq!(code, Some(1), "{verb}: {stderr}");
    assert!(
        stderr.contains(&format!(
            "lr: {verb} needs POSIX signals, which Windows lacks"
        )) && stderr.contains("use `suspend 2` / `resume 2` instead"),
        "{verb}: {stderr}"
    );
    assert!(
        !stderr.contains("run folder"),
        "{verb} refused before the run lookup: {stderr}"
    );
}

/// `sigstop 2` on MSYS refuses, naming POSIX signals and `suspend 2` / `resume 2`.
/// Cut: drop the `is_msys` guard from the `sigstop | sigcont` arm; the script says the run
/// folder is gone instead, red.
#[test]
fn pr156_lr_sigstop_refuses_on_msys() {
    refuses_on_msys("sigstop");
}

/// `sigcont 2` on MSYS refuses the same way. Cut: as `pr156_lr_sigstop_refuses_on_msys`, red.
#[test]
fn pr156_lr_sigcont_refuses_on_msys() {
    refuses_on_msys("sigcont");
}
