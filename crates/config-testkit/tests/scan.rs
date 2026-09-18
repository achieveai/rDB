//! Proves the anti-flake source scanners actually catch what they name, and that the
//! `// testkit:allow-sleep` / `// testkit:allow-port` markers exempt a deliberate line
//! (test plan §6 rules 1 and 4).

/// The "violating" fixture is assembled at runtime from split tokens.
///
/// Written out literally it would be a violation *of this very file*, and it must stay
/// unmarked or [`scan_catches_unmarked_fixed_sleep_and_literal_port`] would be exempting the
/// thing it claims to catch. Splitting the tokens is the only version that is honest in both
/// directions: the scanner sees them joined, the self-scan over `tests/` does not.
fn fixture_with_violations() -> String {
    let sleep_call = format!("tokio::time::{}(d);", "sleep");
    let literal_addr = format!("let _addr = \"127.0.0.1:{}\";", 5000);
    format!("fn unmarked_violations() {{\n    {sleep_call}\n    {literal_addr}\n}}\n")
}

const FIXTURE_ALL_MARKED: &str = r#"
fn marked_and_exempt() {
    tokio::time::sleep(std::time::Duration::from_millis(10)); // testkit:allow-sleep
    let _addr = "127.0.0.1:5001"; // testkit:allow-port
}
"#;

#[config_log::retcd_test]
fn scan_catches_unmarked_fixed_sleep_and_literal_port() {
    let dir = config_testkit::fs::temp_dir();
    std::fs::write(dir.path().join("fixture.rs"), fixture_with_violations())
        .expect("write fixture");

    let sleeps = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        config_testkit::scan::assert_no_fixed_sleeps(dir.path())
    }));
    assert!(sleeps.is_err(), "an unmarked fixed sleep must be caught");

    let ports = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        config_testkit::scan::assert_no_literal_ports(dir.path())
    }));
    assert!(ports.is_err(), "an unmarked literal port must be caught");
}

#[config_log::retcd_test]
fn scan_accepts_lines_carrying_the_allow_markers() {
    let dir = config_testkit::fs::temp_dir();
    std::fs::write(dir.path().join("fixture.rs"), FIXTURE_ALL_MARKED).expect("write fixture");

    // Must not panic.
    config_testkit::scan::assert_no_fixed_sleeps(dir.path());
    config_testkit::scan::assert_no_literal_ports(dir.path());
}

#[config_log::retcd_test]
fn scan_ignores_the_ephemeral_port_zero() {
    let dir = config_testkit::fs::temp_dir();
    std::fs::write(
        dir.path().join("fixture.rs"),
        "fn ok() { let _a = \"127.0.0.1:0\"; }\n",
    )
    .expect("write fixture");

    // Port 0 (ephemeral) is never a violation, marked or not.
    config_testkit::scan::assert_no_literal_ports(dir.path());
}

/// Crates whose `tests/` directory anti-flake rules 1 and 4 are asserted over.
///
/// Scanning only this crate's own tests was the original rule, and it left the majority of the
/// workspace's integration tests unscanned — a fixed sleep in `config-engine/tests` is exactly
/// as flaky as one here. `config-core` is deliberately absent: its `m0_*` tests are pure unit
/// tests over value types, and the literal endpoint strings they build are data, not sockets.
const SCANNED_CRATES: &[&str] = &[
    "config-testkit",
    "config-server",
    "config-engine",
    "config-grpc",
    "config-client",
    "config-storage",
];

/// Whole-file exemptions. Empty on purpose: every bounded poll interval in the workspace
/// (including `config-engine/tests/common/mod.rs`) carries a line-level marker, so a new fixed
/// sleep anywhere is a scan failure rather than a shrug.
const EXEMPT_FILES: &[&str] = &[];

/// The workspace root, derived from this crate's manifest directory (`<root>/crates/<crate>`).
fn workspace_root() -> std::path::PathBuf {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .ancestors()
        .nth(2)
        .expect("the manifest dir is <workspace>/crates/config-testkit")
        .to_path_buf()
}

/// The scanners run over every integration-test tree in the workspace, which is where
/// anti-flake rules 1 and 4 actually have to hold. A self-scan is the only version of this
/// rule that cannot rot.
#[config_log::retcd_test]
fn workspace_tests_contain_no_fixed_sleeps_or_literal_ports() {
    let root = workspace_root();
    let mut scanned = 0usize;
    for crate_name in SCANNED_CRATES {
        let tests = root.join("crates").join(crate_name).join("tests");
        assert!(
            tests.is_dir(),
            "{} is not a directory: SCANNED_CRATES names a crate that moved or was renamed, and a silent skip would turn this test into a no-op",
            tests.display()
        );
        config_testkit::scan::assert_no_fixed_sleeps_except(&tests, EXEMPT_FILES);
        config_testkit::scan::assert_no_literal_ports_except(&tests, EXEMPT_FILES);
        scanned += 1;
    }
    assert_eq!(
        scanned,
        SCANNED_CRATES.len(),
        "not every crate in SCANNED_CRATES was scanned"
    );
}
