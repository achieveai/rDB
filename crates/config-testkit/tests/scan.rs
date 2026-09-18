//! Proves the anti-flake source scanners actually catch what they name, and that the
//! `// testkit:allow-sleep` / `// testkit:allow-port` markers exempt a deliberate line
//! (test plan §6 rules 1 and 4).

const FIXTURE_WITH_VIOLATIONS: &str = r#"
fn unmarked_violations() {
    tokio::time::sleep(std::time::Duration::from_millis(10));
    let _addr = "127.0.0.1:5000";
}
"#;

const FIXTURE_ALL_MARKED: &str = r#"
fn marked_and_exempt() {
    tokio::time::sleep(std::time::Duration::from_millis(10)); // testkit:allow-sleep
    let _addr = "127.0.0.1:5001"; // testkit:allow-port
}
"#;

#[config_log::retcd_test]
fn scan_catches_unmarked_fixed_sleep_and_literal_port() {
    let dir = config_testkit::fs::temp_dir();
    std::fs::write(dir.path().join("fixture.rs"), FIXTURE_WITH_VIOLATIONS).expect("write fixture");

    let sleeps = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        config_testkit::scan::assert_no_fixed_sleeps(dir.path())
    }));
    assert!(
        sleeps.is_err(),
        "an unmarked tokio::time::sleep must be caught"
    );

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
