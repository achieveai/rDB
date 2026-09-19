//! Anti-flake source scanners (test plan §6 rules 1 and 4).
//!
//! Mirrors the mechanism `config-core`'s `m0_purity` tests use for TA-9: reviewers cannot
//! reliably catch a fixed sleep or a literal port three refactors from now, so these scan the
//! actual source text and fail with a file and line.
//!
//! A line that is deliberately exempt (the *subject* of a test, e.g. an explicit
//! `NetFault::delay`, or an intentional fixture string) carries an allow marker:
//! `// testkit:allow-sleep` or `// testkit:allow-port`.

use std::path::{Path, PathBuf};

/// Marker that exempts a line from [`assert_no_fixed_sleeps`].
pub const ALLOW_SLEEP_MARKER: &str = "testkit:allow-sleep";
/// Marker that exempts a line from [`assert_no_literal_ports`].
pub const ALLOW_PORT_MARKER: &str = "testkit:allow-port";

/// Tokens that mean "a fixed sleep used as synchronization" (rule 1).
const SLEEP_TOKENS: &[&str] = &["tokio::time::sleep", "thread::sleep", "yield_now"];

/// Loopback/any-host prefixes checked for a literal non-ephemeral port (rule 4).
const HOST_PREFIXES: &[&str] = &["127.0.0.1:", "0.0.0.0:", "localhost:", "[::1]:"];

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Whether `file` ends with one of `exempt` (a `/`-separated path suffix, e.g.
/// `"tests/common/mod.rs"`). Windows separators are normalised so a suffix written the POSIX
/// way matches on both platforms.
fn is_exempt(file: &Path, exempt: &[&str]) -> bool {
    if exempt.is_empty() {
        return false;
    }
    let normalised = file
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    exempt.iter().any(|suffix| normalised.ends_with(suffix))
}

fn scan_lines(
    dir: &Path,
    allow_marker: &str,
    exempt: &[&str],
    mut check: impl FnMut(&str) -> Option<String>,
) -> Vec<String> {
    let mut files = Vec::new();
    rust_sources(dir, &mut files);
    let mut findings = Vec::new();
    for file in &files {
        if is_exempt(file, exempt) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(file) else {
            continue;
        };
        for (index, line) in text.lines().enumerate() {
            if line.contains(allow_marker) {
                continue;
            }
            if let Some(what) = check(line) {
                findings.push(format!(
                    "{}:{}: {what} in: {}",
                    file.display(),
                    index + 1,
                    line.trim()
                ));
            }
        }
    }
    findings
}

/// Fail if `dir` (scanned recursively) contains `tokio::time::sleep`, `thread::sleep`, or a
/// `yield_now` busy-loop outside a line carrying [`ALLOW_SLEEP_MARKER`] (rule 1: "no fixed
/// sleeps... allowlisting only sleeps that are the *subject* of a test").
pub fn assert_no_fixed_sleeps(dir: &Path) {
    assert_no_fixed_sleeps_except(dir, &[]);
}

/// [`assert_no_fixed_sleeps`], skipping files whose path ends with one of `exempt_files`.
///
/// This exists for one situation: a file that *should* carry a line-level
/// [`ALLOW_SLEEP_MARKER`] but belongs to a crate the caller does not own, so the marker cannot
/// be added there. A whole-file exemption is weaker than a line marker — it hides future
/// violations in that file too — so every entry must be justified at the call site, and the
/// marker should be pushed into the file itself as soon as its owner can take it.
pub fn assert_no_fixed_sleeps_except(dir: &Path, exempt_files: &[&str]) {
    let findings = scan_lines(dir, ALLOW_SLEEP_MARKER, exempt_files, |line| {
        SLEEP_TOKENS
            .iter()
            .find(|tok| line.contains(*tok))
            .map(|tok| format!("forbidden fixed sleep `{tok}`"))
    });
    assert!(
        findings.is_empty(),
        "fixed sleeps used as synchronization (test plan §6 rule 1); mark a deliberate exception \
         with `// {ALLOW_SLEEP_MARKER}`:\n{}",
        findings.join("\n")
    );
}

/// Fail if `dir` (scanned recursively) contains a literal loopback/any-host port other than
/// `0` outside a line carrying [`ALLOW_PORT_MARKER`] (rule 4: "no literal port anywhere in
/// `tests/**`").
pub fn assert_no_literal_ports(dir: &Path) {
    assert_no_literal_ports_except(dir, &[]);
}

/// [`assert_no_literal_ports`], skipping files whose path ends with one of `exempt_files`.
/// See [`assert_no_fixed_sleeps_except`] for when a whole-file exemption is justified.
pub fn assert_no_literal_ports_except(dir: &Path, exempt_files: &[&str]) {
    let findings = scan_lines(dir, ALLOW_PORT_MARKER, exempt_files, |line| {
        for prefix in HOST_PREFIXES {
            for (idx, _) in line.match_indices(prefix) {
                let after = idx + prefix.len();
                let digits: String = line[after..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                if !digits.is_empty() && digits != "0" {
                    return Some(format!("literal non-ephemeral port `{prefix}{digits}`"));
                }
            }
        }
        None
    });
    assert!(
        findings.is_empty(),
        "literal non-ephemeral port (test plan §6 rule 4); bind `127.0.0.1:0` and mark a \
         deliberate exception with `// {ALLOW_PORT_MARKER}`:\n{}",
        findings.join("\n")
    );
}
