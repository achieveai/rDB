//! M0-58..M0-61 — purity is machine-checked, not reviewed (TA-9, spec §21 M0 bullet 4).
//!
//! Review cannot hold this line: a clock, an ambient lookup, or an unordered iteration added
//! three refactors from now would pass code review and break replica agreement in production.
//! These tests read the crate's own source and manifest and fail with a file and line.

use std::path::{Path, PathBuf};

/// Tokens that must not appear in `config-core`'s source.
///
/// Each one is a way for apply to stop being a function of its inputs: a clock or ambient
/// lookup makes two replicas disagree, an unordered collection makes one replica disagree
/// with itself, and an async runtime or network crate breaks ADR-0004's dependency direction.
const FORBIDDEN: &[&str] = &[
    "std::time",
    "SystemTime",
    "Instant",
    "std::env",
    "env!",
    "std::fs",
    "rand",
    "thread_rng",
    "HashMap",
    "HashSet",
    "std::net",
    "tokio",
    "reqwest",
];

/// A line carrying this marker is exempt. It exists so a deliberate, reviewed exception is
/// visible in the diff rather than silently widening the scan.
const ALLOW_MARKER: &str = "purity-allow";

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("readable source directory") {
        let path = entry.expect("readable entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[config_log::retcd_test]
fn m0_58_source_scan_no_ambient_inputs() {
    let src = crate_root().join("src");
    let mut files = Vec::new();
    rust_sources(&src, &mut files);
    assert!(!files.is_empty(), "the scan must actually find sources");

    let mut findings = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file).expect("readable source file");
        for (index, line) in text.lines().enumerate() {
            if line.contains(ALLOW_MARKER) {
                continue;
            }
            for token in FORBIDDEN {
                if line.contains(token) {
                    findings.push(format!(
                        "{}:{}: forbidden `{token}` in: {}",
                        file.display(),
                        index + 1,
                        line.trim()
                    ));
                }
            }
        }
    }

    assert!(
        findings.is_empty(),
        "config-core source must not reference ambient inputs, unordered collections, or a \
         runtime:\n{}",
        findings.join("\n")
    );
}

#[config_log::retcd_test]
fn m0_59_dependency_surface_is_minimal() {
    let manifest = std::fs::read_to_string(crate_root().join("Cargo.toml")).expect("manifest");

    let (deps, dev_deps) = split_sections(&manifest);

    // `bytes`, `serde`, `thiserror` and `sha2` carry the data model and the determinism
    // oracle; `async-trait` is what makes `ConfigStore` object-safe; `tracing` is a facade
    // with no runtime of its own (ADR-0013). Nothing else may enter.
    let allowed = [
        "bytes",
        "serde",
        "thiserror",
        "async-trait",
        "tracing",
        "sha2",
    ];
    for name in &deps {
        assert!(
            allowed.contains(&name.as_str()),
            "unexpected config-core dependency `{name}`; ADR-0004 keeps this crate free of \
             runtime, storage, consensus, and transport dependencies"
        );
    }

    let forbidden = [
        "tokio",
        "openraft",
        "rocksdb",
        "tonic",
        "prost",
        "memberlist",
        "rand",
        "chrono",
        "time",
        "reqwest",
        "uuid",
    ];
    for name in deps.iter().chain(dev_deps.iter()) {
        assert!(
            !forbidden.contains(&name.as_str()),
            "`{name}` must not be a dependency of config-core, not even for tests"
        );
    }

    for required in ["config-log", "proptest"] {
        assert!(
            dev_deps.iter().any(|d| d == required),
            "`{required}` is required: every test opens the test-context span (TA-8)"
        );
    }
}

/// Return `(dependencies, dev-dependencies)` crate names from the manifest.
fn split_sections(manifest: &str) -> (Vec<String>, Vec<String>) {
    let mut deps = Vec::new();
    let mut dev = Vec::new();
    let mut current: Option<&mut Vec<String>> = None;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            current = match trimmed {
                "[dependencies]" => Some(&mut deps),
                "[dev-dependencies]" => Some(&mut dev),
                _ => None,
            };
            continue;
        }
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some(target) = current.as_deref_mut() {
            if let Some((name, _)) = trimmed.split_once('=') {
                target.push(name.trim().to_string());
            }
        }
    }
    (deps, dev)
}

/// `apply` must be callable from an ordinary synchronous function, with no runtime, no
/// OpenRaft types in scope, and no `Result` to unwrap.
#[config_log::retcd_test]
fn m0_60_apply_signature_is_sync_and_total() {
    use config_core::{Command, CommandResponse, KvState};

    // If `apply` were async, fallible, or took `self` by value, this coercion would not
    // compile.
    const APPLY: fn(&mut KvState, &Command) -> CommandResponse = KvState::apply;

    fn plain_synchronous_caller() -> CommandResponse {
        let mut state = KvState::new();
        APPLY(
            &mut state,
            &Command::Put {
                key: bytes::Bytes::from_static(b"k"),
                value: bytes::Bytes::from_static(b"v"),
                expected_mod_revision: None,
            },
        )
    }

    assert!(plain_synchronous_caller().is_applied());

    // Total: even a command that can never be valid produces a response rather than a panic
    // or an `Err`.
    let mut state = KvState::new();
    let response = state.apply(&Command::Delete {
        key: bytes::Bytes::new(),
        expected_mod_revision: Some(0),
    });
    assert!(matches!(response, CommandResponse::Rejected { .. }));
}

/// A `HashMap` that happens to iterate consistently within one run would pass a weaker test.
/// Inserting in a shuffled-but-reproducible order and comparing against the sorted
/// expectation catches it.
#[config_log::retcd_test]
fn m0_61_no_unordered_iteration_in_output() {
    use config_core::{Command, KvState, ListRequest};

    // A small deterministic permutation generator; the crate under test may not use one, but
    // the test needs a shuffle that is identical on every run and every platform.
    let mut order: Vec<u32> = (0..200).collect();
    let mut seed: u64 = 0x2545_F491_4F6C_DD1D;
    for i in (1..order.len()).rev() {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (seed >> 33) as usize % (i + 1);
        order.swap(i, j);
    }

    let mut state = KvState::new();
    for n in &order {
        let key = format!("key-{n:04}");
        state.apply(&Command::Put {
            key: bytes::Bytes::from(key.into_bytes()),
            value: bytes::Bytes::from(n.to_le_bytes().to_vec()),
            expected_mod_revision: None,
        });
    }

    let request = ListRequest {
        prefix: bytes::Bytes::from_static(b"key-"),
        max_items: 0,
        max_bytes: 0,
    };
    let first = state.list(&request);
    let second = state.list(&request);
    let hash_first = state.state_hash();
    let hash_second = state.state_hash();

    assert_eq!(
        first, second,
        "two identical lists in one process must agree"
    );
    assert_eq!(hash_first, hash_second);

    let mut expected: Vec<String> = (0..200).map(|n| format!("key-{n:04}")).collect();
    expected.sort();
    let actual: Vec<String> = first
        .records
        .iter()
        .map(|record| String::from_utf8(record.key.to_vec()).expect("ascii key"))
        .collect();
    assert_eq!(
        actual, expected,
        "output order is sorted, not insertion order"
    );
}
