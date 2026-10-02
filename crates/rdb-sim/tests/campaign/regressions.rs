//! The regression corpus on disk: `tests/fixtures/regressions/<slug>.json` paired with
//! `<slug>.orig.json`.
//!
//! Retirement is a **deletion**, not an edit (row M7V-50): when a defect is fixed both files of
//! the pair go together and one line is added to ADR-rdb-0019's Notes. Nothing here may rewrite
//! an expectation, which is why this module reads and pairs and does not parse expectations at
//! all — the only expectation a fixture carries is that it fails, and the replaying row asserts
//! that against the oracle rather than against a field.

use std::collections::BTreeSet;
use std::path::PathBuf;

/// Where the pairs live.
#[must_use]
pub fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/regressions")
}

/// One pair.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Pair {
    /// The slug both files share.
    pub slug: String,
    /// The minimized fixture.
    pub minimized: PathBuf,
    /// The scenario it was shrunk from (critic F4's committed-original half).
    pub original: PathBuf,
}

/// Every complete pair, and every orphan.
///
/// Orphans are returned rather than skipped: a half-deleted pair that quietly vanished from the
/// corpus is the failure mode this split exists to make visible.
#[must_use]
pub fn scan() -> (Vec<Pair>, Vec<String>) {
    let dir = dir();
    let mut minimized: BTreeSet<String> = BTreeSet::new();
    let mut originals: BTreeSet<String> = BTreeSet::new();

    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(slug) = name.strip_suffix(".orig.json") {
                originals.insert(slug.to_owned());
            } else if let Some(slug) = name.strip_suffix(".json") {
                minimized.insert(slug.to_owned());
            }
        }
    }

    let pairs = minimized
        .intersection(&originals)
        .map(|slug| Pair {
            slug: slug.clone(),
            minimized: dir.join(format!("{slug}.json")),
            original: dir.join(format!("{slug}.orig.json")),
        })
        .collect();
    let orphans = minimized
        .symmetric_difference(&originals)
        .cloned()
        .collect();
    (pairs, orphans)
}
