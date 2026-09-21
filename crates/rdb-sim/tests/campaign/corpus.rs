//! The shared corpus description, and the coverage record a campaign builds from it.
//!
//! One corpus per run (plan §2): the rows read the same report rather than each starting their
//! own, because the aggregate budget is what the layered-run scheme rests on.

use std::collections::BTreeMap;

use rdb_core::contracts::trace::{CapabilityState, PackageId};

use crate::support::scenarios::coverage::{self, Axis, CoverageReport};

/// The default corpus a PR runs (design §5.1).
pub const DEFAULT_SEEDS: usize = 64;

/// The extended corpus, and the release run's.
pub const EXTENDED_SEEDS: usize = 1_000;

/// The capability block this build would stamp on every trace.
///
/// Derived from [`rdb_sim::harness::environment_capabilities`] and the dispatcher's own report —
/// never a literal table, which is what row **M7V-82** exists to keep true.
#[must_use]
pub fn capabilities() -> BTreeMap<PackageId, CapabilityState> {
    let mut block: BTreeMap<PackageId, CapabilityState> = PACKAGES
        .iter()
        .map(|package| (*package, CapabilityState::Unavailable))
        .collect();
    for (package, state) in rdb_sim::harness::environment_capabilities() {
        block.insert(package, state);
    }
    block
}

/// Every package a capability block must carry a row for.
///
/// `PackageId` has no `ALL` in the landed contract, so this list is declared here and row
/// **M7V-82** asserts it is complete against the enum by construction: adding a package without
/// adding it here leaves a capability block that silently omits it.
pub const PACKAGES: [PackageId; 10] = [
    PackageId::C0,
    PackageId::H1,
    PackageId::M1,
    PackageId::I1,
    PackageId::A1,
    PackageId::T1,
    PackageId::R1,
    PackageId::P1,
    PackageId::L1,
    PackageId::F1,
];

/// A coverage record with every required cell hit, as a starting point a row then breaks.
///
/// Synthetic on purpose: row **M7V-57** is the negative control for the coverage gate, and it
/// needs to remove exactly one cell from an otherwise complete record. Building that record from
/// a real run would make the row depend on the run's luck.
#[must_use]
pub fn complete_hits() -> BTreeMap<(Axis, String), u32> {
    coverage::required_cells()
        .into_iter()
        .map(|cell| (cell, 1))
        .collect()
}

/// Judge a hit map at a seed count against this build's capabilities.
#[must_use]
pub fn evaluate(seeds: usize, hits: &BTreeMap<(Axis, String), u32>) -> CoverageReport {
    coverage::evaluate(seeds, hits, &capabilities())
}
