//! The harness: run a scenario, record a trace, replay it.
//!
//! Package I1. The seam every other team writes tests against, and the only place the simulator
//! and the kernel meet.
//!
//! | Module | What it owns |
//! |---|---|
//! | [`self::dispatch`] | the module table, the adopted authority triple, and the effect-to-event hop |
//! | [`self::trace`] | recording [`rdb_core::contracts::trace::Trace`] and reading and writing JSONL |
//! | [`self::manifest`] | resolving a run's budgets into [`rdb_core::contracts::trace::RunManifest`] |
//! | [`self::replay`] | re-running a recorded trace and proving the result is identical |
//!
//! # State
//!
//! Dispatch, recording and the manifest are real. Replay is owed and says so.
//! [`environment_capabilities`] is the honest summary the rows log.

pub mod dispatch;
pub mod manifest;
pub mod replay;
pub mod trace;

use rdb_core::contracts::trace::{CapabilityState, PackageId};

/// What the three environment packages report about themselves, in package order.
///
/// The same rule as [`rdb_core::contracts::event::Module::capability`]: `Wired` is claimed only
/// when nothing in the package still answers [`crate::error::SimError::Unavailable`].
///
/// * H1 — `Unavailable`: [`crate::sim::network::Network::send`] and
///   [`crate::sim::cluster::Cluster::suspend`] are owed. The scheduler, clock, control store
///   and cluster lifecycle are real.
/// * M1 — `Wired`: the memory engine, crash images and snapshots are real.
/// * I1 — `Unavailable`: [`self::replay::replay`] is owed. Dispatch, recording and the manifest
///   are real.
#[must_use]
pub const fn environment_capabilities() -> [(PackageId, CapabilityState); 3] {
    [
        (PackageId::H1, CapabilityState::Unavailable),
        (PackageId::M1, CapabilityState::Wired),
        (PackageId::I1, CapabilityState::Unavailable),
    ]
}
