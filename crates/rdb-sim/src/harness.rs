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
//! | [`self::run`] | the run loop: pop, route, step, deliver, record, stop for a named reason |
//! | [`self::replay`] | re-running a recorded run and proving the result is identical |
//!
//! # State
//!
//! Dispatch, recording, the manifest and the run loop are real.
//! [`self::replay::replay_run`] re-runs a [`self::run::RunPlan`] — the reproducer — and judges
//! the result. [`self::replay::replay`], the one that takes a bare
//! [`rdb_core::contracts::trace::Trace`], refuses **by design and permanently**: a `Trace` does
//! not carry the events that went into the run it records, so that signature cannot be honest.
//! What is genuinely owed is the four effect providers in [`self::dispatch`].
//! [`environment_capabilities`] is the honest summary the rows log.

pub mod dispatch;
pub mod hosted;
pub mod manifest;
pub mod protection;
pub mod replay;
pub mod route;
pub mod run;
pub mod trace;
pub mod transfer;

use rdb_core::contracts::event::ModuleName;
use rdb_core::contracts::trace::{CapabilityState, PackageId};

/// What the three environment packages report about themselves, in package order.
///
/// The same rule as [`rdb_core::contracts::event::Module::capability`]: `Wired` is claimed only
/// when nothing in the package still answers [`crate::error::SimError::Unavailable`].
///
/// * H1 — `Unavailable`: [`crate::sim::network::Network::send`] delivers, drops, duplicates,
///   delays and partitions under the scenario's plan, and delivers a forged acknowledgement under
///   its forged label (L-R177do). Two things are owed: a forged acknowledgement whose lie is in its
///   body (`sim::network::Network::forge_ack`: an authenticated claim to another role), and
///   [`crate::sim::cluster::Cluster::suspend`]. The scheduler, clock, control store and cluster
///   lifecycle are real.
/// * M1 — `Wired`: the memory engine, crash images and snapshots are real.
/// * I1 — `Unavailable`, and **ready to flip only by ruling** (A-R61 says do not flip it here).
///   Since 2026-09-26 [`self::dispatch::Dispatcher::deliver`] carries out `Send`, `Store`,
///   `Timer` and every kernel-to-kernel effect with a consumer, which it routes in the same run
///   ([`self::route`]). Three delivery seams are still refused by name, which row `M7F-26`
///   counts as I1's: `harness::dispatch::deliver::crash` (restarting a crashed node),
///   `::recovery` (F1's requests with no provider) and `::kernel` (R1's arms with no consumer),
///   plus `harness::run::route`, the stop on a routed fact a wired consumer declined.
///   Dispatch, recording, the manifest, the run loop ([`self::run::execute`]) and replay from a
///   plan ([`self::replay::replay_run`]) are real.
///
///   **This entry is not held back by [`self::replay::replay`].** That function refuses *by
///   design and permanently* (lead ruling, 2026-09-22): a [`rdb_core::contracts::trace::Trace`]
///   does not determine the run it records, so its signature cannot be honest, and the
///   reproducer ADR-rdb-0003 decision 6 names is [`self::run::RunPlan`], which exists. A seam
///   that is refused by design is not an owed seam, and nobody should read this entry as waiting
///   on it.
///
///   Read the rule above as written: `Wired` is claimed only when nothing in the package still
///   answers `Unavailable`, and the seams above do. Flipping this to `Wired` before they close
///   is the optimistic flip the rule at the top of this list forbids.
#[must_use]
pub const fn environment_capabilities() -> [(PackageId, CapabilityState); 3] {
    [
        (PackageId::H1, CapabilityState::Unavailable),
        (PackageId::M1, CapabilityState::Wired),
        (PackageId::I1, CapabilityState::Unavailable),
    ]
}

/// The packages a recorded trace's capability preamble contains, in the order it records them.
///
/// **Derived, never enumerated** (lead ruling F-2 and its amendment F-2a, 2026-09-22). The two
/// sources are exactly the two [`self::run::Runner::new`] reads when it writes the preamble:
/// [`environment_capabilities`] for the three environment packages, then [`ModuleName::ALL`]
/// mapped through [`self::run::package_of`] for the six kernel modules. No `PackageId` is
/// spelled in this function, so a package cannot be added to one side and forgotten on the
/// other.
///
/// Nine, not ten. [`PackageId`] has ten variants; the tenth is [`PackageId::C0`], the contracts
/// crate, which has no module and therefore emits no capability line. F-2 rules that all nine
/// of these are **required** and `C0` is **permitted and not required** — which is what makes
/// [`self::trace::validate`]'s completeness check bite on a real absence rather than on every
/// trace the run loop produces.
///
/// **Do not add a `PackageId::ALL` instead.** There is none, and one would contain `C0`; the
/// obvious implementation — walk it — rejects every recorded trace, and the obvious repair is to
/// weaken the completeness check until it checks nothing. F-2 names that trap by name.
///
/// The arity is asserted against both sources at compile time rather than trusted: without the
/// assertion, a seventh module would be mapped into a slot this array does not have and the loop
/// below would drop it, which is the same silent drift the ruling exists to prevent.
#[must_use]
pub const fn expected_capability_packages() -> [PackageId; 9] {
    const ENVIRONMENT: usize = environment_capabilities().len();
    const _: () = assert!(
        ENVIRONMENT + ModuleName::ALL.len() == 9,
        "the preamble's two sources no longer sum to nine; widen the return type and re-read \
         ruling F-2 before touching the validator"
    );

    // `C0` is the fill only because an array must be initialised before it is written; the two
    // loops below cover all nine slots, which the assertion above is what makes true.
    let mut packages = [PackageId::C0; 9];
    let mut index = 0;
    while index < ENVIRONMENT {
        packages[index] = environment_capabilities()[index].0;
        index += 1;
    }
    while index < packages.len() {
        packages[index] = run::package_of(ModuleName::ALL[index - ENVIRONMENT]);
        index += 1;
    }
    packages
}
