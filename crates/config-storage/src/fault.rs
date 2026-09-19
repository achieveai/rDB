//! Fault injection at durability boundaries (test plan TA-4, ADR-0008 Clarifications).
//!
//! Every store consults its [`FaultInjector`] on **every** crossing of each [`Boundary`], so a
//! test can fail or crash on the *n*-th crossing. The default [`NoFaults`] is a zero-cost no-op.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A durability boundary inside a store. The eight boundaries are distinct instants: log
/// append is write-then-explicit-sync so `AfterLogAppend` and `BeforeLogFlush` differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Boundary {
    /// Before the vote is made durable (`save_vote`).
    BeforeVoteSync,
    /// After the vote sync returned.
    AfterVoteSync,
    /// Before entries are written to the log.
    BeforeLogAppend,
    /// After entries are written but before they are synced.
    AfterLogAppend,
    /// Before the log sync (`LogFlushed` callback not yet fired).
    BeforeLogFlush,
    /// After the log sync completed (callback about to fire).
    AfterLogFlush,
    /// Before the atomic state-machine batch (kv + revision + last_applied + membership).
    BeforeStateBatch,
    /// After the state batch was written and synced.
    AfterStateBatch,
}

impl Boundary {
    /// All boundaries in crossing order.
    pub const ALL: [Boundary; 8] = [
        Boundary::BeforeVoteSync,
        Boundary::AfterVoteSync,
        Boundary::BeforeLogAppend,
        Boundary::AfterLogAppend,
        Boundary::BeforeLogFlush,
        Boundary::AfterLogFlush,
        Boundary::BeforeStateBatch,
        Boundary::AfterStateBatch,
    ];

    /// Position of this boundary in [`Boundary::ALL`]; the index into [`FaultCounters`].
    pub const fn index(self) -> usize {
        match self {
            Boundary::BeforeVoteSync => 0,
            Boundary::AfterVoteSync => 1,
            Boundary::BeforeLogAppend => 2,
            Boundary::AfterLogAppend => 3,
            Boundary::BeforeLogFlush => 4,
            Boundary::AfterLogFlush => 5,
            Boundary::BeforeStateBatch => 6,
            Boundary::AfterStateBatch => 7,
        }
    }

    /// Stable snake_case name used in log fields (`boundary = "before_log_flush"`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Boundary::BeforeVoteSync => "before_vote_sync",
            Boundary::AfterVoteSync => "after_vote_sync",
            Boundary::BeforeLogAppend => "before_log_append",
            Boundary::AfterLogAppend => "after_log_append",
            Boundary::BeforeLogFlush => "before_log_flush",
            Boundary::AfterLogFlush => "after_log_flush",
            Boundary::BeforeStateBatch => "before_state_batch",
            Boundary::AfterStateBatch => "after_state_batch",
        }
    }
}

impl fmt::Display for Boundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What the injector wants the store to do at a boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FaultAction {
    /// Continue normally.
    Proceed,
    /// Return a recoverable I/O error from this operation; the store stays usable.
    Fail,
    /// Return an error **and poison the store**: every later call fails until the store is
    /// reopened (RocksStore) or recreated (EphemeralStore). `Drop` must not flush pending
    /// data. This simulates a process crash at that instant.
    Crash,
    /// Stall the calling task for the given duration, then proceed normally (the crossing
    /// still succeeds and still counts). Simulates a slow-but-not-failed boundary crossing —
    /// e.g. RocksDB's blocking I/O taking a while under load — without failing or poisoning
    /// anything. A store must consult this off the async runtime's worker threads (RocksStore
    /// already runs every boundary inside `spawn_blocking`) so the stall cannot starve other
    /// tasks; a store that has no such offload point may block its caller for the duration.
    Delay(Duration),
}

impl FaultAction {
    /// Stable snake_case name for log fields. `Delay` carries a duration that this alone
    /// cannot express; callers that need the duration in a log line add it as a separate field.
    pub const fn as_str(self) -> &'static str {
        match self {
            FaultAction::Proceed => "proceed",
            FaultAction::Fail => "fail",
            FaultAction::Crash => "crash",
            FaultAction::Delay(_) => "delay",
        }
    }
}

impl fmt::Display for FaultAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FaultAction::Delay(d) => write!(f, "delay({}ms)", d.as_millis()),
            other => f.write_str(other.as_str()),
        }
    }
}

/// Consulted by a store immediately before each boundary crossing.
///
/// Implementations must be cheap and non-blocking; they are called on the Raft core path.
/// Counting crossings is the injector's job (so `CrashAt { boundary, nth }` is expressible).
pub trait FaultInjector: Send + Sync {
    /// Decide what happens at this crossing.
    fn before(&self, boundary: Boundary) -> FaultAction;
}

/// How many times each [`Boundary`] has been crossed.
///
/// A store owns one of these and records **every** crossing, whatever the injector decided.
/// Tests use it to assert a boundary was actually reached (anti-flake rule 11: an assertion
/// over zero crossings proves nothing).
#[derive(Debug, Default)]
pub struct FaultCounters {
    counts: [AtomicU64; 8],
}

impl FaultCounters {
    /// Record one crossing.
    pub fn record(&self, boundary: Boundary) {
        self.counts[boundary.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Crossings recorded for one boundary.
    pub fn get(&self, boundary: Boundary) -> u64 {
        self.counts[boundary.index()].load(Ordering::Relaxed)
    }

    /// Crossings for every boundary, in [`Boundary::ALL`] order.
    pub fn snapshot(&self) -> BTreeMap<Boundary, u64> {
        Boundary::ALL.iter().map(|b| (*b, self.get(*b))).collect()
    }

    /// Total crossings across all boundaries.
    pub fn total(&self) -> u64 {
        Boundary::ALL.iter().map(|b| self.get(*b)).sum()
    }
}

/// The production injector: never injects anything.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoFaults;

impl FaultInjector for NoFaults {
    #[inline]
    fn before(&self, _boundary: Boundary) -> FaultAction {
        FaultAction::Proceed
    }
}
