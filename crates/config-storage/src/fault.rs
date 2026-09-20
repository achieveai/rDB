//! Fault injection at durability boundaries (test plan TA-4, ADR-0008 Clarifications).
//!
//! Every store consults its [`FaultInjector`] on **every** crossing of each [`Boundary`], so a
//! test can fail or crash on the *n*-th crossing. The default [`NoFaults`] is a zero-cost no-op.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A durability boundary inside a store. The seventeen boundaries are distinct instants: log
/// append is write-then-explicit-sync so `AfterLogAppend` and `BeforeLogFlush` differ, since
/// M4 the durable-but-unpublished window has a name of its own (TA-28), and since M5 every
/// step of snapshot publication, snapshot install and log purge is separately crashable
/// (TA-41, lead ruling M5-R2).
///
/// # Adding a variant
///
/// [`Boundary::ALL`] is the single source of truth: it is the crossing order, the index into
/// [`FaultCounters`], and what every coverage assertion iterates. Nothing in the workspace
/// pattern-matches the enum exhaustively — deliberately, so a new milestone's boundary cannot
/// break a test file it does not own (M4 tester ruling). A new variant therefore needs three
/// edits here and nothing anywhere else: the variant, its entry in `ALL`, and its arms in
/// [`Boundary::index`] and [`Boundary::as_str`].
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
    /// Between the durable state batch and the watch hub being told about it (M4, TA-28).
    ///
    /// The window this names is the reason the journal exists: the batch — KV change *and*
    /// journal event — is on disk, but no watcher has seen it. A crash here must be
    /// recoverable by replaying from the journal after restart, which is spec §21 M4's "no
    /// silent loss" in its strictest form (test plan M4-06, M4-91).
    ///
    /// Crossed exactly once per applied batch, strictly after [`Boundary::AfterStateBatch`] and
    /// strictly before the publish, and never inside the store's write batch.
    AfterStateBatchBeforePublish,

    // ---------------------------------------------------------------------------------
    // M5 — snapshot publication (ADR-0022 "Publish ordering", test plan M5-10..M5-15)
    // ---------------------------------------------------------------------------------
    /// The snapshot body is fully written to `<id>.tmp` but no fsync has been issued.
    ///
    /// A crash here must leave **no** publication: the `.tmp` file is not a snapshot, the
    /// previous `current_snapshot` still stands, and nothing may have been purged against the
    /// build that was in flight (M5-11).
    BeforeSnapshotTmpSync,

    /// `<id>.tmp` has been fsynced and renamed to `<id>.snap`; the directory fsync and the
    /// `state_meta/current_snapshot` batch have not happened.
    ///
    /// The rename is deliberately **not** the publication point — the meta batch is — so a
    /// crash here leaves a complete but unreferenced `.snap` that is never served (M5-12).
    AfterSnapshotRename,

    /// The directory entry has been synced; the `state_meta/current_snapshot` batch has not
    /// been written.
    ///
    /// The sharpest form of invariant §19.7: a crash here must leave `purges() == 0` for this
    /// build, because OpenRaft schedules `Command::PurgeLog` the instant `build_snapshot`
    /// returns `Ok`, and `Ok` is only allowed to mean *published* (research trap T2, M5-13).
    BeforeCurrentSnapshotMeta,

    // ---------------------------------------------------------------------------------
    // M5 — snapshot install (ADR-0022 "Install: two-phase", test plan M5-30..M5-33)
    // ---------------------------------------------------------------------------------
    /// The received stream has been validated and durably renamed to `<id>.snap`, but
    /// `state_meta/install_in_progress` has not been written.
    ///
    /// Everything before this point is side-effect-free with respect to applied state, so a
    /// crash here must leave the old state machine completely untouched (M5-30).
    BeforeInstallMarker,

    /// The install marker is durable and the data column families have been dropped and
    /// recreated, but none of the snapshot's records have been streamed back in yet.
    ///
    /// The dangerous window: the state machine is empty and only the marker plus the retained
    /// `.snap` can rebuild it. Reopening must redo the install rather than come up empty
    /// (M5-31).
    AfterInstallDropCf,

    /// Every record has been streamed in, but the single synced batch that publishes
    /// `last_applied`, membership, the revisions, `current_snapshot` **and** deletes the
    /// marker has not been written.
    ///
    /// That batch must be atomic with the marker deletion, or a crash between the two loops
    /// forever (M5-32).
    BeforeInstallFinalBatch,

    // ---------------------------------------------------------------------------------
    // M5 — log purge (lead ruling M5-R2, which overrides test-plan OQ-41/M5-23)
    // ---------------------------------------------------------------------------------
    /// Before `RaftLogStorage::purge` deletes anything.
    ///
    /// Purge used to borrow [`Boundary::BeforeLogFlush`], which was harmless while purge never
    /// ran and fatal to M5's accounting once it does — a crash attributed to a log flush that
    /// was really a purge. M5-R2 gives purge its own pair.
    BeforePurge,

    /// After the synced range-delete-plus-`last_purged` batch returned, before the caller is
    /// told.
    AfterPurge,
}

impl Boundary {
    /// All boundaries in crossing order.
    pub const ALL: [Boundary; 17] = [
        Boundary::BeforeVoteSync,
        Boundary::AfterVoteSync,
        Boundary::BeforeLogAppend,
        Boundary::AfterLogAppend,
        Boundary::BeforeLogFlush,
        Boundary::AfterLogFlush,
        Boundary::BeforeStateBatch,
        Boundary::AfterStateBatch,
        Boundary::AfterStateBatchBeforePublish,
        Boundary::BeforeSnapshotTmpSync,
        Boundary::AfterSnapshotRename,
        Boundary::BeforeCurrentSnapshotMeta,
        Boundary::BeforeInstallMarker,
        Boundary::AfterInstallDropCf,
        Boundary::BeforeInstallFinalBatch,
        Boundary::BeforePurge,
        Boundary::AfterPurge,
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
            Boundary::AfterStateBatchBeforePublish => 8,
            Boundary::BeforeSnapshotTmpSync => 9,
            Boundary::AfterSnapshotRename => 10,
            Boundary::BeforeCurrentSnapshotMeta => 11,
            Boundary::BeforeInstallMarker => 12,
            Boundary::AfterInstallDropCf => 13,
            Boundary::BeforeInstallFinalBatch => 14,
            Boundary::BeforePurge => 15,
            Boundary::AfterPurge => 16,
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
            Boundary::AfterStateBatchBeforePublish => "after_state_batch_before_publish",
            Boundary::BeforeSnapshotTmpSync => "before_snapshot_tmp_sync",
            Boundary::AfterSnapshotRename => "after_snapshot_rename",
            Boundary::BeforeCurrentSnapshotMeta => "before_current_snapshot_meta",
            Boundary::BeforeInstallMarker => "before_install_marker",
            Boundary::AfterInstallDropCf => "after_install_drop_cf",
            Boundary::BeforeInstallFinalBatch => "before_install_final_batch",
            Boundary::BeforePurge => "before_purge",
            Boundary::AfterPurge => "after_purge",
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
    counts: [AtomicU64; Boundary::ALL.len()],
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
