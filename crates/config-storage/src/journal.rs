//! The retained event journal and the apply-side publish seam (M4, ADR-0019, spec §11.4).
//!
//! # What the journal is
//!
//! One [`MutationEvent`] per state-changing mutation, keyed by the public revision it
//! allocated, written in the **same synced batch** as the KV change it describes. That is the
//! whole durability claim: a journal written after the state batch could be lost while the
//! mutation survived, and a watch resuming across that crash would skip a revision without
//! anyone noticing — the silent loss spec §21 M4 forbids.
//!
//! # What is deliberately *not* hashed into `state_hash`
//!
//! Neither the journal nor `compact_revision` is part of [`config_core::KvState::state_hash`]
//! (lead ruling R1 of 2026-09-18; notes on ADR-0019 and ADR-0021). The v1 -> v2 migration
//! stamps each node's watermark from its own `cluster_revision` at upgrade time, so three
//! correct voters mid-rolling-upgrade legitimately hold three different watermarks. Folding
//! that into the divergence oracle would report a correct cluster as corrupt.
//!
//! Journal equality is instead asserted directly, by
//! [`StateReader::journal_hash`](crate::StateReader::journal_hash) over a common lower bound —
//! `max(compact_revision)` across the nodes being compared. Below that bound the nodes are
//! *entitled* to differ; above it they must not.
//!
//! # The publish seam
//!
//! [`AppliedBatchSink`] is defined here, in storage, and implemented by the engine's watch hub
//! (ruling R8). The dependency still points engine -> storage, exactly as it did at M2: storage
//! names a trait, it does not name the engine. Inverting it — an engine-owned trait that
//! storage called — is what would actually be a layering break.

use std::sync::Arc;

use config_core::{Command, MutationEvent, MutationEventKind};
use openraft::entry::{Entry, EntryPayload};
use sha2::{Digest, Sha256};

use crate::types::TypeConfig;

/// Kind tag folded into [`journal_hash`] for a `Put`.
const KIND_PUT: u8 = 1;
/// Kind tag folded into [`journal_hash`] for a `Delete` tombstone.
const KIND_DELETE: u8 = 2;

/// What the retained journal currently holds.
///
/// `bytes` is the **serialized** size actually stored — the `postcard` length of each retained
/// [`MutationEvent`] — because that is the number retention arithmetic is expressed in
/// (TA-29.2). An in-memory estimate would let a store pass its byte ceiling on one platform
/// and breach it on another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct JournalStats {
    /// Lowest retained revision, or `None` when the journal is empty.
    pub oldest_revision: Option<u64>,
    /// Highest retained revision, or `None` when the journal is empty.
    pub newest_revision: Option<u64>,
    /// Number of retained events.
    pub count: u64,
    /// Total serialized bytes of the retained events.
    pub bytes: u64,
}

/// Serialized size of one event, measured exactly as the store writes it.
///
/// Shared by both store kinds so the ephemeral parity rows (M4-11) compare one number rather
/// than two independently derived ones. An encoding failure is reported as `0` rather than
/// panicking: this feeds a *statistic*, and a store must not die because a byte count could
/// not be taken — the event itself is encoded, and fails loudly, on the write path.
pub fn event_bytes(event: &MutationEvent) -> u64 {
    postcard::to_stdvec(event).map_or(0, |bytes| bytes.len() as u64)
}

/// Fold one event into a running journal digest.
///
/// Every variable-length field is length-prefixed, and the kind is tagged, so no two distinct
/// journals can collide by concatenation — the same rule `state_hash` follows.
fn update_hash(hasher: &mut Sha256, event: &MutationEvent) {
    hasher.update(event.revision.to_le_bytes());
    hasher.update((event.key.len() as u32).to_le_bytes());
    hasher.update(&event.key);
    match &event.kind {
        MutationEventKind::Put {
            value,
            create_revision,
        } => {
            hasher.update([KIND_PUT]);
            hasher.update((value.len() as u32).to_le_bytes());
            hasher.update(value);
            hasher.update(create_revision.to_le_bytes());
        }
        MutationEventKind::Delete => {
            hasher.update([KIND_DELETE]);
        }
    }
}

/// Digest the retained events with `revision > from_exclusive`, in ascending revision order.
///
/// The `from_exclusive` bound is the whole point of the function (ruling R1): comparing two
/// nodes means comparing them above the highest watermark either of them has reached, because
/// below it they are *entitled* to hold different history. Callers pass
/// `max(compact_revision)` over the nodes under comparison.
///
/// The event count is folded in first so a journal that is a strict prefix of another cannot
/// hash alike.
pub fn journal_hash<'a>(events: impl ExactSizeIterator<Item = &'a MutationEvent>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((events.len() as u64).to_le_bytes());
    for event in events {
        update_hash(&mut hasher, event);
    }
    hasher.finalize().into()
}

/// One applied state batch, handed to the watch hub after it is durable.
///
/// Carries the events as `Arc` so one applied batch fans out to every subscriber without
/// cloning the values — a single 1 MiB put to a thousand watchers must not become a gigabyte
/// of copies.
#[derive(Debug, Clone)]
pub struct AppliedBatch {
    /// `cluster_revision` after this batch. The high-water mark a registration captures.
    pub applied_revision: u64,
    /// Raft log index of the last entry in this batch. Node-local; for correlation only.
    pub last_applied_index: u64,
    /// Events this batch produced, in ascending revision order. **Empty is normal**: a batch
    /// of blank, membership, rejected, or conflicting entries produces none, and the hub is
    /// still told, because "applied up to R with no events" is what advances an idle stream's
    /// progress cursor.
    pub events: Vec<Arc<MutationEvent>>,
    /// `Some(up_to)` when this batch applied a [`config_core::Command::Compact`], carrying the
    /// watermark **after** applying — which for a monotonic no-op is the unchanged one.
    pub compacted_to: Option<u64>,
}

/// The apply-side publish seam: storage tells its owner that a batch is durable.
///
/// # Contract
///
/// * [`AppliedBatchSink::on_applied`] runs **after** the synced state batch returned `Ok`,
///   exactly once per batch, **even when `events` is empty**, on the blocking thread that did
///   the write.
/// * [`AppliedBatchSink::before_compact`] and [`AppliedBatchSink::after_compact`] bracket a
///   batch that applies a `Compact`, so the implementation can hold its journal gate across
///   the deletion and no cursor validation can straddle it (spec §11.2 step 4).
/// * **No method may block.** No `await`, no lock a watcher can hold, no channel send that can
///   wait. These run on the apply path: a sink that blocks stalls apply for the whole node,
///   which is the failure mode spec §11.5 exists to prevent. A slow consumer is terminated,
///   never allowed to back-pressure consensus.
pub trait AppliedBatchSink: Send + Sync + 'static {
    /// A batch is durable. Called exactly once per applied batch, empty or not.
    fn on_applied(&self, batch: AppliedBatch);

    /// A batch that will delete journal events up to `up_to_revision` is about to be written.
    ///
    /// `up_to_revision` is the **requested** watermark, read from the batch's inputs by
    /// [`compact_target`] before apply runs. It is deliberately the wider of the two bounds:
    /// [`config_core::KvState::apply`] clamps to `cluster_revision`, so `Compact { up_to: 500 }`
    /// on a cluster at revision 100 arrives here as 500.
    fn before_compact(&self, up_to_revision: u64);

    /// The compacting batch is durable and its watermark is visible.
    ///
    /// `up_to_revision` is the same **requested** value `before_compact` was given, so the
    /// implementation can match the bracket it opened. It is **not** a watermark and must not
    /// be cached as one: an implementation that stored it would, after one clamped no-op,
    /// treat revisions that still exist as compacted. The effective watermark is
    /// [`AppliedBatch::compacted_to`] — `CommandResponse::Compacted.compact_revision`.
    ///
    /// **Ordering contract, not an observation (C4-09):** an implementor of this trait may rely
    /// on `compacted_to` having been published through [`AppliedBatchSink::on_applied`] *before*
    /// `after_compact` is called, so a sink that holds a gate across the pair sees the new floor
    /// without a second synchronisation. A store must therefore close the bracket after it
    /// publishes, never before — releasing early re-opens the window in which a reader admits a
    /// cursor into a range the batch has already deleted. The same applies wherever else a store
    /// publishes a `compacted_to`: the snapshot-install path opens its own [`CompactGuard`]
    /// around that publish for exactly this reason.
    fn after_compact(&self, up_to_revision: u64);
}

/// The highest watermark the `Compact` entries in this batch will drive the journal to, if any.
///
/// Read from the batch's *inputs*, before apply, because the sink has to learn what is about to
/// happen while it can still act on it. The clamp and monotonicity rules inside
/// [`config_core::KvState::apply`] may settle on a lower effective watermark; the bracket is
/// deliberately the wider of the two, since a gate held slightly too long is correct and one
/// released early is not.
pub(crate) fn compact_target(entries: &[Entry<TypeConfig>]) -> Option<u64> {
    entries
        .iter()
        .filter_map(|entry| match &entry.payload {
            EntryPayload::Normal(Command::Compact { up_to_revision, .. }) => Some(*up_to_revision),
            _ => None,
        })
        .max()
}

/// Holds a sink's compaction bracket open for the duration of a compacting batch.
///
/// A guard rather than a matched pair of calls because the store's write path is littered with
/// `?`: a fault injected between [`AppliedBatchSink::before_compact`] and the end of the batch
/// would otherwise leave the engine's journal gate held forever, and every later watch
/// registration on that node would hang instead of reporting the storage error. The bracket is
/// closed on every path out, including the injected-crash one.
pub(crate) struct CompactGuard<'a> {
    sink: &'a dyn AppliedBatchSink,
    up_to_revision: u64,
}

impl<'a> CompactGuard<'a> {
    /// Open the bracket. The sink learns the watermark **before** any event is deleted.
    pub(crate) fn open(sink: &'a dyn AppliedBatchSink, up_to_revision: u64) -> Self {
        sink.before_compact(up_to_revision);
        Self {
            sink,
            up_to_revision,
        }
    }
}

impl Drop for CompactGuard<'_> {
    fn drop(&mut self) {
        self.sink.after_compact(self.up_to_revision);
    }
}

/// The sink for a store with nothing listening: tests, tools, and the M2/M3 code paths that
/// predate the watch hub.
///
/// It is a real implementation rather than an `Option<Arc<dyn AppliedBatchSink>>` on the store
/// so the publish path has exactly one shape. A `None` branch would be a second, untested code
/// path through the most safety-critical moment in apply.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSink;

impl AppliedBatchSink for NoopSink {
    #[inline]
    fn on_applied(&self, _batch: AppliedBatch) {}

    #[inline]
    fn before_compact(&self, _up_to_revision: u64) {}

    #[inline]
    fn after_compact(&self, _up_to_revision: u64) {}
}
