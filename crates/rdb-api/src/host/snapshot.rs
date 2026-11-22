//! Issue #61 S2 (design section 2, PR-A commit A8a): a copy below its primary's History floor is
//! rebuilt from a state snapshot, started by the target.
//!
//! **Source (the primary).** R1's `SnapshotCatchupRequired{copy, barrier}` reaches
//! `Host::snapshot_catchup`. When the primary's own R1 ladder is not dense from the engine's
//! trim floor to its head (lead ruling, A8: "ladder floor > engine floor"), the ladder is refilled
//! from the engine and the copy's next `NeedPrefix` walks it: `snapshot_catchup_served{how =
//! "ladder"}`. Otherwise the engine cannot serve the record either, and a `Transfer` opens: a
//! view pinned at `S` = the lineage's applied seq, and a pin that stops a trim past `S`. The offer
//! ([`SnapshotOffer`]) goes out once the engine's `durable` has reached `S`, so a target never
//! installs a record its source could still lose in a crash. The source answers each
//! [`SnapshotPull`] with one page ([`SnapshotChunk`], at most `Host::snapshot_page_keys` entries,
//! `COPY_BATCH_KEYS` unless a row lowers it) and
//! keeps no chain state: the pull carries the target's running chain. A transfer ends on the
//! copy's `CopyCaughtUp` (`snapshot_transfer_released`), on the target's refusal, on its node's
//! `PeerBooted`, or after `SNAPSHOT_IDLE_MILLIS` with no pull (`snapshot_transfer_abandoned`).
//!
//! **Target.** `Host::snapshot_offer` accepts an offer only when its receiver of the partition
//! adopted the offer's lineage `(p, g, owner_epoch)`, the offerer is that config's primary node
//! and boot, `S` is above the receiver's applied head, and the barrier record decodes, names `S`
//! and recomputes its digest. Otherwise it answers [`SnapshotOfferRefused`] and logs
//! `snapshot_offer_refused{reason}`, changing nothing. The first chunk whose chain checks runs
//! `install_begin`; each chunk is written by `install_chunk`; the last runs `install_finish`
//! (synced), logged `snapshot_transfer_installed`.
//!
//! **Re-seed and the ACK at `S`** (the sim twin copies this): after the finish the target
//! replaces its receiver with a new live one over the same config, copy and lineage, at head
//! `(S, digest of the barrier record)` and `durable = S` (the finish was synced), drops every
//! ladder this host kept for the partition, and sends the primary that receiver's own ACK,
//! unsolicited (`AppendOutcome::Accepted(current_ack)`). R1's cursor takes it as any ACK: at
//! the head it reports `CopyCaughtUp`, below it it walks `S + 1 ..= head`. Design section 2 said
//! `NeedPrefix(S)`; that sends nothing when `S` is the head, and only an ACK ends a catch-up,
//! so the copy never caught up (S2-R1, red). The copy's `CopyCaughtUp` releases the pin.
//!
//! **The chunk chain.** `chain = Digest::of(Domain::Record, [SNAPSHOT_CHUNK_LABEL, prev, then
//! per entry: namespace tag, key, value, version LE, generation LE])`. Chunk 0's `prev` is the
//! barrier record's digest. The target checks every chunk as it arrives, before it writes it,
//! so a corrupt chunk is refused `snapshot_install_refused{reason = "chain"}` and the lineage is
//! never switched (the switch is `install_finish`).

use bytes::Bytes;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::envelope::{AppendOutcome, ReplicationEnvelope};
use rdb_core::contracts::ids::ReplicaRole;
use rdb_core::contracts::ids::{BootId, ConfigVersion, Generation, NodeId, PartitionId, Seq};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::storage::Namespace;
use rdb_core::contracts::transport::Frame;
use rdb_core::replication::append::{AppendReceiver, ReceiverInit, UNSOLICITED};
use rdb_core::replication::progress::DigestLadder;
use rdb_core::replication::wire::encode_reply;
use rdb_storage::{RocksSnapshot, SnapshotEntry};
use serde::{Deserialize, Serialize};

use super::{Host, Msg, Site};

/// The label part of every chunk chain digest: it keeps a chunk digest apart from a record
/// digest, whose parts differ in number and content (as `Host::fork_head` labels its own).
pub const SNAPSHOT_CHUNK_LABEL: &[u8] = b"rdb snapshot chunk v1";

/// A source drops a transfer no pull has touched for this long (design section 2, step 6).
pub(crate) const SNAPSHOT_IDLE_MILLIS: u64 = 60_000;

/// A target that has had no page for this long re-sends its pull, the same
/// cursor, so a lost pull or page is asked again. A source serves a repeated pull again.
pub(crate) const SNAPSHOT_PULL_RETRY_MILLIS: u64 = 5_000;

/// The pulls a target re-sends with no page between them before its install
/// expires (`snapshot_install_expired`) and the source is told `install_retryable`.
pub(crate) const SNAPSHOT_PULL_RETRIES: u32 = 3;

/// The `install_retryable` refusals a source takes at one barrier, each
/// forgetting it so R1's next ask re-offers it. So 3 retries are allowed: refusals 1 to 3
/// forget the barrier, and refusal 4 keeps it (`snapshot_retry_exhausted`). Kept, it is
/// re-opened after [`SNAPSHOT_RETRY_BACKOFF_MILLIS`], or a new barrier is offered.
pub(crate) const SNAPSHOT_RETRY_LIMIT: u32 = 3;

/// How long a source keeps a barrier whose retries ran out before
/// `Host::retry_duty` re-opens it, counts afresh, and probes the copy with the head. A target
/// whose fault has cleared is then rebuilt; one whose fault persists is refused again, at most
/// [`SNAPSHOT_RETRY_LIMIT`] + 1 times per this interval.
pub(crate) const SNAPSHOT_RETRY_BACKOFF_MILLIS: u64 = 30_000;

/// One `Host.snapshot_retries` entry: the `(generation, barrier)` counted, the
/// `install_retryable` refusals heard there, and the tick the limit ran out, once it has.
pub(crate) type RetryCount = (
    Generation,
    Seq,
    u32,
    Option<rdb_core::contracts::time::Tick>,
);

/// Which refusal an engine install error is. A storage fault may pass; every
/// other error would recur on a re-offer at the same barrier.
pub(crate) const fn install_refusal(error: &rdb_storage::InstallError) -> SnapshotRefusal {
    match error {
        rdb_storage::InstallError::Storage(_) => SnapshotRefusal::InstallRetryable,
        _ => SnapshotRefusal::Install,
    }
}

/// The most bytes one snapshot page carries by default, a quarter of the
/// 64 MiB frame ([`crate::net::MAX_FRAME`]), so a page of large values always fits it with the
/// frame's own fields to spare. A page holds at least one entry, so a single entry larger than
/// the frame still cannot be sent.
pub(crate) const SNAPSHOT_PAGE_BYTES: usize = crate::net::MAX_FRAME / 4;

// Any stored entry is from one write record, at most `MAX_ENVELOPE_BYTES`: it fits a page.
const _: () = assert!(rdb_core::replication::append::MAX_ENVELOPE_BYTES < SNAPSHOT_PAGE_BYTES);

/// Why a target refused an offer, or stopped an install. Sent back in [`SnapshotOfferRefused`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotRefusal {
    /// The target's receiver did not adopt the offer's `(partition, generation, owner_epoch)`,
    /// or it holds no live receiver of the partition.
    NotAdopted,
    /// The offerer is not the primary node and boot the adopted config names.
    NotPrimary,
    /// `S` is at or below the receiver's applied head: nothing to install.
    NotAhead,
    /// The barrier record does not decode, names another seq, or does not recompute.
    BadBarrier,
    /// A held #105 tail the target has not been released from (issue #61 rev 4 N2). Sent from
    /// commit A8b on; the variant is here so the wire shape does not change.
    TailHeld,
    /// A chunk's chain did not match the entries it carried. Nothing was switched.
    Chain,
    /// The engine refused a step of the install (`install_begin`, `install_chunk` or
    /// `install_finish`) for a reason a re-offer cannot cure: a newer generation held, or an
    /// entry or barrier the export never sends.
    Install,
    /// The install stopped for a reason a re-offer may cure, a storage fault in
    /// an install step or the target's install expiry (no page after
    /// [`SNAPSHOT_PULL_RETRIES`] retried pulls). The source forgets the barrier for the first
    /// [`SNAPSHOT_RETRY_LIMIT`] (3) such refusals at it, so R1's next ask re-offers it; the 4th
    /// keeps it until the retry back-off re-opens it.
    InstallRetryable,
}

impl SnapshotRefusal {
    /// The `reason` field of `snapshot_offer_refused` and `snapshot_install_refused`.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::NotAdopted => "not_adopted",
            Self::NotPrimary => "not_primary",
            Self::NotAhead => "not_ahead",
            Self::BadBarrier => "bad_barrier",
            Self::TailHeld => "tail_held",
            Self::Chain => "chain",
            Self::Install => "install",
            Self::InstallRetryable => "install_retryable",
        }
    }
}

/// The primary's offer of a snapshot of `lineage` at `at` to `copy` (design section 2, step 1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotOffer {
    /// The offerer's lineage: partition, generation and owner epoch.
    pub lineage: Lineage,
    /// The offerer's config version.
    pub config_version: ConfigVersion,
    /// The offering node, the config's primary.
    pub from: NodeId,
    /// Its boot.
    pub boot: BootId,
    /// The copy offered to.
    pub copy: CopyId,
    /// `S`: the seq the snapshot is taken at.
    pub at: Seq,
    /// The lineage's root: its link base and the digest there, written into the installed
    /// trim floor so a later recovery anchored at that base finds it (design section 1a).
    pub root: (Seq, Digest),
    /// The canonical record at `S`.
    pub barrier: Bytes,
    /// The source's transfer instance, unique on its node. A refusal names it, so one
    /// from an earlier transfer at the same `S` never ends a later one.
    pub transfer: u64,
}

/// The target's request for the next page after `after` (design section 2, steps 3 and 4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotPull {
    /// The partition.
    pub partition: PartitionId,
    /// The offer's generation.
    pub generation: Generation,
    /// The offer's `S`.
    pub at: Seq,
    /// The target copy.
    pub copy: CopyId,
    /// The target's node, where the chunk goes.
    pub node: NodeId,
    /// The last `(namespace, key)` received, or `None` for the first page.
    pub after: Option<(Namespace, Bytes)>,
    /// The target's chain so far: the barrier's digest before the first page.
    pub prev: Digest,
}

/// One page of the source's view, answering a [`SnapshotPull`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotChunk {
    /// The source's node. The net layer holds it to the sender's certificate, and the target
    /// takes a page only from the node its install's offer came from.
    pub from: NodeId,
    /// The partition.
    pub partition: PartitionId,
    /// The offer's generation, echoed (m-d): a chunk that does not match the install is dropped.
    pub generation: Generation,
    /// The offer's `S`, echoed.
    pub at: Seq,
    /// The target copy.
    pub copy: CopyId,
    /// The cursor the pull named: the chunk answers that pull and no other.
    pub after: Option<(Namespace, Bytes)>,
    /// The entries, in view order.
    pub entries: Vec<SnapshotEntry>,
    /// [`chunk_chain`] of the pull's `prev` and these entries.
    pub chain: Digest,
    /// The view has nothing after this page.
    pub done: bool,
}

/// The target refused an offer or stopped an install (design section 2, step 2; rev 5 M1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotOfferRefused {
    /// The partition.
    pub partition: PartitionId,
    /// The offer's generation.
    pub generation: Generation,
    /// The offer's `S`.
    pub at: Seq,
    /// The target copy.
    pub copy: CopyId,
    /// The target's node.
    pub node: NodeId,
    /// Why.
    pub reason: SnapshotRefusal,
    /// For [`SnapshotRefusal::TailHeld`], the held tail's head: the `--through` an operator
    /// passes to `admin forget-tail`. `None` otherwise.
    pub through: Option<Seq>,
    /// The refused offer's [`SnapshotOffer::transfer`].
    pub transfer: u64,
}

/// The command that gives up a held tail through `through` on `partition` (design rev 5a): the
/// one text the warn, `NodeStatus::needs_operator` and `PartitionReady::degraded` all name.
#[must_use]
pub fn forget_tail_command(partition: PartitionId, through: Seq) -> String {
    format!(
        "admin forget-tail --partition {} --through {} --give-up-live-copy",
        partition.0, through.0
    )
}

/// `NodeStatus::needs_operator`'s text for `copy` on `node` holding a tail through `head` past
/// the history floor (design rev 5 M1, with rev 5a's command).
#[must_use]
pub fn needs_forget_tail(partition: PartitionId, copy: CopyId, node: NodeId, head: Seq) -> String {
    format!(
        "copy {} on node {} holds a tail through {} past the history floor; run {} (this can lose \
         acknowledged writes; see embed.md)",
        copy.0,
        node.0,
        head.0,
        forget_tail_command(partition, head)
    )
}

/// Why an offer is refused, with the held tail for [`SnapshotRefusal::TailHeld`].
type OfferRefusal = (SnapshotRefusal, Option<super::TailHit>);

/// A copy's `tail_held` refusal of this primary's offer, as the owner keeps it (design rev 5b).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OfferHold {
    /// The copy's node.
    pub(crate) node: NodeId,
    /// The held tail's head: the `--through` that gives it up.
    pub(crate) head: Seq,
}

/// The tail this copy refused an offer for (design rev 5 M1), warned once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OfferTailHeld {
    /// The copy the refused offer was for.
    pub(crate) copy: CopyId,
    /// The lineage holding the tail.
    pub(crate) from: Generation,
    /// The tail's head.
    pub(crate) head: Seq,
}

/// Lead ruling 1 (issue #61 A8a): `ladder` holds a rung at every seq from `floor` (the engine's
/// floor) through its highest, with none below. Counted, never walked: rungs are distinct seqs,
/// so a ladder whose lowest is `floor` is dense exactly when it has `highest - floor + 1`.
/// An empty ladder is not dense.
pub(crate) fn ladder_dense(ladder: &DigestLadder, floor: Seq) -> bool {
    match (ladder.floor(), ladder.highest()) {
        (Some(lowest), Some(highest)) => {
            lowest == floor
                && u64::try_from(ladder.len()).is_ok_and(|len| len == highest.0 - lowest.0 + 1)
        }
        _ => false,
    }
}

/// The chain digest of a page: [`SNAPSHOT_CHUNK_LABEL`], `prev`, then each entry's namespace
/// tag, key, value, version and generation. `Digest::of` length-prefixes every part, so the
/// encoding is unambiguous.
#[must_use]
pub fn chunk_chain(prev: Digest, entries: &[SnapshotEntry]) -> Digest {
    let tags: Vec<[u8; 1]> = entries.iter().map(|(ns, ..)| [ns_tag(*ns)]).collect();
    let numbers: Vec<([u8; 8], [u8; 8])> = entries
        .iter()
        .map(|(_, _, _, version, generation)| (version.to_le_bytes(), generation.0.to_le_bytes()))
        .collect();
    let mut parts: Vec<&[u8]> = Vec::with_capacity(2 + entries.len() * 5);
    parts.push(SNAPSHOT_CHUNK_LABEL);
    parts.push(&prev.0);
    for (((_, key, value, _, _), tag), (version, generation)) in
        entries.iter().zip(&tags).zip(&numbers)
    {
        parts.extend_from_slice(&[tag, key, value, version, generation]);
    }
    Digest::of(Domain::Record, &parts)
}

/// The ACK a re-seeded receiver sends its primary: only when it is at `at` and durable through
/// it. The ACK says `durable = S`, which is honest only because `install_finish` synced the
/// install and the re-seed credited exactly that; a receiver durable below `S` withholds it
/// (`snapshot_reseed_ack_withheld`, error) and its next flush ACKs instead.
#[must_use]
pub(crate) fn reseed_ack(
    receiver: &AppendReceiver,
    at: Seq,
) -> Option<rdb_core::contracts::envelope::AppendAck> {
    let ack = receiver.current_ack();
    (ack.progress.durable.0 >= at.0 && receiver.applied_head().seq == at).then_some(ack)
}

/// A namespace's tag in the chain, the same as the envelope's wire tag.
const fn ns_tag(ns: Namespace) -> u8 {
    match ns {
        Namespace::User => 1,
        Namespace::History => 2,
        Namespace::Dedup => 3,
        Namespace::Progress => 4,
        Namespace::Meta => 5,
    }
}

/// An outbound transfer this primary serves (source side).
pub(crate) struct Transfer {
    /// The lineage offered.
    pub(crate) lineage: Lineage,
    /// The target's node.
    pub(crate) node: NodeId,
    /// `S`.
    pub(crate) at: Seq,
    /// The pinned view at `S`.
    pub(crate) view: RocksSnapshot,
    /// The offer, sent once `durable` reaches `S`; `None` once sent.
    pub(crate) offer: Option<SnapshotOffer>,
    /// Entries sent so far (a move's progress, A8c).
    pub(crate) pulled: u64,
    /// Key and value bytes sent so far (A8c, design N3: the read-only refusal's detail).
    pub(crate) bytes: u64,
    /// When the transfer was opened (A8c, design N3).
    pub(crate) opened: rdb_core::contracts::time::Tick,
    /// Pages sent so far.
    pub(crate) pages: u64,
    /// The last time the transfer was opened, offered or pulled.
    pub(crate) touched: rdb_core::contracts::time::Tick,
    /// This transfer's instance ([`SnapshotOffer::transfer`]).
    pub(crate) id: u64,
}

/// An install in progress (target side).
#[derive(Debug, Clone)]
pub(crate) struct Install {
    /// The offer being installed.
    pub(crate) offer: SnapshotOffer,
    /// `install_begin` ran.
    pub(crate) begun: bool,
    /// The chain so far.
    pub(crate) chain: Digest,
    /// The last `(namespace, key)` received.
    pub(crate) after: Option<(Namespace, Bytes)>,
    /// Entries written.
    pub(crate) entries: u64,
    /// Pages written.
    pub(crate) chunks: u64,
    /// When the offer was accepted.
    pub(crate) started: std::time::Instant,
    /// Ordinary appends of the offer's lineage not delivered while this install owned the
    /// partition ([`Host::append_deferred`]).
    pub(crate) deferred: u64,
    /// The largest page written, in [`rdb_storage::snapshot_entry_bytes`].
    pub(crate) largest_page: u64,
    /// When the offer was accepted, a page written, or a pull re-sent.
    pub(crate) touched: rdb_core::contracts::time::Tick,
    /// Pulls re-sent since the last page written.
    pub(crate) retries: u32,
}

impl Host {
    /// R1's `SnapshotCatchupRequired` (module docs). Never faults the node: no primary here, an
    /// unreadable engine or a refused ladder is logged `snapshot_catchup_unserved{reason}`
    /// (warn). While a transfer of this generation is open for the copy, every ask is dropped,
    /// a newer barrier too (`snapshot_catchup_in_flight`, lead ruling 2: replacing it on each
    /// write would restart the install for ever). A transfer is replaced only after it ends: the
    /// target's `PeerBooted`, a refusal, idle abandonment, or a new generation. A repeat
    /// `(copy, barrier)` after a refusal is dropped (B-R48); a refill is never deduped, so the
    /// ask after it reaches the transfer branch.
    ///
    /// The pin at the first `S` keeps `S ..= head` readable on this source until the copy's
    /// `CopyCaughtUp`: the trim refuses past `S` while the transfer is open, so the walk after the
    /// re-seed's ACK always finds the records it sends.
    pub(super) fn snapshot_catchup(&mut self, partition: PartitionId, copy: CopyId, barrier: Seq) {
        let node = self.node;
        let unserved = |reason: &str| {
            tracing::warn!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                barrier = barrier.0,
                reason,
                "snapshot_catchup_unserved"
            );
        };
        let Some(primary) = self.replication.primary(node, partition) else {
            return unserved("no_primary");
        };
        let tracker = primary.tracker();
        let lineage = tracker.lineage();
        let generation = lineage.generation;
        let config = tracker.config().clone();
        let floor = self.engine.floor_seq(partition, generation);
        // Lead ruling 1 (A8): a ladder that is not dense from the engine's floor to its head
        // (its first rung above that floor, or a gap, as `Recovered`'s `{root, cutoff}` on an
        // untrimmed engine) lacks rungs the engine holds: refill. Only a dense ladder transfers.
        if !ladder_dense(tracker.history(), floor) {
            let started = std::time::Instant::now();
            let read = match self.dormant_rungs(partition, generation) {
                Ok(read) => read,
                Err(reason) => return unserved(reason),
            };
            if self
                .replication
                .restore_primary_history(node, partition, &read)
                .is_err()
            {
                return unserved("ladder_refused");
            }
            tracing::info!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                barrier = barrier.0,
                generation = generation.0,
                how = "ladder",
                floor = floor.0,
                rungs = read.len(),
                millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "snapshot_catchup_served"
            );
            return;
        }
        // Lead ruling 2 (A8a): an open transfer of this generation is never replaced by a newer
        // barrier. Every write makes a copy at the root ask again with the new head; replacing
        // would restart the install each time, and under steady writes it would never finish.
        // The re-seed's ACK walks the writes past `S`, which the pin keeps readable.
        if let Some(open) = self
            .transfers
            .get(&(partition, copy))
            .filter(|open| open.lineage.generation == generation)
        {
            tracing::info!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                at = open.at.0,
                barrier = barrier.0,
                "snapshot_catchup_in_flight"
            );
            return;
        }
        // Owner ruling 2026-11-20, option 1: after the give-up the copy is offered nothing more
        // until a caught-up release, a raised budget or a newer generation clears it (R27);
        // `needs_operator` names it meanwhile.
        if let Some(given) = self.given_up(partition, copy) {
            tracing::info!(
                node = node.0,
                partition = partition.0,
                copy = copy.0,
                barrier = barrier.0,
                attempts = given.attempts,
                "snapshot_catchup_given_up"
            );
            return;
        }
        // Only a transfer is deduped: a refill leaves the ladder dense, so R1's next ask for the
        // same barrier must reach the transfer branch, never be dropped (ruling 1: a refill
        // can repeat). Keyed by generation too (lead ruling R18): a refused offer at `S` must
        // not hold back the offer a later commit makes at the same `S`.
        // Written only beside the transfer, after every fallible step below:
        // a step that fails leaves no marker, so R1's next ask at this barrier is served.
        if self.snapshots_served.get(&(partition, copy)) == Some(&(generation, barrier)) {
            return;
        }
        let Some(target) = config.members.iter().find(|member| member.copy == copy) else {
            return unserved("copy_not_in_config");
        };
        let Some(primary_member) = config
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary)
        else {
            return unserved("no_primary_member");
        };
        let at = Seq(self.engine.buffered_applied(partition, generation).0);
        let root = match self.transfer_root(partition, generation) {
            Ok(root) => root,
            Err(reason) => return unserved(reason),
        };
        let barrier_record = match self.engine.history_at(partition, generation, at) {
            Ok(Some((record, _))) => record,
            Ok(None) => return unserved("no_barrier_record"),
            Err(fault) => {
                tracing::warn!(
                    node = node.0,
                    partition = partition.0,
                    ?fault,
                    "snapshot_barrier_read_failed"
                );
                return unserved("barrier_read_failed");
            }
        };
        #[cfg(debug_assertions)]
        if std::mem::take(&mut self.snapshot_export_fault) {
            return unserved("view_failed");
        }
        let view = match self.engine.snapshot(
            partition,
            generation,
            rdb_core::contracts::ids::SnapshotHandle(u64::MAX),
        ) {
            Ok(view) => view,
            Err(fault) => {
                tracing::warn!(
                    node = node.0,
                    partition = partition.0,
                    ?fault,
                    "snapshot_view_failed"
                );
                return unserved("view_failed");
            }
        };
        let offer = SnapshotOffer {
            lineage,
            config_version: config.config_version,
            from: node,
            boot: primary_member.boot,
            copy,
            at,
            root,
            barrier: barrier_record,
            transfer: self.next_transfer_id,
        };
        let id = self.next_transfer_id;
        self.next_transfer_id += 1;
        self.snapshots_served
            .insert((partition, copy), (generation, barrier));
        let replaced = self
            .transfers
            .insert(
                (partition, copy),
                Transfer {
                    lineage,
                    node: target.node,
                    at,
                    view,
                    offer: Some(offer),
                    pulled: 0,
                    bytes: 0,
                    opened: self.clock.now(),
                    pages: 0,
                    touched: self.clock.now(),
                    id,
                },
            )
            .map(|old| old.at.0);
        tracing::info!(
            node = node.0,
            partition = partition.0,
            copy = copy.0,
            to = target.node.0,
            barrier = barrier.0,
            generation = generation.0,
            at = at.0,
            floor = floor.0,
            replaced = ?replaced,
            "snapshot_transfer_opened"
        );
        self.offer_due(partition, copy);
    }

    /// The root a snapshot of `generation` carries: the trim mark's root when the mark is this
    /// lineage's own, else the lineage's link base and the digest there, else `(0, ROOT)`.
    fn transfer_root(
        &self,
        partition: PartitionId,
        generation: Generation,
    ) -> Result<(Seq, Digest), &'static str> {
        if let Some((marked, mark)) = self.engine.floor(partition, generation) {
            if marked == generation {
                return Ok((mark.root_base, mark.root_digest));
            }
        }
        match self.engine.link(partition, generation) {
            Some(link) => Ok((link.base, self.digest_at(partition, generation, link.base)?)),
            None => Ok((Seq::ZERO, Digest::ROOT)),
        }
    }

    /// Send `(partition, copy)`'s offer once the engine's `durable` has reached its `S`.
    fn offer_due(&mut self, partition: PartitionId, copy: CopyId) {
        #[cfg(debug_assertions)]
        if self.snapshot_offer_held {
            return;
        }
        let Some(transfer) = self.transfers.get_mut(&(partition, copy)) else {
            return;
        };
        let durable = self
            .engine
            .durable(partition, transfer.lineage.generation)
            .0;
        if durable < transfer.at.0 {
            return;
        }
        let Some(offer) = transfer.offer.take() else {
            return;
        };
        transfer.touched = self.clock.now();
        let to = transfer.node;
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            copy = copy.0,
            to = to.0,
            generation = offer.lineage.generation.0,
            at = offer.at.0,
            durable,
            "snapshot_offer_sent"
        );
        self.send_peer(to, Msg::SnapshotOffer(Box::new(offer)), "snapshot_offer");
    }

    /// Every step: send the offers whose `S` is now durable, and drop transfers idle past
    /// [`SNAPSHOT_IDLE_MILLIS`] (`snapshot_transfer_abandoned`).
    pub(super) fn transfer_duty(&mut self, now: rdb_core::contracts::time::Tick) {
        let keys: Vec<_> = self.transfers.keys().copied().collect();
        for (partition, copy) in keys {
            self.offer_due(partition, copy);
            let Some(transfer) = self.transfers.get(&(partition, copy)) else {
                continue;
            };
            let idle = super::HostClock::millis_until(transfer.touched, now);
            if idle >= SNAPSHOT_IDLE_MILLIS {
                self.end_transfer(partition, copy, "idle", "snapshot_transfer_abandoned");
            }
        }
        self.retry_duty(now);
    }

    /// Every step: a barrier kept because its retries ran out
    /// ([`Self::refusal_kept`]) is re-opened once [`Host::snapshot_retry_backoff_millis`] has
    /// passed: the served barrier is forgotten (`snapshot_retry_reopened`), and the copy is
    /// probed with the head (`Host::send_envelopes`, the `SendEnvelopes` R1's keepalive and
    /// shadow timer send). An idle copy would otherwise never be heard again: on a writable
    /// partition with no writes nothing else is sent to it. Its answer reaches its cursor,
    /// which asks again, and that ask is served. The count is re-armed with
    /// this tick before the probe, so a probe that sends nothing is tried again after the
    /// back-off. It stops only when a transfer is open, when a new barrier was served, or when
    /// this node no longer leads the generation (a retired primary leads nothing).
    pub(super) fn retry_duty(&mut self, now: rdb_core::contracts::time::Tick) {
        let due: Vec<_> = self
            .snapshot_retries
            .iter()
            .filter(|(_, &(_, _, _, at))| {
                at.is_some_and(|at| {
                    super::HostClock::millis_until(at, now) >= self.snapshot_retry_backoff_millis
                })
            })
            .map(|(&key, &(generation, barrier, ..))| (key, generation, barrier))
            .collect();
        for (key, generation, barrier) in due {
            let (partition, copy) = key;
            if self.transfers.contains_key(&key) {
                continue;
            }
            if self
                .snapshots_served
                .get(&key)
                .is_some_and(|&served| served != (generation, barrier))
            {
                self.snapshot_retries.remove(&key);
                continue;
            }
            self.snapshots_served.remove(&key);
            let head = self
                .replication
                .primary(self.node, partition)
                .filter(|primary| {
                    !primary.tracker().retired()
                        && primary.tracker().lineage().generation == generation
                })
                .map(|primary| primary.tracker().head());
            tracing::info!(
                node = self.node.0,
                partition = partition.0,
                copy = copy.0,
                barrier = barrier.0,
                probe = ?head.map(|head| head.0),
                "snapshot_retry_reopened"
            );
            let Some(head) = head else {
                self.snapshot_retries.remove(&key);
                continue;
            };
            if let Some(retry) = self.snapshot_retries.get_mut(&key) {
                retry.3 = Some(now);
            }
            let site = Site {
                partition,
                correlation: rdb_core::contracts::ids::CorrelationId(0),
            };
            if let Err(fault) = self.send_envelopes(copy, head, head, site) {
                tracing::info!(
                    node = self.node.0,
                    partition = partition.0,
                    copy = copy.0,
                    ?fault,
                    "snapshot_retry_probe_failed"
                );
            }
        }
    }

    /// The `why` a heard refusal ends its transfer with. Every refusal is
    /// `refused`, which keeps the served barrier, except the first [`SNAPSHOT_RETRY_LIMIT`] (3)
    /// `install_retryable` ones at that barrier: `refused_retryable`, which forgets it, so R1's
    /// next ask re-offers the same barrier. The count is per `(generation, barrier)`: a new one
    /// starts it again. The 4th is `snapshot_retry_exhausted`: the barrier is kept, and the
    /// tick is recorded for [`Self::retry_duty`].
    fn refusal_kept(&mut self, refused: &SnapshotOfferRefused) -> &'static str {
        if refused.reason != SnapshotRefusal::InstallRetryable {
            return "refused";
        }
        let key = (refused.partition, refused.copy);
        let Some(&(generation, barrier)) = self.snapshots_served.get(&key) else {
            return "refused";
        };
        let count = match self.snapshot_retries.get(&key) {
            Some(&(at_generation, at_barrier, count, _))
                if (at_generation, at_barrier) == (generation, barrier) =>
            {
                count + 1
            }
            _ => 1,
        };
        let exhausted = count > SNAPSHOT_RETRY_LIMIT;
        self.snapshot_retries.insert(
            key,
            (
                generation,
                barrier,
                count,
                exhausted.then(|| self.clock.now()),
            ),
        );
        if exhausted {
            tracing::warn!(
                node = self.node.0,
                partition = refused.partition.0,
                copy = refused.copy.0,
                barrier = barrier.0,
                refusals = count,
                "snapshot_retry_exhausted"
            );
            return "refused";
        }
        tracing::info!(
            node = self.node.0,
            partition = refused.partition.0,
            copy = refused.copy.0,
            barrier = barrier.0,
            refusals = count,
            "snapshot_retry_allowed"
        );
        "refused_retryable"
    }

    /// Drop `(partition, copy)`'s transfer and its pin, logged `event{why}`. A dropped transfer
    /// also forgets the barrier it served, so R1's next ask opens a new one, except after a
    /// refusal, which waits for a new barrier (rev 5 M1: no re-offer at that S).
    pub(super) fn end_transfer(
        &mut self,
        partition: PartitionId,
        copy: CopyId,
        why: &str,
        event: &'static str,
    ) {
        // A caught-up copy has its History: its overrun count starts again (owner ruling
        // 2026-11-20, option 1), whether or not a transfer is still open.
        if why == "caught_up" {
            self.overruns_caught_up(partition, copy);
        }
        let Some(transfer) = self.transfers.remove(&(partition, copy)) else {
            return;
        };
        if why != "refused" {
            self.snapshots_served.remove(&(partition, copy));
        }
        if why == "caught_up" {
            self.snapshot_retries.remove(&(partition, copy));
        }
        if event == "snapshot_transfer_abandoned" {
            tracing::warn!(
                node = self.node.0,
                partition = partition.0,
                copy = copy.0,
                at = transfer.at.0,
                pulled = transfer.pulled,
                why,
                "snapshot_transfer_abandoned"
            );
        } else {
            tracing::info!(
                node = self.node.0,
                partition = partition.0,
                copy = copy.0,
                at = transfer.at.0,
                pulled = transfer.pulled,
                why,
                "snapshot_transfer_released"
            );
        }
    }

    /// The lowest `S` of a transfer this node serves for `partition`: a trim must not pass it
    /// (design section 3.1, the transfer pin).
    pub(super) fn transfer_pin(&self, partition: PartitionId) -> Option<Seq> {
        self.transfers
            .iter()
            .filter(|((p, _), _)| *p == partition)
            .map(|(_, transfer)| transfer.at)
            .min()
    }

    /// Peer `node` started again: its transfers are dropped at once (m-d), so R1's next ask
    /// re-offers to the new process. A barrier kept for one of its copies, and
    /// that copy's retry count, are forgotten too; the new process is not the one that refused.
    pub(super) fn transfers_peer_booted(&mut self, node: NodeId) {
        let gone: Vec<_> = self
            .transfers
            .iter()
            .filter(|(_, transfer)| transfer.node == node)
            .map(|(key, _)| *key)
            .collect();
        for (partition, copy) in gone {
            self.end_transfer(
                partition,
                copy,
                "peer_booted",
                "snapshot_transfer_abandoned",
            );
        }
        let kept: Vec<_> = self
            .snapshots_served
            .keys()
            .chain(self.snapshot_retries.keys())
            .copied()
            .filter(|key| !self.transfers.contains_key(key))
            .filter(|&(partition, copy)| {
                self.replication
                    .primary(self.node, partition)
                    .is_some_and(|primary| {
                        primary
                            .tracker()
                            .config()
                            .members
                            .iter()
                            .any(|member| member.copy == copy && member.node == node)
                    })
            })
            .collect();
        for key in kept {
            self.snapshots_served.remove(&key);
            self.snapshot_retries.remove(&key);
        }
    }

    /// Whether `transfer`'s receiver is still authorised. This node still leads
    /// the transfer's generation, and its committed config still names `copy` at the
    /// transfer's node. A pull from a copy removed from the plan, or after this node stopped
    /// leading that generation, ends the transfer (`why = not_a_member`). A retired tracker
    /// keeps its old lineage and config, so it is skipped first.
    fn still_a_member(&self, partition: PartitionId, copy: CopyId, transfer: &Transfer) -> bool {
        self.replication
            .primary(self.node, partition)
            .map(|primary| primary.tracker())
            .filter(|tracker| !tracker.retired())
            .is_some_and(|tracker| {
                tracker.lineage().generation == transfer.lineage.generation
                    && tracker
                        .config()
                        .members
                        .iter()
                        .any(|member| member.copy == copy && member.node == transfer.node)
            })
    }

    /// A pull from the target: answer with the next page, or drop it when no transfer matches.
    pub(super) fn snapshot_pull(&mut self, pull: SnapshotPull) {
        let key = (pull.partition, pull.copy);
        let why = match self.transfers.get(&key) {
            None => Some("no_transfer"),
            Some(transfer) if transfer.node != pull.node => Some("not_the_planned_receiver"),
            Some(transfer)
                if transfer.lineage.generation != pull.generation
                    || transfer.at != pull.at
                    || transfer.offer.is_some() =>
            {
                Some("mismatch")
            }
            Some(transfer) if !self.still_a_member(pull.partition, pull.copy, transfer) => {
                Some("not_a_member")
            }
            Some(_) => None,
        };
        if why == Some("not_a_member") {
            self.end_transfer(
                pull.partition,
                pull.copy,
                "not_a_member",
                "snapshot_transfer_abandoned",
            );
        }
        if let Some(why) = why {
            tracing::info!(
                node = self.node.0,
                partition = pull.partition.0,
                copy = pull.copy.0,
                generation = pull.generation.0,
                at = pull.at.0,
                from = pull.node.0,
                why,
                "snapshot_pull_dropped"
            );
            return;
        }
        let now = self.clock.now();
        let Some(transfer) = self.transfers.get_mut(&key) else {
            return;
        };
        #[cfg(debug_assertions)]
        if self
            .snapshot_pages_held_from
            .is_some_and(|from| transfer.pages >= from)
        {
            tracing::info!(
                node = self.node.0,
                partition = pull.partition.0,
                copy = pull.copy.0,
                page = transfer.pages,
                "snapshot_pull_held"
            );
            return;
        }
        // Capped by size too, so a page of large values fits the frame, and
        // ended from the cursor: a page cut short by size is not the last.
        let page = transfer.view.page_after(
            pull.after.as_ref().map(|(ns, key)| (*ns, key.as_ref())),
            self.snapshot_page_keys,
            self.snapshot_page_bytes,
        );
        let (page, more) = match (page, transfer.view.fault()) {
            (Ok(page), None) => page,
            (result, fault) => {
                tracing::warn!(
                    node = self.node.0,
                    partition = pull.partition.0,
                    copy = pull.copy.0,
                    error = ?result.err(),
                    fault = ?fault,
                    "snapshot_page_read_failed"
                );
                self.end_transfer(
                    pull.partition,
                    pull.copy,
                    "read_failed",
                    "snapshot_transfer_abandoned",
                );
                return;
            }
        };
        transfer.touched = now;
        transfer.pulled += page.len() as u64;
        transfer.bytes += page
            .iter()
            .map(|(_, key, value, _, _)| (key.len() + value.len()) as u64)
            .sum::<u64>();
        let index = transfer.pages;
        transfer.pages += 1;
        let done = !more;
        let chunk = SnapshotChunk {
            from: self.node,
            partition: pull.partition,
            generation: pull.generation,
            at: pull.at,
            copy: pull.copy,
            chain: chunk_chain(pull.prev, &page),
            after: pull.after,
            entries: page,
            done,
        };
        tracing::debug!(
            node = self.node.0,
            partition = pull.partition.0,
            copy = pull.copy.0,
            entries = chunk.entries.len(),
            done,
            "snapshot_chunk_sent"
        );
        #[cfg(debug_assertions)]
        let chunk = self.snapshot_chunk_fault(index, chunk);
        #[cfg(not(debug_assertions))]
        let _ = index;
        self.send_peer(
            pull.node,
            Msg::SnapshotChunk(Box::new(chunk)),
            "snapshot_chunk",
        );
    }

    /// S2-R3's hook: page `index` with one byte of its first entry's value flipped, when
    /// [`Host::snapshot_chunk_fault`] names it. Logged `snapshot_chunk_corrupted`.
    #[cfg(debug_assertions)]
    fn snapshot_chunk_fault(&self, index: u64, mut chunk: SnapshotChunk) -> SnapshotChunk {
        if self.snapshot_chunk_fault != Some(index) {
            return chunk;
        }
        let Some(entry) = chunk.entries.first_mut() else {
            return chunk;
        };
        let mut value = entry.2.to_vec();
        match value.first_mut() {
            Some(byte) => *byte ^= 0xff,
            None => value.push(0xff),
        }
        entry.2 = Bytes::from(value);
        tracing::warn!(node = self.node.0, index, "snapshot_chunk_corrupted");
        chunk
    }

    /// The target refused: drop the transfer and its pin at once (rev 5 M1).
    pub(super) fn snapshot_refused_heard(&mut self, refused: &SnapshotOfferRefused) {
        tracing::info!(
            node = self.node.0,
            partition = refused.partition.0,
            copy = refused.copy.0,
            from = refused.node.0,
            at = refused.at.0,
            reason = refused.reason.reason(),
            through = ?refused.through.map(|seq| seq.0),
            transfer = refused.transfer,
            "snapshot_offer_refused_heard"
        );
        let open = self.transfers.get(&(refused.partition, refused.copy));
        let ours = open.is_some_and(|transfer| {
            transfer.node == refused.node
                && transfer.lineage.generation == refused.generation
                && transfer.at == refused.at
                && transfer.id == refused.transfer
        });
        // A refusal of an earlier transfer at the same `S` (an install that expired,
        // its refusal delayed past the transfer that replaced it) ends nothing and counts
        // toward no limit.
        if let Some(open) = open.filter(|open| !ours && open.id != refused.transfer) {
            tracing::info!(
                node = self.node.0,
                partition = refused.partition.0,
                copy = refused.copy.0,
                at = refused.at.0,
                transfer = refused.transfer,
                open = open.id,
                "snapshot_refusal_stale"
            );
        }
        if ours {
            let why = self.refusal_kept(refused);
            self.end_transfer(
                refused.partition,
                refused.copy,
                why,
                "snapshot_transfer_released",
            );
        }
        // Rev 5b: a held tail is recorded, so `admin forget-tail` counts it and the owner's
        // status names the command. Only for the transfer open now: a late
        // refusal of an ended one, after the writable re-plan cleared its hold, records none.
        if let (true, SnapshotRefusal::TailHeld, Some(through)) =
            (ours, refused.reason, refused.through)
        {
            self.offer_holds
                .entry(refused.partition)
                .or_default()
                .insert(
                    refused.copy,
                    OfferHold {
                        node: refused.node,
                        head: through,
                    },
                );
            tracing::warn!(
                node = self.node.0,
                partition = refused.partition.0,
                copy = refused.copy.0,
                holder = refused.node.0,
                head = through.0,
                "offer_hold_recorded"
            );
        }
    }

    /// An accepted `admin forget-tail --through through` (rev 5b, lead ruling R18): every copy
    /// whose offer was refused for a tail at or below `through` is no longer held. No offer is
    /// sent for it here: the forget-tail's re-plan commits writable, which supersedes the tail,
    /// and the next ordinary catch-up offer under that commit goes through. Answers `(copy,
    /// node, head)` of each released copy.
    pub(super) fn offer_holds_released(
        &mut self,
        partition: PartitionId,
        through: Seq,
    ) -> Vec<(CopyId, NodeId, Seq)> {
        let holds = self.offer_holds.entry(partition).or_default();
        let released: Vec<_> = holds
            .iter()
            .filter(|(_, hold)| hold.head <= through)
            .map(|(copy, hold)| (*copy, hold.node, hold.head))
            .collect();
        holds.retain(|_, hold| hold.head > through);
        for (copy, node, head) in &released {
            tracing::warn!(
                node = self.node.0,
                partition = partition.0,
                copy = copy.0,
                holder = node.0,
                head = head.0,
                through = through.0,
                "offer_hold_released"
            );
        }
        released
    }

    /// `NodeStatus::needs_operator` for `partition` (rev 5 M1): this copy's own held refusal,
    /// else the first held copy the owner heard of that no forget-tail has released yet, else
    /// the first copy this node gave up offering after repeated budget overruns (issue #61
    /// S3-R4 (b)). A forget-tail comes first: acting on it decides what data survives.
    pub(super) fn needs_operator(&self, partition: PartitionId) -> Option<String> {
        if let Some(held) = self.offer_tail_held.get(&partition) {
            return Some(needs_forget_tail(
                partition, held.copy, self.node, held.head,
            ));
        }
        self.offer_holds
            .get(&partition)
            .and_then(|holds| holds.iter().next())
            .map(|(copy, hold)| needs_forget_tail(partition, *copy, hold.node, hold.head))
            .or_else(|| self.needs_history_budget(partition))
    }

    /// An offer reached this target (module docs, "Target"). A held install it replaces is
    /// aborted first; a failed abort faults the node.
    pub(super) fn snapshot_offer(&mut self, offer: SnapshotOffer) -> Result<(), super::Fault> {
        let partition = offer.lineage.partition;
        match self.offer_check(&offer) {
            Ok(()) => {}
            Err((reason, held)) => {
                tracing::warn!(
                    node = self.node.0,
                    partition = partition.0,
                    copy = offer.copy.0,
                    from = offer.from.0,
                    generation = offer.lineage.generation.0,
                    epoch = offer.lineage.owner_epoch.0,
                    at = offer.at.0,
                    reason = reason.reason(),
                    through = ?held.map(|hit| hit.head.0),
                    "snapshot_offer_refused"
                );
                if let Some(hit) = held {
                    self.copy_needs_forget_tail(&offer, hit);
                }
                self.refuse_offer(&offer, reason, held.map(|hit| hit.head));
                return Ok(());
            }
        }
        if let Some(held) = self.installs.remove(&partition) {
            tracing::info!(
                node = self.node.0,
                partition = partition.0,
                replaced_at = held.offer.at.0,
                at = offer.at.0,
                deferred = held.deferred,
                "snapshot_install_replaced"
            );
            self.abort_begun(&held)?;
        }
        let barrier = ReplicationEnvelope::decode(&offer.barrier)
            .map(|envelope| envelope.record_digest)
            .unwrap_or(Digest::ROOT);
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            copy = offer.copy.0,
            from = offer.from.0,
            generation = offer.lineage.generation.0,
            at = offer.at.0,
            "snapshot_offer_accepted"
        );
        let install = Install {
            offer,
            begun: false,
            chain: barrier,
            after: None,
            entries: 0,
            chunks: 0,
            started: std::time::Instant::now(),
            deferred: 0,
            largest_page: 0,
            touched: self.clock.now(),
            retries: 0,
        };
        self.pull_next(&install);
        self.installs.insert(partition, install);
        Ok(())
    }

    /// Design section 2 step 2: the offer's authority and freshness, or why not.
    ///
    /// Last, the held-tail check ([`Self::offer_tail`]): a held tail is refused `tail_held`,
    /// with the hit.
    fn offer_check(&self, offer: &SnapshotOffer) -> Result<(), OfferRefusal> {
        let partition = offer.lineage.partition;
        let Some(receiver) = self
            .replication
            .receiver(self.node, partition)
            .filter(|receiver| !receiver.is_dormant() && !receiver.retired())
        else {
            return Err((SnapshotRefusal::NotAdopted, None));
        };
        if receiver.lineage() != offer.lineage || receiver.copy() != offer.copy {
            return Err((SnapshotRefusal::NotAdopted, None));
        }
        let primary = receiver
            .config()
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary);
        if primary.is_none_or(|primary| (primary.node, primary.boot) != (offer.from, offer.boot)) {
            return Err((SnapshotRefusal::NotPrimary, None));
        }
        if offer.at <= receiver.applied_head().seq {
            return Err((SnapshotRefusal::NotAhead, None));
        }
        let barrier = ReplicationEnvelope::decode(&offer.barrier)
            .map_err(|_| (SnapshotRefusal::BadBarrier, None))?;
        if barrier.header.seq != offer.at
            || barrier.compute_record_digest().ok() != Some(barrier.record_digest)
        {
            return Err((SnapshotRefusal::BadBarrier, None));
        }
        match self.offer_tail(offer)? {
            None => Ok(()),
            Some(hit) => Err((SnapshotRefusal::TailHeld, Some(hit))),
        }
    }

    /// Issue #61 (design rev 4 N2, rev 5 M1 and 5b): the #105 tail this copy holds under the
    /// offer's lineage, if any.
    ///
    /// Nothing else lets the offer pass (lead ruling R19): a forget-tail's run commits
    /// writable, which supersedes the tail before any later offer, and a run that does not
    /// commit leaves the next run to report the tail held again.
    ///
    /// **L comes from the landed commit (lead ruling R15)**, as [`Host::landing_scan`] takes it.
    /// A writable commit at generation L was formed through a barrier that already carries every
    /// acknowledged record of older generations, so an older tail above the base holds only
    /// unacknowledged records, and a forget-tail alarm over it would be false. That is why a
    /// copy left out of a writable commit (a dropped copy, re-added as a shadow) never hits
    /// here: the commit's L is its own generation, every older tail is stamped below it, and the
    /// install discards those records as the superseded ones they are
    /// (`t61_a_dropped_copys_older_tail_under_a_writable_commit_is_discarded_by_the_install`).
    /// A hit needs a commit that supersedes nothing older: L unknown (`None`), which on this
    /// build is a partition whose record an older build wrote and that has committed no
    /// writable generation since (an upgrade, lead ruling R18). The first writable commit ends
    /// it, as the forget-tail's re-plan does.
    ///
    /// The landed commit anchors the scan; with none at the offer's lineage nothing proves the
    /// tail superseded, so the offer is refused `not_adopted`.
    fn offer_tail(&self, offer: &SnapshotOffer) -> Result<Option<super::TailHit>, OfferRefusal> {
        let partition = offer.lineage.partition;
        let Some(committed) = self
            .committed
            .get(&partition)
            .filter(|committed| committed.selected.root == offer.lineage)
        else {
            return Err((SnapshotRefusal::NotAdopted, None));
        };
        let anchor = rdb_core::contracts::recovery::LineageAnchor {
            lineage: committed.selected.root,
            base_seq: committed.selected.cutoff_seq,
            base_digest: committed.selected.cutoff_digest,
        };
        Ok(self.tail_scan(partition, anchor, committed.ledger.last_writable))
    }

    /// Warn `copy_needs_forget_tail` for `hit` once per `(partition, copy)` (rev 5 M1), with the
    /// full command (lead binding), and keep it for `NodeStatus::needs_operator`.
    fn copy_needs_forget_tail(&mut self, offer: &SnapshotOffer, hit: super::TailHit) {
        let partition = offer.lineage.partition;
        if self
            .offer_tail_held
            .get(&partition)
            .is_some_and(|held| held.copy == offer.copy)
        {
            return;
        }
        tracing::warn!(
            node = self.node.0,
            partition = partition.0,
            copy = offer.copy.0,
            from = hit.from.0,
            head = hit.head.0,
            through = hit.head.0,
            command = %forget_tail_command(partition, hit.head),
            "copy_needs_forget_tail"
        );
        self.offer_tail_held.insert(
            partition,
            OfferTailHeld {
                copy: offer.copy,
                from: hit.from,
                head: hit.head,
            },
        );
    }

    /// Tell the offerer why, so it drops its transfer and pin at once (rev 5 M1).
    fn refuse_offer(
        &mut self,
        offer: &SnapshotOffer,
        reason: SnapshotRefusal,
        through: Option<Seq>,
    ) {
        let refused = SnapshotOfferRefused {
            partition: offer.lineage.partition,
            generation: offer.lineage.generation,
            at: offer.at,
            copy: offer.copy,
            node: self.node,
            reason,
            through,
            transfer: offer.transfer,
        };
        self.send_peer(
            offer.from,
            Msg::SnapshotOfferRefused(refused),
            "snapshot_offer_refused",
        );
    }

    /// Ask the source for the page after `install`'s cursor.
    fn pull_next(&mut self, install: &Install) {
        let pull = SnapshotPull {
            partition: install.offer.lineage.partition,
            generation: install.offer.lineage.generation,
            at: install.offer.at,
            copy: install.offer.copy,
            node: self.node,
            after: install.after.clone(),
            prev: install.chain,
        };
        self.send_peer(install.offer.from, Msg::SnapshotPull(pull), "snapshot_pull");
    }

    /// Whether `frame`, delivered for `partition`, is an ordinary append of the
    /// lineage an install here owns, contiguous with the receiver's accept head, and so is not
    /// handed to the receiver. Only such a record would be staged; any other append (a head
    /// probe or a keepalive past a gap, or a repeat) reaches the receiver's validation, which
    /// answers it without staging (`NeedPrefix`, or `AlreadyHave` and an ACK), so R1 still
    /// hears where this copy stands and can ask again. Staged, its commit
    /// would meet the engine's installing guard (which stays) as `WriteFailed`, and R1 would
    /// fence the partition for this boot. Dropped, it is not lost: R1's retransmit re-sends an
    /// unacked record, and the re-seed's ACK at `S` walks `S + 1 ..= head`. Every other frame
    /// is delivered: replies, recovery appends, other partitions, another lineage's appends.
    ///
    /// Queueing the frames for after the install was rejected: during a slow install (8.9 s
    /// under load) the queue grows with the write rate, and on replay its entries are mostly at
    /// or below `S`, stale by then. Logged once per install, `append_deferred_during_install`
    /// with the first seq; the count rides `snapshot_transfer_installed{deferred}`.
    pub(super) fn append_deferred(&mut self, partition: PartitionId, frame: &Frame) -> bool {
        let Some(install) = self.installs.get_mut(&partition) else {
            return false;
        };
        if frame.sender != install.offer.lineage
            || rdb_core::replication::wire::classify(&frame.body)
                != Some(rdb_core::replication::wire::R1Frame::Append)
        {
            return false;
        }
        let contiguous = ReplicationEnvelope::decode_header(&frame.body)
            .ok()
            .zip(self.replication.receiver(self.node, partition))
            .is_some_and(|(header, receiver)| header.seq == receiver.accept_head().seq.next());
        if !contiguous {
            return false;
        }
        install.deferred += 1;
        if install.deferred == 1 {
            let seq =
                ReplicationEnvelope::decode(&frame.body).map(|envelope| envelope.header.seq.0);
            tracing::info!(
                node = self.node.0,
                partition = partition.0,
                generation = install.offer.lineage.generation.0,
                at = install.offer.at.0,
                seq = ?seq.ok(),
                "append_deferred_during_install"
            );
        }
        true
    }

    /// A page reached this target: check its chain, write it, and pull the next or finish.
    pub(super) fn snapshot_chunk(&mut self, chunk: SnapshotChunk) -> Result<(), super::Fault> {
        let partition = chunk.partition;
        let Some(mut install) = self.installs.remove(&partition) else {
            self.chunk_dropped(&chunk, "no_install");
            return Ok(());
        };
        let offer = &install.offer;
        if (offer.lineage.generation, offer.at, offer.copy)
            != (chunk.generation, chunk.at, chunk.copy)
            || install.after != chunk.after
        {
            self.chunk_dropped(&chunk, "mismatch");
            self.installs.insert(partition, install);
            return Ok(());
        }
        if chunk.from != offer.from {
            self.chunk_dropped(&chunk, "not_the_source");
            self.installs.insert(partition, install);
            return Ok(());
        }
        // m-d: a target that re-adopted another lineage drops the install it held, and aborts
        // it as an expiry does, so no installing marker outlives the install.
        let adopted = self
            .replication
            .receiver(self.node, partition)
            .is_some_and(|receiver| receiver.lineage() == install.offer.lineage);
        if !adopted {
            tracing::info!(
                node = self.node.0,
                partition = partition.0,
                at = install.offer.at.0,
                deferred = install.deferred,
                "snapshot_install_dropped"
            );
            return self.abort_begun(&install);
        }
        if chunk_chain(install.chain, &chunk.entries) != chunk.chain {
            return self.install_refused(&install, SnapshotRefusal::Chain, "chain");
        }
        let (generation, at) = (install.offer.lineage.generation, install.offer.at);
        if !install.begun {
            if let Err(error) = self.engine.install_begin(partition, generation, at) {
                return self.install_refused(&install, install_refusal(&error), error.reason());
            }
            install.begun = true;
            self.offer_tail_held.remove(&partition);
            self.dormant_ladders.remove(&partition);
            self.inventory_ladders.remove(&partition);
            self.ladders.remove(&partition);
        }
        let written = self.write_page(partition, generation, at, &chunk.entries);
        if let Err(error) = written {
            return self.install_refused(&install, install_refusal(&error), error.reason());
        }
        install.chain = chunk.chain;
        install.entries += chunk.entries.len() as u64;
        let page_bytes: usize = chunk
            .entries
            .iter()
            .map(|(_, key, value, ..)| rdb_storage::snapshot_entry_bytes(key, value))
            .sum();
        install.largest_page = install.largest_page.max(page_bytes as u64);
        install.chunks += 1;
        install.after = chunk
            .entries
            .last()
            .map(|(ns, key, ..)| (*ns, key.clone()))
            .or(install.after);
        if !chunk.done {
            install.touched = self.clock.now();
            install.retries = 0;
            self.pull_next(&install);
            self.installs.insert(partition, install);
            return Ok(());
        }
        let offer = &install.offer;
        if let Err(error) =
            self.engine
                .install_finish(partition, generation, at, &offer.barrier, offer.root)
        {
            return self.install_refused(&install, install_refusal(&error), error.reason());
        }
        self.reseed(&install)
    }

    /// `install_chunk`, or, while [`Host::snapshot_install_faults`] is above 0, a storage fault
    /// in its place, counting it down. Rows only set it.
    fn write_page(
        &mut self,
        partition: PartitionId,
        generation: Generation,
        at: Seq,
        entries: &[SnapshotEntry],
    ) -> Result<usize, rdb_storage::InstallError> {
        #[cfg(debug_assertions)]
        if self.snapshot_install_faults > 0 {
            self.snapshot_install_faults -= 1;
            return Err(rdb_storage::InstallError::Storage(
                rdb_core::contracts::storage::StorageFault::WriteFailed,
            ));
        }
        self.engine
            .install_chunk(partition, generation, at, entries)
    }

    /// Every step: an install with no page for
    /// [`Host::snapshot_pull_retry_millis`] re-sends its pull (`snapshot_pull_retried`), up to
    /// [`SNAPSHOT_PULL_RETRIES`] times between pages. Past that it expires: the install is
    /// dropped (`snapshot_install_expired`) and the source is told `install_retryable`, so it
    /// ends the transfer and R1's next ask re-offers. Nothing here touches the owner's repair
    /// or its read-only window, which `Host::repair_duty` ends on its own clock. An install
    /// that had begun is given up first ([`Self::abort_begun`]), whose failure is the fault
    /// this answers.
    pub(super) fn install_duty(
        &mut self,
        now: rdb_core::contracts::time::Tick,
    ) -> Result<(), super::Fault> {
        let partitions: Vec<PartitionId> = self.installs.keys().copied().collect();
        for partition in partitions {
            let Some(install) = self.installs.get_mut(&partition) else {
                continue;
            };
            if super::HostClock::millis_until(install.touched, now)
                < self.snapshot_pull_retry_millis
            {
                continue;
            }
            if install.retries < SNAPSHOT_PULL_RETRIES {
                install.retries += 1;
                install.touched = now;
                let install = install.clone();
                tracing::info!(
                    node = self.node.0,
                    partition = partition.0,
                    copy = install.offer.copy.0,
                    at = install.offer.at.0,
                    chunks = install.chunks,
                    retry = install.retries,
                    "snapshot_pull_retried"
                );
                self.pull_next(&install);
                continue;
            }
            let Some(install) = self.installs.remove(&partition) else {
                continue;
            };
            tracing::warn!(
                node = self.node.0,
                partition = partition.0,
                copy = install.offer.copy.0,
                at = install.offer.at.0,
                begun = install.begun,
                chunks = install.chunks,
                deferred = install.deferred,
                retries = install.retries,
                "snapshot_install_expired"
            );
            self.abort_begun(&install)?;
            self.refuse_offer(&install.offer, SnapshotRefusal::InstallRetryable, None);
        }
        Ok(())
    }

    fn chunk_dropped(&self, chunk: &SnapshotChunk, why: &str) {
        tracing::info!(
            node = self.node.0,
            partition = chunk.partition.0,
            copy = chunk.copy.0,
            generation = chunk.generation.0,
            at = chunk.at.0,
            from = chunk.from.0,
            why,
            "snapshot_chunk_dropped"
        );
    }

    /// Stop `install` (dropped from `installs` already), log why, give up what it began
    /// ([`Self::abort_begun`]), and tell the source.
    fn install_refused(
        &mut self,
        install: &Install,
        reason: SnapshotRefusal,
        detail: &str,
    ) -> Result<(), super::Fault> {
        tracing::warn!(
            node = self.node.0,
            partition = install.offer.lineage.partition.0,
            copy = install.offer.copy.0,
            at = install.offer.at.0,
            begun = install.begun,
            chunks = install.chunks,
            deferred = install.deferred,
            reason = reason.reason(),
            detail,
            "snapshot_install_refused"
        );
        self.abort_begun(install)?;
        self.refuse_offer(&install.offer.clone(), reason, None);
        Ok(())
    }

    /// `install` ran `install_begin` and ends without `install_finish`, expired
    /// or refused. `install_begin` wiped the partition and left the engine's installing guard,
    /// while the receiver still claims the head it held before. Left so, a contiguous append
    /// (the head the keepalive or the shadow timer sends) would be staged and refused at commit
    /// as `WriteFailed`, and A1 fences the partition for the boot. So the lineage is removed
    /// ([`rdb_storage::RocksEngine::install_abort`], synced), and the receiver of the offer's
    /// lineage starts again at the root, as `Recovered` builds one for a copy holding nothing.
    /// Every later frame then meets a receiver that says what the disk holds: its
    /// `NeedPrefix(0)` draws a catch-up from seq 1, or, below a source's floor, a new ask.
    /// Logged `snapshot_install_receiver_rooted{held}` (warn). An abort that fails faults the
    /// node: the guard would stay under a receiver that claims a head the disk lacks.
    fn abort_begun(&mut self, install: &Install) -> Result<(), super::Fault> {
        if !install.begun {
            return Ok(());
        }
        let offer = &install.offer;
        let partition = offer.lineage.partition;
        self.engine
            .install_abort(partition, offer.lineage.generation, offer.at)
            .map_err(|error| {
                super::Fault::from(format!(
                    "snapshot install of p{} at {} could not be aborted: {error}",
                    partition.0, offer.at.0
                ))
            })?;
        let Some(held) = self
            .replication
            .receiver(self.node, partition)
            .filter(|held| held.lineage() == offer.lineage)
        else {
            return Ok(());
        };
        let before = held.accept_head().seq;
        let receiver = held.rewound().map_err(|error| {
            super::Fault::from(format!("snapshot abort re-root refused: {error:?}"))
        })?;
        self.replication.install_receiver(receiver);
        tracing::warn!(
            node = self.node.0,
            partition = partition.0,
            copy = offer.copy.0,
            at = offer.at.0,
            held = before.0,
            "snapshot_install_receiver_rooted"
        );
        Ok(())
    }

    /// After `install_finish`: a live receiver at `(S, barrier digest)`, then its ACK to the
    /// primary (module docs, "Re-seed").
    fn reseed(&mut self, install: &Install) -> Result<(), super::Fault> {
        let offer = &install.offer;
        let partition = offer.lineage.partition;
        let digest = ReplicationEnvelope::decode(&offer.barrier)
            .map(|envelope| envelope.record_digest)
            .map_err(|error| {
                super::Fault::from(format!("installed barrier does not decode: {error:?}"))
            })?;
        let Some(held) = self.replication.receiver(self.node, partition) else {
            return Err(super::Fault::from(format!(
                "snapshot install of p{} finished with no receiver",
                partition.0
            )));
        };
        let config = held.config().clone();
        let head = rdb_core::replication::append::Head {
            seq: offer.at,
            digest,
        };
        let receiver = AppendReceiver::new(ReceiverInit {
            config: config.clone(),
            own: offer.copy,
            lineage: offer.lineage,
            head,
            durable: rdb_core::contracts::ids::DurableSeq(offer.at.0),
        })
        .map_err(|error| super::Fault::from(format!("snapshot re-seed refused: {error:?}")))?;
        let mut receiver = receiver;
        receiver.carry_control_from(held);
        let ack = reseed_ack(&receiver, offer.at);
        self.replication.install_receiver(receiver);
        self.dormant_ladders.remove(&partition);
        self.inventory_ladders.remove(&partition);
        self.ladders.remove(&partition);
        tracing::info!(
            node = self.node.0,
            partition = partition.0,
            copy = offer.copy.0,
            from = offer.from.0,
            generation = offer.lineage.generation.0,
            at = offer.at.0,
            entries = install.entries,
            chunks = install.chunks,
            deferred = install.deferred,
            largest_page_bytes = install.largest_page,
            millis = u64::try_from(install.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "snapshot_transfer_installed"
        );
        let Some(ack) = ack else {
            tracing::error!(
                node = self.node.0,
                partition = partition.0,
                at = offer.at.0,
                "snapshot_reseed_ack_withheld"
            );
            return Ok(());
        };
        let frame = Frame {
            id: UNSOLICITED,
            protocol: rdb_core::contracts::version::ENVELOPE_VERSION,
            config: config.config_version,
            sender: offer.lineage,
            body: encode_reply(&AppendOutcome::Accepted(ack)),
        };
        let site = Site {
            partition,
            correlation: rdb_core::contracts::ids::CorrelationId(0),
        };
        self.send(offer.from, frame, site);
        Ok(())
    }

    /// Send a snapshot message to a peer; an unreachable peer is logged, never a fault: the
    /// transfer's idle timeout or R1's next ask recovers it.
    fn send_peer(&mut self, to: NodeId, msg: Msg, what: &'static str) {
        if let Err(fault) = self.links.send(self.node, to, msg) {
            tracing::warn!(
                node = self.node.0,
                to = to.0,
                what,
                ?fault,
                "snapshot_send_failed"
            );
        }
    }
}
