//! P1's state machine for one partition on one boot (team kernel-a `design.md` §4.1, §4.2).
//!
//! [`PubKernel::apply`] takes one [`PubEvent`] and returns the [`PubEffect`]s in order. Rows are
//! evaluated top to bottom and the first match wins (K-A-43); the order of the `AuthorityAnswer`
//! rows is load-bearing (K-A-57). Pure: no clock, no randomness, no I/O, `BTreeMap` only.
//!
//! [`PubEvent`] and [`PubEffect`] are P1's own vocabulary. [`super::Publication`] translates the
//! contract's [`crate::contracts::event::Event`] into them and them back into
//! [`crate::contracts::event::Effect`]s; the rows here never see a carrier.

use std::collections::{BTreeMap, VecDeque};

use crate::contracts::authority::{
    AuthorityDecision, AuthorityView, BlockReason, Boundary, Checkpoint, DenyReason, Lineage,
    PartitionMode, Verdict,
};
use crate::contracts::digest::Digest;
use crate::contracts::errors::ErrorKind;
use crate::contracts::ids::{
    BootId, CorrelationId, Generation, PartitionId, RequestIdentity, Seq, SnapshotHandle,
    TimerVersion,
};
pub use crate::contracts::publication::{
    AppliedCandidate, FreezeCause, PubMode, StatusEntry, StatusOutcome,
};
use crate::contracts::qualification::{
    QualificationCause, QualificationChanged, QualificationDirection,
};
use crate::contracts::recovery::{RecoveryResult, RetainedStatusMap};
use crate::contracts::storage::{Namespace, SnapshotRead};
use crate::contracts::time::Tick;
use crate::contracts::txn::{Durability, Outcome as TxnOutcome, TxnResult};
use crate::replication::progress::DigestLookup;
use crate::transaction::dedup::{parse_dedup_key, parse_dedup_value, SEED_PAGE};

use super::status::{recovered_outcome, StatusIndex};
use super::view::ReplicationView;

/// The first correlation P1 mints for its own authority checks. The block `0x00D1 << 48` keeps
/// them apart from request correlations, which the harness numbers from zero.
pub const PUBLICATION_CORRELATION_BASE: u64 = 0x00D1 << 48;

/// The block of [`SnapshotHandle`]s P1 mints for the old-prefix view it keeps (lead ruling
/// A-R69a, M7A-108).
///
/// The scheme: `PUBLICATION_SNAPSHOT_BASE | u64::from(partition) << 16 | n`, where bits 48-63
/// hold the block `0x00D1`, bits 16-47 the partition, and bits 0-15 a wrapping `u16` counter per
/// kernel, starting at `0` (the view `Publication::install` asks for). A `PartitionId` is a
/// `u32`, so it fits bits 16-47 exactly and no two partitions share a handle. No other module
/// mints a `SnapshotHandle`; the harness's step-context view, rdb-sim's `STEP_VIEW`
/// (`u64::MAX`), lies outside the block. The same `0x00D1` tag marks P1's other minted identifiers,
/// each in its own id space: [`PUBLICATION_CORRELATION_BASE`] (`0x00D1 << 48`) for correlations,
/// and `PUBLICATION_TIMER_BASE` (`0x00D1 << 48`) for timers, beside `AUTHORITY_TIMER_BASE`
/// (`0x00A1 << 48`, `authority.rs`), `PROTECTION_TIMER_BASE` (`0x00B1 << 48`, `protection.rs`)
/// and `RECOVERY_TIMER_BASE` (`0x00F1 << 48`, `recovery.rs`).
///
/// The counter restarts with the kernel on a new boot. Until storage drops views on a crash, a
/// restarted counter can reuse a handle a crashed boot leaked.
pub const PUBLICATION_SNAPSHOT_BASE: u64 = 0x00D1 << 48;

/// The handle `n` in P1's block for `partition`.
#[must_use]
pub fn publication_snapshot(partition: PartitionId, n: u16) -> SnapshotHandle {
    SnapshotHandle(PUBLICATION_SNAPSHOT_BASE | u64::from(partition.0) << 16 | u64::from(n))
}

/// Whether `handle` is in P1's block for `partition`.
#[must_use]
pub fn is_publication_snapshot(handle: SnapshotHandle, partition: PartitionId) -> bool {
    handle.0 & !0xFFFF == PUBLICATION_SNAPSHOT_BASE | u64::from(partition.0) << 16
}

/// How long after a candidate arrives P1 answers `UNKNOWN_OUTCOME` (spec §5.3), in milliseconds.
///
/// **An assumption, not a specified value** (lead ruling A-R66). ADR 0007 names the post-apply
/// deadline but not its length, and neither the design nor `Budgets` gives one. 2,000 ms sits
/// below the 5,000-tick hold M7A-153 uses. Tests derive from this constant; none repeats it.
pub const POST_APPLY_DEADLINE_MILLIS: u64 = 2_000;

/// How many `Fresh` readers may wait behind one pending candidate by default (K-A-14).
pub const WAITER_CAP: usize = 8;

/// P1's tunables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubConfig {
    /// How many `Fresh` readers may wait behind one pending candidate. Default [`WAITER_CAP`].
    pub waiter_cap: usize,
    /// The post-apply deadline. Default [`POST_APPLY_DEADLINE_MILLIS`].
    pub post_apply_deadline_millis: u64,
}

impl Default for PubConfig {
    fn default() -> Self {
        Self {
            waiter_cap: WAITER_CAP,
            post_apply_deadline_millis: POST_APPLY_DEADLINE_MILLIS,
        }
    }
}

/// What a reader wants (design §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReadIntent {
    /// The newest published prefix; waits behind a pending candidate.
    Fresh,
    /// Whatever is published now; never waits for publication, but passes A1's read gate like
    /// any primary read (lead ruling A-R72a), and is refused by the same modes as `Fresh`
    /// (A-R72c).
    PreviousPublished,
}

/// A published position. The only thing a reader is ever handed (§4.3 invariant 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublishedAt {
    /// The generation.
    pub generation: Generation,
    /// The published sequence.
    pub seq: Seq,
}

/// Which conjunct of `may_publish` failed (design §4.2 refusal row).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PredicateFalse {
    /// Lineage, configuration or `qualifies_now`.
    Qualification,
    /// The digest binding (K-A-51).
    Digest(DigestLookup),
}

/// Why a reply was withheld.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Withheld {
    /// The `Reply` checkpoint denied.
    Denied(DenyReason),
    /// A1 fenced the partition first.
    Fence,
    /// Recovery replaced the lineage first.
    Recovery,
}

/// P1's trace facts (design §4.2). Every one is "the kernel handled the event on purpose".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PubFact {
    /// A candidate accepted while not serving (K-A-45).
    CandidateWhileNotServing {
        /// The mode it arrived in.
        mode: PubMode,
    },
    /// A candidate that cannot arrive; stated so the arm is total.
    CandidateUnreachable {
        /// The mode it arrived in.
        mode: PubMode,
    },
    /// A qualification edge for some other sequence, lineage or configuration.
    NotForThisCandidate {
        /// The edge's sequence.
        at_seq: Seq,
        /// The pending candidate's, if any.
        candidate: Option<Seq>,
    },
    /// A second `Gained` while the `Publication` check is outstanding.
    RecheckOutstanding,
    /// R1's predicate went false for the pending sequence.
    QualificationLost {
        /// Why, as R1 said.
        cause: QualificationCause,
    },
    /// R1's predicate went false for a published sequence. Publication is irreversible.
    QualificationLostAfterPublish {
        /// Why, as R1 said.
        cause: QualificationCause,
    },
    /// An answer that is not for any outstanding check.
    StaleAuthorityAnswer,
    /// `Admit` at `Publication` in `Blocked`.
    PublishRefusedBlocked,
    /// `Admit` at `Publication` in `Frozen{RecoveryReadOnly}` (test plan Q-6).
    PublishDeferred,
    /// `Admit` at `Publication`, but R1's predicate is false now.
    PublishPredicateFalse {
        /// Which conjunct.
        which: PredicateFalse,
    },
    /// Bytes applied under a lineage that may not publish. A marking, never a data move.
    Quarantined {
        /// The candidate's generation.
        generation: Generation,
        /// The candidate's sequence.
        seq: Seq,
    },
    /// `Admit` at `Reply` after the deadline already answered `Unknown`.
    ReplySuppressedAfterTimeout,
    /// No reply, nothing undone.
    ReplyWithheld {
        /// Why.
        why: Withheld,
    },
    /// A deadline for nothing pending.
    StaleTimer,
    /// An older authority view.
    StaleAuthorityView,
    /// R1 blocked the partition.
    Blocked {
        /// The operator-facing reason.
        reason: BlockReason,
    },
    /// A second block.
    AlreadyBlocked,
    /// A fence while blocked: replies withheld, mode kept.
    FenceWhileBlocked {
        /// The fence's cause.
        cause: FreezeCause,
    },
}

/// A terminal reply to a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReplyOutcome {
    /// Published: the only success in the system.
    Published {
        /// The result.
        result: TxnResult,
    },
    /// `UNKNOWN_OUTCOME`. Never a definitive failure.
    Unknown,
}

/// P1's inputs (design §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubEvent {
    /// T1's applied candidate.
    Candidate(AppliedCandidate),
    /// R1's predicate changed value.
    QualificationChanged(QualificationChanged),
    /// A1 answered a check.
    AuthorityAnswer(AuthorityDecision),
    /// The post-apply deadline timer fired at this version.
    PostApplyDeadline {
        /// The version it was armed under.
        version: TimerVersion,
    },
    /// A reader acquires the barrier.
    BarrierAcquire {
        /// Who.
        reader: RequestIdentity,
        /// What they want.
        intent: ReadIntent,
    },
    /// A status query. `None` when the caller named no generation.
    StatusQuery {
        /// The request asked about.
        request: RequestIdentity,
        /// The generation, if named.
        generation: Option<Generation>,
    },
    /// A1 fenced a scope covering this partition.
    Freeze {
        /// Why.
        cause: FreezeCause,
    },
    /// A1 pushed its view.
    AuthorityView(AuthorityView),
    /// R1 blocked the partition.
    BlockPartition {
        /// Why.
        reason: BlockReason,
    },
    /// What mode is this partition in.
    ModeQuery {
        /// Who asks.
        reader: RequestIdentity,
    },
    /// Recovery finished.
    Recovered(Box<RecoveryResult>),
    /// Trim `generation`'s status below `below`.
    StatusTrim {
        /// Which generation.
        generation: Generation,
        /// The watermark.
        below: Seq,
    },
    /// Drop `generation` whole.
    RetireGeneration {
        /// Which generation.
        generation: Generation,
    },
    /// Storage bound a view P1 asked for.
    SnapshotReady {
        /// The handle P1 minted.
        handle: SnapshotHandle,
        /// The sequence storage bound it at.
        at: Seq,
    },
}

/// P1's outputs (design §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubEffect {
    /// Arm the post-apply deadline.
    ArmTimer {
        /// The version.
        version: TimerVersion,
        /// When.
        at: Tick,
    },
    /// Cancel the post-apply deadline.
    CancelTimer {
        /// The version.
        version: TimerVersion,
    },
    /// Ask A1 for a recheck.
    AuthorityCheck {
        /// Where.
        checkpoint: Checkpoint,
        /// Under which lineage.
        lineage: Lineage,
        /// P1-minted, matched by the answer.
        correlation: CorrelationId,
    },
    /// A status-index write.
    Status(Box<StatusEntry>),
    /// A transaction's one terminal reply.
    Reply {
        /// Whose.
        request: RequestIdentity,
        /// What.
        outcome: ReplyOutcome,
    },
    /// Ask storage for a view of this partition, bound to `handle`.
    OpenSnapshot {
        /// P1-minted.
        handle: SnapshotHandle,
    },
    /// Drop a view P1 asked for.
    ReleaseSnapshot {
        /// P1-minted.
        handle: SnapshotHandle,
    },
    /// A `PreviousPublished` answer: the kept view, or the refusal.
    Previous {
        /// Who asked.
        reader: RequestIdentity,
        /// The kept view's handle, or why there is none.
        handle: Result<SnapshotHandle, ErrorKind>,
    },
    /// A `Fresh` barrier answer.
    Answer {
        /// Who asked.
        reader: RequestIdentity,
        /// The published position, or the refusal.
        answer: Result<PublishedAt, ErrorKind>,
        /// Whether the reader waited behind a candidate.
        waited: bool,
    },
    /// A status answer.
    StatusAnswer {
        /// The request asked about.
        request: RequestIdentity,
        /// The generation asked about, if named.
        generation: Option<Generation>,
        /// The answer.
        outcome: StatusOutcome,
    },
    /// A mode answer.
    Mode {
        /// Who asked.
        reader: RequestIdentity,
        /// The mode.
        mode: PubMode,
    },
    /// T1's `Published{seq}`.
    NotifyTxn {
        /// The lineage.
        lineage: Lineage,
        /// The published sequence.
        seq: Seq,
        /// Its record digest.
        record_digest: Digest,
        /// Its request.
        request: RequestIdentity,
    },
    /// A trace fact.
    Fact(PubFact),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    cand: AppliedCandidate,
    qualifying: Option<QualificationChanged>,
    recheck: Option<(CorrelationId, Tick)>,
    deadline: TimerVersion,
    replied: bool,
}

/// A published candidate whose `Reply` checkpoint has not answered (K-A-40).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AwaitingReply {
    /// The request.
    pub request: RequestIdentity,
    /// Its published sequence.
    pub seq: Seq,
    /// Its result.
    pub result: TxnResult,
    /// Whether the deadline already answered `Unknown`.
    pub replied: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Waiter {
    reader: RequestIdentity,
}

/// A read waiting for A1's `Read` check (lead ruling A-R72). `arrival` orders reads against the
/// check in flight: a check decides only the reads that had arrived when it was issued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GatedRead {
    reader: RequestIdentity,
    intent: ReadIntent,
    arrival: u64,
}

/// The `Read` check in flight. Invariant: `Some` whenever a read is gated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadCheck {
    correlation: CorrelationId,
    /// The lineage it was asked under; an `Admit` for any other admits no read.
    lineage: Lineage,
    /// The last arrival it decides.
    covers: u64,
}

/// A read refused before it is answered, in the carrier its intent is answered on.
fn refuse(reader: RequestIdentity, intent: ReadIntent, kind: ErrorKind) -> PubEffect {
    match intent {
        ReadIntent::Fresh => PubEffect::Answer {
            reader,
            answer: Err(kind),
            waited: false,
        },
        ReadIntent::PreviousPublished => PubEffect::Previous {
            reader,
            handle: Err(kind),
        },
    }
}

/// Design §3.4's code for a deny, on the pre-apply side: a read wrote nothing, so a refused read
/// has no outcome to be unknown about (lead ruling A-R72). The mapping is the contract's
/// [`DenyReason::client_error_kind`], the one T1 answers with too (lead ruling A-R72a).
const fn deny_kind(reason: DenyReason) -> ErrorKind {
    reason.client_error_kind(Boundary::PreApply)
}

/// The pending candidate, as a row may assert it. No sequence: §4.3 invariant 2 forbids a `pub`
/// accessor for the applied prefix, and the pending candidate's sequence is exactly that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingView {
    /// Its request.
    pub request: RequestIdentity,
    /// Whether a `Gained` is held for it.
    pub qualifying: bool,
    /// The outstanding `Publication` check, if any.
    pub recheck: Option<CorrelationId>,
    /// The deadline's timer version.
    pub deadline: TimerVersion,
    /// Whether the deadline already replied.
    pub replied: bool,
}

/// Everything a row may assert on, as one cloned value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubStateView {
    /// The boot this kernel lives in.
    pub boot: BootId,
    /// The lineage served.
    pub lineage: Lineage,
    /// The published position.
    pub published: PublishedAt,
    /// The pending candidate.
    pub pending: Option<PendingView>,
    /// Published candidates awaiting their `Reply` answer.
    pub awaiting_reply: BTreeMap<CorrelationId, AwaitingReply>,
    /// Queued `Fresh` readers, in arrival order.
    pub waiters: Vec<RequestIdentity>,
    /// The last authority view adopted.
    pub authority: Option<AuthorityView>,
    /// The mode.
    pub mode: PubMode,
    /// The status index.
    pub status: StatusIndex,
    /// The view bound at the published position, which `PreviousPublished` is answered from.
    pub kept: Option<SnapshotHandle>,
    /// Views asked for and not yet bound, with the published position each was asked at.
    pub opening: BTreeMap<SnapshotHandle, PublishedAt>,
    /// Reads waiting for A1's `Read` check, in arrival order (A-R72).
    pub gated: Vec<RequestIdentity>,
    /// The `Read` check in flight.
    pub read_check: Option<CorrelationId>,
}

/// The durable status a recovered kernel has not loaded yet (M7A-194, spec §8.1).
///
/// P1's status index lives in memory, so a kernel that recovered into `serving` on a node that
/// did not apply the predecessor's requests holds nothing for them, and `StatusIndex::lookup`
/// would answer `StatusExpired` for a generation still inside retention. The rows are not lost:
/// every transaction wrote its dedup record in the same atomic batch as its data (spec §4), and
/// T1 seeds its own index from them (`DedupIndex::seed`, A-R68/A-R70). This is the same seed,
/// from the same rows, at the same point: set by `Recovered`, loaded by the first step whose
/// snapshot shows the retained prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusSeed {
    /// The partition.
    pub partition: PartitionId,
    /// The generation the kernel serves. Every row loaded is from an older one.
    pub serving: Generation,
    /// Load rows at or below this sequence (`RetainedStatusMap.retained_through`), and load
    /// nothing until the snapshot shows at least this far.
    pub retained_through: Seq,
    /// The map the loaded rows fold under, exactly as `fold_recovered` folds held entries.
    pub map: RetainedStatusMap,
}

/// P1's state for one partition on one boot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubKernel {
    config: PubConfig,
    boot: BootId,
    lineage: Lineage,
    published_seq: Seq,
    pending: Option<Pending>,
    awaiting_reply: BTreeMap<CorrelationId, AwaitingReply>,
    status: StatusIndex,
    seed: Option<StatusSeed>,
    /// The snapshot `(generation, at)` the pending seed last found blocked by a row of the
    /// served generation or newer. Nothing is rescanned while the snapshot stays there
    /// (reviewer R-3); `None` while the seed is not blocked.
    seed_blocked_at: Option<(Generation, Seq)>,
    waiters: VecDeque<Waiter>,
    authority: Option<AuthorityView>,
    mode: PubMode,
    next_correlation: u64,
    next_timer_version: u64,
    /// Invariant: when `Some`, storage bound it at [`Self::published`].
    kept: Option<SnapshotHandle>,
    opening: BTreeMap<SnapshotHandle, PublishedAt>,
    next_snapshot: u16,
    gated: VecDeque<GatedRead>,
    read_check: Option<ReadCheck>,
    next_arrival: u64,
}

impl PubKernel {
    /// A kernel serving `lineage` with `published_seq` already published.
    #[must_use]
    pub fn new(config: PubConfig, boot: BootId, lineage: Lineage, published_seq: Seq) -> Self {
        Self {
            config,
            boot,
            lineage,
            published_seq,
            pending: None,
            awaiting_reply: BTreeMap::new(),
            status: StatusIndex::new(lineage.generation),
            seed: None,
            seed_blocked_at: None,
            waiters: VecDeque::new(),
            authority: None,
            mode: PubMode::Serving,
            next_correlation: 0,
            next_timer_version: 0,
            kept: None,
            opening: BTreeMap::new(),
            next_snapshot: 0,
            gated: VecDeque::new(),
            read_check: None,
            next_arrival: 0,
        }
    }

    /// The boot this kernel lives in.
    #[must_use]
    pub const fn boot(&self) -> BootId {
        self.boot
    }

    /// The lineage served.
    #[must_use]
    pub const fn lineage(&self) -> Lineage {
        self.lineage
    }

    /// The published position — the only snapshot a reader is ever handed.
    #[must_use]
    pub const fn published(&self) -> PublishedAt {
        PublishedAt {
            generation: self.lineage.generation,
            seq: self.published_seq,
        }
    }

    /// The durable status not loaded yet, or `None` once it is (or was never owed).
    #[must_use]
    pub const fn seed_pending(&self) -> Option<&StatusSeed> {
        self.seed.as_ref()
    }

    /// Load the pending seed from `snapshot` if it now shows the retained prefix (M7A-194).
    /// Does nothing once the seed is loaded. A blocked attempt leaves everything as it was, and
    /// the next step tries again — T1's `try_seed` discipline, over the same rows.
    ///
    /// A row that does not decode is skipped with a log line and the seed completes (lead
    /// ruling A-R90, tester-ka-next T1): fail-closed on that row, never on the whole seed, so
    /// one bad row cannot leave a retained generation unanswerable for ever. A row written in
    /// the generation being served or a newer one is different: it is not a predecessor's row,
    /// and its presence ahead of the seed means the snapshot is not the one the recovery
    /// described, so nothing is loaded and the seed stays pending until it no longer shows. The
    /// block logs one warning when it starts, and the seed rescans only once the snapshot moves
    /// (reviewer R-3).
    ///
    /// Only rows of the predecessor generation fold under the recovery's three-way rule; a row
    /// of an older generation loads as `RecoveredApplied`, as `fold_recovered` leaves an older
    /// generation's held entries alone (lead ruling A-R91, reviewer C-1).
    ///
    /// Each row lands through [`StatusIndex::restore`], so what this instance recorded itself,
    /// a retired generation and a trim watermark all win over the durable row. `record_digest`
    /// is `None`: the dedup row carries the request digest, not the record's.
    pub fn try_seed(&mut self, now: Tick, snapshot: &dyn SnapshotRead) {
        let Some(seed) = self.seed else {
            return;
        };
        if snapshot.at() < seed.retained_through {
            return;
        }
        let position = (snapshot.generation(), snapshot.at());
        if self.seed_blocked_at == Some(position) {
            return;
        }
        let mut found = Vec::new();
        let mut from = Vec::new();
        loop {
            let page = snapshot.scan(Namespace::Dedup, &from, SEED_PAGE);
            for (key, value) in &page {
                let (Some(key), Some(value)) = (parse_dedup_key(key), parse_dedup_value(value))
                else {
                    tracing::warn!(
                        partition = ?seed.partition,
                        serving = ?seed.serving,
                        key_len = key.len(),
                        value_len = value.len(),
                        "status seed: a dedup row does not decode; skipped (A-R90)"
                    );
                    continue;
                };
                // Above the cut: the discarded suffix, which a copy may still hold.
                if value.seq > seed.retained_through {
                    continue;
                }
                if key.generation >= seed.serving {
                    if self.seed_blocked_at.is_none() {
                        tracing::warn!(
                            partition = ?seed.partition,
                            serving = ?seed.serving,
                            row_generation = ?key.generation,
                            at = ?position.1,
                            "status seed: blocked by a row of the served generation or newer"
                        );
                    }
                    self.seed_blocked_at = Some(position);
                    return;
                }
                found.push((key, value));
            }
            match page.last() {
                Some((last, _)) if page.len() == SEED_PAGE => {
                    from = last.to_vec();
                    from.push(0);
                }
                _ => break,
            }
        }
        for (key, value) in found {
            let lineage = Lineage {
                partition: seed.partition,
                generation: key.generation,
                owner_epoch: value.owner_epoch,
            };
            // The result P1 published for the row, at the durability publication requires —
            // what T1's seed replays for the same retry.
            let result = TxnResult {
                partition: lineage.partition,
                owner_epoch: lineage.owner_epoch,
                generation: lineage.generation,
                seq: value.seq,
                outcome: TxnOutcome::Published,
                durability: Durability::BufferedOnTwo,
            };
            self.status.restore(StatusEntry {
                request: key.identity,
                lineage,
                seq: Some(value.seq),
                record_digest: None,
                outcome: if key.generation == seed.map.predecessor_generation {
                    recovered_outcome(&seed.map, value.seq, result)
                } else {
                    StatusOutcome::RecoveredApplied { result }
                },
                snapshot: None,
                at: now,
            });
        }
        self.seed = None;
        self.seed_blocked_at = None;
    }

    /// Everything a row may assert on.
    #[must_use]
    pub fn view(&self) -> PubStateView {
        PubStateView {
            boot: self.boot,
            lineage: self.lineage,
            published: self.published(),
            pending: self.pending.as_ref().map(|p| PendingView {
                request: p.cand.request,
                qualifying: p.qualifying.is_some(),
                recheck: p.recheck.map(|(c, _)| c),
                deadline: p.deadline,
                replied: p.replied,
            }),
            awaiting_reply: self.awaiting_reply.clone(),
            waiters: self.waiters.iter().map(|w| w.reader).collect(),
            authority: self.authority,
            mode: self.mode.clone(),
            status: self.status.clone(),
            kept: self.kept,
            opening: self.opening.clone(),
            gated: self.gated.iter().map(|r| r.reader).collect(),
            read_check: self.read_check.map(|c| c.correlation),
        }
    }

    /// One step. `repl` is R1's live predicate; `None` means no R1 view is reachable, and then
    /// nothing publishes.
    pub fn apply(
        &mut self,
        now: Tick,
        event: PubEvent,
        repl: Option<&dyn ReplicationView>,
    ) -> Vec<PubEffect> {
        match event {
            PubEvent::Candidate(cand) => self.on_candidate(now, cand),
            PubEvent::QualificationChanged(q) => self.on_qualification(now, q),
            PubEvent::AuthorityAnswer(answer) => self.on_answer(now, &answer, repl),
            PubEvent::PostApplyDeadline { version } => self.on_deadline(now, version),
            PubEvent::BarrierAcquire { reader, intent } => self.on_acquire(reader, intent),
            PubEvent::StatusQuery {
                request,
                generation,
            } => {
                let outcome = match generation {
                    Some(generation) => self.status_for(request, generation),
                    None => self.status.lookup_any(request),
                };
                vec![PubEffect::StatusAnswer {
                    request,
                    generation,
                    outcome,
                }]
            }
            PubEvent::Freeze { cause } => self.on_freeze(cause),
            PubEvent::AuthorityView(view) => self.on_view(view),
            PubEvent::BlockPartition { reason } => self.on_block(reason),
            PubEvent::ModeQuery { reader } => vec![PubEffect::Mode {
                reader,
                mode: self.mode.clone(),
            }],
            PubEvent::Recovered(result) => self.on_recovered(&result),
            PubEvent::StatusTrim { generation, below } => {
                self.status.trim(generation, below);
                Vec::new()
            }
            PubEvent::RetireGeneration { generation } => {
                self.status.retire(generation);
                Vec::new()
            }
            PubEvent::SnapshotReady { handle, at } => self.on_snapshot_ready(handle, at),
        }
    }

    // ---- candidate ----------------------------------------------------------------------------

    fn on_candidate(&mut self, now: Tick, cand: AppliedCandidate) -> Vec<PubEffect> {
        let accepts = self.pending.is_none() && cand.lineage == self.lineage;
        let not_serving = match &self.mode {
            PubMode::Serving => false,
            PubMode::Frozen {
                cause: FreezeCause::AuthorityLost(_) | FreezeCause::LocalStorageFenced,
            }
            | PubMode::Blocked { .. } => true,
            PubMode::Frozen {
                cause: FreezeCause::UnresolvedTransaction | FreezeCause::RecoveryReadOnly,
            } => {
                return vec![PubEffect::Fact(PubFact::CandidateUnreachable {
                    mode: self.mode.clone(),
                })];
            }
        };
        if !accepts {
            return vec![PubEffect::Fact(PubFact::CandidateUnreachable {
                mode: self.mode.clone(),
            })];
        }
        self.next_timer_version += 1;
        let deadline = TimerVersion(self.next_timer_version);
        let at = Tick(now.0.saturating_add(self.config.post_apply_deadline_millis));
        let mut effects = vec![
            PubEffect::ArmTimer {
                version: deadline,
                at,
            },
            self.write_status(now, &cand, StatusOutcome::Unknown),
        ];
        if not_serving {
            effects.push(PubEffect::Fact(PubFact::CandidateWhileNotServing {
                mode: self.mode.clone(),
            }));
        }
        self.pending = Some(Pending {
            cand,
            qualifying: None,
            recheck: None,
            deadline,
            replied: false,
        });
        effects
    }

    // ---- qualification ------------------------------------------------------------------------

    fn on_qualification(&mut self, now: Tick, q: QualificationChanged) -> Vec<PubEffect> {
        let pending_seq = self.pending.as_ref().map(|p| p.cand.seq);
        if q.direction == QualificationDirection::Lost
            && q.at_seq <= self.published_seq
            && pending_seq != Some(q.at_seq)
        {
            return vec![PubEffect::Fact(PubFact::QualificationLostAfterPublish {
                cause: q.cause,
            })];
        }
        let Some(pending) = self.pending.as_mut() else {
            return vec![PubEffect::Fact(PubFact::NotForThisCandidate {
                at_seq: q.at_seq,
                candidate: None,
            })];
        };
        let cand = pending.cand;
        if q.at_seq != cand.seq
            || q.lineage != cand.lineage
            || q.config_version != cand.config_version
        {
            return vec![PubEffect::Fact(PubFact::NotForThisCandidate {
                at_seq: q.at_seq,
                candidate: Some(cand.seq),
            })];
        }
        match q.direction {
            QualificationDirection::Gained if pending.recheck.is_some() => {
                vec![PubEffect::Fact(PubFact::RecheckOutstanding)]
            }
            QualificationDirection::Gained => {
                self.next_correlation += 1;
                let correlation =
                    CorrelationId(PUBLICATION_CORRELATION_BASE | self.next_correlation);
                // `pending` borrows `self`, so the minting is spelled out here.
                pending.qualifying = Some(q);
                pending.recheck = Some((correlation, now));
                vec![PubEffect::AuthorityCheck {
                    checkpoint: Checkpoint::Publication,
                    lineage: cand.lineage,
                    correlation,
                }]
            }
            QualificationDirection::Lost => {
                pending.qualifying = None;
                pending.recheck = None;
                vec![PubEffect::Fact(PubFact::QualificationLost {
                    cause: q.cause,
                })]
            }
        }
    }

    // ---- authority answers --------------------------------------------------------------------

    /// `answer_is_ours` (design §1.2): the correlation is the one we asked under, and the answer
    /// is no older than the view we hold. No view held means no answer is ours.
    fn fresh(&self, answer: &AuthorityDecision) -> bool {
        self.authority
            .is_some_and(|v| answer.authority_seq >= v.authority_seq)
    }

    fn on_answer(
        &mut self,
        now: Tick,
        answer: &AuthorityDecision,
        repl: Option<&dyn ReplicationView>,
    ) -> Vec<PubEffect> {
        if !self.fresh(answer) {
            let mut effects = vec![PubEffect::Fact(PubFact::StaleAuthorityAnswer)];
            effects.extend(self.reask(now, answer));
            return effects;
        }
        let for_reads = answer.checkpoint == Checkpoint::Read
            && self
                .read_check
                .is_some_and(|c| c.correlation == answer.correlation);
        if for_reads {
            return self.on_read_answer(answer);
        }
        let for_pending = answer.checkpoint == Checkpoint::Publication
            && self
                .pending
                .as_ref()
                .and_then(|p| p.recheck)
                .is_some_and(|(c, _)| c == answer.correlation);
        if for_pending {
            return self.on_publication_answer(now, answer, repl);
        }
        if answer.checkpoint == Checkpoint::Reply {
            if let Some(entry) = self.awaiting_reply.remove(&answer.correlation) {
                return vec![match answer.verdict {
                    Verdict::Admit if !entry.replied => PubEffect::Reply {
                        request: entry.request,
                        outcome: ReplyOutcome::Published {
                            result: entry.result,
                        },
                    },
                    Verdict::Admit => PubEffect::Fact(PubFact::ReplySuppressedAfterTimeout),
                    Verdict::Deny(reason) => PubEffect::Fact(PubFact::ReplyWithheld {
                        why: Withheld::Denied(reason),
                    }),
                }];
            }
        }
        vec![PubEffect::Fact(PubFact::StaleAuthorityAnswer)]
    }

    /// Lead ruling A-R69 (F2): a stale answer is never acted on, but when it answers the check
    /// still outstanding, that check is asked again against the view P1 now holds, under a fresh
    /// correlation. Otherwise nothing would ever answer it: A1 resyncs while a check is in flight,
    /// the answer lands older than the pushed view, and the recheck strands until the deadline.
    /// With no view held nothing is re-asked — no answer can be ours then, and asking would only
    /// loop. An answer that matches no outstanding check changes nothing.
    fn reask(&mut self, now: Tick, answer: &AuthorityDecision) -> Option<PubEffect> {
        self.authority?;
        if answer.checkpoint == Checkpoint::Read {
            // Issued now, so it may decide every read gated so far (A-R72).
            return self
                .read_check
                .is_some_and(|c| c.correlation == answer.correlation)
                .then(|| self.ask_read());
        }
        let (checkpoint, lineage) = match answer.checkpoint {
            Checkpoint::Publication => {
                let pending = self.pending.as_ref()?;
                let (outstanding, _) = pending.recheck?;
                if outstanding != answer.correlation {
                    return None;
                }
                (Checkpoint::Publication, pending.cand.lineage)
            }
            Checkpoint::Reply if self.awaiting_reply.contains_key(&answer.correlation) => {
                (Checkpoint::Reply, self.lineage)
            }
            _ => return None,
        };
        let correlation = self.mint_correlation();
        if checkpoint == Checkpoint::Publication {
            if let Some(pending) = self.pending.as_mut() {
                pending.recheck = Some((correlation, now));
            }
        } else if let Some(entry) = self.awaiting_reply.remove(&answer.correlation) {
            self.awaiting_reply.insert(correlation, entry);
        }
        Some(PubEffect::AuthorityCheck {
            checkpoint,
            lineage,
            correlation,
        })
    }

    fn mint_correlation(&mut self) -> CorrelationId {
        self.next_correlation += 1;
        CorrelationId(PUBLICATION_CORRELATION_BASE | self.next_correlation)
    }

    fn on_publication_answer(
        &mut self,
        now: Tick,
        answer: &AuthorityDecision,
        repl: Option<&dyn ReplicationView>,
    ) -> Vec<PubEffect> {
        let Some(pending) = self.pending.as_mut() else {
            return Vec::new();
        };
        let cand = pending.cand;
        let deny = match answer.verdict {
            Verdict::Deny(reason) => Some(reason),
            Verdict::Admit if !answer.same_lineage_as(&cand.authority) => {
                Some(DenyReason::GenerationChanged)
            }
            Verdict::Admit => None,
        };
        let Some(reason) = deny else {
            match &self.mode {
                PubMode::Blocked { .. } => {
                    pending.recheck = None;
                    return vec![PubEffect::Fact(PubFact::PublishRefusedBlocked)];
                }
                PubMode::Frozen {
                    cause: FreezeCause::RecoveryReadOnly,
                } => {
                    pending.recheck = None;
                    return vec![PubEffect::Fact(PubFact::PublishDeferred)];
                }
                PubMode::Serving | PubMode::Frozen { .. } => {}
            }
            return match may_publish(repl, &cand) {
                Ok(()) => self.publish(now),
                Err(which) => {
                    pending.qualifying = None;
                    pending.recheck = None;
                    vec![PubEffect::Fact(PubFact::PublishPredicateFalse { which })]
                }
            };
        };
        // Quarantine: the bytes stay where they are, in a namespace nobody reads (K-A-22).
        pending.qualifying = None;
        pending.recheck = None;
        let mut effects = vec![
            self.write_status(now, &cand, StatusOutcome::Unknown),
            PubEffect::Fact(PubFact::Quarantined {
                generation: cand.lineage.generation,
                seq: cand.seq,
            }),
        ];
        if !matches!(self.mode, PubMode::Blocked { .. }) {
            self.mode = PubMode::Frozen {
                cause: FreezeCause::AuthorityLost(reason),
            };
        }
        effects.extend(self.drain_waiters());
        effects
    }

    /// The publish row, steps 1-7 (design §4.2).
    fn publish(&mut self, now: Tick) -> Vec<PubEffect> {
        let Some(pending) = self.pending.take() else {
            return Vec::new();
        };
        let cand = pending.cand;
        // 1. The published position. Nothing is ever handed out above it.
        self.published_seq = cand.seq;
        // 2. Status.
        let mut effects = vec![self.write_status(
            now,
            &cand,
            StatusOutcome::Published {
                result: cand.pending_result,
            },
        )];
        // 3. Release the waiters onto the new position.
        let published = self.published();
        effects.extend(self.waiters.drain(..).map(|w| PubEffect::Answer {
            reader: w.reader,
            answer: Ok(published),
            waited: true,
        }));
        // 3a. The old-prefix view moves with the position (A-R69a, M7A-108), and the new one is
        // asked for **before** T1 hears `Published`.
        effects.extend(self.move_view());
        // 4. T1, and the deadline.
        effects.push(PubEffect::NotifyTxn {
            lineage: cand.lineage,
            seq: cand.seq,
            record_digest: cand.record_digest,
            request: cand.request,
        });
        effects.push(PubEffect::CancelTimer {
            version: pending.deadline,
        });
        // 5. The fourth revalidation; the reply state moves, it is not dropped (K-A-40).
        let correlation = self.mint_correlation();
        effects.push(PubEffect::AuthorityCheck {
            checkpoint: Checkpoint::Reply,
            lineage: cand.lineage,
            correlation,
        });
        self.awaiting_reply.insert(
            correlation,
            AwaitingReply {
                request: cand.request,
                seq: cand.seq,
                result: cand.pending_result,
                replied: pending.replied,
            },
        );
        // 7. Resolving the transaction is what unfreezes it; no other cause reopens (K-A-47).
        if self.mode
            == (PubMode::Frozen {
                cause: FreezeCause::UnresolvedTransaction,
            })
        {
            self.mode = PubMode::Serving;
        }
        tracing::debug!(
            partition = cand.lineage.partition.0,
            generation = cand.lineage.generation.0,
            seq = cand.seq.0,
            "p1.publish"
        );
        effects
    }

    // ---- deadline -----------------------------------------------------------------------------

    fn on_deadline(&mut self, now: Tick, version: TimerVersion) -> Vec<PubEffect> {
        let live = self
            .pending
            .as_ref()
            .is_some_and(|p| p.deadline == version && !p.replied);
        if !live {
            return vec![PubEffect::Fact(PubFact::StaleTimer)];
        }
        let Some(pending) = self.pending.as_mut() else {
            return Vec::new();
        };
        pending.replied = true;
        let cand = pending.cand;
        if self.mode == PubMode::Serving {
            self.mode = PubMode::Frozen {
                cause: FreezeCause::UnresolvedTransaction,
            };
        }
        let mut effects = vec![
            self.write_status(now, &cand, StatusOutcome::Unknown),
            PubEffect::Reply {
                request: cand.request,
                outcome: ReplyOutcome::Unknown,
            },
        ];
        effects.extend(self.drain_waiters());
        effects
    }

    // ---- reads --------------------------------------------------------------------------------

    /// A read arrives. Every read passes A1's gate before it is answered (lead ruling A-R72; spec
    /// §5.3, "primary reads pass the same authority gate"): it is answered only after a `Read`
    /// check **issued at or after its arrival** comes back `Admit` in the lineage served. A read
    /// the mode already refuses needs no check, and is refused here (A-R72c). A
    /// `PreviousPublished` read skips the wait for publication, never the gate (A-R72a).
    fn on_acquire(&mut self, reader: RequestIdentity, intent: ReadIntent) -> Vec<PubEffect> {
        // One identity rule for every read kind (lead ruling A-R72b, after A-R71 Q1): a read
        // under an identity whose read is still gated or waiting is refused on arrival and never
        // queued, so the first is answered once and only once.
        if self.holds(reader) {
            return vec![refuse(reader, intent, ErrorKind::RequestIdReuse)];
        }
        if self.waiters.len() + self.gated.len() >= self.config.waiter_cap {
            return vec![refuse(reader, intent, ErrorKind::Overloaded)];
        }
        if let Some(kind) = self.mode_refusal(intent) {
            return vec![refuse(reader, intent, kind)];
        }
        // Lead ruling A-R72d: with no authority view held, no answer can be ours (`fresh`), so a
        // check asked now would be dropped as stale with nothing to re-ask under, and every later
        // read would queue behind it. Such a read is one P1 cannot place: `Unavailable`.
        if self.authority.is_none() {
            return vec![refuse(reader, intent, ErrorKind::Unavailable)];
        }
        self.next_arrival += 1;
        self.gated.push_back(GatedRead {
            reader,
            intent,
            arrival: self.next_arrival,
        });
        // A check already in flight was issued before this read arrived, so it does not decide
        // this read; the next one, asked when it answers, does.
        if self.read_check.is_some() {
            return Vec::new();
        }
        vec![self.ask_read()]
    }

    /// Whether a read under `reader` is still gated or waiting at the barrier.
    fn holds(&self, reader: RequestIdentity) -> bool {
        self.gated.iter().any(|g| g.reader == reader)
            || self.waiters.iter().any(|w| w.reader == reader)
    }

    /// Ask A1 about every read gated so far, under a fresh correlation and the lineage served.
    ///
    /// Invariant (A-R72d): a view is held whenever a read check is issued, so a stale answer to
    /// it can always be re-asked and no check strands. It holds because a read is gated only
    /// with a view held (`on_acquire`), `on_view` only moves the view forward, and only a new
    /// slot (install, new boot) drops it — and a new slot holds no gated read.
    fn ask_read(&mut self) -> PubEffect {
        debug_assert!(
            self.authority.is_some(),
            "a read check is issued only with an authority view held (A-R72d)"
        );
        let correlation = self.mint_correlation();
        self.read_check = Some(ReadCheck {
            correlation,
            lineage: self.lineage,
            covers: self.next_arrival,
        });
        PubEffect::AuthorityCheck {
            checkpoint: Checkpoint::Read,
            lineage: self.lineage,
            correlation,
        }
    }

    /// The read check in flight answered, and the answer is ours. It decides exactly the reads
    /// that had arrived when it was issued; any read that arrived later is asked about again.
    fn on_read_answer(&mut self, answer: &AuthorityDecision) -> Vec<PubEffect> {
        let Some(check) = self.read_check.take() else {
            return Vec::new();
        };
        let deny = match answer.verdict {
            Verdict::Deny(reason) => Some(reason),
            Verdict::Admit if answer.lineage != check.lineage => {
                Some(DenyReason::GenerationChanged)
            }
            Verdict::Admit => None,
        };
        let mut effects = Vec::new();
        while let Some(read) = self.gated.front().copied() {
            if read.arrival > check.covers {
                break;
            }
            self.gated.pop_front();
            match deny {
                Some(reason) => effects.push(refuse(read.reader, read.intent, deny_kind(reason))),
                None => effects.extend(self.admit_read(read)),
            }
        }
        if !self.gated.is_empty() {
            effects.push(self.ask_read());
        }
        effects
    }

    /// A read A1 admitted. The mode as it stands still decides first: an admit never overrides it
    /// (A-R72c). A previous read the mode allows is answered from the kept view — never the step's
    /// storage view, which may be above the published prefix (A-R69a). A fresh read waits behind
    /// a pending candidate, or is answered as the mode now answers it.
    fn admit_read(&mut self, read: GatedRead) -> Option<PubEffect> {
        let reader = read.reader;
        match read.intent {
            ReadIntent::PreviousPublished => Some(PubEffect::Previous {
                reader,
                handle: match self.mode_refusal(read.intent) {
                    Some(kind) => Err(kind),
                    None => self.kept.ok_or(ErrorKind::Unavailable),
                },
            }),
            ReadIntent::Fresh if self.mode == PubMode::Serving && self.pending.is_some() => {
                self.waiters.push_back(Waiter { reader });
                None
            }
            ReadIntent::Fresh => Some(PubEffect::Answer {
                reader,
                answer: self.read_answer(),
                waited: false,
            }),
        }
    }

    /// Storage bound a view P1 asked for (A-R69a). It is kept only when it was asked at the
    /// position still published **and** storage bound it there; a view superseded by a later
    /// publish or a `Recovered`, or bound anywhere else, is released at once and never served.
    ///
    /// Only a view still being bound is decided here (A-R71). A completion for the kept view, for
    /// one already released, or for one never asked for is a no-op: none of them is owed a
    /// release, and a second `Release` for one handle is a double release against storage.
    fn on_snapshot_ready(&mut self, handle: SnapshotHandle, at: Seq) -> Vec<PubEffect> {
        let Some(asked) = self.opening.remove(&handle) else {
            return Vec::new();
        };
        let published = self.published();
        if asked != published || at != published.seq {
            return vec![PubEffect::ReleaseSnapshot { handle }];
        }
        // Nothing is kept here: whatever asked for this view released the old one, and it asked
        // for exactly one view at this position.
        self.kept = Some(handle);
        Vec::new()
    }

    /// The kept view moves with the published position (A-R69a; A-R71 F6 for `Recovered`): the
    /// view kept at the old position is released, and one at the current position is asked for.
    /// Until storage binds it, `PreviousPublished` answers `Unavailable`.
    fn move_view(&mut self) -> Vec<PubEffect> {
        let mut effects = Vec::with_capacity(2);
        if let Some(old) = self.kept.take() {
            effects.push(PubEffect::ReleaseSnapshot { handle: old });
        }
        effects.push(PubEffect::OpenSnapshot {
            handle: self.open_view(),
        });
        effects
    }

    /// Mint the next handle in P1's block and record it as asked for at the published position.
    /// It is kept only if storage binds it there (see [`Self::on_snapshot_ready`]).
    pub(crate) fn open_view(&mut self) -> SnapshotHandle {
        let handle = publication_snapshot(self.lineage.partition, self.next_snapshot);
        self.next_snapshot = self.next_snapshot.wrapping_add(1);
        self.opening.insert(handle, self.published());
        handle
    }

    /// What a `Fresh` reader that does not wait is answered. `Serving` and a read-only recovery
    /// serve the published position: a read-only partition refuses writes, never reads (lead
    /// ruling A-R71 F6, superseding design §4.2's `Frozen ⇒ RECOVERY_READ_ONLY` read row; T1
    /// refuses the writes). Every other mode refuses with its own code (lead ruling A-R72a Q3): a
    /// freeze for lost authority or a fenced store refuses with design §3.4's pre-apply code for
    /// its reason, because a refused read has no outcome to be unknown about. Only an unresolved
    /// transaction answers `UnknownOutcome`, which is spec §5.3's "return unknown" for a reader
    /// behind it.
    fn read_answer(&self) -> Result<PublishedAt, ErrorKind> {
        match &self.mode {
            PubMode::Serving
            | PubMode::Frozen {
                cause: FreezeCause::RecoveryReadOnly,
            } => Ok(self.published()),
            PubMode::Blocked { .. } => Err(ErrorKind::ProtectionPaused),
            PubMode::Frozen {
                cause: FreezeCause::UnresolvedTransaction,
            } => Err(ErrorKind::UnknownOutcome),
            PubMode::Frozen {
                cause: FreezeCause::AuthorityLost(reason),
            } => Err(deny_kind(*reason)),
            PubMode::Frozen {
                cause: FreezeCause::LocalStorageFenced,
            } => Err(deny_kind(DenyReason::LocalStorageFenced)),
        }
    }

    /// Whether the mode refuses a read of this kind, and with which code. Both kinds are refused
    /// alike, with [`Self::read_answer`]'s code, in a lost-authority freeze, a fenced store and a
    /// block (lead ruling A-R72c): A1's `Admit` never overrides the local mode, so an admit that
    /// races a fence serves nothing. A read-only recovery serves both. A previous read also passes
    /// an unresolved transaction: the view it asks for was published before it, and no ruling
    /// refuses it there.
    fn mode_refusal(&self, intent: ReadIntent) -> Option<ErrorKind> {
        match (intent, &self.mode) {
            (
                ReadIntent::PreviousPublished,
                PubMode::Frozen {
                    cause: FreezeCause::UnresolvedTransaction,
                },
            ) => None,
            _ => self.read_answer().err(),
        }
    }

    /// Every mode transition drains the waiters (K-A-14), answering each as a new reader would be
    /// answered in the new mode. Call after the mode write. A waiter already passed the read
    /// gate; a read still waiting for its check is not a waiter, and its check decides it.
    fn drain_waiters(&mut self) -> Vec<PubEffect> {
        let waiters: Vec<Waiter> = self.waiters.drain(..).collect();
        waiters
            .into_iter()
            .map(|w| PubEffect::Answer {
                reader: w.reader,
                answer: self.read_answer(),
                waited: true,
            })
            .collect()
    }

    // ---- A1 and R1 mode inputs ----------------------------------------------------------------

    fn withhold_awaiting(&mut self, why: Withheld) -> Vec<PubEffect> {
        let withheld = self.awaiting_reply.len();
        self.awaiting_reply.clear();
        (0..withheld)
            .map(|_| PubEffect::Fact(PubFact::ReplyWithheld { why }))
            .collect()
    }

    fn on_freeze(&mut self, cause: FreezeCause) -> Vec<PubEffect> {
        if let Some(pending) = self.pending.as_mut() {
            pending.recheck = None;
        }
        if matches!(self.mode, PubMode::Blocked { .. }) {
            let mut effects = self.withhold_awaiting(Withheld::Fence);
            effects.push(PubEffect::Fact(PubFact::FenceWhileBlocked { cause }));
            return effects;
        }
        self.mode = PubMode::Frozen { cause };
        let mut effects = self.drain_waiters();
        effects.extend(self.withhold_awaiting(Withheld::Fence));
        effects
    }

    fn on_view(&mut self, view: AuthorityView) -> Vec<PubEffect> {
        if self
            .authority
            .is_some_and(|held| view.authority_seq < held.authority_seq)
        {
            return vec![PubEffect::Fact(PubFact::StaleAuthorityView)];
        }
        self.authority = Some(view);
        Vec::new()
    }

    fn on_block(&mut self, reason: BlockReason) -> Vec<PubEffect> {
        if matches!(self.mode, PubMode::Blocked { .. }) {
            return vec![PubEffect::Fact(PubFact::AlreadyBlocked)];
        }
        self.mode = PubMode::Blocked {
            reason: reason.clone(),
        };
        let mut effects = vec![PubEffect::Fact(PubFact::Blocked { reason })];
        effects.extend(self.drain_waiters());
        effects
    }

    /// [`StatusIndex::lookup`], except that while the seed is pending no generation below the
    /// served one answers `StatusExpired` unless this boot retired it (lead ruling A-R91 Q2 as
    /// revised, reviewer R-1). The rows that would answer are not loaded yet, and
    /// `StatusExpired` tells a client a generation inside retention is past it; `Unknown` never
    /// proves nonexecution.
    fn status_for(&self, request: RequestIdentity, generation: Generation) -> StatusOutcome {
        let outcome = self.status.lookup(request, generation);
        match self.seed {
            Some(seed)
                if matches!(outcome, StatusOutcome::StatusExpired)
                    && generation < seed.serving
                    && !self.status.is_retired(generation) =>
            {
                StatusOutcome::Unknown
            }
            _ => outcome,
        }
    }

    fn on_recovered(&mut self, result: &RecoveryResult) -> Vec<PubEffect> {
        self.lineage = result.selected.root;
        self.published_seq = result.selected.cutoff_seq;
        self.status.fold_recovered(&result.retained_status_map);
        self.status.open(self.lineage.generation);
        // The predecessor is retained from this step on, loaded or not (lead ruling A-R90,
        // tester-ka-next T1): until the seed lands an absent identity there answers `Unknown`,
        // never `StatusExpired`, which would tell a client that a generation inside retention
        // is past it. A no-op if the generation was already retired.
        self.status
            .open(result.retained_status_map.predecessor_generation);
        // What this node never recorded is loaded from the durable rows once the snapshot shows
        // them (M7A-194); `fold_recovered` above has already folded what it did record.
        self.seed_blocked_at = None;
        self.seed = Some(StatusSeed {
            partition: self.lineage.partition,
            serving: self.lineage.generation,
            retained_through: result.retained_status_map.retained_through,
            map: result.retained_status_map,
        });
        let mut effects = self.withhold_awaiting(Withheld::Recovery);
        // The predecessor's view is not a prefix of the new lineage (A-R69a), so it is released
        // and one at the cutoff asked for: design §4.2 rebases `published_snapshot` here, and
        // `PreviousPublished` answers from it once admitted, in a mode that allows it (A-R71 F6,
        // A-R72a, A-R72c). A
        // view still being bound is released when its `SnapshotReady` lands, because it was
        // asked at a position no longer published.
        effects.extend(self.move_view());
        self.pending = None;
        self.mode = match &result.mode {
            PartitionMode::Active | PartitionMode::DegradedRf2 => PubMode::Serving,
            PartitionMode::ReadOnly => PubMode::Frozen {
                cause: FreezeCause::RecoveryReadOnly,
            },
            PartitionMode::Blocked { reason } => PubMode::Blocked {
                reason: reason.clone(),
            },
        };
        // Not in the design: a waiter queued behind the dropped candidate would otherwise hang.
        // `Serving` and read-only answer it at the rebased position; `Blocked` refuses it.
        effects.extend(self.drain_waiters());
        // A read check asked under the predecessor lineage decides no read in this one (A-R72).
        // The reads it covered are asked about again, under the new lineage, by a check issued
        // after every one of them arrived; its predecessor's answer then matches nothing.
        if self.read_check.take().is_some() {
            effects.push(self.ask_read());
        }
        effects
    }

    // ---- helpers ------------------------------------------------------------------------------

    fn write_status(
        &mut self,
        now: Tick,
        cand: &AppliedCandidate,
        outcome: StatusOutcome,
    ) -> PubEffect {
        // `snapshot` stays `None`: P1 never mints a handle (K-A-27), and the kernel does not see
        // the storage view that owns one. The published position is `(lineage.generation, seq)`.
        let entry = StatusEntry {
            request: cand.request,
            lineage: cand.lineage,
            seq: Some(cand.seq),
            record_digest: Some(cand.record_digest),
            outcome,
            snapshot: None,
            at: now,
        };
        self.status.record(entry);
        PubEffect::Status(Box::new(entry))
    }
}

/// The publish predicate, kernel-b §3.5 — three conjuncts, all live (A-R25, K-A-51).
fn may_publish(
    repl: Option<&dyn ReplicationView>,
    cand: &AppliedCandidate,
) -> Result<(), PredicateFalse> {
    let Some(view) = repl else {
        return Err(PredicateFalse::Qualification);
    };
    if view.lineage() != cand.lineage
        || view.config_version() != cand.config_version
        || !view.qualifies_now(cand.seq)
    {
        return Err(PredicateFalse::Qualification);
    }
    match view.digest_at(cand.seq, cand.record_digest) {
        DigestLookup::Match => Ok(()),
        other => Err(PredicateFalse::Digest(other)),
    }
}
