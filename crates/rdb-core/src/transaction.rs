//! Package T1: request admission, conditions, the atomic local batch, and retained request
//! outcomes (team kernel-a `design.md` §3).
//!
//! Two layers, so the kernel can be reached before every carrier it needs exists:
//!
//! * [`TxnKernel`] is one `(node, partition)` instance stepped over the design-shaped
//!   [`TxnEvent`] / [`TxnEffect`] vocabulary. [`Transaction::step_txn`] drives it directly.
//! * [`Transaction`]'s [`Module::step`] translates the contract events that already exist into
//!   [`TxnEvent`]s and the resulting [`TxnEffect`]s back into contract [`Effect`]s. An effect
//!   with no contract carrier yet (the `StorageDispatch` check, the applied candidate) is refused
//!   **atomically**: the instance is restored to its state before the step and the step returns
//!   [`RdbError::Unavailable`] naming the missing carrier, so nothing is dropped silently and
//!   no state moves without its effects.
//!
//! T1 never constructs a success. [`TxnRejection`] has no success variant; the only successful
//! answer T1 emits is [`TxnEffect::Replay`], which re-sends a result **P1 published** and T1
//! retained (M7A-72, M7A-84).

pub mod admission;
pub mod dedup;

use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;

use crate::contracts::authority::{
    AuthorityDecision, AuthorityEvent, AuthorityIgnoreReason, AuthorityView, Checkpoint,
    DenyReason, FenceScope, Lineage, PartitionMode, Verdict,
};
use crate::contracts::digest::Digest;
use crate::contracts::envelope::{EnvelopeHeader, ReplicationEnvelope, ENVELOPE_HEADER_LEN};
use crate::contracts::errors::{Capability, ErrorKind, RdbError};
use crate::contracts::event::{
    ClientEvent, Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module,
    ModuleName, ReplyEffect, StepCtx,
};
use crate::contracts::ids::{
    AffinityId, BatchId, BootId, ConfigVersion, CorrelationId, Generation, GrantId, LeaseId,
    NodeId, PartitionId, RequestIdentity, Seq,
};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::protection::AdmissionState;
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::storage::{
    Batch, Namespace, SnapshotRead, StorageEvent, StorageFault, StoreEffect, Write,
};
use crate::contracts::time::Tick;
use crate::contracts::trace::CapabilityState;
use crate::contracts::txn::{
    Condition, ConditionOutcome, Durability, Mutation, Outcome, TxnRequest, TxnResult,
};
use crate::contracts::version::{API_VERSION, ENVELOPE_VERSION};
use crate::replication::append::{MAX_ENVELOPE_BYTES, MAX_MUTATIONS, PROGRESS_KEY};

pub use crate::contracts::publication::{AppliedCandidate, FreezeCause};
pub use admission::{admit, deny_error, freeze_error, Boundary, DenyContext, QUEUE_CAP};
pub use dedup::{
    DedupIndex, Retained, RetainedAnswer, SeedBlocked, SeedPending, TrimMemory,
    RETENTION_CAP_ENTRIES,
};

/// Top byte of every [`BatchId`] T1 allocates (`'T'` in ASCII). Every module is offered every
/// storage completion, and R1 numbers its own batches from zero, so T1 claims a completion only
/// when its id carries this tag.
///
/// # Id layout (lead ruling A-R70)
///
/// Every batch id and every correlation id T1 allocates is:
///
/// ```text
///  63      56 55      48 47      40 39                                   0
/// +----------+----------+----------+--------------------------------------+
/// | tag 0x54 |   boot   |generation|        counter (40 bits)             |
/// +----------+----------+----------+--------------------------------------+
/// ```
///
/// * `boot` is the low byte of the `StepCtx.boot` the instance was created under, and
///   `generation` the low byte of the generation it serves.
/// * `counter` starts at 1 in each instance. When it would pass [`ID_COUNTER_MAX`], the
///   instance admits nothing more (`OVERLOADED`) rather than reuse an id.
///
/// So two instances of one `(node, partition)` share an id only if their boots and their
/// generations both agree modulo 256. A completion or answer from an older instance cannot
/// match what the current one is waiting on unless it outlived 256 recoveries or 256 reboots.
/// Before A-R70 the counter restarted at 1 in every instance, and a late completion from g7
/// completed g8's first batch (tester-t1 hunt_11 and hunt_12).
pub const BATCH_TAG: u64 = 0x54 << 56;

/// Mask selecting [`BATCH_TAG`]'s bits.
const TAG_MASK: u64 = 0xFF << 56;

/// Top byte of every [`CorrelationId`] T1 puts on a `StorageDispatch` check, for the same
/// reason as [`BATCH_TAG`], and in the same layout.
pub const CORRELATION_TAG: u64 = 0x54 << 56;

/// The largest per-instance counter an id can carry (40 bits).
pub const ID_COUNTER_MAX: u64 = (1 << 40) - 1;

/// The bits every id one instance allocates shares: the boot and generation bytes of the layout
/// on [`BATCH_TAG`]. The tag is added by the caller.
const fn id_base(boot: BootId, generation: Generation) -> u64 {
    ((boot.0 & 0xFF) << 48) | ((generation.0 & 0xFF) << 40)
}

/// Whether T1 admits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QueueMode {
    /// Admitting.
    Open,
    /// Refusing, for `cause`. `unresolved` is the applied sequence P1 has not yet published.
    Frozen {
        /// Why.
        cause: FreezeCause,
        /// The dispatched sequence still awaiting `Published`, if any (K-A-46).
        unresolved: Option<Seq>,
    },
}

/// How long the start record may wait for its `StorageDispatch` answer before it is refused and
/// owed again (M9 S0 rules 1 and 2). One hour, as in the probe that proved the trigger
/// (`s0-probe.md` E4b): no client waits on it, so the deadline only bounds a check A1 never
/// answers.
pub const START_RECORD_DEADLINE_MILLIS: u64 = 3_600_000;

/// The kernel's own start record: the empty record at seq 1 of a partition whose first
/// activation was at cutoff 0 (M9 S0 lead ruling, Gautam chose A on 2026-10-07). Without it an
/// empty partition never opens: L1 resumes only after a copy ACKs a record, and at head 0 there
/// is none. Its identity is [`RequestIdentity::START_RECORD`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartRecord {
    /// Not owed: the recovery's cutoff was not 0, or the record was sent and not refused.
    NotOwed,
    /// Owed. Sent once T1 is `Open`, nothing is in flight, `next_seq` is 1 and T1 holds a view
    /// whose `authority_seq` is above `after` (rule 1), or, after a refusal, any view other
    /// than the one that refused it and at least one renew interval after the refusal (rule 2,
    /// review F-003).
    Owed {
        /// The recovery's view, or the view a refused attempt was sent under (rule 2).
        after: u64,
    },
    /// Sent under the view with this `authority_seq`. A refusal makes it [`Self::Owed`] again.
    Sent {
        /// The view it was sent under.
        under: u64,
    },
}

/// What [`admit`] returns: the request, its digest, and the view it was admitted under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    /// The request.
    pub req: TxnRequest,
    /// [`TxnRequest::request_digest`], computed once.
    pub request_digest: Digest,
    /// The pushed view check 6 passed against; the dispatch answer is compared to it.
    pub admitted_under: AuthorityView,
    /// When it was admitted.
    pub at: Tick,
}

impl Admitted {
    /// Whether its deadline has passed at `now`: `remaining_millis` counted from admission.
    #[must_use]
    pub fn expired_at(&self, now: Tick) -> bool {
        now >= self.at.plus_millis(self.req.remaining_millis)
    }
}

/// Step 13's reservation: built, digested, not yet committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    /// The reserved position (`next_seq`, read, not advanced).
    pub seq: Seq,
    /// The envelope at that position.
    pub envelope: ReplicationEnvelope,
    /// Its canonical encoding, the history record.
    pub record: Bytes,
}

/// The one transaction past the queue (spec §5.2's last paragraph).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inflight {
    /// Pre-apply: nothing is written. Discardable on a freeze (K-A-33).
    AwaitingDispatchCheck {
        /// The request.
        admitted: Admitted,
        /// The correlation the `StorageDispatch` check went out under.
        correlation: CorrelationId,
        /// When it was asked.
        asked_at: Tick,
        /// Step 13's reservation.
        reservation: Reservation,
    },
    /// Post-apply: the batch may have landed. Never discarded while the lineage stands.
    Dispatched {
        /// The request.
        admitted: Admitted,
        /// The storage batch.
        batch: BatchId,
        /// Its sequence.
        seq: Seq,
        /// The digest it chains from.
        prev_digest: Digest,
        /// Its chained record digest.
        record_digest: Digest,
        /// Encoded record length, for `LocalApplied.bytes`.
        bytes: u64,
        /// The `Admit` that let it through.
        authority: AuthorityDecision,
        /// Whether storage has answered for it.
        completed: bool,
    },
}

/// How storage answered a T1 batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOutcome {
    /// Applied, and storage says the applied prefix now reaches `applied`.
    Ok {
        /// `StorageEvent::Committed::applied`. It must be the batch's own sequence.
        applied: Seq,
    },
    /// Failed or incomplete: the batch may have partly landed (§3.3).
    Err(StorageFault),
}

/// Every input the §3.3 table names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnEvent {
    /// A client transaction.
    Submit(TxnRequest),
    /// A1's answer to a `StorageDispatch` check.
    AuthorityAnswer(AuthorityDecision),
    /// Storage answered batch `batch`.
    BatchCompleted {
        /// The batch.
        batch: BatchId,
        /// How.
        outcome: BatchOutcome,
    },
    /// P1 published `seq` — the only resolution T1 is told about. The fields are
    /// [`KernelEvent::Published`]'s; T1 matches all four against what it dispatched.
    Published {
        /// The lineage it was published in.
        lineage: Lineage,
        /// The published sequence.
        seq: Seq,
        /// Its chained digest.
        record_digest: Digest,
        /// The request it answers.
        request: RequestIdentity,
    },
    /// A1 fenced `scope`.
    Freeze {
        /// What the fence covers.
        scope: FenceScope,
        /// Why, mapped from the fence's `DenyReason`.
        cause: FreezeCause,
    },
    /// A1 pushed a view.
    AuthorityView(AuthorityView),
    /// L1's admission edge.
    AdmissionState(AdmissionState),
    /// F1 finished a recovery.
    Recovered(Box<RecoveryResult>),
    /// Drop one generation's entries below `below`.
    DedupTrim {
        /// The generation.
        generation: Generation,
        /// The watermark.
        below: Seq,
    },
    /// Drop a whole generation.
    RetireGeneration {
        /// The generation.
        generation: Generation,
    },
}

/// A refusal. **No success variant** — "T1 never returns success" is a type (M7A-84).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnRejection {
    /// Definitive: nothing was written (checks 1–10, steps 11–14, a pre-apply freeze).
    NotAdmitted(RdbError),
    /// Not definitive: the batch may have landed (`UNKNOWN_OUTCOME`).
    Ambiguous(RdbError),
}

impl TxnRejection {
    /// The client error.
    #[must_use]
    pub const fn error(&self) -> &RdbError {
        match self {
            Self::NotAdmitted(error) | Self::Ambiguous(error) => error,
        }
    }
}

/// Every output the §3.3 table names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnEffect {
    /// Step 14: ask A1 to recheck at `checkpoint`.
    AuthorityCheck {
        /// Always `StorageDispatch` from T1.
        checkpoint: Checkpoint,
        /// The lineage T1 serves.
        lineage: Lineage,
        /// Matches the answer back.
        correlation: CorrelationId,
    },
    /// Step 15: the atomic batch — mutations, dedup record, history record, progress record.
    StorageBatch(Batch),
    /// To R1 and L1, once per sequence, in order, before shipping (B-R47).
    LocalApplied {
        /// The applied sequence.
        seq: Seq,
        /// Its encoded record length.
        bytes: u64,
        /// Its chained digest.
        record_digest: Digest,
    },
    /// To R1 and P1.
    Emit(Box<AppliedCandidate>),
    /// A refusal.
    Reply {
        /// The request answered.
        identity: RequestIdentity,
        /// Why.
        rejection: TxnRejection,
    },
    /// A retained published result, replayed verbatim to a same-digest retry.
    Replay {
        /// The request answered.
        identity: RequestIdentity,
        /// The result P1 published for it.
        result: TxnResult,
    },
    /// Handled, and deliberately did nothing.
    Ignored(KernelIgnoredReason),
}

/// Queue and dedup bounds (check 9), and the id counter's bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Requests allowed to wait behind the one in flight.
    pub queue_cap: usize,
    /// Retained entries allowed before admission answers `OVERLOADED`.
    pub dedup_cap: usize,
    /// The largest id counter an instance allocates before it answers `OVERLOADED` rather than
    /// reuse an id (lead ruling A-R73). A value above [`ID_COUNTER_MAX`] is read as
    /// [`ID_COUNTER_MAX`], because a larger counter would overwrite the generation byte.
    pub id_counter_max: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            queue_cap: QUEUE_CAP,
            dedup_cap: RETENTION_CAP_ENTRIES,
            id_counter_max: ID_COUNTER_MAX,
        }
    }
}

/// One `(node, partition)` T1 instance (design §3.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnKernel {
    lineage: Lineage,
    config_version: ConfigVersion,
    next_seq: Seq,
    prev_digest: Digest,
    queue: VecDeque<Admitted>,
    inflight: Option<Inflight>,
    dedup: DedupIndex,
    admission: Option<AdmissionState>,
    authority: Option<AuthorityView>,
    mode: QueueMode,
    /// [`id_base`] for this instance.
    id_base: u64,
    next_batch: u64,
    next_correlation: u64,
    limits: Limits,
    /// The predecessor's durable dedup records, while not yet loaded (A-R68). Admission check 7
    /// refuses while this is `Some`.
    seed: Option<SeedPending>,
    /// The `authority_seq` of the view T1 held when it last asked the outstanding
    /// `StorageDispatch` check again, or `None` if it has not asked again since the check was
    /// first asked. At most one re-ask per view held (lead ledger L-R177gf, M7A-178): see
    /// [`Self::reask`].
    reasked_under: Option<u64>,
    /// The start record's state (M9 S0).
    start: StartRecord,
    /// The whole view the start record was last sent under, `None` until it is first sent. A
    /// refusal for a reason that clears with no `authority_seq` bump, `ControlUnavailable` or
    /// `ClockSampleStale`, is followed by a view at the same `authority_seq` with a moved
    /// horizon, and only that tells T1 (review F-003).
    start_sent_under: Option<AuthorityView>,
    /// When the start record was last refused, `None` until it is. A send under such a
    /// same-seq view waits one renew interval from it (review F-003, critic C1).
    start_refused_at: Option<Tick>,
}

impl TxnKernel {
    /// The lineage T1 writes into.
    #[must_use]
    pub const fn lineage(&self) -> Lineage {
        self.lineage
    }

    /// The membership pin T1 stamps on envelopes.
    #[must_use]
    pub const fn config_version(&self) -> ConfigVersion {
        self.config_version
    }

    /// The next sequence to reserve.
    #[must_use]
    pub const fn next_seq(&self) -> Seq {
        self.next_seq
    }

    /// The digest at `next_seq - 1`.
    #[must_use]
    pub const fn prev_digest(&self) -> Digest {
        self.prev_digest
    }

    /// Open or frozen.
    #[must_use]
    pub const fn mode(&self) -> &QueueMode {
        &self.mode
    }

    /// The transaction past the queue, if any.
    #[must_use]
    pub const fn inflight(&self) -> Option<&Inflight> {
        self.inflight.as_ref()
    }

    /// Requests waiting behind it.
    #[must_use]
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    /// The identities waiting, in order.
    pub fn queued(&self) -> impl Iterator<Item = RequestIdentity> + '_ {
        self.queue.iter().map(|admitted| admitted.req.identity)
    }

    /// The dedup index.
    #[must_use]
    pub const fn dedup(&self) -> &DedupIndex {
        &self.dedup
    }

    /// The predecessor records not yet loaded, or `None` once they are (A-R68).
    #[must_use]
    pub const fn seed_pending(&self) -> Option<&SeedPending> {
        self.seed.as_ref()
    }

    /// The start record's state (M9 S0).
    #[must_use]
    pub const fn start_record(&self) -> StartRecord {
        self.start
    }

    /// Load the pending seed from `snapshot` if it now shows the retained prefix. Does nothing
    /// once the seed is loaded. A blocked attempt leaves everything as it was, and the next T1
    /// step tries again.
    fn try_seed(&mut self, snapshot: &dyn SnapshotRead) {
        let Some(pending) = self.seed else {
            return;
        };
        if self
            .dedup
            .seed(&pending, snapshot, published_result)
            .is_ok()
        {
            self.seed = None;
        }
    }

    /// L1's last edge, or `None` before the first.
    #[must_use]
    pub const fn admission(&self) -> Option<&AdmissionState> {
        self.admission.as_ref()
    }

    /// A1's last pushed view.
    #[must_use]
    pub const fn authority(&self) -> Option<&AuthorityView> {
        self.authority.as_ref()
    }

    /// The bounds check 9 applies.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        self.limits
    }

    /// The fields a mapped error names for `identity`, with `current` as the authority's
    /// generation.
    #[must_use]
    pub fn deny_context(&self, identity: RequestIdentity, current: Generation) -> DenyContext {
        DenyContext {
            partition: self.lineage.partition,
            identity,
            grant: self.authority.map_or(GrantId(0), |view| view.grant_id),
            expected: self.lineage.generation,
            current,
            paused_after: Seq(self.next_seq.0.saturating_sub(1)),
        }
    }

    /// Check 8: `None` when L1 allows, the passed-through error otherwise. Fails closed before
    /// L1's first edge.
    #[must_use]
    pub fn admission_refusal(&self, partition: PartitionId) -> Option<RdbError> {
        let paused_after = Seq(self.next_seq.0.saturating_sub(1));
        match &self.admission {
            Some(state) if state.allow => None,
            Some(state) => Some(admission::admission_error(
                state.reason,
                partition,
                state.paused_prefix,
            )),
            None => Some(admission::admission_error(
                Some(ErrorKind::ProtectionPaused),
                partition,
                paused_after,
            )),
        }
    }

    /// The result P1 publishes for `seq` in this lineage: the candidate's `pending_result`, and
    /// what T1 retains once `Published` names it.
    #[must_use]
    pub const fn pending_result(&self, seq: Seq) -> TxnResult {
        published_result(self.lineage, seq)
    }

    /// A fresh instance for `result` on `boot`, starting from `dedup`: an empty index with this
    /// node's remembered trims applied, which the seed then fills (A-R73b).
    fn at_recovery(
        result: &RecoveryResult,
        dedup: DedupIndex,
        limits: Limits,
        boot: BootId,
    ) -> Self {
        let view = result.committed.authority_view;
        let partition = result.committed.pinned_config.partition;
        // Design §3.3's `Frozen × Recovered` row: T1 has no `Blocked` mode, so a blocked
        // recovery refuses writes as `RECOVERY_READ_ONLY` at check 7 and P1 carries the
        // distinction (A-R70 F4). `DIVERGENCE_REQUIRES_OPERATOR` reaches T1 only as L1's reason
        // at check 8.
        let mode = match &result.mode {
            PartitionMode::Active | PartitionMode::DegradedRf2 => QueueMode::Open,
            PartitionMode::ReadOnly | PartitionMode::Blocked { .. } => {
                frozen(FreezeCause::RecoveryReadOnly)
            }
        };
        Self {
            lineage: Lineage {
                partition,
                generation: result.new_generation,
                owner_epoch: view.lineage.owner_epoch,
            },
            config_version: result.committed.pinned_config.config_version,
            next_seq: result.selected.cutoff_seq.next(),
            prev_digest: result.selected.cutoff_digest,
            queue: VecDeque::new(),
            inflight: None,
            dedup,
            admission: None,
            authority: Some(view),
            mode,
            id_base: id_base(boot, result.new_generation),
            next_batch: 0,
            next_correlation: 0,
            limits,
            seed: Some(SeedPending {
                partition,
                serving: result.new_generation,
                retained_through: result.retained_status_map.retained_through,
            }),
            reasked_under: None,
            // Rule 1: owed only when the partition activates at cutoff 0, and never sent at
            // `Recovered` itself, so it waits for a view newer than this recovery's. Sent at
            // `Recovered` it was refused `LEASE_EXPIRED` (`s0-probe.md` E4a).
            start: if result.selected.cutoff_seq == Seq::ZERO {
                StartRecord::Owed {
                    after: view.authority_seq,
                }
            } else {
                StartRecord::NotOwed
            },
            start_sent_under: None,
            start_refused_at: None,
        }
    }

    /// A `Recovered` for the generation this instance already serves (lead ruling A-R73a).
    ///
    /// F1 re-emits the same result as `Active` when its activation CAS lands (T-B-03), so a
    /// recovery that came up read-only is lifted **in place**: nothing is rebuilt and no id is
    /// reset, because a new instance at the same boot and generation would reissue this one's
    /// ids. Nothing else changes. An identical re-delivery, a read-only re-emit after the lift,
    /// and an instance frozen for any other cause are `NotRequired`: a `Recovered` re-emit never
    /// freezes a live generation, because pausing one is L1's job (`PROTECTION_PAUSED`).
    ///
    /// A `Blocked` recovery shares the `RecoveryReadOnly` cause, yet this never lifts one: F1
    /// emits no `Recovered{Blocked}` (`recovery::select` blocks before commit, `recovery::commit`
    /// maps the same non-empty required set, `recovery::activate` re-emits only that commit's
    /// result) (tester-t1 g3_lift_04).
    fn on_reemitted(&mut self, ctx: &StepCtx<'_>, result: &RecoveryResult) -> Vec<TxnEffect> {
        let activates = matches!(
            result.mode,
            PartitionMode::Active | PartitionMode::DegradedRf2
        );
        let read_only = matches!(
            self.mode,
            QueueMode::Frozen {
                cause: FreezeCause::RecoveryReadOnly,
                ..
            }
        );
        if !(activates && read_only) {
            return vec![ignored(ReplicaIgnoreReason::NotRequired)];
        }
        self.mode = QueueMode::Open;
        self.pump(ctx)
    }

    /// One §3.3 row, then the start record if it is now due (M9 S0). `Recovered` is handled by
    /// [`Transaction`], which owns instance lifetime.
    fn step(&mut self, ctx: &StepCtx<'_>, event: TxnEvent) -> Vec<TxnEffect> {
        // A client's own `Submit` under the reserved identity is refused at check 10 and
        // answered like any refusal (rule 4). It never enters the queue, so a `Submit` row
        // carries no refusal of the kernel's record, and every other row's does.
        let submit = matches!(event, TxnEvent::Submit(_));
        let row = self.row(ctx, event);
        let mut out = if submit {
            row
        } else {
            self.quiet(ctx.now, row)
        };
        let started = self.start_if_due(ctx);
        out.extend(self.quiet(ctx.now, started));
        out
    }

    /// The kernel's own start record answered to nobody (rule 5), and owed again when refused
    /// (rule 2). A refusal at any boundary owes it under any view other than the one it was sent
    /// under: a newer one, or one A1 republished at the same `authority_seq` because the horizon
    /// moved (review F-003). So a refusal never leaves the partition stuck, and it is never sent
    /// again under the view that refused it. A refusal at `now` is remembered, which spaces a
    /// same-seq re-send (see [`Self::start_if_due`]). Each refusal is recorded as ignored, with
    /// the error kind it would have carried, and logged.
    fn quiet(&mut self, now: Tick, effects: Vec<TxnEffect>) -> Vec<TxnEffect> {
        effects
            .into_iter()
            .map(|effect| {
                let TxnEffect::Reply {
                    identity,
                    rejection,
                } = &effect
                else {
                    return effect;
                };
                if *identity != RequestIdentity::START_RECORD {
                    return effect;
                }
                if let StartRecord::Sent { under } = self.start {
                    self.start = StartRecord::Owed { after: under };
                    self.start_refused_at = Some(now);
                }
                let error = rejection.error();
                tracing::info!(
                    partition = self.lineage.partition.0,
                    generation = self.lineage.generation.0,
                    error = ?error,
                    "t1.start_record_refused"
                );
                ignored(error.kind())
            })
            .collect()
    }

    /// Rule 1: send the start record when every condition holds at once.
    ///
    /// The view conjunct has two arms. Rule 1's: a view newer than `after`, so the first send
    /// waits for a view newer than the recovery's. Rule 2's, once a send was refused: a view
    /// other than the one it was sent under, at least one renew interval after the refusal
    /// (review F-003). A1 republishes at the same `authority_seq` whenever the admission horizon
    /// moves (a committed renewal, an adopted record, a moved sample; A-R54.1), which is what
    /// clears `ControlUnavailable` and `ClockSampleStale`. A moved sample is every A1 step at a
    /// new millisecond, so the view alone would re-ask a deny that persists about once per
    /// millisecond (critic C1). The spacing bounds that to one send per renew interval, and the
    /// first changed view after it still sends at once.
    fn start_if_due(&mut self, ctx: &StepCtx<'_>) -> Vec<TxnEffect> {
        let StartRecord::Owed { after } = self.start else {
            return Vec::new();
        };
        let Some(view) = self.authority else {
            return Vec::new();
        };
        let spaced = self
            .start_refused_at
            .is_some_and(|at| ctx.now >= at.plus_millis(ctx.budgets.renew_millis));
        let moved = view.authority_seq > after
            || (spaced && self.start_sent_under.is_some_and(|refused| refused != view));
        if self.mode != QueueMode::Open
            || self.inflight.is_some()
            || self.next_seq != Seq::ZERO.next()
            || !moved
        {
            return Vec::new();
        }
        self.start = StartRecord::Sent {
            under: view.authority_seq,
        };
        self.start_sent_under = Some(view);
        let req = TxnRequest {
            api_version: API_VERSION,
            identity: RequestIdentity::START_RECORD,
            affinity: AffinityId(0),
            expected_generation: None,
            remaining_millis: START_RECORD_DEADLINE_MILLIS,
            conditions: Vec::new(),
            mutations: Vec::new(),
        };
        tracing::info!(
            partition = self.lineage.partition.0,
            generation = self.lineage.generation.0,
            authority_seq = view.authority_seq,
            "t1.start_record_sent"
        );
        // Not through `admit`: check 10 refuses this identity to every client, and check 8
        // refuses while L1 is paused, which is the state this record exists to end.
        self.queue.push_front(Admitted {
            request_digest: req.request_digest(),
            req,
            admitted_under: view,
            at: ctx.now,
        });
        self.pump(ctx)
    }

    fn row(&mut self, ctx: &StepCtx<'_>, event: TxnEvent) -> Vec<TxnEffect> {
        match event {
            TxnEvent::Submit(req) => self.on_submit(ctx, &req),
            TxnEvent::AuthorityAnswer(answer) => self.on_answer(ctx, &answer),
            TxnEvent::BatchCompleted { batch, outcome } => self.on_completed(batch, outcome),
            TxnEvent::Published {
                lineage,
                seq,
                record_digest,
                request,
            } => self.on_published(ctx, lineage, seq, record_digest, request),
            TxnEvent::Freeze { scope, cause } => self.on_freeze(scope, cause),
            TxnEvent::AuthorityView(view) => self.on_view(view),
            TxnEvent::AdmissionState(state) => {
                self.admission = Some(state);
                Vec::new()
            }
            TxnEvent::DedupTrim { generation, below } => {
                self.dedup.trim(generation, below);
                Vec::new()
            }
            TxnEvent::RetireGeneration { generation } => {
                // Only a generation behind the served one retires. Retiring the served one would
                // drop every identity it retained and let each execute again (A-R70, hunt_07).
                // Retiring a newer one would make it retain nothing if this node later serves it
                // (A-R71, R9). Both retire nothing.
                match generation.cmp(&self.lineage.generation) {
                    Ordering::Greater => {
                        vec![ignored(AuthorityIgnoreReason::RetireNewerGeneration)]
                    }
                    Ordering::Equal => vec![ignored(AuthorityIgnoreReason::RetireServedGeneration)],
                    Ordering::Less => {
                        self.dedup.retire(generation);
                        Vec::new()
                    }
                }
            }
            TxnEvent::Recovered(_) => vec![ignored(ReplicaIgnoreReason::NotRequired)],
        }
    }

    fn on_submit(&mut self, ctx: &StepCtx<'_>, req: &TxnRequest) -> Vec<TxnEffect> {
        match admit(req, self.lineage.partition, Some(self), ctx.now) {
            Err(rejection) => vec![reply(req.identity, rejection)],
            Ok(admitted) => {
                self.queue.push_back(admitted);
                if self.inflight.is_none() {
                    self.pump(ctx)
                } else {
                    Vec::new()
                }
            }
        }
    }

    /// Start queued requests until one reaches the `StorageDispatch` check or the queue is
    /// empty. Steps 11–12 answer without a check, so several may finish in one pump.
    fn pump(&mut self, ctx: &StepCtx<'_>) -> Vec<TxnEffect> {
        let mut out = Vec::new();
        while self.inflight.is_none() && self.mode == QueueMode::Open {
            let Some(admitted) = self.queue.pop_front() else {
                break;
            };
            out.extend(self.head(ctx, admitted));
        }
        out
    }

    /// Steps 11–14 for the request at the head of the queue.
    fn head(&mut self, ctx: &StepCtx<'_>, admitted: Admitted) -> Vec<TxnEffect> {
        let identity = admitted.req.identity;
        let affinity = admitted.req.affinity;
        let generation = self.lineage.generation;
        // 11. Dedup, before evaluation (ADR-0025): a retry sees the original answer.
        if let Some(retained) = self.dedup.get(generation, affinity, identity) {
            return vec![replay(identity, admitted.request_digest, retained)];
        }
        if let Some((older, retained)) = self.dedup.older(generation, affinity, identity) {
            // Generation reconciliation (spec §8.1): never re-execute a request an older
            // generation retained, and never replay it transparently either.
            let error = if retained.request_digest == admitted.request_digest {
                RdbError::GenerationChanged {
                    expected: older,
                    current: generation,
                }
            } else {
                RdbError::RequestIdReuse { identity }
            };
            return vec![reply(identity, TxnRejection::NotAdmitted(error))];
        }
        // 12. Conditions, against the authoritative local state. No sequence is reserved, so
        //     the failure is aged at `next_seq`, the first sequence published after it: aged at
        //     the one before, a trim of that older sequence dropped it early (A-R71, hunt_06).
        if let Some(index) = first_failed_condition(&admitted.req, ctx.snapshot) {
            self.dedup.insert(
                generation,
                affinity,
                identity,
                Retained {
                    request_digest: admitted.request_digest,
                    answer: RetainedAnswer::ConditionFailed { index },
                    applied_at_seq: self.next_seq,
                },
            );
            return vec![reply(
                identity,
                TxnRejection::NotAdmitted(RdbError::ConditionFailed { index }),
            )];
        }
        // 13. Reserve: read `next_seq`, do not advance it.
        let reservation = match self.reserve(&admitted) {
            Ok(reservation) => reservation,
            Err(error) => return vec![reply(identity, TxnRejection::NotAdmitted(error))],
        };
        // 14. The `StorageDispatch` recheck.
        let Some(correlation) = self.mint_correlation() else {
            let error = RdbError::Overloaded {
                partition: self.lineage.partition,
            };
            return vec![reply(identity, TxnRejection::NotAdmitted(error))];
        };
        self.inflight = Some(Inflight::AwaitingDispatchCheck {
            admitted,
            correlation,
            asked_at: ctx.now,
            reservation,
        });
        self.reasked_under = None;
        vec![TxnEffect::AuthorityCheck {
            checkpoint: Checkpoint::StorageDispatch,
            lineage: self.lineage,
            correlation,
        }]
    }

    /// A fresh correlation in the layout on [`BATCH_TAG`], or `None` once this instance's
    /// counter is spent ([`Limits::id_counter_max`]). Never an id this instance has already used.
    fn mint_correlation(&mut self) -> Option<CorrelationId> {
        let bound = self.limits.id_counter_max.min(ID_COUNTER_MAX);
        let next = self
            .next_correlation
            .checked_add(1)
            .filter(|next| *next <= bound)?;
        self.next_correlation = next;
        Some(CorrelationId(CORRELATION_TAG | self.id_base | next))
    }

    /// The envelope for `req` at `next_seq`, chained to `prev_digest`, with its `record_digest`
    /// still [`Digest::ROOT`]. Every field but the request's own is fixed-width, so its encoded
    /// length is a function of the request alone: [`encoded_len`].
    fn envelope(
        &self,
        req: &TxnRequest,
        request_digest: Digest,
        grant: GrantId,
    ) -> ReplicationEnvelope {
        let seq = self.next_seq;
        let mut mutations: Vec<Write> = req
            .mutations
            .iter()
            .map(|mutation| match mutation {
                Mutation::Put { key, value, .. } => Write {
                    ns: Namespace::User,
                    key: key.clone(),
                    value: Some(value.clone()),
                },
                Mutation::Delete { key, .. } => Write {
                    ns: Namespace::User,
                    key: key.clone(),
                    value: None,
                },
            })
            .collect();
        // The start record has no dedup row: no client can retry it (M9 S0 rule 3).
        if req.identity != RequestIdentity::START_RECORD {
            mutations.push(Write {
                ns: Namespace::Dedup,
                key: dedup::dedup_key(self.lineage.generation, req.affinity, req.identity),
                value: Some(dedup::dedup_value(
                    request_digest,
                    seq,
                    self.lineage.owner_epoch,
                )),
            });
        }
        ReplicationEnvelope {
            header: EnvelopeHeader {
                protocol_version: ENVELOPE_VERSION,
                partition: self.lineage.partition,
                generation: self.lineage.generation,
                config_version: self.config_version,
                owner_epoch: self.lineage.owner_epoch,
                seq,
                body_len: 0,
            },
            lease_id: LeaseId(grant.0),
            prev_digest: self.prev_digest,
            request_identity: req.identity,
            request_digest,
            conditions_result: vec![ConditionOutcome::Met; req.conditions.len()],
            mutations,
            result: Outcome::Published,
            record_digest: Digest::ROOT,
        }
    }

    /// Step 13: the envelope at `next_seq`, chained to `prev_digest`.
    ///
    /// Check 10 has already refused a record over [`MAX_ENVELOPE_BYTES`] and more writes than
    /// [`admission::MAX_REQUEST_MUTATIONS`] (spec §4.2, rulings L-R184y and L-R185b), so this
    /// record passes every secondary's append row 2. Both bounds are checked again here on
    /// the record itself, each refused with check 10's error (ruling L-R185d).
    fn reserve(&self, admitted: &Admitted) -> Result<Reservation, RdbError> {
        let mut envelope = self.envelope(
            &admitted.req,
            admitted.request_digest,
            admitted.admitted_under.grant_id,
        );
        // Check 10's two bounds, in its order, refused again on the record itself in every
        // build: either one, shipped, stalls the partition on a record no secondary takes
        // (rulings L-R184y, L-R185d). The count first, before any hashing.
        if envelope.mutations.len() > MAX_MUTATIONS {
            return Err(RdbError::InvalidArgument { field: "mutations" });
        }
        envelope.record_digest = envelope.compute_record_digest()?;
        let record = envelope.encode()?;
        // The bytes, likewise: check 10 measured them with `encoded_len`; this is the record.
        if record.len() > MAX_ENVELOPE_BYTES {
            return Err(RdbError::InvalidArgument {
                field: "envelope_bytes",
            });
        }
        envelope.header.body_len = u32::try_from(record.len() - ENVELOPE_HEADER_LEN)
            .map_err(|_| RdbError::InvalidArgument { field: "body_len" })?;
        Ok(Reservation {
            seq: envelope.header.seq,
            envelope,
            record,
        })
    }

    /// `answer_is_ours`: the check we are waiting on, answered no older than the view we hold.
    fn answer_is_ours(&self, answer: &AuthorityDecision) -> bool {
        let Some(Inflight::AwaitingDispatchCheck { correlation, .. }) = &self.inflight else {
            return false;
        };
        answer.correlation == *correlation
            && answer.checkpoint == Checkpoint::StorageDispatch
            && self
                .authority
                .is_some_and(|view| answer.authority_seq >= view.authority_seq)
    }

    /// Lead ruling A-R69, applied to T1 by A-R70 (tester-t1 hunt_22). A stale answer is never
    /// acted on. But when it answers the check still outstanding, that check is asked again
    /// against the view T1 now holds, under a fresh correlation. Otherwise nothing would answer
    /// it: the view moved on while the check was in flight, the answer lands older than that
    /// view, and the queue waits behind it for good.
    ///
    /// **At most once per view T1 holds** (lead ledger L-R177gf, M7A-178). A re-ask is asked
    /// "against the view T1 now holds"; a second stale answer while that view has not moved
    /// means A1 answered the same question from the same stale place, and asking it a third
    /// time cannot get a different answer. Before this bound an A1 that could not move (`Unheld`,
    /// answering at `authority_seq` 0) and T1 asked each other forever at one tick — 29,506
    /// checks in one simulated run — and the deadline never fired because the tick never
    /// advanced. A view that moves between the two stale answers allows one more re-ask.
    ///
    /// When the check cannot be asked again, the request is refused instead and the queue
    /// behind it moves on, so it is never left waiting (lead ruling A-R73). Past its deadline it
    /// is `DEADLINE_BEFORE_ADMISSION` (spec §5.2 step 3, as at dispatch). Asked again already
    /// under this view it is `LEASE_EXPIRED`, the code a pre-apply authority deny maps to: A1
    /// could not confirm the grant for it. With the id counter spent it is `OVERLOADED`. All
    /// three are truthful: nothing was dispatched.
    ///
    /// Empty when the answer is for no outstanding check.
    fn reask(&mut self, ctx: &StepCtx<'_>, answer: &AuthorityDecision) -> Vec<TxnEffect> {
        let Some(Inflight::AwaitingDispatchCheck {
            correlation,
            admitted,
            ..
        }) = &self.inflight
        else {
            return Vec::new();
        };
        // An instance always holds a view (it is created with the recovery's), so unlike P1
        // there is no "no view held" case to exclude.
        if *correlation != answer.correlation || answer.checkpoint != Checkpoint::StorageDispatch {
            return Vec::new();
        }
        let expired = admitted.expired_at(ctx.now);
        let held = self.authority.map(|view| view.authority_seq);
        let exhausted = self.reasked_under.is_some() && self.reasked_under == held;
        let fresh = if expired || exhausted {
            None
        } else {
            self.mint_correlation()
        };
        if let Some(fresh) = fresh {
            if let Some(Inflight::AwaitingDispatchCheck { correlation, .. }) = &mut self.inflight {
                *correlation = fresh;
            }
            self.reasked_under = held;
            return vec![TxnEffect::AuthorityCheck {
                checkpoint: Checkpoint::StorageDispatch,
                lineage: self.lineage,
                correlation: fresh,
            }];
        }
        let Some(Inflight::AwaitingDispatchCheck { admitted, .. }) = self.inflight.take() else {
            return Vec::new();
        };
        let partition = self.lineage.partition;
        let error = if expired {
            RdbError::DeadlineBeforeAdmission { partition }
        } else if exhausted {
            RdbError::LeaseExpired {
                partition,
                grant: self
                    .deny_context(admitted.req.identity, self.lineage.generation)
                    .grant,
            }
        } else {
            RdbError::Overloaded { partition }
        };
        let refused = reply(admitted.req.identity, TxnRejection::NotAdmitted(error));
        // The queue behind the refused request is still owed its turn.
        let next = self.pump(ctx);
        std::iter::once(refused).chain(next).collect()
    }

    fn on_answer(&mut self, ctx: &StepCtx<'_>, answer: &AuthorityDecision) -> Vec<TxnEffect> {
        if !self.answer_is_ours(answer) {
            let mut out = vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)];
            out.extend(self.reask(ctx, answer));
            return out;
        }
        let Some(Inflight::AwaitingDispatchCheck {
            admitted,
            reservation,
            ..
        }) = self.inflight.take()
        else {
            return vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)];
        };
        let identity = admitted.req.identity;
        let deny = match answer.verdict {
            Verdict::Deny(reason) => Some(reason),
            Verdict::Admit if answer.lineage != admitted.admitted_under.lineage => {
                Some(DenyReason::GenerationChanged)
            }
            Verdict::Admit if !answer.same_lineage_as_view(&admitted.admitted_under) => {
                Some(DenyReason::AuthorityGenerationChanged)
            }
            Verdict::Admit => None,
        };
        let mut out = Vec::new();
        if let Some(reason) = deny {
            // Discard the reservation: `next_seq` and `prev_digest` never moved.
            let at = self.deny_context(identity, answer.lineage.generation);
            out.push(reply(
                identity,
                TxnRejection::NotAdmitted(deny_error(reason, Boundary::PreApply, &at)),
            ));
        } else if let QueueMode::Frozen { cause, .. } = self.mode {
            let at = self.deny_context(identity, self.lineage.generation);
            out.push(reply(
                identity,
                TxnRejection::NotAdmitted(freeze_error(cause, &at)),
            ));
            out.push(ignored(AuthorityIgnoreReason::DispatchRefusedFrozen));
        } else if admitted.expired_at(ctx.now) {
            // Spec §5.2 step 3: cancel expired work at storage dispatch (A-R71, hunt_19). Nothing
            // is written and the reservation is discarded, so it is a definitive non-admission.
            out.push(reply(
                identity,
                TxnRejection::NotAdmitted(RdbError::DeadlineBeforeAdmission {
                    partition: self.lineage.partition,
                }),
            ));
        } else {
            // 15. Commit the reservation. At most one batch per correlation, so the batch
            //     counter never passes the correlation counter's bound.
            self.next_batch += 1;
            let batch = BatchId(BATCH_TAG | self.id_base | self.next_batch);
            let seq = reservation.seq;
            let record_digest = reservation.envelope.record_digest;
            let mut progress = seq.0.to_le_bytes().to_vec();
            progress.extend_from_slice(&record_digest.0);
            let mut writes = reservation.envelope.mutations.clone();
            writes.push(Write {
                ns: Namespace::History,
                key: Bytes::copy_from_slice(&seq.0.to_be_bytes()),
                value: Some(reservation.record.clone()),
            });
            writes.push(Write {
                ns: Namespace::Progress,
                key: Bytes::from_static(PROGRESS_KEY),
                value: Some(Bytes::from(progress)),
            });
            let prev_digest = self.prev_digest;
            self.next_seq = seq.next();
            self.prev_digest = record_digest;
            self.inflight = Some(Inflight::Dispatched {
                admitted,
                batch,
                seq,
                prev_digest,
                record_digest,
                bytes: u64::try_from(reservation.record.len()).unwrap_or(u64::MAX),
                authority: *answer,
                completed: false,
            });
            return vec![TxnEffect::StorageBatch(Batch {
                id: batch,
                partition: self.lineage.partition,
                generation: self.lineage.generation,
                seq,
                writes,
            })];
        }
        out.extend(self.pump(ctx));
        out
    }

    fn on_completed(&mut self, batch: BatchId, outcome: BatchOutcome) -> Vec<TxnEffect> {
        let Some(Inflight::Dispatched {
            admitted,
            batch: ours,
            seq,
            prev_digest,
            record_digest,
            bytes,
            authority,
            completed,
        }) = &mut self.inflight
        else {
            return vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)];
        };
        if *ours != batch || *completed {
            return vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)];
        }
        *completed = true;
        let seq = *seq;
        let (cause, mut out) = match outcome {
            BatchOutcome::Ok { applied } if applied == seq => {
                let candidate = AppliedCandidate {
                    lineage: self.lineage,
                    config_version: self.config_version,
                    seq,
                    prev_digest: *prev_digest,
                    record_digest: *record_digest,
                    request: admitted.req.identity,
                    request_digest: admitted.request_digest,
                    pending_result: published_result(self.lineage, seq),
                    authority: *authority,
                };
                (
                    FreezeCause::UnresolvedTransaction,
                    vec![
                        TxnEffect::LocalApplied {
                            seq,
                            bytes: *bytes,
                            record_digest: *record_digest,
                        },
                        TxnEffect::Emit(Box::new(candidate)),
                    ],
                )
            }
            // A failed batch, or one storage reports at a position other than its own: storage
            // and the envelope disagree about where the record sits, so no candidate is built
            // (A-R71, hunt_16).
            BatchOutcome::Ok { .. } | BatchOutcome::Err(_) => (
                FreezeCause::LocalStorageFenced,
                vec![reply(
                    admitted.req.identity,
                    TxnRejection::Ambiguous(RdbError::UnknownOutcome {
                        partition: self.lineage.partition,
                        identity: admitted.req.identity,
                    }),
                )],
            ),
        };
        // The cause is written only from `Open` (K-A-46); `unresolved` always.
        self.mode = match self.mode {
            QueueMode::Open => QueueMode::Frozen {
                cause,
                unresolved: Some(seq),
            },
            QueueMode::Frozen { cause, .. } => QueueMode::Frozen {
                cause,
                unresolved: Some(seq),
            },
        };
        if cause == FreezeCause::LocalStorageFenced {
            // A storage failure fences the partition (spec §5.2 step 3), and nothing reopens it
            // in this generation, so the queue is answered now, as a fence answers it (A-R71,
            // hunt_20). Nothing queued was written.
            out.extend(self.drain(|k, identity| {
                freeze_error(
                    FreezeCause::LocalStorageFenced,
                    &k.deny_context(identity, k.lineage.generation),
                )
            }));
        }
        out
    }

    fn on_published(
        &mut self,
        ctx: &StepCtx<'_>,
        lineage: Lineage,
        seq: Seq,
        record_digest: Digest,
        request: RequestIdentity,
    ) -> Vec<TxnEffect> {
        let QueueMode::Frozen {
            cause,
            unresolved: Some(unresolved),
        } = self.mode
        else {
            return vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)];
        };
        let Some(Inflight::Dispatched {
            admitted,
            record_digest: dispatched,
            ..
        }) = &self.inflight
        else {
            return vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)];
        };
        if unresolved != seq
            || lineage != self.lineage
            || record_digest != *dispatched
            || request != admitted.req.identity
        {
            return vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)];
        }
        // `RetainDedup`: a state write here, because no contract carries it and nothing outside
        // T1 consumes it. Read it back through `TxnKernel::dedup`. The retained result is the
        // candidate's `pending_result`, which is the result P1 published.
        // The start record retains nothing, as it wrote no dedup row (M9 S0 rule 3).
        if admitted.req.identity != RequestIdentity::START_RECORD {
            self.dedup.insert(
                self.lineage.generation,
                admitted.req.affinity,
                admitted.req.identity,
                Retained {
                    request_digest: admitted.request_digest,
                    answer: RetainedAnswer::Applied(self.pending_result(seq)),
                    applied_at_seq: seq,
                },
            );
        }
        self.inflight = None;
        if cause == FreezeCause::UnresolvedTransaction {
            self.mode = QueueMode::Open;
            self.pump(ctx)
        } else {
            // The transaction resolved; the authority did not come back. Only `Recovered`
            // reopens.
            self.mode = frozen(cause);
            vec![ignored(AuthorityIgnoreReason::PublishedWhileFrozen)]
        }
    }

    fn on_freeze(&mut self, scope: FenceScope, cause: FreezeCause) -> Vec<TxnEffect> {
        let covers = match scope {
            FenceScope::Node => true,
            FenceScope::Partition(partition) => partition == self.lineage.partition,
        };
        if !covers {
            return vec![ignored(AuthorityIgnoreReason::NotOurs)];
        }
        let mut out = Vec::new();
        let unresolved = match self.inflight.take() {
            Some(Inflight::AwaitingDispatchCheck { admitted, .. }) => {
                // Pre-apply, nothing written: a definitive non-admission (K-A-33).
                let at = self.deny_context(admitted.req.identity, self.lineage.generation);
                out.push(reply(
                    admitted.req.identity,
                    TxnRejection::NotAdmitted(freeze_error(cause, &at)),
                ));
                out.push(ignored(AuthorityIgnoreReason::DispatchDroppedByFreeze));
                None
            }
            Some(dispatched @ Inflight::Dispatched { seq, .. }) => {
                // Post-apply: kept, so `Published{seq}` can still match (K-A-46).
                self.inflight = Some(dispatched);
                Some(seq)
            }
            None => None,
        };
        out.extend(self.drain(|k, identity| {
            freeze_error(cause, &k.deny_context(identity, k.lineage.generation))
        }));
        self.mode = QueueMode::Frozen { cause, unresolved };
        out
    }

    fn on_view(&mut self, view: AuthorityView) -> Vec<TxnEffect> {
        // Another partition's view is not this kernel's authority (lead ruling A-R84), exactly as
        // another partition's fence is not its freeze.
        if view.lineage.partition != self.lineage.partition {
            return vec![ignored(AuthorityIgnoreReason::NotOurs)];
        }
        if self
            .authority
            .is_some_and(|held| view.authority_seq < held.authority_seq)
        {
            return vec![ignored(AuthorityIgnoreReason::StaleAuthorityView)];
        }
        self.authority = Some(view);
        Vec::new()
    }

    /// Answer every queued request with `error`, in queue order. Nothing queued was written.
    fn drain(&mut self, error: impl Fn(&Self, RequestIdentity) -> RdbError) -> Vec<TxnEffect> {
        let queued = std::mem::take(&mut self.queue);
        queued
            .into_iter()
            .map(|admitted| {
                let identity = admitted.req.identity;
                reply(identity, TxnRejection::NotAdmitted(error(self, identity)))
            })
            .collect()
    }

    /// Everything waiting, answered with `error`, and the pre-apply inflight with it: what a
    /// recovery or a demotion owes the requests it strands. A `Dispatched` inflight gets no
    /// reply from T1 — P1 holds its outcome.
    fn strand(
        &mut self,
        now: Tick,
        error: impl Fn(&Self, RequestIdentity) -> RdbError,
    ) -> Vec<TxnEffect> {
        let mut out = Vec::new();
        if let Some(Inflight::AwaitingDispatchCheck { admitted, .. }) = &self.inflight {
            let identity = admitted.req.identity;
            out.push(reply(
                identity,
                TxnRejection::NotAdmitted(error(self, identity)),
            ));
        }
        self.inflight = None;
        out.extend(self.drain(error));
        // The stranded check may be the kernel's own start record (M9 S0 rule 5).
        self.quiet(now, out)
    }
}

/// The result P1 publishes for `seq` in `lineage`, at the durability publication requires.
const fn published_result(lineage: Lineage, seq: Seq) -> TxnResult {
    TxnResult {
        partition: lineage.partition,
        owner_epoch: lineage.owner_epoch,
        generation: lineage.generation,
        seq,
        outcome: Outcome::Published,
        durability: Durability::BufferedOnTwo,
    }
}

/// Check 10's byte bound, computed without building the record: the length
/// [`ReplicationEnvelope::encode`] gives the envelope [`TxnKernel::envelope`] builds for `req`,
/// which is the record step 13 ships and every secondary's append row 2 measures. The layout is
/// the one in [`crate::contracts::envelope`]'s module docs; the test
/// `encoded_len_is_the_encoded_records_length` pins this against `encode` (ruling L-R185d).
///
/// Saturating, so an absurd request still measures over the cap rather than wrapping under it.
fn encoded_len(req: &TxnRequest) -> usize {
    record_len(req.conditions.len(), &req.mutations)
}

/// [`encoded_len`] for a request's conditions count and mutations, the only parts of a request
/// its record length depends on. Public so a compiler can refuse what check 10 would refuse
/// against [`MAX_ENVELOPE_BYTES`] without copying this layout (L-R186v).
#[must_use]
pub fn record_len(conditions: usize, mutations: &[Mutation]) -> usize {
    // Header; then lease id, prev digest, identity, request digest, the conditions and mutations
    // count prefixes, the result tag and the record digest.
    const FIXED: usize = ENVELOPE_HEADER_LEN + 8 + 32 + 16 + 32 + 4 + 4 + 1 + 32;
    // Per write: namespace tag, key length and has-value tag. A put adds a value length.
    const WRITE: usize = 1 + 4 + 1;
    const VALUE: usize = 4;
    // Step 13's one `Dedup` write, fixed-width.
    const DEDUP: usize = WRITE + dedup::DEDUP_KEY_LEN + VALUE + dedup::DEDUP_VALUE_LEN;
    // One outcome byte per condition.
    let fixed = (FIXED + DEDUP).saturating_add(conditions);
    mutations.iter().fold(fixed, |len, mutation| {
        let write = match mutation {
            Mutation::Put { key, value, .. } => (WRITE + VALUE)
                .saturating_add(key.len())
                .saturating_add(value.len()),
            Mutation::Delete { key, .. } => WRITE.saturating_add(key.len()),
        };
        len.saturating_add(write)
    })
}

/// `Frozen{cause}` with nothing unresolved.
const fn frozen(cause: FreezeCause) -> QueueMode {
    QueueMode::Frozen {
        cause,
        unresolved: None,
    }
}

fn reply(identity: RequestIdentity, rejection: TxnRejection) -> TxnEffect {
    TxnEffect::Reply {
        identity,
        rejection,
    }
}

fn ignored(reason: impl Into<Ignore>) -> TxnEffect {
    TxnEffect::Ignored(reason.into().0)
}

/// Either ignore family T1 uses, so [`ignored`] takes both.
struct Ignore(KernelIgnoredReason);

impl From<AuthorityIgnoreReason> for Ignore {
    fn from(reason: AuthorityIgnoreReason) -> Self {
        Self(KernelIgnoredReason::Authority(reason))
    }
}

impl From<ReplicaIgnoreReason> for Ignore {
    fn from(reason: ReplicaIgnoreReason) -> Self {
        Self(KernelIgnoredReason::Replica(reason))
    }
}

impl From<ErrorKind> for Ignore {
    fn from(kind: ErrorKind) -> Self {
        Self(KernelIgnoredReason::Error(kind))
    }
}

/// Step 11's hit: the retained answer, verbatim, or `REQUEST_ID_REUSE` for another payload.
fn replay(identity: RequestIdentity, digest: Digest, retained: &Retained) -> TxnEffect {
    if retained.request_digest != digest {
        return reply(
            identity,
            TxnRejection::NotAdmitted(RdbError::RequestIdReuse { identity }),
        );
    }
    match &retained.answer {
        RetainedAnswer::Applied(result) => TxnEffect::Replay {
            identity,
            result: *result,
        },
        RetainedAnswer::ConditionFailed { index } => reply(
            identity,
            TxnRejection::NotAdmitted(RdbError::ConditionFailed { index: *index }),
        ),
    }
}

/// Step 12: the zero-based index of the first condition that does not hold, or `None`.
///
/// Conditions are numbered first, then each mutation's `expected_version` continues the count
/// (`conditions.len() + i`), because `CONDITION_FAILED` carries one index and spec §5.1 makes a
/// mutation's expected version a condition like any other.
fn first_failed_condition(req: &TxnRequest, snapshot: &dyn SnapshotRead) -> Option<u32> {
    let version = |key: &Bytes| snapshot.version(Namespace::User, key);
    let conditions = req.conditions.iter().map(|condition| match condition {
        Condition::VersionEquals { key, version: want } => version(key) == Some(*want),
        Condition::Absent { key } => version(key).is_none(),
        Condition::Present { key } => version(key).is_some(),
    });
    let mutations = req.mutations.iter().map(|mutation| match mutation {
        Mutation::Put {
            key,
            expected_version: Some(want),
            ..
        }
        | Mutation::Delete {
            key,
            expected_version: Some(want),
        } => version(key) == Some(*want),
        Mutation::Put { .. } | Mutation::Delete { .. } => true,
    });
    conditions
        .chain(mutations)
        .position(|held| !held)
        .map(|index| u32::try_from(index).unwrap_or(u32::MAX))
}

/// The T1 module: one [`TxnKernel`] per `(node, partition)` this node is primary for.
///
/// An instance is created by a `Recovered` whose pinned configuration names this node primary,
/// and removed by one naming another node (the L1/R1 instance model). Without an instance, a
/// `Submit` is answered `NOT_PRIMARY` after checks 1–3, and every other T1 input is
/// `Ignored{Error(NotPrimary)}`.
#[derive(Debug, Default)]
pub struct Transaction {
    kernels: BTreeMap<(NodeId, PartitionId), TxnKernel>,
    limits: Limits,
    /// Per `(node, partition)`, the newest generation a `Recovered` T1 acted on named, whether
    /// it created an instance or demoted one. It outlives the instance, so a late `Recovered`
    /// for a generation this node was demoted from cannot re-create it (lead ruling A-R73, N2).
    floors: BTreeMap<(NodeId, PartitionId), Generation>,
    /// Per `(node, partition)`, every trim and retire it was told of, with or without an
    /// instance, applied to each instance it creates (lead rulings A-R73 N1, A-R73a).
    trims: BTreeMap<(NodeId, PartitionId), TrimMemory>,
}

impl Transaction {
    /// No instances, default [`Limits`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// No instances, with `limits` for every instance created later.
    #[must_use]
    pub fn with_limits(limits: Limits) -> Self {
        Self {
            limits,
            ..Self::default()
        }
    }

    /// The instance for `(node, partition)`, if this node serves it.
    #[must_use]
    pub fn kernel(&self, node: NodeId, partition: PartitionId) -> Option<&TxnKernel> {
        self.kernels.get(&(node, partition))
    }

    /// The generation floor held for `(node, partition)` (lead ruling A-R73 N2), if one is.
    #[must_use]
    pub fn floor(&self, node: NodeId, partition: PartitionId) -> Option<Generation> {
        self.floors.get(&(node, partition)).copied()
    }

    /// Whether any trim or retire is remembered for `(node, partition)` (A-R73 N1, A-R73a).
    #[must_use]
    pub fn remembers_trims(&self, node: NodeId, partition: PartitionId) -> bool {
        self.trims.contains_key(&(node, partition))
    }

    /// Forget everything held for `node`: its instances, and the floors and trims that outlive
    /// them. All of it is process memory, so this is what a restart of that node loses (lead
    /// ruling V-R35); every other node is untouched. The simulator calls it from its restart.
    pub fn forget_node(&mut self, node: NodeId) {
        self.kernels.retain(|(held, _), _| *held != node);
        self.floors.retain(|(held, _), _| *held != node);
        self.trims.retain(|(held, _), _| *held != node);
    }

    /// Step the `(ctx.node, ctx.partition)` instance with a design-shaped event.
    ///
    /// Every §3.3 row is reachable here, including the ones whose contract carrier does not
    /// exist yet. [`Module::step`] is the same kernel behind the carriers that do.
    ///
    /// # Errors
    ///
    /// None today; the `Result` is the [`Module::step`] shape, kept so a caller can swap one for
    /// the other.
    pub fn step_txn(
        &mut self,
        ctx: &StepCtx<'_>,
        event: TxnEvent,
    ) -> Result<Vec<TxnEffect>, RdbError> {
        let key = (ctx.node, ctx.partition);
        if let Some(kernel) = self.kernels.get_mut(&key) {
            // A-R68: any T1 input is a chance to load a seed the snapshot now covers.
            kernel.try_seed(ctx.snapshot);
        }
        // A-R73a: remembered whether or not an instance is held. The answer below is unchanged.
        match event {
            TxnEvent::DedupTrim { generation, below } => {
                self.trims.entry(key).or_default().trim(generation, below);
            }
            TxnEvent::RetireGeneration { generation } => {
                let floor = self.floors.get(&key).copied();
                self.trims.entry(key).or_default().retire(generation, floor);
            }
            _ => {}
        }
        match event {
            TxnEvent::Recovered(result) => Ok(self.on_recovered(ctx, &result)),
            TxnEvent::Submit(req) => match self.kernels.get_mut(&key) {
                Some(kernel) => Ok(kernel.step(ctx, TxnEvent::Submit(req))),
                None => Ok(match admit(&req, ctx.partition, None, ctx.now) {
                    Err(rejection) => vec![reply(req.identity, rejection)],
                    // `admit` without an instance always fails at check 4.
                    Ok(_) => Vec::new(),
                }),
            },
            other => Ok(match self.kernels.get_mut(&key) {
                Some(kernel) => kernel.step(ctx, other),
                None => vec![ignored(ErrorKind::NotPrimary)],
            }),
        }
    }

    fn on_recovered(&mut self, ctx: &StepCtx<'_>, result: &RecoveryResult) -> Vec<TxnEffect> {
        let config = &result.committed.pinned_config;
        if config.partition != ctx.partition {
            return vec![ignored(ReplicaIgnoreReason::InvalidConfig)];
        }
        let key = (ctx.node, ctx.partition);
        let generation = result.new_generation;
        // The generation floor (lead rulings A-R73 N2, A-R73a). Below it, a `Recovered` is late:
        // acting on it would re-create a generation this node has left, with the same ids, or
        // demote a newer instance. At it, only the instance serving that generation may still
        // hear it (F1's activation re-emit); with none held, it was demoted from it.
        let held_at = self
            .kernels
            .get(&key)
            .is_some_and(|held| held.lineage.generation == generation);
        if let Some(floor) = self.floors.get(&key) {
            if generation < *floor || (generation == *floor && !held_at) {
                return vec![ignored(AuthorityIgnoreReason::RecoveredGenerationNotNewer)];
            }
        }
        self.floors.insert(key, generation);
        let primary = config.primary().map(|member| member.node);
        if primary != Some(ctx.node) {
            // Demotion: whatever was waiting is answered `NOT_PRIMARY` with the new primary.
            return match self.kernels.remove(&key) {
                Some(mut demoted) => demoted.strand(ctx.now, |k, _| RdbError::NotPrimary {
                    partition: k.lineage.partition,
                    hint: primary,
                }),
                None => vec![ignored(ErrorKind::NotPrimary)],
            };
        }
        let out = match self.kernels.remove(&key) {
            Some(mut held) if held.lineage.generation >= generation => {
                let out = held.on_reemitted(ctx, result);
                self.kernels.insert(key, held);
                return out;
            }
            Some(mut held) => held.strand(ctx.now, |k, _| RdbError::GenerationChanged {
                expected: k.lineage.generation,
                current: generation,
            }),
            None => Vec::new(),
        };
        // Lead ruling A-R73b: the replaced instance's index goes with it. Every new instance,
        // on this node or any other, starts from the durable seed and the remembered trims, so
        // a retry gets one answer whichever node serves it.
        let mut dedup = DedupIndex::default();
        if let Some(trims) = self.trims.get(&key) {
            trims.apply(&mut dedup);
        }
        let mut kernel = TxnKernel::at_recovery(result, dedup, self.limits, ctx.boot);
        kernel.try_seed(ctx.snapshot);
        self.kernels.insert(key, kernel);
        out
    }

    /// The contract event as a T1 input, or `None` when it is not one.
    fn input(event: &Event) -> Option<TxnEvent> {
        match &event.kind {
            EventKind::Client(ClientEvent::Submit(req)) => Some(TxnEvent::Submit(req.clone())),
            EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Answer(answer)))
                if answer.checkpoint == Checkpoint::StorageDispatch =>
            {
                Some(TxnEvent::AuthorityAnswer(*answer))
            }
            EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Fence { scope, reason })) => {
                Some(TxnEvent::Freeze {
                    scope: *scope,
                    cause: freeze_cause(*reason),
                })
            }
            EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::View(view))) => {
                Some(TxnEvent::AuthorityView(*view))
            }
            EventKind::Kernel(KernelEvent::Published {
                lineage,
                seq,
                record_digest,
                request,
            }) => Some(TxnEvent::Published {
                lineage: *lineage,
                seq: *seq,
                record_digest: *record_digest,
                request: *request,
            }),
            EventKind::Kernel(KernelEvent::DedupTrim { generation, below }) => {
                Some(TxnEvent::DedupTrim {
                    generation: *generation,
                    below: *below,
                })
            }
            EventKind::Kernel(KernelEvent::RetireGeneration { generation }) => {
                Some(TxnEvent::RetireGeneration {
                    generation: *generation,
                })
            }
            EventKind::Kernel(KernelEvent::SetAdmission(state)) => {
                Some(TxnEvent::AdmissionState(state.clone()))
            }
            EventKind::Kernel(KernelEvent::Recovered(result)) => {
                Some(TxnEvent::Recovered(result.clone()))
            }
            EventKind::Storage(StorageEvent::Committed { batch, applied }) if ours(*batch) => {
                Some(TxnEvent::BatchCompleted {
                    batch: *batch,
                    outcome: BatchOutcome::Ok {
                        applied: Seq(applied.0),
                    },
                })
            }
            EventKind::Storage(StorageEvent::CommitFailed { batch, fault }) if ours(*batch) => {
                Some(TxnEvent::BatchCompleted {
                    batch: *batch,
                    outcome: BatchOutcome::Err(*fault),
                })
            }
            _ => None,
        }
    }

    /// A T1 effect as a contract effect.
    fn output(event: &Event, effect: TxnEffect) -> Effect {
        let kind = match effect {
            TxnEffect::AuthorityCheck {
                checkpoint,
                lineage,
                correlation,
            } => EffectKind::Kernel(KernelEffect::AuthorityCheck {
                checkpoint,
                lineage,
                correlation,
            }),
            TxnEffect::Emit(candidate) => {
                EffectKind::Kernel(KernelEffect::AppliedCandidate(candidate))
            }
            TxnEffect::StorageBatch(batch) => EffectKind::Store(StoreEffect::Commit(batch)),
            TxnEffect::LocalApplied {
                seq,
                bytes,
                record_digest,
            } => EffectKind::Kernel(KernelEffect::LocalApplied {
                seq,
                bytes,
                record_digest,
            }),
            TxnEffect::Reply {
                identity,
                rejection,
            } => EffectKind::Reply(ReplyEffect::Failed {
                identity,
                error: rejection.error().clone(),
            }),
            TxnEffect::Replay { identity, result } => {
                EffectKind::Reply(ReplyEffect::Transaction { identity, result })
            }
            TxnEffect::Ignored(reason) => EffectKind::Kernel(KernelEffect::Ignored { reason }),
        };
        Effect {
            correlation: event.correlation,
            from: ModuleName::Transaction,
            partition: event.partition,
            kind,
        }
    }
}

/// Whether `batch` is one T1 allocated.
const fn ours(batch: BatchId) -> bool {
    batch.0 & TAG_MASK == BATCH_TAG
}

impl Module for Transaction {
    fn name(&self) -> ModuleName {
        ModuleName::Transaction
    }

    fn capability(&self) -> CapabilityState {
        // Deliberately still `Unavailable` (lead ruling V-R40 Q1), although A1 and P1 now report
        // `Wired` (V-R38). Every §3.3 row runs behind `step`, and the sim dispatcher carries this
        // module's `AuthorityCheck` to A1 and its `AppliedCandidate` to P1 (A-R65), with no T1
        // edge owed (A-R82..A-R84). What holds it is the campaign, not this module: reporting
        // `Wired` makes INV-DEDUP judge T1, which needs a recorded `ClientSubmit`, and the trace
        // validator's check 3 (ruling F-1) refuses a `ClientSubmit` recorded before a
        // `SchedulePhaseChanged{Healed}`. Nothing in the sim produces that phase yet; tried in
        // verif-corpus round 3, both armed F1/T1 cases then failed as harness errors. The
        // `Healed` producer is a follow-up slice owned by verification/sim.
        CapabilityState::Unavailable
    }

    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
        let Some(input) = Self::input(event) else {
            return Err(RdbError::unavailable(
                Capability::Transaction,
                "not a T1 input (Submit, a StorageDispatch answer, Fence, View, SetAdmission, \
                 Recovered, Published, DedupTrim, RetireGeneration, or a T1 batch completion)",
            ));
        };
        Ok(self
            .step_txn(ctx, input)?
            .into_iter()
            .map(|effect| Self::output(event, effect))
            .collect())
    }
}

/// The dispatcher's mapping of A1's fence reason onto the shared freeze vocabulary (K-A-29): a
/// local storage failure is its own cause, every other reason is lost authority.
#[must_use]
pub const fn freeze_cause(reason: DenyReason) -> FreezeCause {
    match reason {
        DenyReason::LocalStorageFenced => FreezeCause::LocalStorageFenced,
        other => FreezeCause::AuthorityLost(other),
    }
}

#[cfg(test)]
mod tests {
    //! SCAFFOLDING, not test rows (ruling L-R185d).
    //!
    //! They reach two private seams no public input can: step 13 handed a request check 10
    //! would have refused, and the record length check 10 computes without encoding. They are
    //! not `M7*` rows and carry no `#[retcd_test]`, as in `contracts::ignore`.

    use std::collections::VecDeque;

    use bytes::Bytes;

    use super::admission::{MAX_CONDITIONS, MAX_REQUEST_MUTATIONS};
    use super::{encoded_len, Admitted, DedupIndex, Limits, QueueMode, TxnKernel};
    use crate::contracts::authority::{AuthorityView, DenyReason, Lineage};
    use crate::contracts::digest::Digest;
    use crate::contracts::errors::RdbError;
    use crate::contracts::ids::{
        AffinityId, AuthorityGeneration, BootId, ClientId, ConfigVersion, Generation, GrantId,
        OwnerEpoch, PartitionId, RequestId, RequestIdentity, Seq, TenantId,
    };
    use crate::contracts::time::Tick;
    use crate::contracts::txn::{scoped_key, Condition, Mutation, TxnRequest};
    use crate::replication::append::{MAX_ENVELOPE_BYTES, MAX_MUTATIONS};

    const TENANT: TenantId = TenantId(3);
    const AFFINITY: AffinityId = AffinityId(9);

    fn lineage() -> Lineage {
        Lineage {
            partition: PartitionId(1),
            generation: Generation(7),
            owner_epoch: OwnerEpoch(1),
        }
    }

    fn view() -> AuthorityView {
        AuthorityView {
            lineage: lineage(),
            grant_id: GrantId(2),
            boot_id: BootId(1),
            authority_generation: AuthorityGeneration(1),
            config_version: ConfigVersion(1),
            authority_seq: 1,
            valid_through_tick: Tick(1_000),
            past_horizon: DenyReason::Expired,
        }
    }

    /// An open instance at seq 12, as recovery would leave it.
    fn kernel() -> TxnKernel {
        TxnKernel {
            lineage: lineage(),
            config_version: ConfigVersion(1),
            next_seq: Seq(12),
            prev_digest: Digest::ROOT,
            queue: VecDeque::new(),
            inflight: None,
            dedup: DedupIndex::default(),
            admission: None,
            authority: Some(view()),
            mode: QueueMode::Open,
            id_base: 0,
            next_batch: 0,
            next_correlation: 0,
            limits: Limits::default(),
            seed: None,
            reasked_under: None,
            start: super::StartRecord::NotOwed,
            start_sent_under: None,
            start_refused_at: None,
        }
    }

    fn key(user: &[u8]) -> Bytes {
        scoped_key(TENANT, AFFINITY, user)
    }

    fn request(conditions: Vec<Condition>, mutations: Vec<Mutation>) -> TxnRequest {
        TxnRequest {
            api_version: 1,
            identity: RequestIdentity {
                tenant: TENANT,
                client: ClientId(5),
                request: RequestId(1),
            },
            affinity: AFFINITY,
            expected_generation: None,
            remaining_millis: 1_000,
            conditions,
            mutations,
        }
    }

    fn put(user: &[u8], len: usize) -> Mutation {
        Mutation::Put {
            key: key(user),
            value: Bytes::from(vec![0xAB; len]),
            expected_version: None,
        }
    }

    /// Step 13 refuses a record over the cap itself, in every build, rather than trust check
    /// 10: `INVALID_ARGUMENT { envelope_bytes }`, check 10's error, and no reservation, so
    /// nothing reaches the batch. Unreachable through `admit`; this hands `reserve` a request
    /// check 10 never saw. Before ruling L-R185d it was a `debug_assert!`, and a release build
    /// shipped the record.
    #[test]
    fn reserve_refuses_an_oversized_record_check_ten_never_saw() {
        let k = kernel();
        let req = request(Vec::new(), vec![put(b"big", MAX_ENVELOPE_BYTES)]);
        let admitted = Admitted {
            request_digest: req.request_digest(),
            req,
            admitted_under: view(),
            at: Tick::ZERO,
        };
        assert_eq!(
            // The length, not the reservation: a failure prints a number, not a MiB of record.
            k.reserve(&admitted)
                .map(|reservation| reservation.record.len()),
            Err(RdbError::InvalidArgument {
                field: "envelope_bytes"
            })
        );
    }

    /// Step 13 refuses a record of more writes than every secondary's append row 2 accepts, in
    /// every build: 256 client writes plus its own `Dedup` write is 257. `INVALID_ARGUMENT {
    /// mutations }`, check 10's error, and no reservation. Unreachable through `admit`, which
    /// stops at 255; before ruling L-R185d nothing here checked it, and the record shipped.
    #[test]
    fn reserve_refuses_more_writes_than_a_secondary_takes() {
        let k = kernel();
        let req = request(
            Vec::new(),
            (0..MAX_MUTATIONS)
                .map(|i| Mutation::Delete {
                    key: key(&i.to_le_bytes()),
                    expected_version: None,
                })
                .collect(),
        );
        let admitted = Admitted {
            request_digest: req.request_digest(),
            req,
            admitted_under: view(),
            at: Tick::ZERO,
        };
        assert_eq!(
            k.reserve(&admitted)
                .map(|reservation| reservation.envelope.mutations.len()),
            Err(RdbError::InvalidArgument { field: "mutations" })
        );
    }

    /// Check 10's [`encoded_len`] is the length `encode` gives the envelope step 13 builds, for
    /// every shape that moves it: no writes, each mutation kind and each condition kind, empty
    /// keys and values, the most writes and conditions check 10 admits, and one value below, at
    /// and above the cap.
    #[test]
    fn encoded_len_is_the_encoded_records_length() {
        let k = kernel();
        let encoded = |req: &TxnRequest| {
            k.envelope(req, req.request_digest(), GrantId(2))
                .encode()
                .expect("every case fits the wire's u32 lengths")
                .len()
        };
        let delete = |user: &[u8]| Mutation::Delete {
            key: key(user),
            expected_version: None,
        };
        let many = |i: usize| key(&i.to_le_bytes());
        let mut cases = vec![
            request(Vec::new(), Vec::new()),
            request(
                Vec::new(),
                vec![Mutation::Put {
                    key: Bytes::new(),
                    value: Bytes::new(),
                    expected_version: None,
                }],
            ),
            request(
                Vec::new(),
                vec![
                    put(b"a", 1),
                    delete(b"b"),
                    Mutation::Put {
                        key: key(b"c"),
                        value: Bytes::from_static(b"v"),
                        expected_version: Some(4),
                    },
                    Mutation::Delete {
                        key: key(b"d"),
                        expected_version: Some(5),
                    },
                ],
            ),
            request(
                vec![
                    Condition::VersionEquals {
                        key: key(b"a"),
                        version: 3,
                    },
                    Condition::Absent { key: key(b"b") },
                    Condition::Present { key: key(b"c") },
                ],
                vec![put(b"a", 1)],
            ),
            request(
                vec![Condition::Absent { key: key(b"x") }; MAX_CONDITIONS],
                (0..MAX_REQUEST_MUTATIONS)
                    .map(|i| Mutation::Put {
                        key: many(i),
                        value: Bytes::from(vec![0xCD; 16]),
                        expected_version: None,
                    })
                    .collect(),
            ),
            request(
                Vec::new(),
                (0..MAX_REQUEST_MUTATIONS)
                    .map(|i| Mutation::Delete {
                        key: many(i),
                        expected_version: None,
                    })
                    .collect(),
            ),
        ];
        let at_cap = MAX_ENVELOPE_BYTES - encoded(&request(Vec::new(), vec![put(b"big", 0)]));
        for len in [at_cap - 1, at_cap, at_cap + 1] {
            cases.push(request(Vec::new(), vec![put(b"big", len)]));
        }
        for req in &cases {
            assert_eq!(
                encoded_len(req),
                encoded(req),
                "{} conditions, {} mutations",
                req.conditions.len(),
                req.mutations.len()
            );
        }
        assert_eq!(encoded_len(&cases[cases.len() - 2]), MAX_ENVELOPE_BYTES);
    }
}
