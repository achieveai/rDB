//! Package P1 (team kernel-a `design.md` §4). Two kinds of test live here:
//!
//! - **Test-plan rows** of `docs/testing/test-plan-m7-kernel-a.md` §5 and §8, one function per
//!   id, named `m7a_<id>_<the plan's Name column>` with no zero padding, so
//!   `scripts/m7-census.sh kernel-a` counts them. A row whose plan text predates a lead ruling
//!   follows the ruling and cites it in its doc comment.
//! - **Developer tests** with plain names. They are not rows and the census does not count them.
//!
//! Where a row names a fact's payload, it drives [`PubKernel`] directly as well ([`Direct`]):
//! through `Module::step` a fact becomes `Ignored{reason}` and the payload is not on the wire.
//!
//! Every test is a trace: a sequence of events stepped through [`Module::step`] (or
//! [`Publication::step_with`] with a real R1 tracker), with the returned effect vector asserted
//! at each step and the state read back through [`Publication::view`].

use std::collections::BTreeMap;

use bytes::Bytes;
use config_log::retcd_test;

use rdb_core::contracts::authority::{
    AuthorityDecision, AuthorityEvent, AuthorityIgnoreReason, AuthorityView, BlockReason,
    Checkpoint, DenyReason, FenceScope, FencingProof, Lineage, PartitionMode, Revocation, Verdict,
};
use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::envelope::{AppendAck, ReplicaProgress};
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{
    Budgets, ClientEvent, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module,
    ReplyEffect, StepCtx,
};
use rdb_core::contracts::ids::{
    AffinityId, AppliedSeq, AuthorityGeneration, BootId, ClientId, ConfigVersion, CorrelationId,
    DurableSeq, EventId, Generation, GrantId, NodeId, OwnerEpoch, PartitionId, ReceivedSeq,
    ReplicaRole, RequestId, RequestIdentity, Revision, Seq, SnapshotHandle, TenantId, TimerVersion,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::publication::{
    AppliedCandidate, FreezeCause, PubMode, PublicationEffect, PublicationEvent, StatusEntry,
    StatusOutcome,
};
use rdb_core::contracts::qualification::{
    QualificationCause, QualificationChanged, QualificationDirection,
};
use rdb_core::contracts::recovery::{
    CommittedRoot, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap, SelectedLineage,
};
use rdb_core::contracts::storage::{Namespace, SnapshotRead, StorageEvent, StoreEffect};
use rdb_core::contracts::time::{ControlTime, Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::{CapabilityState, ReadServiceOutcome, Version};
use rdb_core::contracts::transport::PeerLabel;
use rdb_core::contracts::txn::{Durability, Outcome, TxnResult, TxnStatus};
use rdb_core::publication::status::to_wire;
use rdb_core::publication::{
    post_apply_timer, AwaitingReply, PendingView, PredicateFalse, PubConfig, PubEffect, PubEvent,
    PubFact, PubKernel, PubStateView, Publication, PublishedAt, ReplicationView,
    ScriptedReplication, StatusIndex, Withheld, POST_APPLY_DEADLINE_MILLIS,
    PUBLICATION_CORRELATION_BASE, PUBLICATION_SNAPSHOT_BASE, WAITER_CAP,
};
use rdb_core::replication::progress::{DigestLadder, DigestLookup, ProgressTracker, TrackerInit};
use rdb_core::transaction::dedup::{dedup_key, dedup_value};
use rdb_core::transaction::RETENTION_CAP_ENTRIES;

const NODE: NodeId = NodeId(1);
const BOOT: BootId = BootId(1);
const P: PartitionId = PartitionId(3);
const P2: PartitionId = PartitionId(4);
const GEN: Generation = Generation(2);
const EPOCH: OwnerEpoch = OwnerEpoch(1);
const CONFIG: ConfigVersion = ConfigVersion(1);
const BUDGETS: Budgets = Budgets::SPEC_DEFAULTS;
/// What every rig has published at install.
const START: u64 = 4;
/// The default post-apply deadline, never repeated as a literal (lead ruling A-R66).
const DEADLINE: u64 = POST_APPLY_DEADLINE_MILLIS;
/// The default waiter cap.
const CAP: u64 = WAITER_CAP as u64;

// ---- fixture ------------------------------------------------------------------------------------

fn lineage_of(partition: PartitionId) -> Lineage {
    Lineage {
        partition,
        generation: GEN,
        owner_epoch: EPOCH,
    }
}

fn lineage() -> Lineage {
    lineage_of(P)
}

fn req(n: u64) -> RequestIdentity {
    RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(1),
        request: RequestId(n),
    }
}

/// The record digest at `seq`: any injective map will do.
fn d(seq: u64) -> Digest {
    Digest::of(Domain::Record, &[&seq.to_le_bytes()])
}

fn result(partition: PartitionId, seq: u64) -> TxnResult {
    TxnResult {
        partition,
        owner_epoch: EPOCH,
        generation: GEN,
        seq: Seq(seq),
        outcome: Outcome::Published,
        durability: Durability::BufferedOnTwo,
    }
}

fn decision(
    partition: PartitionId,
    checkpoint: Checkpoint,
    correlation: CorrelationId,
    verdict: Verdict,
    authority_seq: u64,
) -> AuthorityDecision {
    AuthorityDecision {
        owner: NODE,
        boot: BOOT,
        grant: GrantId(1),
        authority_generation: AuthorityGeneration(1),
        lineage: lineage_of(partition),
        expiry_utc_ms: 0,
        decided_at: Tick::ZERO,
        authority_seq,
        checkpoint,
        correlation,
        verdict,
    }
}

fn candidate_of(partition: PartitionId, seq: u64, request: u64) -> AppliedCandidate {
    AppliedCandidate {
        lineage: lineage_of(partition),
        config_version: CONFIG,
        seq: Seq(seq),
        prev_digest: d(seq - 1),
        record_digest: d(seq),
        request: req(request),
        request_digest: Digest::of(Domain::Request, &[&request.to_le_bytes()]),
        pending_result: result(partition, seq),
        authority: decision(
            partition,
            Checkpoint::StorageDispatch,
            CorrelationId(request),
            Verdict::Admit,
            1,
        ),
    }
}

fn candidate(seq: u64, request: u64) -> EventKind {
    EventKind::Kernel(KernelEvent::AppliedCandidate(Box::new(candidate_of(
        P, seq, request,
    ))))
}

fn edge_of(partition: PartitionId, seq: u64, direction: QualificationDirection) -> EventKind {
    EventKind::Kernel(KernelEvent::QualificationChanged(QualificationChanged {
        lineage: lineage_of(partition),
        config_version: CONFIG,
        at_seq: Seq(seq),
        direction,
        qualified_copies: vec![CopyId(2)],
        qualified_ack_count: 1,
        cause: QualificationCause::AckAdvanced,
        tick: Tick::ZERO,
    }))
}

fn gained(seq: u64) -> EventKind {
    edge_of(P, seq, QualificationDirection::Gained)
}

fn lost(seq: u64) -> EventKind {
    edge_of(P, seq, QualificationDirection::Lost)
}

fn answer(checkpoint: Checkpoint, correlation: CorrelationId, verdict: Verdict) -> EventKind {
    answer_at(checkpoint, correlation, verdict, 1)
}

fn answer_at(
    checkpoint: Checkpoint,
    correlation: CorrelationId,
    verdict: Verdict,
    authority_seq: u64,
) -> EventKind {
    EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Answer(decision(
        P,
        checkpoint,
        correlation,
        verdict,
        authority_seq,
    ))))
}

fn view_push(partition: PartitionId, authority_seq: u64) -> EventKind {
    EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::View(
        AuthorityView {
            lineage: lineage_of(partition),
            grant_id: GrantId(1),
            boot_id: BOOT,
            authority_generation: AuthorityGeneration(1),
            config_version: CONFIG,
            authority_seq,
            valid_through_tick: Tick(u64::MAX),
            past_horizon: DenyReason::Expired,
        },
    )))
}

fn fence(scope: FenceScope, reason: DenyReason) -> EventKind {
    EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Fence {
        scope,
        reason,
    }))
}

fn deadline(partition: PartitionId, version: u64) -> EventKind {
    EventKind::Timer(TimerFired {
        id: post_apply_timer(partition),
        version: TimerVersion(version),
        scheduled_at: Tick::ZERO,
    })
}

fn read(n: u64) -> EventKind {
    read_key(n, b"k")
}

fn read_key(n: u64, key: &'static [u8]) -> EventKind {
    EventKind::Client(ClientEvent::Read {
        identity: req(n),
        key: Bytes::from_static(key),
    })
}

fn status(n: u64, generation: Option<Generation>) -> EventKind {
    EventKind::Client(ClientEvent::Status {
        identity: req(n),
        generation,
    })
}

fn block() -> BlockReason {
    BlockReason::DivergenceRequiresOperator {
        diverged: vec![CopyId(2)],
    }
}

fn corr(n: u64) -> CorrelationId {
    CorrelationId(PUBLICATION_CORRELATION_BASE | n)
}

fn ignored(reason: AuthorityIgnoreReason) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Ignored {
        reason: KernelIgnoredReason::Authority(reason),
    })
}

fn check(checkpoint: Checkpoint, correlation: CorrelationId) -> EffectKind {
    EffectKind::Kernel(KernelEffect::AuthorityCheck {
        checkpoint,
        lineage: lineage(),
        correlation,
    })
}

fn status_write(seq: u64, request: u64, outcome: StatusOutcome, at: u64) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Publication(PublicationEffect::Status(
        Box::new(StatusEntry {
            request: req(request),
            lineage: lineage(),
            seq: Some(Seq(seq)),
            record_digest: Some(d(seq)),
            outcome,
            snapshot: None,
            at: Tick(at),
        }),
    )))
}

fn published_status(seq: u64) -> StatusOutcome {
    StatusOutcome::Published {
        result: result(P, seq),
    }
}

fn arm(version: u64, at: u64) -> EffectKind {
    EffectKind::Timer(TimerEffect::Arm {
        id: post_apply_timer(P),
        version: TimerVersion(version),
        at: Tick(at),
    })
}

fn quarantined(seq: u64) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Publication(PublicationEffect::Quarantined {
        generation: GEN,
        seq: Seq(seq),
    }))
}

fn read_reply(n: u64, outcome: ReadServiceOutcome, value: Option<(Version, Digest)>) -> EffectKind {
    EffectKind::Reply(ReplyEffect::Read {
        identity: req(n),
        outcome,
        value,
    })
}

fn rejected_read(n: u64, kind: ErrorKind) -> EffectKind {
    read_reply(n, ReadServiceOutcome::Rejected(kind), None)
}

fn txn_reply(n: u64, seq: u64) -> EffectKind {
    EffectKind::Reply(ReplyEffect::Transaction {
        identity: req(n),
        result: result(P, seq),
    })
}

fn unknown_reply(n: u64) -> EffectKind {
    EffectKind::Reply(ReplyEffect::Failed {
        identity: req(n),
        error: RdbError::UnknownOutcome {
            partition: P,
            identity: req(n),
        },
    })
}

/// How many terminal transaction replies `effects` carries for request `n`.
fn replies_for(effects: &[EffectKind], n: u64) -> usize {
    effects
        .iter()
        .filter(|e| {
            matches!(e, EffectKind::Reply(ReplyEffect::Transaction { identity, .. })
                | EffectKind::Reply(ReplyEffect::Failed { identity, .. }) if *identity == req(n))
        })
        .count()
}

/// P1's snapshot handle `n` for `partition`: the scheme `PUBLICATION_SNAPSHOT_BASE` documents,
/// written out here rather than borrowed from the minter, so a drift in the minter fails a row.
fn snap_of(partition: PartitionId, n: u64) -> SnapshotHandle {
    SnapshotHandle(PUBLICATION_SNAPSHOT_BASE | u64::from(partition.0) << 16 | n)
}

fn snap(n: u64) -> SnapshotHandle {
    snap_of(P, n)
}

fn open(n: u64) -> EffectKind {
    EffectKind::Store(StoreEffect::Snapshot {
        handle: snap(n),
        partition: P,
    })
}

fn release(n: u64) -> EffectKind {
    EffectKind::Store(StoreEffect::Release { handle: snap(n) })
}

/// The test-local stand-in for storage binding a view (A-R69a: sim routing is not P1's).
fn ready_of(handle: SnapshotHandle, at: u64) -> EventKind {
    EventKind::Storage(StorageEvent::SnapshotReady {
        handle,
        at: Seq(at),
    })
}

fn ready(n: u64, at: u64) -> EventKind {
    ready_of(snap(n), at)
}

fn read_previous(n: u64) -> EventKind {
    EventKind::Kernel(KernelEvent::Publication(PublicationEvent::ReadPrevious {
        identity: req(n),
    }))
}

fn previous(n: u64, handle: Result<SnapshotHandle, ErrorKind>) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Publication(PublicationEffect::Snapshot {
        identity: req(n),
        handle,
    }))
}

fn notify(seq: u64) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Published {
        lineage: lineage(),
        seq: Seq(seq),
        record_digest: d(seq),
        request: req(seq),
    })
}

fn cancel(version: u64) -> EffectKind {
    EffectKind::Timer(TimerEffect::Cancel {
        id: post_apply_timer(P),
        version: TimerVersion(version),
    })
}

/// The handles `effects` opens and releases, in order.
fn snapshot_ledger(effects: &[EffectKind]) -> (Vec<SnapshotHandle>, Vec<SnapshotHandle>) {
    let mut opened = Vec::new();
    let mut released = Vec::new();
    for effect in effects {
        match effect {
            EffectKind::Store(StoreEffect::Snapshot { handle, .. }) => opened.push(*handle),
            EffectKind::Store(StoreEffect::Release { handle }) => released.push(*handle),
            _ => {}
        }
    }
    (opened, released)
}

fn published_any(effects: &[EffectKind]) -> bool {
    effects
        .iter()
        .any(|e| matches!(e, EffectKind::Kernel(KernelEffect::Published { .. })))
}

/// M7A-115's wire half, checked on every step of every row: no status answer P1 emits anywhere in
/// this binary is `TxnStatus::Unresolved` (KA-9: it is T1's answer, and P1 has no state for it).
fn assert_no_status_is_unresolved(effects: &[EffectKind]) {
    for effect in effects {
        if let EffectKind::Reply(ReplyEffect::Status {
            status: status @ TxnStatus::Unresolved { .. },
            ..
        }) = effect
        {
            panic!("P1 answered a status {status:?}: {effects:?}");
        }
    }
}

/// A snapshot at one position with one key in it, and the dedup rows a status seed scans
/// (M7A-194).
struct FixedSnapshot {
    generation: Generation,
    at: Seq,
    data: BTreeMap<Vec<u8>, (Version, Bytes)>,
    dedup: Vec<(Bytes, Bytes)>,
    /// How many times `scan` was called: the status seed's rescans (reviewer R-3).
    scans: std::cell::Cell<usize>,
}

impl SnapshotRead for FixedSnapshot {
    fn handle(&self) -> SnapshotHandle {
        SnapshotHandle(self.generation.0 << 32 | self.at.0)
    }
    fn at(&self) -> Seq {
        self.at
    }
    fn generation(&self) -> Generation {
        self.generation
    }
    fn get(&self, _ns: Namespace, key: &[u8]) -> Option<Bytes> {
        self.data.get(key).map(|(_, v)| v.clone())
    }
    fn version(&self, _ns: Namespace, key: &[u8]) -> Option<Version> {
        self.data.get(key).map(|(v, _)| *v)
    }
    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        self.scans.set(self.scans.get() + 1);
        if ns != Namespace::Dedup {
            return Vec::new();
        }
        let mut rows: Vec<_> = self
            .dedup
            .iter()
            .filter(|(key, _)| key.as_ref() >= from)
            .cloned()
            .collect();
        rows.sort();
        rows.truncate(limit);
        rows
    }
}

/// The value `k` holds in a snapshot at `seq`: written by `seq` itself.
fn value_at(seq: u64) -> (Version, Digest) {
    value_of(b"k", seq)
}

/// The value `key` holds in a snapshot at `seq`. Both keys hold `seq`'s bytes, so only the key in
/// the digest's preimage tells them apart — which is exactly what a wrong-key answer gets wrong.
fn value_of(key: &[u8], seq: u64) -> (Version, Digest) {
    let bytes = seq.to_le_bytes();
    (seq, Digest::of(Domain::ReadValue, &[key, &bytes]))
}

/// P1 with partition `P` installed at [`START`], a KA-8 view, and a clock.
struct Rig {
    p1: Publication,
    now: u64,
    next: u64,
    boot: BootId,
    snapshot: FixedSnapshot,
}

impl Rig {
    /// `P` installed, the view pushed at authority seq 1, nothing qualifying.
    fn new() -> Self {
        Self::new_with(PubConfig::default())
    }

    /// [`Self::new`] under `config`.
    fn new_with(config: PubConfig) -> Self {
        let mut rig = Self::bare_with(config);
        assert_eq!(
            rig.step(view_push(P, 1)),
            vec![],
            "adopting a view emits nothing"
        );
        rig
    }

    /// `P` installed, no view held. The install's snapshot view (handle `0`) is asked for and
    /// never bound here; a row that wants it bound steps `ready(0, START)`.
    fn bare() -> Self {
        Self::bare_with(PubConfig::default())
    }

    /// [`Self::bare`] under `config`.
    fn bare_with(config: PubConfig) -> Self {
        let mut p1 = Publication::with_config(config);
        assert_eq!(p1.install(NODE, BOOT, lineage(), Seq(START)), open(0));
        p1.script_replication(NODE, P, ScriptedReplication::new(lineage(), CONFIG));
        let mut rig = Self {
            p1,
            now: 100,
            next: 0,
            boot: BOOT,
            snapshot: FixedSnapshot {
                generation: GEN,
                at: Seq(START),
                data: BTreeMap::new(),
                dedup: Vec::new(),
                scans: std::cell::Cell::new(0),
            },
        };
        rig.snapshot_at(START);
        rig
    }

    /// The storage view the next step sees: at `seq`, with `a` and `k` written by `seq`.
    fn snapshot_at(&mut self, seq: u64) {
        self.snapshot.at = Seq(seq);
        self.snapshot.data.clear();
        for key in [&b"a"[..], b"k"] {
            self.snapshot.data.insert(
                key.to_vec(),
                (seq, Bytes::copy_from_slice(&seq.to_le_bytes())),
            );
        }
    }

    fn qualify(&mut self, seq: u64) {
        self.p1
            .scripted_mut(NODE, P)
            .expect("scripted")
            .set_qualifies(Seq(seq), true);
    }

    fn event(&mut self, partition: PartitionId, kind: EventKind) -> Event {
        self.next += 1;
        self.now += 10;
        Event {
            id: EventId(self.next),
            at: Tick(self.now),
            node: NODE,
            boot: self.boot,
            partition,
            correlation: CorrelationId(self.next),
            kind,
        }
    }

    fn ctx(&self, partition: PartitionId) -> StepCtx<'_> {
        StepCtx {
            now: Tick(self.now),
            control_time: ControlTime {
                estimate: Tick(self.now),
                error_millis: 10,
                bound_established: true,
                sampled_at: Tick(self.now),
            },
            node: NODE,
            boot: self.boot,
            partition,
            generation: GEN,
            owner_epoch: EPOCH,
            config_version: CONFIG,
            snapshot: &self.snapshot,
            budgets: &BUDGETS,
        }
    }

    fn try_step_on(
        &mut self,
        partition: PartitionId,
        kind: EventKind,
    ) -> Result<Vec<EffectKind>, RdbError> {
        let event = self.event(partition, kind);
        let ctx = StepCtx {
            snapshot: &self.snapshot,
            budgets: &BUDGETS,
            ..self.ctx(partition)
        };
        let mut p1 = std::mem::take(&mut self.p1);
        let out = p1.step(&ctx, &event);
        self.p1 = p1;
        let effects: Vec<EffectKind> = out?.into_iter().map(|e| e.kind).collect();
        self.assert_only_kept_handles_leave(partition, &effects);
        assert_no_status_is_unresolved(&effects);
        Ok(effects)
    }

    /// A-R69a addendum, checked on every step of every row: a handle P1 hands a reader is the
    /// one it minted and kept, never the step's storage view (the sim's `STEP_VIEW`), which
    /// nobody binds or releases.
    fn assert_only_kept_handles_leave(&self, partition: PartitionId, effects: &[EffectKind]) {
        for effect in effects {
            if let EffectKind::Kernel(KernelEffect::Publication(PublicationEffect::Snapshot {
                handle: Ok(handle),
                ..
            })) = effect
            {
                assert_ne!(*handle, self.snapshot.handle(), "the step's view escaped");
                let kept = self.p1.view(NODE, partition).and_then(|v| v.kept);
                assert_eq!(Some(*handle), kept, "handed out a view P1 does not keep");
            }
        }
    }

    fn step_on(&mut self, partition: PartitionId, kind: EventKind) -> Vec<EffectKind> {
        self.try_step_on(partition, kind).expect("a P1 input")
    }

    fn step(&mut self, kind: EventKind) -> Vec<EffectKind> {
        self.step_on(P, kind)
    }

    fn view(&self) -> PubStateView {
        self.p1.view(NODE, P).expect("installed")
    }

    /// Candidate `seq` for request `seq`, qualified, and its `Publication` check emitted.
    /// Returns that check's correlation.
    fn pending_with_recheck(&mut self, seq: u64) -> CorrelationId {
        self.step(candidate(seq, seq));
        self.qualify(seq);
        let effects = self.step(gained(seq));
        match effects.as_slice() {
            [EffectKind::Kernel(KernelEffect::AuthorityCheck {
                checkpoint: Checkpoint::Publication,
                correlation,
                ..
            })] => *correlation,
            other => panic!("expected one Publication check, got {other:?}"),
        }
    }

    /// [`Self::pending_with_recheck`], then `Admit`: published, its snapshot asked for and not
    /// yet bound. Returns every effect of the publish step.
    fn publish_only(&mut self, seq: u64) -> Vec<EffectKind> {
        let c = self.pending_with_recheck(seq);
        self.snapshot_at(seq);
        let effects = self.step(answer(Checkpoint::Publication, c, Verdict::Admit));
        assert!(published_any(&effects), "{effects:?}");
        effects
    }

    /// [`Self::publish_only`], then storage binds the view at `seq`: published, the old-prefix
    /// view kept, awaiting its `Reply` answer. Returns the `Reply` check's correlation.
    fn published(&mut self, seq: u64) -> CorrelationId {
        let effects = self.publish_only(seq);
        let (opened, _) = snapshot_ledger(&effects);
        assert_eq!(opened.len(), 1, "{effects:?}");
        assert_eq!(self.step(ready_of(opened[0], seq)), vec![]);
        effects
            .iter()
            .find_map(|e| match e {
                EffectKind::Kernel(KernelEffect::AuthorityCheck {
                    checkpoint: Checkpoint::Reply,
                    correlation,
                    ..
                }) => Some(*correlation),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected a Reply check, got {effects:?}"))
    }
}

// ---- reach ------------------------------------------------------------------------------------

/// Every carrier P1 consumes steps through `Module::step`; every other kind is refused by name
/// and leaves the state untouched.
#[retcd_test]
fn reach_every_p1_carrier_steps_and_foreign_kinds_are_refused() {
    let mut rig = Rig::new();
    let accepted = [
        candidate(5, 5),
        gained(5),
        answer(Checkpoint::Publication, corr(99), Verdict::Admit),
        view_push(P, 1),
        fence(FenceScope::Partition(P2), DenyReason::Expired),
        deadline(P, 99),
        read(1),
        status(1, None),
        EventKind::Kernel(KernelEvent::Publication(PublicationEvent::ModeQuery {
            identity: req(2),
        })),
        EventKind::Kernel(KernelEvent::Publication(PublicationEvent::ReadPrevious {
            identity: req(3),
        })),
        EventKind::Kernel(KernelEvent::StatusTrim {
            generation: GEN,
            below: Seq(1),
        }),
        EventKind::Kernel(KernelEvent::RetireGeneration {
            generation: Generation(1),
        }),
        EventKind::Kernel(KernelEvent::BlockPartition(block())),
        // A handle in P1's block for this partition, though never opened: stepped, a no-op.
        ready(9, 5),
    ];
    let mut refused = Vec::new();
    for kind in accepted {
        if let Err(e) = rig.try_step_on(P, kind.clone()) {
            refused.push((kind, e));
        }
    }
    // Every carrier is stepped, the fence for another partition included: it is answered
    // `Ignored(NotOurs)` (M7A-189), no longer refused.
    assert!(refused.is_empty(), "{refused:?}");

    let before = rig.view();
    for kind in [
        EventKind::Kernel(KernelEvent::DedupTrim {
            generation: GEN,
            below: Seq(1),
        }),
        EventKind::Kernel(KernelEvent::CopyLost { copy: CopyId(2) }),
        EventKind::Kernel(KernelEvent::Authority(
            AuthorityEvent::RevokeEpochRequested {
                partition: P,
                epoch: EPOCH,
            },
        )),
        EventKind::Timer(TimerFired {
            id: post_apply_timer(P2),
            version: TimerVersion(1),
            scheduled_at: Tick::ZERO,
        }),
        // Storage completions for views P1 did not mint: another minter's, and P1's own block
        // for another partition.
        ready_of(SnapshotHandle(7), 5),
        ready_of(snap_of(P2, 1), 5),
    ] {
        let err = rig.try_step_on(P, kind).expect_err("not a P1 input");
        assert_eq!(err.kind(), ErrorKind::Unavailable, "{err}");
    }
    assert_eq!(
        rig.view(),
        before,
        "a refused event does not step the kernel"
    );
    // `Wired` since ruling V-R38: the refusals above are the module's own answer to a foreign
    // kind, not a sign that it is unwired.
    assert_eq!(rig.p1.capability(), CapabilityState::Wired);
}

// ---- the publication rule ---------------------------------------------------------------------

/// M7A-91: candidate → qualifying ACK → `Publication` check → publish → `Reply` check → reply.
/// The candidate asks nothing; the check comes only with the qualification, the publish only with
/// its answer, and `Serving` stays `Serving`.
#[retcd_test]
fn m7a_91_candidate_qualifying_ack_recheck_publish_in_order() {
    let mut rig = Rig::new();

    let t = rig.now + 10;
    assert_eq!(
        rig.step(candidate(5, 5)),
        vec![
            arm(1, t + DEADLINE),
            status_write(5, 5, StatusOutcome::Unknown, t)
        ]
    );
    assert_eq!(
        rig.view().published.seq,
        Seq(START),
        "a candidate is not a success"
    );

    rig.qualify(5);
    assert_eq!(
        rig.step(gained(5)),
        vec![check(Checkpoint::Publication, corr(1))]
    );
    assert_eq!(
        rig.view().published.seq,
        Seq(START),
        "an ACK is not a success either"
    );

    rig.snapshot_at(5);
    let t = rig.now + 10;
    assert_eq!(
        rig.step(answer(Checkpoint::Publication, corr(1), Verdict::Admit)),
        vec![
            status_write(5, 5, published_status(5), t),
            // A-R69a: the old-prefix view is asked for before T1 hears `Published`.
            open(1),
            notify(5),
            cancel(1),
            check(Checkpoint::Reply, corr(2)),
        ]
    );
    let view = rig.view();
    assert_eq!(view.published.seq, Seq(5));
    assert_eq!(view.pending, None);
    assert_eq!(view.mode, PubMode::Serving);
    assert!(!view.awaiting_reply[&corr(2)].replied);

    assert_eq!(
        rig.step(answer(Checkpoint::Reply, corr(2), Verdict::Admit)),
        vec![txn_reply(5, 5)]
    );
    assert!(rig.view().awaiting_reply.is_empty());
    assert_eq!(
        rig.step(status(5, Some(GEN))),
        vec![EffectKind::Reply(ReplyEffect::Status {
            identity: req(5),
            status: TxnStatus::Resolved(result(P, 5)),
        })]
    );
}

/// Spike §7 mutation "publish before ACK": an `Admit` at `Publication` with no check outstanding
/// is not an answer to anything, even when it names the correlation P1 would mint next.
#[retcd_test]
fn admit_before_any_qualifying_ack_never_publishes() {
    let mut rig = Rig::new();
    rig.step(candidate(5, 5));
    rig.qualify(5);
    assert_eq!(
        rig.step(answer(Checkpoint::Publication, corr(1), Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
    assert_eq!(rig.view().published.seq, Seq(START));
    assert!(rig.view().pending.is_some());

    // Twin: the same answer after the qualifying ACK publishes.
    assert_eq!(
        rig.step(gained(5)),
        vec![check(Checkpoint::Publication, corr(1))]
    );
    let effects = rig.step(answer(Checkpoint::Publication, corr(1), Verdict::Admit));
    assert!(published_any(&effects), "{effects:?}");
    assert_eq!(rig.view().published.seq, Seq(5));
}

/// Invariant 1, scripted: a `Gained` edge only wakes P1. The publish re-reads `qualifies_now`,
/// so an edge R1's live state no longer backs (a shadow's ACK, a copy excluded since) publishes
/// nothing.
#[retcd_test]
fn a_gained_edge_is_not_trusted_publication_rereads_qualifies_now() {
    let mut rig = Rig::new();
    rig.step(candidate(5, 5));
    assert_eq!(
        rig.step(gained(5)),
        vec![check(Checkpoint::Publication, corr(1))]
    );
    assert_eq!(
        rig.step(answer(Checkpoint::Publication, corr(1), Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::PublishPredicateFalse)]
    );
    let view = rig.view();
    assert_eq!(view.published.seq, Seq(START));
    let pending = view.pending.expect("pending kept");
    assert!(!pending.qualifying && pending.recheck.is_none());

    // A fresh `Gained` once R1 really qualifies it: a new check, a new correlation, a publish.
    rig.qualify(5);
    assert_eq!(
        rig.step(gained(5)),
        vec![check(Checkpoint::Publication, corr(2))]
    );
    let effects = rig.step(answer(Checkpoint::Publication, corr(2), Verdict::Admit));
    assert!(published_any(&effects), "{effects:?}");
}

/// Invariant 1 against kernel-b's real tracker, spike §7 mutation "count a shadow ACK": a shadow
/// that has applied seq 5 does not make it publishable; the one regular secondary does.
#[retcd_test]
fn a_shadow_ack_never_qualifies_against_the_real_tracker() {
    let a = NodeId(1);
    let b = NodeId(2);
    let s = NodeId(4);
    let member = |copy: u8, node: NodeId, role| Member {
        copy: CopyId(copy),
        node,
        boot: BootId(u64::from(node.0)),
        role,
    };
    let mut config = PartitionConfig::new(
        P,
        CONFIG,
        vec![
            member(0, a, ReplicaRole::Primary),
            member(1, b, ReplicaRole::RegularSecondary),
            member(3, s, ReplicaRole::Shadow),
        ],
    );
    config.min_regular_acks = 1;
    let mut history = DigestLadder::new();
    for seq in 1..=5 {
        history.insert(Seq(seq), d(seq));
    }
    let progress = |seq: u64| ReplicaProgress {
        received: ReceivedSeq(seq),
        buffered_applied: AppliedSeq(seq),
        durable: DurableSeq(seq),
    };
    let mut tracker = ProgressTracker::new(TrackerInit {
        config,
        own: CopyId(0),
        lineage: lineage(),
        history,
        local: progress(5),
    })
    .expect("tracker");
    let ack = |node: NodeId, role| AppendAck {
        partition: P,
        generation: GEN,
        owner_epoch: EPOCH,
        config_version: CONFIG,
        from: node,
        boot: BootId(u64::from(node.0)),
        role,
        progress: progress(5),
        digest_at_buffered: d(5),
    };
    let label = |node: NodeId| PeerLabel {
        node,
        boot: BootId(u64::from(node.0)),
        authenticated: true,
    };

    let mut rig = Rig::new();
    rig.step(candidate(5, 5));
    tracker.on_ack(&label(s), &ack(s, ReplicaRole::Shadow), Tick(1));
    assert!(
        !tracker.qualifies_now(Seq(5)),
        "kernel-b: a shadow never counts"
    );

    // A `Gained` P1 was handed anyway (a forged or stale edge) asks the check...
    assert_eq!(
        rig.step(gained(5)),
        vec![check(Checkpoint::Publication, corr(1))]
    );
    // ...and the publish row, reading the real tracker, refuses.
    let effects = step_with_tracker(
        &mut rig,
        &tracker,
        answer(Checkpoint::Publication, corr(1), Verdict::Admit),
    );
    assert_eq!(
        effects,
        vec![ignored(AuthorityIgnoreReason::PublishPredicateFalse)]
    );
    assert_eq!(rig.view().published.seq, Seq(START));

    // The regular secondary's ACK is the one fact that makes it publish.
    tracker.on_ack(&label(b), &ack(b, ReplicaRole::RegularSecondary), Tick(2));
    assert!(tracker.qualifies_now(Seq(5)));
    rig.step(gained(5));
    rig.snapshot_at(5);
    let effects = step_with_tracker(
        &mut rig,
        &tracker,
        answer(Checkpoint::Publication, corr(2), Verdict::Admit),
    );
    assert!(published_any(&effects), "{effects:?}");
    assert_eq!(rig.view().published.seq, Seq(5));
}

fn step_with_tracker(rig: &mut Rig, tracker: &ProgressTracker, kind: EventKind) -> Vec<EffectKind> {
    let event = rig.event(P, kind);
    let ctx = StepCtx {
        snapshot: &rig.snapshot,
        budgets: &BUDGETS,
        ..rig.ctx(P)
    };
    let mut p1 = std::mem::take(&mut rig.p1);
    let out = p1
        .step_with(&ctx, &event, Some(tracker as &dyn ReplicationView))
        .expect("a P1 input");
    rig.p1 = p1;
    let effects: Vec<EffectKind> = out.into_iter().map(|e| e.kind).collect();
    rig.assert_only_kept_handles_leave(P, &effects);
    assert_no_status_is_unresolved(&effects);
    effects
}

/// K-A-51: the digest conjunct alone refuses, with `qualifies_now` true throughout.
#[retcd_test]
fn the_digest_binding_refuses_differs_and_not_retained() {
    for lookup in [
        DigestLookup::Differs {
            stored: Digest([7; 32]),
        },
        DigestLookup::NotRetained,
    ] {
        let mut rig = Rig::new();
        rig.p1
            .scripted_mut(NODE, P)
            .expect("scripted")
            .set_digest(Seq(5), lookup);
        let c = rig.pending_with_recheck(5);
        assert_eq!(
            rig.step(answer(Checkpoint::Publication, c, Verdict::Admit)),
            vec![ignored(AuthorityIgnoreReason::PublishPredicateFalse)],
            "{lookup:?}"
        );
        assert_eq!(rig.view().published.seq, Seq(START));
        assert!(rig.view().pending.is_some());
    }
}

/// M7A-94: `Lost` for the pending sequence cancels the outstanding check; its answer then lands
/// stale and publishes nothing.
#[retcd_test]
fn m7a_94_qualification_lost_cancels_recheck() {
    let mut rig = Rig::new();
    let c = rig.pending_with_recheck(5);
    assert_eq!(
        rig.step(lost(5)),
        vec![ignored(AuthorityIgnoreReason::QualificationLost)]
    );
    assert_eq!(rig.view().pending.expect("kept").recheck, None);
    assert_eq!(
        rig.step(answer(Checkpoint::Publication, c, Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
    assert_eq!(rig.view().published.seq, Seq(START));
}

/// M7A-96 (A-R20, B-R27): a second `Gained` while the check is outstanding asks nothing and
/// changes nothing.
#[retcd_test]
fn m7a_96_qualification_changed_on_predicate_change_only() {
    let mut rig = Rig::new();
    rig.pending_with_recheck(5);
    let before = rig.view();
    assert_eq!(
        rig.step(gained(5)),
        vec![ignored(AuthorityIgnoreReason::RecheckOutstanding)]
    );
    assert_eq!(rig.view(), before);
}

/// Edges for another sequence, and `Lost` after publication, are facts only.
#[retcd_test]
fn edges_for_other_sequences_and_after_publication_are_facts_only() {
    let mut rig = Rig::new();
    assert_eq!(
        rig.step(gained(9)),
        vec![ignored(AuthorityIgnoreReason::NotForThisCandidate)]
    );
    rig.step(candidate(5, 5));
    assert_eq!(
        rig.step(gained(6)),
        vec![ignored(AuthorityIgnoreReason::NotForThisCandidate)]
    );
    assert_eq!(
        rig.step(lost(START)),
        vec![ignored(
            AuthorityIgnoreReason::QualificationLostAfterPublish
        )]
    );
    assert_eq!(
        rig.view().published.seq,
        Seq(START),
        "publication is irreversible"
    );
}

fn moved_lineage() -> Lineage {
    Lineage {
        generation: Generation(GEN.0 + 1),
        ..lineage()
    }
}

/// An edge at the candidate's own sequence but for another lineage or configuration is not for
/// this candidate: nothing held, nothing asked.
#[retcd_test]
fn an_edge_for_another_lineage_or_configuration_is_not_for_this_candidate() {
    let mut rig = Rig::new();
    rig.step(candidate(5, 5));
    rig.qualify(5);
    for (edge_lineage, config) in [
        (moved_lineage(), CONFIG),
        (lineage(), ConfigVersion(CONFIG.0 + 1)),
    ] {
        let edge = EventKind::Kernel(KernelEvent::QualificationChanged(QualificationChanged {
            lineage: edge_lineage,
            config_version: config,
            at_seq: Seq(5),
            direction: QualificationDirection::Gained,
            qualified_copies: vec![CopyId(2)],
            qualified_ack_count: 1,
            cause: QualificationCause::AckAdvanced,
            tick: Tick::ZERO,
        }));
        assert_eq!(
            rig.step(edge),
            vec![ignored(AuthorityIgnoreReason::NotForThisCandidate)],
            "{edge_lineage:?} {config:?}"
        );
        let pending = rig.view().pending.expect("kept");
        assert!(!pending.qualifying && pending.recheck.is_none());
    }
}

/// The publish re-reads R1's lineage and configuration, not only `qualifies_now`: an R1 view on
/// another lineage or configuration publishes nothing even where it says the sequence qualifies.
#[retcd_test]
fn the_publish_rereads_r1s_lineage_and_configuration() {
    for (view_lineage, config) in [
        (moved_lineage(), CONFIG),
        (lineage(), ConfigVersion(CONFIG.0 + 1)),
    ] {
        let mut rig = Rig::new();
        let mut scripted = ScriptedReplication::new(view_lineage, config);
        scripted.set_qualifies(Seq(5), true);
        rig.p1.script_replication(NODE, P, scripted);
        rig.step(candidate(5, 5));
        assert_eq!(
            rig.step(gained(5)),
            vec![check(Checkpoint::Publication, corr(1))]
        );
        rig.snapshot_at(5);
        assert_eq!(
            rig.step(answer(Checkpoint::Publication, corr(1), Verdict::Admit)),
            vec![ignored(AuthorityIgnoreReason::PublishPredicateFalse)],
            "{view_lineage:?} {config:?}"
        );
        assert_eq!(rig.view().published.seq, Seq(START));
    }
}

// ---- authority --------------------------------------------------------------------------------

/// M7A-99: a `Publication` deny quarantines: status `Unknown`, the old-generation bytes marked,
/// the waiters drained, the partition frozen — and nothing published, replied or handed to T1.
/// The exact vector carries no `Freeze` effect (K-A-54: the self-freeze is a state write) and no
/// transaction reply. The plan's waiter code `UNKNOWN_OUTCOME` predates lead ruling A-R72a Q3,
/// which answers a waiter drained into `Frozen{AuthorityLost(Expired)}` with §3.4's pre-apply code,
/// `LeaseExpired`; the row follows the ruling.
#[retcd_test]
fn m7a_99_publication_check_deny_quarantines_freezes_authority_lost() {
    let mut rig = Rig::new();
    let c = rig.pending_with_recheck(5);
    assert_eq!(
        rig.admitted(read(21)),
        vec![],
        "a fresh read waits behind the candidate"
    );
    assert_eq!(rig.admitted(read(22)), vec![]);
    let t = rig.now + 10;
    let effects = rig.step(answer(
        Checkpoint::Publication,
        c,
        Verdict::Deny(DenyReason::Expired),
    ));
    assert_eq!(
        effects,
        vec![
            status_write(5, 5, StatusOutcome::Unknown, t),
            quarantined(5),
            // Refused with the freeze cause's §3.4 code (A-R72a Q3).
            rejected_read(21, ErrorKind::LeaseExpired),
            rejected_read(22, ErrorKind::LeaseExpired),
        ]
    );
    let view = rig.view();
    assert_eq!(
        view.mode,
        PubMode::Frozen {
            cause: FreezeCause::AuthorityLost(DenyReason::Expired)
        }
    );
    assert!(view.waiters.is_empty());
    assert_eq!(view.published.seq, Seq(START));
    assert!(view.pending.is_some(), "pending kept");
}

/// `Admit` under a moved lineage quarantines exactly as a deny, as `GenerationChanged`.
#[retcd_test]
fn an_admit_under_a_moved_lineage_quarantines() {
    let mut rig = Rig::new();
    let c = rig.pending_with_recheck(5);
    assert_eq!(rig.admitted(read(21)), vec![]);
    let mut moved = decision(P, Checkpoint::Publication, c, Verdict::Admit, 1);
    moved.grant = GrantId(2);
    let t = rig.now + 10;
    let effects = rig.step(EventKind::Kernel(KernelEvent::Authority(
        AuthorityEvent::Answer(moved),
    )));
    assert_eq!(
        effects,
        vec![
            status_write(5, 5, StatusOutcome::Unknown, t),
            quarantined(5),
            rejected_read(21, ErrorKind::GenerationChanged),
        ],
        "the freeze drains the queue exactly as a deny's does"
    );
    assert_eq!(
        rig.view().mode,
        PubMode::Frozen {
            cause: FreezeCause::AuthorityLost(DenyReason::GenerationChanged)
        }
    );
    assert_eq!(rig.view().published.seq, Seq(START));
}

/// Invariant 3: a deny at `Reply` withholds the reply and undoes nothing.
#[retcd_test]
fn a_reply_deny_withholds_the_reply_and_publication_stands() {
    let mut rig = Rig::new();
    let c = rig.published(5);
    assert_eq!(
        rig.step(answer(
            Checkpoint::Reply,
            c,
            Verdict::Deny(DenyReason::Expired)
        )),
        vec![ignored(AuthorityIgnoreReason::ReplyWithheld)]
    );
    let view = rig.view();
    assert!(view.awaiting_reply.is_empty());
    assert_eq!(view.published.seq, Seq(5));
    assert_eq!(view.status.lookup(req(5), GEN), published_status(5));
    assert_eq!(
        rig.admitted(read_previous(30)),
        vec![previous(30, Ok(snap(1)))],
        "the old prefix is still readable at 5"
    );
}

/// M7A-157 (K-A-41): newer views replace, older ones are facts; answers are judged against the
/// held view. The plan's `Admit{4}` ⇒ `StaleAuthorityAnswer` holds, and since lead ruling A-R69 F2
/// the check it answered is also asked again under a fresh correlation, which the row asserts.
#[retcd_test]
fn m7a_157_p1_authority_view_newer_adopted_older_dropped() {
    let mut rig = Rig::new();
    let c = rig.pending_with_recheck(5);
    assert_eq!(rig.step(view_push(P, 5)), vec![]);
    assert_eq!(rig.view().authority, Some(authority_view(5)), "adopted");
    assert_eq!(
        rig.step(view_push(P, 4)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityView)]
    );
    assert_eq!(
        rig.view().authority,
        Some(authority_view(5)),
        "the older view is dropped"
    );
    // A-R69 F2: dropped, and the check it answered is asked again under a fresh correlation.
    assert_eq!(
        rig.step(answer_at(Checkpoint::Publication, c, Verdict::Admit, 4)),
        vec![
            ignored(AuthorityIgnoreReason::StaleAuthorityAnswer),
            check(Checkpoint::Publication, corr(2)),
        ]
    );
    assert!(published_any(&rig.step(answer_at(
        Checkpoint::Publication,
        corr(2),
        Verdict::Admit,
        5
    ))));
}

/// A-R69 F2 (tester PROBE_D): A1 resyncs while the `Publication` check is in flight, so its
/// answer lands older than the view P1 now holds. P1 drops it — neither a publish nor a
/// quarantine — and asks again; the re-asked check is the one that publishes. Without the re-ask
/// every later `Gained` is `RecheckOutstanding` and only the deadline ends it.
#[retcd_test]
fn a_stale_publication_answer_re_asks_the_check_it_answered() {
    for verdict in [Verdict::Admit, Verdict::Deny(DenyReason::Expired)] {
        let mut rig = Rig::new();
        let c = rig.pending_with_recheck(5);
        assert_eq!(rig.step(view_push(P, 2)), vec![]);
        let before = rig.view();
        assert_eq!(
            rig.step(answer_at(Checkpoint::Publication, c, verdict, 1)),
            vec![
                ignored(AuthorityIgnoreReason::StaleAuthorityAnswer),
                check(Checkpoint::Publication, corr(2)),
            ],
            "{verdict:?}"
        );
        let view = rig.view();
        assert_eq!(
            view.pending.expect("kept").recheck,
            Some(corr(2)),
            "{verdict:?}"
        );
        assert_eq!(
            (view.mode, view.published),
            (before.mode, before.published),
            "{verdict:?}: the stale answer is never acted on"
        );
        assert_eq!(
            rig.step(gained(5)),
            vec![ignored(AuthorityIgnoreReason::RecheckOutstanding)]
        );

        rig.snapshot_at(5);
        assert!(published_any(&rig.step(answer_at(
            Checkpoint::Publication,
            corr(2),
            Verdict::Admit,
            2
        ))));
        // The first answer arriving again now matches nothing: a fact, and no third check.
        assert_eq!(
            rig.step(answer_at(Checkpoint::Publication, c, Verdict::Admit, 1)),
            vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
        );
    }
}

/// A-R69 F2, the `Reply` side: a stale `Reply` answer moves the owed reply to a fresh check
/// rather than leaking it, and that check's answer replies.
#[retcd_test]
fn a_stale_reply_answer_re_asks_and_the_reply_still_arrives() {
    let mut rig = Rig::new();
    let c = rig.published(5);
    assert_eq!(rig.step(view_push(P, 2)), vec![]);
    assert_eq!(
        rig.step(answer_at(Checkpoint::Reply, c, Verdict::Admit, 1)),
        vec![
            ignored(AuthorityIgnoreReason::StaleAuthorityAnswer),
            check(Checkpoint::Reply, corr(3)),
        ]
    );
    assert_eq!(
        rig.view()
            .awaiting_reply
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![corr(3)]
    );
    assert_eq!(
        rig.step(answer_at(Checkpoint::Reply, corr(3), Verdict::Admit, 2)),
        vec![txn_reply(5, 5)]
    );
    assert!(rig.view().awaiting_reply.is_empty());

    // A stale answer for a correlation nobody asked is a fact and changes nothing.
    let before = rig.view();
    assert_eq!(
        rig.step(answer_at(Checkpoint::Reply, corr(77), Verdict::Admit, 1)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
    assert_eq!(rig.view(), before);
}

/// N03 (A-R71): a re-ask is exact. A duplicate of the stale answer that was already re-asked
/// answers a check no longer in flight, so it is dropped: no third check, and the check in flight
/// is kept and still decides. Both checkpoints.
#[retcd_test]
fn a_duplicate_stale_answer_keeps_the_check_in_flight() {
    // `Publication`: stale c1 → re-ask c2 → stale c1 again.
    let mut rig = Rig::new();
    let c1 = rig.pending_with_recheck(5);
    assert_eq!(rig.step(view_push(P, 2)), vec![]);
    let stale = answer_at(Checkpoint::Publication, c1, Verdict::Admit, 1);
    assert_eq!(
        rig.step(stale.clone()),
        vec![
            ignored(AuthorityIgnoreReason::StaleAuthorityAnswer),
            check(Checkpoint::Publication, corr(2)),
        ]
    );
    assert_eq!(
        rig.step(stale),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)],
        "the duplicate asks nothing"
    );
    assert_eq!(rig.view().pending.expect("kept").recheck, Some(corr(2)));
    rig.snapshot_at(5);
    assert!(published_any(&rig.step(answer_at(
        Checkpoint::Publication,
        corr(2),
        Verdict::Admit,
        2
    ))));

    // `Reply`: stale c → re-ask c' → stale c again.
    let mut rig = Rig::new();
    let c = rig.published(5);
    assert_eq!(rig.step(view_push(P, 2)), vec![]);
    let stale = answer_at(Checkpoint::Reply, c, Verdict::Admit, 1);
    assert_eq!(
        rig.step(stale.clone()),
        vec![
            ignored(AuthorityIgnoreReason::StaleAuthorityAnswer),
            check(Checkpoint::Reply, corr(3)),
        ]
    );
    assert_eq!(
        rig.step(stale),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)],
        "the duplicate asks nothing"
    );
    assert_eq!(
        rig.view()
            .awaiting_reply
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![corr(3)]
    );
    assert_eq!(
        rig.step(answer_at(Checkpoint::Reply, corr(3), Verdict::Admit, 2)),
        vec![txn_reply(5, 5)]
    );
}

/// With no view held, no answer is ours (`answer_is_ours`, design §1.2).
#[retcd_test]
fn with_no_view_held_no_answer_is_ours() {
    let mut rig = Rig::bare();
    rig.step(candidate(5, 5));
    rig.qualify(5);
    rig.step(gained(5));
    assert_eq!(
        rig.step(answer(Checkpoint::Publication, corr(1), Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
}

/// M7A-156 (K-A-41): A1's fence reaches P1 — at the fence, not at the deadline: waiters drained
/// (with §3.4's code, A-R72a Q3), owed replies withheld, the partition frozen, the candidate kept
/// with its recheck cleared. Twin: a fence for another partition is answered `Ignored(NotOurs)`
/// and touches nothing (A-R84; it was refused before M7A-189).
#[retcd_test]
fn m7a_156_p1_freeze_drains_waiters_withholds_awaiting_replies_sets_frozen() {
    let mut rig = Rig::new();
    rig.published(5);
    rig.pending_with_recheck(6);
    rig.admitted(read(21));
    rig.admitted(read(22));

    let before = rig.view();
    assert_eq!(
        rig.step(fence(FenceScope::Partition(P2), DenyReason::Expired)),
        vec![ignored(AuthorityIgnoreReason::NotOurs)],
        "another partition's fence is not ours (A-R84)"
    );
    assert_eq!(rig.view(), before);

    assert_eq!(
        rig.step(fence(FenceScope::Node, DenyReason::Expired)),
        vec![
            rejected_read(21, ErrorKind::LeaseExpired),
            rejected_read(22, ErrorKind::LeaseExpired),
            ignored(AuthorityIgnoreReason::ReplyWithheld),
        ]
    );
    let view = rig.view();
    assert_eq!(
        view.mode,
        PubMode::Frozen {
            cause: FreezeCause::AuthorityLost(DenyReason::Expired)
        }
    );
    assert!(view.awaiting_reply.is_empty() && view.waiters.is_empty());
    let pending = view.pending.expect("kept");
    assert_eq!((pending.request, pending.recheck), (req(6), None));

    let mut storage = Rig::new();
    storage.step(fence(
        FenceScope::Partition(P),
        DenyReason::LocalStorageFenced,
    ));
    assert_eq!(
        storage.view().mode,
        PubMode::Frozen {
            cause: FreezeCause::LocalStorageFenced
        }
    );
}

/// Invariant 3: a fence or a block after publication un-publishes nothing. The published
/// position, the status and the kept old-prefix view all stand (A-R69a). The mode refuses a
/// previous read over it meanwhile, as it refuses a fresh one (A-R72c): the view is kept, not
/// served.
#[retcd_test]
fn a_fence_or_block_after_publication_unpublishes_nothing() {
    for (entry, code) in [
        (
            fence(FenceScope::Node, DenyReason::Expired),
            ErrorKind::LeaseExpired,
        ),
        (
            fence(FenceScope::Partition(P), DenyReason::LocalStorageFenced),
            ErrorKind::ProtectionPaused,
        ),
        (
            EventKind::Kernel(KernelEvent::BlockPartition(block())),
            ErrorKind::ProtectionPaused,
        ),
    ] {
        let mut rig = Rig::new();
        rig.published(5);
        rig.pending_with_recheck(6);
        rig.step(entry.clone());
        let view = rig.view();
        assert_eq!(
            view.published,
            PublishedAt {
                generation: GEN,
                seq: Seq(5)
            },
            "{entry:?}"
        );
        assert_eq!(
            view.status.lookup(req(5), GEN),
            published_status(5),
            "{entry:?}"
        );
        assert_eq!(view.kept, Some(snap(1)), "{entry:?}");
        assert_eq!(
            rig.step(read_previous(30)),
            vec![previous(30, Err(code))],
            "{entry:?}"
        );
    }
}

// ---- the deadline and exactly one reply -------------------------------------------------------

/// Invariant 4: the deadline answers `Unknown` once and freezes only its own partition, and it
/// never erases an existing freeze's cause.
#[retcd_test]
fn the_post_apply_deadline_replies_unknown_and_freezes_only_its_partition() {
    let mut rig = Rig::new();
    assert_eq!(
        rig.p1.install(NODE, BOOT, lineage_of(P2), Seq(START)),
        EffectKind::Store(StoreEffect::Snapshot {
            handle: snap_of(P2, 0),
            partition: P2,
        })
    );
    rig.step_on(P2, view_push(P2, 1));
    rig.step_on(
        P2,
        EventKind::Kernel(KernelEvent::AppliedCandidate(Box::new(candidate_of(
            P2, 5, 50,
        )))),
    );
    rig.step(candidate(5, 5));
    rig.admitted(read(21));
    let t = rig.now + 10;
    assert_eq!(
        rig.step(deadline(P, 1)),
        vec![
            status_write(5, 5, StatusOutcome::Unknown, t),
            unknown_reply(5),
            rejected_read(21, ErrorKind::UnknownOutcome),
        ]
    );
    let view = rig.view();
    assert_eq!(
        view.mode,
        PubMode::Frozen {
            cause: FreezeCause::UnresolvedTransaction
        }
    );
    assert!(view.pending.expect("kept").replied);
    assert_eq!(rig.p1.view(NODE, P2).expect("p2").mode, PubMode::Serving);

    // Entered frozen for another cause: one reply, cause kept — for every fence cause.
    for (reason, cause) in [
        (
            DenyReason::Expired,
            FreezeCause::AuthorityLost(DenyReason::Expired),
        ),
        (
            DenyReason::LocalStorageFenced,
            FreezeCause::LocalStorageFenced,
        ),
    ] {
        let mut frozen = Rig::new();
        frozen.step(candidate(5, 5));
        frozen.step(fence(FenceScope::Node, reason));
        let effects = frozen.step(deadline(P, 1));
        assert_eq!(replies_for(&effects, 5), 1, "{reason:?}");
        assert_eq!(frozen.view().mode, PubMode::Frozen { cause }, "{reason:?}");
    }
}

/// Invariant 6: the deadline replies once. A second firing at the same version is a stale timer.
#[retcd_test]
fn a_duplicate_deadline_replies_once() {
    let mut rig = Rig::new();
    rig.step(candidate(5, 5));
    let mut all = rig.step(deadline(P, 1));
    let again = rig.step(deadline(P, 1));
    assert_eq!(again, vec![ignored(AuthorityIgnoreReason::StaleTimer)]);
    all.extend(again);
    assert_eq!(replies_for(&all, 5), 1, "{all:?}");
}

/// The deadline answered request 5 `Unknown` (`Frozen{UnresolvedTransaction}`); then 5 qualifies
/// and publishes. Returns every effect so far and the `Reply` check's correlation. Shared by the
/// twins M7A-103 (the publish half) and M7A-154 (the reply half).
fn published_after_the_deadline(rig: &mut Rig) -> (Vec<EffectKind>, CorrelationId) {
    let mut all = rig.step(candidate(5, 5));
    all.extend(rig.step(deadline(P, 1)));
    assert!(rig.view().pending.expect("kept").replied);
    rig.qualify(5);
    let asked = rig.step(gained(5));
    let c = publication_check(&asked);
    all.extend(asked);
    rig.snapshot_at(5);
    let effects = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
    let r = reply_check(&effects);
    all.extend(effects);
    (all, r)
}

/// M7A-103, invariant 6: the deadline replied `Unknown`; the late publish still publishes, reopens
/// the partition (K-A-47) and tells T1 — and replies to nobody a second time.
#[retcd_test]
fn m7a_103_post_apply_deadline_then_late_qualification_no_second_reply() {
    let mut rig = Rig::new();
    let (mut all, r) = published_after_the_deadline(&mut rig);
    assert!(published_any(&all));
    assert_eq!(rig.view().published.seq, Seq(5));
    assert_eq!(
        rig.view().mode,
        PubMode::Serving,
        "K-A-47: resolving it reopens"
    );
    assert!(rig.view().awaiting_reply[&r].replied);
    let effects = rig.step(answer(Checkpoint::Reply, r, Verdict::Admit));
    assert_eq!(
        effects,
        vec![ignored(AuthorityIgnoreReason::ReplySuppressedAfterTimeout)]
    );
    all.extend(effects);
    assert_eq!(replies_for(&all, 5), 1, "{all:?}");
    assert_eq!(rig.view().status.lookup(req(5), GEN), published_status(5));
}

/// Publication cancels the deadline; a firing that races it is a stale timer, not a reply.
#[retcd_test]
fn a_deadline_that_fires_after_publication_is_a_stale_timer() {
    let mut rig = Rig::new();
    rig.published(5);
    assert_eq!(
        rig.step(deadline(P, 1)),
        vec![ignored(AuthorityIgnoreReason::StaleTimer)]
    );
    assert_eq!(rig.view().mode, PubMode::Serving);
}

/// M7A-155: two published candidates await their answers at once (a map, not an `Option`); each
/// answer replies to its own identity, and the map empties.
#[retcd_test]
fn m7a_155_delayed_reply_answer_outlives_next_candidate_publish() {
    let mut rig = Rig::new();
    let c5 = rig.published(5);
    let c6 = rig.published(6);
    assert_eq!(rig.view().awaiting_reply.len(), 2);
    assert_eq!(
        rig.step(answer(Checkpoint::Reply, c5, Verdict::Admit)),
        vec![txn_reply(5, 5)]
    );
    assert_eq!(
        rig.step(answer(Checkpoint::Reply, c6, Verdict::Admit)),
        vec![txn_reply(6, 6)]
    );
    assert!(rig.view().awaiting_reply.is_empty());
}

// ---- modes ------------------------------------------------------------------------------------

/// K-A-47 and test plan Q-6: which frozen causes may publish, and which reopen.
#[retcd_test]
fn frozen_causes_gate_publication_and_only_the_unresolved_one_reopens() {
    // (a) Frozen{UnresolvedTransaction}: publishes and reopens. Covered above; one line here.
    let mut rig = Rig::new();
    rig.step(candidate(5, 5));
    rig.step(deadline(P, 1));
    rig.qualify(5);
    rig.step(gained(5));
    rig.step(answer(Checkpoint::Publication, corr(1), Verdict::Admit));
    assert_eq!(rig.view().mode, PubMode::Serving);

    // (b), (c): publishes, mode unchanged.
    for reason in [DenyReason::Expired, DenyReason::LocalStorageFenced] {
        let mut rig = Rig::new();
        rig.step(candidate(5, 5));
        rig.step(fence(FenceScope::Node, reason));
        let entry = rig.view().mode;
        rig.qualify(5);
        rig.step(gained(5));
        let effects = rig.step(answer(Checkpoint::Publication, corr(1), Verdict::Admit));
        assert!(published_any(&effects), "{reason:?}: {effects:?}");
        assert_eq!(rig.view().mode, entry, "{reason:?}");
    }

    // (d) Frozen{RecoveryReadOnly}: deferred. `Recovered` clears the candidate and no wire
    // event freezes read-only with one pending, so this row drives `PubKernel` directly: it
    // is the kernel's own guard, not a reachable trace.
    let mut kernel = PubKernel::new(Default::default(), BOOT, lineage(), Seq(START));
    let mut view = ScriptedReplication::new(lineage(), CONFIG);
    view.set_qualifies(Seq(5), true);
    let gained_edge = || match gained(5) {
        EventKind::Kernel(KernelEvent::QualificationChanged(q)) => q,
        _ => unreachable!(),
    };
    kernel.apply(
        Tick(1),
        PubEvent::AuthorityView(authority_view(1)),
        Some(&view),
    );
    kernel.apply(
        Tick(2),
        PubEvent::Candidate(candidate_of(P, 5, 5)),
        Some(&view),
    );
    kernel.apply(
        Tick(3),
        PubEvent::Freeze {
            cause: FreezeCause::RecoveryReadOnly,
        },
        Some(&view),
    );
    let asked = kernel.apply(
        Tick(4),
        PubEvent::QualificationChanged(gained_edge()),
        Some(&view),
    );
    let c = match asked.as_slice() {
        [PubEffect::AuthorityCheck { correlation, .. }] => *correlation,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        kernel.apply(
            Tick(5),
            PubEvent::AuthorityAnswer(decision(P, Checkpoint::Publication, c, Verdict::Admit, 1)),
            Some(&view),
        ),
        vec![PubEffect::Fact(PubFact::PublishDeferred)]
    );
    assert_eq!(kernel.published().seq, Seq(START));
    assert_eq!(
        kernel.view().mode,
        PubMode::Frozen {
            cause: FreezeCause::RecoveryReadOnly
        }
    );
}

fn authority_view(authority_seq: u64) -> AuthorityView {
    match view_push(P, authority_seq) {
        EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::View(v))) => v,
        _ => unreachable!(),
    }
}

/// B-R29: a block drains readers, leaves owed replies alone, refuses fresh reads and the
/// publish, survives the deadline and a fence, and answers `ModeQuery` as `Blocked`.
#[retcd_test]
fn blocked_is_sticky_and_refuses_reads_and_publish() {
    let mut rig = Rig::new();
    let c5 = rig.published(5);
    let c6 = rig.pending_with_recheck(6);
    rig.admitted(read(21));
    let reason = block();
    assert_eq!(
        rig.step(EventKind::Kernel(KernelEvent::BlockPartition(
            reason.clone()
        ))),
        vec![
            ignored(AuthorityIgnoreReason::Blocked {
                reason: reason.clone()
            }),
            rejected_read(21, ErrorKind::ProtectionPaused),
        ]
    );
    let blocked = PubMode::Blocked {
        reason: reason.clone(),
    };
    assert_eq!(rig.view().mode, blocked);
    assert!(
        rig.view().awaiting_reply.contains_key(&c5),
        "owed reply untouched"
    );

    assert_eq!(
        rig.step(read(22)),
        vec![rejected_read(22, ErrorKind::ProtectionPaused)]
    );
    assert_eq!(
        rig.step(answer(Checkpoint::Publication, c6, Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::PublishRefusedBlocked)]
    );
    assert_eq!(rig.view().published.seq, Seq(5));
    let effects = rig.step(deadline(P, 2));
    assert_eq!(replies_for(&effects, 6), 1);
    assert_eq!(rig.view().mode, blocked, "the deadline does not unblock");
    assert_eq!(
        rig.step(EventKind::Kernel(KernelEvent::Publication(
            PublicationEvent::ModeQuery { identity: req(40) }
        ))),
        vec![EffectKind::Kernel(KernelEffect::Publication(
            PublicationEffect::Mode {
                identity: req(40),
                mode: blocked.clone(),
            }
        ))]
    );
    assert_eq!(
        rig.step(EventKind::Kernel(KernelEvent::BlockPartition(reason))),
        vec![ignored(AuthorityIgnoreReason::AlreadyBlocked)]
    );
    assert_eq!(
        rig.step(fence(FenceScope::Node, DenyReason::Expired)),
        vec![
            ignored(AuthorityIgnoreReason::ReplyWithheld),
            ignored(AuthorityIgnoreReason::FenceWhileBlocked),
        ]
    );
    assert_eq!(
        rig.view().mode,
        blocked,
        "a fence does not downgrade a block"
    );
}

/// K-A-57 / K-A-48: in `Blocked`, an `Admit` under a moved lineage quarantines — it is not
/// swallowed by the refusal row — and the block survives it.
/// A plain `Deny` in `Blocked` quarantines the same way, and does not unblock either.
#[retcd_test]
fn a_blocked_refusal_does_not_swallow_a_lineage_move() {
    let deny = |c: CorrelationId| {
        decision(
            P,
            Checkpoint::Publication,
            c,
            Verdict::Deny(DenyReason::Expired),
            1,
        )
    };
    let moved = |c: CorrelationId| {
        let mut moved = decision(P, Checkpoint::Publication, c, Verdict::Admit, 1);
        moved.lineage.generation = Generation(GEN.0 + 1);
        moved
    };
    let answers: [fn(CorrelationId) -> AuthorityDecision; 2] = [moved, deny];
    for make in answers {
        let mut rig = Rig::new();
        let c = rig.pending_with_recheck(7);
        rig.step(EventKind::Kernel(KernelEvent::BlockPartition(block())));
        let answer = make(c);
        let t = rig.now + 10;
        assert_eq!(
            rig.step(EventKind::Kernel(KernelEvent::Authority(
                AuthorityEvent::Answer(answer)
            ))),
            vec![
                status_write(7, 7, StatusOutcome::Unknown, t),
                quarantined(7)
            ],
            "{answer:?}"
        );
        assert_eq!(
            rig.view().mode,
            PubMode::Blocked { reason: block() },
            "{answer:?}"
        );
    }
}

/// B-R29: a block overrides every freeze — lost authority, fenced storage, the deadline's own,
/// and a read-only recovery (tester-p1 F6d).
#[retcd_test]
fn a_block_overrides_every_freeze() {
    let entries: [fn(&mut Rig); 4] = [
        |rig| {
            rig.published(5);
            rig.step(recovered(PartitionMode::ReadOnly, 5, false, None));
        },
        |rig| {
            rig.step(fence(FenceScope::Node, DenyReason::Expired));
        },
        |rig| {
            rig.step(fence(
                FenceScope::Partition(P),
                DenyReason::LocalStorageFenced,
            ));
        },
        |rig| {
            rig.step(candidate(5, 5));
            rig.step(deadline(P, 1));
        },
    ];
    for enter in entries {
        let mut rig = Rig::new();
        enter(&mut rig);
        let frozen = rig.view().mode;
        assert!(matches!(frozen, PubMode::Frozen { .. }), "{frozen:?}");
        assert_eq!(
            rig.step(EventKind::Kernel(KernelEvent::BlockPartition(block()))),
            vec![ignored(AuthorityIgnoreReason::Blocked { reason: block() })],
            "{frozen:?}"
        );
        assert_eq!(
            rig.view().mode,
            PubMode::Blocked { reason: block() },
            "{frozen:?}"
        );
        assert_eq!(
            rig.step(read(21)),
            vec![rejected_read(21, ErrorKind::ProtectionPaused)],
            "{frozen:?}"
        );
    }
}

/// K-A-45: a candidate arriving in a lost-authority freeze or a block is accepted (deadline
/// armed, status `Unknown`), mode unchanged; one arriving where none can is a fact only.
#[retcd_test]
fn a_candidate_while_not_serving_is_accepted_and_the_unreachable_arm_says_so() {
    for entry in [
        fence(FenceScope::Node, DenyReason::Expired),
        fence(FenceScope::Node, DenyReason::LocalStorageFenced),
        EventKind::Kernel(KernelEvent::BlockPartition(block())),
    ] {
        let mut rig = Rig::new();
        rig.step(entry);
        let mode = rig.view().mode;
        let effects = rig.step(candidate(7, 7));
        assert_eq!(effects.len(), 3, "{effects:?}");
        assert!(matches!(
            effects[0],
            EffectKind::Timer(TimerEffect::Arm { .. })
        ));
        assert_eq!(
            effects[2],
            ignored(AuthorityIgnoreReason::CandidateWhileNotServing)
        );
        assert_eq!(rig.view().mode, mode);
        assert_eq!(rig.view().pending.expect("accepted").request, req(7));
    }
    // A second candidate while one is pending.
    let mut rig = Rig::new();
    rig.step(candidate(5, 5));
    assert_eq!(
        rig.step(candidate(6, 6)),
        vec![ignored(AuthorityIgnoreReason::CandidateUnreachable)]
    );
    assert_eq!(rig.view().pending.expect("first kept").request, req(5));
}

/// A-R66 (2): a candidate from a lineage P1 does not hold is unreachable. It arms no deadline,
/// writes no status, is never pending, and does not block the next candidate of P1's own lineage.
#[retcd_test]
fn a_candidate_from_a_foreign_lineage_is_unreachable_and_leaves_nothing_behind() {
    for moved in [
        Lineage {
            generation: Generation(GEN.0 + 1),
            ..lineage()
        },
        Lineage {
            owner_epoch: OwnerEpoch(EPOCH.0 + 1),
            ..lineage()
        },
    ] {
        let mut rig = Rig::new();
        let before = rig.view();
        let mut foreign = candidate_of(P, 5, 5);
        foreign.lineage = moved;
        assert_eq!(
            rig.step(EventKind::Kernel(KernelEvent::AppliedCandidate(Box::new(
                foreign
            )))),
            vec![ignored(AuthorityIgnoreReason::CandidateUnreachable)],
            "{moved:?}"
        );
        assert_eq!(rig.view(), before, "{moved:?}: nothing recorded");

        let t = rig.now + 10;
        assert_eq!(
            rig.step(candidate(5, 5)),
            vec![
                arm(1, t + DEADLINE),
                status_write(5, 5, StatusOutcome::Unknown, t)
            ],
            "{moved:?}: P1's own candidate is still accepted"
        );
    }
}

// ---- recovery ---------------------------------------------------------------------------------

fn recovered(
    mode: PartitionMode,
    cutoff: u64,
    uncertain: bool,
    discarded_from: Option<u64>,
) -> EventKind {
    let next = Lineage {
        partition: P,
        generation: Generation(GEN.0 + 1),
        owner_epoch: OwnerEpoch(EPOCH.0 + 1),
    };
    let cutoff = Seq(cutoff);
    let pinned = PartitionConfig::new(P, CONFIG, Vec::new());
    EventKind::Kernel(KernelEvent::Recovered(Box::new(RecoveryResult {
        fenced_prior: FencingProof {
            partition: P,
            prior_generation: GEN,
            prior_owner_epoch: EPOCH,
            prior_grant_id: GrantId(1),
            prior_boot_id: BOOT,
            revocation: Revocation::DurableDrain {
                ack_revision: Revision(1),
            },
            control_revision: Revision(1),
            decision_tick: Tick::ZERO,
        },
        inventories: Vec::new(),
        selected: SelectedLineage {
            root: next,
            cutoff_seq: cutoff,
            cutoff_digest: Digest::ROOT,
            source: CopyId(1),
        },
        new_generation: next.generation,
        mode,
        barrier: RecoveryBarrier::try_new(&[], &Default::default(), cutoff, Digest::ROOT)
            .expect("an empty required set needs no proof"),
        loss: LossRecord {
            queried: Vec::new(),
            unavailable: Vec::new(),
            cutoff_seq: cutoff,
            highest_advertised_seq: cutoff,
            uncertain,
        },
        committed: CommittedRoot {
            revision: Revision(2),
            authority_view: authority_view(2),
            pinned_config: pinned,
        },
        retained_status_map: RetainedStatusMap {
            predecessor_generation: GEN,
            predecessor_cutoff: cutoff,
            retained_through: cutoff,
            discarded_from: discarded_from.map(Seq),
            uncertain,
        },
    })))
}

/// `Recovered` maps every shared `PartitionMode`, withholds owed replies, and rebases.
#[retcd_test]
fn recovered_maps_every_partition_mode_and_withholds_owed_replies() {
    for (mode, expected) in [
        (PartitionMode::Active, PubMode::Serving),
        (PartitionMode::DegradedRf2, PubMode::Serving),
        (
            PartitionMode::ReadOnly,
            PubMode::Frozen {
                cause: FreezeCause::RecoveryReadOnly,
            },
        ),
        (
            PartitionMode::Blocked { reason: block() },
            PubMode::Blocked { reason: block() },
        ),
    ] {
        let mut rig = Rig::new();
        rig.published(5);
        rig.step(EventKind::Kernel(KernelEvent::BlockPartition(block())));
        assert_eq!(
            rig.step(recovered(mode.clone(), 5, false, None)),
            vec![
                ignored(AuthorityIgnoreReason::ReplyWithheld),
                release(1),
                open(2)
            ],
            "{mode:?}"
        );
        let view = rig.view();
        assert_eq!(view.mode, expected, "{mode:?}");
        assert_eq!(view.lineage.generation, Generation(GEN.0 + 1));
        assert_eq!(view.published.seq, Seq(5));
        assert!(view.pending.is_none() && view.awaiting_reply.is_empty());
    }
}

/// The F1/T1/P1 row spike §6 makes mandatory, through `Module::step`. One `Recovered` with a
/// cutoff below the published position: the status folds, the position rebases to the cutoff in
/// the new generation, the pending candidate is dropped, the queued reader is answered, and the
/// kept old-prefix view is released and one at the cutoff asked for. `ReadOnly` answers the
/// reader at the cutoff too: it refuses writes, never reads (A-R71 F6).
#[retcd_test]
fn recovered_folds_rebases_drops_the_candidate_and_answers_its_waiters() {
    let next = Generation(GEN.0 + 1);
    let reader = read_reply(21, ReadServiceOutcome::WaitedAtBarrier, Some(value_at(5)));
    for mode in [PartitionMode::Active, PartitionMode::ReadOnly] {
        let reader = reader.clone();
        let mut rig = Rig::new();
        let c5 = rig.published(5);
        assert_eq!(
            rig.step(answer(Checkpoint::Reply, c5, Verdict::Admit)),
            vec![txn_reply(5, 5)]
        );
        rig.published(6);
        rig.pending_with_recheck(7);
        assert_eq!(rig.admitted(read(21)), vec![]);

        // Storage now serves the recovered lineage at its cutoff.
        rig.snapshot.generation = next;
        rig.snapshot_at(5);
        assert_eq!(
            rig.step(recovered(mode.clone(), 5, false, Some(6))),
            vec![
                ignored(AuthorityIgnoreReason::ReplyWithheld),
                release(2),
                open(3),
                reader
            ],
            "{mode:?}"
        );
        let view = rig.view();
        assert_eq!(
            view.published,
            PublishedAt {
                generation: next,
                seq: Seq(5)
            },
            "{mode:?}: rebased to the cutoff"
        );
        assert!(view.pending.is_none(), "{mode:?}: the candidate is dropped");
        assert!(view.waiters.is_empty() && view.awaiting_reply.is_empty());
        assert_eq!(view.kept, None, "{mode:?}");

        let applied = TxnResult {
            outcome: Outcome::RecoveredApplied,
            ..result(P, 5)
        };
        for (n, want) in [
            (5, TxnStatus::Resolved(applied)),
            (6, TxnStatus::Unknown),
            (7, TxnStatus::Unknown),
        ] {
            assert_eq!(
                rig.step(status(n, Some(GEN))),
                vec![EffectKind::Reply(ReplyEffect::Status {
                    identity: req(n),
                    status: want,
                })],
                "{mode:?}: r{n} folds by its own sequence"
            );
        }
        assert_eq!(
            rig.admitted(read_previous(30)),
            vec![previous(30, Err(ErrorKind::Unavailable))],
            "{mode:?}: the predecessor's view is gone, the cutoff's not yet bound"
        );
        assert_eq!(rig.step(ready(3, 5)), vec![], "{mode:?}");
        assert_eq!(
            rig.admitted(read_previous(31)),
            vec![previous(31, Ok(snap(3)))],
            "{mode:?}"
        );
    }
}

/// This test's own log lines whose message is `message`. Reads the per-test JSONL file the
/// `#[retcd_test]` layer writes, the way `config_testkit::logs::lines_for_current_test` does;
/// rdb-core has no `config-testkit` dev-dependency, and `config-log` is enough.
fn own_log_lines(method: &str, message: &str) -> Vec<serde_json::Value> {
    let path = config_log::layer::test_file_path(
        &config_log::testing::test_log_dir(),
        module_path!(),
        method,
    );
    let run = config_log::testing::test_run_id();
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read test log {}: {e}", path.display()))
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .unwrap_or_else(|e| panic!("malformed JSONL line in {}: {e}", path.display()))
        })
        .filter(|line| line["testRun"].as_str() == Some(run) && line["@m"] == message)
        .collect()
}

/// M7A-194's unit twin: the status seed on a kernel that recorded nothing for the predecessor.
/// The rig recovers `P` from `GEN` into `GEN + 1` with `retained_through = 6` while the snapshot's
/// dedup namespace holds gen `GEN`'s rows for requests 4, 5, 6 and 7 (at seq 4, 5, 6 and 7) — the
/// rows `DedupIndex::seed` loads for T1 — and one row that does not decode, which the seed skips
/// (A-R90). While the snapshot shows only seq 5 nothing is loaded and
/// the seed stays owed; once it shows seq 6 the next step loads it: 4, 5 and 6 (the cut itself)
/// answer `RecoveredApplied` with the published result, asked with and without the generation; 7
/// is above the cut and stays `Unknown`; under an uncertain map every seeded row answers
/// `Unknown`, as `fold_recovered` answers for a held one. What the kernel recorded itself is not
/// overwritten: request 5, published here before the recovery, folds through `fold_recovered` and
/// keeps its own `record_digest`, while seeded requests 4 and 6 have none.
#[retcd_test]
fn m7a_194_status_seed_loads_the_predecessors_durable_rows() {
    let next = Generation(GEN.0 + 1);
    let row = |n: u64| {
        (
            dedup_key(GEN, AffinityId(1), req(n)),
            dedup_value(Digest::ROOT, Seq(n), EPOCH),
        )
    };
    let status_reply = |n: u64, status: TxnStatus| {
        vec![EffectKind::Reply(ReplyEffect::Status {
            identity: req(n),
            status,
        })]
    };
    let applied = |n: u64| {
        TxnStatus::Resolved(TxnResult {
            outcome: Outcome::RecoveredApplied,
            ..result(P, n)
        })
    };
    for uncertain in [false, true] {
        let mut rig = Rig::new();
        rig.published(5);
        rig.snapshot.dedup = vec![
            row(4),
            row(5),
            row(6),
            row(7),
            (
                Bytes::from_static(&[1, 2, 3]),
                Bytes::from_static(&[4, 5, 6]),
            ),
        ];
        // The recovery lands while storage shows only seq 5: the seed is owed, not loaded.
        rig.snapshot.generation = next;
        rig.step(recovered(PartitionMode::Active, 6, uncertain, None));
        let seed = rig
            .p1
            .kernel(NODE, P)
            .expect("P1 holds P")
            .seed_pending()
            .expect("the seed is owed until the snapshot shows seq 5");
        assert_eq!(
            (seed.serving, seed.retained_through, seed.map.uncertain),
            (next, Seq(6), uncertain),
            "uncertain {uncertain}: the seed names the served generation and the cut"
        );
        assert_eq!(
            rig.step(status(4, Some(GEN))),
            status_reply(4, TxnStatus::Unknown),
            "uncertain {uncertain}: before the seed an identity this kernel never recorded is Unknown"
        );

        rig.snapshot_at(6);
        let want = |n: u64| {
            if uncertain {
                TxnStatus::Unknown
            } else {
                applied(n)
            }
        };
        for (n, generation, want) in [
            (4, Some(GEN), want(4)),
            (5, Some(GEN), want(5)),
            (5, None, want(5)),
            (6, Some(GEN), want(6)),
            (6, None, want(6)),
            (7, Some(GEN), TxnStatus::Unknown),
        ] {
            assert_eq!(
                rig.step(status(n, generation)),
                status_reply(n, want),
                "uncertain {uncertain}: r{n} asked with {generation:?}, once the snapshot shows the cut"
            );
        }
        let kernel = rig.p1.kernel(NODE, P).expect("P1 holds P");
        assert!(
            kernel.seed_pending().is_none(),
            "uncertain {uncertain}: loaded"
        );
        let view = kernel.view();
        assert_eq!(
            view.status.entry(GEN, req(5)).and_then(|e| e.record_digest),
            Some(candidate_of(P, 5, 5).record_digest),
            "uncertain {uncertain}: the entry this kernel recorded itself is kept, digest and all"
        );
        for n in [4, 6] {
            assert_eq!(
                view.status
                    .entry(GEN, req(n))
                    .map(|e| (e.record_digest, e.seq)),
                Some((None, Some(Seq(n)))),
                "uncertain {uncertain}: seeded r{n} has the row's seq and no record digest"
            );
        }
        assert_eq!(
            view.status.len(),
            3,
            "uncertain {uncertain}: 5 (own), 4 and 6 (seeded); 7 is above the cut, the row that \
             does not decode is skipped"
        );
    }
    // The skip is logged (reviewer T-1): once per seed that met the row, one seed per case.
    assert_eq!(
        own_log_lines(
            "m7a_194_status_seed_loads_the_predecessors_durable_rows",
            "status seed: a dedup row does not decode; skipped (A-R90)",
        )
        .len(),
        2,
        "one skip line per case"
    );
}

/// M7A-194's seed on the `Recovered` step itself, and its one whole-seed refusal (tester-ka-next
/// T8 and T9, A-R90). When the snapshot already shows the cut, the seed lands in the step that
/// set it — the `try_seed` after `apply` in `Publication::step_with`, because the one before it
/// ran when no seed existed — so nothing reaches the kernel between the recovery and a status
/// query in the same batch. A dedup row written in the served generation or a newer one is not a
/// predecessor's row: the seed loads nothing and stays owed while the snapshot shows it, and lands
/// once it does not. The block logs one warning when it starts and rescans nothing until the
/// snapshot moves (reviewer R-3): a second input at the same position does zero scans.
#[retcd_test]
fn m7a_194_status_seed_lands_on_the_recovered_step_and_fails_closed_on_a_newer_row() {
    let next = Generation(GEN.0 + 1);
    let row = |n: u64| {
        (
            dedup_key(GEN, AffinityId(1), req(n)),
            dedup_value(Digest::ROOT, Seq(n), EPOCH),
        )
    };
    let status_of = |n: u64, status: TxnStatus| {
        vec![EffectKind::Reply(ReplyEffect::Status {
            identity: req(n),
            status,
        })]
    };
    let applied = |n: u64| {
        TxnStatus::Resolved(TxnResult {
            outcome: Outcome::RecoveredApplied,
            ..result(P, n)
        })
    };

    // T8: the snapshot shows the cut before the recovery lands.
    let mut rig = Rig::new();
    rig.published(5);
    rig.snapshot.dedup = vec![row(4)];
    rig.snapshot.generation = next;
    rig.snapshot_at(6);
    rig.step(recovered(PartitionMode::Active, 6, false, None));
    let kernel = rig.p1.kernel(NODE, P).expect("P1 holds P");
    assert!(
        kernel.seed_pending().is_none(),
        "the seed lands on the Recovered step when the snapshot already shows the cut"
    );
    assert_eq!(
        kernel.view().status.entry(GEN, req(4)).map(|e| e.seq),
        Some(Some(Seq(4))),
        "and request 4 is held from it"
    );

    // T9: a row of the served generation ahead of the seed, at or below the cut (above it the
    // row is the discarded suffix and is skipped before its generation is looked at).
    let mut rig = Rig::new();
    rig.published(5);
    let newer = (
        dedup_key(next, AffinityId(1), req(8)),
        dedup_value(Digest::ROOT, Seq(6), EPOCH),
    );
    rig.snapshot.dedup = vec![row(4), newer.clone()];
    rig.snapshot.generation = next;
    rig.snapshot_at(6);
    rig.step(recovered(PartitionMode::Active, 6, false, None));
    let kernel = rig.p1.kernel(NODE, P).expect("P1 holds P");
    assert!(
        kernel.seed_pending().is_some(),
        "a row of the served generation keeps the whole seed owed"
    );
    assert!(
        kernel.view().status.entry(GEN, req(4)).is_none(),
        "nothing is loaded around it"
    );
    let scans = rig.snapshot.scans.get();
    assert_eq!(
        rig.step(status(4, Some(GEN))),
        status_of(4, TxnStatus::Unknown),
        "the row stays owed across a status query, which answers Unknown for the open predecessor"
    );
    assert_eq!(
        rig.snapshot.scans.get(),
        scans,
        "a second input at the same snapshot position rescans nothing"
    );
    // The snapshot moves and still shows the row: rescanned, still blocked, not logged again.
    rig.snapshot_at(7);
    rig.step(status(4, Some(GEN)));
    assert!(
        rig.snapshot.scans.get() > scans,
        "a moved snapshot is rescanned"
    );
    assert!(
        rig.p1
            .kernel(NODE, P)
            .expect("P1 holds P")
            .seed_pending()
            .is_some(),
        "still blocked"
    );
    rig.snapshot.dedup.retain(|r| r.0 != newer.0);
    rig.snapshot_at(8);
    assert_eq!(
        rig.step(status(4, Some(GEN))),
        status_of(4, applied(4)),
        "once the snapshot no longer shows it the seed lands and request 4 answers"
    );
    assert_eq!(
        own_log_lines(
            "m7a_194_status_seed_lands_on_the_recovered_step_and_fails_closed_on_a_newer_row",
            "status seed: blocked by a row of the served generation or newer",
        )
        .len(),
        1,
        "one warning for the block, however many steps it lasted"
    );
    assert!(
        rig.p1
            .kernel(NODE, P)
            .expect("P1 holds P")
            .seed_pending()
            .is_none(),
        "loaded"
    );
}

/// M7A-194 under an uncertain map with two retained generations (lead ruling A-R91, reviewer
/// C-1). The seed loads a row of `GEN - 1` and one of the predecessor `GEN`, recovering into
/// `GEN + 1` with `uncertain: true`. Only the predecessor folds under the three-way rule: `GEN - 1`
/// answers `RecoveredApplied`, `GEN` answers `Unknown` — the same split `fold_recovered` makes over
/// the same two entries held in memory, where the older one is left as it was.
#[retcd_test]
fn m7a_194_status_seed_folds_only_the_predecessor_under_an_uncertain_map() {
    let older = Generation(GEN.0 - 1);
    let row = |generation: Generation, n: u64| {
        (
            dedup_key(generation, AffinityId(1), req(n)),
            dedup_value(Digest::ROOT, Seq(n), EPOCH),
        )
    };
    let mut rig = Rig::new();
    rig.snapshot.dedup = vec![row(older, 3), row(GEN, 4)];
    rig.snapshot.generation = Generation(GEN.0 + 1);
    rig.snapshot_at(6);
    rig.step(recovered(PartitionMode::Active, 6, true, None));
    let kernel = rig.p1.kernel(NODE, P).expect("P1 holds P");
    assert!(kernel.seed_pending().is_none(), "fixture: the seed landed");
    let seeded = kernel.view().status;
    assert_eq!(
        seeded.lookup(req(3), older),
        StatusOutcome::RecoveredApplied {
            result: TxnResult {
                generation: older,
                ..result(P, 3)
            }
        },
        "an older generation's row is not folded under the predecessor's uncertain map"
    );
    assert_eq!(
        seeded.lookup(req(4), GEN),
        StatusOutcome::Unknown,
        "the predecessor's row is"
    );

    // The same two entries held in memory, folded by `fold_recovered` under the same map.
    let mut held = StatusIndex::new(older);
    for (generation, n) in [(older, 3), (GEN, 4)] {
        held.record(StatusEntry {
            request: req(n),
            lineage: Lineage {
                generation,
                ..lineage()
            },
            seq: Some(Seq(n)),
            record_digest: None,
            outcome: StatusOutcome::Published {
                result: TxnResult {
                    generation,
                    ..result(P, n)
                },
            },
            snapshot: None,
            at: Tick::ZERO,
        })
        .expect("under the cap");
    }
    held.fold_recovered(&RetainedStatusMap {
        predecessor_generation: GEN,
        predecessor_cutoff: Seq(6),
        retained_through: Seq(6),
        discarded_from: None,
        uncertain: true,
    });
    let wire = |index: &StatusIndex, n: u64, generation: Generation| {
        matches!(
            to_wire(index.lookup(req(n), generation)),
            TxnStatus::Resolved(_)
        )
    };
    assert_eq!(
        (wire(&seeded, 3, older), wire(&seeded, 4, GEN)),
        (wire(&held, 3, older), wire(&held, 4, GEN)),
        "the seed and fold_recovered resolve the same generations"
    );
    assert_eq!(
        (wire(&held, 3, older), wire(&held, 4, GEN)),
        (true, false),
        "fixture: fold_recovered resolves the older entry and not the predecessor's"
    );
}

/// M7A-194, a generation older than the predecessor while the seed is pending (lead ruling A-R91
/// Q2 as revised, reviewer R-1). The slot opened at `GEN` only and recovers into `GEN + 1` with
/// the snapshot below `retained_through`, so `GEN - 1` is neither open nor loaded. Before the seed
/// it answers `Unknown`, not `StatusExpired`; after the seed its durable row answers
/// `RecoveredApplied`. A generation this boot retired still answers `StatusExpired` throughout.
#[retcd_test]
fn m7a_194_status_seed_pending_never_expires_an_older_generation() {
    let older = Generation(GEN.0 - 1);
    let status_of = |status: TxnStatus| {
        vec![EffectKind::Reply(ReplyEffect::Status {
            identity: req(3),
            status,
        })]
    };
    for retired in [false, true] {
        let mut rig = Rig::new();
        rig.snapshot.dedup = vec![(
            dedup_key(older, AffinityId(1), req(3)),
            dedup_value(Digest::ROOT, Seq(3), EPOCH),
        )];
        rig.snapshot.generation = Generation(GEN.0 + 1);
        if retired {
            rig.step(EventKind::Kernel(KernelEvent::RetireGeneration {
                generation: older,
            }));
        }
        rig.step(recovered(PartitionMode::Active, 6, false, None));
        assert!(
            rig.p1
                .kernel(NODE, P)
                .expect("P1 holds P")
                .seed_pending()
                .is_some(),
            "retired {retired}: fixture: the seed is owed"
        );
        let (before, after) = if retired {
            (TxnStatus::Expired, TxnStatus::Expired)
        } else {
            (
                TxnStatus::Unknown,
                TxnStatus::Resolved(TxnResult {
                    generation: older,
                    outcome: Outcome::RecoveredApplied,
                    ..result(P, 3)
                }),
            )
        };
        assert_eq!(
            rig.step(status(3, Some(older))),
            status_of(before),
            "retired {retired}: before the seed"
        );
        rig.snapshot_at(6);
        assert_eq!(
            rig.step(status(3, Some(older))),
            status_of(after),
            "retired {retired}: after the seed"
        );
        assert!(
            rig.p1
                .kernel(NODE, P)
                .expect("P1 holds P")
                .seed_pending()
                .is_none(),
            "retired {retired}: loaded"
        );
    }
}

/// M7A-172 (K-A-52): `fold_recovered` in isolation. The predecessor's entries fold by their own
/// sequence, never as a blanket success; the hand-built `discarded_from: None` map is the one
/// construction that reaches the `StatusExpired` arm.
#[retcd_test]
fn m7a_172_recovery_folds_status_by_sequence_not_by_presence() {
    let entry = |seq: u64| StatusEntry {
        request: req(seq),
        lineage: lineage(),
        seq: Some(Seq(seq)),
        record_digest: Some(d(seq)),
        outcome: published_status(seq),
        snapshot: None,
        at: Tick::ZERO,
    };
    let map = |uncertain, discarded_from: Option<u64>| RetainedStatusMap {
        predecessor_generation: GEN,
        predecessor_cutoff: Seq(10),
        retained_through: Seq(10),
        discarded_from: discarded_from.map(Seq),
        uncertain,
    };
    let applied = |seq: u64| StatusOutcome::RecoveredApplied {
        result: result(P, seq),
    };
    for (m, want) in [
        (
            map(false, Some(11)),
            [
                applied(9),
                applied(10),
                StatusOutcome::Unknown,
                StatusOutcome::Unknown,
            ],
        ),
        (map(true, Some(11)), [StatusOutcome::Unknown; 4]),
        (
            map(false, None),
            [
                applied(9),
                applied(10),
                StatusOutcome::StatusExpired,
                StatusOutcome::StatusExpired,
            ],
        ),
    ] {
        let mut index = StatusIndex::new(GEN);
        for seq in 9..=12 {
            index.record(entry(seq)).expect("under the cap");
        }
        index.fold_recovered(&m);
        let got: Vec<_> = (9..=12).map(|seq| index.lookup(req(seq), GEN)).collect();
        assert_eq!(got, want, "{m:?}");
    }
}

// ---- reads ------------------------------------------------------------------------------------

/// A fresh read waits behind the candidate and is served at the new published position.
#[retcd_test]
fn a_fresh_read_waits_for_publication_and_sees_the_published_value() {
    let mut rig = Rig::new();
    assert_eq!(
        rig.admitted(read(21)),
        vec![read_reply(
            21,
            ReadServiceOutcome::Served,
            Some(value_at(START))
        )]
    );
    let c = rig.pending_with_recheck(5);
    assert_eq!(rig.admitted(read(22)), vec![]);
    rig.snapshot_at(5);
    let effects = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
    assert!(
        effects.contains(&read_reply(
            22,
            ReadServiceOutcome::WaitedAtBarrier,
            Some(value_at(5))
        )),
        "{effects:?}"
    );
}

/// Invariant 2: a view above the published prefix — the raw applied one — serves nothing.
#[retcd_test]
fn a_read_never_serves_a_view_above_the_published_prefix() {
    let mut rig = Rig::new();
    rig.snapshot_at(START + 2);
    assert_eq!(
        rig.admitted(read(21)),
        vec![
            ignored(AuthorityIgnoreReason::ReadViewNotPublished),
            rejected_read(21, ErrorKind::Unavailable),
        ]
    );
}

/// Invariant 2, the generation half (tester-p1 F1, gate5 N23): the published position is a
/// generation **and** a seq, so a view at the published seq under another generation serves
/// nothing. In both modes that serve: a successor's view over the installed position, and the
/// predecessor's view over a read-only recovery's cutoff. Twin: the same view under the published
/// generation is served.
#[retcd_test]
fn a_read_never_serves_a_view_from_another_generation() {
    let others = [Generation(GEN.0 + 1), GEN];
    for ((label, enter), other) in GATED_MODES.into_iter().zip(others) {
        let mut rig = Rig::new();
        enter(&mut rig);
        let published = rig.view().published;
        assert_ne!(published.generation, other, "{label}");
        rig.snapshot.generation = other;
        rig.snapshot_at(published.seq.0);
        assert_eq!(
            rig.admitted(read(21)),
            vec![
                ignored(AuthorityIgnoreReason::ReadViewNotPublished),
                rejected_read(21, ErrorKind::Unavailable),
            ],
            "{label}: {other:?} at the published seq is not the published position"
        );
        rig.snapshot.generation = published.generation;
        assert_eq!(
            rig.admitted(read(22)),
            vec![read_reply(
                22,
                ReadServiceOutcome::Served,
                Some(value_at(published.seq.0))
            )],
            "{label}: twin"
        );
    }
}

/// Steps that take a fresh rig into some mode.
type Enter = fn(&mut Rig);

/// Invariant 2 for fresh reads, in every mode that may not serve — every mode but `Serving` and a
/// read-only recovery, which refuses writes only (A-R71 F6; that one is served in
/// `a_recovered_or_installed_position_serves_both_reads_from_its_own_view`): refused, with the
/// mode's own code, never queued, and asking A1 nothing. The codes are A-R72a Q3's: an
/// unresolved transaction answers `UnknownOutcome` (spec §5.3); a lost authority or a fenced
/// store answers design §3.4's pre-apply code for its reason; `Blocked` answers
/// `ProtectionPaused`.
#[retcd_test]
fn a_fresh_read_is_refused_in_every_mode_that_may_not_serve() {
    let entries: [(Enter, ErrorKind); 4] = [
        (
            |rig| {
                rig.step(candidate(5, 5));
                rig.step(deadline(P, 1));
            },
            ErrorKind::UnknownOutcome,
        ),
        (
            |rig| {
                rig.step(fence(FenceScope::Node, DenyReason::Expired));
            },
            ErrorKind::LeaseExpired,
        ),
        (
            |rig| {
                rig.step(fence(
                    FenceScope::Partition(P),
                    DenyReason::LocalStorageFenced,
                ));
            },
            ErrorKind::ProtectionPaused,
        ),
        (
            |rig| {
                rig.step(EventKind::Kernel(KernelEvent::BlockPartition(block())));
            },
            ErrorKind::ProtectionPaused,
        ),
    ];
    for (enter, code) in entries {
        let mut rig = Rig::new();
        enter(&mut rig);
        let mode = rig.view().mode;
        assert_ne!(mode, PubMode::Serving);
        assert_eq!(
            rig.step(read(21)),
            vec![rejected_read(21, code)],
            "{mode:?}"
        );
        assert!(rig.view().waiters.is_empty(), "{mode:?}");
    }
}

/// Q1 (A-R71): two queued reads under one identity asking for **different** keys, in both
/// orders. The barrier holds one key per identity, so the second read is refused on arrival as a
/// reused identity and never queued; the first keeps its key, and its reply carries that key's
/// value. (`ReplyEffect::Read` carries no key, so a client could not tell a wrong-key answer.)
#[retcd_test]
fn a_second_queued_read_under_one_identity_is_refused_not_guessed() {
    for (first, second) in [(&b"a"[..], &b"k"[..]), (b"k", b"a")] {
        let label = (first, second);
        let mut rig = Rig::new();
        let c = rig.pending_with_recheck(5);
        assert_eq!(rig.admitted(read_key(21, first)), vec![], "{label:?}");
        assert_eq!(
            rig.step(read_key(21, second)),
            vec![rejected_read(21, ErrorKind::RequestIdReuse)],
            "{label:?}: refused on arrival"
        );
        assert_eq!(rig.view().waiters, vec![req(21)], "{label:?}: never queued");
        rig.snapshot_at(5);
        let effects = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
        let reads: Vec<_> = effects
            .iter()
            .filter(|e| matches!(e, EffectKind::Reply(ReplyEffect::Read { .. })))
            .cloned()
            .collect();
        assert_eq!(
            reads,
            vec![read_reply(
                21,
                ReadServiceOutcome::WaitedAtBarrier,
                Some(value_of(first, 5))
            )],
            "{label:?}: the first reply is the first key's"
        );
    }
}

// ---- the old-prefix view (A-R69a, M7A-108) ------------------------------------------------------

/// `PreviousPublished` never waits and never reads the step's storage view: it answers with the
/// handle P1 kept at the published position, even when storage has applied above it. Until
/// storage binds a view there is none to answer from. (Every row also checks, on every step, that
/// no handle but the kept one leaves P1: `Rig::assert_only_kept_handles_leave`.)
#[retcd_test]
fn previous_published_is_the_kept_view_never_the_storage_view() {
    let mut rig = Rig::new();
    assert_eq!(
        rig.admitted(read_previous(30)),
        vec![previous(30, Err(ErrorKind::Unavailable))],
        "the install's view is never bound in this rig"
    );
    rig.published(5);
    assert_eq!(rig.snapshot.at, Seq(5));
    assert_eq!(
        rig.admitted(read_previous(32)),
        vec![previous(32, Ok(snap(1)))],
        "not the step's view, even where it sits at the published position"
    );
    rig.pending_with_recheck(6);
    rig.snapshot_at(6);
    assert_eq!(
        rig.admitted(read_previous(31)),
        vec![previous(31, Ok(snap(1)))],
        "behind a pending candidate, over a storage view above the published prefix"
    );
}

/// A publish releases the kept view and asks for the next one. Until storage binds it, the old
/// prefix is not served — neither the released handle nor the step's view.
#[retcd_test]
fn a_publish_replaces_the_kept_view_and_the_gap_answers_unavailable() {
    let mut rig = Rig::new();
    rig.published(5);
    let c = rig.pending_with_recheck(6);
    rig.snapshot_at(6);
    let t = rig.now + 10;
    assert_eq!(
        rig.step(answer(Checkpoint::Publication, c, Verdict::Admit)),
        vec![
            status_write(6, 6, published_status(6), t),
            release(1),
            open(2),
            notify(6),
            cancel(2),
            check(Checkpoint::Reply, corr(4)),
        ]
    );
    assert_eq!(rig.view().kept, None);
    assert_eq!(
        rig.admitted(read_previous(30)),
        vec![previous(30, Err(ErrorKind::Unavailable))],
        "the gap"
    );
    assert_eq!(rig.step(ready(2, 6)), vec![]);
    assert_eq!(
        rig.admitted(read_previous(31)),
        vec![previous(31, Ok(snap(2)))]
    );
    // A duplicate completion for the kept view neither releases nor replaces it.
    assert_eq!(rig.step(ready(2, 6)), vec![]);
    assert_eq!(rig.view().kept, Some(snap(2)));
}

/// A view storage bound anywhere but the published position is released, never served — in
/// either order of two publishes' completions.
#[retcd_test]
fn a_view_bound_off_the_published_position_is_released() {
    let mut rig = Rig::new();
    let (opened, _) = snapshot_ledger(&rig.publish_only(5));
    assert_eq!(opened, vec![snap(1)]);
    assert_eq!(
        rig.step(ready(1, 6)),
        vec![release(1)],
        "bound above the published prefix"
    );
    assert_eq!(rig.view().kept, None);
    assert_eq!(
        rig.admitted(read_previous(30)),
        vec![previous(30, Err(ErrorKind::Unavailable))]
    );

    for newer_first in [false, true] {
        let mut rig = Rig::new();
        rig.publish_only(5);
        let (opened, released) = snapshot_ledger(&rig.publish_only(6));
        assert_eq!(
            (opened, released),
            (vec![snap(2)], vec![]),
            "nothing kept yet, nothing to release"
        );
        let order = if newer_first {
            [(2, 6), (1, 5)]
        } else {
            [(1, 5), (2, 6)]
        };
        for (n, at) in order {
            let want = if n == 1 { vec![release(1)] } else { vec![] };
            assert_eq!(rig.step(ready(n, at)), want, "newer_first={newer_first}");
        }
        assert_eq!(rig.view().kept, Some(snap(2)), "newer_first={newer_first}");
        assert_eq!(
            rig.view().opening.into_keys().collect::<Vec<_>>(),
            vec![snap(0)],
            "only the install's view, which this rig never binds"
        );
    }
}

/// A-R69a condition 4: every view P1 asks storage for is released exactly once — across the
/// install's view, a keep, a replace, a view bound off the published position, and a `Recovered`
/// that lands while a view is still being bound — except the one kept at the end. No row here
/// crashes: a restarted counter can reuse a leaked handle until storage drops views on crash
/// (owed by the sim, not P1).
#[retcd_test]
fn every_view_p1_opens_is_released_exactly_once() {
    let mut rig = Rig::new();
    let mut all = vec![open(0)];
    all.extend(rig.step(ready(0, START)));
    all.extend(rig.publish_only(5));
    all.extend(rig.step(ready(1, 5)));
    all.extend(rig.publish_only(6));
    all.extend(rig.step(ready(2, 7)));
    all.extend(rig.publish_only(7));
    all.extend(rig.step(ready(3, 7)));
    all.extend(rig.publish_only(8));
    all.extend(rig.step(recovered(PartitionMode::Active, 8, false, None)));
    all.extend(rig.step(ready(4, 8)));
    all.extend(rig.step(ready(5, 8)));

    let (opened, mut released) = snapshot_ledger(&all);
    assert_eq!(opened, (0..=5).map(snap).collect::<Vec<_>>());
    released.sort();
    assert_eq!(released, opened[..5], "{all:?}");
    let view = rig.view();
    assert_eq!(view.kept, Some(snap(5)), "the cutoff's view");
    assert!(view.opening.is_empty());
}

/// A-R71 F6: `Recovered` and `install` each ask for a view at their own position, so
/// `PreviousPublished` answers from it once storage binds it; design §4.2's `Recovered` row
/// rebases `published_snapshot`. A read-only recovery serves **both** reads there: it refuses
/// writes, never reads. Near-miss: storage above the position is never visible — a `Fresh` read
/// over it is refused, and a view bound there is released rather than kept.
#[retcd_test]
fn a_recovered_or_installed_position_serves_both_reads_from_its_own_view() {
    let next = Generation(GEN.0 + 1);
    // How the rig reaches the position; the view that opened for it; the position.
    let entries: [(&str, Enter, u64, Generation, u64); 2] = [
        (
            "recovered read-only",
            |rig| {
                rig.published(5);
                rig.published(6);
                assert_eq!(
                    rig.step(recovered(PartitionMode::ReadOnly, 5, false, Some(6))),
                    vec![
                        ignored(AuthorityIgnoreReason::ReplyWithheld),
                        ignored(AuthorityIgnoreReason::ReplyWithheld),
                        release(2),
                        open(3),
                    ]
                );
                assert_eq!(
                    rig.view().mode,
                    PubMode::Frozen {
                        cause: FreezeCause::RecoveryReadOnly
                    }
                );
                // It refuses writes: the candidate carries the recovered lineage, so read-only
                // is the only reason left to refuse it (tester-p1 F6f).
                let mut cand = candidate_of(P, 6, 9);
                cand.lineage = rig.view().lineage;
                assert_eq!(
                    rig.step(EventKind::Kernel(KernelEvent::AppliedCandidate(Box::new(
                        cand
                    )))),
                    vec![ignored(AuthorityIgnoreReason::CandidateUnreachable)]
                );
            },
            3,
            next,
            5,
        ),
        ("install", |_| {}, 0, GEN, START),
    ];
    for (label, enter, n, generation, at) in entries {
        for above in [false, true] {
            let label = format!("{label}, bound above: {above}");
            let mut rig = Rig::new();
            enter(&mut rig);
            assert_eq!(
                rig.view().opening.get(&snap(n)),
                Some(&PublishedAt {
                    generation,
                    seq: Seq(at)
                }),
                "{label}: asked for at the position"
            );
            assert_eq!(
                rig.admitted(read_previous(30)),
                vec![previous(30, Err(ErrorKind::Unavailable))],
                "{label}: the gap before storage binds it"
            );
            rig.snapshot.generation = generation;
            if above {
                rig.snapshot_at(at + 1);
                assert_eq!(rig.step(ready(n, at + 1)), vec![release(n)], "{label}");
                assert_eq!(
                    rig.admitted(read_previous(31)),
                    vec![previous(31, Err(ErrorKind::Unavailable))],
                    "{label}"
                );
                assert_eq!(
                    rig.admitted(read(32)),
                    vec![
                        ignored(AuthorityIgnoreReason::ReadViewNotPublished),
                        rejected_read(32, ErrorKind::Unavailable),
                    ],
                    "{label}: the row above the position is never visible"
                );
            } else {
                rig.snapshot_at(at);
                assert_eq!(rig.step(ready(n, at)), vec![], "{label}");
                assert_eq!(
                    rig.admitted(read_previous(31)),
                    vec![previous(31, Ok(snap(n)))],
                    "{label}"
                );
                assert_eq!(
                    rig.admitted(read(32)),
                    vec![read_reply(
                        32,
                        ReadServiceOutcome::Served,
                        Some(value_at(at))
                    )],
                    "{label}"
                );
            }
        }
    }
}

/// A-R71: a completion for a view P1 no longer waits on is a no-op — whether P1 already released
/// it (superseded before it bound, replaced by a publish, or bound off the position) or never
/// asked for it. A second `Release` for one handle would be a double release against storage.
#[retcd_test]
fn a_duplicate_ready_for_a_released_view_is_a_no_op() {
    // Superseded before it bound, then bound at its own position twice.
    let mut rig = Rig::new();
    rig.publish_only(5);
    rig.publish_only(6);
    assert_eq!(rig.step(ready(1, 5)), vec![release(1)], "superseded");
    assert_eq!(rig.step(ready(1, 5)), vec![], "superseded, again");

    // Kept, then replaced by a publish; its completion arrives again.
    let mut rig = Rig::new();
    rig.published(5);
    let (_, released) = snapshot_ledger(&rig.publish_only(6));
    assert_eq!(released, vec![snap(1)]);
    assert_eq!(rig.step(ready(1, 5)), vec![], "replaced by a publish");

    // Bound above the position, released, then completed again.
    let mut rig = Rig::new();
    rig.publish_only(5);
    assert_eq!(
        rig.step(ready(1, 6)),
        vec![release(1)],
        "bound off position"
    );
    assert_eq!(rig.step(ready(1, 6)), vec![], "bound off position, again");

    // Never asked for.
    let before = rig.view();
    assert_eq!(rig.step(ready(9, 5)), vec![], "never asked for");
    assert_eq!(rig.view(), before);
}

/// K-A-14: the queue is capped.
#[retcd_test]
fn the_waiter_cap_answers_overloaded() {
    let mut rig = Rig::new();
    rig.pending_with_recheck(5);
    for n in 0..CAP {
        assert_eq!(rig.admitted(read(100 + n)), vec![]);
    }
    assert_eq!(
        rig.step(read(200)),
        vec![rejected_read(200, ErrorKind::Overloaded)]
    );
    assert_eq!(rig.view().waiters.len(), WAITER_CAP);
}

// ---- status -----------------------------------------------------------------------------------

/// A-R10: present, trimmed within a live generation, retired, never held — and no generation.
#[retcd_test]
fn status_answers_are_a_total_function() {
    let wire = |status: TxnStatus| {
        vec![EffectKind::Reply(ReplyEffect::Status {
            identity: req(5),
            status,
        })]
    };
    let mut rig = Rig::new();
    rig.published(5);
    assert_eq!(
        rig.step(status(5, Some(GEN))),
        wire(TxnStatus::Resolved(result(P, 5)))
    );
    assert_eq!(
        rig.step(status(5, Some(Generation(GEN.0 + 7)))),
        wire(TxnStatus::Expired),
        "never held"
    );
    rig.step(EventKind::Kernel(KernelEvent::StatusTrim {
        generation: GEN,
        below: Seq(6),
    }));
    assert_eq!(
        rig.step(status(5, Some(GEN))),
        wire(TxnStatus::Unknown),
        "trimmed in a live generation"
    );
    assert_eq!(
        rig.step(status(5, None)),
        wire(TxnStatus::Unknown),
        "no generation"
    );
    rig.step(EventKind::Kernel(KernelEvent::RetireGeneration {
        generation: GEN,
    }));
    assert_eq!(
        rig.step(status(5, Some(GEN))),
        wire(TxnStatus::Expired),
        "retired"
    );
    assert_eq!(
        rig.step(status(5, None)),
        wire(TxnStatus::Unknown),
        "A-R63: without a generation nothing proves it retired"
    );
}

/// The refusal a status write past the cap answers with: `OVERLOADED`, the contract's
/// client-facing condition (plan §13 Q-4).
fn status_overloaded() -> EffectKind {
    EffectKind::Kernel(KernelEffect::Ignored {
        reason: KernelIgnoredReason::Error(ErrorKind::Overloaded),
    })
}

/// How many status-index writes in `effects` are for request `n`.
fn status_writes_for(effects: &[EffectKind], n: u64) -> usize {
    effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                EffectKind::Kernel(KernelEffect::Publication(PublicationEffect::Status(entry)))
                    if entry.request == req(n)
            )
        })
        .count()
}

/// M7A-90's status half, on P1's own index (PR #1 review, R1-F006). Plan §13 Q-4 (lead ruling
/// A-R24): a named capacity, `retention_cap_entries`, and `OVERLOADED` past it. The row's T1
/// function bounds status only through T1's admission cap; this one bounds the index itself, so
/// it holds when `StatusTrim` stops arriving while T1's own trims keep freeing room.
///
/// With no trim, the index stops at its cap: a write that would add an entry is refused with an
/// explicit `Ignored(Error(Overloaded))` in place of its `Status` effect, and nothing is held for
/// it. The candidate itself still publishes and replies: its bytes are applied, and past apply
/// the only answers are `Published` and `UNKNOWN_OUTCOME`. Entries already held, and their
/// answers, are untouched; the refused identity answers `Unknown`, which never proves
/// nonexecution. A trim frees room and the next candidate is held again.
///
/// Red at 7e262dc: `PubConfig` had no `status_cap`, and the index grew without bound.
#[retcd_test]
fn m7a_90_status_index_stops_at_its_cap_without_a_trim() {
    assert_eq!(PubConfig::default().status_cap, RETENTION_CAP_ENTRIES);
    assert_eq!(StatusIndex::new(GEN).cap(), RETENTION_CAP_ENTRIES);

    // The index alone: an overwrite at the cap is always taken, a new entry is refused.
    let entry = |n: u64| StatusEntry {
        request: req(n),
        lineage: lineage(),
        seq: Some(Seq(n)),
        record_digest: Some(d(n)),
        outcome: StatusOutcome::Unknown,
        snapshot: None,
        at: Tick::ZERO,
    };
    let mut index = StatusIndex::with_cap(GEN, 2);
    assert_eq!(index.record(entry(5)), Ok(()));
    assert_eq!(index.record(entry(6)), Ok(()));
    assert_eq!(
        index.record(StatusEntry {
            outcome: published_status(5),
            ..entry(5)
        }),
        Ok(()),
        "an overwrite adds nothing"
    );
    assert_eq!(index.record(entry(7)), Err(ErrorKind::Overloaded));
    assert_eq!(index.len(), 2);
    // The seed's write meets the same cap (PR #1 tester finding F-4).
    assert_eq!(index.restore(entry(7)), Err(ErrorKind::Overloaded));
    assert_eq!(index.len(), 2);
    assert_eq!(index.lookup(req(5), GEN), published_status(5));
    assert_eq!(index.lookup(req(7), GEN), StatusOutcome::Unknown);
    index.trim(GEN, Seq(6));
    assert_eq!(index.record(entry(7)), Ok(()), "the trim freed room");
    assert_eq!(index.len(), 2);

    // Through P1, with a cap of 3 and no trim.
    let mut rig = Rig::new_with(PubConfig {
        status_cap: 3,
        ..PubConfig::default()
    });
    for seq in 5..=7 {
        rig.published(seq);
    }
    assert_eq!(rig.view().status.len(), 3);

    let at_cap = rig.step(candidate(8, 8));
    assert!(at_cap.contains(&status_overloaded()), "{at_cap:?}");
    assert_eq!(status_writes_for(&at_cap, 8), 0, "{at_cap:?}");
    assert_eq!(
        rig.view().pending.map(|pending| pending.request),
        Some(req(8)),
        "the candidate itself is pending publication"
    );
    rig.qualify(8);
    let c = match rig.step(gained(8)).as_slice() {
        [EffectKind::Kernel(KernelEffect::AuthorityCheck {
            checkpoint: Checkpoint::Publication,
            correlation,
            ..
        })] => *correlation,
        other => panic!("expected one Publication check, got {other:?}"),
    };
    rig.snapshot_at(8);
    let publish = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
    assert!(published_any(&publish), "{publish:?}");
    assert!(publish.contains(&status_overloaded()), "{publish:?}");
    assert_eq!(status_writes_for(&publish, 8), 0, "{publish:?}");
    assert_eq!(rig.view().status.len(), 3, "never past the cap");

    let wire = |n: u64, status: TxnStatus| {
        vec![EffectKind::Reply(ReplyEffect::Status {
            identity: req(n),
            status,
        })]
    };
    assert_eq!(
        rig.step(status(5, Some(GEN))),
        wire(5, TxnStatus::Resolved(result(P, 5))),
        "a held entry is untouched"
    );
    assert_eq!(
        rig.step(status(8, Some(GEN))),
        wire(8, TxnStatus::Unknown),
        "the refused identity is Unknown, never Expired"
    );
    assert_eq!(rig.step(status(8, None)), wire(8, TxnStatus::Unknown));

    rig.step(EventKind::Kernel(KernelEvent::StatusTrim {
        generation: GEN,
        below: Seq(6),
    }));
    assert_eq!(rig.view().status.len(), 2, "the trim freed room");
    let (opened, _) = snapshot_ledger(&publish);
    rig.step(ready_of(opened[0], 8));
    rig.published(9);
    assert_eq!(rig.view().status.len(), 3);
    assert_eq!(
        rig.step(status(9, Some(GEN))),
        wire(9, TxnStatus::Resolved(result(P, 9))),
        "held again once room was freed"
    );
}

/// `lookup_any` answers from the newest generation holding the identity, whatever order the
/// generations were written, trimmed, restored and retired in (PR #1 review, R1-F007). Claims no
/// row: it pins the answers so that indexing the lookup by identity cannot change them.
#[retcd_test]
fn status_lookup_any_answers_the_newest_generation_that_holds_the_identity() {
    let at = |n: u64, generation: u64, seq: u64| StatusEntry {
        request: req(n),
        lineage: Lineage {
            generation: Generation(generation),
            ..lineage()
        },
        seq: Some(Seq(seq)),
        record_digest: Some(d(seq)),
        outcome: StatusOutcome::Published {
            result: TxnResult {
                generation: Generation(generation),
                ..result(P, seq)
            },
        },
        snapshot: None,
        at: Tick::ZERO,
    };
    let outcome = |entry: StatusEntry| entry.outcome;
    let mut index = StatusIndex::new(Generation(3));
    // Generations written out of order, and identities interleaved across them.
    for entry in [
        at(1, 5, 50),
        at(2, 3, 30),
        at(1, 3, 31),
        at(3, 5, 51),
        at(2, 4, 40),
    ] {
        index.record(entry).expect("under the cap");
    }
    assert_eq!(index.lookup_any(req(1)), outcome(at(1, 5, 50)));
    assert_eq!(index.lookup_any(req(2)), outcome(at(2, 4, 40)));
    assert_eq!(index.lookup_any(req(3)), outcome(at(3, 5, 51)));
    assert_eq!(index.lookup_any(req(9)), StatusOutcome::Unknown);

    // An overwrite moves the answer, not the generation.
    index
        .record(StatusEntry {
            outcome: StatusOutcome::Unknown,
            ..at(1, 5, 50)
        })
        .expect("under the cap");
    assert_eq!(index.lookup_any(req(1)), StatusOutcome::Unknown);

    // A trim of the newest generation falls through to the next one down.
    index.trim(Generation(5), Seq(51));
    assert_eq!(index.lookup_any(req(1)), outcome(at(1, 3, 31)));
    assert_eq!(index.lookup_any(req(3)), outcome(at(3, 5, 51)));

    // A restored entry in a newer generation answers; one under a trim watermark does not land.
    assert_eq!(index.restore(at(2, 6, 60)), Ok(true));
    assert_eq!(index.lookup_any(req(2)), outcome(at(2, 6, 60)));
    assert_eq!(index.restore(at(1, 5, 49)), Ok(false));
    assert_eq!(index.lookup_any(req(1)), outcome(at(1, 3, 31)));

    // A retired generation is gone; with none left the answer is `Unknown`, never `Expired`.
    index.retire(Generation(6));
    assert_eq!(index.lookup_any(req(2)), outcome(at(2, 4, 40)));
    index.retire(Generation(4));
    index.retire(Generation(3));
    assert_eq!(index.lookup_any(req(2)), StatusOutcome::Unknown);
    assert_eq!(index.lookup_any(req(1)), StatusOutcome::Unknown);
    assert_eq!(index.lookup_any(req(3)), outcome(at(3, 5, 51)));

    // Nothing is kept for an identity no generation holds any more, through a retire or a trim
    // (PR #1 tester finding F-6): such an index equals one that never held it, `by_request`
    // included.
    let mut dropped = StatusIndex::new(Generation(3));
    dropped.record(at(1, 4, 40)).expect("under the cap");
    dropped.record(at(2, 3, 30)).expect("under the cap");
    dropped.retire(Generation(4));
    dropped.trim(Generation(3), Seq(31));
    let mut never = StatusIndex::new(Generation(3));
    never.retire(Generation(4));
    never.trim(Generation(3), Seq(31));
    assert_eq!(dropped, never);
}

// ---- boots ------------------------------------------------------------------------------------

/// P1's state is volatile: after a reboot the old candidate is gone, its old check's answer is
/// stale, and nothing is published or replied for it.
#[retcd_test]
fn a_new_boot_starts_with_nothing_pending() {
    let mut rig = Rig::new();
    let c = rig.pending_with_recheck(5);
    rig.boot = BootId(2);
    let effects = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
    assert_eq!(
        effects,
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
    let view = rig.view();
    assert_eq!(view.boot, BootId(2));
    assert!(view.pending.is_none());
    assert_eq!(view.published.seq, Seq::ZERO);
}

// ---- the read gate (A-R72) ----------------------------------------------------------------------

/// A1's answer to a check P1 asked under `lineage`.
fn answer_in(
    lineage: Lineage,
    checkpoint: Checkpoint,
    correlation: CorrelationId,
    verdict: Verdict,
    authority_seq: u64,
) -> EventKind {
    let mut decided = decision(
        lineage.partition,
        checkpoint,
        correlation,
        verdict,
        authority_seq,
    );
    decided.lineage = lineage;
    EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Answer(decided)))
}

/// The one `Read` check `effects` asks, and nothing else.
fn read_check(effects: &[EffectKind]) -> CorrelationId {
    match effects {
        [EffectKind::Kernel(KernelEffect::AuthorityCheck {
            checkpoint: Checkpoint::Read,
            correlation,
            ..
        })] => *correlation,
        other => panic!("expected one Read check, got {other:?}"),
    }
}

impl Rig {
    /// The authority seq of the view P1 holds.
    fn held_seq(&self) -> u64 {
        self.view().authority.map_or(1, |v| v.authority_seq)
    }

    /// A1's current answer to the read check `c`, under the lineage P1 serves.
    fn gate_answer(&self, c: CorrelationId, verdict: Verdict) -> EventKind {
        answer_in(
            self.view().lineage,
            Checkpoint::Read,
            c,
            verdict,
            self.held_seq(),
        )
    }

    /// Step a read; it must ask exactly one `Read` check under the lineage served. Returns the
    /// check's correlation.
    fn ask(&mut self, kind: EventKind) -> CorrelationId {
        let served = self.view().lineage;
        let effects = self.step(kind);
        match effects.as_slice() {
            [EffectKind::Kernel(KernelEffect::AuthorityCheck {
                checkpoint: Checkpoint::Read,
                lineage,
                correlation,
            })] if *lineage == served => *correlation,
            other => panic!("expected one Read check under {served:?}, got {other:?}"),
        }
    }

    /// Step a read through the gate: its one `Read` check, then A1's `Admit`. Returns every
    /// effect of the answer's step.
    fn admitted(&mut self, kind: EventKind) -> Vec<EffectKind> {
        let c = self.ask(kind);
        self.step(self.gate_answer(c, Verdict::Admit))
    }
}

/// `Serving`, with the install's view bound at [`START`].
fn gate_serving(rig: &mut Rig) {
    assert_eq!(rig.step(ready(0, START)), vec![]);
}

/// A read-only recovery at 5 (A-R71 F6), with the cutoff's view bound.
fn gate_read_only(rig: &mut Rig) {
    rig.published(5);
    let effects = rig.step(recovered(PartitionMode::ReadOnly, 5, false, None));
    let (opened, _) = snapshot_ledger(&effects);
    assert_eq!(opened.len(), 1, "{effects:?}");
    rig.snapshot.generation = Generation(GEN.0 + 1);
    rig.snapshot_at(5);
    assert_eq!(rig.step(ready_of(opened[0], 5)), vec![]);
    assert_eq!(
        rig.view().mode,
        PubMode::Frozen {
            cause: FreezeCause::RecoveryReadOnly
        }
    );
}

/// The two modes that serve reads, and so gate them (A-R72).
const GATED_MODES: [(&str, Enter); 2] = [("serving", gate_serving), ("read-only", gate_read_only)];

/// A-R72 row 1: the grant has expired and A1's timer has not fired, so P1 still holds the mode it
/// served in. A read asks A1, A1 denies, and the read is refused with design §3.4's pre-apply code
/// for that reason: never `UnknownOutcome`, and P1 freezes nothing. Fresh and previous alike
/// (A-R72a), in both modes that serve.
#[retcd_test]
fn a_read_after_the_grant_expires_is_refused_before_a1s_timer_fires() {
    for (label, enter) in GATED_MODES {
        for (reason, code) in [
            (DenyReason::Expired, ErrorKind::LeaseExpired),
            (DenyReason::GenerationChanged, ErrorKind::GenerationChanged),
            (DenyReason::LocalStorageFenced, ErrorKind::ProtectionPaused),
        ] {
            let label = format!("{label}, {reason:?}");
            let mut rig = Rig::new();
            enter(&mut rig);
            let mode = rig.view().mode;
            let c = rig.ask(read(40));
            assert_eq!(
                rig.step(rig.gate_answer(c, Verdict::Deny(reason))),
                vec![rejected_read(40, code)],
                "{label}"
            );
            let c = rig.ask(read_previous(41));
            assert_eq!(
                rig.step(rig.gate_answer(c, Verdict::Deny(reason))),
                vec![previous(41, Err(code))],
                "{label}"
            );
            assert_eq!(
                rig.view().mode,
                mode,
                "{label}: a refused read freezes nothing"
            );
            assert!(rig.view().waiters.is_empty(), "{label}");
        }
    }
}

/// A-R72 row 2, the near-miss: under a valid grant the same reads are served, fresh from the
/// published view and previous from the kept one. An `Admit` decided for another lineage is not
/// an admit for this read.
#[retcd_test]
fn a_read_under_a_valid_grant_is_served_from_the_published_view() {
    for (label, enter) in GATED_MODES {
        let mut rig = Rig::new();
        enter(&mut rig);
        let at = rig.view().published.seq.0;
        let kept = rig.view().kept.expect("bound");
        let c = rig.ask(read(40));
        assert_eq!(
            rig.step(rig.gate_answer(c, Verdict::Admit)),
            vec![read_reply(
                40,
                ReadServiceOutcome::Served,
                Some(value_at(at))
            )],
            "{label}"
        );
        let c = rig.ask(read_previous(41));
        assert_eq!(
            rig.step(rig.gate_answer(c, Verdict::Admit)),
            vec![previous(41, Ok(kept))],
            "{label}"
        );
        let served = rig.view().lineage;
        let moved = Lineage {
            owner_epoch: OwnerEpoch(served.owner_epoch.0 + 1),
            ..served
        };
        let c = rig.ask(read(42));
        let seq = rig.held_seq();
        assert_eq!(
            rig.step(answer_in(moved, Checkpoint::Read, c, Verdict::Admit, seq)),
            vec![rejected_read(42, ErrorKind::GenerationChanged)],
            "{label}: an admit for another lineage"
        );
    }
}

/// A-R72 row 3, and the liveness row the lead asked for with A-R72b: a read that arrives after
/// the check in flight was issued does not ride it, whatever that check answers. It is not
/// stranded either: the next check is issued when the first one answers, and only that one
/// decides it — served on its `Admit`, refused on its `Deny`, never left waiting.
#[retcd_test]
fn a_read_that_arrives_after_its_check_was_issued_waits_for_the_next() {
    for (verdict, first) in [
        (
            Verdict::Admit,
            read_reply(40, ReadServiceOutcome::Served, Some(value_at(START))),
        ),
        (
            Verdict::Deny(DenyReason::Expired),
            rejected_read(40, ErrorKind::LeaseExpired),
        ),
    ] {
        for (follow, late) in [
            (
                Verdict::Admit,
                vec![
                    read_reply(41, ReadServiceOutcome::Served, Some(value_at(START))),
                    previous(42, Ok(snap(0))),
                ],
            ),
            (
                Verdict::Deny(DenyReason::Expired),
                vec![
                    rejected_read(41, ErrorKind::LeaseExpired),
                    previous(42, Err(ErrorKind::LeaseExpired)),
                ],
            ),
        ] {
            let label = format!("{verdict:?}, then {follow:?}");
            let mut rig = Rig::new();
            gate_serving(&mut rig);
            let c1 = rig.ask(read(40));
            assert_eq!(rig.step(read(41)), vec![], "{label}: 41 does not ride c1");
            assert_eq!(rig.step(read_previous(42)), vec![], "{label}");
            let effects = rig.step(rig.gate_answer(c1, verdict));
            assert_eq!(effects.first(), Some(&first), "{label}: {effects:?}");
            let c2 = read_check(&effects[1..]);
            assert_ne!(c2, c1);
            assert_eq!(
                rig.step(rig.gate_answer(c2, follow)),
                late,
                "{label}: the follow-up decides the late reads"
            );
            let view = rig.view();
            assert!(view.gated.is_empty(), "{label}: none left waiting");
            assert_eq!(view.read_check, None, "{label}");
            // Nothing left in flight: the next read asks again.
            rig.ask(read(43));
        }
    }
}

/// A-R72 row 4: a read-only partition, then A1's `Fence{Expired}`. A read arriving after it is
/// refused by the mode, with the §3.4 code (A-R72a Q3); a read whose check was in flight when the
/// fence landed is refused even when that check answers `Admit`; a previous read is refused by
/// the mode as well (A-R72c).
#[retcd_test]
fn a_read_only_partition_fenced_expired_refuses_every_read() {
    let mut rig = Rig::new();
    gate_read_only(&mut rig);
    let c = rig.ask(read(41));
    rig.step(fence(FenceScope::Node, DenyReason::Expired));
    assert_eq!(
        rig.view().mode,
        PubMode::Frozen {
            cause: FreezeCause::AuthorityLost(DenyReason::Expired)
        }
    );
    assert_eq!(
        rig.step(read(40)),
        vec![rejected_read(40, ErrorKind::LeaseExpired)],
        "refused by the mode, with no check"
    );
    assert_eq!(
        rig.step(rig.gate_answer(c, Verdict::Admit)),
        vec![rejected_read(41, ErrorKind::LeaseExpired)],
        "an admit decided before the fence serves nothing after it"
    );
    assert_eq!(
        rig.step(read_previous(42)),
        vec![previous(42, Err(ErrorKind::LeaseExpired))],
        "a previous read is refused by the mode too, with no check (A-R72c)"
    );
}

/// A-R72 row 5: an answer older than the view P1 holds decides no read, neither an `Admit` nor a
/// `Deny`. It re-asks under a new correlation (A-R69 F2); a duplicate of that older answer is
/// dropped and the check in flight kept (A-R71 N03); only the current answer serves.
#[retcd_test]
fn a_stale_read_check_answer_re_asks_and_never_serves() {
    for (label, kind, done) in [
        (
            "fresh",
            read(40),
            read_reply(40, ReadServiceOutcome::Served, Some(value_at(START))),
        ),
        ("previous", read_previous(40), previous(40, Ok(snap(0)))),
    ] {
        let mut rig = Rig::new();
        gate_serving(&mut rig);
        assert_eq!(rig.step(view_push(P, 3)), vec![]);
        let c1 = rig.ask(kind);
        let stale = |c, verdict| answer_in(lineage(), Checkpoint::Read, c, verdict, 2);
        let effects = rig.step(stale(c1, Verdict::Admit));
        assert_eq!(
            effects.first(),
            Some(&ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)),
            "{label}: {effects:?}"
        );
        let c2 = read_check(&effects[1..]);
        assert_ne!(c2, c1, "{label}");
        assert_eq!(
            rig.step(stale(c1, Verdict::Admit)),
            vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)],
            "{label}: a duplicate of the older answer keeps c2 in flight"
        );
        let effects = rig.step(stale(c2, Verdict::Deny(DenyReason::Expired)));
        assert_eq!(
            effects.first(),
            Some(&ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)),
            "{label}: a stale deny refuses nothing: {effects:?}"
        );
        let c3 = read_check(&effects[1..]);
        assert_eq!(
            rig.step(answer_in(
                lineage(),
                Checkpoint::Read,
                c3,
                Verdict::Admit,
                3
            )),
            vec![done],
            "{label}"
        );
    }
}

/// A-R72: a read gated when `Recovered` lands is asked about again under the new lineage, by a
/// check issued after it arrived. The predecessor's check then decides nothing.
#[retcd_test]
fn a_read_gated_across_recovered_is_asked_again_under_the_new_lineage() {
    let mut rig = Rig::new();
    rig.published(5);
    let c1 = rig.ask(read(40));
    rig.snapshot.generation = Generation(GEN.0 + 1);
    rig.snapshot_at(5);
    let effects = rig.step(recovered(PartitionMode::Active, 5, false, None));
    let served = rig.view().lineage;
    assert_ne!(served, lineage());
    let c2 = match effects.last() {
        Some(EffectKind::Kernel(KernelEffect::AuthorityCheck {
            checkpoint: Checkpoint::Read,
            lineage,
            correlation,
        })) if *lineage == served => *correlation,
        other => panic!("expected a Read check under {served:?} last, got {other:?}"),
    };
    assert_ne!(c2, c1);
    let seq = rig.held_seq();
    assert_eq!(
        rig.step(answer_in(
            lineage(),
            Checkpoint::Read,
            c1,
            Verdict::Admit,
            seq
        )),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)],
        "the predecessor's check decides nothing"
    );
    assert_eq!(
        rig.step(rig.gate_answer(c2, Verdict::Admit)),
        vec![read_reply(
            40,
            ReadServiceOutcome::Served,
            Some(value_at(5))
        )]
    );
}

/// A-R72: a read waiting for its check counts against the waiter cap, fresh or previous.
#[retcd_test]
fn the_waiter_cap_counts_reads_waiting_for_their_check() {
    let mut rig = Rig::new();
    rig.ask(read(100));
    for n in 1..CAP {
        assert_eq!(rig.step(read(100 + n)), vec![], "{n}");
    }
    assert_eq!(rig.view().gated.len(), WAITER_CAP);
    assert_eq!(
        rig.step(read(900)),
        vec![rejected_read(900, ErrorKind::Overloaded)]
    );
    assert_eq!(
        rig.step(read_previous(901)),
        vec![previous(901, Err(ErrorKind::Overloaded))]
    );
}

/// A-R72b: one identity rule for every read kind. A second read under an identity whose read is
/// still gated or waiting is refused `RequestIdReuse` on arrival and never queued, whichever kind
/// either read is. The first is answered once, as if the second never came, and once it is
/// answered the identity is free again.
#[retcd_test]
fn a_second_read_under_a_waiting_identity_is_refused_whatever_its_kind() {
    type ReadOf = fn(u64) -> EventKind;
    let kinds: [(&str, ReadOf); 2] = [("fresh", read), ("previous", read_previous)];
    let served = |kind: &str| {
        if kind == "fresh" {
            read_reply(40, ReadServiceOutcome::Served, Some(value_at(START)))
        } else {
            previous(40, Ok(snap(0)))
        }
    };
    let reused = |kind: &str| {
        if kind == "fresh" {
            rejected_read(40, ErrorKind::RequestIdReuse)
        } else {
            previous(40, Err(ErrorKind::RequestIdReuse))
        }
    };
    for (first, open) in kinds {
        for (second, again) in kinds {
            let label = format!("{first} then {second}");
            let mut rig = Rig::new();
            gate_serving(&mut rig);
            let c = rig.ask(open(40));
            assert_eq!(
                rig.step(again(40)),
                vec![reused(second)],
                "{label}: refused on arrival"
            );
            assert_eq!(rig.view().gated, vec![req(40)], "{label}: never queued");
            assert_eq!(
                rig.step(rig.gate_answer(c, Verdict::Admit)),
                vec![served(first)],
                "{label}: the first is answered once"
            );
            assert_eq!(rig.admitted(again(40)), vec![served(second)], "{label}");
        }
    }
    // A Fresh read waiting at the barrier holds its identity too.
    let mut rig = Rig::new();
    rig.pending_with_recheck(5);
    assert_eq!(rig.admitted(read(21)), vec![]);
    assert_eq!(rig.view().waiters, vec![req(21)]);
    assert_eq!(
        rig.step(read_previous(21)),
        vec![previous(21, Err(ErrorKind::RequestIdReuse))],
        "a waiter's identity"
    );
    assert!(rig.view().gated.is_empty(), "never queued");
}

/// tester-p1 N22: an answered read frees its identity's key. A new read under the same identity
/// is served, never refused as a reuse, after each way a read is answered: served on arrival,
/// answered from the barrier by a publish, and refused by its check's deny. The barrier's key
/// count is back to its size before the read each time.
#[retcd_test]
fn an_answered_read_frees_its_identity_for_the_next_read() {
    let held = |rig: &Rig| rig.p1.held_read_keys(NODE, P);
    let served_at = |seq| read_reply(21, ReadServiceOutcome::Served, Some(value_at(seq)));
    let mut rig = Rig::new();
    gate_serving(&mut rig);
    assert_eq!(held(&rig), 0);
    // Served on arrival.
    assert_eq!(rig.admitted(read(21)), vec![served_at(START)]);
    assert_eq!(held(&rig), 0, "served");
    assert_eq!(rig.admitted(read(21)), vec![served_at(START)], "again");
    assert_eq!(held(&rig), 0);
    // Answered from the barrier by a publish.
    let c = rig.pending_with_recheck(5);
    assert_eq!(rig.admitted(read(21)), vec![]);
    assert_eq!(held(&rig), 1, "waiting at the barrier");
    rig.snapshot_at(5);
    let effects = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
    assert!(
        effects.contains(&read_reply(
            21,
            ReadServiceOutcome::WaitedAtBarrier,
            Some(value_at(5))
        )),
        "{effects:?}"
    );
    assert_eq!(held(&rig), 0, "answered at the barrier");
    assert_eq!(
        rig.admitted(read(21)),
        vec![served_at(5)],
        "after the barrier"
    );
    // Refused by its check's deny.
    let c = rig.ask(read(21));
    assert_eq!(held(&rig), 1, "gated");
    assert_eq!(
        rig.step(rig.gate_answer(c, Verdict::Deny(DenyReason::Expired))),
        vec![rejected_read(21, ErrorKind::LeaseExpired)]
    );
    assert_eq!(held(&rig), 0, "refused");
    assert_eq!(rig.admitted(read(21)), vec![served_at(5)], "after the deny");
    assert_eq!(held(&rig), 0);
}

/// A-R72c: the mode refuses `PreviousPublished` exactly as it refuses `Fresh`, with the same
/// code (A-R72a Q3), in a lost-authority freeze, a fenced store and a block. A read arriving in
/// such a mode is refused with no check. One already gated when the mode changed is refused even
/// though A1's `Admit` for it is delivered: an admit never overrides the local mode, so one that
/// races a fence serves nothing. Near-miss: a read-only recovery still serves it on `Admit`.
#[retcd_test]
fn a_previous_read_is_refused_by_a_refusing_mode_even_when_admitted() {
    let entries: [(&str, EventKind, ErrorKind); 3] = [
        (
            "authority lost",
            fence(FenceScope::Node, DenyReason::Expired),
            ErrorKind::LeaseExpired,
        ),
        (
            "store fenced",
            fence(FenceScope::Partition(P), DenyReason::LocalStorageFenced),
            ErrorKind::ProtectionPaused,
        ),
        (
            "blocked",
            EventKind::Kernel(KernelEvent::BlockPartition(block())),
            ErrorKind::ProtectionPaused,
        ),
    ];
    for (label, enter, code) in entries {
        let mut rig = Rig::new();
        gate_serving(&mut rig);
        let c = rig.ask(read_previous(40));
        rig.step(enter);
        assert!(
            !matches!(rig.view().mode, PubMode::Serving),
            "{label}: {:?}",
            rig.view().mode
        );
        assert_eq!(
            rig.step(rig.gate_answer(c, Verdict::Admit)),
            vec![previous(40, Err(code))],
            "{label}: an admit that raced the mode change serves nothing"
        );
        assert_eq!(
            rig.step(read_previous(41)),
            vec![previous(41, Err(code))],
            "{label}: refused by the mode on arrival, with no check"
        );
        assert_eq!(
            rig.step(read(42)),
            vec![rejected_read(42, code)],
            "{label}: the same code as a fresh read"
        );
        assert_eq!(rig.view().read_check, None, "{label}");
    }
    // Near-miss: a read-only recovery serves the previous view on `Admit`.
    let mut rig = Rig::new();
    gate_read_only(&mut rig);
    let kept = rig.view().kept.expect("the cutoff view is bound");
    assert_eq!(
        rig.admitted(read_previous(43)),
        vec![previous(43, Ok(kept))]
    );
}

/// A-R72d (tester-p1 gate4 PROBE_L): with no authority view held — never pushed, or reset by a
/// new boot — a read of either kind is refused `Unavailable` on arrival and asks nothing. No
/// answer could be ours then, so a check asked now would strand: its answer is dropped as stale,
/// nothing re-asks, and every later read would queue behind it until the cap. A stale answer
/// changes nothing; after a view is pushed, a later read is asked about, and served.
#[retcd_test]
fn a_read_with_no_authority_view_is_refused_and_strands_nothing() {
    for label in ["never pushed", "after a new boot"] {
        let mut rig = if label == "never pushed" {
            Rig::bare()
        } else {
            let mut rig = Rig::new();
            gate_serving(&mut rig);
            rig.boot = BootId(2);
            rig
        };
        assert_eq!(
            rig.step(read(40)),
            vec![rejected_read(40, ErrorKind::Unavailable)],
            "{label}: refused on arrival, with no check"
        );
        assert_eq!(
            rig.step(read_previous(41)),
            vec![previous(41, Err(ErrorKind::Unavailable))],
            "{label}"
        );
        assert_eq!(
            rig.step(answer_in(
                lineage(),
                Checkpoint::Read,
                corr(1),
                Verdict::Admit,
                1
            )),
            vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)],
            "{label}: an answer with no view held is stale, and decides nothing"
        );
        let view = rig.view();
        assert!(view.gated.is_empty(), "{label}: nothing held");
        assert_eq!(view.read_check, None, "{label}: nothing in flight");
        assert_eq!(rig.step(view_push(P, 1)), vec![], "{label}");
        let c = rig.ask(read(42));
        if label == "never pushed" {
            gate_serving(&mut rig);
            assert_eq!(
                rig.step(rig.gate_answer(c, Verdict::Admit)),
                vec![read_reply(
                    42,
                    ReadServiceOutcome::Served,
                    Some(value_at(START))
                )],
                "{label}: a later read is asked about, and served"
            );
        }
    }
}

/// A-R72c scope note (tester-p1 gate4 `C-unresolved-refuses-previous`): under an unresolved
/// transaction a fresh read answers `UnknownOutcome` with no check, while a previous read is
/// asked about and, once admitted, served from the kept view — which predates the unresolved
/// candidate.
#[retcd_test]
fn a_previous_read_under_an_unresolved_transaction_is_served_once_admitted() {
    let mut rig = Rig::new();
    gate_serving(&mut rig);
    rig.step(candidate(5, 5));
    rig.step(deadline(P, 1));
    assert_eq!(
        rig.view().mode,
        PubMode::Frozen {
            cause: FreezeCause::UnresolvedTransaction
        }
    );
    assert_eq!(
        rig.step(read(40)),
        vec![rejected_read(40, ErrorKind::UnknownOutcome)],
        "fresh: refused by the mode, with no check"
    );
    let c = rig.ask(read_previous(41));
    assert_eq!(
        rig.step(rig.gate_answer(c, Verdict::Admit)),
        vec![previous(41, Ok(snap(0)))],
        "previous: served once admitted"
    );
}

// ---- test-plan rows: shared helpers -------------------------------------------------------------

/// The one `Publication` check `effects` asks, and nothing else.
fn publication_check(effects: &[EffectKind]) -> CorrelationId {
    match effects {
        [EffectKind::Kernel(KernelEffect::AuthorityCheck {
            checkpoint: Checkpoint::Publication,
            correlation,
            ..
        })] => *correlation,
        other => panic!("expected one Publication check, got {other:?}"),
    }
}

/// The `Reply` check among `effects`.
fn reply_check(effects: &[EffectKind]) -> CorrelationId {
    effects
        .iter()
        .find_map(|e| match e {
            EffectKind::Kernel(KernelEffect::AuthorityCheck {
                checkpoint: Checkpoint::Reply,
                correlation,
                ..
            }) => Some(*correlation),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected a Reply check, got {effects:?}"))
}

/// Whether `effects` asks a `Publication` check.
fn asks_publication(effects: &[EffectKind]) -> bool {
    effects.iter().any(|e| {
        matches!(
            e,
            EffectKind::Kernel(KernelEffect::AuthorityCheck {
                checkpoint: Checkpoint::Publication,
                ..
            })
        )
    })
}

/// How many read replies `effects` carries for reader `n`.
fn read_replies_for(effects: &[EffectKind], n: u64) -> usize {
    effects
        .iter()
        .filter(|e| {
            matches!(e, EffectKind::Reply(ReplyEffect::Read { identity, .. }) if *identity == req(n))
        })
        .count()
}

fn block_event(reason: BlockReason) -> EventKind {
    EventKind::Kernel(KernelEvent::BlockPartition(reason))
}

fn answer_event(decided: AuthorityDecision) -> EventKind {
    EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Answer(decided)))
}

/// P1's wire answer to a status query for request `n`, checked to be the step's one effect.
fn status_answer(rig: &mut Rig, n: u64, generation: Option<Generation>) -> TxnStatus {
    let effects = rig.step(status(n, generation));
    match effects.as_slice() {
        [EffectKind::Reply(ReplyEffect::Status { identity, status })] => {
            assert_eq!(*identity, req(n));
            *status
        }
        other => panic!("expected one status answer, got {other:?}"),
    }
}

/// Request `seq` is `Published{result}` on state and `Resolved(result)` on the wire (KA-9).
fn assert_status_published(rig: &mut Rig, seq: u64) {
    assert_eq!(
        rig.view().status.lookup(req(seq), GEN),
        published_status(seq)
    );
    assert_eq!(
        status_answer(rig, seq, Some(GEN)),
        TxnStatus::Resolved(result(P, seq))
    );
}

fn edge(seq: u64, direction: QualificationDirection) -> QualificationChanged {
    match edge_of(P, seq, direction) {
        EventKind::Kernel(KernelEvent::QualificationChanged(q)) => q,
        _ => unreachable!(),
    }
}

const fn fact(fact: PubFact) -> PubEffect {
    PubEffect::Fact(fact)
}

fn notified(effects: &[PubEffect]) -> bool {
    effects
        .iter()
        .any(|e| matches!(e, PubEffect::NotifyTxn { .. }))
}

/// [`PubKernel`] driven directly with a KA-8 fake, for the clauses that name a fact's payload
/// (the wire carries only the fact's kind).
struct Direct {
    kernel: PubKernel,
    repl: ScriptedReplication,
    now: u64,
}

impl Direct {
    /// `P` at [`START`], the view pushed at authority seq 1, nothing qualifying.
    fn new() -> Self {
        let mut direct = Self {
            kernel: PubKernel::new(PubConfig::default(), BOOT, lineage(), Seq(START)),
            repl: ScriptedReplication::new(lineage(), CONFIG),
            now: 0,
        };
        assert_eq!(
            direct.apply(PubEvent::AuthorityView(authority_view(1))),
            vec![]
        );
        direct
    }

    fn apply(&mut self, event: PubEvent) -> Vec<PubEffect> {
        self.now += 10;
        self.kernel.apply(Tick(self.now), event, Some(&self.repl))
    }

    /// Qualify `seq` and deliver its `Gained`; returns the `Publication` check's correlation.
    fn qualify_and_ask(&mut self, seq: u64) -> CorrelationId {
        self.repl.set_qualifies(Seq(seq), true);
        let effects = self.apply(PubEvent::QualificationChanged(edge(
            seq,
            QualificationDirection::Gained,
        )));
        match effects.as_slice() {
            [PubEffect::AuthorityCheck {
                checkpoint: Checkpoint::Publication,
                correlation,
                ..
            }] => *correlation,
            other => panic!("expected one Publication check, got {other:?}"),
        }
    }

    /// Candidate `seq` for request `seq`, qualified, its check asked.
    fn pending_with_recheck(&mut self, seq: u64) -> CorrelationId {
        self.apply(PubEvent::Candidate(candidate_of(P, seq, seq)));
        self.qualify_and_ask(seq)
    }

    fn answer(
        &mut self,
        c: CorrelationId,
        checkpoint: Checkpoint,
        verdict: Verdict,
    ) -> Vec<PubEffect> {
        self.apply(PubEvent::AuthorityAnswer(decision(
            P, checkpoint, c, verdict, 1,
        )))
    }

    /// Candidate `seq` published; returns the `Reply` check's correlation.
    fn published(&mut self, seq: u64) -> CorrelationId {
        let c = self.pending_with_recheck(seq);
        let effects = self.answer(c, Checkpoint::Publication, Verdict::Admit);
        effects
            .iter()
            .find_map(|e| match e {
                PubEffect::AuthorityCheck {
                    checkpoint: Checkpoint::Reply,
                    correlation,
                    ..
                } => Some(*correlation),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected a publish, got {effects:?}"))
    }
}

// ---- test-plan rows: §5.1 the publication rule ------------------------------------------------

/// M7A-92 (charter P1 "Late ACK revalidates authority"): the qualification lands 2,000 ticks after
/// the candidate. The check is asked then, under a correlation P1 mints then; the candidate's own
/// dispatch admission is not an answer to it, and nothing publishes before that check's answer.
/// The post-apply timer is due at the same tick; its delivery order is the sim's, and this row
/// does not deliver it (M7A-103 is the row where it fires first).
#[retcd_test]
fn m7a_92_late_ack_revalidates_authority() {
    let mut rig = Rig::new();
    let applied_at = rig.now + 10;
    let effects = rig.step(candidate(5, 5));
    assert!(!asks_publication(&effects), "{effects:?}");
    rig.now = applied_at + DEADLINE - 10;
    rig.qualify(5);
    let late = rig.step(gained(5));
    assert_eq!(rig.now, applied_at + DEADLINE);
    let c = publication_check(&late);
    let dispatch = candidate_of(P, 5, 5).authority.correlation;
    assert_ne!(
        c, dispatch,
        "a correlation minted at the late qualification"
    );
    assert_eq!(rig.view().pending.expect("kept").recheck, Some(c));

    assert_eq!(
        rig.step(answer(Checkpoint::Publication, dispatch, Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)],
        "the candidate's dispatch admission does not answer the late check"
    );
    assert_eq!(
        rig.view().published.seq,
        Seq(START),
        "no publish before its answer"
    );
    rig.snapshot_at(5);
    assert!(published_any(&rig.step(answer(
        Checkpoint::Publication,
        c,
        Verdict::Admit
    ))));
    assert_eq!(rig.view().published.seq, Seq(5));
}

/// M7A-93 (A-R20/A-R21/B-R21): P1 re-reads `qualifies_now` when the answer lands. The KA-8 fake
/// says 5 qualifies at the `Gained` and not at the answer, with `digest_at` still `Match`.
/// Class sim in the plan: R1's own flip (`DivergenceDetected`) routed to P1 by the sim is owed by
/// dev-sim-route; this is the K-row, with the flip scripted.
#[retcd_test]
fn m7a_93_publish_reevaluates_qualifies_now_live() {
    let mut rig = Rig::new();
    let c = rig.pending_with_recheck(5);
    rig.p1
        .scripted_mut(NODE, P)
        .expect("scripted")
        .set_qualifies(Seq(5), false);
    rig.snapshot_at(5);
    assert_eq!(
        rig.step(answer(Checkpoint::Publication, c, Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::PublishPredicateFalse)]
    );
    let view = rig.view();
    assert_eq!(view.published.seq, Seq(START));
    let pending = view.pending.expect("pending kept");
    assert_eq!(
        (pending.request, pending.qualifying, pending.recheck),
        (req(5), false, None)
    );
    // It waits for a fresh `Gained`, which asks again.
    rig.qualify(5);
    assert_ne!(publication_check(&rig.step(gained(5))), c);

    // The payload names the conjunct.
    let mut direct = Direct::new();
    let c = direct.pending_with_recheck(5);
    direct.repl.set_qualifies(Seq(5), false);
    assert_eq!(
        direct.answer(c, Checkpoint::Publication, Verdict::Admit),
        vec![fact(PubFact::PublishPredicateFalse {
            which: PredicateFalse::Qualification
        })]
    );
    // Twin (M7A-91's one fact): the fake still says it qualifies, and it publishes.
    let mut direct = Direct::new();
    let c = direct.pending_with_recheck(5);
    assert!(notified(&direct.answer(
        c,
        Checkpoint::Publication,
        Verdict::Admit
    )));
}

/// M7A-95 (invariant 3): `Lost` for a published sequence is a fact, and only a fact. The fact
/// carries R1's cause.
#[retcd_test]
fn m7a_95_qualification_lost_after_publish_is_fact_only() {
    let mut rig = Rig::new();
    rig.published(5);
    let before = rig.view();
    assert_eq!(
        rig.step(lost(5)),
        vec![ignored(
            AuthorityIgnoreReason::QualificationLostAfterPublish
        )]
    );
    assert_eq!(rig.view(), before);
    assert_eq!(rig.view().published.seq, Seq(5));

    let mut direct = Direct::new();
    direct.published(5);
    let mut lost_edge = edge(5, QualificationDirection::Lost);
    lost_edge.cause = QualificationCause::DivergenceDetected(CopyId(2));
    assert_eq!(
        direct.apply(PubEvent::QualificationChanged(lost_edge)),
        vec![fact(PubFact::QualificationLostAfterPublish {
            cause: QualificationCause::DivergenceDetected(CopyId(2))
        })]
    );
    assert_eq!(direct.kernel.published().seq, Seq(5));
}

/// kernel-b's real R1 for `P`: copies `(copy, node, role)`, one regular ACK required, history
/// through 5, the primary applied to 5. Shared by M7A-97 and M7A-98.
fn real_tracker(members: &[(u8, NodeId, ReplicaRole)]) -> ProgressTracker {
    let members = members
        .iter()
        .map(|&(copy, node, role)| Member {
            copy: CopyId(copy),
            node,
            boot: BootId(u64::from(node.0)),
            role,
        })
        .collect();
    let mut config = PartitionConfig::new(P, CONFIG, members);
    config.min_regular_acks = 1;
    let mut history = DigestLadder::new();
    for seq in 1..=5 {
        history.insert(Seq(seq), d(seq));
    }
    ProgressTracker::new(TrackerInit {
        config,
        own: CopyId(0),
        lineage: lineage(),
        history,
        local: progress_at(5),
    })
    .expect("tracker")
}

const fn progress_at(seq: u64) -> ReplicaProgress {
    ReplicaProgress {
        received: ReceivedSeq(seq),
        buffered_applied: AppliedSeq(seq),
        durable: DurableSeq(seq),
    }
}

/// `node`'s ACK of everything through 5.
fn ack_at_5(node: NodeId, role: ReplicaRole) -> AppendAck {
    AppendAck {
        partition: P,
        generation: GEN,
        owner_epoch: EPOCH,
        config_version: CONFIG,
        from: node,
        boot: BootId(u64::from(node.0)),
        role,
        progress: progress_at(5),
        digest_at_buffered: d(5),
    }
}

fn peer(node: NodeId) -> PeerLabel {
    PeerLabel {
        node,
        boot: BootId(u64::from(node.0)),
        authenticated: true,
    }
}

/// Hands P1 every `QualificationChanged` R1 emitted, as the sim routes it: a test-local stand-in
/// for dev-sim-route's routing. Returns P1's effects.
fn route_r1(rig: &mut Rig, tracker: &ProgressTracker, r1: &[EffectKind]) -> Vec<EffectKind> {
    let mut out = Vec::new();
    for effect in r1 {
        if let EffectKind::Kernel(KernelEffect::QualificationChanged(q)) = effect {
            out.extend(step_with_tracker(
                rig,
                tracker,
                EventKind::Kernel(KernelEvent::QualificationChanged(q.clone())),
            ));
        }
    }
    out
}

/// M7A-97 (invariant 1; charter DO-NOT "no shadow ACK qualifies"): against kernel-b's real R1, a
/// shadow's ACK of 5 leaves `qualifies_now(5)` false and emits no edge, so P1, handed everything
/// R1 emitted, never asks `Check{Publication}` for 5. Class sim: the sim's own routing of R1 to
/// P1 is owed by dev-sim-route; `route_r1` stands in for it. (A forged edge is a different case:
/// `a_shadow_ack_never_qualifies_against_the_real_tracker`.)
#[retcd_test]
fn m7a_97_no_shadow_ack_ever_qualifies_integration() {
    let (a, b, s) = (NodeId(1), NodeId(2), NodeId(4));
    let mut tracker = real_tracker(&[
        (0, a, ReplicaRole::Primary),
        (1, b, ReplicaRole::RegularSecondary),
        (3, s, ReplicaRole::Shadow),
    ]);
    let mut rig = Rig::new();
    let mut p1 = rig.step(candidate(5, 5));
    for tick in 1..=3 {
        let r1 = tracker.on_ack(&peer(s), &ack_at_5(s, ReplicaRole::Shadow), Tick(tick));
        assert!(
            !tracker.qualifies_now(Seq(5)),
            "kernel-b: a shadow never counts"
        );
        assert!(
            !r1.iter()
                .any(|e| matches!(e, EffectKind::Kernel(KernelEffect::QualificationChanged(_)))),
            "{r1:?}"
        );
        p1.extend(route_r1(&mut rig, &tracker, &r1));
    }
    assert!(!asks_publication(&p1), "{p1:?}");
    assert_eq!(rig.view().pending.expect("kept").recheck, None);
    assert_eq!(rig.view().published.seq, Seq(START));
}

/// M7A-98 (charter DO-NOT "no success weaker than primary + one regular buffered"): RF2 against
/// kernel-b's real R1. With only the primary applied, even a forged edge's check cannot publish:
/// no `Publish`, no reply. The one regular ACK is the one fact: R1 emits `Gained` at 5, P1 asks,
/// and the answer publishes and replies. Class sim: `route_r1` stands in for the sim's routing,
/// owed by dev-sim-route.
#[retcd_test]
fn m7a_98_success_requires_primary_plus_one_regular_buffered() {
    let (a, b) = (NodeId(1), NodeId(2));
    let mut tracker = real_tracker(&[
        (0, a, ReplicaRole::Primary),
        (1, b, ReplicaRole::RegularSecondary),
    ]);
    let mut rig = Rig::new();
    let mut before = rig.step(candidate(5, 5));
    assert!(!tracker.qualifies_now(Seq(5)), "the primary alone");
    let c = publication_check(&rig.step(gained(5)));
    rig.snapshot_at(5);
    before.extend(step_with_tracker(
        &mut rig,
        &tracker,
        answer(Checkpoint::Publication, c, Verdict::Admit),
    ));
    assert!(!published_any(&before), "{before:?}");
    assert_eq!(replies_for(&before, 5), 0, "{before:?}");
    assert_eq!(rig.view().published.seq, Seq(START));

    let r1 = tracker.on_ack(
        &peer(b),
        &ack_at_5(b, ReplicaRole::RegularSecondary),
        Tick(2),
    );
    assert!(tracker.qualifies_now(Seq(5)));
    let gained_at: Vec<_> = r1
        .iter()
        .filter_map(|e| match e {
            EffectKind::Kernel(KernelEffect::QualificationChanged(q)) => {
                Some((q.at_seq, q.direction))
            }
            _ => None,
        })
        .collect();
    assert_eq!(gained_at, vec![(Seq(5), QualificationDirection::Gained)]);
    let c = publication_check(&route_r1(&mut rig, &tracker, &r1));
    let effects = step_with_tracker(
        &mut rig,
        &tracker,
        answer(Checkpoint::Publication, c, Verdict::Admit),
    );
    assert!(published_any(&effects), "{effects:?}");
    assert_eq!(rig.view().published.seq, Seq(5));
    let r = reply_check(&effects);
    assert_eq!(
        step_with_tracker(
            &mut rig,
            &tracker,
            answer(Checkpoint::Reply, r, Verdict::Admit)
        ),
        vec![txn_reply(5, 5)]
    );
}

/// M7A-100 (K-A-54): an `Admit` whose lineage is not the candidate's quarantines exactly as
/// M7A-99's deny does, as `GenerationChanged` (one fact vs M7A-99: the verdict). Waiters get
/// §3.4's code for that reason (A-R72a Q3).
#[retcd_test]
fn m7a_100_publication_check_lineage_moved_quarantines() {
    let mut rig = Rig::new();
    let c = rig.pending_with_recheck(5);
    assert_eq!(rig.admitted(read(21)), vec![]);
    assert_eq!(rig.admitted(read(22)), vec![]);
    let mut moved = decision(P, Checkpoint::Publication, c, Verdict::Admit, 1);
    moved.lineage.generation = Generation(GEN.0 + 1);
    let t = rig.now + 10;
    assert_eq!(
        rig.step(answer_event(moved)),
        vec![
            status_write(5, 5, StatusOutcome::Unknown, t),
            quarantined(5),
            rejected_read(21, ErrorKind::GenerationChanged),
            rejected_read(22, ErrorKind::GenerationChanged),
        ]
    );
    let view = rig.view();
    assert_eq!(
        view.mode,
        PubMode::Frozen {
            cause: FreezeCause::AuthorityLost(DenyReason::GenerationChanged)
        }
    );
    assert!(view.waiters.is_empty());
    assert_eq!(view.published.seq, Seq(START));
}

/// M7A-101 (invariant 6; K-A-40, K-A-45): exactly one terminal reply per request on every path,
/// counted over every effect of the path plus a duplicate deadline at its end; zero on (b) alone.
/// `awaiting_reply` is empty at the end of every path.
#[retcd_test]
fn m7a_101_exactly_one_reply_per_request_on_every_path() {
    type Path = fn(&mut Rig) -> Vec<EffectKind>;
    let paths: [(&str, Path, usize); 6] = [
        (
            "(a) publish and reply",
            |rig| {
                let mut all = rig.step(candidate(5, 5));
                rig.qualify(5);
                let asked = rig.step(gained(5));
                let c = publication_check(&asked);
                rig.snapshot_at(5);
                let published = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
                let r = reply_check(&published);
                all.extend(asked);
                all.extend(published);
                all.extend(rig.step(answer(Checkpoint::Reply, r, Verdict::Admit)));
                all
            },
            1,
        ),
        (
            "(b) publish, reply denied",
            |rig| {
                let mut all = rig.step(candidate(5, 5));
                rig.qualify(5);
                let c = publication_check(&rig.step(gained(5)));
                rig.snapshot_at(5);
                let published = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
                let r = reply_check(&published);
                all.extend(published);
                let withheld = rig.step(answer(
                    Checkpoint::Reply,
                    r,
                    Verdict::Deny(DenyReason::Expired),
                ));
                assert_eq!(
                    withheld,
                    vec![ignored(AuthorityIgnoreReason::ReplyWithheld)]
                );
                all.extend(withheld);
                all
            },
            0,
        ),
        (
            "(c) deadline, then a late publish",
            |rig| {
                let (mut all, r) = published_after_the_deadline(rig);
                all.extend(rig.step(answer(Checkpoint::Reply, r, Verdict::Admit)));
                all
            },
            1,
        ),
        (
            "(d) quarantine, then the deadline",
            |rig| {
                let mut all = rig.step(candidate(5, 5));
                rig.qualify(5);
                let c = publication_check(&rig.step(gained(5)));
                all.extend(rig.step(answer(
                    Checkpoint::Publication,
                    c,
                    Verdict::Deny(DenyReason::Expired),
                )));
                all.extend(rig.step(deadline(P, 1)));
                all
            },
            1,
        ),
        (
            "(e) candidate in Frozen{AuthorityLost}, then the deadline",
            |rig| {
                let mut all = rig.step(fence(FenceScope::Node, DenyReason::Expired));
                all.extend(rig.step(candidate(5, 5)));
                all.extend(rig.step(deadline(P, 1)));
                all
            },
            1,
        ),
        (
            "(f) candidate in Blocked, the deadline, then Recovered",
            |rig| {
                let mut all = rig.step(block_event(block()));
                all.extend(rig.step(candidate(5, 5)));
                all.extend(rig.step(deadline(P, 1)));
                all.extend(rig.step(recovered(PartitionMode::Active, START, false, None)));
                all
            },
            1,
        ),
    ];
    for (label, path, want) in paths {
        let mut rig = Rig::new();
        let mut all = path(&mut rig);
        all.extend(rig.step(deadline(P, 1)));
        assert_eq!(replies_for(&all, 5), want, "{label}: {all:?}");
        assert!(rig.view().awaiting_reply.is_empty(), "{label}");
    }
}

// ---- test-plan rows: §5.2 uncertain outcomes and the freeze -----------------------------------

/// `P` and `P2` installed on one node, both `Serving`, each with a candidate at 5.
fn two_partitions_with_candidates() -> Rig {
    let mut rig = Rig::new();
    assert_eq!(
        rig.p1.install(NODE, BOOT, lineage_of(P2), Seq(START)),
        EffectKind::Store(StoreEffect::Snapshot {
            handle: snap_of(P2, 0),
            partition: P2,
        })
    );
    rig.step_on(P2, view_push(P2, 1));
    rig.step_on(
        P2,
        EventKind::Kernel(KernelEvent::AppliedCandidate(Box::new(candidate_of(
            P2, 5, 50,
        )))),
    );
    rig.step(candidate(5, 5));
    rig
}

/// Closed and node-free (KA-7): a fifth cause, or a node field on one, fails to compile here.
const fn freeze_cause_is_closed(cause: &FreezeCause) {
    match cause {
        FreezeCause::UnresolvedTransaction
        | FreezeCause::LocalStorageFenced
        | FreezeCause::RecoveryReadOnly
        | FreezeCause::AuthorityLost(_) => {}
    }
}

/// M7A-102 (invariant 4; K-A-54): the deadline on `P` answers `Unknown` (status, reply, and each
/// waiter) with no `Freeze` effect, freezes `P` only, and keeps the candidate with `replied`.
/// `P2` sees nothing. Second sub-run (one fact, the entry mode): entered in
/// `Frozen{AuthorityLost(Expired)}`, the same status and reply, and the mode is unchanged. No
/// waiter can queue there (the fence drained them and a fresh read is refused on arrival), so the
/// second sub-run's vector is the first's without the waiters. Class sim: the sim's timer firing
/// on one node of a two-partition cluster is owed by dev-sim-route; this is the K-row.
#[retcd_test]
fn m7a_102_post_apply_deadline_status_unknown_reply_unknown_freeze_one_partition() {
    let mut rig = two_partitions_with_candidates();
    assert_eq!(rig.admitted(read(21)), vec![]);
    assert_eq!(rig.admitted(read(22)), vec![]);
    let p2_before = rig.p1.view(NODE, P2).expect("p2");
    let t = rig.now + 10;
    assert_eq!(
        rig.step(deadline(P, 1)),
        vec![
            status_write(5, 5, StatusOutcome::Unknown, t),
            unknown_reply(5),
            rejected_read(21, ErrorKind::UnknownOutcome),
            rejected_read(22, ErrorKind::UnknownOutcome),
        ]
    );
    let view = rig.view();
    let cause = FreezeCause::UnresolvedTransaction;
    freeze_cause_is_closed(&cause);
    assert_eq!(view.mode, PubMode::Frozen { cause });
    let pending = view.pending.expect("kept");
    assert!(pending.replied);
    assert_eq!(pending.request, req(5));
    assert_eq!(rig.p1.view(NODE, P2).expect("p2"), p2_before);
    assert_eq!(p2_before.mode, PubMode::Serving);

    let mut frozen = two_partitions_with_candidates();
    frozen.step(fence(FenceScope::Partition(P), DenyReason::Expired));
    let entry = frozen.view().mode;
    assert_eq!(
        entry,
        PubMode::Frozen {
            cause: FreezeCause::AuthorityLost(DenyReason::Expired)
        }
    );
    let t = frozen.now + 10;
    assert_eq!(
        frozen.step(deadline(P, 1)),
        vec![
            status_write(5, 5, StatusOutcome::Unknown, t),
            unknown_reply(5)
        ]
    );
    assert_eq!(
        frozen.view().mode,
        entry,
        "the deadline never erases the cause"
    );
    assert!(frozen.view().pending.expect("kept").replied);
}

/// A publish of 5 whose `Reply` check A1 denies `Expired`. Returns that answer's effects. Shared
/// by M7A-104 and M7A-105.
fn reply_denied(rig: &mut Rig) -> Vec<EffectKind> {
    let r = rig.published(5);
    rig.step(answer(
        Checkpoint::Reply,
        r,
        Verdict::Deny(DenyReason::Expired),
    ))
}

/// M7A-104 (charter P1 "lost reply remains queryable"): a deny at `Reply` withholds the reply and
/// undoes nothing. Twin: `Admit` replies once. The fact names the deny's reason.
#[retcd_test]
fn m7a_104_reply_check_deny_no_reply_status_stays_published() {
    let mut rig = Rig::new();
    let effects = reply_denied(&mut rig);
    assert_eq!(effects, vec![ignored(AuthorityIgnoreReason::ReplyWithheld)]);
    assert_eq!(replies_for(&effects, 5), 0);
    let view = rig.view();
    assert!(view.awaiting_reply.is_empty(), "entry removed");
    assert_eq!(view.published.seq, Seq(5));
    assert_status_published(&mut rig, 5);

    let mut twin = Rig::new();
    let r = twin.published(5);
    assert_eq!(
        twin.step(answer(Checkpoint::Reply, r, Verdict::Admit)),
        vec![txn_reply(5, 5)]
    );

    let mut direct = Direct::new();
    let r = direct.published(5);
    assert_eq!(
        direct.answer(r, Checkpoint::Reply, Verdict::Deny(DenyReason::Expired)),
        vec![fact(PubFact::ReplyWithheld {
            why: Withheld::Denied(DenyReason::Expired)
        })]
    );
}

/// M7A-105 (invariant 3): after M7A-104's lost reply, `PreviousPublished` answers the view bound at
/// 5 (the handle P1 kept, per lead ruling A-R69a, not a `SnapshotId::at(g, 5)` literal), and a
/// read sees seq 5's write. `PreviousPublished` passes A1's read gate first (A-R72a).
#[retcd_test]
fn m7a_105_lost_reply_does_not_reverse_publication() {
    let mut rig = Rig::new();
    reply_denied(&mut rig);
    let view = rig.view();
    assert_eq!(view.kept, Some(snap(1)), "the view bound at 5");
    assert_eq!(
        view.published,
        PublishedAt {
            generation: GEN,
            seq: Seq(5)
        }
    );
    assert_eq!(
        rig.admitted(read_previous(30)),
        vec![previous(30, Ok(snap(1)))]
    );
    assert_eq!(
        rig.admitted(read(31)),
        vec![read_reply(
            31,
            ReadServiceOutcome::Served,
            Some(value_at(5))
        )],
        "seq 5's write is what a read sees"
    );
}

/// Candidate 5, then `enter`, then 5 qualifies (`qualifies_now` true, `digest_at` `Match`) and its
/// check is answered `Admit`. Returns the rig, the mode `enter` left, and the answer's effects.
/// Shared by the twins M7A-106 and M7A-170.
fn admit_publication_in(enter: Enter) -> (Rig, PubMode, Vec<EffectKind>) {
    let mut rig = Rig::new();
    rig.step(candidate(5, 5));
    enter(&mut rig);
    let entry = rig.view().mode;
    rig.qualify(5);
    let c = publication_check(&rig.step(gained(5)));
    rig.snapshot_at(5);
    let effects = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
    (rig, entry, effects)
}

/// `Frozen{RecoveryReadOnly}` with a qualifying candidate, driven directly: `Recovered` drops the
/// candidate and no wire event freezes read-only with one pending, so this is the kernel's own
/// guard. Returns the mode before and after, the answer's effects, and the published sequence.
fn admit_publication_read_only() -> (PubMode, PubMode, Vec<PubEffect>, Seq) {
    let mut direct = Direct::new();
    direct.apply(PubEvent::Candidate(candidate_of(P, 5, 5)));
    direct.apply(PubEvent::Freeze {
        cause: FreezeCause::RecoveryReadOnly,
    });
    let entry = direct.kernel.view().mode;
    let c = direct.qualify_and_ask(5);
    let effects = direct.answer(c, Checkpoint::Publication, Verdict::Admit);
    (
        entry,
        direct.kernel.view().mode,
        effects,
        direct.kernel.published().seq,
    )
}

fn enter_unresolved(rig: &mut Rig) {
    rig.step(deadline(P, 1));
}

fn enter_lost_authority(rig: &mut Rig) {
    rig.step(fence(FenceScope::Node, DenyReason::Expired));
}

fn enter_storage_fenced(rig: &mut Rig) {
    rig.step(fence(
        FenceScope::Partition(P),
        DenyReason::LocalStorageFenced,
    ));
}

fn enter_blocked(rig: &mut Rig) {
    rig.step(block_event(block()));
}

/// M7A-106 (K-A-47, K-A-57): the mode guard at publish, with `qualifies_now` true and `digest_at`
/// `Match` in every sub-run so the predicate row cannot shadow it. (a) `Frozen{Unresolved}`
/// publishes and reopens; (b) `Frozen{AuthorityLost}` publishes, mode unchanged; (c) read-only
/// defers; (d) `Blocked` refuses with its own fact, clears the recheck and keeps the candidate.
#[retcd_test]
fn m7a_106_mode_frozen_permits_publish_recovery_read_only_and_blocked_do_not() {
    let (rig, _, effects) = admit_publication_in(enter_unresolved);
    assert!(published_any(&effects), "(a) {effects:?}");
    assert_eq!(rig.view().mode, PubMode::Serving, "(a)");

    let (rig, entry, effects) = admit_publication_in(enter_lost_authority);
    assert!(published_any(&effects), "(b) {effects:?}");
    assert_eq!(rig.view().mode, entry, "(b)");

    let (entry, after, effects, published) = admit_publication_read_only();
    assert_eq!(effects, vec![fact(PubFact::PublishDeferred)], "(c)");
    assert_eq!((after, published), (entry, Seq(START)), "(c)");

    let (rig, entry, effects) = admit_publication_in(enter_blocked);
    assert_eq!(
        effects,
        vec![ignored(AuthorityIgnoreReason::PublishRefusedBlocked)],
        "(d)"
    );
    let view = rig.view();
    let pending = view.pending.expect("(d) pending kept");
    assert_eq!((pending.request, pending.recheck), (req(5), None), "(d)");
    assert_eq!(view.mode, entry, "(d)");
    assert_eq!(view.published.seq, Seq(START), "(d)");
}

// ---- test-plan rows: §5.3 barrier, snapshots and waiters --------------------------------------

/// M7A-107 (charter P1 "Publication barrier"): with 4 published and 5 pending, a `Fresh` read gets
/// no reply (through a duplicate edge and a status query) until 5 publishes, and then it is
/// answered at 5.
#[retcd_test]
fn m7a_107_barrier_acquire_fresh_waits_for_publication() {
    let mut rig = Rig::new();
    let c = rig.pending_with_recheck(5);
    let mut before = rig.admitted(read(3));
    before.extend(rig.step(gained(5)));
    before.extend(rig.step(status(5, Some(GEN))));
    assert_eq!(read_replies_for(&before, 3), 0, "{before:?}");
    assert_eq!(rig.view().waiters, vec![req(3)]);

    rig.snapshot_at(5);
    let effects = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
    let reads: Vec<_> = effects
        .iter()
        .filter(|e| matches!(e, EffectKind::Reply(ReplyEffect::Read { .. })))
        .cloned()
        .collect();
    assert_eq!(
        reads,
        vec![read_reply(
            3,
            ReadServiceOutcome::WaitedAtBarrier,
            Some(value_at(5))
        )]
    );
}

/// M7A-108 (charter P1 "old-prefix snapshots"): 4 published, storage applied to 6.
/// `PreviousPublished` is answered at once, from the view bound at 4. Per lead ruling A-R69a the
/// answer is the handle P1 kept, not a `SnapshotId::at(g, 4)` literal; it is never the step's
/// storage view at 6 (`Rig::assert_only_kept_handles_leave` checks that on every step).
#[retcd_test]
fn m7a_108_barrier_acquire_previous_published_returns_old_prefix_snapshot() {
    let mut rig = Rig::new();
    gate_serving(&mut rig);
    rig.pending_with_recheck(5);
    rig.snapshot_at(6);
    assert_eq!(
        rig.admitted(read_previous(30)),
        vec![previous(30, Ok(snap(0)))]
    );
    assert_eq!(rig.view().kept, Some(snap(0)));
    assert_eq!(rig.view().published.seq, Seq(START));
}

/// The moves of a seeded walk.
#[derive(Debug, Clone, Copy)]
enum Move {
    /// Storage applies one more position, ahead of any candidate.
    Apply,
    Candidate,
    Gained,
    Lost,
    Admit,
    Deny,
    Deadline,
    Fence,
    /// Storage binds a view P1 asked for, at its position or at the applied one.
    Bind,
    Fresh,
    Previous,
}

/// One seeded interleaving over `Module::step` (M7A-109, M7A-112). A local LCG (Knuth's MMIX
/// constants) picks each move, so a seed names one interleaving.
struct Walk {
    rig: Rig,
    rng: u64,
    applied: u64,
    pending: Option<u64>,
    reader: u64,
    /// Where storage bound each view P1 asked for.
    bound: BTreeMap<SnapshotHandle, u64>,
}

impl Walk {
    fn new(seed: u64) -> Self {
        let mut rig = Rig::new();
        gate_serving(&mut rig);
        Self {
            rig,
            rng: seed,
            applied: START,
            pending: None,
            reader: 1000,
            bound: BTreeMap::from([(snap(0), START)]),
        }
    }

    fn below(&mut self, n: usize) -> usize {
        self.rng = self
            .rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        usize::try_from(self.rng >> 33).expect("31 bits") % n
    }

    fn pick(&mut self, moves: &[Move]) -> Move {
        moves[self.below(moves.len())]
    }

    /// A read through the gate: asked, and admitted when it asked.
    fn read(&mut self, kind: EventKind) -> Vec<EffectKind> {
        let mut effects = self.rig.step(kind);
        if let [EffectKind::Kernel(KernelEffect::AuthorityCheck {
            checkpoint: Checkpoint::Read,
            correlation,
            ..
        })] = effects.as_slice()
        {
            let admit = self.rig.gate_answer(*correlation, Verdict::Admit);
            effects.extend(self.rig.step(admit));
        }
        effects
    }

    fn take(&mut self, m: Move) -> Vec<EffectKind> {
        let view = self.rig.view();
        let recheck = view.pending.and_then(|p| p.recheck);
        let effects = match (m, self.pending) {
            (Move::Apply, _) => {
                self.applied += 1;
                self.rig.snapshot_at(self.applied);
                Vec::new()
            }
            (Move::Candidate, None) if view.pending.is_none() => {
                let seq = view.published.seq.0 + 1;
                if self.applied < seq {
                    self.applied = seq;
                    self.rig.snapshot_at(seq);
                }
                self.pending = Some(seq);
                self.rig.step(candidate(seq, seq))
            }
            (Move::Gained, Some(seq)) => {
                self.rig.qualify(seq);
                self.rig.step(gained(seq))
            }
            (Move::Lost, Some(seq)) => {
                self.rig
                    .p1
                    .scripted_mut(NODE, P)
                    .expect("scripted")
                    .set_qualifies(Seq(seq), false);
                self.rig.step(lost(seq))
            }
            (Move::Admit, _) if recheck.is_some() => self.rig.step(answer(
                Checkpoint::Publication,
                recheck.expect("guarded"),
                Verdict::Admit,
            )),
            (Move::Deny, _) if recheck.is_some() => self.rig.step(answer(
                Checkpoint::Publication,
                recheck.expect("guarded"),
                Verdict::Deny(DenyReason::Expired),
            )),
            (Move::Deadline, _) if view.pending.is_some() => {
                let version = view.pending.expect("guarded").deadline;
                self.rig.step(deadline(P, version.0))
            }
            (Move::Fence, _) => self.rig.step(fence(FenceScope::Node, DenyReason::Expired)),
            (Move::Bind, _) if !view.opening.is_empty() => {
                let (&handle, asked) = view.opening.iter().next().expect("guarded");
                let at = if self.below(2) == 0 {
                    asked.seq.0
                } else {
                    self.applied
                };
                self.bound.insert(handle, at);
                self.rig.step(ready_of(handle, at))
            }
            (Move::Fresh, _) => {
                self.reader += 1;
                self.read(read(self.reader))
            }
            (Move::Previous, _) => {
                self.reader += 1;
                self.read(read_previous(self.reader))
            }
            _ => Vec::new(),
        };
        if self.rig.view().pending.is_none() {
            self.pending = None;
        }
        effects
    }
}

/// How many seeds each interleaving row walks, and how many moves each walk takes.
const SEEDS: u64 = 1000;
const MOVES: usize = 32;

/// M7A-109 (invariant 2, "no read sees the raw applied prefix"): 1,000 seeded interleavings of
/// storage applying ahead, candidates, publishes, deadlines, view binds and both reads. Every
/// value a `Fresh` read is served was written at or below the published position at the step it
/// was served, and every handle a `PreviousPublished` read is handed was bound at or below it.
/// The counters at the end make the walk non-vacuous: it must serve reads, hand out views, and
/// spend steps with storage ahead of the published prefix.
#[retcd_test]
fn m7a_109_barrier_never_hands_out_applied_prefix() {
    let moves = [
        Move::Apply,
        Move::Candidate,
        Move::Candidate,
        Move::Gained,
        Move::Gained,
        Move::Admit,
        Move::Admit,
        Move::Lost,
        Move::Deadline,
        Move::Bind,
        Move::Bind,
        Move::Fresh,
        Move::Fresh,
        Move::Previous,
        Move::Previous,
    ];
    let (mut served, mut handed, mut above) = (0_u32, 0_u32, 0_u32);
    for seed in 0..SEEDS {
        let mut walk = Walk::new(seed);
        for step in 0..MOVES {
            let m = walk.pick(&moves);
            let effects = walk.take(m);
            let published = walk.rig.view().published.seq.0;
            above += u32::from(walk.applied > published);
            for effect in &effects {
                match effect {
                    EffectKind::Reply(ReplyEffect::Read {
                        value: Some((version, _)),
                        ..
                    }) => {
                        served += 1;
                        assert!(
                            *version <= published,
                            "seed {seed} step {step} {m:?}: served {version} over published {published}"
                        );
                    }
                    EffectKind::Kernel(KernelEffect::Publication(
                        PublicationEffect::Snapshot {
                            handle: Ok(handle), ..
                        },
                    )) => {
                        handed += 1;
                        let at = walk.bound[handle];
                        assert!(
                            at <= published,
                            "seed {seed} step {step} {m:?}: handed a view bound at {at} over published {published}"
                        );
                    }
                    _ => {}
                }
            }
        }
    }
    eprintln!("m7a_109: served {served}, handed {handed}, steps above the prefix {above}");
    assert!(
        served > 500 && handed > 1500 && above > 10_000,
        "a vacuous walk: served {served}, handed {handed}, steps above the prefix {above}"
    );
}

/// The `pub` functions in P1's four source files (any visibility scope, any qualifiers), sorted,
/// and why none is an applied-prefix accessor.
///
/// - `publication.rs`: the timer id; construction and install; scripting the KA-8 fake; the kernel
///   and its view; how many read keys a slot holds; the step. `forget_node` drops a node's slots
///   when the simulator restarts it (V-R35) and returns nothing, so it reads no prefix.
/// - `kernel.rs`: the handle arithmetic of P1's block (`publication_snapshot`,
///   `is_publication_snapshot`: a number, bound by storage only when P1 asks); construction;
///   boot, lineage, the **published** position; the view; the step. `open_view` (`pub(crate)`)
///   mints a handle asked for at the **published** position, for a publish and for `install`
///   (A-R71); storage binds it, and P1 keeps it only if bound there. `try_seed` and
///   `seed_pending` (M7A-194) read the dedup namespace of the step's snapshot — rows at or below
///   `retained_through`, the recovered cutoff, which is the published position — and expose
///   whether that read is still owed; neither hands back a view or a sequence above it.
/// - `status.rs`: the KA-9 wire map and the status index. `entry` hands back a status entry: its
///   `seq` is the request's own position and its `snapshot` is `None` while pending, so it names no
///   view anyone can read. `restore` and `recovered_outcome` (M7A-194) are the seed's write and
///   the fold rule it shares with `fold_recovered`; both take entries in and hand nothing out.
///   `is_retired` (reviewer R-1, A-R91) answers whether a generation was retired in this boot —
///   a generation, never a sequence or a view.
///   `with_cap` and `cap` (PR #1 R1-F006, K-A-12) build the index with a bound and report it: a
///   number, never a sequence or a view.
/// - `view.rs`: the KA-8 fake's constructor and scripting.
const PUB_FNS: [(&str, &str, &[&str]); 4] = [
    (
        "publication.rs",
        include_str!("../src/publication.rs"),
        &[
            "forget_node",
            "held_read_keys",
            "install",
            "kernel",
            "new",
            "post_apply_timer",
            "script_replication",
            "scripted_mut",
            "step_with",
            "view",
            "with_config",
        ],
    ),
    (
        "publication/kernel.rs",
        include_str!("../src/publication/kernel.rs"),
        &[
            "apply",
            "boot",
            "is_publication_snapshot",
            "lineage",
            "new",
            "open_view",
            "publication_snapshot",
            "published",
            "seed_pending",
            "try_seed",
            "view",
        ],
    ),
    (
        "publication/status.rs",
        include_str!("../src/publication/status.rs"),
        &[
            "cap",
            "entry",
            "fold_recovered",
            "is_empty",
            "is_retired",
            "len",
            "lookup",
            "lookup_any",
            "new",
            "open",
            "record",
            "recovered_outcome",
            "restore",
            "retire",
            "to_wire",
            "trim",
            "with_cap",
        ],
    ),
    (
        "publication/view.rs",
        include_str!("../src/publication/view.rs"),
        &["new", "set_digest", "set_qualifies"],
    ),
];

/// The trait impls in P1's four source files. A trait method needs no `pub`, so an impl for a P1
/// type is an accessor [`PUB_FNS`] cannot see. `Module for Publication` is the step, the one way
/// in; `Default for PubConfig` is configuration; the two `ReplicationView` impls are R1's tracker
/// and the KA-8 fake, which P1 reads, not P1's own state. `Default for StatusIndex` (PR #1 R1-F006)
/// is an empty index with the default cap: it holds nothing to read.
const TRAIT_IMPLS: [&str; 5] = [
    "impl Default for PubConfig",
    "impl Default for StatusIndex",
    "impl Module for Publication",
    "impl ReplicationView for ProgressTracker",
    "impl ReplicationView for ScriptedReplication",
];

/// The names of the `pub` functions in `source`, sorted: any visibility scope (`pub(crate)`) and
/// any qualifiers (`const`, `async`, `unsafe`, `extern "C"`).
fn pub_fns(source: &str) -> Vec<&str> {
    let mut names: Vec<&str> = source
        .lines()
        .filter_map(|line| {
            let rest = line.trim_start().strip_prefix("pub")?;
            let rest = match rest.strip_prefix('(') {
                Some(scoped) => scoped.split_once(')')?.1,
                None => rest.strip_prefix(' ')?,
            };
            let (qualifiers, name) = rest.split_once("fn ")?;
            qualifiers
                .split_whitespace()
                .all(|q| matches!(q, "const" | "async" | "unsafe" | "extern" | "\"C\""))
                .then(|| name.split(['(', '<']).next())?
        })
        .collect();
    names.sort_unstable();
    names
}

/// The `impl … for …` headers in `source`.
fn trait_impls(source: &str) -> impl Iterator<Item = &str> {
    source
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("impl") && line.contains(" for "))
        .map(|line| line.trim_end_matches('{').trim_end())
}

/// M7A-110 (invariant 2, source level): P1 has no accessor for the applied-but-unpublished prefix.
///
/// How it fails:
/// - **Compile error E0027** ("pattern does not mention field") if `PubStateView` or `PendingView`
///   gains a field: both are destructured below with no `..`, so a new field (an applied `seq` on
///   the pending view, say) cannot land without this row being re-read.
/// - **Assertion** naming the file if any `pub` function, whatever its scope or qualifiers, is
///   added to or removed from P1's four source files: [`PUB_FNS`] is the whole list, each entry
///   justified. The same for trait impls, whose methods need no `pub`: [`TRAIT_IMPLS`].
/// - **Assertion** if a view asked for is not at the published position, or a pending candidate's
///   status entry names a view.
#[retcd_test]
fn m7a_110_no_accessor_for_applied_prefix_source_check() {
    for (file, source, allowed) in PUB_FNS {
        assert_eq!(
            pub_fns(source),
            allowed,
            "{file}: the pub fn list changed; re-read invariant 2 before extending PUB_FNS"
        );
    }
    let mut impls: Vec<&str> = PUB_FNS
        .iter()
        .flat_map(|(_, source, _)| trait_impls(source))
        .collect();
    impls.sort_unstable();
    assert_eq!(
        impls, TRAIT_IMPLS,
        "a trait impl changed; re-read invariant 2 before extending TRAIT_IMPLS"
    );

    let mut rig = Rig::new();
    rig.pending_with_recheck(5);
    rig.snapshot_at(6);
    let PubStateView {
        boot: _,
        lineage: _,
        published,
        pending,
        awaiting_reply,
        waiters: _,
        authority: _,
        mode: _,
        status,
        kept,
        opening,
        gated: _,
        read_check: _,
    } = rig.view();
    let PendingView {
        request,
        qualifying: _,
        recheck: _,
        deadline: _,
        replied: _,
    } = pending.expect("pending");
    assert_eq!(published.seq, Seq(START));
    assert_eq!(request, req(5));
    assert!(awaiting_reply.is_empty());
    assert_eq!(kept, None, "the install's view is not bound in this rig");
    assert!(
        opening.values().all(|at| *at == published),
        "every view asked for is at the published position: {opening:?}"
    );
    let entry = status.entry(GEN, req(5)).expect("the candidate's status");
    assert_eq!(
        (entry.outcome, entry.snapshot),
        (StatusOutcome::Unknown, None),
        "a pending candidate's status names no view"
    );
}

/// `P` with candidate 5 pending and [`WAITER_CAP`] fresh readers queued behind it. Shared by
/// M7A-111's cap half and its drain half.
fn full_queue() -> Rig {
    let mut rig = Rig::new();
    rig.pending_with_recheck(5);
    for n in 0..CAP {
        assert_eq!(rig.admitted(read(100 + n)), vec![]);
    }
    assert_eq!(rig.view().waiters.len(), WAITER_CAP);
    rig
}

/// M7A-111 (K-A-14): the reader over the cap is refused `Overloaded`, and a freeze answers every
/// waiter once and empties the queue. No snapshot identity is asserted (TD-14). The plan names
/// `ReplyEffect::Failed{identity, error}`; the landed read reply is `ReplyEffect::Read` with a
/// `Rejected(code)` outcome (lead ruling F-R7), and the code is §3.4's for the freeze's reason,
/// not `UNKNOWN_OUTCOME` (A-R72a Q3).
#[retcd_test]
fn m7a_111_waiter_cap_overloaded_and_drained_on_freeze() {
    let mut rig = full_queue();
    assert_eq!(
        rig.step(read(200)),
        vec![rejected_read(200, ErrorKind::Overloaded)]
    );
    assert!(!rig.view().waiters.contains(&req(200)));
    assert_eq!(
        rig.step(fence(FenceScope::Node, DenyReason::Expired)),
        (0..CAP)
            .map(|n| rejected_read(100 + n, ErrorKind::LeaseExpired))
            .collect::<Vec<_>>()
    );
    assert!(rig.view().waiters.is_empty());
}

/// M7A-112 (design §4.1; spec §5.2 step 7): over 1,000 seeded interleavings of publishes, fences,
/// quarantines, `Lost` edges and deadlines, the published position never decreases. `Recovered`
/// is not in the plan's input and is not walked: it rebases to a new generation, which M7A-116 and
/// the recovery rows own. The counters make the walk non-vacuous.
#[retcd_test]
fn m7a_112_published_seq_monotone_never_decreases() {
    let moves = [
        Move::Candidate,
        Move::Candidate,
        Move::Gained,
        Move::Gained,
        Move::Admit,
        Move::Admit,
        Move::Deny,
        Move::Lost,
        Move::Fence,
        Move::Deadline,
        Move::Apply,
    ];
    let (mut publishes, mut quarantines, mut fences) = (0_u32, 0_u32, 0_u32);
    for seed in 0..SEEDS {
        let mut walk = Walk::new(seed);
        let mut last = walk.rig.view().published;
        for step in 0..MOVES {
            let m = walk.pick(&moves);
            let effects = walk.take(m);
            let now = walk.rig.view().published;
            assert!(
                now >= last,
                "seed {seed} step {step} {m:?}: published went {last:?} -> {now:?}"
            );
            publishes += u32::from(now > last);
            quarantines += u32::from(effects.iter().any(|e| {
                matches!(
                    e,
                    EffectKind::Kernel(KernelEffect::Publication(
                        PublicationEffect::Quarantined { .. }
                    ))
                )
            }));
            fences += u32::from(matches!(m, Move::Fence));
            last = now;
        }
    }
    eprintln!("m7a_112: publishes {publishes}, quarantines {quarantines}, fences {fences}");
    assert!(
        publishes > 500 && quarantines > 250 && fences > 1000,
        "a vacuous walk: publishes {publishes}, quarantines {quarantines}, fences {fences}"
    );
}

// ---- test-plan rows: §5.4 status --------------------------------------------------------------

/// Request 5 published at 5 and its reply answered. Shared by M7A-113, M7A-114, M7A-115 and
/// M7A-118.
fn status_rig() -> Rig {
    let mut rig = Rig::new();
    let r = rig.published(5);
    assert_eq!(
        rig.step(answer(Checkpoint::Reply, r, Verdict::Admit)),
        vec![txn_reply(5, 5)]
    );
    rig
}

fn trim_below(rig: &mut Rig, seq: u64) {
    rig.step(EventKind::Kernel(KernelEvent::StatusTrim {
        generation: GEN,
        below: Seq(seq),
    }));
}

/// M7A-113 (ADR 0004 "retention boundary three answers"; A-R10): present, trimmed within the live
/// generation, and retired answer `Published` / `Unknown` / `StatusExpired` on state, and
/// `Resolved` / `Unknown` / `Expired` on the wire (KA-9).
#[retcd_test]
fn m7a_113_status_retention_boundary_three_answers() {
    let mut rig = status_rig();
    assert_status_published(&mut rig, 5);

    trim_below(&mut rig, 6);
    assert_eq!(
        rig.view().status.lookup(req(5), GEN),
        StatusOutcome::Unknown
    );
    assert_eq!(status_answer(&mut rig, 5, Some(GEN)), TxnStatus::Unknown);

    rig.step(EventKind::Kernel(KernelEvent::RetireGeneration {
        generation: GEN,
    }));
    assert_eq!(
        rig.view().status.lookup(req(5), GEN),
        StatusOutcome::StatusExpired
    );
    assert_eq!(status_answer(&mut rig, 5, Some(GEN)), TxnStatus::Expired);
}

/// M7A-114 (A-R10 "never held ⇒ StatusExpired"): a generation this node never held answers
/// `StatusExpired`, not `Unknown`. Twin (one fact, the generation is live): the same absent entry
/// in `GEN` after a trim answers `Unknown`.
#[retcd_test]
fn m7a_114_status_never_held_generation_is_status_expired() {
    let mut rig = status_rig();
    let never = Generation(GEN.0 + 7);
    assert_eq!(
        rig.view().status.lookup(req(5), never),
        StatusOutcome::StatusExpired
    );
    assert_eq!(status_answer(&mut rig, 5, Some(never)), TxnStatus::Expired);

    trim_below(&mut rig, 6);
    assert_eq!(
        rig.view().status.lookup(req(5), GEN),
        StatusOutcome::Unknown
    );
    assert_eq!(status_answer(&mut rig, 5, Some(GEN)), TxnStatus::Unknown);
}

/// The design's five status outcomes (KA-9), each by name. A sixth member fails to compile here.
const fn outcome_name(outcome: &StatusOutcome) -> &'static str {
    match outcome {
        StatusOutcome::Published { .. } => "Published",
        StatusOutcome::Unknown => "Unknown",
        StatusOutcome::Rejected { .. } => "Rejected",
        StatusOutcome::RecoveredApplied { .. } => "RecoveredApplied",
        StatusOutcome::StatusExpired => "StatusExpired",
    }
}

/// M7A-115 (invariant 5; KA-7; KA-9, TD-10): no status answer proves nonexecution. The exhaustive
/// match over the five outcomes has no `NotExecuted`; an absent identity in a live generation is
/// `Unknown`; and no P1 status answer is `TxnStatus::Unresolved`: not `to_wire` over all five
/// outcomes, and not any answer P1 gives across the states it reaches here (pending, published,
/// absent, never held, no generation named, trimmed, recovered, retired). The plan's "whole
/// binary" is [`assert_no_status_is_unresolved`], which every `Rig` step runs.
#[retcd_test]
fn m7a_115_status_never_proves_nonexecution() {
    let all = [
        StatusOutcome::Published {
            result: result(P, 5),
        },
        StatusOutcome::Unknown,
        StatusOutcome::Rejected {
            error: ErrorKind::Unavailable,
        },
        StatusOutcome::RecoveredApplied {
            result: result(P, 5),
        },
        StatusOutcome::StatusExpired,
    ];
    assert_eq!(
        all.iter().map(outcome_name).collect::<Vec<_>>(),
        [
            "Published",
            "Unknown",
            "Rejected",
            "RecoveredApplied",
            "StatusExpired"
        ]
    );
    for outcome in all {
        assert!(
            !matches!(to_wire(outcome), TxnStatus::Unresolved { .. }),
            "{outcome:?}"
        );
    }

    let mut rig = status_rig();
    assert_eq!(
        rig.view().status.lookup(req(99), GEN),
        StatusOutcome::Unknown,
        "absent in a live generation"
    );
    rig.step(candidate(6, 6));
    let mut answers = Vec::new();
    for (n, generation) in [
        (99, Some(GEN)),
        (5, Some(GEN)),
        (6, Some(GEN)),
        (5, None),
        (5, Some(Generation(GEN.0 + 7))),
    ] {
        answers.push(status_answer(&mut rig, n, generation));
    }
    trim_below(&mut rig, 6);
    answers.push(status_answer(&mut rig, 5, Some(GEN)));
    rig.step(recovered(PartitionMode::Active, 5, false, Some(6)));
    for n in [5, 6, 99] {
        answers.push(status_answer(&mut rig, n, Some(GEN)));
    }
    rig.step(EventKind::Kernel(KernelEvent::RetireGeneration {
        generation: GEN,
    }));
    answers.push(status_answer(&mut rig, 5, Some(GEN)));
    assert!(
        !answers
            .iter()
            .any(|a| matches!(a, TxnStatus::Unresolved { .. })),
        "{answers:?}"
    );
    for want in ["Resolved", "Unknown", "Expired"] {
        assert!(
            answers.iter().any(|a| format!("{a:?}").starts_with(want)),
            "the sweep never reached {want}: {answers:?}"
        );
    }
}

/// M7A-116 (K-A-52; ADR 0004 "Recovery folds status by sequence, not by presence"): the
/// kernel-level twin of M7A-172. Through `Recovered` with `retained_through: k` and
/// `discarded_from: k + 1`, the identities at `k − 1` and `k` answer `RecoveredApplied` and the one
/// at `k + 2` answers `Unknown`; with `uncertain: true`, all three answer `Unknown`. None answers
/// `Published` for the old generation, and `StatusExpired` is not produced in either trace.
#[retcd_test]
fn m7a_116_recovery_folds_status_by_sequence_not_by_presence() {
    const K: u64 = 7;
    for uncertain in [false, true] {
        let mut rig = Rig::new();
        for seq in 5..=K + 2 {
            rig.published(seq);
        }
        rig.step(recovered(PartitionMode::Active, K, uncertain, Some(K + 1)));
        for (seq, retained) in [(K - 1, true), (K, true), (K + 2, false)] {
            let applied = retained && !uncertain;
            let state = rig.view().status.lookup(req(seq), GEN);
            let (want_state, want_wire) = if applied {
                (
                    StatusOutcome::RecoveredApplied {
                        result: result(P, seq),
                    },
                    TxnStatus::Resolved(TxnResult {
                        outcome: Outcome::RecoveredApplied,
                        ..result(P, seq)
                    }),
                )
            } else {
                (StatusOutcome::Unknown, TxnStatus::Unknown)
            };
            assert_eq!(state, want_state, "uncertain={uncertain} seq {seq}");
            assert_ne!(state, StatusOutcome::StatusExpired);
            assert_eq!(
                status_answer(&mut rig, seq, Some(GEN)),
                want_wire,
                "uncertain={uncertain} seq {seq}"
            );
        }
    }
}

/// M7A-118 (KA-4, KA-9): a status query is answered by one `ReplyEffect::Status{identity, status}`
/// in the returned effect vector, with `status` KA-9's map of the state-side outcome. The match
/// below names both fields and no `..`, so a payload field cannot be added without failing
/// compilation.
#[retcd_test]
fn m7a_118_status_reply_effect_carries_outcome_not_bytes() {
    let mut rig = status_rig();
    rig.step(candidate(6, 6));
    for (n, generation) in [
        (5, Some(GEN)),
        (6, Some(GEN)),
        (5, Some(Generation(GEN.0 + 7))),
        (5, None),
    ] {
        let state = generation.map_or_else(
            || rig.view().status.lookup_any(req(n)),
            |g| rig.view().status.lookup(req(n), g),
        );
        let effects = rig.step(status(n, generation));
        let [EffectKind::Reply(reply)] = effects.as_slice() else {
            panic!("expected one reply, got {effects:?}");
        };
        match reply {
            ReplyEffect::Status { identity, status } => {
                assert_eq!(*identity, req(n));
                assert_eq!(*status, to_wire(state), "{n} {generation:?}");
            }
            other => panic!("expected a status reply, got {other:?}"),
        }
    }
}

// ---- test-plan rows: §8 -------------------------------------------------------------------------

/// M7A-141 (K-A-34; ADR 0007 "Stale authority answer is dropped", variants 1 and 2).
///
/// Variant 1: A1 fences `Expired` and pushes the view at authority seq 2; the pre-fence `Admit`
/// (seq 1) is dropped, nothing publishes or replies, and the state is unchanged. The fence already
/// cleared the recheck, so there is nothing to re-ask. Twin: the `Admit{authority_seq 2}` to the
/// check asked after the fence is consumed (a lost-authority freeze may publish, K-A-47).
/// Variant 2: that accepted `Admit` delivered again is dropped and publishes nothing twice.
///
/// The plan predates lead ruling A-R69 F2: with no fence, a stale answer to the check still in
/// flight re-asks it under a fresh correlation; and a duplicate of that stale answer asks nothing
/// (A-R71 N03). Both are asserted in the last part.
#[retcd_test]
fn m7a_141_stale_authority_answer_dropped_by_authority_seq_and_duplicate() {
    let mut rig = Rig::new();
    let c = rig.pending_with_recheck(5);
    rig.step(fence(FenceScope::Node, DenyReason::Expired));
    assert_eq!(rig.step(view_push(P, 2)), vec![]);
    let before = rig.view();
    rig.snapshot_at(5);
    assert_eq!(
        rig.step(answer_at(Checkpoint::Publication, c, Verdict::Admit, 1)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
    assert_eq!(rig.view(), before, "state unchanged");

    let c2 = publication_check(&rig.step(gained(5)));
    let accepted = answer_at(Checkpoint::Publication, c2, Verdict::Admit, 2);
    let effects = rig.step(accepted.clone());
    assert!(published_any(&effects), "{effects:?}");
    assert_eq!(rig.view().published.seq, Seq(5));

    let again = rig.step(accepted);
    assert_eq!(
        again,
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
    assert_eq!(replies_for(&again, 5), 0);
    assert_eq!(rig.view().published.seq, Seq(5));

    // A-R69 F2 and A-R71 N03: no fence; A1 resyncs while the check is in flight.
    let mut rig = Rig::new();
    let c = rig.pending_with_recheck(5);
    assert_eq!(rig.step(view_push(P, 2)), vec![]);
    let stale = answer_at(Checkpoint::Publication, c, Verdict::Admit, 1);
    let reasked = rig.step(stale.clone());
    assert_eq!(
        reasked[0],
        ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)
    );
    let fresh = publication_check(&reasked[1..]);
    assert_ne!(fresh, c);
    assert_eq!(
        rig.step(stale),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)],
        "A-R71 N03: no third check"
    );
    assert_eq!(rig.view().pending.expect("kept").recheck, Some(fresh));
    assert_eq!(rig.view().published.seq, Seq(START));
}

/// M7A-142 (K-A-34, ADR 0007 variant 3): A1 decides `Admit{authority_seq 1}` and fences at the same
/// tick `t`, pushing a view at authority seq 2 valid through `t − 1` with `past_horizon Expired`.
/// P1 holds that view when the `Admit` lands: dropped, nothing published. Twin (one fact: the
/// answer carries authority seq 2, same `decided_at`): ours, and it publishes. The tick is the
/// same in both; only the sequence differs.
///
/// The drop re-asks the check under a fresh correlation (lead ruling A-R69 F2), which is neither
/// a dispatch nor a publish. The twin's "then refused by the mode guard (M7A-140)" is T1's
/// dispatch guard; P1's half is only that the answer is ours.
#[retcd_test]
fn m7a_142_authority_answer_and_fence_at_same_tick_dropped() {
    for authority_seq in [1, 2] {
        let mut rig = Rig::new();
        let c = rig.pending_with_recheck(5);
        let t = rig.now + 10;
        let mut view = authority_view(2);
        view.valid_through_tick = Tick(t - 1);
        view.past_horizon = DenyReason::Expired;
        assert_eq!(
            rig.step(EventKind::Kernel(KernelEvent::Authority(
                AuthorityEvent::View(view)
            ))),
            vec![]
        );
        assert_eq!(rig.now, t);
        let mut decided = decision(P, Checkpoint::Publication, c, Verdict::Admit, authority_seq);
        decided.decided_at = Tick(t);
        rig.snapshot_at(5);
        let effects = rig.step(answer_event(decided));
        if authority_seq == 1 {
            assert_eq!(
                effects[0],
                ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)
            );
            assert_ne!(publication_check(&effects[1..]), c, "A-R69 F2 re-ask");
            assert_eq!(rig.view().published.seq, Seq(START));
        } else {
            assert!(published_any(&effects), "{effects:?}");
            assert_eq!(rig.view().published.seq, Seq(5));
        }
    }
}

/// M7A-152 (K-A-40): the publish moves the reply into `awaiting_reply`, cancels the deadline and
/// asks the `Reply` check; a deadline fired anyway is a stale timer: no reply, no freeze.
#[retcd_test]
fn m7a_152_publish_moves_reply_into_awaiting_reply_and_cancels_deadline() {
    let mut rig = Rig::new();
    let effects = rig.publish_only(5);
    assert!(effects.contains(&notify(5)), "{effects:?}");
    assert!(effects.contains(&cancel(1)), "{effects:?}");
    let r = reply_check(&effects);
    let view = rig.view();
    assert_eq!(
        view.awaiting_reply.into_iter().collect::<Vec<_>>(),
        vec![(
            r,
            AwaitingReply {
                request: req(5),
                seq: Seq(5),
                result: result(P, 5),
                replied: false,
            }
        )]
    );
    assert_eq!(view.pending, None);

    let before = rig.view();
    assert_eq!(
        rig.step(deadline(P, 1)),
        vec![ignored(AuthorityIgnoreReason::StaleTimer)]
    );
    assert_eq!(rig.view(), before);
    assert_eq!(rig.view().mode, PubMode::Serving);
}

/// M7A-153 (ADR 0007 "Reply checkpoint outlives publication"): the `Reply` answer is held 5,000
/// ticks, past the post-apply deadline's value, and a firing of the cancelled deadline changes
/// nothing. Then `Admit` replies once, or `Deny` withholds; either way the entry goes, and status
/// answers `Published` on state and `Resolved` on the wire at every probe after the publish.
#[retcd_test]
fn m7a_153_reply_checkpoint_outlives_publication() {
    for verdict in [Verdict::Admit, Verdict::Deny(DenyReason::Expired)] {
        let mut rig = Rig::new();
        let r = rig.published(5);
        let published_at = rig.now;
        while rig.now < published_at + 5000 {
            rig.now += 490;
            assert_status_published(&mut rig, 5);
        }
        assert!(rig.now >= published_at + DEADLINE);
        assert_eq!(
            rig.step(deadline(P, 1)),
            vec![ignored(AuthorityIgnoreReason::StaleTimer)]
        );
        assert!(rig.view().awaiting_reply.contains_key(&r));

        let effects = rig.step(answer(Checkpoint::Reply, r, verdict));
        match verdict {
            Verdict::Admit => assert_eq!(effects, vec![txn_reply(5, 5)]),
            Verdict::Deny(_) => {
                assert_eq!(effects, vec![ignored(AuthorityIgnoreReason::ReplyWithheld)]);
            }
        }
        assert!(rig.view().awaiting_reply.is_empty(), "{verdict:?}");
        assert_status_published(&mut rig, 5);
    }
}

/// M7A-154 (invariant 6 across both slots): the deadline already replied `Unknown`, so the late
/// publish's entry carries `replied: true` (the one fact vs M7A-153), and the `Reply` admit is
/// suppressed. The one reply the identity ever gets is the `Unknown`; status is `Published`.
#[retcd_test]
fn m7a_154_reply_admit_after_timeout_suppressed_status_published() {
    let mut rig = Rig::new();
    let (mut all, r) = published_after_the_deadline(&mut rig);
    assert!(rig.view().awaiting_reply[&r].replied);
    let effects = rig.step(answer(Checkpoint::Reply, r, Verdict::Admit));
    assert_eq!(
        effects,
        vec![ignored(AuthorityIgnoreReason::ReplySuppressedAfterTimeout)]
    );
    all.extend(effects);
    assert_eq!(replies_for(&all, 5), 1, "{all:?}");
    assert!(
        all.contains(&unknown_reply(5)),
        "the one reply is the Unknown"
    );
    assert!(rig.view().awaiting_reply.is_empty());
    assert_status_published(&mut rig, 5);
}

/// M7A-158 (B-R29, K-A-56): `Serving` with an owed reply for 5, candidate 6 checking and two
/// waiters. A block drains the waiters once and leaves the owed reply; a fence then withholds it
/// once, drains nothing and leaves the block; `Recovered` maps each of the four `PartitionMode`
/// variants, carrying a `Blocked` reason byte-equal to the recovery's own, not the block's.
#[retcd_test]
fn m7a_158_blocked_then_freeze_then_recovered_mode_sequence() {
    let carried = BlockReason::DivergenceRequiresOperator {
        diverged: vec![CopyId(3)],
    };
    assert_ne!(carried, block());
    for (mode, expected) in [
        (PartitionMode::Active, PubMode::Serving),
        (PartitionMode::DegradedRf2, PubMode::Serving),
        (
            PartitionMode::ReadOnly,
            PubMode::Frozen {
                cause: FreezeCause::RecoveryReadOnly,
            },
        ),
        (
            PartitionMode::Blocked {
                reason: carried.clone(),
            },
            PubMode::Blocked {
                reason: carried.clone(),
            },
        ),
    ] {
        let mut rig = Rig::new();
        let c5 = rig.published(5);
        rig.pending_with_recheck(6);
        assert_eq!(rig.admitted(read(21)), vec![]);
        assert_eq!(rig.admitted(read(22)), vec![]);
        let blocked = PubMode::Blocked { reason: block() };

        let mut all = rig.step(block_event(block()));
        assert_eq!(
            all,
            vec![
                ignored(AuthorityIgnoreReason::Blocked { reason: block() }),
                rejected_read(21, ErrorKind::ProtectionPaused),
                rejected_read(22, ErrorKind::ProtectionPaused),
            ],
            "{mode:?}"
        );
        let view = rig.view();
        assert_eq!(view.mode, blocked);
        assert!(view.awaiting_reply.contains_key(&c5), "untouched");
        assert_eq!(view.pending.expect("kept").request, req(6));

        let fenced = rig.step(fence(FenceScope::Node, DenyReason::Expired));
        assert_eq!(
            fenced,
            vec![
                ignored(AuthorityIgnoreReason::ReplyWithheld),
                ignored(AuthorityIgnoreReason::FenceWhileBlocked),
            ],
            "{mode:?}"
        );
        assert_eq!(
            rig.view().mode,
            blocked,
            "{mode:?}: the fence keeps the block"
        );
        all.extend(fenced);

        all.extend(rig.step(recovered(mode.clone(), 5, false, None)));
        let view = rig.view();
        assert_eq!(view.mode, expected, "{mode:?}");
        assert_eq!(view.pending, None, "{mode:?}");
        for n in [21, 22] {
            assert_eq!(read_replies_for(&all, n), 1, "{mode:?}: reader {n}");
        }
        let withheld = all
            .iter()
            .filter(|e| **e == ignored(AuthorityIgnoreReason::ReplyWithheld))
            .count();
        assert_eq!(withheld, 1, "{mode:?}");
    }
}

/// M7A-159 (B-R29, K-A-57): in `Blocked` with candidate 6 checking, a fresh read is refused on
/// arrival and never queued; the `Admit` is refused with its own fact and clears the recheck; the
/// deadline still answers `Unknown` and the block stands; `ModeQuery` says `Blocked`; a second
/// block is a fact.
///
/// The plan's "`PreviousPublished` still answers the published snapshot" predates lead ruling
/// A-R72c: the mode refuses both reads alike, so a previous read in `Blocked` is refused
/// `ProtectionPaused`. What survives of the clause is that the kept view is not released.
#[retcd_test]
fn m7a_159_blocked_refuses_fresh_reads_and_publish_and_survives_deadline() {
    let mut rig = Rig::new();
    rig.published(5);
    let c6 = rig.pending_with_recheck(6);
    let reason = block();
    rig.step(block_event(reason.clone()));
    let blocked = PubMode::Blocked {
        reason: reason.clone(),
    };

    assert_eq!(
        rig.step(read(21)),
        vec![rejected_read(21, ErrorKind::ProtectionPaused)]
    );
    assert!(rig.view().waiters.is_empty());
    assert!(rig.view().gated.is_empty());

    assert_eq!(
        rig.step(answer(Checkpoint::Publication, c6, Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::PublishRefusedBlocked)]
    );
    assert_eq!(rig.view().pending.expect("kept").recheck, None);
    assert_eq!(rig.view().published.seq, Seq(5));

    let t = rig.now + 10;
    assert_eq!(
        rig.step(deadline(P, 2)),
        vec![
            status_write(6, 6, StatusOutcome::Unknown, t),
            unknown_reply(6)
        ]
    );
    assert_eq!(rig.view().mode, blocked);

    assert_eq!(
        rig.step(EventKind::Kernel(KernelEvent::Publication(
            PublicationEvent::ModeQuery { identity: req(40) }
        ))),
        vec![EffectKind::Kernel(KernelEffect::Publication(
            PublicationEffect::Mode {
                identity: req(40),
                mode: blocked.clone(),
            }
        ))]
    );
    assert_eq!(
        rig.step(block_event(reason)),
        vec![ignored(AuthorityIgnoreReason::AlreadyBlocked)]
    );
    assert_eq!(
        rig.step(read_previous(30)),
        vec![previous(30, Err(ErrorKind::ProtectionPaused))]
    );
    assert_eq!(rig.view().kept, Some(snap(1)), "the view at 5 is kept");
    assert_eq!(rig.view().mode, blocked);
}

/// M7A-167 (K-A-45; ADR 0007 "Post-apply candidate under a lost authority is accepted, not
/// dropped"): in `Frozen{AuthorityLost(Expired)}`, `Frozen{LocalStorageFenced}` and `Blocked`, a
/// candidate arms the deadline, writes `Unknown` and says so, is pending, and leaves the mode
/// alone. The fact carries the mode. Total-arm twin (one fact, the mode): in `Frozen{Unresolved}`
/// or read-only, the candidate is `CandidateUnreachable{mode}` and arms nothing.
#[retcd_test]
fn m7a_167_post_apply_candidate_under_lost_authority_is_accepted_not_dropped() {
    let entries: [(&str, Enter); 3] = [
        ("lost authority", enter_lost_authority),
        ("fenced storage", enter_storage_fenced),
        ("blocked", enter_blocked),
    ];
    for (label, enter) in entries {
        let mut rig = Rig::new();
        enter(&mut rig);
        let mode = rig.view().mode;
        let t = rig.now + 10;
        assert_eq!(
            rig.step(candidate(7, 7)),
            vec![
                arm(1, t + DEADLINE),
                status_write(7, 7, StatusOutcome::Unknown, t),
                ignored(AuthorityIgnoreReason::CandidateWhileNotServing),
            ],
            "{label}"
        );
        assert_eq!(rig.view().mode, mode, "{label}");
        assert_eq!(rig.view().pending.expect(label).request, req(7));
    }

    for entry in [
        PubEvent::Freeze {
            cause: FreezeCause::AuthorityLost(DenyReason::Expired),
        },
        PubEvent::Freeze {
            cause: FreezeCause::LocalStorageFenced,
        },
        PubEvent::BlockPartition { reason: block() },
    ] {
        let mut direct = Direct::new();
        direct.apply(entry);
        let mode = direct.kernel.view().mode;
        let effects = direct.apply(PubEvent::Candidate(candidate_of(P, 7, 7)));
        assert!(
            matches!(effects[0], PubEffect::ArmTimer { .. }),
            "{effects:?}"
        );
        assert_eq!(
            effects.last(),
            Some(&fact(PubFact::CandidateWhileNotServing {
                mode: mode.clone()
            }))
        );
        assert_eq!(direct.kernel.view().mode, mode);
    }
    for cause in [
        FreezeCause::UnresolvedTransaction,
        FreezeCause::RecoveryReadOnly,
    ] {
        let mut direct = Direct::new();
        direct.apply(PubEvent::Freeze { cause });
        let mode = direct.kernel.view().mode;
        assert_eq!(
            direct.apply(PubEvent::Candidate(candidate_of(P, 7, 7))),
            vec![fact(PubFact::CandidateUnreachable { mode })]
        );
        assert_eq!(direct.kernel.view().pending, None);
    }
}

/// M7A-170 (K-A-47; ADR 0007 "Publishing from the unresolved freeze reopens; from an authority
/// loss it does not"): (a) `Frozen{Unresolved}` publishes and reopens; (b) `Frozen{AuthorityLost}`
/// and (c) `Frozen{LocalStorageFenced}` publish with the whole mode unchanged; (d) read-only
/// defers. The reopen is keyed on the cause: (a) and (b) differ by that one fact.
#[retcd_test]
fn m7a_170_publishing_from_unresolved_freeze_reopens_from_authority_loss_it_does_not() {
    let (rig, entry, effects) = admit_publication_in(enter_unresolved);
    assert_eq!(
        entry,
        PubMode::Frozen {
            cause: FreezeCause::UnresolvedTransaction
        }
    );
    assert!(published_any(&effects), "(a) {effects:?}");
    assert_eq!(rig.view().mode, PubMode::Serving, "(a)");

    let entries: [(&str, Enter); 2] =
        [("(b)", enter_lost_authority), ("(c)", enter_storage_fenced)];
    for (label, enter) in entries {
        let (rig, entry, effects) = admit_publication_in(enter);
        assert!(matches!(entry, PubMode::Frozen { .. }), "{label}");
        assert!(published_any(&effects), "{label} {effects:?}");
        assert_eq!(rig.view().mode, entry, "{label}");
        assert_eq!(rig.view().published.seq, Seq(5), "{label}");
    }

    let (entry, after, effects, published) = admit_publication_read_only();
    assert_eq!(effects, vec![fact(PubFact::PublishDeferred)], "(d)");
    assert_eq!((after, published), (entry, Seq(START)), "(d)");
}

/// Candidate 7 checking and two readers queued behind it; then a block when `blocked`. Returns the
/// check and the block's effects. Shared by M7A-171 and M7A-174.
fn checking_with_waiters(rig: &mut Rig, blocked: bool) -> (CorrelationId, Vec<EffectKind>) {
    let c = rig.pending_with_recheck(7);
    assert_eq!(rig.admitted(read(21)), vec![]);
    assert_eq!(rig.admitted(read(22)), vec![]);
    let effects = if blocked {
        rig.step(block_event(block()))
    } else {
        Vec::new()
    };
    (c, effects)
}

/// M7A-171 (K-A-48; ADR 0007 "Blocked is sticky under a publication deny"): in `Blocked`, a deny at
/// `Publication` quarantines and the mode stays `Blocked`, byte-equal to the entry value.
/// Near-miss twin (one fact, the entry mode): the same deny in `Serving` freezes
/// `AuthorityLost(Expired)`. The readers are answered once each: in `Blocked` the block already
/// drained them (B-R29), so the deny drains none; in `Serving` the deny drains them with §3.4's
/// code (A-R72a Q3; the plan's `UNKNOWN_OUTCOME` predates it).
#[retcd_test]
fn m7a_171_blocked_is_sticky_under_a_publication_deny() {
    for blocked in [true, false] {
        let mut rig = Rig::new();
        let (c, mut all) = checking_with_waiters(&mut rig, blocked);
        let entry = rig.view().mode;
        let t = rig.now + 10;
        let effects = rig.step(answer(
            Checkpoint::Publication,
            c,
            Verdict::Deny(DenyReason::Expired),
        ));
        let mut want = vec![
            status_write(7, 7, StatusOutcome::Unknown, t),
            quarantined(7),
        ];
        if !blocked {
            want.push(rejected_read(21, ErrorKind::LeaseExpired));
            want.push(rejected_read(22, ErrorKind::LeaseExpired));
        }
        assert_eq!(effects, want, "blocked={blocked}");
        all.extend(effects);
        let mode = rig.view().mode;
        if blocked {
            assert_eq!(mode, entry);
            assert_eq!(mode, PubMode::Blocked { reason: block() });
        } else {
            assert_eq!(
                mode,
                PubMode::Frozen {
                    cause: FreezeCause::AuthorityLost(DenyReason::Expired)
                }
            );
        }
        for n in [21, 22] {
            assert_eq!(read_replies_for(&all, n), 1, "blocked={blocked} reader {n}");
        }
    }
}

/// M7A-173 (K-A-51; ADR 0004 "Publication binds the digest"): with `qualifies_now(7)` true
/// throughout, `digest_at` alone decides. `Differs{stored}` and `NotRetained` refuse with the
/// digest conjunct named (`Differs` carrying the stored digest), keep the candidate and publish
/// nothing; `Match` publishes 7.
#[retcd_test]
fn m7a_173_publication_binds_the_digest() {
    let stored = Digest([7; 32]);
    for lookup in [
        DigestLookup::Differs { stored },
        DigestLookup::NotRetained,
        DigestLookup::Match,
    ] {
        let refused = lookup != DigestLookup::Match;
        let mut rig = Rig::new();
        rig.p1
            .scripted_mut(NODE, P)
            .expect("scripted")
            .set_digest(Seq(7), lookup);
        let c = rig.pending_with_recheck(7);
        rig.snapshot_at(7);
        let effects = rig.step(answer(Checkpoint::Publication, c, Verdict::Admit));
        if refused {
            assert_eq!(
                effects,
                vec![ignored(AuthorityIgnoreReason::PublishPredicateFalse)],
                "{lookup:?}"
            );
            assert_eq!(rig.view().published.seq, Seq(START), "{lookup:?}");
            assert_eq!(rig.view().pending.expect("kept").request, req(7));
        } else {
            assert!(published_any(&effects), "{effects:?}");
            assert_eq!(rig.view().published.seq, Seq(7));
        }

        let mut direct = Direct::new();
        direct.repl.set_digest(Seq(7), lookup);
        let c = direct.pending_with_recheck(7);
        let effects = direct.answer(c, Checkpoint::Publication, Verdict::Admit);
        if refused {
            assert_eq!(
                effects,
                vec![fact(PubFact::PublishPredicateFalse {
                    which: PredicateFalse::Digest(lookup)
                })]
            );
            assert!(direct.kernel.view().pending.is_some());
        } else {
            assert!(notified(&effects), "{effects:?}");
        }
    }
}

/// M7A-174 (K-A-57, K-A-48): `Serving` with candidate 7 checking and two readers; a block; then an
/// `Admit` whose lineage is not the candidate's. It quarantines (`Status(Unknown)`,
/// `Quarantined{g, 7}`), the block stands, and `PublishRefusedBlocked` is absent. Near-miss twin
/// (one fact: the lineage stays ours): exactly M7A-159's refusal and no quarantine. The readers
/// are answered once each, at the block (B-R29), so the answer drains none; the plan's per-waiter
/// `UNKNOWN_OUTCOME` at the answer cannot occur once the block has drained the queue.
#[retcd_test]
fn m7a_174_blocked_publish_refusal_does_not_swallow_a_lineage_move() {
    for moved in [true, false] {
        let mut rig = Rig::new();
        let (c, mut all) = checking_with_waiters(&mut rig, true);
        let mut decided = decision(P, Checkpoint::Publication, c, Verdict::Admit, 1);
        if moved {
            decided.lineage.generation = Generation(GEN.0 + 1);
        }
        let t = rig.now + 10;
        let effects = rig.step(answer_event(decided));
        let refused = ignored(AuthorityIgnoreReason::PublishRefusedBlocked);
        if moved {
            assert_eq!(
                effects,
                vec![
                    status_write(7, 7, StatusOutcome::Unknown, t),
                    quarantined(7)
                ]
            );
            assert!(!effects.contains(&refused));
        } else {
            assert_eq!(effects, vec![refused]);
        }
        all.extend(effects);
        assert_eq!(
            rig.view().mode,
            PubMode::Blocked { reason: block() },
            "moved={moved}"
        );
        for n in [21, 22] {
            assert_eq!(read_replies_for(&all, n), 1, "moved={moved} reader {n}");
        }
    }
}

// ---- supporting: the arrival order in `on_acquire` (tester-p1 gate5) ----------------------------

/// Lead ruling A-R72d's order: the mode refuses before the missing view does. With no authority
/// view held and the partition blocked, a read of either kind is refused `ProtectionPaused`, the
/// block's code, not `Unavailable`. Kills tester-p1's gate5 mutant `V-no-view-before-mode`.
#[retcd_test]
fn a_blocked_partition_with_no_view_refuses_by_mode_not_unavailable() {
    let mut rig = Rig::bare();
    assert_eq!(rig.view().authority, None);
    rig.step(block_event(block()));
    assert_eq!(
        rig.step(read(21)),
        vec![rejected_read(21, ErrorKind::ProtectionPaused)]
    );
    assert_eq!(
        rig.step(read_previous(22)),
        vec![previous(22, Err(ErrorKind::ProtectionPaused))]
    );
    assert!(rig.view().gated.is_empty() && rig.view().read_check.is_none());
}

/// The order ruled for tester-p1's advisory `B-reuse-after-cap` (design §4.2): a held identity is
/// refused `RequestIdReuse` even when the queue is at the cap, and a read refused `Overloaded` was
/// answered, not held, so its retry is refused `Overloaded` again. The previous read reaches the
/// kernel's own order (the adapter's key guard covers fresh reads only). Kills gate5 mutant
/// `B-reuse-after-cap`.
#[retcd_test]
fn a_held_identity_is_refused_as_reuse_even_at_the_cap() {
    let mut rig = full_queue();
    assert_eq!(
        rig.step(read_previous(100)),
        vec![previous(100, Err(ErrorKind::RequestIdReuse))]
    );
    assert_eq!(
        rig.step(read(100)),
        vec![rejected_read(100, ErrorKind::RequestIdReuse)]
    );
    for _ in 0..2 {
        assert_eq!(
            rig.step(read_previous(200)),
            vec![previous(200, Err(ErrorKind::Overloaded))]
        );
    }
    assert_eq!(rig.view().waiters.len(), WAITER_CAP);
}

/// M7A-189. Lead ruling A-R84, batch C (dev-edges defect D2). A partition fence for another
/// partition is not P1's to act on, and P1 says so as T1's `on_freeze` does: `Ignored(NotOurs)`,
/// and nothing about this partition changes. Before the fix P1 refused it `Unavailable`, which
/// the sim recorded as an owed decline and, once the edge left `OWED_EDGES`, stopped the run.
/// The same answer on a partition P1 holds no kernel for: the guard creates no slot.
#[retcd_test]
fn m7a_189_p1_answers_another_partitions_fence_not_ours() {
    let mut rig = Rig::new();
    rig.published(5);
    rig.pending_with_recheck(6);
    rig.admitted(read(21));
    let before = rig.view();

    assert_eq!(
        rig.try_step_on(
            P,
            fence(FenceScope::Partition(P2), DenyReason::EpochRevoked)
        )
        .expect("M7A-189: answered, not refused"),
        vec![ignored(AuthorityIgnoreReason::NotOurs)]
    );
    assert_eq!(rig.view(), before, "M7A-189: P's state untouched");

    // Still P's own fence freezes it: the guard is the partition, not the reason.
    let own = rig.step(fence(FenceScope::Partition(P), DenyReason::EpochRevoked));
    assert!(
        !own.contains(&ignored(AuthorityIgnoreReason::NotOurs)),
        "M7A-189: P's own fence is P's: {own:?}"
    );
    assert!(matches!(rig.view().mode, PubMode::Frozen { .. }));

    // tester-edges TP5: the same answer on a partition P1 holds no kernel for, and no slot is
    // made for it. Stepped on P2, so a guard that ran after slot creation would leave one.
    assert!(rig.p1.view(NODE, P2).is_none(), "fixture: no slot for P2");
    assert_eq!(
        rig.try_step_on(
            P2,
            fence(FenceScope::Partition(P), DenyReason::EpochRevoked)
        )
        .expect("M7A-189: answered on P2, not refused"),
        vec![ignored(AuthorityIgnoreReason::NotOurs)]
    );
    assert!(
        rig.p1.view(NODE, P2).is_none(),
        "M7A-189: no slot for P2 made"
    );
}

/// M7A-191. Lead ruling A-R84 (dev-edges defect D3, P1 half). A view whose lineage names
/// another partition is not adopted: `Ignored(NotOurs)`, and the view P1 holds is unchanged.
/// Before the fix P1 adopted it silently (`ok0` in the sim trace), so `p2`'s view, delivered at
/// `p1`, became `p1`'s authority.
#[retcd_test]
fn m7a_191_p1_does_not_adopt_another_partitions_view() {
    let mut rig = Rig::new();
    let held = rig.view().authority;
    assert!(held.is_some(), "fixture: P holds its own view");

    assert_eq!(
        rig.try_step_on(P, view_push(P2, 9))
            .expect("M7A-191: answered, not refused"),
        vec![ignored(AuthorityIgnoreReason::NotOurs)]
    );
    assert_eq!(rig.view().authority, held, "M7A-191: P's view unchanged");

    // tester-edges TP2: a lower partition id is just as foreign. The guard is inequality, not
    // order, so a view naming partition 1 at P (3) is not adopted either.
    assert_eq!(
        rig.try_step_on(P, view_push(PartitionId(1), 10))
            .expect("M7A-191: answered, not refused"),
        vec![ignored(AuthorityIgnoreReason::NotOurs)]
    );
    assert_eq!(rig.view().authority, held, "M7A-191: P's view unchanged");

    // tester-edges TP5: stepped on P2, where P1 holds no kernel, a view for P makes no slot.
    // The earlier steps were all on P, so they could not have made one either way.
    assert!(rig.p1.view(NODE, P2).is_none(), "fixture: no slot for P2");
    assert_eq!(
        rig.try_step_on(P2, view_push(P, 11))
            .expect("M7A-191: answered on P2, not refused"),
        vec![ignored(AuthorityIgnoreReason::NotOurs)]
    );
    assert!(
        rig.p1.view(NODE, P2).is_none(),
        "M7A-191: no slot for P2 made"
    );
}

// ---- M9 S0: the kernel's start record -----------------------------------------------------------

/// The kernel's start record as a candidate at `seq`: request [`RequestIdentity::START_RECORD`].
fn start_candidate(seq: u64) -> EventKind {
    let mut candidate = candidate_of(P, seq, seq);
    candidate.request = RequestIdentity::START_RECORD;
    EventKind::Kernel(KernelEvent::AppliedCandidate(Box::new(candidate)))
}

/// T1 is told the start record published, as it is told any record.
fn notify_start(seq: u64) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Published {
        lineage: lineage(),
        seq: Seq(seq),
        record_digest: d(seq),
        request: RequestIdentity::START_RECORD,
    })
}

/// M9 S0 rule 5, publish. The start record's candidate arms the deadline and writes no status
/// entry; its publish moves the position, opens the old-prefix view, tells T1 and cancels the
/// deadline, and asks no `Reply` check, so nothing awaits a reply. Twin, one fact apart (the
/// identity): the next client record's candidate writes its `Unknown` entry as ever.
#[retcd_test]
fn m9_s0_08_p1_publishes_the_start_record_with_no_status_entry_and_no_reply() {
    let mut rig = Rig::new();
    let t = rig.now + 10;
    assert_eq!(rig.step(start_candidate(5)), vec![arm(1, t + DEADLINE)]);
    rig.qualify(5);
    assert_eq!(
        rig.step(gained(5)),
        vec![check(Checkpoint::Publication, corr(1))]
    );
    rig.snapshot_at(5);
    assert_eq!(
        rig.step(answer(Checkpoint::Publication, corr(1), Verdict::Admit)),
        vec![open(1), notify_start(5), cancel(1)]
    );
    let view = rig.view();
    assert_eq!(view.published.seq, Seq(5));
    assert_eq!(view.pending, None);
    assert_eq!(view.mode, PubMode::Serving);
    assert!(view.awaiting_reply.is_empty(), "nothing awaits a reply");

    let t = rig.now + 10;
    assert_eq!(
        rig.step(candidate(6, 6)),
        vec![
            arm(2, t + DEADLINE),
            status_write(6, 6, StatusOutcome::Unknown, t)
        ]
    );
}

/// M9 S0 rule 5, deadline. The start record's post-apply deadline writes no status entry and
/// sends no reply, and freezes the partition as for any unresolved record. The late publish still
/// publishes and reopens it (K-A-47), again with no reply.
#[retcd_test]
fn m9_s0_09_p1_the_start_records_deadline_writes_no_status_and_no_reply() {
    let mut rig = Rig::new();
    rig.step(start_candidate(5));
    assert_eq!(rig.step(deadline(P, 1)), vec![]);
    let view = rig.view();
    assert_eq!(
        view.mode,
        PubMode::Frozen {
            cause: FreezeCause::UnresolvedTransaction
        }
    );
    assert!(view.pending.expect("kept").replied);

    rig.qualify(5);
    let c = publication_check(&rig.step(gained(5)));
    rig.snapshot_at(5);
    assert_eq!(
        rig.step(answer(Checkpoint::Publication, c, Verdict::Admit)),
        vec![open(1), notify_start(5), cancel(1)]
    );
    assert_eq!(rig.view().mode, PubMode::Serving);
    assert!(rig.view().awaiting_reply.is_empty());
}

/// M9 S0 rule 4. A status query for the reserved identity is refused
/// `INVALID_ARGUMENT{identity}`, with or without a generation. Twin: a client's identity P1 never
/// saw is answered, not refused.
#[retcd_test]
fn m9_s0_10_p1_status_refuses_the_start_record_identity() {
    let mut rig = Rig::new();
    for generation in [Some(GEN), None] {
        assert_eq!(
            rig.step(EventKind::Client(ClientEvent::Status {
                identity: RequestIdentity::START_RECORD,
                generation,
            })),
            vec![EffectKind::Reply(ReplyEffect::Failed {
                identity: RequestIdentity::START_RECORD,
                error: RdbError::InvalidArgument { field: "identity" },
            })],
            "{generation:?}"
        );
    }
    let answered = rig.step(status(9, Some(GEN)));
    assert!(
        matches!(answered.as_slice(), [EffectKind::Reply(ReplyEffect::Status { identity, .. })] if *identity == req(9)),
        "{answered:?}"
    );
}

// ---- M9 S0 D3: F1's re-emit of the lineage already served ---------------------------------------

/// F1's re-emit of the lineage P1 already serves, at selected cutoff `cutoff` (T-B-03): the
/// result `recovered` builds, with its root, generation and view on the rig's own lineage.
fn reemitted(mode: PartitionMode, cutoff: u64) -> EventKind {
    let EventKind::Kernel(KernelEvent::Recovered(mut result)) =
        recovered(mode, cutoff, false, None)
    else {
        unreachable!("`recovered` builds a Recovered")
    };
    result.selected.root = lineage();
    result.new_generation = GEN;
    result.committed.authority_view.lineage = lineage();
    EventKind::Kernel(KernelEvent::Recovered(result))
}

/// M9 S0 D3 (lead ruling "S0 D3" rule 4). F1 re-emits the lineage P1 already serves as `Active`
/// once the rebuild finishes, with the cutoff it selected long before. Published 5 (its reply
/// still owed), candidate 6 pending, and a `Fresh` reader waiting behind it: the re-emit changes
/// none of them, and the view kept at 5 stays (PR #33 F-002). Before the fix the published
/// position fell to the cutoff, the candidate was dropped, the owed reply was withheld, and a
/// read was refused because storage's view sat above what P1 called published (host walk A12).
#[retcd_test]
fn m9_d3_01_p1_a_re_emit_of_the_served_lineage_keeps_published_pending_and_owed_replies() {
    let mut rig = Rig::new();
    let c5 = rig.published(5);
    let c6 = rig.pending_with_recheck(6);
    assert_eq!(rig.admitted(read(21)), vec![], "21 waits behind 6");
    let before = rig.view();
    assert_eq!(
        rig.step(reemitted(PartitionMode::Active, START)),
        vec![],
        "no reply withheld, no reader answered, and the kept view neither released nor reopened"
    );
    let after = rig.view();
    assert_eq!(
        after.published, before.published,
        "published never goes down"
    );
    assert_eq!(after.pending, before.pending, "the candidate is kept");
    assert_eq!(
        after.awaiting_reply, before.awaiting_reply,
        "the owed reply is kept"
    );
    assert_eq!(after.waiters, vec![req(21)], "the reader still waits");
    assert_eq!(after.mode, PubMode::Serving);
    assert_eq!(
        (after.kept, &after.opening),
        (before.kept, &before.opening),
        "the view kept at 5 stays, and no other is asked for"
    );
    assert!(after.kept.is_some());

    assert_eq!(
        rig.step(answer(Checkpoint::Reply, c5, Verdict::Admit)),
        vec![txn_reply(5, 5)],
        "the write published before the re-emit is answered"
    );
    rig.snapshot_at(6);
    let effects = rig.step(answer(Checkpoint::Publication, c6, Verdict::Admit));
    assert!(
        effects.contains(&read_reply(
            21,
            ReadServiceOutcome::WaitedAtBarrier,
            Some(value_at(6))
        )),
        "the kept candidate publishes and the Fresh reader is answered there: {effects:?}"
    );
    assert_eq!(rig.view().published.seq, Seq(6));
}

/// M9 S0 D3, the mode half of rule 4. A re-emit of the served lineage that changes the mode is
/// still a mode transition, so it drains the waiters (K-A-14), here into `ReadOnly`, which
/// answers a read at the published position. The candidate, the owed reply and the kept view
/// are kept (PR #33 F-002).
#[retcd_test]
fn m9_d3_02_p1_a_re_emit_that_changes_the_mode_answers_its_waiters_and_keeps_the_rest() {
    let mut rig = Rig::new();
    rig.published(5);
    rig.pending_with_recheck(6);
    assert_eq!(rig.admitted(read(21)), vec![]);
    let before = rig.view();
    assert_eq!(
        rig.step(reemitted(PartitionMode::ReadOnly, START)),
        vec![read_reply(
            21,
            ReadServiceOutcome::WaitedAtBarrier,
            Some(value_at(5))
        )]
    );
    let after = rig.view();
    assert_eq!(
        after.mode,
        PubMode::Frozen {
            cause: FreezeCause::RecoveryReadOnly
        }
    );
    assert_eq!(
        (
            after.published,
            after.pending,
            after.awaiting_reply,
            after.kept
        ),
        (
            before.published,
            before.pending,
            before.awaiting_reply,
            before.kept
        )
    );
}
