//! F1 lineage and recovery, driven through `Module::step` and F1's pure selection functions.
//!
//! The plan rows (`m7b_NN_*`, team kernel-b test plan §8 and §9) are the last section, written
//! after the manual tester's thumbs-up (B-R45b). Everything above them is scaffolding and the
//! ruling rows of the B-R41/B-R45 gates, the tester's `tester_*` rows included; a scaffold names
//! the plan row it prepared.
//!
//! F1 rows that are not here, each for one reason: M7B-96, 104, 136, 137 and 156 are sim rows
//! (rdb-sim, F:H1/F:M1). M7B-108 (re-worded by B-R49), 97 and 113 come next; the last two read
//! `SelectionSpy`, F1's `LengthSpy`/`SelectSpy` seam. M7B-153 and 154 (ruling B-R52, the bounded
//! rebuild sync) sit before package 5. M7B-155 was promoted in place from the F-a scaffold, so it
//! sits with the pre-commit wait rows; M7B-156 is its sim twin (rdb-sim).
//!
//! Package 5 is the last section: M7B-116 (re-worded by B-R49b: no merge, union or delete, and
//! length read only by `longest`), M7B-112 (`DegradedRf2` needs both copies; R1 blocks on losing
//! either) and M7B-138 (a holder that cannot lead transfers under a credential naming itself).
//! 112 and 138 cross into R1 through `Replication::step` and `AppendReceiver`, because their
//! claims end at R1's receiver, not at F1.
//!
//! A1's `FenceProven` is not routed to F1 by the sim yet (lead ruling on B-R35), so every test
//! builds its `FencingProof` directly and delivers it as `RecoveryEvent::FenceProven`.
//!
//! Histories are modelled as digests: every copy shares branch 0 up to its fork point, and a
//! copy on branch `n` has a different digest at every sequence after the fork. Two copies on
//! different branches past their fork are divergent by construction.

use config_log::retcd_test;

use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::authority::AuthorityTimer;
use rdb_core::contracts::authority::{
    AuthorityView, BlockReason, DenyReason, FencingProof, Lineage, PartitionMode, Revocation,
};
use rdb_core::contracts::control::{
    CasOutcome, ControlEffect, ControlEvent, ControlKey, ReadOutcome,
};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::RdbError;
use rdb_core::contracts::event::{
    Budgets, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, NodeLifecycle,
    StepCtx,
};
use rdb_core::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, ControlRequestId, CorrelationId, DurableSeq,
    EventId, Generation, GrantId, NodeId, OwnerEpoch, PartitionId, ReplicaRole, Revision, Seq,
    SnapshotHandle, TimerId, TimerVersion,
};
use rdb_core::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::recovery::{
    Candidate, CommittedRoot, DivergenceEvidence, DurableProof, InventoryOutcome, LineageAnchor,
    LossRecord, MissingProof, RecoveryBarrier, RecoveryEffect, RecoveryEvent, RecoveryPlan,
    RecoveryResult, RetainedStatusMap, SelectedLineage, SurvivorInventory, UnavailableReason,
};
use rdb_core::contracts::storage::{Namespace, SnapshotRead};
use rdb_core::contracts::time::{ControlTime, Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::{QuarantineReason, Version};
use rdb_core::recovery::lineage::{
    select_leader, select_prefix, select_prefix_spied, verify_ancestry, Rejected, SelectionOutcome,
    SelectionSpy, VerifiedInventory,
};
use rdb_core::recovery::{
    mode_for, new_root, Recovery, RecoveryPhase, DISCOVERY_TIMER, MAX_WINDOW_EXTENSIONS,
};
// M7B-112 and 138 follow F1's output into R1.
use rdb_core::contracts::authority::FenceCredential;
use rdb_core::contracts::digest::Domain;
use rdb_core::contracts::envelope::{
    AppendAck, AppendOutcome, AppendReject, EnvelopeHeader, ReplicaProgress, ReplicationEnvelope,
};
use rdb_core::contracts::ids::{
    AppliedSeq, BatchId, ClientId, LeaseId, MessageId, ReceivedSeq, RequestId, RequestIdentity,
    TenantId,
};
use rdb_core::contracts::storage::{StorageEvent, StoreEffect, Write};
use rdb_core::contracts::transport::{Frame, PeerLabel, SendEffect, TransportEvent};
use rdb_core::contracts::txn::Outcome;
use rdb_core::contracts::version::ENVELOPE_VERSION;
use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
use rdb_core::replication::wire::{decode_reply, encode_recovery_append, encode_reply};
use rdb_core::replication::Replication;

use std::collections::BTreeSet;

use bytes::Bytes;

// ---------------------------------------------------------------------------------------------
// Fixture: primary A, regular B and C (optional shadow D); prior lineage (7, 1); root at seq 10.
// ---------------------------------------------------------------------------------------------

const PARTITION: PartitionId = PartitionId(1);
const A: CopyId = CopyId(1);
const B: CopyId = CopyId(2);
const C: CopyId = CopyId(3);
const D: CopyId = CopyId(4);
const C1: ConfigVersion = ConfigVersion(1);
const PRIOR_GEN: Generation = Generation(7);
const PRIOR_EPOCH: OwnerEpoch = OwnerEpoch(1);
const BASE: u64 = 10;
const CONTROL_REV: Revision = Revision(5);
const RETENTION: u64 = 60_000;
const WINDOW: u64 = Budgets::SPEC_DEFAULTS.discovery_window_millis;
const BUDGETS: Budgets = Budgets::SPEC_DEFAULTS;
const CORRELATION: CorrelationId = CorrelationId(42);

struct NoSnapshot;

impl SnapshotRead for NoSnapshot {
    fn handle(&self) -> SnapshotHandle {
        SnapshotHandle(0)
    }
    fn at(&self) -> Seq {
        Seq::ZERO
    }
    fn generation(&self) -> Generation {
        Generation(0)
    }
    fn get(&self, _ns: Namespace, _key: &[u8]) -> Option<Bytes> {
        None
    }
    fn version(&self, _ns: Namespace, _key: &[u8]) -> Option<Version> {
        None
    }
    fn scan(&self, _ns: Namespace, _from: &[u8], _limit: usize) -> Vec<(Bytes, Bytes)> {
        Vec::new()
    }
}

const SNAPSHOT: NoSnapshot = NoSnapshot;

fn ctx(now: u64) -> StepCtx<'static> {
    StepCtx {
        now: Tick(now),
        control_time: ControlTime {
            estimate: Tick(now),
            error_millis: 0,
            bound_established: true,
            sampled_at: Tick(now),
        },
        node: NodeId(1),
        boot: BootId(1),
        partition: PARTITION,
        generation: PRIOR_GEN,
        owner_epoch: PRIOR_EPOCH,
        config_version: C1,
        snapshot: &SNAPSHOT,
        budgets: &BUDGETS,
    }
}

/// The digest at `seq` on `branch`. Branch 0 is the shared history.
fn dg(branch: u8, seq: u64) -> Digest {
    let mut bytes = [0u8; 32];
    bytes[0] = branch;
    bytes[1..9].copy_from_slice(&seq.to_be_bytes());
    Digest(bytes)
}

/// The digest at `seq` of a history that leaves branch 0 after `fork`.
fn on(branch: u8, fork: u64, seq: u64) -> Digest {
    if seq <= fork {
        dg(0, seq)
    } else {
        dg(branch, seq)
    }
}

fn prior() -> Lineage {
    Lineage {
        partition: PARTITION,
        generation: PRIOR_GEN,
        owner_epoch: PRIOR_EPOCH,
    }
}

fn root() -> Lineage {
    Lineage {
        partition: PARTITION,
        generation: Generation(8),
        owner_epoch: OwnerEpoch(2),
    }
}

fn anchor() -> LineageAnchor {
    LineageAnchor {
        lineage: prior(),
        base_seq: Seq(BASE),
        base_digest: dg(0, BASE),
    }
}

/// A full ladder from the root to `head` on `branch`, leaving branch 0 after `fork`.
fn inv_on(copy: CopyId, head: u64, branch: u8, fork: u64) -> SurvivorInventory {
    SurvivorInventory {
        copy,
        anchor_seen: anchor(),
        head: (Seq(head), on(branch, fork, head)),
        ladder: (BASE..=head)
            .map(|s| (Seq(s), on(branch, fork, s)))
            .collect(),
        quarantined: None,
    }
}

/// A full ladder on the shared history.
fn inv(copy: CopyId, head: u64) -> SurvivorInventory {
    inv_on(copy, head, 0, u64::MAX)
}

/// Only the root and the head: a pairwise check against it needs a probe.
fn sparse(copy: CopyId, head: u64) -> SurvivorInventory {
    SurvivorInventory {
        ladder: vec![(Seq(BASE), dg(0, BASE))],
        ..inv(copy, head)
    }
}

fn member(copy: CopyId, role: ReplicaRole) -> Member {
    Member {
        copy,
        node: NodeId(u32::from(copy.0)),
        boot: BootId(1),
        role,
    }
}

fn candidate(copy: CopyId) -> Candidate {
    Candidate {
        copy,
        primary_eligible: true,
        healthy: true,
        within_capacity: true,
        has_valid_grant: true,
    }
}

/// `{A primary, B, C regular}` plus `extra`, every member a viable candidate.
fn plan(extra: &[Member]) -> RecoveryPlan {
    let mut members = vec![
        member(A, ReplicaRole::Primary),
        member(B, ReplicaRole::RegularSecondary),
        member(C, ReplicaRole::RegularSecondary),
    ];
    members.extend_from_slice(extra);
    let candidates = members.iter().map(|m| candidate(m.copy)).collect();
    RecoveryPlan {
        anchor: anchor(),
        config: PartitionConfig::new(PARTITION, C1, members),
        candidates,
        rebuild_required: [A, B, C].into_iter().collect(),
        authority_view: AuthorityView {
            lineage: prior(),
            grant_id: GrantId(3),
            boot_id: BootId(1),
            authority_generation: AuthorityGeneration(1),
            config_version: C1,
            authority_seq: 1,
            valid_through_tick: Tick(u64::MAX),
            past_horizon: DenyReason::NoGrant,
        },
        retention_millis: RETENTION,
    }
}

fn proof() -> FencingProof {
    FencingProof {
        partition: PARTITION,
        prior_generation: PRIOR_GEN,
        prior_owner_epoch: PRIOR_EPOCH,
        prior_grant_id: GrantId(2),
        prior_boot_id: BootId(1),
        revocation: Revocation::DurableDrain {
            ack_revision: Revision(4),
        },
        control_revision: CONTROL_REV,
        decision_tick: Tick(100),
    }
}

fn proven(copy: CopyId, seq: u64, digest: Digest) -> DurableProof {
    DurableProof {
        copy,
        partition: PARTITION,
        seq: DurableSeq(seq),
        digest,
    }
}

fn durable(copy: CopyId, seq: u64, digest: Digest) -> RecoveryEvent {
    RecoveryEvent::DurableAt(proven(copy, seq, digest))
}

fn caught_up(copy: CopyId, head: u64, digest: Digest) -> RecoveryEvent {
    RecoveryEvent::CopyCaughtUp {
        copy,
        head: Seq(head),
        digest,
    }
}

/// A plan anchored on the new root at seq 20: what a later recovery would be given.
fn replan() -> RecoveryPlan {
    RecoveryPlan {
        anchor: LineageAnchor {
            lineage: root(),
            base_seq: Seq(20),
            base_digest: dg(0, 20),
        },
        ..plan(&[])
    }
}

/// A fence proved against the new root, (8, 2). It read control after the peer's commit, so its
/// revision is newer than anything the first run saw (ruling A-5).
fn refence() -> FencingProof {
    FencingProof {
        prior_generation: Generation(8),
        prior_owner_epoch: OwnerEpoch(2),
        control_revision: Revision(7),
        ..proof()
    }
}

/// A survivor already on the new root (8, 2) at base 20: a peer's recovery committed first.
fn on_new_root(copy: CopyId, head: u64) -> SurvivorInventory {
    SurvivorInventory {
        copy,
        anchor_seen: replan().anchor,
        head: (Seq(head), dg(0, head)),
        ladder: (20..=head).map(|s| (Seq(s), dg(0, s))).collect(),
        quarantined: None,
    }
}

/// The record the recovery CAS writes: A leads the new root, serving.
fn record(owner: CopyId) -> PartitionRecord {
    PartitionRecord {
        partition: PARTITION,
        owner: NodeId(u32::from(owner.0)),
        generation: Generation(8),
        owner_epoch: OwnerEpoch(2),
        config_version: C1,
        lifecycle: PartitionLifecycle::Serving,
    }
}

// ---------------------------------------------------------------------------------------------
// Effect helpers
// ---------------------------------------------------------------------------------------------

fn r(effect: RecoveryEffect) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Recovery(effect))
}

fn ign(reason: ReplicaIgnoreReason) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Ignored {
        reason: KernelIgnoredReason::Replica(reason),
    })
}

fn arm(version: u64, at: u64) -> EffectKind {
    EffectKind::Timer(TimerEffect::Arm {
        id: DISCOVERY_TIMER,
        version: TimerVersion(version),
        at: Tick(at),
    })
}

fn cas(expected: Revision, owner: CopyId) -> EffectKind {
    EffectKind::Control(ControlEffect::Cas {
        request: LATEST,
        key: ControlKey::Partition(PARTITION),
        expected: Some(expected),
        value: Some(record(owner).encode()),
    })
}

fn selected(cutoff: u64, source: CopyId) -> EffectKind {
    r(RecoveryEffect::Selected(SelectedLineage {
        root: root(),
        cutoff_seq: Seq(cutoff),
        cutoff_digest: dg(0, cutoff),
        source,
    }))
}

fn catch_up(from: CopyId, to: CopyId, through: u64) -> EffectKind {
    r(RecoveryEffect::CatchUp {
        from,
        to,
        through: Seq(through),
        credential: proof().credential_for(from),
    })
}

fn sync(copy: CopyId, cutoff: u64) -> EffectKind {
    r(RecoveryEffect::SyncWalThrough {
        copy,
        cutoff: Seq(cutoff),
    })
}

fn lost(copy: CopyId, reason: UnavailableReason) -> EffectKind {
    r(RecoveryEffect::RecordSourceUnavailable { copy, reason })
}

fn block(reason: BlockReason) -> EffectKind {
    r(RecoveryEffect::BlockPromotion { reason })
}

fn fired(version: u64) -> EventKind {
    EventKind::Timer(TimerFired {
        id: DISCOVERY_TIMER,
        version: TimerVersion(version),
        scheduled_at: Tick(0),
    })
}

fn lose(copy: CopyId) -> EventKind {
    EventKind::Kernel(KernelEvent::CopyLost { copy })
}

fn cas_result(outcome: CasOutcome) -> EventKind {
    EventKind::Control(ControlEvent::CasResult {
        request: LATEST,
        key: ControlKey::Partition(PARTITION),
        outcome,
    })
}

fn read_result(outcome: ReadOutcome) -> EventKind {
    EventKind::Control(ControlEvent::Value {
        request: LATEST,
        key: ControlKey::Partition(PARTITION),
        outcome,
    })
}

/// The `Recovered` result in `effects`; exactly one.
fn recovered(effects: &[EffectKind]) -> RecoveryResult {
    let found: Vec<&RecoveryResult> = effects
        .iter()
        .filter_map(|e| match e {
            EffectKind::Kernel(KernelEffect::Recovered(result)) => Some(&**result),
            _ => None,
        })
        .collect();
    assert_eq!(found.len(), 1, "exactly one Recovered in {effects:?}");
    found[0].clone()
}

fn has_selected(effects: &[EffectKind]) -> bool {
    effects.iter().any(|e| {
        matches!(
            e,
            EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::Selected(_)))
        )
    })
}

// ---------------------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------------------

/// One F1 instance plus a log of every effect it emitted.
/// The request id of every F1 request and answer as these rows spell it (see [`F1::try_at`]).
const LATEST: ControlRequestId = ControlRequestId(u64::MAX);

struct F1 {
    module: Recovery,
    log: Vec<EffectKind>,
    /// Every control request id F1 has sent, oldest first, as it minted them.
    requests: Vec<ControlRequestId>,
}

impl F1 {
    fn new() -> Self {
        Self {
            module: Recovery::new(),
            log: Vec::new(),
            requests: Vec::new(),
        }
    }

    /// Step `kind` with the context at `now` and the event arriving at `at`.
    ///
    /// F1 matches an answer by the request id it echoes (lead ledger L-R177hs). These rows were
    /// written when the key alone matched it, so every answer in them is to the request F1 sent
    /// last. This keeps that reading exact: an answer carrying [`LATEST`] is sent echoing the
    /// newest id in [`Self::requests`], and every `Cas` and `Get` F1 returns is recorded there and
    /// shown with its id replaced by [`LATEST`], so a row compares what is written and read. A row
    /// about the id itself sends the raw ids in [`Self::requests`].
    fn try_at(&mut self, now: u64, at: u64, kind: EventKind) -> Result<Vec<EffectKind>, RdbError> {
        let latest = self.requests.last().copied().unwrap_or(LATEST);
        let mut kind = kind;
        if let EventKind::Control(
            ControlEvent::CasResult { request, .. } | ControlEvent::Value { request, .. },
        ) = &mut kind
        {
            if *request == LATEST {
                *request = latest;
            }
        }
        let event = Event {
            id: EventId(now),
            at: Tick(at),
            node: NodeId(1),
            boot: BootId(1),
            partition: PARTITION,
            correlation: CORRELATION,
            kind,
        };
        let effects = self.module.step(&ctx(now), &event)?;
        assert!(!effects.is_empty(), "BA-2: never an empty effect vector");
        let kinds: Vec<EffectKind> = effects
            .into_iter()
            .map(|effect| {
                assert_eq!(effect.correlation, CORRELATION);
                assert_eq!(effect.partition, PARTITION);
                let mut kind = effect.kind;
                if let EffectKind::Control(
                    ControlEffect::Cas { request, .. } | ControlEffect::Get { request, .. },
                ) = &mut kind
                {
                    self.requests.push(*request);
                    *request = LATEST;
                }
                kind
            })
            .collect();
        self.log.extend(kinds.iter().cloned());
        Ok(kinds)
    }

    /// `kind` is not F1's: `step` declines it with `Unavailable` and state does not move.
    fn declines(&mut self, now: u64, kind: EventKind) {
        let phase = self.phase();
        let answer = self.try_at(now, now, kind);
        assert!(
            matches!(answer, Err(RdbError::Unavailable { .. })),
            "expected a decline, got {answer:?}"
        );
        assert_eq!(self.phase(), phase, "a decline moves nothing");
    }

    /// A late answer to one of F1's own requests, arriving once that exchange is over: taken,
    /// named `UnmatchedCompletion`, and the whole module is unchanged (lead ledger L-R177hs, the
    /// tester's F5). It used to be declined as "not an F1 input", but it is one: F1 minted its
    /// id.
    fn ignores_late(&mut self, now: u64, kind: EventKind) {
        let before = self.module.clone();
        assert_eq!(
            self.step(now, kind),
            vec![ign(ReplicaIgnoreReason::UnmatchedCompletion)],
            "a late answer to F1's own request is named"
        );
        assert_eq!(self.module, before, "a late answer moves nothing");
    }

    fn at(&mut self, now: u64, at: u64, kind: EventKind) -> Vec<EffectKind> {
        self.try_at(now, at, kind)
            .expect("an F1 input is never refused")
    }

    fn step(&mut self, now: u64, kind: EventKind) -> Vec<EffectKind> {
        self.at(now, now, kind)
    }

    fn rec(&mut self, now: u64, input: RecoveryEvent) -> Vec<EffectKind> {
        self.step(now, EventKind::Kernel(KernelEvent::Recovery(input)))
    }

    fn report(&mut self, now: u64, inventory: SurvivorInventory) -> Vec<EffectKind> {
        self.rec(now, RecoveryEvent::InventoryReported(Box::new(inventory)))
    }

    fn phase(&self) -> RecoveryPhase {
        self.module.phase()
    }

    /// How many control CAS effects the run has emitted.
    fn cas_count(&self) -> usize {
        self.log
            .iter()
            .filter(|e| matches!(e, EffectKind::Control(ControlEffect::Cas { .. })))
            .count()
    }
}

/// Planned on `plan` and fenced at tick 0.
fn fenced_on(plan: RecoveryPlan) -> F1 {
    let mut f1 = F1::new();
    f1.rec(0, RecoveryEvent::Plan(Box::new(plan)));
    f1.rec(0, RecoveryEvent::FenceProven(Box::new(proof())));
    f1
}

/// Planned (with `extra` members) and fenced at tick 0.
fn fenced(extra: &[Member]) -> F1 {
    fenced_on(plan(extra))
}

/// Fenced, `inventories` reported, `failed` copies failed; returns the window-close effects.
fn closed(
    extra: &[Member],
    inventories: Vec<SurvivorInventory>,
    failed: &[CopyId],
) -> (F1, Vec<EffectKind>) {
    let mut f1 = fenced(extra);
    for inventory in inventories {
        f1.report(10, inventory);
    }
    for &copy in failed {
        f1.rec(10, RecoveryEvent::InventoryFailed { copy });
    }
    let close = f1.step(WINDOW, fired(1));
    (f1, close)
}

/// All three copies at head `head`: selection needs no catch-up and goes straight to the barrier.
fn at_barrier(head: u64) -> F1 {
    let (f1, close) = closed(&[], vec![inv(A, head), inv(B, head), inv(C, head)], &[]);
    assert_eq!(f1.phase(), RecoveryPhase::Barrier, "{close:?}");
    f1
}

/// At the barrier, every copy durable at `head`: the recovery CAS is in flight.
fn proposing(head: u64) -> F1 {
    let mut f1 = at_barrier(head);
    for copy in [A, B, C] {
        f1.rec(3_000, durable(copy, head, dg(0, head)));
    }
    assert_eq!(f1.phase(), RecoveryPhase::Proposing);
    f1
}

/// A alone survived at head 20 and committed at revision 9 in `ReadOnly`.
fn lone_committed() -> F1 {
    lone_committed_with_result().0
}

/// [`lone_committed`], and the `Recovered` result its commit emitted.
fn lone_committed_with_result() -> (F1, RecoveryResult) {
    let (mut f1, _) = closed(&[], vec![inv(A, 20)], &[B, C]);
    f1.rec(3_000, durable(A, 20, dg(0, 20)));
    let result = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
    assert_eq!(result.mode, PartitionMode::ReadOnly);
    (f1, result)
}

// ---------------------------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------------------------

/// Scaffolds M7B-104/§5.6 mode table.
#[retcd_test]
fn mode_table_counts_eligible_regulars() {
    assert_eq!(
        mode_for(0),
        PartitionMode::Blocked {
            reason: BlockReason::NoEligibleRegular
        }
    );
    assert_eq!(mode_for(1), PartitionMode::ReadOnly);
    assert_eq!(mode_for(2), PartitionMode::DegradedRf2);
    assert_eq!(mode_for(3), PartitionMode::Active);
    assert_eq!(mode_for(4), PartitionMode::Active);
}

/// Q4: the new root is the next generation and the next owner epoch.
#[retcd_test]
fn new_root_is_next_generation_and_epoch() {
    assert_eq!(new_root(&proof()), root());
}

// ---------------------------------------------------------------------------------------------
// Idle and the fence (§2.1)
// ---------------------------------------------------------------------------------------------

/// Scaffolds M7B-84: `Idle` leaves only on a `FencingProof`.
#[retcd_test]
fn idle_takes_nothing_but_a_fence() {
    let mut f1 = F1::new();
    f1.rec(0, RecoveryEvent::Plan(Box::new(plan(&[]))));
    let not_fenced = vec![ign(ReplicaIgnoreReason::NotFenced)];
    assert_eq!(f1.report(1, inv(A, 20)), not_fenced);
    assert_eq!(f1.rec(2, durable(A, 20, dg(0, 20))), not_fenced);
    assert_eq!(f1.step(3, fired(1)), not_fenced);
    assert_eq!(f1.step(5, lose(B)), not_fenced);
    assert_eq!(f1.phase(), RecoveryPhase::Idle);
}

#[retcd_test]
fn plan_is_recorded_and_a_fence_without_one_is_invalid_config() {
    let mut f1 = F1::new();
    assert_eq!(
        f1.rec(0, RecoveryEvent::FenceProven(Box::new(proof()))),
        vec![ign(ReplicaIgnoreReason::InvalidConfig)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Idle);
    assert_eq!(
        f1.rec(1, RecoveryEvent::Plan(Box::new(plan(&[])))),
        vec![ign(ReplicaIgnoreReason::Recorded)]
    );
}

/// Scaffolds M7B-85/86: every member is queried, and the window is anchored to the fence's
/// arrival tick — not to `ctx.now`, not to `proof.decision_tick`.
#[retcd_test]
fn fence_queries_every_member_and_anchors_the_window_at_arrival() {
    let mut f1 = F1::new();
    f1.rec(
        0,
        RecoveryEvent::Plan(Box::new(plan(&[member(D, ReplicaRole::Shadow)]))),
    );
    let effects = f1.at(
        900,
        700,
        EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(Box::new(
            proof(),
        )))),
    );
    assert_eq!(
        effects,
        vec![
            r(RecoveryEffect::QueryInventory {
                copies: vec![A, B, C, D]
            }),
            arm(1, 700 + WINDOW),
        ]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
}

/// `step` refuses a kind F1 never takes rather than answering it.
#[retcd_test]
fn step_refuses_a_kind_f1_never_takes() {
    let mut f1 = F1::new();
    let refused = f1.try_at(
        0,
        0,
        EventKind::Node(NodeLifecycle::Resumed {
            suspended_millis: 5,
        }),
    );
    assert!(
        matches!(refused, Err(RdbError::Unavailable { .. })),
        "{refused:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// The window (§5.2, §5.5)
// ---------------------------------------------------------------------------------------------

/// Scaffolds M7B-87: a fire before the deadline, or of an older version, is stale; another
/// module's timer is not F1's at all.
#[retcd_test]
fn only_the_current_due_timer_closes_the_window() {
    let mut f1 = fenced(&[]);
    f1.report(10, inv(A, 20));
    let stale = vec![ign(ReplicaIgnoreReason::StaleTimer)];
    assert_eq!(f1.step(WINDOW - 1, fired(1)), stale);
    assert_eq!(f1.step(WINDOW, fired(0)), stale);
    let other = EventKind::Timer(TimerFired {
        id: TimerId(DISCOVERY_TIMER.0 + 1),
        version: TimerVersion(1),
        scheduled_at: Tick(WINDOW),
    });
    f1.declines(WINDOW, other);
    assert_eq!(f1.phase(), RecoveryPhase::Collecting);
    assert!(has_selected(&f1.step(WINDOW, fired(1))));
}

/// Scaffolds M7B-88/89: a stale-lineage or quarantined survivor is recorded, never selected, and
/// its later reports are refused.
#[retcd_test]
fn stale_lineage_and_quarantined_survivors_are_recorded_unavailable() {
    let mut f1 = fenced(&[]);
    let stale = SurvivorInventory {
        anchor_seen: LineageAnchor {
            lineage: Lineage {
                generation: Generation(6),
                ..prior()
            },
            ..anchor()
        },
        ..inv(B, 40)
    };
    assert_eq!(
        f1.report(10, stale),
        vec![lost(B, UnavailableReason::StaleLineage)]
    );
    let quarantined = SurvivorInventory {
        quarantined: Some(QuarantineReason::CorruptHistory),
        ..inv(C, 40)
    };
    assert_eq!(
        f1.report(11, quarantined),
        vec![lost(C, UnavailableReason::Quarantined)]
    );
    assert_eq!(
        f1.report(12, inv(B, 40)),
        vec![ign(ReplicaIgnoreReason::NotASource)]
    );
    assert_eq!(
        f1.report(13, inv(CopyId(9), 40)),
        vec![ign(ReplicaIgnoreReason::NotASource)]
    );
    assert_eq!(
        f1.report(14, inv(A, 20)),
        vec![ign(ReplicaIgnoreReason::Recorded)]
    );
    let close = f1.step(WINDOW, fired(1));
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            selected(20, A),
            sync(A, 20),
            arm(2, 2 * WINDOW),
        ]
    );
}

/// Scaffolds M7B-90: a survivor whose history does not contain the committed root diverged.
#[retcd_test]
fn a_root_mismatch_quarantines_at_once_and_quarantine_is_terminal() {
    let mut f1 = fenced(&[]);
    let mut wrong = inv(B, 20);
    wrong.ladder[0].1 = dg(9, BASE);
    assert_eq!(
        f1.report(10, wrong),
        vec![
            r(RecoveryEffect::Quarantine(
                DivergenceEvidence::RootMismatch {
                    copy: B,
                    base_seq: Seq(BASE),
                    expected: dg(0, BASE),
                    found: Some(dg(9, BASE)),
                }
            )),
            block(BlockReason::DivergenceRequiresOperator { diverged: vec![B] }),
        ]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
    let terminal = vec![ign(ReplicaIgnoreReason::QuarantinedTerminal)];
    assert_eq!(f1.step(WINDOW, fired(1)), terminal);
    assert_eq!(
        f1.rec(WINDOW, RecoveryEvent::FenceProven(Box::new(proof()))),
        terminal
    );
}

/// Scaffolds M7B-91/92: the longest compatible prefix wins, and every shorter regular is sent it.
#[retcd_test]
fn window_close_selects_the_longest_compatible_prefix() {
    let (f1, close) = closed(&[], vec![inv(A, 20), inv(B, 12), inv(C, 17)], &[]);
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            selected(20, A),
            catch_up(A, B, 20),
            catch_up(A, C, 20),
            arm(2, 2 * WINDOW),
        ]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Synchronizing);
}

/// The longest history is not the answer when it is not compatible: divergence is never
/// broken by length. Enumerates every fork point and head pair in a small fixed range.
#[retcd_test]
fn a_divergent_pair_quarantines_for_every_fork_and_length() {
    for fork in BASE..=13 {
        for head_a in fork + 1..=16 {
            for head_b in fork + 1..=16 {
                let (f1, close) = closed(
                    &[],
                    vec![inv_on(A, head_a, 1, fork), inv_on(B, head_b, 2, fork)],
                    &[C],
                );
                assert!(
                    !has_selected(&close),
                    "fork {fork} {head_a}/{head_b}: {close:?}"
                );
                assert!(
                    close.iter().any(|e| matches!(
                        e,
                        EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::Quarantine(
                            DivergenceEvidence::Pairwise { .. }
                        )))
                    )),
                    "fork {fork} {head_a}/{head_b}: {close:?}"
                );
                assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
            }
        }
    }
}

/// The pairwise evidence names the shorter head, and both digests there. The block names the
/// copies sorted by id (ruling A-2, B-R45), whichever side of the evidence they are on.
#[retcd_test]
fn divergence_evidence_names_the_shorter_head() {
    let (_, close) = closed(&[], vec![inv_on(A, 20, 1, 12), inv_on(B, 15, 2, 12)], &[C]);
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            r(RecoveryEffect::Quarantine(DivergenceEvidence::Pairwise {
                seq: Seq(15),
                a: (B, dg(2, 15)),
                b: (A, dg(1, 15)),
            })),
            block(BlockReason::DivergenceRequiresOperator {
                diverged: vec![A, B]
            }),
        ]
    );
}

// ---------------------------------------------------------------------------------------------
// Probes (§5.4)
// ---------------------------------------------------------------------------------------------

/// Scaffolds M7B-93: a missing ladder rung is probed once per `(copy, seq)`, in order, and the
/// wait is bounded by a re-armed timer.
#[retcd_test]
fn missing_rungs_are_probed_once_each() {
    let (f1, close) = closed(&[], vec![sparse(A, 20), inv(B, 15), inv(C, 17)], &[]);
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            r(RecoveryEffect::ProbeDigestAt {
                copy: A,
                seq: Seq(15)
            }),
            r(RecoveryEffect::ProbeDigestAt {
                copy: A,
                seq: Seq(17)
            }),
            arm(2, 2 * WINDOW),
        ]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Collecting);

    let (_, close) = closed(&[], vec![sparse(A, 20), inv(B, 15), inv(C, 15)], &[]);
    let probes = close
        .iter()
        .filter(|e| {
            matches!(
                e,
                EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::ProbeDigestAt { .. }))
            )
        })
        .count();
    assert_eq!(probes, 1, "deduplicated: {close:?}");
}

/// A matching answer completes selection; the probed copy stays the source.
#[retcd_test]
fn a_matching_probe_answer_completes_selection() {
    let (mut f1, _) = closed(&[], vec![sparse(A, 20), inv(B, 15), inv(C, 15)], &[]);
    assert_eq!(
        f1.rec(
            2_100,
            RecoveryEvent::ProbeAnswered {
                copy: A,
                seq: Seq(15),
                digest: dg(0, 15)
            }
        ),
        vec![
            selected(20, A),
            catch_up(A, B, 20),
            catch_up(A, C, 20),
            arm(3, 2_100 + WINDOW),
        ]
    );
}

/// A mismatching answer is divergence, never a tie-break.
#[retcd_test]
fn a_mismatching_probe_answer_quarantines() {
    let (mut f1, _) = closed(&[], vec![sparse(A, 20), inv(B, 15), inv(C, 15)], &[]);
    let effects = f1.rec(
        2_100,
        RecoveryEvent::ProbeAnswered {
            copy: A,
            seq: Seq(15),
            digest: dg(5, 15),
        },
    );
    assert!(!has_selected(&effects), "{effects:?}");
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
}

/// An unanswerable probe, or the probe deadline, drops that source before selecting.
#[retcd_test]
fn an_unanswered_probe_drops_the_source() {
    let (mut f1, _) = closed(&[], vec![sparse(A, 20), inv(B, 15), inv(C, 15)], &[]);
    assert_eq!(
        f1.rec(
            2_100,
            RecoveryEvent::ProbeUnavailable {
                copy: A,
                seq: Seq(15)
            }
        ),
        vec![
            lost(A, UnavailableReason::Stalled),
            selected(15, B),
            sync(B, 15),
            sync(C, 15),
            arm(3, 2_100 + WINDOW),
        ]
    );

    let (mut f1, _) = closed(&[], vec![sparse(A, 20), inv(B, 15), inv(C, 15)], &[]);
    assert_eq!(
        f1.step(2 * WINDOW - 1, fired(2)),
        vec![ign(ReplicaIgnoreReason::StaleTimer)]
    );
    assert_eq!(
        f1.step(2 * WINDOW, fired(2))[..2],
        [lost(A, UnavailableReason::Stalled), selected(15, B)]
    );
}

/// Divergence already visible between two full ladders is found before any probe is sent.
#[retcd_test]
fn divergence_is_found_before_probes_are_sent() {
    let (_, close) = closed(
        &[],
        vec![sparse(A, 20), inv(B, 15), inv_on(C, 17, 3, 12)],
        &[],
    );
    assert!(
        !close.iter().any(|e| matches!(
            e,
            EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::ProbeDigestAt { .. }))
        )),
        "{close:?}"
    );
    assert_eq!(close[0], r(RecoveryEffect::CloseWindow));
    assert!(matches!(
        close[1],
        EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::Quarantine(
            DivergenceEvidence::Pairwise { .. }
        )))
    ));
}

// ---------------------------------------------------------------------------------------------
// Extensions and stalled sources (§5.5)
// ---------------------------------------------------------------------------------------------

/// Scaffolds M7B-94/95: a stalled source is recorded, and always before `CloseWindow`.
#[retcd_test]
fn a_stalled_source_is_recorded_before_the_window_closes() {
    let mut f1 = fenced(&[]);
    f1.report(10, inv(A, 20));
    let transfer = |received| RecoveryEvent::TransferProgress {
        copy: C,
        advertised_seq: Seq(30),
        received_seq: Seq(received),
    };
    assert_eq!(
        f1.rec(100, transfer(5)),
        vec![ign(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(f1.step(WINDOW, fired(1)), vec![arm(2, 2 * WINDOW)]);
    assert_eq!(
        f1.step(2 * WINDOW, fired(2)),
        vec![
            lost(C, UnavailableReason::Stalled),
            lost(B, UnavailableReason::Stalled),
            r(RecoveryEffect::CloseWindow),
            selected(20, A),
            sync(A, 20),
            arm(3, 3 * WINDOW),
        ]
    );
}

/// A source advertising no more than the best verified head never extends the window.
#[retcd_test]
fn a_source_behind_the_best_head_does_not_extend() {
    let mut f1 = fenced(&[]);
    f1.report(10, inv(A, 20));
    f1.report(10, inv(B, 20));
    f1.rec(
        100,
        RecoveryEvent::TransferProgress {
            copy: C,
            advertised_seq: Seq(20),
            received_seq: Seq(15),
        },
    );
    let close = f1.step(WINDOW, fired(1));
    assert_eq!(
        close[..2],
        [
            lost(C, UnavailableReason::Stalled),
            r(RecoveryEffect::CloseWindow)
        ]
    );
}

/// Scaffolds M7B-96: at most three extensions; discovery closes by 8 s however fast a source
/// is still delivering, and the unfinished source is recorded.
#[retcd_test]
fn three_extensions_then_the_window_closes() {
    let mut f1 = fenced(&[]);
    f1.report(10, inv(A, 20));
    for (i, deadline) in [WINDOW, 2 * WINDOW, 3 * WINDOW].into_iter().enumerate() {
        let received = 5 * (i as u64 + 1);
        f1.rec(
            deadline - 100,
            RecoveryEvent::TransferProgress {
                copy: C,
                advertised_seq: Seq(30),
                received_seq: Seq(received),
            },
        );
        assert_eq!(
            f1.step(deadline, fired(i as u64 + 1)),
            vec![arm(i as u64 + 2, deadline + WINDOW)]
        );
    }
    f1.rec(
        4 * WINDOW - 100,
        RecoveryEvent::TransferProgress {
            copy: C,
            advertised_seq: Seq(30),
            received_seq: Seq(20),
        },
    );
    assert_eq!(4 * WINDOW, 8_000);
    assert_eq!(
        f1.step(4 * WINDOW, fired(4)),
        vec![
            lost(B, UnavailableReason::Stalled),
            lost(C, UnavailableReason::Stalled),
            r(RecoveryEffect::CloseWindow),
            selected(20, A),
            sync(A, 20),
            arm(5, 5 * WINDOW),
        ]
    );
}

/// Before commit a returning owner is one more survivor (M7B-114's pre-commit half).
#[retcd_test]
fn a_returning_owner_before_commit_is_an_inventory() {
    let mut f1 = fenced(&[]);
    assert_eq!(
        f1.rec(10, RecoveryEvent::StaleOwnerReturned(Box::new(inv(A, 20)))),
        vec![ign(ReplicaIgnoreReason::Recorded)]
    );
}

// ---------------------------------------------------------------------------------------------
// Leader and catch-up (§5.4, §5.6)
// ---------------------------------------------------------------------------------------------

/// Scaffolds M7B-97/98: a shadow may hold the longest prefix, but never leads and never counts;
/// the leader that lags is caught up before its grant.
#[retcd_test]
fn a_shadow_never_leads_and_a_lagging_leader_catches_up_before_grant() {
    let shadow = [member(D, ReplicaRole::Shadow)];
    let (_, close) = closed(
        &shadow,
        vec![inv(A, 18), inv(B, 18), inv(C, 18), inv(D, 25)],
        &[],
    );
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            selected(25, D),
            r(RecoveryEffect::CatchUpBeforeGrant {
                from: D,
                to: A,
                through: Seq(25),
                credential: proof().credential_for(D),
            }),
            catch_up(D, B, 25),
            catch_up(D, C, 25),
            arm(2, 2 * WINDOW),
        ]
    );
}

/// Q5: with no viable regular the recovery blocks rather than promoting anyone.
#[retcd_test]
fn no_viable_leader_blocks() {
    let mut f1 = F1::new();
    let mut unhealthy = plan(&[]);
    for candidate in &mut unhealthy.candidates {
        candidate.healthy = false;
    }
    f1.rec(0, RecoveryEvent::Plan(Box::new(unhealthy)));
    f1.rec(0, RecoveryEvent::FenceProven(Box::new(proof())));
    f1.report(10, inv(A, 20));
    let close = f1.step(WINDOW, fired(1));
    assert_eq!(close.last(), Some(&block(BlockReason::NoEligibleRegular)));
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Blocked(BlockReason::NoEligibleRegular)
    );
}

/// No survivor at all blocks, and a fresh fence restarts discovery.
#[retcd_test]
fn nothing_verified_blocks_until_a_fresh_fence() {
    let (mut f1, close) = closed(&[], Vec::new(), &[A, B, C]);
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            block(BlockReason::NoEligibleRegular)
        ]
    );
    assert_eq!(
        f1.report(WINDOW + 1, inv(A, 20)),
        vec![ign(ReplicaIgnoreReason::RecoveryBlocked)]
    );
    assert_eq!(
        f1.rec(9_000, RecoveryEvent::FenceProven(Box::new(proof()))),
        vec![
            r(RecoveryEffect::QueryInventory {
                copies: vec![A, B, C]
            }),
            arm(2, 9_000 + WINDOW),
        ]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
}

// ---------------------------------------------------------------------------------------------
// Synchronise and the barrier (§5.6)
// ---------------------------------------------------------------------------------------------

/// Scaffolds M7B-99/100: catch-up completes, then every required copy syncs its WAL through the
/// cutoff; a copy that was not behind is not required to report.
#[retcd_test]
fn synchronise_then_sync_every_required_copy() {
    let (mut f1, _) = closed(&[], vec![inv(A, 20), inv(B, 12), inv(C, 17)], &[]);
    let caught = |copy| RecoveryEvent::CopyCaughtUp {
        copy,
        head: Seq(20),
        digest: dg(0, 20),
    };
    assert_eq!(
        f1.rec(2_100, caught(A)),
        vec![ign(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(
        f1.rec(2_200, caught(B)),
        vec![ign(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(
        f1.rec(2_300, caught(C)),
        vec![sync(A, 20), sync(B, 20), sync(C, 20)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Barrier);
}

/// Scaffolds M7B-101/102: the barrier is built by `RecoveryBarrier::try_new` only — a short proof
/// or a proof bound to another digest does not count, and one CAS is proposed once it holds.
#[retcd_test]
fn the_barrier_needs_every_required_copy_durable_at_the_cutoff() {
    let mut f1 = at_barrier(20);
    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];
    assert_eq!(f1.rec(3_000, durable(A, 20, dg(0, 20))), not_durable);
    assert_eq!(
        f1.rec(3_001, durable(B, 19, dg(0, 19))),
        not_durable,
        "short"
    );
    // Past the cutoff but not bound to its digest. A foreign digest at the cutoff itself is
    // divergence, not a missing proof (ruling A-4).
    assert_eq!(
        f1.rec(3_002, durable(C, 21, dg(0, 21))),
        not_durable,
        "mis-bound"
    );
    assert_eq!(
        f1.rec(3_003, durable(CopyId(9), 20, dg(0, 20))),
        vec![ign(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(f1.rec(3_004, durable(B, 20, dg(0, 20))), not_durable);
    assert_eq!(
        f1.rec(3_005, durable(C, 20, dg(0, 20))),
        vec![cas(CONTROL_REV, A), arm(3, 3_005 + WINDOW)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Proposing);
}

/// Lead addition to Q1: the commit CAS writes one record on `partitions/{id}`, `Serving`.
#[retcd_test]
fn the_commit_cas_writes_a_serving_record_on_the_partition_key() {
    let f1 = proposing(20);
    assert_eq!(f1.cas_count(), 1);
    let Some(EffectKind::Control(ControlEffect::Cas {
        request: LATEST,
        key,
        value: Some(body),
        ..
    })) = f1
        .log
        .iter()
        .rev()
        .find(|e| matches!(e, EffectKind::Control(_)))
    else {
        panic!("the last control effect is the CAS: {:?}", f1.log);
    };
    assert_eq!(*key, ControlKey::Partition(PARTITION));
    let written = PartitionRecord::decode(body).expect("A1's codec reads it");
    assert_eq!(written.lifecycle, PartitionLifecycle::Serving);
    assert_eq!(written, record(A));
}

/// Scaffolds M7B-103/104: `Committed` emits one `Recovered` carrying everything decided.
#[retcd_test]
fn a_committed_cas_emits_the_recovery_result() {
    let mut f1 = proposing(20);
    let result = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
    assert_eq!(result.fenced_prior, proof());
    assert_eq!(
        result.inventories,
        vec![
            InventoryOutcome::Verified { copy: A },
            InventoryOutcome::Verified { copy: B },
            InventoryOutcome::Verified { copy: C },
        ]
    );
    assert_eq!(result.selected.root, root());
    assert_eq!(result.selected.cutoff_seq, Seq(20));
    assert_eq!(result.new_generation, Generation(8));
    assert_eq!(result.mode, PartitionMode::Active);
    assert_eq!(result.loss.queried, vec![A, B, C]);
    assert!(result.loss.unavailable.is_empty());
    assert!(!result.loss.uncertain);
    assert_eq!(result.committed.revision, Revision(9));
    assert_eq!(result.committed.pinned_config, plan(&[]).config);
    assert_eq!(
        result.committed.authority_view.lineage,
        root(),
        "Q3: lineage overwritten"
    );
    assert_eq!(result.committed.authority_view.grant_id, GrantId(3));
    assert_eq!(result.retained_status_map.predecessor_generation, PRIOR_GEN);
    assert_eq!(result.retained_status_map.discarded_from, None);
    assert_eq!(f1.phase(), RecoveryPhase::Committed);
    assert_eq!(f1.cas_count(), 1);
}

/// An advertised suffix nobody delivered makes the loss uncertain, and the status map says where
/// the discarded range starts.
#[retcd_test]
fn an_undelivered_suffix_is_uncertain_loss() {
    let mut f1 = fenced(&[]);
    f1.report(10, inv(A, 20));
    f1.report(10, inv(B, 20));
    f1.rec(
        20,
        RecoveryEvent::TransferProgress {
            copy: C,
            advertised_seq: Seq(30),
            received_seq: Seq(0),
        },
    );
    f1.step(WINDOW, fired(1));
    f1.rec(3_000, durable(A, 20, dg(0, 20)));
    f1.rec(3_000, durable(B, 20, dg(0, 20)));
    let result = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
    assert_eq!(result.mode, PartitionMode::DegradedRf2);
    assert_eq!(result.loss.highest_advertised_seq, Seq(30));
    assert!(result.loss.uncertain);
    assert_eq!(
        result.loss.unavailable,
        vec![(C, UnavailableReason::Stalled)]
    );
    assert_eq!(result.retained_status_map.discarded_from, Some(Seq(21)));
    assert!(result.retained_status_map.uncertain);
}

// ---------------------------------------------------------------------------------------------
// The four CAS arms (§5.6)
// ---------------------------------------------------------------------------------------------

/// Scaffolds M7B-105/106: `Unavailable` and `Unknown` block with distinct reasons and are never
/// re-proposed blind.
#[retcd_test]
fn unavailable_and_unknown_block_distinctly_and_never_retry() {
    for (outcome, reason) in [
        (CasOutcome::Unavailable, BlockReason::ControlUnavailable),
        (CasOutcome::Unknown, BlockReason::ControlUnknown),
    ] {
        let mut f1 = proposing(20);
        assert_eq!(
            f1.step(3_100, cas_result(outcome)),
            vec![block(reason.clone())]
        );
        assert_eq!(f1.phase(), RecoveryPhase::Blocked(reason));
        f1.ignores_late(3_200, cas_result(CasOutcome::Committed(Revision(9))));
        assert_eq!(f1.cas_count(), 1, "never re-proposed");
    }
}

/// Proposing with the one CAS answered `Conflict`: the re-read is in flight.
fn rereading() -> F1 {
    let mut f1 = proposing(20);
    assert_eq!(
        f1.step(
            3_100,
            cas_result(CasOutcome::Conflict {
                exists: true,
                current: Revision(6),
            })
        ),
        vec![EffectKind::Control(ControlEffect::Get {
            request: LATEST,
            key: ControlKey::Partition(PARTITION)
        })]
    );
    f1
}

fn found(value: Bytes) -> EventKind {
    read_result(ReadOutcome::Found {
        revision: Revision(6),
        value,
    })
}

/// Rulings F-f (B-R41) and F-g (B-R45), re-derived from the former
/// `a_conflict_over_anything_but_a_newer_owner_is_contention`: a conflict re-reads, and a record
/// at an older or equal epoch with other content, a withdrawn record, or unreadable bytes is
/// contention. The prior owner still there, our owner at the prior epoch: none is re-proposed.
#[retcd_test]
fn a_conflict_over_an_older_or_equal_epoch_is_contention() {
    let prior_record = PartitionRecord {
        owner: NodeId(9),
        generation: PRIOR_GEN,
        owner_epoch: PRIOR_EPOCH,
        ..record(A)
    };
    let ours_at_the_prior_epoch = PartitionRecord {
        owner_epoch: PRIOR_EPOCH,
        ..record(A)
    };
    for read in [
        found(prior_record.encode()),
        found(ours_at_the_prior_epoch.encode()),
        found(Bytes::from_static(b"not a record")),
        read_result(ReadOutcome::Absent { as_of: Revision(6) }),
    ] {
        let mut f1 = rereading();
        assert_eq!(
            f1.step(3_200, read.clone()),
            vec![block(BlockReason::CasContention)],
            "{read:?}"
        );
        assert_eq!(f1.cas_count(), 1, "never re-proposed: {read:?}");
    }
}

/// Q2 and rulings F-f, F-g: a record at an epoch newer than the prior is a peer that recovered
/// first, whoever it names as owner, our own leader included.
#[retcd_test]
fn a_conflict_over_a_newer_epoch_is_overtaken() {
    let newer = PartitionRecord {
        owner: NodeId(2),
        generation: Generation(9),
        ..record(A)
    };
    let ours_later = PartitionRecord {
        owner_epoch: OwnerEpoch(3),
        ..record(A)
    };
    for current in [newer, ours_later] {
        let mut f1 = rereading();
        assert_eq!(
            f1.step(3_200, found(current.encode())),
            vec![block(BlockReason::OvertakenByPeer)],
            "{current:?}"
        );
    }
    let mut f1 = rereading();
    assert_eq!(
        f1.step(3_200, read_result(ReadOutcome::Unavailable)),
        vec![block(BlockReason::ControlUnavailable)]
    );
}

/// Ruling F-g (B-R45), re-derived from the former `a_conflict_over_our_own_record_is_the_cas_landing`:
/// `Conflict` is definitive, so our own bytes on the re-read are a peer's identical decision,
/// never our CAS landing. No `Recovered`, no second CAS; the peer's result is the only one.
#[retcd_test]
fn identical_bytes_on_the_re_read_are_a_peers_decision() {
    let mut f1 = rereading();
    assert_eq!(
        f1.step(3_200, found(record(A).encode())),
        vec![block(BlockReason::OvertakenByPeer)]
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Blocked(BlockReason::OvertakenByPeer)
    );
    assert_eq!(f1.cas_count(), 1);
}

/// The sim offers every control answer to every module. F1 answers only its own pending CAS or
/// re-read on `partitions/{id}`, and declines everything else like any module without that input.
/// The one exception is a late answer echoing an id F1 minted: that is F1's, and is
/// `UnmatchedCompletion` (see [`F1::ignores_late`]).
#[retcd_test]
fn control_answers_f1_did_not_request_are_declined() {
    let committed = cas_result(CasOutcome::Committed(Revision(9)));
    let other_key = |key| {
        EventKind::Control(ControlEvent::CasResult {
            request: LATEST,
            key,
            outcome: CasOutcome::Committed(Revision(9)),
        })
    };
    // Nothing requested: Idle, and every phase before the barrier holds.
    let mut idle = F1::new();
    idle.declines(1, committed.clone());
    let mut collecting = fenced(&[]);
    collecting.declines(10, committed.clone());
    // A CAS is pending: a read nobody asked for, another partition, another key.
    let mut f1 = proposing(20);
    f1.declines(3_100, read_result(ReadOutcome::Unavailable));
    f1.declines(3_100, other_key(ControlKey::Partition(PartitionId(2))));
    f1.declines(3_100, other_key(ControlKey::ClusterSchema));
    // A re-read is pending: a CAS answer is no longer awaited.
    f1.step(
        3_200,
        cas_result(CasOutcome::Conflict {
            exists: true,
            current: Revision(6),
        }),
    );
    f1.declines(3_300, committed.clone());
    assert_eq!(f1.cas_count(), 1);
    // Committed and fully protected: nothing is pending, and a late answer echoing F1's own
    // CAS id is F1's, so it is named rather than declined.
    let mut done = proposing(20);
    done.step(3_100, committed.clone());
    done.ignores_late(3_200, committed);
}

/// Another module's timer is declined in every phase, never answered as stale.
#[retcd_test]
fn another_modules_timer_is_declined() {
    let foreign = EventKind::Timer(TimerFired {
        id: AuthorityTimer::Acquire.id(),
        version: TimerVersion(1),
        scheduled_at: Tick(0),
    });
    F1::new().declines(0, foreign.clone());
    fenced(&[]).declines(WINDOW, foreign.clone());
    proposing(20).declines(3_100, foreign);
}

// ---------------------------------------------------------------------------------------------
// Degraded modes (§5.6 mode table)
// ---------------------------------------------------------------------------------------------

/// Scaffolds M7B-107: a lone survivor commits `ReadOnly` and starts rebuilding.
#[retcd_test]
fn a_lone_survivor_commits_read_only() {
    let f1 = lone_committed();
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    assert_eq!(
        f1.module.rebuild_required(),
        Some(&[A, B, C].into_iter().collect())
    );
}

/// Scaffolds M7B-108: two survivors must both be durable — no one-copy fallback.
#[retcd_test]
fn two_survivors_both_must_be_durable() {
    let (mut f1, _) = closed(&[], vec![inv(A, 20), inv(B, 20)], &[C]);
    assert_eq!(
        f1.rec(3_000, durable(A, 20, dg(0, 20))),
        vec![ign(ReplicaIgnoreReason::BarrierNotDurable)]
    );
    assert_eq!(
        f1.rec(3_001, durable(B, 20, dg(0, 20))),
        vec![cas(CONTROL_REV, A), arm(3, 3_001 + WINDOW)]
    );
    let result = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
    assert_eq!(result.mode, PartitionMode::DegradedRf2);
}

// ---------------------------------------------------------------------------------------------
// After commit: returning owner, rebuild, activation (§5.6a, §5.7)
// ---------------------------------------------------------------------------------------------

/// Scaffolds M7B-114/115: after commit a returning owner is quarantined and rebuilt from the
/// root, however long its history; retention runs from the event's arrival.
#[retcd_test]
fn a_returning_owner_after_commit_never_overrides() {
    let mut f1 = proposing(20);
    f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9))));
    let effects = f1.at(
        50_500,
        50_000,
        EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::StaleOwnerReturned(
            Box::new(inv(A, 99)),
        ))),
    );
    assert_eq!(
        effects,
        vec![
            r(RecoveryEffect::QuarantineSuffix {
                copy: A,
                from: Seq(21),
                until: Tick(50_000 + RETENTION),
            }),
            r(RecoveryEffect::RebuildFromAuthoritative {
                copy: A,
                root: LineageAnchor {
                    lineage: root(),
                    base_seq: Seq(20),
                    base_digest: dg(0, 20),
                },
            }),
        ]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Committed);
    assert_eq!(f1.cas_count(), 1);
}

/// Scaffolds M7B-109..112: rebuild proves a three-copy barrier through `try_new`, then one
/// activation CAS conditioned on the commit revision re-emits `Recovered{mode: Active}`.
#[retcd_test]
fn rebuild_activates_through_a_three_copy_barrier() {
    let mut f1 = lone_committed();
    let caught = |copy, head| RecoveryEvent::CopyCaughtUp {
        copy,
        head: Seq(head),
        digest: dg(0, head),
    };
    assert_eq!(
        f1.rec(4_000, caught(B, 20)),
        vec![
            sync(A, 20),
            sync(B, 20),
            sync(C, 20),
            arm(4, 4_000 + WINDOW)
        ]
    );
    assert_eq!(
        f1.rec(4_001, caught(C, 22)),
        vec![sync(C, 20), arm(5, 4_001 + WINDOW)],
        "the point is pinned"
    );
    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];
    assert_eq!(f1.rec(4_100, durable(A, 20, dg(0, 20))), not_durable);
    assert_eq!(f1.rec(4_101, durable(B, 20, dg(0, 20))), not_durable);
    assert_eq!(
        f1.rec(4_102, durable(C, 21, dg(0, 21))),
        not_durable,
        "past the point"
    );
    assert_eq!(
        f1.rec(4_103, durable(C, 20, dg(0, 20))),
        vec![cas(Revision(9), A), arm(6, 4_103 + WINDOW)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    let result = recovered(&f1.step(4_200, cas_result(CasOutcome::Committed(Revision(11)))));
    assert_eq!(result.mode, PartitionMode::Active);
    assert_eq!(result.committed.revision, Revision(11));
    assert_eq!(
        result.selected.cutoff_seq,
        Seq(20),
        "lineage and cutoff do not move"
    );
    assert_eq!(f1.phase(), RecoveryPhase::Committed);
    assert_eq!(f1.module.rebuild_required(), None);
}

/// Scaffolds M7B-113: a lost copy stalls the rebuild loudly and never shrinks `required`.
#[retcd_test]
fn a_lost_copy_stalls_the_rebuild_and_never_shrinks_it() {
    let mut f1 = lone_committed();
    f1.rec(
        4_000,
        RecoveryEvent::CopyCaughtUp {
            copy: B,
            head: Seq(20),
            digest: dg(0, 20),
        },
    );
    f1.rec(4_100, durable(B, 20, dg(0, 20)));
    assert_eq!(
        f1.step(4_200, lose(B)),
        vec![r(RecoveryEffect::RebuildStalled { copy: B })]
    );
    assert_eq!(
        f1.step(4_201, lose(B)),
        vec![ign(ReplicaIgnoreReason::NotASource)],
        "advisory 21: one alert per loss"
    );
    assert_eq!(
        f1.step(4_202, lose(D)),
        vec![ign(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(
        f1.module.rebuild_required(),
        Some(&[A, B, C].into_iter().collect())
    );
    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];
    assert_eq!(f1.rec(4_300, durable(A, 20, dg(0, 20))), not_durable);
    assert_eq!(f1.rec(4_301, durable(C, 20, dg(0, 20))), not_durable);
    assert_eq!(
        f1.rec(4_302, durable(B, 20, dg(0, 20))),
        not_durable,
        "a lost copy's proof is refused"
    );
    assert_eq!(f1.cas_count(), 1);
}

/// The activation CAS answers the same four arms: `Unknown` blocks.
#[retcd_test]
fn an_unknown_activation_blocks() {
    let mut f1 = lone_committed();
    f1.rec(
        4_000,
        RecoveryEvent::CopyCaughtUp {
            copy: B,
            head: Seq(20),
            digest: dg(0, 20),
        },
    );
    for copy in [A, B, C] {
        f1.rec(4_100, durable(copy, 20, dg(0, 20)));
    }
    assert_eq!(
        f1.step(4_200, cas_result(CasOutcome::Unknown)),
        vec![block(BlockReason::ControlUnknown)]
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Blocked(BlockReason::ControlUnknown)
    );
}

/// Committed and fully protected: rebuild inputs have nothing to act on.
#[retcd_test]
fn an_active_commit_ignores_rebuild_inputs() {
    let mut f1 = proposing(20);
    f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9))));
    assert_eq!(
        f1.rec(4_000, durable(A, 20, dg(0, 20))),
        vec![ign(ReplicaIgnoreReason::OutOfPhase)]
    );
    assert_eq!(
        f1.step(4_000, fired(1)),
        vec![ign(ReplicaIgnoreReason::StaleTimer)]
    );
}

// ---------------------------------------------------------------------------------------------
// B-R41: the tester's NOT YET gate, one section per ruling
// ---------------------------------------------------------------------------------------------

fn incomplete(missing: Vec<CopyId>) -> BlockReason {
    BlockReason::BarrierIncomplete { missing }
}

fn has_query(effects: &[EffectKind]) -> bool {
    effects.first()
        == Some(&r(RecoveryEffect::QueryInventory {
            copies: vec![A, B, C],
        }))
}

/// Ruling F-b(i): a fence proves the lineage the plan is anchored on, or it is not this plan's
/// fence.
#[retcd_test]
fn a_fence_must_prove_the_plans_anchor() {
    let mut f1 = F1::new();
    f1.rec(0, RecoveryEvent::Plan(Box::new(plan(&[]))));
    let newer_epoch = FencingProof {
        prior_owner_epoch: OwnerEpoch(2),
        ..proof()
    };
    let other_partition = FencingProof {
        partition: PartitionId(2),
        ..proof()
    };
    for wrong in [refence(), newer_epoch, other_partition] {
        assert_eq!(
            f1.rec(1, RecoveryEvent::FenceProven(Box::new(wrong))),
            vec![ign(ReplicaIgnoreReason::InvalidConfig)]
        );
        assert_eq!(f1.phase(), RecoveryPhase::Idle);
    }
    assert!(has_query(
        &f1.rec(2, RecoveryEvent::FenceProven(Box::new(proof())))
    ));
}

/// Ruling F-b(ii): a survivor already on a newer root of this partition means a peer recovered
/// first, so this plan is stale. It blocks, and never selects around the newer root. Newer is
/// `(generation, owner_epoch)` compared in that order. An older root, or another partition's,
/// is only stale lineage.
#[retcd_test]
fn a_survivor_on_a_newer_root_means_the_plan_is_stale() {
    let newer_generation = Lineage {
        generation: Generation(8),
        ..prior()
    };
    let newer_epoch = Lineage {
        owner_epoch: OwnerEpoch(2),
        ..prior()
    };
    for newer in [root(), newer_generation, newer_epoch] {
        let mut f1 = fenced(&[]);
        f1.report(10, inv(A, 20));
        let mut ahead = on_new_root(B, 30);
        ahead.anchor_seen.lineage = newer;
        assert_eq!(
            f1.report(11, ahead),
            vec![block(BlockReason::OvertakenByPeer)],
            "{newer:?}"
        );
        assert_eq!(
            f1.phase(),
            RecoveryPhase::Blocked(BlockReason::OvertakenByPeer)
        );
        assert!(!has_selected(&f1.log));
    }
    let mut f1 = fenced(&[]);
    let mut foreign = inv(B, 30);
    foreign.anchor_seen.lineage = Lineage {
        partition: PartitionId(2),
        ..root()
    };
    assert_eq!(
        f1.report(10, foreign),
        vec![lost(B, UnavailableReason::StaleLineage)]
    );
}

/// Ruling F-b(iii): Blocked holds a replacement plan, so the fresh fence runs on the new anchor
/// and never on the stale one.
#[retcd_test]
fn blocked_holds_a_new_plan_for_a_fresh_fence() {
    let mut f1 = fenced(&[]);
    f1.report(10, on_new_root(B, 30));
    let overtaken = RecoveryPhase::Blocked(BlockReason::OvertakenByPeer);
    assert_eq!(f1.phase(), overtaken);
    assert_eq!(
        f1.rec(9_000, RecoveryEvent::FenceProven(Box::new(refence()))),
        vec![ign(ReplicaIgnoreReason::InvalidConfig)],
        "the held plan is still the stale one"
    );
    assert_eq!(
        f1.rec(9_001, RecoveryEvent::Plan(Box::new(replan()))),
        vec![ign(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(f1.phase(), overtaken, "a plan is held, not acted on");
    assert_eq!(
        f1.rec(9_002, RecoveryEvent::FenceProven(Box::new(refence()))),
        vec![
            r(RecoveryEffect::QueryInventory {
                copies: vec![A, B, C]
            }),
            arm(2, 9_002 + WINDOW),
        ]
    );
    assert_eq!(
        f1.report(9_010, on_new_root(B, 30)),
        vec![ign(ReplicaIgnoreReason::Recorded)],
        "checked against the new anchor"
    );
}

/// Closed with A's ladder lacking the base rung (only its head and one stride rung); B and C are
/// full at 20. Selection probes A at the base.
fn baseless_closed() -> F1 {
    let baseless = SurvivorInventory {
        ladder: vec![(Seq(16), dg(0, 16))],
        ..inv(A, 20)
    };
    let (f1, close) = closed(&[], vec![baseless, inv(B, 20), inv(C, 20)], &[]);
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            r(RecoveryEffect::ProbeDigestAt {
                copy: A,
                seq: Seq(BASE)
            }),
            arm(2, 2 * WINDOW),
        ]
    );
    f1
}

/// Ruling F-c (design §5.2): a ladder without the base rung is probed at the base. The absence
/// is never taken as compatibility and never as divergence; the answer decides.
#[retcd_test]
fn a_missing_base_rung_is_probed_never_assumed() {
    let answer = |digest| RecoveryEvent::ProbeAnswered {
        copy: A,
        seq: Seq(BASE),
        digest,
    };
    let mut f1 = baseless_closed();
    assert_eq!(
        f1.rec(2_100, answer(dg(0, BASE))),
        vec![
            selected(20, A),
            sync(A, 20),
            sync(B, 20),
            sync(C, 20),
            arm(3, 2_100 + WINDOW),
        ]
    );

    let mut f1 = baseless_closed();
    assert_eq!(
        f1.rec(2_100, answer(dg(9, BASE))),
        vec![
            r(RecoveryEffect::Quarantine(
                DivergenceEvidence::RootMismatch {
                    copy: A,
                    base_seq: Seq(BASE),
                    expected: dg(0, BASE),
                    found: Some(dg(9, BASE)),
                }
            )),
            block(BlockReason::DivergenceRequiresOperator { diverged: vec![A] }),
        ]
    );

    let mut f1 = baseless_closed();
    assert_eq!(
        f1.rec(
            2_100,
            RecoveryEvent::ProbeUnavailable {
                copy: A,
                seq: Seq(BASE)
            }
        ),
        vec![
            lost(A, UnavailableReason::Stalled),
            selected(20, B),
            sync(B, 20),
            sync(C, 20),
            arm(3, 2_100 + WINDOW),
        ]
    );
}

/// Ruling F-c: a head below the base cannot hold the root. The copy is stale and recorded
/// unavailable, not divergent.
#[retcd_test]
fn a_head_below_the_base_is_ineligible_not_divergent() {
    let mut f1 = fenced(&[]);
    assert_eq!(
        f1.report(10, inv(A, BASE - 2)),
        vec![lost(A, UnavailableReason::StaleLineage)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Collecting);
}

/// Ruling F-d (spec §8.4 step 5): when the point pins, every required copy is asked to make it
/// durable. That includes the holders, not only the copy that caught up.
#[retcd_test]
fn rebuild_asks_every_required_copy_to_prove_the_point() {
    let mut f1 = lone_committed();
    assert_eq!(
        f1.rec(4_000, caught_up(B, 22, dg(0, 22))),
        vec![
            sync(A, 22),
            sync(B, 22),
            sync(C, 22),
            arm(4, 4_000 + WINDOW)
        ]
    );
    assert_eq!(
        f1.rec(4_001, caught_up(C, 22, dg(0, 22))),
        vec![sync(C, 22), arm(5, 4_001 + WINDOW)]
    );
    f1.rec(4_100, durable(A, 22, dg(0, 22)));
    f1.rec(4_101, durable(B, 22, dg(0, 22)));
    assert_eq!(
        f1.rec(4_102, durable(C, 22, dg(0, 22))),
        vec![cas(Revision(9), A), arm(6, 4_102 + WINDOW)]
    );
}

/// Ruling F-e: the rebuild point is never below the committed cutoff. A short catch-up is still
/// owed and pins nothing, so proofs below the cutoff never activate.
#[retcd_test]
fn the_rebuild_point_is_never_below_the_cutoff() {
    let mut f1 = lone_committed();
    assert_eq!(
        f1.rec(4_000, caught_up(B, 5, dg(0, 5))),
        vec![ign(ReplicaIgnoreReason::Outstanding)]
    );
    for copy in [A, B, C] {
        assert_eq!(
            f1.rec(4_100, durable(copy, 5, dg(0, 5))),
            vec![ign(ReplicaIgnoreReason::BarrierNotDurable)]
        );
    }
    assert_eq!(f1.cas_count(), 1);
    assert_eq!(
        f1.rec(4_200, caught_up(B, 20, dg(0, 20))),
        vec![
            sync(A, 20),
            sync(B, 20),
            sync(C, 20),
            arm(4, 4_200 + WINDOW)
        ]
    );
}

/// Ruling F-e: at the cutoff the digest must be the committed one. Anything else is a copy that
/// does not hold the root.
#[retcd_test]
fn a_foreign_digest_at_the_cutoff_is_divergence() {
    let mut f1 = lone_committed();
    assert_eq!(
        f1.rec(4_000, caught_up(B, 20, dg(4, 20))),
        vec![
            r(RecoveryEffect::Quarantine(
                DivergenceEvidence::RootMismatch {
                    copy: B,
                    base_seq: Seq(20),
                    expected: dg(0, 20),
                    found: Some(dg(4, 20)),
                }
            )),
            block(BlockReason::DivergenceRequiresOperator { diverged: vec![B] }),
        ]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
}

/// Ruling F-e: a pin is never replaced. A second digest at the rebuild point is divergence,
/// whether it arrives as a catch-up or as a proof.
#[retcd_test]
fn two_digests_at_the_rebuild_point_are_divergence() {
    let pairwise = |a: (CopyId, Digest), b: (CopyId, Digest), diverged: Vec<CopyId>| {
        vec![
            r(RecoveryEffect::Quarantine(DivergenceEvidence::Pairwise {
                seq: Seq(22),
                a,
                b,
            })),
            block(BlockReason::DivergenceRequiresOperator { diverged }),
        ]
    };
    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 22, dg(0, 22)));
    assert_eq!(
        f1.rec(4_001, caught_up(C, 22, dg(4, 22))),
        pairwise((B, dg(0, 22)), (C, dg(4, 22)), vec![B, C])
    );
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);

    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 22, dg(4, 22)));
    assert_eq!(
        f1.rec(4_100, durable(A, 22, dg(0, 22))),
        pairwise((B, dg(4, 22)), (A, dg(0, 22)), vec![A, B]),
        "sorted by copy id (ruling A-2)"
    );
}

/// Advisory 14: a catch-up short of `through` is still owed, and the barrier is not asked for.
#[retcd_test]
fn a_catch_up_short_of_the_cutoff_is_still_owed() {
    let (mut f1, _) = closed(&[], vec![inv(A, 20), inv(B, 12), inv(C, 20)], &[]);
    assert_eq!(
        f1.rec(2_100, caught_up(B, 15, dg(0, 15))),
        vec![ign(ReplicaIgnoreReason::Outstanding)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Synchronizing);
    assert_eq!(
        f1.rec(2_200, caught_up(B, 20, dg(0, 20))),
        vec![sync(A, 20), sync(B, 20), sync(C, 20)]
    );
}

/// Ruling F-a: before commit a lost required copy ends the wait at once and names it. Nothing is
/// proposed, and `required` does not shrink to fit.
#[retcd_test]
fn a_lost_required_copy_blocks_before_commit() {
    let (mut f1, _) = closed(&[], vec![inv(A, 20), inv(B, 12), inv(C, 20)], &[]);
    assert_eq!(
        f1.step(2_100, lose(D)),
        vec![ign(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Synchronizing);
    assert_eq!(
        f1.step(2_101, lose(C)),
        vec![block(incomplete(vec![C]))],
        "C was not behind, but the barrier needs it"
    );
    assert_eq!(f1.phase(), RecoveryPhase::Blocked(incomplete(vec![C])));

    let mut f1 = at_barrier(20);
    f1.rec(3_000, durable(A, 20, dg(0, 20)));
    assert_eq!(
        f1.step(3_001, lose(D)),
        vec![ign(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(f1.step(3_002, lose(B)), vec![block(incomplete(vec![B]))]);
    assert_eq!(f1.cas_count(), 0);
}

/// M7B-155 (ruling F-a; D §5.1): the wait after selection is bounded by a re-armed discovery
/// timer. At the deadline, in `Synchronizing` or at the `Barrier`, `BarrierIncomplete` names
/// exactly the copies still owing their catch-up, or a proof that reaches and binds, and nothing
/// is committed. Near-miss: every copy answers before the deadline, the recovery CAS is proposed,
/// and the fire is `StaleTimer` with no block. Promoted in place from the F-a scaffold.
#[retcd_test]
fn m7b_155_the_wait_after_selection_ends_in_barrier_incomplete() {
    let (mut f1, close) = closed(&[], vec![inv(A, 20), inv(B, 12), inv(C, 17)], &[]);
    assert_eq!(close.last(), Some(&arm(2, 2 * WINDOW)));
    f1.rec(2_100, caught_up(C, 20, dg(0, 20)));
    let stale = vec![ign(ReplicaIgnoreReason::StaleTimer)];
    assert_eq!(f1.step(2 * WINDOW - 1, fired(2)), stale);
    assert_eq!(f1.step(2 * WINDOW, fired(1)), stale);
    assert_eq!(
        f1.step(2 * WINDOW, fired(2)),
        vec![block(incomplete(vec![B]))]
    );

    // B falls short of the cutoff; C reaches past it but does not bind to the cutoff digest. A
    // foreign digest at the cutoff itself would be divergence (ruling A-4), not a missing proof.
    let mut f1 = at_barrier(20);
    f1.rec(3_000, durable(A, 20, dg(0, 20)));
    f1.rec(3_001, durable(B, 19, dg(0, 19)));
    f1.rec(3_002, durable(C, 21, dg(0, 21)));
    assert_eq!(f1.step(2 * WINDOW - 1, fired(2)), stale);
    assert_eq!(
        f1.step(2 * WINDOW, fired(2)),
        vec![block(incomplete(vec![B, C]))]
    );
    assert_eq!(f1.cas_count(), 0);

    // Near-miss: the same wait, answered in full before the deadline.
    let (mut f1, _) = closed(&[], vec![inv(A, 20), inv(B, 12), inv(C, 17)], &[]);
    f1.rec(2_100, caught_up(C, 20, dg(0, 20)));
    f1.rec(2_200, caught_up(B, 20, dg(0, 20)));
    assert_eq!(f1.phase(), RecoveryPhase::Barrier);
    for copy in [A, B, C] {
        f1.rec(3_000, durable(copy, 20, dg(0, 20)));
    }
    assert_eq!(f1.phase(), RecoveryPhase::Proposing);
    assert_eq!(f1.step(2 * WINDOW, fired(2)), stale);
    assert_eq!(f1.phase(), RecoveryPhase::Proposing);
    assert_eq!(f1.cas_count(), 1);
    assert!(!f1.log.iter().any(|e| matches!(
        e,
        EffectKind::Kernel(KernelEffect::Recovery(
            RecoveryEffect::BlockPromotion { .. }
        ))
    )));
}

/// Rulings F-f and F-g on the activation CAS: its prior is the committed record's epoch, so a
/// peer at a newer epoch overtakes it, and so do bytes identical to our proposal, which here sit
/// at that same epoch. Any other record is contention.
#[retcd_test]
fn an_activation_conflict_classifies_by_epoch() {
    let peer = |owner_epoch| PartitionRecord {
        owner: NodeId(2),
        owner_epoch,
        ..record(A)
    };
    for (current, reason) in [
        (peer(OwnerEpoch(3)), BlockReason::OvertakenByPeer),
        (record(A), BlockReason::OvertakenByPeer),
        (peer(OwnerEpoch(2)), BlockReason::CasContention),
    ] {
        let mut f1 = lone_committed();
        f1.rec(4_000, caught_up(B, 20, dg(0, 20)));
        for copy in [A, B, C] {
            f1.rec(4_100, durable(copy, 20, dg(0, 20)));
        }
        assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
        f1.step(
            4_200,
            cas_result(CasOutcome::Conflict {
                exists: true,
                current: Revision(12),
            }),
        );
        assert_eq!(f1.step(4_300, found(current.encode())), vec![block(reason)]);
    }
}

// ---------------------------------------------------------------------------------------------
// Tester rows (manual tester, mutation gaps), ported from the F1 gate export. Not plan rows; no
// m7b_ name. Two answers moved with B-R41, and each says which ruling moved it.
// ---------------------------------------------------------------------------------------------

/// Kills M07. A copy whose ladder lacks the base rung has no ancestry evidence; it is never
/// selected on that ladder alone (design §5.2: "never assumes compatibility from the absence").
#[retcd_test]
fn tester_a_copy_without_base_evidence_is_never_selected_unverified() {
    let mut f1 = fenced(&[]);
    f1.report(
        10,
        SurvivorInventory {
            ladder: vec![(Seq(16), dg(0, 16))],
            ..inv(A, 20)
        },
    );
    f1.report(10, inv(B, 20));
    f1.report(10, inv(C, 20));
    let close = f1.step(WINDOW, fired(1));
    assert!(
        !f1.log.contains(&selected(20, A)),
        "selected on no ancestry evidence: {:?}",
        f1.log
    );
    assert!(!close.contains(&selected(20, A)));
    assert_eq!(f1.cas_count(), 0);
}

/// Kills M08. A prefix holder without a valid grant does not lead; the first viable candidate
/// does, and its transfer is `CatchUpBeforeGrant` (spec §8.3).
#[retcd_test]
fn tester_a_holder_without_a_valid_grant_does_not_lead() {
    let mut f1 = F1::new();
    let mut p = plan(&[]);
    p.candidates[0].has_valid_grant = false;
    f1.rec(0, RecoveryEvent::Plan(Box::new(p)));
    f1.rec(0, RecoveryEvent::FenceProven(Box::new(proof())));
    for inventory in [inv(A, 20), inv(B, 18), inv(C, 18)] {
        f1.report(10, inventory);
    }
    let close = f1.step(WINDOW, fired(1));
    assert!(
        close.contains(&r(RecoveryEffect::CatchUpBeforeGrant {
            from: A,
            to: B,
            through: Seq(20),
            credential: proof().credential_for(A),
        })),
        "{close:?}"
    );
}

/// Kills M25. A viable prefix holder leads even when placement lists it last. Ruling F-a adds the
/// trailing `Arm`: the wait after selection is bounded.
#[retcd_test]
fn tester_a_viable_holder_leads_whatever_the_candidate_order() {
    let mut f1 = F1::new();
    let mut p = plan(&[]);
    p.candidates.reverse();
    f1.rec(0, RecoveryEvent::Plan(Box::new(p)));
    f1.rec(0, RecoveryEvent::FenceProven(Box::new(proof())));
    for inventory in [inv(A, 20), inv(B, 18), inv(C, 18)] {
        f1.report(10, inventory);
    }
    assert_eq!(
        f1.step(WINDOW, fired(1)),
        vec![
            r(RecoveryEffect::CloseWindow),
            selected(20, A),
            catch_up(A, B, 20),
            catch_up(A, C, 20),
            arm(2, 2 * WINDOW),
        ]
    );
}

/// Kills M22. A `DegradedRf2` commit rebuilds too: the third copy's catch-up is answered. Ruling
/// F-d moves the answer: the pin asks every required copy, holders included, to prove the point.
#[retcd_test]
fn tester_a_degraded_rf2_commit_rebuilds_the_third_copy() {
    let (mut f1, _) = closed(&[], vec![inv(A, 20), inv(B, 20)], &[C]);
    f1.rec(3_000, durable(A, 20, dg(0, 20)));
    f1.rec(3_000, durable(B, 20, dg(0, 20)));
    let effects = f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9))));
    assert_eq!(recovered(&effects).mode, PartitionMode::DegradedRf2);
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    assert_eq!(
        f1.rec(4_000, caught_up(C, 20, dg(0, 20))),
        vec![
            sync(A, 20),
            sync(B, 20),
            sync(C, 20),
            arm(4, 4_000 + WINDOW)
        ]
    );
}

// ---------------------------------------------------------------------------------------------
// B-R45: the re-gate's F-g and advisories A-1 to A-5, one row per ruling
// ---------------------------------------------------------------------------------------------

/// The quarantine and the block that names `diverged`.
fn quarantined(evidence: DivergenceEvidence, diverged: Vec<CopyId>) -> Vec<EffectKind> {
    vec![
        r(RecoveryEffect::Quarantine(evidence)),
        block(BlockReason::DivergenceRequiresOperator { diverged }),
    ]
}

/// `copy` reported a foreign digest, `dg(7, 20)`, at the committed cutoff 20.
fn off_the_cutoff(copy: CopyId) -> Vec<EffectKind> {
    quarantined(
        DivergenceEvidence::RootMismatch {
            copy,
            base_seq: Seq(20),
            expected: dg(0, 20),
            found: Some(dg(7, 20)),
        },
        vec![copy],
    )
}

/// Ruling A-1: a proof is judged the same whenever it lands. At the committed cutoff it must carry
/// the cutoff digest, before the pin or after, wherever the point is. A proof held before the pin
/// is judged against the pin when it lands.
#[retcd_test]
fn a_proof_is_judged_whenever_it_arrives() {
    let mut f1 = lone_committed();
    assert_eq!(f1.rec(4_000, durable(A, 20, dg(7, 20))), off_the_cutoff(A));

    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 25, dg(0, 25)));
    assert_eq!(f1.rec(4_100, durable(C, 20, dg(7, 20))), off_the_cutoff(C));

    let mut f1 = lone_committed();
    assert_eq!(
        f1.rec(4_000, durable(A, 22, dg(4, 22))),
        vec![ign(ReplicaIgnoreReason::BarrierNotDurable)]
    );
    assert_eq!(
        f1.rec(4_100, caught_up(B, 22, dg(0, 22))),
        quarantined(
            DivergenceEvidence::Pairwise {
                seq: Seq(22),
                a: (B, dg(0, 22)),
                b: (A, dg(4, 22)),
            },
            vec![A, B]
        )
    );
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
}

/// Ruling A-2: the block names each diverged copy once, even a copy that contradicts its own pin.
#[retcd_test]
fn a_copy_that_contradicts_its_own_pin_is_named_once() {
    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 25, dg(0, 25)));
    assert_eq!(
        f1.rec(4_100, durable(B, 25, dg(7, 25))),
        quarantined(
            DivergenceEvidence::Pairwise {
                seq: Seq(25),
                a: (B, dg(0, 25)),
                b: (B, dg(7, 25)),
            },
            vec![B]
        )
    );
}

/// Ruling A-3: a copy already recorded lost never chooses the rebuild point. Its catch-up is
/// refused, and the point is pinned by a live copy.
#[retcd_test]
fn a_lost_copy_never_sets_the_rebuild_point() {
    let mut f1 = lone_committed();
    f1.step(4_000, lose(B));
    assert_eq!(
        f1.rec(4_100, caught_up(B, 25, dg(0, 25))),
        vec![ign(ReplicaIgnoreReason::NotASource)]
    );
    assert_eq!(
        f1.rec(4_200, caught_up(C, 20, dg(0, 20))),
        vec![
            sync(A, 20),
            sync(B, 20),
            sync(C, 20),
            arm(4, 4_200 + WINDOW)
        ]
    );
}

/// Ruling A-4: before commit, a foreign digest at the cutoff quarantines, from a catch-up or a
/// proof, the same as after commit (F-e). It is never read as a copy still owing its proof. A
/// copy the barrier does not need is not judged, as after commit.
#[retcd_test]
fn before_commit_a_foreign_digest_at_the_cutoff_is_divergence() {
    let (mut f1, _) = closed(&[], vec![inv(A, 20), inv(B, 12), inv(C, 20)], &[]);
    assert_eq!(
        f1.rec(2_050, caught_up(D, 20, dg(7, 20))),
        vec![ign(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(
        f1.rec(2_100, caught_up(B, 20, dg(7, 20))),
        off_the_cutoff(B)
    );
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);

    let mut f1 = at_barrier(20);
    assert_eq!(f1.rec(3_000, durable(B, 20, dg(7, 20))), off_the_cutoff(B));
    assert_eq!(f1.cas_count(), 0);
}

/// Ruling A-5: once a peer's decision blocks the run, a fence read no later than that decision is
/// refused by name; only a fence proven after it re-fences. The decision is seen either as a
/// survivor on a newer root (floor: the run's own fence read) or on the CAS re-read (floor: the
/// read's revision).
#[retcd_test]
fn after_overtaken_only_a_newer_fence_refences() {
    let at = |revision| {
        RecoveryEvent::FenceProven(Box::new(FencingProof {
            control_revision: Revision(revision),
            ..proof()
        }))
    };
    let refused = vec![ign(ReplicaIgnoreReason::RecoveryBlocked)];
    let overtaken = RecoveryPhase::Blocked(BlockReason::OvertakenByPeer);

    let mut f1 = fenced(&[]);
    f1.report(10, on_new_root(B, 30));
    assert_eq!(
        f1.rec(20, at(CONTROL_REV.0)),
        refused,
        "the old fence, replayed"
    );
    assert_eq!(f1.phase(), overtaken);
    assert!(has_query(&f1.rec(30, at(CONTROL_REV.0 + 1))));

    let mut f1 = rereading();
    f1.step(3_200, found(record(A).encode()));
    for revision in [CONTROL_REV.0, 6] {
        assert_eq!(f1.rec(3_300, at(revision)), refused, "read at {revision}");
        assert_eq!(f1.phase(), overtaken);
    }
    assert!(has_query(&f1.rec(3_400, at(7))));
}

/// Ruling D-1 (B-R45b), ported from the tester's `probe_dz_g2`: on either CAS path the floor is
/// the newer of the run's own fence read and the re-read. A peer's record read at a revision older
/// than our fence still refuses our own fence, replayed; only a fence past both re-fences.
#[retcd_test]
fn a_re_read_older_than_our_fence_never_lowers_the_floor() {
    let peer = PartitionRecord {
        owner: NodeId(2),
        owner_epoch: OwnerEpoch(3),
        ..record(A)
    };
    let fence_at = |revision| {
        RecoveryEvent::FenceProven(Box::new(FencingProof {
            control_revision: Revision(revision),
            ..proof()
        }))
    };
    let mut activation = lone_committed();
    activation.rec(4_000, caught_up(B, 20, dg(0, 20)));
    for copy in [A, B, C] {
        activation.rec(4_100, durable(copy, 20, dg(0, 20)));
    }
    activation.step(
        4_200,
        cas_result(CasOutcome::Conflict {
            exists: true,
            current: Revision(12),
        }),
    );
    for (path, mut f1) in [
        ("recovery CAS", rereading()),
        ("activation CAS", activation),
    ] {
        let stale_read = read_result(ReadOutcome::Found {
            revision: Revision(3),
            value: peer.encode(),
        });
        assert_eq!(
            f1.step(4_300, stale_read),
            vec![block(BlockReason::OvertakenByPeer)],
            "{path}"
        );
        assert_eq!(
            f1.rec(4_400, fence_at(CONTROL_REV.0)),
            vec![ign(ReplicaIgnoreReason::RecoveryBlocked)],
            "{path}: our own fence, replayed"
        );
        assert_eq!(
            f1.phase(),
            RecoveryPhase::Blocked(BlockReason::OvertakenByPeer),
            "{path}"
        );
        assert!(
            has_query(&f1.rec(4_500, fence_at(CONTROL_REV.0 + 1))),
            "{path}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Tester rows from the B-R41 re-gate (manual tester, mutation gaps). Not plan rows; no m7b_ name.
// ---------------------------------------------------------------------------------------------

/// Kills re-gate G03 and G04. "Newer" is the same partition and `(generation, owner_epoch)`
/// greater, generation first (ruling F-b(ii)). The plan's own lineage seen at another base is not
/// newer.
#[retcd_test]
fn tester_b_a_newer_anchor_orders_generation_before_epoch() {
    let seen = |generation: u64, epoch: u64, base: u64| SurvivorInventory {
        anchor_seen: LineageAnchor {
            lineage: Lineage {
                partition: PARTITION,
                generation: Generation(generation),
                owner_epoch: OwnerEpoch(epoch),
            },
            base_seq: Seq(base),
            base_digest: dg(0, base),
        },
        ..inv(A, 30)
    };
    let overtaken = vec![block(BlockReason::OvertakenByPeer)];
    let stale = vec![lost(A, UnavailableReason::StaleLineage)];
    for (inventory, want) in [
        (seen(8, 0, BASE), overtaken),
        (seen(6, 9, BASE), stale.clone()),
        (seen(7, 1, 12), stale),
    ] {
        let mut f1 = fenced(&[]);
        assert_eq!(f1.report(10, inventory), want);
    }
}

/// Kills re-gate G05. A head exactly at the base is verified, not ineligible (ruling F-c: only a
/// head below the base is).
#[retcd_test]
fn tester_b_a_head_at_the_base_is_verified() {
    let mut f1 = fenced(&[]);
    assert_eq!(
        f1.report(10, inv(B, BASE)),
        vec![ign(ReplicaIgnoreReason::Recorded)]
    );
}

/// Kills re-gate G16. A short catch-up is `Outstanding` only for a copy still owed one; any other
/// copy is `NotRequired`.
#[retcd_test]
fn tester_b_a_short_catch_up_from_a_copy_not_owed_is_not_required() {
    let (mut f1, _) = closed(&[], vec![inv(A, 20), inv(B, 12), inv(C, 17)], &[]);
    assert_eq!(
        f1.rec(2_100, caught_up(A, 15, dg(0, 15))),
        vec![ign(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Synchronizing);
}

// ---------------------------------------------------------------------------------------------
// Tester rows from the B-R45 delta check (manual tester, mutation gaps). Not plan rows; no m7b_
// name.
// ---------------------------------------------------------------------------------------------

/// Kills delta H12. A fence for another lineage is not this plan's fence, whatever its revision:
/// the lineage check runs before the floor (ruling B-R45a Q4).
#[retcd_test]
fn tester_b_b_the_lineage_check_runs_before_the_floor() {
    let mut f1 = fenced(&[]);
    f1.report(10, on_new_root(B, 25));
    f1.rec(20, RecoveryEvent::Plan(Box::new(replan())));
    assert_eq!(
        f1.rec(30, RecoveryEvent::FenceProven(Box::new(proof()))),
        vec![ign(ReplicaIgnoreReason::InvalidConfig)]
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Blocked(BlockReason::OvertakenByPeer)
    );
}

/// Kills delta H21. A report that breaks both the pin and the committed cutoff is named
/// `Pairwise` (ruling B-R45a Q6).
#[retcd_test]
fn tester_b_b_a_report_breaking_pin_and_cutoff_is_pairwise() {
    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 20, dg(0, 20)));
    let evidence = DivergenceEvidence::Pairwise {
        seq: Seq(20),
        a: (B, dg(0, 20)),
        b: (C, dg(7, 20)),
    };
    assert_eq!(
        f1.rec(4_100, caught_up(C, 20, dg(7, 20))),
        vec![
            r(RecoveryEffect::Quarantine(evidence)),
            block(BlockReason::DivergenceRequiresOperator {
                diverged: vec![B, C]
            }),
        ]
    );
}

/// Kills delta H23. A copy recorded lost is judged for divergence before it is refused as a
/// source (ruling A-3).
#[retcd_test]
fn tester_b_b_a_lost_copy_is_judged_before_it_is_refused() {
    let mut f1 = lone_committed();
    f1.step(4_000, lose(C));
    let evidence = DivergenceEvidence::RootMismatch {
        copy: C,
        base_seq: Seq(20),
        expected: dg(0, 20),
        found: Some(dg(7, 20)),
    };
    assert_eq!(
        f1.rec(4_100, caught_up(C, 20, dg(7, 20))),
        vec![
            r(RecoveryEffect::Quarantine(evidence)),
            block(BlockReason::DivergenceRequiresOperator { diverged: vec![C] }),
        ]
    );
}

// ---------------------------------------------------------------------------------------------
// Plan rows, team kernel-b test plan §8.1: phases, inventory and ancestry (M7B-84..91)
// ---------------------------------------------------------------------------------------------

fn fence_event() -> EventKind {
    EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(Box::new(
        proof(),
    ))))
}

fn verified(reports: &[SurvivorInventory]) -> Vec<VerifiedInventory> {
    reports
        .iter()
        .map(|report| verify_ancestry(&anchor(), report).expect("on the committed root"))
        .collect()
}

fn is_quarantine(effect: &EffectKind) -> bool {
    matches!(
        effect,
        EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::Quarantine(_)))
    )
}

fn is_probe(effect: &EffectKind) -> bool {
    matches!(
        effect,
        EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::ProbeDigestAt { .. }))
    )
}

/// M7B-84 (D §5.1, §2.1, ADR 0009 §1): `Idle` leaves on a fence and on nothing else. The plan's
/// fourth input, a `CasResult`, is declined rather than answered `NotFenced`: F1 has no CAS
/// pending, and a control answer it did not request is not its input at all (the decline rule,
/// `control_answers_f1_did_not_request_are_declined`). Either way `Idle` holds.
#[retcd_test]
fn m7b_84_idle_accepts_only_fence_proven() {
    let mut f1 = F1::new();
    f1.rec(0, RecoveryEvent::Plan(Box::new(plan(&[]))));
    let not_fenced = vec![ign(ReplicaIgnoreReason::NotFenced)];
    assert_eq!(f1.report(1, inv(A, 20)), not_fenced);
    let deadline_now = EventKind::Timer(TimerFired {
        id: DISCOVERY_TIMER,
        version: TimerVersion(1),
        scheduled_at: Tick(2),
    });
    assert_eq!(f1.step(2, deadline_now), not_fenced);
    assert_eq!(f1.rec(3, durable(A, 20, dg(0, 20))), not_fenced);
    f1.declines(4, cas_result(CasOutcome::Committed(Revision(9))));
    assert_eq!(f1.phase(), RecoveryPhase::Idle);
    assert_eq!(
        f1.step(5, fence_event()),
        vec![
            r(RecoveryEffect::QueryInventory {
                copies: vec![A, B, C]
            }),
            arm(1, 5 + WINDOW),
        ]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
}

/// M7B-85 (D §5.5, ADR 0009 §3): the window runs from the fence's arrival tick. Stepped at 900
/// but arrived at 700, so the deadline is 2700, not 2900; 2699 does not close it, 2700 does.
#[retcd_test]
fn m7b_85_window_is_anchored_to_the_arrival_tick() {
    assert_eq!(WINDOW, 2_000);
    let mut f1 = F1::new();
    f1.rec(0, RecoveryEvent::Plan(Box::new(plan(&[]))));
    assert_eq!(
        f1.at(900, 700, fence_event()),
        vec![
            r(RecoveryEffect::QueryInventory {
                copies: vec![A, B, C]
            }),
            arm(1, 2_700),
        ]
    );
    for copy in [A, B, C] {
        f1.report(1_000, inv(copy, 20));
    }
    assert_eq!(
        f1.step(2_699, fired(1)),
        vec![ign(ReplicaIgnoreReason::StaleTimer)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Collecting);
    assert_eq!(f1.step(2_700, fired(1))[0], r(RecoveryEffect::CloseWindow));
}

/// B, whose history is `ineligible_b`, reports beside A and C at 20; the run commits on A and C.
/// B's head is the longest, so a selection that read it would pick B.
fn commit_without_b(ineligible_b: SurvivorInventory) -> (F1, RecoveryResult) {
    let (mut f1, close) = closed(&[], vec![inv(A, 20), ineligible_b, inv(C, 20)], &[]);
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            selected(20, A),
            sync(A, 20),
            sync(C, 20),
            arm(2, 2 * WINDOW),
        ]
    );
    for copy in [A, C] {
        f1.rec(3_000, durable(copy, 20, dg(0, 20)));
    }
    let result = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
    (f1, result)
}

/// M7B-86 (D §5.3 step 1, ADR 0009 §4): a survivor on another lineage is ineligible, never
/// divergent. It is recorded as lost, and its longer head is not in the selection input.
#[retcd_test]
fn m7b_86_stale_lineage_is_ineligible_not_divergence() {
    let stale = SurvivorInventory {
        anchor_seen: LineageAnchor {
            lineage: Lineage {
                generation: Generation(6),
                ..prior()
            },
            ..anchor()
        },
        ..inv(B, 40)
    };
    assert_eq!(
        verify_ancestry(&anchor(), &stale),
        Err(Rejected::Ineligible(UnavailableReason::StaleLineage))
    );
    let (f1, result) = commit_without_b(stale);
    assert_eq!(
        result.loss.unavailable,
        vec![(B, UnavailableReason::StaleLineage)]
    );
    assert!(!f1.log.iter().any(is_quarantine), "no divergence");
}

/// M7B-87 (D §5.3 step 2, B-R17): a quarantined survivor is ineligible and kept as evidence.
/// Twin of M7B-86 by one field.
#[retcd_test]
fn m7b_87_quarantined_survivor_is_ineligible_and_kept_as_evidence() {
    let quarantined = SurvivorInventory {
        quarantined: Some(QuarantineReason::CorruptHistory),
        ..inv(B, 40)
    };
    assert_eq!(
        verify_ancestry(&anchor(), &quarantined),
        Err(Rejected::Ineligible(UnavailableReason::Quarantined))
    );
    let (_, result) = commit_without_b(quarantined);
    assert_eq!(
        result.inventories,
        vec![
            InventoryOutcome::Verified { copy: A },
            InventoryOutcome::Ineligible {
                copy: B,
                reason: UnavailableReason::Quarantined,
            },
            InventoryOutcome::Verified { copy: C },
        ]
    );
    assert_eq!(
        result.loss.unavailable,
        vec![(B, UnavailableReason::Quarantined)]
    );
}

/// M7B-88 (D §5.3 step 3, ADR 0009 §4): a digest at `base_seq` other than the root's is
/// divergence. The run quarantines and never selects.
#[retcd_test]
fn m7b_88_root_mismatch_is_divergence() {
    let mut wrong = inv(B, 20);
    wrong.ladder[0].1 = dg(9, BASE);
    let evidence = DivergenceEvidence::RootMismatch {
        copy: B,
        base_seq: Seq(BASE),
        expected: dg(0, BASE),
        found: Some(dg(9, BASE)),
    };
    assert_eq!(
        verify_ancestry(&anchor(), &wrong),
        Err(Rejected::Divergence(evidence))
    );
    let (f1, _) = closed(&[], vec![inv(A, 20), wrong, inv(C, 20)], &[]);
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
    assert!(!has_selected(&f1.log), "{:?}", f1.log);
}

/// M7B-89 (D §5.3 "the pairwise loop is the only guard", D §7, BA-9): two histories sharing the
/// root and forking above it never select, whichever is longer. BA-9's first option, enumerated
/// instead of seeded: every fork from the root to 29 and every pair of heads above it to 30,
/// 2870 pairs. The bounds go to the test's JSONL.
#[retcd_test]
fn m7b_89_divergent_above_root_pairs_never_select() {
    const TOP: u64 = 30;
    let mut pairs = 0;
    for fork in BASE..TOP {
        for head_a in fork + 1..=TOP {
            for head_b in fork + 1..=TOP {
                let both = verified(&[inv_on(A, head_a, 1, fork), inv_on(B, head_b, 2, fork)]);
                let outcome = select_prefix(&both, root());
                assert!(
                    matches!(
                        outcome,
                        SelectionOutcome::Divergence(DivergenceEvidence::Pairwise { .. })
                    ),
                    "fork {fork}, heads {head_a}/{head_b}: {outcome:?}"
                );
                pairs += 1;
            }
        }
    }
    assert_eq!(pairs, 2_870);
    tracing::info!(
        pairs,
        fork_from = BASE,
        top = TOP,
        "m7b_89 enumerated divergent pairs"
    );
}

/// The M7B-90 fixture: A and D full ladders at 40, B and C sparse at 50. A and D each need B's and
/// C's digest at 40, so `(B, 40)` and `(C, 40)` are each asked for twice. `d` is D's history.
fn probe_fixture(d: SurvivorInventory) -> (Vec<SurvivorInventory>, [Member; 1]) {
    (
        vec![inv(A, 40), sparse(B, 50), sparse(C, 50), d],
        [member(D, ReplicaRole::RegularSecondary)],
    )
}

/// M7B-90 (D §5.4 `NeedProbes`, ADR 0009 §4): the probes a selection needs are deduplicated,
/// sorted and sent in one vector, and the run stays collecting.
#[retcd_test]
fn m7b_90_needed_probes_are_deduplicated_sorted_and_batched() {
    let (reports, extra) = probe_fixture(inv(D, 40));
    assert_eq!(
        select_prefix(&verified(&reports), root()),
        SelectionOutcome::NeedProbes(vec![(B, Seq(40)), (C, Seq(40))])
    );
    let (f1, close) = closed(&extra, reports, &[]);
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            r(RecoveryEffect::ProbeDigestAt {
                copy: B,
                seq: Seq(40)
            }),
            r(RecoveryEffect::ProbeDigestAt {
                copy: C,
                seq: Seq(40)
            }),
            arm(2, 2 * WINDOW),
        ]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Collecting);
}

/// M7B-91 (D §5.4 order): divergence is decided before probes are sent. Twin of M7B-90 by one
/// digest: D's head digest at 40 is off A's history, while B and C still owe probes.
#[retcd_test]
fn m7b_91_divergence_is_decided_before_probes_are_sent() {
    let (reports, extra) = probe_fixture(inv_on(D, 40, 3, 39));
    let evidence = DivergenceEvidence::Pairwise {
        seq: Seq(40),
        a: (A, dg(0, 40)),
        b: (D, dg(3, 40)),
    };
    assert_eq!(
        select_prefix(&verified(&reports), root()),
        SelectionOutcome::Divergence(evidence)
    );
    let (f1, close) = closed(&extra, reports, &[]);
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            r(RecoveryEffect::Quarantine(evidence)),
            block(BlockReason::DivergenceRequiresOperator {
                diverged: vec![A, D]
            }),
        ]
    );
    assert!(!f1.log.iter().any(is_probe), "{:?}", f1.log);
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
}

// ---------------------------------------------------------------------------------------------
// Plan rows, §8.2: selection, window and leader (M7B-92..98)
// ---------------------------------------------------------------------------------------------

/// M7B-92 (spec §5 F1; charter "all unequal secondary prefix pairings"; D §5.4; gate V3): with the
/// old primary A gone, every unequal pairing of B and C over heads {10, 20, 30} selects the longer
/// at its head, and the loss is certain because nothing was advertised past it. The
/// synchronization half is M7B-136 (sim).
#[retcd_test]
fn m7b_92_all_unequal_secondary_prefix_pairings_select_longest_compatible() {
    let heads = [10, 20, 30];
    let mut pairings = 0;
    for head_b in heads {
        for head_c in heads.into_iter().filter(|head| *head != head_b) {
            let (holder, behind, cutoff) = if head_b > head_c {
                (B, C, head_b)
            } else {
                (C, B, head_c)
            };
            let (mut f1, close) = closed(&[], vec![inv(B, head_b), inv(C, head_c)], &[A]);
            assert_eq!(
                close[..3],
                [
                    r(RecoveryEffect::CloseWindow),
                    selected(cutoff, holder),
                    catch_up(holder, behind, cutoff),
                ],
                "B {head_b}, C {head_c}"
            );
            f1.rec(2_100, caught_up(behind, cutoff, dg(0, cutoff)));
            for copy in [B, C] {
                f1.rec(2_200, durable(copy, cutoff, dg(0, cutoff)));
            }
            let result = recovered(&f1.step(2_300, cas_result(CasOutcome::Committed(Revision(9)))));
            assert_eq!(result.selected.source, holder);
            assert_eq!(result.loss.cutoff_seq, Seq(cutoff));
            assert!(!result.loss.uncertain, "B {head_b}, C {head_c}");
            pairings += 1;
        }
    }
    assert_eq!(pairings, 6);
}

/// M7B-93 (D §5.4 loop `Collecting -> NeedProbes -> Collecting -> Selected`): the first step asks
/// for a probe and stays collecting; a `Match` answer selects, a `Differs` answer is divergence.
#[retcd_test]
fn m7b_93_collecting_loop_probe_then_select() {
    for matches in [true, false] {
        let (mut f1, close) = closed(&[], vec![sparse(A, 20), inv(B, 15), inv(C, 15)], &[]);
        assert_eq!(
            close,
            vec![
                r(RecoveryEffect::CloseWindow),
                r(RecoveryEffect::ProbeDigestAt {
                    copy: A,
                    seq: Seq(15)
                }),
                arm(2, 2 * WINDOW),
            ]
        );
        assert_eq!(f1.phase(), RecoveryPhase::Collecting);
        let answer = if matches { dg(0, 15) } else { dg(5, 15) };
        let effects = f1.rec(
            2_100,
            RecoveryEvent::ProbeAnswered {
                copy: A,
                seq: Seq(15),
                digest: answer,
            },
        );
        if matches {
            assert_eq!(effects[0], selected(20, A));
            assert_eq!(f1.phase(), RecoveryPhase::Synchronizing);
        } else {
            assert_eq!(
                effects,
                vec![
                    r(RecoveryEffect::Quarantine(DivergenceEvidence::Pairwise {
                        seq: Seq(15),
                        a: (B, dg(0, 15)),
                        b: (A, dg(5, 15)),
                    })),
                    block(BlockReason::DivergenceRequiresOperator {
                        diverged: vec![A, B]
                    }),
                ]
            );
            assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
        }
    }
}

fn transfer(copy: CopyId, received: u64) -> RecoveryEvent {
    RecoveryEvent::TransferProgress {
        copy,
        advertised_seq: Seq(30),
        received_seq: Seq(received),
    }
}

/// M7B-94 (D §5.5 ordering; spec §6 "record source failure before choosing a shorter prefix"):
/// B and C transfer through three extensions; in the fourth window B still moves and C has
/// stopped. At the capped deadline both are recorded, each before `CloseWindow` in the same
/// vector, and the loss is uncertain because 30 was advertised past the cutoff at 20.
#[retcd_test]
fn m7b_94_stalled_sources_are_recorded_before_close_by_effect_index() {
    let mut f1 = fenced(&[]);
    f1.report(10, inv(A, 20));
    for window in 1..=3u64 {
        let deadline = window * WINDOW;
        f1.rec(deadline - 100, transfer(B, 5 * window));
        f1.rec(deadline - 100, transfer(C, 5 * window));
        assert_eq!(
            f1.step(deadline, fired(window)),
            vec![arm(window + 1, deadline + WINDOW)]
        );
    }
    f1.rec(4 * WINDOW - 100, transfer(B, 20));
    let close = f1.step(4 * WINDOW, fired(4));
    let index = |effect: &EffectKind| close.iter().position(|e| e == effect);
    let closed_at = index(&r(RecoveryEffect::CloseWindow)).expect("the window closes");
    for copy in [B, C] {
        let recorded = index(&lost(copy, UnavailableReason::Stalled));
        assert!(
            recorded.is_some_and(|at| at < closed_at),
            "{copy:?}: {close:?}"
        );
    }
    let every_record_first = close.iter().enumerate().all(|(at, effect)| {
        !matches!(
            effect,
            EffectKind::Kernel(KernelEffect::Recovery(
                RecoveryEffect::RecordSourceUnavailable { .. }
            ))
        ) || at < closed_at
    });
    assert!(every_record_first, "{close:?}");
    f1.rec(9_000, durable(A, 20, dg(0, 20)));
    let result = recovered(&f1.step(9_100, cas_result(CasOutcome::Committed(Revision(9)))));
    assert_eq!(result.loss.highest_advertised_seq, Seq(30));
    assert!(result.loss.uncertain);
}

/// M7B-95 (D §5.5 `MAX_WINDOW_EXTENSIONS = 3`, ADR 0009 §3): B advertises past the best head and
/// delivers one record per window. The deadlines at 2000, 4000 and 6000 extend by 2000 each; the
/// one at 8000 closes, exactly then and not a tick before.
#[retcd_test]
fn m7b_95_window_extends_at_most_three_times_then_closes() {
    assert_eq!(MAX_WINDOW_EXTENSIONS, 3);
    let mut f1 = fenced(&[]);
    f1.report(10, inv(A, 20));
    for (extension, deadline) in [(1, 2_000), (2, 4_000), (3, 6_000)] {
        f1.rec(deadline - 100, transfer(B, extension));
        assert_eq!(
            f1.step(deadline, fired(extension)),
            vec![arm(extension + 1, deadline + 2_000)],
            "extension {extension}"
        );
    }
    f1.rec(7_900, transfer(B, 4));
    assert_eq!(
        f1.step(7_999, fired(4)),
        vec![ign(ReplicaIgnoreReason::StaleTimer)]
    );
    assert_eq!(
        f1.step(8_000, fired(4)),
        vec![
            lost(B, UnavailableReason::Stalled),
            lost(C, UnavailableReason::Stalled),
            r(RecoveryEffect::CloseWindow),
            selected(20, A),
            sync(A, 20),
            arm(5, 10_000),
        ]
    );
}

/// M7B-98 (D §5.4 `select_leader`, `CatchUpBeforeGrant`; spec §8.3): B holds the prefix but may
/// not lead, so C is caught up before its grant and nothing is proposed in that step. The
/// proposal names C, and comes after C's catch-up and the barrier (M7B-99..102): `ProposeOwnership`
/// is the landed `ControlEffect::Cas` on `partitions/{id}`.
#[retcd_test]
fn m7b_98_holder_that_cannot_lead_gets_catch_up_before_grant() {
    let mut holder_cannot_lead = plan(&[]);
    holder_cannot_lead.candidates = vec![
        Candidate {
            primary_eligible: false,
            ..candidate(B)
        },
        candidate(C),
    ];
    let mut f1 = fenced_on(holder_cannot_lead);
    f1.report(10, inv(B, 30));
    f1.report(10, inv(C, 20));
    f1.rec(10, RecoveryEvent::InventoryFailed { copy: A });
    assert_eq!(
        f1.step(WINDOW, fired(1)),
        vec![
            r(RecoveryEffect::CloseWindow),
            selected(30, B),
            r(RecoveryEffect::CatchUpBeforeGrant {
                from: B,
                to: C,
                through: Seq(30),
                credential: proof().credential_for(B),
            }),
            arm(2, 2 * WINDOW),
        ]
    );
    assert_eq!(f1.cas_count(), 0);
    assert_eq!(
        f1.rec(2_100, caught_up(C, 30, dg(0, 30))),
        vec![sync(B, 30), sync(C, 30)]
    );
    f1.rec(2_200, durable(B, 30, dg(0, 30)));
    assert_eq!(
        f1.rec(2_300, durable(C, 30, dg(0, 30))),
        vec![cas(CONTROL_REV, C), arm(3, 2_300 + WINDOW)]
    );
}

// ---------------------------------------------------------------------------------------------
// Plan rows, §8.3: barrier, CAS and modes (M7B-99..112)
// ---------------------------------------------------------------------------------------------

/// The barrier rows' required set, `{B, C}`, cut at `(50, d50)`.
fn barrier_over_b_and_c(proofs: &[DurableProof]) -> Result<RecoveryBarrier, MissingProof> {
    RecoveryBarrier::try_new(proofs, &BTreeSet::from([B, C]), Seq(50), dg(0, 50))
}

/// A gone, B and C at 50: the run waits at the barrier with `required {B, C}`.
fn b_and_c_at_the_barrier() -> F1 {
    let (f1, _) = closed(&[], vec![inv(B, 50), inv(C, 50)], &[A]);
    assert_eq!(f1.phase(), RecoveryPhase::Barrier);
    f1
}

/// M7B-99 (D §5.6 `try_new` Ok, ADR 0009 §6): a complete proof set bound to the cutoff builds the
/// barrier, and the barrier holds exactly what it was built from.
#[retcd_test]
fn m7b_99_try_new_accepts_a_complete_bound_proof_set() {
    let proofs = [proven(B, 50, dg(0, 50)), proven(C, 50, dg(0, 50))];
    let barrier = barrier_over_b_and_c(&proofs).expect("complete and bound");
    assert_eq!(barrier.cutoff(), Seq(50));
    assert_eq!(barrier.cutoff_digest(), dg(0, 50));
    assert_eq!(barrier.required(), [B, C]);
    assert_eq!(barrier.proofs(), proofs);
    assert!(barrier.proofs().iter().all(|p| p.seq == DurableSeq(50)));
}

/// M7B-100 (D §5.6 `NoProofFrom`, D §7 "fallible ctor tested failing"): without C's proof there is
/// no barrier, and the run stays at it.
#[retcd_test]
fn m7b_100_try_new_rejects_a_missing_required_copy() {
    assert_eq!(
        barrier_over_b_and_c(&[proven(B, 50, dg(0, 50))]),
        Err(MissingProof::NoProofFrom(C))
    );
    let mut f1 = b_and_c_at_the_barrier();
    assert_eq!(
        f1.rec(3_000, durable(B, 50, dg(0, 50))),
        vec![ign(ReplicaIgnoreReason::BarrierNotDurable)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Barrier);
    assert_eq!(f1.cas_count(), 0);
}

/// M7B-101 (D §5.6 `ProofBelowCutoff`): durable at 49 says nothing about 50.
#[retcd_test]
fn m7b_101_try_new_rejects_a_proof_below_cutoff() {
    assert_eq!(
        barrier_over_b_and_c(&[proven(B, 50, dg(0, 50)), proven(C, 49, dg(0, 49))]),
        Err(MissingProof::ProofBelowCutoff {
            copy: C,
            proof_seq: DurableSeq(49),
            cutoff: Seq(50),
        })
    );
}

/// M7B-102 (D §5.6 `ProofDigestMismatch`, "durable at different histories"): twin of M7B-99 by one
/// digest.
#[retcd_test]
fn m7b_102_try_new_rejects_a_proof_with_the_wrong_digest() {
    assert_eq!(
        barrier_over_b_and_c(&[proven(B, 50, dg(0, 50)), proven(C, 50, dg(7, 50))]),
        Err(MissingProof::ProofDigestMismatch {
            copy: C,
            proof_digest: dg(7, 50),
            cutoff_digest: dg(0, 50),
        })
    );
}

/// M7B-103 (D §5.6 `UnknownCopy`): a proof from outside `required` refuses the set, even though B
/// and C are complete.
#[retcd_test]
fn m7b_103_try_new_rejects_a_proof_from_an_unknown_copy() {
    assert_eq!(
        barrier_over_b_and_c(&[
            proven(B, 50, dg(0, 50)),
            proven(C, 50, dg(0, 50)),
            proven(D, 50, dg(0, 50)),
        ]),
        Err(MissingProof::UnknownCopy(D))
    );
}

/// M7B-105 (D §5.1 "one CAS on `partitions/{id}`", ADR 0009 §5, ADR 0008): the proof that completes
/// the barrier emits one control effect, the CAS on the partition key conditioned on the fence's
/// revision, and the run never writes another key. The same step arms the timer that bounds the
/// exchange (issue #2, M7B-240).
#[retcd_test]
fn m7b_105_commit_is_one_cas_on_the_partition_record() {
    let mut f1 = at_barrier(20);
    for copy in [A, B] {
        f1.rec(3_000, durable(copy, 20, dg(0, 20)));
    }
    assert_eq!(
        f1.rec(3_001, durable(C, 20, dg(0, 20))),
        vec![cas(CONTROL_REV, A), arm(3, 3_001 + WINDOW)]
    );
    let keys: Vec<&ControlKey> = f1
        .log
        .iter()
        .filter_map(|effect| match effect {
            EffectKind::Control(
                ControlEffect::Cas { key, .. } | ControlEffect::Get { key, .. },
            ) => Some(key),
            _ => None,
        })
        .collect();
    assert_eq!(keys, [&ControlKey::Partition(PARTITION)]);
}

/// M7B-106 (D §5.1 `Committed` arm; landed `CasOutcome::Committed(revision)`): the run is
/// committed, the revision flows into the result, and `Recovered` is the step's one effect.
#[retcd_test]
fn m7b_106_cas_committed_enters_committed_with_mode() {
    let mut f1 = proposing(20);
    let effects = f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9))));
    assert_eq!(effects.len(), 1, "{effects:?}");
    let result = recovered(&effects);
    assert_eq!(result.committed.revision, Revision(9));
    assert_eq!(result.mode, PartitionMode::Active);
    assert_eq!(f1.phase(), RecoveryPhase::Committed);
}

/// A fence proved against the plan's anchor that read control at `revision`.
fn fence_read_at(revision: u64) -> RecoveryEvent {
    RecoveryEvent::FenceProven(Box::new(FencingProof {
        control_revision: Revision(revision),
        ..proof()
    }))
}

/// M7B-107 (D §5.1 `Conflict` arm, ADR 0009 §5): a conflict whose re-read shows another owner at a
/// newer epoch is overtaken. Blocked holds against every input but a fresh fence, which re-enters
/// by the M7B-84 door.
#[retcd_test]
fn m7b_107_cas_conflict_with_newer_owner_is_overtaken() {
    let peer = PartitionRecord {
        owner: NodeId(2),
        owner_epoch: OwnerEpoch(3),
        ..record(A)
    };
    let mut f1 = rereading();
    assert_eq!(
        f1.step(3_200, found(peer.encode())),
        vec![block(BlockReason::OvertakenByPeer)]
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Blocked(BlockReason::OvertakenByPeer)
    );
    assert_eq!(
        f1.report(3_300, inv(A, 20)),
        vec![ign(ReplicaIgnoreReason::RecoveryBlocked)]
    );
    assert!(has_query(&f1.rec(3_400, fence_read_at(7))));
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
}

/// M7B-109 and M7B-146 share this: `outcome` answers the recovery CAS; the run blocks for
/// `reason`, emits no CAS in that step and never re-proposes on anything short of a fresh fence.
fn control_blocks_without_retry(outcome: CasOutcome, reason: &BlockReason) {
    let mut f1 = proposing(20);
    assert_eq!(
        f1.step(3_100, cas_result(outcome)),
        vec![block(reason.clone())]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Blocked(reason.clone()));
    let refused = vec![ign(ReplicaIgnoreReason::RecoveryBlocked)];
    assert_eq!(f1.rec(3_200, durable(A, 20, dg(0, 20))), refused);
    assert_eq!(f1.step(3_300, fired(3)), refused);
    assert_eq!(f1.report(3_400, inv(B, 20)), refused);
    f1.ignores_late(3_500, cas_result(CasOutcome::Committed(Revision(9))));
    assert_eq!(f1.cas_count(), 1, "never re-proposed");
    assert_eq!(f1.phase(), RecoveryPhase::Blocked(reason.clone()));
    assert!(has_query(&f1.rec(3_600, fence_read_at(7))));
}

/// M7B-109 (D §5.1 `Unavailable` arm "never retry blind"; ADR 0009 §5): twin of M7B-146 by the
/// outcome arm.
#[retcd_test]
fn m7b_109_cas_unavailable_blocks_without_blind_retry() {
    control_blocks_without_retry(CasOutcome::Unavailable, &BlockReason::ControlUnavailable);
}

/// The type name of `value`'s type.
fn type_of<T>(_: &T) -> &'static str {
    std::any::type_name::<T>()
}

/// M7B-110 (D §5.6 mode table, BA-8): 3, 2 and 1 eligible regulars commit `Active`,
/// `DegradedRf2` and `ReadOnly`; none blocks with `NoEligibleRegular`, a reason an operator can
/// tell from divergence and from the control plane being down. `RecoveryResult.mode` is the shared
/// `PartitionMode`. The plan's other two carriers carry no mode on today's code: L1 is mode-blind
/// by ruling T-B-03, and A1's `PartitionRecord` has none.
#[retcd_test]
fn m7b_110_mode_is_derived_from_eligible_regular_count() {
    let runs: [(&[CopyId], &[CopyId], PartitionMode); 3] = [
        (&[A, B, C], &[], PartitionMode::Active),
        (&[A, B], &[C], PartitionMode::DegradedRf2),
        (&[A], &[B, C], PartitionMode::ReadOnly),
    ];
    for (survivors, failed, mode) in runs {
        let (mut f1, _) = closed(
            &[],
            survivors.iter().map(|copy| inv(*copy, 20)).collect(),
            failed,
        );
        for copy in survivors {
            f1.rec(3_000, durable(*copy, 20, dg(0, 20)));
        }
        let result = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
        assert_eq!(result.mode, mode, "{survivors:?}");
        assert_eq!(
            type_of(&result.mode),
            std::any::type_name::<PartitionMode>()
        );
    }
    let none = BlockReason::NoEligibleRegular;
    let (f1, close) = closed(&[], Vec::new(), &[A, B, C]);
    assert_eq!(close.last(), Some(&block(none.clone())));
    assert_eq!(f1.phase(), RecoveryPhase::Blocked(none.clone()));
    assert_eq!(
        mode_for(0),
        PartitionMode::Blocked {
            reason: none.clone()
        }
    );
    for other in [
        BlockReason::DivergenceRequiresOperator {
            diverged: Vec::new(),
        },
        BlockReason::ControlUnavailable,
        BlockReason::ControlUnknown,
    ] {
        assert_ne!(none, other);
    }
}

/// M7B-111 (charter "all three lone-survivor choices"; D §5.6; spec §8.4; gate V3): whichever copy
/// survives alone, it holds the prefix at its own head and the partition commits `ReadOnly`. The
/// old primary's head, 100, is known from its transfer advertisement, so a shorter lone survivor's
/// loss is uncertain. The plan's `recovery_mode` is T1's read-trace field, not an F1 output; F1's
/// half of it is `mode == ReadOnly` (the row's own scope note, T-B-03 / Q-B-2).
#[retcd_test]
fn m7b_111_all_three_lone_survivor_choices_are_read_only_until_the_barrier() {
    for (survivor, head) in [(A, 100), (B, 90), (C, 80)] {
        let mut f1 = fenced(&[]);
        f1.report(10, inv(survivor, head));
        for other in [A, B, C].into_iter().filter(|copy| *copy != survivor) {
            f1.rec(
                10,
                if other == A {
                    RecoveryEvent::TransferProgress {
                        copy: A,
                        advertised_seq: Seq(100),
                        received_seq: Seq(0),
                    }
                } else {
                    RecoveryEvent::InventoryFailed { copy: other }
                },
            );
        }
        let close = f1.step(WINDOW, fired(1));
        assert!(close.contains(&selected(head, survivor)), "{close:?}");
        f1.rec(3_000, durable(survivor, head, dg(0, head)));
        let result = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
        assert_eq!(result.mode, PartitionMode::ReadOnly, "{survivor:?}");
        assert_eq!(result.selected.source, survivor);
        assert_eq!(result.selected.cutoff_seq, Seq(head));
        assert_eq!(result.loss.highest_advertised_seq, Seq(100));
        assert_eq!(result.loss.uncertain, survivor != A, "{survivor:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// Plan rows, §8.4: stale owner, retention, result (M7B-113..119)
// ---------------------------------------------------------------------------------------------

/// M7B-114's close: A's 500 is the longest verified prefix, so A is the source and leads, and
/// B and C catch up to it.
fn a_selected_at_500() -> Vec<EffectKind> {
    vec![
        r(RecoveryEffect::CloseWindow),
        selected(500, A),
        catch_up(A, B, 500),
        catch_up(A, C, 500),
        arm(2, 2 * WINDOW),
    ]
}

/// M7B-114's `Collecting` trace: B reports, then A's head-500 report arrives as `a_arrives`, then
/// C reports, and the run goes on to commit. Every step's effects and phase are asserted; A's
/// report must be recorded, selected, and listed `Verified` in the result.
fn a_arrives_in_collecting(a_arrives: RecoveryEvent) -> F1 {
    let recorded = vec![ign(ReplicaIgnoreReason::Recorded)];
    let mut f1 = fenced(&[]);
    assert_eq!(f1.report(10, inv(B, 20)), recorded);
    assert_eq!(f1.phase(), RecoveryPhase::Collecting);
    assert_eq!(f1.rec(11, a_arrives), recorded, "A arrives in Collecting");
    assert_eq!(f1.phase(), RecoveryPhase::Collecting);
    assert_eq!(f1.report(12, inv(C, 20)), recorded);
    assert_eq!(f1.step(WINDOW, fired(1)), a_selected_at_500());
    assert_eq!(f1.phase(), RecoveryPhase::Synchronizing);
    let caught = |copy| RecoveryEvent::CopyCaughtUp {
        copy,
        head: Seq(500),
        digest: dg(0, 500),
    };
    assert_eq!(f1.rec(2_100, caught(B)), recorded);
    assert_eq!(
        f1.rec(2_200, caught(C)),
        vec![sync(A, 500), sync(B, 500), sync(C, 500)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Barrier);
    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];
    assert_eq!(f1.rec(3_000, durable(A, 500, dg(0, 500))), not_durable);
    assert_eq!(f1.rec(3_001, durable(B, 500, dg(0, 500))), not_durable);
    assert_eq!(
        f1.rec(3_002, durable(C, 500, dg(0, 500))),
        vec![cas(CONTROL_REV, A), arm(3, 3_002 + WINDOW)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Proposing);
    let effects = f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9))));
    assert_eq!(effects.len(), 1, "{effects:?}");
    let result = recovered(&effects);
    assert_eq!(
        result.inventories,
        [A, B, C].map(|copy| InventoryOutcome::Verified { copy })
    );
    assert_eq!(result.selected.source, A);
    assert_eq!(result.selected.cutoff_seq, Seq(500));
    assert_eq!(f1.phase(), RecoveryPhase::Committed);
    f1
}

/// M7B-114 (D §5.7 "the discriminator is the phase", ADR 0009 §7): before commit, a returning
/// owner is one more survivor. The plan's fixture delivers M7B-113's input (A, head 500) in
/// `Collecting`; this row delivers it there and in `Fenced`, the other pre-selection phase.
/// Fenced: recorded, then selected at the close. Collecting: the whole run to commit, where A is
/// recorded, selected, and `Verified` in the result (verified and eligible), and every step is
/// identical to the same report arriving as `InventoryReported`. Twin of M7B-113 by phase only.
#[retcd_test]
fn m7b_114_same_node_before_commit_is_an_ordinary_survivor() {
    let returning = inv(A, 500);
    assert!(verify_ancestry(&anchor(), &returning).is_ok());
    let recorded = vec![ign(ReplicaIgnoreReason::Recorded)];

    let mut f1 = fenced(&[]);
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
    assert_eq!(
        f1.rec(
            10,
            RecoveryEvent::StaleOwnerReturned(Box::new(returning.clone()))
        ),
        recorded,
        "A arrives in Fenced"
    );
    assert_eq!(f1.phase(), RecoveryPhase::Collecting);
    assert_eq!(f1.report(11, inv(B, 20)), recorded);
    assert_eq!(f1.report(12, inv(C, 20)), recorded);
    assert_eq!(f1.step(WINDOW, fired(1)), a_selected_at_500());

    let returned = a_arrives_in_collecting(RecoveryEvent::StaleOwnerReturned(Box::new(
        returning.clone(),
    )));
    let reported = a_arrives_in_collecting(RecoveryEvent::InventoryReported(Box::new(returning)));
    assert_eq!(
        returned.log, reported.log,
        "an ordinary survivor, step for step"
    );
}

/// The identifiers of the variants of every `pub enum *Effect*` declared at the top level of
/// `source`: a line indented four spaces, starting upper-case, inside the enum's braces.
fn effect_variants(source: &str) -> Vec<&str> {
    let mut variants = Vec::new();
    let mut inside = false;
    for line in source.lines() {
        if line.starts_with("pub enum ") && line.contains("Effect") {
            inside = true;
        } else if line == "}" {
            inside = false;
        } else if let Some(body) = line.strip_prefix("    ").filter(|_| inside) {
            if body.starts_with(|c: char| c.is_ascii_uppercase()) {
                variants.extend(body.split(|c: char| !c.is_alphanumeric()).next());
            }
        }
    }
    variants
}

/// M7B-115 (D §5.7 `ev.tick` (K-B-25), "no deletion effect exists in M7"; spec §8.4): the
/// retention window runs from the event's own tick, not the step's, and no effect can delete.
/// The Q-52 grep, in the test: F1's source never says `Delete`, and no `*Effect*` enum in the
/// contracts has a `Delete*` variant (`txn::Mutation::Delete` is a client write, not an effect).
#[retcd_test]
fn m7b_115_retain_suffix_uses_the_event_tick_and_no_delete_exists() {
    for at in [100, 200] {
        let mut f1 = proposing(20);
        f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9))));
        let returned = RecoveryEvent::StaleOwnerReturned(Box::new(inv(A, 99)));
        let effects = f1.at(
            50_000,
            at,
            EventKind::Kernel(KernelEvent::Recovery(returned)),
        );
        assert_eq!(
            effects[0],
            r(RecoveryEffect::QuarantineSuffix {
                copy: A,
                from: Seq(21),
                until: Tick(at + RETENTION),
            })
        );
    }
    let f1_source = [
        include_str!("../src/recovery.rs"),
        include_str!("../src/recovery/commit.rs"),
        include_str!("../src/recovery/emit.rs"),
        include_str!("../src/recovery/inventory.rs"),
        include_str!("../src/recovery/lineage.rs"),
        include_str!("../src/recovery/rebuild.rs"),
    ];
    assert!(f1_source.iter().all(|source| !source.contains("Delete")));
    let variants: Vec<&str> = [
        include_str!("../src/contracts/authority.rs"),
        include_str!("../src/contracts/control.rs"),
        include_str!("../src/contracts/event.rs"),
        include_str!("../src/contracts/recovery.rs"),
        include_str!("../src/contracts/storage.rs"),
        include_str!("../src/contracts/time.rs"),
        include_str!("../src/contracts/transport.rs"),
    ]
    .into_iter()
    .flat_map(effect_variants)
    .collect();
    for known in ["QuarantineSuffix", "Recovered", "Cas", "Arm"] {
        assert!(variants.contains(&known), "the scan misses {known}");
    }
    let deleting: Vec<&&str> = variants
        .iter()
        .filter(|v| v.starts_with("Delete"))
        .collect();
    assert!(deleting.is_empty(), "{deleting:?}");
}

/// M7B-117's trace (the plan's "after M7B-106"): A and B report head 20, C sends `c_sends`, the
/// window closes on the cutoff 20 with C unavailable, A and B prove it, and the CAS lands at 9.
/// `c_answer` is C's step and `close` the window's; every other step's effects and phase are fixed
/// here. Returns the `Recovered` result, the commit step's only effect.
fn committed_without_c(
    c_sends: RecoveryEvent,
    c_answer: Vec<EffectKind>,
    close: Vec<EffectKind>,
) -> RecoveryResult {
    let recorded = vec![ign(ReplicaIgnoreReason::Recorded)];
    let mut f1 = fenced(&[]);
    assert_eq!(f1.report(10, inv(A, 20)), recorded);
    assert_eq!(f1.report(10, inv(B, 20)), recorded);
    assert_eq!(f1.rec(20, c_sends), c_answer);
    assert_eq!(f1.phase(), RecoveryPhase::Collecting);
    assert_eq!(f1.step(WINDOW, fired(1)), close);
    assert_eq!(f1.phase(), RecoveryPhase::Barrier);
    assert_eq!(
        f1.rec(3_000, durable(A, 20, dg(0, 20))),
        vec![ign(ReplicaIgnoreReason::BarrierNotDurable)]
    );
    assert_eq!(
        f1.rec(3_001, durable(B, 20, dg(0, 20))),
        vec![cas(CONTROL_REV, A), arm(3, 3_001 + WINDOW)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Proposing);
    let effects = f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9))));
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    recovered(&effects)
}

/// M7B-117 (D §5.8 `RecoveryResult`, ADR 0009 §7, K-B-19): the result carries the bounds, the mode
/// and the status map, with the status map's uncertainty equal to the loss's, on both sides.
/// Three runs lose C before the cutoff at 20. C streaming a prefix it advertised at 30 leaves the
/// loss uncertain, and the status map discards from 21. Its twin differs by one fact, C
/// advertising 20, the cutoff itself: the loss is certain and nothing is discarded. The third is
/// the tester's `probe_ec_117`: C fails outright, advertising nothing, and the loss is certain.
/// Every struct is destructured without `..`, so a field added to any of them (a client-ACK field
/// included) stops this row compiling until it is asserted here.
#[retcd_test]
fn m7b_117_recovery_result_carries_bounds_mode_and_status_map() {
    let streaming = |advertised: u64| RecoveryEvent::TransferProgress {
        copy: C,
        advertised_seq: Seq(advertised),
        received_seq: Seq(0),
    };
    // Selection at the cutoff 20 from A; A and B sync; the wait after selection is bounded.
    let select_20 = vec![
        r(RecoveryEffect::CloseWindow),
        selected(20, A),
        sync(A, 20),
        sync(B, 20),
        arm(2, 2 * WINDOW),
    ];
    let c_lost = lost(C, UnavailableReason::Stalled);
    // A streaming C is recorded, and the close finds it stalled before it closes.
    let c_stalls = |advertised| {
        committed_without_c(
            streaming(advertised),
            vec![ign(ReplicaIgnoreReason::Recorded)],
            [vec![c_lost.clone()], select_20.clone()].concat(),
        )
    };
    let uncertain_loss = c_stalls(30);
    let certain_twin = c_stalls(20);
    let c_failed = committed_without_c(
        RecoveryEvent::InventoryFailed { copy: C },
        vec![c_lost],
        select_20,
    );
    for (result, highest, expect_uncertain) in [
        (uncertain_loss, 30, true),
        (certain_twin, 20, false),
        (c_failed, 20, false),
    ] {
        result_carries_bounds_mode_and_status_map(result, Seq(highest), expect_uncertain);
    }
}

/// M7B-117's clause, field by field, for one run of [`committed_without_c`]: C was lost, the
/// highest advertised head was `highest`, and the loss is `expect_uncertain`.
fn result_carries_bounds_mode_and_status_map(
    result: RecoveryResult,
    highest: Seq,
    expect_uncertain: bool,
) {
    let RecoveryResult {
        fenced_prior,
        inventories,
        selected,
        new_generation,
        mode,
        barrier,
        loss,
        committed,
        retained_status_map,
    } = result;
    assert_eq!(fenced_prior, proof());
    assert_eq!(
        inventories,
        vec![
            InventoryOutcome::Verified { copy: A },
            InventoryOutcome::Verified { copy: B },
            InventoryOutcome::Failed {
                copy: C,
                reason: UnavailableReason::Stalled
            },
        ]
    );
    assert_eq!(new_generation, Generation(8));
    assert_eq!(new_generation, selected.root.generation);
    assert_eq!(mode, PartitionMode::DegradedRf2);
    assert_eq!(
        (barrier.cutoff(), barrier.required()),
        (Seq(20), &[A, B][..])
    );
    let LossRecord {
        queried,
        unavailable,
        cutoff_seq,
        highest_advertised_seq,
        uncertain,
    } = loss;
    assert_eq!(queried, [A, B, C]);
    assert_eq!(unavailable, [(C, UnavailableReason::Stalled)]);
    assert_eq!((cutoff_seq, highest_advertised_seq), (Seq(20), highest));
    assert_eq!(uncertain, expect_uncertain, "loss.uncertain");
    let RetainedStatusMap {
        predecessor_generation,
        predecessor_cutoff,
        retained_through,
        discarded_from,
        uncertain: status_uncertain,
    } = retained_status_map;
    assert_eq!(predecessor_generation, PRIOR_GEN);
    assert_eq!((predecessor_cutoff, retained_through), (Seq(20), Seq(20)));
    assert_eq!(
        discarded_from,
        expect_uncertain.then_some(Seq(21)),
        "discarded_from"
    );
    assert_eq!(status_uncertain, uncertain, "status map uncertain == loss");
    let CommittedRoot {
        revision,
        pinned_config,
        authority_view,
    } = committed;
    assert_eq!(revision, Revision(9));
    assert_eq!(pinned_config, plan(&[]).config);
    assert_eq!(authority_view.lineage, root());
}

/// M7B-118 (D §5.2 shadows never recover, §5.4 `select_leader`, ADR 0009 §5): shadow D holds the
/// longest verified prefix and every candidate flag is set. The prefix is D's; the leader is a
/// regular, and D sends the prefix to it before its grant.
#[retcd_test]
fn m7b_118_verified_shadow_source_is_never_leader() {
    let shadow = [member(D, ReplicaRole::Shadow)];
    let reports = vec![inv(A, 18), inv(B, 18), inv(C, 18), inv(D, 25)];
    let SelectionOutcome::Selected(chosen) = select_prefix(&verified(&reports), root()) else {
        panic!("four compatible histories select");
    };
    assert_eq!(chosen.source, D);
    let candidates = plan(&shadow).candidates;
    assert!(
        candidates.contains(&candidate(D)),
        "D is viable by every flag"
    );
    assert_eq!(
        select_leader(&chosen, &candidates, &BTreeSet::from([A, B, C])),
        Some(A)
    );
    let (_, close) = closed(&shadow, reports, &[]);
    assert!(
        close.contains(&r(RecoveryEffect::CatchUpBeforeGrant {
            from: D,
            to: A,
            through: Seq(25),
            credential: proof().credential_for(D),
        })),
        "{close:?}"
    );
}

/// M7B-119 (B-R6/B-R17, D §5.1, ADR 0009 §4): `Quarantined` answers every F1 input
/// `QuarantinedTerminal`, a fresh fence included, and never leaves. A control answer F1 did not
/// request is not an F1 input, so it is declined, as in every phase.
#[retcd_test]
fn m7b_119_quarantined_phase_is_terminal_in_m7() {
    let (mut f1, _) = closed(&[], vec![inv_on(A, 20, 1, 12), inv_on(B, 15, 2, 12)], &[C]);
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
    let recovery_inputs = [
        RecoveryEvent::Plan(Box::new(plan(&[]))),
        fence_read_at(CONTROL_REV.0),
        fence_read_at(7),
        RecoveryEvent::InventoryReported(Box::new(inv(C, 20))),
        RecoveryEvent::InventoryFailed { copy: C },
        transfer(C, 5),
        RecoveryEvent::ProbeAnswered {
            copy: A,
            seq: Seq(15),
            digest: dg(0, 15),
        },
        RecoveryEvent::ProbeUnavailable {
            copy: A,
            seq: Seq(15),
        },
        caught_up(B, 20, dg(0, 20)),
        durable(A, 20, dg(0, 20)),
        RecoveryEvent::StaleOwnerReturned(Box::new(inv(A, 20))),
    ];
    let inputs = recovery_inputs
        .into_iter()
        .map(|input| EventKind::Kernel(KernelEvent::Recovery(input)))
        .chain([lose(C), fired(1), fired(2)]);
    for (tick, input) in (4_000..).zip(inputs) {
        assert_eq!(
            f1.step(tick, input.clone()),
            vec![ign(ReplicaIgnoreReason::QuarantinedTerminal)],
            "{input:?}"
        );
        assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
    }
    f1.declines(5_000, cas_result(CasOutcome::Committed(Revision(9))));
}

// ---------------------------------------------------------------------------------------------
// Plan rows, §9: rebuild and activation (M7B-126..128, 146, 148)
// ---------------------------------------------------------------------------------------------

/// [`lone_committed`] rebuilt: B caught up, all three proofs in, the activation CAS in flight.
/// Also returns the `ReadOnly` result the recovery commit emitted.
fn activating() -> (F1, RecoveryResult) {
    let (mut f1, read_only) = lone_committed_with_result();
    f1.rec(4_000, caught_up(B, 20, dg(0, 20)));
    for copy in [A, B, C] {
        f1.rec(4_100, durable(copy, 20, dg(0, 20)));
    }
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    (f1, read_only)
}

fn is_recovered(effect: &EffectKind) -> bool {
    matches!(effect, EffectKind::Kernel(KernelEffect::Recovered(_)))
}

/// M7B-126 (D §5.6a, ADR 0009 §7/§8, spec §8.3/§8.4): from `ReadOnly`, each catch-up syncs its copy
/// through the rebuild point; two proofs of three are not a barrier; a proof bound to another
/// digest is not one either; the third proof proposes one CAS conditioned on the recovery commit's
/// revision, and only its `Committed` activates. A `Conflict` re-reads and never activates over
/// the decision it finds. The twin's proof sits past the point: at the point itself a foreign
/// digest is divergence (ruling A-4), so only `try_new`'s binding check can refuse it there.
#[retcd_test]
fn m7b_126_rebuilding_reaches_activation_only_through_try_new() {
    let mut f1 = lone_committed();
    assert!(f1
        .rec(4_000, caught_up(B, 20, dg(0, 20)))
        .contains(&sync(B, 20)));
    assert_eq!(
        f1.rec(4_001, caught_up(C, 20, dg(0, 20))),
        vec![sync(C, 20), arm(5, 4_001 + WINDOW)]
    );
    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];
    let required = BTreeSet::from([A, B, C]);
    let two = [proven(A, 20, dg(0, 20)), proven(B, 20, dg(0, 20))];
    assert_eq!(
        RecoveryBarrier::try_new(&two, &required, Seq(20), dg(0, 20)),
        Err(MissingProof::NoProofFrom(C))
    );
    assert_eq!(f1.rec(4_100, durable(A, 20, dg(0, 20))), not_durable);
    assert_eq!(f1.rec(4_101, durable(B, 20, dg(0, 20))), not_durable);
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    let mis_bound = proven(C, 21, dg(0, 21));
    assert_eq!(
        RecoveryBarrier::try_new(&[two[0], two[1], mis_bound], &required, Seq(20), dg(0, 20)),
        Err(MissingProof::ProofDigestMismatch {
            copy: C,
            proof_digest: dg(0, 21),
            cutoff_digest: dg(0, 20),
        })
    );
    assert_eq!(
        f1.rec(4_102, RecoveryEvent::DurableAt(mis_bound)),
        not_durable
    );
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    assert_eq!(
        f1.rec(4_103, durable(C, 20, dg(0, 20))),
        vec![cas(Revision(9), A), arm(6, 4_103 + WINDOW)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    let activated = recovered(&f1.step(4_200, cas_result(CasOutcome::Committed(Revision(11)))));
    assert_eq!(activated.mode, PartitionMode::Active);
    assert_eq!(f1.phase(), RecoveryPhase::Committed);

    let (mut f1, _) = activating();
    let conflict = CasOutcome::Conflict {
        exists: true,
        current: Revision(12),
    };
    assert_eq!(
        f1.step(4_200, cas_result(conflict)),
        vec![EffectKind::Control(ControlEffect::Get {
            request: LATEST,
            key: ControlKey::Partition(PARTITION)
        })]
    );
    let peer = PartitionRecord {
        owner: NodeId(2),
        owner_epoch: OwnerEpoch(3),
        ..record(A)
    };
    let read = read_result(ReadOutcome::Found {
        revision: Revision(12),
        value: peer.encode(),
    });
    assert_eq!(
        f1.step(4_300, read),
        vec![block(BlockReason::OvertakenByPeer)]
    );
    assert_eq!(f1.log.iter().filter(|e| is_recovered(e)).count(), 1);
}

/// M7B-127 (D §5.6a `DegradedRf2` required = the third copy and both holders; spec §8.3): a
/// `DegradedRf2` commit stays below `Active` through any number of ticks, and leaves only through
/// the three-copy barrier's `ActivationProposed -> Committed`. `HealthEval` is not a landed event;
/// the ticks arrive as F1's own timer.
#[retcd_test]
fn m7b_127_degraded_rf2_leaves_only_on_the_rebuild_barrier() {
    let (mut f1, _) = closed(&[], vec![inv(A, 20), inv(B, 20)], &[C]);
    for copy in [A, B] {
        f1.rec(3_000, durable(copy, 20, dg(0, 20)));
    }
    let degraded = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
    assert_eq!(degraded.mode, PartitionMode::DegradedRf2);
    assert_eq!(
        f1.module.rebuild_required(),
        Some(&BTreeSet::from([A, B, C]))
    );
    for tick in 1..=50 {
        assert_eq!(
            f1.step(3_100 + tick * 1_000, fired(tick)),
            vec![ign(ReplicaIgnoreReason::StaleTimer)]
        );
        assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    }
    f1.ignores_late(60_000, cas_result(CasOutcome::Committed(Revision(10))));
    f1.rec(60_100, caught_up(C, 20, dg(0, 20)));
    for copy in [A, B, C] {
        f1.rec(60_200, durable(copy, 20, dg(0, 20)));
    }
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    assert_eq!(f1.log.iter().filter(|e| is_recovered(e)).count(), 1);
    let active = recovered(&f1.step(60_300, cas_result(CasOutcome::Committed(Revision(11)))));
    assert_eq!(active.mode, PartitionMode::Active);
}

/// M7B-128 (D §5.6a `CopyLost` arm, "never shrinks `required`"; ADR 0009 §7; K-B-43): losing B
/// after its proof stalls the rebuild once and loudly, keeps B required, forgets B's proof and
/// refuses it re-sent, so no later proof activates. The plan's `Alert{RebuildStalled}` is the
/// landed `RecoveryEffect::RebuildStalled`. Twin: losing D, which is not required, is
/// `Ignored{NotRequired}`, never an empty vector (BA-2).
#[retcd_test]
fn m7b_128_copy_lost_during_rebuilding_stalls_loudly_and_never_shrinks_required() {
    let stalled = |f1: &F1| {
        f1.log
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    EffectKind::Kernel(KernelEffect::Recovery(
                        RecoveryEffect::RebuildStalled { .. }
                    ))
                )
            })
            .count()
    };
    let everyone = BTreeSet::from([A, B, C]);
    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 20, dg(0, 20)));
    f1.rec(4_100, durable(A, 20, dg(0, 20)));
    f1.rec(4_101, durable(B, 20, dg(0, 20)));
    assert_eq!(
        f1.step(4_200, lose(B)),
        vec![r(RecoveryEffect::RebuildStalled { copy: B })]
    );
    assert_eq!(f1.module.rebuild_required(), Some(&everyone));
    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];
    assert_eq!(f1.rec(4_300, durable(C, 20, dg(0, 20))), not_durable);
    assert_eq!(f1.rec(4_301, durable(B, 20, dg(0, 20))), not_durable);
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    assert_eq!((f1.cas_count(), stalled(&f1)), (1, 1));

    let mut f1 = lone_committed();
    assert_eq!(
        f1.step(4_200, lose(D)),
        vec![ign(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(f1.module.rebuild_required(), Some(&everyone));
    assert_eq!(stalled(&f1), 0);
}

/// M7B-146 (D §5.1 `Unknown` arm: the CAS is more likely to have landed, so a blind retry is
/// worse; ADR 0009 §5): `ControlUnknown`, by value distinct from `ControlUnavailable`, with no CAS
/// and no re-proposal short of a fresh fence. Twin of M7B-109 by the outcome arm. The activation
/// CAS answers `Unknown` the same way: no activation over a CAS that may have landed.
#[retcd_test]
fn m7b_146_cas_unknown_blocks_and_is_the_worse_case_to_retry() {
    assert_ne!(BlockReason::ControlUnknown, BlockReason::ControlUnavailable);
    control_blocks_without_retry(CasOutcome::Unknown, &BlockReason::ControlUnknown);
    let (mut f1, _) = activating();
    assert_eq!(
        f1.step(4_200, cas_result(CasOutcome::Unknown)),
        vec![block(BlockReason::ControlUnknown)]
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Blocked(BlockReason::ControlUnknown)
    );
    assert_eq!(
        f1.rec(4_300, durable(C, 20, dg(0, 20))),
        vec![ign(ReplicaIgnoreReason::RecoveryBlocked)]
    );
    assert_eq!(f1.cas_count(), 2, "the recovery CAS and one activation CAS");
    assert_eq!(f1.log.iter().filter(|e| is_recovered(e)).count(), 1);
}

/// M7B-148 (D §5.1 `ActivationProposed --CasResult--> Committed{Active}`, "re-emit `Recovered`";
/// ADR 0009; T-B-03 / Q-B-2): the activation's `Committed(revision)` step re-emits `Recovered`
/// with `mode: Active` and that revision. It differs from the `ReadOnly` result in exactly
/// `mode`, `control_revision` and `barrier`. F1 never emits `SetAdmission`: admission is L1's.
#[retcd_test]
fn m7b_148_activation_commit_re_emits_recovered_with_mode_active() {
    let (mut f1, read_only) = activating();
    let effects = f1.step(4_200, cas_result(CasOutcome::Committed(Revision(11))));
    assert_eq!(effects.len(), 1, "{effects:?}");
    let active = recovered(&effects);
    assert_eq!(active.mode, PartitionMode::Active);
    assert_eq!(active.committed.revision, Revision(11));
    assert_ne!(active.barrier, read_only.barrier);
    assert_eq!(active.barrier.required(), [A, B, C]);
    let mut only_those_three = active;
    only_those_three.mode = read_only.mode.clone();
    only_those_three.committed.revision = read_only.committed.revision;
    only_those_three.barrier = read_only.barrier.clone();
    assert_eq!(only_those_three, read_only);
    assert!(!f1
        .log
        .iter()
        .any(|e| matches!(e, EffectKind::Kernel(KernelEffect::SetAdmission(_)))));
}

// ---------------------------------------------------------------------------------------------
// Plan rows landed by B-R49: M7B-108 as re-worded, and M7B-97 and 113 on F1's selection spy
// ---------------------------------------------------------------------------------------------

/// M7B-108 (D §5.1 `Conflict` row; ADR 0009 §5; rulings F-f, F-g, B-R49): a conflict whose re-read
/// finds an older or equal epoch with other content is `CasContention` at once, and F1 never
/// proposes again. There is one CAS, and none after it on any later input, a repeated conflict
/// and re-read included, until a fresh fence. Twin of M7B-107 by the re-read result. The
/// activation CAS answers the same way: one activation CAS, and no second `Recovered`.
#[retcd_test]
fn m7b_108_cas_conflict_unchanged_record_never_reproposes() {
    let conflict = |current| {
        cas_result(CasOutcome::Conflict {
            exists: true,
            current: Revision(current),
        })
    };
    let other_content = |owner_epoch| PartitionRecord {
        owner: NodeId(9),
        owner_epoch,
        ..record(A)
    };
    let contention = RecoveryPhase::Blocked(BlockReason::CasContention);
    for epoch in [OwnerEpoch(0), PRIOR_EPOCH] {
        let current = other_content(epoch);
        let mut f1 = rereading();
        assert_eq!(
            f1.step(3_200, found(current.encode())),
            vec![block(BlockReason::CasContention)],
            "{current:?}"
        );
        assert_eq!(f1.phase(), contention);
        f1.ignores_late(3_300, conflict(6));
        f1.ignores_late(3_301, found(current.encode()));
        assert_eq!(
            f1.report(3_400, inv(A, 20)),
            vec![ign(ReplicaIgnoreReason::RecoveryBlocked)]
        );
        assert_eq!(f1.cas_count(), 1, "never re-proposed: {current:?}");
        assert!(has_query(&f1.rec(3_500, fence_read_at(7))));
        assert_eq!(f1.cas_count(), 1);
    }

    for epoch in [OwnerEpoch(1), OwnerEpoch(2)] {
        let current = other_content(epoch);
        let (mut f1, _) = activating();
        f1.step(4_200, conflict(12));
        assert_eq!(
            f1.step(4_300, found(current.encode())),
            vec![block(BlockReason::CasContention)],
            "{current:?}"
        );
        assert_eq!(f1.phase(), contention);
        f1.ignores_late(4_400, conflict(12));
        assert_eq!(
            f1.rec(4_500, durable(C, 20, dg(0, 20))),
            vec![ign(ReplicaIgnoreReason::RecoveryBlocked)]
        );
        assert_eq!(
            f1.cas_count(),
            2,
            "the recovery CAS and one activation CAS: {current:?}"
        );
        assert_eq!(f1.log.iter().filter(|e| is_recovered(e)).count(), 1);
    }
}

/// M7B-97 (charter "divergent digest at the same position quarantines and blocks promotion";
/// D §5.3/§5.4; ADR 0009 §4; charter DO-NOT "no transaction-wise union"): B and C both head 50,
/// equal below 30 and different from 30 up. Their equal length is never consulted: selection
/// returns the pair's divergence with no length read on the spy, where the compatible twin reads
/// length exactly once. The run quarantines, never selects, never proposes, and stays there.
#[retcd_test]
fn m7b_97_divergent_digest_at_the_same_position_quarantines_and_blocks_promotion() {
    let forked = vec![inv(B, 50), inv_on(C, 50, 1, 29)];
    let evidence = DivergenceEvidence::Pairwise {
        seq: Seq(50),
        a: (B, dg(0, 50)),
        b: (C, dg(1, 50)),
    };
    let mut spy = SelectionSpy::new();
    assert_eq!(
        select_prefix_spied(&verified(&forked), root(), &mut spy),
        SelectionOutcome::Divergence(evidence)
    );
    assert_eq!(spy.length_reads(), 0, "length never consulted");
    let mut twin = SelectionSpy::new();
    let compatible = verified(&[inv(B, 50), inv(C, 50)]);
    assert!(matches!(
        select_prefix_spied(&compatible, root(), &mut twin),
        SelectionOutcome::Selected(_)
    ));
    assert_eq!(twin.length_reads(), 1, "the spy sees the one length read");

    let (mut f1, close) = closed(&[], forked, &[A]);
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            r(RecoveryEffect::Quarantine(evidence)),
            block(BlockReason::DivergenceRequiresOperator {
                diverged: vec![B, C]
            }),
        ]
    );
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
    assert_eq!(
        (f1.module.spy().selections(), f1.module.spy().length_reads()),
        (1, 0)
    );
    for copy in [B, C] {
        f1.rec(3_000, durable(copy, 50, dg(0, 50)));
    }
    f1.declines(3_100, cas_result(CasOutcome::Committed(Revision(9))));
    assert!(!has_selected(&f1.log), "{:?}", f1.log);
    assert_eq!(f1.cas_count(), 0, "promotion stays blocked");
    assert_eq!(f1.phase(), RecoveryPhase::Quarantined);
}

/// M7B-113 (D §5.7; spec §8.1 "never overrides a newer committed root, even with a longer
/// suffix"; ADR 0009 §7): after commit, a returning owner with a far longer head (500 over a
/// cutoff of 20) is quarantined from the predecessor cutoff and rebuilt from the committed root,
/// and selection never runs for it: the spy holds the run's one selection before and after. The
/// phase does not move.
#[retcd_test]
fn m7b_113_stale_owner_after_commit_is_quarantined_without_length_comparison() {
    let mut f1 = proposing(20);
    let result = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
    let before = f1.module.spy();
    assert_eq!(
        (before.selections(), before.length_reads()),
        (1, 1),
        "the run's one selection"
    );
    let phase = f1.phase();
    let returned = RecoveryEvent::StaleOwnerReturned(Box::new(inv(A, 500)));
    assert_eq!(
        f1.at(
            9_000,
            9_000,
            EventKind::Kernel(KernelEvent::Recovery(returned))
        ),
        vec![
            r(RecoveryEffect::QuarantineSuffix {
                copy: A,
                from: result.retained_status_map.predecessor_cutoff.next(),
                until: Tick(9_000 + RETENTION),
            }),
            r(RecoveryEffect::RebuildFromAuthoritative {
                copy: A,
                root: LineageAnchor {
                    lineage: result.selected.root,
                    base_seq: Seq(20),
                    base_digest: dg(0, 20),
                },
            }),
        ]
    );
    assert_eq!(f1.module.spy(), before, "select_prefix not called");
    assert_eq!(f1.phase(), phase);
}

// ---------------------------------------------------------------------------------------------
// B-R52 rows (plan §9): a rebuild sync is bounded by F1's timer (design §5.6a, "A sync is
// bounded")
// ---------------------------------------------------------------------------------------------

fn is_stalled(effect: &EffectKind) -> bool {
    matches!(
        effect,
        EffectKind::Kernel(KernelEffect::Recovery(
            RecoveryEffect::RebuildStalled { .. }
        ))
    )
}

fn stalled(copy: CopyId) -> EffectKind {
    r(RecoveryEffect::RebuildStalled { copy })
}

/// The sync deadline: C's catch-up at 4_000 re-arms F1's one timer as version 4 (the fence armed
/// 1, selection 2, the recovery CAS 3) at 4_000 plus the discovery window.
const SYNC_DEADLINE: u64 = 4_000 + WINDOW;

/// M7B-153/154's fixture: a `DegradedRf2` commit (A and B hold 20, C failed) in `Rebuilding`.
/// C catches up to 20, which pins the point and syncs every required copy under a fresh timer;
/// A and B prove the point, C does not. Each step's effects are asserted.
fn rebuilding_while_c_syncs() -> F1 {
    let (mut f1, _) = closed(&[], vec![inv(A, 20), inv(B, 20)], &[C]);
    for copy in [A, B] {
        f1.rec(3_000, durable(copy, 20, dg(0, 20)));
    }
    let degraded = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
    assert_eq!(degraded.mode, PartitionMode::DegradedRf2);
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    assert_eq!(
        f1.rec(4_000, caught_up(C, 20, dg(0, 20))),
        vec![sync(A, 20), sync(B, 20), sync(C, 20), arm(4, SYNC_DEADLINE)]
    );
    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];
    assert_eq!(f1.rec(4_100, durable(A, 20, dg(0, 20))), not_durable);
    assert_eq!(f1.rec(4_101, durable(B, 20, dg(0, 20))), not_durable);
    f1
}

/// M7B-153 (D §5.6a "A sync is bounded", K-B-43 no silent stall; spec §6 "error or partial
/// completion advances nothing"; ruling B-R52): C's sync is never answered. At the deadline F1
/// names C, and only C, stays `Rebuilding` with `required` whole and the mode not moved; the
/// deadline is then spent. A later `CopyCaughtUp{C}` re-syncs and re-arms. Two more traces: in
/// `ReadOnly` both unproven copies are named, in copy order, and never the proven one; and a copy
/// already reported lost is not named twice (ruling B-R52a, M7B-128). Last, per §5.6a ("a sync
/// that misses its deadline is a report, not a loss: … a late `DurableAt` from it is judged like
/// any other proof and, if it completes the barrier, proposes activation"), C's late proof
/// proposes activation.
#[retcd_test]
fn m7b_153_a_rebuild_sync_that_never_answers_stalls_by_name() {
    let everyone = BTreeSet::from([A, B, C]);
    let stale = vec![ign(ReplicaIgnoreReason::StaleTimer)];
    let mut f1 = rebuilding_while_c_syncs();
    assert_eq!(f1.step(SYNC_DEADLINE - 1, fired(4)), stale, "not yet due");
    assert_eq!(f1.step(SYNC_DEADLINE, fired(3)), stale, "an older version");
    assert_eq!(f1.step(SYNC_DEADLINE, fired(4)), vec![stalled(C)]);
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    assert_eq!(f1.module.rebuild_required(), Some(&everyone));
    assert_eq!(
        f1.log.iter().filter(|e| is_recovered(e)).count(),
        1,
        "mode not moved"
    );
    assert_eq!(
        f1.step(SYNC_DEADLINE + 1, fired(4)),
        stale,
        "the deadline is spent"
    );

    // A later catch-up re-syncs C under a fresh deadline, which stalls again unanswered.
    assert_eq!(
        f1.rec(7_000, caught_up(C, 20, dg(0, 20))),
        vec![sync(C, 20), arm(5, 7_000 + WINDOW)]
    );
    assert_eq!(f1.step(7_000 + WINDOW, fired(5)), vec![stalled(C)]);
    assert_eq!(f1.module.rebuild_required(), Some(&everyone));

    // §5.6a: the stall is a report, not a loss. C's late proof completes the barrier.
    assert_eq!(
        f1.rec(9_500, durable(C, 20, dg(0, 20))),
        vec![cas(Revision(9), A), arm(6, 9_500 + WINDOW)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    assert_eq!(f1.log.iter().filter(|e| is_stalled(e)).count(), 2);

    // ReadOnly: A proved, B and C did not. Each unproven copy is named once, in copy order.
    let mut f1 = lone_committed();
    assert_eq!(
        f1.rec(4_000, caught_up(B, 20, dg(0, 20))),
        vec![sync(A, 20), sync(B, 20), sync(C, 20), arm(4, SYNC_DEADLINE)]
    );
    f1.rec(4_100, durable(A, 20, dg(0, 20)));
    assert_eq!(
        f1.step(SYNC_DEADLINE, fired(4)),
        vec![stalled(B), stalled(C)]
    );
    assert_eq!(f1.module.rebuild_required(), Some(&everyone));

    // A copy already reported lost is not named again by the timer.
    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 20, dg(0, 20)));
    f1.rec(4_100, durable(A, 20, dg(0, 20)));
    assert_eq!(f1.step(4_200, lose(B)), vec![stalled(B)]);
    assert_eq!(f1.step(SYNC_DEADLINE, fired(4)), vec![stalled(C)]);
    assert_eq!(f1.log.iter().filter(|e| is_stalled(e)).count(), 2);
    assert_eq!(f1.module.rebuild_required(), Some(&everyone));

    // Every unproven copy already lost: the deadline has nobody left to name, and still answers
    // (BA-2) with the ignore a spent deadline gets.
    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 20, dg(0, 20)));
    f1.rec(4_100, durable(A, 20, dg(0, 20)));
    f1.rec(4_101, durable(B, 20, dg(0, 20)));
    assert_eq!(f1.step(4_200, lose(C)), vec![stalled(C)]);
    assert_eq!(f1.step(SYNC_DEADLINE, fired(4)), stale);
    assert_eq!(f1.log.iter().filter(|e| is_stalled(e)).count(), 1);
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
}

/// M7B-154 (D §5.6a; timer versioning; near-miss twin of M7B-153 by one fact): C's proof arrives
/// before the deadline. The rebuild proceeds as before the timer existed: the barrier holds and
/// activation is proposed. The deadline's fire is then `StaleTimer`, and nothing stalls.
#[retcd_test]
fn m7b_154_a_sync_answered_before_its_deadline_makes_the_timer_stale() {
    let mut f1 = rebuilding_while_c_syncs();
    assert_eq!(
        f1.rec(SYNC_DEADLINE - 1, durable(C, 20, dg(0, 20))),
        vec![cas(Revision(9), A), arm(5, SYNC_DEADLINE - 1 + WINDOW)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    assert_eq!(
        f1.step(SYNC_DEADLINE, fired(4)),
        vec![ign(ReplicaIgnoreReason::StaleTimer)]
    );
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    assert_eq!(f1.log.iter().filter(|e| is_stalled(e)).count(), 0);
    let active = recovered(&f1.step(
        SYNC_DEADLINE + 100,
        cas_result(CasOutcome::Committed(Revision(11))),
    ));
    assert_eq!(active.mode, PartitionMode::Active);
}

// ---------------------------------------------------------------------------------------------
// Package 5 rows (plan §8.3, §9): the no-merge scan (M7B-116), and the two rows that follow F1's
// output into R1 (M7B-112, M7B-138)
// ---------------------------------------------------------------------------------------------

/// The identifiers in `source` outside `//` comments (doc comments included). A string literal's
/// words count as identifiers, which only makes the scan stricter.
fn identifiers(source: &str) -> Vec<&str> {
    source
        .lines()
        .map(|line| line.find("//").map_or(line, |at| &line[..at]))
        .flat_map(|code| code.split(|c: char| !(c.is_alphanumeric() || c == '_')))
        .filter(|word| word.starts_with(|c: char| c.is_alphabetic() || c == '_'))
        .collect()
}

/// The identifiers the charter's DO-NOT list forbids in F1: anything naming a merge, a union or
/// a delete, and the length-only chooser `longest_by_len`.
fn forbidden(word: &str) -> bool {
    let lower = word.to_ascii_lowercase();
    ["merge", "union", "delete"]
        .iter()
        .any(|bad| lower.contains(bad))
        || lower == "longest_by_len"
}

/// M7B-116 (charter DO-NOT list; D §5.9; re-worded by lead ruling B-R49b): F1's source names no
/// merge, union or delete and no `longest_by_len`, outside comments; `max_by` appears once, in
/// `lineage::longest`, the one place length chooses between survivors; and `select_prefix` reaches
/// it only after every pair has passed: `length_reads` is 0 on `Divergence` and on `NeedProbes`,
/// and 1 on `Selected`. The scan reads the files at compile time and checks, at run time, that
/// `src/recovery/` holds exactly those files, so a new F1 file cannot escape it.
#[retcd_test]
fn m7b_116_no_merge_union_or_delete_in_recovery_source() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/recovery");
    let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
        .expect("F1's source directory")
        .map(|entry| {
            entry
                .expect("a directory entry")
                .file_name()
                .into_string()
                .expect("a UTF-8 name")
        })
        .collect();
    on_disk.sort();
    assert_eq!(
        on_disk,
        [
            "commit.rs",
            "emit.rs",
            "inventory.rs",
            "lineage.rs",
            "rebuild.rs"
        ],
        "a file added to F1 must be added to this scan"
    );
    let lineage_rs = include_str!("../src/recovery/lineage.rs");
    let f1_source = [
        ("recovery.rs", include_str!("../src/recovery.rs")),
        ("commit.rs", include_str!("../src/recovery/commit.rs")),
        ("emit.rs", include_str!("../src/recovery/emit.rs")),
        ("inventory.rs", include_str!("../src/recovery/inventory.rs")),
        ("lineage.rs", lineage_rs),
        ("rebuild.rs", include_str!("../src/recovery/rebuild.rs")),
    ];

    // Positive control: the scan sees code and skips comments.
    let control = identifiers("let x = a.merge_all().max_by_key(k); // union and delete");
    assert!(control.iter().any(|w| forbidden(w)), "{control:?}");
    assert!(control.contains(&"max_by_key"), "{control:?}");
    assert!(!control.contains(&"union"), "{control:?}");

    let mut max_by = 0;
    for (file, source) in f1_source {
        let words = identifiers(source);
        assert!(words.len() > 100, "{file}: the scan read nothing");
        let bad: Vec<&&str> = words.iter().filter(|w| forbidden(w)).collect();
        assert!(bad.is_empty(), "{file}: {bad:?}");
        max_by += words.iter().filter(|w| w.starts_with("max_by")).count();
    }
    assert_eq!(max_by, 1, "one length chooser in all of F1");
    let longest = lineage_rs
        .split_once("\nfn longest<")
        .and_then(|(_, rest)| rest.split_once("\n}\n"))
        .expect("lineage::longest")
        .0;
    assert_eq!(
        identifiers(longest)
            .iter()
            .filter(|w| w.starts_with("max_by"))
            .count(),
        1,
        "and it is in lineage::longest"
    );

    // The spy: length is read only once every pair has passed.
    for (reports, outcome_is, reads) in [
        (vec![inv(B, 30), inv_on(C, 30, 1, 20)], "Divergence", 0),
        (vec![sparse(B, 30), inv(C, 20)], "NeedProbes", 0),
        (vec![inv(B, 30), inv(C, 20)], "Selected", 1),
    ] {
        let mut spy = SelectionSpy::new();
        let outcome = select_prefix_spied(&verified(&reports), root(), &mut spy);
        let kind = match outcome {
            SelectionOutcome::Divergence(_) => "Divergence",
            SelectionOutcome::NeedProbes(_) => "NeedProbes",
            SelectionOutcome::Selected(_) => "Selected",
            SelectionOutcome::Empty => "Empty",
        };
        assert_eq!(kind, outcome_is, "{outcome:?}");
        assert_eq!(spy.selections(), 1);
        assert_eq!(spy.length_reads(), reads, "{outcome_is}");
    }
}

/// A step of R1 on `node`, returning the effect kinds.
fn r1_on(module: &mut Replication, node: NodeId, now: u64, kind: EventKind) -> Vec<EffectKind> {
    let event = Event {
        id: EventId(now),
        at: Tick(now),
        node,
        boot: BootId(1),
        partition: PARTITION,
        correlation: CORRELATION,
        kind,
    };
    module
        .step(&ctx(now), &event)
        .expect("R1 answers its own event")
        .into_iter()
        .map(|effect| effect.kind)
        .collect()
}

fn node_of(copy: CopyId) -> NodeId {
    NodeId(u32::from(copy.0))
}

/// `copy`'s own label: the fixture boots every node as 1.
fn label_of(copy: CopyId) -> PeerLabel {
    PeerLabel {
        node: node_of(copy),
        boot: BootId(1),
        authenticated: true,
    }
}

/// M7B-112's plan: `{primary, secondary}` and nothing else, so two regular copies is the whole
/// membership. Only `primary` may lead, so the record's owner and the pin's primary are one copy.
fn rf2_plan(primary: CopyId, secondary: CopyId) -> RecoveryPlan {
    let mut rf2 = plan(&[]);
    rf2.config = PartitionConfig::new(
        PARTITION,
        C1,
        vec![
            member(primary, ReplicaRole::Primary),
            member(secondary, ReplicaRole::RegularSecondary),
        ],
    );
    rf2.candidates = vec![
        candidate(primary),
        Candidate {
            primary_eligible: false,
            ..candidate(secondary)
        },
    ];
    rf2.rebuild_required = [primary, secondary].into_iter().collect();
    rf2
}

/// `secondary`'s ACK at 20 in the new root, holding `digest` at 20.
fn rf2_ack(secondary: CopyId, digest: Digest) -> EventKind {
    let ack = AppendAck {
        partition: PARTITION,
        generation: root().generation,
        owner_epoch: root().owner_epoch,
        config_version: C1,
        from: node_of(secondary),
        boot: BootId(1),
        role: ReplicaRole::RegularSecondary,
        progress: ReplicaProgress {
            received: ReceivedSeq(20),
            buffered_applied: AppliedSeq(20),
            durable: DurableSeq(20),
        },
        digest_at_buffered: digest,
    };
    EventKind::Transport(TransportEvent::Delivered {
        from: label_of(secondary),
        frame: Frame {
            id: MessageId(20),
            protocol: ENVELOPE_VERSION,
            config: C1,
            sender: root(),
            body: encode_reply(&AppendOutcome::Accepted(ack)),
        },
    })
}

/// M7B-112 (D §5.6 `DegradedRf2` row; B-R3; gate V3 "degraded RF2 requires both"): B and C
/// survive, both at 20, and F1 commits `DegradedRf2` pinning `min_regular_acks` 1. The commit's
/// `Recovered` builds R1's primary on B, whose only regular secondary is C: 1 of 1. C's good ACK
/// qualifies 20; C's forked ACK (the M7B-51 path) loses C, and the same step blocks the partition
/// and leaves nothing qualifying. The twin swaps the roles (C primary, B its secondary), and
/// losing B stops writes the same way, so losing either stops writes. The primary's own loss is
/// not an R1 path: a primary that is gone emits nothing, and a new recovery is F1's input, not
/// this row's.
#[retcd_test]
fn m7b_112_degraded_rf2_requires_both_copies_losing_either_stops_writes() {
    for (primary, secondary) in [(B, C), (C, B)] {
        let mut f1 = fenced_on(rf2_plan(primary, secondary));
        assert_eq!(
            f1.log[1],
            r(RecoveryEffect::QueryInventory { copies: vec![B, C] })
        );
        f1.report(10, inv(B, 20));
        f1.report(10, inv(C, 20));
        assert_eq!(f1.phase(), RecoveryPhase::Collecting);
        f1.step(WINDOW, fired(1));
        assert_eq!(f1.phase(), RecoveryPhase::Barrier);
        f1.rec(3_000, durable(B, 20, dg(0, 20)));
        assert_eq!(
            f1.rec(3_001, durable(C, 20, dg(0, 20))),
            vec![cas(CONTROL_REV, primary), arm(3, 3_001 + WINDOW)]
        );
        let result = recovered(&f1.step(3_100, cas_result(CasOutcome::Committed(Revision(9)))));
        assert_eq!(result.mode, PartitionMode::DegradedRf2);
        assert_eq!(result.committed.pinned_config.min_regular_acks, 1);

        let mut r1 = Replication::new();
        r1_on(
            &mut r1,
            node_of(primary),
            4_000,
            EventKind::Kernel(KernelEvent::Recovered(Box::new(result))),
        );
        let tracker = |r1: &Replication| {
            r1.primary(node_of(primary), PARTITION)
                .expect("the pin's primary is built")
                .tracker()
                .clone()
        };
        assert_eq!(
            tracker(&r1).regular_secondaries(),
            vec![secondary],
            "1 of 1"
        );
        assert!(
            !tracker(&r1).qualifies_now(Seq(20)),
            "RF2 needs the secondary"
        );

        r1_on(
            &mut r1,
            node_of(primary),
            4_100,
            rf2_ack(secondary, dg(0, 20)),
        );
        assert!(tracker(&r1).qualifies_now(Seq(20)), "both hold 20");

        let lost = r1_on(
            &mut r1,
            node_of(primary),
            4_200,
            rf2_ack(secondary, dg(9, 20)),
        );
        let block = EffectKind::Kernel(KernelEffect::BlockPartition(
            BlockReason::DivergenceRequiresOperator {
                diverged: vec![secondary],
            },
        ));
        assert!(
            lost.contains(&EffectKind::Kernel(KernelEffect::CopyLost {
                copy: secondary
            })),
            "{lost:?}"
        );
        assert!(lost.contains(&block), "writes stop the same step: {lost:?}");
        for seq in [1, 20] {
            assert!(
                !tracker(&r1).qualifies_now(Seq(seq)),
                "no one-copy fallback"
            );
        }
    }
}

/// A sealed record at `seq` in the prior lineage (7, 1) under `C1`, chained on `prev`.
fn record_at(seq: u64, prev: Digest) -> ReplicationEnvelope {
    let mut env = ReplicationEnvelope {
        header: EnvelopeHeader {
            protocol_version: ENVELOPE_VERSION,
            partition: PARTITION,
            generation: PRIOR_GEN,
            config_version: C1,
            owner_epoch: PRIOR_EPOCH,
            seq: Seq(seq),
            body_len: 0,
        },
        lease_id: LeaseId(1),
        prev_digest: prev,
        request_identity: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(seq),
        },
        request_digest: Digest::of(Domain::Record, &[&seq.to_le_bytes()]),
        conditions_result: Vec::new(),
        mutations: vec![Write {
            ns: Namespace::User,
            key: Bytes::from_static(b"k"),
            value: Some(Bytes::from_static(b"v")),
        }],
        result: Outcome::Published,
        record_digest: Digest::ROOT,
    };
    env.record_digest = env.compute_record_digest().expect("digest");
    env
}

/// The prior lineage's history 1..=n, so `history(n)[i - 1]` is the record at seq `i`.
fn history(n: u64) -> Vec<ReplicationEnvelope> {
    let mut out: Vec<ReplicationEnvelope> = Vec::new();
    for seq in 1..=n {
        let prev = out.last().map_or(Digest::ROOT, |env| env.record_digest);
        out.push(record_at(seq, prev));
    }
    out
}

/// `copy`'s receiver under the plan's config, applied and durable at 20 of [`history`].
fn receiver_at_20(config: &PartitionConfig, copy: CopyId) -> AppendReceiver {
    let head = history(20).pop().expect("seq 20");
    AppendReceiver::new(ReceiverInit {
        config: config.clone(),
        own: copy,
        lineage: prior(),
        head: Head {
            seq: Seq(20),
            digest: head.record_digest,
        },
        durable: DurableSeq(20),
    })
    .expect("a receiver at 20")
}

/// `env` as a `RecoveryAppend` from `from`'s label under `credential`, on the credential's
/// lineage.
fn recovery_append(
    from: CopyId,
    credential: &FenceCredential,
    env: &ReplicationEnvelope,
) -> EventKind {
    EventKind::Transport(TransportEvent::Delivered {
        from: label_of(from),
        frame: Frame {
            id: MessageId(u32::try_from(env.header.seq.0).expect("a small seq")),
            protocol: ENVELOPE_VERSION,
            config: C1,
            sender: Lineage {
                partition: credential.partition,
                generation: credential.prior_generation,
                owner_epoch: credential.prior_owner_epoch,
            },
            body: encode_recovery_append(credential, &env.encode().expect("encode")),
        },
    })
}

/// The credential in F1's transfer effect to `to`, whichever of the two transfer kinds it is.
fn credential_to(effects: &[EffectKind], to: CopyId) -> FenceCredential {
    let found: Vec<FenceCredential> = effects
        .iter()
        .filter_map(|e| match e {
            EffectKind::Kernel(KernelEffect::Recovery(
                RecoveryEffect::CatchUpBeforeGrant {
                    to: dest,
                    credential,
                    ..
                }
                | RecoveryEffect::CatchUp {
                    to: dest,
                    credential,
                    ..
                },
            )) if *dest == to => Some(*credential),
            _ => None,
        })
        .collect();
    assert_eq!(found.len(), 1, "one transfer to {to:?} in {effects:?}");
    found[0]
}

/// M7B-138 (D §3.2a row (a), §5.4 `CatchUpBeforeGrant{credential{sender holder}}`; 0005 §2 "the
/// designated sender is admitted wherever it sends"; 0009 §4 "holder != leader transfers land"):
/// B holds 30 and may not lead; C leads, D is a lagging regular, both at 20. F1's transfers both
/// carry a credential naming B, the holder. Under that credential every record 21..=30 sent from
/// B's label is staged at C and at D: 6R' admits it. The twin is the round-2 shape, a credential
/// naming F1's own node's copy A: the same record from B's label is `NotAMember` at C and changes
/// nothing. F1 proposes C only after C's `CopyCaughtUp` at the cutoff and the barrier (M7B-98
/// continues); D's catch-up alone proposes nothing.
#[retcd_test]
fn m7b_138_holder_that_cannot_lead_transfers_under_a_credential_naming_the_holder() {
    let mut holder_cannot_lead = plan(&[member(D, ReplicaRole::RegularSecondary)]);
    holder_cannot_lead.candidates = vec![
        Candidate {
            primary_eligible: false,
            ..candidate(B)
        },
        candidate(C),
        candidate(D),
    ];
    let config = holder_cannot_lead.config.clone();
    let mut f1 = fenced_on(holder_cannot_lead);
    f1.report(10, inv(B, 30));
    f1.report(10, inv(C, 20));
    f1.report(10, inv(D, 20));
    f1.rec(10, RecoveryEvent::InventoryFailed { copy: A });
    let close = f1.step(WINDOW, fired(1));
    assert_eq!(
        close,
        vec![
            r(RecoveryEffect::CloseWindow),
            selected(30, B),
            r(RecoveryEffect::CatchUpBeforeGrant {
                from: B,
                to: C,
                through: Seq(30),
                credential: proof().credential_for(B),
            }),
            catch_up(B, D, 30),
            arm(2, 2 * WINDOW),
        ]
    );
    let to_c = credential_to(&close, C);
    let to_d = credential_to(&close, D);
    assert_eq!(
        (to_c.sender, to_d.sender),
        (B, B),
        "the holder, not the leader"
    );

    // Every record B sends under F1's credential is admitted at C and at D.
    let records = history(30);
    let mut r1 = Replication::new();
    for copy in [C, D] {
        r1.install_receiver(receiver_at_20(&config, copy));
    }
    for (copy, credential) in [(C, to_c), (D, to_d)] {
        for (batch, env) in (0..).zip(&records[20..]) {
            let seq = env.header.seq;
            let staged = r1_on(
                &mut r1,
                node_of(copy),
                5_000,
                recovery_append(B, &credential, env),
            );
            match staged.as_slice() {
                [EffectKind::Store(StoreEffect::Commit(stored))] => {
                    assert_eq!((stored.generation, stored.seq), (PRIOR_GEN, seq));
                }
                other => panic!("{copy:?} did not admit {seq:?}: {other:?}"),
            }
            r1_on(
                &mut r1,
                node_of(copy),
                5_001,
                EventKind::Storage(StorageEvent::Committed {
                    batch: BatchId(batch),
                    applied: AppliedSeq(seq.0),
                }),
            );
        }
        let rx = r1.receiver(node_of(copy), PARTITION).expect("installed");
        assert_eq!(
            rx.applied_head(),
            Head {
                seq: Seq(30),
                digest: records[29].record_digest
            },
            "{copy:?}"
        );
    }

    // The twin: the round-2 shape names F1's own node's copy, A, and B's record is refused.
    let round_2 = proof().credential_for(A);
    let mut twin = Replication::new();
    twin.install_receiver(receiver_at_20(&config, C));
    let before = twin.receiver(node_of(C), PARTITION).cloned();
    let refused = r1_on(
        &mut twin,
        node_of(C),
        5_000,
        recovery_append(B, &round_2, &records[20]),
    );
    match refused.as_slice() {
        [EffectKind::Send(SendEffect::Unicast { to, frame })] => {
            assert_eq!(*to, node_of(B));
            assert_eq!(
                decode_reply(&frame.body).expect("a reply"),
                AppendOutcome::Rejected(AppendReject::NotAMember)
            );
        }
        other => panic!("expected one refusal, got {other:?}"),
    }
    assert_eq!(twin.receiver(node_of(C), PARTITION).cloned(), before);

    // F1: D's catch-up proposes nothing; C's does, after the barrier.
    f1.rec(6_000, caught_up(D, 30, dg(0, 30)));
    assert_eq!(f1.phase(), RecoveryPhase::Synchronizing);
    assert_eq!(f1.cas_count(), 0);
    assert_eq!(
        f1.rec(6_100, caught_up(C, 30, dg(0, 30))),
        vec![sync(B, 30), sync(C, 30), sync(D, 30)]
    );
    assert_eq!(f1.cas_count(), 0);
    f1.rec(6_200, durable(B, 30, dg(0, 30)));
    f1.rec(6_200, durable(D, 30, dg(0, 30)));
    assert_eq!(
        f1.rec(6_300, durable(C, 30, dg(0, 30))),
        vec![cas(CONTROL_REV, C), arm(3, 6_300 + WINDOW)]
    );
}

// ---------------------------------------------------------------------------------------------
// B-R74 rows (plan §9; Gautam 2026-09-27): a fresh fence after commit re-enters recovery
// (M7B-227..229). A fence is newer when the root it would commit is newer than the one this
// instance committed: `new_root(proof).generation > committed.new_generation`, which is
// `proof.prior_generation >= committed.new_generation`.
// ---------------------------------------------------------------------------------------------

/// When the second run is fenced. Past every tick the first run used.
const REFENCE_AT: u64 = 90_000;

/// A later takeover's fence: it proves the root the first run committed, (8, 2), and read control
/// after that commit landed at revision 9 (or 11, once activated).
fn later_fence() -> FencingProof {
    FencingProof {
        control_revision: Revision(12),
        decision_tick: Tick(REFENCE_AT),
        ..refence()
    }
}

/// Rebuilding, with every rebuild sync answered: the activation CAS is in flight (timer at v5).
fn activation_proposed() -> F1 {
    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 20, dg(0, 20)));
    for copy in [A, B, C] {
        f1.rec(4_100, durable(copy, 20, dg(0, 20)));
    }
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    f1
}

/// Every phase at or after commit, reached the way a run reaches it, and the timer version the
/// run last armed there.
fn after_commit() -> Vec<(&'static str, F1, u64)> {
    let mut active = proposing(20);
    active.step(3_100, cas_result(CasOutcome::Committed(Revision(9))));
    assert_eq!(active.phase(), RecoveryPhase::Committed);
    let rebuilding = lone_committed();
    assert_eq!(rebuilding.phase(), RecoveryPhase::Rebuilding);
    let mut activated = activation_proposed();
    activated.step(4_200, cas_result(CasOutcome::Committed(Revision(11))));
    assert_eq!(activated.phase(), RecoveryPhase::Committed);
    vec![
        ("committed active", active, 3),
        ("rebuilding", rebuilding, 3),
        ("activation proposed", activation_proposed(), 5),
        ("active after rebuild", activated, 5),
    ]
}

/// The version of the last timer the run armed.
fn armed_version(log: &[EffectKind]) -> u64 {
    log.iter()
        .rev()
        .find_map(|e| match e {
            EffectKind::Timer(TimerEffect::Arm { version, .. }) => Some(version.0),
            _ => None,
        })
        .expect("a timer was armed")
}

/// `effects` without its timer arms: two instances number their timers differently.
fn untimed(effects: &[EffectKind]) -> Vec<EffectKind> {
    effects
        .iter()
        .filter(|e| !matches!(e, EffectKind::Timer(_)))
        .cloned()
        .collect()
}

/// A fenced second run, driven to its commit: every copy on the new root at 30, the window
/// closes, all three prove 30, the CAS lands at 14. Returns each step's effects, untimed, and the
/// result.
fn second_run(f1: &mut F1) -> (Vec<Vec<EffectKind>>, RecoveryResult) {
    let t = REFENCE_AT;
    let version = armed_version(&f1.log);
    let mut steps = Vec::new();
    for copy in [A, B, C] {
        steps.push(f1.report(t + 10, on_new_root(copy, 30)));
    }
    steps.push(f1.step(t + WINDOW, fired(version)));
    for copy in [A, B, C] {
        steps.push(f1.rec(t + WINDOW + 100, durable(copy, 30, dg(0, 30))));
    }
    let commit = f1.step(
        t + WINDOW + 200,
        cas_result(CasOutcome::Committed(Revision(14))),
    );
    let result = recovered(&commit);
    steps.push(commit);
    (steps.iter().map(|s| untimed(s)).collect(), result)
}

/// The second run on a fresh instance: what the re-entered run must equal.
fn fresh_second_run() -> (Vec<Vec<EffectKind>>, RecoveryResult) {
    let mut f1 = F1::new();
    f1.rec(REFENCE_AT, RecoveryEvent::Plan(Box::new(replan())));
    f1.rec(
        REFENCE_AT,
        RecoveryEvent::FenceProven(Box::new(later_fence())),
    );
    second_run(&mut f1)
}

/// M7B-227 (B-R74, Gautam 2026-09-27; D §5.1 re-entry edge): in `Committed`, `Rebuilding` and
/// `Committed` after activation (every phase after commit with no CAS of its own in flight), a
/// replacement plan is held and a fence newer than the committed root starts a new recovery
/// exactly as `Idle` does. Nothing of the first run reaches the second: its effects and its
/// result equal a fresh instance's. Update (B-R74b): `ActivationProposed` holds the fence
/// instead (M7B-230..232).
#[retcd_test]
fn m7b_227_a_newer_fence_after_commit_re_enters_recovery() {
    let (fresh_steps, fresh_result) = fresh_second_run();
    assert_eq!(fresh_result.fenced_prior, later_fence());
    assert_eq!(fresh_result.new_generation, Generation(9));
    assert_eq!(
        fresh_result.retained_status_map.predecessor_generation,
        Generation(8)
    );
    for (label, mut f1, version) in after_commit() {
        if label == "activation proposed" {
            continue; // held, not entered: M7B-230
        }
        let phase = f1.phase();
        assert_eq!(
            f1.rec(REFENCE_AT, RecoveryEvent::Plan(Box::new(replan()))),
            vec![ign(ReplicaIgnoreReason::Recorded)],
            "{label}: the plan is held"
        );
        assert_eq!(f1.phase(), phase, "{label}: a plan is held, not acted on");
        assert_eq!(
            f1.rec(
                REFENCE_AT,
                RecoveryEvent::FenceProven(Box::new(later_fence()))
            ),
            vec![
                r(RecoveryEffect::QueryInventory {
                    copies: vec![A, B, C]
                }),
                arm(version + 1, REFENCE_AT + WINDOW),
            ],
            "{label}: the first effect vector is the one Idle gives"
        );
        assert_eq!(f1.phase(), RecoveryPhase::Fenced, "{label}");
        assert_eq!(f1.module.rebuild_required(), None, "{label}: no rebuild");
        let (steps, result) = second_run(&mut f1);
        assert_eq!(steps, fresh_steps, "{label}: the second run is a fresh one");
        assert_eq!(
            result, fresh_result,
            "{label}: nothing of the first run leaks"
        );
        assert_eq!(f1.phase(), RecoveryPhase::Committed, "{label}");
    }
}

/// M7B-228 (B-R74, Gautam 2026-09-27): a fence whose root is not newer than the committed one is
/// ignored `OutOfPhase` in every phase after commit, and nothing moves, even when the held plan
/// matches it. Generation decides, not the control revision.
#[retcd_test]
fn m7b_228_a_stale_or_equal_fence_after_commit_is_ignored() {
    let fence = |proof: FencingProof| RecoveryEvent::FenceProven(Box::new(proof));
    let out_of_phase = vec![ign(ReplicaIgnoreReason::OutOfPhase)];
    let read_later = FencingProof {
        control_revision: Revision(50),
        ..proof()
    };
    let older = FencingProof {
        prior_generation: Generation(6),
        prior_owner_epoch: OwnerEpoch(0),
        control_revision: Revision(50),
        ..proof()
    };
    let older_plan = RecoveryPlan {
        anchor: LineageAnchor {
            lineage: Lineage {
                partition: PARTITION,
                generation: Generation(6),
                owner_epoch: OwnerEpoch(0),
            },
            ..anchor()
        },
        ..plan(&[])
    };
    for (label, mut f1, _) in after_commit() {
        let phase = f1.phase();
        let before = f1.module.clone();
        assert_eq!(
            f1.rec(REFENCE_AT, fence(proof())),
            out_of_phase,
            "{label}: the fence this run committed on, replayed; the held plan matches it"
        );
        assert_eq!(f1.module, before, "{label}: nothing moved");
        assert_eq!(
            f1.rec(REFENCE_AT, fence(read_later.clone())),
            out_of_phase,
            "{label}: an equal root read later is not newer"
        );
        assert_eq!(f1.module, before, "{label}: nothing moved");
        f1.rec(
            REFENCE_AT,
            RecoveryEvent::Plan(Box::new(older_plan.clone())),
        );
        let held = f1.module.clone();
        assert_eq!(
            f1.rec(REFENCE_AT, fence(older.clone())),
            out_of_phase,
            "{label}: an older root, its plan held"
        );
        assert_eq!(f1.module, held, "{label}: nothing moved");
        assert_eq!(f1.phase(), phase, "{label}");
    }
}

/// M7B-229 (B-R74, Gautam 2026-09-27): what the first run left in flight (its timers, and a
/// rebuild catch-up and sync answer) reaches the second run as input to the second run's own
/// phase and is judged there alone: stale timers, out-of-phase events, and a proof below the new
/// cutoff that proves nothing and displaces nothing. The second run commits exactly what a fresh
/// instance commits. Update (B-R74b): the fixture is `Rebuilding` with every rebuild sync
/// outstanding; from `ActivationProposed` a fence is held until the activation answer, so no
/// answer of the first run's can arrive after re-entry (M7B-230) unless the exchange's deadline
/// released it first, and then that answer is `UnmatchedCompletion` (issue #2, M7B-240).
#[retcd_test]
fn m7b_229_a_late_event_from_the_first_run_does_not_touch_the_second() {
    let (_, fresh_result) = fresh_second_run();
    let t = REFENCE_AT;
    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 20, dg(0, 20)));
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    f1.rec(t, RecoveryEvent::Plan(Box::new(replan())));
    f1.rec(t, RecoveryEvent::FenceProven(Box::new(later_fence())));
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);

    let out_of_phase = vec![ign(ReplicaIgnoreReason::OutOfPhase)];
    assert_eq!(
        f1.rec(t + 5, durable(A, 20, dg(0, 20))),
        out_of_phase,
        "a rebuild sync's answer"
    );
    assert_eq!(
        f1.rec(t + 5, caught_up(C, 20, dg(0, 20))),
        out_of_phase,
        "a rebuild catch-up"
    );
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);

    for copy in [A, B, C] {
        f1.report(t + 10, on_new_root(copy, 30));
    }
    for version in 1..=4 {
        assert_eq!(
            f1.step(t + WINDOW, fired(version)),
            vec![ign(ReplicaIgnoreReason::StaleTimer)],
            "the first run's timer v{version}, due by the clock"
        );
    }
    assert_eq!(f1.phase(), RecoveryPhase::Collecting);
    let close = f1.step(t + WINDOW, fired(5));
    assert!(has_selected(&close), "{close:?}");
    assert_eq!(f1.phase(), RecoveryPhase::Barrier);

    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];
    assert_eq!(
        f1.rec(t + WINDOW + 100, durable(A, 30, dg(0, 30))),
        not_durable
    );
    assert_eq!(
        f1.rec(t + WINDOW + 101, durable(A, 20, dg(0, 20))),
        not_durable,
        "the first run's sync answer proves nothing at 30"
    );
    assert_eq!(
        f1.rec(t + WINDOW + 102, durable(B, 30, dg(0, 30))),
        not_durable
    );
    let proposed = f1.rec(t + WINDOW + 103, durable(C, 30, dg(0, 30)));
    assert!(
        matches!(
            proposed.as_slice(),
            [
                EffectKind::Control(ControlEffect::Cas {
                    expected: Some(Revision(12)),
                    ..
                }),
                EffectKind::Timer(TimerEffect::Arm {
                    version: TimerVersion(7),
                    ..
                })
            ]
        ),
        "A's proof at 30 still stands: {proposed:?}"
    );
    let result = recovered(&f1.step(
        t + WINDOW + 200,
        cas_result(CasOutcome::Committed(Revision(14))),
    ));
    assert_eq!(
        result, fresh_result,
        "the second run commits what a fresh one does"
    );
    assert!(
        !f1.log.iter().any(|e| matches!(
            e,
            EffectKind::Kernel(KernelEffect::Recovery(
                RecoveryEffect::RebuildStalled { .. }
            ))
        )),
        "the first run's rebuild never stalls into the second"
    );
}

// ---------------------------------------------------------------------------------------------
// B-R74b rows (plan §9): while this run's activation CAS is in flight a newer fence is held, and
// re-enters only once the activation exchange has ended (M7B-230..232).
// ---------------------------------------------------------------------------------------------

/// A fence newer than [`later_fence`]: a peer's recovery committed (9, 3), and a later takeover
/// fenced it, reading control at revision 20.
fn newest_fence() -> FencingProof {
    FencingProof {
        prior_generation: Generation(9),
        prior_owner_epoch: OwnerEpoch(3),
        control_revision: Revision(20),
        ..later_fence()
    }
}

/// A plan anchored on (9, 3): the one [`newest_fence`] proves, and [`later_fence`] does not.
fn newest_plan() -> RecoveryPlan {
    RecoveryPlan {
        anchor: LineageAnchor {
            lineage: Lineage {
                partition: PARTITION,
                generation: Generation(9),
                owner_epoch: OwnerEpoch(3),
            },
            base_seq: Seq(25),
            base_digest: dg(0, 25),
        },
        ..plan(&[])
    }
}

/// [`activation_proposed`], with [`replan`] held and [`later_fence`] held behind the activation.
fn held_behind_activation() -> F1 {
    let mut f1 = activation_proposed();
    f1.rec(REFENCE_AT, RecoveryEvent::Plan(Box::new(replan())));
    assert_eq!(
        f1.rec(
            REFENCE_AT,
            RecoveryEvent::FenceProven(Box::new(later_fence()))
        ),
        vec![ign(ReplicaIgnoreReason::Recorded)],
        "held, not entered"
    );
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    f1
}

/// The re-entry `fence()` gives at `at`, armed at v6 (the first run armed v5).
fn re_entry(at: u64) -> Vec<EffectKind> {
    vec![
        r(RecoveryEffect::QueryInventory {
            copies: vec![A, B, C],
        }),
        arm(6, at + WINDOW),
    ]
}

/// M7B-230 (B-R74b, Gautam 2026-09-27): a newer fence while the activation CAS is in flight is
/// held (`Recorded`, phase unchanged). When the activation exchange ends, whatever its arm, the
/// answer is applied to the first run first, and then the held fence re-enters in the same step,
/// through the door of the phase the answer left: `Committed` (landed), `Blocked` (`Unknown`), and
/// `Blocked{OvertakenByPeer}` after a `Conflict` and its re-read (the exchange ends at the re-read,
/// not at the `Conflict`, so no request of the first run is left outstanding). The second run is a
/// fresh one. If no answer ever arrives (ADR 0008 item 8, `DropCompletion`), the exchange's deadline
/// ends it as `Unknown` and the fence re-enters then (issue #2, Gautam 2026-10-02, superseding
/// B-R74b's "no timer bounds it"; M7B-240).
#[retcd_test]
fn m7b_230_a_newer_fence_behind_an_activation_waits_for_its_answer() {
    let (fresh_steps, fresh_result) = fresh_second_run();
    let t = REFENCE_AT;

    let mut f1 = held_behind_activation();
    let landed = f1.step(t, cas_result(CasOutcome::Committed(Revision(11))));
    let active = recovered(&landed);
    assert_eq!(active.mode, PartitionMode::Active);
    assert_eq!(active.committed.revision, Revision(11));
    let mut expected = vec![EffectKind::Kernel(KernelEffect::Recovered(Box::new(
        active,
    )))];
    expected.extend(re_entry(t));
    assert_eq!(landed, expected, "the first run's Recovered, then re-entry");
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
    let (steps, result) = second_run(&mut f1);
    assert_eq!(steps, fresh_steps, "landed: the second run is a fresh one");
    assert_eq!(result, fresh_result, "landed");

    let mut f1 = held_behind_activation();
    let mut expected = vec![block(BlockReason::ControlUnknown)];
    expected.extend(re_entry(t));
    assert_eq!(f1.step(t, cas_result(CasOutcome::Unknown)), expected);
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
    let (steps, result) = second_run(&mut f1);
    assert_eq!(steps, fresh_steps, "unknown: the second run is a fresh one");
    assert_eq!(result, fresh_result, "unknown");

    let mut f1 = held_behind_activation();
    assert_eq!(
        f1.step(
            t,
            cas_result(CasOutcome::Conflict {
                exists: true,
                current: Revision(10),
            })
        ),
        vec![EffectKind::Control(ControlEffect::Get {
            request: LATEST,
            key: ControlKey::Partition(PARTITION)
        })],
        "the re-read is the first run's; the fence stays held"
    );
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    let peer = PartitionRecord {
        owner: NodeId(2),
        owner_epoch: OwnerEpoch(3),
        ..record(A)
    };
    let mut expected = vec![block(BlockReason::OvertakenByPeer)];
    expected.extend(re_entry(t + 1));
    assert_eq!(
        f1.step(t + 1, found(peer.encode())),
        expected,
        "read at 6, the floor is 6; the held fence read 12"
    );
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
}

/// M7B-231 (B-R74b, Gautam 2026-09-27): while held, a newer fence replaces the held one; one
/// not newer than the held fence, or than the committed root, is ignored `OutOfPhase` and moves
/// nothing. The fence that re-enters is the newest: only it proves the plan held.
#[retcd_test]
fn m7b_231_a_held_fence_is_replaced_only_by_a_newer_one() {
    let fence = |proof: FencingProof| RecoveryEvent::FenceProven(Box::new(proof));
    let out_of_phase = vec![ign(ReplicaIgnoreReason::OutOfPhase)];
    let mut f1 = held_behind_activation();
    assert_eq!(
        f1.rec(REFENCE_AT + 1, RecoveryEvent::Plan(Box::new(newest_plan()))),
        vec![ign(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(
        f1.rec(REFENCE_AT + 2, fence(newest_fence())),
        vec![ign(ReplicaIgnoreReason::Recorded)],
        "a newer fence replaces the held one"
    );
    let held = f1.module.clone();
    let read_later = FencingProof {
        control_revision: Revision(40),
        ..later_fence()
    };
    for (why, stale) in [
        ("older than the held fence", later_fence()),
        ("older than the held fence, read later", read_later),
        (
            "equal to the held fence, read later",
            FencingProof {
                control_revision: Revision(40),
                ..newest_fence()
            },
        ),
        ("the first run's own fence", proof()),
    ] {
        assert_eq!(f1.rec(REFENCE_AT + 3, fence(stale)), out_of_phase, "{why}");
        assert_eq!(f1.module, held, "{why}: nothing moved");
    }
    let t = REFENCE_AT + 50;
    let landed = f1.step(t, cas_result(CasOutcome::Committed(Revision(11))));
    assert_eq!(
        landed[1..],
        re_entry(t)[..],
        "the newest fence proves the held plan: {landed:?}"
    );
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
}

/// M7B-232 (B-R74b, Gautam 2026-09-27): nothing but the end of the activation exchange lets a
/// held fence in. Every other F1 input is answered as `ActivationProposed` answers it today, with
/// no inventory query and no re-armed discovery; a control answer F1 did not ask for is declined.
/// Update (issue #2, Gautam 2026-10-02): the end of the exchange is its answer **or its deadline**.
/// The first run's earlier timers (v1..v4) are still `StaleTimer`; the activation's own timer (v5)
/// at its deadline ends the exchange as `Unknown`, which M7B-240 pins, so it is not fired here.
#[retcd_test]
fn m7b_232_no_re_entry_while_a_fence_is_held() {
    let mut f1 = held_behind_activation();
    let t = REFENCE_AT + 10;
    let out_of_phase = vec![ign(ReplicaIgnoreReason::OutOfPhase)];
    assert_eq!(f1.report(t, on_new_root(A, 30)), out_of_phase);
    assert_eq!(f1.rec(t, durable(A, 20, dg(0, 20))), out_of_phase);
    assert_eq!(f1.rec(t, caught_up(C, 20, dg(0, 20))), out_of_phase);
    assert_eq!(f1.step(t, lose(B)), out_of_phase);
    for version in 1..=4 {
        assert_eq!(
            f1.step(t + WINDOW, fired(version)),
            vec![ign(ReplicaIgnoreReason::StaleTimer)],
            "v{version}: a timer older than the activation's own"
        );
    }
    assert_eq!(
        f1.rec(t, RecoveryEvent::Plan(Box::new(replan()))),
        vec![ign(ReplicaIgnoreReason::Recorded)]
    );
    assert!(!has_query(&f1.at(
        t,
        t,
        EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::StaleOwnerReturned(
            Box::new(inv(A, 99)),
        ))),
    )));
    f1.declines(
        t,
        read_result(ReadOutcome::Found {
            revision: Revision(10),
            value: record(A).encode(),
        }),
    );
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
    let queries = f1
        .log
        .iter()
        .filter(|e| has_query(std::slice::from_ref(*e)))
        .count();
    assert_eq!(queries, 1, "only the first run's own inventory query");
    let t = REFENCE_AT + 50;
    let landed = f1.step(t, cas_result(CasOutcome::Committed(Revision(11))));
    assert_eq!(landed[1..], re_entry(t)[..], "the answer lets it in");
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
}

// ---------------------------------------------------------------------------------------------
// Round 3 (plan §9, M7B-233..237): B-R74d extends B-R74a to the rebuild barrier, and three
// clauses of B-R74a/B-R74b that were right but unpinned.
// ---------------------------------------------------------------------------------------------

/// `effects` is exactly one activation CAS expecting `expected`, and the arm of the timer that
/// bounds it (issue #2, M7B-240).
fn is_activation_cas(effects: &[EffectKind], expected: Revision) -> bool {
    matches!(
        effects,
        [
            EffectKind::Control(ControlEffect::Cas { expected: Some(at), .. }),
            EffectKind::Timer(TimerEffect::Arm { id, .. }),
        ] if *at == expected && *id == DISCOVERY_TIMER
    )
}

/// The first run `Rebuilding` with its syncs at 20 outstanding; a newer fence re-enters; the
/// second run, A alone on the new root at 30, commits `ReadOnly` at 14; B catches up at 30, which
/// pins the second rebuild's point at 30 and syncs every copy there.
fn second_rebuild() -> F1 {
    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 20, dg(0, 20)));
    f1.rec(REFENCE_AT, RecoveryEvent::Plan(Box::new(replan())));
    f1.rec(
        REFENCE_AT,
        RecoveryEvent::FenceProven(Box::new(later_fence())),
    );
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
    let t = REFENCE_AT;
    f1.report(t + 10, on_new_root(A, 30));
    for copy in [B, C] {
        f1.rec(t + 10, RecoveryEvent::InventoryFailed { copy });
    }
    let version = armed_version(&f1.log);
    f1.step(t + WINDOW, fired(version));
    f1.rec(t + WINDOW + 100, durable(A, 30, dg(0, 30)));
    let result = recovered(&f1.step(
        t + WINDOW + 200,
        cas_result(CasOutcome::Committed(Revision(14))),
    ));
    assert_eq!(result.mode, PartitionMode::ReadOnly);
    assert_eq!(result.new_generation, Generation(9));
    assert_eq!(
        f1.rec(t + WINDOW + 300, caught_up(B, 30, dg(0, 30))),
        vec![
            sync(A, 30),
            sync(B, 30),
            sync(C, 30),
            arm(version + 3, t + WINDOW + 300 + WINDOW)
        ],
        "the second rebuild's point is 30"
    );
    f1
}

/// M7B-233 (B-R74d, Gautam 2026-09-27; ledger L-R177hw): a first-run rebuild answer lands in the
/// second run's rebuild. `DurableAt{A,20}` from the first run's syncs arrives after A proved the
/// second point (30): it is answered `BarrierNotDurable` and moves nothing, and B and C at 30 then
/// send the activation CAS. A first-run answer for a copy with no proof yet held is replaced by
/// that copy's own proof at the point.
#[retcd_test]
fn m7b_233_a_first_run_rebuild_answer_never_displaces_a_second_run_proof() {
    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];
    let mut f1 = second_rebuild();
    let t = REFENCE_AT + WINDOW + 400;
    assert_eq!(f1.rec(t, durable(A, 30, dg(0, 30))), not_durable);
    let proved = f1.module.clone();
    assert_eq!(
        f1.rec(t + 1, durable(A, 20, dg(0, 20))),
        not_durable,
        "run 1, late"
    );
    assert_eq!(f1.module, proved, "A's proof at 30 still stands");
    assert_eq!(
        f1.rec(t + 2, durable(C, 20, dg(0, 20))),
        not_durable,
        "run 1, late, before C's own"
    );
    assert_eq!(f1.rec(t + 3, durable(B, 30, dg(0, 30))), not_durable);
    let last = f1.rec(t + 4, durable(C, 30, dg(0, 30)));
    assert!(is_activation_cas(&last, Revision(14)), "{last:?}");
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);
}

/// M7B-234 (B-R74d, Gautam 2026-09-27): the same within one run. Per copy, a proof that does not
/// bind to the rebuild point never replaces one that does, and before the point is pinned a lower
/// proof never replaces a higher one. A proof that binds is always taken: the point binds by
/// digest at its own seq, so the copy's answer at the point may be lower than a proof it gave
/// before the pin.
#[retcd_test]
fn m7b_234_a_reordered_rebuild_proof_never_displaces_a_better_one() {
    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];

    // After the pin: A at the point, then a reordered A 15, then A 25 past the point.
    let mut f1 = lone_committed();
    f1.rec(4_000, caught_up(B, 20, dg(0, 20)));
    assert_eq!(f1.rec(4_100, durable(A, 20, dg(0, 20))), not_durable);
    let proved = f1.module.clone();
    for (why, seq) in [("lower", 15), ("higher, not at the point", 25)] {
        assert_eq!(
            f1.rec(4_101, durable(A, seq, dg(0, seq))),
            not_durable,
            "{why}"
        );
        assert_eq!(
            f1.module, proved,
            "after the pin, {why}: A's proof at 20 still stands"
        );
    }
    assert_eq!(f1.rec(4_102, durable(B, 20, dg(0, 20))), not_durable);
    let last = f1.rec(4_103, durable(C, 20, dg(0, 20)));
    assert!(
        is_activation_cas(&last, Revision(9)),
        "after the pin: {last:?}"
    );

    // Before the pin: A 20, then a reordered A 19, one below; B pins at 20.
    let mut f1 = lone_committed();
    assert_eq!(f1.rec(4_000, durable(A, 20, dg(0, 20))), not_durable);
    let held = f1.module.clone();
    assert_eq!(f1.rec(4_001, durable(A, 19, dg(0, 19))), not_durable);
    assert_eq!(
        f1.module, held,
        "before the pin: the lower proof moves nothing"
    );
    f1.rec(4_002, caught_up(B, 20, dg(0, 20)));
    assert_eq!(f1.rec(4_100, durable(B, 20, dg(0, 20))), not_durable);
    let last = f1.rec(4_101, durable(C, 20, dg(0, 20)));
    assert!(
        is_activation_cas(&last, Revision(9)),
        "before the pin: {last:?}"
    );

    // A proof that binds is taken over a higher one that does not.
    let mut f1 = lone_committed();
    assert_eq!(f1.rec(4_000, durable(A, 25, dg(0, 25))), not_durable);
    f1.rec(4_001, caught_up(B, 20, dg(0, 20)));
    assert_eq!(f1.rec(4_100, durable(A, 20, dg(0, 20))), not_durable);
    assert_eq!(f1.rec(4_101, durable(B, 20, dg(0, 20))), not_durable);
    let last = f1.rec(4_102, durable(C, 20, dg(0, 20)));
    assert!(
        is_activation_cas(&last, Revision(9)),
        "binding wins: {last:?}"
    );
}

/// M7B-235 (B-R74b and ruling A-5, Gautam 2026-09-27): a held fence released into `Blocked` must
/// still beat the peer's floor. The activation `Conflict` re-reads, and the peer's record was
/// read at 15; the held fence was read at 12, so it is refused `RecoveryBlocked` in the same step,
/// and only a fence read after 15 re-enters.
#[retcd_test]
fn m7b_235_a_released_fence_below_the_peer_floor_stays_blocked() {
    let mut f1 = held_behind_activation();
    let t = REFENCE_AT;
    assert_eq!(
        f1.step(
            t,
            cas_result(CasOutcome::Conflict {
                exists: true,
                current: Revision(15),
            })
        ),
        vec![EffectKind::Control(ControlEffect::Get {
            request: LATEST,
            key: ControlKey::Partition(PARTITION)
        })]
    );
    let peer = PartitionRecord {
        owner: NodeId(2),
        owner_epoch: OwnerEpoch(3),
        ..record(A)
    };
    assert_eq!(
        f1.step(
            t + 1,
            read_result(ReadOutcome::Found {
                revision: Revision(15),
                value: peer.encode(),
            })
        ),
        vec![
            block(BlockReason::OvertakenByPeer),
            ign(ReplicaIgnoreReason::RecoveryBlocked)
        ],
        "the held fence (read at 12) is below the floor (15)"
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Blocked(BlockReason::OvertakenByPeer)
    );
    let read_at = |revision: u64| {
        RecoveryEvent::FenceProven(Box::new(FencingProof {
            control_revision: Revision(revision),
            ..later_fence()
        }))
    };
    assert_eq!(
        f1.rec(t + 2, read_at(15)),
        vec![ign(ReplicaIgnoreReason::RecoveryBlocked)],
        "at the floor"
    );
    assert_eq!(f1.rec(t + 2, read_at(20)), re_entry(t + 2), "above it");
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
}

/// M7B-236 (B-R74a, Gautam 2026-09-27): the single-run half. In one run's `Barrier`, a
/// reordered lower `DurableAt`, or one past the cutoff that does not carry the cutoff digest,
/// never erases a copy's binding proof: the answer is `BarrierNotDurable` and nothing moves.
#[retcd_test]
fn m7b_236_in_one_run_a_non_binding_proof_never_displaces_a_binding_one() {
    let not_durable = vec![ign(ReplicaIgnoreReason::BarrierNotDurable)];
    let mut f1 = at_barrier(20);
    assert_eq!(f1.rec(3_000, durable(A, 20, dg(0, 20))), not_durable);
    let proved = f1.module.clone();
    for (why, seq) in [("reordered, below the cutoff", 15), ("past the cutoff", 25)] {
        assert_eq!(
            f1.rec(3_001, durable(A, seq, dg(0, seq))),
            not_durable,
            "{why}"
        );
        assert_eq!(f1.module, proved, "{why}: nothing moved");
    }
    assert_eq!(f1.rec(3_002, durable(B, 20, dg(0, 20))), not_durable);
    f1.rec(3_003, durable(C, 20, dg(0, 20)));
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Proposing,
        "A's binding proof still stands"
    );
}

/// M7B-237 (B-R74, Gautam 2026-09-27): a newer fence that does not prove the held plan answers
/// `InvalidConfig` and the phase holds, both where it would enter at once and when a held fence is
/// released. A released fence that fails is dropped: a plan that arrives later does not bring it
/// back.
#[retcd_test]
fn m7b_237_a_newer_fence_without_its_plan_is_invalid_config_and_the_phase_holds() {
    let invalid = vec![ign(ReplicaIgnoreReason::InvalidConfig)];
    for (label, mut f1, _) in after_commit() {
        if label == "activation proposed" {
            continue; // released below
        }
        let (phase, before) = (f1.phase(), f1.module.clone());
        assert_eq!(
            f1.rec(
                REFENCE_AT,
                RecoveryEvent::FenceProven(Box::new(later_fence()))
            ),
            invalid,
            "{label}"
        );
        assert_eq!(f1.phase(), phase, "{label}: the phase holds");
        assert_eq!(f1.module, before, "{label}: nothing moved");
    }

    let mut f1 = activation_proposed();
    assert_eq!(
        f1.rec(
            REFENCE_AT,
            RecoveryEvent::FenceProven(Box::new(later_fence()))
        ),
        vec![ign(ReplicaIgnoreReason::Recorded)],
        "held"
    );
    let landed = f1.step(REFENCE_AT, cas_result(CasOutcome::Committed(Revision(11))));
    assert_eq!(recovered(&landed).mode, PartitionMode::Active);
    assert_eq!(landed[1..], invalid[..], "released, and it fails");
    assert_eq!(f1.phase(), RecoveryPhase::Committed, "the phase holds");
    assert_eq!(
        f1.rec(REFENCE_AT + 1, RecoveryEvent::Plan(Box::new(replan()))),
        vec![ign(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Committed,
        "the failed fence was dropped"
    );
}

// ---------------------------------------------------------------------------------------------
// M7B-238 and M7B-239: an answer is matched by its request id (lead ledger L-R177hs).
// ---------------------------------------------------------------------------------------------

/// [`cas_result`], echoing `request` rather than F1's newest request.
fn cas_result_to(request: ControlRequestId, outcome: CasOutcome) -> EventKind {
    EventKind::Control(ControlEvent::CasResult {
        request,
        key: ControlKey::Partition(PARTITION),
        outcome,
    })
}

/// [`read_result`], echoing `request` rather than F1's newest request.
fn read_result_to(request: ControlRequestId, outcome: ReadOutcome) -> EventKind {
    EventKind::Control(ControlEvent::Value {
        request,
        key: ControlKey::Partition(PARTITION),
        outcome,
    })
}

/// The newest request F1 has sent.
fn newest(f1: &F1) -> ControlRequestId {
    *f1.requests.last().expect("fixture: F1 sent a request")
}

/// Blocked on `Unknown`, re-fenced on the same plan, and driven back to `Proposing`: the first
/// run's CAS was never answered for real, and the second run's CAS is now in flight. Returns the
/// first run's CAS id with it.
fn re_proposing_after_unknown() -> (F1, ControlRequestId) {
    let mut f1 = proposing(20);
    let abandoned = newest(&f1);
    assert_eq!(
        f1.step(3_100, cas_result(CasOutcome::Unknown)),
        vec![block(BlockReason::ControlUnknown)]
    );
    let t = 9_000;
    assert!(has_query(
        &f1.rec(t, RecoveryEvent::FenceProven(Box::new(proof())))
    ));
    for copy in [A, B, C] {
        f1.report(t + 10, inv(copy, 20));
    }
    let version = armed_version(&f1.log);
    f1.step(t + WINDOW, fired(version));
    for copy in [A, B, C] {
        f1.rec(t + WINDOW + 100, durable(copy, 20, dg(0, 20)));
    }
    assert_eq!(f1.phase(), RecoveryPhase::Proposing, "fixture");
    assert_eq!(
        f1.cas_count(),
        2,
        "fixture: the second run's CAS is in flight"
    );
    (f1, abandoned)
}

/// M7B-238 (lead ledger L-R177hs; B-R74b; the tester's F4): a late answer to an abandoned run's
/// root CAS is not the answer to a newer run's CAS on the same key. The first run blocks on
/// `Unknown`; a new fence drives the same instance back to `Proposing` with a fresh CAS. The first
/// CAS's real answer, `Committed`, then arrives: it echoes the first CAS's id, so it is
/// `UnmatchedCompletion` and moves nothing. The second CAS's own answer is taken. Red on `HEAD`
/// (547c82c): the late answer produced `Recovered` at revision 9 — a false commit.
#[retcd_test]
fn m7b_238_an_abandoned_root_cas_answer_is_not_the_new_runs() {
    let (mut f1, abandoned) = re_proposing_after_unknown();
    assert_ne!(newest(&f1), abandoned, "M7B-238: a fresh id per request");
    let before = f1.module.clone();
    assert_eq!(
        f1.step(
            12_000,
            cas_result_to(abandoned, CasOutcome::Committed(Revision(9)))
        ),
        vec![ign(ReplicaIgnoreReason::UnmatchedCompletion)],
        "M7B-238: the abandoned CAS's late answer is ignored, named"
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Proposing,
        "M7B-238: nothing moved"
    );
    assert_eq!(f1.cas_count(), 2, "M7B-238: and nothing was re-sent");
    assert_eq!(
        f1.module, before,
        "M7B-238: the whole module is unchanged, the plan included"
    );

    let result = recovered(&f1.step(12_100, cas_result(CasOutcome::Committed(Revision(12)))));
    assert_eq!(
        result.committed.revision,
        Revision(12),
        "M7B-238: the new run's own answer commits it"
    );
}

/// M7B-238, the activation half: B-R74b's re-entry path. A newer fence is held behind the
/// activation CAS; the activation answers `Unknown`, so the held fence re-enters and a second run
/// proposes its own root CAS. The abandoned activation CAS's real answer then arrives: it is
/// `UnmatchedCompletion`, and the second run's own answer commits it. Red on `HEAD` (547c82c):
/// the late activation answer produced `Recovered` at revision 11.
#[retcd_test]
fn m7b_238_an_abandoned_activation_answer_is_not_the_new_runs() {
    let t = REFENCE_AT;
    let mut f1 = held_behind_activation();
    let abandoned = newest(&f1);
    let mut expected = vec![block(BlockReason::ControlUnknown)];
    expected.extend(re_entry(t));
    assert_eq!(
        f1.step(t, cas_result(CasOutcome::Unknown)),
        expected,
        "fixture"
    );
    for copy in [A, B, C] {
        f1.report(t + 10, on_new_root(copy, 30));
    }
    let version = armed_version(&f1.log);
    f1.step(t + WINDOW, fired(version));
    for copy in [A, B, C] {
        f1.rec(t + WINDOW + 100, durable(copy, 30, dg(0, 30)));
    }
    assert_eq!(f1.phase(), RecoveryPhase::Proposing, "fixture");
    assert_ne!(newest(&f1), abandoned, "M7B-238: a fresh id per request");

    let before = f1.module.clone();
    assert_eq!(
        f1.step(
            t + WINDOW + 150,
            cas_result_to(abandoned, CasOutcome::Committed(Revision(11)))
        ),
        vec![ign(ReplicaIgnoreReason::UnmatchedCompletion)],
        "M7B-238: the abandoned activation's late answer is ignored, named"
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Proposing,
        "M7B-238: nothing moved"
    );
    assert_eq!(
        f1.module, before,
        "M7B-238: the whole module is unchanged, the plan included"
    );

    let result = recovered(&f1.step(
        t + WINDOW + 200,
        cas_result(CasOutcome::Committed(Revision(14))),
    ));
    assert_eq!(result.committed.revision, Revision(14));
    assert_eq!(
        result.new_generation,
        Generation(9),
        "M7B-238: the second run's own answer commits the second run"
    );
}

/// M7B-239 (lead ledger L-R177hs): the same for the `Conflict` re-read `Get`. The first run's
/// re-read answers `Unavailable` and blocks; a new fence drives a second run to its own
/// `Conflict` and its own re-read. A second, late answer to the first re-read — a peer's record,
/// which would read as `OvertakenByPeer` — is `UnmatchedCompletion` and decides nothing. The
/// second re-read's own answer decides the second run. Red on `HEAD` (547c82c): the late answer
/// blocked the second run `OvertakenByPeer`.
#[retcd_test]
fn m7b_239_an_abandoned_reread_answer_is_not_the_new_runs() {
    let mut f1 = rereading();
    let abandoned = newest(&f1);
    assert_eq!(
        f1.step(3_200, read_result(ReadOutcome::Unavailable)),
        vec![block(BlockReason::ControlUnavailable)],
        "fixture"
    );
    let t = 9_000;
    assert!(has_query(
        &f1.rec(t, RecoveryEvent::FenceProven(Box::new(proof())))
    ));
    for copy in [A, B, C] {
        f1.report(t + 10, inv(copy, 20));
    }
    let version = armed_version(&f1.log);
    f1.step(t + WINDOW, fired(version));
    for copy in [A, B, C] {
        f1.rec(t + WINDOW + 100, durable(copy, 20, dg(0, 20)));
    }
    assert_eq!(
        f1.step(
            t + WINDOW + 200,
            cas_result(CasOutcome::Conflict {
                exists: true,
                current: Revision(6),
            }),
        ),
        vec![EffectKind::Control(ControlEffect::Get {
            request: LATEST,
            key: ControlKey::Partition(PARTITION),
        })],
        "fixture: the second run re-reads"
    );
    assert_ne!(newest(&f1), abandoned, "M7B-239: a fresh id per request");

    let peer = PartitionRecord {
        owner: NodeId(2),
        owner_epoch: OwnerEpoch(3),
        ..record(A)
    };
    let before = f1.module.clone();
    assert_eq!(
        f1.step(
            t + WINDOW + 250,
            read_result_to(
                abandoned,
                ReadOutcome::Found {
                    revision: Revision(6),
                    value: peer.encode(),
                }
            )
        ),
        vec![ign(ReplicaIgnoreReason::UnmatchedCompletion)],
        "M7B-239: the first re-read's late answer is ignored, named"
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Proposing,
        "M7B-239: nothing decided"
    );
    assert_eq!(
        f1.module, before,
        "M7B-239: the whole module is unchanged, the plan included"
    );

    assert_eq!(
        f1.step(
            t + WINDOW + 300,
            read_result(ReadOutcome::Absent { as_of: Revision(6) })
        ),
        vec![block(BlockReason::CasContention)],
        "M7B-239: the second re-read's own answer decides the second run"
    );
}

/// M7B-239, the activation half (the tester's F2): the activation CAS's `Conflict` re-read is
/// matched by its own id too. [`activating`] has the activation CAS in flight; it answers
/// `Conflict` and F1 re-reads. A peer's record echoing an earlier request of this instance — the
/// activation CAS's id, then the first id this instance minted — is `UnmatchedCompletion` each
/// time, and the module does not move. The re-read's own answer, the same peer record, then
/// decides the run: `OvertakenByPeer`, as in M7B-126.
#[retcd_test]
fn m7b_239_an_earlier_requests_answer_is_not_the_activation_reread() {
    let (mut f1, _) = activating();
    let activation = newest(&f1);
    let conflict = CasOutcome::Conflict {
        exists: true,
        current: Revision(12),
    };
    assert_eq!(
        f1.step(4_200, cas_result(conflict)),
        vec![EffectKind::Control(ControlEffect::Get {
            request: LATEST,
            key: ControlKey::Partition(PARTITION),
        })],
        "fixture: the activation conflict re-reads"
    );
    let reread = newest(&f1);
    let root = f1.requests[0];
    assert_ne!(reread, activation, "M7B-239: a fresh id per request");
    assert_ne!(reread, root, "M7B-239: a fresh id per request");

    let peer = PartitionRecord {
        owner: NodeId(2),
        owner_epoch: OwnerEpoch(3),
        ..record(A)
    };
    let found = || ReadOutcome::Found {
        revision: Revision(12),
        value: peer.encode(),
    };
    let before = f1.module.clone();
    for (tick, earlier) in [(4_250, activation), (4_260, root)] {
        assert_eq!(
            f1.step(tick, read_result_to(earlier, found())),
            vec![ign(ReplicaIgnoreReason::UnmatchedCompletion)],
            "M7B-239: an answer echoing an earlier request is not the re-read's"
        );
        assert_eq!(
            f1.module, before,
            "M7B-239: the whole module is unchanged, the plan included"
        );
    }
    assert_eq!(f1.phase(), RecoveryPhase::ActivationProposed);

    assert_eq!(
        f1.step(4_300, read_result(found())),
        vec![block(BlockReason::OvertakenByPeer)],
        "M7B-239: the re-read's own answer decides the run"
    );
}

// ---------------------------------------------------------------------------------------------
// Issue #2 (F003; Gautam 2026-10-02, supersedes B-R74b's "no timer bounds it"): every CAS
// exchange F1 opens is bounded by the discovery timer (M7B-240).
// ---------------------------------------------------------------------------------------------

/// M7B-240 (issue #2, Gautam 2026-10-02; supersedes B-R74b's unbounded hold): the recovery CAS
/// and the activation CAS each arm the discovery timer when they are sent. If no answer has come
/// by its deadline (ADR 0008 item 8, `DropCompletion`), the expiry is read as `CasOutcome::Unknown`:
/// `BlockPromotion{ControlUnknown}`, and a fence held behind the activation re-enters in the same
/// step, as M7B-230's `Unknown` arm does. A completion arriving after that is the late answer to
/// an exchange already decided: `UnmatchedCompletion`, nothing moves. A fire before the deadline,
/// or of an earlier version, is still `StaleTimer`. A conflict's re-read is inside the same bound.
#[retcd_test]
fn m7b_240_a_lost_cas_completion_is_bounded_by_the_discovery_timer() {
    let stale = vec![ign(ReplicaIgnoreReason::StaleTimer)];

    // Activation, with a newer fence held behind it.
    let (fresh_steps, fresh_result) = fresh_second_run();
    let sent = activation_proposed();
    let deadline = 4_100 + WINDOW;
    assert_eq!(
        sent.log.last(),
        Some(&arm(5, deadline)),
        "M7B-240: the activation CAS arms the timer"
    );
    let mut f1 = held_behind_activation();
    let t = REFENCE_AT;
    assert_eq!(f1.step(t, fired(4)), stale, "M7B-240: an earlier version");
    let mut early = activation_proposed();
    assert_eq!(
        early.step(deadline - 1, fired(5)),
        stale,
        "M7B-240: before the deadline"
    );
    assert_eq!(early.phase(), RecoveryPhase::ActivationProposed);
    let mut expected = vec![block(BlockReason::ControlUnknown)];
    expected.extend(re_entry(t));
    assert_eq!(
        f1.step(t, fired(5)),
        expected,
        "M7B-240: expiry is Unknown, and the held fence re-enters"
    );
    assert_eq!(f1.phase(), RecoveryPhase::Fenced);
    f1.ignores_late(t + 1, cas_result(CasOutcome::Committed(Revision(11))));
    let (steps, result) = second_run(&mut f1);
    assert_eq!(steps, fresh_steps, "M7B-240: the second run is a fresh one");
    assert_eq!(result, fresh_result, "M7B-240");

    // Activation with nothing held: blocked, and only a fresh fence moves it.
    let mut f1 = activation_proposed();
    assert_eq!(
        f1.step(deadline, fired(5)),
        vec![block(BlockReason::ControlUnknown)]
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Blocked(BlockReason::ControlUnknown)
    );
    f1.ignores_late(
        deadline + 1,
        cas_result(CasOutcome::Committed(Revision(11))),
    );

    // The recovery CAS.
    let mut f1 = proposing(20);
    let deadline = 3_000 + WINDOW;
    assert_eq!(
        f1.log.last(),
        Some(&arm(3, deadline)),
        "M7B-240: the recovery CAS arms the timer"
    );
    assert_eq!(f1.step(deadline - 1, fired(3)), stale);
    assert_eq!(f1.step(deadline, fired(2)), stale);
    assert_eq!(f1.phase(), RecoveryPhase::Proposing);
    assert_eq!(
        f1.step(deadline, fired(3)),
        vec![block(BlockReason::ControlUnknown)],
        "M7B-240: expiry is Unknown"
    );
    assert_eq!(
        f1.phase(),
        RecoveryPhase::Blocked(BlockReason::ControlUnknown)
    );
    f1.ignores_late(deadline + 1, cas_result(CasOutcome::Committed(Revision(9))));
    assert_eq!(f1.cas_count(), 1, "M7B-240: never re-proposed");

    // A conflict's re-read is inside the same exchange and the same bound.
    let mut f1 = proposing(20);
    f1.step(
        3_100,
        cas_result(CasOutcome::Conflict {
            exists: true,
            current: Revision(6),
        }),
    );
    assert_eq!(
        f1.step(deadline, fired(3)),
        vec![block(BlockReason::ControlUnknown)],
        "M7B-240: an unanswered re-read is bounded too"
    );
    f1.ignores_late(
        deadline + 1,
        read_result(ReadOutcome::Absent { as_of: Revision(6) }),
    );
}
