//! L1 lag protection, driven through `Module::step` only.
//!
//! `m7b_<n>_*` functions are the unit-class L1 rows of `docs/testing/test-plan-m7-kernel-b.md`
//! §7 and §9 (written after tester-kb-l1's thumbs-up). Plain-named functions are not plan rows:
//! supporting guards, the manual tester's rows, and the lead rulings' rows. The sim-class rows
//! are not here: M7B-67 and M7B-147 are in `rdb-sim/tests/harness.rs`; M7B-68 and M7B-78 wait on
//! the storage faults (`StallFlush`, `FailFlush`) in the loop.
//!
//! Every instance is made live the way production makes it: a `Recovered` naming this node the
//! primary. No test reaches into the state; every assertion reads a public accessor or the
//! returned effect vector.

use bytes::Bytes;
use config_log::retcd_test;

use rdb_core::authority::AuthorityTimer;
use rdb_core::contracts::authority::{
    AuthorityView, BlockReason, DenyReason, FencingProof, Lineage, PartitionMode, Revocation,
};
use rdb_core::contracts::control::{CasOutcome, ControlEvent, ControlKey};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{
    Budgets, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, StepCtx,
};
use rdb_core::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, ControlRequestId, CorrelationId, DurableSeq,
    EventId, Generation, GrantId, NodeId, OwnerEpoch, PartitionId, ReplicaRole, Revision, Seq,
    SnapshotHandle, TimerId, TimerVersion,
};
use rdb_core::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::protection::{AdmissionState, ReplicationLag};
use rdb_core::contracts::qualification::{
    QualificationCause, QualificationChanged, QualificationDirection,
};
use rdb_core::contracts::recovery::{
    CommittedRoot, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap, SelectedLineage,
};
use rdb_core::contracts::storage::{Namespace, SnapshotRead};
use rdb_core::contracts::time::{ControlTime, Tick, TimerFired};
use rdb_core::contracts::trace::{ProtectionPhase, Version};
use rdb_core::protection::{Mode, Protection, HEALTH_EVAL_TIMER, PROTECTION_TIMER_BASE};

// ---------------------------------------------------------------------------------------------
// Fixture: primary A, regular B and C, shadow D; one predicate at config 1 (plan §7 golden).
// ---------------------------------------------------------------------------------------------

const PARTITION: PartitionId = PartitionId(1);
const NODE_A: NodeId = NodeId(1);
const A: CopyId = CopyId(1);
const B: CopyId = CopyId(2);
const C: CopyId = CopyId(3);
const D: CopyId = CopyId(4);
const C1: ConfigVersion = ConfigVersion(1);
const GEN: Generation = Generation(7);
const BUDGETS: Budgets = Budgets::SPEC_DEFAULTS;

/// The tick at which [`golden`] reaches `Healthy`: the 5 s hold after the first eval.
const T0: u64 = 5_100;

/// A1 and every L1 input arrive as events; nothing reads through the snapshot.
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

fn ctx(now: u64, node: NodeId) -> StepCtx<'static> {
    StepCtx {
        now: Tick(now),
        control_time: ControlTime {
            estimate: Tick(now),
            error_millis: 0,
            bound_established: true,
            sampled_at: Tick(now),
        },
        node,
        boot: BootId(1),
        partition: PARTITION,
        generation: GEN,
        owner_epoch: OwnerEpoch(1),
        config_version: C1,
        snapshot: &SNAPSHOT,
        budgets: &BUDGETS,
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

/// `{A primary, B, C regular}` plus any extra members, at `version`.
fn config(version: ConfigVersion, extra: &[Member]) -> PartitionConfig {
    let mut members = vec![
        member(A, ReplicaRole::Primary),
        member(B, ReplicaRole::RegularSecondary),
        member(C, ReplicaRole::RegularSecondary),
    ];
    members.extend_from_slice(extra);
    PartitionConfig::new(PARTITION, version, members)
}

/// The plan fixture at config 1 with the primary slot on node 99: a `Recovered` pinning it
/// names another node the primary, so it leaves node A inert or demotes it.
fn elsewhere() -> PartitionConfig {
    let mut pinned = config(C1, &[]);
    pinned.members[0].node = NodeId(99);
    pinned
}

fn lineage() -> Lineage {
    Lineage {
        partition: PARTITION,
        generation: GEN,
        owner_epoch: OwnerEpoch(1),
    }
}

/// A recovery that pinned `pinned` and cut at `cutoff`, under generation `generation`.
fn recovered(pinned: PartitionConfig, cutoff: u64, generation: Generation) -> KernelEvent {
    let cutoff = Seq(cutoff);
    KernelEvent::Recovered(Box::new(RecoveryResult {
        fenced_prior: FencingProof {
            partition: PARTITION,
            prior_generation: Generation(generation.0 - 1),
            prior_owner_epoch: OwnerEpoch(1),
            prior_grant_id: GrantId(1),
            prior_boot_id: BootId(1),
            revocation: Revocation::DurableDrain {
                ack_revision: Revision(1),
            },
            control_revision: Revision(1),
            decision_tick: Tick::ZERO,
        },
        inventories: Vec::new(),
        selected: SelectedLineage {
            root: lineage(),
            cutoff_seq: cutoff,
            cutoff_digest: Digest::ROOT,
            source: A,
        },
        new_generation: generation,
        mode: PartitionMode::Active,
        barrier: RecoveryBarrier::try_new(&[], &Default::default(), cutoff, Digest::ROOT)
            .expect("an empty required set needs no proof"),
        loss: LossRecord {
            queried: Vec::new(),
            unavailable: Vec::new(),
            cutoff_seq: cutoff,
            highest_advertised_seq: cutoff,
            uncertain: false,
        },
        committed: CommittedRoot {
            revision: Revision(2),
            authority_view: AuthorityView {
                lineage: lineage(),
                grant_id: GrantId(2),
                boot_id: BootId(1),
                authority_generation: AuthorityGeneration(1),
                config_version: pinned.config_version,
                authority_seq: 1,
                valid_through_tick: Tick(u64::MAX),
                past_horizon: DenyReason::NoGrant,
            },
            pinned_config: pinned,
        },
        retained_status_map: RetainedStatusMap {
            predecessor_generation: Generation(generation.0 - 1),
            predecessor_cutoff: cutoff,
            retained_through: cutoff,
            discarded_from: None,
            uncertain: false,
        },
    }))
}

fn edge(direction: QualificationDirection) -> KernelEvent {
    KernelEvent::QualificationChanged(QualificationChanged {
        lineage: lineage(),
        config_version: C1,
        at_seq: Seq(0),
        direction,
        qualified_copies: vec![B],
        qualified_ack_count: 1,
        cause: QualificationCause::AckAdvanced,
        tick: Tick::ZERO,
    })
}

fn applied(seq: u64, bytes: u64) -> KernelEvent {
    KernelEvent::LocalApplied {
        seq: Seq(seq),
        bytes,
        // L1 does not read the digest (B-R47).
        record_digest: Digest::ROOT,
    }
}

fn durable(per_predicate: &[(ConfigVersion, u64)]) -> KernelEvent {
    KernelEvent::DurableAdvanced {
        per_predicate: per_predicate
            .iter()
            .map(|(v, s)| (*v, DurableSeq(*s)))
            .collect(),
    }
}

fn progress(copy: CopyId) -> KernelEvent {
    KernelEvent::PeerProgress {
        peer: NodeId(u32::from(copy.0)),
        contiguous_seq: Seq(0),
    }
}

fn divergence() -> BlockReason {
    BlockReason::DivergenceRequiresOperator {
        diverged: vec![C, B],
    }
}

/// Steps `kind` on node A at `now` and returns the kernel effects, checking the envelope.
fn try_step(p: &mut Protection, now: u64, kind: EventKind) -> Result<Vec<KernelEffect>, RdbError> {
    try_step_on(p, PARTITION, now, kind)
}

/// [`try_step`] for an event addressed to `partition`.
fn try_step_on(
    p: &mut Protection,
    partition: PartitionId,
    now: u64,
    kind: EventKind,
) -> Result<Vec<KernelEffect>, RdbError> {
    let event = Event {
        id: EventId(now),
        at: Tick(now),
        node: NODE_A,
        boot: BootId(1),
        partition,
        correlation: CorrelationId(42),
        kind,
    };
    let ctx = StepCtx {
        partition,
        ..ctx(now, NODE_A)
    };
    let effects = p.step(&ctx, &event)?;
    assert!(!effects.is_empty(), "BA-2: never an empty effect vector");
    Ok(effects
        .into_iter()
        .map(|effect| {
            assert_eq!(effect.correlation, CorrelationId(42));
            assert_eq!(effect.partition, partition);
            match effect.kind {
                EffectKind::Kernel(kind) => kind,
                other => panic!("L1 emits kernel effects only, got {other:?}"),
            }
        })
        .collect())
}

fn step(p: &mut Protection, now: u64, input: KernelEvent) -> Vec<KernelEffect> {
    try_step(p, now, EventKind::Kernel(input)).expect("an L1 input is never refused")
}

/// Design's `HealthEval{now}`: L1's timer fired, `now` read from `ctx.now` (T-B-02).
fn health(p: &mut Protection, now: u64) -> Vec<KernelEffect> {
    try_step(
        p,
        now,
        EventKind::Timer(TimerFired {
            id: HEALTH_EVAL_TIMER,
            version: TimerVersion(0),
            // Deliberately not `now`: L1 must never read the firing's stamp as the time.
            scheduled_at: Tick::ZERO,
        }),
    )
    .expect("the health timer is L1's")
}

fn ignored(reason: ReplicaIgnoreReason) -> Vec<KernelEffect> {
    vec![KernelEffect::Ignored {
        reason: KernelIgnoredReason::Replica(reason),
    }]
}

fn state(p: &Protection, now: u64) -> AdmissionState {
    p.admission_state(Tick(now)).expect("a live instance")
}

/// The single `SetAdmission` payload in `effects`.
fn admission(effects: &[KernelEffect]) -> &AdmissionState {
    match effects {
        [KernelEffect::SetAdmission(s)] => s,
        other => panic!("expected exactly one SetAdmission, got {other:?}"),
    }
}

fn no_allow(effects: &[KernelEffect]) -> bool {
    !effects
        .iter()
        .any(|e| matches!(e, KernelEffect::SetAdmission(s) if s.allow))
}

/// A fresh primary instance: `Recovered{cutoff}` on node A.
fn fresh(cutoff: u64) -> Protection {
    let mut p = Protection::new();
    let effects = step(&mut p, 0, recovered(config(C1, &[]), cutoff, GEN));
    let s = admission(&effects);
    assert!(!s.allow);
    assert_eq!(s.reason, Some(ErrorKind::ProtectionPaused));
    p
}

/// Plan §7's golden fixture: `Healthy` with an empty queue and a qualifying secondary, reached
/// through the real resume path at tick [`T0`].
fn golden() -> Protection {
    let mut p = fresh(0);
    step(&mut p, 0, edge(QualificationDirection::Gained));
    assert_eq!(health(&mut p, 0), ignored(ReplicaIgnoreReason::ResumeHeld));
    assert_eq!(p.mode(), Some(Mode::Reprotecting { below_since: None }));
    for t in (100..=T0).step_by(100) {
        step(&mut p, t, progress(B));
        step(&mut p, t, progress(C));
        let effects = health(&mut p, t);
        if t < T0 {
            assert_eq!(effects, ignored(ReplicaIgnoreReason::ResumeHeld), "t={t}");
        } else {
            assert!(admission(&effects).allow);
        }
    }
    assert_eq!(p.mode(), Some(Mode::Healthy));
    p
}

// ---------------------------------------------------------------------------------------------
// Slice 1: reach, warn and pause.
// ---------------------------------------------------------------------------------------------

/// M7B-82: an inert (secondary) instance answers every input `NotPrimary` and keeps nothing; a
/// `Recovered` naming another node leaves it inert, and one promoting it builds a fresh instance
/// with no inherited exposure.
#[retcd_test]
fn m7b_82_protection_is_fresh_at_promotion_and_inert_on_secondaries() {
    let not_primary = vec![KernelEffect::Ignored {
        reason: KernelIgnoredReason::Error(ErrorKind::NotPrimary),
    }];
    let mut p = Protection::default();
    for seq in 1..=3 {
        assert_eq!(step(&mut p, 0, applied(seq, 100)), not_primary);
    }
    assert_eq!(health(&mut p, 5_000), not_primary);

    assert_eq!(
        step(&mut p, 5_000, recovered(elsewhere(), 3, GEN)),
        not_primary
    );
    assert_eq!(p.mode(), None);
    assert_eq!(p.unsafe_len(), 0);
    assert_eq!(p.admission_state(Tick(5_000)), None);

    let effects = step(&mut p, 5_000, recovered(config(C1, &[]), 3, GEN));
    assert!(!admission(&effects).allow);
    assert_eq!(p.mode(), Some(Mode::Paused));
    assert_eq!(p.unsafe_len(), 0, "no inherited exposure");
    let s = state(&p, 5_000);
    assert_eq!((s.paused_prefix, s.resume_barrier), (Seq(3), Seq(3)));
    assert_eq!(s.required_config_versions, vec![C1]);
}

/// Anything that is not L1's timer or an L1 kernel input is refused `Unavailable`, live or
/// inert. The timer block is disjoint from A1's.
#[retcd_test]
fn foreign_timers_and_events_are_refused() {
    assert_eq!(HEALTH_EVAL_TIMER, TimerId(PROTECTION_TIMER_BASE));
    for kind in AuthorityTimer::ALL {
        assert_ne!(kind.id(), HEALTH_EVAL_TIMER, "{kind:?}");
    }
    for mut p in [Protection::new(), golden()] {
        let timer = EventKind::Timer(TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: TimerVersion(0),
            scheduled_at: Tick::ZERO,
        });
        let echo = EventKind::Kernel(KernelEvent::SetAdmission(state(&golden(), 0)));
        // A control event, as the sim's `harness::run` tests seed: not an L1 input.
        let control = EventKind::Control(ControlEvent::CasResult {
            request: ControlRequestId(1),
            key: ControlKey::ClusterSchema,
            outcome: CasOutcome::Conflict {
                exists: true,
                current: Revision(1),
            },
        });
        for kind in [timer, echo, control] {
            match try_step(&mut p, T0, kind) {
                Err(RdbError::Unavailable { .. }) => {}
                other => panic!("expected Unavailable, got {other:?}"),
            }
        }
    }
}

/// M7B-64: an idle partition is never unsafe.
#[retcd_test]
fn m7b_64_idle_partition_is_never_unsafe() {
    let mut p = golden();
    let now = 10_000;
    assert_eq!(
        health(&mut p, now),
        ignored(ReplicaIgnoreReason::NothingOutstanding)
    );
    assert_eq!(state(&p, now).oldest_unsafe_age, 0);
    assert_eq!(p.mode(), Some(Mode::Healthy));
}

/// M7B-65: warn at exactly 1000 ms, not at 999. The golden fixture is `Healthy` at [`T0`], so
/// the row's ticks are offsets from it.
#[retcd_test]
fn m7b_65_warn_at_exactly_1000_ms_not_before() {
    let mut p = golden();
    assert_eq!(
        step(&mut p, T0, applied(1, 100)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert_eq!(
        health(&mut p, T0 + 999),
        ignored(ReplicaIgnoreReason::Outstanding)
    );
    assert_eq!(p.mode(), Some(Mode::Healthy));
    assert_eq!(
        health(&mut p, T0 + 1_000),
        vec![KernelEffect::ProtectionWarn {
            oldest_unsafe_seq: Seq(1),
            age_ms: 1_000,
        }]
    );
    assert_eq!(p.mode(), Some(Mode::Warn));
    assert!(state(&p, T0 + 1_000).allow, "warn does not stop admission");
}

/// M7B-66 (kernel row): the pause lands in the same step that first sees 2000 ms.
#[retcd_test]
fn m7b_66_kernel_row_pause_in_the_same_step_that_sees_2000_ms() {
    let mut p = golden();
    step(&mut p, T0, applied(1, 100));
    health(&mut p, T0 + 1_000);
    assert_eq!(
        health(&mut p, T0 + 1_999),
        ignored(ReplicaIgnoreReason::Outstanding)
    );
    assert_eq!(p.mode(), Some(Mode::Warn));

    let effects = health(&mut p, T0 + 2_000);
    let s = admission(&effects);
    assert!(!s.allow);
    assert_eq!(s.reason, Some(ErrorKind::ProtectionPaused));
    assert_eq!((s.paused_prefix, s.resume_barrier), (Seq(1), Seq(1)));
    assert_eq!(p.mode(), Some(Mode::Paused));
}

/// A queue that jumps straight past the pause age pauses from `Healthy` without a warn step.
#[retcd_test]
fn healthy_pauses_directly_when_first_seen_past_2000_ms() {
    let mut p = golden();
    step(&mut p, T0, applied(1, 100));
    let effects = health(&mut p, T0 + 2_500);
    assert!(!admission(&effects).allow);
    assert_eq!(p.mode(), Some(Mode::Paused));
}

/// M7B-69: `Lost` pauses in the same step, whatever the age.
#[retcd_test]
fn m7b_69_lost_qualification_pauses_in_the_same_step_regardless_of_age() {
    let mut p = golden();
    step(&mut p, T0, applied(1, 100));
    let effects = step(&mut p, T0 + 5, edge(QualificationDirection::Lost));
    let s = admission(&effects);
    assert!(!s.allow);
    assert_eq!(s.reason, Some(ErrorKind::ProtectionPaused));
    assert_eq!(s.oldest_unsafe_age, 5);
    assert_eq!(p.mode(), Some(Mode::Paused));
    assert!(!p.qualifies_now_at_head());
    // A second `Lost` moves no admission edge.
    assert_eq!(
        step(&mut p, T0 + 6, edge(QualificationDirection::Lost)),
        ignored(ReplicaIgnoreReason::NoQualifyingSecondary)
    );
}

/// M7B-70: `direction` is the only field L1 reads.
#[retcd_test]
fn m7b_70_direction_is_the_only_field_l1_reads() {
    let other_lost = KernelEvent::QualificationChanged(QualificationChanged {
        qualified_copies: vec![C, B, A],
        qualified_ack_count: 0,
        cause: QualificationCause::DivergenceDetected(C),
        at_seq: Seq(99),
        tick: Tick(12_345),
        ..match edge(QualificationDirection::Lost) {
            KernelEvent::QualificationChanged(q) => q,
            _ => unreachable!(),
        }
    });
    let (mut one, mut two) = (golden(), golden());
    let a = step(&mut one, T0, edge(QualificationDirection::Lost));
    let b = step(&mut two, T0, other_lost);
    assert_eq!(a, b);
    assert_eq!(one, two);

    let mut gained = fresh(0);
    let zero_acks = KernelEvent::QualificationChanged(QualificationChanged {
        qualified_ack_count: 0,
        qualified_copies: Vec::new(),
        ..match edge(QualificationDirection::Gained) {
            KernelEvent::QualificationChanged(q) => q,
            _ => unreachable!(),
        }
    });
    step(&mut gained, 0, zero_acks);
    assert!(gained.qualifies_now_at_head());
}

/// M7B-81: `Warn` returns to `Healthy` when the queue drains below the warn age. The design
/// names no `ProtectionCleared` effect, so the step answers `Ignored` (recorded, per the row).
#[retcd_test]
fn m7b_81_warn_returns_to_healthy_when_age_drops_below_warn() {
    let mut p = golden();
    step(&mut p, T0, applied(1, 100));
    health(&mut p, T0 + 1_000);
    step(&mut p, T0 + 1_000, durable(&[(C1, 1)]));
    assert_eq!(
        health(&mut p, T0 + 1_001),
        ignored(ReplicaIgnoreReason::NothingOutstanding)
    );
    assert_eq!(p.mode(), Some(Mode::Healthy));
}

/// M7B-79: age and bytes are exported separately.
#[retcd_test]
fn m7b_79_age_and_bytes_are_exported_separately() {
    let mut p = golden();
    step(&mut p, T0, applied(1, 100));
    step(&mut p, T0 + 500, applied(2, 900));
    let s = state(&p, T0 + 1_500);
    assert_eq!(
        (
            s.oldest_unsafe_age,
            s.oldest_unsafe_seq,
            s.outstanding_unsafe_bytes
        ),
        (1_500, Seq(1), 1_000)
    );
    step(&mut p, T0 + 1_500, durable(&[(C1, 1)]));
    let s = state(&p, T0 + 1_500);
    assert_eq!(
        (
            s.oldest_unsafe_age,
            s.oldest_unsafe_seq,
            s.outstanding_unsafe_bytes
        ),
        (1_000, Seq(2), 900)
    );
}

/// M7B-71: a rename never resets the age.
#[retcd_test]
fn m7b_71_rename_never_resets_the_age() {
    let mut p = golden();
    step(&mut p, T0, applied(1, 100));
    health(&mut p, T0 + 1_000);
    assert_eq!(p.mode(), Some(Mode::Warn));
    let renamed = PartitionConfig::new(
        PARTITION,
        ConfigVersion(2),
        vec![
            member(A, ReplicaRole::Primary),
            member(B, ReplicaRole::RegularSecondary),
            member(CopyId(30), ReplicaRole::RegularSecondary),
        ],
    );
    assert_eq!(
        step(&mut p, T0 + 1_200, KernelEvent::ConfigChanged(renamed)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    health(&mut p, T0 + 1_200);
    assert_eq!(p.mode(), Some(Mode::Warn));
    let s = state(&p, T0 + 1_200);
    assert_eq!(s.oldest_unsafe_age, 1_200, "applied_at is still T0");
    assert_eq!(s.required_config_versions, vec![ConfigVersion(2), C1]);
    assert!(!admission(&health(&mut p, T0 + 2_000)).allow);
}

const C2: ConfigVersion = ConfigVersion(2);
const C3: ConfigVersion = ConfigVersion(3);

/// Plan §7's two-predicate fixture: seqs 1..=50 applied, predicates `[c+1, c]`, durable under
/// `c` through 10 and under `c+1` through 50.
fn two_predicates_durable_10_and_50() -> Protection {
    let mut p = golden();
    for seq in 1..=50 {
        step(&mut p, T0, applied(seq, 1));
    }
    step(&mut p, T0, KernelEvent::ConfigChanged(config(C2, &[])));
    step(&mut p, T0, durable(&[(C1, 10), (C2, 50)]));
    p
}

/// `TransitionBarrierConfirmed` for the old predicate `c`.
fn confirm_c1(through: u64) -> KernelEvent {
    KernelEvent::TransitionBarrierConfirmed {
        config_version: C1,
        through_seq: Seq(through),
    }
}

/// M7B-72: the queue drains through the minimum over every active predicate, so the old
/// predicate's floor holds until its barrier is confirmed; then it drains through the new one.
/// The retirement itself drains through the survivor's stored floor (lead ruling B-R46a, review
/// R2), so the next `DurableAdvanced` finds nothing left.
#[retcd_test]
fn m7b_72_old_predicate_floor_is_kept_until_its_barrier_is_confirmed() {
    let mut p = two_predicates_durable_10_and_50();
    assert_eq!(p.unsafe_len(), 40, "drained only through 10");
    step(&mut p, T0, confirm_c1(10));
    assert_eq!(p.unsafe_len(), 0, "retirement drains through the stored 50");
    step(&mut p, T0, durable(&[(C2, 50)]));
    assert_eq!(p.unsafe_len(), 0, "drained through 50");
}

/// M7B-73: a confirmed barrier retires the old predicate.
#[retcd_test]
fn m7b_73_confirmed_barrier_retires_the_old_predicate() {
    let mut p = two_predicates_durable_10_and_50();
    assert_eq!(
        step(&mut p, T0, confirm_c1(10)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert_eq!(state(&p, T0).required_config_versions, vec![C2]);
}

/// M7B-74: an unconfirmed barrier does not retire; the twin of M7B-73 by one number.
#[retcd_test]
fn m7b_74_unconfirmed_barrier_does_not_retire() {
    let mut p = two_predicates_durable_10_and_50();
    assert_eq!(
        step(&mut p, T0, confirm_c1(60)),
        ignored(ReplicaIgnoreReason::BarrierNotDurable)
    );
    assert_eq!(state(&p, T0).required_config_versions, vec![C2, C1]);
}

/// The current predicate never retires, and an unknown one is refused.
#[retcd_test]
fn current_or_unknown_predicate_does_not_retire() {
    let mut p = golden();
    for version in [C1, ConfigVersion(9)] {
        let effects = step(
            &mut p,
            T0,
            KernelEvent::TransitionBarrierConfirmed {
                config_version: version,
                through_seq: Seq(0),
            },
        );
        assert_eq!(effects, ignored(ReplicaIgnoreReason::InvalidConfig));
    }
    assert_eq!(state(&p, T0).required_config_versions, vec![C1]);
}

/// M7B-80: between `now` and `next_interesting_tick()` no health eval changes the state, and
/// at it the state changes. "Unchanged" is the mode and the admission verdict: every eval before
/// `h` answers `Ignored{Outstanding}`. `AdmissionState::oldest_unsafe_age` moves with `ctx.now`
/// by definition, so it is not compared.
#[retcd_test]
fn m7b_80_next_interesting_tick_is_sound_in_healthy_warn_and_idle() {
    let mut idle = golden();
    assert_eq!(idle.next_interesting_tick(), None);

    let mut p = golden();
    step(&mut p, T0, applied(1, 100));
    let mut now = T0;
    for (expected_before, expected_after) in
        [(Mode::Healthy, Mode::Warn), (Mode::Warn, Mode::Paused)]
    {
        let h = p.next_interesting_tick().expect("a front entry").0;
        for t in now..h {
            assert_eq!(
                health(&mut p, t),
                ignored(ReplicaIgnoreReason::Outstanding),
                "t={t}"
            );
            assert_eq!(p.mode(), Some(expected_before), "t={t}");
        }
        health(&mut p, h);
        assert_eq!(p.mode(), Some(expected_after), "h={h}");
        now = h + 1;
    }
    assert_eq!(
        p.next_interesting_tick(),
        None,
        "Paused waits on events, not time"
    );
    assert_eq!(
        health(&mut idle, T0 + 1),
        ignored(ReplicaIgnoreReason::NothingOutstanding)
    );
}

// ---------------------------------------------------------------------------------------------
// Slice 2: resume with hysteresis.
// ---------------------------------------------------------------------------------------------

/// A `Paused{40}` instance with two predicates, qualifying, nothing durable yet.
fn paused_at_40_with_two_predicates() -> Protection {
    let mut p = golden();
    for seq in 1..=40 {
        step(&mut p, T0, applied(seq, 1));
    }
    step(
        &mut p,
        T0,
        KernelEvent::ConfigChanged(config(ConfigVersion(2), &[])),
    );
    assert!(!admission(&health(&mut p, T0 + 2_000)).allow);
    assert_eq!(state(&p, T0 + 2_000).resume_barrier, Seq(40));
    p
}

/// M7B-75: `Paused -> Reprotecting` needs the exact barrier on every predicate.
#[retcd_test]
fn m7b_75_paused_to_reprotecting_needs_the_exact_barrier_on_every_predicate() {
    let now = T0 + 2_100;
    let mut p = paused_at_40_with_two_predicates();
    step(&mut p, now, durable(&[(C1, 40), (ConfigVersion(2), 39)]));
    assert_eq!(
        health(&mut p, now),
        ignored(ReplicaIgnoreReason::BarrierNotDurable)
    );
    assert_eq!(p.mode(), Some(Mode::Paused));

    step(&mut p, now, durable(&[(C1, 40), (ConfigVersion(2), 40)]));
    let effects = health(&mut p, now);
    assert_eq!(effects, ignored(ReplicaIgnoreReason::ResumeHeld));
    assert!(no_allow(&effects));
    assert_eq!(p.mode(), Some(Mode::Reprotecting { below_since: None }));
}

/// M7B-76: the barrier without a qualifying secondary stays `Paused`.
#[retcd_test]
fn m7b_76_barrier_without_qualification_stays_paused() {
    let now = T0 + 2_100;
    let mut p = paused_at_40_with_two_predicates();
    step(&mut p, now, edge(QualificationDirection::Lost));
    step(&mut p, now, durable(&[(C1, 40), (ConfigVersion(2), 40)]));
    assert_eq!(
        health(&mut p, now),
        ignored(ReplicaIgnoreReason::NoQualifyingSecondary)
    );
    assert_eq!(p.mode(), Some(Mode::Paused));
}

/// M7B-77: a new predicate not durable through the barrier sends `Reprotecting` back to
/// `Paused` at the highest applied seq.
#[retcd_test]
fn m7b_77_barrier_invalidated_during_reprotecting_returns_to_paused() {
    let now = T0 + 2_100;
    let mut p = paused_at_40_with_two_predicates();
    step(&mut p, now, durable(&[(C1, 40), (ConfigVersion(2), 40)]));
    health(&mut p, now);
    assert_eq!(p.mode(), Some(Mode::Reprotecting { below_since: None }));
    for seq in 41..=45 {
        step(&mut p, now, applied(seq, 1));
    }
    step(
        &mut p,
        now,
        KernelEvent::ConfigChanged(config(ConfigVersion(3), &[])),
    );
    step(&mut p, now, durable(&[(ConfigVersion(3), 30)]));
    assert_eq!(
        health(&mut p, now),
        ignored(ReplicaIgnoreReason::BarrierNotDurable)
    );
    assert_eq!(p.mode(), Some(Mode::Paused));
    let s = state(&p, now);
    assert!(!s.allow);
    assert_eq!(s.resume_barrier, Seq(45));
}

/// An instance in `Reprotecting{below_since: None}` at `start`, barrier 0, domain `{B, C}`, no
/// peer heard from since construction.
fn reprotecting_at(start: u64) -> Protection {
    let mut p = fresh(0);
    step(&mut p, start, edge(QualificationDirection::Gained));
    health(&mut p, start);
    assert_eq!(p.mode(), Some(Mode::Reprotecting { below_since: None }));
    p
}

/// Drives B and C (per `report`) every 100 ms and a health eval every 100 ms over
/// `(from, to]`; returns the tick of the first `SetAdmission(Allow)`, if any.
fn run(
    p: &mut Protection,
    from: u64,
    to: u64,
    report: impl Fn(CopyId, u64) -> bool,
) -> Option<u64> {
    for t in (from + 100..=to).step_by(100) {
        for copy in [B, C] {
            if report(copy, t) {
                step(p, t, progress(copy));
            }
        }
        let effects = health(p, t);
        if !no_allow(&effects) {
            return Some(t);
        }
    }
    None
}

/// M7B-129: resume needs 5 s of lag below 250 ms after the exact barrier; one 300 ms gap
/// restarts the hold.
#[retcd_test]
fn m7b_129_resume_needs_5_s_of_peer_progress_below_250_ms_after_the_exact_barrier() {
    let mut p = reprotecting_at(10_000);
    step(&mut p, 10_000, progress(B));
    step(&mut p, 10_000, progress(C));
    assert_eq!(run(&mut p, 10_000, 10_100, |_, _| true), None);
    assert_eq!(
        p.mode(),
        Some(Mode::Reprotecting {
            below_since: Some(Tick(10_100))
        })
    );
    assert_eq!(p.next_interesting_tick(), Some(Tick(15_100)));
    assert_eq!(run(&mut p, 10_100, 15_000, |_, _| true), None);
    assert_eq!(
        health(&mut p, 15_099),
        ignored(ReplicaIgnoreReason::ResumeHeld)
    );
    assert!(matches!(p.mode(), Some(Mode::Reprotecting { .. })));
    assert_eq!(run(&mut p, 15_000, 15_100, |_, _| true), Some(15_100));
    assert_eq!(p.mode(), Some(Mode::Healthy));

    // Twin: C silent over 12_000..12_300 -> lag 300 at 12_300 restarts the hold.
    let mut twin = reprotecting_at(10_000);
    step(&mut twin, 10_000, progress(B));
    step(&mut twin, 10_000, progress(C));
    let gap = |copy: CopyId, t: u64| copy == B || !(12_100..=12_300).contains(&t);
    let resumed = run(&mut twin, 10_000, 30_000, gap).expect("resumes after the gap");
    assert!(resumed >= 17_100, "resumed at {resumed}");
}

/// M7B-130: a peer never heard from has infinite lag and blocks resume. Twin: C reporting from
/// 30_000 resumes at 35_000 exactly, the 5 s hold.
#[retcd_test]
fn m7b_130_never_heard_peer_has_infinite_lag_and_blocks_resume() {
    let mut p = reprotecting_at(10_000);
    let only_b = |copy: CopyId, _| copy == B;
    assert_eq!(run(&mut p, 10_000, 60_000, only_b), None);
    let s = state(&p, 60_000);
    assert_eq!(s.replication_lag, ReplicationLag::INFINITE);
    assert_eq!(s.stalest_copy, Some(C));

    let mut twin = reprotecting_at(10_000);
    let c_heard_from_30_000 = |copy: CopyId, t: u64| copy == B || t >= 30_000;
    assert_eq!(
        run(&mut twin, 10_000, 60_000, c_heard_from_30_000),
        Some(35_000)
    );
}

/// ADR-rdb-0006 row "a peer never heard from blocks resume", as corrected on 2026-09-26 (lead
/// ruling B-R60). Near-miss: one `PeerProgress` per peer in `Reprotecting` starts the hold, and
/// 250 ms later `replication_lag` reaches 250 ms and the hold restarts, so the partition never
/// resumes. Twin: the same peers reporting every 250 ms, as R1's keepalive will, resume 5 s
/// after the first report.
#[retcd_test]
fn a_single_peer_progress_does_not_resume_and_a_stream_every_250_ms_does() {
    let mut p = reprotecting_at(10_000);
    step(&mut p, 10_000, progress(B));
    step(&mut p, 10_000, progress(C));
    health(&mut p, 10_000);
    let started = Mode::Reprotecting {
        below_since: Some(Tick(10_000)),
    };
    assert_eq!(p.mode(), Some(started));
    assert_eq!(
        health(&mut p, 10_200),
        ignored(ReplicaIgnoreReason::ResumeHeld)
    );
    assert_eq!(p.mode(), Some(started), "lag 200 keeps the hold");
    health(&mut p, 10_250);
    assert_eq!(
        p.mode(),
        Some(Mode::Reprotecting { below_since: None }),
        "lag 250 restarts the hold"
    );
    for t in (10_300..=70_000).step_by(50) {
        assert!(no_allow(&health(&mut p, t)), "t={t}");
    }
    assert_eq!(p.mode(), Some(Mode::Reprotecting { below_since: None }));
    let s = state(&p, 70_000);
    assert!(!s.allow);
    assert_eq!(s.replication_lag, ReplicationLag::millis(60_000));

    let mut twin = reprotecting_at(10_000);
    let mut allowed = None;
    for t in (10_000..=20_000).step_by(50) {
        if (t - 10_000) % 250 == 0 {
            step(&mut twin, t, progress(B));
            step(&mut twin, t, progress(C));
        }
        if !no_allow(&health(&mut twin, t)) {
            allowed = Some(t);
            break;
        }
    }
    assert_eq!(allowed, Some(15_000), "5 s after the first report");
    assert_eq!(twin.mode(), Some(Mode::Healthy));
}

/// M7B-131: self is not in the lag domain, a shadow never is, and `CopyLost` shrinks it so
/// resume proceeds on the remaining peer alone.
#[retcd_test]
fn m7b_131_self_is_not_in_the_lag_domain_and_copy_lost_shrinks_it() {
    let mut p = Protection::new();
    step(
        &mut p,
        0,
        recovered(config(C1, &[member(D, ReplicaRole::Shadow)]), 0, GEN),
    );
    assert_eq!(p.lag_domain(), vec![B, C]);
    step(&mut p, 0, KernelEvent::CopyLost { copy: C });
    assert_eq!(p.lag_domain(), vec![B]);
    assert_eq!(state(&p, 0).lost_copies, vec![C]);

    step(&mut p, 0, edge(QualificationDirection::Gained));
    health(&mut p, 0);
    let only_b = |copy: CopyId, _| copy == B;
    assert_eq!(run(&mut p, 0, 10_000, only_b), Some(5_100));
}

/// A `PeerProgress` from a node in no member slot is refused, not stored.
#[retcd_test]
fn progress_from_an_unknown_node_is_invalid_config() {
    let mut p = golden();
    let stranger = KernelEvent::PeerProgress {
        peer: NodeId(77),
        contiguous_seq: Seq(1),
    };
    assert_eq!(
        step(&mut p, T0, stranger),
        ignored(ReplicaIgnoreReason::InvalidConfig)
    );
}

/// M7B-132: the `Reprotecting` arm of `next_interesting_tick` is `below_since + resume_hold`;
/// with peers reporting, no health eval before it changes the state. `below_since None` has no
/// hint (recorded; lead ruling B-R38 on F3 scopes the soundness property to M7B-80's modes).
#[retcd_test]
fn m7b_132_next_interesting_tick_reprotecting_arm() {
    let mut p = reprotecting_at(10_000);
    assert_eq!(p.next_interesting_tick(), None, "below_since None");
    step(&mut p, 10_100, progress(B));
    step(&mut p, 10_100, progress(C));
    health(&mut p, 10_100);
    let held = Mode::Reprotecting {
        below_since: Some(Tick(10_100)),
    };
    assert_eq!(p.mode(), Some(held));
    assert_eq!(p.next_interesting_tick(), Some(Tick(15_100)));
    for t in 10_101..15_100 {
        if t % 100 == 0 {
            step(&mut p, t, progress(B));
            step(&mut p, t, progress(C));
        }
        assert_eq!(
            health(&mut p, t),
            ignored(ReplicaIgnoreReason::ResumeHeld),
            "t={t}"
        );
        assert_eq!(p.mode(), Some(held), "t={t}");
    }
    assert!(admission(&health(&mut p, 15_100)).allow);
    assert_eq!(p.mode(), Some(Mode::Healthy));
}

/// M7B-83: health evals alone never re-allow admission.
#[retcd_test]
fn m7b_83_health_eval_never_re_allows_admission_by_itself() {
    let mut p = fresh(10);
    step(&mut p, 0, edge(QualificationDirection::Gained));
    for t in (0..=60_000).step_by(50) {
        let effects = health(&mut p, t);
        assert!(no_allow(&effects), "t={t}");
        assert_eq!(p.mode(), Some(Mode::Paused), "t={t}");
    }
}

/// M7B-141: a fresh instance starts `Paused` at the `Recovered` cutoff and publishes the reject
/// at construction; no admission before the first `Gained` and the durable barrier, then
/// `Reprotecting` and the hold.
#[retcd_test]
fn m7b_141_protection_starts_paused_at_recovered_and_resumes_only_through_reprotecting() {
    let mut p = Protection::new();
    let effects = step(&mut p, 0, recovered(config(C1, &[]), 100, GEN));
    let s = admission(&effects);
    assert!(!s.allow);
    assert_eq!(s.reason, Some(ErrorKind::ProtectionPaused));
    assert_eq!((s.paused_prefix, s.resume_barrier), (Seq(100), Seq(100)));
    assert_eq!(s.required_config_versions, vec![C1]);
    assert_eq!(s.outstanding_unsafe_bytes, 0);
    assert_eq!(s.lost_copies, Vec::new());
    assert_eq!(p.mode(), Some(Mode::Paused));
    assert!(!p.qualifies_now_at_head());
    assert_eq!(p.blocked(), None);
    assert_eq!(p.unsafe_len(), 0);

    for t in 0..50 {
        assert_eq!(
            health(&mut p, t * 50),
            ignored(ReplicaIgnoreReason::BarrierNotDurable)
        );
    }
    step(&mut p, 2_500, durable(&[(C1, 100)]));
    assert_eq!(
        health(&mut p, 2_500),
        ignored(ReplicaIgnoreReason::NoQualifyingSecondary)
    );
    step(&mut p, 2_500, edge(QualificationDirection::Gained));
    health(&mut p, 2_500);
    assert_eq!(p.mode(), Some(Mode::Reprotecting { below_since: None }));
    assert_eq!(run(&mut p, 2_500, 20_000, |_, _| true), Some(7_600));
}

/// F1's activation re-emits `Recovered` for the same generation (design §5.6a); L1 is
/// mode-blind and keeps its state. A new generation rebuilds.
#[retcd_test]
fn activation_re_emission_keeps_the_instance_and_a_new_generation_rebuilds() {
    let mut p = golden();
    step(&mut p, T0, applied(1, 100));
    let before = p.clone();
    assert_eq!(
        step(&mut p, T0, recovered(config(C1, &[]), 0, GEN)),
        ignored(ReplicaIgnoreReason::NotRequired)
    );
    assert_eq!(p, before);

    let effects = step(
        &mut p,
        T0,
        recovered(config(C1, &[]), 1, Generation(GEN.0 + 1)),
    );
    assert!(!admission(&effects).allow);
    assert_eq!(p.mode(), Some(Mode::Paused));
    assert_eq!(p.unsafe_len(), 0, "no inherited exposure");
}

// ---------------------------------------------------------------------------------------------
// Slice 3: block.
// ---------------------------------------------------------------------------------------------

/// M7B-142: `BlockPartition` is L1's fourth input and reads as a block. (a) On a fresh
/// (`Paused`) instance the mode stays `Paused` at the cutoff; (b) on a `Healthy` one it pauses at
/// the head. In both: the reject names `DivergenceRequiresOperator`, `blocked` carries the
/// payload in the order given, `lost_copies` is untouched, 100 health ticks never clear it, and
/// a second block is exactly `[Ignored{AlreadyBlocked}]`.
#[retcd_test]
fn m7b_142_block_partition_is_l1s_fourth_input_and_reads_as_a_block() {
    // (a) applies past its cutoff first, so a block that re-paused at the head would move the
    // barrier to 12 and fail the barrier assertion (review A4).
    let mut paused = fresh(5);
    step(&mut paused, T0, applied(12, 1));
    let mut healthy = golden();
    step(&mut healthy, T0, applied(3, 10));
    for (mut p, barrier) in [(paused, Seq(5)), (healthy, Seq(3))] {
        let now = T0 + 1;
        let effects = step(&mut p, now, KernelEvent::BlockPartition(divergence()));
        let s = admission(&effects);
        assert!(!s.allow);
        assert_eq!(s.reason, Some(ErrorKind::DivergenceRequiresOperator));
        assert_eq!(s.lost_copies, Vec::new(), "a block is not a loss");
        assert_eq!((s.paused_prefix, s.resume_barrier), (barrier, barrier));
        assert_eq!(p.blocked(), Some(&divergence()));
        assert_eq!(p.mode(), Some(Mode::Paused));

        for t in 1..=100 {
            assert_eq!(
                health(&mut p, now + t * 50),
                ignored(ReplicaIgnoreReason::AlreadyBlocked)
            );
        }
        assert_eq!(p.blocked(), Some(&divergence()));
        assert_eq!(
            step(
                &mut p,
                now + 6_000,
                KernelEvent::BlockPartition(divergence())
            ),
            ignored(ReplicaIgnoreReason::AlreadyBlocked)
        );
    }
}

/// M7B-143: while blocked, `Paused` never reprotects, even after a config change, a `Gained`
/// and a satisfied barrier. The twin without the block does reprotect.
#[retcd_test]
fn m7b_143_paused_never_reprotects_while_blocked_even_after_config_change_and_gained() {
    let c2 = ConfigVersion(2);
    let with_d = config(c2, &[member(D, ReplicaRole::RegularSecondary)]);
    let drive = |blocked: bool| {
        let mut p = fresh(5);
        let mut effects = Vec::new();
        if blocked {
            effects.extend(step(&mut p, 0, KernelEvent::BlockPartition(divergence())));
        }
        effects.extend(step(&mut p, 0, KernelEvent::ConfigChanged(with_d.clone())));
        effects.extend(step(&mut p, 0, edge(QualificationDirection::Gained)));
        effects.extend(step(&mut p, 0, durable(&[(C1, 5), (c2, 5)])));
        effects.extend(health(&mut p, 0));
        (p, effects)
    };

    let (blocked, effects) = drive(true);
    assert_eq!(blocked.mode(), Some(Mode::Paused));
    assert!(no_allow(&effects));
    let s = state(&blocked, 0);
    assert!(!s.allow);
    assert_eq!(s.reason, Some(ErrorKind::DivergenceRequiresOperator));

    let (twin, _) = drive(false);
    assert_eq!(twin.mode(), Some(Mode::Reprotecting { below_since: None }));
}

/// A block while `Reprotecting` pauses; `Mode::phase` traces `Reprotecting` as `Resuming`.
#[retcd_test]
fn block_while_reprotecting_pauses_and_reprotecting_traces_as_resuming() {
    let mut p = reprotecting_at(0);
    assert_eq!(p.mode().map(Mode::phase), Some(ProtectionPhase::Resuming));
    let effects = step(&mut p, 0, KernelEvent::BlockPartition(divergence()));
    assert_eq!(
        admission(&effects).reason,
        Some(ErrorKind::DivergenceRequiresOperator)
    );
    assert_eq!(p.mode().map(Mode::phase), Some(ProtectionPhase::Paused));
}

// ---------------------------------------------------------------------------------------------
// Tester rows: one per guard the mutation pass found unguarded (tester-l1-handoff.md). Not plan
// rows; the row author folds them into the m7b_ rows they belong to.
// ---------------------------------------------------------------------------------------------

/// Tester row (mutant M03, `>=` -> `>` on the resume lag): a lag of exactly `resume_lag_ms`
/// restarts the hold; one below starts it (design §4.4 Reprotecting arms).
#[retcd_test]
fn tester_lag_of_exactly_250_ms_restarts_the_hold() {
    let mut p = reprotecting_at(0);
    step(&mut p, 1_000, progress(B));
    step(&mut p, 1_000, progress(C));
    health(&mut p, 1_249);
    assert_eq!(
        p.mode(),
        Some(Mode::Reprotecting {
            below_since: Some(Tick(1_249))
        })
    );
    health(&mut p, 1_250);
    assert_eq!(p.mode(), Some(Mode::Reprotecting { below_since: None }));
}

/// Tester row (mutant M07): a block on an instance already `Paused` keeps its barrier; the
/// barrier moves to the head only "if mode != Paused" (design §4.4 BlockPartition arm).
#[retcd_test]
fn tester_block_on_paused_keeps_the_barrier() {
    let mut p = fresh(10);
    step(&mut p, 1, applied(12, 1));
    let effects = step(&mut p, 2, KernelEvent::BlockPartition(divergence()));
    let s = admission(&effects);
    assert_eq!((s.paused_prefix, s.resume_barrier), (Seq(10), Seq(10)));
}

/// Tester row (mutant M11): `ProtectionWarn` names the OLDEST unsafe entry and its age, not the
/// newest (design §4.2 `unsafe_queue.front()`, ruling B-R34).
#[retcd_test]
fn tester_warn_names_the_oldest_entry_not_the_newest() {
    let mut p = golden();
    step(&mut p, T0, applied(5, 10));
    step(&mut p, T0 + 300, applied(6, 20));
    assert_eq!(
        health(&mut p, T0 + 1_000),
        vec![KernelEffect::ProtectionWarn {
            oldest_unsafe_seq: Seq(5),
            age_ms: 1_000,
        }]
    );
}

/// Tester row (mutant M12): `Lost` while already `Paused` moves the barrier to the highest
/// applied seq (design §4.4 first arm is `* -> Paused{highest_applied}`).
#[retcd_test]
fn tester_lost_while_paused_moves_the_barrier_to_the_head() {
    let mut p = fresh(10);
    step(&mut p, 1, applied(12, 1));
    assert_eq!(
        step(&mut p, 2, edge(QualificationDirection::Lost)),
        ignored(ReplicaIgnoreReason::NoQualifyingSecondary)
    );
    let s = state(&p, 2);
    assert_eq!((s.paused_prefix, s.resume_barrier), (Seq(12), Seq(12)));
}

/// Tester row (mutant M20): a PARTIAL drain that leaves a younger front below the warn age
/// returns `Warn -> Healthy` (design §4.4 `Warn, unsafe_age < warn_ms`).
#[retcd_test]
fn tester_warn_returns_to_healthy_on_a_partial_drain() {
    let mut p = golden();
    step(&mut p, T0, applied(1, 1));
    step(&mut p, T0 + 800, applied(2, 1));
    health(&mut p, T0 + 1_000);
    assert_eq!(p.mode(), Some(Mode::Warn));
    step(&mut p, T0 + 1_000, durable(&[(C1, 1)]));
    assert_eq!(
        health(&mut p, T0 + 1_100),
        ignored(ReplicaIgnoreReason::Outstanding)
    );
    assert_eq!(p.mode(), Some(Mode::Healthy));
}

// ---------------------------------------------------------------------------------------------
// Lead ruling B-R38: the manual gate's findings F1, F4, F5, F6, F7 (tester-l1-handoff.md).
// ---------------------------------------------------------------------------------------------

/// F1 (design §4.5): whenever `blocked` is set the reason is `DivergenceRequiresOperator`,
/// whatever the `BlockReason`. From `Paused` that is still an admission edge: the reason moves.
#[retcd_test]
fn a_non_divergence_block_reads_as_a_block_from_healthy_and_from_paused() {
    for (mut p, from) in [(golden(), Mode::Healthy), (fresh(5), Mode::Paused)] {
        assert_eq!(p.mode(), Some(from));
        let effects = step(
            &mut p,
            T0,
            KernelEvent::BlockPartition(BlockReason::OvertakenByPeer),
        );
        let s = admission(&effects);
        assert!(!s.allow, "from {from:?}");
        assert_eq!(
            s.reason,
            Some(ErrorKind::DivergenceRequiresOperator),
            "from {from:?}"
        );
        assert_eq!(p.blocked(), Some(&BlockReason::OvertakenByPeer));
    }
}

/// F4: a `Recovered` for an older generation is stale, even when it names another node. The
/// instance and its unsafe queue are kept, exactly as for the same generation.
#[retcd_test]
fn a_stale_generation_recovered_keeps_the_instance_and_its_queue() {
    for pinned in [config(C1, &[]), elsewhere()] {
        let mut p = golden();
        step(&mut p, T0, applied(1, 100));
        let before = p.clone();
        assert_eq!(
            step(&mut p, T0, recovered(pinned, 0, Generation(GEN.0 - 1))),
            ignored(ReplicaIgnoreReason::NotRequired)
        );
        assert_eq!(p, before);
        assert_eq!(p.unsafe_len(), 1, "exposure is not dropped");
    }
}

/// F5: demotion is fail-closed. A live instance that was admitting publishes a reject as it
/// goes inert; one already rejecting has no edge to publish.
#[retcd_test]
fn demotion_while_admitting_publishes_a_reject() {
    let newer = Generation(GEN.0 + 1);

    let mut p = golden();
    let effects = step(&mut p, T0, recovered(elsewhere(), 0, newer));
    let s = admission(&effects);
    assert!(!s.allow);
    assert_eq!(s.reason, Some(ErrorKind::ProtectionPaused));
    assert_eq!(p.mode(), None, "inert after demotion");

    let mut paused = fresh(5);
    assert_eq!(
        step(&mut paused, T0, recovered(elsewhere(), 0, newer)),
        vec![KernelEffect::Ignored {
            reason: KernelIgnoredReason::Error(ErrorKind::NotPrimary),
        }]
    );
    assert_eq!(paused.mode(), None);
}

/// F6: a `ConfigChanged` at or below the current version is refused and changes nothing, held
/// or not.
#[retcd_test]
fn an_older_config_version_is_invalid_and_changes_nothing() {
    let mut p = golden();
    step(
        &mut p,
        T0,
        KernelEvent::ConfigChanged(config(ConfigVersion(3), &[])),
    );
    let before = p.clone();
    for version in [ConfigVersion(2), C1, ConfigVersion(3)] {
        assert_eq!(
            step(&mut p, T0, KernelEvent::ConfigChanged(config(version, &[]))),
            ignored(ReplicaIgnoreReason::InvalidConfig),
            "{version:?}"
        );
    }
    assert_eq!(p, before);
}

/// F7 (design §4.2, fail-closed on absence): with every peer lost nobody reports, so the lag
/// is infinite and resume stays held, however long it waits.
#[retcd_test]
fn with_every_peer_lost_resume_is_blocked() {
    let mut p = reprotecting_at(0);
    step(&mut p, 0, KernelEvent::CopyLost { copy: B });
    step(&mut p, 0, KernelEvent::CopyLost { copy: C });
    assert_eq!(p.lag_domain(), Vec::new());
    assert_eq!(run(&mut p, 0, 60_000, |_, _| true), None);
    assert_eq!(p.mode(), Some(Mode::Reprotecting { below_since: None }));
    let s = state(&p, 60_000);
    assert_eq!(s.replication_lag, ReplicationLag::INFINITE);
    assert_eq!(s.stalest_copy, None);
}

// ---------------------------------------------------------------------------------------------
// Lead ruling B-R42: the L1 code review's fixes (review-l1.md).
// ---------------------------------------------------------------------------------------------

/// `required_copy_set` is the current predicate by node: primary plus regular secondaries,
/// never a shadow, sorted, following a `ConfigChanged`; empty while inert and after demotion.
#[retcd_test]
fn required_copy_set_is_the_current_predicate_by_node() {
    assert_eq!(Protection::new().required_copy_set(), Vec::new());

    let mut p = Protection::new();
    let shadow = [member(D, ReplicaRole::Shadow)];
    step(&mut p, 0, recovered(config(C1, &shadow), 0, GEN));
    assert_eq!(p.required_copy_set(), vec![NodeId(1), NodeId(2), NodeId(3)]);

    // Primary first, then a regular on node 30 ahead of one on node 2: the set is sorted.
    let unsorted = PartitionConfig::new(
        PARTITION,
        C2,
        vec![
            member(A, ReplicaRole::Primary),
            member(CopyId(30), ReplicaRole::RegularSecondary),
            member(B, ReplicaRole::RegularSecondary),
            member(D, ReplicaRole::Shadow),
        ],
    );
    step(&mut p, 0, KernelEvent::ConfigChanged(unsorted));
    assert_eq!(
        p.required_copy_set(),
        vec![NodeId(1), NodeId(2), NodeId(30)]
    );

    step(&mut p, 0, recovered(elsewhere(), 0, Generation(GEN.0 + 1)));
    assert_eq!(p.required_copy_set(), Vec::new(), "demoted");
}

/// A second partition, for the partition guard (review M1).
const OTHER: PartitionId = PartitionId(2);

/// The plan fixture's `{A primary, B, C regular}` at `version`, in `partition`.
fn config_in(partition: PartitionId, version: ConfigVersion) -> PartitionConfig {
    PartitionConfig::new(partition, version, config(version, &[]).members)
}

/// Review M1, `Protection::step`: once live, an instance serves one partition. Every L1 input
/// addressed to another partition answers `InvalidConfig` and changes nothing, even one that
/// would pause (the health eval at +3000) or rebuild (a newer `Recovered` for that partition).
#[retcd_test]
fn an_input_addressed_to_another_partition_is_invalid_and_changes_nothing() {
    let mut p = golden();
    step(&mut p, T0, applied(1, 100));
    let before = p.clone();
    let inputs = [
        EventKind::Timer(TimerFired {
            id: HEALTH_EVAL_TIMER,
            version: TimerVersion(0),
            scheduled_at: Tick::ZERO,
        }),
        EventKind::Kernel(applied(2, 100)),
        EventKind::Kernel(durable(&[(C1, 1)])),
        EventKind::Kernel(edge(QualificationDirection::Lost)),
        EventKind::Kernel(KernelEvent::BlockPartition(divergence())),
        EventKind::Kernel(KernelEvent::CopyLost { copy: C }),
        EventKind::Kernel(KernelEvent::ConfigChanged(config_in(OTHER, C2))),
        EventKind::Kernel(recovered(config_in(OTHER, C1), 0, Generation(GEN.0 + 1))),
    ];
    for (i, kind) in inputs.into_iter().enumerate() {
        assert_eq!(
            try_step_on(&mut p, OTHER, T0 + 3_000, kind).expect("an L1 input"),
            ignored(ReplicaIgnoreReason::InvalidConfig),
            "input {i}"
        );
    }
    assert_eq!(p, before);
}

/// Review M1, `State::at_recovery`: a `Recovered` whose pinned configuration belongs to another
/// partition than the one it was delivered for is refused, inert or live, and changes nothing.
#[retcd_test]
fn a_recovered_pinning_another_partition_is_invalid_and_changes_nothing() {
    let foreign = recovered(config_in(OTHER, C1), 0, Generation(GEN.0 + 1));
    let mut inert = Protection::new();
    assert_eq!(
        step(&mut inert, T0, foreign.clone()),
        ignored(ReplicaIgnoreReason::InvalidConfig)
    );
    assert_eq!(inert, Protection::new());

    let mut live = golden();
    step(&mut live, T0, applied(1, 100));
    let before = live.clone();
    assert_eq!(
        step(&mut live, T0, foreign),
        ignored(ReplicaIgnoreReason::InvalidConfig)
    );
    assert_eq!(live, before);
}

/// Review M1, `State::pin`: a newer configuration for another partition is not this
/// partition's predicate.
#[retcd_test]
fn a_config_change_for_another_partition_is_invalid_and_changes_nothing() {
    let mut p = golden();
    let before = p.clone();
    assert_eq!(
        step(
            &mut p,
            T0,
            KernelEvent::ConfigChanged(config_in(OTHER, ConfigVersion(5)))
        ),
        ignored(ReplicaIgnoreReason::InvalidConfig)
    );
    assert_eq!(p, before);
}

/// Review A1: the served generation outlives demotion. Demoted at GEN+1, a `Recovered` for GEN
/// or older naming this node is still stale; a newer one still promotes.
#[retcd_test]
fn a_stale_recovered_after_demotion_is_still_stale() {
    let mut p = golden();
    step(&mut p, T0, recovered(elsewhere(), 0, Generation(GEN.0 + 1)));
    assert_eq!(p.mode(), None);
    for stale in [GEN, Generation(GEN.0 - 1)] {
        assert_eq!(
            step(&mut p, T0, recovered(config(C1, &[]), 0, stale)),
            ignored(ReplicaIgnoreReason::NotRequired),
            "{stale:?}"
        );
        assert_eq!(p.mode(), None, "{stale:?}");
    }
    let effects = step(
        &mut p,
        T0,
        recovered(config(C1, &[]), 0, Generation(GEN.0 + 2)),
    );
    assert!(!admission(&effects).allow);
    assert_eq!(p.mode(), Some(Mode::Paused));
}

/// Review A2: a `DurableAdvanced` naming no active predicate and no version above the current
/// one records nothing, so it answers `InvalidConfig` and drains nothing; so does an empty one.
/// One matching entry is enough. (Lead ruling B-R46d: a version above the current one is now
/// kept for its pin, so the refused case is one below the lowest active predicate.)
#[retcd_test]
fn a_durable_advance_for_no_active_version_is_invalid_and_changes_nothing() {
    let mut p = golden();
    step(&mut p, T0, applied(1, 100));
    let before = p.clone();
    for per_predicate in [&[(ConfigVersion(0), 1)][..], &[]] {
        assert_eq!(
            step(&mut p, T0, durable(per_predicate)),
            ignored(ReplicaIgnoreReason::InvalidConfig),
            "{per_predicate:?}"
        );
    }
    assert_eq!(p, before);
    assert_eq!(
        step(&mut p, T0, durable(&[(ConfigVersion(0), 1), (C1, 1)])),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert_eq!(p.unsafe_len(), 0);
}

/// Review A2: a `CopyLost` for a copy in no member slot is refused, never inserted and never
/// published in `lost_copies`. A member's loss is still recorded.
#[retcd_test]
fn a_copy_lost_for_no_member_slot_is_invalid_and_never_published() {
    let mut p = golden();
    let before = p.clone();
    assert_eq!(
        step(&mut p, T0, KernelEvent::CopyLost { copy: CopyId(77) }),
        ignored(ReplicaIgnoreReason::InvalidConfig)
    );
    assert_eq!(p, before);
    assert_eq!(state(&p, T0).lost_copies, Vec::new());
    assert_eq!(
        step(&mut p, T0, KernelEvent::CopyLost { copy: C }),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert_eq!(state(&p, T0).lost_copies, vec![C]);
}

/// Review A3: a drain re-reads exposure in the same step. A front left younger than the warn
/// age returns `Warn -> Healthy` there, so the hint names the next warn, not a pause; a front
/// left at exactly the warn age keeps `Warn`; a full drain leaves no hint. The step still
/// answers `Recorded`.
#[retcd_test]
fn a_drain_returns_warn_to_healthy_in_the_draining_step() {
    let warn_then_drain = |second_at: u64, eval_at: u64| {
        let mut p = golden();
        step(&mut p, T0, applied(1, 1));
        step(&mut p, T0 + second_at, applied(2, 1));
        health(&mut p, T0 + eval_at);
        assert_eq!(p.mode(), Some(Mode::Warn));
        assert_eq!(
            step(&mut p, T0 + eval_at, durable(&[(C1, 1)])),
            ignored(ReplicaIgnoreReason::Recorded)
        );
        p
    };

    let younger = warn_then_drain(800, 1_000);
    assert_eq!(younger.mode(), Some(Mode::Healthy));
    assert_eq!(younger.next_interesting_tick(), Some(Tick(T0 + 1_800)));

    let at_warn_age = warn_then_drain(500, 1_500);
    assert_eq!(at_warn_age.mode(), Some(Mode::Warn));
    assert_eq!(at_warn_age.next_interesting_tick(), Some(Tick(T0 + 2_500)));

    let mut drained = golden();
    step(&mut drained, T0, applied(1, 1));
    health(&mut drained, T0 + 1_000);
    step(&mut drained, T0 + 1_000, durable(&[(C1, 1)]));
    assert_eq!(drained.mode(), Some(Mode::Healthy));
    assert_eq!(drained.next_interesting_tick(), None);
}

// ---------------------------------------------------------------------------------------------
// Lead ruling B-R46: the sim gate's S1 and S4 (tester-l1-handoff.md, "Sim gate").
// ---------------------------------------------------------------------------------------------

/// `Recovered{cutoff 40}`, `Gained` at 10 and `DurableAdvanced{C1: 40}` at 20: `Paused` with its
/// barrier durable, a qualifying secondary and nothing queued (tester probes s15 and d04).
fn paused_with_barrier_40_durable() -> Protection {
    let mut p = fresh(40);
    step(&mut p, 10, edge(QualificationDirection::Gained));
    step(&mut p, 20, durable(&[(C1, 40)]));
    p
}

/// Tester probe s15's walk at a 100 ms cadence: barrier 40 durable, a qualifying secondary, and
/// seq 41 applied at `applied_at` and never made durable, while B and C report every 100 ms.
/// The hold starts at 200 and completes at 5200; returns the instance and that step's answer.
fn hold_completes_with_41_applied_at(applied_at: u64) -> (Protection, Vec<KernelEffect>) {
    let mut p = paused_with_barrier_40_durable();
    let last_eval_before = applied_at - applied_at % 100;
    assert_eq!(run(&mut p, 0, last_eval_before, |_, _| true), None);
    step(&mut p, applied_at, applied(41, 1));
    assert_eq!(run(&mut p, last_eval_before, 5_100, |_, _| true), None);
    assert_eq!(
        p.mode(),
        Some(Mode::Reprotecting {
            below_since: Some(Tick(200))
        })
    );
    step(&mut p, 5_200, progress(B));
    step(&mut p, 5_200, progress(C));
    let effects = health(&mut p, 5_200);
    (p, effects)
}

/// Lead ruling B-R46 S1 (probe s15): a completed hold does not resume while the oldest unsafe
/// record is at or past the pause age. L1 pauses at the head instead: no admission edge (both
/// states reject), `Ignored{Outstanding}`, prefix and barrier at 41, and it stays paused while
/// 41 is never durable. A record one millisecond younger than the pause age still resumes.
#[retcd_test]
fn a_completed_hold_pauses_at_the_head_while_exposure_is_past_the_pause_age() {
    for applied_at in [30, 3_200] {
        let (mut p, effects) = hold_completes_with_41_applied_at(applied_at);
        assert_eq!(
            effects,
            ignored(ReplicaIgnoreReason::Outstanding),
            "applied at {applied_at}"
        );
        assert_eq!(p.mode(), Some(Mode::Paused), "applied at {applied_at}");
        let s = state(&p, 5_200);
        assert!(!s.allow);
        assert_eq!(s.oldest_unsafe_age, 5_200 - applied_at);
        assert_eq!((s.paused_prefix, s.resume_barrier), (Seq(41), Seq(41)));
        assert_eq!(run(&mut p, 5_200, 7_000, |_, _| true), None);
        assert_eq!(p.mode(), Some(Mode::Paused), "applied at {applied_at}");
    }

    let (p, effects) = hold_completes_with_41_applied_at(3_201);
    let s = admission(&effects);
    assert!(s.allow);
    assert_eq!(s.oldest_unsafe_age, 1_999);
    assert_eq!(p.mode(), Some(Mode::Healthy));
}

/// Lead ruling B-R46 S4 (probe s03b): once bound, `served` records the highest generation seen,
/// a demoting one included. Live at GEN and demoted at GEN+2, a `Recovered` for GEN+1 naming
/// this node is stale and builds nothing. A demotion seen while inert is recorded the same way;
/// only a generation above every one seen promotes.
#[retcd_test]
fn a_generation_below_a_demoting_one_is_stale() {
    let above = |n: u64| Generation(GEN.0 + n);
    let mut p = golden();
    let effects = step(&mut p, T0, recovered(elsewhere(), 0, above(2)));
    assert!(!admission(&effects).allow);
    assert_eq!(
        step(&mut p, T0, recovered(config(C1, &[]), 0, above(1))),
        ignored(ReplicaIgnoreReason::NotRequired)
    );
    assert_eq!(p.mode(), None);

    assert_eq!(
        step(&mut p, T0, recovered(elsewhere(), 0, above(3))),
        vec![KernelEffect::Ignored {
            reason: KernelIgnoredReason::Error(ErrorKind::NotPrimary),
        }]
    );
    for stale in [above(1), above(3)] {
        assert_eq!(
            step(&mut p, T0, recovered(config(C1, &[]), 0, stale)),
            ignored(ReplicaIgnoreReason::NotRequired),
            "{stale:?}"
        );
        assert_eq!(p.mode(), None, "{stale:?}");
    }

    let effects = step(&mut p, T0, recovered(config(C1, &[]), 0, above(4)));
    assert!(!admission(&effects).allow);
    assert_eq!(p.mode(), Some(Mode::Paused));
}

// ---------------------------------------------------------------------------------------------
// Lead rulings B-R46a and B-R46b: review R2 and tester D1. Nothing at or below the durable floor
// is ever queued or left queued, whichever order the doors open in.
// ---------------------------------------------------------------------------------------------

/// The rulings' liveness bound: an allow edge within one hold of the tick the exposure drained,
/// plus two 100 ms evaluations of [`run`] (the one that leaves `Paused`, the one that starts the
/// hold). A pause loop on an idle partition has no allow at all.
fn assert_allow_within_one_hold(allow: Option<u64>, drained_at: u64) {
    let bound = drained_at + BUDGETS.resume_hold_millis + 200;
    assert!(
        allow.is_some_and(|t| t <= bound),
        "allow {allow:?}, drained at {drained_at}, bound {bound}"
    );
}

/// A `Healthy` instance whose front record (`front`, applied at `applied_at`) is never durable
/// does not pause one millisecond before `pause_age_millis` and pauses exactly at it.
fn assert_pauses_at_the_pause_age(p: &mut Protection, applied_at: u64, front: Seq) {
    let at = applied_at + BUDGETS.pause_age_millis;
    health(p, at - 1);
    assert_ne!(p.mode(), Some(Mode::Paused), "not before {at}");
    assert!(!admission(&health(p, at)).allow, "paused at {at}");
    let s = state(p, at);
    assert_eq!(
        (s.oldest_unsafe_seq, s.oldest_unsafe_age),
        (front, BUDGETS.pause_age_millis)
    );
}

/// Tester D1 door A (probe d04): R1's `DurableAdvanced{41}` reaches L1 before the primary's own
/// `LocalApplied{41}`. The record is durable when it is applied, so it is not queued
/// (`NothingOutstanding`), and the partition resumes after one hold instead of pausing at a
/// durable head every 5 s (S1). The applied head still moves: a later pause is at 41.
#[retcd_test]
fn a_record_durable_before_it_is_applied_is_not_queued() {
    let mut p = paused_with_barrier_40_durable();
    assert_eq!(
        step(&mut p, 30, durable(&[(C1, 41)])),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert_eq!(
        step(&mut p, 40, applied(41, 1)),
        ignored(ReplicaIgnoreReason::NothingOutstanding)
    );
    let allow = run(&mut p, 0, 20_000, |_, _| true);
    assert_allow_within_one_hold(allow, 40);
    assert_eq!((p.mode(), p.unsafe_len()), (Some(Mode::Healthy), 0));

    let now = allow.expect("resumed");
    assert!(!admission(&step(&mut p, now, edge(QualificationDirection::Lost))).allow);
    assert_eq!(state(&p, now).resume_barrier, Seq(41));
}

/// Tester D1 door B (probe d04): a `LocalApplied{38}` after promotion with cutoff 40, whose
/// barrier is durable. Below the floor, so it is not queued and the partition resumes.
#[retcd_test]
fn a_record_at_or_below_the_cutoff_is_not_queued() {
    let mut p = paused_with_barrier_40_durable();
    assert_eq!(
        step(&mut p, 40, applied(38, 1)),
        ignored(ReplicaIgnoreReason::NothingOutstanding)
    );
    assert_allow_within_one_hold(run(&mut p, 0, 20_000, |_, _| true), 40);
    assert_eq!((p.mode(), p.unsafe_len()), (Some(Mode::Healthy), 0));
}

/// The review's R2 state at `T0 + 2000`: seqs 1..=50 applied at [`T0`], predicates `[C2, C1]`
/// both durable through 40, so 41..=50 are queued and old enough to pause: `Paused` at barrier
/// 50. Then R1's `DurableAdvanced{C2: 50}` and L1's own retirement of C1 (barrier 40) arrive,
/// the durable report first when `durable_first`.
fn paused_across_a_retirement(durable_first: bool) -> Protection {
    let now = T0 + 2_000;
    let mut p = golden();
    for seq in 1..=50 {
        step(&mut p, T0, applied(seq, 1));
    }
    step(&mut p, T0, KernelEvent::ConfigChanged(config(C2, &[])));
    step(&mut p, T0, durable(&[(C1, 40), (C2, 40)]));
    assert!(!admission(&health(&mut p, now)).allow);
    assert_eq!(state(&p, now).resume_barrier, Seq(50));
    let (first, second) = (durable(&[(C2, 50)]), confirm_c1(40));
    let order = if durable_first {
        [first, second]
    } else {
        [second, first]
    };
    for input in order {
        assert_eq!(
            step(&mut p, now, input),
            ignored(ReplicaIgnoreReason::Recorded)
        );
    }
    assert_eq!(state(&p, now).required_config_versions, vec![C2]);
    assert_eq!(p.unsafe_len(), 0, "drained through 50 whichever came first");
    p
}

/// Review R2 (a), tester D1 door C: R1's `DurableAdvanced` for the surviving predicate arrives
/// BEFORE L1's own retirement. The floor is still C1's 40 when it lands, so only the retirement
/// can drain; it does, and the partition resumes within one hold.
#[retcd_test]
fn a_durable_advance_before_the_retirement_still_resumes() {
    let mut p = paused_across_a_retirement(true);
    assert_allow_within_one_hold(run(&mut p, T0 + 2_000, 20_000, |_, _| true), T0 + 2_000);
    assert_eq!(p.mode(), Some(Mode::Healthy));
}

/// Review R2 (b): the same with the order reversed. The retirement drains nothing (the
/// survivor's stored floor is 40), and R1's `DurableAdvanced` then drains through 50.
#[retcd_test]
fn a_durable_advance_after_the_retirement_resumes() {
    let mut p = paused_across_a_retirement(false);
    assert_allow_within_one_hold(run(&mut p, T0 + 2_000, 20_000, |_, _| true), T0 + 2_000);
    assert_eq!(p.mode(), Some(Mode::Healthy));
}

/// The positive twin: only what is at or below the floor is dropped. A record one above the
/// floor is queued (`Recorded`) and still pauses at exactly `pause_age_millis`. So do the
/// records a retirement leaves above the survivors' floor: C2 durable through 45 keeps 46..=50.
#[retcd_test]
fn records_above_the_floor_stay_queued_and_pause_at_the_pause_age() {
    let mut p = golden();
    step(&mut p, T0, durable(&[(C1, 41)]));
    assert_eq!(
        step(&mut p, T0, applied(41, 1)),
        ignored(ReplicaIgnoreReason::NothingOutstanding)
    );
    assert_eq!(
        step(&mut p, T0, applied(42, 1)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert_eq!(p.unsafe_len(), 1);
    assert_pauses_at_the_pause_age(&mut p, T0, Seq(42));

    let mut p = golden();
    for seq in 1..=50 {
        step(&mut p, T0, applied(seq, 1));
    }
    step(&mut p, T0, KernelEvent::ConfigChanged(config(C2, &[])));
    step(&mut p, T0, durable(&[(C1, 40), (C2, 45)]));
    assert_eq!(p.unsafe_len(), 10, "drained through min(40, 45)");
    step(&mut p, T0 + 500, confirm_c1(40));
    assert_eq!(p.unsafe_len(), 5, "46..=50 stay");
    assert_pauses_at_the_pause_age(&mut p, T0, Seq(46));
}

// ---------------------------------------------------------------------------------------------
// Lead ruling B-R46d: tester E1. A durable view for a version L1 has not pinned yet is kept for
// the pin, so the answer does not depend on whether R1 or L1 hears `ConfigChanged` first.
// ---------------------------------------------------------------------------------------------

/// A scenario trace (spikes §6): per step, the whole effect vector and the mode after it.
type Trace = Vec<(Vec<KernelEffect>, Option<Mode>)>;

/// Tester probes e05/e05b, ported: d04's seed ([`paused_with_barrier_40_durable`]), 41..=50
/// applied at 100..=109, then `inputs` at their ticks, with B and C reporting and a health eval
/// every 100 ms throughout. Returns the inputs' trace and the first allow after the last one.
fn walk_with(inputs: Vec<(u64, KernelEvent)>) -> (Trace, Option<u64>) {
    let mut p = paused_with_barrier_40_durable();
    let mut evaluated = 0;
    for (seq, t) in (41..=50).zip(100..) {
        run_to(&mut p, &mut evaluated, t);
        step(&mut p, t, applied(seq, 1));
    }
    let trace = inputs
        .into_iter()
        .map(|(t, input)| {
            run_to(&mut p, &mut evaluated, t);
            let effects = step(&mut p, t, input);
            (effects, p.mode())
        })
        .collect();
    (trace, run(&mut p, evaluated, 20_000, |_, _| true))
}

/// [`run`] from `*evaluated` through the last 100 ms tick at or before `t`, expecting no allow.
fn run_to(p: &mut Protection, evaluated: &mut u64, t: u64) {
    let upto = (t - t % 100).max(*evaluated);
    assert_eq!(
        run(p, *evaluated, upto, |_, _| true),
        None,
        "no allow by {t}"
    );
    *evaluated = upto;
}

fn pin(version: ConfigVersion) -> KernelEvent {
    KernelEvent::ConfigChanged(config(version, &[]))
}

/// Tester E1 (probe e05): R1 reports C2 durable through 50 BEFORE L1 pins C2, once beside C1
/// and once alone. Both are recorded, the pin seeds C2 at 50 instead of 0, and the partition
/// resumes within one hold of the pin instead of pausing at the head for good.
#[retcd_test]
fn a_durable_report_before_its_pin_is_not_lost() {
    let (answers, allow) = walk_with(vec![
        (200, durable(&[(C1, 50), (C2, 50)])),
        (300, durable(&[(C2, 50)])),
        (400, pin(C2)),
        (500, confirm_c1(50)),
    ]);
    for (answer, _) in &answers {
        assert_eq!(
            *answer,
            ignored(ReplicaIgnoreReason::Recorded),
            "{answers:?}"
        );
    }
    assert!(allow.is_some_and(|t| t > 500), "{allow:?}");
    assert_allow_within_one_hold(allow, 400);
}

/// Tester E1's control (probe e05b): the same report after the pin lands on an active predicate
/// and the partition resumes, as it always did.
#[retcd_test]
fn a_durable_report_after_its_pin_resumes() {
    let (answers, allow) = walk_with(vec![
        (200, durable(&[(C1, 50)])),
        (250, pin(C2)),
        (300, durable(&[(C2, 50)])),
        (500, confirm_c1(50)),
    ]);
    for (answer, _) in &answers {
        assert_eq!(
            *answer,
            ignored(ReplicaIgnoreReason::Recorded),
            "{answers:?}"
        );
    }
    assert!(allow.is_some_and(|t| t > 500), "{allow:?}");
    assert_allow_within_one_hold(allow, 300);
}

/// B-R46d: a report naming an active predicate and one not pinned yet records both. The pending
/// view keeps R1's latest report per version, as an active predicate does (lead ruling B-R46e,
/// tester F1), so the lower later view replaces 45, and the pin seeds from it: the floor is
/// min(C1 50, C2 30) = 30 and the entry is gone.
#[retcd_test]
fn a_mixed_report_is_remembered_for_the_pin() {
    let mut p = golden();
    for (report, why) in [
        (durable(&[(C1, 50), (C2, 45)]), "active and pending"),
        (durable(&[(C2, 30)]), "pending only, lower"),
    ] {
        assert_eq!(
            step(&mut p, T0, report),
            ignored(ReplicaIgnoreReason::Recorded),
            "{why}"
        );
    }
    assert_eq!(p.pending_durable_versions(), vec![C2]);
    assert_eq!(
        step(&mut p, T0, pin(C2)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert!(p.pending_durable_versions().is_empty());
    assert_eq!(
        step(&mut p, T0, applied(30, 1)),
        ignored(ReplicaIgnoreReason::NothingOutstanding),
        "C2 seeded at 30, the latest view, not 0"
    );
    assert_eq!(
        step(&mut p, T0, applied(31, 1)),
        ignored(ReplicaIgnoreReason::Recorded),
        "C2 seeded at 30, not the highest view 45"
    );
}

/// B-R46d: a pin drops every pending view it reaches or passes. Pinning C3 over pending C2 and
/// C3 seeds C3 at 70 and forgets C2. A later C2 report names no active predicate and nothing
/// above C3, so it is refused and not kept. With C1 retired the floor is C3's 70.
#[retcd_test]
fn a_pin_drops_every_view_it_reaches_or_passes() {
    let mut p = golden();
    assert_eq!(
        step(&mut p, T0, durable(&[(C2, 60), (C3, 70)])),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert_eq!(p.pending_durable_versions(), vec![C2, C3]);
    assert_eq!(
        step(&mut p, T0, pin(C3)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert!(p.pending_durable_versions().is_empty());
    assert_eq!(
        step(&mut p, T0, durable(&[(C2, 80)])),
        ignored(ReplicaIgnoreReason::InvalidConfig)
    );
    assert!(p.pending_durable_versions().is_empty());
    assert_eq!(
        step(&mut p, T0, confirm_c1(0)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert_eq!(
        step(&mut p, T0, applied(70, 1)),
        ignored(ReplicaIgnoreReason::NothingOutstanding)
    );
    assert_eq!(
        step(&mut p, T0, applied(71, 1)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
}

/// B-R46d: a pin keeps every pending view above it. Pinning C2 while C3 is pending leaves C3's
/// view in place, and the later C3 pin still seeds from it: with C1 and C2 durable through 80,
/// the floor is C3's seeded 70.
#[retcd_test]
fn a_pin_keeps_the_views_above_it() {
    let mut p = golden();
    for input in [durable(&[(C3, 70)]), pin(C2)] {
        assert_eq!(
            step(&mut p, T0, input),
            ignored(ReplicaIgnoreReason::Recorded)
        );
    }
    assert_eq!(p.pending_durable_versions(), vec![C3], "C2's pin kept C3");
    for input in [pin(C3), durable(&[(C1, 80), (C2, 80)])] {
        assert_eq!(
            step(&mut p, T0, input),
            ignored(ReplicaIgnoreReason::Recorded)
        );
    }
    assert!(p.pending_durable_versions().is_empty());
    assert_eq!(
        step(&mut p, T0, applied(70, 1)),
        ignored(ReplicaIgnoreReason::NothingOutstanding),
        "C3 seeded at 70"
    );
    assert_eq!(
        step(&mut p, T0, applied(71, 1)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
}

/// B-R46d: a report whose every version is below the lowest active predicate (C1, retired) is
/// still refused. It is not kept and moves no floor.
#[retcd_test]
fn a_report_for_a_retired_version_is_still_refused() {
    let mut p = golden();
    step(&mut p, T0, pin(C2));
    assert_eq!(
        step(&mut p, T0, confirm_c1(0)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
    assert_eq!(
        step(&mut p, T0, durable(&[(C1, 60)])),
        ignored(ReplicaIgnoreReason::InvalidConfig)
    );
    assert!(p.pending_durable_versions().is_empty());
    assert_eq!(
        step(&mut p, T0, applied(1, 1)),
        ignored(ReplicaIgnoreReason::Recorded)
    );
}

// ---------------------------------------------------------------------------------------------
// Lead ruling B-R46e: tester F1. The pending store keeps R1's latest report, as the active store
// does, so the same reports end in the same state whether L1 pins before them or after them.
// Tester probes f06 and f06b have no plan id; these rows carry plain names.
// ---------------------------------------------------------------------------------------------

/// One beat of a scenario (spikes §6): an L1 input at a tick, or L1's health timer firing at one.
#[derive(Clone)]
enum Beat {
    Input(u64, KernelEvent),
    Eval(u64),
}

/// Plays `beats` on `p` in order and returns their trace.
fn play(p: &mut Protection, beats: impl IntoIterator<Item = Beat>) -> Trace {
    beats
        .into_iter()
        .map(|beat| {
            let effects = match beat {
                Beat::Input(t, input) => step(p, t, input),
                Beat::Eval(t) => health(p, t),
            };
            (effects, p.mode())
        })
        .collect()
}

/// Tester F1, probe f06: C1 is durable through 90, R1 reports C2 twice, and L1 pins C2 before
/// both reports or after both. Either way C2 holds R1's latest view, `seed`: seq `seed` is
/// durable when applied, `seed + 1` is queued, warns at 1000 ms and pauses at 2000 ms, and the
/// two orders end in the same state. Two pairs, because latest must beat both neighbours: falling
/// (50 then 30, seed 30) separates latest from highest, the old max rule, under which the
/// pin-last order took 31 as durable and never paused; rising (30 then 50, seed 50; lead ruling
/// B-R50, tester G1) separates latest from lowest.
#[retcd_test]
fn the_pin_order_does_not_decide_the_seeded_view() {
    for (pair, first, seed) in [("falling", 50, 30), ("rising", 30, 50)] {
        let report = |view: &[(ConfigVersion, u64)]| Beat::Input(T0, durable(view));
        let (c1_90, earlier, latest) = (
            report(&[(C1, 90)]),
            report(&[(C2, first)]),
            report(&[(C2, seed)]),
        );
        let pin_c2 = Beat::Input(T0, pin(C2));
        let exposure = [
            Beat::Input(T0, applied(seed, 1)),
            Beat::Input(T0, applied(seed + 1, 1)),
            Beat::Eval(T0 + 1_000),
            Beat::Eval(T0 + 2_000),
        ];
        let orders = [
            (
                "pin first",
                [
                    c1_90.clone(),
                    pin_c2.clone(),
                    earlier.clone(),
                    latest.clone(),
                ],
            ),
            ("pin last", [c1_90, earlier, latest, pin_c2]),
        ];

        let healthy = |reason| (ignored(reason), Some(Mode::Healthy));
        let recorded = healthy(ReplicaIgnoreReason::Recorded);
        let front = Seq(seed + 1);
        let paused = AdmissionState {
            allow: false,
            reason: Some(ErrorKind::ProtectionPaused),
            oldest_unsafe_age: 2_000,
            oldest_unsafe_seq: front,
            replication_lag: ReplicationLag::millis(2_000),
            stalest_copy: Some(C),
            lost_copies: vec![],
            paused_prefix: front,
            resume_barrier: front,
            required_config_versions: vec![C2, C1],
            outstanding_unsafe_bytes: 1,
        };
        let expected: Trace = vec![
            recorded.clone(),
            recorded.clone(),
            recorded.clone(),
            recorded.clone(),
            healthy(ReplicaIgnoreReason::NothingOutstanding),
            recorded,
            (
                vec![KernelEffect::ProtectionWarn {
                    oldest_unsafe_seq: front,
                    age_ms: 1_000,
                }],
                Some(Mode::Warn),
            ),
            (vec![KernelEffect::SetAdmission(paused)], Some(Mode::Paused)),
        ];

        let mut ends = Vec::new();
        for (order, beats) in orders {
            let mut p = golden();
            let trace = play(&mut p, beats.into_iter().chain(exposure.clone()));
            assert_eq!(trace, expected, "{pair}, {order}");
            assert!(p.pending_durable_versions().is_empty(), "{pair}, {order}");
            ends.push(p);
        }
        assert_eq!(ends[0], ends[1], "{pair}: the same state whichever order");
    }
}

/// Tester F1, probe f06b: the same two C2 reports at the admission edge, on d04's walk. L1 pins
/// C2 last (at 400) or first (at 150), and C1 retires at 50. R1's latest view says C2 is durable
/// only through 30, so in both orders L1 is `Paused` at barrier 50 after the retirement and does
/// not resume until R1 reports C2 through 50 at 3000. Then both resume within one hold of that
/// report, at the same tick. Under the old max rule the pin-last order stayed `Reprotecting` on
/// the 50 R1 had since withdrawn and resumed at 5200, before C2 was durable through the barrier.
///
/// The rising pair (C2 30, then 50; lead ruling B-R50, tester G1) is the twin: R1's latest view
/// meets the barrier, so both orders resume with no further report, within one hold of it. A
/// store that kept the lowest view would seed 30 in the pin-last order and never resume.
#[retcd_test]
fn the_pin_order_does_not_decide_the_resume() {
    let recorded = |mode| (ignored(ReplicaIgnoreReason::Recorded), Some(mode));
    let catch_up = (3_000, durable(&[(C2, 50)]));

    let (pin_last, pin_last_allow) = walk_with(vec![
        (200, durable(&[(C1, 50), (C2, 50)])),
        (300, durable(&[(C1, 50), (C2, 30)])),
        (400, pin(C2)),
        (500, confirm_c1(50)),
        catch_up.clone(),
    ]);
    let holding = Mode::Reprotecting {
        below_since: Some(Tick(200)),
    };
    assert_eq!(
        pin_last,
        vec![
            recorded(holding),
            recorded(holding),
            recorded(holding),
            recorded(Mode::Paused),
            recorded(Mode::Paused),
        ],
        "pin last: C2 seeded at 30 fails barrier 40 at the 500 eval"
    );

    let (pin_first, pin_first_allow) = walk_with(vec![
        (150, pin(C2)),
        (200, durable(&[(C1, 50), (C2, 50)])),
        (300, durable(&[(C1, 50), (C2, 30)])),
        (500, confirm_c1(50)),
        catch_up,
    ]);
    let barrier_met = Mode::Reprotecting { below_since: None };
    assert_eq!(
        pin_first,
        vec![
            recorded(barrier_met),
            recorded(Mode::Paused),
            recorded(barrier_met),
            recorded(Mode::Paused),
            recorded(Mode::Paused),
        ],
        "pin first: C2 at 30 fails barrier 50 at the 400 eval"
    );

    assert_eq!(pin_last_allow, pin_first_allow);
    assert!(
        pin_last_allow.is_some_and(|t| t >= 3_000 + BUDGETS.resume_hold_millis),
        "a full hold after the catch-up report: {pin_last_allow:?}"
    );
    assert_allow_within_one_hold(pin_last_allow, 3_000);

    // Rising pair: no catch-up report is sent, because the latest view already meets 50.
    let (pin_last, pin_last_allow) = walk_with(vec![
        (200, durable(&[(C1, 50), (C2, 30)])),
        (300, durable(&[(C1, 50), (C2, 50)])),
        (400, pin(C2)),
        (500, confirm_c1(50)),
    ]);
    assert_eq!(
        pin_last,
        vec![recorded(holding); 4],
        "rising, pin last: C2 seeded at 50 keeps barrier 40 durable, so the hold never breaks"
    );
    assert_eq!(pin_last_allow, Some(5_200), "one hold from 200");

    let (pin_first, pin_first_allow) = walk_with(vec![
        (150, pin(C2)),
        (200, durable(&[(C1, 50), (C2, 30)])),
        (300, durable(&[(C1, 50), (C2, 50)])),
        (500, confirm_c1(50)),
    ]);
    assert_eq!(
        pin_first,
        vec![
            recorded(barrier_met),
            recorded(Mode::Paused),
            recorded(Mode::Paused),
            recorded(Mode::Reprotecting {
                below_since: Some(Tick(500)),
            }),
        ],
        "rising, pin first: paused at 50 by the 200 eval, barrier met by the 400 eval"
    );
    assert_eq!(pin_first_allow, Some(5_500), "one hold from 500");
    for allow in [pin_last_allow, pin_first_allow] {
        assert_allow_within_one_hold(allow, 300);
    }
}
