//! R1 `ProgressTracker`: the design §3.4 ACK ladder, the divergence vector, the control events,
//! and the §3.5 views they move.
//!
//! Rows named `m7b_NN_*` are plan rows (`docs/testing/test-plan-m7-kernel-b.md` §4–§5); the
//! rest are developer scaffolding and tester rows, written against `design.md` §3.4–§3.5. The
//! tracker is driven directly, except in the routing section, which routes the same inputs
//! through `Replication::step` (lead ruling B-R48). The A1-view rows (M7B-161..163, lead ruling
//! B-R53) and the primary `Recovered` builds (M7B-164, lead ruling B-R54) come next; the
//! receiver's own are in `replication_append.rs`. Last are the §9 rows M7B-133..135, 140, 144
//! and 145, the edge, retirement and the two divergence routes; two of them run R1 beside a real
//! L1 or F1.
//!
//! Fixture: A primary (copy 0, node 1), B and C regular (copies 1, 2), D a shadow (copy 3).
//! `min_regular_acks` 1. Every copy's boot is its node number. The primary has applied 12 and
//! synced 12, and vouches for 5..=12. B and C have proved nothing yet.

use std::collections::{BTreeMap, VecDeque};

use config_log::retcd_test;

use bytes::Bytes;
use rdb_core::contracts::authority::{
    AuthorityDecision, AuthorityEvent, AuthorityView, BlockReason, Checkpoint, DenyReason,
    FencingProof, Lineage, PartitionMode, Revocation, Verdict,
};
use rdb_core::contracts::control::{CasOutcome, ControlEffect, ControlEvent, ControlKey};
use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::envelope::{
    AppendAck, AppendOutcome, AppendReject, EnvelopeHeader, ReplicaProgress, ReplicationEnvelope,
};
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, ModuleName,
    StepCtx,
};
use rdb_core::contracts::ids::{
    AppliedSeq, AuthorityGeneration, BatchId, BootId, ClientId, ConfigVersion, ControlRequestId,
    CorrelationId, DurableSeq, EventId, FlushTicket, Generation, GrantId, LeaseId, MessageId,
    NodeId, OwnerEpoch, PartitionId, ReceivedSeq, ReplicaRole, RequestId, RequestIdentity,
    Revision, Seq, SnapshotHandle, TenantId, TimerId, TimerVersion,
};
use rdb_core::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::protection::{AdmissionState, ReplicationLag};
use rdb_core::contracts::qualification::{
    QualificationCause, QualificationChanged, QualificationDirection,
};
use rdb_core::contracts::recovery::{
    Candidate, CommittedRoot, DurableProof, LineageAnchor, LossRecord, RecoveryBarrier,
    RecoveryEffect, RecoveryEvent, RecoveryPlan, RecoveryResult, RetainedStatusMap,
    SelectedLineage, SurvivorInventory,
};
use rdb_core::contracts::storage::{
    DurablePrefix, Namespace, SnapshotRead, StorageEvent, StoreEffect, Write,
};
use rdb_core::contracts::time::{ControlTime, Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::{AckRejectReason, Version};
use rdb_core::contracts::transport::{Frame, PeerLabel, SendEffect, TransportEvent};
use rdb_core::contracts::txn::{Durability, Outcome, TxnResult};
use rdb_core::contracts::version::ENVELOPE_VERSION;
use rdb_core::protection::{Mode, Protection, HEALTH_EVAL_TIMER};
use rdb_core::publication::{AppliedCandidate, PubConfig, PubEffect, PubEvent, PubKernel};
use rdb_core::recovery::{Recovery, RecoveryPhase, DISCOVERY_TIMER};
use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit};
use rdb_core::replication::catchup::{
    retransmit_timer, CatchupCursor, MAX_PROBE_ROUNDS, RETRANSMIT_MS,
};
use rdb_core::replication::primary::{keepalive_timer, Primary as PrimarySide, Shipped};
use rdb_core::replication::progress::{DigestLadder, DigestLookup, ProgressTracker, TrackerInit};
use rdb_core::replication::wire::{decode_reply, encode_reply};
use rdb_core::replication::Replication;

use ReplicaRole::{Primary, RegularSecondary, Shadow};

const P: PartitionId = PartitionId(4);
const GEN: Generation = Generation(3);
const EPOCH: OwnerEpoch = OwnerEpoch(5);
const CONFIG: ConfigVersion = ConfigVersion(7);
const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const C: NodeId = NodeId(3);
const D: NodeId = NodeId(4);
const COPY_A: CopyId = CopyId(0);
const COPY_B: CopyId = CopyId(1);
const COPY_C: CopyId = CopyId(2);
const COPY_D: CopyId = CopyId(3);
const HEAD: u64 = 12;
const T: Tick = Tick(7);
// What `recovered` installs.
const NEW_GEN: Generation = Generation(4);
const NEW_EPOCH: OwnerEpoch = OwnerEpoch(6);
const NEW_CONFIG: ConfigVersion = ConfigVersion(8);

// --- fixture -----------------------------------------------------------------------------

/// The primary's digest at `seq`. The tracker never chains digests, so any injective map will do.
fn d(seq: u64) -> Digest {
    Digest::of(Domain::Record, &[&seq.to_le_bytes()])
}

fn member(copy: CopyId, node: NodeId, role: ReplicaRole) -> Member {
    Member {
        copy,
        node,
        boot: BootId(u64::from(node.0)),
        role,
    }
}

fn config_with(version: ConfigVersion, members: Vec<Member>) -> PartitionConfig {
    let mut config = PartitionConfig::new(P, version, members);
    config.min_regular_acks = 1;
    config
}

/// A, B, C, D as the module doc says, at `CONFIG`.
fn config() -> PartitionConfig {
    config_with(
        CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_B, B, RegularSecondary),
            member(COPY_C, C, RegularSecondary),
            member(COPY_D, D, Shadow),
        ],
    )
}

fn lineage() -> Lineage {
    Lineage {
        partition: P,
        generation: GEN,
        owner_epoch: EPOCH,
    }
}

fn progress(received: u64, applied: u64, durable: u64) -> ReplicaProgress {
    ReplicaProgress {
        received: ReceivedSeq(received),
        buffered_applied: AppliedSeq(applied),
        durable: DurableSeq(durable),
    }
}

/// Rungs `from..=HEAD`.
fn ladder(from: u64) -> DigestLadder {
    let mut ladder = DigestLadder::new();
    for seq in from..=HEAD {
        ladder.insert(Seq(seq), d(seq));
    }
    ladder
}

fn init(config: PartitionConfig) -> TrackerInit {
    TrackerInit {
        config,
        own: COPY_A,
        lineage: lineage(),
        history: ladder(5),
        local: progress(HEAD, HEAD, HEAD),
    }
}

fn tracker() -> ProgressTracker {
    ProgressTracker::new(init(config())).expect("golden tracker")
}

fn label(node: NodeId) -> PeerLabel {
    PeerLabel {
        node,
        boot: BootId(u64::from(node.0)),
        authenticated: true,
    }
}

/// `node`'s ACK in the golden lineage and configuration, with the primary's digest at `applied`.
fn ack(node: NodeId, role: ReplicaRole, received: u64, applied: u64, durable: u64) -> AppendAck {
    AppendAck {
        partition: P,
        generation: GEN,
        owner_epoch: EPOCH,
        config_version: CONFIG,
        from: node,
        boot: BootId(u64::from(node.0)),
        role,
        progress: progress(received, applied, durable),
        digest_at_buffered: d(applied),
    }
}

/// B's ACK at `(received, applied, durable)`.
fn b(received: u64, applied: u64, durable: u64) -> AppendAck {
    ack(B, RegularSecondary, received, applied, durable)
}

/// C's ACK at `(received, applied, durable)`.
fn c(received: u64, applied: u64, durable: u64) -> AppendAck {
    ack(C, RegularSecondary, received, applied, durable)
}

/// One field edit applied to a golden ACK.
type Change = dyn Fn(&mut AppendAck);

/// Deliver `ack` from `node`'s own label.
fn deliver(tracker: &mut ProgressTracker, ack: &AppendAck) -> Vec<EffectKind> {
    tracker.on_ack(&label(ack.from), ack, T)
}

/// `ack` with a digest the primary does not hold at its applied seq.
fn forked(mut ack: AppendAck) -> AppendAck {
    ack.digest_at_buffered = Digest([0xEE; 32]);
    ack
}

fn ignored(reason: KernelIgnoredReason) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Ignored { reason })
}

fn rejected(reason: AckRejectReason) -> EffectKind {
    ignored(KernelIgnoredReason::AckRejected(reason))
}

fn replica(reason: ReplicaIgnoreReason) -> EffectKind {
    ignored(KernelIgnoredReason::Replica(reason))
}

fn kernel(effect: KernelEffect) -> EffectKind {
    EffectKind::Kernel(effect)
}

fn peer_progress(node: NodeId, seq: u64) -> EffectKind {
    kernel(KernelEffect::PeerProgress {
        peer: node,
        contiguous_seq: Seq(seq),
    })
}

fn alert() -> EffectKind {
    kernel(KernelEffect::Alert {
        reason: ErrorKind::CorruptHistory,
    })
}

fn copy_lost(copy: CopyId) -> EffectKind {
    kernel(KernelEffect::CopyLost { copy })
}

fn durable_advanced(per_predicate: &[(ConfigVersion, u64)]) -> EffectKind {
    kernel(KernelEffect::DurableAdvanced {
        per_predicate: per_predicate
            .iter()
            .map(|&(version, seq)| (version, DurableSeq(seq)))
            .collect(),
    })
}

/// The edge at `at` under `lineage` and `config`.
fn edge(
    lineage: Lineage,
    config: ConfigVersion,
    at: u64,
    direction: QualificationDirection,
    copies: &[CopyId],
    cause: QualificationCause,
) -> EffectKind {
    kernel(KernelEffect::QualificationChanged(QualificationChanged {
        lineage,
        config_version: config,
        at_seq: Seq(at),
        direction,
        qualified_copies: copies.to_vec(),
        qualified_ack_count: u8::try_from(copies.len()).expect("few copies"),
        cause,
        tick: T,
    }))
}

fn gained(copies: &[CopyId]) -> EffectKind {
    edge(
        lineage(),
        CONFIG,
        HEAD,
        QualificationDirection::Gained,
        copies,
        QualificationCause::AckAdvanced,
    )
}

fn lost(cause: QualificationCause) -> EffectKind {
    edge(
        lineage(),
        CONFIG,
        HEAD,
        QualificationDirection::Lost,
        &[],
        cause,
    )
}

/// Deliver `ack` and assert it was dropped with `reason` and changed nothing.
fn dropped(
    tracker: &mut ProgressTracker,
    from: PeerLabel,
    ack: &AppendAck,
    reason: AckRejectReason,
) {
    let before = tracker.clone();
    assert_eq!(
        tracker.on_ack(&from, ack, T),
        vec![rejected(reason)],
        "{ack:?}"
    );
    assert_eq!(*tracker, before, "a dropped ACK changes nothing");
}

/// B and C both at `(HEAD, HEAD, HEAD)`: the predicate holds with two copies behind it.
fn both_caught_up() -> ProgressTracker {
    let mut tracker = tracker();
    deliver(&mut tracker, &b(HEAD, HEAD, HEAD));
    deliver(&mut tracker, &c(HEAD, HEAD, HEAD));
    tracker
}

// --- construction ------------------------------------------------------------------------

#[retcd_test]
fn new_refuses_an_impossible_start() {
    let mut not_primary = init(config());
    not_primary.own = COPY_B;
    let mut unordered = init(config());
    unordered.local = progress(HEAD, HEAD, HEAD + 1);
    let mut unvouched = init(config());
    unvouched.history = DigestLadder::new();
    let mut elsewhere = init(config());
    elsewhere.lineage.partition = PartitionId(9);
    let mut zero = config();
    zero.min_regular_acks = 0;
    for init in [
        not_primary,
        unordered,
        unvouched,
        elsewhere,
        self::init(zero),
    ] {
        assert!(ProgressTracker::new(init.clone()).is_err(), "{init:?}");
    }
    let tracker = tracker();
    assert_eq!(tracker.head(), Seq(HEAD));
    assert_eq!(tracker.node(), A);
    assert_eq!(
        tracker.peer(COPY_B).expect("B").progress,
        ReplicaProgress::EMPTY
    );
}

// --- rules 1-9 -----------------------------------------------------------------------------

#[retcd_test]
fn a_valid_ack_advances_its_copy_and_the_first_one_to_qualify_is_the_gained_edge() {
    let mut tracker = tracker();
    assert!(!tracker.qualifies_now(Seq(HEAD)));
    assert_eq!(
        deliver(&mut tracker, &b(HEAD, HEAD, 10)),
        vec![peer_progress(B, HEAD), gained(&[COPY_B])]
    );
    assert_eq!(
        tracker.peer(COPY_B).expect("B").progress,
        progress(HEAD, HEAD, 10)
    );
    assert!(tracker.qualifies_now(Seq(HEAD)));
    // The predicate already holds, so no edge. C was the durable floor at 0, so the view moves.
    assert_eq!(
        deliver(&mut tracker, &c(11, 11, 10)),
        vec![peer_progress(C, 11), durable_advanced(&[(CONFIG, 10)])]
    );
    assert_eq!(tracker.qualified_copies(Seq(11)), vec![COPY_B, COPY_C]);
    assert_eq!(tracker.qualified_copies(Seq(HEAD)), vec![COPY_B]);
}

#[retcd_test]
fn each_rule_drops_the_ack_with_its_own_reason_and_changes_nothing() {
    use AckRejectReason as R;
    let mut tracker = tracker();
    deliver(&mut tracker, &b(10, 10, 10));
    let golden = b(11, 11, 10);
    let edit = |change: &Change| {
        let mut ack = golden;
        change(&mut ack);
        ack
    };
    let unauthenticated = PeerLabel {
        authenticated: false,
        ..label(B)
    };
    let rebooted = PeerLabel {
        boot: BootId(99),
        ..label(B)
    };
    dropped(&mut tracker, unauthenticated, &golden, R::ForgedIdentity);
    dropped(&mut tracker, label(C), &golden, R::ForgedIdentity);
    dropped(
        &mut tracker,
        label(A),
        &edit(&|a| a.from = A),
        R::ForgedIdentity,
    );
    dropped(
        &mut tracker,
        label(NodeId(9)),
        &edit(&|a| a.from = NodeId(9)),
        R::NotAMember,
    );
    dropped(
        &mut tracker,
        label(B),
        &edit(&|a| a.partition = PartitionId(9)),
        R::NotAMember,
    );
    let cases: [(&Change, R); 9] = [
        (&|a| a.generation = Generation(2), R::StaleGeneration),
        (&|a| a.generation = Generation(4), R::StaleGeneration),
        (&|a| a.owner_epoch = OwnerEpoch(4), R::StaleEpoch),
        (&|a| a.config_version = ConfigVersion(8), R::StaleConfig),
        (&|a| a.role = Shadow, R::RoleMismatch),
        (&|a| a.boot = BootId(99), R::StaleBoot),
        (
            &|a| a.progress = progress(11, 12, 10),
            R::InconsistentProgress,
        ),
        (
            &|a| a.progress = progress(11, 11, 12),
            R::InconsistentProgress,
        ),
        (&|a| a.progress = progress(11, 11, 9), R::RegressedProgress),
    ];
    for (change, reason) in cases {
        dropped(&mut tracker, label(B), &edit(change), reason);
    }
    dropped(&mut tracker, rebooted, &golden, R::StaleBoot);
    for regressed in [progress(9, 9, 9), progress(9, 10, 10), progress(10, 9, 9)] {
        let mut ack = golden;
        ack.progress = regressed;
        ack.digest_at_buffered = d(regressed.buffered_applied.0);
        let expected = if regressed.buffered_applied.0 > regressed.received.0 {
            R::InconsistentProgress
        } else {
            R::RegressedProgress
        };
        dropped(&mut tracker, label(B), &ack, expected);
    }
    // No rule was an accident: the golden ACK itself is admitted.
    assert_eq!(deliver(&mut tracker, &golden), vec![peer_progress(B, 11)]);
}

#[retcd_test]
fn m7b_33_ack_ladder_reports_the_first_failure_in_order() {
    use AckRejectReason as R;
    let mut tracker = tracker();
    deliver(&mut tracker, &b(10, 10, 10));
    deliver(&mut tracker, &forked(c(HEAD, HEAD, HEAD)));
    let edited = |changes: &[&Change]| {
        let mut ack = b(11, 11, 10);
        for change in changes {
            change(&mut ack);
        }
        ack
    };
    // Each adjacent pair reports only the earlier rule; fixing that field surfaces the later.
    let pairs: [(&Change, &Change, R, R); 7] = [
        (
            &|a| a.partition = PartitionId(9),
            &|a| a.generation = Generation(2),
            R::NotAMember,
            R::StaleGeneration,
        ),
        (
            &|a| a.generation = Generation(2),
            &|a| a.owner_epoch = OwnerEpoch(4),
            R::StaleGeneration,
            R::StaleEpoch,
        ),
        (
            &|a| a.owner_epoch = OwnerEpoch(4),
            &|a| a.config_version = ConfigVersion(6),
            R::StaleEpoch,
            R::StaleConfig,
        ),
        (
            &|a| a.config_version = ConfigVersion(6),
            &|a| a.role = Shadow,
            R::StaleConfig,
            R::RoleMismatch,
        ),
        (
            &|a| a.role = Shadow,
            &|a| a.boot = BootId(99),
            R::RoleMismatch,
            R::StaleBoot,
        ),
        (
            &|a| a.boot = BootId(99),
            &|a| a.progress = progress(11, 12, 10),
            R::StaleBoot,
            R::InconsistentProgress,
        ),
        (
            &|a| a.progress = progress(9, 12, 10),
            &|a| a.progress.durable = DurableSeq(9),
            R::InconsistentProgress,
            R::RegressedProgress,
        ),
    ];
    for (first, second, reason, then) in pairs {
        dropped(&mut tracker, label(B), &edited(&[first, second]), reason);
        dropped(&mut tracker, label(B), &edited(&[second]), then);
    }
    // Rule 1 before 1d, and 1d before everything after it.
    let unauthenticated = PeerLabel {
        authenticated: false,
        ..label(C)
    };
    dropped(
        &mut tracker,
        unauthenticated,
        &c(HEAD, HEAD, HEAD),
        R::ForgedIdentity,
    );
    let mut stale = c(HEAD, HEAD, HEAD);
    stale.generation = Generation(2);
    dropped(&mut tracker, label(C), &stale, R::Diverged);
    let mut unordered = c(HEAD, HEAD, HEAD);
    unordered.progress = progress(11, 12, 10);
    dropped(&mut tracker, label(C), &unordered, R::Diverged);
    // Rules 8 and 9: a regressed ACK on another history is reported as regressed. Fixing its
    // progress surfaces rule 9, which proves the divergence.
    dropped(
        &mut tracker,
        label(B),
        &forked(b(9, 9, 9)),
        R::RegressedProgress,
    );
    let effects = deliver(&mut tracker, &forked(b(11, 11, 10)));
    assert!(
        effects.starts_with(&[
            kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
            alert(),
            copy_lost(COPY_B),
        ]),
        "{effects:?}"
    );
    assert!(tracker.is_diverged(COPY_B));
}

#[retcd_test]
fn rule_8_holds_each_watermark_on_its_own() {
    let mut tracker = tracker();
    deliver(&mut tracker, &b(HEAD, 11, 10));
    // Each ACK keeps rule 7's order and moves back exactly one watermark.
    for regressed in [b(11, 11, 10), b(HEAD, 10, 10), b(HEAD, 11, 9)] {
        dropped(
            &mut tracker,
            label(B),
            &regressed,
            AckRejectReason::RegressedProgress,
        );
    }
}

#[retcd_test]
fn m7b_40_duplicate_and_reordered_acks_leave_watermarks_monotone() {
    let mut tracker = tracker();
    let applied =
        |tracker: &ProgressTracker| tracker.peer(COPY_B).expect("B").progress.buffered_applied;
    deliver(&mut tracker, &b(11, 11, 10));
    assert_eq!(applied(&tracker), AppliedSeq(11));
    assert_eq!(
        deliver(&mut tracker, &b(11, 11, 10)),
        vec![peer_progress(B, 11)]
    );
    assert_eq!(applied(&tracker), AppliedSeq(11));
    deliver(&mut tracker, &b(HEAD, HEAD, 10));
    assert_eq!(applied(&tracker), AppliedSeq(HEAD));
    dropped(
        &mut tracker,
        label(B),
        &b(11, 11, 10),
        AckRejectReason::RegressedProgress,
    );
    assert_eq!(applied(&tracker), AppliedSeq(HEAD));
}

#[retcd_test]
fn a_shadow_ack_is_recorded_and_qualifies_nothing() {
    let mut tracker = tracker();
    assert_eq!(
        deliver(&mut tracker, &ack(D, Shadow, HEAD, HEAD, HEAD)),
        vec![peer_progress(D, HEAD)]
    );
    assert_eq!(
        tracker.peer(COPY_D).expect("D").progress,
        progress(HEAD, HEAD, HEAD)
    );
    assert!(tracker.qualified_copies(Seq(HEAD)).is_empty());
    assert!(!tracker.qualifies_now(Seq(HEAD)));
    assert_eq!(tracker.required_copies(), vec![COPY_A, COPY_B, COPY_C]);
}

#[retcd_test]
fn m7b_42_not_retained_at_buffered_is_unverifiable_never_divergence() {
    // Above the head needs a primary that received 13 but applied 12: rule 7 bounds an ACK's
    // `received` by the primary's own (B-R43 S3-F2).
    let mut ahead = init(config());
    ahead.local = progress(HEAD + 1, HEAD, HEAD);
    let ahead = ProgressTracker::new(ahead).expect("tracker");
    // Below the lowest rung and above the head: absence either way, never divergence.
    for (mut tracker, ack) in [
        (tracker(), b(4, 4, 4)),
        (tracker(), forked(b(4, 4, 4))),
        (ahead, b(13, 13, 10)),
    ] {
        let before = tracker.clone();
        assert_eq!(
            deliver(&mut tracker, &ack),
            vec![
                rejected(AckRejectReason::Unverifiable),
                kernel(KernelEffect::SnapshotCatchupRequired {
                    copy: COPY_B,
                    barrier: Seq(HEAD),
                }),
            ]
        );
        assert_eq!(tracker, before);
        assert!(!tracker.is_diverged(COPY_B));
    }
}

// --- divergence ----------------------------------------------------------------------------

#[retcd_test]
fn m7b_41_differs_at_buffered_marks_divergence_sticky_and_emits_the_vector() {
    let mut tracker = both_caught_up();
    let effects = deliver(&mut tracker, &forked(b(HEAD, HEAD, HEAD)));
    // C still qualifies and every durable view is where it was: the vector is three wide.
    assert_eq!(
        effects,
        vec![
            kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
            alert(),
            copy_lost(COPY_B),
        ]
    );
    assert_eq!(tracker.diverged(), &[COPY_B]);
    assert_eq!(tracker.regular_secondaries(), vec![COPY_C]);
    assert_eq!(tracker.qualified_copies(Seq(HEAD)), vec![COPY_C]);
    // Rule 1d: B's watermarks are frozen, whatever it sends next.
    let frozen = tracker.peer(COPY_B).expect("B").progress;
    dropped(
        &mut tracker,
        label(B),
        &b(13, 13, 13),
        AckRejectReason::Diverged,
    );
    assert_eq!(tracker.peer(COPY_B).expect("B").progress, frozen);
}

#[retcd_test]
fn the_last_regular_secondary_diverging_loses_the_predicate_and_blocks_the_partition() {
    let mut tracker = both_caught_up();
    deliver(&mut tracker, &forked(c(HEAD, HEAD, HEAD)));
    assert_eq!(
        deliver(&mut tracker, &forked(b(HEAD, HEAD, HEAD))),
        vec![
            kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
            alert(),
            copy_lost(COPY_B),
            lost(QualificationCause::DivergenceDetected(COPY_B)),
            kernel(KernelEffect::BlockPartition(
                BlockReason::DivergenceRequiresOperator {
                    diverged: vec![COPY_C, COPY_B],
                }
            )),
        ]
    );
    assert!(!tracker.qualifies_now(Seq(HEAD)));
}

#[retcd_test]
fn m7b_54_diverged_copy_leaves_the_durable_views_and_empty_floor_blocks() {
    let mut tracker = tracker();
    deliver(&mut tracker, &b(HEAD, HEAD, HEAD));
    deliver(&mut tracker, &c(HEAD, HEAD, 10));
    assert_eq!(tracker.min_required_durable(), DurableSeq(10));
    assert_eq!(
        deliver(&mut tracker, &forked(c(HEAD, HEAD, 10))),
        vec![
            kernel(KernelEffect::DivergenceDetected { copy: COPY_C }),
            alert(),
            copy_lost(COPY_C),
            durable_advanced(&[(CONFIG, HEAD)]),
        ]
    );
    assert!(tracker.all_durable_through(Seq(HEAD)));
    assert_eq!(tracker.min_required_durable(), DurableSeq(HEAD));
    dropped(
        &mut tracker,
        label(C),
        &c(HEAD, HEAD, HEAD),
        AckRejectReason::Diverged,
    );
    // B was the floor. Losing it loses the predicate and blocks the partition.
    assert_eq!(
        deliver(&mut tracker, &forked(b(HEAD, HEAD, HEAD))),
        vec![
            kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
            alert(),
            copy_lost(COPY_B),
            lost(QualificationCause::DivergenceDetected(COPY_B)),
            kernel(KernelEffect::BlockPartition(
                BlockReason::DivergenceRequiresOperator {
                    diverged: vec![COPY_C, COPY_B],
                }
            )),
        ]
    );
    assert_eq!(tracker.required_copies(), vec![COPY_A]);
}

#[retcd_test]
fn a_routed_divergence_has_one_writer_and_emits_the_vector_once() {
    let mut tracker = both_caught_up();
    // Index 0 is the alert: the routed event is the proof's own trace.
    assert_eq!(
        tracker.on_divergence(COPY_B, T),
        vec![alert(), copy_lost(COPY_B)]
    );
    assert_eq!(
        tracker.on_divergence(COPY_C, T),
        vec![
            alert(),
            copy_lost(COPY_C),
            lost(QualificationCause::DivergenceDetected(COPY_C)),
            kernel(KernelEffect::BlockPartition(
                BlockReason::DivergenceRequiresOperator {
                    diverged: vec![COPY_B, COPY_C],
                }
            )),
        ]
    );
    let before = tracker.clone();
    assert_eq!(
        tracker.on_divergence(COPY_B, T),
        vec![replica(ReplicaIgnoreReason::AlreadyDiverged)]
    );
    for stranger in [COPY_A, CopyId(9)] {
        assert_eq!(
            tracker.on_divergence(stranger, T),
            vec![rejected(AckRejectReason::NotAMember)]
        );
    }
    assert_eq!(tracker, before);
}

// --- control -------------------------------------------------------------------------------

/// `config()` at `version`, with B on `boot`.
fn reannounced(version: ConfigVersion, boot: BootId) -> PartitionConfig {
    let mut config = config();
    config.config_version = version;
    config.members[1].boot = boot;
    config
}

#[retcd_test]
fn a_control_announced_boot_zeroes_the_copy_and_leaves_diverged_alone() {
    let mut tracker = both_caught_up();
    tracker.on_divergence(COPY_C, T);
    let effects = tracker.on_config_changed(&reannounced(NEW_CONFIG, BootId(22)), T);
    // B restarted: it proved nothing, so the predicate is lost, and B's durable 12 leaves.
    assert_eq!(
        effects,
        vec![
            edge(
                lineage(),
                NEW_CONFIG,
                HEAD,
                QualificationDirection::Lost,
                &[],
                QualificationCause::StaleBoot(COPY_B),
            ),
            durable_advanced(&[(CONFIG, 0), (NEW_CONFIG, 0)]),
        ]
    );
    let peer = tracker.peer(COPY_B).expect("B");
    assert_eq!(
        (peer.boot, peer.progress),
        (BootId(22), ReplicaProgress::EMPTY)
    );
    assert!(
        tracker.is_diverged(COPY_C),
        "diverged is sticky across a reconfiguration"
    );
    // Its old boot is stale now; its new one is heard.
    let mut fresh = b(HEAD, HEAD, HEAD);
    fresh.config_version = NEW_CONFIG;
    dropped(&mut tracker, label(B), &fresh, AckRejectReason::StaleBoot);
    fresh.boot = BootId(22);
    let from = PeerLabel {
        boot: BootId(22),
        ..label(B)
    };
    assert_eq!(tracker.on_ack(&from, &fresh, T)[0], peer_progress(B, HEAD));
}

#[retcd_test]
fn a_configuration_that_is_not_newer_or_does_not_keep_us_primary_is_invalid() {
    let mut tracker = tracker();
    let before = tracker.clone();
    let mut demoted = reannounced(NEW_CONFIG, BootId(2));
    demoted.members[0].role = RegularSecondary;
    demoted.members[1].role = Primary;
    let mut moved = reannounced(NEW_CONFIG, BootId(2));
    moved.members[0].boot = BootId(11);
    let mut elsewhere = reannounced(NEW_CONFIG, BootId(2));
    elsewhere.partition = PartitionId(9);
    let mut zero = reannounced(NEW_CONFIG, BootId(2));
    zero.min_regular_acks = 0;
    let same = reannounced(CONFIG, BootId(22));
    let older = reannounced(ConfigVersion(6), BootId(2));
    for config in [demoted, moved, elsewhere, zero, same, older] {
        assert_eq!(
            tracker.on_config_changed(&config, T),
            vec![replica(ReplicaIgnoreReason::InvalidConfig)],
            "{config:?}"
        );
    }
    assert_eq!(tracker, before);
    // A newer configuration is a new active predicate, so the durable views gain an entry.
    assert_eq!(
        tracker.on_config_changed(&reannounced(NEW_CONFIG, BootId(2)), T),
        vec![durable_advanced(&[(CONFIG, 0), (NEW_CONFIG, 0)])]
    );
}

#[retcd_test]
fn retired_predicates_keep_their_copies_until_the_barrier_is_confirmed() {
    let mut tracker = both_caught_up();
    let next = config_with(
        NEW_CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_B, B, RegularSecondary),
            member(COPY_D, D, RegularSecondary),
        ],
    );
    // D is a regular now with nothing proved: the new predicate's durable view starts at 0.
    assert_eq!(
        tracker.on_config_changed(&next, T),
        vec![durable_advanced(&[(CONFIG, HEAD), (NEW_CONFIG, 0)])]
    );
    assert!(
        tracker.peer(COPY_C).is_some(),
        "nothing is removed before retirement"
    );
    // C keeps acknowledging under the configuration that still names it, and counts only there.
    assert_eq!(
        deliver(&mut tracker, &c(HEAD, HEAD, HEAD)),
        vec![peer_progress(C, HEAD)]
    );
    let mut b_next = b(HEAD, HEAD, HEAD);
    b_next.config_version = NEW_CONFIG;
    let mut d_next = ack(D, RegularSecondary, HEAD, HEAD, 10);
    d_next.config_version = NEW_CONFIG;
    dropped(
        &mut tracker,
        label(B),
        &b(HEAD, HEAD, HEAD),
        AckRejectReason::StaleConfig,
    );
    assert_eq!(
        deliver(&mut tracker, &d_next),
        vec![
            peer_progress(D, HEAD),
            durable_advanced(&[(CONFIG, HEAD), (NEW_CONFIG, 10)])
        ]
    );
    assert_eq!(tracker.regular_secondaries(), vec![COPY_B, COPY_D]);
    // The pinned configuration never retires, and an unknown one is not required.
    assert_eq!(
        tracker.on_transition_confirmed(NEW_CONFIG),
        vec![replica(ReplicaIgnoreReason::InvalidConfig)]
    );
    assert_eq!(
        tracker.on_transition_confirmed(ConfigVersion(1)),
        vec![replica(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(
        tracker.on_transition_confirmed(CONFIG),
        vec![durable_advanced(&[(NEW_CONFIG, 10)])]
    );
    assert_eq!(tracker.predicates().len(), 1);
    assert!(tracker.peer(COPY_C).is_none());
    dropped(
        &mut tracker,
        label(C),
        &c(13, HEAD, HEAD),
        AckRejectReason::NotAMember,
    );
    assert_eq!(
        tracker.on_ack(&label(B), &b_next, T),
        vec![peer_progress(B, HEAD)]
    );
}

// --- flush ---------------------------------------------------------------------------------

fn flush(generation: Generation, through: u64) -> Vec<DurablePrefix> {
    vec![DurablePrefix {
        partition: P,
        generation,
        through: DurableSeq(through),
    }]
}

#[retcd_test]
fn the_primary_can_be_the_laggard_and_its_own_flush_moves_the_durable_view() {
    let mut init = init(config());
    init.local = progress(HEAD, HEAD, 5);
    let mut tracker = ProgressTracker::new(init).expect("tracker");
    deliver(&mut tracker, &b(HEAD, HEAD, HEAD));
    deliver(&mut tracker, &c(HEAD, HEAD, HEAD));
    assert_eq!(tracker.min_required_durable(), DurableSeq(5));
    assert!(!tracker.all_durable_through(Seq(6)));
    assert_eq!(tracker.on_flushed(&flush(Generation(2), HEAD)), None);
    assert_eq!(
        tracker.on_flushed(&flush(GEN, 9)),
        Some(vec![durable_advanced(&[(CONFIG, 9)])])
    );
    // Never past what the primary has applied.
    assert_eq!(
        tracker.on_flushed(&flush(GEN, 99)),
        Some(vec![durable_advanced(&[(CONFIG, HEAD)])])
    );
    assert_eq!(
        tracker.on_flushed(&flush(GEN, HEAD)),
        Some(vec![replica(ReplicaIgnoreReason::NothingOutstanding)])
    );
    assert!(tracker.all_durable_through(Seq(HEAD)));
    assert!(!tracker.all_durable_through(Seq(HEAD + 1)));
}

#[retcd_test]
fn a_flush_that_moves_no_view_is_recorded() {
    let mut tracker = tracker();
    let mut init = self::init(config());
    init.local = progress(HEAD, HEAD, 5);
    let mut behind = ProgressTracker::new(init).expect("tracker");
    // B and C have proved nothing, so the floor is 0 whatever the primary syncs.
    assert_eq!(
        behind.on_flushed(&flush(GEN, HEAD)),
        Some(vec![replica(ReplicaIgnoreReason::Recorded)])
    );
    assert_eq!(
        behind.peer(COPY_A).expect("A").progress.durable,
        DurableSeq(HEAD)
    );
    assert_eq!(
        tracker.on_flushed(&flush(GEN, HEAD)),
        Some(vec![replica(ReplicaIgnoreReason::NothingOutstanding)])
    );
}

// --- Recovered -------------------------------------------------------------------------------

/// A committed recovery to `NEW_GEN`/`NEW_EPOCH` whose selected prefix ends at
/// `(cutoff, cutoff_digest)`, pinning `pinned`.
fn recovery(cutoff: u64, cutoff_digest: Digest, pinned: PartitionConfig) -> RecoveryResult {
    let cutoff = Seq(cutoff);
    let root = Lineage {
        partition: P,
        generation: NEW_GEN,
        owner_epoch: NEW_EPOCH,
    };
    RecoveryResult {
        fenced_prior: FencingProof {
            partition: P,
            prior_generation: GEN,
            prior_owner_epoch: EPOCH,
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
            root,
            cutoff_seq: cutoff,
            cutoff_digest,
            source: COPY_A,
        },
        new_generation: NEW_GEN,
        mode: PartitionMode::Active,
        barrier: RecoveryBarrier::try_new(&[], &Default::default(), cutoff, cutoff_digest)
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
                lineage: root,
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
            predecessor_generation: GEN,
            predecessor_cutoff: cutoff,
            retained_through: cutoff,
            discarded_from: None,
            uncertain: false,
        },
    }
}

/// A primary, C regular, D regular at `NEW_CONFIG`: B is gone.
fn pin_without_b() -> PartitionConfig {
    config_with(
        NEW_CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_C, C, RegularSecondary),
            member(COPY_D, D, RegularSecondary),
        ],
    )
}

/// A primary, B and C regular at `NEW_CONFIG`: the pin keeps B.
fn pin_with_b() -> PartitionConfig {
    config_with(
        NEW_CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_B, B, RegularSecondary),
            member(COPY_C, C, RegularSecondary),
        ],
    )
}

#[retcd_test]
fn m7b_45_recovered_rebuilds_the_tracker_from_the_result() {
    let mut tracker = both_caught_up();
    tracker.on_divergence(COPY_B, T);
    let new_root = Lineage {
        partition: P,
        generation: NEW_GEN,
        owner_epoch: NEW_EPOCH,
    };
    let effects = tracker.on_recovered(&recovery(10, d(10), pin_without_b()), T);
    assert_eq!(
        effects,
        vec![
            edge(
                new_root,
                NEW_CONFIG,
                10,
                QualificationDirection::Lost,
                &[],
                QualificationCause::ConfigChanged,
            ),
            durable_advanced(&[(NEW_CONFIG, 0)]),
        ]
    );
    assert_eq!(tracker.lineage(), new_root);
    assert_eq!(tracker.head(), Seq(10));
    assert!(
        tracker.diverged().is_empty(),
        "diverged clears here and only here"
    );
    assert!(tracker.peer(COPY_B).is_none());
    assert_eq!(
        tracker.peer(COPY_A).expect("A").progress,
        progress(10, 10, 10)
    );
    for (copy, boot) in [(COPY_C, BootId(3)), (COPY_D, BootId(4))] {
        let peer = tracker.peer(copy).expect("member");
        assert_eq!((peer.boot, peer.progress), (boot, ReplicaProgress::EMPTY));
    }
    assert_eq!(tracker.history().highest(), Some(Seq(10)));
    assert_eq!(tracker.predicates().len(), 1);
    // Every copy re-proves its prefix under the new root.
    let mut again = c(10, 10, 10);
    (again.generation, again.owner_epoch, again.config_version) = (NEW_GEN, NEW_EPOCH, NEW_CONFIG);
    assert_eq!(deliver(&mut tracker, &again)[0], peer_progress(C, 10));

    // The pin keeps the diverged B, and moves B to boot 22 and C to boot 33. Above, B left the
    // pin, so its removal alone emptied `diverged`, and C's and D's boots were the same in both
    // configurations; neither clause was told apart from a rebuild that skipped it (tester
    // probe p17, mutants A45a and A45b).
    let mut tracker = both_caught_up();
    tracker.on_divergence(COPY_B, T);
    let mut pin = pin_with_b();
    pin.members[1].boot = BootId(22);
    pin.members[2].boot = BootId(33);
    tracker.on_recovered(&recovery(10, d(10), pin), T);
    assert!(
        tracker.diverged().is_empty(),
        "diverged clears for a copy the new configuration keeps"
    );
    for (copy, boot) in [(COPY_B, BootId(22)), (COPY_C, BootId(33))] {
        let peer = tracker.peer(copy).expect("member");
        assert_eq!(
            (peer.boot, peer.progress),
            (boot, ReplicaProgress::EMPTY),
            "{copy:?}: boot from the new configuration"
        );
    }
    // B is heard again, at its new boot, and only there.
    let mut again = b(10, 10, 10);
    (again.generation, again.owner_epoch, again.config_version) = (NEW_GEN, NEW_EPOCH, NEW_CONFIG);
    dropped(&mut tracker, label(B), &again, AckRejectReason::StaleBoot);
    again.boot = BootId(22);
    let from = PeerLabel {
        boot: BootId(22),
        ..label(B)
    };
    assert_eq!(tracker.on_ack(&from, &again, T)[0], peer_progress(B, 10));
}

#[retcd_test]
fn recovered_is_refused_when_this_copy_cannot_lead_the_selected_prefix() {
    let before = both_caught_up();
    let mut demoted = pin_without_b();
    demoted.members[0].role = RegularSecondary;
    demoted.members[1].role = Primary;
    let mut moved = pin_without_b();
    moved.members[0].node = NodeId(9);
    let mut elsewhere = pin_without_b();
    elsewhere.partition = PartitionId(9);
    // Another partition's pin that would demote this node: still no retirement here.
    let mut demoted_elsewhere = demoted.clone();
    demoted_elsewhere.partition = PartitionId(9);
    // The third field: whether the pin retires the primary (lead ruling B-R58a). A pin that
    // names this node something other than the primary does; another partition's pin, or a
    // cutoff this copy cannot lead from, does not.
    let cases = [
        (
            recovery(10, d(10), demoted),
            replica(ReplicaIgnoreReason::InvalidConfig),
            true,
        ),
        (
            recovery(10, d(10), moved),
            replica(ReplicaIgnoreReason::InvalidConfig),
            true,
        ),
        (
            recovery(10, d(10), elsewhere),
            replica(ReplicaIgnoreReason::InvalidConfig),
            false,
        ),
        (
            recovery(10, d(10), demoted_elsewhere),
            replica(ReplicaIgnoreReason::InvalidConfig),
            false,
        ),
        (recovery(10, d(11), pin_without_b()), alert(), false),
        (
            recovery(4, d(4), pin_without_b()),
            replica(ReplicaIgnoreReason::BarrierNotDurable),
            false,
        ),
        (
            recovery(13, d(13), pin_without_b()),
            replica(ReplicaIgnoreReason::BarrierNotDurable),
            false,
        ),
    ];
    for (result, answer, retires) in cases {
        let mut tracker = before.clone();
        assert_eq!(tracker.on_recovered(&result, T), vec![answer]);
        assert_eq!(tracker.retired(), retires);
        // Retiring writes the flag and nothing else.
        let unflagged = format!("{tracker:?}").replace("retired: true", "retired: false");
        assert_eq!(unflagged, format!("{before:?}"));
    }
}

// --- the QualifiedPrefix seam (§3.5) ----------------------------------------------------------

#[retcd_test]
fn m7b_53_qualified_ack_count_never_counts_shadow_diverged_or_self() {
    // Every subset of {self, shadow D, diverged B, regular C} that has applied HEAD. No copy
    // can prove more than the primary received (rule 7, B-R43), so without self nothing
    // reaches HEAD; with it, only C counts.
    for subset in 0u8..16 {
        let has = |bit: u8| subset & (1 << bit) != 0;
        let applied = |yes: bool| if yes { HEAD } else { 11 };
        let mut init = init(config());
        let own = applied(has(0));
        init.local = progress(own, own, own);
        init.history.truncate_above(Seq(own));
        let mut tracker = ProgressTracker::new(init).expect("tracker");
        let d_seq = applied(has(1));
        deliver(&mut tracker, &ack(D, Shadow, d_seq, d_seq, 0));
        let b_seq = applied(has(2));
        deliver(&mut tracker, &b(b_seq, b_seq, 0));
        tracker.on_divergence(COPY_B, T);
        let c_seq = applied(has(3));
        deliver(&mut tracker, &c(c_seq, c_seq, 0));
        assert_eq!(
            tracker.qualified_ack_count(Seq(HEAD)),
            usize::from(has(0) && has(3)),
            "{subset:04b}"
        );
        assert_eq!(
            tracker.qualifies_now(Seq(HEAD)),
            has(0) && has(3),
            "{subset:04b}"
        );
    }
}

#[retcd_test]
fn m7b_49_two_of_two_needs_both_acks() {
    let mut two = config();
    two.min_regular_acks = 2;
    let mut tracker = ProgressTracker::new(init(two)).expect("tracker");
    assert_eq!(
        deliver(&mut tracker, &b(HEAD, HEAD, HEAD)),
        vec![peer_progress(B, HEAD)]
    );
    assert_eq!(tracker.qualified_ack_count(Seq(HEAD)), 1);
    assert!(!tracker.qualifies_now(Seq(HEAD)));
    assert_eq!(
        deliver(&mut tracker, &c(HEAD, HEAD, HEAD)),
        vec![
            peer_progress(C, HEAD),
            gained(&[COPY_B, COPY_C]),
            durable_advanced(&[(CONFIG, HEAD)]),
        ]
    );
    assert_eq!(tracker.qualified_ack_count(Seq(HEAD)), 2);
}

#[retcd_test]
fn m7b_48_digest_binding_rejects_an_ack_at_the_right_seq_wrong_history() {
    let mut tracker = tracker();
    // B at the right seq on the wrong history: dropped by rule 9, and it qualifies nothing.
    assert_eq!(
        deliver(&mut tracker, &forked(b(11, 11, 10))),
        vec![
            kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
            alert(),
            copy_lost(COPY_B),
        ]
    );
    assert!(tracker.qualified_copies(Seq(11)).is_empty());
    assert!(!tracker.qualifies_now(Seq(11)));
    // P1's third conjunct reads the same ladder rule 9 does.
    let history = tracker.history();
    assert_eq!(history.lookup(Seq(11), d(11)), DigestLookup::Match);
    assert!(matches!(
        history.lookup(Seq(11), Digest([0xEE; 32])),
        DigestLookup::Differs { .. }
    ));
    assert_eq!(history.lookup(Seq(4), d(4)), DigestLookup::NotRetained);
}

#[retcd_test]
fn m7b_51_rf2_degraded_is_one_of_one_and_stops_on_loss() {
    let rf2 = config_with(
        CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_B, B, RegularSecondary),
        ],
    );
    let mut tracker = ProgressTracker::new(init(rf2)).expect("tracker");
    assert_eq!(
        deliver(&mut tracker, &b(HEAD, HEAD, HEAD)),
        vec![
            peer_progress(B, HEAD),
            gained(&[COPY_B]),
            durable_advanced(&[(CONFIG, HEAD)]),
        ]
    );
    assert_eq!(
        deliver(&mut tracker, &forked(b(HEAD, HEAD, HEAD))),
        vec![
            kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
            alert(),
            copy_lost(COPY_B),
            lost(QualificationCause::DivergenceDetected(COPY_B)),
            kernel(KernelEffect::BlockPartition(
                BlockReason::DivergenceRequiresOperator {
                    diverged: vec![COPY_B],
                }
            )),
        ]
    );
    // No one-copy fallback: nothing qualifies, whatever seq.
    for seq in [1, HEAD] {
        assert!(!tracker.qualifies_now(Seq(seq)));
    }
    assert_eq!(tracker.required_copies(), vec![COPY_A]);
    dropped(
        &mut tracker,
        label(B),
        &b(13, 13, 13),
        AckRejectReason::Diverged,
    );
}

// --- M7B rows: the ACK ladder and the qualified-prefix seam (plan §4–§5) ---------------------
//
// The plan's golden fixture is B at (10, 10, 10) and B's ACK at (11, 11, 10) with `d11`, the
// predicate already true. Here C holds it at HEAD. Rows renamed from scaffolding above are
// M7B-33, -40, -41, -42, -45, -48, -49, -51, -53 and -54.

/// The plan's golden tracker: B at (10, 10, 10), C at HEAD, so the predicate already holds.
fn golden() -> ProgressTracker {
    let mut tracker = tracker();
    deliver(&mut tracker, &b(10, 10, 10));
    deliver(&mut tracker, &c(HEAD, HEAD, HEAD));
    tracker
}

/// The plan's golden ACK: B at (11, 11, 10), with the primary's digest at 11.
fn golden_ack() -> AppendAck {
    b(11, 11, 10)
}

/// The golden ACK with one field edited.
fn golden_with(change: &Change) -> AppendAck {
    let mut ack = golden_ack();
    change(&mut ack);
    ack
}

/// M7B-30. The landed `PeerProgress` carries no tick; the effect vector is the whole answer.
#[retcd_test]
fn m7b_30_valid_ack_advances_the_peer_and_emits_peer_progress() {
    let mut tracker = golden();
    assert!(tracker.qualifies_now(Seq(HEAD)));
    assert_eq!(
        deliver(&mut tracker, &golden_ack()),
        vec![peer_progress(B, 11)],
        "no QualificationChanged: the predicate already held"
    );
    assert_eq!(
        tracker.peer(COPY_B).expect("B").progress,
        progress(11, 11, 10)
    );
}

#[retcd_test]
fn m7b_31_forged_label_is_dropped_forged_ack() {
    let mut tracker = golden();
    let forged = PeerLabel {
        authenticated: false,
        ..label(B)
    };
    dropped(
        &mut tracker,
        forged,
        &golden_ack(),
        AckRejectReason::ForgedIdentity,
    );
}

#[retcd_test]
fn m7b_34_stale_generation_epoch_config_each_drop_the_ack() {
    use AckRejectReason as R;
    let mut tracker = golden();
    let cases: [(&Change, R); 3] = [
        (
            &|a| a.generation = Generation(GEN.0 - 1),
            R::StaleGeneration,
        ),
        (&|a| a.owner_epoch = OwnerEpoch(EPOCH.0 - 1), R::StaleEpoch),
        (
            &|a| a.config_version = ConfigVersion(CONFIG.0 - 1),
            R::StaleConfig,
        ),
    ];
    for (change, reason) in cases {
        dropped(&mut tracker, label(B), &golden_with(change), reason);
    }
}

/// M7B-35 (b) starts from a tracker whose predicate does not hold, so a shadow that counted
/// would show as a `Gained` edge.
#[retcd_test]
fn m7b_35_role_mismatch_is_dropped_and_shadow_ack_qualifies_nothing() {
    let mut tracker = golden();
    dropped(
        &mut tracker,
        label(B),
        &golden_with(&|a| a.role = Shadow),
        AckRejectReason::RoleMismatch,
    );
    let mut tracker = self::tracker();
    assert_eq!(
        deliver(&mut tracker, &ack(D, Shadow, 11, 11, 10)),
        vec![peer_progress(D, 11)]
    );
    assert_eq!(
        tracker.peer(COPY_D).expect("D").progress,
        progress(11, 11, 10)
    );
    assert!(!tracker.qualifies_now(Seq(11)));
    assert!(tracker.qualified_copies(Seq(11)).is_empty());
}

#[retcd_test]
fn m7b_36_stale_boot_on_ack_resets_nothing() {
    let mut tracker = golden();
    dropped(
        &mut tracker,
        label(B),
        &golden_with(&|a| a.boot = BootId(22)),
        AckRejectReason::StaleBoot,
    );
    assert_eq!(
        tracker.peer(COPY_B).expect("B").progress,
        progress(10, 10, 10)
    );
}

#[retcd_test]
fn m7b_37_control_announced_boot_change_zeroes_copy_and_keeps_diverged() {
    let mut tracker = golden();
    tracker.on_divergence(COPY_B, T);
    tracker.on_config_changed(&reannounced(NEW_CONFIG, BootId(22)), T);
    let peer = tracker.peer(COPY_B).expect("B");
    assert_eq!(
        (peer.boot, peer.progress),
        (BootId(22), ReplicaProgress::EMPTY)
    );
    assert!(tracker.is_diverged(COPY_B), "diverged is sticky");
}

/// Rules 7 and 8 drop the ACK, never the copy.
#[retcd_test]
fn m7b_38_inconsistent_progress_is_dropped() {
    let mut tracker = golden();
    for unordered in [progress(11, 12, 10), progress(11, 11, 12)] {
        dropped(
            &mut tracker,
            label(B),
            &golden_with(&move |a| a.progress = unordered),
            AckRejectReason::InconsistentProgress,
        );
    }
    assert!(!tracker.is_diverged(COPY_B));
}

#[retcd_test]
fn m7b_39_regressed_progress_is_dropped_and_watermarks_never_retreat() {
    let mut tracker = golden();
    dropped(
        &mut tracker,
        label(B),
        &b(9, 9, 9),
        AckRejectReason::RegressedProgress,
    );
    assert_eq!(
        tracker.peer(COPY_B).expect("B").progress,
        progress(10, 10, 10)
    );
    assert!(!tracker.is_diverged(COPY_B));
}

/// M7B-43. Recovery is the only clearer; M7B-45 asserts that half.
#[retcd_test]
fn m7b_43_diverged_flag_survives_boot_change_and_reconfig() {
    let mut tracker = golden();
    deliver(&mut tracker, &forked(golden_ack()));
    assert!(tracker.is_diverged(COPY_B));
    tracker.on_config_changed(&reannounced(NEW_CONFIG, BootId(22)), T);
    assert!(tracker.is_diverged(COPY_B), "after the boot change");
    tracker.on_config_changed(&reannounced(ConfigVersion(NEW_CONFIG.0 + 1), BootId(22)), T);
    assert_eq!(
        tracker.config().config_version,
        ConfigVersion(NEW_CONFIG.0 + 1)
    );
    assert!(
        tracker.config().member(COPY_B).is_some(),
        "B is still a member"
    );
    assert!(tracker.is_diverged(COPY_B), "after the reconfiguration");
}

#[retcd_test]
fn m7b_44_ack_from_a_copy_outside_the_pinned_config_has_nowhere_to_land() {
    let mut tracker = golden();
    let stranger = NodeId(9);
    dropped(
        &mut tracker,
        label(stranger),
        &golden_with(&move |a| a.from = stranger),
        AckRejectReason::NotAMember,
    );
    assert!(tracker.config().members.iter().all(|m| m.node != stranger));
}

/// M7B-46. `qualifies_now` is evaluated when asked, so excluding B bites at once; 11 stays
/// qualified only on the branch where C also holds it.
#[retcd_test]
fn m7b_46_qualifies_now_is_live_and_exclusion_bites_at_re_evaluation() {
    for c_holds_11 in [true, false] {
        let mut tracker = tracker();
        if c_holds_11 {
            deliver(&mut tracker, &c(11, 11, 10));
        }
        deliver(&mut tracker, &b(HEAD, HEAD, 10));
        assert!(tracker.qualifies_now(Seq(HEAD)), "{c_holds_11}");
        assert!(tracker.qualifies_now(Seq(11)), "{c_holds_11}");
        deliver(&mut tracker, &forked(b(HEAD, HEAD, 10)));
        assert!(tracker.is_diverged(COPY_B));
        assert!(!tracker.qualifies_now(Seq(HEAD)), "{c_holds_11}");
        assert_eq!(tracker.qualifies_now(Seq(11)), c_holds_11);
    }
}

#[retcd_test]
fn m7b_50_primary_as_laggard_lowers_min_required_durable() {
    let mut laggard = init(config());
    laggard.local = progress(HEAD, HEAD, 10);
    let mut tracker = ProgressTracker::new(laggard).expect("tracker");
    deliver(&mut tracker, &b(HEAD, HEAD, HEAD));
    deliver(&mut tracker, &c(HEAD, HEAD, HEAD));
    assert_eq!(tracker.min_required_durable(), DurableSeq(10));
    assert!(tracker.all_durable_through(Seq(10)));
    assert!(!tracker.all_durable_through(Seq(11)));
    assert!(tracker.required_copies().contains(&COPY_A));
}

/// M7B-52. Refused on every path: the constructor, the decoder, a tracker's start, and a
/// `ConfigChanged` that would pin it (the previous configuration stays in force).
#[retcd_test]
fn m7b_52_config_with_min_regular_acks_zero_is_rejected() {
    let zero_field = RdbError::InvalidArgument {
        field: "min_regular_acks",
    };
    assert_eq!(
        config().with_min_regular_acks(0).expect_err("zero"),
        zero_field
    );
    let mut wire = serde_json::to_value(config()).expect("encode");
    wire["min_regular_acks"] = 0.into();
    assert!(serde_json::from_value::<PartitionConfig>(wire).is_err());

    let mut zero = config();
    zero.min_regular_acks = 0;
    assert_eq!(
        ProgressTracker::new(init(zero)).expect_err("zero"),
        zero_field
    );
    let mut tracker = golden();
    let before = tracker.clone();
    let mut next = reannounced(NEW_CONFIG, BootId(2));
    next.min_regular_acks = 0;
    assert_eq!(
        tracker.on_config_changed(&next, T),
        vec![replica(ReplicaIgnoreReason::InvalidConfig)]
    );
    assert_eq!(tracker, before);
    assert_eq!(tracker.config().config_version, CONFIG);
}

// --- tester rows (manual tester, R1 slice 3) -----------------------------------------------

/// Tester row: rules 3 and 5 are equalities, so each refuses in both directions. A newer owner
/// epoch is as stale as an older one: the ACK was made under a lineage this primary does not
/// hold. A regular secondary claiming `Primary` is a role mismatch even though both roles may
/// qualify an ACK. Kills rule 3 weakened to `<` and rule 5 weakened to the qualify class.
#[retcd_test]
fn tester_r1s_rules_3_and_5_hold_in_both_directions() {
    let mut tracker = tracker();
    deliver(&mut tracker, &b(10, 10, 10));
    let mut newer_epoch = b(11, 11, 10);
    newer_epoch.owner_epoch = OwnerEpoch(EPOCH.0 + 1);
    dropped(
        &mut tracker,
        label(B),
        &newer_epoch,
        AckRejectReason::StaleEpoch,
    );
    let mut claims_primary = b(11, 11, 10);
    claims_primary.role = Primary;
    dropped(
        &mut tracker,
        label(B),
        &claims_primary,
        AckRejectReason::RoleMismatch,
    );
    let shadow_claims_regular = ack(D, RegularSecondary, HEAD, HEAD, HEAD);
    dropped(
        &mut tracker,
        label(D),
        &shadow_claims_regular,
        AckRejectReason::RoleMismatch,
    );
    // Positive control: the same ACK with the golden epoch and role is admitted.
    assert_eq!(
        deliver(&mut tracker, &b(11, 11, 10)),
        vec![peer_progress(B, 11)]
    );
}

/// Tester row for finding S3-F1, ruled B-R43: `PartitionConfig::validate` refuses it. The
/// tracker keys copies from the pinned configuration, so a configuration that names one copy
/// id twice, or places two copies on one node, lets one node's ACK stand for two copies: with
/// copy 1 on B and on C, one ACK from C makes `qualifies_now` true at 2 of 2. Refused at `new`
/// and at `ConfigChanged`. Developer change: `new`'s expected error is the contract's.
#[retcd_test]
fn tester_r1s_a_configuration_naming_a_copy_or_node_twice_is_refused() {
    let with = |members: Vec<Member>| {
        let mut config = config_with(CONFIG, members);
        config.min_regular_acks = 2;
        config
    };
    let copy_twice = with(vec![
        member(COPY_A, A, Primary),
        member(COPY_B, B, RegularSecondary),
        member(COPY_B, C, RegularSecondary),
    ]);
    let node_twice = with(vec![
        member(COPY_A, A, Primary),
        member(COPY_B, B, RegularSecondary),
        member(COPY_C, B, RegularSecondary),
    ]);
    let beside_the_primary = with(vec![
        member(COPY_A, A, Primary),
        member(COPY_B, A, RegularSecondary),
        member(COPY_C, C, RegularSecondary),
    ]);
    for config in [copy_twice, node_twice, beside_the_primary] {
        assert_eq!(
            ProgressTracker::new(init(config.clone())).err(),
            Some(RdbError::InvalidArgument { field: "members" }),
            "{config:?}"
        );
        let mut tracker = tracker();
        let before = tracker.clone();
        let mut newer = config;
        newer.config_version = NEW_CONFIG;
        assert_eq!(
            tracker.on_config_changed(&newer, T),
            vec![replica(ReplicaIgnoreReason::InvalidConfig)]
        );
        assert_eq!(tracker, before);
    }
}

/// Tester row for finding S3-F2, ruled B-R43 (the tester's default). `received` is
/// diagnostic and qualifies nothing (design §0), but rule 8 holds it monotone, so one ACK that
/// claims more than the primary holds freezes the copy: every later honest ACK is
/// `RegressedProgress` until control re-announces its boot. So rule 7 also requires
/// `received` to be at most the primary's own `received`, and drops the ACK as
/// `InconsistentProgress` otherwise.
#[retcd_test]
fn tester_r1s_an_ack_claiming_more_than_the_primary_holds_is_inconsistent() {
    let mut tracker = tracker();
    for received in [HEAD + 1, u64::MAX] {
        dropped(
            &mut tracker,
            label(B),
            &b(received, HEAD, HEAD),
            AckRejectReason::InconsistentProgress,
        );
    }
    // The copy is not wedged: its honest ACK still lands and qualifies.
    assert_eq!(
        deliver(&mut tracker, &b(HEAD, HEAD, HEAD)),
        vec![peer_progress(B, HEAD), gained(&[COPY_B])]
    );
}

/// Finding S3-F5, ruled B-R43: the primary's ladder vouches only for what the primary has
/// applied. A rung above its own head would let rule 9 admit an ACK for a record the primary
/// does not hold, and `qualifies_now` would then answer for it. `new` refuses such a ladder.
#[retcd_test]
fn new_refuses_a_ladder_rung_above_the_primarys_own_head() {
    let mut above = init(config());
    above.history.insert(Seq(HEAD + 1), d(HEAD + 1));
    assert_eq!(
        ProgressTracker::new(above).err(),
        Some(RdbError::InvalidArgument { field: "tracker" })
    );
    // Positive control: a ladder whose highest rung is the head is the golden start.
    assert_eq!(tracker().history().highest(), Some(Seq(HEAD)));
}

/// Tester row, slice 3 re-gate (B-R43, S3-F1's third shape). A regular copy on the primary's
/// own node lets the primary's node stand in for a secondary. `PartitionConfig::validate`
/// refuses it at `new` and at `ConfigChanged`, and the refused configuration changes nothing.
#[retcd_test]
fn tester_r1s_a_regular_copy_on_the_primary_node_is_refused() {
    let colocated = config_with(
        CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_B, A, RegularSecondary),
            member(COPY_C, C, RegularSecondary),
        ],
    );
    assert_eq!(
        ProgressTracker::new(init(colocated.clone())).err(),
        Some(RdbError::InvalidArgument { field: "members" })
    );
    let mut tracker = tracker();
    let before = tracker.clone();
    let mut newer = colocated;
    newer.config_version = NEW_CONFIG;
    assert_eq!(
        tracker.on_config_changed(&newer, T),
        vec![replica(ReplicaIgnoreReason::InvalidConfig)]
    );
    assert_eq!(tracker, before);
}

/// Tester row, slice 3 re-gate. `PeerProgress` reports the applied seq, never `received`. Fixture
/// edit 2 of B-R43 (`c(13, 12, 12)` became `c(12, 12, 12)`) removed the only developer ACK whose
/// `received` differed from its applied seq, and with it the only kill of the mutant that
/// reports `received` (A3). This keeps the two apart under a primary that received 13.
#[retcd_test]
fn tester_r1s_peer_progress_reports_the_applied_seq_not_received() {
    let mut ahead = init(config());
    ahead.local = progress(HEAD + 1, HEAD, HEAD);
    let mut tracker = ProgressTracker::new(ahead).expect("tracker");
    let effects = deliver(&mut tracker, &b(HEAD + 1, HEAD, HEAD));
    assert_eq!(effects[0], peer_progress(B, HEAD));
    assert_eq!(
        tracker.peer(COPY_B).map(|p| p.progress),
        Some(progress(HEAD + 1, HEAD, HEAD))
    );
}

// --- the primary's own history: `LocalApplied` (lead rulings B-R47, B-R47a) -----------------

/// The primary's own `LocalApplied` for `seq`, with the digest its ladder holds there. Seeded
/// the way L1's rows seed it: one call per record, in order.
fn local_applied(tracker: &mut ProgressTracker, seq: u64) -> Vec<EffectKind> {
    tracker.on_local_applied(Seq(seq), d(seq))
}

/// B-R47 ruling 1: `LocalApplied` grows the primary's own `received`, applied head and ladder
/// rung together. That closes R-F1: the copy's ACK for record 13, dropped while the primary had
/// not heard of 13, is admitted once it has. The ordering rule (the primary emits it before it
/// ships the record) is what makes the rule-7 bound sound, so the bound stays.
///
/// Update (B-R47b): the admitted ACK for 13 also reports `Gained` at the head 13, after the
/// anchor's `Gained` at 12, because one ACK made both qualify.
#[retcd_test]
fn local_applied_grows_received_applied_and_the_ladder_together() {
    let mut tracker = tracker();
    dropped(
        &mut tracker,
        label(B),
        &b(HEAD + 1, HEAD, HEAD),
        AckRejectReason::InconsistentProgress,
    );
    assert_eq!(
        local_applied(&mut tracker, HEAD + 1),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(tracker.head(), Seq(HEAD + 1));
    assert_eq!(
        tracker.peer(COPY_A).expect("A").progress,
        progress(HEAD + 1, HEAD + 1, HEAD)
    );
    assert_eq!(
        tracker.history().digest_at(Seq(HEAD + 1)),
        Some(d(HEAD + 1))
    );
    assert_eq!(
        deliver(&mut tracker, &b(HEAD + 1, HEAD + 1, HEAD)),
        vec![
            peer_progress(B, HEAD + 1),
            gained(&[COPY_B]),
            gained_at(HEAD + 1, &[COPY_B])
        ]
    );
}

/// B-R47 ruling 1: a gap or a regress stores nothing and says `OutOfOrder`.
#[retcd_test]
fn local_applied_out_of_order_stores_nothing() {
    let mut tracker = tracker();
    for seq in [HEAD + 2, HEAD, 5, 0, u64::MAX] {
        let before = tracker.clone();
        assert_eq!(
            local_applied(&mut tracker, seq),
            vec![replica(ReplicaIgnoreReason::OutOfOrder)],
            "{seq}"
        );
        assert_eq!(tracker, before, "{seq}");
    }
    // A primary that received 13 and 14 before applying them still applies 13 next, and its
    // received stays at 14: a local apply never pulls `received` back.
    let mut ahead = init(config());
    ahead.local = progress(HEAD + 2, HEAD, HEAD);
    let mut ahead = ProgressTracker::new(ahead).expect("tracker");
    // The anchor is the seed head, which is applied, not received (B-R47a).
    assert_eq!(ahead.anchor(), Seq(HEAD));
    local_applied(&mut ahead, HEAD + 1);
    assert_eq!(
        ahead.peer(COPY_A).expect("A").progress,
        progress(HEAD + 2, HEAD + 1, HEAD)
    );
}

/// B-R47a: the edge is `qualifies_now(anchor)`, and the anchor is the head the tracker was
/// seeded at. A local write past it emits no edge and keeps the cached value, so an ACK for the
/// newest record does not report a second `Gained` at the anchor.
///
/// Update (B-R47b): that ACK makes the head 15 qualify, so it reports `Gained` at 15 — the
/// head's edge, not a repeat of the anchor's.
#[retcd_test]
fn local_applied_past_the_anchor_emits_no_edge_and_keeps_the_cached_value() {
    let mut tracker = tracker();
    assert_eq!(
        deliver(&mut tracker, &b(HEAD, HEAD, HEAD)),
        vec![peer_progress(B, HEAD), gained(&[COPY_B])]
    );
    for seq in HEAD + 1..=HEAD + 3 {
        assert_eq!(
            local_applied(&mut tracker, seq),
            vec![replica(ReplicaIgnoreReason::Recorded)]
        );
    }
    assert_eq!(tracker.anchor(), Seq(HEAD));
    assert!(tracker.qualifies_now(tracker.anchor()));
    assert_eq!(
        deliver(&mut tracker, &b(HEAD + 3, HEAD + 3, HEAD)),
        vec![peer_progress(B, HEAD + 3), gained_at(HEAD + 3, &[COPY_B])]
    );
}

/// B-R47a: a divergence after local writes still flips `Lost`, at the anchor. Evaluated at the
/// moving head the predicate would already be false, and the `Lost` L1 pauses on would be lost.
#[retcd_test]
fn a_divergence_after_local_writes_still_flips_lost_at_the_anchor() {
    let mut tracker = tracker();
    deliver(&mut tracker, &b(HEAD, HEAD, HEAD));
    local_applied(&mut tracker, HEAD + 1);
    local_applied(&mut tracker, HEAD + 2);
    assert_eq!(
        tracker.on_divergence(COPY_B, T),
        vec![
            alert(),
            copy_lost(COPY_B),
            lost(QualificationCause::DivergenceDetected(COPY_B)),
        ]
    );
}

/// B-R47a: a rebuild re-seeds the anchor at the cutoff. After recovery at 13 and a local write
/// of 14, a copy that proves 13 under the new root gains the predicate at 13, not at 14.
#[retcd_test]
fn a_rebuild_moves_the_anchor_to_the_cutoff() {
    let mut tracker = tracker();
    local_applied(&mut tracker, HEAD + 1);
    tracker.on_recovered(&recovery(HEAD + 1, d(HEAD + 1), pin_without_b()), T);
    assert_eq!(tracker.anchor(), Seq(HEAD + 1));
    local_applied(&mut tracker, HEAD + 2);
    assert_eq!(tracker.anchor(), Seq(HEAD + 1));
    let mut ack = c(HEAD + 1, HEAD + 1, 0);
    (ack.generation, ack.owner_epoch, ack.config_version) = (NEW_GEN, NEW_EPOCH, NEW_CONFIG);
    let effects = deliver(&mut tracker, &ack);
    let new_root = Lineage {
        partition: P,
        generation: NEW_GEN,
        owner_epoch: NEW_EPOCH,
    };
    assert_eq!(
        effects,
        vec![
            peer_progress(C, HEAD + 1),
            edge(
                new_root,
                NEW_CONFIG,
                HEAD + 1,
                QualificationDirection::Gained,
                &[COPY_C],
                QualificationCause::AckAdvanced,
            ),
        ]
    );
}

/// B-R47 ruling 3: a copy ahead of the primary's own head (a tail the primary never had) stores
/// nothing, and asks for a snapshot, as before B-R43. S3-F2 stays closed: an inflated `received`
/// at or below the head is still `InconsistentProgress`.
#[retcd_test]
fn an_ack_past_the_primarys_own_head_asks_for_a_snapshot_and_stores_nothing() {
    let mut tracker = tracker();
    for ack in [b(HEAD + 2, HEAD + 2, HEAD), b(u64::MAX, u64::MAX, 0)] {
        let before = tracker.clone();
        assert_eq!(
            deliver(&mut tracker, &ack),
            vec![
                rejected(AckRejectReason::Unverifiable),
                kernel(KernelEffect::SnapshotCatchupRequired {
                    copy: COPY_B,
                    barrier: Seq(HEAD),
                }),
            ],
            "{ack:?}"
        );
        assert_eq!(tracker, before);
    }
    dropped(
        &mut tracker,
        label(B),
        &b(HEAD + 1, HEAD, HEAD),
        AckRejectReason::InconsistentProgress,
    );
}

// --- tester rows (B-R47 re-gate, 2026-09-22) -------------------------------------------------

/// Tester row, B-R47 ruling 3 at a moving head. After local writes the head has left the
/// anchor, and rule 7 must follow the head, not the anchor: an inflated `received` with applied
/// at the head (above the anchor) is still `InconsistentProgress`, and an ACK past the head asks
/// for a snapshot at the head it has now. The ahead exemption lifts only the received bound: an
/// unordered ACK past the head is still dropped. "Ahead" is applied past the applied head, even
/// on a seed whose `received` is higher.
#[retcd_test]
fn tester_r1s_rule_7_follows_the_moving_head_and_still_holds_order() {
    use AckRejectReason as R;
    let mut tracker = tracker();
    for seq in HEAD + 1..=HEAD + 3 {
        local_applied(&mut tracker, seq);
    }
    let head = HEAD + 3;
    assert_eq!(tracker.anchor(), Seq(HEAD));
    dropped(
        &mut tracker,
        label(B),
        &b(head + 1, head, head),
        R::InconsistentProgress,
    );
    let before = tracker.clone();
    assert_eq!(
        deliver(&mut tracker, &b(head + 2, head + 2, head)),
        vec![
            rejected(R::Unverifiable),
            kernel(KernelEffect::SnapshotCatchupRequired {
                copy: COPY_B,
                barrier: Seq(head),
            }),
        ]
    );
    assert_eq!(tracker, before);
    for unordered in [b(head + 1, head + 2, head), b(head + 5, head + 5, head + 6)] {
        dropped(&mut tracker, label(B), &unordered, R::InconsistentProgress);
    }
    // The honest copy is not frozen by any of that.
    assert_eq!(
        deliver(&mut tracker, &b(head, head, head))[0],
        peer_progress(B, head)
    );
    // A seed that received 14 but applied 12: an ACK applied 13, received 16, is ahead.
    let mut ahead = init(config());
    ahead.local = progress(HEAD + 2, HEAD, HEAD);
    let mut ahead = ProgressTracker::new(ahead).expect("tracker");
    assert_eq!(
        deliver(&mut ahead, &b(HEAD + 4, HEAD + 1, HEAD + 1)),
        vec![
            rejected(R::Unverifiable),
            kernel(KernelEffect::SnapshotCatchupRequired {
                copy: COPY_B,
                barrier: Seq(HEAD),
            }),
        ]
    );
}

/// Tester row, B-R47 ruling 1 at the top of the range. A primary whose head is `u64::MAX` has
/// no next sequence: `u64::MAX` again is a duplicate and `0` is not a wrap. Both store nothing.
#[retcd_test]
fn tester_r1s_local_applied_at_the_top_of_the_range_stores_nothing() {
    let mut top = init(config());
    let mut rungs = DigestLadder::new();
    rungs.insert(Seq(u64::MAX), d(u64::MAX));
    top.history = rungs;
    top.local = progress(u64::MAX, u64::MAX, 0);
    let mut tracker = ProgressTracker::new(top).expect("tracker");
    for seq in [u64::MAX, 0] {
        let before = tracker.clone();
        assert_eq!(
            local_applied(&mut tracker, seq),
            vec![replica(ReplicaIgnoreReason::OutOfOrder)],
            "{seq}"
        );
        assert_eq!(tracker, before, "{seq}");
    }
}

// --- routing through `Replication::step` (lead ruling B-R48) --------------------------------

/// R1 never reads through the snapshot.
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
const BUDGETS: Budgets = Budgets::SPEC_DEFAULTS;

/// A's step context at tick `T`: every tracker effect a routed event makes is stamped `T`,
/// exactly as the directly driven reference is.
fn step_ctx() -> StepCtx<'static> {
    StepCtx {
        now: T,
        control_time: ControlTime {
            estimate: T,
            error_millis: 10,
            bound_established: true,
            sampled_at: T,
        },
        node: A,
        boot: BootId(1),
        partition: P,
        generation: GEN,
        owner_epoch: EPOCH,
        config_version: CONFIG,
        snapshot: &SNAPSHOT,
        budgets: &BUDGETS,
    }
}

/// A module hosting the golden tracker as A's primary side, with no cursor running.
fn routed() -> Replication {
    let mut module = Replication::new();
    module.install_primary(tracker());
    module
}

fn primary_side(module: &Replication) -> &PrimarySide {
    module.primary(A, P).expect("installed")
}

/// Step `kind` on `node` for partition `P`, as `Replication::step` answers it.
fn step_on(
    module: &mut Replication,
    node: NodeId,
    kind: EventKind,
) -> Result<Vec<Effect>, RdbError> {
    let event = Event {
        id: EventId(1),
        at: T,
        node,
        boot: BootId(u64::from(node.0)),
        partition: P,
        correlation: CorrelationId(9),
        kind,
    };
    module.step(&step_ctx(), &event)
}

/// Route `kind` to A and return the effect kinds, after checking each is R1's and stamped with
/// the event's correlation and partition.
fn route(module: &mut Replication, kind: EventKind) -> Vec<EffectKind> {
    step_on(module, A, kind)
        .expect("R1 answers its own event")
        .into_iter()
        .map(|effect| {
            assert_eq!(
                (effect.correlation, effect.from, effect.partition),
                (CorrelationId(9), ModuleName::Replication, P)
            );
            effect.kind
        })
        .collect()
}

/// A reply frame with body `body`, delivered from `from`.
fn raw_reply(from: PeerLabel, body: Bytes) -> EventKind {
    EventKind::Transport(TransportEvent::Delivered {
        from,
        frame: Frame {
            id: MessageId(42),
            protocol: ENVELOPE_VERSION,
            config: CONFIG,
            sender: lineage(),
            body,
        },
    })
}

/// A reply frame carrying `outcome`, delivered from `from`.
fn reply_from(from: PeerLabel, outcome: &AppendOutcome) -> EventKind {
    raw_reply(from, encode_reply(outcome))
}

/// A reply frame carrying `outcome`, from `node`'s own label.
fn reply(node: NodeId, outcome: &AppendOutcome) -> EventKind {
    reply_from(label(node), outcome)
}

/// `ack` as the `Accepted` reply from its own node.
fn accepted(ack: &AppendAck) -> EventKind {
    reply(ack.from, &AppendOutcome::Accepted(*ack))
}

fn local_applied_event(seq: u64) -> EventKind {
    EventKind::Kernel(KernelEvent::LocalApplied {
        seq: Seq(seq),
        bytes: 1,
        record_digest: d(seq),
    })
}

/// `NeedPrefix` from a copy holding the primary's own record at `have`.
fn need_prefix(have: u64) -> AppendOutcome {
    AppendOutcome::Rejected(AppendReject::NeedPrefix {
        have: Seq(have),
        head_digest: d(have),
    })
}

/// A direct call on a tracker, the reference a routed input is compared with.
type Direct<'a> = dyn Fn(&mut ProgressTracker) -> Vec<EffectKind> + 'a;

fn send(copy: CopyId, seq: u64) -> EffectKind {
    kernel(KernelEffect::SendEnvelopes {
        copy,
        from: Seq(seq),
        through: Seq(seq),
    })
}

/// B's cursor, started by `NeedPrefix(10)`: record 11 is in flight.
fn catching_up_b() -> Replication {
    let mut module = routed();
    assert_eq!(
        route(&mut module, reply(B, &need_prefix(10))),
        vec![send(COPY_B, 11), retransmit_arm(1)]
    );
    assert_eq!(
        primary_side(&module)
            .cursor(COPY_B)
            .expect("running")
            .outstanding(),
        Some(Seq(11))
    );
    module
}

/// B and C at the head through routing, with no cursor running.
fn both_routed() -> Replication {
    let mut module = routed();
    for ack in [b(HEAD, HEAD, HEAD), c(HEAD, HEAD, HEAD)] {
        route(&mut module, accepted(&ack));
    }
    module
}

fn caught_up(copy: CopyId, head: u64) -> EffectKind {
    kernel(KernelEffect::CopyCaughtUp {
        copy,
        head: Seq(head),
        digest: d(head),
    })
}

/// B-R48 ruling 2: routing delivers a partition's events in order, and R1 relies on it. A
/// `LocalApplied(13)` routed before B's ACK for 13 lets the ACK in. The same ACK routed first
/// is past the primary's head, is not verified, and stores nothing. The sim's one queue per
/// partition supplies the first order; a lost `LocalApplied` stays the P1 gap-Alert item.
///
/// Update (B-R67i): the routed `LocalApplied` ships 13 to B and C and arms the retransmit,
/// where it was `Recorded`. Update (B-R47b): the ACK also reports `Gained` at the head 13.
#[retcd_test]
fn routing_hands_local_applied_to_the_tracker_before_an_ack_naming_its_seq() {
    let ack = b(HEAD + 1, HEAD + 1, HEAD);
    let mut in_order = routed();
    assert_eq!(
        route(&mut in_order, local_applied_event(HEAD + 1)),
        vec![
            send(COPY_B, HEAD + 1),
            send(COPY_C, HEAD + 1),
            retransmit_arm(1)
        ]
    );
    assert_eq!(
        route(&mut in_order, accepted(&ack)),
        vec![
            peer_progress(B, HEAD + 1),
            gained(&[COPY_B]),
            gained_at(HEAD + 1, &[COPY_B])
        ]
    );

    let mut overtaken = routed();
    assert_eq!(
        route(&mut overtaken, accepted(&ack)),
        vec![
            rejected(AckRejectReason::Unverifiable),
            kernel(KernelEffect::SnapshotCatchupRequired {
                copy: COPY_B,
                barrier: Seq(HEAD),
            }),
        ]
    );
    assert_eq!(*primary_side(&overtaken).tracker(), tracker());
    // Once the tracker has heard of 13, the copy's next ACK is admitted.
    route(&mut overtaken, local_applied_event(HEAD + 1));
    assert_eq!(
        route(&mut overtaken, accepted(&ack))[0],
        peer_progress(B, HEAD + 1)
    );
}

/// B-R48 ruling 6: the cursor is dropped on the ACK that reports `CopyCaughtUp`. A later ACK
/// reaches the tracker alone, and an admitted ACK never starts a cursor.
#[retcd_test]
fn a_cursor_is_dropped_on_the_ack_that_reports_copy_caught_up() {
    let mut module = catching_up_b();
    let mut reference = tracker();
    let first = b(11, 11, 10);
    let mut want = deliver(&mut reference, &first);
    want.push(send(COPY_B, HEAD));
    assert_eq!(route(&mut module, accepted(&first)), want);

    let closing = b(HEAD, HEAD, 11);
    let mut want = deliver(&mut reference, &closing);
    want.push(kernel(KernelEffect::CopyCaughtUp {
        copy: COPY_B,
        head: Seq(HEAD),
        digest: d(HEAD),
    }));
    assert_eq!(route(&mut module, accepted(&closing)), want);
    assert!(primary_side(&module).cursor(COPY_B).is_none());

    let later = b(HEAD, HEAD, HEAD);
    assert_eq!(
        route(&mut module, accepted(&later)),
        deliver(&mut reference, &later)
    );
    assert!(primary_side(&module).cursor(COPY_B).is_none());
    assert_eq!(*primary_side(&module).tracker(), reference);
}

/// The cursor chases the primary's moving head, not the anchor. B's ACKs for 13..=15 are lost
/// while C keeps up; B asks from 12 when 16 lands, and 17 lands while B catches up. Every ACK
/// sends the next record, `CopyCaughtUp` names 17, the head B actually reached, and B's later
/// ACKs reach the tracker alone. Handed the anchor, the cursor would send nothing past 12
/// (tester probe p02, mutant P14).
///
/// Update (B-R67i): the stream's ship of 13 already armed the retransmit, so the cursor's first
/// send arms nothing. Update (B-R47b): B's ACKs at 17 and 18 make the head qualify, so each also
/// reports `Gained` there.
#[retcd_test]
fn m7b_152_a_copy_that_falls_behind_catches_up_to_the_head_that_moved_under_it() {
    let mut module = both_routed();
    for seq in HEAD + 1..=HEAD + 3 {
        route(&mut module, local_applied_event(seq));
        route(&mut module, accepted(&c(seq, seq, seq)));
    }
    route(&mut module, local_applied_event(HEAD + 4));
    assert_eq!(
        route(&mut module, reply(B, &need_prefix(HEAD))),
        vec![send(COPY_B, HEAD + 1)]
    );
    for seq in HEAD + 1..HEAD + 5 {
        if seq == HEAD + 3 {
            route(&mut module, local_applied_event(HEAD + 5));
        }
        assert_eq!(
            route(&mut module, accepted(&b(seq, seq, seq))),
            vec![peer_progress(B, seq), send(COPY_B, seq + 1)],
            "B's ACK at {seq}"
        );
    }
    let top = HEAD + 5;
    assert_eq!(
        route(&mut module, accepted(&b(top, top, top))),
        vec![
            peer_progress(B, top),
            gained_at(top, &[COPY_B]),
            caught_up(COPY_B, top)
        ]
    );
    assert!(primary_side(&module).cursor(COPY_B).is_none());
    route(&mut module, local_applied_event(top + 1));
    assert_eq!(
        route(&mut module, accepted(&b(top + 1, top + 1, top + 1))),
        vec![peer_progress(B, top + 1), gained_at(top + 1, &[COPY_B])]
    );
}

/// B-R48 ruling 6: the cursor is dropped on any stop. The next non-ACK reply starts a fresh
/// one, where a stopped cursor kept alive would answer with a no-op instead.
#[retcd_test]
fn a_cursor_is_dropped_when_it_stops() {
    let stale = AppendReject::StaleEpoch {
        current: OwnerEpoch(4),
    };
    let forked_prefix = AppendOutcome::Rejected(AppendReject::NeedPrefix {
        have: Seq(10),
        head_digest: Digest([0xEE; 32]),
    });
    for (stop, effect) in [
        (
            AppendOutcome::Rejected(AppendReject::Quarantined),
            kernel(KernelEffect::CopyQuarantined { copy: COPY_B }),
        ),
        (
            AppendOutcome::Rejected(stale),
            ignored(KernelIgnoredReason::AppendRejected(stale)),
        ),
        (
            forked_prefix,
            kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
        ),
    ] {
        let mut module = catching_up_b();
        assert_eq!(
            route(&mut module, reply(B, &stop)),
            vec![effect],
            "{stop:?}"
        );
        assert!(primary_side(&module).cursor(COPY_B).is_none(), "{stop:?}");
        assert_eq!(
            route(&mut module, reply(B, &need_prefix(10))),
            vec![send(COPY_B, 11)],
            "{stop:?}"
        );
    }
}

/// B-R48 ruling 6 and c11's case: `Recovered` drops every cursor. B's cursor was chasing head
/// 12 when recovery cut the head to 10. Kept alive, it would call B caught up at 10 on the
/// first ACK B sends under the new root. A copy past the cut gets the snapshot request.
#[retcd_test]
fn recovered_drops_every_cursor_and_a_head_cut_reports_no_copy_caught_up() {
    let result = recovery(10, d(10), pin_with_b());
    let mut module = catching_up_b();
    let mut reference = tracker();
    assert_eq!(
        route(
            &mut module,
            EventKind::Kernel(KernelEvent::Recovered(Box::new(result.clone())))
        ),
        reference.on_recovered(&result, T)
    );
    assert!(primary_side(&module).cursor(COPY_B).is_none());

    let new_root = |mut ack: AppendAck| {
        (ack.generation, ack.owner_epoch, ack.config_version) = (NEW_GEN, NEW_EPOCH, NEW_CONFIG);
        ack
    };
    let at_cut = new_root(b(10, 10, 10));
    let want = deliver(&mut reference, &at_cut);
    assert_eq!(want[0], peer_progress(B, 10), "the tracker admits it");
    assert_eq!(route(&mut module, accepted(&at_cut)), want);
    assert_eq!(
        route(&mut module, accepted(&new_root(b(HEAD, HEAD, 10)))),
        vec![
            rejected(AckRejectReason::Unverifiable),
            kernel(KernelEffect::SnapshotCatchupRequired {
                copy: COPY_B,
                barrier: Seq(10),
            }),
        ]
    );
    assert!(primary_side(&module).cursor(COPY_B).is_none());
}

/// `Recovered` on a node that holds both halves of the partition answers the receiver's effects
/// first, then the primary's. A leads P and also holds copy D as a secondary under a
/// configuration B leads; the pin names A primary, so the receiver refuses it while the primary
/// rebuilds. The routed vector is the receiver's answer followed by the primary's, each equal to
/// what that half gives alone (tester probe p12, mutant RT16).
#[retcd_test]
fn m7b_151_recovered_answers_the_receiver_first_on_a_node_holding_both_halves() {
    let result = recovery(10, d(10), pin_with_b());
    let receiver = AppendReceiver::new(ReceiverInit {
        config: config_with(
            CONFIG,
            vec![
                member(COPY_B, B, Primary),
                member(COPY_D, A, RegularSecondary),
            ],
        ),
        own: COPY_D,
        lineage: lineage(),
        head: Head {
            seq: Seq(10),
            digest: d(10),
        },
        durable: DurableSeq(10),
    })
    .expect("a receiver on A");
    let recovered = || EventKind::Kernel(KernelEvent::Recovered(Box::new(result.clone())));

    // The half itself, not a module holding only it: routed alone, `Recovered` would also try to
    // build A's primary (lead ruling B-R54), and this row compares halves.
    let from_receiver = receiver.clone().on_recovered(&result);
    assert_eq!(
        from_receiver,
        vec![replica(ReplicaIgnoreReason::InvalidConfig)]
    );
    let from_primary = route(&mut catching_up_b(), recovered());
    assert_eq!(from_primary, tracker().on_recovered(&result, T));
    assert!(!from_primary.is_empty() && from_primary != from_receiver);

    let mut module = catching_up_b();
    module.install_receiver(receiver);
    let mut want = from_receiver;
    want.extend(from_primary);
    assert_eq!(route(&mut module, recovered()), want);
    assert!(primary_side(&module).cursor(COPY_B).is_none());
}

/// Only an ACK the tracker admits reaches a running cursor. The cursor trusts what it is given,
/// so a forged, diverged or otherwise dropped ACK must not move it.
///
/// First, six ACKs from B's own label, which `sender()` passes and the ladder drops, each
/// claiming the progress that would close B's gap, and one below what B holds, which is `Recorded`
/// as a repeat (update B-R67f): B is at 12, the head at 16, 13 in flight.
/// Each is answered with its drop reason alone and leaves the primary as it was. Handed to the
/// cursor, the first would report `CopyCaughtUp` for B at 16 (tester probe p16, mutant P07).
/// Then a forged label and a forked digest, which `sender()` refuses as well.
///
/// Update (B-R67i): the stream's ship of 13 already armed the retransmit, so the cursor's first
/// send arms nothing.
#[retcd_test]
fn only_an_admitted_ack_reaches_a_running_cursor() {
    let top = HEAD + 4;
    let mut module = both_routed();
    for seq in HEAD + 1..=top {
        route(&mut module, local_applied_event(seq));
    }
    assert_eq!(
        route(&mut module, reply(B, &need_prefix(HEAD))),
        vec![send(COPY_B, HEAD + 1)]
    );
    let closing = |change: &Change| {
        let mut ack = b(top, top, top);
        change(&mut ack);
        ack
    };
    for (ack, reason) in [
        (
            closing(&|ack| ack.generation = Generation(GEN.0 - 1)),
            AckRejectReason::StaleGeneration,
        ),
        (
            closing(&|ack| ack.owner_epoch = OwnerEpoch(EPOCH.0 - 1)),
            AckRejectReason::StaleEpoch,
        ),
        (
            closing(&|ack| ack.config_version = ConfigVersion(CONFIG.0 - 1)),
            AckRejectReason::StaleConfig,
        ),
        (
            closing(&|ack| ack.role = Shadow),
            AckRejectReason::RoleMismatch,
        ),
        (
            closing(&|ack| ack.boot = BootId(99)),
            AckRejectReason::StaleBoot,
        ),
        (
            closing(&move |ack| ack.progress = progress(top - 1, top, top - 1)),
            AckRejectReason::InconsistentProgress,
        ),
    ] {
        let before = primary_side(&module).clone();
        assert_eq!(
            route(&mut module, accepted(&ack)),
            vec![rejected(reason)],
            "{reason:?}"
        );
        assert_eq!(*primary_side(&module), before, "{reason:?}");
    }
    // Update (B-R67f): an ACK below what the tracker holds for B, with no mark on B's cursor,
    // repeats what the primary knows. It is `Recorded` before rule 8 is asked, and it still
    // reaches neither the cursor nor the tracker.
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, accepted(&b(HEAD - 1, HEAD - 1, HEAD - 1))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(*primary_side(&module), before);

    let mut module = catching_up_b();
    let running = primary_side(&module).cursor(COPY_B).cloned();
    assert_eq!(
        route(
            &mut module,
            reply_from(label(C), &AppendOutcome::Accepted(b(11, 11, 10)))
        ),
        vec![rejected(AckRejectReason::ForgedIdentity)]
    );
    assert_eq!(
        route(&mut module, accepted(&forked(b(11, 11, 10)))),
        vec![
            kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
            alert(),
            copy_lost(COPY_B),
        ]
    );
    assert_eq!(primary_side(&module).cursor(COPY_B).cloned(), running);
}

/// A reply that is not an ACK carries no identity of its own, so its label must name a member
/// that is not this node and has not diverged. Each refusal changes nothing and starts no
/// cursor.
#[retcd_test]
fn a_reply_that_is_not_an_ack_needs_a_live_member_behind_its_label() {
    let mut unauthenticated = label(B);
    unauthenticated.authenticated = false;
    for (from, reason) in [
        (unauthenticated, AckRejectReason::ForgedIdentity),
        (label(NodeId(9)), AckRejectReason::NotAMember),
        (label(A), AckRejectReason::ForgedIdentity),
    ] {
        let mut module = routed();
        assert_eq!(
            route(&mut module, reply_from(from, &need_prefix(10))),
            vec![rejected(reason)],
            "{from:?}"
        );
        assert_eq!(
            *primary_side(&module),
            PrimarySide::new(tracker()),
            "{from:?}"
        );
    }
    let mut module = routed();
    route(
        &mut module,
        EventKind::Kernel(KernelEvent::DivergenceDetected { copy: COPY_B }),
    );
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, reply(B, &need_prefix(10))),
        vec![rejected(AckRejectReason::Diverged)]
    );
    assert_eq!(*primary_side(&module), before);
}

/// Every primary-side input reaches the tracker exactly as if driven directly. An undecodable
/// reply is answered with its error's kind. An event for a node with no primary, a flush that
/// names no prefix of ours, and an event the primary does not consume are declined.
///
/// Update (B-R67i): `LocalApplied` still reaches the tracker as the direct call does, and the
/// tracker's `Recorded` is answered instead by the stream's sends to B and C and the retransmit
/// arm.
#[retcd_test]
fn every_primary_input_reaches_the_tracker_as_if_driven_directly() {
    let next = config_with(
        NEW_CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_B, B, RegularSecondary),
            member(COPY_D, D, RegularSecondary),
        ],
    );
    let mut module = routed();
    let mut reference = tracker();
    let flushed = flush(GEN, HEAD + 1);
    // Each input, and the direct call it must be equivalent to.
    let steps: Vec<(EventKind, Box<Direct>)> = vec![
        (
            local_applied_event(HEAD + 1),
            Box::new(|t| {
                assert_eq!(
                    t.on_local_applied(Seq(HEAD + 1), d(HEAD + 1)),
                    vec![replica(ReplicaIgnoreReason::Recorded)]
                );
                vec![
                    send(COPY_B, HEAD + 1),
                    send(COPY_C, HEAD + 1),
                    retransmit_arm(1),
                ]
            }),
        ),
        (
            accepted(&c(HEAD + 1, HEAD + 1, HEAD)),
            Box::new(|t| deliver(t, &c(HEAD + 1, HEAD + 1, HEAD))),
        ),
        (
            EventKind::Kernel(KernelEvent::ConfigChanged(next.clone())),
            Box::new(|t| t.on_config_changed(&next, T)),
        ),
        (
            EventKind::Kernel(KernelEvent::TransitionBarrierConfirmed {
                config_version: NEW_CONFIG,
                through_seq: Seq(HEAD + 1),
            }),
            Box::new(|t| t.on_transition_confirmed(NEW_CONFIG)),
        ),
        (
            EventKind::Kernel(KernelEvent::CopyQuarantined { copy: COPY_B }),
            Box::new(|t| t.on_divergence(COPY_B, T)),
        ),
        (
            EventKind::Storage(StorageEvent::Flushed {
                ticket: FlushTicket(1),
                durable: flushed.clone(),
            }),
            Box::new(|t| t.on_flushed(&flushed).expect("names A's prefix")),
        ),
    ];
    for (kind, direct) in steps {
        let want = direct(&mut reference);
        assert_eq!(route(&mut module, kind.clone()), want, "{kind:?}");
        assert_eq!(*primary_side(&module).tracker(), reference, "{kind:?}");
    }

    let truncated = encode_reply(&need_prefix(10)).slice(..8);
    let kind = decode_reply(&truncated).expect_err("truncated").kind();
    assert_eq!(
        route(&mut module, raw_reply(label(B), truncated)),
        vec![ignored(KernelIgnoredReason::Error(kind))]
    );

    let declined = |result: Result<Vec<Effect>, RdbError>| {
        assert!(
            matches!(result, Err(RdbError::Unavailable { .. })),
            "{result:?}"
        );
    };
    let ack = b(HEAD, HEAD, HEAD);
    declined(step_on(&mut Replication::new(), A, accepted(&ack)));
    declined(step_on(
        &mut Replication::new(),
        A,
        local_applied_event(HEAD + 1),
    ));
    declined(step_on(&mut routed(), B, accepted(&ack)));
    declined(step_on(
        &mut routed(),
        A,
        EventKind::Kernel(KernelEvent::CopyLost { copy: COPY_B }),
    ));
    declined(step_on(
        &mut routed(),
        A,
        EventKind::Storage(StorageEvent::Flushed {
            ticket: FlushTicket(1),
            durable: flush(Generation(9), HEAD),
        }),
    ));
}

// --- cursor life cycle (lead ruling B-R48a) ------------------------------------------------

fn config_event(config: PartitionConfig) -> EventKind {
    EventKind::Kernel(KernelEvent::ConfigChanged(config))
}

fn barrier_event(config_version: ConfigVersion) -> EventKind {
    EventKind::Kernel(KernelEvent::TransitionBarrierConfirmed {
        config_version,
        through_seq: Seq(HEAD),
    })
}

/// `pin_with_b` one version later: B back after it was retired.
fn re_adds_b() -> PartitionConfig {
    let mut config = pin_with_b();
    config.config_version = ConfigVersion(9);
    config
}

/// Route B out of every predicate and back in, checking each control step against `reference`:
/// pin `pin_without_b`, retire `CONFIG`, pin `re_adds_b`, retire `NEW_CONFIG`.
fn retire_and_re_add_b(module: &mut Replication, reference: &mut ProgressTracker) {
    assert_eq!(
        route(module, config_event(pin_without_b())),
        reference.on_config_changed(&pin_without_b(), T)
    );
    assert_eq!(
        route(module, barrier_event(CONFIG)),
        reference.on_transition_confirmed(CONFIG)
    );
    assert!(primary_side(module).tracker().peer(COPY_B).is_none());
    assert_eq!(
        route(module, config_event(re_adds_b())),
        reference.on_config_changed(&re_adds_b(), T)
    );
    assert_eq!(
        route(module, barrier_event(NEW_CONFIG)),
        reference.on_transition_confirmed(NEW_CONFIG)
    );
}

/// B-R48a F1 (reverses B-R48 Q2): `Busy` and `AlreadyHave` answer an envelope the copy was sent.
/// With no catch-up running, that envelope was the stream's, so the reply starts no cursor.
///
/// The trace: B and C at the head; B answers `Busy` or `AlreadyHave`; 20 writes, with B's ACK
/// for each arriving `lag` writes later; then the drain to the head. Every routed step answers
/// exactly what the tracker answers when driven directly: no record is re-sent beside the
/// stream, and no `CopyCaughtUp` is reported for a copy that was never behind. Before B-R48a the
/// reply made a cursor; at lag 1 that re-sent 19 records and reported `CopyCaughtUp` on the
/// drain (tester probe p04). The label is still checked first.
#[retcd_test]
fn a_busy_or_already_have_reply_starts_no_cursor_and_a_steady_copy_is_never_caught_up() {
    let writes = 20;
    for kick in [
        AppendOutcome::Busy {
            accepted_through: Seq(HEAD),
        },
        AppendOutcome::AlreadyHave,
    ] {
        // Lag 0 last: there the old cursor sent nothing and only added a `Recorded`.
        for lag in [1, 2, 0] {
            let mut module = routed();
            let mut reference = tracker();
            for ack in [b(HEAD, HEAD, HEAD), c(HEAD, HEAD, HEAD)] {
                assert_eq!(
                    route(&mut module, accepted(&ack)),
                    deliver(&mut reference, &ack)
                );
            }
            assert_eq!(
                route(&mut module, reply(B, &kick)),
                vec![replica(ReplicaIgnoreReason::NothingOutstanding)],
                "{kick:?}"
            );
            assert!(primary_side(&module).cursor(COPY_B).is_none(), "{kick:?}");

            let mut acks = (HEAD + 1..=HEAD + writes).map(|at| b(at, at, at));
            for seq in HEAD + 1..=HEAD + writes {
                // Update (B-R67i): each write is the stream's, to B and C, and the first arms
                // the retransmit; the tracker still takes it as the direct call does.
                assert_eq!(
                    reference.on_local_applied(Seq(seq), d(seq)),
                    vec![replica(ReplicaIgnoreReason::Recorded)]
                );
                let mut want = vec![send(COPY_B, seq), send(COPY_C, seq)];
                want.extend((seq == HEAD + 1).then(|| retransmit_arm(1)));
                assert_eq!(route(&mut module, local_applied_event(seq)), want);
                if seq > HEAD + lag {
                    let ack = acks.next().expect("one ACK per write");
                    assert_eq!(
                        route(&mut module, accepted(&ack)),
                        deliver(&mut reference, &ack),
                        "{kick:?} lag {lag}: B's ACK at {}",
                        ack.progress.received.0
                    );
                }
            }
            for ack in acks {
                assert_eq!(
                    route(&mut module, accepted(&ack)),
                    deliver(&mut reference, &ack),
                    "{kick:?} lag {lag}: drain at {}",
                    ack.progress.received.0
                );
            }
            assert!(primary_side(&module).cursor(COPY_B).is_none(), "{kick:?}");
            assert_eq!(*primary_side(&module).tracker(), reference, "{kick:?}");
        }
        let mut module = routed();
        assert_eq!(
            route(&mut module, reply_from(label(NodeId(9)), &kick)),
            vec![rejected(AckRejectReason::NotAMember)],
            "{kick:?}"
        );
    }
}

/// B-R48a F1, the other half: `Busy` and `AlreadyHave` still update a cursor that is running.
/// The trace: B asks for 11, answers `Busy` or `AlreadyHave`, then ACKs 11. The reply clears
/// what is in flight and keeps the cursor; the ACK sends the next record.
#[retcd_test]
fn a_busy_or_already_have_reply_still_moves_a_running_cursor() {
    for kick in [
        AppendOutcome::Busy {
            accepted_through: Seq(10),
        },
        AppendOutcome::AlreadyHave,
    ] {
        let mut module = catching_up_b();
        assert_eq!(
            route(&mut module, reply(B, &kick)),
            vec![replica(ReplicaIgnoreReason::Recorded)],
            "{kick:?}"
        );
        let running = primary_side(&module).cursor(COPY_B).expect("still running");
        assert_eq!(running.outstanding(), None, "{kick:?}");

        let mut reference = tracker();
        let ack = b(11, 11, 10);
        let mut want = deliver(&mut reference, &ack);
        want.push(send(COPY_B, HEAD));
        assert_eq!(route(&mut module, accepted(&ack)), want, "{kick:?}");
    }
}

/// M7B-149 (B-R48a F2, ruling B-R48b Q1): a copy's cursor goes when the copy leaves every active
/// predicate, and a re-added copy starts a fresh one.
///
/// The trace: B's cursor has answered three probes and C's cursor has 11 in flight. Pinning a
/// configuration without B keeps B's cursor, because the retiring predicate still names B. The
/// barrier retires it: B leaves every predicate and its cursor goes; C's stays. B's replies are
/// then `NotAMember`. B is re-added one version later, and its probes get the full
/// `MAX_PROBE_ROUNDS` answers before the snapshot request, as on a fresh primary. B's ACK at the
/// head is then tracker progress, and the fresh cursor, which sent no record, only records it:
/// no `CopyCaughtUp` from the catch-up the old cursor started. Before B-R48a the old cursor
/// lived on, and the re-added copy got one answer (tester probe p06).
#[retcd_test]
fn m7b_149_a_retired_copy_loses_its_cursor_and_a_re_added_copy_starts_fresh() {
    let probe = AppendOutcome::ProbeDigestAt { seq: Seq(8) };
    let mut module = catching_up_b();
    let mut reference = tracker();
    for _ in 0..3 {
        assert_eq!(route(&mut module, reply(B, &probe)), vec![send(COPY_B, 8)]);
    }
    assert_eq!(
        route(&mut module, reply(C, &need_prefix(10))),
        vec![send(COPY_C, 11)]
    );

    assert_eq!(
        route(&mut module, config_event(pin_without_b())),
        reference.on_config_changed(&pin_without_b(), T)
    );
    assert_eq!(
        primary_side(&module)
            .cursor(COPY_B)
            .map(CatchupCursor::probe_rounds),
        Some(3),
        "the retiring predicate still names B"
    );

    assert_eq!(
        route(&mut module, barrier_event(CONFIG)),
        reference.on_transition_confirmed(CONFIG)
    );
    assert!(primary_side(&module).tracker().peer(COPY_B).is_none());
    assert!(primary_side(&module).cursor(COPY_B).is_none(), "B left");
    assert_eq!(
        primary_side(&module)
            .cursor(COPY_C)
            .and_then(CatchupCursor::outstanding),
        Some(Seq(11)),
        "C is still a member"
    );
    assert_eq!(
        route(&mut module, reply(B, &need_prefix(10))),
        vec![rejected(AckRejectReason::NotAMember)]
    );

    assert_eq!(
        route(&mut module, config_event(re_adds_b())),
        reference.on_config_changed(&re_adds_b(), T)
    );
    assert_eq!(
        route(&mut module, barrier_event(NEW_CONFIG)),
        reference.on_transition_confirmed(NEW_CONFIG)
    );
    assert!(primary_side(&module).cursor(COPY_B).is_none());
    for round in 1..=MAX_PROBE_ROUNDS {
        assert_eq!(
            route(&mut module, reply(B, &probe)),
            vec![send(COPY_B, 8)],
            "probe {round}"
        );
    }
    assert_eq!(
        route(&mut module, reply(B, &probe)),
        vec![kernel(KernelEffect::SnapshotCatchupRequired {
            copy: COPY_B,
            barrier: Seq(HEAD),
        })]
    );

    let mut at_head = b(HEAD, HEAD, HEAD);
    at_head.config_version = re_adds_b().config_version;
    let mut want = deliver(&mut reference, &at_head);
    assert_eq!(want[0], peer_progress(B, HEAD), "the tracker admits it");
    want.push(replica(ReplicaIgnoreReason::Recorded));
    assert_eq!(route(&mut module, accepted(&at_head)), want);
    assert_eq!(*primary_side(&module).tracker(), reference);
}

/// B-R48a F2: the catch-up a retired copy's cursor started does not finish on the re-added
/// copy. The trace: B's cursor has 11 in flight, B is retired and re-added, and B's first ACK
/// under the new configuration is at the head. That ACK is tracker progress only. Before B-R48a
/// the old cursor took it and reported `CopyCaughtUp` for a catch-up no live cursor ran
/// (tester probe p06).
#[retcd_test]
fn a_re_added_copy_is_not_reported_caught_up_by_the_cursor_it_left_behind() {
    let mut module = catching_up_b();
    let mut reference = tracker();
    retire_and_re_add_b(&mut module, &mut reference);
    assert!(primary_side(&module).cursor(COPY_B).is_none());

    let mut at_head = b(HEAD, HEAD, HEAD);
    at_head.config_version = re_adds_b().config_version;
    let want = deliver(&mut reference, &at_head);
    assert_eq!(want[0], peer_progress(B, HEAD), "the tracker admits it");
    assert_eq!(route(&mut module, accepted(&at_head)), want);
    assert!(primary_side(&module).cursor(COPY_B).is_none());
    assert_eq!(*primary_side(&module).tracker(), reference);
}

/// M7B-150 (ruling B-R48b Q2): a copy control announces at a new boot has restarted. Its
/// `CopyProgress` is fresh, and the cursor its old incarnation ran goes with it.
///
/// The trace: B proved 10, asked from 10 (11 in flight) and has had three probes answered.
/// `ConfigChanged` re-announces B, still a member, at boot 22. B's next probe starts a new
/// cursor at one round, and B's ACK at the head is tracker progress that the new cursor, which
/// sent no record, only records: no `CopyCaughtUp` on the old incarnation's catch-up. The
/// near-miss twin re-announces B at its own boot: the cursor is kept, the probe is its fourth,
/// and the same ACK closes the catch-up it started.
///
/// Widened by ruling B-R51c (tester probes p18–p20, hand mutants H4–H6):
/// (a) B moved to node 9 at the same boot is a restart too: fresh, cursor gone, and a probe
/// from node 9 starts a new cursor at one round;
/// (b) a refused `ConfigChanged` announcing B at boot 22 (stale version, `min_regular_acks` 0,
/// a pin demoting A) answers `InvalidConfig` and leaves the primary byte-equal, cursor included;
/// (c) with C's cursor running (11 in flight), B's restart drops B's cursor only; when B and C
/// both restart, both go.
#[retcd_test]
fn m7b_150_a_copy_restarted_by_control_drops_its_old_cursor() {
    let probe = AppendOutcome::ProbeDigestAt { seq: Seq(8) };
    for (boot, restarted) in [(BootId(22), true), (BootId(2), false)] {
        let from = PeerLabel { boot, ..label(B) };
        let (mut module, mut reference) = b_mid_probe();

        let announced = reannounced(NEW_CONFIG, boot);
        assert_eq!(
            route(&mut module, config_event(announced.clone())),
            reference.on_config_changed(&announced, T),
            "{boot:?}"
        );
        let peer = *primary_side(&module).tracker().peer(COPY_B).expect("B");
        let kept = if restarted {
            ReplicaProgress::EMPTY
        } else {
            progress(10, 10, 10)
        };
        assert_eq!((peer.boot, peer.progress), (boot, kept), "{boot:?}");
        assert_eq!(
            primary_side(&module).cursor(COPY_B).is_none(),
            restarted,
            "{boot:?}"
        );

        assert_eq!(
            route(&mut module, reply_from(from, &probe)),
            vec![send(COPY_B, 8)],
            "{boot:?}"
        );
        let rounds = if restarted { 1 } else { 4 };
        assert_eq!(
            primary_side(&module)
                .cursor(COPY_B)
                .map(CatchupCursor::probe_rounds),
            Some(rounds),
            "{boot:?}"
        );

        let mut at_head = b(HEAD, HEAD, HEAD);
        (at_head.config_version, at_head.boot) = (NEW_CONFIG, boot);
        let mut want = reference.on_ack(&from, &at_head, T);
        assert_eq!(
            want[0],
            peer_progress(B, HEAD),
            "{boot:?}: the tracker admits it"
        );
        want.push(if restarted {
            replica(ReplicaIgnoreReason::Recorded)
        } else {
            caught_up(COPY_B, HEAD)
        });
        assert_eq!(
            route(
                &mut module,
                reply_from(from, &AppendOutcome::Accepted(at_head))
            ),
            want,
            "{boot:?}"
        );
        assert_eq!(*primary_side(&module).tracker(), reference, "{boot:?}");
    }

    // (a) A new node at the same boot.
    let moved_to = NodeId(9);
    let (mut module, mut reference) = b_mid_probe();
    let mut moved = config();
    moved.config_version = NEW_CONFIG;
    moved.members[1].node = moved_to;
    assert_eq!(
        route(&mut module, config_event(moved.clone())),
        reference.on_config_changed(&moved, T)
    );
    let peer = *primary_side(&module).tracker().peer(COPY_B).expect("B");
    assert_eq!(
        (peer.node, peer.boot, peer.progress),
        (moved_to, BootId(2), ReplicaProgress::EMPTY)
    );
    assert!(
        primary_side(&module).cursor(COPY_B).is_none(),
        "B moved node at the same boot: its old cursor goes"
    );
    assert_eq!(
        route(&mut module, reply(B, &need_prefix(10))),
        vec![rejected(AckRejectReason::NotAMember)],
        "the old node is no longer B"
    );
    let from = PeerLabel {
        node: moved_to,
        ..label(B)
    };
    assert_eq!(
        route(&mut module, reply_from(from, &probe)),
        vec![send(COPY_B, 8)]
    );
    assert_eq!(
        primary_side(&module)
            .cursor(COPY_B)
            .map(CatchupCursor::probe_rounds),
        Some(1)
    );

    // (b) A refused configuration announcing a new boot drops nothing.
    let mut stale = config();
    stale.members[1].boot = BootId(22);
    let mut zero = stale.clone();
    zero.config_version = NEW_CONFIG;
    zero.min_regular_acks = 0;
    let mut demoted = zero.clone();
    demoted.min_regular_acks = 1;
    demoted.members[0].role = RegularSecondary;
    demoted.members[2].role = Primary;
    for (name, refused) in [("stale", stale), ("zero", zero), ("demoted", demoted)] {
        let (mut module, _) = b_mid_probe();
        let before = primary_side(&module).clone();
        assert_eq!(
            route(&mut module, config_event(refused)),
            vec![replica(ReplicaIgnoreReason::InvalidConfig)],
            "{name}"
        );
        assert_eq!(*primary_side(&module), before, "{name}: cursor kept");
    }

    // (c) One copy's restart drops that copy's cursor only.
    for c_restarts in [false, true] {
        let (mut module, _) = b_mid_probe();
        assert_eq!(
            route(&mut module, reply(C, &need_prefix(10))),
            vec![send(COPY_C, 11)]
        );
        let mut announced = reannounced(NEW_CONFIG, BootId(22));
        if c_restarts {
            announced.members[2].boot = BootId(33);
        }
        route(&mut module, config_event(announced));
        assert!(
            primary_side(&module).cursor(COPY_B).is_none(),
            "C restarts: {c_restarts}"
        );
        assert_eq!(
            primary_side(&module)
                .cursor(COPY_C)
                .and_then(CatchupCursor::outstanding),
            if c_restarts { None } else { Some(Seq(11)) },
            "C restarts: {c_restarts}"
        );
    }
}

/// M7B-150's start: B proved 10, asked from 10 (11 in flight), and has had three probes
/// answered. Returns the module and the tracker driven directly to the same state.
fn b_mid_probe() -> (Replication, ProgressTracker) {
    let mut module = routed();
    let mut reference = tracker();
    let proved = b(10, 10, 10);
    assert_eq!(
        route(&mut module, accepted(&proved)),
        deliver(&mut reference, &proved)
    );
    assert_eq!(
        route(&mut module, reply(B, &need_prefix(10))),
        vec![send(COPY_B, 11), retransmit_arm(1)]
    );
    for _ in 0..3 {
        assert_eq!(
            route(
                &mut module,
                reply(B, &AppendOutcome::ProbeDigestAt { seq: Seq(8) })
            ),
            vec![send(COPY_B, 8)]
        );
    }
    (module, reference)
}

// --- A1's view on the primary (design §2.2, lead ruling B-R53) -------------------------------

/// A1's view of `P` in `generation`, published at `seq`, pinning `epoch` and `config`.
fn view_of(
    seq: u64,
    generation: Generation,
    epoch: OwnerEpoch,
    config: ConfigVersion,
) -> AuthorityView {
    AuthorityView {
        lineage: Lineage {
            partition: P,
            generation,
            owner_epoch: epoch,
        },
        grant_id: GrantId(2),
        boot_id: BootId(1),
        authority_generation: AuthorityGeneration(1),
        config_version: config,
        authority_seq: seq,
        valid_through_tick: Tick(u64::MAX),
        past_horizon: DenyReason::NoGrant,
    }
}

fn view_event(view: AuthorityView) -> EventKind {
    EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::View(view)))
}

/// A view in the golden generation.
fn view(seq: u64, epoch: OwnerEpoch, config: ConfigVersion) -> EventKind {
    view_event(view_of(seq, GEN, epoch, config))
}

/// `(owner_epoch, config_version, authority_seq)` as A's tracker holds them.
fn tracker_pin(module: &Replication) -> (OwnerEpoch, ConfigVersion, u64) {
    let tracker = primary_side(module).tracker();
    (
        tracker.lineage().owner_epoch,
        tracker.config().config_version,
        tracker.authority_seq(),
    )
}

/// The primary installs a newer epoch from A1's view, and **only** the epoch and the seq: the
/// configuration version is a gate on the tracker, never written (lead ruling B-R53), because
/// its version is the newest predicate's and only `ConfigChanged` pushes one.
///
/// The view (seq 1, epoch 6, config 8) is `Recorded`; the tracker is at epoch 6 and still
/// config 7 with one predicate. B's ACK at epoch 5 is now `StaleEpoch`, and at epoch 6 is
/// admitted. The `ConfigChanged` to 8 that follows is still taken, not refused as stale, and
/// its incarnation reset still runs (M7B-150): B, re-announced at a new boot, is fresh. The
/// same view again is `OutOfOrder`, and so is a newer one that names config 7 now that the
/// tracker pins 8: the gate reads the version `ConfigChanged` wrote. Neither changes anything.
/// A view touches no cursor: B's catch-up runs through one unchanged.
#[retcd_test]
fn m7b_161_a_newer_view_moves_the_primarys_epoch_and_never_its_config() {
    let mut module = routed();
    assert_eq!(
        route(&mut module, view(1, NEW_EPOCH, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(tracker_pin(&module), (NEW_EPOCH, CONFIG, 1));
    assert_eq!(primary_side(&module).tracker().predicates(), &[config()]);

    let old = b(HEAD, HEAD, HEAD);
    let before = module.clone();
    assert_eq!(
        route(&mut module, accepted(&old)),
        vec![rejected(AckRejectReason::StaleEpoch)]
    );
    assert_eq!(module, before, "a dropped ACK changes nothing");
    let new = AppendAck {
        owner_epoch: NEW_EPOCH,
        ..old
    };
    assert_eq!(
        route(&mut module, accepted(&new)).first(),
        Some(&peer_progress(B, HEAD))
    );

    let restarted = BootId(9);
    let answer = route(
        &mut module,
        config_event(reannounced(NEW_CONFIG, restarted)),
    );
    assert_ne!(answer, vec![replica(ReplicaIgnoreReason::InvalidConfig)]);
    let tracker = primary_side(&module).tracker();
    assert_eq!(tracker.config().config_version, NEW_CONFIG);
    let peer = tracker.peer(COPY_B).expect("B is still a member");
    assert_eq!(
        (peer.boot, peer.progress),
        (restarted, ReplicaProgress::EMPTY)
    );

    let before = module.clone();
    for (stale, case) in [
        (view(1, NEW_EPOCH, NEW_CONFIG), "the same seq again"),
        (view(2, OwnerEpoch(7), CONFIG), "a newer seq below config 8"),
    ] {
        assert_eq!(
            route(&mut module, stale),
            vec![replica(ReplicaIgnoreReason::OutOfOrder)],
            "{case}"
        );
        assert_eq!(module, before, "{case}: a refused view changes nothing");
    }

    // A view touches no cursor (tester probe q08): B's catch-up runs through it unchanged.
    let mut module = catching_up_b();
    let cursor = primary_side(&module).cursor(COPY_B).cloned();
    assert_eq!(
        route(&mut module, view(1, NEW_EPOCH, CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(primary_side(&module).cursor(COPY_B).cloned(), cursor);
}

/// R1 is a named consumer of A1's view, so it **answers** one for a partition it holds nothing
/// for, `NotRequired`, where it would decline any other event (lead ruling B-R53). A decline
/// there would stop the run (B-R28). On a node holding both halves, the receiver answers first.
///
/// 1. Nothing installed: `NotRequired`, and the module is unchanged. `LocalApplied` on the same
///    empty module is still declined, so the answer is the view's alone.
/// 2. A primary on A, the view on B, where nothing is installed: `NotRequired`, A untouched.
/// 3. Both halves on A. The receiver (copy D, pinned at config 8) refuses a view at config 7 as
///    `OutOfOrder`; the tracker (config 7) installs it. The answer is exactly
///    `[OutOfOrder, Recorded]`, in that order.
#[retcd_test]
fn m7b_162_a_view_is_answered_where_nothing_is_installed_and_receiver_first_where_both_are() {
    let answered = |module: &mut Replication, node: NodeId, kind: EventKind| {
        let before = module.clone();
        let effects = step_on(module, node, kind).expect("a view is answered, never declined");
        assert_eq!(
            *module, before,
            "an answered view with no copy changes nothing"
        );
        effects
            .into_iter()
            .map(|effect| effect.kind)
            .collect::<Vec<_>>()
    };

    let mut empty = Replication::new();
    assert_eq!(
        answered(&mut empty, A, view(1, NEW_EPOCH, CONFIG)),
        vec![replica(ReplicaIgnoreReason::NotRequired)]
    );
    assert!(matches!(
        step_on(&mut empty, A, local_applied_event(HEAD + 1)),
        Err(RdbError::Unavailable { .. })
    ));

    let mut primary_only = routed();
    assert_eq!(
        answered(&mut primary_only, B, view(1, NEW_EPOCH, CONFIG)),
        vec![replica(ReplicaIgnoreReason::NotRequired)]
    );

    let receiver = AppendReceiver::new(ReceiverInit {
        config: config_with(
            NEW_CONFIG,
            vec![
                member(COPY_B, B, Primary),
                member(COPY_D, A, RegularSecondary),
            ],
        ),
        own: COPY_D,
        lineage: lineage(),
        head: Head {
            seq: Seq(10),
            digest: d(10),
        },
        durable: DurableSeq(10),
    })
    .expect("a receiver on A");
    let mut both = routed();
    both.install_receiver(receiver.clone());
    assert_eq!(
        route(&mut both, view(1, NEW_EPOCH, CONFIG)),
        vec![
            replica(ReplicaIgnoreReason::OutOfOrder),
            replica(ReplicaIgnoreReason::Recorded),
        ]
    );
    assert_eq!(both.receiver(A, P), Some(&receiver));
    assert_eq!(tracker_pin(&both), (NEW_EPOCH, CONFIG, 1));
}

/// `Recovered` and `View` make one write (lead ruling B-R53: one `adopt_view`), so the
/// recovered view's `authority_seq` is the floor the next view must beat, on both halves.
///
/// The committed root's view is seq 1. After the rebuild, a view at seq 1 with a higher epoch is
/// `OutOfOrder` on the tracker and on B's receiver alike, and changes neither; seq 2 installs
/// on both. The pin wins over the view's version: a committed view naming config 9 over a pin
/// at 8 leaves the receiver at 8, and the rebuilt primary's only predicate is the pin.
#[retcd_test]
fn m7b_163_the_recovered_view_is_the_floor_the_next_view_must_beat() {
    let result = recovery(10, d(10), pin_with_b());
    let higher = OwnerEpoch(7);

    let mut tracker = both_caught_up();
    tracker.on_recovered(&result, T);
    assert_eq!(
        (tracker.lineage().owner_epoch, tracker.authority_seq()),
        (NEW_EPOCH, 1)
    );
    let before = tracker.clone();
    assert_eq!(
        tracker.on_view(&view_of(1, NEW_GEN, higher, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::OutOfOrder)]
    );
    assert_eq!(tracker, before);
    assert_eq!(
        tracker.on_view(&view_of(2, NEW_GEN, higher, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(
        (tracker.lineage().owner_epoch, tracker.authority_seq()),
        (higher, 2)
    );

    let mut receiver = AppendReceiver::new(ReceiverInit {
        config: config(),
        own: COPY_B,
        lineage: lineage(),
        head: Head {
            seq: Seq(10),
            digest: d(10),
        },
        durable: DurableSeq(10),
    })
    .expect("B's receiver");
    receiver.on_recovered(&result);
    assert_eq!(
        (
            receiver.lineage().generation,
            receiver.lineage().owner_epoch,
            receiver.authority_seq()
        ),
        (NEW_GEN, NEW_EPOCH, 1)
    );
    let before = receiver.clone();
    assert_eq!(
        receiver.on_view(&view_of(1, NEW_GEN, higher, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::OutOfOrder)]
    );
    assert_eq!(receiver, before);
    assert_eq!(
        receiver.on_view(&view_of(2, NEW_GEN, higher, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(
        (receiver.lineage().owner_epoch, receiver.authority_seq()),
        (higher, 2)
    );

    // The pin wins over the view's version (tester probe q07): a committed view naming config
    // 9 over a pin at 8 still leaves the receiver at the pin, version included.
    let mut ahead = result.clone();
    ahead.committed.authority_view.config_version = ConfigVersion(9);
    let mut pinned = before;
    pinned.on_recovered(&ahead);
    assert_eq!(pinned.config(), &pin_with_b());

    // The tracker half of q07: the rebuilt primary's only predicate is the pin, version included.
    let mut rebuilt = both_caught_up();
    rebuilt.on_recovered(&ahead, T);
    assert_eq!(rebuilt.predicates(), &[pin_with_b()][..]);
}

// --- Recovered builds R1's side (lead ruling B-R54) --------------------------------------------

/// `result` with a barrier over `copies`, each proved durable at the cutoff.
fn requiring(mut result: RecoveryResult, copies: &[CopyId]) -> RecoveryResult {
    let (cutoff, digest) = (result.selected.cutoff_seq, result.selected.cutoff_digest);
    let proofs: Vec<DurableProof> = copies
        .iter()
        .map(|&copy| DurableProof {
            copy,
            partition: P,
            seq: DurableSeq(cutoff.0),
            digest,
        })
        .collect();
    let required = copies.iter().copied().collect();
    result.barrier = RecoveryBarrier::try_new(&proofs, &required, cutoff, digest)
        .expect("every required copy proved");
    result
}

fn recovered_event(result: &RecoveryResult) -> EventKind {
    EventKind::Kernel(KernelEvent::Recovered(Box::new(result.clone())))
}

/// M7B-164. Nothing is installed on A, and F1's result pins A primary with a barrier that names
/// A. `Recovered` builds A's primary: exactly the tracker an installed one seeded at the proved
/// cutoff — ladder `{0: ROOT, 10: d10}`, the genesis rung included (lead ruling B-R58b), and
/// received, applied and durable 10 — becomes through the same rebuild, so it answers the same
/// effects and ends equal, and it holds the recovered view's epoch and `authority_seq`. No
/// receiver is built beside it.
///
/// Near-misses: a barrier that does not name A builds nothing and answers `BarrierNotDurable`; a
/// node the pin does not name builds nothing and answers `NotRequired`; and a node that still
/// holds a receiver from the old pin keeps it fenced — it answers `InvalidConfig` and is retired
/// under the new generation (lead ruling B-R58a, F4) — while the primary is built beside it. A
/// barrier that names A over a pin that does not validate answers `InvalidConfig`, not
/// `BarrierNotDurable`, and builds nothing (F3).
#[retcd_test]
fn m7b_164_recovered_builds_the_primary_on_the_node_the_pin_names_primary() {
    let result = requiring(recovery(10, d(10), pin_with_b()), &[COPY_A, COPY_B]);
    let seed = || {
        let mut history = DigestLadder::new();
        history.insert(Seq::ZERO, Digest::ROOT);
        history.insert(Seq(10), d(10));
        ProgressTracker::new(TrackerInit {
            config: pin_with_b(),
            own: COPY_A,
            lineage: Lineage {
                partition: P,
                generation: NEW_GEN,
                owner_epoch: NEW_EPOCH,
            },
            history,
            local: progress(10, 10, 10),
        })
        .expect("the proved seed")
    };
    let mut reference = Replication::new();
    reference.install_primary(seed());
    let want = route(&mut reference, recovered_event(&result));

    let mut module = Replication::new();
    assert_eq!(route(&mut module, recovered_event(&result)), want);
    assert_eq!(module, reference);
    let built = primary_side(&module).tracker();
    assert_eq!(
        (built.lineage(), built.authority_seq(), built.own()),
        (
            Lineage {
                partition: P,
                generation: NEW_GEN,
                owner_epoch: NEW_EPOCH
            },
            1,
            COPY_A
        )
    );
    assert_eq!(built.config(), &pin_with_b());
    assert_eq!(
        built.peer(COPY_A).expect("A").progress,
        progress(10, 10, 10)
    );
    assert_eq!(built.history().lookup(Seq(10), d(10)), DigestLookup::Match);
    assert_eq!(built.head(), Seq(10));
    assert!(module.receiver(A, P).is_none());

    // The barrier names B only: nothing proves A holds the cutoff it would lead from.
    let unproved = requiring(recovery(10, d(10), pin_with_b()), &[COPY_B]);
    let mut empty = Replication::new();
    assert_eq!(
        route(&mut empty, recovered_event(&unproved)),
        vec![replica(ReplicaIgnoreReason::BarrierNotDurable)]
    );
    assert_eq!(empty, Replication::new());

    // D is not in the pin.
    let answered = step_on(&mut empty, D, recovered_event(&result)).expect("answered");
    assert_eq!(
        answered
            .into_iter()
            .map(|effect| effect.kind)
            .collect::<Vec<_>>(),
        vec![replica(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(empty, Replication::new());

    // F3 (lead ruling B-R58): the barrier names A, but the pin does not validate. That is the
    // pin's fault, not the barrier's, so it is `InvalidConfig`, the answer `ConfigChanged`
    // gives the same pin, and nothing is built.
    let mut invalid = pin_with_b();
    invalid.min_regular_acks = 0;
    let unpinnable = requiring(recovery(10, d(10), invalid), &[COPY_A, COPY_B]);
    assert_eq!(
        route(&mut empty, recovered_event(&unpinnable)),
        vec![replica(ReplicaIgnoreReason::InvalidConfig)]
    );
    assert_eq!(empty, Replication::new());

    // A still holds copy D's receiver from a pin B led.
    let stale = AppendReceiver::new(ReceiverInit {
        config: config_with(
            CONFIG,
            vec![
                member(COPY_B, B, Primary),
                member(COPY_D, A, RegularSecondary),
            ],
        ),
        own: COPY_D,
        lineage: lineage(),
        head: Head {
            seq: Seq(10),
            digest: d(10),
        },
        durable: DurableSeq(10),
    })
    .expect("a receiver on A");
    let mut swapped = Replication::new();
    swapped.install_receiver(stale.clone());
    let mut expected = vec![replica(ReplicaIgnoreReason::InvalidConfig)];
    expected.extend(want);
    assert_eq!(route(&mut swapped, recovered_event(&result)), expected);
    let mut fenced = stale.clone();
    fenced.on_recovered(&result);
    assert!(fenced.retired());
    assert_eq!(swapped.receiver(A, P), Some(&fenced));
    assert_eq!(swapped.primary(A, P), reference.primary(A, P));

    // The pin's primary in slot 2, on C: nothing ties the primary to copy 0. `TrackerInit`
    // requires only that `own` is the pin's `Primary` member.
    let slot_two = config_with(
        NEW_CONFIG,
        vec![
            member(COPY_A, A, RegularSecondary),
            member(COPY_B, B, RegularSecondary),
            member(COPY_C, C, Primary),
        ],
    );
    let result = requiring(recovery(10, d(10), slot_two.clone()), &[COPY_A, COPY_C]);
    let mut module = Replication::new();
    let effects = step_on(&mut module, C, recovered_event(&result)).expect("answered");
    assert!(!effects.is_empty());
    let built = module.primary(C, P).expect("C's primary").tracker();
    assert_eq!(
        (built.own(), built.node(), built.config(), built.head()),
        (COPY_C, C, &slot_two, Seq(10))
    );
    assert_eq!(
        built.peer(COPY_C).expect("C").progress,
        progress(10, 10, 10)
    );
}

// --- B-R58b/c and the retired primary --------------------------------------------------------

/// A module where `Recovered` at `cutoff` built A's primary, the barrier naming A and C: B holds
/// nothing the barrier proved.
fn built_at(cutoff: u64) -> Replication {
    let result = requiring(recovery(cutoff, d(cutoff), pin_with_b()), &[COPY_A, COPY_C]);
    let mut module = Replication::new();
    route(&mut module, recovered_event(&result));
    module
}

/// `NeedPrefix` from a copy at `have` claiming `head_digest` there.
fn need_prefix_at(have: u64, head_digest: Digest) -> AppendOutcome {
    AppendOutcome::Rejected(AppendReject::NeedPrefix {
        have: Seq(have),
        head_digest,
    })
}

/// `ack` restamped in the recovered lineage and pin.
fn in_new_root(mut ack: AppendAck) -> AppendAck {
    (ack.generation, ack.owner_epoch, ack.config_version) = (NEW_GEN, NEW_EPOCH, NEW_CONFIG);
    ack
}

/// M7B-171 (lead ruling B-R58b). A primary `Recovered` built holds the genesis rung `(0, ROOT)`
/// beside the cutoff, so a copy that holds nothing is walked from record 1 instead of being
/// sent to a snapshot the simulation refuses. The rung changes no other lookup: a copy at 5
/// with a digest the ladder has no rung for still gets the snapshot request, and a copy at 0
/// that claims anything other than `ROOT` there has diverged — true divergence, since every
/// lineage starts at `ROOT`.
#[retcd_test]
fn m7b_171_a_built_primary_walks_a_copy_that_holds_nothing_from_record_one() {
    let mut module = built_at(10);
    assert_eq!(
        primary_side(&module)
            .tracker()
            .history()
            .lookup(Seq::ZERO, Digest::ROOT),
        DigestLookup::Match
    );
    assert_eq!(
        route(&mut module, reply(B, &need_prefix_at(0, Digest::ROOT))),
        vec![send(COPY_B, 1), retransmit_arm(1)]
    );
    assert_eq!(
        primary_side(&module)
            .cursor(COPY_B)
            .expect("running")
            .outstanding(),
        Some(Seq(1))
    );

    // Near-miss: no rung at 5, so it is truncation, never divergence (K-B-17).
    let mut module = built_at(10);
    assert_eq!(
        route(
            &mut module,
            reply(B, &need_prefix_at(5, forked(b(5, 5, 5)).digest_at_buffered))
        ),
        vec![kernel(KernelEffect::SnapshotCatchupRequired {
            copy: COPY_B,
            barrier: Seq(10),
        })]
    );

    // A copy at 0 that is not at `ROOT` is on another history.
    let mut module = built_at(10);
    let effects = route(&mut module, reply(B, &need_prefix_at(0, d(0))));
    assert_eq!(
        effects[0],
        kernel(KernelEffect::DivergenceDetected { copy: COPY_B })
    );
    assert!(primary_side(&module)
        .cursor(COPY_B)
        .is_none_or(|cursor| cursor.stopped().is_some()));
}

/// The pin the r03 swap installs: C leads, A and B are regular.
fn pin_c_leads() -> PartitionConfig {
    config_with(
        NEW_CONFIG,
        vec![
            member(COPY_A, A, RegularSecondary),
            member(COPY_B, B, RegularSecondary),
            member(COPY_C, C, Primary),
        ],
    )
}

/// M7B-172 (lead ruling on the kept primary: fence it). After the r03 swap A holds a receiver for
/// the new pin and its old primary for generation 3. That primary is retired: an ACK its old
/// lineage would admit and a `LocalApplied` it would record reach nothing — each is declined as
/// if no primary were installed, and nothing changes — and a view is answered by the receiver
/// alone. `Flushed` naming its old prefix finds no one either.
///
/// Near-miss: a later `Recovered` that pins A primary over a cutoff A holds clears `retired`; the
/// rebuilt primary is exactly the one an unretired tracker becomes, and it serves again.
#[retcd_test]
fn m7b_172_a_primary_the_pin_retires_is_absent_until_a_pin_names_it_primary_again() {
    let swap = requiring(recovery(10, d(10), pin_c_leads()), &[COPY_A, COPY_C]);
    let mut module = routed();
    let answered = route(&mut module, recovered_event(&swap));
    assert_eq!(
        answered.last(),
        Some(&replica(ReplicaIgnoreReason::InvalidConfig))
    );
    assert!(primary_side(&module).tracker().retired());
    assert!(module.receiver(A, P).is_some(), "the new side is built");

    // Each input the old lineage would act on: the ACK admits (golden lineage), the
    // `LocalApplied` is the next sequence.
    assert_eq!(
        deliver(&mut tracker(), &b(HEAD, HEAD, HEAD))[0],
        peer_progress(B, HEAD)
    );
    let fenced = module.clone();
    for kind in [
        accepted(&b(HEAD, HEAD, HEAD)),
        local_applied_event(HEAD + 1),
        reply(B, &need_prefix(10)),
        EventKind::Storage(StorageEvent::Flushed {
            ticket: FlushTicket(1),
            durable: flush(GEN, HEAD),
        }),
    ] {
        let result = step_on(&mut module, A, kind);
        assert!(
            matches!(result, Err(RdbError::Unavailable { .. })),
            "{result:?}"
        );
        assert_eq!(module, fenced);
    }
    let newer = view_of(2, NEW_GEN, NEW_EPOCH, NEW_CONFIG);
    let mut alone = module.receiver(A, P).expect("built").clone();
    let want = alone.on_view(&newer);
    assert_eq!(route(&mut module, view_event(newer)), want);
    assert_eq!(
        primary_side(&module).tracker(),
        fenced.primary(A, P).expect("kept").tracker()
    );

    // Near-miss: pinned primary again, over the cutoff its ladder holds.
    let back = requiring(recovery(10, d(10), pin_with_b()), &[COPY_A, COPY_B]);
    let mut reference = tracker();
    let want = reference.on_recovered(&back, T);
    let answered = route(&mut module, recovered_event(&back));
    assert!(answered.ends_with(&want), "{answered:?}");
    assert!(!primary_side(&module).tracker().retired());
    assert_eq!(primary_side(&module).tracker(), &reference);
    // Update (B-R67i): serving again means the stream ships 11 to B and C and arms the
    // retransmit, where the tracker alone answered `Recorded`.
    assert_eq!(
        route(&mut module, local_applied_event(11)),
        vec![send(COPY_B, 11), send(COPY_C, 11), retransmit_arm(1)]
    );
}

/// B's cursor on a primary built at cutoff 3, started from the root: record 1 is in flight.
fn walking_b_from_root() -> Replication {
    let mut module = built_at(3);
    assert_eq!(
        route(&mut module, reply(B, &need_prefix_at(0, Digest::ROOT))),
        vec![send(COPY_B, 1), retransmit_arm(1)]
    );
    module
}

/// M7B-173 (lead ruling B-R58c, rows 1 and 5). A primary built at cutoff 3 holds rungs 0 and 3
/// only. B, root-seeded, is walked from record 1. Its ACKs at 1 and 2 are exactly the records in
/// flight, below the cutoff, where the ladder holds nothing to verify them against: each drives
/// the cursor and nothing else — the effects are exactly `[Ignored(AckRejected(
/// InFlightUnverified)), SendEnvelopes{B, next, next}]`, with no `PeerProgress`, no qualification
/// edge and no `DurableAdvanced`, and the tracker is unchanged. The ACK at 3 is verified against
/// the cutoff rung and moves B's watermarks to 3 in one step, and the cursor reports B caught up.
#[retcd_test]
fn m7b_173_an_in_flight_ack_below_the_cutoff_drives_the_cursor_and_moves_no_watermark() {
    let mut module = walking_b_from_root();
    let before = primary_side(&module).tracker().clone();
    for seq in 1..3 {
        assert_eq!(
            route(&mut module, accepted(&in_new_root(b(seq, seq, seq)))),
            vec![
                rejected(AckRejectReason::InFlightUnverified),
                send(COPY_B, seq + 1)
            ]
        );
        assert_eq!(primary_side(&module).tracker(), &before);
        assert_eq!(
            primary_side(&module)
                .cursor(COPY_B)
                .expect("running")
                .outstanding(),
            Some(Seq(seq + 1))
        );
    }

    let at_cut = in_new_root(b(3, 3, 3));
    let mut reference = before;
    let mut want = deliver(&mut reference, &at_cut);
    assert_eq!(want[0], peer_progress(B, 3), "the tracker verifies it");
    want.push(caught_up(COPY_B, 3));
    assert_eq!(route(&mut module, accepted(&at_cut)), want);
    assert_eq!(primary_side(&module).tracker(), &reference);
    assert_eq!(
        primary_side(&module)
            .tracker()
            .peer(COPY_B)
            .expect("B")
            .progress,
        progress(3, 3, 3)
    );
    assert!(primary_side(&module).cursor(COPY_B).is_none());
}

/// M7B-174 (lead ruling B-R58c, rows 2–4, as amended by B-R67c). Outside the three bounds an
/// unretained ACK gets today's answer, `[Ignored(AckRejected(Unverifiable)), SnapshotCatchupRequired]`:
/// an ACK ahead of the one in flight, a repeat of the last with another digest, and an ACK with
/// no cursor running. A repeat of the last with the digest the cursor accepted is not outside
/// the bounds: it is `Recorded` and changes nothing (B-R67c; it answered a snapshot request
/// before). A wrong digest at the cutoff is divergence, and no watermark of B's
/// ever moved.
#[retcd_test]
fn m7b_174_an_unretained_ack_outside_the_in_flight_bounds_gets_todays_answer() {
    let todays = vec![
        rejected(AckRejectReason::Unverifiable),
        kernel(KernelEffect::SnapshotCatchupRequired {
            copy: COPY_B,
            barrier: Seq(3),
        }),
    ];

    // Not the sequence in flight: 1 is, 2 is not.
    let mut module = walking_b_from_root();
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(2, 2, 2)))),
        todays
    );
    // A repeat of the ACK that already moved the cursor, with another digest: 2 is in flight
    // now. The matching repeat is `Recorded` and changes nothing (lead ruling B-R67c).
    let mut module = walking_b_from_root();
    route(&mut module, accepted(&in_new_root(b(1, 1, 1))));
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(primary_side(&module), &before);
    assert_eq!(
        route(&mut module, accepted(&forked(in_new_root(b(1, 1, 1))))),
        todays
    );

    // No cursor running.
    let mut module = built_at(3);
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        todays
    );

    // A wrong digest at the cutoff, after the walk.
    let mut module = walking_b_from_root();
    for seq in 1..3 {
        route(&mut module, accepted(&in_new_root(b(seq, seq, seq))));
    }
    let mut reference = primary_side(&module).tracker().clone();
    let wrong = forked(in_new_root(b(3, 3, 3)));
    let want = deliver(&mut reference, &wrong);
    assert_eq!(
        want[0],
        kernel(KernelEffect::DivergenceDetected { copy: COPY_B })
    );
    assert_eq!(route(&mut module, accepted(&wrong)), want);
    let tracker = primary_side(&module).tracker();
    assert!(tracker.is_diverged(COPY_B));
    assert_eq!(tracker.peer(COPY_B).expect("B").progress, progress(0, 0, 0));
}

// --- §9 rows: the edge is the predicate, retirement, and the quarantine route ----------------

/// A, B and C at `CONFIG` with nobody else: the plan's RF3.
fn rf3() -> PartitionConfig {
    config_with(
        CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_B, B, RegularSecondary),
            member(COPY_C, C, RegularSecondary),
        ],
    )
}

fn rf3_tracker() -> ProgressTracker {
    ProgressTracker::new(init(rf3())).expect("RF3 tracker")
}

/// M7B-134 (design §3.4 "emitted iff `qualifies_now(head)` changed value"; ADR 0005 §5): RF3,
/// B and C at the head, B diverges by rule 9. C still carries the predicate, so the set
/// shrinks and nothing about the predicate is said: the vector is exactly three wide, with no
/// edge, no block and no durable view (every copy stays at the head). The twin differs in one
/// fact, C never ACKed, and there the same divergence flips the predicate and says so.
#[retcd_test]
fn m7b_134_set_change_without_a_predicate_flip_emits_nothing() {
    let vector = [
        kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
        alert(),
        copy_lost(COPY_B),
    ];
    let mut tracker = rf3_tracker();
    deliver(&mut tracker, &b(HEAD, HEAD, HEAD));
    deliver(&mut tracker, &c(HEAD, HEAD, HEAD));
    assert!(tracker.qualifies_now(Seq(HEAD)));
    assert_eq!(
        deliver(&mut tracker, &forked(b(HEAD, HEAD, HEAD))),
        vector.to_vec()
    );
    assert!(tracker.qualifies_now(Seq(HEAD)));
    assert_eq!(tracker.qualified_copies(Seq(HEAD)), vec![COPY_C]);

    // Twin: C had not ACKed, so B was the predicate.
    let mut twin = rf3_tracker();
    deliver(&mut twin, &b(HEAD, HEAD, HEAD));
    assert!(twin.qualifies_now(Seq(HEAD)));
    let mut want = vector.to_vec();
    want.push(lost(QualificationCause::DivergenceDetected(COPY_B)));
    assert_eq!(deliver(&mut twin, &forked(b(HEAD, HEAD, HEAD))), want);
    assert!(!twin.qualifies_now(Seq(HEAD)));
    // C is still a live regular secondary, so the floor is not gone and nothing blocks.
    assert_eq!(twin.regular_secondaries(), vec![COPY_C]);
}

/// The `QualificationChanged` M7B-134's twin emits.
fn twin_lost_edge() -> QualificationChanged {
    let mut twin = rf3_tracker();
    deliver(&mut twin, &b(HEAD, HEAD, HEAD));
    let edges: Vec<QualificationChanged> = deliver(&mut twin, &forked(b(HEAD, HEAD, HEAD)))
        .into_iter()
        .filter_map(|effect| match effect {
            EffectKind::Kernel(KernelEffect::QualificationChanged(q)) => Some(q),
            _ => None,
        })
        .collect();
    assert_eq!(edges.len(), 1, "{edges:?}");
    edges[0].clone()
}

/// P1's candidate at `seq`, bound to this file's lineage and `CONFIG`.
fn p1_candidate(seq: u64) -> AppliedCandidate {
    let digest = |s: u64| Digest::of(Domain::Record, &[&s.to_le_bytes()]);
    AppliedCandidate {
        lineage: lineage(),
        config_version: CONFIG,
        seq: Seq(seq),
        prev_digest: digest(seq - 1),
        record_digest: digest(seq),
        request: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(seq),
        },
        request_digest: Digest::of(Domain::Request, &[&seq.to_le_bytes()]),
        pending_result: TxnResult {
            partition: P,
            owner_epoch: EPOCH,
            generation: GEN,
            seq: Seq(seq),
            outcome: Outcome::Published,
            durability: Durability::BufferedOnTwo,
        },
        authority: AuthorityDecision {
            owner: A,
            boot: BootId(1),
            grant: GrantId(1),
            authority_generation: AuthorityGeneration(1),
            lineage: lineage(),
            expiry_utc_ms: 0,
            decided_at: Tick::ZERO,
            authority_seq: 1,
            checkpoint: Checkpoint::StorageDispatch,
            correlation: CorrelationId(seq),
            verdict: Verdict::Admit,
        },
    }
}

/// P1 with everything through `HEAD - 1` published and nothing pending.
fn p1_idle() -> PubKernel {
    PubKernel::new(PubConfig::default(), BootId(1), lineage(), Seq(HEAD - 1))
}

/// [`p1_idle`] holding the candidate at `HEAD`.
fn p1_pending() -> PubKernel {
    let mut p1 = p1_idle();
    let armed = p1_kinds(&p1.apply(T, PubEvent::Candidate(p1_candidate(HEAD)), None));
    assert_eq!(
        armed.first().map(String::as_str),
        Some("ArmTimer"),
        "{armed:?}"
    );
    p1
}

/// [`p1_pending`] after a `Gained` edge for it: a recheck is outstanding.
fn p1_rechecking() -> PubKernel {
    let mut p1 = p1_pending();
    let gained = QualificationChanged {
        direction: QualificationDirection::Gained,
        ..twin_lost_edge()
    };
    assert_eq!(gained.at_seq, Seq(HEAD));
    let asked = p1_kinds(&p1.apply(T, PubEvent::QualificationChanged(gained), None));
    assert_eq!(asked, ["AuthorityCheck"]);
    p1
}

/// Which P1 branch each effect came from: the effect's variant and, for a fact, the fact's,
/// with every payload dropped. `cause` is copied into `QualificationLost` and
/// `QualificationLostAfterPublish`, so a payload is not a branch.
fn p1_kinds(effects: &[PubEffect]) -> Vec<String> {
    let name = |s: &str| -> String { s.chars().take_while(char::is_ascii_alphanumeric).collect() };
    effects
        .iter()
        .map(|effect| {
            let debug = format!("{effect:?}");
            match (effect, debug.strip_prefix("Fact(")) {
                (PubEffect::Fact(_), Some(fact)) => format!("Fact({})", name(fact)),
                _ => name(&debug),
            }
        })
        .collect()
}

/// M7B-135, on the event M7B-134's twin emits (B-R27 "no third variant"; design §3.4/§4.1
/// field list; P1 clause as reworded by ruling B-R63a).
///
/// The match on `direction` has no wildcard and the destructure has no `..`, so a third
/// direction or a changed field list stops this file compiling. L1 is then handed the event
/// and a copy with every other field changed, in both directions, and ends identical.
///
/// P1 is driven through every arm of its qualification step, both directions, with the edge
/// and two copies that change only `qualified_copies`, `qualified_ack_count`, `cause` and
/// `tick`. All three land in the same branch, and each branch is named, so P1 is shown to
/// branch on `direction` and on the candidate binding (`lineage`, `config_version`, `at_seq`)
/// and on nothing else.
#[retcd_test]
fn m7b_135_qualification_changed_has_two_directions_and_trace_fields_only() {
    let edge = twin_lost_edge();
    let QualificationChanged {
        lineage: edge_lineage,
        config_version,
        at_seq,
        direction,
        qualified_copies,
        qualified_ack_count,
        cause,
        tick,
    } = edge.clone();
    let flipped = match direction {
        QualificationDirection::Gained => QualificationDirection::Lost,
        QualificationDirection::Lost => QualificationDirection::Gained,
    };
    assert_eq!(direction, QualificationDirection::Lost);
    assert_eq!(flipped, QualificationDirection::Gained);
    assert_eq!(
        (edge_lineage, config_version, at_seq, tick),
        (lineage(), CONFIG, Seq(HEAD), T)
    );
    assert_eq!(
        (qualified_copies, qualified_ack_count, cause),
        (
            Vec::new(),
            0,
            QualificationCause::DivergenceDetected(COPY_B)
        )
    );

    // Every field but `direction` changed, and the same event with only its direction changed.
    let other = |direction| QualificationChanged {
        lineage: Lineage {
            generation: NEW_GEN,
            owner_epoch: NEW_EPOCH,
            ..lineage()
        },
        config_version: NEW_CONFIG,
        at_seq: Seq(99),
        direction,
        qualified_copies: vec![COPY_C, COPY_B],
        qualified_ack_count: 2,
        cause: QualificationCause::StaleBoot(COPY_C),
        tick: Tick(12_345),
    };
    let with = |direction| QualificationChanged {
        direction,
        ..edge.clone()
    };
    for (start, direction) in [
        (
            l1_healthy as fn() -> (Protection, u64),
            QualificationDirection::Lost,
        ),
        (l1_paused, QualificationDirection::Gained),
    ] {
        let ((mut one, now), (mut two, _)) = (start(), start());
        let a = l1_step(&mut one, now, qualification(with(direction)));
        let b = l1_step(&mut two, now, qualification(other(direction)));
        assert_eq!(a, b, "{direction:?}");
        assert_eq!(one, two, "{direction:?}");
    }

    // P1: published through HEAD - 1; the candidate, when there is one, is HEAD.
    use QualificationDirection::{Gained, Lost};
    let variants = |at: u64, direction| {
        let at_seq = Seq(at);
        [
            QualificationChanged {
                at_seq,
                direction,
                ..edge.clone()
            },
            QualificationChanged {
                at_seq,
                direction,
                qualified_copies: vec![COPY_B],
                qualified_ack_count: 1,
                cause: QualificationCause::AckAdvanced,
                tick: Tick(12_345),
                ..edge.clone()
            },
            QualificationChanged {
                at_seq,
                direction,
                qualified_copies: vec![COPY_D, COPY_C, COPY_B],
                qualified_ack_count: 3,
                cause: QualificationCause::ConfigChanged,
                tick: Tick(1),
                ..edge.clone()
            },
        ]
    };
    let (idle, pending, rechecking) = (p1_idle, p1_pending, p1_rechecking);
    let stale_lineage = |q: QualificationChanged| QualificationChanged {
        lineage: Lineage {
            owner_epoch: NEW_EPOCH,
            ..lineage()
        },
        ..q
    };
    let stale_config = |q: QualificationChanged| QualificationChanged {
        config_version: NEW_CONFIG,
        ..q
    };
    let same = |q: QualificationChanged| q;
    type Start = fn() -> PubKernel;
    type Bind = fn(QualificationChanged) -> QualificationChanged;
    let arms: [(&str, Start, u64, QualificationDirection, Bind, &str); 11] = [
        (
            "no candidate, at published",
            idle,
            HEAD - 1,
            Lost,
            same,
            "QualificationLostAfterPublish",
        ),
        (
            "no candidate, above published",
            idle,
            HEAD,
            Lost,
            same,
            "NotForThisCandidate",
        ),
        (
            "no candidate",
            idle,
            HEAD,
            Gained,
            same,
            "NotForThisCandidate",
        ),
        (
            "other seq",
            pending,
            HEAD + 1,
            Gained,
            same,
            "NotForThisCandidate",
        ),
        (
            "other seq",
            pending,
            HEAD + 1,
            Lost,
            same,
            "NotForThisCandidate",
        ),
        (
            "other lineage",
            pending,
            HEAD,
            Gained,
            stale_lineage,
            "NotForThisCandidate",
        ),
        (
            "other config",
            pending,
            HEAD,
            Lost,
            stale_config,
            "NotForThisCandidate",
        ),
        (
            "the candidate",
            pending,
            HEAD,
            Gained,
            same,
            "AuthorityCheck",
        ),
        (
            "the candidate",
            pending,
            HEAD,
            Lost,
            same,
            "QualificationLost",
        ),
        (
            "recheck outstanding",
            rechecking,
            HEAD,
            Gained,
            same,
            "RecheckOutstanding",
        ),
        (
            "recheck outstanding",
            rechecking,
            HEAD,
            Lost,
            same,
            "QualificationLost",
        ),
    ];
    for (arm, start, at, direction, bind, branch) in arms {
        let want = if branch == "AuthorityCheck" {
            branch.to_owned()
        } else {
            format!("Fact({branch})")
        };
        for q in variants(at, direction) {
            let mut p1 = start();
            let got = p1_kinds(&p1.apply(T, PubEvent::QualificationChanged(bind(q.clone())), None));
            assert_eq!(got, [want.as_str()], "{arm}, {direction:?}: {q:?}");
        }
    }
}

/// Which of A, B, C and D the tracker holds an entry for.
fn entries(tracker: &ProgressTracker) -> Vec<CopyId> {
    [COPY_A, COPY_B, COPY_C, COPY_D]
        .into_iter()
        .filter(|copy| tracker.peer(*copy).is_some())
        .collect()
}

/// M7B-145 (design §3.5 "Retired predicates keep their copies", K-B-49; §4.3 retirement):
/// predicate `c` is `{A, B, C}`, `c+1` is `{A, B, D}`. `ConfigChanged` adds D and removes
/// nothing. C keeps acknowledging under `c`: its entry moves, `c`'s durable view reads it, and
/// `c+1`'s does not, nor does the pinned predicate count it. `TransitionBarrierConfirmed{c}`
/// removes C and nothing else, and C is then a stranger.
///
/// The plan writes the survivors as `{B, D}`. The design keeps an entry for every member of
/// every active predicate, and A, the primary, is a member of both, so the entries are
/// `{A, B, D}`: A's is the one M7B-45 and M7B-50 read.
#[retcd_test]
fn m7b_145_retired_predicates_keep_their_copies_until_the_barrier_is_confirmed() {
    let mut tracker = rf3_tracker();
    deliver(&mut tracker, &b(HEAD, HEAD, HEAD));
    deliver(&mut tracker, &c(HEAD, HEAD, 10));
    assert_eq!(entries(&tracker), vec![COPY_A, COPY_B, COPY_C]);
    assert_eq!(
        tracker.durable_per_predicate(),
        vec![(CONFIG, DurableSeq(10))]
    );

    let next = config_with(
        NEW_CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_B, B, RegularSecondary),
            member(COPY_D, D, RegularSecondary),
        ],
    );
    assert_eq!(
        tracker.on_config_changed(&next, T),
        vec![durable_advanced(&[(CONFIG, 10), (NEW_CONFIG, 0)])]
    );
    assert_eq!(entries(&tracker), vec![COPY_A, COPY_B, COPY_C, COPY_D]);
    assert_eq!(
        tracker.peer(COPY_C).expect("C").progress,
        progress(HEAD, HEAD, 10),
        "C's entry is kept as it was"
    );

    // C's ACK under `c` moves C and `c`'s view, and nothing of `c+1`'s.
    assert_eq!(
        deliver(&mut tracker, &c(HEAD, HEAD, HEAD)),
        vec![
            peer_progress(C, HEAD),
            durable_advanced(&[(CONFIG, HEAD), (NEW_CONFIG, 0)]),
        ]
    );
    assert_eq!(
        tracker.peer(COPY_C).expect("C").progress,
        progress(HEAD, HEAD, HEAD)
    );
    assert_eq!(tracker.qualified_copies(Seq(HEAD)), vec![COPY_B]);
    assert!(
        !tracker.all_durable_through(Seq(1)),
        "the pinned `c+1` waits on D"
    );

    assert_eq!(
        tracker.on_transition_confirmed(CONFIG),
        vec![durable_advanced(&[(NEW_CONFIG, 0)])]
    );
    assert_eq!(entries(&tracker), vec![COPY_A, COPY_B, COPY_D]);
    dropped(
        &mut tracker,
        label(C),
        &c(HEAD, HEAD, HEAD),
        AckRejectReason::NotAMember,
    );
}

// L1 beside R1 on node A. The rows below hand R1's effects to a real `Protection` the way
// routing does (rdb-sim `harness::route`: `PeerProgress`, `CopyLost`, `DurableAdvanced`,
// `QualificationChanged` and `BlockPartition` are L1's), so what L1 believes is what R1 said.

/// L1's context at `now` on node A.
fn l1_ctx(now: u64) -> StepCtx<'static> {
    StepCtx {
        now: Tick(now),
        control_time: ControlTime {
            estimate: Tick(now),
            error_millis: 10,
            bound_established: true,
            sampled_at: Tick(now),
        },
        ..step_ctx()
    }
}

/// Step `kind` on L1 at `now` and return the effect kinds.
fn l1_on(p: &mut Protection, now: u64, kind: EventKind) -> Vec<EffectKind> {
    let event = Event {
        id: EventId(now),
        at: Tick(now),
        node: A,
        boot: BootId(1),
        partition: P,
        correlation: CorrelationId(9),
        kind,
    };
    let effects = p.step(&l1_ctx(now), &event).expect("an L1 input");
    assert!(!effects.is_empty(), "BA-2: never an empty effect vector");
    effects.into_iter().map(|effect| effect.kind).collect()
}

fn l1_step(p: &mut Protection, now: u64, input: KernelEvent) -> Vec<EffectKind> {
    l1_on(p, now, EventKind::Kernel(input))
}

/// `HealthEval` at `now`, read from `ctx.now`.
fn health_eval(p: &mut Protection, now: u64) -> Vec<EffectKind> {
    l1_on(
        p,
        now,
        EventKind::Timer(TimerFired {
            id: HEALTH_EVAL_TIMER,
            version: TimerVersion(0),
            scheduled_at: Tick::ZERO,
        }),
    )
}

fn qualification(edge: QualificationChanged) -> KernelEvent {
    KernelEvent::QualificationChanged(edge)
}

/// The event routing hands L1 for one of R1's effects, or `None` when the effect is not L1's.
fn for_l1(effect: &EffectKind) -> Option<KernelEvent> {
    let EffectKind::Kernel(effect) = effect else {
        return None;
    };
    Some(match effect {
        KernelEffect::PeerProgress {
            peer,
            contiguous_seq,
        } => KernelEvent::PeerProgress {
            peer: *peer,
            contiguous_seq: *contiguous_seq,
        },
        KernelEffect::CopyLost { copy } => KernelEvent::CopyLost { copy: *copy },
        KernelEffect::DurableAdvanced { per_predicate } => KernelEvent::DurableAdvanced {
            per_predicate: per_predicate.clone(),
        },
        KernelEffect::QualificationChanged(edge) => qualification(edge.clone()),
        KernelEffect::BlockPartition(reason) => KernelEvent::BlockPartition(reason.clone()),
        _ => return None,
    })
}

/// Hand every L1 effect in `effects` to `p` at `now`, except those `drop` names.
fn dispatch(
    p: &mut Protection,
    now: u64,
    effects: &[EffectKind],
    drop: impl Fn(&KernelEvent) -> bool,
) {
    for event in effects.iter().filter_map(for_l1) {
        if !drop(&event) {
            l1_step(p, now, event);
        }
    }
}

/// L1 built on A by the RF3 pin, `Paused` at the head until R1 proves it.
fn l1_paused() -> (Protection, u64) {
    let mut p = Protection::new();
    l1_on(&mut p, 0, recovered_event(&recovery(HEAD, d(HEAD), rf3())));
    assert_eq!(p.mode(), Some(Mode::Paused));
    (p, 0)
}

/// R1 and L1 side by side, resumed through the real path: B and C acknowledge the head every
/// 100 ms, every R1 effect reaches L1, and L1 evaluates every 100 ms until the 5 s hold ends.
/// Returns both and the tick of the `Allow`.
fn resumed_side_by_side() -> (ProgressTracker, Protection, u64) {
    let mut tracker = rf3_tracker();
    let (mut p, _) = l1_paused();
    for now in (0..=10_000).step_by(100) {
        for ack in [b(HEAD, HEAD, HEAD), c(HEAD, HEAD, HEAD)] {
            let effects = deliver(&mut tracker, &ack);
            dispatch(&mut p, now, &effects, |_| false);
        }
        if health_eval(&mut p, now)
            .iter()
            .any(|e| matches!(e, EffectKind::Kernel(KernelEffect::SetAdmission(s)) if s.allow))
        {
            assert_eq!(p.mode(), Some(Mode::Healthy));
            return (tracker, p, now);
        }
    }
    panic!("never resumed: {:?}", p.mode());
}

fn l1_healthy() -> (Protection, u64) {
    let (_, p, now) = resumed_side_by_side();
    (p, now)
}

/// M7B-133 (design §4.1 "there is no HealthEval backstop"; ADR 0006 §3 "A dropped Lost edge is
/// caught outside L1"). R1 and L1 resume together. Then control re-announces B and C at new
/// boots: R1's predicate is false and it says so with `QualificationChanged{Lost}`, which a
/// lossy dispatcher drops, handing L1 the rest. L1 still believes the head qualifies, and
/// 100 health evaluations over 10 s, with B and C silent, leave it `Healthy` and admitting.
///
/// This documents the risk; it is not a fix. The guard is I1's lossless dispatcher (ADR 0003
/// §9, B-R23) and verification's mutation row (V-R9), cross-referenced here, not re-asserted.
/// The lossless twin pauses in the same step.
#[retcd_test]
fn m7b_133_dropped_lost_edge_is_not_caught_by_health_eval() {
    let restarted = || {
        let mut next = rf3();
        next.config_version = NEW_CONFIG;
        next.members[1].boot = BootId(22);
        next.members[2].boot = BootId(33);
        next
    };
    let lost_edge = |event: &KernelEvent| {
        matches!(event, KernelEvent::QualificationChanged(q)
            if q.direction == QualificationDirection::Lost)
    };

    let (mut tracker, mut p, start) = resumed_side_by_side();
    let at = start + 100;
    let effects = tracker.on_config_changed(&restarted(), T);
    assert!(
        effects.iter().filter_map(for_l1).any(|e| lost_edge(&e)),
        "R1 emitted the edge: {effects:?}"
    );
    l1_step(&mut p, at, KernelEvent::ConfigChanged(restarted()));
    dispatch(&mut p, at, &effects, lost_edge);
    assert!(!tracker.qualifies_now(tracker.head()));
    assert!(p.qualifies_now_at_head());
    for eval in 1..=100 {
        let now = at + eval * 100;
        let effects = health_eval(&mut p, now);
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, EffectKind::Kernel(KernelEffect::SetAdmission(_)))),
            "no admission edge at {now}: {effects:?}"
        );
        assert_eq!(p.mode(), Some(Mode::Healthy), "at {now}");
    }
    let admitting = p.admission_state(Tick(at + 10_000)).expect("live");
    assert!(admitting.allow);
    assert!(p.qualifies_now_at_head());

    // Lossless: the same step pauses.
    let (mut tracker, mut p, start) = resumed_side_by_side();
    let at = start + 100;
    let effects = tracker.on_config_changed(&restarted(), T);
    l1_step(&mut p, at, KernelEvent::ConfigChanged(restarted()));
    dispatch(&mut p, at, &effects, |_| false);
    assert_eq!(p.mode(), Some(Mode::Paused));
    assert!(!p.admission_state(Tick(at)).expect("live").allow);
}

// F1 beside R1 on node A, over the same RF3 copies, so an R1 effect naming a copy is an F1
// input naming the same one. Only what M7B-144 needs: A alone survives at the head, F1 commits
// `ReadOnly` and rebuilds B and C.

/// Step `kind` on F1 at `now` and return the effect kinds.
fn f1_on(f1: &mut Recovery, now: u64, kind: EventKind) -> Vec<EffectKind> {
    let event = Event {
        id: EventId(now),
        at: Tick(now),
        node: A,
        boot: BootId(1),
        partition: P,
        correlation: CorrelationId(9),
        kind,
    };
    let effects = f1.step(&l1_ctx(now), &event).expect("an F1 input");
    assert!(!effects.is_empty(), "BA-2: never an empty effect vector");
    effects.into_iter().map(|effect| effect.kind).collect()
}

fn f1_rec(f1: &mut Recovery, now: u64, input: RecoveryEvent) -> Vec<EffectKind> {
    f1_on(f1, now, EventKind::Kernel(KernelEvent::Recovery(input)))
}

fn f1_durable(copy: CopyId) -> RecoveryEvent {
    RecoveryEvent::DurableAt(DurableProof {
        copy,
        partition: P,
        seq: DurableSeq(HEAD),
        digest: d(HEAD),
    })
}

/// F1 on A: the RF3 plan anchored on the golden lineage at 5, fenced; A reports its ladder to
/// the head, B and C fail; the window closes; A proves the head durable; the CAS commits. The
/// commit's `Recovered` is `ReadOnly` and F1 is `Rebuilding` with `{A, B, C}` required.
fn f1_rebuilding() -> (Recovery, RecoveryResult) {
    let anchor = LineageAnchor {
        lineage: lineage(),
        base_seq: Seq(5),
        base_digest: d(5),
    };
    let viable = |copy| Candidate {
        copy,
        primary_eligible: true,
        healthy: true,
        within_capacity: true,
        has_valid_grant: true,
    };
    let plan = RecoveryPlan {
        anchor,
        config: rf3(),
        candidates: vec![viable(COPY_A), viable(COPY_B), viable(COPY_C)],
        rebuild_required: [COPY_A, COPY_B, COPY_C].into_iter().collect(),
        authority_view: AuthorityView {
            lineage: lineage(),
            grant_id: GrantId(3),
            boot_id: BootId(1),
            authority_generation: AuthorityGeneration(1),
            config_version: CONFIG,
            authority_seq: 1,
            valid_through_tick: Tick(u64::MAX),
            past_horizon: DenyReason::NoGrant,
        },
        retention_millis: 60_000,
    };
    let mut f1 = Recovery::new();
    f1_rec(&mut f1, 0, RecoveryEvent::Plan(Box::new(plan)));
    let proof = recovery(HEAD, d(HEAD), rf3()).fenced_prior;
    f1_rec(&mut f1, 0, RecoveryEvent::FenceProven(Box::new(proof)));
    let a = SurvivorInventory {
        copy: COPY_A,
        anchor_seen: anchor,
        head: (Seq(HEAD), d(HEAD)),
        ladder: (5..=HEAD).map(|seq| (Seq(seq), d(seq))).collect(),
        quarantined: None,
    };
    f1_rec(&mut f1, 10, RecoveryEvent::InventoryReported(Box::new(a)));
    for copy in [COPY_B, COPY_C] {
        f1_rec(&mut f1, 10, RecoveryEvent::InventoryFailed { copy });
    }
    let window = BUDGETS.discovery_window_millis;
    f1_on(
        &mut f1,
        window,
        EventKind::Timer(TimerFired {
            id: DISCOVERY_TIMER,
            version: TimerVersion(1),
            scheduled_at: Tick::ZERO,
        }),
    );
    let proposal = f1_rec(&mut f1, window + 100, f1_durable(COPY_A));
    let request = cas_request(&proposal).expect("fixture: the durable copy proposes");
    let committed = f1_on(
        &mut f1,
        window + 200,
        EventKind::Control(ControlEvent::CasResult {
            request,
            key: ControlKey::Partition(P),
            outcome: CasOutcome::Committed(Revision(9)),
        }),
    );
    let results: Vec<RecoveryResult> = committed
        .iter()
        .filter_map(|effect| match effect {
            EffectKind::Kernel(KernelEffect::Recovered(result)) => Some((**result).clone()),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1, "{committed:?}");
    let result = results[0].clone();
    assert_eq!(result.mode, PartitionMode::ReadOnly);
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    (f1, result)
}

fn is_stall(effect: &EffectKind) -> bool {
    matches!(
        effect,
        EffectKind::Kernel(KernelEffect::Recovery(
            RecoveryEffect::RebuildStalled { .. }
        ))
    )
}

fn is_cas(effect: &EffectKind) -> bool {
    matches!(effect, EffectKind::Control(ControlEffect::Cas { .. }))
}

/// The request id of the control CAS in `effects`, if F1 sent one: its answer must echo it (lead
/// ledger L-R177hs).
fn cas_request(effects: &[EffectKind]) -> Option<ControlRequestId> {
    effects.iter().find_map(|effect| match effect {
        EffectKind::Control(ControlEffect::Cas { request, .. }) => Some(*request),
        _ => None,
    })
}

/// M7B-144 (design §3.3 `Differs` row: the copy is not a target of this rebuild; §3.4 the
/// tracker consumes `CopyQuarantined` exactly like `DivergenceDetected`; §5.6a a `CopyLost` is a
/// stall). One story on nodes A and B, carried by F1's own `Recovered`:
///
/// 1. B's receiver holds another digest at the cutoff. `Recovered` quarantines it
///    (`DivergentHistory`) and it sends nothing, so no `AppendAck`.
/// 2. A's primary, rebuilt by the same result, sends B an append. B answers `Quarantined`, and
///    B's cursor turns that into `CopyQuarantined{B}`.
/// 3. Routed back to R1, the tracker marks B diverged and emits the B-R26 vector once: the
///    alert and the loss (the predicate was already false, and C keeps the floor). A later
///    `DivergenceDetected(B)`, and the same `CopyQuarantined` again, are `AlreadyDiverged`. The
///    routed step is the Q-56 consumer arm, shown by behaviour rather than by grep.
/// 4. R1's `CopyLost{B}` is handed to F1 as `KernelEvent::CopyLost`, the event routing makes of
///    it: exactly one `RebuildStalled{B}`, `required` unchanged. C then catches up and A and C
///    prove the head durable; no activation CAS follows, and F1 stays `Rebuilding`.
///
/// The plan's `Alert{RebuildStalled, B}` is landed as `RecoveryEffect::RebuildStalled{copy}`.
#[retcd_test]
fn m7b_144_copy_quarantined_reaches_the_tracker_and_stalls_rebuilding_loudly() {
    let (mut f1, result) = f1_rebuilding();
    let mut log = Vec::new();

    // 1. B's receiver: another history at the cutoff.
    let mut receiver = AppendReceiver::new(ReceiverInit {
        config: rf3(),
        own: COPY_B,
        lineage: lineage(),
        head: Head {
            seq: Seq(HEAD),
            digest: Digest([0xEE; 32]),
        },
        durable: DurableSeq(HEAD),
    })
    .expect("B's receiver");
    assert_eq!(receiver.on_recovered(&result), vec![alert()]);
    assert_eq!(receiver.applied_head().seq, Seq(HEAD), "no head moved");

    // 2. A's primary, rebuilt by the same result, and B's answer to its next append. Row 0
    // answers before the frame is decoded, so the body is immaterial.
    let mut module = Replication::new();
    module.install_primary(rf3_tracker());
    route(&mut module, recovered_event(&result));
    let append = Frame {
        id: MessageId(77),
        protocol: ENVELOPE_VERSION,
        config: CONFIG,
        sender: result.selected.root,
        body: Bytes::from_static(b"RDBA"),
    };
    let answer = receiver.on_append(&label(A), &append);
    let [EffectKind::Send(SendEffect::Unicast { to, frame })] = answer.as_slice() else {
        panic!("one reply: {answer:?}");
    };
    assert_eq!(*to, A);
    assert_eq!(
        decode_reply(&frame.body).expect("a reply"),
        AppendOutcome::Rejected(AppendReject::Quarantined)
    );
    assert_eq!(
        route(
            &mut module,
            EventKind::Transport(TransportEvent::Delivered {
                from: label(B),
                frame: frame.clone(),
            })
        ),
        vec![kernel(KernelEffect::CopyQuarantined { copy: COPY_B })]
    );

    // 3. Routed back to R1.
    let vector = route(
        &mut module,
        EventKind::Kernel(KernelEvent::CopyQuarantined { copy: COPY_B }),
    );
    assert_eq!(vector, vec![alert(), copy_lost(COPY_B)]);
    let tracker = primary_side(&module).tracker();
    assert!(tracker.is_diverged(COPY_B));
    assert_eq!(tracker.regular_secondaries(), vec![COPY_C]);
    for again in [
        KernelEvent::DivergenceDetected { copy: COPY_B },
        KernelEvent::CopyQuarantined { copy: COPY_B },
    ] {
        let answer = route(&mut module, EventKind::Kernel(again));
        assert_eq!(answer, vec![replica(ReplicaIgnoreReason::AlreadyDiverged)]);
        log.extend(answer);
    }

    // 4. The loss reaches F1.
    let required = f1.rebuild_required().cloned();
    for effect in &vector {
        if let EffectKind::Kernel(KernelEffect::CopyLost { copy }) = effect {
            log.extend(f1_on(
                &mut f1,
                20_000,
                EventKind::Kernel(KernelEvent::CopyLost { copy: *copy }),
            ));
        }
    }
    assert_eq!(
        log,
        vec![
            replica(ReplicaIgnoreReason::AlreadyDiverged),
            replica(ReplicaIgnoreReason::AlreadyDiverged),
            kernel(KernelEffect::Recovery(RecoveryEffect::RebuildStalled {
                copy: COPY_B
            })),
        ]
    );
    assert_eq!(f1.rebuild_required().cloned(), required);
    assert_eq!(
        required,
        Some([COPY_A, COPY_B, COPY_C].into_iter().collect())
    );
    log.extend(f1_rec(
        &mut f1,
        20_100,
        RecoveryEvent::CopyCaughtUp {
            copy: COPY_C,
            head: Seq(HEAD),
            digest: d(HEAD),
        },
    ));
    for (now, copy) in [(20_200, COPY_A), (20_300, COPY_C)] {
        let answer = f1_rec(&mut f1, now, f1_durable(copy));
        assert_eq!(
            answer,
            vec![replica(ReplicaIgnoreReason::BarrierNotDurable)],
            "{copy:?}"
        );
        log.extend(answer);
    }
    assert_eq!(f1.phase(), RecoveryPhase::Rebuilding);
    assert_eq!(log.iter().filter(|e| is_stall(e)).count(), 1);
    assert!(!log.iter().any(is_cas), "no activation CAS: {log:?}");
}

/// M7B-140 (design §3.6 step 1 `Differs`: the cursor emits `DivergenceDetected(copy)` and
/// nothing else; §3.4 `diverged` has one writer, the tracker; ruling Q-B-5). One story per
/// fixture, all routed through `Replication::step`:
///
/// 1. B answers `NeedPrefix{have 10}` with a digest the ladder holds another value for. The
///    cursor's step is exactly `[DivergenceDetected(B)]`, and the tracker is untouched by it.
/// 2. That effect, handed back as the event routing makes of it, reaches the tracker. It marks
///    B diverged and emits the B-R26 vector without index 0: `Alert`, `CopyLost`, then
///    `QualificationChanged{Lost}` only if the predicate flipped and `BlockPartition` only if the
///    floor is gone. Three fixtures take neither, the first, and both conditionals.
/// 3. The same event again is `AlreadyDiverged` and changes nothing.
/// 4. B's next ACK is dropped at rule 1d.
#[retcd_test]
fn m7b_140_catch_up_side_divergence_has_one_writer_and_one_vector() {
    let forked_prefix = AppendOutcome::Rejected(AppendReject::NeedPrefix {
        have: Seq(10),
        head_digest: Digest([0xEE; 32]),
    });
    let alone = || {
        let mut module = routed();
        route(&mut module, accepted(&b(HEAD, HEAD, HEAD)));
        module
    };
    let c_forked = || {
        let mut module = both_routed();
        route(&mut module, accepted(&forked(c(HEAD, HEAD, HEAD))));
        module
    };
    for (name, mut module, vector) in [
        // C still qualifies and is a floor: two wide.
        ("c healthy", both_routed(), vec![alert(), copy_lost(COPY_B)]),
        // B alone qualified, C is still a regular secondary: the edge, no block.
        (
            "c lagging",
            alone(),
            vec![
                alert(),
                copy_lost(COPY_B),
                lost(QualificationCause::DivergenceDetected(COPY_B)),
            ],
        ),
        // C already diverged by rule 9: the edge and the block, four wide.
        (
            "c diverged",
            c_forked(),
            vec![
                alert(),
                copy_lost(COPY_B),
                lost(QualificationCause::DivergenceDetected(COPY_B)),
                kernel(KernelEffect::BlockPartition(
                    BlockReason::DivergenceRequiresOperator {
                        diverged: vec![COPY_C, COPY_B],
                    },
                )),
            ],
        ),
    ] {
        // 1. The cursor proves it and writes nothing else.
        let before = primary_side(&module).tracker().clone();
        let proof = route(&mut module, reply(B, &forked_prefix));
        assert_eq!(
            proof,
            vec![kernel(KernelEffect::DivergenceDetected { copy: COPY_B })],
            "{name}"
        );
        assert_eq!(*primary_side(&module).tracker(), before, "{name}");
        assert!(primary_side(&module).cursor(COPY_B).is_none(), "{name}");

        // 2. Routed back, the tracker is the one writer, and emits the vector once.
        let [EffectKind::Kernel(KernelEffect::DivergenceDetected { copy })] = proof.as_slice()
        else {
            unreachable!("asserted above");
        };
        let event = EventKind::Kernel(KernelEvent::DivergenceDetected { copy: *copy });
        assert_eq!(route(&mut module, event.clone()), vector, "{name}");
        let tracker = primary_side(&module).tracker();
        assert!(tracker.is_diverged(COPY_B), "{name}");
        assert_eq!(
            tracker.diverged().iter().filter(|c| **c == COPY_B).count(),
            1,
            "{name}"
        );

        // 3. Idempotent.
        let before = tracker.clone();
        assert_eq!(
            route(&mut module, event),
            vec![replica(ReplicaIgnoreReason::AlreadyDiverged)],
            "{name}"
        );
        assert_eq!(*primary_side(&module).tracker(), before, "{name}");

        // 4. Rule 1d: B's next ACK counts for nothing.
        assert_eq!(
            route(&mut module, accepted(&b(HEAD, HEAD, HEAD))),
            vec![rejected(AckRejectReason::Diverged)],
            "{name}"
        );
        assert_eq!(*primary_side(&module).tracker(), before, "{name}");
    }
}

// --- Lead rulings B-R67, B-R67a: a lost catch-up ACK -----------------------------------------
//
// Joint-gate B1 (probe S3): one catch-up ACK lost below the cutoff stalled a rebuild for good,
// because the cursor waited on it and the keepalive skips a copy with a record in flight. R1's
// catch-up now re-sends: one retransmit timer per partition, every `RETRANSMIT_MS`, re-sending
// a record the cursor sent and nobody ACKed when nothing moved the cursor since the last fire.
// The rows below run R1 routed beside a real F1, with the test playing B's and C's receivers.

/// `P`'s retransmit timer firing at `version`.
fn retransmit_fired(version: u64) -> EventKind {
    fired_at(retransmit_timer(P), TimerVersion(version))
}

fn fired_at(id: TimerId, version: TimerVersion) -> EventKind {
    EventKind::Timer(TimerFired {
        id,
        version,
        scheduled_at: T,
    })
}

/// The retransmit arm a step at `T` makes.
fn retransmit_arm(version: u64) -> EffectKind {
    EffectKind::Timer(TimerEffect::Arm {
        id: retransmit_timer(P),
        version: TimerVersion(version),
        at: T.plus_millis(RETRANSMIT_MS),
    })
}

/// The version of the last arm of `id` in `effects`.
fn last_arm(effects: &[EffectKind], id: TimerId) -> Option<TimerVersion> {
    effects.iter().rev().find_map(|effect| match effect {
        EffectKind::Timer(TimerEffect::Arm {
            id: armed, version, ..
        }) if *armed == id => Some(*version),
        _ => None,
    })
}

/// L1's `SetAdmission` as routed to R1.
fn set_admission(allow: bool) -> EventKind {
    EventKind::Kernel(KernelEvent::SetAdmission(AdmissionState {
        allow,
        reason: (!allow).then_some(ErrorKind::ProtectionPaused),
        oldest_unsafe_age: 0,
        oldest_unsafe_seq: Seq(HEAD),
        replication_lag: ReplicationLag::millis(0),
        stalest_copy: None,
        lost_copies: vec![],
        paused_prefix: Seq(HEAD),
        resume_barrier: Seq(HEAD),
        required_config_versions: vec![CONFIG],
        outstanding_unsafe_bytes: 0,
    }))
}

/// How C's ACK for record 2 is lost in [`rebuild_losing_one_ack`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Loss {
    /// C takes record 2, and its ACK never arrives.
    Ack,
    /// The network also duplicates record 2. C answers the copy `AlreadyHave`, which arrives,
    /// and its ACK, which is lost as well. The cursor then has nothing outstanding (design
    /// §3.6), and record 2 is still unACKed.
    AfterAlreadyHave,
    /// Nothing is lost: C's ACK for record 2 is late. It arrives after the retransmit re-sent
    /// record 2, just before C's answers to that re-send (lead ruling B-R67c).
    Delay,
}

/// B's and C's receivers as the test plays them: the head each holds, in F1's new root.
struct Host {
    result: RecoveryResult,
    held: BTreeMap<NodeId, u64>,
    loss: Loss,
    lost: bool,
    /// C's late ACK for record 2, under `Loss::Delay`, until the re-send it trails.
    late: Option<EventKind>,
}

impl Host {
    /// `node`'s ACK at `at`, in the recovered root and pin.
    fn ack_at(&self, node: NodeId, at: u64) -> EventKind {
        let mut ack = ack(node, RegularSecondary, at, at, at);
        ack.generation = self.result.new_generation;
        ack.owner_epoch = self.result.committed.authority_view.lineage.owner_epoch;
        ack.config_version = self.result.committed.pinned_config.config_version;
        accepted(&ack)
    }

    /// What the copy a bare send names answers it: a record it holds draws `AlreadyHave` and
    /// its current ACK; the next record is taken and ACKed; a gap draws `NeedPrefix` from its
    /// head. Any other effect draws nothing.
    fn answer(&mut self, effect: &EffectKind) -> Vec<EventKind> {
        let EffectKind::Kernel(KernelEffect::SendEnvelopes {
            copy,
            from,
            through,
        }) = effect
        else {
            return Vec::new();
        };
        assert_eq!(from, through, "one record in flight: {effect:?}");
        let node = match *copy {
            COPY_B => B,
            COPY_C => C,
            other => panic!("A sends only to B and C here, not {other:?}"),
        };
        let held = self.held.get(&node).copied().unwrap_or(0);
        let seq = from.0;
        if seq <= held {
            let late = if node == C { self.late.take() } else { None };
            let mut answers: Vec<_> = late.into_iter().collect();
            answers.extend([
                reply(node, &AppendOutcome::AlreadyHave),
                self.ack_at(node, held),
            ]);
            return answers;
        }
        if seq > held + 1 {
            let digest = if held == 0 { Digest::ROOT } else { d(held) };
            return vec![reply(node, &need_prefix_at(held, digest))];
        }
        self.held.insert(node, seq);
        if (node, seq, self.lost) == (C, 2, false) {
            self.lost = true;
            return match self.loss {
                Loss::Ack => Vec::new(),
                Loss::AfterAlreadyHave => vec![reply(C, &AppendOutcome::AlreadyHave)],
                Loss::Delay => {
                    self.late = Some(self.ack_at(C, 2));
                    Vec::new()
                }
            };
        }
        vec![self.ack_at(node, seq)]
    }
}

/// Lead ruling B-R67 (joint-gate B1, probe S3), in one story on A. F1 commits `ReadOnly` and
/// rebuilds A, B and C. `Recovered` builds A's primary at the cutoff, 12, holding rungs 0 and
/// 12, and B and C, holding nothing, are walked from the root by A's cursors. B's walk is clean.
/// C's ACK for record 2, below the cutoff, is lost as `loss` says. L1 has said `admission`
/// (`None`: nothing). Each round the host answers every send, then fires every timer R1 armed
/// and has not had fired. Every `CopyCaughtUp` goes to F1; then A and each copy caught up
/// prove the head durable, and a CAS the rebuild proposes commits. Returns F1 and every effect
/// R1 emitted.
fn rebuild_losing_one_ack(admission: Option<bool>, loss: Loss) -> (Recovery, Vec<EffectKind>) {
    let (mut f1, result) = f1_rebuilding();
    let mut module = Replication::new();
    let mut log = route(&mut module, recovered_event(&result));
    assert!(module.primary(A, P).is_some(), "A's primary is built");
    let mut host = Host {
        result,
        held: BTreeMap::new(),
        loss,
        lost: false,
        late: None,
    };
    let mut queue = VecDeque::new();
    if let Some(allow) = admission {
        queue.extend(route(&mut module, set_admission(allow)));
    }
    // Without a keepalive round, B and C answer the stream's last append from their head.
    if admission != Some(false) {
        for node in [B, C] {
            queue.extend(route(
                &mut module,
                reply(node, &need_prefix_at(0, Digest::ROOT)),
            ));
        }
    }
    let mut fired = Vec::new();
    for _round in 0..8 {
        while let Some(effect) = queue.pop_front() {
            for answer in host.answer(&effect) {
                queue.extend(route(&mut module, answer));
            }
            log.push(effect);
        }
        for id in [retransmit_timer(P), keepalive_timer(P)] {
            if let Some(version) = last_arm(&log, id).filter(|v| !fired.contains(&(id, *v))) {
                fired.push((id, version));
                queue.extend(route(&mut module, fired_at(id, version)));
            }
        }
        if queue.is_empty() {
            break;
        }
    }

    let caught = caught_up_copies(&log);
    let mut now = 20_000;
    for copy in &caught {
        f1_rec(
            &mut f1,
            now,
            RecoveryEvent::CopyCaughtUp {
                copy: *copy,
                head: Seq(HEAD),
                digest: d(HEAD),
            },
        );
        now += 1;
    }
    let mut proposed = None;
    for copy in std::iter::once(COPY_A).chain(caught) {
        proposed = cas_request(&f1_rec(&mut f1, now, f1_durable(copy))).or(proposed);
        now += 1;
    }
    if let Some(request) = proposed {
        f1_on(
            &mut f1,
            now + 100,
            EventKind::Control(ControlEvent::CasResult {
                request,
                key: ControlKey::Partition(P),
                outcome: CasOutcome::Committed(Revision(10)),
            }),
        );
    }
    (f1, log)
}

/// The copies R1 reported caught up at the head, in order.
fn caught_up_copies(log: &[EffectKind]) -> Vec<CopyId> {
    log.iter()
        .filter_map(|effect| match effect {
            EffectKind::Kernel(KernelEffect::CopyCaughtUp { copy, head, digest })
                if (*head, *digest) == (Seq(HEAD), d(HEAD)) =>
            {
                Some(*copy)
            }
            _ => None,
        })
        .collect()
}

/// Every bare send in `log`, as `(copy, seq)`.
fn sends(log: &[EffectKind]) -> Vec<(u8, u64)> {
    log.iter()
        .filter_map(|effect| match effect {
            EffectKind::Kernel(KernelEffect::SendEnvelopes { copy, from, .. }) => {
                Some((copy.0, from.0))
            }
            _ => None,
        })
        .collect()
}

/// How many times `log` sends C record 2.
fn twos(log: &[EffectKind]) -> usize {
    log.iter().filter(|e| **e == send(COPY_C, 2)).count()
}

/// M7B-186 (lead ruling B-R67; joint-gate B1 under `Reject`, probe S3). The keepalive runs, and
/// it skips C, whose cursor has record 2 in flight. The retransmit re-sends record 2 once, C
/// answers `AlreadyHave` and its ACK, the walk goes on, and the rebuild reaches `Committed`.
/// Record 2 is sent exactly twice: the send and one re-send.
#[retcd_test]
fn m7b_186_a_lost_catch_up_ack_under_reject_still_reaches_committed() {
    let (f1, log) = rebuild_losing_one_ack(Some(false), Loss::Ack);
    assert!(
        last_arm(&log, keepalive_timer(P)).is_some(),
        "the keepalive ran"
    );
    assert_eq!(
        caught_up_copies(&log),
        vec![COPY_B, COPY_C],
        "{:?}",
        sends(&log)
    );
    assert_eq!(f1.phase(), RecoveryPhase::Committed);
    assert_eq!(twos(&log), 2, "{:?}", sends(&log));
}

/// M7B-187 (lead ruling B-R67; joint-gate B1 under `Allow`, probe S3b). No keepalive runs at
/// all, so nothing but the retransmit can recover the lost ACK. The rebuild reaches `Committed`,
/// and record 2 is sent exactly twice.
#[retcd_test]
fn m7b_187_a_lost_catch_up_ack_under_allow_still_reaches_committed() {
    let (f1, log) = rebuild_losing_one_ack(Some(true), Loss::Ack);
    assert_eq!(last_arm(&log, keepalive_timer(P)), None, "no keepalive");
    assert_eq!(
        caught_up_copies(&log),
        vec![COPY_B, COPY_C],
        "{:?}",
        sends(&log)
    );
    assert_eq!(f1.phase(), RecoveryPhase::Committed);
    assert_eq!(twos(&log), 2, "{:?}", sends(&log));
}

/// M7B-191 (lead ruling B-R67a). The ACK is lost after `AlreadyHave`, under `Allow`.
/// `AlreadyHave` left the cursor with nothing outstanding, as design §3.6 says it must, but
/// record 2 is still sent and unACKed, and that is what the timer watches. The rebuild reaches
/// `Committed`, and R1 sent record 2 exactly twice.
#[retcd_test]
fn m7b_191_an_ack_lost_after_already_have_under_allow_still_reaches_committed() {
    let (f1, log) = rebuild_losing_one_ack(Some(true), Loss::AfterAlreadyHave);
    assert_eq!(
        caught_up_copies(&log),
        vec![COPY_B, COPY_C],
        "{:?}",
        sends(&log)
    );
    assert_eq!(f1.phase(), RecoveryPhase::Committed);
    assert_eq!(twos(&log), 2, "{:?}", sends(&log));
}

/// M7B-188 (lead rulings B-R67, B-R67a: the rate bound). A primary built at cutoff 3 walks B
/// from the root, and B's ACK for record 1 is lost.
///
/// * The first send arms the partition's retransmit timer.
/// * The first fire finds nothing that has waited a whole interval: it re-sends nothing and
///   re-arms.
/// * Each later fire with no ACK progress since the one before re-sends record 1 once: the
///   same effect as the first send, byte for byte, and one per fire.
/// * A fire of a version no longer armed is `StaleTimer`.
/// * After ACK progress, the next fire re-sends nothing though a record is unACKed.
/// * Once nothing is unACKed, a fire is `NotRequired` and does not re-arm.
/// * One timer serves every cursor of the partition: B and C both unACKed draw one arm, and one
///   fire re-sends each once.
#[retcd_test]
fn m7b_188_a_re_send_is_the_same_send_once_per_interval_and_only_without_progress() {
    let mut module = built_at(3);
    let first = route(&mut module, reply(B, &need_prefix_at(0, Digest::ROOT)));
    assert_eq!(first, vec![send(COPY_B, 1), retransmit_arm(1)]);
    assert_eq!(
        route(&mut module, retransmit_fired(1)),
        vec![retransmit_arm(2)]
    );
    assert_eq!(
        route(&mut module, retransmit_fired(2)),
        vec![first[0].clone(), retransmit_arm(3)]
    );
    assert_eq!(
        route(&mut module, retransmit_fired(3)),
        vec![send(COPY_B, 1), retransmit_arm(4)]
    );
    assert_eq!(
        route(&mut module, retransmit_fired(3)),
        vec![replica(ReplicaIgnoreReason::StaleTimer)]
    );

    // B answers a re-send, and the ACK moves the cursor to record 2.
    assert_eq!(
        route(&mut module, reply(B, &AppendOutcome::AlreadyHave)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        vec![
            rejected(AckRejectReason::InFlightUnverified),
            send(COPY_B, 2)
        ]
    );
    assert_eq!(
        route(&mut module, retransmit_fired(4)),
        vec![retransmit_arm(5)],
        "progress since the last fire"
    );
    route(&mut module, accepted(&in_new_root(b(2, 2, 2))));
    let at_cut = route(&mut module, accepted(&in_new_root(b(3, 3, 3))));
    assert!(at_cut.contains(&caught_up(COPY_B, 3)), "{at_cut:?}");
    assert_eq!(
        route(&mut module, retransmit_fired(5)),
        vec![replica(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(
        route(&mut module, retransmit_fired(6)),
        vec![replica(ReplicaIgnoreReason::StaleTimer)]
    );

    // One timer for the partition, sweeping every cursor.
    let mut module = built_at(3);
    assert_eq!(
        route(&mut module, reply(B, &need_prefix_at(0, Digest::ROOT))),
        vec![send(COPY_B, 1), retransmit_arm(1)]
    );
    assert_eq!(
        route(&mut module, reply(C, &need_prefix_at(0, Digest::ROOT))),
        vec![send(COPY_C, 1)]
    );
    assert_eq!(
        route(&mut module, retransmit_fired(1)),
        vec![retransmit_arm(2)]
    );
    assert_eq!(
        route(&mut module, retransmit_fired(2)),
        vec![send(COPY_B, 1), send(COPY_C, 1), retransmit_arm(3)]
    );
}

/// M7B-192 (lead ruling B-R67a, read with B-R58c). The ACK a re-send draws below the cutoff
/// names a record the cursor sent and nobody ACKed. That is the record in flight as B-R58c means
/// it, even though the `AlreadyHave` before it left nothing outstanding (design §3.6). It drives
/// the cursor and nothing else: exactly `[InFlightUnverified, SendEnvelopes{B, 2}]`, and the
/// tracker is unchanged. A repeat of that ACK, once record 2 is the one sent, is a matching
/// repeat and answers `Recorded` (lead ruling B-R67c; it answered a snapshot request before).
/// Near-miss: the same ACK with another digest is not a repeat, and gets today's answer.
#[retcd_test]
fn m7b_192_the_ack_a_re_send_draws_below_the_cutoff_still_drives_the_cursor() {
    let mut module = built_at(3);
    route(&mut module, reply(B, &need_prefix_at(0, Digest::ROOT)));
    assert_eq!(
        route(&mut module, reply(B, &AppendOutcome::AlreadyHave)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    let cursor = primary_side(&module).cursor(COPY_B).expect("running");
    assert_eq!(cursor.outstanding(), None, "design §3.6 is unchanged");
    let before = primary_side(&module).tracker().clone();
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        vec![
            rejected(AckRejectReason::InFlightUnverified),
            send(COPY_B, 2)
        ]
    );
    assert_eq!(primary_side(&module).tracker(), &before);

    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(
        route(&mut module, accepted(&forked(in_new_root(b(1, 1, 1))))),
        vec![
            rejected(AckRejectReason::Unverifiable),
            kernel(KernelEffect::SnapshotCatchupRequired {
                copy: COPY_B,
                barrier: Seq(3),
            }),
        ]
    );
}

// --- Lead ruling B-R67c: the per-cursor ACK high-water mark --------------------------------

/// Every `SendEnvelopes` to `copy` in `log`, as sequence numbers.
fn sends_to(log: &[EffectKind], copy: CopyId) -> Vec<u64> {
    log.iter()
        .filter_map(|effect| match effect {
            EffectKind::Kernel(KernelEffect::SendEnvelopes { copy: to, from, .. })
                if *to == copy =>
            {
                Some(from.0)
            }
            _ => None,
        })
        .collect()
}

fn asks_for_a_snapshot(log: &[EffectKind]) -> bool {
    log.iter().any(|effect| {
        matches!(
            effect,
            EffectKind::Kernel(KernelEffect::SnapshotCatchupRequired { .. })
        )
    })
}

/// M7B-194 (lead ruling B-R67c; tester-kb-r1 re-gate B2, probe R1). A slow ACK, not a lost one,
/// below the cutoff. B's ACK for record 1 is still in the network when the second fire re-sends
/// record 1. Then the answers arrive in their natural order: the original ACK, which moves the
/// cursor to record 2, then the re-send's `AlreadyHave` and its ACK for record 1. That second
/// ACK is a repeat: at the high-water mark, with the digest already accepted there. It answers
/// exactly `Recorded`: no snapshot request, no send, and neither the tracker nor the cursor
/// changes.
/// A repeat below the mark is one too, where no rung contradicts it: after B's ACK at 2, a
/// third ACK for record 1 is `Recorded`.
#[retcd_test]
fn m7b_194_a_slow_ack_below_the_cutoff_and_its_re_sends_ack_ask_for_no_snapshot() {
    let mut module = walking_b_from_root();
    route(&mut module, retransmit_fired(1));
    assert_eq!(
        route(&mut module, retransmit_fired(2)),
        vec![send(COPY_B, 1), retransmit_arm(3)]
    );
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        vec![
            rejected(AckRejectReason::InFlightUnverified),
            send(COPY_B, 2)
        ]
    );
    assert_eq!(
        route(&mut module, reply(B, &AppendOutcome::AlreadyHave)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(primary_side(&module), &before);

    // Below the mark, where no rung contradicts it: B's ACK at 2 moves the mark, and a third
    // ACK for record 1, later still, is a repeat as well.
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(2, 2, 2)))),
        vec![
            rejected(AckRejectReason::InFlightUnverified),
            send(COPY_B, 3)
        ]
    );
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(primary_side(&module), &before);
}

/// M7B-195 (lead ruling B-R67c: a conflicting repeat is not a repeat). After B's ACK at 1 moved
/// the cursor, an ACK at 1 whose digest differs from the one accepted there is divergence
/// evidence, not a repeat: it takes today's path, `[Unverifiable, SnapshotCatchupRequired{B,
/// 3}]`, below the cutoff. Above it, on the golden tracker, the ladder's rung is the digest taken
/// there: after B's ACK at 10, an ACK at 9 with another digest is not a repeat either, and gets
/// today's answer for a watermark that retreats, `RegressedProgress`. Nor is an ACK in another
/// generation, however well it matches: it is `StaleGeneration`, as rules 1–7 say. Near-misses:
/// the matching repeat is `Recorded` each time (M7B-194).
#[retcd_test]
fn m7b_195_a_repeat_ack_with_another_digest_still_asks_for_a_snapshot() {
    let mut module = walking_b_from_root();
    route(&mut module, accepted(&in_new_root(b(1, 1, 1))));
    let before = primary_side(&module).tracker().clone();
    assert_eq!(
        route(&mut module, accepted(&forked(in_new_root(b(1, 1, 1))))),
        vec![
            rejected(AckRejectReason::Unverifiable),
            kernel(KernelEffect::SnapshotCatchupRequired {
                copy: COPY_B,
                barrier: Seq(3),
            }),
        ]
    );
    assert_eq!(primary_side(&module).tracker(), &before);
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    // Another generation: the old root's ACK, digest and progress unchanged.
    assert_eq!(
        route(&mut module, accepted(&b(1, 1, 1))),
        vec![rejected(AckRejectReason::StaleGeneration)]
    );

    // Above the cutoff, below the mark.
    let mut module = routed();
    route(&mut module, reply(B, &need_prefix(8)));
    route(&mut module, accepted(&b(9, 9, 9)));
    assert_eq!(
        sends_to(&route(&mut module, accepted(&b(10, 10, 10))), COPY_B),
        vec![11]
    );
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, accepted(&forked(b(9, 9, 9)))),
        vec![rejected(AckRejectReason::RegressedProgress)]
    );
    assert_eq!(
        route(&mut module, accepted(&b(9, 9, 9))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(primary_side(&module), &before);
}

/// M7B-196 (lead ruling B-R67c; tester-kb-r1 re-gate M3, probe R2). A slow ACK above the
/// cutoff: the golden tracker at head 12, B walked from 8. Record 9 is re-sent, and both its ACKs
/// arrive, the re-send's after the cursor moved on. Every send is then answered once, in order,
/// until B is caught up. The repeat ACK sits at the mark, so it reports `PeerProgress{B, 9}` and
/// sends nothing (update B-R67d; under B-R67c it was `Recorded`), and no record after the
/// re-sent one is sent twice: one duplicate ACK no longer doubles the rest of the walk.
#[retcd_test]
fn m7b_196_a_slow_ack_above_the_cutoff_does_not_double_the_rest_of_the_walk() {
    let mut module = routed();
    let mut log = route(&mut module, reply(B, &need_prefix(8)));
    log.extend(route(&mut module, retransmit_fired(1)));
    log.extend(route(&mut module, retransmit_fired(2)));
    assert_eq!(sends_to(&log, COPY_B), vec![9, 9]);
    log.extend(route(&mut module, accepted(&b(9, 9, 9))));
    log.extend(route(&mut module, reply(B, &AppendOutcome::AlreadyHave)));
    assert_eq!(
        route(&mut module, accepted(&b(9, 9, 9))),
        vec![peer_progress(B, 9)]
    );
    let mut queue: VecDeque<EventKind> = VecDeque::new();
    queue.push_back(accepted(&b(10, 10, 10)));
    let mut steps = 0;
    while let Some(kind) = queue.pop_front() {
        steps += 1;
        assert!(
            steps < 32,
            "the walk does not end: {:?}",
            sends_to(&log, COPY_B)
        );
        let out = route(&mut module, kind);
        for seq in sends_to(&out, COPY_B) {
            if seq < HEAD {
                queue.push_back(accepted(&b(seq, seq, seq)));
            }
        }
        log.extend(out);
    }
    assert_eq!(sends_to(&log, COPY_B), vec![9, 9, 10, 11, 12]);
}

/// M7B-197 (lead ruling B-R67c item A2; tester-kb-r1 re-gate probe R4). The keepalive's "record
/// in flight" is the retransmit's: sent and not ACKed. Under `Reject`, B's cursor sends record 9
/// and B answers `AlreadyHave`, whose ACK is lost. `outstanding` is `None` (design §3.6) and
/// `unacked` is 9, so the next keepalive round sends the head to C and not to B, and B's record
/// is left to the retransmit.
#[retcd_test]
fn m7b_197_the_keepalive_skips_a_copy_whose_record_is_unacked_after_already_have() {
    let mut module = routed();
    let paused = route(&mut module, set_admission(false));
    let version = last_arm(&paused, keepalive_timer(P)).expect("the keepalive armed");
    route(&mut module, reply(B, &need_prefix(8)));
    route(&mut module, reply(B, &AppendOutcome::AlreadyHave));
    let cursor = primary_side(&module).cursor(COPY_B).expect("running");
    assert_eq!(
        (cursor.outstanding(), cursor.unacked()),
        (None, Some(Seq(9)))
    );
    let round = route(&mut module, fired_at(keepalive_timer(P), version));
    assert_eq!(sends_to(&round, COPY_B), Vec::<u64>::new(), "{round:?}");
    assert_eq!(sends_to(&round, COPY_C), vec![HEAD], "{round:?}");
}

/// M7B-198 (lead ruling B-R67; tester-kb-r1 re-gate A4, probe R5, mutant U06). The record a
/// probe answer sends is the record in flight. After `ProbeDigestAt{8}` the cursor sent 8, not
/// 9, so when that reply is lost the retransmit re-sends 8.
#[retcd_test]
fn m7b_198_a_lost_probe_answer_re_sends_the_probed_record() {
    let mut module = routed();
    route(&mut module, reply(B, &need_prefix(8)));
    assert_eq!(
        sends_to(
            &route(
                &mut module,
                reply(B, &AppendOutcome::ProbeDigestAt { seq: Seq(8) })
            ),
            COPY_B
        ),
        vec![8]
    );
    let mut fires = route(&mut module, retransmit_fired(1));
    fires.extend(route(&mut module, retransmit_fired(2)));
    assert_eq!(sends_to(&fires, COPY_B), vec![8], "{fires:?}");
}

/// M7B-199 (lead ruling B-R67; tester-kb-r1 re-gate A4, probe R6, mutant U08). A `NeedPrefix`
/// below retention asks for a snapshot and leaves nothing in flight, so the retransmit re-sends
/// nothing: its next fire is `NotRequired` and does not re-arm, and the one after is stale.
#[retcd_test]
fn m7b_199_a_snapshot_request_leaves_nothing_to_re_send() {
    let mut module = built_at(10);
    route(&mut module, reply(B, &need_prefix_at(0, Digest::ROOT)));
    let snapshot = route(
        &mut module,
        reply(B, &need_prefix_at(5, forked(b(5, 5, 5)).digest_at_buffered)),
    );
    assert!(asks_for_a_snapshot(&snapshot), "{snapshot:?}");
    assert_eq!(
        route(&mut module, retransmit_fired(1)),
        vec![replica(ReplicaIgnoreReason::NotRequired)]
    );
    assert_eq!(
        route(&mut module, retransmit_fired(2)),
        vec![replica(ReplicaIgnoreReason::StaleTimer)]
    );
}

/// M7B-200 (lead ruling B-R67c; tester-kb-r1 re-gate B2 and its blind-spot note: delay, not
/// loss). The M7B-186 rebuild under `Allow`, with nothing lost: C's ACK for record 2 is only
/// late. It arrives after the retransmit re-sent record 2, just before C's `AlreadyHave` and
/// second ACK for it. The rebuild reaches `Committed`; R1 never asks for a snapshot; record 2
/// goes to C twice, the send and one re-send, and every other record once.
#[retcd_test]
fn m7b_200_a_late_catch_up_ack_under_allow_reaches_committed_with_no_snapshot() {
    let (f1, log) = rebuild_losing_one_ack(Some(true), Loss::Delay);
    assert!(!asks_for_a_snapshot(&log), "{log:?}");
    assert_eq!(
        caught_up_copies(&log),
        vec![COPY_B, COPY_C],
        "{:?}",
        sends(&log)
    );
    assert_eq!(f1.phase(), RecoveryPhase::Committed);
    let to_c = sends_to(&log, COPY_C);
    assert_eq!(twos(&log), 2, "{to_c:?}");
    for seq in (1..=HEAD).filter(|seq| *seq != 2) {
        assert_eq!(
            to_c.iter().filter(|s| **s == seq).count(),
            1,
            "record {seq}: {to_c:?}"
        );
    }
}

/// M7B-202 (lead ruling B-R67c: "sends only when the ACK moves past the high-water mark"). An
/// ACK that raises any one watermark is not a repeat: it takes the ladder, which admits it as it
/// would any other. But the cursor sends only for one whose `received` moves past the mark. B's
/// cursor has record 10 in flight after an ACK at `(9, 9, 8)`:
/// * `(9, 9, 9)` raises only `durable`: the ladder's effects, then `Recorded`; nothing is sent
///   and record 10 stays unACKed.
/// * `(10, 9, 9)` raises `received`: the ladder's effects, then record 11.
/// * `(10, 10, 9)` raises only `buffered_applied`: the ladder's effects, then `Recorded`.
#[retcd_test]
fn m7b_202_an_ack_that_raises_a_watermark_but_not_received_reaches_the_tracker_and_sends_nothing() {
    let mut module = routed();
    route(&mut module, reply(B, &need_prefix(8)));
    assert_eq!(
        sends_to(&route(&mut module, accepted(&b(9, 9, 8))), COPY_B),
        vec![10]
    );
    let mut reference = primary_side(&module).tracker().clone();
    for (ack, then) in [
        (b(9, 9, 9), replica(ReplicaIgnoreReason::Recorded)),
        (b(10, 9, 9), send(COPY_B, 11)),
        (b(10, 10, 9), replica(ReplicaIgnoreReason::Recorded)),
    ] {
        let mut want = deliver(&mut reference, &ack);
        assert!(
            matches!(
                want[0],
                EffectKind::Kernel(KernelEffect::PeerProgress { .. })
            ),
            "the ladder admits {ack:?}: {want:?}"
        );
        want.push(then);
        assert_eq!(route(&mut module, accepted(&ack)), want, "{ack:?}");
        assert_eq!(primary_side(&module).tracker(), &reference, "{ack:?}");
        if ack == b(9, 9, 9) {
            assert_eq!(
                primary_side(&module)
                    .cursor(COPY_B)
                    .expect("running")
                    .unacked(),
                Some(Seq(10))
            );
        }
    }
}

// --- Lead ruling B-R67d: a repeat at the mark is a liveness report ---------------------------

/// Paused, with B's cursor idle at its high-water mark, the head: B asked from the head, its
/// ACK there moved the cursor, and nothing is unACKed.
fn paused_with_b_idle_at_the_head() -> (Replication, TimerVersion) {
    let mut module = routed();
    let paused = route(&mut module, set_admission(false));
    let version = last_arm(&paused, keepalive_timer(P)).expect("the keepalive armed");
    route(&mut module, reply(B, &need_prefix(HEAD)));
    route(&mut module, accepted(&b(HEAD, HEAD, HEAD)));
    let cursor = primary_side(&module).cursor(COPY_B).expect("running");
    assert_eq!((cursor.outstanding(), cursor.unacked()), (None, None));
    (module, version)
}

/// M7B-203 (lead ruling B-R67d, read with B-R60). A repeat at the mark is a liveness report. B's
/// cursor is idle at 12; the keepalive round re-sends the head and B answers `AlreadyHave` and
/// an ACK at 12: every watermark at the mark, with the digest taken there. It is still split off
/// before the cursor, so nothing is sent, but it runs the tracker's rules 1–9 and emits exactly
/// what a first ACK at 12 would against the same tracker: `PeerProgress{B, 12}`. Without it L1's
/// lag never refreshes and a paused partition never resumes. The cursor is unchanged.
#[retcd_test]
fn m7b_203_a_keepalive_ack_at_an_idle_cursors_mark_reports_peer_progress_and_sends_nothing() {
    let (mut module, version) = paused_with_b_idle_at_the_head();
    let round = route(&mut module, fired_at(keepalive_timer(P), version));
    assert_eq!(sends_to(&round, COPY_B), vec![HEAD], "{round:?}");
    assert_eq!(
        route(&mut module, reply(B, &AppendOutcome::AlreadyHave)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    let cursor = primary_side(&module).cursor(COPY_B).cloned();
    let mut reference = primary_side(&module).tracker().clone();
    let keepalive = b(HEAD, HEAD, HEAD);
    let want = deliver(&mut reference, &keepalive);
    assert_eq!(want, vec![peer_progress(B, HEAD)]);
    assert_eq!(route(&mut module, accepted(&keepalive)), want);
    assert_eq!(primary_side(&module).tracker(), &reference);
    assert_eq!(primary_side(&module).cursor(COPY_B).cloned(), cursor);
}

/// M7B-204 (lead ruling B-R67d). A repeat strictly below the mark reports nothing: a late
/// duplicate must not tell L1 a position older than the one it already has. B's cursor is idle
/// at 12. A late ACK at 11, and one at 12 whose `durable` is still 11, each answer exactly
/// `Recorded`: no `PeerProgress`, no send, and neither the tracker nor the cursor changes.
/// Near-miss: the ACK at the mark itself reports (M7B-203).
#[retcd_test]
fn m7b_204_a_late_duplicate_below_an_idle_cursors_mark_is_recorded_and_reports_nothing() {
    let (mut module, _) = paused_with_b_idle_at_the_head();
    let before = primary_side(&module).clone();
    for late in [b(11, 11, 11), b(HEAD, HEAD, 11)] {
        assert_eq!(
            route(&mut module, accepted(&late)),
            vec![replica(ReplicaIgnoreReason::Recorded)],
            "{late:?}"
        );
        assert_eq!(primary_side(&module), &before, "{late:?}");
    }
}

// --- Lead rulings B-R67e and B-R67f: a repeat is judged against what the primary knows --------

/// `built_at(3)` after B, root-seeded, was walked to the cutoff: its ACKs at 1 and 2 drove the
/// cursor unverified, and the one at 3 was verified.
fn b_walked_to_the_cutoff() -> Replication {
    let mut module = walking_b_from_root();
    for seq in 1..=3 {
        route(&mut module, accepted(&in_new_root(b(seq, seq, seq))));
    }
    module
}

/// A second `Recovered` in the same generation at the same cutoff 3, its barrier naming
/// `copies`: F1's rebuild re-announcing once B is proved durable there.
fn recovered_again(copies: &[CopyId]) -> EventKind {
    recovered_event(&requiring(recovery(3, d(3), pin_with_b()), copies))
}

/// M7B-205 (lead rulings B-R67e and B-R67f; rdb-sim S4b and the k=7 run of
/// `rebuild_two_slow_replies_end_as_the_control`). A second `Recovered`, whose barrier proves B
/// durable at 3, drops B's cursor and starts B's watermarks at zero. A duplicate of B's ACK at 1
/// from before it then arrives. That ACK is below B's proved floor, so it is a repeat: exactly
/// `Recorded`, with no snapshot request and no change. The same holds once B's `NeedPrefix` has
/// made a new cursor that has taken no ACK yet (the k=7 shape): with no mark, the floor still
/// judges. Near-misses: an ACK at the floor with another digest is divergence, not a repeat; and
/// a later `Recovered` whose barrier does not name B replaces the floor, so the duplicate gets
/// today's answer again.
#[retcd_test]
fn m7b_205_a_duplicate_ack_right_after_a_recovered_rebuild_is_not_escalated() {
    let mut module = b_walked_to_the_cutoff();
    route(&mut module, recovered_again(&[COPY_A, COPY_B, COPY_C]));
    assert!(primary_side(&module).cursor(COPY_B).is_none());
    let before = primary_side(&module).clone();
    for late in [b(1, 1, 1), b(2, 2, 2), b(1, 1, 0)] {
        assert_eq!(
            route(&mut module, accepted(&in_new_root(late))),
            vec![replica(ReplicaIgnoreReason::Recorded)],
            "{late:?}"
        );
        assert_eq!(primary_side(&module), &before, "{late:?}");
    }

    route(&mut module, reply(B, &need_prefix_at(3, d(3))));
    let cursor = primary_side(&module).cursor(COPY_B).cloned();
    assert!(cursor.is_some(), "the NeedPrefix made a cursor");
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 0)))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(primary_side(&module), &before);

    let wrong = forked(in_new_root(b(3, 3, 3)));
    let effects = route(&mut module, accepted(&wrong));
    assert_eq!(
        effects[0],
        kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
        "{effects:?}"
    );

    // A later `Recovered` restates what is proved, replacing the floor, not raising it: its
    // barrier does not name B, so B knows nothing and the same duplicate gets today's answer.
    let mut module = b_walked_to_the_cutoff();
    route(&mut module, recovered_again(&[COPY_A, COPY_B, COPY_C]));
    route(&mut module, recovered_again(&[COPY_A, COPY_C]));
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        vec![
            rejected(AckRejectReason::Unverifiable),
            kernel(KernelEffect::SnapshotCatchupRequired {
                copy: COPY_B,
                barrier: Seq(3),
            }),
        ]
    );
}

/// M7B-206 (lead ruling B-R67f). An ACK exactly at B's proved floor, with the digest the ladder
/// holds there, is a liveness report, not `Recorded`: it takes the ladder and emits exactly what
/// a reference tracker fed the same ACK emits, `PeerProgress{B, 3}` first.
#[retcd_test]
fn m7b_206_an_ack_at_the_proved_floor_takes_the_ladder() {
    let mut module = b_walked_to_the_cutoff();
    route(&mut module, recovered_again(&[COPY_A, COPY_B, COPY_C]));
    let mut reference = primary_side(&module).tracker().clone();
    let at_floor = in_new_root(b(3, 3, 3));
    let want = deliver(&mut reference, &at_floor);
    assert_eq!(want[0], peer_progress(B, 3), "{want:?}");
    assert_eq!(route(&mut module, accepted(&at_floor)), want);
    assert_eq!(primary_side(&module).tracker(), &reference);
}

/// M7B-207 (lead ruling B-R67f: the floor is read by the repeat judgment only). Two primaries
/// rebuilt by the same second `Recovered`, except that one barrier proves B durable at 3 and
/// the other does not. They answer the `Recovered` alike, hold B's watermarks at zero alike,
/// and answer the same stream of ACKs that are not repeats alike, every `PeerProgress`,
/// qualification edge and durable view included. The floor moves no watermark and no view.
#[retcd_test]
fn m7b_207_the_proved_floor_moves_no_watermark_and_no_view() {
    let mut proved = b_walked_to_the_cutoff();
    let mut unproved = b_walked_to_the_cutoff();
    assert_eq!(
        route(&mut proved, recovered_again(&[COPY_A, COPY_B, COPY_C])),
        route(&mut unproved, recovered_again(&[COPY_A, COPY_C]))
    );
    for ack in [
        in_new_root(c(3, 3, 3)),
        in_new_root(b(3, 3, 3)),
        in_new_root(b(3, 3, 3)),
    ] {
        assert_eq!(
            route(&mut proved, accepted(&ack)),
            route(&mut unproved, accepted(&ack)),
            "{ack:?}"
        );
        for copy in [COPY_A, COPY_B, COPY_C] {
            assert_eq!(
                primary_side(&proved)
                    .tracker()
                    .peer(copy)
                    .map(|peer| peer.progress),
                primary_side(&unproved)
                    .tracker()
                    .peer(copy)
                    .map(|peer| peer.progress),
                "{copy:?} after {ack:?}"
            );
        }
    }
    assert_eq!(
        primary_side(&proved)
            .tracker()
            .peer(COPY_B)
            .map(|peer| peer.progress),
        Some(progress(3, 3, 3))
    );
}

// --- B-R67g: a flush ACK is not new content; the floor's reset; below an unverified mark -----

/// B's cursor on a primary built at cutoff 3, after B's applied ACK `(1, 1, 0)` for record 1:
/// the cursor has moved on, record 2 is in flight, and nothing is durable on B yet.
fn b_applied_one_below_the_cutoff() -> Replication {
    let mut module = walking_b_from_root();
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 0)))),
        vec![
            rejected(AckRejectReason::InFlightUnverified),
            send(COPY_B, 2)
        ]
    );
    module
}

/// M7B-208 (lead ruling B-R67g; tester-kb-r1 regate-2 F1, probe `zz_f`). Below the cutoff, B's
/// flush ACK `(1, 1, 1)` arrives after its applied ACK `(1, 1, 0)` already moved the cursor on
/// to record 2. It carries no new content, only that B made durable what it applied, and the
/// ladder has no rung to verify it. It is exactly `Recorded`: no snapshot, no send, the tracker
/// unchanged, record 2 still the one unACKed, and the mark's `durable` raised to 1, so a
/// duplicate of the flush is a repeat at the mark. A flush ACK overtaken on the wire by the
/// next record's applied ACK is below the mark that ACK set: `Recorded`. Near-misses: a flush whose digest contradicts
/// the mark's still escalates; so does a `durable` rise whose `received` and applied are below
/// the mark, which is not a flush at it; and above the cutoff, where the ladder verifies it, the flush
/// takes the ladder as before (M7B-202's pattern): `PeerProgress`, then the cursor's
/// `Recorded`, and a forked one is divergence.
#[retcd_test]
fn m7b_208_a_flush_ack_below_the_cutoff_is_recorded_and_raises_only_the_marks_durable() {
    let mut module = b_applied_one_below_the_cutoff();
    let tracker = primary_side(&module).tracker().clone();
    let flush = in_new_root(b(1, 1, 1));
    for _ in 0..2 {
        assert_eq!(
            route(&mut module, accepted(&flush)),
            vec![replica(ReplicaIgnoreReason::Recorded)]
        );
        assert_eq!(primary_side(&module).tracker(), &tracker);
        let cursor = primary_side(&module).cursor(COPY_B).expect("running");
        assert_eq!(
            cursor.mark(),
            Some((progress(1, 1, 1), flush.digest_at_buffered))
        );
        assert_eq!(cursor.unacked(), Some(Seq(2)));
    }
    // The walk goes on from the ACK for record 2.
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(2, 2, 1)))),
        vec![
            rejected(AckRejectReason::InFlightUnverified),
            send(COPY_B, 3)
        ]
    );

    // Near-miss: a flush that contradicts the digest the mark took is not a repeat.
    let mut module = b_applied_one_below_the_cutoff();
    assert_eq!(
        route(&mut module, accepted(&forked(in_new_root(b(1, 1, 1))))),
        vec![
            rejected(AckRejectReason::Unverifiable),
            kernel(KernelEffect::SnapshotCatchupRequired {
                copy: COPY_B,
                barrier: Seq(3),
            }),
        ]
    );

    // The flush ACK overtaken on the wire by the next record's applied ACK `(2, 2, 1)` is below
    // the mark that ACK set, with `durable` equal to it: `Recorded`, and nothing changes.
    let mut module = b_applied_one_below_the_cutoff();
    route(&mut module, accepted(&in_new_root(b(2, 2, 1))));
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, accepted(&flush)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(primary_side(&module), &before);

    // Near-miss: `durable` above the mark with `received` and applied below it is not a flush
    // at the mark, and gets today's answer. An honest receiver never sends it: every ACK after
    // its flush of 1 carries `durable` of at least 1.
    let mut module = b_applied_one_below_the_cutoff();
    route(&mut module, accepted(&in_new_root(b(2, 2, 0))));
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(1, 1, 1)))),
        vec![
            rejected(AckRejectReason::Unverifiable),
            kernel(KernelEffect::SnapshotCatchupRequired {
                copy: COPY_B,
                barrier: Seq(3),
            }),
        ]
    );

    // Above the cutoff the ladder verifies the flush, and it takes the ladder as before.
    let mut module = routed();
    route(&mut module, reply(B, &need_prefix(8)));
    route(&mut module, accepted(&b(9, 9, 8)));
    let mut reference = primary_side(&module).tracker().clone();
    let mut want = deliver(&mut reference, &b(9, 9, 9));
    assert_eq!(want[0], peer_progress(B, 9), "{want:?}");
    want.push(replica(ReplicaIgnoreReason::Recorded));
    let forked_flush = route(&mut module.clone(), accepted(&forked(b(9, 9, 9))));
    assert_eq!(
        forked_flush[0],
        kernel(KernelEffect::DivergenceDetected { copy: COPY_B }),
        "{forked_flush:?}"
    );
    assert_eq!(route(&mut module, accepted(&b(9, 9, 9))), want);
    assert_eq!(primary_side(&module).tracker(), &reference);
}

/// The new root's record `seq`, chained on `prev` and sealed, as A sends it to B after F1's
/// takeover. B's real receiver computes its digest, so the cutoff rung is the one it computes.
fn new_root_record(seq: u64, prev: Digest) -> ReplicationEnvelope {
    let mut env = ReplicationEnvelope {
        header: EnvelopeHeader {
            protocol_version: ENVELOPE_VERSION,
            partition: P,
            generation: NEW_GEN,
            config_version: NEW_CONFIG,
            owner_epoch: NEW_EPOCH,
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

/// The one reply frame `effects` sends A.
fn reply_to_a(effects: &[EffectKind]) -> Frame {
    let [EffectKind::Send(SendEffect::Unicast { to, frame })] = effects else {
        panic!("one reply: {effects:?}");
    };
    assert_eq!(*to, A);
    frame.clone()
}

/// M7B-209 (lead ruling B-R67g; tester-kb-r1 regate-2 F1 and its retrospective). A walk whose
/// copy is B's real `AppendReceiver`, not a model of it. A primary built at cutoff 3 walks B,
/// which holds nothing, from the root. For each record B stages, storage commits, and B sends
/// its applied ACK; then storage flushes, and `on_flushed` sends B's flush ACK. Each flush ACK
/// reaches A after its applied ACK already moved the cursor on, and before the next record is
/// staged. That is one real interleaving of several: a flush can also land while the next record
/// is staged (M7B-215), and a delayed re-send can draw an ACK then (M7B-216). B ACKs every
/// record twice, applied then durable; each record is sent once; nothing asks for a snapshot
/// or is `Unverifiable`; B is caught up once, at the cutoff; and B's last flush, which the
/// cutoff rung verifies, leaves B durable at 3.
#[retcd_test]
fn m7b_209_a_receiver_that_acks_applied_then_flushed_is_walked_below_the_cutoff() {
    let mut records: Vec<ReplicationEnvelope> = Vec::new();
    for seq in 1..=3 {
        let prev = records.last().map_or(Digest::ROOT, |env| env.record_digest);
        records.push(new_root_record(seq, prev));
    }
    let result = requiring(
        recovery(3, records[2].record_digest, pin_with_b()),
        &[COPY_A, COPY_C],
    );
    let mut module = Replication::new();
    route(&mut module, recovered_event(&result));
    let mut receiver = AppendReceiver::new(ReceiverInit {
        config: pin_with_b(),
        own: COPY_B,
        lineage: result.selected.root,
        head: Head {
            seq: Seq::ZERO,
            digest: Digest::ROOT,
        },
        durable: DurableSeq(0),
    })
    .expect("B's receiver");

    let mut log = route(&mut module, reply(B, &need_prefix_at(0, Digest::ROOT)));
    let (mut sent, mut acks, mut next) = (Vec::new(), Vec::new(), 0);
    while let Some(effect) = log.get(next).cloned() {
        next += 1;
        let EffectKind::Kernel(KernelEffect::SendEnvelopes {
            copy,
            from,
            through,
        }) = effect
        else {
            continue;
        };
        assert_eq!(
            (copy, from),
            (COPY_B, through),
            "one record to B: {effect:?}"
        );
        sent.push(from.0);
        let record = &records[usize::try_from(from.0 - 1).expect("seq")];
        let append = Frame {
            id: MessageId(u32::try_from(from.0).expect("seq")),
            protocol: ENVELOPE_VERSION,
            config: NEW_CONFIG,
            sender: result.selected.root,
            body: record.encode().expect("encode"),
        };
        let staged = receiver.on_append(&label(A), &append);
        let [EffectKind::Store(StoreEffect::Commit(batch))] = staged.as_slice() else {
            panic!("B stages the record: {staged:?}");
        };
        let applied = receiver.on_committed(batch.id).expect("B staged it");
        let durable = [DurablePrefix {
            partition: P,
            generation: NEW_GEN,
            through: DurableSeq(from.0),
        }];
        let flushed = receiver.on_flushed(&durable).expect("B's prefix");
        for answer in [applied, flushed] {
            let frame = reply_to_a(&answer);
            let AppendOutcome::Accepted(ack) = decode_reply(&frame.body).expect("a reply") else {
                panic!("an ACK: {answer:?}");
            };
            acks.push(ack.progress);
            log.extend(route(
                &mut module,
                EventKind::Transport(TransportEvent::Delivered {
                    from: label(B),
                    frame,
                }),
            ));
        }
    }

    assert_eq!(
        acks,
        vec![
            progress(1, 1, 0),
            progress(1, 1, 1),
            progress(2, 2, 1),
            progress(2, 2, 2),
            progress(3, 3, 2),
            progress(3, 3, 3),
        ]
    );
    assert_eq!(sent, vec![1, 2, 3]);
    let escalated = |effect: &&EffectKind| {
        matches!(
            effect,
            EffectKind::Kernel(
                KernelEffect::SnapshotCatchupRequired { .. }
                    | KernelEffect::DivergenceDetected { .. }
                    | KernelEffect::Ignored {
                        reason: KernelIgnoredReason::AckRejected(AckRejectReason::Unverifiable)
                    }
            )
        )
    };
    assert_eq!(log.iter().find(escalated), None, "{log:?}");
    let caught: Vec<_> = log
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                EffectKind::Kernel(KernelEffect::CopyCaughtUp { .. })
            )
        })
        .collect();
    assert_eq!(
        caught,
        vec![&kernel(KernelEffect::CopyCaughtUp {
            copy: COPY_B,
            head: Seq(3),
            digest: records[2].record_digest,
        })]
    );
    assert_eq!(
        primary_side(&module)
            .tracker()
            .peer(COPY_B)
            .map(|peer| peer.progress),
        Some(progress(3, 3, 3))
    );
}

/// M7B-210 (lead ruling B-R67f; tester-kb-r1 regate-2 F2, probe `zz_k4`, mutant K4). A
/// restarted copy does not inherit its old incarnation's proved floor. B is proved durable at
/// the cutoff 3 by a `Recovered`; control then re-announces B at a new boot, and B, which holds
/// nothing now, is walked from the root. Its ACKs at 1 and 2 are new content, not repeats of a
/// floor the old boot proved: each drives the cursor to the next record, and the ACK at 3
/// verifies at the cutoff rung and catches B up.
#[retcd_test]
fn m7b_210_a_restarted_copy_does_not_inherit_the_proved_floor() {
    let mut module = b_walked_to_the_cutoff();
    route(&mut module, recovered_again(&[COPY_A, COPY_B, COPY_C]));
    let mut pin = pin_with_b();
    pin.config_version = ConfigVersion(NEW_CONFIG.0 + 1);
    pin.members[1].boot = BootId(22);
    route(&mut module, config_event(pin));
    let restarted = PeerLabel {
        node: B,
        boot: BootId(22),
        authenticated: true,
    };
    let at = |seq: u64| {
        let mut ack = in_new_root(b(seq, seq, seq));
        ack.boot = BootId(22);
        ack.config_version = ConfigVersion(NEW_CONFIG.0 + 1);
        reply_from(restarted, &AppendOutcome::Accepted(ack))
    };
    assert_eq!(
        route(
            &mut module,
            reply_from(restarted, &need_prefix_at(0, Digest::ROOT))
        ),
        vec![send(COPY_B, 1)]
    );
    for seq in 1..3 {
        assert_eq!(
            route(&mut module, at(seq)),
            vec![
                rejected(AckRejectReason::InFlightUnverified),
                send(COPY_B, seq + 1)
            ],
            "ACK {seq}"
        );
    }
    let last = route(&mut module, at(3));
    assert_eq!(last[0], peer_progress(B, 3), "{last:?}");
    assert_eq!(last.last(), Some(&caught_up(COPY_B, 3)), "{last:?}");
}

/// M7B-211 (lead ruling B-R67d; tester-kb-r1 regate-2 F3, probe `zz_d3`, mutant D3). A repeat
/// strictly below the mark answers `Recorded` even where the ladder would verify it. Below the
/// cutoff an in-flight ACK raises the cursor's mark to `(1, 1, 1)` and leaves the tracker at 0.
/// A reordered earlier ACK `(1, 0, 0)` at the genesis rung is below the mark, passes rule 8,
/// and the ladder matches it: it is still exactly `Recorded`, with no `PeerProgress` at 0, and
/// the primary side is unchanged. The same holds below the proved floor after a `Recovered`
/// (`(2, 0, 0)` at the genesis rung, floor 3, tracker 0). This is why routing a below-mark
/// repeat through the at-mark path is not equivalent.
#[retcd_test]
fn m7b_211_a_verifiable_ack_below_an_unverified_mark_is_recorded_and_reports_nothing() {
    let mut module = walking_b_from_root();
    route(&mut module, accepted(&in_new_root(b(1, 1, 1))));
    let mut early = in_new_root(b(1, 0, 0));
    early.digest_at_buffered = Digest::ROOT;
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, accepted(&early)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(primary_side(&module), &before);

    let mut module = b_walked_to_the_cutoff();
    route(&mut module, recovered_again(&[COPY_A, COPY_B, COPY_C]));
    let mut early = in_new_root(b(2, 0, 0));
    early.digest_at_buffered = Digest::ROOT;
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, accepted(&early)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(primary_side(&module), &before);
}

// --- B-R67h: an ACK sent while the next record is staged is not new content ------------------

/// M7B-212 (lead ruling B-R67h; tester-kb-r1 F1b, probe `zz_n1`). B's real receiver reports
/// `received` as the record it has staged, so an ACK it sends while the next record is staged
/// carries `received` one past applied: its flush `(2, 1, 1)`, the ACK `(2, 1, 0)` that follows
/// `AlreadyHave` for a delayed re-send of record 1, and, one record on, the flush `(3, 2, 2)`.
/// Below the cutoff the ladder cannot verify any of them. Each is exactly `Recorded`: no
/// snapshot, no send, the tracker unchanged, `unacked` unchanged, and the mark's `received` and
/// applied unchanged — only its `durable` takes the ACK's. So the applied ACK for the staged
/// record still moves the cursor on, with no re-send. Near-misses keep today's escalation:
/// `received` beyond the record in flight, a forked digest, and applied below the mark's.
#[retcd_test]
fn m7b_212_a_staged_ack_below_the_cutoff_is_recorded_and_raises_only_the_marks_durable() {
    for (shape, before, staged, durable) in [
        ("a flush while 2 is staged", None, b(2, 1, 1), 1),
        (
            "the ACK after AlreadyHave while 2 is staged",
            None,
            b(2, 1, 0),
            0,
        ),
        ("a flush while 3 is staged", Some(b(2, 2, 1)), b(3, 2, 2), 2),
    ] {
        let mut module = b_applied_one_below_the_cutoff();
        if let Some(ack) = before {
            route(&mut module, accepted(&in_new_root(ack)));
        }
        let tracker = primary_side(&module).tracker().clone();
        let cursor = primary_side(&module).cursor(COPY_B).expect("running");
        let (mark, digest) = cursor.mark().expect("a mark");
        let unacked = cursor.unacked();
        assert_eq!(
            route(&mut module, accepted(&in_new_root(staged))),
            vec![replica(ReplicaIgnoreReason::Recorded)],
            "{shape}"
        );
        assert_eq!(primary_side(&module).tracker(), &tracker, "{shape}");
        let cursor = primary_side(&module).cursor(COPY_B).expect("running");
        let raised = ReplicaProgress {
            durable: DurableSeq(durable),
            ..mark
        };
        assert_eq!(cursor.mark(), Some((raised, digest)), "{shape}");
        assert_eq!(cursor.unacked(), unacked, "{shape}");
    }

    // The walk goes on from the applied ACK for the staged record, with no re-send.
    let mut module = b_applied_one_below_the_cutoff();
    route(&mut module, accepted(&in_new_root(b(2, 1, 1))));
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(2, 2, 1)))),
        vec![
            rejected(AckRejectReason::InFlightUnverified),
            send(COPY_B, 3)
        ]
    );
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(3, 2, 2)))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    let last = route(&mut module, accepted(&in_new_root(b(3, 3, 2))));
    assert_eq!(last[0], peer_progress(B, 3), "{last:?}");
    assert_eq!(last.last(), Some(&caught_up(COPY_B, 3)), "{last:?}");

    let escalated = vec![
        rejected(AckRejectReason::Unverifiable),
        kernel(KernelEffect::SnapshotCatchupRequired {
            copy: COPY_B,
            barrier: Seq(3),
        }),
    ];
    // Near-miss: `received` 3 beyond record 2, the one in flight. B stages only what it was
    // sent, so this is not staging state, and it gets today's answer.
    let mut module = b_applied_one_below_the_cutoff();
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(3, 1, 1)))),
        escalated
    );
    // Near-miss: the staged shape with a digest that contradicts the mark's.
    let mut module = b_applied_one_below_the_cutoff();
    assert_eq!(
        route(&mut module, accepted(&forked(in_new_root(b(2, 1, 1))))),
        escalated
    );
    // Near-miss: applied one below the mark's, `received` above it.
    let mut module = b_applied_one_below_the_cutoff();
    route(&mut module, accepted(&in_new_root(b(2, 2, 1))));
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(3, 1, 1)))),
        escalated
    );
}

/// M7B-213 (lead ruling B-R67h; tester-kb-r1 F7, mutant T1). The mark's `durable` takes the
/// ACK's, and never falls. A partial flush — the mark at `(2, 2, 0)`, then B flushes through 1
/// only — leaves the mark at `(2, 2, 1)`, not at its applied sequence. And a reordered older
/// ACK does not lower it: after the flush `(3, 2, 2)` raised it to 2, the earlier ACK
/// `(3, 2, 1)`, sent after `AlreadyHave` before that flush, is `Recorded` and the mark keeps 2.
#[retcd_test]
fn m7b_213_the_marks_durable_takes_a_partial_flush_and_never_falls() {
    let mut module = b_applied_one_below_the_cutoff();
    route(&mut module, accepted(&in_new_root(b(2, 2, 0))));
    let digest = in_new_root(b(2, 2, 1)).digest_at_buffered;
    assert_eq!(
        route(&mut module, accepted(&in_new_root(b(2, 2, 1)))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    let mark = |module: &Replication| {
        primary_side(module)
            .cursor(COPY_B)
            .and_then(CatchupCursor::mark)
    };
    assert_eq!(mark(&module), Some((progress(2, 2, 1), digest)));

    let mut module = b_applied_one_below_the_cutoff();
    route(&mut module, accepted(&in_new_root(b(2, 2, 1))));
    for ack in [b(3, 2, 2), b(3, 2, 1)] {
        assert_eq!(
            route(&mut module, accepted(&in_new_root(ack))),
            vec![replica(ReplicaIgnoreReason::Recorded)],
            "{ack:?}"
        );
        assert_eq!(mark(&module), Some((progress(2, 2, 2), digest)), "{ack:?}");
    }
}

/// M7B-214 (lead ruling B-R67h, with B-R67f). With no cursor the known position is B's held
/// progress raised to its proved floor. After a `Recovered` whose barrier proves B at the cutoff
/// 3 drops B's cursor, the stale staged ACKs B sent on its way there — `(3, 2, 2)` and
/// `(2, 1, 1)` — are each exactly `Recorded`, and the primary side is unchanged. With no
/// cursor the bound on a staged `received` is one past the known applied sequence: on the golden
/// primary, whose ladder starts at 5, B holds 0, so `(1, 0, 0)` is `Recorded` and changes
/// nothing, and `(2, 0, 0)` is beyond the bound and gets today's answer.
#[retcd_test]
fn m7b_214_a_stale_staged_ack_after_a_recovered_is_recorded() {
    let mut module = b_walked_to_the_cutoff();
    route(&mut module, recovered_again(&[COPY_A, COPY_B, COPY_C]));
    assert!(primary_side(&module).cursor(COPY_B).is_none());
    for ack in [b(3, 2, 2), b(2, 1, 1)] {
        let before = primary_side(&module).clone();
        assert_eq!(
            route(&mut module, accepted(&in_new_root(ack))),
            vec![replica(ReplicaIgnoreReason::Recorded)],
            "{ack:?}"
        );
        assert_eq!(primary_side(&module), &before, "{ack:?}");
    }

    // With no cursor the bound is one past the known applied sequence. On the golden primary B
    // holds 0 and the ladder starts at 5, so no rung verifies an ACK at 0: `(1, 0, 0)` is
    // staging state at the known position and is `Recorded`, and `(2, 0, 0)`, two records
    // ahead, is not, and gets today's answer.
    let mut module = routed();
    assert!(primary_side(&module).cursor(COPY_B).is_none());
    let before = primary_side(&module).clone();
    assert_eq!(
        route(&mut module, accepted(&b(1, 0, 0))),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(primary_side(&module), &before);
    assert_eq!(
        route(&mut module, accepted(&b(2, 0, 0))),
        vec![
            rejected(AckRejectReason::Unverifiable),
            kernel(KernelEffect::SnapshotCatchupRequired {
                copy: COPY_B,
                barrier: Seq(HEAD),
            }),
        ]
    );
}

/// B's real `AppendReceiver`, holding nothing, beside a primary built at `cutoff` whose cutoff
/// rung is the one B computes (M7B-209's fixture): the primary, B, the new root's records 1 to
/// `cutoff`, and the lineage A sends them in.
fn real_b_at_the_root(
    cutoff: u64,
) -> (
    Replication,
    AppendReceiver,
    Vec<ReplicationEnvelope>,
    Lineage,
) {
    let mut records: Vec<ReplicationEnvelope> = Vec::new();
    for seq in 1..=cutoff {
        let prev = records.last().map_or(Digest::ROOT, |env| env.record_digest);
        records.push(new_root_record(seq, prev));
    }
    let top = records.last().expect("records").record_digest;
    let result = requiring(recovery(cutoff, top, pin_with_b()), &[COPY_A, COPY_C]);
    let mut module = Replication::new();
    route(&mut module, recovered_event(&result));
    let receiver = AppendReceiver::new(ReceiverInit {
        config: pin_with_b(),
        own: COPY_B,
        lineage: result.selected.root,
        head: Head {
            seq: Seq::ZERO,
            digest: Digest::ROOT,
        },
        durable: DurableSeq(0),
    })
    .expect("B's receiver");
    (module, receiver, records, result.selected.root)
}

/// Record `seq` of `records` as A's append frame to B.
fn append_frame(records: &[ReplicationEnvelope], sender: Lineage, seq: u64) -> Frame {
    Frame {
        id: MessageId(u32::try_from(seq).expect("seq")),
        protocol: ENVELOPE_VERSION,
        config: NEW_CONFIG,
        sender,
        body: records[usize::try_from(seq - 1).expect("seq")]
            .encode()
            .expect("encode"),
    }
}

/// B stages `frame`: the commit it asks storage for.
fn stage(receiver: &mut AppendReceiver, frame: &Frame) -> BatchId {
    let staged = receiver.on_append(&label(A), frame);
    let [EffectKind::Store(StoreEffect::Commit(batch))] = staged.as_slice() else {
        panic!("B stages the record: {staged:?}");
    };
    batch.id
}

/// B's durable prefix through `seq` in the new root.
fn flushed_through(seq: u64) -> [DurablePrefix; 1] {
    [DurablePrefix {
        partition: P,
        generation: NEW_GEN,
        through: DurableSeq(seq),
    }]
}

/// Route every reply in B's `effects` to A, in order. Each ACK's progress goes to `acks` with
/// the primary's answer to it; every effect goes to `log`.
fn to_a(
    module: &mut Replication,
    effects: &[EffectKind],
    acks: &mut Vec<(ReplicaProgress, Vec<EffectKind>)>,
    log: &mut Vec<EffectKind>,
) {
    for effect in effects {
        let EffectKind::Send(SendEffect::Unicast { to, frame }) = effect else {
            panic!("B only replies: {effect:?}");
        };
        assert_eq!(*to, A);
        let answer = route(
            module,
            EventKind::Transport(TransportEvent::Delivered {
                from: label(B),
                frame: frame.clone(),
            }),
        );
        if let Ok(AppendOutcome::Accepted(ack)) = decode_reply(&frame.body) {
            acks.push((ack.progress, answer.clone()));
        }
        log.extend(answer);
    }
}

/// A walk below the cutoff ended well: nothing escalated, B caught up exactly once, at the
/// cutoff, with the cutoff's digest.
fn walked_without_escalation(log: &[EffectKind], cutoff: u64, digest: Digest) {
    let escalated = |effect: &&EffectKind| {
        matches!(
            effect,
            EffectKind::Kernel(
                KernelEffect::SnapshotCatchupRequired { .. }
                    | KernelEffect::DivergenceDetected { .. }
                    | KernelEffect::Ignored {
                        reason: KernelIgnoredReason::AckRejected(AckRejectReason::Unverifiable)
                    }
            )
        )
    };
    assert_eq!(log.iter().find(escalated), None, "{log:?}");
    let caught: Vec<_> = log
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                EffectKind::Kernel(KernelEffect::CopyCaughtUp { .. })
            )
        })
        .collect();
    assert_eq!(
        caught,
        vec![&kernel(KernelEffect::CopyCaughtUp {
            copy: COPY_B,
            head: Seq(cutoff),
            digest,
        })]
    );
}

/// M7B-215 (lead ruling B-R67h; tester-kb-r1 F1b, probe `zz_rwalk` profile 0). A walk of B's
/// real receiver in which every flush lands while the next record is staged. A primary built at
/// cutoff 4 walks B from the root. For each record B stages, the flush of the record before
/// completes first, then the staged record commits. So B's flush ACKs carry `received` one past
/// applied: `(2, 1, 1)`, `(3, 2, 2)`, `(4, 3, 3)`. Each of those is exactly `Recorded`; each record
/// is sent once; nothing escalates; B is caught up once, at the cutoff, and ends durable there.
#[retcd_test]
fn m7b_215_a_walk_whose_flushes_land_while_the_next_record_is_staged() {
    let (mut module, mut receiver, records, sender) = real_b_at_the_root(4);
    let mut log = route(&mut module, reply(B, &need_prefix_at(0, Digest::ROOT)));
    let (mut acks, mut unflushed, mut next) = (Vec::new(), None, 0);
    while let Some(effect) = log.get(next).cloned() {
        next += 1;
        let &[seq] = sends_to(&[effect], COPY_B).as_slice() else {
            continue;
        };
        let batch = stage(&mut receiver, &append_frame(&records, sender, seq));
        if let Some(through) = unflushed.take() {
            let flushed = receiver
                .on_flushed(&flushed_through(through))
                .expect("B's prefix");
            to_a(&mut module, &flushed, &mut acks, &mut log);
        }
        let applied = receiver.on_committed(batch).expect("B staged it");
        to_a(&mut module, &applied, &mut acks, &mut log);
        unflushed = Some(seq);
    }
    let flushed = receiver
        .on_flushed(&flushed_through(4))
        .expect("B's prefix");
    to_a(&mut module, &flushed, &mut acks, &mut log);

    let reported: Vec<_> = acks.iter().map(|(ack, _)| *ack).collect();
    assert_eq!(
        reported,
        vec![
            progress(1, 1, 0),
            progress(2, 1, 1),
            progress(2, 2, 1),
            progress(3, 2, 2),
            progress(3, 3, 2),
            progress(4, 3, 3),
            progress(4, 4, 3),
            progress(4, 4, 4),
        ]
    );
    for (ack, answer) in &acks {
        if ack.received.0 > ack.buffered_applied.0 {
            assert_eq!(
                answer,
                &vec![replica(ReplicaIgnoreReason::Recorded)],
                "{ack:?}"
            );
        }
    }
    assert_eq!(sends_to(&log, COPY_B), vec![1, 2, 3, 4]);
    walked_without_escalation(&log, 4, records[3].record_digest);
    assert_eq!(
        primary_side(&module)
            .tracker()
            .peer(COPY_B)
            .map(|peer| peer.progress),
        Some(progress(4, 4, 4))
    );
}

/// M7B-216 (lead ruling B-R67h; tester-kb-r1 F1b, probe `zz_rwalk` profiles 2 and 3). A walk of
/// B's real receiver in which a delayed re-send of each record lands while the next one is
/// staged. A primary built at cutoff 4 walks B from the root; B flushes right after each commit.
/// B's two ACKs for each record are slow, so the retransmit timer re-sends it, and that copy is
/// slower still: the ACKs move the cursor on, B stages the next record, and only then does the
/// re-send arrive. B answers it `AlreadyHave` and its current ACK, `(s + 1, s, s)`. Each of those
/// is exactly `Recorded`; nothing escalates; B is caught up once, at the cutoff; and each record
/// is sent twice — once, and once re-sent — except the last, which is ACKed at once.
#[retcd_test]
fn m7b_216_a_walk_whose_delayed_re_sends_land_while_the_next_record_is_staged() {
    let (mut module, mut receiver, records, sender) = real_b_at_the_root(4);
    let mut log = route(&mut module, reply(B, &need_prefix_at(0, Digest::ROOT)));
    let mut armed = last_arm(&log, retransmit_timer(P));
    let mut acks = Vec::new();
    let mut batch = stage(&mut receiver, &append_frame(&records, sender, 1));
    for seq in 1..=4 {
        let mut replies = receiver.on_committed(batch).expect("B staged it");
        replies.extend(
            receiver
                .on_flushed(&flushed_through(seq))
                .expect("B's prefix"),
        );
        if seq == 4 {
            to_a(&mut module, &replies, &mut acks, &mut log);
            break;
        }
        // B's ACKs are slow: the retransmit timer re-sends the record, and holds it.
        let mut resent = Vec::new();
        for _ in 0..3 {
            let fired = route(
                &mut module,
                fired_at(retransmit_timer(P), armed.expect("armed")),
            );
            armed = last_arm(&fired, retransmit_timer(P)).or(armed);
            resent = sends_to(&fired, COPY_B);
            log.extend(fired);
            if !resent.is_empty() {
                break;
            }
        }
        assert_eq!(resent, vec![seq], "the re-send of {seq}");
        let before = log.len();
        to_a(&mut module, &replies, &mut acks, &mut log);
        armed = last_arm(&log[before..], retransmit_timer(P)).or(armed);
        assert_eq!(sends_to(&log[before..], COPY_B), vec![seq + 1]);
        batch = stage(&mut receiver, &append_frame(&records, sender, seq + 1));
        let late = receiver.on_append(&label(A), &append_frame(&records, sender, seq));
        let before = log.len();
        to_a(&mut module, &late, &mut acks, &mut log);
        armed = last_arm(&log[before..], retransmit_timer(P)).or(armed);
    }

    let staged: Vec<_> = acks
        .iter()
        .filter(|(ack, _)| ack.received.0 > ack.buffered_applied.0)
        .collect();
    assert_eq!(
        staged.iter().map(|(ack, _)| *ack).collect::<Vec<_>>(),
        vec![progress(2, 1, 1), progress(3, 2, 2), progress(4, 3, 3)]
    );
    for (ack, answer) in staged {
        assert_eq!(
            answer,
            &vec![replica(ReplicaIgnoreReason::Recorded)],
            "{ack:?}"
        );
    }
    assert_eq!(sends_to(&log, COPY_B), vec![1, 1, 2, 2, 3, 3, 4]);
    walked_without_escalation(&log, 4, records[3].record_digest);
    assert_eq!(
        primary_side(&module)
            .tracker()
            .peer(COPY_B)
            .map(|peer| peer.progress),
        Some(progress(4, 4, 4))
    );
}

// --- the stream and qualification at the head (Gautam 2026-09-27, L-R177gd; lead rulings
// B-R47b and B-R67i) ------------------------------------------------------------------------
//
// Spec §5.2 step 4: each record the primary applies goes to both regular secondaries. The
// stream re-sends a record no ACK has reached on the B-R67 retransmit timer (B-R67i), and the
// tracker reports `Gained` at the head as well as at the anchor (B-R47b), so P1 hears that the
// candidate qualifies. One transaction is in flight at a time (spec §5.2), so the head is the
// candidate. Every routed row starts from B and C at the head 12, anchor qualified.

/// `QualificationChanged{Gained}` at `at`, the head, carried by an ACK.
fn gained_at(at: u64, copies: &[CopyId]) -> EffectKind {
    edge(
        lineage(),
        CONFIG,
        at,
        QualificationDirection::Gained,
        copies,
        QualificationCause::AckAdvanced,
    )
}

/// The edge inside `effect`, which must be one.
fn edge_in(effect: &EffectKind) -> QualificationChanged {
    match effect {
        EffectKind::Kernel(KernelEffect::QualificationChanged(q)) => q.clone(),
        other => panic!("expected a qualification edge, got {other:?}"),
    }
}

/// The view P1 holds: the one its candidates' authority was decided under.
fn p1_view() -> AuthorityView {
    AuthorityView {
        lineage: lineage(),
        grant_id: GrantId(1),
        boot_id: BootId(1),
        authority_generation: AuthorityGeneration(1),
        config_version: CONFIG,
        authority_seq: 1,
        valid_through_tick: Tick(u64::MAX),
        past_horizon: DenyReason::NoGrant,
    }
}

/// P1 with everything below `seq` published, A1's view held, and the candidate at `seq`
/// pending: what T1 leaves after `LocalApplied(seq)` (one transaction in flight).
fn p1_waiting_on(seq: u64) -> PubKernel {
    let mut p1 = PubKernel::new(PubConfig::default(), BootId(1), lineage(), Seq(seq - 1));
    assert!(p1
        .apply(T, PubEvent::AuthorityView(p1_view()), None)
        .is_empty());
    p1.apply(T, PubEvent::Candidate(p1_candidate(seq)), None);
    p1
}

/// Hand P1 R1's edge, and return the correlation of the `Publication` check it asks.
fn p1_asks(p1: &mut PubKernel, effect: &EffectKind) -> CorrelationId {
    match p1
        .apply(T, PubEvent::QualificationChanged(edge_in(effect)), None)
        .as_slice()
    {
        [PubEffect::AuthorityCheck {
            checkpoint: Checkpoint::Publication,
            correlation,
            ..
        }] => *correlation,
        other => panic!("expected one Publication check, got {other:?}"),
    }
}

/// A1 admits P1's `Publication` check for the candidate at `seq`; P1 reads R1's live tracker.
fn p1_admitted(
    p1: &mut PubKernel,
    seq: u64,
    correlation: CorrelationId,
    tracker: &ProgressTracker,
) -> Vec<PubEffect> {
    let answer = AuthorityDecision {
        checkpoint: Checkpoint::Publication,
        correlation,
        ..p1_candidate(seq).authority
    };
    p1.apply(T, PubEvent::AuthorityAnswer(answer), Some(tracker))
}

/// Whether P1 published `seq`: it told T1.
fn p1_published(effects: &[PubEffect], seq: u64) -> bool {
    effects
        .iter()
        .any(|effect| matches!(effect, PubEffect::NotifyTxn { seq: s, .. } if *s == Seq(seq)))
}

/// R1's `Gained` at the head reaches P1, A1 admits, and P1 publishes `seq`.
fn publishes(module: &Replication, effect: &EffectKind, seq: u64) {
    let mut p1 = p1_waiting_on(seq);
    let correlation = p1_asks(&mut p1, effect);
    let effects = p1_admitted(&mut p1, seq, correlation, primary_side(module).tracker());
    assert!(p1_published(&effects, seq), "{effects:?}");
}

fn busy_at(seq: u64) -> AppendOutcome {
    AppendOutcome::Busy {
        accepted_through: Seq(seq),
    }
}

/// The retransmit timer's version `version` fires on A.
fn retransmit_fires(module: &mut Replication, version: u64) -> Vec<EffectKind> {
    route(module, retransmit_fired(version))
}

/// [`both_routed`] after `LocalApplied(13)`: 13 is shipped to B and C, and the retransmit
/// timer is armed at version 1.
fn shipped_13() -> Replication {
    let mut module = both_routed();
    assert_eq!(
        route(&mut module, local_applied_event(HEAD + 1)),
        vec![
            send(COPY_B, HEAD + 1),
            send(COPY_C, HEAD + 1),
            retransmit_arm(1)
        ]
    );
    module
}

/// Two fires of the retransmit timer from version `first`: the first only marks the wait, the
/// second re-sends. Returns the second fire's effects.
fn two_fires(module: &mut Replication, first: u64) -> Vec<EffectKind> {
    assert_eq!(
        retransmit_fires(module, first),
        vec![retransmit_arm(first + 1)],
        "the first fire only marks the wait"
    );
    retransmit_fires(module, first + 1)
}

/// Spec §5.2 step 4, Gautam 2026-09-27 (L-R177gd): the step that records `LocalApplied`
/// ships the record to every regular secondary, and to no one else — not the primary itself,
/// not the shadow D (spec: "shadows receive separately"), not a diverged copy — and arms the
/// retransmit (B-R67i). A gap or a regress stores nothing and ships nothing. Kills mutant
/// "no ship".
#[retcd_test]
fn m7b_217_local_applied_ships_the_record_to_every_regular_secondary() {
    let mut module = shipped_13();
    assert_eq!(
        route(&mut module, local_applied_event(HEAD + 3)),
        vec![replica(ReplicaIgnoreReason::OutOfOrder)]
    );
    assert_eq!(
        route(&mut module, local_applied_event(HEAD + 1)),
        vec![replica(ReplicaIgnoreReason::OutOfOrder)]
    );

    let mut module = both_routed();
    route(
        &mut module,
        EventKind::Kernel(KernelEvent::DivergenceDetected { copy: COPY_C }),
    );
    assert_eq!(
        route(&mut module, local_applied_event(HEAD + 1)),
        vec![send(COPY_B, HEAD + 1), retransmit_arm(1)]
    );
}

/// B-R67c A2 carried to the stream: a copy whose cursor has a record in flight is not shipped
/// the new head, and the retransmit re-sends the cursor's record to it, never the stream's. C,
/// with no cursor, gets both. Kills mutant "ship to a copy a cursor is driving".
#[retcd_test]
fn m7b_218_the_stream_skips_a_copy_whose_cursor_has_a_record_in_flight() {
    let mut module = catching_up_b();
    route(&mut module, accepted(&c(HEAD, HEAD, HEAD)));
    assert_eq!(
        route(&mut module, local_applied_event(HEAD + 1)),
        vec![send(COPY_C, HEAD + 1)],
        "the timer is already armed for B's record"
    );
    let resent = two_fires(&mut module, 1);
    assert_eq!(sends_to(&resent, COPY_B), vec![11], "the cursor's record");
    assert_eq!(sends_to(&resent, COPY_C), vec![HEAD + 1], "the stream's");
}

/// B-R67i, red-first: 13 is shipped and both sends are lost. The retransmit re-sends 13 to both
/// after one quiet interval, B's ACK gains the predicate at the head (B-R47b), and P1 publishes
/// 13. Without the re-send nothing else would send 13, because no next record comes while 13 is
/// unpublished, and the write would end `UnknownOutcome`. Kills mutant "no re-send".
#[retcd_test]
fn m7b_219_a_ship_lost_to_both_copies_is_re_sent_and_publishes() {
    let mut module = shipped_13();
    let resent = two_fires(&mut module, 1);
    assert_eq!(
        resent,
        vec![
            send(COPY_B, HEAD + 1),
            send(COPY_C, HEAD + 1),
            retransmit_arm(3)
        ]
    );
    let effects = route(&mut module, accepted(&b(HEAD + 1, HEAD + 1, HEAD)));
    assert_eq!(
        effects,
        vec![peer_progress(B, HEAD + 1), gained_at(HEAD + 1, &[COPY_B])]
    );
    publishes(&module, &effects[1], HEAD + 1);
}

/// B-R67i, red-first: B applies 13 and its ACK is lost; C says nothing. The re-send reaches B,
/// which answers `AlreadyHave` (no cursor: `NothingOutstanding`, B-R48a F1) and its ACK again,
/// and that ACK publishes 13.
///
/// Update (tester-kbr1 F2): the shipped record clears only on an admitted ACK with
/// applied ≥ seq. B's staged ACK first (received 13, applied 12) is admitted and reports
/// progress, yet keeps B's record, so the next fire re-sends 13 to B. Kills mutant K5 "clear on
/// `received`", under which the staged ACK clears it, and a lost applied ACK after it would
/// leave nothing to re-send.
#[retcd_test]
fn m7b_220_a_lost_qualifying_ack_is_asked_again_and_publishes() {
    let mut module = shipped_13();
    assert_eq!(sends_to(&two_fires(&mut module, 1), COPY_B), vec![HEAD + 1]);
    assert_eq!(
        route(&mut module, reply(B, &AppendOutcome::AlreadyHave)),
        vec![replica(ReplicaIgnoreReason::NothingOutstanding)]
    );
    let staged = shipped_to_b_after_staged_ack(&mut module);
    assert_eq!(
        staged,
        Some(Shipped {
            seq: Seq(HEAD + 1),
            waited: true
        }),
        "a staged ACK (applied < seq) keeps the shipped record"
    );
    assert_eq!(
        sends_to(&retransmit_fires(&mut module, 3), COPY_B),
        vec![HEAD + 1],
        "so the next fire re-sends it"
    );
    let effects = route(&mut module, accepted(&b(HEAD + 1, HEAD + 1, HEAD)));
    assert_eq!(
        effects,
        vec![peer_progress(B, HEAD + 1), gained_at(HEAD + 1, &[COPY_B])]
    );
    assert_eq!(
        primary_side(&module).shipped(COPY_B),
        None,
        "applied ≥ seq clears it"
    );
    publishes(&module, &effects[1], HEAD + 1);
}

/// B's staged ACK for 13 (received 13, applied 12): admitted, it reports progress at 12 and
/// nothing else. Returns what the stream still holds for B afterwards.
fn shipped_to_b_after_staged_ack(module: &mut Replication) -> Option<Shipped> {
    assert_eq!(
        route(module, accepted(&b(HEAD + 1, HEAD, HEAD))),
        vec![peer_progress(B, HEAD)]
    );
    primary_side(module).shipped(COPY_B)
}

/// B-R67i, red-first (lead's item 4): both copies answer `Busy` to the one shipped record.
/// `Busy` starts no cursor (B-R48a F1) and nothing is paused, so only the stream's re-send can
/// reach them. It does, two intervals (200 ms) after the ship, well inside P1's 2000 ms
/// deadline, and the write publishes.
#[retcd_test]
fn m7b_221_both_copies_busy_on_the_shipped_record_still_publish() {
    let mut module = shipped_13();
    for node in [B, C] {
        assert_eq!(
            route(&mut module, reply(node, &busy_at(HEAD))),
            vec![replica(ReplicaIgnoreReason::NothingOutstanding)]
        );
    }
    assert!(primary_side(&module).cursor(COPY_B).is_none());
    assert!(primary_side(&module).cursor(COPY_C).is_none());
    let resent = two_fires(&mut module, 1);
    assert_eq!(
        (sends_to(&resent, COPY_B), sends_to(&resent, COPY_C)),
        (vec![HEAD + 1], vec![HEAD + 1])
    );
    let effects = route(&mut module, accepted(&c(HEAD + 1, HEAD + 1, HEAD)));
    assert_eq!(
        effects,
        vec![peer_progress(C, HEAD + 1), gained_at(HEAD + 1, &[COPY_C])]
    );
    publishes(&module, &effects[1], HEAD + 1);
}

/// `min_regular_acks` 1: C answers `Busy`, and B's ACK publishes 13 at once. C is still re-sent
/// 13 and B is not, because B's admitted ACK reached it. C's ACK then reports progress and no
/// second `Gained`: the head stayed qualified (B-R47b, no repeat). With both answered, the next
/// fire finds nothing and the timer stays down. Kills mutants "not cleared on ACK" and "Gained
/// repeated for the same seq" (routed).
#[retcd_test]
fn m7b_222_one_copy_busy_while_the_other_acks_publishes_and_the_busy_copy_is_re_sent() {
    let mut module = shipped_13();
    assert_eq!(
        route(&mut module, reply(C, &busy_at(HEAD))),
        vec![replica(ReplicaIgnoreReason::NothingOutstanding)]
    );
    let effects = route(&mut module, accepted(&b(HEAD + 1, HEAD + 1, HEAD)));
    assert_eq!(
        effects,
        vec![peer_progress(B, HEAD + 1), gained_at(HEAD + 1, &[COPY_B])]
    );
    publishes(&module, &effects[1], HEAD + 1);

    let resent = two_fires(&mut module, 1);
    assert_eq!(resent, vec![send(COPY_C, HEAD + 1), retransmit_arm(3)]);
    assert_eq!(
        route(&mut module, accepted(&c(HEAD + 1, HEAD + 1, HEAD))),
        vec![peer_progress(C, HEAD + 1)]
    );
    assert_eq!(
        retransmit_fires(&mut module, 3),
        vec![replica(ReplicaIgnoreReason::NotRequired)]
    );
}

/// B-R47b on the tracker alone: `Gained` at the head is edge-triggered. It fires on the step
/// that makes the head qualify, never again while it stays qualified, never at a head equal to
/// the anchor (the anchor's own edge says it), and beside the anchor's when one ACK flips both.
/// A local write that leaves the head unqualified reports nothing: there is no `Lost` at the
/// head. Kills mutants "no Gained at the head" and "Gained repeated for the same seq".
#[retcd_test]
fn m7b_223_qualification_is_gained_at_the_head_once_per_edge() {
    let mut one = tracker();
    assert_eq!(
        deliver(&mut one, &b(HEAD, HEAD, HEAD)),
        vec![peer_progress(B, HEAD), gained(&[COPY_B])],
        "at the anchor, one edge"
    );
    assert_eq!(
        local_applied(&mut one, HEAD + 1),
        vec![replica(ReplicaIgnoreReason::Recorded)],
        "no Lost at the head"
    );
    assert_eq!(
        deliver(&mut one, &b(HEAD + 1, HEAD + 1, HEAD)),
        vec![peer_progress(B, HEAD + 1), gained_at(HEAD + 1, &[COPY_B])]
    );
    // C's ACKs move the durable view, and no edge: the head stayed qualified.
    for (ack, why) in [
        (c(HEAD + 1, HEAD + 1, HEAD), "still qualified: no repeat"),
        (c(HEAD + 1, HEAD + 1, HEAD + 1), "a flush ACK: no repeat"),
    ] {
        let effects = deliver(&mut one, &ack);
        assert_eq!(effects[0], peer_progress(C, HEAD + 1), "{why}");
        assert!(
            !effects.iter().any(|effect| matches!(
                effect,
                EffectKind::Kernel(KernelEffect::QualificationChanged(_))
            )),
            "{why}: {effects:?}"
        );
    }

    let mut fresh = tracker();
    local_applied(&mut fresh, HEAD + 1);
    assert_eq!(
        deliver(&mut fresh, &b(HEAD + 1, HEAD + 1, HEAD)),
        vec![
            peer_progress(B, HEAD + 1),
            gained(&[COPY_B]),
            gained_at(HEAD + 1, &[COPY_B])
        ],
        "one ACK flips the anchor and the head"
    );
}

/// B, diverged mid-recheck: the module, and P1 after its outstanding recheck was admitted and
/// answered `PublishPredicateFalse`.
fn rechecking_13_after_b_diverged() -> (Replication, PubKernel) {
    let mut module = shipped_13();
    let effects = route(&mut module, accepted(&b(HEAD + 1, HEAD + 1, HEAD)));
    assert_eq!(effects[1], gained_at(HEAD + 1, &[COPY_B]));
    let mut p1 = p1_waiting_on(HEAD + 1);
    let correlation = p1_asks(&mut p1, &effects[1]);
    assert_eq!(
        route(
            &mut module,
            EventKind::Kernel(KernelEvent::DivergenceDetected { copy: COPY_B })
        ),
        vec![alert(), copy_lost(COPY_B)],
        "C holds the anchor: no edge, and no Lost at the head"
    );
    let answered = p1_admitted(
        &mut p1,
        HEAD + 1,
        correlation,
        primary_side(&module).tracker(),
    );
    assert_eq!(p1_kinds(&answered), ["Fact(PublishPredicateFalse)"]);
    assert!(!p1_published(&answered, HEAD + 1));
    (module, p1)
}

/// B-R47b: no `Lost` is reported at the head, and none is needed. B's ACK gained 13 and P1 asked
/// A1; B then diverges. C still holds the anchor, so no edge moves and R1 reports only the
/// divergence. A1 admits P1's recheck, and P1 re-reads `qualifies_now(13)` live (kernel-b §3.5,
/// `may_publish`): it is false, so P1 answers `PublishPredicateFalse` and publishes nothing.
#[retcd_test]
fn m7b_224_a_head_loss_is_never_reported_and_still_cannot_publish() {
    let (module, _) = rechecking_13_after_b_diverged();
    assert!(!primary_side(&module).tracker().qualifies_now(Seq(HEAD + 1)));
    assert!(primary_side(&module).tracker().qualifies_now(Seq(HEAD)));
}

/// B-R47b: the edge is stateless, so `Gained` fires again when the head qualifies again. After
/// [`rechecking_13_after_b_diverged`], C's ACK for 13 makes the head qualify once more; R1
/// reports a second `Gained` at 13, P1 asks A1 again, and publishes. A remembered "already
/// gained 13" would leave P1 waiting for its deadline.
#[retcd_test]
fn m7b_225_gained_fires_again_after_the_head_lost_and_regained_and_publishes() {
    let (mut module, mut p1) = rechecking_13_after_b_diverged();
    let effects = route(&mut module, accepted(&c(HEAD + 1, HEAD + 1, HEAD)));
    assert_eq!(
        effects,
        vec![peer_progress(C, HEAD + 1), gained_at(HEAD + 1, &[COPY_C])]
    );
    let correlation = p1_asks(&mut p1, &effects[1]);
    let answered = p1_admitted(
        &mut p1,
        HEAD + 1,
        correlation,
        primary_side(&module).tracker(),
    );
    assert!(p1_published(&answered, HEAD + 1), "{answered:?}");
}

/// B-R67i: a shipped record is forgotten, for C, when a cursor takes C (the cursor then owns
/// what C is sent: one send of 13, not two), on `Recovered`, when control re-announces C at a
/// new boot, when C diverges, and when C leaves every active predicate. B, untouched, is still
/// re-sent in each case but `Recovered`, which forgets both. Kills mutants "re-send while a
/// cursor owns the copy" and "not cleared on `Recovered`", and each other clearing site.
#[retcd_test]
fn m7b_226_a_shipped_record_is_forgotten_when_a_cursor_takes_the_copy_or_the_copy_leaves() {
    // A cursor takes C.
    let mut module = shipped_13();
    assert_eq!(
        route(&mut module, reply(C, &need_prefix(HEAD))),
        vec![send(COPY_C, HEAD + 1)]
    );
    let resent = two_fires(&mut module, 1);
    assert_eq!(
        sends_to(&resent, COPY_C),
        vec![HEAD + 1],
        "once, by the cursor"
    );
    assert_eq!(sends_to(&resent, COPY_B), vec![HEAD + 1]);

    // `Recovered` forgets every copy's.
    let mut module = shipped_13();
    route(
        &mut module,
        recovered_event(&recovery(10, d(10), pin_with_b())),
    );
    assert_eq!(
        retransmit_fires(&mut module, 1),
        vec![replica(ReplicaIgnoreReason::NotRequired)]
    );

    // C at a new boot, C diverged, and C retired: B alone is re-sent.
    let restarted = config_with(
        NEW_CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_B, B, RegularSecondary),
            Member {
                boot: BootId(99),
                ..member(COPY_C, C, RegularSecondary)
            },
            member(COPY_D, D, Shadow),
        ],
    );
    let without_c = config_with(
        NEW_CONFIG,
        vec![
            member(COPY_A, A, Primary),
            member(COPY_B, B, RegularSecondary),
            member(COPY_D, D, Shadow),
        ],
    );
    let cases: [(&str, Vec<EventKind>); 3] = [
        (
            "new boot",
            vec![EventKind::Kernel(KernelEvent::ConfigChanged(restarted))],
        ),
        (
            "diverged",
            vec![EventKind::Kernel(KernelEvent::DivergenceDetected {
                copy: COPY_C,
            })],
        ),
        (
            "retired",
            vec![
                EventKind::Kernel(KernelEvent::ConfigChanged(without_c)),
                EventKind::Kernel(KernelEvent::TransitionBarrierConfirmed {
                    config_version: CONFIG,
                    through_seq: Seq(HEAD + 1),
                }),
            ],
        ),
    ];
    for (case, events) in cases {
        let mut module = shipped_13();
        for event in events {
            route(&mut module, event);
        }
        let resent = two_fires(&mut module, 1);
        assert_eq!(
            resent,
            vec![send(COPY_B, HEAD + 1), retransmit_arm(3)],
            "{case}"
        );
    }
}

// --- M9 S0 D3: F1's re-emit of the generation already served ------------------------------

/// The primary rebuilt into `NEW_GEN` at `HEAD + 1`, then one local write: head `HEAD + 2`.
/// Returns it and the recovery it was rebuilt from.
fn written_past_the_cutoff() -> (ProgressTracker, RecoveryResult) {
    let mut tracker = tracker();
    local_applied(&mut tracker, HEAD + 1);
    let result = recovery(HEAD + 1, d(HEAD + 1), pin_without_b());
    tracker.on_recovered(&result, T);
    local_applied(&mut tracker, HEAD + 2);
    assert_eq!(
        (tracker.lineage().generation, tracker.head()),
        (NEW_GEN, Seq(HEAD + 2))
    );
    (tracker, result)
}

/// M9 S0 D3 (lead ruling "S0 D3" rule 3). F1 re-emits the result this primary was rebuilt from,
/// once its rebuild finishes. The primary wrote `HEAD + 2` in this generation since: the rebuild
/// keeps it, so R1's head still agrees with T1's next sequence and the keepalive has a record to
/// send. The base stays at the cutoff; the anchor is the seed head, as for every rebuild
/// (B-R47a), so a copy re-proves through `HEAD + 2` to qualify. Before the fix the head fell back
/// to the cutoff and the primary sent nothing again (`s0-probe.md` D3).
#[retcd_test]
fn m9_d3_07_tracker_a_re_emit_of_its_generation_keeps_its_own_head() {
    let (mut tracker, result) = written_past_the_cutoff();
    tracker.on_recovered(&result, T);
    assert_eq!(tracker.head(), Seq(HEAD + 2));
    assert_eq!(tracker.history().highest(), Some(Seq(HEAD + 2)));
    assert_eq!(
        tracker.peer(COPY_A).expect("A").progress,
        progress(HEAD + 2, HEAD + 2, HEAD)
    );
    assert_eq!(
        (tracker.base_seq(), tracker.anchor(), tracker.retired()),
        (Seq(HEAD + 1), Seq(HEAD + 2), false)
    );
}

/// M9 S0 D3, rule 3's retired guard. A pin in the same generation that names this node a
/// secondary retires the primary; a re-emit that pins it primary again rebuilds it at the
/// cutoff, as before the fix, and unretires it.
#[retcd_test]
fn m9_d3_08_tracker_a_retired_primary_is_rebuilt_at_the_cutoff_on_a_re_emit() {
    let (mut tracker, result) = written_past_the_cutoff();
    let elsewhere = config_with(
        NEW_CONFIG,
        vec![
            member(COPY_A, A, RegularSecondary),
            member(COPY_C, C, Primary),
            member(COPY_D, D, RegularSecondary),
        ],
    );
    let mut moved = result.clone();
    moved.committed.pinned_config = elsewhere;
    tracker.on_recovered(&moved, T);
    assert!(tracker.retired());
    tracker.on_recovered(&result, T);
    assert_eq!((tracker.head(), tracker.retired()), (Seq(HEAD + 1), false));
}
