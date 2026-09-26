//! R1 `ProgressTracker`: the design §3.4 ACK ladder, the divergence vector, the control events,
//! and the §3.5 views they move.
//!
//! Rows named `m7b_NN_*` are plan rows (`docs/testing/test-plan-m7-kernel-b.md` §4–§5); the
//! rest are developer scaffolding and tester rows, written against `design.md` §3.4–§3.5. The
//! tracker is driven directly, except in the routing section, which routes the same inputs
//! through `Replication::step` (lead ruling B-R48).
//!
//! Fixture: A primary (copy 0, node 1), B and C regular (copies 1, 2), D a shadow (copy 3).
//! `min_regular_acks` 1. Every copy's boot is its node number. The primary has applied 12 and
//! synced 12, and vouches for 5..=12. B and C have proved nothing yet.

use config_log::retcd_test;

use bytes::Bytes;
use rdb_core::contracts::authority::{
    AuthorityView, BlockReason, DenyReason, FencingProof, Lineage, PartitionMode, Revocation,
};
use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::envelope::{AppendAck, AppendOutcome, AppendReject, ReplicaProgress};
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, ModuleName,
    StepCtx,
};
use rdb_core::contracts::ids::{
    AppliedSeq, AuthorityGeneration, BootId, ConfigVersion, CorrelationId, DurableSeq, EventId,
    FlushTicket, Generation, GrantId, MessageId, NodeId, OwnerEpoch, PartitionId, ReceivedSeq,
    ReplicaRole, Revision, Seq, SnapshotHandle,
};
use rdb_core::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::qualification::{
    QualificationCause, QualificationChanged, QualificationDirection,
};
use rdb_core::contracts::recovery::{
    CommittedRoot, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap, SelectedLineage,
};
use rdb_core::contracts::storage::{DurablePrefix, Namespace, SnapshotRead, StorageEvent};
use rdb_core::contracts::time::{ControlTime, Tick};
use rdb_core::contracts::trace::{AckRejectReason, Version};
use rdb_core::contracts::transport::{Frame, PeerLabel, TransportEvent};
use rdb_core::contracts::version::ENVELOPE_VERSION;
use rdb_core::replication::primary::Primary as PrimarySide;
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
}

#[retcd_test]
fn recovered_is_refused_when_this_copy_cannot_lead_the_selected_prefix() {
    let mut tracker = both_caught_up();
    let before = tracker.clone();
    let mut demoted = pin_without_b();
    demoted.members[0].role = RegularSecondary;
    demoted.members[1].role = Primary;
    let mut moved = pin_without_b();
    moved.members[0].node = NodeId(9);
    let mut elsewhere = pin_without_b();
    elsewhere.partition = PartitionId(9);
    let cases = [
        (
            recovery(10, d(10), demoted),
            replica(ReplicaIgnoreReason::InvalidConfig),
        ),
        (
            recovery(10, d(10), moved),
            replica(ReplicaIgnoreReason::InvalidConfig),
        ),
        (
            recovery(10, d(10), elsewhere),
            replica(ReplicaIgnoreReason::InvalidConfig),
        ),
        (recovery(10, d(11), pin_without_b()), alert()),
        (
            recovery(4, d(4), pin_without_b()),
            replica(ReplicaIgnoreReason::BarrierNotDurable),
        ),
        (
            recovery(13, d(13), pin_without_b()),
            replica(ReplicaIgnoreReason::BarrierNotDurable),
        ),
    ];
    for (result, answer) in cases {
        assert_eq!(tracker.on_recovered(&result, T), vec![answer]);
        assert_eq!(tracker, before);
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
        vec![peer_progress(B, HEAD + 1), gained(&[COPY_B])]
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
/// newest record does not report a second `Gained`.
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
        vec![peer_progress(B, HEAD + 3)]
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
        vec![send(COPY_B, 11)]
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

/// B-R48 ruling 2: routing delivers a partition's events in order, and R1 relies on it. A
/// `LocalApplied(13)` routed before B's ACK for 13 lets the ACK in. The same ACK routed first
/// is past the primary's head, is not verified, and stores nothing. The sim's one queue per
/// partition supplies the first order; a lost `LocalApplied` stays the P1 gap-Alert item.
#[retcd_test]
fn routing_hands_local_applied_to_the_tracker_before_an_ack_naming_its_seq() {
    let ack = b(HEAD + 1, HEAD + 1, HEAD);
    let mut in_order = routed();
    assert_eq!(
        route(&mut in_order, local_applied_event(HEAD + 1)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(
        route(&mut in_order, accepted(&ack)),
        vec![peer_progress(B, HEAD + 1), gained(&[COPY_B])]
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

/// Only an ACK the tracker admits reaches a running cursor. The cursor trusts what it is given,
/// so a forged or diverged ACK must not move it.
#[retcd_test]
fn only_an_admitted_ack_reaches_a_running_cursor() {
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
            Box::new(|t| t.on_local_applied(Seq(HEAD + 1), d(HEAD + 1))),
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
