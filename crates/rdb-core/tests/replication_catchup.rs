//! R1 `CatchupCursor`: design §3.6, with carriers from lead ruling B-R40.
//!
//! These are not `M7B-*` rows. They are developer scaffolding, written against the design
//! before any manual test. The cursor is driven directly and is not routed (B-R40).
//!
//! Fixture: the primary vouches for 5..=12 and its head is 12. The copy being caught up is B.

use config_log::retcd_test;

use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::envelope::{AppendAck, AppendOutcome, AppendReject, ReplicaProgress};
use rdb_core::contracts::event::{EffectKind, KernelEffect};
use rdb_core::contracts::ids::{
    AppliedSeq, BootId, ConfigVersion, DurableSeq, Generation, NodeId, OwnerEpoch, PartitionId,
    ReceivedSeq, ReplicaRole, Seq,
};
use rdb_core::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use rdb_core::contracts::membership::CopyId;
use rdb_core::replication::catchup::{CatchupCursor, Stop, MAX_PROBE_ROUNDS};
use rdb_core::replication::progress::DigestLadder;

const COPY_B: CopyId = CopyId(1);
const HEAD: u64 = 12;

fn d(seq: u64) -> Digest {
    Digest::of(Domain::Record, &[&seq.to_le_bytes()])
}

fn ladder() -> DigestLadder {
    let mut ladder = DigestLadder::new();
    for seq in 5..=HEAD {
        ladder.insert(Seq(seq), d(seq));
    }
    ladder
}

/// Feed one outcome against the fixture's ladder and head.
fn feed(cursor: &mut CatchupCursor, outcome: AppendOutcome) -> Vec<EffectKind> {
    cursor.on_outcome(outcome, &ladder(), Seq(HEAD))
}

/// B's ACK at `(received, applied)`. The cursor reads only the watermarks.
fn accepted(received: u64, applied: u64) -> AppendOutcome {
    AppendOutcome::Accepted(AppendAck {
        partition: PartitionId(4),
        generation: Generation(3),
        owner_epoch: OwnerEpoch(5),
        config_version: ConfigVersion(7),
        from: NodeId(2),
        boot: BootId(2),
        role: ReplicaRole::RegularSecondary,
        progress: ReplicaProgress {
            received: ReceivedSeq(received),
            buffered_applied: AppliedSeq(applied),
            durable: DurableSeq(applied),
        },
        digest_at_buffered: d(applied),
    })
}

fn need_prefix(have: u64, head_digest: Digest) -> AppendOutcome {
    AppendOutcome::Rejected(AppendReject::NeedPrefix {
        have: Seq(have),
        head_digest,
    })
}

fn kernel(effect: KernelEffect) -> EffectKind {
    EffectKind::Kernel(effect)
}

fn send(seq: u64) -> EffectKind {
    kernel(KernelEffect::SendEnvelopes {
        copy: COPY_B,
        from: Seq(seq),
        through: Seq(seq),
    })
}

fn snapshot() -> EffectKind {
    kernel(KernelEffect::SnapshotCatchupRequired {
        copy: COPY_B,
        barrier: Seq(HEAD),
    })
}

fn replica(reason: ReplicaIgnoreReason) -> EffectKind {
    kernel(KernelEffect::Ignored {
        reason: KernelIgnoredReason::Replica(reason),
    })
}

fn rejected(reject: AppendReject) -> EffectKind {
    kernel(KernelEffect::Ignored {
        reason: KernelIgnoredReason::AppendRejected(reject),
    })
}

/// A cursor with 11 in flight, after `NeedPrefix{have 10}`.
fn sending_11() -> CatchupCursor {
    let mut cursor = CatchupCursor::new(COPY_B);
    assert_eq!(feed(&mut cursor, need_prefix(10, d(10))), vec![send(11)]);
    cursor
}

#[retcd_test]
fn a_need_prefix_at_a_matching_head_sends_exactly_one_envelope() {
    let mut cursor = sending_11();
    assert_eq!(cursor.outstanding(), Some(Seq(11)));
    // The same need again is answered again, with the same one envelope (B-R47 S5-F1): the
    // first send may have been lost, and a re-send is idempotent at the receiver.
    let before = cursor.clone();
    assert_eq!(feed(&mut cursor, need_prefix(10, d(10))), vec![send(11)]);
    assert_eq!(cursor, before);
    // A need below the record in flight means that send was a gap, so it is answered.
    assert_eq!(feed(&mut cursor, need_prefix(8, d(8))), vec![send(9)]);
    assert_eq!(cursor.outstanding(), Some(Seq(9)));
}

#[retcd_test]
fn retention_is_checked_before_ancestry_and_below_the_floor_means_snapshot() {
    for outcome in [
        need_prefix(2, d(2)),
        need_prefix(4, Digest([0xEE; 32])),
        need_prefix(0, d(0)),
    ] {
        // 11 is in flight; a need below the floor supersedes it.
        let mut cursor = sending_11();
        assert_eq!(feed(&mut cursor, outcome), vec![snapshot()]);
        assert_eq!(cursor.stopped(), None, "absence is not divergence");
        assert_eq!(cursor.outstanding(), None);
        // So the next need for 11 is answered, not held as already in flight.
        assert_eq!(feed(&mut cursor, need_prefix(10, d(10))), vec![send(11)]);
    }
}

#[retcd_test]
fn a_differing_head_is_divergence_and_the_cursor_sends_nothing_again() {
    let mut cursor = CatchupCursor::new(COPY_B);
    assert_eq!(
        feed(&mut cursor, need_prefix(10, Digest([0xEE; 32]))),
        vec![kernel(KernelEffect::DivergenceDetected { copy: COPY_B })]
    );
    assert_eq!(cursor.stopped(), Some(Stop::Quarantined));
    assert_eq!(cursor.outstanding(), None);
    for outcome in [need_prefix(10, d(10)), accepted(HEAD, HEAD)] {
        assert_eq!(
            feed(&mut cursor, outcome),
            vec![replica(ReplicaIgnoreReason::QuarantinedTerminal)]
        );
    }
}

#[retcd_test]
fn probe_rounds_are_capped_then_a_snapshot_is_asked_for() {
    let mut cursor = CatchupCursor::new(COPY_B);
    for (round, seq) in (1..=MAX_PROBE_ROUNDS).zip([10, 9, 8, 7]) {
        assert_eq!(
            feed(&mut cursor, AppendOutcome::ProbeDigestAt { seq: Seq(seq) }),
            vec![send(seq)]
        );
        assert_eq!(cursor.probe_rounds(), round);
    }
    assert_eq!(
        feed(&mut cursor, AppendOutcome::ProbeDigestAt { seq: Seq(6) }),
        vec![snapshot()]
    );
    assert_eq!(cursor.probe_rounds(), MAX_PROBE_ROUNDS);
    assert_eq!(cursor.stopped(), None);
    // A probed record the receiver already holds ends the probing, so the count resets.
    feed(&mut cursor, AppendOutcome::AlreadyHave);
    assert_eq!(cursor.probe_rounds(), 0);
    feed(&mut cursor, AppendOutcome::ProbeDigestAt { seq: Seq(9) });
    assert_eq!(cursor.probe_rounds(), 1);
    // A record accepted resets the count.
    feed(&mut cursor, accepted(6, 6));
    assert_eq!(cursor.probe_rounds(), 0);
    // A probe the primary cannot answer is a snapshot too, and uses no round.
    assert_eq!(
        feed(&mut cursor, AppendOutcome::ProbeDigestAt { seq: Seq(3) }),
        vec![snapshot()]
    );
    assert_eq!(cursor.probe_rounds(), 0);
}

#[retcd_test]
fn every_outcome_reaches_a_named_handler() {
    use AppendReject as R;
    let copy_quarantined = kernel(KernelEffect::CopyQuarantined { copy: COPY_B });
    let ahead = kernel(KernelEffect::CopyAheadOnControl { copy: COPY_B });
    let stopping: [(R, EffectKind, Stop); 15] = [
        (R::Quarantined, copy_quarantined.clone(), Stop::Quarantined),
        (
            R::CorruptHistory { at: Seq(11) },
            copy_quarantined.clone(),
            Stop::Quarantined,
        ),
        (
            R::DivergentHistory { at: Seq(11) },
            copy_quarantined,
            Stop::Quarantined,
        ),
        (
            R::StaleGeneration {
                current: Generation(2),
            },
            rejected(R::StaleGeneration {
                current: Generation(2),
            }),
            Stop::BehindOnControl,
        ),
        (
            R::StaleEpoch {
                current: OwnerEpoch(4),
            },
            rejected(R::StaleEpoch {
                current: OwnerEpoch(4),
            }),
            Stop::BehindOnControl,
        ),
        (
            R::StaleConfig {
                current: ConfigVersion(6),
            },
            rejected(R::StaleConfig {
                current: ConfigVersion(6),
            }),
            Stop::BehindOnControl,
        ),
        (
            R::NeedLineage {
                current: Generation(4),
            },
            ahead.clone(),
            Stop::AheadOnControl,
        ),
        (
            R::NeedConfig {
                current: ConfigVersion(8),
            },
            ahead.clone(),
            Stop::AheadOnControl,
        ),
        (
            R::UnknownEpoch {
                current: OwnerEpoch(6),
            },
            ahead,
            Stop::AheadOnControl,
        ),
        (R::NotAMember, rejected(R::NotAMember), Stop::Refused),
        (
            R::WrongPartition,
            rejected(R::WrongPartition),
            Stop::Refused,
        ),
        (
            R::IncompatibleVersion,
            rejected(R::IncompatibleVersion),
            Stop::Refused,
        ),
        (R::TooLarge, rejected(R::TooLarge), Stop::Refused),
        (
            R::Unauthenticated,
            rejected(R::Unauthenticated),
            Stop::Refused,
        ),
        (
            R::StaleFence,
            replica(ReplicaIgnoreReason::RecoveryOnly),
            Stop::Refused,
        ),
    ];
    for (reject, effect, stop) in stopping {
        let mut cursor = sending_11();
        assert_eq!(
            feed(&mut cursor, AppendOutcome::Rejected(reject)),
            vec![effect],
            "{reject:?}"
        );
        assert_eq!(cursor.stopped(), Some(stop), "{reject:?}");
        assert_eq!(cursor.outstanding(), None, "{reject:?}");
        // Stopped means stopped: a later ACK sends nothing.
        let terminal = if stop == Stop::Quarantined {
            ReplicaIgnoreReason::QuarantinedTerminal
        } else {
            ReplicaIgnoreReason::NotRequired
        };
        assert_eq!(
            feed(&mut cursor, accepted(11, 11)),
            vec![replica(terminal)],
            "{reject:?}"
        );
    }
    // The five outcomes that do not stop the cursor.
    let mut cursor = sending_11();
    assert_eq!(
        feed(&mut cursor, AppendOutcome::AlreadyHave),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(cursor.outstanding(), None);
    assert_eq!(feed(&mut cursor, accepted(11, 11)), vec![send(12)]);
    assert_eq!(
        feed(
            &mut cursor,
            AppendOutcome::Busy {
                accepted_through: Seq(11)
            }
        ),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(
        feed(&mut cursor, AppendOutcome::ProbeDigestAt { seq: Seq(9) }),
        vec![send(9)]
    );
    assert_eq!(feed(&mut cursor, need_prefix(10, d(10))), vec![send(11)]);
    assert_eq!(cursor.stopped(), None);
}

#[retcd_test]
fn busy_is_resent_on_the_next_progress_event_not_before() {
    let mut cursor = sending_11();
    assert_eq!(
        feed(
            &mut cursor,
            AppendOutcome::Busy {
                accepted_through: Seq(10)
            }
        ),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(cursor.outstanding(), None);
    assert_eq!(feed(&mut cursor, accepted(11, 10)), vec![send(12)]);
    assert_eq!(cursor.outstanding(), Some(Seq(12)));
}

#[retcd_test]
fn the_ack_that_closes_the_gap_reports_caught_up_once() {
    let mut cursor = sending_11();
    assert_eq!(feed(&mut cursor, accepted(11, 11)), vec![send(12)]);
    // Received the head, not yet applied it: nothing to send and not caught up.
    assert_eq!(
        feed(&mut cursor, accepted(12, 11)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(
        feed(&mut cursor, accepted(12, 12)),
        vec![kernel(KernelEffect::CopyCaughtUp {
            copy: COPY_B,
            head: Seq(HEAD),
            digest: d(HEAD),
        })]
    );
    assert_eq!(
        feed(&mut cursor, accepted(12, 12)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    // A copy that was never behind never caught up.
    let mut current = CatchupCursor::new(COPY_B);
    assert_eq!(
        feed(&mut current, accepted(12, 12)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
}

// --- tester rows (R1 slice 5 gate, 2026-09-22) ----------------------------------------------

/// Tester row for finding S5-F1, ruled B-R47. A lost envelope stalled the cursor for good: the
/// copy's later `NeedPrefix` for the same record was taken as a duplicate of the need already
/// answered, and nothing else an honest copy says moves the cursor (§3.6 has no timer).
/// Re-sending is idempotent (§3.6, "Retransmission is idempotent by construction"), so every
/// `NeedPrefix` re-enters step 1, as the §3.6 outcome table says. The first half of
/// `a_need_prefix_at_a_matching_head_sends_exactly_one_envelope` inverted with it.
#[retcd_test]
fn tester_r1c_a_need_for_the_record_in_flight_is_answered_again() {
    let mut cursor = sending_11();
    // Envelope 11 was lost; the copy, still at 10, asks again.
    assert_eq!(feed(&mut cursor, need_prefix(10, d(10))), vec![send(11)]);
    assert_eq!(cursor.outstanding(), Some(Seq(11)));
    // The copy takes it and the catch-up finishes.
    assert_eq!(feed(&mut cursor, accepted(11, 11)), vec![send(12)]);
    assert_eq!(
        feed(&mut cursor, accepted(12, 12)),
        vec![kernel(KernelEffect::CopyCaughtUp {
            copy: COPY_B,
            head: Seq(HEAD),
            digest: d(HEAD),
        })]
    );
}

/// Tester row: K-B-17 at every position. A need at a seq the primary does not hold (below the
/// floor, at an interior hole, above the head) is a snapshot whatever digest it names, and the
/// cursor keeps running. At every retained seq, a differing digest is divergence and stops it.
#[retcd_test]
fn tester_r1c_only_a_retained_differing_digest_is_divergence() {
    let mut holed = ladder();
    holed.truncate_above(Seq(8));
    for seq in 10..=HEAD {
        holed.insert(Seq(seq), d(seq));
    }
    let other = Digest([0xEE; 32]);
    for have in [0, 4, 9, HEAD + 1, 1_000] {
        for digest in [d(have), other] {
            let mut cursor = CatchupCursor::new(COPY_B);
            assert_eq!(
                cursor.on_outcome(need_prefix(have, digest), &holed, Seq(HEAD)),
                vec![snapshot()],
                "{have}"
            );
            assert_eq!(cursor.stopped(), None, "{have}");
        }
    }
    for have in (5..=HEAD).filter(|s| *s != 9) {
        let mut cursor = CatchupCursor::new(COPY_B);
        assert_eq!(
            cursor.on_outcome(need_prefix(have, other), &holed, Seq(HEAD)),
            vec![kernel(KernelEffect::DivergenceDetected { copy: COPY_B })],
            "{have}"
        );
        assert_eq!(cursor.stopped(), Some(Stop::Quarantined), "{have}");
    }
}

/// Tester row for finding S5-F3, ruled B-R47. `Seq::next` is `self.0 + 1`, so
/// `NeedPrefix{have: u64::MAX}` or an ACK at `received: u64::MAX` panicked the kernel in a debug
/// build and wrapped to seq 0 in release. A need above the head is a snapshot (it is
/// `NotRetained`, and the lookup now runs before any arithmetic), and an ACK at or past the
/// head sends nothing.
#[retcd_test]
fn tester_r1c_the_top_of_the_seq_range_is_answered_not_panicked() {
    let mut cursor = CatchupCursor::new(COPY_B);
    assert_eq!(
        feed(&mut cursor, need_prefix(u64::MAX, d(0))),
        vec![snapshot()]
    );
    let mut cursor = CatchupCursor::new(COPY_B);
    assert_eq!(
        feed(&mut cursor, accepted(u64::MAX, HEAD)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
}
