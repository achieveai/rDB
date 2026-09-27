//! R1 `CatchupCursor`: design §3.6, with carriers from lead ruling B-R40.
//!
//! `m7b_<n>_*` functions are the §6 rows (M7B-55..61, 63) of
//! `docs/testing/test-plan-m7-kernel-b.md`. Where a later lead ruling overrode a plan literal,
//! the row asserts the ruling and says which. Plain-named functions are not plan rows: developer
//! scaffolding written against the design before any manual test, and the manual tester's
//! `tester_r1c_*` rows. The cursor is driven directly and is not routed (B-R40), except where a
//! row is about what reaches it.
//!
//! Fixture: the primary vouches for 5..=12 and its head is 12. The copy being caught up is B.

use config_log::retcd_test;

use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::envelope::{AppendAck, AppendOutcome, AppendReject, ReplicaProgress};
use rdb_core::contracts::event::{EffectKind, EventKind, KernelEffect};
use rdb_core::contracts::ids::{
    AppliedSeq, BootId, ConfigVersion, DurableSeq, Generation, NodeId, OwnerEpoch, PartitionId,
    ReceivedSeq, ReplicaRole, Seq, TimerVersion,
};
use rdb_core::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::time::{Tick, TimerFired};
use rdb_core::replication::catchup::{retransmit_timer, CatchupCursor, Stop, MAX_PROBE_ROUNDS};
use rdb_core::replication::progress::{DigestLadder, DigestLookup};

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

// --- §6 rows (M7B-55..61, 63) -------------------------------------------------------------------

/// A digest the fixture's ladder holds at no position.
const OTHER: Digest = Digest([0xEE; 32]);

/// M7B-55: D §3.6 steps 1–2. A `NeedPrefix` whose `have` the ladder holds with the same digest
/// sends exactly the one record after it, and that record is in flight.
///
/// **Lead ruling B-R47 (S5-F1) overrides the plan literal** "second `NeedPrefix` while outstanding
/// → `Ignored{OUTSTANDING}`": every `NeedPrefix` re-enters step 1, because the first send may
/// have been lost and nothing else an honest copy says would clear it. So the second need gets
/// the same one envelope, and the cursor is left exactly as the first need left it.
#[retcd_test]
fn m7b_55_need_prefix_with_matching_head_sends_exactly_one_envelope() {
    assert_eq!(ladder().lookup(Seq(10), d(10)), DigestLookup::Match);
    let mut cursor = CatchupCursor::new(COPY_B);
    assert_eq!(feed(&mut cursor, need_prefix(10, d(10))), vec![send(11)]);
    assert_eq!(cursor.outstanding(), Some(Seq(11)));
    assert_eq!(cursor.unacked(), Some(Seq(11)));
    assert_eq!(cursor.stopped(), None);
    let before = cursor.clone();
    assert_eq!(feed(&mut cursor, need_prefix(10, d(10))), vec![send(11)]);
    assert_eq!(
        cursor, before,
        "a repeated need re-sends and changes nothing"
    );
}

/// M7B-56: D §3.6 "retention is checked before ancestry" (K-B-17, K-B-27). `have 2` is below the
/// floor 5, so the ladder answers `NotRetained` whatever digest is named, and the answer is a
/// snapshot request at the head: no `DivergenceDetected`, no envelope, and the cursor keeps
/// running. The record that was in flight is superseded.
#[retcd_test]
fn m7b_56_retention_is_checked_before_ancestry_below_floor_means_snapshot() {
    for digest in [OTHER, d(2)] {
        assert_eq!(ladder().lookup(Seq(2), digest), DigestLookup::NotRetained);
        let mut cursor = sending_11();
        assert_eq!(
            feed(&mut cursor, need_prefix(2, digest)),
            vec![snapshot()],
            "{digest:?}"
        );
        assert_eq!(cursor.stopped(), None, "absence is not divergence");
        assert_eq!((cursor.outstanding(), cursor.unacked()), (None, None));
        assert_eq!(cursor.on_retransmit(), None, "nothing left to re-send");
    }
}

/// M7B-57: D §3.6 `Differs` arm (K-B-45, B-R31). `have 10` is retained and the copy's digest
/// there is not ours: the cursor emits `DivergenceDetected(B)` and nothing else (the rest of the
/// vector is the tracker's, M7B-140), sends nothing, and quarantines B's stream in the cursor.
/// Its twins: the same `have` with our digest sends (M7B-55); the same foreign digest below the
/// floor asks for a snapshot (M7B-56).
#[retcd_test]
fn m7b_57_differs_at_head_is_divergence_and_sends_nothing() {
    assert_eq!(
        ladder().lookup(Seq(10), OTHER),
        DigestLookup::Differs { stored: d(10) }
    );
    let mut cursor = sending_11();
    assert_eq!(
        feed(&mut cursor, need_prefix(10, OTHER)),
        vec![kernel(KernelEffect::DivergenceDetected { copy: COPY_B })]
    );
    assert_eq!(cursor.stopped(), Some(Stop::Quarantined));
    assert_eq!((cursor.outstanding(), cursor.unacked()), (None, None));
    // Quarantined in the cursor: nothing B says moves it again, and no timer re-sends 11.
    for outcome in [
        need_prefix(10, d(10)),
        accepted(11, 11),
        AppendOutcome::AlreadyHave,
        AppendOutcome::ProbeDigestAt { seq: Seq(9) },
    ] {
        assert_eq!(
            feed(&mut cursor, outcome),
            vec![replica(ReplicaIgnoreReason::QuarantinedTerminal)],
            "{outcome:?}"
        );
    }
    assert_eq!(
        (cursor.on_retransmit(), cursor.on_retransmit()),
        (None, None)
    );
    // The twins differ from it in one input each.
    assert_eq!(
        feed(&mut CatchupCursor::new(COPY_B), need_prefix(10, d(10))),
        vec![send(11)]
    );
    assert_eq!(
        feed(&mut CatchupCursor::new(COPY_B), need_prefix(4, OTHER)),
        vec![snapshot()]
    );
}

/// M7B-58: D §3.6 `probe_rounds` (K-B-27). Probing is capped at four answered rounds, then the
/// cursor asks for a snapshot and does not quarantine.
///
/// **Lead ruling B-R40 (Q-C1) reshapes the plan fixture.** `ProbeDigestAt{seq}` is B's outcome,
/// and the cursor answers it with the record itself, `SendEnvelopes{seq..=seq}`, not with a
/// `ProbeDigestAt` of its own. B walks back between probes with a `NeedPrefix` at a lower retained
/// seq; that re-enters step 1 and is not a probe answer, so it neither counts a round nor resets
/// the count. Only an accepted record resets it (D §3.6).
#[retcd_test]
fn m7b_58_probe_rounds_are_capped_at_four_then_snapshot() {
    assert_eq!(MAX_PROBE_ROUNDS, 4);
    let mut cursor = sending_11();
    for (round, seq) in (1..=MAX_PROBE_ROUNDS).zip([10, 9, 8, 7]) {
        assert_eq!(
            feed(&mut cursor, AppendOutcome::ProbeDigestAt { seq: Seq(seq) }),
            vec![send(seq)],
            "round {round}"
        );
        assert_eq!(cursor.probe_rounds(), round);
        assert_eq!(
            feed(&mut cursor, need_prefix(seq - 1, d(seq - 1))),
            vec![send(seq)],
            "walk back after round {round}"
        );
        assert_eq!(cursor.probe_rounds(), round, "a need is not a probe");
    }
    assert_eq!(
        feed(&mut cursor, AppendOutcome::ProbeDigestAt { seq: Seq(6) }),
        vec![snapshot()],
        "round 5"
    );
    assert_eq!(cursor.probe_rounds(), MAX_PROBE_ROUNDS);
    assert_eq!(cursor.stopped(), None, "no quarantine");
}

/// Every `AppendReject` variant, once. The `match` in [`reject_name`] has no wildcard, so a new
/// variant fails to compile there, and M7B-59's count check fails until it is listed here.
fn every_reject() -> [AppendReject; 16] {
    use AppendReject as R;
    [
        R::NeedPrefix {
            have: Seq(10),
            head_digest: d(10),
        },
        R::Quarantined,
        R::CorruptHistory { at: Seq(11) },
        R::DivergentHistory { at: Seq(11) },
        R::StaleGeneration {
            current: Generation(2),
        },
        R::StaleEpoch {
            current: OwnerEpoch(4),
        },
        R::StaleConfig {
            current: ConfigVersion(6),
        },
        R::NeedLineage {
            current: Generation(4),
        },
        R::NeedConfig {
            current: ConfigVersion(8),
        },
        R::UnknownEpoch {
            current: OwnerEpoch(6),
        },
        R::NotAMember,
        R::WrongPartition,
        R::IncompatibleVersion,
        R::TooLarge,
        R::Unauthenticated,
        R::StaleFence,
    ]
}

const fn reject_name(reject: AppendReject) -> &'static str {
    use AppendReject as R;
    match reject {
        R::NeedPrefix { .. } => "NeedPrefix",
        R::Quarantined => "Quarantined",
        R::CorruptHistory { .. } => "CorruptHistory",
        R::DivergentHistory { .. } => "DivergentHistory",
        R::StaleGeneration { .. } => "StaleGeneration",
        R::StaleEpoch { .. } => "StaleEpoch",
        R::StaleConfig { .. } => "StaleConfig",
        R::NeedLineage { .. } => "NeedLineage",
        R::NeedConfig { .. } => "NeedConfig",
        R::UnknownEpoch { .. } => "UnknownEpoch",
        R::NotAMember => "NotAMember",
        R::WrongPartition => "WrongPartition",
        R::IncompatibleVersion => "IncompatibleVersion",
        R::TooLarge => "TooLarge",
        R::Unauthenticated => "Unauthenticated",
        R::StaleFence => "StaleFence",
    }
}

const fn outcome_name(outcome: AppendOutcome) -> &'static str {
    match outcome {
        AppendOutcome::Accepted(_) => "Accepted",
        AppendOutcome::Busy { .. } => "Busy",
        AppendOutcome::AlreadyHave => "AlreadyHave",
        AppendOutcome::ProbeDigestAt { .. } => "ProbeDigestAt",
        AppendOutcome::Rejected(_) => "Rejected",
    }
}

/// The body of `fn <name>(` in the cursor's source, up to the end of that function.
fn cursor_fn_body(name: &str) -> &'static str {
    const SOURCE: &str = include_str!("../src/replication/catchup.rs");
    let start = SOURCE
        .find(&format!("fn {name}("))
        .unwrap_or_else(|| panic!("no fn {name} in catchup.rs"));
    let rest = &SOURCE[start..];
    let end = rest.find("\n    }\n").expect("the fn ends");
    &rest[..end]
}

/// M7B-59: D §3.6 outcome table and D §7 "no `_ =>`" (K-B-18). Every `AppendOutcome` the cursor
/// can be given reaches its own handler, and so does every `AppendReject` nested in `Rejected`
/// (CB-4: one enum, so the claim spans two matches, `on_outcome` and `on_reject`).
///
/// Per variant: `Accepted` sends the next record; `AlreadyHave` clears `outstanding` and the
/// probe count and sends nothing, because the receiver follows it with its ACK and that ACK
/// advances the cursor (§3.2); `Busy` clears `outstanding`, sends nothing, and the next progress
/// event re-sends; `ProbeDigestAt` is a probe; `NeedPrefix` is step 1; `Quarantined`,
/// `CorruptHistory`, `DivergentHistory` → `CopyQuarantined`; `NeedLineage`/`NeedConfig`/
/// `UnknownEpoch` → `CopyAheadOnControl`; `Stale{Generation,Epoch,Config}` → stopped
/// `BehindOnControl`; `TooLarge` and the other deployment faults → `Ignored` with the rejection
/// itself; `StaleFence` → `Ignored{RecoveryOnly}`.
///
/// Q-50: the plan's path `replication.rs` is now `replication/catchup.rs`, and a whole-file grep
/// would hit `Repeat::judge`'s `match digest`, which is not an outcome match. So the grep is
/// scoped to the two handler bodies.
#[retcd_test]
fn m7b_59_every_append_outcome_variant_reaches_a_named_handler() {
    use AppendReject as R;
    let rejects = every_reject();
    let names: std::collections::BTreeSet<_> = rejects.iter().map(|r| reject_name(*r)).collect();
    assert_eq!(names.len(), 16, "every AppendReject variant, once");

    let quarantined = || kernel(KernelEffect::CopyQuarantined { copy: COPY_B });
    let ahead = || kernel(KernelEffect::CopyAheadOnControl { copy: COPY_B });
    for reject in rejects {
        let (effect, stop) = match reject {
            R::NeedPrefix { .. } => (send(11), None),
            R::Quarantined | R::CorruptHistory { .. } | R::DivergentHistory { .. } => {
                (quarantined(), Some(Stop::Quarantined))
            }
            R::StaleGeneration { .. } | R::StaleEpoch { .. } | R::StaleConfig { .. } => {
                (rejected(reject), Some(Stop::BehindOnControl))
            }
            R::NeedLineage { .. } | R::NeedConfig { .. } | R::UnknownEpoch { .. } => {
                (ahead(), Some(Stop::AheadOnControl))
            }
            R::NotAMember
            | R::WrongPartition
            | R::IncompatibleVersion
            | R::TooLarge
            | R::Unauthenticated => (rejected(reject), Some(Stop::Refused)),
            R::StaleFence => (
                replica(ReplicaIgnoreReason::RecoveryOnly),
                Some(Stop::Refused),
            ),
        };
        let mut cursor = sending_11();
        assert_eq!(
            feed(&mut cursor, AppendOutcome::Rejected(reject)),
            vec![effect],
            "{reject:?}"
        );
        assert_eq!(cursor.stopped(), stop, "{reject:?}");
    }
    // `TooLarge` and `RecoveryOnly` stay apart in one vector: different arms, different types.
    assert_ne!(
        rejected(R::TooLarge),
        replica(ReplicaIgnoreReason::RecoveryOnly)
    );

    // The four outer variants besides `Rejected`, each from a cursor with 11 in flight.
    let mut outer = std::collections::BTreeSet::new();
    let mut cursor = sending_11();
    let ack = accepted(11, 11);
    outer.insert(outcome_name(ack));
    assert_eq!(feed(&mut cursor, ack), vec![send(12)], "Accepted advances");

    let mut cursor = sending_11();
    feed(&mut cursor, AppendOutcome::ProbeDigestAt { seq: Seq(9) });
    outer.insert(outcome_name(AppendOutcome::AlreadyHave));
    assert_eq!(
        feed(&mut cursor, AppendOutcome::AlreadyHave),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!((cursor.outstanding(), cursor.probe_rounds()), (None, 0));
    assert_eq!(
        feed(&mut cursor, accepted(11, 11)),
        vec![send(12)],
        "the ACK after AlreadyHave advances"
    );

    let mut cursor = sending_11();
    let busy = AppendOutcome::Busy {
        accepted_through: Seq(10),
    };
    outer.insert(outcome_name(busy));
    assert_eq!(
        feed(&mut cursor, busy),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(cursor.outstanding(), None);
    assert_eq!(
        feed(&mut cursor, accepted(11, 10)),
        vec![send(12)],
        "the next progress event re-sends"
    );

    let mut cursor = sending_11();
    let probe = AppendOutcome::ProbeDigestAt { seq: Seq(9) };
    outer.insert(outcome_name(probe));
    assert_eq!(feed(&mut cursor, probe), vec![send(9)]);
    assert_eq!(cursor.probe_rounds(), 1);

    outer.insert(outcome_name(AppendOutcome::Rejected(R::TooLarge)));
    assert_eq!(outer.len(), 5, "every AppendOutcome variant, once");

    // Q-50, over both matches: no wildcard arm, and every variant named in its own arm.
    let on_outcome = cursor_fn_body("on_outcome");
    let on_reject = cursor_fn_body("on_reject");
    assert!(on_outcome.contains("match outcome {"), "{on_outcome}");
    assert!(on_reject.contains("match reject {"), "{on_reject}");
    for (name, body) in [("on_outcome", on_outcome), ("on_reject", on_reject)] {
        assert!(!body.contains("_ =>"), "{name} has a wildcard arm");
    }
    for variant in outer {
        assert!(
            on_outcome.contains(&format!("AppendOutcome::{variant}")),
            "{variant}"
        );
    }
    for variant in names {
        assert!(on_reject.contains(&format!("R::{variant}")), "{variant}");
    }
}

/// M7B-63: D §3.2 gap row, §3.3 `CommitFailed` and §3.3 behind-the-cutoff `Recovered` each ask
/// with one shape, `NeedPrefix{have: accept_head.seq, head_digest: accept_head.digest}`, the
/// receiver's accept head after the step. The cursor answers every one of them with the record
/// after `have`, the same answer for the same value whichever path produced it.
#[retcd_test]
fn m7b_63_need_prefix_has_one_shape_at_both_ends() {
    use producers::{behind_the_cutoff, commit_failed, gap, gap_while_staged, record_digest};
    let asks = [
        ("gap", gap()),
        ("gap while 11 is staged", gap_while_staged()),
        ("CommitFailed", commit_failed()),
        ("behind the cutoff", behind_the_cutoff()),
    ];
    let mut history = DigestLadder::new();
    for seq in 5..=HEAD {
        history.insert(Seq(seq), record_digest(seq));
    }
    for (path, (outcome, accept_head)) in asks {
        assert_eq!(
            outcome,
            need_prefix(accept_head.seq.0, accept_head.digest),
            "{path}"
        );
        assert_eq!(
            accept_head.digest,
            record_digest(accept_head.seq.0),
            "{path}"
        );
        let mut cursor = CatchupCursor::new(COPY_B);
        assert_eq!(
            cursor.on_outcome(outcome, &history, Seq(HEAD)),
            vec![send(accept_head.seq.0 + 1)],
            "{path}"
        );
    }
    // Three different paths, one value, when they start from the same head.
    assert_eq!(gap().0, commit_failed().0);
    assert_eq!(gap().0, behind_the_cutoff().0);
}

/// B's real `AppendReceiver`, driven through `Replication::step`, for M7B-63. B holds `(10, d10)`
/// of the chain `1..` from the root; A (node 1) is primary; the takeover in `behind_the_cutoff`
/// pins C (node 3) primary with a cutoff of 100.
mod producers {
    use bytes::Bytes;
    use rdb_core::contracts::authority::{
        AuthorityView, DenyReason, FencingProof, Lineage, PartitionMode, Revocation,
    };
    use rdb_core::contracts::digest::{Digest, Domain};
    use rdb_core::contracts::envelope::{AppendOutcome, EnvelopeHeader, ReplicationEnvelope};
    use rdb_core::contracts::event::{
        Budgets, EffectKind, Event, EventKind, KernelEvent, Module, StepCtx,
    };
    use rdb_core::contracts::ids::{
        AuthorityGeneration, BatchId, BootId, ClientId, ConfigVersion, CorrelationId, DurableSeq,
        EventId, Generation, GrantId, LeaseId, MessageId, NodeId, OwnerEpoch, PartitionId,
        ReplicaRole, RequestId, RequestIdentity, Revision, Seq, SnapshotHandle, TenantId,
    };
    use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
    use rdb_core::contracts::recovery::{
        CommittedRoot, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap,
        SelectedLineage,
    };
    use rdb_core::contracts::storage::{
        Namespace, SnapshotRead, StorageEvent, StorageFault, Write,
    };
    use rdb_core::contracts::time::{ControlTime, Tick};
    use rdb_core::contracts::trace::Version;
    use rdb_core::contracts::transport::{Frame, PeerLabel, SendEffect, TransportEvent};
    use rdb_core::contracts::txn::Outcome;
    use rdb_core::contracts::version::ENVELOPE_VERSION;
    use rdb_core::replication::append::{AppendReceiver, Head, ReceiverInit, UNSOLICITED};
    use rdb_core::replication::wire::decode_reply;
    use rdb_core::replication::Replication;

    const P: PartitionId = PartitionId(4);
    const GEN: Generation = Generation(3);
    const EPOCH: OwnerEpoch = OwnerEpoch(5);
    const CONFIG: ConfigVersion = ConfigVersion(7);
    const A: NodeId = NodeId(1);
    const B: NodeId = NodeId(2);
    const C: NodeId = NodeId(3);
    const FRAME_ID: MessageId = MessageId(42);
    const NEW_GEN: Generation = Generation(4);
    const NEW_EPOCH: OwnerEpoch = OwnerEpoch(6);
    const NEW_CONFIG: ConfigVersion = ConfigVersion(8);

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

    fn ctx() -> StepCtx<'static> {
        StepCtx {
            now: Tick(0),
            control_time: ControlTime {
                estimate: Tick(0),
                error_millis: 10,
                bound_established: true,
                sampled_at: Tick(0),
            },
            node: B,
            boot: BootId(2),
            partition: P,
            generation: GEN,
            owner_epoch: EPOCH,
            config_version: CONFIG,
            snapshot: &SNAPSHOT,
            budgets: &BUDGETS,
        }
    }

    fn member(copy: u8, node: NodeId, role: ReplicaRole) -> Member {
        Member {
            copy: CopyId(copy),
            node,
            boot: BootId(u64::from(node.0)),
            role,
        }
    }

    const fn lineage(generation: Generation, owner_epoch: OwnerEpoch) -> Lineage {
        Lineage {
            partition: P,
            generation,
            owner_epoch,
        }
    }

    /// The chain's record at `seq`, sealed on `prev`.
    fn envelope(seq: u64, prev: Digest) -> ReplicationEnvelope {
        let mut env = ReplicationEnvelope {
            header: EnvelopeHeader {
                protocol_version: ENVELOPE_VERSION,
                partition: P,
                generation: GEN,
                config_version: CONFIG,
                owner_epoch: EPOCH,
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

    /// The records `1..=n` from the root.
    fn chain(n: u64) -> Vec<ReplicationEnvelope> {
        let mut out: Vec<ReplicationEnvelope> = Vec::new();
        for seq in 1..=n {
            let prev = out.last().map_or(Digest::ROOT, |env| env.record_digest);
            out.push(envelope(seq, prev));
        }
        out
    }

    /// The chain's record digest at `seq`: what both ends hold there.
    pub(super) fn record_digest(seq: u64) -> Digest {
        chain(seq).pop().expect("seq >= 1").record_digest
    }

    /// Replication with only B's receiver, at `(10, d10)`, durable 10.
    fn module() -> Replication {
        let receiver = AppendReceiver::new(ReceiverInit {
            config: PartitionConfig::new(
                P,
                CONFIG,
                vec![
                    member(0, A, ReplicaRole::Primary),
                    member(1, B, ReplicaRole::RegularSecondary),
                    member(2, C, ReplicaRole::RegularSecondary),
                ],
            ),
            own: CopyId(1),
            lineage: lineage(GEN, EPOCH),
            head: Head {
                seq: Seq(10),
                digest: record_digest(10),
            },
            durable: DurableSeq(10),
        })
        .expect("golden receiver");
        let mut module = Replication::new();
        module.install_receiver(receiver);
        module
    }

    fn event(kind: EventKind) -> Event {
        Event {
            id: EventId(1),
            at: Tick(0),
            node: B,
            boot: BootId(2),
            partition: P,
            correlation: CorrelationId(9),
            kind,
        }
    }

    fn step(module: &mut Replication, event: &Event) -> Vec<EffectKind> {
        module
            .step(&ctx(), event)
            .expect("R1 answers its own event")
            .into_iter()
            .map(|effect| effect.kind)
            .collect()
    }

    /// A's append of the chain's record at `seq`.
    fn append(module: &mut Replication, seq: u64) -> Vec<EffectKind> {
        let body = chain(seq).pop().expect("seq").encode().expect("encode");
        let delivered = event(EventKind::Transport(TransportEvent::Delivered {
            from: PeerLabel {
                node: A,
                boot: BootId(1),
                authenticated: true,
            },
            frame: Frame {
                id: FRAME_ID,
                protocol: ENVELOPE_VERSION,
                config: CONFIG,
                sender: lineage(GEN, EPOCH),
                body,
            },
        }));
        step(module, &delivered)
    }

    /// The one reply in `effects`, after checking where it goes and under which message id and
    /// configuration.
    fn only_reply(
        effects: &[EffectKind],
        to: NodeId,
        id: MessageId,
        config: ConfigVersion,
    ) -> AppendOutcome {
        match effects {
            [EffectKind::Send(SendEffect::Unicast { to: dest, frame })] => {
                assert_eq!((*dest, frame.id, frame.config), (to, id, config));
                decode_reply(&frame.body).expect("reply decodes")
            }
            other => panic!("expected one reply, got {other:?}"),
        }
    }

    fn accept_head(module: &Replication) -> Head {
        module.receiver(B, P).expect("installed").accept_head()
    }

    /// §3.2 gap row: 13 arrives at a copy holding 10.
    pub(super) fn gap() -> (AppendOutcome, Head) {
        let mut module = module();
        let effects = append(&mut module, 13);
        (
            only_reply(&effects, A, FRAME_ID, CONFIG),
            accept_head(&module),
        )
    }

    /// §3.2 gap row with 11 staged and not yet committed: the accept head is the staged record.
    pub(super) fn gap_while_staged() -> (AppendOutcome, Head) {
        let mut module = module();
        append(&mut module, 11);
        let effects = append(&mut module, 13);
        let head = accept_head(&module);
        assert_eq!(head.seq, Seq(11), "11 is staged");
        (only_reply(&effects, A, FRAME_ID, CONFIG), head)
    }

    /// §3.3 `CommitFailed`: 11 is staged, then its batch fails.
    pub(super) fn commit_failed() -> (AppendOutcome, Head) {
        let mut module = module();
        append(&mut module, 11);
        let failed = event(EventKind::Storage(StorageEvent::CommitFailed {
            batch: BatchId(0),
            fault: StorageFault::WriteFailed,
        }));
        let effects = step(&mut module, &failed);
        (
            only_reply(&effects, A, FRAME_ID, CONFIG),
            accept_head(&module),
        )
    }

    /// §3.3 behind the cutoff: a takeover whose selected prefix ends at 100, past B's 10.
    pub(super) fn behind_the_cutoff() -> (AppendOutcome, Head) {
        let cutoff = Seq(100);
        let cutoff_digest = record_digest(100);
        let root = lineage(NEW_GEN, NEW_EPOCH);
        let pinned = PartitionConfig::new(
            P,
            NEW_CONFIG,
            vec![
                member(0, A, ReplicaRole::RegularSecondary),
                member(1, B, ReplicaRole::RegularSecondary),
                member(2, C, ReplicaRole::Primary),
            ],
        );
        let result = RecoveryResult {
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
                source: CopyId(2),
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
                    boot_id: BootId(3),
                    authority_generation: AuthorityGeneration(1),
                    config_version: NEW_CONFIG,
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
        };
        let mut module = module();
        let recovered = event(EventKind::Kernel(KernelEvent::Recovered(Box::new(result))));
        let effects = step(&mut module, &recovered);
        (
            only_reply(&effects, C, UNSOLICITED, NEW_CONFIG),
            accept_head(&module),
        )
    }
}

/// M7B-60: D §3.6 `Busy` handler. `Busy` is not an error and not a backoff: the cursor clears
/// `outstanding`, sends nothing, and the next progress event sends exactly one record. L1's
/// health ticks between them never reach the cursor, so none of them sends.
///
/// **Rulings that bound the title.** "Not a timer" means no health tick and no `Busy` backoff.
/// It does not mean R1 has no timer: lead rulings B-R67 and B-R67a re-send a record that is sent
/// and not ACKed on the partition's retransmit timer, and `Busy` leaves that record unACKed.
/// That re-send is pinned by M7B-186..193, not here. The health ticks are driven through
/// `Replication::step`, which declines them (M7B-61), because the cursor has no tick entry.
#[retcd_test]
fn m7b_60_busy_is_resent_on_the_next_progress_event_not_a_timer() {
    use routed::{b_ack, catching_up_b, cursor, declined, from_b, health_tick, sends, step};
    let mut module = catching_up_b();
    let busy = AppendOutcome::Busy {
        accepted_through: Seq(10),
    };
    assert_eq!(
        step(&mut module, 1, from_b(&busy)).expect("routed"),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    let running = cursor(&module).expect("Busy does not stop the cursor");
    assert_eq!(running.outstanding(), None);
    for now in [10, 20, 30] {
        declined(&mut module, now, health_tick());
    }
    let effects = step(&mut module, 40, from_b(&b_ack(11, 10))).expect("routed");
    assert_eq!(sends(&effects), vec![12], "{effects:?}");
    assert_eq!(
        cursor(&module).expect("running").outstanding(),
        Some(Seq(12))
    );
}

/// M7B-61: D §3.6 "cursor has no timers" (BA-2, BA-10). L1's health tick — the landed
/// `EventKind::Timer(TimerFired{id: HEALTH_EVAL_TIMER, ..})`, tick from `ctx.now` — does not
/// move a cursor, and R1 names why.
///
/// **The plan literal `[Ignored{NOT_A_CURSOR_EVENT}]` has no producer.** The run loop offers every
/// event to every module, and R1 declines every event that is not its own with
/// `Unavailable(Replication)` (the module doc of `replication.rs`). So the named answer is that
/// decline, with nothing changed: no cursor, tracker or timer state moves, on a node with a
/// cursor running, a primary with none, or nothing installed. D §3.6's "no timers" is itself
/// superseded by B-R60 (keepalive) and B-R67 (retransmit); neither timer moves a cursor's
/// position on its own, and both are R1's, so both are answered, never declined (the twin below).
#[retcd_test]
fn m7b_61_cursor_ignores_timer_events_with_a_reason() {
    use routed::{catching_up_b, cursor, declined, health_tick, primary, step};
    let mut running = catching_up_b();
    let in_flight = cursor(&running).expect("running").clone();
    declined(&mut running, 100, health_tick());
    assert_eq!(cursor(&running), Some(&in_flight));
    declined(&mut primary(), 100, health_tick());
    declined(
        &mut rdb_core::replication::Replication::new(),
        100,
        health_tick(),
    );
    // The twin: R1's own retransmit timer, at the version it armed, is answered, not declined.
    let fired = EventKind::Timer(TimerFired {
        id: retransmit_timer(PartitionId(4)),
        version: TimerVersion(1),
        scheduled_at: Tick::ZERO,
    });
    assert!(step(&mut running, 100, fired).is_ok());
}

/// A's primary side, routed through `Replication::step`, for the rows about what reaches the
/// cursor (M7B-60, 61). The tracker vouches for 5..=12 with the same digests as the cursor
/// fixture above, and B's cursor starts on `NeedPrefix{have 10}`.
mod routed {
    use bytes::Bytes;
    use rdb_core::contracts::authority::Lineage;
    use rdb_core::contracts::envelope::{AppendAck, AppendOutcome, ReplicaProgress};
    use rdb_core::contracts::errors::{Capability, RdbError};
    use rdb_core::contracts::event::{
        Budgets, EffectKind, Event, EventKind, KernelEffect, Module, StepCtx,
    };
    use rdb_core::contracts::ids::{
        AppliedSeq, BootId, ConfigVersion, CorrelationId, DurableSeq, EventId, Generation,
        MessageId, NodeId, OwnerEpoch, PartitionId, ReceivedSeq, ReplicaRole, Seq, SnapshotHandle,
        TimerVersion,
    };
    use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
    use rdb_core::contracts::storage::{Namespace, SnapshotRead};
    use rdb_core::contracts::time::{ControlTime, Tick, TimerFired};
    use rdb_core::contracts::trace::Version;
    use rdb_core::contracts::transport::{Frame, PeerLabel, TransportEvent};
    use rdb_core::contracts::version::ENVELOPE_VERSION;
    use rdb_core::protection::HEALTH_EVAL_TIMER;
    use rdb_core::replication::catchup::CatchupCursor;
    use rdb_core::replication::progress::{ProgressTracker, TrackerInit};
    use rdb_core::replication::wire::encode_reply;
    use rdb_core::replication::Replication;

    use super::{d, ladder, need_prefix, HEAD};

    const P: PartitionId = PartitionId(4);
    const GEN: Generation = Generation(3);
    const EPOCH: OwnerEpoch = OwnerEpoch(5);
    const CONFIG: ConfigVersion = ConfigVersion(7);
    const A: NodeId = NodeId(1);
    const B: NodeId = NodeId(2);
    const C: NodeId = NodeId(3);

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

    fn ctx(now: u64) -> StepCtx<'static> {
        StepCtx {
            now: Tick(now),
            control_time: ControlTime {
                estimate: Tick(now),
                error_millis: 10,
                bound_established: true,
                sampled_at: Tick(now),
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

    const fn lineage() -> Lineage {
        Lineage {
            partition: P,
            generation: GEN,
            owner_epoch: EPOCH,
        }
    }

    fn member(copy: u8, node: NodeId, role: ReplicaRole) -> Member {
        Member {
            copy: CopyId(copy),
            node,
            boot: BootId(u64::from(node.0)),
            role,
        }
    }

    /// Replication hosting A's primary side, head 12, and no cursor.
    pub(super) fn primary() -> Replication {
        let mut config = PartitionConfig::new(
            P,
            CONFIG,
            vec![
                member(0, A, ReplicaRole::Primary),
                member(1, B, ReplicaRole::RegularSecondary),
                member(2, C, ReplicaRole::RegularSecondary),
            ],
        );
        config.min_regular_acks = 1;
        let tracker = ProgressTracker::new(TrackerInit {
            config,
            own: CopyId(0),
            lineage: lineage(),
            history: ladder(),
            local: ReplicaProgress {
                received: ReceivedSeq(HEAD),
                buffered_applied: AppliedSeq(HEAD),
                durable: DurableSeq(HEAD),
            },
        })
        .expect("golden tracker");
        let mut module = Replication::new();
        module.install_primary(tracker);
        module
    }

    fn event(now: u64, kind: EventKind) -> Event {
        Event {
            id: EventId(1),
            at: Tick(now),
            node: A,
            boot: BootId(1),
            partition: P,
            correlation: CorrelationId(9),
            kind,
        }
    }

    /// Step `kind` on A at `now`, as `Replication::step` answers it.
    pub(super) fn step(
        module: &mut Replication,
        now: u64,
        kind: EventKind,
    ) -> Result<Vec<EffectKind>, RdbError> {
        module
            .step(&ctx(now), &event(now, kind))
            .map(|effects| effects.into_iter().map(|effect| effect.kind).collect())
    }

    /// B's reply carrying `outcome`.
    pub(super) fn from_b(outcome: &AppendOutcome) -> EventKind {
        EventKind::Transport(TransportEvent::Delivered {
            from: PeerLabel {
                node: B,
                boot: BootId(2),
                authenticated: true,
            },
            frame: Frame {
                id: MessageId(42),
                protocol: ENVELOPE_VERSION,
                config: CONFIG,
                sender: lineage(),
                body: encode_reply(outcome),
            },
        })
    }

    /// B's ACK at `(received, applied)`, durable at applied, with our digest at applied.
    pub(super) fn b_ack(received: u64, applied: u64) -> AppendOutcome {
        AppendOutcome::Accepted(AppendAck {
            partition: P,
            generation: GEN,
            owner_epoch: EPOCH,
            config_version: CONFIG,
            from: B,
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

    /// L1's health tick, as it reaches every module: the landed `TimerFired` shape, the tick
    /// read from `ctx.now` (T-B-02, BA-10).
    pub(super) fn health_tick() -> EventKind {
        EventKind::Timer(TimerFired {
            id: HEALTH_EVAL_TIMER,
            version: TimerVersion(0),
            scheduled_at: Tick::ZERO,
        })
    }

    /// `primary()` after B's `NeedPrefix{have 10}`: B's cursor has 11 in flight.
    pub(super) fn catching_up_b() -> Replication {
        let mut module = primary();
        let effects = step(&mut module, 0, from_b(&need_prefix(10, d(10)))).expect("routed");
        assert_eq!(sends(&effects), vec![11], "{effects:?}");
        module
    }

    /// B's cursor on A, if one runs.
    pub(super) fn cursor(module: &Replication) -> Option<&CatchupCursor> {
        module.primary(A, P).expect("installed").cursor(CopyId(1))
    }

    /// `kind` is not R1's: `step` declines it as `Unavailable(Replication)`, and the module,
    /// every cursor included, is exactly as it was.
    pub(super) fn declined(module: &mut Replication, now: u64, kind: EventKind) {
        let before = module.clone();
        match step(module, now, kind) {
            Err(RdbError::Unavailable { capability, .. }) => {
                assert_eq!(capability, Capability::Replication);
            }
            other => panic!("expected R1 to decline, got {other:?}"),
        }
        assert_eq!(*module, before, "a declined event changes nothing");
    }

    /// The records `effects` sends to B, in order.
    pub(super) fn sends(effects: &[EffectKind]) -> Vec<u64> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                EffectKind::Kernel(KernelEffect::SendEnvelopes {
                    copy: CopyId(1),
                    from,
                    through,
                }) => {
                    assert_eq!(from, through, "one record in flight");
                    Some(from.0)
                }
                _ => None,
            })
            .collect()
    }
}
