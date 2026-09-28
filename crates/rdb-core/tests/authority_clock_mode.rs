//! A-R42: `ClockMode::Unbounded` on a held grant with a valid sample denies every check and
//! **does not fence**. A rejected sample still fences in either mode (finding F1, last row).
//!
//! The first five tests are not `M7A-*` rows. They are the regression guard for one lead ruling,
//! written before the plan rows that cover the same ground, so that the defect it names could
//! not come back silently while those rows were still being written. The plan's §3.4 rows
//! (`m7a_36_*` to `m7a_50_*`) follow them, in their own marked section at the end of the file.
//!
//! The defect: [`rdb_core::authority::clock::utc_ok`] answered
//! `Err(DenyReason::ClockUnbounded)` both for the configured *mode* and for
//! `ClockFault::Terminal`, and `Authority::revalidate` fences on that value with no way to tell
//! them apart — so `set_clock_mode(Unbounded)` on a healthy primary fenced it. `ClockMode`'s own
//! doc cites spec §7.2 to say the mode does not fence: the node "simply denies every check until
//! the mode is bounded".
//!
//! # The two halves are one test on purpose
//!
//! The fix splits a `DenyReason` in two, and a split can be wrong in **both** directions. The
//! mode half proves the new reason does not fence; the `Terminal` half proves the old one still
//! does. A test carrying only the first would pass against a kernel that had stopped fencing on
//! a bad *sample*, which is an ADR-rdb-0007 §3 trigger and the more dangerous direction of the
//! two.

use config_log::retcd_test;

use std::collections::BTreeSet;

use bytes::Bytes;
use rdb_core::authority::clock::ClockMode;
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::authority::{Authority, AuthorityTimer};
use rdb_core::contracts::authority::{
    AuthorityEffect, AuthorityEvent, AuthorityIgnoreReason, AuthorityView, DenyReason, FenceScope,
    Lineage, Verdict,
};
use rdb_core::contracts::control::{
    CasOutcome, ControlEffect, ControlEvent, ControlKey, ControlPrefix, ControlRecord, ReadOutcome,
};
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module,
    NodeLifecycle, StepCtx,
};
use rdb_core::contracts::ids::{
    AuthorityGeneration, BatchId, BootId, ConfigVersion, ControlRequestId, CorrelationId, EventId,
    Generation, GrantId, NodeId, OwnerEpoch, PartitionId, Revision, Seq, SnapshotHandle,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::storage::{Namespace, SnapshotRead, StorageEvent, StorageFault};
use rdb_core::contracts::time::{ControlTime, Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::Version;

const NODE: NodeId = NodeId(1);
const BOOT: BootId = BootId(1);
const PARTITION: PartitionId = PartitionId(1);

/// A snapshot of nothing. A1 never reads through it — every input it acts on arrives as an
/// event — so a fake that answers `None` is the whole requirement.
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

/// A context at `now`, carrying a bounded sample taken at `sampled_at` with `error_millis`.
///
/// The sample is a parameter and not a constant because lead ruling A-R45 says an exact effect
/// vector moves with `ctx.control_time`: an unchanged sample publishes no view, a moved one
/// publishes one. Both rows below pin it deliberately.
fn ctx_at(now: u64, sampled_at: u64, error_millis: u64, bound: bool) -> StepCtx<'static> {
    StepCtx {
        now: Tick(now),
        control_time: ControlTime {
            estimate: Tick(1_000_000),
            error_millis,
            bound_established: bound,
            sampled_at: Tick(sampled_at),
        },
        node: NODE,
        boot: BOOT,
        partition: PARTITION,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        config_version: ConfigVersion(1),
        snapshot: &SNAPSHOT,
        budgets: &BUDGETS,
    }
}

/// Any event at all. A1 judges both admission conjuncts on whatever arrives, so the event kind
/// is not the subject here — a control read for a family A1 does not serve from is the quietest
/// thing that reaches `step`.
fn probe(id: u64, at: u64) -> Event {
    Event {
        id: EventId(id),
        at: Tick(at),
        node: NODE,
        boot: BOOT,
        partition: PARTITION,
        correlation: CorrelationId(1),
        kind: EventKind::Control(ControlEvent::WatchProgress {
            prefix: rdb_core::contracts::control::ControlPrefix::Partitions,
            revision: Revision(1),
        }),
    }
}

/// Into `Held` through the real acquisition (lead ruling A-R47): `AcquireDue` issues the
/// create-only CAS on `grants/{us}`, and its commit, under the same correlation, adopts it.
///
/// `E` is `e_new` computed from the sample in this context at the dispatch tick, so the fixture
/// chooses the expiry by choosing the sample and needs no reach-through and no test-only
/// constructor.
fn held_kernel() -> Authority {
    let mut kernel = Authority::new();
    let ctx = ctx_at(0, 0, 10, true);
    let at = |id: u64, kind: EventKind| Event {
        id: EventId(id),
        at: Tick::ZERO,
        node: NODE,
        boot: BOOT,
        partition: PARTITION,
        correlation: CorrelationId(1),
        kind,
    };
    let due = at(
        1,
        EventKind::Timer(TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: kernel.timer_version(AuthorityTimer::Acquire),
            scheduled_at: Tick::ZERO,
        }),
    );
    kernel
        .step(&ctx, &due)
        .expect("the AcquireDue row is built");
    let committed = at(
        2,
        EventKind::Control(ControlEvent::CasResult {
            request: in_flight(&kernel),
            key: ControlKey::Grant(NODE),
            outcome: CasOutcome::Committed(Revision(7)),
        }),
    );
    kernel
        .step(&ctx, &committed)
        .expect("the acquisition commit row is built");
    assert!(
        kernel.state().is_held(),
        "preamble must reach Held or nothing below is testing what it says"
    );
    kernel
}

/// Every `Fence` in an effect vector, as its reason.
fn fences(effects: &[Effect]) -> Vec<DenyReason> {
    scoped_fences(effects)
        .into_iter()
        .map(|(_, reason)| reason)
        .collect()
}

fn lineage() -> Lineage {
    Lineage {
        partition: PARTITION,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    }
}

/// A-R42, the half the tester found: the **configured mode** denies and never fences.
///
/// Fails before the fix with `fences == [ClockUnbounded]` and `state().is_fenced()`.
#[retcd_test]
fn unbounded_clock_mode_denies_every_check_and_never_fences() {
    let mut kernel = held_kernel();
    kernel.set_clock_mode(ClockMode::Unbounded);

    // The same sample as the preamble, so lead ruling A-R45's other mover is pinned: nothing in
    // the vector below is a published view reacting to a sample that moved.
    let ctx = ctx_at(100, 0, 10, true);
    let effects = kernel
        .step(&ctx, &probe(2, 100))
        .expect("an unbounded-mode step is not an unavailable seam");

    assert_eq!(
        fences(&effects),
        Vec::<DenyReason>::new(),
        "spec §7.2: a node that cannot establish a bound stops accepting requests, it does not \
         fence — there is no grant-ending event (lead ruling A-R42)"
    );
    assert!(
        kernel.state().is_held(),
        "the grant is still held, and it stays held: `e_new` is guarded on the sample, not the \
         mode (K-A-07), so with a valid sample renewals continue. Safety holds by denial, and the \
         cost is availability (spec §7.2; the lead corrected A-R42's first premise)"
    );
    assert_eq!(
        kernel.may_admit_at(lineage(), Tick(100), &BUDGETS),
        Verdict::Deny(DenyReason::ClockModeUnbounded),
        "and it denies every check meanwhile, under the reason that names the configured mode \
         rather than the one that names a bad sample"
    );

    // Still true many steps later: the point of the ruling is that no fence ever arrives, not
    // that the first one is late.
    for step in 0..20u64 {
        let now = 200 + step * 10;
        let effects = kernel
            .step(&ctx_at(now, 0, 10, true), &probe(10 + step, now))
            .expect("still not an unavailable seam");
        assert_eq!(
            fences(&effects),
            Vec::<DenyReason>::new(),
            "no fence on step {step} either"
        );
    }
    assert!(kernel.state().is_held(), "and still held after 20 steps");
}

/// The other direction of the same split: a **sample** with no bound still fences, under the old
/// reason, in bounded mode.
///
/// This is the half a fix that simply stopped fencing would break. All four of
/// ADR-rdb-0007 §3's clock-bound triggers reach the fence through the no-sample arm of `utc_ok`,
/// because `absorb_sample` retracts the rejected sample and emits no fence of its own.
#[retcd_test]
fn a_rejected_sample_still_fences_clock_unbounded() {
    let mut kernel = held_kernel();

    // Bounded mode, but the sample arrives with no established bound: ADR-rdb-0007 §3's
    // "clock-bound violation fails closed" row.
    let effects = kernel
        .step(&ctx_at(100, 100, 10, false), &probe(2, 100))
        .expect("the clock-bound row is built");

    assert_eq!(
        fences(&effects),
        vec![DenyReason::ClockUnbounded],
        "a sample is an observation, and a bad one is a fence trigger — the split is between the \
         configured mode and the sample, not between fencing and not fencing"
    );
    assert!(
        kernel.state().is_fenced(),
        "node-scoped, so the state is terminal"
    );
    assert!(
        kernel.clock().sample().is_none(),
        "and the good sample is retracted with it (finding K-A-50)"
    );
}

/// A-R42's availability half, pinned. The configured mode denies, but `e_new` is guarded on the
/// sample and not the mode (K-A-07), so a fresh sample still renews. A kernel that also withheld
/// renewals in `Unbounded` mode would let the grant run into an `Expired` fence — the one the
/// ruling says never arrives. Handed back by the phase-2 manual tester: mutant M03 (`e_new`
/// returning `None` in `Unbounded` mode) passed every existing row.
#[retcd_test]
fn unbounded_clock_mode_still_dispatches_a_renewal_on_a_fresh_sample() {
    let mut kernel = held_kernel();
    kernel.set_clock_mode(ClockMode::Unbounded);
    let due = Event {
        id: EventId(3),
        at: Tick(500),
        node: NODE,
        boot: BOOT,
        partition: PARTITION,
        correlation: CorrelationId(1),
        kind: EventKind::Timer(TimerFired {
            id: AuthorityTimer::Renew.id(),
            version: kernel.timer_version(AuthorityTimer::Renew),
            scheduled_at: Tick(500),
        }),
    };

    // The preamble's sample, 500 ms old: fresh, bounded, unmoved.
    let effects = kernel
        .step(&ctx_at(500, 0, 10, true), &due)
        .expect("the RenewDue row is built");

    assert!(
        effects.iter().any(|effect| matches!(
            &effect.kind,
            EffectKind::Control(ControlEffect::Cas {
                key: ControlKey::Grant(NODE),
                ..
            })
        )),
        "unbounded mode denies admission, it does not stop renewal: {effects:?}"
    );
    assert!(
        kernel.view().renewal.is_some(),
        "and the renewal is in flight"
    );
    assert_eq!(fences(&effects), Vec::<DenyReason>::new());
}

/// A-R42 as amended by the phase-2 gate (finding F1): a **rejected** sample fences
/// `ClockUnbounded` in either mode. The configured mode decides what a *valid* sample does; it
/// does not turn a sample the clock subsystem disowned into a denial.
///
/// The tester's `finding_1e` sequence, in `Unbounded` mode: a sample 10 s behind the held one (a
/// backward jump beyond epsilon, an ADR-rdb-0007 §3 trigger), then a next sample consistent with
/// the jump, a renewal due, and a return to `Bounded`. Before the fix the jump only retracted,
/// the next sample was accepted against nothing, the renewal wrote an `E` from the jumped clock,
/// and the node admitted after returning to `Bounded` on a record whose expiry was already in
/// the true past.
#[retcd_test]
fn a_rejected_sample_fences_clock_unbounded_in_unbounded_mode_too() {
    const SKEW: u64 = 10_000;
    // A sample taken at `now` on a clock running `SKEW` ms behind the preamble's.
    let jumped = |now: u64| {
        let ctx = ctx_at(now, now, 10, true);
        StepCtx {
            control_time: ControlTime {
                estimate: Tick(1_000_000 + now - SKEW),
                ..ctx.control_time
            },
            ..ctx
        }
    };
    let mut kernel = held_kernel();
    kernel.set_clock_mode(ClockMode::Unbounded);

    let effects = kernel
        .step(&jumped(100), &probe(2, 100))
        .expect("the backward-jump row is built");
    assert_eq!(
        fences(&effects),
        vec![DenyReason::ClockUnbounded],
        "a backward jump is a rejected sample, and a rejected sample fences whatever the mode"
    );
    assert!(kernel.state().is_fenced(), "node-scoped, so terminal");
    assert!(
        kernel.clock().sample().is_none(),
        "and the jumped sample is retracted"
    );

    // The rest of `finding_1e`: nothing after the fence may bring admission back.
    kernel
        .step(&jumped(200), &probe(3, 200))
        .expect("a later sample is not an unavailable seam");
    let due = Event {
        id: EventId(4),
        at: Tick(500),
        node: NODE,
        boot: BOOT,
        partition: PARTITION,
        correlation: CorrelationId(1),
        kind: EventKind::Timer(TimerFired {
            id: AuthorityTimer::Renew.id(),
            version: kernel.timer_version(AuthorityTimer::Renew),
            scheduled_at: Tick(500),
        }),
    };
    let effects = kernel
        .step(&jumped(500), &due)
        .expect("the RenewDue row is built");
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(&effect.kind, EffectKind::Control(ControlEffect::Cas { .. }))),
        "a fenced node writes no expiry from the jumped clock: {effects:?}"
    );
    kernel.set_clock_mode(ClockMode::Bounded);
    kernel
        .step(&jumped(600), &probe(5, 600))
        .expect("the return to bounded is not an unavailable seam");
    assert_ne!(
        kernel.may_admit_at(lineage(), Tick(600), &BUDGETS),
        Verdict::Admit,
        "and returning to bounded mode admits nothing"
    );
}

/// Tester gate 3, mutant G05 (`ClockFault::Terminal` answered as the mode reason, missed by every
/// row above). A check stamped before the held sample was taken has no bound for that tick: it
/// is `ClockUnbounded`, the fence-class reason, in either mode, never the deny-only mode reason.
#[retcd_test]
fn a_check_stamped_before_the_held_sample_is_clock_unbounded_not_the_mode() {
    for mode in [ClockMode::Bounded, ClockMode::Unbounded] {
        let mut kernel = held_kernel();
        kernel.set_clock_mode(mode);
        let later = StepCtx {
            control_time: ControlTime {
                estimate: Tick(1_000_100),
                ..ctx_at(100, 100, 10, true).control_time
            },
            ..ctx_at(100, 100, 10, true)
        };
        kernel
            .step(&later, &probe(3, 100))
            .expect("a fresh sample is absorbed");
        assert!(kernel.state().is_held(), "fixture: {mode:?}");
        assert_eq!(
            kernel.clock().sample(),
            Some(later.control_time),
            "fixture: the sample at 100 is held"
        );
        assert_eq!(
            kernel.may_admit_at(lineage(), Tick(50), &BUDGETS),
            Verdict::Deny(DenyReason::ClockUnbounded),
            "{mode:?}"
        );
    }
}

// =============================================================================================
// M7A §3.4 — clock, tick and process rows (`docs/testing/test-plan-m7-kernel-a.md` §3.4).
//
// One row = one test, named by its row id. Written against the landed contract, with the plan's
// own re-words applied (§11):
// - there is no tick or clock event (A-R27): "`Tick(t)`" is the clock wake firing at `t`, and a
//   sample is `ctx.control_time`;
// - a wake re-arms, so an exact vector includes the re-arm (A-R35), and an exact vector pins the
//   sample across the step (A-R45);
// - an ignore fact is spelled as the contract spells it, `Ignored(..)` (A-R25b, A-R43, A-R44).
// Surface 1 only (KA-4): the returned effects, `state()`, `clock()` and `may_admit_at`.
// =============================================================================================

/// A context at `now` whose sample was taken at `sampled_at` and reads `ahead` ms ahead of the
/// truth, which on this fixture's scale is `1_000_000 + tick`. Unlike [`ctx_at`], the estimate
/// moves with `sampled_at`, so a later sample is consistent with the preamble's and is not a
/// backward jump.
fn sampled(now: u64, sampled_at: u64, ahead: i64, error: u64, bound: bool) -> StepCtx<'static> {
    StepCtx {
        control_time: ControlTime {
            estimate: Tick((1_000_000 + sampled_at as i64 + ahead) as u64),
            error_millis: error,
            bound_established: bound,
            sampled_at: Tick(sampled_at),
        },
        ..ctx_at(now, sampled_at, error, bound)
    }
}

/// The preamble's sample, pinned, at `now` (lead ruling A-R45).
fn pinned(now: u64) -> StepCtx<'static> {
    ctx_at(now, 0, 10, true)
}

fn event_at(id: u64, correlation: u64, kind: EventKind) -> Event {
    Event {
        id: EventId(id),
        at: Tick(id),
        node: NODE,
        boot: BOOT,
        partition: PARTITION,
        correlation: CorrelationId(correlation),
        kind,
    }
}

/// `kind` firing at tick `at`, at the version it is armed under now.
fn fired(kernel: &Authority, at: u64, kind: AuthorityTimer) -> Event {
    event_at(
        at,
        at,
        EventKind::Timer(TimerFired {
            id: kind.id(),
            version: kernel.timer_version(kind),
            scheduled_at: Tick(at),
        }),
    )
}

fn lifecycle(at: u64, lifecycle: NodeLifecycle) -> Event {
    event_at(at, at, EventKind::Node(lifecycle))
}

/// [`held_kernel`], serving `PARTITION` at epoch 1 from a snapshot at tick 1, so `may_admit_at`
/// has a lineage to admit. The preamble's sample is pinned, so the grant is unchanged.
fn serving() -> Authority {
    serving_of(&[PARTITION])
}

/// [`serving`], serving every one of `partitions` at epoch 1 from the same snapshot.
fn serving_of(partitions: &[PartitionId]) -> Authority {
    let mut kernel = held_kernel();
    let records = partitions
        .iter()
        .map(|partition| ControlRecord {
            key: ControlKey::Partition(*partition),
            revision: Revision(10),
            value: PartitionRecord {
                partition: *partition,
                owner: NODE,
                generation: Generation(1),
                owner_epoch: OwnerEpoch(1),
                config_version: ConfigVersion(1),
                lifecycle: PartitionLifecycle::Serving,
            }
            .encode(),
        })
        .collect();
    let snapshot = event_at(
        1,
        1,
        EventKind::Control(ControlEvent::FamilySnapshot {
            prefix: ControlPrefix::Partitions,
            snapshot_revision: Revision(10),
            records,
        }),
    );
    kernel
        .step(&pinned(1), &snapshot)
        .expect("the family-snapshot install is built");
    assert_eq!(
        kernel.may_admit_at(lineage(), Tick(1), &BUDGETS),
        Verdict::Admit,
        "fixture: the partition is served"
    );
    kernel
}

/// Every `Fence` in an effect vector, with its scope.
fn scoped_fences(effects: &[Effect]) -> Vec<(FenceScope, DenyReason)> {
    effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fence {
                scope,
                reason,
                ..
            })) => Some((*scope, *reason)),
            _ => None,
        })
        .collect()
}

/// Every A1 `Ignored` reason in an effect vector.
fn ignored(effects: &[Effect]) -> Vec<AuthorityIgnoreReason> {
    effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Kernel(KernelEffect::Ignored {
                reason: KernelIgnoredReason::Authority(reason),
            }) => Some(reason.clone()),
            _ => None,
        })
        .collect()
}

/// Every timer arm in an effect vector, as (kind, tick).
fn arms(effects: &[Effect]) -> Vec<(Option<AuthorityTimer>, Tick)> {
    effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Timer(TimerEffect::Arm { id, at, .. }) => {
                Some((AuthorityTimer::from_id(*id), *at))
            }
            _ => None,
        })
        .collect()
}

/// The request id of the grant CAS in flight: the acquisition while `Unheld`, else the renewal.
/// Its completion must echo it to be its answer (lead ledger L-R177hs).
fn in_flight(kernel: &Authority) -> ControlRequestId {
    kernel
        .state()
        .acquire()
        .map(|acquire| acquire.request)
        .or_else(|| kernel.view().renewal.map(|renewal| renewal.request))
        .expect("fixture: a grant CAS in flight")
}

/// The request id these rows put on a read A1 judges by its content: a read of its own grant, or
/// of a partition it did not ask for as a `Recovered` read-back. A1 matches neither by id.
const BY_CONTENT: ControlRequestId = ControlRequestId(0);

/// How many CASes on our own grant record an effect vector issues.
fn grant_cases(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|effect| {
            matches!(
                &effect.kind,
                EffectKind::Control(ControlEffect::Cas {
                    key: ControlKey::Grant(NODE),
                    ..
                })
            )
        })
        .count()
}

/// M7A-36 and M7A-37's shared input: [`serving`], then the renewal wake at 2500 dispatches a CAS
/// that never completes. `renewed_at` stays 0, so the local window lapses at 2900
/// (`2900 + delta >= grant`).
///
/// The sample taken at 2500 is precise (error 0) and reads 5 ms slow. That keeps `utc_ok`
/// holding at 2900 (`c = E - 105 < E - 0 - 100`), so the fence at 2900 is the **local** window's
/// alone. On a clock reading true, `utc_ok` fails at the same tick and the row could not tell the
/// two conjuncts apart. The 5 ms is inside both samples' epsilon (lead ruling A-R56.2), so it is
/// not a backward jump.
fn renewal_outstanding() -> Authority {
    let mut kernel = serving();
    let due = fired(&kernel, 2_500, AuthorityTimer::Renew);
    let effects = kernel
        .step(&sampled(2_500, 2_500, -5, 0, true), &due)
        .expect("the RenewDue row is built");
    assert_eq!(grant_cases(&effects), 1, "fixture: {effects:?}");
    assert!(kernel.view().renewal.is_some(), "fixture: in flight");
    assert_eq!(kernel.view().renewed_at, Some(Tick::ZERO), "fixture");
    kernel
}

/// M7A-36. §2.3 `local_ok`; ADR 0007 "expiry fences with renewal outstanding" and "expired
/// denies". The clock wake at 2900 fences `{Node, Expired}` with the renewal CAS still in flight
/// (finding K-A-02), and the CAS's later `Committed` is `LateRenewalIgnored` and nothing else.
/// Twin: M7A-37.
#[retcd_test]
fn m7a_36_tick_local_window_lapsed_fences_expired_with_renewal_outstanding() {
    let mut kernel = renewal_outstanding();
    let renewal = in_flight(&kernel);
    let wake = fired(&kernel, 2_900, AuthorityTimer::ClockWake);
    let effects = kernel
        .step(&sampled(2_900, 2_500, -5, 0, true), &wake)
        .expect("the clock wake is built");

    assert_eq!(
        scoped_fences(&effects),
        vec![(FenceScope::Node, DenyReason::Expired)],
        "{effects:?}"
    );
    assert!(kernel.state().is_fenced());
    assert_eq!(
        kernel.may_admit_at(lineage(), Tick(2_900), &BUDGETS),
        Verdict::Deny(DenyReason::Expired)
    );

    let seq = kernel.authority_seq();
    let late = event_at(
        2_950,
        2_500,
        EventKind::Control(ControlEvent::CasResult {
            request: renewal,
            key: ControlKey::Grant(NODE),
            outcome: CasOutcome::Committed(Revision(8)),
        }),
    );
    let effects = kernel
        .step(&sampled(2_950, 2_500, -5, 0, true), &late)
        .expect("the late completion is built");
    assert_eq!(
        ignored(&effects),
        vec![AuthorityIgnoreReason::LateRenewalIgnored],
        "{effects:?}"
    );
    assert_eq!(effects.len(), 1, "nothing else: {effects:?}");
    assert!(kernel.state().is_fenced(), "no resurrection");
    assert_eq!(kernel.authority_seq(), seq);
}

/// M7A-37, the twin of M7A-36. One fact: the wake fires at 2899, one tick before the local window
/// lapses. No fence, and the check admits.
#[retcd_test]
fn m7a_37_tick_local_window_one_before_lapse_admits() {
    let mut kernel = renewal_outstanding();
    let wake = fired(&kernel, 2_899, AuthorityTimer::ClockWake);
    let effects = kernel
        .step(&sampled(2_899, 2_500, -5, 0, true), &wake)
        .expect("the clock wake is built");

    assert_eq!(scoped_fences(&effects), vec![], "{effects:?}");
    assert!(kernel.state().is_held());
    assert_eq!(
        kernel.may_admit_at(lineage(), Tick(2_899), &BUDGETS),
        Verdict::Admit
    );
}

/// M7A-38. A-R12 "epsilon over bound denies and fences"; §2.3 `effective_epsilon` Terminal; ADR
/// 0007 clock-bound case 1. A sample with error 101 against a ceiling of 100 fences
/// `{Node, ClockUnbounded}` and is retracted. Twin: M7A-39 (at the bound).
///
/// The plan's "`clock.mode == Unbounded`" predates lead ruling A-R42: the mode is configuration
/// and a bad sample does not change it. What a bad sample changes is the sample, which is
/// retracted (K-A-50). The row asserts both.
#[retcd_test]
fn m7a_38_clock_epsilon_over_bound_by_one_fences_clock_unbounded() {
    let mut kernel = serving();
    let over = BUDGETS.clock_error_millis + 1;
    let wake = fired(&kernel, 100, AuthorityTimer::ClockWake);
    let effects = kernel
        .step(&sampled(100, 100, 0, over, true), &wake)
        .expect("the clock row is built");

    assert_eq!(
        scoped_fences(&effects),
        vec![(FenceScope::Node, DenyReason::ClockUnbounded)],
        "{effects:?}"
    );
    assert!(kernel.state().is_fenced());
    assert_eq!(kernel.clock().sample(), None, "retracted (K-A-50)");
    assert_eq!(
        kernel.clock().mode(),
        ClockMode::Bounded,
        "the mode is configuration (A-R42)"
    );
}

/// M7A-39, the twin of M7A-38. ADR 0007 "sample epsilon used (at bound, wider margin)"; §2.3
/// `utc_ok`. Two kernels, one fact: the sample's error, 100 (at the ceiling) or 10.
///
/// Both hold `E = 1_003_000`. Each takes a fresh sample at 1000 and is queried at 2800, where
/// `c_now = E - 200`. With error 100 the margin is `100 + delta = 200`, so it denies `Expired`.
/// With error 10 it admits. Neither fences: the at-bound sample is valid.
#[retcd_test]
fn m7a_39_clock_epsilon_at_bound_admits_with_narrower_window() {
    let at_error = |error: u64| {
        let mut kernel = serving();
        let effects = kernel
            .step(&sampled(1_000, 1_000, 0, error, true), &probe(2, 1_000))
            .expect("the clock row is built");
        assert_eq!(
            scoped_fences(&effects),
            vec![],
            "error {error}: {effects:?}"
        );
        assert!(kernel.state().is_held(), "error {error}");
        kernel.may_admit_at(lineage(), Tick(2_800), &BUDGETS)
    };

    assert_eq!(
        at_error(BUDGETS.clock_error_millis),
        Verdict::Deny(DenyReason::Expired),
        "at the bound: a narrower window"
    );
    assert_eq!(at_error(10), Verdict::Admit);
}

/// M7A-40. ADR 0007 clock-bound case 2; ADR 0007 "a rejected sample retracts the good one"
/// (K-A-50). Held on a good sample from tick 0; the wake at 100 carries a sample with no
/// established bound. It fences `{Node, ClockUnbounded}` and the good sample is gone.
///
/// Twin in the same test, entered `Fenced` (a reboot under another boot id): the same rejected
/// sample is `Ignored(SampleRejected)`, no fence, and it still leaves no sample.
#[retcd_test]
fn m7a_40_clock_sample_invalid_fences_and_retracts_the_good_sample() {
    let mut kernel = serving();
    assert!(kernel.clock().sample().is_some(), "fixture: a good sample");
    let wake = fired(&kernel, 100, AuthorityTimer::ClockWake);
    let effects = kernel
        .step(&sampled(100, 100, 0, 10, false), &wake)
        .expect("the clock row is built");
    assert_eq!(
        scoped_fences(&effects),
        vec![(FenceScope::Node, DenyReason::ClockUnbounded)],
        "{effects:?}"
    );
    assert_eq!(kernel.clock().sample(), None, "the good sample is gone");

    let mut fenced = serving();
    fenced
        .step(
            &pinned(50),
            &lifecycle(50, NodeLifecycle::Rebooted { boot: BootId(2) }),
        )
        .expect("the reboot row is built");
    assert!(fenced.state().is_fenced(), "fixture");
    assert!(fenced.clock().sample().is_some(), "fixture: sample kept");
    let effects = fenced
        .step(&sampled(100, 100, 0, 10, false), &probe(3, 100))
        .expect("the clock row is built");
    assert_eq!(scoped_fences(&effects), vec![], "{effects:?}");
    assert!(
        ignored(&effects).contains(&AuthorityIgnoreReason::SampleRejected),
        "{effects:?}"
    );
    assert_eq!(fenced.clock().sample(), None);
}

/// M7A-41. ADR 0007 clock-bound case 3; K-A-50; §2.3 "future-stamped ⇒ Terminal". A sample
/// stamped one tick after `now` fences `{Node, ClockUnbounded}` and is retracted.
///
/// Twin in the same test, on a second kernel. One fact: the stamp is `now`, so the sample is
/// adopted and nothing fences.
#[retcd_test]
fn m7a_41_clock_sample_future_stamped_fences_and_retracts() {
    let stamped = |sampled_at: u64| {
        let mut kernel = serving();
        let ctx = sampled(100, sampled_at, 0, 10, true);
        let wake = fired(&kernel, 100, AuthorityTimer::ClockWake);
        let effects = kernel.step(&ctx, &wake).expect("the clock row is built");
        (kernel, ctx.control_time, scoped_fences(&effects))
    };

    let (kernel, _, fences) = stamped(101);
    assert_eq!(fences, vec![(FenceScope::Node, DenyReason::ClockUnbounded)]);
    assert_eq!(kernel.clock().sample(), None);

    let (kernel, sample, fences) = stamped(100);
    assert_eq!(fences, vec![]);
    assert_eq!(kernel.clock().sample(), Some(sample), "adopted");
    assert!(kernel.state().is_held());
}

/// M7A-42. ADR 0007 clock-bound case 4; K-A-50; §3 "backward jump". Held on the sample taken at
/// tick 0 reading `1_000_000` (the plan's `5_000_000`, on this file's scale). At tick 500 a sample
/// reads `990_000` (the plan's `4_990_000`), 10.5 s behind where the held sample puts the clock.
/// It fences `{Node, ClockUnbounded}` and is retracted.
///
/// Twin in the same test, on a second kernel. One fact: the sample reads `1_000_400` (the plan's
/// `5_000_400`), 100 ms behind. Since lead ruling A-R56.2 the tolerance is both samples' epsilon:
/// 10 for the held one, 100 for this one (at the ceiling, still valid). 100 is inside 110, so it
/// is adopted. Both readings carry error 100, so they differ in the reading alone.
///
/// Unavailable half (lead ruling A-R57): "`AcquireDue` at tick 600 after the fence is cleared".
/// A node fence has no exit in this build, so there is nothing to clear. Owed, not asserted weaker.
#[retcd_test]
fn m7a_42_clock_backward_jump_fences_and_retracts() {
    let reading = |ahead: i64| {
        let mut kernel = serving();
        let ctx = sampled(500, 500, ahead, BUDGETS.clock_error_millis, true);
        let wake = fired(&kernel, 500, AuthorityTimer::ClockWake);
        let effects = kernel.step(&ctx, &wake).expect("the clock row is built");
        (kernel, ctx.control_time, scoped_fences(&effects))
    };

    let (kernel, _, fences) = reading(-10_500);
    assert_eq!(fences, vec![(FenceScope::Node, DenyReason::ClockUnbounded)]);
    assert!(kernel.state().is_fenced());
    assert_eq!(kernel.clock().sample(), None, "retracted (K-A-50)");

    let (kernel, sample, fences) = reading(-100);
    assert_eq!(fences, vec![]);
    assert_eq!(kernel.clock().sample(), Some(sample), "adopted");
    assert!(kernel.state().is_held());
}

/// M7A-43. A-R12 "stale sample denies only"; §2.3 Stale; ADR 0007 "stale sample denies without
/// fencing"; K-A-42. The last sample was taken at 0 and nothing renewed. The wake at 2001 (age
/// 2001 > 2000) answers exactly `[Ignored(AdmissionSuspended), Arm(ClockWake, 2501)]`: the re-arm
/// is part of the vector (A-R35), and the pinned sample publishes nothing (A-R45). No fence; still
/// `Held`; the check denies `ClockSampleStale`.
///
/// The observation window is 2001..2899 (rule A5). At 2899 there is still no fence. At 2900 the
/// fence that comes is the local window's `Expired`, not staleness. Twin: M7A-44.
#[retcd_test]
fn m7a_43_clock_sample_stale_denies_admission_suspended_no_fence() {
    let mut kernel = serving();
    let wake = fired(&kernel, 2_001, AuthorityTimer::ClockWake);
    let effects = kernel.step(&pinned(2_001), &wake).expect("the wake");

    assert_eq!(
        ignored(&effects),
        vec![AuthorityIgnoreReason::AdmissionSuspended]
    );
    assert_eq!(
        arms(&effects),
        vec![(Some(AuthorityTimer::ClockWake), Tick(2_501))]
    );
    assert_eq!(effects.len(), 2, "nothing else: {effects:?}");
    assert!(kernel.state().is_held());
    assert_eq!(
        kernel.may_admit_at(lineage(), Tick(2_001), &BUDGETS),
        Verdict::Deny(DenyReason::ClockSampleStale)
    );

    let effects = kernel.step(&pinned(2_899), &probe(3, 2_899)).expect("2899");
    assert_eq!(scoped_fences(&effects), vec![], "{effects:?}");
    let effects = kernel.step(&pinned(2_900), &probe(4, 2_900)).expect("2900");
    assert_eq!(
        scoped_fences(&effects),
        vec![(FenceScope::Node, DenyReason::Expired)],
        "{effects:?}"
    );
}

/// M7A-44, the twin of M7A-43. One fact: a valid sample (taken at 2002, error 20) arrives after
/// the stale wake. The check admits again at 2003. The grant is the one acquired at 0: no grant id
/// was consumed.
///
/// "Zero CAS while stale" is M7A-45's rule, so the stale phase also delivers the renewal wake.
/// Without it that assertion could not fail: nothing else in the row asks for a CAS.
#[retcd_test]
fn m7a_44_clock_sample_fresh_after_stale_resumes_admission() {
    let mut kernel = serving();
    let grant = kernel.held().map(|held| held.identity());
    let mut effects = Vec::new();
    for timer in [AuthorityTimer::ClockWake, AuthorityTimer::Renew] {
        let wake = fired(&kernel, 2_001, timer);
        effects.extend(kernel.step(&pinned(2_001), &wake).expect("the wake"));
    }
    assert_eq!(
        kernel.may_admit_at(lineage(), Tick(2_001), &BUDGETS),
        Verdict::Deny(DenyReason::ClockSampleStale),
        "fixture: stale"
    );

    effects.extend(
        kernel
            .step(&sampled(2_002, 2_002, 0, 20, true), &probe(3, 2_002))
            .expect("the fresh sample"),
    );
    assert_eq!(
        kernel.may_admit_at(lineage(), Tick(2_003), &BUDGETS),
        Verdict::Admit
    );
    assert_eq!(grant_cases(&effects), 0, "{effects:?}");
    assert_eq!(kernel.held().map(|held| held.identity()), grant);
}

/// M7A-45. §2.3's renewal guard: `e_new` is `None` — the sample decides, not the mode (round 2,
/// K-A-36). Two parts:
/// - Stale, as M7A-43: the renewal wake issues no CAS and answers `Ignored(RenewalWithheld)`.
/// - `Unbounded` mode **with** a valid sample: the renewal CAS is issued. There is no
///   withhold-expire-fence loop.
#[retcd_test]
fn m7a_45_no_cas_issued_on_invalid_or_stale_sample() {
    let mut kernel = serving();
    let due = fired(&kernel, 2_001, AuthorityTimer::Renew);
    let effects = kernel.step(&pinned(2_001), &due).expect("RenewDue");
    assert_eq!(grant_cases(&effects), 0, "stale: {effects:?}");
    assert!(
        ignored(&effects).contains(&AuthorityIgnoreReason::RenewalWithheld),
        "{effects:?}"
    );
    assert!(kernel.view().renewal.is_none());

    let mut kernel = serving();
    kernel.set_clock_mode(ClockMode::Unbounded);
    let due = fired(&kernel, 500, AuthorityTimer::Renew);
    let effects = kernel.step(&pinned(500), &due).expect("RenewDue");
    assert_eq!(grant_cases(&effects), 1, "unbounded, valid: {effects:?}");
    assert!(kernel.view().renewal.is_some());
    assert_eq!(scoped_fences(&effects), vec![]);
}

/// M7A-46. K-A-38: effective epsilon is `error + age * ppm / 1e6`, **plus**, not `max`.
///
/// Held on `E = 1_003_000`. At tick 2 a sample taken at 1 arrives, error 50, reading 848 ms ahead
/// (a forward step, accepted). At tick 2001 its age is exactly 2000: not stale, and drift adds
/// 1 ms, so epsilon is 51. There `c_now = E - 151 = E - 51 - delta`, so it denies `Expired`.
/// Under `max(50, 1)` it would admit: that is the one-fact difference.
///
/// Twin in the same test, on a second kernel. One fact: the sample reads 847 ahead, so
/// `c_now = E - 52 - delta`, and it admits.
#[retcd_test]
fn m7a_46_clock_effective_epsilon_grows_by_plus_not_max() {
    let at_ahead = |ahead: i64| {
        let mut kernel = serving();
        let effects = kernel
            .step(&sampled(2, 1, ahead, 50, true), &probe(2, 2))
            .expect("the clock row is built");
        assert_eq!(scoped_fences(&effects), vec![], "fixture: {effects:?}");
        assert_eq!(kernel.view().expiry_utc_ms, Some(1_003_000), "fixture: E");
        kernel.may_admit_at(lineage(), Tick(2_001), &BUDGETS)
    };

    assert_eq!(at_ahead(848), Verdict::Deny(DenyReason::Expired));
    assert_eq!(at_ahead(847), Verdict::Admit);
}

/// M7A-47. §2.4 `ProcessResumed gap > tolerance ⇒ fence`; ADR 0007 "pause/suspend"; charter
/// "pause/suspend fail closed". A resume after `resume_gap_tolerance_millis + 1` fences
/// `{Node, ProcessSuspended}`. Twin: M7A-48.
#[retcd_test]
fn m7a_47_process_resumed_gap_over_tolerance_fences() {
    let mut kernel = serving();
    let gap = BUDGETS.resume_gap_tolerance_millis + 1;
    let effects = kernel
        .step(
            &pinned(100),
            &lifecycle(
                100,
                NodeLifecycle::Resumed {
                    suspended_millis: gap,
                },
            ),
        )
        .expect("the lifecycle row is built");
    assert_eq!(
        scoped_fences(&effects),
        vec![(FenceScope::Node, DenyReason::ProcessSuspended)],
        "{effects:?}"
    );
    assert!(kernel.state().is_fenced());
}

/// M7A-48, the twin of M7A-47. One fact: the gap is exactly the tolerance. No fence; still
/// `Held`; the answer names the gap (A-R43).
#[retcd_test]
fn m7a_48_process_resumed_gap_within_tolerance_no_fence() {
    let mut kernel = serving();
    let gap = BUDGETS.resume_gap_tolerance_millis;
    let effects = kernel
        .step(
            &pinned(100),
            &lifecycle(
                100,
                NodeLifecycle::Resumed {
                    suspended_millis: gap,
                },
            ),
        )
        .expect("the lifecycle row is built");
    assert_eq!(scoped_fences(&effects), vec![], "{effects:?}");
    assert_eq!(
        ignored(&effects),
        vec![AuthorityIgnoreReason::ResumeGapWithinTolerance]
    );
    assert!(kernel.state().is_held());
}

/// M7A-49. §2.4 `BootObserved mismatch ⇒ fence`; ADR 0007 §3 "reboot / boot UUID". A reboot
/// notice under another boot id fences `{Node, BootMismatch}`.
///
/// Twin in the same test, on a second kernel. One fact: the same boot id, which is
/// `Ignored(BootUnchanged)` and nothing else (A-R43).
#[retcd_test]
fn m7a_49_boot_observed_mismatch_fences_boot_mismatch() {
    let rebooted = |boot: BootId| {
        let mut kernel = serving();
        let effects = kernel
            .step(
                &pinned(100),
                &lifecycle(100, NodeLifecycle::Rebooted { boot }),
            )
            .expect("the lifecycle row is built");
        (kernel, effects)
    };

    let (kernel, effects) = rebooted(BootId(2));
    assert_eq!(
        scoped_fences(&effects),
        vec![(FenceScope::Node, DenyReason::BootMismatch)],
        "{effects:?}"
    );
    assert!(kernel.state().is_fenced());

    let (kernel, effects) = rebooted(BOOT);
    assert_eq!(scoped_fences(&effects), vec![], "{effects:?}");
    assert_eq!(
        ignored(&effects),
        vec![AuthorityIgnoreReason::BootUnchanged]
    );
    assert_eq!(effects.len(), 1, "nothing else: {effects:?}");
    assert!(kernel.state().is_held());
}

/// Every `DenyReason`, and whether a fence can carry it: the contract's declared split (lead
/// rulings A-R33b, A-R42). The match has no wildcard, so a new variant fails to compile here
/// until someone decides which side it is on (KA-7).
fn fence_reachable(reason: DenyReason) -> bool {
    use DenyReason::*;
    match reason {
        Frozen
        | Revoked
        | EpochRevoked
        | Expired
        | ClockUnbounded
        | ProcessSuspended
        | BootMismatch
        | AuthorityGenerationChanged
        | GenerationChanged
        | LocalStorageFenced => true,
        NoGrant | ExpiryUnproven | ClockModeUnbounded | ClockSampleStale | SelfFenced
        | ControlUnavailable => false,
    }
}

/// All sixteen variants, listed once. [`fence_reachable`] keeps it complete: a variant missing
/// here is still a compile error there.
const ALL_REASONS: [DenyReason; 16] = [
    DenyReason::NoGrant,
    DenyReason::Frozen,
    DenyReason::Revoked,
    DenyReason::EpochRevoked,
    DenyReason::Expired,
    DenyReason::ExpiryUnproven,
    DenyReason::ClockUnbounded,
    DenyReason::ClockModeUnbounded,
    DenyReason::ClockSampleStale,
    DenyReason::ProcessSuspended,
    DenyReason::BootMismatch,
    DenyReason::AuthorityGenerationChanged,
    DenyReason::GenerationChanged,
    DenyReason::SelfFenced,
    DenyReason::ControlUnavailable,
    DenyReason::LocalStorageFenced,
];

/// A linearizable read of our own `grants/{node}` record at tick 100.
fn grant_read(outcome: ReadOutcome) -> Event {
    event_at(
        100,
        100,
        EventKind::Control(ControlEvent::Value {
            request: BY_CONTENT,
            key: ControlKey::Grant(NODE),
            outcome,
        }),
    )
}

/// Our grant record as the kernel holds it, changed by `change`, read at a newer revision.
fn our_record_but(kernel: &Authority, change: fn(&mut GrantRecord)) -> Event {
    let held = kernel.held().expect("fixture: Held").identity();
    let mut record = GrantRecord {
        grant: held.grant,
        node: NODE,
        boot: held.boot,
        authority_generation: held.authority_generation,
        expiry_utc_ms: 1_003_000,
        frozen: false,
    };
    change(&mut record);
    grant_read(ReadOutcome::Found {
        revision: Revision(8),
        value: record.encode(),
    })
}

/// One fence trigger: its name, the tick it fires at, the one fence it must emit, and the input,
/// applied at that tick to a fresh [`serving`] kernel. The tick is a column so a row about the
/// fence's view can name it without restating it.
type Trigger = (
    &'static str,
    u64,
    (FenceScope, DenyReason),
    fn(&mut Authority, u64) -> Vec<Effect>,
);

/// ADR 0007 §3's trigger table, one row per distinct trigger this build has (A-R33 counted the
/// design's `Fence{` sites; this also splits the two expiry conjuncts and the record's boot).
fn triggers() -> Vec<Trigger> {
    use DenyReason::*;
    let node = FenceScope::Node;
    let p1 = FenceScope::Partition(PARTITION);
    vec![
        (
            "sample error over the ceiling",
            100,
            (node, ClockUnbounded),
            |k, t| {
                let over = BUDGETS.clock_error_millis + 1;
                k.step(&sampled(t, t, 0, over, true), &probe(3, t))
                    .expect("built")
            },
        ),
        (
            "sample with no bound",
            100,
            (node, ClockUnbounded),
            |k, t| {
                k.step(&sampled(t, t, 0, 10, false), &probe(3, t))
                    .expect("built")
            },
        ),
        (
            "sample stamped in the future",
            100,
            (node, ClockUnbounded),
            |k, t| {
                k.step(&sampled(t, t + 1, 0, 10, true), &probe(3, t))
                    .expect("built")
            },
        ),
        ("backward jump", 500, (node, ClockUnbounded), |k, t| {
            k.step(&sampled(t, t, -10_500, 10, true), &probe(3, t))
                .expect("built")
        }),
        (
            "resume after a suspension",
            100,
            (node, ProcessSuspended),
            |k, t| {
                let gap = BUDGETS.resume_gap_tolerance_millis + 1;
                let resumed = NodeLifecycle::Resumed {
                    suspended_millis: gap,
                };
                k.step(&pinned(t), &lifecycle(t, resumed)).expect("built")
            },
        ),
        (
            "reboot under another boot id",
            100,
            (node, BootMismatch),
            |k, t| {
                let rebooted = NodeLifecycle::Rebooted { boot: BootId(2) };
                k.step(&pinned(t), &lifecycle(t, rebooted)).expect("built")
            },
        ),
        ("grant record frozen", 100, (node, Frozen), |k, t| {
            let read = our_record_but(k, |record| record.frozen = true);
            k.step(&pinned(t), &read).expect("built")
        }),
        (
            "grant record names another boot",
            100,
            (node, BootMismatch),
            |k, t| {
                let read = our_record_but(k, |record| record.boot = BootId(2));
                k.step(&pinned(t), &read).expect("built")
            },
        ),
        (
            "grant record names another grant",
            100,
            (node, Revoked),
            |k, t| {
                let read = our_record_but(k, |record| record.grant = GrantId(record.grant.0 + 1));
                k.step(&pinned(t), &read).expect("built")
            },
        ),
        ("grant record absent", 100, (node, Revoked), |k, t| {
            let read = grant_read(ReadOutcome::Absent { as_of: Revision(8) });
            k.step(&pinned(t), &read).expect("built")
        }),
        (
            "authority generation moved",
            100,
            (node, AuthorityGenerationChanged),
            |k, t| {
                let read = our_record_but(k, |record| {
                    record.authority_generation =
                        AuthorityGeneration(record.authority_generation.0 + 1);
                });
                k.step(&pinned(t), &read).expect("built")
            },
        ),
        ("local window lapsed", 2_900, (node, Expired), |k, t| {
            k.step(&pinned(t), &probe(3, t)).expect("built")
        }),
        (
            "authority clock past E - eps - delta",
            1_000,
            (node, Expired),
            |k, t| {
                k.step(&sampled(t, t, 1_900, 10, true), &probe(3, t))
                    .expect("built")
            },
        ),
        (
            "epoch revocation persisted",
            100,
            (p1, EpochRevoked),
            |k, t| {
                let persisted = AuthorityEvent::EpochRevocationPersisted {
                    partition: PARTITION,
                    epoch: OwnerEpoch(1),
                };
                let event = event_at(t, t, EventKind::Kernel(KernelEvent::Authority(persisted)));
                k.step(&pinned(t), &event).expect("built")
            },
        ),
        (
            "local storage write failed",
            100,
            (p1, LocalStorageFenced),
            |k, t| {
                let failed = StorageEvent::CommitFailed {
                    batch: BatchId(1),
                    fault: StorageFault::WriteFailed,
                };
                let event = event_at(t, t, EventKind::Storage(failed));
                k.step(&pinned(t), &event).expect("built")
            },
        ),
        (
            "snapshot drops the partition",
            100,
            (p1, GenerationChanged),
            |k, t| {
                let snapshot = ControlEvent::FamilySnapshot {
                    prefix: ControlPrefix::Partitions,
                    snapshot_revision: Revision(11),
                    records: Vec::new(),
                };
                let event = event_at(t, t, EventKind::Control(snapshot));
                k.step(&pinned(t), &event).expect("built")
            },
        ),
        (
            "read finds the record absent",
            100,
            (p1, GenerationChanged),
            |k, t| {
                let read = ControlEvent::Value {
                    request: BY_CONTENT,
                    key: ControlKey::Partition(PARTITION),
                    outcome: ReadOutcome::Absent {
                        as_of: Revision(11),
                    },
                };
                let event = event_at(t, t, EventKind::Control(read));
                k.step(&pinned(t), &event).expect("built")
            },
        ),
    ]
}

/// M7A-50. ADR 0007 §3's trigger table with its Scope column; KA-7; lead rulings A-R33, A-R33b,
/// A-R42, A-R44.
///
/// **Per trigger**, on a fresh `Held` kernel serving `PARTITION`: exactly one `Fence`, with the
/// table's scope and reason. A `Node` fence leaves the state `Fenced`; a `Partition` fence leaves
/// it `Held`.
///
/// **The scope split, by value (A-R33):** seven reasons fence the node (`Revoked`, `Frozen`,
/// `BootMismatch`, `AuthorityGenerationChanged`, `Expired`, `ClockUnbounded`, `ProcessSuspended`)
/// and three fence one partition (`LocalStorageFenced`, `EpochRevoked`, `GenerationChanged`). The
/// function keeps the plan's name; A-R33 withdrew its "two partition", and the row asserts three.
///
/// **The split, by value:** 16 variants, 10 fence-reachable, 6 deny-only. The reasons the triggers
/// reach are exactly the declared fence-reachable ten, so the split is checked in both directions:
/// a deny-only reason that became a fence, or a fence reason that lost its trigger.
///
/// **The deny-only six, with A-R44's caveats:**
/// - (a) `SelfFenced`, `ControlUnavailable` and `ExpiryUnproven` have no producer in this build.
///   For them "never a fence" cannot be falsified, so the row calls them unreachable and asserts
///   nothing about them beyond the split.
/// - (b) `NoGrant` and `ClockSampleStale` are produced, as denials, and the row shows that.
/// - (c) `ClockModeUnbounded` is produced and falsifiable. The A-R42 guard above is its standing
///   guard; the row asserts the denial here too.
#[retcd_test]
fn m7a_50_fence_scope_table_seven_node_two_partition() {
    let (mut reached, mut node, mut partition) =
        (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    for (name, t, (scope, reason), trigger) in triggers() {
        let mut kernel = serving();
        let effects = trigger(&mut kernel, t);
        assert_eq!(
            scoped_fences(&effects),
            vec![(scope, reason)],
            "{name}: {effects:?}"
        );
        reached.insert(reason);
        match scope {
            FenceScope::Node => {
                assert!(kernel.state().is_fenced(), "{name}");
                node.insert(reason);
            }
            FenceScope::Partition(_) => {
                assert!(kernel.state().is_held(), "{name}");
                partition.insert(reason);
            }
        }
    }

    let (fenceable, deny_only): (BTreeSet<_>, BTreeSet<_>) = ALL_REASONS
        .into_iter()
        .partition(|reason| fence_reachable(*reason));
    assert_eq!(
        (
            ALL_REASONS.iter().collect::<BTreeSet<_>>().len(),
            fenceable.len(),
            deny_only.len()
        ),
        (16, 10, 6)
    );
    assert_eq!(
        reached, fenceable,
        "every fence reason has a trigger, and nothing else fences"
    );
    assert_eq!(
        (node.len(), partition.len()),
        (7, 3),
        "{node:?} {partition:?}"
    );
    assert!(node.is_disjoint(&partition), "one scope per reason");
    assert_eq!(
        deny_only,
        BTreeSet::from([
            DenyReason::NoGrant,
            DenyReason::ExpiryUnproven,
            DenyReason::ClockModeUnbounded,
            DenyReason::ClockSampleStale,
            DenyReason::SelfFenced,
            DenyReason::ControlUnavailable,
        ])
    );

    // (b) and (c): the produced deny-only reasons, each a denial and not a fence.
    assert_eq!(
        Authority::new().may_admit_at(lineage(), Tick(0), &BUDGETS),
        Verdict::Deny(DenyReason::NoGrant)
    );
    assert_eq!(
        serving().may_admit_at(lineage(), Tick(2_001), &BUDGETS),
        Verdict::Deny(DenyReason::ClockSampleStale)
    );
    let mut unbounded = serving();
    unbounded.set_clock_mode(ClockMode::Unbounded);
    let effects = unbounded.step(&pinned(100), &probe(3, 100)).expect("built");
    assert_eq!(scoped_fences(&effects), vec![]);
    assert_eq!(
        unbounded.may_admit_at(lineage(), Tick(100), &BUDGETS),
        Verdict::Deny(DenyReason::ClockModeUnbounded)
    );
}

/// The grant id of every create-only CAS on our grant record in `effects`.
fn created_grants(effects: &[Effect]) -> Vec<GrantId> {
    effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Control(ControlEffect::Cas {
                key: ControlKey::Grant(NODE),
                expected: None,
                value,
                ..
            }) => value
                .as_deref()
                .and_then(GrantRecord::decode)
                .map(|record| record.grant),
            _ => None,
        })
        .collect()
}

/// M7A-24. ADR 0007 "Unbounded mode does not burn grant ids", as amended in round 2 (with a valid
/// sample, exactly one id); `design.md` §2.3; K-A-07; the checkpoint re-pointed by A-R42.
///
/// An `Unheld` kernel in `ClockMode::Unbounded` with a valid, fresh sample at every step:
/// `AcquireDue`, its commit, then 20 renewal intervals of `RenewDue`, its commit and a tick
/// between. Exactly **one** create-only CAS in the whole run, so one grant id; and at every step
/// once `Held`, the check answers `Deny(ClockModeUnbounded)` — the configured mode, which denies
/// and never fences. The no-sample condition is M7A-148's.
#[retcd_test]
fn m7a_24_unbounded_mode_does_not_burn_grant_ids() {
    let mut kernel = Authority::new();
    kernel.set_clock_mode(ClockMode::Unbounded);
    let mut created = Vec::new();
    let mut step = |kernel: &mut Authority, now: u64, event: &Event| {
        let effects = kernel
            .step(&sampled(now, now, 0, 10, true), event)
            .expect("built");
        assert_eq!(scoped_fences(&effects), vec![], "at {now}");
        created.extend(created_grants(&effects));
        // Checked at the step's own tick: a check stamped before the held sample is a different
        // row (`ClockUnbounded`, not the mode).
        if kernel.state().is_held() {
            assert_eq!(
                kernel.may_admit_at(lineage(), Tick(now), &BUDGETS),
                Verdict::Deny(DenyReason::ClockModeUnbounded),
                "at {now}"
            );
        }
    };
    let committed = |request: ControlRequestId, at: u64, correlation: u64, revision: u64| {
        event_at(
            at,
            correlation,
            EventKind::Control(ControlEvent::CasResult {
                request,
                key: ControlKey::Grant(NODE),
                outcome: CasOutcome::Committed(Revision(revision)),
            }),
        )
    };

    let acquire = fired(&kernel, 10, AuthorityTimer::Acquire);
    step(&mut kernel, 10, &acquire);
    let request = in_flight(&kernel);
    step(&mut kernel, 11, &committed(request, 11, 10, 7));
    assert!(kernel.state().is_held(), "fixture: acquired");
    for i in 1..=20 {
        let due_at = 10 + BUDGETS.renew_millis * i;
        let renew = fired(&kernel, due_at, AuthorityTimer::Renew);
        step(&mut kernel, due_at, &renew);
        let request = in_flight(&kernel);
        step(
            &mut kernel,
            due_at + 1,
            &committed(request, due_at + 1, due_at, 7 + i),
        );
        step(&mut kernel, due_at + 250, &probe(3, due_at + 250));
    }
    assert_eq!(created.len(), 1, "one create-only CAS: {created:?}");
    assert!(kernel.state().is_held());
}

/// `Unheld`, holding the good sample taken at tick 0 and nothing else.
fn unheld_with_a_sample() -> Authority {
    let mut kernel = Authority::new();
    let effects = kernel.step(&pinned(0), &probe(1, 0)).expect("built");
    assert_eq!(effects, vec![], "fixture: an accepted sample says nothing");
    assert!(kernel.clock().sample().is_some(), "fixture: a good sample");
    kernel
}

/// [`serving`], then fenced by a reboot under another boot id; the sample is kept.
fn fenced_with_a_sample() -> Authority {
    let mut kernel = serving();
    kernel
        .step(
            &pinned(50),
            &lifecycle(50, NodeLifecycle::Rebooted { boot: BootId(2) }),
        )
        .expect("the reboot row is built");
    assert!(kernel.state().is_fenced(), "fixture");
    assert!(kernel.clock().sample().is_some(), "fixture: a good sample");
    kernel
}

/// Every A1 `Fact` in an effect vector.
fn facts(effects: &[Effect]) -> Vec<&Effect> {
    effects
        .iter()
        .filter(|effect| {
            matches!(
                effect.kind,
                EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fact(_)))
            )
        })
        .collect()
}

/// M7A-165. ADR 0007 "A rejected sample retracts the good one"; K-A-50; `design.md` §2.4
/// `Unheld|Fenced / Clock(s)` reject row.
///
/// Three sub-runs, one per state, each holding a good sample from before tick 100 and then
/// stepped at 100 on a sample with no established bound. `Unheld` and `Fenced`: the whole effect
/// vector is the one rejection, no fence, and the good sample is gone. `Held`: the node fence
/// `ClockUnbounded`, and the good sample is gone too. Near-miss twin in every state (one fact:
/// the bound is established): the sample is adopted, and there is no fence, no rejection and no
/// fact.
///
/// Landed spelling: the rejection is `Ignored(SampleRejected)`, an ignore reason with no
/// `{Invalid}` sub-reason — the contract has one variant for every rejection cause.
#[retcd_test]
fn m7a_165_rejected_sample_retracts_the_good_one() {
    let states = [
        ("Unheld", unheld_with_a_sample as fn() -> Authority),
        ("Fenced", fenced_with_a_sample),
        ("Held", serving),
    ];
    for (state, build) in states {
        let mut kernel = build();
        let effects = kernel
            .step(&sampled(100, 100, 0, 10, false), &probe(3, 100))
            .expect("the clock row is built");
        if state == "Held" {
            assert_eq!(
                scoped_fences(&effects),
                vec![(FenceScope::Node, DenyReason::ClockUnbounded)],
                "{state}: {effects:?}"
            );
        } else {
            assert_eq!(effects.len(), 1, "{state}: {effects:?}");
            assert_eq!(
                ignored(&effects),
                vec![AuthorityIgnoreReason::SampleRejected],
                "{state}"
            );
        }
        assert_eq!(
            kernel.clock().sample(),
            None,
            "{state}: the good sample is gone"
        );

        let mut twin = build();
        let good = sampled(100, 100, 0, 10, true);
        let effects = twin.step(&good, &probe(3, 100)).expect("built");
        assert_eq!(scoped_fences(&effects), vec![], "{state} twin: {effects:?}");
        assert_eq!(ignored(&effects), vec![], "{state} twin: {effects:?}");
        assert!(facts(&effects).is_empty(), "{state} twin: {effects:?}");
        assert_eq!(
            twin.clock().sample(),
            Some(good.control_time),
            "{state} twin: adopted"
        );
    }
}

/// A second served partition, for the rows about a fence's scope.
const SECOND: PartitionId = PartitionId(2);

/// Every view published in an effect vector, in order.
fn published(effects: &[Effect]) -> Vec<AuthorityView> {
    effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::PublishAuthorityView(
                view,
            ))) => Some(*view),
            _ => None,
        })
        .collect()
}

/// M7A-166. ADR 0007 "Every fence publishes an already-past view" and §3's trigger table with
/// its Scope column; K-A-49; M7A-50's scope split; `design.md` §1.7 "`fence()` writes the view
/// directly".
///
/// Every trigger of M7A-50's table, fired at its tick `t` on a kernel serving `PARTITION` and
/// `SECOND`. Each fence's views are `(t − 1, <that fence's own reason>)` and carry
/// `authority_seq + 1`. A node fence pushes one view per served partition and leaves the node
/// `Fenced`; a partition fence pushes only the fenced partition's view and leaves `SECOND`
/// admitting. No view reaches `t`, and the deny-only reasons `ClockSampleStale` and
/// `ControlUnavailable` are nobody's `past_horizon`.
///
/// The table is the build's, not the plan's nine: it splits clock error, missing bound and future
/// stamp, and it has two more partition triggers (`GenerationChanged`, A-R33). Those two are left
/// out here, because a snapshot that drops `PARTITION` from this fixture drops `SECOND` too; M7A-50
/// asserts their scope. **Deviation:** the plan names "grant record absent ⇒ `NoGrant`". `NoGrant`
/// is deny-only (A-R33b) and the landed fence is `Revoked`, as M7A-16 asserts.
#[retcd_test]
fn m7a_166_every_fence_publishes_an_already_past_view() {
    let mut horizons = BTreeSet::new();
    for (name, t, (scope, reason), trigger) in triggers() {
        if reason == DenyReason::GenerationChanged {
            continue;
        }
        let mut kernel = serving_of(&[PARTITION, SECOND]);
        let seq = kernel.authority_seq();
        let effects = trigger(&mut kernel, t);

        assert_eq!(
            scoped_fences(&effects),
            vec![(scope, reason)],
            "{name}: {effects:?}"
        );
        let views = published(&effects);
        let expected: Vec<PartitionId> = match scope {
            FenceScope::Node => vec![PARTITION, SECOND],
            FenceScope::Partition(partition) => vec![partition],
        };
        assert_eq!(
            views
                .iter()
                .map(|view| view.lineage.partition)
                .collect::<Vec<_>>(),
            expected,
            "{name}: one view per partition in scope: {effects:?}"
        );
        for view in &views {
            assert_eq!(
                (
                    view.valid_through_tick,
                    view.past_horizon,
                    view.authority_seq
                ),
                (Tick(t - 1), reason, seq + 1),
                "{name}: {view:?}"
            );
            horizons.insert(view.past_horizon);
        }
        match scope {
            FenceScope::Node => assert!(kernel.state().is_fenced(), "{name}"),
            FenceScope::Partition(_) => {
                assert!(kernel.state().is_held(), "{name}");
                let second = Lineage {
                    partition: SECOND,
                    ..lineage()
                };
                assert_eq!(
                    kernel.may_admit_at(second, Tick(t), &BUDGETS),
                    Verdict::Admit,
                    "{name}: the grant is intact for the other partition"
                );
            }
        }
    }
    assert!(
        !horizons.contains(&DenyReason::ClockSampleStale)
            && !horizons.contains(&DenyReason::ControlUnavailable),
        "{horizons:?}"
    );
}
