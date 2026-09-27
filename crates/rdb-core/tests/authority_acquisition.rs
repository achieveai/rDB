//! The acquisition rows, `Unheld → Held` (team kernel-a `design.md` §2.4, "Acquisition"), and the
//! renewal rows of `Held`'s steady state, built in the A1 phase-2 dispatch as item §3.1 under lead
//! ruling A-R47. The renewal rows are the second half of the file.
//!
//! Not an `M7A-*` row. This is the build's own evidence, written test-first against the design
//! table, so the behaviour it adds has been seen to fail before the plan rows that will own it are
//! written. Each function names the design row it reads.
//!
//! # What A-R47 changed, and the row that holds it
//!
//! Until A-R47 a `CasResult { Committed }` on `grants/{node}` moved an `Unheld` kernel to `Held`
//! whatever CAS it answered — including one this kernel never issued. Every fixture in the
//! workspace reached `Held` that way. The real row matches the completion to the outstanding
//! acquisition by correlation, and [`an_unmatched_commit_grants_nothing`] is the row that fails
//! if the shortcut comes back.

use config_log::retcd_test;

use bytes::Bytes;
use rdb_core::authority::clock;
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::authority::{
    Acquire, Authority, AuthorityState, AuthorityStateView, AuthorityTimer, Renewal,
};
use rdb_core::contracts::authority::{
    AuthorityEffect, AuthorityFact, AuthorityIgnoreReason, DenyReason, FenceScope, Lineage, Verdict,
};
use rdb_core::contracts::control::{
    CasOutcome, ControlChange, ControlEffect, ControlEvent, ControlKey, ControlPrefix,
    ControlRecord, ReadOutcome, WatchCursor, WatchTermination,
};
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, Event, EventKind, KernelEffect, Module, NodeLifecycle, StepCtx,
};
use rdb_core::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, CorrelationId, EventId, Generation, GrantId,
    NodeId, OwnerEpoch, PartitionId, Revision, Seq, SnapshotHandle, TimerVersion,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::storage::{Namespace, SnapshotRead};
use rdb_core::contracts::time::{ControlTime, Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::Version;

const NODE: NodeId = NodeId(1);
const BOOT: BootId = BootId(1);
const P1: PartitionId = PartitionId(1);
/// The authority-clock estimate the fixed sample carries, taken at tick zero.
const ESTIMATE: u64 = 1_000_000;

/// A1 never reads through the snapshot; every input it acts on arrives as an event.
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

/// One fixed, bounded sample taken at tick zero, so no effect vector below carries a view
/// published because the sample moved (lead ruling A-R45). Fresh until `max_sample_age_millis`.
fn ctx(now: u64) -> StepCtx<'static> {
    StepCtx {
        now: Tick(now),
        control_time: ControlTime {
            estimate: Tick(ESTIMATE),
            error_millis: 10,
            bound_established: true,
            sampled_at: Tick(0),
        },
        node: NODE,
        boot: BOOT,
        partition: P1,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        config_version: ConfigVersion(1),
        snapshot: &SNAPSHOT,
        budgets: &BUDGETS,
    }
}

/// `E_new` for a CAS dispatched at `now` under [`ctx`]'s sample: the estimate carried forward,
/// plus the grant duration. Restated here rather than called, so a kernel that computed it
/// differently fails.
const fn e_new_at(now: u64) -> i64 {
    (ESTIMATE + now + BUDGETS.grant_millis) as i64
}

fn event(id: u64, correlation: u64, kind: EventKind) -> Event {
    Event {
        id: EventId(id),
        at: Tick(id),
        node: NODE,
        boot: BOOT,
        partition: P1,
        correlation: CorrelationId(correlation),
        kind,
    }
}

/// `AcquireDue`, at the version the kernel has armed — the live firing, never a stale one.
fn acquire_due(kernel: &Authority, id: u64, correlation: u64) -> Event {
    event(
        id,
        correlation,
        EventKind::Timer(TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: kernel.timer_version(AuthorityTimer::Acquire),
            scheduled_at: Tick(id),
        }),
    )
}

fn cas_result(id: u64, correlation: u64, outcome: CasOutcome) -> Event {
    event(
        id,
        correlation,
        EventKind::Control(ControlEvent::CasResult {
            key: ControlKey::Grant(NODE),
            outcome,
        }),
    )
}

fn grant_read(id: u64, outcome: ReadOutcome) -> Event {
    event(
        id,
        id,
        EventKind::Control(ControlEvent::Value {
            key: ControlKey::Grant(NODE),
            outcome,
        }),
    )
}

/// A kernel with one acquisition CAS in flight, dispatched at tick 5 under correlation 5.
fn acquiring() -> (Authority, Acquire) {
    let mut kernel = Authority::new();
    let due = acquire_due(&kernel, 5, 5);
    kernel
        .step(&ctx(5), &due)
        .expect("the AcquireDue row is built");
    let acquire = kernel.view().acquire.expect("fixture: a CAS is in flight");
    (kernel, acquire)
}

/// An effect vector reduced to what these rows assert on, in order.
#[derive(Debug, PartialEq, Eq)]
enum Shape {
    /// A `grants/{node}` CAS, with its body decoded.
    Cas {
        expected: Option<Revision>,
        record: Option<GrantRecord>,
    },
    Get(ControlKey),
    Reload(ControlPrefix),
    Watch(ControlPrefix, Revision),
    Arm(AuthorityTimer, TimerVersion, Tick),
    Publish,
    Fact(AuthorityFact),
    Ignored(AuthorityIgnoreReason),
    Fence(FenceScope, DenyReason),
    Other,
}

fn shapes(effects: &[Effect]) -> Vec<Shape> {
    effects
        .iter()
        .map(|effect| match &effect.kind {
            EffectKind::Control(ControlEffect::Cas {
                key: ControlKey::Grant(NODE),
                expected,
                value,
            }) => Shape::Cas {
                expected: *expected,
                record: value.as_deref().and_then(GrantRecord::decode),
            },
            EffectKind::Control(ControlEffect::Get { key }) => Shape::Get(*key),
            EffectKind::Control(ControlEffect::Reload { prefix }) => Shape::Reload(*prefix),
            EffectKind::Control(ControlEffect::Watch { prefix, from }) => {
                Shape::Watch(*prefix, *from)
            }
            EffectKind::Timer(TimerEffect::Arm { id, version, at }) => Shape::Arm(
                AuthorityTimer::from_id(*id).expect("A1 arms only its own timers"),
                *version,
                *at,
            ),
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::PublishAuthorityView(
                _,
            ))) => Shape::Publish,
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fact(fact))) => {
                Shape::Fact(fact.clone())
            }
            EffectKind::Kernel(KernelEffect::Ignored {
                reason: KernelIgnoredReason::Authority(reason),
            }) => Shape::Ignored(reason.clone()),
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fence {
                scope,
                reason,
            })) => Shape::Fence(*scope, *reason),
            _ => Shape::Other,
        })
        .collect()
}

/// `Unheld | AcquireDue`, version current, `acquire.is_none()`, `e_new` is `Some` ⇒
/// `Cas{grants/{node}, expected: None, value: grant{new id, our boot, e_new}}`;
/// `acquire = Some{op, dispatched_at: now, e_new}`.
#[retcd_test]
fn acquire_due_with_a_fresh_sample_issues_one_create_only_grant_cas() {
    let mut kernel = Authority::new();
    let due = acquire_due(&kernel, 5, 5);

    let effects = kernel
        .step(&ctx(5), &due)
        .expect("the AcquireDue row is built");

    let acquire = kernel
        .view()
        .acquire
        .expect("the CAS is recorded as in flight");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Cas {
            expected: None,
            record: Some(GrantRecord {
                grant: acquire.grant,
                node: NODE,
                boot: BOOT,
                authority_generation: AuthorityGeneration::default(),
                expiry_utc_ms: e_new_at(5),
                frozen: false,
            }),
        }],
        "one create-only CAS whose body is our node, our boot and E_new at the dispatch tick"
    );
    assert_eq!(
        acquire,
        Acquire {
            correlation: CorrelationId(5),
            dispatched_at: Tick(5),
            e_new: e_new_at(5),
            grant: acquire.grant,
        },
        "acquire remembers the correlation, the dispatch tick and the E_new it wrote"
    );
    assert_ne!(
        acquire.grant,
        GrantId::default(),
        "a new id, not the zero id"
    );
    assert!(kernel.state().is_unheld(), "a CAS in flight grants nothing");
}

/// `Unheld | AcquireDue`, version current, `acquire.is_none()`, `e_new` is `None` ⇒
/// `AcquireWithheld`, backoff rearm `AcquireDue`; **no CAS**.
///
/// `e_new` is `None` here because the sample is older than `max_sample_age_millis` — stale, which
/// is accepted and denies, rather than rejected — so the vector holds this row's effects and no
/// `SampleRejected` from the step's revalidation.
#[retcd_test]
fn acquire_due_without_an_e_new_is_withheld_and_rearmed() {
    let mut kernel = Authority::new();
    let now = BUDGETS.max_sample_age_millis + 1;
    let due = acquire_due(&kernel, now, 1);
    let armed = kernel.timer_version(AuthorityTimer::Acquire);

    let effects = kernel
        .step(&ctx(now), &due)
        .expect("the AcquireDue row is built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Ignored(AuthorityIgnoreReason::AcquireWithheld),
            Shape::Arm(
                AuthorityTimer::Acquire,
                TimerVersion(armed.0 + 1),
                Tick(now + BUDGETS.renew_millis)
            ),
        ],
        "no sample, no E_new, no write — and a retry, or the node never acquires"
    );
    assert_eq!(kernel.view().acquire, None, "nothing is in flight");
}

/// `Unheld | AcquireDue`, `acquire.is_some()` ⇒ `StaleTimer`, and the CAS in flight is untouched.
#[retcd_test]
fn a_second_acquire_due_while_one_is_in_flight_issues_nothing() {
    let (mut kernel, acquire) = acquiring();
    let due = acquire_due(&kernel, 6, 6);

    let effects = kernel.step(&ctx(6), &due).expect("built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::StaleTimer)],
        "one create-only CAS at a time"
    );
    assert_eq!(
        kernel.view().acquire,
        Some(acquire),
        "the first CAS is still the one in flight"
    );
}

/// `Unheld | CasApplied{op, rev}`, `op == acquire.op` ⇒ `ReadFamily{partitions}`,
/// `Watch{grants}`, `ArmTimer(renew)`; `authority_seq += 1`;
/// `Held{expiry = acquire.e_new, record_revision = rev, renewed_at = acquire.dispatched_at}`.
/// The design row's `PublishAuthorityView` is gone (finding F2): nothing is served yet, and a
/// view for an unserved partition admits where `may_admit` denies.
///
/// The completion arrives at tick 40, well after the dispatch at 5. Both `expiry` and
/// `renewed_at` must come from the **dispatch** (finding K-A-06): anchoring at the completion
/// would hand the node a local window it never earned.
#[retcd_test]
fn the_matched_commit_enters_held_from_the_dispatch_not_the_completion() {
    let (mut kernel, acquire) = acquiring();
    let seq = kernel.authority_seq();

    let effects = kernel
        .step(
            &ctx(40),
            &cas_result(40, 5, CasOutcome::Committed(Revision(7))),
        )
        .expect("the commit row is built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Reload(ControlPrefix::Partitions),
            Shape::Watch(ControlPrefix::Grants, Revision(7)),
            Shape::Arm(
                AuthorityTimer::Renew,
                TimerVersion(1),
                Tick(5 + BUDGETS.renew_millis)
            ),
        ],
        "load the lineage, watch the grant, schedule the renewal; no view before a lineage (F2)"
    );
    let view = kernel.view();
    assert!(view.state.is_held());
    assert_eq!(
        view.expiry_utc_ms,
        Some(acquire.e_new),
        "E is what the CAS wrote"
    );
    assert_eq!(
        view.renewed_at,
        Some(Tick(5)),
        "renewed_at is the dispatch tick"
    );
    assert_eq!(view.record_revision, Some(Revision(7)));
    assert_eq!(
        kernel.held().map(|held| held.identity().grant),
        Some(acquire.grant),
        "the grant held is the one the CAS wrote"
    );
    assert_eq!(
        view.authority_seq,
        seq + 1,
        "a grant adoption bumps authority_seq"
    );
    assert_eq!(view.acquire, None);
}

/// Lead ruling A-R47: a commit this kernel did not issue grants nothing — neither with nothing
/// in flight nor with a different CAS in flight.
#[retcd_test]
fn an_unmatched_commit_grants_nothing() {
    let mut fresh = Authority::new();
    let effects = fresh
        .step(
            &ctx(1),
            &cas_result(1, 1, CasOutcome::Committed(Revision(7))),
        )
        .expect("built");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::UnmatchedCompletion)],
        "nothing in flight: the retired one-event shortcut"
    );
    assert!(fresh.state().is_unheld());

    let (mut kernel, acquire) = acquiring();
    let effects = kernel
        .step(
            &ctx(9),
            &cas_result(9, 99, CasOutcome::Committed(Revision(7))),
        )
        .expect("built");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::UnmatchedCompletion)],
        "a different correlation from the CAS in flight"
    );
    assert!(kernel.state().is_unheld());
    assert_eq!(
        kernel.view().acquire,
        Some(acquire),
        "and the real CAS is still awaited"
    );
}

/// `Unheld | CasConflict{op}`, `op == acquire.op` ⇒ `Read{grants/{node}}`, `Fact(AcquireLost)`;
/// `acquire = None`. The loser learns nothing from the conflict itself (rEtcd ADR-0006).
#[retcd_test]
fn a_conflict_loses_and_reads_the_grant_back() {
    let (mut kernel, _) = acquiring();

    let effects = kernel
        .step(
            &ctx(9),
            &cas_result(
                9,
                5,
                CasOutcome::Conflict {
                    exists: true,
                    current: Revision(3),
                },
            ),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Get(ControlKey::Grant(NODE)),
            Shape::Fact(AuthorityFact::AcquireLost),
        ]
    );
    assert!(kernel.state().is_unheld());
    assert_eq!(
        kernel.view().acquire,
        None,
        "the lost CAS is no longer in flight"
    );
}

/// `Unheld | Unknown{op}`, `op == acquire.op` ⇒ `Read{grants/{node}}`; `acquire = None`; no
/// rights assumed either way (rEtcd ADR-0015).
#[retcd_test]
fn an_unknown_outcome_reads_back_and_assumes_nothing() {
    let (mut kernel, _) = acquiring();

    let effects = kernel
        .step(&ctx(9), &cas_result(9, 5, CasOutcome::Unknown))
        .expect("built");

    assert_eq!(shapes(&effects), vec![Shape::Get(ControlKey::Grant(NODE))]);
    let view = kernel.view();
    assert!(view.state.is_unheld(), "an unknown CAS is not a commit");
    assert_eq!(view.acquire, None);
    assert_eq!(view.expiry_utc_ms, None);
}

/// `Unheld | ReadOk{Some(rec)}`, `rec.boot == our boot`, not frozen, `utc_ok(rec.E)` is `Ok` ⇒
/// `Held` adopting `rec`, with `renewed_at` **derived** from `rec.E − grant_millis` through the
/// sample, not `now` (finding K-A-06).
///
/// The read-back of an `Unknown` whose write had landed: `E` is what the dispatch at tick 5
/// wrote, so the derived `renewed_at` is tick 5 — and the read arrives at tick 900.
#[retcd_test]
fn the_read_back_of_our_own_record_adopts_it_without_restarting_the_window() {
    let (mut kernel, acquire) = acquiring();
    kernel
        .step(&ctx(9), &cas_result(9, 5, CasOutcome::Unknown))
        .expect("built");
    let seq = kernel.authority_seq();
    let written = GrantRecord {
        grant: acquire.grant,
        node: NODE,
        boot: BOOT,
        authority_generation: AuthorityGeneration::default(),
        expiry_utc_ms: acquire.e_new,
        frozen: false,
    };

    let effects = kernel
        .step(
            &ctx(900),
            &grant_read(
                900,
                ReadOutcome::Found {
                    revision: Revision(8),
                    value: written.encode(),
                },
            ),
        )
        .expect("the Unheld adopt row is built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Reload(ControlPrefix::Partitions),
            Shape::Watch(ControlPrefix::Grants, Revision(8)),
            Shape::Arm(
                AuthorityTimer::Renew,
                TimerVersion(1),
                Tick(5 + BUDGETS.renew_millis)
            ),
        ],
        "the commit row's effects less the view: nothing is served yet (finding F2)"
    );
    let view = kernel.view();
    assert!(view.state.is_held());
    assert_eq!(view.expiry_utc_ms, Some(acquire.e_new));
    assert_eq!(view.record_revision, Some(Revision(8)));
    assert_eq!(
        view.renewed_at,
        Some(Tick(5)),
        "tick_of(E − grant_millis), not the tick the read arrived"
    );
    assert_eq!(view.authority_seq, seq + 1);
}

/// `Unheld | ReadOk{Some(rec)}`, someone else's grant, or frozen ⇒ `NotOurs`, backoff rearm
/// `AcquireDue`; stays `Unheld`. One function, three twins of the adopt row, each differing from
/// it in one field.
#[retcd_test]
fn a_read_back_that_is_not_ours_to_adopt_rearms_the_acquisition() {
    let ours = GrantRecord {
        grant: GrantId(1),
        node: NODE,
        boot: BOOT,
        authority_generation: AuthorityGeneration::default(),
        expiry_utc_ms: e_new_at(5),
        frozen: false,
    };
    let twins = [
        (
            "another boot",
            GrantRecord {
                boot: BootId(2),
                ..ours
            },
        ),
        (
            "frozen",
            GrantRecord {
                frozen: true,
                ..ours
            },
        ),
        (
            "an expired E",
            GrantRecord {
                expiry_utc_ms: ESTIMATE as i64,
                ..ours
            },
        ),
    ];
    for (why, record) in twins {
        let mut kernel = Authority::new();
        let armed = kernel.timer_version(AuthorityTimer::Acquire);
        let effects = kernel
            .step(
                &ctx(10),
                &grant_read(
                    10,
                    ReadOutcome::Found {
                        revision: Revision(8),
                        value: record.encode(),
                    },
                ),
            )
            .expect("built");
        assert_eq!(
            shapes(&effects),
            vec![
                Shape::Ignored(AuthorityIgnoreReason::NotOurs),
                Shape::Arm(
                    AuthorityTimer::Acquire,
                    TimerVersion(armed.0 + 1),
                    Tick(10 + BUDGETS.renew_millis)
                ),
            ],
            "{why}"
        );
        assert!(kernel.state().is_unheld(), "{why}");
    }
}

// =============================================================================================
// Renewal: the steady-state rows of `Held` (team kernel-a `design.md` §2.4, "Steady state"),
// the second half of item §3.1.
// =============================================================================================

/// `RenewDue`, at the version the kernel has armed.
fn renew_due(kernel: &Authority, id: u64, correlation: u64) -> Event {
    event(
        id,
        correlation,
        EventKind::Timer(TimerFired {
            id: AuthorityTimer::Renew.id(),
            version: kernel.timer_version(AuthorityTimer::Renew),
            scheduled_at: Tick(id),
        }),
    )
}

/// A kernel holding the grant the acquisition at tick 5 wrote: `E = e_new_at(5)`, committed at
/// revision 7, `renewed_at = 5`, and `Renew` armed at version 1.
fn held() -> (Authority, GrantId) {
    let (mut kernel, acquire) = acquiring();
    kernel
        .step(
            &ctx(5),
            &cas_result(5, 5, CasOutcome::Committed(Revision(7))),
        )
        .expect("the commit row is built");
    assert!(kernel.state().is_held(), "fixture: Held");
    (kernel, acquire.grant)
}

/// [`held`], with one renewal CAS dispatched at tick 505 under correlation 505.
fn renewing() -> (Authority, GrantId, Renewal) {
    let (mut kernel, grant) = held();
    let due = renew_due(&kernel, 505, 505);
    kernel
        .step(&ctx(505), &due)
        .expect("the RenewDue row is built");
    let renewal = kernel
        .view()
        .renewal
        .expect("fixture: a renewal is in flight");
    (kernel, grant, renewal)
}

/// Install `p1` as ours through a coherent partitions snapshot at revision 10, stepped under
/// `ctx` at its own tick.
///
/// A view is published only for a served partition (finding F2), so a row asserting that a
/// renewal publishes its moved horizon serves the event's partition first.
fn serve_p1(kernel: &mut Authority, ctx: &StepCtx<'_>) {
    let at = ctx.now.0;
    let record = PartitionRecord {
        partition: P1,
        owner: NODE,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        config_version: ConfigVersion(1),
        lifecycle: PartitionLifecycle::Serving,
    };
    let install = event(
        at,
        at,
        EventKind::Control(ControlEvent::FamilySnapshot {
            prefix: ControlPrefix::Partitions,
            snapshot_revision: Revision(10),
            records: vec![ControlRecord {
                key: ControlKey::Partition(P1),
                revision: Revision(10),
                value: record.encode(),
            }],
        }),
    );
    kernel
        .step(ctx, &install)
        .expect("the partitions install is built");
    assert!(kernel.view().served.contains_key(&P1), "fixture: p1 served");
}

fn our_record(grant: GrantId, expiry_utc_ms: i64) -> GrantRecord {
    GrantRecord {
        grant,
        node: NODE,
        boot: BOOT,
        authority_generation: AuthorityGeneration::default(),
        expiry_utc_ms,
        frozen: false,
    }
}

/// `Held | RenewDue`, version current, `renewal.is_none()`, `e_new` is `Some` ⇒
/// `Cas{expected: Some(record_revision), value: grant with e_new}`;
/// `renewal = Some{op, dispatched_at: now, e_new}`.
#[retcd_test]
fn renew_due_with_a_fresh_sample_issues_one_cas_on_the_held_revision() {
    let (mut kernel, grant) = held();
    let due = renew_due(&kernel, 505, 505);

    let effects = kernel
        .step(&ctx(505), &due)
        .expect("the RenewDue row is built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Cas {
            expected: Some(Revision(7)),
            record: Some(our_record(grant, e_new_at(505))),
        }],
        "one CAS on the exact revision held, writing E_new at the dispatch tick (spec §7.3 step 1)"
    );
    let view = kernel.view();
    assert_eq!(
        view.renewal,
        Some(Renewal {
            correlation: CorrelationId(505),
            dispatched_at: Tick(505),
            e_new: e_new_at(505),
        })
    );
    assert_eq!(
        view.expiry_utc_ms,
        Some(e_new_at(5)),
        "a CAS in flight extends nothing (property 1)"
    );
}

/// `Held | RenewDue`, `e_new` is `None` ⇒ `RenewalWithheld`, rearm; **no CAS** (ADR-rdb-0007 §2).
///
/// The sample is stale, which is accepted and denies, so the step's revalidation adds nothing;
/// and the local window, renewed at 5, is still open at this tick.
#[retcd_test]
fn renew_due_without_an_e_new_is_withheld_and_rearmed() {
    let (mut kernel, _) = held();
    let now = BUDGETS.max_sample_age_millis + 1;
    let due = renew_due(&kernel, now, now);
    let armed = kernel.timer_version(AuthorityTimer::Renew);

    let effects = kernel.step(&ctx(now), &due).expect("built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Ignored(AuthorityIgnoreReason::RenewalWithheld),
            Shape::Arm(
                AuthorityTimer::Renew,
                TimerVersion(armed.0 + 1),
                Tick(now + BUDGETS.renew_millis)
            ),
        ],
        "no sample, no E_new, no write, and a retry"
    );
    assert!(kernel.state().is_held(), "withholding is not a fence");
    assert_eq!(kernel.view().renewal, None);
}

/// `Held | RenewDue`, `renewal.is_some()` ⇒ `StaleTimer`; the CAS in flight is untouched.
#[retcd_test]
fn a_second_renew_due_while_one_is_in_flight_issues_nothing() {
    let (mut kernel, _, renewal) = renewing();
    let due = renew_due(&kernel, 506, 506);

    let effects = kernel.step(&ctx(506), &due).expect("built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::StaleTimer)]
    );
    assert_eq!(kernel.view().renewal, Some(renewal));
}

/// `Held | CasApplied{op, rev}`, `op == renewal.op` ⇒ `ArmTimer(next renew)`,
/// `PublishAuthorityView`; `expiry = renewal.e_new`, `record_revision = rev`,
/// `renewed_at = renewal.dispatched_at`; **no `authority_seq` bump**.
///
/// The completion arrives at 540, after the dispatch at 505. Both anchors come from the dispatch.
/// `p1` is served first, because only a served partition gets a view (finding F2).
#[retcd_test]
fn a_matched_renewal_commit_advances_e_from_the_dispatch() {
    let (mut kernel, _, renewal) = renewing();
    serve_p1(&mut kernel, &ctx(520));
    let seq = kernel.authority_seq();
    let armed = kernel.timer_version(AuthorityTimer::Renew);

    let effects = kernel
        .step(
            &ctx(540),
            &cas_result(540, 505, CasOutcome::Committed(Revision(9))),
        )
        .expect("the renewal commit row is built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Arm(
                AuthorityTimer::Renew,
                TimerVersion(armed.0 + 1),
                Tick(505 + BUDGETS.renew_millis)
            ),
            Shape::Publish,
        ],
        "schedule the next renewal from the dispatch, publish the moved horizon (K-A-35)"
    );
    let view = kernel.view();
    assert_eq!(view.expiry_utc_ms, Some(renewal.e_new));
    assert_eq!(view.record_revision, Some(Revision(9)));
    assert_eq!(view.renewed_at, Some(Tick(505)), "the dispatch, not 540");
    assert_eq!(view.renewal, None);
    assert_eq!(view.authority_seq, seq, "the lineage did not move");
}

/// A renewal completion this kernel is not awaiting moves nothing (lead ruling A-R47's rule,
/// applied to `Held`).
#[retcd_test]
fn an_unmatched_renewal_commit_extends_nothing() {
    let (mut kernel, _, renewal) = renewing();

    let effects = kernel
        .step(
            &ctx(540),
            &cas_result(540, 99, CasOutcome::Committed(Revision(9))),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::UnmatchedCompletion)]
    );
    let view = kernel.view();
    assert_eq!(view.expiry_utc_ms, Some(e_new_at(5)));
    assert_eq!(view.record_revision, Some(Revision(7)));
    assert_eq!(view.renewal, Some(renewal), "the real CAS is still awaited");
}

/// `Held | CasConflict`, `Held | Unknown`, `Held | Unavailable`, each matching `renewal`: the
/// read-back, the fact that names the outcome, and **the expiry unchanged** (property 1).
#[retcd_test]
fn a_renewal_that_did_not_commit_reads_back_and_leaves_e_alone() {
    let cases = [
        (
            CasOutcome::Conflict {
                exists: true,
                current: Revision(8),
            },
            vec![
                Shape::Get(ControlKey::Grant(NODE)),
                Shape::Fact(AuthorityFact::RenewLost),
            ],
        ),
        (
            CasOutcome::Unknown,
            vec![
                Shape::Get(ControlKey::Grant(NODE)),
                Shape::Fact(AuthorityFact::RenewUnknown),
            ],
        ),
        (
            CasOutcome::Unavailable,
            vec![
                Shape::Arm(
                    AuthorityTimer::Renew,
                    TimerVersion(2),
                    Tick(540 + BUDGETS.renew_millis),
                ),
                Shape::Get(ControlKey::Grant(NODE)),
            ],
        ),
    ];
    for (outcome, expected) in cases {
        let (mut kernel, _, _) = renewing();
        let effects = kernel
            .step(&ctx(540), &cas_result(540, 505, outcome))
            .expect("built");
        assert_eq!(shapes(&effects), expected, "{outcome:?}");
        let view = kernel.view();
        assert!(view.state.is_held(), "{outcome:?}: not a fence");
        assert_eq!(view.expiry_utc_ms, Some(e_new_at(5)), "{outcome:?}");
        assert_eq!(view.renewed_at, Some(Tick(5)), "{outcome:?}");
        assert_eq!(view.record_revision, Some(Revision(7)), "{outcome:?}");
        assert_eq!(view.renewal, None, "{outcome:?}: no longer in flight");
    }
}

/// `Held | ReadOk{Some(rec)}`, same grant, higher revision, not frozen ⇒ `ArmTimer`,
/// `PublishAuthorityView`, `Fact(Adopted)`; adopt `rec.E` and `rec.revision`, `renewed_at`
/// **derived**, not `now` (K-A-06); no `authority_seq` bump.
///
/// The read-back of an `Unknown` renewal whose write had landed: `E` is what the dispatch at 505
/// wrote, so the derived `renewed_at` is 505, and the read arrives at 900. `p1` is served first,
/// because only a served partition gets a view (finding F2).
#[retcd_test]
fn the_read_back_of_a_landed_renewal_adopts_it_without_restarting_the_window() {
    let (mut kernel, grant, renewal) = renewing();
    serve_p1(&mut kernel, &ctx(520));
    kernel
        .step(&ctx(540), &cas_result(540, 505, CasOutcome::Unknown))
        .expect("built");
    let seq = kernel.authority_seq();
    let armed = kernel.timer_version(AuthorityTimer::Renew);

    let effects = kernel
        .step(
            &ctx(900),
            &grant_read(
                900,
                ReadOutcome::Found {
                    revision: Revision(9),
                    value: our_record(grant, renewal.e_new).encode(),
                },
            ),
        )
        .expect("the Held adopt row is built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Arm(
                AuthorityTimer::Renew,
                TimerVersion(armed.0 + 1),
                Tick(505 + BUDGETS.renew_millis)
            ),
            Shape::Fact(AuthorityFact::Adopted),
            Shape::Publish,
        ]
    );
    let view = kernel.view();
    assert_eq!(view.expiry_utc_ms, Some(renewal.e_new));
    assert_eq!(view.record_revision, Some(Revision(9)));
    assert_eq!(
        view.renewed_at,
        Some(Tick(505)),
        "tick_of(E − grant_millis), not the tick the read arrived"
    );
    assert_eq!(view.authority_seq, seq, "no lineage moved");
}

/// `Held | ReadOk{Some(rec)}`, same grant, **not** a higher revision: the record is the one held.
/// **No row in `design.md`.** Nothing is adopted, and the renewal wake is re-armed. Without the
/// re-arm, an `Unknown` renewal that did not land leaves no renewal scheduled at all, and the
/// grant runs out on a healthy store.
#[retcd_test]
fn the_read_back_of_the_record_already_held_adopts_nothing_and_rearms() {
    let (mut kernel, grant, _) = renewing();
    kernel
        .step(&ctx(540), &cas_result(540, 505, CasOutcome::Unknown))
        .expect("built");
    let armed = kernel.timer_version(AuthorityTimer::Renew);

    let effects = kernel
        .step(
            &ctx(560),
            &grant_read(
                560,
                ReadOutcome::Found {
                    revision: Revision(7),
                    value: our_record(grant, e_new_at(5)).encode(),
                },
            ),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![
            // `design.md` §2.6a decision e3: `StaleAuthorityView` was a homograph here.
            Shape::Ignored(AuthorityIgnoreReason::GrantRecordNotNewer),
            Shape::Arm(AuthorityTimer::Renew, TimerVersion(armed.0 + 1), Tick(560)),
        ],
        "the next renewal is due now: renewed_at 5 + renew_millis is already past"
    );
    let view = kernel.view();
    assert_eq!(view.expiry_utc_ms, Some(e_new_at(5)));
    assert_eq!(view.record_revision, Some(Revision(7)));
    assert_eq!(view.renewed_at, Some(Tick(5)));
}

/// `Fenced | CasApplied`, matching a pre-fence renewal ⇒ `LateRenewalIgnored`; **nothing**.
/// Terminal means terminal (K-A-02).
#[retcd_test]
fn a_renewal_that_commits_after_the_fence_is_ignored() {
    let (mut kernel, _, _) = renewing();
    kernel
        .step(
            &ctx(510),
            &grant_read(510, ReadOutcome::Absent { as_of: Revision(8) }),
        )
        .expect("the absent-record fence is built");
    assert!(kernel.state().is_fenced(), "fixture: the grant was revoked");
    let seq = kernel.authority_seq();

    let effects = kernel
        .step(
            &ctx(520),
            &cas_result(520, 505, CasOutcome::Committed(Revision(9))),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::LateRenewalIgnored)]
    );
    assert!(kernel.state().is_fenced(), "no resurrection");
    assert_eq!(kernel.authority_seq(), seq);
}

// =============================================================================================
// Lifecycle near-misses (lead ruling A-R43). Handed back by the phase-2 manual tester: mutants
// M04 and M06 put `StaleTimer` back at each site and every existing row stayed green.
// =============================================================================================

fn lifecycle(id: u64, lifecycle: NodeLifecycle) -> Event {
    event(id, id, EventKind::Node(lifecycle))
}

/// `Held | Resumed{gap}`, `gap <= resume_gap_tolerance` ⇒ `ResumeGapWithinTolerance`, nothing
/// else; the grant stays held. The boundary itself is inside the tolerance.
#[retcd_test]
fn a_resume_gap_within_tolerance_is_named_and_keeps_the_grant() {
    let (mut kernel, _) = held();
    let gap = BUDGETS.resume_gap_tolerance_millis;

    let effects = kernel
        .step(
            &ctx(6),
            &lifecycle(
                6,
                NodeLifecycle::Resumed {
                    suspended_millis: gap,
                },
            ),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(
            AuthorityIgnoreReason::ResumeGapWithinTolerance
        )],
        "the near-miss twin of the ProcessSuspended fence names the gap, not a timer (A-R43)"
    );
    assert!(kernel.state().is_held());
}

/// `Held | Rebooted{boot}`, `boot == held.boot` ⇒ `BootUnchanged`, nothing else.
#[retcd_test]
fn a_reboot_notice_under_the_held_boot_is_boot_unchanged() {
    let (mut kernel, _) = held();

    let effects = kernel
        .step(
            &ctx(6),
            &lifecycle(6, NodeLifecycle::Rebooted { boot: BOOT }),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::BootUnchanged)],
        "the near-miss twin of the BootMismatch fence names the boot, not a timer (A-R43)"
    );
    assert!(kernel.state().is_held());
}

// =============================================================================================
// WatchBackoff counter reset. Handed back by the phase-2 manual tester: mutant M11 (a healthy
// `Watched` delivery no longer resets `watch_refused_attempts`) passed every test in rdb-core
// and rdb-sim, including the tester's own probes.
// =============================================================================================

fn watch_refused(id: u64) -> Event {
    event(
        id,
        id,
        EventKind::Control(ControlEvent::WatchTerminated {
            prefix: ControlPrefix::Grants,
            from: Revision(7),
            termination: WatchTermination::ResourceExhaustedFatal,
        }),
    )
}

/// `Held | Watched{..}` ⇒ `watch_refused_attempts = 0`. Two refusals, one healthy delivery, one
/// more refusal: that is attempt 1 again (base backoff), not attempt 3 (the latch).
#[retcd_test]
fn a_healthy_watch_delivery_resets_the_refusal_count() {
    let (mut kernel, _) = held();
    kernel.step(&ctx(10), &watch_refused(10)).expect("built");
    kernel.step(&ctx(11), &watch_refused(11)).expect("built");
    assert_eq!(kernel.watch_refused_attempts(), 2, "fixture: two refusals");

    kernel
        .step(
            &ctx(12),
            &event(
                12,
                12,
                EventKind::Control(ControlEvent::Watched {
                    prefix: ControlPrefix::Grants,
                    cursor: WatchCursor {
                        revision: Revision(8),
                    },
                    changes: vec![],
                }),
            ),
        )
        .expect("built");
    assert_eq!(
        kernel.watch_refused_attempts(),
        0,
        "a contiguous delivery is the external reset the latch waits for"
    );

    let effects = kernel.step(&ctx(13), &watch_refused(13)).expect("built");
    let shapes = shapes(&effects);
    assert_eq!(
        shapes.first(),
        Some(&Shape::Ignored(AuthorityIgnoreReason::AdmissionRefused)),
        "declined and retried, not latched: {shapes:?}"
    );
    assert!(
        matches!(
            shapes.get(1),
            Some(Shape::Arm(AuthorityTimer::WatchBackoff, _, Tick(63)))
        ),
        "attempt 1 waits the base 50 ms: {shapes:?}"
    );
}

// =============================================================================================
// Plan rows, §3.1 of `docs/testing/test-plan-m7-kernel-a.md`: acquisition and adoption. One
// function per row, named `m7a_NN_<plan name>`; each was seen to fail under a mutant in a private
// copy before it was accepted (lead ruling A-R57).
// =============================================================================================

/// The fences in `effects`, as shapes, in order.
fn fence_shapes(effects: &[Effect]) -> Vec<Shape> {
    shapes(effects)
        .into_iter()
        .filter(|shape| matches!(shape, Shape::Fence(..)))
        .collect()
}

/// The lineage [`serve_p1`] installs, as an admission check names it.
fn p1_lineage() -> Lineage {
    Lineage {
        partition: P1,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    }
}

/// M7A-01. `design.md` §2.4 acquisition; ADR 0007 "CAS races have one winner"; spec §7.2.
///
/// Two `Unheld` kernels, both fed `AcquireDue` at tick 10. The store commits the first create-only
/// CAS and answers the second `Conflict`, which is the only answer a create-only CAS on a key that
/// now exists can get. Exactly one kernel is `Held`; the other read the grant back and is
/// `Unheld`; both CASes were create-only.
#[retcd_test]
fn m7a_01_acquire_create_only_cas_one_winner() {
    let race = |outcome: CasOutcome| {
        let mut kernel = Authority::new();
        let due = acquire_due(&kernel, 10, 10);
        let cas = shapes(&kernel.step(&ctx(10), &due).expect("the AcquireDue row"));
        let answer = shapes(
            &kernel
                .step(&ctx(11), &cas_result(11, 10, outcome))
                .expect("the completion row"),
        );
        (kernel, cas, answer)
    };
    let (winner, won, _) = race(CasOutcome::Committed(Revision(7)));
    let (loser, lost, answer) = race(CasOutcome::Conflict {
        exists: true,
        current: Revision(7),
    });

    for cas in [&won, &lost] {
        assert!(
            matches!(cas.as_slice(), [Shape::Cas { expected: None, .. }]),
            "one create-only CAS each: {cas:?}"
        );
    }
    assert_eq!(
        [winner.state().is_held(), loser.state().is_held()],
        [true, false],
        "exactly one winner"
    );
    assert!(
        answer.contains(&Shape::Get(ControlKey::Grant(NODE))),
        "{answer:?}"
    );
    assert!(loser.state().is_unheld());
}

/// M7A-02. `design.md` §2.4 "CasConflict ⇒ Read, learns nothing"; ADR 0008 §7 item 1. The twin
/// of M7A-01's loser, one fact: no second kernel.
///
/// **Deviation, recorded in the handoff:** the plan writes the vector as `[Control(Get)]`; the
/// design row is `Read{grants/{node}}, Fact(AcquireLost)`, and this asserts the design's whole
/// vector, which is the stronger claim. No `Held` field is populated from the conflict.
#[retcd_test]
fn m7a_02_acquire_cas_conflict_reads_learns_nothing() {
    let (mut kernel, _) = acquiring();

    let effects = kernel
        .step(
            &ctx(9),
            &cas_result(
                9,
                5,
                CasOutcome::Conflict {
                    exists: false,
                    current: Revision(0),
                },
            ),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Get(ControlKey::Grant(NODE)),
            Shape::Fact(AuthorityFact::AcquireLost),
        ]
    );
    let view = kernel.view();
    assert!(view.state.is_unheld());
    assert_eq!(
        (view.expiry_utc_ms, view.record_revision, view.renewed_at),
        (None, None, None),
        "nothing learned from the conflict"
    );
    assert!(kernel.held().is_none());
}

/// M7A-03. `design.md` §2.4 "Unknown ⇒ Read, no rights"; ADR 0007 "unknown CAS".
///
/// An unknown outcome reads the grant back and assumes nothing: `[Control(Get)]`, `Unheld`, and
/// the admission check answers `Deny(NoGrant)`.
#[retcd_test]
fn m7a_03_acquire_cas_unknown_reads_no_rights() {
    let (mut kernel, _) = acquiring();

    let effects = kernel
        .step(&ctx(9), &cas_result(9, 5, CasOutcome::Unknown))
        .expect("built");

    assert_eq!(shapes(&effects), vec![Shape::Get(ControlKey::Grant(NODE))]);
    assert!(kernel.state().is_unheld());
    assert_eq!(
        kernel.may_admit_at(p1_lineage(), Tick(9), &BUDGETS),
        Verdict::Deny(DenyReason::NoGrant)
    );
}

/// The tick M7A-08 and M7A-09 adopt at. Large enough that `t0 − 2500` is a tick.
const T0: u64 = 3_000;

/// [`ctx`] with its sample taken at `t0` rather than zero, on the same authority clock
/// (`ESTIMATE + tick`), so it is fresh for `max_sample_age_millis` after `t0`.
fn sampled_at(now: u64, t0: u64) -> StepCtx<'static> {
    let mut ctx = ctx(now);
    ctx.control_time.estimate = Tick(ESTIMATE + t0);
    ctx.control_time.sampled_at = Tick(t0);
    ctx
}

/// An event every state accepts and no row here is about.
fn probe(at: u64) -> Event {
    event(
        at,
        at,
        EventKind::Control(ControlEvent::WatchProgress {
            prefix: ControlPrefix::Partitions,
            revision: Revision(1),
        }),
    )
}

/// An `Unheld` kernel that adopted, at [`T0`], a committed record of ours whose `E` is
/// `utc(T0) + left_ms`: `grant_millis − left_ms` of its window already spent.
fn adopted_with(left_ms: i64) -> Authority {
    let mut kernel = Authority::new();
    let committed = our_record(GrantId(1), (ESTIMATE + T0) as i64 + left_ms);
    kernel
        .step(
            &sampled_at(T0, T0),
            &grant_read(
                T0,
                ReadOutcome::Found {
                    revision: Revision(8),
                    value: committed.encode(),
                },
            ),
        )
        .expect("the Unheld adopt row is built");
    assert!(kernel.state().is_held(), "fixture: adopted");
    kernel
}

/// M7A-08. `design.md` §2.4 adopt path; ADR 0007 "adoption doesn't restart the local window".
///
/// `E_committed = utc(t0) + 500`, so 2500 ms of the window are already spent. `renewed_at` is
/// `t0 − 2500`, not `t0`, and at `t0 + 400` the local window has lapsed
/// (`2500 + 400 + δ ≥ 3000`): `Fence{Node, Expired}`.
///
/// **Port, recorded in the handoff (E7-Q2):** the plan's input is `Recovered{E_committed}`, but
/// `KernelEvent::Recovered` carries a `RecoveryResult`, which has no `E`, and `design.md`'s
/// `Recovered` pair is the partition-lineage install. The adoption of a committed `E` is the
/// `Unheld | ReadOk{Some(rec)}` row, so that is the input here. With the plan's numbers the clock
/// conjunct also fails at `t0 + 400` (`E − ε − δ < utc(t0 + 400)`); the local conjunct is asserted
/// on its own through [`clock::local_ok`], so the fence is not credited to the wrong conjunct.
#[retcd_test]
fn m7a_08_adopt_authority_derives_renewed_at_from_committed_expiry() {
    let mut kernel = adopted_with(500);
    let renewed_at = kernel.view().renewed_at;
    assert_eq!(renewed_at, Some(Tick(T0 - 2_500)), "derived, not t0");

    let now = T0 + 400;
    assert!(!clock::local_ok(Tick(T0 - 2_500), Tick(now), &BUDGETS));
    let effects = kernel
        .step(&sampled_at(now, T0), &probe(now))
        .expect("built");
    assert_eq!(
        fence_shapes(&effects),
        vec![Shape::Fence(FenceScope::Node, DenyReason::Expired)]
    );
}

/// M7A-09. The twin of M7A-08, one fact: `E_committed = utc(t0) + 1000`. At `t0 + 400` the
/// window renewed at `t0 − 2000` still holds: no fence, and the check admits.
#[retcd_test]
fn m7a_09_adopt_authority_window_still_open_admits() {
    let mut kernel = adopted_with(1_000);
    assert_eq!(kernel.view().renewed_at, Some(Tick(T0 - 2_000)));
    serve_p1(&mut kernel, &sampled_at(T0 + 1, T0));

    let now = T0 + 400;
    let effects = kernel
        .step(&sampled_at(now, T0), &probe(now))
        .expect("built");
    assert_eq!(fence_shapes(&effects), vec![]);
    assert_eq!(
        kernel.may_admit_at(p1_lineage(), Tick(now), &BUDGETS),
        Verdict::Admit
    );
}

// =============================================================================================
// Plan rows, §3.2 of `docs/testing/test-plan-m7-kernel-a.md`: renewal, freeze and revocation.
// =============================================================================================

/// The tick the local window of a grant renewed at 5 lapses: the first `t` with
/// `t − 5 + dispatch_margin ≥ grant_millis`.
const LAPSE: u64 = 5 + BUDGETS.grant_millis - BUDGETS.dispatch_margin_millis;

/// [`renewing`], answered `outcome` at tick 540.
fn renewal_answered(outcome: CasOutcome) -> (Authority, GrantId, Vec<Shape>) {
    let (mut kernel, grant, _) = renewing();
    let effects = kernel
        .step(&ctx(540), &cas_result(540, 505, outcome))
        .expect("the renewal completion row is built");
    (kernel, grant, shapes(&effects))
}

/// [`held`], then a read of our grant record at revision 8, changed by `change`.
fn held_then_read(change: fn(&mut GrantRecord)) -> (Authority, Vec<Effect>) {
    let (mut kernel, grant) = held();
    let mut record = our_record(grant, e_new_at(5));
    change(&mut record);
    let effects = kernel
        .step(
            &ctx(600),
            &grant_read(
                600,
                ReadOutcome::Found {
                    revision: Revision(8),
                    value: record.encode(),
                },
            ),
        )
        .expect("the Held grant read row is built");
    (kernel, effects)
}

/// `Held | Watched` on the grants family, naming our record at `revision`.
fn grant_changed(at: u64, revision: u64) -> Event {
    event(
        at,
        at,
        EventKind::Control(ControlEvent::Watched {
            prefix: ControlPrefix::Grants,
            cursor: WatchCursor {
                revision: Revision(revision),
            },
            changes: vec![ControlChange {
                key: ControlKey::Grant(NODE),
                revision: Revision(revision),
            }],
        }),
    )
}

/// The number of `grants/{node}` CASes in `effects`, create-only or not.
fn cases(effects: &[Effect]) -> usize {
    shapes(effects)
        .iter()
        .filter(|shape| matches!(shape, Shape::Cas { .. }))
        .count()
}

/// M7A-10. `design.md` §2.4 "RenewDue CAS expected: Some(record_revision)". The held revision is
/// 7 here where the plan says 41; the value is arbitrary, the equality is the claim.
#[retcd_test]
fn m7a_10_renew_due_cas_expected_record_revision() {
    let (mut kernel, grant) = held();
    let due = renew_due(&kernel, 505, 505);

    let effects = kernel.step(&ctx(505), &due).expect("built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Cas {
            expected: Some(Revision(7)),
            record: Some(our_record(grant, e_new_at(505))),
        }],
        "one CAS on the held revision, and nothing else"
    );
}

/// M7A-11. `design.md` §2.4 "CasApplied writes expiry/renewed_at"; A-R11
/// `E_new = extrapolated_utc(dispatch tick) + grant_duration`.
///
/// `RenewDue` at 1000, `Committed(42)` at 1100. The plan's clock (`5_000_000` at tick 800, ε 20)
/// is this file's (`ESTIMATE + tick`, ε 10): `E_new` does not read ε, and one clock the whole
/// file shares keeps the fixture from moving the sample. So `E = ESTIMATE + 1000 + 3000`.
#[retcd_test]
fn m7a_11_renew_cas_applied_writes_expiry_and_renewed_at() {
    let view = renewed_at_1000_committed_at(1_100);
    assert_eq!(view.expiry_utc_ms, Some(e_new_at(1_000)));
    assert_eq!(view.renewed_at, Some(Tick(1_000)));
    assert_eq!(view.record_revision, Some(Revision(42)));
}

/// M7A-12. The twin of M7A-11, one fact: the completion is delayed to tick 1900. `E` and
/// `renewed_at` are M7A-11's, both from the dispatch (A-R11).
#[retcd_test]
fn m7a_12_renew_e_new_uses_dispatch_tick_not_completion_tick() {
    let view = renewed_at_1000_committed_at(1_900);
    assert_eq!(view.expiry_utc_ms, Some(e_new_at(1_000)));
    assert_eq!(view.renewed_at, Some(Tick(1_000)));
}

/// A renewal dispatched at 1000 and committed at revision 42 at tick `completed`.
fn renewed_at_1000_committed_at(completed: u64) -> AuthorityStateView {
    let (mut kernel, _) = held();
    let due = renew_due(&kernel, 1_000, 1_000);
    kernel.step(&ctx(1_000), &due).expect("the RenewDue row");
    kernel
        .step(
            &ctx(completed),
            &cas_result(completed, 1_000, CasOutcome::Committed(Revision(42))),
        )
        .expect("the renewal commit row");
    kernel.view()
}

/// M7A-13. `design.md` §2.4 "Conflict leaves expiry"; ADR 0008 §7 item 1.
///
/// **Deviation, recorded in the handoff:** the plan writes the vector as `[Control(Get)]`; the
/// design row adds `Fact(RenewLost)`, and this asserts the whole of it. `E` is unchanged and
/// nothing fences.
#[retcd_test]
fn m7a_13_renew_conflict_leaves_expiry_reads() {
    let (kernel, _, answer) = renewal_answered(CasOutcome::Conflict {
        exists: true,
        current: Revision(8),
    });

    assert_eq!(
        answer,
        vec![
            Shape::Get(ControlKey::Grant(NODE)),
            Shape::Fact(AuthorityFact::RenewLost),
        ]
    );
    assert_eq!(kernel.view().expiry_utc_ms, Some(e_new_at(5)));
    assert!(kernel.state().is_held());
}

/// M7A-14. `design.md` §2.4 "Unknown leaves expiry"; ADR 0007 "unknown CAS" deny-only; ADR 0008
/// §7 item 2.
///
/// After `Unknown`: `E` unchanged, no fence, the read-back (**deviation**: plus
/// `Fact(RenewUnknown)`, the design row's). Then every tick up to the lapse of the **old** window,
/// renewed at 5: no fence before [`LAPSE`], `Fence{Node, Expired}` at it. The sample is stale
/// from tick 2001, which denies and does not fence, so the fence at [`LAPSE`] is the local
/// window's.
#[retcd_test]
fn m7a_14_renew_unknown_leaves_expiry_denies_only() {
    let (mut kernel, _, answer) = renewal_answered(CasOutcome::Unknown);
    assert_eq!(
        answer,
        vec![
            Shape::Get(ControlKey::Grant(NODE)),
            Shape::Fact(AuthorityFact::RenewUnknown),
        ]
    );
    assert_eq!(kernel.view().expiry_utc_ms, Some(e_new_at(5)));

    for now in 541..LAPSE {
        let effects = kernel.step(&ctx(now), &probe(now)).expect("built");
        assert_eq!(fence_shapes(&effects), vec![], "no fence at {now}");
    }
    let effects = kernel.step(&ctx(LAPSE), &probe(LAPSE)).expect("built");
    assert_eq!(
        fence_shapes(&effects),
        vec![Shape::Fence(FenceScope::Node, DenyReason::Expired)],
        "at the old window's lapse, {LAPSE}"
    );
}

/// M7A-15. `design.md` §2.4 "Unavailable leaves expiry"; ADR 0008 "control-quorum loss denies".
///
/// `E` unchanged; the read-back and a backoff re-arm of the renewal; the check still admits while
/// the local window holds. **Deviation, recorded in the handoff:** the plan orders the vector
/// `[Control(Get), Timer(backoff)]`; the kernel emits the re-arm first. This asserts the
/// kernel's order exactly.
#[retcd_test]
fn m7a_15_renew_unavailable_leaves_expiry_read_backoff() {
    let (mut kernel, _, _) = renewing();
    serve_p1(&mut kernel, &ctx(520));
    let armed = kernel.timer_version(AuthorityTimer::Renew);

    let effects = kernel
        .step(&ctx(540), &cas_result(540, 505, CasOutcome::Unavailable))
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Arm(
                AuthorityTimer::Renew,
                TimerVersion(armed.0 + 1),
                Tick(540 + BUDGETS.renew_millis)
            ),
            Shape::Get(ControlKey::Grant(NODE)),
        ]
    );
    assert_eq!(kernel.view().expiry_utc_ms, Some(e_new_at(5)));
    assert_eq!(
        kernel.may_admit_at(p1_lineage(), Tick(540), &BUDGETS),
        Verdict::Admit
    );
}

/// M7A-16. `design.md` §2.4 "ReadOk None ⇒ Fence Revoked"; ADR 0007 §3 "grant absent". After
/// M7A-13's read-back, the record is absent: `Fence{Node, Revoked}`, and the state is
/// `Fenced{Revoked}`.
#[retcd_test]
fn m7a_16_renewal_read_absent_fences_revoked() {
    let (mut kernel, _, _) = renewal_answered(CasOutcome::Conflict {
        exists: true,
        current: Revision(8),
    });

    let effects = kernel
        .step(
            &ctx(560),
            &grant_read(560, ReadOutcome::Absent { as_of: Revision(8) }),
        )
        .expect("built");

    assert_eq!(
        fence_shapes(&effects),
        vec![Shape::Fence(FenceScope::Node, DenyReason::Revoked)]
    );
    assert!(matches!(
        kernel.state(),
        AuthorityState::Fenced {
            reason: DenyReason::Revoked,
            ..
        }
    ));
}

/// M7A-17. `design.md` §2.4 "frozen ⇒ Fence Frozen"; ADR 0007 "renewal after freeze". A read of
/// our record, frozen: `Fence{Node, Frozen}`, and the next `RenewDue` issues no CAS.
#[retcd_test]
fn m7a_17_renewal_after_freeze_fences_frozen() {
    let (mut kernel, answer) = held_then_read(|record| record.frozen = true);
    assert_eq!(
        fence_shapes(&answer),
        vec![Shape::Fence(FenceScope::Node, DenyReason::Frozen)]
    );

    let due = renew_due(&kernel, 1_005, 1_005);
    let effects = kernel.step(&ctx(1_005), &due).expect("built");
    assert_eq!(cases(&effects), 0, "{effects:?}");
}

/// M7A-18. ADR 0007 "renewal-before-freeze race"; `design.md` §2.4.
///
/// The renewal commits at 42; then a watch names our record at 43, where it is frozen (the flag
/// is in the body, which a watch does not carry, TD-17). The watch reads; the read fences
/// `Frozen`; a later commit for the earlier renewal's correlation is ignored.
///
/// **Deviation, recorded in the handoff:** the plan says `Fact(LateRenewalIgnored)`; the landed
/// name is an ignore reason, `AuthorityIgnoreReason::LateRenewalIgnored`, never a fact.
#[retcd_test]
fn m7a_18_renewal_before_freeze_race_committed_renewal_does_not_unfence() {
    let (mut kernel, grant, _) = renewing();
    kernel
        .step(
            &ctx(540),
            &cas_result(540, 505, CasOutcome::Committed(Revision(42))),
        )
        .expect("the renewal commit");

    let watched = kernel
        .step(&ctx(550), &grant_changed(550, 43))
        .expect("built");
    assert_eq!(shapes(&watched), vec![Shape::Get(ControlKey::Grant(NODE))]);

    let frozen = GrantRecord {
        frozen: true,
        ..our_record(grant, e_new_at(505))
    };
    let read = kernel
        .step(
            &ctx(560),
            &grant_read(
                560,
                ReadOutcome::Found {
                    revision: Revision(43),
                    value: frozen.encode(),
                },
            ),
        )
        .expect("built");
    assert_eq!(
        fence_shapes(&read),
        vec![Shape::Fence(FenceScope::Node, DenyReason::Frozen)]
    );

    let late = kernel
        .step(
            &ctx(570),
            &cas_result(570, 505, CasOutcome::Committed(Revision(44))),
        )
        .expect("built");
    assert_eq!(
        shapes(&late),
        vec![Shape::Ignored(AuthorityIgnoreReason::LateRenewalIgnored)]
    );
    assert!(kernel.state().is_fenced(), "the commit did not unfence");
}

/// M7A-19. `design.md` §2.4 "other grant/boot ⇒ BootMismatch|Revoked"; ADR 0007 "old-boot grants
/// deny"; spec §7.2. Our grant, another boot: `Fence{Node, BootMismatch}`. Twin: M7A-21.
#[retcd_test]
fn m7a_19_renewal_read_other_boot_fences_boot_mismatch() {
    let (_, answer) = held_then_read(|record| record.boot = BootId(2));
    assert_eq!(
        fence_shapes(&answer),
        vec![Shape::Fence(FenceScope::Node, DenyReason::BootMismatch)]
    );
}

/// M7A-20. `design.md` §2.4 "auth gen ⇒ AuthorityGenerationChanged"; ADR 0007 §3. Twin: M7A-21.
#[retcd_test]
fn m7a_20_renewal_read_authority_generation_changed_fences() {
    let (_, answer) = held_then_read(|record| {
        record.authority_generation = AuthorityGeneration(record.authority_generation.0 + 1);
    });
    assert_eq!(
        fence_shapes(&answer),
        vec![Shape::Fence(
            FenceScope::Node,
            DenyReason::AuthorityGenerationChanged
        )]
    );
}

/// M7A-21. The twin of M7A-19 and M7A-20, one fact each: our grant, our boot, our authority
/// generation, not frozen. No fence, and `record_revision` moves to the read's.
#[retcd_test]
fn m7a_21_renewal_read_same_grant_same_boot_no_fence() {
    let (kernel, answer) = held_then_read(|_| {});
    assert_eq!(fence_shapes(&answer), vec![]);
    assert_eq!(kernel.view().record_revision, Some(Revision(8)));
}

/// M7A-22. `design.md` §2.4 `Fenced | CasApplied ⇒ LateRenewalIgnored`; ADR 0008 §7 item 7.
///
/// Fenced `Expired` at the lapse with a renewal in flight; its commit then arrives. The only
/// effect is the ignore reason (**deviation**, as M7A-18: the plan says `Fact`); the state is
/// still `Fenced{Expired}`; `E` and the record revision are not written.
#[retcd_test]
fn m7a_22_fenced_then_cas_applied_late_renewal_ignored() {
    let (mut kernel, _, _) = renewing();
    kernel
        .step(&ctx(LAPSE), &probe(LAPSE))
        .expect("the lapse fence");
    let before = kernel.view();

    let effects = kernel
        .step(
            &ctx(LAPSE + 1),
            &cas_result(LAPSE + 1, 505, CasOutcome::Committed(Revision(50))),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::LateRenewalIgnored)]
    );
    assert!(matches!(
        kernel.state(),
        AuthorityState::Fenced {
            reason: DenyReason::Expired,
            ..
        }
    ));
    let after = kernel.view();
    assert_eq!(
        (after.expiry_utc_ms, after.record_revision),
        (before.expiry_utc_ms, before.record_revision)
    );
}

/// M7A-23. `design.md` §2.1 "Fenced terminal; exit only via new grant id". **The terminal half
/// only** (lead ruling A-R57's reasoning for M7A-42, applied here and flagged in the handoff):
/// `Fenced` has no exit in this build, so "only a fresh `AcquireDue` produces a create-only
/// `Cas`" is Unavailable, not asserted.
///
/// `Fenced`, then `RenewDue`, a tick, a newer valid sample, 20 grant watch deliveries and an
/// `AcquireDue`: zero CASes, and `Fenced` throughout.
#[retcd_test]
fn m7a_23_fenced_is_terminal_until_new_grant_id() {
    let (mut kernel, _) = held_then_read(|record| record.frozen = true);
    assert!(kernel.state().is_fenced(), "fixture");

    let mut inputs = vec![
        (ctx(1_005), renew_due(&kernel, 1_005, 1_005)),
        (ctx(1_006), probe(1_006)),
        (sampled_at(1_007, 1_007), probe(1_007)),
    ];
    for i in 0..20 {
        inputs.push((ctx(1_010 + i), grant_changed(1_010 + i, 50 + i)));
    }
    inputs.push((ctx(1_040), acquire_due(&kernel, 1_040, 1_040)));
    for (ctx, event) in &inputs {
        let effects = kernel.step(ctx, event).expect("built");
        assert_eq!(cases(&effects), 0, "{event:?}: {effects:?}");
        assert!(kernel.state().is_fenced(), "{event:?}");
    }
}

/// M7A-27. ADR 0007 "renewed expiry no runaway (10 min of renewals)"; A-R11.
///
/// 1200 renewals at 500 ms cadence, each under a fresh sample taken at its dispatch tick (ε 20),
/// each committed a tick later. Every `E` is at most `extrapolated_utc(dispatch) + 3000`, and the
/// sequence of `E` rises by exactly 500 each time.
#[retcd_test]
fn m7a_27_renewed_expiry_no_runaway_ten_minutes() {
    let (mut kernel, _) = held();
    let mut previous = kernel.view().expiry_utc_ms.expect("fixture: E");
    for i in 1..=1_200_u64 {
        let dispatch = 5 + BUDGETS.renew_millis * i;
        let mut sample = sampled_at(dispatch, dispatch);
        sample.control_time.error_millis = 20;
        let due = renew_due(&kernel, dispatch, dispatch);
        kernel.step(&sample, &due).expect("the RenewDue row");
        sample.now = Tick(dispatch + 1);
        kernel
            .step(
                &sample,
                &cas_result(
                    dispatch + 1,
                    dispatch,
                    CasOutcome::Committed(Revision(7 + i)),
                ),
            )
            .expect("the renewal commit row");

        let expiry = kernel.view().expiry_utc_ms.expect("E");
        assert!(
            expiry <= (ESTIMATE + dispatch + BUDGETS.grant_millis) as i64,
            "renewal {i}: E {expiry}"
        );
        assert_eq!(expiry - previous, 500, "renewal {i}");
        previous = expiry;
    }
}

/// A refusal at `now`, reduced to the back-off it arms: `Some(delay)` when it declined and
/// re-armed `WatchBackoff`, `None` when it latched (`[Fact(WatchAdmissionExhausted)]`).
fn refusal_backoff(kernel: &mut Authority, now: u64) -> Option<u64> {
    let shapes = shapes(&kernel.step(&ctx(now), &watch_refused(now)).expect("built"));
    match shapes.as_slice() {
        [Shape::Ignored(AuthorityIgnoreReason::AdmissionRefused), Shape::Arm(AuthorityTimer::WatchBackoff, _, Tick(at))] => {
            Some(at - now)
        }
        [Shape::Fact(AuthorityFact::WatchAdmissionExhausted)] => None,
        other => panic!("a refusal either re-arms or latches: {other:?}"),
    }
}

/// M7A-164. `design.md` §2.4 sweep, the `watch_refused_attempts` reset row. One fact against
/// M7A-31: the healthy event between.
///
/// Five `ResourceExhaustedFatal` refusals: the back-off climbs over the two the cap allows, then
/// latches for the other three (lead ruling A-R41 caps at 3, so "five, climbing" is undrivable as
/// the plan words it). Then a healthy `WatchProgress`, the reset site the earlier
/// `a_healthy_watch_delivery_resets_the_refusal_count` does not reach. The next refusal waits
/// exactly what the first did, not what a sixth would.
#[retcd_test]
fn m7a_164_watch_admission_refused_counter_resets_on_healthy_watch() {
    let (mut kernel, _) = held();
    let before: Vec<Option<u64>> = (10..15)
        .map(|now| refusal_backoff(&mut kernel, now))
        .collect();
    let (first, second) = (before[0].expect("under cap"), before[1].expect("under cap"));
    assert!(second > first, "climbing: {before:?}");
    assert_eq!(before[2..], [None, None, None], "latched: {before:?}");

    kernel
        .step(
            &ctx(15),
            &event(
                15,
                15,
                EventKind::Control(ControlEvent::WatchProgress {
                    prefix: ControlPrefix::Grants,
                    revision: Revision(8),
                }),
            ),
        )
        .expect("built");

    assert_eq!(refusal_backoff(&mut kernel, 16), Some(first));
}

// =============================================================================================
// Plan row M7A-148, §8.3 of `docs/testing/test-plan-m7-kernel-a.md`: the acquisition guard.
// =============================================================================================

/// [`ctx`] at `now`, on the same authority clock (`ESTIMATE + tick`), holding a sample taken at
/// `at` with error `error` and bound `established`.
fn reading(now: u64, at: u64, error: u64, established: bool) -> StepCtx<'static> {
    let mut ctx = ctx(now);
    ctx.control_time = ControlTime {
        estimate: Tick(ESTIMATE + at),
        error_millis: error,
        bound_established: established,
        sampled_at: Tick(at),
    };
    ctx
}

/// Why an acquisition was withheld: the two reasons K-A-50 leaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Withheld {
    NoSample,
    Stale,
}

/// The reason an `AcquireWithheld` at `now` was withheld for.
///
/// The landed `AuthorityIgnoreReason::AcquireWithheld` is a unit variant, as `SampleRejected` is
/// (M7A-165), so the reason is read from the one input `e_new` is a function of: the held sample.
/// None held is `NoSample`; a held sample that `effective_epsilon` calls stale is `Stale`. A held
/// sample refused for any other reason is a third withholding reason, which is this row's red.
fn withheld_reason(kernel: &Authority, now: u64) -> Withheld {
    match kernel.clock().sample() {
        None => Withheld::NoSample,
        Some(sample) => match clock::effective_epsilon(&sample, Tick(now), &BUDGETS) {
            Err(clock::ClockFault::Stale) => Withheld::Stale,
            other => panic!("a held sample withholds only as stale: {other:?} for {sample:?}"),
        },
    }
}

/// `AcquireDue` at `step_ctx.now`, asserted withheld: no CAS, the retry re-armed one back-off
/// later, nothing in flight. Returns the reason and the step's effects.
fn withheld_at(kernel: &mut Authority, step_ctx: &StepCtx<'_>) -> (Withheld, Vec<Shape>) {
    let now = step_ctx.now.0;
    let armed = kernel.timer_version(AuthorityTimer::Acquire);
    let due = acquire_due(kernel, now, now);
    let effects = shapes(&kernel.step(step_ctx, &due).expect("AcquireDue"));
    let [.., Shape::Ignored(AuthorityIgnoreReason::AcquireWithheld), Shape::Arm(AuthorityTimer::Acquire, version, Tick(at))] =
        effects.as_slice()
    else {
        panic!("withheld and re-armed: {effects:?}");
    };
    assert_eq!(
        (*version, *at),
        (TimerVersion(armed.0 + 1), now + BUDGETS.renew_millis),
        "the back-off is re-armed"
    );
    assert!(
        !effects
            .iter()
            .any(|shape| matches!(shape, Shape::Cas { .. })),
        "no CAS: {effects:?}"
    );
    assert_eq!(kernel.view().acquire, None, "nothing in flight");
    (withheld_reason(kernel, now), effects)
}

/// A fresh, in-bound sample and one more `AcquireDue` at `now`: exactly one create-only CAS
/// whose `E` is `E_new` at `now`, and its commit enters `Held` with `renewed_at == now`.
fn acquires_at(kernel: &mut Authority, now: u64) {
    let due = acquire_due(kernel, now, now);
    let effects = kernel
        .step(&reading(now, now, 10, true), &due)
        .expect("AcquireDue");
    let cases: Vec<Shape> = shapes(&effects)
        .into_iter()
        .filter(|shape| matches!(shape, Shape::Cas { .. }))
        .collect();
    let [Shape::Cas {
        expected: None,
        record: Some(record),
    }] = cases.as_slice()
    else {
        panic!("one create-only CAS: {effects:?}");
    };
    assert_eq!(record.expiry_utc_ms, e_new_at(now), "E == E_new at {now}");
    kernel
        .step(
            &reading(now + 1, now, 10, true),
            &cas_result(now + 1, now, CasOutcome::Committed(Revision(7))),
        )
        .expect("the commit");
    assert!(kernel.state().is_held(), "the commit adopts");
    assert_eq!(
        kernel.view().renewed_at,
        Some(Tick(now)),
        "renewed_at is the dispatch tick"
    );
}

/// M7A-148. K-A-36; K-A-50; `design.md` §2.4 `Unheld / AcquireDue / e_new is None ⇒
/// AcquireWithheld`, no CAS, and the `Unheld|Fenced / Clock(s)` reject row; ADR 0007 "No sample,
/// no acquisition", "A rejected sample retracts the good one".
///
/// Four `Unheld` kernels, one per case, each ending in a withheld `AcquireDue` that issues no CAS
/// and re-arms the back-off:
///
/// * (a) no good sample ever. The seam delivers a reading on every step, even without a bound
///   (§13 Q-12), so "no sample" is an unestablished one, refused on the `AcquireDue` step itself;
/// * (b) a good sample, then one with no bound: that step is exactly `[SampleRejected]`, and the
///   good sample is gone;
/// * (c) a good sample, then one with error 101 (over the 100 ms bound): the same;
/// * (d) a good sample aged 2001, past `max_sample_age_millis`: withheld, and the sample stays.
///
/// Exactly two reasons appear across the four: `NoSample` for (a)–(c) and `Stale` for (d). A
/// rejected sample leaves no sample behind rather than a reason of its own. Each kernel then takes
/// a fresh in-bound sample and acquires with exactly one create-only CAS at `E_new`, and the commit
/// sets `renewed_at` to the dispatch tick. Near-miss twin of (c), one fact (error 100, at the
/// bound): the sample is adopted and the `AcquireDue` issues the CAS.
///
/// Landed spelling: `Ignored(AcquireWithheld)` and `Ignored(SampleRejected)` are unit variants, so
/// the plan's `{NoSample}` / `{Stale}` / `{Invalid}` / `{OverBound}` payloads are read from the
/// kernel's held sample by [`withheld_reason`], not from the effect.
#[retcd_test]
fn m7a_148_acquire_withheld_reasons_collapse_to_no_sample_and_stale() {
    let rejected = Shape::Ignored(AuthorityIgnoreReason::SampleRejected);
    let mut reasons = std::collections::BTreeSet::new();

    // (a) No good sample ever.
    let mut kernel = Authority::new();
    let (reason, effects) = withheld_at(&mut kernel, &reading(5, 5, 10, false));
    assert_eq!(reason, Withheld::NoSample, "(a): {effects:?}");
    assert_eq!(effects[0], rejected, "(a): the reading itself is refused");
    reasons.insert(reason);
    acquires_at(&mut kernel, 300);

    // (b) and (c): a good sample, then one the guard rejects.
    for (what, error, established) in [("(b) no bound", 10, false), ("(c) error 101", 101, true)] {
        let mut kernel = Authority::new();
        kernel
            .step(&reading(10, 0, 10, true), &probe(10))
            .expect("the good sample");
        assert!(kernel.clock().sample().is_some(), "{what}: fixture");
        let bad = reading(100, 100, error, established);
        let effects = kernel.step(&bad, &probe(100)).expect("the bad sample");
        assert_eq!(
            shapes(&effects),
            vec![Shape::Ignored(AuthorityIgnoreReason::SampleRejected)],
            "{what}"
        );
        assert_eq!(
            kernel.clock().sample(),
            None,
            "{what}: the good sample is retracted"
        );
        let (reason, effects) = withheld_at(&mut kernel, &reading(200, 100, error, established));
        assert_eq!(reason, Withheld::NoSample, "{what}: {effects:?}");
        reasons.insert(reason);
        acquires_at(&mut kernel, 300);
    }

    // (d) A good sample, aged past `max_sample_age_millis`.
    let mut kernel = Authority::new();
    kernel
        .step(&reading(10, 0, 10, true), &probe(10))
        .expect("the good sample");
    let age = BUDGETS.max_sample_age_millis + 1;
    let (reason, effects) = withheld_at(&mut kernel, &reading(age, 0, 10, true));
    assert_eq!(reason, Withheld::Stale, "(d): {effects:?}");
    assert_eq!(effects.len(), 2, "(d): only the withholding: {effects:?}");
    assert!(
        kernel.clock().sample().is_some(),
        "(d): a stale sample is not retracted"
    );
    reasons.insert(reason);
    acquires_at(&mut kernel, age + 100);

    assert_eq!(
        reasons,
        std::collections::BTreeSet::from([Withheld::NoSample, Withheld::Stale]),
        "exactly two withholding reasons (the helper enum has two variants, so this holds by
         construction; the per-case asserts above carry the claim)"
    );

    // Near-miss twin of (c): error 100 is at the bound, so the sample is adopted.
    let mut kernel = Authority::new();
    kernel
        .step(&reading(10, 0, 10, true), &probe(10))
        .expect("the good sample");
    let at_bound = reading(100, 100, BUDGETS.clock_error_millis, true);
    let effects = kernel.step(&at_bound, &probe(100)).expect("the sample");
    assert_eq!(shapes(&effects), vec![], "twin: adopted, not rejected");
    assert_eq!(kernel.clock().sample(), Some(at_bound.control_time), "twin");
    let due = acquire_due(&kernel, 200, 200);
    let effects = kernel
        .step(&reading(200, 100, BUDGETS.clock_error_millis, true), &due)
        .expect("AcquireDue");
    assert_eq!(
        shapes(&effects)
            .iter()
            .filter(|shape| matches!(shape, Shape::Cas { .. }))
            .count(),
        1,
        "twin: the CAS is issued: {effects:?}"
    );
}
