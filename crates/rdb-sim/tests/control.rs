//! Rows M7F-10, M7F-12, M7F-13, M7F-15, M7F-20 and M7F-57: package H1, the fake control store.
//!
//! | Row | Claim |
//! |---|---|
//! | M7F-10 | a `ControlInteraction` is recorded for every completed effect, `Terminated { termination, gap }` on a `TerminateWatch`; a `FamilyReload` carries the back-reference (K-F-06) |
//! | M7F-12 | two `snapshot_family` reads at one revision return identical records across an interleaved CAS (K-F-12) |
//! | M7F-13 | two nodes race a create-only CAS; `PlanCas { node: b }` and `DelayCompletion { node: a }` past the grant: `b` sees `Committed`, `a`'s late completion reports what really happened (K-F-14) |
//! | M7F-15 | a create-only CAS keyed on an `Absent { as_of }` older than the store after a create is `Conflict`, not `Committed` (K-F-20) |
//! | M7F-20 | `PlanReadUnavailable` makes the next `Get` `Unavailable` and the one after it real (F-R3/F-R5) |
//! | M7F-57 | every `CasResult` and `Value` echoes the request id its `Cas` or `Get` carried, unchanged, through real, planned, delayed and dropped completions; none carries another request's (L-R177hs) |
//!
//! Log fields are revisions, ticks and counts; never a key or value byte.

mod support;

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::contracts::control::{
    CasOutcome, ControlEffect, ControlEvent, ControlKey, ControlPrefix, ReadOutcome,
    WatchTermination,
};
use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::{ControlRequestId, EventId, NodeId, Revision};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{ControlOpKind, ControlOutcomeKind, TraceKind};
use rdb_sim::sim::control::{ControlOp, ControlStore};

const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const KEY: ControlKey = ControlKey::Grant(NodeId(1));

/// A create-only CAS of [`KEY`], sent as request `request`.
fn create(request: u64, value: &'static [u8]) -> ControlEffect {
    ControlEffect::Cas {
        request: ControlRequestId(request),
        key: KEY,
        expected: None,
        value: Some(Bytes::from_static(value)),
    }
}

/// One completion, or a failed assertion naming how many there were.
fn only(store: &mut ControlStore, now: Tick) -> rdb_sim::sim::control::Completion {
    let mut completions = store.complete(now);
    assert_eq!(completions.len(), 1, "exactly one completion expected");
    completions.remove(0)
}

#[retcd_test]
fn m7f_10_every_completion_records_a_control_interaction() {
    support::preamble();
    let mut store = ControlStore::new();

    store
        .submit(A, &support::control_effect(1, create(1, b"g")))
        .expect("cas");
    store
        .submit(
            A,
            &support::control_effect(
                2,
                ControlEffect::Get {
                    request: ControlRequestId(2),
                    key: KEY,
                },
            ),
        )
        .expect("get");
    store
        .submit(
            A,
            &support::control_effect(
                3,
                ControlEffect::Watch {
                    prefix: ControlPrefix::Grants,
                    from: Revision(0),
                },
            ),
        )
        .expect("watch");
    store
        .inject(ControlOp::TerminateWatch {
            node: A,
            termination: WatchTermination::ResourceExhaustedResumable,
        })
        .expect("terminate");
    store
        .submit(
            A,
            &support::control_effect(
                4,
                ControlEffect::Reload {
                    prefix: ControlPrefix::Grants,
                },
            ),
        )
        .expect("reload");

    let completions = store.complete(Tick::ZERO);
    let interactions = store.drain_interactions();
    tracing::info!(
        completions = completions.len(),
        interactions = interactions.len(),
        "m7f_10"
    );
    support::log_control_interactions(&interactions);

    assert_eq!(completions.len(), 4, "cas, get, terminated watch, reload");
    assert_eq!(
        interactions,
        vec![
            TraceKind::ControlInteraction {
                op: ControlOpKind::Cas,
                key: Some(KEY),
                prefix: None,
                outcome: ControlOutcomeKind::Committed,
            },
            TraceKind::ControlInteraction {
                op: ControlOpKind::Get,
                key: Some(KEY),
                prefix: None,
                outcome: ControlOutcomeKind::Found,
            },
            TraceKind::ControlInteraction {
                op: ControlOpKind::Watch,
                key: None,
                prefix: Some(ControlPrefix::Grants),
                outcome: ControlOutcomeKind::Terminated {
                    termination: WatchTermination::ResourceExhaustedResumable,
                    gap: true,
                },
            },
            TraceKind::ControlInteraction {
                op: ControlOpKind::Reload,
                key: None,
                prefix: Some(ControlPrefix::Grants),
                outcome: ControlOutcomeKind::Found,
            },
        ]
    );
    assert!(
        matches!(
            completions[2].event,
            ControlEvent::WatchTerminated {
                prefix: ControlPrefix::Grants,
                from: Revision(0),
                termination: WatchTermination::ResourceExhaustedResumable,
            }
        ),
        "the watch ended with the planned termination"
    );
    assert!(store.drain_interactions().is_empty(), "drained once");

    // The trap termination: the stream ends, and it is *not* a gap. Reloading on it in a loop
    // turns a capacity error into an outage.
    store
        .submit(
            A,
            &support::control_effect(
                5,
                ControlEffect::Watch {
                    prefix: ControlPrefix::Grants,
                    from: Revision(1),
                },
            ),
        )
        .expect("watch again");
    store
        .inject(ControlOp::TerminateWatch {
            node: A,
            termination: WatchTermination::ResourceExhaustedFatal,
        })
        .expect("terminate fatally");
    assert_eq!(store.complete(Tick::ZERO).len(), 1);
    let interactions = store.drain_interactions();
    support::log_control_interactions(&interactions);
    assert_eq!(
        interactions,
        vec![TraceKind::ControlInteraction {
            op: ControlOpKind::Watch,
            key: None,
            prefix: Some(ControlPrefix::Grants),
            outcome: ControlOutcomeKind::Terminated {
                termination: WatchTermination::ResourceExhaustedFatal,
                gap: false,
            },
        }]
    );

    // The reload the kernel emits after that termination carries the back-reference. The
    // event id is the recorder's, so the shape is what this row pins.
    let reload = TraceKind::FamilyReload {
        prefix: ControlPrefix::Grants,
        snapshot_revision: Revision(1),
        after_termination: Some(EventId(2)),
    };
    assert!(matches!(
        reload,
        TraceKind::FamilyReload {
            after_termination: Some(_),
            ..
        }
    ));
}

#[retcd_test]
fn m7f_12_family_snapshots_at_one_revision_are_identical_across_a_cas() {
    support::preamble();
    let mut store = ControlStore::new();
    store
        .submit(A, &support::control_effect(1, create(1, b"g1")))
        .expect("create");
    store.complete(Tick::ZERO);

    let (first_revision, first) = store
        .snapshot_family(ControlPrefix::Grants)
        .expect("first read");
    let (again_revision, again) = store
        .snapshot_family(ControlPrefix::Grants)
        .expect("second read");
    assert_eq!(first_revision, again_revision);
    assert_eq!(first, again, "no write between two reads: identical");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].key, KEY);

    // A CAS on the family, interleaved.
    store
        .submit(
            B,
            &support::control_effect(
                2,
                ControlEffect::Cas {
                    request: ControlRequestId(2),
                    key: ControlKey::Grant(B),
                    expected: None,
                    value: Some(Bytes::from_static(b"g2")),
                },
            ),
        )
        .expect("second create");
    store.complete(Tick::ZERO);

    let (later_revision, later) = store
        .snapshot_family(ControlPrefix::Grants)
        .expect("third read");
    tracing::info!(
        first_revision = first_revision.0,
        later_revision = later_revision.0,
        first_records = first.len(),
        later_records = later.len(),
        "m7f_12"
    );

    assert_eq!(first, again, "the earlier snapshot did not move");
    assert!(later_revision > first_revision);
    assert_eq!(later.len(), 2);
    assert_eq!(
        later[0], first[0],
        "the record that predates the CAS is unchanged"
    );
}

#[retcd_test]
fn m7f_13_two_nodes_race_a_create_and_the_late_completion_tells_the_truth() {
    support::preamble();
    let grant = Budgets::SPEC_DEFAULTS.grant_millis;
    let delay = grant + 100;
    let mut store = ControlStore::new();
    store
        .inject(ControlOp::PlanCas {
            node: B,
            outcome: CasOutcome::Committed(Revision(1)),
        })
        .expect("plan b");
    store
        .inject(ControlOp::DelayCompletion {
            node: A,
            by_millis: delay,
        })
        .expect("delay a");

    // b wins for real; the plan for b agrees with reality.
    store
        .submit(B, &support::control_effect(1, create(1, b"b")))
        .expect("b");
    // a's create arrives second, and loses for real.
    store
        .submit(A, &support::control_effect(2, create(2, b"a")))
        .expect("a");

    let now = Tick(10);
    let completions = store.complete(now);
    tracing::info!(
        completions = completions.len(),
        b_at = completions[0].at.0,
        a_at = completions[1].at.0,
        grant_millis = grant,
        "m7f_13"
    );

    assert_eq!(completions[0].node, B);
    assert_eq!(completions[0].at, now, "b's completion is not delayed");
    assert!(matches!(
        completions[0].event,
        ControlEvent::CasResult {
            key: KEY,
            outcome: CasOutcome::Committed(Revision(1)),
            ..
        }
    ));

    assert_eq!(completions[1].node, A);
    assert_eq!(
        completions[1].at,
        now.plus_millis(delay),
        "a's completion lands after the grant would have expired"
    );
    assert!(
        matches!(
            completions[1].event,
            ControlEvent::CasResult {
                key: KEY,
                outcome: CasOutcome::Conflict {
                    exists: true,
                    current: Revision(1),
                },
                ..
            }
        ),
        "a's completion reports what really happened, not b's plan"
    );
    assert_eq!(store.revision(), Revision(1), "exactly one write landed");
}

/// The other two planned answers a scenario can force: `Unknown` and `Unavailable` replace the
/// report and leave the state as it really is.
#[retcd_test]
fn m7f_13_a_planned_report_never_changes_the_state() {
    support::preamble();
    let mut store = ControlStore::new();
    store
        .inject(ControlOp::PlanCas {
            node: A,
            outcome: CasOutcome::Unknown,
        })
        .expect("plan");

    store
        .submit(A, &support::control_effect(1, create(1, b"a")))
        .expect("cas");
    let completion = only(&mut store, Tick::ZERO);

    assert!(matches!(
        completion.event,
        ControlEvent::CasResult {
            outcome: CasOutcome::Unknown,
            ..
        }
    ));
    assert_eq!(
        store.revision(),
        Revision(1),
        "the write landed even though the caller was told nothing is known"
    );
    assert!(matches!(store.get(KEY), ReadOutcome::Found { .. }));
}

#[retcd_test]
fn m7f_15_a_create_keyed_on_a_stale_absent_conflicts() {
    support::preamble();
    let mut store = ControlStore::new();

    // a reads: absent, as of revision 0.
    store
        .submit(
            A,
            &support::control_effect(
                1,
                ControlEffect::Get {
                    request: ControlRequestId(1),
                    key: KEY,
                },
            ),
        )
        .expect("get");
    let read = only(&mut store, Tick::ZERO);
    let ControlEvent::Value {
        outcome: ReadOutcome::Absent { as_of },
        ..
    } = read.event
    else {
        panic!(
            "a fresh store answers Absent with an as_of, got {:?}",
            read.event
        );
    };
    assert_eq!(as_of, Revision(0));

    // b creates in between.
    store
        .submit(B, &support::control_effect(2, create(2, b"b")))
        .expect("b creates");
    store.complete(Tick::ZERO);
    assert!(store.revision() > as_of, "the absence is now stale");

    // a's create-only CAS, keyed on that absence, must not win.
    store
        .submit(A, &support::control_effect(3, create(3, b"a")))
        .expect("a creates");
    let completion = only(&mut store, Tick::ZERO);
    tracing::info!(as_of = as_of.0, now = store.revision().0, "m7f_15");

    assert!(matches!(
        completion.event,
        ControlEvent::CasResult {
            outcome: CasOutcome::Conflict {
                exists: true,
                current: Revision(1),
            },
            ..
        }
    ));
}

#[retcd_test]
fn m7f_20_plan_read_unavailable_hits_the_next_get_only() {
    support::preamble();
    let mut store = ControlStore::new();
    store
        .submit(A, &support::control_effect(1, create(1, b"g")))
        .expect("create");
    store.complete(Tick::ZERO);
    store.inject(ControlOp::PlanReadUnavailable).expect("plan");

    store
        .submit(
            A,
            &support::control_effect(
                2,
                ControlEffect::Get {
                    request: ControlRequestId(2),
                    key: KEY,
                },
            ),
        )
        .expect("get 1");
    let first = only(&mut store, Tick::ZERO);
    store
        .submit(
            A,
            &support::control_effect(
                3,
                ControlEffect::Get {
                    request: ControlRequestId(3),
                    key: KEY,
                },
            ),
        )
        .expect("get 2");
    let second = only(&mut store, Tick::ZERO);

    assert!(matches!(
        first.event,
        ControlEvent::Value {
            outcome: ReadOutcome::Unavailable,
            ..
        }
    ));
    assert!(
        matches!(
            second.event,
            ControlEvent::Value {
                outcome: ReadOutcome::Found {
                    revision: Revision(1),
                    ..
                },
                ..
            }
        ),
        "the plan is consumed by one read"
    );
    let interactions = store.drain_interactions();
    support::log_control_interactions(&interactions);
    assert_eq!(
        interactions,
        vec![
            TraceKind::ControlInteraction {
                op: ControlOpKind::Cas,
                key: Some(KEY),
                prefix: None,
                outcome: ControlOutcomeKind::Committed,
            },
            TraceKind::ControlInteraction {
                op: ControlOpKind::Get,
                key: Some(KEY),
                prefix: None,
                outcome: ControlOutcomeKind::Unavailable,
            },
            TraceKind::ControlInteraction {
                op: ControlOpKind::Get,
                key: Some(KEY),
                prefix: None,
                outcome: ControlOutcomeKind::Found,
            },
        ]
    );
}

/// A dropped completion never arrives and leaves no interaction, because nothing completed.
#[retcd_test]
fn m7f_20_a_dropped_completion_leaves_no_trace_of_completing() {
    support::preamble();
    let mut store = ControlStore::new();
    store
        .inject(ControlOp::DropCompletion { node: A })
        .expect("plan");

    store
        .submit(A, &support::control_effect(1, create(1, b"g")))
        .expect("cas");
    let completions = store.complete(Tick::ZERO);

    assert!(completions.is_empty(), "the completion never arrives");
    assert!(store.drain_interactions().is_empty());
    assert_eq!(store.revision(), Revision(1), "but the write landed");
}

/// The request id a completion echoes, and whether it is a CAS answer.
fn echoed(completion: &rdb_sim::sim::control::Completion) -> (ControlRequestId, bool) {
    match completion.event {
        ControlEvent::CasResult { request, .. } => (request, true),
        ControlEvent::Value { request, .. } => (request, false),
        ref other => panic!("only CAS and read answers here, got {other:?}"),
    }
}

/// M7F-57 (lead ledger L-R177hs; Gautam 2026-09-27, "contract"): the store echoes the request id
/// each `Cas` and `Get` carried, unchanged, whatever the answer: a real one, a `PlanCas` or
/// `PlanReadUnavailable` report, and one `DelayCompletion` holds past a later request's. No
/// completion ever carries another request's id, with several requests on one key from one node
/// in flight together, and a `DropCompletion`'s id never reappears on the request after it.
#[retcd_test]
fn m7f_57_every_answer_echoes_its_own_request_id() {
    support::preamble();
    let mut store = ControlStore::new();
    for op in [
        ControlOp::DelayCompletion {
            node: A,
            by_millis: 500,
        },
        ControlOp::PlanCas {
            node: B,
            outcome: CasOutcome::Unknown,
        },
        ControlOp::PlanReadUnavailable,
    ] {
        store.inject(op).expect("plan");
    }
    let get = |request| ControlEffect::Get {
        request: ControlRequestId(request),
        key: KEY,
    };
    // Ids out of order and one correlation for all of them: only the id tells them apart.
    let sent = [
        (A, create(70, b"a")), // delayed past the rest
        (A, get(71)),          // planned Unavailable
        (B, create(9, b"b")),  // planned Unknown
        (A, get(72)),          // real
        (A, create(73, b"c")), // real
    ];
    for (node, effect) in &sent {
        store
            .submit(*node, &support::control_effect(1, effect.clone()))
            .expect("submit");
    }
    let completions = store.complete(Tick::ZERO);
    let got: Vec<(NodeId, ControlRequestId, bool)> = completions
        .iter()
        .map(|completion| {
            let (request, cas) = echoed(completion);
            (completion.node, request, cas)
        })
        .collect();
    tracing::info!(completions = got.len(), "m7f_57");
    assert_eq!(
        got,
        vec![
            (A, ControlRequestId(70), true),
            (A, ControlRequestId(71), false),
            (B, ControlRequestId(9), true),
            (A, ControlRequestId(72), false),
            (A, ControlRequestId(73), true),
        ],
        "each completion echoes its own request's id, and only that"
    );
    assert_eq!(
        completions[0].at,
        Tick(500),
        "the delayed one is the first request's"
    );
    assert!(matches!(
        completions[1].event,
        ControlEvent::Value {
            outcome: ReadOutcome::Unavailable,
            ..
        }
    ));
    assert!(matches!(
        completions[2].event,
        ControlEvent::CasResult {
            outcome: CasOutcome::Unknown,
            ..
        }
    ));

    // A dropped completion's id is gone; the next request on the key carries only its own.
    store
        .inject(ControlOp::DropCompletion { node: A })
        .expect("drop");
    store
        .submit(A, &support::control_effect(1, get(80)))
        .expect("dropped");
    assert!(store.complete(Tick(600)).is_empty(), "dropped");
    store
        .submit(A, &support::control_effect(1, get(81)))
        .expect("next");
    let next = only(&mut store, Tick(700));
    assert_eq!(echoed(&next), (ControlRequestId(81), false));
}
