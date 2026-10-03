//! The control fake's conformance suite: ADR-rdb-0008's "fake fidelity" row.
//!
//! The ADR asks for "a conformance test over the requirements in §7, asserted against the fake
//! itself so a later relaxation is caught". [`control_fake`] is that test, as a value a row can
//! run and log. Each check drives a fresh [`ControlStore`] through its public surface — `submit`,
//! `inject`, `complete` — with no kernel, and reports whether the fake still produced the hostile
//! behaviour the requirement names. A fake that became friendlier on any of them is a fake
//! against which gate V2's evidence is invalid (ADR-rdb-0008 §7, last paragraph).
//!
//! Which requirements, and why not the others:
//!
//! | Requirement | Here | Why |
//! |---|---|---|
//! | 1, 2, 3, 6, 7, 8 | yes | §7 as amended by A-R15: properties of the fake |
//! | 4 | **no** | restated by A-R15 as a kernel-side assertion over the trace; the fake cannot hold it |
//! | 5 | yes | deleted by A-R15, restored by lead ruling F-R3 as [`ControlOp::PlanReadUnavailable`] |
//!
//! The fake's own module table (`rdb_sim::sim::control`) is the source of that list.

use bytes::Bytes;
use rdb_core::contracts::control::{
    CasOutcome, ControlChange, ControlEffect, ControlEvent, ControlKey, ControlPrefix,
    ControlRecord, ReadOutcome, WatchCursor, WatchTermination,
};
use rdb_core::contracts::event::{Effect, EffectKind, ModuleName};
use rdb_core::contracts::ids::{ControlRequestId, CorrelationId, NodeId, PartitionId, Revision};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{ControlOutcomeKind, TraceKind};

use rdb_sim::sim::control::{Completion, ControlOp, ControlStore};

/// One requirement's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conformance {
    /// The ADR-rdb-0008 §7 requirement number.
    pub requirement: u8,
    /// What the fake must still do, in a few words.
    pub claim: &'static str,
    /// `Ok` when it did; otherwise what it did instead.
    pub outcome: Result<(), String>,
}

/// One requirement's check: `Ok`, or what the fake did instead.
type Check = fn() -> Result<(), String>;

/// Run every check, in requirement order, each on its own fresh store.
#[must_use]
pub fn control_fake() -> Vec<Conformance> {
    let checks: [(u8, &'static str, Check); 7] = [
        (
            1,
            "a CAS conflict reports exists and current, never the value",
            conflict,
        ),
        (
            2,
            "Unknown, Unavailable and Conflict are three answers; Unknown may have landed",
            unknown,
        ),
        (
            3,
            "all five watch terminations, and a progress tick",
            terminations,
        ),
        (
            5,
            "a planned read unavailable hits the next read only",
            read_unavailable,
        ),
        (
            6,
            "a family read at a revision a watch resumes from without a gap",
            coherent_reload,
        ),
        (
            7,
            "a completion delivered arbitrarily late still tells the truth",
            late_completion,
        ),
        (
            8,
            "a dropped completion never arrives, though the write landed",
            dropped_completion,
        ),
    ];
    checks
        .into_iter()
        .map(|(requirement, claim, check)| Conformance {
            requirement,
            claim,
            outcome: check(),
        })
        .collect()
}

const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const KEY: ControlKey = ControlKey::Partition(PartitionId(1));

fn effect(correlation: u64, control: ControlEffect) -> Effect {
    Effect {
        correlation: CorrelationId(correlation),
        from: ModuleName::Authority,
        partition: PartitionId(1),
        kind: EffectKind::Control(control),
    }
}

fn cas(request: u64, expected: Option<Revision>, value: &'static [u8]) -> ControlEffect {
    ControlEffect::Cas {
        request: ControlRequestId(request),
        key: KEY,
        expected,
        value: Some(Bytes::from_static(value)),
    }
}

fn submit(store: &mut ControlStore, node: NodeId, control: ControlEffect) -> Result<(), String> {
    store
        .submit(node, &effect(1, control))
        .map_err(|error| format!("submit refused: {error:?}"))
}

fn inject(store: &mut ControlStore, op: ControlOp) -> Result<(), String> {
    store
        .inject(op)
        .map_err(|error| format!("{op:?} refused: {error:?}"))
}

fn ensure(holds: bool, what: impl FnOnce() -> String) -> Result<(), String> {
    if holds {
        Ok(())
    } else {
        Err(what())
    }
}

/// The one completion `node`'s submission produced, drained at `now`.
fn only(store: &mut ControlStore, now: Tick) -> Result<Completion, String> {
    let mut completions = store.complete(now);
    ensure(completions.len() == 1, || {
        format!("expected one completion, got {completions:?}")
    })?;
    Ok(completions.remove(0))
}

fn cas_outcome(completion: &Completion) -> Result<CasOutcome, String> {
    match completion.event {
        ControlEvent::CasResult { outcome, .. } => Ok(outcome),
        ref other => Err(format!("expected a CAS result, got {other:?}")),
    }
}

/// Requirement 1. A loses a create-only race and is told who holds the key, not what it holds:
/// the conflict says `exists` and `current`, and the type has no value to give. A store that let
/// the second writer win, or blurred the race into `Unknown`, fails here.
fn conflict() -> Result<(), String> {
    let mut store = ControlStore::new();
    submit(&mut store, B, cas(1, None, b"b"))?;
    let won = cas_outcome(&only(&mut store, Tick(0))?)?;
    submit(&mut store, A, cas(2, None, b"a"))?;
    let lost = cas_outcome(&only(&mut store, Tick(0))?)?;
    ensure(won == CasOutcome::Committed(Revision(1)), || {
        format!("the first create commits: {won:?}")
    })?;
    ensure(
        lost == CasOutcome::Conflict {
            exists: true,
            current: Revision(1),
        },
        || format!("the second create conflicts with exists and current: {lost:?}"),
    )?;
    let held = store.get(KEY);
    ensure(
        held == ReadOutcome::Found {
            revision: Revision(1),
            value: Bytes::from_static(b"b"),
        },
        || format!("the winner's value stays: {held:?}"),
    )
}

/// Requirement 2. Forced `Unknown` and `Unavailable` reports are what the caller sees, a real
/// race is `Conflict`, and the three are distinct. `Unknown` means the write may have landed, and
/// here it did: the store applied the CAS whatever it reported.
fn unknown() -> Result<(), String> {
    let mut store = ControlStore::new();
    inject(
        &mut store,
        ControlOp::PlanCas {
            node: A,
            outcome: CasOutcome::Unknown,
        },
    )?;
    submit(&mut store, A, cas(1, None, b"a"))?;
    let unknown = cas_outcome(&only(&mut store, Tick(0))?)?;
    let landed = store.revision();
    inject(
        &mut store,
        ControlOp::PlanCas {
            node: A,
            outcome: CasOutcome::Unavailable,
        },
    )?;
    submit(&mut store, A, cas(2, Some(Revision(1)), b"a2"))?;
    let unavailable = cas_outcome(&only(&mut store, Tick(0))?)?;
    submit(&mut store, B, cas(3, None, b"b"))?;
    let conflict = cas_outcome(&only(&mut store, Tick(0))?)?;
    ensure(unknown == CasOutcome::Unknown, || {
        format!("the forced report is Unknown: {unknown:?}")
    })?;
    ensure(landed == Revision(1), || {
        format!("an Unknown CAS was applied for real: revision {landed:?}")
    })?;
    ensure(unavailable == CasOutcome::Unavailable, || {
        format!("the forced report is Unavailable: {unavailable:?}")
    })?;
    ensure(matches!(conflict, CasOutcome::Conflict { .. }), || {
        format!("a real race is a Conflict: {conflict:?}")
    })?;
    ensure(
        unknown != unavailable && unknown != conflict && unavailable != conflict,
        || "the three answers are distinct".to_owned(),
    )
}

/// Requirement 3. Each of the five terminations reaches the watcher as itself, with the gap
/// flag its type gives it, and a progress tick carries the store's revision. One more: the fake
/// produces `RevisionCompacted` **on its own** for a watcher left behind a compaction, so a gap is
/// something the store does, not only something a scenario says.
fn terminations() -> Result<(), String> {
    let mut store = ControlStore::new();
    let watch = |store: &mut ControlStore| {
        let from = store.revision();
        submit(
            store,
            A,
            ControlEffect::Watch {
                prefix: ControlPrefix::Partitions,
                from,
            },
        )
    };
    let all = [
        WatchTermination::RevisionCompacted {
            minimum_available_revision: Revision(7),
        },
        WatchTermination::ResourceExhaustedResumable,
        WatchTermination::ResourceExhaustedFatal,
        WatchTermination::NotLeader,
        WatchTermination::Unavailable,
    ];
    for termination in all {
        watch(&mut store)?;
        inject(
            &mut store,
            ControlOp::TerminateWatch {
                node: A,
                termination,
            },
        )?;
        let event = only(&mut store, Tick(0))?.event;
        ensure(
            event
                == ControlEvent::WatchTerminated {
                    prefix: ControlPrefix::Partitions,
                    from: store.revision(),
                    termination,
                },
            || format!("{termination:?} delivered as itself: {event:?}"),
        )?;
        let recorded = store.drain_interactions();
        ensure(
            matches!(
                recorded.as_slice(),
                [TraceKind::ControlInteraction {
                    outcome: ControlOutcomeKind::Terminated { termination: t, gap },
                    ..
                }] if *t == termination && *gap == termination.is_gap()
            ),
            || format!("{termination:?} recorded with its gap flag: {recorded:?}"),
        )?;
        ensure(store.open_watches(A) == 0, || {
            format!("{termination:?} ended the watch")
        })?;
    }

    watch(&mut store)?;
    submit(&mut store, B, cas(1, None, b"b"))?;
    let _ = store.complete(Tick(0));
    inject(&mut store, ControlOp::EmitProgress { node: A })?;
    let progress = only(&mut store, Tick(0))?.event;
    ensure(
        progress
            == ControlEvent::WatchProgress {
                prefix: ControlPrefix::Partitions,
                revision: store.revision(),
            },
        || format!("progress carries the store's revision: {progress:?}"),
    )?;

    submit(&mut store, B, cas(2, Some(Revision(1)), b"b2"))?;
    let _ = store.complete(Tick(0));
    inject(&mut store, ControlOp::Compact { up_to: Revision(2) })?;
    // The watch opened above is still at revision 0 — a progress tick does not move it — so it
    // is now behind the compaction.
    inject(&mut store, ControlOp::EmitWatch { node: A })?;
    let compacted = only(&mut store, Tick(0))?.event;
    ensure(
        compacted
            == ControlEvent::WatchTerminated {
                prefix: ControlPrefix::Partitions,
                from: Revision(0),
                termination: WatchTermination::RevisionCompacted {
                    minimum_available_revision: Revision(2),
                },
            },
        || format!("a watcher behind a compaction is told so: {compacted:?}"),
    )
}

/// Requirement 5, as restored by lead ruling F-R3. The planned answer is the next read's and only
/// its: a fault that stuck would disable every later read, and one that never fired would hide a
/// lost control plane.
fn read_unavailable() -> Result<(), String> {
    let mut store = ControlStore::new();
    store
        .seed(KEY, Bytes::from_static(b"v"))
        .map_err(|error| format!("seed refused: {error:?}"))?;
    inject(&mut store, ControlOp::PlanReadUnavailable)?;
    let read = |store: &mut ControlStore, request: u64| -> Result<ReadOutcome, String> {
        submit(
            store,
            A,
            ControlEffect::Get {
                request: ControlRequestId(request),
                key: KEY,
            },
        )?;
        match only(store, Tick(0))?.event {
            ControlEvent::Value { outcome, .. } => Ok(outcome),
            other => Err(format!("expected a value, got {other:?}")),
        }
    };
    let first = read(&mut store, 1)?;
    let second = read(&mut store, 2)?;
    ensure(first == ReadOutcome::Unavailable, || {
        format!("the planned read is Unavailable: {first:?}")
    })?;
    ensure(matches!(second, ReadOutcome::Found { .. }), || {
        format!("the next read is the store's own answer: {second:?}")
    })
}

/// Requirement 6. A reload returns the family at one revision, and a watch from that revision
/// delivers exactly the writes after it: nothing skipped, nothing repeated.
fn coherent_reload() -> Result<(), String> {
    let mut store = ControlStore::new();
    let other = ControlKey::Partition(PartitionId(2));
    for key in [KEY, other] {
        store
            .seed(key, Bytes::from_static(b"v"))
            .map_err(|error| format!("seed refused: {error:?}"))?;
    }
    submit(
        &mut store,
        A,
        ControlEffect::Reload {
            prefix: ControlPrefix::Partitions,
        },
    )?;
    let ControlEvent::FamilySnapshot {
        snapshot_revision,
        records,
        ..
    } = only(&mut store, Tick(0))?.event
    else {
        return Err("a reload answers with a family snapshot".to_owned());
    };
    let keys: Vec<ControlKey> = records
        .iter()
        .map(|record: &ControlRecord| record.key)
        .collect();
    ensure(
        snapshot_revision == Revision(2) && keys == [KEY, other],
        || format!("the family at one revision: {snapshot_revision:?} {keys:?}"),
    )?;
    submit(&mut store, B, cas(1, Some(Revision(1)), b"w"))?;
    let _ = store.complete(Tick(0));
    submit(
        &mut store,
        A,
        ControlEffect::Watch {
            prefix: ControlPrefix::Partitions,
            from: snapshot_revision,
        },
    )?;
    inject(&mut store, ControlOp::EmitWatch { node: A })?;
    let watched = only(&mut store, Tick(0))?.event;
    ensure(
        watched
            == ControlEvent::Watched {
                prefix: ControlPrefix::Partitions,
                cursor: WatchCursor {
                    revision: Revision(3),
                },
                changes: vec![ControlChange {
                    key: KEY,
                    revision: Revision(3),
                }],
            },
        || format!("a watch from the snapshot sees only the later write: {watched:?}"),
    )
}

/// Requirement 7. A held completion arrives `by_millis` late, reports what really happened, and
/// holds back only the node it names.
fn late_completion() -> Result<(), String> {
    let mut store = ControlStore::new();
    inject(
        &mut store,
        ControlOp::DelayCompletion {
            node: A,
            by_millis: 5_000,
        },
    )?;
    submit(&mut store, A, cas(1, None, b"a"))?;
    submit(
        &mut store,
        B,
        ControlEffect::Get {
            request: ControlRequestId(2),
            key: KEY,
        },
    )?;
    let completions = store.complete(Tick(10));
    let at: Vec<(NodeId, Tick)> = completions.iter().map(|c| (c.node, c.at)).collect();
    ensure(at == [(A, Tick(5_010)), (B, Tick(10))], || {
        format!("A's completion is late, B's is not: {at:?}")
    })?;
    let outcome = cas_outcome(&completions[0])?;
    ensure(outcome == CasOutcome::Committed(Revision(1)), || {
        format!("the late completion tells the truth: {outcome:?}")
    })
}

/// Requirement 8. The completion never arrives and leaves no interaction, while the write it
/// would have reported is in the store.
fn dropped_completion() -> Result<(), String> {
    let mut store = ControlStore::new();
    inject(&mut store, ControlOp::DropCompletion { node: A })?;
    submit(&mut store, A, cas(1, None, b"a"))?;
    let first = store.complete(Tick(0));
    let later = store.complete(Tick(1_000_000));
    let recorded = store.drain_interactions();
    ensure(first.is_empty() && later.is_empty(), || {
        format!("nothing arrives, now or later: {first:?} {later:?}")
    })?;
    ensure(recorded.is_empty(), || {
        format!("nothing completed, so nothing is recorded: {recorded:?}")
    })?;
    ensure(store.revision() == Revision(1), || {
        format!("the write landed: {:?}", store.revision())
    })
}
