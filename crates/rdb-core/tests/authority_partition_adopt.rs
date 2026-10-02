//! The single-record `partitions/{id}` adopt path (team kernel-a `design.md` §2.4, the partition
//! lineage path), built in the A1 phase-2 dispatch as item §3.2.
//!
//! Not an `M7A-*` row. This is the build's own evidence, written test-first against the design
//! table, so the behaviour it adds has been seen to fail before the plan rows that will own it are
//! written. It asserts one thing per function and names the design row each one reads.
//!
//! # What it is built on, and what replaces that
//!
//! [`held_kernel`] reached `Held` through the one-event `CasResult` preamble until lead ruling
//! A-R47 retired it: a commit this kernel did not issue must not grant a hold. It now reaches
//! `Held` through the real `AcquireDue → Cas → Committed` sequence (§3.1). That was the **only**
//! function here that depended on the shortcut, and it is the only one that changed: every row
//! below starts from `Held` and does not care how it got there.

use config_log::retcd_test;

use bytes::Bytes;
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::authority::{Authority, AuthorityTimer, ServedLineage};
use rdb_core::contracts::authority::{
    AuthorityEffect, AuthorityEvent, AuthorityFact, AuthorityIgnoreReason, AuthorityView,
    Checkpoint, DenyReason, FenceScope, FencingProof, Lineage, PartitionMode, Revocation, Verdict,
};
use rdb_core::contracts::control::{
    CasOutcome, ControlChange, ControlEffect, ControlEvent, ControlKey, ControlPrefix,
    ControlRecord, ReadOutcome, WatchCursor,
};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module,
    NodeLifecycle, StepCtx,
};
use rdb_core::contracts::ids::{
    AuthorityGeneration, BatchId, BootId, ConfigVersion, ControlRequestId, CorrelationId, EventId,
    Generation, GrantId, NodeId, OwnerEpoch, PartitionId, Revision, Seq, SnapshotHandle,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::membership::{CopyId, PartitionConfig};
use rdb_core::contracts::recovery::{
    CommittedRoot, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap, SelectedLineage,
};
use rdb_core::contracts::storage::{
    Namespace, SnapshotRead, StorageEvent, StorageFault, StoreEffect,
};
use rdb_core::contracts::time::{ControlTime, Tick, TimerFired};
use rdb_core::contracts::trace::Version;

const NODE: NodeId = NodeId(1);
const OTHER: NodeId = NodeId(2);
const BOOT: BootId = BootId(1);
const P1: PartitionId = PartitionId(1);
const P2: PartitionId = PartitionId(2);

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

/// One fixed, bounded sample for every step, so no effect vector below carries a view published
/// because the sample moved (lead ruling A-R45).
fn ctx(now: u64) -> StepCtx<'static> {
    StepCtx {
        now: Tick(now),
        control_time: ControlTime {
            estimate: Tick(1_000_000),
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

fn event(id: u64, control: ControlEvent) -> Event {
    event_of(id, EventKind::Control(control))
}

/// [`event`] for any kind: `p1`'s context, correlated by its own id.
fn event_of(id: u64, kind: EventKind) -> Event {
    Event {
        id: EventId(id),
        at: Tick(id),
        node: NODE,
        boot: BOOT,
        partition: P1,
        correlation: CorrelationId(id),
        kind,
    }
}

fn record(partition: PartitionId, owner: NodeId, epoch: u64) -> PartitionRecord {
    PartitionRecord {
        partition,
        owner,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(epoch),
        config_version: ConfigVersion(1),
        lifecycle: PartitionLifecycle::Serving,
    }
}

fn lineage(epoch: u64) -> ServedLineage {
    record(P1, NODE, epoch).lineage()
}

/// Into `Held` through the real acquisition (lead ruling A-R47): `AcquireDue` issues the
/// create-only CAS, and its commit, under the same correlation, adopts it.
fn held_kernel() -> Authority {
    acquired().0
}

/// [`held_kernel`], and the acquisition commit's effect vector.
fn acquired() -> (Authority, Vec<Effect>) {
    acquired_under(&ctx(0))
}

/// [`acquired`], both steps under `step_ctx`: a row that needs its own sample or budgets
/// acquires under them, so `E` and `renewed_at` come from the real sequence.
fn acquired_under(step_ctx: &StepCtx<'_>) -> (Authority, Vec<Effect>) {
    let mut kernel = Authority::new();
    let effects = acquire(&mut kernel, step_ctx);
    (kernel, effects)
}

/// The acquisition [`acquired_under`] runs, on a kernel that already exists: an `Unheld` one
/// that has seen other events first. Returns the commit's effect vector.
fn acquire(kernel: &mut Authority, step_ctx: &StepCtx<'_>) -> Vec<Effect> {
    // The same id and correlation as `event(1, ..)` below. The commit is matched to the CAS by the
    // request id it echoes (lead ledger L-R177hs), which [`in_flight`] reads.
    let due = Event {
        id: EventId(1),
        at: Tick(0),
        node: NODE,
        boot: BOOT,
        partition: P1,
        correlation: CorrelationId(1),
        kind: EventKind::Timer(TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: kernel.timer_version(AuthorityTimer::Acquire),
            scheduled_at: Tick(0),
        }),
    };
    kernel
        .step(step_ctx, &due)
        .expect("the AcquireDue row is built");
    let request = in_flight(kernel);
    let effects = kernel
        .step(
            step_ctx,
            &event(
                1,
                ControlEvent::CasResult {
                    request,
                    key: ControlKey::Grant(NODE),
                    outcome: CasOutcome::Committed(Revision(7)),
                },
            ),
        )
        .expect("the acquisition commit row is built");
    assert!(kernel.state().is_held(), "the preamble must reach Held");
    effects
}

/// A request id A1 never minted: its ids start above `AUTHORITY_CONTROL_REQUEST_BASE`. Carried by
/// reads A1 judges by content — its own grant, a partition read that is no `Recovered` read-back —
/// and by answers to no request of A1's.
const UNASKED: ControlRequestId = ControlRequestId(0);

/// The request id of the grant CAS in flight: the acquisition while `Unheld`, else the renewal.
fn in_flight(kernel: &Authority) -> ControlRequestId {
    let view = kernel.view();
    view.acquire
        .map(|acquire| acquire.request)
        .or_else(|| view.renewal.map(|renewal| renewal.request))
        .expect("fixture: a grant CAS in flight")
}

/// The request id of the one control `Get` in `effects`.
fn asked(effects: &[Effect]) -> ControlRequestId {
    let asked: Vec<ControlRequestId> = effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Control(ControlEffect::Get { request, .. }) => Some(*request),
            _ => None,
        })
        .collect();
    let [request] = asked.as_slice() else {
        panic!("fixture: one Get: {effects:?}");
    };
    *request
}

/// `Held`, with `p1` installed at `epoch` by a coherent snapshot at revision 10.
fn serving_p1(epoch: u64) -> Authority {
    let mut kernel = held_kernel();
    kernel
        .step(
            &ctx(2),
            &event(
                2,
                ControlEvent::FamilySnapshot {
                    prefix: ControlPrefix::Partitions,
                    snapshot_revision: Revision(10),
                    records: vec![ControlRecord {
                        key: ControlKey::Partition(P1),
                        revision: Revision(9),
                        value: record(P1, NODE, epoch).encode(),
                    }],
                },
            ),
        )
        .expect("the family-snapshot install is built");
    let view = kernel.view();
    assert_eq!(view.served.get(&P1), Some(&lineage(epoch)), "fixture");
    assert_eq!(view.partitions_revision, Some(Revision(10)), "fixture");
    kernel
}

/// A linearizable read of `partitions/{key}` answering `body` at `revision`, to no request A1
/// matches by id: the watch-driven and generic reads, which A1 judges by content.
fn read(id: u64, key: PartitionId, revision: u64, body: &PartitionRecord) -> Event {
    read_as(UNASKED, id, key, revision, body)
}

/// [`read`], answering the `Get` sent as `request`: a `Recovered` read-back is matched by it.
fn read_as(
    request: ControlRequestId,
    id: u64,
    key: PartitionId,
    revision: u64,
    body: &PartitionRecord,
) -> Event {
    event(
        id,
        ControlEvent::Value {
            request,
            key: ControlKey::Partition(key),
            outcome: ReadOutcome::Found {
                revision: Revision(revision),
                value: body.encode(),
            },
        },
    )
}

/// An effect vector reduced to what these rows assert on, in order.
#[derive(Debug, PartialEq, Eq)]
enum Shape {
    Adopt(PartitionId, OwnerEpoch),
    Publish(PartitionId),
    Fact(AuthorityFact),
    Ignored(AuthorityIgnoreReason),
    Fence(DenyReason),
    Other,
}

fn shapes(effects: &[Effect]) -> Vec<Shape> {
    effects
        .iter()
        .map(|effect| match &effect.kind {
            EffectKind::AdoptAuthority {
                partition,
                owner_epoch,
                ..
            } => Shape::Adopt(*partition, *owner_epoch),
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::PublishAuthorityView(
                view,
            ))) => Shape::Publish(view.lineage.partition),
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fact(fact))) => {
                Shape::Fact(fact.clone())
            }
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fence {
                reason, ..
            })) => Shape::Fence(*reason),
            EffectKind::Kernel(KernelEffect::Ignored {
                reason: KernelIgnoredReason::Authority(reason),
            }) => Shape::Ignored(reason.clone()),
            _ => Shape::Other,
        })
        .collect()
}

/// `design.md` §2.4, the generic changed row: `part.owner == us`, `part.revision >
/// partitions_revision`, `part` differs from `served[id]` ⇒ `AdoptAuthority`,
/// `PublishAuthorityView`, `Fact(LineageChanged)`; `authority_seq += 1`; `served[id]` rewritten.
#[retcd_test]
fn a_newer_record_of_ours_is_adopted_as_lineage_changed() {
    let mut kernel = serving_p1(1);
    let seq = kernel.authority_seq();

    let effects = kernel
        .step(&ctx(3), &read(3, P1, 15, &record(P1, NODE, 2)))
        .expect("the single-record adopt row is built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Adopt(P1, OwnerEpoch(2)),
            Shape::Publish(P1),
            Shape::Fact(AuthorityFact::LineageChanged),
        ],
        "the three emit points of a served write, in the install path's order (K-A-49 pairing)"
    );
    let view = kernel.view();
    assert_eq!(
        view.served.get(&P1),
        Some(&lineage(2)),
        "served[p1] is the new lineage"
    );
    assert_eq!(
        view.authority_seq,
        seq + 1,
        "a write of served bumps authority_seq exactly once (K-A-34)"
    );
    assert_eq!(
        view.partitions_revision,
        Some(Revision(10)),
        "partitions_revision is the coherent snapshot's revision, and a single record is not a \
         coherent snapshot — the design row writes served[id] and nothing else"
    );
}

/// The same row, where `served[id]` is absent rather than different: a partition of ours this
/// node was not yet serving. Absence differs from every lineage, so it is the changed row, and a
/// linearizable read is exactly what property 3 lets widen rights.
#[retcd_test]
fn a_record_of_ours_not_yet_served_is_adopted() {
    let mut kernel = serving_p1(1);

    let effects = kernel
        .step(&ctx(3), &read(3, P2, 15, &record(P2, NODE, 1)))
        .expect("the single-record adopt row is built");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Adopt(P2, OwnerEpoch(1)),
            Shape::Publish(P2),
            Shape::Fact(AuthorityFact::LineageChanged),
        ]
    );
    let served = kernel.view().served;
    assert_eq!(
        served.keys().copied().collect::<Vec<_>>(),
        vec![P1, P2],
        "p2 joins served; p1 is untouched by a read of a different key"
    );
}

/// `design.md` §2.4, the unchanged row: `part.owner == us`, unchanged ⇒ no write, no bump, no
/// view. Spelled `Ignored(LineageUnchanged)` by lead ruling A-R41, not the design's `Fact`.
#[retcd_test]
fn rereading_the_installed_record_is_lineage_unchanged() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read(3, P1, 15, &record(P1, NODE, 2)))
        .expect("adopt");
    let seq = kernel.authority_seq();

    let effects = kernel
        .step(&ctx(4), &read(4, P1, 15, &record(P1, NODE, 2)))
        .expect("the unchanged row is built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::LineageUnchanged)],
        "nothing moved, and that has to be something a row can assert"
    );
    assert_eq!(
        kernel.authority_seq(),
        seq,
        "no write of served, so no bump"
    );
    assert_eq!(kernel.view().served.get(&P1), Some(&lineage(2)));
}

/// A read **older** than the coherent snapshot already installed, carrying a lineage that differs
/// from it. The design table has no row for this — its changed row requires `part.revision >
/// partitions_revision` and its unchanged row requires equality — and installing it would roll
/// `served` back to a lineage the snapshot has already superseded.
///
/// Reachable only when read responses arrive out of order with a snapshot, but that is a network
/// property and not an adversarial one.
#[retcd_test]
fn a_read_older_than_the_installed_snapshot_is_superseded() {
    let mut kernel = serving_p1(2);
    let seq = kernel.authority_seq();

    let effects = kernel
        .step(&ctx(3), &read(3, P1, 8, &record(P1, NODE, 1)))
        .expect("the superseded read is answered, not unavailable");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(
            AuthorityIgnoreReason::PartitionReadSuperseded
        )]
    );
    assert_eq!(
        kernel.view().served.get(&P1),
        Some(&lineage(2)),
        "the snapshot's newer lineage stands"
    );
    assert_eq!(kernel.authority_seq(), seq, "no write, no bump");
}

/// A body that names a different partition than the key it was read from. Not a partition record
/// A1 can read *for that key*, so it widens nothing. Only the adopt branch is guarded: a record
/// naming another owner still reaches the landed fence path unchanged.
#[retcd_test]
fn a_body_naming_another_partition_widens_nothing() {
    let mut kernel = serving_p1(1);
    let before = kernel.view();

    let effects = kernel
        .step(&ctx(3), &read(3, P1, 15, &record(P2, NODE, 5)))
        .expect("a mismatched body is answered, not unavailable");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::FamilyRejected)]
    );
    let after = kernel.view();
    assert_eq!(
        after.served, before.served,
        "nothing adopted under either id"
    );
    assert_eq!(after.authority_seq, before.authority_seq);

    // Positive control on the same fixture: a record of someone else's still fences exactly as
    // before this build, so the guard above is scoped to the branch that widens rights.
    let effects = kernel
        .step(&ctx(4), &read(4, P1, 16, &record(P1, OTHER, 5)))
        .expect("the landed fence path");
    assert!(
        shapes(&effects).contains(&Shape::Fence(DenyReason::GenerationChanged)),
        "the owner-moved fence is unchanged: {:?}",
        shapes(&effects)
    );
}

// ---------------------------------------------------------------------------------------------
// Lead ruling A-R48: a per-partition installed revision, so reordered replies cannot roll
// `served` back. Both interleavings below published a lower `owner_epoch` after a higher one
// before the ruling was built.
// ---------------------------------------------------------------------------------------------

/// A coherent partitions snapshot at `revision` listing `records`.
fn snapshot(id: u64, revision: u64, records: &[PartitionRecord]) -> Event {
    event(
        id,
        ControlEvent::FamilySnapshot {
            prefix: ControlPrefix::Partitions,
            snapshot_revision: Revision(revision),
            records: records
                .iter()
                .map(|record| ControlRecord {
                    key: ControlKey::Partition(record.partition),
                    revision: Revision(revision),
                    value: record.encode(),
                })
                .collect(),
        },
    )
}

/// Interleaving 1: two reads of one partition, delivered newest first. Before A-R48 the older
/// read passed the snapshot-wide guard (`15 > 10`) and installed epoch 2 over epoch 3.
#[retcd_test]
fn two_reads_of_one_partition_delivered_newest_first_do_not_roll_back() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read(3, P1, 20, &record(P1, NODE, 3)))
        .expect("adopt at 20");
    let seq = kernel.authority_seq();

    let effects = kernel
        .step(&ctx(4), &read(4, P1, 15, &record(P1, NODE, 2)))
        .expect("the older read is answered, not unavailable");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(
            AuthorityIgnoreReason::PartitionReadSuperseded
        )],
        "15 is older than the revision p1 was installed at (20), so it is superseded — not \
         merely older than the snapshot's 10"
    );
    let view = kernel.view();
    assert_eq!(view.served.get(&P1), Some(&lineage(3)), "epoch 3 stands");
    assert_eq!(
        view.served_revisions.get(&P1),
        Some(&Revision(20)),
        "and the revision it was installed at is observable, beside `served` (A-R48)"
    );
    assert_eq!(kernel.authority_seq(), seq, "no write, no bump");
}

/// Interleaving 2: a reload snapshot taken at 17, delivered after a single read at 20. Before
/// A-R48 the snapshot passed its gate (`17 >= 10`) and replaced epoch 3 with its own epoch 2.
#[retcd_test]
fn an_older_snapshot_delivered_after_a_newer_read_keeps_the_newer_entry() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read(3, P1, 20, &record(P1, NODE, 3)))
        .expect("adopt at 20");

    kernel
        .step(
            &ctx(4),
            &snapshot(4, 17, &[record(P1, NODE, 2), record(P2, NODE, 1)]),
        )
        .expect("the snapshot install is built");

    let view = kernel.view();
    assert_eq!(
        view.served.get(&P1),
        Some(&lineage(3)),
        "p1's entry was installed at 20 > 17: newer than the snapshot about that key, so it \
         survives the replace"
    );
    assert_eq!(view.served_revisions.get(&P1), Some(&Revision(20)));
    assert_eq!(
        view.served.get(&P2),
        Some(&record(P2, NODE, 1).lineage()),
        "every other entry comes from the snapshot"
    );
    assert_eq!(
        view.served_revisions.get(&P2),
        Some(&Revision(17)),
        "and is installed at the snapshot's revision"
    );
    assert_eq!(view.partitions_revision, Some(Revision(17)));
}

/// The carve-out's limit, which is what keeps it from reopening merge (lead ruling A-R37). A
/// snapshot that lacks a key drops it **unless** that key was installed strictly after the
/// snapshot was taken. Both cases in one fixture, so the survivor and the dropped entry are judged
/// by the same install.
#[retcd_test]
fn a_snapshot_drops_what_it_lacks_except_what_is_strictly_newer() {
    let mut kernel = serving_p1(1); // p1 installed at 10
    kernel
        .step(&ctx(3), &read(3, P2, 25, &record(P2, NODE, 1)))
        .expect("adopt p2 at 25");

    kernel
        .step(&ctx(4), &snapshot(4, 17, &[]))
        .expect("an empty snapshot is a coherent answer");

    let view = kernel.view();
    assert_eq!(
        view.served.keys().copied().collect::<Vec<_>>(),
        vec![P2],
        "p1 (installed at 10, not newer than 17) is dropped — replace, not merge; p2 (installed \
         at 25) survives a snapshot that does not list it"
    );
    assert_eq!(
        view.served_revisions.keys().copied().collect::<Vec<_>>(),
        vec![P2],
        "the revision map mirrors served exactly"
    );
}

/// The fallback guard, for a key that was **never ours as of the snapshot**: a snapshot at 30
/// says p2 belongs to another node, and a single read of p2 taken at 20 — before that snapshot —
/// arrives afterwards saying it is ours. The read is older than what the node already knows about
/// p2, so it must not widen rights.
///
/// Separate from the tombstone rows below on purpose (lead review of §3.1): those cover "was
/// ours, then removed"; this covers a key with no entry and no removal, where the only thing a
/// read must be newer than is `partitions_revision`. A surviving mutant — that fallback replaced
/// with `Revision(0)` — showed nothing pinned it.
#[retcd_test]
fn a_read_older_than_a_snapshot_that_denied_the_partition_adopts_nothing() {
    let mut kernel = serving_p1(1);
    kernel
        .step(
            &ctx(3),
            &snapshot(3, 30, &[record(P1, NODE, 1), record(P2, OTHER, 1)]),
        )
        .expect("the snapshot install is built");
    let seq = kernel.authority_seq();

    let effects = kernel
        .step(&ctx(4), &read(4, P2, 20, &record(P2, NODE, 1)))
        .expect("the older read is answered, not unavailable");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(
            AuthorityIgnoreReason::PartitionReadSuperseded
        )],
        "20 is older than the snapshot at 30 that denied p2, so it is superseded"
    );
    let view = kernel.view();
    assert!(!view.served.contains_key(&P2), "p2 is not adopted");
    assert!(
        view.removed_revisions.is_empty(),
        "no removal happened here: the guard that held is the snapshot fallback, not a tombstone"
    );
    assert_eq!(kernel.authority_seq(), seq, "no write, no bump");
}

// ---------------------------------------------------------------------------------------------
// Lead ruling A-R48b: a removal keeps its revision as a tombstone, the entry absent. Before it,
// both removal sites dropped the entry and its revision together, so a read of ours delivered
// after the fence re-adopted the partition — re-granting serving rights after a fence.
// ---------------------------------------------------------------------------------------------

/// A1's own peer-event arm: an epoch revocation is now durable.
fn revocation_persisted(id: u64, partition: PartitionId, epoch: u64) -> Event {
    Event {
        id: EventId(id),
        at: Tick(id),
        node: NODE,
        boot: BOOT,
        partition: P1,
        correlation: CorrelationId(id),
        kind: EventKind::Kernel(KernelEvent::Authority(
            AuthorityEvent::EpochRevocationPersisted {
                partition,
                epoch: OwnerEpoch(epoch),
            },
        )),
    }
}

/// A linearizable read of `partitions/{key}` answering that there is no record, as of `as_of`.
fn read_absent(id: u64, key: PartitionId, as_of: u64) -> Event {
    event(
        id,
        ControlEvent::Value {
            request: UNASKED,
            key: ControlKey::Partition(key),
            outcome: ReadOutcome::Absent {
                as_of: Revision(as_of),
            },
        },
    )
}

/// Interleaving 1 of A-R48b: a read at 25 fences p1 (the owner moved); a read of ours taken at
/// 15 arrives after it. Before the ruling the key fell back to the snapshot's 10, and 15 > 10
/// re-adopted a partition this node had just been fenced from.
#[retcd_test]
fn a_read_older_than_the_fence_that_removed_the_partition_adopts_nothing() {
    let mut kernel = serving_p1(1);
    let effects = kernel
        .step(&ctx(3), &read(3, P1, 25, &record(P1, OTHER, 1)))
        .expect("the owner-moved fence");
    assert!(
        shapes(&effects).contains(&Shape::Fence(DenyReason::GenerationChanged)),
        "fixture: the read at 25 fences p1"
    );
    let view = kernel.view();
    assert!(!view.served.contains_key(&P1), "fixture: p1 removed");
    assert_eq!(
        view.removed_revisions.get(&P1),
        Some(&Revision(25)),
        "the removal keeps the revision it was learned at, with the entry absent (A-R48b)"
    );
    let seq = kernel.authority_seq();

    let effects = kernel
        .step(&ctx(4), &read(4, P1, 15, &record(P1, NODE, 1)))
        .expect("the older read is answered, not unavailable");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(
            AuthorityIgnoreReason::PartitionReadSuperseded
        )],
        "15 is older than the removal at 25: no serving rights come back after a fence"
    );
    assert!(!kernel.view().served.contains_key(&P1), "p1 stays removed");
    assert_eq!(kernel.authority_seq(), seq, "no write, no bump");
}

/// Interleaving 2 of A-R48b: a read at 25 finds p1's record gone; a reload snapshot taken at 17
/// arrives after it, still listing p1 as ours. The snapshot's gate (`17 >= 10`) passes, and
/// before the ruling it re-installed p1. A tombstone newer than the snapshot keeps the key absent.
#[retcd_test]
fn a_snapshot_older_than_the_fence_that_removed_a_partition_keeps_it_removed() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read_absent(3, P1, 25))
        .expect("the absent-record fence");
    assert_eq!(
        kernel.view().removed_revisions.get(&P1),
        Some(&Revision(25)),
        "fixture: an absent read's removal revision is its as_of"
    );

    kernel
        .step(
            &ctx(4),
            &snapshot(4, 17, &[record(P1, NODE, 1), record(P2, NODE, 1)]),
        )
        .expect("the snapshot install is built");

    let view = kernel.view();
    assert_eq!(
        view.served.keys().copied().collect::<Vec<_>>(),
        vec![P2],
        "p1 was removed at 25 > 17: the snapshot could not have seen that, so p1 stays absent; \
         p2 comes from the snapshot as usual"
    );
    assert_eq!(
        view.removed_revisions.get(&P1),
        Some(&Revision(25)),
        "and the tombstone survives, since it is still newer than what was installed"
    );
    assert_eq!(view.partitions_revision, Some(Revision(17)));
}

/// A removal never lowers what the node knew about the key. p1 was installed at 20 by a read; a
/// **stale** read at 18 then fences it. The fence stands — narrowing rights needs no ordering —
/// but the tombstone is 20, not 18, or a read of ours at 19 would re-adopt an older lineage.
#[retcd_test]
fn a_removal_keeps_the_revision_the_entry_was_installed_at() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read(3, P1, 20, &record(P1, NODE, 3)))
        .expect("adopt at 20");
    kernel
        .step(&ctx(4), &read(4, P1, 18, &record(P1, OTHER, 3)))
        .expect("the owner-moved fence");
    assert_eq!(
        kernel.view().removed_revisions.get(&P1),
        Some(&Revision(20)),
        "the newer of the removal (18) and the removed entry's install (20)"
    );

    let effects = kernel
        .step(&ctx(5), &read(5, P1, 19, &record(P1, NODE, 2)))
        .expect("the older read is answered");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(
            AuthorityIgnoreReason::PartitionReadSuperseded
        )]
    );
    assert!(!kernel.view().served.contains_key(&P1));
}

/// The second removal site: a durable epoch revocation. The event carries no control revision,
/// so the tombstone is the revision the removed entry was installed at — which is what a read of
/// ours must now beat.
#[retcd_test]
fn an_epoch_revocation_keeps_the_revision_the_entry_was_installed_at() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read(3, P1, 20, &record(P1, NODE, 3)))
        .expect("adopt at 20");
    kernel
        .step(&ctx(4), &revocation_persisted(4, P1, 3))
        .expect("the epoch-revocation fence");
    let view = kernel.view();
    assert!(!view.served.contains_key(&P1), "fixture: p1 removed");
    assert_eq!(view.removed_revisions.get(&P1), Some(&Revision(20)));

    // Epoch 2, not the revoked 3, so this row is about the revision guard and nothing else.
    let effects = kernel
        .step(&ctx(5), &read(5, P1, 15, &record(P1, NODE, 2)))
        .expect("the older read is answered");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(
            AuthorityIgnoreReason::PartitionReadSuperseded
        )],
        "15 is older than the entry the revocation removed (20)"
    );
    assert!(!kernel.view().served.contains_key(&P1));
}

/// Two removals of one key: the tombstone keeps the newer. A read fence at 25 leaves 25; a later
/// epoch revocation of the already-removed key has no entry to take a revision from, and must
/// not replace 25 with the snapshot's 10.
#[retcd_test]
fn a_second_removal_does_not_lower_the_tombstone() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read(3, P1, 25, &record(P1, OTHER, 1)))
        .expect("the owner-moved fence");
    kernel
        .step(&ctx(4), &revocation_persisted(4, P1, 1))
        .expect("the epoch-revocation fence");
    assert_eq!(
        kernel.view().removed_revisions.get(&P1),
        Some(&Revision(25)),
        "still 25"
    );

    let effects = kernel
        .step(&ctx(5), &read(5, P1, 20, &record(P1, NODE, 2)))
        .expect("the older read is answered");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(
            AuthorityIgnoreReason::PartitionReadSuperseded
        )]
    );
}

/// The tombstone is a floor, not a ban. A read of ours newer than the removal is the changed row
/// as usual, and the key moves from the tombstone map to the installed one — the two never share
/// a key.
#[retcd_test]
fn a_read_newer_than_the_removal_readopts_and_clears_the_tombstone() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read(3, P1, 25, &record(P1, OTHER, 1)))
        .expect("the owner-moved fence");

    let effects = kernel
        .step(&ctx(4), &read(4, P1, 30, &record(P1, NODE, 2)))
        .expect("the newer read is the changed row");

    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Adopt(P1, OwnerEpoch(2)),
            Shape::Publish(P1),
            Shape::Fact(AuthorityFact::LineageChanged),
        ]
    );
    let view = kernel.view();
    assert_eq!(view.served.get(&P1), Some(&lineage(2)));
    assert_eq!(view.served_revisions.get(&P1), Some(&Revision(30)));
    assert!(
        view.removed_revisions.is_empty(),
        "an installed key has no tombstone"
    );
}

/// A snapshot newer than the removal has seen it, so it is the whole truth about the key again:
/// it installs p1 and the tombstone goes.
#[retcd_test]
fn a_snapshot_newer_than_the_removal_installs_and_clears_the_tombstone() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read(3, P1, 25, &record(P1, OTHER, 1)))
        .expect("the owner-moved fence");

    kernel
        .step(&ctx(4), &snapshot(4, 30, &[record(P1, NODE, 2)]))
        .expect("the snapshot install is built");

    let view = kernel.view();
    assert_eq!(view.served.get(&P1), Some(&lineage(2)));
    assert_eq!(view.served_revisions.get(&P1), Some(&Revision(30)));
    assert!(
        view.removed_revisions.is_empty(),
        "a tombstone the snapshot is newer than says nothing the snapshot does not"
    );
}

/// Lead ruling A-R41: a read that removes a partition this node never served is `NotOurs` — not
/// `StaleAuthorityView`, which is about a held view being superseded — and it fences nothing,
/// bumps nothing and writes no tombstone. Handed back by the phase-2 manual tester: mutant M05
/// put `StaleAuthorityView` back and every existing row stayed green.
#[retcd_test]
fn a_removal_read_of_a_partition_never_served_is_not_ours() {
    let mut kernel = serving_p1(1);
    let before = kernel.view();

    let effects = kernel
        .step(&ctx(3), &read_absent(3, P2, 11))
        .expect("answered, not unavailable");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::NotOurs)]
    );
    let after = kernel.view();
    assert_eq!(after.served, before.served, "p1 untouched");
    assert_eq!(
        after.authority_seq, before.authority_seq,
        "no fence, no bump"
    );
    assert!(
        !after.removed_revisions.contains_key(&P2),
        "nothing was removed, so nothing is tombstoned"
    );
}

/// The read side of the A-R48b equality boundary: a read of ours taken **at** the tombstone's
/// revision adopts nothing. The removal was learned at 25, so a record read at 25 cannot also
/// say the partition is ours; only a strictly newer read re-adopts. Handed back by the phase-2
/// manual tester: mutant M25 (adopt guard `<=` weakened to `<`) passed every existing row.
#[retcd_test]
fn a_read_exactly_at_the_tombstone_adopts_nothing() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read_absent(3, P1, 25))
        .expect("the absent-record fence");
    let seq = kernel.authority_seq();

    let effects = kernel
        .step(&ctx(4), &read(4, P1, 25, &record(P1, NODE, 1)))
        .expect("answered, not unavailable");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(
            AuthorityIgnoreReason::PartitionReadSuperseded
        )],
        "25 is not newer than the removal at 25"
    );
    assert!(!kernel.view().served.contains_key(&P1), "p1 stays removed");
    assert_eq!(kernel.authority_seq(), seq, "no write, no bump");
}

/// The snapshot side of the same boundary: a snapshot **at** the tombstone's revision has seen
/// the removal, so its word is final and the tombstone goes ("keeps a tombstone only when it is
/// strictly newer than S", A-R48b). Carried as an unpinned ADVISORY since phase 1. Handed back by
/// the phase-2 manual tester: mutant M29 (retain `>` weakened to `>=`) passed every existing row.
#[retcd_test]
fn a_snapshot_exactly_at_the_tombstone_clears_it() {
    let mut kernel = serving_p1(1);
    kernel
        .step(&ctx(3), &read_absent(3, P1, 25))
        .expect("the absent-record fence");

    kernel
        .step(&ctx(4), &snapshot(4, 25, &[]))
        .expect("the snapshot install is built");

    let view = kernel.view();
    assert!(
        !view.served.contains_key(&P1),
        "the snapshot does not list p1"
    );
    assert_eq!(
        view.removed_revisions.get(&P1),
        None,
        "a tombstone equal to S says nothing S does not"
    );
    assert_eq!(view.partitions_revision, Some(Revision(25)));
}

// ---------------------------------------------------------------------------------------------
// Finding F2 (lead ruling on the phase-2 gate; design §1.7, K-A-49): **no published view admits
// where `may_admit` denies, and no view is published for a partition this node does not serve.**
// A view is a copy of the check that a secondary honours through `valid_through_tick` without
// asking again, so a view wider than the check is admission the check refused. Before the fix
// every publish site built its view from the admission horizon alone — which judges the grant,
// not the partition — and fell back to the step context's lineage for an unserved partition. One
// row per publish path, each red on the tree before the fix.
// ---------------------------------------------------------------------------------------------

/// `p1` at `epoch`, as a caller of `may_admit` names it.
fn p1_at(epoch: u64) -> Lineage {
    Lineage {
        partition: P1,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(epoch),
    }
}

/// [`ctx`] at `now` with a sample taken at `now` on a clock running with the tick, so the sample
/// has moved and the clock row publishes a view for the context's partition (lead ruling A-R45).
fn moved_ctx(now: u64) -> StepCtx<'static> {
    let pinned = ctx(now);
    StepCtx {
        control_time: ControlTime {
            estimate: Tick(1_000_000 + now),
            sampled_at: Tick(now),
            ..pinned.control_time
        },
        ..pinned
    }
}

/// Every view in `effects` that still admits at `now` names a lineage `may_admit` admits at
/// `now`. Vacuous on an effect vector with no view, which is why each row also pins the denial.
fn assert_no_view_outruns_may_admit(kernel: &Authority, effects: &[Effect], now: u64) {
    for effect in effects {
        let EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::PublishAuthorityView(
            view,
        ))) = &effect.kind
        else {
            continue;
        };
        if view.valid_through_tick >= Tick(now) {
            assert_eq!(
                kernel.may_admit_at(view.lineage, Tick(now), &BUDGETS),
                Verdict::Admit,
                "a view admitting {:?} through {:?} where may_admit denies: {effects:?}",
                view.lineage,
                view.valid_through_tick
            );
        }
    }
}

/// Path 1, the clock row: a moved sample publishes a view for the context's partition. Here that
/// partition's storage is fenced (tester probe `finding_x1`).
#[retcd_test]
fn a_moved_sample_publishes_no_admitting_view_for_a_storage_fenced_partition() {
    let mut kernel = serving_p1(1);
    let failed = event_of(
        3,
        EventKind::Storage(StorageEvent::CommitFailed {
            batch: BatchId(1),
            fault: StorageFault::WriteFailed,
        }),
    );
    let effects = kernel.step(&ctx(3), &failed).expect("the storage fence");
    assert!(
        shapes(&effects).contains(&Shape::Fence(DenyReason::LocalStorageFenced)),
        "fixture: {effects:?}"
    );

    let moved = moved_ctx(1_500);
    let effects = kernel
        .step(
            &moved,
            &event(
                4,
                ControlEvent::WatchProgress {
                    prefix: ControlPrefix::Grants,
                    revision: Revision(7),
                },
            ),
        )
        .expect("a moved sample is absorbed");

    assert_eq!(
        kernel.clock().sample(),
        Some(moved.control_time),
        "fixture: the sample moved and was accepted"
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(1), Tick(1_500), &BUDGETS),
        Verdict::Deny(DenyReason::LocalStorageFenced),
        "fixture: the check denies"
    );
    assert_no_view_outruns_may_admit(&kernel, &effects, 1_500);
}

/// Path 2, the renewal commit: it publishes a view for the event's partition. Here the owner
/// moved and the fence removed `p1` (tester probe `finding_x2`), so it is not served at all.
#[retcd_test]
fn a_renewal_commit_publishes_no_view_for_a_partition_that_moved() {
    let mut kernel = serving_p1(1);
    let effects = kernel
        .step(&ctx(3), &read(3, P1, 25, &record(P1, OTHER, 2)))
        .expect("the owner-moved fence");
    assert!(
        shapes(&effects).contains(&Shape::Fence(DenyReason::GenerationChanged)),
        "fixture: {effects:?}"
    );
    // The sample stays pinned, so the clock row publishes nothing and only the commit can.
    let due = event_of(
        5,
        EventKind::Timer(TimerFired {
            id: AuthorityTimer::Renew.id(),
            version: kernel.timer_version(AuthorityTimer::Renew),
            scheduled_at: Tick(500),
        }),
    );
    kernel.step(&ctx(500), &due).expect("the RenewDue row");
    assert!(
        kernel.view().renewal.is_some(),
        "fixture: renewal in flight"
    );

    let effects = kernel
        .step(
            &ctx(1_400),
            &event(
                5,
                ControlEvent::CasResult {
                    request: in_flight(&kernel),
                    key: ControlKey::Grant(NODE),
                    outcome: CasOutcome::Committed(Revision(8)),
                },
            ),
        )
        .expect("the renewal commit");

    assert!(
        kernel.view().renewal.is_none(),
        "fixture: the commit matched"
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(1), Tick(1_400), &BUDGETS),
        Verdict::Deny(DenyReason::GenerationChanged),
        "fixture: the check denies"
    );
    assert!(
        !shapes(&effects).contains(&Shape::Publish(P1)),
        "no view for a partition this node no longer serves: {effects:?}"
    );
    assert_no_view_outruns_may_admit(&kernel, &effects, 1_400);
}

/// Path 3, acquisition: it published a view for the event's partition before any partitions
/// snapshot had installed a lineage (tester probe `finding_x3`).
#[retcd_test]
fn an_acquisition_publishes_no_view_for_a_partition_not_yet_served() {
    let (kernel, effects) = acquired();

    assert!(
        kernel.view().served.is_empty(),
        "fixture: nothing is installed before the partitions snapshot"
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(1), Tick(0), &BUDGETS),
        Verdict::Deny(DenyReason::GenerationChanged),
        "fixture: the check denies"
    );
    assert!(
        !shapes(&effects).contains(&Shape::Publish(P1)),
        "no view for a partition this node does not serve yet: {effects:?}"
    );
    assert_no_view_outruns_may_admit(&kernel, &effects, 0);
}

/// Path 4, the snapshot reload: a reload at the tombstone's own revision reinstalls a revoked
/// `(p1, e3)` — the revocation is local, and the control plane still names us at epoch 3 — and
/// published an admitting view for it (tester probe `finding_5d`).
#[retcd_test]
fn a_snapshot_reload_publishes_no_admitting_view_for_a_revoked_epoch() {
    let mut kernel = serving_p1(3);
    let effects = kernel
        .step(&ctx(3), &revocation_persisted(3, P1, 3))
        .expect("the epoch-revocation fence");
    assert!(
        shapes(&effects).contains(&Shape::Fence(DenyReason::EpochRevoked)),
        "fixture: {effects:?}"
    );

    let effects = kernel
        .step(&ctx(4), &snapshot(4, 10, &[record(P1, NODE, 3)]))
        .expect("the reload");

    assert_eq!(
        kernel.view().served.get(&P1),
        Some(&lineage(3)),
        "fixture: the reload reinstalled (p1, e3)"
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(3), Tick(4), &BUDGETS),
        Verdict::Deny(DenyReason::EpochRevoked),
        "fixture: the check denies"
    );
    assert_no_view_outruns_may_admit(&kernel, &effects, 4);
}

/// Path 5, the single-record adopt. Not one of the four the lead listed, but it is a fifth
/// caller of the same view builder: a read of ours newer than the tombstone re-adopts the revoked
/// `(p1, e3)`.
#[retcd_test]
fn a_read_adopting_a_revoked_epoch_publishes_no_admitting_view() {
    let mut kernel = serving_p1(3);
    kernel
        .step(&ctx(3), &revocation_persisted(3, P1, 3))
        .expect("the epoch-revocation fence");

    let effects = kernel
        .step(&ctx(4), &read(4, P1, 11, &record(P1, NODE, 3)))
        .expect("the adopt");

    assert_eq!(
        kernel.view().served.get(&P1),
        Some(&lineage(3)),
        "fixture: the read re-adopted (p1, e3)"
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(3), Tick(4), &BUDGETS),
        Verdict::Deny(DenyReason::EpochRevoked),
        "fixture: the check denies"
    );
    assert_no_view_outruns_may_admit(&kernel, &effects, 4);
}

/// `Held`, serving `p1` and `p2` at epoch 1 through one snapshot at revision 10, tick 2, and the
/// install's effect vector (one admitting view per partition).
fn serving_p1_p2() -> (Authority, Vec<Effect>) {
    let mut kernel = held_kernel();
    let effects = kernel
        .step(
            &ctx(2),
            &snapshot(2, 10, &[record(P1, NODE, 1), record(P2, NODE, 1)]),
        )
        .expect("the family-snapshot install is built");
    assert_eq!(
        kernel.view().served.keys().copied().collect::<Vec<_>>(),
        vec![P1, P2],
        "fixture"
    );
    (kernel, effects)
}

/// The view a consumer holds per partition after `effects`, in delivery order: the highest
/// `authority_seq`, and a later view wins a tie.
fn newest_views(effects: &[Effect]) -> std::collections::BTreeMap<PartitionId, AuthorityView> {
    let mut newest = std::collections::BTreeMap::new();
    for effect in effects {
        if let EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::PublishAuthorityView(
            view,
        ))) = &effect.kind
        {
            let replaces = newest
                .get(&view.lineage.partition)
                .is_none_or(|held: &AuthorityView| view.authority_seq >= held.authority_seq);
            if replaces {
                newest.insert(view.lineage.partition, *view);
            }
        }
    }
    newest
}

/// Gate 3 finding A (lead ruling A-R53.1), `design.md` §1.7: a node-scoped fence supersedes the
/// view of **every** served partition, not only the partition the event arrived on. Otherwise a
/// secondary keeps honouring the other partition's install view to its horizon after the node
/// stopped admitting anything. Table-driven over the five node-fence triggers, each fired at the
/// install's own tick so the install views are still standing.
#[retcd_test]
fn a_node_fence_publishes_a_past_view_for_every_served_partition() {
    let own_grant = |frozen: bool| ReadOutcome::Found {
        revision: Revision(8),
        value: GrantRecord {
            grant: GrantId(1),
            node: NODE,
            boot: BOOT,
            authority_generation: AuthorityGeneration::default(),
            expiry_utc_ms: 1_003_000,
            frozen,
        }
        .encode(),
    };
    let rejected = StepCtx {
        control_time: ControlTime {
            bound_established: false,
            ..ctx(2).control_time
        },
        ..ctx(2)
    };
    let triggers: [(DenyReason, StepCtx<'static>, EventKind); 5] = [
        (
            DenyReason::BootMismatch,
            ctx(2),
            EventKind::Node(NodeLifecycle::Rebooted { boot: BootId(2) }),
        ),
        (
            DenyReason::ProcessSuspended,
            ctx(2),
            EventKind::Node(NodeLifecycle::Resumed {
                suspended_millis: 501,
            }),
        ),
        (
            DenyReason::Revoked,
            ctx(2),
            EventKind::Control(ControlEvent::Value {
                request: UNASKED,
                key: ControlKey::Grant(NODE),
                outcome: ReadOutcome::Absent { as_of: Revision(8) },
            }),
        ),
        (
            DenyReason::Frozen,
            ctx(2),
            EventKind::Control(ControlEvent::Value {
                request: UNASKED,
                key: ControlKey::Grant(NODE),
                outcome: own_grant(true),
            }),
        ),
        (
            DenyReason::ClockUnbounded,
            rejected,
            EventKind::Control(ControlEvent::WatchProgress {
                prefix: ControlPrefix::Grants,
                revision: Revision(8),
            }),
        ),
    ];
    for (reason, step_ctx, kind) in triggers {
        let (mut kernel, mut effects) = serving_p1_p2();
        let fenced = kernel
            .step(&step_ctx, &event_of(9, kind))
            .expect("the node-fence row is built");
        assert!(
            shapes(&fenced).contains(&Shape::Fence(reason)),
            "{reason:?}: fixture fences the node: {fenced:?}"
        );
        effects.extend(fenced);

        let newest = newest_views(&effects);
        for partition in [P1, P2] {
            let view = newest[&partition];
            assert!(
                view.valid_through_tick < Tick(2),
                "{reason:?}: {partition:?}'s newest view still admits: {view:?}"
            );
            assert_eq!(view.past_horizon, reason, "{reason:?}: {partition:?}");
        }
    }
}

/// Tester gate 3, mutant G11 (the local horizon without its `- 1`, missed by every row above,
/// because [`assert_no_view_outruns_may_admit`] judges a view only at the tick it was published).
/// A view is a promise **through** `valid_through_tick`, so `may_admit` must admit at that tick
/// too. Reached when an adopted record's `E` is ahead of the sample (so `renewed_at` clamps to
/// now) and a later fresh sample leaves the clock conjunct holding at the local window's end:
/// only then is the horizon the local one rather than `now`.
#[retcd_test]
fn a_view_still_admits_at_its_own_horizon_when_the_local_window_binds() {
    let mut kernel = serving_p1(1);
    let ahead = rdb_core::authority::grant::GrantRecord {
        grant: rdb_core::contracts::ids::GrantId(1),
        node: NODE,
        boot: BOOT,
        authority_generation: rdb_core::contracts::ids::AuthorityGeneration::default(),
        expiry_utc_ms: (1_000_000 + 100 + BUDGETS.grant_millis + 500) as i64,
        frozen: false,
    };
    kernel
        .step(
            &ctx(100),
            &event(
                3,
                ControlEvent::Value {
                    request: UNASKED,
                    key: ControlKey::Grant(NODE),
                    outcome: ReadOutcome::Found {
                        revision: Revision(8),
                        value: ahead.encode(),
                    },
                },
            ),
        )
        .expect("our newer grant record is adopted");
    assert_eq!(
        kernel.view().renewed_at,
        Some(Tick(100)),
        "fixture: clamped to now"
    );

    let moved = moved_ctx(1_000);
    let effects = kernel
        .step(
            &moved,
            &event(
                4,
                ControlEvent::WatchProgress {
                    prefix: ControlPrefix::Grants,
                    revision: Revision(8),
                },
            ),
        )
        .expect("a moved sample is absorbed");
    let views: Vec<_> = effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::PublishAuthorityView(
                view,
            ))) => Some(*view),
            _ => None,
        })
        .collect();
    assert_eq!(views.len(), 1, "{effects:?}");
    let horizon = Tick(100 + BUDGETS.grant_millis - BUDGETS.dispatch_margin_millis - 1);
    assert_eq!(
        views[0].valid_through_tick, horizon,
        "the local window's last tick"
    );
    assert_eq!(
        kernel.may_admit_at(views[0].lineage, views[0].valid_through_tick, &BUDGETS),
        Verdict::Admit,
        "the view's own horizon is a tick may_admit admits"
    );
}

/// §B7 Q2, closed by lead ruling A-R53.5: `AdoptAuthority` is withheld with the view when the
/// partition half of `may_admit` refuses the installed lineage. Both install paths reinstall the
/// revoked `(p1, e3)`: a snapshot newer than the tombstone, and a read newer than it. Neither may
/// tell the dispatcher to serve an epoch the check denies.
#[retcd_test]
fn no_adopt_authority_for_a_reinstalled_revoked_epoch() {
    let revoked = || {
        let mut kernel = serving_p1(3);
        kernel
            .step(&ctx(3), &revocation_persisted(3, P1, 3))
            .expect("the epoch-revocation fence");
        kernel
    };
    let paths = [
        ("snapshot", snapshot(4, 11, &[record(P1, NODE, 3)])),
        ("read", read(4, P1, 11, &record(P1, NODE, 3))),
    ];
    for (path, reinstall) in paths {
        let mut kernel = revoked();
        let effects = kernel.step(&ctx(4), &reinstall).expect("the reinstall");

        assert_eq!(
            kernel.view().served.get(&P1),
            Some(&lineage(3)),
            "{path}: fixture: (p1, e3) is reinstalled"
        );
        assert_eq!(
            kernel.may_admit_at(p1_at(3), Tick(4), &BUDGETS),
            Verdict::Deny(DenyReason::EpochRevoked),
            "{path}: fixture: the check denies"
        );
        let shapes = shapes(&effects);
        assert!(
            !shapes.contains(&Shape::Adopt(P1, OwnerEpoch(3))),
            "{path}: {shapes:?}"
        );
        assert!(!shapes.contains(&Shape::Publish(P1)), "{path}: {shapes:?}");
    }
}

/// Gate 3 finding E (lead ruling A-R53.4): a steady-state view promises admission past the tick
/// it is published at, up to the **latest** tick at which every conjunct holds, sample age
/// included. A fresh sample at 100 on a grant renewed at 0 binds on age: it goes stale after
/// `100 + max_sample_age`, well inside the local window (2 899), so that is the horizon. Before
/// A-R53 the view fell back to its own publishing tick whenever the clock conjunct failed before
/// the local horizon, which in steady state is always.
#[retcd_test]
fn a_steady_state_view_promises_through_the_last_tick_every_conjunct_holds() {
    let mut kernel = serving_p1(1);

    let effects = kernel
        .step(
            &moved_ctx(100),
            &event(
                3,
                ControlEvent::WatchProgress {
                    prefix: ControlPrefix::Grants,
                    revision: Revision(8),
                },
            ),
        )
        .expect("a moved sample publishes");

    let views: Vec<AuthorityView> = effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::PublishAuthorityView(
                view,
            ))) => Some(*view),
            _ => None,
        })
        .collect();
    assert_eq!(views.len(), 1, "{effects:?}");
    let view = views[0];
    let last = Tick(100 + BUDGETS.max_sample_age_millis);
    assert_eq!(
        view.valid_through_tick, last,
        "the sample's last fresh tick"
    );
    assert_eq!(view.past_horizon, DenyReason::ClockSampleStale);
    assert_eq!(
        kernel.may_admit_at(view.lineage, last, &BUDGETS),
        Verdict::Admit,
        "the view's horizon is a tick may_admit admits"
    );
    assert_eq!(
        kernel.may_admit_at(view.lineage, Tick(last.0 + 1), &BUDGETS),
        Verdict::Deny(DenyReason::ClockSampleStale),
        "and the tick after it is not: the horizon is the latest, not merely a safe one"
    );
}

/// Tester re-gate, mutant GA1 (the node fence's views keep the old `authority_seq`, missed by
/// every row above, because a consumer that lets a later view win a tie still ends on the past
/// view). `design.md` §2.4: every `Fence` bumps `authority_seq` (K-A-34), and it bumps it once,
/// so every view the fence publishes carries the new value and outranks the install views
/// without relying on delivery order.
#[retcd_test]
fn a_node_fence_moves_authority_seq_once_and_every_view_it_publishes_carries_it() {
    let (mut kernel, installed) = serving_p1_p2();
    let before = kernel.authority_seq();
    let fenced = kernel
        .step(
            &ctx(2),
            &event_of(
                9,
                EventKind::Node(NodeLifecycle::Rebooted { boot: BootId(2) }),
            ),
        )
        .expect("the node-fence row is built");
    assert_eq!(kernel.authority_seq(), before + 1, "one bump per fence");
    let seqs: Vec<u64> = newest_views(&fenced)
        .values()
        .map(|view| view.authority_seq)
        .collect();
    assert_eq!(seqs, vec![before + 1, before + 1], "{fenced:?}");
    for view in newest_views(&installed).values() {
        assert!(
            view.authority_seq < before + 1,
            "the fence's views must outrank the install's by seq: {view:?}"
        );
    }
}

/// Tester re-gate, mutant GE3 (`admission_horizon` without its "fails at `now`" answer, missed
/// by every row above). When the clock conjunct already fails at the publishing tick, the view
/// must be past (`now - 1`, K-A-53). Without that early answer the bisection starts from a
/// `good` tick that does not hold, and returns `now`: a view admitting at `now` where `may_admit`
/// denies. Reached by an install on a sample that is stale at the step: stale falls through
/// `revalidate` without a fence, and the install publishes.
#[retcd_test]
fn a_view_published_on_a_clock_that_already_denies_is_past() {
    let mut kernel = held_kernel();
    // The fixed sample was taken at 0; at 2100 it is 100 ms past `max_sample_age_millis`, and
    // the local window (renewed at 0) still holds to 2899.
    let effects = kernel
        .step(&ctx(2_100), &snapshot(2, 10, &[record(P1, NODE, 1)]))
        .expect("the family-snapshot install is built");
    assert_eq!(
        kernel.may_admit_at(p1_at(1), Tick(2_100), &BUDGETS),
        Verdict::Deny(DenyReason::ClockSampleStale),
        "fixture: the clock conjunct fails at the publishing tick"
    );
    let view = newest_views(&effects)[&P1];
    assert_eq!(view.valid_through_tick, Tick(2_099), "{effects:?}");
    assert_eq!(view.past_horizon, DenyReason::ClockSampleStale);
    assert_no_view_outruns_may_admit(&kernel, &effects, 2_100);
}

// ---------------------------------------------------------------------------------------------
// Re-gate, lead rulings A-R54 (N1, N3, N2). With A-R53.4 a view promises into the future, so
// every path that shrinks the horizon must republish every view it shrinks.
// ---------------------------------------------------------------------------------------------

/// A partition this node never serves.
const P9: PartitionId = PartitionId(9);

/// [`ctx`] at `now`, holding a sample taken at `sampled_at` on a clock `ahead` ms ahead of the
/// tick, with error `error`.
fn sample_ctx(now: u64, sampled_at: u64, ahead: i64, error: u64) -> StepCtx<'static> {
    StepCtx {
        control_time: ControlTime {
            estimate: Tick((1_000_000 + sampled_at as i64 + ahead) as u64),
            error_millis: error,
            bound_established: true,
            sampled_at: Tick(sampled_at),
        },
        ..ctx(now)
    }
}

/// An event that routes to nothing while `Held`: its step is the clock row's alone.
fn progress(id: u64) -> Event {
    event(
        id,
        ControlEvent::WatchProgress {
            prefix: ControlPrefix::Grants,
            revision: Revision(1),
        },
    )
}

/// `Held` since 0 (`E` = 1 003 000), serving `p1` and `p2` through a snapshot taken at 1500 on
/// a fresh sample: both views are bound by `E - epsilon - delta` and promise through 2889. The
/// install's effects.
fn long_views() -> (Authority, Vec<Effect>) {
    long_views_of(&[P1, P2])
}

/// [`long_views`], serving `partitions` at epoch 1.
fn long_views_of(partitions: &[PartitionId]) -> (Authority, Vec<Effect>) {
    let mut kernel = held_kernel();
    let records: Vec<PartitionRecord> = partitions
        .iter()
        .map(|partition| record(*partition, NODE, 1))
        .collect();
    let effects = kernel
        .step(&sample_ctx(1_500, 1_500, 0, 10), &snapshot(2, 10, &records))
        .expect("the family-snapshot install is built");
    for (partition, view) in newest_views(&effects) {
        assert_eq!(
            view.valid_through_tick,
            Tick(2_889),
            "fixture: {partition:?}"
        );
    }
    (kernel, effects)
}

/// Every served partition's newest view in `effects` promises exactly what `may_admit` allows:
/// it admits at its own horizon, and not one tick later.
fn assert_every_view_is_exact(kernel: &Authority, effects: &[Effect], what: &str) {
    assert_views_exact_except(kernel, effects, what, &[]);
}

/// [`assert_every_view_is_exact`] for every served partition but `fenced`, whose views a
/// partition fence has made past on purpose: a past view admits nothing, so it has no horizon
/// for `may_admit` to agree with. The caller asserts those separately.
fn assert_views_exact_except(
    kernel: &Authority,
    effects: &[Effect],
    what: &str,
    fenced: &[PartitionId],
) {
    let newest = newest_views(effects);
    for partition in kernel.view().served.keys() {
        if fenced.contains(partition) {
            continue;
        }
        let view = newest[partition];
        let at = view.valid_through_tick;
        assert_eq!(
            kernel.may_admit_at(view.lineage, at, &BUDGETS),
            Verdict::Admit,
            "{what}: {partition:?}'s view outruns may_admit: {view:?}"
        );
        assert_ne!(
            kernel.may_admit_at(view.lineage, Tick(at.0 + 1), &BUDGETS),
            Verdict::Admit,
            "{what}: {partition:?}'s view stops short of the horizon: {view:?}"
        );
    }
}

/// N1 (lead ruling A-R54.1), `design.md` §1.7: a node-wide horizon that shrinks is republished
/// for **every** served partition, not only the step's. Each case narrows the horizon of both
/// `p1` and `p2`, including when the step's partition is one A1 does not serve and when the
/// narrowing comes from an adopted grant record rather than a sample. An older sample is the
/// fourth narrowing case the tester found; since A-R54.3 it is refused, so the views stand.
#[retcd_test]
fn a_shrinking_horizon_republishes_every_served_partition() {
    let lower_e = GrantRecord {
        grant: GrantId(1),
        node: NODE,
        boot: BOOT,
        authority_generation: AuthorityGeneration::default(),
        expiry_utc_ms: 1_002_500,
        frozen: false,
    };
    let unserved = StepCtx {
        partition: P9,
        ..sample_ctx(1_600, 1_600, 600, 10)
    };
    let cases: [(&str, StepCtx<'static>, Event); 5] = [
        (
            "clock 600 ms ahead",
            sample_ctx(1_600, 1_600, 600, 10),
            progress(3),
        ),
        (
            "error widens to 100",
            sample_ctx(1_600, 1_600, 0, 100),
            progress(3),
        ),
        (
            "older sample, taken at 800",
            sample_ctx(1_600, 800, 0, 10),
            progress(3),
        ),
        (
            "step partition not served",
            unserved,
            Event {
                partition: P9,
                ..progress(3)
            },
        ),
        (
            "own grant record with a lower E",
            sample_ctx(1_600, 1_500, 0, 10),
            event(
                3,
                ControlEvent::Value {
                    request: UNASKED,
                    key: ControlKey::Grant(NODE),
                    outcome: ReadOutcome::Found {
                        revision: Revision(8),
                        value: lower_e.encode(),
                    },
                },
            ),
        ),
    ];
    for (what, step_ctx, narrowing) in cases {
        let (mut kernel, mut effects) = long_views();
        let step = kernel.step(&step_ctx, &narrowing).expect("built");
        assert!(kernel.state().is_held(), "{what}: fixture: no fence");
        effects.extend(step);
        assert_every_view_is_exact(&kernel, &effects, what);
    }
}

/// N1's renewal site: a commit writes `E`, so it moves the node-wide horizon, and it republishes
/// every served partition. Reached by a renewal dispatched on a sample 10 ms behind the one the
/// grant was acquired on (inside the epsilon, so accepted), which writes an `E` 7 ms lower. The
/// commit step's own sample move first republishes both views on the old `E`; the commit must
/// then narrow both, not only the event's.
#[retcd_test]
fn a_renewal_that_lowers_e_republishes_every_served_partition() {
    let (mut kernel, mut effects) = serving_p1_p2();
    let due = event_of(
        20,
        EventKind::Timer(TimerFired {
            id: AuthorityTimer::Renew.id(),
            version: kernel.timer_version(AuthorityTimer::Renew),
            scheduled_at: Tick(3),
        }),
    );
    effects.extend(
        kernel
            .step(&sample_ctx(3, 3, -10, 10), &due)
            .expect("RenewDue"),
    );
    assert_eq!(
        kernel.view().renewal.map(|renewal| renewal.e_new),
        Some(1_002_993),
        "fixture: a renewal 7 ms below E = 1 003 000 is in flight"
    );
    let commit = event(
        20,
        ControlEvent::CasResult {
            request: in_flight(&kernel),
            key: ControlKey::Grant(NODE),
            outcome: CasOutcome::Committed(Revision(8)),
        },
    );
    effects.extend(
        kernel
            .step(&sample_ctx(1_000, 1_000, -10, 10), &commit)
            .expect("the renewal commit"),
    );
    assert_eq!(kernel.view().expiry_utc_ms, Some(1_002_993), "fixture");
    assert_every_view_is_exact(&kernel, &effects, "renewal commit");
}

/// N3 (lead ruling A-R54.2): a snapshot that drops a served partition — unlisted, or listed
/// under another owner — fences it and publishes its past view in the same step. The other
/// partition's view is untouched.
#[retcd_test]
fn a_snapshot_that_drops_a_served_partition_fences_it() {
    let cases = [
        ("unlisted", vec![record(P1, NODE, 1)]),
        (
            "listed under another owner",
            vec![record(P1, NODE, 1), record(P2, OTHER, 1)],
        ),
    ];
    for (what, records) in cases {
        let (mut kernel, mut effects) = long_views();
        let step = kernel
            .step(&sample_ctx(1_600, 1_500, 0, 10), &snapshot(3, 11, &records))
            .expect("the reload");
        assert!(!kernel.view().served.contains_key(&P2), "{what}: fixture");
        assert!(
            step.iter().any(|effect| matches!(
                effect.kind,
                EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fence {
                    scope: FenceScope::Partition(P2),
                    reason: DenyReason::GenerationChanged,
                }))
            )),
            "{what}: {step:?}"
        );
        effects.extend(step);
        let newest = newest_views(&effects);
        assert!(
            newest[&P2].valid_through_tick < Tick(1_600),
            "{what}: the dropped partition's view still admits: {:?}",
            newest[&P2]
        );
        assert_eq!(newest[&P2].past_horizon, DenyReason::GenerationChanged);
        assert_every_view_is_exact(&kernel, &effects, what);
    }
}

/// N2 (lead ruling A-R54.3): a stale sample must not hide a backward jump. A sample older than
/// the held one is refused (`SampleRejected`) and the held sample stays, so a jump delivered
/// after it is still judged against the good sample. And a stale sample newer than the held one
/// is judged for a jump whatever its age.
#[retcd_test]
fn a_stale_sample_does_not_hide_a_backward_jump() {
    let (mut kernel, _) = long_views();
    let held = kernel.clock().sample();
    let older = kernel
        .step(&sample_ctx(2_100, 50, -1_000, 10), &progress(3))
        .expect("built");
    assert_eq!(
        shapes(&older),
        vec![Shape::Ignored(AuthorityIgnoreReason::SampleRejected)],
        "older than the held sample"
    );
    assert_eq!(kernel.clock().sample(), held, "the held sample stays");
    let jumped = kernel
        .step(&sample_ctx(2_200, 2_200, -1_000, 10), &progress(4))
        .expect("built");
    assert!(
        shapes(&jumped).contains(&Shape::Fence(DenyReason::ClockUnbounded)),
        "the jump behind the refused sample: {jumped:?}"
    );

    // Held sample at 0; at 2100 a sample taken at 50 is newer but stale, and 1000 ms behind.
    let mut kernel = serving_p1(1);
    let stale = kernel
        .step(&sample_ctx(2_100, 50, -1_000, 10), &progress(3))
        .expect("built");
    assert!(
        shapes(&stale).contains(&Shape::Fence(DenyReason::ClockUnbounded)),
        "a stale sample is still judged for a jump: {stale:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// §E, lead rulings A-R56: N4, J1, and the four probe-only kills (DN1o, DN3l, DN2h, DN2d).
// Every row judges the **last** view per partition (A-R56.3): one step may publish a fresh view
// and then supersede it at a higher seq.
// ---------------------------------------------------------------------------------------------

/// Every partition's newest view in `effects` admits at each of `ticks` it still covers only
/// where [`Authority::may_admit_at`] admits: no standing view outruns the check.
fn assert_standing(kernel: &Authority, effects: &[Effect], ticks: &[u64]) {
    for (partition, view) in newest_views(effects) {
        for &tick in ticks {
            if view.valid_through_tick >= Tick(tick) {
                assert_eq!(
                    kernel.may_admit_at(view.lineage, Tick(tick), &BUDGETS),
                    Verdict::Admit,
                    "{partition:?}'s standing view admits at {tick} where may_admit denies: {view:?}"
                );
            }
        }
    }
}

/// The `(scope, reason)` of every `Fence` in `effects`, in order.
fn fences(effects: &[Effect]) -> Vec<(FenceScope, DenyReason)> {
    effects
        .iter()
        .filter_map(|effect| match effect.kind {
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fence {
                scope,
                reason,
            })) => Some((scope, reason)),
            _ => None,
        })
        .collect()
}

/// Every view in `effects`, in order.
fn views(effects: &[Effect]) -> Vec<AuthorityView> {
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

/// N4 (lead ruling A-R56.1): a served partition whose lineage moves to one `may_admit` refuses
/// gets no new view, so the old lineage's view must be superseded by a fence under the **old**
/// lineage, the shape N3 gives a drop. Through a snapshot and through a single-record read. The
/// new lineage here is an epoch this node already revoked. One row per path, so each is seen red
/// on its own.
fn a_lineage_change_to_a_withheld_lineage_fences_the_old_one(via: &str) {
    let p2_at = |epoch: u64| Lineage {
        partition: P2,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(epoch),
    };
    let (mut kernel, mut effects) = long_views();
    let revoked = kernel
        .step(
            &sample_ctx(1_550, 1_500, 0, 10),
            &revocation_persisted(3, P2, 2),
        )
        .expect("the revocation fence");
    assert!(shapes(&revoked).contains(&Shape::Fence(DenyReason::EpochRevoked)));
    effects.extend(revoked);
    let reinstalled = kernel
        .step(
            &sample_ctx(1_600, 1_500, 0, 10),
            &snapshot(4, 11, &[record(P1, NODE, 1), record(P2, NODE, 1)]),
        )
        .expect("the re-install");
    assert!(
        shapes(&reinstalled).contains(&Shape::Adopt(P2, OwnerEpoch(1))),
        "{via}: fixture: {reinstalled:?}"
    );
    effects.extend(reinstalled);

    let moved = if via == "snapshot" {
        snapshot(5, 12, &[record(P1, NODE, 1), record(P2, NODE, 2)])
    } else {
        read(5, P2, 12, &record(P2, NODE, 2))
    };
    let step = kernel
        .step(&sample_ctx(1_700, 1_500, 0, 10), &moved)
        .expect("the lineage change");
    assert_eq!(
        fences(&step),
        vec![(FenceScope::Partition(P2), DenyReason::GenerationChanged)],
        "{via}: {step:?}"
    );
    assert!(
        !shapes(&step).contains(&Shape::Adopt(P2, OwnerEpoch(2))),
        "{via}: the revoked epoch is not adopted: {step:?}"
    );
    effects.extend(step);
    let last = newest_views(&effects)[&P2];
    assert_eq!(last.lineage, p2_at(1), "{via}: under the old lineage");
    assert_eq!(last.valid_through_tick, Tick(1_699), "{via}");
    assert_eq!(last.past_horizon, DenyReason::GenerationChanged, "{via}");
    assert_eq!(
        kernel.may_admit_at(p2_at(1), Tick(1_700), &BUDGETS),
        Verdict::Deny(DenyReason::GenerationChanged),
        "{via}: fixture"
    );
    assert_standing(&kernel, &effects, &[1_700, 2_000, 2_889]);
}

#[retcd_test]
fn a_snapshot_moving_a_partition_to_a_withheld_lineage_fences_the_old_one() {
    a_lineage_change_to_a_withheld_lineage_fences_the_old_one("snapshot");
}

#[retcd_test]
fn a_read_moving_a_partition_to_a_withheld_lineage_fences_the_old_one() {
    a_lineage_change_to_a_withheld_lineage_fences_the_old_one("read");
}

/// The one-fact twin of N4: the lineage does **not** change. A snapshot re-listing a
/// storage-fenced partition under the lineage it already serves replaces nothing, so it fences
/// nothing, even though its adopt is still withheld. The storage fence already superseded the
/// view.
#[retcd_test]
fn a_snapshot_relisting_a_withheld_lineage_unchanged_fences_nothing() {
    let (mut kernel, _) = long_views();
    let failed = event_of(
        3,
        EventKind::Storage(StorageEvent::CommitFailed {
            batch: BatchId(1),
            fault: StorageFault::WriteFailed,
        }),
    );
    let fenced = kernel
        .step(&sample_ctx(1_550, 1_500, 0, 10), &failed)
        .expect("the storage fence");
    assert_eq!(
        fences(&fenced),
        vec![(FenceScope::Partition(P1), DenyReason::LocalStorageFenced)],
        "fixture: {fenced:?}"
    );

    let step = kernel
        .step(
            &sample_ctx(1_600, 1_500, 0, 10),
            &snapshot(4, 11, &[record(P1, NODE, 1), record(P2, NODE, 1)]),
        )
        .expect("the re-listing");
    assert_eq!(fences(&step), vec![], "unchanged: {step:?}");
    assert!(
        !shapes(&step).contains(&Shape::Adopt(P1, OwnerEpoch(1))),
        "fixture: still withheld: {step:?}"
    );
    assert!(
        shapes(&step).contains(&Shape::Adopt(P2, OwnerEpoch(1))),
        "fixture: {step:?}"
    );
}

/// J1 (lead ruling A-R56.2): the backward-jump tolerance is **both** samples' epsilon. A loose
/// sample (error 100, reading 90 ms ahead) then a precise one on the truth (error 5) are two
/// honest readings whose intervals overlap; judged on the new sample's epsilon alone, they were
/// a 90 ms jump and fenced the node.
#[retcd_test]
fn a_precise_sample_after_a_loose_one_is_not_a_backward_jump() {
    let (mut kernel, _) = long_views();
    let loose = kernel
        .step(&sample_ctx(1_600, 1_600, 90, 100), &progress(3))
        .expect("built");
    assert!(fences(&loose).is_empty(), "fixture: {loose:?}");
    let precise = kernel
        .step(&sample_ctx(1_700, 1_700, 0, 5), &progress(4))
        .expect("built");
    assert!(fences(&precise).is_empty(), "{precise:?}");
    assert!(kernel.state().is_held());
    assert_eq!(
        kernel.may_admit_at(p1_at(1), Tick(1_700), &BUDGETS),
        Verdict::Admit,
        "the precise sample admits"
    );
}

/// J1's positive twin: a jump beyond the **summed** epsilon still fences. After the loose
/// sample the tolerance is 100 + 5 (no drift yet at 100 ms): 105 ms behind is inside it, 106 ms
/// is a backward jump.
#[retcd_test]
fn a_backward_jump_beyond_both_samples_epsilon_still_fences() {
    for (behind, fenced) in [(15, false), (16, true)] {
        let (mut kernel, _) = long_views();
        kernel
            .step(&sample_ctx(1_600, 1_600, 90, 100), &progress(3))
            .expect("the loose sample");
        let step = kernel
            .step(&sample_ctx(1_700, 1_700, -behind, 5), &progress(4))
            .expect("built");
        let want = if fenced {
            vec![(FenceScope::Node, DenyReason::ClockUnbounded)]
        } else {
            Vec::new()
        };
        assert_eq!(fences(&step), want, "{}ms behind: {step:?}", 90 + behind);
    }
}

/// Ported from the gate-3 tester's `re_n4_a_r57_a_same_epoch_change_on_a_denied_partition_still_
/// fences` by lead ruling A-R58 Q2; not an `M7A-*` row. Lead ruling A-R57, literal: the N4 fence
/// fires on **any** change of the served lineage onto a withheld one, also when only the
/// generation or only the config version moves and the old lineage was already denied.
///
/// `p1` and `p2` served; `p1`'s storage fails, so `p1` is denied. Then `p1`'s record changes at
/// the same owner epoch — its generation, or its config version alone — by a snapshot and by a
/// read. Each: one partition fence `GenerationChanged` for `p1`, only past views of `p1`, and no
/// `p1` adopt. Pins `must_fence`'s whole-lineage comparison: the tester's mutant EN4o compares the
/// owner epoch only, and this is the one row it fails.
#[retcd_test]
fn a_same_epoch_change_on_a_denied_partition_still_fences() {
    let changes = [
        (
            "generation",
            PartitionRecord {
                generation: Generation(2),
                ..record(P1, NODE, 1)
            },
        ),
        (
            "config only",
            PartitionRecord {
                config_version: ConfigVersion(2),
                ..record(P1, NODE, 1)
            },
        ),
    ];
    for via in ["snapshot", "read"] {
        for (what, changed) in &changes {
            let (mut kernel, _) = serving_p1_p2();
            let failed = kernel
                .step(
                    &ctx(3),
                    &event_of(
                        3,
                        EventKind::Storage(StorageEvent::CommitFailed {
                            batch: BatchId(1),
                            fault: StorageFault::WriteFailed,
                        }),
                    ),
                )
                .expect("built");
            assert_eq!(
                fences(&failed),
                vec![(FenceScope::Partition(P1), DenyReason::LocalStorageFenced)],
                "fixture"
            );

            let moved = if via == "snapshot" {
                snapshot(5, 11, &[*changed, record(P2, NODE, 1)])
            } else {
                read(5, P1, 11, changed)
            };
            let effects = kernel.step(&ctx(5), &moved).expect("built");

            assert_eq!(
                fences(&effects),
                vec![(FenceScope::Partition(P1), DenyReason::GenerationChanged)],
                "{via} {what}: {effects:?}"
            );
            assert!(
                views(&effects)
                    .iter()
                    .filter(|view| view.lineage.partition == P1)
                    .all(|view| view.valid_through_tick < Tick(5)),
                "{via} {what}: only a past p1 view: {effects:?}"
            );
            assert!(
                !adopts(&effects)
                    .iter()
                    .any(|adopt| matches!(adopt, Shape::Adopt(P1, _))),
                "{via} {what}: {effects:?}"
            );
        }
    }
}

/// A third served partition, for the DN1o row: `p2` sits between two others, so a fence on it
/// is not the first or the last view in the step.
const P3: PartitionId = PartitionId(3);

/// Probe DN1o's row (lead ruling A-R56.4), widened to the tester's probe by lead ruling A-R58
/// (three served partitions, six event kinds): a moved sample republishes every served partition
/// in `PartitionId` order, once each, whatever the event and whatever the step's partition, and
/// every republished view is narrower than the one it replaces. When the event then fences `p2`,
/// `p2`'s last view is past.
///
/// Every step names the unserved `P9`, except the storage fault, which names the partition whose
/// write failed (the storage fence is scoped by the event's partition).
#[retcd_test]
fn a_moved_sample_republishes_every_served_partition_in_partition_order() {
    type Narrowing = fn(&Authority) -> Event;
    let events: [(&str, PartitionId, Narrowing); 6] = [
        ("watch progress", P9, |_| progress(3)),
        ("unrelated CAS", P9, |_| {
            event(
                3,
                ControlEvent::CasResult {
                    request: UNASKED,
                    key: ControlKey::ClusterSchema,
                    outcome: CasOutcome::Conflict {
                        exists: true,
                        current: Revision(1),
                    },
                },
            )
        }),
        ("renew timer", P9, |kernel| {
            event_of(
                3,
                EventKind::Timer(TimerFired {
                    id: AuthorityTimer::Renew.id(),
                    version: kernel.timer_version(AuthorityTimer::Renew),
                    scheduled_at: Tick(2_000),
                }),
            )
        }),
        ("read of an unserved partition", P9, |_| {
            read(3, P9, 11, &record(P9, OTHER, 1))
        }),
        ("P2 moved away", P9, |_| {
            read(3, P2, 11, &record(P2, OTHER, 1))
        }),
        ("storage fault on P2", P2, |_| {
            event_of(
                3,
                EventKind::Storage(StorageEvent::CommitFailed {
                    batch: BatchId(1),
                    fault: StorageFault::WriteFailed,
                }),
            )
        }),
    ];
    let samples = [
        ("600 ms ahead", sample_ctx(2_000, 2_000, 600, 10)),
        ("error 100", sample_ctx(2_000, 2_000, 0, 100)),
        (
            "same sampled_at, error 100",
            sample_ctx(2_000, 1_500, 0, 100),
        ),
    ];
    for (event_name, partition, narrowing) in &events {
        for (sample_name, step_ctx) in &samples {
            let what = format!("{event_name} / {sample_name}");
            let (mut kernel, mut effects) = long_views_of(&[P1, P2, P3]);
            let event = Event {
                partition: *partition,
                ..narrowing(&kernel)
            };
            let step = kernel
                .step(
                    &StepCtx {
                        partition: *partition,
                        ..*step_ctx
                    },
                    &event,
                )
                .expect("built");
            let fresh: Vec<AuthorityView> = views(&step)
                .into_iter()
                .filter(|view| view.valid_through_tick >= Tick(2_000))
                .collect();
            assert_eq!(
                fresh
                    .iter()
                    .map(|view| view.lineage.partition)
                    .collect::<Vec<_>>(),
                vec![P1, P2, P3],
                "{what}: in PartitionId order, once each: {step:?}"
            );
            assert!(
                fresh
                    .iter()
                    .all(|view| view.valid_through_tick < Tick(2_889)),
                "{what}: narrowed: {step:?}"
            );
            effects.extend(step);
            let p2_fenced = *partition == P2;
            if p2_fenced || event_name.starts_with("P2") {
                assert!(
                    newest_views(&effects)[&P2].valid_through_tick < Tick(2_000),
                    "{what}: P2 ends past"
                );
            }
            let fenced: &[PartitionId] = if p2_fenced { &[P2] } else { &[] };
            assert_views_exact_except(&kernel, &effects, &what, fenced);
        }
    }
}

/// Probe DN3l's row (lead ruling A-R56.4): a dropped partition is fenced under the lineage it
/// was **served** at, not the step context's. `p2` is served at epoch 2 so the two differ.
/// Fences come before adopts, each fence bumps `authority_seq` once, and the install once more.
#[retcd_test]
fn a_dropped_partition_is_fenced_under_the_lineage_it_was_served_at() {
    let mut kernel = held_kernel();
    kernel
        .step(
            &sample_ctx(1_000, 1_000, 0, 10),
            &snapshot(
                2,
                10,
                &[
                    record(P1, NODE, 1),
                    record(P2, NODE, 2),
                    record(P9, NODE, 1),
                ],
            ),
        )
        .expect("the install");
    let seq = kernel.authority_seq();
    let step = kernel
        .step(
            &sample_ctx(1_100, 1_000, 0, 10),
            &snapshot(3, 11, &[record(P1, NODE, 1), record(P9, OTHER, 1)]),
        )
        .expect("the reload");
    assert_eq!(
        fences(&step),
        vec![
            (FenceScope::Partition(P2), DenyReason::GenerationChanged),
            (FenceScope::Partition(P9), DenyReason::GenerationChanged),
        ],
        "{step:?}"
    );
    let summary: Vec<(PartitionId, OwnerEpoch, u64)> = views(&step)
        .iter()
        .map(|view| {
            (
                view.lineage.partition,
                view.lineage.owner_epoch,
                view.authority_seq,
            )
        })
        .collect();
    assert_eq!(
        summary,
        vec![
            (P2, OwnerEpoch(2), seq + 1),
            (P9, OwnerEpoch(1), seq + 2),
            (P1, OwnerEpoch(1), seq + 3),
        ],
        "p2 under its served epoch 2, not the step's 1: {step:?}"
    );
    assert!(views(&step)[..2]
        .iter()
        .all(|view| view.valid_through_tick == Tick(1_099)));
    assert_eq!(kernel.authority_seq(), seq + 3);
    let first_adopt = shapes(&step)
        .iter()
        .position(|shape| matches!(shape, Shape::Adopt(..)))
        .expect("p1 adopted");
    let last_fence = shapes(&step)
        .iter()
        .rposition(|shape| matches!(shape, Shape::Fence(..)))
        .expect("fences");
    assert!(last_fence < first_adopt, "fences before adopts: {step:?}");
    assert_eq!(
        kernel.may_admit_at(
            Lineage {
                partition: P2,
                generation: Generation(1),
                owner_epoch: OwnerEpoch(2),
            },
            Tick(1_100),
            &BUDGETS
        ),
        Verdict::Deny(DenyReason::GenerationChanged)
    );
}

/// Probe DN2h's row (lead ruling A-R56.4): a sample older than the held one is refused in
/// **every** state, and the held one stays — also when the older sample is itself invalid.
#[retcd_test]
fn an_older_sample_is_refused_in_every_state_and_the_held_one_stays() {
    let rejected = || Shape::Ignored(AuthorityIgnoreReason::SampleRejected);
    let sampled_at = |kernel: &Authority| kernel.clock().sample().map(|sample| sample.sampled_at);

    // Unheld.
    let mut kernel = Authority::new();
    kernel
        .step(&sample_ctx(100, 100, 0, 10), &progress(1))
        .expect("built");
    assert_eq!(sampled_at(&kernel), Some(Tick(100)), "fixture: accepted");
    let step = kernel
        .step(&sample_ctx(200, 50, 0, 10), &progress(2))
        .expect("built");
    assert_eq!(shapes(&step), vec![rejected()], "unheld");
    assert_eq!(sampled_at(&kernel), Some(Tick(100)), "unheld keeps it");

    // Held, a valid older sample and two invalid ones.
    let (mut kernel, _) = long_views();
    let older = sample_ctx(1_600, 1_499, 0, 10);
    let unbounded = StepCtx {
        control_time: ControlTime {
            bound_established: false,
            ..older.control_time
        },
        ..older
    };
    let too_loose = StepCtx {
        control_time: ControlTime {
            error_millis: 500,
            ..older.control_time
        },
        ..older
    };
    for (what, step_ctx) in [
        ("valid", older),
        ("unbounded", unbounded),
        ("over the ceiling", too_loose),
    ] {
        let step = kernel.step(&step_ctx, &progress(3)).expect("built");
        assert_eq!(shapes(&step), vec![rejected()], "held, {what}");
        assert!(kernel.state().is_held(), "held, {what}: no fence");
        assert_eq!(sampled_at(&kernel), Some(Tick(1_500)), "held, {what}");
    }
    assert_eq!(
        kernel.may_admit_at(p1_at(1), Tick(1_600), &BUDGETS),
        Verdict::Admit
    );

    // Fenced.
    kernel
        .step(
            &sample_ctx(1_700, 1_500, 0, 10),
            &event_of(
                4,
                EventKind::Node(NodeLifecycle::Rebooted { boot: BootId(2) }),
            ),
        )
        .expect("the node fence");
    assert!(kernel.state().is_fenced(), "fixture");
    let step = kernel
        .step(&sample_ctx(1_800, 1_499, 0, 10), &progress(5))
        .expect("built");
    assert!(shapes(&step).contains(&rejected()), "fenced: {step:?}");
    assert_eq!(sampled_at(&kernel), Some(Tick(1_500)), "fenced keeps it");
}

/// Probe DN2d's row (lead ruling A-R56.4): the drift term of the epsilon is live. A sample taken
/// at 800, 89 ms ahead, error 10: at age 2000 (tick 2800) 500 ppm adds 1 ms, and that 1 ms fails
/// the clock conjunct as `Expired`. Without drift the view would reach 2800 and end on
/// staleness instead.
#[retcd_test]
fn a_view_is_exact_where_one_millisecond_of_drift_decides() {
    let mut kernel = held_kernel();
    let effects = kernel
        .step(
            &sample_ctx(800, 800, 89, 10),
            &snapshot(2, 10, &[record(P1, NODE, 1)]),
        )
        .expect("the install");
    let view = newest_views(&effects)[&P1];
    assert_eq!(
        (view.valid_through_tick, view.past_horizon),
        (Tick(2_799), DenyReason::Expired),
        "{effects:?}"
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(1), Tick(2_799), &BUDGETS),
        Verdict::Admit
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(1), Tick(2_800), &BUDGETS),
        Verdict::Deny(DenyReason::Expired)
    );
}

// =============================================================================================
// Plan rows, §3.1 of `docs/testing/test-plan-m7-kernel-a.md`: the partition lineage path. One
// function per row, named `m7a_NN_<plan name>`; each was seen to fail under a mutant in a private
// copy before it was accepted (lead ruling A-R57).
// =============================================================================================

/// The `AdoptAuthority` effects in `effects`, in order.
fn adopts(effects: &[Effect]) -> Vec<Shape> {
    shapes(effects)
        .into_iter()
        .filter(|shape| matches!(shape, Shape::Adopt(..)))
        .collect()
}

/// M7A-04. `design.md` §2.4 lineage path "FamilyOk replaces `served` wholesale"; ADR 0008 §7
/// item 6; F-R10.
///
/// A snapshot `[p1 e3, p2 e1]`, then a newer one `[p1 e4]`. After the second, `served` is
/// `{p1: e4}` — `p2` is gone, not merged — and `partitions_revision` is the second snapshot's
/// revision. Each snapshot emits one `AdoptAuthority` per partition it serves and its install
/// bumps `authority_seq` once.
///
/// **Deviation, recorded in the handoff (question to the lead):** the plan says `authority_seq`
/// moves once per snapshot. Since lead ruling A-R54.2 a snapshot that drops a served partition
/// fences it first, and a fence moves `authority_seq` too, so the second snapshot moves it twice.
/// The row counts both separately: exactly one partition fence (`p2`, `GenerationChanged`), and a
/// delta of one for the install plus one per fence.
#[retcd_test]
fn m7a_04_lineage_family_ok_replaces_served_wholesale() {
    let mut kernel = held_kernel();
    let seq = kernel.authority_seq();

    let first = kernel
        .step(
            &ctx(2),
            &snapshot(2, 10, &[record(P1, NODE, 3), record(P2, NODE, 1)]),
        )
        .expect("the install");
    assert_eq!(
        adopts(&first),
        vec![
            Shape::Adopt(P1, OwnerEpoch(3)),
            Shape::Adopt(P2, OwnerEpoch(1))
        ]
    );
    assert_eq!(kernel.authority_seq(), seq + 1, "one bump for the first");

    let second = kernel
        .step(&ctx(3), &snapshot(3, 11, &[record(P1, NODE, 4)]))
        .expect("the reinstall");
    assert_eq!(adopts(&second), vec![Shape::Adopt(P1, OwnerEpoch(4))]);
    let view = kernel.view();
    assert_eq!(
        view.served,
        std::collections::BTreeMap::from([(P1, lineage(4))]),
        "replaced wholesale: p2 is gone, not merged"
    );
    assert_eq!(view.partitions_revision, Some(Revision(11)));
    let dropped = fences(&second);
    assert_eq!(
        dropped,
        vec![(FenceScope::Partition(P2), DenyReason::GenerationChanged)],
        "the drop fences p2 (A-R54.2)"
    );
    assert_eq!(
        view.authority_seq,
        seq + 1 + 1 + dropped.len() as u64,
        "one bump for the second install, plus the fence's own"
    );
}

/// M7A-05. `design.md` §2.4 "WatchEvent ⇒ Read, never state change"; ADR 0008 "watch never
/// grants".
///
/// A `Watched` delivery on the partitions family naming `p1` at revision 15. The watch carries a
/// revision and no body (TD-17), so whatever the record behind it says, A1 cannot see it: the
/// only effect is `Get{Partition(p1)}`, and every field of the state view but the watch cursor is
/// identical before and after — `served`, `state`, `expiry_utc_ms`, `authority_seq` among them.
/// No `AdoptAuthority`.
#[retcd_test]
fn m7a_05_lineage_watch_event_reads_never_mutates() {
    let mut kernel = serving_p1(1);
    let before = kernel.view();

    let effects = kernel
        .step(
            &ctx(3),
            &event(
                3,
                ControlEvent::Watched {
                    prefix: ControlPrefix::Partitions,
                    cursor: WatchCursor {
                        revision: Revision(15),
                    },
                    changes: vec![ControlChange {
                        key: ControlKey::Partition(P1),
                        revision: Revision(15),
                    }],
                },
            ),
        )
        .expect("the watch row is built");

    assert!(
        matches!(
            effects.as_slice(),
            [Effect {
                kind: EffectKind::Control(ControlEffect::Get {
                    key: ControlKey::Partition(P1),
                    ..
                }),
                ..
            }]
        ),
        "{effects:?}"
    );
    let mut after = kernel.view();
    after.cursors.clone_from(&before.cursors);
    assert_eq!(after, before, "only the cursor moved");
}

/// M7A-06. `design.md` §2.4 "ReadOk owner≠us ⇒ Fence{Partition, GenerationChanged}"; ADR 0007 §3
/// fence table, partition scope.
///
/// `p1` served at generation 1; a read of it names another owner at generation 2. One partition
/// fence, and the node stays `Held`.
#[retcd_test]
fn m7a_06_lineage_read_owner_not_us_fences_partition_generation_changed() {
    let mut kernel = serving_p1(1);
    let moved = PartitionRecord {
        owner: OTHER,
        generation: Generation(2),
        ..record(P1, NODE, 1)
    };

    let effects = kernel
        .step(&ctx(3), &read(3, P1, 15, &moved))
        .expect("the partition read row is built");

    assert_eq!(
        fences(&effects),
        vec![(FenceScope::Partition(P1), DenyReason::GenerationChanged)]
    );
    assert!(
        kernel.state().is_held(),
        "a partition fence, not a node fence"
    );
}

/// M7A-07. The twin of M7A-06, one fact: the record names us, at the generation served. No
/// fence, and `served[p1]` keeps that generation.
#[retcd_test]
fn m7a_07_lineage_read_owner_us_no_fence() {
    let mut kernel = serving_p1(1);

    let effects = kernel
        .step(&ctx(3), &read(3, P1, 15, &record(P1, NODE, 1)))
        .expect("the partition read row is built");

    assert_eq!(fences(&effects), vec![]);
    assert_eq!(
        kernel
            .view()
            .served
            .get(&P1)
            .map(|served| served.generation),
        Some(Generation(1))
    );
}

// =============================================================================================
// Plan rows, §3.2 of `docs/testing/test-plan-m7-kernel-a.md`: the partition-scoped fences.
// =============================================================================================

/// M7A-25. `design.md` §2.4 `LocalStorageFailure ⇒ Fence{Partition, LocalStorageFenced}`; ADR
/// 0007 §3 partition scope.
///
/// `p1` and `p2` served; a local write for `p1` fails (`CommitFailed{WriteFailed}`, the landed
/// spelling of `LocalStorageFailure`). One partition fence for `p1`; `storage_fenced == {p1}`;
/// the node stays `Held`, and `p2` still admits.
#[retcd_test]
fn m7a_25_local_storage_failure_fences_partition_stays_held() {
    let (mut kernel, _) = serving_p1_p2();

    let effects = kernel
        .step(
            &ctx(3),
            &event_of(
                3,
                EventKind::Storage(StorageEvent::CommitFailed {
                    batch: BatchId(1),
                    fault: StorageFault::WriteFailed,
                }),
            ),
        )
        .expect("the storage fence row is built");

    assert_eq!(
        fences(&effects),
        vec![(FenceScope::Partition(P1), DenyReason::LocalStorageFenced)]
    );
    assert_eq!(
        kernel.view().storage_fenced,
        std::collections::BTreeSet::from([P1])
    );
    assert!(kernel.state().is_held());
    let p2 = Lineage {
        partition: P2,
        ..p1_at(1)
    };
    assert_eq!(kernel.may_admit_at(p2, Tick(3), &BUDGETS), Verdict::Admit);
}

/// M7A-26. `design.md` §2.4 `RevokeEpochRequested ⇒ PersistEpochRevocation`;
/// `EpochRevocationPersisted ⇒ Fence{Partition, EpochRevoked}`; ADR 0007 §3 "epoch revoked via
/// durable drain".
///
/// Two halves in one function, the first being the plan's twin: the request asks storage to make
/// the revocation durable and fences nothing; the durable completion fences `p1` `EpochRevoked`
/// and records `(p1, e3)` in `revoked_epochs`.
#[retcd_test]
fn m7a_26_revoke_epoch_persist_then_fence_epoch_revoked() {
    let mut kernel = serving_p1(3);
    let requested = event_of(
        3,
        EventKind::Kernel(KernelEvent::Authority(
            AuthorityEvent::RevokeEpochRequested {
                partition: P1,
                epoch: OwnerEpoch(3),
            },
        )),
    );

    let request = kernel.step(&ctx(3), &requested).expect("built");
    assert!(
        matches!(
            request.as_slice(),
            [Effect {
                kind: EffectKind::Store(StoreEffect::PersistEpochRevocation {
                    partition: P1,
                    epoch: OwnerEpoch(3),
                }),
                ..
            }]
        ),
        "{request:?}"
    );
    assert!(kernel.view().revoked_epochs.is_empty(), "not durable yet");

    let persisted = kernel
        .step(&ctx(4), &revocation_persisted(4, P1, 3))
        .expect("built");
    assert_eq!(
        fences(&persisted),
        vec![(FenceScope::Partition(P1), DenyReason::EpochRevoked)]
    );
    assert!(kernel.view().revoked_epochs.contains(&(P1, OwnerEpoch(3))));
}

// =============================================================================================
// Plan rows, §3.6 of `docs/testing/test-plan-m7-kernel-a.md`: the checkpoint pair.
// =============================================================================================

/// M7A-60. `design.md` §1.2 `Check{checkpoint, correlation}` / `AuthorityDecision{authority_seq}`;
/// K-A-34.
///
/// `p1` served. Three checks, one per cross-module checkpoint, each correlated by its own id and
/// carried in an envelope correlated by another, so an answer that echoes the envelope instead of
/// the check is red. Each answer names its own checkpoint and correlation, in order, carries A1's
/// current `authority_seq`, and stamps `decided_at` with the tick it was decided at.
#[retcd_test]
fn m7a_60_check_answer_pair_echoes_correlation_checkpoint_and_authority_seq() {
    let mut kernel = serving_p1(3);
    let seq = kernel.view().authority_seq;
    assert_ne!(seq, 0, "fixture: a sequence a constant cannot match");

    let checks = [
        (Checkpoint::StorageDispatch, 9),
        (Checkpoint::Publication, 10),
        (Checkpoint::Reply, 11),
    ];
    for (at, (checkpoint, correlation)) in (20..).zip(checks) {
        let check = event_of(
            at,
            EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Check {
                checkpoint,
                lineage: p1_at(3),
                correlation: CorrelationId(correlation),
            })),
        );
        let effects = kernel
            .step(&ctx(at), &check)
            .expect("the check row is built");

        let [Effect {
            kind: EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Answer(decision))),
            ..
        }] = effects.as_slice()
        else {
            panic!("one answer per check: {effects:?}");
        };
        assert_eq!(
            (
                decision.checkpoint,
                decision.correlation,
                decision.authority_seq,
                decision.decided_at,
                decision.lineage,
                decision.verdict,
            ),
            (
                checkpoint,
                CorrelationId(correlation),
                seq,
                Tick(at),
                p1_at(3),
                Verdict::Admit,
            ),
            "check {correlation}"
        );
    }
}

/// M7A-61. `design.md` §2.5: `OutboxDispatch` is declared and unused in M7.
///
/// The plan allows two answers: `Check{OutboxDispatch}` denies `ControlUnavailable`, **or** the
/// variant is `#[cfg(feature = "m11")]`-gated (§13 Q-7 recommends the gate). The gate is not
/// available to this row: `Checkpoint` lives in `crates/rdb-core/src/contracts/authority.rs`,
/// which kernel-a may not edit, and `OutboxDispatch` compiles in every M7 build. So the code
/// supports only the first answer, and this row asserts it. Lead ruling A-R77b confirms that
/// branch: no gate, no contract ask.
///
/// Twin, one fact apart: the same kernel, the same lineage, the same tick, at
/// `StorageDispatch`. That one admits. Without it a deny here could be any of the reasons a
/// served `p1` might fail, and the row would not show the checkpoint is what decides.
#[retcd_test]
fn m7a_61_outbox_dispatch_checkpoint_unused_in_m7() {
    let mut kernel = serving_p1(3);

    let answer = |kernel: &mut Authority, checkpoint: Checkpoint, id: u64| {
        let check = event_of(
            id,
            EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Check {
                checkpoint,
                lineage: p1_at(3),
                correlation: CorrelationId(id),
            })),
        );
        let effects = kernel
            .step(&ctx(20), &check)
            .expect("the check row is built");
        let [Effect {
            kind: EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Answer(decision))),
            ..
        }] = effects.as_slice()
        else {
            panic!("one answer per check: {effects:?}");
        };
        (decision.checkpoint, decision.verdict)
    };

    assert_eq!(
        answer(&mut kernel, Checkpoint::StorageDispatch, 30),
        (Checkpoint::StorageDispatch, Verdict::Admit),
        "M7A-61 twin: a served p1 at a live checkpoint admits, so a deny below is the checkpoint's"
    );
    assert_eq!(
        answer(&mut kernel, Checkpoint::OutboxDispatch, 31),
        (
            Checkpoint::OutboxDispatch,
            Verdict::Deny(DenyReason::ControlUnavailable)
        ),
        "M7A-61: the outbox checkpoint is unused in M7, so it never admits"
    );
    assert!(
        kernel.state().is_held(),
        "M7A-61: a deny, not a fence: the grant is untouched"
    );
}

// =============================================================================================
// Plan rows, §8 of `docs/testing/test-plan-m7-kernel-a.md`: the published horizon.
// =============================================================================================

/// M7A-143's sample, in the plan's own numbers: taken at tick 0, authority time 5 000 000, ε 20.
const HORIZON_SAMPLE: ControlTime = ControlTime {
    estimate: Tick(5_000_000),
    error_millis: 20,
    bound_established: true,
    sampled_at: Tick(0),
};

/// Acquired at tick 0 under [`HORIZON_SAMPLE`] and `budgets` (so `renewed_at` 0 and
/// `E = 5 000 000 + grant_millis`), then `p1` installed at tick 1: the install's view of `p1`,
/// and the whole install step.
fn horizon_install(budgets: &Budgets) -> (Authority, AuthorityView, Vec<Effect>) {
    let at = |now: u64| StepCtx {
        control_time: HORIZON_SAMPLE,
        budgets,
        ..ctx(now)
    };
    let (mut kernel, _) = acquired_under(&at(0));
    let effects = kernel
        .step(&at(1), &snapshot(2, 10, &[record(P1, NODE, 1)]))
        .expect("the install is built");
    let view = *views(&effects)
        .last()
        .unwrap_or_else(|| panic!("the install publishes p1: {effects:?}"));
    (kernel, view, effects)
}

/// M7A-143. K-A-53; `design.md` §1.7 `valid_through_tick = min(local_horizon, utc_horizon)`,
/// `a_max` the **largest** `a ≥ 0` with `utc + a < E − ε − floor(a·ppm/1e6) − δ` (the
/// inequality, not a closed form); `past_horizon`.
///
/// (a) `renewed_at 0`, grant 3000, δ 100 (local horizon 2899); `E − utc = 3000`, ε 20, 500 ppm:
/// `a_max` is 2878 (`2878 + 1 < 2880`, and 2879 is not). With a 2000-tick sample age the sample
/// goes stale first, so the view is `(2000, ClockSampleStale)`. Twin, one fact (age 4000):
/// `(2878, Expired)`. (b) T-A-13's exact-division twin: grant 2121 makes `E − utc − ε − δ` 2001,
/// and the inequality gives 1999 where `floor(2001 / 1.0005)` would give 2000. (c) No solution
/// (`E − utc == ε + δ`, grant 120): the view is `now − 1`. **It arrives as a fence** — the same
/// comparison is the expiry conjunct, so the node fences `Expired` before any horizon is asked
/// for, and the fence's view carries `now − 1`.
///
/// Every view is also checked against `may_admit` on both sides of its horizon.
#[retcd_test]
fn m7a_143_authority_view_valid_through_tick_formula() {
    let spec = Budgets::SPEC_DEFAULTS;
    let cases = [
        ("(a)", spec, 2_000, DenyReason::ClockSampleStale),
        (
            "(a) twin, sample age 4000",
            Budgets {
                max_sample_age_millis: 4_000,
                ..spec
            },
            2_878,
            DenyReason::Expired,
        ),
        (
            "(b) exact division",
            Budgets {
                grant_millis: 2_121,
                ..spec
            },
            1_999,
            DenyReason::Expired,
        ),
    ];
    for (what, budgets, through, past) in cases {
        let (kernel, view, _) = horizon_install(&budgets);
        assert_eq!(
            (view.valid_through_tick, view.past_horizon),
            (Tick(through), past),
            "{what}"
        );
        assert_eq!(
            kernel.may_admit_at(p1_at(1), Tick(through), &budgets),
            Verdict::Admit,
            "{what}"
        );
        assert_eq!(
            kernel.may_admit_at(p1_at(1), Tick(through + 1), &budgets),
            Verdict::Deny(past),
            "{what}"
        );
    }

    let no_solution = Budgets {
        grant_millis: 120,
        ..spec
    };
    let (_, view, effects) = horizon_install(&no_solution);
    assert_eq!(
        fences(&effects),
        vec![(FenceScope::Node, DenyReason::Expired)],
        "(c): {effects:?}"
    );
    assert_eq!(
        (view.valid_through_tick, view.past_horizon),
        (Tick(0), DenyReason::Expired),
        "(c)"
    );
}

/// The partitions `effects` publishes a view for, in order.
fn published_for(effects: &[Effect]) -> Vec<PartitionId> {
    views(effects)
        .iter()
        .map(|view| view.lineage.partition)
        .collect()
}

/// A read of our own grant record at `revision`, with `expiry_utc_ms` and `frozen` as given and
/// every identity field the kernel holds.
fn our_grant_read(kernel: &Authority, id: u64, revision: u64, e: i64, frozen: bool) -> Event {
    let held = kernel.held().expect("fixture: Held").identity();
    let record = GrantRecord {
        grant: held.grant,
        node: NODE,
        boot: held.boot,
        authority_generation: held.authority_generation,
        expiry_utc_ms: e,
        frozen,
    };
    event(
        id,
        ControlEvent::Value {
            request: UNASKED,
            key: ControlKey::Grant(NODE),
            outcome: ReadOutcome::Found {
                revision: Revision(revision),
                value: record.encode(),
            },
        },
    )
}

/// `Renew` fired at the version the kernel armed, under correlation `id`.
fn renew_due(kernel: &Authority, id: u64, at: u64) -> Event {
    event_of(
        id,
        EventKind::Timer(TimerFired {
            id: AuthorityTimer::Renew.id(),
            version: kernel.timer_version(AuthorityTimer::Renew),
            scheduled_at: Tick(at),
        }),
    )
}

/// The grant CAS in flight on `kernel` answered `outcome`, under correlation `id`.
fn renew_answered(kernel: &Authority, id: u64, outcome: CasOutcome) -> Event {
    event(
        id,
        ControlEvent::CasResult {
            request: in_flight(kernel),
            key: ControlKey::Grant(NODE),
            outcome,
        },
    )
}

/// M7A-145. K-A-35, K-A-53; `design.md` §1.7's republish list (grant adoption, committed
/// renewal, accepted `Clock(s)`, `served[id]` write, fence) and its fan-out rule; §2.4.
///
/// One trace on a kernel serving `p1` and `p2`, touching each of the five points, plus a renewal
/// `Conflict` and two `Watched` deliveries:
///
/// * the four node-scoped points publish one view per served partition, `[p1, p2]`;
/// * the `served[p2]` write publishes one view, for `p2`, with `authority_seq` bumped;
/// * the committed renewal's views keep `authority_seq` and promise later than the views before
///   it (3600, the renewed sample's age bound, against 2889, the old `E`'s);
/// * the fence's views are `(fence_tick − 1, Frozen)`;
/// * the `Conflict` and the `Watched` publish nothing.
///
/// Every step after the first holds the sample the first one moved to, so no view below comes
/// from the clock row except the first step's.
#[retcd_test]
fn m7a_145_authority_view_republished_at_five_points_fans_out_per_served_partition() {
    let (mut kernel, _) = long_views();
    let at = |now: u64| sample_ctx(now, 1_600, 0, 10);
    let both = vec![P1, P2];

    let sample = kernel.step(&at(1_600), &progress(29)).expect("the sample");
    assert_eq!(published_for(&sample), both, "accepted sample: {sample:?}");

    let seq = kernel.authority_seq();
    kernel
        .step(&at(1_600), &renew_due(&kernel, 30, 1_600))
        .expect("RenewDue");
    let committed = kernel
        .step(
            &at(1_601),
            &renew_answered(&kernel, 30, CasOutcome::Committed(Revision(8))),
        )
        .expect("the renewal commit");
    assert_eq!(published_for(&committed), both, "committed renewal");
    for (renewed, before) in views(&committed).iter().zip(views(&sample)) {
        assert_eq!(renewed.authority_seq, seq, "the renewal moves no seq");
        assert!(
            renewed.valid_through_tick > before.valid_through_tick,
            "the renewal promises later: {renewed:?} after {before:?}"
        );
        assert_eq!(
            (renewed.valid_through_tick, renewed.past_horizon),
            (Tick(3_600), DenyReason::ClockSampleStale),
            "the renewed horizon is the sample's age bound, not the new E's"
        );
    }

    kernel
        .step(&at(1_700), &renew_due(&kernel, 31, 1_700))
        .expect("RenewDue");
    let conflict = kernel
        .step(
            &at(1_701),
            &renew_answered(
                &kernel,
                31,
                CasOutcome::Conflict {
                    exists: true,
                    current: Revision(9),
                },
            ),
        )
        .expect("the renewal conflict");
    assert_eq!(published_for(&conflict), vec![], "Conflict: {conflict:?}");

    let adoption = kernel
        .step(
            &at(1_702),
            &our_grant_read(&kernel, 32, 9, 1_004_700, false),
        )
        .expect("the grant adoption");
    assert!(
        shapes(&adoption).contains(&Shape::Fact(AuthorityFact::Adopted)),
        "fixture: {adoption:?}"
    );
    assert_eq!(published_for(&adoption), both, "grant adoption");

    for prefix in [ControlPrefix::Partitions, ControlPrefix::Grants] {
        let key = match prefix {
            ControlPrefix::Grants => ControlKey::Grant(NODE),
            _ => ControlKey::Partition(P1),
        };
        let watched = ControlEvent::Watched {
            prefix,
            cursor: WatchCursor {
                revision: Revision(15),
            },
            changes: vec![ControlChange {
                key,
                revision: Revision(15),
            }],
        };
        let effects = kernel
            .step(&at(1_703), &event(33, watched))
            .expect("the watch delivery");
        assert_eq!(published_for(&effects), vec![], "{prefix:?}: {effects:?}");
    }

    let seq = kernel.authority_seq();
    let write = kernel
        .step(&at(1_704), &read(34, P2, 12, &record(P2, NODE, 2)))
        .expect("the served[p2] write");
    assert_eq!(
        published_for(&write),
        vec![P2],
        "served[p2] write: {write:?}"
    );
    assert_eq!(
        views(&write)[0].authority_seq,
        seq + 1,
        "the write bumps seq"
    );

    let fence = kernel
        .step(
            &at(1_705),
            &our_grant_read(&kernel, 35, 10, 1_004_700, true),
        )
        .expect("the fence");
    assert_eq!(fences(&fence), vec![(FenceScope::Node, DenyReason::Frozen)]);
    assert_eq!(published_for(&fence), both, "fence");
    for view in views(&fence) {
        assert_eq!(
            (view.valid_through_tick, view.past_horizon),
            (Tick(1_704), DenyReason::Frozen),
            "{view:?}"
        );
    }
}

// =============================================================================================
// The post-`Recovered` install (`design.md` §2.4, the `Recovered` pair; lead ledger L-R177gf,
// lead ruling A-R78).
// =============================================================================================

/// `partitions/{partition}` naming this node at `generation` and `epoch`.
fn record_at(partition: PartitionId, generation: u64, epoch: u64) -> PartitionRecord {
    PartitionRecord {
        generation: Generation(generation),
        ..record(partition, NODE, epoch)
    }
}

/// F1's `Recovered` for `partition`, under correlation `id`: the prior owner served
/// `(prior.0, prior.1)` as `(generation, owner_epoch)`, and the activation CAS committed
/// `new_generation` at revision 20. Only `fenced_prior` and `new_generation` are A1's inputs;
/// the rest is filled in so the value is whole.
fn recovered(id: u64, partition: PartitionId, prior: (u64, u64), new_generation: u64) -> Event {
    let (prior_generation, prior_epoch) = (Generation(prior.0), OwnerEpoch(prior.1));
    let root = Lineage {
        partition,
        generation: prior_generation,
        owner_epoch: prior_epoch,
    };
    let result = RecoveryResult {
        fenced_prior: FencingProof {
            partition,
            prior_generation,
            prior_owner_epoch: prior_epoch,
            prior_grant_id: GrantId(1),
            prior_boot_id: BOOT,
            revocation: Revocation::DurableDrain {
                ack_revision: Revision(1),
            },
            control_revision: Revision(1),
            decision_tick: Tick::ZERO,
        },
        inventories: Vec::new(),
        selected: SelectedLineage {
            root,
            cutoff_seq: Seq::ZERO,
            cutoff_digest: Digest::ROOT,
            source: CopyId(1),
        },
        new_generation: Generation(new_generation),
        mode: PartitionMode::Active,
        barrier: RecoveryBarrier::try_new(&[], &Default::default(), Seq::ZERO, Digest::ROOT)
            .expect("an empty required set needs no proof"),
        loss: LossRecord {
            queried: Vec::new(),
            unavailable: Vec::new(),
            cutoff_seq: Seq::ZERO,
            highest_advertised_seq: Seq::ZERO,
            uncertain: false,
        },
        committed: CommittedRoot {
            revision: Revision(20),
            pinned_config: PartitionConfig::new(partition, ConfigVersion(1), Vec::new()),
            authority_view: AuthorityView {
                lineage: Lineage {
                    generation: Generation(new_generation),
                    ..root
                },
                grant_id: GrantId(1),
                boot_id: BOOT,
                authority_generation: AuthorityGeneration::default(),
                config_version: ConfigVersion(1),
                authority_seq: 0,
                valid_through_tick: Tick::ZERO,
                past_horizon: DenyReason::Expired,
            },
        },
        retained_status_map: RetainedStatusMap {
            predecessor_generation: prior_generation,
            predecessor_cutoff: Seq::ZERO,
            retained_through: Seq::ZERO,
            discarded_from: None,
            uncertain: false,
        },
    };
    Event {
        partition,
        ..event_of(
            id,
            EventKind::Kernel(KernelEvent::Recovered(Box::new(result))),
        )
    }
}

/// `effects` split for the `Recovered` trigger row: each `Get` as the partition it is routed to
/// and the key it reads, and everything else through [`shapes`].
fn trigger(effects: &[Effect]) -> (Vec<(PartitionId, ControlKey)>, Vec<Shape>) {
    let gets = effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Control(ControlEffect::Get { key, .. }) => Some((effect.partition, *key)),
            _ => None,
        })
        .collect();
    let rest = effects
        .iter()
        .filter(|effect| !matches!(effect.kind, EffectKind::Control(ControlEffect::Get { .. })))
        .cloned()
        .collect::<Vec<_>>();
    (gets, shapes(&rest))
}

/// The lineage of every `AdoptAuthority` in `effects`, by partition, in order.
fn adopted_lineages(effects: &[Effect]) -> Vec<(PartitionId, ServedLineage)> {
    effects
        .iter()
        .filter_map(|effect| match effect.kind {
            EffectKind::AdoptAuthority {
                partition,
                generation,
                owner_epoch,
                config_version,
            } => Some((
                partition,
                ServedLineage {
                    generation,
                    owner_epoch,
                    config_version,
                },
            )),
            _ => None,
        })
        .collect()
}

/// M7A-176. `design.md` §2.4, the `Recovered` pair: the trigger row
/// (`Held|Unheld|Fenced | Recovered(r)` ⇒ `Control(Read{partitions/{r.partition}})`,
/// `Fact(RecoveryObserved)`, no rights change) and the install row above the generic changed row
/// (K-A-55): the read-back that trigger issued, naming `r.new_generation` and us, is
/// `AdoptAuthority`, `PublishAuthorityView`, `Fact(LineageInstalled)` with `authority_seq += 1`.
/// Lead ledger L-R177gf (inv-publish-path break 2: A1 declined `Recovered`, so no adopt followed
/// F1 and T1's seed never loaded). Lead ruling A-R78 for the `Unheld` case: the read-back installs
/// nothing, and the acquire's coherent load installs the recovered generation (§2.1).
///
/// Near misses: the same record read under another correlation is the generic changed row
/// (`LineageChanged`), after which the recovery's own read-back is `LineageUnchanged`; a
/// read-back naming another generation than the recovery's is the generic row too; and a late
/// read-back older than an install a newer read made is A-R48 superseded, with no roll-back.
#[retcd_test]
fn m7a_176_post_recovered_read_back_installs_lineage() {
    // Held, serving p1 only; F1 recovers p2 (prior g1 e1, which this node does not serve) to g2.
    let mut kernel = serving_p1(1);
    let before = kernel.view();
    let effects = kernel
        .step(&ctx(3), &recovered(40, P2, (1, 1), 2))
        .expect("the Recovered trigger row is built");
    let read_back = asked(&effects);
    assert_eq!(
        trigger(&effects),
        (
            vec![(P2, ControlKey::Partition(P2))],
            vec![Shape::Fact(AuthorityFact::RecoveryObserved)]
        ),
        "one linearizable read of the recovered partition, and the fact"
    );
    assert_eq!(kernel.view(), before, "no rights change at the trigger");

    let effects = kernel
        .step(
            &ctx(4),
            &read_as(read_back, 40, P2, 20, &record_at(P2, 2, 2)),
        )
        .expect("the install row is built");
    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Adopt(P2, OwnerEpoch(2)),
            Shape::Publish(P2),
            Shape::Fact(AuthorityFact::LineageInstalled),
        ]
    );
    let installed = record_at(P2, 2, 2).lineage();
    assert_eq!(adopted_lineages(&effects), vec![(P2, installed)]);
    let view = kernel.view();
    assert_eq!(view.served.get(&P2), Some(&installed), "served[p2] = g2");
    assert_eq!(view.served.get(&P1), before.served.get(&P1), "p1 untouched");
    assert_eq!(view.authority_seq, before.authority_seq + 1, "K-A-34");

    // Near miss: the same record, read as the answer to a request the trigger did not issue.
    let mut kernel = serving_p1(1);
    let read_back = asked(
        &kernel
            .step(&ctx(3), &recovered(40, P2, (1, 1), 2))
            .expect("trigger"),
    );
    let effects = kernel
        .step(&ctx(4), &read(41, P2, 20, &record_at(P2, 2, 2)))
        .expect("a generic read");
    assert_eq!(
        shapes(&effects)[2..],
        [Shape::Fact(AuthorityFact::LineageChanged)],
        "not the recovery's read: the generic changed row"
    );
    let seq = kernel.authority_seq();
    let effects = kernel
        .step(
            &ctx(5),
            &read_as(read_back, 40, P2, 20, &record_at(P2, 2, 2)),
        )
        .expect("the recovery's read-back, late");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::LineageUnchanged)],
        "already served: nothing re-installed, no bump"
    );
    assert_eq!(kernel.authority_seq(), seq);

    // Near miss: the recovery's read-back names a generation other than the recovery's.
    let mut kernel = serving_p1(1);
    let read_back = asked(
        &kernel
            .step(&ctx(3), &recovered(40, P2, (1, 1), 2))
            .expect("trigger"),
    );
    let effects = kernel
        .step(
            &ctx(4),
            &read_as(read_back, 40, P2, 20, &record_at(P2, 3, 2)),
        )
        .expect("a read-back at g3");
    assert_eq!(
        shapes(&effects)[2..],
        [Shape::Fact(AuthorityFact::LineageChanged)],
        "part.generation != r.new_generation: the generic changed row"
    );

    // Unheld at Recovered (A-R78): the trigger still reads and observes...
    let mut kernel = Authority::new();
    let effects = kernel
        .step(&ctx(3), &recovered(40, P1, (1, 1), 2))
        .expect("the trigger row answers in every state");
    let read_back = asked(&effects);
    assert_eq!(
        trigger(&effects),
        (
            vec![(P1, ControlKey::Partition(P1))],
            vec![Shape::Fact(AuthorityFact::RecoveryObserved)]
        )
    );
    // ...its read-back installs nothing, because there is no grant to serve it under...
    let effects = kernel
        .step(
            &ctx(4),
            &read_as(read_back, 40, P1, 20, &record_at(P1, 2, 2)),
        )
        .expect("an unheld read-back");
    assert_eq!(adopted_lineages(&effects), vec![], "Unheld: no install");
    assert!(kernel.view().served.is_empty());
    // ...and the acquire's coherent load installs the recovered generation.
    let _ = acquire(&mut kernel, &ctx(5));
    let effects = kernel
        .step(&ctx(6), &snapshot(6, 20, &[record_at(P1, 2, 2)]))
        .expect("the coherent load");
    assert_eq!(
        adopted_lineages(&effects),
        vec![(P1, record_at(P1, 2, 2).lineage())]
    );
    assert!(shapes(&effects).contains(&Shape::Fact(AuthorityFact::LineageLoaded)));

    // The answered read is forgotten: after the acquire, a read echoing its id is the generic
    // changed row, not a second install.
    let mut kernel = Authority::new();
    let read_back = asked(
        &kernel
            .step(&ctx(3), &recovered(40, P1, (1, 1), 2))
            .expect("trigger"),
    );
    let _ = kernel
        .step(
            &ctx(4),
            &read_as(read_back, 40, P1, 20, &record_at(P1, 2, 2)),
        )
        .expect("an unheld read-back");
    let _ = acquire(&mut kernel, &ctx(5));
    let effects = kernel
        .step(
            &ctx(6),
            &read_as(read_back, 40, P1, 21, &record_at(P1, 2, 2)),
        )
        .expect("a read after the acquire");
    assert_eq!(
        shapes(&effects)[2..],
        [Shape::Fact(AuthorityFact::LineageChanged)]
    );

    // (f) A late read-back older than an install a newer read already made (A-R48): superseded,
    // not installed, and the newer lineage is not rolled back.
    let mut kernel = serving_p1(1);
    let read_back = asked(
        &kernel
            .step(&ctx(3), &recovered(40, P2, (1, 1), 2))
            .expect("trigger"),
    );
    let effects = kernel
        .step(&ctx(4), &read(41, P2, 25, &record_at(P2, 3, 3)))
        .expect("a newer generic read");
    assert_eq!(
        shapes(&effects)[2..],
        [Shape::Fact(AuthorityFact::LineageChanged)]
    );
    let seq = kernel.authority_seq();
    let effects = kernel
        .step(
            &ctx(5),
            &read_as(read_back, 40, P2, 20, &record_at(P2, 2, 2)),
        )
        .expect("the recovery's read-back, older, late");
    assert_eq!(adopted_lineages(&effects), vec![], "{effects:?}");
    assert!(
        shapes(&effects).contains(&Shape::Ignored(
            AuthorityIgnoreReason::PartitionReadSuperseded
        )),
        "{effects:?}"
    );
    assert_eq!(kernel.authority_seq(), seq, "no bump");
    assert_eq!(
        kernel.view().served.get(&P2),
        Some(&record_at(P2, 3, 3).lineage()),
        "g3 stays served: no roll-back to g2"
    );
}

/// M7A-177. Lead ruling A-R78, closing the design gap in §2.4's trigger row: its guard is
/// "`r.fenced_prior` names a lineage we do not currently serve", and the table had no row for the
/// complement. The case: a node lost its grant, re-acquired it, and its coherent reload installed
/// the partition record as it then stood — the prior lineage `(g1, e1)` — before F1's activation
/// CAS moved it. Then F1, on this same node, recovers `p1` from that very lineage to `g2`.
///
/// [`serving_p1`] is that state: `Held` through the real acquisition, with the coherent load at
/// revision 10 having installed `(g1, e1)`. `Recovered` then issues the `Get` and **no**
/// `RecoveryObserved` (nothing is observed that A1 did not already serve; nothing widens without
/// the read), and the read-back lands on the install row: `g2` installed, `LineageInstalled`.
///
/// Near miss: serving the same generation at another epoch is not serving the fenced prior, so
/// the fact is emitted.
#[retcd_test]
fn m7a_177_same_node_recovery_after_re_acquire_installs_the_new_generation() {
    let mut kernel = serving_p1(1);
    let before = kernel.view();
    let effects = kernel
        .step(&ctx(3), &recovered(40, P1, (1, 1), 2))
        .expect("the Recovered trigger row is built");
    let read_back = asked(&effects);
    assert_eq!(
        trigger(&effects),
        (vec![(P1, ControlKey::Partition(P1))], vec![]),
        "the Get, and no RecoveryObserved: this node serves the fenced prior (A-R78)"
    );
    assert_eq!(kernel.view(), before, "no rights change at the trigger");

    let effects = kernel
        .step(
            &ctx(4),
            &read_as(read_back, 40, P1, 20, &record_at(P1, 2, 2)),
        )
        .expect("the install row is built");
    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Adopt(P1, OwnerEpoch(2)),
            Shape::Publish(P1),
            Shape::Fact(AuthorityFact::LineageInstalled),
        ]
    );
    let installed = record_at(P1, 2, 2).lineage();
    assert_eq!(adopted_lineages(&effects), vec![(P1, installed)]);
    let view = kernel.view();
    assert_eq!(view.served.get(&P1), Some(&installed), "the new generation");
    assert_eq!(view.authority_seq, before.authority_seq + 1);

    // Near miss: serving g1 at e2 is not serving the fenced prior (g1, e1).
    let mut kernel = serving_p1(2);
    let effects = kernel
        .step(&ctx(3), &recovered(40, P1, (1, 1), 2))
        .expect("trigger");
    assert_eq!(
        trigger(&effects),
        (
            vec![(P1, ControlKey::Partition(P1))],
            vec![Shape::Fact(AuthorityFact::RecoveryObserved)]
        )
    );
}

/// M7A-162. `design.md` §2.4's three emit points (F-R10, K-A-34); lead ledger L-R177gf. One trace:
/// a coherent load of two partitions, a partition-record change, a post-`Recovered` install, five
/// `Watched` deliveries and a committed renewal. `AdoptAuthority` comes out exactly at the three
/// install points — four effects, two from the load — each step that emits one bumps
/// `authority_seq` once, and no other step emits one or moves the seq.
///
/// **The dispatcher clause is modelled, not run.** The row says the dispatcher's `StepCtx`
/// lineage equals the last `AdoptAuthority` per partition at every step. This folds every
/// `AdoptAuthority` into a map the way `rdb-sim`'s `Dispatcher::deliver` fills its `adopted`
/// table, and after every step compares it with A1's `served`: what a `StepCtx` would carry is
/// always what A1 installed.
#[retcd_test]
fn m7a_162_adopt_authority_only_from_lineage_installs() {
    let mut kernel = held_kernel();
    let at = |now: u64| sample_ctx(now, 1_600, 0, 10);
    let mut adopted = std::collections::BTreeMap::new();
    let mut total = 0;
    let mut step = |kernel: &mut Authority, now: u64, event: Event, installs: usize| {
        let seq = kernel.authority_seq();
        let effects = kernel.step(&at(now), &event).expect("a built row");
        let emitted = adopted_lineages(&effects);
        assert_eq!(emitted.len(), installs, "at {now}: {effects:?}");
        let bump = u64::from(installs > 0);
        assert_eq!(
            kernel.authority_seq(),
            seq + bump,
            "at {now}: one bump per install step"
        );
        adopted.extend(emitted);
        assert_eq!(
            adopted,
            kernel.view().served,
            "at {now}: StepCtx lineage == served"
        );
        total += installs;
        effects
    };

    let load = snapshot(2, 10, &[record(P1, NODE, 1), record(P2, NODE, 1)]);
    step(&mut kernel, 1_600, load, 2);
    step(&mut kernel, 1_601, read(3, P1, 15, &record(P1, NODE, 2)), 1);
    let read_back = asked(&step(&mut kernel, 1_602, recovered(4, P2, (1, 1), 2), 0));
    step(
        &mut kernel,
        1_603,
        read_as(read_back, 4, P2, 20, &record_at(P2, 2, 2)),
        1,
    );
    for n in 0..5_u64 {
        let key = if n % 2 == 0 { P1 } else { P2 };
        let watched = ControlEvent::Watched {
            prefix: ControlPrefix::Partitions,
            cursor: WatchCursor {
                revision: Revision(21 + n),
            },
            changes: vec![ControlChange {
                key: ControlKey::Partition(key),
                revision: Revision(21 + n),
            }],
        };
        step(&mut kernel, 1_610 + n, event(10 + n, watched), 0);
    }
    let due = renew_due(&kernel, 30, 1_700);
    step(&mut kernel, 1_700, due, 0);
    assert!(
        kernel.view().renewal.is_some(),
        "fixture: a renewal in flight"
    );
    let commit = renew_answered(&kernel, 30, CasOutcome::Committed(Revision(8)));
    step(&mut kernel, 1_701, commit, 0);
    assert!(
        kernel.view().renewal.is_none(),
        "fixture: the renewal committed"
    );
    assert_eq!(total, 4, "the three install points, four effects");
}

/// M7A-180. Lead ledger L-R177hs; the tester's F4. A `Recovered` read-back is matched by the
/// request id its `Get` was sent as, not by the partition or the correlation.
///
/// Held, serving `p1`; F1 recovers `p2` under correlation 40, and A1 sends the read-back `Get`. A
/// watch then names `p2` in an event under the **same correlation**, and A1 sends a second `Get`
/// for the watch. The watch read's answer — the same record, same partition, same correlation —
/// is the generic changed row (`LineageChanged`), not the install row. The read-back's own answer,
/// echoing its own id, then finds the lineage already served: `LineageUnchanged`, no bump. Red on
/// `HEAD` (547c82c): the watch read's answer took the read-back's place and was `LineageInstalled`.
#[retcd_test]
fn m7a_180_a_watch_read_is_not_the_recovered_read_back() {
    let mut kernel = serving_p1(1);
    let read_back = asked(
        &kernel
            .step(&ctx(3), &recovered(40, P2, (1, 1), 2))
            .expect("trigger"),
    );
    let watched = ControlEvent::Watched {
        prefix: ControlPrefix::Partitions,
        cursor: WatchCursor {
            revision: Revision(20),
        },
        changes: vec![ControlChange {
            key: ControlKey::Partition(P2),
            revision: Revision(20),
        }],
    };
    let watch_read = asked(
        &kernel
            .step(&ctx(4), &event(40, watched))
            .expect("the watch read"),
    );
    assert_ne!(watch_read, read_back, "M7A-180: a fresh id per request");

    let effects = kernel
        .step(
            &ctx(5),
            &read_as(watch_read, 40, P2, 20, &record_at(P2, 2, 2)),
        )
        .expect("the watch read's answer");
    assert_eq!(
        shapes(&effects)[2..],
        [Shape::Fact(AuthorityFact::LineageChanged)],
        "M7A-180: the watch's read is not the recovery's: the generic changed row"
    );
    let seq = kernel.authority_seq();
    let effects = kernel
        .step(
            &ctx(6),
            &read_as(read_back, 40, P2, 20, &record_at(P2, 2, 2)),
        )
        .expect("the read-back's own answer");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::LineageUnchanged)],
        "M7A-180: the read-back finds the lineage served: nothing re-installed"
    );
    assert_eq!(kernel.authority_seq(), seq, "M7A-180: no bump");
}

/// M7A-182. Lead ledger L-R177hs; the tester's F3. A1's request ids stay fresh across a fence: an
/// id minted after a fence is none an earlier request carried, so a late answer to a request from
/// before the fence can never pass for one sent after it.
///
/// `Unheld`, F1 recovers `p2` and A1 sends its read-back `Get`; the real acquisition sends its
/// CAS; a coherent snapshot installs `p1`; a read naming another owner fences `p1`; F1 recovers
/// `p2` again and A1 sends a second read-back. Every `Cas` and `Get` of the run carries a distinct
/// id.
#[retcd_test]
fn m7a_182_request_ids_stay_fresh_across_a_fence() {
    let mut kernel = Authority::new();
    let mut sent = vec![asked(
        &kernel
            .step(&ctx(1), &recovered(40, P2, (1, 1), 2))
            .expect("trigger"),
    )];
    let due = Event {
        id: EventId(1),
        at: Tick(0),
        node: NODE,
        boot: BOOT,
        partition: P1,
        correlation: CorrelationId(1),
        kind: EventKind::Timer(TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: kernel.timer_version(AuthorityTimer::Acquire),
            scheduled_at: Tick(0),
        }),
    };
    kernel
        .step(&ctx(2), &due)
        .expect("the AcquireDue row is built");
    let acquire = in_flight(&kernel);
    sent.push(acquire);
    kernel
        .step(
            &ctx(2),
            &event(
                1,
                ControlEvent::CasResult {
                    request: acquire,
                    key: ControlKey::Grant(NODE),
                    outcome: CasOutcome::Committed(Revision(7)),
                },
            ),
        )
        .expect("the acquisition commit row is built");
    assert!(kernel.state().is_held(), "fixture: Held");
    kernel
        .step(&ctx(3), &snapshot(3, 10, &[record(P1, NODE, 1)]))
        .expect("the coherent load");
    let fenced = kernel
        .step(&ctx(4), &read(4, P1, 16, &record(P1, OTHER, 5)))
        .expect("the owner-moved fence");
    assert!(
        shapes(&fenced).contains(&Shape::Fence(DenyReason::GenerationChanged)),
        "fixture: p1 is fenced: {:?}",
        shapes(&fenced)
    );
    sent.push(asked(
        &kernel
            .step(&ctx(5), &recovered(50, P2, (2, 2), 3))
            .expect("trigger after the fence"),
    ));
    let distinct: std::collections::BTreeSet<ControlRequestId> = sent.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        sent.len(),
        "M7A-182: every request id is fresh, across the fence: {sent:?}"
    );
}

/// M7A-183. Lead ledger L-R177hs; the tester's F4, the same misattribution on the unheld arm. An
/// `Unheld` node answers a partition read with nothing, and forgets a `Recovered` read-back only
/// when the answer echoes that read-back's id (lead ruling A-R78). A watch read's answer on the
/// same partition is not the read-back's, so the read-back stays remembered: once the node
/// acquires, the read-back's own answer lands on the install row. Were the watch answer taken for
/// the read-back, the install would degrade to the generic changed row.
#[retcd_test]
fn m7a_183_an_unheld_watch_answer_does_not_clear_the_recovered_read_back() {
    let mut kernel = Authority::new();
    let read_back = asked(
        &kernel
            .step(&ctx(3), &recovered(40, P1, (1, 1), 2))
            .expect("trigger"),
    );
    let watched = ControlEvent::Watched {
        prefix: ControlPrefix::Partitions,
        cursor: WatchCursor {
            revision: Revision(20),
        },
        changes: vec![ControlChange {
            key: ControlKey::Partition(P1),
            revision: Revision(20),
        }],
    };
    let watch_read = asked(
        &kernel
            .step(&ctx(4), &event(41, watched))
            .expect("the unheld watch read"),
    );
    assert_ne!(watch_read, read_back, "M7A-183: a fresh id per request");
    let effects = kernel
        .step(
            &ctx(5),
            &read_as(watch_read, 41, P1, 20, &record_at(P1, 2, 2)),
        )
        .expect("the watch read's answer, unheld");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::StaleAuthorityView)],
        "M7A-183: unheld, a read answers nothing but that (M7A-195)"
    );

    let _ = acquire(&mut kernel, &ctx(6));
    let effects = kernel
        .step(
            &ctx(7),
            &read_as(read_back, 40, P1, 20, &record_at(P1, 2, 2)),
        )
        .expect("the read-back's own answer, held");
    assert_eq!(
        shapes(&effects),
        vec![
            Shape::Adopt(P1, OwnerEpoch(2)),
            Shape::Publish(P1),
            Shape::Fact(AuthorityFact::LineageInstalled),
        ],
        "M7A-183: the read-back was still remembered: the install row"
    );
}

// ---------------------------------------------------------------------------------------------
// Lead ledger L-R178d/e (inv-a1-restart, gap F-B). A durable revocation outlives the grant it was
// made under and the process that made it: the set lives on the kernel, is recorded in every
// state, and a restarted process gets it back through `EpochRevocationRestored`, which the host
// replays at start, before the first `AcquireDue`.
// ---------------------------------------------------------------------------------------------

/// A1's own peer-event arm: a revocation an earlier process on this node made durable, read back
/// from disk at start.
fn revocation_restored(id: u64, partition: PartitionId, epoch: u64) -> Event {
    event_of(
        id,
        EventKind::Kernel(KernelEvent::Authority(
            AuthorityEvent::EpochRevocationRestored {
                partition,
                epoch: OwnerEpoch(epoch),
            },
        )),
    )
}

/// The revocation set as a sorted list, for an equality assertion.
fn revoked(kernel: &Authority) -> Vec<(PartitionId, OwnerEpoch)> {
    kernel.view().revoked_epochs.into_iter().collect()
}

/// A kernel that acquires at tick 4 and then loads `p1` and `p2`, both ours at epoch 3, from one
/// coherent snapshot at revision 10, tick 5. Returns the snapshot's effect vector.
fn acquire_and_load_p1_p2_at_e3(kernel: &mut Authority) -> Vec<Effect> {
    let _ = acquire(kernel, &ctx(4));
    kernel
        .step(
            &ctx(5),
            &snapshot(5, 10, &[record(P1, NODE, 3), record(P2, NODE, 3)]),
        )
        .expect("the coherent load")
}

/// `p2` at `epoch`, as a caller of `may_admit` names it.
fn p2_at(epoch: u64) -> Lineage {
    Lineage {
        partition: P2,
        ..p1_at(epoch)
    }
}

/// M7A-184. Lead ledger L-R178e: a revocation is recorded in every state. An `Unheld` node that
/// completes a durable revocation keeps it through the acquisition that follows, so the coherent
/// load that installs `(p1, e3)` as ours withholds its adopt and its view, and `may_admit` denies
/// `EpochRevoked`. `p2`, loaded by the same snapshot, admits.
///
/// Red on `HEAD` 56f952d: the completion answered `Ignored(StaleAuthorityView)` and recorded
/// nothing, and `enter_held` started every grant with an empty set, so `(p1, e3)` was adopted and
/// admitted. The fence and the drain proof stay `Held`-only: there is no grant to fence and no
/// view to supersede.
#[retcd_test]
fn m7a_184_a_revocation_persisted_while_unheld_survives_into_held() {
    let mut kernel = Authority::new();
    let effects = kernel
        .step(&ctx(3), &revocation_persisted(3, P1, 3))
        .expect("the completion, unheld");
    let unheld = shapes(&effects);
    assert!(
        !unheld.contains(&Shape::Fence(DenyReason::EpochRevoked))
            && !unheld.contains(&Shape::Fact(AuthorityFact::DrainProof)),
        "M7A-184: no fence and no drain proof without a grant: {unheld:?}"
    );
    assert_eq!(
        revoked(&kernel),
        vec![(P1, OwnerEpoch(3))],
        "M7A-184: recorded while Unheld"
    );

    let effects = acquire_and_load_p1_p2_at_e3(&mut kernel);

    assert_eq!(
        revoked(&kernel),
        vec![(P1, OwnerEpoch(3))],
        "M7A-184: the acquisition kept the set"
    );
    assert_eq!(
        kernel.view().served.get(&P1),
        Some(&lineage(3)),
        "M7A-184 fixture: the load installed (p1, e3) as ours"
    );
    let loaded = shapes(&effects);
    assert!(
        !loaded.contains(&Shape::Adopt(P1, OwnerEpoch(3))) && !loaded.contains(&Shape::Publish(P1)),
        "M7A-184: no adopt and no view for the revoked epoch: {loaded:?}"
    );
    assert!(
        loaded.contains(&Shape::Adopt(P2, OwnerEpoch(3))) && loaded.contains(&Shape::Publish(P2)),
        "M7A-184: p2 is adopted, so the load really ran: {loaded:?}"
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(3), Tick(5), &BUDGETS),
        Verdict::Deny(DenyReason::EpochRevoked)
    );
    assert_eq!(
        kernel.may_admit_at(p2_at(3), Tick(5), &BUDGETS),
        Verdict::Admit
    );
    assert_no_view_outruns_may_admit(&kernel, &effects, 5);
}

/// M7A-185. Lead ledger L-R178e: a restored revocation blocks the install of its epoch. The
/// restore reaches a fresh kernel first, as the host replays it, and answers only
/// `Ignored(NotOurs)`: no fence, no drain proof, no view. The acquisition and the coherent load
/// that follow install `(p1, e3)` as ours, withhold its adopt and its view, and `may_admit` denies
/// it; `p2` admits.
///
/// One-fact twin: the same kernel without the restore adopts and admits `(p1, e3)`, so the
/// denial is the restore's and not the fixture's.
///
/// Red on `HEAD` 56f952d with only the contract variant added: A1 had no arm for it and refused
/// it as `Unavailable`.
#[retcd_test]
fn m7a_185_a_restored_revocation_blocks_the_install_of_its_epoch() {
    let mut kernel = Authority::new();
    let effects = kernel
        .step(&ctx(3), &revocation_restored(3, P1, 3))
        .expect("the restore is answered");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::NotOurs)],
        "M7A-185: a restore records only (M7A-195: and says so)"
    );
    assert_eq!(revoked(&kernel), vec![(P1, OwnerEpoch(3))]);

    let effects = acquire_and_load_p1_p2_at_e3(&mut kernel);

    assert_eq!(
        kernel.view().served.get(&P1),
        Some(&lineage(3)),
        "M7A-185 fixture: the load installed (p1, e3) as ours"
    );
    let loaded = shapes(&effects);
    assert!(
        !loaded.contains(&Shape::Adopt(P1, OwnerEpoch(3))) && !loaded.contains(&Shape::Publish(P1)),
        "M7A-185: no adopt and no view for the restored revocation: {loaded:?}"
    );
    assert!(
        loaded.contains(&Shape::Adopt(P2, OwnerEpoch(3))) && loaded.contains(&Shape::Publish(P2)),
        "M7A-185: p2 is adopted: {loaded:?}"
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(3), Tick(5), &BUDGETS),
        Verdict::Deny(DenyReason::EpochRevoked)
    );
    assert_eq!(
        kernel.may_admit_at(p2_at(3), Tick(5), &BUDGETS),
        Verdict::Admit
    );
    assert_no_view_outruns_may_admit(&kernel, &effects, 5);

    // The twin: no restore.
    let mut twin = Authority::new();
    let effects = acquire_and_load_p1_p2_at_e3(&mut twin);
    assert!(
        shapes(&effects).contains(&Shape::Adopt(P1, OwnerEpoch(3))),
        "M7A-185 twin: without the restore (p1, e3) is adopted"
    );
    assert_eq!(
        twin.may_admit_at(p1_at(3), Tick(5), &BUDGETS),
        Verdict::Admit
    );
}

/// M7A-186. Lead ledger L-R178e: a restored revocation names one `(partition, epoch)` and blocks
/// nothing else. Restores of `(p2, e3)` and of `(p1, e2)` reach a fresh kernel; the load then
/// installs `(p1, e3)`, which is neither, and it is adopted and admits.
///
/// Kills a restore recorded under the event's partition instead of its own field (every event in
/// this file is addressed to `p1`), and one keyed by partition alone.
#[retcd_test]
fn m7a_186_a_restored_revocation_for_another_partition_or_epoch_blocks_nothing_else() {
    let mut kernel = Authority::new();
    for (id, partition, epoch) in [(2, P2, 3), (3, P1, 2)] {
        let effects = kernel
            .step(&ctx(id), &revocation_restored(id, partition, epoch))
            .expect("the restore is answered");
        assert_eq!(
            shapes(&effects),
            vec![Shape::Ignored(AuthorityIgnoreReason::NotOurs)],
            "M7A-186: a restore records only (M7A-195: and says so)"
        );
    }
    assert_eq!(
        revoked(&kernel),
        vec![(P1, OwnerEpoch(2)), (P2, OwnerEpoch(3))],
        "M7A-186: each restore recorded under its own partition and epoch"
    );

    let effects = acquire_and_load_p1_p2_at_e3(&mut kernel);

    let loaded = shapes(&effects);
    assert!(
        loaded.contains(&Shape::Adopt(P1, OwnerEpoch(3))) && loaded.contains(&Shape::Publish(P1)),
        "M7A-186: (p1, e3) was never revoked, so it is adopted: {loaded:?}"
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(3), Tick(5), &BUDGETS),
        Verdict::Admit
    );
    assert!(
        !loaded.contains(&Shape::Adopt(P2, OwnerEpoch(3))),
        "M7A-186 fixture: the (p2, e3) restore is live: {loaded:?}"
    );
    assert_eq!(
        kernel.may_admit_at(p2_at(3), Tick(5), &BUDGETS),
        Verdict::Deny(DenyReason::EpochRevoked)
    );
}

/// M7A-187. A restore is replayed before the first `AcquireDue`, so a conforming host never
/// delivers one to a `Held` kernel. If one arrives anyway, it must not leave the check and the
/// view disagreeing: a restore naming the **served** epoch fences that partition the way a
/// completion does (`Fence{Partition, EpochRevoked}` and its past view, the entry removed), but
/// emits no `DrainProof`, because it completes no request. A restore naming another epoch of a
/// served partition is recorded and fences nothing.
#[retcd_test]
fn m7a_187_a_restore_reaching_a_held_kernel_fences_only_its_served_epoch_and_proves_no_drain() {
    let mut kernel = serving_p1(3);
    let effects = kernel
        .step(&ctx(3), &revocation_restored(3, P1, 2))
        .expect("a restore of another epoch");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::NotOurs)],
        "M7A-187: another epoch fences nothing (M7A-195: and says so)"
    );
    assert_eq!(kernel.view().served.get(&P1), Some(&lineage(3)));

    let effects = kernel
        .step(&ctx(4), &revocation_restored(4, P1, 3))
        .expect("a restore of the served epoch");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Fence(DenyReason::EpochRevoked), Shape::Publish(P1)],
        "M7A-187: the fence and its past view, and no drain proof"
    );
    assert!(!kernel.view().served.contains_key(&P1), "M7A-187: removed");
    assert_eq!(
        revoked(&kernel),
        vec![(P1, OwnerEpoch(2)), (P1, OwnerEpoch(3))]
    );
    assert_eq!(
        kernel.may_admit_at(p1_at(3), Tick(4), &BUDGETS),
        Verdict::Deny(DenyReason::GenerationChanged),
        "M7A-187: p1 is no longer served"
    );
    assert_no_view_outruns_may_admit(&kernel, &effects, 4);
}

/// M7A-188. Lead ruling A-R84 (dev-edges defect D1). An effect is sited at the partition it
/// concerns (`Effect.partition`): A1's view of `p2`, and its fence of `p2`, are delivered to
/// `(node, p2)` even when the event A1 stepped was `p1`'s. Before the fix every A1 effect took
/// `event.partition`, so the install stepped on `p1` handed `p2`'s view to `p1`'s T1 and P1, and a
/// revocation of `p2` fenced `p1`'s consumers with `p2`'s fence. What the fence **is not**: a
/// node fence stays at the event's partition, and a fact is not a delivery and stays there too.
#[retcd_test]
fn m7a_188_a1_sites_a_view_and_a_partition_fence_at_their_partition() {
    let sited = |effects: &[Effect]| -> Vec<(PartitionId, Shape)> {
        effects
            .iter()
            .map(|effect| {
                let shape = shapes(std::slice::from_ref(effect)).remove(0);
                (effect.partition, shape)
            })
            .collect()
    };

    // The install is stepped on p1 and serves p1 and p2: each view lands at its own partition.
    let (mut kernel, install) = serving_p1_p2();
    let views: Vec<(PartitionId, Shape)> = sited(&install)
        .into_iter()
        .filter(|(_, shape)| matches!(shape, Shape::Publish(_)))
        .collect();
    assert_eq!(
        views,
        vec![(P1, Shape::Publish(P1)), (P2, Shape::Publish(P2))],
        "M7A-188: each install view at its own partition: {install:?}"
    );

    // p2's epoch revocation, persisted on an event of p1: the fence and p2's past view at p2.
    let revoked = kernel
        .step(&ctx(3), &revocation_persisted(3, P2, 1))
        .expect("the epoch-revocation fence");
    let fence_and_view: Vec<(PartitionId, Shape)> = sited(&revoked)
        .into_iter()
        .filter(|(_, shape)| matches!(shape, Shape::Fence(_) | Shape::Publish(_)))
        .collect();
    assert_eq!(
        fence_and_view,
        vec![
            (P2, Shape::Fence(DenyReason::EpochRevoked)),
            (P2, Shape::Publish(P2)),
        ],
        "M7A-188: p2's fence and its past view at p2, not at p1: {revoked:?}"
    );
    assert_eq!(
        fences(&revoked),
        vec![(FenceScope::Partition(P2), DenyReason::EpochRevoked)]
    );
    for effect in &revoked {
        if let EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fact(_))) = effect.kind {
            assert_eq!(
                effect.partition, P1,
                "M7A-188: a fact stays where it was stepped"
            );
        }
    }

    // A node fence ends admission on every partition: the fence stays at the event's partition,
    // and each served partition's past view lands at its own.
    let (mut kernel, _) = serving_p1_p2();
    let rebooted = kernel
        .step(
            &ctx(2),
            &event_of(
                9,
                EventKind::Node(NodeLifecycle::Rebooted { boot: BootId(2) }),
            ),
        )
        .expect("the node-fence row is built");
    let fence_and_views: Vec<(PartitionId, Shape)> = sited(&rebooted)
        .into_iter()
        .filter(|(_, shape)| matches!(shape, Shape::Fence(_) | Shape::Publish(_)))
        .collect();
    assert_eq!(
        fence_and_views,
        vec![
            (P1, Shape::Fence(DenyReason::BootMismatch)),
            (P1, Shape::Publish(P1)),
            (P2, Shape::Publish(P2)),
        ],
        "M7A-188: a node fence at the event's partition, each past view at its own: {rebooted:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// M7A-195 (PR #1 review, R1-F012). Lead rulings A-R24 and B-R33: an empty effect vector is never
// an answer, so each deliberate no-op arm of `Authority::step` says which one it is.
// ---------------------------------------------------------------------------------------------

/// `kernel` after `Rebooted` under a new boot: the node fence, so the state is `Fenced`.
fn fenced_kernel() -> Authority {
    let mut kernel = held_kernel();
    kernel
        .step(
            &ctx(2),
            &event_of(
                2,
                EventKind::Node(NodeLifecycle::Rebooted { boot: BootId(2) }),
            ),
        )
        .expect("the node-fence row is built");
    assert!(kernel.state().is_fenced(), "fixture: Fenced");
    kernel
}

/// M7A-195. A `partitions/{id}` read-back reaching a node that holds no grant installs nothing
/// (lead ruling A-R78) and says so: exactly `[Ignored(StaleAuthorityView)]`, the answer
/// `on_partition_read` gives the same read on the same state. Red at 7e262dc: `[]`.
#[retcd_test]
fn m7a_195_a_partition_read_back_without_a_grant_is_ignored() {
    let read_back = ControlEvent::Value {
        request: UNASKED,
        key: ControlKey::Partition(P1),
        outcome: ReadOutcome::Absent { as_of: Revision(4) },
    };
    for (state, mut kernel) in [("Unheld", Authority::new()), ("Fenced", fenced_kernel())] {
        let effects = kernel
            .step(&ctx(3), &event(3, read_back.clone()))
            .expect("the read-back is answered");
        assert_eq!(
            shapes(&effects),
            vec![Shape::Ignored(AuthorityIgnoreReason::StaleAuthorityView)],
            "M7A-195 ({state}): never an empty vector: {effects:?}"
        );
    }
}

/// M7A-195. Any other control event reaching a node that holds no grant — a watch progress mark,
/// a watch termination — moves no right, and says so: exactly `[Ignored(StaleAuthorityView)]`,
/// as `Unheld` and `Fenced` answer every other input they take no action on. Red at 7e262dc: `[]`.
#[retcd_test]
fn m7a_195_a_control_event_without_a_grant_is_ignored() {
    let progress = ControlEvent::WatchProgress {
        prefix: ControlPrefix::Grants,
        revision: Revision(8),
    };
    let terminated = ControlEvent::WatchTerminated {
        prefix: ControlPrefix::Partitions,
        from: Revision(8),
        termination: rdb_core::contracts::control::WatchTermination::NotLeader,
    };
    for (state, make) in [
        ("Unheld", Authority::new as fn() -> Authority),
        ("Fenced", fenced_kernel),
    ] {
        for control in [progress.clone(), terminated.clone()] {
            let mut kernel = make();
            let effects = kernel
                .step(&ctx(3), &event(3, control.clone()))
                .expect("the control event is answered");
            assert_eq!(
                shapes(&effects),
                vec![Shape::Ignored(AuthorityIgnoreReason::StaleAuthorityView)],
                "M7A-195 ({state}, {control:?}): never an empty vector: {effects:?}"
            );
        }
    }
}

/// M7A-195. A held node's `WatchProgress` moves the cursor and nothing else, and says so:
/// exactly `[Ignored(WatchProgressOnly)]` (A-R24; variant approved by Gautam 2026-10-01 for
/// PR #1 R1-F012). Red before the arm changed: `[]`.
#[retcd_test]
fn m7a_195_a_held_watch_progress_moves_only_the_cursor() {
    let mut kernel = held_kernel();
    let before = kernel.view();
    let effects = kernel
        .step(
            &ctx(3),
            &event(
                3,
                ControlEvent::WatchProgress {
                    prefix: ControlPrefix::Partitions,
                    revision: Revision(9),
                },
            ),
        )
        .expect("the watermark is answered");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::WatchProgressOnly)],
        "M7A-195 (Held): never an empty vector: {effects:?}"
    );
    assert_eq!(
        kernel.cursor(ControlPrefix::Partitions),
        Some(Revision(9)),
        "the cursor moved"
    );
    assert!(kernel.state().is_held(), "and the grant stands");
    assert_eq!(
        kernel.view().served,
        before.served,
        "and nothing is served differently"
    );
}

/// M7A-195. A restored revocation that names no epoch this node serves fences nothing (lead
/// ledger L-R178e) and says so: exactly `[Ignored(NotOurs)]` — no right to end, no view to
/// supersede. On a fresh `Unheld` kernel, which is where the host delivers every restore, and on
/// a `Held` one serving `p1` at epoch 3 given `(p1, e2)` and `(p2, e3)`. The pair is still
/// recorded. Red at 7e262dc: `[]` each time.
#[retcd_test]
fn m7a_195_a_restore_naming_no_served_epoch_is_ignored_not_ours() {
    let not_ours = vec![Shape::Ignored(AuthorityIgnoreReason::NotOurs)];
    let mut kernel = Authority::new();
    let effects = kernel
        .step(&ctx(3), &revocation_restored(3, P1, 3))
        .expect("the restore is answered");
    assert_eq!(shapes(&effects), not_ours, "M7A-195 (Unheld): {effects:?}");
    assert_eq!(revoked(&kernel), vec![(P1, OwnerEpoch(3))]);

    let mut kernel = serving_p1(3);
    for (id, partition, epoch) in [(3, P1, 2), (4, P2, 3)] {
        let effects = kernel
            .step(&ctx(id), &revocation_restored(id, partition, epoch))
            .expect("the restore is answered");
        assert_eq!(
            shapes(&effects),
            not_ours,
            "M7A-195 (Held, {partition:?} e{epoch}): {effects:?}"
        );
    }
    assert_eq!(kernel.view().served.get(&P1), Some(&lineage(3)));
    assert_eq!(
        revoked(&kernel),
        vec![(P1, OwnerEpoch(2)), (P2, OwnerEpoch(3))]
    );
}

/// M7A-195 (e). A CAS completion on a key A1 never writes — the cluster schema, a partition
/// record, another node's grant — matches nothing this module asked for, so it moves nothing and
/// says so: exactly `[Ignored(UnmatchedCompletion)]`, in `Unheld`, `Held` and `Fenced` alike
/// (lead ruling on tester finding F-1a). Red before the fix: `[]` in all three.
#[retcd_test]
fn m7a_195_a_cas_of_a_key_a1_never_writes_is_an_unmatched_completion() {
    let foreign = [
        (
            ControlKey::ClusterSchema,
            CasOutcome::Conflict {
                exists: true,
                current: Revision(1),
            },
        ),
        (
            ControlKey::Partition(P1),
            CasOutcome::Committed(Revision(5)),
        ),
        (ControlKey::Grant(OTHER), CasOutcome::Committed(Revision(5))),
    ];
    for (state, make) in [
        ("Unheld", Authority::new as fn() -> Authority),
        ("Held", held_kernel),
        ("Fenced", fenced_kernel),
    ] {
        for (key, outcome) in foreign {
            let mut kernel = make();
            let before = kernel.view();
            let effects = kernel
                .step(
                    &ctx(3),
                    &event(
                        3,
                        ControlEvent::CasResult {
                            request: ControlRequestId(1),
                            key,
                            outcome,
                        },
                    ),
                )
                .expect("the completion is answered");
            assert_eq!(
                shapes(&effects),
                vec![Shape::Ignored(AuthorityIgnoreReason::UnmatchedCompletion)],
                "M7A-195 ({state}, {key:?}): never an empty vector: {effects:?}"
            );
            let after = kernel.view();
            assert_eq!(
                (after.state, after.served),
                (before.state, before.served),
                "M7A-195 ({state}, {key:?}): and nothing moved"
            );
        }
    }
}

/// M7A-195 (f). A watch delivery with no changes issues no read. Held, it moves the cursor and
/// nothing else, as a `WatchProgress` does: exactly `[Ignored(WatchProgressOnly)]`. Not held, the
/// cursor stays and the answer is `[Ignored(StaleAuthorityView)]`, as every other input those
/// states take no action on (lead ruling on tester finding F-1b). Red before the fix: `[]`.
#[retcd_test]
fn m7a_195_an_empty_watch_delivery_is_answered() {
    let empty = ControlEvent::Watched {
        prefix: ControlPrefix::Partitions,
        cursor: WatchCursor {
            revision: Revision(9),
        },
        changes: vec![],
    };

    let mut kernel = held_kernel();
    let before = kernel.view();
    let effects = kernel
        .step(&ctx(3), &event(3, empty.clone()))
        .expect("the delivery is answered");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Ignored(AuthorityIgnoreReason::WatchProgressOnly)],
        "M7A-195 (Held): never an empty vector: {effects:?}"
    );
    assert_eq!(kernel.cursor(ControlPrefix::Partitions), Some(Revision(9)));
    assert!(kernel.state().is_held(), "and the grant stands");
    assert_eq!(kernel.view().served, before.served);

    for (state, mut kernel) in [("Unheld", Authority::new()), ("Fenced", fenced_kernel())] {
        let cursor = kernel.cursor(ControlPrefix::Partitions);
        let effects = kernel
            .step(&ctx(3), &event(3, empty.clone()))
            .expect("the delivery is answered");
        assert_eq!(
            shapes(&effects),
            vec![Shape::Ignored(AuthorityIgnoreReason::StaleAuthorityView)],
            "M7A-195 ({state}): never an empty vector: {effects:?}"
        );
        assert_eq!(
            kernel.cursor(ControlPrefix::Partitions),
            cursor,
            "M7A-195 ({state}): the cursor is a held node's"
        );
    }
}

/// A quiet arm's reason is spent on its own step (PR #1 tester finding F-3). A kernel that
/// answered a watermark through the quiet path equals one that answered a lifecycle event
/// directly with the same `Ignored`, so no reason is left in the state for a later step to emit,
/// and the next step's answer stands alone. Claims no row: it pins `Module::step`'s reset.
#[retcd_test]
fn a_quiet_answer_leaves_nothing_behind_in_the_kernel() {
    let resumed = |id: u64| {
        event_of(
            id,
            EventKind::Node(NodeLifecycle::Resumed {
                suspended_millis: 0,
            }),
        )
    };
    let stale = vec![Shape::Ignored(AuthorityIgnoreReason::StaleAuthorityView)];

    let mut quiet = Authority::new();
    let answered = quiet
        .step(
            &ctx(3),
            &event(
                3,
                ControlEvent::WatchProgress {
                    prefix: ControlPrefix::Grants,
                    revision: Revision(8),
                },
            ),
        )
        .expect("the watermark is answered");
    assert_eq!(shapes(&answered), stale, "the quiet path: {answered:?}");

    let mut direct = Authority::new();
    let answered = direct
        .step(&ctx(3), &resumed(3))
        .expect("the lifecycle event is answered");
    assert_eq!(shapes(&answered), stale, "the direct path: {answered:?}");

    assert_eq!(
        quiet, direct,
        "nothing of the quiet step is left in the kernel"
    );
    let next = quiet
        .step(&ctx(4), &resumed(4))
        .expect("the lifecycle event is answered");
    assert_eq!(
        shapes(&next),
        stale,
        "the next answer stands alone: {next:?}"
    );
}
