//! The takeover side of A1: `Takeover` creation, the three proof routes and the external-fence
//! binding (team kernel-a `design.md` §2.6a, rows T1–T13, as amended by lead ruling A-R51).
//!
//! Not an `M7A-*` row. This is the build's own evidence for §3.4, written against the design
//! table so the behaviour has been seen before the plan rows that will own it are written. Each
//! function names the T-row it reads.
//!
//! # The fixture
//!
//! One node, `NODE`, holds a grant from tick 0 (the real `AcquireDue → Cas → Committed` sequence,
//! lead ruling A-R47). Partition `TAKEN` is owned by `OTHER`, whose frozen grant expired at
//! authority time `F`. The sample is fixed (taken at tick 0, error 10 ms), so with the spec's
//! 100 ms dispatch margin `C_auth > F + epsilon + delta` first holds at tick [`PROOF_TICK`]. Every
//! step stays below the sample's 2 s freshness window unless a row is about staleness.
//!
//! Every event is stepped in partition `P1`'s context and `TAKEN` is a different partition, so a
//! row that checks an effect's partition also checks that proofs and deferrals are routed to the
//! partition taken over, not the one the event arrived on.

use config_log::retcd_test;

use bytes::Bytes;
use rdb_core::authority::clock::ClockMode;
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::authority::{Authority, AuthorityTimer, FrozenGrant, Takeover};
use rdb_core::contracts::authority::{
    AuthorityEffect, AuthorityIgnoreReason, DenyReason, EvidenceRef, ExternalFenceMismatch,
    FencingProof, Revocation,
};
use rdb_core::contracts::control::{
    CasOutcome, ControlEffect, ControlEvent, ControlKey, ControlPrefix, ControlRecord, ReadOutcome,
};
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, Event, EventKind, KernelEffect, Module, StepCtx,
};
use rdb_core::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, CorrelationId, EventId, Generation, GrantId,
    NodeId, OwnerEpoch, PartitionId, Revision, Seq, SnapshotHandle,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::storage::{Namespace, SnapshotRead};
use rdb_core::contracts::time::{ControlTime, Tick, TimerFired};
use rdb_core::contracts::trace::Version;

const NODE: NodeId = NodeId(1);
const OTHER: NodeId = NodeId(2);
const THIRD: NodeId = NodeId(3);
const BOOT: BootId = BootId(1);
/// The partition every event is stepped in.
const P1: PartitionId = PartitionId(1);
/// The partition taken over from `OTHER`.
const TAKEN: PartitionId = PartitionId(2);
/// Another partition of `OTHER`'s.
const SIBLING: PartitionId = PartitionId(3);
const PRIOR_GRANT: GrantId = GrantId(9);
const PRIOR_BOOT: BootId = BootId(4);
const PRIOR_EPOCH: u64 = 3;
/// The revision the fixture's `TAKEN` record is read at, and the one its frozen grant is read at.
const RECORD_REV: u64 = 20;
const FROZEN_REV: u64 = 21;
/// The revision the fixture's **unfrozen** grant is read at: one below [`FROZEN_REV`], so the
/// freeze a T6 row delivers later is strictly newer (lead ruling A-R53.2). One revision cannot
/// hold both an unfrozen and a frozen body; before A-R53 the fixture read both at 21.
const UNFROZEN_REV: u64 = FROZEN_REV - 1;

/// The revision T3 reads the fixture's grant at.
const fn t3_rev(frozen: bool) -> u64 {
    if frozen {
        FROZEN_REV
    } else {
        UNFROZEN_REV
    }
}
/// The authority-clock estimate the fixed sample carries, taken at tick zero.
const ESTIMATE: u64 = 1_000_000;
/// `F`: the prior grant's final expiry, 500 ms of authority time after the sample.
const F: i64 = (ESTIMATE + 500) as i64;
/// The sample's own error. No drift accrues below 2 000 ticks at 500 ppm (integer millis).
const EPSILON: u64 = 10;
/// The first tick at which `ESTIMATE + now > F + EPSILON + dispatch_margin`. Restated rather than
/// computed by the kernel's code, so a kernel that derived it differently fails.
const PROOF_TICK: u64 = 500 + EPSILON + BUDGETS.dispatch_margin_millis + 1;
const EVIDENCE: EvidenceRef = EvidenceRef([7; 32]);

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

/// One fixed, bounded sample taken at tick zero (lead ruling A-R45): no step republishes a view
/// because the sample moved.
fn ctx(now: u64) -> StepCtx<'static> {
    StepCtx {
        now: Tick(now),
        control_time: ControlTime {
            estimate: Tick(ESTIMATE),
            error_millis: EPSILON,
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

fn control(id: u64, correlation: u64, control: ControlEvent) -> Event {
    event(id, correlation, EventKind::Control(control))
}

/// `Held` from tick 0, through the real acquisition (lead ruling A-R47).
fn held() -> Authority {
    let mut kernel = Authority::new();
    let due = event(
        1,
        1,
        EventKind::Timer(TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: kernel.timer_version(AuthorityTimer::Acquire),
            scheduled_at: Tick(0),
        }),
    );
    kernel.step(&ctx(0), &due).expect("AcquireDue is built");
    kernel
        .step(
            &ctx(0),
            &control(
                1,
                1,
                ControlEvent::CasResult {
                    key: ControlKey::Grant(NODE),
                    outcome: CasOutcome::Committed(Revision(7)),
                },
            ),
        )
        .expect("the acquisition commit is built");
    assert!(kernel.state().is_held(), "fixture: Held");
    kernel
}

/// `partitions/{partition}` naming `owner` at `epoch` in `lifecycle`.
fn record(
    partition: PartitionId,
    owner: NodeId,
    epoch: u64,
    lifecycle: PartitionLifecycle,
) -> PartitionRecord {
    PartitionRecord {
        partition,
        owner,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(epoch),
        config_version: ConfigVersion(1),
        lifecycle,
    }
}

/// `TAKEN`'s record, owned by `OTHER` at the prior epoch.
fn taken(lifecycle: PartitionLifecycle) -> PartitionRecord {
    record(TAKEN, OTHER, PRIOR_EPOCH, lifecycle)
}

/// A linearizable read of `partitions/{body.partition}` found `body` at `revision`.
fn partition_read(id: u64, correlation: u64, revision: u64, body: &PartitionRecord) -> Event {
    control(
        id,
        correlation,
        ControlEvent::Value {
            key: ControlKey::Partition(body.partition),
            outcome: ReadOutcome::Found {
                revision: Revision(revision),
                value: body.encode(),
            },
        },
    )
}

/// A coherent `partitions/*` snapshot at `revision`, each record written at `revision`.
fn snapshot(id: u64, revision: u64, bodies: &[PartitionRecord]) -> Event {
    control(
        id,
        id,
        ControlEvent::FamilySnapshot {
            prefix: ControlPrefix::Partitions,
            snapshot_revision: Revision(revision),
            records: bodies
                .iter()
                .map(|body| ControlRecord {
                    key: ControlKey::Partition(body.partition),
                    revision: Revision(revision),
                    value: body.encode(),
                })
                .collect(),
        },
    )
}

/// `OTHER`'s grant record, found at `revision`.
fn prior_grant(frozen: bool, revision: u64) -> ReadOutcome {
    ReadOutcome::Found {
        revision: Revision(revision),
        value: GrantRecord {
            grant: PRIOR_GRANT,
            node: OTHER,
            boot: PRIOR_BOOT,
            authority_generation: AuthorityGeneration::default(),
            expiry_utc_ms: F,
            frozen,
        }
        .encode(),
    }
}

fn grant_read(id: u64, correlation: u64, node: NodeId, outcome: ReadOutcome) -> Event {
    control(
        id,
        correlation,
        ControlEvent::Value {
            key: ControlKey::Grant(node),
            outcome,
        },
    )
}

/// An event that routes to nothing while `Held`, so the step's effects are the sweep's alone.
fn tick(id: u64) -> Event {
    control(
        id,
        id,
        ControlEvent::WatchProgress {
            prefix: ControlPrefix::Grants,
            revision: Revision(1),
        },
    )
}

/// The claim a correct external fence makes about the fixture's entry.
fn claim() -> ExternalClaim {
    ExternalClaim {
        partition: TAKEN,
        prior_generation: Generation(1),
        prior_owner_epoch: OwnerEpoch(PRIOR_EPOCH),
        prior_boot_id: PRIOR_BOOT,
        control_revision: Revision(FROZEN_REV),
    }
}

/// The six bindings of an `ExternalFenceVerified`, less the evidence, so a row can spoil one.
#[derive(Clone, Copy)]
struct ExternalClaim {
    partition: PartitionId,
    prior_generation: Generation,
    prior_owner_epoch: OwnerEpoch,
    prior_boot_id: BootId,
    control_revision: Revision,
}

impl ExternalClaim {
    fn kind(self) -> EventKind {
        EventKind::ExternalFenceVerified {
            partition: self.partition,
            prior_generation: self.prior_generation,
            prior_owner_epoch: self.prior_owner_epoch,
            prior_boot_id: self.prior_boot_id,
            control_revision: self.control_revision,
            evidence: EVIDENCE,
        }
    }
}

/// `Held`, with `TAKEN` read `Fencing` (T1) and a `Takeover` created by the correlated read of
/// `OTHER`'s grant (T3), at ticks 10 and 11 — far below [`PROOF_TICK`].
fn taking_over(frozen: bool) -> Authority {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("T1 is built");
    kernel
        .step(
            &ctx(11),
            &grant_read(11, 10, OTHER, prior_grant(frozen, t3_rev(frozen))),
        )
        .expect("T3 is built");
    assert!(kernel.view().takeover.contains_key(&TAKEN), "fixture: T3");
    kernel
}

/// The fixture's entry as T3 creates it.
fn created(frozen: bool) -> Takeover {
    Takeover {
        prior_node: OTHER,
        prior_generation: Generation(1),
        prior_owner_epoch: OwnerEpoch(PRIOR_EPOCH),
        prior_grant_id: PRIOR_GRANT,
        prior_boot_id: PRIOR_BOOT,
        frozen: frozen.then_some(FrozenGrant {
            expiry_utc_ms: F,
            control_revision: Revision(FROZEN_REV),
        }),
        observed_revision: Revision(t3_rev(frozen)),
        revoked_at: None,
        proven: None,
    }
}

/// The proof the fixture's entry gives by `revocation`, decided at `now`.
fn proof(revocation: Revocation, now: u64) -> FencingProof {
    FencingProof {
        partition: TAKEN,
        prior_generation: Generation(1),
        prior_owner_epoch: OwnerEpoch(PRIOR_EPOCH),
        prior_grant_id: PRIOR_GRANT,
        prior_boot_id: PRIOR_BOOT,
        revocation,
        control_revision: Revision(FROZEN_REV),
        decision_tick: Tick(now),
    }
}

/// `ExpiryProven` as the fixture's sample gives it at `now`.
fn expiry_proven_at(now: u64) -> Revocation {
    Revocation::ExpiryProven {
        frozen_expiry_utc_ms: F,
        authority_utc_ms: (ESTIMATE + now) as i64,
        authority_tick: Tick(now),
        epsilon_ms: EPSILON as u32,
        delta_ms: BUDGETS.dispatch_margin_millis as u32,
    }
}

/// An effect reduced to what these rows assert on, with the partition it is about.
#[derive(Debug, PartialEq, Eq)]
enum Shape {
    Get(ControlKey),
    Ignored(AuthorityIgnoreReason, PartitionId),
    Proven(PartitionId, FencingProof),
    Other,
}

fn shapes(effects: &[Effect]) -> Vec<Shape> {
    effects
        .iter()
        .map(|effect| match &effect.kind {
            EffectKind::Control(ControlEffect::Get { key }) => Shape::Get(*key),
            EffectKind::Kernel(KernelEffect::Ignored {
                reason: KernelIgnoredReason::Authority(reason),
            }) => Shape::Ignored(reason.clone(), effect.partition),
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::FenceProven(proof))) => {
                Shape::Proven(effect.partition, proof.clone())
            }
            _ => Shape::Other,
        })
        .collect()
}

/// The shapes that are not [`Shape::Other`]: what a snapshot step says about takeovers, without
/// the install's own facts and the re-watch.
fn takeover_shapes(effects: &[Effect]) -> Vec<Shape> {
    shapes(effects)
        .into_iter()
        .filter(|shape| *shape != Shape::Other)
        .collect()
}

fn ignored(reason: AuthorityIgnoreReason, partition: PartitionId) -> Shape {
    Shape::Ignored(reason, partition)
}

fn proofs(effects: &[Effect]) -> usize {
    shapes(effects)
        .iter()
        .filter(|shape| matches!(shape, Shape::Proven(..)))
        .count()
}

// ---- the record ----------------------------------------------------------------------------

/// A-R51: `PartitionRecord` carries its lifecycle at `LAYOUT_VERSION` 2, every lifecycle
/// round-trips, and neither a version-1 body nor an unknown lifecycle byte decodes.
#[retcd_test]
fn the_partition_record_round_trips_its_lifecycle_at_layout_version_2() {
    for lifecycle in [
        PartitionLifecycle::Serving,
        PartitionLifecycle::Fencing,
        PartitionLifecycle::FencingDrained,
    ] {
        let body = taken(lifecycle);
        let bytes = body.encode();
        assert_eq!(bytes[0], 2, "the layout version byte");
        assert_eq!(bytes.len(), 34, "version, five fields, lifecycle byte");
        assert_eq!(PartitionRecord::decode(&bytes), Some(body), "{lifecycle:?}");
    }
    let bytes = taken(PartitionLifecycle::Fencing).encode();
    let mut version_one = bytes[..33].to_vec();
    version_one[0] = 1;
    assert_eq!(
        PartitionRecord::decode(&version_one),
        None,
        "no Serving guess"
    );
    let mut unknown = bytes.to_vec();
    unknown[33] = 3;
    assert_eq!(PartitionRecord::decode(&unknown), None, "no lifecycle 3");
}

// ---- T1, T2: creation starts and stops -------------------------------------------------------

/// T1: `Held`, another owner, not `Serving`, no entry ⇒ one `Get(grants/{owner})`. The entry is
/// not created yet: T3 creates it whole (A-R31).
#[retcd_test]
fn t1_a_fencing_record_of_another_owner_reads_its_grant() {
    let mut kernel = held();

    let effects = kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![
            ignored(AuthorityIgnoreReason::NotOurs, P1),
            Shape::Get(ControlKey::Grant(OTHER)),
        ],
        "serving rights first (never ours), then the takeover read"
    );
    assert!(kernel.view().takeover.is_empty(), "no half-built entry");
}

/// A-R51's T1 guard: a `Serving` record of another owner starts nothing.
#[retcd_test]
fn t1_a_serving_record_of_another_owner_starts_nothing() {
    let mut kernel = held();

    let effects = kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, RECORD_REV, &taken(PartitionLifecycle::Serving)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![ignored(AuthorityIgnoreReason::NotOurs, P1)]
    );
    assert!(kernel.view().takeover.is_empty());
}

/// Lead ruling A-R51, the case it was ruled for. A freeze of `OTHER` is node-scoped, so its
/// grant reads frozen for **every** partition it owns. Only the partition the planner moved to
/// `Fencing` may be taken over; its `Serving` sibling gets no entry and no proof, even far past
/// `F`.
#[retcd_test]
fn a_node_scoped_freeze_proves_only_the_partition_being_fenced() {
    let mut kernel = held();
    let effects = kernel
        .step(
            &ctx(10),
            &snapshot(
                10,
                RECORD_REV,
                &[
                    taken(PartitionLifecycle::Fencing),
                    record(SIBLING, OTHER, 5, PartitionLifecycle::Serving),
                ],
            ),
        )
        .expect("built");
    assert_eq!(
        takeover_shapes(&effects),
        vec![Shape::Get(ControlKey::Grant(OTHER))]
    );

    kernel
        .step(
            &ctx(11),
            &grant_read(11, 10, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");
    let effects = kernel.step(&ctx(900), &tick(12)).expect("built");

    assert_eq!(
        kernel.view().takeover.keys().copied().collect::<Vec<_>>(),
        vec![TAKEN]
    );
    assert_eq!(
        shapes(&effects),
        vec![Shape::Proven(TAKEN, proof(expiry_proven_at(900), 900))],
        "one proof, for the fencing partition only"
    );
}

/// T1 over a snapshot: one `Get` per owner, in `PartitionId` order, however many partitions each
/// owner has.
#[retcd_test]
fn t1_a_snapshot_reads_each_owners_grant_once() {
    let mut kernel = held();

    let effects = kernel
        .step(
            &ctx(10),
            &snapshot(
                10,
                RECORD_REV,
                &[
                    taken(PartitionLifecycle::Fencing),
                    record(SIBLING, OTHER, 5, PartitionLifecycle::Fencing),
                    record(PartitionId(4), THIRD, 1, PartitionLifecycle::FencingDrained),
                ],
            ),
        )
        .expect("built");

    assert_eq!(
        takeover_shapes(&effects),
        vec![
            Shape::Get(ControlKey::Grant(OTHER)),
            Shape::Get(ControlKey::Grant(THIRD)),
        ]
    );
}

/// T1: a second, newer read of the same lineage while its grant read is outstanding issues
/// nothing. Newer, so that it passes the A-R52 mark and it is the outstanding read that answers.
#[retcd_test]
fn t1_repeat_with_a_read_outstanding_issues_no_second_get() {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");

    let effects = kernel
        .step(
            &ctx(11),
            &partition_read(11, 11, RECORD_REV + 2, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![ignored(AuthorityIgnoreReason::NotOurs, P1)]
    );
}

/// T1: an entry for another lineage is stale. It is dropped and the new lineage's grant is read.
#[retcd_test]
fn t1_a_new_owner_epoch_replaces_a_stale_entry() {
    let mut kernel = taking_over(true);

    let effects = kernel
        .step(
            &ctx(12),
            &partition_read(
                12,
                12,
                30,
                &record(TAKEN, OTHER, PRIOR_EPOCH + 1, PartitionLifecycle::Fencing),
            ),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![
            ignored(AuthorityIgnoreReason::NotOurs, P1),
            Shape::Get(ControlKey::Grant(OTHER)),
        ]
    );
    assert!(kernel.view().takeover.is_empty(), "the stale entry is gone");
}

/// An older snapshot delivered after a newer one does not restart a takeover the newer one
/// ended: T1 over a snapshot sits behind the install's own freshness guard.
#[retcd_test]
fn an_older_snapshot_does_not_restart_a_takeover() {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &snapshot(10, 30, &[taken(PartitionLifecycle::Serving)]),
        )
        .expect("built");

    let effects = kernel
        .step(
            &ctx(11),
            &snapshot(11, RECORD_REV, &[taken(PartitionLifecycle::Fencing)]),
        )
        .expect("built");

    assert_eq!(takeover_shapes(&effects), Vec::new());
}

/// T2: the record names this node. The takeover is over.
#[retcd_test]
fn t2_the_record_naming_us_drops_the_entry() {
    let mut kernel = taking_over(false);

    kernel
        .step(
            &ctx(12),
            &partition_read(
                12,
                12,
                30,
                &record(TAKEN, NODE, PRIOR_EPOCH + 1, PartitionLifecycle::Serving),
            ),
        )
        .expect("built");

    assert!(kernel.view().takeover.is_empty());
}

/// T2 as amended by A-R51: the record returned to `Serving` drops a non-proven entry.
#[retcd_test]
fn t2_a_record_back_to_serving_drops_an_unproven_entry() {
    let mut kernel = taking_over(true);

    let effects = kernel
        .step(
            &ctx(12),
            &partition_read(12, 12, 30, &taken(PartitionLifecycle::Serving)),
        )
        .expect("built");

    assert!(kernel.view().takeover.is_empty());
    let effects_later = kernel.step(&ctx(900), &tick(13)).expect("built");
    assert_eq!(proofs(&effects) + proofs(&effects_later), 0, "no proof");
}

/// T2 for an absent record: a non-proven entry is dropped, a proven one is kept.
#[retcd_test]
fn t2_an_absent_record_drops_only_an_unproven_entry() {
    let mut unproven = taking_over(true);
    unproven
        .step(&ctx(12), &absent_read(12, 40))
        .expect("built");
    assert!(unproven.view().takeover.is_empty(), "unproven: dropped");

    let mut proven = taking_over(true);
    proven.step(&ctx(900), &tick(12)).expect("built");
    proven.step(&ctx(901), &absent_read(13, 40)).expect("built");
    assert!(
        proven.view().takeover[&TAKEN].proven.is_some(),
        "proven: kept, so the proof is never re-issued"
    );
}

// ---- A-R52: the takeover high-water mark ---------------------------------------------------

/// `partitions/{TAKEN}` read absent as of `as_of`.
fn absent_read(id: u64, as_of: u64) -> Event {
    control(
        id,
        id,
        ControlEvent::Value {
            key: ControlKey::Partition(TAKEN),
            outcome: ReadOutcome::Absent {
                as_of: Revision(as_of),
            },
        },
    )
}

/// After a late read at `RECORD_REV`: the answer to the grant read it would have issued, then a
/// sweep far past the bound. Neither may create an entry or prove. This is A-R52's hazard carried
/// to its end: a node-scoped freeze proving expiry for a partition that is no longer being
/// fenced.
fn assert_no_takeover_follows(kernel: &mut Authority, correlation: u64) {
    let answer = kernel
        .step(
            &ctx(50),
            &grant_read(50, correlation, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");
    let sweep = kernel.step(&ctx(900), &tick(51)).expect("built");
    assert!(kernel.view().takeover.is_empty(), "no entry");
    assert_eq!(proofs(&answer) + proofs(&sweep), 0, "no proof");
}

/// A-R52, the reorder that restarted a takeover: `Serving`@30 drops the entry (T2), and the
/// `Fencing`@20 delivered after it is older, so it starts nothing.
#[retcd_test]
fn a_late_older_fencing_read_after_serving_starts_nothing() {
    let mut kernel = taking_over(true);
    kernel
        .step(
            &ctx(12),
            &partition_read(12, 12, 30, &taken(PartitionLifecycle::Serving)),
        )
        .expect("built");
    assert!(kernel.view().takeover.is_empty(), "fixture: T2 dropped it");

    let late = kernel
        .step(
            &ctx(13),
            &partition_read(13, 13, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");

    assert_eq!(
        shapes(&late),
        vec![ignored(AuthorityIgnoreReason::NotOurs, P1)],
        "no Get: the older read changes nothing takeover-side"
    );
    assert_no_takeover_follows(&mut kernel, 13);
}

/// A-R52 for an absent read: absent as of 30, then `Fencing`@20 delivered late, starts nothing.
#[retcd_test]
fn a_late_older_fencing_read_after_an_absent_read_starts_nothing() {
    let mut kernel = taking_over(true);
    kernel.step(&ctx(12), &absent_read(12, 30)).expect("built");
    assert!(kernel.view().takeover.is_empty(), "fixture: T2 dropped it");

    let late = kernel
        .step(
            &ctx(13),
            &partition_read(13, 13, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");

    assert_eq!(
        shapes(&late),
        vec![ignored(AuthorityIgnoreReason::NotOurs, P1)]
    );
    assert_no_takeover_follows(&mut kernel, 13);
}

/// A-R52: a snapshot at 30 is an observation of **every** partition at 30, listed or not. Here
/// `TAKEN` is unlisted and was never read before, so only the snapshot's mark can refuse the
/// `Fencing`@20 delivered after it.
#[retcd_test]
fn a_late_older_fencing_read_after_a_snapshot_starts_nothing() {
    let mut kernel = held();
    kernel
        .step(&ctx(10), &snapshot(10, 30, &[]))
        .expect("built");

    let late = kernel
        .step(
            &ctx(11),
            &partition_read(11, 11, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");

    assert_eq!(
        shapes(&late),
        vec![ignored(AuthorityIgnoreReason::NotOurs, P1)]
    );
    assert_no_takeover_follows(&mut kernel, 11);
}

/// A-R52 does not freeze the table: a `Fencing` read newer than the `Serving` one starts T1, and
/// its grant read creates the entry.
#[retcd_test]
fn a_newer_fencing_read_after_serving_starts_a_takeover() {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, 30, &taken(PartitionLifecycle::Serving)),
        )
        .expect("built");

    let newer = kernel
        .step(
            &ctx(11),
            &partition_read(11, 11, 40, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");
    assert_eq!(
        shapes(&newer),
        vec![
            ignored(AuthorityIgnoreReason::NotOurs, P1),
            Shape::Get(ControlKey::Grant(OTHER)),
        ]
    );

    kernel
        .step(
            &ctx(12),
            &grant_read(12, 11, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");
    assert_eq!(kernel.view().takeover[&TAKEN], created(true));
}

/// A-R52 for a snapshot that **lists** `p`: a snapshot at 20, delivered after a `Serving` read
/// at 30, is older than `p`'s mark and starts nothing. The install's own guard compares against
/// the last install (none here), not against single reads, so only the mark refuses it.
#[retcd_test]
fn an_older_snapshot_listing_fencing_after_a_newer_serving_read_starts_nothing() {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, 30, &taken(PartitionLifecycle::Serving)),
        )
        .expect("built");

    let late = kernel
        .step(
            &ctx(11),
            &snapshot(11, RECORD_REV, &[taken(PartitionLifecycle::Fencing)]),
        )
        .expect("built");

    assert_eq!(takeover_shapes(&late), Vec::new(), "no Get");
    assert_no_takeover_follows(&mut kernel, 11);
}

/// A-R52 for a snapshot that does **not** list `p`: its absence is an observation at 35, older
/// than the `Fencing` read at 40 that started the entry, so the entry is kept.
#[retcd_test]
fn an_older_snapshot_not_listing_p_keeps_a_newer_entry() {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, 40, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");
    kernel
        .step(
            &ctx(11),
            &grant_read(11, 10, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");
    assert_eq!(kernel.view().takeover[&TAKEN], created(true), "fixture: T3");

    kernel
        .step(&ctx(12), &snapshot(12, 35, &[]))
        .expect("built");

    assert_eq!(kernel.view().takeover[&TAKEN], created(true), "kept");
}

// ---- T3–T6: the prior owner's grant -------------------------------------------------------

/// T3: the correlated grant read creates the entry whole, frozen, and defers — about `TAKEN`.
#[retcd_test]
fn t3_the_correlated_grant_read_creates_the_entry_whole() {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");

    let effects = kernel
        .step(
            &ctx(11),
            &grant_read(11, 10, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![ignored(AuthorityIgnoreReason::TakeoverDeferred, TAKEN)]
    );
    assert_eq!(kernel.view().takeover[&TAKEN], created(true));
}

/// T3 with an unfrozen grant: the entry exists, `frozen` is `None`.
#[retcd_test]
fn t3_an_unfrozen_grant_creates_an_entry_with_no_freeze() {
    let kernel = taking_over(false);
    assert_eq!(kernel.view().takeover[&TAKEN], created(false));
}

/// T3 proving in its own step (M7A-52): the frozen read arrives after the inequality already
/// holds, so the step answers the proof and not the deferral.
#[retcd_test]
fn t3_a_frozen_read_past_the_bound_proves_in_its_own_step() {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");

    let effects = kernel
        .step(
            &ctx(PROOF_TICK),
            &grant_read(11, 10, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Proven(
            TAKEN,
            proof(expiry_proven_at(PROOF_TICK), PROOF_TICK)
        )]
    );
}

/// T4: the prior owner has no grant record. Defer, drop the pending read; the next partitions
/// read retries with a fresh `Get` — the next **newer** one (A-R52). A re-read of the unchanged
/// record carries the same revision, and an equal read changes nothing takeover-side; a newer
/// snapshot retries.
#[retcd_test]
fn t4_an_absent_prior_grant_defers_and_the_next_read_retries() {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");

    let effects = kernel
        .step(
            &ctx(11),
            &grant_read(
                11,
                10,
                OTHER,
                ReadOutcome::Absent {
                    as_of: Revision(21),
                },
            ),
        )
        .expect("built");
    assert_eq!(
        shapes(&effects),
        vec![ignored(AuthorityIgnoreReason::TakeoverDeferred, TAKEN)]
    );
    assert!(kernel.view().takeover.is_empty());

    let equal = kernel
        .step(
            &ctx(12),
            &partition_read(12, 12, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");
    assert_eq!(
        shapes(&equal),
        vec![ignored(AuthorityIgnoreReason::NotOurs, P1)],
        "an equal revision is not newer: no retry"
    );

    let retry = kernel
        .step(
            &ctx(13),
            &snapshot(13, 30, &[taken(PartitionLifecycle::Fencing)]),
        )
        .expect("built");
    assert_eq!(
        takeover_shapes(&retry),
        vec![Shape::Get(ControlKey::Grant(OTHER))],
        "the pending read was dropped, so the newer snapshot issues a new one"
    );
}

/// T5: unavailable ⇒ re-read (ADR-rdb-0007 §3). The pending read moves to the re-read, whose
/// answer creates the entry.
#[retcd_test]
fn t5_an_unavailable_prior_grant_is_read_again() {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("built");

    let effects = kernel
        .step(
            &ctx(11),
            &grant_read(11, 10, OTHER, ReadOutcome::Unavailable),
        )
        .expect("built");
    assert_eq!(shapes(&effects), vec![Shape::Get(ControlKey::Grant(OTHER))]);
    assert!(kernel.view().takeover.is_empty());

    kernel
        .step(
            &ctx(12),
            &grant_read(12, 10, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");
    assert_eq!(kernel.view().takeover[&TAKEN], created(true));
}

/// T6: an uncorrelated read (the grants watch) of the prior owner's grant, newer than the entry's
/// freeze, overwrites it — here the freeze that arrives after creation.
#[retcd_test]
fn t6_an_uncorrelated_read_records_a_later_freeze() {
    let mut kernel = taking_over(false);

    let effects = kernel
        .step(
            &ctx(12),
            &grant_read(12, 12, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![ignored(AuthorityIgnoreReason::TakeoverDeferred, TAKEN)]
    );
    assert_eq!(kernel.view().takeover[&TAKEN], created(true));
}

/// T6's guard: a read no newer than the recorded freeze changes nothing.
#[retcd_test]
fn t6_an_older_uncorrelated_read_does_not_unfreeze() {
    let mut kernel = taking_over(true);

    let effects = kernel
        .step(
            &ctx(12),
            &grant_read(12, 12, OTHER, prior_grant(false, FROZEN_REV - 1)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityView, P1)]
    );
    assert_eq!(kernel.view().takeover[&TAKEN], created(true));
}

// ---- T7, T8: the durable drain -----------------------------------------------------------------

/// T7 on an entry not yet frozen: record `revoked_at` and read the grant (M7A-57, frozen before
/// drained). T6 then records the freeze, and T8 proves in that same step.
#[retcd_test]
fn t7_a_drained_record_reads_the_grant_and_the_freeze_proves_the_drain() {
    let mut kernel = taking_over(false);

    let effects = kernel
        .step(
            &ctx(12),
            &partition_read(12, 12, 25, &taken(PartitionLifecycle::FencingDrained)),
        )
        .expect("built");
    assert_eq!(
        shapes(&effects),
        vec![
            ignored(AuthorityIgnoreReason::NotOurs, P1),
            Shape::Get(ControlKey::Grant(OTHER)),
        ]
    );
    let entry = kernel.view().takeover[&TAKEN].clone();
    assert_eq!(entry.revoked_at, Some(Revision(25)));
    assert_eq!(entry.proven, None, "not frozen, so no drain proof");

    let effects = kernel
        .step(
            &ctx(13),
            &grant_read(13, 13, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Proven(
            TAKEN,
            proof(
                Revocation::DurableDrain {
                    ack_revision: Revision(25)
                },
                13
            )
        )],
        "the deferral is superseded by the proof of the same step"
    );
}

/// T8: frozen and drained ⇒ `DurableDrain`, well before the expiry inequality holds.
#[retcd_test]
fn t8_frozen_and_drained_proves_a_durable_drain() {
    let mut kernel = taking_over(true);

    let effects = kernel
        .step(
            &ctx(12),
            &partition_read(12, 12, 25, &taken(PartitionLifecycle::FencingDrained)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![
            ignored(AuthorityIgnoreReason::NotOurs, P1),
            Shape::Proven(
                TAKEN,
                proof(
                    Revocation::DurableDrain {
                        ack_revision: Revision(25)
                    },
                    12
                )
            ),
        ]
    );
}

/// A drain seen by the read that started the takeover is kept on the pending read, so the entry
/// T3 creates already carries it and proves in that step.
#[retcd_test]
fn a_drain_seen_before_the_entry_exists_is_not_lost() {
    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(
                10,
                10,
                RECORD_REV,
                &taken(PartitionLifecycle::FencingDrained),
            ),
        )
        .expect("built");

    let effects = kernel
        .step(
            &ctx(11),
            &grant_read(11, 10, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![Shape::Proven(
            TAKEN,
            proof(
                Revocation::DurableDrain {
                    ack_revision: Revision(RECORD_REV)
                },
                11
            )
        )]
    );
}

/// T7 repeated on a proven entry answers `TakeoverAlreadyAuthorized`, and no second proof.
#[retcd_test]
fn t7_repeat_on_a_proven_entry_is_already_authorized() {
    let mut kernel = taking_over(true);
    kernel
        .step(
            &ctx(12),
            &partition_read(12, 12, 25, &taken(PartitionLifecycle::FencingDrained)),
        )
        .expect("built");

    let effects = kernel
        .step(
            &ctx(13),
            &partition_read(13, 13, 26, &taken(PartitionLifecycle::FencingDrained)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![
            ignored(AuthorityIgnoreReason::NotOurs, P1),
            ignored(AuthorityIgnoreReason::TakeoverAlreadyAuthorized, TAKEN),
        ]
    );
}

// ---- T9, T10: expiry ----------------------------------------------------------------------

/// T9 at its boundary: one tick short of `F + epsilon + delta` nothing (T10); at it,
/// `ExpiryProven` with every term the inequality used.
#[retcd_test]
fn t9_expiry_is_proven_exactly_past_f_plus_epsilon_plus_delta() {
    let mut kernel = taking_over(true);

    let short = kernel.step(&ctx(PROOF_TICK - 1), &tick(12)).expect("built");
    assert_eq!(
        shapes(&short),
        Vec::new(),
        "T10: the sweep is not an answer"
    );

    let effects = kernel.step(&ctx(PROOF_TICK), &tick(13)).expect("built");
    assert_eq!(
        shapes(&effects),
        vec![Shape::Proven(
            TAKEN,
            proof(expiry_proven_at(PROOF_TICK), PROOF_TICK)
        )]
    );
    assert_eq!(
        kernel.view().takeover[&TAKEN].proven,
        Some(expiry_proven_at(PROOF_TICK))
    );
}

/// At most once (K-A-30): once proven, later steps prove nothing again.
#[retcd_test]
fn a_proven_entry_is_never_proven_again() {
    let mut kernel = taking_over(true);
    kernel.step(&ctx(PROOF_TICK), &tick(12)).expect("built");

    let effects = kernel.step(&ctx(900), &tick(13)).expect("built");

    assert_eq!(shapes(&effects), Vec::new());
}

/// T9's admissibility: an unbounded mode or a stale sample proves nothing, however far past `F`.
/// The fresh twin at the same distance does.
#[retcd_test]
fn t9_an_inadmissible_sample_proves_nothing() {
    let mut unbounded = taking_over(true);
    unbounded.set_clock_mode(ClockMode::Unbounded);
    let effects = unbounded.step(&ctx(900), &tick(12)).expect("built");
    assert_eq!(proofs(&effects), 0, "unbounded mode");

    let stale_tick = BUDGETS.max_sample_age_millis + 1;
    let mut stale = taking_over(true);
    let effects = stale.step(&ctx(stale_tick), &tick(12)).expect("built");
    assert!(stale.state().is_held(), "stale denies, it does not fence");
    assert_eq!(proofs(&effects), 0, "stale sample");

    let mut fresh = taking_over(true);
    let effects = fresh
        .step(&ctx(BUDGETS.max_sample_age_millis), &tick(12))
        .expect("built");
    assert_eq!(proofs(&effects), 1, "the fresh twin");
}

/// T10: an unfrozen entry proves nothing, however far past `F`.
#[retcd_test]
fn t10_an_unfrozen_entry_proves_nothing() {
    let mut kernel = taking_over(false);

    let effects = kernel.step(&ctx(900), &tick(12)).expect("built");

    assert_eq!(shapes(&effects), Vec::new());
}

/// The scope rule: the sweep runs only while `Held`. A fenced node keeps its entry (it lives on
/// the kernel) and proves nothing.
#[retcd_test]
fn a_fenced_node_keeps_its_entry_and_proves_nothing() {
    let mut kernel = taking_over(true);
    kernel
        .step(
            &ctx(12),
            &grant_read(
                12,
                12,
                NODE,
                ReadOutcome::Absent {
                    as_of: Revision(30),
                },
            ),
        )
        .expect("built");
    assert!(kernel.state().is_fenced(), "fixture: our grant is revoked");

    let effects = kernel.step(&ctx(900), &tick(13)).expect("built");

    assert_eq!(proofs(&effects), 0);
    assert_eq!(kernel.view().takeover[&TAKEN], created(true));
}

// ---- T11–T13: the external fence --------------------------------------------------------------

fn external(kernel: &mut Authority, now: u64, claim: ExternalClaim) -> Vec<Shape> {
    let effects = kernel
        .step(&ctx(now), &event(now, now, claim.kind()))
        .expect("ExternalFenceVerified is supported");
    shapes(&effects)
}

fn rejected(mismatch: ExternalFenceMismatch, partition: PartitionId) -> Vec<Shape> {
    vec![ignored(
        AuthorityIgnoreReason::ExternalFenceRejected { mismatch },
        partition,
    )]
}

/// T11: the bindings are checked in the order Partition, NotFrozen, PriorGeneration,
/// PriorOwnerEpoch, PriorBootId, ControlRevision, and the first failure wins. Each step of the
/// ladder spoils every later binding too, so a kernel that checked in another order answers a
/// later mismatch.
#[retcd_test]
fn t11_the_first_failed_binding_in_order_is_the_answer() {
    let wrong = ExternalClaim {
        partition: TAKEN,
        prior_generation: Generation(99),
        prior_owner_epoch: OwnerEpoch(99),
        prior_boot_id: BootId(99),
        control_revision: Revision(99),
    };

    let mut no_entry = held();
    assert_eq!(
        external(&mut no_entry, 12, wrong),
        rejected(ExternalFenceMismatch::Partition, TAKEN)
    );

    let mut unfrozen = taking_over(false);
    assert_eq!(
        external(&mut unfrozen, 12, wrong),
        rejected(ExternalFenceMismatch::NotFrozen, TAKEN)
    );

    let mut kernel = taking_over(true);
    let mut claim = wrong;
    assert_eq!(
        external(&mut kernel, 12, claim),
        rejected(ExternalFenceMismatch::PriorGeneration, TAKEN)
    );
    claim.prior_generation = Generation(1);
    assert_eq!(
        external(&mut kernel, 13, claim),
        rejected(ExternalFenceMismatch::PriorOwnerEpoch, TAKEN)
    );
    claim.prior_owner_epoch = OwnerEpoch(PRIOR_EPOCH);
    assert_eq!(
        external(&mut kernel, 14, claim),
        rejected(ExternalFenceMismatch::PriorBootId, TAKEN)
    );
    claim.prior_boot_id = PRIOR_BOOT;
    assert_eq!(
        external(&mut kernel, 15, claim),
        rejected(ExternalFenceMismatch::ControlRevision, TAKEN)
    );
    assert_eq!(
        kernel.view().takeover[&TAKEN].proven,
        None,
        "no rejection proves"
    );
}

/// T11's `Partition` has two causes and this is the second: an entry exists, for another
/// partition than the claim names.
#[retcd_test]
fn t11_a_claim_for_another_partition_has_no_entry() {
    let mut kernel = taking_over(true);
    let mut claim = claim();
    claim.partition = SIBLING;

    assert_eq!(
        external(&mut kernel, 12, claim),
        rejected(ExternalFenceMismatch::Partition, SIBLING)
    );
}

/// T13: every binding holds and nothing is proven ⇒ `FenceProven(ExternalFence(..))`, built by
/// `Revocation::from_external_fence_verified` from the claim itself.
#[retcd_test]
fn t13_a_binding_claim_proves_an_external_fence() {
    let mut kernel = taking_over(true);

    let shapes = external(&mut kernel, 12, claim());

    let revocation =
        Revocation::from_external_fence_verified(&claim().kind()).expect("the event is the claim");
    assert_eq!(
        shapes,
        vec![Shape::Proven(TAKEN, proof(revocation.clone(), 12))]
    );
    assert_eq!(kernel.view().takeover[&TAKEN].proven, Some(revocation));
}

/// T12: the same claim again, or any route after a proof, is `TakeoverAlreadyAuthorized`.
#[retcd_test]
fn t12_a_claim_after_a_proof_is_already_authorized() {
    let mut kernel = taking_over(true);
    external(&mut kernel, 12, claim());

    assert_eq!(
        external(&mut kernel, 13, claim()),
        vec![ignored(
            AuthorityIgnoreReason::TakeoverAlreadyAuthorized,
            TAKEN
        )]
    );
}

/// The scope rule for T11: while not `Held`, a claim is deferred, not judged.
#[retcd_test]
fn an_external_claim_while_unheld_is_deferred() {
    let mut kernel = Authority::new();

    assert_eq!(
        external(&mut kernel, 12, claim()),
        vec![ignored(AuthorityIgnoreReason::TakeoverDeferred, TAKEN)]
    );
}

// ---- Gate 3, lead ruling A-R53 -----------------------------------------------------------------

/// `OTHER`'s key answering a grant body of `node`'s, found at `revision`.
fn grant_body(
    node: NodeId,
    grant: GrantId,
    boot: BootId,
    frozen: bool,
    revision: u64,
) -> ReadOutcome {
    ReadOutcome::Found {
        revision: Revision(revision),
        value: GrantRecord {
            grant,
            node,
            boot,
            authority_generation: AuthorityGeneration::default(),
            expiry_utc_ms: F - 400,
            frozen,
        }
        .encode(),
    }
}

/// Finding B (A-R53.2): T3 keeps the revision it saw even for an unfrozen grant, and T6 accepts
/// only a read strictly newer than it. A frozen read of an **older** grant (grant 5, boot 3, rev
/// 15), delivered after T3's unfrozen read, is stale: it must not freeze the entry on the
/// superseded grant, and the sweep must not prove expiry on it. Before A-R53 T6 accepted any read
/// while `frozen` was `None`, and proved.
#[retcd_test]
fn t6_a_frozen_read_older_than_the_unfrozen_one_t3_saw_is_stale() {
    let mut kernel = taking_over(false);

    let effects = kernel
        .step(
            &ctx(12),
            &grant_read(
                12,
                12,
                OTHER,
                grant_body(OTHER, GrantId(5), BootId(3), true, 15),
            ),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityView, P1)]
    );
    assert_eq!(kernel.view().takeover[&TAKEN], created(false));
    let sweep = kernel
        .step(&ctx(PROOF_TICK + 500), &tick(13))
        .expect("built");
    assert_eq!(proofs(&sweep), 0, "no expiry proof on a superseded grant");
}

/// Finding C (A-R53.3): a `grants/{OTHER}` body naming another node is not `OTHER`'s grant, at T3
/// and at T6 alike. Refused as `FamilyRejected`, as a partition body naming another partition
/// is: T3 creates no entry, and T6 leaves the entry as it was.
#[retcd_test]
fn a_prior_grant_body_naming_another_node_is_family_rejected() {
    let foreign = || grant_body(THIRD, GrantId(77), BootId(7), true, 30);

    let mut kernel = held();
    kernel
        .step(
            &ctx(10),
            &partition_read(10, 10, RECORD_REV, &taken(PartitionLifecycle::Fencing)),
        )
        .expect("T1 is built");
    let t3 = kernel
        .step(&ctx(11), &grant_read(11, 10, OTHER, foreign()))
        .expect("built");
    assert_eq!(
        shapes(&t3),
        vec![ignored(AuthorityIgnoreReason::FamilyRejected, P1)],
        "T3"
    );
    assert!(kernel.view().takeover.is_empty(), "T3 creates no entry");

    let mut kernel = taking_over(false);
    let t6 = kernel
        .step(&ctx(12), &grant_read(12, 12, OTHER, foreign()))
        .expect("built");
    assert_eq!(
        shapes(&t6),
        vec![ignored(AuthorityIgnoreReason::FamilyRejected, P1)],
        "T6"
    );
    assert_eq!(
        kernel.view().takeover[&TAKEN],
        created(false),
        "T6 refreshes nothing"
    );
}

/// Tester gate 3, mutant G12 (T6's guard at `<=`, missed by every row above). A read at exactly
/// the recorded freeze's revision is the same observation, so it is stale, not a refresh.
#[retcd_test]
fn t6_an_uncorrelated_read_at_the_freeze_revision_is_stale() {
    let mut kernel = taking_over(true);

    let effects = kernel
        .step(
            &ctx(12),
            &grant_read(12, 12, OTHER, prior_grant(true, FROZEN_REV)),
        )
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityView, P1)]
    );
    assert_eq!(kernel.view().takeover[&TAKEN], created(true));
}

// =============================================================================================
// M7A §3.5 — takeover rows (`docs/testing/test-plan-m7-kernel-a.md` §3.5), as re-worded by lead
// ruling A-R51: the proof comes from the end-of-step sweep, a repeat after a proof answers
// `Ignored(TakeoverAlreadyAuthorized)`, and a deferral is `Ignored(TakeoverDeferred)` or nothing.
//
// One row = one test, named by its row id. Every function above this section is the build's own
// T-row evidence, not a row. M7A-58 is a campaign row and is not here.
// =============================================================================================

/// A second, wider sample (error [`WIDE_EPSILON`]), taken at tick [`WIDE_SAMPLED_AT`] on the same
/// clock as the fixture's, so it is consistent with it and not a jump. M7A-52 names `ε 20`; the
/// fixture's own sample carries 10, so these rows re-sample instead of changing the fixture.
const WIDE_EPSILON: u64 = 20;
const WIDE_SAMPLED_AT: u64 = 12;
/// The first tick at which `C_auth > F + WIDE_EPSILON + delta`. No drift accrues below 2 000 ms
/// of age at 500 ppm, so epsilon stays 20 through it.
const WIDE_PROOF_TICK: u64 = 500 + WIDE_EPSILON + BUDGETS.dispatch_margin_millis + 1;

fn wide(now: u64) -> StepCtx<'static> {
    StepCtx {
        control_time: ControlTime {
            estimate: Tick(ESTIMATE + WIDE_SAMPLED_AT),
            error_millis: WIDE_EPSILON,
            bound_established: true,
            sampled_at: Tick(WIDE_SAMPLED_AT),
        },
        ..ctx(now)
    }
}

/// [`taking_over`] with a frozen grant, then the wide sample adopted. Nothing is proven yet.
fn taking_over_on_a_wide_sample() -> Authority {
    let mut kernel = taking_over(true);
    let effects = kernel
        .step(&wide(WIDE_SAMPLED_AT), &tick(WIDE_SAMPLED_AT))
        .expect("built");
    assert_eq!(proofs(&effects), 0, "fixture");
    assert_eq!(
        kernel.clock().sample(),
        Some(wide(WIDE_SAMPLED_AT).control_time),
        "fixture: the wide sample is adopted"
    );
    kernel
}

/// The proofs in `effects`, each with the partition its effect is about.
fn proven(effects: &[Effect]) -> Vec<(PartitionId, FencingProof)> {
    shapes(effects)
        .into_iter()
        .filter_map(|shape| match shape {
            Shape::Proven(partition, proof) => Some((partition, proof)),
            _ => None,
        })
        .collect()
}

/// Every `Fence` in `effects`, as its reason.
fn fence_reasons(effects: &[Effect]) -> Vec<DenyReason> {
    effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fence {
                reason, ..
            })) => Some(*reason),
            _ => None,
        })
        .collect()
}

/// `TAKEN`, owned by `OTHER` at `epoch`, drained.
fn drained(epoch: u64) -> PartitionRecord {
    record(TAKEN, OTHER, epoch, PartitionLifecycle::FencingDrained)
}

/// M7A-51. §2.6 takeover table row `DurableDrain{ack_revision}`; ADR 0007. The entry is frozen
/// and the record is read drained at revision 77: the proof is `DurableDrain{77}`, the frozen
/// read's revision, decided now, and it echoes the entry's prior lineage.
///
/// `FencingProof` carries four `prior_*` fields and the partition, not five `prior_*`: the entry's
/// `prior_node` is not on the proof. The row compares all five against the entry. Twin: M7A-57.
#[retcd_test]
fn m7a_51_takeover_authorized_by_durable_drain() {
    let mut kernel = taking_over(true);
    let entry = kernel.view().takeover[&TAKEN].clone();

    let effects = kernel
        .step(&ctx(12), &partition_read(12, 12, 77, &drained(PRIOR_EPOCH)))
        .expect("built");

    let proofs = proven(&effects);
    assert_eq!(
        proofs,
        vec![(
            TAKEN,
            proof(
                Revocation::DurableDrain {
                    ack_revision: Revision(77)
                },
                12
            )
        )]
    );
    let (_, first) = &proofs[0];
    assert_eq!(
        (
            first.partition,
            first.prior_generation,
            first.prior_owner_epoch,
            first.prior_grant_id,
            first.prior_boot_id,
        ),
        (
            TAKEN,
            entry.prior_generation,
            entry.prior_owner_epoch,
            entry.prior_grant_id,
            entry.prior_boot_id,
        ),
        "the proof echoes the entry it proves"
    );
    assert_eq!(
        first.control_revision,
        entry.frozen.expect("fixture: frozen").control_revision
    );
}

/// The shared input of M7A-52 and M7A-53: the wide sample, then a step at `now`.
fn wide_sweep_at(now: u64) -> (Authority, Vec<Effect>) {
    let mut kernel = taking_over_on_a_wide_sample();
    let effects = kernel.step(&wide(now), &tick(now)).expect("built");
    (kernel, effects)
}

/// M7A-52. §2.6 `ExpiryProven`: a frozen record read (the fixture's T3), and
/// `authority_utc_ms > frozen_expiry + eff_eps + δ`. At `F + 20 + 100 + 1` the proof carries every
/// term the inequality used. Twin: M7A-53.
#[retcd_test]
fn m7a_52_takeover_expiry_proven_requires_linearizable_read_and_margin() {
    let (kernel, effects) = wide_sweep_at(WIDE_PROOF_TICK);
    let revocation = Revocation::ExpiryProven {
        frozen_expiry_utc_ms: F,
        authority_utc_ms: F + 20 + 100 + 1,
        authority_tick: Tick(WIDE_PROOF_TICK),
        epsilon_ms: 20,
        delta_ms: 100,
    };

    assert_eq!(
        shapes(&effects),
        vec![Shape::Proven(
            TAKEN,
            proof(revocation.clone(), WIDE_PROOF_TICK)
        )]
    );
    assert_eq!(kernel.view().takeover[&TAKEN].proven, Some(revocation));
}

/// M7A-53, the twin of M7A-52. One fact: the step is at `F + 20 + 100`, on the margin. No proof,
/// the entry unchanged, still `Held`. The sweep says nothing: §13 Q-6 allows `TakeoverDeferred`
/// or nothing, and the sweep is not an answer to the event (A-R51).
#[retcd_test]
fn m7a_53_takeover_expiry_not_proven_at_margin() {
    let (kernel, effects) = wide_sweep_at(WIDE_PROOF_TICK - 1);

    assert_eq!(shapes(&effects), Vec::new());
    assert_eq!(kernel.view().takeover[&TAKEN], created(true));
    assert!(kernel.state().is_held());
}

/// M7A-54. §2.6 "sample invalid/stale ⇒ no proof indefinitely". Every tick from the first stale
/// one (2001) to 10 000. The sample goes stale first; at tick 2500 an invalid one (no bound)
/// arrives and stays. Zero proofs across the run, although `C_auth` passes `F + ε + δ` from tick
/// 611 on.
///
/// The one fence in the run is the invalid sample's `ClockUnbounded`, which is a fence trigger.
/// None is for staleness: through 2001..2499 the taker stays `Held`. The invalid sample is
/// delivered before 2900, where the stale grant's local window would lapse and fence `Expired`.
#[retcd_test]
fn m7a_54_takeover_expiry_never_proven_on_invalid_or_stale_sample() {
    const INVALID_AT: u64 = 2_500;
    let invalid = |now: u64| StepCtx {
        control_time: ControlTime {
            estimate: Tick(ESTIMATE + now),
            error_millis: EPSILON,
            bound_established: false,
            sampled_at: Tick(now),
        },
        ..ctx(now)
    };
    let mut kernel = taking_over(true);
    let (mut proofs_seen, mut fences) = (0, Vec::new());

    for now in BUDGETS.max_sample_age_millis + 1..=10_000 {
        let step = if now < INVALID_AT {
            ctx(now)
        } else {
            invalid(now)
        };
        let effects = kernel.step(&step, &tick(now)).expect("built");
        proofs_seen += proofs(&effects);
        fences.extend(fence_reasons(&effects));
        if now < INVALID_AT {
            assert!(kernel.state().is_held(), "stale is not a fence: {now}");
        }
    }

    assert_eq!(proofs_seen, 0);
    assert_eq!(fences, vec![DenyReason::ClockUnbounded]);
}

/// M7A-55. §2.6 "at most once per (partition, prior_owner_epoch)"; ADR 0007 "takeover at most
/// once". Re-worded by A-R51: two proving inputs for one entry, the second answering
/// `TakeoverAlreadyAuthorized`. Then a new lineage (epoch 4) is a new entry and gets its own
/// proof. Across the run: exactly one proof for epoch 3 and one for epoch 4.
#[retcd_test]
fn m7a_55_takeover_authorized_at_most_once_per_prior_owner_epoch() {
    let mut kernel = taking_over(true);
    let mut effects = kernel
        .step(&ctx(12), &partition_read(12, 12, 25, &drained(PRIOR_EPOCH)))
        .expect("built");

    let second = kernel
        .step(&ctx(13), &partition_read(13, 13, 26, &drained(PRIOR_EPOCH)))
        .expect("built");
    assert_eq!(
        shapes(&second),
        vec![
            ignored(AuthorityIgnoreReason::NotOurs, P1),
            ignored(AuthorityIgnoreReason::TakeoverAlreadyAuthorized, TAKEN),
        ]
    );
    effects.extend(second);

    let next = PRIOR_EPOCH + 1;
    for (event, now) in [
        (partition_read(14, 14, 30, &drained(next)), 14),
        (grant_read(15, 14, OTHER, prior_grant(true, 31)), 15),
    ] {
        effects.extend(kernel.step(&ctx(now), &event).expect("built"));
    }

    let epochs: Vec<OwnerEpoch> = proven(&effects)
        .into_iter()
        .map(|(_, proof)| proof.prior_owner_epoch)
        .collect();
    assert_eq!(epochs, vec![OwnerEpoch(PRIOR_EPOCH), OwnerEpoch(next)]);
}

/// M7A-56. A-R13 honesty; hard rule 8. Source-level: every `m7a_51`..`m7a_55` function in this
/// file is named `takeover_authorized` or `takeover_expiry`, and none claims `_fenced` or
/// `_proven_ok`. The plan names `tests/authority.rs`; the rows live here.
///
/// All five ids must be found, so the check cannot pass on a file that lost its rows.
#[retcd_test]
fn m7a_56_takeover_test_names_never_claim_fenced_or_proven() {
    let names: Vec<&str> = include_str!("authority_takeover.rs")
        .lines()
        .filter_map(|line| line.strip_prefix("fn "))
        .filter_map(|rest| rest.split('(').next())
        .filter(|name| (51..=55).any(|id| name.starts_with(&format!("m7a_{id}_"))))
        .collect();

    let ids: Vec<&str> = names.iter().map(|name| &name[..6]).collect();
    assert_eq!(
        ids,
        vec!["m7a_51", "m7a_52", "m7a_53", "m7a_54", "m7a_55"],
        "{names:?}"
    );
    for name in names {
        assert!(
            name.contains("takeover_authorized") || name.contains("takeover_expiry"),
            "{name}"
        );
        assert!(
            !name.contains("_fenced") && !name.contains("_proven_ok"),
            "{name}"
        );
    }
}

/// M7A-57, the twin of M7A-51. One fact: the entry's grant read was not frozen. Re-worded by
/// A-R51: a `DurableDrain` proof also needs a frozen read, and it fires on `FencingDrained`. The
/// drained record records the drain and reads the prior owner's grant again; no proof.
///
/// The exact vector also carries `Ignored(NotOurs)` about `P1`: the drained record names `OTHER`,
/// and every event here is stepped in `P1`'s context (the fixture's routing check).
#[retcd_test]
fn m7a_57_takeover_without_frozen_record_is_not_authorized() {
    let mut kernel = taking_over(false);

    let effects = kernel
        .step(&ctx(12), &partition_read(12, 12, 77, &drained(PRIOR_EPOCH)))
        .expect("built");

    assert_eq!(
        shapes(&effects),
        vec![
            ignored(AuthorityIgnoreReason::NotOurs, P1),
            Shape::Get(ControlKey::Grant(OTHER)),
        ]
    );
    assert_eq!(kernel.view().takeover[&TAKEN].proven, None);
}

// ---- Plan rows, §8 of `docs/testing/test-plan-m7-kernel-a.md`: the external fence ------------

/// The fixture's claim with exactly one binding spoiled, and the mismatch that names it.
fn spoiled_claims() -> [(ExternalClaim, ExternalFenceMismatch); 5] {
    let good = claim();
    [
        (
            ExternalClaim {
                partition: SIBLING,
                ..good
            },
            ExternalFenceMismatch::Partition,
        ),
        (
            ExternalClaim {
                prior_generation: Generation(2),
                ..good
            },
            ExternalFenceMismatch::PriorGeneration,
        ),
        (
            ExternalClaim {
                prior_owner_epoch: OwnerEpoch(PRIOR_EPOCH + 1),
                ..good
            },
            ExternalFenceMismatch::PriorOwnerEpoch,
        ),
        (
            ExternalClaim {
                prior_boot_id: BootId(PRIOR_BOOT.0 + 1),
                ..good
            },
            ExternalFenceMismatch::PriorBootId,
        ),
        (
            ExternalClaim {
                control_revision: Revision(FROZEN_REV + 1),
                ..good
            },
            ExternalFenceMismatch::ControlRevision,
        ),
    ]
}

/// M7A-149. K-A-37; `design.md` §2.2 six fields and the §2.6 guard.
///
/// The prior grant read frozen at `FROZEN_REV`, and a claim whose five bindings all match. The
/// proof carries `ExternalFence` with the six fields spelled out here, not rebuilt by the
/// kernel's own constructor, so a constructor that dropped or swapped a field is red. The five
/// bindings are compared; `evidence` rides along as the landed `EvidenceRef`.
#[retcd_test]
fn m7a_149_external_fence_verified_six_fields_takeover_authorized() {
    let mut kernel = taking_over(true);

    let shapes = external(&mut kernel, 12, claim());

    let revocation = Revocation::ExternalFence {
        partition: TAKEN,
        prior_generation: Generation(1),
        prior_owner_epoch: OwnerEpoch(PRIOR_EPOCH),
        prior_boot_id: PRIOR_BOOT,
        control_revision: Revision(FROZEN_REV),
        evidence_ref: EVIDENCE,
    };
    assert_eq!(
        shapes,
        vec![Shape::Proven(TAKEN, proof(revocation.clone(), 12))]
    );
    assert_eq!(kernel.view().takeover[&TAKEN].proven, Some(revocation));
}

/// M7A-150. K-A-37; ADR 0007 "each single-field mismatch produces the rejection fact naming that
/// field".
///
/// M7A-149's claim five times, each with exactly one of the five compared fields changed, each
/// against a fresh kernel. Every run: no proof, one `ExternalFenceRejected` naming the changed
/// field, and the entry stays unproven. Landed spelling: an ignore reason, not a `Fact`.
#[retcd_test]
fn m7a_150_external_fence_single_field_mismatch_rejected_naming_field() {
    for (claim, mismatch) in spoiled_claims() {
        let mut kernel = taking_over(true);

        assert_eq!(
            external(&mut kernel, 12, claim),
            rejected(mismatch, claim.partition),
            "{mismatch:?}"
        );
        assert_eq!(kernel.view().takeover[&TAKEN].proven, None, "{mismatch:?}");
    }
}

/// M7A-151. §2.6 "at most once per (partition, prior_owner_epoch)" for the `ExternalFence`
/// variant; ADR 0007 external-fence row 3.
///
/// M7A-149's claim twice, then a third naming `prior_owner_epoch + 1`: one proof, then
/// `TakeoverAlreadyAuthorized`, then a rejection, because no entry is at the next epoch. The
/// once-only rule is per epoch, not a latch: once the record moves to the next epoch and its
/// frozen grant is read, the same claim for that epoch proves once and only once.
#[retcd_test]
fn m7a_151_external_fence_takeover_authorized_at_most_once_per_prior_owner_epoch() {
    let mut kernel = taking_over(true);
    let next = ExternalClaim {
        prior_owner_epoch: OwnerEpoch(PRIOR_EPOCH + 1),
        control_revision: Revision(31),
        ..claim()
    };
    let mut effects = Vec::new();
    let mut step = |kernel: &mut Authority, now: u64, event: Event| {
        let out = kernel.step(&ctx(now), &event).expect("built");
        effects.extend(out.iter().cloned());
        shapes(&out)
    };

    step(&mut kernel, 12, event(12, 12, claim().kind()));
    assert_eq!(
        step(&mut kernel, 13, event(13, 13, claim().kind())),
        vec![ignored(
            AuthorityIgnoreReason::TakeoverAlreadyAuthorized,
            TAKEN
        )]
    );
    assert_eq!(
        step(&mut kernel, 14, event(14, 14, next.kind())),
        rejected(ExternalFenceMismatch::PriorOwnerEpoch, TAKEN)
    );

    let fencing = record(TAKEN, OTHER, PRIOR_EPOCH + 1, PartitionLifecycle::Fencing);
    step(&mut kernel, 15, partition_read(15, 15, 30, &fencing));
    step(
        &mut kernel,
        16,
        grant_read(16, 15, OTHER, prior_grant(true, 31)),
    );
    step(&mut kernel, 17, event(17, 17, next.kind()));
    step(&mut kernel, 18, event(18, 18, next.kind()));

    let epochs: Vec<OwnerEpoch> = proven(&effects)
        .into_iter()
        .map(|(_, proof)| proof.prior_owner_epoch)
        .collect();
    assert_eq!(
        epochs,
        vec![OwnerEpoch(PRIOR_EPOCH), OwnerEpoch(PRIOR_EPOCH + 1)]
    );
}
