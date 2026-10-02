//! T1 dev scaffolding, driven through `Module::step` (and `Transaction::step_txn` only where a
//! test says why).
//!
//! Two kinds of function live here. An `m7a_NNN_<name>` function is plan row M7A-NNN of
//! `docs/testing/test-plan-m7-kernel-a.md`, named by the row's Name column, and asserts that
//! row's clauses and no other row's. When two rows share a setup, the setup is a helper and each
//! row has its own function. Every other function is dev scaffolding and claims no row. Every test
//! is trace-shaped: a sequence of step events, each followed by the effect vector it produced,
//! asserted whole where the vector is short. Every instance is made live the way production
//! makes it: a `Recovered` naming this node the primary.

use std::collections::BTreeMap;

use bytes::Bytes;
use config_log::retcd_test;

use rdb_core::contracts::authority::{
    AuthorityDecision, AuthorityEvent, AuthorityIgnoreReason, AuthorityView, BlockReason,
    Checkpoint, DenyReason, FenceScope, FencingProof, Lineage, PartitionMode, Revocation, Verdict,
};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{
    Budgets, ClientEvent, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module,
    ReplyEffect, StepCtx,
};
use rdb_core::contracts::ids::{
    AffinityId, AppliedSeq, AuthorityGeneration, BatchId, BootId, ClientId, ConfigVersion,
    CorrelationId, EventId, Generation, GrantId, NodeId, OwnerEpoch, PartitionId, ReplicaRole,
    RequestId, RequestIdentity, Revision, Seq, SnapshotHandle, TenantId,
};
use rdb_core::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::protection::{AdmissionState, ReplicationLag};
use rdb_core::contracts::recovery::{
    CommittedRoot, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap, SelectedLineage,
};
use rdb_core::contracts::storage::{
    Batch, Namespace, SnapshotRead, StorageEvent, StorageFault, StoreEffect,
};
use rdb_core::contracts::time::{ControlTime, Tick};
use rdb_core::contracts::trace::{CapabilityState, Version};
use rdb_core::contracts::txn::{
    scoped_key, Condition, Durability, Mutation, Outcome, TxnRequest, TxnResult, KEY_SCOPE_LEN,
};
use rdb_core::transaction::admission::{MAX_CONDITIONS, MAX_MUTATIONS};
use rdb_core::transaction::dedup::{
    dedup_key, dedup_value, DedupIndex, Retained, RetainedAnswer, SEED_PAGE,
};
use rdb_core::transaction::{
    deny_error, Boundary, DenyContext, FreezeCause, Inflight, Limits, QueueMode, Transaction,
    TxnEffect, TxnEvent, TxnRejection, BATCH_TAG, ID_COUNTER_MAX, RETENTION_CAP_ENTRIES,
};

// The kernels beside T1 in the rows that cross a seam (M7A-144, M7A-146, M7A-147).
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::authority::{Authority, AuthorityTimer};
use rdb_core::contracts::authority::AuthorityEffect;
use rdb_core::contracts::control::{
    CasOutcome, ControlEffect, ControlEvent, ControlKey, ControlPrefix, ControlRecord,
};
use rdb_core::contracts::event::Effect;
use rdb_core::contracts::publication::PubMode;
use rdb_core::contracts::time::{TimerEffect, TimerFired};
use rdb_core::publication::{post_apply_timer, Publication};

// ---------------------------------------------------------------------------------------------
// Fixture: node A primary of partition 1 at generation 7, cut at seq 0; one tenant, one group.
// ---------------------------------------------------------------------------------------------

const PARTITION: PartitionId = PartitionId(1);
const NODE_A: NodeId = NodeId(1);
/// The failover target: not a member of the g7 configuration.
const NODE_B: NodeId = NodeId(4);
const GEN: Generation = Generation(7);
const C1: ConfigVersion = ConfigVersion(1);
const TENANT: TenantId = TenantId(3);
const AFF: AffinityId = AffinityId(9);
const BUDGETS: Budgets = Budgets::SPEC_DEFAULTS;
const CORR: CorrelationId = CorrelationId(42);

/// A snapshot whose `User` versions a test sets, and whose `Dedup` rows a seed scans.
///
/// Also the test-local storage stub (A-R68): [`Snap::apply`] lands a T1 batch the way storage
/// would, so the durable `Dedup` rows a recovering node scans are the ones T1 itself wrote.
#[derive(Default)]
struct Snap {
    versions: BTreeMap<Vec<u8>, Version>,
    dedup: BTreeMap<Vec<u8>, Bytes>,
    at: Seq,
}

impl Snap {
    fn apply(&mut self, batch: &Batch) {
        for write in &batch.writes {
            match (write.ns, &write.value) {
                (Namespace::User, Some(_)) => {
                    self.versions.insert(write.key.to_vec(), batch.seq.0);
                }
                (Namespace::User, None) => {
                    self.versions.remove(write.key.as_ref());
                }
                (Namespace::Dedup, Some(value)) => {
                    self.dedup.insert(write.key.to_vec(), value.clone());
                }
                _ => {}
            }
        }
        self.at = self.at.max(batch.seq);
    }
}

impl SnapshotRead for Snap {
    fn handle(&self) -> SnapshotHandle {
        SnapshotHandle(0)
    }
    fn at(&self) -> Seq {
        self.at
    }
    fn generation(&self) -> Generation {
        GEN
    }
    fn get(&self, _ns: Namespace, key: &[u8]) -> Option<Bytes> {
        self.versions.get(key).map(|_| Bytes::from_static(b"v"))
    }
    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version> {
        (ns == Namespace::User)
            .then(|| self.versions.get(key).copied())
            .flatten()
    }
    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        if ns != Namespace::Dedup {
            return Vec::new();
        }
        self.dedup
            .range(from.to_vec()..)
            .take(limit)
            .map(|(k, v)| (Bytes::from(k.clone()), v.clone()))
            .collect()
    }
}

fn lineage_at(generation: Generation) -> Lineage {
    Lineage {
        partition: PARTITION,
        generation,
        owner_epoch: OwnerEpoch(1),
    }
}

fn lineage() -> Lineage {
    lineage_at(GEN)
}

fn view(generation: Generation, authority_seq: u64, valid_through: u64) -> AuthorityView {
    AuthorityView {
        lineage: lineage_at(generation),
        grant_id: GrantId(2),
        boot_id: BootId(1),
        authority_generation: AuthorityGeneration(1),
        config_version: C1,
        authority_seq,
        valid_through_tick: Tick(valid_through),
        past_horizon: DenyReason::Expired,
    }
}

fn config(primary: NodeId) -> PartitionConfig {
    let member = |copy: u8, node: NodeId, role| Member {
        copy: CopyId(copy),
        node,
        boot: BootId(1),
        role,
    };
    PartitionConfig::new(
        PARTITION,
        C1,
        vec![
            member(1, primary, ReplicaRole::Primary),
            member(2, NodeId(2), ReplicaRole::RegularSecondary),
            member(3, NodeId(3), ReplicaRole::RegularSecondary),
        ],
    )
}

/// A recovery at `generation`, cut at `cutoff`, pinning `primary`, in `mode`.
fn recovered(generation: Generation, cutoff: u64, primary: NodeId, mode: PartitionMode) -> Event {
    let cutoff = Seq(cutoff);
    let result = RecoveryResult {
        fenced_prior: FencingProof {
            partition: PARTITION,
            prior_generation: Generation(generation.0 - 1),
            prior_owner_epoch: OwnerEpoch(1),
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
            root: lineage_at(Generation(generation.0 - 1)),
            cutoff_seq: cutoff,
            cutoff_digest: Digest::ROOT,
            source: CopyId(1),
        },
        new_generation: generation,
        mode,
        barrier: RecoveryBarrier::try_new(&[], &Default::default(), cutoff, Digest::ROOT)
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
            authority_view: view(generation, 1, u64::MAX),
            pinned_config: config(primary),
        },
        retained_status_map: RetainedStatusMap {
            predecessor_generation: Generation(generation.0 - 1),
            predecessor_cutoff: cutoff,
            retained_through: cutoff,
            discarded_from: None,
            uncertain: false,
        },
    };
    kernel(KernelEvent::Recovered(Box::new(result)))
}

fn admission(allow: bool, reason: Option<ErrorKind>) -> Event {
    kernel(KernelEvent::SetAdmission(AdmissionState {
        allow,
        reason,
        oldest_unsafe_age: 0,
        oldest_unsafe_seq: Seq::ZERO,
        replication_lag: ReplicationLag::ZERO,
        stalest_copy: None,
        lost_copies: Vec::new(),
        paused_prefix: Seq(4),
        resume_barrier: Seq::ZERO,
        required_config_versions: vec![C1],
        outstanding_unsafe_bytes: 0,
    }))
}

fn kernel(event: KernelEvent) -> Event {
    Event {
        id: EventId(0),
        at: Tick::ZERO,
        node: NODE_A,
        boot: BootId(1),
        partition: PARTITION,
        correlation: CORR,
        kind: EventKind::Kernel(event),
    }
}

fn key(user: &[u8]) -> Bytes {
    scoped_key(TENANT, AFF, user)
}

fn identity(request: u64) -> RequestIdentity {
    RequestIdentity {
        tenant: TENANT,
        client: ClientId(5),
        request: RequestId(request),
    }
}

/// A valid one-`Put` request for `request`, writing `value` at `k`.
fn put(request: u64, k: &[u8], value: &'static [u8]) -> TxnRequest {
    TxnRequest {
        api_version: 1,
        identity: identity(request),
        affinity: AFF,
        expected_generation: None,
        remaining_millis: 1_000,
        conditions: Vec::new(),
        mutations: vec![Mutation::Put {
            key: key(k),
            value: Bytes::from_static(value),
            expected_version: None,
        }],
    }
}

fn submit(req: TxnRequest) -> Event {
    Event {
        kind: EventKind::Client(ClientEvent::Submit(req)),
        ..kernel(KernelEvent::DedupTrim {
            generation: GEN,
            below: Seq::ZERO,
        })
    }
}

fn answer(correlation: CorrelationId, authority_seq: u64, verdict: Verdict) -> Event {
    kernel(KernelEvent::Authority(AuthorityEvent::Answer(decision(
        correlation,
        authority_seq,
        verdict,
    ))))
}

fn decision(correlation: CorrelationId, authority_seq: u64, verdict: Verdict) -> AuthorityDecision {
    AuthorityDecision {
        owner: NODE_A,
        boot: BootId(1),
        grant: GrantId(2),
        authority_generation: AuthorityGeneration(1),
        lineage: lineage(),
        expiry_utc_ms: 0,
        decided_at: Tick::ZERO,
        authority_seq,
        checkpoint: Checkpoint::StorageDispatch,
        correlation,
        verdict,
    }
}

fn storage(event: StorageEvent) -> Event {
    Event {
        kind: EventKind::Storage(event),
        ..kernel(KernelEvent::RetireGeneration {
            generation: Generation(0),
        })
    }
}

fn committed(batch: BatchId, seq: u64) -> Event {
    storage(StorageEvent::Committed {
        batch,
        applied: AppliedSeq(seq),
    })
}

fn published(seq: u64, record_digest: Digest, request: u64) -> Event {
    kernel(KernelEvent::Published {
        lineage: lineage(),
        seq: Seq(seq),
        record_digest,
        request: identity(request),
    })
}

fn fence(scope: FenceScope, reason: DenyReason) -> Event {
    kernel(KernelEvent::Authority(AuthorityEvent::Fence {
        scope,
        reason,
    }))
}

/// `node`'s step context at `now`. A free function, so a test can hold it beside `&mut h.t1`.
fn ctx(now: u64, node: NodeId, snap: &Snap) -> StepCtx<'_> {
    StepCtx {
        now: Tick(now),
        control_time: ControlTime {
            estimate: Tick(now),
            error_millis: 0,
            bound_established: true,
            sampled_at: Tick(now),
        },
        node,
        boot: BootId(1),
        partition: PARTITION,
        generation: GEN,
        owner_epoch: OwnerEpoch(1),
        config_version: C1,
        snapshot: snap,
        budgets: &BUDGETS,
    }
}

/// One T1 on node A, with a snapshot the test can move.
struct H {
    t1: Transaction,
    snap: Snap,
    now: u64,
    /// The node every step is taken on: its `StepCtx.node` and its event's `node`.
    node: NodeId,
    /// That node's boot: its `StepCtx.boot` and its event's `boot`.
    boot: BootId,
}

impl H {
    /// Live at generation 7, cut at 0, L1 allowing: the state every happy trace starts from.
    fn live() -> Self {
        Self::live_with(Limits::default())
    }

    fn live_with(limits: Limits) -> Self {
        let mut h = Self {
            t1: Transaction::with_limits(limits),
            snap: Snap::default(),
            now: 10,
            node: NODE_A,
            boot: BootId(1),
        };
        assert_eq!(
            h.step(recovered(GEN, 0, NODE_A, PartitionMode::Active)),
            vec![]
        );
        assert_eq!(h.step(admission(true, None)), vec![]);
        h
    }

    /// One step on `self.node`. A `Store Commit` T1 emits is landed in the snapshot straight
    /// away, standing in for storage.
    fn try_step(&mut self, mut event: Event) -> Result<Vec<EffectKind>, RdbError> {
        event.node = self.node;
        event.boot = self.boot;
        let ctx = StepCtx {
            boot: self.boot,
            ..ctx(self.now, self.node, &self.snap)
        };
        let effects = self.t1.step(&ctx, &event)?;
        let kinds: Vec<EffectKind> = effects
            .into_iter()
            .map(|effect| {
                assert_eq!(effect.correlation, event.correlation);
                assert_eq!(effect.partition, event.partition);
                effect.kind
            })
            .collect();
        for kind in &kinds {
            if let EffectKind::Store(StoreEffect::Commit(batch)) = kind {
                self.snap.apply(batch);
            }
        }
        Ok(kinds)
    }

    fn step(&mut self, event: Event) -> Vec<EffectKind> {
        self.try_step(event).expect("a T1 input is never declined")
    }

    fn k(&self) -> &rdb_core::transaction::TxnKernel {
        self.t1
            .kernel(self.node, PARTITION)
            .expect("the stepping node is live")
    }

    /// Submit `req`, expect exactly the dispatch check, and return its correlation.
    fn admit(&mut self, req: TxnRequest) -> CorrelationId {
        let effects = self.step(submit(req));
        let [EffectKind::Kernel(KernelEffect::AuthorityCheck {
            checkpoint: Checkpoint::StorageDispatch,
            lineage: l,
            correlation,
        })] = effects.as_slice()
        else {
            panic!("expected exactly one StorageDispatch check, got {effects:?}");
        };
        assert_eq!(*l, self.k().lineage());
        *correlation
    }

    /// Admit, answer `Admit`, and return the batch and its record digest.
    fn dispatch(&mut self, req: TxnRequest) -> (BatchId, Digest, u64) {
        let seq = self.k().next_seq().0;
        let correlation = self.admit(req);
        let effects = self.step(answer(correlation, 1, Verdict::Admit));
        let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
            panic!("expected exactly one batch, got {effects:?}");
        };
        assert_eq!(batch.seq, Seq(seq));
        let Some(Inflight::Dispatched { record_digest, .. }) = self.k().inflight() else {
            panic!("expected Dispatched");
        };
        (batch.id, *record_digest, seq)
    }

    /// Dispatch, complete, publish: one fully resolved transaction.
    fn resolve(&mut self, req: TxnRequest) -> (Digest, u64) {
        let request = req.identity.request.0;
        let (batch, digest, seq) = self.dispatch(req);
        assert_eq!(self.step(committed(batch, seq)).len(), 2);
        assert_eq!(self.step(published(seq, digest, request)), vec![]);
        (digest, seq)
    }
}

/// The error of the one `Failed` reply in `effects`.
fn failed(effects: &[EffectKind]) -> ErrorKind {
    let [EffectKind::Reply(ReplyEffect::Failed { error, .. })] = effects else {
        panic!("expected exactly one Failed reply, got {effects:?}");
    };
    error.kind()
}

fn ignored(reason: AuthorityIgnoreReason) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Ignored {
        reason: KernelIgnoredReason::Authority(reason),
    })
}

/// `effects` is exactly a stale answer followed by the re-asked `StorageDispatch` check for
/// generation 7 (A-R69 via A-R70); returns the fresh correlation.
fn reasked(effects: &[EffectKind]) -> CorrelationId {
    let [stale, EffectKind::Kernel(KernelEffect::AuthorityCheck {
        checkpoint: Checkpoint::StorageDispatch,
        lineage: l,
        correlation,
    })] = effects
    else {
        panic!("expected a stale answer and a re-asked check, got {effects:?}");
    };
    assert_eq!(*stale, ignored(AuthorityIgnoreReason::StaleAuthorityAnswer));
    assert_eq!(*l, lineage());
    *correlation
}

// ---------------------------------------------------------------------------------------------
// Instance lifetime
// ---------------------------------------------------------------------------------------------

#[retcd_test]
fn an_inert_node_runs_checks_one_to_three_before_not_primary() {
    let mut h = H {
        t1: Transaction::new(),
        snap: Snap::default(),
        now: 10,
        node: NODE_A,
        boot: BootId(1),
    };
    assert_eq!(h.t1.capability(), CapabilityState::Unavailable);
    let mut stale = put(1, b"k", b"v");
    stale.api_version = 9;
    assert_eq!(
        failed(&h.step(submit(stale))),
        ErrorKind::IncompatibleVersion
    );
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"v")))),
        ErrorKind::NotPrimary
    );
    assert_eq!(
        h.step(admission(true, None)),
        vec![EffectKind::Kernel(KernelEffect::Ignored {
            reason: KernelIgnoredReason::Error(ErrorKind::NotPrimary)
        })]
    );
    // Not T1's: declined, not answered.
    assert!(h
        .try_step(kernel(KernelEvent::LocalApplied {
            seq: Seq(1),
            bytes: 1,
            record_digest: Digest::ROOT
        }))
        .is_err());
}

#[retcd_test]
fn recovered_elsewhere_demotes_and_strands_the_queue_with_the_new_primary() {
    let mut h = H::live();
    let _ = h.admit(put(1, b"a", b"1"));
    let effects = h.step(recovered(
        Generation(8),
        0,
        NodeId(99),
        PartitionMode::Active,
    ));
    let [EffectKind::Reply(ReplyEffect::Failed {
        identity: who,
        error: RdbError::NotPrimary { hint, .. },
    })] = effects.as_slice()
    else {
        panic!("expected the awaiting request answered NOT_PRIMARY, got {effects:?}");
    };
    assert_eq!((*who, *hint), (identity(1), Some(NodeId(99))));
    assert!(h.t1.kernel(NODE_A, PARTITION).is_none());
}

#[retcd_test]
fn a_recovered_no_newer_than_the_held_lineage_is_ignored() {
    let mut h = H::live();
    assert_eq!(
        h.step(recovered(GEN, 5, NODE_A, PartitionMode::Active)),
        vec![EffectKind::Kernel(KernelEffect::Ignored {
            reason: KernelIgnoredReason::Replica(ReplicaIgnoreReason::NotRequired)
        })]
    );
    assert_eq!(h.k().next_seq(), Seq(1), "the cut did not move");
}

// ---------------------------------------------------------------------------------------------
// The happy trace, end to end through the carriers
// ---------------------------------------------------------------------------------------------

#[retcd_test]
fn submit_check_batch_apply_publish_then_retry_replays() {
    let mut h = H::live();
    let before = h.k().prev_digest();

    // Submit -> [AuthorityCheck{StorageDispatch}]; nothing reserved yet.
    let correlation = h.admit(put(1, b"k", b"v"));
    assert_eq!(correlation.0 & BATCH_TAG, BATCH_TAG, "T1's correlation tag");
    assert_eq!(h.k().next_seq(), Seq(1));

    // Answer(Admit) -> [Store(Commit)]; the reservation commits here, not at completion.
    let effects = h.step(answer(correlation, 1, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("{effects:?}");
    };
    assert_eq!((batch.seq, batch.generation), (Seq(1), GEN));
    let namespaces: Vec<Namespace> = batch.writes.iter().map(|w| w.ns).collect();
    assert_eq!(
        namespaces,
        vec![
            Namespace::User,
            Namespace::Dedup,
            Namespace::History,
            Namespace::Progress
        ]
    );
    assert_eq!(h.k().next_seq(), Seq(2));
    assert_ne!(h.k().prev_digest(), before);
    let digest = h.k().prev_digest();

    // Committed -> [LocalApplied, AppliedCandidate]; no reply (T1 never answers success).
    let effects = h.step(committed(batch.id, 1));
    let [EffectKind::Kernel(KernelEffect::LocalApplied {
        seq, record_digest, ..
    }), EffectKind::Kernel(KernelEffect::AppliedCandidate(candidate))] = effects.as_slice()
    else {
        panic!("{effects:?}");
    };
    assert_eq!((*seq, *record_digest), (Seq(1), digest));
    assert_eq!(
        (
            candidate.seq,
            candidate.prev_digest,
            candidate.record_digest
        ),
        (Seq(1), before, digest)
    );
    assert_eq!(
        h.k().mode(),
        &QueueMode::Frozen {
            cause: FreezeCause::UnresolvedTransaction,
            unresolved: Some(Seq(1))
        }
    );

    // While unresolved, the queue is frozen.
    assert_eq!(
        failed(&h.step(submit(put(2, b"k", b"w")))),
        ErrorKind::ProtectionPaused
    );

    // Published -> [] and the queue reopens; the identity is retained.
    assert_eq!(h.step(published(1, digest, 1)), vec![]);
    assert_eq!(h.k().mode(), &QueueMode::Open);
    assert_eq!(h.k().dedup().len(), 1);

    // Same identity, same payload, new deadline: the published result, verbatim, no seq.
    let mut retry = put(1, b"k", b"v");
    retry.remaining_millis = 7;
    assert_eq!(
        h.step(submit(retry)),
        vec![EffectKind::Reply(ReplyEffect::Transaction {
            identity: identity(1),
            result: TxnResult {
                partition: PARTITION,
                owner_epoch: OwnerEpoch(1),
                generation: GEN,
                seq: Seq(1),
                outcome: Outcome::Published,
                durability: Durability::BufferedOnTwo,
            }
        })]
    );
    // Same identity, another payload.
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"other")))),
        ErrorKind::RequestIdReuse
    );
    assert_eq!(h.k().next_seq(), Seq(2), "neither retry reserved anything");
}

// ---------------------------------------------------------------------------------------------
// Dedup and the request digest (plan §4.2)
// ---------------------------------------------------------------------------------------------

/// R(1) = `put(1, "k", "v")` submitted, dispatched, applied and published, so its identity is
/// retained as `Applied`. Returns the harness and the retained result.
fn retained_after_publish() -> (H, TxnResult) {
    let mut h = H::live();
    let (_, seq) = h.resolve(put(1, b"k", b"v"));
    let Some(Retained {
        answer: RetainedAnswer::Applied(result),
        ..
    }) = h.k().dedup().get(GEN, AFF, identity(1))
    else {
        panic!("expected R(1) retained as Applied");
    };
    assert_eq!(result.seq, Seq(seq));
    let result = *result;
    (h, result)
}

/// M7A-72. The same identity and payload, with a new deadline, is answered with the retained
/// result verbatim. That reply is the step's only effect, so there is no batch and no `Check`,
/// and nothing moves.
#[retcd_test]
fn m7a_72_dedup_hit_same_digest_replays_verbatim_no_seq() {
    let (mut h, retained) = retained_after_publish();
    let before = (h.k().next_seq(), h.k().prev_digest());
    let mut retry = put(1, b"k", b"v");
    retry.remaining_millis = 7;
    assert_eq!(
        h.step(submit(retry)),
        vec![EffectKind::Reply(ReplyEffect::Transaction {
            identity: identity(1),
            result: retained,
        })]
    );
    assert_eq!((h.k().next_seq(), h.k().prev_digest()), before);
    assert_eq!(h.k().inflight(), None);
}

/// M7A-73, M7A-72's twin (one fact: one mutation's value). `REQUEST_ID_REUSE`, nothing
/// reserved, and the retained answer is still the original one.
#[retcd_test]
fn m7a_73_dedup_hit_different_digest_request_id_reuse() {
    let (mut h, retained) = retained_after_publish();
    let before = (h.k().next_seq(), h.k().prev_digest());
    assert_eq!(
        h.step(submit(put(1, b"k", b"w"))),
        fail(
            1,
            RdbError::RequestIdReuse {
                identity: identity(1)
            }
        )
    );
    assert_eq!((h.k().next_seq(), h.k().prev_digest()), before);
    assert_eq!(
        h.k().dedup().get(GEN, AFF, identity(1)).map(|r| &r.answer),
        Some(&RetainedAnswer::Applied(retained))
    );
}

/// M7A-74. What a legitimate retry may change leaves the digest alone: the remaining deadline,
/// and the fields that are not the payload (`client_id` and `request_id` are the key the digest
/// is compared under, and `expected_generation` is an admission check). `TxnRequest` carries no
/// transport metadata, so there is none to vary. T1 retains exactly this digest.
#[retcd_test]
fn m7a_74_digest_same_request_two_deadlines_same_digest() {
    let base = put(1, b"k", b"v");
    let digest = base.request_digest();
    let mut later = base.clone();
    later.remaining_millis = 7;
    let mut client = base.clone();
    client.identity.client = ClientId(99);
    let mut request = base.clone();
    request.identity.request = RequestId(99);
    let mut pinned = base.clone();
    pinned.expected_generation = Some(GEN);
    let mut stale = base.clone();
    stale.expected_generation = Some(Generation(6));
    for (field, varied) in [
        ("deadline", later),
        ("client_id", client),
        ("request_id", request),
        ("expected_generation", pinned),
        ("stale expected_generation", stale),
    ] {
        assert_eq!(varied.request_digest(), digest, "{field}");
    }
    let (h, _) = retained_after_publish();
    assert_eq!(
        h.k()
            .dedup()
            .get(GEN, AFF, identity(1))
            .map(|r| r.request_digest),
        Some(digest)
    );
}

/// M7A-75, five one-fact twins of M7A-74. Each field in the A-R18 preimage, varied alone, gives
/// a digest unlike the base and unlike every other variation.
#[retcd_test]
fn m7a_75_digest_changes_on_each_included_field() {
    let base = put(1, b"k", b"v");
    let mut tenant = base.clone();
    tenant.identity.tenant = TenantId(4);
    let mut affinity = base.clone();
    affinity.affinity = AffinityId(10);
    let mut api = base.clone();
    api.api_version += 1;
    let mut conditions = base.clone();
    conditions
        .conditions
        .push(Condition::Absent { key: key(b"x") });
    let mut mutations = base.clone();
    mutations.mutations = vec![Mutation::Put {
        key: key(b"k"),
        value: Bytes::from_static(b"w"),
        expected_version: None,
    }];
    let digests = [
        ("tenant", tenant.request_digest()),
        ("affinity_id", affinity.request_digest()),
        ("api_version", api.request_digest()),
        ("conditions", conditions.request_digest()),
        ("mutations", mutations.request_digest()),
    ];
    for (i, (field, digest)) in digests.iter().enumerate() {
        assert_ne!(*digest, base.request_digest(), "{field}");
        for (other, theirs) in &digests[i + 1..] {
            assert_ne!(digest, theirs, "{field} vs {other}");
        }
    }
}

/// M7A-76. Check 3 runs before the dedup lookup (step 11). A fresh identity whose mutations name
/// two groups is `CROSS_AFFINITY` and leaves no entry. A retained identity resubmitted with a
/// crossing key is `CROSS_AFFINITY` too, not `REQUEST_ID_REUSE`. The twin, one fact apart (the
/// added key is in the request's own group): `REQUEST_ID_REUSE`.
#[retcd_test]
fn m7a_76_cross_affinity_rejects_before_dedup() {
    let crossing = |mut req: TxnRequest| {
        req.mutations.push(Mutation::Delete {
            key: scoped_key(TENANT, AffinityId(10), b"x"),
            expected_version: None,
        });
        req
    };
    let mut h = H::live();
    assert_eq!(
        h.step(submit(crossing(put(2, b"k", b"v")))),
        fail(
            2,
            RdbError::CrossAffinity {
                expected: AFF,
                found: AffinityId(10)
            }
        )
    );
    assert!(h.k().dedup().is_empty(), "no dedup entry");

    let (mut h, _) = retained_after_publish();
    assert_eq!(
        failed(&h.step(submit(crossing(put(1, b"k", b"v"))))),
        ErrorKind::CrossAffinity
    );
    let mut own_group = put(1, b"k", b"v");
    own_group.mutations.push(Mutation::Delete {
        key: key(b"x"),
        expected_version: None,
    });
    assert_eq!(
        failed(&h.step(submit(own_group))),
        ErrorKind::RequestIdReuse
    );
    assert_eq!(h.k().dedup().len(), 1);
}

/// M7A-77. The request's affinity is extracted from each key's C0 prefix, and these are C0's
/// known-answer bytes (`scoped_key_known_answer_vector` in `tests/contracts.rs`), written out
/// literally so a change to the encoding cannot move both sides at once. No request-level
/// `affinity_id(req)` exists: check 3 reads every key's prefix and compares it with the request's
/// `(tenant, affinity)`. So for each vector entry a request naming the vector's ids is admitted,
/// one naming another group is `CROSS_AFFINITY` whose `found` is the id extracted from the bytes,
/// and a key one byte short of the prefix has no ids at all (`INVALID_ARGUMENT{"key"}`).
#[retcd_test]
fn m7a_77_affinity_extraction_vector() {
    let vector: [(&[u8], TenantId, AffinityId); 2] = [
        (
            &[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, b'k'],
            TenantId(1),
            AffinityId(2),
        ),
        (&[0xFF; 12], TenantId(u32::MAX), AffinityId(u64::MAX)),
    ];
    for (bytes, tenant, affinity) in vector {
        let request = |id: u64, group: AffinityId, key: &[u8]| TxnRequest {
            identity: RequestIdentity {
                tenant,
                client: ClientId(5),
                request: RequestId(id),
            },
            affinity: group,
            mutations: vec![Mutation::Put {
                key: Bytes::copy_from_slice(key),
                value: Bytes::from_static(b"v"),
                expected_version: None,
            }],
            ..put(id, b"k", b"v")
        };
        let mut h = H::live();
        let _ = h.admit(request(1, affinity, bytes));
        // `fail` names the fixture tenant; these requests name the vector's.
        let refused = |id: u64, error| {
            vec![EffectKind::Reply(ReplyEffect::Failed {
                identity: RequestIdentity {
                    tenant,
                    client: ClientId(5),
                    request: RequestId(id),
                },
                error,
            })]
        };
        let other = AffinityId(affinity.0 ^ 1);
        assert_eq!(
            h.step(submit(request(2, other, bytes))),
            refused(
                2,
                RdbError::CrossAffinity {
                    expected: other,
                    found: affinity
                }
            ),
            "{bytes:?}"
        );
        assert_eq!(
            h.step(submit(request(3, affinity, &bytes[..KEY_SCOPE_LEN - 1]))),
            refused(3, RdbError::InvalidArgument { field: "key" })
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Checks 1-10, in order
// ---------------------------------------------------------------------------------------------

/// The three faults checks 2, 3 and 5 look for: a past deadline, a key in another affinity group,
/// and a stale `expected_generation`.
#[derive(Debug, Clone, Copy)]
enum Fault {
    Deadline,
    Affinity,
    Generation,
}

/// Every order in which a fixture can apply the three faults.
const FAULT_ORDERS: [[Fault; 3]; 6] = {
    use Fault::{Affinity as A, Deadline as D, Generation as G};
    [
        [D, A, G],
        [D, G, A],
        [A, D, G],
        [A, G, D],
        [G, D, A],
        [G, A, D],
    ]
};

/// R(1) with each fault in `order` applied, in that order.
fn with_faults(order: [Fault; 3]) -> TxnRequest {
    let mut req = put(1, b"k", b"v");
    for fault in order {
        match fault {
            Fault::Deadline => req.remaining_millis = 0,
            Fault::Affinity => req.mutations.push(Mutation::Delete {
                key: scoped_key(TENANT, AffinityId(10), b"x"),
                expected_version: None,
            }),
            Fault::Generation => req.expected_generation = Some(Generation(6)),
        }
    }
    req
}

/// M7A-62. One request carrying all three faults gets check 2's answer, whichever order the
/// fixture applied them in: the check order decides, not the construction. Nothing is reserved,
/// and the step's whole effect vector is the one reply, so no `Check` went to A1.
#[retcd_test]
fn m7a_62_admit_order_checks_2_3_5_simultaneous_yields_deadline_before_admission() {
    for order in FAULT_ORDERS {
        let mut h = H::live();
        let before = (h.k().next_seq(), h.k().prev_digest());
        assert_eq!(
            h.step(submit(with_faults(order))),
            fail(
                1,
                RdbError::DeadlineBeforeAdmission {
                    partition: PARTITION
                }
            ),
            "{order:?}"
        );
        assert_eq!((h.k().next_seq(), h.k().prev_digest()), before);
        assert_eq!((h.k().inflight(), h.k().queue_len()), (None, 0));
    }
}

/// M7A-63, M7A-62's twin. One fact at a time: with the deadline in the future the same request
/// is `CROSS_AFFINITY`; without the foreign key it is `GENERATION_CHANGED`; with the generation
/// right as well it is admitted.
#[retcd_test]
fn m7a_63_admit_order_remove_deadline_fact_shifts_to_cross_affinity() {
    let mut h = H::live();
    let mut req = with_faults(FAULT_ORDERS[0]);
    req.remaining_millis = 5;
    assert_eq!(
        h.step(submit(req.clone())),
        fail(
            1,
            RdbError::CrossAffinity {
                expected: AFF,
                found: AffinityId(10)
            }
        )
    );
    req.mutations.pop();
    assert_eq!(h.step(submit(req.clone())), fail(1, gen_changed(6, 7)));
    req.expected_generation = Some(GEN);
    let _ = h.admit(req);
}

/// M7A-64. Check 1 reads the version before anything in the body: an unknown `api_version` with
/// a past deadline is `INCOMPATIBLE_VERSION`. The twin, one fact apart: the known version with the
/// same past deadline is `DEADLINE_BEFORE_ADMISSION`.
#[retcd_test]
fn m7a_64_admit_incompatible_version_first() {
    let mut h = H::live();
    let known = put(1, b"k", b"v").api_version;
    let mut req = put(1, b"k", b"v");
    req.remaining_millis = 0;
    req.api_version = known + 8;
    assert_eq!(
        failed(&h.step(submit(req.clone()))),
        ErrorKind::IncompatibleVersion
    );
    req.api_version = known;
    assert_eq!(
        failed(&h.step(submit(req))),
        ErrorKind::DeadlineBeforeAdmission
    );
    assert_eq!(h.k().next_seq(), Seq(1));
}

/// M7A-65, half (a) only: a node that holds no lineage for the partition answers check 4 with
/// `NOT_PRIMARY` and no hint, in the same step, and nothing else. Half (b), `ROUTE_CHANGED`, is
/// deferred to the routing milestone (Gautam, 2026-09-27; lead ruling A-R74: `TxnRequest`
/// carries no `route_revision`), so this function asserts nothing about it.
///
/// "No lineage for the partition" is per node: node A is live at generation 7, and the same
/// request submitted on node B, which never saw a `Recovered`, is refused there. The twin, one
/// fact apart (the stepping node holds the lineage), is node A admitting the same request to its
/// dispatch check.
#[retcd_test]
fn m7a_65_admit_not_primary_vs_route_changed() {
    let mut h = H::live();
    h.node = NODE_B;
    assert!(
        h.t1.kernel(NODE_B, PARTITION).is_none(),
        "B holds no lineage"
    );
    assert_eq!(
        h.step(submit(put(1, b"k", b"v"))),
        vec![EffectKind::Reply(ReplyEffect::Failed {
            identity: identity(1),
            error: RdbError::NotPrimary {
                partition: PARTITION,
                hint: None,
            },
        })],
        "exactly one NOT_PRIMARY reply naming the partition, no hint, no check, no batch"
    );
    assert!(
        h.t1.kernel(NODE_B, PARTITION).is_none(),
        "a refused submit creates no lineage"
    );

    // The twin: on A, which holds the lineage, the same request passes check 4.
    h.node = NODE_A;
    assert_eq!(
        h.k().next_seq(),
        Seq(1),
        "B's refusal reserved nothing on A"
    );
    let _ = h.admit(put(1, b"k", b"v"));
}

#[retcd_test]
fn a_key_without_a_scope_prefix_is_invalid_not_a_pass() {
    let mut h = H::live();
    let mut req = put(1, b"k", b"v");
    req.conditions.push(Condition::Present {
        key: Bytes::from_static(b"short"),
    });
    let effects = h.step(submit(req));
    assert_eq!(
        effects,
        vec![EffectKind::Reply(ReplyEffect::Failed {
            identity: identity(1),
            error: RdbError::InvalidArgument { field: "key" }
        })]
    );
    // Another tenant's key in this group is cross-affinity too.
    let mut req = put(2, b"k", b"v");
    req.conditions.push(Condition::Absent {
        key: scoped_key(TenantId(4), AFF, b"k"),
    });
    assert_eq!(failed(&h.step(submit(req))), ErrorKind::CrossAffinity);
}

#[retcd_test]
fn check_six_denies_past_the_horizon_with_the_views_reason() {
    let mut h = H::live();
    let mut fenced = view(GEN, 2, 20);
    fenced.past_horizon = DenyReason::GenerationChanged;
    assert_eq!(
        h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(fenced)))),
        vec![]
    );
    h.now = 20;
    let _ = h.admit(put(1, b"k", b"v"));
    let mut h = H::live();
    let _ = h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(fenced))));
    h.now = 21;
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"v")))),
        ErrorKind::GenerationChanged
    );
    fenced.past_horizon = DenyReason::Expired;
    fenced.authority_seq = 3;
    let _ = h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(fenced))));
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"v")))),
        ErrorKind::LeaseExpired
    );
}

#[retcd_test]
fn check_six_denies_a_view_for_another_lineage_and_keeps_the_newer_view() {
    let mut h = H::live();
    let _ = h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(view(
        Generation(8),
        5,
        u64::MAX,
    )))));
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"v")))),
        ErrorKind::GenerationChanged
    );
    // An older view never replaces a newer one.
    assert_eq!(
        h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(view(
            GEN,
            4,
            u64::MAX
        ))))),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityView)]
    );
    assert_eq!(h.k().authority().map(|v| v.authority_seq), Some(5));
}

/// The tick A1 fences at in the horizon rows.
const FENCE_TICK: u64 = 20;

/// A1's superseding view for a fence at [`FENCE_TICK`] (K-A-49): one newer than the recovery's,
/// horizon one tick before the fence, `past_horizon` the fence's own reason.
fn fence_view(reason: DenyReason) -> Event {
    let mut superseding = view(GEN, 2, FENCE_TICK - 1);
    superseding.past_horizon = reason;
    kernel(KernelEvent::Authority(AuthorityEvent::View(superseding)))
}

/// Live, holding the fence view for `reason`, and nothing else from the fence. The `Freeze` that
/// travels with the view is M7A-138's; holding it back is what leaves check 6 alone to decide.
fn behind_fence_view(reason: DenyReason) -> H {
    let mut h = H::live();
    assert_eq!(h.step(fence_view(reason)), vec![]);
    h
}

/// M7A-66. At the fence tick itself, and after it, a `Submit` is refused in the same step with
/// the fence's reason: `LEASE_EXPIRED`, nothing reserved, no `Check`. A horizon **at** the fence
/// tick would have admitted the first one. (b) The reason picks the code: a `GenerationChanged`
/// fence is `GENERATION_CHANGED`. (c) No view held is `NoGrant`, which is `LEASE_EXPIRED`. (c) is
/// asserted at the deny mapping only: every T1 instance is created holding its `Recovered`'s
/// `authority_view`, and `on_view` only ever replaces it, so check 6's `NoGrant` branch has no
/// event path (lead ruling A-R74 accepted this disclosure).
#[retcd_test]
fn m7a_66_admit_at_fence_tick_denies_with_the_fence_reason() {
    for now in [FENCE_TICK, FENCE_TICK + 1] {
        let mut h = behind_fence_view(DenyReason::Expired);
        let before = (h.k().next_seq(), h.k().prev_digest());
        h.now = now;
        assert_eq!(
            h.step(submit(put(1, b"k", b"v"))),
            fail(
                1,
                RdbError::LeaseExpired {
                    partition: PARTITION,
                    grant: GrantId(2)
                }
            ),
            "tick {now}"
        );
        assert_eq!((h.k().next_seq(), h.k().prev_digest()), before);
        assert_eq!((h.k().inflight(), h.k().queue_len()), (None, 0));
    }

    let mut h = behind_fence_view(DenyReason::GenerationChanged);
    h.now = FENCE_TICK;
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"v")))),
        ErrorKind::GenerationChanged
    );

    assert_eq!(
        deny_error(DenyReason::NoGrant, Boundary::PreApply, &deny_at()),
        RdbError::LeaseExpired {
            partition: PARTITION,
            grant: GrantId(2)
        }
    );
}

/// M7A-67, M7A-66's twin (one fact: `now <= valid_through_tick`). One tick before the fence the
/// same view admits: the effects are exactly the `StorageDispatch` check, and that check is what
/// T1 now awaits.
#[retcd_test]
fn m7a_67_admit_authority_view_allow_proceeds_to_dispatch_check() {
    let mut h = behind_fence_view(DenyReason::Expired);
    h.now = FENCE_TICK - 1;
    let asked = only_check(&h.step(submit(put(1, b"k", b"v"))));
    let Some(Inflight::AwaitingDispatchCheck { correlation, .. }) = h.k().inflight() else {
        panic!("expected AwaitingDispatchCheck, got {:?}", h.k().inflight());
    };
    assert_eq!(*correlation, asked);
}

/// M7A-68. A is admitted and waiting on its check. A1 fences at [`FENCE_TICK`] and pushes the
/// superseding view. B, submitted at the fence tick, is refused `LEASE_EXPIRED` in that step with
/// nothing sent to A1, and is not queued; A still waits (its fate is the `Freeze`'s, M7A-138).
/// The twin, one fact apart (a tick earlier): B is queued behind A.
#[retcd_test]
fn m7a_68_admit_superseding_view_push_denies_next_submit_without_message() {
    for (now, refused) in [(FENCE_TICK, true), (FENCE_TICK - 1, false)] {
        let mut h = H::live();
        let _ = h.admit(put(1, b"a", b"1"));
        h.now = now;
        assert_eq!(h.step(fence_view(DenyReason::Expired)), vec![]);
        let effects = h.step(submit(put(2, b"b", b"2")));
        if refused {
            assert_eq!(
                effects,
                fail(
                    2,
                    RdbError::LeaseExpired {
                        partition: PARTITION,
                        grant: GrantId(2)
                    }
                )
            );
            assert_eq!(h.k().queue_len(), 0);
        } else {
            assert_eq!(effects, vec![], "queued");
            assert_eq!(h.k().queue_len(), 1);
        }
        assert!(matches!(
            h.k().inflight(),
            Some(Inflight::AwaitingDispatchCheck { .. })
        ));
    }
}

#[retcd_test]
fn check_seven_maps_each_freeze_cause() {
    let mut h = H::live();
    assert_eq!(
        h.step(fence(FenceScope::Node, DenyReason::SelfFenced)),
        vec![]
    );
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"v")))),
        ErrorKind::LeaseExpired
    );
    let mut h = H::live();
    let _ = h.step(fence(
        FenceScope::Partition(PARTITION),
        DenyReason::LocalStorageFenced,
    ));
    assert_eq!(
        h.k().mode(),
        &QueueMode::Frozen {
            cause: FreezeCause::LocalStorageFenced,
            unresolved: None
        }
    );
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"v")))),
        ErrorKind::ProtectionPaused
    );
    let mut h = H::live();
    let _ = h.step(recovered(Generation(8), 0, NODE_A, PartitionMode::ReadOnly));
    let _ = h.step(admission(true, None));
    let _ = h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(view(
        Generation(8),
        2,
        u64::MAX,
    )))));
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"v")))),
        ErrorKind::RecoveryReadOnly
    );
}

#[retcd_test]
fn a_fence_for_another_partition_is_not_ours() {
    let mut h = H::live();
    assert_eq!(
        h.step(fence(
            FenceScope::Partition(PartitionId(2)),
            DenyReason::Expired
        )),
        vec![ignored(AuthorityIgnoreReason::NotOurs)]
    );
    assert_eq!(h.k().mode(), &QueueMode::Open);
}

#[retcd_test]
fn check_eight_fails_closed_then_passes_the_reason_through() {
    let mut h = H {
        t1: Transaction::new(),
        snap: Snap::default(),
        now: 10,
        node: NODE_A,
        boot: BootId(1),
    };
    let blocked = BlockReason::DivergenceRequiresOperator {
        diverged: vec![CopyId(2)],
    };
    let _ = h.step(recovered(GEN, 0, NODE_A, PartitionMode::Active));
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"v")))),
        ErrorKind::ProtectionPaused,
        "no SetAdmission yet: fail closed"
    );
    let _ = h.step(admission(false, Some(blocked.client_error_kind())));
    assert_eq!(
        failed(&h.step(submit(put(1, b"k", b"v")))),
        ErrorKind::DivergenceRequiresOperator
    );
    let _ = h.step(admission(false, Some(ErrorKind::ProtectionPaused)));
    let effects = h.step(submit(put(1, b"k", b"v")));
    assert_eq!(
        effects,
        vec![EffectKind::Reply(ReplyEffect::Failed {
            identity: identity(1),
            error: RdbError::ProtectionPaused {
                partition: PARTITION,
                paused_after: Seq(4)
            }
        })],
        "paused_after is L1's paused_prefix"
    );
}

/// A queue of one behind the one in flight.
fn queue_of_one() -> Limits {
    Limits {
        queue_cap: 1,
        dedup_cap: 100,
        ..Limits::default()
    }
}

/// M7A-69. Every case runs with the queue at its cap, so check 9 would say `OVERLOADED`; each
/// answer is an earlier check's. (a) Frozen on an unresolved transaction: check 7's
/// `PROTECTION_PAUSED`. (b) Open, L1 refusing with `PROTECTION_PAUSED`: that. (c) Open, L1
/// refusing with `DIVERGENCE_REQUIRES_OPERATOR`: that code, passed through, in the same step, and
/// the reply is the step's only effect. (b) and (c) differ by the `reason` field alone.
#[retcd_test]
fn m7a_69_admit_mode_frozen_protection_paused_and_admission_state_reason_passthrough() {
    let mut h = H::live_with(queue_of_one());
    let (batch, _, seq) = h.dispatch(put(1, b"a", b"1"));
    assert_eq!(
        h.step(submit(put(2, b"b", b"2"))),
        vec![],
        "queued: at the cap"
    );
    assert_eq!(h.step(committed(batch, seq)).len(), 2);
    assert_eq!(
        h.k().mode(),
        &QueueMode::Frozen {
            cause: FreezeCause::UnresolvedTransaction,
            unresolved: Some(Seq(seq))
        }
    );
    assert_eq!(
        h.step(submit(put(3, b"c", b"3"))),
        fail(
            3,
            RdbError::ProtectionPaused {
                partition: PARTITION,
                paused_after: Seq(seq)
            }
        )
    );

    let passed_through = [
        (
            ErrorKind::ProtectionPaused,
            RdbError::ProtectionPaused {
                partition: PARTITION,
                paused_after: Seq(4),
            },
        ),
        (
            ErrorKind::DivergenceRequiresOperator,
            RdbError::DivergenceRequiresOperator {
                partition: PARTITION,
                diverged: vec![],
            },
        ),
    ];
    for (reason, error) in passed_through {
        let mut h = H::live_with(queue_of_one());
        let _ = h.admit(put(1, b"a", b"1"));
        assert_eq!(
            h.step(submit(put(2, b"b", b"2"))),
            vec![],
            "queued: at the cap"
        );
        assert_eq!(h.step(admission(false, Some(reason))), vec![]);
        assert_eq!(h.k().mode(), &QueueMode::Open);
        assert_eq!(
            h.step(submit(put(3, b"c", b"3"))),
            fail(3, error),
            "{reason:?}"
        );
    }
}

/// M7A-70. (a) With the queue at its cap a valid request is `OVERLOADED`, and so is an invalid
/// one: check 9 runs before check 10. (b) With room in the queue the invalid one is
/// `INVALID_ARGUMENT` naming the field.
#[retcd_test]
fn m7a_70_admit_overloaded_then_invalid_argument_last() {
    let mut h = H::live_with(queue_of_one());
    let _ = h.admit(put(1, b"a", b"1"));
    assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![], "queued");
    assert_eq!(
        h.step(submit(put(3, b"c", b"3"))),
        fail(
            3,
            RdbError::Overloaded {
                partition: PARTITION
            }
        )
    );
    let mut bad = put(4, b"c", b"3");
    bad.mutations.clear();
    assert_eq!(failed(&h.step(submit(bad.clone()))), ErrorKind::Overloaded);
    let mut h = H::live();
    assert_eq!(
        h.step(submit(bad)),
        fail(4, RdbError::InvalidArgument { field: "mutations" })
    );
}

#[retcd_test]
fn check_nine_overloaded_once_the_dedup_index_is_full() {
    let mut h = H::live_with(Limits {
        queue_cap: 8,
        dedup_cap: 1,
        ..Limits::default()
    });
    let mut req = put(1, b"k", b"v");
    req.conditions.push(Condition::Present { key: key(b"k") });
    assert_eq!(failed(&h.step(submit(req))), ErrorKind::ConditionFailed);
    assert_eq!(h.k().dedup().len(), 1);
    assert_eq!(
        failed(&h.step(submit(put(2, b"k", b"v")))),
        ErrorKind::Overloaded
    );
}

// ---------------------------------------------------------------------------------------------
// Steps 11-15 at the head of the queue
// ---------------------------------------------------------------------------------------------

/// R(1) with the condition `version == 3` on `k`, and `k` at version `at`.
fn needs_version_3(at: Version) -> (H, TxnRequest) {
    let mut h = H::live();
    let mut req = put(1, b"k", b"v");
    req.conditions.push(Condition::VersionEquals {
        key: key(b"k"),
        version: 3,
    });
    h.snap.versions.insert(key(b"k").to_vec(), at);
    (h, req)
}

/// M7A-78. `k` is at version 2: `CONDITION_FAILED` at index 0 is the step's only effect, so no
/// batch, and nothing is reserved: no sequence, no record digest, nothing in flight. The twin,
/// one fact apart (`k` at version 3): the request goes on to its dispatch check.
#[retcd_test]
fn m7a_78_condition_failed_allocates_nothing() {
    let (mut h, req) = needs_version_3(2);
    let before = (h.k().next_seq(), h.k().prev_digest());
    assert_eq!(
        h.step(submit(req)),
        fail(1, RdbError::ConditionFailed { index: 0 })
    );
    assert_eq!((h.k().next_seq(), h.k().prev_digest()), before);
    assert_eq!(h.k().inflight(), None);

    let (mut h, req) = needs_version_3(3);
    let _ = h.admit(req);
}

/// M7A-79. The failure is retained: after `k` reaches version 3 the same request is still
/// answered with the original `CONDITION_FAILED`, not evaluated again. The twin, one fact apart
/// (a new `request_id`): evaluated fresh against version 3, and it commits.
#[retcd_test]
fn m7a_79_condition_failed_retained_replayed_verbatim() {
    let (mut h, mut req) = needs_version_3(2);
    assert_eq!(
        failed(&h.step(submit(req.clone()))),
        ErrorKind::ConditionFailed
    );
    h.snap.versions.insert(key(b"k").to_vec(), 3);
    assert_eq!(
        h.step(submit(req.clone())),
        fail(1, RdbError::ConditionFailed { index: 0 })
    );
    assert_eq!(h.k().next_seq(), Seq(1));
    req.identity = identity(2);
    let (_, _, seq) = h.dispatch(req);
    assert_eq!(seq, 1);
}

/// M7A-175 (ADR 0004 §8, lead ruling A-R75), kernel half. R(5) pinned to g7 fails its condition
/// in g7; the partition recovers into g8, on the same node and, in a second trace, on another;
/// the condition becomes true; the same request is re-delivered. Both traces answer exactly
/// `GENERATION_CHANGED{7, 8}` at check 5: no check, no batch, `next_seq` and `prev_digest`
/// unchanged. The condition failure was never durable, so this is check 5's answer, not a dedup
/// replay. The twin, one fact apart (no `expected_generation`): the re-delivery is evaluated
/// fresh and admitted, and its dispatch check is the only effect. That is the documented
/// raw-wire limit: a caller that does not pin the generation can see a failure reversed.
#[retcd_test]
fn m7a_175_condition_failure_is_not_reversed_by_a_duplicate() {
    let at_g8 = |failover: bool, pinned: bool| {
        let mut h = H::live();
        let mut req = put(5, b"k", b"v");
        req.conditions.push(Condition::Present { key: key(b"k") });
        req.expected_generation = pinned.then_some(GEN);
        assert_eq!(
            h.step(submit(req.clone())),
            fail(5, RdbError::ConditionFailed { index: 0 })
        );
        let next = if failover { NODE_B } else { NODE_A };
        assert_eq!(
            h.step(recovered(Generation(8), 0, next, PartitionMode::Active)),
            vec![]
        );
        if failover {
            h.node = NODE_B;
            assert_eq!(
                h.step(recovered(Generation(8), 0, NODE_B, PartitionMode::Active)),
                vec![]
            );
        }
        let _ = h.step(admission(true, None));
        assert_eq!(h.k().lineage().generation, Generation(8));
        h.snap.versions.insert(key(b"k").to_vec(), 1);
        (h, req)
    };
    for failover in [false, true] {
        let (mut h, req) = at_g8(failover, true);
        let before = (h.k().next_seq(), h.k().prev_digest());
        assert_eq!(
            h.step(submit(req)),
            fail(5, gen_changed(7, 8)),
            "failover: {failover}"
        );
        assert_eq!((h.k().next_seq(), h.k().prev_digest()), before);
        assert_eq!(h.k().inflight(), None);

        let (mut h, req) = at_g8(failover, false);
        let _ = h.admit(req);
    }
}

#[retcd_test]
fn a_mutations_expected_version_counts_after_the_conditions() {
    let mut h = H::live();
    let mut req = put(1, b"k", b"v");
    req.conditions.push(Condition::Absent { key: key(b"x") });
    req.mutations = vec![
        Mutation::Delete {
            key: key(b"x"),
            expected_version: None,
        },
        Mutation::Put {
            key: key(b"k"),
            value: Bytes::from_static(b"v"),
            expected_version: Some(4),
        },
    ];
    assert_eq!(
        h.step(submit(req)),
        vec![EffectKind::Reply(ReplyEffect::Failed {
            identity: identity(1),
            error: RdbError::ConditionFailed { index: 2 }
        })]
    );
}

/// M7A-80. A `Deny(Expired)` for the outstanding check, at the current `authority_seq`, answers
/// `LEASE_EXPIRED` and discards the reservation: `next_seq` and `prev_digest` are what they were
/// before the `Submit`, nothing is retained, and the queue is pumped. The twin, one fact apart
/// (the next answer is `Admit`): B's batch takes the discarded position, so it was given back,
/// not skipped.
#[retcd_test]
fn m7a_80_seq_reservation_discardable_on_dispatch_deny() {
    let mut h = H::live();
    let (seq, digest) = (h.k().next_seq(), h.k().prev_digest());
    let first = h.admit(put(1, b"a", b"1"));
    assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![]);
    let effects = h.step(answer(first, 1, Verdict::Deny(DenyReason::Expired)));
    let [EffectKind::Reply(ReplyEffect::Failed {
        identity: who,
        error,
    }), EffectKind::Kernel(KernelEffect::AuthorityCheck {
        correlation: second,
        ..
    })] = effects.as_slice()
    else {
        panic!("{effects:?}");
    };
    assert_eq!((*who, error.kind()), (identity(1), ErrorKind::LeaseExpired));
    assert_eq!((h.k().next_seq(), h.k().prev_digest()), (seq, digest));
    assert_eq!(h.k().dedup().len(), 0, "a deny retains nothing");
    let second = *second;
    let effects = h.step(answer(second, 1, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("expected B's batch, got {effects:?}");
    };
    assert_eq!(batch.seq, seq, "the discarded position is reused");
}

#[retcd_test]
fn only_our_answer_at_a_fresh_enough_authority_seq_is_heard() {
    let mut h = H::live();
    let _ = h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(view(
        GEN,
        5,
        u64::MAX,
    )))));
    let correlation = h.admit(put(1, b"a", b"1"));
    let stale = vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)];
    assert_eq!(
        h.step(answer(CorrelationId(correlation.0 + 1), 5, Verdict::Admit)),
        stale
    );
    // Older than the view: not heard. It answers the outstanding check, so T1 asks again
    // (A-R69 via A-R70), and the answer to the old correlation is plainly stale from then on.
    let correlation = reasked(&h.step(answer(correlation, 4, Verdict::Admit)));
    // Another checkpoint's answer is not a T1 input at all.
    let mut publication = decision(correlation, 5, Verdict::Admit);
    publication.checkpoint = Checkpoint::Publication;
    assert!(h
        .try_step(kernel(KernelEvent::Authority(AuthorityEvent::Answer(
            publication
        ))))
        .is_err());
    // Through `step_txn` it reaches the kernel, and `answer_is_ours` refuses it.
    let ctx = ctx(h.now, h.node, &h.snap);
    let reached =
        h.t1.step_txn(&ctx, TxnEvent::AuthorityAnswer(publication))
            .expect("step_txn never fails");
    assert_eq!(
        reached,
        vec![TxnEffect::Ignored(KernelIgnoredReason::Authority(
            AuthorityIgnoreReason::StaleAuthorityAnswer
        ))]
    );
    assert!(matches!(
        h.k().inflight(),
        Some(Inflight::AwaitingDispatchCheck { .. })
    ));
    assert_eq!(h.step(answer(correlation, 5, Verdict::Admit)).len(), 1);
}

#[retcd_test]
fn an_admit_for_another_lineage_or_grant_generation_is_a_deny() {
    let mut h = H::live();
    let correlation = h.admit(put(1, b"a", b"1"));
    let mut moved = decision(correlation, 1, Verdict::Admit);
    moved.lineage = lineage_at(Generation(8));
    assert_eq!(
        failed(
            &h.step(kernel(KernelEvent::Authority(AuthorityEvent::Answer(
                moved
            ))))
        ),
        ErrorKind::GenerationChanged
    );
    let correlation = h.admit(put(2, b"a", b"1"));
    let mut regranted = decision(correlation, 1, Verdict::Admit);
    regranted.authority_generation = AuthorityGeneration(2);
    assert_eq!(
        failed(
            &h.step(kernel(KernelEvent::Authority(AuthorityEvent::Answer(
                regranted
            ))))
        ),
        ErrorKind::LeaseExpired
    );
    assert_eq!(h.k().next_seq(), Seq(1));
}

// ---------------------------------------------------------------------------------------------
// Completion, freeze and publication (§3.3, K-A-33, K-A-46)
// ---------------------------------------------------------------------------------------------

/// M7A-81. `Admit` gives exactly the batch at `s`. Its completion gives `LocalApplied` and the
/// candidate for `s`, chained from the digest before it, and no reply: T1 never answers success.
/// `next_seq` is `s + 1`.
#[retcd_test]
fn m7a_81_batch_commit_increments_next_seq_emits_candidate() {
    let mut h = H::live();
    let (s, before) = (h.k().next_seq(), h.k().prev_digest());
    let correlation = h.admit(put(1, b"a", b"1"));
    let effects = h.step(answer(correlation, 1, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("expected exactly the batch, got {effects:?}");
    };
    assert_eq!(batch.seq, s);
    let (id, digest) = (batch.id, h.k().prev_digest());
    let effects = h.step(committed(id, s.0));
    let [EffectKind::Kernel(KernelEffect::LocalApplied { seq, .. }), EffectKind::Kernel(KernelEffect::AppliedCandidate(candidate))] =
        effects.as_slice()
    else {
        panic!("expected LocalApplied then the candidate, and no reply; got {effects:?}");
    };
    assert_eq!(*seq, s);
    assert_eq!(
        (
            candidate.seq,
            candidate.prev_digest,
            candidate.record_digest
        ),
        (s, before, digest)
    );
    assert_eq!(h.k().next_seq(), s.next());
}

/// A batch dispatched at seq 1, then `ending` (built from its id and seq) as its completion, with
/// R(2) queued behind it. Returns the harness, the effects of the completion, and the seq.
fn a_batch_that_fails(
    ending: impl Fn(BatchId, u64) -> Event,
) -> (H, Vec<EffectKind>, BatchId, u64) {
    let mut h = H::live();
    let (batch, _, seq) = h.dispatch(put(1, b"a", b"1"));
    assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![]);
    let effects = h.step(ending(batch, seq));
    (h, effects, batch, seq)
}

/// What every failed batch leaves: the original answered `UNKNOWN_OUTCOME` and R(2) behind it
/// `PROTECTION_PAUSED`, the partition frozen on local storage with `seq` unresolved, `next_seq`
/// not rolled back, and a new `Submit` refused `PROTECTION_PAUSED`.
fn assert_fenced_after(h: &mut H, effects: &[EffectKind], seq: u64, what: &str) {
    assert_eq!(
        replies(effects),
        vec![
            (1, ErrorKind::UnknownOutcome),
            (2, ErrorKind::ProtectionPaused)
        ],
        "{what}"
    );
    assert_eq!(
        h.k().mode(),
        &QueueMode::Frozen {
            cause: FreezeCause::LocalStorageFenced,
            unresolved: Some(Seq(seq))
        },
        "{what}"
    );
    assert_eq!(h.k().next_seq(), Seq(seq + 1), "{what}: no rollback");
    assert_eq!(
        failed(&h.step(submit(put(3, b"c", b"3")))),
        ErrorKind::ProtectionPaused,
        "{what}"
    );
}

fn commit_failed(fault: StorageFault) -> impl Fn(BatchId, u64) -> Event {
    move |batch, _| storage(StorageEvent::CommitFailed { batch, fault })
}

/// M7A-82. A batch that fails answers `UNKNOWN_OUTCOME`, freezes on `LocalStorageFenced` and
/// does not roll `next_seq` back. The twin, asserted here as one fact: a flush that completed
/// partially (`FlushFailed`, the contract's "incomplete") is answered identically. A second
/// completion for the failed batch is unmatched, and another module's batch is declined.
#[retcd_test]
fn m7a_82_batch_err_reply_unknown_freeze_local_storage_fenced_no_rollback() {
    for fault in [StorageFault::WriteFailed, StorageFault::FlushFailed] {
        let (mut h, effects, batch, seq) = a_batch_that_fails(commit_failed(fault));
        assert_fenced_after(&mut h, &effects, seq, &format!("{fault:?}"));
        assert_eq!(
            h.step(committed(batch, seq)),
            vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)]
        );
        assert!(h.try_step(committed(BatchId(1), seq)).is_err());
    }
}

/// M7A-83. Every way a batch can end other than as dispatched freezes the partition the same
/// way: each `StorageFault` a `CommitFailed` can carry, and a `Committed` reporting another
/// position than the batch's own.
///
/// T1 cannot tell the plan's three boundaries apart (an error before any write, after the first
/// mutation, after the last). `StorageEvent::CommitFailed` carries no position, so whichever
/// boundary storage failed at, T1 receives one and the same event. This row therefore covers every
/// input T1 can receive for a failed batch (lead ruling A-R74, item 5). Injecting the fault at
/// each boundary is a property of the sim's storage, and is owed there as a follow-up: an
/// `rdb-sim` crash-image row that fails one batch at each of the three boundaries and checks that
/// T1 is fenced in all three.
#[retcd_test]
fn m7a_83_batch_error_freezes_at_every_boundary() {
    let faults = [
        StorageFault::WriteFailed,
        StorageFault::FlushFailed,
        StorageFault::ProcessCrash,
        StorageFault::HostCrash,
        StorageFault::Corrupt,
    ];
    for fault in faults {
        let (mut h, effects, _, seq) = a_batch_that_fails(commit_failed(fault));
        assert_fenced_after(&mut h, &effects, seq, &format!("{fault:?}"));
    }
    let misplaced = |batch, seq: u64| committed(batch, seq + 1);
    let (mut h, effects, _, seq) = a_batch_that_fails(misplaced);
    assert_fenced_after(&mut h, &effects, seq, "Committed at another position");
}

/// A completion carrying T1's tag but naming a batch that is not in flight is claimed (it is a
/// T1 input) and refused, and it does not consume the real batch's completion.
#[retcd_test]
fn a_t1_tagged_completion_for_another_batch_is_unmatched() {
    let mut h = H::live();
    let (batch, _, seq) = h.dispatch(put(1, b"a", b"1"));
    assert_eq!(
        h.step(committed(BatchId(BATCH_TAG | 99), seq)),
        vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)]
    );
    assert_eq!(h.k().mode(), &QueueMode::Open);
    assert_eq!(h.step(committed(batch, seq)).len(), 2);
}

/// A-R70, tester-t1 hunt_07. Retiring the generation being served is refused with a named
/// reason and drops nothing: accepting it would let every retained request execute again.
#[retcd_test]
fn retiring_the_served_generation_is_refused_and_keeps_its_entries() {
    let mut h = H::live();
    let _ = h.resolve(put(1, b"a", b"1"));
    assert_eq!(
        h.step(kernel(KernelEvent::RetireGeneration { generation: GEN })),
        vec![ignored(AuthorityIgnoreReason::RetireServedGeneration)]
    );
    assert!(!h.k().dedup().is_retired(GEN));
    assert_eq!(h.k().dedup().len(), 1);
    assert!(matches!(
        h.step(submit(put(1, b"a", b"1"))).as_slice(),
        [EffectKind::Reply(ReplyEffect::Transaction { .. })]
    ));
    assert_eq!(h.k().next_seq(), Seq(2), "replayed, not executed again");
    // A condition failure is still retained afterwards.
    let mut req = put(2, b"k", b"v");
    req.conditions.push(Condition::Present { key: key(b"k") });
    assert_eq!(failed(&h.step(submit(req))), ErrorKind::ConditionFailed);
    assert_eq!(h.k().dedup().len(), 2);
}

/// A retired generation retains nothing, including from a later seed: its rows are still on
/// disk, and loading them would resurrect a generation declared gone.
#[retcd_test]
fn a_retired_generation_is_not_reloaded_by_a_later_seed() {
    let mut h = H::live();
    let _ = h.resolve(put(1, b"a", b"1"));
    let _ = h.step(recovered(Generation(8), 1, NODE_A, PartitionMode::Active));
    assert_eq!(
        h.step(kernel(KernelEvent::RetireGeneration { generation: GEN })),
        vec![]
    );
    assert!(h.k().dedup().is_empty());
    let _ = h.step(recovered(Generation(9), 1, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert_eq!(h.k().seed_pending(), None);
    assert!(
        h.k().dedup().is_empty(),
        "g7's row was read and not retained"
    );
    let _ = h.admit(put(1, b"a", b"1"));
}

/// Live at view `authority_seq` 2; A admitted and awaiting its check, B queued; then a node
/// `Freeze{Expired}`. Returns the harness, A's correlation, the freeze's effects, and
/// `(next_seq, prev_digest)` from before A.
fn frozen_while_awaiting() -> (H, CorrelationId, Vec<EffectKind>, (Seq, Digest)) {
    let mut h = H::live();
    assert_eq!(h.step(push_view(2)), vec![]);
    let before = (h.k().next_seq(), h.k().prev_digest());
    let correlation = h.admit(put(1, b"a", b"1"));
    assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![]);
    let effects = h.step(fence(FenceScope::Node, DenyReason::Expired));
    (h, correlation, effects, before)
}

fn lost_to_expiry() -> QueueMode {
    QueueMode::Frozen {
        cause: FreezeCause::AuthorityLost(DenyReason::Expired),
        unresolved: None,
    }
}

/// M7A-138. The freeze answers A and B `LEASE_EXPIRED`, records that it dropped A's dispatch,
/// and leaves nothing in flight or queued, frozen for lost authority. A's `Admit` arriving later
/// at an old `authority_seq` is stale: no batch, and `next_seq` and `prev_digest` are as before A.
#[retcd_test]
fn m7a_138_freeze_drops_awaiting_dispatch_check_definitive_rejection() {
    let (mut h, correlation, effects, before) = frozen_while_awaiting();
    let expired = |who| {
        EffectKind::Reply(ReplyEffect::Failed {
            identity: identity(who),
            error: RdbError::LeaseExpired {
                partition: PARTITION,
                grant: GrantId(2),
            },
        })
    };
    assert_eq!(
        effects,
        vec![
            expired(1),
            ignored(AuthorityIgnoreReason::DispatchDroppedByFreeze),
            expired(2)
        ]
    );
    assert_eq!((h.k().inflight(), h.k().queue_len()), (None, 0));
    assert_eq!(h.k().mode(), &lost_to_expiry());
    assert_eq!(
        h.step(answer(correlation, 1, Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
    assert_eq!((h.k().next_seq(), h.k().prev_digest()), before);
}

/// M7A-140, as re-worded by lead ruling A-R74. The same freeze, then A's `Admit` at the
/// **current** `authority_seq`, so the seq predicate of `answer_is_ours` passes (one fact vs
/// M7A-138). The freeze already dropped the check, so the answer finds nothing outstanding: it is
/// stale, and that is the step's only effect. No second reply for A, no batch, no re-asked check,
/// and nothing moves. The dispatch-time frozen arm the row once named has no event path (the
/// equivalent mutant `S14-mode-conjunct`).
#[retcd_test]
fn m7a_140_dispatch_admit_while_frozen_refused_no_batch() {
    let (mut h, correlation, _, before) = frozen_while_awaiting();
    assert_eq!(h.k().authority().map(|v| v.authority_seq), Some(2));
    assert_eq!(
        h.step(answer(correlation, 2, Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
    assert_eq!((h.k().next_seq(), h.k().prev_digest()), before);
    assert_eq!(h.k().mode(), &lost_to_expiry());
    assert_eq!(h.k().inflight(), None);
}

/// K-A-46, both orders: the freeze cause survives, `unresolved` is set, and `Published` retains
/// the identity and keeps the queue frozen.
#[retcd_test]
fn published_while_frozen_retains_in_both_orders() {
    for freeze_first in [true, false] {
        let mut h = H::live();
        let (batch, digest, seq) = h.dispatch(put(1, b"a", b"1"));
        let fence = || fence(FenceScope::Node, DenyReason::Revoked);
        if freeze_first {
            assert_eq!(h.step(fence()), vec![]);
            assert_eq!(h.step(committed(batch, seq)).len(), 2);
        } else {
            assert_eq!(h.step(committed(batch, seq)).len(), 2);
            assert_eq!(h.step(fence()), vec![]);
        }
        let lost = FreezeCause::AuthorityLost(DenyReason::Revoked);
        assert_eq!(
            h.k().mode(),
            &QueueMode::Frozen {
                cause: lost,
                unresolved: Some(Seq(seq))
            },
            "freeze_first={freeze_first}"
        );
        assert_eq!(
            h.step(published(seq, digest, 1)),
            vec![ignored(AuthorityIgnoreReason::PublishedWhileFrozen)]
        );
        assert_eq!(
            h.k().mode(),
            &QueueMode::Frozen {
                cause: lost,
                unresolved: None
            }
        );
        assert_eq!(h.k().dedup().len(), 1);
    }
}

#[retcd_test]
fn a_published_that_does_not_match_the_dispatch_is_unmatched() {
    let mut h = H::live();
    let (batch, digest, seq) = h.dispatch(put(1, b"a", b"1"));
    let _ = h.step(committed(batch, seq));
    let unmatched = vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)];
    assert_eq!(h.step(published(seq + 1, digest, 1)), unmatched);
    assert_eq!(h.step(published(seq, Digest::ROOT, 1)), unmatched);
    assert_eq!(h.step(published(seq, digest, 2)), unmatched);
    let other_lineage = kernel(KernelEvent::Published {
        lineage: lineage_at(Generation(8)),
        seq: Seq(seq),
        record_digest: digest,
        request: identity(1),
    });
    assert_eq!(h.step(other_lineage), unmatched);
    assert_eq!(h.step(published(seq, digest, 1)), vec![]);
}

/// A-R65 with B-R47's ordering rule: `LocalApplied` exactly once per seq, in seq order, and
/// ahead of that record's `AppliedCandidate` (P1's input, the path to shipping). A duplicate
/// completion and a failed batch emit none.
#[retcd_test]
fn local_applied_is_once_per_seq_in_order_and_before_the_candidate() {
    let mut h = H::live();
    let mut trace = Vec::new();
    for request in 1..=3 {
        let (batch, digest, seq) = h.dispatch(put(request, b"a", b"1"));
        trace.extend(h.step(committed(batch, seq)));
        trace.extend(h.step(committed(batch, seq)));
        trace.extend(h.step(published(seq, digest, request)));
    }
    let (batch, _, _) = h.dispatch(put(4, b"a", b"1"));
    trace.extend(h.step(storage(StorageEvent::CommitFailed {
        batch,
        fault: StorageFault::WriteFailed,
    })));

    let order: Vec<(&str, Seq)> = trace
        .iter()
        .filter_map(|effect| match effect {
            EffectKind::Kernel(KernelEffect::LocalApplied { seq, .. }) => Some(("local", *seq)),
            EffectKind::Kernel(KernelEffect::AppliedCandidate(candidate)) => {
                Some(("candidate", candidate.seq))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        vec![
            ("local", Seq(1)),
            ("candidate", Seq(1)),
            ("local", Seq(2)),
            ("candidate", Seq(2)),
            ("local", Seq(3)),
            ("candidate", Seq(3)),
        ]
    );
}

// ---------------------------------------------------------------------------------------------
// Generation reconciliation, trim and retire
// ---------------------------------------------------------------------------------------------

#[retcd_test]
fn after_recovery_a_retry_must_reconcile_and_never_re_executes() {
    let mut h = H::live();
    let _ = h.resolve(put(1, b"a", b"1"));
    let effects = h.step(recovered(Generation(8), 1, NODE_A, PartitionMode::Active));
    assert_eq!(effects, vec![]);
    let _ = h.step(admission(true, None));
    // Same payload: never replayed transparently, never re-executed.
    assert_eq!(
        h.step(submit(put(1, b"a", b"1"))),
        vec![EffectKind::Reply(ReplyEffect::Failed {
            identity: identity(1),
            error: RdbError::GenerationChanged {
                expected: GEN,
                current: Generation(8)
            }
        })]
    );
    // Another payload under the same identity.
    assert_eq!(
        failed(&h.step(submit(put(1, b"a", b"2")))),
        ErrorKind::RequestIdReuse
    );
    // Retired: absence proves nothing, and the retry runs as a new request.
    assert_eq!(
        h.step(kernel(KernelEvent::RetireGeneration { generation: GEN })),
        vec![]
    );
    assert!(h.k().dedup().is_retired(GEN));
    let _ = h.admit(put(1, b"a", b"1"));
}

/// A-R68, scenario S-F1T1-4. A applies R(1) in g7 and fails over to B in g8. B's in-memory
/// index starts empty, so B must load g7's durable dedup rows before it admits anything, and
/// until its copy shows the retained prefix it refuses the way a frozen partition does.
#[retcd_test]
fn failover_loads_the_predecessors_dedup_before_admitting() {
    let mut h = H::live();
    let _ = h.resolve(put(1, b"a", b"1"));
    assert_eq!(h.snap.at, Seq(1));

    // A is demoted; nothing was waiting.
    assert_eq!(
        h.step(recovered(Generation(8), 1, NODE_B, PartitionMode::Active)),
        vec![]
    );
    assert!(h.t1.kernel(NODE_A, PARTITION).is_none());

    // B's copy has not yet caught up to the retained prefix.
    h.node = NODE_B;
    h.snap.at = Seq::ZERO;
    assert_eq!(
        h.step(recovered(Generation(8), 1, NODE_B, PartitionMode::Active)),
        vec![]
    );
    let _ = h.step(admission(true, None));
    assert!(h.k().seed_pending().is_some());
    assert!(h.k().dedup().is_empty());
    assert_eq!(
        h.step(submit(put(1, b"a", b"1"))),
        vec![EffectKind::Reply(ReplyEffect::Failed {
            identity: identity(1),
            error: RdbError::ProtectionPaused {
                partition: PARTITION,
                paused_after: Seq(1)
            }
        })],
        "no new error code: the frozen partition's refusal"
    );
    assert_eq!(
        failed(&h.step(submit(put(9, b"z", b"9")))),
        ErrorKind::ProtectionPaused,
        "a brand-new identity is refused too: nothing is admitted while loading"
    );

    // B's copy catches up; the next T1 input loads the seed first, then admits against it.
    h.snap.at = Seq(1);
    assert_eq!(
        h.step(submit(put(1, b"a", b"1"))),
        vec![EffectKind::Reply(ReplyEffect::Failed {
            identity: identity(1),
            error: RdbError::GenerationChanged {
                expected: GEN,
                current: Generation(8)
            }
        })]
    );
    assert_eq!(h.k().seed_pending(), None);
    assert_eq!(h.k().dedup().len(), 1);
    assert_eq!(
        failed(&h.step(submit(put(1, b"a", b"2")))),
        ErrorKind::RequestIdReuse
    );
    // The partition is not stuck: a new identity runs.
    let _ = h.admit(put(9, b"z", b"9"));
}

/// One `Failed{identity(request), error}` reply.
fn fail(request: u64, error: RdbError) -> Vec<EffectKind> {
    vec![EffectKind::Reply(ReplyEffect::Failed {
        identity: identity(request),
        error,
    })]
}

fn gen_changed(expected: u64, current: u64) -> RdbError {
    RdbError::GenerationChanged {
        expected: Generation(expected),
        current: Generation(current),
    }
}

/// A1's `StorageDispatch` answer for a check T1 asked while serving `generation`.
fn answer_in(generation: Generation, correlation: CorrelationId, verdict: Verdict) -> Event {
    let mut decision = decision(correlation, 1, verdict);
    decision.lineage = lineage_at(generation);
    kernel(KernelEvent::Authority(AuthorityEvent::Answer(decision)))
}

/// A-R70 F1, tester-t1 hunt_08 (M7A-136). Recovery on the same node, with a request applied
/// but never published inside the cut. It was never retained in memory, and its durable row is
/// what the new instance loads: the retry reconciles and never becomes seq 2.
#[retcd_test]
fn same_node_recovery_reconciles_an_applied_unpublished_request() {
    let mut h = H::live();
    let (batch, _, seq) = h.dispatch(put(1, b"a", b"1"));
    assert_eq!(h.step(committed(batch, seq)).len(), 2);
    let _ = h.step(fence(FenceScope::Node, DenyReason::Expired));
    assert!(
        h.k().dedup().is_empty(),
        "applied, not published: not retained"
    );
    let _ = h.step(recovered(Generation(8), 1, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert_eq!(
        h.step(submit(put(1, b"a", b"1"))),
        fail(1, gen_changed(7, 8))
    );
    assert_eq!(
        failed(&h.step(submit(put(1, b"a", b"2")))),
        ErrorKind::RequestIdReuse
    );
    assert_eq!(h.k().next_seq(), Seq(2));
}

/// A-R70 F2, tester-t1 hunt_09. Demoted, then promoted again on the same node: the instance
/// and its index went with the demotion, and the new one reconciles against the generation
/// that actually applied the request, not merely its predecessor.
#[retcd_test]
fn demote_then_repromote_reconciles_against_the_generation_that_applied() {
    let mut h = H::live();
    let _ = h.resolve(put(1, b"a", b"1"));
    let _ = h.step(recovered(Generation(8), 1, NODE_B, PartitionMode::Active));
    assert!(h.t1.kernel(NODE_A, PARTITION).is_none());
    let _ = h.step(recovered(Generation(9), 1, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert_eq!(
        h.step(submit(put(1, b"a", b"1"))),
        fail(1, gen_changed(7, 9))
    );
}

/// A-R70 F3, tester-t1 hunt_11. A completion for g7's batch that lands after g8's instance
/// dispatched its own first batch is not g8's: the ids differ, so it is unmatched, and g8's
/// real completion still decides g8's batch.
#[retcd_test]
fn a_late_completion_from_an_older_instance_is_unmatched() {
    let mut h = H::live();
    let (b7, _, _) = h.dispatch(put(1, b"a", b"1"));
    let _ = h.step(recovered(Generation(8), 0, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    let c8 = h.admit(put(2, b"b", b"2"));
    let effects = h.step(answer_in(Generation(8), c8, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("expected g8's batch, got {effects:?}");
    };
    let b8 = batch.id;
    assert_ne!(b7, b8);
    assert_eq!(
        h.step(committed(b7, 1)),
        vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)]
    );
    let real = h.step(storage(StorageEvent::CommitFailed {
        batch: b8,
        fault: StorageFault::WriteFailed,
    }));
    assert_eq!(failed(&real), ErrorKind::UnknownOutcome);
}

/// A-R70 F3, tester-t1 hunt_12. A late g7 answer never decides g8's check: the correlations
/// differ, so it is stale, it re-asks nothing, and g8's own answer still admits.
#[retcd_test]
fn a_late_answer_from_an_older_instance_is_stale() {
    let mut h = H::live();
    let c7 = h.admit(put(1, b"a", b"1"));
    let _ = h.step(recovered(Generation(8), 0, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    let c8 = h.admit(put(2, b"b", b"2"));
    assert_ne!(c7, c8);
    assert_eq!(
        h.step(answer(c7, 1, Verdict::Deny(DenyReason::Expired))),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
    assert!(matches!(
        h.step(answer_in(Generation(8), c8, Verdict::Admit))
            .as_slice(),
        [EffectKind::Store(StoreEffect::Commit(_))]
    ));
}

/// A-R70 F3: the boot is in the id too. After a reboot the module starts afresh, even at the
/// same generation, and nothing allocated before the reboot matches anything allocated after.
#[retcd_test]
fn a_rebooted_node_never_reuses_an_id_from_before_the_reboot() {
    let mut h = H::live();
    let c1 = h.admit(put(1, b"a", b"1"));
    let effects = h.step(answer(c1, 1, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("expected a batch, got {effects:?}");
    };
    let b1 = batch.id;

    h.t1 = Transaction::new();
    h.boot = BootId(2);
    let _ = h.step(recovered(GEN, 0, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    let c2 = h.admit(put(2, b"b", b"2"));
    assert_ne!(c1, c2);
    let effects = h.step(answer(c2, 1, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("expected a batch, got {effects:?}");
    };
    assert_ne!(b1, batch.id);
    assert_eq!(
        h.step(committed(b1, 1)),
        vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)]
    );
}

/// A-R70 F4, tester-t1 hunt_15. A `Blocked` divergence recovery answers `RECOVERY_READ_ONLY` at
/// check 7: T1 has no `Blocked` mode (design §3.3's `Frozen × Recovered` row). L1's divergence
/// reason reaches the client only on a partition check 7 lets through, and it names no copies.
#[retcd_test]
fn a_blocked_divergence_recovery_answers_recovery_read_only() {
    let mut h = H::live();
    let _ = h.step(admission(
        false,
        Some(ErrorKind::DivergenceRequiresOperator),
    ));
    assert_eq!(
        h.step(submit(put(1, b"k", b"v"))),
        fail(
            1,
            RdbError::DivergenceRequiresOperator {
                partition: PARTITION,
                diverged: vec![]
            }
        )
    );

    let mut h = H::live();
    let blocked = PartitionMode::Blocked {
        reason: BlockReason::DivergenceRequiresOperator {
            diverged: vec![CopyId(2)],
        },
    };
    let _ = h.step(recovered(Generation(8), 0, NODE_A, blocked));
    let _ = h.step(admission(
        false,
        Some(ErrorKind::DivergenceRequiresOperator),
    ));
    assert_eq!(
        h.step(submit(put(1, b"k", b"v"))),
        fail(
            1,
            RdbError::RecoveryReadOnly {
                partition: PARTITION,
                generation: Generation(8)
            }
        )
    );
}

/// A-R69 via A-R70, tester-t1 hunt_22. The view moves on while the dispatch check is in flight,
/// so its answer lands stale. T1 does not act on it, and it does not wait for good either: it
/// asks again under a fresh correlation, and that answer admits.
#[retcd_test]
fn a_stale_answer_to_the_outstanding_check_asks_again() {
    let mut h = H::live();
    let c = h.admit(put(1, b"a", b"1"));
    let _ = h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(view(
        GEN,
        2,
        u64::MAX,
    )))));
    let fresh = reasked(&h.step(answer(c, 1, Verdict::Admit)));
    assert_ne!(fresh, c);
    assert_eq!(
        h.step(answer(c, 2, Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)],
        "the old correlation no longer names the outstanding check"
    );
    assert_eq!(
        h.step(submit(put(2, b"b", b"2"))),
        vec![],
        "queued behind it"
    );
    assert!(matches!(
        h.step(answer(fresh, 2, Verdict::Admit)).as_slice(),
        [EffectKind::Store(StoreEffect::Commit(_))]
    ));
}

/// The seed loads only what survived: nothing above `retained_through`, and on the same node
/// nothing below a trim this node already applied (it must not resurrect a trimmed identity).
#[retcd_test]
fn a_seed_loads_nothing_past_the_prefix_or_below_the_trim() {
    // Above the prefix: B is cut at 2, and a row for seq 3 is still on its copy.
    let mut h = H::live();
    for request in 1..=3 {
        let _ = h.resolve(put(request, b"a", b"1"));
    }
    let _ = h.step(recovered(Generation(8), 2, NODE_B, PartitionMode::Active));
    h.node = NODE_B;
    let _ = h.step(recovered(Generation(8), 2, NODE_B, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert_eq!(h.k().dedup().len(), 2);
    assert_eq!(
        failed(&h.step(submit(put(2, b"a", b"1")))),
        ErrorKind::GenerationChanged
    );
    let _ = h.admit(put(3, b"a", b"1"));

    // Below the trim, on the same node: R(1) was trimmed from memory and is still on disk.
    let mut h = H::live();
    for request in 1..=2 {
        let _ = h.resolve(put(request, b"a", b"1"));
    }
    let _ = h.step(kernel(KernelEvent::DedupTrim {
        generation: GEN,
        below: Seq(2),
    }));
    let _ = h.step(recovered(Generation(8), 2, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert_eq!(h.k().seed_pending(), None);
    assert_eq!(h.k().dedup().len(), 1, "only R(2)");
    let _ = h.admit(put(1, b"a", b"1"));
}

/// A durable row that does not decode loads nothing and keeps the partition refusing: loading
/// around it could drop a retained identity and re-execute it.
#[retcd_test]
fn a_malformed_durable_row_keeps_the_seed_pending() {
    let mut h = H::live();
    let _ = h.resolve(put(1, b"a", b"1"));
    h.snap
        .dedup
        .insert(vec![0xFF; 3], Bytes::from_static(b"not a dedup value"));
    let _ = h.step(recovered(Generation(8), 1, NODE_B, PartitionMode::Active));
    h.node = NODE_B;
    let _ = h.step(recovered(Generation(8), 1, NODE_B, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert!(h.k().seed_pending().is_some());
    assert!(h.k().dedup().is_empty(), "all or nothing");
    assert_eq!(
        failed(&h.step(submit(put(9, b"z", b"9")))),
        ErrorKind::ProtectionPaused
    );

    // A well-formed row that names the generation being served is not T1's either: no
    // instance has written in it yet.
    let mut h = H::live();
    let _ = h.resolve(put(1, b"a", b"1"));
    h.snap.dedup.insert(
        dedup_key(Generation(8), AFF, identity(2)).to_vec(),
        dedup_value(Digest::ROOT, Seq(1), OwnerEpoch(1)),
    );
    let _ = h.step(recovered(Generation(8), 1, NODE_A, PartitionMode::Active));
    assert!(h.k().seed_pending().is_some());
    // One generation older is T1's, and loads.
    h.snap.dedup.clear();
    h.snap.dedup.insert(
        dedup_key(GEN, AFF, identity(2)).to_vec(),
        dedup_value(Digest::ROOT, Seq(1), OwnerEpoch(1)),
    );
    let _ = h.step(admission(true, None));
    assert_eq!(h.k().seed_pending(), None);
    assert!(h
        .k()
        .dedup()
        .older(Generation(8), AFF, identity(2))
        .is_some());
}

/// The load pages through every durable row, not just the first `SEED_PAGE`.
#[retcd_test]
fn a_seed_pages_through_every_durable_record() {
    let mut h = H::live();
    let rows = SEED_PAGE + SEED_PAGE / 2;
    for request in 1..=rows as u64 {
        h.snap.dedup.insert(
            dedup_key(GEN, AFF, identity(request)).to_vec(),
            dedup_value(Digest::ROOT, Seq(1), OwnerEpoch(1)),
        );
    }
    h.snap.at = Seq(1);
    let _ = h.step(recovered(Generation(8), 1, NODE_B, PartitionMode::Active));
    h.node = NODE_B;
    let _ = h.step(recovered(Generation(8), 1, NODE_B, PartitionMode::Active));
    assert_eq!(h.k().dedup().len(), rows);
    let _ = h.step(admission(true, None));
    let last = u64::try_from(rows).expect("small");
    assert_eq!(
        failed(&h.step(submit(put(last, b"a", b"1")))),
        ErrorKind::RequestIdReuse,
        "the last row was loaded (its digest differs from this payload's)"
    );
}

#[retcd_test]
fn dedup_trim_is_generation_qualified_and_the_watermark_only_rises() {
    let mut h = H::live();
    let _ = h.resolve(put(1, b"a", b"1"));
    let _ = h.resolve(put(2, b"b", b"2"));
    let trim = |below| {
        kernel(KernelEvent::DedupTrim {
            generation: GEN,
            below: Seq(below),
        })
    };
    // Another generation's trim touches nothing here.
    let _ = h.step(kernel(KernelEvent::DedupTrim {
        generation: Generation(6),
        below: Seq(99),
    }));
    assert_eq!(h.k().dedup().len(), 2);
    assert_eq!(h.step(trim(2)), vec![]);
    assert_eq!(h.k().dedup().len(), 1, "seq 1 dropped, seq 2 kept");
    assert_eq!(h.k().dedup().retained_from_seq(GEN), Some(Seq(2)));
    let _ = h.step(trim(1));
    assert_eq!(h.k().dedup().retained_from_seq(GEN), Some(Seq(2)));
    // The trimmed identity executes fresh; the kept one still replays.
    assert!(matches!(
        h.step(submit(put(2, b"b", b"2"))).as_slice(),
        [EffectKind::Reply(ReplyEffect::Transaction { .. })]
    ));
    let _ = h.admit(put(1, b"a", b"1"));
}

// ---------------------------------------------------------------------------------------------
// A-R71: R8, R9, and tester-t1's advisory hunts as rows
// ---------------------------------------------------------------------------------------------

fn retire(generation: u64) -> Event {
    kernel(KernelEvent::RetireGeneration {
        generation: Generation(generation),
    })
}

/// `effects` is exactly one `StorageDispatch` check; returns its correlation.
fn only_check(effects: &[EffectKind]) -> CorrelationId {
    let [EffectKind::Kernel(KernelEffect::AuthorityCheck {
        checkpoint: Checkpoint::StorageDispatch,
        correlation,
        ..
    })] = effects
    else {
        panic!("expected exactly one StorageDispatch check, got {effects:?}");
    };
    *correlation
}

/// Each reply in `effects` as `(request, error kind)`, in order. Panics on any other effect.
fn replies(effects: &[EffectKind]) -> Vec<(u64, ErrorKind)> {
    effects
        .iter()
        .map(|effect| match effect {
            EffectKind::Reply(ReplyEffect::Failed { identity, error }) => {
                (identity.request.0, error.kind())
            }
            other => panic!("expected only Failed replies, got {other:?}"),
        })
        .collect()
}

/// A-R71, R9. A retire at or above the served generation is refused and retires nothing. At the
/// served one it would drop every retained identity (hunt_07). Above it, it would record a
/// generation this node may later serve as retaining nothing. The near miss: a generation below
/// the served one still retires.
#[retcd_test]
fn a_retire_at_or_above_the_served_generation_is_refused_and_retires_nothing() {
    let mut h = H::live();
    let _ = h.resolve(put(1, b"a", b"1"));
    assert_eq!(
        h.step(retire(9)),
        vec![ignored(AuthorityIgnoreReason::RetireNewerGeneration)]
    );
    assert!(!h.k().dedup().is_retired(Generation(9)));
    assert_eq!(
        h.step(retire(7)),
        vec![ignored(AuthorityIgnoreReason::RetireServedGeneration)]
    );
    assert!(!h.k().dedup().is_retired(GEN));
    assert_eq!(h.k().dedup().len(), 1);
    let _ = h.step(recovered(Generation(8), 1, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert_eq!(h.step(retire(7)), vec![], "below the served one: retired");
    assert!(h.k().dedup().is_retired(GEN));
    assert!(h.k().dedup().is_empty());
}

/// A-R71, R9's consequence. After a refused retire of g9, serving g9 still retains: a retry
/// there replays and is not executed a second time.
#[retcd_test]
fn a_refused_retire_of_a_newer_generation_leaves_it_retaining() {
    let mut h = H::live();
    let _ = h.step(retire(9));
    let _ = h.step(recovered(Generation(9), 0, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    let c = h.admit(put(1, b"a", b"1"));
    let effects = h.step(answer_in(Generation(9), c, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("expected g9's batch, got {effects:?}");
    };
    let (id, seq) = (batch.id, batch.seq);
    let Some(Inflight::Dispatched { record_digest, .. }) = h.k().inflight() else {
        panic!("expected Dispatched");
    };
    let record_digest = *record_digest;
    assert_eq!(h.step(committed(id, seq.0)).len(), 2);
    let publish = kernel(KernelEvent::Published {
        lineage: lineage_at(Generation(9)),
        seq,
        record_digest,
        request: identity(1),
    });
    assert_eq!(h.step(publish), vec![]);
    assert_eq!(h.k().dedup().len(), 1, "g9 retains");
    assert!(matches!(
        h.step(submit(put(1, b"a", b"1"))).as_slice(),
        [EffectKind::Reply(ReplyEffect::Transaction { .. })]
    ));
    assert_eq!(h.k().next_seq(), seq.next(), "replayed, not executed again");
}

/// A-R71, tester-t1 hunt_06. A condition failure reserves no sequence, so it is aged at
/// `next_seq`: the first sequence published after it. At `next_seq - 1` it was dropped when the
/// sequence published *before* it expired, which is early. (The trim boundary itself is
/// `dedup_trim_is_generation_qualified_and_the_watermark_only_rises`.)
#[retcd_test]
fn a_condition_failure_is_aged_at_the_next_sequence() {
    let mut h = H::live();
    let _ = h.resolve(put(1, b"a", b"1"));
    let mut cf = put(5, b"k", b"v");
    cf.conditions.push(Condition::Present { key: key(b"k") });
    assert_eq!(failed(&h.step(submit(cf))), ErrorKind::ConditionFailed);
    let trim = |below| {
        kernel(KernelEvent::DedupTrim {
            generation: GEN,
            below: Seq(below),
        })
    };
    assert_eq!(h.step(trim(2)), vec![]);
    assert_eq!(
        h.k().dedup().len(),
        1,
        "seq 1 expired; the condition failure came after it and is kept"
    );
    assert_eq!(h.step(trim(3)), vec![]);
    assert!(h.k().dedup().is_empty(), "dropped once seq 2's age is past");
}

/// A-R71, tester-t1 hunt_16. Under a freeze, a completion still gives `LocalApplied` before the
/// candidate. And storage's `applied` must be the batch's own sequence. A completion that reports
/// any other position is a storage fault, answered as a failed batch is: `UNKNOWN_OUTCOME`, the
/// partition fenced, no candidate. Before, the field was never read.
#[retcd_test]
fn a_completion_must_report_the_batchs_own_sequence() {
    let mut h = H::live();
    let (batch, _, seq) = h.dispatch(put(1, b"a", b"1"));
    let _ = h.step(fence(FenceScope::Node, DenyReason::Expired));
    let effects = h.step(committed(batch, seq));
    assert!(
        matches!(
            effects.as_slice(),
            [
                EffectKind::Kernel(KernelEffect::LocalApplied { seq: s, .. }),
                EffectKind::Kernel(KernelEffect::AppliedCandidate(_))
            ] if *s == Seq(seq)
        ),
        "{effects:?}"
    );
    for off_by in [1, -1] {
        let mut h = H::live();
        let (batch, _, seq) = h.dispatch(put(1, b"a", b"1"));
        assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![]);
        let applied = seq.saturating_add_signed(off_by);
        assert_eq!(
            replies(&h.step(committed(batch, applied))),
            vec![
                (1, ErrorKind::UnknownOutcome),
                (2, ErrorKind::ProtectionPaused)
            ],
            "applied {applied} for seq {seq}"
        );
        assert_eq!(
            h.k().mode(),
            &QueueMode::Frozen {
                cause: FreezeCause::LocalStorageFenced,
                unresolved: Some(Seq(seq))
            }
        );
    }
}

/// A-R71, tester-t1 hunt_17. One pump answers several heads in queue order, in one step: a
/// replay, a retained condition failure, then a fresh dispatch check.
#[retcd_test]
fn one_pump_answers_replay_condition_failure_and_check_in_order() {
    let mut h = H::live();
    let mut cf = put(5, b"k", b"v");
    cf.conditions.push(Condition::Present { key: key(b"k") });
    assert_eq!(
        failed(&h.step(submit(cf.clone()))),
        ErrorKind::ConditionFailed
    );
    let (batch, digest, seq) = h.dispatch(put(1, b"a", b"1"));
    for queued in [put(1, b"a", b"1"), cf, put(6, b"c", b"3")] {
        assert_eq!(h.step(submit(queued)), vec![]);
    }
    assert_eq!(h.step(committed(batch, seq)).len(), 2);
    let effects = h.step(published(seq, digest, 1));
    let [EffectKind::Reply(ReplyEffect::Transaction {
        identity: first, ..
    }), EffectKind::Reply(ReplyEffect::Failed {
        identity: second,
        error: RdbError::ConditionFailed { index: 0 },
    }), EffectKind::Kernel(KernelEffect::AuthorityCheck { .. })] = effects.as_slice()
    else {
        panic!("expected replay, condition failure, check; got {effects:?}");
    };
    assert_eq!((*first, *second), (identity(1), identity(5)));
    assert_eq!(h.k().queue_len(), 0);
}

/// A-R71, tester-t1 hunt_18, pinned as designed. Check 9 counts the queue behind the one in
/// flight, exactly. It counts the dedup index as it stands at admission, so that cap is soft:
/// requests admitted before the index filled still retain, and it overruns by at most what was
/// already queued. Check 9 precedes step 11 (the order is normative), so a full index refuses
/// even a retry that would only replay. `OVERLOADED` is a definitive non-admission, and the
/// retry replays once a trim makes room.
#[retcd_test]
fn the_queue_cap_is_exact_and_the_dedup_cap_is_soft() {
    let mut h = H::live_with(Limits {
        queue_cap: 2,
        dedup_cap: 2,
        ..Limits::default()
    });
    let mut next = h.admit(put(1, b"a", b"1"));
    assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![]);
    assert_eq!(h.step(submit(put(3, b"c", b"3"))), vec![]);
    assert_eq!(
        failed(&h.step(submit(put(4, b"d", b"4")))),
        ErrorKind::Overloaded
    );
    for request in 1..=3 {
        let effects = h.step(answer(next, 1, Verdict::Admit));
        let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        let (id, seq) = (batch.id, batch.seq.0);
        let digest = h.k().prev_digest();
        assert_eq!(h.step(committed(id, seq)).len(), 2);
        let effects = h.step(published(seq, digest, request));
        if request < 3 {
            next = only_check(&effects);
        }
    }
    assert_eq!(
        h.k().dedup().len(),
        3,
        "cap 2, holds 3: all three were admitted below it"
    );
    assert_eq!(
        failed(&h.step(submit(put(1, b"a", b"1")))),
        ErrorKind::Overloaded,
        "a replay-only retry is refused at check 9"
    );
    let _ = h.step(kernel(KernelEvent::DedupTrim {
        generation: GEN,
        below: Seq(3),
    }));
    assert!(matches!(
        h.step(submit(put(3, b"c", b"3"))).as_slice(),
        [EffectKind::Reply(ReplyEffect::Transaction { .. })]
    ));
}

/// A-R71, tester-t1 hunt_19. Expired work is cancelled at storage dispatch (spec §5.2 step 3).
/// A request whose deadline passed while it waited is answered `DEADLINE_BEFORE_ADMISSION` when
/// its dispatch check admits: nothing is written, no sequence is used, and the queue moves on.
/// The boundary: one millisecond before the deadline still dispatches.
#[retcd_test]
fn expired_work_is_cancelled_at_storage_dispatch() {
    for (now, expired) in [(15, true), (14, false)] {
        let mut h = H::live();
        let (batch, digest, seq) = h.dispatch(put(1, b"a", b"1"));
        let mut short = put(2, b"b", b"2");
        short.remaining_millis = 5;
        assert_eq!(h.step(submit(short)), vec![], "admitted at 10, due at 15");
        assert_eq!(h.step(submit(put(3, b"c", b"3"))), vec![]);
        assert_eq!(h.step(committed(batch, seq)).len(), 2);
        let c2 = only_check(&h.step(published(seq, digest, 1)));
        h.now = now;
        let effects = h.step(answer(c2, 1, Verdict::Admit));
        if expired {
            let [EffectKind::Reply(ReplyEffect::Failed {
                identity: cancelled,
                error: RdbError::DeadlineBeforeAdmission { partition },
            }), EffectKind::Kernel(KernelEffect::AuthorityCheck { .. })] = effects.as_slice()
            else {
                panic!("expected the cancel, then request 3's check; got {effects:?}");
            };
            assert_eq!((*cancelled, *partition), (identity(2), PARTITION));
            assert_eq!(h.k().next_seq(), Seq(seq + 1), "no sequence used");
        } else {
            assert!(
                matches!(
                    effects.as_slice(),
                    [EffectKind::Store(StoreEffect::Commit(_))]
                ),
                "{effects:?}"
            );
        }
    }
}

/// A-R71, tester-t1 hunt_20. A failed batch fences the partition (spec §5.2 step 3), so the
/// requests queued behind it are answered at once, as a fence answers them. Nothing of theirs
/// was written, so `PROTECTION_PAUSED` is a definitive non-admission. Before, they waited
/// unanswered until A1's `Fence` arrived, and T1 emits no `FenceRequest` to make it come.
#[retcd_test]
fn a_failed_batch_answers_the_queue_behind_it() {
    let mut h = H::live();
    let (batch, _, _) = h.dispatch(put(1, b"a", b"1"));
    assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![]);
    assert_eq!(h.step(submit(put(3, b"c", b"3"))), vec![]);
    let effects = h.step(storage(StorageEvent::CommitFailed {
        batch,
        fault: StorageFault::WriteFailed,
    }));
    assert_eq!(
        replies(&effects),
        vec![
            (1, ErrorKind::UnknownOutcome),
            (2, ErrorKind::ProtectionPaused),
            (3, ErrorKind::ProtectionPaused)
        ]
    );
    assert_eq!(h.k().queue_len(), 0);
}

/// A-R71, tester-t1 hunt_21, pinned as designed (design §3.3: only `Recovered` reopens). A
/// freeze for lost authority holds for the rest of the generation. A fresh view does not reopen
/// it, and a `Recovered` no newer than the held lineage is ignored. A newer generation does.
#[retcd_test]
fn a_lost_authority_freeze_reopens_only_on_a_newer_generation() {
    let mut h = H::live();
    let _ = h.step(fence(FenceScope::Node, DenyReason::ClockSampleStale));
    let _ = h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(view(
        GEN,
        9,
        u64::MAX,
    )))));
    let _ = h.step(recovered(GEN, 0, NODE_A, PartitionMode::Active));
    assert_eq!(
        failed(&h.step(submit(put(1, b"a", b"1")))),
        ErrorKind::LeaseExpired
    );
    let _ = h.step(recovered(Generation(8), 0, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    let _ = h.admit(put(1, b"a", b"1"));
}

/// A-R71, tester-t1 hunt_23. Check 10's upper bounds: exactly `MAX_MUTATIONS` mutations and
/// exactly `MAX_CONDITIONS` conditions are admitted, and one more of either is
/// `INVALID_ARGUMENT` naming the field.
#[retcd_test]
fn check_ten_admits_each_upper_bound_and_refuses_one_more() {
    let mut h = H::live_with(Limits {
        queue_cap: 8,
        dedup_cap: 100,
        ..Limits::default()
    });
    let _ = h.admit(put(100, b"z", b"z"));
    let deletes = |n: usize| -> Vec<Mutation> {
        (0..n)
            .map(|i| Mutation::Delete {
                key: key(format!("m{i}").as_bytes()),
                expected_version: None,
            })
            .collect()
    };
    let absent = |n: usize| -> Vec<Condition> {
        (0..n)
            .map(|i| Condition::Absent {
                key: key(format!("c{i}").as_bytes()),
            })
            .collect()
    };
    let mut at = put(1, b"k", b"v");
    at.mutations = deletes(MAX_MUTATIONS);
    assert_eq!(h.step(submit(at)), vec![], "queued");
    let mut over = put(2, b"k", b"v");
    over.mutations = deletes(MAX_MUTATIONS + 1);
    assert_eq!(
        h.step(submit(over)),
        fail(2, RdbError::InvalidArgument { field: "mutations" })
    );
    let mut at = put(3, b"k", b"v");
    at.conditions = absent(MAX_CONDITIONS);
    assert_eq!(h.step(submit(at)), vec![], "queued");
    let mut over = put(4, b"k", b"v");
    over.conditions = absent(MAX_CONDITIONS + 1);
    assert_eq!(
        h.step(submit(over)),
        fail(
            4,
            RdbError::InvalidArgument {
                field: "conditions"
            }
        )
    );
}

// ---------------------------------------------------------------------------------------------
// A-R73: id exhaustion, the generation byte, the generation floor, remembered trims
// ---------------------------------------------------------------------------------------------

/// Limits with every bound at its default except the id counter's.
fn ids_spent_after(id_counter_max: u64) -> Limits {
    Limits {
        id_counter_max,
        ..Limits::default()
    }
}

fn push_view(authority_seq: u64) -> Event {
    kernel(KernelEvent::Authority(AuthorityEvent::View(view(
        GEN,
        authority_seq,
        u64::MAX,
    ))))
}

/// A-R73 item 3 (was A-R71 R8; kills R70-counter-unbounded and tester-t1 N10). With the counter
/// bounded at 2, exactly two correlations are minted, each one a dispatch. After that a new
/// identity is `OVERLOADED` at step 14, a retry of an applied identity still replays (step 11
/// runs first), and nothing is ever given an id twice.
#[retcd_test]
fn at_id_counter_exhaustion_a_new_identity_is_overloaded_and_a_retry_replays() {
    let mut h = H::live_with(ids_spent_after(2));
    let c1 = h.admit(put(1, b"a", b"1"));
    assert_eq!(c1.0 & ID_COUNTER_MAX, 1);
    let effects = h.step(answer(c1, 1, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("{effects:?}");
    };
    let (id, seq, digest) = (batch.id, batch.seq.0, h.k().prev_digest());
    assert_eq!(h.step(committed(id, seq)).len(), 2);
    assert_eq!(h.step(published(seq, digest, 1)), vec![]);
    let (_, last) = h.resolve(put(2, b"b", b"2"));
    assert_eq!(
        last, 2,
        "the second correlation, the bound itself, still dispatches"
    );
    assert_eq!(
        h.step(submit(put(3, b"c", b"3"))),
        fail(
            3,
            RdbError::Overloaded {
                partition: PARTITION
            }
        )
    );
    assert!(matches!(
        h.step(submit(put(2, b"b", b"2"))).as_slice(),
        [EffectKind::Reply(ReplyEffect::Transaction { .. })]
    ));
    assert_eq!(h.k().next_seq(), Seq(3));
    assert_eq!(h.k().inflight(), None);
}

/// A-R73 item 3 (tester-t1's hunt_22 wedge at the bound). The re-ask that would answer the
/// outstanding check cannot mint an id, so the request is refused `OVERLOADED` and the queue
/// behind it is answered too. Before A-R73 the stale answer re-asked nothing and both requests
/// waited for good. The near miss: one id left still re-asks.
#[retcd_test]
fn at_id_counter_exhaustion_an_outstanding_check_is_refused_not_wedged() {
    let mut h = H::live_with(ids_spent_after(2));
    let c1 = h.admit(put(1, b"a", b"1"));
    assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![]);
    let _ = h.step(push_view(2));
    let c2 = reasked(&h.step(answer(c1, 1, Verdict::Admit)));
    assert_ne!(c1, c2);
    let _ = h.step(push_view(3));
    let effects = h.step(answer(c2, 2, Verdict::Admit));
    let [stale, rest @ ..] = effects.as_slice() else {
        panic!("{effects:?}");
    };
    assert_eq!(*stale, ignored(AuthorityIgnoreReason::StaleAuthorityAnswer));
    assert_eq!(
        replies(rest),
        vec![(1, ErrorKind::Overloaded), (2, ErrorKind::Overloaded)]
    );
    assert_eq!((h.k().inflight(), h.k().queue_len()), (None, 0));
    assert_eq!(h.k().next_seq(), Seq(1), "nothing was dispatched");
}

/// A-R73 item 3, with tester-t1's attack_c1. A re-ask never outlives the request's deadline:
/// at the deadline the stale answer refuses it `DEADLINE_BEFORE_ADMISSION`, as dispatch would
/// (A-R71, hunt_19). The near miss: one millisecond earlier it is asked again.
#[retcd_test]
fn a_stale_answer_past_the_deadline_refuses_rather_than_re_asks() {
    let mut h = H::live();
    let c1 = h.admit(put(1, b"a", b"1"));
    let _ = h.step(push_view(2));
    h.now = 10 + 1_000 - 1;
    let c2 = reasked(&h.step(answer(c1, 1, Verdict::Admit)));
    let _ = h.step(push_view(3));
    h.now = 10 + 1_000;
    let effects = h.step(answer(c2, 2, Verdict::Admit));
    let [stale, rest @ ..] = effects.as_slice() else {
        panic!("{effects:?}");
    };
    assert_eq!(*stale, ignored(AuthorityIgnoreReason::StaleAuthorityAnswer));
    assert_eq!(replies(rest), vec![(1, ErrorKind::DeadlineBeforeAdmission)]);
    assert_eq!(h.k().inflight(), None);
}

/// A-R73 item 4 (kills tester-t1 N11). The whole generation byte is in the id: an instance at
/// generation 135 shares no id with the one at 7 (135 mod 128), so neither 7's late answer nor
/// its late completion is taken as 135's.
#[retcd_test]
fn an_instance_at_generation_128_or_more_shares_no_id_with_generation_less_128() {
    let mut h = H::live();
    let c7 = h.admit(put(1, b"a", b"1"));
    let effects = h.step(answer(c7, 1, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("{effects:?}");
    };
    let b7 = batch.id;
    let g135 = Generation(135);
    let _ = h.step(recovered(g135, 0, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    let c135 = h.admit(put(2, b"b", b"2"));
    assert_eq!((c135.0 >> 40) & 0xFF, 135, "the generation byte, unmasked");
    assert_ne!(c7, c135);
    assert_eq!(
        h.step(answer(c7, 1, Verdict::Admit)),
        vec![ignored(AuthorityIgnoreReason::StaleAuthorityAnswer)]
    );
    let effects = h.step(answer_in(g135, c135, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("{effects:?}");
    };
    assert_ne!(b7, batch.id);
    assert_eq!(
        h.step(committed(b7, 1)),
        vec![ignored(AuthorityIgnoreReason::UnmatchedCompletion)]
    );
}

fn not_newer() -> EffectKind {
    ignored(AuthorityIgnoreReason::RecoveredGenerationNotNewer)
}

fn not_required() -> EffectKind {
    EffectKind::Kernel(KernelEffect::Ignored {
        reason: KernelIgnoredReason::Replica(ReplicaIgnoreReason::NotRequired),
    })
}

/// F1's re-emit of `event`'s result at a later activation revision (B-R56, T-B-03).
fn at_revision(mut event: Event, revision: u64) -> Event {
    if let EventKind::Kernel(KernelEvent::Recovered(result)) = &mut event.kind {
        result.committed.revision = Revision(revision);
    }
    event
}

/// A-R73 N2, as refined by A-R73a (tester-t1 attack_b1). Demoted from g7 by g8, the node's
/// floor is 8. A late `Recovered{g7}` and a late `Recovered{g8}` naming it primary both create
/// nothing. Taking the g7 one would re-create an instance whose ids equal the demoted one's.
/// The near miss: `Recovered{g9}` acts.
#[retcd_test]
fn a_recovered_at_or_below_a_generation_this_node_was_demoted_from_is_ignored() {
    let mut h = H::live();
    let _ = h.admit(put(1, b"a", b"1"));
    let g8 = Generation(8);
    let _ = h.step(recovered(g8, 0, NodeId(2), PartitionMode::Active));
    assert!(h.t1.kernel(NODE_A, PARTITION).is_none());
    assert_eq!(
        h.step(recovered(GEN, 0, NODE_A, PartitionMode::Active)),
        vec![not_newer()]
    );
    assert_eq!(
        h.step(recovered(g8, 0, NODE_A, PartitionMode::Active)),
        vec![not_newer()]
    );
    assert!(h.t1.kernel(NODE_A, PARTITION).is_none());

    assert_eq!(
        h.step(recovered(Generation(9), 0, NODE_A, PartitionMode::Active)),
        vec![]
    );
    assert_eq!(h.k().lineage().generation, Generation(9));
}

/// A-R73a row 2, the mirror of N2. Serving g8, a late `Recovered{g7}` naming another primary
/// demotes nothing: g8 keeps serving and its queue is untouched.
#[retcd_test]
fn a_late_recovered_for_an_older_generation_demotes_nothing() {
    let mut h = H::live();
    let g8 = Generation(8);
    let _ = h.step(recovered(g8, 0, NODE_A, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    let _ = h.admit(put(1, b"a", b"1"));
    assert_eq!(
        h.step(recovered(GEN, 0, NodeId(2), PartitionMode::Active)),
        vec![not_newer()]
    );
    assert_eq!(h.k().lineage().generation, g8);
    assert!(matches!(
        h.k().inflight(),
        Some(Inflight::AwaitingDispatchCheck { .. })
    ));
}

/// A-R73a rows 3 and (a). Recovered read-only at g8, F1's activation re-emits g8 as `Active`
/// at a later revision, and T1 lifts `RECOVERY_READ_ONLY` in place: same instance, same ids.
/// Near misses: an identical read-only re-delivery before the activation lifts nothing, and a
/// read-only re-emit after it re-freezes nothing (a live generation is paused by L1, not by a
/// `Recovered`).
#[retcd_test]
fn the_activation_re_emit_lifts_read_only_in_place_and_nothing_re_freezes_it() {
    let mut h = H::live();
    let g8 = Generation(8);
    let read_only = recovered(g8, 0, NODE_A, PartitionMode::ReadOnly);
    let _ = h.step(recovered(g8, 0, NODE_A, PartitionMode::ReadOnly));
    let _ = h.step(admission(true, None));
    let refused = RdbError::RecoveryReadOnly {
        partition: PARTITION,
        generation: g8,
    };
    assert_eq!(h.step(submit(put(1, b"a", b"1"))), fail(1, refused.clone()));
    assert_eq!(h.step(read_only), vec![not_required()]);
    assert_eq!(h.step(submit(put(1, b"a", b"1"))), fail(1, refused));

    let active = at_revision(recovered(g8, 0, NODE_A, PartitionMode::Active), 3);
    assert_eq!(h.step(active), vec![]);
    assert_eq!(*h.k().mode(), QueueMode::Open);
    let c = h.admit(put(1, b"a", b"1"));
    assert_eq!(
        c.0 & ID_COUNTER_MAX,
        1,
        "the same instance: its ids were not reset"
    );

    let again = at_revision(recovered(g8, 0, NODE_A, PartitionMode::ReadOnly), 4);
    assert_eq!(h.step(again), vec![not_required()]);
    assert_eq!(*h.k().mode(), QueueMode::Open);
}

/// A-R73a: an activation re-emit that comes back at reduced redundancy still serves writes, so
/// `DegradedRf2` lifts read-only the same way `Active` does.
#[retcd_test]
fn a_degraded_activation_re_emit_also_lifts_read_only() {
    let mut h = H::live();
    let g8 = Generation(8);
    let _ = h.step(recovered(g8, 0, NODE_A, PartitionMode::ReadOnly));
    let _ = h.step(admission(true, None));
    assert!(matches!(
        h.k().mode(),
        QueueMode::Frozen {
            cause: FreezeCause::RecoveryReadOnly,
            ..
        }
    ));
    let degraded = at_revision(recovered(g8, 0, NODE_A, PartitionMode::DegradedRf2), 3);
    assert_eq!(h.step(degraded), vec![]);
    assert_eq!(*h.k().mode(), QueueMode::Open);
    let _ = h.admit(put(1, b"a", b"1"));
}

/// A-R73a (b). An activation re-emit lifts only `RECOVERY_READ_ONLY`: an instance frozen
/// because it lost authority stays frozen, and only a newer generation reopens it.
#[retcd_test]
fn the_activation_re_emit_lifts_no_other_freeze() {
    let mut h = H::live();
    let _ = h.step(fence(FenceScope::Node, DenyReason::Expired));
    let before = *h.k().mode();
    assert!(matches!(before, QueueMode::Frozen { .. }));
    assert_ne!(
        before,
        QueueMode::Frozen {
            cause: FreezeCause::RecoveryReadOnly,
            unresolved: None
        }
    );
    let active = at_revision(recovered(GEN, 0, NODE_A, PartitionMode::Active), 3);
    assert_eq!(h.step(active), vec![not_required()]);
    assert_eq!(*h.k().mode(), before);
}

fn trim(generation: Generation, below: u64) -> Event {
    kernel(KernelEvent::DedupTrim {
        generation,
        below: Seq(below),
    })
}

fn not_primary() -> EffectKind {
    EffectKind::Kernel(KernelEffect::Ignored {
        reason: KernelIgnoredReason::Error(ErrorKind::NotPrimary),
    })
}

/// Which of `requests` the stepping node's index holds for generation 7.
fn holds(h: &H, requests: &[u64]) -> Vec<bool> {
    requests
        .iter()
        .map(|r| h.k().dedup().get(GEN, AFF, identity(*r)).is_some())
        .collect()
}

/// A-R73 N1 as ruled in A-R73a (tester-t1 attack_a4, the equality case). The trim reaches both
/// nodes, as a partition-scoped trim would. B has no instance, so it answers `NOT_PRIMARY` as
/// before, but it remembers the trim. When B takes over, its seed holds exactly what A held
/// after the trim, so the dedup cap A was admitting under does not refuse B's new identities.
/// The near miss: a retry of the identity both retained is still answered from it.
#[retcd_test]
fn a_failovers_seed_never_refuses_new_identities_for_history_already_trimmed() {
    let mut h = H::live_with(Limits {
        queue_cap: 8,
        dedup_cap: 3,
        ..Limits::default()
    });
    for r in 1..=3 {
        let _ = h.resolve(put(r, b"a", b"1"));
    }
    assert_eq!(h.step(trim(GEN, 3)), vec![]);
    h.node = NODE_B;
    assert_eq!(h.step(trim(GEN, 3)), vec![not_primary()]);
    h.node = NODE_A;
    let a_held = holds(&h, &[1, 2, 3]);
    assert_eq!(a_held, [false, false, true]);
    let _ = h.admit(put(4, b"d", b"4"));
    let _ = h.step(fence(FenceScope::Node, DenyReason::Expired));

    h.node = NODE_B;
    let _ = h.step(recovered(Generation(8), 3, NODE_B, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert_eq!(h.k().seed_pending(), None);
    assert_eq!(holds(&h, &[1, 2, 3]), a_held, "B holds what A held");
    assert_eq!(
        h.step(submit(put(3, b"a", b"1"))),
        fail(3, gen_changed(7, 8))
    );
    let _ = h.admit(put(5, b"e", b"5"));
}

/// A-R73a: tester-t1's attack_a4 as ruled. The trim reached A only, so B's seed holds a
/// superset of A's index: it retains what A trimmed, which costs at most an earlier
/// `OVERLOADED`. A retry of an identity A trimmed is still answered from the durable record on
/// B, never executed a second time.
#[retcd_test]
fn a_trim_that_never_reached_the_new_primary_leaves_it_a_superset_and_nothing_runs_twice() {
    let mut h = H::live();
    for r in 1..=3 {
        let _ = h.resolve(put(r, b"a", b"1"));
    }
    let _ = h.step(trim(GEN, 4));
    assert!(h.k().dedup().is_empty());

    h.node = NODE_B;
    let _ = h.step(recovered(Generation(8), 3, NODE_B, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert_eq!(holds(&h, &[1, 2, 3]), [true, true, true]);
    assert_eq!(
        h.step(submit(put(1, b"a", b"1"))),
        fail(1, gen_changed(7, 8))
    );
    assert_eq!(h.k().next_seq(), Seq(4), "nothing ran");
}

/// A-R73a. Remembered trims are in memory only. B applies the trim it was told of, crashes, and
/// recovers again: the re-seed holds every durable row again, a superset, and a retry of an
/// identity trimmed before the crash is answered from it rather than executed.
#[retcd_test]
fn after_a_crash_the_re_seed_retains_a_superset_and_a_trimmed_retry_is_still_deduped() {
    let mut h = H::live();
    for r in 1..=3 {
        let _ = h.resolve(put(r, b"a", b"1"));
    }
    let _ = h.step(trim(GEN, 4));
    h.node = NODE_B;
    assert_eq!(h.step(trim(GEN, 4)), vec![not_primary()]);
    let _ = h.step(recovered(Generation(8), 3, NODE_B, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert!(h.k().dedup().is_empty(), "B applied the trim it remembered");

    h.t1 = Transaction::new();
    h.boot = BootId(2);
    let _ = h.step(recovered(Generation(9), 3, NODE_B, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    assert_eq!(holds(&h, &[1, 2, 3]), [true, true, true]);
    assert_eq!(
        h.step(submit(put(1, b"a", b"1"))),
        fail(1, gen_changed(7, 9))
    );
    assert_eq!(h.k().next_seq(), Seq(4), "nothing ran");
}

/// A-R73a. Trims and retires delivered out of order to a node with no instance. A watermark
/// only rises: the later, lower trim changes nothing. A retire is remembered only for a
/// generation behind this node's floor (here 7, from the `Recovered` naming A): 6 is retired
/// when B takes over, while 7, being served when it arrived, and 8 are not (A-R71, R9).
#[retcd_test]
fn remembered_trims_only_rise_and_retire_only_behind_the_floor_in_any_order() {
    let mut h = H::live();
    for r in 1..=3 {
        let _ = h.resolve(put(r, b"a", b"1"));
    }
    let g6 = Generation(6);
    h.snap.dedup.insert(
        dedup_key(g6, AFF, identity(50)).to_vec(),
        dedup_value(Digest::ROOT, Seq(1), OwnerEpoch(1)),
    );

    h.node = NODE_B;
    // Before B knows any floor, a retire of 7 is not remembered: 7 may be what B serves next.
    assert_eq!(h.step(retire(7)), vec![not_primary()]);
    assert_eq!(
        h.step(recovered(GEN, 0, NODE_A, PartitionMode::Active)),
        vec![not_primary()]
    );
    for event in [trim(GEN, 3), trim(GEN, 2), retire(7), retire(6), retire(8)] {
        assert_eq!(h.step(event), vec![not_primary()]);
    }
    let g8 = Generation(8);
    let _ = h.step(recovered(g8, 3, NODE_B, PartitionMode::Active));
    let _ = h.step(admission(true, None));
    let dedup = h.k().dedup();
    assert_eq!(dedup.retained_from_seq(GEN), Some(Seq(3)));
    assert_eq!(holds(&h, &[1, 2, 3]), [false, false, true]);
    assert!(dedup.get(g6, AFF, identity(50)).is_none());
    assert_eq!(
        (
            dedup.is_retired(g6),
            dedup.is_retired(GEN),
            dedup.is_retired(g8)
        ),
        (true, false, false)
    );
    assert_eq!(dedup.len(), 1);
}

/// Lead ruling A-R73b (S11). A retry after recovery gets one answer whether the next instance
/// is on the same node or another one: both start from the durable seed and remembered trims,
/// never from the old instance's memory. The applied request reconciles, and nothing applied
/// runs again. The condition failure was never durable, so it is evaluated fresh either way.
#[retcd_test]
fn a_retry_after_recovery_answers_the_same_on_the_same_node_and_after_failover() {
    let answers = |failover: bool| {
        let mut h = H::live();
        let _ = h.resolve(put(1, b"a", b"1"));
        let mut cf = put(5, b"k", b"v");
        cf.conditions.push(Condition::Present { key: key(b"k") });
        assert_eq!(
            failed(&h.step(submit(cf.clone()))),
            ErrorKind::ConditionFailed
        );
        let next = if failover { NODE_B } else { NODE_A };
        let _ = h.step(recovered(Generation(8), 1, next, PartitionMode::Active));
        if failover {
            h.node = NODE_B;
            let _ = h.step(recovered(Generation(8), 1, NODE_B, PartitionMode::Active));
        }
        let _ = h.step(admission(true, None));
        let applied = h.step(submit(put(1, b"a", b"1")));
        let condition_failed = h.step(submit(cf));
        (applied, condition_failed, h.k().next_seq())
    };
    let same_node = answers(false);
    assert_eq!(same_node, answers(true), "same node, then failover");
    assert_eq!(same_node.0, fail(1, gen_changed(7, 8)));
    assert_eq!(failed(&same_node.1), ErrorKind::ConditionFailed);
    assert_eq!(same_node.2, Seq(2), "nothing applied ran again");
}

// ---------------------------------------------------------------------------------------------
// Types and total maps
// ---------------------------------------------------------------------------------------------

/// M7A-84. Would stop compiling if `TxnRejection` grew a success variant: every reply T1
/// produces is a `TxnRejection`, and this `match` is exhaustive.
#[retcd_test]
fn m7a_84_local_apply_never_returns_success_type() {
    let rejection = TxnRejection::NotAdmitted(RdbError::Overloaded {
        partition: PARTITION,
    });
    let kind = match &rejection {
        TxnRejection::NotAdmitted(error) | TxnRejection::Ambiguous(error) => error.kind(),
    };
    assert_eq!(kind, ErrorKind::Overloaded);
}

/// The fields every mapped error in these rows names.
fn deny_at() -> DenyContext {
    DenyContext {
        partition: PARTITION,
        identity: identity(1),
        grant: GrantId(2),
        expected: GEN,
        current: Generation(8),
        paused_after: Seq(3),
    }
}

/// Every `DenyReason`, once each.
const EVERY_DENY_REASON: [DenyReason; 16] = [
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

/// §3.4 as the row states it, one arm per reason (KA-7). A reason added to the contract stops
/// this compiling until someone decides its code here, and must also join
/// [`EVERY_DENY_REASON`], whose length is part of its type.
fn expected_code(reason: DenyReason, boundary: Boundary) -> ErrorKind {
    match reason {
        DenyReason::GenerationChanged => ErrorKind::GenerationChanged,
        DenyReason::LocalStorageFenced => match boundary {
            Boundary::PreApply => ErrorKind::ProtectionPaused,
            Boundary::PostApply => ErrorKind::UnknownOutcome,
        },
        DenyReason::NoGrant
        | DenyReason::Frozen
        | DenyReason::Revoked
        | DenyReason::EpochRevoked
        | DenyReason::Expired
        | DenyReason::ExpiryUnproven
        | DenyReason::ClockUnbounded
        | DenyReason::ClockModeUnbounded
        | DenyReason::ClockSampleStale
        | DenyReason::ProcessSuspended
        | DenyReason::BootMismatch
        | DenyReason::AuthorityGenerationChanged
        | DenyReason::SelfFenced
        | DenyReason::ControlUnavailable => ErrorKind::LeaseExpired,
    }
}

/// M7A-71. The mapping is total over every `DenyReason` (16 landed; the plan's "15" predates
/// `ClockModeUnbounded`) at both boundaries, and checkpoint-sensitive only for
/// `LocalStorageFenced`: `PROTECTION_PAUSED` before apply, `UNKNOWN_OUTCOME` after. No arm
/// produces `DIVERGENCE_REQUIRES_OPERATOR`; that code reaches a client only through L1's
/// `AdmissionState.reason` (M7A-69 (c)). T1's `deny_error` and the contract's
/// `client_error_kind` agree everywhere (lead ruling A-R72a).
#[retcd_test]
fn m7a_71_deny_error_mapping_total_and_checkpoint_sensitive() {
    let at = deny_at();
    for (i, reason) in EVERY_DENY_REASON.iter().enumerate() {
        assert!(
            !EVERY_DENY_REASON[i + 1..].contains(reason),
            "{reason:?} listed twice"
        );
        for boundary in [Boundary::PreApply, Boundary::PostApply] {
            let kind = deny_error(*reason, boundary, &at).kind();
            assert_eq!(
                kind,
                expected_code(*reason, boundary),
                "{reason:?} at {boundary:?}"
            );
            assert_eq!(kind, reason.client_error_kind(boundary));
            assert_ne!(kind, ErrorKind::DivergenceRequiresOperator);
        }
    }
    assert_eq!(
        deny_error(DenyReason::GenerationChanged, Boundary::PreApply, &at),
        RdbError::GenerationChanged {
            expected: GEN,
            current: Generation(8)
        }
    );
    assert_eq!(
        deny_error(DenyReason::LocalStorageFenced, Boundary::PreApply, &at),
        RdbError::ProtectionPaused {
            partition: PARTITION,
            paused_after: Seq(3)
        }
    );
    assert_eq!(
        deny_error(DenyReason::LocalStorageFenced, Boundary::PostApply, &at),
        RdbError::UnknownOutcome {
            partition: PARTITION,
            identity: identity(1)
        }
    );
}

// ---------------------------------------------------------------------------------------------
// Retention across two generations (plan §4.4)
// ---------------------------------------------------------------------------------------------

/// Dispatch, apply and publish `req` in `generation`, which the stepping node serves.
fn resolve_in(h: &mut H, generation: Generation, req: TxnRequest) {
    let request = req.identity.request.0;
    let seq = h.k().next_seq();
    let correlation = h.admit(req);
    let effects = h.step(answer_in(generation, correlation, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("expected exactly one batch, got {effects:?}");
    };
    assert_eq!(batch.seq, seq);
    let batch = batch.id;
    let Some(Inflight::Dispatched { record_digest, .. }) = h.k().inflight() else {
        panic!("expected Dispatched");
    };
    let record_digest = *record_digest;
    assert_eq!(h.step(committed(batch, seq.0)).len(), 2);
    assert_eq!(
        h.step(kernel(KernelEvent::Published {
            lineage: lineage_at(generation),
            seq,
            record_digest,
            request: identity(request),
        })),
        vec![]
    );
}

/// Ten requests applied in g7 at seqs 1..=10; this node recovered into g8 cut at 10, which loads
/// them from the durable seed under g7's key (A-R73b); then five applied in g8 at seqs 11..=15.
/// Request `r` sits at seq `r`.
fn two_generations() -> H {
    let mut h = H::live();
    for r in 1..=10 {
        let _ = h.resolve(put(r, b"a", b"1"));
    }
    let g8 = Generation(8);
    assert_eq!(
        h.step(recovered(g8, 10, NODE_A, PartitionMode::Active)),
        vec![]
    );
    let _ = h.step(admission(true, None));
    assert_eq!(h.k().seed_pending(), None);
    for r in 11..=15 {
        resolve_in(&mut h, g8, put(r, b"a", b"1"));
    }
    assert_eq!(h.k().dedup().len(), 15);
    h
}

/// Whether the index holds each of `requests` under `generation`.
fn held_in(h: &H, generation: Generation, requests: std::ops::RangeInclusive<u64>) -> Vec<bool> {
    requests
        .map(|r| h.k().dedup().get(generation, AFF, identity(r)).is_some())
        .collect()
}

/// M7A-87. `DedupTrim{g7, below: 6}` drops g7's seqs 1..=5, keeps 6..=10, records g7's
/// watermark as 6, and leaves every g8 entry. The twin, one fact apart (the trim names g8,
/// below 13): g7 keeps all ten and has no watermark, g8 drops 11 and 12.
#[retcd_test]
fn m7a_87_dedup_trim_generation_qualified() {
    let g8 = Generation(8);
    let mut h = two_generations();
    assert_eq!(h.step(trim(GEN, 6)), vec![]);
    assert_eq!(held_in(&h, GEN, 1..=10), [[false; 5], [true; 5]].concat());
    assert_eq!(held_in(&h, g8, 11..=15), [true; 5]);
    assert_eq!(h.k().dedup().retained_from_seq(GEN), Some(Seq(6)));
    assert_eq!(h.k().dedup().retained_from_seq(g8), None);

    let mut h = two_generations();
    assert_eq!(h.step(trim(g8, 13)), vec![]);
    assert_eq!(held_in(&h, GEN, 1..=10), [true; 10]);
    assert_eq!(held_in(&h, g8, 11..=15), [false, false, true, true, true]);
    assert_eq!(h.k().dedup().retained_from_seq(GEN), None);
    assert_eq!(h.k().dedup().retained_from_seq(g8), Some(Seq(13)));
}

/// M7A-88. `RetireGeneration{g7}` while serving g8 removes every g7 entry, records g7 as
/// retired, and leaves g8's five untouched. The twin, one fact apart (it names the served g8):
/// refused, nothing removed, nothing retired (A-R71 R9).
#[retcd_test]
fn m7a_88_retire_generation_removes_and_marks_retired() {
    let g8 = Generation(8);
    let mut h = two_generations();
    assert_eq!(
        h.step(retire(8)),
        vec![ignored(AuthorityIgnoreReason::RetireServedGeneration)]
    );
    assert_eq!(h.k().dedup().len(), 15);
    assert_eq!(h.k().dedup().retired_generations().count(), 0);

    assert_eq!(h.step(retire(7)), vec![]);
    assert_eq!(held_in(&h, GEN, 1..=10), [false; 10]);
    assert_eq!(held_in(&h, g8, 11..=15), [true; 5]);
    assert!(h.k().dedup().is_retired(GEN));
    assert_eq!(
        h.k().dedup().retired_generations().collect::<Vec<_>>(),
        vec![GEN]
    );
    assert_eq!(h.k().dedup().len(), 5);
}

// ---------------------------------------------------------------------------------------------
// Freeze, recovery and publication (plan §8)
// ---------------------------------------------------------------------------------------------

/// A recovery into g8 cut at 5 with cut digest `digest`, pinning this node, in `mode`.
fn recovered_at_cut(mode: PartitionMode, digest: Digest) -> Event {
    let mut event = recovered(Generation(8), 5, NODE_A, mode);
    if let EventKind::Kernel(KernelEvent::Recovered(result)) = &mut event.kind {
        result.selected.cutoff_digest = digest;
    }
    event
}

/// The queue mode T1 takes up after a `Recovered` in `mode`: design §3.3's total match, stated
/// as its own exhaustive `match` (KA-7), so a fifth `PartitionMode` stops this compiling.
fn mode_after(mode: &PartitionMode) -> QueueMode {
    match mode {
        PartitionMode::Active | PartitionMode::DegradedRf2 => QueueMode::Open,
        PartitionMode::ReadOnly | PartitionMode::Blocked { .. } => QueueMode::Frozen {
            cause: FreezeCause::RecoveryReadOnly,
            unresolved: None,
        },
    }
}

/// M7A-160. From `Frozen{AuthorityLost(Expired)}`, a `Recovered` into g8 in each of the four
/// modes: `Active` and `DegradedRf2` open, `ReadOnly` and `Blocked` freeze read-only. Lineage,
/// `next_seq` and `prev_digest` come from the result's `selected.cutoff_seq` and
/// `selected.cutoff_digest`, never from the frozen instance. The entry g7 retained is still held
/// under g7's key: under A-R73b it is re-loaded from the durable seed, not carried over.
#[retcd_test]
fn m7a_160_t1_recovered_maps_four_partition_modes_totally() {
    let cut = Digest([7; 32]);
    let modes = [
        PartitionMode::Active,
        PartitionMode::DegradedRf2,
        PartitionMode::ReadOnly,
        PartitionMode::Blocked {
            reason: BlockReason::DivergenceRequiresOperator {
                diverged: vec![CopyId(2)],
            },
        },
    ];
    for mode in modes {
        let mut h = H::live();
        let _ = h.resolve(put(1, b"a", b"1"));
        let _ = h.step(fence(FenceScope::Node, DenyReason::Expired));
        assert_eq!(h.k().mode(), &lost_to_expiry());
        h.snap.at = Seq(5);
        assert_eq!(h.step(recovered_at_cut(mode.clone(), cut)), vec![]);
        assert_eq!(h.k().mode(), &mode_after(&mode), "{mode:?}");
        assert_eq!(h.k().lineage(), lineage_at(Generation(8)), "{mode:?}");
        assert_eq!(
            (h.k().next_seq(), h.k().prev_digest()),
            (Seq(6), cut),
            "{mode:?}"
        );
        assert_eq!(h.k().seed_pending(), None, "{mode:?}");
        assert!(
            h.k().dedup().get(GEN, AFF, identity(1)).is_some(),
            "{mode:?}: g7's entry under g7's key"
        );
    }
}

/// Live, R(1) dispatched and applied but not published, R(2) queued behind it. Returns the
/// harness, R(1)'s record digest, and its seq.
fn applied_unpublished() -> (H, Digest, u64) {
    let mut h = H::live();
    let (batch, digest, seq) = h.dispatch(put(1, b"a", b"1"));
    assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![]);
    assert_eq!(h.step(committed(batch, seq)).len(), 2);
    (h, digest, seq)
}

/// M7A-161. (a) `Frozen{UnresolvedTransaction, Some(s)}`: `Published{s}` retains R(1), reopens,
/// and pumps the queue (R(2)'s check is the step's only effect). (b) One fact apart, the cause is
/// `AuthorityLost(Expired)`: `Published{s}` still retains, answers `PublishedWhileFrozen`, clears
/// `unresolved`, keeps the cause, and the next `Submit` is `LEASE_EXPIRED`: the kept cause picks
/// the code.
#[retcd_test]
fn m7a_161_published_while_frozen_reopens_only_for_unresolved_transaction() {
    let (mut h, digest, seq) = applied_unpublished();
    assert_eq!(
        h.k().mode(),
        &QueueMode::Frozen {
            cause: FreezeCause::UnresolvedTransaction,
            unresolved: Some(Seq(seq))
        }
    );
    let _ = only_check(&h.step(published(seq, digest, 1)));
    assert_eq!(h.k().mode(), &QueueMode::Open);
    assert!(h.k().dedup().get(GEN, AFF, identity(1)).is_some());

    let (mut h, digest, seq) = applied_unpublished();
    let _ = h.step(fence(FenceScope::Node, DenyReason::Expired));
    assert_eq!(
        h.k().mode(),
        &QueueMode::Frozen {
            cause: FreezeCause::AuthorityLost(DenyReason::Expired),
            unresolved: Some(Seq(seq))
        }
    );
    assert_eq!(
        h.step(published(seq, digest, 1)),
        vec![ignored(AuthorityIgnoreReason::PublishedWhileFrozen)]
    );
    assert_eq!(h.k().mode(), &lost_to_expiry());
    assert!(h.k().dedup().get(GEN, AFF, identity(1)).is_some());
    assert_eq!(
        failed(&h.step(submit(put(3, b"c", b"3")))),
        ErrorKind::LeaseExpired
    );
}

/// One of the three events after a dispatched batch in M7A-168 and M7A-169.
#[derive(Debug, Clone, Copy)]
enum After {
    Freeze,
    Complete,
    Publish,
}

/// Live with R(1) dispatched at seq 1, then `events` in order. After each event the whole mode
/// is compared with `modes`, and R(1) becomes retained at exactly the `Publish`. Returns the
/// harness and the retained entry.
fn after_dispatch(events: [After; 3], modes: [QueueMode; 3]) -> (H, Retained) {
    let mut h = H::live();
    let (batch, digest, seq) = h.dispatch(put(1, b"a", b"1"));
    for (event, mode) in events.into_iter().zip(modes) {
        let held_before = h.k().dedup().get(GEN, AFF, identity(1)).is_some();
        let effects = match event {
            After::Freeze => h.step(fence(FenceScope::Node, DenyReason::Expired)),
            After::Complete => h.step(committed(batch, seq)),
            After::Publish => h.step(published(seq, digest, 1)),
        };
        assert_eq!(h.k().mode(), &mode, "after {event:?} in {events:?}");
        let held_after = h.k().dedup().get(GEN, AFF, identity(1)).is_some();
        match event {
            After::Publish => assert!(!held_before && held_after, "{events:?}"),
            After::Complete => {
                assert_eq!(effects.len(), 2, "LocalApplied and the candidate");
                assert_eq!(held_after, held_before);
            }
            After::Freeze => {
                assert_eq!(effects, vec![], "nothing waiting to answer");
                assert_eq!(held_after, held_before);
            }
        }
    }
    assert_eq!(h.k().dedup().len(), 1, "retained once");
    let retained = h
        .k()
        .dedup()
        .get(GEN, AFF, identity(1))
        .expect("retained")
        .clone();
    (h, retained)
}

fn frozen_with(cause: FreezeCause, unresolved: Option<u64>) -> QueueMode {
    QueueMode::Frozen {
        cause,
        unresolved: unresolved.map(Seq),
    }
}

const LOST: FreezeCause = FreezeCause::AuthorityLost(DenyReason::Expired);
const ORDER_A: [After; 3] = [After::Freeze, After::Complete, After::Publish];
const ORDER_B: [After; 3] = [After::Complete, After::Freeze, After::Publish];
const ORDER_C: [After; 3] = [After::Complete, After::Publish, After::Freeze];

fn modes_a() -> [QueueMode; 3] {
    [
        frozen_with(LOST, Some(1)),
        frozen_with(LOST, Some(1)),
        frozen_with(LOST, None),
    ]
}

fn modes_b() -> [QueueMode; 3] {
    [
        frozen_with(FreezeCause::UnresolvedTransaction, Some(1)),
        frozen_with(LOST, Some(1)),
        frozen_with(LOST, None),
    ]
}

fn modes_c() -> [QueueMode; 3] {
    [
        frozen_with(FreezeCause::UnresolvedTransaction, Some(1)),
        QueueMode::Open,
        frozen_with(LOST, None),
    ]
}

/// M7A-168. The three orders of freeze, completion and publication for one dispatched batch,
/// with the whole mode compared after every event. A: the completion under the freeze sets
/// `unresolved` and leaves the cause. B: the completion in `Open` writes both, and the freeze
/// then overwrites the cause and keeps `unresolved`. C: the publication reopens, and the freeze
/// then writes `unresolved: None`. The final mode is `Frozen{AuthorityLost(Expired), None}` in
/// all three, and R(1) is retained exactly at the publication in all three.
#[retcd_test]
fn m7a_168_freeze_keeps_its_cause_across_the_batch_completion_in_three_orderings() {
    for (events, modes) in [
        (ORDER_A, modes_a()),
        (ORDER_B, modes_b()),
        (ORDER_C, modes_c()),
    ] {
        let (h, _) = after_dispatch(events, modes);
        assert_eq!(h.k().mode(), &frozen_with(LOST, None), "{events:?}");
    }
}

/// M7A-169, as corrected by lead ruling A-R74. Orders A and B of M7A-168 retain the same entry.
/// While the freeze holds, a retry is refused at check 7 with `LEASE_EXPIRED`, before the dedup
/// lookup, and nothing is reserved. Lost authority never reopens in the same generation (A-R71
/// hunt_21); a `Recovered` into g8 does, and there the retry is reconciled, not replayed:
/// `GENERATION_CHANGED{7, 8}` (A-R68 Q1), nothing reserved. The twin, one fact apart (the batch
/// fails under the freeze): nothing is retained, the original is `UNKNOWN_OUTCOME`, and the
/// retry is refused with the freeze's `LEASE_EXPIRED`, leaving `next_seq` at `s + 1`.
#[retcd_test]
fn m7a_169_published_while_frozen_retains_dedup_in_both_orders() {
    let mut retained = Vec::new();
    for (events, modes) in [(ORDER_A, modes_a()), (ORDER_B, modes_b())] {
        let (mut h, entry) = after_dispatch(events, modes);
        retained.push(entry);
        let before = (h.k().next_seq(), h.k().prev_digest());
        assert_eq!(
            h.step(submit(put(1, b"a", b"1"))),
            fail(
                1,
                RdbError::LeaseExpired {
                    partition: PARTITION,
                    grant: GrantId(2)
                }
            ),
            "{events:?}"
        );
        assert_eq!((h.k().next_seq(), h.k().prev_digest()), before);
        let _ = h.step(recovered(Generation(8), 1, NODE_A, PartitionMode::Active));
        let _ = h.step(admission(true, None));
        assert_eq!(h.k().mode(), &QueueMode::Open);
        let before = h.k().next_seq();
        assert_eq!(
            h.step(submit(put(1, b"a", b"1"))),
            fail(1, gen_changed(7, 8)),
            "{events:?}"
        );
        assert_eq!(h.k().next_seq(), before);
    }
    assert_eq!(retained[0], retained[1], "orders A and B retain one entry");

    let mut h = H::live();
    let (batch, _, seq) = h.dispatch(put(1, b"a", b"1"));
    let _ = h.step(fence(FenceScope::Node, DenyReason::Expired));
    assert_eq!(
        failed(&h.step(storage(StorageEvent::CommitFailed {
            batch,
            fault: StorageFault::WriteFailed,
        }))),
        ErrorKind::UnknownOutcome
    );
    assert!(h.k().dedup().is_empty(), "nothing retained");
    assert_eq!(
        failed(&h.step(submit(put(1, b"a", b"1")))),
        ErrorKind::LeaseExpired
    );
    assert_eq!(h.k().next_seq(), Seq(seq + 1));
}

/// The T1 half of M7A-139, which is **not** that row (lead ruling A-R74, item 4). The row's P1
/// half (`UNKNOWN_OUTCOME` through P1's post-apply deadline, and P1's mode) needs T1 and P1 wired
/// in the sim, and a function named for the row would claim what it does not assert. A batch
/// dispatched before a node freeze stays in flight through it; its completion still gives the
/// candidate; `next_seq` has advanced; the completion keeps the freeze's cause (K-A-46); and the
/// next `Submit` is `LEASE_EXPIRED`, not `PROTECTION_PAUSED`.
fn the_t1_half_of_m7a_139() {
    let mut h = H::live();
    let (batch, _, seq) = h.dispatch(put(1, b"a", b"1"));
    assert_eq!(h.step(fence(FenceScope::Node, DenyReason::Expired)), vec![]);
    assert!(matches!(
        h.k().inflight(),
        Some(Inflight::Dispatched { .. })
    ));
    let effects = h.step(committed(batch, seq));
    assert!(
        matches!(
            effects.as_slice(),
            [
                EffectKind::Kernel(KernelEffect::LocalApplied { .. }),
                EffectKind::Kernel(KernelEffect::AppliedCandidate(candidate))
            ] if candidate.seq == Seq(seq)
        ),
        "{effects:?}"
    );
    assert_eq!(h.k().next_seq(), Seq(seq + 1));
    assert_eq!(h.k().mode(), &frozen_with(LOST, Some(seq)));
    assert_eq!(
        failed(&h.step(submit(put(2, b"b", b"2")))),
        ErrorKind::LeaseExpired
    );
}

/// Dev scaffolding that runs [`the_t1_half_of_m7a_139`]; it claims no row.
#[retcd_test]
fn a_freeze_keeps_a_dispatched_batch_and_its_cause_through_the_completion() {
    the_t1_half_of_m7a_139();
}

// ---------------------------------------------------------------------------------------------
// Rows that cross a seam: T1 beside a real A1 or a real P1, hand-wired, one event at a time
// ---------------------------------------------------------------------------------------------

/// `event` as `kind`, on node A and partition 1, under `correlation`.
fn on_node_a(now: u64, correlation: u64, kind: EventKind) -> Event {
    Event {
        id: EventId(now),
        at: Tick(now),
        node: NODE_A,
        boot: BootId(1),
        partition: PARTITION,
        correlation: CorrelationId(correlation),
        kind,
    }
}

/// A sample on M7A-143's authority clock (5 000 000 at tick 0, running with the tick), taken at
/// `at` with error `error`.
const fn a1_sample(at: u64, error: u64) -> ControlTime {
    ControlTime {
        estimate: Tick(5_000_000 + at),
        error_millis: error,
        bound_established: true,
        sampled_at: Tick(at),
    }
}

/// A real A1 on node A. Its views are what T1 is handed, so T1's boundary is judged against the
/// horizon A1 computes and not against a number a row wrote down.
struct A1 {
    a1: Authority,
    snap: Snap,
}

impl A1 {
    /// One step at `now`, holding `sample`.
    fn step(
        &mut self,
        now: u64,
        sample: ControlTime,
        correlation: u64,
        kind: EventKind,
    ) -> Vec<Effect> {
        let ctx = StepCtx {
            control_time: sample,
            ..ctx(now, NODE_A, &self.snap)
        };
        self.a1
            .step(&ctx, &on_node_a(now, correlation, kind))
            .expect("an A1 row")
    }

    /// An event that routes to nothing while `Held`: its step is the clock row's alone.
    fn progress(&mut self, now: u64, sample: ControlTime) -> Vec<Effect> {
        self.step(
            now,
            sample,
            now,
            EventKind::Control(ControlEvent::WatchProgress {
                prefix: ControlPrefix::Grants,
                revision: Revision(1),
            }),
        )
    }

    /// M7A-143 (a): acquired at tick 0 under `a1_sample(0, 20)` with the spec budgets, then
    /// partition 1 installed at tick 1 at T1's lineage (generation 7, epoch 1). Returns the
    /// install's view, which is `(2000, ClockSampleStale)`: the sample ages out first.
    fn serving() -> (Self, AuthorityView) {
        let mut a1 = Self {
            a1: Authority::new(),
            snap: Snap::default(),
        };
        let sample = a1_sample(0, 20);
        let due = EventKind::Timer(TimerFired {
            id: AuthorityTimer::Acquire.id(),
            version: a1.a1.timer_version(AuthorityTimer::Acquire),
            scheduled_at: Tick::ZERO,
        });
        let acquire = a1.step(0, sample, 1, due);
        let request = acquire
            .iter()
            .find_map(|effect| match &effect.kind {
                EffectKind::Control(ControlEffect::Cas { request, .. }) => Some(*request),
                _ => None,
            })
            .expect("M7A-143 fixture: the acquire CAS");
        let _ = a1.step(
            0,
            sample,
            1,
            EventKind::Control(ControlEvent::CasResult {
                request,
                key: ControlKey::Grant(NODE_A),
                outcome: CasOutcome::Committed(Revision(7)),
            }),
        );
        let record = PartitionRecord {
            partition: PARTITION,
            owner: NODE_A,
            generation: GEN,
            owner_epoch: OwnerEpoch(1),
            config_version: C1,
            lifecycle: PartitionLifecycle::Serving,
        };
        let effects = a1.step(
            1,
            sample,
            2,
            EventKind::Control(ControlEvent::FamilySnapshot {
                prefix: ControlPrefix::Partitions,
                snapshot_revision: Revision(10),
                records: vec![ControlRecord {
                    key: ControlKey::Partition(PARTITION),
                    revision: Revision(9),
                    value: record.encode(),
                }],
            }),
        );
        let [view] = a1_views(&effects)[..] else {
            panic!("the install publishes partition 1 once: {effects:?}");
        };
        assert_eq!(view.lineage, lineage(), "fixture: T1's lineage");
        (a1, view)
    }
}

/// Every view A1 published in `effects`, in order.
fn a1_views(effects: &[Effect]) -> Vec<AuthorityView> {
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

/// Every fence A1 raised in `effects`, in order.
fn a1_fences(effects: &[Effect]) -> Vec<(FenceScope, DenyReason)> {
    effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fence {
                scope,
                reason,
            })) => Some((*scope, *reason)),
            _ => None,
        })
        .collect()
}

fn push(view: AuthorityView) -> Event {
    kernel(KernelEvent::Authority(AuthorityEvent::View(view)))
}

/// A live T1 holding `view`: at its `valid_through_tick` a `Submit` is admitted (the step's only
/// effect is the dispatch check), and one tick later the next is refused in the same step with
/// `past_horizon`'s code and nothing sent to A1.
fn assert_boundary_at(view: AuthorityView, what: &str) {
    let mut h = H::live();
    assert_eq!(h.step(push(view)), vec![], "{what}: adopted");
    assert_boundary_in(&mut h, view, what);
}

/// [`assert_boundary_at`] on a T1 that already holds `view`.
fn assert_boundary_in(h: &mut H, view: AuthorityView, what: &str) {
    let through = view.valid_through_tick.0;
    h.now = through;
    let _ = h.admit(put(1, b"a", b"1"));
    h.now = through + 1;
    assert_eq!(
        h.step(submit(put(2, b"b", b"2"))),
        fail(
            2,
            deny_error(
                view.past_horizon,
                Boundary::PreApply,
                &h.k().deny_context(identity(2), GEN)
            )
        ),
        "{what}: refused at {}",
        through + 1
    );
    assert_eq!(h.k().queue_len(), 0, "{what}: not queued");
}

/// M7A-144. K-A-35; `design.md` §3.2's entry check `now <= view.valid_through_tick`, with
/// `view.past_horizon` as the reason; A-R16.
///
/// T1 holds the view A1 published in M7A-143 (a) — `(2000, ClockSampleStale)`, taken from a real
/// A1 rather than written down. A `Submit` at 2000 is admitted: the dispatch check is the step's
/// only effect. At 2001 the next is refused in that step `LEASE_EXPIRED` (§3.4 maps
/// `ClockSampleStale` there), and the reply is the step's only effect, so nothing goes to A1. One
/// fact between the two: the tick.
#[retcd_test]
fn m7a_144_admission_boundary_at_valid_through_tick() {
    let (_, view) = A1::serving();
    assert_eq!(
        (view.valid_through_tick, view.past_horizon),
        (Tick(2_000), DenyReason::ClockSampleStale),
        "M7A-143's view"
    );
    let mut h = H::live();
    assert_eq!(h.step(push(view)), vec![]);
    h.now = 2_000;
    let _ = h.admit(put(1, b"a", b"1"));
    h.now = 2_001;
    assert_eq!(
        h.step(submit(put(2, b"b", b"2"))),
        fail(
            2,
            RdbError::LeaseExpired {
                partition: PARTITION,
                grant: view.grant_id,
            }
        )
    );
    assert_eq!(h.k().queue_len(), 0);
}

/// M7A-146. ADR 0007 "Admission horizon follows the sample"; `design.md` §1.7 "a wider
/// `epsilon_ms` shortens `utc_horizon`".
///
/// A1 from M7A-144, `E = 5 003 000`. A sample at 1000 with ε 20 promises through 2879 (the
/// largest `a` with `a + 1 < 1880` is 1879, under the local window's 2899). The next tick's sample
/// has ε 90, and **on that step** A1 pushes a superseding view through 2809 (`a + 1 < 1809`):
/// smaller. T1's boundary follows each view to its tick, and one T1 that held the ε 20 view
/// adopts the ε 90 one at the same `authority_seq` and moves its boundary to 2809. Then a sample with ε 101, over the
/// 100 ms bound: `Fence{Node, ClockUnbounded}`, and the view it pushes is
/// `(fence_tick − 1, ClockUnbounded)` with `authority_seq` moved.
#[retcd_test]
fn m7a_146_admission_horizon_follows_the_sample() {
    let (mut a1, _) = A1::serving();

    let effects = a1.progress(1_000, a1_sample(1_000, 20));
    let [narrow] = a1_views(&effects)[..] else {
        panic!("ε 20: one view: {effects:?}");
    };
    assert_eq!(narrow.valid_through_tick, Tick(2_879), "ε 20");

    let effects = a1.progress(1_001, a1_sample(1_001, 90));
    let [wide] = a1_views(&effects)[..] else {
        panic!("ε 90: a superseding view on the sample's own step: {effects:?}");
    };
    assert_eq!(wide.valid_through_tick, Tick(2_809), "ε 90");
    assert!(wide.valid_through_tick < narrow.valid_through_tick);
    assert!(wide.authority_seq >= narrow.authority_seq, "it supersedes");

    assert_boundary_at(narrow, "ε 20");
    assert_boundary_at(wide, "ε 90");
    // One T1 through the move: it holds ε 20's view, adopts ε 90's (a sample does not bump
    // `authority_seq`, so an equal seq must replace), and its boundary moves to 2809.
    let mut h = H::live();
    assert_eq!(h.step(push(narrow)), vec![]);
    assert_eq!(
        h.step(push(wide)),
        vec![],
        "the superseding view is adopted"
    );
    assert_eq!(h.k().authority(), Some(&wide));
    assert_boundary_in(&mut h, wide, "ε 20 then ε 90");

    let effects = a1.progress(1_002, a1_sample(1_002, 101));
    assert_eq!(
        a1_fences(&effects),
        vec![(FenceScope::Node, DenyReason::ClockUnbounded)],
        "ε 101: {effects:?}"
    );
    let [fenced] = a1_views(&effects)[..] else {
        panic!("ε 101: one view: {effects:?}");
    };
    assert_eq!(
        (fenced.valid_through_tick, fenced.past_horizon),
        (Tick(1_001), DenyReason::ClockUnbounded)
    );
    assert!(
        fenced.authority_seq > wide.authority_seq,
        "the fence moves the seq"
    );
    assert_boundary_at(fenced, "ε 101");
}

/// M7A-147. `design.md` §3.3 and §4.2, the `AuthorityView` rows: `v.authority_seq < held seq ⇒
/// StaleAuthorityView`.
///
/// T1 and P1 each take a view at `authority_seq` 5 promising through tick 30, then one at 4
/// promising for ever. Both answer the second `Ignored(StaleAuthorityView)` (the landed spelling
/// of the plan's `Fact`) and still hold the seq-5 view. T1 judges a `Submit` against seq 5's
/// horizon: at 31 it is `LEASE_EXPIRED`, which seq 4's would have admitted, and at 30 it is
/// admitted.
#[retcd_test]
fn m7a_147_stale_authority_view_never_replaces_newer() {
    let newer = view(GEN, 5, 30);
    let older = view(GEN, 4, u64::MAX);
    let stale = vec![ignored(AuthorityIgnoreReason::StaleAuthorityView)];

    let mut h = H::live();
    assert_eq!(h.step(push(newer)), vec![], "T1");
    assert_eq!(h.step(push(older)), stale, "T1");
    assert_eq!(h.k().authority(), Some(&newer), "T1 keeps seq 5");
    h.now = 31;
    assert_eq!(
        failed(&h.step(submit(put(1, b"a", b"1")))),
        ErrorKind::LeaseExpired,
        "T1 judges against seq 5's horizon"
    );
    h.now = 30;
    let _ = h.admit(put(1, b"a", b"1"));

    let mut p1 = P1::installed();
    assert_eq!(p1.step(10, push(newer)), vec![], "P1");
    assert_eq!(p1.step(11, push(older)), stale, "P1");
    assert_eq!(p1.view().authority, Some(newer), "P1 keeps seq 5");
}

/// A real P1 on node A, serving T1's lineage from seq 0. No R1 view is scripted, so nothing it
/// holds ever qualifies or publishes.
struct P1 {
    p1: Publication,
    snap: Snap,
}

impl P1 {
    fn installed() -> Self {
        let mut p1 = Publication::new();
        let _ = p1.install(NODE_A, BootId(1), lineage(), Seq::ZERO);
        Self {
            p1,
            snap: Snap::default(),
        }
    }

    fn step(&mut self, now: u64, mut event: Event) -> Vec<EffectKind> {
        event.at = Tick(now);
        let ctx = ctx(now, NODE_A, &self.snap);
        self.p1
            .step(&ctx, &event)
            .expect("a P1 input")
            .into_iter()
            .map(|effect| effect.kind)
            .collect()
    }

    fn view(&self) -> rdb_core::publication::PubStateView {
        self.p1.view(NODE_A, PARTITION).expect("installed")
    }
}

/// Every reply in `effects` naming `request`.
fn replies_to(effects: &[EffectKind], request: u64) -> Vec<&ReplyEffect> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            EffectKind::Reply(
                reply @ (ReplyEffect::Failed { identity: who, .. }
                | ReplyEffect::Transaction { identity: who, .. }),
            ) if *who == identity(request) => Some(reply),
            _ => None,
        })
        .collect()
}

/// The kernel halves of M7A-139, hand-wired. **Claims no row** (lead ruling A-R76, 2026-09-27):
/// M7A-139 is sim class on purpose and stays owed, because this function passes whether or not
/// the production dispatcher hands T1's candidate to P1 — and that wiring is the open
/// kernel-a finding. What it pins is that each kernel does its part once the wiring exists.
///
/// T1 and P1 hand-wired: T1's candidate is delivered to P1 as the dispatcher would, and the node
/// fence reaches both. R(1)'s batch is dispatched before the `Freeze{AuthorityLost(Expired)}`.
/// T1 keeps it in flight through the freeze; `BatchCompleted{Ok}` gives the candidate and
/// advances `next_seq`; and after it T1's whole mode is `Frozen{AuthorityLost(Expired),
/// unresolved: Some(seq)}` — the completion set `unresolved` and did not overwrite the cause.
/// P1 takes the candidate while frozen and arms its deadline; at `PostApplyDeadline` R(1) is
/// answered `UNKNOWN_OUTCOME`, and that is the only answer R(1) ever gets — never a rejection.
/// After the deadline P1's mode is `Frozen{AuthorityLost(Expired)}`, and T1's next `Submit` is
/// `LEASE_EXPIRED`, not `PROTECTION_PAUSED`: the kept cause picks the code.
#[retcd_test]
fn t1_and_p1_hand_wired_keep_a_dispatched_batch_and_resolve_it_unknown() {
    let mut h = H::live();
    let mut p1 = P1::installed();
    let mut answers = Vec::new();

    let (batch, _, seq) = h.dispatch(put(1, b"a", b"1"));
    let freeze = fence(FenceScope::Node, DenyReason::Expired);
    answers.extend(h.step(freeze.clone()));
    answers.extend(p1.step(20, freeze));
    assert!(
        matches!(h.k().inflight(), Some(Inflight::Dispatched { .. })),
        "kept through the freeze: {:?}",
        h.k().inflight()
    );

    let effects = h.step(committed(batch, seq));
    let [EffectKind::Kernel(KernelEffect::LocalApplied { .. }), EffectKind::Kernel(KernelEffect::AppliedCandidate(candidate))] =
        effects.as_slice()
    else {
        panic!("the completion gives the candidate: {effects:?}");
    };
    assert_eq!(candidate.seq, Seq(seq));
    assert_eq!(h.k().next_seq(), Seq(seq + 1), "next_seq advanced");
    assert_eq!(
        h.k().mode(),
        &QueueMode::Frozen {
            cause: FreezeCause::AuthorityLost(DenyReason::Expired),
            unresolved: Some(Seq(seq)),
        },
        "the completion keeps the freeze's cause"
    );

    let effects = p1.step(30, kernel(KernelEvent::AppliedCandidate(candidate.clone())));
    let Some((version, at)) = effects.iter().find_map(|effect| match effect {
        EffectKind::Timer(TimerEffect::Arm { id, version, at })
            if *id == post_apply_timer(PARTITION) =>
        {
            Some((*version, *at))
        }
        _ => None,
    }) else {
        panic!("P1 arms its post-apply deadline: {effects:?}");
    };
    answers.extend(effects);

    let deadline = kernel(KernelEvent::DedupTrim {
        generation: GEN,
        below: Seq::ZERO,
    });
    let deadline = Event {
        kind: EventKind::Timer(TimerFired {
            id: post_apply_timer(PARTITION),
            version,
            scheduled_at: at,
        }),
        ..deadline
    };
    answers.extend(p1.step(at.0, deadline));

    assert_eq!(
        replies_to(&answers, 1),
        vec![&ReplyEffect::Failed {
            identity: identity(1),
            error: RdbError::UnknownOutcome {
                partition: PARTITION,
                identity: identity(1),
            },
        }],
        "UNKNOWN_OUTCOME through P1's deadline, and nothing else: {answers:?}"
    );
    assert_eq!(
        p1.view().mode,
        PubMode::Frozen {
            cause: FreezeCause::AuthorityLost(DenyReason::Expired),
        },
        "P1 keeps the cause"
    );
    assert_eq!(
        failed(&h.step(submit(put(2, b"b", b"2")))),
        ErrorKind::LeaseExpired,
        "the kept cause picks the code"
    );
}

// ---------------------------------------------------------------------------------------------
// §4 rows: the queue and the retained outcomes
// ---------------------------------------------------------------------------------------------

/// The request T1 has in flight, whichever half it is in.
fn in_flight(h: &H) -> RequestIdentity {
    match h.k().inflight() {
        Some(
            Inflight::AwaitingDispatchCheck { admitted, .. }
            | Inflight::Dispatched { admitted, .. },
        ) => admitted.req.identity,
        None => panic!("nothing in flight"),
    }
}

/// M7A-86. `design.md` §3.1's queue and one-in-flight rule; §2.5.
///
/// A, B and C are submitted in that order: A is admitted to its check, B and C are queued. Each
/// in turn is the one in flight, in submission order, and the next one's `StorageDispatch` check
/// is emitted only after the one ahead of it has completed — not at its `Admit`, and not at its
/// `BatchCompleted` (A's completes at tick 50), but at its publication, which is what ends
/// one-in-flight in the landed T1 (§3.3: the completion freezes `UnresolvedTransaction`, and
/// `Published` reopens and pumps; M7A-161 (a)). The queue shortens by one each time.
#[retcd_test]
fn m7a_86_one_in_flight_fifo_drains_one_at_a_time() {
    let mut h = H::live();
    let mut next = h.admit(put(1, b"a", b"1"));
    assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![]);
    assert_eq!(h.step(submit(put(3, b"c", b"3"))), vec![]);
    assert_eq!(h.k().queue_len(), 2);

    for request in 1..=3 {
        assert_eq!(in_flight(&h), identity(request), "FIFO");
        let effects = h.step(answer(next, 1, Verdict::Admit));
        let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
            panic!("R({request}): one batch, no check: {effects:?}");
        };
        let (id, seq) = (batch.id, batch.seq.0);
        let digest = h.k().prev_digest();
        h.now = 50 * request;
        let effects = h.step(committed(id, seq));
        assert!(
            !effects.iter().any(|effect| matches!(
                effect,
                EffectKind::Kernel(KernelEffect::AuthorityCheck { .. })
            )),
            "R({request}): no check at the completion: {effects:?}"
        );
        assert_eq!(h.k().queue_len(), usize::try_from(3 - request).unwrap());
        assert_eq!(in_flight(&h), identity(request), "still one in flight");
        let effects = h.step(published(seq, digest, request));
        if request < 3 {
            next = only_check(&effects);
        } else {
            assert_eq!(effects, vec![], "drained");
            assert_eq!(h.k().inflight(), None);
        }
    }
}

/// M7A-89. A-R19 "trim never removes a retained outcome the spec requires"; spec §5.3; lead
/// ruling A-R68 Q2.
///
/// R(1)..R(7) resolved at seqs 1..7, so R(7) is retained with `applied_at_seq 7`.
/// `DedupTrim{g7, below: 7}` keeps it (and drops R(6), as a trim must): a retry replays the
/// retained result verbatim. `below: 8` drops it, and the same `Submit` is then a **new**
/// request, admitted to a fresh batch at the next sequence, 8 — past retention, absence proves
/// nothing. The `Unknown` answer after the trim is P1's `Status`, not this row's (A-R68 Q2).
#[retcd_test]
fn m7a_89_trim_never_removes_a_required_retained_outcome() {
    let mut h = H::live();
    for request in 1..=7 {
        let _ = h.resolve(put(request, b"k", b"v"));
    }
    let retained = h
        .k()
        .dedup()
        .get(GEN, AFF, identity(7))
        .expect("R(7) retained")
        .clone();
    assert_eq!(retained.applied_at_seq, Seq(7));
    let RetainedAnswer::Applied(result) = retained.answer else {
        panic!("R(7) retained as applied: {retained:?}");
    };

    assert_eq!(h.step(trim(GEN, 7)), vec![]);
    assert!(
        h.k().dedup().get(GEN, AFF, identity(7)).is_some(),
        "below 7 keeps 7"
    );
    assert!(
        h.k().dedup().get(GEN, AFF, identity(6)).is_none(),
        "below 7 drops 6"
    );
    assert_eq!(
        h.step(submit(put(7, b"k", b"v"))),
        vec![EffectKind::Reply(ReplyEffect::Transaction {
            identity: identity(7),
            result,
        })],
        "before the trim: replayed"
    );

    assert_eq!(h.step(trim(GEN, 8)), vec![]);
    assert!(
        h.k().dedup().get(GEN, AFF, identity(7)).is_none(),
        "below 8 drops 7"
    );
    let correlation = h.admit(put(7, b"k", b"v"));
    let effects = h.step(answer(correlation, 1, Verdict::Admit));
    let [EffectKind::Store(StoreEffect::Commit(batch))] = effects.as_slice() else {
        panic!("after the trim: a fresh batch: {effects:?}");
    };
    assert_eq!(batch.seq, Seq(8), "at the next sequence");
}

/// M7A-90. A-R19 growth bounded; ADR 0004 "unbounded growth explicit"; §4.4 (A-R7); §13 Q-4.
///
/// Q-4's default is a named constant and `OVERLOADED` past it. The constant is
/// `RETENTION_CAP_ENTRIES` (65 536), and it is what a default T1 is built with. 100 000 distinct
/// identities are submitted one after another with **no** trim and no retire: the first 65 536 are
/// committed and retained, and every one after that is refused `OVERLOADED` at admission — nothing
/// reserved, no check, no batch. That is Q-4's second disjunct, and `DedupIndex::len()` never
/// passes the constant. T1 emits exactly one candidate per retained identity, and a candidate is
/// what P1's status index grows by, so status is held to the same number.
///
/// **Over the unit budget: about 3.5 s in a debug build**, because the row keeps the default cap
/// as §13 Q-4 asks. It took 236 s until `DedupIndex::older` stopped scanning every entry on
/// each admission (lead ruling A-R76).
#[retcd_test]
fn m7a_90_no_trim_bounded_growth_dedup_and_status() {
    let mut h = H::live();
    assert_eq!(h.k().limits().dedup_cap, RETENTION_CAP_ENTRIES);
    let cap = u64::try_from(RETENTION_CAP_ENTRIES).unwrap();
    let mut candidates = 0_u64;
    for request in 1..=100_000_u64 {
        if request <= cap {
            let (batch, digest, seq) = h.dispatch(put(request, b"k", b"v"));
            let effects = h.step(committed(batch, seq));
            candidates += effects
                .iter()
                .filter(|effect| {
                    matches!(
                        effect,
                        EffectKind::Kernel(KernelEffect::AppliedCandidate(_))
                    )
                })
                .count() as u64;
            assert_eq!(h.step(published(seq, digest, request)), vec![]);
        } else {
            let before = h.k().next_seq();
            assert_eq!(
                failed(&h.step(submit(put(request, b"k", b"v")))),
                ErrorKind::Overloaded,
                "R({request}), past the cap"
            );
            assert_eq!(h.k().next_seq(), before);
            assert_eq!(h.k().inflight(), None);
        }
    }
    assert_eq!(
        h.k().dedup().len(),
        RETENTION_CAP_ENTRIES,
        "never past the cap"
    );
    assert_eq!(
        candidates, cap,
        "one candidate, so one status entry, per retained identity"
    );
}

/// A condition-failure entry retained at `seq`: the smallest `Retained` there is.
fn retained_at(seq: u64) -> Retained {
    Retained {
        request_digest: Digest::ROOT,
        answer: RetainedAnswer::ConditionFailed { index: 0 },
        applied_at_seq: Seq(seq),
    }
}

/// The generation-reconciliation lookup (`DedupIndex::older`, spec §8.1) answers with the newest
/// generation **strictly below** the current one that retained this exact `(affinity, identity)`,
/// skipping generations that hold only other identities. Claims no row: it pins the lookup's
/// answers so that making the lookup bounded (lead ruling A-R76) cannot change them. The zero
/// identity sits at the very first key of a generation, which is where a range bound would slip.
#[retcd_test]
fn dedup_older_answers_the_newest_older_generation_holding_that_identity() {
    let zero = RequestIdentity {
        tenant: TenantId(0),
        client: ClientId(0),
        request: RequestId(0),
    };
    let other = AffinityId(AFF.0 + 1);
    let mut index = DedupIndex::default();
    // g3: identity 1, the zero identity, and identity 1 under another affinity.
    index.insert(Generation(3), AFF, identity(1), retained_at(31));
    index.insert(Generation(3), AffinityId(0), zero, retained_at(30));
    index.insert(Generation(3), other, identity(1), retained_at(32));
    // g5: identity 2 and the zero identity. Identity 1 is **not** here.
    index.insert(Generation(5), AFF, identity(2), retained_at(51));
    index.insert(Generation(5), AffinityId(0), zero, retained_at(50));
    // g6: identity 3 only, adjacent to the g7 below which it is found.
    index.insert(Generation(6), AFF, identity(3), retained_at(63));
    // g7, the current one: identities 1, 2 and zero.
    index.insert(Generation(7), AFF, identity(1), retained_at(71));
    index.insert(Generation(7), AFF, identity(2), retained_at(72));
    index.insert(Generation(7), AffinityId(0), zero, retained_at(70));

    let older = |index: &DedupIndex, current: u64, affinity: AffinityId, who: RequestIdentity| {
        index
            .older(Generation(current), affinity, who)
            .map(|(g, retained)| (g.0, retained.applied_at_seq.0))
    };
    // Past a generation that holds only other identities, down to the one that holds it.
    assert_eq!(older(&index, 7, AFF, identity(1)), Some((3, 31)));
    // The generation right below one that misses: g7 holds no identity 3, g6 does.
    assert_eq!(older(&index, 8, AFF, identity(3)), Some((6, 63)));
    // The newest older generation wins, and the current one is never an answer.
    assert_eq!(older(&index, 7, AFF, identity(2)), Some((5, 51)));
    assert_eq!(older(&index, 8, AFF, identity(2)), Some((7, 72)));
    assert_eq!(older(&index, 6, AFF, identity(2)), Some((5, 51)));
    assert_eq!(older(&index, 5, AFF, identity(2)), None);
    // The first key of a generation: found below, never at, the current generation.
    assert_eq!(older(&index, 7, AffinityId(0), zero), Some((5, 50)));
    assert_eq!(older(&index, 5, AffinityId(0), zero), Some((3, 30)));
    assert_eq!(older(&index, 3, AffinityId(0), zero), None);
    // The affinity is part of the scope.
    assert_eq!(older(&index, 7, other, identity(1)), Some((3, 32)));
    assert_eq!(older(&index, 7, other, identity(2)), None);
    assert_eq!(older(&index, 7, AFF, identity(9)), None);
    assert_eq!(older(&index, 0, AFF, identity(1)), None);

    // A retired generation is gone: the lookup falls through to the next one down.
    index.retire(Generation(5));
    assert_eq!(older(&index, 7, AffinityId(0), zero), Some((3, 30)));
    assert_eq!(older(&index, 7, AFF, identity(2)), None);
}

/// M7A-178 (plan §8.1, lead ledger L-R177gf; the reask livelock inv-publish-path found). An A1
/// that cannot move — here `Unheld`, answering every check `Deny(NoGrant)` at `authority_seq` 0
/// while T1 holds the recovery's view at 1 — answers every check stale. Before the fix each stale
/// answer re-asked, A1 answered stale again at the same tick, and a sim run spun 29,506 checks
/// at one tick until its event cap. Now a check is asked again at most once per view T1 holds
/// (A-R70's "against the view T1 now holds": a second ask under an unmoved view is the same
/// question), and then refused `LEASE_EXPIRED` (A-R73: refuse when the check cannot be asked
/// again) — before the deadline, at the same tick — and the queue behind it gets its own turn.
/// The near miss: when the view moves between two stale answers, the second one re-asks.
/// Two more clauses pin the edges: a request both expired and already re-asked is refused
/// `DEADLINE_BEFORE_ADMISSION` (expiry is checked first), and a view republished at the same
/// `authority_seq` has not moved, so it does not reset the bound.
#[retcd_test]
fn m7a_178_a_stale_authority_at_one_tick_is_asked_once_more_then_refused() {
    let mut h = H::live();
    let first = h.admit(put(1, b"a", b"1"));
    assert_eq!(h.step(submit(put(2, b"b", b"2"))), vec![], "queued behind");
    let start = (h.now, h.k().next_seq());

    // Stand in for the stale A1: answer every check this tick, at seq 0, until none is asked.
    let mut outstanding = vec![first];
    let (mut checks, mut refused) = (1, Vec::new());
    let mut steps = 0;
    while let Some(correlation) = outstanding.pop() {
        steps += 1;
        assert!(
            steps <= 100,
            "unbounded re-asks at one tick: {checks} checks"
        );
        let effects = h.step(answer(correlation, 0, Verdict::Deny(DenyReason::NoGrant)));
        let [stale, rest @ ..] = effects.as_slice() else {
            panic!("{effects:?}");
        };
        assert_eq!(*stale, ignored(AuthorityIgnoreReason::StaleAuthorityAnswer));
        for effect in rest {
            match effect {
                EffectKind::Kernel(KernelEffect::AuthorityCheck {
                    checkpoint: Checkpoint::StorageDispatch,
                    correlation,
                    ..
                }) => {
                    checks += 1;
                    outstanding.push(*correlation);
                }
                other => refused.extend(replies(std::slice::from_ref(other))),
            }
        }
    }
    assert_eq!(
        checks, 4,
        "each request: its check and one re-ask under the unmoved view"
    );
    assert_eq!(
        refused,
        vec![(1, ErrorKind::LeaseExpired), (2, ErrorKind::LeaseExpired)],
        "refused before the deadline, not DEADLINE_BEFORE_ADMISSION"
    );
    assert_eq!(
        (h.now, h.k().next_seq()),
        start,
        "one tick; nothing dispatched"
    );
    assert_eq!((h.k().inflight(), h.k().queue_len()), (None, 0));

    // The near miss: the view moved between the two stale answers, so it is asked again.
    let mut h = H::live();
    let c1 = h.admit(put(1, b"a", b"1"));
    let c2 = reasked(&h.step(answer(c1, 0, Verdict::Deny(DenyReason::NoGrant))));
    let _ = h.step(push_view(2));
    let c3 = reasked(&h.step(answer(c2, 0, Verdict::Deny(DenyReason::NoGrant))));
    assert_ne!(c2, c3);
    assert!(matches!(
        h.step(answer(c3, 2, Verdict::Admit)).as_slice(),
        [EffectKind::Store(StoreEffect::Commit(_))]
    ));

    // Expired as well as already re-asked under the unmoved view: expiry is checked first, so
    // the refusal is DEADLINE_BEFORE_ADMISSION, not LEASE_EXPIRED.
    let mut h = H::live();
    let c1 = h.admit(put(1, b"a", b"1"));
    h.now = 10 + 1_000 - 1;
    let c2 = reasked(&h.step(answer(c1, 0, Verdict::Deny(DenyReason::NoGrant))));
    h.now = 10 + 1_000;
    let effects = h.step(answer(c2, 0, Verdict::Deny(DenyReason::NoGrant)));
    let [stale, rest @ ..] = effects.as_slice() else {
        panic!("{effects:?}");
    };
    assert_eq!(*stale, ignored(AuthorityIgnoreReason::StaleAuthorityAnswer));
    assert_eq!(
        replies(rest),
        vec![(1, ErrorKind::DeadlineBeforeAdmission)],
        "expired beats the re-ask bound"
    );

    // "Per view" means per authority_seq: a view republished at the same seq has not moved, so
    // the second stale answer is still refused.
    let mut h = H::live();
    let c1 = h.admit(put(1, b"a", b"1"));
    let c2 = reasked(&h.step(answer(c1, 0, Verdict::Deny(DenyReason::NoGrant))));
    let _ = h.step(push_view(1));
    let effects = h.step(answer(c2, 0, Verdict::Deny(DenyReason::NoGrant)));
    let [stale, rest @ ..] = effects.as_slice() else {
        panic!("{effects:?}");
    };
    assert_eq!(*stale, ignored(AuthorityIgnoreReason::StaleAuthorityAnswer));
    assert_eq!(
        replies(rest),
        vec![(1, ErrorKind::LeaseExpired)],
        "a same-seq view does not reset the bound"
    );
}

/// M7A-190. Lead ruling A-R84 (dev-edges defect D3, T1 half). A view whose lineage names
/// another partition is not adopted: `Ignored(NotOurs)`, as `on_freeze` answers another
/// partition's fence, and the view T1 holds is unchanged. Before the fix `on_view` compared only
/// `authority_seq`, so a newer view for `p2`, delivered at `p1`, became `p1`'s authority.
#[retcd_test]
fn m7a_190_t1_does_not_adopt_another_partitions_view() {
    let mut h = H::live();
    assert_eq!(
        h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(view(
            GEN, 1, 1_000
        ))))),
        vec![]
    );
    let held = h.k().authority().copied();
    assert!(held.is_some(), "fixture: T1 holds its own view");

    let mut foreign = view(GEN, 9, 1_000);
    foreign.lineage.partition = PartitionId(2);
    assert_eq!(
        h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(
            foreign
        )))),
        vec![ignored(AuthorityIgnoreReason::NotOurs)]
    );
    assert_eq!(
        h.k().authority().copied(),
        held,
        "M7A-190: T1's view unchanged"
    );
    assert_eq!(h.k().mode(), &QueueMode::Open);

    // tester-edges TT1: a lower partition id is just as foreign. The guard is inequality, not
    // order, so a newer view naming partition 0 at partition 1 is not adopted either.
    let mut lower = view(GEN, 10, 1_000);
    lower.lineage.partition = PartitionId(0);
    assert_eq!(
        h.step(kernel(KernelEvent::Authority(AuthorityEvent::View(lower)))),
        vec![ignored(AuthorityIgnoreReason::NotOurs)]
    );
    assert_eq!(
        h.k().authority().copied(),
        held,
        "M7A-190: T1's view unchanged"
    );
}
