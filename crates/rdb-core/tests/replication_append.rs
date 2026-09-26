//! R1 `AppendReceiver`: the design §3.2 validation ladder (and §3.2a's recovery append), and the
//! §3.3 completion events, reached through `Replication::step`.
//!
//! `m7b_<n>_*` functions are the §3 rows, and the receiver's §9 rows (M7B-120, 121, 122,
//! 124), of `docs/testing/test-plan-m7-kernel-b.md` (written
//! after tester-kb-r1's slice-1 thumbs-up). Where lead ruling F-4 overrode a plan literal
//! (M7B-14, 16, 17), the row asserts the ruling and says so. Plain-named functions are not plan
//! rows: developer scaffolding, supporting guards, and the manual tester's `tester_r1_*` rows.
//! Every test drives a real `Event` through the module, so "reachable through step" is what
//! each one proves as well as its ladder row.
//!
//! Not here: M7B-23's `StorageFault` effect and M7B-25 (no landed carrier), M7B-26 (sim),
//! M7B-123 (the cursor's step 1a is not built) and M7B-139 (an R1 `QuarantineSuffix` and a
//! tracker clause).
//!
//! Fixture: three copies — A primary (node 1), B regular secondary (node 2, the receiver under
//! test), C regular secondary (node 3). B holds `(10, d10)` applied, durable 10, generation 3,
//! owner epoch 5, config 7. The golden append is seq 11 from A, chained on `d10`. Every
//! rejecting test changes one field of it.

use config_log::retcd_test;

use bytes::Bytes;
use rdb_core::contracts::authority::{
    AuthorityView, DenyReason, FenceCredential, FencingProof, Lineage, PartitionMode, Revocation,
};
use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::envelope::{
    AppendAck, AppendOutcome, AppendReject, EnvelopeHeader, ReplicaProgress, ReplicationEnvelope,
};
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, ModuleName,
    NodeLifecycle, StepCtx,
};
use rdb_core::contracts::ids::{
    AppliedSeq, AuthorityGeneration, BatchId, BootId, ClientId, ConfigVersion, CorrelationId,
    DurableSeq, EventId, FlushTicket, Generation, GrantId, LeaseId, MessageId, NodeId, OwnerEpoch,
    PartitionId, ReceivedSeq, ReplicaRole, RequestId, RequestIdentity, Revision, Seq,
    SnapshotHandle, TenantId, TimerId, TimerVersion,
};
use rdb_core::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::recovery::{
    CommittedRoot, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap, SelectedLineage,
};
use rdb_core::contracts::storage::{
    Batch, DurablePrefix, Namespace, SnapshotRead, StorageEvent, StorageFault, StoreEffect, Write,
};
use rdb_core::contracts::time::{ControlTime, Tick, TimerFired};
use rdb_core::contracts::trace::{CapabilityState, Version};
use rdb_core::contracts::transport::{Frame, PeerLabel, SendEffect, TransportEvent};
use rdb_core::contracts::txn::Outcome;
use rdb_core::contracts::version::ENVELOPE_VERSION;
use rdb_core::replication::append::{
    AppendReceiver, Head, HistoryRoot, ReceiverInit, MAX_ENVELOPE_BYTES, MAX_MUTATIONS,
    PROGRESS_KEY, UNSOLICITED,
};
use rdb_core::replication::catchup::{CatchupCursor, MAX_PROBE_ROUNDS};
use rdb_core::replication::progress::DigestLadder;
use rdb_core::replication::wire::{
    decode_reply, encode_recovery_append, encode_reply, REPLY_MAGIC,
};
use rdb_core::replication::Replication;

const P: PartitionId = PartitionId(4);
const GEN: Generation = Generation(3);
const EPOCH: OwnerEpoch = OwnerEpoch(5);
const CONFIG: ConfigVersion = ConfigVersion(7);
const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const C: NodeId = NodeId(3);
const D: NodeId = NodeId(4);
const FRAME_ID: MessageId = MessageId(42);
// What the takeover in `recovered` installs.
const NEW_GEN: Generation = Generation(4);
const NEW_EPOCH: OwnerEpoch = OwnerEpoch(6);
const NEW_CONFIG: ConfigVersion = ConfigVersion(8);
const REVISION: Revision = Revision(2);

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

fn label(node: NodeId) -> PeerLabel {
    PeerLabel {
        node,
        boot: BootId(u64::from(node.0)),
        authenticated: true,
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

/// A primary (copy 0), B and C regular (copies 1 and 2), for `partition`.
fn config_for(partition: PartitionId) -> PartitionConfig {
    PartitionConfig::new(
        partition,
        CONFIG,
        vec![
            member(0, A, ReplicaRole::Primary),
            member(1, B, ReplicaRole::RegularSecondary),
            member(2, C, ReplicaRole::RegularSecondary),
        ],
    )
}

fn config() -> PartitionConfig {
    config_for(P)
}

/// What the takeover pins, with `roles` for copies 0-3 on nodes A-D.
fn pinned(roles: [ReplicaRole; 4]) -> PartitionConfig {
    let members = [A, B, C, D]
        .into_iter()
        .zip(roles)
        .zip(0..)
        .map(|((node, role), copy)| member(copy, node, role))
        .collect();
    PartitionConfig::new(P, NEW_CONFIG, members)
}

/// The takeover's usual pin: C primary, A and B regular, D a shadow.
fn takeover_config() -> PartitionConfig {
    use ReplicaRole::{Primary, RegularSecondary, Shadow};
    pinned([RegularSecondary, RegularSecondary, Primary, Shadow])
}

/// A sealed envelope at `seq` chained on `prev`, carrying `value` as its one user write.
fn envelope(seq: u64, prev: Digest, value: &'static [u8]) -> ReplicationEnvelope {
    seal(ReplicationEnvelope {
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
            value: Some(Bytes::from_static(value)),
        }],
        result: Outcome::Published,
        record_digest: Digest::ROOT,
    })
}

/// Recompute the record digest after a field was changed on purpose.
fn seal(mut env: ReplicationEnvelope) -> ReplicationEnvelope {
    env.record_digest = env.compute_record_digest().expect("digest");
    env
}

/// The history 1..=n from the root, so `chain(n)[i - 1]` is the record at seq `i`.
fn chain(n: u64) -> Vec<ReplicationEnvelope> {
    let mut out: Vec<ReplicationEnvelope> = Vec::new();
    for seq in 1..=n {
        let prev = out.last().map_or(Digest::ROOT, |env| env.record_digest);
        out.push(envelope(seq, prev, b"v"));
    }
    out
}

fn d(seq: u64) -> Digest {
    chain(seq)[usize::try_from(seq).expect("seq") - 1].record_digest
}

/// The golden append: seq 11 from A, chained on `d10`.
fn golden() -> ReplicationEnvelope {
    chain(11).pop().expect("seq 11")
}

/// The same record relabelled into another lineage and re-sealed. Its `prev_digest` is kept.
fn under(
    mut env: ReplicationEnvelope,
    generation: Generation,
    epoch: OwnerEpoch,
    config: ConfigVersion,
) -> ReplicationEnvelope {
    env.header.generation = generation;
    env.header.owner_epoch = epoch;
    env.header.config_version = config;
    seal(env)
}

/// `env` relabelled into the lineage `recovered` installs.
fn taken_over(env: ReplicationEnvelope) -> ReplicationEnvelope {
    under(env, NEW_GEN, NEW_EPOCH, NEW_CONFIG)
}

fn head(seq: u64) -> Head {
    Head {
        seq: Seq(seq),
        digest: d(seq),
    }
}

/// B's receiver for `partition` at `(10, d10)`, durable `durable`.
fn receiver(partition: PartitionId, durable: u64) -> AppendReceiver {
    receiver_at(partition, 10, durable)
}

/// B's receiver for `partition` at `(at, d(at))`, durable `durable`. Its ladder holds `at` only.
fn receiver_at(partition: PartitionId, at: u64, durable: u64) -> AppendReceiver {
    AppendReceiver::new(ReceiverInit {
        config: config_for(partition),
        own: CopyId(1),
        lineage: Lineage {
            partition,
            generation: GEN,
            owner_epoch: EPOCH,
        },
        head: head(at),
        durable: DurableSeq(durable),
    })
    .expect("golden receiver")
}

/// Replication with B's receiver at `(10, d10)`, durable 10.
fn module() -> Replication {
    let mut module = Replication::new();
    module.install_receiver(receiver(P, 10));
    module
}

/// `module()` after A's records 11..=n were each staged and committed. Durable stays 10.
fn applied_to(n: u64) -> Replication {
    let mut module = module();
    for (batch, env) in (0..).zip(chain(n).into_iter().skip(10)) {
        send(&mut module, label(A), &env);
        step(&mut module, &committed(batch, env.header.seq.0));
    }
    module
}

/// B's acknowledgement in the original lineage, at the given watermarks.
fn ack(received: u64, applied: u64, durable: u64) -> AppendAck {
    AppendAck {
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
            durable: DurableSeq(durable),
        },
        digest_at_buffered: d(applied),
    }
}

fn rx(module: &Replication) -> &AppendReceiver {
    module.receiver(B, P).expect("installed")
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

fn delivered(from: PeerLabel, body: Bytes) -> Event {
    event(EventKind::Transport(TransportEvent::Delivered {
        from,
        frame: Frame {
            id: FRAME_ID,
            protocol: ENVELOPE_VERSION,
            config: CONFIG,
            body,
        },
    }))
}

/// Step one event and return the effect kinds, after checking every effect is stamped with the
/// event's correlation and partition and comes from R1.
fn step(module: &mut Replication, ev: &Event) -> Vec<EffectKind> {
    let effects = module.step(&ctx(), ev).expect("R1 answers its own event");
    effects
        .into_iter()
        .map(
            |Effect {
                 correlation,
                 from,
                 partition,
                 kind,
             }| {
                assert_eq!(
                    (correlation, from, partition),
                    (ev.correlation, ModuleName::Replication, ev.partition)
                );
                kind
            },
        )
        .collect()
}

fn send(module: &mut Replication, from: PeerLabel, env: &ReplicationEnvelope) -> Vec<EffectKind> {
    send_bytes(module, from, env.encode().expect("encode"))
}

fn send_bytes(module: &mut Replication, from: PeerLabel, body: Bytes) -> Vec<EffectKind> {
    step(module, &delivered(from, body))
}

/// The outcome a `Send` effect carries, after checking its destination, its message id and the
/// configuration it was sent under.
fn reply_at(kind: &EffectKind, to: NodeId, id: MessageId, config: ConfigVersion) -> AppendOutcome {
    match kind {
        EffectKind::Send(SendEffect::Unicast { to: dest, frame }) => {
            assert_eq!((*dest, frame.id, frame.config), (to, id, config));
            decode_reply(&frame.body).expect("reply decodes")
        }
        other => panic!("expected a reply, got {other:?}"),
    }
}

/// The outcome a `Send` effect carries back to A, reusing the request's message id.
fn reply_of(kind: &EffectKind) -> AppendOutcome {
    reply_at(kind, A, FRAME_ID, CONFIG)
}

/// Send `env` from A, assert exactly one reply and no state change, and return the reply.
fn refused(env: &ReplicationEnvelope) -> AppendOutcome {
    refused_bytes(env.encode().expect("encode"))
}

fn refused_bytes(body: Bytes) -> AppendOutcome {
    refused_in(&mut module(), A, CONFIG, body)
}

/// Send `body` from `from` into `module`; assert one reply back to `from` under `config` and no
/// state change, and return the reply.
fn refused_in(
    module: &mut Replication,
    from: NodeId,
    config: ConfigVersion,
    body: Bytes,
) -> AppendOutcome {
    let before = rx(module).clone();
    let effects = send_bytes(module, label(from), body);
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(rx(module), &before, "a refused append changes nothing");
    reply_at(&effects[0], from, FRAME_ID, config)
}

fn storage(kind: StorageEvent) -> Event {
    event(EventKind::Storage(kind))
}

fn committed(batch: u64, applied: u64) -> Event {
    storage(StorageEvent::Committed {
        batch: BatchId(batch),
        applied: AppliedSeq(applied),
    })
}

fn commit_failed(batch: u64) -> Event {
    commit_failed_with(batch, StorageFault::WriteFailed)
}

fn commit_failed_with(batch: u64, fault: StorageFault) -> Event {
    storage(StorageEvent::CommitFailed {
        batch: BatchId(batch),
        fault,
    })
}

/// A flush confirming the `(partition, generation, through)` prefixes.
fn flushed(prefixes: &[(PartitionId, Generation, u64)]) -> Event {
    storage(StorageEvent::Flushed {
        ticket: FlushTicket(1),
        durable: prefixes
            .iter()
            .map(|&(partition, generation, through)| DurablePrefix {
                partition,
                generation,
                through: DurableSeq(through),
            })
            .collect(),
    })
}

fn ignored(reason: KernelIgnoredReason) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Ignored { reason })
}

fn withheld() -> EffectKind {
    ignored(KernelIgnoredReason::AppendRejected(
        AppendReject::Quarantined,
    ))
}

fn declined(module: &mut Replication, ev: &Event) {
    let before = module.clone();
    let err = module.step(&ctx(), ev).expect_err("declined");
    assert!(matches!(err, RdbError::Unavailable { .. }), "{err:?}");
    assert_eq!(module, &before, "a declined event changes nothing");
}

/// A committed takeover to `NEW_GEN`/`NEW_EPOCH` at revision `REVISION`, pinning `pinned`,
/// whose selected prefix ends at `(cutoff, cutoff_digest)` of generation `GEN`.
fn recovered(cutoff: u64, cutoff_digest: Digest, pinned: PartitionConfig) -> Event {
    let cutoff = Seq(cutoff);
    let root = Lineage {
        partition: P,
        generation: NEW_GEN,
        owner_epoch: NEW_EPOCH,
    };
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
            revision: REVISION,
            authority_view: AuthorityView {
                lineage: root,
                grant_id: GrantId(2),
                boot_id: BootId(3),
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
    };
    event(EventKind::Kernel(KernelEvent::Recovered(Box::new(result))))
}

/// The one effect of a re-anchoring `Recovered`: an unsolicited `NeedPrefix` from `have` to the
/// new primary, under the new pin.
fn asks_new_primary(effects: &[EffectKind], have: u64) {
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(
        reply_at(&effects[0], C, UNSOLICITED, NEW_CONFIG),
        rejected(AppendReject::NeedPrefix {
            have: Seq(have),
            head_digest: d(have),
        })
    );
}

/// A fence credential naming copy `sender` as the transfer source.
fn fence(sender: u8, epoch: OwnerEpoch, revision: Revision) -> FenceCredential {
    FenceCredential {
        partition: P,
        prior_generation: GEN,
        prior_owner_epoch: epoch,
        control_revision: revision,
        sender: CopyId(sender),
    }
}

/// A `RecoveryAppend` frame body: `fence` wrapped round `env`'s envelope bytes.
fn recovery_body(fence: &FenceCredential, env: &ReplicationEnvelope) -> Bytes {
    encode_recovery_append(fence, &env.encode().expect("encode"))
}

/// The one `Store` a staged record emits, as `(batch, generation, seq)`.
fn staged_batch(effects: &[EffectKind]) -> (BatchId, Generation, Seq) {
    match effects {
        [EffectKind::Store(StoreEffect::Commit(batch))] => (batch.id, batch.generation, batch.seq),
        other => panic!("expected one staged batch, got {other:?}"),
    }
}

fn rejected(reason: AppendReject) -> AppendOutcome {
    AppendOutcome::Rejected(reason)
}

fn quarantine_alert() -> EffectKind {
    EffectKind::Kernel(KernelEffect::Alert {
        reason: ErrorKind::CorruptHistory,
    })
}

// --- accept -------------------------------------------------------------------------------

#[retcd_test]
fn m7b_01_accept_stages_one_atomic_batch() {
    let mut module = module();
    let env = golden();
    let body = env.encode().expect("encode");
    let effects = send_bytes(&mut module, label(A), body.clone());

    let mut progress = 11u64.to_le_bytes().to_vec();
    progress.extend_from_slice(&env.record_digest.0);
    let mut writes = env.mutations.clone();
    writes.push(Write {
        ns: Namespace::History,
        key: Bytes::copy_from_slice(&11u64.to_be_bytes()),
        value: Some(body),
    });
    writes.push(Write {
        ns: Namespace::Progress,
        key: Bytes::from_static(PROGRESS_KEY),
        value: Some(Bytes::from(progress)),
    });
    assert_eq!(
        effects,
        vec![EffectKind::Store(StoreEffect::Commit(Batch {
            id: BatchId(0),
            partition: P,
            generation: GEN,
            seq: Seq(11),
            writes,
        }))]
    );
    let rx = rx(&module);
    let staged = rx.staged().expect("staged");
    assert_eq!(
        (staged.batch, staged.head),
        (
            BatchId(0),
            Head {
                seq: Seq(11),
                digest: env.record_digest
            }
        )
    );
    assert_eq!(rx.accept_head(), staged.head);
    assert_eq!(rx.received_seq(), ReceivedSeq(11));
    assert_eq!(
        rx.applied_head(),
        Head {
            seq: Seq(10),
            digest: d(10)
        }
    );
    assert_eq!(rx.buffered_applied_seq(), AppliedSeq(10));
    assert_eq!(rx.durable_seq(), DurableSeq(10));
    assert_eq!(rx.quarantine(), None);
}

// --- rows 0-7 -----------------------------------------------------------------------------

#[retcd_test]
fn m7b_02_quarantined_receiver_rejects_every_append_and_changes_nothing() {
    let mut module = module();
    let mut corrupt = golden();
    corrupt.record_digest.0[0] ^= 1;
    send(&mut module, label(A), &corrupt);
    let before = rx(&module).clone();
    assert_eq!(
        before.quarantine(),
        Some(AppendReject::CorruptHistory { at: Seq(11) })
    );

    // Eight appends, one valid and one failing each later row: all get row 0's answer.
    let mut version = golden();
    version.header.protocol_version += 1;
    let mut large = golden();
    large.mutations = vec![large.mutations[0].clone(); MAX_MUTATIONS + 1];
    let mut stale = golden();
    stale.header.owner_epoch = OwnerEpoch(1);
    let mut elsewhere = golden();
    elsewhere.header.partition = PartitionId(9);
    for env in [
        golden(),
        seal(version),
        seal(large),
        seal(elsewhere),
        seal(stale),
        corrupt.clone(),
        chain(10).pop().expect("10"),
        chain(13).pop().expect("13"),
    ] {
        let effects = send(&mut module, label(A), &env);
        assert_eq!(effects.len(), 1);
        assert_eq!(reply_of(&effects[0]), rejected(AppendReject::Quarantined));
        assert_eq!(rx(&module), &before);
    }
}

/// The plan's `DecodeSpy` does not exist; "before decode" is proved by behaviour instead. The
/// same body cut one byte short is refused the same way, while a current-version body cut short
/// is malformed. So nothing past the header was decoded.
#[retcd_test]
fn m7b_03_unknown_mandatory_protocol_version_is_refused_before_decode() {
    let mut env = golden();
    env.header.protocol_version = ENVELOPE_VERSION + 1;
    assert_eq!(refused(&env), rejected(AppendReject::IncompatibleVersion));

    let cut = |env: &ReplicationEnvelope| {
        let body = env.encode().expect("encode");
        body.slice(..body.len() - 1)
    };
    assert_eq!(
        refused_bytes(cut(&env)),
        rejected(AppendReject::IncompatibleVersion)
    );
    malformed_in(&mut module(), cut(&golden()));
}

/// The plan's `HashSpy` does not exist; "before any hashing" is proved by behaviour instead.
/// Neither record is re-sealed, so its `record_digest` is stale, and row 7 would answer
/// `CorruptHistory`. Row 2 answers first. The byte bound is also checked before decode, which
/// `tester_r1_byte_bound_is_inclusive_and_checked_before_decode` proves.
#[retcd_test]
fn m7b_04_too_large_is_refused_before_any_hashing() {
    let stale = |env: &ReplicationEnvelope| {
        assert_ne!(
            env.compute_record_digest().expect("digest"),
            env.record_digest
        );
    };
    let mut big = golden();
    big.mutations[0].value = Some(Bytes::from(vec![0u8; MAX_ENVELOPE_BYTES]));
    stale(&big);
    assert_eq!(refused(&big), rejected(AppendReject::TooLarge));

    let mut many = golden();
    many.mutations = vec![many.mutations[0].clone(); MAX_MUTATIONS + 1];
    stale(&many);
    assert_eq!(refused(&many), rejected(AppendReject::TooLarge));

    // The bound is inclusive: exactly MAX_MUTATIONS is accepted.
    let mut at_bound = golden();
    at_bound.mutations = vec![at_bound.mutations[0].clone(); MAX_MUTATIONS];
    let mut module = module();
    let effects = send(&mut module, label(A), &seal(at_bound));
    assert!(matches!(effects[..], [EffectKind::Store(_)]), "{effects:?}");
}

#[retcd_test]
fn m7b_05_wrong_partition_rejected() {
    let mut env = golden();
    env.header.partition = PartitionId(9);
    assert_eq!(refused(&seal(env)), rejected(AppendReject::WrongPartition));
}

// Rows 4-6: generation, epoch and configuration are never learned from an append. Each row
// changes one header field of the golden append by one and re-seals it; `refused` asserts the
// receiver is unchanged, so none of them quarantines.

/// Not historical: seq 11 is above the history floor, and no `Recovered` has set a predecessor.
#[retcd_test]
fn m7b_06_stale_generation_rejected() {
    assert_eq!(
        refused(&under(golden(), Generation(2), EPOCH, CONFIG)),
        rejected(AppendReject::StaleGeneration { current: GEN })
    );
}

#[retcd_test]
fn m7b_07_need_lineage_when_generation_never_learned() {
    assert_eq!(
        refused(&under(golden(), Generation(4), EPOCH, CONFIG)),
        rejected(AppendReject::NeedLineage { current: GEN })
    );
}

#[retcd_test]
fn m7b_08_stale_epoch_rejected() {
    assert_eq!(
        refused(&under(golden(), GEN, OwnerEpoch(4), CONFIG)),
        rejected(AppendReject::StaleEpoch { current: EPOCH })
    );
}

#[retcd_test]
fn m7b_09_unknown_epoch_never_learned_is_not_stale() {
    assert_eq!(
        refused(&under(golden(), GEN, OwnerEpoch(6), CONFIG)),
        rejected(AppendReject::UnknownEpoch { current: EPOCH })
    );
}

#[retcd_test]
fn m7b_10_stale_config_rejected() {
    assert_eq!(
        refused(&under(golden(), GEN, EPOCH, ConfigVersion(6))),
        rejected(AppendReject::StaleConfig { current: CONFIG })
    );
}

#[retcd_test]
fn m7b_11_need_config_when_version_never_pinned() {
    assert_eq!(
        refused(&under(golden(), GEN, EPOCH, ConfigVersion(8))),
        rejected(AppendReject::NeedConfig { current: CONFIG })
    );
}

/// C first, as the plan names it; then a stranger, and the primary's node under a stale boot.
#[retcd_test]
fn m7b_12_authenticated_non_primary_member_is_not_a_member() {
    let env = golden().encode().expect("encode");
    // A regular member, a stranger, and the primary's node under a stale boot.
    let stale_boot = PeerLabel {
        boot: BootId(99),
        ..label(A)
    };
    for from in [label(C), label(NodeId(77)), stale_boot] {
        let mut module = module();
        let before = rx(&module).clone();
        let effects = send_bytes(&mut module, from, env.clone());
        assert_eq!(effects.len(), 1);
        match &effects[0] {
            EffectKind::Send(SendEffect::Unicast { to, frame }) => {
                assert_eq!(*to, from.node);
                assert_eq!(
                    decode_reply(&frame.body).expect("reply"),
                    rejected(AppendReject::NotAMember)
                );
            }
            other => panic!("expected a reply, got {other:?}"),
        }
        assert_eq!(rx(&module), &before);
    }
}

/// The plan's `LookupSpy` does not exist; "no lookup on the ladder" is proved by behaviour. A
/// corrupt record, a duplicate and an unretained seq would each draw a different answer from
/// rows 7 and 8 (quarantine, `AlreadyHave`, `ProbeDigestAt`). From an unauthenticated label
/// all four draw the same `Ignored`, and nothing is replied or changed.
#[retcd_test]
fn m7b_13_unauthenticated_label_rejected_before_any_state() {
    let mut module = module();
    let before = rx(&module).clone();
    let forged = PeerLabel {
        authenticated: false,
        ..label(A)
    };
    let mut corrupt = golden();
    corrupt.record_digest.0[0] ^= 1;
    for env in [
        golden(),
        corrupt,
        chain(10).pop().expect("10"),
        chain(5).pop().expect("5"),
    ] {
        assert_eq!(
            send(&mut module, forged, &env),
            vec![EffectKind::Kernel(KernelEffect::Ignored {
                reason: KernelIgnoredReason::AppendRejected(AppendReject::Unauthenticated),
            })]
        );
        assert_eq!(rx(&module), &before);
    }
}

/// Lead ruling F-4: the alert is `Alert{CorruptHistory}` (spec §5.4), not the plan's
/// `SelfQuarantined`, which has no `ErrorKind`.
#[retcd_test]
fn m7b_14_record_digest_mismatch_quarantines_corrupt_history() {
    let mut module = module();
    let mut env = golden();
    env.record_digest.0[31] ^= 0x80;
    let effects = send(&mut module, label(A), &env);
    let proof = AppendReject::CorruptHistory { at: Seq(11) };
    assert_eq!(effects.len(), 2);
    assert_eq!(reply_of(&effects[0]), rejected(proof));
    assert_eq!(effects[1], quarantine_alert());
    let rx = rx(&module);
    assert_eq!(rx.quarantine(), Some(proof));
    assert_eq!(rx.staged(), None);
    assert_eq!(
        rx.accept_head(),
        Head {
            seq: Seq(10),
            digest: d(10)
        }
    );
    assert_eq!(rx.received_seq(), ReceivedSeq(10));
}

/// One golden append broken on purpose: header and body fields, who sends it, and whether its
/// record digest is flipped after sealing.
struct Broken {
    env: ReplicationEnvelope,
    from: NodeId,
    corrupt: bool,
}

/// Apply `breaks` to the golden append, send it to a fresh receiver, and return the reply with
/// the seq it was sent at.
fn first_failure(breaks: &[fn(&mut Broken)]) -> (AppendOutcome, Seq) {
    let mut case = Broken {
        env: golden(),
        from: A,
        corrupt: false,
    };
    for brk in breaks {
        brk(&mut case);
    }
    let mut env = seal(case.env);
    if case.corrupt {
        env.record_digest.0[0] ^= 1;
    }
    let effects = send(&mut module(), label(case.from), &env);
    let reply = reply_at(&effects[0], case.from, FRAME_ID, CONFIG);
    (reply, env.header.seq)
}

/// Rows 1 to 8 in order, row 6 as its two halves (configuration, then sender). For every
/// adjacent pair broken together, only the lower row's code is reported; with the lower field
/// fixed, the higher row's code appears. The code of row 7 names the seq it was sent at.
#[retcd_test]
fn m7b_15_ladder_reports_the_first_failure_and_walks_down_in_order() {
    type Row = (fn(&mut Broken), fn(Seq) -> AppendReject);
    let rows: [Row; 9] = [
        (
            |c| c.env.header.protocol_version += 1,
            |_| AppendReject::IncompatibleVersion,
        ),
        (
            |c| c.env.mutations = vec![c.env.mutations[0].clone(); MAX_MUTATIONS + 1],
            |_| AppendReject::TooLarge,
        ),
        (
            |c| c.env.header.partition = PartitionId(9),
            |_| AppendReject::WrongPartition,
        ),
        (
            |c| c.env.header.generation = Generation(1),
            |_| AppendReject::StaleGeneration { current: GEN },
        ),
        (
            |c| c.env.header.owner_epoch = OwnerEpoch(1),
            |_| AppendReject::StaleEpoch { current: EPOCH },
        ),
        (
            |c| c.env.header.config_version = ConfigVersion(1),
            |_| AppendReject::StaleConfig { current: CONFIG },
        ),
        (|c| c.from = C, |_| AppendReject::NotAMember),
        (
            |c| c.corrupt = true,
            |at| AppendReject::CorruptHistory { at },
        ),
        (
            |c| c.env.header.seq = Seq(13),
            |_| AppendReject::NeedPrefix {
                have: Seq(10),
                head_digest: d(10),
            },
        ),
    ];
    for pair in rows.windows(2) {
        let [(lower, lower_code), (higher, higher_code)] = pair else {
            unreachable!("windows(2)")
        };
        let (reply, seq) = first_failure(&[*lower, *higher]);
        assert_eq!(reply, rejected(lower_code(seq)));
        let (reply, seq) = first_failure(&[*higher]);
        assert_eq!(reply, rejected(higher_code(seq)));
    }
}

// --- row 8 --------------------------------------------------------------------------------

#[retcd_test]
fn m7b_21_gap_returns_need_prefix_with_head_digest_and_buffers_nothing() {
    assert_eq!(
        refused(&chain(13).pop().expect("13")),
        rejected(AppendReject::NeedPrefix {
            have: Seq(10),
            head_digest: d(10),
        })
    );
}

#[retcd_test]
fn m7b_20_prev_digest_mismatch_at_next_seq_quarantines() {
    let mut module = module();
    let fork = envelope(11, d(9), b"v");
    let effects = send(&mut module, label(A), &fork);
    let proof = AppendReject::DivergentHistory { at: Seq(11) };
    assert_eq!(reply_of(&effects[0]), rejected(proof));
    assert_eq!(effects[1..], [quarantine_alert()]);
    assert_eq!(rx(&module).quarantine(), Some(proof));
    assert_eq!(rx(&module).staged(), None);
}

/// Lead ruling F-4: two replies, `AlreadyHave` then the current ACK (design §3.2 "re-emit the
/// current ACK"), not the plan's one-effect literal.
#[retcd_test]
fn m7b_17_duplicate_append_is_idempotent() {
    let mut module = module();
    let before = rx(&module).clone();
    let effects = send(&mut module, label(A), &chain(10).pop().expect("10"));
    assert_eq!(effects.len(), 2);
    assert_eq!(reply_of(&effects[0]), AppendOutcome::AlreadyHave);
    assert_eq!(
        reply_of(&effects[1]),
        AppendOutcome::Accepted(ack(10, 10, 10))
    );
    assert_eq!(rx(&module), &before);
}

/// A record at seq 10 that is not the one B holds there. Both twins below receive it.
fn other_10() -> ReplicationEnvelope {
    envelope(10, d(9), b"other")
}

/// B at head 12 still holds 10 on its ladder: a different record there is proof. Seq 10 is
/// also checked as the head's own rung, on a receiver at head 10.
#[retcd_test]
fn m7b_18_retained_differing_digest_below_head_quarantines() {
    let proof = AppendReject::DivergentHistory { at: Seq(10) };
    for mut module in [applied_to(12), module()] {
        assert_eq!(rx(&module).history().digest_at(Seq(10)), Some(d(10)));
        let effects = send(&mut module, label(A), &other_10());
        assert_eq!(reply_of(&effects[0]), rejected(proof));
        assert_eq!(effects[1..], [quarantine_alert()]);
        assert_eq!(rx(&module).quarantine(), Some(proof));
    }
}

/// M7B-18's twin by retention only: the same record, sent to a receiver also at head 12 whose
/// ladder does not hold 10. Absence is not proof.
#[retcd_test]
fn m7b_19_not_retained_probes_and_never_quarantines() {
    let mut module = Replication::new();
    module.install_receiver(receiver_at(P, 12, 10));
    assert_eq!(rx(&module).history().digest_at(Seq(10)), None);
    assert_eq!(
        refused_in(&mut module, A, CONFIG, other_10().encode().expect("encode")),
        AppendOutcome::ProbeDigestAt { seq: Seq(10) }
    );
    assert_eq!(rx(&module).quarantine(), None);
}

/// Lead ruling F-4: `Busy{accepted_through: 11}`, the accept head (design §3.2), not the
/// plan's 10.
#[retcd_test]
fn m7b_16_busy_while_one_batch_is_staged() {
    let mut module = module();
    send(&mut module, label(A), &golden());
    let staged = rx(&module).clone();

    // Seq 12 chained on the staged 11: Busy, resume after the accept head.
    let effects = send(&mut module, label(A), &chain(12).pop().expect("12"));
    assert_eq!(effects.len(), 1);
    assert_eq!(
        reply_of(&effects[0]),
        AppendOutcome::Busy {
            accepted_through: Seq(11)
        }
    );
    assert_eq!(rx(&module).received_seq(), ReceivedSeq(11));
    assert_eq!(rx(&module), &staged, "no second stage");
    // The staged record re-sent: also Busy, nothing staged twice.
    let effects = send(&mut module, label(A), &golden());
    assert_eq!(
        reply_of(&effects[0]),
        AppendOutcome::Busy {
            accepted_through: Seq(11)
        }
    );
    assert_eq!(rx(&module), &staged);

    // A different record at the staged seq differs from the staging slot: proof.
    let effects = send(&mut module, label(A), &envelope(11, d(10), b"other"));
    assert_eq!(
        reply_of(&effects[0]),
        rejected(AppendReject::DivergentHistory { at: Seq(11) })
    );
}

/// Lead ruling Q-C1 (B-R40), both sides composed: B probes a seq it does not retain, and A's
/// cursor answers by re-sending that one record. B still does not retain it, so it probes again.
/// Nothing is quarantined and nothing changes on B. The cursor's round cap ends the loop in a
/// snapshot request. A held seq (match or differ) never reaches a probe; row 8 decides it first.
#[retcd_test]
fn a_probe_answered_by_the_same_record_loops_until_the_cap_asks_for_a_snapshot() {
    let mut primary = DigestLadder::new();
    for env in chain(11) {
        primary.insert(env.header.seq, env.record_digest);
    }
    let copy = CopyId(1);
    let mut cursor = CatchupCursor::new(copy);
    let mut module = module();
    let before = rx(&module).clone();
    let record = chain(5).pop().expect("seq 5");
    let mut answer = |cursor: &mut CatchupCursor| {
        let effects = send(&mut module, label(A), &record);
        assert_eq!(effects.len(), 1, "{effects:?}");
        let outcome = reply_of(&effects[0]);
        assert_eq!(outcome, AppendOutcome::ProbeDigestAt { seq: Seq(5) });
        cursor.on_outcome(outcome, &primary, Seq(11))
    };
    for _ in 0..MAX_PROBE_ROUNDS {
        assert_eq!(
            answer(&mut cursor),
            vec![EffectKind::Kernel(KernelEffect::SendEnvelopes {
                copy,
                from: Seq(5),
                through: Seq(5),
            })]
        );
    }
    assert_eq!(
        answer(&mut cursor),
        vec![EffectKind::Kernel(KernelEffect::SnapshotCatchupRequired {
            copy,
            barrier: Seq(11),
        })]
    );
    assert_eq!(cursor.stopped(), None);
    assert_eq!(rx(&module), &before, "a probe never changes the receiver");
}

// --- completion (§3.3) --------------------------------------------------------------------

#[retcd_test]
fn m7b_22_batch_completed_advances_applied_and_acks() {
    let mut module = module();
    send(&mut module, label(A), &golden());
    let effects = step(&mut module, &committed(0, 11));
    assert_eq!(effects.len(), 1);
    assert_eq!(
        reply_of(&effects[0]),
        AppendOutcome::Accepted(ack(11, 11, 10))
    );
    let rx = rx(&module);
    assert_eq!(
        (rx.staged(), rx.applied_head(), rx.accept_head()),
        (None, head(11), head(11))
    );
    assert_eq!(
        (rx.received_seq(), rx.buffered_applied_seq()),
        (ReceivedSeq(11), AppliedSeq(11))
    );
    assert_eq!(rx.durable_seq(), DurableSeq(10), "applied is not durable");
    assert_eq!(rx.history().digest_at(Seq(11)), Some(d(11)));

    // The slot is free again: seq 12 stages as the next batch.
    let effects = send(&mut module, label(A), &chain(12).pop().expect("12"));
    assert_eq!(staged_batch(&effects), (BatchId(1), GEN, Seq(12)));
}

#[retcd_test]
fn storage_answers_for_another_batch_are_declined() {
    let mut module = module();
    send(&mut module, label(A), &golden());
    declined(&mut module, &committed(1, 11));
    declined(&mut module, &commit_failed(1));
}

/// M7B-23's buildable half, at every `StorageFault`. Not the row: design §3.3 also emits a
/// `StorageFault` effect for A1, and no carrier for it has landed (handoff contract ask).
#[retcd_test]
fn commit_failed_drops_the_staged_record_and_asks_again_without_quarantine() {
    use StorageFault::{Corrupt, FlushFailed, HostCrash, ProcessCrash, WriteFailed};
    for fault in [WriteFailed, FlushFailed, ProcessCrash, HostCrash, Corrupt] {
        let mut module = module();
        send(&mut module, label(A), &golden());
        let effects = step(&mut module, &commit_failed_with(0, fault));
        assert_eq!(effects.len(), 1);
        assert_eq!(
            reply_of(&effects[0]),
            rejected(AppendReject::NeedPrefix {
                have: Seq(10),
                head_digest: d(10),
            })
        );
        let rx_ = rx(&module);
        assert_eq!(
            (rx_.staged(), rx_.accept_head(), rx_.applied_head()),
            (None, head(10), head(10))
        );
        assert_eq!(rx_.received_seq(), ReceivedSeq(10));
        assert_eq!(rx_.quarantine(), None, "{fault:?}");

        // The same record is welcome again.
        let effects = send(&mut module, label(A), &golden());
        assert_eq!(staged_batch(&effects), (BatchId(1), GEN, Seq(11)));
    }
}

#[retcd_test]
fn m7b_24_flush_completed_raises_durable_to_max_for_this_generation_only() {
    let mut module = applied_to(12);
    // Only this partition's prefixes in this generation count, and the highest of them wins.
    let effects = step(
        &mut module,
        &flushed(&[
            (P, GEN, 11),
            (P, Generation(2), 20),
            (PartitionId(9), GEN, 99),
            (P, GEN, 9),
        ]),
    );
    assert_eq!(effects.len(), 1);
    assert_eq!(
        reply_at(&effects[0], A, UNSOLICITED, CONFIG),
        AppendOutcome::Accepted(ack(12, 12, 11))
    );
    assert_eq!(rx(&module).durable_seq(), DurableSeq(11));

    // A later, lower flush leaves durable where it is: `max`, a named no-op.
    let effects = step(&mut module, &flushed(&[(P, GEN, 9)]));
    assert_eq!(
        effects,
        [ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::NothingOutstanding
        ))]
    );
    assert_eq!(rx(&module).durable_seq(), DurableSeq(11));

    // A false report past the applied head moves durable to the applied head and no further.
    let effects = step(&mut module, &flushed(&[(P, GEN, 99)]));
    assert_eq!(
        reply_at(&effects[0], A, UNSOLICITED, CONFIG),
        AppendOutcome::Accepted(ack(12, 12, 12))
    );

    // Nothing new to confirm: a named no-op.
    let before = rx(&module).clone();
    let effects = step(&mut module, &flushed(&[(P, GEN, 12)]));
    assert_eq!(
        effects,
        [ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::NothingOutstanding
        ))]
    );
    assert_eq!(rx(&module), &before);
}

#[retcd_test]
fn a_flush_cannot_make_a_staged_record_durable() {
    let mut module = module();
    send(&mut module, label(A), &golden());
    let effects = step(&mut module, &flushed(&[(P, GEN, 11)]));
    assert_eq!(
        effects,
        [ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::NothingOutstanding
        ))]
    );
    assert_eq!(rx(&module).durable_seq(), DurableSeq(10));
}

#[retcd_test]
fn one_flush_reaches_every_receiver_on_the_node_each_under_its_own_partition() {
    const Q: PartitionId = PartitionId(5);
    let mut module = Replication::new();
    module.install_receiver(receiver(P, 9));
    module.install_receiver(receiver(Q, 9));
    let ev = flushed(&[(Q, GEN, 10), (P, GEN, 10)]);
    let effects = module.step(&ctx(), &ev).expect("both answer");
    assert_eq!(effects.len(), 2, "{effects:?}");
    for (effect, partition) in effects.iter().zip([P, Q]) {
        assert_eq!(
            (effect.correlation, effect.from, effect.partition),
            (ev.correlation, ModuleName::Replication, partition)
        );
        let AppendOutcome::Accepted(ack) = reply_at(&effect.kind, A, UNSOLICITED, CONFIG) else {
            panic!("expected an ACK, got {effect:?}");
        };
        assert_eq!(
            (ack.partition, ack.progress.durable),
            (partition, DurableSeq(10))
        );
    }
    // Another node's flush is not B's.
    let mut elsewhere = flushed(&[(P, GEN, 10)]);
    elsewhere.node = C;
    declined(&mut module, &elsewhere);
}

#[retcd_test]
fn a_quarantined_copy_mirrors_storage_but_withholds_every_ack() {
    let quarantined_while_staged = || {
        let mut module = module();
        send(&mut module, label(A), &golden());
        let mut corrupt = chain(12).pop().expect("12");
        corrupt.record_digest.0[0] ^= 1;
        send(&mut module, label(A), &corrupt);
        module
    };
    let proof = Some(AppendReject::CorruptHistory { at: Seq(12) });

    let mut module = quarantined_while_staged();
    assert_eq!(rx(&module).quarantine(), proof);
    assert_eq!(step(&mut module, &committed(0, 11)), [withheld()]);
    assert_eq!(rx(&module).applied_head(), head(11));
    assert_eq!(step(&mut module, &flushed(&[(P, GEN, 11)])), [withheld()]);
    assert_eq!(rx(&module).durable_seq(), DurableSeq(11));
    // Neither completion clears the quarantine.
    assert_eq!(rx(&module).quarantine(), proof);

    // A failed commit answers as row 0 would, not with an invitation to resend.
    let mut module = quarantined_while_staged();
    let effects = step(&mut module, &commit_failed(0));
    assert_eq!(effects.len(), 1);
    assert_eq!(reply_of(&effects[0]), rejected(AppendReject::Quarantined));
    assert_eq!(rx(&module).quarantine(), proof);
}

// --- Recovered (§3.3) ---------------------------------------------------------------------

#[retcd_test]
fn recovered_on_a_matching_cutoff_re_anchors_there_and_adopts_the_new_lineage() {
    let mut module = applied_to(12);
    step(&mut module, &flushed(&[(P, GEN, 12)]));
    // Seq 13 is in flight when the takeover commits.
    send(&mut module, label(A), &chain(13).pop().expect("13"));

    let effects = step(&mut module, &recovered(11, d(11), takeover_config()));
    asks_new_primary(&effects, 11);
    let rx_ = rx(&module);
    assert_eq!(
        (rx_.staged(), rx_.applied_head(), rx_.received_seq()),
        (None, head(11), ReceivedSeq(11))
    );
    assert_eq!(
        rx_.durable_seq(),
        DurableSeq(11),
        "durable drops to the anchor"
    );
    assert_eq!(rx_.history().highest(), Some(Seq(11)));
    assert_eq!(
        rx_.lineage(),
        Lineage {
            partition: P,
            generation: NEW_GEN,
            owner_epoch: NEW_EPOCH,
        }
    );
    assert_eq!(rx_.config(), &takeover_config());
    assert_eq!(
        rx_.root(),
        HistoryRoot {
            floor: Seq(11),
            base_digest: d(11),
            predecessor: Some(GEN),
        }
    );
    assert_eq!(rx_.last_partition_revision(), REVISION);
    assert_eq!(rx_.quarantine(), None);

    // The old lineage's in-flight batch no longer lands, and its primary no longer appends.
    declined(&mut module, &committed(2, 13));
    let old = chain(12).pop().expect("12").encode().expect("encode");
    assert_eq!(
        refused_in(&mut module, A, NEW_CONFIG, old),
        rejected(AppendReject::StaleGeneration { current: NEW_GEN })
    );
    // The new primary's next record stages in the new generation.
    let effects = send(
        &mut module,
        label(C),
        &taken_over(chain(12).pop().expect("12")),
    );
    assert_eq!(staged_batch(&effects), (BatchId(3), NEW_GEN, Seq(12)));
}

/// The plan's fixture: B at `(15, d15)`, 16 staged, durable 12, quarantined; the takeover's
/// cutoff is `(15, d15)`, the `Match` arm. Every field the receiver holds is asserted. The
/// design table's `authority` and `known_boot` rows have no receiver field: the receiver keeps
/// what it reads from the authority view as `lineage` (generation and owner epoch), and each
/// member's boot inside `config` (handoff question).
#[retcd_test]
fn m7b_27_recovered_rewrites_every_field_in_the_table() {
    let mut module = applied_to(15);
    step(&mut module, &flushed(&[(P, GEN, 12)]));
    send(&mut module, label(A), &chain(16).pop().expect("16"));
    let mut corrupt = chain(17).pop().expect("17");
    corrupt.record_digest.0[0] ^= 1;
    send(&mut module, label(A), &corrupt);
    let before = rx(&module).clone();
    assert_eq!(
        (before.staged().map(|s| s.head), before.durable_seq()),
        (Some(head(16)), DurableSeq(12))
    );
    assert!(before.quarantine().is_some());

    let effects = step(&mut module, &recovered(15, d(15), takeover_config()));
    asks_new_primary(&effects, 15);
    let rx_ = rx(&module);
    assert_eq!(
        rx_.lineage(),
        Lineage {
            partition: P,
            generation: NEW_GEN,
            owner_epoch: NEW_EPOCH,
        }
    );
    assert_eq!(rx_.config(), &takeover_config());
    assert_eq!(
        (rx_.applied_head(), rx_.accept_head(), rx_.staged()),
        (head(15), head(15), None)
    );
    assert_eq!(
        (rx_.received_seq(), rx_.buffered_applied_seq()),
        (ReceivedSeq(15), AppliedSeq(15))
    );
    assert_eq!(rx_.durable_seq(), DurableSeq(12), "min, never raised");
    assert_eq!(rx_.history().digest_at(Seq(16)), None);
    assert_eq!(rx_.history().highest(), Some(Seq(15)));
    assert_eq!(
        rx_.root(),
        HistoryRoot {
            floor: Seq(15),
            base_digest: d(15),
            predecessor: Some(GEN),
        }
    );
    assert_eq!(rx_.last_partition_revision(), REVISION);
    assert_eq!(rx_.quarantine(), None);
    assert_eq!((rx_.partition(), rx_.node()), (P, B));
}

/// B at `(50, d50)`, holding nothing above; the cutoff is 100. The head stays, the floor moves,
/// and the new primary is asked for the gap.
#[retcd_test]
fn m7b_124_recovered_behind_the_cutoff_keeps_applied_and_asks_for_the_gap() {
    let mut module = Replication::new();
    module.install_receiver(receiver_at(P, 50, 50));
    let effects = step(&mut module, &recovered(100, d(100), takeover_config()));
    asks_new_primary(&effects, 50);
    let rx_ = rx(&module);
    assert_eq!(
        (rx_.applied_head(), rx_.accept_head()),
        (head(50), head(50))
    );
    assert_eq!(rx_.root().floor, Seq(100));
    assert_eq!(
        (rx_.received_seq(), rx_.buffered_applied_seq()),
        (ReceivedSeq(50), AppliedSeq(50))
    );
}

/// Durable 12 and a cutoff of 40, past this copy's head: durable stays 12, the head stays its
/// own, and the new primary is asked for what follows it.
#[retcd_test]
fn m7b_29_recovered_never_raises_durable() {
    let mut module = applied_to(12);
    step(&mut module, &flushed(&[(P, GEN, 12)]));
    let effects = step(&mut module, &recovered(40, d(40), takeover_config()));
    asks_new_primary(&effects, 12);
    let rx_ = rx(&module);
    assert_eq!(
        (rx_.applied_head(), rx_.durable_seq(), rx_.quarantine()),
        (head(12), DurableSeq(12), None)
    );
    assert_eq!(rx_.root().floor, Seq(40));
}

#[retcd_test]
fn recovered_on_a_differing_cutoff_quarantines_and_moves_no_head() {
    let mut module = applied_to(11);
    let effects = step(&mut module, &recovered(11, d(9), takeover_config()));
    assert_eq!(effects, [quarantine_alert()]);
    let rx_ = rx(&module).clone();
    assert_eq!(
        rx_.quarantine(),
        Some(AppendReject::DivergentHistory { at: Seq(11) })
    );
    assert_eq!(
        (rx_.applied_head(), rx_.durable_seq()),
        (head(11), DurableSeq(10))
    );
    assert_eq!(rx_.history().highest(), Some(Seq(11)));
    assert_eq!(rx_.lineage().generation, NEW_GEN);
    // The new primary is refused like everyone else.
    let next = taken_over(chain(12).pop().expect("12"));
    assert_eq!(
        refused_in(&mut module, C, NEW_CONFIG, next.encode().expect("encode")),
        rejected(AppendReject::Quarantined)
    );
}

/// Every other event kind a quarantined receiver can be offered, answered or declined, leaves
/// the quarantine set. The receiver has 11 staged so that storage events for it still route.
/// The plan's `PinnedConfig` and `ControlBoot` have no event to R1; `ConfigChanged` and a
/// node reboot stand for them.
#[retcd_test]
fn m7b_28_recovered_is_the_only_clearer_of_quarantine() {
    let mut module = module();
    send(&mut module, label(A), &golden());
    let mut corrupt = chain(12).pop().expect("12");
    corrupt.record_digest.0[0] ^= 1;
    send(&mut module, label(A), &corrupt);
    let proof = rx(&module).quarantine();
    assert!(proof.is_some());

    let others = [
        delivered(label(A), golden().encode().expect("encode")),
        commit_failed(0),
        committed(0, 11),
        flushed(&[(P, GEN, 11)]),
        event(EventKind::Kernel(KernelEvent::ConfigChanged(
            takeover_config(),
        ))),
        event(EventKind::Node(NodeLifecycle::Rebooted { boot: BootId(7) })),
        event(EventKind::Timer(TimerFired {
            id: TimerId(1),
            version: TimerVersion(1),
            scheduled_at: Tick(0),
        })),
    ];
    for ev in &others {
        // Answered or declined; either way the quarantine stands.
        let _ = module.step(&ctx(), ev);
        assert_eq!(rx(&module).quarantine(), proof, "{ev:?}");
    }

    let effects = step(&mut module, &recovered(10, d(10), takeover_config()));
    asks_new_primary(&effects, 10);
    assert_eq!(rx(&module).quarantine(), None);
    let effects = send(&mut module, label(C), &taken_over(golden()));
    assert_eq!(staged_batch(&effects), (BatchId(1), NEW_GEN, Seq(11)));
}

#[retcd_test]
fn recovered_with_a_pin_this_copy_cannot_serve_under_is_invalid() {
    use ReplicaRole::{Primary, RegularSecondary as Regular, Shadow};
    let pins = [
        // B promoted: the tracker's handover, not the receiver's.
        pinned([Regular, Primary, Regular, Shadow]),
        // Nobody to ask for the prefix.
        pinned([Regular, Regular, Regular, Shadow]),
        // B dropped, B's copy on another node, another partition.
        PartitionConfig::new(
            P,
            NEW_CONFIG,
            vec![member(0, A, Regular), member(2, C, Primary)],
        ),
        PartitionConfig::new(
            P,
            NEW_CONFIG,
            vec![member(1, D, Regular), member(2, C, Primary)],
        ),
        PartitionConfig::new(
            PartitionId(9),
            NEW_CONFIG,
            vec![member(1, B, Regular), member(2, C, Primary)],
        ),
    ];
    for pin in pins {
        let mut module = module();
        let before = rx(&module).clone();
        let effects = step(&mut module, &recovered(10, d(10), pin));
        assert_eq!(
            effects,
            [ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::InvalidConfig
            ))]
        );
        assert_eq!(rx(&module), &before);
    }
}

#[retcd_test]
fn a_predecessor_record_at_or_below_the_floor_must_match_the_committed_root() {
    // B holds 10; the committed root ends at (11, d11) of generation GEN.
    let behind = || {
        let mut module = module();
        step(&mut module, &recovered(11, d(11), takeover_config()));
        module
    };
    // The predecessor's own record 11, carried by the new primary: rows 4-6 do not apply.
    let effects = send(&mut behind(), label(C), &golden());
    assert_eq!(staged_batch(&effects), (BatchId(0), NEW_GEN, Seq(11)));

    // Its twin: same seq, same generation, chained on the same d10, not the root's record.
    let mut module = behind();
    let effects = send(&mut module, label(C), &envelope(11, d(10), b"other"));
    let proof = AppendReject::DivergentHistory { at: Seq(11) };
    assert_eq!(
        reply_at(&effects[0], C, FRAME_ID, NEW_CONFIG),
        rejected(proof)
    );
    assert_eq!(effects[1..], [quarantine_alert()]);
    assert_eq!(rx(&module).quarantine(), Some(proof));

    // Above the floor the predecessor's generation is stale again.
    let above = chain(12).pop().expect("12").encode().expect("encode");
    assert_eq!(
        refused_in(&mut behind(), C, NEW_CONFIG, above),
        rejected(AppendReject::StaleGeneration { current: NEW_GEN })
    );
    // The sender row is never skipped: the old primary cannot deliver history.
    let history = golden().encode().expect("encode");
    assert_eq!(
        refused_in(&mut behind(), A, NEW_CONFIG, history),
        rejected(AppendReject::NotAMember)
    );
}

// --- RecoveryAppend (§3.2a) ---------------------------------------------------------------

/// `module()` after a takeover that re-anchored at `(10, d10)` at revision `REVISION`.
fn recovered_at_ten() -> Replication {
    let mut module = module();
    step(&mut module, &recovered(10, d(10), takeover_config()));
    module
}

#[retcd_test]
fn a_fenced_transfer_is_admitted_from_the_designated_source() {
    let mut module = module();
    let body = recovery_body(&fence(2, EPOCH, Revision(0)), &golden());
    let effects = send_bytes(&mut module, label(C), body);
    assert_eq!(staged_batch(&effects), (BatchId(0), GEN, Seq(11)));
    // Completion answers the transfer's sender under its request id.
    let effects = step(&mut module, &committed(0, 11));
    assert_eq!(
        reply_at(&effects[0], C, FRAME_ID, CONFIG),
        AppendOutcome::Accepted(ack(11, 11, 10))
    );
}

/// The credential's shape is pinned by an exhaustive pattern: a `prior_grant_id` or a
/// `recoverer` field (the plan's Q-53/Q-55 greps) would stop this row compiling.
#[retcd_test]
fn m7b_120_recovery_append_5r_epoch_alone_and_6r_revision() {
    let FenceCredential {
        partition: _,
        prior_generation: _,
        prior_owner_epoch: _,
        control_revision: _,
        sender: _,
    } = fence(2, EPOCH, Revision(0));
    // 5R: the epoch alone, in either direction.
    for epoch in [OwnerEpoch(EPOCH.0 - 1), OwnerEpoch(EPOCH.0 + 1)] {
        let body = recovery_body(&fence(2, epoch, Revision(0)), &golden());
        assert_eq!(
            refused_in(&mut module(), C, CONFIG, body),
            rejected(AppendReject::StaleFence)
        );
    }
    // 6R: after a takeover committed at REVISION, a fence read before it is stale; one read at
    // it is not.
    let env = taken_over(golden());
    let older = recovery_body(&fence(2, NEW_EPOCH, Revision(REVISION.0 - 1)), &env);
    assert_eq!(
        refused_in(&mut recovered_at_ten(), C, NEW_CONFIG, older),
        rejected(AppendReject::StaleFence)
    );
    let current = recovery_body(&fence(2, NEW_EPOCH, REVISION), &env);
    let effects = send_bytes(&mut recovered_at_ten(), label(C), current);
    assert_eq!(staged_batch(&effects), (BatchId(0), NEW_GEN, Seq(11)));
}

/// Our fixture's source is C (the plan's B′). After the takeover A is a regular member, so it
/// plays the plan's authenticated regular replayer.
#[retcd_test]
fn m7b_121_replayed_fence_credential_from_a_second_member_is_not_a_member() {
    let for_c = recovery_body(&fence(2, NEW_EPOCH, REVISION), &taken_over(golden()));
    assert_eq!(
        refused_in(&mut recovered_at_ten(), A, NEW_CONFIG, for_c.clone()),
        rejected(AppendReject::NotAMember)
    );
    // Twin: the same credential from the copy it names is admitted.
    let effects = send_bytes(&mut recovered_at_ten(), label(C), for_c);
    assert_eq!(staged_batch(&effects), (BatchId(0), NEW_GEN, Seq(11)));
    // Second twin, the membership half: the shadow D, named by its own credential.
    let for_d = recovery_body(&fence(3, NEW_EPOCH, REVISION), &taken_over(golden()));
    assert_eq!(
        refused_in(&mut recovered_at_ten(), D, NEW_CONFIG, for_d),
        rejected(AppendReject::NotAMember)
    );
}

/// Send `env` into a fresh receiver from `prepare` twice: as an `Append` from the pinned primary
/// A, and as a `RecoveryAppend` from C under a valid credential naming C. Return each reply and
/// each receiver afterwards.
fn both_ways(
    prepare: impl Fn() -> Replication,
    env: &ReplicationEnvelope,
) -> [(AppendOutcome, AppendReceiver); 2] {
    let valid = fence(2, EPOCH, Revision(0));
    [
        (A, env.encode().expect("encode")),
        (C, recovery_body(&valid, env)),
    ]
    .map(|(from, body)| {
        let mut module = prepare();
        let effects = send_bytes(&mut module, label(from), body);
        (
            reply_at(&effects[0], from, FRAME_ID, CONFIG),
            rx(&module).clone(),
        )
    })
}

/// The M7B-03..07, 14, 18 and 20 deltas give the same answer and leave the same receiver
/// whether they arrive as an `Append` or under a fence. So a fence cannot overwrite a divergent
/// suffix. Row 0 (M7B-02) is not bypassed either.
#[retcd_test]
fn m7b_122_recovery_append_reuses_rows_0_to_4_7_and_8_unchanged() {
    let mut version = golden();
    version.header.protocol_version += 1;
    let mut large = golden();
    large.mutations = vec![large.mutations[0].clone(); MAX_MUTATIONS + 1];
    let mut elsewhere = golden();
    elsewhere.header.partition = PartitionId(9);
    let mut corrupt = golden();
    corrupt.record_digest.0[0] ^= 1;
    let cases = [
        (seal(version), AppendReject::IncompatibleVersion),
        (seal(large), AppendReject::TooLarge),
        (seal(elsewhere), AppendReject::WrongPartition),
        (
            under(golden(), Generation(2), EPOCH, CONFIG),
            AppendReject::StaleGeneration { current: GEN },
        ),
        (
            under(golden(), Generation(4), EPOCH, CONFIG),
            AppendReject::NeedLineage { current: GEN },
        ),
        (
            corrupt.clone(),
            AppendReject::CorruptHistory { at: Seq(11) },
        ),
        (other_10(), AppendReject::DivergentHistory { at: Seq(10) }),
        (
            envelope(11, d(9), b"v"),
            AppendReject::DivergentHistory { at: Seq(11) },
        ),
    ];
    for (env, want) in cases {
        let [(append, after_append), (recovery, after_recovery)] = both_ways(module, &env);
        assert_eq!(append, rejected(want));
        assert_eq!(recovery, append, "{want:?}");
        assert_eq!(after_recovery, after_append, "{want:?}");
    }

    let quarantined = || {
        let mut module = module();
        send(&mut module, label(A), &corrupt);
        module
    };
    for (reply, _) in both_ways(quarantined, &golden()) {
        assert_eq!(reply, rejected(AppendReject::Quarantined));
    }
}

/// M7B-121's pre-takeover half: being the pinned primary does not make a peer the named source.
#[retcd_test]
fn a_captured_credential_is_refused_from_anyone_but_its_named_source() {
    // 6R′: the credential names C; A replaying it — even as the pinned primary — is not C.
    let for_c = recovery_body(&fence(2, EPOCH, Revision(0)), &golden());
    assert_eq!(
        refused_in(&mut module(), A, CONFIG, for_c),
        rejected(AppendReject::NotAMember)
    );
    // C presenting a credential that names A.
    let for_a = recovery_body(&fence(0, EPOCH, Revision(0)), &golden());
    assert_eq!(
        refused_in(&mut module(), C, CONFIG, for_a),
        rejected(AppendReject::NotAMember)
    );
}

#[retcd_test]
fn a_fence_keeps_rows_one_to_four() {
    let valid = fence(2, EPOCH, Revision(0));
    let mut elsewhere = valid;
    elsewhere.partition = PartitionId(9);
    assert_eq!(
        refused_in(
            &mut module(),
            C,
            CONFIG,
            recovery_body(&elsewhere, &golden())
        ),
        rejected(AppendReject::WrongPartition)
    );
    let old = under(golden(), Generation(2), EPOCH, CONFIG);
    assert_eq!(
        refused_in(&mut module(), C, CONFIG, recovery_body(&valid, &old)),
        rejected(AppendReject::StaleGeneration { current: GEN })
    );
    // An unknown version on the wrapper is row 1's refusal; a truncated one is not an append.
    let mut future = recovery_body(&valid, &golden()).to_vec();
    future[4] = 0xFF;
    assert_eq!(
        refused_in(&mut module(), C, CONFIG, Bytes::from(future)),
        rejected(AppendReject::IncompatibleVersion)
    );
    let mut module = module();
    let before = rx(&module).clone();
    let truncated = recovery_body(&valid, &golden()).slice(..20);
    assert_eq!(
        send_bytes(&mut module, label(C), truncated),
        [ignored(KernelIgnoredReason::Error(
            ErrorKind::InvalidArgument
        ))]
    );
    assert_eq!(rx(&module), &before);
}

// --- routing ------------------------------------------------------------------------------

#[retcd_test]
fn module_declines_what_is_not_r1s_and_stays_unavailable() {
    let mut module = module();
    assert_eq!(module.capability(), CapabilityState::Unavailable);
    // Not an R1 magic.
    declined(
        &mut module,
        &delivered(label(A), Bytes::from_static(b"XXXXpayload")),
    );
    // A reply is the primary tracker's; a secondary's receiver does not consume one.
    declined(
        &mut module,
        &delivered(label(A), encode_reply(&AppendOutcome::AlreadyHave)),
    );
    // Storage answering a batch this receiver never staged, and a failed flush.
    declined(&mut module, &committed(0, 11));
    declined(&mut module, &commit_failed(0));
    declined(
        &mut module,
        &storage(StorageEvent::FlushFailed {
            ticket: FlushTicket(1),
            fault: StorageFault::FlushFailed,
        }),
    );
    // A flush naming no prefix of any receiver on this node.
    declined(
        &mut module,
        &flushed(&[(P, Generation(2), 11), (PartitionId(9), GEN, 11)]),
    );
    // No receiver for this node.
    let mut elsewhere = delivered(label(A), golden().encode().expect("encode"));
    elsewhere.node = C;
    declined(&mut module, &elsewhere);
    // Nothing installed at all.
    declined(
        &mut Replication::new(),
        &delivered(label(A), golden().encode().expect("encode")),
    );
}

#[retcd_test]
fn malformed_r1_frame_is_ignored_as_invalid_argument() {
    let mut body = golden().encode().expect("encode").to_vec();
    body.pop();
    malformed_in(&mut module(), Bytes::from(body));
}

/// Send `body` from A into `module`; assert the malformed answer — one `Ignored`, no reply, no
/// batch, no quarantine — and no state change.
fn malformed_in(module: &mut Replication, body: Bytes) {
    let before = rx(module).clone();
    let effects = send_bytes(module, label(A), body);
    assert_eq!(
        effects,
        vec![ignored(KernelIgnoredReason::Error(
            ErrorKind::InvalidArgument
        ))]
    );
    assert_eq!(rx(module), &before, "a malformed append changes nothing");
}

/// Lead ruling F-1: an envelope may write `User` and `Dedup` only. The receiver writes History
/// and Progress itself, and Meta is recovery's, so an envelope carrying any of them is
/// malformed. Tester probe p09's shape: the golden append also writing `History[5]` and `Meta`.
#[retcd_test]
fn an_envelope_writing_a_namespace_r1_or_recovery_owns_is_malformed() {
    let write = |ns, key: &'static [u8]| Write {
        ns,
        key: Bytes::from_static(key),
        value: Some(Bytes::from_static(b"x")),
    };
    let mut p09 = golden();
    p09.mutations
        .push(write(Namespace::History, &[5, 0, 0, 0, 0, 0, 0, 0]));
    p09.mutations.push(write(Namespace::Meta, b"lineage"));
    malformed_in(&mut module(), seal(p09).encode().expect("encode"));
    for ns in [Namespace::History, Namespace::Progress, Namespace::Meta] {
        let mut env = golden();
        env.mutations.push(write(ns, b"k"));
        malformed_in(&mut module(), seal(env).encode().expect("encode"));
    }
    // `Dedup` beside `User` is the shape the primary sends, and stages.
    let mut env = golden();
    env.mutations.push(write(Namespace::Dedup, b"r"));
    let effects = send(&mut module(), label(A), &seal(env));
    assert!(matches!(effects[..], [EffectKind::Store(_)]), "{effects:?}");
}

/// Lead ruling F-2: seq 0 is the ladder's `(0, ROOT)` placeholder, not a record, so a seq-0
/// append is malformed and never proof — on a fresh copy (tester probe p08b) and at any head.
#[retcd_test]
fn a_seq_zero_append_is_malformed_on_a_fresh_copy_and_at_any_head() {
    let fresh = AppendReceiver::new(ReceiverInit {
        config: config(),
        own: CopyId(1),
        lineage: Lineage {
            partition: P,
            generation: GEN,
            owner_epoch: EPOCH,
        },
        head: Head {
            seq: Seq::ZERO,
            digest: Digest::ROOT,
        },
        durable: DurableSeq(0),
    })
    .expect("a fresh copy");
    let p08b = envelope(0, Digest::ROOT, b"v").encode().expect("encode");
    let mut module = Replication::new();
    module.install_receiver(fresh);
    malformed_in(&mut module, p08b.clone());
    malformed_in(&mut self::module(), p08b);
}

#[retcd_test]
fn receiver_refuses_an_impossible_start() {
    let init = |own, durable| ReceiverInit {
        config: config(),
        own: CopyId(own),
        lineage: Lineage {
            partition: P,
            generation: GEN,
            owner_epoch: EPOCH,
        },
        head: Head {
            seq: Seq(10),
            digest: d(10),
        },
        durable: DurableSeq(durable),
    };
    assert!(AppendReceiver::new(init(1, 10)).is_ok());
    // The primary, a copy not in the config, and durable above applied.
    for bad in [init(0, 10), init(9, 10), init(1, 11)] {
        assert!(AppendReceiver::new(bad).is_err());
    }
}

// --- wire ---------------------------------------------------------------------------------

#[retcd_test]
fn every_reply_round_trips_and_nothing_else_decodes() {
    let ack = AppendAck {
        partition: P,
        generation: GEN,
        owner_epoch: EPOCH,
        config_version: CONFIG,
        from: B,
        boot: BootId(u64::MAX),
        role: ReplicaRole::Shadow,
        progress: ReplicaProgress {
            received: ReceivedSeq(3),
            buffered_applied: AppliedSeq(2),
            durable: DurableSeq(1),
        },
        digest_at_buffered: d(2),
    };
    let rejects = [
        AppendReject::Quarantined,
        AppendReject::IncompatibleVersion,
        AppendReject::TooLarge,
        AppendReject::WrongPartition,
        AppendReject::StaleGeneration { current: GEN },
        AppendReject::NeedLineage { current: GEN },
        AppendReject::StaleEpoch { current: EPOCH },
        AppendReject::UnknownEpoch { current: EPOCH },
        AppendReject::StaleConfig { current: CONFIG },
        AppendReject::NeedConfig { current: CONFIG },
        AppendReject::NotAMember,
        AppendReject::CorruptHistory { at: Seq(8) },
        AppendReject::DivergentHistory { at: Seq(9) },
        AppendReject::NeedPrefix {
            have: Seq(10),
            head_digest: d(10),
        },
        AppendReject::StaleFence,
        AppendReject::Unauthenticated,
    ];
    let mut outcomes = vec![
        AppendOutcome::Accepted(ack),
        AppendOutcome::Busy {
            accepted_through: Seq(4),
        },
        AppendOutcome::AlreadyHave,
        AppendOutcome::ProbeDigestAt { seq: Seq(5) },
    ];
    outcomes.extend(rejects.map(AppendOutcome::Rejected));
    for outcome in outcomes {
        let bytes = encode_reply(&outcome);
        assert_eq!(&bytes[..4], REPLY_MAGIC);
        assert_eq!(decode_reply(&bytes).expect("round trip"), outcome);
        // Exact length both ways.
        let mut long = bytes.to_vec();
        long.push(0);
        assert!(decode_reply(&long).is_err());
        assert!(decode_reply(&bytes[..bytes.len() - 1]).is_err());
    }
    let mut future = encode_reply(&AppendOutcome::AlreadyHave).to_vec();
    future[4] = 0xFF;
    assert!(matches!(
        decode_reply(&future),
        Err(RdbError::IncompatibleVersion { .. })
    ));
}

// --- manual tester (R1 slice 1): rows for mutations the tests above did not notice ---------
//
// Each test below failed on one named mutant and passes on clean code. Evidence:
// `C:/rdbr1t/evidence/mut/<mutant>.*` and `tester-r1-handoff.md`. Not `m7b_` rows: the
// developer adopts or renames them.

/// Mutants M02a (`>` became `>=`) and M02d (byte check moved after the full decode).
#[retcd_test]
fn tester_r1_byte_bound_is_inclusive_and_checked_before_decode() {
    // Exactly MAX_ENVELOPE_BYTES encoded bytes is inside the bound (spec §4.2 "at most").
    let base = golden().encode().expect("encode").len() - 1;
    let mut exact = golden();
    exact.mutations[0].value = Some(Bytes::from(vec![0u8; MAX_ENVELOPE_BYTES - base]));
    let exact = seal(exact);
    assert_eq!(exact.encode().expect("encode").len(), MAX_ENVELOPE_BYTES);
    let mut module = module();
    let effects = send(&mut module, label(A), &exact);
    assert!(matches!(effects[..], [EffectKind::Store(_)]), "{effects:?}");

    // A valid header followed by more than 1 MiB that would not decode: row 2 answers, because
    // row 2 runs before the body is decoded (design §3.2 "before any hashing"; plan M7B-04).
    let mut huge = golden().encode().expect("encode")[..46].to_vec();
    huge.resize(MAX_ENVELOPE_BYTES + 1, 0xAB);
    assert_eq!(
        refused_bytes(Bytes::from(huge)),
        rejected(AppendReject::TooLarge)
    );
}

/// Mutants M06c (row 6's peer check hoisted above row 0) and M06e (peer check above rows 4-6).
#[retcd_test]
fn tester_r1_row_6_peer_half_runs_after_rows_0_to_6_version() {
    // Row 0 before row 6: a quarantined receiver answers a non-member QUARANTINED.
    let mut module = module();
    let mut corrupt = golden();
    corrupt.record_digest.0[0] ^= 1;
    send(&mut module, label(A), &corrupt);
    let effects = send(&mut module, label(C), &golden());
    match &effects[..] {
        [EffectKind::Send(SendEffect::Unicast { frame, .. })] => assert_eq!(
            decode_reply(&frame.body).expect("reply"),
            rejected(AppendReject::Quarantined)
        ),
        other => panic!("{other:?}"),
    }
    // Row 5 before row 6: a stale epoch from a non-primary reports the epoch.
    let mut stale = golden();
    stale.header.owner_epoch = OwnerEpoch(4);
    let mut module = self::module();
    let effects = send(&mut module, label(C), &seal(stale));
    match &effects[..] {
        [EffectKind::Send(SendEffect::Unicast { frame, .. })] => assert_eq!(
            decode_reply(&frame.body).expect("reply"),
            rejected(AppendReject::StaleEpoch { current: EPOCH })
        ),
        other => panic!("{other:?}"),
    }
}

/// Mutant M08c (gap answer built from the applied head, not the accept head).
#[retcd_test]
fn tester_r1_gap_while_staged_reports_the_accept_head() {
    let mut module = module();
    send(&mut module, label(A), &golden());
    let staged = rx(&module).clone();
    let effects = send(&mut module, label(A), &chain(13).pop().expect("13"));
    assert_eq!(effects.len(), 1);
    assert_eq!(
        reply_of(&effects[0]),
        rejected(AppendReject::NeedPrefix {
            have: Seq(11),
            head_digest: d(11),
        })
    );
    assert_eq!(rx(&module), &staged);
}

/// Mutant M09c (NotRetained quarantines at the bottom of the ladder only).
#[retcd_test]
fn tester_r1_not_retained_at_both_ends_only_probes() {
    for seq in [1, 9] {
        // The canonical record there, and a forged one: absence is not proof either way.
        for env in [
            chain(seq).pop().expect("seq"),
            envelope(seq, Digest([0xEE; 32]), b"forged"),
        ] {
            assert_eq!(
                refused(&env),
                AppendOutcome::ProbeDigestAt { seq: Seq(seq) }
            );
        }
    }
}

/// Mutant M10c (a duplicate resets `received_seq` to the applied head).
#[retcd_test]
fn tester_r1_duplicate_while_staged_changes_nothing() {
    let mut module = module();
    send(&mut module, label(A), &golden());
    let staged = rx(&module).clone();
    let effects = send(&mut module, label(A), &chain(10).pop().expect("10"));
    assert_eq!(reply_of(&effects[0]), AppendOutcome::AlreadyHave);
    assert_eq!(
        rx(&module),
        &staged,
        "a duplicate is idempotent while staged too"
    );
}

/// Mutant M11b (unauthenticated gate skipped on a quarantined receiver).
#[retcd_test]
fn tester_r1_unauthenticated_label_gets_no_reply_even_when_quarantined() {
    let mut module = module();
    let mut corrupt = golden();
    corrupt.record_digest.0[0] ^= 1;
    send(&mut module, label(A), &corrupt);
    let forged = PeerLabel {
        authenticated: false,
        ..label(A)
    };
    assert_eq!(
        send(&mut module, forged, &golden()),
        vec![EffectKind::Kernel(KernelEffect::Ignored {
            reason: KernelIgnoredReason::AppendRejected(AppendReject::Unauthenticated),
        })]
    );
}

/// Mutants M12a (two reject tags swapped in both directions) and M12b (a `u64` written big
/// endian in both directions). A round trip cannot see a symmetric change; the bytes can.
#[retcd_test]
fn tester_r1_reply_bytes_match_the_documented_layout() {
    let head = |tag: u8| {
        let mut out = REPLY_MAGIC.to_vec();
        out.extend_from_slice(&ENVELOPE_VERSION.to_le_bytes());
        out.push(tag);
        out
    };
    let with_u64 = |mut out: Vec<u8>, value: u64| {
        out.extend_from_slice(&value.to_le_bytes());
        out
    };
    let busy = AppendOutcome::Busy {
        accepted_through: Seq(0x0102_0304_0506_0708),
    };
    assert_eq!(
        encode_reply(&busy).to_vec(),
        with_u64(head(2), 0x0102_0304_0506_0708)
    );
    let mut stale = head(5);
    stale.push(7);
    assert_eq!(
        encode_reply(&rejected(AppendReject::StaleEpoch { current: EPOCH })).to_vec(),
        with_u64(stale, EPOCH.0)
    );
    let mut unknown = head(5);
    unknown.push(8);
    assert_eq!(
        encode_reply(&rejected(AppendReject::UnknownEpoch { current: EPOCH })).to_vec(),
        with_u64(unknown, EPOCH.0)
    );
    let mut need = head(5);
    need.push(14);
    let mut need = with_u64(need, 10);
    need.extend_from_slice(&d(10).0);
    assert_eq!(
        encode_reply(&rejected(AppendReject::NeedPrefix {
            have: Seq(10),
            head_digest: d(10),
        }))
        .to_vec(),
        need
    );
}

/// Tester row (re-gate, B-R37): the shape half runs in row 2, so it wins over rows 3 to 7. A
/// malformed envelope that is also on the wrong partition, generation, epoch or config, sent by
/// a copy that is not the primary, or carrying a corrupt digest, is `Ignored(InvalidArgument)`
/// with no reply and no quarantine. Kills the `well_formed` check moved after row 6 (a reply
/// leaks) and moved after row 7 (a corrupt malformed envelope quarantines `CorruptHistory`).
#[retcd_test]
fn tester_r1_shape_half_runs_in_row_2_before_rows_3_to_7() {
    let smuggling = || {
        let mut env = golden();
        env.mutations.push(Write {
            ns: Namespace::History,
            key: Bytes::copy_from_slice(&5u64.to_be_bytes()),
            value: Some(Bytes::from_static(b"x")),
        });
        seal(env)
    };
    let malformed_from = |from: NodeId, env: ReplicationEnvelope, case: &str| {
        let mut module = module();
        let before = rx(&module).clone();
        let effects = send(&mut module, label(from), &env);
        assert_eq!(
            effects,
            vec![ignored(KernelIgnoredReason::Error(
                ErrorKind::InvalidArgument
            ))],
            "{case}"
        );
        assert_eq!(
            rx(&module),
            &before,
            "{case}: a malformed append changes nothing"
        );
    };

    // Row 7 first: a corrupt digest on a malformed envelope never quarantines.
    let mut corrupt = smuggling();
    corrupt.record_digest.0[3] ^= 1;
    malformed_from(A, corrupt, "row 7 smuggling");
    let mut zero = envelope(0, Digest::ROOT, b"zero");
    zero.record_digest.0[0] ^= 1;
    malformed_from(A, zero, "row 7 seq 0");

    // Rows 3 to 6: each would reply, and a malformed envelope gets no reply.
    let mut wrong_partition = smuggling();
    wrong_partition.header.partition = PartitionId(9);
    malformed_from(A, seal(wrong_partition), "row 3 partition");
    malformed_from(
        A,
        under(smuggling(), Generation(2), EPOCH, CONFIG),
        "row 4 generation",
    );
    malformed_from(
        A,
        under(smuggling(), GEN, OwnerEpoch(4), CONFIG),
        "row 5 epoch",
    );
    malformed_from(
        A,
        under(smuggling(), GEN, EPOCH, ConfigVersion(6)),
        "row 6 config",
    );
    malformed_from(C, smuggling(), "row 6 sender");
}
