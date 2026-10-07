//! R1 `AppendReceiver`: the design §3.2 validation ladder (and §3.2a's recovery append), and the
//! §3.3 completion events, reached through `Replication::step`.
//!
//! `m7b_<n>_*` functions are the §3 rows, the receiver's §9 rows (M7B-120, 121, 122, 124),
//! its A1-view rows (M7B-157..160, lead ruling B-R53) and the receiver `Recovered` builds
//! (M7B-165, lead ruling B-R54) of
//! `docs/testing/test-plan-m7-kernel-b.md` (written after tester-kb-r1's slice-1 thumbs-up).
//! Where lead ruling F-4 overrode a plan literal (M7B-14, 16, 17), the row asserts the ruling
//! and says so. Plain-named functions are not plan
//! rows: developer scaffolding, supporting guards, and the manual tester's `tester_r1_*` rows.
//! Every test drives a real `Event` through the module, so "reachable through step" is what
//! each one proves as well as its ladder row.
//!
//! M7B-23 and M7B-25 assert the plan rows as re-worded by lead rulings L-R173 Q2 and Q3 (no
//! `StorageFault` alert carrier; `FlushFailed` declined). M7B-123 drives the primary's catch-up
//! cursor directly beside the receiver. M7B-139 asserts lead ruling B-R68 (`Differs` emits the
//! alert alone and retains by deleting nothing) and drives the tracker directly for its tracker
//! clause. M7B-125 drives a `Primary` whose bases came from its own rebuilds (lead rulings B-R71
//! and B-R71a).
//!
//! Not here: M7B-26 (sim).
//!
//! Fixture: three copies — A primary (node 1), B regular secondary (node 2, the receiver under
//! test), C regular secondary (node 3). B holds `(10, d10)` applied, durable 10, generation 3,
//! owner epoch 5, config 7. The golden append is seq 11 from A, chained on `d10`. Every
//! rejecting test changes one field of it.

use config_log::retcd_test;

use bytes::Bytes;
use rdb_core::contracts::authority::{
    AuthorityEvent, AuthorityView, DenyReason, FenceCredential, FencingProof, Lineage,
    PartitionMode, Revocation,
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
use rdb_core::contracts::protection::{AdmissionState, ReplicationLag};
use rdb_core::contracts::recovery::{
    CommittedRoot, DurableProof, LossRecord, RecoveryBarrier, RecoveryResult, RetainedStatusMap,
    SelectedLineage,
};
use rdb_core::contracts::storage::{
    Batch, DurablePrefix, Namespace, SnapshotRead, StorageEvent, StorageFault, StoreEffect, Write,
};
use rdb_core::contracts::time::{ControlTime, Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::{AckRejectReason, CapabilityState, Version};
use rdb_core::contracts::transport::{Frame, PeerLabel, SendEffect, TransportEvent};
use rdb_core::contracts::txn::Outcome;
use rdb_core::contracts::version::ENVELOPE_VERSION;
use rdb_core::replication::append::{
    AppendReceiver, Head, HistoryRoot, ReceiverInit, MAX_ENVELOPE_BYTES, MAX_MUTATIONS,
    PROGRESS_KEY, UNSOLICITED,
};
use rdb_core::replication::catchup::{retransmit_timer, CatchupCursor, MAX_PROBE_ROUNDS};
use rdb_core::replication::primary::{keepalive_timer, Primary, KEEPALIVE_MS};
use rdb_core::replication::progress::{DigestLadder, ProgressTracker, TrackerInit};
use rdb_core::replication::wire::{
    decode_recovery_append, decode_reply, encode_recovery_append, encode_reply, REPLY_MAGIC,
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

/// The partition's root: where a copy that holds nothing starts.
fn root_head() -> Head {
    Head {
        seq: Seq::ZERO,
        digest: Digest::ROOT,
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
    event_at(B, kind)
}

/// `kind` stepped on `node`, under the boot `label(node)` names.
fn event_at(node: NodeId, kind: EventKind) -> Event {
    Event {
        id: EventId(1),
        at: Tick(0),
        node,
        boot: BootId(u64::from(node.0)),
        partition: P,
        correlation: CorrelationId(9),
        kind,
    }
}

/// `body` from a sender whose authority is the lineage its record was sealed under — for a
/// recovery append, its credential's — and the golden lineage when the body names none. That
/// is the honest sender of every in-generation row; a row that separates the two uses
/// [`framed`].
fn delivered(from: PeerLabel, body: Bytes) -> Event {
    let golden = (
        Lineage {
            partition: P,
            generation: GEN,
            owner_epoch: EPOCH,
        },
        CONFIG,
    );
    let (sender, config) = match decode_recovery_append(&body) {
        Ok((fence, _)) => (
            Lineage {
                partition: fence.partition,
                generation: fence.prior_generation,
                owner_epoch: fence.prior_owner_epoch,
            },
            CONFIG,
        ),
        Err(_) => ReplicationEnvelope::decode_header(&body).map_or(golden, |header| {
            (
                Lineage {
                    partition: header.partition,
                    generation: header.generation,
                    owner_epoch: header.owner_epoch,
                },
                header.config_version,
            )
        }),
    };
    framed(from, sender, config, body)
}

/// `body` in a frame whose sender holds `sender` under `config` (lead ruling B-R58a).
fn framed(from: PeerLabel, sender: Lineage, config: ConfigVersion, body: Bytes) -> Event {
    event(EventKind::Transport(TransportEvent::Delivered {
        from,
        frame: Frame {
            id: FRAME_ID,
            protocol: ENVELOPE_VERSION,
            config,
            sender,
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
    fence_in(GEN, sender, epoch, revision)
}

/// [`fence`] issued in `generation`. The frame carrying it is sent on the credential's lineage
/// (lead ruling B-R59), so a receiver that adopted a newer generation needs one issued there.
fn fence_in(
    generation: Generation,
    sender: u8,
    epoch: OwnerEpoch,
    revision: Revision,
) -> FenceCredential {
    FenceCredential {
        partition: P,
        prior_generation: generation,
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
///
/// Each record here is sent by its honest sender, which `delivered` derives from its header, so
/// a lineage edit is the frame's as well. That puts partition, generation, epoch, configuration
/// and sender in the frame fence, which runs before row 2 (lead ruling B-R58a): `TooLarge`
/// follows `NotAMember`.
#[retcd_test]
fn m7b_15_ladder_reports_the_first_failure_and_walks_down_in_order() {
    type Row = (fn(&mut Broken), fn(Seq) -> AppendReject);
    let rows: [Row; 9] = [
        (
            |c| c.env.header.protocol_version += 1,
            |_| AppendReject::IncompatibleVersion,
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
            |c| c.env.mutations = vec![c.env.mutations[0].clone(); MAX_MUTATIONS + 1],
            |_| AppendReject::TooLarge,
        ),
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

/// Every [`StorageFault`] variant. The `match` has no wildcard, so a new variant fails to compile
/// here until it is listed, and a row that walks this list cannot silently skip it.
fn every_storage_fault() -> [StorageFault; 5] {
    use StorageFault::{Corrupt, FlushFailed, HostCrash, ProcessCrash, WriteFailed};
    let listed = |fault: StorageFault| match fault {
        WriteFailed | FlushFailed | ProcessCrash | HostCrash | Corrupt => fault,
    };
    [WriteFailed, FlushFailed, ProcessCrash, HostCrash, Corrupt].map(listed)
}

/// M7B-23 (design §3.3 `BatchFailed`; ADR-rdb-0005 §3; gate V1), at every `StorageFault`. The
/// staged record is dropped and nothing of it survives: both heads stay `(10, d10)`, `received`
/// falls back to 10, the ladder gains no rung at 11, and a flush naming 11 afterwards has nothing
/// to make durable. The sender is asked again with `NeedPrefix{have 10, head_digest d10}`, and
/// nothing is quarantined. **Update (lead rulings L-R173 Q2 and A-R25):** that reply is the only
/// effect. R1 raises no `Alert{StorageFault}`: A1 reads `CommitFailed` itself and fences the
/// partition, and no carrier was built. The landed field is `have`, not `from`. Renamed from
/// `commit_failed_drops_the_staged_record_and_asks_again_without_quarantine`, which asserted the
/// heads, the reply and the quarantine; the suffix and flush clauses are new here.
#[retcd_test]
fn m7b_23_batch_failed_leaves_no_partial_suffix_at_every_fault_kind() {
    for fault in every_storage_fault() {
        let mut module = module();
        send(&mut module, label(A), &golden());
        let effects = step(&mut module, &commit_failed_with(0, fault));
        assert_eq!(effects.len(), 1, "{fault:?}: {effects:?}");
        assert_eq!(
            reply_of(&effects[0]),
            rejected(AppendReject::NeedPrefix {
                have: Seq(10),
                head_digest: d(10),
            }),
            "{fault:?}"
        );
        let rx_ = rx(&module);
        assert_eq!(
            (rx_.staged(), rx_.accept_head(), rx_.applied_head()),
            (None, head(10), head(10)),
            "{fault:?}"
        );
        assert_eq!(
            (
                rx_.received_seq(),
                rx_.buffered_applied_seq(),
                rx_.durable_seq()
            ),
            (ReceivedSeq(10), AppliedSeq(10), DurableSeq(10)),
            "{fault:?}"
        );
        assert_eq!(
            (rx_.history().digest_at(Seq(11)), rx_.history().highest()),
            (None, Some(Seq(10))),
            "{fault:?}: no rung for the failed record"
        );
        assert_eq!(rx_.quarantine(), None, "{fault:?}");

        // No partial suffix to flush: a flush naming 11 moves nothing.
        let effects = step(&mut module, &flushed(&[(P, GEN, 11)]));
        assert_eq!(
            effects,
            [ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::NothingOutstanding
            ))],
            "{fault:?}"
        );
        assert_eq!(rx(&module).durable_seq(), DurableSeq(10), "{fault:?}");

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

/// M7B-25 (design §3.3 `FlushFailed`; ADR-rdb-0005 §3; gate V1 "no `DurableProof` without a
/// successful flush"). B has applied 12 and proved 10 durable. A failed flush, at every
/// `StorageFault` (a flush that completed partially is `FlushFailed` too, contract
/// `StorageFault::FlushFailed`), advances nothing: no ACK leaves, so no durable proof does, and
/// the receiver is byte-identical. **Update (lead ruling L-R173 Q3):** R1 does not consume
/// `FlushFailed`, so it is declined (`Unavailable`) rather than answered with the design's
/// telemetry; there is no `Alert` carrier. Near-miss: a flush that confirms less than was applied
/// (K-F-25's narrower `Flushed`) raises `durable` to what it confirmed and no further, and the
/// next full flush still raises it to the applied head.
#[retcd_test]
fn m7b_25_flush_failed_and_partial_flush_advance_nothing() {
    let mut module = applied_to(12);
    assert_eq!(
        (
            rx(&module).buffered_applied_seq(),
            rx(&module).durable_seq()
        ),
        (AppliedSeq(12), DurableSeq(10))
    );
    for (ticket, fault) in (1..).zip(every_storage_fault()) {
        declined(
            &mut module,
            &storage(StorageEvent::FlushFailed {
                ticket: FlushTicket(ticket),
                fault,
            }),
        );
        assert_eq!(rx(&module).durable_seq(), DurableSeq(10), "{fault:?}");
    }

    // A narrower confirmation raises durable to what it names, not to the applied head.
    let effects = step(&mut module, &flushed(&[(P, GEN, 11)]));
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(
        reply_at(&effects[0], A, UNSOLICITED, CONFIG),
        AppendOutcome::Accepted(ack(12, 12, 11))
    );
    // A failed flush after it still moves nothing.
    declined(
        &mut module,
        &storage(StorageEvent::FlushFailed {
            ticket: FlushTicket(9),
            fault: StorageFault::FlushFailed,
        }),
    );
    assert_eq!(rx(&module).durable_seq(), DurableSeq(11));
    let effects = step(&mut module, &flushed(&[(P, GEN, 12)]));
    assert_eq!(
        reply_at(&effects[0], A, UNSOLICITED, CONFIG),
        AppendOutcome::Accepted(ack(12, 12, 12))
    );
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

/// B at `(60, d60)`, then A's records 61..=120 each staged and committed, durable 110. Records
/// from `fork` on are a stale ex-owner's (`b"x"`, not the golden `b"v"`), so B ends at
/// `(120, d120')` and its ladder is dense from 60: golden below `fork`, stale from it.
fn took_from_stale_owner(fork: u64) -> Replication {
    let mut module = Replication::new();
    module.install_receiver(receiver_at(P, 60, 60));
    let mut records = chain(fork - 1).split_off(60);
    for seq in fork..=120 {
        let prev = records
            .last()
            .map_or_else(|| d(seq - 1), |env| env.record_digest);
        records.push(envelope(seq, prev, b"x"));
    }
    for (batch, env) in (0..).zip(&records) {
        send(&mut module, label(A), env);
        step(&mut module, &committed(batch, env.header.seq.0));
    }
    step(&mut module, &flushed(&[(P, GEN, 110)]));
    let rx_ = rx(&module);
    assert_eq!(
        (
            rx_.applied_head().seq,
            rx_.durable_seq(),
            rx_.history().len()
        ),
        (Seq(120), DurableSeq(110), 61)
    );
    assert_ne!(rx_.applied_head().digest, d(120));
    module
}

/// B's ACK in the lineage `recovered` installs, at `(seq, seq, seq)` with the digest `d(seq)`.
fn ack_after_takeover(seq: u64) -> AppendAck {
    AppendAck {
        generation: NEW_GEN,
        owner_epoch: NEW_EPOCH,
        config_version: NEW_CONFIG,
        ..ack(seq, seq, seq)
    }
}

/// M7B-139, lead ruling B-R68 (option a). `Recovered{cutoff 100, d100}` reaches a non-primary
/// copy that took 61..=120 from a stale ex-owner, and the copy looks the anchor up before it
/// adopts it. One fixture per arm of the lookup:
///
/// - `Match` (the ex-owner forked at 101): the M7B-27 table applies at `(100, d100)`.
/// - `Differs` (forked at 61, so rung 100 is stale): quarantine `DivergentHistory{100}`, and the
///   effects are exactly `[Alert{CorruptHistory}]`. "Retain" is B-R68's: no delete or truncate,
///   so heads, watermarks and the whole ladder are what they were, and only the control rows
///   (lineage, config, authority, floor, revision) move. Retention itself is F1's
///   `QuarantineSuffix`, not R1's. The copy's next ACK never reaches the tracker, and one that
///   did would be dropped and never qualify: the tracker clause drives C's cursor and tracker
///   directly, as M7B-41 does.
/// - `NotRetained`: the copy holds no rung at 100 and anchors on its highest rung below it,
///   then asks from there (M7B-124's behind path). The written arm is a copy behind the cutoff.
///   The plan's shape, rungs above the cutoff and one below it but none at it, needs a sparse
///   ladder, and this build cannot make one: the ladder is dense from its seeded floor and
///   nothing drops a rung (`DigestLadder`'s doc).
///
/// Update (lead, after tester-kbr1): the round-2 residual is now pinned. A copy seeded above the
/// cutoff holds no rung at or below it, and `on_recovered` answers
/// `Ignored{Error(InvalidArgument)}` and changes nothing. The code calls that a handoff
/// question, and no design row names an answer; the clause pins today's answer and that
/// nothing is adopted, and changes if the design names another.
#[retcd_test]
fn m7b_139_recovered_looks_up_the_anchor_before_adopting_it_three_arms() {
    let new_lineage = Lineage {
        partition: P,
        generation: NEW_GEN,
        owner_epoch: NEW_EPOCH,
    };
    let new_root = HistoryRoot {
        floor: Seq(100),
        base_digest: d(100),
        predecessor: Some(GEN),
    };

    // --- Match: the ex-owner forked above the cutoff.
    let mut module = took_from_stale_owner(101);
    let effects = step(&mut module, &recovered(100, d(100), takeover_config()));
    asks_new_primary(&effects, 100);
    let rx_ = rx(&module);
    assert_eq!(
        (rx_.lineage(), rx_.config()),
        (new_lineage, &takeover_config())
    );
    assert_eq!(
        (rx_.applied_head(), rx_.accept_head(), rx_.staged()),
        (head(100), head(100), None)
    );
    assert_eq!(
        (
            rx_.received_seq(),
            rx_.buffered_applied_seq(),
            rx_.durable_seq()
        ),
        (ReceivedSeq(100), AppliedSeq(100), DurableSeq(100))
    );
    assert_eq!(
        (rx_.history().highest(), rx_.history().digest_at(Seq(100))),
        (Some(Seq(100)), Some(d(100)))
    );
    assert_eq!(
        (rx_.root(), rx_.last_partition_revision(), rx_.quarantine()),
        (new_root, REVISION, None)
    );

    // --- Differs: rung 100 is the ex-owner's.
    let mut module = took_from_stale_owner(61);
    let before = rx(&module).clone();
    let effects = step(&mut module, &recovered(100, d(100), takeover_config()));
    assert_eq!(
        effects,
        [quarantine_alert()],
        "B-R68: the alert and nothing else"
    );
    let rx_ = rx(&module).clone();
    assert_eq!(
        rx_.quarantine(),
        Some(AppendReject::DivergentHistory { at: Seq(100) })
    );
    // The rows that move.
    assert_eq!(
        (rx_.lineage(), rx_.config()),
        (new_lineage, &takeover_config())
    );
    assert_eq!(
        (rx_.root(), rx_.last_partition_revision()),
        (new_root, REVISION)
    );
    // The rows that do not: no head, watermark or rung is deleted or truncated.
    assert_eq!(
        (rx_.applied_head(), rx_.accept_head(), rx_.staged()),
        (before.applied_head(), before.accept_head(), before.staged())
    );
    assert_eq!(
        (
            rx_.received_seq(),
            rx_.buffered_applied_seq(),
            rx_.durable_seq()
        ),
        (ReceivedSeq(120), AppliedSeq(120), DurableSeq(110))
    );
    assert_eq!(rx_.history(), before.history());
    assert_eq!(rx_.history().highest(), Some(Seq(120)));
    assert_eq!((rx_.partition(), rx_.node()), (P, B));
    // The new primary's first record is refused, and the refusal is all B sends it: no ACK.
    let next = taken_over(chain(101).pop().expect("101"));
    let refusal = refused_in(&mut module, C, NEW_CONFIG, next.encode().expect("encode"));
    assert_eq!(refusal, rejected(AppendReject::Quarantined));

    // The tracker clause, driven directly (M7B-41's fixture shape): C leads from (100, d100).
    let mut ladder = DigestLadder::new();
    ladder.insert(Seq::ZERO, Digest::ROOT);
    ladder.insert(Seq(100), d(100));
    let mut tracker = ProgressTracker::new(TrackerInit {
        config: takeover_config(),
        own: CopyId(2),
        lineage: new_lineage,
        history: ladder.clone(),
        local: ReplicaProgress {
            received: ReceivedSeq(100),
            buffered_applied: AppliedSeq(100),
            durable: DurableSeq(100),
        },
    })
    .expect("C's tracker");
    // Control: before the quarantine is known, the same ACK would be admitted and qualify.
    let mut unaware = tracker.clone();
    unaware.on_ack(&label(B), &ack_after_takeover(100), Tick(1));
    assert!(unaware.qualified_copies(Seq(100)).contains(&CopyId(1)));
    // C's cursor turns B's refusal into `CopyQuarantined`; routed back, it marks B diverged.
    let mut cursor = CatchupCursor::new(CopyId(1));
    assert_eq!(
        cursor.on_outcome(refusal, &ladder, Seq(100)),
        [EffectKind::Kernel(KernelEffect::CopyQuarantined {
            copy: CopyId(1)
        })]
    );
    tracker.on_divergence(CopyId(1), Tick(1));
    assert!(tracker.is_diverged(CopyId(1)));
    let frozen = tracker.clone();
    assert_eq!(
        tracker.on_ack(&label(B), &ack_after_takeover(100), Tick(2)),
        [ignored(KernelIgnoredReason::AckRejected(
            AckRejectReason::Diverged
        ))]
    );
    assert_eq!(tracker, frozen, "a dropped ACK changes nothing");
    assert!(!tracker.qualified_copies(Seq(100)).contains(&CopyId(1)));

    // --- NotRetained: B holds nothing at or above the cutoff; it anchors on its own head.
    let mut module = Replication::new();
    module.install_receiver(receiver_at(P, 60, 60));
    let effects = step(&mut module, &recovered(100, d(100), takeover_config()));
    asks_new_primary(&effects, 60);
    let rx_ = rx(&module);
    assert_eq!(
        (rx_.applied_head(), rx_.accept_head(), rx_.durable_seq()),
        (head(60), head(60), DurableSeq(60))
    );
    assert_eq!(
        (rx_.root(), rx_.lineage(), rx_.quarantine()),
        (new_root, new_lineage, None)
    );

    // --- NotRetained, nothing to anchor on (tester-kb-append F2 residual, pinned by the lead):
    // B seeded above the cutoff holds no rung at or below 100. It adopts nothing: the answer is
    // `InvalidArgument` and the receiver is exactly what it was. The design names no answer
    // here yet; if it names one, this clause changes with it.
    let mut module = Replication::new();
    module.install_receiver(receiver_at(P, 120, 120));
    let before = rx(&module).clone();
    assert_eq!(before.history().at_or_below(Seq(100)), None);
    let effects = step(&mut module, &recovered(100, d(100), takeover_config()));
    assert_eq!(
        effects,
        [ignored(KernelIgnoredReason::Error(
            ErrorKind::InvalidArgument
        ))]
    );
    assert_eq!(rx(&module), &before, "nothing adopted");
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
    // Whether each pin retires B (lead ruling B-R58a, F4): one with no primary, or for another
    // partition, changes nothing; one that names B no serving member on its node retires it
    // into the new generation, and changes nothing else.
    for (pin, retires) in pins.into_iter().zip([false, true, true, false]) {
        let mut module = module();
        let before = rx(&module).clone();
        let effects = step(&mut module, &recovered(10, d(10), pin));
        assert_eq!(
            effects,
            [ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::InvalidConfig
            ))]
        );
        let after = rx(&module);
        assert_eq!(after.retired(), retires);
        let generation = if retires { NEW_GEN } else { GEN };
        assert_eq!(after.lineage(), authority(generation, EPOCH));
        assert_kept(after, &before);
    }
    // B promoted: the tracker's handover, not the receiver's. The receiver still refuses it, and
    // `Recovered` builds B's primary beside it only from a barrier that names B (lead ruling
    // B-R54); this one names nobody, so nothing is built and the refusal says why.
    let mut module = module();
    let before = module.clone();
    let effects = step(
        &mut module,
        &recovered(10, d(10), pinned([Regular, Primary, Regular, Shadow])),
    );
    assert_eq!(
        effects,
        [
            ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::InvalidConfig
            )),
            ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::BarrierNotDurable
            )),
        ]
    );
    assert!(module.primary(B, P).is_none());
    assert!(rx(&module).retired());
    assert_kept(rx(&module), rx(&before));

    // Near-miss: a later pin that names B a secondary on its node clears the flag, and B stages
    // the new primary's record again.
    let effects = step(&mut module, &recovered(10, d(10), takeover_config()));
    asks_new_primary(&effects, 10);
    assert!(!rx(&module).retired());
    let (_, generation, seq) = staged_batch(&send(&mut module, label(C), &taken_over(golden())));
    assert_eq!((generation, seq), (NEW_GEN, Seq(11)));
}

/// Everything but the generation and the retired flag, which retiring writes, is as `before`.
fn assert_kept(after: &AppendReceiver, before: &AppendReceiver) {
    assert_eq!(
        (
            after.applied_head(),
            after.staged(),
            after.received_seq(),
            after.durable_seq(),
            after.quarantine(),
            after.root(),
            after.config(),
            after.history(),
            after.authority_seq(),
            after.lineage().owner_epoch,
            after.last_partition_revision(),
        ),
        (
            before.applied_head(),
            before.staged(),
            before.received_seq(),
            before.durable_seq(),
            before.quarantine(),
            before.root(),
            before.config(),
            before.history(),
            before.authority_seq(),
            before.lineage().owner_epoch,
            before.last_partition_revision(),
        )
    );
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
    let effects = sent_now(&mut behind(), C, &golden());
    assert_eq!(staged_batch(&effects), (BatchId(0), NEW_GEN, Seq(11)));

    // Its twin: same seq, same generation, chained on the same d10, not the root's record.
    let mut module = behind();
    let effects = sent_now(&mut module, C, &envelope(11, d(10), b"other"));
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
    // The fence is never skipped: the old primary cannot deliver history, on its old authority
    // or on the new one.
    let history = golden().encode().expect("encode");
    assert_eq!(
        refused_in(&mut behind(), A, NEW_CONFIG, history),
        rejected(AppendReject::StaleGeneration { current: NEW_GEN })
    );
    let effects = sent_now(&mut behind(), A, &golden());
    assert_eq!(
        reply_at(&effects[0], A, FRAME_ID, NEW_CONFIG),
        rejected(AppendReject::NotAMember)
    );
}

/// `env` sent from `from` on the authority `recovered` installs: `(NEW_GEN, NEW_EPOCH)` under
/// `NEW_CONFIG`, whatever lineage the record inside was sealed under (lead ruling B-R58a).
fn sent_now(module: &mut Replication, from: NodeId, env: &ReplicationEnvelope) -> Vec<EffectKind> {
    step(
        module,
        &framed_on(B, from, authority(NEW_GEN, NEW_EPOCH), NEW_CONFIG, env),
    )
}

/// The committed root's cutoff in M7B-123: `(100, d100)` of generation `GEN`.
const ROOT_CUT: u64 = 100;

/// B at `(50, d50)`, holding nothing above it, after the takeover committed the root at
/// `(100, d100)` of `GEN`, with C the new primary. Returns the module and the unsolicited
/// `NeedPrefix` B sent C.
fn behind_the_root() -> (Replication, AppendOutcome) {
    let mut module = Replication::new();
    module.install_receiver(receiver_at(P, 50, 50));
    let effects = step(
        &mut module,
        &recovered(ROOT_CUT, d(ROOT_CUT), takeover_config()),
    );
    asks_new_primary(&effects, 50);
    let asked = reply_at(&effects[0], C, UNSOLICITED, NEW_CONFIG);
    assert_eq!(rx(&module).history().digest_at(Seq(ROOT_CUT)), None);
    (module, asked)
}

/// Stage `env` from C on the new authority and commit it; return B's ACK to C.
fn applied_from_c(module: &mut Replication, env: &ReplicationEnvelope) -> AppendOutcome {
    let seq = env.header.seq;
    let effects = sent_now(module, C, env);
    let (batch, generation, staged) = staged_batch(&effects);
    assert_eq!((generation, staged), (NEW_GEN, seq), "B stages {seq:?}");
    let effects = step(module, &committed(batch.0, seq.0));
    assert_eq!(effects.len(), 1, "{effects:?}");
    reply_at(&effects[0], C, FRAME_ID, NEW_CONFIG)
}

/// M7B-123 (design §3.2 "Historical envelopes", §3.6 step 2; ADR-rdb-0005 §2; ADR-rdb-0009 §7;
/// K-B-37, B-R25). B is at 50 under `GEN`; `Recovered` commits the root at `(100, d100)` of `GEN`
/// and `lookup(100)` is `NotRetained` on B, so B takes the behind path and asks C for 51. C's
/// catch-up cursor, driven by B's real replies, sends 51..100 one at a time as the canonical
/// envelopes sealed under `GEN`, `EPOCH` and `CONFIG`, in frames on C's new authority. B accepts
/// every one: rows 4-6 would refuse `GEN` as `StaleGeneration`, so acceptance is the skip, and
/// the sender check still runs (A, not the pinned primary, is `NotAMember`). The ACK for 100
/// makes the cursor emit `CopyCaughtUp{B, 100, d100}`, once: the flush ACK that repeats 100 and
/// the ACK for 101 emit no second one. 101 under `NEW_GEN`, chained on `d100`, passes the normal
/// ladder. Twins: a record at 100 under `GEN` whose digest is not `d100` quarantines
/// `DIVERGENT_HISTORY` on the root-anchor clause (it chains on `d99`, so only the anchor can
/// catch it), and the cursor stops on `CopyQuarantined`; and 51 under `GEN - 1` is not historical,
/// so rows 4-6 run and answer `StaleGeneration`.
///
/// Not claimed here: the primary-side one-generation check (§3.6 step 1a), which is M7B-125's.
/// Nor is "unrestamped" at the sender: the record checked below is this test's own. The cursor
/// below is driven directly, as in
/// `a_probe_answered_by_the_same_record_loops_until_the_cap_asks_for_a_snapshot`.
#[retcd_test]
fn m7b_123_historical_envelopes_reach_copy_caught_up_and_root_anchor_quarantines() {
    let history = chain(ROOT_CUT + 1);
    let record = |seq: u64| history[usize::try_from(seq).expect("seq") - 1].clone();
    let mut ladder = DigestLadder::new();
    for env in &history[..usize::try_from(ROOT_CUT).expect("cut")] {
        ladder.insert(env.header.seq, env.record_digest);
    }
    let copy = CopyId(1);
    let head_at = Seq(ROOT_CUT);

    let (mut module, asked) = behind_the_root();
    // The sender check runs for a historical record: only the pinned primary may deliver one.
    let effects = sent_now(&mut module.clone(), A, &record(51));
    assert_eq!(
        reply_at(&effects[0], A, FRAME_ID, NEW_CONFIG),
        rejected(AppendReject::NotAMember)
    );

    let mut cursor = CatchupCursor::new(copy);
    let mut outcome = asked;
    let mut sent = Vec::new();
    let caught_up = loop {
        let effects = cursor.on_outcome(outcome, &ladder, head_at);
        match effects.as_slice() {
            [EffectKind::Kernel(KernelEffect::SendEnvelopes {
                copy: to,
                from,
                through,
            })] => {
                assert_eq!((*to, from), (copy, through));
                assert!(sent.len() < 100, "the walk must end: {sent:?}");
                sent.push(from.0);
                let env = record(from.0);
                assert_eq!(env.header.generation, GEN, "the fixture's record is GEN's");
                outcome = applied_from_c(&mut module, &env);
            }
            _ => break effects,
        }
    };
    assert_eq!(sent, (51..=ROOT_CUT).collect::<Vec<_>>());
    assert_eq!(
        caught_up,
        [EffectKind::Kernel(KernelEffect::CopyCaughtUp {
            copy,
            head: head_at,
            digest: d(ROOT_CUT),
        })]
    );
    assert_eq!(
        (rx(&module).applied_head(), rx(&module).quarantine()),
        (head(ROOT_CUT), None)
    );

    // Exactly once: the flush ACK repeating 100 does not report it again.
    let effects = step(&mut module, &flushed(&[(P, NEW_GEN, ROOT_CUT)]));
    let flush_ack = reply_at(&effects[0], C, UNSOLICITED, NEW_CONFIG);
    assert_eq!(
        cursor.on_outcome(flush_ack, &ladder, head_at),
        [ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::Recorded
        ))]
    );

    // 101 under the new generation passes the normal ladder, and its ACK reports nothing again.
    let next = taken_over(record(ROOT_CUT + 1));
    assert_eq!(next.prev_digest, d(ROOT_CUT));
    let ack_101 = applied_from_c(&mut module, &next);
    ladder.insert(next.header.seq, next.record_digest);
    assert_eq!(
        cursor.on_outcome(ack_101, &ladder, next.header.seq),
        [ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::Recorded
        ))]
    );
    assert_eq!(rx(&module).applied_head().seq, Seq(ROOT_CUT + 1));

    // Twin: the record at the root's cutoff with another digest. It chains on d99, so only the
    // root anchor can refuse it.
    let (mut module, _) = behind_the_root();
    for seq in 51..ROOT_CUT {
        applied_from_c(&mut module, &record(seq));
    }
    let wrong = envelope(ROOT_CUT, d(ROOT_CUT - 1), b"other");
    assert_ne!(wrong.record_digest, d(ROOT_CUT));
    let effects = sent_now(&mut module, C, &wrong);
    let proof = AppendReject::DivergentHistory { at: Seq(ROOT_CUT) };
    let answer = reply_at(&effects[0], C, FRAME_ID, NEW_CONFIG);
    assert_eq!(answer, rejected(proof));
    assert_eq!(effects[1..], [quarantine_alert()]);
    assert_eq!(
        (rx(&module).quarantine(), rx(&module).applied_head()),
        (Some(proof), head(ROOT_CUT - 1))
    );
    let mut cursor = CatchupCursor::new(copy);
    assert_eq!(
        cursor.on_outcome(answer, &ladder, head_at),
        [EffectKind::Kernel(KernelEffect::CopyQuarantined { copy })]
    );

    // Twin: two generations back is not historical, so rows 4-6 refuse it.
    let (mut module, _) = behind_the_root();
    let older = under(record(51), Generation(GEN.0 - 1), EPOCH, CONFIG);
    let before = rx(&module).clone();
    let effects = sent_now(&mut module, C, &older);
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(
        reply_at(&effects[0], C, FRAME_ID, NEW_CONFIG),
        rejected(AppendReject::StaleGeneration { current: NEW_GEN })
    );
    assert_eq!(rx(&module), &before, "a refused append changes nothing");
}

/// `recovered(cutoff, d(cutoff), takeover_config())`'s result, recovering into `generation`
/// from the one before it.
fn recovery_at(cutoff: u64, generation: Generation) -> RecoveryResult {
    let EventKind::Kernel(KernelEvent::Recovered(result)) =
        recovered(cutoff, d(cutoff), takeover_config()).kind
    else {
        unreachable!("recovered builds a Recovered");
    };
    let mut result = *result;
    let prior = Generation(generation.0 - 1);
    result.fenced_prior.prior_generation = prior;
    result.retained_status_map.predecessor_generation = prior;
    result.new_generation = generation;
    result.selected.root.generation = generation;
    result.committed.authority_view.lineage.generation = generation;
    result
}

/// C leading P and rebuilt by two of its own recoveries: at `(first, d(first))` into `NEW_GEN`,
/// then — after applying `first + 1..=100` itself — at `(100, d100)` into generation 5. So its
/// lineage's base is 100 and the one before began at `first` (lead ruling B-R71a).
fn primary_after_two_recoveries(first: u64) -> Primary {
    let mut history = DigestLadder::new();
    history.insert(Seq::ZERO, Digest::ROOT);
    for seq in 1..=first {
        history.insert(Seq(seq), d(seq));
    }
    let tracker = ProgressTracker::new(TrackerInit {
        config: takeover_config(),
        own: CopyId(2),
        lineage: authority(GEN, EPOCH),
        history,
        local: ReplicaProgress {
            received: ReceivedSeq(first),
            buffered_applied: AppliedSeq(first),
            durable: DurableSeq(first),
        },
    })
    .expect("C's tracker");
    assert_eq!(
        (tracker.base_seq(), tracker.prior_base()),
        (Seq::ZERO, None)
    );
    let mut primary = Primary::new(tracker);
    primary.on_recovered(&recovery_at(first, NEW_GEN), Tick(1));
    for seq in first + 1..=100 {
        let applied = KernelEvent::LocalApplied {
            seq: Seq(seq),
            bytes: 0,
            record_digest: d(seq),
        };
        primary.on_kernel(&applied, Tick(1));
    }
    primary.on_recovered(&recovery_at(100, Generation(5)), Tick(2));
    let tracker = primary.tracker();
    assert_eq!(
        (tracker.head(), tracker.base_seq(), tracker.prior_base()),
        (Seq(100), Seq(100), Some(Seq(first)))
    );
    primary
}

/// B's `NeedPrefix` from `(have, d(have))`, as C's primary receives it.
fn b_needs(primary: &mut Primary, have: u64) -> Vec<EffectKind> {
    let need = rejected(AppendReject::NeedPrefix {
        have: Seq(have),
        head_digest: d(have),
    });
    primary.on_reply(&label(B), encode_reply(&need).as_ref(), Tick(3))
}

fn sends_b(seq: u64) -> EffectKind {
    EffectKind::Kernel(KernelEffect::SendEnvelopes {
        copy: CopyId(1),
        from: Seq(seq),
        through: Seq(seq),
    })
}

/// B's ACK at `seq`, in C's generation 5, as C's primary receives it.
fn b_acks(primary: &mut Primary, seq: u64) -> Vec<EffectKind> {
    let ack = AppendAck {
        generation: Generation(5),
        ..ack_after_takeover(seq)
    };
    let accepted = AppendOutcome::Accepted(ack);
    primary.on_reply(&label(B), encode_reply(&accepted).as_ref(), Tick(3))
}

const fn snapshot_b() -> EffectKind {
    EffectKind::Kernel(KernelEffect::SnapshotCatchupRequired {
        copy: CopyId(1),
        barrier: Seq(100),
    })
}

/// M7B-125, lead rulings B-R71 and B-R71a: catch-up's one-generation limit (design §3.6 step
/// 1a, K-B-37). C's lineage is based at 100, so records 51..=100 are its predecessor's
/// history: B, at 50, gets them as they are. The record-level `generation` is not in the
/// effect, which names sequences only. M7B-123 shows B admits such a record; that the sender
/// does not re-stamp it (the sim's `send_envelopes` sends the stored bytes) no row pins.
///
/// - Twin (a), prior base 50: every record above it goes out, the first of them (51), the one at
///   the base (100), and nothing is refused.
/// - Twin (b), prior base 51: record 51 is older than the predecessor, which the receiver's
///   historical rule does not admit, so C asks for a snapshot and sends nothing. From 51 on,
///   record 52 goes out: the limit is exactly the prior base.
///
/// Twin (b) also takes an ACK: B at 40 would be sent 41, and gets the snapshot instead.
///
/// Credited for the continuous primary only: a node that stays primary across a generation
/// change. Not covered (B-R71a): every primary `Recovered` builds starts with no prior base and
/// keeps none through a same-generation `Recovered`, so step 1a never fires in a node's first
/// generation as primary, including the B-R58b genesis walk; nor for the recovery source, nor a
/// probe answer. A prior base taken across generations this node did not serve is not checked
/// and can be too low.
#[retcd_test]
fn m7b_125_catch_up_sends_historical_records_unrestamped_or_snapshot_if_older() {
    // Twin (a).
    let mut primary = primary_after_two_recoveries(50);
    assert_eq!(b_needs(&mut primary, 50), [sends_b(51)]);
    assert_eq!(b_needs(&mut primary, 99), [sends_b(100)]);

    // Twin (b).
    let mut primary = primary_after_two_recoveries(51);
    assert_eq!(b_needs(&mut primary, 50), [snapshot_b()]);
    assert_eq!(
        primary.cursor(CopyId(1)).and_then(CatchupCursor::unacked),
        None,
        "nothing sent"
    );
    assert!(!primary.awaits_ack());
    // The ACK path reads the same limit: the tracker admits B's ACK at 40, and the cursor would
    // send 41, which is older than the predecessor too.
    let progress = EffectKind::Kernel(KernelEffect::PeerProgress {
        peer: B,
        contiguous_seq: Seq(40),
    });
    assert_eq!(b_acks(&mut primary, 40), [progress, snapshot_b()]);
    assert_eq!(
        primary.cursor(CopyId(1)).and_then(CatchupCursor::unacked),
        None,
        "nothing sent on the ACK"
    );
    assert_eq!(b_needs(&mut primary, 51), [sends_b(52)]);

    // Twin (c), tester-kbr1 F3: a `Recovered` in the generation already served (a mode change,
    // or F1's rebuild) re-announces it. It keeps the prior base (B-R71a: only a rebuild into a
    // new generation sets one), so step 1a still fires. Kills mutant K7 "same generation drops
    // the prior base".
    let mut primary = primary_after_two_recoveries(51);
    primary.on_recovered(&recovery_at(100, Generation(5)), Tick(4));
    let tracker = primary.tracker();
    assert_eq!(
        (
            tracker.lineage().generation,
            tracker.base_seq(),
            tracker.prior_base()
        ),
        (Generation(5), Seq(100), Some(Seq(51))),
        "the same generation keeps the prior base"
    );
    assert_eq!(b_needs(&mut primary, 50), [snapshot_b()]);
    assert_eq!(b_needs(&mut primary, 51), [sends_b(52)]);
}

/// The bases step 1a reads follow the primary's own recoveries (lead ruling B-R71a). A primary
/// `Recovered` builds on C starts at the cutoff with no prior base; the next `Recovered`
/// rebuilds it, and its base becomes the prior one.
#[retcd_test]
fn a_built_primary_knows_its_base_and_a_rebuilt_one_its_prior_base() {
    let mut module = Replication::new();
    let first = requiring(recovered(40, d(40), takeover_config()), &[2], C);
    step(&mut module, &on(C, first));
    let tracker = module.primary(C, P).expect("built").tracker();
    assert_eq!((tracker.base_seq(), tracker.prior_base()), (Seq(40), None));
    let second = event(EventKind::Kernel(KernelEvent::Recovered(Box::new(
        recovery_at(40, Generation(5)),
    ))));
    step(&mut module, &on(C, requiring(second, &[2], C)));
    let tracker = module.primary(C, P).expect("rebuilt").tracker();
    assert_eq!(
        (
            tracker.lineage().generation,
            tracker.base_seq(),
            tracker.prior_base()
        ),
        (Generation(5), Seq(40), Some(Seq(40)))
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
    let older = recovery_body(
        &fence_in(NEW_GEN, 2, NEW_EPOCH, Revision(REVISION.0 - 1)),
        &env,
    );
    assert_eq!(
        refused_in(&mut recovered_at_ten(), C, NEW_CONFIG, older),
        rejected(AppendReject::StaleFence)
    );
    let current = recovery_body(&fence_in(NEW_GEN, 2, NEW_EPOCH, REVISION), &env);
    let effects = send_bytes(&mut recovered_at_ten(), label(C), current);
    assert_eq!(staged_batch(&effects), (BatchId(0), NEW_GEN, Seq(11)));
}

/// Our fixture's source is C (the plan's B′). After the takeover A is a regular member, so it
/// plays the plan's authenticated regular replayer.
#[retcd_test]
fn m7b_121_replayed_fence_credential_from_a_second_member_is_not_a_member() {
    let for_c = recovery_body(
        &fence_in(NEW_GEN, 2, NEW_EPOCH, REVISION),
        &taken_over(golden()),
    );
    assert_eq!(
        refused_in(&mut recovered_at_ten(), A, NEW_CONFIG, for_c.clone()),
        rejected(AppendReject::NotAMember)
    );
    // Twin: the same credential from the copy it names is admitted.
    let effects = send_bytes(&mut recovered_at_ten(), label(C), for_c);
    assert_eq!(staged_batch(&effects), (BatchId(0), NEW_GEN, Seq(11)));
    // Second twin, the membership half: the shadow D, named by its own credential.
    let for_d = recovery_body(
        &fence_in(NEW_GEN, 3, NEW_EPOCH, REVISION),
        &taken_over(golden()),
    );
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
// Each test below failed on one named mutant and passes on clean code. Evidence: the R1
// tester's mutant runs and handoff, working notes not in the repository. Not `m7b_` rows: the
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
/// malformed envelope, sent in the current primary's frame, whose record is also on the wrong
/// partition or generation, sealed above its sender's epoch or configuration, or carrying a
/// corrupt digest, is `Ignored(InvalidArgument)` with no reply and no quarantine. Kills the
/// `well_formed` check moved after row 6 (a reply leaks) and moved after row 7 (a corrupt
/// malformed envelope quarantines `CorruptHistory`).
///
/// The sender half is the frame fence's since lead ruling B-R58a, and the fence runs before
/// row 2: a malformed append from a copy that is not the primary is refused `NotAMember`.
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
        let golden_frame = framed_on(B, from, authority(GEN, EPOCH), CONFIG, &env);
        let effects = step(&mut module, &golden_frame);
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
        under(smuggling(), GEN, NEW_EPOCH, CONFIG),
        "row 5 epoch above the sender's",
    );
    malformed_from(
        A,
        under(smuggling(), GEN, EPOCH, NEW_CONFIG),
        "row 6 config above the sender's",
    );
    let effects = step(
        &mut module(),
        &framed_on(B, C, authority(GEN, EPOCH), CONFIG, &smuggling()),
    );
    assert_eq!(
        reply_at(&effects[0], C, FRAME_ID, CONFIG),
        rejected(AppendReject::NotAMember)
    );
}

// --- A1's view (design §2.2, lead ruling B-R53) --------------------------------------------

/// A1's view of `P` in `generation`, published at `seq`, pinning `epoch` and `config`.
fn view_of(
    seq: u64,
    generation: Generation,
    epoch: OwnerEpoch,
    config: ConfigVersion,
) -> AuthorityView {
    AuthorityView {
        lineage: Lineage {
            partition: P,
            generation,
            owner_epoch: epoch,
        },
        grant_id: GrantId(2),
        boot_id: BootId(1),
        authority_generation: AuthorityGeneration(1),
        config_version: config,
        authority_seq: seq,
        valid_through_tick: Tick(u64::MAX),
        past_horizon: DenyReason::NoGrant,
    }
}

/// `view` as the event R1 receives on B.
fn viewed(view: AuthorityView) -> Event {
    event(EventKind::Kernel(KernelEvent::Authority(
        AuthorityEvent::View(view),
    )))
}

/// A view in B's own generation.
fn view(seq: u64, epoch: OwnerEpoch, config: ConfigVersion) -> Event {
    viewed(view_of(seq, GEN, epoch, config))
}

fn replica(reason: ReplicaIgnoreReason) -> EffectKind {
    ignored(KernelIgnoredReason::Replica(reason))
}

/// Route `ev` to B and assert it is refused with `reason` and changes nothing.
fn view_refused(module: &mut Replication, ev: &Event, reason: ReplicaIgnoreReason, case: &str) {
    let before = rx(module).clone();
    assert_eq!(step(module, ev), vec![replica(reason)], "{case}");
    assert_eq!(
        rx(module),
        &before,
        "{case}: a refused view changes nothing"
    );
}

/// `(owner_epoch, config_version, authority_seq)` as B holds them.
fn pin_of(module: &Replication) -> (OwnerEpoch, ConfigVersion, u64) {
    let rx = rx(module);
    (
        rx.lineage().owner_epoch,
        rx.config().config_version,
        rx.authority_seq(),
    )
}

/// The golden append re-sealed at epoch `epoch` and configuration `config`.
fn golden_at(epoch: OwnerEpoch, config: ConfigVersion) -> ReplicationEnvelope {
    under(golden(), GEN, epoch, config)
}

/// Design §2.2: a secondary learns a newer owner epoch from A1's view, never from an append.
///
/// Before the view, an append at epoch 6 is `UnknownEpoch` and changes nothing, twice. The view
/// (seq 1, epoch 6, config 8) is `Recorded` and moves exactly the epoch, the configuration
/// version and the seq: the members, heads and watermarks stay. After it, the same append is
/// staged and its ACK carries epoch 6 under config 8, while an append at the old epoch is now
/// `StaleEpoch`. A record staged before the view is still ACKed when it commits after it, and a
/// quarantined copy installs the view and stays quarantined on the same proof.
#[retcd_test]
fn m7b_157_a_newer_view_installs_its_epoch_and_an_append_never_does() {
    let mut module = module();
    let newer = golden_at(NEW_EPOCH, NEW_CONFIG);
    for _ in 0..2 {
        assert_eq!(
            refused_in(&mut module, A, CONFIG, newer.encode().expect("encode")),
            rejected(AppendReject::UnknownEpoch { current: EPOCH })
        );
        assert_eq!(pin_of(&module), (EPOCH, CONFIG, 0));
    }

    let before = rx(&module).clone();
    assert_eq!(
        step(&mut module, &view(1, NEW_EPOCH, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(pin_of(&module), (NEW_EPOCH, NEW_CONFIG, 1));
    let after = rx(&module);
    assert_eq!(after.config().members, before.config().members);
    assert_eq!(
        (after.lineage().partition, after.lineage().generation),
        (P, GEN)
    );
    assert_eq!(
        (
            after.applied_head(),
            after.staged(),
            after.received_seq(),
            after.durable_seq(),
            after.quarantine(),
            after.root(),
            after.last_partition_revision(),
        ),
        (
            before.applied_head(),
            before.staged(),
            before.received_seq(),
            before.durable_seq(),
            before.quarantine(),
            before.root(),
            before.last_partition_revision(),
        )
    );

    assert_eq!(
        refused_in(
            &mut module,
            A,
            NEW_CONFIG,
            golden_at(EPOCH, NEW_CONFIG).encode().expect("encode")
        ),
        rejected(AppendReject::StaleEpoch { current: NEW_EPOCH })
    );
    let staged = send(&mut module, label(A), &newer);
    assert_eq!(staged_batch(&staged), (BatchId(0), GEN, Seq(11)));
    let acked = step(&mut module, &committed(0, 11));
    assert_eq!(acked.len(), 1, "{acked:?}");
    let AppendOutcome::Accepted(ack) = reply_at(&acked[0], A, FRAME_ID, NEW_CONFIG) else {
        panic!("expected an ACK, got {acked:?}");
    };
    assert_eq!(
        (
            ack.owner_epoch,
            ack.config_version,
            ack.progress.buffered_applied
        ),
        (NEW_EPOCH, NEW_CONFIG, AppliedSeq(11))
    );

    // A view keeps a staged record (tester gate C07): 11 staged before the view is still
    // ACKed when it commits after it.
    let mut staging = crate::module();
    let staged = send(&mut staging, label(A), &golden());
    assert_eq!(staged_batch(&staged), (BatchId(0), GEN, Seq(11)));
    assert_eq!(
        step(&mut staging, &view(1, NEW_EPOCH, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    let acked = step(&mut staging, &committed(0, 11));
    assert_eq!(acked.len(), 1, "{acked:?}");
    let AppendOutcome::Accepted(ack) = reply_at(&acked[0], A, FRAME_ID, NEW_CONFIG) else {
        panic!("expected an ACK, got {acked:?}");
    };
    assert_eq!(ack.progress.buffered_applied, AppliedSeq(11));

    // Quarantine withholds evidence, not what control says (tester probe q06): a quarantined
    // copy installs the view and stays quarantined on the same proof.
    let mut quarantined = crate::module();
    send(&mut quarantined, label(A), &envelope(11, d(9), b"v"));
    let proof = rx(&quarantined).quarantine();
    assert!(proof.is_some());
    assert_eq!(
        step(&mut quarantined, &view(1, NEW_EPOCH, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(pin_of(&quarantined), (NEW_EPOCH, NEW_CONFIG, 1));
    assert_eq!(rx(&quarantined).quarantine(), proof);
}

/// The ordering key is `authority_seq` (lead ruling B-R53). Once seq 5 is installed, a view at
/// seq 5 or 4 is `OutOfOrder` and changes nothing, even though both carry a higher epoch and
/// configuration, so an append at that epoch stays `UnknownEpoch`. The near-miss twin: seq 6 at
/// the **same** epoch is a configuration-only bump, and installs.
#[retcd_test]
fn m7b_158_a_view_that_is_not_newer_installs_nothing() {
    let mut module = module();
    assert_eq!(
        step(&mut module, &view(5, NEW_EPOCH, CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(pin_of(&module), (NEW_EPOCH, CONFIG, 5));

    let higher = OwnerEpoch(7);
    for (seq, case) in [(5, "equal seq"), (4, "lower seq")] {
        view_refused(
            &mut module,
            &view(seq, higher, NEW_CONFIG),
            ReplicaIgnoreReason::OutOfOrder,
            case,
        );
    }
    assert_eq!(
        refused_in(
            &mut module,
            A,
            CONFIG,
            golden_at(higher, CONFIG).encode().expect("encode")
        ),
        rejected(AppendReject::UnknownEpoch { current: NEW_EPOCH })
    );

    assert_eq!(
        step(&mut module, &view(6, NEW_EPOCH, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(pin_of(&module), (NEW_EPOCH, NEW_CONFIG, 6));
}

/// A newer seq that lowers the epoch or the configuration version contradicts itself, and none
/// of it is installed: not the half that went up, not the seq (lead ruling B-R53). A view of
/// another generation or partition is `NotRequired`: installing a generation is `Recovered`'s
/// job. Each leaves B exactly as it was, and a following view at seq 2 still installs, which
/// shows the refused seq 9 was not kept either.
#[retcd_test]
fn m7b_159_an_inconsistent_view_installs_no_part_of_itself() {
    let mut module = module();
    assert_eq!(
        step(&mut module, &view(1, NEW_EPOCH, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );

    let other_partition = AuthorityView {
        lineage: Lineage {
            partition: PartitionId(9),
            generation: GEN,
            owner_epoch: OwnerEpoch(7),
        },
        ..view_of(9, GEN, OwnerEpoch(7), ConfigVersion(9))
    };
    let cases = [
        (
            view(9, EPOCH, ConfigVersion(9)),
            ReplicaIgnoreReason::OutOfOrder,
            "lower epoch, higher config",
        ),
        (
            view(9, OwnerEpoch(7), CONFIG),
            ReplicaIgnoreReason::OutOfOrder,
            "higher epoch, lower config",
        ),
        (
            viewed(view_of(9, NEW_GEN, OwnerEpoch(7), ConfigVersion(9))),
            ReplicaIgnoreReason::NotRequired,
            "newer generation",
        ),
        (
            viewed(view_of(9, Generation(2), OwnerEpoch(7), ConfigVersion(9))),
            ReplicaIgnoreReason::NotRequired,
            "older generation",
        ),
        (
            viewed(other_partition),
            ReplicaIgnoreReason::NotRequired,
            "another partition",
        ),
    ];
    for (ev, reason, case) in &cases {
        view_refused(&mut module, ev, reason.clone(), case);
        assert_eq!(pin_of(&module), (NEW_EPOCH, NEW_CONFIG, 1), "{case}");
    }
    assert_eq!(
        step(&mut module, &view(2, OwnerEpoch(7), NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    assert_eq!(pin_of(&module), (OwnerEpoch(7), NEW_CONFIG, 2));
}

/// Pins the receiver-side risk logged in lead ruling B-R53: a view carries a configuration
/// **version**, not its members, and a receiver learns members only from `Recovered`.
///
/// Control pins config 8 without B, in the same generation. A's tracker holds both predicates
/// (7 with B, 8 without), and B's view says 8. B then:
/// 1. accepts A's append at config 8, because its member list is still 7's and names A primary;
/// 2. ACKs it under config 8;
/// 3. and A's tracker drops that ACK as `StaleConfig`, because the newest predicate naming B is
///    7 (K-B-49). The same progress under config 7 is admitted. That is a control on the
///    tracker's rule 4 alone, not an ACK B can send: without the view, B answers this
///    8-sealed record `NeedConfig{7}` and never ACKs it (tester probe q02).
///
/// So the view cannot make a removed copy count: no quorum is inflated. What it costs is the
/// removed copy's contribution to the retiring predicate until the barrier retires it.
#[retcd_test]
fn m7b_160_a_view_naming_a_config_without_this_copy_keeps_its_ack_out_of_every_count() {
    let record = golden_at(EPOCH, NEW_CONFIG);
    assert_eq!(
        refused_in(&mut module(), A, CONFIG, record.encode().expect("encode")),
        rejected(AppendReject::NeedConfig { current: CONFIG }),
        "without the view, B never ACKs the 8-sealed record"
    );
    let mut module = module();
    assert_eq!(
        step(&mut module, &view(1, EPOCH, NEW_CONFIG)),
        vec![replica(ReplicaIgnoreReason::Recorded)]
    );
    let staged = send(&mut module, label(A), &record);
    assert_eq!(staged_batch(&staged), (BatchId(0), GEN, Seq(11)));
    let acked = step(&mut module, &committed(0, 11));
    assert_eq!(acked.len(), 1, "{acked:?}");
    let AppendOutcome::Accepted(ack) = reply_at(&acked[0], A, FRAME_ID, NEW_CONFIG) else {
        panic!("expected an ACK, got {acked:?}");
    };
    assert_eq!(ack.config_version, NEW_CONFIG);

    let mut history = DigestLadder::new();
    for seq in 1..=10 {
        history.insert(Seq(seq), d(seq));
    }
    // A holds the record it sent: sealed under config 8, so not `d(11)`.
    history.insert(Seq(11), record.record_digest);
    let mut tracker = ProgressTracker::new(TrackerInit {
        config: config(),
        own: CopyId(0),
        lineage: Lineage {
            partition: P,
            generation: GEN,
            owner_epoch: EPOCH,
        },
        history,
        local: ReplicaProgress {
            received: ReceivedSeq(11),
            buffered_applied: AppliedSeq(11),
            durable: DurableSeq(11),
        },
    })
    .expect("A's tracker");
    let without_b = PartitionConfig::new(
        P,
        NEW_CONFIG,
        vec![
            member(0, A, ReplicaRole::Primary),
            member(2, C, ReplicaRole::RegularSecondary),
        ],
    );
    tracker.on_config_changed(&without_b, Tick(0));
    assert_eq!(
        tracker
            .predicates()
            .iter()
            .map(|predicate| predicate.config_version)
            .collect::<Vec<_>>(),
        vec![CONFIG, NEW_CONFIG]
    );

    let before = tracker.clone();
    assert_eq!(
        tracker.on_ack(&label(B), &ack, Tick(0)),
        vec![ignored(KernelIgnoredReason::AckRejected(
            AckRejectReason::StaleConfig
        ))]
    );
    assert_eq!(tracker, before, "a dropped ACK changes nothing");
    let under_seven = AppendAck {
        config_version: CONFIG,
        ..ack
    };
    assert_eq!(
        tracker.on_ack(&label(B), &under_seven, Tick(0)).first(),
        Some(&EffectKind::Kernel(KernelEffect::PeerProgress {
            peer: B,
            contiguous_seq: Seq(11),
        }))
    );
}

// --- Recovered builds R1's side (lead ruling B-R54) --------------------------------------------

/// `ev`, a `Recovered`, with a barrier over copies `copies`, each proved durable at the cutoff,
/// stepped on `node`.
fn requiring(mut ev: Event, copies: &[u8], node: NodeId) -> Event {
    let EventKind::Kernel(KernelEvent::Recovered(result)) = &mut ev.kind else {
        panic!("not a Recovered: {ev:?}");
    };
    let (cutoff, digest) = (result.selected.cutoff_seq, result.selected.cutoff_digest);
    let proofs: Vec<DurableProof> = copies
        .iter()
        .map(|&copy| DurableProof {
            copy: CopyId(copy),
            partition: P,
            seq: DurableSeq(cutoff.0),
            digest,
        })
        .collect();
    let required = copies.iter().map(|&copy| CopyId(copy)).collect();
    result.barrier = RecoveryBarrier::try_new(&proofs, &required, cutoff, digest)
        .expect("every required copy proved");
    ev.node = node;
    ev
}

/// M7B-165. Nothing is installed, and F1's result pins C primary, A and B regular, D a shadow,
/// with a barrier over B and C. On B, `Recovered` builds B's receiver: exactly what an
/// installed receiver seeded at the proved cutoff — `(10, d10)`, durable 10 — becomes through
/// the same rebuild. It asks C for everything after 10, holds the recovered view's epoch and
/// `authority_seq`, builds no primary, and stages C's next record under the new root.
///
/// Near-misses: A, a member the barrier does not name, is seeded at the root holding nothing
/// (lead ruling B-R54, item 3). It asks C from 0, and C's record 11 is answered `NeedPrefix`
/// from 0 — never accepted above the prefix A holds — and changes nothing. On C, which the pin
/// names primary, no receiver is built. D, the shadow, is built too: at the root outside the
/// barrier, at the cutoff inside it. And on A, which led the old pin, the old primary is kept
/// but retired (lead ruling B-R58a) and the receiver is built beside it, answering first
/// (tester probes r02, r03).
#[retcd_test]
fn m7b_165_recovered_builds_a_receiver_on_every_other_member_node() {
    let on = |node| requiring(recovered(10, d(10), takeover_config()), &[1, 2], node);
    let new_root = Lineage {
        partition: P,
        generation: NEW_GEN,
        owner_epoch: NEW_EPOCH,
    };
    let mut reference = Replication::new();
    reference.install_receiver(
        AppendReceiver::new(ReceiverInit {
            config: takeover_config(),
            own: CopyId(1),
            lineage: new_root,
            head: head(10),
            durable: DurableSeq(10),
        })
        .expect("the proved seed"),
    );
    let want = step(&mut reference, &on(B));

    let mut module = Replication::new();
    let effects = step(&mut module, &on(B));
    assert_eq!(effects, want);
    asks_new_primary(&effects, 10);
    assert_eq!(module, reference);
    assert_eq!(
        (
            rx(&module).applied_head(),
            rx(&module).durable_seq(),
            rx(&module).lineage(),
            rx(&module).authority_seq()
        ),
        (head(10), DurableSeq(10), new_root, 1)
    );
    assert_eq!(rx(&module).config(), &takeover_config());
    assert!(module.primary(B, P).is_none());
    let effects = send(&mut module, label(C), &taken_over(golden()));
    assert_eq!(staged_batch(&effects), (BatchId(0), NEW_GEN, Seq(11)));

    // A: a member the barrier does not name.
    let from_zero = rejected(AppendReject::NeedPrefix {
        have: Seq::ZERO,
        head_digest: Digest::ROOT,
    });
    let mut module = Replication::new();
    let effects = step(&mut module, &on(A));
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(reply_at(&effects[0], C, UNSOLICITED, NEW_CONFIG), from_zero);
    let built = module.receiver(A, P).expect("A's receiver").clone();
    assert_eq!(
        (
            built.applied_head(),
            built.durable_seq(),
            built.lineage(),
            built.authority_seq()
        ),
        (
            Head {
                seq: Seq::ZERO,
                digest: Digest::ROOT
            },
            DurableSeq(0),
            new_root,
            1
        )
    );
    let mut append = delivered(label(C), taken_over(golden()).encode().expect("encode"));
    append.node = A;
    let effects = step(&mut module, &append);
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(reply_at(&effects[0], C, FRAME_ID, NEW_CONFIG), from_zero);
    assert_eq!(module.receiver(A, P), Some(&built));

    // D, the shadow, is a member too (tester probe r02): at the root outside the barrier, at
    // the cutoff inside it.
    for (copies, seed) in [(&[1, 2][..], root_head()), (&[1, 2, 3][..], head(10))] {
        let mut module = Replication::new();
        let effects = step(
            &mut module,
            &requiring(recovered(10, d(10), takeover_config()), copies, D),
        );
        assert_eq!(effects.len(), 1, "{copies:?}: {effects:?}");
        assert_eq!(
            reply_at(&effects[0], C, UNSOLICITED, NEW_CONFIG),
            rejected(AppendReject::NeedPrefix {
                have: seed.seq,
                head_digest: seed.digest,
            }),
            "{copies:?}"
        );
        let shadow = module.receiver(D, P).expect("D's receiver");
        assert_eq!(
            (
                shadow.applied_head(),
                shadow.durable_seq(),
                shadow.lineage()
            ),
            (seed, DurableSeq(seed.seq.0), new_root),
            "{copies:?}"
        );
    }

    // A led the old pin (tester probe r03): its primary is kept, retired, and the receiver the
    // new pin gives it is built beside it and answers first.
    let mut history = DigestLadder::new();
    history.insert(Seq(10), d(10));
    let old_primary = ProgressTracker::new(TrackerInit {
        config: config(),
        own: CopyId(0),
        lineage: Lineage {
            partition: P,
            generation: GEN,
            owner_epoch: EPOCH,
        },
        history,
        local: ReplicaProgress {
            received: ReceivedSeq(10),
            buffered_applied: AppliedSeq(10),
            durable: DurableSeq(10),
        },
    })
    .expect("A's old primary");
    let mut module = Replication::new();
    module.install_primary(old_primary);
    let kept = module.primary(A, P).cloned();
    let effects = step(
        &mut module,
        &requiring(recovered(10, d(10), takeover_config()), &[0, 2], A),
    );
    assert_eq!(effects.len(), 2, "{effects:?}");
    assert_eq!(
        reply_at(&effects[0], C, UNSOLICITED, NEW_CONFIG),
        rejected(AppendReject::NeedPrefix {
            have: Seq(10),
            head_digest: d(10),
        })
    );
    assert_eq!(effects[1], replica(ReplicaIgnoreReason::InvalidConfig));
    // Kept, but retired (lead ruling B-R58a; M7B-172 pins what that fences): the flag is all
    // that changed.
    let retired = module.primary(A, P).expect("kept");
    assert!(retired.tracker().retired());
    assert_eq!(
        format!("{retired:?}").replace("retired: true", "retired: false"),
        format!("{:?}", kept.as_ref().expect("installed"))
    );
    let beside = module.receiver(A, P).expect("A's receiver");
    assert_eq!(
        (
            beside.applied_head(),
            beside.durable_seq(),
            beside.lineage()
        ),
        (head(10), DurableSeq(10), new_root)
    );

    // C: the pin's primary.
    let mut module = Replication::new();
    step(&mut module, &on(C));
    assert!(module.receiver(C, P).is_none());
    assert!(module.primary(C, P).is_some());

    // A pin for another partition, naming B, stepped for P: nothing is built anywhere.
    let elsewhere = PartitionConfig::new(
        PartitionId(9),
        NEW_CONFIG,
        vec![
            member(1, B, ReplicaRole::RegularSecondary),
            member(2, C, ReplicaRole::Primary),
        ],
    );
    let mut module = Replication::new();
    assert_eq!(
        step(
            &mut module,
            &requiring(recovered(10, d(10), elsewhere), &[1, 2], B)
        ),
        [ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::NotRequired
        ))]
    );
    assert_eq!(module, Replication::new());
}

// --- The frame fence (lead rulings B-R58, B-R58a) ------------------------------------------

/// A sender's authority in `P`.
fn authority(generation: Generation, epoch: OwnerEpoch) -> Lineage {
    Lineage {
        partition: P,
        generation,
        owner_epoch: epoch,
    }
}

/// `env` in a frame from `from` on `sender`'s authority under `config`, stepped on `node`.
fn framed_on(
    node: NodeId,
    from: NodeId,
    sender: Lineage,
    config: ConfigVersion,
    env: &ReplicationEnvelope,
) -> Event {
    let mut ev = framed(label(from), sender, config, env.encode().expect("encode"));
    ev.node = node;
    ev
}

/// Whether any effect sends an ACK.
fn acks(effects: &[EffectKind]) -> bool {
    effects.iter().any(|kind| {
        matches!(kind, EffectKind::Send(SendEffect::Unicast { frame, .. })
            if matches!(decode_reply(&frame.body), Ok(AppendOutcome::Accepted(_))))
    })
}

/// The lineage a reply frame carries: the receiver's own (lead ruling B-R58a, item 3).
fn reply_sender(kind: &EffectKind) -> Lineage {
    match kind {
        EffectKind::Send(SendEffect::Unicast { frame, .. }) => frame.sender,
        other => panic!("expected a reply, got {other:?}"),
    }
}

/// M7B-166 (F4, tester probe r04). F1's result pins B primary where B held a receiver under A.
/// The receiver is kept, as M7B-151 needs, and answers `InvalidConfig`; but once B's R1 has
/// adopted generation 4 it is fenced, not live. A's record 11, staged before the result, still
/// lands in storage and is never ACKed. A, fenced by that result and still on its generation-3
/// authority, sends record 12: `StaleGeneration{4}`, no `Store`, no ACK, nothing changed, and
/// the reply carries B's own lineage. A frame claiming generation 4 reaches nothing either, and
/// a flush of B's generation-4 writes sends A no ACK.
///
/// Near-miss, on A, which the result makes a secondary under C: its receiver is the right side.
/// From the same label, C's frame on generation 3 is `StaleGeneration{4}` and its frame on
/// generation 4 is staged.
#[retcd_test]
fn m7b_166_a_node_that_adopted_a_generation_never_acks_an_older_one() {
    use ReplicaRole::{Primary, RegularSecondary, Shadow};
    let b_leads = pinned([RegularSecondary, Primary, RegularSecondary, Shadow]);
    let mut module = module();
    let staged = send(&mut module, label(A), &golden());
    assert_eq!(staged_batch(&staged), (BatchId(0), GEN, Seq(11)));
    let answer = step(
        &mut module,
        &requiring(recovered(10, d(10), b_leads), &[1], B),
    );
    assert_eq!(
        answer.first(),
        Some(&replica(ReplicaIgnoreReason::InvalidConfig))
    );
    assert!(module.primary(B, P).is_some());
    assert!(rx(&module).retired());
    assert_eq!(rx(&module).lineage().generation, NEW_GEN);
    let stale = rejected(AppendReject::StaleGeneration { current: NEW_GEN });

    let withheld = step(&mut module, &committed(0, 11));
    assert_eq!(
        withheld,
        vec![ignored(KernelIgnoredReason::AppendRejected(
            AppendReject::StaleGeneration { current: NEW_GEN }
        ))]
    );

    let twelve = chain(12).pop().expect("seq 12");
    let before = rx(&module).clone();
    let zombie = send(&mut module, label(A), &twelve);
    assert_eq!(zombie.len(), 1, "{zombie:?}");
    assert_eq!(reply_at(&zombie[0], A, FRAME_ID, CONFIG), stale);
    assert_eq!(reply_sender(&zombie[0]), before.lineage());
    assert_eq!(rx(&module), &before);

    let claimed = step(
        &mut module,
        &framed_on(
            B,
            A,
            authority(NEW_GEN, NEW_EPOCH),
            NEW_CONFIG,
            &taken_over(twelve),
        ),
    );
    assert_eq!(
        reply_at(&claimed[0], A, FRAME_ID, CONFIG),
        rejected(AppendReject::NotAMember)
    );
    assert_eq!(rx(&module), &before);
    assert!(!acks(&step(&mut module, &flushed(&[(P, NEW_GEN, 11)]))));

    // Near-miss: A's receiver, built by the result that makes C primary.
    let mut on_a = Replication::new();
    step(
        &mut on_a,
        &requiring(recovered(10, d(10), takeover_config()), &[0, 2], A),
    );
    let refused = step(
        &mut on_a,
        &framed_on(A, C, authority(GEN, EPOCH), CONFIG, &golden()),
    );
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(reply_at(&refused[0], C, FRAME_ID, NEW_CONFIG), stale);
    let effects = step(
        &mut on_a,
        &framed_on(
            A,
            C,
            authority(NEW_GEN, NEW_EPOCH),
            NEW_CONFIG,
            &taken_over(golden()),
        ),
    );
    assert_eq!(staged_batch(&effects), (BatchId(0), NEW_GEN, Seq(11)));
}

/// M7B-167 (F1). The frame's sender and the record's seal are two lineages (lead ruling
/// B-R58a): the fence reads who is sending now, and a record may be older than its sender.
///
/// In one generation: B installs A1's view (epoch 6, config 8), and A, now on epoch 6 under
/// config 8, sends record 11 it sealed at epoch 5 under 7. It is staged. The same record from a
/// sender still on epoch 5 is `StaleEpoch{6}`, and from one still on config 7 is
/// `StaleConfig{8}`; neither changes anything.
///
/// Across generations: A, a member the barrier does not name, is seeded at the root. C, on its
/// generation-4 authority, sends records 1..=10 sealed under generation 3 at epoch 5, and A
/// stages, applies and ACKs each; then C's own record 11.
#[retcd_test]
fn m7b_167_a_current_sender_delivers_records_sealed_under_an_older_lineage() {
    let viewed = || {
        let mut module = module();
        assert_eq!(
            step(&mut module, &view(1, NEW_EPOCH, NEW_CONFIG)),
            vec![replica(ReplicaIgnoreReason::Recorded)]
        );
        module
    };
    let mut module = viewed();
    let effects = step(
        &mut module,
        &framed_on(B, A, authority(GEN, NEW_EPOCH), NEW_CONFIG, &golden()),
    );
    assert_eq!(staged_batch(&effects), (BatchId(0), GEN, Seq(11)));
    for (sender, config, want) in [
        (
            authority(GEN, EPOCH),
            NEW_CONFIG,
            AppendReject::StaleEpoch { current: NEW_EPOCH },
        ),
        (
            authority(GEN, NEW_EPOCH),
            CONFIG,
            AppendReject::StaleConfig {
                current: NEW_CONFIG,
            },
        ),
    ] {
        let mut module = viewed();
        let before = rx(&module).clone();
        let effects = step(&mut module, &framed_on(B, A, sender, config, &golden()));
        assert_eq!(effects.len(), 1, "{effects:?}");
        assert_eq!(
            reply_at(&effects[0], A, FRAME_ID, NEW_CONFIG),
            rejected(want)
        );
        assert_eq!(rx(&module), &before);
    }

    let mut on_a = Replication::new();
    step(
        &mut on_a,
        &requiring(recovered(10, d(10), takeover_config()), &[1, 2], A),
    );
    let current = authority(NEW_GEN, NEW_EPOCH);
    for (batch, env) in (0..).zip(chain(10)) {
        let seq = env.header.seq;
        let effects = step(&mut on_a, &framed_on(A, C, current, NEW_CONFIG, &env));
        assert_eq!(staged_batch(&effects), (BatchId(batch), NEW_GEN, seq));
        let mut done = committed(batch, seq.0);
        done.node = A;
        let acked = step(&mut on_a, &done);
        assert_eq!(acked.len(), 1, "{acked:?}");
        let AppendOutcome::Accepted(ack) = reply_at(&acked[0], C, FRAME_ID, NEW_CONFIG) else {
            panic!("expected an ACK at {seq:?}, got {acked:?}");
        };
        assert_eq!(ack.progress.buffered_applied, AppliedSeq(seq.0));
    }
    let effects = step(
        &mut on_a,
        &framed_on(A, C, current, NEW_CONFIG, &taken_over(golden())),
    );
    assert_eq!(staged_batch(&effects), (BatchId(10), NEW_GEN, Seq(11)));
}

/// M7B-168. The frame fence runs before any record check, and a frame whose sender is not the
/// current primary is refused on the sender alone, carrying a record B would otherwise take:
/// another partition, an older or newer generation, an older or newer epoch, an older or newer
/// configuration, or a label that is not the pinned primary. Each answers its row's reason
/// against B's current value and changes nothing. The control, the same record from A on the
/// current authority, is staged.
#[retcd_test]
fn m7b_168_a_frame_whose_sender_is_not_the_current_primary_is_refused() {
    use AppendReject as R;
    let current = authority(GEN, EPOCH);
    let cases = [
        (
            Lineage {
                partition: PartitionId(9),
                ..current
            },
            CONFIG,
            A,
            R::WrongPartition,
        ),
        (
            authority(Generation(2), EPOCH),
            CONFIG,
            A,
            R::StaleGeneration { current: GEN },
        ),
        (
            authority(NEW_GEN, EPOCH),
            CONFIG,
            A,
            R::NeedLineage { current: GEN },
        ),
        (
            authority(GEN, OwnerEpoch(4)),
            CONFIG,
            A,
            R::StaleEpoch { current: EPOCH },
        ),
        (
            authority(GEN, NEW_EPOCH),
            CONFIG,
            A,
            R::UnknownEpoch { current: EPOCH },
        ),
        (
            current,
            ConfigVersion(6),
            A,
            R::StaleConfig { current: CONFIG },
        ),
        (current, NEW_CONFIG, A, R::NeedConfig { current: CONFIG }),
        (current, CONFIG, C, R::NotAMember),
    ];
    for (sender, config, from, want) in cases {
        let mut module = module();
        let before = rx(&module).clone();
        let effects = step(&mut module, &framed_on(B, from, sender, config, &golden()));
        assert_eq!(effects.len(), 1, "{want:?}: {effects:?}");
        assert_eq!(
            reply_at(&effects[0], from, FRAME_ID, CONFIG),
            rejected(want),
            "{sender:?} under {config:?} from {from:?}"
        );
        assert_eq!(rx(&module), &before, "{want:?}");
    }
    let effects = step(&mut module(), &framed_on(B, A, current, CONFIG, &golden()));
    assert_eq!(staged_batch(&effects), (BatchId(0), GEN, Seq(11)));
}

/// M7B-169. After the fence, rows 5 and 6 read the record against its sender: a record may be
/// sealed at or below the sender's lineage, never above it. From A on the current authority, a
/// record sealed at epoch 6 is `UnknownEpoch{5}`, one sealed under config 8 is `NeedConfig{7}`,
/// and one sealed under generation 4 is row 4's `NeedLineage{3}`. None changes anything.
#[retcd_test]
fn m7b_169_a_record_sealed_above_its_senders_lineage_is_refused() {
    use AppendReject as R;
    for (env, want) in [
        (
            under(golden(), GEN, NEW_EPOCH, CONFIG),
            R::UnknownEpoch { current: EPOCH },
        ),
        (
            under(golden(), GEN, EPOCH, NEW_CONFIG),
            R::NeedConfig { current: CONFIG },
        ),
        (
            under(golden(), NEW_GEN, EPOCH, CONFIG),
            R::NeedLineage { current: GEN },
        ),
    ] {
        let mut module = module();
        let before = rx(&module).clone();
        let effects = step(
            &mut module,
            &framed_on(B, A, authority(GEN, EPOCH), CONFIG, &env),
        );
        assert_eq!(effects.len(), 1, "{want:?}: {effects:?}");
        assert_eq!(reply_at(&effects[0], A, FRAME_ID, CONFIG), rejected(want));
        assert_eq!(rx(&module), &before, "{want:?}");
    }
}

/// M7B-170 (lead ruling B-R59's constraint on B-R58a). A `RecoveryAppend` meets the credential,
/// not the pinned-primary check: its frame carries the credential's lineage, and C, a regular
/// copy the credential names, passes the frame fence and is staged. The same frame with a
/// credential for an older epoch is `StaleFence`, and one whose frame names an older generation
/// is `StaleGeneration`; neither changes anything.
#[retcd_test]
fn m7b_170_a_credentialed_recovery_append_passes_the_frame_fence_from_a_non_primary() {
    let credential = |epoch| fence(2, epoch, Revision(0));
    let from_c = |credential: FenceCredential, generation| {
        framed(
            label(C),
            Lineage {
                partition: credential.partition,
                generation,
                owner_epoch: credential.prior_owner_epoch,
            },
            CONFIG,
            recovery_body(&credential, &golden()),
        )
    };
    let effects = step(&mut module(), &from_c(credential(EPOCH), GEN));
    assert_eq!(staged_batch(&effects), (BatchId(0), GEN, Seq(11)));
    for (ev, want) in [
        (
            from_c(credential(OwnerEpoch(4)), GEN),
            AppendReject::StaleFence,
        ),
        (
            from_c(credential(EPOCH), Generation(2)),
            AppendReject::StaleGeneration { current: GEN },
        ),
    ] {
        let mut module = module();
        let before = rx(&module).clone();
        let effects = step(&mut module, &ev);
        assert_eq!(effects.len(), 1, "{want:?}: {effects:?}");
        assert_eq!(reply_at(&effects[0], C, FRAME_ID, CONFIG), rejected(want));
        assert_eq!(rx(&module), &before, "{want:?}");
    }
}

// --- The recovery source (lead rulings B-R59, B-R59a) --------------------------------------
//
// F1 asks B (copy 1), which holds 11..=15, to catch C (copy 2, node C) up through 15. Both
// receivers live in one module, as two nodes do in one dispatcher. The test plays the host:
// it serves each `SendRecoveryEnvelopes` by reading B's records, wrapping each in the
// credential and delivering it to C, and it carries C's replies back to B.

/// The credential F1 mints for this transfer: source B, in B's lineage. The revision is not 0,
/// so a rewrap that rebuilt the credential from defaults would not equal it.
fn b_credential() -> FenceCredential {
    fence(1, EPOCH, Revision(7))
}

/// C's receiver at `(at, d(at))`, durable `at`.
fn c_receiver_at(at: u64) -> AppendReceiver {
    AppendReceiver::new(ReceiverInit {
        config: config(),
        own: CopyId(2),
        lineage: authority(GEN, EPOCH),
        head: head(at),
        durable: DurableSeq(at),
    })
    .expect("C's receiver")
}

/// B at 15 (ladder 10..=15), and C's receiver at `c_at`.
fn b_and_c(c_at: u64) -> Replication {
    let mut module = applied_to(15);
    module.install_receiver(c_receiver_at(c_at));
    module
}

/// F1's routed `CatchUp`, stepped on B.
fn catch_up(from: u8, to: u8, through: u64, credential: FenceCredential) -> Event {
    event(EventKind::Kernel(KernelEvent::CatchUp {
        from: CopyId(from),
        to: CopyId(to),
        through: Seq(through),
        credential,
    }))
}

/// `b_and_c(c_at)` after F1 asked B to catch C up through 15.
fn started(c_at: u64) -> Replication {
    let mut module = b_and_c(c_at);
    let effects = step(&mut module, &catch_up(1, 2, 15, b_credential()));
    assert_eq!(effects, vec![recovery_send(15, 15), retransmit_arm(1, 100)]);
    module
}

fn recovery_send(from: u64, through: u64) -> EffectKind {
    EffectKind::Kernel(KernelEffect::SendRecoveryEnvelopes {
        copy: CopyId(2),
        from: Seq(from),
        through: Seq(through),
        credential: b_credential(),
    })
}

/// `ev` stepped on `node` instead.
fn on(node: NodeId, mut ev: Event) -> Event {
    ev.node = node;
    ev.boot = BootId(u64::from(node.0));
    ev
}

/// `outcome` in a reply frame from `from`, stepped on `node`.
fn reply_from_to(from: PeerLabel, node: NodeId, outcome: &AppendOutcome) -> Event {
    let body = encode_reply(outcome);
    on(node, framed(from, authority(GEN, EPOCH), CONFIG, body))
}

/// `outcome` in a reply frame from `from`, stepped on B.
fn reply_from(from: PeerLabel, outcome: &AppendOutcome) -> Event {
    reply_from_to(from, B, outcome)
}

/// C's ACK at `n`, durable 12, carrying `digest` at `n`.
fn c_ack_with(n: u64, digest: Digest) -> AppendAck {
    AppendAck {
        from: C,
        boot: BootId(3),
        digest_at_buffered: digest,
        ..ack(n, n, 12)
    }
}

fn c_ack(n: u64) -> AppendAck {
    c_ack_with(n, d(n))
}

/// Play the host for one `SendRecoveryEnvelopes`: deliver each record to C wrapped in the
/// effect's credential, commit what C stages, carry every reply C sends back to B, and return
/// what B answered.
fn serve(module: &mut Replication, send: &EffectKind) -> Vec<EffectKind> {
    let EffectKind::Kernel(KernelEffect::SendRecoveryEnvelopes {
        copy,
        from,
        through,
        credential,
    }) = send
    else {
        panic!("expected a recovery send, got {send:?}");
    };
    assert_eq!(*copy, CopyId(2));
    let records = chain(through.0);
    let mut at_b = Vec::new();
    for seq in from.0..=through.0 {
        let record = &records[usize::try_from(seq).expect("seq") - 1];
        let body = recovery_body(credential, record);
        let mut at_c = step(module, &on(C, delivered(label(B), body)));
        if let [EffectKind::Store(StoreEffect::Commit(batch))] = at_c.as_slice() {
            at_c = step(module, &on(C, committed(batch.id.0, seq)));
        }
        for kind in at_c {
            let EffectKind::Send(SendEffect::Unicast { to, frame }) = kind else {
                panic!("C answers only by reply, got {kind:?}");
            };
            assert_eq!(to, B, "C answers the source");
            let ev = event_at(
                B,
                EventKind::Transport(TransportEvent::Delivered {
                    from: label(C),
                    frame,
                }),
            );
            at_b.extend(step(module, &ev));
        }
    }
    at_b
}

/// Serve every recovery send B makes, starting from `first`, until B makes none; return every
/// effect B emitted along the way, `first` included.
fn serve_all(module: &mut Replication, first: EffectKind) -> Vec<EffectKind> {
    let mut all = vec![first.clone()];
    let mut pending = vec![first];
    while let Some(send) = pending.pop() {
        let answered = serve(module, &send);
        pending.extend(
            answered
                .iter()
                .filter(|kind| is_recovery_send(kind))
                .cloned(),
        );
        all.extend(answered);
    }
    all
}

fn is_recovery_send(kind: &EffectKind) -> bool {
    matches!(
        kind,
        EffectKind::Kernel(KernelEffect::SendRecoveryEnvelopes { .. })
    )
}

fn is_bare_send(kind: &EffectKind) -> bool {
    matches!(kind, EffectKind::Kernel(KernelEffect::SendEnvelopes { .. }))
}

fn caught_up_c() -> EffectKind {
    EffectKind::Kernel(KernelEffect::CopyCaughtUp {
        copy: CopyId(2),
        head: Seq(15),
        digest: d(15),
    })
}

/// M7B-175 (lead rulings B-R59, B-R59a). F1's `CatchUp{from B, to C, through 15}` on B starts a
/// source there, and B walks C from 12 to 15 exactly as a primary's cursor would, each record a
/// `RecoveryAppend` under F1's credential: the record at 15 first, C's `NeedPrefix{12}`, then
/// 13, 14 and 15 one at a time, and `CopyCaughtUp{C, 15, d15}` once, on the ACK that closes the
/// gap. The source is then gone; B's own receiver is untouched; C holds `(15, d15)`.
#[retcd_test]
fn m7b_175_a_recovery_source_walks_the_target_to_through_under_the_credential() {
    let mut module = b_and_c(12);
    let b_before = rx(&module).clone();
    let effects = step(&mut module, &catch_up(1, 2, 15, b_credential()));
    assert_eq!(effects, vec![recovery_send(15, 15), retransmit_arm(1, 100)]);
    assert_eq!(
        module
            .source(B, P, CopyId(2))
            .map(|source| source.through()),
        Some(Seq(15))
    );
    // The target lacks 13..=15, so it answers the first record with its head.
    assert_eq!(
        serve(&mut module, &recovery_send(15, 15)),
        vec![recovery_send(13, 13)]
    );
    assert_eq!(
        serve(&mut module, &recovery_send(13, 13)),
        vec![recovery_send(14, 14)]
    );
    assert_eq!(
        serve(&mut module, &recovery_send(14, 14)),
        vec![recovery_send(15, 15)]
    );
    assert_eq!(
        serve(&mut module, &recovery_send(15, 15)),
        vec![caught_up_c()]
    );
    assert!(
        module.source(B, P, CopyId(2)).is_none(),
        "dropped once caught up"
    );
    assert_eq!(
        rx(&module),
        &b_before,
        "sourcing reads B's ladder and writes nothing"
    );
    let c = module.receiver(C, P).expect("C");
    assert_eq!(c.applied_head(), head(15));
}

/// M7B-176 (lead ruling B-R59a, first direction). A primary never emits
/// `SendRecoveryEnvelopes`: A, leading at 15, walks C up from 12 over the same span M7B-175's
/// source does, and every send it makes is a bare `SendEnvelopes` for C.
#[retcd_test]
fn m7b_176_a_primary_never_emits_a_recovery_send() {
    let mut history = DigestLadder::new();
    for seq in 1..=15 {
        history.insert(Seq(seq), d(seq));
    }
    let mut module = Replication::new();
    module.install_primary(
        ProgressTracker::new(TrackerInit {
            config: config(),
            own: CopyId(0),
            lineage: authority(GEN, EPOCH),
            history,
            local: ReplicaProgress {
                received: ReceivedSeq(15),
                buffered_applied: AppliedSeq(15),
                durable: DurableSeq(15),
            },
        })
        .expect("A's tracker"),
    );
    let need = rejected(AppendReject::NeedPrefix {
        have: Seq(12),
        head_digest: d(12),
    });
    let mut effects = step(&mut module, &reply_from_to(label(C), A, &need));
    for n in 13..=15 {
        let accepted = AppendOutcome::Accepted(c_ack(n));
        effects.extend(step(&mut module, &reply_from_to(label(C), A, &accepted)));
    }
    assert!(!effects.iter().any(is_recovery_send), "{effects:?}");
    let sends: Vec<_> = effects.iter().filter(|kind| is_bare_send(kind)).collect();
    let want: Vec<_> = (13..=15)
        .map(|n| {
            EffectKind::Kernel(KernelEffect::SendEnvelopes {
                copy: CopyId(2),
                from: Seq(n),
                through: Seq(n),
            })
        })
        .collect();
    assert_eq!(sends, want.iter().collect::<Vec<_>>());
    assert!(effects.contains(&caught_up_c()), "{effects:?}");
}

/// M7B-177 (lead ruling B-R59a, second direction). A source never emits a bare
/// `SendEnvelopes`, and every recovery send carries F1's credential byte for byte: C from 10 is
/// walked 11..=15, and each send's credential equals, and encodes to the same bytes as, the one
/// the `CatchUp` carried.
#[retcd_test]
fn m7b_177_a_recovery_source_never_emits_a_bare_send_and_passes_the_credential_through() {
    let mut module = b_and_c(10);
    let first = step(&mut module, &catch_up(1, 2, 15, b_credential()));
    assert_eq!(first[1..], [retransmit_arm(1, 100)], "{first:?}");
    let effects = serve_all(&mut module, first[0].clone());
    assert!(!effects.iter().any(is_bare_send), "{effects:?}");
    let sent: Vec<_> = effects
        .iter()
        .filter_map(|kind| match kind {
            EffectKind::Kernel(KernelEffect::SendRecoveryEnvelopes {
                from, credential, ..
            }) => Some((from.0, *credential)),
            _ => None,
        })
        .collect();
    assert_eq!(
        sent.iter().map(|(from, _)| *from).collect::<Vec<_>>(),
        vec![15, 11, 12, 13, 14, 15]
    );
    let wire = |credential: &FenceCredential| encode_recovery_append(credential, &[]);
    for (from, credential) in &sent {
        assert_eq!(credential, &b_credential(), "send {from}");
        assert_eq!(wire(credential), wire(&b_credential()), "send {from}");
    }
    assert_eq!(effects.last(), Some(&caught_up_c()));
}

/// Step `ev` into `module`, assert it is answered with exactly `reason` and changes nothing.
fn catch_up_refused(module: &mut Replication, ev: &Event, reason: ReplicaIgnoreReason) {
    let before = module.clone();
    let case = format!("{reason:?}");
    assert_eq!(step(module, ev), vec![replica(reason)], "{case}");
    assert_eq!(
        module, &before,
        "{case}: a refused catch-up installs nothing"
    );
}

/// M7B-178 (lead ruling B-R59a). A `CatchUp` the node cannot source is refused and installs
/// nothing: `NotASource` when the receiver here is not `from` (C's copy named on B), and when
/// its head is below `through` (16 on a B at 15); `QuarantinedTerminal` when the receiver is
/// quarantined. Near-miss: the valid `CatchUp` starts a source.
#[retcd_test]
fn m7b_178_a_catch_up_this_copy_cannot_source_is_refused() {
    let mut module = b_and_c(12);
    catch_up_refused(
        &mut module,
        &catch_up(2, 1, 15, fence(2, EPOCH, Revision(7))),
        ReplicaIgnoreReason::NotASource,
    );
    catch_up_refused(
        &mut module,
        &catch_up(1, 2, 16, b_credential()),
        ReplicaIgnoreReason::NotASource,
    );
    let mut quarantined = b_and_c(12);
    let fork = envelope(16, d(14), b"v");
    send(&mut quarantined, label(A), &fork);
    assert!(rx(&quarantined).quarantine().is_some());
    catch_up_refused(
        &mut quarantined,
        &catch_up(1, 2, 15, b_credential()),
        ReplicaIgnoreReason::QuarantinedTerminal,
    );
    // Near-miss.
    assert_eq!(
        step(&mut module, &catch_up(1, 2, 15, b_credential())),
        vec![recovery_send(15, 15), retransmit_arm(1, 100)]
    );
    assert!(module.source(B, P, CopyId(2)).is_some());
}

/// Supporting M7B-178: the other ways a node cannot source, and the target and credential
/// checks. `NotASource`: no receiver on the node (D), a receiver `Recovered` retired, a
/// `through` below B's ladder (9), and `through` 0 on a copy at the root. `InvalidConfig`: a
/// target outside the configuration, the source's own copy, and a credential naming another
/// source or another partition.
#[retcd_test]
fn a_catch_up_with_no_source_here_or_an_invalid_target_or_credential_is_refused() {
    use ReplicaIgnoreReason::{InvalidConfig, NotASource};
    use ReplicaRole::{Primary, RegularSecondary, Shadow};
    let mut module = b_and_c(12);
    catch_up_refused(
        &mut module,
        &on(D, catch_up(1, 2, 15, b_credential())),
        NotASource,
    );
    catch_up_refused(&mut module, &catch_up(1, 2, 9, b_credential()), NotASource);
    let mut retired = b_and_c(12);
    step(
        &mut retired,
        &recovered(
            15,
            d(15),
            pinned([RegularSecondary, Primary, RegularSecondary, Shadow]),
        ),
    );
    assert!(rx(&retired).retired());
    catch_up_refused(
        &mut retired,
        &catch_up(1, 2, 15, fence_in(NEW_GEN, 1, NEW_EPOCH, REVISION)),
        NotASource,
    );
    let mut at_root = Replication::new();
    at_root.install_receiver(
        AppendReceiver::new(ReceiverInit {
            config: config(),
            own: CopyId(1),
            lineage: authority(GEN, EPOCH),
            head: root_head(),
            durable: DurableSeq(0),
        })
        .expect("B at the root"),
    );
    catch_up_refused(&mut at_root, &catch_up(1, 2, 0, b_credential()), NotASource);
    let other_partition = FenceCredential {
        partition: PartitionId(9),
        ..b_credential()
    };
    for (ev, case) in [
        (catch_up(1, 9, 15, b_credential()), "target not a member"),
        (catch_up(1, 1, 15, b_credential()), "target is the source"),
        (
            catch_up(1, 2, 15, fence(2, EPOCH, Revision(7))),
            "credential names C",
        ),
        (
            catch_up(1, 2, 15, other_partition),
            "credential for partition 9",
        ),
    ] {
        let before = module.clone();
        assert_eq!(
            step(&mut module, &ev),
            vec![replica(InvalidConfig)],
            "{case}"
        );
        assert_eq!(module, before, "{case}");
    }
}

/// M7B-179 (lead ruling B-R59a). `Recovered` drops the source: the cut it was sending towards
/// may be gone. C's later ACK, and its later `NeedPrefix`, find no source and no primary on B,
/// so they are declined and change nothing, as every reply to a node with neither is.
#[retcd_test]
fn m7b_179_recovered_drops_the_source_and_its_later_replies_reach_nothing() {
    let mut module = started(12);
    assert_eq!(
        serve(&mut module, &recovery_send(15, 15)),
        vec![recovery_send(13, 13)]
    );
    step(&mut module, &recovered(15, d(15), takeover_config()));
    assert!(module.source(B, P, CopyId(2)).is_none());
    declined(
        &mut module,
        &reply_from(label(C), &AppendOutcome::Accepted(c_ack(13))),
    );
    let need = rejected(AppendReject::NeedPrefix {
        have: Seq(12),
        head_digest: d(12),
    });
    declined(&mut module, &reply_from(label(C), &need));
}

/// Step a reply into a module running a source; assert it is refused with `reason` and changes
/// nothing.
fn reply_refused(module: &mut Replication, ev: &Event, reason: AckRejectReason) {
    let before = module.clone();
    let case = format!("{reason:?}");
    assert_eq!(
        step(module, ev),
        vec![ignored(KernelIgnoredReason::AckRejected(reason))],
        "{case}"
    );
    assert_eq!(module, &before, "{case}");
}

/// `started(12)` after C's `NeedPrefix{12}`: record 13 is in flight.
fn thirteen_in_flight() -> Replication {
    let mut module = started(12);
    let need = rejected(AppendReject::NeedPrefix {
        have: Seq(12),
        head_digest: d(12),
    });
    assert_eq!(
        step(&mut module, &reply_from(label(C), &need)),
        vec![recovery_send(13, 13)]
    );
    module
}

/// M7B-180 (lead ruling B-R59a). The source gates the ACKs it is given, as the tracker's ladder
/// does for a primary's cursor: an ACK labelled with a node other than `to` (D's, for 13) is
/// `NotAMember`, and C's ACK at 13 carrying another digest is `DigestMismatch`; neither moves
/// the cursor. Near-miss: C's honest ACK at 13 sends 14.
#[retcd_test]
fn m7b_180_the_source_refuses_an_ack_from_another_node_or_with_another_digest() {
    let mut module = thirteen_in_flight();
    let from_d = AppendAck {
        from: D,
        boot: BootId(4),
        ..c_ack(13)
    };
    reply_refused(
        &mut module,
        &reply_from(label(D), &AppendOutcome::Accepted(from_d)),
        AckRejectReason::NotAMember,
    );
    reply_refused(
        &mut module,
        &reply_from(label(C), &AppendOutcome::Accepted(c_ack_with(13, d(12)))),
        AckRejectReason::DigestMismatch,
    );
    assert_eq!(
        step(
            &mut module,
            &reply_from(label(C), &AppendOutcome::Accepted(c_ack(13)))
        ),
        vec![recovery_send(14, 14)]
    );
}

/// Supporting M7B-180: the gate's identity half and its absence half. An unauthenticated label,
/// and an ACK speaking for D under C's label, are `ForgedIdentity`; an ACK at 16, which B's
/// ladder does not hold, is `Unverifiable`. None moves the cursor.
#[retcd_test]
fn the_source_refuses_a_forged_or_unverifiable_ack() {
    let mut module = thirteen_in_flight();
    let unauthenticated = PeerLabel {
        authenticated: false,
        ..label(C)
    };
    reply_refused(
        &mut module,
        &reply_from(unauthenticated, &AppendOutcome::Accepted(c_ack(13))),
        AckRejectReason::ForgedIdentity,
    );
    let for_d = AppendAck {
        from: D,
        ..c_ack(13)
    };
    reply_refused(
        &mut module,
        &reply_from(label(C), &AppendOutcome::Accepted(for_d)),
        AckRejectReason::ForgedIdentity,
    );
    reply_refused(
        &mut module,
        &reply_from(label(C), &AppendOutcome::Accepted(c_ack(16))),
        AckRejectReason::Unverifiable,
    );
}

/// Supporting M7B-179: a source whose cursor stops is dropped as well. C refusing the credential
/// as stale (`StaleFence`) stops it with `RecoveryOnly` — F1 handles the fence — and C's later
/// ACK reaches nothing.
#[retcd_test]
fn a_source_whose_cursor_stops_is_dropped() {
    let mut module = thirteen_in_flight();
    assert_eq!(
        step(
            &mut module,
            &reply_from(label(C), &rejected(AppendReject::StaleFence))
        ),
        vec![replica(ReplicaIgnoreReason::RecoveryOnly)]
    );
    assert!(module.source(B, P, CopyId(2)).is_none());
    declined(
        &mut module,
        &reply_from(label(C), &AppendOutcome::Accepted(c_ack(13))),
    );
}

/// M7B-181 (lead ruling B-R59b). A source seeded at 100 on restart holds no rung below 100, so
/// when C at 80 answers the first record with `NeedPrefix{80}`, B cannot prove 81 follows C's
/// head. It asks for a snapshot, `SnapshotCatchupRequired{C, 100}`, and sends nothing else: it
/// never walks from a rung it does not hold. That is the restart liveness gap, made explicit;
/// seeding a copy from its own storage is the owed fix.
#[retcd_test]
fn m7b_181_a_restarted_source_without_the_targets_rung_asks_for_a_snapshot_and_never_walks() {
    let mut module = Replication::new();
    module.install_receiver(receiver_at(P, 100, 100));
    module.install_receiver(c_receiver_at(80));
    let b_before = rx(&module).clone();
    let c_before = module.receiver(C, P).expect("C").clone();
    assert_eq!(
        step(&mut module, &catch_up(1, 2, 100, b_credential())),
        vec![recovery_send(100, 100), retransmit_arm(1, 100)]
    );
    assert_eq!(
        serve(&mut module, &recovery_send(100, 100)),
        vec![EffectKind::Kernel(KernelEffect::SnapshotCatchupRequired {
            copy: CopyId(2),
            barrier: Seq(100),
        })]
    );
    assert_eq!(rx(&module), &b_before);
    assert_eq!(module.receiver(C, P), Some(&c_before), "C took nothing");
}

// --- The keepalive (ADR-rdb-0006 amendment 2026-09-26, lead ruling B-R60) -------------------
//
// A leads at 15 (ladder 1..=15) with B and C regular and D a shadow; B and C each hold 15. The
// test plays the host for A's bare sends: each record goes to its copy's node as an ordinary
// append from A, and every reply comes back to A. One tick is one millisecond.

/// A primary (copy 0), B and C regular (copies 1 and 2), D a shadow (copy 3).
fn with_shadow() -> PartitionConfig {
    use ReplicaRole::{Primary, RegularSecondary as Regular, Shadow};
    PartitionConfig::new(
        P,
        CONFIG,
        vec![
            member(0, A, Primary),
            member(1, B, Regular),
            member(2, C, Regular),
            member(3, D, Shadow),
        ],
    )
}

/// A leading at 15 under `with_shadow()`, its ladder 1..=15, and B and C each holding 15.
fn a_b_c() -> Replication {
    let mut history = DigestLadder::new();
    for seq in 1..=15 {
        history.insert(Seq(seq), d(seq));
    }
    let mut module = Replication::new();
    module.install_primary(
        ProgressTracker::new(TrackerInit {
            config: with_shadow(),
            own: CopyId(0),
            lineage: authority(GEN, EPOCH),
            history,
            local: ReplicaProgress {
                received: ReceivedSeq(15),
                buffered_applied: AppliedSeq(15),
                durable: DurableSeq(15),
            },
        })
        .expect("A's tracker"),
    );
    module.install_receiver(receiver_at(P, 15, 15));
    module.install_receiver(c_receiver_at(15));
    module
}

/// L1's `SetAdmission` as routed to R1 on `node`.
fn admission_on(node: NodeId, allow: bool) -> Event {
    let state = AdmissionState {
        allow,
        reason: (!allow).then_some(ErrorKind::ProtectionPaused),
        oldest_unsafe_age: 0,
        oldest_unsafe_seq: Seq(15),
        replication_lag: ReplicationLag::millis(0),
        stalest_copy: None,
        lost_copies: vec![],
        paused_prefix: Seq(15),
        resume_barrier: Seq(15),
        required_config_versions: vec![CONFIG],
        outstanding_unsafe_bytes: 0,
    };
    event_at(node, EventKind::Kernel(KernelEvent::SetAdmission(state)))
}

fn admission(allow: bool) -> Event {
    admission_on(A, allow)
}

/// `P`'s keepalive timer firing on `node` at `version`, scheduled for `at`.
fn fired_on(node: NodeId, version: u64, at: u64) -> Event {
    event_at(
        node,
        EventKind::Timer(TimerFired {
            id: keepalive_timer(P),
            version: TimerVersion(version),
            scheduled_at: Tick(at),
        }),
    )
}

fn fired(version: u64, at: u64) -> Event {
    fired_on(A, version, at)
}

/// `ev` stepped at tick `now`, checking each effect is R1's for `P`.
fn step_at(module: &mut Replication, now: u64, ev: &Event) -> Vec<EffectKind> {
    let ctx = StepCtx {
        now: Tick(now),
        ..ctx()
    };
    let effects = module.step(&ctx, ev).expect("R1 answers");
    effects
        .into_iter()
        .map(|effect| {
            assert_eq!(
                (effect.from, effect.partition),
                (ModuleName::Replication, P)
            );
            effect.kind
        })
        .collect()
}

fn arm(version: u64, at: u64) -> EffectKind {
    EffectKind::Timer(TimerEffect::Arm {
        id: keepalive_timer(P),
        version: TimerVersion(version),
        at: Tick(at),
    })
}

fn cancel(version: u64) -> EffectKind {
    EffectKind::Timer(TimerEffect::Cancel {
        id: keepalive_timer(P),
        version: TimerVersion(version),
    })
}

/// A's head, 15, to `copy`.
fn head_to(copy: u8) -> EffectKind {
    EffectKind::Kernel(KernelEffect::SendEnvelopes {
        copy: CopyId(copy),
        from: Seq(15),
        through: Seq(15),
    })
}

/// One keepalive round started at `now`: the head to B and C, never to the shadow D, and the
/// next arm `KEEPALIVE_MS` later.
fn round(version: u64, now: u64) -> Vec<EffectKind> {
    vec![head_to(1), head_to(2), arm(version, now + KEEPALIVE_MS)]
}

fn replica_answer(reason: ReplicaIgnoreReason) -> Vec<EffectKind> {
    vec![ignored(KernelIgnoredReason::Replica(reason))]
}

/// Play the host for A's bare sends: each record goes to its copy's node as an append from A,
/// what that copy stages is committed, and each reply comes back to A. A node in `silent` drops
/// what it is sent. Returns what A answered.
fn host_a(module: &mut Replication, sends: &[EffectKind], silent: &[NodeId]) -> Vec<EffectKind> {
    let records = chain(15);
    let mut at_a = Vec::new();
    for send in sends {
        let EffectKind::Kernel(KernelEffect::SendEnvelopes {
            copy,
            from,
            through,
        }) = send
        else {
            continue;
        };
        let node = match copy.0 {
            1 => B,
            2 => C,
            other => panic!("A sends to no copy {other}"),
        };
        if silent.contains(&node) {
            continue;
        }
        for seq in from.0..=through.0 {
            let record = &records[usize::try_from(seq).expect("seq") - 1];
            let body = record.encode().expect("encode");
            let mut at_copy = step(module, &on(node, delivered(label(A), body)));
            if let [EffectKind::Store(StoreEffect::Commit(batch))] = at_copy.as_slice() {
                at_copy = step(module, &on(node, committed(batch.id.0, seq)));
            }
            for kind in at_copy {
                let EffectKind::Send(SendEffect::Unicast { to, frame }) = kind else {
                    panic!("a copy answers only by reply, got {kind:?}");
                };
                assert_eq!(to, A, "the copy answers the primary");
                let ev = event_at(
                    A,
                    EventKind::Transport(TransportEvent::Delivered {
                        from: label(node),
                        frame,
                    }),
                );
                at_a.extend(step(module, &ev));
            }
        }
    }
    at_a
}

/// Every `PeerProgress` in `effects`, in order.
fn progressed(effects: &[EffectKind]) -> Vec<(NodeId, Seq)> {
    effects
        .iter()
        .filter_map(|kind| match kind {
            EffectKind::Kernel(KernelEffect::PeerProgress {
                peer,
                contiguous_seq,
            }) => Some((*peer, *contiguous_seq)),
            _ => None,
        })
        .collect()
}

fn is_keepalive_output(kind: &EffectKind) -> bool {
    matches!(kind, EffectKind::Timer(_)) || is_bare_send(kind)
}

fn keepalive_of(module: &Replication) -> Option<TimerVersion> {
    module.primary(A, P).expect("A's primary").keepalive()
}

/// M7B-182 (lead ruling B-R60; ADR-rdb-0006 amendment 2026-09-26). A primary whose admission L1
/// rejects draws `PeerProgress` from every live regular secondary every `KEEPALIVE_MS`, with no
/// writes at all. The rejecting `SetAdmission` at 0 sends A's head to B and C at once and arms the
/// timer for 100; each fire does the same and re-arms 100 later. Each copy answers `AlreadyHave`
/// and its ACK, and each ACK emits `PeerProgress{peer, 15}`. The shadow D is never sent one.
#[retcd_test]
fn m7b_182_a_paused_primary_draws_peer_progress_from_every_live_peer_every_100_ms() {
    assert_eq!(KEEPALIVE_MS, 100);
    let mut module = a_b_c();
    let mut sends = step_at(&mut module, 0, &admission(false));
    assert_eq!(sends, round(1, 0));
    for (version, now) in [(1, 100), (2, 200), (3, 300)] {
        let answered = host_a(&mut module, &sends, &[]);
        assert_eq!(
            progressed(&answered),
            vec![(B, Seq(15)), (C, Seq(15))],
            "the round before {now}: {answered:?}"
        );
        sends = step_at(&mut module, now, &fired(version, now));
        assert_eq!(sends, round(version + 1, now), "the fire at {now}");
    }
    let answered = host_a(&mut module, &sends, &[]);
    assert_eq!(progressed(&answered), vec![(B, Seq(15)), (C, Seq(15))]);
}

/// M7B-183 (lead ruling B-R60). The keepalive stops on the allowing `SetAdmission`: that edge
/// answers exactly the cancel of the armed version, and the fire already in flight is
/// `StaleTimer`, sends nothing and changes nothing. A second allow has nothing to stop. A new
/// pause starts a new keepalive under a new version, a repeated reject does not re-arm it, and
/// the old version stays stale.
#[retcd_test]
fn m7b_183_the_keepalive_stops_on_allow_and_a_fire_in_flight_sends_nothing() {
    let mut module = a_b_c();
    assert_eq!(step_at(&mut module, 0, &admission(false)), round(1, 0));
    assert_eq!(step_at(&mut module, 50, &admission(true)), vec![cancel(1)]);
    assert_eq!(keepalive_of(&module), None);
    let before = module.clone();
    assert_eq!(
        step_at(&mut module, 100, &fired(1, 100)),
        replica_answer(ReplicaIgnoreReason::StaleTimer)
    );
    assert_eq!(module, before, "a stale fire changes nothing");
    assert_eq!(
        step_at(&mut module, 120, &admission(true)),
        replica_answer(ReplicaIgnoreReason::NotRequired)
    );
    assert_eq!(
        module, before,
        "an allow with nothing armed changes nothing"
    );

    assert_eq!(step_at(&mut module, 150, &admission(false)), round(2, 150));
    assert_eq!(
        step_at(&mut module, 160, &admission(false)),
        replica_answer(ReplicaIgnoreReason::NotRequired),
        "a repeated reject keeps the cadence"
    );
    assert_eq!(
        step_at(&mut module, 200, &fired(1, 100)),
        replica_answer(ReplicaIgnoreReason::StaleTimer)
    );
    assert_eq!(step_at(&mut module, 250, &fired(2, 250)), round(3, 250));
}

/// M7B-184 (lead ruling B-R60; ADR-rdb-0006 "absence is infinite lag"). A silent peer draws no
/// `PeerProgress`: with C dropping everything, every round still sends C the head, only B's ACK
/// comes back, and A never reports progress for C or writes any for it.
#[retcd_test]
fn m7b_184_a_silent_peer_draws_no_peer_progress() {
    let mut module = a_b_c();
    let mut sends = step_at(&mut module, 0, &admission(false));
    for (version, now) in [(1, 100), (2, 200), (3, 300)] {
        let answered = host_a(&mut module, &sends, &[C]);
        assert_eq!(progressed(&answered), vec![(B, Seq(15))], "{answered:?}");
        sends = step_at(&mut module, now, &fired(version, now));
        assert_eq!(
            sends,
            round(version + 1, now),
            "a silent copy is still sent"
        );
    }
    let tracker = module.primary(A, P).expect("A").tracker();
    assert_eq!(
        tracker.peer(CopyId(2)).map(|peer| peer.progress),
        Some(ReplicaProgress::EMPTY),
        "C proved nothing"
    );
}

/// M7B-185 (lead ruling B-R60). A healthy or idle partition sends nothing. A primary L1 has not
/// paused takes an ACK and an allowing `SetAdmission` and arms no timer and sends no head; a
/// keepalive fire with nothing armed is `StaleTimer` and changes nothing. A node with no primary
/// answers both, since R1 is a named consumer, and writes nothing.
#[retcd_test]
fn m7b_185_a_healthy_or_idle_partition_sends_nothing() {
    let mut module = a_b_c();
    let mut effects = step_at(
        &mut module,
        0,
        &reply_from_to(label(B), A, &AppendOutcome::Accepted(ack(15, 15, 15))),
    );
    assert_eq!(progressed(&effects), vec![(B, Seq(15))]);
    let before = module.clone();
    let allowed = step_at(&mut module, 10, &admission(true));
    assert_eq!(allowed, replica_answer(ReplicaIgnoreReason::NotRequired));
    assert_eq!(module, before);
    let stale = step_at(&mut module, 100, &fired(1, 100));
    assert_eq!(stale, replica_answer(ReplicaIgnoreReason::StaleTimer));
    assert_eq!(module, before);
    assert_eq!(keepalive_of(&module), None);
    effects.extend(allowed);
    effects.extend(stale);
    assert!(
        !effects.iter().any(is_keepalive_output),
        "nothing armed and nothing sent: {effects:?}"
    );

    let mut elsewhere = self::module();
    let before = elsewhere.clone();
    for allow in [false, true] {
        assert_eq!(
            step_at(&mut elsewhere, 0, &admission_on(B, allow)),
            replica_answer(ReplicaIgnoreReason::NotRequired)
        );
    }
    assert_eq!(
        step_at(&mut elsewhere, 100, &fired_on(B, 1, 100)),
        replica_answer(ReplicaIgnoreReason::StaleTimer)
    );
    assert_eq!(elsewhere, before);
}

/// A keepalive round skips a copy whose cursor is running, whose head it would disturb, and a
/// diverged copy, whose ACKs rule 1d drops.
#[retcd_test]
fn a_keepalive_skips_a_copy_being_caught_up_and_a_diverged_copy() {
    let mut module = a_b_c();
    // B asks for the prefix it already holds: its cursor matches at the head, sends nothing and
    // stays installed, idle. An idle cursor draws no ACK, so the keepalive still sends to B.
    let at_head = rejected(AppendReject::NeedPrefix {
        have: Seq(15),
        head_digest: d(15),
    });
    let idle = step(&mut module, &reply_from_to(label(B), A, &at_head));
    assert!(!idle.iter().any(is_bare_send), "{idle:?}");
    let cursor = module.primary(A, P).expect("A's primary").cursor(CopyId(1));
    assert_eq!(cursor.map(CatchupCursor::outstanding), Some(None));
    let need = rejected(AppendReject::NeedPrefix {
        have: Seq(12),
        head_digest: d(12),
    });
    let walk = step(&mut module, &reply_from_to(label(C), A, &need));
    assert_eq!(
        walk,
        vec![
            EffectKind::Kernel(KernelEffect::SendEnvelopes {
                copy: CopyId(2),
                from: Seq(13),
                through: Seq(13),
            }),
            retransmit_arm(1, 100),
        ]
    );
    assert_eq!(
        step_at(&mut module, 0, &admission(false)),
        vec![head_to(1), arm(1, 100)]
    );
    let diverged = event_at(
        A,
        EventKind::Kernel(KernelEvent::DivergenceDetected { copy: CopyId(1) }),
    );
    step(&mut module, &diverged);
    assert_eq!(step_at(&mut module, 100, &fired(1, 100)), vec![arm(2, 200)]);
}

/// A primary a `Recovered` retires cancels its keepalive, and then answers neither the next
/// `SetAdmission` nor the fire in flight with anything but a no-op.
#[retcd_test]
fn a_retired_primary_cancels_its_keepalive() {
    let mut module = a_b_c();
    assert_eq!(step_at(&mut module, 0, &admission(false)), round(1, 0));
    let retiring = on(A, recovered(15, d(15), takeover_config()));
    let effects = step(&mut module, &retiring);
    assert!(effects.contains(&cancel(1)), "{effects:?}");
    assert_eq!(keepalive_of(&module), None);
    assert_eq!(
        step_at(&mut module, 100, &fired(1, 100)),
        replica_answer(ReplicaIgnoreReason::StaleTimer)
    );
    assert_eq!(
        step_at(&mut module, 110, &admission(false)),
        replica_answer(ReplicaIgnoreReason::NotRequired)
    );
}

/// The keepalive timer is one per partition: a fire of another partition's id is not R1's here.
/// A primary with no record yet has no head to send, and still arms the next round.
#[retcd_test]
fn the_keepalive_timer_is_per_partition_and_a_primary_with_no_record_sends_no_head() {
    assert_ne!(keepalive_timer(P), keepalive_timer(PartitionId(5)));
    let mut module = a_b_c();
    let other = event_at(
        A,
        EventKind::Timer(TimerFired {
            id: keepalive_timer(PartitionId(5)),
            version: TimerVersion(1),
            scheduled_at: Tick(100),
        }),
    );
    declined(&mut module, &other);

    let mut history = DigestLadder::new();
    history.insert(Seq::ZERO, Digest::ROOT);
    let mut empty = Replication::new();
    empty.install_primary(
        ProgressTracker::new(TrackerInit {
            config: with_shadow(),
            own: CopyId(0),
            lineage: authority(GEN, EPOCH),
            history,
            local: ReplicaProgress::EMPTY,
        })
        .expect("A at the root"),
    );
    assert_eq!(step_at(&mut empty, 0, &admission(false)), vec![arm(1, 100)]);
}

// --- Lead rulings B-R67, B-R67a: re-sends, at the receiver and at a source ------------------

/// `P`'s retransmit timer firing on `node` at `version`, scheduled for `at`.
fn retransmit_fired_on(node: NodeId, version: u64, at: u64) -> Event {
    event_at(
        node,
        EventKind::Timer(TimerFired {
            id: retransmit_timer(P),
            version: TimerVersion(version),
            scheduled_at: Tick(at),
        }),
    )
}

fn retransmit_arm(version: u64, at: u64) -> EffectKind {
    EffectKind::Timer(TimerEffect::Arm {
        id: retransmit_timer(P),
        version: TimerVersion(version),
        at: Tick(at),
    })
}

/// M7B-189 (lead ruling B-R67: a re-send is idempotent at the receiver). A re-send is the same
/// frame again, so the receiver sees a record it already holds. After B took record 11 from A
/// and committed it, the same bytes again draw exactly `AlreadyHave` and B's current ACK, stage
/// nothing, and leave B's receiver unchanged. The same holds for a `RecoveryAppend`: C takes
/// record 13 from B's source under F1's credential, and the same frame again draws
/// `AlreadyHave` and C's ACK at 13, back to B, and changes nothing on C.
#[retcd_test]
fn m7b_189_a_re_sent_record_is_answered_already_have_and_changes_nothing() {
    let mut module = module();
    let body = chain(11)[10].encode().expect("encode");
    let staged = send_bytes(&mut module, label(A), body.clone());
    let (batch, _, _) = staged_batch(&staged);
    let acked = step(&mut module, &committed(batch.0, 11));
    assert_eq!(
        acked.iter().map(reply_of).collect::<Vec<_>>(),
        vec![AppendOutcome::Accepted(ack(11, 11, 10))]
    );
    let before = rx(&module).clone();
    let again = send_bytes(&mut module, label(A), body);
    assert_eq!(
        again.iter().map(reply_of).collect::<Vec<_>>(),
        vec![
            AppendOutcome::AlreadyHave,
            AppendOutcome::Accepted(ack(11, 11, 10))
        ]
    );
    assert_eq!(rx(&module), &before);

    let mut module = b_and_c(12);
    let body = recovery_body(&b_credential(), &chain(13)[12]);
    let staged = step(&mut module, &on(C, delivered(label(B), body.clone())));
    let (batch, _, _) = staged_batch(&staged);
    let acked = step(&mut module, &on(C, committed(batch.0, 13)));
    let to_b = |kinds: &[EffectKind]| -> Vec<AppendOutcome> {
        kinds
            .iter()
            .map(|kind| reply_at(kind, B, FRAME_ID, CONFIG))
            .collect()
    };
    assert_eq!(to_b(&acked), vec![AppendOutcome::Accepted(c_ack(13))]);
    let before = module.receiver(C, P).expect("C").clone();
    let again = step(&mut module, &on(C, delivered(label(B), body)));
    assert_eq!(
        to_b(&again),
        vec![
            AppendOutcome::AlreadyHave,
            AppendOutcome::Accepted(c_ack(13))
        ]
    );
    assert_eq!(module.receiver(C, P).expect("C"), &before);
}

/// Deliver record `seq` to C under B's credential and commit it, and lose every reply C sends.
fn delivered_to_c_reply_lost(module: &mut Replication, seq: u64) {
    let body = recovery_body(
        &b_credential(),
        &chain(seq)[usize::try_from(seq - 1).expect("seq")],
    );
    let staged = step(module, &on(C, delivered(label(B), body)));
    let (batch, _, _) = staged_batch(&staged);
    let replies = step(module, &on(C, committed(batch.0, seq)));
    assert_eq!(
        replies
            .iter()
            .map(|kind| reply_at(kind, B, FRAME_ID, CONFIG))
            .collect::<Vec<_>>(),
        vec![AppendOutcome::Accepted(c_ack(seq))],
        "the reply the network loses"
    );
}

/// M7B-193 (lead rulings B-R67, B-R59a). A recovery source re-sends as a primary's cursor
/// does, on the same per-partition timer: C's ACK for record 13 is lost, the first fire re-sends
/// nothing, and the second re-sends record 13 as the same `SendRecoveryEnvelopes` under the same
/// credential, byte for byte. C answers it `AlreadyHave` and its ACK, which moves the source on,
/// and C reaches 15 and is reported caught up once.
#[retcd_test]
fn m7b_193_a_source_re_sends_the_same_recovery_send_and_its_target_still_catches_up() {
    let mut module = b_and_c(12);
    let first = step_at(&mut module, 0, &catch_up(1, 2, 15, b_credential()));
    assert_eq!(first, vec![recovery_send(15, 15), retransmit_arm(1, 100)]);
    assert_eq!(
        serve(&mut module, &recovery_send(15, 15)),
        vec![recovery_send(13, 13)]
    );
    delivered_to_c_reply_lost(&mut module, 13);

    assert_eq!(
        step_at(&mut module, 100, &retransmit_fired_on(B, 1, 100)),
        vec![retransmit_arm(2, 200)]
    );
    let again = step_at(&mut module, 200, &retransmit_fired_on(B, 2, 200));
    assert_eq!(again, vec![recovery_send(13, 13), retransmit_arm(3, 300)]);
    assert_eq!(
        serve(&mut module, &again[0]),
        vec![
            ignored(KernelIgnoredReason::Replica(ReplicaIgnoreReason::Recorded)),
            recovery_send(14, 14)
        ]
    );
    assert_eq!(
        serve(&mut module, &recovery_send(14, 14)),
        vec![recovery_send(15, 15)]
    );
    assert_eq!(
        serve(&mut module, &recovery_send(15, 15)),
        vec![caught_up_c()]
    );
    assert!(module.source(B, P, CopyId(2)).is_none());
    assert_eq!(module.receiver(C, P).expect("C").applied_head(), head(15));
    assert_eq!(
        step_at(&mut module, 300, &retransmit_fired_on(B, 3, 300)),
        replica_answer(ReplicaIgnoreReason::NotRequired)
    );
}

/// M7B-190 (joint gate A3: the survivor of `if self.tracker.retired()` → `if true` in
/// `Primary::on_recovered`). A primary whose `Recovered` rebuilds it and does not retire it keeps
/// its keepalive: the step cancels no timer, the armed version is still 1, and the fire of that
/// version sends a full round and arms the next. Tester probe p9's shape.
#[retcd_test]
fn m7b_190_a_primary_recovered_and_not_retired_keeps_its_keepalive() {
    use ReplicaRole::{Primary, RegularSecondary, Shadow};
    let mut module = a_b_c();
    assert_eq!(step_at(&mut module, 0, &admission(false)), round(1, 0));
    let keep = on(
        A,
        recovered(
            15,
            d(15),
            pinned([Primary, RegularSecondary, RegularSecondary, Shadow]),
        ),
    );
    let effects = step(&mut module, &keep);
    assert!(
        !effects
            .iter()
            .any(|kind| matches!(kind, EffectKind::Timer(TimerEffect::Cancel { .. }))),
        "{effects:?}"
    );
    assert!(!module.primary(A, P).expect("A").tracker().retired());
    assert_eq!(keepalive_of(&module), Some(TimerVersion(1)));
    let next = step_at(&mut module, 100, &fired(1, 100));
    assert_eq!(next, round(2, 100));
}

/// M7B-201 (lead ruling B-R67c at a recovery source; tester-kb-r1 re-gate M3). C's ACK for
/// record 13 is late, not lost: the second fire re-sends record 13, then the late ACK moves the
/// source to 14, then C answers the re-send with `AlreadyHave` and a second ACK for 13. That
/// ACK is a repeat and sends nothing, so record 14 goes out once, and C still reaches 15.
#[retcd_test]
fn m7b_201_a_source_sends_nothing_for_a_repeat_ack_and_never_doubles_its_walk() {
    let mut module = b_and_c(12);
    let first = step_at(&mut module, 0, &catch_up(1, 2, 15, b_credential()));
    assert_eq!(first, vec![recovery_send(15, 15), retransmit_arm(1, 100)]);
    assert_eq!(
        serve(&mut module, &recovery_send(15, 15)),
        vec![recovery_send(13, 13)]
    );
    delivered_to_c_reply_lost(&mut module, 13);
    step_at(&mut module, 100, &retransmit_fired_on(B, 1, 100));
    assert_eq!(
        step_at(&mut module, 200, &retransmit_fired_on(B, 2, 200)),
        vec![recovery_send(13, 13), retransmit_arm(3, 300)]
    );
    assert_eq!(
        step(
            &mut module,
            &reply_from(label(C), &AppendOutcome::Accepted(c_ack(13)))
        ),
        vec![recovery_send(14, 14)],
        "the late ACK"
    );
    assert_eq!(
        serve(&mut module, &recovery_send(13, 13)),
        vec![
            ignored(KernelIgnoredReason::Replica(ReplicaIgnoreReason::Recorded)),
            ignored(KernelIgnoredReason::Replica(ReplicaIgnoreReason::Recorded)),
        ],
        "the re-send's AlreadyHave and its repeat ACK"
    );
    assert_eq!(
        serve(&mut module, &recovery_send(14, 14)),
        vec![recovery_send(15, 15)]
    );
    assert_eq!(
        serve(&mut module, &recovery_send(15, 15)),
        vec![caught_up_c()]
    );
    assert!(module.source(B, P, CopyId(2)).is_none());
}

// --- M9 S0 D3: F1's re-emit of the generation already served -------------------------------

/// B after the takeover at `(10, d10)` into `NEW_GEN`, then records 11 and 12 from C, the new
/// primary, sealed in `NEW_GEN` and chained on each other, applied and ACKed: a copy that has
/// applied past the cutoff in this generation.
fn applied_past_the_cutoff() -> Replication {
    let mut module = recovered_at_ten();
    let mut prev = d(10);
    for mut env in chain(12).into_iter().skip(10) {
        env.prev_digest = prev;
        let env = taken_over(env);
        prev = env.record_digest;
        applied_from_c(&mut module, &env);
    }
    assert_eq!(rx(&module).applied_head().seq, Seq(12));
    module
}

/// The one unsolicited `NeedPrefix` a re-anchoring `Recovered` sends C, from `have`.
fn asks_from(effects: &[EffectKind], have: Head) {
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(
        reply_at(&effects[0], C, UNSOLICITED, NEW_CONFIG),
        rejected(AppendReject::NeedPrefix {
            have: have.seq,
            head_digest: have.digest,
        })
    );
}

/// M9 S0 D3 (lead ruling "S0 D3" rule 2). F1 re-emits the takeover it already committed, same
/// generation and cutoff, once its rebuild finishes. B holds the cutoff and has applied 11 and 12
/// from this generation's primary since: it keeps them and asks C for what follows 12. Before
/// the fix it truncated to 10, though 11 and 12 were this generation's records.
#[retcd_test]
fn m9_d3_03_receiver_a_re_emit_of_its_generation_keeps_the_head_it_applied() {
    let mut module = applied_past_the_cutoff();
    let held = rx(&module).applied_head();
    let effects = step(&mut module, &recovered(10, d(10), takeover_config()));
    asks_from(&effects, held);
    let rx_ = rx(&module);
    assert_eq!(
        (
            rx_.applied_head(),
            rx_.received_seq(),
            rx_.history().highest(),
            rx_.history().digest_at(Seq(10)),
        ),
        (held, ReceivedSeq(12), Some(Seq(12)), Some(d(10)))
    );
    assert_eq!(
        (rx_.lineage().generation, rx_.quarantine(), rx_.retired()),
        (NEW_GEN, None, false)
    );
}

/// M9 S0 D3, rule 2's quarantine guard. B applied 11 and 12 under `GEN`, then the takeover into
/// `NEW_GEN` named a different record at 11, so B is quarantined with head 12. F1's re-emit in
/// that generation at `(10, d10)` still truncates to 10 and clears quarantine: 11 and 12 are
/// the divergence, and keeping them would unflag it (`s0-probe.md` E6 mutant).
#[retcd_test]
fn m9_d3_04_receiver_a_quarantined_copy_still_truncates_on_a_re_emit_of_its_generation() {
    let mut module = applied_to(12);
    assert_eq!(
        step(&mut module, &recovered(11, d(9), takeover_config())),
        [quarantine_alert()]
    );
    let quarantined = rx(&module);
    assert_eq!(
        (quarantined.lineage().generation, quarantined.applied_head()),
        (NEW_GEN, head(12))
    );
    assert!(quarantined.quarantine().is_some());
    let effects = step(&mut module, &recovered(10, d(10), takeover_config()));
    asks_new_primary(&effects, 10);
    let rx_ = rx(&module);
    assert_eq!(
        (
            rx_.applied_head(),
            rx_.quarantine(),
            rx_.history().highest()
        ),
        (head(10), None, Some(Seq(10)))
    );
}

/// M9 S0 D3, rule 2's lookup guard. A re-emit of B's own generation whose cutoff digest is not
/// the one B holds still quarantines and moves no head, as any `Differs` does (K-B-44).
#[retcd_test]
fn m9_d3_05_receiver_a_re_emit_whose_cutoff_differs_still_quarantines() {
    let mut module = applied_past_the_cutoff();
    let before = rx(&module).applied_head();
    assert_eq!(
        step(&mut module, &recovered(10, d(9), takeover_config())),
        [quarantine_alert()]
    );
    let rx_ = rx(&module);
    assert_eq!(rx_.applied_head(), before);
    assert!(rx_.quarantine().is_some());
}

/// M9 S0 D3, rule 2's retired guard. The takeover first pins B primary, so B's receiver retires
/// into `NEW_GEN` while A's record 11, staged under `GEN`, still commits. A re-emit in `NEW_GEN`
/// that pins B a secondary again truncates to the cutoff: 11 is the old primary's record, not
/// this generation's, so it is not kept.
#[retcd_test]
fn m9_d3_06_receiver_a_retired_copy_still_truncates_on_a_re_emit_of_its_generation() {
    use ReplicaRole::{Primary, RegularSecondary, Shadow};
    let b_leads = pinned([RegularSecondary, Primary, RegularSecondary, Shadow]);
    let mut module = module();
    send(&mut module, label(A), &golden());
    step(
        &mut module,
        &requiring(recovered(10, d(10), b_leads), &[1], B),
    );
    step(&mut module, &committed(0, 11));
    let retired = rx(&module);
    assert!(retired.retired());
    assert_eq!(
        (retired.lineage().generation, retired.applied_head()),
        (NEW_GEN, head(11))
    );
    // B's primary, which the first pin built, is refused by the second; its receiver re-anchors.
    let effects = step(&mut module, &recovered(10, d(10), takeover_config()));
    assert_eq!(effects.len(), 2, "{effects:?}");
    asks_new_primary(&effects[..1], 10);
    assert_eq!(effects[1], replica(ReplicaIgnoreReason::InvalidConfig));
    let rx_ = rx(&module);
    assert_eq!(
        (rx_.applied_head(), rx_.retired(), rx_.history().highest()),
        (head(10), false, Some(Seq(10)))
    );
}
