//! M9 close-out C1: one client script, run through the M7 simulator and through the real host,
//! with the replies compared step by step (ruling A-R150; M9 architecture §3, and §9 risk 1,
//! "host diverges from `Dispatcher`").
//!
//! - **Simulator:** the S0 case B world (`cases::m9_s0_case_b`): three empty copies, prior
//!   generation 0, so F1 creates generation 1, the real host's shape. Each step is a seed on the
//!   primary B, spaced so it finishes before the next.
//! - **Host:** `rdb_api::Db` over a `config_testkit::MemStore`, three node threads on RocksDB
//!   and wall-clock time. Writes go through `Db::resend` as raw requests, so both sides run the
//!   same `TxnRequest`; reads go through `Db::get`.
//! - **The script** builds keys with `rdb_value::keys::root_key` and values as byte documents in
//!   the envelope, exactly what `Db::get` reads. The simulator answers a read with a version and
//!   a digest, never bytes; the digest is matched to a value the script wrote and decoded with
//!   `rdb_value::read`, the function the host's read uses.
//! - **Compared per step:** the result kind, a write's `seq` and `generation`, and a read's
//!   version and decoded bytes; then the final published `(generation, seq)` on the owner. A
//!   read's view position is not compared: the simulator's read reply does not carry one. Never
//!   ticks, node ids or timings.
//!
//! **What this does not prove (critic F8).** It proves equality at the reply level only. The
//! divergences risk 1 names (zero-hop order, `ConfigChanged` before R1, the host's §4 duties)
//! show here only if they change a reply or the final published position.

mod support;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_log::retcd_test;
use rdb_api::{Db, DbConfig, Timeouts};
use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::errors::ErrorKind;
use rdb_core::contracts::event::{ClientEvent, EventKind, ReplyEffect};
use rdb_core::contracts::ids::{
    AffinityId, ClientId, CorrelationId, Generation, RequestId, RequestIdentity, Seq, TenantId,
};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{ReadServiceOutcome, Trace, TraceKind};
use rdb_core::contracts::txn::{Mutation, TxnRequest};
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::harness::run::{RunPlan, Runner, SeedEvent, StopReason};
use rdb_sim::harness::trace::validate;
use rdb_value::envelope::{seal, Kind};
use rdb_value::keys::{root_key, RootKey};
use rdb_value::testing::MapSnapshot;
use rdb_value::value::Value;

use support::oracle::Oracle;
use support::scenarios::cases::{self, B_NODE, PARTITION};
use support::scenarios::run as scenario_run;

/// The tenant, client and group `Db` uses for its own calls (`db.rs` `TENANT`, `CLIENT`,
/// `GROUP`), so `Db::get` reads the keys the script writes.
const TENANT: TenantId = TenantId(1);
const CLIENT: ClientId = ClientId(1);
const GROUP: AffinityId = AffinityId(1);

/// Every write's kernel deadline, on both sides (critic F2): long, so a stalled host thread is
/// not read as a divergence. The deadline is outside the request digest.
const DEADLINE_MILLIS: u64 = 30_000;

/// The first step's tick: case B's write tick, after L1 resumes.
const FIRST_AT: u64 = cases::M9_S0_SUBMIT_AT;
/// The ticks between two steps: each finishes well inside it.
const STEP_GAP: u64 = 500;

/// How long the host may take to admit writes after `Db::open`, before the deadline scale. The
/// spec's resume hold alone is 5 s.
const READY_PATIENCE: Duration = Duration::from_secs(60);

/// What a step asks.
#[derive(Debug, Clone)]
enum Action {
    /// Send this request; the host sends it with `Db::resend`.
    Write(TxnRequest),
    /// Read this object in [`GROUP`].
    Read(&'static [u8]),
}

/// One step of the script.
#[derive(Debug, Clone)]
struct Step {
    label: &'static str,
    action: Action,
}

/// A byte document in the envelope: what `Db::put` stores and `Db::get` reads.
fn byte_doc(value: &[u8]) -> Bytes {
    let payload = rdb_value::cbor::encode(&Value::Bytes(value.to_vec())).expect("encodes");
    seal(Kind::Document, &payload).expect("seals")
}

/// The root key of `object` in `group`.
fn key(group: AffinityId, object: &[u8]) -> RootKey {
    root_key(TENANT, group, object)
}

/// A put of `value` at `object` in `group`.
fn put(group: AffinityId, object: &[u8], value: &[u8], expected_version: Option<u64>) -> Mutation {
    Mutation::Put {
        key: key(group, object).to_bytes(),
        value: byte_doc(value),
        expected_version,
    }
}

/// A write by this client under `request` in [`GROUP`].
fn write(label: &'static str, request: u64, mutations: Vec<Mutation>) -> Step {
    Step {
        label,
        action: Action::Write(TxnRequest {
            api_version: API_VERSION,
            identity: RequestIdentity {
                tenant: TENANT,
                client: CLIENT,
                request: RequestId(request),
            },
            affinity: GROUP,
            expected_generation: None,
            remaining_millis: DEADLINE_MILLIS,
            conditions: Vec::new(),
            mutations,
        }),
    }
}

/// Another group of the same tenant, for the cross-group refusal.
const OTHER_GROUP: AffinityId = AffinityId(2);

/// What step 1 writes at `a`, and step 2 must read back.
const HELLO: &[u8] = b"hello";

/// The kinds the script means each step to get, in step order (review F-001). Step 4 is the
/// identical retry of step 1: the kernel replays its answer as `Published`, or as
/// `RecoveredApplied` from retained history; either is the same write.
const INTENDED: [&[&str]; 7] = [
    &["Published"],
    &["Read"],
    &["ConditionFailed"],
    &["Published", "RecoveredApplied"],
    &["RequestIdReuse"],
    &["Published"],
    &["CrossAffinity"],
];

/// The script: S0 and S1 shapes.
fn script() -> Vec<Step> {
    let first = vec![put(GROUP, b"a", HELLO, None)];
    vec![
        write("put a", 1, first.clone()),
        Step {
            label: "get a",
            action: Action::Read(b"a"),
        },
        // `a` is at version 2; this asks for 7.
        write(
            "put a if version 7",
            2,
            vec![put(GROUP, b"a", b"world", Some(7))],
        ),
        write("put a again, same request", 1, first),
        write(
            "request 1, changed payload",
            1,
            vec![put(GROUP, b"a", b"other", None)],
        ),
        write(
            "txn put b and c",
            3,
            vec![
                put(GROUP, b"b", b"bee", None),
                put(GROUP, b"c", b"sea", None),
            ],
        ),
        write(
            "txn across two groups",
            4,
            vec![
                put(GROUP, b"d", b"dee", None),
                put(OTHER_GROUP, b"e", b"eee", None),
            ],
        ),
    ]
}

/// What a step's read saw, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReadBytes {
    /// The step is not a read, or the read was refused.
    NotRead,
    /// The object is absent.
    Absent,
    /// The object's version and its byte string.
    Found { version: u64, bytes: Vec<u8> },
    /// The simulator's digest matches no value the script wrote, or does not decode.
    Undecoded(String),
}

impl fmt::Display for ReadBytes {
    /// Never the bytes themselves (team rules): their length and a digest.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRead => f.write_str("-"),
            Self::Absent => f.write_str("absent"),
            Self::Found { version, bytes } => {
                let digest = Digest::of(Domain::ReadValue, &[bytes]).to_hex();
                write!(f, "v{version} {}B {}", bytes.len(), &digest[..12])
            }
            Self::Undecoded(why) => write!(f, "undecoded: {why}"),
        }
    }
}

/// One step's reply, normalised: nothing in it depends on ticks, node ids or timings.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reply {
    /// `Published` or `RecoveredApplied` for a write, `Read` for a served read, or the error
    /// kind.
    kind: String,
    /// A write's position. `None` for a read: the simulator's read reply carries no view
    /// position (`ReplyEffect::Read` holds a version and a digest only, and its trace records
    /// no `Read` line), so the host's view position is logged, not compared. A read's version,
    /// the seq that wrote what it saw, is in [`Self::read`].
    seq: Option<u64>,
    /// A write's generation; `None` for a read, as [`Self::seq`].
    generation: Option<u64>,
    /// What a read saw.
    read: ReadBytes,
}

impl Reply {
    fn failed(kind: ErrorKind) -> Self {
        Self {
            kind: format!("{kind:?}"),
            seq: None,
            generation: None,
            read: ReadBytes::NotRead,
        }
    }

    /// A served read that saw `read`; whether it waited at the barrier is timing, not compared.
    fn served(read: ReadBytes) -> Self {
        Self {
            kind: String::from("Read"),
            seq: None,
            generation: None,
            read,
        }
    }
}

fn show(field: Option<u64>) -> String {
    field.map_or_else(|| String::from("-"), |n| n.to_string())
}

/// One field of one step that differs between the two sides.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Difference {
    /// The step, from 1.
    step: usize,
    label: String,
    field: &'static str,
    sim: String,
    host: String,
}

impl fmt::Display for Difference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "step {} ({}): {} differs: sim {}, host {}",
            self.step, self.label, self.field, self.sim, self.host
        )
    }
}

/// Every field that differs, step by step. A pure function: the positive control (critic F9)
/// drives it with edited tables.
fn compare(labels: &[&str], sim: &[Reply], host: &[Reply]) -> Vec<Difference> {
    let mut out = Vec::new();
    for (index, label) in labels.iter().enumerate() {
        let step = index + 1;
        let (Some(s), Some(h)) = (sim.get(index), host.get(index)) else {
            out.push(Difference {
                step,
                label: (*label).to_owned(),
                field: "reply",
                sim: sim.get(index).map_or("none", |_| "some").to_owned(),
                host: host.get(index).map_or("none", |_| "some").to_owned(),
            });
            continue;
        };
        let mut differ = |field, sim: String, host: String| {
            if sim != host {
                out.push(Difference {
                    step,
                    label: (*label).to_owned(),
                    field,
                    sim,
                    host,
                });
            }
        };
        differ("kind", s.kind.clone(), h.kind.clone());
        differ("seq", show(s.seq), show(h.seq));
        differ("generation", show(s.generation), show(h.generation));
        // Rendered values carry a digest of the bytes, so equal renders are equal bytes; the
        // full values are compared too, so a digest prefix collision cannot hide a difference.
        if s.read != h.read {
            out.push(Difference {
                step,
                label: (*label).to_owned(),
                field: "read",
                sim: s.read.to_string(),
                host: h.read.to_string(),
            });
        }
    }
    out
}

// ---------------------------------------------------------------- the simulator

/// The correlation of step `index`'s seed.
fn correlation(index: usize) -> CorrelationId {
    CorrelationId(9_200 + index as u64)
}

/// The identity of step `index`'s read: its own request id, apart from the writes'.
fn read_identity(index: usize) -> RequestIdentity {
    RequestIdentity {
        tenant: TENANT,
        client: CLIENT,
        request: RequestId(1_000 + index as u64),
    }
}

/// Case B with one seed per step on the primary B.
fn sim_plan(steps: &[Step]) -> RunPlan {
    let last_at = FIRST_AT + STEP_GAP * steps.len() as u64;
    let mut plan = cases::m9_s0_case_b(last_at + 2_000);
    for (index, step) in steps.iter().enumerate() {
        let kind = match &step.action {
            Action::Write(request) => ClientEvent::Submit(request.clone()),
            Action::Read(object) => ClientEvent::Read {
                identity: read_identity(index),
                key: key(GROUP, object).to_bytes(),
            },
        };
        plan.seed.push(SeedEvent {
            at: Tick(FIRST_AT + STEP_GAP * index as u64),
            node: B_NODE,
            boot: scenario_run::BOOT,
            partition: PARTITION,
            correlation: correlation(index),
            kind: EventKind::Client(kind),
        });
    }
    plan
}

/// The run, to its tick budget: the replies and the validated trace, which every oracle accepts.
fn sim_run(plan: &RunPlan) -> (Vec<ReplyEffect>, Trace) {
    let mut runner = Runner::new(plan).expect("the harness takes the plan");
    let report = runner.run(plan.limits).expect("the run completes");
    tracing::info!(stop = ?report.stop, events = report.events_consumed, "sim run report");
    assert!(
        matches!(report.stop, StopReason::DeadlineReached { deadline, .. }
            if deadline == plan.limits.deadline),
        "runs to its tick budget, never out of events: {:?}",
        report.stop
    );
    let trace = runner.finish().expect("the trace closes");
    validate(&trace).expect("a well-formed trace");
    let judged = Oracle::new().judge(&trace);
    assert!(
        judged.is_clean(),
        "every oracle accepts the run: {:#?}",
        judged.violations()
    );
    let replies = report.replies.into_iter().map(|(_, reply)| reply).collect();
    (replies, trace)
}

/// The identity a reply answers.
fn answered(reply: &ReplyEffect) -> RequestIdentity {
    match reply {
        ReplyEffect::Transaction { identity, .. }
        | ReplyEffect::Failed { identity, .. }
        | ReplyEffect::Status { identity, .. }
        | ReplyEffect::Read { identity, .. } => *identity,
    }
}

/// The simulator's read of `object`, decoded: its digest matched to a value the script wrote at
/// that key, and that value read back with `rdb_value::read`, as the host reads.
fn sim_read_bytes(steps: &[Step], object: &[u8], version: u64, digest: Digest) -> ReadBytes {
    let root = key(GROUP, object);
    let written = steps
        .iter()
        .filter_map(|step| match &step.action {
            Action::Write(request) => Some(request.mutations.iter()),
            Action::Read(_) => None,
        })
        .flatten()
        .find_map(|mutation| match mutation {
            Mutation::Put { key, value, .. }
                if key.as_ref() == root.as_bytes()
                    && Digest::of(Domain::ReadValue, &[key, value]) == digest =>
            {
                Some(value.clone())
            }
            _ => None,
        });
    let Some(stored) = written else {
        return ReadBytes::Undecoded(format!("digest {} matches no written value", digest));
    };
    let mut snapshot = MapSnapshot::new(Generation(0));
    snapshot.insert(root.to_bytes(), version, stored);
    match rdb_value::read(&snapshot, &root) {
        Ok(Some(document)) => match document.value {
            Value::Bytes(bytes) => ReadBytes::Found {
                version: document.version,
                bytes,
            },
            other => ReadBytes::Undecoded(format!("not a byte string: {other:?}")),
        },
        Ok(None) => ReadBytes::Undecoded(String::from("the record vanished")),
        Err(error) => ReadBytes::Undecoded(error.to_string()),
    }
}

/// Every `publish` on the primary, as `(generation, seq)`, in trace order.
fn sim_published(trace: &Trace) -> Vec<(Generation, Seq)> {
    trace
        .events
        .iter()
        .filter(|event| event.node == B_NODE)
        .filter_map(|event| match &event.kind {
            TraceKind::Publish {
                generation, seq, ..
            } => Some((*generation, *seq)),
            _ => None,
        })
        .collect()
}

/// The script through the simulator: one normalised reply per step, and the final published
/// position on the primary.
fn run_sim(steps: &[Step]) -> (Vec<Reply>, (Generation, Seq)) {
    let (replies, trace) = sim_run(&sim_plan(steps));
    let client: Vec<&ReplyEffect> = replies
        .iter()
        .filter(|reply| {
            let identity = answered(reply);
            identity.tenant == TENANT && identity.client == CLIENT
        })
        .collect();
    assert_eq!(
        client.len(),
        steps.len(),
        "the simulator answers each step once: {client:#?}"
    );
    let normalised = steps
        .iter()
        .zip(client)
        .enumerate()
        .map(|(index, (step, reply))| {
            let asked = match &step.action {
                Action::Write(request) => request.identity,
                Action::Read(_) => read_identity(index),
            };
            assert_eq!(
                answered(reply),
                asked,
                "step {} is answered in step order",
                index + 1
            );
            match (reply, &step.action) {
                (ReplyEffect::Transaction { result, .. }, _) => Reply {
                    kind: format!("{:?}", result.outcome),
                    seq: Some(result.seq.0),
                    generation: Some(result.generation.0),
                    read: ReadBytes::NotRead,
                },
                (ReplyEffect::Failed { error, .. }, _) => Reply::failed(error.kind()),
                (
                    ReplyEffect::Read {
                        outcome: ReadServiceOutcome::Rejected(kind),
                        ..
                    },
                    _,
                ) => Reply::failed(*kind),
                (ReplyEffect::Read { value, .. }, Action::Read(object)) => {
                    Reply::served(value.map_or(ReadBytes::Absent, |(version, digest)| {
                        sim_read_bytes(steps, object, version, digest)
                    }))
                }
                (other, _) => panic!("step {}: an unexpected reply {other:?}", index + 1),
            }
        })
        .collect();
    let published = sim_published(&trace);
    let last = *published.last().expect("the primary published");
    (normalised, last)
}

// ---------------------------------------------------------------- the host

/// The kinds that say the host's environment failed the step, not that it answered otherwise
/// (critic F2): no answer in time, no quorum, a protection pause, a lost grant.
const HOST_ENVIRONMENT: [ErrorKind; 4] = [
    ErrorKind::UnknownOutcome,
    ErrorKind::Unavailable,
    ErrorKind::ProtectionPaused,
    ErrorKind::LeaseExpired,
];

fn host_refused(index: usize, step: &Step, error: &rdb_api::ApiError) -> Reply {
    assert!(
        !HOST_ENVIRONMENT.contains(&error.kind),
        "host environment, not divergence: step {} ({}) answered {:?}: {}",
        index + 1,
        step.label,
        error.kind,
        error.detail
    );
    Reply::failed(error.kind)
}

/// Wait until node 1 admits writes.
fn wait_ready(db: &Db) {
    let patience = READY_PATIENCE * config_testkit::poll::deadline_scale();
    let started = Instant::now();
    while db.node_status().first().and_then(|node| node.admits) != Some(true) {
        assert!(
            started.elapsed() < patience,
            "host environment, not divergence: node 1 does not admit writes within {patience:?}: {:#?}",
            db.node_status()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    tracing::info!(
        millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "host ready"
    );
}

/// The script through the host: one normalised reply per step, and the final published
/// position on node 1.
fn run_host(steps: &[Step]) -> (Vec<Reply>, (Generation, Seq)) {
    let dir = config_testkit::fs::temp_dir();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let config = DbConfig {
        dir: dir.path().to_path_buf(),
        hold: Vec::new(),
        timeouts: Timeouts {
            put: Duration::from_millis(DEADLINE_MILLIS),
            read: Duration::from_millis(DEADLINE_MILLIS),
        },
        dedup_cap: None,
        dedup_retention: None,
        local: rdb_api::db::ALL_NODES.to_vec(),
        links: None,
        client: rdb_api::db::DEFAULT_CLIENT,
    };
    let mut db = rt
        .block_on(Db::open(
            config,
            Arc::new(config_testkit::MemStore::new()),
            rt.handle().clone(),
        ))
        .expect("the host opens");
    wait_ready(&db);
    let replies = steps
        .iter()
        .enumerate()
        .map(|(index, step)| match &step.action {
            Action::Write(request) => match db.resend(request.clone()) {
                Ok(ok) => Reply {
                    kind: format!("{:?}", ok.outcome),
                    seq: Some(ok.seq.0),
                    generation: Some(ok.generation.0),
                    read: ReadBytes::NotRead,
                },
                Err(refused) => host_refused(index, step, &refused.error),
            },
            Action::Read(object) => match db.get(object) {
                Ok(got) => {
                    tracing::info!(
                        step = index + 1,
                        generation = got.generation.0,
                        at = got.at.0,
                        waited = got.waited,
                        "host read view, not compared"
                    );
                    Reply::served(got.value.map_or(ReadBytes::Absent, |(version, bytes)| {
                        ReadBytes::Found {
                            version,
                            bytes: bytes.to_vec(),
                        }
                    }))
                }
                Err(error) => host_refused(index, step, &error),
            },
        })
        .collect();
    let published = db
        .node_status()
        .first()
        .and_then(|node| node.published)
        .expect("node 1 reports a published position");
    db.shutdown();
    (replies, published)
}

/// One info line per step with both sides' fields, for DuckDB.
fn log_table(labels: &[&str], sim: &[Reply], host: &[Reply]) {
    for (index, label) in labels.iter().enumerate() {
        let (s, h) = (&sim[index], &host[index]);
        tracing::info!(
            step = index + 1,
            label = *label,
            sim_kind = %s.kind,
            sim_seq = s.seq,
            sim_generation = s.generation,
            sim_read = %s.read,
            host_kind = %h.kind,
            host_seq = h.seq,
            host_generation = h.generation,
            host_read = %h.read,
            same = s == h,
            "host_vs_sim_step"
        );
    }
}

/// C1: the script gets the same replies from the simulator and from the real host, step by
/// step, both end on the same published position, and those are the answers the script means.
///
/// Integration (~8 s, 7.5 s of it waiting for the host to admit writes): three host node
/// threads on wall-clock time and RocksDB, with the spec's budgets as the simulator runs them.
/// The resume hold alone keeps writes out for 5 s after the open; `Db::open` takes no other.
#[retcd_test]
fn m9_c1_01_one_script_gets_the_same_replies_from_the_simulator_and_the_host() {
    support::preamble();
    let steps = script();
    let labels: Vec<&str> = steps.iter().map(|step| step.label).collect();
    let (sim, sim_published) = run_sim(&steps);
    let (host, host_published) = run_host(&steps);
    log_table(&labels, &sim, &host);
    tracing::info!(
        sim_generation = sim_published.0 .0,
        sim_seq = sim_published.1 .0,
        host_generation = host_published.0 .0,
        host_seq = host_published.1 .0,
        same = sim_published == host_published,
        "host_vs_sim_published"
    );
    let differences = compare(&labels, &sim, &host);
    assert!(
        differences.is_empty(),
        "the host diverges from the simulator:\n{}",
        differences
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(
        sim_published, host_published,
        "final published (generation, seq) differs: sim {sim_published:?}, host {host_published:?}"
    );
    // The host equals the simulator, so pinning the simulator pins both.
    assert_script_intent(&labels, &sim, sim_published);
}

/// The simulator got the answers the script means (review F-001). Without this, a drift in a
/// helper both sides share (the api version, the key or value format) refuses every step on
/// both sides alike, and the comparison alone stays green.
fn assert_script_intent(labels: &[&str], sim: &[Reply], published: (Generation, Seq)) {
    assert_eq!(sim.len(), INTENDED.len(), "one intended kind per step");
    for (index, (reply, intended)) in sim.iter().zip(INTENDED).enumerate() {
        assert!(
            intended.contains(&reply.kind.as_str()),
            "script intent: step {} ({}) answered {}, meant {intended:?}",
            index + 1,
            labels[index],
            reply.kind
        );
    }
    let (first, retry, txn) = (&sim[0], &sim[3], &sim[5]);
    assert_eq!(
        (retry.seq, retry.generation),
        (first.seq, first.generation),
        "script intent: step 4, the identical retry, answers step 1's position"
    );
    let first_seq = first.seq.expect("step 1 published");
    assert_eq!(
        sim[1].read,
        ReadBytes::Found {
            version: first_seq,
            bytes: HELLO.to_vec(),
        },
        "script intent: step 2 reads what step 1 wrote"
    );
    let txn_seq = txn.seq.expect("step 6 published");
    assert_eq!(
        txn_seq,
        first_seq + 1,
        "script intent: the two-key txn is the next write after step 1"
    );
    assert_eq!(
        (published.0 .0, published.1 .0),
        (txn.generation.expect("step 6 published"), txn_seq),
        "script intent: the two-key txn is the last write published"
    );
}

/// The positive control (critic F9): the comparator sees an edit to each field, and names the
/// step and the field. Unit (<1 ms).
#[retcd_test]
fn m9_c1_02_the_comparator_names_the_step_and_field_of_each_edit() {
    support::preamble();
    let labels = ["put a", "get a", "put again"];
    let base = vec![
        Reply {
            kind: String::from("Published"),
            seq: Some(2),
            generation: Some(1),
            read: ReadBytes::NotRead,
        },
        Reply {
            kind: String::from("Read"),
            seq: Some(2),
            generation: Some(1),
            read: ReadBytes::Found {
                version: 2,
                bytes: b"hello".to_vec(),
            },
        },
        Reply::failed(ErrorKind::RequestIdReuse),
    ];
    assert_eq!(
        compare(&labels, &base, &base),
        Vec::new(),
        "a table equals itself"
    );

    type Edit = fn(&mut Vec<Reply>);
    let cases: [(&str, usize, Edit); 4] = [
        ("kind", 3, |t| t[2].kind = String::from("ConditionFailed")),
        ("seq", 1, |t| t[0].seq = Some(3)),
        ("generation", 2, |t| t[1].generation = Some(2)),
        ("read", 2, |t| {
            t[1].read = ReadBytes::Found {
                version: 2,
                bytes: b"hellp".to_vec(),
            }
        }),
    ];
    for (field, step, edit) in cases {
        let mut edited = base.clone();
        edit(&mut edited);
        let differences = compare(&labels, &base, &edited);
        tracing::info!(field, step, found = differences.len(), "comparator control");
        assert_eq!(
            differences
                .iter()
                .map(|d| (d.step, d.field))
                .collect::<Vec<_>>(),
            vec![(step, field)],
            "an edit to {field} at step {step} is named, and nothing else: {differences:?}"
        );
        let message = differences[0].to_string();
        assert!(
            message.starts_with(&format!("step {step} ({})", labels[step - 1])),
            "the message names the step: {message}"
        );
    }
}
