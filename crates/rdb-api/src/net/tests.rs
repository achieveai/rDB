//! Rows for the process links (M10 S2, W4).
//!
//! Wire rows are unit rows: a message goes through the same `outbound → encode → decode →
//! inbound` path a link takes, and must come out as it went in. Link rows start two [`Net`]s
//! in one process on loopback, under real mutual TLS, and are integration rows (~100 ms each:
//! a TLS handshake each way).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path as FsPath;
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rdb_core::contracts::ids::{
    BootId, ClientId, Generation, NodeId, PartitionId, RequestId, RequestIdentity,
};
use rdb_core::contracts::membership::{CopyId, PartitionConfig};
use rdb_core::contracts::recovery::{CommittedRoot, DurableProof, RecoveryBarrier, RecoveryResult};
use rdb_value::blob::{Blob, Manifest};
use rdb_value::collection::{Collection, CollectionKind, ElemOp, Member, Members};
use rdb_value::delta::{Delta, Op};
use rdb_value::list::{Item, Items, List, Token};
use rdb_value::path::Path;
use rdb_value::value::{Int, Value};
use rdb_value::{Document, Expected};

use super::faults::Faults;
use super::wire::{self, Frame, Inbound, Label, Outbound, WireAnswer, WireMsg, WireReply};
use super::{certs, LinkState, Net, NetConfig, PROTOCOL_VERSION};
use crate::db::ApiError;
use crate::host::{
    Answer, Change, Client, ClientCall, DormantSeed, Holding, ListEdit, Look, Msg, NodeStatus,
    PeerAsk, PutOp, Seen,
};
use crate::transport::Links;
use rdb_core::contracts::errors::{ErrorKind, RetryRule};

/// A value of `T` decoded from zeros: every integer 0, every list empty, every enum its first
/// variant. For contract types a link carries as they are (serde-derived in rdb-core), where
/// the row cares that the field arrives, not what is in it.
fn zero<T: serde::de::DeserializeOwned>() -> T {
    postcard::from_bytes(&[0u8; 4096])
        .unwrap_or_else(|e| panic!("{} from zeros: {e}", std::any::type_name::<T>()))
}

fn identity(client: u32, request: u64) -> RequestIdentity {
    RequestIdentity {
        client: ClientId(client),
        request: RequestId(request),
        ..zero()
    }
}

fn int(v: i128) -> Value {
    Value::Integer(Int::new(v).expect("an int"))
}

/// Which [`WireMsg`] kind a message is. No wildcard: a new kind fails to compile here until a
/// row covers it.
const fn kind(msg: &WireMsg) -> usize {
    match msg {
        WireMsg::Frame { .. } => 0,
        WireMsg::PeerAsk { .. } => 1,
        WireMsg::CatchUp { .. } => 2,
        WireMsg::Restore { .. } => 3,
        WireMsg::Routed { .. } => 4,
        WireMsg::Landed { .. } => 5,
        WireMsg::Client { .. } => 6,
        WireMsg::Inspect { .. } => 7,
        WireMsg::Answered { .. } => 8,
    }
}
const KINDS: usize = 9;

/// `msg` as the far side of a link posts it, and the kind it travelled as.
fn across(to: NodeId, msg: Msg) -> (Inbound, usize) {
    let wire = match wire::outbound(to, msg).expect("a peer message") {
        Outbound::Plain(wire) => wire,
        Outbound::Client {
            to,
            partition,
            call,
            ..
        } => WireMsg::Client {
            to,
            partition,
            call,
            reply: 7,
        },
        Outbound::Inspect { to, partition, .. } => WireMsg::Inspect {
            to,
            partition,
            reply: 8,
        },
    };
    let bytes = wire::encode(&Frame::Msg(wire));
    let Frame::Msg(wire) = wire::decode(&bytes).expect("decodes") else {
        panic!("not a message frame");
    };
    let kind = kind(&wire);
    (wire::inbound(wire).expect("lands"), kind)
}

/// A valid partition 1 configuration: the two contract types that validate on decode
/// (`PartitionConfig`, `RecoveryBarrier`) cannot come from zeros.
fn config() -> PartitionConfig {
    PartitionConfig::new(PartitionId(1), zero(), crate::admin::members_at(BootId(5)))
}

/// A recovery result that decodes: a one-copy barrier at seq 0, the rest from zeros.
fn landed() -> RecoveryResult {
    let barrier = RecoveryBarrier::try_new(
        &[DurableProof {
            copy: CopyId(1),
            partition: PartitionId(1),
            seq: zero(),
            digest: zero(),
            boot: BootId(5),
        }],
        &BTreeSet::from([CopyId(1)]),
        zero(),
        zero(),
    )
    .expect("a one-copy barrier");
    RecoveryResult {
        fenced_prior: zero(),
        inventories: Vec::new(),
        selected: zero(),
        new_generation: Generation(9),
        mode: zero(),
        barrier,
        loss: zero(),
        committed: CommittedRoot {
            revision: zero(),
            pinned_config: config(),
            authority_view: zero(),
        },
        retained_status_map: zero(),
    }
}

fn seed() -> Box<DormantSeed> {
    let mut seed: DormantSeed = DormantSeed {
        config: config(),
        lineage: zero(),
        revision: zero(),
    };
    seed.lineage.partition = PartitionId(1);
    seed.revision.0 = 41;
    Box::new(seed)
}

/// Every client call shape the API sends: each change, each look.
fn calls() -> Vec<ClientCall> {
    let upload = [5u8; 16];
    let changes = vec![
        Change::Doc(Delta(vec![
            Op::Replace(Value::Text("whole".to_owned())),
            Op::Set(Path::parse("/a/b").expect("path"), int(-3)),
            Op::Remove(Path::parse("/c").expect("path")),
            Op::Increment(
                Path::parse("/n").expect("path"),
                Int::new(i128::from(i64::MIN) - 1).expect("an int"),
            ),
        ])),
        Change::Map(vec![
            ElemOp::Put(Value::Text("k".to_owned()), Value::Bytes(vec![1, 2])),
            ElemOp::Remove(Value::Text("gone".to_owned())),
            ElemOp::NeedPresent(Value::Text("here".to_owned())),
        ]),
        Change::Set(vec![
            ElemOp::Add(int(9)),
            ElemOp::NeedAbsent(Value::Bool(true)),
        ]),
        Change::List(vec![
            ListEdit::Push(Value::Null),
            ListEdit::Insert {
                at: -1,
                value: int(4),
            },
            ListEdit::Remove { at: 2 },
            ListEdit::Replace {
                at: 0,
                value: Value::Array(vec![int(1), Value::Text("x".to_owned())]),
            },
            ListEdit::Move { from: 3, to: -2 },
        ]),
        Change::Chunk {
            upload,
            index: 3,
            bytes: Bytes::from_static(b"chunk"),
        },
        Change::Publish {
            upload,
            size: 10,
            chunk_size: 4,
            sha256: [7; 32],
        },
        Change::Gc { floor: 12 },
        Change::Clear,
    ];
    let looks = vec![
        Look::Doc,
        Look::Members {
            after: Some(Value::Text("m".to_owned())),
            limit: 50,
        },
        Look::Members {
            after: None,
            limit: 1,
        },
        Look::Items { at: -5, limit: 3 },
        Look::ItemsAfter {
            token: Token {
                generation: zero(),
                version: 4,
                position: 9,
            },
            limit: 2,
        },
        Look::Range { offset: 8, len: 16 },
    ];
    let mut calls: Vec<ClientCall> = changes
        .into_iter()
        .enumerate()
        .map(|(n, change)| ClientCall::Put {
            identity: identity(3, n as u64 + 1),
            object: Bytes::from_static(b"obj"),
            change,
            expect: [None, Some(Expected::Absent), Some(Expected::Version(6))][n % 3],
            remaining_millis: 900,
        })
        .collect();
    calls.extend(looks.into_iter().map(|look| ClientCall::Get {
        identity: identity(3, 20),
        object: Bytes::from_static(b"obj"),
        look,
    }));
    calls.push(ClientCall::Compile {
        identity: identity(4, 30),
        puts: vec![PutOp {
            group: zero(),
            object: Bytes::from_static(b"a"),
            change: Change::Clear,
            expect: Some(Expected::Version(2)),
        }],
        remaining_millis: 1_000,
    });
    calls.push(ClientCall::Resend { request: zero() });
    calls.push(ClientCall::GetPrevious {
        identity: identity(4, 31),
        object: Bytes::from_static(b"b"),
    });
    calls.push(ClientCall::Status {
        identity: identity(4, 32),
        generation: Some(zero()),
    });
    calls
}

/// Every answer shape a node sends back: each `Seen`, and an error with a retry hint.
fn answers() -> Vec<Answer> {
    let seen = vec![
        Seen::Absent,
        Seen::Doc(Document {
            version: 3,
            value: Value::Text("v".to_owned()),
        }),
        Seen::Members(Members {
            collection: Collection {
                kind: CollectionKind::Map,
                version: 2,
                count: 1,
            },
            members: vec![Member {
                key: Value::Text("k".to_owned()),
                value: Some(int(1)),
                version: 2,
            }],
        }),
        Seen::Members(Members {
            collection: Collection {
                kind: CollectionKind::Set,
                version: 1,
                count: 1,
            },
            members: vec![Member {
                key: int(5),
                value: None,
                version: 1,
            }],
        }),
        Seen::Items(Items {
            list: List {
                version: 4,
                count: 2,
                bytes: 30,
                blocks: 1,
                records: true,
            },
            items: vec![Item {
                position: 1,
                id: u128::MAX,
                value: int(8),
                inline: false,
                version: 4,
            }],
            next: Some(Token {
                generation: zero(),
                version: 4,
                position: 2,
            }),
        }),
        Seen::Range {
            blob: Blob {
                version: 5,
                manifest: Manifest {
                    size: 10,
                    sha256: [1; 32],
                    upload: [2; 16],
                    chunk_size: 4,
                    chunk_sha256: vec![[3; 32], [4; 32]],
                },
            },
            bytes: Bytes::from_static(b"rang"),
        },
    ];
    let mut answers: Vec<Answer> = seen
        .into_iter()
        .map(|seen| Answer::Read {
            outcome: zero(),
            seen,
            generation: zero(),
            at: zero(),
            recovery_mode: true,
        })
        .collect();
    let mut error = ApiError::host("boom");
    error.retry_after = Some(Duration::from_millis(1_500));
    answers.extend([
        Answer::Txn {
            result: zero(),
            request: zero(),
        },
        Answer::Compiled(zero()),
        Answer::Unchanged,
        Answer::Status(zero()),
        Answer::Error {
            error,
            request: Some(zero()),
        },
    ]);
    answers
}

fn status() -> NodeStatus {
    NodeStatus {
        node: NodeId(2),
        boot: BootId(77),
        fault: Some("f".to_owned()),
        holds: Some(Holding {
            generation: zero(),
            owner_epoch: zero(),
            applied: 5,
            durable: 4,
        }),
        authority: "Serving".to_owned(),
        fence: None,
        fence_polling: true,
        replace_stopped: false,
        recovery: Some("Done".to_owned()),
        recovered: Some(zero()),
        role: "secondary",
        protection: None,
        admits: Some(true),
        published: Some((zero(), zero())),
        in_flight: Some(RequestId(3)),
        waits_on: None,
        stalled: Some("s".to_owned()),
        reload_pending: Some(2),
        lengths: None,
        pinned: vec![zero()],
        config_version: Some(zero()),
    }
}

/// M10 S2 W4 wire row: every peer message kind, every client call and every answer cross the
/// wire unchanged, and a process-local message is refused by name. Unit.
#[test]
fn every_message_kind_crosses_the_wire_unchanged() {
    let to = NodeId(3);
    let mut covered = BTreeSet::new();
    let label = rdb_core::contracts::transport::PeerLabel {
        node: NodeId(1),
        boot: BootId(5),
        authenticated: true,
    };
    let mut frame: rdb_core::contracts::transport::Frame = zero();
    frame.protocol = 3;
    frame.body = Bytes::from_static(b"record");
    let plain = vec![
        Msg::Frame {
            partition: PartitionId(1),
            correlation: zero(),
            from: label,
            frame,
        },
        Msg::PeerAsk {
            asker: NodeId(1),
            partition: PartitionId(1),
            correlation: zero(),
            ask: PeerAsk::Inventory {
                copy: CopyId(2),
                anchor: zero(),
            },
        },
        Msg::PeerAsk {
            asker: NodeId(2),
            partition: PartitionId(1),
            correlation: zero(),
            ask: PeerAsk::Sync {
                copy: CopyId(3),
                cutoff: zero(),
                generation: zero(),
                post_commit: true,
            },
        },
        Msg::CatchUp {
            asker: NodeId(1),
            partition: PartitionId(1),
            correlation: zero(),
            from: CopyId(1),
            to: CopyId(2),
            through: zero(),
            credential: zero(),
            seed: seed(),
        },
        Msg::Restore {
            partition: PartitionId(1),
            seed: seed(),
        },
        Msg::Routed {
            partition: PartitionId(1),
            correlation: zero(),
            kind: zero(),
        },
        Msg::Answered {
            partition: PartitionId(1),
            correlation: zero(),
            asked: zero(),
            kind: zero(),
        },
        Msg::Landed {
            result: Box::new(landed()),
            partition: PartitionId(1),
            correlation: zero(),
            emitter: NodeId(2),
            not_before: zero(),
        },
    ];
    for msg in plain {
        let before = format!("{msg:?}");
        match across(to, msg) {
            (Inbound::Plain(at, after), kind) => {
                assert_eq!(at, to);
                assert_eq!(format!("{after:?}"), before);
                covered.insert(kind);
            }
            (other, _) => panic!("{before} landed as {other:?}"),
        }
    }

    for call in calls() {
        let before = format!("{call:?}");
        let (reply, _rx) = mpsc::channel();
        let msg = Msg::Client(Client {
            partition: PartitionId(1),
            call,
            reply,
        });
        match across(to, msg) {
            (
                Inbound::Client {
                    to: at,
                    call,
                    reply,
                    ..
                },
                kind,
            ) => {
                assert_eq!((at, reply), (to, 7));
                assert_eq!(format!("{call:?}"), before);
                covered.insert(kind);
            }
            (other, _) => panic!("{before} landed as {other:?}"),
        }
    }

    let (reply, _rx) = mpsc::channel();
    match across(
        to,
        Msg::Inspect {
            partition: PartitionId(4),
            reply,
        },
    ) {
        (
            Inbound::Inspect {
                to: at,
                partition,
                reply,
            },
            kind,
        ) => {
            assert_eq!((at, partition, reply), (to, PartitionId(4), 8));
            covered.insert(kind);
        }
        (other, _) => panic!("inspect landed as {other:?}"),
    }
    assert_eq!(covered, (0..KINDS).collect(), "a message kind has no row");

    for answer in answers() {
        let before = format!("{answer:?}");
        let bytes = wire::encode(&Frame::Reply {
            correlation: 1,
            reply: WireReply::Answer(WireAnswer::carry(&answer)),
        });
        let Ok(Frame::Reply {
            reply: WireReply::Answer(back),
            ..
        }) = wire::decode(&bytes)
        else {
            panic!("{before}: not a reply");
        };
        assert_eq!(format!("{:?}", back.land()), before);
    }

    let status = status();
    let bytes = wire::encode(&Frame::Reply {
        correlation: 2,
        reply: WireReply::Status(wire::WireNodeStatus::from(&status)),
    });
    let Ok(Frame::Reply {
        reply: WireReply::Status(back),
        ..
    }) = wire::decode(&bytes)
    else {
        panic!("not a status reply");
    };
    let mut odd = back.clone();
    assert_eq!(
        format!("{:?}", NodeStatus::try_from(back).expect("lands")),
        format!("{status:?}")
    );
    odd.role = "leader".to_owned();
    assert!(
        NodeStatus::try_from(odd).is_err(),
        "an unknown role is refused"
    );

    for local in [
        Msg::Stop,
        Msg::Fault {
            boot: BootId(1),
            kind: "probe",
            detail: String::new(),
        },
    ] {
        let Err(refused) = wire::outbound(to, local) else {
            panic!("a local message went out");
        };
        let name = refused.0.to_string();
        assert!(
            name.contains("Stop") || name.contains("Fault"),
            "refused by name: {name}"
        );
    }
}

// ---- links ----------------------------------------------------------------------------------

/// One process in a link row: its links, its process links and its one node's mailbox.
struct Side {
    links: Arc<Links>,
    net: Arc<Net>,
    mail: Receiver<Msg>,
    /// The node's mailbox and boot, while a row holds its registration back.
    unregistered: Option<(NodeId, mpsc::Sender<Msg>, BootId)>,
}

impl Side {
    /// Register the node a row held back, as `Db::open` does once its copy is open.
    fn register(&mut self) {
        let (node, tx, boot) = self.unregistered.take().expect("held back");
        self.links.register(node, tx, boot);
    }
}

struct Spec<'a> {
    /// The node this side runs.
    node: u32,
    /// The label it claims; its node's unless a row lies.
    claim: Option<Label>,
    /// Where its certificate comes from.
    certs: &'a FsPath,
    /// Whose certificate it presents; its own unless a row lies.
    cert_of: Option<Label>,
    version: u32,
    dial: BTreeMap<NodeId, String>,
    peers: &'a [u32],
    /// Whether its node is registered before its links start.
    registered: bool,
}

impl<'a> Spec<'a> {
    fn node(node: u32, certs: &'a FsPath, peers: &'a [u32]) -> Self {
        Self {
            node,
            claim: None,
            certs,
            cert_of: None,
            version: PROTOCOL_VERSION,
            dial: BTreeMap::new(),
            peers,
            registered: true,
        }
    }

    /// Start the links before the node is registered, as `rdb_node` does: its `Db` opens after.
    fn unregistered(mut self) -> Self {
        self.registered = false;
        self
    }

    fn dialling(mut self, node: u32, addr: std::net::SocketAddr) -> Self {
        self.dial.insert(NodeId(node), addr.to_string());
        self
    }
}

async fn side(spec: Spec<'_>, listen: bool) -> Side {
    let own = Label::Node(NodeId(spec.node));
    let label = spec.claim.unwrap_or_else(|| own.clone());
    let tls = certs::load(spec.certs, spec.cert_of.as_ref().unwrap_or(&own)).expect("certs");
    let links = Links::new();
    let (tx, mail) = mpsc::channel();
    let boot = BootId(u64::from(spec.node) * 11);
    let unregistered = if spec.registered {
        links.register(NodeId(spec.node), tx, boot);
        None
    } else {
        Some((NodeId(spec.node), tx, boot))
    };
    let listener = if listen {
        Some(config_testkit::ports::ephemeral_listener().await.0)
    } else {
        None
    };
    let net = Net::start(
        NetConfig {
            label,
            boot: BootId(u64::from(spec.node) * 11),
            tls,
            peers: spec.peers.iter().map(|n| NodeId(*n)).collect(),
            dial: spec.dial,
            protocol_version: spec.version,
            faults: Faults::new(),
        },
        &links,
        listener,
    )
    .expect("net starts");
    Side {
        links,
        net,
        mail,
        unregistered,
    }
}

fn certs_for(dir: &FsPath, nodes: &[u32]) {
    let labels: Vec<Label> = nodes.iter().map(|n| Label::Node(NodeId(*n))).collect();
    certs::generate(dir, &labels).expect("certs generate");
}

const PATIENCE: Duration = Duration::from_secs(5);

/// Take the next message from `mail` on a blocking thread, so the runtime keeps serving the
/// link while a row waits.
async fn next(mail: Receiver<Msg>) -> (Msg, Receiver<Msg>) {
    tokio::task::spawn_blocking(move || {
        let msg = mail.recv_timeout(PATIENCE).expect("a message arrives");
        (msg, mail)
    })
    .await
    .expect("join")
}

/// M10 S2 W4 link row: node 2's process dials node 1's under mutual TLS. Each learns the
/// other's boot from the handshake. A peer message crosses both ways; a client call and an
/// `Inspect` are answered on the link that carried them; the node logs the call as
/// `net_client_request{n, client, request}`. Integration (~150 ms: real TLS on loopback).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_message_crosses_processes_and_a_call_is_answered_on_its_link() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2]);
    let one = side(Spec::node(1, dir.path(), &[2]), true).await;
    let addr = one.net.addr().expect("listening");
    let two = side(Spec::node(2, dir.path(), &[1]).dialling(1, addr), false).await;
    two.net
        .wait_up(&[NodeId(1)], PATIENCE)
        .await
        .expect("2 -> 1 up");
    one.net
        .wait_up(&[NodeId(2)], PATIENCE)
        .await
        .expect("1 sees 2");
    assert_eq!(one.links.boot_of(NodeId(2)), Some(BootId(22)));
    assert_eq!(two.links.boot_of(NodeId(1)), Some(BootId(11)));
    assert_eq!(
        one.links.boot_of(NodeId(1)),
        Some(BootId(11)),
        "a local node answers with the boot it registered (A-R232)"
    );
    assert_eq!(
        one.links.boot_of(NodeId(3)),
        None,
        "never registered, never linked"
    );

    let routed = |n: u32| Msg::Restore {
        partition: PartitionId(n),
        seed: seed(),
    };
    two.links
        .send(NodeId(2), NodeId(1), routed(1))
        .expect("sent");
    let (got, mail_one) = next(one.mail).await;
    assert_eq!(format!("{got:?}"), format!("{:?}", routed(1)), "2 -> 1");
    one.links
        .send(NodeId(1), NodeId(2), routed(2))
        .expect("sent back");
    let (got, _mail_two) = next(two.mail).await;
    assert_eq!(
        format!("{got:?}"),
        format!("{:?}", routed(2)),
        "1 -> 2 on the accepted link"
    );

    // Node 1 answers one client call and one Inspect, as its host would.
    let answered = Answer::Read {
        outcome: zero(),
        seen: Seen::Doc(Document {
            version: 1,
            value: Value::Text("hello".to_owned()),
        }),
        generation: zero(),
        at: zero(),
        recovery_mode: true,
    };
    let expected = format!("{answered:?}");
    let host = tokio::task::spawn_blocking(move || {
        for _ in 0..2 {
            match mail_one.recv_timeout(PATIENCE).expect("a call arrives") {
                Msg::Client(Client { call, reply, .. }) => {
                    assert!(matches!(call, ClientCall::Get { .. }), "{call:?}");
                    reply.send(answered.clone()).expect("caller waits");
                }
                Msg::Inspect { reply, .. } => reply.send(status()).expect("caller waits"),
                other => panic!("unexpected {other:?}"),
            }
        }
    });
    let (reply, answer) = mpsc::channel();
    two.links
        .post(
            NodeId(1),
            Msg::Client(Client {
                partition: PartitionId(1),
                call: ClientCall::Get {
                    identity: identity(42, 7),
                    object: Bytes::from_static(b"k"),
                    look: Look::Doc,
                },
                reply,
            }),
        )
        .expect("posted");
    let (inspect, inspected) = mpsc::channel();
    two.links
        .post(
            NodeId(1),
            Msg::Inspect {
                partition: PartitionId(1),
                reply: inspect,
            },
        )
        .expect("posted");
    let (answer, inspected) = tokio::task::spawn_blocking(move || {
        (
            answer.recv_timeout(PATIENCE).expect("answered"),
            inspected.recv_timeout(PATIENCE).expect("inspected"),
        )
    })
    .await
    .expect("join");
    host.await.expect("host");
    assert_eq!(format!("{answer:?}"), expected);
    assert_eq!(format!("{inspected:?}"), format!("{:?}", status()));

    let lines = config_testkit::logs::lines_for_current_test(
        module_path!(),
        "a_peer_message_crosses_processes_and_a_call_is_answered_on_its_link",
    );
    let calls: Vec<_> = lines
        .iter()
        .filter(|line| {
            line["@m"]
                .as_str()
                .is_some_and(|m| m.contains("net_client_request"))
        })
        .map(|line| {
            (
                line["n"].clone(),
                line["client"].clone(),
                line["request"].clone(),
            )
        })
        .collect();
    assert_eq!(
        calls,
        [(
            serde_json::json!(1),
            serde_json::json!(42),
            serde_json::json!(7)
        )],
        "call 1 maps to (client 42, request 7)"
    );
}

/// M10 S2 review-s2 F-015 row: a client call node 1's process cannot decode (a value that is
/// not CBOR) is answered at once with a named error, never left to the caller's timeout. The
/// error says nothing ran and a resend will not help; node 1's node never sees the call.
/// Cut: no reply on a decode failure → the caller hears nothing within `PATIENCE`.
/// Integration (~150 ms).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_the_far_process_cannot_decode_is_answered_with_a_named_error() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2]);
    let one = side(Spec::node(1, dir.path(), &[2]), true).await;
    let addr = one.net.addr().expect("listening");
    let two = side(Spec::node(2, dir.path(), &[1]).dialling(1, addr), false).await;
    two.net
        .wait_up(&[NodeId(1)], PATIENCE)
        .await
        .expect("2 -> 1 up");

    let (reply, answer) = mpsc::channel();
    two.net.send_wire_call(
        NodeId(1),
        PartitionId(1),
        wire::WireCall::Put {
            identity: identity(42, 9),
            object: Bytes::from_static(b"k"),
            change: wire::WireChange::Doc(vec![wire::WireOp::Replace(vec![0xff, 0xff])]),
            expect: None,
            remaining_millis: 1_000,
        },
        reply,
    );
    let started = std::time::Instant::now();
    let answer = tokio::task::spawn_blocking(move || answer.recv_timeout(PATIENCE))
        .await
        .expect("join")
        .expect("answered, not left to time out");
    let Answer::Error { error, request } = answer else {
        panic!("a named error: {answer:?}");
    };
    assert_eq!(
        (
            error.kind,
            error.retry,
            error.no_mutation,
            request.is_none()
        ),
        (ErrorKind::Unavailable, RetryRule::NotWired, true, true),
        "{error:?}"
    );
    assert!(
        error.detail.contains("node 1") && error.detail.contains("could not decode"),
        "{}",
        error.detail
    );
    assert!(started.elapsed() < PATIENCE, "{:?}", started.elapsed());
    assert!(
        one.mail.try_recv().is_err(),
        "node 1 never sees an undecodable call"
    );
}

/// M10 S2 review-s2 F-015 row, status half: a status reply that does not decode (a role this
/// build does not know) closes the link with the reason, so the next look fails fast and says
/// why instead of logging the same fault on every poll. The caller's channel closes at once.
/// Cut: log and go on (`handle` answers `Ok`, the link stays up). Integration (~150 ms).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_status_reply_that_does_not_decode_closes_the_link_with_the_reason() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2]);
    let one = side(Spec::node(1, dir.path(), &[2]), true).await;
    let addr = one.net.addr().expect("listening");
    let two = side(Spec::node(2, dir.path(), &[1]).dialling(1, addr), false).await;
    two.net
        .wait_up(&[NodeId(1)], PATIENCE)
        .await
        .expect("2 -> 1 up");

    let mut bad = wire::WireNodeStatus::from(&status());
    bad.role = "no such role".to_owned();
    let (handled, caller) = two.net.status_reply_from(NodeId(1), bad);
    let reason = handled.expect_err("the read loop closes the link");
    assert!(
        reason.contains("a status reply did not decode") && reason.contains("no such role"),
        "{reason}"
    );
    assert!(
        matches!(caller.try_recv(), Err(mpsc::TryRecvError::Disconnected)),
        "the caller's channel is closed"
    );
}

/// A frame for partition 1 that claims to come from `node`.
fn frame_from(node: u32) -> Msg {
    let mut frame: rdb_core::contracts::transport::Frame = zero();
    frame.protocol = 3;
    frame.body = Bytes::from_static(b"record");
    Msg::Frame {
        partition: PartitionId(1),
        correlation: zero(),
        from: rdb_core::contracts::transport::PeerLabel {
            node: NodeId(node),
            boot: BootId(u64::from(node) * 11),
            authenticated: true,
        },
        frame,
    }
}

/// An inventory ask for partition 1 that names `asker`.
fn ask_from(asker: u32) -> Msg {
    Msg::PeerAsk {
        asker: NodeId(asker),
        partition: PartitionId(1),
        correlation: zero(),
        ask: PeerAsk::Inventory {
            copy: CopyId(1),
            anchor: zero(),
        },
    }
}

/// The `net_msg_sender_refused` lines of `test`, as (peer, msg).
fn sender_refusals(test: &str) -> Vec<(String, String)> {
    config_testkit::logs::lines_for_current_test(module_path!(), test)
        .iter()
        .filter(|line| line["@m"].as_str() == Some("net_msg_sender_refused"))
        .map(|line| {
            (
                line["peer"].as_str().unwrap_or_default().to_owned(),
                line["msg"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

/// M10 S2 review-s2 F-008 row (a): node 2's process sends node 1 a frame and an ask that
/// name node 3 as their sender. Both are refused before node 1's node sees them, and logged
/// `net_msg_sender_refused`; the honest frame sent after them is the first thing node 1 gets.
/// Cut: no sender check in `receive` → node 1's first message is the forged frame.
/// Integration (~150 ms).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_naming_another_node_as_sender_is_refused() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2]);
    let one = side(Spec::node(1, dir.path(), &[2]), true).await;
    let addr = one.net.addr().expect("listening");
    let two = side(Spec::node(2, dir.path(), &[1]).dialling(1, addr), false).await;
    two.net
        .wait_up(&[NodeId(1)], PATIENCE)
        .await
        .expect("2 -> 1 up");

    for forged in [frame_from(3), ask_from(3), frame_from(2)] {
        two.links.send(NodeId(2), NodeId(1), forged).expect("sent");
    }
    let (got, _mail) = next(one.mail).await;
    assert_eq!(
        format!("{got:?}"),
        format!("{:?}", frame_from(2)),
        "the forged frame and ask never reach node 1"
    );
    assert_eq!(
        sender_refusals("a_message_naming_another_node_as_sender_is_refused"),
        [
            ("node-2".to_owned(), "frame".to_owned()),
            ("node-2".to_owned(), "peer_ask".to_owned())
        ]
    );
}

/// M10 S2 review-s2 F-008 row (b): an app's process sends node 1 a peer frame and an ask.
/// Neither reaches node 1's node; the `Inspect` sent after them is the first thing it gets,
/// and both refusals are logged. Cut: no app allow-list → node 1's first message is the frame.
/// Integration (~150 ms).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn an_app_link_never_delivers_a_peer_message() {
    let dir = config_testkit::fs::temp_dir();
    let app = Label::App("pilot".to_owned());
    certs::generate(dir.path(), &[Label::Node(NodeId(1)), app.clone()]).expect("certs");
    let one = side(Spec::node(1, dir.path(), &[]), true).await;
    let addr = one.net.addr().expect("listening");
    let pilot = Spec {
        claim: Some(app.clone()),
        cert_of: Some(app),
        ..Spec::node(9, dir.path(), &[1]).dialling(1, addr)
    };
    let pilot = side(pilot, false).await;
    pilot
        .net
        .wait_up(&[NodeId(1)], PATIENCE)
        .await
        .expect("app -> 1 up");

    for peer in [frame_from(1), ask_from(1)] {
        pilot.links.send(NodeId(9), NodeId(1), peer).expect("sent");
    }
    let (inspect, _inspected) = mpsc::channel();
    pilot
        .links
        .post(
            NodeId(1),
            Msg::Inspect {
                partition: PartitionId(1),
                reply: inspect,
            },
        )
        .expect("posted");
    let (got, _mail) = next(one.mail).await;
    assert!(
        matches!(got, Msg::Inspect { .. }),
        "an app's peer messages never reach node 1: {got:?}"
    );
    assert_eq!(
        sender_refusals("an_app_link_never_delivers_a_peer_message"),
        [
            ("app-pilot".to_owned(), "frame".to_owned()),
            ("app-pilot".to_owned(), "peer_ask".to_owned())
        ]
    );
}

/// M10 S2 review-s2 F-008 row: which kinds each label may send, checked on every kind. An app
/// may send a call, an `Inspect` and a promote's plan or fence, no other recovery event (C-3);
/// a node may not name another
/// node as sender, and kinds that name no sender pass. Unit.
#[test]
fn a_sender_is_held_to_its_certificate_label() {
    use rdb_core::contracts::event::KernelEvent;
    use rdb_core::contracts::recovery::RecoveryEvent;
    let to = NodeId(1);
    let on_wire = |msg: Msg| match wire::outbound(to, msg).map_err(|refused| refused.0.to_string())
    {
        Ok(Outbound::Plain(wire)) => wire,
        other => panic!("a plain message: {other:?}"),
    };
    let app = Label::App("pilot".to_owned());
    let node2 = Label::Node(NodeId(2));
    let client = WireMsg::Client {
        to,
        partition: PartitionId(1),
        call: wire::WireCall::Status {
            identity: identity(1, 1),
            generation: None,
        },
        reply: 1,
    };
    let inspect = WireMsg::Inspect {
        to,
        partition: PartitionId(1),
        reply: 2,
    };
    let routed = |event: RecoveryEvent| {
        on_wire(Msg::Routed {
            partition: PartitionId(1),
            correlation: zero(),
            kind: KernelEvent::Recovery(event),
        })
    };
    // A promote sends a plan and a fence (A-R415 C-3); an app may send no other recovery event.
    let plan = routed(RecoveryEvent::Plan(Box::new(crate::admin::recovery_plan(
        zero(),
        zero(),
        Vec::new(),
        zero(),
        zero(),
        zero(),
    ))));
    let fence = routed(RecoveryEvent::FenceProven(Box::new(zero())));
    let inventory_failed = routed(RecoveryEvent::InventoryFailed { copy: CopyId(1) });
    let probe = routed(RecoveryEvent::ProbeUnavailable {
        copy: CopyId(1),
        seq: zero(),
    });
    for allowed in [&client, &inspect, &plan, &fence] {
        assert_eq!(super::sender_refused(&app, allowed), None, "{allowed:?}");
        assert_eq!(super::sender_refused(&node2, allowed), None, "{allowed:?}");
    }
    let progress = on_wire(Msg::Routed {
        partition: PartitionId(1),
        correlation: zero(),
        kind: zero(),
    });
    assert!(
        matches!(
            &progress,
            WireMsg::Routed {
                kind: rdb_core::contracts::event::KernelEvent::PeerProgress { .. },
                ..
            }
        ),
        "{progress:?}"
    );
    for peer in [
        on_wire(frame_from(2)),
        on_wire(ask_from(2)),
        progress,
        inventory_failed,
        probe,
    ] {
        assert!(
            super::sender_refused(&app, &peer).is_some(),
            "app sends {peer:?}"
        );
        assert_eq!(super::sender_refused(&node2, &peer), None, "{peer:?}");
    }
    for forged in [on_wire(frame_from(3)), on_wire(ask_from(3))] {
        let reason = super::sender_refused(&node2, &forged).expect("forged");
        assert!(reason.contains("node 3"), "{reason}");
    }
}

/// The reason node 1's dialler holds for node `to`, once it is refused. Polls until the link is
/// refused or up, within 3 s scaled by `RETCD_TEST_DEADLINE_SCALE`: under load the handshake
/// alone took longer than a fixed 3 s (A-R257), and the row read `Waiting(None)` as a fault.
async fn refusal(side: &Side, to: u32) -> String {
    let patience = crate::host::test_patience(Duration::from_secs(3));
    let deadline = tokio::time::Instant::now() + patience;
    loop {
        match side.net.states().get(&NodeId(to)) {
            Some(LinkState::Refused(reason)) => return reason.clone(),
            Some(LinkState::Up(boot)) => panic!("the link must not come up: up at {boot:?}"),
            other => assert!(
                tokio::time::Instant::now() < deadline,
                "not refused within {patience:?}: {other:?}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// M10 S2 W4 handshake row (S4 F1): a process that presents node 3's certificate and claims
/// to be node 2 is refused, and the refusal names both. The label comes from the certificate,
/// never from the claim. Cut: trust the claim → the link comes up. Integration (~150 ms).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claimed_label_the_certificate_does_not_name_is_refused_naming_both() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2, 3]);
    let one = side(Spec::node(1, dir.path(), &[2, 3]), true).await;
    let addr = one.net.addr().expect("listening");
    let liar = Spec {
        claim: Some(Label::Node(NodeId(2))),
        cert_of: Some(Label::Node(NodeId(3))),
        ..Spec::node(2, dir.path(), &[1]).dialling(1, addr)
    };
    let liar = side(liar, false).await;
    let reason = refusal(&liar, 1).await;
    assert!(
        reason.contains("node-2") && reason.contains("node-3"),
        "names both: {reason}"
    );
    assert!(
        !matches!(one.net.states()[&NodeId(3)], LinkState::Up(_)),
        "no link as node 3 either"
    );
}

/// M10 S2 W4 handshake row: a process speaking a newer protocol_version is refused, and the
/// refusal names both sides and both versions. Cut: no version check → the link comes up.
/// Integration (~150 ms).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_newer_protocol_version_is_refused_naming_both_sides() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2]);
    let one = side(Spec::node(1, dir.path(), &[2]), true).await;
    let addr = one.net.addr().expect("listening");
    let newer = Spec {
        version: PROTOCOL_VERSION + 1,
        ..Spec::node(2, dir.path(), &[1]).dialling(1, addr)
    };
    let newer = side(newer, false).await;
    let reason = refusal(&newer, 1).await;
    let want = format!(
        "node-2 speaks protocol_version {}, node-1 speaks {PROTOCOL_VERSION}",
        PROTOCOL_VERSION + 1
    );
    assert_eq!(reason, want);
}

/// M10 S2 W4 handshake row (`--cert-dir other`): a certificate from another CA never gets a
/// link: the dialler is refused at once, naming the certificate (arch-s2 S2-1: "wrong cert
/// refused at handshake"; tester-s2 found it waited, then said "not reachable"). Integration.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_certificate_from_another_ca_gets_no_link() {
    let dir = config_testkit::fs::temp_dir();
    let ours = dir.path().join("ours");
    let other = dir.path().join("other");
    certs_for(&ours, &[1, 2]);
    certs_for(&other, &[1, 2]);
    let one = side(Spec::node(1, &ours, &[2]), true).await;
    let addr = one.net.addr().expect("listening");
    let stranger = side(Spec::node(2, &other, &[1]).dialling(1, addr), false).await;
    let error = stranger
        .net
        .wait_up(&[NodeId(1)], PATIENCE)
        .await
        .expect_err("no link");
    assert!(error.contains("node 1: refused"), "{error}");
    assert!(
        error.contains("certificate"),
        "names the certificate: {error}"
    );
    assert!(
        matches!(stranger.net.states()[&NodeId(1)], LinkState::Refused(_)),
        "a certificate the peer rejects does not heal by waiting"
    );
    assert!(!matches!(one.net.states()[&NodeId(2)], LinkState::Up(_)));
}

/// M10 S2, found walking `lr up`: node 3's link came up while its `Db` was still opening, so
/// node 1's recovery ask reached a process whose node could not take it yet and was dropped
/// (`net_deliver_unreachable`), and node 1 healed `DegradedRf2`. A node's link now comes up
/// only once its node is registered, so nothing sent on it arrives before the node can take it.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_link_comes_up_only_once_its_node_can_take_messages() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2]);
    let mut one = side(Spec::node(1, dir.path(), &[2]).unregistered(), true).await;
    let addr = one.net.addr().expect("listening");
    let two = side(Spec::node(2, dir.path(), &[1]).dialling(1, addr), false).await;
    let early = two
        .net
        .wait_up(&[NodeId(1)], Duration::from_millis(600))
        .await;
    assert!(
        early.is_err(),
        "node 1 cannot take messages yet, so its link is not up: {early:?}"
    );
    assert_eq!(two.links.boot_of(NodeId(1)), None, "no handshake yet");

    one.register();
    two.net
        .wait_up(&[NodeId(1)], PATIENCE)
        .await
        .expect("up once node 1 is registered");
    let ask = || Msg::Restore {
        partition: PartitionId(1),
        seed: seed(),
    };
    two.links.send(NodeId(2), NodeId(1), ask()).expect("sent");
    let (got, _mail) = next(one.mail).await;
    assert_eq!(
        format!("{got:?}"),
        format!("{:?}", ask()),
        "delivered, not dropped"
    );
}

/// The dialling half of the row above: node 2's process dials node 1 only once node 2 is
/// registered, so node 1 never holds a link to a node that cannot take its messages yet.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_dials_only_once_its_node_can_take_messages() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2]);
    let one = side(Spec::node(1, dir.path(), &[2]), true).await;
    let addr = one.net.addr().expect("listening");
    let mut two = side(
        Spec::node(2, dir.path(), &[1])
            .dialling(1, addr)
            .unregistered(),
        false,
    )
    .await;
    let early = one
        .net
        .wait_up(&[NodeId(2)], Duration::from_millis(600))
        .await;
    assert!(
        early.is_err(),
        "node 2 cannot take messages yet, so it has not dialled: {early:?}"
    );
    assert_eq!(one.links.boot_of(NodeId(2)), None, "no handshake yet");

    two.register();
    one.net
        .wait_up(&[NodeId(2)], PATIENCE)
        .await
        .expect("up once node 2 is registered");
    let ask = || Msg::Restore {
        partition: PartitionId(1),
        seed: seed(),
    };
    one.links.send(NodeId(1), NodeId(2), ask()).expect("sent");
    let (got, _mail) = next(two.mail).await;
    assert_eq!(
        format!("{got:?}"),
        format!("{:?}", ask()),
        "delivered, not dropped"
    );
}

/// tester-s2 S2-1 F: a handshake that cannot succeed is refused at once, saying why, instead of
/// waiting out `--ready-secs` as "not reachable". Node 1 dialled at node 3's address meets a
/// certificate for another name; an address held by something that does not speak TLS answers
/// garbage. Both name the address and ask whether it is that node of this run.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handshake_that_cannot_succeed_is_refused_naming_the_address() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2, 3]);
    let three = side(Spec::node(3, dir.path(), &[2]), true).await;
    let wrong = three.net.addr().expect("listening");
    let (plain, foreign) = config_testkit::ports::ephemeral_listener().await;
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        // Held open after the answer: dropping a socket with the hello unread resets it on
        // Windows (os error 10053), and the dialler may see the reset before the garbage.
        let mut held = Vec::new();
        while let Ok((mut socket, _)) = plain.accept().await {
            let _ = socket.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
            held.push(socket);
        }
    });
    for (addr, names) in [(wrong, "certificate"), (foreign, "InvalidContentType")] {
        let two = side(Spec::node(2, dir.path(), &[1]).dialling(1, addr), false).await;
        let error = two
            .net
            .wait_up(&[NodeId(1)], PATIENCE)
            .await
            .expect_err("no link");
        assert!(error.contains("node 1: refused"), "{error}");
        assert!(error.contains(names), "says why ({names}): {error}");
        assert!(
            error.contains(&addr.to_string()) && error.contains("node-1 of this run"),
            "names the address and what was expected there: {error}"
        );
    }
    // tester-s2 paper cut: the cause was said twice, `X: X`. Cut: `error_chain` appends every
    // cause again and this goes red.
    let two = side(Spec::node(2, dir.path(), &[1]).dialling(1, foreign), false).await;
    let error = two
        .net
        .wait_up(&[NodeId(1)], PATIENCE)
        .await
        .expect_err("no link");
    assert_eq!(
        error.matches("InvalidContentType").count(),
        1,
        "the cause once: {error}"
    );
}

/// tester-s2 F1c: a port nothing listens on said `connect: transport error: tcp connect error:
/// tcp connect error: ...`, with no address and no node. It now says nothing is listening
/// there, naming the address and the node, once. It is still redialled until the caller's wait
/// ends (A-R273). Integration (~4 s on Windows: the OS answers a closed port late).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn a_port_nothing_listens_on_says_so_naming_the_address_and_node() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2]);
    let (listener, addr) = config_testkit::ports::ephemeral_listener().await;
    drop(listener);
    let two = side(Spec::node(2, dir.path(), &[1]).dialling(1, addr), false).await;
    // The first dial's error lands after ~2-5 s on Windows (a closed loopback port answers
    // only after SYN retries), so wait for it rather than for a fixed time.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let error = loop {
        if let Some(LinkState::Waiting(Some(error))) = two.net.states().get(&NodeId(1)) {
            break error.clone();
        }
        assert!(tokio::time::Instant::now() < deadline, "no dial error");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        error.contains(&format!(
            "nothing is listening at {addr} (connection refused); is node-1 of this run"
        )),
        "{error}"
    );
    assert!(!error.contains("tcp connect error"), "{error}");
}

/// Walking the row above by hand: an address whose listener accepts and never answers (rEtcd's
/// plain HTTP health port) left the dial waiting forever, so the pilot waited out `--ready-secs`
/// and said only "not reachable yet". A connect now ends after `CONNECT`, naming the address.
/// Not a refusal: a slow node may answer next time. Integration (~2 s, the connect bound).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn an_address_that_never_answers_the_tls_hello_says_so() {
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2]);
    let (silent, addr) = config_testkit::ports::ephemeral_listener().await;
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = silent.accept().await {
            held.push(socket);
        }
    });
    let two = side(Spec::node(2, dir.path(), &[1]).dialling(1, addr), false).await;
    let error = two
        .net
        .wait_up(&[NodeId(1)], Duration::from_secs(4))
        .await
        .expect_err("no link");
    assert!(
        error.contains("node 1: not reachable: ")
            && error.contains("no TLS answer")
            && error.contains(&addr.to_string()),
        "{error}"
    );
}

/// A formation config on `dir`: all three nodes, no links, as `rdb_node --form` opens.
fn db_config(dir: &FsPath) -> crate::db::DbConfig {
    crate::db::DbConfig {
        ack_floor: true,
        dir: dir.to_path_buf(),
        hold: Vec::new(),
        timeouts: crate::Timeouts::default(),
        dedup_cap: None,
        dedup_retention: None,
        local: crate::db::ALL_NODES.to_vec(),
        links: None,
        client: crate::db::DEFAULT_CLIENT,
        boot: None,
        storage: crate::StorageOptions::default(),
    }
}

/// One node process of a link row: its links, its `Db` and its boot.
struct NodeProcess {
    net: Arc<Net>,
    db: crate::db::Db,
    boot: BootId,
}

/// Start node `node`'s process on the formed folder `dir`, as `rdb_node` does: the links first
/// (serving on `listener`, dialling the lower nodes in `addrs`), then its `Db` on its own copy at
/// `next_boot(dir)`.
fn node_process(
    rt: &tokio::runtime::Runtime,
    dir: &FsPath,
    certs_dir: &FsPath,
    store: &Arc<dyn config_core::ConfigStore>,
    node: u32,
    listener: tokio::net::TcpListener,
    addrs: &BTreeMap<NodeId, String>,
) -> NodeProcess {
    let me = NodeId(node);
    let boot = crate::db::next_boot(dir).expect("next boot");
    let links = Links::new();
    let net = {
        let _entered = rt.enter();
        Net::start(
            NetConfig {
                label: Label::Node(me),
                boot,
                tls: certs::load(certs_dir, &Label::Node(me)).expect("certs"),
                peers: crate::db::ALL_NODES
                    .iter()
                    .copied()
                    .filter(|n| *n != me)
                    .collect(),
                dial: NetConfig::dial_lower(me, addrs),
                protocol_version: PROTOCOL_VERSION,
                faults: Faults::new(),
            },
            &links,
            Some(listener),
        )
        .expect("net starts")
    };
    let config = crate::db::DbConfig {
        local: vec![me],
        links: Some(links),
        boot: Some(boot),
        ..db_config(dir)
    };
    let db = rt
        .block_on(crate::db::Db::open(
            config,
            Arc::clone(store),
            rt.handle().clone(),
        ))
        .unwrap_or_else(|e| panic!("node {node}'s process opens: {e}"));
    NodeProcess { net, db, boot }
}

/// #42 caller (rulings A-R251, A-R270), on tester-s2's sequence at 3b676c2: three node
/// processes, each with its own links under mutual TLS. Node 3's process stops; writes go on
/// through node 1 until the owner pauses them for the unheard copy. Node 3's process starts
/// again at a new boot. Node 1's links see a known peer back at another boot, log
/// `peer_rebooted` and post `PeerBooted` to node 1, whose host logs `peer_booted` and, as the
/// record owner whose plan pinned node 3's old boot, re-plans (`rejoin_revoke` ..
/// `rejoin_committed`). Writes then resume, and every write reads back. Cut: no post, so node 1
/// never hears of it and stays paused.
/// Integration (~15 s): a formation, three node opens until ready, the owner's pause, one
/// process restart and the re-plan, real TLS on loopback.
#[config_log::retcd_test]
fn a_restarted_peer_process_is_posted_to_the_owner_as_peer_booted() {
    const METHOD: &str = "a_restarted_peer_process_is_posted_to_the_owner_as_peer_booted";
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let dir = config_testkit::fs::temp_dir();
    let certs_dir = dir.path().join("certs");
    certs_for(&certs_dir, &[1, 2, 3]);
    let data = dir.path().join("db");
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let patience = crate::host::test_patience(Duration::from_secs(30));
    {
        let mut formed = rt
            .block_on(crate::db::Db::open(
                db_config(&data),
                Arc::clone(&store),
                rt.handle().clone(),
            ))
            .expect("formation opens");
        formed.wait_ready(patience).expect("formation is ready");
        formed.shutdown();
    }

    let mut listeners: BTreeMap<NodeId, tokio::net::TcpListener> = BTreeMap::new();
    let mut addrs = BTreeMap::new();
    for node in crate::db::ALL_NODES {
        let (listener, addr) = rt.block_on(config_testkit::ports::ephemeral_listener());
        listeners.insert(node, listener);
        addrs.insert(node, addr.to_string());
    }
    let take = |listeners: &mut BTreeMap<NodeId, tokio::net::TcpListener>, n: u32| {
        listeners.remove(&NodeId(n)).expect("a listener")
    };
    let start = |node: u32, listener: tokio::net::TcpListener| {
        node_process(&rt, &data, &certs_dir, &store, node, listener, &addrs)
    };
    // The copies open before the owner heals, so its plan finds them up (A-R165).
    let mut two = start(2, take(&mut listeners, 2));
    let three = start(3, take(&mut listeners, 3));
    let mut one = start(1, take(&mut listeners, 1));
    one.db
        .wait_ready(patience)
        .expect("node 1's process heals and is ready");
    let old = three.boot;
    assert_eq!(one.net.boot_of(NodeId(3)), Some(old), "node 1 links node 3");
    one.db
        .put(b"before", b"1", None)
        .expect("a put with every copy up");

    // Node 3's process stops and starts again at a new boot. Nothing dials node 3 (the higher
    // id dials), so it may listen on a fresh port: its old one can still be held for a moment.
    let NodeProcess {
        net: three_net,
        db: mut three_db,
        ..
    } = three;
    // A-R388: the owner's pause clock starts at `before`, which node 3 stops too soon to make
    // durable, so the away writes have one pause budget (~2 s) from that put. The teardown
    // after node 3's node thread stops (its RocksDB close, the join, its links) took 2.8 s
    // beside load and spent all of it. So the teardown runs aside, and the writes start once
    // node 3's node thread has stopped: node 3 hears nothing from then on.
    let teardown = std::thread::spawn(move || {
        three_db.shutdown();
        drop(three_db);
        drop(three_net);
    });
    let stopped_by = std::time::Instant::now() + patience;
    loop {
        config_log::layer::flush_test_logs();
        let stopped = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .iter()
            .any(|line| line["@m"] == "node_stopped" && line["node"] == 3 && line["boot"] == old.0);
        if stopped {
            break;
        }
        assert!(
            std::time::Instant::now() < stopped_by,
            "node 3's node thread did not stop within {patience:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    // Writes go on with node 3 away until the owner pauses them for the unheard copy (B-R38).
    let mut landed = 0u32;
    let paused_by = std::time::Instant::now() + patience;
    loop {
        match one.db.put(format!("away-{landed}").as_bytes(), b"1", None) {
            Ok(_) => landed += 1,
            Err(e) if e.error.name() == "PROTECTION_PAUSED" => break,
            Err(e) => panic!("a put with node 3 away, after {landed} landed: {e}"),
        }
        assert!(
            std::time::Instant::now() < paused_by,
            "writes never paused with node 3 away: {landed} landed"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(landed >= 1, "no put landed with node 3 away");
    teardown.join().expect("node 3's process stops");
    let (relisten, _) = rt.block_on(config_testkit::ports::ephemeral_listener());
    let mut again = start(3, relisten);
    assert!(
        again.boot > old,
        "a new boot: {:?} after {old:?}",
        again.boot
    );

    let deadline = std::time::Instant::now() + patience;
    let wanted = |line: &serde_json::Value| {
        let m = line["@m"].as_str().unwrap_or_default();
        (m == "peer_rebooted" && line["node"] == 3 && line["told"] == "[1]")
            || (m == "peer_booted" && line["node"] == 1 && line["peer"] == 3)
            || (m == "rejoin_revoke" && line["node"] == 1)
            || (m == "rejoin_committed" && line["node"] == 1)
    };
    let seen = loop {
        config_log::layer::flush_test_logs();
        let lines: Vec<serde_json::Value> =
            config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
                .into_iter()
                .filter(|line| wanted(line))
                .collect();
        let has = |m: &str| lines.iter().any(|line| line["@m"] == m);
        if [
            "peer_rebooted",
            "peer_booted",
            "rejoin_revoke",
            "rejoin_committed",
        ]
        .iter()
        .all(|m| has(m))
        {
            break lines;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node 1 did not hear node 3's new boot within {patience:?}: {lines:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    let rebooted = seen
        .iter()
        .find(|line| line["@m"] == "peer_rebooted")
        .expect("peer_rebooted");
    assert_eq!(rebooted["old"], old.0, "{rebooted}");
    assert_eq!(rebooted["boot"], again.boot.0, "{rebooted}");

    // Node 3 rejoined: writes resume on every copy, and nothing written before is lost.
    let resumed_by = std::time::Instant::now() + patience;
    loop {
        match one.db.put(b"after", b"2", None) {
            Ok(_) => break,
            Err(e)
                if e.error.name() == "PROTECTION_PAUSED"
                    && std::time::Instant::now() < resumed_by =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("writes did not resume after node 3 rejoined: {e}"),
        }
    }
    // A rejoin, not a give-up (A-R278): no catch-up was refused and no re-plan abandoned.
    config_log::layer::flush_test_logs();
    let gave_up: Vec<serde_json::Value> =
        config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "rejoin_abandoned" || line["@m"] == "catch_up_refused")
            .collect();
    assert!(gave_up.is_empty(), "{gave_up:?}");
    let last = format!("away-{}", landed - 1);
    for key in ["before", "away-0", last.as_str(), "after"] {
        let got = one
            .db
            .get(key.as_bytes())
            .unwrap_or_else(|e| panic!("read {key}: {e}"));
        assert!(got.value.is_some(), "{key} reads back");
    }

    again.db.shutdown();
    one.db.shutdown();
    two.db.shutdown();
}

/// A-R313, the hang in the row above (L5-3, killed at 900 s): node 2 dialled node 1 while node
/// 1's `Db` was still opening, gave up after `HANDSHAKE` and dialled again, so node 1 held two
/// handshakes from node 2 waiting on `serving`. Both went on once node 1 registered, and the
/// second link replaced the first while it was still live. `install` closed the old link while
/// it held the routes lock, `close` locked it again on the same thread, and that runtime worker
/// never came back: the reopen's heal waited on `boot_of` behind it, `wait_ready` timed out,
/// and the runtime's drop waited for the worker for ever. Now the old link is closed after the
/// lock is let go: the new link is the route, the old one is gone with its waiters, and the
/// routes answer. Cut: close under the lock, and the install never returns (red at `PATIENCE`,
/// not a hang). Unit (~1 ms): no sockets, no TLS.
#[config_log::retcd_test]
fn a_link_that_replaces_a_live_one_comes_up_and_the_routes_still_answer() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let shared = Arc::new(super::Shared {
        label: Label::Node(NodeId(1)),
        boot: BootId(11),
        protocol_version: PROTOCOL_VERSION,
        faults: Faults::new(),
        links: std::sync::Weak::new(),
        routes: std::sync::Mutex::new(BTreeMap::from([(
            NodeId(2),
            super::Route {
                conn: None,
                queue: std::collections::VecDeque::new(),
                boot: None,
                refused: None,
                last_error: None,
            },
        )])),
        conns: std::sync::Mutex::new(BTreeMap::new()),
        next_conn: std::sync::atomic::AtomicU64::new(0),
        rt: rt.handle().clone(),
        span: tracing::Span::current(),
        on_boot: std::sync::Mutex::new(None),
    });
    let boot = BootId(22);
    let (first_tx, _first_out) = tokio::sync::mpsc::unbounded_channel();
    let first = super::new_conn(&shared, Label::Node(NodeId(2)), first_tx);
    super::install(&shared, NodeId(2), &first, boot);
    let (waiter, answer) = mpsc::channel();
    super::lock(&first.pending).insert(1, super::Waiter::Status(waiter));

    // The second handshake, still live beside the first. On its own thread, so a lock that is
    // never let go fails the row at `PATIENCE` instead of hanging it.
    let (second_tx, _second_out) = tokio::sync::mpsc::unbounded_channel();
    let second = super::new_conn(&shared, Label::Node(NodeId(2)), second_tx);
    let (done, installed) = mpsc::channel();
    {
        let shared = Arc::clone(&shared);
        let second = Arc::clone(&second);
        let span = tracing::Span::current();
        std::thread::spawn(move || {
            let _entered = span.enter();
            super::install(&shared, NodeId(2), &second, boot);
            let routed = super::lock(&shared.routes)[&NodeId(2)]
                .conn
                .as_ref()
                .map(|conn| conn.id);
            let _ = done.send(routed);
        });
    }
    let routed = installed.recv_timeout(PATIENCE).unwrap_or_else(|_| {
        panic!("replacing a live link did not return within {PATIENCE:?}: the routes lock is held")
    });
    assert_eq!(routed, Some(second.id), "the newer link is node 2's route");
    let conns: Vec<u64> = super::lock(&shared.conns).keys().copied().collect();
    assert_eq!(conns, vec![second.id], "the replaced link is gone");
    assert!(
        matches!(
            answer.recv_timeout(Duration::ZERO),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ),
        "a call waiting on the replaced link is let go, not left waiting"
    );
}

/// A handshake from node 2's certificate that this row holds open, as a dialler that has not
/// given up yet would; dropping both halves abandons it, as a dialler past `HANDSHAKE` has.
async fn handshake_as_node_2(
    certs: &FsPath,
    addr: std::net::SocketAddr,
) -> (
    tokio::sync::mpsc::UnboundedSender<super::pb::Envelope>,
    tonic::Streaming<super::pb::Envelope>,
) {
    let tls = certs::load(certs, &Label::Node(NodeId(2))).expect("certs");
    let channel = tonic::transport::Endpoint::from_shared(format!("https://{addr}"))
        .expect("endpoint")
        .tls_config(tls.client_tls_config_for(&certs::node_domain(NodeId(1))))
        .expect("tls")
        .connect()
        .await
        .expect("connects");
    let mut client = super::pb::rdb_net_client::RdbNetClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let hello = Frame::Hello(wire::Hello {
        protocol_version: PROTOCOL_VERSION,
        label: Label::Node(NodeId(2)),
        boot: BootId(22),
    });
    tx.send(super::envelope(&hello)).expect("hello");
    let inbound = client
        .link(tonic::Request::new(
            tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
        ))
        .await
        .expect("link")
        .into_inner();
    (tx, inbound)
}

/// The id of the link node `net` routes node 2 by, read on a thread of its own so a lock that
/// is never let go fails the row instead of hanging it.
fn route_to_2(net: &Arc<Net>) -> Option<u64> {
    let net = Arc::clone(net);
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let id = super::lock(&net.shared.routes)[&NodeId(2)]
            .conn
            .as_ref()
            .map(|conn| conn.id);
        let _ = tx.send(id);
    });
    rx.recv_timeout(PATIENCE)
        .expect("the routes answer within PATIENCE")
}

/// Poll `route_to_2(net)` until `wanted` holds or `PATIENCE` passes.
fn until_route(net: &Arc<Net>, what: &str, wanted: impl Fn(Option<u64>) -> bool) -> Option<u64> {
    let deadline = std::time::Instant::now() + PATIENCE;
    loop {
        let id = route_to_2(net);
        if wanted(id) {
            return id;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what} within {PATIENCE:?}: node 1 routes node 2 by {id:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A-R320, both install orders of an abandoned handshake beside node 2's live dial (see
/// [`a_link_that_replaces_a_live_one_comes_up_and_the_routes_still_answer`]). Whichever installs
/// second, nodes 1 and 2 end with one live link and a route both ways: a message crosses each
/// way within `PATIENCE`.
fn an_abandoned_handshake_beside_a_live_link_leaves_routes_both_ways(abandoned_first: bool) {
    let runtime = Bounded(Some(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime"),
    ));
    let rt = runtime.0.as_ref().expect("runtime");
    let dir = config_testkit::fs::temp_dir();
    certs_for(dir.path(), &[1, 2]);
    let one = rt.block_on(side(Spec::node(1, dir.path(), &[2]), true));
    let addr = one.net.addr().expect("listening");
    let dial_2 = || {
        rt.block_on(side(
            Spec::node(2, dir.path(), &[1]).dialling(1, addr),
            false,
        ))
    };
    let two = if abandoned_first {
        let abandoned = rt.block_on(handshake_as_node_2(dir.path(), addr));
        let first = until_route(&one.net, "the held handshake installs", |id| id.is_some());
        let two = dial_2();
        until_route(&one.net, "node 2's dial replaces it", |id| {
            id.is_some_and(|id| id != first.expect("installed"))
        });
        drop(abandoned);
        two
    } else {
        let two = dial_2();
        let live = until_route(&one.net, "node 2's dial installs", |id| id.is_some());
        let abandoned = rt.block_on(handshake_as_node_2(dir.path(), addr));
        // Node 1 numbers links as it accepts them, and the held handshake is the only one it
        // accepts while node 2's live link is up, so it takes the next id. Node 2 dials again
        // about 100 ms after its link is replaced, so a slow poll can first see that redial.
        // Waiting for the first id seen after `live` to go away then waits on the good link for
        // ever (A-R356: 5 of 100 under load, `routes node 2 by Some(3)`).
        let abandoned_id = live.expect("installed") + 1;
        until_route(&one.net, "the held handshake replaces it", |id| id != live);
        drop(abandoned);
        until_route(&one.net, "node 1 has a live route to node 2 again", |id| {
            id.is_some_and(|id| id != abandoned_id)
        });
        two
    };

    let ask = || Msg::Restore {
        partition: PartitionId(1),
        seed: seed(),
    };
    one.links.send(NodeId(1), NodeId(2), ask()).expect("sent");
    let got = two
        .mail
        .recv_timeout(PATIENCE)
        .expect("node 1 reaches node 2: it has a route to node 2 again");
    assert_eq!(format!("{got:?}"), format!("{:?}", ask()));
    two.links.send(NodeId(2), NodeId(1), ask()).expect("sent");
    let got = one
        .mail
        .recv_timeout(PATIENCE)
        .expect("node 2 reaches node 1");
    assert_eq!(format!("{got:?}"), format!("{:?}", ask()));
}

/// A runtime that, dropped, waits at most a second for its workers: one a cut leaves stuck on
/// a lock must fail the row, not hang it, panicking or not.
struct Bounded(Option<tokio::runtime::Runtime>);

impl Drop for Bounded {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_timeout(Duration::from_secs(1));
        }
    }
}

/// The held handshake installs first and node 2's dial replaces it. Cut: close a replaced link
/// under the routes lock (before 27620de), and the routes never answer again.
/// Integration (~0.1 s: real TLS on loopback).
#[config_log::retcd_test]
fn an_abandoned_handshake_installed_first_leaves_routes_both_ways() {
    an_abandoned_handshake_beside_a_live_link_leaves_routes_both_ways(true);
}

/// L5-3's order: the abandoned handshake installs second and replaces node 2's live link.
/// Closing the live link ends its stream, so node 2 dials again and node 1 gets a route back.
/// Cut: close only forgets the link, node 2 still holds it up and never dials, and node 1's
/// message to node 2 waits for ever. Integration (~0.1 s: real TLS on loopback).
#[config_log::retcd_test]
fn an_abandoned_handshake_installed_second_leaves_routes_both_ways() {
    an_abandoned_handshake_beside_a_live_link_leaves_routes_both_ways(false);
}
