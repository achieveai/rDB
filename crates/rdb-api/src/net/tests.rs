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
    }
}
const KINDS: usize = 8;

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

/// The reason node 1's dialler holds for node `to`, once it settles.
async fn refusal(side: &Side, to: u32) -> String {
    let error = side
        .net
        .wait_up(&[NodeId(to)], Duration::from_secs(3))
        .await
        .expect_err("the link must not come up");
    match side.net.states().get(&NodeId(to)) {
        Some(LinkState::Refused(reason)) => reason.clone(),
        other => panic!("not refused: {other:?} ({error})"),
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
