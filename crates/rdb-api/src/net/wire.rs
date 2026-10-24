//! What crosses the wire between rDB processes (M10 S2, arch-s2 "Network (W4)").
//!
//! [`WireMsg`] mirrors the [`Msg`] variants that travel between processes: `Frame`, `PeerAsk`,
//! `CatchUp`, `Restore`, `Routed`, `Landed`, `Client` and `Inspect`. The rest (`Control`,
//! `Fault`, `Activated`, `Inject`, `Stop`) are local to one process and are refused here by
//! name. The kernel's contract types already derive serde; the host's own types (a client call,
//! its answer, a node's status) and `rdb-value`'s do not, so they get a mirror here. A value is
//! carried as its canonical CBOR, through `rdb_value::cbor`, the one codec a stored value
//! already goes through.
//!
//! A reply `Sender` cannot cross a process. `Client` and `Inspect` carry a correlation instead,
//! minted by the sending side's connection; the answer comes back as a [`Frame::Reply`] on the
//! same connection and is handed to the `Sender` that correlation names.
//!
//! Every frame is postcard bytes. The schema is this file, versioned by
//! [`super::PROTOCOL_VERSION`]: change a type here and the version moves.

use std::time::Duration;

use bytes::Bytes;
use rdb_core::contracts::authority::{DenyReason, FenceCredential, Lineage};
use rdb_core::contracts::errors::{ErrorKind, RetryRule};
use rdb_core::contracts::event::KernelEvent;
use rdb_core::contracts::ids::{
    AffinityId, BootId, CorrelationId, Generation, NodeId, OwnerEpoch, PartitionId, RequestId,
    RequestIdentity, Revision, Seq,
};
use rdb_core::contracts::membership::{CopyId, PartitionConfig};
use rdb_core::contracts::recovery::{LineageAnchor, RecoveryResult};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::ReadServiceOutcome;
use rdb_core::contracts::transport::{Frame as PeerFrame, PeerLabel};
use rdb_core::contracts::txn::{TxnRequest, TxnResult, TxnStatus};
use rdb_value::blob::{Blob, Manifest, Upload};
use rdb_value::collection::{Collection, CollectionKind, ElemOp, Member, Members};
use rdb_value::delta::{Delta, Op};
use rdb_value::list::{Item, Items, List, Token};
use rdb_value::path::Path;
use rdb_value::value::{Int, Value};
use rdb_value::{cbor, Document, Expected};
use serde::{Deserialize, Serialize};

use crate::db::ApiError;
use crate::host::{
    Answer, Change, ClientCall, DormantSeed, Holding, ListEdit, Look, Msg, NodeStatus, PeerAsk,
    PutOp, Seen,
};

/// Who is at the other end of a connection, as its certificate names it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Label {
    /// rDB node `n`.
    Node(NodeId),
    /// An app that holds no copy: it only makes client calls.
    App(String),
}

impl std::fmt::Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Node(node) => write!(f, "node-{}", node.0),
            Self::App(name) => write!(f, "app-{name}"),
        }
    }
}

/// The first frame each way.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// The wire version the sender speaks.
    pub protocol_version: u32,
    /// Who the sender says it is. Checked against its certificate; never trusted alone.
    pub label: Label,
    /// The sender's boot. An app sends `BootId(0)`.
    pub boot: BootId,
}

/// One frame of a link.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Frame {
    /// The dialler's handshake.
    Hello(Hello),
    /// The listener's answer to an accepted [`Frame::Hello`].
    Welcome(Hello),
    /// The handshake was refused, and why. The link closes after it.
    Refused {
        /// Names both sides' versions or labels.
        reason: String,
    },
    /// A message for a node in the receiving process.
    Msg(WireMsg),
    /// The answer to a `Client` or `Inspect` this side sent on this connection.
    Reply {
        /// The correlation the call carried.
        correlation: u64,
        /// The answer.
        reply: WireReply,
    },
}

/// The answer to a proxied call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WireReply {
    /// A client call's answer.
    Answer(WireAnswer),
    /// An `Inspect`'s answer.
    Status(WireNodeStatus),
    /// The node dropped the call's waiter unanswered. The caller sees its channel close, as it
    /// would in one process.
    Dropped,
}

/// A [`Msg`] that travels between processes, addressed to node `to`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WireMsg {
    Frame {
        to: NodeId,
        partition: PartitionId,
        correlation: CorrelationId,
        from: PeerLabel,
        frame: PeerFrame,
    },
    PeerAsk {
        to: NodeId,
        asker: NodeId,
        partition: PartitionId,
        correlation: CorrelationId,
        ask: WirePeerAsk,
    },
    CatchUp {
        to: NodeId,
        asker: NodeId,
        partition: PartitionId,
        correlation: CorrelationId,
        source: CopyId,
        target: CopyId,
        through: Seq,
        credential: FenceCredential,
        seed: WireSeed,
    },
    Restore {
        to: NodeId,
        partition: PartitionId,
        seed: WireSeed,
    },
    Routed {
        to: NodeId,
        partition: PartitionId,
        correlation: CorrelationId,
        kind: KernelEvent,
    },
    Landed {
        to: NodeId,
        result: Box<RecoveryResult>,
        partition: PartitionId,
        correlation: CorrelationId,
        emitter: NodeId,
        not_before: Tick,
    },
    Client {
        to: NodeId,
        partition: PartitionId,
        call: WireCall,
        /// The sending connection's correlation for the answer.
        reply: u64,
    },
    Inspect {
        to: NodeId,
        partition: PartitionId,
        /// The sending connection's correlation for the answer.
        reply: u64,
    },
}

impl WireMsg {
    /// The node the message is for.
    #[must_use]
    pub const fn to(&self) -> NodeId {
        match self {
            Self::Frame { to, .. }
            | Self::PeerAsk { to, .. }
            | Self::CatchUp { to, .. }
            | Self::Restore { to, .. }
            | Self::Routed { to, .. }
            | Self::Landed { to, .. }
            | Self::Client { to, .. }
            | Self::Inspect { to, .. } => *to,
        }
    }

    /// The variant's name, for logs.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Frame { .. } => "frame",
            Self::PeerAsk { .. } => "peer_ask",
            Self::CatchUp { .. } => "catch_up",
            Self::Restore { .. } => "restore",
            Self::Routed { .. } => "routed",
            Self::Landed { .. } => "landed",
            Self::Client { .. } => "client",
            Self::Inspect { .. } => "inspect",
        }
    }
}

/// Why a [`Msg`] cannot go on the wire.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    /// The variant is local to one process.
    #[error("{0} is local to one process and never crosses the wire")]
    LocalOnly(&'static str),
    /// A value in it has no canonical CBOR form.
    #[error("a value cannot be carried: {0}")]
    Value(String),
    /// A frame did not decode.
    #[error("a frame did not decode: {0}")]
    Decode(String),
}

/// What a [`Msg`] becomes on the wire, and what stays behind with the sender.
#[derive(Debug)]
pub enum Outbound {
    /// A message with no answer.
    Plain(WireMsg),
    /// A client call. The `Sender` waits for the answer under the correlation the connection
    /// mints; `with_reply` builds the message once it has one.
    Client {
        /// The node.
        to: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The call.
        call: WireCall,
        /// Where the answer goes.
        reply: std::sync::mpsc::Sender<Answer>,
    },
    /// An `Inspect`, as `Client`.
    Inspect {
        /// The node.
        to: NodeId,
        /// The partition.
        partition: PartitionId,
        /// Where the answer goes.
        reply: std::sync::mpsc::Sender<NodeStatus>,
    },
}

/// Turn `msg`, addressed to `to`, into what goes on the wire.
///
/// # Errors
///
/// [`WireError::LocalOnly`] for a variant that never leaves its process; [`WireError::Value`]
/// for a client call holding a value with no canonical form (the caller answers it). Either
/// way the message comes back with the error, boxed: a `Msg` is large.
pub fn outbound(to: NodeId, msg: Msg) -> Result<Outbound, Box<(WireError, Msg)>> {
    let plain = match msg {
        Msg::Frame {
            partition,
            correlation,
            from,
            frame,
        } => WireMsg::Frame {
            to,
            partition,
            correlation,
            from,
            frame,
        },
        Msg::PeerAsk {
            asker,
            partition,
            correlation,
            ask,
        } => WireMsg::PeerAsk {
            to,
            asker,
            partition,
            correlation,
            ask: WirePeerAsk::from(ask),
        },
        Msg::CatchUp {
            asker,
            partition,
            correlation,
            from,
            to: target,
            through,
            credential,
            seed,
        } => WireMsg::CatchUp {
            to,
            asker,
            partition,
            correlation,
            source: from,
            target,
            through,
            credential,
            seed: WireSeed::from(*seed),
        },
        Msg::Restore { partition, seed } => WireMsg::Restore {
            to,
            partition,
            seed: WireSeed::from(*seed),
        },
        Msg::Routed {
            partition,
            correlation,
            kind,
        } => WireMsg::Routed {
            to,
            partition,
            correlation,
            kind,
        },
        Msg::Landed {
            result,
            partition,
            correlation,
            emitter,
            not_before,
        } => WireMsg::Landed {
            to,
            result,
            partition,
            correlation,
            emitter,
            not_before,
        },
        Msg::Client(client) => {
            return match WireCall::try_from(&client.call) {
                Ok(call) => Ok(Outbound::Client {
                    to,
                    partition: client.partition,
                    call,
                    reply: client.reply,
                }),
                Err(error) => Err(Box::new((error, Msg::Client(client)))),
            }
        }
        Msg::Inspect { partition, reply } => {
            return Ok(Outbound::Inspect {
                to,
                partition,
                reply,
            })
        }
        local @ (Msg::Control { .. }
        | Msg::Fault { .. }
        | Msg::Activated { .. }
        | Msg::Inject { .. }
        | Msg::PeerBooted { .. }
        | Msg::Stop) => return Err(Box::new((WireError::LocalOnly(local_name(&local)), local))),
        #[cfg(debug_assertions)]
        local @ Msg::Damage { .. } => {
            return Err(Box::new((WireError::LocalOnly("Damage"), local)))
        }
    };
    Ok(Outbound::Plain(plain))
}

const fn local_name(msg: &Msg) -> &'static str {
    match msg {
        Msg::Control { .. } => "Control",
        Msg::Fault { .. } => "Fault",
        Msg::Activated { .. } => "Activated",
        Msg::Inject { .. } => "Inject",
        Msg::PeerBooted { .. } => "PeerBooted",
        Msg::Stop => "Stop",
        _ => "a peer message",
    }
}

/// What a received [`WireMsg`] becomes in this process.
#[derive(Debug)]
pub enum Inbound {
    /// A message to post to `to` as is.
    Plain(NodeId, Msg),
    /// A client call: post it with a local `Sender`, and send its answer back under `reply`.
    Client {
        /// The node.
        to: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The call.
        call: ClientCall,
        /// The caller's correlation.
        reply: u64,
    },
    /// An `Inspect`, as `Client`.
    Inspect {
        /// The node.
        to: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The caller's correlation.
        reply: u64,
    },
}

/// Turn a received message into what this process posts.
///
/// # Errors
///
/// [`WireError::Value`] when a carried value or path does not decode.
pub fn inbound(msg: WireMsg) -> Result<Inbound, WireError> {
    Ok(match msg {
        WireMsg::Frame {
            to,
            partition,
            correlation,
            from,
            frame,
        } => Inbound::Plain(
            to,
            Msg::Frame {
                partition,
                correlation,
                from,
                frame,
            },
        ),
        WireMsg::PeerAsk {
            to,
            asker,
            partition,
            correlation,
            ask,
        } => Inbound::Plain(
            to,
            Msg::PeerAsk {
                asker,
                partition,
                correlation,
                ask: ask.into(),
            },
        ),
        WireMsg::CatchUp {
            to,
            asker,
            partition,
            correlation,
            source,
            target,
            through,
            credential,
            seed,
        } => Inbound::Plain(
            to,
            Msg::CatchUp {
                asker,
                partition,
                correlation,
                from: source,
                to: target,
                through,
                credential,
                seed: Box::new(seed.into()),
            },
        ),
        WireMsg::Restore {
            to,
            partition,
            seed,
        } => Inbound::Plain(
            to,
            Msg::Restore {
                partition,
                seed: Box::new(seed.into()),
            },
        ),
        WireMsg::Routed {
            to,
            partition,
            correlation,
            kind,
        } => Inbound::Plain(
            to,
            Msg::Routed {
                partition,
                correlation,
                kind,
            },
        ),
        WireMsg::Landed {
            to,
            result,
            partition,
            correlation,
            emitter,
            not_before,
        } => Inbound::Plain(
            to,
            Msg::Landed {
                result,
                partition,
                correlation,
                emitter,
                not_before,
            },
        ),
        WireMsg::Client {
            to,
            partition,
            call,
            reply,
        } => Inbound::Client {
            to,
            partition,
            call: ClientCall::try_from(call)?,
            reply,
        },
        WireMsg::Inspect {
            to,
            partition,
            reply,
        } => Inbound::Inspect {
            to,
            partition,
            reply,
        },
    })
}

/// Postcard bytes of `frame`.
///
/// # Panics
///
/// Never in practice: every wire type serialises (postcard fails only on an unsized sequence,
/// which no type here has).
#[must_use]
pub fn encode(frame: &Frame) -> Vec<u8> {
    postcard::to_stdvec(frame).expect("every wire type serialises")
}

/// The frame in `bytes`.
///
/// # Errors
///
/// [`WireError::Decode`] when they are not one.
pub fn decode(bytes: &[u8]) -> Result<Frame, WireError> {
    postcard::from_bytes(bytes).map_err(|error| WireError::Decode(error.to_string()))
}

// ---- values ---------------------------------------------------------------------------------

/// A value's canonical CBOR.
fn val(value: &Value) -> Result<Vec<u8>, WireError> {
    cbor::encode(value).map_err(|error| WireError::Value(error.to_string()))
}

fn unval(bytes: &[u8]) -> Result<Value, WireError> {
    cbor::decode(bytes).map_err(|error| WireError::Value(error.to_string()))
}

fn path(text: &str) -> Result<Path, WireError> {
    Path::parse(text).map_err(|error| WireError::Value(format!("path {text:?}: {error}")))
}

fn int(v: i128) -> Result<Int, WireError> {
    Int::new(v).ok_or_else(|| WireError::Value(format!("integer {v} out of range")))
}

// ---- peer asks and seeds --------------------------------------------------------------------

/// [`PeerAsk`] on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WirePeerAsk {
    Inventory {
        copy: CopyId,
        anchor: LineageAnchor,
    },
    Sync {
        copy: CopyId,
        cutoff: Seq,
        generation: Generation,
        post_commit: bool,
    },
}

impl From<PeerAsk> for WirePeerAsk {
    fn from(ask: PeerAsk) -> Self {
        match ask {
            PeerAsk::Inventory { copy, anchor } => Self::Inventory { copy, anchor },
            PeerAsk::Sync {
                copy,
                cutoff,
                generation,
                post_commit,
            } => Self::Sync {
                copy,
                cutoff,
                generation,
                post_commit,
            },
        }
    }
}

impl From<WirePeerAsk> for PeerAsk {
    fn from(ask: WirePeerAsk) -> Self {
        match ask {
            WirePeerAsk::Inventory { copy, anchor } => Self::Inventory { copy, anchor },
            WirePeerAsk::Sync {
                copy,
                cutoff,
                generation,
                post_commit,
            } => Self::Sync {
                copy,
                cutoff,
                generation,
                post_commit,
            },
        }
    }
}

/// [`DormantSeed`] on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct WireSeed {
    pub config: PartitionConfig,
    pub lineage: Lineage,
    pub revision: Revision,
}

impl From<DormantSeed> for WireSeed {
    fn from(seed: DormantSeed) -> Self {
        Self {
            config: seed.config,
            lineage: seed.lineage,
            revision: seed.revision,
        }
    }
}

impl From<WireSeed> for DormantSeed {
    fn from(seed: WireSeed) -> Self {
        Self {
            config: seed.config,
            lineage: seed.lineage,
            revision: seed.revision,
        }
    }
}

// ---- client calls ---------------------------------------------------------------------------

/// [`ClientCall`] on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WireCall {
    Put {
        identity: RequestIdentity,
        object: Bytes,
        change: WireChange,
        expect: Option<WireExpected>,
        remaining_millis: u64,
    },
    Compile {
        identity: RequestIdentity,
        puts: Vec<WirePutOp>,
        remaining_millis: u64,
    },
    Resend {
        request: TxnRequest,
    },
    Get {
        identity: RequestIdentity,
        object: Bytes,
        look: WireLook,
    },
    GetPrevious {
        identity: RequestIdentity,
        object: Bytes,
    },
    Status {
        identity: RequestIdentity,
        generation: Option<Generation>,
    },
}

impl WireCall {
    /// The identity the call is made under: the client and request a node logs it by.
    #[must_use]
    pub fn identity(&self) -> RequestIdentity {
        match self {
            Self::Put { identity, .. }
            | Self::Compile { identity, .. }
            | Self::Get { identity, .. }
            | Self::GetPrevious { identity, .. }
            | Self::Status { identity, .. } => *identity,
            Self::Resend { request } => request.identity,
        }
    }

    /// The variant's name, for logs.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Put { .. } => "put",
            Self::Compile { .. } => "compile",
            Self::Resend { .. } => "resend",
            Self::Get { .. } => "get",
            Self::GetPrevious { .. } => "get_previous",
            Self::Status { .. } => "status",
        }
    }
}

impl TryFrom<&ClientCall> for WireCall {
    type Error = WireError;

    fn try_from(call: &ClientCall) -> Result<Self, WireError> {
        Ok(match call {
            ClientCall::Put {
                identity,
                object,
                change,
                expect,
                remaining_millis,
            } => Self::Put {
                identity: *identity,
                object: object.clone(),
                change: WireChange::try_from(change)?,
                expect: expect.map(WireExpected::from),
                remaining_millis: *remaining_millis,
            },
            ClientCall::Compile {
                identity,
                puts,
                remaining_millis,
            } => Self::Compile {
                identity: *identity,
                puts: puts
                    .iter()
                    .map(WirePutOp::try_from)
                    .collect::<Result<_, _>>()?,
                remaining_millis: *remaining_millis,
            },
            ClientCall::Resend { request } => Self::Resend {
                request: request.clone(),
            },
            ClientCall::Get {
                identity,
                object,
                look,
            } => Self::Get {
                identity: *identity,
                object: object.clone(),
                look: WireLook::try_from(look)?,
            },
            ClientCall::GetPrevious { identity, object } => Self::GetPrevious {
                identity: *identity,
                object: object.clone(),
            },
            ClientCall::Status {
                identity,
                generation,
            } => Self::Status {
                identity: *identity,
                generation: *generation,
            },
        })
    }
}

impl TryFrom<WireCall> for ClientCall {
    type Error = WireError;

    fn try_from(call: WireCall) -> Result<Self, WireError> {
        Ok(match call {
            WireCall::Put {
                identity,
                object,
                change,
                expect,
                remaining_millis,
            } => Self::Put {
                identity,
                object,
                change: change.try_into()?,
                expect: expect.map(Expected::from),
                remaining_millis,
            },
            WireCall::Compile {
                identity,
                puts,
                remaining_millis,
            } => Self::Compile {
                identity,
                puts: puts
                    .into_iter()
                    .map(PutOp::try_from)
                    .collect::<Result<_, _>>()?,
                remaining_millis,
            },
            WireCall::Resend { request } => Self::Resend { request },
            WireCall::Get {
                identity,
                object,
                look,
            } => Self::Get {
                identity,
                object,
                look: look.try_into()?,
            },
            WireCall::GetPrevious { identity, object } => Self::GetPrevious { identity, object },
            WireCall::Status {
                identity,
                generation,
            } => Self::Status {
                identity,
                generation,
            },
        })
    }
}

/// [`Expected`] on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WireExpected {
    Absent,
    Version(u64),
}

impl From<Expected> for WireExpected {
    fn from(expect: Expected) -> Self {
        match expect {
            Expected::Absent => Self::Absent,
            Expected::Version(v) => Self::Version(v),
        }
    }
}

impl From<WireExpected> for Expected {
    fn from(expect: WireExpected) -> Self {
        match expect {
            WireExpected::Absent => Self::Absent,
            WireExpected::Version(v) => Self::Version(v),
        }
    }
}

/// [`PutOp`] on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct WirePutOp {
    pub group: AffinityId,
    pub object: Bytes,
    pub change: WireChange,
    pub expect: Option<WireExpected>,
}

impl TryFrom<&PutOp> for WirePutOp {
    type Error = WireError;

    fn try_from(put: &PutOp) -> Result<Self, WireError> {
        Ok(Self {
            group: put.group,
            object: put.object.clone(),
            change: WireChange::try_from(&put.change)?,
            expect: put.expect.map(WireExpected::from),
        })
    }
}

impl TryFrom<WirePutOp> for PutOp {
    type Error = WireError;

    fn try_from(put: WirePutOp) -> Result<Self, WireError> {
        Ok(Self {
            group: put.group,
            object: put.object,
            change: put.change.try_into()?,
            expect: put.expect.map(Expected::from),
        })
    }
}

/// A document op on the wire: values as CBOR, paths as their text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WireOp {
    Replace(Vec<u8>),
    Set(String, Vec<u8>),
    Remove(String),
    Increment(String, i128),
}

/// An element op on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WireElemOp {
    Put(Vec<u8>, Vec<u8>),
    Add(Vec<u8>),
    Remove(Vec<u8>),
    NeedPresent(Vec<u8>),
    NeedAbsent(Vec<u8>),
}

/// A list edit on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WireListEdit {
    Push(Vec<u8>),
    Insert { at: i64, value: Vec<u8> },
    Remove { at: i64 },
    Replace { at: i64, value: Vec<u8> },
    Move { from: i64, to: i64 },
}

/// [`Change`] on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WireChange {
    Doc(Vec<WireOp>),
    Map(Vec<WireElemOp>),
    Set(Vec<WireElemOp>),
    List(Vec<WireListEdit>),
    Chunk {
        upload: Upload,
        index: u32,
        bytes: Bytes,
    },
    Publish {
        upload: Upload,
        size: u64,
        chunk_size: u64,
        sha256: [u8; 32],
    },
    Gc {
        floor: u64,
    },
    Clear,
}

fn elem_ops(ops: &[ElemOp]) -> Result<Vec<WireElemOp>, WireError> {
    ops.iter()
        .map(|op| {
            Ok(match op {
                ElemOp::Put(key, value) => WireElemOp::Put(val(key)?, val(value)?),
                ElemOp::Add(key) => WireElemOp::Add(val(key)?),
                ElemOp::Remove(key) => WireElemOp::Remove(val(key)?),
                ElemOp::NeedPresent(key) => WireElemOp::NeedPresent(val(key)?),
                ElemOp::NeedAbsent(key) => WireElemOp::NeedAbsent(val(key)?),
            })
        })
        .collect()
}

fn unelem_ops(ops: Vec<WireElemOp>) -> Result<Vec<ElemOp>, WireError> {
    ops.into_iter()
        .map(|op| {
            Ok(match op {
                WireElemOp::Put(key, value) => ElemOp::Put(unval(&key)?, unval(&value)?),
                WireElemOp::Add(key) => ElemOp::Add(unval(&key)?),
                WireElemOp::Remove(key) => ElemOp::Remove(unval(&key)?),
                WireElemOp::NeedPresent(key) => ElemOp::NeedPresent(unval(&key)?),
                WireElemOp::NeedAbsent(key) => ElemOp::NeedAbsent(unval(&key)?),
            })
        })
        .collect()
}

impl TryFrom<&Change> for WireChange {
    type Error = WireError;

    fn try_from(change: &Change) -> Result<Self, WireError> {
        Ok(match change {
            Change::Doc(Delta(ops)) => Self::Doc(
                ops.iter()
                    .map(|op| {
                        Ok(match op {
                            Op::Replace(value) => WireOp::Replace(val(value)?),
                            Op::Set(at, value) => WireOp::Set(at.to_string(), val(value)?),
                            Op::Remove(at) => WireOp::Remove(at.to_string()),
                            Op::Increment(at, by) => WireOp::Increment(at.to_string(), by.get()),
                        })
                    })
                    .collect::<Result<_, WireError>>()?,
            ),
            Change::Map(ops) => Self::Map(elem_ops(ops)?),
            Change::Set(ops) => Self::Set(elem_ops(ops)?),
            Change::List(edits) => Self::List(
                edits
                    .iter()
                    .map(|edit| {
                        Ok(match edit {
                            ListEdit::Push(value) => WireListEdit::Push(val(value)?),
                            ListEdit::Insert { at, value } => WireListEdit::Insert {
                                at: *at,
                                value: val(value)?,
                            },
                            ListEdit::Remove { at } => WireListEdit::Remove { at: *at },
                            ListEdit::Replace { at, value } => WireListEdit::Replace {
                                at: *at,
                                value: val(value)?,
                            },
                            ListEdit::Move { from, to } => WireListEdit::Move {
                                from: *from,
                                to: *to,
                            },
                        })
                    })
                    .collect::<Result<_, WireError>>()?,
            ),
            Change::Chunk {
                upload,
                index,
                bytes,
            } => Self::Chunk {
                upload: *upload,
                index: *index,
                bytes: bytes.clone(),
            },
            Change::Publish {
                upload,
                size,
                chunk_size,
                sha256,
            } => Self::Publish {
                upload: *upload,
                size: *size,
                chunk_size: *chunk_size,
                sha256: *sha256,
            },
            Change::Gc { floor } => Self::Gc { floor: *floor },
            Change::Clear => Self::Clear,
        })
    }
}

impl TryFrom<WireChange> for Change {
    type Error = WireError;

    fn try_from(change: WireChange) -> Result<Self, WireError> {
        Ok(match change {
            WireChange::Doc(ops) => Self::Doc(Delta(
                ops.into_iter()
                    .map(|op| {
                        Ok(match op {
                            WireOp::Replace(value) => Op::Replace(unval(&value)?),
                            WireOp::Set(at, value) => Op::Set(path(&at)?, unval(&value)?),
                            WireOp::Remove(at) => Op::Remove(path(&at)?),
                            WireOp::Increment(at, by) => Op::Increment(path(&at)?, int(by)?),
                        })
                    })
                    .collect::<Result<_, WireError>>()?,
            )),
            WireChange::Map(ops) => Self::Map(unelem_ops(ops)?),
            WireChange::Set(ops) => Self::Set(unelem_ops(ops)?),
            WireChange::List(edits) => Self::List(
                edits
                    .into_iter()
                    .map(|edit| {
                        Ok(match edit {
                            WireListEdit::Push(value) => ListEdit::Push(unval(&value)?),
                            WireListEdit::Insert { at, value } => ListEdit::Insert {
                                at,
                                value: unval(&value)?,
                            },
                            WireListEdit::Remove { at } => ListEdit::Remove { at },
                            WireListEdit::Replace { at, value } => ListEdit::Replace {
                                at,
                                value: unval(&value)?,
                            },
                            WireListEdit::Move { from, to } => ListEdit::Move { from, to },
                        })
                    })
                    .collect::<Result<_, WireError>>()?,
            ),
            WireChange::Chunk {
                upload,
                index,
                bytes,
            } => Self::Chunk {
                upload,
                index,
                bytes,
            },
            WireChange::Publish {
                upload,
                size,
                chunk_size,
                sha256,
            } => Self::Publish {
                upload,
                size,
                chunk_size,
                sha256,
            },
            WireChange::Gc { floor } => Self::Gc { floor },
            WireChange::Clear => Self::Clear,
        })
    }
}

/// [`Token`] on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct WireToken {
    pub generation: Generation,
    pub version: u64,
    pub position: u64,
}

impl From<Token> for WireToken {
    fn from(token: Token) -> Self {
        Self {
            generation: token.generation,
            version: token.version,
            position: token.position,
        }
    }
}

impl From<WireToken> for Token {
    fn from(token: WireToken) -> Self {
        Self {
            generation: token.generation,
            version: token.version,
            position: token.position,
        }
    }
}

/// [`Look`] on the wire. Limits travel as `u64`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WireLook {
    Doc,
    Members { after: Option<Vec<u8>>, limit: u64 },
    Items { at: i64, limit: u64 },
    ItemsAfter { token: WireToken, limit: u64 },
    Range { offset: u64, len: u64 },
}

fn limit(limit: u64) -> Result<usize, WireError> {
    usize::try_from(limit).map_err(|_| WireError::Value(format!("limit {limit} out of range")))
}

impl TryFrom<&Look> for WireLook {
    type Error = WireError;

    fn try_from(look: &Look) -> Result<Self, WireError> {
        Ok(match look {
            Look::Doc => Self::Doc,
            Look::Members { after, limit } => Self::Members {
                after: after.as_ref().map(val).transpose()?,
                limit: *limit as u64,
            },
            Look::Items { at, limit } => Self::Items {
                at: *at,
                limit: *limit as u64,
            },
            Look::ItemsAfter { token, limit } => Self::ItemsAfter {
                token: (*token).into(),
                limit: *limit as u64,
            },
            Look::Range { offset, len } => Self::Range {
                offset: *offset,
                len: *len,
            },
        })
    }
}

impl TryFrom<WireLook> for Look {
    type Error = WireError;

    fn try_from(look: WireLook) -> Result<Self, WireError> {
        Ok(match look {
            WireLook::Doc => Self::Doc,
            WireLook::Members { after, limit: n } => Self::Members {
                after: after.as_deref().map(unval).transpose()?,
                limit: limit(n)?,
            },
            WireLook::Items { at, limit: n } => Self::Items {
                at,
                limit: limit(n)?,
            },
            WireLook::ItemsAfter { token, limit: n } => Self::ItemsAfter {
                token: token.into(),
                limit: limit(n)?,
            },
            WireLook::Range { offset, len } => Self::Range { offset, len },
        })
    }
}

// ---- answers --------------------------------------------------------------------------------

/// [`ApiError`] on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct WireApiError {
    pub kind: ErrorKind,
    pub retry: RetryRule,
    pub no_mutation: bool,
    pub detail: String,
    pub retry_after_millis: Option<u64>,
}

impl From<&ApiError> for WireApiError {
    fn from(error: &ApiError) -> Self {
        Self {
            kind: error.kind,
            retry: error.retry,
            no_mutation: error.no_mutation,
            detail: error.detail.clone(),
            retry_after_millis: error
                .retry_after
                .map(|after| u64::try_from(after.as_millis()).unwrap_or(u64::MAX)),
        }
    }
}

impl From<WireApiError> for ApiError {
    fn from(error: WireApiError) -> Self {
        Self {
            kind: error.kind,
            retry: error.retry,
            no_mutation: error.no_mutation,
            detail: error.detail,
            retry_after: error.retry_after_millis.map(Duration::from_millis),
        }
    }
}

/// A list item on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct WireItem {
    pub position: u64,
    pub id: u128,
    pub value: Vec<u8>,
    pub inline: bool,
    pub version: u64,
}

/// A map or set member on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct WireMember {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub version: u64,
}

/// A blob manifest on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct WireManifest {
    pub size: u64,
    pub sha256: [u8; 32],
    pub upload: Upload,
    pub chunk_size: u64,
    pub chunk_sha256: Vec<[u8; 32]>,
}

/// [`Seen`] on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WireSeen {
    Absent,
    Doc {
        version: u64,
        value: Vec<u8>,
    },
    Members {
        /// `true` for a map, `false` for a set.
        map: bool,
        version: u64,
        count: u64,
        members: Vec<WireMember>,
    },
    Items {
        version: u64,
        count: u64,
        bytes: u64,
        blocks: u64,
        records: bool,
        items: Vec<WireItem>,
        next: Option<WireToken>,
    },
    Range {
        version: u64,
        manifest: WireManifest,
        bytes: Bytes,
    },
}

impl TryFrom<&Seen> for WireSeen {
    type Error = WireError;

    fn try_from(seen: &Seen) -> Result<Self, WireError> {
        Ok(match seen {
            Seen::Absent => Self::Absent,
            Seen::Doc(doc) => Self::Doc {
                version: doc.version,
                value: val(&doc.value)?,
            },
            Seen::Members(Members {
                collection,
                members,
            }) => Self::Members {
                map: collection.kind == CollectionKind::Map,
                version: collection.version,
                count: collection.count,
                members: members
                    .iter()
                    .map(|member| {
                        Ok(WireMember {
                            key: val(&member.key)?,
                            value: member.value.as_ref().map(val).transpose()?,
                            version: member.version,
                        })
                    })
                    .collect::<Result<_, WireError>>()?,
            },
            Seen::Items(Items { list, items, next }) => Self::Items {
                version: list.version,
                count: list.count,
                bytes: list.bytes,
                blocks: list.blocks as u64,
                records: list.records,
                items: items
                    .iter()
                    .map(|item| {
                        Ok(WireItem {
                            position: item.position,
                            id: item.id,
                            value: val(&item.value)?,
                            inline: item.inline,
                            version: item.version,
                        })
                    })
                    .collect::<Result<_, WireError>>()?,
                next: next.map(WireToken::from),
            },
            Seen::Range { blob, bytes } => Self::Range {
                version: blob.version,
                manifest: WireManifest {
                    size: blob.manifest.size,
                    sha256: blob.manifest.sha256,
                    upload: blob.manifest.upload,
                    chunk_size: blob.manifest.chunk_size,
                    chunk_sha256: blob.manifest.chunk_sha256.clone(),
                },
                bytes: bytes.clone(),
            },
        })
    }
}

impl TryFrom<WireSeen> for Seen {
    type Error = WireError;

    fn try_from(seen: WireSeen) -> Result<Self, WireError> {
        Ok(match seen {
            WireSeen::Absent => Self::Absent,
            WireSeen::Doc { version, value } => Self::Doc(Document {
                version,
                value: unval(&value)?,
            }),
            WireSeen::Members {
                map,
                version,
                count,
                members,
            } => Self::Members(Members {
                collection: Collection {
                    kind: if map {
                        CollectionKind::Map
                    } else {
                        CollectionKind::Set
                    },
                    version,
                    count,
                },
                members: members
                    .into_iter()
                    .map(|member| {
                        Ok(Member {
                            key: unval(&member.key)?,
                            value: member.value.as_deref().map(unval).transpose()?,
                            version: member.version,
                        })
                    })
                    .collect::<Result<_, WireError>>()?,
            }),
            WireSeen::Items {
                version,
                count,
                bytes,
                blocks,
                records,
                items,
                next,
            } => Self::Items(Items {
                list: List {
                    version,
                    count,
                    bytes,
                    blocks: usize::try_from(blocks)
                        .map_err(|_| WireError::Value(format!("{blocks} blocks")))?,
                    records,
                },
                items: items
                    .into_iter()
                    .map(|item| {
                        Ok(Item {
                            position: item.position,
                            id: item.id,
                            value: unval(&item.value)?,
                            inline: item.inline,
                            version: item.version,
                        })
                    })
                    .collect::<Result<_, WireError>>()?,
                next: next.map(Token::from),
            }),
            WireSeen::Range {
                version,
                manifest,
                bytes,
            } => Self::Range {
                blob: Blob {
                    version,
                    manifest: Manifest {
                        size: manifest.size,
                        sha256: manifest.sha256,
                        upload: manifest.upload,
                        chunk_size: manifest.chunk_size,
                        chunk_sha256: manifest.chunk_sha256,
                    },
                },
                bytes,
            },
        })
    }
}

/// [`Answer`] on the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum WireAnswer {
    Txn {
        result: TxnResult,
        request: TxnRequest,
    },
    Read {
        outcome: ReadServiceOutcome,
        seen: WireSeen,
        generation: Generation,
        at: Seq,
    },
    Compiled(TxnRequest),
    Unchanged,
    Status(TxnStatus),
    Error {
        error: WireApiError,
        request: Option<TxnRequest>,
    },
}

impl WireAnswer {
    /// `answer` on the wire. A read whose value has no canonical form becomes an error answer
    /// that says so, never a silent loss.
    #[must_use]
    pub fn carry(answer: &Answer) -> Self {
        match answer {
            Answer::Txn { result, request } => Self::Txn {
                result: *result,
                request: request.clone(),
            },
            Answer::Read {
                outcome,
                seen,
                generation,
                at,
            } => match WireSeen::try_from(seen) {
                Ok(seen) => Self::Read {
                    outcome: *outcome,
                    seen,
                    generation: *generation,
                    at: *at,
                },
                Err(error) => Self::Error {
                    error: WireApiError::from(&ApiError::host(&error.to_string())),
                    request: None,
                },
            },
            Answer::Compiled(request) => Self::Compiled(request.clone()),
            Answer::Unchanged => Self::Unchanged,
            Answer::Status(status) => Self::Status(*status),
            Answer::Error { error, request } => Self::Error {
                error: WireApiError::from(error),
                request: request.clone(),
            },
        }
    }

    /// The answer this process hands its caller. A read that does not decode is an error
    /// answer naming why.
    #[must_use]
    pub fn land(self) -> Answer {
        match self {
            Self::Txn { result, request } => Answer::Txn { result, request },
            Self::Read {
                outcome,
                seen,
                generation,
                at,
            } => match Seen::try_from(seen) {
                Ok(seen) => Answer::Read {
                    outcome,
                    seen,
                    generation,
                    at,
                },
                Err(error) => Answer::Error {
                    error: ApiError::host(&error.to_string()),
                    request: None,
                },
            },
            Self::Compiled(request) => Answer::Compiled(request),
            Self::Unchanged => Answer::Unchanged,
            Self::Status(status) => Answer::Status(status),
            Self::Error { error, request } => Answer::Error {
                error: error.into(),
                request,
            },
        }
    }
}

/// [`Holding`] on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct WireHolding {
    pub generation: Generation,
    pub owner_epoch: OwnerEpoch,
    pub applied: u64,
    pub durable: u64,
}

/// [`NodeStatus`] on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct WireNodeStatus {
    pub node: NodeId,
    pub boot: BootId,
    pub fault: Option<String>,
    pub holds: Option<WireHolding>,
    pub authority: String,
    pub fence: Option<DenyReason>,
    pub fence_polling: bool,
    pub replace_stopped: bool,
    pub recovery: Option<String>,
    pub recovered: Option<Generation>,
    pub role: String,
    pub protection: Option<String>,
    pub admits: Option<bool>,
    pub published: Option<(Generation, Seq)>,
    pub in_flight: Option<RequestId>,
    pub waits_on: Option<Seq>,
    pub stalled: Option<String>,
    pub reload_pending: Option<u32>,
}

/// The role words a host reports (`host::Node::status`).
const ROLES: [&str; 3] = ["primary", "secondary", "none"];

impl From<&NodeStatus> for WireNodeStatus {
    fn from(status: &NodeStatus) -> Self {
        Self {
            node: status.node,
            boot: status.boot,
            fault: status.fault.clone(),
            holds: status.holds.map(|held| WireHolding {
                generation: held.generation,
                owner_epoch: held.owner_epoch,
                applied: held.applied,
                durable: held.durable,
            }),
            authority: status.authority.clone(),
            fence: status.fence,
            fence_polling: status.fence_polling,
            replace_stopped: status.replace_stopped,
            recovery: status.recovery.clone(),
            recovered: status.recovered,
            role: status.role.to_owned(),
            protection: status.protection.clone(),
            admits: status.admits,
            published: status.published,
            in_flight: status.in_flight,
            waits_on: status.waits_on,
            stalled: status.stalled.clone(),
            reload_pending: status.reload_pending,
        }
    }
}

impl TryFrom<WireNodeStatus> for NodeStatus {
    type Error = WireError;

    fn try_from(status: WireNodeStatus) -> Result<Self, WireError> {
        let role = ROLES
            .iter()
            .find(|role| **role == status.role)
            .ok_or_else(|| WireError::Value(format!("unknown role {:?}", status.role)))?;
        Ok(Self {
            node: status.node,
            boot: status.boot,
            fault: status.fault,
            holds: status.holds.map(|held| Holding {
                generation: held.generation,
                owner_epoch: held.owner_epoch,
                applied: held.applied,
                durable: held.durable,
            }),
            authority: status.authority,
            fence: status.fence,
            fence_polling: status.fence_polling,
            replace_stopped: status.replace_stopped,
            recovery: status.recovery,
            recovered: status.recovered,
            role,
            protection: status.protection,
            admits: status.admits,
            published: status.published,
            in_flight: status.in_flight,
            waits_on: status.waits_on,
            stalled: status.stalled,
            reload_pending: status.reload_pending,
        })
    }
}
