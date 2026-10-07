//! [`Db`]: the embedded API (M9 architecture §3, §5).
//!
//! `Db` mints identities, sets deadlines, sends each call to the owner's node thread and maps the
//! answer to an [`ApiError`] with the §5.4 name and retry rule. It holds no kernel state: the
//! node threads own everything (§4).
//!
//! A call that gets no answer in time is `UNKNOWN_OUTCOME` for a write (the request may still
//! publish; ask `status` with the same identity) and `UNAVAILABLE` for a read (a read mutates
//! nothing). Neither waits for ever.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::ConfigStore;
use rdb_core::contracts::errors::{ErrorKind, RdbError, RetryRule};
use rdb_core::contracts::ids::{
    ClientId, Generation, NodeId, OwnerEpoch, PartitionId, RequestId, RequestIdentity, Seq,
    TenantId,
};
use rdb_core::contracts::trace::ReadServiceOutcome;
use rdb_core::contracts::txn::{Durability, Outcome, TxnRequest, TxnStatus};
use tokio::runtime::Handle;

use crate::admin::{self, PARTITION};
use crate::clock::HostClock;
use crate::control::ControlAdapter;
use crate::host::{self, Answer, Client, ClientCall, Msg, NodeHandle, NodeStatus};
use crate::transport::Links;

/// The node every S0 call goes to: partition 1's owner.
const OWNER: NodeId = admin::OWNER;

/// The only tenant and client S0 writes as. Not settable by a caller (lead ruling, 2026-10-07:
/// no raw identity on the REPL).
const TENANT: TenantId = TenantId(1);
const CLIENT: ClientId = ClientId(1);

/// A failed call, with the §5.4 name and what the caller may do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// The §5.4 kind.
    pub kind: ErrorKind,
    /// What the caller may do.
    pub retry: RetryRule,
    /// Whether nothing was mutated, provably.
    pub no_mutation: bool,
    /// Detail, for the log and the operator.
    pub detail: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", wire_name(self.kind), self.detail)
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    /// An error of `kind`, with the retry rule and mutation proof that kind carries when the
    /// kernel raises it.
    #[must_use]
    pub fn new(kind: ErrorKind, detail: impl Into<String>) -> Self {
        let (retry, no_mutation) = match kind {
            ErrorKind::ConditionFailed | ErrorKind::InvalidArgument => {
                (RetryRule::Definitive, true)
            }
            ErrorKind::UnknownOutcome => (RetryRule::QueryStatus, false),
            ErrorKind::CorruptHistory => (RetryRule::Quarantine, false),
            _ => (RetryRule::BoundedJitter, false),
        };
        Self {
            kind,
            retry,
            no_mutation,
            detail: detail.into(),
        }
    }

    /// The kernel's own error, with its own retry rule and proof.
    #[must_use]
    pub fn from_kernel(error: &RdbError) -> Self {
        Self {
            kind: error.kind(),
            retry: error.retry_rule(),
            no_mutation: error.proves_no_mutation(),
            detail: error.to_string(),
        }
    }

    /// A caller's argument the API refuses before the kernel sees it.
    #[must_use]
    pub fn invalid(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, detail)
    }

    /// A put refused before it was sent: this node has adopted no generation yet, so there is
    /// none to fence it with (`expected_generation` is always set; ADR-0004 §8). Nothing left
    /// the host, so nothing was mutated, and it may be retried once a generation is adopted.
    #[must_use]
    pub fn not_adopted(node: NodeId, partition: PartitionId) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::BoundedJitter,
            no_mutation: true,
            detail: format!(
                "no generation adopted yet for partition {} on node {}; nothing was sent",
                partition.0, node.0
            ),
        }
    }

    /// The node faulted (§4.4): the host could not serve an effect. Nothing is retried.
    #[must_use]
    pub fn host(detail: &str) -> Self {
        Self {
            kind: ErrorKind::Unavailable,
            retry: RetryRule::NotWired,
            no_mutation: false,
            detail: format!("host fault: {detail}"),
        }
    }

    /// The §5.4 wire name, `SCREAMING_SNAKE_CASE`.
    #[must_use]
    pub fn name(&self) -> String {
        wire_name(self.kind)
    }
}

/// `ProtectionPaused` → `PROTECTION_PAUSED`.
fn wire_name(kind: ErrorKind) -> String {
    let camel = format!("{kind:?}");
    let mut out = String::with_capacity(camel.len() + 4);
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}

/// How long `Db` waits for each kind of call.
#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    /// A write: past this it is `UNKNOWN_OUTCOME`. Also the kernel deadline it is sent with.
    pub put: Duration,
    /// A read or a status query.
    pub read: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            put: Duration::from_secs(5),
            read: Duration::from_secs(1),
        }
    }
}

/// Where and how to open a [`Db`].
#[derive(Debug, Clone)]
pub struct DbConfig {
    /// The data directory. Each node's RocksDB goes under `<dir>/nodes/<n>`. Must not hold one.
    pub dir: PathBuf,
    /// Peer links to hold from the start, as `(a, b)` node pairs.
    pub hold: Vec<(NodeId, NodeId)>,
    /// Call timeouts.
    pub timeouts: Timeouts,
}

/// A published write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOk {
    /// The request id, for `status` and `retry`.
    pub request: RequestId,
    /// The generation it published in.
    pub generation: Generation,
    /// The owner epoch that ran it.
    pub owner_epoch: OwnerEpoch,
    /// Its position.
    pub seq: Seq,
    /// `Published`, or `RecoveredApplied` for an answer from retained history.
    pub outcome: Outcome,
    /// What was true when the reply was sent.
    pub durability: Durability,
    /// The request as sent, so the caller can [`Db::resend`] it.
    pub sent: Box<TxnRequest>,
}

/// A served read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetOk {
    /// The object's version and bytes, `None` when absent.
    pub value: Option<(u64, Bytes)>,
    /// The generation of the view it was read from.
    pub generation: Generation,
    /// The position of that view.
    pub at: Seq,
    /// Whether it waited at the barrier for an in-flight write.
    pub waited: bool,
}

/// A refused write, with the request so the caller can send it again (`retry`).
#[derive(Debug, Clone)]
pub struct PutError {
    /// Why.
    pub error: ApiError,
    /// The request sent, when one was.
    pub request: Option<Box<TxnRequest>>,
}

impl std::fmt::Display for PutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

/// Why [`Db::open`] failed.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The data directory already holds node data.
    #[error("{0} already holds node data; M9 opens only an empty directory")]
    NotEmpty(PathBuf),
    /// A node did not start.
    #[error("node start: {0}")]
    Node(String),
    /// The bootstrap was refused.
    #[error(transparent)]
    Bootstrap(#[from] admin::BootstrapError),
    /// The filesystem refused.
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// Three rDB nodes in this process, serving partition 1.
pub struct Db {
    nodes: Vec<NodeHandle>,
    links: Arc<Links>,
    control: Arc<ControlAdapter>,
    clock: HostClock,
    timeouts: Timeouts,
    next_request: AtomicU64,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

impl Db {
    /// Start three nodes on `config.dir` against `store`, then bootstrap partition 1.
    ///
    /// Returns once the bootstrap's events are queued, not when the partition is ready: watch
    /// [`Self::node_status`] for that.
    ///
    /// # Errors
    ///
    /// [`OpenError`].
    pub async fn open(
        config: DbConfig,
        store: Arc<dyn ConfigStore>,
        rt: Handle,
    ) -> Result<Self, OpenError> {
        let nodes_dir = config.dir.join("nodes");
        if nodes_dir.exists() && std::fs::read_dir(&nodes_dir)?.next().is_some() {
            return Err(OpenError::NotEmpty(nodes_dir));
        }
        std::fs::create_dir_all(&nodes_dir)?;
        let clock = HostClock::start();
        let links = Links::new();
        let control = ControlAdapter::new(Arc::clone(&store), rt, Arc::clone(&links));
        for (a, b) in &config.hold {
            links.hold(*a, *b);
        }
        let mut nodes = Vec::with_capacity(3);
        for n in 1..=3u32 {
            let node = NodeId(n);
            let dir = nodes_dir.join(n.to_string());
            let handle = host::spawn(node, dir, Arc::clone(&links), Arc::clone(&control), clock)
                .map_err(OpenError::Node)?;
            nodes.push(handle);
        }
        let db = Self {
            nodes,
            links,
            control,
            clock,
            timeouts: config.timeouts,
            next_request: AtomicU64::new(1),
        };
        let owner = db
            .node(OWNER)
            .ok_or_else(|| OpenError::Node("no owner node".into()))?;
        admin::bootstrap(&store, clock.now(), |msg| owner.send(msg)).await?;
        Ok(db)
    }

    fn node(&self, node: NodeId) -> Option<&NodeHandle> {
        self.nodes.iter().find(|handle| handle.node == node)
    }

    fn identity(&self) -> RequestIdentity {
        RequestIdentity {
            tenant: TENANT,
            client: CLIENT,
            request: RequestId(self.next_request.fetch_add(1, Ordering::Relaxed)),
        }
    }

    fn call(&self, call: ClientCall, wait: Duration) -> Option<Answer> {
        let (reply, answer) = mpsc::channel();
        let msg = Msg::Client(Client {
            partition: PARTITION,
            call,
            reply,
        });
        let owner = self.node(OWNER)?;
        if owner.send(msg).is_err() {
            return Some(Answer::Error {
                error: ApiError::host("the owner's node thread has stopped"),
                request: None,
            });
        }
        answer.recv_timeout(wait).ok()
    }

    /// Replace `object` with `value`, optionally only if it is at `if_version`.
    ///
    /// # Errors
    ///
    /// [`PutError`]: a §5.4 refusal, or `UNKNOWN_OUTCOME` when no answer came in time.
    pub fn put(
        &self,
        object: &[u8],
        value: &[u8],
        if_version: Option<u64>,
    ) -> Result<PutOk, PutError> {
        self.put_identified(self.identity(), object, value, if_version)
    }

    /// [`Self::put`] under request id `request`, which this client may have used already. A
    /// fresh compile, so a changed payload under a used id meets the dedup rules
    /// (`REQUEST_ID_REUSE`). The tenant and client are this `Db`'s own.
    ///
    /// # Errors
    ///
    /// As [`Self::put`].
    pub fn put_as(
        &self,
        request: RequestId,
        object: &[u8],
        value: &[u8],
        if_version: Option<u64>,
    ) -> Result<PutOk, PutError> {
        let identity = RequestIdentity {
            tenant: TENANT,
            client: CLIENT,
            request,
        };
        self.put_identified(identity, object, value, if_version)
    }

    fn put_identified(
        &self,
        identity: RequestIdentity,
        object: &[u8],
        value: &[u8],
        if_version: Option<u64>,
    ) -> Result<PutOk, PutError> {
        let call = ClientCall::Put {
            identity,
            object: Bytes::copy_from_slice(object),
            value: Bytes::copy_from_slice(value),
            if_version,
            remaining_millis: millis(self.timeouts.put),
        };
        self.txn(identity, call)
    }

    /// Send `request` again, unchanged: same identity, same payload.
    ///
    /// # Errors
    ///
    /// As [`Self::put`].
    pub fn resend(&self, request: TxnRequest) -> Result<PutOk, PutError> {
        let identity = request.identity;
        self.txn(identity, ClientCall::Resend { request })
    }

    fn txn(&self, identity: RequestIdentity, call: ClientCall) -> Result<PutOk, PutError> {
        let sent = match &call {
            ClientCall::Resend { request } => Some(request.clone()),
            _ => None,
        };
        match self.call(call, self.timeouts.put) {
            Some(Answer::Txn { result, request }) => Ok(PutOk {
                request: identity.request,
                generation: result.generation,
                owner_epoch: result.owner_epoch,
                seq: result.seq,
                outcome: result.outcome,
                durability: result.durability,
                sent: Box::new(request),
            }),
            Some(Answer::Error { error, request }) => Err(PutError {
                error,
                request: request.or(sent).map(Box::new),
            }),
            Some(other) => Err(PutError {
                error: ApiError::host(&format!("a write was answered with {other:?}")),
                request: sent.map(Box::new),
            }),
            None => Err(PutError {
                error: ApiError::new(
                    ErrorKind::UnknownOutcome,
                    format!(
                        "no answer in {:?} for request {}",
                        self.timeouts.put, identity.request.0
                    ),
                ),
                request: sent.map(Box::new),
            }),
        }
    }

    /// Read `object` at the publication barrier.
    ///
    /// # Errors
    ///
    /// [`ApiError`]: a §5.4 refusal, or `UNAVAILABLE` when no answer came in time.
    pub fn get(&self, object: &[u8]) -> Result<GetOk, ApiError> {
        let identity = self.identity();
        let call = ClientCall::Get {
            identity,
            object: Bytes::copy_from_slice(object),
        };
        match self.call(call, self.timeouts.read) {
            Some(Answer::Read {
                outcome: ReadServiceOutcome::Rejected(kind),
                ..
            }) => Err(ApiError::new(kind, "the read was refused")),
            Some(Answer::Read {
                outcome,
                value,
                generation,
                at,
            }) => Ok(GetOk {
                value,
                generation,
                at,
                waited: outcome == ReadServiceOutcome::WaitedAtBarrier,
            }),
            Some(Answer::Error { error, .. }) => Err(error),
            Some(other) => Err(ApiError::host(&format!(
                "a read was answered with {other:?}"
            ))),
            None => Err(ApiError::new(
                ErrorKind::Unavailable,
                format!("no read answer in {:?}", self.timeouts.read),
            )),
        }
    }

    /// What became of this client's request `request`.
    ///
    /// # Errors
    ///
    /// [`ApiError`]: a refusal, or `UNAVAILABLE` when no answer came in time.
    pub fn status(
        &self,
        request: RequestId,
        generation: Option<Generation>,
    ) -> Result<TxnStatus, ApiError> {
        let identity = RequestIdentity {
            tenant: TENANT,
            client: CLIENT,
            request,
        };
        match self.call(
            ClientCall::Status {
                identity,
                generation,
            },
            self.timeouts.read,
        ) {
            Some(Answer::Status(status)) => Ok(status),
            Some(Answer::Error { error, .. }) => Err(error),
            Some(other) => Err(ApiError::host(&format!(
                "a status query was answered with {other:?}"
            ))),
            None => Err(ApiError::new(
                ErrorKind::Unavailable,
                format!("no status answer in {:?}", self.timeouts.read),
            )),
        }
    }

    /// Each node's state for partition 1, in node order. A node that does not answer within
    /// the read timeout is left out.
    #[must_use]
    pub fn node_status(&self) -> Vec<NodeStatus> {
        self.nodes
            .iter()
            .filter_map(|handle| {
                let (reply, answer) = mpsc::channel();
                handle
                    .send(Msg::Inspect {
                        partition: PARTITION,
                        reply,
                    })
                    .ok()?;
                answer.recv_timeout(self.timeouts.read).ok()
            })
            .collect()
    }

    /// The peer links, to hold and heal.
    #[must_use]
    pub fn links(&self) -> &Arc<Links> {
        &self.links
    }

    /// Milliseconds since this `Db` opened.
    #[must_use]
    pub fn now(&self) -> rdb_core::contracts::time::Tick {
        self.clock.now()
    }

    /// Stop every node and every control call. The control store stays the caller's.
    pub fn shutdown(&mut self) {
        for handle in &mut self.nodes {
            handle.stop();
        }
        self.control.shutdown();
        tracing::info!("db_shutdown");
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_names_are_screaming_snake() {
        assert_eq!(wire_name(ErrorKind::ProtectionPaused), "PROTECTION_PAUSED");
        assert_eq!(wire_name(ErrorKind::UnknownOutcome), "UNKNOWN_OUTCOME");
        assert_eq!(wire_name(ErrorKind::Unavailable), "UNAVAILABLE");
    }
}
