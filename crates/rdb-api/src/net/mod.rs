//! Links between rDB processes (M10 S2, arch-s2 "Network (W4)").
//!
//! A [`Net`] is the remote backend of a process's [`Links`]: a message for a node this process
//! does not run goes here, is turned into a [`wire::WireMsg`] and sent on that node's link. A
//! link is one tonic bidirectional stream per pair of processes, under mutual TLS from
//! `config-grpc`. Between nodes the higher id dials the lower; an app dials every node and is
//! never dialled.
//!
//! # Handshake
//!
//! The dialler sends [`wire::Hello`]: its [`PROTOCOL_VERSION`], the label it claims and its
//! boot. The listener refuses, naming both sides, a version other than its own, and a claimed
//! label other than the one its certificate's URI SAN names: the label comes from the
//! certificate, never from the claim (S4 F1). A dialler pins the node it dials by its DNS
//! name, so TLS itself proves it reached that node. A link that comes up reports the peer's
//! boot ([`Net::boot_of`], `net_peer_booted`).
//!
//! # Replies
//!
//! A client call's `Sender` stays with the caller. Its connection mints a correlation, the
//! call travels with it, and the answer comes back on the same connection as
//! [`wire::Frame::Reply`]. A link that goes down drops every `Sender` waiting on it, so a
//! caller sees its channel close, as it would if a node thread in its own process stopped.
//!
//! # Faults
//!
//! [`faults::Faults`], read from a file, apply after TLS: a hold delays every frame on a
//! link, both ways; a dropped request or reply is lost. Every client call a node receives is
//! logged `net_client_request{n, client, request}`.

pub mod certs;
pub mod faults;
pub mod wire;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use rdb_core::contracts::ids::{BootId, NodeId};
use rdb_core::contracts::transport::LinkFault;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;

use crate::db::ApiError;
use crate::host::{Answer, Client, Msg, NodeStatus};
use crate::transport::Links;

use self::faults::Faults;
use self::wire::{Frame, Hello, Inbound, Label, Outbound, WireAnswer, WireMsg, WireReply};

/// The wire version this build speaks. A peer that speaks another is refused at the
/// handshake, naming both. Move it whenever a type in [`wire`] changes.
pub const PROTOCOL_VERSION: u32 = 1;

/// The largest frame either side accepts. A catch-up frame carries whole records.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// How long a handshake may take before the link is dropped.
const HANDSHAKE: Duration = Duration::from_secs(5);

/// Frames kept for a node whose link is not up yet, before the oldest is dropped.
const QUEUE_CAP: usize = 10_000;

/// Generated stubs for `proto/rdb_net.proto`.
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod pb {
    tonic::include_proto!("rdb.net.v1");
}

use pb::rdb_net_client::RdbNetClient;
use pb::rdb_net_server::{RdbNet, RdbNetServer};
use pb::Envelope;

/// What one process's links are.
#[derive(Debug, Clone)]
pub struct NetConfig {
    /// Who this process is. Its certificate must name the same label.
    pub label: Label,
    /// This process's boot, sent in the handshake. An app sends `BootId(0)`.
    pub boot: BootId,
    /// This process's TLS material.
    pub tls: config_grpc::MtlsConfig,
    /// Every node this process may send to.
    pub peers: BTreeSet<NodeId>,
    /// The nodes this process dials, by `host:port`. The others dial it.
    pub dial: BTreeMap<NodeId, String>,
    /// The version sent in the handshake: [`PROTOCOL_VERSION`] unless a walk overrides it
    /// (`rdb_node --protocol-version N`).
    pub protocol_version: u32,
    /// The faults this process applies.
    pub faults: Arc<Faults>,
}

impl NetConfig {
    /// The dial set for node `me` of a three-node run: the lower ids, by `addrs`.
    #[must_use]
    pub fn dial_lower(me: NodeId, addrs: &BTreeMap<NodeId, String>) -> BTreeMap<NodeId, String> {
        addrs
            .iter()
            .filter(|(node, _)| node.0 < me.0)
            .map(|(node, addr)| (*node, addr.clone()))
            .collect()
    }
}

/// How a node's link stands, for an operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkState {
    /// Up, with the peer's boot.
    Up(BootId),
    /// Not up yet; the last dial error, if any.
    Waiting(Option<String>),
    /// The peer refused the handshake, or this side refused the peer's, and why.
    Refused(String),
}

/// A proxied call's waiter.
enum Waiter {
    Answer(mpsc::Sender<Answer>),
    Status(mpsc::Sender<NodeStatus>),
}

/// One live connection.
struct Conn {
    id: u64,
    peer: Label,
    tx: UnboundedSender<Envelope>,
    pending: Mutex<BTreeMap<u64, Waiter>>,
    next: AtomicU64,
    /// Frames a hold keeps back, oldest first.
    held: Mutex<VecDeque<Frame>>,
    faults: Arc<Faults>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Conn {
    /// Send `frame`, unless a hold keeps it back. Order is kept: once anything is held, every
    /// later frame waits behind it.
    fn out(&self, frame: Frame) {
        let mut held = lock(&self.held);
        if !held.is_empty() || self.faults.holds(&self.peer) {
            held.push_back(frame);
            return;
        }
        self.write(&frame);
    }

    fn write(&self, frame: &Frame) {
        let body = wire::encode(frame);
        if self
            .tx
            .send(Envelope { body: body.into() })
            .is_err()
        {
            tracing::debug!(peer = %self.peer, conn = self.id, "net_write_after_close");
        }
    }

    /// Send what a hold kept, if the hold is lifted.
    fn release(&self) {
        let mut held = lock(&self.held);
        if held.is_empty() || self.faults.holds(&self.peer) {
            return;
        }
        let released = held.len();
        while let Some(frame) = held.pop_front() {
            self.write(&frame);
        }
        tracing::info!(peer = %self.peer, released, "net_hold_released_out");
    }

    fn correlation(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// A node's route: its live connection, or what waits for one.
struct Route {
    conn: Option<Arc<Conn>>,
    queue: VecDeque<Frame>,
    boot: Option<BootId>,
    refused: Option<String>,
    last_error: Option<String>,
}

struct Shared {
    label: Label,
    boot: BootId,
    protocol_version: u32,
    faults: Arc<Faults>,
    links: Weak<Links>,
    routes: Mutex<BTreeMap<NodeId, Route>>,
    conns: Mutex<BTreeMap<u64, Arc<Conn>>>,
    next_conn: AtomicU64,
    rt: tokio::runtime::Handle,
    /// The span `Net::start` ran in. Every task the links spawn runs in it, so a line logged on
    /// a link lands where the process (or a row) logs its own.
    span: tracing::Span,
    #[allow(clippy::type_complexity)]
    on_boot: Mutex<Option<Box<dyn Fn(NodeId, BootId) + Send + Sync>>>,
}

/// The remote backend of one process's [`Links`].
pub struct Net {
    shared: Arc<Shared>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    addr: Option<std::net::SocketAddr>,
}

impl std::fmt::Debug for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Net")
            .field("label", &self.shared.label)
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl Net {
    /// Start this process's links: serve on `listener` (a node; `None` for an app), dial the
    /// nodes `config.dial` names, and attach to `links`, so a message for a node `links` does
    /// not hold goes out here. Call inside a tokio runtime.
    ///
    /// # Errors
    ///
    /// The listener's address could not be read.
    pub fn start(
        config: NetConfig,
        links: &Arc<Links>,
        listener: Option<tokio::net::TcpListener>,
    ) -> Result<Arc<Self>, String> {
        let shared = Arc::new(Shared {
            label: config.label.clone(),
            boot: config.boot,
            protocol_version: config.protocol_version,
            faults: Arc::clone(&config.faults),
            links: Arc::downgrade(links),
            routes: Mutex::new(
                config
                    .peers
                    .iter()
                    .map(|node| {
                        (
                            *node,
                            Route {
                                conn: None,
                                queue: VecDeque::new(),
                                boot: None,
                                refused: None,
                                last_error: None,
                            },
                        )
                    })
                    .collect(),
            ),
            conns: Mutex::new(BTreeMap::new()),
            next_conn: AtomicU64::new(0),
            rt: tokio::runtime::Handle::current(),
            span: tracing::Span::current(),
            on_boot: Mutex::new(None),
        });
        let mut tasks = Vec::new();
        let mut addr = None;
        if let Some(listener) = listener {
            addr = Some(
                listener
                    .local_addr()
                    .map_err(|error| format!("listener address: {error}"))?,
            );
            let service = RdbNetServer::new(Service {
                shared: Arc::clone(&shared),
            })
            .max_decoding_message_size(MAX_FRAME)
            .max_encoding_message_size(MAX_FRAME);
            let tls = config.tls.server_tls_config();
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let label = config.label.clone();
            tasks.push(tokio::spawn(async move {
                let served = tonic::transport::Server::builder()
                    .tls_config(tls)
                    .map(|mut builder| builder.add_service(service));
                let result = match served {
                    Ok(router) => router.serve_with_incoming(incoming).await,
                    Err(error) => Err(error),
                };
                if let Err(error) = result {
                    tracing::error!(%label, %error, "net_serve_failed");
                }
            }
            .instrument(shared.span.clone())));
            tracing::info!(label = %config.label, addr = ?addr, "net_listening");
        }
        for (node, address) in &config.dial {
            tasks.push(tokio::spawn(
                dial(
                    Arc::clone(&shared),
                    *node,
                    address.clone(),
                    config.tls.clone(),
                )
                .instrument(shared.span.clone()),
            ));
        }
        {
            let shared = Arc::clone(&shared);
            let span = shared.span.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(faults::POLL).await;
                    let conns: Vec<Arc<Conn>> = lock(&shared.conns).values().cloned().collect();
                    for conn in conns {
                        conn.release();
                    }
                }
            }
            .instrument(span)));
        }
        let net = Arc::new(Self {
            shared,
            tasks: Mutex::new(tasks),
            addr,
        });
        links.attach(Arc::clone(&net));
        Ok(net)
    }

    /// The address this process serves on, for a node.
    #[must_use]
    pub const fn addr(&self) -> Option<std::net::SocketAddr> {
        self.addr
    }

    /// Call `observer(node, boot)` whenever a node's link comes up at a boot not seen before
    /// on it. The owner's host uses it to heal a restarted peer (arch-s2, W5).
    pub fn on_peer_booted(&self, observer: impl Fn(NodeId, BootId) + Send + Sync + 'static) {
        *lock(&self.shared.on_boot) = Some(Box::new(observer));
    }

    /// The boot node `node` reported at its last handshake with this process.
    #[must_use]
    pub fn boot_of(&self, node: NodeId) -> Option<BootId> {
        lock(&self.shared.routes).get(&node).and_then(|route| route.boot)
    }

    /// How each node's link stands.
    #[must_use]
    pub fn states(&self) -> BTreeMap<NodeId, LinkState> {
        lock(&self.shared.routes)
            .iter()
            .map(|(node, route)| {
                let state = match (&route.conn, &route.refused) {
                    (Some(_), _) => {
                        LinkState::Up(route.boot.expect("a link that is up has a boot"))
                    }
                    (None, Some(reason)) => LinkState::Refused(reason.clone()),
                    (None, None) => LinkState::Waiting(route.last_error.clone()),
                };
                (*node, state)
            })
            .collect()
    }

    /// Wait until every node in `nodes` has its link up, or `within` passes, or one of them
    /// refuses: a refusal does not heal by waiting.
    ///
    /// # Errors
    ///
    /// One line per node not up, saying why: refused (naming both sides), or the last dial
    /// error.
    pub async fn wait_up(&self, nodes: &[NodeId], within: Duration) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let states = self.states();
            let down: Vec<String> = nodes
                .iter()
                .filter_map(|node| match states.get(node) {
                    Some(LinkState::Up(_)) => None,
                    Some(LinkState::Refused(reason)) => {
                        Some(format!("node {}: refused: {reason}", node.0))
                    }
                    Some(LinkState::Waiting(Some(error))) => {
                        Some(format!("node {}: not reachable: {error}", node.0))
                    }
                    Some(LinkState::Waiting(None)) => {
                        Some(format!("node {}: not reachable yet", node.0))
                    }
                    None => Some(format!("node {}: not a peer of this process", node.0)),
                })
                .collect();
            if down.is_empty() {
                return Ok(());
            }
            let refused = nodes
                .iter()
                .any(|node| matches!(states.get(node), Some(LinkState::Refused(_))));
            if refused || tokio::time::Instant::now() >= deadline {
                return Err(down.join("; "));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Send `msg` to remote node `to`.
    ///
    /// A peer message for a node whose link is not up waits for it ([`QUEUE_CAP`] frames,
    /// then the oldest is dropped, logged). A client call or an `Inspect` does not wait: with
    /// no link up it is `Unreachable`, and its `Sender` is dropped.
    ///
    /// # Errors
    ///
    /// [`LinkFault::Unreachable`] for a node that is not a peer, a local-only message, or a
    /// call with no link up.
    pub fn send(&self, to: NodeId, msg: Msg) -> Result<(), LinkFault> {
        let outbound = match wire::outbound(to, msg).map_err(|refused| *refused) {
            Ok(outbound) => outbound,
            Err((error @ wire::WireError::LocalOnly(_), _)) => {
                tracing::error!(to = to.0, %error, "net_send_refused");
                return Err(LinkFault::Unreachable);
            }
            Err((error, Msg::Client(Client { reply, .. }))) => {
                // The caller gets the reason, as an in-process compile would refuse it.
                let _ = reply.send(Answer::Error {
                    error: ApiError::invalid(error.to_string()),
                    request: None,
                });
                return Ok(());
            }
            Err((error, _)) => {
                tracing::error!(to = to.0, %error, "net_send_refused");
                return Err(LinkFault::Unreachable);
            }
        };
        let mut routes = lock(&self.shared.routes);
        let Some(route) = routes.get_mut(&to) else {
            tracing::warn!(to = to.0, "net_send_not_a_peer");
            return Err(LinkFault::Unreachable);
        };
        match outbound {
            Outbound::Plain(wire) => {
                let frame = Frame::Msg(wire);
                match &route.conn {
                    Some(conn) => conn.out(frame),
                    None => {
                        route.queue.push_back(frame);
                        if route.queue.len() > QUEUE_CAP {
                            route.queue.pop_front();
                            tracing::warn!(to = to.0, cap = QUEUE_CAP, "net_queue_dropped_oldest");
                        }
                    }
                }
                Ok(())
            }
            Outbound::Client {
                to,
                partition,
                call,
                reply,
            } => {
                let Some(conn) = &route.conn else {
                    tracing::info!(to = to.0, call = call.name(), "net_call_unreachable");
                    return Err(LinkFault::Unreachable);
                };
                let correlation = conn.correlation();
                lock(&conn.pending).insert(correlation, Waiter::Answer(reply));
                conn.out(Frame::Msg(WireMsg::Client {
                    to,
                    partition,
                    call,
                    reply: correlation,
                }));
                Ok(())
            }
            Outbound::Inspect {
                to,
                partition,
                reply,
            } => {
                let Some(conn) = &route.conn else {
                    return Err(LinkFault::Unreachable);
                };
                let correlation = conn.correlation();
                lock(&conn.pending).insert(correlation, Waiter::Status(reply));
                conn.out(Frame::Msg(WireMsg::Inspect {
                    to,
                    partition,
                    reply: correlation,
                }));
                Ok(())
            }
        }
    }
}

impl Drop for Net {
    fn drop(&mut self) {
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
        let conns: Vec<Arc<Conn>> = std::mem::take(&mut *lock(&self.shared.conns))
            .into_values()
            .collect();
        for conn in conns {
            lock(&conn.pending).clear();
        }
    }
}

/// Bring `conn` up as node `node`'s route, at `boot`; send what waited for it.
fn install(shared: &Arc<Shared>, node: NodeId, conn: &Arc<Conn>, boot: BootId) {
    let new_boot = {
        let mut routes = lock(&shared.routes);
        let Some(route) = routes.get_mut(&node) else {
            tracing::warn!(peer = %conn.peer, "net_link_from_unknown_node");
            return;
        };
        let previous = route.boot.replace(boot);
        route.refused = None;
        route.last_error = None;
        if let Some(old) = route.conn.replace(Arc::clone(conn)) {
            tracing::info!(node = node.0, old = old.id, new = conn.id, "net_link_replaced");
            close(shared, &old, "replaced by a newer link");
        }
        let queued = route.queue.len();
        while let Some(frame) = route.queue.pop_front() {
            conn.out(frame);
        }
        tracing::info!(
            node = node.0,
            conn = conn.id,
            boot = boot.0,
            queued,
            "net_link_up"
        );
        previous != Some(boot)
    };
    if new_boot {
        tracing::info!(node = node.0, boot = boot.0, "net_peer_booted");
        if let Some(observer) = &*lock(&shared.on_boot) {
            observer(node, boot);
        }
    }
}

/// Take `conn` down: forget it, drop every waiter on it, and free its route.
fn close(shared: &Shared, conn: &Arc<Conn>, reason: &str) {
    if lock(&shared.conns).remove(&conn.id).is_none() {
        return;
    }
    let dropped = std::mem::take(&mut *lock(&conn.pending)).len();
    if let Label::Node(node) = &conn.peer {
        // `install` holds the routes lock when it replaces a link, so only free a route that
        // still points here; a replaced link's route is already the new one.
        if let Ok(mut routes) = shared.routes.try_lock() {
            if let Some(route) = routes.get_mut(node) {
                if route.conn.as_ref().is_some_and(|c| c.id == conn.id) {
                    route.conn = None;
                }
            }
        } else {
            let mut routes = lock(&shared.routes);
            if let Some(route) = routes.get_mut(node) {
                if route.conn.as_ref().is_some_and(|c| c.id == conn.id) {
                    route.conn = None;
                }
            }
        }
    }
    tracing::info!(peer = %conn.peer, conn = conn.id, dropped, reason, "net_link_down");
}

fn new_conn(shared: &Shared, peer: Label, tx: UnboundedSender<Envelope>) -> Arc<Conn> {
    let conn = Arc::new(Conn {
        id: shared.next_conn.fetch_add(1, Ordering::Relaxed) + 1,
        peer,
        tx,
        pending: Mutex::new(BTreeMap::new()),
        next: AtomicU64::new(0),
        held: Mutex::new(VecDeque::new()),
        faults: Arc::clone(&shared.faults),
    });
    lock(&shared.conns).insert(conn.id, Arc::clone(&conn));
    conn
}

/// Read `inbound` until it ends, handling each frame in order. A hold keeps frames back here
/// too, so a held link is held both ways.
async fn read(shared: Arc<Shared>, conn: Arc<Conn>, mut inbound: Streaming<Envelope>) -> String {
    let mut held: VecDeque<Frame> = VecDeque::new();
    let mut tick = tokio::time::interval(faults::POLL);
    loop {
        tokio::select! {
            next = inbound.next() => {
                let frame = match next {
                    None => return "the peer closed the link".to_owned(),
                    Some(Err(status)) => return format!("link error: {status}"),
                    Some(Ok(envelope)) => match wire::decode(&envelope.body) {
                        Ok(frame) => frame,
                        Err(error) => return error.to_string(),
                    },
                };
                if !held.is_empty() || shared.faults.holds(&conn.peer) {
                    held.push_back(frame);
                    continue;
                }
                if let Err(reason) = handle(&shared, &conn, frame) {
                    return reason;
                }
            }
            _ = tick.tick() => {
                if held.is_empty() || shared.faults.holds(&conn.peer) {
                    continue;
                }
                let released = held.len();
                while let Some(frame) = held.pop_front() {
                    if let Err(reason) = handle(&shared, &conn, frame) {
                        return reason;
                    }
                }
                tracing::info!(peer = %conn.peer, released, "net_hold_released_in");
            }
        }
    }
}

/// Handle one received frame. An error ends the link.
fn handle(shared: &Arc<Shared>, conn: &Arc<Conn>, frame: Frame) -> Result<(), String> {
    match frame {
        Frame::Msg(msg) => {
            receive(shared, conn, msg);
            Ok(())
        }
        Frame::Reply { correlation, reply } => {
            let Some(waiter) = lock(&conn.pending).remove(&correlation) else {
                tracing::warn!(peer = %conn.peer, correlation, "net_reply_unclaimed");
                return Ok(());
            };
            match (waiter, reply) {
                (Waiter::Answer(tx), WireReply::Answer(answer)) => {
                    let _ = tx.send(answer.land());
                }
                (Waiter::Status(tx), WireReply::Status(status)) => {
                    match NodeStatus::try_from(status) {
                        Ok(status) => {
                            let _ = tx.send(status);
                        }
                        Err(error) => {
                            tracing::error!(peer = %conn.peer, %error, "net_status_undecodable");
                        }
                    }
                }
                // The node dropped the waiter unanswered: dropping ours closes the caller's
                // channel, as in one process.
                (_, WireReply::Dropped) => {}
                (_, other) => {
                    return Err(format!(
                        "reply {correlation} does not match its call: {other:?}"
                    ))
                }
            }
            Ok(())
        }
        Frame::Hello(_) | Frame::Welcome(_) | Frame::Refused { .. } => {
            Err("a handshake frame after the handshake".to_owned())
        }
    }
}

/// Deliver a received message to the local node it names.
fn receive(shared: &Arc<Shared>, conn: &Arc<Conn>, msg: WireMsg) {
    let client = match &msg {
        WireMsg::Client { call, .. } => {
            let n = shared.faults.next_client_request();
            let identity = call.identity();
            tracing::info!(
                n,
                client = identity.client.0,
                request = identity.request.0,
                call = call.name(),
                from = %conn.peer,
                to = msg.to().0,
                "net_client_request"
            );
            if shared.faults.drops_request(n) {
                tracing::info!(n, "net_fault_drop_request");
                return;
            }
            Some(n)
        }
        _ => None,
    };
    let name = msg.name();
    let to = msg.to();
    let inbound = match wire::inbound(msg) {
        Ok(inbound) => inbound,
        Err(error) => {
            tracing::error!(peer = %conn.peer, msg = name, %error, "net_msg_undecodable");
            return;
        }
    };
    let Some(links) = shared.links.upgrade() else {
        return;
    };
    match inbound {
        Inbound::Plain(to, msg) => {
            if let Err(fault) = links.post_local(to, msg) {
                tracing::warn!(to = to.0, msg = name, ?fault, "net_deliver_unreachable");
            }
        }
        Inbound::Client {
            to,
            partition,
            call,
            reply,
        } => {
            let (tx, rx) = mpsc::channel();
            let posted = links.post_local(
                to,
                Msg::Client(Client {
                    partition,
                    call,
                    reply: tx,
                }),
            );
            if posted.is_err() {
                let error = ApiError::host(&format!("node {} is not served by {}", to.0, shared.label));
                conn.out(Frame::Reply {
                    correlation: reply,
                    reply: WireReply::Answer(WireAnswer::carry(&Answer::Error {
                        error,
                        request: None,
                    })),
                });
                return;
            }
            let conn = Arc::clone(conn);
            let faults = Arc::clone(&shared.faults);
            let n = client.expect("a client message was counted");
            let span = shared.span.clone();
            shared.rt.spawn_blocking(move || {
                let _entered = span.enter();
                let reply_frame = match rx.recv() {
                    Ok(answer) => {
                        if faults.drops_reply(n) {
                            tracing::info!(n, "net_fault_drop_reply");
                            return;
                        }
                        WireReply::Answer(WireAnswer::carry(&answer))
                    }
                    Err(_) => WireReply::Dropped,
                };
                conn.out(Frame::Reply {
                    correlation: reply,
                    reply: reply_frame,
                });
            });
        }
        Inbound::Inspect {
            to,
            partition,
            reply,
        } => {
            let (tx, rx) = mpsc::channel();
            if links
                .post_local(to, Msg::Inspect { partition, reply: tx })
                .is_err()
            {
                conn.out(Frame::Reply {
                    correlation: reply,
                    reply: WireReply::Dropped,
                });
                return;
            }
            let conn = Arc::clone(conn);
            let span = shared.span.clone();
            shared.rt.spawn_blocking(move || {
                let _entered = span.enter();
                let reply_frame = match rx.recv() {
                    Ok(status) => WireReply::Status(wire::WireNodeStatus::from(&status)),
                    Err(_) => WireReply::Dropped,
                };
                conn.out(Frame::Reply {
                    correlation: reply,
                    reply: reply_frame,
                });
            });
        }
    }
    let _ = to;
}

fn own_hello(shared: &Shared) -> Hello {
    Hello {
        protocol_version: shared.protocol_version,
        label: shared.label.clone(),
        boot: shared.boot,
    }
}

fn envelope(frame: &Frame) -> Envelope {
    Envelope {
        body: wire::encode(frame).into(),
    }
}

/// Dial node `node` at `address` for as long as this process runs, bringing the link up
/// again after each loss.
async fn dial(shared: Arc<Shared>, node: NodeId, address: String, tls: config_grpc::MtlsConfig) {
    let peer = Label::Node(node);
    let mut backoff = Duration::from_millis(100);
    let mut reported: Option<String> = None;
    loop {
        let reason = match dial_once(&shared, node, &address, &tls).await {
            Ok(reason) => {
                backoff = Duration::from_millis(100);
                reason
            }
            Err(reason) => reason,
        };
        {
            let mut routes = lock(&shared.routes);
            if let Some(route) = routes.get_mut(&node) {
                if route.refused.is_none() {
                    route.last_error = Some(reason.clone());
                }
            }
        }
        if reported.as_ref() != Some(&reason) {
            tracing::info!(%peer, %address, %reason, "net_dial_failed");
            reported = Some(reason);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

/// One dial: connect, handshake, then read until the link ends. `Ok` with why it ended once it
/// was up; `Err` with why it never came up.
async fn dial_once(
    shared: &Arc<Shared>,
    node: NodeId,
    address: &str,
    tls: &config_grpc::MtlsConfig,
) -> Result<String, String> {
    let endpoint = tonic::transport::Endpoint::from_shared(format!("https://{address}"))
        .map_err(|error| format!("bad address {address}: {error}"))?
        .tls_config(tls.client_tls_config_for(&certs::node_domain(node)))
        .map_err(|error| format!("tls config: {error}"))?
        .connect_timeout(Duration::from_secs(2));
    let channel = endpoint
        .connect()
        .await
        .map_err(|error| format!("connect: {}", error_chain(&error)))?;
    let mut client = RdbNetClient::new(channel)
        .max_decoding_message_size(MAX_FRAME)
        .max_encoding_message_size(MAX_FRAME);
    let (tx, rx) = unbounded_channel();
    tx.send(envelope(&Frame::Hello(own_hello(shared))))
        .map_err(|_| "the outbound stream closed".to_owned())?;
    let mut inbound = client
        .link(Request::new(UnboundedReceiverStream::new(rx)))
        .await
        .map_err(|status| format!("link: {}", status.message()))?
        .into_inner();
    let first = tokio::time::timeout(HANDSHAKE, inbound.next())
        .await
        .map_err(|_| format!("no handshake answer in {HANDSHAKE:?}"))?;
    let frame = match first {
        Some(Ok(envelope)) => wire::decode(&envelope.body).map_err(|error| error.to_string())?,
        Some(Err(status)) => return Err(format!("handshake: {}", status.message())),
        None => return Err("the peer closed the link during the handshake".to_owned()),
    };
    let welcome = match frame {
        Frame::Welcome(welcome) => welcome,
        Frame::Refused { reason } => {
            tracing::warn!(peer = node.0, %reason, "net_handshake_refused");
            refuse(shared, node, &reason);
            return Err(format!("refused: {reason}"));
        }
        other => return Err(format!("handshake answered with {other:?}")),
    };
    if welcome.protocol_version != shared.protocol_version {
        let reason = format!(
            "node-{} speaks protocol_version {}, {} speaks {}",
            node.0, welcome.protocol_version, shared.label, shared.protocol_version
        );
        tracing::warn!(peer = node.0, %reason, "net_handshake_refused");
        refuse(shared, node, &reason);
        return Err(reason);
    }
    if welcome.label != Label::Node(node) {
        return Err(format!(
            "dialled node-{} and was answered by {}",
            node.0, welcome.label
        ));
    }
    let conn = new_conn(shared, Label::Node(node), tx);
    install(shared, node, &conn, welcome.boot);
    let reason = read(Arc::clone(shared), Arc::clone(&conn), inbound).await;
    close(shared, &conn, &reason);
    Ok(reason)
}

fn refuse(shared: &Shared, node: NodeId, reason: &str) {
    if let Some(route) = lock(&shared.routes).get_mut(&node) {
        route.refused = Some(reason.to_owned());
    }
}

fn error_chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

/// The listener side.
struct Service {
    shared: Arc<Shared>,
}

/// The stream a listener answers with.
type Outgoing = std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<Envelope, Status>> + Send>>;

#[tonic::async_trait]
impl RdbNet for Service {
    type LinkStream = Outgoing;

    async fn link(
        &self,
        request: Request<Streaming<Envelope>>,
    ) -> Result<Response<Self::LinkStream>, Status> {
        let certs = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("no client certificate"))?;
        let leaf = certs
            .first()
            .ok_or_else(|| Status::unauthenticated("no client certificate"))?;
        let label = certs::label_from_der(leaf.as_ref()).ok_or_else(|| {
            Status::unauthenticated("the client certificate names no single rdb label")
        })?;
        let mut inbound = request.into_inner();
        let (tx, rx) = unbounded_channel::<Envelope>();
        let shared = Arc::clone(&self.shared);
        let span = shared.span.clone();
        tokio::spawn(async move {
            let first = tokio::time::timeout(HANDSHAKE, inbound.next()).await;
            let hello = match first {
                Ok(Some(Ok(envelope))) => match wire::decode(&envelope.body) {
                    Ok(Frame::Hello(hello)) => hello,
                    Ok(other) => {
                        tracing::warn!(peer = %label, frame = ?other, "net_handshake_bad_first_frame");
                        return;
                    }
                    Err(error) => {
                        tracing::warn!(peer = %label, %error, "net_handshake_undecodable");
                        return;
                    }
                },
                _ => {
                    tracing::warn!(peer = %label, "net_handshake_missing");
                    return;
                }
            };
            if let Some(reason) = refusal(&shared, &label, &hello) {
                tracing::warn!(peer = %label, %reason, "net_handshake_refused");
                let _ = tx.send(envelope(&Frame::Refused { reason }));
                return;
            }
            let _ = tx.send(envelope(&Frame::Welcome(own_hello(&shared))));
            let conn = new_conn(&shared, label.clone(), tx);
            if let Label::Node(node) = &label {
                install(&shared, *node, &conn, hello.boot);
            } else {
                tracing::info!(peer = %label, conn = conn.id, "net_app_link_up");
            }
            let reason = read(Arc::clone(&shared), Arc::clone(&conn), inbound).await;
            close(&shared, &conn, &reason);
        }
        .instrument(span));
        let outgoing = UnboundedReceiverStream::new(rx).map(Ok::<_, Status>);
        Ok(Response::new(Box::pin(outgoing)))
    }
}

/// Why a listener refuses `hello` from a peer whose certificate names `label`, or `None`.
fn refusal(shared: &Shared, label: &Label, hello: &Hello) -> Option<String> {
    if hello.protocol_version != shared.protocol_version {
        return Some(format!(
            "{label} speaks protocol_version {}, {} speaks {}",
            hello.protocol_version, shared.label, shared.protocol_version
        ));
    }
    if hello.label != *label {
        return Some(format!(
            "claimed to be {} but its certificate names {label}",
            hello.label
        ));
    }
    None
}
