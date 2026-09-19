//! The running gossip node.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use config_core::hint::{GossipObservationSource, Liveness, ObservedPeerHint};
use config_core::identity::{ClusterId, NodeId};
use memberlist::delegate::{
    CompositeDelegate, Event, EventKind, EventSubscriber, NodeDelegate, SubscribleEventDelegate,
    VoidDelegate,
};
use memberlist::net::NetTransportOptions;
use memberlist::proto::{
    ChecksumAlgorithm, EncryptionAlgorithm, Label, MaybeResolvedAddress, Meta, SecretKey,
};
use memberlist::tokio::{TokioNetTransport, TokioSocketAddrResolver, TokioTcp};
use memberlist::{Memberlist, Options};
use smol_str::SmolStr;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, info, trace, warn, Instrument, Span};

use crate::config::GossipConfig;
use crate::error::{GossipError, HintDecodeError};
use crate::meta::{decode_hint, encode_hint};

/// Depth of the membership-event channel. Events only *wake* the refresher, which then
/// rebuilds the snapshot from authoritative membership, so a full channel loses nothing but
/// timeliness.
const EVENT_CHANNEL_DEPTH: usize = 256;

type GossipTransport = TokioNetTransport<SmolStr, TokioSocketAddrResolver, TokioTcp>;
type TransportOptions = NetTransportOptions<SmolStr, TokioSocketAddrResolver, TokioTcp>;

/// `CompositeDelegate` type parameters are ordered alive, conflict, event, merge, node, ping.
type GossipDelegate = CompositeDelegate<
    SmolStr,
    SocketAddr,
    VoidDelegate<SmolStr, SocketAddr>,
    VoidDelegate<SmolStr, SocketAddr>,
    SubscribleEventDelegate<SmolStr, SocketAddr>,
    VoidDelegate<SmolStr, SocketAddr>,
    Arc<HintDelegate>,
    VoidDelegate<SmolStr, SocketAddr>,
>;

type Inner = Memberlist<GossipTransport, GossipDelegate>;

/// Publishes this node's own hint as gossip node metadata.
struct HintDelegate {
    encoded: RwLock<Vec<u8>>,
}

impl HintDelegate {
    fn new(encoded: Vec<u8>) -> Self {
        Self {
            encoded: RwLock::new(encoded),
        }
    }

    /// Replace the advertised bytes. The caller has already enforced the size budget.
    fn set(&self, encoded: Vec<u8>) {
        if let Ok(mut slot) = self.encoded.write() {
            *slot = encoded;
        }
    }

    fn current(&self) -> Meta {
        let bytes = self
            .encoded
            .read()
            .map(|slot| (*slot).clone())
            .unwrap_or_default();
        // The bytes were size-checked at encode time; fall back to empty rather than panic.
        Meta::try_from(bytes).unwrap_or_else(|_| Meta::empty())
    }
}

impl NodeDelegate for HintDelegate {
    fn node_meta(&self, _limit: usize) -> impl Future<Output = Meta> + Send {
        // Read eagerly so no lock guard is held across an await point.
        std::future::ready(self.current())
    }
}

/// Peers this node has already warned about, so a periodic refresh does not spam the log.
///
/// Every set is pruned to current membership on each refresh ([`Shared::retain_warned`]), so
/// the bookkeeping is bounded by cluster size rather than by the number of ids ever seen, and
/// a peer that leaves and comes back misconfigured warns again instead of staying silent.
#[derive(Default)]
struct Warned {
    cluster_mismatch: HashSet<SmolStr>,
    decode_failure: HashSet<SmolStr>,
    duplicate_identity: HashSet<SmolStr>,
}

/// State shared between the public handle and the background refresher.
struct Shared {
    cluster_id: ClusterId,
    self_id: SmolStr,
    self_node_id: NodeId,
    peers: RwLock<Arc<Vec<ObservedPeerHint>>>,
    warned: Mutex<Warned>,
}

impl Shared {
    fn snapshot(&self) -> Vec<ObservedPeerHint> {
        // Clone the `Arc` under the lock, clone the `Vec` after releasing it: the read guard
        // is held for a pointer copy, not for a deep copy proportional to cluster size.
        let current = self
            .peers
            .read()
            .map(|slot| Arc::clone(&slot))
            .unwrap_or_default();
        (*current).clone()
    }

    fn store(&self, peers: Vec<ObservedPeerHint>) {
        if let Ok(mut slot) = self.peers.write() {
            *slot = Arc::new(peers);
        }
    }

    /// `true` the first time `id` is seen for this warning kind.
    fn first_time(&self, id: &SmolStr, select: fn(&mut Warned) -> &mut HashSet<SmolStr>) -> bool {
        match self.warned.lock() {
            Ok(mut warned) => select(&mut warned).insert(id.clone()),
            Err(_) => true,
        }
    }

    /// Drop warning bookkeeping for ids that are no longer members.
    fn retain_warned(&self, live: &HashSet<SmolStr>) {
        if let Ok(mut warned) = self.warned.lock() {
            warned.cluster_mismatch.retain(|id| live.contains(id));
            warned.decode_failure.retain(|id| live.contains(id));
            warned.duplicate_identity.retain(|id| live.contains(id));
        }
    }
}

/// An advisory gossip participant.
///
/// Implements [`GossipObservationSource`], whose `peers()` is synchronous and non-blocking:
/// it clones a snapshot maintained by a background task, so the engine can poll it from the
/// Raft path without ever awaiting gossip.
///
/// # Dropping versus shutting down
///
/// [`GossipNode::shutdown`] is the supported exit: it broadcasts a leave, stops the refresher
/// and tears the transport down, and the sockets are free when it returns.
///
/// Dropping without it is a backstop for panics and tests, not an equivalent. No leave is
/// broadcast, so peers must time the node out through the failure detector, and the teardown
/// itself is detached — the port comes back shortly after the drop rather than at it, and not
/// at all if the runtime is already gone. See the [`Drop`] impl for why it cannot be
/// synchronous.
pub struct GossipNode {
    inner: Inner,
    delegate: Arc<HintDelegate>,
    shared: Arc<Shared>,
    stop: Arc<Notify>,
    worker: Mutex<Option<JoinHandle<()>>>,
    span: Span,
    cluster_id: ClusterId,
    node_id: NodeId,
    join_attempts: u32,
    join_retry_delay: Duration,
    broadcast_timeout: Duration,
}

impl fmt::Debug for GossipNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GossipNode")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("advertise_addr", self.inner.advertise_address())
            .finish_non_exhaustive()
    }
}

impl GossipNode {
    /// Start gossiping and advertise `self_hint`.
    ///
    /// Binds the gossip socket, installs the encryption key when one is configured, contacts
    /// [`GossipConfig::seeds`], and starts the background task that maintains the
    /// [`GossipObservationSource`] snapshot.
    ///
    /// Seed joins that fail are logged at `warn` and do **not** fail this call — gossip is
    /// advisory (ADR-0003).
    ///
    /// # Errors
    ///
    /// [`GossipError::HintTooLarge`] if `self_hint` does not fit the 512-byte metadata budget
    /// (checked here because `memberlist` would otherwise panic),
    /// [`GossipError::Config`] for an unusable label, and [`GossipError::Start`] if the
    /// socket cannot be bound.
    pub async fn start(
        cfg: GossipConfig,
        self_hint: ObservedPeerHint,
    ) -> Result<Self, GossipError> {
        let span = tracing::info_span!(
            "gossip",
            node_id = %cfg.node_id,
            cluster_id = %cfg.cluster_id,
            bind = %cfg.bind_addr,
        );
        let outer = span.clone();
        async move { Self::start_instrumented(cfg, self_hint, span).await }
            .instrument(outer)
            .await
    }

    async fn start_instrumented(
        cfg: GossipConfig,
        self_hint: ObservedPeerHint,
        span: Span,
    ) -> Result<Self, GossipError> {
        // Enforce the metadata budget before memberlist can panic on it.
        let encoded = encode_hint(&self_hint)?;

        let self_id = SmolStr::new(cfg.node_id.to_string());
        let mut transport_opts = TransportOptions::new(self_id.clone());
        transport_opts.add_bind_address(cfg.bind_addr);
        let transport_opts = transport_opts.maybe_advertise_address(cfg.advertise_addr);

        // The label is AES-GCM additional authenticated data when encrypting, so a node with
        // the right key but the wrong cluster id still cannot be understood.
        let label = Label::try_from(cfg.cluster_id.to_string().as_str())
            .map_err(|e| GossipError::Config(format!("gossip label: {e}")))?;

        let mut opts = Options::local()
            .with_label(label)
            .with_checksum_algo(ChecksumAlgorithm::Crc32)
            .with_probe_interval(cfg.probe_interval)
            .with_probe_timeout(cfg.probe_timeout)
            .with_gossip_interval(cfg.gossip_interval);

        let encrypted = cfg.secret_key.is_some();
        if let Some(key) = cfg.secret_key {
            opts = opts
                .with_primary_key(SecretKey::Aes256(key))
                .with_encryption_algo(EncryptionAlgorithm::NoPadding)
                .with_gossip_verify_incoming(true)
                .with_gossip_verify_outgoing(true);
        }

        let (event_delegate, subscriber) =
            SubscribleEventDelegate::<SmolStr, SocketAddr>::bounded(EVENT_CHANNEL_DEPTH);
        let delegate = Arc::new(HintDelegate::new(encoded));
        let composite = CompositeDelegate::new()
            .with_event_delegate(event_delegate)
            .with_node_delegate(delegate.clone());

        let inner: Inner = Memberlist::with_delegate(composite, transport_opts, opts)
            .await
            .map_err(|e| GossipError::Start(e.to_string()))?;

        let shared = Arc::new(Shared {
            cluster_id: cfg.cluster_id,
            self_id,
            self_node_id: cfg.node_id,
            peers: RwLock::new(Arc::new(Vec::new())),
            warned: Mutex::new(Warned::default()),
        });
        let stop = Arc::new(Notify::new());

        let worker = tokio::spawn(
            run_refresher(
                inner.clone(),
                shared.clone(),
                subscriber,
                stop.clone(),
                cfg.refresh_interval,
            )
            .instrument(span.clone()),
        );

        let node = Self {
            inner,
            delegate,
            shared,
            stop,
            worker: Mutex::new(Some(worker)),
            span,
            cluster_id: cfg.cluster_id,
            node_id: cfg.node_id,
            join_attempts: cfg.join_attempts,
            join_retry_delay: cfg.join_retry_delay,
            broadcast_timeout: cfg.broadcast_timeout,
        };

        info!(
            advertise = %node.inner.advertise_address(),
            encrypted,
            seeds = cfg.seeds.len(),
            "gossip node started"
        );

        if !cfg.seeds.is_empty() {
            node.join(&cfg.seeds).await;
        }
        refresh_snapshot(&node.inner, &node.shared).await;

        Ok(node)
    }

    /// Contact `seeds`, retrying up to [`GossipConfig::join_attempts`] times, and return how
    /// many were reached.
    ///
    /// Never fails: an unreachable seed is a `warn`, because static Raft seeds — not gossip —
    /// are what must bootstrap the cluster (ADR-0003).
    pub async fn join(&self, seeds: &[SocketAddr]) -> usize {
        let span = self.span.clone();
        async move {
            if seeds.is_empty() {
                return 0;
            }
            let attempts = self.join_attempts.max(1);
            let mut best = 0usize;

            for attempt in 1..=attempts {
                let targets = seeds.iter().copied().map(MaybeResolvedAddress::resolved);
                match self.inner.join_many(targets).await {
                    Ok(joined) => {
                        info!(joined = joined.len(), attempt, "gossip joined seeds");
                        best = best.max(joined.len());
                        break;
                    }
                    Err((joined, e)) => {
                        best = best.max(joined.len());
                        warn!(
                            joined = joined.len(),
                            seeds = seeds.len(),
                            attempt,
                            error = %e,
                            "gossip seed join incomplete (advisory, not fatal)"
                        );
                        if best == seeds.len() {
                            break;
                        }
                        if attempt < attempts {
                            tokio::time::sleep(self.join_retry_delay).await;
                        }
                    }
                }
            }

            refresh_snapshot(&self.inner, &self.shared).await;
            best
        }
        .instrument(span)
        .await
    }

    /// Re-advertise a changed local hint (new endpoints, new liveness) to the cluster.
    ///
    /// # Errors
    ///
    /// [`GossipError::HintTooLarge`] if the new hint exceeds the metadata budget — in which
    /// case the previously advertised hint is left untouched — or
    /// [`GossipError::Advertise`] if the update broadcast times out.
    pub async fn update_hint(&self, hint: ObservedPeerHint) -> Result<(), GossipError> {
        let span = self.span.clone();
        async move {
            let encoded = encode_hint(&hint)?;
            let size = encoded.len();
            self.delegate.set(encoded);
            self.inner
                .update_node(self.broadcast_timeout)
                .await
                .map_err(|e| GossipError::Advertise(e.to_string()))?;
            info!(meta_bytes = size, "gossip re-advertised local hint");
            Ok(())
        }
        .instrument(span)
        .await
    }

    /// Leave the cluster gracefully, then stop the listeners and the refresher.
    ///
    /// Idempotent and infallible: a failed leave broadcast is a `warn`, since by then the
    /// node is going away regardless.
    pub async fn shutdown(&self) {
        let span = self.span.clone();
        async move {
            self.stop.notify_one();
            let worker = self.worker.lock().ok().and_then(|mut slot| slot.take());
            if let Some(worker) = worker {
                let _ = worker.await;
            }
            if let Err(e) = self.inner.leave(self.broadcast_timeout).await {
                warn!(error = %e, "gossip leave broadcast failed");
            }
            if let Err(e) = self.inner.shutdown().await {
                warn!(error = %e, "gossip shutdown failed");
            }
            self.shared.store(Vec::new());
            info!("gossip node shut down");
        }
        .instrument(span)
        .await
    }

    /// The address peers are told to reach this node on.
    ///
    /// When [`GossipConfig::bind_addr`] used port `0`, this reports the port actually bound.
    pub fn advertise_addr(&self) -> SocketAddr {
        *self.inner.advertise_address()
    }

    /// The cluster this node gossips for.
    pub fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    /// This node's stable id.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

/// Releases the gossip sockets when a node is dropped without [`GossipNode::shutdown`].
///
/// Two things keep a `memberlist` alive, and both have to be dealt with:
///
/// 1. our refresher task owns a clone of the `memberlist` handle and loops forever;
/// 2. `memberlist` itself is self-referential — `stream_listener`, `packet_listener` and
///    `packet_handler` each capture a strong `Memberlist` clone
///    (`memberlist-core-0.8.5/src/network/stream.rs:28`, `network/packet/listener.rs:49`)
///    and exit only when `shutdown_tx` closes. Since `shutdown_tx` lives *inside* the
///    structure those tasks keep alive, `Drop for MemberlistCore` (`base.rs:284`) can never
///    run on its own, so simply dropping every handle we hold releases nothing.
///
/// So this aborts the refresher and asks the runtime to run `Memberlist::shutdown()`, which
/// closes `shutdown_tx` and tears the transport down. The listener sockets close when
/// `NetTransport::drop` (`memberlist-net-0.8.5/src/lib.rs:504`) follows.
///
/// `Drop` cannot await, hence the detached task. Off a runtime, or on one that is already
/// shutting down, the task never runs — acceptable, because that only happens when the
/// process is going away with it. Callers that need the port back deterministically must call
/// [`GossipNode::shutdown`]; `Drop` is the backstop for panics and tests.
///
/// No leave is broadcast either way, so peers time the node out through the failure detector.
impl Drop for GossipNode {
    fn drop(&mut self) {
        self.stop.notify_one();
        // If `shutdown()` already ran, the handle is `None` and this is a no-op.
        if let Some(worker) = self.worker.get_mut().ok().and_then(Option::take) {
            worker.abort();
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let inner = self.inner.clone();
            runtime.spawn(async move {
                if let Err(e) = inner.shutdown().await {
                    warn!(error = %e, "gossip shutdown on drop failed");
                }
            });
        } else {
            warn!("gossip node dropped outside a Tokio runtime; sockets close with the process");
        }
    }
}

impl GossipObservationSource for GossipNode {
    /// A snapshot of peers observed so far, excluding this node.
    ///
    /// Hints whose `cluster_id` differs from ours are still returned — rejecting them is the
    /// engine's `validate_hint` decision, not gossip's — but each mismatching peer is logged
    /// at `warn` once.
    ///
    /// Fields come from the peer's own advertised hint and are passed through verbatim, with
    /// one exception: `liveness` is overwritten with this node's local observation. In
    /// particular an **empty `peer_endpoint` is reported empty**. Substituting the peer's
    /// gossip address would be wrong — that is a different plane, on a different port, with a
    /// different key — so an unset endpoint stays unset and the engine rejects the hint.
    ///
    /// # Liveness fidelity
    ///
    /// Only [`Liveness::Alive`] and [`Liveness::Dead`] are ever reported, and the mapping is:
    ///
    /// | `memberlist` state | reported |
    /// |---|---|
    /// | `Alive` | [`Liveness::Alive`] |
    /// | `Suspect` | [`Liveness::Alive`] |
    /// | `Dead` | [`Liveness::Dead`] |
    /// | `Left` | [`Liveness::Dead`] |
    ///
    /// `memberlist` 0.8.5 keeps the authoritative per-node state in a private
    /// `LocalNodeState.state` field (`memberlist-core-0.8.5/src/state.rs:38-44`); the
    /// `NodeState` handed out by `members()` carries the value it had when the node was first
    /// declared alive and is never updated. The only honest public signal is therefore
    /// membership in `online_members()`, which filters on `!dead_or_left()`
    /// (`api.rs:116-127`), and `dead_or_left()` is exactly `state == Dead || state == Left`
    /// (`state.rs:68-70`).
    ///
    /// **Residual risk:** `Suspect` is not observable, so a peer that has started missing
    /// probes is advertised as healthy for the whole suspicion window (`memberlist` escalates
    /// `Suspect` to `Dead` after roughly `suspicion_mult × log(N+1) × probe_interval`). This
    /// deviates from spec §5.2, which lists `suspect` among the advertised observations; the
    /// deviation is recorded in ADR-0003. It is tolerable only because the signal is
    /// advisory: a hint is a *candidate* endpoint that must still pass mTLS identity and
    /// cluster/node-id binding, and a `Dead` observation is telemetry that never removes a
    /// voter (ADR-0003). Nothing in rEtcd may treat `Alive` here as proof of health.
    fn peers(&self) -> Vec<ObservedPeerHint> {
        self.shared.snapshot()
    }
}

fn liveness_str(liveness: Liveness) -> &'static str {
    match liveness {
        Liveness::Alive => "alive",
        Liveness::Suspect => "suspect",
        Liveness::Dead => "dead",
        Liveness::Left => "left",
    }
}

/// Background task: rebuild the snapshot on every membership event, and at least every
/// `refresh_interval` so silent state changes cannot make it arbitrarily stale.
async fn run_refresher(
    inner: Inner,
    shared: Arc<Shared>,
    subscriber: EventSubscriber<SmolStr, SocketAddr>,
    stop: Arc<Notify>,
    refresh_interval: Duration,
) {
    loop {
        tokio::select! {
            biased;
            () = stop.notified() => break,
            event = subscriber.recv() => match event {
                Ok(event) => log_event(&event),
                // The delegate was dropped: nothing more will arrive.
                Err(_) => break,
            },
            () = tokio::time::sleep(refresh_interval) => {}
        }
        refresh_snapshot(&inner, &shared).await;
    }
    debug!("gossip observation refresher stopped");
}

/// Log a membership event.
///
/// Logging is all these events are used for. They wake the refresher, which then rebuilds the
/// snapshot from `members()`/`online_members()`; no `liveness` is derived from an event,
/// because `memberlist` raises one `Leave` event for both a graceful leave and a
/// failure-detected death and does not say which. The `event_kind` field below is the raw
/// event, deliberately not dressed up as a liveness verdict.
fn log_event(event: &Event<SmolStr, SocketAddr>) {
    let state = event.node_state();
    let peer_node_id = state.id().as_str();
    let peer_addr = state.address();
    match event.kind() {
        EventKind::Join => {
            debug!(peer_node_id, peer_addr = %peer_addr, event_kind = "join", "gossip peer joined")
        }
        EventKind::Leave => debug!(
            peer_node_id,
            peer_addr = %peer_addr,
            event_kind = "leave",
            "gossip peer left or was declared dead"
        ),
        EventKind::Update => debug!(
            peer_node_id,
            peer_addr = %peer_addr,
            event_kind = "update",
            "gossip peer meta updated"
        ),
        // `EventKind` is `#[non_exhaustive]`.
        _ => {
            debug!(peer_node_id, peer_addr = %peer_addr, event_kind = "other", "gossip peer event")
        }
    }
}

/// Rebuild the observation snapshot from current membership.
///
/// Liveness comes from the *difference* between `members()` and `online_members()`, not from
/// `NodeState::state()`: in `memberlist` 0.8.5 the state carried by the `NodeState` handed out
/// by `members()` is fixed at the moment the node was first declared alive and is never
/// updated, while the authoritative state lives in the private `LocalNodeState.state`.
/// `online_members()` filters on that private state — specifically on `!dead_or_left()`, i.e.
/// `state != Dead && state != Left` (`memberlist-core-0.8.5/src/api.rs:116-127`,
/// `src/state.rs:68-70`) — so it is the only honest public signal.
///
/// The cost is that a `Suspect` peer is still in `online_members()` and is reported
/// [`Liveness::Alive`]; only `Dead` and `Left` fall out of the online set. See
/// [`GossipObservationSource::peers`] for the full mapping and the residual risk.
async fn refresh_snapshot(inner: &Inner, shared: &Shared) {
    let members = inner.members().await;
    let online: HashSet<SmolStr> = inner
        .online_members()
        .await
        .iter()
        .map(|m| m.id().clone())
        .collect();
    let mut observed = Vec::with_capacity(members.len());
    let mut live_ids = HashSet::with_capacity(members.len());

    for member in members.iter() {
        let id = member.id();
        live_ids.insert(id.clone());
        if id == &shared.self_id {
            continue;
        }
        // Present in `online_members()` means "not Dead and not Left"; a Suspect peer is
        // still in there and is therefore reported Alive.
        let liveness = if online.contains(id) {
            Liveness::Alive
        } else {
            Liveness::Dead
        };
        trace!(
            peer_node_id = id.as_str(),
            peer_addr = %member.address(),
            liveness = liveness_str(liveness),
            "gossip observed member"
        );
        match decode_hint(member.meta().as_bytes()) {
            Ok(mut hint) => {
                // Only the local failure detector decides liveness; the peer's own claim is
                // discarded. Every other field is passed through verbatim — including an
                // empty `peer_endpoint`. The gossip address is *not* a substitute for it:
                // it is a different plane on a different port, and an endpoint the engine
                // would have to reject must not be made to look valid here.
                hint.liveness = liveness;
                if hint.cluster_id != shared.cluster_id {
                    warn_cluster_mismatch(shared, member.id(), member.address(), &hint);
                }
                if hint.node_id == shared.self_node_id {
                    warn_duplicate_identity(shared, member.id(), member.address());
                }
                observed.push(hint);
            }
            Err(e) => warn_decode_failure(shared, member.id(), member.address(), &e),
        }
    }

    // Warning bookkeeping tracks only current members, so it cannot grow without bound and a
    // peer that returns misconfigured warns again.
    shared.retain_warned(&live_ids);
    shared.store(observed);
}

fn warn_cluster_mismatch(
    shared: &Shared,
    id: &SmolStr,
    addr: &SocketAddr,
    hint: &ObservedPeerHint,
) {
    if shared.first_time(id, |w| &mut w.cluster_mismatch) {
        warn!(
            peer_node_id = id.as_str(),
            peer_addr = %addr,
            peer_cluster_id = %hint.cluster_id,
            "gossip peer advertises a different cluster id; reported as a hint for the engine to reject"
        );
    }
}

/// A peer is advertising *our* node id. Either two nodes were configured with the same id, or
/// something is impersonating us. Gossip cannot adjudicate it — the engine's `validate_hint`
/// binds ids to committed membership and mTLS identity — but an operator needs to see it.
fn warn_duplicate_identity(shared: &Shared, id: &SmolStr, addr: &SocketAddr) {
    if shared.first_time(id, |w| &mut w.duplicate_identity) {
        warn!(
            peer_node_id = id.as_str(),
            peer_addr = %addr,
            claimed_node_id = %shared.self_node_id,
            "gossip peer advertises this node's own node id; duplicate identity or impersonation"
        );
    }
}

fn warn_decode_failure(shared: &Shared, id: &SmolStr, addr: &SocketAddr, e: &HintDecodeError) {
    if shared.first_time(id, |w| &mut w.decode_failure) {
        warn!(
            peer_node_id = id.as_str(),
            peer_addr = %addr,
            error = %e,
            "gossip peer metadata could not be decoded; peer ignored"
        );
    }
}
