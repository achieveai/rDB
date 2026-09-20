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
use crate::meta::{
    decode_hint, decode_hint_extras, encode_hint_with_extras, fingerprint_hex,
    gossip_key_fingerprint, AcceptedGossipKeys, GossipKeyFingerprint,
};

/// Depth of the membership-event channel. Events only *wake* the refresher, which then
/// rebuilds the snapshot from authoritative membership, so a full channel loses nothing but
/// timeliness.
const EVENT_CHANNEL_DEPTH: usize = 256;

/// How many times `start` re-runs the whole bind when the caller asked for an ephemeral port
/// (`bind_addr` port `0`). memberlist picks the port by binding TCP first, retrying that up to
/// ten times, and then binds UDP on the *same* port with no retry at all — so on a busy host a
/// port that was free for TCP can already be held for UDP by another process, and the start
/// fails for nothing the caller did. Retrying the whole bind is the only way to ask for a new
/// pair; a fixed port is not retried, because a taken fixed port is the caller's problem.
const EPHEMERAL_BIND_ATTEMPTS: u32 = 8;

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
    /// Advisory trailer re-applied on every re-advertisement (ADR-0030).
    ///
    /// Behind a lock because two of its slots change while the node runs and neither owner can
    /// see the other's: `schema` is fixed at start, but `accepted_gossip_keys` moves with a key
    /// rotation (ADR-0028) and `policy_version` with a document rotation (ADR-0027). Each owner
    /// therefore edits **its own field** through [`GossipNode::update_extras`] rather than
    /// replacing the whole value, which is the only shape under which two rotations in flight
    /// at once cannot silently undo each other (ruling M6-R18).
    extras: Mutex<Option<crate::HintExtras>>,
    /// The last hint advertised, so the trailer can be re-encoded without one being supplied.
    ///
    /// A meta change only reaches peers when memberlist re-advertises and bumps the node's
    /// incarnation; writing new bytes into the delegate alone would leave every peer on the
    /// old trailer until something else happened to re-advertise. So changing a slot means
    /// re-running the advertisement, and that needs the hint the node is currently publishing.
    last_hint: Mutex<ObservedPeerHint>,
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
    /// socket cannot be bound — for an ephemeral port only after [`EPHEMERAL_BIND_ATTEMPTS`]
    /// whole-bind attempts, because memberlist's own port-0 pick is free for TCP, not for the
    /// UDP socket it then binds on the same port.
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
        let ephemeral = cfg.bind_addr.port() == 0;
        async move {
            let mut attempt = 1;
            loop {
                match Self::start_instrumented(cfg.clone(), self_hint.clone(), span.clone()).await {
                    Err(GossipError::Start(e))
                        if ephemeral && attempt < EPHEMERAL_BIND_ATTEMPTS =>
                    {
                        tracing::debug!(attempt, error = %e, "gossip_ephemeral_bind_retry");
                        attempt += 1;
                    }
                    result => return result,
                }
            }
        }
        .instrument(outer)
        .await
    }

    async fn start_instrumented(
        cfg: GossipConfig,
        self_hint: ObservedPeerHint,
        span: Span,
    ) -> Result<Self, GossipError> {
        // The advertised key set is derived from the keyring this node is about to build, not
        // asked of the caller. A node advertising a set its keyring does not match would make a
        // rotation impossible to follow safely — an operator promoting a key because every peer
        // claims to accept it needs that claim to be the keyring's, not a copy of it somebody
        // forgot to update (ADR-0028).
        let mut cfg = cfg;
        if let Some(primary) = cfg.secret_key {
            let advertised = AcceptedGossipKeys::new(
                std::iter::once(primary)
                    .chain(cfg.accepted_keys.iter().copied())
                    .map(|key| gossip_key_fingerprint(&key)),
            );
            cfg.extras
                .get_or_insert_with(crate::HintExtras::default)
                .accepted_gossip_keys = Some(advertised);
        }

        // Enforce the metadata budget before memberlist can panic on it.
        let encoded = encode_hint_with_extras(&self_hint, cfg.extras.as_ref())?;

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
                .with_secret_keys(
                    cfg.accepted_keys
                        .iter()
                        .map(|k| SecretKey::Aes256(*k))
                        .collect(),
                )
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
            extras: Mutex::new(cfg.extras),
            last_hint: Mutex::new(self_hint),
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
            let encoded = {
                let extras = self.extras.lock().unwrap_or_else(|e| e.into_inner());
                encode_hint_with_extras(&hint, extras.as_ref())?
            };
            let size = encoded.len();
            *self.last_hint.lock().unwrap_or_else(|e| e.into_inner()) = hint;
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

    /// Change one slot of the advisory trailer and re-advertise (ruling M6-R18).
    ///
    /// A closure over the live value rather than a whole-value setter, because more than one
    /// owner writes this struct while the node runs — `accepted_gossip_keys` on a key rotation
    /// (ADR-0028), `policy_version` on a document rotation (ADR-0027) — and a setter would let
    /// whichever wrote second quietly revert the other. Each caller edits only its own field.
    ///
    /// A trailer that has never been set starts from [`HintExtras::default`], so a node
    /// configured without one can still begin advertising a slot later.
    ///
    /// # Errors
    ///
    /// As [`Self::update_hint`], whose path this takes: the new trailer is advertised with the
    /// hint already in force, so a slot that pushes the metadata past the budget leaves the
    /// previously advertised bytes untouched.
    pub async fn update_extras(
        &self,
        edit: impl FnOnce(&mut crate::HintExtras) + Send,
    ) -> Result<(), GossipError> {
        {
            let mut extras = self.extras.lock().unwrap_or_else(|e| e.into_inner());
            edit(extras.get_or_insert_with(crate::HintExtras::default));
        }
        let hint = self
            .last_hint
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        self.update_hint(hint).await
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

    /// The gossip keys this node holds, or `None` when gossip on this node is unencrypted
    /// (M6, ADR-0028).
    pub fn keyring(&self) -> Option<GossipKeyring> {
        self.inner.keyring().map(GossipKeyring::read)
    }

    /// Accept `key` on receive from now on, without signing with it.
    ///
    /// The first step of a rotation, and the only one that is safe to run node by node: a node
    /// that merely accepts one more key can still be understood by every peer, so a half-done
    /// sweep leaves a working cluster. Idempotent — `memberlist` treats re-adding a key it
    /// already holds as a no-op.
    ///
    /// # Errors
    ///
    /// [`GossipError::Keyring`] if gossip is unencrypted here, or
    /// [`GossipError::HintTooLarge`]/[`GossipError::Advertise`] from re-advertising the
    /// changed fingerprints — in which case the key **is** installed and only the
    /// advertisement failed, so the operator is told rather than left believing nothing
    /// happened.
    pub async fn add_gossip_key(&self, key: &[u8; 32]) -> Result<GossipKeyring, GossipError> {
        let keyring = self.require_keyring()?;
        keyring.insert(SecretKey::Aes256(*key));
        self.publish_keyring("added", gossip_key_fingerprint(key))
            .await
    }

    /// Sign outgoing gossip with `key` from now on.
    ///
    /// The second step, and the dangerous one: run it before every peer accepts `key` and those
    /// peers stop being able to read this node. `memberlist` refuses a key that was never added
    /// here, which enforces the add-before-use half of that rule locally; the other half —
    /// every *peer* having added it — is what [`GossipKeyring::accepted`] is advertised for.
    ///
    /// # Errors
    ///
    /// [`GossipError::Keyring`] if gossip is unencrypted here or `key` was never added,
    /// otherwise as [`Self::add_gossip_key`].
    pub async fn use_gossip_key(&self, key: &[u8; 32]) -> Result<GossipKeyring, GossipError> {
        let keyring = self.require_keyring()?;
        let fingerprint = gossip_key_fingerprint(key);
        keyring.use_key(key.as_slice()).map_err(|e| {
            GossipError::Keyring(format!(
                "cannot sign with gossip key {}: {e}",
                fingerprint_hex(fingerprint)
            ))
        })?;
        self.publish_keyring("promoted", fingerprint).await
    }

    /// Stop accepting `key`, completing the rotation.
    ///
    /// Refused while any advertised peer is still signing with `key` — because it accepts
    /// nothing else, or because it has added the replacement but not yet promoted it (ruling
    /// M6-R21). In both states this node would go deaf to that peer: after the removal nothing
    /// here can decrypt what it sends. `force` overrules the check, which is what an operator
    /// retiring a key after a node has been decommissioned needs, since a departed peer can
    /// linger in the membership list until the failure detector catches up.
    ///
    /// # Errors
    ///
    /// [`GossipError::GossipKeyStillNeeded`] for the refusal above,
    /// [`GossipError::Keyring`] if gossip is unencrypted here or `key` is the one being signed
    /// with (`memberlist` refuses to remove the primary), otherwise as
    /// [`Self::add_gossip_key`].
    pub async fn remove_gossip_key(
        &self,
        key: &[u8; 32],
        force: bool,
    ) -> Result<GossipKeyring, GossipError> {
        let keyring = self.require_keyring()?;
        let fingerprint = gossip_key_fingerprint(key);
        if !force {
            let peers = self.peers_still_needing(fingerprint).await;
            if peers > 0 {
                return Err(GossipError::GossipKeyStillNeeded {
                    fingerprint: fingerprint_hex(fingerprint),
                    peers,
                });
            }
        }
        keyring.remove(key.as_slice()).map_err(|e| {
            GossipError::Keyring(format!(
                "cannot remove gossip key {}: {e}",
                fingerprint_hex(fingerprint)
            ))
        })?;
        self.publish_keyring("removed", fingerprint).await
    }

    /// The live keyring, or the refusal to give when gossip is not encrypted here.
    fn require_keyring(&self) -> Result<&memberlist::keyring::Keyring, GossipError> {
        self.inner.keyring().ok_or_else(|| {
            GossipError::Keyring(
                "gossip is not encrypted on this node; there is no keyring to rotate".to_string(),
            )
        })
    }

    /// How many advertised peers still need `fingerprint` — accept it and nothing else, or are
    /// still signing with it (ruling M6-R21).
    ///
    /// Both clauses are the same outage seen from two stages of a rotation: a peer with no
    /// other key cannot be read at all after the removal, and a peer that has added the new key
    /// but not yet promoted it still *sends* under the old one, so it cannot be read either.
    /// The second clause is the one an operator trips, because `add` and `remove` both look
    /// node-local while only the `use` sweep changes what a peer signs with.
    ///
    /// Read from the peers' advertised metadata on demand rather than from the observation
    /// snapshot, because the snapshot deliberately carries only [`ObservedPeerHint`] — the
    /// advisory trailer is not part of what the Raft path is allowed to see. A peer that
    /// advertises no trailer at all is not counted: it is running a build from before ADR-0028
    /// and has never been told about a second key, so there is nothing here to protect.
    ///
    /// This node's own advertisement is skipped (critic-m6 delta N1): the caller is the one
    /// removing the key, so counting itself would report one peer too many and, before the
    /// `use` step, turn the keyring's own primary refusal into a peer refusal.
    async fn peers_still_needing(&self, fingerprint: GossipKeyFingerprint) -> usize {
        self.inner
            .members()
            .await
            .iter()
            .filter(|member| member.id() != &self.shared.self_id)
            .map(|member| member.meta().as_bytes().to_vec())
            .filter_map(|meta| decode_hint_extras(&meta))
            .filter_map(|extras| extras.accepted_gossip_keys)
            .filter(|keys| keys.is_sole(fingerprint) || keys.is_primary(fingerprint))
            .count()
    }

    /// Re-advertise the changed key set and log the stage the rotation reached.
    ///
    /// The advertisement is the point: a rotation is a cluster-wide operation driven by an
    /// operator who can only see what nodes publish, so a key added here that nobody can see
    /// was added is a key they cannot safely promote.
    async fn publish_keyring(
        &self,
        stage: &'static str,
        key_fingerprint: GossipKeyFingerprint,
    ) -> Result<GossipKeyring, GossipError> {
        let state = self.keyring().ok_or_else(|| {
            GossipError::Keyring("gossip keyring disappeared mid-rotation".to_string())
        })?;
        let advertised = AcceptedGossipKeys::new(state.accepted.iter().copied());
        self.update_extras(|extras| extras.accepted_gossip_keys = Some(advertised))
            .await?;
        info!(
            stage,
            key_fingerprint = %fingerprint_hex(key_fingerprint),
            primary = %fingerprint_hex(state.primary),
            accepted_keys = state.accepted.len(),
            "gossip_key_rotated"
        );
        Ok(state)
    }

    /// The raw metadata every member currently advertises, this node included.
    ///
    /// Deliberately raw: it is asserted by a test against what is *on the wire* (ADR-0030
    /// M6-85), and handing back decoded values would assert this crate's decoder against
    /// itself instead of against the bytes a peer actually published. [`Self::keyring`]'s
    /// removal check reads it too, and decodes only the one slot it owns.
    pub async fn member_meta(&self) -> Vec<Vec<u8>> {
        self.inner
            .members()
            .await
            .iter()
            .map(|member| member.meta().as_bytes().to_vec())
            .collect()
    }
}

/// What a node's gossip keyring holds, in fingerprints (M6, ADR-0028).
///
/// Fingerprints and never key bytes: this value is logged, advertised in the gossip trailer and
/// returned over the admin plane, so every one of its fields has to be safe to publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipKeyring {
    /// The key outgoing gossip is signed with.
    pub primary: GossipKeyFingerprint,
    /// Every key accepted on receive, primary first — a superset of [`Self::primary`], because
    /// a node always accepts what it signs with. "Primary first" is `memberlist`'s ordering,
    /// recorded UNVERIFIED by ruling M6-R5 and evidenced here by `m6_57`, which asserts
    /// `accepted[0] == primary` after a promotion; the removal refusal reads slot 0, so the
    /// claim is load-bearing rather than decorative (ruling M6-R21).
    pub accepted: Vec<GossipKeyFingerprint>,
}

impl GossipKeyring {
    /// Read the live `memberlist` keyring into fingerprints.
    fn read(keyring: &memberlist::keyring::Keyring) -> Self {
        Self {
            primary: gossip_key_fingerprint(keyring.primary_key().as_ref()),
            accepted: keyring
                .keys()
                .map(|key| gossip_key_fingerprint(key.as_ref()))
                .collect(),
        }
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
