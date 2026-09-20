//! Behaviour tests for the advisory gossip adapter (ADR-0003, spec §21 M1).
//!
//! # Conventions (test plan §6, normative)
//!
//! - **Names carry their plan id** (rule 13). `docs/testing/test-plan-m0-m1.md` §4.2 rows
//!   M1-28..M1-36 are *cluster* gates owned by the workspace-level `tests/m1_*.rs` crate;
//!   only M1-30 (poisoned wrong-`cluster_id` gossip) has a half that lives in this crate, so
//!   the two tests that evidence it are named `m1_30_…`. The rest evidence no §4.2 row and
//!   use the authorized `m1_gossip_NN_` prefix instead of borrowing an id they do not prove.
//! - **No literal ports** (rule 4). Listeners bind an ephemeral loopback port built from
//!   [`Ipv4Addr::LOCALHOST`], and advertised *payload* endpoints are `.invalid` hostnames
//!   with no port at all, so nothing in this file can be mistaken for a reserved port.
//! - **Derived deadlines** (rule 3). Every wait is a multiple of [`TestTimers`], which also
//!   configures the nodes. One edit re-tunes the whole file for a slower machine.
//! - **Polls, never sleeps** (rule 1). [`poll_until`] / [`stays_false`] are the only waits;
//!   their `sleep` is the poll tick, marked `testkit:allow-sleep`. A shared testkit helper is
//!   being built separately; this file deliberately does not depend on it yet.
//! - **Diagnosable timeouts** (rule 2). Every timeout assertion renders every participating
//!   node's `peers()` snapshot.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use config_core::hint::{GossipObservationSource, Liveness, ObservedPeerHint};
use config_core::identity::{ClusterId, NodeId, RecoveryEpoch};
use config_gossip::{
    decode_hint, decode_hint_extras, encode_hint, gossip_key_fingerprint, AcceptedGossipKeys,
    GossipConfig, GossipError, GossipNode, HintDecodeError, HintExtras, StaticObservationSource,
    HINT_WIRE_VERSION, MAX_HINT_BYTES,
};

const KEY_A: [u8; 32] = [0x11; 32];
const KEY_B: [u8; 32] = [0x22; 32];

/// The timers every test derives both its node configuration and its deadlines from.
///
/// Waits are expressed as multiples of the failure detector's probe interval rather than as
/// literals, so a CI box three times slower is one edit here (§6 rule 3).
#[derive(Clone, Copy)]
struct TestTimers {
    probe_interval: Duration,
    probe_timeout: Duration,
    gossip_interval: Duration,
    refresh_interval: Duration,
    join_retry_delay: Duration,
    /// "The cluster converged" bound, in probe intervals.
    converge_probes: u32,
    /// "This stayed absent" bound, in probe intervals.
    absent_probes: u32,
}

impl TestTimers {
    /// Fast single-host profile: 200 ms probes, 100 ms gossip rounds, a 250 ms snapshot
    /// refresh. Yields a 10 s convergence bound and a 3 s absence bound, inside §6 rule 9's
    /// 30 s per-test budget even when a test does both.
    const fn fast() -> Self {
        Self {
            probe_interval: Duration::from_millis(200),
            probe_timeout: Duration::from_millis(200),
            gossip_interval: Duration::from_millis(100),
            refresh_interval: Duration::from_millis(250),
            join_retry_delay: Duration::from_millis(250),
            converge_probes: 50,
            absent_probes: 15,
        }
    }

    fn converge(&self) -> Duration {
        self.probe_interval * self.converge_probes
    }

    fn stays_absent(&self) -> Duration {
        self.probe_interval * self.absent_probes
    }

    /// Poll tick: one gossip round, so a poll cannot outrun dissemination.
    fn poll_interval(&self) -> Duration {
        self.gossip_interval
    }

    fn config(&self, cluster_id: ClusterId, node_id: u64, key: [u8; 32]) -> GossipConfig {
        let mut cfg = GossipConfig::new(cluster_id, NodeId(node_id), ephemeral_loopback());
        cfg.secret_key = Some(key);
        cfg.probe_interval = self.probe_interval;
        cfg.probe_timeout = self.probe_timeout;
        cfg.gossip_interval = self.gossip_interval;
        cfg.refresh_interval = self.refresh_interval;
        cfg.join_retry_delay = self.join_retry_delay;
        cfg
    }
}

/// Loopback with an OS-assigned port (§6 rule 4). Built, not parsed, so no port literal —
/// not even `0` — appears in this file.
fn ephemeral_loopback() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

fn cluster() -> ClusterId {
    ClusterId::from_bytes([0x5a; 16])
}

/// An advertised endpoint that is syntactically plausible and unroutable.
///
/// `.invalid` is reserved by RFC 2606 and carries no port, so these strings can never be
/// confused with a real listener or trip the "no literal ports" source scan.
fn fake_endpoint(node_id: u64) -> String {
    format!("node-{node_id}.retcd.invalid")
}

fn fake_client_endpoint(node_id: u64) -> String {
    format!("node-{node_id}-client.retcd.invalid")
}

fn hint(node_id: u64) -> ObservedPeerHint {
    ObservedPeerHint {
        cluster_id: cluster(),
        recovery_epoch: RecoveryEpoch(1),
        node_id: NodeId(node_id),
        peer_endpoint: fake_endpoint(node_id),
        client_endpoint: Some(fake_client_endpoint(node_id)),
        software_version: "0.1.0".into(),
        protocol_version: 1,
        zone: Some("z1".into()),
        liveness: Liveness::Alive,
    }
}

/// Poll `check` every `interval` until it is true or `limit` elapses.
async fn poll_until(limit: Duration, interval: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        if check() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(interval).await; // testkit:allow-sleep
    }
}

/// [`poll_until`] for a condition that has to `await` to answer — `member_meta` is async
/// because it asks memberlist for the current member list.
async fn poll_until_async<Check, Answer>(
    limit: Duration,
    interval: Duration,
    mut check: Check,
) -> bool
where
    Check: FnMut() -> Answer,
    Answer: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + limit;
    loop {
        if check().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(interval).await; // testkit:allow-sleep
    }
}

/// What `observer` currently believes node `id` advertises in its advisory trailer.
async fn trailer_for(observer: &GossipNode, id: u64) -> Option<HintExtras> {
    observer
        .member_meta()
        .await
        .into_iter()
        .find(|meta| decode_hint(meta).is_ok_and(|hint| hint.node_id == NodeId(id)))
        .and_then(|meta| decode_hint_extras(&meta))
}

/// Assert `check` never becomes true for `limit`.
async fn stays_false(limit: Duration, interval: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if check() {
            return false;
        }
        tokio::time::sleep(interval).await; // testkit:allow-sleep
    }
    true
}

fn find(peers: &[ObservedPeerHint], node_id: u64) -> Option<ObservedPeerHint> {
    peers.iter().find(|h| h.node_id == NodeId(node_id)).cloned()
}

/// Every participating node's observation snapshot, for a timeout failure message (§6 rule 2).
fn snapshots(nodes: &[(&str, &GossipNode)]) -> String {
    let mut out = String::from("\nobservation snapshots at failure:");
    for (label, node) in nodes {
        let peers = node.peers();
        out.push_str(&format!(
            "\n  {label}: node_id={} gossip_addr={} observes {} peer(s)",
            node.node_id(),
            node.advertise_addr(),
            peers.len()
        ));
        if peers.is_empty() {
            out.push_str("\n    <none>");
        }
        for p in &peers {
            out.push_str(&format!(
                "\n    node_id={} liveness={:?} cluster_id={} peer_endpoint={:?} client_endpoint={:?} zone={:?}",
                p.node_id, p.liveness, p.cluster_id, p.peer_endpoint, p.client_endpoint, p.zone
            ));
        }
    }
    out
}

// ---------------------------------------------------------------------------------------
// Adapter behaviour (no §4.2 row: these are this crate's own contract, not a cluster gate)
// ---------------------------------------------------------------------------------------

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m1_gossip_01_two_nodes_observe_each_other() {
    let t = TestTimers::fast();
    let a = GossipNode::start(t.config(cluster(), 1, KEY_A), hint(1))
        .await
        .expect("node a starts");

    let mut cfg_b = t.config(cluster(), 2, KEY_A);
    cfg_b.seeds = vec![a.advertise_addr()];
    let b = GossipNode::start(cfg_b, hint(2))
        .await
        .expect("node b starts");

    let converged = poll_until(t.converge(), t.poll_interval(), || {
        let a_sees = find(&a.peers(), 2).is_some_and(|h| h.liveness == Liveness::Alive);
        let b_sees = find(&b.peers(), 1).is_some_and(|h| h.liveness == Liveness::Alive);
        a_sees && b_sees
    })
    .await;
    assert!(
        converged,
        "nodes did not observe each other within {:?}{}",
        t.converge(),
        snapshots(&[("a", &a), ("b", &b)])
    );

    // The advertised payload survives the round trip, and self is excluded.
    let observed = find(&a.peers(), 2).expect("a observes b");
    assert_eq!(observed.cluster_id, cluster());
    assert_eq!(observed.peer_endpoint, fake_endpoint(2));
    assert_eq!(
        observed.client_endpoint.as_deref(),
        Some(fake_client_endpoint(2).as_str())
    );
    assert_eq!(observed.software_version, "0.1.0");
    assert_eq!(observed.zone.as_deref(), Some("z1"));
    assert!(find(&a.peers(), 1).is_none(), "peers() must exclude self");

    // A re-advertised hint propagates.
    let mut updated = hint(2);
    updated.zone = Some("z2".into());
    b.update_hint(updated).await.expect("re-advertise");
    let propagated = poll_until(t.converge(), t.poll_interval(), || {
        find(&a.peers(), 2).and_then(|h| h.zone) == Some("z2".to_string())
    })
    .await;
    assert!(
        propagated,
        "updated hint did not propagate within {:?}{}",
        t.converge(),
        snapshots(&[("a", &a), ("b", &b)])
    );

    b.shutdown().await;
    a.shutdown().await;
}

/// Ruling M6-R18: one owner changes one slot of the advisory trailer while the node runs, the
/// change reaches the peers, and the slots that owner does not own are left exactly as they
/// were.
///
/// Both halves are the point. Without the re-advertisement a node would keep publishing the
/// trailer it booted with — the whole reason `policy_version` is on the wire is that it moves
/// (ADR-0027 §15.3) — and without the closure form the second of two rotations in flight would
/// silently revert the first (ADR-0028's key set and ADR-0027's version change independently).
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_r18_update_extras_changes_one_slot_and_re_advertises() {
    let t = TestTimers::fast();
    let a = GossipNode::start(t.config(cluster(), 1, KEY_A), hint(1))
        .await
        .expect("node a starts");

    // Derived, not chosen. `GossipNode::start` fills this slot from the keyring it builds, so
    // the only value node b can advertise is the fingerprint of the key it gossips with
    // (ADR-0028). It still serves this row's purpose — a slot whose owner is not the caller of
    // `update_extras` — and it now also proves the two cannot be set to disagree.
    let keys = AcceptedGossipKeys::new([gossip_key_fingerprint(&KEY_A)]);
    let booted = HintExtras {
        schema: Some(config_core::CURRENT_SCHEMA),
        accepted_gossip_keys: Some(keys),
        policy_version: None,
    };
    let mut cfg_b = t.config(cluster(), 2, KEY_A);
    cfg_b.seeds = vec![a.advertise_addr()];
    cfg_b.extras = Some(booted);
    let b = GossipNode::start(cfg_b, hint(2))
        .await
        .expect("node b starts");

    let seen = poll_until_async(t.converge(), t.poll_interval(), || async {
        trailer_for(&a, 2).await == Some(booted)
    })
    .await;
    assert!(
        seen,
        "node a never saw node b's booted trailer within {:?}{}",
        t.converge(),
        snapshots(&[("a", &a), ("b", &b)])
    );

    b.update_extras(|extras| extras.policy_version = Some(8))
        .await
        .expect("re-advertise the changed trailer");

    let propagated = poll_until_async(t.converge(), t.poll_interval(), || async {
        trailer_for(&a, 2)
            .await
            .and_then(|extras| extras.policy_version)
            == Some(8)
    })
    .await;
    assert!(
        propagated,
        "the changed policy_version did not propagate within {:?}{}",
        t.converge(),
        snapshots(&[("a", &a), ("b", &b)])
    );

    let extras = trailer_for(&a, 2)
        .await
        .expect("node a holds node b's trailer");
    assert_eq!(
        extras.schema,
        Some(config_core::CURRENT_SCHEMA),
        "a slot this caller does not own must survive its edit"
    );
    assert_eq!(
        extras.accepted_gossip_keys,
        Some(keys),
        "and so must the other owner's, or two rotations in flight would undo each other"
    );

    b.shutdown().await;
    a.shutdown().await;
}

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m1_gossip_02_oversized_hint_is_rejected_before_start() {
    let t = TestTimers::fast();
    let mut oversized = hint(1);
    oversized.software_version = "v".repeat(MAX_HINT_BYTES + 128);

    match GossipNode::start(t.config(cluster(), 1, KEY_A), oversized).await {
        Err(GossipError::HintTooLarge { size, limit }) => {
            assert!(size > limit, "size {size} should exceed limit {limit}");
            assert_eq!(limit, MAX_HINT_BYTES);
        }
        Err(other) => panic!("expected HintTooLarge, got {other:?}"),
        Ok(_) => panic!("oversized hint must not start a node"),
    }
}

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m1_gossip_03_wrong_key_never_becomes_a_peer() {
    let t = TestTimers::fast();
    let a = GossipNode::start(t.config(cluster(), 1, KEY_A), hint(1))
        .await
        .expect("node a starts");

    let mut cfg_b = t.config(cluster(), 2, KEY_A);
    cfg_b.seeds = vec![a.advertise_addr()];
    let b = GossipNode::start(cfg_b, hint(2))
        .await
        .expect("node b starts");

    assert!(
        poll_until(t.converge(), t.poll_interval(), || find(&a.peers(), 2)
            .is_some())
        .await,
        "encrypted pair did not form{}",
        snapshots(&[("a", &a), ("b", &b)])
    );

    // Same cluster, same label, different key: memberlist cannot authenticate it.
    let mut cfg_c = t.config(cluster(), 3, KEY_B);
    cfg_c.join_attempts = 1;
    let c = GossipNode::start(cfg_c, hint(3))
        .await
        .expect("node c starts");

    // The join is not merely eventually useless — it reaches nobody, right now.
    assert_eq!(
        c.join(&[a.advertise_addr()]).await,
        0,
        "a seed with a different gossip key must not be joinable{}",
        snapshots(&[("a", &a), ("b", &b), ("c", &c)])
    );

    let isolated = stays_false(t.stays_absent(), t.poll_interval(), || {
        find(&a.peers(), 3).is_some()
            || find(&b.peers(), 3).is_some()
            || find(&c.peers(), 1).is_some()
            || find(&c.peers(), 2).is_some()
    })
    .await;
    assert!(
        isolated,
        "a node with a different gossip key became a peer{}",
        snapshots(&[("a", &a), ("b", &b), ("c", &c)])
    );

    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m1_gossip_04_shutdown_is_observed_by_the_survivor() {
    let t = TestTimers::fast();
    let a = GossipNode::start(t.config(cluster(), 1, KEY_A), hint(1))
        .await
        .expect("node a starts");

    let mut cfg_b = t.config(cluster(), 2, KEY_A);
    cfg_b.seeds = vec![a.advertise_addr()];
    let b = GossipNode::start(cfg_b, hint(2))
        .await
        .expect("node b starts");

    assert!(
        poll_until(t.converge(), t.poll_interval(), || find(&a.peers(), 2)
            .is_some())
        .await,
        "a never observed b{}",
        snapshots(&[("a", &a), ("b", &b)])
    );

    b.shutdown().await;

    // `Left` is not observable separately from `Dead` in memberlist 0.8.5; either the peer
    // drops out of the snapshot or it is reported `Dead`. See `GossipNode::peers` docs.
    let noticed = poll_until(t.converge(), t.poll_interval(), || {
        match find(&a.peers(), 2) {
            None => true,
            Some(h) => h.liveness == Liveness::Dead,
        }
    })
    .await;
    assert!(
        noticed,
        "a did not observe b leaving within {:?}{}",
        t.converge(),
        snapshots(&[("a", &a)])
    );

    a.shutdown().await;
}

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m1_gossip_05_drop_without_shutdown_releases_port() {
    // A `GossipNode` dropped without `shutdown()` must not strand its listener. memberlist's
    // own listener tasks hold a strong handle to the structure that owns their shutdown
    // channel, so nothing is released unless `Drop` explicitly tears it down.
    let t = TestTimers::fast();
    let addr = {
        let node = GossipNode::start(t.config(cluster(), 1, KEY_A), hint(1))
            .await
            .expect("node starts");
        let addr = node.advertise_addr();
        assert!(
            tokio::net::TcpListener::bind(addr).await.is_err(),
            "{addr} should be occupied while the node is running"
        );
        addr
        // `node` drops here, without `shutdown()`.
    };

    let released = poll_until(t.converge(), t.poll_interval(), || {
        std::net::TcpListener::bind(addr).is_ok()
    })
    .await;
    assert!(
        released,
        "gossip port {addr} was still bound {:?} after the node was dropped without shutdown()",
        t.converge()
    );
}

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m1_gossip_06_empty_peer_endpoint_is_reported_empty() {
    // An unset `peer_endpoint` propagates unchanged. Substituting the peer's *gossip* address
    // would manufacture a plausible-looking endpoint on the wrong plane; the engine must see
    // the emptiness and reject the hint.
    let t = TestTimers::fast();
    let a = GossipNode::start(t.config(cluster(), 1, KEY_A), hint(1))
        .await
        .expect("node a starts");

    let mut endpointless = hint(2);
    endpointless.peer_endpoint = String::new();
    endpointless.client_endpoint = None;

    let mut cfg_b = t.config(cluster(), 2, KEY_A);
    cfg_b.seeds = vec![a.advertise_addr()];
    let b = GossipNode::start(cfg_b, endpointless)
        .await
        .expect("node b starts");

    let seen = poll_until(t.converge(), t.poll_interval(), || {
        find(&a.peers(), 2).is_some()
    })
    .await;
    assert!(
        seen,
        "a never observed b within {:?}{}",
        t.converge(),
        snapshots(&[("a", &a), ("b", &b)])
    );

    let observed = find(&a.peers(), 2).expect("a observes b");
    assert_eq!(
        observed.peer_endpoint,
        "",
        "an empty advertised endpoint must stay empty, not become the gossip address{}",
        snapshots(&[("a", &a), ("b", &b)])
    );
    assert_eq!(observed.client_endpoint, None);
    assert_ne!(
        observed.peer_endpoint,
        b.advertise_addr().to_string(),
        "the gossip address must never be substituted for a missing peer endpoint"
    );

    b.shutdown().await;
    a.shutdown().await;
}

// ---------------------------------------------------------------------------------------
// M1-30 — poisoned gossip carrying a foreign cluster id (gossip-side half of the §4.2 row)
// ---------------------------------------------------------------------------------------

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m1_30_mismatched_cluster_hint_is_still_reported() {
    // A misconfigured node: it gossips in our cluster (same label and key) but advertises a
    // hint claiming a different cluster. The adapter must surface it unchanged and let the
    // engine's `validate_hint` reject it (ADR-0003); suppressing it here would hide the
    // misconfiguration from the operator.
    let t = TestTimers::fast();
    let foreign = ClusterId::from_bytes([0xee; 16]);
    let a = GossipNode::start(t.config(cluster(), 1, KEY_A), hint(1))
        .await
        .expect("node a starts");

    let mut poisoned = hint(2);
    poisoned.cluster_id = foreign;

    let mut cfg_b = t.config(cluster(), 2, KEY_A);
    cfg_b.seeds = vec![a.advertise_addr()];
    let b = GossipNode::start(cfg_b, poisoned)
        .await
        .expect("node b starts");

    let seen = poll_until(t.converge(), t.poll_interval(), || {
        find(&a.peers(), 2).is_some_and(|h| h.cluster_id == foreign)
    })
    .await;
    assert!(
        seen,
        "a must still report the mismatched hint verbatim within {:?}{}",
        t.converge(),
        snapshots(&[("a", &a), ("b", &b)])
    );

    b.shutdown().await;
    a.shutdown().await;
}

#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m1_30_wrong_cluster_label_never_becomes_a_peer() {
    // The other half of the cluster-id boundary: the `cluster_id` is also the gossip label,
    // which is the AES-GCM additional authenticated data. A node holding the *correct* secret
    // key but gossiping under a different cluster id must not join, even though its
    // ciphertext would otherwise decrypt.
    let t = TestTimers::fast();
    let a = GossipNode::start(t.config(cluster(), 1, KEY_A), hint(1))
        .await
        .expect("node a starts");

    let mut cfg_b = t.config(cluster(), 2, KEY_A);
    cfg_b.seeds = vec![a.advertise_addr()];
    let b = GossipNode::start(cfg_b, hint(2))
        .await
        .expect("node b starts");

    // Positive control first: a and b really do converge, so a later absence means isolation
    // rather than a suite that never gossips at all (§6 rule 11).
    let converged = poll_until(t.converge(), t.poll_interval(), || {
        find(&a.peers(), 2).is_some() && find(&b.peers(), 1).is_some()
    })
    .await;
    assert!(
        converged,
        "the same-cluster pair did not converge within {:?}; the negative assertion below \
         would prove nothing{}",
        t.converge(),
        snapshots(&[("a", &a), ("b", &b)])
    );

    // Same key, different cluster id — and therefore a different gossip label.
    let other_cluster = ClusterId::from_bytes([0xc3; 16]);
    let mut cfg_c = t.config(other_cluster, 3, KEY_A);
    cfg_c.join_attempts = 1;
    let mut hint_c = hint(3);
    hint_c.cluster_id = other_cluster;
    let c = GossipNode::start(cfg_c, hint_c)
        .await
        .expect("node c starts");

    assert_eq!(
        c.join(&[a.advertise_addr()]).await,
        0,
        "a seed in another cluster must not be joinable{}",
        snapshots(&[("a", &a), ("b", &b), ("c", &c)])
    );

    let isolated = stays_false(t.stays_absent(), t.poll_interval(), || {
        find(&a.peers(), 3).is_some()
            || find(&b.peers(), 3).is_some()
            || find(&c.peers(), 1).is_some()
            || find(&c.peers(), 2).is_some()
    })
    .await;
    assert!(
        isolated,
        "a node gossiping under a different cluster label became a peer{}",
        snapshots(&[("a", &a), ("b", &b), ("c", &c)])
    );

    c.shutdown().await;
    b.shutdown().await;
    a.shutdown().await;
}

// ---------------------------------------------------------------------------------------
// Injection source and wire format (network-free)
// ---------------------------------------------------------------------------------------

#[config_log::retcd_test]
fn m1_gossip_07_static_source_returns_what_it_is_given() {
    let injected = vec![hint(7), hint(8)];
    let source = StaticObservationSource::new(injected.clone());
    assert_eq!(source.peers(), injected);
    assert_eq!(source.hints(), injected.as_slice());
    assert_eq!(source.peers(), injected, "repeated reads are stable");

    // A poisoned hint is passed through unchanged; rejecting it is the engine's job.
    let mut poisoned = hint(9);
    poisoned.cluster_id = ClusterId::from_bytes([0xff; 16]);
    let source = StaticObservationSource::from(vec![poisoned.clone()]);
    assert_eq!(source.peers(), vec![poisoned]);

    assert!(StaticObservationSource::empty().peers().is_empty());
}

#[config_log::retcd_test]
fn m1_gossip_08_meta_round_trips_within_budget() {
    let original = hint(3);
    let encoded = encode_hint(&original).expect("encode");
    assert!(
        encoded.len() <= MAX_HINT_BYTES,
        "encoded hint is {} bytes",
        encoded.len()
    );
    assert_eq!(encoded[0], HINT_WIRE_VERSION, "version byte comes first");
    assert_eq!(decode_hint(&encoded).expect("decode"), original);

    // Malformed metadata is an error, never a panic.
    assert!(decode_hint(&[]).is_err());
    assert!(decode_hint(&[0xffu8; 16]).is_err());
}

/// The exact bytes of a fixed hint. A change here is a wire-format change: either bump
/// [`HINT_WIRE_VERSION`] and update this vector, or the change is a bug.
#[config_log::retcd_test]
fn m1_gossip_09_hint_wire_format_golden_bytes() {
    let golden_hint = ObservedPeerHint {
        cluster_id: ClusterId::from_bytes([0x5a; 16]),
        recovery_epoch: RecoveryEpoch(1),
        node_id: NodeId(2),
        peer_endpoint: "node-2.retcd.invalid".into(),
        client_endpoint: None,
        software_version: "0.1.0".into(),
        protocol_version: 1,
        zone: None,
        liveness: Liveness::Alive,
    };

    const GOLDEN: &[u8] = &GOLDEN_HINT_V1;

    let encoded = encode_hint(&golden_hint).expect("encode");
    assert_eq!(
        encoded, GOLDEN,
        "gossip wire format changed without a HINT_WIRE_VERSION bump; actual = {encoded:?}"
    );
    assert_eq!(decode_hint(GOLDEN).expect("decode"), golden_hint);
}

#[config_log::retcd_test]
fn m1_gossip_10_wire_version_is_checked_and_trailing_bytes_ignored() {
    let original = hint(4);
    let v1 = encode_hint(&original).expect("encode");

    // Forward compatibility: a newer build appending fields is still understood.
    let mut with_slack = v1.clone();
    with_slack.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(
        decode_hint(&with_slack).expect("trailing bytes must be ignored"),
        original
    );

    // An older or newer *format* is refused cleanly, naming the version it saw.
    for bad in [0u8, HINT_WIRE_VERSION + 1] {
        let mut wrong = v1.clone();
        wrong[0] = bad;
        assert_eq!(
            decode_hint(&wrong),
            Err(HintDecodeError::UnsupportedVersion(bad)),
            "version {bad} must be refused as UnsupportedVersion"
        );
    }

    // A correct version byte over a corrupt body is malformed, not "unsupported".
    let corrupt = [HINT_WIRE_VERSION, 0xff, 0xff, 0xff];
    assert!(matches!(
        decode_hint(&corrupt),
        Err(HintDecodeError::Malformed { .. })
    ));

    // Empty metadata has no version byte at all.
    assert!(matches!(
        decode_hint(&[]),
        Err(HintDecodeError::Malformed { len: 0, .. })
    ));
}

/// Golden encoding of the hint in [`m1_gossip_09_hint_wire_format_golden_bytes`]:
/// `HINT_WIRE_VERSION`, then postcard (16 raw cluster-id bytes, **varint recovery epoch**,
/// varint node id, length-prefixed strings, option tags, varint protocol version, enum
/// discriminant).
///
/// A10 added `recovery_epoch` as byte 17 without bumping `HINT_WIRE_VERSION`: rEtcd has not
/// shipped, so there is no v1 encoder anywhere to be incompatible with, and a `2` would be a
/// version number nothing ever spoke. The golden vector is what actually guards the format —
/// it fails loudly on any layout change, which is how this change was caught and updated.
const GOLDEN_HINT_V1: [u8; 50] = [
    0x01, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
    0x5a, 0x01, 0x02, 0x14, 0x6e, 0x6f, 0x64, 0x65, 0x2d, 0x32, 0x2e, 0x72, 0x65, 0x74, 0x63, 0x64,
    0x2e, 0x69, 0x6e, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x00, 0x05, 0x30, 0x2e, 0x31, 0x2e, 0x30, 0x01,
    0x00, 0x00,
];

/// A10: the recovery epoch survives the gossip wire, and two hints that differ only in their
/// epoch encode to different bytes. Without this the engine's epoch check would be validating
/// a field the transport had silently dropped.
#[config_log::retcd_test]
fn a10_recovery_epoch_round_trips_over_the_gossip_wire() {
    let mut at_1 = hint(5);
    at_1.recovery_epoch = RecoveryEpoch(1);
    let mut at_2 = at_1.clone();
    at_2.recovery_epoch = RecoveryEpoch(2);

    let enc_1 = encode_hint(&at_1).expect("encode");
    let enc_2 = encode_hint(&at_2).expect("encode");
    assert_ne!(
        enc_1, enc_2,
        "hints differing only in recovery_epoch must not encode identically"
    );

    assert_eq!(decode_hint(&enc_1).expect("decode"), at_1);
    assert_eq!(
        decode_hint(&enc_2).expect("decode").recovery_epoch,
        RecoveryEpoch(2)
    );
    assert!(enc_2.len() <= MAX_HINT_BYTES);

    // A large epoch is still a varint, so the 512-byte budget is not at risk.
    let mut at_max = at_1.clone();
    at_max.recovery_epoch = RecoveryEpoch(u32::MAX);
    let enc_max = encode_hint(&at_max).expect("encode");
    assert!(
        enc_max.len() <= MAX_HINT_BYTES,
        "u32::MAX epoch encodes to {} bytes",
        enc_max.len()
    );
    assert_eq!(decode_hint(&enc_max).expect("decode"), at_max);
}

/// M6-57/M6-58: a key must be accepted before it can be signed with, and the keyring says so.
///
/// One node is enough for the ordering rule: `use_key` is refused for a key that was never
/// added *here*, which is the half of ADR-0028's add-before-use rule that a node can enforce on
/// its own. The other half — every peer having added it — is what the advertised fingerprints
/// exist for, and M6-58's cluster row covers that.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_57_a_gossip_key_must_be_accepted_before_it_is_signed_with() {
    let timers = TestTimers::fast();
    let old = [0x11; 32];
    let new = [0x22; 32];
    let node = GossipNode::start(timers.config(cluster(), 1, old), hint(1))
        .await
        .expect("start");

    let before = node.keyring().expect("an encrypted node has a keyring");
    assert_eq!(before.accepted.len(), 1, "{before:#?}");

    let refused = node
        .use_gossip_key(&new)
        .await
        .expect_err("a key nobody added cannot be signed with");
    assert!(
        matches!(refused, GossipError::Keyring(ref detail) if !detail.contains("22")),
        "the refusal must name the key by fingerprint, never by value: {refused}"
    );

    let added = node.add_gossip_key(&new).await.expect("add");
    assert_eq!(added.primary, before.primary, "adding does not promote");
    assert_eq!(added.accepted.len(), 2, "{added:#?}");

    let promoted = node.use_gossip_key(&new).await.expect("use");
    assert_ne!(promoted.primary, before.primary, "{promoted:#?}");
    assert_eq!(
        promoted.accepted.len(),
        2,
        "promoting swaps which key signs, it does not drop the other: {promoted:#?}"
    );
    assert_eq!(
        promoted.accepted[0], promoted.primary,
        "the primary is advertised first, so a peer comparing sets sees it first: {promoted:#?}"
    );

    node.shutdown().await;
}

/// M6-59: a key a peer has nothing else to fall back on is not removable without `force`.
///
/// Two nodes, mid-rotation, in exactly the state that makes the removal dangerous. Node 1 has
/// finished its half — it holds both keys and signs with the new one. Node 2 has not started:
/// it holds only the old key. Node 1 can still *read* node 2, which signs with the old key node
/// 1 still accepts, so node 2's advertisement is visible and says that old key is all it has.
/// Dropping it there is what would make node 2 unreadable, and that is the refusal.
///
/// `force` is the escape hatch for the case the check cannot distinguish: a peer that is gone
/// but has not yet timed out of the membership list looks exactly like a peer that is behind.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_59_removing_the_last_key_a_peer_accepts_is_refused() {
    let timers = TestTimers::fast();
    let old = [0x33; 32];
    let new = [0x44; 32];

    let one = GossipNode::start(timers.config(cluster(), 1, old), hint(1))
        .await
        .expect("start 1");
    let mut two_cfg = timers.config(cluster(), 2, old);
    two_cfg.seeds = vec![one.advertise_addr()];
    let two = GossipNode::start(two_cfg, hint(2)).await.expect("start 2");
    assert!(
        poll_until(timers.converge(), timers.poll_interval(), || find(
            &one.peers(),
            2
        )
        .is_some_and(|h| h.liveness == Liveness::Alive))
        .await,
        "node 2 never became visible within {:?}{}",
        timers.converge(),
        snapshots(&[("one", &one), ("two", &two)])
    );

    // Node 1 finishes its half of the rotation; node 2 has not begun its own.
    one.add_gossip_key(&new).await.expect("add");
    one.use_gossip_key(&new).await.expect("use");

    let refused = one
        .remove_gossip_key(&old, false)
        .await
        .expect_err("node 2 accepts nothing else");
    assert!(
        matches!(refused, GossipError::GossipKeyStillNeeded { peers, .. } if peers >= 1),
        "expected the sole-key refusal, got {refused}"
    );
    assert!(
        !refused.to_string().contains("33"),
        "the refusal names the key by fingerprint, never by value: {refused}"
    );

    // The operator who knows that peer is gone says so, and the removal goes through.
    let after = one.remove_gossip_key(&old, true).await.expect("forced");
    assert_eq!(after.accepted.len(), 1, "{after:#?}");
    assert_eq!(after.accepted[0], after.primary, "{after:#?}");

    two.shutdown().await;
    one.shutdown().await;
}

/// M6-R21: a key a peer is still *signing* with is not removable either, not just a key that
/// peer has nothing else to fall back on.
///
/// The window this covers is the one an operator actually lands in. After the `add` sweep and
/// before the `use` sweep every peer advertises both keys — `is_sole` is false everywhere —
/// while every peer is still signing with the old one. Removing it there makes this node deaf
/// to all of them, which is the outage the refusal exists to prevent, so the refusal has to
/// read the peer's advertised primary and not only its set size.
///
/// Node 2 is driven through the two sweeps one stage at a time: `add` (refusal must hold),
/// then `use` (removal must be allowed). Both halves are asserted against what node 1 can
/// *see*, so the row proves the refusal reads the wire rather than local state.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_r21_removing_a_key_a_peer_still_signs_with_is_refused() {
    let timers = TestTimers::fast();
    let old = [0x3a; 32];
    let new = [0x4b; 32];
    let new_fingerprint = gossip_key_fingerprint(&new);

    let one = GossipNode::start(timers.config(cluster(), 1, old), hint(1))
        .await
        .expect("start 1");
    let mut two_cfg = timers.config(cluster(), 2, old);
    two_cfg.seeds = vec![one.advertise_addr()];
    let two = GossipNode::start(two_cfg, hint(2)).await.expect("start 2");
    assert!(
        poll_until(timers.converge(), timers.poll_interval(), || find(
            &one.peers(),
            2
        )
        .is_some_and(|h| h.liveness == Liveness::Alive))
        .await,
        "node 2 never became visible within {:?}{}",
        timers.converge(),
        snapshots(&[("one", &one), ("two", &two)])
    );

    // The `add` sweep completes everywhere: both nodes hold both keys, both still sign with
    // the old one, and node 1 then finishes its own half by promoting the new key.
    two.add_gossip_key(&new).await.expect("add on 2");
    one.add_gossip_key(&new).await.expect("add on 1");
    one.use_gossip_key(&new).await.expect("use on 1");
    assert!(
        poll_until_async(timers.converge(), timers.poll_interval(), || async {
            trailer_for(&one, 2)
                .await
                .and_then(|extras| extras.accepted_gossip_keys)
                .is_some_and(|keys| keys.len() == 2)
        })
        .await,
        "node 1 never saw node 2 advertise both keys within {:?}{}",
        timers.converge(),
        snapshots(&[("one", &one), ("two", &two)])
    );

    let refused = one
        .remove_gossip_key(&old, false)
        .await
        .expect_err("node 2 is still signing with the old key");
    assert!(
        matches!(refused, GossipError::GossipKeyStillNeeded { peers, .. } if peers >= 1),
        "expected the still-needed refusal mid-sweep, got {refused}"
    );

    // The `use` sweep completes: node 2 now signs with the new key and the old one is only a
    // fallback, which is exactly when dropping it is safe.
    two.use_gossip_key(&new).await.expect("use on 2");
    assert!(
        poll_until_async(timers.converge(), timers.poll_interval(), || async {
            trailer_for(&one, 2)
                .await
                .and_then(|extras| extras.accepted_gossip_keys)
                .is_some_and(|keys| keys.iter().next() == Some(new_fingerprint))
        })
        .await,
        "node 1 never saw node 2 promote the new key within {:?}{}",
        timers.converge(),
        snapshots(&[("one", &one), ("two", &two)])
    );

    let after = one
        .remove_gossip_key(&old, false)
        .await
        .expect("every peer has completed the use sweep");
    assert_eq!(after.accepted.len(), 1, "{after:#?}");
    assert_eq!(after.accepted[0], after.primary, "{after:#?}");

    two.shutdown().await;
    one.shutdown().await;
}

/// The trailer a peer reads must be the keyring, not a copy of it that a rotation forgot to
/// update. Asserted off `member_meta`, which is the bytes actually advertised.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_58_a_rotation_advertises_the_keys_it_now_accepts() {
    let timers = TestTimers::fast();
    let node = GossipNode::start(timers.config(cluster(), 1, [0x55; 32]), hint(1))
        .await
        .expect("start");

    let keyring = node.add_gossip_key(&[0x66; 32]).await.expect("add");
    let meta = node.member_meta().await;
    let trailer = decode_hint_extras(meta.first().expect("this node advertises"))
        .and_then(|extras| extras.accepted_gossip_keys)
        .expect("a trailer after a rotation");
    assert_eq!(
        trailer,
        AcceptedGossipKeys::new(keyring.accepted.iter().copied()),
        "the advertised set must be the keyring's, in the keyring's order"
    );

    node.shutdown().await;
}

/// A node gossiping in plaintext has no keyring, and says so rather than reporting an empty one.
/// "No keys" and "encryption is off" are different operator problems.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_60_an_unencrypted_node_has_no_keyring_to_rotate() {
    let timers = TestTimers::fast();
    let mut cfg = timers.config(cluster(), 1, [0x77; 32]);
    cfg.secret_key = None;
    let node = GossipNode::start(cfg, hint(1)).await.expect("start");

    assert!(node.keyring().is_none());
    let refused = node
        .add_gossip_key(&[0x88; 32])
        .await
        .expect_err("nothing to add a key to");
    assert!(
        matches!(refused, GossipError::Keyring(ref detail) if detail.contains("not encrypted")),
        "{refused}"
    );

    node.shutdown().await;
}
