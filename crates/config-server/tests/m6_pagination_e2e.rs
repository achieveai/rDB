//! M6 daemon row: a revision-pinned walk across a real `config-server` process (ADR-0029).
//!
//! Every other M6 row runs against a library or a scripted double. This one exists for the two
//! claims that only a spawned daemon can make:
//!
//! 1. The `[list]` section is actually *reached*. `config.rs` parses it and `run.rs` builds the
//!    node's one `Paginator` from it; a walk that succeeds against a process started from a
//!    document proves both, where an in-process test proves neither.
//! 2. The pin survives a **leader-side write taken mid-walk**. That is the property the whole
//!    milestone is for, and it is only meaningful against real consensus: the write is
//!    committed and applied by the same node that is serving the walk, and the walk still
//!    reports its original revision and never shows the new key.
//!
//! This is a **new** row, not the plan's E2E-44. E2E-44 is the *failover* case — kill the
//! leader mid-walk and present the token to its successor — and it is still uncovered; the row
//! below deliberately keeps one leader, because the mid-walk-write claim is only sharp when the
//! node serving the walk is the node applying the write.
//!
//! Anti-flake: no fixed sleeps and no literal ports — every address comes from the harness's
//! reserved listeners, and every wait is a bounded poll derived from the cluster's own timers.

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigStore, ListRequest, MutationOutcome, PutRequest};
use config_log::retcd_test;
use config_testkit::poll::{poll_until_async, Timeout};

use support::{deadline, DaemonProcess, Harness, Health, ListTuning, NodeOptions, PRINCIPAL};

/// Keys written under [`PREFIX`]. Comfortably more than three pages, so a walk that silently
/// served one wide `List` would be visible as a single page rather than as a short read.
const POPULATION: usize = 25;

/// Records per page. Small on purpose: the row is about page boundaries, not about volume.
const PAGE: u32 = 4;

/// The prefix the walk covers. A second, disjoint prefix holds the mid-walk write.
const PREFIX: &str = "/m6/";

/// A client over the whole cluster, presenting the granted principal's certificate.
fn cluster_client(harness: &Harness, nodes: &[DaemonProcess]) -> GrpcClient {
    let opts = GrpcClientOptions {
        request_deadline: deadline(5),
        tls: TlsMode::MutualTls(harness.tls.client_mtls(PRINCIPAL)),
        ..GrpcClientOptions::default()
    };
    GrpcClient::connect(
        nodes
            .iter()
            .map(|n| n.client_endpoint().to_string())
            .collect(),
        opts,
    )
    .expect("the client plane endpoints are well formed")
    .with_cluster_id(harness.cluster_id)
}

/// Wait until every node agrees on one leader and the full voter set.
async fn wait_formed(nodes: &[DaemonProcess]) {
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let voters: Vec<u64> = nodes.iter().map(DaemonProcess::node_id).collect();
    let result = poll_until_async(deadline(10), Duration::from_millis(50), || async {
        let mut payloads = Vec::with_capacity(endpoints.len());
        for endpoint in &endpoints {
            payloads.push(support::health(endpoint).await);
        }
        payloads
            .iter()
            .all(|p: &Health| {
                p.membership_voter_ids == voters && p.current_leader.is_some() && p.ready
            })
            .then_some(())
    })
    .await;
    if let Err(Timeout { elapsed, .. }) = result {
        panic!("the cluster did not form within {elapsed:?}");
    }
}

fn key(index: usize) -> Bytes {
    // Zero padded so byte order and numeric order agree; the walk's ordering claim would be
    // vacuous against `/m6/10` sorting before `/m6/2`.
    Bytes::from(format!("{PREFIX}{index:04}"))
}

/// E2E-M6-WIRING (new row): a daemon started with a `[list]` section serves a pinned walk that returns every key
/// exactly once and does not observe a write committed while the walk is open.
#[retcd_test]
async fn e2e_m6_wiring_a_pinned_walk_returns_every_key_once_and_ignores_a_mid_walk_write() {
    let harness = Harness::new(
        "e2e_m6_wiring_a_pinned_walk_returns_every_key_once_and_ignores_a_mid_walk_write",
    )
    .await;
    // Four pins is well under what one walk needs (one) and small enough that an implementation
    // leaking a pin per page would exhaust the table inside this row rather than pass quietly.
    let options = NodeOptions {
        list: Some(ListTuning {
            max_pinned_snapshots: 4,
            ttl_seconds: 60,
        }),
        ..harness.node_options()
    };
    for node in &harness.nodes {
        harness.write_node_files(node, &options);
    }

    // The daemons are started from these documents, so this is what makes the row's first
    // claim checkable: a `[list]` section that never reached the file would leave the walk
    // below passing on the built-in defaults and proving nothing about the wiring.
    let document = std::fs::read_to_string(&harness.nodes[0].config).expect("read the document");
    assert!(
        document.contains("[list]") && document.contains("max_pinned_snapshots = 4"),
        "the node document carries the section under test:
{document}"
    );

    let mut nodes = harness.start_all();
    wait_formed(&nodes).await;
    let client = cluster_client(&harness, &nodes);

    for index in 0..POPULATION {
        let response = client
            .put(PutRequest {
                key: key(index),
                value: Bytes::from_static(b"v"),
                expected_mod_revision: None,
                dedup: None,
            })
            .await
            .expect("the granted principal may write");
        assert_eq!(response.outcome, MutationOutcome::Applied);
    }

    let request = ListRequest {
        prefix: Bytes::from(PREFIX),
        max_items: PAGE,
        max_bytes: 0,
    };
    let mut walk = client.list_pages(request);

    // Page one opens the pin. Everything after it is read from that snapshot.
    let first = walk
        .next_page()
        .await
        .expect("a walk over a populated prefix has a first page")
        .expect("the daemon serves the walk");
    let pinned_revision = first.revision;
    assert_eq!(first.items.len(), PAGE as usize, "the page cap is honoured");
    assert!(
        first.next_page_token.is_some(),
        "{POPULATION} keys do not fit in one page of {PAGE}"
    );

    // The mid-walk write: committed and applied by the same leader that is serving the walk,
    // under the prefix the walk covers, so an unpinned implementation could not miss it.
    let intruder = Bytes::from(format!("{PREFIX}9999"));
    let write = client
        .put(PutRequest {
            key: intruder.clone(),
            value: Bytes::from_static(b"late"),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("the write lands");
    assert_eq!(write.outcome, MutationOutcome::Applied);
    assert!(
        write.revision > pinned_revision,
        "the write must be strictly later than the pin for this row to mean anything"
    );

    let mut keys: Vec<Bytes> = first.items.into_iter().map(|r| r.key).collect();
    while let Some(page) = walk.next_page().await {
        let page = page.expect("the token stays valid for the life of the walk");
        assert_eq!(
            page.revision, pinned_revision,
            "every page of one walk reports the revision page one pinned"
        );
        keys.extend(page.items.into_iter().map(|r| r.key));
    }

    let unique: BTreeSet<Bytes> = keys.iter().cloned().collect();
    assert_eq!(unique.len(), keys.len(), "no key was returned twice");
    assert_eq!(
        keys,
        (0..POPULATION).map(key).collect::<Vec<_>>(),
        "the walk returns every key once, in order"
    );
    assert!(
        !unique.contains(&intruder),
        "the pinned walk must not observe a revision later than the one it pinned"
    );

    // And the key really is there afterwards — otherwise the assertion above would pass against
    // a daemon that simply failed to apply the write. A `Get`, not a `List`: the intruder sorts
    // last under the prefix, so a capped list would answer "absent" for a reason that has
    // nothing to do with pinning.
    let after = client
        .get(config_core::GetRequest {
            key: intruder.clone(),
        })
        .await
        .expect("an unpinned read");
    assert!(
        after.record.is_some(),
        "the mid-walk write is visible to a fresh read at revision {}",
        after.read_revision
    );

    for node in &mut nodes {
        node.stop_gracefully(deadline(10)).await;
    }
}
