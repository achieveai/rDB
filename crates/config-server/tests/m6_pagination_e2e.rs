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
//! The first row is a **new** one, not the plan's E2E-44. E2E-44 is the *failover* case — kill
//! the leader mid-walk and present the token to its successor — and it is still uncovered; the
//! row below deliberately keeps one leader, because the mid-walk-write claim is only sharp when
//! the node serving the walk is the node applying the write.
//!
//! The second row is the test plan's **M6-32**, which needs the same two things this file
//! already assembles — a real `[list]` section and a real client walk — plus a real signed-policy
//! adoption. See its own doc comment for why it cannot live in `config-engine`.
//!
//! Anti-flake: no fixed sleeps and no literal ports — every address comes from the harness's
//! reserved listeners, and every wait is a bounded poll derived from the cluster's own timers.

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{
    ConfigError, ConfigStore, ListRequest, MutationOutcome, PageRequest, PageTokenExpiredReason,
    PutRequest,
};
use config_log::retcd_test;
use config_testkit::poll::{poll_until_async, Timeout};

use support::{
    deadline, DaemonProcess, Harness, Health, ListTuning, NodeOptions, PolicyFixture, PRINCIPAL,
};

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

/// Write [`POPULATION`] keys under [`PREFIX`], so that a walk of [`PAGE`] records paginates.
async fn seed_prefix(client: &GrpcClient) {
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
}

/// Poll `endpoint`'s health until it reports `version` as the active signed policy.
///
/// The observable for "the daemon has adopted the document that is now on disk": `/health`
/// publishes the authorizer's own version, so waiting on it waits on the adoption itself rather
/// than on the poll interval elapsing (M6-16).
async fn wait_policy_version(endpoint: &str, version: u64) {
    let result = poll_until_async(deadline(10), Duration::from_millis(50), || async {
        (support::health(endpoint).await.policy_version == Some(version)).then_some(())
    })
    .await;
    if let Err(Timeout { elapsed, .. }) = result {
        let last = support::health(endpoint).await;
        panic!("policy version {version} was not adopted within {elapsed:?}; health was {last:#?}");
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
            token_key_file: None,
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

    seed_prefix(&client).await;

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

/// How often the M6-32 node re-reads its policy files. One second, so the bounded poll for the
/// adoption is short; the wait is still derived from a deadline, never slept through.
const POLICY_POLL_SECS: u64 = 1;

/// M6-32: a page token minted under one signed policy document is refused, by name, once the
/// daemon has adopted the next one (ADR-0027 §15.3, ADR-0029).
///
/// `config-engine`'s M6-71 proves the paginator's own rule by storing into the shared cell
/// directly, which is the only thing a library row can do. What only a process can show is that
/// the daemon *binds* that cell at all: until this row existed `Paginator::bind_policy_version`
/// had no caller outside that one test, so every token a running node minted sealed
/// `policy_version: None` and `PageTokenExpiredReason::PolicyVersion` was unreachable in
/// production. The adoption here is therefore a real one — a newly signed document dropped on
/// disk and picked up by the node's own poller — and the token is a real one, minted by the
/// daemon and carried back over the client plane.
#[retcd_test]
async fn m6_32_a_policy_adoption_invalidates_an_outstanding_page_token() {
    const METHOD: &str = "m6_32_a_policy_adoption_invalidates_an_outstanding_page_token";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let fixture = PolicyFixture::new(harness.root());
    fixture.write(1, &[""], &["root"]);
    let options = NodeOptions {
        // Signed mode and the static allowlist are mutually exclusive (M6-37), so the harness
        // default policy has to be cleared rather than merely overridden.
        policy: None,
        signed_policy: Some(fixture.authz(POLICY_POLL_SECS)),
        // A TTL far longer than this row takes, so an expiry can never stand in for the
        // policy-version refusal the row is about.
        list: Some(ListTuning {
            max_pinned_snapshots: 4,
            ttl_seconds: 600,
            token_key_file: None,
        }),
        ..harness.node_options()
    };
    harness.write_node_files(&harness.nodes[0], &options);

    let mut node = harness.start(0, true);
    let nodes = std::slice::from_ref(&node);
    wait_formed(nodes).await;
    let endpoint = node.health_endpoint().to_string();
    let client = cluster_client(&harness, nodes);
    seed_prefix(&client).await;

    let request = ListRequest {
        prefix: Bytes::from(PREFIX),
        max_items: PAGE,
        max_bytes: 0,
    };
    let first = client
        .list_page(PageRequest::first(request.clone()))
        .await
        .expect("the daemon serves the first page");
    let token = first
        .next_page_token
        .expect("the seeded population does not fit in one page, so page one has a cursor");

    // The adoption: a second signed document, with the same grants, reaching the node the way an
    // operator's deploy would. Same grants on purpose — a narrowed one would let a plain
    // `PermissionDenied` pass for the refusal this row is about.
    fixture.write(2, &[""], &["root"]);
    wait_policy_version(&endpoint, 2).await;

    // `/health` reports the *authorizer's* version, which the daemon publishes one statement
    // before it republishes the version page tokens are sealed against (see the ordering note in
    // `config-server/src/policy.rs`; the lag is deliberate and is the safe direction). So health
    // reaching 2 does not by itself prove the token path has caught up, and a single shot here
    // would flake if the reload thread were descheduled across this request. The token is a
    // sealed value, so resending it is free, and once the refusal appears it is permanent.
    let error = poll_until_async(deadline(5), Duration::from_millis(20), || async {
        client
            .list_page(PageRequest::resume(request.clone(), token.clone()))
            .await
            .err()
    })
    .await
    .expect("the page token outlived the policy adoption that /health had already reported");
    assert!(
        matches!(
            error,
            ConfigError::PageTokenExpired {
                reason: PageTokenExpiredReason::PolicyVersion
            }
        ),
        "the grants the walk started under are no longer in force, and the refusal has to say \
         so by name rather than as an eviction or a node mismatch: {error:?}"
    );

    node.stop_gracefully(deadline(10)).await;
}
