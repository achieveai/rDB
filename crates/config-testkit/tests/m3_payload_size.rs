//! M3 transport payload sizing (test plan §4.10, rows M3-86..M3-87).
//!
//! Both rows exist because tonic refuses to decode a message larger than 4 MiB unless it is
//! told otherwise, and `config-core`'s [`Limits`] permit larger messages on *both* planes:
//!
//! * **M3-86 (peer plane).** One `AppendEntries` may legally carry `MAX_PAYLOAD_ENTRIES`
//!   commands of up to `max_request_bytes` each, which is far above 4 MiB even though the
//!   payload is postcard and so does not expand the bytes it carries. Under the default cap
//!   the follower answers `OUT_OF_RANGE`, OpenRaft reads
//!   that as retryable, and replication wedges rather than failing: the put never commits and
//!   the row times out. The value is all-`0xFF` deliberately — it was the worst case of the
//!   retired serde-JSON encoding, and it is the case a caller can choose.
//! * **M3-87 (client plane).** A `List` reply is filled to [`Limits::max_list_bytes`] *before*
//!   the server sets `truncated`, so under the default cap a client asking for a large prefix
//!   gets a transport failure instead of the truncated page §10.2 promises it.
//!
//! Anti-flake: no fixed sleeps. Leader election and replication are awaited with the harness's
//! own pollers (`wait_for_leader`, `wait_revision_all`), and every voter is inspected through
//! its applied state rather than through a `DirectClient` — a `DirectClient` on a follower
//! answers `NotLeader` by design, so "readable from every voter" has to be asserted against the
//! replicated state machine.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_core::{ConfigStore, Limits, ListRequest, MutationOutcome, PutRequest};
use config_testkit::cluster::{Cluster, ClusterBuilder, StorageKind};

use support::key;

/// One maximum-size value under [`Limits::DEFAULT`].
const MAX_VALUE_BYTES: usize = 1024 * 1024;

/// `List` byte budget for M3-87: above tonic's 4 MiB default (so the reply cannot be decoded
/// without the derived cap) and below [`Limits::DEFAULT`]'s 8 MiB (so the row writes 6 MiB
/// through Raft rather than 9 and still finishes well inside its budget).
const LIST_BYTE_BUDGET: u64 = 6 * 1024 * 1024;

/// tonic's own default receive cap — the number both rows must beat to mean anything.
const TONIC_DEFAULT_RECV_CAP: u64 = 4 * 1024 * 1024;

/// Client-side budget for a maximum-size call.
///
/// Generous rather than tight: these rows move megabytes through Raft in an unoptimized build,
/// and the assertion is about the size caps, not about latency.
const BIG_PAYLOAD_TIMEOUT: Duration = Duration::from_secs(10);

fn max_size_value() -> Bytes {
    Bytes::from(vec![0xFFu8; MAX_VALUE_BYTES])
}

fn put_value(k: &str, value: Bytes) -> PutRequest {
    PutRequest {
        key: key(k),
        value,
        expected_mod_revision: None,
    }
}

/// A three-node ephemeral cluster with client budgets sized for megabyte payloads.
///
/// Raft timers are the harness default (250 ms heartbeat). OpenRaft bounds one `AppendEntries`
/// by `heartbeat_interval`, and a maximum-size value fits that budget now that the payload is
/// postcard rather than serde JSON: the leader ships 1 MiB, not 4.2 MiB of decimal digits, and
/// neither end pays to render or parse them.
fn big_payload_cluster() -> ClusterBuilder {
    Cluster::builder()
        .nodes(3)
        .storage(StorageKind::Ephemeral)
        .timeouts(BIG_PAYLOAD_TIMEOUT, BIG_PAYLOAD_TIMEOUT)
}

/// M3-86: a `Put` carrying a maximum-size all-`0xFF` value commits on a three-node cluster and
/// is present in every voter's applied state.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_86_max_size_value_replicates_to_every_voter() {
    const KEY: &str = "/m3-86/max-size";

    let cluster = big_payload_cluster().start().await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(5))
        .await
        .expect("a leader elects");

    let value = max_size_value();
    let response = cluster
        .grpc_client(leader)
        .put(put_value(KEY, value.clone()))
        .await
        .expect("a value at the configured maximum is a legal write, not a transport failure");
    assert_eq!(
        response.outcome,
        MutationOutcome::Applied,
        "an unconditional put of a legal value must apply"
    );

    // Replication is what the peer-plane cap actually governs: the leader can apply locally
    // whether or not the followers can receive the entry.
    cluster
        .wait_revision_all(response.revision, cluster.deadline(5))
        .await
        .expect("every voter applies the oversize entry");

    for id in cluster.ids() {
        let mut seen: Option<Bytes> = None;
        cluster
            .store(id)
            .reader()
            .with_state(&mut |state| seen = state.get(KEY.as_bytes()).map(|r| r.value.clone()));
        assert_eq!(
            seen.as_ref(),
            Some(&value),
            "node {id} does not hold the replicated value"
        );
    }

    cluster.shutdown().await;
}

/// M3-87: a `List` whose reply exceeds tonic's default receive cap comes back truncated to the
/// gRPC client, rather than as a transport error.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_87_oversize_list_reply_is_truncated_not_unavailable() {
    const PREFIX: &str = "/m3-87/";
    /// One more maximum-size record than [`LIST_BYTE_BUDGET`] can hold, so the server has to
    /// stop early and say so.
    const RECORDS: usize = 6;

    let cluster = big_payload_cluster()
        .limits(Limits {
            max_list_bytes: LIST_BYTE_BUDGET,
            ..Limits::DEFAULT
        })
        .start()
        .await;
    let leader = cluster
        .wait_for_leader(cluster.deadline(5))
        .await
        .expect("a leader elects");

    // Written through the embedded client: the row is about the *reply*, and routing six
    // megabytes of setup through the client plane would only slow it down.
    let writer = cluster.client(leader);
    let mut last_revision = 0;
    for i in 0..RECORDS {
        let response = writer
            .put(put_value(&format!("{PREFIX}{i:02}"), max_size_value()))
            .await
            .expect("a maximum-size value is a legal write");
        last_revision = response.revision;
    }
    cluster
        .wait_revision_all(last_revision, cluster.deadline(5))
        .await
        .expect("every voter applies the prefix");

    let listed = cluster
        .grpc_client(leader)
        .list(ListRequest {
            prefix: key(PREFIX),
            ..Default::default()
        })
        .await
        .expect("an over-cap prefix must answer with a truncated page, not a transport error");

    assert!(
        listed.truncated,
        "the server filled its {LIST_BYTE_BUDGET}-byte budget and must report truncation"
    );
    let weight: u64 = listed
        .records
        .iter()
        .map(|r| Limits::list_record_cost(r.key.len(), r.value.len()))
        .sum();
    assert!(
        weight > TONIC_DEFAULT_RECV_CAP,
        "a reply of {weight} bytes would fit tonic's default cap, so this row proves nothing"
    );
    assert!(
        weight <= LIST_BYTE_BUDGET,
        "the server must not exceed the byte budget it truncates against, got {weight}"
    );
    assert!(
        listed.records.len() < RECORDS,
        "truncation means fewer records than were written"
    );

    cluster.shutdown().await;
}
