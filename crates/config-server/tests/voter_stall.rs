//! Issue #80 S2d, design row 23: one control voter stops running for 3 s under a renewal load.
//!
//! A stopped process's kernel still accepts TCP and the connection stays up, so this is the case
//! `connect_timeout` cannot see and HTTP/2 keepalive exists for. Real daemons, real mutual TLS.
#![cfg(unix)]

mod support;

use std::time::{Duration, Instant};

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigError, ConfigStore, GetRequest, PutRequest};
use config_log::retcd_test;
use config_testkit::poll::poll_until_async;

use support::{deadline, wait_formed, Harness, PRINCIPAL};

/// How long a voter stays stopped.
const STALL: Duration = Duration::from_secs(3);

/// A renewal is due every 500 ms.
const RENEW_EVERY: Duration = Duration::from_millis(500);

/// What a grant must still cover after a voter stalls: `write_horizon_floor_millis` at the
/// product default (6000 - 100 - 100 - 1 - 500).
const HORIZON_FLOOR: Duration = Duration::from_millis(5_299);

fn signal(pid: u32, which: &str) {
    let status = std::process::Command::new("kill")
        .args([which, &pid.to_string()])
        .status()
        .expect("run kill");
    assert!(status.success(), "kill {which} {pid}: {status}");
}

fn client(harness: &Harness, endpoints: Vec<String>) -> GrpcClient {
    GrpcClient::connect(
        endpoints,
        GrpcClientOptions {
            tls: TlsMode::MutualTls(harness.tls.client_mtls(PRINCIPAL)),
            request_deadline: Duration::from_secs(5),
            ..GrpcClientOptions::default()
        },
    )
    .expect("the client plane endpoints are well formed")
    .with_cluster_id(harness.cluster_id)
}

fn renewal(i: u64) -> PutRequest {
    PutRequest {
        dedup: None,
        key: Bytes::from_static(b"/renewal"),
        value: Bytes::from(i.to_string()),
        expected_mod_revision: None,
    }
}

/// Stop the leader, the worst voter to lose and first in the client's list, for 3 s. Writes on
/// the renewal cadence carry on inside the horizon, and once it resumes the same address answers
/// again on a new connection.
/// Cut: no keepalive in `build_endpoint`, and a write hangs for its whole 5 s deadline.
/// Integration (~10 s): three daemons, the stall, then the resume.
#[retcd_test]
async fn a_voter_stopped_for_3_s_leaves_renewals_inside_the_horizon_and_reconnects() {
    let harness =
        Harness::new("a_voter_stopped_for_3_s_leaves_renewals_inside_the_horizon_and_reconnects")
            .await;
    let nodes = harness.start_all();
    let health = wait_formed(&harness, &nodes).await;
    let leader = health[0].current_leader.expect("formed with a leader");
    let stalled = nodes
        .iter()
        .find(|node| node.node_id() == leader)
        .expect("the leader is one of the daemons");
    let mut endpoints = vec![stalled.client_endpoint().to_string()];
    endpoints.extend(
        nodes
            .iter()
            .filter(|node| node.node_id() != leader)
            .map(|node| node.client_endpoint().to_string()),
    );
    let renewals = client(&harness, endpoints);
    renewals.put(renewal(0)).await.expect("the first renewal");

    signal(stalled.pid(), "-STOP");
    let stopped_at = Instant::now();
    let mut resumed = false;
    let mut last_ack = stopped_at;
    let mut longest = Duration::ZERO;
    let mut acked = 0u64;
    let mut outcomes = Vec::new();
    for i in 1.. {
        let elapsed = stopped_at.elapsed();
        if !resumed && elapsed >= STALL {
            signal(stalled.pid(), "-CONT");
            resumed = true;
        }
        if elapsed >= STALL * 2 {
            break;
        }
        let result = renewals.put(renewal(i)).await;
        if result.is_ok() {
            acked += 1;
            longest = longest.max(last_ack.elapsed());
            last_ack = Instant::now();
        }
        outcomes.push(result.map(|_| ()).map_err(|error| error.to_string()));
        tokio::time::sleep(RENEW_EVERY).await; // testkit:allow-sleep: the renewal cadence is the load
    }
    assert!(resumed, "the voter was resumed");
    assert!(acked >= 3, "renewals went on: {outcomes:?}");
    assert!(
        longest < HORIZON_FLOOR,
        "longest gap between renewals {longest:?}: {outcomes:?}"
    );

    // The stalled address answers again, on a channel made after the resume. A follower's
    // `NotLeader` is an answer: it came from the process that was stopped.
    let direct = client(&harness, vec![stalled.client_endpoint().to_string()]);
    poll_until_async(deadline(10), Duration::from_millis(100), || async {
        let answer = direct
            .get(GetRequest {
                key: Bytes::from_static(b"/renewal"),
            })
            .await;
        matches!(answer, Ok(_) | Err(ConfigError::NotLeader { .. })).then_some(())
    })
    .await
    .expect("the resumed voter answers");
}
