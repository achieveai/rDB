//! The automatic dedup resubmit outlives an election (E2E-38 on Linux).
//!
//! The unknown outcome that triggers a resubmit is usually the leader dying. The resubmit
//! runs at once, while the survivors still have no leader and answer `Unavailable`. Those
//! answers prove nothing was applied, and the dedup key makes asking again safe, so the
//! client must ask again until the new leader answers instead of handing the caller an
//! error the resubmit was meant to absorb.

mod support;

use std::time::{Duration, Instant};

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions};
use config_core::{
    Capabilities, ConfigError, ConfigStore, Dedup, DeleteRequest, MutationOutcome,
    MutationResponse, PutRequest,
};
use config_log::retcd_test;

use support::{grpc_timeout_nanos, start_spy, Node};

fn put() -> PutRequest {
    PutRequest {
        dedup: None,
        key: Bytes::from_static(b"/app/a"),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: None,
    }
}

fn delete() -> DeleteRequest {
    DeleteRequest {
        dedup: None,
        key: Bytes::from_static(b"/app/a"),
        expected_mod_revision: None,
    }
}

fn leader_unknown() -> ConfigError {
    ConfigError::Unavailable {
        reason: "leader unknown".into(),
    }
}

fn dedup_client(node: &Node, budget: Duration) -> GrpcClient {
    dedup_client_at(node.endpoint.clone(), budget)
}

fn dedup_client_at(endpoint: String, budget: Duration) -> GrpcClient {
    GrpcClient::connect(
        vec![endpoint],
        GrpcClientOptions {
            request_deadline: budget,
            expected_capabilities: Some(Capabilities {
                dedup: Dedup::Bounded {
                    window_requests: 64,
                },
                ..Capabilities::EPHEMERAL_DEVELOPMENT
            }),
            ..Default::default()
        },
    )
    .expect("client connects")
    .with_dedup([0x38; 16])
}

/// Call 1 is the unknown outcome; the next three resubmits find no leader; the fourth is
/// answered. The caller must see `Applied`, after the pauses between the asks (100 + 200 +
/// 400 ms). Counted in calls, not wall clock, so no test sleeps.
#[retcd_test]
async fn a_resubmit_that_finds_no_leader_asks_again_until_one_answers() {
    let node = Node::start().await;
    node.store.fail_first(vec![
        ConfigError::DeadlineExceededUnknownOutcome,
        leader_unknown(),
        leader_unknown(),
        leader_unknown(),
    ]);
    node.store.set_mutation(MutationResponse::applied_put(7));
    let client = dedup_client(&node, Duration::from_secs(5));

    let started = Instant::now();
    let response = client
        .put(put())
        .await
        .expect("the resubmit rides out the election");

    assert_eq!(response.outcome, MutationOutcome::Applied);
    assert_eq!(response.revision, 7);
    assert_eq!(
        node.store.call_count(),
        5,
        "1 unknown + 3 no leader + 1 answer"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(600),
        "the asks were paced: {:?}",
        started.elapsed()
    );
    node.shutdown().await;
}

/// The control: no leader ever appears. The asking stops with the budget, and the caller gets
/// the unknown outcome the first attempt left: every `Unavailable` speaks only for its own ask,
/// so reporting one would call a mutation that may have applied safe to send again. Cut: return
/// the last `Unavailable`, and the `matches!` is red.
#[retcd_test]
async fn a_resubmit_that_never_finds_a_leader_ends_unknown_at_the_budget() {
    const BUDGET: Duration = Duration::from_millis(1000);
    for op in ["put", "delete"] {
        let node = Node::start().await;
        node.store
            .fail_first(vec![ConfigError::DeadlineExceededUnknownOutcome]);
        node.store.set_error(Some(leader_unknown()));
        let client = dedup_client(&node, BUDGET);

        let started = Instant::now();
        let error = match op {
            "put" => client.put(put()).await.map(|_| ()),
            _ => client.delete(delete()).await.map(|_| ()),
        }
        .expect_err("there is no leader");
        let elapsed = started.elapsed();

        assert!(
            matches!(error, ConfigError::DeadlineExceededUnknownOutcome),
            "{op}: {error:?}"
        );
        assert!(!error.is_safe_to_resubmit(), "{op}");
        assert!(
            elapsed >= BUDGET - Duration::from_millis(500),
            "{op}: {elapsed:?}"
        );
        assert!(
            elapsed < BUDGET + Duration::from_millis(800),
            "{op}: {elapsed:?}"
        );
        node.shutdown().await;
    }
}

/// The delete path of [`a_resubmit_that_finds_no_leader_asks_again_until_one_answers`]: delete
/// runs the same resubmit, with its own closure. Cut: delete returns the first unknown outcome
/// without resubmitting, and this row is red at the `expect`.
#[retcd_test]
async fn a_delete_after_an_unknown_outcome_asks_again_until_one_answers() {
    let node = Node::start().await;
    node.store.fail_first(vec![
        ConfigError::DeadlineExceededUnknownOutcome,
        leader_unknown(),
        leader_unknown(),
        leader_unknown(),
    ]);
    node.store.set_mutation(MutationResponse::applied_delete(9));
    let client = dedup_client(&node, Duration::from_secs(5));

    let response = client
        .delete(delete())
        .await
        .expect("the resubmit rides out the election");

    assert_eq!(response.outcome, MutationOutcome::Applied);
    assert_eq!(response.revision, 9);
    assert_eq!(
        node.store.call_count(),
        5,
        "1 unknown + 3 no leader + 1 answer"
    );
    node.shutdown().await;
}

/// One deadline covers every ask of a resubmit. Each ask here is answered at once, so the 100 +
/// 200 + 400 ms of pauses leave at most 300 ms of a 1 s budget for the ask that stalls. A timed
/// out submitted call stays unknown, for a put and for a delete. Cut: the resubmit gives each
/// ask a fresh budget, and the last timeout is about 1 s and the call runs to about 1.7 s.
#[retcd_test]
async fn a_resubmit_ask_that_stalls_gets_only_what_is_left_of_one_deadline() {
    const BUDGET: Duration = Duration::from_millis(1000);
    for op in ["put", "delete"] {
        let server = start_spy().await;
        server.spy.answer_first(vec![
            ConfigError::DeadlineExceededUnknownOutcome,
            leader_unknown(),
            leader_unknown(),
            leader_unknown(),
        ]);
        let client = dedup_client_at(server.endpoint.clone(), BUDGET);

        let started = Instant::now();
        let error = match op {
            "put" => client.put(put()).await.map(|_| ()),
            _ => client.delete(delete()).await.map(|_| ()),
        }
        .expect_err("the stalled ask is never answered");
        let elapsed = started.elapsed();

        assert!(
            matches!(error, ConfigError::DeadlineExceededUnknownOutcome),
            "{op}: {error:?}"
        );
        assert!(
            elapsed < BUDGET + Duration::from_millis(500),
            "{op}: {elapsed:?}"
        );
        let timeouts = server.spy.timeouts();
        assert_eq!(
            timeouts.len(),
            5,
            "{op}: 1 unknown + 3 no leader + 1 stall: {timeouts:?}"
        );
        let last = grpc_timeout_nanos(timeouts.last().expect("five calls"));
        assert!(
            last > 0 && last <= Duration::from_millis(300).as_nanos(),
            "{op}: the stalled ask had {last} ns: {timeouts:?}"
        );
    }
}

/// Unchanged rule: a second unknown outcome is not asked again.
#[retcd_test]
async fn a_second_unknown_outcome_still_stops_the_resubmit() {
    let node = Node::start().await;
    node.store
        .set_error(Some(ConfigError::DeadlineExceededUnknownOutcome));
    let client = dedup_client(&node, Duration::from_secs(5));

    let error = client.put(put()).await.expect_err("unknown twice");

    assert!(
        matches!(error, ConfigError::DeadlineExceededUnknownOutcome),
        "{error:?}"
    );
    assert_eq!(
        node.store.call_count(),
        2,
        "one send and one resubmit, no more"
    );
    node.shutdown().await;
}
