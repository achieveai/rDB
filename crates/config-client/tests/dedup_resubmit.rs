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
    Capabilities, ConfigError, ConfigStore, Dedup, MutationOutcome, MutationResponse, PutRequest,
};
use config_log::retcd_test;

use support::Node;

fn put() -> PutRequest {
    PutRequest {
        dedup: None,
        key: Bytes::from_static(b"/app/a"),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: None,
    }
}

fn leader_unknown() -> ConfigError {
    ConfigError::Unavailable {
        reason: "leader unknown".into(),
    }
}

fn dedup_client(node: &Node, budget: Duration) -> GrpcClient {
    GrpcClient::connect(
        vec![node.endpoint.clone()],
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

/// The control: no leader ever appears. The asking stops with the budget, and the caller
/// gets the plain `Unavailable`, which is safe to resubmit.
#[retcd_test]
async fn a_resubmit_that_never_finds_a_leader_ends_unavailable_at_the_budget() {
    const BUDGET: Duration = Duration::from_millis(1000);
    let node = Node::start().await;
    node.store
        .fail_first(vec![ConfigError::DeadlineExceededUnknownOutcome]);
    node.store.set_error(Some(leader_unknown()));
    let client = dedup_client(&node, BUDGET);

    let started = Instant::now();
    let error = client.put(put()).await.expect_err("there is no leader");
    let elapsed = started.elapsed();

    assert!(
        matches!(error, ConfigError::Unavailable { .. }),
        "{error:?}"
    );
    assert!(error.is_safe_to_resubmit());
    assert!(
        elapsed >= BUDGET - Duration::from_millis(500),
        "{elapsed:?}"
    );
    assert!(elapsed < BUDGET + Duration::from_millis(800), "{elapsed:?}");
    node.shutdown().await;
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
