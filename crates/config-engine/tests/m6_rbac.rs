//! Watch behaviour across a signed-policy rotation (M6; spec §15.3 bullets 4 and 6, ADR-0027).
//!
//! These are the rbac rows that need the *engine*: the journal gate, the live broadcast and the
//! per-stream delivery task. The signature, rollback and intersection rows are pure computation
//! and live in `config-core/tests/m6_rbac.rs`; nothing here re-verifies a signature, so nothing
//! here needs a signing key — a [`SignedPolicy`] is built directly from the document it hashes,
//! exactly as `verify_policy` would have returned it.
//!
//! Nothing sleeps to synchronize. The ordering row drives its interleaving through the M4
//! [`GateHook`] pause points, which is the only way "terminated before the first enqueue under
//! the new version" can be a claim rather than a hope (test plan M6-31, anti-flake rule 1).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{key, put_request, Cluster};
use config_core::policy::{document_hash, grant, SignedPolicy, SignedPolicyAuthorizer};
use config_core::{
    Action, Authorizer, Grant, MutationEvent, PolicyDocument, Principal, PrincipalKind, WatchItem,
    WatchRequest, WatchStream, REASON_POLICY_CHANGED, REASON_POLICY_CONVERGING,
};
use config_engine::watch::testing::GateHook;
use config_engine::TerminationReason;
use futures::StreamExt;

/// A failure bound, never a success bound.
const DEADLINE: Duration = Duration::from_secs(10);

/// Long enough that no row below ever sees a progress frame it did not ask for.
const NO_PROGRESS: Duration = Duration::from_secs(3600);

/// The principal every row authorizes as. `Embedded` because a signed policy refuses an
/// unverified kind outright, and that refusal is M3's row, not one of these.
fn app() -> Principal {
    Principal::new("app", PrincipalKind::Embedded)
}

fn rw(principal: &str, prefix: &str) -> Grant {
    grant(principal, prefix, &[Action::Read, Action::Write])
}

/// A document, encoded and hashed exactly as a verified one would have been.
fn signed(version: u64, grants: Vec<Grant>) -> SignedPolicy {
    let document = PolicyDocument {
        version,
        issued_unix_ms: version,
        grants,
        admins: vec!["ops".to_string()],
    };
    let bytes = serde_json::to_vec(&document).expect("a policy document serializes");
    SignedPolicy {
        hash: document_hash(&bytes),
        bytes: bytes.into(),
        document,
    }
}

/// Version 7: `app` reads and writes `old/` and `same/`; `other` also reads `old/`.
fn v7() -> SignedPolicy {
    signed(
        7,
        vec![
            rw("app", "old/"),
            rw("app", "same/"),
            grant("other", "old/", &[Action::Read]),
        ],
    )
}

/// Version 8: the same grants for `app`, but `other` loses `old/` — so `old/` is a *changed*
/// prefix while `app`'s own access to it is untouched.
///
/// That separation is the point. A watch on `old/` must still terminate, because the grants
/// covering its prefix changed; an implementation that only checked the watching principal's own
/// grants would pass a weaker test and miss exactly the case §15.3 is written for.
fn v8() -> SignedPolicy {
    signed(8, vec![rw("app", "old/"), rw("app", "same/")])
}

/// Version 8 plus a prefix only the new document grants, for the admission row.
fn v8_with_new_prefix() -> SignedPolicy {
    signed(
        8,
        vec![rw("app", "old/"), rw("app", "same/"), rw("app", "new/")],
    )
}

/// One node, formed, serving under `initial`, with the authorizer the test keeps a handle on.
async fn fixture(initial: SignedPolicy) -> (Cluster, Arc<SignedPolicyAuthorizer>) {
    let authorizer = Arc::new(SignedPolicyAuthorizer::new(false));
    authorizer.adopt(initial).expect("the first adoption");
    let cluster =
        Cluster::formed_with_authorizer(1, Arc::clone(&authorizer) as Arc<dyn Authorizer>).await;
    (cluster, authorizer)
}

/// Rotate the node onto `incoming`: revoke the streams it narrows, *then* adopt it.
///
/// The order is the contract (ADR-0027). `on_policy_change` publishes the new epoch before the
/// authorizer starts answering under the new document, so no stream can enqueue an event that
/// was evaluated under a document it has not been revoked against.
async fn rotate(
    cluster: &Cluster,
    authorizer: &Arc<SignedPolicyAuthorizer>,
    incoming: SignedPolicy,
) {
    let old = authorizer.active_document().expect("an active document");
    let new = incoming.document.clone();
    let hub = Arc::clone(cluster.node(1).watch_hub());
    // Off the runtime worker: `on_policy_change` takes the journal gate, which parks a whole
    // thread if a compaction holds it — the same reason registration is spawned blocking.
    tokio::task::spawn_blocking(move || hub.on_policy_change(&old, &new))
        .await
        .expect("the gate section runs");
    authorizer.adopt(incoming).expect("a forward adoption");
}

async fn write(cluster: &Cluster, k: &str) -> u64 {
    cluster
        .node(1)
        .put(&app(), put_request(k, "v"))
        .await
        .unwrap_or_else(|e| panic!("put {k}: {e}"))
        .revision
}

fn watch_request(prefix: &str, start_after_revision: u64) -> WatchRequest {
    WatchRequest {
        prefix: key(prefix),
        start_after_revision,
        progress_interval: Some(NO_PROGRESS),
    }
}

/// Drain a stream to its end, returning the events it delivered and how it finished.
///
/// A stream that neither ends nor delivers within [`DEADLINE`] is a failure, not a hang.
async fn drain(stream: &mut WatchStream) -> (Vec<MutationEvent>, Option<config_core::ConfigError>) {
    let mut events = Vec::new();
    let outcome = tokio::time::timeout(DEADLINE, async {
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(event))) => events.push(event),
                Some(Ok(WatchItem::Progress { .. })) => {}
                Some(Err(err)) => return Some(err),
                None => return None,
            }
        }
    })
    .await;
    match outcome {
        Ok(terminal) => (events, terminal),
        Err(_) => panic!(
            "the stream neither ended nor failed within {DEADLINE:?} after {} events",
            events.len()
        ),
    }
}

/// Take exactly `want` events, failing rather than hanging.
async fn take(stream: &mut WatchStream, want: usize) -> Vec<MutationEvent> {
    let mut events = Vec::with_capacity(want);
    let outcome = tokio::time::timeout(DEADLINE, async {
        while events.len() < want {
            match stream.next().await {
                Some(Ok(WatchItem::Event(event))) => events.push(event),
                Some(Ok(WatchItem::Progress { .. })) => {}
                Some(Err(e)) => panic!("stream terminated with {e} after {} events", events.len()),
                None => panic!("stream ended after {} of {want} events", events.len()),
            }
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "only {} of {want} events arrived within {DEADLINE:?}",
        events.len()
    );
    events
}

fn assert_reason(err: &config_core::ConfigError, reason: &str) {
    assert!(
        err.is_permission_denied_reason(reason),
        "expected PermissionDenied with reason {reason:?}, got {err:?}"
    );
}

// ---------------------------------------------------------------------------------------
// §3.6 — watches under a policy change
// ---------------------------------------------------------------------------------------

/// M6-28: a watch whose prefix's grants changed ends with `policy_changed`, and the last event
/// it received is strictly below the first revision evaluated under the new document.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_28_watch_on_a_changed_prefix_terminates_before_any_new_version_event() {
    let (cluster, authorizer) = fixture(v7()).await;
    let start = write(&cluster, "old/a").await;

    let mut stream = cluster
        .node(1)
        .watch(&app(), watch_request("old/", start))
        .await
        .expect("a granted prefix opens");
    let under_v7 = write(&cluster, "old/b").await;
    let delivered = take(&mut stream, 1).await;
    assert_eq!(delivered[0].revision, under_v7);

    rotate(&cluster, &authorizer, v8()).await;
    // Evaluated under v8, and inside the watch's prefix: if the stream were still alive it
    // would be the next thing enqueued.
    let under_v8 = write(&cluster, "old/c").await;
    assert!(under_v8 > under_v7);

    let (more, terminal) = drain(&mut stream).await;
    let terminal = terminal.expect("a revoked stream ends with an error, not silently");
    assert_reason(&terminal, REASON_POLICY_CHANGED);
    assert!(
        more.iter().all(|e| e.revision < under_v8),
        "no event evaluated under the new document may be delivered, got {:?}",
        more.iter().map(|e| e.revision).collect::<Vec<_>>()
    );

    let stats = cluster.node(1).watch_stats();
    assert_eq!(
        stats
            .terminated_by_reason
            .get(&TerminationReason::PolicyChanged)
            .copied(),
        Some(1),
        "the termination is counted under its own reason: {:?}",
        stats.terminated_by_reason
    );
    cluster.shutdown().await;
}

/// M6-29: the converse. Terminating every watch on every rotation is the easy wrong
/// implementation, and it would make policy rotation an availability event.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_29_watch_on_an_unchanged_prefix_survives_a_policy_change() {
    let (cluster, authorizer) = fixture(v7()).await;
    let start = write(&cluster, "same/a").await;

    let mut stream = cluster
        .node(1)
        .watch(&app(), watch_request("same/", start))
        .await
        .expect("a granted prefix opens");
    let before = write(&cluster, "same/b").await;

    rotate(&cluster, &authorizer, v8()).await;
    let after = write(&cluster, "same/c").await;

    let events = take(&mut stream, 2).await;
    assert_eq!(
        events.iter().map(|e| e.revision).collect::<Vec<_>>(),
        vec![before, after],
        "an unchanged prefix delivers across the version boundary with no gap"
    );
    let stats = cluster.node(1).watch_stats();
    assert_eq!(
        stats
            .terminated_by_reason
            .get(&TerminationReason::PolicyChanged),
        None,
        "an unchanged prefix is not in the blast radius of the change"
    );
    assert_eq!(stats.streams_open, 1, "the stream is still registered");
    cluster.shutdown().await;
}

/// M6-30: "must not expand access early" applies to watch *admission*, not only to reads and
/// writes. The refusal is typed, immediate, and distinguishable from an ordinary denial.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_30_watch_on_a_newly_granted_prefix_is_not_retroactively_opened() {
    let (cluster, authorizer) = fixture(v7()).await;
    rotate(&cluster, &authorizer, v8_with_new_prefix()).await;
    assert!(
        authorizer.is_converging(),
        "no voter has reported v8 yet, so the node is still converging"
    );

    let refused = cluster
        .node(1)
        .watch(&app(), watch_request("new/", 0))
        .await
        .err()
        .expect("a prefix only the new document grants is not open yet");
    assert_reason(&refused, REASON_POLICY_CONVERGING);

    // And it is genuinely held, not merely delayed: the same request still fails after a write
    // has been applied and observed, which is the only "later" a test may assert on.
    write(&cluster, "same/a").await;
    let still_refused = cluster
        .node(1)
        .watch(&app(), watch_request("new/", 0))
        .await
        .err()
        .expect("still not open");
    assert_reason(&still_refused, REASON_POLICY_CONVERGING);

    // Once every voter reports the new version the prefix opens, with no restart.
    assert!(
        authorizer.note_cluster_min_version(Some(8)),
        "the last voter's report completes convergence"
    );
    let _opened = cluster
        .node(1)
        .watch(&app(), watch_request("new/", 0))
        .await
        .expect("a converged grant opens");
    cluster.shutdown().await;
}

/// M6-31: the ordering claim, driven rather than observed.
///
/// [`GateHook::BeforeLiveDrain`] parks the delivery task with the pre-rotation batch already in
/// its broadcast receiver. Releasing it *after* the rotation puts the stream in the exact state
/// the claim is about: events it is entitled to, and a policy that has moved. The assertion is
/// that it enqueues **none** of them — the revocation is checked on the enqueue path, so it wins
/// against anything already buffered, not merely against what arrives later.
///
/// Ten repeats, zero inversions. The interleaving is forced, so a single repeat would already be
/// meaningful; the repeats are what catch an implementation that is ordered only by luck.
#[config_log::retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_31_watch_termination_ordering_is_asserted_from_the_journal() {
    for repeat in 0..10 {
        let (cluster, authorizer) = fixture(v7()).await;
        let start = write(&cluster, "old/a").await;
        let gate = cluster.node(1).watch_hub().testing();

        let pass = gate.pause(GateHook::BeforeLiveDrain);
        let mut stream = cluster
            .node(1)
            .watch(&app(), watch_request("old/", start))
            .await
            .expect("a granted prefix opens");
        gate.wait_arrived(GateHook::BeforeLiveDrain).await;

        // Buffered, entitled to, and evaluated entirely under v7.
        let buffered = write(&cluster, "old/b").await;
        rotate(&cluster, &authorizer, v8()).await;
        gate.release(pass);

        let (events, terminal) = drain(&mut stream).await;
        let terminal = terminal
            .unwrap_or_else(|| panic!("repeat {repeat}: the stream ended without an error"));
        assert_reason(&terminal, REASON_POLICY_CHANGED);
        assert!(
            events.is_empty(),
            "repeat {repeat}: a revoked stream enqueues nothing once the rotation is visible, \
             got {:?} (buffered revision {buffered})",
            events.iter().map(|e| e.revision).collect::<Vec<_>>()
        );
        cluster.shutdown().await;
    }
}
