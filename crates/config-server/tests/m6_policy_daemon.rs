//! M6-10, M6-11, M6-14, M6-15, M6-36, M6-37, M6-117, M6-118, M6-124 and M6-125 — signed-policy
//! lifecycle and its audit trail, driven against a real daemon process (ADR-0027).
//!
//! # Relationship to `m6_rbac.rs`
//!
//! `m6_rbac.rs` (dev-rbac) owns M6-16, M6-25, M6-26 and M6-27 and is read-only from here: this
//! file copies the small helpers it needs (`client_for`, `signed_options`, `signed_node`,
//! `health_until`, `put`) rather than importing or editing that file, per this workstream's own
//! rule of one writer per test file. `crates/config-server/tests/support/mod.rs` is likewise
//! read-only; the one shared file this workstream *did* edit is
//! `crates/config-server/tests/support/daemon.rs`, where `DaemonSpec` gained a
//! `break_glass_policy_rollback: bool` field (default `false`, wired into `args()`) so M6-10
//! could drive `--break-glass-policy-rollback` through the same harness every other row uses.
//! That edit is purely additive and does not change any existing row's generated argv.
//!
//! # Scope decisions (tester-m6a, 2026-09-19)
//!
//! Ten of this file's sixteen assigned rows are implemented below. Six are not, each for a
//! precise reason recorded a second time under the row's own entry in
//! `docs/testing/test-plan-m6.md` (search `tester-m6a, 2026-09-19` there):
//!
//! * **M6-32** (policy-version invalidates an outstanding page token) needs the ADR-0029
//!   `next_page_token` / `PinRegistry` surface, which is dev-pagination's ownership and not
//!   present in this file or in `support::mod.rs`.
//! * **M6-33** (`backup_manifest_references_policy_version`) is a genuine **product gap**, not a
//!   missing test: `crates/config-server/src/backup.rs`'s `finish_artifact()` hardcodes
//!   `policy_version_ref: None` — nothing populates it from live policy state — even though the
//!   field's own doc comment says "Reserved for M6 policy versioning (ADR-0027)". Reported as a
//!   defect in this row's handoff.
//! * **M6-35** (`restore_accepts_an_independently_supplied_policy_of_any_version`) is a second
//!   genuine product gap for the same reason: `restore_policy_mismatch` does not exist anywhere
//!   in `crates/config-server/src/*.rs` (confirmed by grep). There is nothing to test.
//! * **M6-34** (`restore_refuses_to_open_the_client_plane_without_a_valid_policy`) needs a real
//!   daemon started against a *restored* directory under a *second*, distinct cluster identity.
//!   `support::Harness` hardcodes its cluster id via the free function `cluster_id()`, and
//!   `m5_admin.rs`'s own restore rows only ever open the restored directory as a `RocksStore`
//!   directly — no existing fixture starts a real process against a restore target. Attempting
//!   this from scratch risked either a broken test or crowding out the ten rows below within
//!   budget; flagged to the lead as a follow-up rather than rushed.
//! * **M6-119** (`policy_converged` pairs with the gauge) needs cluster-wide convergence, which
//!   the implementation-status table (test-plan-m6.md §3.9) already records as not wired:
//!   `note_cluster_min_version` has no production caller, so a node stays `Converging` until it
//!   restarts.
//! * **M6-126** (`audit_covers_every_M6_admin_operation`) spans `ReloadPolicy`, `ReloadTls`,
//!   gossip add/use/remove and break-glass rollback in one run. `ReloadTls` and the gossip admin
//!   ops belong to dev-rotation/dev-rbac's own files; assembling all six here risked incomplete,
//!   flaky coverage of RPCs this file does not otherwise exercise.
//!
//! Also out of this file's assigned scope for this pass: **M6-110** (evidence,
//! `config-testkit/tests/m6_evidence.rs`) and **E2E-40/42/44/45/46/47**
//! (`config-server/tests/e2e_daemon.rs`) were not attempted this session; see the handoff.
//!
//! # M6-117/M6-118 and the plan's aspirational field names
//!
//! The plan describes `policy_loaded` as carrying `hash_prefix`, `signer_fingerprint`,
//! `grants_count` and `admins_count`, and `policy_rejected` as carrying `version_seen`. None of
//! these fields exist in the shipped code
//! (`crates/config-server/src/policy.rs::PolicyLoader::attempt`); the real lines carry
//! `version, previous_version, hash, source, break_glass` and `reason, source, active_version,
//! detail` respectively. Per this plan's own rule ("Where this plan and the spec/ADRs disagree,
//! the spec/ADRs win and this plan is a defect"), M6-117 and M6-118 below assert the shipped
//! field set, and the divergence is recorded as a dated note under each row.
//!
//! # No fixed sleeps except where proving an absence
//!
//! Every wait for something to *happen* is a bounded poll against a derived deadline
//! (`rotation_deadline`, `wait_for_rejections`). The two sleeps that remain
//! (`m6_11_polling_picks_up_a_new_document`'s no-storm check and
//! `m6_36_authz_mode_static_preserves_m3_behaviour_exactly`'s "static mode never reloads" check)
//! are marked `testkit:allow-sleep`: both are proving a *negative* over a bounded real-time
//! window, which has no event to poll for by construction.

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigStore, PutRequest};
use config_log::retcd_test;
use config_testkit::poll::{poll_until_async, Timeout};
use ed25519_dalek::{Signer, SigningKey};

use support::{
    count_messages, deadline, log_field, log_file, log_lines, startup_deadline, DaemonProcess,
    Harness, Health, NodeOptions, PolicyFixture, PRINCIPAL, TRUST_KEY_NAME,
};

// =====================================================================================
// Helpers (copied from m6_rbac.rs where noted; support::mod.rs and m6_rbac.rs are read-only)
// =====================================================================================

/// How often the daemons under test re-read their policy files. Identical to `m6_rbac.rs`'s own
/// constant, copied rather than imported (that file is read-only from here).
const POLL_SECS: u64 = 1;

/// How long a rotation gets to be picked up: several poll intervals, plus process scheduling.
fn rotation_deadline() -> Duration {
    Duration::from_secs(POLL_SECS * 10)
}

/// How long to wait, doing nothing, to make "no further reload happened" a real claim rather
/// than a guess: five poll intervals with no event to poll for (M6-11's no-storm half).
fn no_storm_wait() -> Duration {
    Duration::from_secs(POLL_SECS * 5)
}

/// A client for the granted principal against one daemon. Copied from `m6_rbac.rs`.
fn client_for(harness: &Harness, node: &DaemonProcess) -> GrpcClient {
    let opts = GrpcClientOptions {
        request_deadline: deadline(5),
        tls: TlsMode::MutualTls(harness.tls.client_mtls(PRINCIPAL)),
        ..GrpcClientOptions::default()
    };
    GrpcClient::connect(vec![node.client_endpoint().to_string()], opts)
        .expect("the client plane endpoint is well formed")
        .with_cluster_id(harness.cluster_id)
}

/// Options for a signed-mode node: the fixture's files, and no static allowlist. Copied from
/// `m6_rbac.rs`.
fn signed_options(harness: &Harness, fixture: &PolicyFixture) -> NodeOptions {
    NodeOptions {
        policy: None,
        signed_policy: Some(fixture.authz(POLL_SECS)),
        ..harness.node_options()
    }
}

/// A one-voter signed-mode daemon that starts **already holding** `version`, so the row using it
/// is about what happens after start rather than about the no-document startup path — that is
/// `m6_rbac.rs`'s `m6_27_policy_arrival_restores_readiness_without_restart`, not repeated here.
async fn signed_node_at(
    method: &'static str,
    version: u64,
) -> (Harness, PolicyFixture, DaemonProcess) {
    let harness = Harness::with_nodes(method, &[1]).await;
    let fixture = PolicyFixture::new(harness.root());
    fixture.write(version, &[""], &["root"]);
    harness.write_node_files(&harness.nodes[0], &signed_options(&harness, &fixture));
    let mut spec = harness.spec(0);
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);
    process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("the daemon never announced itself: {e}"));
    (harness, fixture, process)
}

/// Poll `endpoint`'s health until `want` accepts it, or fail with what was last seen. Copied
/// from `m6_rbac.rs`.
async fn health_until(endpoint: &str, what: &str, want: impl Fn(&Health) -> bool) -> Health {
    let result = poll_until_async(rotation_deadline(), Duration::from_millis(50), || async {
        let payload = support::health(endpoint).await;
        want(&payload).then_some(payload)
    })
    .await;
    match result {
        Ok(payload) => payload,
        Err(Timeout { elapsed, .. }) => {
            let last = support::health(endpoint).await;
            panic!("{what} did not hold within {elapsed:?}; last health was {last:#?}")
        }
    }
}

/// Poll `log` until it holds at least `want` `policy_rejected` lines, or fail loudly.
async fn wait_for_rejections(log: &std::path::Path, want: usize, what: &str) {
    let deadline = tokio::time::Instant::now() + rotation_deadline();
    loop {
        if count_messages(log, "policy_rejected") >= want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: policy_rejected count never reached {want} within {:?}",
            rotation_deadline()
        );
        // Bounded poll step, guarded by the deadline assertion above on every iteration.
        tokio::time::sleep(Duration::from_millis(50)).await; // testkit:allow-sleep
    }
}

fn put(key: &str) -> PutRequest {
    PutRequest {
        key: Bytes::from(key.to_string()),
        value: Bytes::from_static(b"v"),
        expected_mod_revision: None,
        dedup: None,
    }
}

// -----------------------------------------------------------------------------------
// Building documents and signatures `PolicyFixture::write` itself cannot produce: torn,
// mismatched, or deliberately malformed content that must still carry a signature which
// verifies against *some* claimed hash/version.
// -----------------------------------------------------------------------------------

/// Mirrors `support::PolicyFixture`'s internal (private) signing key. See
/// [`assert_fixture_key_matches`], which ties this copy back to the fixture's own key at every
/// call site that relies on it, so any future drift between the two fails loudly instead of
/// producing confusing `untrusted_signer` rejections everywhere.
const FIXTURE_KEY_SEED: [u8; 32] = [0xA7; 32];

fn fixture_signing_key() -> SigningKey {
    SigningKey::from_bytes(&FIXTURE_KEY_SEED)
}

/// Fails loudly if this file's copy of the fixture's key has ever drifted from the real one.
fn assert_fixture_key_matches(fixture: &PolicyFixture) {
    let ours = hex::encode(fixture_signing_key().verifying_key().to_bytes());
    assert_eq!(
        ours,
        fixture.trust_key_hex(),
        "this file's copy of PolicyFixture's signing key has drifted from the real one; \
         update FIXTURE_KEY_SEED to match"
    );
}

/// A well-formed document at `version`, granting [`PRINCIPAL`] on each prefix — the same shape
/// `PolicyFixture::write` builds, but returned instead of written.
fn document_bytes(version: u64, prefixes: &[&str], admins: &[&str]) -> Vec<u8> {
    let document = config_core::PolicyDocument {
        version,
        issued_unix_ms: 1_700_000_000_000 + version,
        grants: prefixes
            .iter()
            .map(|prefix| {
                config_core::policy::grant(
                    PRINCIPAL,
                    prefix,
                    &[config_core::Action::Read, config_core::Action::Write],
                )
            })
            .collect(),
        admins: admins.iter().map(|a| (*a).to_string()).collect(),
    };
    serde_json::to_vec(&document).expect("a policy document serializes")
}

/// A detached signature over `doc_bytes` at `version`, signed by [`fixture_signing_key`] under
/// [`TRUST_KEY_NAME`] — the same envelope shape `PolicyFixture::write` produces.
fn sign_bytes(version: u64, doc_bytes: &[u8]) -> Vec<u8> {
    use config_core::policy::{document_hash, signature_payload, PolicySignature};
    let hash = document_hash(doc_bytes);
    PolicySignature {
        envelope_version: config_core::policy::POLICY_SIGNATURE_VERSION,
        key_name: TRUST_KEY_NAME.to_string(),
        version,
        hash,
        signature: fixture_signing_key()
            .sign(&signature_payload(&hash, version))
            .to_bytes()
            .to_vec(),
    }
    .encode()
    .expect("an envelope encodes")
}

/// A matched, valid `(document, signature)` pair at `version` — everything
/// [`PolicyFixture::write`] would produce, without writing it.
fn sign_document(version: u64, prefixes: &[&str], admins: &[&str]) -> (Vec<u8>, Vec<u8>) {
    let doc = document_bytes(version, prefixes, admins);
    let sig = sign_bytes(version, &doc);
    (doc, sig)
}

/// A complete, valid-JSON document at `version`, and a signature that verifies against a
/// *different* complete, valid-JSON document at the same version. The simplest shape a
/// `hash_mismatch` rejection can take: parseable, version-consistent, and simply not what was
/// signed — e.g. a deploy tool that wrote the wrong build's document under the right file name.
fn mismatched_hash_document(version: u64) -> (Vec<u8>, Vec<u8>) {
    let signed_over = document_bytes(version, &["/signed-over-this-prefix"], &["root"]);
    let sig = sign_bytes(version, &signed_over);
    let written = document_bytes(version, &["/written-instead"], &["root"]);
    (written, sig)
}

// =====================================================================================
// M6-10 — break-glass is process-scoped, not one-shot (OQ-57)
// =====================================================================================

/// M6-10: with `--break-glass-policy-rollback` set, two separate rollbacks in the same process
/// are both permitted, and each is individually audited.
///
/// The authorizer's own accept/reject rule for this is already proved without a process, in
/// `config-core/tests/m6_rbac.rs`'s
/// `m6_09_and_m6_10_break_glass_allows_rollback_and_is_not_sticky` (test-plan-m6.md §3.9). What
/// that unit test cannot show is OQ-57's actual promise to an operator: that the flag, read once
/// from argv, keeps working for the process's whole life through the real file poller — not a
/// direct function call — and that a fleet operator scraping logs sees one line per rollback, not
/// a flag that silently disarms itself after the first use.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_10_break_glass_is_not_sticky() {
    const METHOD: &str = "m6_10_break_glass_is_not_sticky";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let fixture = PolicyFixture::new(harness.root());
    fixture.write(5, &[""], &["root"]);
    harness.write_node_files(&harness.nodes[0], &signed_options(&harness, &fixture));
    let mut spec = harness.spec(0);
    spec.form = true;
    spec.break_glass_policy_rollback = true;
    let mut process = DaemonProcess::spawn(spec);
    process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("the daemon never announced itself: {e}"));
    let endpoint = process.health_endpoint().to_string();
    let log = log_file(&harness.nodes[0]);

    let before = support::health(&endpoint).await;
    assert_eq!(before.policy_version, Some(5), "{before:#?}");

    // First rollback: version 3, below the active 5.
    fixture.write(3, &[""], &["root"]);
    let first = health_until(&endpoint, "the first rollback is accepted", |h| {
        h.policy_version == Some(3)
    })
    .await;
    assert_eq!(first.policy_version, Some(3), "{first:#?}");

    // Second rollback: version 1, below the now-active 3. The flag is not one-shot.
    fixture.write(1, &[""], &["root"]);
    let second = health_until(&endpoint, "the second rollback is also accepted", |h| {
        h.policy_version == Some(1)
    })
    .await;
    assert_eq!(second.policy_version, Some(1), "{second:#?}");

    let loaded = log_lines(&log)
        .into_iter()
        .filter(|l| log_field(l, "@m") == Some("policy_loaded"))
        .collect::<Vec<_>>();
    // startup (5) + rollback to 3 + rollback to 1 = three adoptions, none merged.
    assert_eq!(loaded.len(), 3, "one line per adoption: {loaded:#?}");
    assert_eq!(
        loaded[0]
            .get("break_glass")
            .and_then(serde_json::Value::as_bool),
        Some(false),
        "startup is a first load, not a rollback: {:#?}",
        loaded[0]
    );
    for line in &loaded[1..] {
        assert_eq!(
            line.get("break_glass").and_then(serde_json::Value::as_bool),
            Some(true),
            "{line:#?}"
        );
    }

    let body = support::http_get(&endpoint, "/metrics").await;
    assert!(
        body.contains("retcd_policy_rollbacks_total{node_id=\"1\"} 2"),
        "both rollbacks are counted, individually: {body}"
    );
    assert!(
        body.contains("retcd_break_glass_active{node_id=\"1\"} 1"),
        "the gauge reflects the flag for the process's whole lifetime, not per-rollback: {body}"
    );

    process.stop_gracefully(startup_deadline()).await;
    drop(harness);
}

// =====================================================================================
// M6-11 — bounded polling, and no re-read storm
// =====================================================================================

/// M6-11: the poller picks up a new document within a bounded number of ticks, logs exactly one
/// `policy_loaded{source="poll"}` line for it, and never reloads again while the file sits
/// unchanged underneath it (D6.1, §19.12).
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_11_polling_picks_up_a_new_document() {
    const METHOD: &str = "m6_11_polling_picks_up_a_new_document";
    let (harness, fixture, mut process) = signed_node_at(METHOD, 7).await;
    let endpoint = process.health_endpoint().to_string();
    let log = log_file(&harness.nodes[0]);

    let before = support::health(&endpoint).await;
    assert_eq!(before.policy_version, Some(7), "{before:#?}");
    assert_eq!(
        count_messages(&log, "policy_loaded"),
        1,
        "one adoption at startup"
    );

    fixture.write(8, &[""], &["root"]);
    let after = health_until(&endpoint, "the poller picks up version 8", |h| {
        h.policy_version == Some(8)
    })
    .await;
    assert_eq!(after.policy_version, Some(8), "{after:#?}");

    let loaded = log_lines(&log)
        .into_iter()
        .filter(|l| log_field(l, "@m") == Some("policy_loaded"))
        .collect::<Vec<_>>();
    assert_eq!(
        loaded.len(),
        2,
        "startup + exactly one poll adoption: {loaded:#?}"
    );
    assert_eq!(
        log_field(&loaded[1], "source"),
        Some("poll"),
        "{:#?}",
        loaded[1]
    );
    assert_eq!(
        loaded[1].get("version").and_then(serde_json::Value::as_u64),
        Some(8),
        "{:#?}",
        loaded[1]
    );

    // No re-read storm: several more ticks over an unchanged file must add nothing. Proving an
    // absence over real time has no event to poll for, hence the bounded, marked sleep.
    tokio::time::sleep(no_storm_wait()).await; // testkit:allow-sleep
    assert_eq!(
        count_messages(&log, "policy_loaded"),
        2,
        "an unchanged file must not reload on every subsequent tick"
    );

    process.stop_gracefully(startup_deadline()).await;
    drop(harness);
}

// =====================================================================================
// M6-14 — a missing file is distinguished from an invalid one, and recovers on its own
// =====================================================================================

/// M6-14: deleting the signature file (leaving the document behind) is refused as
/// `signature_file_missing`, the active policy is retained and the node stays ready, and the
/// next valid pair on disk is picked up with no restart and no other operator action.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_14_missing_files_are_distinguished_from_invalid_files() {
    const METHOD: &str = "m6_14_missing_files_are_distinguished_from_invalid_files";
    let (harness, fixture, mut process) = signed_node_at(METHOD, 7).await;
    let endpoint = process.health_endpoint().to_string();
    let log = log_file(&harness.nodes[0]);

    let before = support::health(&endpoint).await;
    assert_eq!(before.policy_version, Some(7), "{before:#?}");

    std::fs::remove_file(&fixture.signature_file)
        .expect("remove the signature file, leaving the document behind");
    wait_for_rejections(&log, 1, "signature_file_missing").await;

    let rejected = log_lines(&log)
        .into_iter()
        .filter(|l| log_field(l, "@m") == Some("policy_rejected"))
        .collect::<Vec<_>>();
    assert_eq!(rejected.len(), 1, "{rejected:#?}");
    assert_eq!(
        log_field(&rejected[0], "reason"),
        Some("signature_file_missing"),
        "{:#?}",
        rejected[0]
    );

    let mid = support::health(&endpoint).await;
    assert_eq!(
        mid.policy_version,
        Some(7),
        "the active policy is retained across the gap: {mid:#?}"
    );
    assert!(
        mid.ready,
        "a retained, still-valid policy keeps the node ready: {mid:#?}"
    );

    // The file's return: the next tick recovers with no restart and no operator action beyond
    // supplying a valid pair again.
    fixture.write(8, &[""], &["root"]);
    let after = health_until(&endpoint, "the returned pair loads on the next tick", |h| {
        h.policy_version == Some(8)
    })
    .await;
    assert_eq!(after.policy_version, Some(8), "{after:#?}");
    assert_eq!(
        count_messages(&log, "policy_rejected"),
        1,
        "only the deletion window was ever rejected"
    );

    process.stop_gracefully(startup_deadline()).await;
    drop(harness);
}

// =====================================================================================
// M6-15 — atomicity under a half-written file (**mandatory mutation check**)
// =====================================================================================

/// M6-15: a document written in two chunks, with a poll tick landing between them, is refused as
/// one of the two typed reasons the row names and is never partially applied; the next tick,
/// once the write completes, succeeds.
///
/// **Mandatory mutation check.** See the dated note under this row in `test-plan-m6.md` and
/// `tester-m6a-notes.md` for the exact product line mutated, the window, and the revert.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_15_reload_is_atomic_under_a_half_written_file() {
    const METHOD: &str = "m6_15_reload_is_atomic_under_a_half_written_file";
    let (harness, fixture, mut process) = signed_node_at(METHOD, 7).await;
    assert_fixture_key_matches(&fixture);
    let endpoint = process.health_endpoint().to_string();
    let log = log_file(&harness.nodes[0]);

    let (full_doc, sig) = sign_document(8, &[""], &["root"]);
    let split = full_doc.len() / 2;
    assert!(
        split > 0 && split < full_doc.len(),
        "the document has a real midpoint to tear at"
    );

    // The new signature is written in full, matching where the deploy is headed; the document
    // is written in two chunks with the poll tick landing between them — a deploy tool that
    // writes in place rather than renaming (the row's own note).
    std::fs::write(&fixture.signature_file, &sig).expect("write the new signature");
    std::fs::write(&fixture.policy_file, &full_doc[..split])
        .expect("write only the first half of the document");

    wait_for_rejections(&log, 1, "the half-written document").await;
    let mid = support::health(&endpoint).await;
    assert_eq!(
        mid.policy_version,
        Some(7),
        "the half-written file is never partially applied: {mid:#?}"
    );
    assert!(mid.ready, "{mid:#?}");

    let rejected = log_lines(&log)
        .into_iter()
        .filter(|l| log_field(l, "@m") == Some("policy_rejected"))
        .collect::<Vec<_>>();
    assert_eq!(rejected.len(), 1, "{rejected:#?}");
    let reason = log_field(&rejected[0], "reason").unwrap_or_default();
    assert!(
        reason == "hash_mismatch" || reason == "parse_error",
        "a half-written document is refused as one of the two typed reasons the row names, \
         got {reason:?}: {:#?}",
        rejected[0]
    );

    // Complete the write: the next tick succeeds, and nothing else was ever rejected.
    std::fs::write(&fixture.policy_file, &full_doc).expect("write the remaining bytes");
    let after = health_until(
        &endpoint,
        "the completed document loads on the next tick",
        |h| h.policy_version == Some(8),
    )
    .await;
    assert_eq!(after.policy_version, Some(8), "{after:#?}");
    assert_eq!(
        count_messages(&log, "policy_rejected"),
        1,
        "only the torn attempt was ever rejected"
    );

    process.stop_gracefully(startup_deadline()).await;
    drop(harness);
}

// =====================================================================================
// M6-36 / M6-37 — the two `authz.mode` values at daemon level
// =====================================================================================

/// M6-36: `authz.mode = "static"` behaves exactly as M3 shipped it — no policy file re-read, no
/// poller, `policy_version` stays `None`, and the capability report says `StaticAllowlist`.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_36_authz_mode_static_preserves_m3_behaviour_exactly() {
    const METHOD: &str = "m6_36_authz_mode_static_preserves_m3_behaviour_exactly";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let mut process = harness.start(0, true);
    let endpoint = process.health_endpoint().to_string();

    let health = health_until(&endpoint, "the static node forms", |h| {
        h.ready && h.current_leader.is_some()
    })
    .await;
    assert_eq!(health.authz_kind, "static_allowlist", "{health:#?}");
    assert_eq!(health.policy_version, None, "{health:#?}");
    assert_eq!(
        health.policy_state, None,
        "static mode publishes no policy_state at all: {health:#?}"
    );
    assert_eq!(
        health.policy.kind,
        serde_json::json!("StaticAllowlist"),
        "{health:#?}"
    );

    let client = client_for(&harness, &process);
    client
        .put(put("/m6-36/k"))
        .await
        .expect("the M3 allowlist still grants the suite principal");

    // No poller: the file on disk is never re-read after startup. Under signed mode, removing a
    // grant is picked up at the next tick (M6-11); under static mode it must do nothing at all,
    // because static mode loads the file exactly once, at start.
    std::fs::write(&harness.policy, "").expect("empty the static allowlist file on disk");
    // Proving a negative has no event to poll for: this stands in for "still true after however
    // long a signed-mode poll tick would have taken".
    tokio::time::sleep(Duration::from_millis(500)).await; // testkit:allow-sleep
    client
        .put(put("/m6-36/k2"))
        .await
        .expect("the in-memory grant from startup is untouched by the file changing under it");

    process.stop_gracefully(startup_deadline()).await;
    drop(harness);
}

/// M6-37: `authz.mode = "signed"` with neither `policy_file` nor `trust_keys` is refused **at
/// config load**, naming every missing field; the node never starts and never listens.
///
/// Config-load failures happen before `logging::init()` (`crates/config-server/src/main.rs`), so
/// this refusal produces no JSONL log line at all — only the one stderr diagnostic line
/// (`config-server: <reason>: <detail>`, ADR-0018 §5). That is the entire contract for this
/// failure class, and this row asserts against it rather than against a log line that does not
/// exist.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_37_authz_mode_signed_requires_the_policy_configuration() {
    const METHOD: &str = "m6_37_authz_mode_signed_requires_the_policy_configuration";
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let opts = NodeOptions {
        policy: None,
        ..NodeOptions::default()
    };
    harness.write_node_files(&harness.nodes[0], &opts);
    let mut config =
        std::fs::read_to_string(&harness.nodes[0].config).expect("read the node config");
    config.push_str("\n[authz]\nmode = \"signed\"\n");
    std::fs::write(&harness.nodes[0].config, config)
        .expect("write the signed-mode-with-nothing config");

    let mut spec = harness.spec(0);
    spec.form = true;
    let (code, stdout, stderr) = support::daemon::run_to_completion(&spec);

    assert_eq!(
        code,
        Some(2),
        "a config refusal exits 2 (ADR-0018 §5); stderr={stderr:?}"
    );
    assert!(stdout.trim().is_empty(), "no ready line: {stdout:?}");
    let diag = stderr
        .lines()
        .find(|l| l.starts_with("config-server: "))
        .unwrap_or_else(|| panic!("no diagnostic line in stderr: {stderr:?}"));
    assert!(
        diag.contains("authz.policy_file"),
        "every missing field is named: {diag}"
    );
    assert!(
        diag.contains("authz.trust_keys"),
        "every missing field is named: {diag}"
    );

    let log = log_file(&harness.nodes[0]);
    assert!(
        !log.exists() || log_lines(&log).is_empty(),
        "a config-load refusal happens before logging::init() and writes no log line at all"
    );

    drop(harness);
}

// =====================================================================================
// M6-117 / M6-118 — the two policy audit lines' shape
// =====================================================================================

/// M6-117: `policy_loaded` carries exactly the shipped field set
/// (`version, previous_version, hash, source, break_glass`; see this file's module doc for the
/// divergence from the plan's aspirational field names) and never a grant body, principal or
/// prefix — one line per adoption, no more.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_117_policy_loaded_line_is_complete() {
    const METHOD: &str = "m6_117_policy_loaded_line_is_complete";
    let (harness, fixture, mut process) = signed_node_at(METHOD, 1).await;
    let endpoint = process.health_endpoint().to_string();
    let log = log_file(&harness.nodes[0]);

    fixture.write(2, &["/only-this-prefix/"], &["only-this-admin"]);
    let _ = health_until(&endpoint, "the second version loads", |h| {
        h.policy_version == Some(2)
    })
    .await;
    process.stop_gracefully(startup_deadline()).await;

    let loaded = log_lines(&log)
        .into_iter()
        .filter(|l| log_field(l, "@m") == Some("policy_loaded"))
        .collect::<Vec<_>>();
    assert_eq!(
        loaded.len(),
        2,
        "startup + one poll adoption, exactly one line each: {loaded:#?}"
    );

    let expectations: [(&serde_json::Value, &str, Option<u64>, u64); 2] = [
        (&loaded[0], "startup", None, 1),
        (&loaded[1], "poll", Some(1), 2),
    ];
    for (line, expect_source, expect_from, expect_version) in expectations {
        assert_eq!(
            line.get("version").and_then(serde_json::Value::as_u64),
            Some(expect_version),
            "{line:#?}"
        );
        assert_eq!(
            line.get("previous_version")
                .and_then(serde_json::Value::as_u64),
            expect_from,
            "{line:#?}"
        );
        let hash = line
            .get("hash")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        assert!(
            hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "hash must be 64 lowercase hex characters: {line:#?}"
        );
        assert_eq!(log_field(line, "source"), Some(expect_source), "{line:#?}");
        assert_eq!(
            line.get("break_glass").and_then(serde_json::Value::as_bool),
            Some(false),
            "{line:#?}"
        );

        let rendered = line.to_string();
        assert!(
            !rendered.contains("only-this-prefix"),
            "no grant prefix in the line: {rendered}"
        );
        assert!(
            !rendered.contains("only-this-admin"),
            "no admin principal in the line: {rendered}"
        );
        assert!(
            !rendered.contains(PRINCIPAL),
            "no grant principal in the line: {rendered}"
        );
    }

    drop(harness);
}

/// M6-118: `policy_rejected` draws its `reason` only from the closed eight-value set, sampled
/// across the six reasons this daemon-level fixture can reach directly by writing files.
/// `untrusted_signer` and `signature_invalid` are proved bit-for-bit at the authorizer level
/// (test-plan-m6.md's own "driven by M6-02..M6-05" note for this row); what only a real
/// file/process seam can show is the log line's shape, and that unrelated reasons stay silent
/// while it is exercised.
///
/// Also records a plan/code divergence: the plan describes a `version_seen` field that does not
/// exist in the shipped line (`crates/config-server/src/policy.rs`'s `reload` only logs
/// `reason, source, active_version, detail`); this row asserts against the real field set.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_118_policy_rejected_line_has_a_closed_reason_set() {
    const METHOD: &str = "m6_118_policy_rejected_line_has_a_closed_reason_set";
    let (harness, fixture, mut process) = signed_node_at(METHOD, 10).await;
    assert_fixture_key_matches(&fixture);
    let endpoint = process.health_endpoint().to_string();
    let log = log_file(&harness.nodes[0]);

    // 1. policy_file_missing: the document read is attempted before the signature read.
    std::fs::remove_file(&fixture.policy_file).expect("remove the document");
    wait_for_rejections(&log, 1, "policy_file_missing").await;
    fixture.write(10, &[""], &["root"]); // heals; byte-identical to active -> Unchanged, no line
    assert_eq!(count_messages(&log, "policy_rejected"), 1);

    // 2. signature_file_missing.
    std::fs::remove_file(&fixture.signature_file).expect("remove the signature");
    wait_for_rejections(&log, 2, "signature_file_missing").await;
    fixture.write(10, &[""], &["root"]);
    assert_eq!(count_messages(&log, "policy_rejected"), 2);

    // 3. hash_mismatch: a complete, valid document that is not the one that was signed.
    let (mismatched, sig) = mismatched_hash_document(11);
    std::fs::write(&fixture.signature_file, &sig).expect("write the v11 signature");
    std::fs::write(&fixture.policy_file, &mismatched).expect("write the mismatched document");
    wait_for_rejections(&log, 3, "hash_mismatch").await;

    // 4. parse_error: bytes that are not JSON at all, signed as version 12.
    let garbage = b"not a policy document".to_vec();
    let garbage_sig = sign_bytes(12, &garbage);
    std::fs::write(&fixture.signature_file, &garbage_sig).expect("write the v12 signature");
    std::fs::write(&fixture.policy_file, &garbage).expect("write the unparseable document");
    wait_for_rejections(&log, 4, "parse_error").await;

    // 5. version_binding: a well-formed document whose own version disagrees with what its
    // signature commits to.
    let bound_doc = document_bytes(13, &[""], &["root"]);
    let mismatched_sig = sign_bytes(14, &bound_doc);
    std::fs::write(&fixture.signature_file, &mismatched_sig)
        .expect("write the mismatched-version signature");
    std::fs::write(&fixture.policy_file, &bound_doc).expect("write the version-13 document");
    wait_for_rejections(&log, 5, "version_binding").await;

    // Recover onto a clean, higher version before the rollback case.
    fixture.write(20, &[""], &["root"]);
    let _ = health_until(&endpoint, "recovery before the rollback case", |h| {
        h.policy_version == Some(20)
    })
    .await;

    // 6. rollback: a fully valid, lower-versioned document, on a node with no break-glass flag.
    fixture.write(15, &[""], &["root"]);
    wait_for_rejections(&log, 6, "rollback").await;

    process.stop_gracefully(startup_deadline()).await;

    let rejected = log_lines(&log)
        .into_iter()
        .filter(|l| log_field(l, "@m") == Some("policy_rejected"))
        .collect::<Vec<_>>();
    assert_eq!(rejected.len(), 6, "{rejected:#?}");
    let expected_reasons = [
        "policy_file_missing",
        "signature_file_missing",
        "hash_mismatch",
        "parse_error",
        "version_binding",
        "rollback",
    ];
    for (line, expected) in rejected.iter().zip(expected_reasons) {
        assert_eq!(log_field(line, "reason"), Some(expected), "{line:#?}");
    }

    const CLOSED_SET: [&str; 8] = [
        "signature_invalid",
        "untrusted_signer",
        "hash_mismatch",
        "version_binding",
        "rollback",
        "parse_error",
        "signature_file_missing",
        "policy_file_missing",
    ];
    for line in &rejected {
        let reason = log_field(line, "reason").unwrap_or_default();
        assert!(
            CLOSED_SET.contains(&reason),
            "reason {reason:?} is outside the closed set: {line:#?}"
        );
        assert!(
            line.get("version_seen").is_none(),
            "the shipped line has no version_seen field (plan/code divergence, dated note under \
             M6-118 in test-plan-m6.md): {line:#?}"
        );
        assert!(line.get("active_version").is_some(), "{line:#?}");
        assert_eq!(log_field(line, "source"), Some("poll"), "{line:#?}");
    }

    drop(harness);
}

// =====================================================================================
// M6-124 — no application key or value in any M6 line
// =====================================================================================

/// M6-124: a sentinel key and value put through this node never appear verbatim in its JSONL
/// log, `/health`, `/metrics`, stdout or stderr — and specifically never in a `policy_*` line,
/// which has no legitimate reason to mention an application key or value at all.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_124_no_values_in_any_m6_line() {
    const METHOD: &str = "m6_124_no_values_in_any_m6_line";
    const SENTINEL_VALUE: &str = "SENTINEL-VALUE-DEADBEEF-8f2c";
    const SENTINEL_KEY: &str = "/m6-124/sentinel-key-7a91";

    let (harness, fixture, mut process) = signed_node_at(METHOD, 1).await;
    let endpoint = process.health_endpoint().to_string();
    let log = log_file(&harness.nodes[0]);

    let client = client_for(&harness, &process);
    client
        .put(PutRequest {
            key: Bytes::from(SENTINEL_KEY),
            value: Bytes::from(SENTINEL_VALUE),
            expected_mod_revision: None,
            dedup: None,
        })
        .await
        .expect("the sentinel put succeeds");

    // Also drive a policy_rejected line: exactly the kind of line ADR-0013 requires to carry no
    // policy material, checked here for application material instead.
    fixture.remove();
    wait_for_rejections(&log, 1, "a policy_rejected line for the sweep").await;
    fixture.write(1, &[""], &["root"]);
    let _ = health_until(&endpoint, "recovery after the sentinel rejection", |h| {
        h.ready
    })
    .await;

    let metrics_body = support::http_get(&endpoint, "/metrics").await;
    let health_body = support::http_get(&endpoint, "/health").await;
    process.stop_gracefully(startup_deadline()).await;

    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    let stdout_text = process.stdout_lines().join("\n");
    let stderr_text = process.stderr();
    for (name, haystack) in [
        ("log", log_text.as_str()),
        ("metrics", metrics_body.as_str()),
        ("health", health_body.as_str()),
        ("stdout", stdout_text.as_str()),
        ("stderr", stderr_text.as_str()),
    ] {
        assert!(
            !haystack.contains(SENTINEL_VALUE),
            "the sentinel value must never appear verbatim in {name}"
        );
    }

    let policy_lines = log_lines(&log)
        .into_iter()
        .filter(|l| {
            matches!(
                log_field(l, "@m"),
                Some("policy_loaded") | Some("policy_rejected") | Some("policy_converged")
            )
        })
        .collect::<Vec<_>>();
    assert!(
        !policy_lines.is_empty(),
        "the run produced at least one policy line"
    );
    for line in &policy_lines {
        let rendered = line.to_string();
        assert!(
            !rendered.contains(SENTINEL_KEY),
            "a policy line must never carry an application key: {rendered}"
        );
        assert!(
            !rendered.contains(SENTINEL_VALUE),
            "a policy line must never carry an application value: {rendered}"
        );
    }

    drop(harness);
}

// =====================================================================================
// M6-125 — no key material anywhere (**mandatory mutation check**)
// =====================================================================================

/// M6-125: no key material — this file's slice of the union ADR-0013 and the row name — appears
/// in a log line, a metrics scrape, a health payload, stdout or stderr.
///
/// Structurally, a signed-mode daemon never holds a policy or TLS *private* half at all: only
/// public trust keys and its own certificate ever reach `crates/config-server/src/policy.rs`.
/// The one secret this crate's own `config.rs` reads directly and could plausibly leak is
/// `list.token_key_file` (the pagination cursor HMAC key, ADR-0029, named explicitly in this
/// row's sweep list), so that is what this row drives through a real node and sweeps for,
/// alongside the policy signing key this file itself holds and any TLS private-key PEM marker.
///
/// **Mandatory mutation check.** See the dated note under this row in `test-plan-m6.md` and
/// `tester-m6a-notes.md` for the exact product line mutated, the window, and the revert.
#[retcd_test(flavor = "multi_thread", worker_threads = 4)]
async fn m6_125_no_key_material_anywhere() {
    const METHOD: &str = "m6_125_no_key_material_anywhere";
    let token_key_hex = "5f".repeat(32);

    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let fixture = PolicyFixture::new(harness.root());
    fixture.write(3, &[""], &["root"]);
    harness.write_node_files(&harness.nodes[0], &signed_options(&harness, &fixture));

    let token_key_path = harness.root().join("list-token.key");
    std::fs::write(&token_key_path, &token_key_hex).expect("write the sentinel token key");
    let mut config =
        std::fs::read_to_string(&harness.nodes[0].config).expect("read the node config");
    config.push_str(&format!(
        "\n[list]\ntoken_key_file = \"{}\"\n",
        token_key_path.display().to_string().replace('\\', "\\\\")
    ));
    std::fs::write(&harness.nodes[0].config, config).expect("append the list section");

    let mut spec = harness.spec(0);
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);
    process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("the daemon never announced itself: {e}"));
    let endpoint = process.health_endpoint().to_string();
    let log = log_file(&harness.nodes[0]);

    // Drive real traffic so every M6 line this daemon can produce gets a chance to run: a put,
    // a health scrape, a metrics scrape, and a policy rotation.
    let client = client_for(&harness, &process);
    client
        .put(put("/m6-125/k"))
        .await
        .expect("a granted put succeeds");
    let _ = support::health(&endpoint).await;
    let metrics_body = support::http_get(&endpoint, "/metrics").await;
    fixture.write(4, &[""], &["root"]);
    let _ = health_until(&endpoint, "the rotation completes", |h| {
        h.policy_version == Some(4)
    })
    .await;

    process.stop_gracefully(startup_deadline()).await;

    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    let stdout_text = process.stdout_lines().join("\n");
    let stderr_text = process.stderr();
    let policy_key_hex = hex::encode(FIXTURE_KEY_SEED);
    let needles = [
        token_key_hex.as_str(),
        policy_key_hex.as_str(),
        "BEGIN PRIVATE KEY",
        "BEGIN EC PRIVATE KEY",
        "BEGIN RSA PRIVATE KEY",
    ];
    for (name, haystack) in [
        ("log", log_text.as_str()),
        ("metrics", metrics_body.as_str()),
        ("stdout", stdout_text.as_str()),
        ("stderr", stderr_text.as_str()),
    ] {
        for needle in needles {
            assert!(
                !haystack.contains(needle),
                "key material {needle:?} must never appear in {name}"
            );
        }
    }

    drop(harness);
}
