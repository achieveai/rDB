//! M3 daemon-only rows (test plan §4.1 M3-43/44/46, §4.9 M3-66..M3-74): real `config-server`
//! processes, mirroring `e2e_daemon.rs`'s conventions (spawned binary, real mTLS, real
//! RocksDB, loopback health) rather than reaching into the libraries.
//!
//! # Ground truth that reshapes several rows' literal assertions
//!
//! Read directly from `crates/config-server/src/run.rs` and `crates/config-server/src/manifest.rs`:
//!
//! * Every manifest refusal — signature, tamper, expiry, cluster/epoch mismatch, missing-voter —
//!   surfaces through exactly one code path: `Fatal::rejected("manifest_rejected", e)`, logged
//!   once by `main()`'s catch-all as `msg="startup_failed"`, `reason="manifest_rejected"`,
//!   `detail="<Display of the specific ManifestError>"`. There are no per-cause tags like
//!   `msg="manifest_signature_invalid"` or `msg="manifest_expired"`, and no `manifest_path`/
//!   `key_id` fields — the row names for those are aspirational. The tests below assert the
//!   real, constant `reason` plus a `detail` substring that pins down which check actually
//!   fired, and say so in each test's doc comment.
//! * `ManifestError` has no key-id concept at all (`verify_document` reads one raw 32-byte
//!   public key, nothing that identifies *which* key). So "tampered signature" (M3-68),
//!   "tampered toml" (M3-67, which invalidates the signature exactly the same way) and "signed
//!   with the wrong key" (M3-69) all collapse to the identical observable outcome —
//!   `ManifestError::BadSignature` — at the process boundary, even though each is built by a
//!   genuinely different technique. This is a real product property (or gap, depending on
//!   whether per-cause diagnosis is a design goal), not a shortcut taken here.
//! * `msg="formation_started"` is real (`run.rs` line ~510) and is logged only after every
//!   manifest/bind check passes, immediately before `ConfigNode::form_cluster` — so "the
//!   refusal happens before formation" is asserted as "no `formation_started` line exists",
//!   which is a faithful, non-aspirational check.
//! * `--capabilities` (`run::capabilities_without_opening`) is documented in its own doc
//!   comment as "deliberately duplicated" from `ConfigNode::capabilities` — the two struct
//!   literals are hand-written independently by design, not unified. `watch_resumption`,
//!   `pagination` and `dedup` are unconditional literals in *both* functions (confirmed by
//!   reading both), so they can never diverge regardless of configuration; there is no live
//!   channel (health endpoint, gRPC) that reports a running node's full six-field
//!   `Capabilities`, so M3-46 compares the CLI report against the daemon's `/health` payload on
//!   the three fields health exposes (`durability`, `authz_kind`, `transport_security`) exactly
//!   as E2E-02 does, and additionally pins the three health cannot see to their documented
//!   unconditional values by asserting the CLI JSON carries them.
//! * The insecure-transport decision is logged by `run::tls_mode` as a single `warn`,
//!   `@m="insecure_transport_enabled"`, on the one path that can reach `TlsMode::Insecure`
//!   (configuration validation refuses `tls.mode = "insecure"` unless `--allow-insecure-dev`
//!   was given, so reaching that arm *is* "the gate was opened"). M3-44 asserts it. An earlier
//!   revision of this file recorded that no such line existed anywhere; it was added for this
//!   row (lead ruling on the M3 gap rows, ADR-0018 §2).

mod support;

use std::time::Duration;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ConfigStore, MutationOutcome, NodeId, PutRequest};
use config_log::retcd_test;
use config_testkit::manifest::{Manifest, ManifestFixture, Tamper};
use config_testkit::poll::{poll_until_async, Timeout};

use support::{
    daemon, deadline, startup_deadline, DaemonProcess, Harness, Health, NodeOptions, PRINCIPAL,
};

/// A client over `endpoints`, presenting `principal`'s certificate.
fn client_for(harness: &Harness, endpoints: Vec<String>, principal: &str) -> GrpcClient {
    let opts = GrpcClientOptions {
        request_deadline: deadline(5),
        tls: TlsMode::MutualTls(harness.tls.client_mtls(principal)),
        ..GrpcClientOptions::default()
    };
    GrpcClient::connect(endpoints, opts)
        .expect("the client plane endpoints are well formed")
        .with_cluster_id(harness.cluster_id)
}

/// A client over the whole cluster, as the granted principal.
fn cluster_client(harness: &Harness, nodes: &[DaemonProcess]) -> GrpcClient {
    let endpoints = nodes
        .iter()
        .map(|n| n.client_endpoint().to_string())
        .collect();
    client_for(harness, endpoints, PRINCIPAL)
}

/// Poll every live node's health until `check` holds for all of them.
async fn wait_for_all(
    endpoints: &[String],
    what: &str,
    check: impl Fn(&[Health]) -> bool,
) -> Vec<Health> {
    let result = poll_until_async(deadline(10), Duration::from_millis(50), || async {
        let mut payloads = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            payloads.push(support::health(endpoint).await);
        }
        check(&payloads).then_some(payloads)
    })
    .await;
    match result {
        Ok(payloads) => payloads,
        Err(Timeout { elapsed, .. }) => {
            let mut last = Vec::new();
            for endpoint in endpoints {
                last.push(support::health(endpoint).await);
            }
            panic!("{what} did not hold within {elapsed:?}; last health payloads: {last:#?}")
        }
    }
}

/// Wait until every node agrees on one leader and the full voter set.
async fn wait_formed(nodes: &[DaemonProcess]) -> Vec<Health> {
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let voters: Vec<u64> = nodes.iter().map(DaemonProcess::node_id).collect();
    wait_for_all(&endpoints, "the cluster to form", |payloads| {
        payloads
            .iter()
            .all(|p| p.membership_voter_ids == voters && p.current_leader.is_some() && p.ready)
    })
    .await
}

/// The daemon's own JSONL lines whose `@m` equals `startup_failed`, for `method`.
fn startup_failed_lines(node: &support::NodeLayout, method: &str) -> Vec<serde_json::Value> {
    support::startup_failed_lines(node, method)
}

fn field<'a>(row: &'a serde_json::Value, name: &str) -> Option<&'a str> {
    support::log_field(row, name)
}

/// Every JSONL line node `node` wrote whose `@m` equals `message`, for `method`.
fn lines_with_message(
    node: &support::NodeLayout,
    method: &str,
    message: &str,
) -> Vec<serde_json::Value> {
    let path = support::log_file(node);
    if !path.exists() {
        return Vec::new();
    }
    support::log_lines(&path)
        .into_iter()
        .filter(|l| field(l, "@m") == Some(message) && field(l, "testMethod") == Some(method))
        .collect()
}

// =====================================================================================
// M3-43 / M3-44 — the insecure-transport gate (test plan §4.1)
// =====================================================================================

/// M3-43: `tls.mode = "insecure"` without `--allow-insecure-dev` is refused before any
/// listener binds. Mirrors E2E-12's exact technique.
#[retcd_test]
async fn m3_43_daemon_refuses_insecure_without_flag() {
    let harness = Harness::new("m3_43_daemon_refuses_insecure_without_flag").await;
    let node = &harness.nodes[0];
    harness.write_node_files(
        node,
        &NodeOptions {
            insecure: true,
            ..harness.node_options()
        },
    );

    let (code, stdout, stderr) = daemon::run_to_completion(&harness.spec(0));
    assert_eq!(
        code,
        Some(2),
        "an insecure config must exit 2; stderr:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "a refused daemon must print no ready line, got: {stdout:?}"
    );
    assert!(
        stderr.contains("--allow-insecure-dev"),
        "the refusal must name the flag that would have allowed it: {stderr}"
    );
    assert!(
        !node.data_dir.exists(),
        "a refused daemon must not have opened the store before the gate runs"
    );
}

/// M3-44: with `--allow-insecure-dev`, the daemon starts, warns exactly once that it is
/// serving plaintext, and reports `Insecure` both from `--capabilities` and from the running
/// node's health payload.
#[retcd_test]
async fn m3_44_daemon_accepts_insecure_with_flag_and_warns() {
    const METHOD: &str = "m3_44_daemon_accepts_insecure_with_flag_and_warns";
    // A genuine single-voter harness: `Harness::new`'s default manifest names three voters,
    // so a lone `--form` node started against it can never reach quorum by itself (this was
    // this test's original bug — not a product defect — it hung waiting for a leader that a
    // one-of-three membership can never elect alone).
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let node = &harness.nodes[0];
    harness.write_node_files(
        node,
        &NodeOptions {
            insecure: true,
            ..harness.node_options()
        },
    );

    // `--capabilities` first: it opens nothing, so the reservation must survive it.
    let capabilities = {
        let mut spec = harness.spec_no_listen(0);
        spec.capabilities = true;
        spec.allow_insecure_dev = true;
        spec.health_listen = None;
        let (code, stdout, stderr) = daemon::run_to_completion(&spec);
        assert_eq!(
            code,
            Some(0),
            "--capabilities must exit 0; stderr:\n{stderr}"
        );
        serde_json::from_str::<serde_json::Value>(stdout.trim())
            .expect("the capability report is JSON")
    };
    assert_eq!(
        capabilities["transport_security"], "Insecure",
        "capabilities must not hide an insecure transport: {capabilities}"
    );

    let mut spec = harness.spec(0);
    spec.allow_insecure_dev = true;
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);
    process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("an --allow-insecure-dev daemon must start: {e}"));

    let health = wait_for_all(
        &[process.health_endpoint().to_string()],
        "the lone insecure node to become ready",
        |p| p[0].ready,
    )
    .await;
    assert_eq!(health[0].transport_security, "Insecure");

    // The gate is loud: exactly one warn line per startup, never zero and never a stream.
    let warnings = lines_with_message(node, METHOD, "insecure_transport_enabled");
    assert_eq!(
        warnings.len(),
        1,
        "accepting tls.mode = \"insecure\" must warn exactly once: {warnings:#?}"
    );
    assert_eq!(
        field(&warnings[0], "@l"),
        Some("Warning"),
        "the insecure-transport line must be a warning: {:#?}",
        warnings[0]
    );
}

// =====================================================================================
// M3-45 — a daemon with no policy and no --dev-allow-all (ADR-0018 §6, OQ-19)
// =====================================================================================

/// A daemon configured with no `[authz]` policy and started without `--dev-allow-all` is
/// *unready*, not dead: it prints its ready line, forms, replicates — and denies every client
/// call, including one from the principal the suite's policy would have granted.
///
/// This is the daemon-level half of the fail-closed rule the in-process rows M3-35..M3-37
/// cover: OQ-19 rules that a node which cannot authorize must still take part in consensus, so
/// "cannot authorize" may not be a startup failure. The ready line is therefore about
/// *listeners*, and `ready` in the health payload is about *client traffic*; the two disagree
/// here on purpose, and that disagreement is the whole point of the row (critic M5).
#[retcd_test]
async fn m3_45_daemon_without_policy_is_unready_and_denies() {
    const METHOD: &str = "m3_45_daemon_without_policy_is_unready_and_denies";
    // One voter, so a single daemon can form and lead on its own.
    let harness = Harness::with_nodes(METHOD, &[1]).await;
    let node = &harness.nodes[0];
    harness.write_node_files(
        node,
        &NodeOptions {
            // No `[authz]` block at all, and the spec below sets no `--dev-allow-all`.
            policy: None,
            ..harness.node_options()
        },
    );

    let mut spec = harness.spec(0);
    spec.form = true;
    let mut process = DaemonProcess::spawn(spec);
    process
        .wait_ready(startup_deadline())
        .unwrap_or_else(|e| panic!("a policy-less daemon must still bind and announce: {e}"));

    // It genuinely runs: it elects itself leader over its one-voter membership.
    let health = wait_for_all(
        &[process.health_endpoint().to_string()],
        "the policy-less node to elect itself",
        |p| p[0].current_leader.is_some(),
    )
    .await;
    assert!(
        !health[0].ready,
        "a node with no policy must report ready=false: {:#?}",
        health[0]
    );
    assert_eq!(
        health[0].authz_kind, "missing",
        "the health payload must name the reason it is unready: {:#?}",
        health[0]
    );

    // The principal the harness policy grants everything to is denied all the same, because
    // there is no policy to grant it anything.
    let client = client_for(
        &harness,
        vec![process.client_endpoint().to_string()],
        PRINCIPAL,
    );
    let err = client
        .get(config_core::GetRequest {
            key: Bytes::from_static(b"/m3-45/k"),
        })
        .await
        .expect_err("a node that cannot authorize must deny a read");
    assert!(
        matches!(err, config_core::ConfigError::PermissionDenied { .. }),
        "expected PermissionDenied, got {err:?}"
    );

    // The refusal is counted, and counted as what it was. An operator watching a policy-less
    // node needs "it is refusing traffic" to be visible without reading logs, and the two
    // counters must not be confused: the client authenticated perfectly well, it simply had no
    // policy to be granted anything by.
    let after = support::health(process.health_endpoint()).await;
    assert_eq!(
        after.authz_denied, 1,
        "the denied read must be counted: {after:#?}"
    );
    assert_eq!(
        after.authn_rejected, 0,
        "a valid certificate that no policy covers is not an authentication failure: {after:#?}"
    );
    // A node that could not load a policy holds no grants and has no document to hash, and
    // must not report otherwise.
    assert_eq!(after.policy.grants, 0, "{after:#?}");
    assert_eq!(after.policy.policy_hash_hex, None, "{after:#?}");

    // The refusal is announced once at startup, so an operator can see why without issuing a
    // request first.
    let unavailable = lines_with_message(node, METHOD, "authz_unavailable");
    assert_eq!(
        unavailable.len(),
        1,
        "starting without a policy must log authz_unavailable exactly once: {unavailable:#?}"
    );
    assert_eq!(
        field(&unavailable[0], "reason"),
        Some("no_policy_configured")
    );

    process.stop_gracefully(startup_deadline()).await;
}

// =====================================================================================
// M3-46 — the CLI capability report matches the running node (test plan §4.1)
// =====================================================================================

/// M3-46: `config-server --capabilities` JSON matches the running node on every field a live
/// channel can check, and the fields no live channel exposes are pinned to their documented
/// unconditional literals (see the module doc comment — this is not weaker coverage, it is the
/// strongest claim actually checkable from outside the process).
#[retcd_test]
async fn m3_46_capabilities_cli_matches_runtime() {
    let harness = Harness::new("m3_46_capabilities_cli_matches_runtime").await;

    let spec = {
        // `spec_no_listen`: a `--capabilities` run binds nothing, so node 0's reserved ports
        // must stay held until `start_all()` below spawns the real daemon (critic A8).
        let mut spec = harness.spec_no_listen(0);
        spec.capabilities = true;
        spec.health_listen = None;
        spec
    };
    let (code, stdout, stderr) = daemon::run_to_completion(&spec);
    assert_eq!(
        code,
        Some(0),
        "--capabilities must exit 0; stderr:\n{stderr}"
    );
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 1, "--capabilities printed {lines:?}");
    let reported: serde_json::Value =
        serde_json::from_str(lines[0]).expect("the capability report is JSON");

    // Fields no live channel can re-derive: pinned to the documented unconditional literals.
    assert_eq!(reported["watch_resumption"], "Unsupported");
    assert_eq!(reported["pagination"], "Unsupported");
    assert_eq!(reported["dedup"], "Unsupported");

    let nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    assert_eq!(health[0].durability, reported["durability"]);
    assert_eq!(health[0].authz_kind, "static_allowlist");
    assert_eq!(reported["authz"], "StaticAllowlist");
    assert_eq!(health[0].transport_security, reported["transport_security"]);
}

// =====================================================================================
// M3-66 — a valid signed manifest forms a real three-process cluster (test plan §4.9)
// =====================================================================================

/// M3-66: three daemons, a validly signed manifest, `--form` on node 1: the cluster forms,
/// every node agrees on the full voter set, and every node reports the same policy.
///
/// The policy half is M3-42's claim at the only level where it can actually fail: three
/// separate processes, each having read the file from disk for itself. The digest is over the
/// document bytes, so agreement here means the three really did load the same file — a check
/// no in-process harness can make, because there is only ever one copy of anything.
#[retcd_test]
async fn m3_66_valid_manifest_forms_cluster() {
    let harness = Harness::new("m3_66_valid_manifest_forms_cluster").await;
    let nodes = harness.start_all();
    let health = wait_formed(&nodes).await;
    for h in &health {
        assert_eq!(h.membership_voter_ids, vec![1, 2, 3]);
    }

    let expected_hash = {
        use sha2::{Digest, Sha256};
        Sha256::digest(support::default_policy().as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    for h in &health {
        assert_eq!(h.policy.kind, "StaticAllowlist", "{h:#?}");
        // `default_policy()` declares exactly one `[[grant]]`; a literal, so that editing the
        // fixture without revisiting this row fails loudly rather than re-deriving itself.
        assert_eq!(
            h.policy.grants, 1,
            "the daemon must publish the real grant count, not 0: {h:#?}"
        );
        assert_eq!(
            h.policy.policy_hash_hex.as_deref(),
            Some(expected_hash.as_str()),
            "the digest must be of the policy document bytes the daemon read: {h:#?}"
        );
    }
    assert!(
        health.windows(2).all(|w| w[0].policy == w[1].policy),
        "all three processes must report an identical policy summary: {health:#?}"
    );
}

// =====================================================================================
// M3-67 / M3-68 / M3-69 / M3-70 — cryptographically broken manifests (test plan §4.9)
// =====================================================================================

/// A refused `--form` attempt: exit 2, no ready line, no `formation_started`, a single
/// `startup_failed`/`manifest_rejected` line whose `detail` contains `detail_contains`.
fn assert_manifest_refused(harness: &Harness, detail_contains: &str) {
    // `verify_document` only runs `if cli.form` (`run.rs` step 2) — without `--form` the
    // daemon never reads the manifest at all, starts as an ordinary (non-forming) node, and
    // never exits, which is exactly what made this hang before this fix.
    let mut spec = harness.spec(0);
    spec.form = true;
    let (code, stdout, stderr) = daemon::run_to_completion(&spec);
    assert_eq!(
        code,
        Some(2),
        "a broken manifest must exit 2; stderr:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "a refused daemon must print no ready line, got: {stdout:?}"
    );

    let lines = startup_failed_lines(&harness.nodes[0], harness.method);
    assert_eq!(
        lines.len(),
        1,
        "expected exactly one startup_failed line; log:\n{lines:#?}"
    );
    assert_eq!(field(&lines[0], "reason"), Some("manifest_rejected"));
    let detail = field(&lines[0], "detail").unwrap_or_default();
    assert!(
        detail.contains(detail_contains),
        "detail {detail:?} does not contain {detail_contains:?}"
    );

    let formed = support::count_messages(
        &harness.nodes[0].log_dir.join("1.jsonl"),
        "formation_started",
    );
    assert_eq!(
        formed, 0,
        "a refused manifest must never reach formation_started"
    );
}

/// M3-67: one byte of `manifest.toml` is flipped after signing (`Tamper::Body`). The signature
/// no longer covers the document and verification fails before any field is read.
#[retcd_test]
async fn m3_67_tampered_toml_rejected() {
    let harness = Harness::new("m3_67_tampered_toml_rejected").await;
    let manifest = base_manifest(&harness);
    harness.manifest_fixture.write_tampered(
        harness.manifest.manifest.parent().unwrap(),
        &manifest,
        Tamper::Body,
    );

    assert_manifest_refused(&harness, "signature does not verify");
}

/// M3-68: `manifest.sig` itself is flipped (`Tamper::Signature`). Same refusal as M3-67 — see
/// the module doc comment on why the daemon cannot and need not distinguish the two.
#[retcd_test]
async fn m3_68_tampered_signature_rejected() {
    let harness = Harness::new("m3_68_tampered_signature_rejected").await;
    let manifest = base_manifest(&harness);
    harness.manifest_fixture.write_tampered(
        harness.manifest.manifest.parent().unwrap(),
        &manifest,
        Tamper::Signature,
    );

    assert_manifest_refused(&harness, "signature does not verify");
}

/// M3-69: the document is re-signed with a different Ed25519 key entirely, while the node
/// keeps trusting the original public key (its `[manifest] signing_key_pub` file is untouched).
/// Verification against the trusted key fails — the same `BadSignature` outcome as M3-67/68,
/// because `ManifestError` has no key-id concept to report separately (see module doc comment).
#[retcd_test]
async fn m3_69_wrong_signing_key_rejected() {
    let harness = Harness::new("m3_69_wrong_signing_key_rejected").await;
    let manifest = base_manifest(&harness);
    let rogue = ManifestFixture::new(0xBAD_51612);
    let toml = manifest.to_toml();
    let signature = rogue.sign(toml.as_bytes());
    std::fs::write(&harness.manifest.manifest, toml.as_bytes()).expect("write manifest.toml");
    std::fs::write(&harness.manifest.signature, signature).expect("write manifest.sig");
    // harness.manifest.public_key is left exactly as the harness wrote it: the genuine key.

    assert_manifest_refused(&harness, "signature does not verify");
}

/// M3-70: a missing signature and a truncated signature are both typed refusals — never
/// "no signature = OK". Two sub-cases against the same harness (manifest verification never
/// opens the store far enough to leave state behind, so reusing node 0 is safe).
#[retcd_test]
async fn m3_70_truncated_or_missing_signature_rejected() {
    let harness = Harness::new("m3_70_truncated_or_missing_signature_rejected").await;

    // Sub-case A: the signature file does not exist at all.
    std::fs::remove_file(&harness.manifest.signature).expect("remove manifest.sig");
    assert_manifest_refused(&harness, "cannot read manifest file");
    clear_log(&harness);

    // Sub-case B: the signature file exists but is truncated to a handful of bytes.
    std::fs::write(&harness.manifest.signature, [0u8; 10]).expect("write a truncated signature");
    assert_manifest_refused(&harness, "expected 64 raw bytes, got 10");
}

/// Remove the previous sub-case's log lines so the next `assert_manifest_refused` call's
/// "exactly one `startup_failed` line" check is not looking at a stale attempt.
fn clear_log(harness: &Harness) {
    let path = harness.nodes[0].log_dir.join("1.jsonl");
    let _ = std::fs::remove_file(path);
}

/// The manifest `base_manifest` was signed for: `harness`'s own three voters, cluster id and
/// epoch — the same document `Harness::new` already wrote, reconstructed so a test can derive
/// a deliberately broken variant from it via `ManifestFixture::write_tampered`.
fn base_manifest(harness: &Harness) -> Manifest {
    let mut manifest = Manifest::new(harness.cluster_id);
    for node in &harness.nodes {
        manifest = manifest.with_voter(node.voter());
    }
    manifest
}

// =====================================================================================
// M3-71 / M3-72 — semantic manifest refusals (test plan §4.9)
// =====================================================================================

/// M3-71: a correctly signed manifest whose `expires_at` is in the past is refused, with no
/// sleeping — the harness never waits for a clock, it writes an already-past instant.
#[retcd_test]
async fn m3_71_expired_manifest_rejected() {
    let harness = Harness::new("m3_71_expired_manifest_rejected").await;
    let manifest = base_manifest(&harness);
    harness.manifest_fixture.write_tampered(
        harness.manifest.manifest.parent().unwrap(),
        &manifest,
        Tamper::Expired,
    );

    assert_manifest_refused(&harness, "expired at");
}

/// M3-72: the manifest names a cluster different from the one this node's own `[node]` block
/// is configured for. Refused with a `Mismatch` detail. The row's suggested `reason=` value
/// (`identity_mismatch`) belongs to a *different* check — the storage-layer identity gate
/// exercised by E2E-14 when a node is pointed at another node's data directory — not to this
/// manifest-layer check, whose real `reason` is the same constant `manifest_rejected` as every
/// other manifest refusal (see module doc comment).
#[retcd_test]
async fn m3_72_manifest_cluster_id_must_match_node_identity() {
    let harness = Harness::new("m3_72_manifest_cluster_id_must_match_node_identity").await;
    let manifest = base_manifest(&harness);
    harness.manifest_fixture.write_tampered(
        harness.manifest.manifest.parent().unwrap(),
        &manifest,
        Tamper::WrongCluster,
    );

    assert_manifest_refused(&harness, "this node is bound to");
}

// =====================================================================================
// M3-73 — the manifest is not authority after formation (test plan §4.9)
// =====================================================================================

/// M3-73: after formation, the manifest is rewritten with a bogus endpoint for node 2, and
/// node 1 is restarted (without `--form`, since `--form` is documented as one-time). Node 1
/// never reads `[manifest]` again on a plain restart — `run::run` only calls
/// `manifest::verify_document` `if cli.form` — so this proves the property about as directly
/// as a black-box test can: replication with the *real* node 2 keeps working even though the
/// on-disk manifest now lies about where node 2 is.
#[retcd_test]
async fn m3_73_manifest_is_not_authority_after_formation() {
    let harness = Harness::new("m3_73_manifest_is_not_authority_after_formation").await;
    let mut nodes = harness.start_all();
    wait_formed(&nodes).await;

    let client = cluster_client(&harness, &nodes);
    let before = client
        .put(PutRequest {
            key: Bytes::from_static(b"/m3-73/before"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
        })
        .await
        .expect("a write before the restart succeeds");
    assert_eq!(before.outcome, MutationOutcome::Applied);

    nodes[0].stop_gracefully(startup_deadline()).await;
    std::fs::remove_file(&harness.nodes[0].shutdown_file).expect("remove shutdown file");

    // Rewrite node 2's manifest entry with an endpoint nothing is listening on, genuinely
    // signed (this is not testing signature validation).
    let mut broken = base_manifest(&harness);
    if let Some(voter) = broken.voters.iter_mut().find(|v| v.node_id == 2) {
        voter.peer = "127.0.0.1:1".to_string(); // testkit:allow-port
        voter.client = "127.0.0.1:2".to_string(); // testkit:allow-port
    }
    harness
        .manifest_fixture
        .write(harness.manifest.manifest.parent().unwrap(), &broken);

    nodes[0] = harness.start(0, false);

    let health = wait_formed(&nodes).await;
    assert_eq!(health[0].node_id, 1);

    // Replication with the real node 2 still works: a write after the restart converges on
    // all three nodes' actual state hash.
    let after = client
        .put(PutRequest {
            key: Bytes::from_static(b"/m3-73/after"),
            value: Bytes::from_static(b"v2"),
            expected_mod_revision: None,
        })
        .await
        .expect("a write after the restart still reaches a real quorum");
    assert_eq!(after.outcome, MutationOutcome::Applied);

    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    let converged = wait_for_all(
        &endpoints,
        "all three nodes to converge on one state hash",
        |p| {
            let hashes: std::collections::BTreeSet<&str> =
                p.iter().map(|h| h.state_hash_hex.as_str()).collect();
            hashes.len() == 1 && p.iter().all(|h| h.cluster_revision == after.revision)
        },
    )
    .await;
    assert_eq!(converged.len(), 3);
    // Exactly one `formation_started` for the whole test: the original `--form` at
    // `start_all()`. A second one would mean the restart (no `--form`) somehow re-read and
    // re-acted on the manifest, which `run::run` structurally cannot do (`verify_document`
    // only runs `if cli.form`, and the restart spec never sets it).
    assert_eq!(
        support::count_messages(
            &harness.nodes[0].log_dir.join("1.jsonl"),
            "formation_started"
        ),
        1,
        "a plain restart (no --form) must never re-read or re-act on the manifest"
    );
}

// =====================================================================================
// M3-74 — a manifest voter set that does not match the real cluster (test plan §4.9)
// =====================================================================================

/// M3-74: the manifest lists voters `{1, 2, 4}` for a real three-process cluster `{1, 2, 3}`.
///
/// The row's expected outcome is "a typed error; no `Raft::initialize`". That expectation
/// assumes the daemon holds a voter set of its own to compare the manifest against. It does
/// not, and by design: **in the daemon the manifest *is* the formation plan** (ADR-0018 §7
/// note). The `FormationPlan` handed to `Raft::initialize` is built directly from
/// `verified.voters`, so "does the manifest match the plan?" is not a question that can be
/// asked. What the daemon actually defends is narrower and stated in the ADR: its own id must
/// be listed, the endpoints the manifest publishes for it must equal the ones it bound, and
/// every peer it later replicates with must present a certificate whose SAN names that peer.
/// A manifest naming a voter that does not exist is a signing-authority mistake, and the
/// signature is the control that covers it.
///
/// So node 1, itself listed, passes every manifest check and formation genuinely proceeds with
/// the manifest's voter set — confirmed by running this: `formation_started` /
/// `formation_succeeded` are logged for real and a leader is elected.
///
/// A first draft of this test additionally expected the resulting cluster to be permanently
/// stuck (reasoning that a 2-of-3 quorum over `{1, 2, 4}` could never be reached since node 4
/// does not exist). That is wrong, caught by actually running it: nodes 1 and 2 are both real
/// and both listed, and **2 of 3 is already a majority** — the daemons elect a leader between
/// themselves without node 4 ever needing to exist. The observable consequence, asserted
/// below, is that committed membership is `{1, 2, 4}`: node 3, a real running process, is
/// never a voter. The lead ruling accepted this as correct-by-design rather than a gap, which
/// is why the assertions below describe it rather than demanding a refusal.
#[retcd_test]
async fn m3_74_manifest_node_set_must_match_formation_plan() {
    let harness = Harness::new("m3_74_manifest_node_set_must_match_formation_plan").await;

    let mut broken = Manifest::new(harness.cluster_id);
    broken = broken.with_voter(harness.nodes[0].voter());
    broken = broken.with_voter(harness.nodes[1].voter());
    // Node 3's real endpoints are replaced by a node id nothing serves.
    broken = broken.with_voter(config_testkit::manifest::Voter::new(
        NodeId(4),
        "127.0.0.1:1", // testkit:allow-port
        "127.0.0.1:2", // testkit:allow-port
    ));
    harness
        .manifest_fixture
        .write(harness.manifest.manifest.parent().unwrap(), &broken);

    // Node 3 is never spawned: the manifest does not name it, so it has nothing to join, and
    // {1, 2} is already a majority of the manifest's {1, 2, 4}.
    let node1 = harness.start(0, true);
    let node2 = harness.start(1, false);
    let nodes = [node1, node2];

    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| n.health_endpoint().to_string())
        .collect();
    // Wait for both nodes to report a leader AND a fully populated membership: a node can
    // observe `current_leader.is_some()` slightly before its locally-applied membership catches
    // up, so gating on the leader alone is too weak for the `membership_voter_ids` assertion
    // below (this under-specification caused a real flake on a second run — same pattern as the
    // stricter predicate `wait_formed` already uses elsewhere in this file).
    let health = wait_for_all(
        &endpoints,
        "nodes 1 and 2 to elect a leader and settle on a 3-voter membership between them",
        |p| {
            p.iter()
                .all(|h| h.current_leader.is_some() && h.membership_voter_ids.len() == 3)
        },
    )
    .await;

    assert_eq!(
        support::count_messages(nodes[0].log_file().as_path(), "formation_started"),
        1,
        "expected finding: formation genuinely proceeds with the mismatched voter set"
    );
    for h in &health {
        let mut voters = h.membership_voter_ids.clone();
        voters.sort_unstable();
        assert_eq!(
            voters,
            vec![1, 2, 4],
            "the manifest is the formation plan (ADR-0018 §7 note): the daemon commits the \
             voter set the signed manifest names, here {{1,2,4}}, excluding the real node 3 — \
             there is no independent voter set to refuse it against"
        );
    }
}
