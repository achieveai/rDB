//! M6-33 and M6-35 — the policy version a backup references, and the divergence a restore
//! reports (ADR-0027 "Backup, restore, and `authz.mode`"; spec §15.3).
//!
//! # Why these rows need a running daemon
//!
//! M6-33's claim is not "the field can hold a number" — it is "the number is the one this node
//! was *enforcing* at export time". That binding exists only where the policy loader and the
//! artifact writer meet, which is the admin-plane `Backup` RPC on a live node
//! (`run.rs::NodeBackend::backup`). An in-process call to `finish_artifact` would assert the
//! argument it was just handed.
//!
//! M6-35 then depends on M6-33 for its input: a restore can only report a divergence against a
//! manifest that names a version, so the artifact these rows restore has to be one a live node
//! produced. That is why the two ship together.
//!
//! The offline `config-server backup` path is deliberately **not** covered here. It reads a
//! stopped data directory and runs no policy loader, so it has no active version to record; the
//! durable policy version floor that would give it one is tracked separately (gap G-09).
//!
//! # Relationship to the other M6 test files
//!
//! `m6_rbac.rs` and `m6_policy_daemon.rs` own the policy *lifecycle* rows and are read-only from
//! here; `crates/config-server/tests/support/mod.rs` is likewise read-only. This file copies the
//! few small helpers it needs rather than editing either, per the one-writer-per-test-file rule.

mod support;

use std::path::{Path, PathBuf};
use std::process::Command;

use bytes::Bytes;
use config_client::{AdminClient, GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ClusterId, ConfigStore, PutRequest};
use config_log::retcd_test;
use config_testkit::manifest::{Manifest, ManifestPaths, Voter};
use ed25519_dalek::{Signature, Verifier};

use support::{deadline, Harness, NodeOptions, PolicyFixture, PRINCIPAL};

/// The version of the document in force while the backup is taken.
///
/// The test plan's M6-33 and M6-35 rows both name 8, and the number is load-bearing: a fix that
/// recorded "some version" rather than "the active version" would still pass against 1.
const POLICY_VERSION: u64 = 8;

/// The version the restored node is declared to run under in M6-35's divergence row.
///
/// The plan names 3, and it is deliberately *lower* than the artifact's: a restore may
/// legitimately be handed an older policy, and that must be reported rather than refused.
const RESTORED_POLICY_VERSION: u64 = 3;

/// How often the daemon re-reads its policy files. Nothing here rotates the document, so this
/// only sets how long a startup load may take to settle.
const POLL_SECS: u64 = 1;

/// Entries written before the backup, so the snapshot it triggers has something in it.
const KEYS: usize = 4;

/// The stem the artifact triple shares.
const NAME: &str = "m6";

/// The cluster a restore mints. Differs from [`support::CLUSTER_HEX`], which every restore row
/// depends on: reusing the source cluster id is the one refusal ADR-0024 checks first.
const NEW_CLUSTER_HEX: &str = "e2ee2ee2e00000000000000000000077";

/// The epoch a restore advances to. The fixture cluster forms at epoch 0.
const NEW_EPOCH: &str = "1";

// -------------------------------------------------------------------------------------------
// driving the binary
// -------------------------------------------------------------------------------------------

/// One completed subprocess run of the shipped binary.
///
/// The restore rows drive the real `config-server` rather than calling `backup::restore`
/// directly, because what M6-35 asserts is a *line an operator reads*: the emission lives in
/// `main.rs`, on the stderr JSONL channel the offline subcommands use in place of a tracing
/// subscriber they do not install. A library call would prove the decision and skip the line.
#[derive(Debug)]
struct CliRun {
    code: i32,
    stdout: String,
    stderr: String,
}

impl CliRun {
    /// Every JSONL audit record named `msg` on stderr. Empty when the line was not emitted,
    /// which is the whole assertion of the agreement row.
    fn events(&self, msg: &str) -> Vec<serde_json::Value> {
        self.stderr
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line.trim()).ok())
            .filter(|v| v["msg"] == msg)
            .collect()
    }

    /// The one record named `msg`, or a failure naming what was actually on stderr.
    fn event(&self, msg: &str) -> serde_json::Value {
        let found = self.events(msg);
        assert_eq!(
            found.len(),
            1,
            "expected exactly one {msg:?} record, got {found:?} from stderr {:?}",
            self.stderr
        );
        found.into_iter().next().expect("checked above")
    }

    fn assert_ok(&self) {
        assert_eq!(
            self.code, 0,
            "expected success; stdout={:?} stderr={:?}",
            self.stdout, self.stderr
        );
    }
}

fn run_cli(args: &[&str]) -> CliRun {
    let output = Command::new(env!("CARGO_BIN_EXE_config-server"))
        .args(args)
        .output()
        .expect("the shipped binary runs");
    CliRun {
        code: output.status.code().expect("the process exited normally"),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

// -------------------------------------------------------------------------------------------
// the fixture
// -------------------------------------------------------------------------------------------

/// A signed-policy cluster that formed, took writes, produced a backup through the **admin
/// plane** while v8 was in force, and then stopped.
///
/// The node is stopped before any row restores, because a restore row reads only files. Keeping
/// it running would add a live authority to a test about a recovered one.
struct PolicyBackup {
    harness: Harness,
    /// `<root>/rpc-backup`, holding the triple named [`NAME`].
    out: PathBuf,
    /// Where `sign.key` and `trust.pub` live.
    keys: PathBuf,
    /// The signed manifest's bytes, already verified against `trust.pub`.
    manifest_bytes: Vec<u8>,
}

impl PolicyBackup {
    async fn build(method: &'static str) -> Self {
        let harness = Harness::with_nodes(method, &[1]).await;

        // The artifact's own signing key. Distinct from the policy fixture's key: a backup
        // manifest and a policy document are signed by different authorities, and sharing one
        // seed here would hide a wiring mistake that crossed them. Fixed bytes rather than
        // random ones, so a failing row replays exactly.
        let keys = harness.root().join("keys");
        std::fs::create_dir_all(&keys).expect("create the key directory");
        let seed = [11u8; 32];
        std::fs::write(keys.join("sign.key"), seed).expect("write the signing seed");
        let signing = ed25519_dalek::SigningKey::from_bytes(&seed);
        std::fs::write(keys.join("trust.pub"), signing.verifying_key().to_bytes())
            .expect("write the trust key");

        // `PRINCIPAL` is an admin **in the document**, not in `[authz] admins`: under signed
        // mode the admin set is the active document's and the static key is not consulted
        // (M6-40). Without this the `Backup` RPC below would be denied.
        let fixture = PolicyFixture::new(harness.root());
        fixture.write(POLICY_VERSION, &["/m6/"], &[PRINCIPAL]);

        let options = NodeOptions {
            policy: None,
            signed_policy: Some(fixture.authz(POLL_SECS)),
            backup_signing_key: Some(keys.join("sign.key")),
            ..harness.node_options()
        };
        harness.write_node_files(&harness.nodes[0], &options);
        let mut node = harness.start(0, true);

        // Establish the premise before exercising the claim: if the node were serving some
        // other version, the rows below would be testing the wrong number rather than failing.
        let health = support::health(node.health_endpoint()).await;
        assert_eq!(
            health.policy.kind,
            serde_json::json!({ "SignedPolicy": { "policy_version": POLICY_VERSION } }),
            "the backup has to be taken while v{POLICY_VERSION} is the document in force"
        );

        let opts = GrpcClientOptions {
            request_deadline: deadline(20),
            tls: TlsMode::MutualTls(harness.tls.client_mtls(PRINCIPAL)),
            ..GrpcClientOptions::default()
        };
        let client = GrpcClient::connect(vec![node.client_endpoint().to_string()], opts)
            .expect("the client plane endpoint is well formed")
            .with_cluster_id(harness.cluster_id);
        for i in 0..KEYS {
            client
                .put(PutRequest {
                    key: Bytes::from(format!("/m6/{i}")),
                    value: Bytes::from(format!("v{i}")),
                    ..PutRequest::default()
                })
                .await
                .unwrap_or_else(|e| panic!("put {i}: {e}"));
        }

        let out = harness.root().join("rpc-backup");
        std::fs::create_dir_all(&out).expect("create the backup directory");
        let admin = AdminClient::new(client);
        let info = admin
            .backup(out.display().to_string(), Some(NAME.to_string()))
            .await
            .expect("the admin plane serves Backup");
        assert_eq!(info.name, NAME);

        let status = node.stop_gracefully(deadline(10)).await;
        assert_eq!(status.code(), Some(0), "the fixture node shut down cleanly");

        // Signature first, over the bytes as they are on disk, exactly as `verify-backup` does.
        // Parsing first would let a manifest carrying the right number but no valid signature
        // pass M6-33, and the property under test is that the version is inside the *signed*
        // bytes — a field outside them could be edited after the fact by anyone.
        let manifest_bytes = std::fs::read(out.join(format!("{NAME}.manifest.json")))
            .expect("the manifest was written");
        let sig_bytes: [u8; 64] = std::fs::read(out.join(format!("{NAME}.manifest.sig")))
            .expect("the detached signature was written")
            .try_into()
            .expect("a detached Ed25519 signature is 64 bytes");
        signing
            .verifying_key()
            .verify(&manifest_bytes, &Signature::from_bytes(&sig_bytes))
            .expect("the manifest is signed by the node's configured backup key");

        Self {
            harness,
            out,
            keys,
            manifest_bytes,
        }
    }

    /// A path under the harness root, as a string, ready to pass to a subprocess.
    fn path(&self, name: &str) -> String {
        self.harness.root().join(name).display().to_string()
    }

    /// A bootstrap manifest for the cluster a restore is about to mint, signed by the same
    /// authority the harness uses for `--form`.
    ///
    /// Restore runs the identical `--form` verification, so this has to be a real signed triple.
    fn new_manifest(&self, dir: &str) -> ManifestPaths {
        let cluster: ClusterId = NEW_CLUSTER_HEX.parse().expect("a well formed cluster id");
        let document = Manifest::new(cluster)
            .with_epoch(config_core::RecoveryEpoch(
                NEW_EPOCH.parse().expect("the epoch is a number"),
            ))
            // Never dialled: `verify_document` checks the signature, the expiry and agreement
            // with the identity being minted. `check_endpoints` runs at `--form`, not here.
            .with_voter(Voter::new(
                config_core::NodeId(1),
                self.harness.nodes[0].peer.to_string(),
                self.harness.nodes[0].client.to_string(),
            ));
        self.harness
            .manifest_fixture
            .write(Path::new(dir), &document)
    }

    /// Restore this fixture's artifact into a fresh directory, optionally declaring the policy
    /// version the restored node will run under.
    fn restore(&self, case: &str, active_policy_version: Option<u64>) -> CliRun {
        let from = self.out.display().to_string();
        let data_dir = self.path(&format!("{case}-data"));
        let manifest = self.new_manifest(&self.path(&format!("{case}-manifest")));
        let (manifest_path, signature, public_key) = (
            manifest.manifest.display().to_string(),
            manifest.signature.display().to_string(),
            manifest.public_key.display().to_string(),
        );
        let trust = self.keys.join("trust.pub").display().to_string();
        let version = active_policy_version.map(|v| v.to_string());

        // `--manifest-sig` and `--manifest-key` are always passed: the testkit writes
        // `manifest.sig` / `manifest.pub`, while the CLI's defaults are `<manifest>.sig` /
        // `<manifest>.pub` — i.e. `manifest.toml.sig`.
        let mut args = vec![
            "restore",
            "--from",
            &from,
            "--name",
            NAME,
            "--data-dir",
            &data_dir,
            "--cluster-id",
            NEW_CLUSTER_HEX,
            "--recovery-epoch",
            NEW_EPOCH,
            "--node-id",
            "1",
            "--manifest",
            &manifest_path,
            "--manifest-sig",
            &signature,
            "--manifest-key",
            &public_key,
            "--trust-key",
            &trust,
        ];
        if let Some(version) = &version {
            args.extend_from_slice(&["--active-policy-version", version]);
        }
        run_cli(&args)
    }
}

// -------------------------------------------------------------------------------------------
// M6-33
// -------------------------------------------------------------------------------------------

/// The signed manifest of a backup taken through the admin plane names the policy version that
/// was in force, and names nothing else about the policy (M6-33).
#[retcd_test]
async fn m6_33_backup_manifest_references_the_active_policy_version() {
    let fixture =
        PolicyBackup::build("m6_33_backup_manifest_references_the_active_policy_version").await;

    let manifest: serde_json::Value =
        serde_json::from_slice(&fixture.manifest_bytes).expect("the signed manifest parses");
    assert_eq!(
        manifest
            .get("policy_version_ref")
            .and_then(serde_json::Value::as_u64),
        Some(POLICY_VERSION),
        "the manifest must reference the policy version that was active at export time; it \
         reads {manifest:#}"
    );

    // The second half of §15.3: artifacts *reference* the RBAC artifact, they do not contain
    // it. A manifest carrying grants or principals would make every backup a second,
    // unversioned copy of the authorization state, outliving the document it was copied from.
    for forbidden in [
        "grants",
        "admins",
        "principals",
        "policy",
        "policy_document",
    ] {
        assert!(
            manifest.get(forbidden).is_none(),
            "the manifest must not carry {forbidden:?}: {manifest:#}"
        );
    }
}

// -------------------------------------------------------------------------------------------
// M6-35
// -------------------------------------------------------------------------------------------

/// A restore whose artifact names a different policy version than the restored node will run
/// under records the divergence and completes anyway (M6-35).
///
/// The row asserts the *line*, not the decision behind it. `restore_policy_mismatch` exists for
/// an operator reading stderr during a recovery, so a test that reached into `RestoreOutcome`
/// would pass with the emission deleted.
#[retcd_test]
async fn m6_35_restore_records_a_policy_divergence_without_blocking() {
    let fixture =
        PolicyBackup::build("m6_35_restore_records_a_policy_divergence_without_blocking").await;

    let run = fixture.restore("diverged", Some(RESTORED_POLICY_VERSION));
    run.assert_ok();

    let line = run.event("restore_policy_mismatch");
    assert_eq!(line["manifest_version"], serde_json::json!(POLICY_VERSION));
    assert_eq!(
        line["active_version"],
        serde_json::json!(RESTORED_POLICY_VERSION)
    );
    assert_eq!(
        line["level"], "warn",
        "ADR-0027 specifies warn, and stderr JSONL is the only channel an offline subcommand \
         has: {line}"
    );

    // Reported, never enforced. A restore that refused here would depend on an artifact the
    // backup does not contain — the independently supplied policy is allowed to be older.
    run.event("restore_completed");
}

/// A restore whose artifact names the same policy version the restored node will run under says
/// nothing (M6-35, the other side).
///
/// Worth its own row: a divergence line that fired on every restore would be noise, and an
/// operator who learns to skip it has lost the diagnostic the line exists to be.
#[retcd_test]
async fn m6_35_restore_says_nothing_when_the_policy_versions_agree() {
    let fixture =
        PolicyBackup::build("m6_35_restore_says_nothing_when_the_policy_versions_agree").await;

    let run = fixture.restore("agreed", Some(POLICY_VERSION));
    run.assert_ok();
    assert!(
        run.events("restore_policy_mismatch").is_empty(),
        "matching versions are not a divergence: {:?}",
        run.stderr
    );
    run.event("restore_completed");
}
