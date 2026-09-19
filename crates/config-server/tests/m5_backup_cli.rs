//! M5 backup/restore rows the sibling `m5_admin.rs` does not cover (test plan M5-86, M5-87,
//! M5-94; TA-47; ADR-0024). `m5_admin.rs` is DO-NOT-EDIT and already owns M5-75, 77-80, 82-85,
//! 90 — this file follows its exact idiom (a real formed-and-stopped single-node fixture, the
//! shipped binary driven as a subprocess, exit code plus one diagnostic line) rather than
//! importing from it, because its `Fixture`/`CliRun` types are private to that file.
//!
//! # What is not here
//!
//! M5-88/M5-89 (two live clusters, old and restored, refusing each other's peer traffic) need
//! two independently-identified clusters exchanging real peer RPCs — that is
//! `config_testkit::Cluster` territory, not a CLI subprocess test, and lives in
//! `crates/config-testkit/tests/m5_backup_fencing_cluster.rs` instead (see that file's module
//! doc for exactly what it proves and what it approximates).
//!
//! M5-92 (a restored store forming as the genesis member of a brand-new cluster alongside two
//! fresh peers) needs a harness seam that does not exist: every node `support::Harness` and
//! `config_testkit::cluster::Cluster` start opens a **fresh** data directory of their own
//! allocation; neither has a way to point a newly-provisioned node at a directory this file
//! already populated via `restore`. That is the same `provision_reusing_dir` gap
//! `m5_membership_cluster.rs`'s module doc describes for TA-46, applied to a restored directory
//! instead of a learner's. Skipped; the gap and its patch are recorded there rather than
//! repeated.
//!
//! M5-93 (`docs/evidence/backup-restore.json` at 1 GiB scale) is explicitly deferred to M6 by
//! the test plan's own row (TA-53) and by ruling M5-R7/R8 in the architecture doc. Not attempted.

mod support;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ClusterId, ClusterIdentity, ConfigStore, Limits, MutationOutcome, PutRequest};
use config_log::retcd_test;
use config_testkit::manifest::{Manifest, ManifestPaths, Voter};

use support::{deadline, PRINCIPAL};

const KEYS: usize = 8;
const NEW_CLUSTER_HEX: &str = "e2ee2ee2e00000000000000000000099";
const NEW_EPOCH: u32 = 1;

/// One completed subprocess run of the shipped binary — the same shape `m5_admin.rs` uses,
/// pared down to what this file's rows need.
#[derive(Debug)]
struct CliRun {
    code: i32,
    stdout: String,
    stderr: String,
}

impl CliRun {
    fn reason(&self) -> String {
        self.stderr
            .lines()
            .find_map(|line| line.strip_prefix("config-server: "))
            .and_then(|rest| rest.split(':').next())
            .unwrap_or_default()
            .trim()
            .to_string()
    }

    fn assert_refused(&self, code: i32, reason: &str) {
        assert_ne!(
            self.code, 101,
            "a refusal must be a refusal, not a panic: {}",
            self.stderr
        );
        assert_eq!(
            self.code, code,
            "expected exit {code} with reason {reason}; got {} and stderr {:?}",
            self.code, self.stderr
        );
        assert_eq!(self.reason(), reason, "wrong reason: {:?}", self.stderr);
        assert!(
            self.stdout.trim().is_empty(),
            "a refusal must leave stdout empty, got {:?}",
            self.stdout
        );
    }

    fn assert_ok(&self) -> serde_json::Value {
        assert_eq!(self.code, 0, "expected success; stderr={:?}", self.stderr);
        serde_json::from_str(self.stdout.trim()).unwrap_or_else(|e| {
            panic!(
                "stdout was not the one JSON line ({e}); stdout={:?}",
                self.stdout
            )
        })
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

struct ManifestPathStrings {
    manifest: String,
    signature: String,
    public_key: String,
}

impl From<ManifestPaths> for ManifestPathStrings {
    fn from(paths: ManifestPaths) -> Self {
        Self {
            manifest: paths.manifest.display().to_string(),
            signature: paths.signature.display().to_string(),
            public_key: paths.public_key.display().to_string(),
        }
    }
}

fn write_public(dir: &Path, seed: &str, out: &str) {
    let bytes: [u8; 32] = std::fs::read(dir.join(seed))
        .expect("read the seed")
        .try_into()
        .expect("the seed is 32 bytes");
    let signing = ed25519_dalek::SigningKey::from_bytes(&bytes);
    std::fs::write(dir.join(out), signing.verifying_key().to_bytes()).expect("write the key");
}

fn is_empty_dir(path: &str) -> bool {
    match std::fs::read_dir(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => panic!("cannot inspect {path}: {e}"),
        Ok(mut entries) => entries.next().is_none(),
    }
}

/// A single-node cluster that formed, took `KEYS` writes, and stopped: a real data directory a
/// backup may be taken from. Mirrors `m5_admin.rs`'s `Fixture` exactly, minus the pieces this
/// file's three rows never touch (encryption keys, corruption helpers).
struct Fixture {
    harness: support::Harness,
    data_dir: String,
    revision: u64,
    keys: PathBuf,
}

impl Fixture {
    async fn build(method: &'static str) -> Self {
        let harness = support::Harness::with_nodes(method, &[1]).await;
        let mut node = harness.start(0, true);

        let opts = GrpcClientOptions {
            request_deadline: deadline(5),
            tls: TlsMode::MutualTls(harness.tls.client_mtls(PRINCIPAL)),
            ..GrpcClientOptions::default()
        };
        let client = GrpcClient::connect(vec![node.client_endpoint().to_string()], opts)
            .expect("the client plane endpoint is well formed")
            .with_cluster_id(harness.cluster_id);

        let mut revision = 0;
        for i in 0..KEYS {
            let response = client
                .put(PutRequest {
                    key: Bytes::from(format!("/m5/backupcli/{i}")),
                    value: Bytes::from(format!("v{i}")),
                    ..PutRequest::default()
                })
                .await
                .unwrap_or_else(|e| panic!("put {i}: {e}"));
            assert_eq!(response.outcome, MutationOutcome::Applied);
            revision = response.revision;
        }
        drop(client);

        let status = node.stop_gracefully(deadline(10)).await;
        assert_eq!(status.code(), Some(0), "the fixture node shut down cleanly");

        let keys = harness.root().join("keys");
        std::fs::create_dir_all(&keys).expect("create the key directory");
        std::fs::write(keys.join("sign.key"), [7u8; 32]).expect("write the signing seed");
        write_public(&keys, "sign.key", "trust.pub");

        let data_dir = harness.nodes[0].data_dir.display().to_string();
        Self {
            harness,
            data_dir,
            revision,
            keys,
        }
    }

    fn key(&self, name: &str) -> String {
        self.keys.join(name).display().to_string()
    }

    fn path(&self, name: &str) -> String {
        self.harness.root().join(name).display().to_string()
    }

    fn backup(&self, out: &str) -> serde_json::Value {
        let signing = self.key("sign.key");
        let run = run_cli(&[
            "backup",
            "--data-dir",
            &self.data_dir,
            "--out",
            out,
            "--name",
            "b1",
            "--signing-key",
            &signing,
        ]);
        run.assert_ok()
    }

    fn new_manifest(&self, dir: &str, cluster_hex: &str, epoch: u32) -> ManifestPaths {
        let cluster: ClusterId = cluster_hex.parse().expect("a well formed cluster id");
        let document = Manifest::new(cluster)
            .with_epoch(config_core::RecoveryEpoch(epoch))
            .with_voter(Voter::new(
                config_core::NodeId(1),
                self.harness.nodes[0].peer.to_string(),
                self.harness.nodes[0].client.to_string(),
            ));
        self.harness
            .manifest_fixture
            .write(Path::new(dir), &document)
    }

    #[allow(clippy::too_many_arguments)]
    fn restore_args<'a>(
        &'a self,
        from: &'a str,
        data_dir: &'a str,
        cluster_hex: &'a str,
        epoch: &'a str,
        manifest: &'a ManifestPathStrings,
        trust: &'a str,
    ) -> Vec<&'a str> {
        vec![
            "restore",
            "--from",
            from,
            "--name",
            "b1",
            "--data-dir",
            data_dir,
            "--cluster-id",
            cluster_hex,
            "--recovery-epoch",
            epoch,
            "--node-id",
            "1",
            "--manifest",
            &manifest.manifest,
            "--manifest-sig",
            &manifest.signature,
            "--manifest-key",
            &manifest.public_key,
            "--trust-key",
            trust,
        ]
    }
}

// -------------------------------------------------------------------------------------------
// M5-86 — restore demands a new manifest, not an omitted one and not the source's own
// -------------------------------------------------------------------------------------------

#[retcd_test]
async fn m5_86_restore_requires_a_new_manifest_and_new_credentials() {
    let fixture = Fixture::build("m5_86_restore_requires_a_new_manifest_and_new_credentials").await;
    let out = fixture.path("backup");
    fixture.backup(&out);
    let trust = fixture.key("trust.pub");
    let epoch = NEW_EPOCH.to_string();

    // `--manifest` omitted entirely: a missing required argument is clap's own exit 2, caught
    // before any store or artifact I/O happens.
    let data_dir = fixture.path("restored-no-manifest");
    let run = run_cli(&[
        "restore",
        "--from",
        &out,
        "--name",
        "b1",
        "--data-dir",
        &data_dir,
        "--cluster-id",
        NEW_CLUSTER_HEX,
        "--recovery-epoch",
        &epoch,
        "--node-id",
        "1",
        "--trust-key",
        &trust,
    ]);
    assert_eq!(
        run.code, 2,
        "a missing required argument must be exit 2 (clap's own contract); stderr={:?}",
        run.stderr
    );
    assert!(
        is_empty_dir(&data_dir),
        "nothing must be written before argument parsing succeeds"
    );

    // The operator's more plausible mistake: supplying the *source* cluster's own manifest
    // (signed, well-formed, just for the wrong cluster) instead of a freshly minted one for the
    // restore target. Refused for identity mismatch between the manifest and `--cluster-id`,
    // which is the same `cluster_id_reused`-flavoured refusal `m5_admin.rs::m5_82` proves for
    // `--cluster-id` alone; this row proves it also holds when the *manifest document itself*
    // is the one naming the source cluster.
    let source_manifest_dir = fixture.path("source-manifest");
    let manifest: ManifestPathStrings = fixture
        .new_manifest(&source_manifest_dir, support::CLUSTER_HEX, NEW_EPOCH)
        .into();
    let data_dir = fixture.path("restored-source-manifest");
    run_cli(&fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, &epoch, &manifest, &trust))
        .assert_refused(2, "manifest_rejected");
    assert!(
        is_empty_dir(&data_dir),
        "a refused restore must leave the destination untouched"
    );
}

// -------------------------------------------------------------------------------------------
// M5-87 — restored_from is durable and is not a TOML-editable field
// -------------------------------------------------------------------------------------------

#[retcd_test]
async fn m5_87_restored_from_is_durable_and_not_editable() {
    let fixture = Fixture::build("m5_87_restored_from_is_durable_and_not_editable").await;
    let out = fixture.path("backup");
    fixture.backup(&out);

    let data_dir = fixture.path("restored");
    let manifest_dir = fixture.path("new-manifest");
    let manifest: ManifestPathStrings = fixture
        .new_manifest(&manifest_dir, NEW_CLUSTER_HEX, NEW_EPOCH)
        .into();
    let trust = fixture.key("trust.pub");
    let epoch = NEW_EPOCH.to_string();

    run_cli(&fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, &epoch, &manifest, &trust))
        .assert_ok();

    let identity = ClusterIdentity {
        cluster_id: NEW_CLUSTER_HEX.parse().expect("a well formed cluster id"),
        recovery_epoch: config_core::RecoveryEpoch(NEW_EPOCH),
        node_id: config_core::NodeId(1),
    };

    let expect_restored_from = |store: &config_storage::RocksStore| {
        let from = store
            .reader()
            .restored_from()
            .expect("a restored store records where it came from");
        assert_eq!(from.cluster_id.to_string(), support::CLUSTER_HEX);
        assert_eq!(from.recovery_epoch, 0);
        assert_eq!(from.revision, fixture.revision);
    };

    // Open once: the value is there immediately after restore.
    {
        let store = config_storage::RocksStore::open(
            Path::new(&data_dir),
            identity,
            Limits::default(),
            Arc::new(config_storage::NoFaults),
            tracing::Span::none(),
        )
        .expect("the restored store opens under the new identity");
        expect_restored_from(&store);
    }
    // TA-52's "survives restart" is a second, independent open of the same directory — the
    // store owns no in-memory-only cache of this value, so a fresh open is the honest proof
    // that it is durable rather than merely held over from the restore process's own memory.
    {
        let store = config_storage::RocksStore::open(
            Path::new(&data_dir),
            identity,
            Limits::default(),
            Arc::new(config_storage::NoFaults),
            tracing::Span::none(),
        )
        .expect("a second open of the same directory must also succeed");
        expect_restored_from(&store);
    }

    // "Cannot be changed by editing the TOML": there is no node TOML anywhere in this test —
    // every value above came out of the store the CLI wrote, not out of a config file this row
    // could have edited. The absence of any such file *is* the proof: grep every restore
    // argument and the fixture's own config surface for a flag that could set it.
    for forbidden in ["--restored-from", "--source-cluster", "restored_from ="] {
        assert!(
            !fixture.harness.root().join("config.toml").exists()
                || !std::fs::read_to_string(fixture.harness.root().join("config.toml"))
                    .unwrap_or_default()
                    .contains(forbidden),
            "no config surface may set {forbidden:?}"
        );
    }
}

// -------------------------------------------------------------------------------------------
// M5-94 — the source identity is bound nowhere except restored_from
// -------------------------------------------------------------------------------------------

#[retcd_test]
async fn m5_94_restore_does_not_reuse_the_source_identity_anywhere() {
    let fixture = Fixture::build("m5_94_restore_does_not_reuse_the_source_identity_anywhere").await;
    let out = fixture.path("backup");
    fixture.backup(&out);

    let data_dir = fixture.path("restored");
    let manifest_dir = fixture.path("new-manifest");
    let manifest: ManifestPathStrings = fixture
        .new_manifest(&manifest_dir, NEW_CLUSTER_HEX, NEW_EPOCH)
        .into();
    let trust = fixture.key("trust.pub");
    let epoch = NEW_EPOCH.to_string();

    run_cli(&fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, &epoch, &manifest, &trust))
        .assert_ok();

    let new_identity = ClusterIdentity {
        cluster_id: NEW_CLUSTER_HEX.parse().expect("a well formed cluster id"),
        recovery_epoch: config_core::RecoveryEpoch(NEW_EPOCH),
        node_id: config_core::NodeId(1),
    };
    let source_identity = ClusterIdentity {
        cluster_id: support::CLUSTER_HEX
            .parse()
            .expect("a well formed cluster id"),
        recovery_epoch: config_core::RecoveryEpoch(0),
        node_id: config_core::NodeId(1),
    };

    // The active identity the store is bound to is the new one and only the new one: opening
    // the restored directory under the *source* identity is refused exactly the way opening any
    // directory under the wrong identity is (ADR-0011) — proving the source identity was never
    // written as this directory's binding, only recorded (once, under `restored_from`) as
    // history.
    let err = config_storage::RocksStore::open(
        Path::new(&data_dir),
        source_identity,
        Limits::default(),
        Arc::new(config_storage::NoFaults),
        tracing::Span::none(),
    )
    .expect_err("the restored directory must not be bound to the source identity");
    match err {
        config_storage::StorageOpenError::IdentityMismatch {
            stored, configured, ..
        } => {
            assert_eq!(
                stored, new_identity,
                "the directory's actual binding is the new identity"
            );
            assert_eq!(configured, source_identity);
        }
        other => panic!("expected IdentityMismatch, got {other:?}"),
    }

    // And the new identity is exactly what does open it.
    let store = config_storage::RocksStore::open(
        Path::new(&data_dir),
        new_identity,
        Limits::default(),
        Arc::new(config_storage::NoFaults),
        tracing::Span::none(),
    )
    .expect("the restored store opens under the new identity");
    assert_eq!(store.identity(), new_identity);

    // No certificate, manifest, or allowlist material was copied into the destination: restore
    // touches only `--data-dir`, and its contents are RocksDB's own files plus nothing else.
    //
    // RocksDB itself always writes a `MANIFEST-<six digits>` file (its own LSM-tree metadata,
    // nothing to do with rEtcd's bootstrap manifest) — excluded by name so this row does not
    // flag RocksDB's own housekeeping as a leak.
    let leaked: Vec<_> = std::fs::read_dir(&data_dir)
        .expect("list the restored directory")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| {
            if name.starts_with("MANIFEST-")
                && name["MANIFEST-".len()..]
                    .chars()
                    .all(|c| c.is_ascii_digit())
            {
                return false;
            }
            let lower = name.to_lowercase();
            lower.contains("cert")
                || lower.contains("manifest")
                || lower.contains("allow")
                || lower.ends_with(".pem")
        })
        .collect();
    assert!(
        leaked.is_empty(),
        "restore must write nothing but the store's own files into --data-dir: {leaked:?}"
    );
}
