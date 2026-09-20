//! M5 rows for the offline CLI: the signed backup triple, its verification, and fenced restore
//! (test plan M5-75, M5-77..M5-80, M5-82..M5-85, M5-90; TA-47; ADR-0024).
//!
//! Every row drives the **shipped binary** as a subprocess and asserts on its exit code and its
//! one diagnostic line. That is the whole point of TA-47: an operator's nightly verification
//! script branches on the exit code, so a row that called the library function directly would
//! prove nothing about the contract that script depends on. `config-server` has no library
//! target either, so there is no shortcut available even if one were wanted.
//!
//! The directory under test is produced by a real daemon that formed a cluster, served real
//! writes and shut down cleanly — never hand-assembled. A synthetic directory would verify the
//! same way and would tell us nothing about whether a backup of a *cluster* does.

mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use bytes::Bytes;
use config_client::{GrpcClient, GrpcClientOptions, TlsMode};
use config_core::{ClusterId, ClusterIdentity, ConfigStore, Limits, MutationOutcome, PutRequest};
use config_log::retcd_test;
use config_testkit::manifest::{Manifest, ManifestPaths, Voter};
use sha2::{Digest, Sha256};

use support::{deadline, PRINCIPAL};

/// How many keys the fixture cluster holds. Small on purpose: these rows are about the
/// artifact's shape and the refusal matrix, not about volume.
const KEYS: usize = 8;

/// The cluster a restore mints. Differs from [`support::CLUSTER_HEX`] in the last nibble,
/// which is the whole point of every restore row below.
const NEW_CLUSTER_HEX: &str = "e2ee2ee2e00000000000000000000099";

/// The epoch a restore advances to. The fixture cluster forms at epoch 0.
const NEW_EPOCH: u32 = 1;

// -------------------------------------------------------------------------------------------
// driving the binary
// -------------------------------------------------------------------------------------------

/// One completed subprocess run of the shipped binary.
#[derive(Debug)]
struct CliRun {
    code: i32,
    stdout: String,
    stderr: String,
}

impl CliRun {
    /// The stable `reason` field of the one diagnostic line, which is what a script matches on
    /// when the exit code alone is too coarse.
    fn reason(&self) -> String {
        self.stderr
            .lines()
            .find_map(|line| line.strip_prefix("config-server: "))
            .and_then(|rest| rest.split(':').next())
            .unwrap_or_default()
            .trim()
            .to_string()
    }

    fn json(&self) -> serde_json::Value {
        serde_json::from_str(self.stdout.trim()).unwrap_or_else(|e| {
            panic!(
                "stdout was not the one JSON line ({e}); stdout={:?} stderr={:?}",
                self.stdout, self.stderr
            )
        })
    }

    /// A refusal is exactly one diagnostic line, an expected exit code, and never a panic.
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
        assert_eq!(
            self.reason(),
            reason,
            "wrong reason on the diagnostic line: {:?}",
            self.stderr
        );
        let lines: Vec<&str> = self
            .stderr
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();
        assert_eq!(
            lines.len(),
            1,
            "a refusal prints one line and nothing else, got {lines:?}"
        );
        assert!(
            self.stdout.trim().is_empty(),
            "a refusal must leave stdout empty, got {:?}",
            self.stdout
        );
    }

    fn assert_ok(&self) -> serde_json::Value {
        assert_eq!(self.code, 0, "expected success; stderr={:?}", self.stderr);
        self.json()
    }

    /// The one JSONL audit record named `msg` from stderr (C5-10, M5-81).
    ///
    /// The offline subcommands install no tracing subscriber — an operator runs them on a host
    /// that may have no cluster at all — so their audit records go to stderr as JSONL, which
    /// leaves stdout free for the one result line a script parses.
    fn event(&self, msg: &str) -> serde_json::Value {
        let found: Vec<serde_json::Value> = self
            .stderr
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line.trim()).ok())
            .filter(|v| v["msg"] == msg)
            .collect();
        assert_eq!(
            found.len(),
            1,
            "expected exactly one {msg:?} record, got {found:?} from stderr {:?}",
            self.stderr
        );
        found.into_iter().next().expect("checked above")
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

/// A single-node cluster that formed, took writes, and stopped: a data directory a backup may
/// legitimately be taken from, plus the key material every row needs.
struct Fixture {
    harness: support::Harness,
    /// `--data-dir` of the stopped node, as a string, because every row passes it to a process.
    data_dir: String,
    /// The revision the last write reached, which the manifest must report.
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
                    key: Bytes::from(format!("/m5/backup/{i}")),
                    value: Bytes::from(format!("v{i}")),
                    ..PutRequest::default()
                })
                .await
                .unwrap_or_else(|e| panic!("put {i}: {e}"));
            assert_eq!(response.outcome, MutationOutcome::Applied);
            revision = response.revision;
        }
        drop(client);

        // Stopped before anything is exported. `export_snapshot` opens RocksDB read-only and
        // would happily run against a live directory, but it would then export only what had
        // been flushed — a silently partial backup, which is the one outcome ADR-0024 refuses
        // to normalise.
        let status = node.stop_gracefully(deadline(10)).await;
        assert_eq!(status.code(), Some(0), "the fixture node shut down cleanly");

        let keys = harness.root().join("keys");
        std::fs::create_dir_all(&keys).expect("create the key directory");
        // Raw 32-byte key material, which is the on-disk format the binary documents. Fixed
        // bytes rather than random ones so a failing row replays exactly.
        std::fs::write(keys.join("sign.key"), [3u8; 32]).expect("write the signing seed");
        std::fs::write(keys.join("other.key"), [4u8; 32]).expect("write a second seed");
        std::fs::write(keys.join("aes.key"), [5u8; 32]).expect("write the AES key");
        std::fs::write(keys.join("aes-wrong.key"), [6u8; 32]).expect("write a second AES key");
        write_public(&keys, "sign.key", "trust.pub");
        write_public(&keys, "other.key", "other.pub");

        let data_dir = harness.nodes[0].data_dir.display().to_string();
        Self {
            harness,
            data_dir,
            revision,
            keys,
        }
    }

    /// A path to key material, as a string, ready to pass to a subprocess.
    fn key(&self, name: &str) -> String {
        self.keys.join(name).display().to_string()
    }

    /// A path under the harness root, as a string.
    fn path(&self, name: &str) -> String {
        self.harness.root().join(name).display().to_string()
    }

    /// `backup --name b1` into `out`, asserted to succeed, as the whole run.
    fn backup_run(&self, out: &str, extra: &[&str]) -> CliRun {
        let signing = self.key("sign.key");
        let mut args = vec![
            "backup",
            "--data-dir",
            &self.data_dir,
            "--out",
            out,
            "--name",
            "b1",
            "--signing-key",
            &signing,
        ];
        args.extend_from_slice(extra);
        let run = run_cli(&args);
        run.assert_ok();
        run
    }

    /// The stdout half of [`Fixture::backup_run`], which is all most rows want.
    fn backup(&self, out: &str, extra: &[&str]) -> serde_json::Value {
        self.backup_run(out, extra).json()
    }

    /// `verify-backup --from <dir> --name b1 --trust-key trust.pub`, plus `extra`.
    fn verify(&self, from: &str, extra: &[&str]) -> CliRun {
        let trust = self.key("trust.pub");
        let mut args = vec![
            "verify-backup",
            "--from",
            from,
            "--name",
            "b1",
            "--trust-key",
            &trust,
        ];
        args.extend_from_slice(extra);
        run_cli(&args)
    }

    /// A bootstrap manifest for the cluster a restore is about to mint, signed by the same
    /// authority the harness uses for `--form`.
    ///
    /// Restore runs the identical `--form` verification, so this has to be a real signed
    /// triple; M5-86's "operator supplied the *source* cluster's manifest" case is the same
    /// code path with a different cluster id in the document.
    fn new_manifest(&self, dir: &str, cluster_hex: &str, epoch: u32) -> ManifestPaths {
        let cluster: ClusterId = cluster_hex.parse().expect("a well formed cluster id");
        let document = Manifest::new(cluster)
            .with_epoch(config_core::RecoveryEpoch(epoch))
            // The endpoints are never dialled here: `verify_document` checks the signature,
            // the expiry and agreement with the identity being minted, and `check_endpoints`
            // — the part that would care — runs at `--form`, not at restore.
            .with_voter(Voter::new(
                config_core::NodeId(1),
                self.harness.nodes[0].peer.to_string(),
                self.harness.nodes[0].client.to_string(),
            ));
        self.harness
            .manifest_fixture
            .write(Path::new(dir), &document)
    }

    /// `restore` with the conventional arguments, overridable through `extra`.
    ///
    /// `--manifest-sig` and `--manifest-key` are always passed: the testkit writes
    /// `manifest.sig` / `manifest.pub`, while the CLI's defaults are `<manifest>.sig` /
    /// `<manifest>.pub` — i.e. `manifest.toml.sig`. Passing them explicitly is the documented
    /// deviation working as intended rather than an accident being papered over.
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

/// [`ManifestPaths`] rendered as strings, because every use is a process argument.
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

/// Write the Ed25519 public key of a 32-byte seed file next to it.
fn write_public(dir: &Path, seed: &str, out: &str) {
    let bytes: [u8; 32] = std::fs::read(dir.join(seed))
        .expect("read the seed")
        .try_into()
        .expect("the seed is 32 bytes");
    let signing = ed25519_dalek::SigningKey::from_bytes(&bytes);
    std::fs::write(dir.join(out), signing.verifying_key().to_bytes()).expect("write the key");
}

/// The three members of the triple named `b1` in `dir`.
fn triple(dir: &str) -> (PathBuf, PathBuf, PathBuf) {
    let dir = Path::new(dir);
    (
        dir.join("b1.snap"),
        dir.join("b1.manifest.json"),
        dir.join("b1.manifest.sig"),
    )
}

/// Whether a directory is absent or holds nothing — what every refusal row asserts about the
/// destination it was pointed at.
fn is_empty_dir(path: &str) -> bool {
    match std::fs::read_dir(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => panic!("cannot inspect {path}: {e}"),
        Ok(mut entries) => entries.next().is_none(),
    }
}

/// Flip one byte of a file, in place, leaving its length unchanged.
fn corrupt(path: &Path) {
    let mut bytes = std::fs::read(path).expect("read the file about to be corrupted");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(path, bytes).expect("write the corrupted file");
}

// -------------------------------------------------------------------------------------------
// M5-75
// -------------------------------------------------------------------------------------------

/// `backup` writes the signed triple, and the manifest describes what was actually exported.
#[retcd_test]
async fn m5_75_backup_writes_the_signed_triple() {
    let fixture = Fixture::build("m5_75_backup_writes_the_signed_triple").await;
    let out = fixture.path("backup");
    let run = fixture.backup_run(&out, &[]);
    let reported = run.json();

    // C5-10: producing an artifact is an event, whichever route produced it. Without this
    // record the CLI path wrote a signed copy of the whole state machine and left no trace.
    let created = run.event("backup_created");
    assert_eq!(created["name"], "b1");
    assert_eq!(created["cluster_id"], support::CLUSTER_HEX);
    assert_eq!(created["revision"], fixture.revision);
    assert_eq!(created["encrypted"], false);
    assert_eq!(created["source"], "cli");

    let (snap, manifest_path, sig) = triple(&out);
    assert!(snap.is_file(), "no snapshot at {}", snap.display());
    assert!(manifest_path.is_file(), "no manifest");
    assert!(sig.is_file(), "no detached signature");
    assert_eq!(
        std::fs::read(&sig).expect("read the signature").len(),
        64,
        "an Ed25519 detached signature is 64 raw bytes"
    );

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).expect("read the manifest"))
            .expect("the manifest is JSON");
    assert_eq!(manifest["format"], config_storage::FORMAT_VERSION);
    assert_eq!(manifest["cluster_id"], support::CLUSTER_HEX);
    assert_eq!(manifest["recovery_epoch"], 0);
    assert_eq!(manifest["node_id"], 1);
    assert_eq!(
        manifest["revision"], fixture.revision,
        "the manifest must name the revision the cluster actually reached"
    );
    assert_eq!(manifest["encrypted"], false);
    assert!(
        manifest["last_applied"]["index"].as_u64().unwrap_or(0) > 0,
        "a cluster that served {KEYS} writes has a last_applied: {manifest}"
    );
    assert!(
        manifest["counts"]["kv"].as_u64().unwrap_or(0) >= KEYS as u64,
        "counts must cover the keys written: {manifest}"
    );

    // ADR-0024's central claim: `sha256` is over the plaintext, and for an unencrypted backup
    // the plaintext *is* the file on disk.
    let digest = hex::encode(Sha256::digest(
        std::fs::read(&snap).expect("read the snapshot"),
    ));
    assert_eq!(manifest["sha256"], digest);
    assert_eq!(reported["sha256"], digest);
    assert_eq!(reported["revision"], fixture.revision);

    // The manifest is the operator's whole view of the artifact, so it must not be a place a
    // grant or a principal can leak to (dispatch rule, spec §14).
    let text = String::from_utf8(std::fs::read(&manifest_path).expect("read it again"))
        .expect("the manifest is UTF-8");
    for forbidden in ["grant", "principal", "policy_hash", PRINCIPAL] {
        assert!(
            !text.contains(forbidden),
            "the manifest must not carry {forbidden:?}: {text}"
        );
    }

    fixture.verify(&out, &[]).assert_ok();
}

// -------------------------------------------------------------------------------------------
// M5-77
// -------------------------------------------------------------------------------------------

/// A triple signed by one authority does not verify under another's key.
#[retcd_test]
async fn m5_77_verify_refuses_a_foreign_trust_key() {
    let fixture = Fixture::build("m5_77_verify_refuses_a_foreign_trust_key").await;
    let out = fixture.path("backup");
    fixture.backup(&out, &[]);

    let other = fixture.key("other.pub");
    run_cli(&[
        "verify-backup",
        "--from",
        &out,
        "--name",
        "b1",
        "--trust-key",
        &other,
    ])
    .assert_refused(2, "signature_invalid");

    // And with no key at all there is nothing to verify against, which is a refusal rather
    // than a weaker check: there is no implicit trust store.
    run_cli(&["verify-backup", "--from", &out, "--name", "b1"])
        .assert_refused(2, "key_unavailable");
}

// -------------------------------------------------------------------------------------------
// M5-78
// -------------------------------------------------------------------------------------------

/// A modified `.snap` is caught by the checksum even though the manifest still verifies.
#[retcd_test]
async fn m5_78_verify_refuses_a_modified_snapshot() {
    let fixture = Fixture::build("m5_78_verify_refuses_a_modified_snapshot").await;
    let out = fixture.path("backup");
    fixture.backup(&out, &[]);
    fixture.verify(&out, &[]).assert_ok();

    let (snap, _, _) = triple(&out);
    corrupt(&snap);

    // `checksum_mismatch`, not `signature_invalid`: the signature covers the manifest, and the
    // manifest is untouched. The two failures are different operator situations — a forged
    // manifest versus a damaged transfer — and collapsing them would hide that.
    fixture
        .verify(&out, &[])
        .assert_refused(2, "checksum_mismatch");
}

// -------------------------------------------------------------------------------------------
// M5-79
// -------------------------------------------------------------------------------------------

/// An encrypted backup round-trips under its key, and refuses every other one.
#[retcd_test]
async fn m5_79_encrypted_backup_round_trip() {
    let fixture = Fixture::build("m5_79_encrypted_backup_round_trip").await;
    let out = fixture.path("backup");
    let aes = fixture.key("aes.key");
    let wrong = fixture.key("aes-wrong.key");
    let reported = fixture.backup(&out, &["--encryption-key", &aes]);
    assert_eq!(reported["encrypted"], true);

    let (snap, _, _) = triple(&out);
    let on_disk = std::fs::read(&snap).expect("read the ciphertext");
    let digest = hex::encode(Sha256::digest(&on_disk));
    assert_ne!(
        reported["sha256"], digest,
        "sha256 is over the plaintext, so it must not match the ciphertext on disk"
    );

    let verified = fixture
        .verify(&out, &["--encryption-key", &aes])
        .assert_ok();
    assert_eq!(verified["encrypted"], true);
    assert_eq!(verified["checksum_checked"], true);

    // C5-11. Without a key the signature still verifies, but the snapshot's own bytes cannot
    // be checked — and that is a refusal, not a quieter success. Exiting 0 here told an
    // unattended nightly script "this backup is good" on the strength of a manifest that says
    // nothing about whether the ciphertext still decrypts to the recorded digest.
    fixture
        .verify(&out, &[])
        .assert_refused(2, "checksum_unverified");

    fixture
        .verify(&out, &["--encryption-key", &wrong])
        .assert_refused(2, "decrypt_failed");

    // The wrong key must not produce a half-written store either. AES-GCM authenticates before
    // it returns anything, so there is no plausible-looking plaintext to write.
    let data_dir = fixture.path("restored-wrong-key");
    let manifest_dir = fixture.path("new-manifest");
    let manifest: ManifestPathStrings = fixture
        .new_manifest(&manifest_dir, NEW_CLUSTER_HEX, NEW_EPOCH)
        .into();
    let trust = fixture.key("trust.pub");
    let epoch = NEW_EPOCH.to_string();
    let mut args =
        fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, &epoch, &manifest, &trust);
    args.extend_from_slice(&["--encryption-key", &wrong]);
    run_cli(&args).assert_refused(2, "decrypt_failed");
    assert!(
        is_empty_dir(&data_dir),
        "a refused restore must leave the destination untouched"
    );

    // And with no key at all, restore refuses rather than writing an unusable store. Restore
    // runs the same verification `verify-backup` does, so it refuses at the same point and for
    // the same stated reason.
    let args = fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, &epoch, &manifest, &trust);
    run_cli(&args).assert_refused(2, "checksum_unverified");
    assert!(is_empty_dir(&data_dir));
}

// -------------------------------------------------------------------------------------------
// M5-80
// -------------------------------------------------------------------------------------------

/// The whole TA-47 exit-code table, in one row.
///
/// Exhaustive on purpose: the codes are the contract with an unattended script, and a table
/// spread over eight rows is a table whose gaps nobody notices. `0`, `2`, `3` and `4` all
/// appear, and no case exits `1` or `101`.
#[retcd_test]
async fn m5_80_exit_codes_match_ta_47() {
    let fixture = Fixture::build("m5_80_exit_codes_match_ta_47").await;
    let good = fixture.path("good");
    fixture.backup(&good, &[]);

    // Each case gets its own copy of the artifact, so one case's damage cannot explain
    // another's result.
    let copy = |name: &str| -> String {
        let dest = fixture.path(name);
        std::fs::create_dir_all(&dest).expect("create the case directory");
        for member in ["b1.snap", "b1.manifest.json", "b1.manifest.sig"] {
            std::fs::copy(Path::new(&good).join(member), Path::new(&dest).join(member))
                .unwrap_or_else(|e| panic!("copy {member}: {e}"));
        }
        dest
    };

    let trust = fixture.key("trust.pub");
    let other = fixture.key("other.pub");
    let mut observed: BTreeMap<&str, (i32, String)> = BTreeMap::new();

    // 0 — a complete, correctly signed, unmodified artifact.
    let run = fixture.verify(&good, &[]);
    observed.insert("verified", (run.code, run.reason()));

    // 2 — the signature does not verify under the supplied key.
    let case = copy("case-sig");
    let run = run_cli(&[
        "verify-backup",
        "--from",
        &case,
        "--name",
        "b1",
        "--trust-key",
        &other,
    ]);
    observed.insert("signature_invalid", (run.code, run.reason()));

    // 2 — the snapshot no longer hashes to what the manifest records.
    let case = copy("case-checksum");
    corrupt(&Path::new(&case).join("b1.snap"));
    let run = fixture.verify(&case, &[]);
    observed.insert("checksum_mismatch", (run.code, run.reason()));

    // 2 — no trust key was supplied at all.
    let case = copy("case-nokey");
    let run = run_cli(&["verify-backup", "--from", &case, "--name", "b1"]);
    observed.insert("key_unavailable", (run.code, run.reason()));

    // 2 — the artifact is encrypted and no encryption key was supplied, so its checksum could
    // not be checked. A separate case from `key_unavailable` because the missing key is a
    // different key and the operator's next action is different (C5-11).
    let sealed = fixture.path("sealed");
    let aes = fixture.key("aes.key");
    fixture.backup(&sealed, &["--encryption-key", &aes]);
    let run = fixture.verify(&sealed, &[]);
    observed.insert("checksum_unverified", (run.code, run.reason()));

    // 2 — a name that is not a file stem. Harmless on a command line, a remote write primitive
    // over the admin plane, so it is refused in the one place both paths share (C5-03).
    let run = run_cli(&[
        "verify-backup",
        "--from",
        &good,
        "--name",
        "../evil",
        "--trust-key",
        &trust,
    ]);
    observed.insert("invalid_name", (run.code, run.reason()));

    // 4 — the manifest is missing. Distinct from every exit 2 because "you pointed me at the
    // wrong directory" and "this artifact is not trustworthy" call for different actions.
    let case = copy("case-no-manifest");
    std::fs::remove_file(Path::new(&case).join("b1.manifest.json")).expect("remove the manifest");
    let run = fixture.verify(&case, &[]);
    observed.insert("no_manifest", (run.code, run.reason()));

    // 4 — the detached signature is missing.
    let case = copy("case-no-sig");
    std::fs::remove_file(Path::new(&case).join("b1.manifest.sig")).expect("remove the signature");
    let run = fixture.verify(&case, &[]);
    observed.insert("no_signature", (run.code, run.reason()));

    // 3 — the source store cannot be opened.
    let absent = fixture.path("not-a-store");
    let signing = fixture.key("sign.key");
    let dest = fixture.path("case-store-out");
    let run = run_cli(&[
        "backup",
        "--data-dir",
        &absent,
        "--out",
        &dest,
        "--name",
        "b1",
        "--signing-key",
        &signing,
    ]);
    observed.insert("store_unavailable", (run.code, run.reason()));

    // 2 — the destination is not empty.
    let occupied = fixture.path("occupied");
    std::fs::create_dir_all(&occupied).expect("create the occupied directory");
    std::fs::write(Path::new(&occupied).join("something"), b"x").expect("occupy it");
    let manifest_dir = fixture.path("m80-manifest");
    let manifest: ManifestPathStrings = fixture
        .new_manifest(&manifest_dir, NEW_CLUSTER_HEX, NEW_EPOCH)
        .into();
    let epoch = NEW_EPOCH.to_string();
    let run = run_cli(&fixture.restore_args(
        &good,
        &occupied,
        NEW_CLUSTER_HEX,
        &epoch,
        &manifest,
        &trust,
    ));
    observed.insert("data_dir_not_empty", (run.code, run.reason()));

    let expected: BTreeMap<&str, (i32, String)> = [
        ("verified", (0, String::new())),
        ("signature_invalid", (2, "signature_invalid".to_string())),
        ("checksum_mismatch", (2, "checksum_mismatch".to_string())),
        ("key_unavailable", (2, "key_unavailable".to_string())),
        (
            "checksum_unverified",
            (2, "checksum_unverified".to_string()),
        ),
        ("invalid_name", (2, "invalid_name".to_string())),
        ("no_manifest", (4, "artifact_incomplete".to_string())),
        ("no_signature", (4, "artifact_incomplete".to_string())),
        ("store_unavailable", (3, "store_unavailable".to_string())),
        ("data_dir_not_empty", (2, "data_dir_not_empty".to_string())),
    ]
    .into_iter()
    .collect();
    assert_eq!(observed, expected, "the TA-47 exit-code table moved");
}

// -------------------------------------------------------------------------------------------
// M5-82, M5-83, M5-84, M5-85
// -------------------------------------------------------------------------------------------

/// Reusing the source cluster id is refused: that is what leaves two writable authorities for
/// one logical service.
#[retcd_test]
async fn m5_82_restore_refuses_the_source_cluster_id() {
    let fixture = Fixture::build("m5_82_restore_refuses_the_source_cluster_id").await;
    let out = fixture.path("backup");
    fixture.backup(&out, &[]);

    let data_dir = fixture.path("restored");
    let manifest_dir = fixture.path("same-cluster-manifest");
    // A correctly signed manifest for the *source* cluster: the operator's mistake here is a
    // plausible one, and it must be refused for the identity, not for the signature.
    let manifest: ManifestPathStrings = fixture
        .new_manifest(&manifest_dir, support::CLUSTER_HEX, NEW_EPOCH)
        .into();
    let trust = fixture.key("trust.pub");
    let epoch = NEW_EPOCH.to_string();

    run_cli(&fixture.restore_args(
        &out,
        &data_dir,
        support::CLUSTER_HEX,
        &epoch,
        &manifest,
        &trust,
    ))
    .assert_refused(2, "cluster_id_reused");
    assert!(
        is_empty_dir(&data_dir),
        "a refused restore must leave the destination untouched"
    );
}

/// A non-empty destination is refused before the artifact is even read.
#[retcd_test]
async fn m5_83_restore_refuses_a_non_empty_destination() {
    let fixture = Fixture::build("m5_83_restore_refuses_a_non_empty_destination").await;
    let out = fixture.path("backup");
    fixture.backup(&out, &[]);

    let data_dir = fixture.path("occupied");
    std::fs::create_dir_all(&data_dir).expect("create the destination");
    std::fs::write(Path::new(&data_dir).join("CURRENT"), b"not yours").expect("occupy it");

    let manifest_dir = fixture.path("new-manifest");
    let manifest: ManifestPathStrings = fixture
        .new_manifest(&manifest_dir, NEW_CLUSTER_HEX, NEW_EPOCH)
        .into();
    let trust = fixture.key("trust.pub");
    let epoch = NEW_EPOCH.to_string();

    run_cli(&fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, &epoch, &manifest, &trust))
        .assert_refused(2, "data_dir_not_empty");

    let survivor = std::fs::read(Path::new(&data_dir).join("CURRENT")).expect("it is still there");
    assert_eq!(
        survivor, b"not yours",
        "the refusal must not have touched what was already there"
    );
}

/// An epoch that does not strictly advance is refused.
#[retcd_test]
async fn m5_84_restore_refuses_an_unadvanced_epoch() {
    let fixture = Fixture::build("m5_84_restore_refuses_an_unadvanced_epoch").await;
    let out = fixture.path("backup");
    fixture.backup(&out, &[]);
    let trust = fixture.key("trust.pub");

    // The fixture formed at epoch 0, so 0 is "equal" and there is no lower value to test:
    // equality is exactly the boundary the refusal is about.
    let data_dir = fixture.path("restored");
    let manifest_dir = fixture.path("epoch-0-manifest");
    let manifest: ManifestPathStrings = fixture
        .new_manifest(&manifest_dir, NEW_CLUSTER_HEX, 0)
        .into();
    run_cli(&fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, "0", &manifest, &trust))
        .assert_refused(2, "epoch_not_advanced");
    assert!(is_empty_dir(&data_dir));
}

/// A damaged artifact is refused, and the destination is still empty afterwards.
#[retcd_test]
async fn m5_85_restore_refuses_a_damaged_artifact() {
    let fixture = Fixture::build("m5_85_restore_refuses_a_damaged_artifact").await;
    let trust = fixture.key("trust.pub");
    let other = fixture.key("other.pub");
    let manifest_dir = fixture.path("new-manifest");
    let manifest: ManifestPathStrings = fixture
        .new_manifest(&manifest_dir, NEW_CLUSTER_HEX, NEW_EPOCH)
        .into();
    let epoch = NEW_EPOCH.to_string();

    // Bad signature: verified under a key that did not sign it.
    let out = fixture.path("backup-sig");
    fixture.backup(&out, &[]);
    let data_dir = fixture.path("restored-sig");
    run_cli(&fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, &epoch, &manifest, &other))
        .assert_refused(2, "signature_invalid");
    assert!(
        is_empty_dir(&data_dir),
        "a refused restore must leave the destination untouched"
    );

    // Bad checksum: the manifest still verifies, the snapshot does not match it.
    let out = fixture.path("backup-checksum");
    fixture.backup(&out, &[]);
    corrupt(&triple(&out).0);
    let data_dir = fixture.path("restored-checksum");
    run_cli(&fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, &epoch, &manifest, &trust))
        .assert_refused(2, "checksum_mismatch");
    assert!(is_empty_dir(&data_dir));
}

// -------------------------------------------------------------------------------------------
// M5-90
// -------------------------------------------------------------------------------------------

/// A successful restore writes the state under the **new** identity, preserves the revision,
/// declares everything below it compacted, keeps no events, and records where it came from.
#[retcd_test]
async fn m5_90_restore_preserves_revision_and_fences_the_identity() {
    let fixture = Fixture::build("m5_90_restore_preserves_revision_and_fences_the_identity").await;
    let out = fixture.path("backup");
    fixture.backup(&out, &[]);

    let data_dir = fixture.path("restored");
    let manifest_dir = fixture.path("new-manifest");
    let manifest: ManifestPathStrings = fixture
        .new_manifest(&manifest_dir, NEW_CLUSTER_HEX, NEW_EPOCH)
        .into();
    let trust = fixture.key("trust.pub");
    let epoch = NEW_EPOCH.to_string();

    let run =
        run_cli(&fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, &epoch, &manifest, &trust));
    let reported = run.assert_ok();

    // M5-81 / C5-10. The only record in which both identities appear: afterwards the store
    // knows only the new one and the artifact knows only the old one, so nothing else can
    // correlate a restored cluster back to what it was restored from.
    let completed = run.event("restore_completed");
    assert_eq!(completed["source_cluster_id"], support::CLUSTER_HEX);
    assert_eq!(completed["source_epoch"], 0);
    assert_eq!(completed["new_cluster_id"], NEW_CLUSTER_HEX);
    assert_eq!(completed["new_epoch"], NEW_EPOCH);
    assert_eq!(completed["node_id"], 1);
    assert_eq!(completed["revision"], fixture.revision);

    assert_eq!(reported["restored"], true);
    assert_eq!(reported["source_cluster_id"], support::CLUSTER_HEX);
    assert_eq!(reported["source_epoch"], 0);
    assert_eq!(reported["new_cluster_id"], NEW_CLUSTER_HEX);
    assert_eq!(reported["new_epoch"], NEW_EPOCH);
    assert_eq!(
        reported["revision"], fixture.revision,
        "the restored store continues the source's revision rather than restarting it"
    );

    // Open the restored directory as the new identity. Binding to the *source* identity would
    // be refused by the store itself, which is the structural half of spec §19.11: the peer
    // plane can never confuse the two clusters because the directory is not bound to the old
    // one any more.
    let identity = ClusterIdentity {
        cluster_id: NEW_CLUSTER_HEX.parse().expect("a well formed cluster id"),
        recovery_epoch: config_core::RecoveryEpoch(NEW_EPOCH),
        node_id: config_core::NodeId(1),
    };
    let store = config_storage::RocksStore::open(
        Path::new(&data_dir),
        identity,
        Limits::default(),
        Arc::new(config_storage::NoFaults),
        tracing::Span::none(),
    )
    .expect("the restored store opens under the new identity");

    let reader = store.reader();
    assert_eq!(reader.cluster_revision(), fixture.revision);
    assert_eq!(
        reader.compact_revision().expect("read compact_revision"),
        fixture.revision,
        "everything at or below the restored revision is compacted, so a watch resuming there \
         is told RevisionCompacted instead of being handed a partial replay"
    );
    let stats = reader.journal_stats().expect("read the journal stats");
    assert_eq!(
        stats.count, 0,
        "restore drops the event journal; it cannot be replayed across a recovery boundary"
    );

    let from = reader
        .restored_from()
        .expect("a restored store records where it came from");
    assert_eq!(from.cluster_id.to_string(), support::CLUSTER_HEX);
    assert_eq!(from.recovery_epoch, 0);
    assert_eq!(from.revision, fixture.revision);

    // OQ-45: a restored store has data but no Raft position, which is what lets `--form` treat
    // it as the genesis member of the new cluster. It is `restored_from` that distinguishes
    // this from a half-wiped directory — and that marker cannot be forged by wiping, because
    // wiping removes it too.
    assert!(
        store.is_fresh(),
        "a restored store must be fresh-for-formation"
    );
    assert!(reader.last_applied().is_none(), "no Raft position survives");
    assert_eq!(store.identity(), identity);

    // The keys are actually there.
    let mut found = 0;
    reader.with_state(&mut |state| {
        found = (0..KEYS)
            .filter(|i| state.get(&Bytes::from(format!("/m5/backup/{i}"))).is_some())
            .count();
    });
    assert_eq!(found, KEYS, "every written key survived the restore");
}

// -------------------------------------------------------------------------------------------
// M5-76 — the admin-plane Backup RPC
// -------------------------------------------------------------------------------------------

/// A node that is still **running**, with an admin allowlist and a backup signing key.
///
/// Separate from [`Fixture`], which stops its node before exporting: these rows are about what
/// the online path does to a node that has to keep serving afterwards, so stopping it first
/// would remove the only thing under test.
struct LiveFixture {
    harness: support::Harness,
    node: Option<support::DaemonProcess>,
    admin: config_client::AdminClient,
    data_dir: PathBuf,
    keys: PathBuf,
}

impl LiveFixture {
    async fn build(method: &'static str) -> Self {
        let harness = support::Harness::with_nodes(method, &[1]).await;
        let keys = harness.root().join("keys");
        std::fs::create_dir_all(&keys).expect("create the key directory");
        std::fs::write(keys.join("sign.key"), [3u8; 32]).expect("write the signing seed");
        write_public(&keys, "sign.key", "trust.pub");

        // The admin plane is authorized separately from the keyspace (ADR-0023), so the
        // principal has to be named in `[authz] admins` as well as in the policy document.
        let options = support::NodeOptions {
            admins: vec![PRINCIPAL.to_string()],
            backup_signing_key: Some(keys.join("sign.key")),
            ..harness.node_options()
        };
        harness.write_node_files(&harness.nodes[0], &options);
        let node = harness.start(0, true);

        let opts = GrpcClientOptions {
            request_deadline: deadline(20),
            tls: TlsMode::MutualTls(harness.tls.client_mtls(PRINCIPAL)),
            ..GrpcClientOptions::default()
        };
        let client = GrpcClient::connect(vec![node.client_endpoint().to_string()], opts)
            .expect("the client plane endpoint is well formed")
            .with_cluster_id(harness.cluster_id);

        // Real entries, so the snapshot the backup triggers has something in it.
        for i in 0..KEYS {
            client
                .put(PutRequest {
                    key: Bytes::from(format!("/m5/live/{i}")),
                    value: Bytes::from(format!("v{i}")),
                    ..PutRequest::default()
                })
                .await
                .unwrap_or_else(|e| panic!("put {i}: {e}"));
        }

        let data_dir = harness.nodes[0].data_dir.clone();
        Self {
            harness,
            node: Some(node),
            admin: config_client::AdminClient::new(client),
            data_dir,
            keys,
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        let path = self.harness.root().join(name);
        std::fs::create_dir_all(&path).expect("create the directory");
        path
    }

    /// Stop the node, then reopen its store offline. Reading `state_meta/current_snapshot`
    /// means holding the directory lock the daemon holds, so the node has to be down first.
    async fn stop_and_open(&mut self) -> config_storage::RocksStore {
        let mut node = self.node.take().expect("the node is still running");
        let status = node.stop_gracefully(deadline(10)).await;
        assert_eq!(status.code(), Some(0), "the node shut down cleanly");
        let identity = ClusterIdentity {
            cluster_id: self.harness.cluster_id,
            recovery_epoch: config_core::RecoveryEpoch(0),
            node_id: config_core::NodeId(1),
        };
        config_storage::RocksStore::open(
            &self.data_dir,
            identity,
            Limits::default(),
            Arc::new(config_storage::NoFaults),
            tracing::Span::none(),
        )
        .expect("the node's own store reopens after a backup")
    }
}

/// The `Backup` RPC produces a verifiable triple **and leaves the node's published snapshot
/// alone** (M5-76).
///
/// The second half is the one that matters. `finish_artifact` used to remove the plaintext it
/// was handed, and the online path handed it the node's live `<id>.snap` — so a successful
/// backup unlinked the file `state_meta/current_snapshot` still names, and the next
/// `InstallSnapshot` to a lagging follower failed with "snapshot not found". Nothing in the
/// artifact shows that; only the source node does.
#[retcd_test]
async fn m5_76_backup_rpc_leaves_the_published_snapshot_intact() {
    let mut fixture =
        LiveFixture::build("m5_76_backup_rpc_leaves_the_published_snapshot_intact").await;
    let out = fixture.path("rpc-backup");

    let info = fixture
        .admin
        .backup(out.display().to_string(), Some("live".to_string()))
        .await
        .expect("the admin plane serves Backup");
    assert_eq!(info.name, "live");
    assert_eq!(info.snapshot_file, "live.snap");
    assert!(!info.encrypted);
    for member in [
        &info.snapshot_file,
        &info.manifest_file,
        &info.signature_file,
    ] {
        assert!(
            out.join(member).is_file(),
            "the RPC must write {member} into dest_dir"
        );
    }
    // The scratch copy the online path stages is part of the export, not of the artifact.
    assert!(
        !out.join("live.snap.tmp").exists(),
        "the staged copy must not survive the call"
    );

    let store = fixture.stop_and_open().await;
    let meta = store
        .snapshot_meta()
        .expect("the node published a snapshot when Backup triggered one");
    let published = config_storage::snapshot::snap_path(store.path(), &meta.snapshot_id);
    assert!(
        published.is_file(),
        "state_meta/current_snapshot names {}, which the backup deleted",
        published.display()
    );
    drop(store);

    // And the artifact itself is a real one: the same verification an operator runs.
    let trust = fixture.keys.join("trust.pub").display().to_string();
    let verified = run_cli(&[
        "verify-backup",
        "--from",
        &out.display().to_string(),
        "--name",
        "live",
        "--trust-key",
        &trust,
    ])
    .assert_ok();
    assert_eq!(verified["verified"], true);
    assert_eq!(verified["sha256"], info.sha256);
}

/// Both `Backup` inputs come off the network, so both are checked before anything is built
/// (C5-03).
///
/// `../evil` is the case that matters: `name` is a file *stem*, and a server that joined an
/// unchecked one onto `dest_dir` would write wherever a caller pointed it.
#[retcd_test]
async fn m5_76b_backup_rpc_validates_its_network_inputs() {
    let fixture = LiveFixture::build("m5_76b_backup_rpc_validates_its_network_inputs").await;
    let out = fixture.path("rpc-backup");

    let refusal = fixture
        .admin
        .backup(out.display().to_string(), Some("../evil".to_string()))
        .await
        .expect_err("a traversing name must be refused");
    let text = refusal.to_string();
    assert!(
        text.contains("invalid_name"),
        "the refusal must carry the stable reason, got {text:?}"
    );
    let escaped = out
        .parent()
        .expect("dest_dir has a parent")
        .join("evil.snap");
    assert!(!escaped.exists(), "nothing may be written outside dest_dir");

    // A destination that is not a directory is refused rather than created: `dest_dir` arrives
    // over the network too, and creating one on demand turns Backup into a remote mkdir with
    // an allowlist in front of it.
    let absent = fixture.harness.root().join("no-such-dir");
    let refusal = fixture
        .admin
        .backup(absent.display().to_string(), Some("b".to_string()))
        .await
        .expect_err("an absent destination must be refused");
    assert!(
        refusal.to_string().contains("dest_dir_not_a_directory"),
        "got {refusal}"
    );
    assert!(!absent.exists(), "a refused Backup must create nothing");
}

/// A manifest that gives the node being minted `role = "learner"` is refused (M5-86b,
/// ADR-0023).
///
/// Restore is formation by another name: the restored directory holds the whole cluster and
/// the node that opens it writes the first membership. A learner cannot do that, so the
/// document that mints it has to call it a voter — and a mismatch here is an operator pointing
/// a restore at the wrong node's manifest, which is worth exit 2 rather than a cluster whose
/// membership disagrees with the document that created it.
#[retcd_test]
async fn m5_86b_restore_refuses_a_manifest_that_makes_this_node_a_learner() {
    let fixture =
        Fixture::build("m5_86b_restore_refuses_a_manifest_that_makes_this_node_a_learner").await;
    let out = fixture.path("backup");
    fixture.backup(&out, &[]);

    let data_dir = fixture.path("restored-learner");
    let manifest_dir = fixture.path("learner-manifest");
    let cluster: ClusterId = NEW_CLUSTER_HEX.parse().expect("a well formed cluster id");
    // Node 2 is a voter, so the refusal cannot be "this manifest names no voters": the only
    // thing wrong with the document is what it says about node 1, the node being restored.
    // Neither entry's endpoints are ever dialled at restore — `check_endpoints` runs at
    // `--form` — so they are the fixture node's, rather than invented addresses.
    let peer = fixture.harness.nodes[0].peer.to_string();
    let client = fixture.harness.nodes[0].client.to_string();
    let document = Manifest::new(cluster)
        .with_epoch(config_core::RecoveryEpoch(NEW_EPOCH))
        .with_voter(Voter::new(config_core::NodeId(1), peer.as_str(), client.as_str()).as_learner())
        .with_voter(Voter::new(
            config_core::NodeId(2),
            peer.as_str(),
            client.as_str(),
        ));
    let manifest: ManifestPathStrings = fixture
        .harness
        .manifest_fixture
        .write(Path::new(&manifest_dir), &document)
        .into();

    let trust = fixture.key("trust.pub");
    let epoch = NEW_EPOCH.to_string();
    run_cli(&fixture.restore_args(&out, &data_dir, NEW_CLUSTER_HEX, &epoch, &manifest, &trust))
        .assert_refused(2, "manifest_rejected");
    assert!(
        !Path::new(&data_dir).join("CURRENT").exists(),
        "a refused restore must leave no store behind"
    );
}
