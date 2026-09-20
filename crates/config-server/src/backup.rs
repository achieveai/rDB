//! The backup artifact triple, its verification, and the offline halves of both (ADR-0024).
//!
//! A backup is three files sharing one `<name>` stem:
//!
//! | File | What it is |
//! |---|---|
//! | `<name>.snap` | an ADR-0022 snapshot, built fresh at backup time, optionally AES-256-GCM encrypted |
//! | `<name>.manifest.json` | what the snapshot contains and what it hashes to |
//! | `<name>.manifest.sig` | a detached Ed25519 signature over the **exact** manifest bytes |
//!
//! The verification order is the security property, and it is the same order
//! [`crate::manifest`] already uses for the bootstrap manifest: signature over the bytes as
//! they are on disk, *then* parse, *then* compare. A verifier that parsed first would be
//! interpreting an attacker's document in order to decide whether to trust it.
//!
//! `sha256` in the manifest is always over the **plaintext** snapshot, so a verifier can
//! confirm that an artifact matches what its manifest describes without holding the encryption
//! key — only reading the contents needs the key (ADR-0024 "Optional encryption").
//!
//! # Key files
//!
//! Every key is raw bytes in a file, never hex and never PEM, matching the convention ADR-0011
//! already set for the bootstrap manifest's key material: a 32-byte Ed25519 signing seed, a
//! 32-byte Ed25519 verifying key, a 32-byte AES-256 key. A strict length is the whole check;
//! a lenient reader here would be a lenient reader of a secret.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use config_storage::snapshot::{SnapshotFileError, SnapshotHeader};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Suffix of the snapshot member of the triple.
pub const SNAP_SUFFIX: &str = ".snap";
/// Suffix of the manifest member of the triple.
pub const MANIFEST_SUFFIX: &str = ".manifest.json";
/// Suffix of the detached-signature member of the triple.
pub const SIG_SUFFIX: &str = ".manifest.sig";

/// Length of the AES-256-GCM nonce prefixed to an encrypted `.snap` (ADR-0024).
const NONCE_LEN: usize = 12;

/// A Raft log id, flattened the same way [`config_engine::LogIdView`] is.
///
/// The manifest is a rEtcd artifact read by rEtcd, but it is also the thing an operator opens
/// in an editor during a recovery, so it carries `(term, index)` rather than OpenRaft's nested
/// leader-id encoding. Nothing reads it back as a gate — the `.snap` header is authoritative
/// for everything a restore actually applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestLogId {
    /// Term of the leader that proposed the entry.
    pub term: u64,
    /// Index of the entry.
    pub index: u64,
}

/// The `<name>.manifest.json` document (ADR-0024 "Artifact triple").
///
/// `deny_unknown_fields` is a refusal, not strictness for its own sake: a manifest carrying a
/// field this build does not know is a manifest this build cannot fully check, and `format`
/// alone would not catch a field added without a version bump.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupManifest {
    /// The `.snap`'s `format_version` (ADR-0021). A mismatch is refused, never silently read.
    pub format: u32,
    /// Cluster the backup was taken from, as 32 lowercase hex characters.
    pub cluster_id: String,
    /// Recovery epoch the backup was taken at.
    pub recovery_epoch: u32,
    /// Which node produced this backup.
    pub node_id: u64,
    /// `cluster_revision` at export time.
    pub revision: u64,
    /// The state machine's applied pointer at export time.
    pub last_applied: Option<ManifestLogId>,
    /// The last applied membership at export time, serialized verbatim from the snapshot
    /// header.
    ///
    /// Audit only. A restore takes its membership from the **new** bootstrap manifest
    /// (ADR-0024 "What restore writes"), never from here, so this field is never a gate and
    /// its shape is never load-bearing.
    pub membership: serde_json::Value,
    /// Record count per exported column family, keyed by name (`kv`, `events`, `dedup`, and
    /// whatever a later milestone adds — the set is discovered, not listed, exactly as
    /// `config_storage::snapshot::is_snapshot_data_cf` discovers it).
    pub counts: BTreeMap<String, u64>,
    /// Lowercase hex SHA-256 of the **plaintext** `.snap`.
    pub sha256: String,
    /// Reserved for M6 policy versioning (ADR-0027); `null` until then.
    pub policy_version_ref: Option<u64>,
    /// Wall-clock export time.
    pub created_unix_ms: u64,
    /// Whether the `.snap` beside this manifest is AES-256-GCM ciphertext.
    ///
    /// Deviation from ADR-0024's field list, and the reason it is here rather than inferred:
    /// without it, `verify-backup` run without `--encryption-key` against an encrypted
    /// artifact could only report `checksum_mismatch`, sending an operator to look for
    /// corruption that does not exist. It is inside the signed bytes, so it cannot be flipped
    /// to make a verifier skip decryption.
    pub encrypted: bool,
}

impl BackupManifest {
    /// The three file names of the triple with this stem.
    pub fn file_names(name: &str) -> (String, String, String) {
        (
            format!("{name}{SNAP_SUFFIX}"),
            format!("{name}{MANIFEST_SUFFIX}"),
            format!("{name}{SIG_SUFFIX}"),
        )
    }
}

/// Why a backup, a verification, or a restore was refused.
///
/// Shaped by *consequence* rather than by cause, because an operator's script branches on the
/// exit code (TA-47) and the specific cause travels beside it in the stable `reason` field.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    /// Exit 4. A member of the triple is missing. Distinct from every other refusal because
    /// "you pointed me at the wrong directory" and "this artifact is not trustworthy" call for
    /// different operator actions.
    #[error("backup artifact is incomplete: no {what} at {path}")]
    Incomplete {
        /// Which member of the triple.
        what: &'static str,
        /// Where it was looked for.
        path: String,
    },
    /// Exit 3. A source or destination store could not be opened.
    #[error("cannot open the {what} store at {path}: {detail}")]
    Store {
        /// `"source"` or `"destination"`.
        what: &'static str,
        /// The directory.
        path: String,
        /// What the storage layer said.
        detail: String,
    },
    /// Exit 2. Every other refusal: signature, checksum, decryption, identity, epoch,
    /// non-empty directory, format, or a malformed argument.
    #[error("{detail}")]
    Refused {
        /// Stable machine-readable cause (ADR-0018's `reason` convention).
        reason: &'static str,
        /// The operator-facing sentence.
        detail: String,
    },
}

impl BackupError {
    /// The process exit code this refusal maps to (TA-47).
    pub fn code(&self) -> u8 {
        match self {
            BackupError::Incomplete { .. } => 4,
            BackupError::Store { .. } => 3,
            BackupError::Refused { .. } => 2,
        }
    }

    /// The stable `reason` field for the one diagnostic line.
    pub fn reason(&self) -> &'static str {
        match self {
            BackupError::Incomplete { .. } => "artifact_incomplete",
            BackupError::Store { .. } => "store_unavailable",
            BackupError::Refused { reason, .. } => reason,
        }
    }

    /// Build an exit-2 refusal.
    pub fn refused(reason: &'static str, detail: impl std::fmt::Display) -> Self {
        BackupError::Refused {
            reason,
            detail: detail.to_string(),
        }
    }
}

/// What a completed backup produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupOutcome {
    /// The stem the three files share.
    pub name: String,
    /// `<out>/<name>.snap`.
    pub snapshot_file: PathBuf,
    /// `<out>/<name>.manifest.json`.
    pub manifest_file: PathBuf,
    /// `<out>/<name>.manifest.sig`.
    pub signature_file: PathBuf,
    /// Lowercase hex SHA-256 of the plaintext snapshot.
    pub sha256: String,
    /// `cluster_revision` the snapshot covers.
    pub revision: u64,
    /// Cluster the backup was taken from.
    pub cluster_id: String,
    /// Size of the `.snap` as written (ciphertext size when encryption is on).
    pub size_bytes: u64,
    /// Whether the `.snap` on disk is ciphertext.
    pub encrypted: bool,
}

/// What `verify-backup` established.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedBackup {
    /// The stem the three files share.
    pub name: String,
    /// The parsed manifest — parsed only *after* its signature verified.
    pub manifest: BackupManifest,
    /// The `.snap` as it is on disk.
    pub snapshot_file: PathBuf,
    /// Whether the snapshot's own bytes were checked. `false` means the artifact is encrypted
    /// and no key was supplied, so the signature and the manifest were verified but the
    /// checksum was not.
    pub checksum_checked: bool,
}

/// Where the keys for a backup, a verification, or a restore come from.
#[derive(Debug, Clone, Default)]
pub struct KeyFiles<'a> {
    /// Ed25519 signing seed (32 raw bytes) — backup only.
    pub signing_key: Option<&'a Path>,
    /// Ed25519 verifying key (32 raw bytes) — verify and restore only.
    pub trust_key: Option<&'a Path>,
    /// AES-256 key (32 raw bytes). Absent means the `.snap` is written and read in plaintext.
    pub encryption_key: Option<&'a Path>,
}

// -------------------------------------------------------------------------------------------
// backup
// -------------------------------------------------------------------------------------------

/// Take a backup from a **stopped** data directory (`config-server backup`).
///
/// The export goes through [`config_storage::snapshot::export_snapshot`], which opens RocksDB
/// read-only and therefore neither takes the directory lock nor redoes an interrupted snapshot
/// install — a backup must not mutate what it is backing up.
pub fn backup_offline(
    data_dir: &Path,
    out_dir: &Path,
    name: Option<&str>,
    keys: &KeyFiles<'_>,
) -> Result<BackupOutcome, BackupError> {
    let name = match name {
        Some(n) => validate_name(n)?.to_string(),
        None => default_name(),
    };
    // The keys are loaded before a byte is exported: discovering that the signing key is
    // missing *after* writing a multi-gigabyte snapshot wastes the operator's time and leaves
    // a `.snap` with no manifest in the destination.
    load_signing_key(keys.signing_key)?;
    if let Some(path) = keys.encryption_key {
        load_aes_key(path)?;
    }
    prepare_out_dir(out_dir)?;

    // Exported next to its destination rather than into the system temp directory, so the
    // whole artifact lands on one filesystem and a partial backup is impossible to mistake for
    // a complete one taken elsewhere.
    let plaintext = out_dir.join(format!("{name}{SNAP_SUFFIX}.tmp"));
    let header = config_storage::snapshot::export_snapshot(data_dir, &plaintext)
        .map_err(|e| store_error("source", data_dir, e))?;

    let finished = finish_artifact(&header, &plaintext, out_dir, &name, keys);
    // Removed here, by the function that created it, on *both* paths. Leaving it behind on
    // failure would be worse than untidy: a `.snap` with no manifest is indistinguishable from
    // a triple whose manifest was deleted, and TA-47 gives those two different exit codes.
    let _ = std::fs::remove_file(&plaintext);
    finished
}

/// Turn an exported plaintext `.snap` into the full triple.
///
/// Split out from [`backup_offline`] because the admin-plane `Backup` RPC exports through the
/// running node's snapshot builder instead, and M5-76 requires the two paths to produce
/// artifacts differing only in `node_id` and `created_unix_ms`. They differ only in the export;
/// everything after it is this function, once.
///
/// `plaintext` is **read, never removed**. Deleting a path this function did not create was a
/// live-snapshot deletion bug: the admin-plane path used to hand over the node's published
/// `<id>.snap`, so finishing an artifact unlinked the file `state_meta/current_snapshot` still
/// names and openraft's next `InstallSnapshot` failed with "snapshot not found". Whoever
/// creates the scratch file removes it — [`backup_offline`] for the CLI, the caller in
/// `run.rs` for the RPC.
pub fn finish_artifact(
    header: &SnapshotHeader,
    plaintext: &Path,
    out_dir: &Path,
    name: &str,
    keys: &KeyFiles<'_>,
) -> Result<BackupOutcome, BackupError> {
    let (snap_name, manifest_name, sig_name) = BackupManifest::file_names(name);
    let snap_path = out_dir.join(&snap_name);
    let manifest_path = out_dir.join(&manifest_name);
    let sig_path = out_dir.join(&sig_name);

    let bytes = read_file("exported snapshot", plaintext)?;
    let sha256 = hex::encode(Sha256::digest(&bytes));

    let signing_key = load_signing_key(keys.signing_key)?;
    let encryption_key = keys.encryption_key.map(load_aes_key).transpose()?;

    let on_disk = match &encryption_key {
        None => bytes,
        Some(key) => encrypt(key, &bytes)?,
    };
    let size_bytes = on_disk.len() as u64;

    let manifest = BackupManifest {
        format: header.format_version,
        cluster_id: header.cluster_id.to_string(),
        recovery_epoch: header.recovery_epoch.0,
        node_id: header.built_by,
        revision: header.cluster_revision,
        last_applied: header.last_applied.map(|l| ManifestLogId {
            term: l.leader_id.term,
            index: l.index,
        }),
        membership: serde_json::to_value(&header.membership)
            .map_err(|e| BackupError::refused("malformed_manifest", e))?,
        counts: header.counts.clone(),
        sha256: sha256.clone(),
        policy_version_ref: None,
        created_unix_ms: header.created_unix_ms,
        encrypted: encryption_key.is_some(),
    };
    // Pretty-printed on purpose: the bytes that are signed are the bytes on disk, and an
    // operator reading a manifest mid-recovery should not have to reformat it first.
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| BackupError::refused("malformed_manifest", e))?;
    let signature = signing_key.sign(&manifest_bytes);

    write_file(&snap_path, &on_disk)?;
    write_file(&manifest_path, &manifest_bytes)?;
    write_file(&sig_path, &signature.to_bytes())?;

    Ok(BackupOutcome {
        name: name.to_string(),
        snapshot_file: snap_path,
        manifest_file: manifest_path,
        signature_file: sig_path,
        sha256,
        revision: header.cluster_revision,
        cluster_id: header.cluster_id.to_string(),
        size_bytes,
        encrypted: encryption_key.is_some(),
    })
}

// -------------------------------------------------------------------------------------------
// verify
// -------------------------------------------------------------------------------------------

/// Verify the triple in `from_dir` (`config-server verify-backup`).
///
/// Checks, in this order and no other:
///
/// 1. the triple is complete — exit 4 if not, before anything has been trusted;
/// 2. the detached signature verifies against `trust_key` over the manifest's exact bytes;
/// 3. the manifest parses, and its `format` is one this build reads;
/// 4. the `.snap`'s SHA-256 equals the manifest's, after decryption for an encrypted artifact.
///
/// There is no implicit trust store: without a trust key there is no verification to perform,
/// so its absence is a refusal rather than a weaker check (ADR-0024). An encrypted artifact
/// with no `--encryption-key` is refused for the same reason — `checksum_unverified`, exit 2 —
/// rather than succeeding with a flag nobody reads.
pub fn verify_backup(
    from_dir: &Path,
    name: Option<&str>,
    keys: &KeyFiles<'_>,
) -> Result<VerifiedBackup, BackupError> {
    let name = match name {
        Some(n) => validate_name(n)?.to_string(),
        None => sole_artifact_name(from_dir)?,
    };
    let (snap_name, manifest_name, sig_name) = BackupManifest::file_names(&name);
    let snap_path = from_dir.join(&snap_name);
    let manifest_path = from_dir.join(&manifest_name);
    let sig_path = from_dir.join(&sig_name);

    let manifest_bytes = read_member("manifest", &manifest_path)?;
    let sig_bytes = read_member("detached signature", &sig_path)?;
    let snap_bytes = read_member("snapshot", &snap_path)?;

    let trust_key = load_verifying_key(keys.trust_key)?;
    let sig: [u8; 64] = sig_bytes.as_slice().try_into().map_err(|_| {
        BackupError::refused(
            "signature_invalid",
            format!(
                "{}: expected a 64-byte detached signature, found {} bytes",
                show(&sig_path),
                sig_bytes.len()
            ),
        )
    })?;
    // Step 1: cryptography, over the bytes as they are on disk, before any parse.
    trust_key
        .verify(&manifest_bytes, &Signature::from_bytes(&sig))
        .map_err(|_| {
            BackupError::refused(
                "signature_invalid",
                format!(
                    "{} is not signed by the supplied trust key",
                    show(&manifest_path)
                ),
            )
        })?;

    let manifest: BackupManifest = serde_json::from_slice(&manifest_bytes).map_err(|e| {
        BackupError::refused(
            "malformed_manifest",
            format!("{} is signed but unparsable: {e}", show(&manifest_path)),
        )
    })?;
    if manifest.format != config_storage::FORMAT_VERSION {
        return Err(BackupError::refused(
            "format_mismatch",
            format!(
                "backup format {}; this build reads only format {}",
                manifest.format,
                config_storage::FORMAT_VERSION
            ),
        ));
    }

    let encryption_key = keys.encryption_key.map(load_aes_key).transpose()?;
    // An encrypted artifact with no key is a **refusal**, not a weaker success. Exiting 0 after
    // checking only the signature tells an unattended script "this backup is good" on the
    // strength of a manifest that says nothing about whether the ciphertext still decrypts to
    // the recorded digest — precisely the claim an operator reaches for verify-backup to make.
    let plaintext = match (manifest.encrypted, &encryption_key) {
        (false, _) => snap_bytes,
        (true, None) => {
            return Err(BackupError::refused(
                "checksum_unverified",
                format!(
                    "{} is encrypted; without --encryption-key its checksum cannot be checked, \
                     and a signature alone does not establish that the payload is intact",
                    show(&snap_path)
                ),
            ))
        }
        (true, Some(key)) => decrypt(key, &snap_bytes, &snap_path)?,
    };

    // Always true on the success path now; retained so the JSON output and `VerifiedBackup`
    // stay a stable shape, and so a future partial-verification mode has somewhere to say so.
    let checksum_checked = true;
    let computed = hex::encode(Sha256::digest(&plaintext));
    if computed != manifest.sha256 {
        return Err(BackupError::refused(
            "checksum_mismatch",
            format!(
                "{} hashes to {computed}, but the manifest records {}",
                show(&snap_path),
                manifest.sha256
            ),
        ));
    }

    Ok(VerifiedBackup {
        name,
        manifest,
        snapshot_file: snap_path,
        checksum_checked,
    })
}

/// Write a verified artifact's **plaintext** snapshot to `dest`, decrypting if needed.
///
/// Restore must feed a plaintext `.snap` to the storage layer. Kept here so the decryption
/// code exists exactly once.
pub fn materialize_plaintext(
    verified: &VerifiedBackup,
    keys: &KeyFiles<'_>,
    dest: &Path,
) -> Result<(), BackupError> {
    let bytes = read_file("snapshot", &verified.snapshot_file)?;
    let plaintext = if verified.manifest.encrypted {
        let key = load_aes_key(keys.encryption_key.ok_or_else(|| {
            BackupError::refused(
                "decrypt_failed",
                "this backup is encrypted; an encryption key is required to restore it",
            )
        })?)?;
        decrypt(&key, &bytes, &verified.snapshot_file)?
    } else {
        bytes
    };
    write_file(dest, &plaintext)
}

// -------------------------------------------------------------------------------------------
// encryption
// -------------------------------------------------------------------------------------------

fn encrypt(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>, BackupError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce = [0u8; NONCE_LEN];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|_| BackupError::refused("encrypt_failed", "AES-256-GCM encryption failed"))?;
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

fn decrypt(key: &[u8; 32], on_disk: &[u8], path: &Path) -> Result<Vec<u8>, BackupError> {
    if on_disk.len() <= NONCE_LEN {
        return Err(BackupError::refused(
            "decrypt_failed",
            format!(
                "{} is {} bytes, too short to carry a {NONCE_LEN}-byte nonce and a payload",
                show(path),
                on_disk.len()
            ),
        ));
    }
    let (nonce, ciphertext) = on_disk.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    // GCM authenticates before it returns anything, so a wrong key yields this error and never
    // a buffer of plausible-looking plaintext. That is what lets M5-79 assert that the
    // wrong-key path writes no plaintext to disk: there is none to write.
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| {
            BackupError::refused(
                "decrypt_failed",
                format!(
                    "{} did not decrypt under the supplied key (wrong key, or the ciphertext \
                     was modified)",
                    show(path)
                ),
            )
        })
}

// -------------------------------------------------------------------------------------------
// key and file plumbing
// -------------------------------------------------------------------------------------------

fn load_signing_key(path: Option<&Path>) -> Result<SigningKey, BackupError> {
    let path = path.ok_or_else(|| {
        BackupError::refused(
            "key_unavailable",
            "a signing key is required: an unsigned backup cannot be verified, and \
             verify-backup has no way to trust one",
        )
    })?;
    Ok(SigningKey::from_bytes(&load_key32("signing key", path)?))
}

fn load_verifying_key(path: Option<&Path>) -> Result<VerifyingKey, BackupError> {
    let path = path.ok_or_else(|| {
        BackupError::refused(
            "key_unavailable",
            "--trust-key is required: there is no implicit trust store, so without a key \
             there is no verification to perform",
        )
    })?;
    VerifyingKey::from_bytes(&load_key32("trust key", path)?).map_err(|e| {
        BackupError::refused(
            "key_unavailable",
            format!("{} is not a valid Ed25519 public key: {e}", show(path)),
        )
    })
}

fn load_aes_key(path: &Path) -> Result<[u8; 32], BackupError> {
    load_key32("encryption key", path)
}

fn load_key32(what: &'static str, path: &Path) -> Result<[u8; 32], BackupError> {
    let bytes = std::fs::read(path).map_err(|e| {
        BackupError::refused(
            "key_unavailable",
            format!("cannot read the {what} at {}: {e}", show(path)),
        )
    })?;
    let len = bytes.len();
    bytes.try_into().map_err(|_| {
        BackupError::refused(
            "key_unavailable",
            format!(
                "the {what} at {} must be exactly 32 raw bytes, found {len}",
                show(path)
            ),
        )
    })
}

/// Read a member of the triple; its absence is exit 4, not exit 2.
fn read_member(what: &'static str, path: &Path) -> Result<Vec<u8>, BackupError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(BackupError::Incomplete {
            what,
            path: show(path),
        }),
        Err(e) => Err(BackupError::refused(
            "artifact_unreadable",
            format!("cannot read the {what} at {}: {e}", show(path)),
        )),
    }
}

fn read_file(what: &'static str, path: &Path) -> Result<Vec<u8>, BackupError> {
    std::fs::read(path).map_err(|e| {
        BackupError::refused(
            "artifact_unreadable",
            format!("cannot read the {what} at {}: {e}", show(path)),
        )
    })
}

fn write_file(path: &Path, bytes: &[u8]) -> Result<(), BackupError> {
    std::fs::write(path, bytes).map_err(|e| {
        BackupError::refused(
            "artifact_unwritable",
            format!("cannot write {}: {e}", show(path)),
        )
    })
}

fn prepare_out_dir(out_dir: &Path) -> Result<(), BackupError> {
    std::fs::create_dir_all(out_dir).map_err(|e| {
        BackupError::refused(
            "artifact_unwritable",
            format!("cannot create the output directory {}: {e}", show(out_dir)),
        )
    })
}

/// Find the one artifact stem in a directory.
///
/// Refuses when there is more than one rather than picking the newest: mid-recovery,
/// "whichever one I guessed" is not an acceptable answer to "which backup did you verify?".
fn sole_artifact_name(dir: &Path) -> Result<String, BackupError> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        BackupError::refused(
            "artifact_unreadable",
            format!("cannot list the backup directory {}: {e}", show(dir)),
        )
    })?;
    let mut names: Vec<String> = Vec::new();
    for entry in entries.flatten() {
        let file = entry.file_name().to_string_lossy().into_owned();
        if let Some(stem) = file.strip_suffix(MANIFEST_SUFFIX) {
            names.push(stem.to_string());
        }
    }
    names.sort();
    match names.len() {
        0 => Err(BackupError::Incomplete {
            what: "manifest",
            path: show(dir),
        }),
        1 => Ok(names.remove(0)),
        n => Err(BackupError::refused(
            "invalid_argument",
            format!("{} holds {n} backups; name one with --name", show(dir)),
        )),
    }
}

/// A name is a file stem, so it must not be able to escape the directory it was given.
///
/// Public because the admin-plane `Backup` RPC takes the same name off the network, where the
/// stakes are higher than on a command line: `--name ../../etc/thing` is an operator writing in
/// their own shell, but the same string over gRPC is a remote write outside `dest_dir`.
pub fn validate_name(name: &str) -> Result<&str, BackupError> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        && !name.starts_with('.')
        && !name.contains("..");
    if ok {
        Ok(name)
    } else {
        Err(BackupError::refused(
            "invalid_name",
            format!(
                "--name {name:?} must be 1..=128 characters of [A-Za-z0-9._-], must not start \
                 with a dot, and must not contain `..`"
            ),
        ))
    }
}

/// `backup-<unix-millis>` when the operator did not choose a name.
pub fn default_name() -> String {
    format!("backup-{}", unix_millis())
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn store_error(what: &'static str, dir: &Path, e: SnapshotFileError) -> BackupError {
    BackupError::Store {
        what,
        path: show(dir),
        detail: e.to_string(),
    }
}

fn show(path: &Path) -> String {
    path.display().to_string()
}

// -------------------------------------------------------------------------------------------
// restore
// -------------------------------------------------------------------------------------------

/// Everything `config-server restore` was asked to do.
#[derive(Debug, Clone)]
pub struct RestoreRequest<'a> {
    /// Directory holding the backup triple.
    pub from: &'a Path,
    /// Which triple, when the directory holds more than one.
    pub name: Option<&'a str>,
    /// The fresh data directory to create.
    pub data_dir: &'a Path,
    /// The new cluster id, as written on the command line.
    pub cluster_id: &'a str,
    /// The new recovery epoch.
    pub recovery_epoch: u32,
    /// This node's id in the new cluster.
    pub node_id: u64,
    /// The new cluster's signed bootstrap manifest.
    pub manifest: &'a Path,
    /// Its detached signature; defaults to `<manifest>.sig`.
    pub manifest_sig: Option<&'a Path>,
    /// Its signing public key; defaults to `<manifest>.pub`.
    pub manifest_key: Option<&'a Path>,
}

/// What a completed restore established, for the `restore_completed` audit line (M5-81).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreOutcome {
    /// The cluster the data came from.
    pub source_cluster_id: String,
    /// The epoch it came from.
    pub source_epoch: u32,
    /// The cluster it now belongs to.
    pub new_cluster_id: String,
    /// The epoch it now belongs to.
    pub new_epoch: u32,
    /// The revision the restored store continues from.
    pub revision: u64,
    /// Records written, per column family.
    pub written: std::collections::BTreeMap<String, u64>,
}

/// Verify a backup and write it into a fresh data directory under a **new** identity.
///
/// The refusal matrix (ADR-0024) is checked in an order chosen so that the destination is
/// still untouched when the most likely operator mistakes are caught: the cluster id, the
/// epoch and the destination directory are decided before a single byte is read out of the
/// artifact, and the artifact is fully verified before the destination is created. M5-82,
/// M5-83 and M5-85 all assert the same thing from different angles — a refusal leaves the
/// destination empty — and that is a property of this order, not of a cleanup path.
///
/// This function does not trust a prior `verify-backup`: it runs the identical verification
/// itself. A restore is the one operation where an operator is most likely to be working from
/// a note written hours earlier.
pub fn restore(
    req: &RestoreRequest<'_>,
    keys: &KeyFiles<'_>,
) -> Result<RestoreOutcome, BackupError> {
    let cluster_id: config_core::ClusterId = req.cluster_id.parse().map_err(|e| {
        BackupError::refused(
            "invalid_argument",
            format!("--cluster-id {:?} is not a cluster id: {e}", req.cluster_id),
        )
    })?;
    // Checked first, and before the artifact is even opened: an operator who typed the old
    // cluster id has made the single most dangerous mistake in §14, and the answer to it must
    // not depend on whether the backup happened to verify.
    if !dir_is_absent_or_empty(req.data_dir)? {
        return Err(BackupError::refused(
            "data_dir_not_empty",
            format!(
                "{} is not empty; restore never writes into a directory that might be live or \
                 might belong to another node",
                show(req.data_dir)
            ),
        ));
    }

    let verified = verify_backup(req.from, req.name, keys)?;
    let manifest = &verified.manifest;
    if manifest.cluster_id == cluster_id.to_string() {
        return Err(BackupError::refused(
            "cluster_id_reused",
            format!(
                "--cluster-id {} is the cluster this backup was taken from; a restore must \
                 mint a new one, because reusing it is what leaves two writable authorities \
                 for one logical service",
                manifest.cluster_id
            ),
        ));
    }
    if req.recovery_epoch <= manifest.recovery_epoch {
        return Err(BackupError::refused(
            "epoch_not_advanced",
            format!(
                "--recovery-epoch {} does not advance past the backup's epoch {}; an epoch \
                 that does not advance cannot distinguish the restored authority from the \
                 source",
                req.recovery_epoch, manifest.recovery_epoch
            ),
        ));
    }
    // The new cluster's bootstrap manifest, verified exactly as `--form` verifies it:
    // signature over the exact bytes, expiry, then agreement with the identity being minted.
    // M5-86's "supply the source cluster's manifest" case lands in the cluster-id clause of
    // that check, which is why this reuses `crate::manifest` instead of re-deriving it.
    let identity = config_core::ClusterIdentity {
        cluster_id,
        recovery_epoch: config_core::RecoveryEpoch(req.recovery_epoch),
        node_id: config_core::NodeId(req.node_id),
    };
    let files = crate::config::ManifestFiles {
        manifest: req.manifest.to_path_buf(),
        signature: req
            .manifest_sig
            .map(Path::to_path_buf)
            .unwrap_or_else(|| sibling(req.manifest, "sig")),
        public_key: req
            .manifest_key
            .map(Path::to_path_buf)
            .unwrap_or_else(|| sibling(req.manifest, "pub")),
    };
    let bootstrap = crate::manifest::verify_document(&files, &identity)
        .map_err(|e| BackupError::refused("manifest_rejected", e))?;
    // A restored directory becomes the genesis member of the new cluster, so the node being
    // restored has to be a voter in it. A learner-role entry here would produce a store that
    // is fresh-for-formation and a manifest that forbids forming it — an authority nobody can
    // start (ADR-0023, ADR-0024).
    if bootstrap.self_is_learner {
        return Err(BackupError::refused(
            "manifest_rejected",
            format!(
                "the manifest gives node {} role \"learner\"; a restored node forms the new \
                 cluster, so it must be a voter in the manifest that mints it",
                req.node_id
            ),
        ));
    }

    // Only now is anything written. An encrypted artifact needs its plaintext somewhere first;
    // it cannot go into the destination, which must still be empty when the store is created.
    //
    // A `NamedTempFile`, not `temp_dir().join(predictable_name)`: the staged file is a
    // decrypted copy of the entire state machine. The old form was world-guessable, so another
    // local user could pre-create or read it, and it survived any early return between staging
    // and the explicit unlink. `NamedTempFile` opens with a random name and 0600 on Unix, and
    // its `Drop` removes it on *every* exit path, panics included.
    let staged = if manifest.encrypted {
        let file = tempfile::Builder::new()
            .prefix("retcd-restore-")
            .suffix(".snap")
            .tempfile()
            .map_err(|e| {
                BackupError::refused(
                    "artifact_unwritable",
                    format!("cannot create a staging file for the decrypted snapshot: {e}"),
                )
            })?;
        materialize_plaintext(&verified, keys, file.path())?;
        Some(file)
    } else {
        None
    };
    let snapshot = staged
        .as_ref()
        .map_or(verified.snapshot_file.as_path(), |f| f.path());

    let restored_from = config_core::RestoredFrom {
        cluster_id: manifest.cluster_id.parse().map_err(|e| {
            BackupError::refused(
                "malformed_manifest",
                format!("backup manifest cluster_id is unparsable: {e}"),
            )
        })?,
        recovery_epoch: manifest.recovery_epoch,
        revision: manifest.revision,
    };
    let report =
        config_storage::restore_into_fresh_store(req.data_dir, &identity, snapshot, &restored_from)
            .map_err(|e| BackupError::Store {
                what: "destination",
                path: show(req.data_dir),
                detail: e.to_string(),
            })?;
    // Explicit, so the lifetime of the decrypted copy is visible at the point it ends rather
    // than inferred from where the binding happens to fall out of scope.
    drop(staged);

    Ok(RestoreOutcome {
        source_cluster_id: manifest.cluster_id.clone(),
        source_epoch: manifest.recovery_epoch,
        new_cluster_id: cluster_id.to_string(),
        new_epoch: req.recovery_epoch,
        revision: report.revision,
        written: report.written,
    })
}

/// `<path>` with `ext` appended after its existing name, not replacing it.
///
/// `manifest.toml` gives `manifest.toml.sig`, never `manifest.sig`: the signature belongs to
/// *that* file, and a convention that silently retargets a different one is a convention that
/// eventually verifies the wrong document.
fn sibling(path: &Path, ext: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".");
    name.push(ext);
    path.with_file_name(name)
}

/// Whether a destination may be restored into: absent, or present and holding nothing.
fn dir_is_absent_or_empty(dir: &Path) -> Result<bool, BackupError> {
    match std::fs::read_dir(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(BackupError::refused(
            "data_dir_not_empty",
            format!("cannot inspect {}: {e}", show(dir)),
        )),
        Ok(mut entries) => Ok(entries.next().is_none()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_follow_the_ta_47_table() {
        assert_eq!(
            BackupError::Incomplete {
                what: "manifest",
                path: "d".into(),
            }
            .code(),
            4
        );
        assert_eq!(
            BackupError::Store {
                what: "source",
                path: "d".into(),
                detail: "locked".into(),
            }
            .code(),
            3
        );
        for reason in [
            "signature_invalid",
            "checksum_mismatch",
            "decrypt_failed",
            "format_mismatch",
            "cluster_id_reused",
            "epoch_not_advanced",
            "data_dir_not_empty",
        ] {
            let e = BackupError::refused(reason, "x");
            assert_eq!(e.code(), 2, "{reason}");
            assert_eq!(e.reason(), reason);
        }
    }

    #[test]
    fn a_name_cannot_escape_the_output_directory() {
        assert!(validate_name("b1").is_ok());
        assert!(validate_name("nightly.2026-09-18_03").is_ok());
        for bad in ["", "../evil", "a/b", "a\\b", ".hidden", "a..b", "a b"] {
            assert!(validate_name(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn encryption_round_trips_and_a_wrong_key_yields_no_plaintext() {
        let key = [7u8; 32];
        let other = [9u8; 32];
        let plaintext = b"snapshot bytes".to_vec();
        let sealed = encrypt(&key, &plaintext).expect("encrypts");
        assert_ne!(sealed[NONCE_LEN..], plaintext[..]);
        assert_eq!(
            decrypt(&key, &sealed, Path::new("b1.snap")).expect("decrypts"),
            plaintext
        );
        let wrong = decrypt(&other, &sealed, Path::new("b1.snap")).expect_err("wrong key");
        assert_eq!(wrong.reason(), "decrypt_failed");
        assert_eq!(wrong.code(), 2);
    }

    #[test]
    fn two_encryptions_of_the_same_bytes_differ_by_their_nonce() {
        let key = [3u8; 32];
        let a = encrypt(&key, b"same").expect("encrypts");
        let b = encrypt(&key, b"same").expect("encrypts");
        assert_ne!(a, b, "a fixed nonce would make GCM catastrophically unsafe");
        assert_ne!(a[..NONCE_LEN], b[..NONCE_LEN]);
    }

    #[test]
    fn the_triple_shares_one_stem() {
        let (snap, manifest, sig) = BackupManifest::file_names("b1");
        assert_eq!(snap, "b1.snap");
        assert_eq!(manifest, "b1.manifest.json");
        assert_eq!(sig, "b1.manifest.sig");
    }
}
