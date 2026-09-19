//! Ed25519-signed bootstrap manifests (test plan TA-19, ADR-0011 §4.3, ADR-0018 §7).
//!
//! A bootstrap manifest is the one document that says "this cluster exists, these are its
//! voters". `config-server --form` verifies the Ed25519 signature **over the exact
//! `manifest.toml` bytes** before it parses a single field, then checks `expires_at` against
//! the system clock, then checks that the cluster id, recovery epoch, its own node id and its
//! own endpoints agree with its configuration.
//!
//! This fixture writes the three files a daemon reads — `manifest.toml`, `manifest.sig`,
//! `manifest.pub` — and, through [`Tamper`], the deliberately broken variants each negative
//! row needs.
//!
//! # Determinism
//!
//! The signing key is derived from the fixture seed the same way [`crate::tls`] derives its
//! keys, so a manifest row replays exactly.
//!
//! # Example
//!
//! ```no_run
//! use config_core::{ClusterId, NodeId};
//! use config_testkit::manifest::{Manifest, ManifestFixture, Voter};
//!
//! let dir = config_testkit::fs::temp_dir();
//! let fixture = ManifestFixture::new(9);
//! let manifest = Manifest::new(ClusterId::from_bytes([3u8; 16]))
//!     .with_voter(Voter::new(NodeId(1), "127.0.0.1:0", "127.0.0.1:0"));
//! let paths = fixture.write(dir.path(), &manifest);
//! # let _ = paths;
//! ```

use std::path::{Path, PathBuf};

use config_core::{ClusterId, NodeId, RecoveryEpoch};
use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Domain-separation string for the fixture's signing-key derivation.
const KEY_DERIVATION_DOMAIN: &[u8] = b"retcd-testkit-manifest-v1";

/// File name of the manifest document, relative to the manifest directory.
pub const MANIFEST_FILE: &str = "manifest.toml";
/// File name of the detached Ed25519 signature.
pub const SIGNATURE_FILE: &str = "manifest.sig";
/// File name of the signing public key.
pub const PUBLIC_KEY_FILE: &str = "manifest.pub";

/// One voter of a bootstrap manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Voter {
    /// Stable node id.
    pub node_id: u64,
    /// Peer-plane endpoint (`host:port`) this voter serves Raft on.
    pub peer: String,
    /// Client-plane endpoint (`host:port`) a leader hint may name.
    pub client: String,
}

impl Voter {
    /// Build a voter entry.
    pub fn new(node_id: NodeId, peer: impl Into<String>, client: impl Into<String>) -> Self {
        Self {
            node_id: node_id.0,
            peer: peer.into(),
            client: client.into(),
        }
    }

    /// This voter's id.
    pub fn id(&self) -> NodeId {
        NodeId(self.node_id)
    }
}

/// The manifest document, exactly as it is serialized into `manifest.toml`.
///
/// Field names are the wire contract between this fixture and `config-server`'s parser; a
/// rename here is a breaking change to the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// The cluster being created, as 32 lowercase hex characters.
    pub cluster_id: String,
    /// The recovery epoch being created at.
    pub recovery_epoch: u32,
    /// RFC 3339 instant after which this manifest must be refused.
    pub expires_at: String,
    /// The initial voter set.
    #[serde(default, rename = "voter")]
    pub voters: Vec<Voter>,
}

/// Far enough in the future that no CI run reaches it, and a literal so the document is
/// reproducible (the daemon's clock read is the only clock in the system, TA-19).
const DEFAULT_EXPIRY: &str = "2120-01-01T00:00:00Z";
/// Already past, for the expired row.
const PAST_EXPIRY: &str = "2021-01-01T00:00:00Z";

impl Manifest {
    /// An empty manifest for `cluster_id` at recovery epoch 0, expiring in the far future.
    pub fn new(cluster_id: ClusterId) -> Self {
        Self {
            cluster_id: cluster_id.to_string(),
            recovery_epoch: 0,
            expires_at: DEFAULT_EXPIRY.to_string(),
            voters: Vec::new(),
        }
    }

    /// Append a voter.
    pub fn with_voter(mut self, voter: Voter) -> Self {
        self.voters.push(voter);
        self
    }

    /// Set the recovery epoch.
    pub fn with_epoch(mut self, epoch: RecoveryEpoch) -> Self {
        self.recovery_epoch = epoch.0;
        self
    }

    /// Set the expiry instant (RFC 3339).
    pub fn with_expiry(mut self, expires_at: impl Into<String>) -> Self {
        self.expires_at = expires_at.into();
        self
    }

    /// Render to TOML — the exact bytes that get signed.
    pub fn to_toml(&self) -> String {
        toml::to_string_pretty(self).expect("a manifest serializes as TOML")
    }
}

/// How a manifest is deliberately broken, one variant per negative row (TA-19).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tamper {
    /// The signature file is valid Ed25519 but does not match the document: one byte of the
    /// signature is flipped.
    Signature,
    /// The document is modified *after* signing, so the signature no longer covers it.
    Body,
    /// Correctly signed, but `expires_at` is in the past.
    Expired,
    /// Correctly signed, but names a different cluster than the node is bound to.
    WrongCluster,
    /// Correctly signed, but names a different recovery epoch.
    WrongEpoch,
    /// Correctly signed, but one voter's endpoints are not the ones that node serves.
    WrongVoter,
}

/// Where a written manifest's three files landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestPaths {
    /// The signed document.
    pub manifest: PathBuf,
    /// The detached signature (64 raw bytes).
    pub signature: PathBuf,
    /// The signing public key (32 raw bytes).
    pub public_key: PathBuf,
}

/// The Ed25519 authority that signs bootstrap manifests.
pub struct ManifestFixture {
    signing_key: SigningKey,
    seed: u64,
}

/// `Debug` carries the replay seed and the public key, never the private one.
impl std::fmt::Debug for ManifestFixture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestFixture")
            .field("seed", &self.seed)
            .field("public_key_hex", &self.public_key_hex())
            .finish()
    }
}

impl ManifestFixture {
    /// Build a signing authority, reproducible from `seed`.
    pub fn new(seed: u64) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(KEY_DERIVATION_DOMAIN);
        hasher.update(seed.to_le_bytes());
        let bytes: [u8; 32] = hasher.finalize().into();
        Self {
            signing_key: SigningKey::from_bytes(&bytes),
            seed,
        }
    }

    /// The seed this fixture was built from; printed in every manifest-row failure message.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The 32 raw public-key bytes a daemon is configured with.
    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// The public key as 64 lowercase hex characters, for log lines and `Debug`.
    pub fn public_key_hex(&self) -> String {
        hex(&self.public_key_bytes())
    }

    /// Sign `bytes` exactly as `config-server` will verify them.
    pub fn sign(&self, bytes: &[u8]) -> [u8; 64] {
        self.signing_key.sign(bytes).to_bytes()
    }

    /// Write a correct manifest, its signature and the public key into `dir`.
    pub fn write(&self, dir: &Path, manifest: &Manifest) -> ManifestPaths {
        let toml = manifest.to_toml();
        let signature = self.sign(toml.as_bytes());
        self.write_parts(dir, toml.as_bytes(), &signature, &self.public_key_bytes())
    }

    /// Write a manifest broken in exactly one way.
    ///
    /// The variants split into two families, and the split is the point: `Signature` and
    /// `Body` break the *cryptography* and must be refused before any field is read;
    /// `Expired`, `WrongCluster`, `WrongEpoch` and `WrongVoter` are correctly signed and must
    /// be refused by a *semantic* check afterwards. A daemon that parsed first would pass the
    /// second family and fail only the first.
    pub fn write_tampered(&self, dir: &Path, manifest: &Manifest, tamper: Tamper) -> ManifestPaths {
        match tamper {
            Tamper::Signature => {
                let toml = manifest.to_toml();
                let mut signature = self.sign(toml.as_bytes());
                signature[0] ^= 0x01;
                self.write_parts(dir, toml.as_bytes(), &signature, &self.public_key_bytes())
            }
            Tamper::Body => {
                let toml = manifest.to_toml();
                let signature = self.sign(toml.as_bytes());
                // Appending a comment leaves the document semantically identical and the
                // signature invalid, which is exactly the "verify the exact bytes" claim.
                let modified = format!("{toml}# tampered\n");
                self.write_parts(
                    dir,
                    modified.as_bytes(),
                    &signature,
                    &self.public_key_bytes(),
                )
            }
            Tamper::Expired => self.write(dir, &manifest.clone().with_expiry(PAST_EXPIRY)),
            Tamper::WrongCluster => {
                let mut broken = manifest.clone();
                broken.cluster_id = flip_hex(&broken.cluster_id);
                self.write(dir, &broken)
            }
            Tamper::WrongEpoch => {
                let mut broken = manifest.clone();
                broken.recovery_epoch = broken.recovery_epoch.wrapping_add(1);
                self.write(dir, &broken)
            }
            Tamper::WrongVoter => {
                let mut broken = manifest.clone();
                if let Some(voter) = broken.voters.first_mut() {
                    voter.peer = "127.0.0.1:1".to_string(); // testkit:allow-port
                    voter.client = "127.0.0.1:2".to_string(); // testkit:allow-port
                }
                self.write(dir, &broken)
            }
        }
    }

    fn write_parts(
        &self,
        dir: &Path,
        manifest: &[u8],
        signature: &[u8],
        public_key: &[u8],
    ) -> ManifestPaths {
        std::fs::create_dir_all(dir)
            .unwrap_or_else(|e| panic!("create manifest dir {}: {e}", dir.display()));
        let paths = ManifestPaths {
            manifest: dir.join(MANIFEST_FILE),
            signature: dir.join(SIGNATURE_FILE),
            public_key: dir.join(PUBLIC_KEY_FILE),
        };
        write(&paths.manifest, manifest);
        write(&paths.signature, signature);
        write(&paths.public_key, public_key);
        paths
    }
}

fn write(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

/// Change a 32-hex cluster id into a different, still well-formed one.
fn flip_hex(cluster_id: &str) -> String {
    let mut chars: Vec<char> = cluster_id.chars().collect();
    if let Some(first) = chars.first_mut() {
        *first = if *first == '0' { '1' } else { '0' };
    }
    chars.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};

    fn sample() -> Manifest {
        Manifest::new(ClusterId::from_bytes([0x33; 16]))
            .with_voter(Voter::new(NodeId(1), "127.0.0.1:0", "127.0.0.1:0"))
            .with_voter(Voter::new(NodeId(2), "127.0.0.1:0", "127.0.0.1:0"))
    }

    fn verify(paths: &ManifestPaths) -> bool {
        let bytes = std::fs::read(&paths.manifest).expect("manifest readable");
        let sig = std::fs::read(&paths.signature).expect("signature readable");
        let pk = std::fs::read(&paths.public_key).expect("public key readable");
        let Ok(pk): Result<[u8; 32], _> = pk.try_into() else {
            return false;
        };
        let Ok(sig): Result<[u8; 64], _> = sig.try_into() else {
            return false;
        };
        let Ok(key) = VerifyingKey::from_bytes(&pk) else {
            return false;
        };
        key.verify(&bytes, &Signature::from_bytes(&sig)).is_ok()
    }

    #[test]
    fn the_same_seed_produces_the_same_key() {
        assert_eq!(
            ManifestFixture::new(4).public_key_hex(),
            ManifestFixture::new(4).public_key_hex()
        );
        assert_ne!(
            ManifestFixture::new(4).public_key_hex(),
            ManifestFixture::new(5).public_key_hex()
        );
    }

    #[test]
    fn a_written_manifest_verifies_and_round_trips() {
        let dir = crate::fs::temp_dir();
        let fixture = ManifestFixture::new(21);
        let manifest = sample();
        let paths = fixture.write(dir.path(), &manifest);
        assert!(verify(&paths), "a freshly written manifest must verify");

        let text = std::fs::read_to_string(&paths.manifest).expect("manifest readable");
        let parsed: Manifest = toml::from_str(&text).expect("manifest parses back");
        assert_eq!(parsed, manifest);
    }

    #[test]
    fn signature_and_body_tampering_break_verification() {
        let dir = crate::fs::temp_dir();
        let fixture = ManifestFixture::new(22);
        for tamper in [Tamper::Signature, Tamper::Body] {
            let sub = dir.path().join(format!("{tamper:?}"));
            let paths = fixture.write_tampered(&sub, &sample(), tamper);
            assert!(!verify(&paths), "{tamper:?} must not verify");
        }
    }

    /// The semantic tampers stay *correctly signed*: a daemon that only checked the signature
    /// would accept all four, which is the defect this split exists to catch.
    #[test]
    fn semantic_tampering_still_verifies_but_changes_the_document() {
        let dir = crate::fs::temp_dir();
        let fixture = ManifestFixture::new(23);
        let base = sample();
        for tamper in [
            Tamper::Expired,
            Tamper::WrongCluster,
            Tamper::WrongEpoch,
            Tamper::WrongVoter,
        ] {
            let sub = dir.path().join(format!("{tamper:?}"));
            let paths = fixture.write_tampered(&sub, &base, tamper);
            assert!(verify(&paths), "{tamper:?} must remain correctly signed");
            let text = std::fs::read_to_string(&paths.manifest).expect("manifest readable");
            let parsed: Manifest = toml::from_str(&text).expect("manifest parses");
            assert_ne!(parsed, base, "{tamper:?} must change the document");
        }
    }
}
