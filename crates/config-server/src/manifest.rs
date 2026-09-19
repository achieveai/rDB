//! Signed bootstrap manifest verification (ADR-0018 §7, spec §4.3, ADR-0011, test plan TA-19).
//!
//! The order of the three checks is the security property:
//!
//! 1. **Signature over the exact `manifest.toml` bytes**, before a single field is parsed. A
//!    daemon that parsed first would be interpreting an attacker's document in order to
//!    decide whether to trust it.
//! 2. **`expires_at` against the system clock.** This is the only behaviour-affecting clock
//!    read in rEtcd; the state machine stays clock-free (TA-9).
//! 3. **Agreement with this node's configuration**: cluster id, recovery epoch, own node id
//!    present, and own endpoints equal to the ones this node is about to advertise.
//!
//! Every failure is exit code 2 and happens before `form_cluster` is called.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use config_core::{ClusterId, ClusterIdentity, NodeId, RecoveryEpoch};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::Deserialize;

use crate::config::ManifestFiles;

/// One voter of the manifest.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestVoter {
    /// Stable node id.
    pub node_id: u64,
    /// Peer-plane endpoint this voter serves Raft on.
    pub peer: String,
    /// Client-plane endpoint a leader hint may name.
    pub client: String,
}

/// The manifest document.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// Cluster being created, as 32 lowercase hex characters.
    pub cluster_id: String,
    /// Recovery epoch being created at.
    pub recovery_epoch: u32,
    /// RFC 3339 instant after which this manifest must be refused.
    pub expires_at: String,
    /// The initial voter set.
    #[serde(default, rename = "voter")]
    pub voters: Vec<ManifestVoter>,
}

/// Why a bootstrap manifest was refused. Every variant is exit code 2.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// One of the three files could not be read.
    #[error("cannot read manifest file {path}: {source}")]
    Io {
        /// The file.
        path: String,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// The public key or signature is not the right length or shape.
    #[error("malformed manifest {what}: {detail}")]
    Malformed {
        /// `"signature"` or `"public key"`.
        what: &'static str,
        /// What was wrong.
        detail: String,
    },
    /// The signature does not cover these bytes. Checked before any field is parsed.
    #[error("manifest signature does not verify against the configured signing key")]
    BadSignature,
    /// The document is signed but is not a valid manifest.
    #[error("manifest is signed but unparsable: {0}")]
    Unparsable(String),
    /// `expires_at` is not an RFC 3339 instant this build can compare.
    #[error("manifest expires_at {value:?} is not an RFC 3339 UTC instant")]
    BadExpiry {
        /// What the document said.
        value: String,
    },
    /// The manifest has expired.
    #[error("manifest expired at {expires_at}; refusing to form a cluster from it")]
    Expired {
        /// The expiry instant from the document.
        expires_at: String,
    },
    /// The system clock could not be read, so `expires_at` cannot be checked at all.
    #[error("system clock is before the Unix epoch ({detail}); cannot check manifest expiry")]
    ClockUnavailable {
        /// What the clock said.
        detail: String,
    },
    /// The manifest describes a different cluster, epoch, node, or endpoint.
    #[error("manifest does not match this node: {0}")]
    Mismatch(String),
}

/// A verified manifest: signature checked, not expired, and in agreement with this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedManifest {
    /// The cluster this manifest creates.
    pub cluster_id: ClusterId,
    /// The recovery epoch it creates at.
    pub recovery_epoch: RecoveryEpoch,
    /// `(node_id, peer endpoint, client endpoint)` for every voter, ascending by id.
    pub voters: Vec<(NodeId, String, String)>,
}

/// Read and verify a bootstrap manifest, and check everything that does not depend on a bound
/// port: signature, expiry, cluster id, recovery epoch, and this node being a listed voter.
///
/// Runs **before any listener is opened** (ADR-0018 §5), so a forged or expired manifest never
/// gets as far as a socket. The one remaining check —
/// [`check_endpoints`] — needs the addresses the OS actually assigned and therefore runs after
/// binding, immediately before `form_cluster`.
pub fn verify_document(
    files: &ManifestFiles,
    identity: &ClusterIdentity,
) -> Result<VerifiedManifest, ManifestError> {
    let bytes = read(&files.manifest)?;
    let signature = read(&files.signature)?;
    let public_key = read(&files.public_key)?;

    let key_bytes: [u8; 32] =
        public_key
            .as_slice()
            .try_into()
            .map_err(|_| ManifestError::Malformed {
                what: "public key",
                detail: format!("expected 32 raw bytes, got {}", public_key.len()),
            })?;
    let key = VerifyingKey::from_bytes(&key_bytes).map_err(|e| ManifestError::Malformed {
        what: "public key",
        detail: e.to_string(),
    })?;
    let sig_bytes: [u8; 64] =
        signature
            .as_slice()
            .try_into()
            .map_err(|_| ManifestError::Malformed {
                what: "signature",
                detail: format!("expected 64 raw bytes, got {}", signature.len()),
            })?;

    // Step 1: cryptography, over the bytes as they are on disk, before any parse.
    key.verify(&bytes, &Signature::from_bytes(&sig_bytes))
        .map_err(|_| ManifestError::BadSignature)?;

    let text = String::from_utf8(bytes)
        .map_err(|e| ManifestError::Unparsable(format!("manifest is not UTF-8: {e}")))?;
    let manifest: Manifest =
        toml::from_str(&text).map_err(|e| ManifestError::Unparsable(e.to_string()))?;

    // Step 2: expiry, the one clock read in the system.
    let expires_at =
        parse_rfc3339_utc(&manifest.expires_at).ok_or_else(|| ManifestError::BadExpiry {
            value: manifest.expires_at.clone(),
        })?;
    // A clock error is a refusal, not `now = 0`. Treating an unreadable clock as the Unix
    // epoch would make *every* manifest look unexpired, which turns the one safety check that
    // depends on the clock into a no-op exactly when the clock cannot be trusted (critic A3).
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .map_err(|e| ManifestError::ClockUnavailable {
            detail: e.to_string(),
        })?;
    if now >= expires_at {
        return Err(ManifestError::Expired {
            expires_at: manifest.expires_at.clone(),
        });
    }

    // Step 3: agreement with this node.
    let cluster_id: ClusterId = manifest
        .cluster_id
        .parse()
        .map_err(|e| ManifestError::Unparsable(format!("{e}")))?;
    if cluster_id != identity.cluster_id {
        return Err(ManifestError::Mismatch(format!(
            "manifest names cluster {cluster_id}, this node is bound to {}",
            identity.cluster_id
        )));
    }
    if RecoveryEpoch(manifest.recovery_epoch) != identity.recovery_epoch {
        return Err(ManifestError::Mismatch(format!(
            "manifest names recovery epoch {}, this node is bound to {}",
            manifest.recovery_epoch, identity.recovery_epoch
        )));
    }
    if manifest.voters.is_empty() {
        return Err(ManifestError::Mismatch("manifest names no voters".into()));
    }

    let mut voters: Vec<(NodeId, String, String)> = manifest
        .voters
        .iter()
        .map(|v| (NodeId(v.node_id), v.peer.clone(), v.client.clone()))
        .collect();
    voters.sort_by_key(|(id, _, _)| *id);
    if voters.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err(ManifestError::Mismatch(
            "manifest lists the same node id twice".into(),
        ));
    }

    if !voters.iter().any(|(id, _, _)| *id == identity.node_id) {
        return Err(ManifestError::Mismatch(format!(
            "manifest does not list this node ({}) as a voter",
            identity.node_id
        )));
    }

    Ok(VerifiedManifest {
        cluster_id,
        recovery_epoch: RecoveryEpoch(manifest.recovery_epoch),
        voters,
    })
}

/// The last manifest check: the endpoints the manifest publishes for this node are the ones it
/// actually bound.
///
/// Committing an address nobody answers on would only surface later as an unreachable peer or
/// an undialable leader hint, so it is refused here instead (mirrors
/// `FormationError::EndpointMismatch` one layer up).
pub fn check_endpoints(
    manifest: &VerifiedManifest,
    identity: &ClusterIdentity,
    peer_endpoint: &str,
    client_endpoint: &str,
) -> Result<(), ManifestError> {
    let own = manifest
        .voters
        .iter()
        .find(|(id, _, _)| *id == identity.node_id)
        .ok_or_else(|| {
            ManifestError::Mismatch(format!(
                "manifest does not list this node ({}) as a voter",
                identity.node_id
            ))
        })?;
    if own.1 != peer_endpoint || own.2 != client_endpoint {
        return Err(ManifestError::Mismatch(format!(
            "manifest gives node {} peer {:?} / client {:?}, but this node serves {:?} / {:?}",
            identity.node_id, own.1, own.2, peer_endpoint, client_endpoint
        )));
    }
    Ok(())
}

fn read(path: &Path) -> Result<Vec<u8>, ManifestError> {
    std::fs::read(path).map_err(|source| ManifestError::Io {
        path: path.display().to_string(),
        source,
    })
}

/// Parse `YYYY-MM-DDTHH:MM:SSZ` into a Unix timestamp.
///
/// Hand-rolled rather than pulling `chrono` into the daemon: the manifest grammar fixes the
/// shape to UTC RFC 3339 with no offset and no fractional seconds, and a parser that accepted
/// more would be accepting shapes the signer never promised to produce. Anything else is
/// [`ManifestError::BadExpiry`], which is a refusal, not a lenient fallback.
fn parse_rfc3339_utc(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() != 20 || bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' {
        return None;
    }
    if bytes[13] != b':' || bytes[16] != b':' || bytes[19] != b'Z' {
        return None;
    }
    let num = |from: usize, to: usize| value.get(from..to)?.parse::<i64>().ok();
    let (year, month, day) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (hour, minute, second) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}

/// Howard Hinnant's `days_from_civil`: days since 1970-01-01 for a proleptic Gregorian date.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_parses_only_the_promised_shape() {
        assert_eq!(parse_rfc3339_utc("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_rfc3339_utc("2000-01-01T00:00:00Z"), Some(946_684_800));
        assert_eq!(
            parse_rfc3339_utc("2021-01-01T00:00:00Z"),
            Some(1_609_459_200)
        );
        for bad in [
            "2021-01-01T00:00:00+01:00",
            "2021-01-01 00:00:00Z",
            "2021-13-01T00:00:00Z",
            "2021-01-01T24:00:00Z",
            "not a date",
            "",
        ] {
            assert_eq!(parse_rfc3339_utc(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn the_far_future_expiry_is_after_the_recent_past() {
        let past = parse_rfc3339_utc("2021-01-01T00:00:00Z").expect("valid");
        let future = parse_rfc3339_utc("2120-01-01T00:00:00Z").expect("valid");
        assert!(future > past);
    }
}
