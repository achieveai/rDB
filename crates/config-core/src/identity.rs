//! Stable cluster and node identity (ADR-0011, spec §4.2).
//!
//! A data directory is permanently bound to a [`ClusterIdentity`]; a mismatch fails startup.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// 128-bit cluster identifier, displayed as 32 lowercase hex characters.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ClusterId(pub [u8; 16]);

impl ClusterId {
    /// Build from raw bytes.
    pub const fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }
    /// Raw bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClusterId({self})")
    }
}

/// Error parsing a [`ClusterId`] from hex.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid cluster id {0:?}: expected 32 hex chars")]
pub struct ParseClusterIdError(pub String);

impl FromStr for ClusterId {
    type Err = ParseClusterIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 32 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ParseClusterIdError(s.to_string()));
        }
        let mut out = [0u8; 16];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hi = (chunk[0] as char).to_digit(16).unwrap() as u8;
            let lo = (chunk[1] as char).to_digit(16).unwrap() as u8;
            out[i] = (hi << 4) | lo;
        }
        Ok(Self(out))
    }
}

/// Stable, never-reused node identifier (1..=3 in the first release).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}
impl From<u64> for NodeId {
    fn from(v: u64) -> Self {
        Self(v)
    }
}
impl From<NodeId> for u64 {
    fn from(v: NodeId) -> Self {
        v.0
    }
}

/// Incremented only by a total-quorum-loss recovery (post-release); `0` for a fresh cluster.
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Debug, Default,
)]
pub struct RecoveryEpoch(pub u32);

impl fmt::Display for RecoveryEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What a data directory is bound to.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
pub struct ClusterIdentity {
    /// Cluster.
    pub cluster_id: ClusterId,
    /// Recovery epoch.
    pub recovery_epoch: RecoveryEpoch,
    /// This node.
    pub node_id: NodeId,
}

impl fmt::Display for ClusterIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cluster={} epoch={} node={}",
            self.cluster_id, self.recovery_epoch, self.node_id
        )
    }
}

/// The provenance marker a restored data directory carries (M5, ADR-0026 runbooks,
/// ADR-0023 recovery).
///
/// Written once when an operator restores a backup into a fresh data directory, and surfaced
/// in the health payload thereafter: an operator looking at a node has to be able to tell a
/// node that recovered its own state from a node that was *seeded from someone else's*
/// snapshot, without reading a deployment log. `recovery_epoch` is the plain `u32` of
/// [`RecoveryEpoch`] so the marker stays a flat, postcard-stable record.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
pub struct RestoredFrom {
    /// The cluster the restored snapshot belonged to.
    pub cluster_id: ClusterId,
    /// The recovery epoch recorded in that snapshot.
    pub recovery_epoch: u32,
    /// The last revision the restored snapshot contained.
    pub revision: u64,
}

impl fmt::Display for RestoredFrom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cluster={} epoch={} revision={}",
            self.cluster_id, self.recovery_epoch, self.revision
        )
    }
}

/// Raised when storage was created for a different identity (ADR-0011).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("storage identity mismatch: stored [{stored}] configured [{configured}]")]
pub struct IdentityMismatch {
    /// What the data directory recorded.
    pub stored: ClusterIdentity,
    /// What the node was configured with.
    pub configured: ClusterIdentity,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[config_log::retcd_test]
    fn cluster_id_hex_round_trip() {
        let id = ClusterId::from_bytes([0xab; 16]);
        let s = id.to_string();
        assert_eq!(s.len(), 32);
        assert_eq!(s.parse::<ClusterId>().unwrap(), id);
        assert!("zz".parse::<ClusterId>().is_err());
        assert!("ab".repeat(15).parse::<ClusterId>().is_err());
    }
}
