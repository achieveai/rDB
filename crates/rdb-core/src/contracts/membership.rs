//! The pinned membership of one partition: who the copies are, and which authenticated peer is
//! which copy.
//!
//! This lives in `rdb-core`, not in the simulator, because the mapping from *an authenticated
//! peer* to *a copy in the required set* is a protocol decision. Team kernel-b asked for it
//! here (2026-09-20) and that is the right home: if the simulator owned it, a forged-identity
//! test would be testing the simulator's lookup rather than the kernel's.
//!
//! Two rules are structural rather than commented:
//!
//! * [`PartitionConfig::copy_of`] returns `None` for an unauthenticated
//!   [`crate::contracts::transport::PeerLabel`]. A kernel module that only ever learns a copy id
//!   through this function cannot act on a forged peer, whatever it forgot to check.
//! * The required-copy set is derived from [`PartitionConfig::config_version`], never from a
//!   node's current name or liveness. Spec §6.2: "Membership changes cannot erase old exposure",
//!   and "no timer reset merely because a replica was renamed/replaced".

use serde::{Deserialize, Serialize};

use crate::contracts::ids::{BootId, ConfigVersion, NodeId, PartitionId, ReplicaRole};
use crate::contracts::transport::PeerLabel;

/// A copy's slot in the configuration.
///
/// Distinct from [`NodeId`] on purpose: a replacement node takes over a slot, and the protection
/// predicate is about slots. Renaming the node behind a slot changes nothing about exposure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CopyId(
    /// Slot index within the configuration.
    pub u8,
);

/// One copy in a pinned configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Member {
    /// The slot.
    pub copy: CopyId,
    /// The node currently filling it.
    pub node: NodeId,
    /// The process lifetime that was admitted to the slot, when one has been observed. A frame
    /// from a different boot of the same node is not from this member.
    pub boot: Option<BootId>,
    /// What the slot is allowed to do for the protection predicate.
    pub role: ReplicaRole,
}

/// The membership of one partition, pinned by a configuration version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionConfig {
    /// The partition.
    pub partition: PartitionId,
    /// The version that pins this set. Every predicate that names copies names this too.
    pub config_version: ConfigVersion,
    /// The copies, in ascending [`CopyId`] order.
    pub members: Vec<Member>,
}

impl PartitionConfig {
    /// The member an authenticated peer is, or `None`.
    ///
    /// Returns `None` when the peer is unauthenticated, when its node is not in this
    /// configuration, or when the member's admitted boot is known and differs. Those three are
    /// deliberately one answer: each of them means "this frame is not from a copy of this
    /// configuration", and splitting them would invite a caller to treat one of them as
    /// recoverable.
    #[must_use]
    pub fn copy_of(&self, peer: &PeerLabel) -> Option<&Member> {
        if !peer.authenticated {
            return None;
        }
        self.members.iter().find(|member| {
            member.node == peer.node && member.boot.is_none_or(|boot| boot == peer.boot)
        })
    }

    /// The copies whose acknowledgement may qualify a transaction: primary and regular
    /// secondaries, never shadows.
    pub fn required_regular(&self) -> impl Iterator<Item = &Member> {
        self.members
            .iter()
            .filter(|member| member.role.may_qualify_ack())
    }

    /// The member filling a slot, or `None` when the slot is not in this configuration.
    #[must_use]
    pub fn member(&self, copy: CopyId) -> Option<&Member> {
        self.members.iter().find(|member| member.copy == copy)
    }
}
