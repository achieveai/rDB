//! The pinned membership of one partition: who the copies are, and which authenticated peer is
//! which copy.
//!
//! This lives in `rdb-core`, not in the simulator, because the mapping from *an authenticated
//! peer* to *a copy in the required set* is a protocol decision. Team kernel-b asked for it
//! here (2026-09-20) and that is the right home: if the simulator owned it, a forged-identity
//! test would be testing the simulator's lookup rather than the kernel's.
//!
//! Four rules are structural rather than commented:
//!
//! * [`PartitionConfig::copy_of`] returns `None` for an unauthenticated
//!   [`crate::contracts::transport::PeerLabel`]. A kernel module that only ever learns a copy id
//!   through this function cannot act on a forged peer, whatever it forgot to check.
//! * A member has exactly one [`BootId`], and `copy_of` matches node **and** boot. A restarted
//!   node is not a copy until a new configuration names its new boot (finding K-F-21).
//! * The required-copy set is derived from [`PartitionConfig::config_version`], never from a
//!   node's current name or liveness. Spec §6.2: "Membership changes cannot erase old exposure",
//!   and "no timer reset merely because a replica was renamed/replaced".
//! * [`PartitionConfig`] deserialises through [`PartitionConfig::validate`], so a decoded
//!   configuration cannot carry a zero acknowledgement threshold (finding K-F-39). The
//!   invariant is enforced by the type, not by remembering to call `validate` after a decode.

use serde::{Deserialize, Serialize};

use crate::contracts::errors::RdbError;
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
    /// The process lifetime that fills it. Not optional (finding K-F-21): the value comes from
    /// where it exists in the real system — the `nodes/{id}` control record carries the boot
    /// UUID (spec §7.1), and the membership the control provider activates
    /// ([`crate::contracts::trace::TraceKind::TopologyChange`]) names it. A frame from a
    /// different boot of the same node is not from this member.
    pub boot: BootId,
    /// What the slot is allowed to do for the protection predicate.
    pub role: ReplicaRole,
}

/// The membership of one partition, pinned by a configuration version.
///
/// Deserialisation goes through [`PartitionConfig::validate`] (finding K-F-39), so a decoded
/// configuration holds the threshold invariant by construction rather than by convention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "UnvalidatedPartitionConfig")]
pub struct PartitionConfig {
    /// The partition.
    pub partition: PartitionId,
    /// The version that pins this set. Every predicate that names copies names this too.
    pub config_version: ConfigVersion,
    /// The copies, in ascending [`CopyId`] order.
    pub members: Vec<Member>,
    /// How many regular-secondary acknowledgements qualify a write (lead ruling B-R30; team
    /// kernel-b `design.md` §3.5 `qualifies_now`). Read from the pinned configuration by R1 and
    /// consumed by P1, so there is no second, independently maintained threshold. Never zero:
    /// [`Self::validate`] refuses it, because "no acknowledgement required" is the spec §5.2
    /// rule with the safety taken out. The default is [`Self::DEFAULT_MIN_REGULAR_ACKS`].
    pub min_regular_acks: u8,
}

/// [`PartitionConfig`]'s wire shape, before [`PartitionConfig::validate`] has run.
///
/// Serde's `try_from` needs a type it may build while the invariant is still unknown, so the
/// fields are repeated here and nowhere else. The rule itself is not repeated: the conversion
/// below calls `validate`, the one place that states it. A field added to `PartitionConfig`
/// and not to this struct fails to compile in that conversion, so the two cannot drift apart
/// unnoticed.
#[derive(Deserialize)]
#[serde(rename = "PartitionConfig")]
struct UnvalidatedPartitionConfig {
    partition: PartitionId,
    config_version: ConfigVersion,
    members: Vec<Member>,
    min_regular_acks: u8,
}

impl TryFrom<UnvalidatedPartitionConfig> for PartitionConfig {
    type Error = RdbError;

    fn try_from(wire: UnvalidatedPartitionConfig) -> Result<Self, Self::Error> {
        let config = Self {
            partition: wire.partition,
            config_version: wire.config_version,
            members: wire.members,
            min_regular_acks: wire.min_regular_acks,
        };
        config.validate()?;
        Ok(config)
    }
}

impl PartitionConfig {
    /// The threshold a configuration carries unless it says otherwise: one qualifying regular
    /// secondary, spec §5.2's `BufferedOnTwo`.
    pub const DEFAULT_MIN_REGULAR_ACKS: u8 = 1;

    /// A configuration at the default threshold.
    #[must_use]
    pub fn new(
        partition: PartitionId,
        config_version: ConfigVersion,
        members: Vec<Member>,
    ) -> Self {
        Self {
            partition,
            config_version,
            members,
            min_regular_acks: Self::DEFAULT_MIN_REGULAR_ACKS,
        }
    }

    /// The same configuration with an explicit threshold, refused when it is zero.
    ///
    /// # Errors
    ///
    /// [`RdbError::InvalidArgument`] naming `min_regular_acks` — see [`Self::validate`].
    pub fn with_min_regular_acks(mut self, min_regular_acks: u8) -> Result<Self, RdbError> {
        self.min_regular_acks = min_regular_acks;
        self.validate()?;
        Ok(self)
    }

    /// Whether the configuration is one the kernel may act on.
    ///
    /// # Errors
    ///
    /// [`RdbError::InvalidArgument`] with `field: "min_regular_acks"` when the threshold is
    /// zero. Deserialisation is routed through here by
    /// `impl TryFrom<UnvalidatedPartitionConfig> for PartitionConfig` and the `try_from`
    /// container attribute (finding K-F-39), so a decoded configuration cannot carry a zero.
    pub fn validate(&self) -> Result<(), RdbError> {
        if self.min_regular_acks == 0 {
            return Err(RdbError::InvalidArgument {
                field: "min_regular_acks",
            });
        }
        Ok(())
    }

    /// The member an authenticated peer is, or `None`.
    ///
    /// Returns `None` when the peer is unauthenticated, when its node is not in this
    /// configuration, or when the member's boot differs from the peer's. Those three are
    /// deliberately one answer: each of them means "this frame is not from a copy of this
    /// configuration", and splitting them would invite a caller to treat one of them as
    /// recoverable.
    ///
    /// The boot match is the fail-closed half of finding K-F-21. A peer always names a boot,
    /// and a reincarnated node — same [`NodeId`], new [`BootId`] — is not the copy it used to
    /// be: matching it anyway would credit the new incarnation with its former self's
    /// acknowledgements. It becomes a copy again only when a new configuration names its new
    /// boot (ADR-rdb-0007).
    #[must_use]
    pub fn copy_of(&self, peer: &PeerLabel) -> Option<&Member> {
        if !peer.authenticated {
            return None;
        }
        self.members
            .iter()
            .find(|member| member.node == peer.node && member.boot == peer.boot)
    }

    /// The primary, or `None` when the configuration names none.
    ///
    /// A configuration with no primary is legal to build — that is the state between a fence
    /// and the next grant — and a caller that needs one must say so.
    #[must_use]
    pub fn primary(&self) -> Option<&Member> {
        self.members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary)
    }

    /// The regular secondaries: the copies whose acknowledgement the primary waits for.
    ///
    /// Excludes the primary (finding K-F-23). Spec §5.2's RF3 rule is "durable on the primary
    /// and acknowledged by two *secondaries*"; an implementation that counted "two of this set"
    /// with the primary inside it would be satisfied by the primary plus one secondary, which
    /// is off by one in the direction that loses writes. Excludes shadows too: a shadow's
    /// acknowledgement never qualifies anything (spec §5.2). [`Self::primary`] is the separate
    /// accessor, and [`ReplicaRole::may_qualify_ack`] is a different question — whether a
    /// role's acknowledgement counts at all — which stays true for the primary.
    pub fn required_regular(&self) -> impl Iterator<Item = &Member> {
        self.members
            .iter()
            .filter(|member| member.role == ReplicaRole::RegularSecondary)
    }

    /// The member filling a slot, or `None` when the slot is not in this configuration.
    #[must_use]
    pub fn member(&self, copy: CopyId) -> Option<&Member> {
        self.members.iter().find(|member| member.copy == copy)
    }
}
