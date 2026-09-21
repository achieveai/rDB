//! The transport seam: bytes to a peer, with no ordering, no uniqueness and no trust.
//!
//! What this seam deliberately does **not** promise (spike §4): delivery, ordering, or
//! at-most-once. A frame may be dropped, duplicated, reordered or delivered long after the
//! sender gave up. Anything the protocol needs beyond that — idempotence, ancestry, progress —
//! is derived from identities carried *inside* the frame, never from the channel.
//!
//! Identity is a claim until it is checked. [`PeerLabel::authenticated`] records whether the
//! environment authenticated the sender; a forged label is injectable on purpose, and a kernel
//! module that acts on an unauthenticated frame is the bug the injection is there to find
//! (spec §6.1: "unauthenticated append is rejected").

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::contracts::ids::{BootId, ConfigVersion, MessageId, NodeId};

/// Who a frame claims to be from, and whether that claim was verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerLabel {
    /// The claimed node.
    pub node: NodeId,
    /// The claimed process lifetime. A frame from a stale boot is not from the node that is
    /// running now.
    pub boot: BootId,
    /// Whether the environment verified the claim. `false` means the frame is forged or
    /// unauthenticated and must be rejected before any state is touched.
    pub authenticated: bool,
}

/// One unit of transport. The body is opaque here; its meaning is the receiving module's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frame {
    /// Identity of this frame. A duplicate carries the same one, which is what makes it
    /// recognisable as a duplicate rather than a second request.
    pub id: MessageId,
    /// Protocol version of the body. Checked against
    /// [`crate::contracts::version::check_mandatory`] **before** the body is decoded.
    pub protocol: u16,
    /// The membership configuration the sender believed it was in. A frame from an old config
    /// cannot advance a predicate pinned to a newer one (spec §6.2).
    pub config: ConfigVersion,
    /// The encoded body.
    pub body: Bytes,
}

/// Why a send did not reach the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum LinkFault {
    /// The environment is partitioned between the two nodes.
    Partitioned,
    /// The peer is not reachable at all.
    Unreachable,
    /// The frame exceeded the configured size bound.
    TooLarge,
}

/// What a kernel module asks the transport environment to do.
///
/// One variant. There is no broadcast: spec §5.2 sends the identical envelope to each secondary
/// *concurrently*, and a broadcast primitive would hide which peer an acknowledgement is
/// attributable to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SendEffect {
    /// Send one frame to one peer.
    Unicast {
        /// The intended recipient.
        to: NodeId,
        /// The frame.
        frame: Frame,
    },
}

/// What the transport environment reports back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportEvent {
    /// A frame arrived. It may be a duplicate, out of order, or forged.
    Delivered {
        /// Who it claims to be from, and whether that was verified.
        from: PeerLabel,
        /// The frame.
        frame: Frame,
    },
    /// A send could not be attempted or could not complete. **Not** proof the peer did not
    /// receive it: a send that failed after the bytes left is indistinguishable here.
    SendFailed {
        /// The frame that failed.
        id: MessageId,
        /// Why.
        fault: LinkFault,
    },
}
