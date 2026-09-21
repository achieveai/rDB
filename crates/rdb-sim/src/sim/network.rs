//! Controlled delivery: drop, duplicate, reorder, partition, heal, and forge.
//!
//! The network is hostile by configuration, not by chance. Every delivery decision is a recorded
//! choice, so a failing history replays exactly.
//!
//! Forged identity is a first-class injectable fault, not a special case. [`NetworkOp::ForgeAck`]
//! delivers an acknowledgement under a node name or a replica role its sender did not earn.
//! Spec §6.1 says an unauthenticated or non-member append is rejected, and spec §5.2 says a
//! shadow's acknowledgement never counts; this is how those two assertions get something to
//! reject. The rejection must come from the kernel's own authentication and membership check —
//! [`rdb_core::contracts::membership::PartitionConfig::copy_of`] returns `None` for an
//! unauthenticated peer, and [`rdb_core::contracts::ids::ReplicaRole::may_qualify_ack`] answers
//! the role question — never from a `cfg` branch or a test-only guard in kernel code.
//!
//! # Seed state
//!
//! The fault vocabulary is real; delivery is package H1.

use rdb_core::contracts::ids::{MessageId, NodeId, ReplicaRole};
use rdb_core::contracts::transport::{Frame, PeerLabel};

use crate::error::SimError;

/// What the network does to one frame.
///
/// A closed set, and the coverage matrix counts it (spike §7). `Reorder` is expressed as a delay
/// rather than a target position because delivery order is decided by the scheduler's
/// `(tick, event_id)` order; any other spelling would put a second ordering rule in the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Delivery {
    /// Deliver after `delay_millis`.
    Deliver {
        /// How long in flight.
        delay_millis: u64,
    },
    /// Never deliver.
    Drop,
    /// Deliver twice, the second copy after a further delay.
    Duplicate {
        /// Delay of the first copy.
        delay_millis: u64,
        /// Additional delay of the second copy.
        second_delay_millis: u64,
    },
}

/// A link-level condition between two nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LinkState {
    /// Frames flow, subject to per-frame [`Delivery`].
    Up,
    /// Every frame is dropped until the link is healed.
    Partitioned,
}

/// An injectable network fault, as a scenario writes it.
///
/// One enum rather than a method per fault, because the scenario generator, the shrinker and the
/// coverage matrix all need to enumerate and compare operations, and a set of methods cannot be
/// enumerated. The `scenario_op_index` on
/// [`rdb_core::contracts::trace::TraceKind::FaultInjected`] is this value's position in the
/// scenario's operation list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NetworkOp {
    /// Set the link between two nodes. Symmetric: a one-way partition is a different fault and
    /// must be asked for explicitly, as two ops.
    SetLink {
        /// One end.
        a: NodeId,
        /// The other.
        b: NodeId,
        /// The condition.
        state: LinkState,
    },
    /// Decide what happens to the next frame from `from` to `to`.
    PlanNext {
        /// Sender.
        from: NodeId,
        /// Recipient.
        to: NodeId,
        /// What the network does with it.
        delivery: Delivery,
    },
    /// Deliver the next acknowledgement from `from` to `to` under an identity its sender did not
    /// earn.
    ///
    /// Two independent lies, because they are rejected by two different rules and a scenario must
    /// be able to tell a passing kernel from one that happens to reject everything:
    ///
    /// - `claimed_node` different from `from` is impersonation. The membership lookup must fail,
    ///   or the frame must arrive with `authenticated: false` and be refused before membership is
    ///   consulted.
    /// - `claimed_role` above the sender's real role — a shadow claiming
    ///   [`ReplicaRole::RegularSecondary`] — is the one spec §5.2 calls out. The receiver must
    ///   resolve the role from *its own* pinned configuration, never from the frame, so the claim
    ///   changes nothing. The disagreement is recorded as the gap between
    ///   [`rdb_core::contracts::trace::TraceKind::ReplicationAck`]'s `peer_role` and
    ///   [`rdb_core::contracts::trace::TraceKind::ReplicationAckDelivered`]'s.
    ForgeAck {
        /// The real sender.
        from: NodeId,
        /// The primary that receives it.
        to: NodeId,
        /// The node identity the frame claims.
        claimed_node: NodeId,
        /// The role the frame claims.
        claimed_role: ReplicaRole,
        /// Whether the frame nevertheless presents valid credentials. `false` is the ordinary
        /// forgery; `true` models a *stolen but real* credential, which membership and epoch
        /// checks must still refuse.
        authenticated: bool,
    },
    /// Deliver the next frame from `from` to `to` under an arbitrary label. The general form of
    /// [`Self::ForgeAck`], for frames that are not acknowledgements.
    ForgeNext {
        /// The real sender.
        from: NodeId,
        /// The recipient.
        to: NodeId,
        /// The label to deliver it under.
        label: PeerLabel,
    },
}

/// The controlled network.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Network;

impl Network {
    /// A network with every link up and no faults scheduled.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Apply one scenario operation.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands delivery.
    pub const fn inject(&mut self, _op: NetworkOp) -> Result<(), SimError> {
        Err(SimError::unavailable("sim::network::Network::inject"))
    }

    /// Hand a frame to the network. Returns the identity the frame was sent under.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands delivery.
    pub fn send(
        &mut self,
        _from: NodeId,
        _to: NodeId,
        _frame: Frame,
    ) -> Result<MessageId, SimError> {
        Err(SimError::unavailable("sim::network::Network::send"))
    }
}
