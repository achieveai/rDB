//! Controlled delivery: drop, duplicate, reorder, partition, heal, forge, and corrupt.
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
//! # State
//!
//! The fault vocabulary is real and [`Network::inject`] records it; link state is kept.
//! Delivery is real since 2026-09-26: [`Network::send`] decides a frame's [`Fate`] from the
//! link and the next matching plan, and hands back the [`Arrival`]s the harness schedules.
//! [`NetworkOp::ForgeAck`] is delivered at the frame level (lead ruling L-R177do): the next
//! acknowledgement on the link arrives under the forged [`PeerLabel`], body unchanged, so R1's
//! own check is what refuses it. Since 2026-10-02 the one forgery that lies inside the body is
//! delivered too: `authenticated: true` with a `claimed_role` other than the role the body
//! carries arrives with that role written into the body, and R1's role rule refuses it.
//!
//! Three faults change a body, and each only one field of it. [`Delivery::Corrupt`] (lead ruling
//! B-R75) flips one bit of an append's record digest in flight, so the receiver's own row 7 —
//! the digest must recompute — is what catches it, never a test-only guard. An authenticated
//! [`NetworkOp::ForgeAck`] rewrites an acknowledgement's role, and nothing else.
//! [`Delivery::OverstateDurable`] (2026-10-02) raises an acknowledgement's `durable` past its
//! `buffered_applied`, and nothing else.
//!
//! Every frame handed to [`Network::send`] leaves one entry in [`Network::transmissions`],
//! whatever its fate, and its bytes in [`Network::frames`]. A drop or a corruption is a fate the
//! scenario asked for by name, never a default, and it is written down like a delivery (ruling
//! B-R28).

use std::collections::BTreeMap;

use rdb_core::contracts::envelope::{
    AppendAck, AppendOutcome, ReplicaProgress, ReplicationEnvelope,
};
use rdb_core::contracts::ids::{DurableSeq, MessageId, NodeId, ReplicaRole};
use rdb_core::contracts::transport::{Frame, PeerLabel};
use rdb_core::replication::wire::{decode_reply, encode_reply};

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
    /// Deliver once, after `delay_millis`, with the record digest of the append it carries
    /// flipped in flight (lead ruling B-R75): one bit, the low bit of the digest's first byte.
    /// Every other byte arrives as sent, so the receiver's ladder reaches row 7 and finds a
    /// record whose digest does not recompute.
    ///
    /// The one fault that changes a body, and only this field of it. It counts only for a frame
    /// whose body decodes as a [`ReplicationEnvelope`] — an `Append`. Any other frame on the link
    /// passes the plan by, as an acknowledgement-less frame passes a [`NetworkOp::ForgeAck`], and
    /// a later plan for the pair decides that frame.
    Corrupt {
        /// How long in flight.
        delay_millis: u64,
    },
    /// Deliver once, after `delay_millis`, with the acknowledgement's `durable` raised to one
    /// past its `buffered_applied`: progress that contradicts itself, durable ahead of what was
    /// applied. Every other field arrives as sent, so R1's identity, lineage, role and boot rules
    /// pass it and its own ordering rule (rule 7) is what refuses it.
    ///
    /// Counts only for an acknowledgement, as [`NetworkOp::ForgeAck`] does. Any other frame on
    /// the link passes the plan by, and a later plan for the pair decides that frame.
    OverstateDurable {
        /// How long in flight.
        delay_millis: u64,
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
    /// An acknowledgement is a frame whose body R1's codec decodes as
    /// [`AppendOutcome::Accepted`]. Any other frame on the link passes this plan by: it waits
    /// for an acknowledgement, and a later plan for the pair decides the other frame. The
    /// acknowledgement arrives once, at once, under `PeerLabel{node: claimed_node, boot: the
    /// sender's, authenticated}` (lead ruling L-R177do). With `authenticated: false` the body is
    /// unchanged and `claimed_role` is never read, because the receiver refuses the label before
    /// it looks at a role. With `authenticated: true` the body carries `claimed_role`: an
    /// acknowledgement whose role is another one has that role, and only that field, rewritten
    /// with R1's own codec. The body's `from` is not rewritten, so a forgery that also claims
    /// another node is still refused on identity first; a role lie that is to reach the role
    /// rule names the real sender as `claimed_node`.
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

/// One copy of a frame the network will hand to its recipient.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arrival {
    /// How long after the send it arrives. The harness adds it to the current tick and nothing
    /// else (ruling B-R23).
    pub delay_millis: u64,
    /// The identity it arrives under: the sender's own label, or a forged one a plan asked for.
    pub label: PeerLabel,
    /// The frame, unchanged. A duplicate carries the same [`Frame::id`], which is what makes it
    /// recognisable as one.
    pub frame: Frame,
}

/// What the network did with one frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fate {
    /// It arrives, once or (for [`Delivery::Duplicate`]) twice, in arrival order.
    Delivered(Vec<Arrival>),
    /// A [`Delivery::Drop`] plan took it in flight. The sender is not told: a real network does
    /// not tell it either.
    Dropped,
    /// The link was [`LinkState::Partitioned`], so it never left. The harness reports this to
    /// the sender as a failed send.
    Partitioned,
}

/// One entry of [`Network::transmissions`]: a frame the network was handed, and its fate in
/// brief.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Transmission {
    /// Sender.
    pub from: NodeId,
    /// Recipient.
    pub to: NodeId,
    /// The frame's own id.
    pub id: MessageId,
    /// How many copies arrive: 0 for a drop or a partition, 2 for a duplicate.
    pub copies: u8,
    /// Whether the link was partitioned.
    pub partitioned: bool,
    /// Whether the copy that arrives carries a flipped record digest ([`Delivery::Corrupt`]).
    pub corrupted: bool,
}

/// The controlled network.
///
/// Holds its state and is not `Copy` (finding K-F-29). Every map is a `BTreeMap`: iteration
/// order is part of the trace.
#[derive(Debug, Default)]
pub struct Network {
    /// Link state, keyed by the ordered pair. A link never set is up.
    links: BTreeMap<(NodeId, NodeId), LinkState>,
    /// Per-frame plans not yet consumed, in injection order.
    planned: Vec<NetworkOp>,
    /// Every frame handed to [`Self::send`], in send order.
    transmissions: Vec<Transmission>,
    /// The frame of each entry of `transmissions`, at the same index, as it was sent.
    frames: Vec<Frame>,
}

impl Network {
    /// A network with every link up and no faults scheduled.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one scenario operation.
    ///
    /// [`NetworkOp::SetLink`] takes effect at once; every other operation is a plan for a future
    /// frame and is kept, in order, until that frame is sent.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `link` for an operation from a node to itself.
    pub fn inject(&mut self, op: NetworkOp) -> Result<(), SimError> {
        match op {
            NetworkOp::SetLink { a, b, state } => {
                if a == b {
                    return Err(SimError::Config { field: "link" });
                }
                self.links.insert(Self::pair(a, b), state);
                Ok(())
            }
            NetworkOp::PlanNext { from, to, .. }
            | NetworkOp::ForgeAck { from, to, .. }
            | NetworkOp::ForgeNext { from, to, .. } => {
                if from == to {
                    return Err(SimError::Config { field: "link" });
                }
                self.planned.push(op);
                Ok(())
            }
        }
    }

    /// The state of the link between two nodes. Up unless set otherwise.
    #[must_use]
    pub fn link(&self, a: NodeId, b: NodeId) -> LinkState {
        self.links
            .get(&Self::pair(a, b))
            .copied()
            .unwrap_or(LinkState::Up)
    }

    /// The plans not yet consumed, in injection order.
    #[must_use]
    pub fn planned(&self) -> &[NetworkOp] {
        &self.planned
    }

    /// Every frame handed to [`Self::send`], in send order, with its fate. A frame that was
    /// dropped or never left is here too: that is what makes a drop a recorded choice rather
    /// than an absence.
    #[must_use]
    pub fn transmissions(&self) -> &[Transmission] {
        &self.transmissions
    }

    /// The frame each entry of [`Self::transmissions`] carried, at the same index, exactly as it
    /// was handed to [`Self::send`]: before any [`Delivery::Corrupt`], whatever its fate.
    ///
    /// Every `Send` a kernel emits reaches the network, so this is the recorded send half of
    /// each step's effect vector (plan BA-4): an append's answer — `AlreadyHave`, `NeedPrefix`,
    /// a quarantine proof — is a reply frame here, correlated to its request by [`Frame::id`].
    /// A frame whose link was partitioned is here too.
    #[must_use]
    pub fn frames(&self) -> &[Frame] {
        &self.frames
    }

    /// Hand `frame` from `from` to `to`, sent under `label`, and say what happens to it.
    ///
    /// In this order:
    ///
    /// 1. A [`LinkState::Partitioned`] link: [`Fate::Partitioned`]. No plan is consumed; a plan
    ///    is about the next frame the link *carries*.
    /// 2. Otherwise the first plan for `(from, to)`, in injection order, is consumed:
    ///    [`NetworkOp::PlanNext`] decides the [`Delivery`], and [`NetworkOp::ForgeNext`] and
    ///    [`NetworkOp::ForgeAck`] deliver at once under their forged label. A `ForgeAck` or
    ///    [`Delivery::OverstateDurable`] plan counts only for an acknowledgement, and a
    ///    [`Delivery::Corrupt`] plan only for an append; every other frame passes each by.
    /// 3. With no plan, the frame arrives once, at once, under `label`. A delay or a drop is
    ///    something a scenario asks for, never a default.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `link` for a frame from a node to itself.
    ///
    /// [`Network::frames`] keeps the frame as it was handed in, before a forged role, an
    /// overstated durable position or a corruption, as it does for every fate.
    pub fn send(
        &mut self,
        from: NodeId,
        to: NodeId,
        label: PeerLabel,
        frame: Frame,
    ) -> Result<Fate, SimError> {
        if from == to {
            return Err(SimError::Config { field: "link" });
        }
        let id = frame.id;
        let mut corrupted = false;
        let fate = if self.link(from, to) == LinkState::Partitioned {
            Fate::Partitioned
        } else {
            let acknowledged = acknowledgement(&frame);
            // Decoded only when a corruption is planned on this link, so a run without one pays
            // nothing for it.
            let flipped = self
                .planned
                .iter()
                .any(|op| Self::corrupts(op, from, to))
                .then(|| flip_record_digest(&frame))
                .flatten();
            let at = self.planned.iter().position(|op| match *op {
                NetworkOp::PlanNext {
                    delivery: Delivery::Corrupt { .. },
                    ..
                } => Self::corrupts(op, from, to) && flipped.is_some(),
                NetworkOp::PlanNext {
                    from: f,
                    to: t,
                    delivery: Delivery::OverstateDurable { .. },
                } => (f, t) == (from, to) && acknowledged.is_some(),
                NetworkOp::PlanNext { from: f, to: t, .. }
                | NetworkOp::ForgeNext { from: f, to: t, .. } => (f, t) == (from, to),
                NetworkOp::ForgeAck { from: f, to: t, .. } => {
                    (f, t) == (from, to) && acknowledged.is_some()
                }
                NetworkOp::SetLink { .. } => false,
            });
            let plan = at.map(|at| self.planned.remove(at));
            let arrive = |delay_millis: u64, label: PeerLabel| Arrival {
                delay_millis,
                label,
                frame: frame.clone(),
            };
            match plan {
                Some(NetworkOp::PlanNext { delivery, .. }) => match delivery {
                    Delivery::Deliver { delay_millis } => {
                        Fate::Delivered(vec![arrive(delay_millis, label)])
                    }
                    Delivery::Drop => Fate::Dropped,
                    Delivery::Duplicate {
                        delay_millis,
                        second_delay_millis,
                    } => Fate::Delivered(vec![
                        arrive(delay_millis, label),
                        arrive(delay_millis.saturating_add(second_delay_millis), label),
                    ]),
                    Delivery::Corrupt { delay_millis } => {
                        corrupted = true;
                        Fate::Delivered(
                            flipped
                                .map(|frame| Arrival {
                                    delay_millis,
                                    label,
                                    frame,
                                })
                                .into_iter()
                                .collect(),
                        )
                    }
                    Delivery::OverstateDurable { delay_millis } => {
                        let Some(ack) = acknowledged else {
                            unreachable!("an OverstateDurable plan is taken only for an ack");
                        };
                        let progress = ReplicaProgress {
                            durable: DurableSeq(ack.progress.buffered_applied.0 + 1),
                            ..ack.progress
                        };
                        Fate::Delivered(vec![Arrival {
                            delay_millis,
                            label,
                            frame: Frame {
                                body: encode_reply(&AppendOutcome::Accepted(AppendAck {
                                    progress,
                                    ..ack
                                })),
                                ..frame.clone()
                            },
                        }])
                    }
                },
                Some(NetworkOp::ForgeNext { label: forged, .. }) => {
                    Fate::Delivered(vec![arrive(0, forged)])
                }
                Some(NetworkOp::ForgeAck {
                    claimed_node,
                    claimed_role,
                    authenticated,
                    ..
                }) => {
                    let label = PeerLabel {
                        node: claimed_node,
                        boot: label.boot,
                        authenticated,
                    };
                    // Only an authenticated forgery lies in the body, and only when the role
                    // differs: otherwise the frame is the sender's own, byte for byte. The plan
                    // was taken only because `acknowledged` decoded, so `None` here means exactly
                    // those two cases and nothing else.
                    let lie = acknowledged
                        .filter(|ack| authenticated && ack.role != claimed_role)
                        .map(|ack| Frame {
                            body: encode_reply(&AppendOutcome::Accepted(AppendAck {
                                role: claimed_role,
                                ..ack
                            })),
                            ..frame.clone()
                        });
                    Fate::Delivered(vec![match lie {
                        Some(frame) => Arrival {
                            delay_millis: 0,
                            label,
                            frame,
                        },
                        None => arrive(0, label),
                    }])
                }
                // `SetLink` is never kept as a plan.
                Some(NetworkOp::SetLink { .. }) | None => Fate::Delivered(vec![arrive(0, label)]),
            }
        };
        let copies = match &fate {
            Fate::Delivered(arrivals) => u8::try_from(arrivals.len()).unwrap_or(u8::MAX),
            Fate::Dropped | Fate::Partitioned => 0,
        };
        self.transmissions.push(Transmission {
            from,
            to,
            id,
            copies,
            partitioned: matches!(fate, Fate::Partitioned),
            corrupted,
        });
        self.frames.push(frame);
        Ok(fate)
    }

    /// Whether `op` is a [`Delivery::Corrupt`] plan for the link `from -> to`.
    fn corrupts(op: &NetworkOp, from: NodeId, to: NodeId) -> bool {
        matches!(
            *op,
            NetworkOp::PlanNext {
                from: f,
                to: t,
                delivery: Delivery::Corrupt { .. },
            } if (f, t) == (from, to)
        )
    }

    const fn pair(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
        if a.0 <= b.0 {
            (a, b)
        } else {
            (b, a)
        }
    }
}

/// The acknowledgement `frame` carries, when it is one: its body decodes under R1's codec as
/// [`AppendOutcome::Accepted`]. `None` for every other frame. Read only.
fn acknowledgement(frame: &Frame) -> Option<AppendAck> {
    match decode_reply(&frame.body) {
        Ok(AppendOutcome::Accepted(ack)) => Some(ack),
        _ => None,
    }
}

/// `frame` with the record digest of the append it carries flipped in one bit, when its body
/// decodes as a [`ReplicationEnvelope`]; `None` for every other frame. Only the digest moves:
/// the envelope is decoded and re-encoded with the codec R1 reads it with, so every other field
/// arrives as sent.
fn flip_record_digest(frame: &Frame) -> Option<Frame> {
    let mut envelope = ReplicationEnvelope::decode(&frame.body).ok()?;
    envelope.record_digest.0[0] ^= 1;
    let body = envelope.encode().ok()?;
    Some(Frame {
        body,
        ..frame.clone()
    })
}
