//! Mutation testing of the oracle itself (design §7).
//!
//! The oracle's own rows prove it fires on a trace written to be wrong. They do not prove it
//! would fire on a trace the *kernel* would produce if the kernel were wrong. A mutation does:
//! take a run the oracle called clean, break one declared fact, and require a named checker to
//! catch it. A mutation nothing catches is a checker that guards nothing.
//!
//! **Five mutations, and none of them touches kernel code** (decision D5). Three are trace
//! rewrites, here. The other two — a forged acknowledgement counted as a regular copy, and a
//! flush that reports success without syncing — are **sim-provider faults** rather than mutants
//! (ruling V-R9): they live in H1's network provider and M1's flush path, because spike §4's
//! transport seam and spike §6's storage boundary list already require both to be injectable.
//! A `cfg` branch in a kernel module would be a sixth copy of the protocol and is forbidden.
//!
//! Every rewrite below takes a recorded run and returns a new one. That is the opposite of what
//! [`super::reduce`] may do, and the two are deliberately different files: a reducer that
//! rewrote a trace would manufacture its own bug, while a mutator that rewrites one is the whole
//! point.

use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::ids::{ReplicaRole, Seq};
use rdb_core::contracts::trace::{
    AuthorityGate, AuthorityOutcome, DurabilityClass, Trace, TraceKind,
};

/// The named mutations. Row **M7V-71** asserts every variant maps to at least one catching row,
/// enumerated rather than hand-listed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MutationId {
    /// Accept stale authority: flip one publication-gate decision from `Expired` to `Valid`.
    Mut1AcceptStaleAuthority,
    /// Count a forged acknowledgement. A **provider fault**, not a rewrite: H1's `ForgeAck`.
    /// Its oracle half is reachable by rewrite until H1 lands, which is row M7V-81.
    Mut2CountForgedAck,
    /// Publish before the acknowledgement it rests on was generated.
    Mut3PublishBeforeAck,
    /// Skip an ancestry link: cite the digest recorded two positions back.
    Mut4SkipAncestry,
    /// Advance a durable watermark on a flush that synced nothing. A **provider fault**: M1's
    /// `FalseDurable`.
    Mut5FalseDurableWatermark,
}

impl MutationId {
    /// Every mutation, in declaration order.
    pub const ALL: [Self; 5] = [
        Self::Mut1AcceptStaleAuthority,
        Self::Mut2CountForgedAck,
        Self::Mut3PublishBeforeAck,
        Self::Mut4SkipAncestry,
        Self::Mut5FalseDurableWatermark,
    ];

    /// The name written into `rdb-m7-campaign.json`'s `mutations{}` map.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Mut1AcceptStaleAuthority => "MUT-1",
            Self::Mut2CountForgedAck => "MUT-2",
            Self::Mut3PublishBeforeAck => "MUT-3",
            Self::Mut4SkipAncestry => "MUT-4",
            Self::Mut5FalseDurableWatermark => "MUT-5",
        }
    }

    /// The rows that must catch it. MUT-2 has **two**: the kernel half that proves the forgery
    /// is rejected, and the oracle half that proves the checker fires when it is not.
    #[must_use]
    pub const fn catching_rows(self) -> &'static [&'static str] {
        match self {
            Self::Mut1AcceptStaleAuthority => &["m7v_66"],
            Self::Mut2CountForgedAck => &["m7v_69", "m7v_81"],
            Self::Mut3PublishBeforeAck => &["m7v_67"],
            Self::Mut4SkipAncestry => &["m7v_68"],
            Self::Mut5FalseDurableWatermark => &["m7v_70"],
        }
    }

    /// Whether this mutation is a trace rewrite or a provider fault (ruling V-R9).
    #[must_use]
    pub const fn is_trace_rewrite(self) -> bool {
        matches!(
            self,
            Self::Mut1AcceptStaleAuthority | Self::Mut3PublishBeforeAck | Self::Mut4SkipAncestry
        )
    }
}

/// Whether a rewrite found anything to change. A mutation that silently changed nothing would
/// report "the oracle caught it" from a trace that was never mutated, which is the one way a
/// mutation row can lie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mutated {
    /// The rewritten run.
    pub trace: Trace,
    /// How many events the rewrite touched. A row asserts this is exactly 1.
    pub touched: usize,
}

/// MUT-1: flip the first publication-gate `authority_decision` that came out `Expired` to
/// `Valid`, leaving the `publish` that follows it alone.
///
/// INV-AUTH catches it at the decision itself — a grant that came out `Valid` at a tick its own
/// window does not cover is a contradiction inside one event, and no later event is needed.
#[must_use]
pub fn mut1_accept_stale_authority(trace: &Trace) -> Mutated {
    let mut trace = trace.clone();
    let mut touched = 0;
    for event in &mut trace.events {
        if let TraceKind::AuthorityDecision { gate, outcome, .. } = &mut event.kind {
            if touched == 0
                && *gate == AuthorityGate::Publication
                && *outcome == AuthorityOutcome::Expired
            {
                *outcome = AuthorityOutcome::Valid;
                touched += 1;
            }
        }
    }
    Mutated { trace, touched }
}

/// MUT-3: move the first `publish` to before the `replication_ack` it rests on.
///
/// Implemented as a move of the publish event, with `event_id`s left where they are and then
/// renumbered, because the oracle folds in `event_id` order and a rewrite that only reordered
/// the vector would change nothing it reads.
///
/// INV-PUB catches it through the existing cardinality clause rather than a separate ordering
/// rule: a publication counts an acknowledgement only if that acknowledgement was **already
/// generated and accepted**, so moving the publish earlier drops the count below
/// `min_regular_acks`.
#[must_use]
pub fn mut3_publish_before_ack(trace: &Trace) -> Mutated {
    let mut trace = trace.clone();
    let Some(publish_at) = trace
        .events
        .iter()
        .position(|event| matches!(event.kind, TraceKind::Publish { .. }))
    else {
        return Mutated { trace, touched: 0 };
    };
    let Some(ack_at) = trace.events[..publish_at]
        .iter()
        .position(|event| matches!(event.kind, TraceKind::ReplicationAck { .. }))
    else {
        return Mutated { trace, touched: 0 };
    };

    let publish = trace.events.remove(publish_at);
    trace.events.insert(ack_at, publish);
    renumber(&mut trace);
    Mutated { trace, touched: 1 }
}

/// MUT-4: point the first `batch_apply` whose predecessor is at least two positions back at the
/// digest recorded at `seq - 2`, skipping a link in the chain.
///
/// INV-LIN catches it: the cited predecessor digest is not the one recorded at the cited
/// predecessor sequence.
#[must_use]
pub fn mut4_skip_ancestry(trace: &Trace) -> Mutated {
    let mut trace = trace.clone();
    let recorded: Vec<(Seq, Digest)> = trace
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::BatchApply {
                seq, entry_digest, ..
            } => Some((*seq, *entry_digest)),
            _ => None,
        })
        .collect();

    let mut touched = 0;
    for event in &mut trace.events {
        if touched > 0 {
            break;
        }
        if let TraceKind::BatchApply {
            seq,
            predecessor_digest,
            ..
        } = &mut event.kind
        {
            let two_back = Seq(seq.0.saturating_sub(2));
            if two_back.0 + 2 != seq.0 {
                continue;
            }
            let Some((_, digest)) = recorded.iter().find(|(at, _)| *at == two_back) else {
                continue;
            };
            if *predecessor_digest == *digest {
                continue;
            }
            *predecessor_digest = *digest;
            touched += 1;
        }
    }
    Mutated { trace, touched }
}

/// MUT-2's oracle half, until H1's `ForgeAck` lands (row M7V-81).
///
/// Takes the first **refused** acknowledgement, elevates its claimed role to `RegularSecondary`,
/// removes the refusal, and makes the publication count it.
///
/// The elevation is the mutation. A refused acknowledgement that claims the role its node really
/// holds is an honest record, which is what lets the pre-mutation run be clean; forging the claim
/// is what INV-PUB has to catch. The topology is left alone on purpose: the disagreement between
/// what the frame claimed and what the environment declared is the whole signal, and editing the
/// topology would erase it.
#[must_use]
pub fn mut2_count_forged_ack(trace: &Trace) -> Mutated {
    let mut trace = trace.clone();
    let mut forged = None;
    for event in &mut trace.events {
        if let TraceKind::ReplicationAck {
            from_node,
            peer_role,
            accepted,
            reject_reason,
            durability_class,
            ..
        } = &mut event.kind
        {
            if forged.is_none() && !*accepted {
                *peer_role = ReplicaRole::RegularSecondary;
                *accepted = true;
                *reject_reason = None;
                *durability_class = DurabilityClass::Durable;
                forged = Some(*from_node);
            }
        }
    }
    let Some(node) = forged else {
        return Mutated { trace, touched: 0 };
    };
    for event in &mut trace.events {
        if let TraceKind::Publish { ack_evidence, .. } = &mut event.kind {
            if !ack_evidence.iter().any(|entry| entry.node == node) {
                ack_evidence.push(rdb_core::contracts::trace::AckEvidence {
                    node,
                    boot: event.boot,
                    role: ReplicaRole::RegularSecondary,
                    durability: DurabilityClass::Durable,
                });
            }
        }
    }
    Mutated { trace, touched: 1 }
}

/// Reassign `event_id` so it is strictly increasing again after a move.
fn renumber(trace: &mut Trace) {
    for (index, event) in trace.events.iter_mut().enumerate() {
        event.event_id = rdb_core::contracts::ids::EventId(index as u64 + 1);
    }
}
