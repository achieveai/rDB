//! The qualification edge: the one fact R1 publishes when the publish predicate changes value.
//!
//! Contract types only. `qualifies_now` itself — the pinned copy set, the restart-resets-to-zero
//! rule, `min_regular_acks` — is package R1's (team kernel-b `design.md` §3.5). The shape is
//! here because two other modules consume it: P1 as the event filter in front of its own live
//! recheck, and L1 as the sole writer of `qualifies_now_at_head`.

use serde::{Deserialize, Serialize};

use crate::contracts::authority::Lineage;
use crate::contracts::ids::{ConfigVersion, Seq};
use crate::contracts::membership::CopyId;
use crate::contracts::time::Tick;

/// Which way the publish predicate moved (team kernel-b `design.md` §4.1, lead ruling A-R21).
///
/// **Two variants, and a third was considered and rejected** (lead ruling B-R27). With two
/// regular secondaries and `min_regular_acks = 1`, one of them diverging changes the qualifying
/// *set* but not the *predicate*, and no event is emitted at all: P1 reads the predicate live,
/// L1's arm is about the predicate, and the set change reaches L1 as `CopyLost` for the one use
/// L1 has for it. A set-only third direction would be an event whose only consumers ignore it.
///
/// [`Self::Lost`] is not `Disqualified`; team kernel-a withdrew that name itself, because one
/// event carrying a direction cannot drift out of sync with its twin the way two event names
/// can.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum QualificationDirection {
    /// The predicate became true.
    Gained,
    /// The predicate stopped being true.
    Lost,
}

/// Why the predicate moved (team kernel-b `design.md` §4.1).
///
/// **Trace only. No consumer branches on it** — P1's publish guard re-evaluates
/// `qualifies_now(cand.seq)` live and reads neither this nor `qualified_ack_count` (lead ruling
/// A-R21), and L1's only assignment reads `direction`. It exists so a JSONL row can say *why*
/// the edge happened instead of leaving an oracle to infer it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum QualificationCause {
    /// An acknowledgement crossed the threshold.
    AckAdvanced,
    /// A copy's history diverged (team kernel-b `design.md` §3.4 rule 9).
    DivergenceDetected(
        /// The diverged copy.
        CopyId,
    ),
    /// A copy was dropped for a stale boot, or a control-announced boot change zeroed it.
    StaleBoot(
        /// The copy whose boot moved.
        CopyId,
    ),
    /// A pinned-configuration change moved `min_regular_acks` or the member set.
    ConfigChanged,
}

/// `R1 -> L1/P1`: the publish predicate changed value (team kernel-b `design.md` §4.1, lead
/// rulings B-R22, B-R27, A-R21).
///
/// Edge-triggered on the predicate, not on the set and not periodic: R1 emits it from
/// `step(ProgressTracker, ..)` when and only when `qualifies_now(head)` changes value. H1
/// detects nothing and I1 only routes the effect back as an event, which keeps the edge
/// synchronous with the acknowledgement that caused it — the `no_qualifying_secondary` arm
/// fires in the same step-and-drain rather than an evaluation cadence later, which is the delay
/// spec §6.2 forbids.
///
/// A [`QualificationDirection::Gained`] is a notification, not a licence: P1 still re-evaluates
/// the three-conjunct publish test at publication time, so a `Lost` that races the recheck
/// cannot be outrun by event ordering. A `Lost` arriving *after* a publish changes nothing;
/// publication is irreversible and the late event is recorded as a fact.
///
/// Not `Copy`: [`Self::qualified_copies`] is a `Vec`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QualificationChanged {
    /// The lineage the predicate was evaluated under.
    ///
    /// The three-field [`Lineage`], **not** a seven-field lineage root (lead ruling R-S4). Two
    /// reasons, and the first is the one that bites: `qualifies` compares exactly three fields
    /// (team kernel-a `design.md` §1.6), and carrying seven while comparing three is the shape
    /// that keeps passing when a fourth differs. The base pair is R1's anchor, not P1's
    /// identity. The second is that `LineageRoot` is already a
    /// [`crate::contracts::trace::TraceKind`] variant carrying six of those seven fields, and a
    /// struct beside it would give one word two meanings in one crate.
    pub lineage: Lineage,
    /// The pinned configuration the predicate was evaluated against.
    pub config_version: ConfigVersion,
    /// The sequence the predicate was evaluated at.
    ///
    /// Compared by equality, never by ordering: P1 holds exactly one candidate at a time, so an
    /// event naming any other sequence is stale by construction and is dropped with a fact.
    pub at_seq: Seq,
    /// **The** decision field. The only field any consumer branches on.
    pub direction: QualificationDirection,
    /// The qualifying set at the edge. Trace only.
    pub qualified_copies: Vec<CopyId>,
    /// How many acknowledgements qualified at the edge. Trace only — `min_regular_acks` is R1's
    /// rule, and a consumer comparing against it here would be the second, weaker copy of that
    /// rule this seam exists to avoid.
    pub qualified_ack_count: u8,
    /// Why the edge happened. Trace only.
    pub cause: QualificationCause,
    /// When.
    pub tick: Tick,
}
