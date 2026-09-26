//! The recovery seam: what F1 decided, what it could prove, and what it could not.
//!
//! Contract types only. Lineage selection, the discovery window and the mode table are package
//! F1's rules (team kernel-b `design.md` §5.2 to §5.8); the shapes are here because three
//! modules consume the answer — A1 takes the committed revision, R1 resets its receivers and
//! trackers from the new root, and P1 takes the mode and the retained-status bounds.
//!
//! Distinct from [`crate::recovery`], which is the F1 kernel module itself.
//!
//! # The two rules this module encodes rather than documents
//!
//! * [`RecoveryBarrier`] cannot be built from a sequence number. Its only ingredient is a
//!   [`DurableProof`], and [`RecoveryBarrier::try_new`] is fallible on purpose.
//! * Nothing here says whether a client received its reply. Spec §8.1: *"Records cannot show
//!   whether the client received its reply. Select using validated ancestry, not inferred
//!   client ACK status."* [`LossRecord`] has no field for it and [`RetainedStatusMap`] exposes
//!   bounds rather than verdicts — the mapping from those bounds to a client-visible status
//!   stays in P1.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::contracts::authority::{
    AuthorityView, BlockReason, FenceCredential, FencingProof, Lineage, PartitionMode,
};
use crate::contracts::digest::Digest;
use crate::contracts::ids::{DurableSeq, Generation, PartitionId, Revision, Seq};
use crate::contracts::membership::{CopyId, PartitionConfig};
use crate::contracts::time::Tick;
use crate::contracts::trace::QuarantineReason;

/// One copy's evidence that a prefix reached disk (team kernel-b `design.md` §1.2, lead ruling
/// B-R13).
///
/// Produced only by a successful `flush_wal(true)` over the captured prefixes. A memtable flush
/// is not a substitute, and a `FlushFailed` yields none of these — which is the charter DO-NOT,
/// "`Durable` is never an alias for applied", carried by the [`DurableSeq`] newtype rather than
/// by a comment.
///
/// # The `copy` field, and why it is here rather than alongside
///
/// Team kernel-b's design spells this struct with three fields. The fourth is forced by
/// [`RecoveryBarrier::try_new`], whose signature is frozen (lead ruling R-S5): its coverage
/// check is *every copy in `required` produced a proof*, and two of its four failure arms name a
/// [`CopyId`]. A slice of proofs that do not say whose they are cannot answer that question, so
/// either the proof names its copy or the signature changes. The signature is the frozen half.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DurableProof {
    /// Whose proof this is.
    pub copy: CopyId,
    /// The partition it is about.
    pub partition: PartitionId,
    /// The last sequence confirmed on disk.
    pub seq: DurableSeq,
    /// The digest at that position, so a proof cannot be reused against a different history.
    pub digest: Digest,
}

/// Why a proof set does not establish a barrier (team kernel-b `design.md` §5.6).
///
/// A returned value, never a panic: [`RecoveryBarrier::try_new`] is pure, so F1 stays in its
/// `Barrier` phase and waits for more durability events.
///
/// It carries `Display` because deserialisation goes through [`RecoveryBarrier::try_new`] and
/// serde needs to render the refusal. Every field it prints is a slot index, a sequence or a
/// truncated digest — never a key, a value, or anything a caller supplied.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    thiserror::Error,
)]
pub enum MissingProof {
    /// A required copy produced no proof at all. The **coverage** check: every copy in
    /// `required`, not "some proofs".
    #[error("no durable proof from copy {0:?}")]
    NoProofFrom(
        /// The copy that produced nothing.
        CopyId,
    ),
    /// The **reach** check. A proof of durability at sequence 90 says nothing about a barrier at
    /// 100.
    #[error("copy {copy:?} proved durability at {proof_seq:?}, below the cutoff {cutoff:?}")]
    ProofBelowCutoff {
        /// Whose proof.
        copy: CopyId,
        /// How far it actually reached.
        proof_seq: DurableSeq,
        /// How far it had to reach.
        cutoff: Seq,
    },
    /// The **binding** check. Durable at sequence 100 *of a different history* is not durable at
    /// our cutoff; without this the barrier is the "longest wins by number" mistake moved one
    /// layer down.
    #[error("copy {copy:?} proved {proof_digest:?}, not the cutoff digest {cutoff_digest:?}")]
    ProofDigestMismatch {
        /// Whose proof.
        copy: CopyId,
        /// The digest it carries.
        proof_digest: Digest,
        /// The digest the cutoff requires.
        cutoff_digest: Digest,
    },
    /// A proof arrived from a copy that is not in `required`.
    #[error("durable proof from copy {0:?}, which is not required")]
    UnknownCopy(
        /// The stranger.
        CopyId,
    ),
}

/// Proof that every required copy holds the selected cutoff, durably and in our history (team
/// kernel-b `design.md` §5.6).
///
/// # Unconstructable from a sequence number, on purpose (finding K-B-09)
///
/// The fields are private and [`Self::try_new`] is the only constructor. An infallible
/// `From<Set<DurableProof>>` was the draft, and it is infallible *by its own signature*: the
/// empty set, a set missing a required copy, and a set of proofs all below the cutoff each
/// produced a barrier that typechecked and meant nothing. A private constructor that cannot
/// fail only proves its input was of the right type.
///
/// Deserialisation goes back through [`Self::try_new`] as well, by the same
/// `#[serde(try_from = ...)]` route [`PartitionConfig`] uses: a barrier decoded from a trace is
/// re-checked rather than trusted, because otherwise the wire is a constructor that cannot
/// fail.
///
/// > "A fallible ctor that is never tested failing is an infallible ctor."
/// > — team kernel-b `design.md` §7
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "UnvalidatedRecoveryBarrier")]
pub struct RecoveryBarrier {
    cutoff: Seq,
    cutoff_digest: Digest,
    required: Vec<CopyId>,
    proofs: Vec<DurableProof>,
}

/// [`RecoveryBarrier`]'s wire shape, before [`RecoveryBarrier::try_new`] has run.
///
/// Serde's `try_from` needs a type it may build while the invariant is still unknown, so the
/// fields are repeated here and nowhere else. The three checks are not repeated: the conversion
/// below calls `try_new`, the one place that states them. A field added to `RecoveryBarrier` and
/// not to this struct fails to compile in that conversion, so the two cannot drift apart
/// unnoticed.
#[derive(Deserialize)]
#[serde(rename = "RecoveryBarrier")]
struct UnvalidatedRecoveryBarrier {
    cutoff: Seq,
    cutoff_digest: Digest,
    required: Vec<CopyId>,
    proofs: Vec<DurableProof>,
}

impl TryFrom<UnvalidatedRecoveryBarrier> for RecoveryBarrier {
    type Error = MissingProof;

    fn try_from(wire: UnvalidatedRecoveryBarrier) -> Result<Self, Self::Error> {
        let required: BTreeSet<CopyId> = wire.required.into_iter().collect();
        Self::try_new(&wire.proofs, &required, wire.cutoff, wire.cutoff_digest)
    }
}

impl RecoveryBarrier {
    /// The only constructor. Fallible, and its three checks are the barrier's whole content
    /// (finding K-B-09, lead ruling R-S5).
    ///
    /// 1. **Coverage** — every copy in `required` has a proof.
    /// 2. **Reach** — every proof reaches `cutoff`.
    /// 3. **Binding** — every proof carries `cutoff_digest`.
    ///
    /// # Errors
    ///
    /// [`MissingProof`], naming the copy and the check that failed. Checks run in that order
    /// and the first failure is returned, so a caller re-running after supplying one more proof
    /// makes progress rather than being told the same thing about a different copy.
    pub fn try_new(
        proofs: &[DurableProof],
        required: &BTreeSet<CopyId>,
        cutoff: Seq,
        cutoff_digest: Digest,
    ) -> Result<Self, MissingProof> {
        for proof in proofs {
            if !required.contains(&proof.copy) {
                return Err(MissingProof::UnknownCopy(proof.copy));
            }
        }
        for copy in required {
            let Some(proof) = proofs.iter().find(|candidate| candidate.copy == *copy) else {
                return Err(MissingProof::NoProofFrom(*copy));
            };
            // `.0` on both sides is the friction the watermark newtypes exist for: there is no
            // conversion between `DurableSeq` and `Seq`, so comparing them is a line a reviewer
            // sees rather than an assignment that reads correctly.
            if proof.seq.0 < cutoff.0 {
                return Err(MissingProof::ProofBelowCutoff {
                    copy: *copy,
                    proof_seq: proof.seq,
                    cutoff,
                });
            }
            if proof.digest != cutoff_digest {
                return Err(MissingProof::ProofDigestMismatch {
                    copy: *copy,
                    proof_digest: proof.digest,
                    cutoff_digest,
                });
            }
        }
        let mut held: Vec<DurableProof> = proofs.to_vec();
        held.sort_unstable();
        Ok(Self {
            cutoff,
            cutoff_digest,
            required: required.iter().copied().collect(),
            proofs: held,
        })
    }

    /// The position every required copy holds durably.
    #[must_use]
    pub const fn cutoff(&self) -> Seq {
        self.cutoff
    }

    /// The digest that binds [`Self::cutoff`] to one history.
    #[must_use]
    pub const fn cutoff_digest(&self) -> Digest {
        self.cutoff_digest
    }

    /// The copies the barrier was required over, in ascending order.
    #[must_use]
    pub fn required(&self) -> &[CopyId] {
        &self.required
    }

    /// The proofs that established it, in ascending order.
    #[must_use]
    pub fn proofs(&self) -> &[DurableProof] {
        &self.proofs
    }
}

/// Why a survivor's prefix did not become selection input (team kernel-b `design.md` §5.3,
/// §5.5).
///
/// One enum for both the inventory outcome and [`LossRecord::unavailable`], because a reader
/// asking "which copies did we lose and why" wants one vocabulary, not two that overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum UnavailableReason {
    /// The survivor's `lineage_root_seen` is not the committed root: an older or a foreign
    /// history. **Not divergence** — nothing has been proved about a shared position.
    StaleLineage,
    /// The survivor reported itself quarantined. Kept as evidence, never selectable.
    Quarantined,
    /// The source stopped making progress inside the discovery window, or was still dribbling
    /// when the extension cap was hit. From recovery's point of view "too slow to finish inside
    /// the budget" and "stopped" have the same consequence, and both must set
    /// [`LossRecord::uncertain`].
    Stalled,
}

/// What happened to one survivor's inventory (team kernel-b `design.md` §5.2 to §5.5).
///
/// Every copy asked appears exactly once, whatever the answer. Recording the failures is not
/// bookkeeping: spike §6 requires F1 to *record source failure before choosing a shorter
/// prefix*, and a copy that never answered must be recorded as unavailable rather than left
/// absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum InventoryOutcome {
    /// Ancestry verified against the committed root. This copy is selection input.
    Verified {
        /// Whose inventory.
        copy: CopyId,
    },
    /// Ancestry rejected the inventory. Recorded, never selectable.
    Ineligible {
        /// Whose inventory.
        copy: CopyId,
        /// Which rejection.
        reason: UnavailableReason,
    },
    /// The source did not answer, or stopped answering.
    Failed {
        /// Whose inventory.
        copy: CopyId,
        /// Why it is not here.
        reason: UnavailableReason,
    },
}

/// The prefix F1 chose, and whose it was (team kernel-b `design.md` §5.4, §5.8).
///
/// Chosen by validated ancestry, never by sequence length alone: every candidate passed
/// `verify_ancestry` against the committed root, and the pairwise compatibility loop proved no
/// two candidates diverge from *each other* above it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SelectedLineage {
    /// The lineage now served.
    ///
    /// The three-field [`Lineage`] (lead ruling R-S4). The base pair a seven-field root would
    /// also carry is [`Self::cutoff_seq`] and [`Self::cutoff_digest`] — the new lineage starts
    /// at the cutoff — and the predecessor pair is on
    /// [`RetainedStatusMap`]. Spelling them a second time here would put four fields in two
    /// places and make the two copies a thing a reviewer has to check.
    pub root: Lineage,
    /// The last sequence the selected prefix includes. T1 rebases from this and
    /// [`Self::cutoff_digest`].
    pub cutoff_seq: Seq,
    /// The digest at [`Self::cutoff_seq`], binding the cutoff to one history.
    pub cutoff_digest: Digest,
    /// The copy whose prefix was selected — the *holder*, which spec §8.3 says need not be the
    /// copy that ends up leading, and need not be the node F1 runs on.
    pub source: CopyId,
}

/// What the activation CAS committed, so a consumer re-anchors without a second control read
/// (team kernel-b `design.md` §5.8).
///
/// One CAS on one key (finding K-B-20): generation, owner, `owner_epoch` and the base pair all
/// live in the single `partitions/{id}` record, and the proposal replaces that record whole.
/// Spec §7.1 gives no multi-key transaction, so anything a reader needs that is not in this
/// record must be *derived* from it rather than fetched from a second key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CommittedRoot {
    /// The revision the CAS committed at. A1 consumes this one.
    pub revision: Revision,
    /// The membership now pinned.
    pub pinned_config: PartitionConfig,
    /// The authority state now published.
    pub authority_view: AuthorityView,
}

/// The seq bounds P1 needs to answer a status query about the previous generation (team
/// kernel-b `design.md` §5.8).
///
/// A pair of bounds plus an uncertainty flag, **not** a per-request table: the mapping from a
/// request identity to a sequence is T1's dedup index, which F1 does not read. P1 combines the
/// two.
///
/// **The mapping to client-visible status codes stays in P1.** F1 exposes bounds and draws no
/// conclusion about what a client saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RetainedStatusMap {
    /// The generation the identities in question were recorded under.
    pub predecessor_generation: Generation,
    /// Where the predecessor was cut off.
    pub predecessor_cutoff: Seq,
    /// Records at or below this sequence survived the cutoff. Equal to
    /// [`SelectedLineage::cutoff_seq`].
    pub retained_through: Seq,
    /// The first sequence that was dropped, when anything above the cutoff was.
    ///
    /// `Option<Seq>`, and that is binding (lead ruling R-S5 §4): `None` means nothing above the
    /// cutoff was discarded, which is a different answer from "the first discarded sequence is
    /// zero". A bare `Seq` would collapse the two and delete a status case P1 has no other
    /// coverage of.
    pub discarded_from: Option<Seq>,
    /// Whether a suffix may have been lost without proof either way. Equal to
    /// [`LossRecord::uncertain`].
    pub uncertain: bool,
}

/// What recovery could not prove about the records above the cutoff (team kernel-b `design.md`
/// §5.6).
///
/// Recorded before any shorter prefix is chosen, never after: spike §6 requires F1 to *record
/// source failure before choosing a shorter prefix and preserve loss uncertainty*, and the
/// ordering is checkable by effect index because effects are a deterministic vector.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LossRecord {
    /// Every copy asked.
    pub queried: Vec<CopyId>,
    /// Every copy that did not contribute a usable prefix, and why.
    pub unavailable: Vec<(CopyId, UnavailableReason)>,
    /// The prefix actually selected.
    pub cutoff_seq: Seq,
    /// The highest prefix any source advertised, whether or not it arrived.
    pub highest_advertised_seq: Seq,
    /// `highest_advertised_seq > cutoff_seq`. Whether a suffix may have been lost.
    ///
    /// There is no field here for whether a client received its reply, on purpose (spec §8.1).
    pub uncertain: bool,
}

/// `F1 -> A1/R1/P1`: everything one recovery decided (team kernel-b `design.md` §5.8, spike §4).
///
/// A1 consumes `committed.revision`. R1 resets its receivers and trackers from `selected.root`,
/// `committed.pinned_config` and `committed.authority_view`. T1 rebases from
/// `selected.cutoff_seq` and `selected.cutoff_digest`. P1 consumes `mode` and
/// `retained_status_map`, and matches `mode` totally.
///
/// It is also the announcement of a *later* mode change: a partition rebuilt back to `Active`
/// emits a second one of these with `mode: Active`, which is the only announcement T1 and P1
/// get that they may leave `RecoveryReadOnly` behind. Without it a successfully rebuilt
/// partition refuses writes forever.
///
/// Not `Copy`: four fields are `Vec`-backed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecoveryResult {
    /// The proof that authorised the takeover. The only door into recovery.
    pub fenced_prior: FencingProof,
    /// Every survivor asked, and what came of it — verified, ineligible or failed, all
    /// recorded.
    pub inventories: Vec<InventoryOutcome>,
    /// The prefix chosen.
    pub selected: SelectedLineage,
    /// The lineage incarnation this recovery created.
    pub new_generation: Generation,
    /// The mode the partition is now in.
    ///
    /// The **shared** [`PartitionMode`] (finding K-B-19), defined once in this crate — not a
    /// kernel-b-private enum plus a kernel-a-private one that happen to have the same variants
    /// today. Two enums with the same variants are two enums that drift, and the drift shows up
    /// as a mode P1 does not handle.
    pub mode: PartitionMode,
    /// Proof that every required copy holds [`SelectedLineage::cutoff_seq`] durably.
    pub barrier: RecoveryBarrier,
    /// What could not be proved about anything above the cutoff.
    pub loss: LossRecord,
    /// What the activation CAS committed.
    pub committed: CommittedRoot,
    /// The bounds P1 needs for a status query against the previous generation.
    pub retained_status_map: RetainedStatusMap,
}

// ---- F1 event and effect vocabulary (lead ruling B-R35, 2026-09-22) ------------------------
//
// Landed verbatim from package F1's contract ask (`recovery/vocab.rs`). Carried by
// `KernelEvent::Recovery` and `KernelEffect::Recovery`, one arm each, owned by kernel-b.

/// The committed root a survivor's history must descend from: the lineage, and the position
/// and digest that history begins at (team kernel-b `design.md` §5.2, §5.3).
///
/// Not `LineageRoot`: that name is a `TraceKind` variant (seam-freeze R-S4). It keeps the base
/// pair on purpose — M7B-88's claim is a digest mismatch *at `base_seq`* (seam-freeze §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LineageAnchor {
    /// The partition, generation and owner epoch of the root.
    pub lineage: Lineage,
    /// Where the root's history begins.
    pub base_seq: Seq,
    /// The record digest at `base_seq`.
    pub base_digest: Digest,
}

/// What one survivor reports about its own history (`design.md` §5.2).
///
/// No `eligible` flag (K-B-26), and no self-reported role or boot: both are read from the pinned
/// [`PartitionConfig`], because a survivor does not get to assert what it is.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SurvivorInventory {
    /// Whose inventory this is.
    pub copy: CopyId,
    /// The committed root this copy believes its history descends from.
    pub anchor_seen: LineageAnchor,
    /// Its highest complete record: position and digest.
    pub head: (Seq, Digest),
    /// Sparse ancestry evidence: a fixed stride plus the durable point. One matching pair proves
    /// the whole shared prefix, because each record digest chains its predecessor.
    pub ladder: Vec<(Seq, Digest)>,
    /// Set when the copy holds a quarantined history. Kept as evidence, never selectable.
    pub quarantined: Option<QuarantineReason>,
}

/// Placement's view of one copy as a leader candidate (`design.md` §5.4). Data, not logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Candidate {
    /// The copy.
    pub copy: CopyId,
    /// It may hold the primary role.
    pub primary_eligible: bool,
    /// It is healthy.
    pub healthy: bool,
    /// It has capacity for the partition.
    pub within_capacity: bool,
    /// Its node holds a valid grant.
    pub has_valid_grant: bool,
}

/// The data a recovery runs over, supplied by the environment before a fence (placement and the
/// control plane own it; F1 has no placement logic, `design.md` §5.9).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecoveryPlan {
    /// The committed root every survivor is verified against.
    pub anchor: LineageAnchor,
    /// The membership queried; each copy's role is read from here, never from its report.
    pub config: PartitionConfig,
    /// Leader candidates, in placement's order of preference.
    pub candidates: Vec<Candidate>,
    /// Copies that must hold the rebuild barrier before activation (`design.md` §5.6a).
    pub rebuild_required: BTreeSet<CopyId>,
    /// The authority view the new owner holds; F1 writes the new lineage into it.
    pub authority_view: AuthorityView,
    /// How long a returning stale owner's suffix is kept (spec §8.4: seven days).
    pub retention_millis: u64,
}

/// Why a recovery quarantined (`design.md` §5.3, §5.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DivergenceEvidence {
    /// A survivor's ladder does not hold the root's digest at `base_seq`.
    RootMismatch {
        /// The survivor.
        copy: CopyId,
        /// The root's base position.
        base_seq: Seq,
        /// The root's digest there.
        expected: Digest,
        /// What the survivor holds there, or `None` when its ladder has no rung at `base_seq`.
        found: Option<Digest>,
    },
    /// Two survivors hold different digests at one position above the root.
    Pairwise {
        /// The position.
        seq: Seq,
        /// The shorter survivor and its digest.
        a: (CopyId, Digest),
        /// The longer survivor and its digest.
        b: (CopyId, Digest),
    },
}

/// Inputs to F1, delivered as `KernelEvent::Recovery`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum RecoveryEvent {
    /// The environment supplies the data a recovery runs over. Accepted in `Idle` only.
    Plan(Box<RecoveryPlan>),
    /// A1's `AuthorityEffect::FenceProven`, delivered. The only way out of `Idle` (§2.1).
    ///
    /// Boxed for the reason `KernelEvent::Recovered` is (lead ruling B-R35): unboxed, the
    /// 128-byte proof set the size of every `EventKind` in the run queue, for an event that
    /// happens once per fencing.
    FenceProven(Box<FencingProof>),
    /// A queried copy's inventory.
    InventoryReported(Box<SurvivorInventory>),
    /// A queried copy could not report.
    InventoryFailed {
        /// The copy.
        copy: CopyId,
    },
    /// A source is still sending its advertised prefix (`design.md` §5.5).
    TransferProgress {
        /// The source.
        copy: CopyId,
        /// The head it advertises.
        advertised_seq: Seq,
        /// How far the transfer has got.
        received_seq: Seq,
    },
    /// The answer to a `ProbeDigestAt`.
    ProbeAnswered {
        /// Whose ladder was probed.
        copy: CopyId,
        /// Where.
        seq: Seq,
        /// The digest it holds there.
        digest: Digest,
    },
    /// A `ProbeDigestAt` the copy could not answer.
    ProbeUnavailable {
        /// Whose ladder was probed.
        copy: CopyId,
        /// Where.
        seq: Seq,
    },
    /// A catch-up target holds the prefix through `head`.
    CopyCaughtUp {
        /// The target.
        copy: CopyId,
        /// Its head after catch-up.
        head: Seq,
        /// The digest at `head`.
        digest: Digest,
    },
    /// A copy's WAL is durable through a position.
    DurableAt(DurableProof),
    /// A copy of the prior owner came back. Routed to quarantine after commit; an ordinary
    /// survivor before it (`design.md` §5.7).
    StaleOwnerReturned(Box<SurvivorInventory>),
}

/// Outputs of F1, emitted as `KernelEffect::Recovery`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum RecoveryEffect {
    /// Ask these copies for their inventory.
    QueryInventory {
        /// Every copy of the pinned config.
        copies: Vec<CopyId>,
    },
    /// Ask a copy for its digest at a position.
    ProbeDigestAt {
        /// Whose ladder.
        copy: CopyId,
        /// Where.
        seq: Seq,
    },
    /// A source is recorded as lost to this recovery. Always before `CloseWindow` in one vector.
    RecordSourceUnavailable {
        /// The source.
        copy: CopyId,
        /// Why.
        reason: UnavailableReason,
    },
    /// Discovery is over; selection runs on what was collected.
    CloseWindow,
    /// The prefix selection decided.
    Selected(SelectedLineage),
    /// Divergence evidence, kept for the operator.
    Quarantine(DivergenceEvidence),
    /// Automatic promotion is blocked, and why.
    BlockPromotion {
        /// Why.
        reason: BlockReason,
    },
    /// Copy the selected prefix to a lagging holder.
    CatchUp {
        /// The source holding the prefix.
        from: CopyId,
        /// The lagging copy.
        to: CopyId,
        /// Through this position.
        through: Seq,
        /// Minted with `sender == from` (K-B-42).
        credential: FenceCredential,
    },
    /// Copy the selected prefix to the chosen leader before it is granted ownership (spec §8.3).
    CatchUpBeforeGrant {
        /// The prefix holder.
        from: CopyId,
        /// The chosen leader.
        to: CopyId,
        /// Through this position.
        through: Seq,
        /// Minted with `sender == from` (K-B-42).
        credential: FenceCredential,
    },
    /// Make a copy's WAL durable through a position, then report `DurableAt`.
    SyncWalThrough {
        /// The copy.
        copy: CopyId,
        /// Through this position.
        cutoff: Seq,
    },
    /// A required copy was lost while rebuilding; activation waits for it or a replacement.
    RebuildStalled {
        /// The lost copy.
        copy: CopyId,
    },
    /// Keep a returning stale owner's suffix, quarantined, until `until`. No delete effect
    /// exists (spec §8.4).
    QuarantineSuffix {
        /// The stale owner's copy.
        copy: CopyId,
        /// The first quarantined position: the committed cutoff plus one.
        from: Seq,
        /// Retention end: the return's tick plus `retention_millis`.
        until: Tick,
    },
    /// Rebuild a stale copy from the committed root.
    RebuildFromAuthoritative {
        /// The stale copy.
        copy: CopyId,
        /// The committed root.
        root: LineageAnchor,
    },
}
