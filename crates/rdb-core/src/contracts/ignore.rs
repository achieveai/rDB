//! Why a kernel module ignored an event: the namespaced carrier, and kernel-b's leaf.
//!
//! Ask CB-7. [`crate::contracts::event::KernelEffect::Ignored`] used to carry an
//! [`ErrorKind`], which is the client-facing vocabulary of spec §5.4 — eighteen names, none of
//! them a kernel fact. Between them the two kernel teams name **43** distinct reasons, and
//! forcing those through `ErrorKind` would have meant either eighteen wrong answers or a
//! foundation edit per kernel fact.
//!
//! # The shape, and whose names are whose
//!
//! One arm per owning vocabulary. Foundation owns [`KernelIgnoredReason`]'s **arm set**; each
//! arm's leaf is owned by whoever names its variants. This is the shape
//! [`crate::contracts::envelope::AppendReject`] already uses one level up — one variant holding
//! an enum the consuming team fills.
//!
//! | Arm | Leaf | Who edits the variants |
//! |---|---|---|
//! | [`KernelIgnoredReason::Error`] | [`ErrorKind`] | foundation, and only when spec §5.4 changes |
//! | [`KernelIgnoredReason::AppendRejected`] | [`AppendReject`] | kernel-b — its append ladder |
//! | [`KernelIgnoredReason::AckRejected`] | [`AckRejectReason`] | kernel-b — its ack tracker |
//! | [`KernelIgnoredReason::Authority`] | [`AuthorityIgnoreReason`] | kernel-a |
//! | [`KernelIgnoredReason::Replica`] | [`ReplicaIgnoreReason`] | kernel-b |
//!
//! [`AuthorityIgnoreReason`]: crate::contracts::authority::AuthorityIgnoreReason
//!
//! **The rule, stated so it can be checked:** a kernel adds a reason name by appending one
//! variant to its own leaf enum. It never edits `event.rs` *for a reason name*, never edits
//! [`KernelIgnoredReason`]'s arm set, and never waits on foundation or on the other kernel for
//! the append itself.
//!
//! # What the arms buy, and what they do not
//!
//! They buy **homograph separation**, and that is structural rather than conventional. Three
//! different facts are spelled `NotAMember`, `Quarantined` and so on in three different
//! ladders; under one flat enum they would collapse onto one name and a row asserting the wrong
//! one would pass for the wrong reason. Under the arms they are distinct Rust types with no
//! `From` between them, so the wrong one is an `E0308` naming both enums, at the row's own line.
//!
//! They do **not** buy `#[non_exhaustive]` protection from the six kernel modules.
//! `#[non_exhaustive]` is a cross-crate attribute and every kernel module lives in this crate,
//! so an in-crate `match` may be exhaustive with no catch-all and breaks on every added variant.
//! The convention that recovers it costs nothing and is written into both leaves below:
//!
//! > **A kernel module never destructures another kernel's leaf.** It matches the arm —
//! > `Replica(_)`, `Authority(_)` — or it does not match the reason at all. Destructuring is for
//! > the owner.
//!
//! # Serde
//!
//! Every type here takes serde's default externally-tagged representation. No
//! `#[serde(untagged)]`, no `#[serde(flatten)]`, no `#[serde(rename)]` on any arm or variant:
//! under an untagged representation `Error(NotPrimary)` and `Replica(NotRequired)` would collide
//! on the wire, and the arms would stop separating anything the moment a value was written down.

use serde::{Deserialize, Serialize};

use crate::contracts::authority::AuthorityIgnoreReason;
use crate::contracts::envelope::AppendReject;
use crate::contracts::errors::ErrorKind;
use crate::contracts::trace::AckRejectReason;

/// Why a kernel module produced [`crate::contracts::event::KernelEffect::Ignored`] (ask CB-7).
///
/// FOUNDATION owns this enum's ARM SET. An arm is added only when a new *owner* appears, which
/// is a foundation-scale event. No team adds an arm to spell a fact; it adds a variant to its
/// own leaf.
///
/// Not `Copy`, and deliberately so: `Copy` on a carrier is a promise about payloads the carrier
/// does not own, and it silently conditions the ownership `#[non_exhaustive]` advertises — a
/// kernel could add a variant only if foundation approved its payload's traits. `Clone` makes
/// the same value available and constrains a payload to nothing a `Serialize` contract does not
/// already require.
///
/// The derive set here is a standing constraint on every leaf: `KernelIgnoredReason: Ord`
/// requires `AuthorityIgnoreReason: Ord`. It is the one foundation promise that still limits
/// what a kernel may put in its own leaf, which is why it is spelled out rather than copied.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum KernelIgnoredReason {
    /// A client-facing spec §5.4 condition.
    Error(ErrorKind),
    /// The append ladder refused the record. The *receiver's* refusal, never the tracker's.
    AppendRejected(AppendReject),
    /// The acknowledgement did not count. The *tracker's* drop, never the receiver's.
    AckRejected(AckRejectReason),
    /// A fact kernel-a's authority module states about itself.
    Authority(AuthorityIgnoreReason),
    /// A fact one of kernel-b's five modules states about itself.
    Replica(ReplicaIgnoreReason),
}

/// A fact one of kernel-b's five modules states about why it did nothing.
///
/// KERNEL-B owns every variant of this enum. Add one by editing this enum and nothing else —
/// not [`KernelIgnoredReason`], not `event.rs`, and without waiting on foundation or kernel-a.
///
/// Not `Copy`: a variant with an owning payload is a variant kernel-b adds without asking
/// anyone, and a `Copy` derive here would take that away.
///
/// The eighteen span four subjects — replication, publication, protection and recovery — which is
/// why they are in a file of their own rather than in a landed subject file. This repository
/// files contracts by subject, and no landed subject file holds them all.
///
/// **Kernel-a never destructures this enum.** It matches `Replica(_)` or it does not match the
/// reason at all.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ReplicaIgnoreReason {
    /// `ALREADY_BLOCKED`: the partition is already blocked, so blocking it again does nothing.
    ///
    /// Distinct from kernel-a's `AuthorityIgnoreReason::AlreadyBlocked`, which is an authority
    /// fact about the same word. The two are one arm apart on purpose.
    AlreadyBlocked,
    /// `ALREADY_DIVERGED`: divergence is already recorded for this copy.
    AlreadyDiverged,
    /// `BARRIER_NOT_DURABLE`: the durable barrier publication waits on is not met yet.
    BarrierNotDurable,
    /// `INVALID_CONFIG`: the pinned configuration cannot be used for this step.
    ///
    /// Its own name rather than [`ErrorKind::InvalidArgument`], which is a client-facing answer
    /// and loses the meaning when a kernel says it to itself.
    InvalidConfig,
    /// `NO_QUALIFYING_SECONDARY`: no secondary qualifies, so there is nothing to publish to.
    NoQualifyingSecondary,
    /// `NOT_A_CURSOR_EVENT`: the event does not move the replication cursor.
    NotACursorEvent,
    /// `NOT_FENCED`: the copy is not fenced, so the protection step does not apply.
    NotFenced,
    /// `NOTHING_OUTSTANDING`: there is no outstanding record for this step to act on.
    NothingOutstanding,
    /// `NOT_REQUIRED`: recovery is not required here.
    NotRequired,
    /// `OUTSTANDING`: a record is still outstanding, so the step does not run yet.
    ///
    /// The complement of [`Self::NothingOutstanding`], and both are kept because a row must be
    /// able to tell "nothing to do" from "not yet".
    Outstanding,
    /// `QUARANTINED_TERMINAL`: F1's terminal quarantine phase.
    ///
    /// Distinct from [`AppendReject::Quarantined`], which is the append ladder refusing one
    /// record, and from kernel-a's `AuthorityIgnoreReason::Quarantined`, which is an authority
    /// fact. Three facts, three arms, one word.
    QuarantinedTerminal,
    /// `RECOVERY_ONLY`: the record is acceptable only on the recovery path.
    RecoveryOnly,
    /// `RECORDED`: a kernel wrote state that a later step reads, and the event moved no
    /// transition itself. L1: `PeerProgress`, `CopyLost`, `LocalApplied`, a `DurableAdvanced`
    /// that drains nothing out of `Warn` (lead ruling B-R42 A3: a drain that does moves
    /// `Warn -> Healthy`), `ConfigChanged`, a `Gained` edge (team kernel-b `design.md` §4.4).
    /// An L1 `LocalApplied` at or below the durable floor answers
    /// [`Self::NothingOutstanding`] instead (lead ruling B-R46b). An L1 `DurableAdvanced` naming a
    /// version above the current predicate is kept for that version's pin and answers `Recorded`
    /// too; one naming only retired or superseded versions answers [`Self::InvalidConfig`] (lead
    /// ruling B-R46d).
    /// F1: an inventory or progress report inside the discovery window (§5.2, §5.5).
    Recorded,
    /// `RESUME_HELD`: L1 is `Reprotecting` and `replication_lag` has not stayed below
    /// `resume_lag_ms` for the full `resume_hold_ms` (team kernel-b `design.md` §4.4).
    ResumeHeld,
    /// `OUT_OF_PHASE`: F1 takes this event, but not in its current phase (`design.md` §5.1).
    OutOfPhase,
    /// `STALE_TIMER`: a wake that is not the armed deadline — an older version, or not yet due.
    ///
    /// Kernel-b's; kernel-a's timer staleness is answered in `AuthorityIgnoreReason`.
    StaleTimer,
    /// `NOT_A_SOURCE`: a report from a copy that is not a live source here — never queried, or
    /// already recorded unavailable (`design.md` §5.5).
    NotASource,
    /// `RECOVERY_BLOCKED`: F1 is `Blocked`; only an operator or a fresh fence ends it
    /// (`design.md` §5.1).
    RecoveryBlocked,
    /// `OUT_OF_ORDER`: a sequenced fact that is not the next one — a gap or a regress — so
    /// nothing is stored. R1: a `LocalApplied` whose `seq` is not the primary's own head + 1
    /// (lead ruling B-R47).
    OutOfOrder,
}

#[cfg(test)]
mod tests {
    //! SCAFFOLDING, not test rows.
    //!
    //! These prove the seam this file adds actually works — the five arms accept their leaves,
    //! and the representation the design committed to is the one serde emits. They are not
    //! `M7F-*` rows, they do not live in `tests/`, and they carry no `#[retcd_test]`. A row that
    //! asserts kernel behaviour through this type belongs in the plan's own file.

    use super::{KernelIgnoredReason, ReplicaIgnoreReason};
    use crate::contracts::authority::AuthorityIgnoreReason;
    use crate::contracts::envelope::AppendReject;
    use crate::contracts::errors::ErrorKind;
    use crate::contracts::trace::AckRejectReason;

    /// Every arm accepts its own leaf and nothing collapses into another arm.
    #[test]
    fn each_arm_carries_its_own_leaf() {
        let arms = [
            KernelIgnoredReason::Error(ErrorKind::NotPrimary),
            KernelIgnoredReason::AppendRejected(AppendReject::NotAMember),
            KernelIgnoredReason::AckRejected(AckRejectReason::NotAMember),
            KernelIgnoredReason::Authority(AuthorityIgnoreReason::Quarantined),
            KernelIgnoredReason::Replica(ReplicaIgnoreReason::QuarantinedTerminal),
        ];
        let mut distinct = arms.to_vec();
        distinct.sort();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            arms.len(),
            "five arms, five distinct values"
        );

        // The homograph the arms exist to separate: one `NotAMember` is the receiver refusing
        // an append, the other is the tracker dropping an acknowledgement. Same word, and under
        // one flat enum there would be no way to write the second one at all.
        assert_ne!(
            KernelIgnoredReason::AppendRejected(AppendReject::NotAMember),
            KernelIgnoredReason::AckRejected(AckRejectReason::NotAMember)
        );
    }

    /// The representation is externally tagged, which is what keeps two arms' variants from
    /// colliding once a value is written down.
    #[test]
    fn the_wire_form_is_externally_tagged() {
        let error = serde_json::to_string(&KernelIgnoredReason::Error(ErrorKind::NotPrimary))
            .expect("a contract type serialises");
        let replica = serde_json::to_string(&KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::NotRequired,
        ))
        .expect("a contract type serialises");

        assert_eq!(error, r#"{"Error":"NotPrimary"}"#);
        assert_eq!(replica, r#"{"Replica":"NotRequired"}"#);

        // Untagged or flattened, these two would be indistinguishable strings on the wire and
        // the arms would stop separating anything the moment a trace was written.
        assert_ne!(error, replica);

        let back: KernelIgnoredReason = serde_json::from_str(&replica).expect("and it round-trips");
        assert_eq!(
            back,
            KernelIgnoredReason::Replica(ReplicaIgnoreReason::NotRequired)
        );
    }
}
