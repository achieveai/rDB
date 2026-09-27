//! `Rebuilding`: back to full protection after a `DegradedRf2` or `ReadOnly` commit (team
//! kernel-b `design.md` §5.6a).
//!
//! Activation is proved by the same `RecoveryBarrier::try_new` as the recovery barrier — one
//! constructor, two call sites — so three copies durable at different histories never activate.
//! A lost required copy stalls the rebuild loudly and **never shrinks `required`** (K-B-43): its
//! proof is dropped, later proofs from it are refused, and only a replacement supplied as data or
//! a fresh fence ends the stall.
//!
//! A sync is bounded (ruling B-R52): each `SyncWalThrough` starts a deadline, and a deadline that
//! passes names each required copy that has not proved the point and was not already reported
//! lost. It is a report, not a loss: `required` and the proofs do not move, and a late proof is
//! judged like any other (ruling B-R52a).
//!
//! The rebuild point is pinned by the first live catch-up at or above the committed cutoff, and
//! never below it (rulings F-e, A-3). A pin is never replaced. Every report is judged the same
//! whenever it lands (ruling A-1): a digest at the cutoff other than the committed one, or a
//! second digest at the point, is divergence, and proofs held before the pin are judged when it
//! lands.

use std::collections::{BTreeMap, BTreeSet};

use crate::contracts::digest::Digest;
use crate::contracts::ids::Seq;
use crate::contracts::ignore::ReplicaIgnoreReason;
use crate::contracts::membership::CopyId;
use crate::contracts::recovery::{DivergenceEvidence, DurableProof, RecoveryBarrier};
use crate::contracts::time::Tick;

use super::lineage::holds_root;

/// A digest `copy` reports at the committed `cutoff` must be the cutoff's own; any other seq says
/// nothing about it. Shared by the pre-commit wait (ruling A-4) and the rebuild (F-e, A-1).
pub(crate) fn check_cutoff(
    copy: CopyId,
    seq: Seq,
    digest: Digest,
    cutoff: (Seq, Digest),
) -> Result<(), DivergenceEvidence> {
    holds_root(copy, cutoff, (seq == cutoff.0).then_some(digest)).map(drop)
}

/// Why a rebuild input moved nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refused {
    /// Answered with this reason; the rebuild waits on.
    Ignored(ReplicaIgnoreReason),
    /// Two histories disagree at one position. Never a tie to break.
    Diverged(DivergenceEvidence),
}

impl From<ReplicaIgnoreReason> for Refused {
    fn from(reason: ReplicaIgnoreReason) -> Self {
        Self::Ignored(reason)
    }
}

/// The rebuild point, and the copy whose catch-up pinned it (the evidence if another disagrees).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Point {
    seq: Seq,
    digest: Digest,
    by: CopyId,
}

/// The rebuild in progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rebuild {
    required: BTreeSet<CopyId>,
    proofs: BTreeMap<CopyId, DurableProof>,
    lost: BTreeSet<CopyId>,
    /// The committed cutoff and its digest: the floor for the point.
    cutoff: (Seq, Digest),
    /// The rebuild point: the head the first target caught up to (`design.md` §5.6a).
    point: Option<Point>,
    /// The last sync's deadline, until it passes and is spent (ruling B-R52).
    deadline: Option<Tick>,
}

impl Rebuild {
    pub(crate) const fn new(required: BTreeSet<CopyId>, cutoff: (Seq, Digest)) -> Self {
        Self {
            required,
            proofs: BTreeMap::new(),
            lost: BTreeSet::new(),
            cutoff,
            point: None,
            deadline: None,
        }
    }

    /// The copies that must hold the barrier. Never shrinks.
    pub(crate) const fn required(&self) -> &BTreeSet<CopyId> {
        &self.required
    }

    fn check_required(&self, copy: CopyId) -> Result<(), ReplicaIgnoreReason> {
        if self.required.contains(&copy) {
            Ok(())
        } else {
            Err(ReplicaIgnoreReason::NotRequired)
        }
    }

    /// A second digest at the pinned point is divergence, whoever reports it.
    fn agrees(&self, copy: CopyId, seq: Seq, digest: Digest) -> Result<(), DivergenceEvidence> {
        match self.point {
            Some(point) if point.seq == seq && point.digest != digest => {
                Err(DivergenceEvidence::Pairwise {
                    seq,
                    a: (point.by, point.digest),
                    b: (copy, digest),
                })
            }
            _ => Ok(()),
        }
    }

    /// One report judged against the pin and the cutoff, the same for a catch-up and a proof.
    fn judge(&self, copy: CopyId, seq: Seq, digest: Digest) -> Result<(), Refused> {
        self.agrees(copy, seq, digest)
            .and_then(|()| check_cutoff(copy, seq, digest, self.cutoff))
            .map_err(Refused::Diverged)
    }

    /// A target caught up. Returns the point and the copies that must now make it durable: every
    /// required copy when this catch-up pins it (ruling F-d, spec §8.4 step 5), otherwise this
    /// one. A lost copy never pins (ruling A-3); a head short of the cutoff is still owed.
    pub(crate) fn caught_up(
        &mut self,
        copy: CopyId,
        head: Seq,
        digest: Digest,
    ) -> Result<(Seq, Vec<CopyId>), Refused> {
        self.check_required(copy)?;
        self.judge(copy, head, digest)?;
        if self.lost.contains(&copy) {
            return Err(ReplicaIgnoreReason::NotASource.into());
        }
        if head < self.cutoff.0 {
            return Err(ReplicaIgnoreReason::Outstanding.into());
        }
        if let Some(point) = self.point {
            return Ok((point.seq, vec![copy]));
        }
        self.point = Some(Point {
            seq: head,
            digest,
            by: copy,
        });
        // Proofs that landed before the pin are judged against it now (ruling A-1).
        for proof in self.proofs.values() {
            self.agrees(proof.copy, Seq(proof.seq.0), proof.digest)
                .map_err(Refused::Diverged)?;
        }
        Ok((head, self.required.iter().copied().collect()))
    }

    /// A durability proof. `Ok` with the barrier once every required copy proves the point.
    pub(crate) fn durable(&mut self, proof: DurableProof) -> Result<RecoveryBarrier, Refused> {
        self.check_required(proof.copy)?;
        self.judge(proof.copy, Seq(proof.seq.0), proof.digest)?;
        if !self.lost.contains(&proof.copy) {
            self.proofs.insert(proof.copy, proof);
        }
        let not_durable = || Refused::Ignored(ReplicaIgnoreReason::BarrierNotDurable);
        let point = self.point.ok_or_else(not_durable)?;
        let proofs: Vec<DurableProof> = self.proofs.values().copied().collect();
        RecoveryBarrier::try_new(&proofs, &self.required, point.seq, point.digest)
            .map_err(|_| not_durable())
    }

    /// A required copy was lost: drop its proof and refuse its later ones. A second loss of the
    /// same copy is already recorded, so it raises no second alert (advisory 21).
    pub(crate) fn copy_lost(&mut self, copy: CopyId) -> Result<(), ReplicaIgnoreReason> {
        self.check_required(copy)?;
        if !self.lost.insert(copy) {
            return Err(ReplicaIgnoreReason::NotASource);
        }
        self.proofs.remove(&copy);
        Ok(())
    }

    /// A sync was emitted: the rebuild waits on it until `deadline` (ruling B-R52).
    pub(crate) fn wait_until(&mut self, deadline: Tick) {
        self.deadline = Some(deadline);
    }

    /// The pending sync deadline, if one is armed and not yet spent.
    pub(crate) const fn deadline(&self) -> Option<Tick> {
        self.deadline
    }

    /// The deadline passed: spend it and return, in copy order, each required copy that has not
    /// proved the point and was not already reported lost. `required`, the proofs and the losses
    /// do not move (ruling B-R52a). Each copy is judged by the one barrier constructor, so this
    /// can never disagree with it.
    pub(crate) fn stall(&mut self) -> Vec<CopyId> {
        self.deadline = None;
        let Some(point) = self.point else {
            return Vec::new();
        };
        self.required
            .iter()
            .copied()
            .filter(|copy| !self.lost.contains(copy))
            .filter(|copy| {
                let held: Vec<DurableProof> = self.proofs.get(copy).copied().into_iter().collect();
                RecoveryBarrier::try_new(&held, &BTreeSet::from([*copy]), point.seq, point.digest)
                    .is_err()
            })
            .collect()
    }
}
