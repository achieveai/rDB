//! Ancestry and selection: the pure half of F1 (team kernel-b `design.md` §5.3, §5.4).
//!
//! Nothing here holds state or emits an effect. [`verify_ancestry`] is the only way to build a
//! [`VerifiedInventory`], and [`select_prefix`] takes nothing else, so an unverified sequence
//! number cannot reach selection. That is half of the charter's "no longest-wins by length
//! alone"; the other half is the pairwise loop in [`select_prefix`], which no type enforces and
//! the divergent-pair tests guard.

use std::collections::{BTreeMap, BTreeSet};

use crate::contracts::authority::Lineage;
use crate::contracts::digest::Digest;
use crate::contracts::ids::Seq;
use crate::contracts::membership::CopyId;
use crate::contracts::recovery::{
    Candidate, DivergenceEvidence, LineageAnchor, SelectedLineage, SurvivorInventory,
    UnavailableReason,
};

/// A survivor whose history is on the committed root and not proved off it.
///
/// Fields are private: the only constructor is [`verify_ancestry`]. A ladder may lack the base
/// rung (the contract ladder is a stride, not every seq), so the root itself can still be owed:
/// [`select_prefix`] probes it before anything is selected (ruling F-c, design §5.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedInventory {
    copy: CopyId,
    head: Seq,
    head_digest: Digest,
    ladder: BTreeMap<Seq, Digest>,
    /// The committed root this copy must hold: `(base_seq, base_digest)`.
    base: (Seq, Digest),
}

impl VerifiedInventory {
    /// Whose history this is.
    #[must_use]
    pub const fn copy(&self) -> CopyId {
        self.copy
    }

    /// The highest complete record. Readable only after verification.
    #[must_use]
    pub const fn head_seq(&self) -> Seq {
        self.head
    }

    /// The digest this copy holds at `seq`, when its ladder (or a probe answer) has it.
    #[must_use]
    pub fn digest_at(&self, seq: Seq) -> Option<Digest> {
        self.ladder.get(&seq).copied()
    }

    /// Whether this copy holds the committed root; see [`holds_root`]. `Ok(false)` means the base
    /// rung must be probed.
    fn holds_base(&self) -> Result<bool, DivergenceEvidence> {
        holds_root(self.copy, self.base, self.digest_at(self.base.0))
    }

    /// Fold a probe answer into the ladder. A position above the head is not history this copy
    /// reported, so it is not learned.
    pub(crate) fn learn(&mut self, seq: Seq, digest: Digest) {
        if seq <= self.head {
            self.ladder.insert(seq, digest);
        }
    }
}

/// Whether `copy`, whose digest at `root.0` is `found`, holds `root`: `Ok(true)` proved, `Ok(false)`
/// not known, `Err` proved not to. Only a digest that is present can prove divergence; an absent
/// one is missing evidence, never divergence. One rule for the base before selection and for the
/// committed cutoff after it (rulings F-c, F-e, A-4).
pub(crate) fn holds_root(
    copy: CopyId,
    root: (Seq, Digest),
    found: Option<Digest>,
) -> Result<bool, DivergenceEvidence> {
    let (base_seq, expected) = root;
    match found {
        None => Ok(false),
        Some(found) if found == expected => Ok(true),
        Some(found) => Err(DivergenceEvidence::RootMismatch {
            copy,
            base_seq,
            expected,
            found: Some(found),
        }),
    }
}

/// Why an inventory was not verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejected {
    /// Kept as evidence and recorded as lost to this recovery, but not divergence.
    Ineligible(UnavailableReason),
    /// Proved incompatible with the committed root.
    Divergence(DivergenceEvidence),
    /// The copy is on a newer root of this partition: a peer recovered first, and the plan this
    /// recovery runs on is stale (ruling F-b(ii); spec §8.1).
    Superseded,
}

/// Whether `seen` is a newer root of the same partition than `ours`: `(generation, owner_epoch)`
/// compared in that order.
fn is_newer(seen: Lineage, ours: Lineage) -> bool {
    seen.partition == ours.partition
        && (seen.generation, seen.owner_epoch) > (ours.generation, ours.owner_epoch)
}

/// Check one inventory against the committed root (`design.md` §5.3), in the design's order:
/// a newer root supersedes the plan, any other root is stale, a quarantined copy is evidence
/// only, a head below the base cannot hold the root, and a digest at `base_seq` other than the
/// root's is divergence. A ladder with no rung at `base_seq` is verified with the root still owed.
pub fn verify_ancestry(
    root: &LineageAnchor,
    inv: &SurvivorInventory,
) -> Result<VerifiedInventory, Rejected> {
    if inv.anchor_seen != *root {
        return Err(if is_newer(inv.anchor_seen.lineage, root.lineage) {
            Rejected::Superseded
        } else {
            Rejected::Ineligible(UnavailableReason::StaleLineage)
        });
    }
    if inv.quarantined.is_some() {
        return Err(Rejected::Ineligible(UnavailableReason::Quarantined));
    }
    let (head, head_digest) = inv.head;
    if head < root.base_seq {
        return Err(Rejected::Ineligible(UnavailableReason::StaleLineage));
    }
    let mut ladder: BTreeMap<Seq, Digest> = inv
        .ladder
        .iter()
        .copied()
        .filter(|(seq, _)| *seq <= head)
        .collect();
    ladder.insert(head, head_digest);
    let verified = VerifiedInventory {
        copy: inv.copy,
        head,
        head_digest,
        ladder,
        base: (root.base_seq, root.base_digest),
    };
    verified.holds_base().map_err(Rejected::Divergence)?;
    Ok(verified)
}

/// What [`select_prefix`] decided. Total: every input gets an answer (K-B-10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectionOutcome {
    /// No verified survivor to select from.
    Empty,
    /// Every pair is proved compatible; the longest prefix wins.
    Selected(SelectedLineage),
    /// The ladders cannot decide some pairs: `(whose ladder, at which seq)`, sorted and
    /// deduplicated.
    NeedProbes(Vec<(CopyId, Seq)>),
    /// Two survivors disagree at one position. Never a tie to break.
    Divergence(DivergenceEvidence),
}

/// How often selection ran, and how often length decided between survivors: the test plan's
/// `SelectSpy` and `LengthSpy` (M7B-97, M7B-113). A pair of counts and nothing else; no decision
/// reads them. [`crate::recovery::Recovery::spy`] exposes the module's own.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SelectionSpy {
    selections: u32,
    length_reads: u32,
}

impl SelectionSpy {
    /// Nothing counted yet.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            selections: 0,
            length_reads: 0,
        }
    }

    /// How many times [`select_prefix_spied`] ran.
    #[must_use]
    pub const fn selections(self) -> u32 {
        self.selections
    }

    /// How many times length chose between survivors. Only the private `longest` counts one, and
    /// it runs only after every pair passed.
    #[must_use]
    pub const fn length_reads(self) -> u32 {
        self.length_reads
    }
}

/// The compatible longest prefix among verified survivors (`design.md` §5.4).
///
/// Every copy is checked at the base, and every pair at the shorter head, before anything is
/// chosen. A proved mismatch returns at once, even with probes outstanding, because more evidence
/// cannot un-prove it. A missing rung, the base's included, is collected, never assumed
/// compatible. `root` is the new lineage the selection is made under; it is carried into the
/// result and read by nothing here.
#[must_use]
pub fn select_prefix(verified: &[VerifiedInventory], root: Lineage) -> SelectionOutcome {
    select_prefix_spied(verified, root, &mut SelectionSpy::new())
}

/// [`select_prefix`], counting on `spy`.
#[must_use]
pub fn select_prefix_spied(
    verified: &[VerifiedInventory],
    root: Lineage,
    spy: &mut SelectionSpy,
) -> SelectionOutcome {
    spy.selections = spy.selections.saturating_add(1);
    let mut needed: BTreeSet<(CopyId, Seq)> = BTreeSet::new();
    for copy in verified {
        match copy.holds_base() {
            Ok(true) => {}
            Ok(false) => {
                needed.insert((copy.copy, copy.base.0));
            }
            Err(evidence) => return SelectionOutcome::Divergence(evidence),
        }
    }
    for (i, x) in verified.iter().enumerate() {
        for y in &verified[i + 1..] {
            let (a, b) = if x.head <= y.head { (x, y) } else { (y, x) };
            match b.digest_at(a.head) {
                None => {
                    needed.insert((b.copy, a.head));
                }
                Some(b_digest) if b_digest != a.head_digest => {
                    return SelectionOutcome::Divergence(DivergenceEvidence::Pairwise {
                        seq: a.head,
                        a: (a.copy, a.head_digest),
                        b: (b.copy, b_digest),
                    });
                }
                Some(_) => {}
            }
        }
    }
    if !needed.is_empty() {
        return SelectionOutcome::NeedProbes(needed.into_iter().collect());
    }
    let Some(longest) = longest(verified, spy) else {
        return SelectionOutcome::Empty;
    };
    SelectionOutcome::Selected(SelectedLineage {
        root,
        cutoff_seq: longest.head,
        cutoff_digest: longest.head_digest,
        source: longest.copy,
    })
}

/// The longest head: the one place length chooses between survivors, counted on `spy`. Safe only
/// once every pair has passed, which is why [`select_prefix_spied`] calls it last. Ties go to the
/// lowest copy id, so the choice does not depend on the order inventories arrived in.
fn longest<'a>(
    verified: &'a [VerifiedInventory],
    spy: &mut SelectionSpy,
) -> Option<&'a VerifiedInventory> {
    spy.length_reads = spy.length_reads.saturating_add(1);
    verified
        .iter()
        .max_by(|x, y| x.head.cmp(&y.head).then(y.copy.cmp(&x.copy)))
}

/// The copy that leads the new lineage (`design.md` §5.4). Separate from [`select_prefix`]:
/// history decides the prefix, eligibility decides the leader.
///
/// `eligible` is the set of verified regular copies; a shadow is never in it, so a shadow never
/// leads even when it holds the longest prefix (M7B-118). The prefix holder leads when it is a
/// viable candidate; otherwise the first viable candidate in placement's order does.
#[must_use]
pub fn select_leader(
    selected: &SelectedLineage,
    candidates: &[Candidate],
    eligible: &BTreeSet<CopyId>,
) -> Option<CopyId> {
    let viable: Vec<CopyId> = candidates
        .iter()
        .filter(|c| {
            eligible.contains(&c.copy)
                && c.primary_eligible
                && c.healthy
                && c.within_capacity
                && c.has_valid_grant
        })
        .map(|c| c.copy)
        .collect();
    if viable.contains(&selected.source) {
        return Some(selected.source);
    }
    viable.first().copied()
}
