//! The digest ladder and the three watermarks: what a copy holds, and how strongly.
//!
//! **Owner:** team kernel-b, package R1. Design `design.md` §3.2 (the three-valued lookup),
//! §3.3 (the watermarks), §3.4 (`ProgressTracker`).
//!
//! # What is here, and what is not
//!
//! [`DigestLadder`] is here because **both** directions of R1 need it and neither may re-derive
//! it: the receiver's step 8 (`design.md` §3.2) and the primary tracker's rule 9 (§3.4) ask the
//! same three-valued question, and an implementation that answered it twice could answer it two
//! ways.
//!
//! [`ProgressTracker`] — the primary-side ACK admission ladder of §3.4 — is in `tracker.rs`, and
//! the derived views of §3.5 over it in `views.rs`. It is driven directly, not yet routed:
//! [`crate::replication::Replication`] still declines every acknowledgement until the tester's
//! thumbs-up on this slice (lead ruling B-R36), because a module that counted an ACK nobody had
//! gated is the single most dangerous thing this package could ship (charter DO-NOT, and spike
//! §5's R1 row "lost/malicious ACK cannot advance progress").
//!
//! # Only `Differs` is evidence
//!
//! The whole reason the lookup is three-valued rather than a comparison (`design.md` §3.2,
//! finding K-B-01). "The stored digest is not equal to this one" and "we never kept a digest for
//! that sequence" are different facts, and collapsing them makes ordinary log truncation
//! manufacture divergence. [`DigestLookup::NotRetained`] **never quarantines**: quarantine is
//! reserved for *proved* disagreement, and absence is not proof.

mod tracker;
mod views;

use std::collections::BTreeMap;

use crate::contracts::authority::Lineage;
use crate::contracts::digest::Digest;
use crate::contracts::ids::{DurableSeq, Seq};
use crate::contracts::storage::DurablePrefix;

pub use tracker::{CopyProgress, ProgressTracker, TrackerInit};

/// What a flush proved durable for `lineage`: the best prefix of its partition **and
/// generation**, never above `applied` — a flush cannot make an unapplied record durable.
/// `None` when no prefix names this lineage. One fold for both sides of the wire.
pub(crate) fn proved_durable(
    durable: &[DurablePrefix],
    lineage: &Lineage,
    applied: Seq,
) -> Option<DurableSeq> {
    durable
        .iter()
        .filter(|prefix| {
            prefix.partition == lineage.partition && prefix.generation == lineage.generation
        })
        .map(|prefix| DurableSeq(prefix.through.0.min(applied.0)))
        .max()
}

/// The answer to "do you hold this digest at this sequence?".
///
/// Three-valued on purpose (`design.md` §3.2, finding K-B-01). A two-valued comparison cannot
/// tell a copy that disagrees from a copy whose record we dropped, and the two must lead to
/// opposite actions: quarantine, and a digest probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DigestLookup {
    /// The ladder holds this sequence and the digests agree.
    Match,
    /// The ladder holds this sequence and the digests differ. **This, and only this, is
    /// evidence of divergence.**
    Differs {
        /// What the ladder holds. The candidate is the caller's own value.
        stored: Digest,
    },
    /// The ladder holds nothing for this sequence — the record was dropped and the sequence fell
    /// between sparse rungs, or it was never inserted. Never quarantines.
    NotRetained,
}

/// Sequence to digest, for every position this copy can still vouch for.
///
/// Dense where the record is retained, and able to hold sparse rungs where records have been
/// dropped — what is retained is storage's policy, not R1's, and the ladder mirrors it. In this
/// build nothing drops a rung, so the ladder is dense from its floor; [`Self::truncate_above`]
/// is the only removal, made by the `Recovered` transition.
///
/// A [`BTreeMap`], never a hash map: ordered iteration is a charter rule, because two runs of
/// one event log must produce one trace.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DigestLadder {
    entries: BTreeMap<Seq, Digest>,
}

impl DigestLadder {
    /// An empty ladder, vouching for nothing.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Record the digest of the record at `seq`.
    ///
    /// Overwrites, because the one caller in this build ([`crate::replication::append`]'s
    /// `Committed` arm) inserts a position it has just validated as a chain extension, and a
    /// position can be validated only once — step 8 answers a second arrival at the same
    /// sequence as `AlreadyHave` or as divergence, never as another insert.
    pub fn insert(&mut self, seq: Seq, digest: Digest) {
        self.entries.insert(seq, digest);
    }

    /// What this ladder holds at `seq`, if anything. The raw read behind [`Self::lookup`].
    #[must_use]
    pub fn digest_at(&self, seq: Seq) -> Option<Digest> {
        self.entries.get(&seq).copied()
    }

    /// The three-valued answer for `candidate` at `seq`.
    #[must_use]
    pub fn lookup(&self, seq: Seq, candidate: Digest) -> DigestLookup {
        match self.entries.get(&seq) {
            None => DigestLookup::NotRetained,
            Some(stored) if *stored == candidate => DigestLookup::Match,
            Some(stored) => DigestLookup::Differs { stored: *stored },
        }
    }

    /// The highest rung at or below `seq`, if any. `Recovered`'s anchor (design §3.3): a copy
    /// re-anchors on the newest position it can still vouch for, never on one it was told.
    #[must_use]
    pub fn at_or_below(&self, seq: Seq) -> Option<(Seq, Digest)> {
        self.entries
            .range(..=seq)
            .next_back()
            .map(|(held, digest)| (*held, *digest))
    }

    /// Drop every rung strictly above `seq`.
    ///
    /// The `Recovered` transition's truncation (`design.md` §3.3): rungs above the new anchor
    /// stop being ladder-visible, though storage keeps the suffix for forensics.
    pub fn truncate_above(&mut self, seq: Seq) {
        self.entries.retain(|held, _| *held <= seq);
    }

    /// How many positions the ladder vouches for.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the ladder vouches for nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The highest sequence the ladder holds, or `None` when it holds nothing.
    #[must_use]
    pub fn highest(&self) -> Option<Seq> {
        self.entries.keys().next_back().copied()
    }
}
