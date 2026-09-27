//! R1's live publish predicate, as P1 reads it (team kernel-a `design.md` §1.6; lead rulings
//! B-R21, A-R25).
//!
//! P1 re-evaluates the predicate **at publication**, never trusting the `Gained` edge that woke
//! it: a copy excluded by `DivergenceDetected` must stop qualifying before the publish, not
//! after it. That needs a synchronous read of R1's state, and R1 is a separate
//! [`crate::contracts::event::Module`], so the read is a trait the caller passes in:
//! [`super::Publication::step_with`] takes one, and the dispatcher hands it the primary's
//! [`ProgressTracker`], which implements it below.
//!
//! [`ScriptedReplication`] is the test plan's KA-8 fake: `qualifies_now` false and `digest_at`
//! `Match` until scripted otherwise.

use std::collections::{BTreeMap, BTreeSet};

use crate::contracts::authority::Lineage;
use crate::contracts::digest::Digest;
use crate::contracts::ids::{ConfigVersion, Seq};
use crate::replication::progress::{DigestLookup, ProgressTracker};

/// R1 → P1, evaluated against R1's state now (design §1.6).
pub trait ReplicationView {
    /// The lineage R1 is replicating.
    fn lineage(&self) -> Lineage;
    /// The pinned configuration R1 is counting against.
    fn config_version(&self) -> ConfigVersion;
    /// Whether `seq` has the pinned configuration's `min_regular_acks` behind it now.
    fn qualifies_now(&self, seq: Seq) -> bool;
    /// R1's own history at `seq`, compared against `expected`.
    fn digest_at(&self, seq: Seq, expected: Digest) -> DigestLookup;
}

impl ReplicationView for ProgressTracker {
    fn lineage(&self) -> Lineage {
        ProgressTracker::lineage(self)
    }

    fn config_version(&self) -> ConfigVersion {
        self.config().config_version
    }

    fn qualifies_now(&self, seq: Seq) -> bool {
        ProgressTracker::qualifies_now(self, seq)
    }

    fn digest_at(&self, seq: Seq, expected: Digest) -> DigestLookup {
        self.history().lookup(seq, expected)
    }
}

/// A scripted [`ReplicationView`] (test plan KA-8).
///
/// Defaults: `qualifies_now` is false for every sequence, and `digest_at` answers `Match`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptedReplication {
    /// What [`ReplicationView::lineage`] answers.
    pub lineage: Lineage,
    /// What [`ReplicationView::config_version`] answers.
    pub config_version: ConfigVersion,
    /// The sequences [`ReplicationView::qualifies_now`] answers true for.
    pub qualified: BTreeSet<Seq>,
    /// Scripted [`ReplicationView::digest_at`] answers; any other sequence answers `Match`.
    pub digests: BTreeMap<Seq, DigestLookup>,
}

impl ScriptedReplication {
    /// A view of `lineage` under `config_version` in which nothing qualifies.
    #[must_use]
    pub const fn new(lineage: Lineage, config_version: ConfigVersion) -> Self {
        Self {
            lineage,
            config_version,
            qualified: BTreeSet::new(),
            digests: BTreeMap::new(),
        }
    }

    /// Make `seq` qualify (or stop qualifying).
    pub fn set_qualifies(&mut self, seq: Seq, qualifies: bool) {
        if qualifies {
            self.qualified.insert(seq);
        } else {
            self.qualified.remove(&seq);
        }
    }

    /// Script the `digest_at` answer at `seq`.
    pub fn set_digest(&mut self, seq: Seq, lookup: DigestLookup) {
        self.digests.insert(seq, lookup);
    }
}

impl ReplicationView for ScriptedReplication {
    fn lineage(&self) -> Lineage {
        self.lineage
    }

    fn config_version(&self) -> ConfigVersion {
        self.config_version
    }

    fn qualifies_now(&self, seq: Seq) -> bool {
        self.qualified.contains(&seq)
    }

    fn digest_at(&self, seq: Seq, _expected: Digest) -> DigestLookup {
        self.digests
            .get(&seq)
            .copied()
            .unwrap_or(DigestLookup::Match)
    }
}
