//! Derived views over a [`ProgressTracker`] (design §3.5): pure functions, no state (K-B-33).
//!
//! The three membership sets, defined once:
//!
//! - *required copies* of a predicate: its non-shadow members, **the primary included**, minus
//!   `diverged`. The durable views' domain; the primary can be the laggard (K-B-13).
//! - [`ProgressTracker::regular_secondaries`]: the pinned configuration's required copies minus
//!   the primary. The ACK predicate's domain; an ACK from ourselves is not replication.
//!
//! Shadows never qualify and there is no branch here that says so: the sets filter on
//! `ReplicaRole::may_qualify_ack`, the contract's one statement of that rule.

use crate::contracts::ids::{ConfigVersion, DurableSeq, Seq};
use crate::contracts::membership::{CopyId, PartitionConfig};

use super::tracker::{CopyProgress, ProgressTracker};

impl ProgressTracker {
    /// The required copies of `predicate` with what the tracker believes about each.
    fn required_in<'a>(
        &'a self,
        predicate: &'a PartitionConfig,
    ) -> impl Iterator<Item = (CopyId, &'a CopyProgress)> + 'a {
        predicate
            .members
            .iter()
            .filter(|member| member.role.may_qualify_ack())
            .filter(|member| !self.is_diverged(member.copy))
            .filter_map(|member| Some((member.copy, self.peer(member.copy)?)))
    }

    /// `required_copies()` of the pinned configuration: the primary included.
    #[must_use]
    pub fn required_copies(&self) -> Vec<CopyId> {
        self.required_in(self.config())
            .map(|(copy, _)| copy)
            .collect()
    }

    /// `regular_secondaries()`: the pinned configuration's required copies minus the primary.
    #[must_use]
    pub fn regular_secondaries(&self) -> Vec<CopyId> {
        self.required_copies()
            .into_iter()
            .filter(|copy| *copy != self.own())
            .collect()
    }

    /// The regular secondaries that have applied `seq`.
    #[must_use]
    pub fn qualified_copies(&self, seq: Seq) -> Vec<CopyId> {
        self.regular_secondaries()
            .into_iter()
            .filter(|copy| {
                self.peer(*copy)
                    .is_some_and(|peer| peer.progress.buffered_applied.0 >= seq.0)
            })
            .collect()
    }

    /// `qualified_ack_count(seq)`: how many regular secondaries have applied `seq`. The
    /// primary, shadows and diverged copies are never counted, because
    /// [`Self::regular_secondaries`] never holds them.
    #[must_use]
    pub fn qualified_ack_count(&self, seq: Seq) -> usize {
        self.qualified_copies(seq).len()
    }

    /// Whether `seq` has the pinned configuration's `min_regular_acks` behind it. P1 reads this
    /// live at publication (§3.5, K-B-33), together with `lineage()`, `config()` and the digest
    /// binding `history().lookup(seq, record_digest)`. Validation refuses a threshold of 0, so
    /// losing the last regular secondary makes this false. There is no one-copy fallback,
    /// because there is no code for one.
    #[must_use]
    pub fn qualifies_now(&self, seq: Seq) -> bool {
        self.qualified_ack_count(seq) >= usize::from(self.config().min_regular_acks)
    }

    /// `min_required_durable()` over the pinned configuration: the lowest durable watermark
    /// among its required copies, the primary's own included.
    #[must_use]
    pub fn min_required_durable(&self) -> DurableSeq {
        durable_floor(self.required_in(self.config()))
    }

    /// `all_durable_through(seq)` over the pinned configuration.
    #[must_use]
    pub fn all_durable_through(&self, seq: Seq) -> bool {
        self.min_required_durable().0 >= seq.0
    }

    /// `(config_version, all_durable_through)` for every active predicate, each over its own
    /// members (K-B-49): what `DurableAdvanced` carries.
    #[must_use]
    pub fn durable_per_predicate(&self) -> Vec<(ConfigVersion, DurableSeq)> {
        self.predicates()
            .iter()
            .map(|predicate| {
                (
                    predicate.config_version,
                    durable_floor(self.required_in(predicate)),
                )
            })
            .collect()
    }
}

/// The lowest durable watermark in `copies`. Every predicate holds the primary, so the set is
/// never empty in practice; an empty one proves nothing and answers zero.
fn durable_floor<'a>(copies: impl Iterator<Item = (CopyId, &'a CopyProgress)>) -> DurableSeq {
    copies
        .map(|(_, peer)| peer.progress.durable)
        .min()
        .unwrap_or(DurableSeq(0))
}
