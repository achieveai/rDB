//! T1's dedup index (team kernel-a `design.md` §3.1).
//!
//! Keyed `(generation, affinity, identity)`, which is spec §5.3's scoping verbatim. Nothing in
//! here reads a clock: an entry's age is the sequence it was retained at (`applied_at_seq`), and
//! the 24 h window is enforced by whoever issues [`DedupIndex::trim`] and
//! [`DedupIndex::retire`]. T1 never drops an entry on its own, so absent a trim the index only
//! grows, and [`RETENTION_CAP_ENTRIES`] bounds that growth by refusing admission (plan Q-4).

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::contracts::authority::Lineage;
use crate::contracts::digest::Digest;
use crate::contracts::ids::{
    AffinityId, ClientId, Generation, OwnerEpoch, PartitionId, RequestId, RequestIdentity, Seq,
    TenantId,
};
use crate::contracts::storage::{Namespace, SnapshotRead};
use crate::contracts::txn::TxnResult;

/// Records read per [`SnapshotRead::scan`] call while seeding.
pub const SEED_PAGE: usize = 1_024;

/// The durable dedup records a new instance has not loaded yet (lead rulings A-R68, A-R70).
///
/// T1's index lives in memory, so a new instance may lack what earlier instances retained:
/// after failover the requests were applied on another node, after a demotion the index went
/// with the instance, and an applied request is retained in memory only once `Published` names
/// it. The records themselves are not lost: each transaction wrote its dedup record in the same
/// atomic batch as its data (spec §4), so every copy that holds the retained prefix holds them.
/// Every instance loads them on creation, which is the one mechanism for all three cases, and
/// admits nothing until it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedPending {
    /// The partition.
    pub partition: PartitionId,
    /// The generation the new instance serves. Every record loaded is from an older one.
    pub serving: Generation,
    /// Load records at or below this sequence (`RetainedStatusMap.retained_through`), and load
    /// nothing until the snapshot shows at least this far.
    pub retained_through: Seq,
}

/// Why a seed attempt loaded nothing and stays pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedBlocked {
    /// The snapshot does not yet show the retained prefix.
    NotCovered {
        /// What the snapshot shows.
        at: Seq,
    },
    /// A durable record did not decode. Loading around it could drop a retained identity and
    /// re-execute that request, so T1 fails closed and loads nothing.
    Malformed,
}

/// How many retained entries T1 holds before it answers `OVERLOADED` at admission check 9
/// (plan Q-4, M7A-90: "a named capacity constant, or `OVERLOADED` past it" — this is both).
///
/// Counted over every generation the index still holds, because the memory is spent either way.
pub const RETENTION_CAP_ENTRIES: usize = 65_536;

/// Bytes of a dedup record's storage key: generation (`u64`), affinity (`u64`), tenant (`u32`),
/// client (`u32`), request (`u64`), all big-endian.
pub const DEDUP_KEY_LEN: usize = 32;

/// Bytes of a dedup record's storage value: the request digest, the sequence (`u64` LE) and the
/// owner epoch (`u64` LE).
pub const DEDUP_VALUE_LEN: usize = 48;

/// The answer a retained identity replays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetainedAnswer {
    /// P1 published it; this is the result P1 published, replayed verbatim.
    Applied(TxnResult),
    /// Step 12 failed at this condition. Retained so a retry sees the answer the original
    /// submission saw, not a fresh evaluation against moved state (M7A-79). No sequence was
    /// allocated.
    ConditionFailed {
        /// The zero-based position [`crate::contracts::errors::RdbError::ConditionFailed`]
        /// named.
        index: u32,
    },
}

/// One retained request outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retained {
    /// The payload digest the identity was first submitted with. A retry with another digest
    /// is `REQUEST_ID_REUSE`.
    pub request_digest: Digest,
    /// What a same-digest retry is answered with.
    pub answer: RetainedAnswer,
    /// The age counter [`DedupIndex::trim`] compares: the transaction's own sequence for an
    /// applied answer, and the last reserved position for a condition failure.
    pub applied_at_seq: Seq,
}

/// `BTreeMap<(Generation, AffinityId, RequestIdentity), Retained>` plus the two structures that
/// make "absent" a total answer: how far each generation has been trimmed, and which
/// generations are retired (design §3.1, K-A-12).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DedupIndex {
    entries: BTreeMap<(Generation, AffinityId, RequestIdentity), Retained>,
    retained_from_seq: BTreeMap<Generation, Seq>,
    retired_generations: BTreeSet<Generation>,
}

impl DedupIndex {
    /// The entry for `identity` in exactly `generation` and `affinity`.
    #[must_use]
    pub fn get(
        &self,
        generation: Generation,
        affinity: AffinityId,
        identity: RequestIdentity,
    ) -> Option<&Retained> {
        self.entries.get(&(generation, affinity, identity))
    }

    /// The newest entry for `identity` in a generation **older** than `current` that is not
    /// retired: the generation-reconciliation lookup (spec §8.1).
    #[must_use]
    pub fn older(
        &self,
        current: Generation,
        affinity: AffinityId,
        identity: RequestIdentity,
    ) -> Option<(Generation, &Retained)> {
        self.entries
            .iter()
            .rev()
            .filter(|((g, a, id), _)| *g < current && *a == affinity && *id == identity)
            .find(|((g, _, _), _)| !self.retired_generations.contains(g))
            .map(|((g, _, _), retained)| (*g, retained))
    }

    /// Retain `retained` for `identity`. A retired generation retains nothing: it is gone by
    /// declaration, and a late insert would resurrect part of it.
    pub fn insert(
        &mut self,
        generation: Generation,
        affinity: AffinityId,
        identity: RequestIdentity,
        retained: Retained,
    ) {
        if self.retired_generations.contains(&generation) {
            return;
        }
        self.entries
            .insert((generation, affinity, identity), retained);
    }

    /// `DedupTrim{generation, below}`: drop that generation's entries with
    /// `applied_at_seq < below`, and record the watermark.
    ///
    /// The watermark only rises. A lower `below` than one already applied drops nothing (the
    /// predicate is already true of everything it would drop) and must not lower the recorded
    /// watermark, or the index would claim to still hold outcomes it has discarded.
    pub fn trim(&mut self, generation: Generation, below: Seq) {
        self.entries
            .retain(|(g, _, _), retained| *g != generation || retained.applied_at_seq >= below);
        let mark = self.retained_from_seq.entry(generation).or_insert(below);
        *mark = (*mark).max(below);
    }

    /// `RetireGeneration{generation}`: drop the whole generation and record that it is retired.
    pub fn retire(&mut self, generation: Generation) {
        self.entries.retain(|(g, _, _), _| *g != generation);
        self.retired_generations.insert(generation);
    }

    /// Entries held, over every generation.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The trim watermark for `generation`, or `None` if it was never trimmed.
    #[must_use]
    pub fn retained_from_seq(&self, generation: Generation) -> Option<Seq> {
        self.retained_from_seq.get(&generation).copied()
    }

    /// Whether `generation` was retired.
    #[must_use]
    pub fn is_retired(&self, generation: Generation) -> bool {
        self.retired_generations.contains(&generation)
    }

    /// Every retired generation, ascending.
    pub fn retired_generations(&self) -> impl Iterator<Item = Generation> + '_ {
        self.retired_generations.iter().copied()
    }

    /// Load the durable records `pending` names from `snapshot`, all or nothing (A-R68, A-R70).
    /// Returns how many entries were added.
    ///
    /// Every record is filed under the generation its own key names, so a retry of a request
    /// any earlier instance applied answers `GENERATION_CHANGED{that generation, current}`,
    /// whichever node it ran on and however many recoveries or demotions came between. A row
    /// replaces an entry already held for that identity and generation. That changes nothing: a
    /// durable row exists only for an applied request, so the entry held was loaded from this
    /// same row or retained from the same `Published`. Nothing below a generation's trim
    /// watermark is loaded, and a retired generation loads nothing ([`DedupIndex::insert`]).
    ///
    /// # Errors
    ///
    /// [`SeedBlocked`]. In that case the index is unchanged.
    pub fn seed(
        &mut self,
        pending: &SeedPending,
        snapshot: &dyn SnapshotRead,
        result: impl Fn(Lineage, Seq) -> TxnResult,
    ) -> Result<usize, SeedBlocked> {
        let at = snapshot.at();
        if at < pending.retained_through {
            return Err(SeedBlocked::NotCovered { at });
        }
        let mut found = Vec::new();
        let mut from = Vec::new();
        loop {
            let page = snapshot.scan(Namespace::Dedup, &from, SEED_PAGE);
            for (key, value) in &page {
                let (Some(key), Some(value)) = (parse_dedup_key(key), parse_dedup_value(value))
                else {
                    return Err(SeedBlocked::Malformed);
                };
                // Above the cut: the discarded suffix, which a copy may still hold.
                if value.seq > pending.retained_through {
                    continue;
                }
                // Within the prefix, no instance can have written in the generation being
                // served, or in a newer one: such a row is not T1's.
                if key.generation >= pending.serving {
                    return Err(SeedBlocked::Malformed);
                }
                found.push((key, value));
            }
            match page.last() {
                Some((last, _)) if page.len() == SEED_PAGE => {
                    from = last.to_vec();
                    from.push(0);
                }
                _ => break,
            }
        }
        let mut added = 0;
        for (key, value) in found {
            let floor = self.retained_from_seq(key.generation).unwrap_or(Seq::ZERO);
            if value.seq < floor {
                continue;
            }
            let lineage = Lineage {
                partition: pending.partition,
                generation: key.generation,
                owner_epoch: value.owner_epoch,
            };
            let before = self.len();
            self.insert(
                key.generation,
                key.affinity,
                key.identity,
                Retained {
                    request_digest: value.request_digest,
                    answer: RetainedAnswer::Applied(result(lineage, value.seq)),
                    applied_at_seq: value.seq,
                },
            );
            added += self.len() - before;
        }
        Ok(added)
    }
}

/// Every `DedupTrim` and `RetireGeneration` one `(node, partition)` has been told of, kept
/// outside any instance so a later instance can apply them (lead rulings A-R73 N1, A-R73a).
///
/// Nothing deletes a durable dedup row, so a seed loads rows a live instance had already
/// trimmed, and they count against the dedup cap. Applying the same trims to the new instance
/// before it admits makes its index what a live instance would hold after them: a superset
/// when some trim never reached this node, equal when every one did. A superset only ever
/// means an earlier `OVERLOADED`, never a second execution.
///
/// Held in memory only. After a crash it is gone and the next seed retains a superset, which
/// is the safe direction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrimMemory {
    below: BTreeMap<Generation, Seq>,
    retired: BTreeSet<Generation>,
}

impl TrimMemory {
    /// Remember `DedupTrim{generation, below}`. A watermark only rises, whatever order trims
    /// arrive in, as [`DedupIndex::trim`]'s does.
    pub fn trim(&mut self, generation: Generation, below: Seq) {
        self.below
            .entry(generation)
            .and_modify(|mark| *mark = (*mark).max(below))
            .or_insert(below);
    }

    /// Remember `RetireGeneration{generation}` when `floor` shows it is behind a generation this
    /// node already knows of. `floor` is the node's generation floor: the generation it serves
    /// when it holds an instance, so exactly the retires that instance accepts (A-R71, R9).
    /// Every instance created later serves a generation above the floor, so a remembered retire
    /// is always of a generation behind the one served. With no floor nothing is remembered: the
    /// retire could be of the generation being served elsewhere, and retiring that one would let
    /// a retry execute twice.
    pub fn retire(&mut self, generation: Generation, floor: Option<Generation>) {
        if floor.is_some_and(|floor| generation < floor) {
            self.retired.insert(generation);
        }
    }

    /// Apply every remembered trim and retire to `index`.
    ///
    /// Applied when an instance is created, before its seed loads. That is the same as after:
    /// the seed skips exactly the rows a trim would drop (`applied_at_seq < below`, against the
    /// watermark [`DedupIndex::trim`] records), and a retired generation takes no insert.
    pub fn apply(&self, index: &mut DedupIndex) {
        for (generation, below) in &self.below {
            index.trim(*generation, *below);
        }
        for generation in &self.retired {
            index.retire(*generation);
        }
    }
}

/// A decoded [`dedup_key`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupKey {
    /// The generation that applied the request.
    pub generation: Generation,
    /// Its affinity group.
    pub affinity: AffinityId,
    /// Its identity.
    pub identity: RequestIdentity,
}

/// A decoded [`dedup_value`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupValue {
    /// The payload digest it was applied with.
    pub request_digest: Digest,
    /// The sequence it was applied at.
    pub seq: Seq,
    /// The owner epoch of the lineage that applied it.
    pub owner_epoch: OwnerEpoch,
}

/// The inverse of [`dedup_key`], or `None` when `key` is not exactly [`DEDUP_KEY_LEN`] bytes.
#[must_use]
pub fn parse_dedup_key(key: &[u8]) -> Option<DedupKey> {
    let key: &[u8; DEDUP_KEY_LEN] = key.try_into().ok()?;
    let (generation, rest) = key.split_at(8);
    let (affinity, rest) = rest.split_at(8);
    let (tenant, rest) = rest.split_at(4);
    let (client, request) = rest.split_at(4);
    Some(DedupKey {
        generation: Generation(u64::from_be_bytes(generation.try_into().ok()?)),
        affinity: AffinityId(u64::from_be_bytes(affinity.try_into().ok()?)),
        identity: RequestIdentity {
            tenant: TenantId(u32::from_be_bytes(tenant.try_into().ok()?)),
            client: ClientId(u32::from_be_bytes(client.try_into().ok()?)),
            request: RequestId(u64::from_be_bytes(request.try_into().ok()?)),
        },
    })
}

/// The inverse of [`dedup_value`], or `None` when `value` is not exactly [`DEDUP_VALUE_LEN`]
/// bytes.
#[must_use]
pub fn parse_dedup_value(value: &[u8]) -> Option<DedupValue> {
    let value: &[u8; DEDUP_VALUE_LEN] = value.try_into().ok()?;
    let (digest, rest) = value.split_at(32);
    let (seq, owner_epoch) = rest.split_at(8);
    Some(DedupValue {
        request_digest: Digest(digest.try_into().ok()?),
        seq: Seq(u64::from_le_bytes(seq.try_into().ok()?)),
        owner_epoch: OwnerEpoch(u64::from_le_bytes(owner_epoch.try_into().ok()?)),
    })
}

/// The storage key of a dedup record: generation, affinity, tenant, client, request, all
/// big-endian. The generation leads, so the record is in its generation's namespace (design
/// §3.3's step 15) and a seed files it under the generation that applied it (A-R70).
#[must_use]
pub fn dedup_key(generation: Generation, affinity: AffinityId, identity: RequestIdentity) -> Bytes {
    let mut out = Vec::with_capacity(DEDUP_KEY_LEN);
    out.extend_from_slice(&generation.0.to_be_bytes());
    out.extend_from_slice(&affinity.0.to_be_bytes());
    out.extend_from_slice(&identity.tenant.0.to_be_bytes());
    out.extend_from_slice(&identity.client.0.to_be_bytes());
    out.extend_from_slice(&identity.request.0.to_be_bytes());
    Bytes::from(out)
}

/// The storage value of a dedup record: the request digest, then the sequence and the owner
/// epoch (`u64` LE each).
#[must_use]
pub fn dedup_value(request_digest: Digest, seq: Seq, owner_epoch: OwnerEpoch) -> Bytes {
    let mut out = Vec::with_capacity(DEDUP_VALUE_LEN);
    out.extend_from_slice(&request_digest.0);
    out.extend_from_slice(&seq.0.to_le_bytes());
    out.extend_from_slice(&owner_epoch.0.to_le_bytes());
    Bytes::from(out)
}
