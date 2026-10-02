//! The status index and its retention window (team kernel-a `design.md` §4.4).
//!
//! Keyed on `(Generation, RequestIdentity)`. There is no clock here: the 24-hour window arrives
//! as [`StatusIndex::trim`] and [`StatusIndex::retire`] watermarks computed outside the kernel,
//! the same discipline ADR-0025 uses for compaction.
//!
//! [`StatusIndex::lookup`] is a total function with three answers for an absent identity (lead
//! ruling A-R10), and no answer anywhere proves nonexecution (§4.3 invariant 5): there is no
//! `NotExecuted` variant to return by accident.
//!
//! The index holds at most [`StatusIndex::cap`] entries (plan §13 Q-4, K-A-12). Absent a trim it
//! stops there: a write that would add an entry is refused `OVERLOADED` and holds nothing, so the
//! identity answers `Unknown`. Overwrites, held entries and every answer are untouched.

use std::collections::{BTreeMap, BTreeSet};

use crate::contracts::errors::ErrorKind;
use crate::contracts::ids::{Generation, RequestIdentity, Seq};
use crate::contracts::publication::{StatusEntry, StatusOutcome};
use crate::contracts::recovery::RetainedStatusMap;
use crate::contracts::txn::{Outcome as TxnOutcome, TxnResult, TxnStatus};
use crate::transaction::RETENTION_CAP_ENTRIES;

/// The wire answer for `outcome` (test plan KA-9). Never [`TxnStatus::Unresolved`]: that is
/// T1's answer for a queue behind a freeze, and no state here maps to it.
///
/// `Rejected` has no producer in P1 (T1 answers a pre-admission rejection itself) and
/// [`TxnStatus`] has no member for it, so it answers `Unknown`: conservative, because `Unknown`
/// never proves nonexecution and a wrong `Expired` would claim the identity is unknowable.
#[must_use]
pub const fn to_wire(outcome: StatusOutcome) -> TxnStatus {
    match outcome {
        StatusOutcome::Published { result } => TxnStatus::Resolved(result),
        StatusOutcome::RecoveredApplied { result } => TxnStatus::Resolved(TxnResult {
            outcome: TxnOutcome::RecoveredApplied,
            ..result
        }),
        StatusOutcome::Unknown | StatusOutcome::Rejected { .. } => TxnStatus::Unknown,
        StatusOutcome::StatusExpired => TxnStatus::Expired,
    }
}

/// The per-request status index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusIndex {
    entries: BTreeMap<(Generation, RequestIdentity), StatusEntry>,
    /// Generations this index still speaks about, and the trim watermark in each. Present means
    /// "retained": absence of an identity there is ambiguous, never proof of nonexecution.
    retained_from_seq: BTreeMap<Generation, Seq>,
    /// Generations dropped by [`Self::retire`].
    retired: BTreeSet<Generation>,
    /// The most entries held at once, over every generation.
    cap: usize,
    /// The generations holding an entry for each request: `entries` keyed the other way round,
    /// so [`Self::lookup_any`] is logarithmic (PR #1 R1-F007). Never holds an empty set.
    by_request: BTreeMap<RequestIdentity, BTreeSet<Generation>>,
}

impl Default for StatusIndex {
    /// An empty index with the default cap, [`RETENTION_CAP_ENTRIES`].
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            retained_from_seq: BTreeMap::new(),
            retired: BTreeSet::new(),
            cap: RETENTION_CAP_ENTRIES,
            by_request: BTreeMap::new(),
        }
    }
}

impl StatusIndex {
    /// An index that speaks about `generation` and nothing else, capped at
    /// [`RETENTION_CAP_ENTRIES`].
    #[must_use]
    pub fn new(generation: Generation) -> Self {
        Self::with_cap(generation, RETENTION_CAP_ENTRIES)
    }

    /// [`Self::new`], holding at most `cap` entries.
    #[must_use]
    pub fn with_cap(generation: Generation, cap: usize) -> Self {
        let mut index = Self {
            cap,
            ..Self::default()
        };
        index.open(generation);
        index
    }

    /// The most entries this index holds at once (plan §13 Q-4: `retention_cap_entries`).
    #[must_use]
    pub const fn cap(&self) -> usize {
        self.cap
    }

    /// Insert `entry` at `key`, in both maps.
    fn hold(&mut self, key: (Generation, RequestIdentity), entry: StatusEntry) {
        self.entries.insert(key, entry);
        self.by_request.entry(key.1).or_default().insert(key.0);
    }

    /// Whether `key` is a new entry that the cap leaves no room for.
    fn full_for(&self, key: &(Generation, RequestIdentity)) -> bool {
        self.entries.len() >= self.cap && !self.entries.contains_key(key)
    }

    /// Start speaking about `generation`. A no-op for a retired or already open one.
    ///
    /// Not in the design's text: §4.4's `lookup` answers `StatusExpired` for a generation with
    /// no `retained_from_seq` entry, and nothing there creates one for the live generation until
    /// its first trim. Without this an absent identity in the live generation would answer
    /// `StatusExpired` — the "never held" answer — instead of `Unknown`, which is what M7A-115
    /// asserts.
    pub fn open(&mut self, generation: Generation) {
        if !self.retired.contains(&generation) {
            self.retained_from_seq
                .entry(generation)
                .or_insert(Seq::ZERO);
        }
    }

    /// Write `entry` over whatever the index held for its request in its generation.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Overloaded`] when the entry would be a new one and the index is at its
    /// [`Self::cap`]. Nothing is written; the generation is still opened, so the identity answers
    /// `Unknown`, never `StatusExpired`.
    pub fn record(&mut self, entry: StatusEntry) -> Result<(), ErrorKind> {
        self.open(entry.lineage.generation);
        let key = (entry.lineage.generation, entry.request);
        if self.full_for(&key) {
            return Err(ErrorKind::Overloaded);
        }
        self.hold(key, entry);
        Ok(())
    }

    /// The entry for `request` in `generation`, if one is held.
    #[must_use]
    pub fn entry(&self, generation: Generation, request: RequestIdentity) -> Option<&StatusEntry> {
        self.entries.get(&(generation, request))
    }

    /// The answer for `request` in `generation` (lead ruling A-R10). Total; no default arm.
    #[must_use]
    pub fn lookup(&self, request: RequestIdentity, generation: Generation) -> StatusOutcome {
        if let Some(entry) = self.entries.get(&(generation, request)) {
            return entry.outcome;
        }
        if self.retired.contains(&generation) {
            return StatusOutcome::StatusExpired;
        }
        match self.retained_from_seq.get(&generation) {
            Some(_) => StatusOutcome::Unknown,
            None => StatusOutcome::StatusExpired,
        }
    }

    /// Whether `generation` was dropped by [`Self::retire`] in this index's life — one boot.
    #[must_use]
    pub fn is_retired(&self, generation: Generation) -> bool {
        self.retired.contains(&generation)
    }

    /// The answer for `request` when the caller named no generation: the newest generation
    /// holding an entry for it, else `Unknown` — never `StatusExpired`, because without a
    /// generation nothing proves the one the client meant was retired (lead ruling A-R63).
    #[must_use]
    pub fn lookup_any(&self, request: RequestIdentity) -> StatusOutcome {
        self.by_request
            .get(&request)
            .and_then(BTreeSet::last)
            .and_then(|generation| self.entries.get(&(*generation, request)))
            .map_or(StatusOutcome::Unknown, |entry| entry.outcome)
    }

    /// Kernel-b §5.8's three-way rule over the predecessor generation's `Published` entries,
    /// each by its own `seq` (design §4.4, K-A-52). `uncertain` is tested first so it wins.
    pub fn fold_recovered(&mut self, map: &RetainedStatusMap) {
        let generation = map.predecessor_generation;
        let held = self
            .entries
            .iter_mut()
            .filter(|((gen, _), _)| *gen == generation);
        for (_, entry) in held {
            let (StatusOutcome::Published { result }, Some(seq)) = (entry.outcome, entry.seq)
            else {
                continue;
            };
            entry.outcome = recovered_outcome(map, seq, result);
        }
    }

    /// Hold `entry` unless the index already speaks for its request in its generation, the
    /// generation is retired, or a trim watermark covers its sequence. The seed's write
    /// (M7A-194): a durable row never overrides what this instance recorded itself, and never
    /// resurrects what a `StatusTrim` or `RetireGeneration` already dropped **in this boot**,
    /// exactly as `DedupIndex::seed` respects T1's trims. Opens the generation, so absence beside
    /// a seeded entry answers `Unknown`, as it does on the node that applied it.
    ///
    /// Retire and trim memory is per boot, like T1's `TrimMemory` (lead rulings A-R73a, A-R91
    /// Q1): after a restart the seed may load rows of a generation an earlier boot retired or
    /// trimmed, so the index answers a superset of what it answered before. That superset is
    /// truthful — a dedup row exists only for a request that was applied.
    ///
    /// Returns whether the entry was added.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Overloaded`] when the entry would be added and the index is at its
    /// [`Self::cap`], exactly as [`Self::record`] refuses. The generation is still opened.
    pub fn restore(&mut self, entry: StatusEntry) -> Result<bool, ErrorKind> {
        let generation = entry.lineage.generation;
        if self.retired.contains(&generation) {
            return Ok(false);
        }
        let floor = self
            .retained_from_seq
            .get(&generation)
            .copied()
            .unwrap_or(Seq::ZERO);
        if entry.seq.is_some_and(|seq| seq < floor) {
            return Ok(false);
        }
        self.open(generation);
        let key = (generation, entry.request);
        if self.entries.contains_key(&key) {
            return Ok(false);
        }
        if self.full_for(&key) {
            return Err(ErrorKind::Overloaded);
        }
        self.hold(key, entry);
        Ok(true)
    }

    /// Drop `generation`'s entries below `below` and remember the watermark. Absence below it
    /// then answers `Unknown`, not `StatusExpired`: the generation is still live.
    pub fn trim(&mut self, generation: Generation, below: Seq) {
        if self.retired.contains(&generation) {
            return;
        }
        let by_request = &mut self.by_request;
        self.entries.retain(|(gen, request), entry| {
            let keep = *gen != generation || entry.seq.is_some_and(|s| s >= below);
            if !keep {
                forget(by_request, *request, generation);
            }
            keep
        });
        let floor = self.retained_from_seq.entry(generation).or_insert(below);
        *floor = (*floor).max(below);
    }

    /// Drop `generation` whole. Every later lookup in it answers `StatusExpired`.
    pub fn retire(&mut self, generation: Generation) {
        let by_request = &mut self.by_request;
        self.entries.retain(|(gen, request), _| {
            let keep = *gen != generation;
            if !keep {
                forget(by_request, *request, generation);
            }
            keep
        });
        self.retained_from_seq.remove(&generation);
        self.retired.insert(generation);
    }

    /// How many entries are held. Never more than [`Self::cap`].
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no entry is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Drop `generation` from `request`'s set in [`StatusIndex::by_request`], and the set once empty.
fn forget(
    by_request: &mut BTreeMap<RequestIdentity, BTreeSet<Generation>>,
    request: RequestIdentity,
    generation: Generation,
) {
    if let Some(generations) = by_request.get_mut(&request) {
        generations.remove(&generation);
        if generations.is_empty() {
            by_request.remove(&request);
        }
    }
}

/// Kernel-b §5.8's three-way rule for one `Published` entry of the predecessor generation, by
/// its own `seq`: `uncertain` is tested first so it wins, then the discarded suffix, then the
/// retained prefix; above it the entry is past retention. Shared by [`StatusIndex::fold_recovered`]
/// and the failover seed (M7A-194), and both apply it to the **predecessor generation only**
/// (`map.predecessor_generation`): `fold_recovered` folds nothing else, and the seed loads an
/// older generation's row as `RecoveredApplied` (lead ruling A-R91, reviewer C-1). The rule is
/// kept in one place; the scope is kept by each caller, and
/// `m7a_194_status_seed_folds_only_the_predecessor_under_an_uncertain_map` compares the two.
#[must_use]
pub fn recovered_outcome(map: &RetainedStatusMap, seq: Seq, result: TxnResult) -> StatusOutcome {
    if map.uncertain || map.discarded_from.is_some_and(|d| seq >= d) {
        StatusOutcome::Unknown
    } else if seq <= map.retained_through {
        StatusOutcome::RecoveredApplied { result }
    } else {
        StatusOutcome::StatusExpired
    }
}
