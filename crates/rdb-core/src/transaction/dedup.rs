//! T1's dedup index (team kernel-a `design.md` §3.1).
//!
//! Keyed `(generation, affinity, identity)`, which is spec §5.3's scoping verbatim. Nothing in
//! here reads a clock: an entry's age is the sequence it was retained at (`applied_at_seq`), and
//! the 24 h window is enforced by whoever issues [`DedupIndex::trim`] and
//! [`DedupIndex::retire`]. T1 never drops an entry on its own, except an entry no `Published`
//! names: a condition failure, which it drops `dedup_retention_millis` after the step that
//! answered it ([`DedupIndex::expire_unpublished`], lead ruling A-R108). That age is event time,
//! the answering step's `StepCtx::now`, so still no clock. Absent a trim the index otherwise only
//! grows, and [`RETENTION_CAP_ENTRIES`] bounds that growth by refusing admission (plan Q-4).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;

use bytes::Bytes;

use crate::contracts::authority::Lineage;
use crate::contracts::digest::Digest;
use crate::contracts::ids::{
    AffinityId, ClientId, Generation, OwnerEpoch, PartitionId, RequestId, RequestIdentity, Seq,
    TenantId,
};
use crate::contracts::storage::{Namespace, SnapshotRead};
use crate::contracts::time::Tick;
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

/// Bits of a `Db` request id below its open's base: each open of an `rdb_api::Db` allocates its
/// request ids upward from `(n - 1) << OPEN_BITS` (`db.rs` `request_floor`). An ack floor covers
/// only the sender's own open (lead ruling A-R252, critic M3).
pub const OPEN_BITS: u32 = 40;

/// The counter bits of a `Db` request id. The counter value `OPEN_MASK` is never allocated: it
/// is the floor record's id ([`floor_record_id`]), and admission refuses a request that uses it.
pub const OPEN_MASK: u64 = (1 << OPEN_BITS) - 1;

/// The first app-chosen request id (`Db::put_as`). An ack floor never covers one: those ids get
/// the 24 h retention rule and the cap, unchanged (A-R241 A3).
pub const APP_ID_MIN: u64 = 1 << 63;

/// The affinity a floor record is filed under: one floor per `(generation, tenant, client,
/// open)`, whichever groups that open wrote (A-R252, critic M2).
pub const FLOOR_AFFINITY: AffinityId = AffinityId(0);

/// The most unsettled ids a request may pin below its floor (A-R241 M4). One constant with the
/// contract, so admission and the client cannot disagree.
pub use crate::contracts::txn::MAX_PINS;

/// The most dedup deletes T1 adds to one envelope. The rest carry to later requests (A-R241 M2).
pub const TRIM_PER_ENVELOPE: usize = 64;

/// The base of the open `request` was allocated in.
#[must_use]
pub const fn open_base(request: RequestId) -> u64 {
    request.0 & !OPEN_MASK
}

/// The reserved id the floor record of the open based at `base` is stored under.
#[must_use]
pub const fn floor_record_id(base: u64) -> RequestId {
    RequestId(base | OPEN_MASK)
}

/// Whether `request` is a floor record's reserved id: a `Db` id whose counter is [`OPEN_MASK`].
#[must_use]
pub const fn is_floor_record_id(request: RequestId) -> bool {
    request.0 < APP_ID_MIN && request.0 & OPEN_MASK == OPEN_MASK
}

/// Why a request's ack floor is refused `INVALID_ARGUMENT` (admission, A-R252 A2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloorRefusal {
    /// The request's own id is the reserved floor record id.
    ReservedId,
    /// A floor on an app-chosen id (at or above [`APP_ID_MIN`]).
    AppChosenId,
    /// The floor is above the request's own id.
    AboveOwnId,
    /// The floor is in another open than the request's own id.
    OtherOpen,
    /// More than [`MAX_PINS`] pinned ids, or a pin outside `[open base, floor)`.
    BadPins,
}

impl FloorRefusal {
    /// The field the `INVALID_ARGUMENT` names.
    #[must_use]
    pub const fn field(self) -> &'static str {
        match self {
            Self::ReservedId => "request",
            Self::AppChosenId | Self::AboveOwnId | Self::OtherOpen => "acked_below",
            Self::BadPins => "pinned",
        }
    }
}

/// Check a request's id, ack floor and pins (A-R252 A2, A-R241 M3). `acked_below` and `pinned`
/// are [`crate::contracts::txn::TxnRequest`]'s raw fields, so admission checks them before it
/// copies anything.
///
/// # Errors
///
/// The first [`FloorRefusal`] that applies.
pub fn check_floor(
    request: RequestId,
    acked_below: Option<u64>,
    pinned: &[u64],
) -> Result<(), FloorRefusal> {
    if is_floor_record_id(request) {
        return Err(FloorRefusal::ReservedId);
    }
    let Some(below) = acked_below else {
        return if pinned.is_empty() {
            Ok(())
        } else {
            Err(FloorRefusal::BadPins)
        };
    };
    if request.0 >= APP_ID_MIN {
        return Err(FloorRefusal::AppChosenId);
    }
    if below > request.0 {
        return Err(FloorRefusal::AboveOwnId);
    }
    if open_base(RequestId(below)) != open_base(request) {
        return Err(FloorRefusal::OtherOpen);
    }
    let base = open_base(request);
    if pinned.len() > MAX_PINS || pinned.iter().any(|pin| *pin < base || *pin >= below) {
        return Err(FloorRefusal::BadPins);
    }
    Ok(())
}

/// `pinned` as [`Floor::pinned`] holds it. [`check_floor`] bounds it to [`MAX_PINS`]; any more are
/// dropped.
fn pin_slots(pinned: &[RequestId]) -> [Option<RequestId>; MAX_PINS] {
    let mut slots = [None; MAX_PINS];
    for (slot, pin) in slots.iter_mut().zip(pinned) {
        *slot = Some(*pin);
    }
    slots
}

/// One ack floor: every `Db` id of its open below `below`, except `pinned`, is held by the
/// client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Floor {
    /// The first id not covered: the highest `acked_below` applied, never lowered to a pin.
    pub below: RequestId,
    /// The ids below `below` the client has not settled, as the request that set `below` (or
    /// the newest with the same `below`) carried them. In memory only: a seed from the floor
    /// record holds none (A-R274).
    pub pinned: [Option<RequestId>; MAX_PINS],
    /// The sequence of the envelope that last moved it: its age, as an entry's
    /// [`Retained::applied_at_seq`] is.
    pub at_seq: Seq,
}

impl Floor {
    /// Whether this floor covers `request`: below it and not pinned.
    #[must_use]
    pub fn covers(&self, request: RequestId) -> bool {
        request < self.below && !self.pinned.contains(&Some(request))
    }
}

/// The dedup deletes and floor record one envelope carries (A-R252 A3). Planned at step 13 from
/// the index as it stands, and applied to it only at that envelope's `Published`.
///
/// The record holds one id, the floor: `acked_below`, never lowered to a pin (A-R274). A resend
/// the floor covers answers `STATUS_EXPIRED`. A pin is exempt, so a pin's resend takes the
/// normal path: its kept entry replays it, and a pin that never applied applies. Every other id
/// below the floor keeps its protection after its entry is trimmed, so an app that resends a
/// request it holds an answer for (`Db::resend` is public) is answered `STATUS_EXPIRED`, never
/// applied twice. Pins are not in the record: after a restart a pin that never applied answers
/// `STATUS_EXPIRED` too, which is true (nothing was applied) and asks the app to submit it anew.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloorPlan {
    /// The serving generation.
    pub generation: Generation,
    /// The sender.
    pub tenant: TenantId,
    /// The sender.
    pub client: ClientId,
    /// The sender's open.
    pub open: u64,
    /// The floor the record carries: the higher of the held floor and `acked_below`.
    pub floor: RequestId,
    /// The request's `acked_below`, which [`DedupIndex::apply_floor`] compares with the floor
    /// it then holds to choose whose pins to keep.
    pub acked_below: RequestId,
    /// The request's pins.
    pub pinned: [Option<RequestId>; MAX_PINS],
    /// The entries deleted: the sender's open in the serving generation, below `acked_below`, not
    /// pinned, in any group, at most [`TRIM_PER_ENVELOPE`].
    pub deletes: Vec<(Generation, AffinityId, RequestIdentity)>,
}

impl FloorPlan {
    /// The floor record's identity.
    #[must_use]
    pub const fn record_identity(&self) -> RequestIdentity {
        RequestIdentity {
            tenant: self.tenant,
            client: self.client,
            request: floor_record_id(self.open),
        }
    }
}

/// The answer a retained identity replays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetainedAnswer {
    /// P1 published it; this is the result P1 published, replayed verbatim.
    Applied(TxnResult),
    /// Step 12 failed at this condition. Retained so a retry answered inside the retention window
    /// sees the answer the original submission saw, not a fresh evaluation against moved state
    /// (M7A-79). No sequence was allocated, so no `Published` ever names it: it ages by
    /// `answered_at` ([`DedupIndex::expire_unpublished`]) as well as by `applied_at_seq`, and
    /// whichever rule fires first drops it. Both fire at or after the retention window.
    ConditionFailed {
        /// The zero-based position [`crate::contracts::errors::RdbError::ConditionFailed`]
        /// named.
        index: u32,
        /// The `StepCtx::now` of the step that answered it. Expiry removes the entry only when
        /// this equals the tick it queued, so a stale queue slot never drops a later answer for
        /// the same key (lead ruling A-R108, K-1).
        answered_at: Tick,
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
    ///
    /// A condition failure also ages by time ([`RetainedAnswer::ConditionFailed`]'s
    /// `answered_at`). Under the host's trim duty the time rule always fires first, because the
    /// duty trims a sequence only once its own publish has aged; this counter is the rule that
    /// drops a failure in the simulator, which issues trims directly (A-R71, hunt_06).
    pub applied_at_seq: Seq,
}

/// The smallest key in `generation`. Every other field of the key is a dense unsigned identity,
/// whose zero is its minimum, so everything in an older generation sorts below this key.
const fn first_key(generation: Generation) -> (Generation, AffinityId, RequestIdentity) {
    (
        generation,
        AffinityId(0),
        RequestIdentity {
            tenant: TenantId(0),
            client: ClientId(0),
            request: RequestId(0),
        },
    )
}

/// What the index knows of one `Db` open in one generation: its ack floor, and the groups it
/// wrote, so a trim finds its entries in every group without a scan (A-R252 M2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct OpenState {
    floor: Option<Floor>,
    groups: BTreeSet<AffinityId>,
}

/// An [`OpenState`]'s key: the sender, its open's base, then the generation, so one open's
/// generations are adjacent.
type OpenKey = (TenantId, ClientId, u64, Generation);

const fn open_key(generation: Generation, identity: RequestIdentity) -> OpenKey {
    (
        identity.tenant,
        identity.client,
        open_base(identity.request),
        generation,
    )
}

/// The entry key of `request` in `generation` and `group`, sent by `open`'s sender.
const fn entry_key(
    generation: Generation,
    group: AffinityId,
    open: OpenKey,
    request: u64,
) -> (Generation, AffinityId, RequestIdentity) {
    (
        generation,
        group,
        RequestIdentity {
            tenant: open.0,
            client: open.1,
            request: RequestId(request),
        },
    )
}

/// Whether `entries` holds an entry of `open` in `group`.
fn holds_open(
    entries: &BTreeMap<(Generation, AffinityId, RequestIdentity), Retained>,
    open: OpenKey,
    group: AffinityId,
) -> bool {
    entries
        .range(
            entry_key(open.3, group, open, open.2)
                ..=entry_key(open.3, group, open, open.2 | OPEN_MASK),
        )
        .next()
        .is_some()
}

/// `BTreeMap<(Generation, AffinityId, RequestIdentity), Retained>` plus the two structures that
/// make "absent" a total answer: how far each generation has been trimmed, and which
/// generations are retired (design §3.1, K-A-12).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DedupIndex {
    entries: BTreeMap<(Generation, AffinityId, RequestIdentity), Retained>,
    retained_from_seq: BTreeMap<Generation, Seq>,
    retired_generations: BTreeSet<Generation>,
    /// Every entry [`Self::insert_unpublished`] retained, oldest answer first, with the tick it
    /// was answered at. A key stays queued after a trim or a newer answer removed or replaced
    /// its entry; [`Self::expire_unpublished`] then skips it.
    unpublished: VecDeque<(Tick, (Generation, AffinityId, RequestIdentity))>,
    /// Each `Db` open's ack floor and groups, per generation. Ids at or above [`APP_ID_MIN`]
    /// have none. The groups set is the side index that bounds the cross-group trim (critic
    /// rev 3, R3-2): [`Self::trim_plan`] ranges only over the groups this open wrote. A trim
    /// costs one range seek per such group, plus at most `limit` deletes and [`MAX_PINS`]
    /// skipped pins. It never walks another open's entries.
    opens: BTreeMap<OpenKey, OpenState>,
    /// How many of `opens` hold a floor.
    floors: usize,
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
    ///
    /// It runs on every admission, so it walks generations, not entries: one range probe finds
    /// the next older generation that holds anything, and one point lookup asks it for
    /// `identity`. The cost is bounded by how many older generations are held. A scan of every
    /// entry made admission linear in the index, and filling it to [`RETENTION_CAP_ENTRIES`]
    /// quadratic (lead ruling A-R76, M7A-90).
    #[must_use]
    pub fn older(
        &self,
        current: Generation,
        affinity: AffinityId,
        identity: RequestIdentity,
    ) -> Option<(Generation, &Retained)> {
        let mut below = current;
        while let Some(((generation, _, _), _)) = self.entries.range(..first_key(below)).next_back()
        {
            let generation = *generation;
            if !self.retired_generations.contains(&generation) {
                if let Some(retained) = self.entries.get(&(generation, affinity, identity)) {
                    return Some((generation, retained));
                }
            }
            below = generation;
        }
        None
    }

    /// Retain `retained` for `identity`. A retired generation retains nothing: it is gone by
    /// declaration, and a late insert would resurrect part of it.
    ///
    /// The trim watermark is ignored, on purpose. A watermark records what was dropped; it never
    /// filters what arrives. Respecting it would turn an over-eager trim into a second execution
    /// of a request already applied. Under the host contract `below` never passes a sequence that
    /// is not yet published, so no answer depends on this today.
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
        if identity.request.0 < APP_ID_MIN {
            self.opens
                .entry(open_key(generation, identity))
                .or_default()
                .groups
                .insert(affinity);
        }
        self.entries
            .insert((generation, affinity, identity), retained);
    }

    /// [`Self::insert`] an entry no `Published` will ever name, and queue it to expire
    /// `dedup_retention_millis` after `answered_at` ([`Self::expire_unpublished`]). `retained`
    /// is a [`RetainedAnswer::ConditionFailed`] stamped with the same `answered_at`.
    pub fn insert_unpublished(
        &mut self,
        generation: Generation,
        affinity: AffinityId,
        identity: RequestIdentity,
        retained: Retained,
        answered_at: Tick,
    ) {
        if self.retired_generations.contains(&generation) {
            return;
        }
        self.insert(generation, affinity, identity, retained);
        self.unpublished
            .push_back((answered_at, (generation, affinity, identity)));
    }

    /// Drop every entry [`Self::insert_unpublished`] retained whose answer is at least
    /// `retention_millis` old at `now`. Returns how many entries were dropped.
    ///
    /// An entry is removed only while it is still the answer that was queued: a
    /// [`RetainedAnswer::ConditionFailed`] whose `answered_at` is the queued tick. A key a trim
    /// dropped and a later answer reused, applied or failed again, keeps its own entry and
    /// window (lead ruling A-R108, K-1). The trim watermark is never touched: these entries are
    /// memory-only, never durable or seeded, and never outlive the instance, so nothing outside
    /// it needs to know they aged.
    ///
    /// Pops in queue order and stops at the first answer still inside the window, so a `now`
    /// that went backwards only delays expiry.
    pub fn expire_unpublished(&mut self, now: Tick, retention_millis: u64) -> usize {
        let mut dropped = 0;
        while let Some(&(queued, key)) = self.unpublished.front() {
            if queued.plus_millis(retention_millis) > now {
                break;
            }
            self.unpublished.pop_front();
            let still_queued = matches!(
                self.entries.get(&key),
                Some(Retained {
                    answer: RetainedAnswer::ConditionFailed { answered_at, .. },
                    ..
                }) if *answered_at == queued
            );
            if still_queued {
                self.entries.remove(&key);
                dropped += 1;
            }
        }
        dropped
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
        // A floor ages as an entry does. An open whose floor and entries are all gone is
        // forgotten, so a long generation does not keep one record per open it ever saw.
        let entries = &self.entries;
        let mut aged = 0;
        self.opens.retain(|key, state| {
            if key.3 != generation {
                return true;
            }
            if state.floor.is_some_and(|floor| floor.at_seq < below) {
                state.floor = None;
                aged += 1;
            }
            state.floor.is_some()
                || state
                    .groups
                    .iter()
                    .any(|group| holds_open(entries, *key, *group))
        });
        self.floors -= aged;
    }

    /// `RetireGeneration{generation}`: drop the whole generation and record that it is retired.
    pub fn retire(&mut self, generation: Generation) {
        self.entries.retain(|(g, _, _), _| *g != generation);
        let mut retired = 0;
        self.opens.retain(|key, state| {
            let keep = key.3 != generation;
            if !keep && state.floor.is_some() {
                retired += 1;
            }
            keep
        });
        self.floors -= retired;
        self.retired_generations.insert(generation);
    }

    /// Entries held, over every generation. A floor counts as one entry: it is held as one, and
    /// the cap bounds the memory either way (design rev 2, A4).
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len() + self.floors
    }

    /// Ack floors held, over every generation.
    #[must_use]
    pub const fn floor_count(&self) -> usize {
        self.floors
    }

    /// The ack floor of `identity`'s open in exactly `generation`.
    #[must_use]
    pub fn floor(&self, generation: Generation, identity: RequestIdentity) -> Option<Floor> {
        if identity.request.0 >= APP_ID_MIN {
            return None;
        }
        self.opens
            .get(&open_key(generation, identity))
            .and_then(|state| state.floor)
    }

    /// Whether a floor of `identity`'s open, in `current` or an older generation that is not
    /// retired, covers `identity` (A-R252 M3). T1 asks after step 11's `get` and `older` found
    /// nothing: such an id was trimmed under a floor, so it answers `STATUS_EXPIRED` and is not
    /// applied again.
    #[must_use]
    pub fn below_floor(&self, current: Generation, identity: RequestIdentity) -> bool {
        if identity.request.0 >= APP_ID_MIN {
            return false;
        }
        let first = open_key(Generation(0), identity);
        let last = open_key(current, identity);
        self.opens.range(first..=last).any(|(key, state)| {
            !self.retired_generations.contains(&key.3)
                && state
                    .floor
                    .is_some_and(|floor| floor.covers(identity.request))
        })
    }

    /// Plan the floor record and deletes the next envelope of `identity`'s request carries, or
    /// `None` when it would change nothing (A-R252 A3). Applied by [`Self::apply_floor`] at that
    /// envelope's `Published`, never before.
    ///
    /// The floor only rises: a plan never lowers one already held in `serving`. Deletes are the
    /// open's entries in `serving` below `acked_below`, minus `pinned`, group by group and lowest
    /// id first, at most `limit`; the rest wait for a later request. An older generation's
    /// entries are left to `RetireGeneration` and the 24 h trim (design rev 2 §2). `acked_below`
    /// and `pinned` passed [`check_floor`].
    #[must_use]
    pub fn trim_plan(
        &self,
        serving: Generation,
        identity: RequestIdentity,
        acked_below: RequestId,
        pinned: &[RequestId],
        limit: usize,
    ) -> Option<FloorPlan> {
        if identity.request.0 >= APP_ID_MIN || self.retired_generations.contains(&serving) {
            return None;
        }
        let held = self.floor(serving, identity).map(|floor| floor.below);
        let floor = held.map_or(acked_below, |held| held.max(acked_below));
        let base = open_base(identity.request);
        let mut deletes = Vec::new();
        let key = open_key(serving, identity);
        let groups = self.opens.get(&key).map(|state| &state.groups);
        'groups: for group in groups.into_iter().flatten() {
            let range = entry_key(serving, *group, key, base)
                ..entry_key(serving, *group, key, acked_below.0);
            for (entry, _) in self.entries.range(range) {
                if deletes.len() == limit {
                    break 'groups;
                }
                if !pinned.contains(&entry.2.request) {
                    deletes.push(*entry);
                }
            }
        }
        if deletes.is_empty() && held == Some(floor) {
            return None;
        }
        Some(FloorPlan {
            generation: serving,
            tenant: identity.tenant,
            client: identity.client,
            open: base,
            floor,
            acked_below,
            pinned: pin_slots(pinned),
            deletes,
        })
    }

    /// Apply `plan` at the `Published` of the envelope that carried it, sequenced at `at_seq`:
    /// drop its deletes and move the floor. A retired generation takes nothing.
    pub fn apply_floor(&mut self, plan: &FloorPlan, at_seq: Seq) {
        for key in &plan.deletes {
            self.entries.remove(key);
        }
        self.set_floor(
            plan.generation,
            plan.record_identity(),
            plan.acked_below,
            plan.pinned,
            at_seq,
        );
    }

    /// Hold `below` as the floor of `identity`'s open in `generation`, with `pinned`, unless a
    /// higher one is held. Its age becomes `at_seq` either way: the record was written again at
    /// `at_seq`.
    ///
    /// Pins follow the highest `below`: a request with a lower one was compiled earlier, so its
    /// pins are older and are dropped. At an equal `below` only the pins both carry are kept,
    /// since a client only ever settles pins below a floor it has already sent (A-R274).
    fn set_floor(
        &mut self,
        generation: Generation,
        identity: RequestIdentity,
        below: RequestId,
        pinned: [Option<RequestId>; MAX_PINS],
        at_seq: Seq,
    ) {
        if self.retired_generations.contains(&generation) {
            return;
        }
        let state = self
            .opens
            .entry(open_key(generation, identity))
            .or_default();
        let (below, pinned) = match state.floor {
            None => {
                self.floors += 1;
                (below, pinned)
            }
            Some(held) if held.below < below => (below, pinned),
            Some(held) if held.below == below => {
                let mut kept = [None; MAX_PINS];
                let both = held
                    .pinned
                    .into_iter()
                    .filter(|pin| pin.is_some() && pinned.contains(pin));
                for (slot, pin) in kept.iter_mut().zip(both) {
                    *slot = pin;
                }
                (below, kept)
            }
            Some(held) => (held.below, held.pinned),
        };
        state.floor = Some(Floor {
            below,
            pinned,
            at_seq,
        });
    }

    /// Whether nothing is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    /// Whether an entry of exactly `generation` has `applied_at_seq <= through`: whether a
    /// `DedupTrim{generation, below: through + 1}` would drop anything. With `Seq(u64::MAX)`,
    /// whether a `RetireGeneration{generation}` would.
    ///
    /// It exists for the host's cap retry hint (ruling A-R187, A-R209), which may name only an
    /// event that frees at least one entry. Read-only; walks one generation's entries and stops
    /// at the first hit, so it costs at most that generation's size.
    #[must_use]
    pub fn holds_through(&self, generation: Generation, through: Seq) -> bool {
        let end = generation
            .0
            .checked_add(1)
            .map_or(Bound::Unbounded, |next| {
                Bound::Excluded(first_key(Generation(next)))
            });
        self.entries
            .range((Bound::Included(first_key(generation)), end))
            .any(|(_, retained)| retained.applied_at_seq <= through)
            || self.opens.iter().any(|(key, state)| {
                key.3 == generation && state.floor.is_some_and(|floor| floor.at_seq <= through)
            })
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
            if key.affinity == FLOOR_AFFINITY && is_floor_record_id(key.identity.request) {
                self.set_floor(
                    key.generation,
                    key.identity,
                    floor_of_value(&value),
                    [None; MAX_PINS],
                    value.seq,
                );
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
///
/// It holds nothing about condition failures. They are never durable and never seeded, so no
/// later instance can hold one, and their time age ([`DedupIndex::expire_unpublished`]) is local
/// to the instance that answered them.
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

/// The storage value of a floor record, stored under [`dedup_key`] with [`FLOOR_AFFINITY`] and
/// [`FloorPlan::record_identity`]: a [`dedup_value`] whose digest field carries the floor (`u64`
/// LE, then zeros). No request has that digest, because no request has that id.
#[must_use]
pub fn floor_value(below: RequestId, seq: Seq, owner_epoch: OwnerEpoch) -> Bytes {
    let mut digest = [0; 32];
    digest[..8].copy_from_slice(&below.0.to_le_bytes());
    dedup_value(Digest(digest), seq, owner_epoch)
}

/// The floor a decoded [`floor_value`] carries.
pub fn floor_of_value(value: &DedupValue) -> RequestId {
    let mut below = [0; 8];
    below.copy_from_slice(&value.request_digest.0[..8]);
    RequestId(u64::from_le_bytes(below))
}

#[cfg(test)]
mod tests {
    //! The ack floor's index half (A-R252). Unit checks of one file's state, not `M7*` rows, so
    //! no `#[retcd_test]`, as in `contracts::ignore`. The floor's behaviour through T1 is pinned
    //! by rows D1-D10.

    use std::collections::BTreeMap;

    use bytes::Bytes;

    use super::{
        check_floor, dedup_key, dedup_value, floor_record_id, floor_value, DedupIndex,
        FloorRefusal, Retained, RetainedAnswer, SeedPending, FLOOR_AFFINITY, MAX_PINS, OPEN_BITS,
        TRIM_PER_ENVELOPE,
    };
    use crate::contracts::authority::Lineage;
    use crate::contracts::digest::Digest;
    use crate::contracts::ids::{
        AffinityId, ClientId, Generation, OwnerEpoch, PartitionId, RequestId, RequestIdentity, Seq,
        SnapshotHandle, TenantId,
    };
    use crate::contracts::storage::{Namespace, SnapshotRead};
    use crate::contracts::trace::Version;
    use crate::contracts::txn::{Durability, Outcome, TxnResult};

    const G1: Generation = Generation(1);
    const G2: Generation = Generation(2);
    const A: AffinityId = AffinityId(1);
    const B: AffinityId = AffinityId(7);
    /// The second open's base: its ids are `OPEN2 + n`.
    const OPEN2: u64 = 1 << OPEN_BITS;

    fn id(request: u64) -> RequestIdentity {
        RequestIdentity {
            tenant: TenantId(3),
            client: ClientId(4),
            request: RequestId(request),
        }
    }

    fn result(lineage: Lineage, seq: Seq) -> TxnResult {
        TxnResult {
            partition: lineage.partition,
            owner_epoch: lineage.owner_epoch,
            generation: lineage.generation,
            seq,
            outcome: Outcome::Published,
            durability: Durability::BufferedOnTwo,
        }
    }

    fn applied(seq: u64) -> Retained {
        let lineage = Lineage {
            partition: PartitionId(0),
            generation: G1,
            owner_epoch: OwnerEpoch(1),
        };
        Retained {
            request_digest: Digest([1; 32]),
            answer: RetainedAnswer::Applied(result(lineage, Seq(seq))),
            applied_at_seq: Seq(seq),
        }
    }

    /// Ids `OPEN2 + 1 ..= OPEN2 + n` in generation `g`, alternating groups A and B.
    fn filled(g: Generation, n: u64) -> DedupIndex {
        let mut index = DedupIndex::default();
        for i in 1..=n {
            let group = if i % 2 == 0 { B } else { A };
            index.insert(g, group, id(OPEN2 + i), applied(i));
        }
        index
    }

    fn requests(plan: &super::FloorPlan) -> Vec<(u64, u64)> {
        let mut out: Vec<_> = plan
            .deletes
            .iter()
            .map(|(g, _, i)| (g.0, i.request.0 - OPEN2))
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn check_floor_refuses_every_out_of_scope_floor() {
        let own = RequestId(OPEN2 + 10);
        let below = Some(OPEN2 + 5);
        assert_eq!(check_floor(own, below, &[OPEN2 + 2]), Ok(()));
        assert_eq!(check_floor(own, None, &[]), Ok(()));
        let reserved = floor_record_id(OPEN2);
        assert_eq!(
            check_floor(reserved, None, &[]),
            Err(FloorRefusal::ReservedId)
        );
        let app = RequestId(1 << 63);
        let app_floor = check_floor(app, Some(1), &[]);
        assert_eq!(app_floor, Err(FloorRefusal::AppChosenId));
        assert_eq!(
            check_floor(app, None, &[]),
            Ok(()),
            "an app id with no floor is admitted"
        );
        let above = Some(OPEN2 + 11);
        assert_eq!(check_floor(own, above, &[]), Err(FloorRefusal::AboveOwnId));
        let other = check_floor(own, Some(5), &[]);
        assert_eq!(other, Err(FloorRefusal::OtherOpen));
        let no_floor = check_floor(own, None, &[OPEN2]);
        assert_eq!(no_floor, Err(FloorRefusal::BadPins));
        let at_floor = check_floor(own, below, &[OPEN2 + 5]);
        assert_eq!(at_floor, Err(FloorRefusal::BadPins));
        let other_open = check_floor(own, below, &[3]);
        assert_eq!(other_open, Err(FloorRefusal::BadPins));
        let nine: Vec<_> = (0..=MAX_PINS as u64).map(|i| OPEN2 + 1 + i).collect();
        let own = RequestId(OPEN2 + 100);
        let below = Some(OPEN2 + 50);
        assert_eq!(check_floor(own, below, &nine[..MAX_PINS]), Ok(()));
        assert_eq!(check_floor(own, below, &nine), Err(FloorRefusal::BadPins));
        assert_eq!(FloorRefusal::BadPins.field(), "pinned");
    }

    #[test]
    fn a_plan_trims_every_group_keeps_pins_and_moves_the_floor_only_at_apply() {
        let mut index = filled(G1, 10);
        let sender = id(OPEN2 + 11);
        let pin = RequestId(OPEN2 + 4);
        let plan = index
            .trim_plan(G1, sender, RequestId(OPEN2 + 8), &[pin], TRIM_PER_ENVELOPE)
            .expect("a plan");
        let below_8 = [(1, 1), (1, 2), (1, 3), (1, 5), (1, 6), (1, 7)];
        assert_eq!(
            requests(&plan),
            below_8,
            "both groups trimmed below 8, pin 4 kept"
        );
        // A-R274: the floor is `acked_below`, never lowered to a pin; the pin is exempt.
        assert_eq!(plan.floor, RequestId(OPEN2 + 8), "the floor is acked_below");
        assert_eq!(plan.record_identity().request, floor_record_id(OPEN2));
        // Planned, not applied: nothing moved yet (A3).
        assert_eq!(index.len(), 10);
        assert_eq!(index.floor(G1, sender), None);
        assert!(!index.below_floor(G1, id(OPEN2 + 1)));

        index.apply_floor(&plan, Seq(11));
        assert_eq!(
            index.len(),
            5,
            "4, 8, 9 and 10 remain, and the floor counts as one"
        );
        assert!(index.get(G1, B, id(OPEN2 + 4)).is_some(), "the pin replays");
        assert!(index.below_floor(G1, id(OPEN2 + 3)));
        assert!(
            !index.below_floor(G1, id(OPEN2 + 4)),
            "a pin is exempt from the floor"
        );
        for above_the_pin in 5..8 {
            assert!(
                index.below_floor(G1, id(OPEN2 + above_the_pin)),
                "r{above_the_pin}: between the pin and acked_below is still covered (A-R274)"
            );
        }
        assert!(!index.below_floor(G1, id(5)), "another open is not covered");
        assert_eq!(index.floor_count(), 1);
    }

    #[test]
    fn a_plan_is_bounded_and_the_rest_carries_to_the_next() {
        let mut index = filled(G1, 10);
        let sender = id(OPEN2 + 11);
        let below = RequestId(OPEN2 + 11);
        let mut sizes = Vec::new();
        for at in 11..14 {
            let plan = index.trim_plan(G1, sender, below, &[], 4).expect("a plan");
            sizes.push(plan.deletes.len());
            index.apply_floor(&plan, Seq(at));
        }
        assert_eq!(sizes, [4, 4, 2]);
        assert_eq!(index.len(), 1, "only the floor is left");
        let after = index.trim_plan(G1, sender, below, &[], 4);
        assert_eq!(
            after, None,
            "no deletes and an unchanged floor need no record"
        );
    }

    /// A-R274: pins follow the floor that carried them. At the same `acked_below` a settled pin
    /// loses its exemption; a higher `acked_below` takes the newer request's pins whole.
    #[test]
    fn a_settled_pin_loses_its_exemption_and_pins_follow_the_highest_floor() {
        let mut index = filled(G1, 10);
        let sender = id(OPEN2 + 11);
        let (four, six) = (RequestId(OPEN2 + 4), RequestId(OPEN2 + 6));
        let covered = |index: &DedupIndex, n: u64| index.below_floor(G1, id(OPEN2 + n));
        let plan = index
            .trim_plan(G1, sender, RequestId(OPEN2 + 8), &[four, six], 64)
            .expect("a plan");
        index.apply_floor(&plan, Seq(11));
        assert_eq!(
            (covered(&index, 4), covered(&index, 5), covered(&index, 6)),
            (false, true, false)
        );

        let settled = index
            .trim_plan(G1, sender, RequestId(OPEN2 + 8), &[four], 64)
            .expect("6 settled: its entry goes");
        index.apply_floor(&settled, Seq(12));
        assert!(covered(&index, 6), "a settled pin is covered");
        assert!(!covered(&index, 4), "4 is still pinned");

        let higher = index
            .trim_plan(G1, sender, RequestId(OPEN2 + 10), &[], 64)
            .expect("a higher floor");
        index.apply_floor(&higher, Seq(13));
        assert!(covered(&index, 4), "the newer request pins nothing");
        assert!(!covered(&index, 10), "nor reaches its own floor");
    }

    #[test]
    fn a_floor_never_falls() {
        let mut index = filled(G1, 4);
        let sender = id(OPEN2 + 5);
        let below = RequestId(OPEN2 + 4);
        let plan = index.trim_plan(G1, sender, below, &[], 64).expect("a plan");
        index.apply_floor(&plan, Seq(5));
        let lower = index.trim_plan(G1, sender, below, &[RequestId(OPEN2 + 2)], 64);
        assert_eq!(
            lower, None,
            "a lower pin neither lowers the floor nor needs a record"
        );
        assert_eq!(index.floor(G1, sender).map(|f| f.below), Some(below));
    }

    #[test]
    fn app_ids_have_no_floor() {
        let mut index = DedupIndex::default();
        let app = id(1 << 63);
        index.insert(G1, A, app, applied(1));
        assert_eq!(index.trim_plan(G1, app, RequestId(1), &[], 64), None);
        assert!(!index.below_floor(G1, app));
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn a_plan_trims_older_generations_and_an_older_floor_still_covers() {
        let mut index = filled(G1, 4);
        let sender = id(OPEN2 + 3);
        let plan = index
            .trim_plan(G1, sender, RequestId(OPEN2 + 3), &[], 64)
            .expect("a plan");
        index.apply_floor(&plan, Seq(5));
        // A new generation serves: its plan leaves the older one's entries to retire and age.
        index.insert(G2, A, id(OPEN2 + 5), applied(6));
        let sender = id(OPEN2 + 6);
        let plan = index
            .trim_plan(G2, sender, RequestId(OPEN2 + 6), &[], 64)
            .expect("a plan");
        assert_eq!(requests(&plan), [(2, 5)]);
        assert!(
            index.below_floor(G2, id(OPEN2 + 1)),
            "G1's floor covers a resend under G2"
        );
        index.retire(G1);
        assert!(
            !index.below_floor(G2, id(OPEN2 + 1)),
            "a retired generation's floor is gone"
        );
        assert_eq!(index.floor_count(), 0, "nor counted against the cap");
    }

    #[test]
    fn a_trim_ages_a_floor_and_forgets_an_empty_open() {
        let mut index = filled(G1, 4);
        let sender = id(OPEN2 + 5);
        let plan = index
            .trim_plan(G1, sender, RequestId(OPEN2 + 3), &[], 64)
            .expect("a plan");
        index.apply_floor(&plan, Seq(5));
        index.trim(G1, Seq(5));
        assert_eq!(
            index.floor_count(),
            1,
            "a floor written at 5 survives a trim below 5"
        );
        index.trim(G1, Seq(6));
        assert_eq!(index.floor_count(), 0);
        assert!(index.is_empty());
        assert!(
            index.opens.is_empty(),
            "nothing is kept for an open once all of it aged"
        );
    }

    struct Rows {
        at: Seq,
        rows: BTreeMap<Bytes, Bytes>,
    }

    impl SnapshotRead for Rows {
        fn handle(&self) -> SnapshotHandle {
            SnapshotHandle(self.at.0)
        }
        fn at(&self) -> Seq {
            self.at
        }
        fn generation(&self) -> Generation {
            G2
        }
        fn get(&self, _ns: Namespace, key: &[u8]) -> Option<Bytes> {
            self.rows.get(key).cloned()
        }
        fn version(&self, _ns: Namespace, _key: &[u8]) -> Option<Version> {
            None
        }
        fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
            if ns != Namespace::Dedup {
                return Vec::new();
            }
            self.rows
                .range(Bytes::copy_from_slice(from)..)
                .take(limit)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
    }

    #[test]
    fn a_seed_learns_the_floor_from_its_record() {
        let record = RequestIdentity {
            request: floor_record_id(OPEN2),
            ..id(0)
        };
        let below = RequestId(OPEN2 + 7);
        let floor_row = (
            dedup_key(G1, FLOOR_AFFINITY, record),
            floor_value(below, Seq(8), OwnerEpoch(1)),
        );
        let entry_row = (
            dedup_key(G1, A, id(OPEN2 + 7)),
            dedup_value(Digest([1; 32]), Seq(7), OwnerEpoch(1)),
        );
        let rows = Rows {
            at: Seq(9),
            rows: BTreeMap::from([floor_row, entry_row]),
        };
        let pending = SeedPending {
            partition: PartitionId(0),
            serving: G2,
            retained_through: Seq(9),
        };
        let mut index = DedupIndex::default();
        let added = index.seed(&pending, &rows, result);
        assert_eq!(added, Ok(1), "the floor record is not an entry");
        let floor = index.floor(G1, id(OPEN2 + 9)).map(|f| (f.below, f.at_seq));
        assert_eq!(floor, Some((below, Seq(8))));
        assert!(index.below_floor(G2, id(OPEN2 + 6)));
        assert!(!index.below_floor(G2, id(OPEN2 + 7)));
        assert!(index.get(G1, FLOOR_AFFINITY, record).is_none());
    }
}
