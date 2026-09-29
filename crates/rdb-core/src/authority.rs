//! Fenced grants, epochs and coherent watch resync.
//!
//! Holds the grant state machine and answers the four revalidation gates of spec §5.2 —
//! admission, storage dispatch, publication and reply. Uncertainty is not a tie: when the
//! bounded-clock comparison cannot place the current instant on one side of the grant's expiry,
//! the answer is deny.
//!
//! **Owner:** team kernel-a, package A1. Specification: spec §7.2, §7.3.
//!
//! # What is wired (2026-09-22, phase 1 of the reach work)
//!
//! Package A1 used to answer one seam. [`Module::step`] returned
//! [`crate::contracts::errors::RdbError::Unavailable`] for every event that was not an
//! [`EventKind::Control`], with one message for all of them, so nothing about a grant could be
//! driven by hand. What is built now:
//!
//! * **The state is data.** [`AuthorityState`] carries a [`Held`] with the nine items team
//!   kernel-a `design.md` §2.1 declares, an `Unheld` that remembers its last fence, and a
//!   `Fenced` that carries its reason and its tick (lead ruling A-R28).
//! * **The clock is read from the context, every step.** [`clock::ClockView`] holds the last
//!   accepted [`crate::contracts::time::ControlTime`]; there is no clock event and no tick event
//!   (lead ruling A-R27).
//! * **Both admission conjuncts are evaluated on every step, whatever the event**
//!   (`Authority::revalidate`). Not on a periodic wake — see that method for why the
//!   difference is a fencing question and not a test convenience.
//! * **Every self-fence trigger.** Ten `(scope, reason)` pairs over thirteen triggers, seven
//!   node-scoped and three partition-scoped (lead ruling A-R33). `Authority::fence` is the one
//!   emitter, so the K-A-49 pairing holds by construction.
//! * **[`Authority::may_admit`]**, on `&self` rather than on `&Held`, returning a
//!   [`Verdict`] and never a `bool`, so a fixture can ask an `Unheld` or `Fenced` kernel and get
//!   the deny it should give (lead rulings A-R29, A-R37).
//! * **The `Check` → `Answer` pair**, so the three asynchronous checkpoints of `design.md` §2.5
//!   are drivable (lead ruling A-R29).
//! * **One read-only state view**, [`Authority::view`] → [`AuthorityStateView`], rather than an
//!   accessor per field (lead ruling A-R37, the Manual Tester's B6).
//! * **[`Authority::timer_kind`]**, so a row can tell a renewal wake from an acquisition wake
//!   from a clock wake through an otherwise opaque [`crate::contracts::ids::TimerId`]
//!   (lead ruling A-R37, B2).
//!
//! # What is deliberately still unavailable, and how it says so
//!
//! An arm that is not built returns an `Unavailable` **naming its transition** — never the old
//! blanket "package A1 answers only the control seam in this build", which told a reader nothing
//! about which row they had reached. Still owed:
//!
//! * ~~the acquisition and renewal CAS rows, and the healthy grant-record read-back~~ — built
//!   under lead ruling A-R47 (item §3.1). The `Fenced | ReadOk` exit row (a record naming a
//!   *new* grant id) is not: `Fenced` does not remember the old id it would compare against;
//! * ~~the **post-`Recovered`** install (`Fact(LineageInstalled)`) and the `Recovered` trigger
//!   row that issues its read~~ — built under lead ledger L-R177gf and lead ruling A-R78;
//! * ~~the whole §2.6 activation side~~ — built under lead ruling A-R51 (`design.md` §2.6a,
//!   T1–T13): `Takeover`, [`EventKind::ExternalFenceVerified`] and
//!   [`crate::contracts::authority::AuthorityEffect::FenceProven`]. No module consumes the proof
//!   yet; the simulator refuses it by name (A-R49).
//!
//! # Vocabulary A1 needs — and appends itself (lead rulings A-R41, A-R43)
//!
//! Five outcomes had no name. Four are now [`AuthorityIgnoreReason`] variants —
//! [`AuthorityIgnoreReason::NotOurs`], [`AuthorityIgnoreReason::LineageUnchanged`],
//! [`AuthorityIgnoreReason::BootUnchanged`],
//! [`AuthorityIgnoreReason::ResumeGapWithinTolerance`] — and one is an
//! [`AuthorityFact`]: [`AuthorityFact::WatchAdmissionExhausted`], **against** this module's own
//! earlier recommendation. The at-cap row does not decline once, it *latches*; latching is an
//! act, and being `AdmissionRefused`'s twin is what makes sharing an enum with it dangerous
//! rather than natural — as adjacent unit variants, a row asserting the cap passes on the
//! under-cap reason.
//!
//! **An earlier draft of this header said `contracts/authority.rs` is not kernel-a's to edit.
//! That was false, and the file it is written in says so.** [`crate::contracts::ignore`]'s rule
//! is that *"a kernel adds a reason name by appending one variant to its own leaf enum … and
//! **never waits on foundation** for the append itself"*, and [`AuthorityFact`]'s own doc says
//! *"KERNEL-A owns every variant. Add one by editing this enum and nothing else."* The premise
//! took from the page that grants this module authority only the part that constrains it, and
//! three rows waited on a ruling that had already been given.
//!
//! Two of the five carried **no** marker: `Resumed{suspended_millis <= tolerance}` and
//! `Rebooted{boot == held.boot}` both emitted `Ignored(StaleTimer)`, and neither is about a
//! timer. Both mean *this lifecycle event is not a discontinuity*. A stand-in nobody marked is
//! worse than one that is marked, because a row written against it looks correct.
//!
//! [`Module::capability`] therefore still answers
//! [`CapabilityState::Unavailable`]: A1's advertised capability is the four gates *over a grant
//! it acquired and a lineage it installed*, and neither of those is built.
//!
//! # The rule the watch slice exists to hold
//!
//! ADR-rdb-0008 §7 item 4, as restated by lead ruling A-R15: **no coherent family reload occurs
//! unless a termination was delivered.** A watch invalidates a cache; it never grants authority
//! and it never, on its own, justifies a reload. The one thing that declares a gap is
//! [`WatchTermination::is_gap`], and rEtcd's stream does not skip silently (ADR-rdb-0008 §4), so
//! a live stream has nothing to reload against. A kernel that reloaded on a `Watched` or a
//! `WatchProgress` would turn cache invalidation into a poll, which is the exact defect the ADR
//! item exists to catch.
//!
//! [`ControlEffect::Reload`] is therefore emitted from **exactly one** match arm in this file:
//! the [`ControlEvent::WatchTerminated`] arm, guarded by `termination.is_gap()`.

pub mod clock;
pub mod grant;
pub mod partition;

use std::collections::{BTreeMap, BTreeSet};

use crate::contracts::authority::{
    AuthorityDecision, AuthorityEffect, AuthorityEvent, AuthorityFact, AuthorityIgnoreReason,
    AuthorityView, Checkpoint, DenyReason, ExternalFenceMismatch, FenceScope, FencingProof,
    Lineage, Revocation, Verdict,
};
use crate::contracts::control::{
    CasOutcome, ControlChange, ControlEffect, ControlEvent, ControlKey, ControlPrefix,
    ControlRecord, ReadOutcome, WatchTermination,
};
use crate::contracts::errors::{Capability, RdbError};
use crate::contracts::event::{
    Budgets, Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, ModuleName,
    NodeLifecycle, StepCtx,
};
use crate::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, ControlRequestId, CorrelationId, Generation,
    GrantId, NodeId, OwnerEpoch, PartitionId, Revision, TimerId, TimerVersion,
};
use crate::contracts::ignore::KernelIgnoredReason;
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::storage::{StorageEvent, StorageFault, StoreEffect};
use crate::contracts::time::{ControlTime, Tick, TimerEffect, TimerFired};
use crate::contracts::trace::CapabilityState;

use self::clock::{e_new, expiry_proven, local_ok, utc_ok, ClockMode, ClockView};
use self::grant::{GrantRecord, HeldIdentity};
use self::partition::{PartitionLifecycle, PartitionRecord};

/// How many consecutive [`WatchTermination::ResourceExhaustedFatal`] terminations A1 will
/// re-arm its watch through before it stops re-arming.
///
/// A capacity error is **not** a gap ([`WatchTermination::is_gap`] is `false` for it), so the
/// answer is a bounded back-off and never a reload. Reloading in a loop on an admission limit
/// turns a capacity error into an outage, which is the trap the termination type exists to
/// spell out.
pub const WATCH_ADMISSION_ATTEMPT_CAP: u32 = 3;

/// The delay before the **first** capacity re-arm, doubled per consecutive refusal up to
/// [`WATCH_BACKOFF_CAP_MILLIS`].
///
/// # Why this pair is a constant here and not a [`Budgets`] field
///
/// Lead ruling A-R36 moved three clock thresholds out of this module and into [`Budgets`],
/// because "a scenario that changes the grant duration and not the sample window is testing a
/// combination no operator can configure". That argument is about thresholds **coupled to
/// another budget** — the sample window against the grant duration. The watch back-off is
/// coupled to exactly one thing, [`WATCH_ADMISSION_ATTEMPT_CAP`], which is already a `pub const`
/// here, and putting one half of that pair in `Budgets` and leaving the other here is the split
/// A-R36 warns about rather than the fix for it.
///
/// It is `pub` for the same reason the attempt cap is: a row asserts the shape against the name,
/// never against a literal. Team kernel-a `design.md` says only "bounded backoff rearm" and names
/// no numbers, so neither of these is a value the design pins.
///
/// # What reverses this, and it is cheap when it happens
///
/// **If a scenario ever needs to *tune* the backoff rather than observe it, all three move to
/// [`Budgets`] together** — this pair and [`WATCH_ADMISSION_ATTEMPT_CAP`]. Splitting three
/// coupled thresholds across two homes is worse than either home, which is A-R36's own argument
/// rather than an exception to it. That edit is `contracts/event.rs` plus
/// [`crate::contracts::trace::BudgetName`], and it is safe now in a way it was not when A-R36
/// was written: `BudgetName::ALL` has an exhaustive-destructure coverage guard in
/// `crates/rdb-core/tests/contracts.rs`, so a field added and forgotten is a compile error.
pub const WATCH_BACKOFF_BASE_MILLIS: u64 = 50;

/// The ceiling the capacity re-arm delay doubles up to, and stays at.
///
/// See [`WATCH_BACKOFF_BASE_MILLIS`] for why the pair lives here.
pub const WATCH_BACKOFF_CAP_MILLIS: u64 = 2_000;

/// The first [`TimerId`] A1 owns. Every id in `[AUTHORITY_TIMER_BASE, +4)` is one of
/// [`AuthorityTimer`]'s.
///
/// A reserved block rather than a counter, so a fixture can author a
/// [`crate::contracts::time::TimerFired`] of a chosen kind without first making A1 arm one
/// (lead ruling A-R37, the Manual Tester's B2). Far from zero so that a default-constructed
/// `TimerId` is not accidentally one of A1's.
///
/// Every kernel module's timer block is its tag in bits 48..64 — A1 `0x00A1`, L1 `0x00B1`,
/// R1 `0x00C1`, P1 `0x00D1`, F1 `0x00F1` — with a kind in bits 32..48 and a partition in bits
/// 0..32, so no partition moves an id into another module's block (`tests/timer_ids.rs`).
pub const AUTHORITY_TIMER_BASE: u64 = 0x00A1 << 48;

/// The first [`ControlRequestId`] A1 owns: A1's tag in bits 48..64, as for its timers. The sim
/// offers every control answer to every module and F1 mints its own ids (in its `0x00F1` block),
/// so a block of its own keeps an A1 id from ever equalling F1's (lead ledger L-R177hs).
pub const AUTHORITY_CONTROL_REQUEST_BASE: u64 = 0x00A1 << 48;

/// Which of A1's timers a [`TimerId`] is.
///
/// [`TimerId`] is opaque — a bare `u64` a scenario may set to anything — so without this a row
/// that fires "the renewal wake" and a row that fires "the clock wake" are the same event, and a
/// row asserting that a *stale* wake is ignored cannot say which wake it staled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AuthorityTimer {
    /// `Unheld | AcquireDue`: the create-only CAS wake, and its back-off re-arm.
    Acquire,
    /// `Held | RenewDue`: the renewal CAS wake.
    Renew,
    /// The bounded-clock sample wake, armed every
    /// [`crate::contracts::event::Budgets::clock_sample_period_millis`].
    ///
    /// It does **not** own the admission decision — `Authority::revalidate` does, on every
    /// step. What this wake gives is a step at all when nothing else is arriving, so a node that
    /// has gone quiet still notices its own expiry.
    ClockWake,
    /// The watch re-arm back-off after a [`WatchTermination::ResourceExhaustedFatal`].
    WatchBackoff,
}

impl AuthorityTimer {
    /// Every kind, in id order. A row that must cover the whole vocabulary iterates this rather
    /// than listing four names that can fall behind a fifth.
    pub const ALL: [Self; 4] = [
        Self::Acquire,
        Self::Renew,
        Self::ClockWake,
        Self::WatchBackoff,
    ];

    /// Its offset inside A1's reserved block.
    const fn offset(self) -> u64 {
        match self {
            Self::Acquire => 0,
            Self::Renew => 1,
            Self::ClockWake => 2,
            Self::WatchBackoff => 3,
        }
    }

    /// The [`TimerId`] this kind is always armed under.
    ///
    /// One id per kind, not one per arming: a re-arm bumps the
    /// [`crate::contracts::ids::TimerVersion`], which is the mechanism
    /// [`crate::contracts::time::TimerEffect::Arm`] already has for making an older firing stale.
    #[must_use]
    pub const fn id(self) -> TimerId {
        TimerId(AUTHORITY_TIMER_BASE + self.offset())
    }

    /// The kind an id names, or `None` when the id is not A1's.
    #[must_use]
    pub const fn from_id(id: TimerId) -> Option<Self> {
        match id.0.checked_sub(AUTHORITY_TIMER_BASE) {
            Some(0) => Some(Self::Acquire),
            Some(1) => Some(Self::Renew),
            Some(2) => Some(Self::ClockWake),
            Some(3) => Some(Self::WatchBackoff),
            _ => None,
        }
    }
}

/// The lineage A1 serves for one partition, as a linearizable read of `partitions/{id}`
/// installed it (team kernel-a `design.md` §2.1).
///
/// One map of triples rather than three parallel maps keyed the same way: `config_version`
/// joined the other two when [`EffectKind::AdoptAuthority`] landed (lead ruling F-R10), and
/// three maps with one writer each is how they drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServedLineage {
    /// Its history incarnation.
    pub generation: Generation,
    /// The owner epoch inside that incarnation.
    pub owner_epoch: OwnerEpoch,
    /// The membership pin the required-copy predicate is bound to.
    pub config_version: ConfigVersion,
}

/// An outstanding renewal CAS (team kernel-a `design.md` §2.1).
///
/// # The request id is the `OpId`
///
/// `design.md` §2.1 spells both this and [`Acquire`] with an `op: OpId`, and §2.4 matches
/// completions with `op == renewal.op`. The contract's [`ControlRequestId`] is that identity:
/// A1 mints one per request, [`ControlEffect::Cas`] carries it, and [`ControlEvent::CasResult`]
/// echoes it. Until lead ledger L-R177hs the seam had no such field and the correlation stood in,
/// which a late answer to an earlier CAS under the same correlation could pass for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Renewal {
    /// The request id the CAS was sent as; only an answer echoing it is this CAS's.
    pub request: ControlRequestId,
    /// The tick the CAS was **dispatched** at. Becomes `Held::renewed_at` on commit, never the
    /// tick the completion arrived (finding K-A-06).
    pub dispatched_at: Tick,
    /// The expiry the CAS writes. Becomes `Held::expiry_utc_ms` on commit, and only then.
    pub e_new: i64,
}

/// An outstanding create-only acquisition CAS (team kernel-a `design.md` §2.1, finding K-A-36).
///
/// Same two fields and the same fate as [`Renewal`]; see that type for why the identity is a
/// request id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Acquire {
    /// The request id the CAS was sent as; only an answer echoing it is this CAS's.
    pub request: ControlRequestId,
    /// The tick the CAS was dispatched at.
    pub dispatched_at: Tick,
    /// The expiry the CAS writes.
    pub e_new: i64,
    /// The grant id the CAS writes. Becomes [`Held`]'s grant on commit.
    ///
    /// Not in `design.md` §2.1's spelling, which writes "grant{new id, ..}" in the CAS and never
    /// says where the id is kept until the commit. Without it the commit row would have to
    /// re-derive the id it wrote, and a re-derivation is a second source of truth.
    pub grant: GrantId,
}

/// A grant this node committed and has not lost (team kernel-a `design.md` §2.1).
///
/// `authority_seq`, the clock and the takeover map are **not** here. They outlive any one grant:
/// a clock sample arrives while `Unheld` and acquisition needs one, and `authority_seq` is never
/// reset, not even by a new grant. The K-A-39 sweep moved all three onto the kernel for exactly
/// that reason. The epoch revocations followed for the same one (lead ledger L-R178e): a
/// revocation is a fact about this node's disk, not about a grant, so a new grant must not start
/// without it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held {
    grant: GrantId,
    node: NodeId,
    boot: BootId,
    authority_generation: AuthorityGeneration,
    expiry_utc_ms: i64,
    record_revision: Revision,
    renewed_at: Tick,
    renewal: Option<Renewal>,
    served: BTreeMap<PartitionId, ServedLineage>,
    /// The control revision each `served` entry was installed at (lead ruling A-R48). Exactly
    /// the keys of `served`: every writer of one writes the other.
    served_revisions: BTreeMap<PartitionId, Revision>,
    /// Tombstones (lead ruling A-R48b): the newest revision known for each partition removed
    /// from `served`, the entry itself absent. No key is in both this and `served`.
    removed_revisions: BTreeMap<PartitionId, Revision>,
    partitions_revision: Revision,
    storage_fenced: BTreeSet<PartitionId>,
    /// The last linearizable read of `grants/{node}` answered `Unavailable`, and no quorum has
    /// answered since. Denies `ControlUnavailable` without ending the grant (`design.md` §2.4
    /// property 6; M7A-59). Cleared by the next read that finds our record, or by a committed
    /// renewal, which is itself a quorum answer (A-R78 F1).
    control_unavailable: bool,
}

impl Held {
    /// The identity a read-back of `grants/{node}` is classified against.
    ///
    /// The one accessor left on this type. The nine that used to sit beside it are gone: lead
    /// ruling **A-R37** replaced them with [`Authority::view`], one cloned struct, because nine
    /// getters make a row that compares four items into four assertions that can each be
    /// forgotten, and because `served` needs *replace versus merge* to be visible — a question
    /// no per-field getter and no effect vector can answer. This one survives because it is not
    /// a getter: it is the argument [`grant::classify`] takes, named so the comparison cannot
    /// silently read a field the rule does not mention.
    #[must_use]
    pub const fn identity(&self) -> HeldIdentity {
        HeldIdentity {
            grant: self.grant,
            boot: self.boot,
            authority_generation: self.authority_generation,
        }
    }

    /// Drop `id` from `served`, keeping a tombstone (lead ruling A-R48b).
    ///
    /// The tombstone is the newest of three: the revision the removal was learned at, the
    /// revision the removed entry was installed at, and any tombstone already there. A removal
    /// never lowers what the node knows about a key: a stale fencing read still fences, but it
    /// must not let a read older than the removed entry re-adopt.
    fn remove_served(&mut self, id: PartitionId, removed_at: Revision) {
        self.served.remove(&id);
        let installed = self.served_revisions.remove(&id).unwrap_or_default();
        let earlier = self.removed_revisions.get(&id).copied().unwrap_or_default();
        self.removed_revisions
            .insert(id, removed_at.max(installed).max(earlier));
    }

    /// The partition half of [`Authority::may_admit_at`]: why this grant cannot admit `lineage`,
    /// or `None` if the partition conjuncts all hold.
    ///
    /// Shared with the view builder (finding F2) so that a published view and the check it
    /// stands in for cannot disagree about a partition. Before it, the view judged the grant
    /// alone and published admitting views for a revoked epoch, a storage-fenced partition, and
    /// a partition this node does not serve.
    fn partition_deny(&self, lineage: Lineage, revoked: &RevokedEpochs) -> Option<DenyReason> {
        let current = self.served.get(&lineage.partition).is_some_and(|served| {
            served.generation == lineage.generation && served.owner_epoch == lineage.owner_epoch
        });
        if !current {
            return Some(DenyReason::GenerationChanged);
        }
        self.installed_deny(lineage.partition, lineage.owner_epoch, revoked)
    }

    /// The conjuncts of [`Self::partition_deny`] that survive an install: why `owner_epoch` of
    /// `partition` would be refused once it *is* the served lineage. Split out so that an install
    /// can ask before writing `served` (lead ruling A-R56.1).
    ///
    /// `revoked` is the kernel's set ([`Authority`]'s `revoked_epochs`), passed in because it no
    /// longer lives on a grant (lead ledger L-R178e).
    fn installed_deny(
        &self,
        partition: PartitionId,
        owner_epoch: OwnerEpoch,
        revoked: &RevokedEpochs,
    ) -> Option<DenyReason> {
        if revoked.contains(&(partition, owner_epoch)) {
            return Some(DenyReason::EpochRevoked);
        }
        if self.storage_fenced.contains(&partition) {
            return Some(DenyReason::LocalStorageFenced);
        }
        None
    }

    /// Whether writing `next` as `id`'s served lineage must first fence the lineage it replaces.
    ///
    /// Two cases, both "the served lineage goes and no view replaces it":
    /// - `next` is `None`: the partition is dropped (lead ruling A-R54.2, finding N3).
    /// - `next` differs and would be withheld by [`Self::installed_deny`] (lead ruling A-R56.1,
    ///   finding N4). The adopt publishes nothing, so without the fence the old lineage's last
    ///   view kept admitting to its horizon on a lineage this node no longer serves.
    ///
    /// A partition not served now has no view to supersede, so it never fences.
    fn must_fence(
        &self,
        id: PartitionId,
        next: Option<&ServedLineage>,
        revoked: &RevokedEpochs,
    ) -> bool {
        let Some(served) = self.served.get(&id) else {
            return false;
        };
        next.is_none_or(|next| {
            next != served && self.installed_deny(id, next.owner_epoch, revoked).is_some()
        })
    }
}

/// The `(partition, epoch)` pairs whose revocation is durable on this node's disk.
type RevokedEpochs = BTreeSet<(PartitionId, OwnerEpoch)>;

/// The prior grant's expiry, as a linearizable read found it **frozen** (team kernel-a
/// `design.md` §2.6 rule 1). A read of an unfrozen grant is not a basis for a proof, because
/// the owner can still extend it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrozenGrant {
    /// `F`: the frozen record's `E`, final because a frozen grant is never renewed.
    pub expiry_utc_ms: i64,
    /// The revision of the read that found it frozen. Every proof's `control_revision`.
    pub control_revision: Revision,
}

/// One partition this node may take over from its prior owner (team kernel-a `design.md` §2.6a,
/// which supersedes §2.6's struct; lead ruling A-R51).
///
/// Lives on the kernel, not in [`Held`], so an entry survives a fence and proof resumes after
/// re-adoption. Created **whole** by the correlated grant read (T3, lead ruling A-R31): there is
/// no half-built entry for a row to observe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Takeover {
    /// The node `partitions/{p}` named as owner. T5–T7 route by it; it is not on the proof.
    pub prior_node: NodeId,
    /// The lineage the prior owner served.
    pub prior_generation: Generation,
    /// The epoch the prior owner held.
    pub prior_owner_epoch: OwnerEpoch,
    /// The prior owner's grant, from its `grants/{node}` record.
    pub prior_grant_id: GrantId,
    /// The prior owner's process lifetime, from the same record.
    pub prior_boot_id: BootId,
    /// Set only by a linearizable read that found the grant record frozen.
    pub frozen: Option<FrozenGrant>,
    /// The revision of the newest `grants/{prior_node}` read this entry holds, frozen or not
    /// (T3 creates it, T6 raises it; finding B, lead ruling A-R53.2). T6 accepts only a read
    /// strictly newer. Never below `frozen`'s `control_revision`, because the read that sets
    /// `frozen` sets this too, so it is the ruling's `max(observed, frozen.control_revision)`.
    /// Without it an unfrozen read left no revision, and an older frozen read delivered after
    /// it froze the entry on a superseded grant and proved expiry on it.
    pub observed_revision: Revision,
    /// The revision of the read that saw `partitions/{p}` at
    /// [`partition::PartitionLifecycle::FencingDrained`] for this lineage.
    pub revoked_at: Option<Revision>,
    /// The one proof emitted for this `(partition, prior_owner_epoch)`. At most once, enforced by
    /// this field and not by prose (finding K-A-30).
    pub proven: Option<Revocation>,
}

/// A `grants/{owner}` read issued by T1, not yet answered: which lineage it is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingTakeover {
    generation: Generation,
    owner_epoch: OwnerEpoch,
    /// Set when the record that started the read was already `FencingDrained`, so the drain is
    /// not lost when the entry is created after it.
    drained_at: Option<Revision>,
}

/// How far the takeover side has read `partitions/*` (lead ruling A-R52, which is A-R48 applied
/// to the takeover side).
///
/// T1, T2 and T7 act only on an observation of `p` **strictly newer** than `p`'s mark; an older
/// or equal one changes nothing takeover-side. Without it, a `Fencing` read delivered after the
/// newer `Serving` read that dropped the entry (T2) re-ran T1, and a node-scoped freeze then
/// proved expiry for a partition that was serving again — the race A-R51 exists to close.
///
/// A partition's mark is the newer of its own last read and `floor`. On the kernel, beside
/// `takeover`, so it survives a fence.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TakeoverMarks {
    /// The last coherent snapshot's revision. A snapshot at `S` observes **every** partition at
    /// `S`, listed or not — absence from it is an observation too.
    floor: Revision,
    /// Single reads newer than `floor`. A snapshot that passes an entry prunes it, so the map
    /// holds only what the last snapshot could not have seen.
    reads: BTreeMap<PartitionId, Revision>,
}

impl TakeoverMarks {
    /// Whether an observation of `partition` at `revision` is newer than everything seen of it.
    fn is_newer(&self, partition: PartitionId, revision: Revision) -> bool {
        let read = self.reads.get(&partition).copied().unwrap_or_default();
        revision > read.max(self.floor)
    }

    /// A single read of `partition` at `revision`: `true`, and it becomes the mark, when it is
    /// newer; `false`, and nothing moves, otherwise.
    fn advance(&mut self, partition: PartitionId, revision: Revision) -> bool {
        let newer = self.is_newer(partition, revision);
        if newer {
            self.reads.insert(partition, revision);
        }
        newer
    }

    /// A coherent snapshot at `revision` raises every partition's mark to it. Call after deciding,
    /// with [`Self::is_newer`], which partitions the snapshot is news about.
    fn raise_floor(&mut self, revision: Revision) {
        self.floor = self.floor.max(revision);
        let floor = self.floor;
        self.reads.retain(|_, read| *read > floor);
    }
}

/// Everything about A1's state that a row may assert on, as one value (lead ruling A-R37).
///
/// Cloned rather than borrowed, and `PartialEq`, so a row that asserts "nothing moved" takes one
/// [`Authority::view`] before and one after and compares them once. A borrowing struct would
/// hold the kernel immutable across the step it is trying to observe.
///
/// # `served` is here whole, not summarised
///
/// The Manual Tester's B6 asked for the map itself, and the reason is specific: the coherent
/// reload installs `served` by **replacing** it, and a kernel that merged instead would keep
/// serving a partition the snapshot no longer lists. Both spellings emit the same effects and
/// admit the same partitions for every partition still present, so the difference is invisible
/// in a trace and visible here.
///
/// The held-only items are `Option`, `None` in [`AuthorityState::Unheld`] and
/// [`AuthorityState::Fenced`]. The held-only collections are empty rather than `None` in those
/// states, which is the same claim without a second layer to unwrap. `takeover` and
/// `revoked_epochs` are not held-only: they live on the kernel and read the same in every state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityStateView {
    /// The grant state itself, including [`AuthorityState::Fenced`]'s reason and tick.
    pub state: AuthorityState,
    /// Whether an error bound can be established on this node at all. Configuration, not an
    /// observation.
    pub clock_mode: ClockMode,
    /// The last **accepted** sample, or `None` when there is none or the last was retracted
    /// (finding K-A-50). `None` is what makes [`Authority::e_new`] `None`.
    pub clock_sample: Option<ControlTime>,
    /// The monotone sequence carried on every decision and every published view (K-A-34).
    pub authority_seq: u64,
    /// Consecutive admission-refused watch terminations since the last healthy delivery.
    pub watch_refused_attempts: u32,
    /// Where each watched family has been consumed to.
    pub cursors: BTreeMap<ControlPrefix, Revision>,
    /// Per-partition lineage, read from `partitions/{id}` and from nowhere else (K-A-04).
    pub served: BTreeMap<PartitionId, ServedLineage>,
    /// The control revision each [`Self::served`] entry was installed at, keyed identically
    /// (lead ruling A-R48).
    ///
    /// Here whole for the same reason `served` is: the ordering rule it enforces — a reply
    /// installs only if it is newer *about that partition* than what is held — is invisible in a
    /// trace. A kernel that compared against [`Self::partitions_revision`] instead emits the
    /// same effects for every in-order delivery and rolls `served` back on a reordered one.
    pub served_revisions: BTreeMap<PartitionId, Revision>,
    /// Tombstones (lead ruling A-R48b): for each partition removed from [`Self::served`], the
    /// newest revision known about it — the later of the removal and the removed entry's
    /// [`Self::served_revisions`]. Never shares a key with `served`.
    ///
    /// Here for the same reason as `served_revisions`: a single read of ours delivered after the
    /// fence re-adopts if the removal forgot its revision, and that re-grants serving rights
    /// after a fence. A read must be newer than this to adopt, and a snapshot older than it
    /// leaves the key absent.
    pub removed_revisions: BTreeMap<PartitionId, Revision>,
    /// The takeover table (team kernel-a `design.md` §2.6a). On the kernel, so it is present in
    /// every state and survives a fence.
    pub takeover: BTreeMap<PartitionId, Takeover>,
    /// The `(partition, epoch)` pairs whose revocation is durable on this node's disk. On the
    /// kernel, like `takeover`: present in every state, kept across grants, and refilled at start
    /// by [`AuthorityEvent::EpochRevocationRestored`] (lead ledger L-R178e).
    pub revoked_epochs: BTreeSet<(PartitionId, OwnerEpoch)>,
    /// The partitions whose local storage failed. Fenced individually; the grant stays held.
    pub storage_fenced: BTreeSet<PartitionId>,
    /// The revision the partitions family was last coherently read at.
    ///
    /// **Not** a view over `cursors[Partitions]` (lead ruling A-R30): the cursor is moved by
    /// three call sites, while this is only ever the last coherent snapshot revision. The two
    /// coincide for one instant after a snapshot, which is what makes the difference easy to
    /// miss — so both are here and a row can watch them diverge.
    pub partitions_revision: Option<Revision>,
    /// `E`, exactly as the committed record says. Never advanced locally, and written only by a
    /// committed CAS — never by a conflict, an unknown or an unavailable (property 1 of
    /// `design.md` §2.4, and the single most important line in the module).
    pub expiry_utc_ms: Option<i64>,
    /// The tick the last committed renewal CAS was **dispatched** at (finding K-A-06). What
    /// [`clock::local_ok`] measures from.
    pub renewed_at: Option<Tick>,
    /// The revision the next renewal CAS must match (spec §7.3 step 1, "its exact revision").
    pub record_revision: Option<Revision>,
    /// The renewal CAS in flight, if there is one.
    pub renewal: Option<Renewal>,
    /// The create-only acquisition CAS in flight, if there is one.
    pub acquire: Option<Acquire>,
    /// Which [`AuthorityTimer`] version A1 last armed, in [`AuthorityTimer::ALL`] order.
    ///
    /// A firing whose version is below the entry for its kind is stale, which is the one thing
    /// about a timer a row cannot otherwise see: [`crate::contracts::ids::TimerVersion`] reaches
    /// the kernel only inside the event a row is trying to judge.
    pub timer_versions: [TimerVersion; 4],
}

/// Which grant state A1 is in (team kernel-a `design.md` §2.1).
///
/// # Why it carries data, and what that cost
///
/// Lead ruling **A-R28**. A state accessor alone cannot answer the questions the fence rows ask:
/// the two partition-scoped fences leave the node in `Held`, so their scope can never reach
/// [`Authority::state`] at all, and no accessor can count *exactly one* fence or assert the
/// ordered pairing with [`AuthorityEffect::PublishAuthorityView`] that finding K-A-49 requires.
/// So both: the [`AuthorityEffect::Fence`] effect for the fence rows and the pairing, and this
/// data for the rows that assert `Fenced { reason: Revoked }`.
///
/// The cost was ruled and accepted: this drops `#[derive(Copy)]`, which un-`const`s
/// [`Authority::state`] and makes it return a reference.
///
/// `Fenced` carries **no scope field** (finding K-A-29). Every row that reaches it is
/// node-scoped, because the two partition-scoped fences stay in `Held`. [`FenceScope`] survives
/// only on the effect, where it is real.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityState {
    /// No validated grant. The entry state, and the state after a fence clears.
    Unheld {
        /// Why the last grant ended, if there was one. `None` at boot.
        last_fence: Option<DenyReason>,
        /// The create-only CAS in flight, if one is.
        acquire: Option<Acquire>,
    },
    /// A grant this node committed and has not lost.
    Held(Held),
    /// Terminal for this grant id. Exit requires a *new* grant id in a control record.
    Fenced {
        /// Why. The same vocabulary the superseding view's `past_horizon` carries.
        reason: DenyReason,
        /// The tick the fence fired at.
        at: Tick,
    },
}

impl Default for AuthorityState {
    fn default() -> Self {
        Self::Unheld {
            last_fence: None,
            acquire: None,
        }
    }
}

impl AuthorityState {
    /// Whether a grant is held. The cheap question, kept separate from [`Self::held`] because
    /// most callers only need the boolean.
    #[must_use]
    pub const fn is_held(&self) -> bool {
        matches!(self, Self::Held(_))
    }

    /// Whether no grant is held and none has ended.
    #[must_use]
    pub const fn is_unheld(&self) -> bool {
        matches!(self, Self::Unheld { .. })
    }

    /// Whether this grant id is terminally over.
    #[must_use]
    pub const fn is_fenced(&self) -> bool {
        matches!(self, Self::Fenced { .. })
    }

    /// The held grant, or `None`.
    #[must_use]
    pub const fn held(&self) -> Option<&Held> {
        match self {
            Self::Held(held) => Some(held),
            _ => None,
        }
    }

    /// Why the node is fenced, or why its last grant ended while `Unheld`.
    ///
    /// One accessor over both states on purpose: a row that drives a fence and then watches the
    /// node re-acquire is asking the same question throughout.
    #[must_use]
    pub const fn fence_reason(&self) -> Option<DenyReason> {
        match self {
            Self::Fenced { reason, .. } => Some(*reason),
            Self::Unheld { last_fence, .. } => *last_fence,
            Self::Held(_) => None,
        }
    }

    /// The tick the fence fired at, when the state is [`Self::Fenced`].
    #[must_use]
    pub const fn fenced_at(&self) -> Option<Tick> {
        match self {
            Self::Fenced { at, .. } => Some(*at),
            _ => None,
        }
    }

    /// The acquisition CAS in flight, when the state is [`Self::Unheld`].
    #[must_use]
    pub const fn acquire(&self) -> Option<Acquire> {
        match self {
            Self::Unheld { acquire, .. } => *acquire,
            _ => None,
        }
    }
}

/// Fenced grants, epochs and coherent watch resync.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Authority {
    state: AuthorityState,
    /// Lives here, not in [`Held`]: samples arrive in every state and acquisition needs one
    /// (the K-A-39 sweep).
    clock: ClockView,
    /// Monotone, and never reset — not even by a new grant (finding K-A-34). Bumped on every
    /// fence, every grant adoption and every write of the served lineage; a committed renewal is
    /// **not** a bump, because the lineage did not move.
    authority_seq: u64,
    /// Where each watched family has been consumed to. A resumed watch starts after this.
    cursors: BTreeMap<ControlPrefix, Revision>,
    /// Consecutive [`WatchTermination::ResourceExhaustedFatal`] terminations since the last
    /// healthy watch event. Reset by any delivery that proves the stream is serving.
    watch_refused_attempts: u32,
    /// The families a [`AuthorityTimer::WatchBackoff`] firing will re-watch, and the revision
    /// each resumes after.
    ///
    /// A map and not a single pair: one `TerminateWatch` ends **every** watch the node holds, so
    /// both families A1 watches terminate together, and one pending slot would drop the second.
    /// Emptied when the timer fires, so a firing with nothing pending re-watches nothing rather
    /// than re-watching the last family twice.
    watch_backoff: BTreeMap<ControlPrefix, Revision>,
    /// The version A1 last armed each [`AuthorityTimer`] under, in [`AuthorityTimer::ALL`] order.
    ///
    /// Starts at [`TimerVersion`] zero for every kind, which is a real version and not "unarmed":
    /// a fixture that fires a wake A1 never armed is firing the current version, and gets the
    /// live row rather than a `StaleTimer` it did not ask for.
    timer_versions: [TimerVersion; 4],
    /// How many acquisition CASes this kernel has dispatched. The next one writes
    /// `GrantId(grants_issued + 1)`, so no two acquisitions by one boot write the same id.
    ///
    /// Per boot, not per node, and that is enough for what the id is compared for: a fresh
    /// kernel starts at one again, but it also has a fresh [`BootId`], and [`grant::classify`]
    /// compares the boot before the grant id. Who allocates grant ids cluster-wide is the grant
    /// service's question (`design.md` §6 puts it at M8/M9), not this counter's.
    grants_issued: u64,
    /// The takeover table (team kernel-a `design.md` §2.6a). Outlives any one grant, like the
    /// clock and `authority_seq`.
    takeover: BTreeMap<PartitionId, Takeover>,
    /// The `(partition, epoch)` pairs whose revocation is durable on this node's disk (lead
    /// ledger L-R178e). On the kernel, not in [`Held`], and for the K-A-39 reason: it outlives
    /// any one grant. It is written in every state — by a completion
    /// ([`AuthorityEvent::EpochRevocationPersisted`]) and by the start-of-process read-back
    /// ([`AuthorityEvent::EpochRevocationRestored`]) — and only ever grows. Before L-R178e it was
    /// a `Held` field that `enter_held` built empty, so a revocation completed while `Unheld`, or
    /// made by an earlier process, was forgotten by the next grant.
    revoked_epochs: RevokedEpochs,
    /// T1's outstanding `grants/{owner}` reads, keyed by the correlation the read was issued
    /// under **and** the owner it reads. `design.md` §2.6a keys by correlation alone, but every
    /// effect of one step carries that step's correlation, so one snapshot naming two owners
    /// would collide. One read per owner answers every partition it names.
    ///
    /// The key only joins the partitions of one step onto one read. The answer is matched by the
    /// [`ControlRequestId`] stored in [`TakeoverRead`], never by the key: T5's re-read keeps the
    /// key, so a late answer to the first read would otherwise pass for the re-read's.
    takeover_reads: BTreeMap<(CorrelationId, NodeId), TakeoverRead>,
    /// The takeover side's high-water mark over `partitions/*` (lead ruling A-R52).
    takeover_marks: TakeoverMarks,
    /// The `partitions/{id}` reads the `Recovered` trigger row issued and no answer has reached
    /// yet, by partition (`design.md` §2.4, the `Recovered` pair; lead ledger L-R177gf). The
    /// install row recognises its read-back by the correlation stored here.
    ///
    /// On the kernel, not in [`Held`]: the trigger row fires in every state (lead ruling A-R78),
    /// and an `Unheld` node still issues the read. One entry per partition: a second
    /// `Recovered` for the same partition supersedes the first, whose read-back is then an
    /// ordinary read.
    recovery_reads: BTreeMap<PartitionId, RecoveryRead>,
    /// How many control requests this kernel has sent. The next is
    /// `AUTHORITY_CONTROL_REQUEST_BASE + requests_issued + 1`: a counter, so every request id is
    /// fresh and deterministic (no clock, no randomness). Per instance, like `grants_issued`.
    requests_issued: u64,
}

/// One outstanding T1 `grants/{owner}` read: the request it was sent as, and every partition it
/// answers for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TakeoverRead {
    request: ControlRequestId,
    pending: BTreeMap<PartitionId, PendingTakeover>,
}

/// One outstanding post-`Recovered` read of `partitions/{id}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveryRead {
    /// The request id the trigger row's `Get` was sent as. Only the answer echoing it is the
    /// read-back; another read of the same key under the same correlation is not (the tester's
    /// F4, closed by lead ledger L-R177hs).
    request: ControlRequestId,
    /// `r.new_generation`: the install row fires only for a record naming it.
    generation: Generation,
}

impl Authority {
    /// A module holding no grant, no sample and no cursor.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: AuthorityState::default(),
            clock: ClockView::new(),
            authority_seq: 0,
            cursors: BTreeMap::new(),
            watch_refused_attempts: 0,
            watch_backoff: BTreeMap::new(),
            timer_versions: [TimerVersion(0); 4],
            grants_issued: 0,
            takeover: BTreeMap::new(),
            revoked_epochs: BTreeSet::new(),
            takeover_reads: BTreeMap::new(),
            takeover_marks: TakeoverMarks::default(),
            recovery_reads: BTreeMap::new(),
            requests_issued: 0,
        }
    }

    /// Everything a row may assert on, as one cloned value (lead ruling A-R37).
    ///
    /// The single read-only state surface. See [`AuthorityStateView`] for why it is one struct
    /// and not a getter per field.
    #[must_use]
    pub fn view(&self) -> AuthorityStateView {
        let held = self.state.held();
        AuthorityStateView {
            state: self.state.clone(),
            clock_mode: self.clock.mode(),
            clock_sample: self.clock.sample(),
            authority_seq: self.authority_seq,
            watch_refused_attempts: self.watch_refused_attempts,
            cursors: self.cursors.clone(),
            served: held.map(|held| held.served.clone()).unwrap_or_default(),
            served_revisions: held
                .map(|held| held.served_revisions.clone())
                .unwrap_or_default(),
            removed_revisions: held
                .map(|held| held.removed_revisions.clone())
                .unwrap_or_default(),
            takeover: self.takeover.clone(),
            revoked_epochs: self.revoked_epochs.clone(),
            storage_fenced: held
                .map(|held| held.storage_fenced.clone())
                .unwrap_or_default(),
            partitions_revision: held.map(|held| held.partitions_revision),
            expiry_utc_ms: held.map(|held| held.expiry_utc_ms),
            renewed_at: held.map(|held| held.renewed_at),
            record_revision: held.map(|held| held.record_revision),
            renewal: held.and_then(|held| held.renewal),
            acquire: self.state.acquire(),
            timer_versions: self.timer_versions,
        }
    }

    /// Which of A1's timers a [`TimerId`] is, or `None` when the id is not A1's.
    ///
    /// A method rather than a bare [`AuthorityTimer::from_id`] call so that it keeps answering
    /// when the mapping stops being a constant: the per-partition timers of `design.md` §2.6 are
    /// not built, and when they are, the id block alone will not decide the kind. A row written
    /// against this survives that; a row written against the constant does not.
    #[must_use]
    pub fn timer_kind(&self, id: TimerId) -> Option<AuthorityTimer> {
        let _ = self;
        AuthorityTimer::from_id(id)
    }

    /// The version a kind is currently armed under. A firing below it is stale.
    #[must_use]
    pub const fn timer_version(&self, kind: AuthorityTimer) -> TimerVersion {
        self.timer_versions[kind.offset() as usize]
    }

    /// Bump a kind's armed version and return the [`TimerEffect::Arm`] that carries it.
    fn arm(&mut self, event: &Event, kind: AuthorityTimer, at: Tick) -> Effect {
        let slot = &mut self.timer_versions[kind.offset() as usize];
        *slot = TimerVersion(slot.0.saturating_add(1));
        Self::effect(
            event,
            EffectKind::Timer(TimerEffect::Arm {
                id: kind.id(),
                version: *slot,
                at,
            }),
        )
    }

    /// The grant state, for a fixture that asserts on it (team kernel-a `KA-1`).
    ///
    /// A reference, not a value: [`AuthorityState`] carries a [`Held`] and is no longer `Copy`
    /// (lead ruling A-R28).
    #[must_use]
    pub const fn state(&self) -> &AuthorityState {
        &self.state
    }

    /// The held grant, or `None` in either other state.
    #[must_use]
    pub const fn held(&self) -> Option<&Held> {
        self.state.held()
    }

    /// The clock as A1 holds it: the configured mode and the last accepted sample.
    #[must_use]
    pub const fn clock(&self) -> &ClockView {
        &self.clock
    }

    /// Set the configured bounded-clock mode.
    ///
    /// A *configuration* input, which is why it is a setter and not an event: spec §7.2's
    /// "automatic promotion is disabled when error bounds cannot be established" is a property of
    /// how the node was deployed, not something that happens to it. A node in
    /// [`clock::ClockMode::Unbounded`] denies every check without ever fencing, and that
    /// difference — cannot admit versus terminally fenced — is the one finding K-A-07 exists to
    /// keep apart.
    pub const fn set_clock_mode(&mut self, mode: clock::ClockMode) {
        self.clock.set_mode(mode);
    }

    /// The monotone authority sequence carried on every decision and every published view.
    #[must_use]
    pub const fn authority_seq(&self) -> u64 {
        self.authority_seq
    }

    /// Consecutive admission-refused terminations since the last healthy watch delivery.
    #[must_use]
    pub const fn watch_refused_attempts(&self) -> u32 {
        self.watch_refused_attempts
    }

    /// How long the `attempt`-th consecutive capacity refusal waits before re-watching.
    ///
    /// [`WATCH_BACKOFF_BASE_MILLIS`] doubled per refusal, saturating at
    /// [`WATCH_BACKOFF_CAP_MILLIS`]. Non-decreasing in `attempt` by construction, and equal to
    /// the cap for every `attempt` from 6 upwards.
    ///
    /// **Public because the event path cannot reach far enough to test the shape.**
    /// [`WATCH_ADMISSION_ATTEMPT_CAP`] latches at 3, so only attempts 1 and 2 ever arm a timer,
    /// and a row asserting "non-decreasing, and `backoff_20 == cap`" has no way to drive 20
    /// refusals past a kernel that stopped re-arming after the third. Asserting the two the
    /// event path does reach *and* the shape here is the whole claim; asserting only the two is
    /// a claim about a curve from two of its points.
    ///
    /// `attempt` 0 is not a refusal and answers the base delay, so the function is total and a
    /// caller never has to special-case an underflow.
    #[must_use]
    pub const fn watch_backoff_millis(attempt: u32) -> u64 {
        // Shifting by 64 or more panics in debug and masks the amount in release, and the cap
        // saturates long before 63 doublings, so the exponent is clamped rather than trusted.
        let doublings = attempt.saturating_sub(1);
        if doublings >= 63 {
            return WATCH_BACKOFF_CAP_MILLIS;
        }
        let uncapped = WATCH_BACKOFF_BASE_MILLIS << doublings;
        if uncapped > WATCH_BACKOFF_CAP_MILLIS {
            WATCH_BACKOFF_CAP_MILLIS
        } else {
            uncapped
        }
    }

    /// Where a watched family has been consumed to, if it is watched at all.
    #[must_use]
    pub fn cursor(&self, prefix: ControlPrefix) -> Option<Revision> {
        self.cursors.get(&prefix).copied()
    }

    /// Answer one authority check, synchronously, against live state.
    ///
    /// `&self`, **not** `&Held` (lead ruling A-R29): a caller must be able to ask an `Unheld` or
    /// a `Fenced` kernel and get the deny it should give, and forcing the caller to find a `Held`
    /// first would put the first guard of the rule outside the function that states the rule.
    ///
    /// This is [`Checkpoint::Admission`], which `design.md` §2.5 makes synchronous on purpose —
    /// it is evaluated against the last [`AuthorityView`] A1 pushed, which removes two events per
    /// transaction from the Q1 budget. The other three checkpoints cross a module boundary and
    /// are the [`AuthorityEvent::Check`] / [`AuthorityEffect::Answer`] pair.
    ///
    /// **The reasons are ordered and the order is part of the contract** (property 5 of §2.4):
    /// the conservative local window, then the bounded-clock comparison, then the lineage, then
    /// the revoked epoch, then the local storage fence. `local_ok` is first because it is certain
    /// regardless of the sample. The same trace therefore always produces the same reason, which
    /// is what lets an oracle assert on it.
    ///
    /// **Returns a [`Verdict`], never a `bool`** (lead ruling A-R37). Half the deny reasons in
    /// [`DenyReason`] are only reachable through this function, and a boolean would collapse
    /// fifteen distinguishable answers into one that no row could assert on.
    ///
    /// Takes the whole [`StepCtx`] rather than `now` and `budgets` separately, because a row that
    /// asks this question outside a step already has a `ctx` to hand and two loose arguments are
    /// two chances to pass a tick the kernel never saw. [`Self::may_admit_at`] is the same rule
    /// for a caller that has neither.
    #[must_use]
    pub fn may_admit(&self, ctx: &StepCtx<'_>, lineage: Lineage) -> Verdict {
        self.may_admit_at(lineage, ctx.now, ctx.budgets)
    }

    /// [`Self::may_admit`] for a caller holding a tick and a budget rather than a [`StepCtx`].
    ///
    /// The same function body; the split exists only so that neither caller has to build the
    /// other's argument.
    #[must_use]
    pub fn may_admit_at(&self, lineage: Lineage, now: Tick, budgets: &Budgets) -> Verdict {
        let held = match &self.state {
            AuthorityState::Unheld { .. } => return Verdict::Deny(DenyReason::NoGrant),
            AuthorityState::Fenced { reason, .. } => return Verdict::Deny(*reason),
            AuthorityState::Held(held) => held,
        };
        if !local_ok(held.renewed_at, now, budgets) {
            return Verdict::Deny(DenyReason::Expired);
        }
        if let Err(reason) = utc_ok(&self.clock, held.expiry_utc_ms, now, budgets) {
            return Verdict::Deny(reason);
        }
        if held.control_unavailable {
            return Verdict::Deny(DenyReason::ControlUnavailable);
        }
        held.partition_deny(lineage, &self.revoked_epochs)
            .map_or(Verdict::Admit, Verdict::Deny)
    }

    /// The `E_new` this kernel would write if it dispatched an acquisition or a renewal CAS now.
    ///
    /// Exposed because "no sample, no `E_new`, no CAS" (ADR-rdb-0007 §2) is a rule a fixture has
    /// to be able to see the input to. `None` means a CAS is withheld, and zero grant ids are
    /// consumed.
    #[must_use]
    pub const fn e_new(&self, now: Tick, budgets: &Budgets) -> Option<i64> {
        e_new(&self.clock, now, budgets)
    }

    // ---- effect constructors -------------------------------------------------------------

    /// Wrap a control request as an effect of this module, for this event.
    fn control(event: &Event, kind: ControlEffect) -> Effect {
        Self::effect(event, EffectKind::Control(kind))
    }

    /// A request id no earlier request of this kernel carried.
    fn request(&mut self) -> ControlRequestId {
        self.requests_issued = self.requests_issued.saturating_add(1);
        ControlRequestId(AUTHORITY_CONTROL_REQUEST_BASE + self.requests_issued)
    }

    /// A linearizable read of `key`, as a fresh request.
    fn get(&mut self, event: &Event, key: ControlKey) -> Effect {
        let request = self.request();
        Self::control(event, ControlEffect::Get { request, key })
    }

    /// Wrap one of kernel-a's own facts on its arm of [`KernelEffect`] (lead ruling A-R25).
    ///
    /// Sited at the partition it concerns (`Effect.partition`; lead ruling A-R84): a view at its
    /// lineage's partition and a partition fence at the fenced partition, because the host
    /// delivers each to that partition's T1 and P1. A1 emits both for partitions other than the
    /// one it stepped (an install serving several, a revocation of another). Everything else
    /// stays at the event's partition: a node fence has no one partition, and a fact or an
    /// answer goes back where it was asked.
    fn authority(event: &Event, kind: AuthorityEffect) -> Effect {
        let partition = match &kind {
            AuthorityEffect::Fence {
                scope: FenceScope::Partition(partition),
                ..
            } => *partition,
            AuthorityEffect::PublishAuthorityView(view) => view.lineage.partition,
            _ => event.partition,
        };
        Self::about(
            Self::effect(event, EffectKind::Kernel(KernelEffect::Authority(kind))),
            partition,
        )
    }

    /// "Handled, and deliberately did nothing", in kernel-a's own vocabulary.
    ///
    /// Required rather than decorative (lead ruling A-R24): an empty effect vector is
    /// indistinguishable from an unhandled event, so a row must be able to assert on the
    /// inaction rather than trust an absence.
    fn ignored(event: &Event, reason: AuthorityIgnoreReason) -> Effect {
        Self::effect(
            event,
            EffectKind::Kernel(KernelEffect::Ignored {
                reason: KernelIgnoredReason::Authority(reason),
            }),
        )
    }

    fn effect(event: &Event, kind: EffectKind) -> Effect {
        Effect {
            correlation: event.correlation,
            from: ModuleName::Authority,
            partition: event.partition,
            kind,
        }
    }

    // ---- the fence -----------------------------------------------------------------------

    /// The **only** way an [`AuthorityEffect::Fence`] is emitted.
    ///
    /// Every fence bumps `authority_seq` once and is followed by a superseding
    /// [`AuthorityEffect::PublishAuthorityView`] whose `valid_through_tick` is the fence tick less
    /// one, saturating, and whose `past_horizon` is this reason (findings K-A-34, K-A-49): one
    /// for the fenced partition, or, for a node fence, one per served partition and the event's
    /// partition, in [`PartitionId`] order (finding A, lead ruling A-R53.1). The
    /// §2.4 rows do not repeat that pair in every effects cell; it is this helper, and it is why
    /// a row can assert the ordered pairing at all.
    ///
    /// The paired view does **not** go through an admission horizon: a fence has no horizon to
    /// compute, it has a reason to carry, and the view must already be past when it lands, so
    /// that a request processed at the fence tick after the view cannot be admitted by it.
    ///
    /// A node-scoped fence moves the state to [`AuthorityState::Fenced`]. A partition-scoped one
    /// leaves the grant held and every other partition serving; what it removes from [`Held`] is
    /// the caller's business, because the two partition-scoped triggers differ there.
    ///
    /// Returns an empty vector when no grant is held: a fence ends a grant, and there is nothing
    /// to end.
    fn fence(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        scope: FenceScope,
        reason: DenyReason,
    ) -> Vec<Effect> {
        let Some(held) = self.state.held() else {
            return Vec::new();
        };
        // The view is per partition (`design.md` §1.7). A partition fence supersedes its own
        // partition's view. A node fence ends admission on **every** partition, so it supersedes
        // every served partition's view, plus the event's partition (finding A, lead ruling
        // A-R53.1). Before A-R53 it superseded only the event's, and a secondary kept honouring
        // every other partition's view to its horizon after the node had stopped admitting.
        let mut partitions = BTreeSet::from([match scope {
            FenceScope::Node => event.partition,
            FenceScope::Partition(partition) => partition,
        }]);
        if matches!(scope, FenceScope::Node) {
            partitions.extend(held.served.keys().copied());
        }
        // One bump for the fence, shared by its views: each names a different partition, and a
        // consumer orders views per partition.
        let authority_seq = self.authority_seq.saturating_add(1);
        let mut effects = vec![Self::authority(
            event,
            AuthorityEffect::Fence { scope, reason },
        )];
        effects.extend(partitions.into_iter().map(|partition| {
            let view = Self::past_view(held, ctx, partition, authority_seq, reason);
            Self::authority(event, AuthorityEffect::PublishAuthorityView(view))
        }));
        self.authority_seq = authority_seq;
        if matches!(scope, FenceScope::Node) {
            self.state = AuthorityState::Fenced {
                reason,
                at: ctx.now,
            };
        }
        effects
    }

    /// A fence's view of `partition`: already past when it lands, carrying the fence's reason.
    ///
    /// Its lineage is the installed one when there is one. Otherwise `StepCtx` carries whatever
    /// the dispatcher last adopted for this partition, which is the same triple by a different
    /// route (lead ruling F-R10). A past view admits nothing, so the fallback widens nothing.
    fn past_view(
        held: &Held,
        ctx: &StepCtx<'_>,
        partition: PartitionId,
        authority_seq: u64,
        reason: DenyReason,
    ) -> AuthorityView {
        let served = held.served.get(&partition).copied();
        AuthorityView {
            lineage: Lineage {
                partition,
                generation: served.map_or(ctx.generation, |served| served.generation),
                owner_epoch: served.map_or(ctx.owner_epoch, |served| served.owner_epoch),
            },
            grant_id: held.grant,
            boot_id: held.boot,
            authority_generation: held.authority_generation,
            config_version: served.map_or(ctx.config_version, |served| served.config_version),
            authority_seq,
            valid_through_tick: Tick(ctx.now.0.saturating_sub(1)),
            past_horizon: reason,
        }
    }

    // ---- the clock, and the two conjuncts it feeds -----------------------------------------

    /// Absorb `ctx.control_time` and re-evaluate both admission conjuncts, before the event is
    /// routed.
    ///
    /// # Why this runs on every step and not on a wake
    ///
    /// There is no clock event and no tick event (lead ruling A-R27), so an earlier draft put
    /// the expiry rows inside [`Self::on_timer`] and armed a wake every
    /// [`Budgets::clock_sample_period_millis`]. **That is a fencing defect, not a test
    /// inconvenience**: between two wakes a grant can be up to a full period past its expiry
    /// while A1 still answers [`Verdict::Admit`], because the fence that would supersede its
    /// published view has not fired yet. The Manual Tester's B3 raised it as a reachability
    /// problem — the one-tick twins either side of a boundary need a step at the boundary — and
    /// it is the same bug seen from the other end. So both conjuncts are evaluated here, on
    /// whatever event arrives, and the wake exists only to guarantee that *some* event arrives
    /// when the node has gone quiet.
    ///
    /// # Order
    ///
    /// The sample is absorbed first, then the conjuncts are judged against whatever survived
    /// that. `design.md` §2.4's order holds between the two fence rows: `Expired` above
    /// `ClockUnbounded`, because [`local_ok`] reads the monotonic tick and is certain regardless
    /// of the sample, so a grant that is past its local window is over whatever the clock says.
    ///
    /// # What a rejected sample does
    ///
    /// The accepting guard is `design.md` §2.4's: an established bound, a sample not stamped
    /// after `now`, an error within the configured ceiling, and no backward jump beyond the
    /// effective epsilon. Staleness is deliberately **not** in it — a stale sample is still the
    /// best reading there is, and it *denies* without fencing (lead ruling A-R12).
    ///
    /// A rejected sample is always **retracted** (finding K-A-50), in every state, so no `E_new`
    /// is ever derived from a reading the clock subsystem has disowned. The fence that follows in
    /// [`AuthorityState::Held`] is not a second rule: with no sample, [`utc_ok`] answers
    /// [`DenyReason::ClockUnbounded`], and the conjunct rows below fire it. In the other two
    /// states there is no grant to end, so the retraction is reported as
    /// [`AuthorityIgnoreReason::SampleRejected`] and nothing else happens.
    ///
    /// One refusal is **not** retracted: a sample older than the held one (lead ruling A-R54.3).
    /// It is reported as [`AuthorityIgnoreReason::SampleRejected`] in every state, and the held
    /// sample stands, with the conjuncts judged on it as on any step.
    ///
    /// # The budgets are configuration
    ///
    /// Every threshold here and in [`Self::admission_horizon`] is read from `ctx.budgets` on each
    /// step. They are configuration, like the clock mode (lead ruling A-R53a): a scenario that
    /// changes them between steps changes the node's configuration, and nothing here guards
    /// against it or republishes for it (lead ruling A-R54.4).
    fn revalidate(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Vec<Effect> {
        let (mut effects, moved) = self.absorb_sample(ctx, event);
        let Some(held) = self.state.held() else {
            return effects;
        };
        let (renewed_at, expiry) = (held.renewed_at, held.expiry_utc_ms);
        let utc = utc_ok(&self.clock, expiry, ctx.now, ctx.budgets);

        // Expiry fences **unconditionally** (finding K-A-02). An earlier draft guarded this row
        // with "and no renewal outstanding", so a renewal whose completion was delayed or dropped
        // — a first-class scenario operation — left the guard false for ever: no fence, therefore
        // no superseding view, therefore secondaries honouring a stale view to its natural
        // horizon, and a late commit resuming admission after true expiry. That is the
        // resurrection ADR-rdb-0007 §3 exists to forbid.
        if !local_ok(renewed_at, ctx.now, ctx.budgets) || utc == Err(DenyReason::Expired) {
            effects.extend(self.fence(ctx, event, FenceScope::Node, DenyReason::Expired));
            return effects;
        }
        if utc == Err(DenyReason::ClockUnbounded) {
            effects.extend(self.fence(ctx, event, FenceScope::Node, DenyReason::ClockUnbounded));
            return effects;
        }
        // Published only now that no fence has fired. A view emitted before the conjuncts were
        // judged would be superseded by its own step, and a row asserting the K-A-49 pair would
        // see `[Publish, Fence, Publish]` for one trigger (lead ruling A-R37, the tester's B8).
        // Every served partition, not `ctx.partition`: the horizon is node-wide (A-R54.1).
        if moved {
            effects.extend(self.publish_all(ctx, event));
        }
        // `Err(ClockSampleStale)` deliberately falls through: the node stays `Held` and every
        // check denies until a fresh sample arrives. Persistent staleness still ends in the
        // expiry row above, through renewals it withholds (ADR-rdb-0007 §3, finding K-A-42). The
        // fact that admission is suspended is reported on the clock wake — see [`Self::on_timer`]
        // — rather than on every step, because "this is still true" is not an event.
        //
        // `Err(ClockModeUnbounded)` falls through for the same reason and it is the **same**
        // reason, which is the whole of lead ruling A-R42: spec §7.2 says a node that cannot
        // establish a bound "stops accepting requests", and [`ClockMode`]'s own doc spells out
        // that it "does not fence, because there is no grant-ending event". Until A-R42 this
        // arrived as `ClockUnbounded` and the row above fenced it, so `set_clock_mode(Unbounded)`
        // ended a healthy grant. Falling through does **not** mean the grant runs out:
        // [`e_new`] is guarded on the sample, not the mode (K-A-07, below), so with a valid
        // sample renewals continue and `E` keeps advancing. An unbounded node keeps its grant and
        // denies every check. That is spec §7.2's "no grant-ending event": safety holds by
        // denial, and the cost is availability. (A-R42 as first written said the grant expires;
        // the lead corrected that premise.)
        //
        // **This reading has now been rejected twice.** The architect rejected "`utc_ok` is
        // `Err`" as the *renewal* guard — unbounded mode with a valid sample would withhold every
        // renewal, expire, fence and re-acquire, the K-A-07 loop — and fixed it by guarding on
        // `e_new is None`, the sample rather than the mode. The fence guard kept the rejected
        // reading one function over. If a third occurrence is ever tempting, the distinction
        // belongs in the returned value, not in this caller.
        effects
    }

    /// The sample half of [`Self::revalidate`]: accept it, or retract and say so.
    ///
    /// Emits no fence and no view — the fences are the conjuncts' business, so that there is
    /// exactly one place in this file where a clock fence is decided, and the view waits until
    /// the caller knows no fence fired. The `bool` is "accepted, and it is not the sample we
    /// already had".
    fn absorb_sample(&mut self, ctx: &StepCtx<'_>, event: &Event) -> (Vec<Effect>, bool) {
        let sample = ctx.control_time;
        let previous = self.clock.sample();
        // `sampled_at` never moves backwards (lead ruling A-R54.3, finding N2). An older sample
        // is a reordered delivery, not evidence that the bound is gone, so it is refused and the
        // held sample **stays** — in every state, and without the K-A-50 retraction below. Kept,
        // because the held sample is what the next one's backward-jump check is judged against:
        // accepting the older one let a stale reading reset that reference and hide a jump.
        if previous.is_some_and(|previous| sample.sampled_at.0 < previous.sampled_at.0) {
            return (
                vec![Self::ignored(event, AuthorityIgnoreReason::SampleRejected)],
                false,
            );
        }
        let rejected = !sample.bound_established
            || sample.sampled_at.0 > ctx.now.0
            || sample.error_millis > ctx.budgets.clock_error_millis
            || previous.is_some_and(|previous| {
                clock::is_backward_jump(&previous, &sample, ctx.now, ctx.budgets)
            });

        if rejected {
            self.clock.retract();
            if self.state.is_held() {
                // The fence is the conjunct row's, one frame up.
                return (Vec::new(), false);
            }
            return (
                vec![Self::ignored(event, AuthorityIgnoreReason::SampleRejected)],
                false,
            );
        }

        if previous == Some(sample) {
            // The horizon is a function of the sample, so an unchanged sample moves nothing and
            // republishing would be noise (finding K-A-35).
            return (Vec::new(), false);
        }
        // Accepted in every state, or `Unheld` could never acquire (the K-A-39 sweep).
        self.clock.accept(sample);
        (Vec::new(), true)
    }

    /// `PublishAuthorityView` for one partition, or nothing (finding F2).
    ///
    /// **Nothing** when no grant is held, when the partition is not served, or when
    /// [`Held::partition_deny`] refuses its served lineage. A view is a copy of
    /// [`Self::may_admit_at`] that a secondary honours until `valid_through_tick` without asking
    /// again (`design.md` §1.7, K-A-49), so a view the check would deny is admission the check
    /// refused. The admission horizon judges only the grant's two conjuncts; the partition's are
    /// judged here, by the same function the check uses. There is no fallback to the step
    /// context's lineage any more: a lineage this node has not installed is not one it may admit.
    ///
    /// A partition that stops admitting already had its view superseded by the fence that
    /// stopped it ([`Self::fence`]), so publishing nothing here leaves that past view standing.
    fn publish(&self, ctx: &StepCtx<'_>, event: &Event, partition: PartitionId) -> Option<Effect> {
        let held = self.state.held()?;
        let served = held.served.get(&partition)?;
        let lineage = Lineage {
            partition,
            generation: served.generation,
            owner_epoch: served.owner_epoch,
        };
        if held.partition_deny(lineage, &self.revoked_epochs).is_some() {
            return None;
        }
        let (valid_through_tick, past_horizon) = self.admission_horizon(held, ctx);
        let view = AuthorityView {
            lineage,
            grant_id: held.grant,
            boot_id: held.boot,
            authority_generation: held.authority_generation,
            config_version: served.config_version,
            authority_seq: self.authority_seq,
            valid_through_tick,
            past_horizon,
        };
        Some(Self::authority(
            event,
            AuthorityEffect::PublishAuthorityView(view),
        ))
    }

    /// [`Self::publish`] for every served partition, in `PartitionId` order (lead ruling A-R54.1,
    /// finding N1).
    ///
    /// For the three rows that move the **node-wide** horizon without moving a lineage: a moved
    /// sample, a committed renewal and an adopted grant record. [`Self::admission_horizon`] reads
    /// only the grant and the clock, so what it changes it changes for every partition, and
    /// since A-R53.4 a view promises into the future: one left standing after its horizon shrank
    /// admits where [`Self::may_admit_at`] denies. Before A-R54 these rows published for the
    /// step's partition only, which is not even a served one when the event names another.
    fn publish_all(&self, ctx: &StepCtx<'_>, event: &Event) -> Vec<Effect> {
        let Some(held) = self.state.held() else {
            return Vec::new();
        };
        held.served
            .keys()
            .filter_map(|partition| self.publish(ctx, event, *partition))
            .collect()
    }

    /// The latest tick at which both admission conjuncts still hold, and the reason that ends it
    /// (finding E, lead ruling A-R53.4).
    ///
    /// Pure, and it saturates to `now - 1` when there is no solution — a view that is already
    /// past cannot admit anything (finding K-A-53). The local conjunct's horizon is arithmetic.
    /// The bounded-clock conjunct's is found by asking [`utc_ok`] itself, because the effective
    /// epsilon grows with age and a closed form would restate the drift rule in a second place.
    ///
    /// # Why a bisection is exact
    ///
    /// For ticks at or after the sample, [`utc_ok`] holds on a prefix and never again after it
    /// first fails: the extrapolated clock and the epsilon both grow with the tick, the sample
    /// only ages, and the mode does not change inside one step. So the last tick it holds is a
    /// single boundary, and bisecting between `now` (holds) and the local horizon (fails) finds
    /// it in about a dozen calls. Before A-R53 this fell back to `now` whenever the clock
    /// conjunct failed before the local horizon, which in steady state is always, so every view
    /// promised nothing past its own tick.
    fn admission_horizon(&self, held: &Held, ctx: &StepCtx<'_>) -> (Tick, DenyReason) {
        let expired_at = held
            .renewed_at
            .0
            .saturating_add(ctx.budgets.grant_millis)
            .saturating_sub(ctx.budgets.dispatch_margin_millis);
        let local_horizon = expired_at.saturating_sub(1);
        let holds = |tick: u64| utc_ok(&self.clock, held.expiry_utc_ms, Tick(tick), ctx.budgets);
        if let Err(reason) = holds(ctx.now.0) {
            return (Tick(ctx.now.0.saturating_sub(1)), reason);
        }
        if local_horizon < ctx.now.0 {
            return (Tick(local_horizon), DenyReason::Expired);
        }
        let (mut good, mut bad) = match holds(local_horizon) {
            Ok(()) => return (Tick(local_horizon), DenyReason::Expired),
            Err(_) => (ctx.now.0, local_horizon),
        };
        // Invariant: `holds(good)` and not `holds(bad)`.
        while bad - good > 1 {
            let mid = good + (bad - good) / 2;
            if holds(mid).is_ok() {
                good = mid;
            } else {
                bad = mid;
            }
        }
        let reason = holds(bad).err().unwrap_or(DenyReason::Expired);
        (Tick(good), reason)
    }

    // ---- the control seam ------------------------------------------------------------------

    /// The family a single record belongs to.
    ///
    /// A watch and a reload are scoped to a [`ControlPrefix`]; a change names a [`ControlKey`].
    /// This is the only mapping between them, and it is total.
    const fn family_of(key: ControlKey) -> ControlPrefix {
        match key {
            ControlKey::ClusterSchema => ControlPrefix::ClusterSchema,
            ControlKey::Node(_) => ControlPrefix::Nodes,
            ControlKey::Grant(_) => ControlPrefix::Grants,
            ControlKey::Partition(_) => ControlPrefix::Partitions,
            ControlKey::Route(_) => ControlPrefix::Routes,
            ControlKey::Operation(_) => ControlPrefix::Operations,
            ControlKey::PlannerGrant => ControlPrefix::PlannerGrant,
        }
    }

    /// A contiguous run of changes arrived, and nothing between the cursors was skipped.
    ///
    /// **A watch event never widens rights** (`design.md` §2.4 property 3): each change becomes a
    /// linearizable [`ControlEffect::Get`] of the record that changed, and the kernel believes
    /// nothing until that read answers. No reload is emitted here — the stream has not gapped, so
    /// there is nothing to reload against.
    fn on_watched(
        &mut self,
        event: &Event,
        prefix: ControlPrefix,
        cursor_revision: Revision,
        changes: &[ControlChange],
    ) -> Vec<Effect> {
        self.watch_refused_attempts = 0;
        self.cursors.insert(prefix, cursor_revision);
        self.reads(event, changes)
    }

    /// One fresh `Get` per watched change, in order.
    fn reads(&mut self, event: &Event, changes: &[ControlChange]) -> Vec<Effect> {
        changes
            .iter()
            .map(|change| self.get(event, change.key))
            .collect()
    }

    /// The stream ended, and [`WatchTermination::is_gap`] — not the stream going quiet — says
    /// whether anything was missed.
    fn on_terminated(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        prefix: ControlPrefix,
        from: Revision,
        termination: WatchTermination,
    ) -> Vec<Effect> {
        let resume = self.cursors.get(&prefix).copied().unwrap_or(from);

        if termination.is_gap() {
            // The one place a reload is legal. The re-watch is deferred to the
            // `FamilySnapshot` arm, which is what makes reload-then-re-watch a closed loop
            // rather than a race: the resumed watch starts after the revision the snapshot was
            // coherent at, and that revision is not known until the snapshot arrives.
            self.watch_refused_attempts = 0;
            return vec![Self::control(event, ControlEffect::Reload { prefix })];
        }

        match termination {
            // A capacity error, not a gap. Bounded re-arm, and never a reload.
            WatchTermination::ResourceExhaustedFatal => {
                self.watch_refused_attempts = self.watch_refused_attempts.saturating_add(1);
                if self.watch_refused_attempts >= WATCH_ADMISSION_ATTEMPT_CAP {
                    // The latch (lead ruling A-R41). A fact rather than an ignore reason because
                    // A1 does not merely decline this one — it stops re-arming until something
                    // external resets the counter, and the absence of a re-arm is the claim the
                    // row makes. `watch_refused_attempts` can already be read as state; the fact
                    // is what says the silence that follows was deliberate.
                    vec![Self::authority(
                        event,
                        AuthorityEffect::Fact(AuthorityFact::WatchAdmissionExhausted),
                    )]
                } else {
                    // Under the cap: declined this time, will retry — and the retry is now
                    // *timed* rather than immediate. An immediate re-watch answers a capacity
                    // refusal by making the same request again in the same instant, which is the
                    // admission-limit-as-outage shape the termination type exists to spell out;
                    // `AuthorityTimer::WatchBackoff` was declared for this and returned
                    // `Unavailable` until now, which is why M7A-31's "bounded backoff,
                    // non-decreasing, `backoff_20 == cap`" had no subject.
                    //
                    // The reason is said out loud (lead ruling A-R41) rather than left as a bare
                    // re-arm: an effect vector that is only a timer cannot be told from a kernel
                    // that armed one for something else.
                    self.watch_backoff.insert(prefix, resume);
                    let at = ctx
                        .now
                        .plus_millis(Self::watch_backoff_millis(self.watch_refused_attempts));
                    vec![
                        Self::ignored(event, AuthorityIgnoreReason::AdmissionRefused),
                        self.arm(event, AuthorityTimer::WatchBackoff, at),
                    ]
                }
            }
            // Leadership moved, or the hub went away. Read our own record before believing
            // anything, and resume the watch after a back-off (`design.md` §2.4, `Held |
            // WatchGap{NotLeader|Unavailable}`; M7A-30). The cursor is still good, so this is a
            // resume from it, never a reload; and it is not an admission refusal.
            WatchTermination::NotLeader | WatchTermination::Unavailable => {
                self.watch_backoff.insert(prefix, resume);
                let at = ctx.now.plus_millis(Self::watch_backoff_millis(1));
                vec![
                    self.get(event, ControlKey::Grant(ctx.node)),
                    self.arm(event, AuthorityTimer::WatchBackoff, at),
                ]
            }
            // Unreachable: both remaining variants answer `true` to `is_gap` and returned above.
            // Spelled out rather than caught by a wildcard so that a sixth termination variant
            // fails compilation here instead of silently taking the no-reload path.
            WatchTermination::RevisionCompacted { .. }
            | WatchTermination::ResourceExhaustedResumable => Vec::new(),
        }
    }

    /// A coherent snapshot of one family arrived. Resume the watch after the revision the whole
    /// snapshot is coherent at, which closes the reload loop.
    ///
    /// # The partitions family **replaces** `served`; it does not merge into it
    ///
    /// A coherent snapshot is the whole truth about the family at one revision, so a partition
    /// this node was serving that the snapshot does not list has had its ownership withdrawn.
    /// Merging would keep serving it for ever, and — this is the trap — merging and replacing
    /// emit the *same effects* and admit the same partitions for every partition still listed,
    /// so no trace distinguishes them. That is why [`AuthorityStateView::served`] hands a row the
    /// map itself (lead ruling A-R37, the Manual Tester's B6).
    ///
    /// An older snapshot than the one already installed is ignored:
    /// [`AuthorityStateView::partitions_revision`] is exactly what makes that decidable, and it
    /// is not the cursor (lead ruling A-R30).
    fn on_family_snapshot(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        prefix: ControlPrefix,
        snapshot_revision: Revision,
        records: &[ControlRecord],
    ) -> Vec<Effect> {
        self.watch_refused_attempts = 0;
        self.cursors.insert(prefix, snapshot_revision);
        let mut effects = Vec::new();

        if prefix == ControlPrefix::Partitions
            && self
                .state
                .held()
                .is_some_and(|held| snapshot_revision.0 >= held.partitions_revision.0)
        {
            effects.extend(self.install_partitions(ctx, event, snapshot_revision, records));
            // The takeover side orders itself by its own per-partition mark (A-R52), so an older
            // snapshot does not re-start a takeover that a newer read dropped (T1, T2).
            effects.extend(self.observe_snapshot(event, ctx.node, snapshot_revision, records));
        }

        effects.push(Self::control(
            event,
            ControlEffect::Watch {
                prefix,
                from: snapshot_revision,
            },
        ));
        effects
    }

    /// Install a coherent `partitions/*` snapshot as the whole of `served`.
    ///
    /// The sole writer of [`Held::served`] (finding K-A-04) together with the single-record adopt
    /// row, [`Self::adopt_partition`]. Bumps `authority_seq` once — the lineage moved, and that is what
    /// the sequence counts (finding K-A-34) — and emits one
    /// [`EffectKind::AdoptAuthority`] and one [`AuthorityEffect::PublishAuthorityView`] per
    /// partition it now serves and may admit ([`Self::adopt`]), in [`PartitionId`] order because
    /// the map is ordered.
    ///
    /// # Replace, not merge — with one carve-out that is not a merge (A-R37, A-R48)
    ///
    /// Lead ruling A-R37: a coherent snapshot is the whole truth about the family at one
    /// revision `S`, so it **replaces** `served`. A partition the snapshot does not list is
    /// dropped, and every entry it does list is installed at revision `S`. A dropped partition
    /// that was served is a removal and fences first — `Fence{Partition(p), GenerationChanged}`
    /// and its past view, in the same step (lead ruling A-R54.2). So does a served partition the
    /// snapshot moves to a lineage whose adopt is withheld (lead ruling A-R56.1): see
    /// `Held::must_fence`.
    ///
    /// Lead ruling A-R48 carves out exactly one case: an entry whose installed revision is
    /// **strictly greater than `S`** survives, including when the snapshot lacks its key. That
    /// entry came from a linearizable read taken *after* the snapshot, so it is newer information
    /// about that key than the snapshot is; the snapshot was merely delivered later. Without the
    /// carve-out a reordered reload rolls `served[id]` back to an older lineage of ours and
    /// publishes a lower `owner_epoch` after a higher one.
    ///
    /// The limit is what keeps this from reopening merge. An entry installed at or below `S` —
    /// which is every entry a previous snapshot installed, and every read the snapshot already
    /// reflects — gets no protection at all: if the snapshot lacks it, it is gone. The carve-out
    /// keeps only what the snapshot *could not have seen*.
    ///
    /// Lead ruling A-R48b applies the same limit to removals. A tombstone newer than `S` keeps its
    /// key absent even when the snapshot lists it as ours: a fence at 25 is not undone by a
    /// snapshot taken at 17. A tombstone at or below `S` is dropped, since the snapshot has seen
    /// that removal.
    ///
    /// A record this build cannot decode is skipped and reported once with
    /// [`AuthorityIgnoreReason::FamilyRejected`]: a body A1 cannot read is not a partition it
    /// owns, and a snapshot is not rejected wholesale for one unreadable member. Reported rather
    /// than dropped, because "the family had a record I ignored" and "the family was empty" are
    /// different facts.
    fn install_partitions(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        snapshot_revision: Revision,
        records: &[ControlRecord],
    ) -> Vec<Effect> {
        let mut served = BTreeMap::new();
        let mut rejected = false;
        for record in records {
            if Self::family_of(record.key) != ControlPrefix::Partitions {
                continue;
            }
            match PartitionRecord::decode(&record.value) {
                Some(partition) if partition.owner == ctx.node => {
                    served.insert(partition.partition, partition.lineage());
                }
                // Someone else's partition. Not a fence: this node never claimed it, and a fence
                // per unowned record would fence the whole family on every snapshot.
                Some(_) => {}
                None => rejected = true,
            }
        }

        let AuthorityState::Held(held) = &mut self.state else {
            return Vec::new();
        };
        let mut revisions: BTreeMap<PartitionId, Revision> =
            served.keys().map(|id| (*id, snapshot_revision)).collect();
        // The carve-out (lead ruling A-R48), and its limit. See the doc above: strictly newer
        // entries only, and this is not a merge.
        for (id, installed) in &held.served_revisions {
            if installed.0 > snapshot_revision.0 {
                if let Some(lineage) = held.served.get(id) {
                    served.insert(*id, *lineage);
                    revisions.insert(*id, *installed);
                }
            }
        }
        // The same carve-out for removals (lead ruling A-R48b): a tombstone newer than `S` is a
        // removal the snapshot could not have seen, so the key stays absent and keeps it. A
        // tombstone at or below `S` says nothing the snapshot does not, so it goes. Written in
        // both branches below, because dropping a tombstone is not a write of `served`.
        held.removed_revisions
            .retain(|_, removed| removed.0 > snapshot_revision.0);
        for id in held.removed_revisions.keys() {
            served.remove(id);
            revisions.remove(id);
        }
        if held.served == served
            && held.served_revisions == revisions
            && held.partitions_revision == snapshot_revision
        {
            // The snapshot installed exactly what was already installed: no write of `served`,
            // so `authority_seq` does not move and no view is published (lead ruling A-R41).
            let mut effects = vec![Self::ignored(
                event,
                AuthorityIgnoreReason::LineageUnchanged,
            )];
            if rejected {
                effects.push(Self::ignored(event, AuthorityIgnoreReason::FamilyRejected));
            }
            return effects;
        }
        let superseded: Vec<PartitionId> = held
            .served
            .keys()
            .filter(|id| held.must_fence(**id, served.get(id), &self.revoked_epochs))
            .copied()
            .collect();

        let mut effects = Vec::new();
        if rejected {
            effects.push(Self::ignored(event, AuthorityIgnoreReason::FamilyRejected));
        }
        // A served partition the snapshot drops, or moves to a lineage the adopt below would
        // withhold, loses its served lineage with no view to replace it, and that fences (lead
        // rulings A-R54.2 and A-R56.1, findings N3 and N4). Fenced while `served` still holds it,
        // so its past view carries the lineage it was served under. Before those rulings its last
        // view kept admitting to its horizon on a lineage this node no longer served.
        for partition in superseded {
            effects.extend(self.fence(
                ctx,
                event,
                FenceScope::Partition(partition),
                DenyReason::GenerationChanged,
            ));
        }
        let AuthorityState::Held(held) = &mut self.state else {
            return effects;
        };
        held.served_revisions = revisions;
        held.served = served;
        held.partitions_revision = snapshot_revision;
        self.authority_seq = self.authority_seq.saturating_add(1);

        let Some(held) = self.state.held() else {
            return effects;
        };
        let installed: Vec<PartitionId> = held.served.keys().copied().collect();
        for partition in installed {
            effects.extend(self.adopt(ctx, event, partition));
        }
        effects.push(Self::authority(
            event,
            AuthorityEffect::Fact(AuthorityFact::LineageLoaded),
        ));
        effects
    }

    /// A linearizable read of one `partitions/{id}` answered.
    ///
    /// The **third** partition-scoped fence (lead ruling A-R33), and it has two triggers that
    /// `design.md` §2.4 lists separately: the record names an owner that is not us, and the
    /// record is absent. Both are [`DenyReason::GenerationChanged`] scoped to that one partition
    /// — the control plane moved a partition, which says nothing about this node's grant, so a
    /// node fence here would drop every other partition for no reason.
    ///
    /// A record that *is* ours goes to [`Self::adopt_partition`], the single-record adopt row.
    fn on_partition_read(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        id: PartitionId,
        outcome: &ReadOutcome,
        recovery: Option<Generation>,
    ) -> Result<Vec<Effect>, RdbError> {
        if !self.state.is_held() {
            return Ok(vec![Self::ignored(
                event,
                AuthorityIgnoreReason::StaleAuthorityView,
            )]);
        }
        let reason = match outcome {
            // Control quorum was lost. A deny, never a fence: the record has not changed, we
            // simply cannot see it (property 6).
            ReadOutcome::Unavailable => {
                return Ok(vec![Self::ignored(
                    event,
                    AuthorityIgnoreReason::AdmissionSuspended,
                )]);
            }
            ReadOutcome::Absent { .. } => partition::classify_absent(),
            ReadOutcome::Found { revision, value } => {
                let Some(record) = PartitionRecord::decode(value) else {
                    return Ok(vec![Self::ignored(
                        event,
                        AuthorityIgnoreReason::FamilyRejected,
                    )]);
                };
                match partition::classify(&record, ctx.node) {
                    Some(reason) => reason,
                    None => {
                        return Ok(
                            self.adopt_partition(ctx, event, id, *revision, &record, recovery)
                        )
                    }
                }
            }
        };
        // Nothing to withdraw: this node was not serving the partition, so there is no right to
        // end and no view to supersede. Fencing anyway would emit a `Fence` per read of a
        // partition we never had, which is exactly the "exactly one Fence per trigger" claim
        // A-R37 asks a row to be able to make.
        if !self
            .state
            .held()
            .is_some_and(|held| held.served.contains_key(&id))
        {
            // Lead ruling A-R41. `StaleAuthorityView` stood in here and is not the same claim:
            // that one is about a view this node holds having been superseded, this one is about
            // a partition it never served.
            return Ok(vec![Self::ignored(event, AuthorityIgnoreReason::NotOurs)]);
        }
        // The revision this removal was learned at (lead ruling A-R48b). `Unavailable` returned
        // above, so every outcome reaching here carries one.
        let removed_at = match outcome {
            ReadOutcome::Found { revision, .. } => *revision,
            ReadOutcome::Absent { as_of } => *as_of,
            ReadOutcome::Unavailable => Revision::default(),
        };
        let effects = self.fence(ctx, event, FenceScope::Partition(id), reason);
        if let AuthorityState::Held(held) = &mut self.state {
            held.remove_served(id, removed_at);
        }
        Ok(effects)
    }

    /// A linearizable read of `partitions/{id}` found a record naming this node as owner: the
    /// single-record adopt path (`design.md` §2.4, the partition lineage path).
    ///
    /// The second of the two writers of [`Held::served`] (finding K-A-04), after
    /// [`Self::install_partitions`]. Property 3 is why this is the row that may widen rights and
    /// the watch arm is not: a watch event carries a revision, never a body, and only a
    /// linearizable read of the record says what the lineage *is*.
    ///
    /// # The rows, first match wins (finding K-A-43)
    ///
    /// 1. **Unchanged** — the lineage already served. No write, no bump, no view.
    /// 2. **Superseded** — a different lineage at a revision no newer than the one this
    ///    partition's entry was installed at (lead ruling A-R48). The design table has no row for
    ///    it; see [`AuthorityIgnoreReason::PartitionReadSuperseded`].
    /// 3. **The post-`Recovered` install** — `recovery` is the generation the `Recovered(r)`
    ///    trigger row named, because this read is the one it issued (lead ledger L-R177gf), and
    ///    the record names that generation: the write below, with `Fact(LineageInstalled)`. It
    ///    is matched ahead of the changed row, as the design puts it (finding K-A-55), so
    ///    `LineageInstalled` is reachable although the revision has advanced and `served` lacks
    ///    or differs at `id`. **Deliberately below rows 1 and 2**, not above them as the design
    ///    table lists it: a read-back of a lineage already served (a coherent load got there
    ///    first) installs nothing and bumps nothing, and one older than an install already made
    ///    must not roll it back (A-R48). Both only narrow what the design row would do.
    /// 4. **Changed** — `served[id]` rewritten, `authority_seq += 1`, and the same three emit
    ///    points [`Self::install_partitions`] uses, in the same order. A move off a served lineage
    ///    onto one the adopt withholds fences the old lineage first, as the snapshot path does
    ///    (lead ruling A-R56.1, finding N4). Rows 3 and 4 differ only in the fact.
    ///
    /// A body naming a different partition than the key it was read from is refused before any
    /// of these, because the branch widens rights. The fence path above is deliberately not given
    /// the same guard: it narrows rights, and a mismatched body there fences the key's partition
    /// exactly as it did before this build.
    ///
    /// # The ordering guard is per partition (lead ruling A-R48)
    ///
    /// The design's guard was `revision > partitions_revision`, and `partitions_revision` is the
    /// **snapshot's** revision, which this row does not move. That ordered a single read against
    /// the snapshot but not two reads of one partition against each other: delivered newest
    /// first, the older one installed a lower `owner_epoch` over a higher one. The guard is now
    /// the revision *this partition's* entry was installed at, from
    /// [`AuthorityStateView::served_revisions`]; a key with no entry falls back to
    /// `partitions_revision`, which is when the last snapshot said it was not ours. The other
    /// half of the same ruling — an older snapshot delivered after a newer read — is the
    /// carve-out in [`Self::install_partitions`].
    ///
    /// # A removal keeps its revision (lead ruling A-R48b)
    ///
    /// Both removal sites (the owner-moved/absent fence above, and the epoch-revocation fence)
    /// leave a tombstone in [`AuthorityStateView::removed_revisions`] through
    /// `Held::remove_served`, and a removed key is guarded by that instead of by
    /// `partitions_revision`. Without it, a read of ours at 15 delivered *after* a read at 25 that
    /// fenced the key re-adopted it, re-granting serving rights after a fence. An adopt clears the
    /// key's tombstone, so the two maps never share a key.
    fn adopt_partition(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        id: PartitionId,
        revision: Revision,
        record: &PartitionRecord,
        recovery: Option<Generation>,
    ) -> Vec<Effect> {
        if record.partition != id {
            return vec![Self::ignored(event, AuthorityIgnoreReason::FamilyRejected)];
        }
        let lineage = record.lineage();
        let AuthorityState::Held(held) = &mut self.state else {
            // The caller returned for every state but `Held` already.
            return vec![Self::ignored(
                event,
                AuthorityIgnoreReason::StaleAuthorityView,
            )];
        };
        if held.served.get(&id) == Some(&lineage) {
            return vec![Self::ignored(
                event,
                AuthorityIgnoreReason::LineageUnchanged,
            )];
        }
        // Lead ruling A-R48: newer *about this partition*, not merely newer than the snapshot.
        // A served key has its install revision. A removed key has its tombstone (A-R48b).
        // Any other key: the last coherent snapshot said it was not ours as of
        // `partitions_revision`, so that is what a read of it must be newer than.
        let installed = held
            .served_revisions
            .get(&id)
            .or_else(|| held.removed_revisions.get(&id))
            .copied()
            .unwrap_or(held.partitions_revision);
        if revision.0 <= installed.0 {
            return vec![Self::ignored(
                event,
                AuthorityIgnoreReason::PartitionReadSuperseded,
            )];
        }
        // A move to a lineage the adopt would withhold fences the old one first, while `served`
        // still holds it (lead ruling A-R56.1, finding N4) — the same shape as a snapshot's.
        let mut effects = if held.must_fence(id, Some(&lineage), &self.revoked_epochs) {
            self.fence(
                ctx,
                event,
                FenceScope::Partition(id),
                DenyReason::GenerationChanged,
            )
        } else {
            Vec::new()
        };
        let AuthorityState::Held(held) = &mut self.state else {
            return effects;
        };
        held.served.insert(id, lineage);
        held.served_revisions.insert(id, revision);
        held.removed_revisions.remove(&id);
        self.authority_seq = self.authority_seq.saturating_add(1);

        effects.extend(self.adopt(ctx, event, id));
        // The install row: the read the `Recovered(r)` trigger issued, naming `r.new_generation`
        // (`part.owner == us` is `partition::classify`'s, already passed). Anything else that
        // reaches here is the generic changed row.
        let fact = if recovery == Some(record.generation) {
            AuthorityFact::LineageInstalled
        } else {
            AuthorityFact::LineageChanged
        };
        effects.push(Self::authority(event, AuthorityEffect::Fact(fact)));
        effects
    }

    /// `Held|Unheld|Fenced | Recovered(r)`: the trigger half of `design.md` §2.4's `Recovered`
    /// pair (lead ledger L-R177gf). **No rights change here** — F1's result says what it
    /// selected, and only a linearizable read of `partitions/{r.partition}` may widen rights
    /// (property 3). So this issues that read, remembers it for the install row, and nothing
    /// else moves.
    ///
    /// * **The read is issued in every state.** An `Unheld` or `Fenced` node's read-back installs
    ///   nothing (`on_control` answers it before the held arms). The grant it acquires later
    ///   loads the family coherently, and that load installs the recovered generation
    ///   (`LineageLoaded`, §2.1) — lead ruling A-R78.
    /// * **`Fact(RecoveryObserved)` only when `r.fenced_prior` names a lineage this node does not
    ///   serve** — the design row's guard. Its complement had no row: a node that re-acquired
    ///   and reloaded the prior `(generation, owner_epoch)` before F1's activation CAS, then ran
    ///   the recovery itself. Lead ruling A-R78: the `Get` is still issued, the fact is not, and
    ///   the read-back lands on the install row.
    ///
    /// Both effects are routed to `r.fenced_prior.partition`, the partition recovered.
    fn on_recovered(&mut self, event: &Event, result: &RecoveryResult) -> Vec<Effect> {
        let prior = &result.fenced_prior;
        let partition = prior.partition;
        let request = self.request();
        self.recovery_reads.insert(
            partition,
            RecoveryRead {
                request,
                generation: result.new_generation,
            },
        );
        let serves_prior = self
            .state
            .held()
            .and_then(|held| held.served.get(&partition))
            .is_some_and(|served| {
                served.generation == prior.prior_generation
                    && served.owner_epoch == prior.prior_owner_epoch
            });
        let read = ControlEffect::Get {
            request,
            key: ControlKey::Partition(partition),
        };
        let mut effects = vec![Self::about(Self::control(event, read), partition)];
        if !serves_prior {
            let observed = AuthorityEffect::Fact(AuthorityFact::RecoveryObserved);
            effects.push(Self::about(Self::authority(event, observed), partition));
        }
        effects
    }

    /// The generation a `Recovered` trigger's read of `partitions/{id}` named, if the answer
    /// echoing `request` is that read's; the entry is removed either way it matches. `None` for
    /// any other read, a read of the same key under the same correlation included.
    fn take_recovery_read(
        &mut self,
        request: ControlRequestId,
        id: PartitionId,
    ) -> Option<Generation> {
        let read = self.recovery_reads.get(&id)?;
        if read.request != request {
            return None;
        }
        self.recovery_reads.remove(&id).map(|read| read.generation)
    }

    /// [`EffectKind::AdoptAuthority`] for an installed `partition`, then its view, or neither
    /// (lead ruling A-R53.5, the tester's closure of §B7 Q2).
    ///
    /// Both tell a reader "this node serves this lineage now": the dispatcher, and a secondary.
    /// [`Self::publish`] withholds the view when [`Held::partition_deny`] refuses the lineage, and
    /// the adopt is withheld with it. Before A-R53 an install that landed on a revoked epoch or a
    /// storage-fenced partition still told the dispatcher to serve what `may_admit` refused.
    fn adopt(&self, ctx: &StepCtx<'_>, event: &Event, partition: PartitionId) -> Vec<Effect> {
        let Some(lineage) = self
            .state
            .held()
            .and_then(|held| held.served.get(&partition).copied())
        else {
            return Vec::new();
        };
        let Some(view) = self.publish(ctx, event, partition) else {
            return Vec::new();
        };
        let adopt = Self::effect(
            event,
            EffectKind::AdoptAuthority {
                partition,
                generation: lineage.generation,
                owner_epoch: lineage.owner_epoch,
                config_version: lineage.config_version,
            },
        );
        vec![adopt, view]
    }

    // ---- acquisition: `Unheld → Held` (`design.md` §2.4, "Acquisition") -------------------

    /// Re-arm [`AuthorityTimer::Acquire`] after an acquisition that did not happen.
    ///
    /// `design.md` §2.4 says "backoff rearm `AcquireDue`" in two rows and names no interval, and
    /// [`Budgets`] has no acquisition field. [`Budgets::renew_millis`] stands in: it is the
    /// cadence a grant is re-asserted at, so a node retries acquiring no faster than a holder
    /// renews. **An open decision, not a derivation** — [`Budgets`] is foundation's contract, and
    /// K-A-07's rule that independent rates get independent fields argues for its own field.
    fn acquire_retry(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Effect {
        let at = ctx.now.plus_millis(ctx.budgets.renew_millis);
        self.arm(event, AuthorityTimer::Acquire, at)
    }

    /// Set or clear the acquisition in flight. A no-op outside [`AuthorityState::Unheld`], which
    /// is the only state that has one.
    fn set_acquire(&mut self, next: Option<Acquire>) {
        if let AuthorityState::Unheld { acquire, .. } = &mut self.state {
            *acquire = next;
        }
    }

    /// `Unheld | AcquireDue`, the three rows in `design.md` §2.4's order; first match wins.
    ///
    /// 1. A CAS already in flight: [`AuthorityIgnoreReason::StaleTimer`]. One create-only CAS
    ///    at a time, so its completion is never ambiguous.
    /// 2. No `E_new` — no valid, fresh, in-bound sample: [`AuthorityIgnoreReason::AcquireWithheld`]
    ///    and a retry. **No CAS**, the same rule as the renewal: no sample, no `E_new`, no write
    ///    (ADR-rdb-0007 §2, finding K-A-36).
    /// 3. Otherwise one create-only `Cas` of `grants/{node}` carrying a new grant id, this boot
    ///    and `E_new` at this tick, and `acquire` remembers all three plus the request id it was
    ///    sent as.
    ///
    /// # What arms the first one: nothing yet
    ///
    /// There is no start-of-life event — [`NodeLifecycle`] has `Resumed` and `Rebooted`, not
    /// `Started` — so nothing in this module arms the first `AcquireDue`. Timer versions start
    /// at zero, so a fixture or a scenario seeds the first firing at version zero and it is
    /// live. Where a real node's first wake comes from is an open decision.
    ///
    /// The `authority_generation` written is the default. A node has no source for the cluster's
    /// current one — `design.md` puts the grant service at M8/M9 — and a read-back adopts
    /// whatever the record says.
    fn on_acquire_due(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Vec<Effect> {
        if self.state.acquire().is_some() {
            return vec![Self::ignored(event, AuthorityIgnoreReason::StaleTimer)];
        }
        let Some(e_new) = e_new(&self.clock, ctx.now, ctx.budgets) else {
            return vec![
                Self::ignored(event, AuthorityIgnoreReason::AcquireWithheld),
                self.acquire_retry(ctx, event),
            ];
        };
        self.grants_issued = self.grants_issued.saturating_add(1);
        let grant = GrantId(self.grants_issued);
        let record = GrantRecord {
            grant,
            node: ctx.node,
            boot: ctx.boot,
            authority_generation: AuthorityGeneration::default(),
            expiry_utc_ms: e_new,
            frozen: false,
        };
        let request = self.request();
        self.set_acquire(Some(Acquire {
            request,
            dispatched_at: ctx.now,
            e_new,
            grant,
        }));
        vec![Self::control(
            event,
            ControlEffect::Cas {
                request,
                key: ControlKey::Grant(ctx.node),
                expected: None,
                value: Some(record.encode()),
            },
        )]
    }

    /// A CAS of `grants/{node}` completed.
    ///
    /// # Matched by request id, or not at all (lead ruling A-R47)
    ///
    /// `design.md` §2.4 matches every completion row on `op == acquire.op`; the
    /// [`ControlRequestId`] the answer echoes is that identity (see [`Renewal`]). A completion that matches no CAS in flight is
    /// [`AuthorityIgnoreReason::UnmatchedCompletion`] and moves nothing. Until A-R47 an
    /// `Unheld` kernel adopted **any** `Committed` on a grant key — the one-event shortcut every
    /// fixture used — and a commit nobody here issued is not a grant.
    ///
    /// A CAS of any other key is not one A1 issues, and is answered with no effects, as before.
    ///
    /// # The matched rows
    ///
    /// * `Committed(rev)`: [`Self::enter_held`] with `E = acquire.e_new` and
    ///   `renewed_at = acquire.dispatched_at` — both from the **dispatch**, never from the
    ///   completion (finding K-A-06).
    /// * `Conflict`: `Get(grants/{node})` and [`AuthorityFact::AcquireLost`]. The loser learns
    ///   nothing from the conflict itself; rEtcd ADR-0006 hides the value.
    /// * `Unknown`: `Get(grants/{node})`, and no rights assumed either way (rEtcd ADR-0015).
    /// * `Unavailable`: the same as `Unknown`. **`design.md` has no `Unheld` row for it**; this
    ///   mirrors its `Held | Unavailable` renewal row, which reads the key back. A store that
    ///   could not be reached proves the write did not land no more than `Unknown` does, so the
    ///   read is what settles it, and the read's own `Unavailable` re-arms the acquisition.
    ///
    /// All four clear `acquire`: the CAS is no longer in flight, whatever it did.
    ///
    /// # In the other two states
    ///
    /// `Held`: the renewal rows, [`Self::on_renewal_result`]. `Fenced`:
    /// [`AuthorityIgnoreReason::LateRenewalIgnored`] and nothing else, `design.md`'s
    /// `Fenced | CasApplied` row. `Fenced` does not remember the renewal that was in flight when
    /// it fenced, so it cannot check the design's "matches an outstanding pre-fence renewal"
    /// guard; every grant completion in `Fenced` is treated as that late renewal. Only A1 writes
    /// `grants/{node}` from this node, and `Fenced` issues no CAS, so there is nothing else it
    /// could be. Whatever it is, the answer is the same: terminal means terminal (K-A-02).
    fn on_cas_result(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        request: ControlRequestId,
        key: ControlKey,
        outcome: CasOutcome,
    ) -> Vec<Effect> {
        if key != ControlKey::Grant(ctx.node) {
            return Vec::new();
        }
        if self.state.is_held() {
            return self.on_renewal_result(ctx, event, request, key, outcome);
        }
        if self.state.is_fenced() {
            return vec![Self::ignored(
                event,
                AuthorityIgnoreReason::LateRenewalIgnored,
            )];
        }
        let Some(acquire) = self
            .state
            .acquire()
            .filter(|acquire| acquire.request == request)
        else {
            return vec![Self::ignored(
                event,
                AuthorityIgnoreReason::UnmatchedCompletion,
            )];
        };
        self.set_acquire(None);
        match outcome {
            CasOutcome::Committed(revision) => {
                let record = GrantRecord {
                    grant: acquire.grant,
                    node: ctx.node,
                    boot: ctx.boot,
                    authority_generation: AuthorityGeneration::default(),
                    expiry_utc_ms: acquire.e_new,
                    frozen: false,
                };
                self.enter_held(ctx, event, &record, revision, acquire.dispatched_at)
            }
            CasOutcome::Conflict { .. } => vec![
                self.get(event, key),
                Self::authority(event, AuthorityEffect::Fact(AuthorityFact::AcquireLost)),
            ],
            CasOutcome::Unknown | CasOutcome::Unavailable => vec![self.get(event, key)],
        }
    }

    /// `Unheld | ReadOk` on `grants/{node}`: the read-back that follows a lost or unknown
    /// acquisition, and the only read that widens rights (property 3).
    ///
    /// * A record naming this node and this boot, not frozen, whose `E` passes [`utc_ok`] now:
    ///   adopted by [`Self::enter_held`], with `renewed_at` **derived** from `E` through the
    ///   sample by [`clock::renewed_at_for`] and never set to `now` (finding K-A-06).
    /// * Any other record — another boot, frozen, or an `E` that [`utc_ok`] refuses:
    ///   [`AuthorityIgnoreReason::NotOurs`] and a retry.
    /// * **Absent, or unavailable: a retry, and no row in `design.md` says so.** The table has
    ///   `ReadOk{Some(rec)}` rows only. Absent means the key is free and unavailable means the
    ///   store cannot say; neither widens anything, and without the retry the node would never
    ///   try again.
    ///
    /// # Errors
    ///
    /// `Unavailable` naming [`Capability::Codec`] when the body is not a [`GrantRecord`], as for
    /// the held read-back.
    fn on_unheld_grant_read(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        outcome: &ReadOutcome,
    ) -> Result<Vec<Effect>, RdbError> {
        let ReadOutcome::Found { revision, value } = outcome else {
            return Ok(vec![self.acquire_retry(ctx, event)]);
        };
        let Some(record) = GrantRecord::decode(value) else {
            return Err(RdbError::unavailable(
                Capability::Codec,
                "authority: grants/{node} body is not a GrantRecord this build can read",
            ));
        };
        let adoptable = record.node == ctx.node
            && record.boot == ctx.boot
            && !record.frozen
            && utc_ok(&self.clock, record.expiry_utc_ms, ctx.now, ctx.budgets).is_ok();
        let renewed_at =
            clock::renewed_at_for(&self.clock, record.expiry_utc_ms, ctx.now, ctx.budgets);
        match renewed_at {
            Some(renewed_at) if adoptable => {
                Ok(self.enter_held(ctx, event, &record, *revision, renewed_at))
            }
            _ => Ok(vec![
                Self::ignored(event, AuthorityIgnoreReason::NotOurs),
                self.acquire_retry(ctx, event),
            ]),
        }
    }

    /// Enter [`AuthorityState::Held`] on `record`, committed at `revision` and last renewed at
    /// `renewed_at` — the effects both acquisition entries share.
    ///
    /// `Reload{partitions}`, `Watch{grants}` from `revision`, and `Arm(Renew)` one renewal
    /// interval after `renewed_at`; `authority_seq += 1` (finding K-A-34). `served` starts empty:
    /// the grant record carries no lineage, and the reload's coherent snapshot is what installs
    /// it (K-A-04).
    ///
    /// # No `PublishAuthorityView`, against `design.md` §2.4's two rows (finding F2)
    ///
    /// Both rows list one. With nothing served it could only name a lineage this node has not
    /// installed — it used to take the step context's — and [`Self::may_admit_at`] denies every
    /// such lineage `GenerationChanged`, so the view admitted where the check refused. The lead
    /// ruled "no view for an unserved partition"; the first views are the snapshot install's.
    ///
    /// # One helper for two rows, and where they differed
    ///
    /// `design.md` §2.4 gives the commit row `ReadFamily`, `Watch{grants+partitions}`,
    /// `ArmTimer(renew)`, `PublishAuthorityView`, and the read-back adopt row the same **less the
    /// watch**. Both rows produce the same `Held`, and a `Held` with no grant watch sees a
    /// freeze only when its next renewal loses, so the adopt row gets the watch too. The
    /// partitions half of the watch is not emitted here: the snapshot handler opens it from the
    /// snapshot's revision, which is the only revision it can resume from without a gap.
    fn enter_held(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        record: &GrantRecord,
        revision: Revision,
        renewed_at: Tick,
    ) -> Vec<Effect> {
        self.authority_seq = self.authority_seq.saturating_add(1);
        self.state = AuthorityState::Held(Held {
            grant: record.grant,
            node: ctx.node,
            boot: ctx.boot,
            authority_generation: record.authority_generation,
            expiry_utc_ms: record.expiry_utc_ms,
            record_revision: revision,
            renewed_at,
            renewal: None,
            served: BTreeMap::new(),
            served_revisions: BTreeMap::new(),
            removed_revisions: BTreeMap::new(),
            partitions_revision: Revision::default(),
            storage_fenced: BTreeSet::new(),
            control_unavailable: false,
        });
        self.cursors.insert(ControlPrefix::Grants, revision);
        let renew = self.arm(
            event,
            AuthorityTimer::Renew,
            renewed_at.plus_millis(ctx.budgets.renew_millis),
        );
        vec![
            Self::control(
                event,
                ControlEffect::Reload {
                    prefix: ControlPrefix::Partitions,
                },
            ),
            Self::control(
                event,
                ControlEffect::Watch {
                    prefix: ControlPrefix::Grants,
                    from: revision,
                },
            ),
            renew,
        ]
    }

    // ---- renewal: `Held`'s steady state (`design.md` §2.4, "Steady state") ------------------

    /// `Held | RenewDue`, the three rows in `design.md` §2.4's order; first match wins.
    ///
    /// 1. A renewal already in flight: [`AuthorityIgnoreReason::StaleTimer`].
    /// 2. No `E_new`: [`AuthorityIgnoreReason::RenewalWithheld`] and a retry one renewal
    ///    interval on. **No CAS** (ADR-rdb-0007 §2). Persistent withholding ends at the
    ///    `Expired` fence by itself, which is the design's intent (finding K-A-42).
    /// 3. Otherwise one `Cas` of `grants/{node}` expecting the **exact** revision held (spec
    ///    §7.3 step 1), writing the held record with `E_new` at this tick; `renewal` remembers the
    ///    request id, the dispatch tick and `E_new`.
    ///
    /// The guard is on the sample, never the mode: with [`clock::ClockMode::Unbounded`] and a valid
    /// sample the renewal is still sent (see [`Self::revalidate`] on the K-A-07 loop).
    fn on_renew_due(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Vec<Effect> {
        let Some(held) = self.state.held() else {
            return vec![Self::ignored(event, AuthorityIgnoreReason::StaleTimer)];
        };
        if held.renewal.is_some() {
            return vec![Self::ignored(event, AuthorityIgnoreReason::StaleTimer)];
        }
        let Some(e_new) = e_new(&self.clock, ctx.now, ctx.budgets) else {
            let at = ctx.now.plus_millis(ctx.budgets.renew_millis);
            return vec![
                Self::ignored(event, AuthorityIgnoreReason::RenewalWithheld),
                self.arm(event, AuthorityTimer::Renew, at),
            ];
        };
        let record = GrantRecord {
            grant: held.grant,
            node: held.node,
            boot: held.boot,
            authority_generation: held.authority_generation,
            expiry_utc_ms: e_new,
            frozen: false,
        };
        let expected = held.record_revision;
        let request = self.request();
        if let AuthorityState::Held(held) = &mut self.state {
            held.renewal = Some(Renewal {
                request,
                dispatched_at: ctx.now,
                e_new,
            });
        }
        vec![Self::control(
            event,
            ControlEffect::Cas {
                request,
                key: ControlKey::Grant(ctx.node),
                expected: Some(expected),
                value: Some(record.encode()),
            },
        )]
    }

    /// A CAS of `grants/{node}` completed while `Held`: the renewal rows.
    ///
    /// Matched by request id, as the acquisition is (lead ruling A-R47); an unmatched one is
    /// [`AuthorityIgnoreReason::UnmatchedCompletion`] and moves nothing. A matched one clears
    /// `renewal`, then:
    ///
    /// * `Committed(rev)`: `E = renewal.e_new`, `record_revision = rev`,
    ///   `renewed_at = renewal.dispatched_at` — the **dispatch**, never the completion (K-A-06) —
    ///   the next `Renew` one interval after that, and `PublishAuthorityView` for every served
    ///   partition, because the horizon moved (K-A-35, A-R54.1). **No `authority_seq` bump**: the
    ///   lineage did not move. `design.md` also lists a bare `Fact` on this row without naming
    ///   one, and [`AuthorityFact`] has no renewal-committed variant, so none is emitted.
    /// * `Conflict`: `Get(grants/{node})` and [`AuthorityFact::RenewLost`].
    /// * `Unknown`: `Get(grants/{node})` and [`AuthorityFact::RenewUnknown`].
    /// * `Unavailable`: a retry one interval on, and `Get(grants/{node})`.
    ///
    /// Only `Committed` writes `E` or `renewed_at` (property 1, rEtcd ADR-0015).
    fn on_renewal_result(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        request: ControlRequestId,
        key: ControlKey,
        outcome: CasOutcome,
    ) -> Vec<Effect> {
        let renewal = match &mut self.state {
            AuthorityState::Held(held) => {
                let Some(renewal) = held.renewal.filter(|renewal| renewal.request == request)
                else {
                    return vec![Self::ignored(
                        event,
                        AuthorityIgnoreReason::UnmatchedCompletion,
                    )];
                };
                held.renewal = None;
                if let CasOutcome::Committed(revision) = outcome {
                    held.expiry_utc_ms = renewal.e_new;
                    held.record_revision = revision;
                    held.renewed_at = renewal.dispatched_at;
                    held.control_unavailable = false;
                }
                renewal
            }
            _ => return Vec::new(),
        };
        match outcome {
            CasOutcome::Committed(_) => {
                let at = renewal.dispatched_at.plus_millis(ctx.budgets.renew_millis);
                let renew = self.arm(event, AuthorityTimer::Renew, at);
                let mut effects = vec![renew];
                effects.extend(self.publish_all(ctx, event));
                effects
            }
            CasOutcome::Conflict { .. } => vec![
                self.get(event, key),
                Self::authority(event, AuthorityEffect::Fact(AuthorityFact::RenewLost)),
            ],
            CasOutcome::Unknown => vec![
                self.get(event, key),
                Self::authority(event, AuthorityEffect::Fact(AuthorityFact::RenewUnknown)),
            ],
            CasOutcome::Unavailable => {
                let at = ctx.now.plus_millis(ctx.budgets.renew_millis);
                let renew = self.arm(event, AuthorityTimer::Renew, at);
                vec![renew, self.get(event, key)]
            }
        }
    }

    /// `Held | ReadOk{Some(rec)}` for a record [`grant::classify`] found no fence in: our grant,
    /// our boot, our authority generation, not frozen.
    ///
    /// * **A higher revision** — `design.md`'s adopt row: take `rec.E` and the revision, with
    ///   `renewed_at` derived from `rec.E` through the sample by [`clock::renewed_at_for`] and
    ///   never set to `now` (K-A-06); re-arm `Renew` from it; [`AuthorityFact::Adopted`] and
    ///   `PublishAuthorityView` for every served partition (A-R54.1). No `authority_seq` bump.
    ///   When no `renewed_at` can be derived — no sample — the one held is kept: it is older, so
    ///   the local window only narrows, which is the design's "does not admit on `local_ok` until
    ///   its own next renewal commits" in its conservative form.
    /// * **Not a higher revision** — the record already held. **No row in `design.md`.** Nothing
    ///   is adopted ([`AuthorityIgnoreReason::GrantRecordNotNewer`], §2.6a decision e3), and
    ///   `Renew` is re-armed at
    ///   the later of now and one interval after `renewed_at`. Without the re-arm an `Unknown`
    ///   renewal that did not land leaves no renewal scheduled, and the grant runs out on a
    ///   healthy store. The watch echo of our own committed renewal also lands here; it re-arms
    ///   at the deadline the commit already set.
    fn on_held_grant_record(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        record: &GrantRecord,
        revision: Revision,
    ) -> Vec<Effect> {
        let renew_millis = ctx.budgets.renew_millis;
        // A read found our record: control answers again (M7A-59).
        if let AuthorityState::Held(held) = &mut self.state {
            held.control_unavailable = false;
        }
        let Some(held) = self.state.held() else {
            return Vec::new();
        };
        if revision <= held.record_revision {
            let at = ctx.now.max(held.renewed_at.plus_millis(renew_millis));
            return vec![
                Self::ignored(event, AuthorityIgnoreReason::GrantRecordNotNewer),
                self.arm(event, AuthorityTimer::Renew, at),
            ];
        }
        let renewed_at =
            clock::renewed_at_for(&self.clock, record.expiry_utc_ms, ctx.now, ctx.budgets)
                .unwrap_or(held.renewed_at);
        if let AuthorityState::Held(held) = &mut self.state {
            held.expiry_utc_ms = record.expiry_utc_ms;
            held.record_revision = revision;
            held.renewed_at = renewed_at;
        }
        let renew = self.arm(
            event,
            AuthorityTimer::Renew,
            renewed_at.plus_millis(renew_millis),
        );
        let mut effects = vec![
            renew,
            Self::authority(event, AuthorityEffect::Fact(AuthorityFact::Adopted)),
        ];
        effects.extend(self.publish_all(ctx, event));
        effects
    }

    /// A linearizable read of `grants/{node}` answered.
    ///
    /// Four of the nine node-scoped fence triggers are decided here and nowhere else, by
    /// [`grant::classify`]: the record is frozen, it names a different boot, it names a different
    /// grant, or the cluster authority generation moved. The fifth grant-shaped trigger is the
    /// record being **absent**, which needs no record at all.
    ///
    /// The healthy read-back — a record that is ours and current — is
    /// [`Self::on_held_grant_record`] in `Held` and [`Self::on_unheld_grant_read`] in `Unheld`.
    ///
    /// # Errors
    ///
    /// `Unavailable` naming [`Capability::Codec`] when the body is not a [`GrantRecord`].
    fn on_grant_read(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        outcome: &ReadOutcome,
    ) -> Result<Vec<Effect>, RdbError> {
        if self.state.is_unheld() {
            return self.on_unheld_grant_read(ctx, event, outcome);
        }
        let Some(identity) = self.state.held().map(Held::identity) else {
            // `Fenced`. Its one way out is `design.md` §2.4's `Fenced | ReadOk` row — a record
            // naming a *new* grant id — and that row is not built: `Fenced` does not remember
            // the old id it would compare against. Until it is, a read widens nothing.
            return Ok(vec![Self::ignored(
                event,
                AuthorityIgnoreReason::StaleAuthorityView,
            )]);
        };
        match outcome {
            // The grant record is gone. A durable drain proof was recorded, or the planner
            // deleted it; either way this node's right is over.
            ReadOutcome::Absent { .. } => {
                Ok(self.fence(ctx, event, FenceScope::Node, DenyReason::Revoked))
            }
            // Control quorum was lost. A **deny**, never "probably still fine" — and never a
            // fence: the grant has not ended, we simply cannot see it (property 6). The deny is
            // remembered until a read finds our record (M7A-59; lead ruling A-R77a). Views are
            // not republished: published service ends at local expiry (ADR-rdb-0008, "Control-
            // quorum loss denies"), and the deny is a check-time answer only.
            ReadOutcome::Unavailable => {
                if let AuthorityState::Held(held) = &mut self.state {
                    held.control_unavailable = true;
                }
                Ok(vec![Self::ignored(
                    event,
                    AuthorityIgnoreReason::AdmissionSuspended,
                )])
            }
            ReadOutcome::Found { revision, value } => {
                let Some(record) = GrantRecord::decode(value) else {
                    return Err(RdbError::unavailable(
                        Capability::Codec,
                        "authority: grants/{node} body is not a GrantRecord this build can read",
                    ));
                };
                match grant::classify(&record, identity) {
                    Some(reason) => Ok(self.fence(ctx, event, FenceScope::Node, reason)),
                    None => Ok(self.on_held_grant_record(ctx, event, &record, *revision)),
                }
            }
        }
    }

    /// Route one control completion.
    fn on_control(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        control: &ControlEvent,
    ) -> Result<Vec<Effect>, RdbError> {
        match control {
            ControlEvent::CasResult {
                request,
                key,
                outcome,
            } => Ok(self.on_cas_result(ctx, event, *request, *key, *outcome)),

            ControlEvent::Value {
                key: ControlKey::Grant(node),
                outcome,
                ..
            } if *node == ctx.node => self.on_grant_read(ctx, event, outcome),

            // An unheld node still reads what a watch names, and only the read's answer can
            // grant (TD-17; M7A-34, M7A-35). A watch event carries a revision, never a body, so
            // it issues the same `Get` a held node would and nothing else: the cursor and the
            // refusal counter belong to a held node's stream (A-R78 F2).
            ControlEvent::Watched { changes, .. } if self.state.is_unheld() => {
                Ok(self.reads(event, changes))
            }

            // The read-back of a `Recovered` trigger reaching a node that holds no grant (lead
            // ruling A-R78): nothing installs, because there is no grant to serve under. The
            // acquire's coherent load installs the recovered generation (`design.md` §2.4/§2.1).
            // The read is answered, so it is forgotten, and a later read cannot pass for it.
            ControlEvent::Value {
                request,
                key: ControlKey::Partition(id),
                ..
            } if !self.state.is_held() => {
                let _ = self.take_recovery_read(*request, *id);
                Ok(Vec::new())
            }

            _ if !self.state.is_held() => Ok(Vec::new()),

            ControlEvent::Watched {
                prefix,
                cursor,
                changes,
            } => Ok(self.on_watched(event, *prefix, cursor.revision, changes)),

            // A liveness watermark. It carries no authority and no record content, so it moves
            // the cursor and nothing else — and specifically it does not reload.
            ControlEvent::WatchProgress { prefix, revision } => {
                self.watch_refused_attempts = 0;
                self.cursors.insert(*prefix, *revision);
                Ok(Vec::new())
            }

            ControlEvent::WatchTerminated {
                prefix,
                from,
                termination,
            } => Ok(self.on_terminated(ctx, event, *prefix, *from, *termination)),

            ControlEvent::FamilySnapshot {
                prefix,
                snapshot_revision,
                records,
            } => Ok(self.on_family_snapshot(ctx, event, *prefix, *snapshot_revision, records)),

            // This node's serving rights first, then the takeover table (T1, T2, T7).
            ControlEvent::Value {
                request,
                key: ControlKey::Partition(id),
                outcome,
            } => {
                let recovery = self.take_recovery_read(*request, *id);
                let mut effects = self.on_partition_read(ctx, event, *id, outcome, recovery)?;
                effects.extend(self.observe_read(event, ctx.node, *id, outcome));
                Ok(effects)
            }

            // Another node's grant: a takeover's prior owner, T3–T6 (`design.md` §2.6a).
            ControlEvent::Value {
                request,
                key: ControlKey::Grant(node),
                outcome,
            } => Ok(self.on_prior_grant_read(event, *request, *node, outcome)),

            // A record read answered for a family A1 does not serve from: the cluster schema, a
            // node record, a route, an operation, or the planner's own grant. None of them widens
            // or narrows this node's rights.
            ControlEvent::Value { .. } => Ok(vec![Self::ignored(
                event,
                AuthorityIgnoreReason::StaleAuthorityView,
            )]),
        }
    }

    // ---- the activation side: `Takeover` and `FenceProven` (`design.md` §2.6a) ---------------
    //
    // Runs only while `Held` (§2.6a scope rule): `on_control` routes nothing else otherwise, the
    // sweep returns at once, and `ExternalFenceVerified` answers `TakeoverDeferred`. The table
    // lives on the kernel, so an entry outlives a fence and proof resumes after re-adoption.

    /// An effect about partition `p` rather than the event's partition. A proof, and the deferral
    /// it replaces, are routed to the partition taken over.
    fn about(mut effect: Effect, partition: PartitionId) -> Effect {
        effect.partition = partition;
        effect
    }

    /// `Ignored(TakeoverDeferred)` about `p`. The step removes it again if the sweep proves `p`
    /// in the same step (§2.6a T3, T6: "unless the sweep proves").
    fn deferred(event: &Event, partition: PartitionId) -> Effect {
        Self::about(
            Self::ignored(event, AuthorityIgnoreReason::TakeoverDeferred),
            partition,
        )
    }

    /// Forget every outstanding T1 read for `p`.
    fn drop_pending(&mut self, partition: PartitionId) {
        self.takeover_reads.retain(|_, outstanding| {
            outstanding.pending.remove(&partition);
            !outstanding.pending.is_empty()
        });
    }

    /// T2 for a record that is absent: drop the entry unless it is proven, and any pending read.
    fn observe_absent(&mut self, partition: PartitionId) {
        if self
            .takeover
            .get(&partition)
            .is_some_and(|entry| entry.proven.is_none())
        {
            self.takeover.remove(&partition);
        }
        self.drop_pending(partition);
    }

    /// T1, T2 and T7 for one `partitions/{p}` record read at `revision` (§2.6a, A-R51).
    ///
    /// * Ours: the takeover is over. Drop the entry, proven or not (T2).
    /// * Someone else's and [`partition::PartitionLifecycle::Serving`]: no transfer in progress.
    ///   Drop a non-proven entry (A-R51's T2 amendment) and start nothing. This is the guard that
    ///   stops a node-scoped freeze from handing out a proof for every partition of the node.
    /// * Someone else's, not serving, same lineage as the entry: T7 when drained.
    /// * Someone else's, not serving, no entry for this lineage: T1, one `grants/{owner}` read.
    fn observe_partition(
        &mut self,
        event: &Event,
        us: NodeId,
        record: &PartitionRecord,
        revision: Revision,
    ) -> Vec<Effect> {
        let partition = record.partition;
        if record.owner == us {
            self.takeover.remove(&partition);
            self.drop_pending(partition);
            return Vec::new();
        }
        if record.lifecycle == PartitionLifecycle::Serving {
            self.observe_absent(partition);
            return Vec::new();
        }
        let drained = record.lifecycle == PartitionLifecycle::FencingDrained;
        if let Some(entry) = self.takeover.get_mut(&partition) {
            if entry.prior_generation == record.generation
                && entry.prior_owner_epoch == record.owner_epoch
            {
                if !drained {
                    return Vec::new();
                }
                // T7, and its repeat on a proven entry.
                if entry.proven.is_some() {
                    return vec![Self::about(
                        Self::ignored(event, AuthorityIgnoreReason::TakeoverAlreadyAuthorized),
                        partition,
                    )];
                }
                entry.revoked_at.get_or_insert(revision);
                if entry.frozen.is_none() {
                    // Frozen before drained (M7A-57): read the grant; the sweep proves once the
                    // read finds it frozen.
                    let prior = entry.prior_node;
                    return vec![self.get(event, ControlKey::Grant(prior))];
                }
                return Vec::new();
            }
            // T1: a stale entry for another lineage is dropped, proven or not — its proof was
            // for a different owner epoch.
            self.takeover.remove(&partition);
        }
        // A read already outstanding for this lineage answers for it: record a drain, issue
        // nothing.
        for outstanding in self.takeover_reads.values_mut() {
            if let Some(read) = outstanding.pending.get_mut(&partition) {
                if read.generation == record.generation && read.owner_epoch == record.owner_epoch {
                    if drained {
                        read.drained_at.get_or_insert(revision);
                    }
                    return Vec::new();
                }
            }
        }
        self.drop_pending(partition);
        let read = PendingTakeover {
            generation: record.generation,
            owner_epoch: record.owner_epoch,
            drained_at: drained.then_some(revision),
        };
        // One read per owner per step: a partition this step already asked that owner about
        // joins that read.
        let key = (event.correlation, record.owner);
        if let Some(outstanding) = self.takeover_reads.get_mut(&key) {
            outstanding.pending.insert(partition, read);
            return Vec::new();
        }
        let request = self.request();
        self.takeover_reads.insert(
            key,
            TakeoverRead {
                request,
                pending: BTreeMap::from([(partition, read)]),
            },
        );
        vec![Self::control(
            event,
            ControlEffect::Get {
                request,
                key: ControlKey::Grant(record.owner),
            },
        )]
    }

    /// T1/T2/T7 for a single linearizable read of `partitions/{id}`, only when it is newer than
    /// the A-R52 mark. Found is observed at the record's revision, Absent at `as_of`. A body
    /// naming another partition than its key still moves the mark — the key was read at that
    /// revision — but starts nothing, as in [`Self::observe_snapshot`].
    fn observe_read(
        &mut self,
        event: &Event,
        us: NodeId,
        id: PartitionId,
        outcome: &ReadOutcome,
    ) -> Vec<Effect> {
        let revision = match outcome {
            ReadOutcome::Found { revision, .. } => *revision,
            ReadOutcome::Absent { as_of } => *as_of,
            ReadOutcome::Unavailable => return Vec::new(),
        };
        if !self.takeover_marks.advance(id, revision) {
            return Vec::new();
        }
        match outcome {
            ReadOutcome::Found { value, .. } => match PartitionRecord::decode(value) {
                Some(record) if record.partition == id => {
                    self.observe_partition(event, us, &record, revision)
                }
                _ => Vec::new(),
            },
            _ => {
                self.observe_absent(id);
                Vec::new()
            }
        }
    }

    /// T1/T2/T7 over a coherent snapshot at `snapshot_revision`, in [`PartitionId`] order. A
    /// partition the snapshot does not list is absent (T2). Only partitions whose A-R52 mark the
    /// snapshot is newer than are observed; then every mark rises to the snapshot.
    fn observe_snapshot(
        &mut self,
        event: &Event,
        us: NodeId,
        snapshot_revision: Revision,
        records: &[ControlRecord],
    ) -> Vec<Effect> {
        let mut listed: BTreeMap<PartitionId, (PartitionRecord, Revision)> = BTreeMap::new();
        for record in records {
            if let ControlKey::Partition(id) = record.key {
                if let Some(body) = PartitionRecord::decode(&record.value) {
                    if body.partition == id {
                        listed.insert(id, (body, record.revision));
                    }
                }
            }
        }
        let unlisted: Vec<PartitionId> = self
            .takeover
            .keys()
            .chain(
                self.takeover_reads
                    .values()
                    .flat_map(|outstanding| outstanding.pending.keys()),
            )
            .filter(|id| !listed.contains_key(id))
            .filter(|id| self.takeover_marks.is_newer(**id, snapshot_revision))
            .copied()
            .collect();
        listed.retain(|id, _| self.takeover_marks.is_newer(*id, snapshot_revision));
        self.takeover_marks.raise_floor(snapshot_revision);
        for id in unlisted {
            self.observe_absent(id);
        }
        let mut effects = Vec::new();
        for (record, revision) in listed.values() {
            effects.extend(self.observe_partition(event, us, record, *revision));
        }
        effects
    }

    /// A `grants/{n}` read for another node answered: T3–T5 when it is one of T1's reads, T6
    /// otherwise.
    fn on_prior_grant_read(
        &mut self,
        event: &Event,
        request: ControlRequestId,
        node: NodeId,
        outcome: &ReadOutcome,
    ) -> Vec<Effect> {
        // A body naming another node than its key is not that node's grant, as a partition body
        // naming another partition is not that partition's record (finding C, A-R53.3).
        let decoded = match outcome {
            ReadOutcome::Found { revision, value } => GrantRecord::decode(value)
                .filter(|record| record.node == node)
                .map(|record| (record, *revision)),
            _ => None,
        };
        // T1's read, matched by the request id its answer echoes and never by key: after T5 the
        // key is the same and only the id tells the re-read's answer from the first read's.
        let answered = self
            .takeover_reads
            .iter()
            .find(|((_, owner), outstanding)| *owner == node && outstanding.request == request)
            .map(|(key, _)| *key);
        if let Some(key) = answered {
            let Some(TakeoverRead { pending, .. }) = self.takeover_reads.remove(&key) else {
                return Vec::new();
            };
            return match outcome {
                // T5: unavailable → re-read (ADR-rdb-0007 §3), as a fresh request under the same
                // key; the first read's id no longer matches.
                ReadOutcome::Unavailable => {
                    let request = self.request();
                    self.takeover_reads
                        .insert(key, TakeoverRead { request, pending });
                    vec![Self::control(
                        event,
                        ControlEffect::Get {
                            request,
                            key: ControlKey::Grant(node),
                        },
                    )]
                }
                // T4: no grant to take over from. The next partitions read retries.
                ReadOutcome::Absent { .. } => pending
                    .keys()
                    .map(|partition| Self::deferred(event, *partition))
                    .collect(),
                ReadOutcome::Found { .. } => {
                    let Some((record, revision)) = decoded else {
                        return vec![Self::ignored(event, AuthorityIgnoreReason::FamilyRejected)];
                    };
                    // T3: create each entry whole (A-R31).
                    let frozen = record.frozen.then_some(FrozenGrant {
                        expiry_utc_ms: record.expiry_utc_ms,
                        control_revision: revision,
                    });
                    let mut effects = Vec::new();
                    for (partition, read) in pending {
                        self.takeover.entry(partition).or_insert(Takeover {
                            prior_node: node,
                            prior_generation: read.generation,
                            prior_owner_epoch: read.owner_epoch,
                            prior_grant_id: record.grant,
                            prior_boot_id: record.boot,
                            frozen,
                            observed_revision: revision,
                            revoked_at: read.drained_at,
                            proven: None,
                        });
                        effects.push(Self::deferred(event, partition));
                    }
                    effects
                }
            };
        }
        // T6: an uncorrelated read of a prior owner's grant — the grants watch turned into a
        // `Get`. It refreshes every unproven entry it is strictly newer than (A-R53.2).
        if matches!(outcome, ReadOutcome::Found { .. }) && decoded.is_none() {
            return vec![Self::ignored(event, AuthorityIgnoreReason::FamilyRejected)];
        }
        let mut effects = Vec::new();
        if let Some((record, revision)) = decoded {
            for (partition, entry) in &mut self.takeover {
                let newer = entry.observed_revision < revision;
                if entry.prior_node == node && entry.proven.is_none() && newer {
                    entry.prior_grant_id = record.grant;
                    entry.prior_boot_id = record.boot;
                    entry.frozen = record.frozen.then_some(FrozenGrant {
                        expiry_utc_ms: record.expiry_utc_ms,
                        control_revision: revision,
                    });
                    entry.observed_revision = revision;
                    effects.push(Self::deferred(event, *partition));
                }
            }
        }
        if effects.is_empty() {
            // Another node's grant that no takeover is waiting on. It neither widens nor narrows
            // this node's rights.
            effects.push(Self::ignored(
                event,
                AuthorityIgnoreReason::StaleAuthorityView,
            ));
        }
        effects
    }

    /// T11–T13: an external fence claim, bound against the takeover table rather than trusted
    /// (finding K-A-16). Checks run in [`ExternalFenceMismatch`]'s order; the first failure wins.
    fn on_external_fence(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Vec<Effect> {
        let EventKind::ExternalFenceVerified {
            partition,
            prior_generation,
            prior_owner_epoch,
            prior_boot_id,
            control_revision,
            ..
        } = &event.kind
        else {
            return Vec::new();
        };
        let partition = *partition;
        let refuse = |mismatch| {
            vec![Self::about(
                Self::ignored(
                    event,
                    AuthorityIgnoreReason::ExternalFenceRejected { mismatch },
                ),
                partition,
            )]
        };
        if !self.state.is_held() {
            return vec![Self::deferred(event, partition)];
        }
        let Some(entry) = self.takeover.get_mut(&partition) else {
            return refuse(ExternalFenceMismatch::Partition);
        };
        let Some(frozen) = entry.frozen else {
            return refuse(ExternalFenceMismatch::NotFrozen);
        };
        if entry.prior_generation != *prior_generation {
            return refuse(ExternalFenceMismatch::PriorGeneration);
        }
        if entry.prior_owner_epoch != *prior_owner_epoch {
            return refuse(ExternalFenceMismatch::PriorOwnerEpoch);
        }
        if entry.prior_boot_id != *prior_boot_id {
            return refuse(ExternalFenceMismatch::PriorBootId);
        }
        if frozen.control_revision != *control_revision {
            return refuse(ExternalFenceMismatch::ControlRevision);
        }
        if entry.proven.is_some() {
            return vec![Self::about(
                Self::ignored(event, AuthorityIgnoreReason::TakeoverAlreadyAuthorized),
                partition,
            )];
        }
        let Some(revocation) = Revocation::from_external_fence_verified(&event.kind) else {
            return Vec::new();
        };
        entry.proven = Some(revocation.clone());
        let proof = Self::proof(partition, entry, frozen, revocation, ctx.now);
        vec![Self::about(
            Self::authority(event, AuthorityEffect::FenceProven(proof)),
            partition,
        )]
    }

    /// T8–T10, the last thing every `Held` step does: prove what can be proven now.
    ///
    /// Per entry in [`PartitionId`] order, unproven and frozen: `DurableDrain` when a drained
    /// record was seen (T8), otherwise `ExpiryProven` when the inequality holds on an admissible
    /// sample (T9), otherwise nothing (T10). The sweep is not an answer to the event, so an entry
    /// that cannot be proven yet emits nothing: an absent proof is a stable state, not a timeout
    /// (§2.6 rule 3). No timer: `Renew` keeps a held node stepping (§2.6a blocker 4, A-R27).
    fn sweep(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Vec<Effect> {
        if !self.state.is_held() {
            return Vec::new();
        }
        let clock = &self.clock;
        let mut effects = Vec::new();
        for (partition, entry) in &mut self.takeover {
            if entry.proven.is_some() {
                continue;
            }
            let Some(frozen) = entry.frozen else {
                continue;
            };
            let revocation = match entry.revoked_at {
                Some(ack_revision) => Revocation::DurableDrain { ack_revision },
                None => match expiry_proven(clock, frozen.expiry_utc_ms, ctx.now, ctx.budgets) {
                    Some(revocation) => revocation,
                    None => continue,
                },
            };
            entry.proven = Some(revocation.clone());
            let proof = Self::proof(*partition, entry, frozen, revocation, ctx.now);
            effects.push(Self::about(
                Self::authority(event, AuthorityEffect::FenceProven(proof)),
                *partition,
            ));
        }
        effects
    }

    /// Drop each `Ignored(TakeoverDeferred)` about a partition the sweep proved in this same step
    /// (§2.6a T3, T6: "unless the sweep proves"). A step says one thing about a partition.
    fn supersede_deferrals(effects: &mut Vec<Effect>, proofs: &[Effect]) {
        let deferral = EffectKind::Kernel(KernelEffect::Ignored {
            reason: KernelIgnoredReason::Authority(AuthorityIgnoreReason::TakeoverDeferred),
        });
        effects.retain(|effect| {
            effect.kind != deferral || proofs.iter().all(|p| p.partition != effect.partition)
        });
    }

    /// The proof for `entry`. `control_revision` is always the frozen read's, and
    /// `decision_tick` is now (§2.6a).
    const fn proof(
        partition: PartitionId,
        entry: &Takeover,
        frozen: FrozenGrant,
        revocation: Revocation,
        now: Tick,
    ) -> FencingProof {
        FencingProof {
            partition,
            prior_generation: entry.prior_generation,
            prior_owner_epoch: entry.prior_owner_epoch,
            prior_grant_id: entry.prior_grant_id,
            prior_boot_id: entry.prior_boot_id,
            revocation,
            control_revision: frozen.control_revision,
            decision_tick: now,
        }
    }

    // ---- the local seams -------------------------------------------------------------------

    /// One of A1's own timers fired (lead ruling A-R27: there is no tick event; A1 arms these).
    ///
    /// # The wake does not own the admission decision
    ///
    /// [`Self::revalidate`] has already run for this step, so both conjuncts have been judged and
    /// any fence they require has already been emitted before this is reached. What is left here
    /// is what is specific to *being woken*: re-arming, and reporting a suspension that no other
    /// event would have mentioned.
    ///
    /// # Staleness
    ///
    /// A firing whose [`TimerVersion`] is below the version its kind is currently armed under is
    /// a wake A1 has already superseded, and is ignored with
    /// [`AuthorityIgnoreReason::StaleTimer`] — before anything else, including before the kind is
    /// consulted, so a stale wake cannot re-arm a timer.
    ///
    /// # Errors
    ///
    /// `Unavailable` for a timer id outside A1's reserved block.
    fn on_timer(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        fired: TimerFired,
    ) -> Result<Vec<Effect>, RdbError> {
        let Some(kind) = self.timer_kind(fired.id) else {
            return Err(RdbError::unavailable(
                Capability::Authority,
                "authority: Timer | the id is outside A1's reserved block (AUTHORITY_TIMER_BASE)",
            ));
        };
        if fired.version.0 < self.timer_version(kind).0 {
            return Ok(vec![Self::ignored(
                event,
                AuthorityIgnoreReason::StaleTimer,
            )]);
        }
        // `Fenced` is terminal. No timeout, no retry, no "probably fine now" — and specifically
        // no re-arm, so a fenced node's timers go quiet rather than waking it for ever.
        if self.state.is_fenced() {
            return Ok(vec![Self::ignored(
                event,
                AuthorityIgnoreReason::StaleTimer,
            )]);
        }
        match kind {
            AuthorityTimer::ClockWake => {
                let mut effects = Vec::new();
                // The one row that belongs to the wake rather than to `revalidate`: a held grant
                // whose sample has aged out denies every check without fencing (lead ruling
                // A-R12), and that is a state nothing else in the effect vector reports.
                let suspended = self.state.held().is_some_and(|held| {
                    utc_ok(&self.clock, held.expiry_utc_ms, ctx.now, ctx.budgets)
                        == Err(DenyReason::ClockSampleStale)
                });
                if suspended {
                    effects.push(Self::ignored(
                        event,
                        AuthorityIgnoreReason::AdmissionSuspended,
                    ));
                }
                let at = ctx.now.plus_millis(ctx.budgets.clock_sample_period_millis);
                effects.push(self.arm(event, AuthorityTimer::ClockWake, at));
                Ok(effects)
            }
            AuthorityTimer::Renew if self.state.is_held() => Ok(self.on_renew_due(ctx, event)),
            AuthorityTimer::Acquire if self.state.is_unheld() => {
                Ok(self.on_acquire_due(ctx, event))
            }
            // A renewal wake with no grant, or an acquisition wake while one is held: the state
            // moved under a timer that was armed for the other one.
            AuthorityTimer::Renew | AuthorityTimer::Acquire => Ok(vec![Self::ignored(
                event,
                AuthorityIgnoreReason::StaleTimer,
            )]),
            // The capacity back-off elapsed. Re-watch every family that was refused, from the
            // revision it was consumed to — a resume, never a reload: a capacity error is not a
            // gap, so there is nothing to re-read.
            //
            // The pending set is drained, so a second firing of the same wake re-watches
            // nothing. That is deliberate and not a missing guard: the version check above
            // already rejects a *stale* firing, and a fixture that fires the current version
            // twice has asked for one re-arm, not two.
            AuthorityTimer::WatchBackoff => {
                let pending = std::mem::take(&mut self.watch_backoff);
                if pending.is_empty() {
                    return Ok(vec![Self::ignored(
                        event,
                        AuthorityIgnoreReason::StaleTimer,
                    )]);
                }
                Ok(pending
                    .into_iter()
                    .map(|(prefix, from)| {
                        Self::control(event, ControlEffect::Watch { prefix, from })
                    })
                    .collect())
            }
        }
    }

    /// The process was suspended, resumed or restarted.
    ///
    /// Two of ADR-rdb-0007 §3's triggers, and they are here rather than inferred from a clock
    /// because a node cannot notice a suspension by looking at a clock it is not allowed to read.
    fn on_lifecycle(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        lifecycle: NodeLifecycle,
    ) -> Vec<Effect> {
        let Some(held) = self.state.held() else {
            return vec![Self::ignored(
                event,
                AuthorityIgnoreReason::StaleAuthorityView,
            )];
        };
        match lifecycle {
            NodeLifecycle::Resumed { suspended_millis } => {
                if suspended_millis > ctx.budgets.resume_gap_tolerance_millis {
                    self.fence(ctx, event, FenceScope::Node, DenyReason::ProcessSuspended)
                } else {
                    // The near-miss twin of the fence above. Lead ruling A-R43: this emitted
                    // `StaleTimer`, which is about a timer version and has nothing to do with a
                    // resume gap — and it carried no marker, so a row written against it would
                    // have looked right.
                    vec![Self::ignored(
                        event,
                        AuthorityIgnoreReason::ResumeGapWithinTolerance,
                    )]
                }
            }
            NodeLifecycle::Rebooted { boot } => {
                if boot == held.boot {
                    // The near-miss twin of `BootMismatch`. `StaleTimer` stood in here for the
                    // same non-reason as above (lead ruling A-R43).
                    vec![Self::ignored(event, AuthorityIgnoreReason::BootUnchanged)]
                } else {
                    self.fence(ctx, event, FenceScope::Node, DenyReason::BootMismatch)
                }
            }
        }
    }

    /// Local storage reported back.
    ///
    /// One of the two partition-scoped fences. Spec §5.2 step 3 says a local storage failure
    /// fences the partition, without qualification, and
    /// [`StorageFault::WriteFailed`]'s own doc says the same — so the grant stays held and every
    /// other partition keeps serving (ADR-rdb-0007 §3, Scope column).
    ///
    /// # Errors
    ///
    /// The other four [`StorageEvent`] variants are T1's and L1's completions, not A1's, and they
    /// report `Unavailable` naming the seam rather than being silently ignored here.
    fn on_storage(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        storage: &StorageEvent,
    ) -> Result<Vec<Effect>, RdbError> {
        let StorageEvent::CommitFailed { fault, .. } = storage else {
            return Err(RdbError::unavailable(
                Capability::Authority,
                "authority: Storage completions other than CommitFailed are not A1's seam",
            ));
        };
        if !matches!(fault, StorageFault::WriteFailed) {
            return Ok(vec![Self::ignored(
                event,
                AuthorityIgnoreReason::StaleAuthorityView,
            )]);
        }
        if !self.state.is_held() {
            return Ok(vec![Self::ignored(
                event,
                AuthorityIgnoreReason::StaleAuthorityView,
            )]);
        }
        let partition = event.partition;
        let effects = self.fence(
            ctx,
            event,
            FenceScope::Partition(partition),
            DenyReason::LocalStorageFenced,
        );
        if let AuthorityState::Held(held) = &mut self.state {
            held.storage_fenced.insert(partition);
        }
        Ok(effects)
    }

    /// One of A1's own events, delivered on kernel-a's arm (lead ruling A-R25).
    ///
    /// # Errors
    ///
    /// [`AuthorityEvent::Answer`] is A1's own output arriving back at it, which only happens if a
    /// dispatcher routes it wrongly; it reports `Unavailable` naming that rather than pretending
    /// to consume it.
    fn on_authority_event(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        authority: &AuthorityEvent,
    ) -> Result<Vec<Effect>, RdbError> {
        match authority {
            // The synchronous checkpoint is `may_admit`; these three cross a module boundary, so
            // the answer is an effect (lead ruling A-R29).
            AuthorityEvent::Check {
                checkpoint,
                lineage,
                correlation,
            } => Ok(vec![Self::authority(
                event,
                AuthorityEffect::Answer(self.decide(ctx, *checkpoint, *lineage, *correlation)),
            )]),
            // A request, not a fact: the revocation counts only once it is durable, and the
            // fence fires on the completion below and not here (spec §7.3 step 2, ruling A-R26).
            AuthorityEvent::RevokeEpochRequested { partition, epoch } => Ok(vec![Self::effect(
                event,
                EffectKind::Store(StoreEffect::PersistEpochRevocation {
                    partition: *partition,
                    epoch: *epoch,
                }),
            )]),
            // The second partition-scoped fence. The grant stays held; this partition's epoch is
            // unserveable for ever, even across a restart.
            //
            // Recorded in **every** state (lead ledger L-R178e): the write is durable whether or
            // not a grant is held, and the next grant must not start without it. The fence and
            // the drain proof are still `Held`-only: with no grant there is nothing to fence and
            // no view to supersede, and the note says the fence was withheld, not the record.
            AuthorityEvent::EpochRevocationPersisted { partition, epoch } => {
                self.revoked_epochs.insert((*partition, *epoch));
                if !self.state.is_held() {
                    return Ok(vec![Self::ignored(
                        event,
                        AuthorityIgnoreReason::StaleAuthorityView,
                    )]);
                }
                let mut effects = self.revoke_served(ctx, event, *partition);
                effects.push(Self::authority(
                    event,
                    AuthorityEffect::Fact(AuthorityFact::DrainProof),
                ));
                Ok(effects)
            }
            // The start-of-process read-back (lead ledger L-R178e): the host replays each durable
            // revocation before the first `AcquireDue`, so this lands on a fresh `Unheld` kernel
            // and records only. No `DrainProof`, ever: a restore completes no request. A `Held`
            // kernel only meets one from a host that broke that order, and then a restore naming
            // the served epoch fences it exactly as a completion does, so the check and the
            // standing view never disagree. Any other restore fences nothing.
            AuthorityEvent::EpochRevocationRestored { partition, epoch } => {
                self.revoked_epochs.insert((*partition, *epoch));
                let serves_it = self
                    .state
                    .held()
                    .and_then(|held| held.served.get(partition))
                    .is_some_and(|served| served.owner_epoch == *epoch);
                if !serves_it {
                    return Ok(Vec::new());
                }
                Ok(self.revoke_served(ctx, event, *partition))
            }
            _ => Err(RdbError::unavailable(
                Capability::Authority,
                "authority: AuthorityEvent::Answer is A1's own output, not an input",
            )),
        }
    }

    /// Fence `partition` as `EpochRevoked` and remove it from `served`, keeping a tombstone. The
    /// shared half of the completion and restore rows; the caller has already recorded the pair.
    fn revoke_served(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        partition: PartitionId,
    ) -> Vec<Effect> {
        let effects = self.fence(
            ctx,
            event,
            FenceScope::Partition(partition),
            DenyReason::EpochRevoked,
        );
        if let AuthorityState::Held(held) = &mut self.state {
            // A local disk fact carries no control revision, so the tombstone is the removed
            // entry's install revision, floored at the last snapshot (A-R48b).
            let removed_at = held.partitions_revision;
            held.remove_served(partition, removed_at);
        }
        effects
    }

    /// Build the decision one checkpoint answers with.
    ///
    /// Carries `authority_seq` (finding K-A-34) so a later checkpoint can prove the lineage did
    /// not move between two decisions without holding the views themselves.
    fn decide(
        &self,
        ctx: &StepCtx<'_>,
        checkpoint: Checkpoint,
        lineage: Lineage,
        correlation: CorrelationId,
    ) -> AuthorityDecision {
        let held = self.state.held();
        AuthorityDecision {
            owner: held.map_or(ctx.node, |held| held.node),
            boot: held.map_or(ctx.boot, |held| held.boot),
            grant: held.map_or_else(GrantId::default, |held| held.grant),
            authority_generation: held.map_or_else(AuthorityGeneration::default, |held| {
                held.authority_generation
            }),
            lineage,
            expiry_utc_ms: held.map_or(0, |held| held.expiry_utc_ms),
            decided_at: ctx.now,
            authority_seq: self.authority_seq,
            checkpoint,
            correlation,
            // `OutboxDispatch` is declared and unused in M7 (`design.md` §2.5; M7A-61). Its
            // variant cannot be feature-gated from here (§13 Q-7), so it never admits.
            verdict: if checkpoint == Checkpoint::OutboxDispatch {
                Verdict::Deny(DenyReason::ControlUnavailable)
            } else {
                self.may_admit(ctx, lineage)
            },
        }
    }

    /// Name the transitions this build does not have, before any state moves.
    ///
    /// A kind-level refusal only. The state-dependent ones live in their own handlers, because
    /// "this row is not built" is sometimes a question about the state and not about the event.
    fn supported(kind: &EventKind) -> Result<(), RdbError> {
        let reason = match kind {
            EventKind::Control(_)
            | EventKind::Timer(_)
            | EventKind::Node(_)
            | EventKind::Storage(_)
            | EventKind::ExternalFenceVerified { .. }
            | EventKind::Kernel(KernelEvent::Authority(_) | KernelEvent::Recovered(_)) => {
                return Ok(())
            }
            EventKind::Client(_) => "authority: client requests reach T1 and P1, never A1",
            EventKind::Transport(_) => "authority: transport frames reach R1, never A1",
            // Kernel-a never destructures kernel-b's facts; it matches the arm or it does not.
            EventKind::Kernel(_) => "authority: kernel-b's facts are not A1's seam",
        };
        Err(RdbError::unavailable(Capability::Authority, reason))
    }
}

impl Module for Authority {
    fn name(&self) -> ModuleName {
        ModuleName::Authority
    }

    fn capability(&self) -> CapabilityState {
        // `Wired` (lead rulings A-R80a, V-R37, V-R38). A1's advertised capability is the four gates
        // over a grant it acquired and a lineage it installed, and the campaign now exercises
        // that end to end. The evidence is the authored F1/T1 cases in `rdb-sim`'s
        // `tests/support/scenarios/cases.rs` (`case_f1_t1_p1_retained_status_24h`,
        // `case_f1_t1_digest_across_recovery`): survivors with a prefix go through F1's
        // recovery and the resume hold, then a client submits. A1 installs the recovered
        // lineage, acquires a grant, and answers each write's `Dispatch`, `Publication` and
        // `Reply` checks `Valid` for that grant. INV-AUTH, which needs A1 alone, arms on those
        // runs and holds (`tests/campaign.rs`, M7V-89). Reporting `Unavailable` here would
        // tell the campaign no such run exists.
        //
        // What that `proven` covers (ruling V-R41): only the paths the default corpus reaches,
        // which is A1's healthy path — every decision it records is `Valid`, inside a held
        // grant, in one generation. No corpus history reaches a NoGrant, Expired, Fenced or
        // lineage denial: a submit before the grant is refused by T1's admission before A1 is
        // asked, and no op the bridge lowers lapses a lease, fences, or moves a lineage.
        // Denial, fence and lineage are covered by `rdb-core` unit rows instead: NoGrant by
        // `m7a_03_acquire_cas_unknown_reads_no_rights`; fence by
        // `m7a_36_tick_local_window_lapsed_fences_expired_with_renewal_outstanding` and
        // `m7a_50_fence_scope_table_seven_node_two_partition`; lineage by
        // `a_read_moving_a_partition_to_a_withheld_lineage_fences_the_old_one` and
        // `m7a_184`..`m7a_187`; P1's side by
        // `m7a_99_publication_check_deny_quarantines_freezes_authority_lost` and
        // `m7a_100_publication_check_lineage_moved_quarantines`. The corpus member that
        // would reach a denial is the A1/P1 case's generation move, owed under M7V-47/M7V-88.
        CapabilityState::Wired
    }

    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
        Self::supported(&event.kind)?;
        // There is no clock event (lead ruling A-R27), so the sample in the context is absorbed
        // and both admission conjuncts are judged **before** the event is routed: a grant that is
        // over must fence before whatever else arrived on the same step is acted on. This is why
        // any event at all drives the expiry rows, and why a grant is never more than one step
        // past its expiry — see `revalidate`.
        let was_held = self.state.is_held();
        let clock_before = self.clock;
        let mut effects = self.revalidate(ctx, event);
        let fenced_here = was_held && self.state.is_fenced();
        // Views `revalidate` republished because the step's sample was newly accepted
        // (reviewer-ka-rows F2). If the event below is declined they leave with nothing, and
        // the sample they were published for is handed back too (see the `Err` arm), so the
        // next accepted step absorbs it again and republishes them then.
        let republished_here = effects.iter().any(|effect| {
            matches!(
                effect.kind,
                EffectKind::Kernel(KernelEffect::Authority(
                    AuthorityEffect::PublishAuthorityView(_)
                ))
            )
        });
        let routed = match &event.kind {
            EventKind::Control(control) => self.on_control(ctx, event, control),
            EventKind::Timer(fired) => self.on_timer(ctx, event, *fired),
            EventKind::Node(lifecycle) => Ok(self.on_lifecycle(ctx, event, *lifecycle)),
            EventKind::Storage(storage) => self.on_storage(ctx, event, storage),
            EventKind::Kernel(KernelEvent::Authority(authority)) => {
                self.on_authority_event(ctx, event, authority)
            }
            EventKind::ExternalFenceVerified { .. } => Ok(self.on_external_fence(ctx, event)),
            EventKind::Kernel(KernelEvent::Recovered(result)) => {
                Ok(self.on_recovered(event, result))
            }
            // `supported` has already refused every remaining kind.
            _ => Ok(Vec::new()),
        };
        match routed {
            Ok(routed) => effects.extend(routed),
            // A kind A1 takes at the door but has no row for (a storage completion other than
            // `CommitFailed`, a timer outside its block), on a step whose judgement above fenced —
            // any node fence `revalidate` decides, expiry or `ClockUnbounded`.
            // The state is already `Fenced`, so the fence and its superseding views leave with
            // this step; declining would drop them and no consumer would ever hear of the fence
            // (defect KA-ROWS-D1). Nothing else changes: any other decline is still a decline.
            Err(RdbError::Unavailable { .. }) if fenced_here => return Ok(effects),
            // A declined step is a step that did not happen — while held; with no grant, or
            // fenced, there are no views, so a sample the step keeps loses nothing (T4).
            // The views a moved sample
            // republished (reviewer-ka-rows F2) are dropped with it, so the sample goes back
            // too: kept, it would never "move" again and the views would never be republished —
            // every secondary would honour the previous horizon past the moved one. Handed back,
            // the next accepted step absorbs it and republishes. Not `Ok(effects)`: that would
            // publish on every foreign timer that lands with a fresh sample, and the F1/R1
            // keepalive budget (ruling B-R65, `m7v_47`) counts those steps.
            Err(declined) => {
                if republished_here {
                    self.clock = clock_before;
                }
                return Err(declined);
            }
        }
        // The takeover sweep is the last thing a step does (`design.md` §2.6a T8). Not reached on
        // an `Err`: a proof recorded in state but never emitted would make at-most-once zero.
        let proofs = self.sweep(ctx, event);
        Self::supersede_deferrals(&mut effects, &proofs);
        effects.extend(proofs);
        Ok(effects)
    }
}
