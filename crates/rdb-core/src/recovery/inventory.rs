//! Discovery: collecting survivor inventories inside the window (team kernel-b `design.md` §5.2,
//! §5.5).
//!
//! The window opens at the fence's arrival tick, not at `proof.decision_tick` (K-B-14). It is
//! extended only while a source advertising more than the best verified head is still
//! delivering, at most [`MAX_WINDOW_EXTENSIONS`] times. Every source that is not progressing is
//! recorded unavailable on every deadline, and always before `CloseWindow` in the same vector.

use std::collections::{BTreeMap, BTreeSet};

use crate::contracts::authority::FencingProof;
use crate::contracts::digest::Digest;
use crate::contracts::ids::Seq;
use crate::contracts::ignore::ReplicaIgnoreReason;
use crate::contracts::membership::CopyId;
use crate::contracts::recovery::{
    InventoryOutcome, LineageAnchor, RecoveryEffect, SurvivorInventory, UnavailableReason,
};
use crate::contracts::time::Tick;

use super::emit::Emit;
use super::lineage::{verify_ancestry, Rejected, VerifiedInventory};

/// The cap on window extensions: discovery lasts at most 2,000 + 3 x 2,000 ms (`design.md` §5.5).
pub const MAX_WINDOW_EXTENSIONS: u32 = 3;

/// One source still sending its advertised prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransferWatch {
    advertised: Seq,
    received: Seq,
    received_at_last_deadline: Seq,
}

/// Whether discovery is still open, or closed and waiting on probe answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Window {
    /// Inventories are still being collected.
    Open {
        deadline: Tick,
        extensions_used: u32,
    },
    /// Closed; selection asked for these `(copy, seq)` digests. Bounded by `deadline`.
    Probing {
        deadline: Tick,
        outstanding: BTreeSet<(CopyId, Seq)>,
    },
}

impl Window {
    pub(crate) const fn deadline(&self) -> Tick {
        match self {
            Self::Open { deadline, .. } | Self::Probing { deadline, .. } => *deadline,
        }
    }
}

/// What a discovery deadline decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Deadline {
    /// A source is still delivering; the window now ends here.
    Extended(Tick),
    /// Discovery is over; selection runs.
    Closed,
}

/// The `Fenced`/`Collecting` phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Collecting {
    pub(crate) proof: FencingProof,
    pub(crate) window: Window,
    queried: BTreeSet<CopyId>,
    verified: BTreeMap<CopyId, VerifiedInventory>,
    lost: BTreeMap<CopyId, InventoryOutcome>,
    transfers: BTreeMap<CopyId, TransferWatch>,
    highest_advertised: Seq,
}

impl Collecting {
    pub(crate) fn new(proof: FencingProof, queried: BTreeSet<CopyId>, deadline: Tick) -> Self {
        Self {
            proof,
            window: Window::Open {
                deadline,
                extensions_used: 0,
            },
            queried,
            verified: BTreeMap::new(),
            lost: BTreeMap::new(),
            transfers: BTreeMap::new(),
            highest_advertised: Seq(0),
        }
    }

    /// `Fenced` until the first report or transfer arrives.
    pub(crate) fn heard_nothing(&self) -> bool {
        self.verified.is_empty() && self.lost.is_empty() && self.transfers.is_empty()
    }

    /// Every copy that was queried, in copy order.
    pub(crate) fn queried(&self) -> Vec<CopyId> {
        self.queried.iter().copied().collect()
    }

    /// The surviving verified inventories, in copy order.
    pub(crate) fn verified(&self) -> Vec<VerifiedInventory> {
        self.verified.values().cloned().collect()
    }

    /// The verified head of one copy.
    pub(crate) fn head_of(&self, copy: CopyId) -> Option<Seq> {
        self.verified.get(&copy).map(VerifiedInventory::head_seq)
    }

    /// The highest head any live source advertised or reported.
    pub(crate) const fn highest_advertised(&self) -> Seq {
        self.highest_advertised
    }

    /// One outcome per queried copy, in copy order: verified, ineligible or failed.
    pub(crate) fn outcomes(&self) -> Vec<InventoryOutcome> {
        let mut all: BTreeMap<CopyId, InventoryOutcome> = self.lost.clone();
        for copy in self.verified.keys() {
            all.insert(*copy, InventoryOutcome::Verified { copy: *copy });
        }
        all.into_values().collect()
    }

    /// Whether a report from `copy` can still be used: the window is open, and the copy was
    /// queried and not already recorded unavailable. Emits the reason when not.
    fn admits(&self, copy: CopyId, emit: &mut Emit<'_>) -> bool {
        if matches!(self.window, Window::Probing { .. }) {
            emit.ignored(ReplicaIgnoreReason::OutOfPhase);
            return false;
        }
        if !self.queried.contains(&copy) || self.lost.contains_key(&copy) {
            emit.ignored(ReplicaIgnoreReason::NotASource);
            return false;
        }
        true
    }

    /// An inventory arrived. Returns the rejection that ends collection: divergence from the
    /// root, or a newer root that supersedes the plan. An ineligible copy is recorded here and
    /// never returned.
    pub(crate) fn report(
        &mut self,
        root: &LineageAnchor,
        inv: &SurvivorInventory,
        emit: &mut Emit<'_>,
    ) -> Option<Rejected> {
        if !self.admits(inv.copy, emit) {
            return None;
        }
        self.transfers.remove(&inv.copy);
        match verify_ancestry(root, inv) {
            Ok(verified) => {
                self.highest_advertised = self.highest_advertised.max(verified.head_seq());
                self.verified.insert(inv.copy, verified);
                emit.ignored(ReplicaIgnoreReason::Recorded);
                None
            }
            Err(Rejected::Ineligible(reason)) => {
                self.verified.remove(&inv.copy);
                let outcome = InventoryOutcome::Ineligible {
                    copy: inv.copy,
                    reason,
                };
                self.record_lost(inv.copy, outcome, reason, emit);
                None
            }
            Err(rejected) => Some(rejected),
        }
    }

    /// A queried copy could not report.
    pub(crate) fn failed(&mut self, copy: CopyId, emit: &mut Emit<'_>) {
        if self.admits(copy, emit) {
            self.drop_source(copy, emit);
        }
    }

    /// A source is still delivering its advertised prefix.
    pub(crate) fn transfer(
        &mut self,
        copy: CopyId,
        advertised: Seq,
        received: Seq,
        emit: &mut Emit<'_>,
    ) {
        if !self.admits(copy, emit) {
            return;
        }
        if self.verified.contains_key(&copy) {
            // Its inventory already arrived, so it is no longer transferring.
            emit.ignored(ReplicaIgnoreReason::NotASource);
            return;
        }
        let last = self
            .transfers
            .get(&copy)
            .map_or(Seq(0), |watch| watch.received_at_last_deadline);
        self.transfers.insert(
            copy,
            TransferWatch {
                advertised,
                received,
                received_at_last_deadline: last,
            },
        );
        self.highest_advertised = self.highest_advertised.max(advertised);
        emit.ignored(ReplicaIgnoreReason::Recorded);
    }

    /// The open window's deadline (`design.md` §5.5). Records every non-progressing source
    /// first, then extends or closes. On close every queried copy that never answered is
    /// recorded too, and `CloseWindow` is the last effect this pushes.
    pub(crate) fn deadline(
        &mut self,
        now: Tick,
        window_millis: u64,
        emit: &mut Emit<'_>,
    ) -> Deadline {
        let Window::Open {
            extensions_used, ..
        } = self.window
        else {
            return Deadline::Closed;
        };
        let best = self
            .verified
            .values()
            .map(VerifiedInventory::head_seq)
            .max()
            .unwrap_or(Seq(0));
        let (progressing, stalled): (Vec<CopyId>, Vec<CopyId>) = self
            .transfers
            .iter()
            .map(|(copy, t)| {
                (
                    *copy,
                    t.advertised > best && t.received > t.received_at_last_deadline,
                )
            })
            .fold(
                (Vec::new(), Vec::new()),
                |(mut going, mut stuck), (copy, moving)| {
                    if moving {
                        going.push(copy)
                    } else {
                        stuck.push(copy)
                    }
                    (going, stuck)
                },
            );
        for copy in stalled {
            self.drop_source(copy, emit);
        }
        if !progressing.is_empty() && extensions_used < MAX_WINDOW_EXTENSIONS {
            let deadline = now.plus_millis(window_millis);
            for watch in self.transfers.values_mut() {
                watch.received_at_last_deadline = watch.received;
            }
            self.window = Window::Open {
                deadline,
                extensions_used: extensions_used + 1,
            };
            return Deadline::Extended(deadline);
        }
        let silent: Vec<CopyId> = self
            .queried
            .iter()
            .copied()
            .filter(|copy| !self.verified.contains_key(copy) && !self.lost.contains_key(copy))
            .collect();
        for copy in silent {
            self.drop_source(copy, emit);
        }
        emit.recovery(RecoveryEffect::CloseWindow);
        Deadline::Closed
    }

    /// Selection needs these digests: the window becomes a probe wait ending at `deadline`.
    pub(crate) fn start_probing(
        &mut self,
        needed: &[(CopyId, Seq)],
        deadline: Tick,
        emit: &mut Emit<'_>,
    ) {
        for &(copy, seq) in needed {
            emit.recovery(RecoveryEffect::ProbeDigestAt { copy, seq });
        }
        self.window = Window::Probing {
            deadline,
            outstanding: needed.iter().copied().collect(),
        };
    }

    /// A probe was answered. `true` when no probe is outstanding any more.
    pub(crate) fn probe_answered(
        &mut self,
        copy: CopyId,
        seq: Seq,
        digest: Digest,
        emit: &mut Emit<'_>,
    ) -> bool {
        let Window::Probing { outstanding, .. } = &mut self.window else {
            emit.ignored(ReplicaIgnoreReason::OutOfPhase);
            return false;
        };
        if !outstanding.remove(&(copy, seq)) {
            emit.ignored(ReplicaIgnoreReason::NotASource);
            return false;
        }
        let done = outstanding.is_empty();
        if let Some(verified) = self.verified.get_mut(&copy) {
            verified.learn(seq, digest);
        }
        if !done {
            emit.ignored(ReplicaIgnoreReason::Recorded);
        }
        done
    }

    /// A probe cannot be answered: that copy's prefix is lost to this recovery. `true` when no
    /// probe is outstanding any more.
    pub(crate) fn probe_unavailable(
        &mut self,
        copy: CopyId,
        seq: Seq,
        emit: &mut Emit<'_>,
    ) -> bool {
        let Window::Probing { outstanding, .. } = &self.window else {
            emit.ignored(ReplicaIgnoreReason::OutOfPhase);
            return false;
        };
        if !outstanding.contains(&(copy, seq)) {
            emit.ignored(ReplicaIgnoreReason::NotASource);
            return false;
        }
        self.drop_source(copy, emit);
        self.probes_done()
    }

    /// The probe wait ran out: every copy still owing an answer is dropped.
    pub(crate) fn probe_deadline(&mut self, emit: &mut Emit<'_>) {
        let Window::Probing { outstanding, .. } = &self.window else {
            return;
        };
        let owing: BTreeSet<CopyId> = outstanding.iter().map(|(copy, _)| *copy).collect();
        for copy in owing {
            self.drop_source(copy, emit);
        }
    }

    fn probes_done(&self) -> bool {
        match &self.window {
            Window::Probing { outstanding, .. } => outstanding.is_empty(),
            Window::Open { .. } => false,
        }
    }

    /// Record a source as lost to this recovery (`Failed`, `Stalled`) and forget anything it
    /// was owed or owing.
    fn drop_source(&mut self, copy: CopyId, emit: &mut Emit<'_>) {
        let reason = UnavailableReason::Stalled;
        self.verified.remove(&copy);
        self.transfers.remove(&copy);
        if let Window::Probing { outstanding, .. } = &mut self.window {
            outstanding.retain(|(owed, _)| *owed != copy);
        }
        self.record_lost(
            copy,
            InventoryOutcome::Failed { copy, reason },
            reason,
            emit,
        );
    }

    fn record_lost(
        &mut self,
        copy: CopyId,
        outcome: InventoryOutcome,
        reason: UnavailableReason,
        emit: &mut Emit<'_>,
    ) {
        self.lost.insert(copy, outcome);
        emit.recovery(RecoveryEffect::RecordSourceUnavailable { copy, reason });
    }
}
