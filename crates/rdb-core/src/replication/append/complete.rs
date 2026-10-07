//! The receiver's completion events (design §3.3): storage answering a staged batch or a flush,
//! control announcing a committed recovery, and A1 publishing a newer view.
//!
//! Each `on_*` returns `None` when the event is not this receiver's — a batch it never staged,
//! a flush that names none of its prefixes — so the module declines it rather than inventing
//! an answer. A quarantined receiver still mirrors what storage did, but sends no ACK: its
//! progress is evidence, never qualification.

use crate::contracts::authority::AuthorityView;
use crate::contracts::envelope::{AppendOutcome, AppendReject};
use crate::contracts::errors::ErrorKind;
use crate::contracts::event::EffectKind;
use crate::contracts::ids::{BatchId, DurableSeq, MessageId, NodeId, ReceivedSeq, ReplicaRole};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::storage::DurablePrefix;
use crate::replication::progress::{proved_durable, view_refusal, DigestLookup, HeldView};

use super::{ignored, quarantine_alert, AppendReceiver, Head, HistoryRoot, UNSOLICITED};

impl AppendReceiver {
    /// `Committed`: the staged batch applied. `applied_head` advances, the ladder gains its
    /// rung, and the sender gets the ACK. `durable` does not move (applied is not durable).
    pub fn on_committed(&mut self, batch: BatchId) -> Option<Vec<EffectKind>> {
        let staged = self.staged.filter(|staged| staged.batch == batch)?;
        self.staged = None;
        self.applied_head = staged.head;
        self.history.insert(staged.head.seq, staged.head.digest);
        Some(vec![self.ack_or_withhold(staged.reply_to, staged.request)])
    }

    /// `CommitFailed`: whole batch or none, so the staging slot is dropped — which is the whole
    /// of `accept_head = applied_head` — and the sender is asked to resend from the applied
    /// head. Never quarantines: a local fault is not evidence of divergence. A1 fences the
    /// partition from the same event (ruling A-R25), so R1 raises no second signal.
    ///
    /// A copy quarantined while the batch was in flight answers `Quarantined` instead, as row 0
    /// would: asking for a resend it will refuse is an invitation it cannot honour.
    pub fn on_commit_failed(&mut self, batch: BatchId) -> Option<Vec<EffectKind>> {
        let staged = self.staged.filter(|staged| staged.batch == batch)?;
        self.staged = None;
        self.received = ReceivedSeq(self.applied_head.seq.0);
        let reject = if self.quarantine.is_some() {
            AppendReject::Quarantined
        } else {
            self.need_prefix()
        };
        let outcome = AppendOutcome::Rejected(reject);
        Some(vec![self.send(staged.reply_to, staged.request, outcome)])
    }

    /// `Flushed`: raise `durable` to the highest prefix storage confirmed for this partition in
    /// **this** generation, clamped to the applied head — a flush cannot make durable what was
    /// never applied, so a false durable report moves nothing past it.
    pub fn on_flushed(&mut self, durable: &[DurablePrefix]) -> Option<Vec<EffectKind>> {
        let proved = proved_durable(durable, &self.lineage, self.applied_head.seq)?;
        if proved <= self.durable {
            return Some(vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::NothingOutstanding,
            ))]);
        }
        let primary = self.config.primary()?.node;
        self.durable = proved;
        Some(vec![self.ack_or_withhold(primary, UNSOLICITED)])
    }

    /// `Recovered` (design §3.3): the only event that rewrites a receiver wholesale, and one in a
    /// new generation is the only thing that clears quarantine.
    ///
    /// The root anchor is **looked up, not adopted** (K-B-44). `Differs` at the cutoff
    /// quarantines and moves no head. Otherwise the copy re-anchors on the highest rung it holds
    /// at or below the cutoff — the cutoff pair itself on `Match`, its own older head when it is
    /// behind — and asks the new primary for everything after it.
    ///
    /// One exception keeps a head (M9 S0 D3): F1's re-emit of the generation this copy already
    /// serves, when the copy holds the cutoff (`Match`) and has applied past it, and is neither
    /// quarantined nor retired. Then the copy keeps its applied head, because everything above
    /// the cutoff came from this generation's primary, and asks for what follows it. A
    /// quarantined copy still truncates to the cutoff, since its suffix is the divergence.
    ///
    /// Quarantine is sticky across that re-emit (M9 S0 ruling 2026-10-07, item 3, amending D3
    /// rule 2): the quarantined copy truncates but stays quarantined, asks for nothing, and
    /// answers `AlreadyDiverged`. The primary keeps it diverged too, so it rejoins only through
    /// a `Recovered` in a new generation.
    ///
    /// A pin for another partition, or one naming no primary, is refused and changes nothing.
    /// A pin that names this copy no serving member on this node — the r04 swap makes it the
    /// primary, or drops it — retires it (lead ruling B-R58a, F4): it adopts the new generation,
    /// so the frame fence refuses the old primary's frames `StaleGeneration`, and it sends no
    /// ACK, not even for a batch already staged. Only a later `Recovered` that pins it a serving
    /// member clears that.
    pub fn on_recovered(&mut self, result: &RecoveryResult) -> Vec<EffectKind> {
        let config = &result.committed.pinned_config;
        let invalid = || {
            vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::InvalidConfig,
            ))]
        };
        let primary = config.primary().map(|p| p.node);
        let (Some(primary), true) = (primary, config.partition == self.lineage.partition) else {
            return invalid();
        };
        let own = config
            .member(self.own.copy)
            .filter(|own| own.node == self.own.node && own.role != ReplicaRole::Primary);
        let Some(own) = own.copied() else {
            self.lineage.generation = result.new_generation;
            self.retired = true;
            return invalid();
        };
        let selected = &result.selected;
        let lookup = self
            .history
            .lookup(selected.cutoff_seq, selected.cutoff_digest);
        let keeps_head = lookup == DigestLookup::Match
            && !self.retired
            && self.quarantine.is_none()
            && self.lineage.generation == result.new_generation
            && self.applied_head.seq > selected.cutoff_seq;
        let stays_quarantined = self.quarantine.is_some()
            && !self.retired
            && self.lineage.generation == result.new_generation;
        let anchor = match lookup {
            _ if keeps_head => Some(self.applied_head),
            DigestLookup::Differs { .. } => None,
            DigestLookup::Match | DigestLookup::NotRetained => {
                let Some((seq, digest)) = self.history.at_or_below(selected.cutoff_seq) else {
                    // Nothing held at or below the cutoff to anchor on. Unreachable from a
                    // receiver that started at its partition's root; a handoff question.
                    return vec![ignored(KernelIgnoredReason::Error(
                        ErrorKind::InvalidArgument,
                    ))];
                };
                Some(Head { seq, digest })
            }
        };
        // The rows every arm adopts.
        self.own = own;
        self.retired = false;
        self.lineage.generation = result.new_generation;
        self.adopt_view(&result.committed.authority_view);
        // The pinned configuration is the whole membership; the view carries only its version.
        self.config = config.clone();
        self.root = HistoryRoot {
            floor: selected.cutoff_seq,
            base_digest: selected.cutoff_digest,
            predecessor: Some(result.retained_status_map.predecessor_generation),
        };
        self.last_partition_revision = result.committed.revision;
        let Some(anchor) = anchor else {
            self.quarantine = Some(AppendReject::DivergentHistory {
                at: selected.cutoff_seq,
            });
            return vec![quarantine_alert()];
        };
        // `staged` goes first: nothing applied after this may extend the old lineage.
        self.staged = None;
        self.applied_head = anchor;
        self.history.truncate_above(anchor.seq);
        self.received = ReceivedSeq(anchor.seq.0);
        self.durable = DurableSeq(self.durable.0.min(anchor.seq.0));
        if stays_quarantined {
            return vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::AlreadyDiverged,
            ))];
        }
        self.quarantine = None;
        let outcome = AppendOutcome::Rejected(self.need_prefix());
        vec![self.send(primary, UNSOLICITED, outcome)]
    }

    /// A1's `View` (design §2.2, lead ruling B-R53): the only door, besides `Recovered`, by which
    /// a receiver learns a newer owner epoch or configuration version. An append never opens it:
    /// row 5 still answers a higher epoch `UnknownEpoch` and learns nothing.
    ///
    /// Installs the view when [`view_refusal`] passes it and answers `Recorded`; otherwise
    /// answers the refusal and changes nothing. A quarantined copy still installs: quarantine
    /// withholds its evidence, not what control tells it.
    pub fn on_view(&mut self, view: &AuthorityView) -> Vec<EffectKind> {
        let held = HeldView {
            lineage: self.lineage,
            config_version: self.config.config_version,
            authority_seq: self.authority_seq,
        };
        if let Some(reason) = view_refusal(&held, view) {
            return vec![ignored(KernelIgnoredReason::Replica(reason))];
        }
        self.adopt_view(view);
        vec![ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::Recorded,
        ))]
    }

    /// Take a view's epoch, configuration version and `authority_seq`: the one write both
    /// `View` and `Recovered` make. The generation is the caller's, because a recovered view
    /// names the root it recovered from, not the generation it starts.
    ///
    /// Only the version moves, not the members: a receiver learns members from `Recovered`
    /// alone. Within one generation the primary does not change, so row 6's sender half holds.
    fn adopt_view(&mut self, view: &AuthorityView) {
        self.lineage.owner_epoch = view.lineage.owner_epoch;
        self.config.config_version = view.config_version;
        self.authority_seq = view.authority_seq;
    }

    /// The current ACK to `to`, or — from a quarantined or retired copy — a named refusal to
    /// send one. A retired copy names the generation it adopted (lead ruling B-R58a, F4).
    fn ack_or_withhold(&self, to: NodeId, request: MessageId) -> EffectKind {
        if self.quarantine.is_some() {
            return ignored(KernelIgnoredReason::AppendRejected(
                AppendReject::Quarantined,
            ));
        }
        if self.retired {
            return ignored(KernelIgnoredReason::AppendRejected(
                AppendReject::StaleGeneration {
                    current: self.lineage.generation,
                },
            ));
        }
        self.send(to, request, AppendOutcome::Accepted(self.current_ack()))
    }
}
