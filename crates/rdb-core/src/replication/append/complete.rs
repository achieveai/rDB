//! The receiver's completion events (design §3.3): storage answering a staged batch or a flush,
//! and control announcing a committed recovery.
//!
//! Each `on_*` returns `None` when the event is not this receiver's — a batch it never staged,
//! a flush that names none of its prefixes — so the module declines it rather than inventing
//! an answer. A quarantined receiver still mirrors what storage did, but sends no ACK: its
//! progress is evidence, never qualification.

use crate::contracts::envelope::{AppendOutcome, AppendReject};
use crate::contracts::errors::ErrorKind;
use crate::contracts::event::EffectKind;
use crate::contracts::ids::{BatchId, DurableSeq, MessageId, NodeId, ReceivedSeq, ReplicaRole};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::storage::DurablePrefix;
use crate::replication::progress::{proved_durable, DigestLookup};

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

    /// `Recovered` (design §3.3): the only event that rewrites a receiver wholesale and the
    /// only one that clears quarantine.
    ///
    /// The root anchor is **looked up, not adopted** (K-B-44). `Differs` at the cutoff
    /// quarantines and moves no head. Otherwise the copy re-anchors on the highest rung it holds
    /// at or below the cutoff — the cutoff pair itself on `Match`, its own older head when it is
    /// behind — and asks the new primary for everything after it.
    pub fn on_recovered(&mut self, result: &RecoveryResult) -> Vec<EffectKind> {
        let config = &result.committed.pinned_config;
        let own = config
            .member(self.own.copy)
            .filter(|_| config.partition == self.lineage.partition)
            .filter(|own| own.node == self.own.node && own.role != ReplicaRole::Primary);
        let (Some(own), Some(primary)) = (own.copied(), config.primary().map(|p| p.node)) else {
            return vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::InvalidConfig,
            ))];
        };
        let selected = &result.selected;
        let anchor = match self
            .history
            .lookup(selected.cutoff_seq, selected.cutoff_digest)
        {
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
        self.config = config.clone();
        self.lineage.generation = result.new_generation;
        self.lineage.owner_epoch = result.committed.authority_view.lineage.owner_epoch;
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
        self.quarantine = None;
        let outcome = AppendOutcome::Rejected(self.need_prefix());
        vec![self.send(primary, UNSOLICITED, outcome)]
    }

    /// The current ACK to `to`, or — from a quarantined copy — a named refusal to send one.
    fn ack_or_withhold(&self, to: NodeId, request: MessageId) -> EffectKind {
        if self.quarantine.is_some() {
            return ignored(KernelIgnoredReason::AppendRejected(
                AppendReject::Quarantined,
            ));
        }
        self.send(to, request, AppendOutcome::Accepted(self.current_ack()))
    }
}
