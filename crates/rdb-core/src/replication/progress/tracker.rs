//! The primary's `ProgressTracker` (design §3.4): the ACK admission ladder, the divergence
//! vector, and the control events that change what the tracker believes. The derived views of
//! §3.5 are in `views.rs`.
//!
//! # What seeds it
//!
//! A tracker is built by [`ProgressTracker::new`] (fixtures, the manual tester) and rebuilt by a
//! committed `Recovered`. The primary's own `LocalApplied` grows its ladder, `received` and
//! applied head one record at a time (lead ruling B-R47, closing B-R36-Q1). The qualification
//! edge stays at the seed head, the anchor (B-R47a).

use std::collections::BTreeMap;

use crate::contracts::authority::{BlockReason, Lineage};
use crate::contracts::digest::Digest;
use crate::contracts::envelope::{AppendAck, ReplicaProgress};
use crate::contracts::errors::RdbError;
use crate::contracts::event::{EffectKind, KernelEffect};
use crate::contracts::ids::{
    AppliedSeq, BootId, ConfigVersion, DurableSeq, NodeId, PartitionId, ReceivedSeq, ReplicaRole,
    Seq,
};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::membership::{CopyId, Member, PartitionConfig};
use crate::contracts::qualification::{
    QualificationCause, QualificationChanged, QualificationDirection,
};
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::storage::DurablePrefix;
use crate::contracts::time::Tick;
use crate::contracts::trace::AckRejectReason;
use crate::contracts::transport::PeerLabel;
use crate::replication::{ignored, quarantine_alert};

use super::{proved_durable, DigestLadder, DigestLookup};

/// One copy's progress as the primary believes it.
///
/// Keyed by copy id from a pinned configuration, never from an ACK (design §3.4): an ACK from a
/// copy no configuration names has nowhere to be written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyProgress {
    /// The node control placed the copy on.
    pub node: NodeId,
    /// The boot control announced for it. Rule 6 compares against this and nothing else; an ACK
    /// never teaches it a new one (K-B-04).
    pub boot: BootId,
    /// Its role in the newest configuration that names it.
    pub role: ReplicaRole,
    /// Its watermarks as its last admitted ACK stated them. Zero until it proves otherwise.
    pub progress: ReplicaProgress,
}

impl CopyProgress {
    /// A copy that has proved nothing yet.
    const fn fresh(member: &Member) -> Self {
        Self {
            node: member.node,
            boot: member.boot,
            role: member.role,
            progress: ReplicaProgress::EMPTY,
        }
    }
}

/// Everything a tracker starts from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackerInit {
    /// The pinned configuration. `own` must be its primary.
    pub config: PartitionConfig,
    /// This copy.
    pub own: CopyId,
    /// The lineage served.
    pub lineage: Lineage,
    /// The digests the primary can vouch for. Must hold `local.buffered_applied`.
    pub history: DigestLadder,
    /// The primary's own watermarks.
    pub local: ReplicaProgress,
}

/// The primary side of R1 (design §3.4).
///
/// `Clone + PartialEq` on purpose: a dropped ACK asserts `before == after` on the whole value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressTracker {
    lineage: Lineage,
    own: CopyId,
    /// Every active predicate, oldest first; the last is the pinned configuration. A
    /// `ConfigChanged` pushes, a `TransitionBarrierConfirmed` retires (K-B-49).
    predicates: Vec<PartitionConfig>,
    /// One entry per member of any active predicate, the primary's own included.
    peers: BTreeMap<CopyId, CopyProgress>,
    /// Copies proved to hold another history, in the order they were proved. Sticky: only
    /// `Recovered` clears it (K-B-28).
    diverged: Vec<CopyId>,
    history: DigestLadder,
    /// The head the tracker was seeded or rebuilt at: the recovery cutoff. The qualification
    /// edge is `qualifies_now(anchor)` (lead ruling B-R47a), so it means "`min_regular_acks`
    /// regular copies are on our history from the cutoff". A `LocalApplied` never moves it:
    /// evaluated at the moving head the predicate would go false on every write.
    anchor: Seq,
}

/// The two views a step compares before and after itself. Every edge the tracker reports is a
/// difference between two of these, so no edge needs remembered state.
struct Views {
    qualifies: bool,
    durable: Vec<(ConfigVersion, DurableSeq)>,
}

impl ProgressTracker {
    /// Build a tracker.
    ///
    /// # Errors
    ///
    /// [`RdbError::InvalidArgument`] when the configuration is invalid, is for another partition
    /// or does not make `own` its primary, when the watermarks are out of order, or when the
    /// ladder cannot vouch for the primary's own applied head or holds a rung above it.
    pub fn new(init: TrackerInit) -> Result<Self, RdbError> {
        let TrackerInit {
            config,
            own,
            lineage,
            history,
            local,
        } = init;
        config.validate()?;
        let leads = config
            .member(own)
            .is_some_and(|member| member.role == ReplicaRole::Primary);
        let head = Seq(local.buffered_applied.0);
        if config.partition != lineage.partition
            || !leads
            || !ordered(&local)
            || history.digest_at(head).is_none()
            // B-R43 S3-F5: a rung above the head would vouch for a record we do not hold.
            || history.highest() > Some(head)
        {
            return Err(RdbError::InvalidArgument { field: "tracker" });
        }
        Ok(Self::seeded(config, own, lineage, history, local))
    }

    /// Every member of `config` at zero, except the primary at `local`; one predicate; nothing
    /// diverged.
    fn seeded(
        config: PartitionConfig,
        own: CopyId,
        lineage: Lineage,
        history: DigestLadder,
        local: ReplicaProgress,
    ) -> Self {
        let peers = config
            .members
            .iter()
            .map(|member| {
                let mut peer = CopyProgress::fresh(member);
                if member.copy == own {
                    peer.progress = local;
                }
                (member.copy, peer)
            })
            .collect();
        Self {
            lineage,
            own,
            predicates: vec![config],
            peers,
            diverged: Vec::new(),
            history,
            anchor: Seq(local.buffered_applied.0),
        }
    }

    /// The partition served.
    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        self.lineage.partition
    }

    /// The lineage served.
    #[must_use]
    pub const fn lineage(&self) -> Lineage {
        self.lineage
    }

    /// This copy: the pinned configuration's primary.
    #[must_use]
    pub const fn own(&self) -> CopyId {
        self.own
    }

    /// The node this tracker runs on.
    #[must_use]
    pub fn node(&self) -> NodeId {
        self.own_progress().node
    }

    /// The pinned configuration: the newest active predicate.
    #[must_use]
    pub fn config(&self) -> &PartitionConfig {
        self.predicates
            .last()
            .unwrap_or_else(|| unreachable!("a tracker always has its pinned configuration"))
    }

    /// Every active predicate, oldest first.
    #[must_use]
    pub fn predicates(&self) -> &[PartitionConfig] {
        &self.predicates
    }

    /// What the tracker believes about `copy`, if any active predicate names it.
    #[must_use]
    pub fn peer(&self, copy: CopyId) -> Option<&CopyProgress> {
        self.peers.get(&copy)
    }

    /// Whether `copy` was proved to hold another history.
    #[must_use]
    pub fn is_diverged(&self, copy: CopyId) -> bool {
        self.diverged.contains(&copy)
    }

    /// The copies proved diverged, in the order they were proved.
    #[must_use]
    pub fn diverged(&self) -> &[CopyId] {
        &self.diverged
    }

    /// The digests the primary can vouch for.
    #[must_use]
    pub const fn history(&self) -> &DigestLadder {
        &self.history
    }

    /// `LocalApplied` on the primary (lead ruling B-R47): the primary applied `seq`, whose
    /// record digest is `record_digest`. Its own `received`, applied head and ladder grow
    /// together, and only by the next sequence: a gap or a regress stores nothing.
    ///
    /// The primary emits this once per sequence, in order, before it ships the record to any
    /// copy (the contract's ordering rule). So no copy can acknowledge a record the tracker has
    /// not heard of, and rule 7's bound on `received` drops nothing honest.
    ///
    /// It moves no edge: the edge is evaluated at [`Self::anchor`], which a local write never
    /// moves (B-R47a). Lag behind fresh writes is L1's.
    pub fn on_local_applied(&mut self, seq: Seq, record_digest: Digest) -> Vec<EffectKind> {
        let head = self.head();
        if head.0.checked_add(1) != Some(seq.0) {
            return vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::OutOfOrder,
            ))];
        }
        self.history.insert(seq, record_digest);
        let own = self.own;
        if let Some(peer) = self.peers.get_mut(&own) {
            let progress = &mut peer.progress;
            progress.buffered_applied = AppliedSeq(seq.0);
            progress.received = ReceivedSeq(progress.received.0.max(seq.0));
        }
        vec![ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::Recorded,
        ))]
    }

    /// Where the qualification edge is evaluated (lead ruling B-R47a): the head this tracker
    /// was seeded or rebuilt at.
    #[must_use]
    pub const fn anchor(&self) -> Seq {
        self.anchor
    }

    /// The primary's own head: the highest sequence it has applied.
    #[must_use]
    pub fn head(&self) -> Seq {
        Seq(self.own_progress().progress.buffered_applied.0)
    }

    /// The configuration `copy` is held to: the newest active predicate that names it. For a
    /// member of the pinned configuration that is the pinned configuration (design §3.4 rule
    /// 4); a copy only a retiring predicate names keeps acknowledging under that predicate until
    /// the barrier retires it (K-B-49).
    fn pinned_for(&self, copy: CopyId) -> Option<ConfigVersion> {
        self.predicates
            .iter()
            .rev()
            .find(|predicate| predicate.member(copy).is_some())
            .map(|predicate| predicate.config_version)
    }

    fn own_progress(&self) -> &CopyProgress {
        self.peers
            .get(&self.own)
            .unwrap_or_else(|| unreachable!("the pinned configuration names the primary"))
    }

    // ---- the ACK ladder ------------------------------------------------------------------

    /// One `Accepted` reply's ACK from `from` (design §3.4): rules 1–8, then rule 9 against the
    /// primary's own ladder. An admitted ACK advances its copy and emits `PeerProgress`; a
    /// dropped one names its rule and changes nothing.
    pub fn on_ack(&mut self, from: &PeerLabel, ack: &AppendAck, tick: Tick) -> Vec<EffectKind> {
        self.ack_ladder(from, ack, tick).1
    }

    /// [`Self::on_ack`], also naming the copy the ACK advanced when all nine rules passed, and
    /// `None` when it was dropped. Routing hands a catch-up cursor only an ACK named here (lead
    /// ruling B-R48; the cursor trusts what it is given).
    pub fn ack_ladder(
        &mut self,
        from: &PeerLabel,
        ack: &AppendAck,
        tick: Tick,
    ) -> (Option<CopyId>, Vec<EffectKind>) {
        let copy = match self.admit(from, ack) {
            Ok(copy) => copy,
            Err(reason) => {
                return (
                    None,
                    vec![ignored(KernelIgnoredReason::AckRejected(reason))],
                )
            }
        };
        let at = Seq(ack.progress.buffered_applied.0);
        let effects = match self.history.lookup(at, ack.digest_at_buffered) {
            DigestLookup::Match => return (Some(copy), self.advance(copy, ack.progress, tick)),
            DigestLookup::Differs { .. } => self.diverge(copy, true, tick),
            // Absence is not proof (K-B-01): drop the ACK unverified and ask for a snapshot.
            // One request per such ACK: R1 keeps no memory of requests, and the consumer
            // dedups them per copy and barrier (lead ruling B-R48).
            DigestLookup::NotRetained => vec![
                ignored(KernelIgnoredReason::AckRejected(
                    AckRejectReason::Unverifiable,
                )),
                EffectKind::Kernel(KernelEffect::SnapshotCatchupRequired {
                    copy,
                    barrier: self.head(),
                }),
            ],
        };
        (None, effects)
    }

    /// Rule 1's identity half and rule 1d, for anything a copy sends us: the authenticated
    /// label names a copy we track, not our own, that has not diverged. Routing asks this of a
    /// reply that carries no ACK before it reaches a catch-up cursor.
    ///
    /// # Errors
    ///
    /// `ForgedIdentity` for an unauthenticated label or our own node, `NotAMember` for a node
    /// no active predicate names, `Diverged` for a diverged copy.
    pub fn sender(&self, from: &PeerLabel) -> Result<CopyId, AckRejectReason> {
        self.sending_peer(from).map(|(copy, _)| copy)
    }

    fn sending_peer(&self, from: &PeerLabel) -> Result<(CopyId, &CopyProgress), AckRejectReason> {
        use AckRejectReason as Reason;
        if !from.authenticated {
            return Err(Reason::ForgedIdentity);
        }
        let (&copy, peer) = self
            .peers
            .iter()
            .find(|(_, peer)| peer.node == from.node)
            .ok_or(Reason::NotAMember)?;
        if copy == self.own {
            // Nothing we send ourselves is evidence of replication.
            return Err(Reason::ForgedIdentity);
        }
        // Rule 1d (K-B-38): a diverged copy's watermarks are frozen.
        if self.is_diverged(copy) {
            return Err(Reason::Diverged);
        }
        Ok((copy, peer))
    }

    /// Rules 1–8, in order; the first failure wins.
    fn admit(&self, from: &PeerLabel, ack: &AppendAck) -> Result<CopyId, AckRejectReason> {
        use AckRejectReason as Reason;
        // Rule 1: the authenticated peer is the node the ACK speaks for, and a copy we track.
        if !from.authenticated || ack.from != from.node {
            return Err(Reason::ForgedIdentity);
        }
        if ack.partition != self.lineage.partition {
            return Err(Reason::NotAMember);
        }
        let (copy, peer) = self.sending_peer(from)?;
        // Rules 2-4: made in the lineage we serve, under the configuration this copy is held to.
        if ack.generation != self.lineage.generation {
            return Err(Reason::StaleGeneration);
        }
        if ack.owner_epoch != self.lineage.owner_epoch {
            return Err(Reason::StaleEpoch);
        }
        if Some(ack.config_version) != self.pinned_for(copy) {
            return Err(Reason::StaleConfig);
        }
        // Rule 5.
        if ack.role != peer.role {
            return Err(Reason::RoleMismatch);
        }
        // Rule 6: the boot control announced, on the ACK and on the transport label alike.
        if ack.boot != peer.boot || from.boot != peer.boot {
            return Err(Reason::StaleBoot);
        }
        // Rule 7, bounded by what the primary itself received (B-R43 S3-F2): rule 8 holds
        // `received` monotone, so one inflated ACK would otherwise freeze this copy. A copy
        // whose applied is past our head is ahead of us, not inflated: it goes on to rule 9,
        // which has no rung there and asks for a snapshot (B-R47 ruling 3).
        let ahead = ack.progress.buffered_applied.0 > self.head().0;
        let inflated = ack.progress.received > self.own_progress().progress.received;
        if !ordered(&ack.progress) || (inflated && !ahead) {
            return Err(Reason::InconsistentProgress);
        }
        // Rule 8: no watermark retreats.
        let (new, held) = (ack.progress, peer.progress);
        if new.received < held.received
            || new.buffered_applied < held.buffered_applied
            || new.durable < held.durable
        {
            return Err(Reason::RegressedProgress);
        }
        Ok(copy)
    }

    /// All nine rules passed: record the watermarks and report what they changed.
    fn advance(&mut self, copy: CopyId, progress: ReplicaProgress, tick: Tick) -> Vec<EffectKind> {
        let before = self.views();
        let Some(peer) = self.peers.get_mut(&copy) else {
            unreachable!("admit returns a key of peers");
        };
        peer.progress = progress;
        let mut effects = vec![EffectKind::Kernel(KernelEffect::PeerProgress {
            peer: peer.node,
            contiguous_seq: Seq(progress.buffered_applied.0),
        })];
        self.push_qualification(&before, QualificationCause::AckAdvanced, tick, &mut effects);
        self.push_durable(&before, &mut effects);
        effects
    }

    // ---- divergence ----------------------------------------------------------------------

    /// A routed `DivergenceDetected` or `CopyQuarantined` (design §3.4 "two routed events, one
    /// arm, one writer"): the cursor proved it, the tracker marks it. The vector is rule 9's
    /// without its first effect — the routed event already is the proof's trace.
    pub fn on_divergence(&mut self, copy: CopyId, tick: Tick) -> Vec<EffectKind> {
        if copy == self.own || !self.peers.contains_key(&copy) {
            return vec![ignored(KernelIgnoredReason::AckRejected(
                AckRejectReason::NotAMember,
            ))];
        }
        if self.is_diverged(copy) {
            return vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::AlreadyDiverged,
            ))];
        }
        self.diverge(copy, false, tick)
    }

    /// Mark `copy` diverged and emit the deterministic vector (ruling B-R26), in this order: the
    /// proof when the tracker made it, the alert, the loss, the edge if the predicate flipped,
    /// and the block if the floor is gone. A durable view that moved because the copy left its
    /// domain follows, after the ruled five.
    fn diverge(&mut self, copy: CopyId, proved_here: bool, tick: Tick) -> Vec<EffectKind> {
        let before = self.views();
        self.diverged.push(copy);
        let mut effects = Vec::new();
        if proved_here {
            effects.push(EffectKind::Kernel(KernelEffect::DivergenceDetected {
                copy,
            }));
        }
        effects.push(quarantine_alert());
        effects.push(EffectKind::Kernel(KernelEffect::CopyLost { copy }));
        let cause = QualificationCause::DivergenceDetected(copy);
        self.push_qualification(&before, cause, tick, &mut effects);
        if self.regular_secondaries().len() < usize::from(self.config().min_regular_acks) {
            effects.push(EffectKind::Kernel(KernelEffect::BlockPartition(
                BlockReason::DivergenceRequiresOperator {
                    diverged: self.diverged.clone(),
                },
            )));
        }
        self.push_durable(&before, &mut effects);
        effects
    }

    // ---- control -------------------------------------------------------------------------

    /// `ConfigChanged` (design §3.4 rule 6's control half, §3.5 K-B-49): pin a newer
    /// configuration as a new active predicate. A copy whose announced node or boot changed has
    /// restarted and proved nothing, so its watermarks reset — and `diverged` stays, because a
    /// copy that disagreed about our history has not stopped by rebooting. No entry is removed;
    /// retirement does that.
    pub fn on_config_changed(&mut self, config: &PartitionConfig, tick: Tick) -> Vec<EffectKind> {
        let own = self.own_progress();
        let keeps_us = config.member(self.own).is_some_and(|member| {
            member.role == ReplicaRole::Primary
                && (member.node, member.boot) == (own.node, own.boot)
        });
        if config.validate().is_err()
            || config.partition != self.lineage.partition
            || config.config_version <= self.config().config_version
            || !keeps_us
        {
            return vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::InvalidConfig,
            ))];
        }
        let before = self.views();
        let mut restarted = None;
        for member in &config.members {
            let peer = self
                .peers
                .entry(member.copy)
                .or_insert_with(|| CopyProgress::fresh(member));
            if (peer.node, peer.boot) != (member.node, member.boot) {
                *peer = CopyProgress::fresh(member);
                restarted.get_or_insert(member.copy);
            }
            peer.role = member.role;
        }
        self.predicates.push(config.clone());
        let cause = restarted.map_or(QualificationCause::ConfigChanged, |copy| {
            QualificationCause::StaleBoot(copy)
        });
        self.edges(&before, cause, tick)
    }

    /// `TransitionBarrierConfirmed`: retire the predicate pinned at `config_version`, and every
    /// entry no remaining predicate names. The pinned configuration itself never retires.
    pub fn on_transition_confirmed(&mut self, config_version: ConfigVersion) -> Vec<EffectKind> {
        let Some(index) = self
            .predicates
            .iter()
            .position(|predicate| predicate.config_version == config_version)
        else {
            return vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::NotRequired,
            ))];
        };
        if config_version == self.config().config_version {
            return vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::InvalidConfig,
            ))];
        }
        let before = self.views();
        self.predicates.remove(index);
        let predicates = &self.predicates;
        self.peers.retain(|copy, _| {
            predicates
                .iter()
                .any(|predicate| predicate.member(*copy).is_some())
        });
        let mut effects = Vec::new();
        self.push_durable(&before, &mut effects);
        recorded_if_empty(effects)
    }

    /// `Recovered` on a copy that already leads (design §3.4, K-B-02): rebuild from its own
    /// ladder and watermarks under the new root. Every other copy starts at zero and re-proves
    /// its prefix; `diverged` clears here and only here.
    pub fn on_recovered(&mut self, result: &RecoveryResult, tick: Tick) -> Vec<EffectKind> {
        if let Some(refusal) = self.refuses(result) {
            return vec![refusal];
        }
        let before = self.views();
        *self = self.rebuilt(result);
        self.edges(&before, QualificationCause::ConfigChanged, tick)
    }

    /// The one effect that answers `result` instead of a rebuild: `InvalidConfig` when the pin
    /// does not keep this copy the primary on this node; `BarrierNotDurable` when this copy
    /// does not hold the cutoff, so it cannot serve the prefix it would lead; the
    /// corrupt-history alert when it holds another record there.
    fn refuses(&self, result: &RecoveryResult) -> Option<EffectKind> {
        let config = &result.committed.pinned_config;
        let leads = config.member(self.own).is_some_and(|member| {
            member.node == self.node() && member.role == ReplicaRole::Primary
        });
        if !leads || config.partition != self.lineage.partition {
            return Some(ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::InvalidConfig,
            )));
        }
        let selected = &result.selected;
        match self
            .history
            .lookup(selected.cutoff_seq, selected.cutoff_digest)
        {
            DigestLookup::Match => None,
            DigestLookup::Differs { .. } => Some(quarantine_alert()),
            DigestLookup::NotRetained => Some(ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::BarrierNotDurable,
            ))),
        }
    }

    /// The tracker an accepted `result` installs: this copy's own ladder and watermarks cut at
    /// the cutoff, every other copy at zero, one predicate, nothing diverged.
    fn rebuilt(&self, result: &RecoveryResult) -> Self {
        let config = &result.committed.pinned_config;
        let cutoff = result.selected.cutoff_seq;
        let mut history = self.history.clone();
        history.truncate_above(cutoff);
        let durable = self.own_progress().progress.durable;
        let local = ReplicaProgress {
            received: ReceivedSeq(cutoff.0),
            buffered_applied: AppliedSeq(cutoff.0),
            durable: DurableSeq(durable.0.min(cutoff.0)),
        };
        let lineage = Lineage {
            partition: config.partition,
            generation: result.new_generation,
            owner_epoch: result.committed.authority_view.lineage.owner_epoch,
        };
        Self::seeded(config.clone(), self.own, lineage, history, local)
    }

    /// `Flushed` on the primary: its own durable watermark, which the durable views include
    /// (the primary can be the laggard, K-B-13). `None` when no prefix names this lineage.
    pub fn on_flushed(&mut self, durable: &[DurablePrefix]) -> Option<Vec<EffectKind>> {
        let held = self.own_progress().progress.durable;
        let proved = proved_durable(durable, &self.lineage, self.head())?;
        if proved <= held {
            return Some(vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::NothingOutstanding,
            ))]);
        }
        let before = self.views();
        if let Some(peer) = self.peers.get_mut(&self.own) {
            peer.progress.durable = proved;
        }
        let mut effects = Vec::new();
        self.push_durable(&before, &mut effects);
        Some(recorded_if_empty(effects))
    }

    // ---- edges ---------------------------------------------------------------------------

    fn views(&self) -> Views {
        Views {
            qualifies: self.qualifies_now(self.anchor),
            durable: self.durable_per_predicate(),
        }
    }

    /// Both edges a control event can move, or `Recorded` when it moved neither (BA-2: an
    /// empty vector is never an answer).
    fn edges(&self, before: &Views, cause: QualificationCause, tick: Tick) -> Vec<EffectKind> {
        let mut effects = Vec::new();
        self.push_qualification(before, cause, tick, &mut effects);
        self.push_durable(before, &mut effects);
        recorded_if_empty(effects)
    }

    /// `QualificationChanged` when and only when `qualifies_now(anchor)` changed value in this
    /// step (rulings B-R27, B-R47a).
    fn push_qualification(
        &self,
        before: &Views,
        cause: QualificationCause,
        tick: Tick,
        effects: &mut Vec<EffectKind>,
    ) {
        let anchor = self.anchor;
        let qualifies = self.qualifies_now(anchor);
        if qualifies == before.qualifies {
            return;
        }
        let qualified_copies = self.qualified_copies(anchor);
        effects.push(EffectKind::Kernel(KernelEffect::QualificationChanged(
            QualificationChanged {
                lineage: self.lineage,
                config_version: self.config().config_version,
                at_seq: anchor,
                direction: if qualifies {
                    QualificationDirection::Gained
                } else {
                    QualificationDirection::Lost
                },
                qualified_ack_count: u8::try_from(qualified_copies.len()).unwrap_or(u8::MAX),
                qualified_copies,
                cause,
                tick,
            },
        )));
    }

    /// `DurableAdvanced` when any active predicate's durable view moved in this step.
    fn push_durable(&self, before: &Views, effects: &mut Vec<EffectKind>) {
        let per_predicate = self.durable_per_predicate();
        if per_predicate != before.durable {
            effects.push(EffectKind::Kernel(KernelEffect::DurableAdvanced {
                per_predicate,
            }));
        }
    }
}

/// `received >= buffered_applied >= durable` (rule 7).
const fn ordered(progress: &ReplicaProgress) -> bool {
    progress.received.0 >= progress.buffered_applied.0
        && progress.buffered_applied.0 >= progress.durable.0
}

/// A step that moved no view is still answered, never silence (BA-2).
fn recorded_if_empty(effects: Vec<EffectKind>) -> Vec<EffectKind> {
    if effects.is_empty() {
        vec![ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::Recorded,
        ))]
    } else {
        effects
    }
}
