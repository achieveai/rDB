//! The primary's `ProgressTracker` (design §3.4): the ACK admission ladder, the divergence
//! vector, and the control events that change what the tracker believes. The derived views of
//! §3.5 are in `views.rs`.
//!
//! # What seeds it
//!
//! A tracker is built by [`ProgressTracker::new`] (fixtures, the manual tester) and rebuilt by a
//! committed `Recovered`. The primary's own `LocalApplied` grows its ladder, `received` and
//! applied head one record at a time (lead ruling B-R47, closing B-R36-Q1). The qualification
//! edge stays at the seed head, the anchor (B-R47a); a head above it reports `Gained` too, when
//! it comes to qualify, and never `Lost` (lead ruling B-R47b).

use std::collections::BTreeMap;

use crate::contracts::authority::{AuthorityView, BlockReason, Lineage};
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

use super::{proved_durable, view_refusal, DigestLadder, DigestLookup, HeldView};

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
    /// The position a `Recovered` barrier's `DurableAt` proof established for this incarnation
    /// of the copy: the cutoff on all three watermarks when the barrier requires the copy, zero
    /// otherwise (lead rulings B-R67e and B-R67f). Each `Recovered` restates it, replacing the
    /// last, and a restarted copy starts again at zero.
    ///
    /// Read **only** by the repeat judgment ([`ProgressTracker::known`]). No watermark, view,
    /// predicate, qualification, lag or `PeerProgress` ever reads it: those move only on an
    /// admitted ACK. It exists because a `Recovered` drops the cursor and zeroes the watermarks
    /// that would have recognised a duplicate ACK from before it.
    ///
    /// Why recording an ACK below it is safe: the proof already establishes that the copy holds
    /// the cutoff durably, on this history, so an ACK at or below it claims nothing the proof has
    /// not established, and recording it cannot hide a divergence. An ACK above it is not a
    /// repeat and still reaches the ladder, and one at it is checked against the ladder's rung.
    proved: ReplicaProgress,
}

impl CopyProgress {
    /// A copy that has proved nothing yet.
    const fn fresh(member: &Member) -> Self {
        Self {
            node: member.node,
            boot: member.boot,
            role: member.role,
            progress: ReplicaProgress::EMPTY,
            proved: ReplicaProgress::EMPTY,
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
    /// evaluated at the moving head the predicate would go false on every write. The head has
    /// its own `Gained`, never `Lost` (lead ruling B-R47b).
    anchor: Seq,
    /// The `authority_seq` of the newest A1 view installed (design §2.2); 0 before any.
    authority_seq: u64,
    /// A `Recovered` pinned this node something other than the primary (lead ruling on the kept
    /// primary, B-R58a): it serves nothing until a `Recovered` pins it primary again.
    retired: bool,
    /// Where this lineage's history begins: the cutoff of the `Recovered` that last rebuilt the
    /// tracker, 0 for one [`Self::new`] built and nothing rebuilt (lead rulings B-R71, B-R71a).
    base_seq: Seq,
    /// The base the lineage before this one began at, when this tracker saw it: a rebuild into a
    /// new generation takes its own `base_seq`, and one in the same generation keeps this. A
    /// tracker built fresh has none. A record at or below it is older than the predecessor, which
    /// the receiver's historical rule does not admit (catch-up step 1a).
    /// It is the base this tracker last served, not checked against the recovery's
    /// `predecessor_generation`: after generations this node did not serve it can sit below the
    /// predecessor's base. Step 1a then under-fires: a record older than the predecessor can still
    /// go out, and draws `StaleGeneration`. It never refuses a record the receiver would admit.
    prior_base: Option<Seq>,
}

/// The two views a step compares before and after itself. Every edge the tracker reports is a
/// difference between two of these, so no edge needs remembered state.
struct Views {
    qualifies: bool,
    /// The primary's head, and whether it qualified (lead ruling B-R47b).
    head: Seq,
    qualifies_head: bool,
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
            authority_seq: 0,
            retired: false,
            base_seq: Seq::ZERO,
            prior_base: None,
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

    /// Every copy any active predicate names, the primary's own included, in copy order.
    pub fn peers(&self) -> impl Iterator<Item = (CopyId, &CopyProgress)> {
        self.peers.iter().map(|(copy, peer)| (*copy, peer))
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
    /// It moves no edge: the anchor's edge is evaluated at [`Self::anchor`], which a local write
    /// never moves (B-R47a), and the new head qualifies on no copy yet, while a head that stops
    /// qualifying reports nothing (B-R47b). Lag behind fresh writes is L1's.
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

    /// Where the qualification edge that goes both ways is evaluated (lead ruling B-R47a): the
    /// head this tracker was seeded or rebuilt at. The head above it has a `Gained` of its own
    /// (B-R47b).
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

    /// Lead ruling B-R58c: the copy `ack` speaks for when rules 1–8 admit it and rule 9 finds
    /// no rung at its applied sequence, which lies strictly below the anchor — an ACK the ladder
    /// can neither verify nor refute, for a record the recovery cutoff precedes. `None`
    /// otherwise. Changes nothing: whether it is the record a cursor has in flight is the
    /// caller's question.
    #[must_use]
    pub fn unverified_below_anchor(&self, from: &PeerLabel, ack: &AppendAck) -> Option<CopyId> {
        let copy = self.admit(from, ack).ok()?;
        let at = Seq(ack.progress.buffered_applied.0);
        let lookup = self.history.lookup(at, ack.digest_at_buffered);
        (at < self.anchor && lookup == DigestLookup::NotRetained).then_some(copy)
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

    /// Lead ruling B-R67c: the copy `ack` speaks for when rules 1–7 admit it — the authenticated
    /// node, the lineage (generation and epoch), configuration, role and boot this copy is held to
    /// now, and ordered progress — without asking rule 8 or rule 9. `None` otherwise. Routing asks
    /// this before it asks whether `ack` repeats what the copy's cursor already took: a repeat is
    /// a repeat only in the same copy, boot and generation.
    #[must_use]
    pub fn identify(&self, from: &PeerLabel, ack: &AppendAck) -> Option<CopyId> {
        self.rules_one_to_seven(from, ack)
            .ok()
            .map(|(copy, _)| copy)
    }

    /// Lead ruling B-R67d: an ACK that repeats a catch-up cursor's high-water mark exactly — a
    /// liveness report, as the keepalive's ACK is by design (B-R60). It runs rules 1–9 as a first
    /// ACK at that position would, and one they admit advances its copy and emits
    /// `PeerProgress`. Anything else answers `Recorded` and changes nothing: a repeat never
    /// escalates, and one the ladder cannot verify, below a recovery cutoff, stays unverified
    /// (lead ruling B-R58c).
    pub fn on_repeat_at_mark(
        &mut self,
        from: &PeerLabel,
        ack: &AppendAck,
        tick: Tick,
    ) -> Vec<EffectKind> {
        let at = Seq(ack.progress.buffered_applied.0);
        match self.admit(from, ack) {
            Ok(copy) if self.history.lookup(at, ack.digest_at_buffered) == DigestLookup::Match => {
                self.advance(copy, ack.progress, tick)
            }
            _ => vec![ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::Recorded,
            ))],
        }
    }

    /// The most the tracker knows `copy` holds, for the repeat judgment only (lead rulings
    /// B-R67e and B-R67f): each watermark the higher of what its last admitted ACK stated and
    /// its proved floor. `None` for a copy no active predicate names.
    #[must_use]
    pub fn known(&self, copy: CopyId) -> Option<ReplicaProgress> {
        self.peers.get(&copy).map(|peer| ReplicaProgress {
            received: peer.progress.received.max(peer.proved.received),
            buffered_applied: peer
                .progress
                .buffered_applied
                .max(peer.proved.buffered_applied),
            durable: peer.progress.durable.max(peer.proved.durable),
        })
    }

    /// Rules 1–8, in order; the first failure wins.
    fn admit(&self, from: &PeerLabel, ack: &AppendAck) -> Result<CopyId, AckRejectReason> {
        let (copy, held) = self.rules_one_to_seven(from, ack)?;
        // Rule 8: no watermark retreats.
        let new = ack.progress;
        if new.received < held.received
            || new.buffered_applied < held.buffered_applied
            || new.durable < held.durable
        {
            return Err(AckRejectReason::RegressedProgress);
        }
        Ok(copy)
    }

    /// Rules 1–7, in order; the first failure wins. Names the copy and the progress it holds.
    fn rules_one_to_seven(
        &self,
        from: &PeerLabel,
        ack: &AppendAck,
    ) -> Result<(CopyId, ReplicaProgress), AckRejectReason> {
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
        Ok((copy, peer.progress))
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
    ///
    /// A pin for this partition that names this node anything but the primary retires the
    /// tracker (lead ruling on the kept primary, B-R58a): routing treats it as absent until a
    /// `Recovered` pins it primary again, and that rebuild starts it unretired.
    pub fn on_recovered(&mut self, result: &RecoveryResult, tick: Tick) -> Vec<EffectKind> {
        if let Some(refusal) = self.refuses(result) {
            let config = &result.committed.pinned_config;
            if config.partition == self.lineage.partition && !self.leads(config) {
                self.retired = true;
            }
            return vec![refusal];
        }
        let before = self.views();
        *self = self.rebuilt(result);
        self.edges(&before, QualificationCause::ConfigChanged, tick)
    }

    /// Whether `config` makes this copy the primary, on this node.
    fn leads(&self, config: &PartitionConfig) -> bool {
        config
            .member(self.own)
            .is_some_and(|member| member.node == self.node() && member.role == ReplicaRole::Primary)
    }

    /// The one effect that answers `result` instead of a rebuild: `InvalidConfig` when the pin
    /// does not keep this copy the primary on this node; `BarrierNotDurable` when this copy
    /// does not hold the cutoff, so it cannot serve the prefix it would lead; the
    /// corrupt-history alert when it holds another record there.
    fn refuses(&self, result: &RecoveryResult) -> Option<EffectKind> {
        let config = &result.committed.pinned_config;
        if !self.leads(config) || config.partition != self.lineage.partition {
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
    /// the cutoff, every other copy at zero, one predicate, nothing diverged. Each other copy the
    /// barrier requires gets its proved floor at the cutoff, which only the repeat judgment reads
    /// (lead ruling B-R67f).
    ///
    /// M9 S0 D3: a re-emit in the generation already served cuts at this copy's own head when
    /// that is above the cutoff, so R1's head and T1's next sequence still agree. Everything
    /// above the cutoff was written in this generation, and [`Self::refuses`] has already
    /// matched the cutoff in this copy's history. A retired tracker, or any other generation,
    /// cuts at the cutoff.
    fn rebuilt(&self, result: &RecoveryResult) -> Self {
        let config = &result.committed.pinned_config;
        let cutoff = result.selected.cutoff_seq;
        let head = if !self.retired && result.new_generation == self.lineage.generation {
            cutoff.max(self.head())
        } else {
            cutoff
        };
        let mut history = self.history.clone();
        history.truncate_above(head);
        let durable = self.own_progress().progress.durable;
        let local = ReplicaProgress {
            received: ReceivedSeq(head.0),
            buffered_applied: AppliedSeq(head.0),
            durable: DurableSeq(durable.0.min(head.0)),
        };
        let lineage = Lineage {
            partition: config.partition,
            generation: result.new_generation,
            // Replaced from the view just below: one write for `View` and `Recovered` alike.
            owner_epoch: self.lineage.owner_epoch,
        };
        let mut rebuilt = Self::seeded(config.clone(), self.own, lineage, history, local);
        // Lead ruling B-R71a. A `Recovered` in the generation already served re-announces it (a
        // mode change, or F1's rebuild proving a copy durable): the lineage before it is the same.
        rebuilt.base_seq = cutoff;
        rebuilt.prior_base = if result.new_generation == self.lineage.generation {
            self.prior_base
        } else {
            Some(self.base_seq)
        };
        rebuilt.adopt_view(&result.committed.authority_view);
        let barrier = &result.barrier;
        let floor = ReplicaProgress {
            received: ReceivedSeq(barrier.cutoff().0),
            buffered_applied: AppliedSeq(barrier.cutoff().0),
            durable: DurableSeq(barrier.cutoff().0),
        };
        for copy in barrier.required() {
            if let Some(peer) = rebuilt.peers.get_mut(copy).filter(|_| *copy != self.own) {
                peer.proved = floor;
            }
        }
        rebuilt
    }

    /// A1's `View` on the primary (design §2.2, lead ruling B-R53): installs a newer owner epoch
    /// and answers `Recorded`, or answers [`view_refusal`]'s reason and changes nothing.
    ///
    /// The configuration version is a gate here and never written: the tracker's version is the
    /// newest predicate's, a whole membership that only `ConfigChanged` pushes. Writing the bare
    /// number would make that `ConfigChanged` look stale, refuse it, and lose the members and the
    /// incarnation reset they carry (M7B-150).
    pub fn on_view(&mut self, view: &AuthorityView) -> Vec<EffectKind> {
        let held = HeldView {
            lineage: self.lineage,
            config_version: self.config().config_version,
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

    /// The `authority_seq` of the newest A1 view installed; 0 before any.
    #[must_use]
    pub const fn authority_seq(&self) -> u64 {
        self.authority_seq
    }

    /// Whether a `Recovered` retired this primary: its pin names this node something other than
    /// the primary. Routing treats a retired primary as absent for everything but `Recovered`.
    #[must_use]
    pub const fn retired(&self) -> bool {
        self.retired
    }

    /// Where this lineage's history begins (lead ruling B-R71).
    #[must_use]
    pub const fn base_seq(&self) -> Seq {
        self.base_seq
    }

    /// Where the lineage before this one began, when this tracker saw it (lead ruling B-R71a).
    #[must_use]
    pub const fn prior_base(&self) -> Option<Seq> {
        self.prior_base
    }

    /// Take a view's epoch and `authority_seq`: the one write `View` and `Recovered` share. The
    /// generation is the caller's, because a recovered view names the root it recovered from.
    fn adopt_view(&mut self, view: &AuthorityView) {
        self.lineage.owner_epoch = view.lineage.owner_epoch;
        self.authority_seq = view.authority_seq;
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
        let head = self.head();
        Views {
            qualifies: self.qualifies_now(self.anchor),
            head,
            qualifies_head: self.qualifies_now(head),
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
    /// step (rulings B-R27, B-R47a), then `Gained` at the head when this step made a head above
    /// the anchor qualify (lead ruling B-R47b).
    ///
    /// The head's edge exists so P1 hears that its candidate qualifies: P1 matches an edge to
    /// the candidate by `at_seq`, and one transaction is in flight at a time (spec §5.2; T1
    /// holds `UnresolvedTransaction` from `LocalApplied` until `Published`), so the candidate is
    /// the head. If pipelining widens, `Gained` must cover every newly qualified seq in (old
    /// frontier, new frontier].
    ///
    /// It is computed from the views like the anchor's, with nothing remembered: it fires again
    /// if the head stops qualifying and then qualifies again, and never while it stays
    /// qualified. The head never reports `Lost`. P1 re-reads `qualifies_now(candidate)` live
    /// before it publishes, so a head that stopped qualifying cannot publish unreported. L1
    /// reads the anchor's edge alone, which is unchanged; a head `Gained` implies the anchor
    /// already qualifies (qualification is monotone), so L1 learns nothing new from it.
    fn push_qualification(
        &self,
        before: &Views,
        cause: QualificationCause,
        tick: Tick,
        effects: &mut Vec<EffectKind>,
    ) {
        let anchor = self.anchor;
        let qualifies = self.qualifies_now(anchor);
        if qualifies != before.qualifies {
            effects.push(self.edge_at(anchor, qualifies, cause, tick));
        }
        let head = self.head();
        let head_gained = head > anchor
            && self.qualifies_now(head)
            && !(before.head == head && before.qualifies_head);
        if head_gained {
            effects.push(self.edge_at(head, true, cause, tick));
        }
    }

    /// The `QualificationChanged` at `at_seq`, `Gained` when `qualifies`.
    fn edge_at(
        &self,
        at_seq: Seq,
        qualifies: bool,
        cause: QualificationCause,
        tick: Tick,
    ) -> EffectKind {
        let qualified_copies = self.qualified_copies(at_seq);
        EffectKind::Kernel(KernelEffect::QualificationChanged(QualificationChanged {
            lineage: self.lineage,
            config_version: self.config().config_version,
            at_seq,
            direction: if qualifies {
                QualificationDirection::Gained
            } else {
                QualificationDirection::Lost
            },
            qualified_ack_count: u8::try_from(qualified_copies.len()).unwrap_or(u8::MAX),
            qualified_copies,
            cause,
            tick,
        }))
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
