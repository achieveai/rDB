//! L1's live state and its transitions (team kernel-b `design.md` §4.1-§4.4).
//!
//! Every fact read here has exactly one writer, and every writer is an event:
//! `qualifies_now_at_head` by `QualificationChanged.direction`, `peer_progress` by
//! `PeerProgress`, `lost` by `CopyLost`, `blocked` by `BlockPartition`. There is no health
//! backstop that re-reads the qualification flag (K-B-40): a dropped `Lost` edge is the
//! dispatcher's to prevent, not L1's to detect.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::{ignored, Mode};
use crate::contracts::authority::BlockReason;
use crate::contracts::errors::ErrorKind;
use crate::contracts::event::{Budgets, KernelEffect, KernelEvent};
use crate::contracts::ids::{ConfigVersion, DurableSeq, NodeId, PartitionId, Seq};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::membership::{CopyId, Member, PartitionConfig};
use crate::contracts::protection::{AdmissionState, ReplicationLag};
use crate::contracts::qualification::{QualificationChanged, QualificationDirection};
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::time::Tick;

/// One required-copy predicate, pinned to a configuration version (design §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Predicate {
    config_version: ConfigVersion,
    /// The primary plus the regular secondaries. Never shadows (K-B-48).
    copies: BTreeSet<CopyId>,
    /// The highest seq durable on every copy of this predicate, as R1 last reported it.
    /// Zero until R1 reports: resume needs R1's evidence, never an assumption.
    durable_through: DurableSeq,
}

impl Predicate {
    fn of(config: &PartitionConfig) -> Self {
        Self {
            config_version: config.config_version,
            copies: required_members(config).map(|member| member.copy).collect(),
            durable_through: DurableSeq::default(),
        }
    }
}

/// The members a predicate requires: the primary plus the regular secondaries, never a shadow
/// (K-B-48). `PartitionConfig::required_regular` excludes the primary, so it is chained in.
fn required_members(config: &PartitionConfig) -> impl Iterator<Item = &Member> {
    config
        .primary()
        .into_iter()
        .chain(config.required_regular())
}

/// A record applied locally and not yet durable on every active predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UnsafeEntry {
    seq: Seq,
    bytes: u64,
    /// Set once, never rewritten, so a membership rename cannot make it young (design §4.3).
    applied_at: Tick,
}

/// The live state of a primary's L1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct State {
    self_copy: CopyId,
    /// The newest pinned configuration; maps a reporting node to its copy. Its partition is the
    /// one this instance serves.
    config: PartitionConfig,
    budgets: Budgets,
    mode: Mode,
    paused_prefix: Seq,
    resume_barrier: Seq,
    /// Current first; an old predicate stays until its transition barrier is confirmed.
    predicates: Vec<Predicate>,
    unsafe_queue: VecDeque<UnsafeEntry>,
    /// R1's durable views for versions above the current predicate, not pinned yet (lead ruling
    /// B-R46d, tester E1). R1 answers its own `ConfigChanged` with a `DurableAdvanced`, and a
    /// router may hand the change to R1 before L1. The highest view per version is kept. A pin
    /// seeds its predicate from its entry and drops every entry it reaches or passes, so only
    /// versions above the current one are ever held: bounded by construction, no cap.
    pending_durable: BTreeMap<ConfigVersion, DurableSeq>,
    highest_applied: Seq,
    qualifies_now_at_head: bool,
    peer_progress: BTreeMap<CopyId, Tick>,
    lost: BTreeSet<CopyId>,
    blocked: Option<BlockReason>,
}

impl State {
    /// The instance a `Recovered` delivered for `partition` builds on the node it names primary;
    /// `Ok(None)` on any other node.
    ///
    /// Starts `Paused` at the cutoff with no qualification (design §4.1, K-B-47), and inherits
    /// no exposure: the queue is empty. A result whose pinned configuration belongs to another
    /// partition is refused `InvalidConfig` (review M1).
    pub(super) fn at_recovery(
        result: &RecoveryResult,
        partition: PartitionId,
        node: NodeId,
        budgets: Budgets,
    ) -> Result<Option<Self>, ReplicaIgnoreReason> {
        let config = &result.committed.pinned_config;
        if config.partition != partition {
            return Err(ReplicaIgnoreReason::InvalidConfig);
        }
        let Some(primary) = config.primary().filter(|member| member.node == node) else {
            return Ok(None);
        };
        let cutoff = result.selected.cutoff_seq;
        Ok(Some(Self {
            self_copy: primary.copy,
            config: config.clone(),
            budgets,
            mode: Mode::Paused,
            paused_prefix: cutoff,
            resume_barrier: cutoff,
            predicates: vec![Predicate::of(config)],
            unsafe_queue: VecDeque::new(),
            pending_durable: BTreeMap::new(),
            highest_applied: cutoff,
            qualifies_now_at_head: false,
            peer_progress: BTreeMap::new(),
            lost: BTreeSet::new(),
            blocked: None,
        }))
    }

    /// Whether `event` is one of L1's kernel inputs other than `Recovered`: the set
    /// [`Self::on_input`] consumes, so an inert instance refuses exactly what a live one does.
    pub(super) fn is_input(event: &KernelEvent) -> bool {
        matches!(
            event,
            KernelEvent::QualificationChanged(_)
                | KernelEvent::BlockPartition(_)
                | KernelEvent::PeerProgress { .. }
                | KernelEvent::CopyLost { .. }
                | KernelEvent::LocalApplied { .. }
                | KernelEvent::DurableAdvanced { .. }
                | KernelEvent::ConfigChanged(_)
                | KernelEvent::TransitionBarrierConfirmed { .. }
        )
    }

    /// Applies one L1 input, or `None` when `event` is not one. Only the qualification edge and
    /// the block drive a transition; the rest write state for the next health evaluation
    /// (design §4.4). A drain may also return Warn to Healthy (B-R42), which is never an
    /// admission edge.
    pub(super) fn on_input(&mut self, now: Tick, event: &KernelEvent) -> Option<Vec<KernelEffect>> {
        let reason = match event {
            KernelEvent::QualificationChanged(edge) => {
                return Some(self.on_qualification(now, edge))
            }
            KernelEvent::BlockPartition(reason) => return Some(self.on_block(now, reason)),
            KernelEvent::PeerProgress { peer, .. } => self.record_progress(now, *peer),
            KernelEvent::CopyLost { copy } => self.record_lost(*copy),
            KernelEvent::LocalApplied { seq, bytes, .. } => self.record_applied(now, *seq, *bytes),
            KernelEvent::DurableAdvanced { per_predicate } => {
                self.record_durable(now, per_predicate)
            }
            KernelEvent::ConfigChanged(config) => self.pin(config),
            KernelEvent::TransitionBarrierConfirmed {
                config_version,
                through_seq,
            } => self.retire(now, *config_version, *through_seq),
            _ => return None,
        };
        Some(ignored(KernelIgnoredReason::Replica(reason)))
    }

    /// The health evaluation at `now` (design §4.4, "on HealthEval only").
    pub(super) fn health_eval(&mut self, now: Tick) -> Vec<KernelEffect> {
        // A backstop (lead rulings B-R46b, B-R46d; tester E2). Every door that raises the floor
        // drains on its own, so today this finds no work. It stops a door added later from
        // repeating D1: a durable record read as unsafe exposure and pausing an idle partition.
        self.drain_to_floor(now);
        let before = self.verdict();
        let reason = match self.mode {
            Mode::Healthy | Mode::Warn => return self.eval_exposure(now),
            Mode::Paused => self.eval_paused(),
            Mode::Reprotecting { below_since } => self.eval_reprotecting(now, below_since),
        };
        self.edge_or(now, before, reason)
    }

    /// Healthy and Warn read exposure: the age of the oldest unsafe record.
    fn eval_exposure(&mut self, now: Tick) -> Vec<KernelEffect> {
        let Some((oldest_unsafe_seq, age_ms)) = self.oldest(now) else {
            self.mode = Mode::Healthy;
            return ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::NothingOutstanding,
            ));
        };
        if age_ms >= self.budgets.pause_age_millis {
            let before = self.verdict();
            self.pause_at_head();
            return self.edge_or(now, before, ReplicaIgnoreReason::Outstanding);
        }
        if age_ms >= self.budgets.warn_age_millis {
            if self.mode == Mode::Healthy {
                self.mode = Mode::Warn;
                return vec![KernelEffect::ProtectionWarn {
                    oldest_unsafe_seq,
                    age_ms,
                }];
            }
        } else {
            self.mode = Mode::Healthy;
        }
        ignored(KernelIgnoredReason::Replica(
            ReplicaIgnoreReason::Outstanding,
        ))
    }

    /// `Paused -> Reprotecting` needs all three conjuncts: the exact barrier durable on every
    /// predicate, a qualifying secondary, and no block (design §4.4, K-B-51).
    fn eval_paused(&mut self) -> ReplicaIgnoreReason {
        if self.blocked.is_some() {
            ReplicaIgnoreReason::AlreadyBlocked
        } else if !self.barrier_durable() {
            ReplicaIgnoreReason::BarrierNotDurable
        } else if !self.qualifies_now_at_head {
            ReplicaIgnoreReason::NoQualifyingSecondary
        } else {
            self.mode = Mode::Reprotecting { below_since: None };
            ReplicaIgnoreReason::ResumeHeld
        }
    }

    /// The hysteresis: `replication_lag` below `resume_lag_millis`, continuously, for
    /// `resume_hold_millis`. A lag at or above the threshold restarts the hold.
    ///
    /// A completed hold resumes only while the oldest unsafe record is younger than
    /// `pause_age_millis`; otherwise it pauses at the head, because that exposure is the state a
    /// pause exists to reject (lead ruling B-R46 S1, spec §6.2). Both states reject, so neither
    /// arm that pauses here is an admission edge.
    fn eval_reprotecting(&mut self, now: Tick, below_since: Option<Tick>) -> ReplicaIgnoreReason {
        if !self.barrier_durable() {
            self.pause_at_head();
            return ReplicaIgnoreReason::BarrierNotDurable;
        }
        let (lag, _) = self.replication_lag(now);
        if lag >= ReplicationLag::millis(self.budgets.resume_lag_millis) {
            self.mode = Mode::Reprotecting { below_since: None };
            return ReplicaIgnoreReason::ResumeHeld;
        }
        let since = below_since.unwrap_or(now);
        if since.millis_until(now) < self.budgets.resume_hold_millis {
            self.mode = Mode::Reprotecting {
                below_since: Some(since),
            };
            return ReplicaIgnoreReason::ResumeHeld;
        }
        if self
            .oldest(now)
            .is_some_and(|(_, age_ms)| age_ms >= self.budgets.pause_age_millis)
        {
            self.pause_at_head();
            return ReplicaIgnoreReason::Outstanding;
        }
        self.mode = Mode::Healthy;
        ReplicaIgnoreReason::ResumeHeld
    }

    /// `Lost` pauses immediately, independent of both ages; `Gained` only sets the flag
    /// (design §4.4 first arm). Only `direction` is read (B-R27).
    fn on_qualification(&mut self, now: Tick, edge: &QualificationChanged) -> Vec<KernelEffect> {
        match edge.direction {
            QualificationDirection::Gained => {
                self.qualifies_now_at_head = true;
                ignored(KernelIgnoredReason::Replica(ReplicaIgnoreReason::Recorded))
            }
            QualificationDirection::Lost => {
                let before = self.verdict();
                self.qualifies_now_at_head = false;
                self.pause_at_head();
                self.edge_or(now, before, ReplicaIgnoreReason::NoQualifyingSecondary)
            }
        }
    }

    /// A block is a pause with no exit inside this instance (design §4.4, K-B-46).
    fn on_block(&mut self, now: Tick, reason: &BlockReason) -> Vec<KernelEffect> {
        if self.blocked.is_some() {
            return ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::AlreadyBlocked,
            ));
        }
        let before = self.verdict();
        self.blocked = Some(reason.clone());
        if self.mode != Mode::Paused {
            self.pause_at_head();
        }
        self.edge_or(now, before, ReplicaIgnoreReason::Recorded)
    }

    fn record_progress(&mut self, now: Tick, peer: NodeId) -> ReplicaIgnoreReason {
        match self
            .config
            .members
            .iter()
            .find(|member| member.node == peer)
        {
            Some(member) => {
                self.peer_progress.insert(member.copy, now);
                ReplicaIgnoreReason::Recorded
            }
            None => ReplicaIgnoreReason::InvalidConfig,
        }
    }

    /// A lost copy leaves the lag domain. A copy in no member slot is refused, so
    /// `lost_copies` never names a phantom (review A2).
    fn record_lost(&mut self, copy: CopyId) -> ReplicaIgnoreReason {
        if self.config.member(copy).is_none() {
            return ReplicaIgnoreReason::InvalidConfig;
        }
        self.lost.insert(copy);
        ReplicaIgnoreReason::Recorded
    }

    /// Queues an applied record as unsafe exposure, unless it is at or below the durable floor:
    /// then it is already durable on every predicate and is not queued (lead ruling B-R46b,
    /// tester D1). That happens when R1's `DurableAdvanced` reaches L1 before the primary's own
    /// `LocalApplied`, or for a seq at or below the recovery cutoff. The head still moves.
    fn record_applied(&mut self, now: Tick, seq: Seq, bytes: u64) -> ReplicaIgnoreReason {
        self.highest_applied = self.highest_applied.max(seq);
        if seq.0 <= self.floor() {
            return ReplicaIgnoreReason::NothingOutstanding;
        }
        self.unsafe_queue.push_back(UnsafeEntry {
            seq,
            bytes,
            applied_at: now,
        });
        ReplicaIgnoreReason::Recorded
    }

    /// Records R1's durable views and drains the queue through the minimum over **every**
    /// active predicate, so an easier new membership cannot erase old exposure (design §4.3).
    ///
    /// An active predicate's view is stored as reported, not maxed (unchanged by B-R46d). A view
    /// for a version above the current predicate is kept in `pending_durable` for its pin, the
    /// highest per version (lead ruling B-R46d, tester E1). Any other entry is dropped. A report
    /// that records nothing — every version below the lowest active predicate, or between active
    /// ones — is refused (review A2).
    fn record_durable(
        &mut self,
        now: Tick,
        per_predicate: &[(ConfigVersion, DurableSeq)],
    ) -> ReplicaIgnoreReason {
        let mut recorded = false;
        for (version, durable) in per_predicate {
            if let Some(index) = self.predicate(*version) {
                self.predicates[index].durable_through = *durable;
                recorded = true;
            } else if *version > self.config.config_version {
                let pending = self.pending_durable.entry(*version).or_default();
                *pending = (*pending).max(*durable);
                recorded = true;
            }
        }
        if !recorded {
            return ReplicaIgnoreReason::InvalidConfig;
        }
        self.drain_to_floor(now);
        ReplicaIgnoreReason::Recorded
    }

    /// The durable floor: the minimum stored `durable_through` over every active predicate, so
    /// an easier new membership cannot erase old exposure (design §4.3).
    fn floor(&self) -> u64 {
        self.predicates
            .iter()
            .map(|p| p.durable_through.0)
            .min()
            .unwrap_or(0)
    }

    /// Drains the queue through [`Self::floor`], then leaves Warn if the front is now young
    /// enough. One drain for every door (lead ruling B-R46b, tester D1): a durable report and a
    /// retirement that drops the lowest predicate (B-R46a, review R2), so the result never
    /// depends on the order those inputs arrive in. The top of every health evaluation calls it
    /// too, as a backstop that finds no work today (B-R46d).
    fn drain_to_floor(&mut self, now: Tick) {
        let floor = self.floor();
        while self.unsafe_queue.front().is_some_and(|e| e.seq.0 <= floor) {
            self.unsafe_queue.pop_front();
        }
        self.leave_warn_if_drained(now);
    }

    /// `Warn -> Healthy` in the draining step once the front is younger than the warn age
    /// (review A3, lead ruling B-R42). A drain only makes the front younger, so this is the only
    /// direction it can move, and the Warn hint never has to cover a pending downgrade. The
    /// upward moves stay with the health evaluation (design §4.4); neither side of this move is
    /// an admission edge, so the step's answer is unchanged.
    fn leave_warn_if_drained(&mut self, now: Tick) {
        let still_warn = self
            .oldest(now)
            .is_some_and(|(_, age_ms)| age_ms >= self.budgets.warn_age_millis);
        if self.mode == Mode::Warn && !still_warn {
            self.mode = Mode::Healthy;
        }
    }

    /// Pushes a new current predicate. The queue is not touched (design §4.3). A version at or
    /// below the current one is refused, held or not (lead ruling B-R38): versions only grow.
    /// Another partition's configuration is refused whatever its version (review M1).
    ///
    /// The new predicate starts from R1's pending view of its version, if one arrived first,
    /// instead of 0; then every pending view at or below it is dropped (lead ruling B-R46d). A
    /// fresh predicate is at 0, so the seed can only raise it. Adding a predicate never raises
    /// the floor, so there is nothing to drain.
    fn pin(&mut self, config: &PartitionConfig) -> ReplicaIgnoreReason {
        if config.partition != self.config.partition
            || self
                .predicates
                .first()
                .is_some_and(|current| config.config_version <= current.config_version)
        {
            return ReplicaIgnoreReason::InvalidConfig;
        }
        let mut predicate = Predicate::of(config);
        if let Some(durable) = self.pending_durable.remove(&config.config_version) {
            predicate.durable_through = durable;
        }
        self.pending_durable
            .retain(|version, _| *version > config.config_version);
        self.predicates.insert(0, predicate);
        self.config = config.clone();
        ReplicaIgnoreReason::Recorded
    }

    /// Retires an old predicate, only if its barrier is durable (design §4.3). The current
    /// predicate never retires. The retirement then drains through the survivors' stored floor
    /// (lead ruling B-R46a, review R2): R1's `DurableAdvanced` for a survivor may have arrived
    /// first, while the retiring predicate still held the floor down, and no later report is
    /// owed. Without this drain the stale exposure would re-pause every completed hold (S1).
    fn retire(
        &mut self,
        now: Tick,
        version: ConfigVersion,
        through_seq: Seq,
    ) -> ReplicaIgnoreReason {
        match self.predicate(version) {
            None | Some(0) => ReplicaIgnoreReason::InvalidConfig,
            Some(index) if self.predicates[index].durable_through.0 < through_seq.0 => {
                ReplicaIgnoreReason::BarrierNotDurable
            }
            Some(index) => {
                self.predicates.remove(index);
                self.drain_to_floor(now);
                ReplicaIgnoreReason::Recorded
            }
        }
    }

    fn predicate(&self, version: ConfigVersion) -> Option<usize> {
        self.predicates
            .iter()
            .position(|p| p.config_version == version)
    }

    /// `* -> Paused { paused_prefix = highest_applied, resume_barrier = highest_applied }`.
    fn pause_at_head(&mut self) {
        self.mode = Mode::Paused;
        self.paused_prefix = self.highest_applied;
        self.resume_barrier = self.highest_applied;
    }

    fn barrier_durable(&self) -> bool {
        self.predicates
            .iter()
            .all(|p| p.durable_through.0 >= self.resume_barrier.0)
    }

    /// `(allow, reason)`: the part of [`AdmissionState`] whose change is an admission edge.
    ///
    /// Whenever `blocked` is set the reason is the block's own client answer,
    /// [`BlockReason::client_error_kind`], which is total and answers
    /// `DivergenceRequiresOperator` for every reason (design §4.5, lead rulings B-R38 and B-R42
    /// A5): a block never reads as "retry later". L1 asks the contract rather than hard-coding
    /// the code, so a reason that ever needs a different answer is decided there.
    fn verdict(&self) -> (bool, Option<ErrorKind>) {
        match (self.mode, &self.blocked) {
            (Mode::Healthy | Mode::Warn, _) => (true, None),
            (Mode::Paused | Mode::Reprotecting { .. }, Some(reason)) => {
                (false, Some(reason.client_error_kind()))
            }
            (Mode::Paused | Mode::Reprotecting { .. }, None) => {
                (false, Some(ErrorKind::ProtectionPaused))
            }
        }
    }

    /// `SetAdmission` when the verdict moved since `before`, otherwise nothing.
    fn edge(&self, now: Tick, before: (bool, Option<ErrorKind>)) -> Option<KernelEffect> {
        (self.verdict() != before).then(|| KernelEffect::SetAdmission(self.admission_state(now)))
    }

    /// [`Self::edge`], otherwise exactly one `Ignored` (BA-2).
    fn edge_or(
        &self,
        now: Tick,
        before: (bool, Option<ErrorKind>),
        otherwise: ReplicaIgnoreReason,
    ) -> Vec<KernelEffect> {
        self.edge(now, before).map_or_else(
            || ignored(KernelIgnoredReason::Replica(otherwise)),
            |edge| vec![edge],
        )
    }

    /// Demotion, live to inert, is fail-closed (lead ruling B-R38): pause at the head and
    /// publish the reject when that is an admission edge. `None` when this node was already
    /// rejecting, so the last state it published already fails closed.
    pub(super) fn demote(mut self, now: Tick) -> Option<KernelEffect> {
        let before = self.verdict();
        self.pause_at_head();
        self.edge(now, before)
    }

    /// The oldest unsafe record and its age; `None` when the queue is empty, so an idle
    /// partition is never unsafe (design §4.2 `None => 0`).
    fn oldest(&self, now: Tick) -> Option<(Seq, u64)> {
        self.unsafe_queue
            .front()
            .map(|e| (e.seq, e.applied_at.millis_until(now)))
    }

    /// `predicates[0].copies - self - lost` (design §4.2).
    pub(super) fn lag_domain(&self) -> BTreeSet<CopyId> {
        self.predicates
            .first()
            .map(|p| {
                p.copies
                    .iter()
                    .filter(|c| **c != self.self_copy && !self.lost.contains(c))
                    .copied()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The max lag over the lag domain and the copy holding it. A peer never heard from has
    /// infinite lag and blocks resume. An empty domain is infinite lag with no stalest copy:
    /// nobody reports, so there is no evidence to resume on (design §4.2, lead ruling B-R38).
    fn replication_lag(&self, now: Tick) -> (ReplicationLag, Option<CopyId>) {
        self.lag_domain()
            .into_iter()
            .map(|copy| {
                let lag = self
                    .peer_progress
                    .get(&copy)
                    .map_or(ReplicationLag::INFINITE, |at| {
                        ReplicationLag::millis(at.millis_until(now))
                    });
                (lag, copy)
            })
            .max_by_key(|(lag, _)| *lag)
            .map_or((ReplicationLag::INFINITE, None), |(lag, copy)| {
                (lag, Some(copy))
            })
    }

    /// The current predicate's members by node, sorted. `config` is always the configuration
    /// the current predicate was pinned from: [`Self::pin`] replaces both together.
    pub(super) fn required_copy_set(&self) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = required_members(&self.config)
            .map(|member| member.node)
            .collect();
        nodes.sort_unstable();
        nodes
    }

    /// What L1 publishes (design §4.5).
    pub(super) fn admission_state(&self, now: Tick) -> AdmissionState {
        let (allow, reason) = self.verdict();
        let (oldest_unsafe_seq, oldest_unsafe_age) = self.oldest(now).unwrap_or((Seq::ZERO, 0));
        let (replication_lag, stalest_copy) = self.replication_lag(now);
        AdmissionState {
            allow,
            reason,
            oldest_unsafe_age,
            oldest_unsafe_seq,
            replication_lag,
            stalest_copy,
            lost_copies: self.lost.iter().copied().collect(),
            paused_prefix: self.paused_prefix,
            resume_barrier: self.resume_barrier,
            required_config_versions: self.predicates.iter().map(|p| p.config_version).collect(),
            outstanding_unsafe_bytes: self
                .unsafe_queue
                .iter()
                .fold(0, |sum, e| sum.saturating_add(e.bytes)),
        }
    }

    /// The earliest tick a health evaluation could change the state (design §4.7).
    pub(super) fn next_interesting_tick(&self) -> Option<Tick> {
        let front = self.unsafe_queue.front().map(|e| e.applied_at);
        match self.mode {
            Mode::Healthy => front.map(|t| t.plus_millis(self.budgets.warn_age_millis)),
            Mode::Warn => front.map(|t| t.plus_millis(self.budgets.pause_age_millis)),
            Mode::Reprotecting {
                below_since: Some(t),
            } => Some(t.plus_millis(self.budgets.resume_hold_millis)),
            Mode::Paused | Mode::Reprotecting { below_since: None } => None,
        }
    }

    pub(super) const fn mode(&self) -> Mode {
        self.mode
    }

    pub(super) const fn qualifies_now_at_head(&self) -> bool {
        self.qualifies_now_at_head
    }

    pub(super) const fn blocked(&self) -> Option<&BlockReason> {
        self.blocked.as_ref()
    }

    pub(super) fn unsafe_len(&self) -> usize {
        self.unsafe_queue.len()
    }

    pub(super) fn pending_durable_versions(&self) -> Vec<ConfigVersion> {
        self.pending_durable.keys().copied().collect()
    }
}
