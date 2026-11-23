//! The host clock: a [`Tick`] is UTC milliseconds, advanced by a monotonic clock.
//!
//! One clock for the whole process (Decision 1). It is sampled once at start, so a wall-clock
//! step after start never moves a tick backwards. A1 reads the same value as its UTC estimate
//! (`StepCtx::control_time`), with the budget's clock error as the bound (ADR-0007 §5
//! assumption 3; M12 qualifies it).
//!
//! Crate tests have a second mode, [`HostClock::capped`] (#50): the same clock, except that
//! one reading moves at most a set step past the last. A stall of the thread that steps the
//! hosts then costs them one step of their time, not the whole stall.
//!
//! # The clock guard (#80, S2c)
//!
//! `Tick` is one offset fixed at start, so two processes can disagree by any amount, and a
//! frozen-clock pause leaves one behind for the rest of its life. [`ClockGuard`] compares this
//! process's `Tick` with each peer's over the node links (`crate::net`): one [`ClockSample`] a
//! second per link. A sample's true offset lies within `theta +- (r/2 + 1)`, whatever the
//! asymmetry of the path: `r/2` for the path, and one more for the whole-millisecond readings
//! ([`TICK_RESOLUTION_MILLIS`], issue #114). So [`verdict`] can tell **Agree** from **Breach**
//! and calls everything between **Inconclusive**: delay and load widen the doubt and never make
//! a Breach.
//!
//! [`HostClock::sample`] is where a [`ControlTime`] now comes from. With no `Net` attached
//! it is always established, as it was before this guard. A node has a bound only while it,
//! with the peers whose fresh verdict is Agree, is a majority of the K-node cluster and
//! outnumbers the fresh Breaches. Otherwise A1 retracts the sample and fails closed. So delay
//! and load that leave every round Inconclusive for [`VERDICT_FRESH_MILLIS`] cost the bound. A node `Fenced` and behind a majority of the cluster moves its clock forward
//! ([`ClockGuard::reanchor_if_behind`]) and recovers with no restart.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::NodeId;
use rdb_core::contracts::time::{ControlTime, Tick};
use rdb_core::recovery::RecoveryPhase;

/// How long one peer's verdict counts after it was formed (design 7.1). A closed link stops
/// probing, so its verdict ages out at the same 5 s.
pub const VERDICT_FRESH_MILLIS: u64 = 5_000;

/// The clock error each link is held to. Production runs [`Budgets::SPEC_DEFAULTS`] for this
/// value on every node (the budgets are configuration, but the guard is process-wide and the
/// links have no `Budgets`). Two established nodes share an agreeing node, so they differ by
/// at most twice this, the 2-epsilon the promote proof needs.
pub const LINK_EPSILON_MILLIS: u64 = Budgets::SPEC_DEFAULTS.clock_error_millis;

/// A [`Tick`] is a whole millisecond, cut down from a finer clock, so one reading can sit up
/// to just under 1 ms behind the clock it reads. Of a round's three stamps, the doubt that adds
/// to `theta +- r/2` is under 1 ms either way: the true offset lies in
/// `(theta - r/2 - 1, theta + r/2 + 1)`. Issue #114: two clocks started inside different
/// milliseconds read 801 for a true offset under 801 with `r` 0, and a re-anchor by 801 put the
/// node past its peers.
pub const TICK_RESOLUTION_MILLIS: u64 = 1;

/// [`TICK_RESOLUTION_MILLIS`] on both sides of a round, in the doubled units the arithmetic
/// runs in.
const DOUBLED_RESOLUTION: u64 = 2 * TICK_RESOLUTION_MILLIS;

/// How one peer's clock compares with this process's, from one probe round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The true offset is provably within epsilon: `abs(theta) + r/2 + 1 <= epsilon`.
    Agree,
    /// The true offset is provably beyond epsilon: `abs(theta) - r/2 - 1 > epsilon`.
    Breach,
    /// Neither proved: this round changes nothing.
    Inconclusive,
}

/// One probe round, as stamped on the two clocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockSample {
    /// This side's clock when the probe left.
    pub sent: Tick,
    /// The peer's clock when it echoed.
    pub at: Tick,
    /// This side's clock when the echo arrived.
    pub received: Tick,
}

impl ClockSample {
    /// The round trip in milliseconds, `None` when `received` is before `sent`: a clock that
    /// was stepped under the probe, which is not a measurement.
    #[must_use]
    pub const fn round_trip_millis(&self) -> Option<u64> {
        self.received.0.checked_sub(self.sent.0)
    }

    /// Twice the offset estimate `theta = at - (sent + received) / 2`: the peer's clock minus
    /// this one's. Doubled so the division by two loses no millisecond.
    #[must_use]
    pub fn doubled_offset(&self) -> i128 {
        2 * i128::from(self.at.0) - i128::from(self.sent.0) - i128::from(self.received.0)
    }
}

/// Judge one round. `doubled_offset` is twice `theta`; `round_trip` is `r`. `theta +- (r/2 +
/// 1)` bounds the true offset ([`TICK_RESOLUTION_MILLIS`]), so Agree and Breach are proofs and
/// the middle changes nothing.
#[must_use]
pub const fn verdict(doubled_offset: i128, round_trip: u64, epsilon: u64) -> Verdict {
    let abs = doubled_offset.unsigned_abs();
    let r = round_trip as u128 + DOUBLED_RESOLUTION as u128;
    let two_epsilon = (epsilon as u128) * 2;
    if abs + r <= two_epsilon {
        Verdict::Agree
    } else if abs > two_epsilon + r {
        Verdict::Breach
    } else {
        Verdict::Inconclusive
    }
}

/// A peer's last conclusive verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PeerVerdict {
    verdict: Verdict,
    /// Twice the round's `theta`.
    doubled_offset: i128,
    /// The round's `r`.
    round_trip: u64,
    /// When it was formed, in [`ClockGuard`]'s monotonic milliseconds.
    formed_at: u64,
}

impl PeerVerdict {
    const fn fresh(&self, now: u64) -> bool {
        now.saturating_sub(self.formed_at) <= VERDICT_FRESH_MILLIS
    }

    /// Twice the least the peer is proved ahead of this clock: `theta - r/2 - 1`, doubled.
    fn doubled_lower_bound(&self) -> i128 {
        self.doubled_offset - i128::from(self.round_trip) - i128::from(DOUBLED_RESOLUTION)
    }

    /// The peer is proved ahead of this clock by more than `epsilon`.
    fn ahead(&self, epsilon: u64) -> bool {
        self.doubled_lower_bound() > 2 * i128::from(epsilon)
    }

    /// The most this clock may be moved forward on this peer's word: `theta - r/2 - 1`, rounded
    /// down, which the true offset exceeds ([`TICK_RESOLUTION_MILLIS`]). Only meaningful for a
    /// peer [`Self::ahead`].
    fn proven_ahead_millis(&self) -> u64 {
        u64::try_from(self.doubled_lower_bound() / 2).unwrap_or(0)
    }
}

/// What a re-anchor did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reanchor {
    /// Milliseconds the clock moved forward.
    pub by_millis: u64,
    /// The peers it moved on the word of, in id order.
    pub peers: Vec<NodeId>,
}

/// The guard's verdict table: a pure value, with time as a parameter, so rows can drive it.
#[derive(Debug, Default)]
pub(crate) struct Verdicts {
    /// At least one Agree since start, or since the last re-anchor.
    agreed: bool,
    peers: BTreeMap<NodeId, PeerVerdict>,
    /// A loss has been reported and not yet cleared, so it is logged once.
    reported_lost: bool,
}

impl Verdicts {
    /// Fold one round in. Inconclusive and nonsense rounds change nothing.
    fn record(
        &mut self,
        peer: NodeId,
        sample: ClockSample,
        epsilon: u64,
        now: u64,
    ) -> Option<Verdict> {
        let round_trip = sample.round_trip_millis()?;
        let doubled_offset = sample.doubled_offset();
        let verdict = verdict(doubled_offset, round_trip, epsilon);
        if verdict == Verdict::Inconclusive {
            return Some(Verdict::Inconclusive);
        }
        if verdict == Verdict::Agree {
            self.agreed = true;
        }
        self.peers.insert(
            peer,
            PeerVerdict {
                verdict,
                doubled_offset,
                round_trip,
                formed_at: now,
            },
        );
        Some(verdict)
    }

    fn fresh(&self, verdict: Verdict, now: u64) -> impl Iterator<Item = (NodeId, &PeerVerdict)> {
        self.peers
            .iter()
            .filter(move |(_, seen)| seen.verdict == verdict && seen.fresh(now))
            .map(|(node, seen)| (*node, seen))
    }

    /// The node's bound in a cluster of `k`: an Agree since start, and counting this node as one
    /// agreement, fresh Agrees are at least `floor(k/2)+1` and outnumber fresh Breaches.
    ///
    /// The majority is of the fixed membership, not of the verdicts that happen to be fresh
    /// (PR #103 review B1). Without it, a minority whose clocks agree with each other regains
    /// a bound once a partition lets the other side's Breaches age out, and two established
    /// nodes need no longer share an agreeing node: the 2-epsilon the promote proof rests on is
    /// gone. Two majorities always meet. The cost is that a node which has heard no fresh
    /// Agree from a majority for [`VERDICT_FRESH_MILLIS`] has no bound, however few Breaches
    /// stand against it: it cannot show its clock is within epsilon, so A1 fails closed.
    fn established(&self, k: usize, now: u64) -> bool {
        let agrees = self.fresh(Verdict::Agree, now).count() + 1;
        let breaches = self.fresh(Verdict::Breach, now).count();
        self.agreed && agrees > breaches && agrees > k / 2
    }

    /// Whether any fresh Breach stands against this node.
    fn any_breach(&self, now: u64) -> bool {
        self.fresh(Verdict::Breach, now).next().is_some()
    }

    /// Re-anchor, if and only if the bound is lost and at least `floor(K/2)+1` of the K-node
    /// cluster are proved ahead: forward by the smallest proven lower bound among them. Those
    /// verdicts are cleared and the Agree latch reset, so the bound stays lost until a new
    /// Agree. The rule for which K applies is the caller's: the guard reads it as one number
    /// (ruling R8-7 for the union that must land before S4's add-node).
    fn reanchor(&mut self, k: usize, epsilon: u64, now: u64) -> Option<Reanchor> {
        if self.established(k, now) {
            return None;
        }
        let ahead: Vec<(NodeId, u64)> = self
            .fresh(Verdict::Breach, now)
            .filter(|(_, seen)| seen.ahead(epsilon))
            .map(|(node, seen)| (node, seen.proven_ahead_millis()))
            .collect();
        if ahead.len() < k / 2 + 1 {
            return None;
        }
        let by_millis = ahead.iter().map(|(_, millis)| *millis).min()?;
        for (node, _) in &ahead {
            self.peers.remove(node);
        }
        self.agreed = false;
        Some(Reanchor {
            by_millis,
            peers: ahead.into_iter().map(|(node, _)| node).collect(),
        })
    }

    /// The fresh verdicts, for the operator's line: peers and offsets in milliseconds.
    fn summary(&self, verdict: Verdict, now: u64) -> (Vec<u32>, Vec<i64>) {
        self.fresh(verdict, now)
            .map(|(node, seen)| {
                (
                    node.0,
                    i64::try_from(seen.doubled_offset / 2).unwrap_or(i64::MAX),
                )
            })
            .unzip()
    }
}

/// A re-anchor waits while a recovery on this node has not heard every copy it waits for
/// (design 7.1, R6-6, R7-4): the deadline of `Collecting` is judged on the clock a step would
/// move, and could drop a silent copy early. Later phases have deadlines that retry.
#[must_use]
pub fn reanchor_deferred(phases: impl IntoIterator<Item = RecoveryPhase>) -> bool {
    phases
        .into_iter()
        .any(|phase| matches!(phase, RecoveryPhase::Fenced | RecoveryPhase::Collecting))
}

/// The process's clock guard, leaked once per [`crate::transport::Links`]. Every
/// [`HostClock`] copy of that process shares it. A crate test can also make one apart from
/// `Links` ([`HostClock::guarded`], [`HostClock::with_guard`]), shared by that clock's copies.
#[derive(Debug)]
pub struct ClockGuard {
    /// Milliseconds a re-anchor moved the clock forward; only ever rises.
    forward_ms: AtomicU64,
    /// Milliseconds a dev hook stepped the clock back (`rdb_node --clock-step-file`).
    back_ms: AtomicU64,
    /// A `Net` is attached, so verdicts count. Without one the bound is always established.
    attached: AtomicBool,
    /// K: the cluster size the re-anchor's majority is taken of (`Net::start`, from the
    /// boot peer list; ruling R8-7's union is not built here).
    cluster: AtomicUsize,
    /// The origin of the monotonic milliseconds the verdict table runs on.
    origin: Instant,
    verdicts: Mutex<Verdicts>,
    /// The live members whose verdicts count (#80 S4, B-2), as the attached `Net` last set
    /// them. `None`: every peer counts, as before membership could change.
    members: Mutex<Option<BTreeSet<NodeId>>>,
}

impl ClockGuard {
    fn new() -> Self {
        Self {
            forward_ms: AtomicU64::new(0),
            back_ms: AtomicU64::new(0),
            attached: AtomicBool::new(false),
            cluster: AtomicUsize::new(0),
            origin: Instant::now(),
            verdicts: Mutex::new(Verdicts::default()),
            members: Mutex::new(None),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Verdicts> {
        self.verdicts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn k(&self) -> usize {
        self.cluster.load(Ordering::Relaxed)
    }

    fn mono(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Verdicts start to count: a `Net` is attached to this process, in a cluster of `k` nodes.
    pub(crate) fn attach(&self, k: usize) {
        self.cluster.store(k, Ordering::Relaxed);
        self.attached.store(true, Ordering::Relaxed);
    }

    /// Count verdicts only from `members` from now on (#80 S4, B-2): a non-member's round is
    /// ignored by [`Self::observe`], and a verdict already held for one is dropped, so a node
    /// that is no live member never counts toward [`Self::established`]. K is
    /// [`Self::attach`]'s, which `Net::set_members` sets beside this.
    pub(crate) fn set_members(&self, members: &BTreeSet<NodeId>) {
        let mut verdicts = self.lock();
        verdicts.peers.retain(|peer, _| members.contains(peer));
        *self
            .members
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(members.clone());
    }

    fn counts(&self, peer: NodeId) -> bool {
        self.members
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_none_or(|members| members.contains(&peer))
    }

    /// The control row's "guard off": as if no `Net` were attached (13e).
    #[cfg(test)]
    pub(crate) fn disable(&self) {
        self.attached.store(false, Ordering::Relaxed);
    }

    /// K, for rows (#80 S4).
    #[cfg(test)]
    pub(crate) fn cluster_k(&self) -> usize {
        self.k()
    }

    /// The last conclusive verdict on `peer`, fresh or not, for rows.
    #[cfg(test)]
    pub(crate) fn verdict_of(&self, peer: NodeId) -> Option<Verdict> {
        self.lock().peers.get(&peer).map(|seen| seen.verdict)
    }

    /// Fold one peer's round in, and report a change of the bound.
    /// Membership is read under the verdict lock (verdicts, then members, as
    /// [`Self::set_members`] takes them), so a retirement between the read and the record
    /// cannot put the retired peer's verdict back (PR #244 review).
    pub(crate) fn observe(&self, peer: NodeId, sample: ClockSample) {
        #[cfg(test)]
        tests::before_verdict_lock();
        let now = self.mono();
        let mut verdicts = self.lock();
        if !self.counts(peer) {
            tracing::debug!(peer = peer.0, "clock_sample_not_a_member");
            return;
        }
        let before = verdicts.peers.get(&peer).map(|seen| seen.verdict);
        let judged = verdicts.record(peer, sample, LINK_EPSILON_MILLIS, now);
        if let Some(judged) = judged {
            let offset_ms = i64::try_from(sample.doubled_offset() / 2).unwrap_or(i64::MAX);
            let rtt_ms = sample.round_trip_millis().unwrap_or(0);
            if before != Some(judged) && judged != Verdict::Inconclusive {
                tracing::info!(peer = peer.0, verdict = ?judged, offset_ms, rtt_ms, "clock_verdict");
            } else {
                tracing::debug!(peer = peer.0, verdict = ?judged, offset_ms, rtt_ms, "clock_sample");
            }
        }
        Self::report(&mut verdicts, self.k(), now);
    }

    /// Re-judge the bound as verdicts age. Called from the links' probe tick.
    pub(crate) fn refresh(&self) {
        let now = self.mono();
        Self::report(&mut self.lock(), self.k(), now);
    }

    /// Log a change of the bound once, at error for a loss.
    fn report(verdicts: &mut Verdicts, k: usize, now: u64) {
        let established = verdicts.established(k, now);
        let lost = !established && (verdicts.agreed || verdicts.any_breach(now));
        if lost && !verdicts.reported_lost {
            verdicts.reported_lost = true;
            let (peers, offsets) = verdicts.summary(Verdict::Breach, now);
            tracing::error!(peers = ?peers, offsets = ?offsets, "clock_bound_lost");
        } else if established && verdicts.reported_lost {
            verdicts.reported_lost = false;
            tracing::info!("clock_bound_restored");
        }
    }

    /// Whether the bound holds now. Always true with no `Net` attached.
    #[must_use]
    pub fn established(&self) -> bool {
        !self.attached.load(Ordering::Relaxed) || self.lock().established(self.k(), self.mono())
    }

    /// Whether a loss of the bound has been reported and has not ended: the operator's word,
    /// and the Host's cheap test before it weighs a re-anchor.
    #[must_use]
    pub fn lost(&self) -> bool {
        self.attached.load(Ordering::Relaxed) && self.lock().reported_lost
    }

    /// Move the clock forward if this node is behind a majority: see [`Verdicts::reanchor`].
    /// Only the Host calls it, only while the node is `Fenced` and no recovery defers it.
    /// Logged `clock_reanchored{by_ms, total_ms, peers}`.
    pub(crate) fn reanchor_if_behind(&self) -> Option<Reanchor> {
        let now = self.mono();
        let k = self.cluster.load(Ordering::Relaxed);
        let done = self.lock().reanchor(k, LINK_EPSILON_MILLIS, now)?;
        let total = self.forward_ms.fetch_add(done.by_millis, Ordering::Relaxed) + done.by_millis;
        let peers: Vec<u32> = done.peers.iter().map(|node| node.0).collect();
        tracing::warn!(
            by_ms = done.by_millis,
            total_ms = total,
            peers = ?peers,
            "clock_reanchored"
        );
        Some(done)
    }

    fn adjust(&self, tick: u64) -> u64 {
        tick.saturating_add(self.forward_ms.load(Ordering::Relaxed))
            .saturating_sub(self.back_ms.load(Ordering::Relaxed))
    }
}

/// UTC milliseconds at start plus monotonic time since.
#[derive(Debug, Clone, Copy)]
pub struct HostClock {
    start_utc_ms: u64,
    start: Instant,
    /// The capped mode's state, when this clock is one (crate tests only).
    #[cfg(test)]
    capped: Option<&'static Capped>,
    /// How far this copy of a capped clock reads behind the others (crate tests only).
    #[cfg(test)]
    lag_millis: u64,
    /// The clock's guard, when it has one: its process's, made once per `Links`, or, in a crate
    /// test, one of its own ([`Self::with_guard`]), shared only by this clock's copies.
    guard: Option<&'static ClockGuard>,
}

impl HostClock {
    /// Sample the wall clock once.
    ///
    /// # Panics
    ///
    /// When the system clock reads before 1970: no tick could be honest then.
    #[must_use]
    pub fn start() -> Self {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock reads after 1970");
        Self {
            start_utc_ms: u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX),
            start: Instant::now(),
            #[cfg(test)]
            capped: None,
            #[cfg(test)]
            lag_millis: 0,
            guard: None,
        }
    }

    /// The clock of a process that owns a [`ClockGuard`]: [`crate::transport::Links`] makes one.
    /// `offset_ms` shifts `Tick` once, for good: the dev hook behind `rdb_node --clock-offset-ms`,
    /// real skew in any container.
    #[must_use]
    pub(crate) fn guarded(offset_ms: i64) -> Self {
        let mut clock = Self::start();
        clock.start_utc_ms = clock.start_utc_ms.saturating_add_signed(offset_ms);
        clock.guard = Some(Box::leak(Box::new(ClockGuard::new())));
        clock
    }

    /// The guard of this clock, when it has one.
    #[must_use]
    pub const fn guard(&self) -> Option<&'static ClockGuard> {
        self.guard
    }

    /// Move `Tick` back by `millis`, once: the dev hook behind `rdb_node --clock-step-file`,
    /// standing for a frozen-clock pause. Does nothing on a clock with no guard. Logged
    /// `clock_stepped_back{ms}`.
    pub fn step_back(&self, millis: u64) {
        if let Some(guard) = self.guard {
            guard.back_ms.fetch_add(millis, Ordering::Relaxed);
            tracing::warn!(ms = millis, "clock_stepped_back");
        }
    }

    /// Move `Tick` forward by `millis`, for the row that needs a node ahead of its peers.
    #[cfg(test)]
    pub(crate) fn step_forward(&self, millis: u64) {
        if let Some(guard) = self.guard {
            guard.forward_ms.fetch_add(millis, Ordering::Relaxed);
        }
    }

    /// A clock whose readings each move at most `max_step_millis` past the last one (#50).
    /// Between readings closer than that it is [`Self::start`]'s clock. A longer gap, a stall
    /// of the reading thread, counts `max_step_millis` and is logged
    /// `test_clock_capped{real_millis, counted_millis}`. Copies share one state, so every host
    /// in a row reads one time, and without a guard it never moves backwards: a capped clock
    /// given one by [`Self::with_guard`] moves back when [`Self::step_back`] is called.
    ///
    /// The state is leaked, once per call, so the clock stays `Copy` as its production callers
    /// take it: a few bytes per test cluster.
    #[cfg(test)]
    pub(crate) fn capped(max_step_millis: u64) -> Self {
        let clock = Self::start();
        let capped: &'static Capped = Box::leak(Box::new(Capped {
            max_step_millis,
            read: Mutex::new((clock.start, 0)),
            held: AtomicBool::new(false),
        }));
        Self {
            capped: Some(capped),
            ..clock
        }
    }

    /// This capped clock, reading `millis` behind every other copy of it: a host whose `Tick`
    /// lags its peers' (issue #80, the frozen-tick condition of R4-1). Copies still share one
    /// state, so the lag stays exactly `millis`.
    ///
    /// # Panics
    ///
    /// On a clock that is not capped: the lag is applied only to a capped reading.
    #[cfg(test)]
    pub(crate) fn lagging(self, millis: u64) -> Self {
        assert!(self.capped.is_some(), "only a capped clock lags");
        Self {
            lag_millis: millis,
            ..self
        }
    }

    /// This capped clock with a guard of its own, so [`Self::step_back`] moves it while it is
    /// held: a rollback read at exact ticks (#84 review). Copies share the guard. The guard is
    /// leaked once per call, not once per `Links`, and a clock built this way can move
    /// backwards, unlike a capped clock without one.
    #[cfg(test)]
    pub(crate) fn with_guard(self) -> Self {
        Self {
            guard: Some(Box::leak(Box::new(ClockGuard::new()))),
            ..self
        }
    }

    /// Stop this capped clock, every copy of it, until the returned hold drops: readings in
    /// between all return the tick of the call, and the real time held counts nothing.
    ///
    /// For a row whose precondition is a deadline on the counted clock and whose claim needs
    /// one node's decision taken before it: under the hold, a check of the precondition and the
    /// decision read the same tick, so the check is the precondition at the decision.
    ///
    /// # Panics
    ///
    /// On a clock that is not capped, or one already held.
    #[cfg(test)]
    pub(crate) fn hold(&self) -> ClockHold {
        let capped = self.capped.expect("only a capped clock holds");
        capped.elapsed();
        assert!(
            !capped.held.swap(true, Ordering::SeqCst),
            "the clock is already held"
        );
        ClockHold { capped }
    }

    /// Now, as a tick.
    #[must_use]
    pub fn now(&self) -> Tick {
        #[cfg(test)]
        if let Some(capped) = self.capped {
            let counted = self.start_utc_ms.saturating_add(capped.elapsed());
            let counted = counted.saturating_sub(self.lag_millis);
            return Tick(self.guard.map_or(counted, |guard| guard.adjust(counted)));
        }
        let elapsed = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let tick = self.start_utc_ms.saturating_add(elapsed);
        Tick(self.guard.map_or(tick, |guard| guard.adjust(tick)))
    }

    /// The control-time sample A1 sees at `now`: the estimate is the tick itself.
    #[must_use]
    pub const fn control_time(now: Tick, budgets: &Budgets) -> ControlTime {
        ControlTime {
            estimate: now,
            error_millis: budgets.clock_error_millis,
            bound_established: true,
            sampled_at: now,
        }
    }

    /// The sample A1, a promote and a clear read at `now`: [`Self::control_time`], with the
    /// bound this process's [`ClockGuard`] reports. Always established with no `Net` attached.
    #[must_use]
    pub fn sample(&self, now: Tick, budgets: &Budgets) -> ControlTime {
        let established = self.guard.is_none_or(ClockGuard::established);
        ControlTime {
            bound_established: established,
            ..Self::control_time(now, budgets)
        }
    }

    /// Milliseconds from `now` until `at`, zero when `at` has passed.
    #[must_use]
    pub const fn millis_until(now: Tick, at: Tick) -> u64 {
        at.0.saturating_sub(now.0)
    }

    /// An `Instant` no later than the moment this clock first reads `at` (#123 review): the
    /// public form of a tick deadline. The `Instant` is read first and the tick after it, both
    /// fresh, so no time spent before the call is counted twice. A tick is a whole millisecond
    /// and the one read may be almost over, so one millisecond is taken off. A deadline already
    /// reached gives the `Instant` read, which is not in the future.
    #[must_use]
    pub fn instant_at(&self, at: Tick) -> Instant {
        let anchor = Instant::now();
        let now = self.now();
        Self::instant_from(anchor, now, at)
    }

    /// [`Self::instant_at`] from an `anchor` read no later than the tick `now` was.
    #[must_use]
    pub(crate) fn instant_from(anchor: Instant, now: Tick, at: Tick) -> Instant {
        let ahead = Self::millis_until(now, at).saturating_sub(1);
        anchor + Duration::from_millis(ahead)
    }
}

/// [`HostClock::capped`]'s shared state: the instant counted up to, and the milliseconds
/// counted since start.
#[cfg(test)]
#[derive(Debug)]
struct Capped {
    max_step_millis: u64,
    read: Mutex<(Instant, u64)>,
    /// Set while a [`ClockHold`] lives: readings count nothing.
    held: AtomicBool,
}

/// [`HostClock::hold`]'s guard: the clock counts again from the drop, the time held dropped.
#[cfg(test)]
#[derive(Debug)]
#[must_use = "the clock is held only while this lives"]
pub(crate) struct ClockHold {
    capped: &'static Capped,
}

#[cfg(test)]
impl ClockHold {
    /// Move every copy of the held clock forward by exactly `millis`, still held: a row that
    /// needs a boundary to the millisecond reads the tick it set, never one real time chose.
    pub(crate) fn advance(&self, millis: u64) {
        let mut read = self
            .capped
            .read
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        read.1 = read.1.saturating_add(millis);
    }
}

#[cfg(test)]
impl Drop for ClockHold {
    fn drop(&mut self) {
        let mut read = self
            .capped
            .read
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        read.0 = Instant::now();
        self.capped.held.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
impl Capped {
    /// Milliseconds counted since start, this reading included. Within the cap the count moves
    /// by whole milliseconds and keeps the remainder for the next reading, so readings closer
    /// than a millisecond still add up. Past the cap it moves by the cap and drops the rest.
    fn elapsed(&self) -> u64 {
        let mut read = self
            .read
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (counted_to, counted) = *read;
        let now = Instant::now();
        if self.held.load(Ordering::SeqCst) {
            read.0 = now;
            return counted;
        }
        let real = u64::try_from(now.saturating_duration_since(counted_to).as_millis())
            .unwrap_or(u64::MAX);
        let (to, step) = if real <= self.max_step_millis {
            (counted_to + std::time::Duration::from_millis(real), real)
        } else {
            tracing::info!(
                real_millis = real,
                counted_millis = self.max_step_millis,
                "test_clock_capped"
            );
            (now, self.max_step_millis)
        };
        *read = (to, counted.saturating_add(step));
        read.1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::time::Duration;

    thread_local! {
        /// Run once by the next [`ClockGuard::observe`] on this thread, before it takes the
        /// verdict lock: a row's way to force an interleaving.
        static BEFORE_VERDICT_LOCK: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }

    pub(super) fn before_verdict_lock() {
        if let Some(hook) = BEFORE_VERDICT_LOCK.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
    }

    /// PR #244 review (clock.rs): node 4 is retired while its observation is between entering
    /// `observe` and taking the verdict lock. The retirement wins: node 4 has no verdict and
    /// does not count, so 2 of 4 stays no majority. Unit.
    /// Cut: the membership check back before the hook, outside the verdict lock: node 4's
    /// Agree is recorded after its retirement ("a retired peer's Agree is not put back"), red.
    #[test]
    fn pr244_a_retirement_racing_an_observation_keeps_the_peer_out() {
        let agree = offset_by(0, 10);
        let guard = std::sync::Arc::new(ClockGuard::new());
        let five: BTreeSet<NodeId> = (1..=5).map(NodeId).collect();
        guard.set_members(&five);
        guard.attach(five.len());
        guard.observe(NodeId(2), agree);
        let retire = std::sync::Arc::clone(&guard);
        BEFORE_VERDICT_LOCK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let four: BTreeSet<NodeId> = [1, 2, 3, 5].into_iter().map(NodeId).collect();
                retire.set_members(&four);
                retire.attach(four.len());
            }));
        });
        guard.observe(NodeId(4), agree);
        assert_eq!(
            guard.verdict_of(NodeId(4)),
            None,
            "a retired peer's Agree is not put back"
        );
        assert!(!guard.established(), "2 of 4 is no majority");
    }

    /// #123 review: a tick deadline's `Instant` is never later than the moment the clock first
    /// reads that tick, however long ago the caller read the clock, and a deadline at or before
    /// the tick read is not in the future. Exact on synthetic readings, then on a real clock in
    /// three cases, each checked by [`converts_no_later_than_the_tick`] against the `Instant`s
    /// read around the conversion, so a scheduling pause cannot fail correct code: the clock read
    /// 60 ms before the conversion; read 400 ms before, past the 300 ms deadline (an injected
    /// pause past the tick, which the old fixed-tick check failed); and a 50 ms pause between
    /// the conversion's `Instant` and its tick (injected through `instant_from`). Cuts: no
    /// millisecond off, red at "ahead"; the tick delta counted from a reading 60 ms old, red at
    /// "a stale reading". Unit, ~0.5 s.
    #[test]
    fn a_tick_deadline_converts_to_an_instant_no_later_than_the_tick() {
        let anchor = Instant::now();
        let at =
            |now: u64, deadline: u64| HostClock::instant_from(anchor, Tick(now), Tick(deadline));
        assert_eq!(at(100, 400), anchor + Duration::from_millis(299), "ahead");
        assert_eq!(at(100, 101), anchor, "the next tick may begin at once");
        assert_eq!(at(100, 100), anchor, "the tick read");
        assert_eq!(
            at(100, 40),
            anchor,
            "a passed deadline is not in the future"
        );

        let clock = HostClock::start();
        for pause in [60, 400] {
            let read_earlier = clock.now();
            std::thread::sleep(Duration::from_millis(pause));
            let deadline = Tick(read_earlier.0 + 300);
            converts_no_later_than_the_tick(&clock, deadline, || clock.instant_at(deadline));
            let passed = clock.instant_at(read_earlier);
            assert!(
                passed <= Instant::now(),
                "a passed deadline is not in the future"
            );
        }
        let deadline = clock.now().plus_millis(300);
        converts_no_later_than_the_tick(&clock, deadline, || {
            let anchor = Instant::now();
            std::thread::sleep(Duration::from_millis(50));
            HostClock::instant_from(anchor, clock.now(), deadline)
        });
    }

    /// Check `convert`'s `Instant` for `deadline` against the `Instant`s read just before and
    /// just after it, never against a fixed tick, so a pause anywhere fails only wrong code.
    /// The answer is no later than the moment `deadline`'s tick begins, or than the conversion's
    /// end when the tick had begun by then (a deadline already reached converts to the `Instant`
    /// read). It is earlier than that beginning only by the time the conversion took (a pause
    /// between its `Instant` and its tick reading), the millisecond taken off and one of
    /// rounding; or, when the tick may have begun during the conversion, it is no earlier than
    /// the conversion's start.
    fn converts_no_later_than_the_tick(
        clock: &HostClock,
        deadline: Tick,
        convert: impl FnOnce() -> Instant,
    ) {
        let begins = clock.start + Duration::from_millis(deadline.0 - clock.start_utc_ms);
        let before = Instant::now();
        let got = convert();
        let after = Instant::now();
        assert!(
            got <= begins.max(after),
            "a stale reading: {:?} after the tick begins",
            got.saturating_duration_since(begins)
        );
        let slack = after.duration_since(before) + Duration::from_millis(2);
        let floor = if after < begins {
            begins - slack
        } else {
            before
        };
        assert!(
            got >= floor,
            "and not far ahead of it: {:?} before the tick begins, more than the {slack:?} the \
             conversion took",
            begins.saturating_duration_since(got)
        );
    }

    /// #50: a stall between two readings counts the cap, and readings far closer together than
    /// a millisecond still add up to the time that passed. Cuts: count the whole gap, red on
    /// the stall; drop the sub-millisecond remainder, red on the close readings.
    /// Unit, ~0.2 s.
    #[test]
    fn a_capped_clock_counts_a_stall_as_one_step_and_close_readings_in_full() {
        let clock = HostClock::capped(20);
        let before = clock.now();
        std::thread::sleep(Duration::from_millis(120));
        let after = clock.now();
        assert_eq!(
            after.0 - before.0,
            20,
            "a 120 ms stall counts the 20 ms cap"
        );

        // A copy reads the same state. Half the real time is the floor, so a scheduler stall
        // inside the loop (clipped to the cap) cannot fail the row; the cut counts ~0.
        let copy = clock;
        let start = clock.now();
        let began = Instant::now();
        let mut last = start;
        while began.elapsed() < Duration::from_millis(60) {
            let now = copy.now();
            assert!(now >= last, "never backwards");
            last = now;
        }
        let counted = clock.now().0 - start.0;
        let real = u64::try_from(began.elapsed().as_millis()).unwrap_or(u64::MAX);
        assert!(
            counted >= real / 2 && counted <= real + 1,
            "{real} ms of close readings count {counted} ms"
        );
    }

    /// A held clock reads one tick on every copy, a lagging one included, and counts none of
    /// the time held once released. Cuts: ignore the hold, red on the first comparison; count
    /// the held time on release, red on the last.
    /// Unit, ~0.1 s.
    #[test]
    fn a_held_clock_stands_still_on_every_copy_and_counts_nothing_held() {
        let clock = HostClock::capped(20);
        let behind = clock.lagging(1_000);
        let hold = clock.hold();
        let held = clock.now();
        std::thread::sleep(Duration::from_millis(15));
        assert_eq!(clock.now(), held, "a held clock stands still");
        assert_eq!(
            behind.now().0,
            held.0 - 1_000,
            "and so does its lagging copy"
        );
        std::thread::sleep(Duration::from_millis(15));
        drop(hold);
        let after = clock.now();
        // 12 ms of slack for the release itself: a drop that kept the last held reading as
        // the count's origin still counts the 15 ms slept after it, so that cut stays red.
        assert!(
            after.0 - held.0 <= 12,
            "30 ms held counted {} ms",
            after.0 - held.0
        );
    }

    const EPS: u64 = 100;

    fn round(sent: u64, at: u64, received: u64) -> ClockSample {
        ClockSample {
            sent: Tick(sent),
            at: Tick(at),
            received: Tick(received),
        }
    }

    /// A round on a peer whose true offset is `offset`, with `r` split evenly.
    fn offset_by(offset: i64, r: u64) -> ClockSample {
        let sent = 1_000_000_u64;
        let at = (sent + r / 2).checked_add_signed(offset).expect("in range");
        round(sent, at, sent + r)
    }

    fn table(rounds: &[(u32, ClockSample)], now: u64) -> Verdicts {
        let mut verdicts = Verdicts::default();
        for (peer, sample) in rounds {
            verdicts.record(NodeId(*peer), *sample, EPS, now);
        }
        verdicts
    }

    /// #80 S4 M9, the grow half and the member filter (B-2): node 1's guard over members 1 to 5
    /// (K = 5) is not established on one Agree (2 of 5 with itself), and a non-member's Agree
    /// neither counts nor is kept, so it stays unestablished; a second member's Agree
    /// establishes it (3 of 5). The members grow to 8 (K = 8): 3 of 8 is no majority, and two
    /// Agrees from members old and new (4, then 6, which joined) restore it at 5 of 8. Unit.
    /// The shrink half (K falls as members are removed) is T11b's.
    /// Cut: `observe` counts non-members: node 9's Agree is kept ("a non-member is not kept":
    /// left `Some(Agree)`), red, observed 2026-11-20.
    #[test]
    fn s80s4_clock_guard_k_follows_live_members() {
        let agree = offset_by(0, 10);
        let guard = ClockGuard::new();
        let five: BTreeSet<NodeId> = (1..=5).map(NodeId).collect();
        guard.set_members(&five);
        guard.attach(five.len());
        guard.observe(NodeId(2), agree);
        assert!(!guard.established(), "2 of 5 is no majority");
        guard.observe(NodeId(9), agree);
        assert_eq!(
            guard.verdict_of(NodeId(9)),
            None,
            "a non-member is not kept"
        );
        assert!(!guard.established(), "a non-member's Agree does not count");
        guard.observe(NodeId(3), agree);
        assert!(guard.established(), "3 of 5");

        let eight: BTreeSet<NodeId> = (1..=8).map(NodeId).collect();
        guard.set_members(&eight);
        guard.attach(eight.len());
        assert_eq!(guard.cluster_k(), 8);
        assert!(!guard.established(), "3 of 8 is no majority");
        guard.observe(NodeId(4), agree);
        assert!(!guard.established(), "4 of 8 is no majority");
        guard.observe(NodeId(6), agree);
        assert!(guard.established(), "5 of 8, node 6 counting once a member");
    }

    /// 13a: the verdict arithmetic at its edges, and the bound rule. Cuts: `<=` to `<` on Agree,
    /// red at the edge; drop the Agree latch from `established`, red below.
    #[test]
    fn row_13a_verdict_edges_and_the_bound_rule() {
        // |theta| + r/2 + 1 == epsilon is the last Agree; one more is no longer proved. The 1 is
        // the readings' resolution (#114).
        assert_eq!(verdict(2 * 79, 40, EPS), Verdict::Agree);
        assert_eq!(verdict(-2 * 79, 40, EPS), Verdict::Agree);
        assert_eq!(verdict(2 * 80, 40, EPS), Verdict::Inconclusive);
        // |theta| - r/2 - 1 == epsilon is not yet a Breach; one more is.
        assert_eq!(verdict(2 * 121, 40, EPS), Verdict::Inconclusive);
        assert_eq!(verdict(2 * 122, 40, EPS), Verdict::Breach);
        assert_eq!(verdict(-2 * 122, 40, EPS), Verdict::Breach);
        // An odd r: the doubled form loses nothing to the division.
        assert_eq!(verdict(2 * 79, 41, EPS), Verdict::Inconclusive);
        // Stamps: the offset is at minus the midpoint, and a backward round is no measurement.
        assert_eq!(round(100, 160, 120).doubled_offset(), 2 * 50);
        assert_eq!(round(120, 100, 100).round_trip_millis(), None);
        assert_eq!(
            Verdicts::default().record(NodeId(2), round(120, 100, 100), EPS, 0),
            None
        );

        // No Agree yet: no bound, however few the Breaches.
        assert!(!Verdicts::default().established(3, 0));
        // One Agree, nothing against it.
        assert!(table(&[(2, offset_by(0, 10))], 0).established(3, 0));
        // K=3, one Agree one Breach: 2 > 1.
        let split = table(&[(2, offset_by(0, 10)), (3, offset_by(500, 10))], 0);
        assert!(split.established(3, 0));
        // Two Breaches outvote self and an Agree that has aged out.
        let mut lost = table(&[(2, offset_by(0, 10))], 0);
        lost.record(NodeId(3), offset_by(500, 10), EPS, 5_000);
        lost.record(NodeId(4), offset_by(500, 10), EPS, 5_000);
        assert!(
            !lost.established(3, 5_001),
            "the Agree has aged out, 1 > 2 is false"
        );
        // A Breach ages out too, but the latch alone is no bound: self is one of three, not a
        // majority (PR #103 review B1). A fresh Agree from either peer brings it back.
        assert!(!lost.any_breach(10_001));
        assert!(!lost.established(3, 10_001));
        lost.record(NodeId(2), offset_by(0, 10), EPS, 10_001);
        assert!(lost.established(3, 10_001));
        // The peer's table never counts a peer twice: its newest verdict replaces the last.
        let mut once = table(&[(2, offset_by(500, 10))], 0);
        once.record(NodeId(2), offset_by(0, 10), EPS, 1);
        assert_eq!(once.fresh(Verdict::Breach, 1).count(), 0);
    }

    /// PR #103 review (B1): K=5, nodes 1 and 2 are 500 ms fast and agree with each other; 3, 4
    /// and 5 are honest. Each side Breaches the other, then a partition between the groups
    /// stops those rounds and the cross-group Breaches age out. The fast pair must not get a
    /// bound back on the strength of each other and the latch: two of five is no majority. The
    /// honest three still are one. Cut: drop the majority from `established`, red on the pair.
    #[test]
    fn b1_a_minority_clock_group_cannot_establish_once_cross_group_breaches_age_out() {
        const K: usize = 5;
        // Node 1's table: node 2 agrees, the honest three are 500 ms behind it.
        let mut fast = table(
            &[
                (2, offset_by(0, 10)),
                (3, offset_by(-500, 10)),
                (4, offset_by(-500, 10)),
                (5, offset_by(-500, 10)),
            ],
            0,
        );
        // Node 3's table: 4 and 5 agree, the fast pair is 500 ms ahead.
        let mut honest = table(
            &[
                (1, offset_by(500, 10)),
                (2, offset_by(500, 10)),
                (4, offset_by(0, 10)),
                (5, offset_by(0, 10)),
            ],
            0,
        );
        assert!(!fast.established(K, 0), "three Breaches outvote two");
        assert!(honest.established(K, 0));

        // The partition: only rounds inside each group go on.
        let later = VERDICT_FRESH_MILLIS + 500;
        fast.record(NodeId(2), offset_by(0, 10), EPS, later);
        honest.record(NodeId(4), offset_by(0, 10), EPS, later);
        honest.record(NodeId(5), offset_by(0, 10), EPS, later);
        assert!(!fast.any_breach(later), "the cross-group Breaches aged out");
        assert!(
            !fast.established(K, later),
            "two fast nodes are no majority of five"
        );
        assert!(honest.established(K, later), "three honest ones are");
    }

    /// Issue #114: the proof holds for clocks read in whole milliseconds. Two clocks run at
    /// one rate, a true `offset` apart in microseconds, each started at some point inside a
    /// millisecond; a round reads each stamp cut down to its millisecond. For every phase of
    /// the two clocks and of the round's three instants, an Agree is within epsilon, a Breach
    /// is beyond it, and a peer proved ahead is ahead by at least the amount a re-anchor
    /// takes. The issue's round, true offset 800.3 ms read as 801 with `r` 0, is among them.
    /// Cut: [`TICK_RESOLUTION_MILLIS`] to 0, red (a re-anchor of 801 for a true 800.3).
    #[test]
    fn issue_114_the_proof_holds_for_whole_millisecond_readings() {
        let read = |micros: i64| u64::try_from(micros.div_euclid(1_000)).expect("positive");
        let phases = (0..1_000).step_by(100).chain([1, 999]);
        for offset in [
            800_300, 800_000, 799_700, 100_000, -100_000, 101_000, 99_000, 0, 1_500,
        ] {
            for here in phases.clone() {
                let there = here + offset;
                for (out, back) in [(0, 0), (0, 400), (300, 300), (999, 1), (2_000, 3_000)] {
                    for start in [10_000_000, 10_000_250, 10_000_999] {
                        let echoed = start + out;
                        let sample = round(
                            read(start + here),
                            read(echoed + there),
                            read(echoed + back + here),
                        );
                        let r = sample.round_trip_millis().expect("forward");
                        let doubled_offset = sample.doubled_offset();
                        let what = format!("offset {offset} us, phase {here}, {sample:?}");
                        let eps = i64::try_from(EPS).expect("small") * 1_000;
                        match verdict(doubled_offset, r, EPS) {
                            Verdict::Agree => assert!(offset.abs() <= eps, "Agree: {what}"),
                            Verdict::Breach => assert!(offset.abs() > eps, "Breach: {what}"),
                            Verdict::Inconclusive => {}
                        }
                        let seen = PeerVerdict {
                            verdict: Verdict::Breach,
                            doubled_offset,
                            round_trip: r,
                            formed_at: 0,
                        };
                        if seen.ahead(EPS) {
                            let by = i64::try_from(seen.proven_ahead_millis()).expect("small");
                            assert!(by * 1_000 <= offset, "a re-anchor by {by} ms: {what}");
                        }
                    }
                }
            }
        }
    }

    /// 13b: a round whose delay exceeds what it could prove is Inconclusive, at any offset the
    /// delay can explain, and changes neither the table nor the latch. Cut: drop `r` from the
    /// Breach test, red.
    #[test]
    fn row_13b_a_large_round_trip_never_makes_a_breach() {
        for r in [201_u64, 500, 2_000, 30_000] {
            for offset in [-(r as i64) / 2, -1, 0, 1, r as i64 / 2] {
                let sample = offset_by(offset, r);
                let v = verdict(sample.doubled_offset(), r, EPS);
                assert_ne!(v, Verdict::Breach, "r={r} offset={offset}");
                assert_ne!(v, Verdict::Agree, "r={r} offset={offset}");
            }
        }
        let verdicts = table(&[(2, offset_by(40, 2_000))], 0);
        assert!(verdicts.peers.is_empty());
        assert!(!verdicts.agreed);
    }

    /// 13g: the re-anchor rule. Cuts: `k / 2 + 1` to `k / 2`, red on the minority cases; `max`
    /// for `min`, red on the amount; do not reset the latch, red on "stays lost".
    #[test]
    fn row_13g_reanchor_needs_a_majority_ahead_and_takes_the_least() {
        // K=3: two peers ahead, by the smallest lower bound.
        let mut three = table(&[(2, offset_by(900, 20)), (3, offset_by(700, 40))], 0);
        let done = three.reanchor(3, EPS, 0).expect("two of three ahead");
        assert_eq!(done.by_millis, 679, "theta - r/2 - 1 of the nearer peer");
        assert_eq!(done.peers, [NodeId(2), NodeId(3)]);
        assert!(three.peers.is_empty(), "those verdicts are cleared");
        assert!(!three.agreed, "the latch is reset");
        assert!(
            !three.established(3, 1),
            "the bound stays lost until a new Agree"
        );
        assert_eq!(three.reanchor(3, EPS, 1), None, "and it cannot fire twice");
        three.record(NodeId(2), offset_by(0, 10), EPS, 2);
        assert!(three.established(3, 2), "a new Agree restores it");

        // A node that had an Agree before it fell behind: the re-anchor clears that latch too,
        // or the empty table would read as a bound with nothing behind it.
        let mut latched = table(&[(2, offset_by(0, 10))], 0);
        latched.record(NodeId(2), offset_by(800, 10), EPS, 1);
        latched.record(NodeId(3), offset_by(800, 10), EPS, 1);
        assert!(latched.agreed && !latched.established(3, 1));
        latched.reanchor(3, EPS, 1).expect("two of three ahead");
        assert!(!latched.established(3, 1), "no Agree since the move");
        latched.record(NodeId(2), offset_by(40, 2_000), EPS, 2);
        assert!(
            !latched.established(3, 2),
            "an Inconclusive round is no Agree"
        );

        // K=5: three ahead are enough, two are not.
        let ahead3: Vec<_> = (2..=4).map(|n| (n, offset_by(800, 10))).collect();
        assert!(table(&ahead3, 0).reanchor(5, EPS, 0).is_some());
        let ahead2: Vec<_> = (2..=3).map(|n| (n, offset_by(800, 10))).collect();
        assert_eq!(table(&ahead2, 0).reanchor(5, EPS, 0), None);

        // K=5, node 1 cut from 2 and 3, with 4 and 5 ahead by a second: two is no majority, so
        // it does not move, however lost its bound.
        let cut = [(4, offset_by(1_000, 10)), (5, offset_by(1_000, 10))];
        let mut verdicts = table(&cut, 0);
        assert!(!verdicts.established(5, 0));
        assert_eq!(verdicts.reanchor(5, EPS, 0), None);

        // K is a parameter: the same two peers are a majority of 3 and not of 4.
        assert!(table(&cut, 0).reanchor(3, EPS, 0).is_some());
        assert_eq!(table(&cut, 0).reanchor(4, EPS, 0), None);

        // Peers behind: never backward.
        let behind = [(2, offset_by(-800, 10)), (3, offset_by(-800, 10))];
        assert_eq!(table(&behind, 0).reanchor(3, EPS, 0), None);

        // Agrees not outnumbered: the bound holds, nothing moves.
        let held = [(2, offset_by(0, 10)), (3, offset_by(800, 10))];
        assert_eq!(table(&held, 0).reanchor(3, EPS, 0), None);

        // A stale Breach is no evidence.
        let mut stale = table(&[(2, offset_by(800, 10)), (3, offset_by(800, 10))], 0);
        assert_eq!(stale.reanchor(3, EPS, VERDICT_FRESH_MILLIS + 1), None);

        // Deferred only by the two phases that have not heard every copy.
        assert!(reanchor_deferred([RecoveryPhase::Fenced]));
        assert!(reanchor_deferred([RecoveryPhase::Collecting]));
        assert!(!reanchor_deferred([]));
    }
}
