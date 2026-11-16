//! Ownership for apps (#84): the owner token of a partition this process leads, and a cheap
//! check that it still does, so an app can fence the side effects it makes outside rDB.
//!
//! The source of truth is the [`AuthorityView`] A1 already publishes to its host. Each host
//! keeps its newest view per partition in an [`OwnerViews`] that [`crate::Db`] reads on the
//! app's thread, with the host's own [`HostClock`]. The readers follow check 6, plus fences and
//! faults, and nothing else:
//!
//! - Check 6: [`owns_at`] is write admission's check 6 ([`view_admits`]), not a copy of it, so
//!   for clock reasons `still_owner` turns false at the same tick admission stops.
//! - Fences: a fence (frozen grant, revoked epoch, a boot) publishes a view whose horizon is the
//!   tick before it, and the host writes the map before it steps T1 again, so `still_owner` is
//!   false before rDB admits another write. That view is removed, not kept, so a clock stepped
//!   back cannot revive it. The host names it a fence's from the step that carries the fence (a
//!   node fence, wherever A1 sites it, or one scoped to the partition), so a fence at tick 0 is
//!   removed too, though its horizon saturates to the fence tick, which check 6 still admits.
//! - Faults: a host fault revokes the map for good, before any caller is answered with the
//!   fault.
//!
//! `true` means only that no other node can hold this token's partition now. It does not mean
//! the partition takes writes: checks 7 and 8 refuse writes under a view that still admits, a
//! frozen queue (check 7, which a read-only recovery also freezes) and a paused or blocked L1
//! (check 8).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use rdb_core::contracts::authority::{AuthorityView, Lineage};
use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::{Generation, NodeId, OwnerEpoch, PartitionId};
use rdb_core::contracts::time::Tick;
use rdb_core::transaction::{view_admits, ViewAdmits};

use crate::clock::HostClock;

/// rDB's fencing token for a partition: `(generation, owner_epoch)`, compared as a whole, in
/// that order. Never compare the epoch alone. Every recovery commit raises both, so a later
/// owner's token is always greater.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerToken {
    /// The partition's history incarnation.
    pub generation: Generation,
    /// The owner epoch inside it.
    pub owner_epoch: OwnerEpoch,
}

impl OwnerToken {
    /// The token of `lineage`. Crate-private: `Lineage` is a core type the app surface does not
    /// re-export (A-R190).
    #[must_use]
    pub(crate) const fn of(lineage: Lineage) -> Self {
        Self {
            generation: lineage.generation,
            owner_epoch: lineage.owner_epoch,
        }
    }

    /// The lineage this token names in `partition`. Crate-private, as [`Self::of`] is.
    #[must_use]
    pub(crate) const fn lineage(self, partition: PartitionId) -> Lineage {
        Lineage {
            partition,
            generation: self.generation,
            owner_epoch: self.owner_epoch,
        }
    }
}

impl std::fmt::Display for OwnerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.generation.0, self.owner_epoch.0)
    }
}

/// A partition a node in this process leads, as [`crate::Db::ownership`] read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ownership {
    /// The node that leads it.
    pub node: NodeId,
    /// The partition.
    pub partition: PartitionId,
    /// The token to stamp on every side effect made for it.
    pub token: OwnerToken,
    /// A lower bound on the time left until [`crate::Db::still_owner`] turns false, unless A1
    /// renews first, as write admission counts it: the whole milliseconds from the tick read to
    /// the view's last admitting tick, never counting the fraction of the tick already gone.
    /// So it reads zero on the last admitting tick. An effect that takes `d` to land needs
    /// `remaining > d`.
    pub remaining: Duration,
}

/// Whether `view` admits a write of `token`'s lineage at `now`: write admission's check 6.
#[must_use]
pub(crate) fn owns_at(view: &AuthorityView, now: Tick, token: OwnerToken) -> bool {
    view_admits(view, token.lineage(view.lineage.partition), now) == ViewAdmits::Admits
}

/// [`Ownership::remaining`] for a view admitting through `valid_through`, read at tick `now`.
///
/// A tick is a whole millisecond, so the real time read is anywhere in `[now, now + 1)` and the
/// view stops admitting at `valid_through + 1`. The time truly left is then more than
/// `valid_through - now` and at most one millisecond more. Only the lower end is safe to hand an
/// app: counting the tick in progress (`+ 1`) would promise up to 1 ms that may be gone.
#[must_use]
pub(crate) fn remaining_at(valid_through: Tick, now: Tick) -> Duration {
    Duration::from_millis(valid_through.0.saturating_sub(now.0))
}

/// One host's newest [`AuthorityView`] per partition, and the clock that host admits writes
/// by. The host writes it when A1 publishes a view; [`crate::Db`] reads it on an app thread.
/// A view that no longer admits at the tick it was published (a fence's: its horizon is the tick
/// before the fence) is removed instead of kept, so a clock stepped back later
/// (`HostClock::step_back`) cannot make it admit again. Any other view for a partition the node
/// no longer serves has expired, or was superseded, so it reads as not owned. A host fault
/// revokes the map for good ([`Self::revoke`]).
#[derive(Debug)]
pub(crate) struct OwnerViews {
    node: NodeId,
    state: RwLock<Published>,
}

/// What [`OwnerViews`] guards: the views and the host clock they are read by, written together.
#[derive(Debug)]
struct Published {
    /// The host's clock as of its last publication. A crate test may replace a host's clock
    /// (`HostClock::lagging`); the next view brings the new one.
    clock: HostClock,
    views: BTreeMap<PartitionId, AuthorityView>,
    /// The host faulted: it admits nothing again, so the map stays empty (#84 review).
    revoked: bool,
}

impl OwnerViews {
    /// An empty map for `node`, read by `clock`: the host's own.
    pub(crate) fn new(node: NodeId, clock: HostClock) -> Arc<Self> {
        Arc::new(Self {
            node,
            state: RwLock::new(Published {
                clock,
                views: BTreeMap::new(),
                revoked: false,
            }),
        })
    }

    /// A1 published `view` in the host step at tick `at`, on a host reading `clock`: it replaces
    /// the partition's last one, as `Host::authority_views` does. A fence's view (`fenced`: A1
    /// fenced the partition in that step) or one already past at `at` removes the partition's
    /// entry instead: it admits nothing from `at` on, and kept, a clock stepped back to its
    /// horizon would read it as admitting again. `fenced` is needed besides the horizon: a fence
    /// at tick 0 publishes a horizon that saturates to 0, the fence's own tick, which check 6
    /// still admits (writes stay frozen there by check 7). Ignored once the map is revoked.
    pub(crate) fn publish(&self, view: AuthorityView, clock: HostClock, at: Tick, fenced: bool) {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        if state.revoked {
            return;
        }
        state.clock = clock;
        if fenced || view.valid_through_tick < at {
            state.views.remove(&view.lineage.partition);
        } else {
            state.views.insert(view.lineage.partition, view);
        }
    }

    /// The host faulted: it admits nothing from now on, so it owns nothing, and no later view
    /// is kept. Called before the host answers any caller with the fault, so a caller that reads
    /// the fault and then asks `still_owner` already reads false.
    pub(crate) fn revoke(&self) {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        state.revoked = true;
        state.views.clear();
    }

    /// The node stopped: it admits nothing, so it owns nothing.
    pub(crate) fn clear(&self) {
        self.state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .views
            .clear();
    }

    /// Hold the map for reading until the returned guard drops: [`Self::revoke`] waits for it.
    /// For the rows that pin a fault's order, revoke before reply (#84 review).
    #[cfg(test)]
    pub(crate) fn hold_readers(&self) -> impl Sized + '_ {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// `partition`'s ownership, while this node's view admits now.
    pub(crate) fn ownership(&self, partition: PartitionId) -> Option<Ownership> {
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        let view = state.views.get(&partition)?;
        let now = state.clock.now();
        let token = OwnerToken::of(view.lineage);
        owns_at(view, now, token).then(|| Ownership {
            node: self.node,
            partition,
            token,
            remaining: remaining_at(view.valid_through_tick, now),
        })
    }

    /// Whether this node's view of `partition` admits `token` now.
    pub(crate) fn still_owner(&self, partition: PartitionId, token: OwnerToken) -> bool {
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        state
            .views
            .get(&partition)
            .is_some_and(|view| owns_at(view, state.clock.now(), token))
    }
}

// ------------------------------------------------------------------ #83: the ownership stream

/// How long before a partition's admission horizon its owner warns `Deactivating`, for a host
/// with `budgets` (#83, lead ruling on B1 item 8):
///
/// ```text
/// lead = max_sample_age - 2 x clock_sample_period      (spec: 2000 - 2 x 500 = 1000 ms)
///        and at most a quarter of the grant            (spec: 1500, so 1000 binds)
/// ```
///
/// **Why the clock settings.** `Authority::admission_horizon` stops at the last tick `utc_ok`
/// holds, and `effective_epsilon` calls a sample stale past `max_sample_age`. Every A1 step
/// takes a fresh local sample, A1 is stepped every `clock_sample_period`, and a store stall does
/// not stop it (the sample is local). So in steady state the horizon is the last sample +
/// `max_sample_age`, and the time left swings between 2000 and 1500 ms at the spec's budgets.
/// A lead of `max_sample_age - clock_sample_period` (1500) sits on that edge with one tick to
/// spare, and anything above it warns and recovers on every sample (measured: 1600 flaps). The
/// second `clock_sample_period` is the slack the ruling asks for: a sample late by up to one
/// whole period still sends nothing. If samples stop altogether, ownership really ends at the
/// horizon, and the warning comes `lead` before it.
///
/// **Why it still rides out an rEtcd stall.** The grant's bound, at `Budgets::SPEC_DEFAULTS`:
///
/// ```text
/// horizon after a committed renewal = dispatch + G - eps - delta - 1 = dispatch + 5799 ms
///                                     (`Authority::admission_horizon`: G 6000, eps 100, delta 100)
/// time with no committed renewal    = renew + S + rtt   (the last dispatch can be one renew,
///                                     500 ms, old when a control stall S starts)
///   S measured 2.4-2.9 s (ws-lease-pause, 2 runs); spec bound for an rEtcd failover 3000 ms
///   worst case = 500 + 3000 + 300 (commit round trip under load, assumed) = 3800 ms
/// Deactivating fires when the time left <= lead, i.e. at 5800 - lead ms after the dispatch
/// no warning on a stall rDB rides out  <=>  5800 - lead > 3800  <=>  lead < 2000 ms
/// lead = 1000 ms: fires 4800 ms after the dispatch, 1000 ms beyond the worst stall renewed
/// ```
///
/// **The grant cap** is for grants far shorter than the spec's (crate tests run 600 ms grants,
/// which warn 150 ms ahead), so a partition there still activates and still warns before its
/// horizon. At the spec's grant it never binds.
///
/// Crate-private: `Budgets` is a core type the app surface does not re-export (#123 review).
/// Apps read [`DEACTIVATING_LEAD_MILLIS`].
#[must_use]
pub(crate) const fn deactivating_lead_millis(budgets: &Budgets) -> u64 {
    let clock = budgets
        .max_sample_age_millis
        .saturating_sub(budgets.clock_sample_period_millis.saturating_mul(2));
    let grant = budgets.grant_millis / 4;
    if clock < grant {
        clock
    } else {
        grant
    }
}

/// How long before a partition's last write tick its owner sends
/// [`OwnershipEvent::Deactivating`], at rDB's default budgets: 1000 ms.
///
/// ```text
/// lead = min(max_sample_age - 2 x clock_sample_period, grant / 4)
///      = min(2000 - 2 x 500, 6000 / 4) = min(1000, 1500) = 1000 ms
/// ```
///
/// The subtraction saturates at 0, and `grant / 4` rounds down. The clock term binds at the
/// defaults: a node's write permission also ends when its clock sample is older than
/// `max_sample_age`, and the second `clock_sample_period` lets a sample come one whole period
/// late without a warning. The grant cap binds only for grants far shorter than the default.
/// The `deadline` an event carries is what to act on; this is the lead it is sent with.
pub const DEACTIVATING_LEAD_MILLIS: u64 = deactivating_lead_millis(&Budgets::SPEC_DEFAULTS);

/// An ownership change on one node of this process (#83), from [`crate::Db::ownership_events`].
///
/// Per node and partition the order is always `Activated`, then `Deactivating`, then either
/// `Activated` again with the same token (the write permission was extended in time, by a
/// renewal or by a fresh clock sample: resume, no rebuild) or `Deactivated`. A fence, fault or
/// stop sends `Deactivating` with a passed deadline and then `Deactivated` from `Activated`,
/// `Deactivated` alone from `Deactivating`, and nothing for a partition not held. Across nodes, an old owner's `Deactivated` comes before a new owner's
/// `Activated` by the grant arithmetic, not by delivery (`docs/rdb/embed.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipEvent {
    /// `node` may serve writes for `partition` under `token`, and its data is readable there:
    /// the node is the partition's writable primary, its generation is marked activated, and
    /// its commit is not read-only. Stamp `token` on every side effect.
    Activated {
        /// The node.
        node: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The token it serves under.
        token: OwnerToken,
    },
    /// `node`'s write permission for `token` is about to end: it stops admitting writes at
    /// `deadline`, unless the permission is extended first (a renewal, or a fresh clock sample).
    /// Start nothing new for the partition and begin to drain.
    Deactivating {
        /// The node.
        node: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The token it was serving under.
        token: OwnerToken,
        /// When write admission stops for `token` on this node, unless the permission is
        /// extended first: never later than the node's clock reaching its first refused tick.
        /// Already past for a fence (frozen grant, revoked epoch), a host fault or a node stop,
        /// which is followed by `Deactivated` at once.
        deadline: Instant,
    },
    /// `node` admits no write for `token` any more. Stop every effect made under it.
    Deactivated {
        /// The node.
        node: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The token it no longer serves under.
        token: OwnerToken,
    },
}

impl OwnershipEvent {
    /// The node the event is about.
    #[must_use]
    pub const fn node(&self) -> NodeId {
        match self {
            Self::Activated { node, .. }
            | Self::Deactivating { node, .. }
            | Self::Deactivated { node, .. } => *node,
        }
    }

    /// The partition the event is about.
    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        match self {
            Self::Activated { partition, .. }
            | Self::Deactivating { partition, .. }
            | Self::Deactivated { partition, .. } => *partition,
        }
    }

    /// The token the event is about.
    #[must_use]
    pub const fn token(&self) -> OwnerToken {
        match self {
            Self::Activated { token, .. }
            | Self::Deactivating { token, .. }
            | Self::Deactivated { token, .. } => *token,
        }
    }

    /// `activated`, `deactivating` or `deactivated`, as `rdb_node` prints it.
    #[must_use]
    pub const fn state(&self) -> &'static str {
        match self {
            Self::Activated { .. } => "activated",
            Self::Deactivating { .. } => "deactivating",
            Self::Deactivated { .. } => "deactivated",
        }
    }
}

/// Where one partition stands in a node's ownership stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum OwnState {
    /// Not sent `Activated`, or sent `Deactivated` since.
    #[default]
    Inactive,
    /// Sent `Activated` for the token.
    Active(OwnerToken),
    /// Sent `Deactivating` for the token.
    Deactivating(OwnerToken),
}

/// What the host knows of one partition at one turn, for [`ownership_step`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct Judged {
    /// A1's newest view of the partition here, if any.
    pub(crate) view: Option<AuthorityView>,
    /// The host's clock.
    pub(crate) now: Tick,
    /// The host has faulted: it admits nothing.
    pub(crate) faulted: bool,
    /// The node is the partition's writable primary now: R1 primary, L1 admitting, A1 admitting
    /// the adopted lineage, the generation marked activated, and the commit not `ReadOnly`
    /// (ruling B-4). Read only for a partition that is not yet `Active`.
    pub(crate) writable: bool,
    /// T1's queue is open. Needed to activate, not to stay active: a brief freeze on an
    /// unresolved transaction is not an ownership change (ruling B-4).
    pub(crate) queue_open: bool,
    /// [`deactivating_lead_millis`].
    pub(crate) lead_millis: u64,
}

/// One transition [`ownership_step`] asks the host to send, before the node and an `Instant`
/// are put on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    Activated(OwnerToken),
    /// Writes stop for the token from `deadline` on (the tick after the horizon).
    Deactivating(OwnerToken, Tick),
    Deactivated(OwnerToken),
}

impl Judged {
    /// The token the view names, when the view admits it now.
    fn owned(&self) -> Option<(OwnerToken, Tick)> {
        let view = self.view?;
        let token = OwnerToken::of(view.lineage);
        owns_at(&view, self.now, token).then_some((token, view.valid_through_tick))
    }

    /// Whether the time left before `horizon` is within the lead: at or after `horizon + 1 - lead`.
    fn low(&self, horizon: Tick) -> bool {
        horizon.0.saturating_add(1).saturating_sub(self.now.0) <= self.lead_millis
    }
}

/// The next state of one partition's stream and the transitions to send, from `prev` and what
/// the host knows now (#83). Pure: the host calls it once per turn per partition, and the rows
/// drive it tick by tick.
///
/// - `Inactive` to `Active` when the view admits, the time left is above the lead, the node is
///   the writable primary and T1's queue is open.
/// - `Active` to `Deactivating` when the time left is within the lead.
/// - `Deactivating` back to `Active`, same token, when a renewal moved the horizon out again.
/// - To `Inactive` with `Deactivated` when the view no longer admits the token (past the
///   horizon, a fence, a newer token) or the host faulted. From `Active` a `Deactivating` with
///   the deadline now comes first, so every `Deactivated` follows a `Deactivating`.
///
/// After a deactivation the same turn may activate a newer token at once.
pub(crate) fn ownership_step(prev: OwnState, judged: &Judged) -> (OwnState, Vec<Step>) {
    let owned = judged.owned().filter(|_| !judged.faulted);
    let mut steps = Vec::new();
    let held = match prev {
        OwnState::Inactive => None,
        OwnState::Active(token) | OwnState::Deactivating(token) => Some(token),
    };
    if let Some(token) = held {
        match owned {
            Some((now_token, horizon)) if now_token == token => {
                let low = judged.low(horizon);
                return match (prev, low) {
                    (OwnState::Active(_), true) => (
                        OwnState::Deactivating(token),
                        vec![Step::Deactivating(token, Tick(horizon.0.saturating_add(1)))],
                    ),
                    (OwnState::Deactivating(_), false) => {
                        (OwnState::Active(token), vec![Step::Activated(token)])
                    }
                    _ => (prev, steps),
                };
            }
            _ => {
                if matches!(prev, OwnState::Active(_)) {
                    steps.push(Step::Deactivating(token, judged.now));
                }
                steps.push(Step::Deactivated(token));
            }
        }
    }
    match owned {
        Some((token, horizon)) if !judged.low(horizon) && judged.writable && judged.queue_open => {
            steps.push(Step::Activated(token));
            (OwnState::Active(token), steps)
        }
        _ => (OwnState::Inactive, steps),
    }
}

/// When the host must judge a partition in `state` again even if nothing else happens: for
/// `Active`, the tick its time left falls within the lead; for `Deactivating`, the tick after its
/// horizon, so `Deactivated` is sent at the first turn past it (ruling B-1), not at the next
/// renew timer. `None` for `Inactive`.
pub(crate) fn ownership_due(
    state: OwnState,
    view: Option<&AuthorityView>,
    lead_millis: u64,
) -> Option<Tick> {
    let after = view?.valid_through_tick.0.saturating_add(1);
    match state {
        OwnState::Inactive => None,
        OwnState::Active(_) => Some(Tick(after.saturating_sub(lead_millis))),
        OwnState::Deactivating(_) => Some(Tick(after)),
    }
}

/// How many events a subscriber may hold unread before it is dropped (ruling B-5).
pub(crate) const SUBSCRIBER_CAPACITY: usize = 4096;

/// One [`crate::Db::ownership_events`] subscriber, shared by every host of the `Db`.
#[derive(Debug)]
pub struct Subscriber {
    tx: SyncSender<OwnershipEvent>,
    /// Set when any host dropped it (full, gone, or the host stopped), when the `Db` could not
    /// reach one of its nodes, or when the app dropped its [`OwnershipEvents`]. Every host then
    /// lets go of its sender at its next turn, quiet or not, so the receiver ends and the app
    /// subscribes again.
    dropped: Arc<AtomicBool>,
}

impl Subscriber {
    /// A subscriber holding at most `capacity` unread events, and its receiver.
    pub(crate) fn new(capacity: usize) -> (Arc<Self>, OwnershipEvents) {
        let (tx, rx) = mpsc::sync_channel(capacity);
        let dropped = Arc::new(AtomicBool::new(false));
        (
            Arc::new(Self {
                tx,
                dropped: Arc::clone(&dropped),
            }),
            OwnershipEvents { rx, dropped },
        )
    }

    /// Whether this subscriber was dropped, by any host or by its receiver.
    pub(crate) fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::SeqCst)
    }

    /// Drop it for every host.
    pub(crate) fn drop_everywhere(&self) {
        self.dropped.store(true, Ordering::SeqCst);
    }

    /// Send `event`. `Err` names why the subscriber must be dropped: `"full"` when it lags by
    /// [`SUBSCRIBER_CAPACITY`] events, `"gone"` when its receiver was dropped, `"dropped"` when
    /// another host dropped it already.
    pub(crate) fn send(&self, event: OwnershipEvent) -> Result<(), &'static str> {
        if self.is_dropped() {
            return Err("dropped");
        }
        match self.tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err("full"),
            Err(TrySendError::Disconnected(_)) => Err("gone"),
        }
    }
}

/// The receiving end of [`crate::Db::ownership_events`]: `recv`, `recv_timeout`, `try_recv`,
/// `iter` and `try_iter`, as on [`std::sync::mpsc::Receiver`].
///
/// It ends (`recv` answers `Err`, `iter` stops) once every node has let go of it: when it fell
/// 4096 events behind, a node of the process stopped or could not be reached, or the `Db` shut
/// down. Events already queued are handed out first. Dropping it tells every node, which lets go
/// of it at its next turn, so a dropped subscription costs nothing for long, quiet or not.
#[derive(Debug)]
pub struct OwnershipEvents {
    rx: Receiver<OwnershipEvent>,
    dropped: Arc<AtomicBool>,
}

impl OwnershipEvents {
    /// The next event, waiting for one.
    ///
    /// # Errors
    ///
    /// The subscription ended.
    pub fn recv(&self) -> Result<OwnershipEvent, mpsc::RecvError> {
        self.rx.recv()
    }

    /// The next event, waiting at most `timeout`.
    ///
    /// # Errors
    ///
    /// `Timeout` when none came in time; `Disconnected` when the subscription ended.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<OwnershipEvent, mpsc::RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }

    /// The next event, if one is queued now.
    ///
    /// # Errors
    ///
    /// `Empty` when none is queued; `Disconnected` when the subscription ended.
    pub fn try_recv(&self) -> Result<OwnershipEvent, mpsc::TryRecvError> {
        self.rx.try_recv()
    }

    /// Every event, waiting for each, until the subscription ends.
    pub fn iter(&self) -> mpsc::Iter<'_, OwnershipEvent> {
        self.rx.iter()
    }

    /// The events queued now, without waiting.
    pub fn try_iter(&self) -> mpsc::TryIter<'_, OwnershipEvent> {
        self.rx.try_iter()
    }
}

impl<'a> IntoIterator for &'a OwnershipEvents {
    type Item = OwnershipEvent;
    type IntoIter = mpsc::Iter<'a, OwnershipEvent>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Drop for OwnershipEvents {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

// ------------------------------------------------------------------ #83: route invalidation

/// What a cached route was read under: the marks' counts when its read began.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RouteStamp {
    every: u64,
    partition: u64,
}

/// Which cached routes a control watch has made stale (#83). Shared by the
/// [`crate::control::ControlAdapter`], whose A1 `partitions/` watches mark it, and the `Db`,
/// whose `route` reads it: no new watch stream is opened for routes. A route read is cached
/// only while at least one such watch is open, and only until a delivered `partitions/{p}`
/// change for its partition, or any watch's end, moves a count it was read under.
#[derive(Debug, Default)]
pub(crate) struct RouteMarks {
    /// `partitions/` watches open now, of every node of the process.
    open: AtomicU32,
    /// Moved by every watch end, which may have missed changes until its successor opens.
    every: AtomicU64,
    /// Moved for a partition by each delivered change of its record.
    partitions: Mutex<BTreeMap<PartitionId, u64>>,
}

impl RouteMarks {
    fn count(&self, partition: PartitionId) -> u64 {
        self.partitions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&partition)
            .copied()
            .unwrap_or(0)
    }

    /// The stamp a read of `partition`'s record begun now is cached under, or `None` while no
    /// watch is open: nothing would tell the cache the record changed.
    pub(crate) fn stamp(&self, partition: PartitionId) -> Option<RouteStamp> {
        let every = self.every.load(Ordering::SeqCst);
        let stamp = RouteStamp {
            every,
            partition: self.count(partition),
        };
        (self.open.load(Ordering::SeqCst) > 0).then_some(stamp)
    }

    /// Whether a route of `partition` read under `stamp` is still current.
    pub(crate) fn current(&self, partition: PartitionId, stamp: RouteStamp) -> bool {
        self.open.load(Ordering::SeqCst) > 0
            && self.every.load(Ordering::SeqCst) == stamp.every
            && self.count(partition) == stamp.partition
    }

    /// A watch delivered a change of `partitions/{partition}`.
    pub(crate) fn changed(&self, partition: PartitionId) {
        *self
            .partitions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(partition)
            .or_insert(0) += 1;
    }

    /// A `partitions/` watch opened. The returned guard counts it closed, and moves every
    /// stamp, when it drops: at its end, its error, or when a newer watch aborts it.
    pub(crate) fn opened(self: &Arc<Self>) -> RouteWatch {
        self.open.fetch_add(1, Ordering::SeqCst);
        RouteWatch(Arc::clone(self))
    }
}

/// An open `partitions/` watch, for [`RouteMarks`].
#[derive(Debug)]
pub(crate) struct RouteWatch(Arc<RouteMarks>);

impl Drop for RouteWatch {
    fn drop(&mut self) {
        self.0.every.fetch_add(1, Ordering::SeqCst);
        self.0.open.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Where [`crate::Db::route`] sends an affinity group's calls (#83): the owner and token that
/// `partitions/{p}` names. A hint, never authority: act on a token only through
/// [`crate::Db::still_owner`] on the owner, and the destination's own token check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteInfo {
    /// The group's partition.
    pub partition: PartitionId,
    /// The node the record names as owner.
    pub owner: NodeId,
    /// The record's `(generation, owner_epoch)`.
    pub token: OwnerToken,
}

#[cfg(test)]
mod tests;
