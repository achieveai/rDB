//! Ownership for apps (#84): the owner token of a partition this process leads, and a cheap
//! check that it still does, so an app can fence the side effects it makes outside rDB.
//!
//! The source of truth is the [`AuthorityView`] A1 already publishes to its host. Each host
//! keeps its newest view per partition in an [`OwnerViews`] that [`crate::Db`] reads on the
//! app's thread, with the host's own [`HostClock`]. [`owns_at`] is write admission's check 6
//! ([`view_admits`]), not a copy of it, so for clock reasons `still_owner` turns false at the
//! same tick admission stops. A fence (frozen grant, revoked epoch, a boot) publishes a view whose
//! horizon is the tick before it, and the host writes the map before it steps T1 again, so
//! `still_owner` is false before rDB admits another write.
//!
//! `true` means only that no other node can hold this token's partition now. It does not mean
//! the partition takes writes: a frozen queue, a paused L1 or a read-only recovery refuse writes
//! under a view that still admits (checks 7 and 8).

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
    /// The token of `lineage`.
    #[must_use]
    pub const fn of(lineage: Lineage) -> Self {
        Self {
            generation: lineage.generation,
            owner_epoch: lineage.owner_epoch,
        }
    }

    /// The lineage this token names in `partition`.
    #[must_use]
    pub const fn lineage(self, partition: PartitionId) -> Lineage {
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
    /// Time left until [`crate::Db::still_owner`] turns false, unless A1 renews first, as
    /// write admission counts it. An effect that takes `d` to land needs `remaining > d`.
    pub remaining: Duration,
}

/// Whether `view` admits a write of `token`'s lineage at `now`: write admission's check 6.
#[must_use]
pub(crate) fn owns_at(view: &AuthorityView, now: Tick, token: OwnerToken) -> bool {
    view_admits(view, token.lineage(view.lineage.partition), now) == ViewAdmits::Admits
}

/// One host's newest [`AuthorityView`] per partition, and the clock that host admits writes
/// by. The host writes it when A1 publishes a view; [`crate::Db`] reads it on an app thread.
/// Entries are never removed: a view for a partition the node no longer serves has expired, or
/// was superseded by its fence, so it reads as not owned.
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
}

impl OwnerViews {
    /// An empty map for `node`, read by `clock`: the host's own.
    pub(crate) fn new(node: NodeId, clock: HostClock) -> Arc<Self> {
        Arc::new(Self {
            node,
            state: RwLock::new(Published {
                clock,
                views: BTreeMap::new(),
            }),
        })
    }

    /// A1 published `view` on a host reading `clock`: it replaces the partition's last one, as
    /// `Host::authority_views` does.
    pub(crate) fn publish(&self, view: AuthorityView, clock: HostClock) {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        state.clock = clock;
        state.views.insert(view.lineage.partition, view);
    }

    /// The node stopped: it admits nothing, so it owns nothing.
    pub(crate) fn clear(&self) {
        self.state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .views
            .clear();
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
            // Admits through `valid_through_tick`, so false from the tick after it.
            remaining: Duration::from_millis(
                view.valid_through_tick
                    .0
                    .saturating_sub(now.0)
                    .saturating_add(1),
            ),
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

/// How long before a partition's admission horizon its owner warns `Deactivating`, at the spec's
/// budgets (#83, ruling B-2). The arithmetic, with `Budgets::SPEC_DEFAULTS`:
///
/// ```text
/// horizon after a committed renewal = dispatch + G - eps - delta - 1 = dispatch + 5799 ms
///                                     (`Authority::admission_horizon`: G 6000, eps 100, delta 100)
/// time with no committed renewal    = renew + S + rtt   (the last dispatch can be one renew,
///                                     500 ms, old when a control stall S starts)
///   S measured 2.4-2.9 s (ws-lease-pause, 2 runs); spec bound for an rEtcd failover 3000 ms
///   worst case = 500 + 3000 + 300 (commit round trip under load, assumed) = 3800 ms
/// Deactivating fires when the time left < lead, i.e. at 5799 - lead ms after the dispatch
/// no warning on a stall rDB rides out  <=>  5799 - lead > 3800  <=>  lead < 1999 ms
/// lead = 1500 ms: fires 4299 ms after the dispatch, 499 ms beyond the worst stall still renewed
/// ```
///
/// The horizon has a second bound, and in steady state it is the one that binds:
/// `Authority::admission_horizon` stops at the last tick `utc_ok` holds, and `effective_epsilon`
/// calls a sample stale past `max_sample_age` (2000 ms). Every A1 step takes a fresh local
/// sample, so with A1 stepped every `clock_sample_period` (500 ms) the time left swings
/// between 2000 and 1500 ms, and a store stall does not stop it (the sample is local):
///
/// ```text
/// no warning between two A1 steps  <=>  max_sample_age - clock_sample_period >= lead
///                                  <=>  2000 - 500 >= lead  <=>  lead <= 1500 ms
/// ```
///
/// 1500 sits on that edge. The `Active` due tick is the sample + 501, one tick after A1's own
/// timer, and the turn runs that timer before the ownership duty, so the two never cross on a
/// turn. A lead above 1500 warns and recovers on every sample (measured: 1600 flaps).
///
/// So a 2-3 s rEtcd stall, which renews before the horizon, warns nothing; a stall long enough
/// to warn leaves the app 1.5 s before writes stop. If a measured stall grows, this shrinks.
pub const DEACTIVATING_LEAD_MILLIS: u64 = 1_500;

/// The lead a host with `budgets` warns by: [`DEACTIVATING_LEAD_MILLIS`] at the spec's grant or
/// a longer one, and the same share of a shorter grant (crate tests run 600 ms grants, which
/// warn 150 ms ahead), so a partition there still activates and still warns before its horizon.
#[must_use]
pub(crate) fn deactivating_lead_millis(budgets: &Budgets) -> u64 {
    let spec = Budgets::SPEC_DEFAULTS.grant_millis;
    DEACTIVATING_LEAD_MILLIS
        .min(DEACTIVATING_LEAD_MILLIS.saturating_mul(budgets.grant_millis) / spec)
}

/// An ownership change on one node of this process (#83), from [`crate::Db::ownership_events`].
///
/// Per node and partition the order is always `Activated`, then `Deactivating`, then either
/// `Activated` again with the same token (a renewal landed in time: resume, no rebuild) or
/// `Deactivated`. Across nodes, an old owner's `Deactivated` comes before a new owner's
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
    /// No renewal has landed and `node` stops admitting writes for `token` at `deadline`, unless
    /// one lands first. Start nothing new for the partition and begin to drain.
    Deactivating {
        /// The node.
        node: NodeId,
        /// The partition.
        partition: PartitionId,
        /// The token it was serving under.
        token: OwnerToken,
        /// When write admission stops for `token` on this node, unless a renewal lands first.
        /// Already past for a fence (frozen grant, revoked epoch, host fault), which is followed
        /// by `Deactivated` at once.
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
    /// Set when any host dropped it: full, gone, or the host stopped. Every host then lets go
    /// of its sender, so the receiver ends and the app subscribes again.
    dropped: AtomicBool,
}

impl Subscriber {
    /// A subscriber holding at most `capacity` unread events, and its receiver.
    pub(crate) fn new(capacity: usize) -> (Arc<Self>, Receiver<OwnershipEvent>) {
        let (tx, rx) = mpsc::sync_channel(capacity);
        (
            Arc::new(Self {
                tx,
                dropped: AtomicBool::new(false),
            }),
            rx,
        )
    }

    /// Whether any host dropped this subscriber.
    pub(crate) fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Drop it for every host.
    pub(crate) fn drop_everywhere(&self) {
        self.dropped.store(true, Ordering::Relaxed);
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
