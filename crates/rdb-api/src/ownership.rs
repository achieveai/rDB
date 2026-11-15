//! Ownership for apps (#84): the owner token of a partition this process leads, and a cheap
//! check that it still does, so an app can fence the side effects it makes outside rDB.
//!
//! The source of truth is the [`AuthorityView`] A1 already publishes to its host. Each host
//! keeps its newest view per partition in an [`OwnerViews`] that [`crate::Db`] reads on the
//! app's thread, with the host's own [`HostClock`]. [`owns_at`] is write admission's check 6
//! ([`view_admits`]), not a copy of it, so for clock reasons `still_owner` turns false at the
//! same tick admission stops. A fence (frozen grant, revoked epoch, a boot) publishes a view whose
//! horizon is the tick before it, and the host writes the map before it steps T1 again, so
//! `still_owner` is false before rDB admits another write. That view is removed, not kept, so a
//! clock stepped back cannot revive it; the host names it a fence's from the step that carries
//! the fence, so a fence at tick 0 (horizon saturated to the fence tick) is removed too. A host fault revokes the map before any caller is
//! answered with the fault.
//!
//! `true` means only that no other node can hold this token's partition now. It does not mean
//! the partition takes writes: a frozen queue, a paused L1 or a read-only recovery refuse writes
//! under a view that still admits (checks 7 and 8).

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use rdb_core::contracts::authority::{AuthorityView, Lineage};
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

#[cfg(test)]
mod tests;
