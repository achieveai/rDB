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

#[cfg(test)]
mod tests;
