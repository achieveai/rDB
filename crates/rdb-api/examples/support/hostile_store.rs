//! `HostileStore`: the control store `rdb_dev` hands its Db (M9 S2a, critic M4).
//!
//! `Cluster::client` is a `DirectClient` bound to one voter. On a follower it answers
//! `NotLeader`, and on a stopped voter `Unavailable { reason: "stopped" }`, so a Db opened on
//! the first leader's client loses its control plane the moment leadership moves. This store
//! follows the leader instead:
//!
//! * a call answered `NotLeader`, or `Unavailable` because its voter stopped, is returned as it
//!   came, and the next call goes to the voter that is leader now (`control_store_rebind`);
//! * `control stop` of the voter the store is bound to moves the store first, and the voter is
//!   never bound to again while it is being stopped ([`HostileStore::stopping`]). Dropping the
//!   old client is also what lets `control start` reopen that voter's RocksDB.
//!
//! It holds the cluster weakly, so `rdb_dev` can still take the cluster back to shut it down.
//! Example-only: the product's Db takes any `ConfigStore` and knows nothing of voters.
//!
//! Written out by hand, as `src/test_store.rs` is, because `async-trait` is not a dependency of
//! this crate.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use config_core::{
    Capabilities, ConfigError, ConfigStore, DeleteRequest, GetRequest, GetResponse, ListRequest,
    ListResponse, MutationResponse, NodeId as VoterId, PutRequest, WatchRequest, WatchStream,
};
use config_testkit::cluster::Cluster;

type Reply<'a, T> = Pin<Box<dyn Future<Output = Result<T, ConfigError>> + Send + 'a>>;

/// The reason a stopped voter's client gives (`config_engine::ConfigNode::stop`).
const STOPPED: &str = "stopped";

/// Which voter the store talks to, and the voters it must not move to.
struct Binding {
    voter: VoterId,
    client: Arc<dyn ConfigStore>,
    /// Voters `control stop` is taking down. Excluded even while one still reports itself
    /// leader, which it does until the stop completes.
    stopping: BTreeSet<VoterId>,
}

/// A control store that follows the rEtcd leader.
pub struct HostileStore {
    cluster: Weak<Cluster>,
    binding: Mutex<Binding>,
}

impl std::fmt::Debug for HostileStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostileStore")
            .field("bound", &self.bound())
            .finish_non_exhaustive()
    }
}

/// Whether `error` says the voter that gave it cannot serve this store any more.
fn moves(error: &ConfigError) -> bool {
    match error {
        ConfigError::NotLeader { .. } => true,
        ConfigError::Unavailable { reason } => reason == STOPPED,
        _ => false,
    }
}

impl HostileStore {
    /// Bound to `voter`, which must be running.
    pub fn new(cluster: &Arc<Cluster>, voter: VoterId) -> Self {
        Self {
            cluster: Arc::downgrade(cluster),
            binding: Mutex::new(Binding {
                voter,
                client: cluster.client(voter),
                stopping: BTreeSet::new(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Binding> {
        self.binding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The voter calls go to now.
    pub fn bound(&self) -> VoterId {
        self.lock().voter
    }

    fn current(&self) -> (VoterId, Arc<dyn ConfigStore>) {
        let binding = self.lock();
        (binding.voter, Arc::clone(&binding.client))
    }

    /// `voter` answered `error`. When that says it cannot serve, bind to the leader now, if
    /// there is one this store may use; otherwise keep the binding, and the next such answer
    /// tries again. Only the voter still bound is moved away from, so a late answer from a
    /// voter already left behind changes nothing.
    fn after(&self, voter: VoterId, error: &ConfigError) {
        if !moves(error) {
            return;
        }
        let Some(cluster) = self.cluster.upgrade() else {
            return;
        };
        let mut binding = self.lock();
        if binding.voter != voter {
            return;
        }
        let leader = cluster.leader_now();
        let usable = leader.filter(|l| {
            *l != voter && !binding.stopping.contains(l) && cluster.running_ids().contains(l)
        });
        match usable {
            Some(to) => {
                // Under the lock: `stopping` is marked under it too, so `to` is still running.
                binding.client = cluster.client(to);
                binding.voter = to;
                tracing::info!(from = voter.0, to = to.0, %error, "control_store_rebind");
            }
            None => tracing::debug!(
                from = voter.0,
                leader = leader.map(|l| l.0),
                %error,
                "control_store_rebind_waiting"
            ),
        }
    }

    /// `control stop <voter>` is about to stop `voter`. Marks it so the store never binds to
    /// it, and when the store is bound to it, moves the store first: to the leader if that is
    /// another voter, else to any other running voter, whose `NotLeader` then moves it on once
    /// a leader is elected. Returns the voter moved to, if the store moved. Refused when no
    /// other voter is running; the voter is then left unmarked and running.
    pub fn stopping(&self, voter: VoterId) -> Result<Option<VoterId>, String> {
        let cluster = self
            .cluster
            .upgrade()
            .ok_or_else(|| "the cluster is gone".to_owned())?;
        let mut binding = self.lock();
        if binding.voter != voter {
            binding.stopping.insert(voter);
            return Ok(None);
        }
        let running = cluster.running_ids();
        let other = |id: &VoterId| *id != voter && !binding.stopping.contains(id);
        let to = cluster
            .leader_now()
            .filter(|l| other(l) && running.contains(l))
            .or_else(|| running.iter().copied().find(|id| other(id)))
            .ok_or_else(|| {
                format!(
                    "the control store is bound to voter {} and no other voter is running to \
                     move it to",
                    voter.0
                )
            })?;
        binding.client = cluster.client(to);
        binding.voter = to;
        binding.stopping.insert(voter);
        tracing::info!(from = voter.0, to = to.0, "control_store_moved_before_stop");
        Ok(Some(to))
    }

    /// The stop of `voter` finished, or failed: it may be bound to again once it runs.
    pub fn stopped(&self, voter: VoterId) {
        self.lock().stopping.remove(&voter);
    }

    /// Run `call` on the bound voter's client, and follow the leader on its answer.
    fn through<'a, 'b, T, F>(&'a self, call: F) -> Reply<'b, T>
    where
        'a: 'b,
        T: Send + 'b,
        F: for<'c> FnOnce(&'c dyn ConfigStore) -> Reply<'c, T> + Send + 'b,
    {
        Box::pin(async move {
            let (voter, client) = self.current();
            let answer = call(&*client).await;
            if let Err(error) = &answer {
                self.after(voter, error);
            }
            answer
        })
    }
}

impl ConfigStore for HostileStore {
    fn get<'a, 'b>(&'a self, request: GetRequest) -> Reply<'b, GetResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.through(move |store| store.get(request))
    }

    fn list<'a, 'b>(&'a self, request: ListRequest) -> Reply<'b, ListResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.through(move |store| store.list(request))
    }

    fn put<'a, 'b>(&'a self, request: PutRequest) -> Reply<'b, MutationResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.through(move |store| store.put(request))
    }

    fn delete<'a, 'b>(&'a self, request: DeleteRequest) -> Reply<'b, MutationResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.through(move |store| store.delete(request))
    }

    fn capabilities(&self) -> Capabilities {
        self.current().1.capabilities()
    }

    fn watch<'a, 'b>(&'a self, request: WatchRequest) -> Reply<'b, WatchStream>
    where
        'a: 'b,
        Self: 'b,
    {
        self.through(move |store| store.watch(request))
    }
}
