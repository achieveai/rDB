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
//! Two faults rEtcd cannot be made to show by hand, for scenario 2:
//!
//! * [`HostileStore::drop_watches`] ends every watch stream it handed out, as a lost connection
//!   would (`control drop-watches`, and the second half of `control gap`);
//! * [`HostileStore::fail_lists`] answers every `list` with one error until turned off
//!   (`control fail list`), without reaching a voter, so the answer moves no binding.
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
use futures::StreamExt;
use tokio::sync::watch;

type Reply<'a, T> = Pin<Box<dyn Future<Output = Result<T, ConfigError>> + Send + 'a>>;

/// The reason a stopped voter's client gives (`config_engine::ConfigNode::stop`).
const STOPPED: &str = "stopped";

/// What `control fail list` makes every `list` answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListFault {
    /// `Unavailable`: transient.
    Unavailable,
    /// `DeadlineExceededUnknownOutcome`: transient.
    Deadline,
    /// `NotLeader` with no hint: transient.
    NotLeader,
    /// `InvalidArgument`: not transient, so a reload must still fault on it.
    Invalid,
}

impl ListFault {
    /// The words `control fail list` takes; `off` is `None`.
    pub const WORDS: &'static str = "unavailable|deadline|notleader|invalid|off";

    /// `control fail list <word>`: `Some` fault, or `None` for `off`.
    pub fn parse(word: &str) -> Result<Option<Self>, String> {
        match word {
            "unavailable" => Ok(Some(Self::Unavailable)),
            "deadline" => Ok(Some(Self::Deadline)),
            "notleader" => Ok(Some(Self::NotLeader)),
            "invalid" => Ok(Some(Self::Invalid)),
            "off" => Ok(None),
            other => Err(format!(
                "control fail list <{}>, not {other:?}",
                Self::WORDS
            )),
        }
    }

    fn error(self) -> ConfigError {
        match self {
            Self::Unavailable => ConfigError::Unavailable {
                reason: "hostile store: list failed on purpose".to_owned(),
            },
            Self::Deadline => ConfigError::DeadlineExceededUnknownOutcome,
            Self::NotLeader => ConfigError::NotLeader { hint: None },
            Self::Invalid => ConfigError::InvalidArgument {
                detail: "hostile store: list failed on purpose".to_owned(),
            },
        }
    }
}

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
    /// The fault every `list` answers, while set.
    list_fault: Mutex<Option<ListFault>>,
    /// Bumped by [`HostileStore::drop_watches`]; every stream handed out ends on the next bump.
    ends: watch::Sender<u64>,
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
            list_fault: Mutex::new(None),
            ends: watch::channel(0).0,
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

    /// `control fail list <word>`: every `list` answers `fault` from now on, or, with `None`,
    /// reaches the voter again.
    pub fn fail_lists(&self, fault: Option<ListFault>) {
        *self
            .list_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = fault;
        tracing::info!(fault = ?fault, "control_store_fail_lists");
    }

    /// `control drop-watches`: end every watch stream this store handed out. Each ends as a
    /// plain end of stream, which the Db reads as `Unavailable` and re-watches from its cursor.
    /// Returns how many were open.
    pub fn drop_watches(&self) -> usize {
        let open = self.ends.receiver_count();
        self.ends.send_modify(|n| *n += 1);
        tracing::info!(open, "control_store_watches_dropped");
        open
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
        let fault = *self
            .list_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(fault) = fault {
            let error = fault.error();
            tracing::info!(fault = ?fault, %error, "control_store_list_failed");
            return Box::pin(std::future::ready(Err(error)));
        }
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
        let mut ended = self.ends.subscribe();
        let opened = self.through(move |store| store.watch(request));
        Box::pin(async move {
            let stream = opened.await?;
            let until = async move {
                // An error means the store is gone; the stream ends then too.
                let _ = ended.changed().await;
            };
            let stream: WatchStream = Box::pin(stream.take_until(until));
            Ok(stream)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use config_testkit::cluster::StorageKind;

    /// M9 S2a ruling guards "re-resolve on `NotLeader`" and "the bound-node swap on `control
    /// stop`": the mutant pass found no row that failed without either. A `NotLeader` answer
    /// moves the store to the leader. Stopping a voter the store is not bound to only marks
    /// it; stopping the bound one moves the store first, never to a voter being stopped.
    /// One in-memory cluster, about a second.
    #[test]
    fn the_store_follows_the_leader_and_leaves_a_voter_before_its_stop() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let cluster = Arc::new(
            rt.block_on(
                Cluster::builder()
                    .nodes(3)
                    .storage(StorageKind::Ephemeral)
                    .start(),
            ),
        );
        let leader = rt.block_on(cluster.leader());
        let followers: Vec<VoterId> = cluster
            .ids()
            .into_iter()
            .filter(|id| *id != leader)
            .collect();
        let store = HostileStore::new(&cluster, followers[0]);

        let answer = rt.block_on(store.get(GetRequest {
            key: Bytes::from_static(b"partitions/1"),
        }));
        assert!(
            matches!(answer, Err(ConfigError::NotLeader { .. })),
            "a follower answers NotLeader: {answer:?}"
        );
        assert_eq!(store.bound(), leader, "the next call goes to the leader");

        assert_eq!(
            store.stopping(followers[0]),
            Ok(None),
            "not bound: only marked"
        );
        assert_eq!(
            store.stopping(leader),
            Ok(Some(followers[1])),
            "bound: moved first, past the voter already being stopped"
        );
        assert_eq!(store.bound(), followers[1]);

        drop(store);
        let cluster = Arc::try_unwrap(cluster).expect("the only handle");
        rt.block_on(cluster.shutdown());
    }

    /// `control fail list`'s words (M9 S2a mutant pass): three transient errors, which a reload
    /// retries, and `invalid`, which it must not (scenario 2f-other).
    #[test]
    fn fail_list_words_name_three_transient_faults_and_one_that_is_not() {
        let class = |word| match ListFault::parse(word)
            .expect("a word")
            .map(ListFault::error)
        {
            Some(ConfigError::Unavailable { .. }) => "unavailable",
            Some(ConfigError::DeadlineExceededUnknownOutcome) => "deadline",
            Some(ConfigError::NotLeader { .. }) => "notleader",
            Some(ConfigError::InvalidArgument { .. }) => "invalid",
            Some(_) => "another error",
            None => "off",
        };
        let words = ["unavailable", "deadline", "notleader", "invalid", "off"];
        assert_eq!(words.map(class), words);
        assert!(ListFault::parse("bogus").is_err());
    }

    /// M9 S2a mutant pass: an injected list fault answers without reaching a voter, so even
    /// `notleader` moves no binding, and `off` reaches the voter again. `drop_watches` ends a
    /// stream the store handed out. One in-memory cluster, well under a second.
    #[test]
    fn a_list_fault_never_reaches_a_voter_and_dropped_watches_end() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let cluster = Arc::new(
            rt.block_on(
                Cluster::builder()
                    .nodes(3)
                    .storage(StorageKind::Ephemeral)
                    .start(),
            ),
        );
        let leader = rt.block_on(cluster.leader());
        let store = HostileStore::new(&cluster, leader);
        let list = || {
            rt.block_on(store.list(ListRequest {
                prefix: Bytes::from_static(b"partitions/"),
                max_items: 0,
                max_bytes: 0,
            }))
        };

        store.fail_lists(Some(ListFault::NotLeader));
        let failed = list();
        assert!(
            matches!(failed, Err(ConfigError::NotLeader { .. })),
            "the injected fault, not the leader's answer: {failed:?}"
        );
        assert_eq!(store.bound(), leader, "an injected fault moves nothing");
        store.fail_lists(None);
        let listed = list();
        assert!(listed.is_ok(), "off: the leader answers: {listed:?}");

        let mut stream = rt
            .block_on(store.watch(WatchRequest {
                prefix: Bytes::from_static(b"partitions/"),
                start_after_revision: 0,
                progress_interval: None,
            }))
            .expect("a watch on the leader");
        assert_eq!(store.drop_watches(), 1, "one stream open");
        let ended = rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while let Some(item) = stream.next().await {
                    assert!(item.is_ok(), "a dropped stream just ends: {item:?}");
                }
            })
            .await
        });
        assert!(ended.is_ok(), "the stream did not end within 1 s");

        drop(stream);
        drop(store);
        let cluster = Arc::try_unwrap(cluster).expect("the only handle");
        rt.block_on(cluster.shutdown());
    }
}
