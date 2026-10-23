//! `ControlEffect` onto a real `config_core::ConfigStore` (M9 architecture §3, `rdb_api::control`).
//!
//! Each call runs as a tokio task and posts its completion to the asking node's mailbox, carrying
//! the effect's partition and correlation back, as the sim's control store does.
//!
//! The mapping (ADR-rdb-0015: Unknown is not Unavailable is not Conflict):
//!
//! * `Cas` with a value is a put; `expected: None` ("must not exist") becomes
//!   `expected_mod_revision: Some(0)`. `Cas` without a value is a delete, which needs a revision;
//!   a delete with `expected: None` has no rEtcd spelling and faults the node.
//! * `Applied` → `Committed(revision)`. `Conflict` → `Conflict{exists, current}`. A delete's
//!   `NotFound` → `Conflict{exists: false}`. A deadline with an unknown outcome → `Unknown`. Any
//!   other error → `Unavailable`, logged.
//! * `Get` → `Found` at the record's `mod_revision`, or `Absent` as of the read revision.
//! * `Watch` → one `Watched` per rEtcd event, `WatchProgress` per progress item, and one
//!   `WatchTerminated` when the stream fails or ends. A new watch on a node's prefix replaces
//!   the old one, as the sim's store does.
//! * `Reload` → one `FamilySnapshot` at the list's read revision. A truncated list faults the node:
//!   a partial family would read as a complete one.
//!
//! Every call logs `control_call{node, op, key, outcome, millis}`. A get adds `revision`, the one it
//! found; a CAS adds `expected`, the revision it requires (0: must not exist), and `revision`, the
//! one it committed at.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use bytes::Bytes;
use config_core::{
    ConfigError, ConfigStore, DeleteRequest, GetRequest, ListRequest, MutationOutcome,
    MutationResponse, PutRequest, WatchItem, WatchRequest,
};
use futures::StreamExt;
use rdb_core::contracts::control::{
    CasOutcome, ControlChange, ControlEffect, ControlEvent, ControlKey, ControlPrefix,
    ControlRecord, ReadOutcome, WatchCursor, WatchTermination,
};
use rdb_core::contracts::ids::{
    BootId, ControlRequestId, CorrelationId, NodeId, PartitionId, Revision,
};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::{Instrument, Span};

use crate::host::Msg;
use crate::transport::Links;

/// Where a completion goes: the asking node and the boot it asked under, and the effect's
/// partition and correlation.
#[derive(Debug, Clone, Copy)]
struct Site {
    node: NodeId,
    boot: BootId,
    partition: PartitionId,
    correlation: CorrelationId,
}

/// The newest reload of a family, running or done, and how many of its lists have failed so
/// far (0 once it answers). The count's allocation is also the reload's identity: a reload
/// answers only while the map still holds its own count (M9 S2a review F-002).
type Reloading = (JoinHandle<()>, Arc<AtomicU32>);

/// The reload of each (node, prefix), shared with the reloads themselves.
type Reloads = Arc<Mutex<BTreeMap<(NodeId, ControlPrefix), Reloading>>>;

/// The newest watch of a family, and how many watches of that family in a row have ended
/// having served nothing. The count outlives the watch: the next watch of the family takes it
/// over, and waits [`retry_backoff`] of it before it opens (M10 S1, the M9 S2a carry).
type Watching = (JoinHandle<()>, Arc<AtomicU32>);

/// The control binding for every node of one process.
pub struct ControlAdapter {
    store: Arc<dyn ConfigStore>,
    rt: Handle,
    links: Arc<Links>,
    /// The gets and CASes still running, per asking node, so a restart abandons only that
    /// node's (M9 S2b, critic M2).
    tasks: Mutex<BTreeMap<NodeId, Vec<JoinHandle<()>>>>,
    /// The watch of each (node, prefix). A newer one aborts the one it replaces and takes over
    /// its count of unserved watches.
    watches: Mutex<BTreeMap<(NodeId, ControlPrefix), Watching>>,
    /// The reload of each (node, prefix). A newer one aborts the one it replaces.
    reloads: Reloads,
}

impl std::fmt::Debug for ControlAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlAdapter").finish_non_exhaustive()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl ControlAdapter {
    /// Bind `store`, running calls on `rt` and answering through `links`.
    #[must_use]
    pub fn new(store: Arc<dyn ConfigStore>, rt: Handle, links: Arc<Links>) -> Arc<Self> {
        Arc::new(Self {
            store,
            rt,
            links,
            tasks: Mutex::new(BTreeMap::new()),
            watches: Mutex::new(BTreeMap::new()),
            reloads: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Carry out `effect` for `node`, running at `boot`; its completion arrives in the node's
    /// mailbox stamped with `boot`, so a later process of the node can tell it is not its own.
    pub fn submit(
        &self,
        node: NodeId,
        boot: BootId,
        partition: PartitionId,
        correlation: CorrelationId,
        effect: ControlEffect,
    ) {
        let site = Site {
            node,
            boot,
            partition,
            correlation,
        };
        let store = Arc::clone(&self.store);
        let links = Arc::clone(&self.links);
        // Every task below runs inside the submitter's span, so its lines land where the
        // submitter's do (a test's own log file, not `_untagged`; #50). A node thread has no
        // span, so there it changes nothing.
        if let ControlEffect::Reload { prefix } = effect {
            // Only the newest reload of a family may answer: an older snapshot posted after a
            // newer one would roll the node's cache back. Spawned under the lock, so the new
            // reload is in the map before it can look for itself there. `abort` only saves the
            // older one's work; `reload` is what keeps it from answering.
            let failed = Arc::new(AtomicU32::new(0));
            let mut reloads = lock(&self.reloads);
            let task = self.rt.spawn(
                reload(
                    store,
                    links,
                    Arc::clone(&self.reloads),
                    site,
                    prefix,
                    Arc::clone(&failed),
                )
                .instrument(Span::current()),
            );
            if let Some((old, _)) = reloads.insert((node, prefix), (task, failed)) {
                old.abort();
            }
            return;
        }
        if let ControlEffect::Watch { prefix, from } = effect {
            let mut watches = lock(&self.watches);
            let unserved = watches.get(&(node, prefix)).map_or_else(
                || Arc::new(AtomicU32::new(0)),
                |(_, count)| Arc::clone(count),
            );
            let task = self.rt.spawn(
                watch(store, links, site, prefix, from, Arc::clone(&unserved))
                    .instrument(Span::current()),
            );
            if let Some((old, _)) = watches.insert((node, prefix), (task, unserved)) {
                old.abort();
            }
            return;
        }
        let task = self.rt.spawn(
            async move {
                match call(&*store, site, effect).await {
                    Ok(event) => post(&links, site, event),
                    Err((kind, detail)) => fault(&links, site, kind, detail),
                }
            }
            .instrument(Span::current()),
        );
        let mut tasks = lock(&self.tasks);
        let calls = tasks.entry(node).or_default();
        calls.retain(|task| !task.is_finished());
        calls.push(task);
    }

    /// Ruling A-R184: mark `generation` activated in `instance`'s open counter for `node`,
    /// running at `boot`, before its first acquire under that generation. Answers
    /// [`Msg::Activated`] once the mark is durable, or faults the node when it cannot be made.
    /// Tracked with the node's calls, so a restart abandons it.
    pub fn activate(
        &self,
        node: NodeId,
        boot: BootId,
        partition: PartitionId,
        instance: [u8; rdb_core::authority::partition::INSTANCE_LEN],
        generation: rdb_core::contracts::ids::Generation,
    ) {
        let site = Site {
            node,
            boot,
            partition,
            correlation: CorrelationId::default(),
        };
        let store = Arc::clone(&self.store);
        let links = Arc::clone(&self.links);
        let task = self.rt.spawn(
            async move {
                match crate::admin::activate(&*store, &instance, generation).await {
                    Ok(_) => {
                        let msg = Msg::Activated {
                            boot,
                            partition,
                            generation,
                        };
                        if links.post(node, msg).is_err() {
                            tracing::debug!(node = node.0, "control_completion_unclaimed");
                        }
                    }
                    Err(error) => fault(&links, site, "activate", error.to_string()),
                }
            }
            .instrument(Span::current()),
        );
        let mut tasks = lock(&self.tasks);
        let calls = tasks.entry(node).or_default();
        calls.retain(|task| !task.is_finished());
        calls.push(task);
    }

    /// While a reload of `node`'s is still retrying, how many of its lists have failed: the
    /// highest count over its families (`rdb_dev nodes`: `reload pending attempt=N`). A reload
    /// puts its count back to 0 before it answers, and the only reloads ever stopped early are
    /// dropped from the map as they are (a newer one replaces it, `forget_node` or `shutdown`
    /// takes them), so a count above 0 is always a reload still retrying.
    #[must_use]
    pub fn reload_pending(&self, node: NodeId) -> Option<u32> {
        lock(&self.reloads)
            .iter()
            .filter(|((at, _), _)| *at == node)
            .map(|(_, (_, failed))| failed.load(Ordering::Relaxed))
            .filter(|failed| *failed > 0)
            .max()
    }

    /// Stop `node`'s watches and reloads and abandon its calls still running, and nobody
    /// else's: the node's process has stopped (`Db::restart_node`). A reload removed here never
    /// answers. `abort` lands only at an await, so a call already past its last one can still
    /// post; that completion carries the old boot, and the node's next process drops it.
    /// `boot` is the stopped process's, for the log: `control_forgot{node, boot, watches,
    /// reloads, calls}`.
    pub fn forget_node(&self, node: NodeId, boot: BootId) {
        let mut watches = 0_usize;
        lock(&self.watches).retain(|(at, _), (task, _)| {
            let mine = *at == node;
            if mine {
                task.abort();
                watches += 1;
            }
            !mine
        });
        let mut reloads = 0_usize;
        lock(&self.reloads).retain(|(at, _), (task, _)| {
            let mine = *at == node;
            if mine {
                task.abort();
                reloads += 1;
            }
            !mine
        });
        let mut calls = 0_usize;
        for task in lock(&self.tasks).remove(&node).unwrap_or_default() {
            if !task.is_finished() {
                calls += 1;
            }
            task.abort();
        }
        tracing::info!(
            node = node.0,
            boot = boot.0,
            watches,
            reloads,
            calls,
            "control_forgot"
        );
    }

    /// Stop every watch and abandon every call still running, reloads included. Called before
    /// the store goes.
    pub fn shutdown(&self) {
        for (_, (task, _)) in std::mem::take(&mut *lock(&self.watches)) {
            task.abort();
        }
        for (_, (task, _)) in std::mem::take(&mut *lock(&self.reloads)) {
            task.abort();
        }
        for task in std::mem::take(&mut *lock(&self.tasks))
            .into_values()
            .flatten()
        {
            task.abort();
        }
    }
}

fn post(links: &Links, site: Site, event: ControlEvent) {
    let msg = Msg::Control {
        partition: site.partition,
        correlation: site.correlation,
        boot: site.boot,
        event,
    };
    if links.post(site.node, msg).is_err() {
        tracing::debug!(node = site.node.0, "control_completion_unclaimed");
    }
}

fn fault(links: &Links, site: Site, kind: &'static str, detail: String) {
    tracing::error!(node = site.node.0, boot = site.boot.0, kind, %detail, "control_fault");
    let _ = links.post(
        site.node,
        Msg::Fault {
            boot: site.boot,
            kind,
            detail,
        },
    );
}

fn log_call(site: Site, op: &str, key: &str, outcome: &str, started: Instant) {
    let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    tracing::info!(node = site.node.0, op, key, outcome, millis, "control_call");
}

/// `control_call` for a get or a CAS, with the revisions it names: `expected`, the revision a CAS
/// requires, and `revision`, the one a get found or a CAS committed at, each absent when there is
/// none. A same-boot fresh grant is checked by these: its replace's `expected` is the `revision`
/// of the get before it (M9 S2b, A-R96).
fn log_revisions(
    site: Site,
    (op, key, outcome): (&str, &str, &str),
    expected: Option<u64>,
    revision: Option<u64>,
    started: Instant,
) {
    let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    tracing::info!(
        node = site.node.0,
        op,
        key,
        outcome,
        expected,
        revision,
        millis,
        "control_call"
    );
}

/// `control_call` for `op=watch`, with `from`, the revision the watch starts after, so a re-watch
/// shows where it resumed (M9 S2a).
fn log_watch(site: Site, key: &str, outcome: &str, from: Revision, started: Instant) {
    let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    tracing::info!(
        node = site.node.0,
        op = "watch",
        key,
        outcome,
        from = from.0,
        millis,
        "control_call"
    );
}

/// `control_call` for a reload that found its family, with `read_revision`, the revision the
/// list read at, which is the snapshot's revision (M9 S2a, tester paper cut).
fn log_reload_found(site: Site, key: &str, read_revision: u64, started: Instant) {
    let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    tracing::info!(
        node = site.node.0,
        op = "reload",
        key,
        outcome = "found",
        read_revision,
        millis,
        "control_call"
    );
}

type Fault = (&'static str, String);

async fn call(
    store: &dyn ConfigStore,
    site: Site,
    effect: ControlEffect,
) -> Result<ControlEvent, Fault> {
    let started = Instant::now();
    match effect {
        ControlEffect::Cas {
            request,
            key,
            expected,
            value,
        } => cas(store, site, started, (request, key, expected, value)).await,
        ControlEffect::Get { request, key } => {
            let name = key.encode();
            let answer = store
                .get(GetRequest {
                    key: Bytes::from(name.clone()),
                })
                .await;
            let outcome = match answer {
                Ok(response) => match response.record {
                    Some(record) => ReadOutcome::Found {
                        revision: Revision(record.mod_revision),
                        value: record.value,
                    },
                    None => ReadOutcome::Absent {
                        as_of: Revision(response.read_revision),
                    },
                },
                Err(error) => {
                    tracing::warn!(node = site.node.0, key = %name, %error, "control_get_failed");
                    ReadOutcome::Unavailable
                }
            };
            let found = match &outcome {
                ReadOutcome::Found { revision, .. } => Some(revision.0),
                _ => None,
            };
            log_revisions(
                site,
                ("get", &name, read_name(&outcome)),
                None,
                found,
                started,
            );
            Ok(ControlEvent::Value {
                request,
                key,
                outcome,
            })
        }
        // Each spawned on its own by `submit`; never reaches here.
        ControlEffect::Reload { .. } => Err(("control_reload_misrouted", String::new())),
        ControlEffect::Watch { .. } => Err(("control_watch_misrouted", String::new())),
    }
}

/// The first wait before a reload's list, or a watch after one that served nothing, is sent
/// again, doubled per attempt.
const RETRY_BACKOFF_BASE_MILLIS: u64 = 50;
/// The longest wait between two attempts (M9 S2a ruling, critic A3).
const RETRY_BACKOFF_CAP_MILLIS: u64 = 2_000;

/// How long to wait after failed attempt `attempt` (1-based): 50, 100, 200 ms ... capped at 2 s.
/// A reload's lists and a family's re-watches both wait this.
fn retry_backoff(attempt: u32) -> std::time::Duration {
    // Shift at most 16: `checked_shl` only refuses a shift of 64 or more, and bits shifted out
    // below that are lost, so `50 << 63` is 0 — a zero wait at attempt 64 (M9 S2a row
    // `a_reload_waits_twice_as_long_each_time_up_to_two_seconds`). 50 << 16 is far past the cap.
    let doubled = RETRY_BACKOFF_BASE_MILLIS << attempt.saturating_sub(1).min(16);
    std::time::Duration::from_millis(doubled.min(RETRY_BACKOFF_CAP_MILLIS))
}

/// Whether a failed list may be sent again: the store could not be reached, did not answer in
/// time, or was not the leader. Any other error says the request itself is wrong, and retrying
/// it would hide that forever.
pub(crate) const fn transient(error: &ConfigError) -> bool {
    matches!(
        error,
        ConfigError::Unavailable { .. }
            | ConfigError::DeadlineExceededUnknownOutcome
            | ConfigError::NotLeader { .. }
    )
}

/// One reload attempt: a list that failed in a way worth repeating, or the attempt's end.
#[derive(Debug)]
enum Attempt {
    /// The list failed with a [`transient`] error.
    Again(ConfigError),
    /// The snapshot, or the fault that ends the node's wait for one.
    Done(Result<ControlEvent, Fault>),
}

/// List `prefix` once. A truncated list faults the node: a partial family would read as a
/// complete one.
async fn reload_once(store: &dyn ConfigStore, site: Site, prefix: ControlPrefix) -> Attempt {
    let started = Instant::now();
    let name = prefix.encode();
    let listed = store
        .list(ListRequest {
            prefix: Bytes::from_static(name.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        })
        .await;
    let response = match listed {
        Ok(response) => response,
        Err(error) if transient(&error) => {
            log_call(site, "reload", name, "unavailable", started);
            return Attempt::Again(error);
        }
        Err(error) => {
            log_call(site, "reload", name, "failed", started);
            return Attempt::Done(Err(("control_reload_failed", format!("{name}: {error}"))));
        }
    };
    if response.truncated {
        log_call(site, "reload", name, "truncated", started);
        return Attempt::Done(Err(("control_reload_truncated", name.to_owned())));
    }
    let mut records = Vec::with_capacity(response.records.len());
    for record in response.records {
        let key = match decode_key(&record.key) {
            Ok(key) => key,
            Err(fault) => return Attempt::Done(Err(fault)),
        };
        records.push(ControlRecord {
            key,
            revision: Revision(record.mod_revision),
            value: record.value,
        });
    }
    log_reload_found(site, name, response.read_revision, started);
    Attempt::Done(Ok(ControlEvent::FamilySnapshot {
        prefix,
        snapshot_revision: Revision(response.read_revision),
        records,
    }))
}

/// `Reload`: list until the store answers, then post the snapshot. A transient failure waits
/// [`retry_backoff`] and tries again, logged as `control_reload_retry` and counted
/// in `failed` for `Db::node_status`; anything else faults the node, as before (M9 S2a, 2f).
///
/// It answers, snapshot or fault, only while `reloads` still holds its own `failed` for the
/// family, and sends under that lock, which a replacement and `shutdown` both take. `abort` is
/// not enough on its own: it lands at the next await, and the step that answers has none, so a
/// reload replaced inside that step would still post its older answer (M9 S2a review F-002).
async fn reload(
    store: Arc<dyn ConfigStore>,
    links: Arc<Links>,
    reloads: Reloads,
    site: Site,
    prefix: ControlPrefix,
    failed: Arc<AtomicU32>,
) {
    let mut attempt = 0_u32;
    let answer = loop {
        match reload_once(&*store, site, prefix).await {
            Attempt::Done(answer) => break answer,
            Attempt::Again(error) => {
                attempt = attempt.saturating_add(1);
                failed.store(attempt, Ordering::Relaxed);
                tracing::warn!(
                    node = site.node.0,
                    prefix = prefix.encode(),
                    attempt,
                    %error,
                    "control_reload_retry"
                );
                tokio::time::sleep(retry_backoff(attempt)).await;
            }
        }
    };
    failed.store(0, Ordering::Relaxed);
    // `post` and `fault` take only the links' lock, never `reloads`.
    let reloads = lock(&reloads);
    let current = reloads
        .get(&(site.node, prefix))
        .is_some_and(|(_, own)| Arc::ptr_eq(own, &failed));
    if !current {
        tracing::debug!(
            node = site.node.0,
            prefix = prefix.encode(),
            "control_reload_superseded"
        );
        return;
    }
    match answer {
        Ok(event) => post(&links, site, event),
        Err((kind, detail)) => fault(&links, site, kind, detail),
    }
    // Held through the send on purpose: released before it, a replacement could slip in
    // between the check and the post, and no row would go red (PR #38 re-review).
    drop(reloads);
}

async fn cas(
    store: &dyn ConfigStore,
    site: Site,
    started: Instant,
    (request, key, expected, value): (
        ControlRequestId,
        ControlKey,
        Option<Revision>,
        Option<Bytes>,
    ),
) -> Result<ControlEvent, Fault> {
    let name = key.encode();
    // A put spells "must not exist" as revision 0; a delete always names one.
    let required = if value.is_some() {
        Some(expected.map_or(0, |revision| revision.0))
    } else {
        expected.map(|revision| revision.0)
    };
    let (op, answer) = match value {
        Some(value) => (
            "cas_put",
            store
                .put(PutRequest {
                    key: Bytes::from(name.clone()),
                    value,
                    expected_mod_revision: Some(expected.map_or(0, |revision| revision.0)),
                    dedup: None,
                })
                .await,
        ),
        None => {
            let Some(expected) = expected else {
                return Err((
                    "control_delete_without_revision",
                    format!("{name}: a delete must name the revision it removes"),
                ));
            };
            (
                "cas_delete",
                store
                    .delete(DeleteRequest {
                        key: Bytes::from(name.clone()),
                        expected_mod_revision: Some(expected.0),
                        dedup: None,
                    })
                    .await,
            )
        }
    };
    let outcome = cas_outcome(site, &name, answer);
    let committed = match &outcome {
        CasOutcome::Committed(revision) => Some(revision.0),
        _ => None,
    };
    log_revisions(
        site,
        (op, &name, cas_name(&outcome)),
        required,
        committed,
        started,
    );
    Ok(ControlEvent::CasResult {
        request,
        key,
        outcome,
    })
}

fn cas_outcome(
    site: Site,
    name: &str,
    answer: Result<MutationResponse, ConfigError>,
) -> CasOutcome {
    match answer {
        Ok(response) => match response.outcome {
            MutationOutcome::Applied => CasOutcome::Committed(Revision(response.revision)),
            MutationOutcome::Conflict => CasOutcome::Conflict {
                exists: response.exists,
                current: Revision(if response.exists {
                    response.current_mod_revision
                } else {
                    response.revision
                }),
            },
            MutationOutcome::NotFound => CasOutcome::Conflict {
                exists: false,
                current: Revision(response.revision),
            },
        },
        Err(ConfigError::Conflict {
            exists,
            current_mod_revision,
        }) => CasOutcome::Conflict {
            exists,
            current: Revision(current_mod_revision),
        },
        Err(ConfigError::NotFound) => CasOutcome::Conflict {
            exists: false,
            current: Revision(0),
        },
        Err(ConfigError::DeadlineExceededUnknownOutcome) => CasOutcome::Unknown,
        Err(error) => {
            tracing::warn!(node = site.node.0, key = name, %error, "control_cas_failed");
            CasOutcome::Unavailable
        }
    }
}

/// Count a watch that is ending: one more unserved in a row when it served nothing and stayed
/// open less than the longest wait, else none. A watch that stayed open that long is not a
/// tight loop, even when its family was quiet the whole time; without that, an idle family
/// whose stream ends at each rEtcd leader change would creep to the 2 s wait over a day.
fn count_end(unserved: &AtomicU32, served: bool, opened: tokio::time::Instant) {
    let long = opened.elapsed() >= std::time::Duration::from_millis(RETRY_BACKOFF_CAP_MILLIS);
    if served || long {
        unserved.store(0, Ordering::Relaxed);
    } else {
        let _ = unserved.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            Some(count.saturating_add(1))
        });
    }
}

/// `Watch`: open the stream and post what it delivers. When the watches of this family before
/// it ended having served nothing, `unserved` in a row, it first waits [`retry_backoff`] of
/// that count, logged as `control_watch_backoff{node, prefix, from, unserved, millis}`. A1's own
/// re-watch timer is a fixed 50 ms, so without this wait a store that refuses or ends every
/// watch — an rEtcd election, a voter stopped — is asked again every 50 ms for as long as it
/// lasts (M9 S2a carry, tester-s2). Any delivered event or progress mark sets the count back to
/// 0 at once, so a watch replaced while it serves leaves no wait behind it.
async fn watch(
    store: Arc<dyn ConfigStore>,
    links: Arc<Links>,
    site: Site,
    prefix: ControlPrefix,
    from: Revision,
    unserved: Arc<AtomicU32>,
) {
    let name = prefix.encode();
    let before = unserved.load(Ordering::Relaxed);
    if before > 0 {
        let wait = retry_backoff(before);
        tracing::info!(
            node = site.node.0,
            prefix = name,
            from = from.0,
            unserved = before,
            millis = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
            "control_watch_backoff"
        );
        tokio::time::sleep(wait).await;
    }
    let opened_at = tokio::time::Instant::now();
    let mut served = false;
    let started = Instant::now();
    let opened = store
        .watch(WatchRequest {
            prefix: Bytes::from_static(name.as_bytes()),
            start_after_revision: from.0,
            progress_interval: None,
        })
        .await;
    let mut stream = match opened {
        Ok(stream) => {
            log_watch(site, name, "open", from, started);
            stream
        }
        Err(error) => {
            log_watch(site, name, "refused", from, started);
            count_end(&unserved, false, opened_at);
            terminate(&links, site, prefix, from, &error);
            return;
        }
    };
    let mut cursor = from;
    while let Some(item) = stream.next().await {
        let event = match item {
            Ok(WatchItem::Event(event)) => {
                let key = match decode_key(&event.key) {
                    Ok(key) => key,
                    Err((kind, detail)) => return fault(&links, site, kind, detail),
                };
                cursor = Revision(event.revision);
                tracing::info!(
                    node = site.node.0,
                    prefix = name,
                    revision = event.revision,
                    "control_watch_event"
                );
                ControlEvent::Watched {
                    prefix,
                    cursor: WatchCursor { revision: cursor },
                    changes: vec![ControlChange {
                        key,
                        revision: cursor,
                    }],
                }
            }
            Ok(WatchItem::Progress { revision }) => {
                cursor = cursor.max(Revision(revision));
                ControlEvent::WatchProgress {
                    prefix,
                    revision: Revision(revision),
                }
            }
            Err(error) => {
                count_end(&unserved, served, opened_at);
                terminate(&links, site, prefix, cursor, &error);
                return;
            }
        };
        if !served {
            served = true;
            unserved.store(0, Ordering::Relaxed);
        }
        post(&links, site, event);
    }
    // Counted before the end is posted: A1 may ask for the next watch as soon as it hears it.
    count_end(&unserved, served, opened_at);
    terminate(
        &links,
        site,
        prefix,
        cursor,
        &ConfigError::Unavailable {
            reason: "watch stream ended".to_owned(),
        },
    );
}

fn terminate(
    links: &Links,
    site: Site,
    prefix: ControlPrefix,
    from: Revision,
    error: &ConfigError,
) {
    let termination = match error {
        ConfigError::RevisionCompacted {
            minimum_available_revision,
        } => WatchTermination::RevisionCompacted {
            minimum_available_revision: Revision(*minimum_available_revision),
        },
        ConfigError::ResourceExhausted {
            resumable: true, ..
        } => WatchTermination::ResourceExhaustedResumable,
        ConfigError::ResourceExhausted {
            resumable: false, ..
        } => WatchTermination::ResourceExhaustedFatal,
        ConfigError::NotLeader { .. } => WatchTermination::NotLeader,
        _ => WatchTermination::Unavailable,
    };
    tracing::warn!(node = site.node.0, prefix = prefix.encode(), from = from.0, %error, ?termination, "control_watch_terminated");
    post(
        links,
        site,
        ControlEvent::WatchTerminated {
            prefix,
            from,
            termination,
        },
    );
}

fn decode_key(raw: &[u8]) -> Result<ControlKey, Fault> {
    let text =
        std::str::from_utf8(raw).map_err(|_| ("control_key_not_utf8", format!("{raw:?}")))?;
    ControlKey::decode(text)
        .map_err(|error| ("control_key_undecodable", format!("{text}: {error}")))
}

const fn cas_name(outcome: &CasOutcome) -> &'static str {
    match outcome {
        CasOutcome::Committed(_) => "committed",
        CasOutcome::Conflict { .. } => "conflict",
        CasOutcome::Unknown => "unknown",
        _ => "unavailable",
    }
}

const fn read_name(outcome: &ReadOutcome) -> &'static str {
    match outcome {
        ReadOutcome::Found { .. } => "found",
        ReadOutcome::Absent { .. } => "absent",
        _ => "unavailable",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_store::{Script, Scripted};

    const SITE: Site = Site {
        node: NodeId(1),
        boot: crate::host::BOOT,
        partition: PartitionId(1),
        correlation: CorrelationId(0),
    };

    fn response(outcome: MutationOutcome, exists: bool) -> MutationResponse {
        MutationResponse {
            outcome,
            revision: 7,
            exists,
            current_mod_revision: if exists { 5 } else { 0 },
            dedup_hit: false,
            dedup_recorded: false,
        }
    }

    /// ADR-rdb-0015, the one rule a retry loop leans on: a CAS whose outcome is unknown is
    /// `Unknown`, a store that could not be reached is `Unavailable`, and a failed
    /// precondition is `Conflict` with the revision to retry against. F1 blocks on the first,
    /// and treating either of the others like it duplicates a write or drops a right.
    #[test]
    fn cas_answers_keep_unknown_unavailable_and_conflict_apart() {
        let cases = [
            (
                Ok(response(MutationOutcome::Applied, true)),
                CasOutcome::Committed(Revision(7)),
            ),
            (
                Ok(response(MutationOutcome::Conflict, true)),
                CasOutcome::Conflict {
                    exists: true,
                    current: Revision(5),
                },
            ),
            (
                Ok(response(MutationOutcome::Conflict, false)),
                CasOutcome::Conflict {
                    exists: false,
                    current: Revision(7),
                },
            ),
            (
                Ok(response(MutationOutcome::NotFound, false)),
                CasOutcome::Conflict {
                    exists: false,
                    current: Revision(7),
                },
            ),
            (
                Err(ConfigError::Conflict {
                    exists: true,
                    current_mod_revision: 4,
                }),
                CasOutcome::Conflict {
                    exists: true,
                    current: Revision(4),
                },
            ),
            (
                Err(ConfigError::NotFound),
                CasOutcome::Conflict {
                    exists: false,
                    current: Revision(0),
                },
            ),
            (
                Err(ConfigError::DeadlineExceededUnknownOutcome),
                CasOutcome::Unknown,
            ),
            (
                Err(ConfigError::Unavailable {
                    reason: "test".to_owned(),
                }),
                CasOutcome::Unavailable,
            ),
            (
                Err(ConfigError::NotLeader { hint: None }),
                CasOutcome::Unavailable,
            ),
        ];
        for (answer, expected) in cases {
            let shown = format!("{answer:?}");
            assert_eq!(
                cas_outcome(SITE, "partitions/1", answer),
                expected,
                "{shown}"
            );
        }
    }

    /// A watch that ends says why, so the kernel can tell a gap it must reload from a stream it
    /// can resume. Each error reaches the node as one `WatchTerminated` from the cursor reached.
    #[test]
    fn a_watch_that_ends_names_why_to_its_node() {
        let links = Links::new();
        let (tx, rx) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx);
        let cases = [
            (
                ConfigError::RevisionCompacted {
                    minimum_available_revision: 9,
                },
                WatchTermination::RevisionCompacted {
                    minimum_available_revision: Revision(9),
                },
            ),
            (
                ConfigError::ResourceExhausted {
                    detail: String::new(),
                    resumable: true,
                },
                WatchTermination::ResourceExhaustedResumable,
            ),
            (
                ConfigError::ResourceExhausted {
                    detail: String::new(),
                    resumable: false,
                },
                WatchTermination::ResourceExhaustedFatal,
            ),
            (
                ConfigError::NotLeader { hint: None },
                WatchTermination::NotLeader,
            ),
            (
                ConfigError::Unavailable {
                    reason: "test".to_owned(),
                },
                WatchTermination::Unavailable,
            ),
        ];
        for (error, expected) in cases {
            terminate(&links, SITE, ControlPrefix::Partitions, Revision(3), &error);
            match rx.try_recv() {
                Ok(Msg::Control {
                    event:
                        ControlEvent::WatchTerminated {
                            prefix,
                            from,
                            termination,
                        },
                    ..
                }) => assert_eq!(
                    (prefix, from, termination),
                    (ControlPrefix::Partitions, Revision(3), expected),
                    "{error:?}"
                ),
                other => panic!("{error:?}: {other:?}"),
            }
        }
    }

    /// A control call the store could not serve is never mistaken for an answer. A failed get
    /// is `Unavailable`, not `Absent`, which would read as "no record". A reload the store
    /// cannot serve is tried again, never handed over as an empty family (M9 S2a 2f; it used to
    /// fault the node). A failed delete keeps its request, so
    /// the module can match the answer. A delete that names no revision never reaches the store.
    #[test]
    fn a_control_call_the_store_cannot_serve_is_never_an_answer() {
        let store = config_testkit::MemStore::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let key = ControlKey::Partition(PartitionId(1));
        let get = || ControlEffect::Get {
            request: ControlRequestId(1),
            key,
        };
        let delete = |request, expected| ControlEffect::Cas {
            request: ControlRequestId(request),
            key,
            expected,
            value: None,
        };

        let absent = rt.block_on(call(&store, SITE, get()));
        assert!(
            matches!(
                absent,
                Ok(ControlEvent::Value {
                    outcome: ReadOutcome::Absent { .. },
                    ..
                })
            ),
            "{absent:?}"
        );
        let unnamed = rt.block_on(call(&store, SITE, delete(2, None)));
        assert_eq!(
            unnamed.map_err(|(kind, _)| kind),
            Err("control_delete_without_revision")
        );

        store.failing_with(ConfigError::Unavailable {
            reason: "down".to_owned(),
        });
        assert_eq!(
            rt.block_on(call(&store, SITE, get())),
            Ok(ControlEvent::Value {
                request: ControlRequestId(1),
                key,
                outcome: ReadOutcome::Unavailable,
            })
        );
        let reload = rt.block_on(reload_once(&store, SITE, ControlPrefix::Partitions));
        assert!(
            matches!(reload, Attempt::Again(ConfigError::Unavailable { .. })),
            "an unreachable store is tried again, never handed over as an empty family: {reload:?}"
        );
        assert_eq!(
            rt.block_on(call(&store, SITE, delete(3, Some(Revision(4))))),
            Ok(ControlEvent::CasResult {
                request: ControlRequestId(3),
                key,
                outcome: CasOutcome::Unavailable,
            })
        );
    }

    /// How long a row waits for a message or state it expects, before
    /// [`crate::host::test_patience`] stretches it. Only a failing row waits it out.
    const PATIENCE: std::time::Duration = std::time::Duration::from_secs(5);

    /// How long a mailbox must stay empty to show that nothing more was posted. Fixed, not
    /// scaled: it proves an absence, so a loaded host only makes the proof weaker, never a red.
    const QUIET: std::time::Duration = std::time::Duration::from_millis(250);

    /// What a node's mailbox received, as event or fault kind: the first `expected` messages,
    /// each waited for with patience (M9 S2a review F-001), then any more that arrive before
    /// the mailbox stays empty for [`QUIET`]. `expected` 0 is a quiet window alone.
    fn received(rx: &std::sync::mpsc::Receiver<Msg>, expected: usize) -> Vec<String> {
        let name = |msg| match msg {
            Msg::Control { event, .. } => format!("{event:?}"),
            Msg::Fault { kind, .. } => format!("fault {kind}"),
            _ => "another message".to_owned(),
        };
        let patience = crate::host::test_patience(PATIENCE);
        let mut seen = Vec::new();
        while seen.len() < expected {
            match rx.recv_timeout(patience) {
                Ok(msg) => seen.push(name(msg)),
                Err(_) => return seen,
            }
        }
        while let Ok(msg) = rx.recv_timeout(QUIET) {
            seen.push(name(msg));
        }
        seen
    }

    /// Walk 2f (M9 S2a): `control fail list unavailable`, then a gap, faulted node 1 with
    /// `control_reload_failed`. A list the store cannot serve for a while is retried, and the
    /// node gets its snapshot once the store answers.
    #[test]
    fn a_reload_the_store_cannot_serve_yet_is_retried_not_faulted() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            fail_lists: 2,
            ..Script::default()
        }));
        let links = Links::new();
        let (tx, rx) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx);
        let adapter = ControlAdapter::new(store, rt.handle().clone(), links);
        adapter.submit(
            NodeId(1),
            crate::host::BOOT,
            PartitionId(1),
            CorrelationId(0),
            ControlEffect::Reload {
                prefix: ControlPrefix::Partitions,
            },
        );
        let seen = received(&rx, 1);
        adapter.shutdown();
        assert_eq!(
            seen,
            [format!(
                "{:?}",
                ControlEvent::FamilySnapshot {
                    prefix: ControlPrefix::Partitions,
                    snapshot_revision: Revision(0),
                    records: Vec::new(),
                }
            )]
        );
    }

    /// M9 S2a ruling: a reload lists again only when the store may still answer — it was
    /// unreachable, out of time, or not the leader. Any other error, and a list cut short, faults
    /// the node at once: retrying would hide a wrong request, and a partial family would read as
    /// records that do not exist.
    #[test]
    fn a_reload_retries_only_a_store_that_may_still_answer() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store = config_testkit::MemStore::new();
        let transient = [
            ConfigError::Unavailable {
                reason: "down".to_owned(),
            },
            ConfigError::DeadlineExceededUnknownOutcome,
            ConfigError::NotLeader { hint: None },
        ];
        for error in transient {
            store.failing_with(error.clone());
            let attempt = rt.block_on(reload_once(&store, SITE, ControlPrefix::Partitions));
            assert!(
                matches!(attempt, Attempt::Again(_)),
                "{error:?}: {attempt:?}"
            );
        }
        let definite = [
            ConfigError::invalid_argument("wrong"),
            ConfigError::NotFound,
            ConfigError::RevisionCompacted {
                minimum_available_revision: 3,
            },
        ];
        for error in definite {
            store.failing_with(error.clone());
            let attempt = rt.block_on(reload_once(&store, SITE, ControlPrefix::Partitions));
            assert!(
                matches!(
                    attempt,
                    Attempt::Done(Err(("control_reload_failed", ref detail)))
                        if detail.starts_with("partitions/: ")
                ),
                "{error:?}: {attempt:?}"
            );
        }

        let short = config_testkit::MemStore::with_limits(config_core::Limits {
            max_list_items: 1,
            ..config_core::Limits::DEFAULT
        });
        for id in [1, 2] {
            let put = short.put(PutRequest {
                key: Bytes::from(ControlKey::Partition(PartitionId(id)).encode()),
                value: Bytes::from_static(b"x"),
                expected_mod_revision: Some(0),
                dedup: None,
            });
            rt.block_on(put).expect("put");
        }
        let attempt = rt.block_on(reload_once(&short, SITE, ControlPrefix::Partitions));
        assert!(
            matches!(
                attempt,
                Attempt::Done(Err(("control_reload_truncated", ref name))) if name == "partitions/"
            ),
            "{attempt:?}"
        );
    }

    /// M9 S2a ruling: a reload that cannot finish faults its node through the mailbox, instead
    /// of leaving it waiting for a snapshot — both a store error worth no retry and a key under
    /// the family that does not decode.
    #[test]
    fn a_reload_that_cannot_finish_faults_its_node() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let mem = Arc::new(config_testkit::MemStore::new());
        let store: Arc<dyn ConfigStore> = Arc::clone(&mem) as _;
        let links = Links::new();
        let (tx, rx) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx);
        let adapter = ControlAdapter::new(store, rt.handle().clone(), links);
        let reload = || {
            adapter.submit(
                NodeId(1),
                crate::host::BOOT,
                PartitionId(1),
                CorrelationId(0),
                ControlEffect::Reload {
                    prefix: ControlPrefix::Partitions,
                },
            );
            received(&rx, 1)
        };

        mem.failing_with(ConfigError::invalid_argument("wrong"));
        assert_eq!(reload(), ["fault control_reload_failed"]);

        mem.stop_failing();
        let put = mem.put(PutRequest {
            key: Bytes::from_static(b"partitions/x"),
            value: Bytes::from_static(b"x"),
            expected_mod_revision: Some(0),
            dedup: None,
        });
        rt.block_on(put).expect("put");
        assert_eq!(reload(), ["fault control_key_undecodable"]);
        adapter.shutdown();
    }

    /// M9 S2a ruling: the wait between lists doubles from 50 ms and never passes 2 s, however
    /// long the store stays down.
    #[test]
    fn a_reload_waits_twice_as_long_each_time_up_to_two_seconds() {
        let waits: Vec<u128> = [1, 2, 3, 6, 7, 8, 40, 64, 65, u32::MAX]
            .into_iter()
            .map(|attempt| retry_backoff(attempt).as_millis())
            .collect();
        assert_eq!(
            waits,
            [50, 100, 200, 1600, 2000, 2000, 2000, 2000, 2000, 2000]
        );
    }

    /// PR #38 review F-001: the retry loop really waits what [`retry_backoff`] computes. On
    /// paused time the gap between one list and the next is exact: 50 ms after the first
    /// failure, doubling, then held at the 2 s cap, and the snapshot follows the first list
    /// that answers.
    #[test]
    fn a_reload_spaces_its_lists_by_the_backoff() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime");
        let store = Arc::new(Scripted::new(Script {
            fail_lists: 7,
            ..Script::default()
        }));
        let links = Links::new();
        let (tx, rx) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx);
        let adapter = ControlAdapter::new(
            Arc::clone(&store) as Arc<dyn ConfigStore>,
            rt.handle().clone(),
            links,
        );
        adapter.submit(
            NodeId(1),
            crate::host::BOOT,
            PartitionId(1),
            CorrelationId(0),
            ControlEffect::Reload {
                prefix: ControlPrefix::Partitions,
            },
        );
        // Paused time jumps to the next timer whenever every task waits, so this costs no wall
        // time. 60 s of it is far past the 5.15 s the eight lists need.
        let answer = rt.block_on(async {
            let mailbox = async {
                loop {
                    if let Ok(msg) = rx.try_recv() {
                        return msg;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            };
            tokio::time::timeout(std::time::Duration::from_secs(60), mailbox).await
        });
        adapter.shutdown();
        assert!(
            matches!(
                answer,
                Ok(Msg::Control {
                    event: ControlEvent::FamilySnapshot { .. },
                    ..
                })
            ),
            "{answer:?}"
        );
        let gaps: Vec<u128> = store
            .list_arrivals()
            .windows(2)
            .map(|pair| (pair[1] - pair[0]).as_millis())
            .collect();
        assert_eq!(gaps, [50, 100, 200, 400, 800, 1600, 2000]);
    }

    /// PR #38 review F-002, follow-up: a replaced reload that is still retrying stops listing.
    /// The newer one's check keeps the older one from answering; `abort` is what stops it from
    /// listing a down store every 2 s until the store answers. On paused time, with every list
    /// failing, the older reload reaches its second retry, then a newer one replaces it. Over
    /// the next 10 s the store sees exactly one reload's lists: the newer one's, on its own
    /// backoff from its first list.
    #[test]
    fn a_replaced_reload_stops_listing() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime");
        let store = Arc::new(Scripted::new(Script::default()));
        store.mem().failing_with(ConfigError::Unavailable {
            reason: "down".to_owned(),
        });
        let links = Links::new();
        let (tx, _rx) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx);
        let adapter = ControlAdapter::new(
            Arc::clone(&store) as Arc<dyn ConfigStore>,
            rt.handle().clone(),
            links,
        );
        let reload = || {
            adapter.submit(
                NodeId(1),
                crate::host::BOOT,
                PartitionId(1),
                CorrelationId(0),
                ControlEffect::Reload {
                    prefix: ControlPrefix::Partitions,
                },
            );
        };
        let tick = std::time::Duration::from_millis(1);

        // The first list, then retries 1 and 2: three lists, the last at 150 ms.
        reload();
        rt.block_on(async {
            while store.list_arrivals().len() < 3 {
                tokio::time::sleep(tick).await;
            }
        });
        reload();
        // Paused time jumps from timer to timer, so 10 s costs no wall time. No list of the
        // newer reload falls on the window's end (its 10th is at 9150 ms, its 11th at 11150).
        rt.block_on(async { tokio::time::sleep(std::time::Duration::from_secs(10)).await });
        adapter.shutdown();

        let arrivals = store.list_arrivals();
        let first = arrivals[3];
        let offsets: Vec<u128> = arrivals[3..]
            .iter()
            .map(|at| (*at - first).as_millis())
            .collect();
        assert_eq!(
            offsets,
            [0, 50, 150, 350, 750, 1550, 3150, 5150, 7150, 9150],
            "only the newer reload lists once it has replaced the older one"
        );
    }

    /// Polls `ready` every 5 ms, for up to [`PATIENCE`] stretched by the deadline scale.
    fn eventually(ready: impl Fn() -> bool) -> bool {
        let until = Instant::now() + crate::host::test_patience(PATIENCE);
        while Instant::now() < until {
            if ready() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        false
    }

    /// M9 S2b review F-005: `forget_node(n)` stops node `n`'s control work and nobody else's.
    /// Nodes 1 and 2 each hold a watch waiting to open, a reload held after its list read, and a
    /// CAS waiting at the store's gate (`MemStore` serves no watches, so a watch that opens
    /// answers `WatchTerminated`: that answer is the watch's sign of life). Node 2 is forgotten,
    /// then everything is let through: node 1 gets all three answers, node 2 gets none, and its
    /// put never reaches the store. A slip that drains every node, or inverts a filter, takes
    /// node 1's work and goes red here. Unit (~300 ms, mostly the quiet window).
    #[test]
    fn forgetting_one_node_stops_its_control_work_and_leaves_the_others() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let store = Arc::new(Scripted::gated_with(Script {
            hold_lists: 2,
            hold_watches: 2,
            ..Script::default()
        }));
        let links = Links::new();
        let (tx1, rx1) = std::sync::mpsc::channel();
        let (tx2, rx2) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx1);
        links.register(NodeId(2), tx2);
        let adapter = ControlAdapter::new(
            Arc::clone(&store) as Arc<dyn ConfigStore>,
            rt.handle().clone(),
            links,
        );
        let submit = |node, effect| {
            adapter.submit(
                node,
                crate::host::BOOT,
                PartitionId(1),
                CorrelationId(0),
                effect,
            );
        };
        let waited = |signal: &tokio::sync::Notify, what: &str| {
            let arrived = rt.block_on(async {
                tokio::time::timeout(crate::host::test_patience(PATIENCE), signal.notified()).await
            });
            assert!(arrived.is_ok(), "{what} did not arrive");
        };
        let grant = |node| ControlKey::Grant(NodeId(node));
        for node in [1, 2] {
            submit(
                NodeId(node),
                ControlEffect::Watch {
                    prefix: ControlPrefix::Partitions,
                    from: Revision(0),
                },
            );
            waited(&store.watch_held, "the watch");
            submit(
                NodeId(node),
                ControlEffect::Reload {
                    prefix: ControlPrefix::Grants,
                },
            );
            waited(&store.list_held, "the reload's list");
            submit(
                NodeId(node),
                ControlEffect::Cas {
                    request: ControlRequestId(u64::from(node)),
                    key: grant(node),
                    expected: None,
                    value: Some(Bytes::from_static(b"grant")),
                },
            );
            waited(&store.put_arrived, "the CAS's put");
        }

        adapter.forget_node(NodeId(2), crate::host::BOOT);
        for _ in [1, 2] {
            store.release();
            store.release_lists();
            store.release_watches();
        }

        let kind = |seen: Vec<String>| {
            let mut kinds: Vec<String> = seen
                .iter()
                .map(|name| name.split([' ', '{']).next().unwrap_or_default().to_owned())
                .collect();
            kinds.sort();
            kinds
        };
        assert_eq!(
            kind(received(&rx1, 3)),
            ["CasResult", "FamilySnapshot", "WatchTerminated"],
            "node 1's watch, reload and CAS all answer"
        );
        assert_eq!(
            received(&rx2, 0),
            Vec::<String>::new(),
            "nothing of the forgotten node's answers"
        );
        let unapplied = rt
            .block_on(store.mem().get(config_core::GetRequest {
                key: Bytes::from(grant(2).encode()),
            }))
            .expect("get grants/2");
        assert!(unapplied.record.is_none(), "{unapplied:?}");
        adapter.shutdown();
    }

    /// M9 S2a ruling: only the newest reload of a family answers. A reload whose list read the
    /// family and is still in flight when a newer one starts never posts: its snapshot is older
    /// than the newer one's, and posted after it would roll the node's cache back. The two
    /// snapshots differ, so a reload that kept the oldest instead goes red too (review F-002).
    /// While a reload retries, `reload_pending` names its node's failed lists; a newer reload
    /// replaces a retrying one, so the node gets one snapshot, not one per reload. Shutdown
    /// abandons a retrying reload, so nothing reaches the node after.
    #[test]
    fn the_newest_reload_wins_and_shutdown_abandons_a_retrying_one() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let store = Arc::new(Scripted::new(Script {
            hold_lists: 1,
            ..Script::default()
        }));
        let links = Links::new();
        let (tx, rx) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx);
        let adapter = ControlAdapter::new(
            Arc::clone(&store) as Arc<dyn ConfigStore>,
            rt.handle().clone(),
            links,
        );
        let reload = || {
            adapter.submit(
                NodeId(1),
                crate::host::BOOT,
                PartitionId(1),
                CorrelationId(0),
                ControlEffect::Reload {
                    prefix: ControlPrefix::Partitions,
                },
            );
        };
        let down = || {
            store.mem().failing_with(ConfigError::Unavailable {
                reason: "down".to_owned(),
            });
        };

        // The older reload reads the empty family, and its answer is held in flight.
        reload();
        let held = rt.block_on(async {
            tokio::time::timeout(
                crate::host::test_patience(PATIENCE),
                store.list_held.notified(),
            )
            .await
        });
        assert!(held.is_ok(), "the older reload's list did not read");
        let key = ControlKey::Partition(PartitionId(1));
        let written = rt
            .block_on(store.put(PutRequest {
                key: Bytes::from(key.encode()),
                value: Bytes::from_static(b"newer"),
                expected_mod_revision: Some(0),
                dedup: None,
            }))
            .expect("put");
        // The newer reload reads the record; then the older one's list may answer.
        reload();
        store.release_lists();
        let newer = format!(
            "{:?}",
            ControlEvent::FamilySnapshot {
                prefix: ControlPrefix::Partitions,
                snapshot_revision: Revision(written.revision),
                records: vec![ControlRecord {
                    key,
                    revision: Revision(written.revision),
                    value: Bytes::from_static(b"newer"),
                }],
            }
        );
        assert_eq!(
            received(&rx, 1),
            std::slice::from_ref(&newer),
            "only the newer reload answers, with the newer family"
        );

        down();
        reload();
        assert!(
            eventually(|| adapter.reload_pending(NodeId(1)).is_some()),
            "a retrying reload is reported"
        );
        assert_eq!(adapter.reload_pending(NodeId(2)), None, "only for its node");
        reload();
        // The newer reload replaced the older one in the map, so this count is its own: it
        // has failed too, and must be put back to 0 when it answers.
        assert!(
            eventually(|| adapter.reload_pending(NodeId(1)).is_some()),
            "the newer reload retries too"
        );
        store.mem().stop_failing();
        assert_eq!(received(&rx, 1), [newer], "the older reload never posts");
        assert_eq!(
            adapter.reload_pending(NodeId(1)),
            None,
            "done is not pending"
        );

        down();
        reload();
        assert!(eventually(|| adapter.reload_pending(NodeId(1)).is_some()));
        adapter.shutdown();
        store.mem().stop_failing();
        assert_eq!(
            received(&rx, 0),
            Vec::<String>::new(),
            "a reload abandoned at shutdown posts nothing"
        );
    }

    /// PR #38 review F-002: `abort` lands only at an await, so a reload replaced while it is
    /// already inside the step that answers would still post its older snapshot. Here the older
    /// reload's list has read the empty family and blocks its worker thread inside that step;
    /// a record is written; the newer reload answers with it; then the older one finishes. Only
    /// the newer snapshot ever reaches the node.
    #[test]
    fn a_reload_replaced_inside_its_last_step_never_posts() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let store = Arc::new(Scripted::new(Script {
            block_lists: 1,
            ..Script::default()
        }));
        let links = Links::new();
        let (tx, rx) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx);
        let adapter = ControlAdapter::new(
            Arc::clone(&store) as Arc<dyn ConfigStore>,
            rt.handle().clone(),
            links,
        );
        let reload = || {
            adapter.submit(
                NodeId(1),
                crate::host::BOOT,
                PartitionId(1),
                CorrelationId(0),
                ControlEffect::Reload {
                    prefix: ControlPrefix::Partitions,
                },
            );
        };
        let patience = crate::host::test_patience(PATIENCE);

        reload();
        assert!(
            store.list_blocked(patience),
            "the older reload's list did not read"
        );
        let key = ControlKey::Partition(PartitionId(1));
        let written = rt
            .block_on(store.put(PutRequest {
                key: Bytes::from(key.encode()),
                value: Bytes::from_static(b"newer"),
                expected_mod_revision: Some(0),
                dedup: None,
            }))
            .expect("put");
        reload();
        let newer = format!(
            "{:?}",
            ControlEvent::FamilySnapshot {
                prefix: ControlPrefix::Partitions,
                snapshot_revision: Revision(written.revision),
                records: vec![ControlRecord {
                    key,
                    revision: Revision(written.revision),
                    value: Bytes::from_static(b"newer"),
                }],
            }
        );
        assert_eq!(
            received(&rx, 1),
            [newer],
            "the newer reload answers while the older one is blocked"
        );

        store.unblock_lists();
        // Shutting the runtime down waits for the poll the older reload is in to end, and that
        // poll is the one that would post.
        rt.shutdown_timeout(patience);
        let late: Vec<String> = rx
            .try_iter()
            .map(|msg| match msg {
                Msg::Control { event, .. } => format!("{event:?}"),
                Msg::Fault { kind, .. } => format!("fault {kind}"),
                _ => "another message".to_owned(),
            })
            .collect();
        assert_eq!(
            late,
            Vec::<String>::new(),
            "the replaced reload never posts"
        );
    }

    type Reply<'a, T> =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, ConfigError>> + Send + 'a>>;

    /// One scripted watch.
    enum Stream {
        /// The open is refused `Unavailable`, as a voter with no leader refuses it.
        Refused,
        /// The stream's items, then this long open with nothing more, then its end.
        Open(Vec<Result<WatchItem, ConfigError>>, std::time::Duration),
    }

    impl Stream {
        /// `items`, then the end at once.
        fn of(items: Vec<Result<WatchItem, ConfigError>>) -> Self {
            Self::Open(items, std::time::Duration::ZERO)
        }
    }

    /// A store that serves scripted watch streams and nothing else: per prefix, each watch
    /// takes the next stream, and once they run out gets one that ends at once with nothing.
    /// It records when each watch arrived, on tokio's clock. `MemStore` refuses every watch, so
    /// without this no lib test reaches the body of [`watch`]. Written out by hand because
    /// `async-trait` is not a dependency of this crate.
    struct ScriptedWatch {
        streams: Mutex<BTreeMap<String, std::collections::VecDeque<Stream>>>,
        arrivals: Mutex<Vec<tokio::time::Instant>>,
    }

    impl ScriptedWatch {
        fn new(streams: BTreeMap<String, Vec<Stream>>) -> Self {
            Self {
                streams: Mutex::new(
                    streams
                        .into_iter()
                        .map(|(prefix, streams)| (prefix, streams.into()))
                        .collect(),
                ),
                arrivals: Mutex::new(Vec::new()),
            }
        }
    }

    fn unserved<'a, T: Send + 'a>() -> Reply<'a, T> {
        Box::pin(std::future::ready(Err(ConfigError::Unavailable {
            reason: "the scripted store serves watches only".to_owned(),
        })))
    }

    impl ConfigStore for ScriptedWatch {
        fn get<'a, 'b>(&'a self, _: GetRequest) -> Reply<'b, config_core::GetResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            unserved()
        }

        fn list<'a, 'b>(&'a self, _: ListRequest) -> Reply<'b, config_core::ListResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            unserved()
        }

        fn put<'a, 'b>(&'a self, _: PutRequest) -> Reply<'b, MutationResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            unserved()
        }

        fn delete<'a, 'b>(&'a self, _: DeleteRequest) -> Reply<'b, MutationResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            unserved()
        }

        fn capabilities(&self) -> config_core::Capabilities {
            config_core::Capabilities::EPHEMERAL_DEVELOPMENT
        }

        fn watch<'a, 'b>(&'a self, request: WatchRequest) -> Reply<'b, config_core::WatchStream>
        where
            'a: 'b,
            Self: 'b,
        {
            lock(&self.arrivals).push(tokio::time::Instant::now());
            let prefix = String::from_utf8_lossy(&request.prefix).into_owned();
            let next = lock(&self.streams)
                .get_mut(&prefix)
                .and_then(std::collections::VecDeque::pop_front)
                .unwrap_or_else(|| Stream::of(Vec::new()));
            let Stream::Open(items, open_for) = next else {
                return Box::pin(std::future::ready(Err(ConfigError::Unavailable {
                    reason: "scripted: the watch was refused".to_owned(),
                })));
            };
            // Only a stream that stays open needs tokio's clock.
            let quiet = futures::stream::once(async move {
                if !open_for.is_zero() {
                    tokio::time::sleep(open_for).await;
                }
            })
            .filter_map(|()| std::future::ready(None));
            let stream: config_core::WatchStream =
                Box::pin(futures::stream::iter(items).chain(quiet));
            Box::pin(std::future::ready(Ok(stream)))
        }
    }

    /// M10 S2 (#57 walk, lead to W7): each rEtcd event a watch delivers is logged
    /// `control_watch_event` at info, so a walk at rdb_dev's default level sees a change reach a
    /// node. It was debug, below what a walk reads. Cut: back to debug, and `@l` reads `Debug`.
    /// Unit.
    #[config_log::retcd_test]
    fn a_delivered_watch_event_is_logged_at_info() {
        let script = BTreeMap::from([(
            ControlPrefix::Partitions.encode().to_owned(),
            vec![Stream::of(vec![Ok(WatchItem::Event(
                config_core::MutationEvent {
                    revision: 4,
                    key: Bytes::from_static(b"partitions/1"),
                    kind: config_core::MutationEventKind::Delete,
                },
            ))])],
        )]);
        let store: Arc<dyn ConfigStore> = Arc::new(ScriptedWatch::new(script));
        let links = Links::new();
        let (tx, _rx) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(watch(
            store,
            links,
            SITE,
            ControlPrefix::Partitions,
            Revision(2),
            Arc::new(AtomicU32::new(0)),
        ));
        let lines: Vec<_> = config_testkit::logs::lines_for_current_test(
            module_path!(),
            "a_delivered_watch_event_is_logged_at_info",
        )
        .into_iter()
        .filter(|line| line["@m"] == "control_watch_event")
        .collect();
        assert_eq!(lines.len(), 1, "{lines:#?}");
        assert_eq!(lines[0]["@l"], "Information", "{}", lines[0]);
        assert_eq!(lines[0]["revision"], 4, "{}", lines[0]);
    }

    /// A resume point is only right if it is the furthest revision the node was told about:
    /// earlier replays a change, later skips one. Each change reaches the node with its
    /// revision, a progress mark moves the cursor without a change, and both a stream error and
    /// a plain end of stream terminate from the furthest revision seen. A key that no family
    /// owns faults the node instead of landing in the wrong cache.
    #[test]
    fn a_watch_stream_ends_from_the_furthest_revision_it_delivered() {
        let event = |revision, key: &str| {
            Ok(WatchItem::Event(config_core::MutationEvent {
                revision,
                key: Bytes::from(key.to_owned()),
                kind: config_core::MutationEventKind::Delete,
            }))
        };
        let script = BTreeMap::from([
            (
                ControlPrefix::Partitions.encode().to_owned(),
                vec![
                    event(4, "partitions/1"),
                    Ok(WatchItem::Progress { revision: 6 }),
                    Err(ConfigError::Unavailable {
                        reason: "lost".to_owned(),
                    }),
                ],
            ),
            (
                ControlPrefix::Nodes.encode().to_owned(),
                vec![event(5, "nodes/2")],
            ),
            (
                ControlPrefix::Grants.encode().to_owned(),
                vec![event(7, "grants/x"), event(8, "grants/1")],
            ),
        ]);
        let script = script
            .into_iter()
            .map(|(prefix, items)| (prefix, vec![Stream::of(items)]))
            .collect();
        let store: Arc<dyn ConfigStore> = Arc::new(ScriptedWatch::new(script));
        let links = Links::new();
        let (tx, rx) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let run = |prefix, from| {
            rt.block_on(watch(
                Arc::clone(&store),
                Arc::clone(&links),
                SITE,
                prefix,
                Revision(from),
                Arc::new(AtomicU32::new(0)),
            ));
            rx.try_iter()
                .map(|msg| match msg {
                    Msg::Control { event, .. } => Ok(event),
                    Msg::Fault { kind, .. } => Err(kind),
                    _ => Err("not a control message"),
                })
                .collect::<Vec<_>>()
        };
        let unavailable = WatchTermination::Unavailable;

        assert_eq!(
            run(ControlPrefix::Partitions, 2),
            [
                Ok(ControlEvent::Watched {
                    prefix: ControlPrefix::Partitions,
                    cursor: WatchCursor {
                        revision: Revision(4)
                    },
                    changes: vec![ControlChange {
                        key: ControlKey::Partition(PartitionId(1)),
                        revision: Revision(4),
                    }],
                }),
                Ok(ControlEvent::WatchProgress {
                    prefix: ControlPrefix::Partitions,
                    revision: Revision(6),
                }),
                Ok(ControlEvent::WatchTerminated {
                    prefix: ControlPrefix::Partitions,
                    from: Revision(6),
                    termination: unavailable,
                }),
            ],
            "a stream error ends from the progress mark"
        );
        assert_eq!(
            run(ControlPrefix::Nodes, 3),
            [
                Ok(ControlEvent::Watched {
                    prefix: ControlPrefix::Nodes,
                    cursor: WatchCursor {
                        revision: Revision(5)
                    },
                    changes: vec![ControlChange {
                        key: ControlKey::Node(NodeId(2)),
                        revision: Revision(5),
                    }],
                }),
                Ok(ControlEvent::WatchTerminated {
                    prefix: ControlPrefix::Nodes,
                    from: Revision(5),
                    termination: unavailable,
                }),
            ],
            "a stream that just ends ends from its last change"
        );
        assert_eq!(
            run(ControlPrefix::Grants, 0),
            [Err("control_key_undecodable")],
            "an unknown key faults the node and nothing after it is delivered"
        );
    }

    /// Drive node 1's partitions watch the way A1 does, minus its own 50 ms timer: each time
    /// a watch ends, submit the next one at once; with `replace`, also each time one delivers a
    /// change, replacing it while it serves. The store serves `streams` in turn. Returns
    /// the gaps, in ms, between the first `watches` watches reaching the store. On paused time,
    /// so it costs no wall time; the mailbox is polled each 1 ms, so a gap can read up to 1 ms
    /// over the wait itself.
    fn rewatch_gaps(streams: Vec<Stream>, watches: usize, replace: bool) -> Vec<u128> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime");
        let prefix = ControlPrefix::Partitions;
        let store = Arc::new(ScriptedWatch::new(BTreeMap::from([(
            prefix.encode().to_owned(),
            streams,
        )])));
        let links = Links::new();
        let (tx, rx) = std::sync::mpsc::channel();
        links.register(NodeId(1), tx);
        let adapter = ControlAdapter::new(
            Arc::clone(&store) as Arc<dyn ConfigStore>,
            rt.handle().clone(),
            links,
        );
        let rewatch = || {
            adapter.submit(
                NodeId(1),
                crate::host::BOOT,
                PartitionId(1),
                CorrelationId(0),
                ControlEffect::Watch {
                    prefix,
                    from: Revision(0),
                },
            );
        };
        rewatch();
        rt.block_on(async {
            while lock(&store.arrivals).len() < watches {
                while let Ok(msg) = rx.try_recv() {
                    match msg {
                        Msg::Control {
                            event: ControlEvent::WatchTerminated { .. },
                            ..
                        } => rewatch(),
                        Msg::Control {
                            event: ControlEvent::Watched { .. },
                            ..
                        } if replace => rewatch(),
                        _ => {}
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });
        adapter.shutdown();
        let arrivals = lock(&store.arrivals).clone();
        arrivals[..watches]
            .windows(2)
            .map(|pair| (pair[1] - pair[0]).as_millis())
            .collect()
    }

    /// Whether each gap is its expected wait, or at most the 1 ms poll over it.
    fn waited(gaps: &[u128], expected: &[u128]) -> bool {
        gaps.len() == expected.len()
            && gaps
                .iter()
                .zip(expected)
                .all(|(gap, wait)| (*wait..=wait + 1).contains(gap))
    }

    /// M9 S2a carry (tester-s2): during an rEtcd election every watch ended at once, and A1
    /// re-watched every 50 ms for as long as it lasted, because only a reload backed off. A
    /// watch that ends having served nothing now makes the next watch of its family wait, 50 ms
    /// after the first such end and doubling up to 2 s, as a reload does. Each way a store
    /// serves nothing: it refuses the open (the walk saw `outcome=refused`), the stream fails
    /// first thing, or it ends at once.
    #[test]
    fn a_rewatch_after_watches_that_served_nothing_waits_twice_as_long_each_time() {
        let expected = [50, 100, 200, 400, 800, 1600, 2000, 2000, 2000];
        let refused = rewatch_gaps((0..10).map(|_| Stream::Refused).collect(), 10, false);
        assert!(
            waited(&refused, &expected),
            "refused: {refused:?}, expected {expected:?}"
        );
        let failed = rewatch_gaps(
            (0..10)
                .map(|_| Stream::of(vec![Err(ConfigError::NotLeader { hint: None })]))
                .collect(),
            10,
            false,
        );
        assert!(
            waited(&failed, &expected),
            "failed: {failed:?}, expected {expected:?}"
        );
        let ended = rewatch_gaps(Vec::new(), 10, false);
        assert!(
            waited(&ended, &expected),
            "ended: {ended:?}, expected {expected:?}"
        );
    }

    /// A watch replaced while it serves leaves no wait behind it, though it never ends: its
    /// first delivery puts the count back to 0 then, not at its end. An unserved watch (50 ms
    /// before the next), then one that delivers a change and stays open; the change is answered
    /// with a new watch, which opens at once.
    #[test]
    fn a_watch_replaced_while_it_serves_leaves_no_wait() {
        let serving = Stream::Open(
            vec![Ok(WatchItem::Event(config_core::MutationEvent {
                revision: 4,
                key: Bytes::from_static(b"partitions/1"),
                kind: config_core::MutationEventKind::Delete,
            }))],
            std::time::Duration::from_secs(60),
        );
        let gaps = rewatch_gaps(vec![Stream::of(Vec::new()), serving], 3, true);
        assert!(waited(&gaps, &[50, 0]), "{gaps:?}, expected [50, 0]");
    }

    /// The wait is for a store that serves nothing, never for a family that is merely quiet. A
    /// watch that delivers a change puts the count back to 0, so the next watch opens at once;
    /// so does one that stayed open the longest wait, 2 s, with nothing to deliver. One that
    /// ends 1 ms short of that still counts. Watches, in turn: two unserved (50, then 100 ms
    /// before the next), one that serves (0 ms), one unserved (50 ms), one open 2 s (none: its
    /// 2 s life is the gap), one open 1999 ms (its life plus 50 ms), one unserved (100 ms).
    #[test]
    fn a_rewatch_waits_only_after_watches_that_served_nothing_and_ended_soon() {
        let served = Stream::of(vec![Ok(WatchItem::Event(config_core::MutationEvent {
            revision: 4,
            key: Bytes::from_static(b"partitions/1"),
            kind: config_core::MutationEventKind::Delete,
        }))]);
        let quiet = |millis| Stream::Open(Vec::new(), std::time::Duration::from_millis(millis));
        let streams = vec![
            Stream::of(Vec::new()),
            Stream::of(Vec::new()),
            served,
            Stream::of(Vec::new()),
            quiet(2_000),
            quiet(1_999),
            Stream::of(Vec::new()),
        ];
        let gaps = rewatch_gaps(streams, 8, false);
        let expected = [50, 100, 0, 50, 2000, 2049, 100];
        assert!(waited(&gaps, &expected), "{gaps:?}, expected {expected:?}");
    }
}
