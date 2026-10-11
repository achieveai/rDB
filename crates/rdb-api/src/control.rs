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
//! Every call logs `control_call{node, op, key, outcome, millis}`.

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
use rdb_core::contracts::ids::{ControlRequestId, CorrelationId, NodeId, PartitionId, Revision};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::host::Msg;
use crate::transport::Links;

/// Where a completion goes: the asking node, and the effect's partition and correlation.
#[derive(Debug, Clone, Copy)]
struct Site {
    node: NodeId,
    partition: PartitionId,
    correlation: CorrelationId,
}

/// The newest reload of a family, running or done, and how many of its lists have failed so
/// far (0 once it answers).
type Reloading = (JoinHandle<()>, Arc<AtomicU32>);

/// The control binding for every node of one process.
pub struct ControlAdapter {
    store: Arc<dyn ConfigStore>,
    rt: Handle,
    links: Arc<Links>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    watches: Mutex<BTreeMap<(NodeId, ControlPrefix), JoinHandle<()>>>,
    /// The reload of each (node, prefix). A newer one aborts the one it replaces.
    reloads: Mutex<BTreeMap<(NodeId, ControlPrefix), Reloading>>,
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
            tasks: Mutex::new(Vec::new()),
            watches: Mutex::new(BTreeMap::new()),
            reloads: Mutex::new(BTreeMap::new()),
        })
    }

    /// Carry out `effect` for `node`; its completion arrives in the node's mailbox.
    pub fn submit(
        &self,
        node: NodeId,
        partition: PartitionId,
        correlation: CorrelationId,
        effect: ControlEffect,
    ) {
        let site = Site {
            node,
            partition,
            correlation,
        };
        let store = Arc::clone(&self.store);
        let links = Arc::clone(&self.links);
        if let ControlEffect::Reload { prefix } = effect {
            // Only the newest reload of a family may answer: an older snapshot posted after a
            // newer one would roll the node's cache back.
            let failed = Arc::new(AtomicU32::new(0));
            let task = self
                .rt
                .spawn(reload(store, links, site, prefix, Arc::clone(&failed)));
            if let Some((old, _)) = lock(&self.reloads).insert((node, prefix), (task, failed)) {
                old.abort();
            }
            return;
        }
        if let ControlEffect::Watch { prefix, from } = effect {
            let task = self.rt.spawn(watch(store, links, site, prefix, from));
            if let Some(old) = lock(&self.watches).insert((node, prefix), task) {
                old.abort();
            }
            return;
        }
        let task = self.rt.spawn(async move {
            match call(&*store, site, effect).await {
                Ok(event) => post(&links, site, event),
                Err((kind, detail)) => fault(&links, site.node, kind, detail),
            }
        });
        let mut tasks = lock(&self.tasks);
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    /// While a reload of `node`'s is still retrying, how many of its lists have failed: the
    /// highest count over its families (`rdb_dev nodes`: `reload pending attempt=N`). A reload
    /// puts its count back to 0 before it answers, and the only reloads ever stopped early are
    /// dropped from the map as they are (a newer one replaces it, or `shutdown` takes them
    /// all), so a count above 0 is always a reload still retrying.
    #[must_use]
    pub fn reload_pending(&self, node: NodeId) -> Option<u32> {
        lock(&self.reloads)
            .iter()
            .filter(|((at, _), _)| *at == node)
            .map(|(_, (_, failed))| failed.load(Ordering::Relaxed))
            .filter(|failed| *failed > 0)
            .max()
    }

    /// Stop every watch and abandon every call still running, reloads included. Called before
    /// the store goes.
    pub fn shutdown(&self) {
        for (_, task) in std::mem::take(&mut *lock(&self.watches)) {
            task.abort();
        }
        for (_, (task, _)) in std::mem::take(&mut *lock(&self.reloads)) {
            task.abort();
        }
        for task in std::mem::take(&mut *lock(&self.tasks)) {
            task.abort();
        }
    }
}

fn post(links: &Links, site: Site, event: ControlEvent) {
    let msg = Msg::Control {
        partition: site.partition,
        correlation: site.correlation,
        event,
    };
    if links.post(site.node, msg).is_err() {
        tracing::debug!(node = site.node.0, "control_completion_unclaimed");
    }
}

fn fault(links: &Links, node: NodeId, kind: &'static str, detail: String) {
    tracing::error!(node = node.0, kind, %detail, "control_fault");
    let _ = links.post(node, Msg::Fault { kind, detail });
}

fn log_call(site: Site, op: &str, key: &str, outcome: &str, started: Instant) {
    let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    tracing::info!(node = site.node.0, op, key, outcome, millis, "control_call");
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
            log_call(site, "get", &name, read_name(&outcome), started);
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

/// The first wait before a reload's list is sent again, doubled per attempt.
const RELOAD_BACKOFF_BASE_MILLIS: u64 = 50;
/// The longest wait between two attempts (M9 S2a ruling, critic A3).
const RELOAD_BACKOFF_CAP_MILLIS: u64 = 2_000;

/// How long to wait after failed attempt `attempt` (1-based): 50, 100, 200 ms ... capped at 2 s.
fn reload_backoff(attempt: u32) -> std::time::Duration {
    // Shift at most 16: `checked_shl` only refuses a shift of 64 or more, and bits shifted out
    // below that are lost, so `50 << 63` is 0 — a zero wait at attempt 64 (M9 S2a row
    // `a_reload_waits_twice_as_long_each_time_up_to_two_seconds`). 50 << 16 is far past the cap.
    let doubled = RELOAD_BACKOFF_BASE_MILLIS << attempt.saturating_sub(1).min(16);
    std::time::Duration::from_millis(doubled.min(RELOAD_BACKOFF_CAP_MILLIS))
}

/// Whether a failed list may be sent again: the store could not be reached, did not answer in
/// time, or was not the leader. Any other error says the request itself is wrong, and retrying
/// it would hide that forever.
const fn transient(error: &ConfigError) -> bool {
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
/// [`reload_backoff`] and tries again, logged as `control_reload_retry` and counted
/// in `failed` for `Db::node_status`; anything else faults the node, as before (M9 S2a, 2f).
async fn reload(
    store: Arc<dyn ConfigStore>,
    links: Arc<Links>,
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
                tokio::time::sleep(reload_backoff(attempt)).await;
            }
        }
    };
    failed.store(0, Ordering::Relaxed);
    match answer {
        Ok(event) => post(&links, site, event),
        Err((kind, detail)) => fault(&links, site.node, kind, detail),
    }
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
    log_call(site, op, &name, cas_name(&outcome), started);
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

async fn watch(
    store: Arc<dyn ConfigStore>,
    links: Arc<Links>,
    site: Site,
    prefix: ControlPrefix,
    from: Revision,
) {
    let name = prefix.encode();
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
                    Err((kind, detail)) => return fault(&links, site.node, kind, detail),
                };
                cursor = Revision(event.revision);
                tracing::debug!(
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
                terminate(&links, site, prefix, cursor, &error);
                return;
            }
        };
        post(&links, site, event);
    }
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
            .map(|attempt| reload_backoff(attempt).as_millis())
            .collect();
        assert_eq!(
            waits,
            [50, 100, 200, 1600, 2000, 2000, 2000, 2000, 2000, 2000]
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

    type Reply<'a, T> =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, ConfigError>> + Send + 'a>>;

    /// A store that serves one scripted watch stream per prefix and nothing else. `MemStore`
    /// refuses every watch, so without this no lib test reaches the body of [`watch`].
    /// Written out by hand because `async-trait` is not a dependency of this crate.
    struct ScriptedWatch(Mutex<BTreeMap<String, Vec<Result<WatchItem, ConfigError>>>>);

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
            let prefix = String::from_utf8_lossy(&request.prefix).into_owned();
            let items = lock(&self.0).remove(&prefix).unwrap_or_default();
            let stream: config_core::WatchStream = Box::pin(futures::stream::iter(items));
            Box::pin(std::future::ready(Ok(stream)))
        }
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
        let store: Arc<dyn ConfigStore> = Arc::new(ScriptedWatch(Mutex::new(script)));
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
}
