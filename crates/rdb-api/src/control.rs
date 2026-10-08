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

/// The control binding for every node of one process.
pub struct ControlAdapter {
    store: Arc<dyn ConfigStore>,
    rt: Handle,
    links: Arc<Links>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    watches: Mutex<BTreeMap<(NodeId, ControlPrefix), JoinHandle<()>>>,
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

    /// Stop every watch and abandon every call still running. Called before the store goes.
    pub fn shutdown(&self) {
        for (_, task) in std::mem::take(&mut *lock(&self.watches)) {
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
        ControlEffect::Reload { prefix } => {
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
                Err(error) => {
                    log_call(site, "reload", name, "unavailable", started);
                    return Err(("control_reload_failed", format!("{name}: {error}")));
                }
            };
            if response.truncated {
                log_call(site, "reload", name, "truncated", started);
                return Err(("control_reload_truncated", name.to_owned()));
            }
            let mut records = Vec::with_capacity(response.records.len());
            for record in response.records {
                let key = decode_key(&record.key)?;
                records.push(ControlRecord {
                    key,
                    revision: Revision(record.mod_revision),
                    value: record.value,
                });
            }
            log_call(site, "reload", name, "found", started);
            Ok(ControlEvent::FamilySnapshot {
                prefix,
                snapshot_revision: Revision(response.read_revision),
                records,
            })
        }
        // Spawned on its own by `submit`; never reaches here.
        ControlEffect::Watch { .. } => Err(("control_watch_misrouted", String::new())),
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
            log_call(site, "watch", name, "open", started);
            stream
        }
        Err(error) => {
            log_call(site, "watch", name, "refused", started);
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
    /// is `Unavailable`, not `Absent`, which would read as "no record". A failed reload faults
    /// the node instead of handing it an empty family. A failed delete keeps its request, so
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
        let reload = rt.block_on(call(
            &store,
            SITE,
            ControlEffect::Reload {
                prefix: ControlPrefix::Partitions,
            },
        ));
        assert_eq!(
            reload.map_err(|(kind, _)| kind),
            Err("control_reload_failed")
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
