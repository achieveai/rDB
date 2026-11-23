//! Runtime data membership (#80 S4, ADR-rdb-0025 Proposed): adding a data node to a declared
//! database by creating its `nodes/{id}` record. The formation record, the reservation,
//! `routes/0` and generation-0 placement never change; the records are the membership.

use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ConfigError, ConfigStore, GetRequest, MutationOutcome, PutRequest, WatchItem, WatchRequest,
};
use rdb_core::authority::node::{machine_refusal, parse_addr, NodeRecord, MAX_ADDR_LEN};
use rdb_core::authority::partition::{PartitionRecord, RecordMember};
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::control::ControlPrefix;
use rdb_core::contracts::ids::NodeId;
use tokio_stream::StreamExt;

use super::{
    control_tls_refusal, list_whole, machine_clash, read_formation, read_nodes, read_nodes_at,
    read_removed_nodes, DeclareError,
};
use crate::net::Net;
use crate::Db;

/// How long [`watch_members`] waits before it lists again after a failed list or watch.
const WATCH_RETRY: Duration = Duration::from_millis(250);

/// Keep `net`'s members the live `nodes/` records on `store` (#80 S4, B-2): list `nodes/`,
/// [`Net::set_members`] with every record's id at the list's revision, then watch the prefix
/// from that revision and list again on any event, and again after a failed list or a watch
/// that fails or ends (a store outage: the re-list catches what the watch missed, R11). Runs
/// until its task is aborted, or until a list finds the record of a node in `own` removed: it
/// then sets `removed` to that node and ends (S-5, logged `members_own_node_removed`). An empty
/// `nodes/` (an undeclared database) admits nothing new. Logged `members_list_failed` and
/// `members_watch_failed` at warn, once per run of failures.
pub async fn watch_members(
    store: Arc<dyn ConfigStore>,
    net: Arc<Net>,
    own: BTreeSet<NodeId>,
    removed: Arc<OnceLock<NodeId>>,
) {
    let prefix = Bytes::from_static(ControlPrefix::Nodes.encode().as_bytes());
    let mut failing = false;
    // Its own flag: a list that succeeds says nothing of the watch, and a store that serves no
    // watches fails every one (r158 counted a warn every 250 ms per node).
    let mut watch_failing = false;
    loop {
        let revision = match read_nodes_at(&*store).await {
            Ok((records, revision)) => {
                if !records.is_empty() {
                    let live: BTreeSet<NodeId> = records.keys().copied().collect();
                    net.set_members(&live, revision);
                    // A record is never deleted, so an own id that is not live is removed: the
                    // `Db` is done for good (S-5), and so is this watch.
                    if let Some(node) = own_removed(&*store, &own, &live).await {
                        tracing::warn!(node = node.0, revision, "members_own_node_removed");
                        let _ = removed.set(node);
                        return;
                    }
                }
                failing = false;
                revision
            }
            Err(error) => {
                if !failing {
                    tracing::warn!(%error, "members_list_failed");
                    failing = true;
                }
                tokio::time::sleep(WATCH_RETRY).await;
                continue;
            }
        };
        let watched = store
            .watch(WatchRequest {
                prefix: prefix.clone(),
                start_after_revision: revision,
                progress_interval: None,
            })
            .await;
        let mut stream = match watched {
            Ok(stream) => stream,
            Err(error) => {
                if !watch_failing {
                    tracing::warn!(%error, revision, "members_watch_failed");
                    watch_failing = true;
                }
                tokio::time::sleep(WATCH_RETRY).await;
                continue;
            }
        };
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(_))) => {
                    watch_failing = false;
                    break;
                }
                Some(Ok(WatchItem::Progress { .. })) => watch_failing = false,
                Some(Err(error)) => {
                    if !watch_failing {
                        tracing::warn!(%error, revision, "members_watch_failed");
                        watch_failing = true;
                    }
                    tokio::time::sleep(WATCH_RETRY).await;
                    break;
                }
                None => {
                    tokio::time::sleep(WATCH_RETRY).await;
                    break;
                }
            }
        }
    }
}

/// The first of `own` whose `nodes/` record is removed, when one of them is not `live`. A failed
/// read is logged and answers `None`: the next list asks again.
async fn own_removed(
    store: &dyn ConfigStore,
    own: &BTreeSet<NodeId>,
    live: &BTreeSet<NodeId>,
) -> Option<NodeId> {
    if own.is_subset(live) {
        return None;
    }
    match read_removed_nodes(store).await {
        Ok(removed) => own.intersection(&removed).next().copied(),
        Err(error) => {
            tracing::warn!(%error, "members_list_failed");
            None
        }
    }
}

/// What every [`add_node`] refusal ends with.
const NOTHING: &str = "nothing was written";

/// Why [`add_node`] added no node.
#[derive(Debug, thiserror::Error)]
pub enum AddNodeError {
    /// The node is not one this database may add: its text says why.
    #[error("{0}")]
    Refused(String),
    /// The formation record or a `nodes/` record could not be read, or does not decode.
    #[error(transparent)]
    Read(#[from] DeclareError),
    /// The store failed the create, or the read after it: `step` names it.
    #[error("{step}: {error}")]
    Store {
        /// The call that failed.
        step: String,
        /// How it failed.
        #[source]
        error: ConfigError,
    },
}

/// Store-only; skips the live check. Use [`add_member`] unless you are writing a row.
///
/// Add a data node listening at `addr` on machine `machine` (`None`: `addr`'s host as
/// [`parse_addr`] parses it, as a declaration takes it) to the database declared on `store`
/// (#80 S4). Its id is one above the highest id any `nodes/` record holds, so an id is never
/// used twice (ruling C3), and it is created with one create-only put of a layout 1 record. No
/// other record is written, and no running node is restarted: each one learns of the new node
/// from `nodes/`.
///
/// Refused, writing nothing, in this order:
/// - no formation record (the database is not declared), or no `nodes/` record (an S1a store);
/// - a `nodes/` record that does not decode ([`AddNodeError::Read`]);
/// - every id up to [`crate::db::MAX_NODES`] already used;
/// - an address [`parse_addr`] refuses, or one longer than [`MAX_ADDR_LEN`];
/// - a machine label [`machine_refusal`] refuses, after lowercasing;
/// - the new node on a machine a recorded node is on ([`machine_clash`]);
/// - a plaintext control link to a database whose nodes, the new one included, are not all on
///   loopback IPs ([`control_tls_refusal`], #80 S2).
///
/// A create whose reply is lost, or that conflicts, reads the key back: the same bytes are
/// adopted as this add's own; any other record means another add took the id first, and this
/// one is refused, "run add-node again". So two adds at once never get one id.
///
/// Logged `add_node_created{node, addr, machine, revision}`, `add_node_adopted{..}` or
/// `add_node_refused{why}`.
///
/// # Errors
///
/// [`AddNodeError`].
pub async fn add_node(
    store: &Arc<dyn ConfigStore>,
    addr: &str,
    machine: Option<&str>,
) -> Result<NodeRecord, AddNodeError> {
    let refused = |why: String| {
        tracing::warn!(%why, "add_node_refused");
        AddNodeError::Refused(format!("{why}; {NOTHING}"))
    };
    if read_formation(&**store).await?.is_none() {
        return Err(refused(
            "no database is declared on this control store: declare it first (rdb_node \
             --declare)"
                .to_owned(),
        ));
    }
    // A removed node's id counts: ids are never reused (#80 S4).
    let records = read_nodes(&**store).await?;
    let removed = read_removed_nodes(&**store).await?;
    let highest = records.keys().chain(&removed).max().copied();
    let Some(highest) = highest else {
        return Err(refused(super::S1A_STORE.to_owned()));
    };
    let max = crate::db::MAX_NODES;
    if highest.0 >= max {
        return Err(refused(format!(
            "all {max} node ids are used (the highest is {}); ids are never reused (ruling C3)",
            highest.0
        )));
    }
    let node = NodeId(highest.0 + 1);
    let host = parse_addr(addr).map_err(|why| refused(format!("node {}: {why}", node.0)))?;
    if addr.len() > MAX_ADDR_LEN {
        return Err(refused(format!(
            "node {} address {addr:?} is longer than {MAX_ADDR_LEN} bytes",
            node.0
        )));
    }
    let machine = machine
        .map_or_else(|| host.to_string(), str::to_owned)
        .to_ascii_lowercase();
    if let Some(why) = machine_refusal(&machine) {
        return Err(refused(format!("node {}: {why}", node.0)));
    }
    let record = NodeRecord {
        node,
        addr: addr.to_owned(),
        machine,
    };
    let mut all: Vec<NodeRecord> = records.into_values().collect();
    all.push(record.clone());
    if let Some(why) = machine_clash(&all) {
        return Err(refused(why));
    }
    if let Some(why) = control_tls_refusal(
        all.iter().map(|record| (record.node, record.addr.as_str())),
        store.capabilities().transport_security,
    ) {
        return Err(refused(why));
    }
    create(store, &record).await
}

/// The create-only put of `record`, or its adoption when the key already holds the same bytes.
async fn create(
    store: &Arc<dyn ConfigStore>,
    record: &NodeRecord,
) -> Result<NodeRecord, AddNodeError> {
    let key = ControlKey::Node(record.node).encode();
    let body = record.encode();
    let created = store
        .put(PutRequest {
            key: Bytes::from(key.clone()),
            value: body.clone(),
            expected_mod_revision: Some(0),
            dedup: None,
        })
        .await;
    match created {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            tracing::info!(
                node = record.node.0, addr = %record.addr, machine = %record.machine,
                revision = response.revision, "add_node_created"
            );
            return Ok(record.clone());
        }
        Ok(_) | Err(ConfigError::DeadlineExceededUnknownOutcome) => {}
        Err(error) => {
            return Err(AddNodeError::Store {
                step: format!("create {key}"),
                error,
            })
        }
    }
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| AddNodeError::Store {
            step: format!("get {key}"),
            error,
        })?;
    match read.record {
        Some(found) if found.value == body => {
            tracing::info!(
                node = record.node.0, addr = %record.addr, machine = %record.machine,
                revision = found.mod_revision, "add_node_adopted"
            );
            Ok(record.clone())
        }
        Some(_) => {
            let why = format!(
                "id {} was just taken by another add ({key} holds another node); run add-node \
                 again",
                record.node.0
            );
            tracing::warn!(%why, "add_node_refused");
            Err(AddNodeError::Refused(format!(
                "{why}; {NOTHING} by this add"
            )))
        }
        None => Err(AddNodeError::Store {
            step: format!("create {key}"),
            error: ConfigError::Unavailable {
                reason: format!("{key} is absent after a create that was not reported applied"),
            },
        }),
    }
}

/// The fewest distinct live machines a database keeps after a [`remove_node`] (#80 S4, N-2).
pub const MIN_MACHINES: usize = 3;

/// Why [`remove_node`] removed no node.
#[derive(Debug, thiserror::Error)]
pub enum RemoveNodeError {
    /// The node may not be removed now: its text says why.
    #[error("{0}")]
    Refused(String),
    /// The formation record, a `nodes/` or a `partitions/` record could not be read, or does
    /// not decode.
    #[error(transparent)]
    Read(#[from] DeclareError),
    /// The store failed the write, or a read around it: `step` names it.
    #[error("{step}: {error}")]
    Store {
        /// The call that failed.
        step: String,
        /// How it failed.
        #[source]
        error: ConfigError,
    },
}

/// Remove data node `node` from the database declared on `store` (#80 S4): one CAS of its
/// `nodes/{id}` record, at the revision read, to layout 3 (removed), keeping its app address.
/// Its id is never used again ([`add_node`] counts it). Every running node's watcher then drops
/// it from its live members.
///
/// Store-only; skips the live check. Use [`remove_member`] unless you are writing a row: a
/// build without layout 3 fails its whole `nodes/` list on the record this writes, and only
/// [`membership_settled`] proves no member runs one.
///
/// Refused, writing nothing, in this order:
/// - no formation record, or `node` already removed, or `node` not a live member;
/// - fewer than [`MIN_MACHINES`] distinct live machines afterwards (N-2);
/// - a `partitions/` record that names `node` owner or pins a copy on it, naming them and both
///   ways on: "run admin drain N" first while `node` answers, or, if it is down, wait until its
///   copies are dropped (about `Timeouts::unreachable_drop_after`, or failover's promote) and
///   run this again.
///
/// After the CAS it lists `partitions/` again and logs `remove_node_still_pinned{node,
/// partitions}` for any record that pins `node` (a move that committed between the check and
/// the CAS, S-2). Logged `remove_node_written{node, revision}` or `remove_node_refused{why}`.
///
/// # Errors
///
/// [`RemoveNodeError`]: refused, a record unread, or a store call failed.
pub async fn remove_node(
    store: &Arc<dyn ConfigStore>,
    node: NodeId,
) -> Result<NodeRecord, RemoveNodeError> {
    let refused = |why: String| {
        tracing::warn!(%why, "remove_node_refused");
        RemoveNodeError::Refused(format!("{why}; {NOTHING}"))
    };
    if read_formation(&**store).await?.is_none() {
        return Err(refused(
            "no database is declared on this control store".to_owned(),
        ));
    }
    if read_removed_nodes(&**store).await?.contains(&node) {
        return Err(refused(format!(
            "node {} is already removed; ids are never reused",
            node.0
        )));
    }
    let live = read_nodes(&**store).await?;
    if !live.contains_key(&node) {
        return Err(refused(format!(
            "node {} is not a live member: no nodes/ record names it",
            node.0
        )));
    }
    let machines: BTreeSet<&str> = live
        .values()
        .filter(|record| record.node != node)
        .map(|record| record.machine.as_str())
        .collect();
    if machines.len() < MIN_MACHINES {
        return Err(refused(format!(
            "removing node {} leaves {} distinct live machines, and a database keeps at least \
             {MIN_MACHINES}",
            node.0,
            machines.len()
        )));
    }
    let pinned = pinning(&**store, node).await?;
    if !pinned.is_empty() {
        return Err(refused(format!(
            "node {} is still pinned by partitions {pinned:?}: run admin drain {} first, or, if \
             node {} is down, wait until its copies are dropped (about \
             Timeouts::unreachable_drop_after, or failover's promote) and run this again",
            node.0, node.0, node.0
        )));
    }
    let key = ControlKey::Node(node).encode();
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| RemoveNodeError::Store {
            step: format!("get {key}"),
            error,
        })?;
    let Some(found) = read.record else {
        return Err(refused(format!("{key} is gone")));
    };
    let (record, app) = NodeRecord::decode_with_app(&found.value)
        .filter(|(record, _)| record.node == node)
        .ok_or(DeclareError::Undecodable { key: key.clone() })?;
    let written = store
        .put(PutRequest {
            key: Bytes::from(key.clone()),
            value: record.encode_removed(app.as_deref()),
            expected_mod_revision: Some(found.mod_revision),
            dedup: None,
        })
        .await
        .map_err(|error| RemoveNodeError::Store {
            step: format!("write {key} removed"),
            error,
        })?;
    if written.outcome != MutationOutcome::Applied {
        return Err(refused(format!(
            "{key} changed while this remove read it; run remove-node again"
        )));
    }
    tracing::info!(
        node = node.0,
        revision = written.revision,
        "remove_node_written"
    );
    match pinning(&**store, node).await {
        Ok(still) if still.is_empty() => {}
        Ok(still) => {
            tracing::warn!(node = node.0, partitions = ?still, "remove_node_still_pinned");
        }
        Err(error) => tracing::warn!(node = node.0, %error, "remove_node_relist_failed"),
    }
    Ok(record)
}

/// Whether every live member has applied the newest `nodes/` change (#80 S4, S-3): one
/// membership change at a time, and none while a member may run an older build. Each live
/// member is sent a fresh status request through `db` ([`Db::fresh_looks`]), answered within
/// `within`, and must report a [`crate::host::NodeStatus::members_revision`] at or above the
/// newest `mod_revision` among the `nodes/` records, removed ones included.
///
/// A status answered over a link proves the member runs this build's protocol version, since
/// the handshake refuses any other; a build without layout 3 would fail its whole `nodes/` list
/// on a removed record. So a member that does not answer (no link up, or not within `within`)
/// blocks, naming it, as one that reports an older revision or none does.
///
/// A member named in `unreachable_ok` is not asked: the operator says it is dead. It must stay
/// down until it is upgraded: started again on a build without layout 3 after a removal, its
/// open fails on the removed record (fail-closed). **Upgrade it before you restart it.**
///
/// Logged `membership_settled{newest, members}` or `membership_unsettled{why}`.
///
/// # Errors
///
/// The refusal, as text naming the first member that blocks; or a `nodes/` read that failed.
pub fn membership_settled(
    db: &Db,
    rt: &tokio::runtime::Handle,
    store: &dyn ConfigStore,
    unreachable_ok: &[NodeId],
    within: Duration,
) -> Result<(), String> {
    let unsettled = |why: String| {
        tracing::warn!(%why, "membership_unsettled");
        why
    };
    let (live, newest) = rt
        .block_on(async {
            let live = read_nodes(store).await?;
            let newest = newest_nodes_revision(store).await?;
            Ok::<_, DeclareError>((live, newest))
        })
        .map_err(|error| unsettled(format!("read nodes/: {error}")))?;
    let asked: Vec<NodeId> = live
        .keys()
        .copied()
        .filter(|member| !unreachable_ok.contains(member))
        .collect();
    let looks = db.fresh_looks(&asked, within);
    for member in &asked {
        let why = match looks.get(member) {
            Some(Ok(status)) => match status.members_revision {
                Some(revision) if revision >= newest => continue,
                Some(revision) => format!(
                    "live member {} has applied nodes/ revision {revision}, below the newest \
                     {newest}: wait for it to apply the last change, then try again",
                    member.0
                ),
                None => format!(
                    "live member {} reports no members revision: its watcher has not listed \
                     nodes/ yet; wait, then try again",
                    member.0
                ),
            },
            Some(Err(why)) => format!(
                "live member {} has sent no status since its boot ({why}; asked within \
                 {within:?}): every live member \
                 must answer at this build's protocol version, or be named in --unreachable-ok \
                 if it is dead (then upgrade it before you restart it)",
                member.0
            ),
            None => format!("live member {} was not asked for its status", member.0),
        };
        return Err(unsettled(why));
    }
    let members: Vec<u32> = asked.iter().map(|member| member.0).collect();
    tracing::info!(newest, members = ?members, "membership_settled");
    Ok(())
}

/// Remove data node `node` the way an operator or an embedding app should (#80 S4):
/// [`membership_settled`] over the live members (`unreachable_ok` and `within` as it takes
/// them), then [`remove_node`]. A check that refuses is [`RemoveNodeError::Refused`] with its
/// text, and nothing is written. Blocking: run it off any runtime thread.
///
/// # Errors
///
/// As [`membership_settled`] (as `Refused`) and [`remove_node`].
pub fn remove_member(
    db: &Db,
    rt: &tokio::runtime::Handle,
    store: &Arc<dyn ConfigStore>,
    node: NodeId,
    unreachable_ok: &[NodeId],
    within: Duration,
) -> Result<NodeRecord, RemoveNodeError> {
    membership_settled(db, rt, &**store, unreachable_ok, within)
        .map_err(|why| RemoveNodeError::Refused(format!("{why}; {NOTHING}")))?;
    rt.block_on(remove_node(store, node))
}

/// Add a data node the way an operator or an embedding app should (#80 S4):
/// [`membership_settled`] (one membership change at a time), then [`add_node`]. A check that
/// refuses is [`AddNodeError::Refused`] with its text, and nothing is written. Blocking: run it
/// off any runtime thread.
///
/// # Errors
///
/// As [`membership_settled`] (as `Refused`) and [`add_node`].
pub fn add_member(
    db: &Db,
    rt: &tokio::runtime::Handle,
    store: &Arc<dyn ConfigStore>,
    addr: &str,
    machine: Option<&str>,
    unreachable_ok: &[NodeId],
    within: Duration,
) -> Result<NodeRecord, AddNodeError> {
    membership_settled(db, rt, &**store, unreachable_ok, within)
        .map_err(|why| AddNodeError::Refused(format!("{why}; {NOTHING}")))?;
    rt.block_on(add_node(store, addr, machine))
}

/// The newest `mod_revision` among the `nodes/` records, removed ones included; 0 for none.
async fn newest_nodes_revision(store: &dyn ConfigStore) -> Result<u64, DeclareError> {
    let listed = list_whole(store, ControlPrefix::Nodes).await?;
    Ok(listed
        .records
        .iter()
        .map(|found| found.mod_revision)
        .max()
        .unwrap_or(0))
}

/// The partitions whose `partitions/` record names `node` owner or pins a copy on it.
async fn pinning(store: &dyn ConfigStore, node: NodeId) -> Result<Vec<u32>, DeclareError> {
    let listed = list_whole(store, ControlPrefix::Partitions).await?;
    let mut pinned = Vec::new();
    for found in &listed.records {
        let record =
            PartitionRecord::decode(&found.value).ok_or_else(|| DeclareError::Undecodable {
                key: String::from_utf8_lossy(&found.key).into_owned(),
            })?;
        let pins = RecordMember::decode_all(&found.value)
            .is_some_and(|members| members.iter().any(|member| member.node == node));
        if record.owner == node || pins {
            pinned.push(record.partition.0);
        }
    }
    Ok(pinned)
}

#[cfg(test)]
mod tests;
