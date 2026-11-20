//! Runtime data membership (#80 S4, ADR-rdb-0025 Proposed): adding a data node to a declared
//! database by creating its `nodes/{id}` record. The formation record, the reservation,
//! `routes/0` and generation-0 placement never change; the records are the membership.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ConfigError, ConfigStore, GetRequest, MutationOutcome, PutRequest, WatchItem, WatchRequest,
};
use rdb_core::authority::node::{machine_refusal, parse_addr, NodeRecord, MAX_ADDR_LEN};
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::control::ControlPrefix;
use rdb_core::contracts::ids::NodeId;
use tokio_stream::StreamExt;

use super::{
    control_tls_refusal, machine_clash, read_formation, read_nodes, read_nodes_at, DeclareError,
};
use crate::net::Net;

/// How long [`watch_members`] waits before it lists again after a failed list or watch.
const WATCH_RETRY: Duration = Duration::from_millis(250);

/// Keep `net`'s members the live `nodes/` records on `store` (#80 S4, B-2): list `nodes/`,
/// [`Net::set_members`] with every record's id at the list's revision, then watch the prefix
/// from that revision and list again on any event, and again after a failed list or a watch
/// that fails or ends (a store outage: the re-list catches what the watch missed, R11). Runs
/// until its task is aborted. An empty `nodes/` (an undeclared database) admits nothing new.
/// Logged `members_list_failed` and `members_watch_failed` at warn, once per run of failures.
pub async fn watch_members(store: Arc<dyn ConfigStore>, net: Arc<Net>) {
    let prefix = Bytes::from_static(ControlPrefix::Nodes.encode().as_bytes());
    let mut failing = false;
    loop {
        let revision = match read_nodes_at(&*store).await {
            Ok((records, revision)) => {
                if !records.is_empty() {
                    let live: BTreeSet<NodeId> = records.keys().copied().collect();
                    net.set_members(&live, revision);
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
                tracing::warn!(%error, revision, "members_watch_failed");
                tokio::time::sleep(WATCH_RETRY).await;
                continue;
            }
        };
        loop {
            match stream.next().await {
                Some(Ok(WatchItem::Event(_))) => break,
                Some(Ok(WatchItem::Progress { .. })) => {}
                Some(Err(error)) => {
                    tracing::warn!(%error, revision, "members_watch_failed");
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
    let records = read_nodes(&**store).await?;
    let Some(highest) = records.keys().next_back().copied() else {
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

#[cfg(test)]
mod tests;
