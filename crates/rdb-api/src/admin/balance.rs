//! Copy placement over the runtime membership (#80 S4 T12, design `arch-80-s34` rev 3): the
//! inventory the records describe, and `rebalance`, which runs `placement::plan`'s copy moves
//! through [`Db::move_copy_in`] one at a time. Copies only (Q4 a): an owner moves by failover or
//! drain, so a primary step is reported as the residual `no_live_handoff`.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::{ConfigStore, ListRequest};
use rdb_core::authority::partition::{PartitionRecord, RecordMember};
use rdb_core::contracts::control::ControlPrefix;
use rdb_core::contracts::errors::RetryRule;
use rdb_core::contracts::ids::{NodeId, PartitionId, ReplicaRole};
use rdb_core::placement::{self, Inventory, Node, NodeState, Partition, Step};

use super::read_nodes;
use crate::Db;

/// How long [`rebalance`] follows one move: the host's own catch-up bound
/// ([`crate::host::MOVE_CATCH_UP_MILLIS`]) and 30 s for the commits around it.
const MOVE_WITHIN: Duration = Duration::from_millis(crate::host::MOVE_CATCH_UP_MILLIS + 30_000);

/// How often [`rebalance`] asks a running move again.
const MOVE_POLL: Duration = Duration::from_millis(200);

/// The placement inventory `store`'s records describe (#80 S4): each live `nodes/` record is a
/// node on its machine (the machine is its failure domain too), primary-eligible, `draining`
/// when named in `draining` and `ok` otherwise. Each `partitions/` record is a partition whose
/// copies are its pinned primary and regular secondaries (a shadow is a move in flight, not a
/// copy) and whose primary is its owner.
///
/// # Errors
///
/// The text of a store call that failed or truncated, of a record this build does not read, or
/// of an inventory `placement` refuses (a record pinning a node with no live record, say).
pub async fn inventory(
    store: &dyn ConfigStore,
    draining: &BTreeSet<NodeId>,
) -> Result<Inventory, String> {
    let live = read_nodes(store)
        .await
        .map_err(|error| format!("read nodes/: {error}"))?;
    let nodes = live
        .values()
        .map(|record| Node {
            id: record.node.0,
            machine: record.machine.clone(),
            domain: record.machine.clone(),
            state: if draining.contains(&record.node) {
                NodeState::Draining
            } else {
                NodeState::Ok
            },
            primary_eligible: true,
        })
        .collect();
    let prefix = ControlPrefix::Partitions.encode();
    let listed = store
        .list(ListRequest {
            prefix: Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .map_err(|error| format!("list {prefix}: {error}"))?;
    if listed.truncated {
        return Err(format!(
            "list {prefix}: truncated at revision {}",
            listed.read_revision
        ));
    }
    let mut partitions = Vec::with_capacity(listed.records.len());
    for found in &listed.records {
        let key = String::from_utf8_lossy(&found.key).into_owned();
        let record = PartitionRecord::decode(&found.value)
            .ok_or_else(|| format!("{key}: a record this build does not read"))?;
        let members = RecordMember::decode_all(&found.value)
            .ok_or_else(|| format!("{key}: no members this build reads"))?;
        partitions.push(Partition {
            id: record.partition.0,
            copies: members
                .iter()
                .filter(|member| member.role != ReplicaRole::Shadow)
                .map(|member| member.node.0)
                .collect(),
            primary: record.owner.0,
        });
    }
    let inventory = Inventory { nodes, partitions };
    placement::plan(&inventory, 0).map_err(|error| format!("inventory: {error}"))?;
    Ok(inventory)
}

/// What [`rebalance`] is asked to do.
#[derive(Debug, Clone, Default)]
pub struct RebalanceOpts {
    /// The most copy moves to run. `0` runs none and reports the plan's residuals.
    pub max_moves: usize,
    /// Nodes whose copies must leave and which take none (`admin drain`).
    pub draining: BTreeSet<NodeId>,
}

/// What [`rebalance`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceReport {
    /// The copy moves that committed, in order.
    pub moved: Vec<Step>,
    /// Claims left unmet, in plain words: `placement`'s residuals, and each primary step as
    /// `no_live_handoff: ...` (Q4 a).
    pub residuals: Vec<String>,
    /// Copy-count skew of the inventory at the end.
    pub copy_skew: usize,
    /// Primary-count skew of the inventory at the end.
    pub primary_skew: usize,
}

/// Move copies until `placement::plan` has no copy step left, or `opts.max_moves` moves ran
/// (#80 S4 T12). Each round reads the inventory again ([`inventory`]), so a run stopped part way
/// resumes where the records are: it plans afresh and runs the plan's first copy step through
/// `db`'s [`Db::move_copy_in`], following it every [`MOVE_POLL`] until it answers `done`. A
/// primary step is never run (Q4 a); the report names it `no_live_handoff`. Blocking: run it
/// off any runtime thread. Logged `rebalance_move` per move and `rebalance_done` at the end.
///
/// # Errors
///
/// The text of a failed inventory read, of a move refused for good (its `RetryRule` is
/// `Definitive`) or given up, or of a move not done within [`MOVE_WITHIN`], naming the step.
pub fn rebalance(
    db: &Db,
    rt: &tokio::runtime::Handle,
    store: &dyn ConfigStore,
    opts: &RebalanceOpts,
) -> Result<RebalanceReport, String> {
    let mut moved: Vec<Step> = Vec::new();
    loop {
        let inventory = rt.block_on(inventory(store, &opts.draining))?;
        let budget = opts.max_moves.saturating_sub(moved.len());
        let plan = placement::plan(&inventory, budget.max(1))
            .map_err(|error| format!("inventory: {error}"))?;
        let next = runnable(&inventory, &plan.steps);
        match next.filter(|_| budget > 0) {
            Some(step) => {
                run_move(db, step)?;
                moved.push(step);
            }
            None => {
                let plan = placement::plan(&inventory, usize::MAX)
                    .map_err(|error| format!("inventory: {error}"))?;
                let mut residuals: Vec<String> = plan
                    .residuals
                    .iter()
                    .map(placement::Residual::describe)
                    .collect();
                residuals.extend(plan.steps.iter().map(|step| {
                    if step.primary {
                        format!(
                            "no_live_handoff: partition {}'s primary from node {} to node {} (an \
                             owner moves by failover or drain, not by rebalance)",
                            step.partition, step.from, step.to
                        )
                    } else {
                        format!(
                            "copy_not_run: partition {}'s copy from node {} to node {} (the move \
                             budget ran out, or it waits on a primary handoff above)",
                            step.partition, step.from, step.to
                        )
                    }
                }));
                let report = RebalanceReport {
                    moved,
                    residuals,
                    copy_skew: placement::copy_skew(&inventory),
                    primary_skew: placement::primary_skew(&inventory),
                };
                tracing::info!(
                    moved = report.moved.len(),
                    copy_skew = report.copy_skew,
                    primary_skew = report.primary_skew,
                    residuals = ?report.residuals,
                    "rebalance_done"
                );
                return Ok(report);
            }
        }
    }
}

/// The first of `steps` that [`rebalance`] may run now: a copy move whose source is not its
/// partition's owner in `inventory`, of a partition no earlier primary step hands over. A plan
/// orders a copy move after the handoff that frees its source, and no handoff runs here.
fn runnable(inventory: &Inventory, steps: &[Step]) -> Option<Step> {
    let owner = |partition: u32| {
        inventory
            .partitions
            .iter()
            .find(|p| p.id == partition)
            .map(|p| p.primary)
    };
    let mut handed: BTreeSet<u32> = BTreeSet::new();
    for step in steps {
        if step.primary {
            handed.insert(step.partition);
        } else if !handed.contains(&step.partition) && owner(step.partition) != Some(step.from) {
            return Some(*step);
        }
    }
    None
}

/// Whether a move answer says the host gave the move up. Covers host.rs's three
/// "the move of node A to node B was given up: ..." texts (`move_blocked`,
/// `shadow_give_up_duty`, the catch-up bound), and no other "given up" text.
fn move_given_up(detail: &str) -> bool {
    detail.contains("the move of node ") && detail.contains(" was given up: ")
}

/// Run `step`'s copy move through `db` until it answers `done`.
fn run_move(db: &Db, step: Step) -> Result<(), String> {
    let partition = PartitionId(step.partition);
    let (from, to) = (NodeId(step.from), NodeId(step.to));
    let started = Instant::now();
    let deadline = started + MOVE_WITHIN;
    let mut last: String;
    loop {
        match db.move_copy_in(partition, from, to) {
            Ok(answer) if answer.done => {
                tracing::info!(
                    partition = step.partition,
                    from = step.from,
                    to = step.to,
                    why = %step.why.describe(),
                    took_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    line = %answer.line,
                    "rebalance_move"
                );
                return Ok(());
            }
            Ok(answer) => last = answer.line,
            Err(error) if error.retry == RetryRule::Definitive => {
                return Err(format!(
                    "rebalance: the move of partition {}'s copy from node {} to node {} was \
                     refused: {}",
                    step.partition, step.from, step.to, error.detail
                ));
            }
            // The host says why it gave a move up once; asking again would start it afresh.
            Err(error) if move_given_up(&error.detail) => {
                return Err(format!(
                    "rebalance: the move of partition {}'s copy from node {} to node {}: {}",
                    step.partition, step.from, step.to, error.detail
                ));
            }
            Err(error) => last = error.detail,
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "rebalance: the move of partition {}'s copy from node {} to node {} was not done \
                 within {MOVE_WITHIN:?}; last: {last}",
                step.partition, step.from, step.to
            ));
        }
        std::thread::sleep(MOVE_POLL);
    }
}
