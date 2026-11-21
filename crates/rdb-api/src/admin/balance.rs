//! Copy placement over the runtime membership (#80 S4 T12, design `arch-80-s34` rev 3): the
//! inventory the records describe, and `rebalance`, which refills a partition below RF through
//! [`Db::refill_in`] and runs `placement::plan`'s copy moves through [`Db::move_copy_in`], one
//! at a time. Copies only (Q4 a): an owner moves by failover or
//! drain, so a primary step is reported as the residual `no_live_handoff`.

use std::collections::{BTreeMap, BTreeSet};
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
    /// The most copy moves and refills to run, together. `0` runs none and reports the plan's
    /// residuals.
    pub max_moves: usize,
    /// Nodes whose copies must leave and which take none (`admin drain`).
    pub draining: BTreeSet<NodeId>,
}

/// What [`rebalance`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceReport {
    /// The copy moves that committed, in order.
    pub moved: Vec<Step>,
    /// The refills that committed, in order, as `(partition, to)`: a copy added to a partition
    /// with fewer than [`crate::admin::RF`] regular copies.
    pub refilled: Vec<(u32, u32)>,
    /// Claims left unmet, in plain words: `placement`'s residuals, and each primary step as
    /// `no_live_handoff: ...` (Q4 a).
    pub residuals: Vec<String>,
    /// Copy-count skew of the inventory at the end.
    pub copy_skew: usize,
    /// Primary-count skew of the inventory at the end.
    pub primary_skew: usize,
}

/// Refill and move copies until every partition has RF regular copies and `placement::plan` has
/// no copy step left, or `opts.max_moves` refills and moves ran (#80 S4 T12). Each round reads
/// the inventory again ([`inventory`]), so a run stopped part way resumes where the records are.
/// A round first refills the lowest partition below RF onto [`refill_target`] through `db`'s
/// [`Db::refill_in`] (A-R456 narrowed by the lead, 2026-11-21); with none below RF, it plans
/// afresh and runs the plan's first copy step through [`Db::move_copy_in`]. Either is followed
/// every [`MOVE_POLL`] until it answers `done`. A primary step is never run (Q4 a); the report
/// names it `no_live_handoff`, and a partition below RF with no node to take a copy
/// `no_refill_target`. Blocking: run it off any runtime thread. Logged `rebalance_refill` per
/// refill, `rebalance_move` per move and `rebalance_done` at the end.
///
/// # Errors
///
/// The text of a failed inventory read, of a refill or move refused for good (its `RetryRule`
/// is `Definitive`) or given up, or of one not done within [`MOVE_WITHIN`], naming the step.
pub fn rebalance(
    db: &Db,
    rt: &tokio::runtime::Handle,
    store: &dyn ConfigStore,
    opts: &RebalanceOpts,
) -> Result<RebalanceReport, String> {
    let mut moved: Vec<Step> = Vec::new();
    let mut refilled: Vec<(u32, u32)> = Vec::new();
    loop {
        let inventory = rt.block_on(inventory(store, &opts.draining))?;
        let budget = opts.max_moves.saturating_sub(moved.len() + refilled.len());
        let refill = next_refill(&inventory);
        if let Some((partition, Some(to))) = refill.filter(|_| budget > 0) {
            run_refill(db, partition, to)?;
            refilled.push((partition, to));
            continue;
        }
        let plan = placement::plan(&inventory, budget.max(1))
            .map_err(|error| format!("inventory: {error}"))?;
        let next = runnable(&inventory, &plan.steps);
        match next.filter(|_| budget > 0 && refill.is_none()) {
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
                residuals.extend(
                    inventory
                        .partitions
                        .iter()
                        .filter(|p| p.copies.len() < crate::admin::RF)
                        .map(|p| {
                            let why = if refill_target(&inventory, p.id).is_some() {
                                "the budget ran out"
                            } else {
                                "no ok node without a copy sits on an unused machine"
                            };
                            format!(
                            "no_refill_target: partition {} has {} of {} regular copies (copies: \
                             {:?}): {why}",
                            p.id,
                            p.copies.len(),
                            crate::admin::RF,
                            p.copies
                        )
                        }),
                );
                let report = RebalanceReport {
                    moved,
                    refilled,
                    residuals,
                    copy_skew: placement::copy_skew(&inventory),
                    primary_skew: placement::primary_skew(&inventory),
                };
                tracing::info!(
                    moved = report.moved.len(),
                    refilled = ?report.refilled,
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

/// Whether a move or refill answer says the host gave it up. Covers host.rs's three "the move
/// of node A to node B was given up: ..." texts (`move_blocked`, `shadow_give_up_duty`, the
/// catch-up bound) and its two "the refill of node T into partition P was given up: ..." texts
/// (`shadow_give_up_duty`, the catch-up bound), and no other "given up" text.
fn move_given_up(detail: &str) -> bool {
    (detail.contains("the move of node ") || detail.contains("the refill of node "))
        && detail.contains(" was given up: ")
}

/// Where a partition below RF copies takes its next one: a node that is not draining, holds no
/// copy of it, and sits on a machine none of its copies uses; of those, the one holding fewest
/// copies, ties to the lowest id. Pure; `None` when no node qualifies.
pub(crate) fn refill_target(inventory: &Inventory, partition: u32) -> Option<u32> {
    let p = inventory.partitions.iter().find(|p| p.id == partition)?;
    let machine = |id: u32| {
        inventory
            .nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| n.machine.as_str())
    };
    let used: BTreeSet<&str> = p.copies.iter().filter_map(|id| machine(*id)).collect();
    let mut held: BTreeMap<u32, usize> = BTreeMap::new();
    for q in &inventory.partitions {
        for id in &q.copies {
            *held.entry(*id).or_default() += 1;
        }
    }
    inventory
        .nodes
        .iter()
        .filter(|n| n.state == NodeState::Ok)
        .filter(|n| !p.copies.contains(&n.id) && !used.contains(n.machine.as_str()))
        .min_by_key(|n| (held.get(&n.id).copied().unwrap_or(0), n.id))
        .map(|n| n.id)
}

/// The lowest partition in `inventory` with fewer than RF regular copies, with its
/// [`refill_target`]. `None` when every partition has RF.
fn next_refill(inventory: &Inventory) -> Option<(u32, Option<u32>)> {
    inventory
        .partitions
        .iter()
        .find(|p| p.copies.len() < crate::admin::RF)
        .map(|p| (p.id, refill_target(inventory, p.id)))
}

/// Refill `partition` onto `to` through `db` until it answers `done`. Logged `rebalance_refill`.
fn run_refill(db: &Db, partition: u32, to: u32) -> Result<(), String> {
    let started = Instant::now();
    let deadline = started + MOVE_WITHIN;
    let mut last: String;
    loop {
        match db.refill_in(PartitionId(partition), NodeId(to)) {
            Ok(answer) if answer.done => {
                tracing::info!(
                    partition,
                    to,
                    took_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    line = %answer.line,
                    "rebalance_refill"
                );
                return Ok(());
            }
            Ok(answer) => last = answer.line,
            // Refused for good, or given up: the host says why once, and the next ask would
            // start afresh.
            Err(error) if error.retry == RetryRule::Definitive || move_given_up(&error.detail) => {
                return Err(format!(
                    "rebalance: the refill of partition {partition} onto node {to}: {}",
                    error.detail
                ));
            }
            Err(error) => last = error.detail,
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "rebalance: the refill of partition {partition} onto node {to} was not done \
                 within {MOVE_WITHIN:?}; last: {last}"
            ));
        }
        std::thread::sleep(MOVE_POLL);
    }
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
