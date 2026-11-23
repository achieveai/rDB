//! Draining a node (#80 S4 T12, design `arch-80-s34` rev 3): freeze its grant so it serves
//! nothing again, promote what it still owns, and rebalance the copies it held.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use config_core::{ConfigStore, GetRequest, MutationOutcome, PutRequest};
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::{PartitionRecord, RecordMember};
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::ids::{NodeId, PartitionId};
use rdb_core::placement::Inventory;

use super::balance::{inventory, rebalance, RebalanceOpts, RebalanceReport};
use crate::Db;

/// How long [`freeze_grant`] retries a write that lost to the node renewing its own grant:
/// ten default read timeouts (`Timeouts::read`, 1 s).
const FREEZE_WITHIN: Duration = Duration::from_secs(10);

/// How long [`freeze_grant`] waits between a lost write and the next read.
const FREEZE_RETRY: Duration = Duration::from_millis(50);

/// Freeze `grants/{node}`: read it and write it back `frozen` at the revision read. A frozen
/// grant never comes back, so the node serves none of its partitions again and a promote of
/// each passes at once. A record already frozen is answered as it is, with no write. A write
/// that loses to the node renewing its grant is read and tried again, for up to 10 s. Returns
/// the frozen record and the revision of the write, or of the read that found it frozen.
/// Logged `grant_frozen{node, grant, revision, already}`.
///
/// # Errors
///
/// The text of a store call that failed, of an absent or undecodable record, or of writes that
/// kept losing for 10 s. Nothing was frozen.
pub async fn freeze_grant(
    store: &dyn ConfigStore,
    node: NodeId,
) -> Result<(GrantRecord, u64), String> {
    let key = ControlKey::Grant(node).encode();
    let deadline = tokio::time::Instant::now() + FREEZE_WITHIN;
    loop {
        let read = store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
            .map_err(|error| format!("get {key}: {error}; nothing was frozen"))?;
        let Some(found) = read.record else {
            return Err(format!("{key} is absent; nothing was frozen"));
        };
        let held = GrantRecord::decode(&found.value).ok_or_else(|| {
            format!(
                "{key} at revision {} is not a grant record; nothing was frozen",
                found.mod_revision
            )
        })?;
        if held.frozen {
            tracing::info!(
                node = node.0,
                grant = held.grant.0,
                revision = found.mod_revision,
                already = true,
                "grant_frozen"
            );
            return Ok((held, found.mod_revision));
        }
        let frozen = GrantRecord {
            frozen: true,
            ..held
        };
        let written = store
            .put(PutRequest {
                key: Bytes::from(key.clone()),
                value: frozen.encode(),
                expected_mod_revision: Some(found.mod_revision),
                dedup: None,
            })
            .await
            .map_err(|error| {
                // PR #244 review: only a refusal before the log proves nothing was written; any
                // other error (a lost reply) may have frozen it.
                if error.is_safe_to_resubmit() {
                    format!("put {key}: {error}; nothing was frozen")
                } else {
                    format!(
                        "put {key}: {error}; the grant may be frozen: read {key}, then freeze \
                         again"
                    )
                }
            })?;
        if written.outcome == MutationOutcome::Applied {
            tracing::info!(
                node = node.0,
                grant = frozen.grant.0,
                revision = written.revision,
                already = false,
                "grant_frozen"
            );
            return Ok((frozen, written.revision));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "{key} changed under every write for {FREEZE_WITHIN:?} (last read at revision \
                 {}); nothing was frozen: freeze again",
                found.mod_revision
            ));
        }
        tracing::debug!(
            node = node.0,
            revision = found.mod_revision,
            "grant_freeze_lost"
        );
        tokio::time::sleep(FREEZE_RETRY).await;
    }
}

/// What [`drain`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainReport {
    /// The last [`rebalance`], after the promotes: its moves, residuals and skews.
    pub rebalance: RebalanceReport,
    /// The partitions the drain itself promoted off the node.
    pub promoted_by_drain: Vec<u32>,
    /// The partitions the node owned at the freeze that another promote (the failover planner's)
    /// moved off it first.
    pub promoted_elsewhere: Vec<u32>,
}

/// Drain `node` (#80 S4 T12, S-1): afterwards no `partitions/*` record pins it, and every
/// partition has [`crate::admin::RF`] regular copies, so `admin::remove_member` can take it out.
/// Blocking: run it off any runtime thread. Four steps:
///
/// 1. [`rebalance`] with `node` draining, which moves every copy it holds but does not own.
/// 2. [`drain_targets`] on a fresh inventory, **before anything is frozen**: it refuses unless
///    the node holds no copy it does not own and each partition it owns has a regular copy
///    elsewhere whose node answers a look. A refusal returns here, having written nothing.
/// 3. [`freeze_grant`]; then a wait of the failover poll (`Failover::default().poll`) plus
///    `Timeouts::read`, so a running failover planner acts first; then, for each partition the
///    node still owns, `promote_partition(p, target, Some(node))`. A partition another promote
///    already moved off the node, before the drain's or by winning the CAS against it, counts as
///    done: the re-read of the record is what keeps a drain beside a planner correct, and the
///    wait only lets the planner go first.
/// 4. [`rebalance`] again, which refills each partition the promotes left below RF through
///    [`Db::refill_in`] and evens the skew.
///
/// Logged `drain_promoted`, `drain_promote_elsewhere`, `drain_promote_lost` and `drain_done`.
///
/// # Errors
///
/// The text of step 2's refusal (nothing was changed), of a failed store call, rebalance,
/// freeze or promote, naming the step.
pub fn drain(
    db: &Db,
    rt: &tokio::runtime::Handle,
    store: &dyn ConfigStore,
    node: NodeId,
) -> Result<DrainReport, String> {
    let draining: BTreeSet<NodeId> = [node].into();
    let opts = RebalanceOpts {
        max_moves: usize::MAX,
        draining: draining.clone(),
    };
    // Step 1.
    rebalance(db, rt, store, &opts)?;
    // Step 2: every check runs, and refuses, before the freeze below writes anything.
    let before = rt.block_on(inventory(store, &draining))?;
    let read = db.timeouts().read;
    let holders: Vec<NodeId> = before
        .partitions
        .iter()
        .filter(|p| p.primary == node.0)
        .flat_map(|p| p.copies.iter().copied())
        .filter(|holder| *holder != node.0)
        .collect::<BTreeSet<u32>>()
        .into_iter()
        .map(NodeId)
        .collect();
    let answering: BTreeSet<u32> = db
        .fresh_looks(&holders, read)
        .into_iter()
        .filter(|(_, looked)| matches!(looked, Ok(status) if status.fault.is_none()))
        .map(|(holder, _)| holder.0)
        .collect();
    let targets = drain_targets(&before, node.0, &answering).map_err(|why| {
        tracing::warn!(node = node.0, %why, "drain_refused");
        format!(
            "drain of node {} refused before the freeze: {why}; nothing was changed",
            node.0
        )
    })?;
    // Step 3.
    rt.block_on(freeze_grant(store, node))?;
    std::thread::sleep(crate::planner::Failover::default().poll + read);
    let mut promoted_by_drain = Vec::new();
    let mut promoted_elsewhere = Vec::new();
    for (partition, target) in &targets {
        let p = PartitionId(*partition);
        let (record, _) = rt.block_on(record_of(store, p))?;
        if record.owner != node {
            tracing::info!(partition, owner = record.owner.0, "drain_promote_elsewhere");
            promoted_elsewhere.push(*partition);
            continue;
        }
        match db.promote_partition(p, NodeId(*target), Some(node)) {
            Ok(_) => {
                tracing::info!(partition, to = target, "drain_promoted");
                promoted_by_drain.push(*partition);
            }
            Err(error) => {
                let (record, _) = rt.block_on(record_of(store, p))?;
                if record.owner == node {
                    return Err(format!(
                        "drain of node {}: the promote of partition {partition} to node {target} \
                         failed and node {} still owns it: {error}",
                        node.0, node.0
                    ));
                }
                tracing::info!(
                    partition,
                    owner = record.owner.0,
                    %error,
                    "drain_promote_lost"
                );
                promoted_elsewhere.push(*partition);
            }
        }
    }
    // Step 4.
    let report = DrainReport {
        rebalance: rebalance(db, rt, store, &opts)?,
        promoted_by_drain,
        promoted_elsewhere,
    };
    tracing::info!(
        node = node.0,
        promoted_by_drain = ?report.promoted_by_drain,
        promoted_elsewhere = ?report.promoted_elsewhere,
        copy_skew = report.rebalance.copy_skew,
        "drain_done"
    );
    Ok(report)
}

/// S-1's checks before a drain's freeze, on `inventory` as step 1 left it, for `node` with
/// `answering` the nodes that answered a look: the promote target of each partition `node`
/// owns. Each target is a copy holder of that partition other than `node` that answered; among
/// several, the one owning fewest partitions so far (counting the targets already chosen), ties
/// to the lowest id. Pure.
///
/// # Errors
///
/// Why the drain must not freeze: `node` still holds a copy it does not own, or a partition it
/// owns has no other copy holder that answered.
pub(crate) fn drain_targets(
    inventory: &Inventory,
    node: u32,
    answering: &BTreeSet<u32>,
) -> Result<BTreeMap<u32, u32>, String> {
    let foreign: Vec<u32> = inventory
        .partitions
        .iter()
        .filter(|p| p.primary != node && p.copies.contains(&node))
        .map(|p| p.id)
        .collect();
    if !foreign.is_empty() {
        return Err(format!(
            "node {node} still holds copies of partitions {foreign:?} that it does not own: run \
             admin rebalance with it draining first"
        ));
    }
    let mut owns: BTreeMap<u32, usize> = BTreeMap::new();
    for p in &inventory.partitions {
        *owns.entry(p.primary).or_default() += 1;
    }
    let mut targets = BTreeMap::new();
    for p in inventory.partitions.iter().filter(|p| p.primary == node) {
        let target = p
            .copies
            .iter()
            .copied()
            .filter(|holder| *holder != node && answering.contains(holder))
            .min_by_key(|holder| (owns.get(holder).copied().unwrap_or(0), *holder))
            .ok_or_else(|| {
                format!(
                    "partition {} has no copy outside node {node} whose node answers (copies: \
                     {:?}, answering: {answering:?})",
                    p.id, p.copies
                )
            })?;
        *owns.entry(target).or_default() += 1;
        targets.insert(p.id, target);
    }
    Ok(targets)
}

/// `partitions/{partition}`'s record and members.
pub(crate) async fn record_of(
    store: &dyn ConfigStore,
    partition: PartitionId,
) -> Result<(PartitionRecord, Vec<RecordMember>), String> {
    let key = ControlKey::Partition(partition).encode();
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| format!("get {key}: {error}"))?;
    let found = read.record.ok_or_else(|| format!("{key} is absent"))?;
    let record = PartitionRecord::decode(&found.value)
        .ok_or_else(|| format!("{key}: a record this build does not read"))?;
    let members = RecordMember::decode_all(&found.value)
        .ok_or_else(|| format!("{key}: no members this build reads"))?;
    Ok((record, members))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdb_core::placement::{Node, NodeState, Partition};

    /// PR #244 review (drain.rs): a freeze write the store applied but answered with an unknown
    /// outcome says the grant may be frozen, never that nothing was, and the store does hold it
    /// frozen. Unit.
    /// Cut: the old "nothing was frozen" for every put error, red.
    #[config_log::retcd_test]
    fn pr244_an_applied_freeze_with_an_unknown_outcome_says_the_grant_may_be_frozen() {
        use rdb_core::contracts::ids::{AuthorityGeneration, BootId, GrantId};
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime");
        let mem: std::sync::Arc<dyn ConfigStore> =
            std::sync::Arc::new(config_testkit::MemStore::new());
        let key = ControlKey::Grant(NodeId(2)).encode();
        let live = GrantRecord {
            grant: GrantId(1),
            node: NodeId(2),
            boot: BootId(7),
            authority_generation: AuthorityGeneration::default(),
            expiry_utc_ms: 1,
            frozen: false,
        };
        rt.block_on(mem.put(PutRequest {
            key: Bytes::from(key.clone()),
            value: live.encode(),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("the grant is written");
        let store = crate::test_store::PutHold::new(
            std::sync::Arc::clone(&mem),
            "grants/",
            crate::test_store::HoldAt::AppliedUnknown,
        );
        let why = rt
            .block_on(freeze_grant(&*store, NodeId(2)))
            .expect_err("the reply was lost");
        assert!(
            why.contains("the grant may be frozen") && !why.contains("nothing was frozen"),
            "{why}"
        );
        let held = rt
            .block_on(mem.get(GetRequest {
                key: Bytes::from(key),
            }))
            .expect("the store answers")
            .record
            .and_then(|found| GrantRecord::decode(&found.value))
            .expect("a grant");
        assert!(held.frozen, "the write applied: {held:?}");
    }

    /// Nodes 1 to 4, each on its own machine, node 2 draining; `partitions` as
    /// `(id, copies, primary)`.
    fn inventory_of(partitions: &[(u32, [u32; 3], u32)]) -> Inventory {
        Inventory {
            nodes: (1..=4)
                .map(|id| Node {
                    id,
                    machine: format!("m{id}"),
                    domain: format!("m{id}"),
                    state: if id == 2 {
                        NodeState::Draining
                    } else {
                        NodeState::Ok
                    },
                    primary_eligible: true,
                })
                .collect(),
            partitions: partitions
                .iter()
                .map(|(id, copies, primary)| Partition {
                    id: *id,
                    copies: copies.to_vec(),
                    primary: *primary,
                })
                .collect(),
        }
    }

    /// #80 S4 T12 (S-1): `drain_targets`, which runs before a drain's freeze, refuses for each
    /// of its two reasons and otherwise names a target that answered, the one owning fewest.
    /// (a) Node 2 still holds a copy of partition 3, which node 1 owns: refused, naming it.
    /// (b) Node 2 owns partition 1 and no other holder answered: refused, naming the partition.
    /// (c) With nodes 3 and 4 answering, partition 1 goes to node 4, which owns nothing (node 3
    /// owns partition 2). Unit.
    /// Cut: drop the check for copies node 2 does not own. Red, observed 2026-11-21 at load 92:
    /// `(a) refused: {1: 3}`.
    #[config_log::retcd_test]
    fn s80s4_drain_targets_refuse_before_the_freeze_for_each_s1_reason() {
        let foreign = inventory_of(&[(1, [2, 3, 4], 2), (3, [1, 2, 4], 1)]);
        let refused = drain_targets(&foreign, 2, &[1, 3, 4].into()).expect_err("(a) refused");
        assert!(
            refused.contains("still holds copies of partitions [3]"),
            "(a): {refused}"
        );
        let owned = inventory_of(&[(1, [2, 3, 4], 2), (2, [1, 3, 4], 3)]);
        let refused = drain_targets(&owned, 2, &BTreeSet::new()).expect_err("(b) refused");
        assert!(
            refused.contains("partition 1 has no copy outside node 2 whose node answers"),
            "(b): {refused}"
        );
        assert_eq!(
            drain_targets(&owned, 2, &[3, 4].into()),
            Ok([(1, 4)].into()),
            "(c)"
        );
    }
}
