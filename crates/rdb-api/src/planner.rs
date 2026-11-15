//! Automatic failover, slice C1 of issue #81: a planner in each node process that promotes the
//! partitions of a node whose process has gone silent and whose grant has run out.
//!
//! Off unless [`crate::DbConfig::failover`] is set. Only the holder of `planner/grant` acts, and
//! it acts only while that grant passes the same UTC rule a node's own grant does
//! ([`utc_ok`]). The grant is for liveness only: every promote is
//! [`admin::promote_looked`], the operator's path, so the `Fencing` CAS, the freeze, the expiry
//! proof and the re-reads decide safety, and two planners at once give one commit.
//!
//! A node `X` that owns a partition is failed over when either holds:
//!
//! 1. `grants/X` is frozen. A frozen grant never comes back for that process, so the silence
//!    check is skipped (ruling C-1): a process that resumes mid-sweep still has every partition
//!    moved.
//! 2. All of: `grants/X`'s expiry is proven past on this process's clock; this process's link
//!    to `X` echoed no clock probe within [`Failover::silent_after`] ([`Links::heard_within`],
//!    ruling C-2); and `X` answers no look within the read timeout.
//!
//! An expired owner that is still heard, or still answers a look, is not failed over: it is
//! faulted or cut from rEtcd but alive (customer answers 2 and 3). The planner logs
//! `planner_owner_alive_not_serving` once per change and leaves it to the operator.
//!
//! The target is [`choose_target`]'s. With fewer than two answering regular copies besides the
//! dead owner nothing is written (customer answer 1): `planner_no_eligible_copy`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{ConfigError, ConfigStore, GetRequest, ListRequest, MutationOutcome, PutRequest};
use futures::StreamExt;
use rdb_core::authority::clock::{e_new, expiry_proven, utc_ok, ClockView};
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::{PartitionRecord, RecordMember};
use rdb_core::contracts::control::{ControlKey, ControlPrefix};
use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, Generation, GrantId, NodeId, PartitionId,
    ReplicaRole,
};
use rdb_core::contracts::time::Tick;

use crate::admin::{self, PromoteCtx, PromoteError, Promoted};
use crate::clock::HostClock;
use crate::host::{Msg, NodeStatus, NodeStopped};
use crate::transport::Links;

/// How the planner runs (#81 C1). Set [`crate::DbConfig::failover`] to turn it on; `None`, the
/// default, never promotes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Failover {
    /// How often the planner renews its grant and looks for dead owners.
    pub poll: Duration,
    /// The most promotes one sweep runs at once.
    pub max_concurrent: usize,
    /// A node whose link echoed no clock probe for this long counts as silent.
    pub silent_after: Duration,
}

impl Default for Failover {
    /// 500 ms, 16 at once, 2 s of silence (design C, ruling C-2).
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(500),
            max_concurrent: 16,
            silent_after: Duration::from_secs(2),
        }
    }
}

/// Why [`choose_target`] picked no copy. Nothing is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NoEligible {
    /// `partitions/{p}` pins no members (layout 3), so the planner cannot tell its copies.
    MembersUnknown,
    /// Fewer than two regular copies besides the dead owner answered without a fault on an
    /// unfrozen grant: the survivor may lack acknowledged writes (customer answer 1).
    LoneSurvivor {
        /// How many did.
        answering: usize,
    },
}

impl NoEligible {
    const fn word(&self) -> &'static str {
        match self {
            Self::MembersUnknown => "members_unknown",
            Self::LoneSurvivor { .. } => "lone_survivor",
        }
    }
}

/// One `partitions/` record and the members it pins, if it pins them (layout 4).
type Entry = (PartitionRecord, Option<Vec<RecordMember>>);

/// The copy to promote for `partition`, whose owner `dead` is dead. Pure.
///
/// Candidates are the regular (non-shadow) members other than `dead` whose look answered with
/// no fault, holding a lineage, on a grant not in `frozen`. At least two must qualify: an
/// acknowledged write is on the primary and at least one secondary, and replication is a
/// prefix, so with both secondaries answering the higher one holds every acknowledged write.
/// The highest `(generation, applied)` wins. Ties are spread by partition: index
/// `partition mod count` of the tied nodes in node order (ruling C-6).
///
/// # Errors
///
/// [`NoEligible`].
pub(crate) fn choose_target(
    partition: PartitionId,
    members: &[RecordMember],
    dead: NodeId,
    looks: &[(NodeId, Option<NodeStatus>)],
    frozen: &BTreeSet<NodeId>,
) -> Result<NodeId, NoEligible> {
    let mut candidates: Vec<(Generation, u64, NodeId)> = members
        .iter()
        .filter(|member| member.node != dead && member.role != ReplicaRole::Shadow)
        .filter(|member| !frozen.contains(&member.node))
        .filter_map(|member| {
            let status = looks
                .iter()
                .find(|(node, _)| *node == member.node)
                .and_then(|(_, status)| status.as_ref())?;
            if status.fault.is_some() {
                return None;
            }
            let holds = status.holds?;
            Some((holds.generation, holds.applied, member.node))
        })
        .collect();
    if candidates.len() < 2 {
        return Err(NoEligible::LoneSurvivor {
            answering: candidates.len(),
        });
    }
    candidates.sort_unstable();
    let best = candidates.last().map(|(g, a, _)| (*g, *a));
    let tied: Vec<NodeId> = candidates
        .iter()
        .filter(|(g, a, _)| Some((*g, *a)) == best)
        .map(|(_, _, node)| *node)
        .collect();
    let index = usize::try_from(partition.0).unwrap_or(0) % tied.len();
    Ok(tied[index])
}

/// What the planner judged a node owning partitions to be.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Verdict {
    /// Its grant is current: nothing to do.
    Alive,
    /// Its grant ran out, but it is heard on the link or answers a look.
    AliveNotServing(&'static str),
    /// Fail it over, for this reason.
    Dead(&'static str),
}

/// The planner's grant while held: the record it last wrote.
#[derive(Debug, Clone, Copy)]
struct Held {
    grant: GrantId,
    expiry_utc_ms: i64,
    dispatched: Tick,
}

/// What the planner remembers between sweeps, so each line is logged once per change.
#[derive(Debug, Default)]
pub(crate) struct Memory {
    held: Option<Held>,
    unbounded: bool,
    verdicts: BTreeMap<NodeId, Verdict>,
    no_eligible: BTreeMap<PartitionId, &'static str>,
}

/// One node process's planner.
#[derive(Clone)]
pub(crate) struct Planner {
    pub(crate) node: NodeId,
    pub(crate) boot: BootId,
    pub(crate) store: Arc<dyn ConfigStore>,
    pub(crate) budgets: Budgets,
    pub(crate) clock: HostClock,
    pub(crate) links: Arc<Links>,
    pub(crate) failover: Failover,
    /// How long a look waits for an answer: the read timeout.
    pub(crate) look_within: Duration,
    /// How long one promote may take ([`PromoteCtx::within`]).
    pub(crate) promote_within: Duration,
}

impl Planner {
    /// Hold the grant and sweep, every [`Failover::poll`], until the task is aborted.
    pub(crate) async fn run(self) {
        tracing::info!(
            node = self.node.0,
            poll_ms = self.failover.poll.as_millis(),
            max_concurrent = self.failover.max_concurrent,
            silent_after_ms = self.failover.silent_after.as_millis(),
            "planner_started"
        );
        let mut memory = Memory::default();
        loop {
            if self.hold(&mut memory).await {
                self.sweep(&mut memory).await;
            }
            tokio::time::sleep(self.failover.poll).await;
        }
    }

    fn view(&self, now: Tick) -> ClockView {
        let mut view = ClockView::new();
        view.accept(self.clock.sample(now, &self.budgets));
        view
    }

    /// Acquire or renew `planner/grant`; whether this planner holds it now. Logged
    /// `planner_grant_acquired` and `planner_grant_lost` on each change.
    pub(crate) async fn hold(&self, memory: &mut Memory) -> bool {
        let was = memory.held.is_some();
        let now = self.clock.now();
        let view = self.view(now);
        let Some(expiry) = e_new(&view, now, &self.budgets) else {
            if !memory.unbounded {
                tracing::warn!(node = self.node.0, "planner_clock_unbounded");
                memory.unbounded = true;
            }
            self.lose(memory, "clock_unbounded");
            return false;
        };
        memory.unbounded = false;
        let key = ControlKey::PlannerGrant.encode();
        match self
            .store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
        {
            Err(error) => tracing::warn!(%error, "planner_grant_read_failed"),
            Ok(read) => {
                let found = read
                    .record
                    .map(|found| (GrantRecord::decode(&found.value), found.mod_revision));
                let write = match found {
                    None => Some((GrantId(1), 0)),
                    Some((None, revision)) => {
                        tracing::error!(revision, "planner_grant_undecodable");
                        None
                    }
                    Some((Some(record), revision)) => {
                        if record.node == self.node && record.boot == self.boot && !record.frozen {
                            Some((record.grant, revision))
                        } else if expiry_proven(&view, record.expiry_utc_ms, now, &self.budgets)
                            .is_some()
                        {
                            Some((GrantId(record.grant.0 + 1), revision))
                        } else {
                            None
                        }
                    }
                };
                if let Some((grant, revision)) = write {
                    let record = GrantRecord {
                        grant,
                        node: self.node,
                        boot: self.boot,
                        authority_generation: AuthorityGeneration::default(),
                        expiry_utc_ms: expiry,
                        frozen: false,
                    };
                    match self
                        .store
                        .put(PutRequest {
                            key: Bytes::from(key),
                            value: record.encode(),
                            expected_mod_revision: Some(revision),
                            dedup: None,
                        })
                        .await
                    {
                        Ok(put) if put.outcome == MutationOutcome::Applied => {
                            memory.held = Some(Held {
                                grant,
                                expiry_utc_ms: expiry,
                                dispatched: now,
                            });
                        }
                        Ok(_) | Err(ConfigError::Conflict { .. }) => {
                            tracing::debug!(revision, "planner_grant_conflict");
                        }
                        Err(error) => tracing::warn!(%error, "planner_grant_write_failed"),
                    }
                }
            }
        }
        // Held only while both of A1's rules pass for the last record written: the UTC rule
        // on this clock, and the local rule from the write's dispatch.
        let now = self.clock.now();
        let valid = memory.held.filter(|held| {
            utc_ok(&self.view(now), held.expiry_utc_ms, now, &self.budgets).is_ok()
                && now
                    .0
                    .saturating_sub(held.dispatched.0)
                    .saturating_add(self.budgets.dispatch_margin_millis)
                    < self.budgets.grant_millis
        });
        match (was, valid) {
            (false, Some(held)) => {
                memory.held = Some(held);
                tracing::info!(
                    node = self.node.0,
                    boot = self.boot.0,
                    grant = held.grant.0,
                    expiry = held.expiry_utc_ms,
                    "planner_grant_acquired"
                );
                true
            }
            (_, Some(_)) => true,
            (_, None) => {
                self.lose(memory, "expired");
                false
            }
        }
    }

    fn lose(&self, memory: &mut Memory, reason: &str) {
        if let Some(held) = memory.held.take() {
            tracing::info!(
                node = self.node.0,
                grant = held.grant.0,
                reason,
                "planner_grant_lost"
            );
        }
    }

    /// One pass, as the holder of the grant: judge each node that owns a partition, and promote
    /// every partition of a dead one. Returns each promote's answer, for the rows.
    pub(crate) async fn sweep(
        &self,
        memory: &mut Memory,
    ) -> Vec<(PartitionId, Result<Promoted, PromoteError>)> {
        let (grants, partitions) = match (self.grants().await, self.partitions().await) {
            (Ok(grants), Ok(partitions)) => (grants, partitions),
            (Err(error), _) | (_, Err(error)) => {
                tracing::warn!(%error, "planner_sweep_unread");
                return Vec::new();
            }
        };
        let mut owned: BTreeMap<NodeId, Vec<&Entry>> = BTreeMap::new();
        for entry in &partitions {
            owned.entry(entry.0.owner).or_default().push(entry);
        }
        let frozen: BTreeSet<NodeId> = grants
            .iter()
            .filter(|(_, record)| record.frozen)
            .map(|(node, _)| *node)
            .collect();
        let mut todo = Vec::new();
        for (owner, records) in owned {
            if owner == self.node {
                continue;
            }
            let verdict = self
                .judge(owner, grants.get(&owner), records[0].0.partition)
                .await;
            let changed = memory.verdicts.get(&owner) != Some(&verdict);
            match &verdict {
                Verdict::Alive => {}
                Verdict::AliveNotServing(evidence) if changed => tracing::warn!(
                    node = owner.0,
                    evidence,
                    grant_expired = true,
                    partitions = records.len(),
                    "planner_owner_alive_not_serving"
                ),
                Verdict::Dead(reason) => {
                    if changed {
                        tracing::info!(
                            node = owner.0,
                            reason,
                            partitions = records.len(),
                            silent_after_ms = self.failover.silent_after.as_millis(),
                            look_ms = self.look_within.as_millis(),
                            "planner_node_silent"
                        );
                    }
                    todo.extend(records.iter().map(|entry| (owner, (*entry).clone())));
                }
                Verdict::AliveNotServing(_) => {}
            }
            memory.verdicts.insert(owner, verdict);
        }
        // Each promote owns what it reads: a future borrowing the sweep's locals is not `Send`
        // for every lifetime, which the planner's spawned task needs.
        let tasks: Vec<_> = todo
            .into_iter()
            .map(|(dead, (record, members))| {
                let planner = self.clone();
                let frozen = frozen.clone();
                let partition = record.partition;
                async move {
                    let answer = planner
                        .fail_over(partition, dead, members.as_deref(), &frozen)
                        .await;
                    (partition, answer)
                }
            })
            .collect();
        let decided: Vec<_> = futures::stream::iter(tasks)
            .buffer_unordered(self.failover.max_concurrent.max(1))
            .collect()
            .await;
        let mut answers = Vec::new();
        for (partition, decided) in decided {
            match decided {
                Err(none) => {
                    let word = none.word();
                    if memory.no_eligible.insert(partition, word) != Some(word) {
                        tracing::warn!(
                            partition = partition.0,
                            reason = word,
                            detail = ?none,
                            "planner_no_eligible_copy"
                        );
                    }
                }
                Ok(answer) => {
                    memory.no_eligible.remove(&partition);
                    answers.push((partition, answer));
                }
            }
        }
        answers
    }

    async fn judge(
        &self,
        owner: NodeId,
        grant: Option<&GrantRecord>,
        partition: PartitionId,
    ) -> Verdict {
        if grant.is_some_and(|record| record.frozen) {
            return Verdict::Dead("grant_frozen");
        }
        let now = self.clock.now();
        let expired = grant.is_none_or(|record| {
            expiry_proven(&self.view(now), record.expiry_utc_ms, now, &self.budgets).is_some()
        });
        if !expired {
            return Verdict::Alive;
        }
        if self.links.heard_within(owner, self.failover.silent_after) {
            return Verdict::AliveNotServing("heard_on_link");
        }
        if self.look(partition, owner).await.is_some() {
            return Verdict::AliveNotServing("answered_look");
        }
        Verdict::Dead("silent_and_expired")
    }

    /// `node`'s state for `partition`, or `None` when it does not answer in the read timeout.
    async fn look(&self, partition: PartitionId, node: NodeId) -> Option<NodeStatus> {
        let (reply, answer) = std::sync::mpsc::channel();
        self.links
            .post(node, Msg::Inspect { partition, reply })
            .ok()?;
        let within = self.look_within;
        tokio::task::spawn_blocking(move || answer.recv_timeout(within).ok())
            .await
            .ok()
            .flatten()
    }

    /// Choose a target for `partition` and promote it, dropping `dead`.
    async fn fail_over(
        &self,
        partition: PartitionId,
        dead: NodeId,
        members: Option<&[RecordMember]>,
        frozen: &BTreeSet<NodeId>,
    ) -> Result<Result<Promoted, PromoteError>, NoEligible> {
        let members = members.ok_or(NoEligible::MembersUnknown)?;
        let mut looks = Vec::new();
        for member in members.iter().filter(|member| member.node != dead) {
            looks.push((member.node, self.look(partition, member.node).await));
        }
        let to = choose_target(partition, members, dead, &looks, frozen)?;
        let looked = looks
            .iter()
            .find(|(node, _)| *node == to)
            .and_then(|(_, status)| status.clone());
        let holds = looked.as_ref().and_then(|status| status.holds);
        tracing::info!(
            partition = partition.0,
            dead = dead.0,
            to = to.0,
            generation = holds.map(|h| h.generation.0),
            applied = holds.map(|h| h.applied),
            answering = ?looks
                .iter()
                .filter(|(_, status)| status.is_some())
                .map(|(node, _)| node.0)
                .collect::<Vec<_>>(),
            "planner_promote_decision"
        );
        let (planned, version, refused) =
            match admin::promote_members(partition, to, looked, self.look_within) {
                Ok((planned, version)) => (planned, version, None),
                Err(refused) => (Vec::new(), ConfigVersion(0), Some(refused)),
            };
        let links = Arc::clone(&self.links);
        let member_boot = move |node: NodeId| links.boot_of(node);
        let links = Arc::clone(&self.links);
        let send =
            move |node: NodeId, msg: Msg| links.post(node, msg).map_err(|_| NodeStopped(node));
        let ctx = PromoteCtx {
            store: Arc::clone(&self.store),
            budgets: self.budgets,
            clock: self.clock,
            partition,
            owner_must_be_dead: true,
            look_within: self.look_within,
            to,
            drop: Some(dead),
            within: self.promote_within,
            members: planned,
            members_version: version,
            member_boot: Arc::new(member_boot),
            send: Arc::new(send),
        };
        let answer = admin::promote_looked(ctx, refused).await;
        match &answer {
            Ok(promoted) => tracing::info!(
                partition = partition.0,
                owner = promoted.owner.0,
                prior = promoted.prior_owner.0,
                generation = promoted.generation.0,
                "planner_promoted"
            ),
            Err(error) => tracing::warn!(
                partition = partition.0,
                to = to.0,
                %error,
                no_mutation = error.no_mutation(),
                "planner_promote_failed"
            ),
        }
        Ok(answer)
    }

    async fn grants(&self) -> Result<BTreeMap<NodeId, GrantRecord>, String> {
        let records = self.list(ControlPrefix::Grants).await?;
        records
            .iter()
            .map(|(key, value)| {
                GrantRecord::decode(value)
                    .map(|record| (record.node, record))
                    .ok_or_else(|| format!("{key} is not a grant record"))
            })
            .collect()
    }

    async fn partitions(&self) -> Result<Vec<Entry>, String> {
        let records = self.list(ControlPrefix::Partitions).await?;
        records
            .iter()
            .map(|(key, value)| {
                PartitionRecord::decode(value)
                    .map(|record| (record, RecordMember::decode_all(value)))
                    .ok_or_else(|| format!("{key} is not a partition record"))
            })
            .collect()
    }

    async fn list(&self, prefix: ControlPrefix) -> Result<Vec<(String, Bytes)>, String> {
        let prefix = prefix.encode();
        let listed = self
            .store
            .list(ListRequest {
                prefix: Bytes::from_static(prefix.as_bytes()),
                max_items: 0,
                max_bytes: 0,
            })
            .await
            .map_err(|error| format!("list {prefix}: {error}"))?;
        if listed.truncated {
            return Err(format!(
                "list {prefix} came back truncated at revision {}",
                listed.read_revision
            ));
        }
        Ok(listed
            .records
            .into_iter()
            .map(|found| {
                (
                    String::from_utf8_lossy(&found.key).into_owned(),
                    found.value,
                )
            })
            .collect())
    }
}

#[cfg(test)]
mod tests;
