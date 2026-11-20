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
use std::time::{Duration, Instant};

use bytes::Bytes;
use config_core::{ConfigError, ConfigStore, GetRequest, ListRequest, MutationOutcome, PutRequest};
use futures::StreamExt;
use rdb_core::authority::clock::{e_new, expiry_proven, utc_ok, ClockView};
use rdb_core::authority::grant::{clear_frozen_verdict, GrantRecord};
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

/// How often one repeated warn is said per cause; the lines between are counted.
pub(crate) const REPEAT_EVERY: Duration = Duration::from_secs(10);

/// A warn said at most once per [`REPEAT_EVERY`] per cause, with how many it held back.
#[derive(Debug, Default)]
pub(crate) struct Repeats {
    /// Per cause: when it was last said, and how many were held back since.
    said: BTreeMap<&'static str, (Instant, u64)>,
}

impl Repeats {
    /// Note one occurrence of `cause` at `now`: `Some(held back since the last)` when it is to
    /// be said, `None` when it is held back.
    pub(crate) fn note(&mut self, cause: &'static str, now: Instant) -> Option<u64> {
        match self.said.get_mut(cause) {
            Some((last, held)) if now.saturating_duration_since(*last) < REPEAT_EVERY => {
                *held += 1;
                None
            }
            Some((last, held)) => {
                let suppressed = std::mem::take(held);
                *last = now;
                Some(suppressed)
            }
            None => {
                self.said.insert(cause, (now, 0));
                Some(0)
            }
        }
    }

    /// The condition [`Self::note`] counted has cleared: `Some(held back)` when anything was
    /// noted since the last clear, and every cause starts afresh, so the next occurrence is
    /// said at once.
    pub(crate) fn clear(&mut self) -> Option<u64> {
        if self.said.is_empty() {
            return None;
        }
        let held = self.said.values().map(|(_, held)| *held).sum();
        self.said.clear();
        Some(held)
    }

    /// Whether any cause was said within [`REPEAT_EVERY`] of `now`.
    fn quiet(&self, now: Instant) -> bool {
        self.said
            .values()
            .any(|(last, _)| now.saturating_duration_since(*last) < REPEAT_EVERY)
    }
}

/// Say once that the warn `warn`, rate-limited by `repeats`, has cleared: `planner_warn_recovered`
/// with how many were held back. Nothing when it was never said.
fn recovered(repeats: &mut Repeats, warn: &'static str) {
    if let Some(suppressed) = repeats.clear() {
        tracing::info!(warn, suppressed, "planner_warn_recovered");
    }
}

/// Why a sweep could not read `grants/` or `partitions/` whole: a cause for [`Repeats`], and
/// the text.
#[derive(Debug)]
struct Unread {
    cause: &'static str,
    text: String,
}

/// A refused promote's cause, for [`Repeats`]: its variant.
const fn promote_cause(error: &PromoteError) -> &'static str {
    match error {
        PromoteError::Shadow { .. } => "shadow",
        PromoteError::NotAMember { .. } => "not_a_member",
        PromoteError::AlreadyOwner { .. } => "already_owner",
        PromoteError::DropIsTarget => "drop_is_target",
        PromoteError::PeersUnknown { .. } => "peers_unknown",
        PromoteError::Unconfigured { .. } => "unconfigured",
        PromoteError::StaleMembers { .. } => "stale_members",
        PromoteError::Control(_) => "control",
        PromoteError::Record(_) => "record",
        PromoteError::Superseded { .. } => "superseded",
        PromoteError::Unreachable { .. } => "unreachable",
        PromoteError::TargetFrozen { .. } => "target_frozen",
        PromoteError::OwnerAlive { .. } => "owner_alive",
        PromoteError::NoSuchPartition { .. } => "no_such_partition",
        PromoteError::NoControlStore => "no_control_store",
        PromoteError::ClockUnbounded => "clock_unbounded",
        PromoteError::Unanswered { .. } => "unanswered",
    }
}

/// A store error's cause, for [`Repeats`]: its kind, not its text, which can name revisions.
fn cause(error: &ConfigError) -> &'static str {
    match error {
        ConfigError::Unavailable { .. } => "unavailable",
        ConfigError::NotLeader { .. } => "not_leader",
        ConfigError::DeadlineExceededUnknownOutcome => "deadline_exceeded",
        ConfigError::Conflict { .. } => "conflict",
        _ => "other",
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

/// How long an owner may be heard with no grant of its own before the planner raises
/// `planner_owner_alive_not_serving` for it (ruling on PR #119 item 9): three grant lifetimes,
/// 18 s at the spec's 6 s.
///
/// The state is a node's start: its recovery replaces the past boot's record once that record
/// has run out (the state begins only then) and the discovery window has passed, which is at
/// most one grant lifetime at the spec defaults (2 s against 6 s). In the failover rows (3 s
/// grants, the discovery window stretched to 6 s) the whole start, from open to the owner's own
/// grant, took about 6.3 s, and the state itself under 0.2 s when left alone. Three lifetimes
/// cover a start twice over and still alert well inside a minute.
pub(crate) fn no_grant_grace(budgets: &Budgets) -> Duration {
    Duration::from_millis(budgets.grant_millis.saturating_mul(3))
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

/// What one look at a node got back.
#[derive(Debug)]
enum Asked {
    /// Its state.
    Answered(Box<NodeStatus>),
    /// Nothing within the read timeout: busy or stalled, not known to be gone.
    NoAnswer,
    /// Its mailbox refused the question or dropped it unanswered: the node's thread is gone,
    /// or the node is not registered.
    Stopped,
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
    /// Whether the last hold found this planner's own host faulted: the warn is said once.
    host_faulted: bool,
    /// Whether the last hold found this planner's own host's mailbox closed: said once.
    host_stopped: bool,
    /// `planner_grant_read_failed`, once per [`REPEAT_EVERY`] per cause.
    read_failed: Repeats,
    /// `planner_grant_undecodable`, `planner_grant_write_failed` and `planner_sweep_unread`,
    /// each once per window per cause, and `planner_warn_recovered` once it clears (PR #119
    /// review). The retries are unchanged.
    grant_undecodable: Repeats,
    grant_write_failed: Repeats,
    sweep_unread: Repeats,
    /// `planner_promote_failed` per partition, once per window per cause; a partition whose
    /// promote failed within the window logs its next decision at debug.
    promote_failed: BTreeMap<PartitionId, Repeats>,
    /// Owners heard but holding no grant of their own: the boot the links knew and when the
    /// planner first saw it so (`planner_owner_no_grant_yet`, said once per entry). A new boot
    /// starts the clock again.
    no_grant_yet: BTreeMap<NodeId, (Option<BootId>, Instant)>,
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
    ///
    /// A planner whose own host has faulted holds nothing: it stops renewing, so the record
    /// lapses within the grant's length and another node's planner takes it (lead ruling on
    /// PR #119). So does one whose own host's mailbox is closed (PR #119 review): that thread
    /// has ended. A host that does not answer the look in time is neither: a stalled node
    /// thread still renews, as V6 shows.
    pub(crate) async fn hold(&self, memory: &mut Memory) -> bool {
        let own = self.ask(PartitionId(1), self.node).await;
        if matches!(own, Asked::Stopped) {
            // The local host's mailbox is closed: its thread has ended and never answers
            // again, so this process serves nothing. Read as "no answer", it kept the grant
            // renewing for good (PR #119 review). Released like a faulted host's.
            if !memory.host_stopped {
                tracing::warn!(
                    node = self.node.0,
                    held = memory.held.is_some(),
                    lapses_within_ms = self.budgets.grant_millis,
                    "planner_grant_released_host_stopped"
                );
                memory.host_stopped = true;
            }
            self.lose(memory, "host_stopped");
            return false;
        }
        memory.host_stopped = false;
        if let Some(fault) = match own {
            Asked::Answered(status) => status.fault,
            Asked::Stopped | Asked::NoAnswer => None,
        } {
            if !memory.host_faulted {
                tracing::warn!(
                    node = self.node.0,
                    %fault,
                    held = memory.held.is_some(),
                    lapses_within_ms = self.budgets.grant_millis,
                    "planner_grant_released_host_faulted"
                );
                memory.host_faulted = true;
            }
            self.lose(memory, "host_faulted");
            return false;
        }
        memory.host_faulted = false;
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
            Err(error) => {
                if let Some(suppressed) = memory.read_failed.note(cause(&error), Instant::now()) {
                    tracing::warn!(%error, suppressed, "planner_grant_read_failed");
                }
            }
            Ok(read) => {
                let found = read
                    .record
                    .map(|found| (GrantRecord::decode(&found.value), found.mod_revision));
                if !matches!(found, Some((None, _))) {
                    recovered(&mut memory.grant_undecodable, "planner_grant_undecodable");
                }
                let write = match found {
                    None => Some((GrantId(1), 0)),
                    Some((None, revision)) => {
                        if let Some(suppressed) =
                            memory.grant_undecodable.note("undecodable", Instant::now())
                        {
                            tracing::error!(revision, suppressed, "planner_grant_undecodable");
                        }
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
                            recovered(&mut memory.grant_write_failed, "planner_grant_write_failed");
                            memory.held = Some(Held {
                                grant,
                                expiry_utc_ms: expiry,
                                dispatched: now,
                            });
                        }
                        Ok(_) | Err(ConfigError::Conflict { .. }) => {
                            tracing::debug!(revision, "planner_grant_conflict");
                        }
                        Err(error) => {
                            if let Some(suppressed) = memory
                                .grant_write_failed
                                .note(cause(&error), Instant::now())
                            {
                                tracing::warn!(%error, suppressed, "planner_grant_write_failed");
                            }
                        }
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
        // `partitions/` is not read when `grants/` failed: the sweep is refused either way.
        let read = match self.grants().await {
            Ok(grants) => self
                .partitions()
                .await
                .map(|partitions| (grants, partitions)),
            Err(unread) => Err(unread),
        };
        let (grants, partitions) = match read {
            Ok(read) => {
                recovered(&mut memory.sweep_unread, "planner_sweep_unread");
                read
            }
            Err(unread) => {
                if let Some(suppressed) = memory.sweep_unread.note(unread.cause, Instant::now()) {
                    tracing::warn!(
                        error = %unread.text,
                        cause = unread.cause,
                        suppressed,
                        "planner_sweep_unread"
                    );
                }
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
        // #81 X2: a frozen grant of a node that owns nothing is a candidate for the clear.
        let records: Vec<PartitionRecord> = partitions.iter().map(|entry| entry.0).collect();
        for node in frozen.iter().filter(|node| !owned.contains_key(node)) {
            self.clear_frozen(*node, &records).await;
        }
        let mut todo = Vec::new();
        for (owner, records) in owned {
            // A planner does not judge its own node while that node's grant stands: its own
            // host answers for itself. A frozen grant never comes back for this process, so
            // its own remaining partitions go through the judgment like any owner's (PR #119
            // review): otherwise a frozen node holding `planner/grant` strands them.
            if owner == self.node && !frozen.contains(&owner) {
                continue;
            }
            let verdict = self
                .judge(owner, grants.get(&owner), records[0].0.partition)
                .await;
            // No alert before the owner's first grant of its own, within [`no_grant_grace`]: a
            // node just started has none until its recovery takes one, and it is not "not
            // serving" (lead ruling on PR #119). Until then `grants/{owner}` is absent or still
            // the record of the boot before, which runs out first. A grant counts as its own
            // when the links know no other boot for the owner. Past the grace it has had its
            // chance, and the Q2 alert is said, once per change as below (ruling on item 9).
            let boot = self.links.boot_of(owner);
            let past_boot = grants
                .get(&owner)
                .is_some_and(|record| boot.is_some_and(|boot| boot != record.boot));
            if past_boot || !grants.contains_key(&owner) {
                if let Verdict::AliveNotServing(evidence) = verdict {
                    let now = Instant::now();
                    let since = match memory.no_grant_yet.get(&owner) {
                        Some((at, since)) if *at == boot => *since,
                        _ => {
                            tracing::info!(
                                node = owner.0,
                                evidence,
                                past_boot,
                                grace_ms = no_grant_grace(&self.budgets).as_millis(),
                                "planner_owner_no_grant_yet"
                            );
                            memory.no_grant_yet.insert(owner, (boot, now));
                            memory.verdicts.remove(&owner);
                            now
                        }
                    };
                    let waited = now.saturating_duration_since(since);
                    if waited < no_grant_grace(&self.budgets) {
                        continue;
                    }
                    if memory.verdicts.get(&owner) != Some(&verdict) {
                        tracing::warn!(
                            node = owner.0,
                            evidence,
                            grant_expired = past_boot,
                            no_grant_of_own = true,
                            since_ms = waited.as_millis(),
                            partitions = records.len(),
                            "planner_owner_alive_not_serving"
                        );
                    }
                    memory.verdicts.insert(owner, verdict);
                    continue;
                }
            }
            memory.no_grant_yet.remove(&owner);
            let changed = memory.verdicts.get(&owner) != Some(&verdict);
            match &verdict {
                Verdict::Alive => {}
                Verdict::AliveNotServing(evidence) if changed => tracing::warn!(
                    node = owner.0,
                    evidence,
                    grant_expired = true,
                    no_grant_of_own = false,
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
        let now = Instant::now();
        let tasks: Vec<_> = todo
            .into_iter()
            .map(|(dead, (record, members))| {
                let planner = self.clone();
                let frozen = frozen.clone();
                let partition = record.partition;
                let quiet = memory
                    .promote_failed
                    .get(&partition)
                    .is_some_and(|repeats| repeats.quiet(now));
                async move {
                    let answer = planner
                        .fail_over(partition, dead, members.as_deref(), &frozen, quiet)
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
                Ok((to, answer)) => {
                    memory.no_eligible.remove(&partition);
                    match &answer {
                        Ok(_) => {
                            if let Some(mut repeats) = memory.promote_failed.remove(&partition) {
                                recovered(&mut repeats, "planner_promote_failed");
                            }
                        }
                        Err(error) => {
                            if let Some(suppressed) = memory
                                .promote_failed
                                .entry(partition)
                                .or_default()
                                .note(promote_cause(error), Instant::now())
                            {
                                tracing::warn!(
                                    partition = partition.0,
                                    to = to.0,
                                    %error,
                                    no_mutation = error.no_mutation(),
                                    suppressed,
                                    "planner_promote_failed"
                                );
                            }
                        }
                    }
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
        match self.ask(partition, node).await {
            Asked::Answered(status) => Some(*status),
            Asked::NoAnswer | Asked::Stopped => None,
        }
    }

    /// [`Self::look`], telling a mailbox that refused the question, or dropped it unanswered,
    /// from one that did not answer in the read timeout.
    async fn ask(&self, partition: PartitionId, node: NodeId) -> Asked {
        let (reply, answer) = std::sync::mpsc::channel();
        if self
            .links
            .post(node, Msg::Inspect { partition, reply })
            .is_err()
        {
            return Asked::Stopped;
        }
        let within = self.look_within;
        match tokio::task::spawn_blocking(move || answer.recv_timeout(within)).await {
            Ok(Ok(status)) => Asked::Answered(Box::new(status)),
            Ok(Err(std::sync::mpsc::RecvTimeoutError::Disconnected)) => Asked::Stopped,
            Ok(Err(std::sync::mpsc::RecvTimeoutError::Timeout)) | Err(_) => Asked::NoAnswer,
        }
    }

    /// Choose a target for `partition` and promote it, dropping `dead`: the target and the
    /// promote's answer. `quiet`, for a partition whose promote failed within the window, logs
    /// the decision at debug (PR #119 review); the sweep logs a failure.
    async fn fail_over(
        &self,
        partition: PartitionId,
        dead: NodeId,
        members: Option<&[RecordMember]>,
        frozen: &BTreeSet<NodeId>,
        quiet: bool,
    ) -> Result<(NodeId, Result<Promoted, PromoteError>), NoEligible> {
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
        let answering: Vec<u32> = looks
            .iter()
            .filter(|(_, status)| status.is_some())
            .map(|(node, _)| node.0)
            .collect();
        let (generation, applied) = (holds.map(|h| h.generation.0), holds.map(|h| h.applied));
        if quiet {
            tracing::debug!(
                partition = partition.0,
                dead = dead.0,
                to = to.0,
                generation,
                applied,
                ?answering,
                "planner_promote_decision"
            );
        } else {
            tracing::info!(
                partition = partition.0,
                dead = dead.0,
                to = to.0,
                generation,
                applied,
                ?answering,
                "planner_promote_decision"
            );
        }
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
        if let Ok(promoted) = &answer {
            tracing::info!(
                partition = partition.0,
                owner = promoted.owner.0,
                prior = promoted.prior_owner.0,
                generation = promoted.generation.0,
                "planner_promoted"
            );
        }
        Ok((to, answer))
    }

    /// #81 X2 (owner answer (a) on #81, 2026-11-19): delete `grants/{node}` when it is a frozen
    /// record of a boot that is gone and the node owns nothing ([`clear_frozen_verdict`]), so
    /// the node's new boot can be chosen and promoted again. Run by [`Self::sweep`] only, so
    /// only while this planner holds `planner/grant`. The record is read again here and deleted
    /// at that read's `mod_revision`: a renewal or a fresh freeze in between makes the delete
    /// a conflict, and nothing is cleared.
    async fn clear_frozen(&self, node: NodeId, partitions: &[PartitionRecord]) {
        let key = ControlKey::Grant(node).encode();
        let found = match self
            .store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
        {
            Ok(read) => read.record,
            Err(error) => {
                tracing::warn!(node = node.0, %error, "planner_frozen_grant_unread");
                return;
            }
        };
        let Some(found) = found else { return };
        let Some(record) = GrantRecord::decode(&found.value) else {
            tracing::warn!(
                node = node.0,
                revision = found.mod_revision,
                "planner_frozen_grant_undecodable"
            );
            return;
        };
        let live = self.links.boot_of(node);
        let now = self.clock.now();
        let sample = self.clock.sample(now, &self.budgets);
        if let Err(refusal) =
            clear_frozen_verdict(node, &record, live, partitions, sample, now, &self.budgets)
        {
            tracing::debug!(
                node = node.0,
                record_boot = record.boot.0,
                live_boot = live.map(|boot| boot.0),
                ?refusal,
                "planner_frozen_grant_kept"
            );
            return;
        }
        match admin::delete_grant_at(&*self.store, node, found.mod_revision).await {
            Ok(admin::Clearance::Cleared(at)) => tracing::info!(
                node = node.0,
                record_boot = record.boot.0,
                live_boot = live.map(|boot| boot.0),
                revision = found.mod_revision,
                cleared_at = ?at,
                "planner_frozen_grant_cleared"
            ),
            // Moved or gone since the read (a new freeze, a takeover): the next sweep reads again.
            Ok(clearance) => tracing::debug!(
                node = node.0,
                revision = found.mod_revision,
                ?clearance,
                "planner_frozen_grant_not_cleared"
            ),
            Err(error) => tracing::warn!(
                node = node.0,
                revision = found.mod_revision,
                %error,
                "planner_frozen_grant_clear_failed"
            ),
        }
    }

    async fn grants(&self) -> Result<BTreeMap<NodeId, GrantRecord>, Unread> {
        let records = self
            .list(
                ControlPrefix::Grants,
                "grants_list_failed",
                "grants_list_truncated",
            )
            .await?;
        records
            .iter()
            .map(|(key, value)| {
                GrantRecord::decode(value)
                    .map(|record| (record.node, record))
                    .ok_or_else(|| Unread {
                        cause: "grant_entry_undecodable",
                        text: format!("{key} is not a grant record"),
                    })
            })
            .collect()
    }

    async fn partitions(&self) -> Result<Vec<Entry>, Unread> {
        let records = self
            .list(
                ControlPrefix::Partitions,
                "partitions_list_failed",
                "partitions_list_truncated",
            )
            .await?;
        records
            .iter()
            .map(|(key, value)| {
                PartitionRecord::decode(value)
                    .map(|record| (record, RecordMember::decode_all(value)))
                    .ok_or_else(|| Unread {
                        cause: "partition_entry_undecodable",
                        text: format!("{key} is not a partition record"),
                    })
            })
            .collect()
    }

    /// The whole family under `prefix`; a failed list is `failed`, a truncated one `truncated`.
    async fn list(
        &self,
        prefix: ControlPrefix,
        failed: &'static str,
        truncated: &'static str,
    ) -> Result<Vec<(String, Bytes)>, Unread> {
        let prefix = prefix.encode();
        let listed = self
            .store
            .list(ListRequest {
                prefix: Bytes::from_static(prefix.as_bytes()),
                max_items: 0,
                max_bytes: 0,
            })
            .await
            .map_err(|error| Unread {
                cause: failed,
                text: format!("list {prefix}: {error}"),
            })?;
        if listed.truncated {
            return Err(Unread {
                cause: truncated,
                text: format!(
                    "list {prefix} came back truncated at revision {}",
                    listed.read_revision
                ),
            });
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
