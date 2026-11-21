//! Bootstrap of partition 1 through F1 (M9 architecture §3, `rdb_api::admin`).
//!
//! A new partition has no prior owner, so nothing in the kernel starts its first recovery. The
//! admin plays the planner's part, once, the way the scenarios lowering does in the sim:
//!
//! 1. Create `partitions/1` in rEtcd: generation 0, epoch 0, owner node 1, `Serving`. Create-only,
//!    so a partition that already has a history is refused rather than overwritten. When the
//!    create is not reported applied (it conflicts, or its reply is lost), the admin reads the
//!    record back and adopts it only if it is exactly this record and nothing has written it since
//!    it was created (`create_revision == mod_revision`). Such a record means no recovery has
//!    committed, so there is no history to lose; a bootstrap that lost its reply can then be run
//!    again. If two bootstraps adopt the same record, F1's generation-1 commit is a CAS on its
//!    revision, so only one of them wins. Anything else is refused (F-004).
//! 2. Hand node 1's F1 the `Plan`: rf3, node 1 primary, nodes 2 and 3 regular secondaries, the
//!    anchor at generation 0, sequence 0, `Digest::ROOT`.
//! 3. Hand it the `FenceProven` whose `DurableDrain` revision is the revision step 1 wrote.
//!
//! From there F1 queries the three empty copies, selects, commits generation 1 and the host
//! carries it to every member. The admin never injects `Recovered`, never writes a record into
//! the partition and never answers for a module (lead ruling: no workaround in `rdb_api`).
//!
//! [`clear_grant`] (M9 S2b) is the planner's other duty here: it removes the `grants/{node}`
//! record a restarted node's old process left behind, through rdb-core's three guards.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use config_core::{
    ConfigError, ConfigStore, DeleteRequest, GetRequest, ListRequest, MutationOutcome, PutRequest,
};
use rdb_core::authority::clock::{expiry_proven, ClockView};
use rdb_core::authority::grant::{clear_verdict, ClearRefusal, ClearVerdict, GrantRecord};
use rdb_core::authority::node::{
    host_of, machine_refusal, parse_addr, Host, NodeRecord, MAX_ADDR_LEN,
};
use rdb_core::authority::partition::{
    prior_of, PartitionLifecycle, PartitionRecord, PartitionRoot, RecordMember, INSTANCE_LEN,
};
use rdb_core::contracts::authority::{
    AuthorityView, DenyReason, FencingProof, Lineage, Revocation,
};
use rdb_core::contracts::control::{ControlKey, ControlPrefix};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::{ErrorKind, RetryRule};
use rdb_core::contracts::event::{Budgets, EventKind, KernelEvent};
use rdb_core::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, CorrelationId, Generation, GrantId, NodeId,
    OperationId, OwnerEpoch, PartitionId, RangeId, ReplicaRole, Revision, Seq,
};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::recovery::{
    Candidate, CommittedWithout, LineageAnchor, PriorRecord, RecoveryEvent, RecoveryPlan,
};
use rdb_core::contracts::time::Tick;
use rdb_core::partition_map::RoutesRoot;

use crate::clock::HostClock;
use crate::db::{ApiError, Db};
use crate::host::{Msg, NodeStatus, NodeStopped, BOOT};

/// The partition S0 serves.
pub const PARTITION: PartitionId = PartitionId(1);

/// The configuration version every M9 member set is pinned at.
pub const CONFIG_VERSION: ConfigVersion = ConfigVersion(1);

/// The node that leads the bootstrap recovery and owns partition 1.
pub const OWNER: NodeId = NodeId(1);

/// How long a returning stale owner's suffix is kept: the lowering's value.
const RETENTION_MILLIS: u64 = 1_000;

/// The correlation the bootstrap's two events carry.
const BOOTSTRAP: CorrelationId = CorrelationId(1);

/// Why the bootstrap did not start.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    /// A partition's record, or `routes/0`, already exists in the control store and is not one
    /// this bootstrap can adopt: it differs from what this bootstrap writes, or it has been
    /// written since it was created.
    #[error("{key} already exists at revision {current}: refusing to bootstrap twice")]
    AlreadyExists {
        /// The control key.
        key: String,
        /// Its revision.
        current: u64,
    },
    /// The control store refused or failed the write.
    #[error("control store: {0}")]
    Control(String),
    /// Node 1's thread is gone.
    #[error("node {} stopped before the bootstrap reached it", OWNER.0)]
    NodeStopped,
}

/// The rf3 member set: node 1 primary as copy 0, nodes 2 and 3 regular secondaries. For open
/// and bootstrap only: it holds no shadow, so a promote reads the live roles from its target
/// instead (review-s2 F-004, A-R412).
#[must_use]
pub fn members() -> Vec<Member> {
    (1..=3u8)
        .map(|n| Member {
            copy: CopyId(n - 1),
            node: NodeId(u32::from(n)),
            boot: BOOT,
            role: if n == 1 {
                ReplicaRole::Primary
            } else {
                ReplicaRole::RegularSecondary
            },
        })
        .collect()
}

/// The rf3 member set of [`members`], every node at `boot`: the boot one open starts all three
/// nodes at (M10, arch R3b).
#[must_use]
pub fn members_at(boot: BootId) -> Vec<Member> {
    members()
        .into_iter()
        .map(|member| Member { boot, ..member })
        .collect()
}

/// The owner a partition is formed with: round robin over the three formed nodes, so partition
/// 1 is node 1's and partition 2 is node 2's (issue #79). `partition` is at least 1.
#[must_use]
pub const fn initial_owner(partition: PartitionId) -> NodeId {
    NodeId((partition.0 - 1) % 3 + 1)
}

/// How many copies every partition has: rf3, at any node count (#80 S1b). Every check that
/// asks "does the partition hold all its copies" compares with this, never with a node count.
pub const RF: usize = rdb_core::placement::INITIAL_COPIES;

/// The members a declaration first places `partition` on, every node at `boot` (#80 S1b,
/// ADR-rdb-0023): [`rdb_core::placement::initial`]'s ring over the declared `nodes`, in node
/// order, each node's copy number `node - 1`, the Primary role on the ring's primary. Over
/// [`crate::db::ALL_NODES`] it equals [`members_for`].
///
/// # Panics
///
/// As [`rdb_core::placement::initial`]: fewer than three nodes, or not in order. Every caller
/// passes a declared node list, which [`declare`] and [`Formation::decode`] check.
#[must_use]
pub fn placed(nodes: &[NodeId], partition: PartitionId, boot: BootId) -> Vec<Member> {
    let ids: Vec<u32> = nodes.iter().map(|node| node.0).collect();
    let ring = rdb_core::placement::initial(&ids, partition.0);
    ring.copies
        .iter()
        .map(|node| Member {
            copy: copy_of(NodeId(*node)),
            node: NodeId(*node),
            boot,
            role: if *node == ring.primary {
                ReplicaRole::Primary
            } else {
                ReplicaRole::RegularSecondary
            },
        })
        .collect()
}

/// The owner a declaration places `partition` with over `nodes` (#80 S1b): the ring's primary.
/// Over [`crate::db::ALL_NODES`] it equals [`initial_owner`].
///
/// # Panics
///
/// As [`placed`].
#[must_use]
pub fn owner_in(nodes: &[NodeId], partition: PartitionId) -> NodeId {
    let ids: Vec<u32> = nodes.iter().map(|node| node.0).collect();
    NodeId(rdb_core::placement::initial(&ids, partition.0).primary)
}

/// The copy number a node's copy takes when it is first placed: `node - 1`, as [`members`]
/// numbers nodes 1 to 3 (#80 S1b).
fn copy_of(node: NodeId) -> CopyId {
    CopyId(
        u8::try_from(node.0 - 1)
            .unwrap_or_else(|_| panic!("node {} is past MAX_NODES: no copy number", node.0)),
    )
}

/// [`members_at`] for `partition`: each node keeps its copy number, and the Primary role sits on
/// [`initial_owner`].
#[must_use]
pub fn members_for(partition: PartitionId, boot: BootId) -> Vec<Member> {
    let owner = initial_owner(partition);
    members_at(boot)
        .into_iter()
        .map(|member| Member {
            role: if member.node == owner {
                ReplicaRole::Primary
            } else {
                ReplicaRole::RegularSecondary
            },
            ..member
        })
        .collect()
}

/// A recovery plan for the anchor's partition from `anchor`: rf3 at `version` with `members`,
/// the owner at `boot` in the authority view, every copy a candidate and in `rebuild_required`,
/// the pinned primary with `grant` at `generation`. The host's restart re-plan uses it too
/// (#42, A-R249).
///
/// Only the pinned primary is `primary_eligible` (ruling A-R215). The host acquires, tracks and
/// adopts on that copy alone, so F1 must lead with it even when another copy holds the longer
/// prefix: the leader then catches up before its grant. Every copy eligible let `select_leader`
/// name the prefix source, which owned the record while node 1 held the grant, and the open
/// served nothing.
pub(crate) fn recovery_plan(
    anchor: LineageAnchor,
    version: ConfigVersion,
    members: Vec<Member>,
    boot: BootId,
    grant: GrantId,
    generation: AuthorityGeneration,
    prior: PriorRecord,
) -> RecoveryPlan {
    let config = PartitionConfig::new(anchor.lineage.partition, version, members);
    RecoveryPlan {
        anchor,
        candidates: config
            .members
            .iter()
            .map(|member| Candidate {
                copy: member.copy,
                primary_eligible: member.role == ReplicaRole::Primary,
                healthy: true,
                within_capacity: true,
                has_valid_grant: true,
            })
            .collect(),
        rebuild_required: config.members.iter().map(|member| member.copy).collect(),
        authority_view: AuthorityView {
            lineage: anchor.lineage,
            grant_id: grant,
            boot_id: boot,
            authority_generation: generation,
            config_version: version,
            authority_seq: 1,
            valid_through_tick: Tick(u64::MAX),
            past_horizon: DenyReason::NoGrant,
        },
        retention_millis: RETENTION_MILLIS,
        must_hold: std::collections::BTreeSet::new(),
        config,
        prior,
        release: rdb_core::contracts::recovery::TailRelease::default(),
    }
}

/// Write `partitions/1`, then start F1 on node 1 through `send`.
///
/// # Errors
///
/// [`BootstrapError`] when a record this bootstrap cannot adopt exists, the store fails, or
/// node 1 is gone.
pub async fn bootstrap(
    store: &Arc<dyn ConfigStore>,
    now: Tick,
    send: impl Fn(Msg) -> Result<(), NodeStopped>,
) -> Result<Revision, BootstrapError> {
    bootstrap_at(store, now, BOOT, send).await
}

/// [`bootstrap`] with every node at `boot`, the boot this open started them at (M10, arch R3b).
///
/// # Errors
///
/// As [`bootstrap`].
pub async fn bootstrap_at(
    store: &Arc<dyn ConfigStore>,
    now: Tick,
    boot: BootId,
    send: impl Fn(Msg) -> Result<(), NodeStopped>,
) -> Result<Revision, BootstrapError> {
    form(store, now, boot, 1, |_, msg| send(msg)).await
}

/// Form a database of `partitions` partitions, every node at `boot`, sending each partition's
/// recovery to its [`initial_owner`] through `send` (issue #79). In order: `routes/0` at N>1 (none
/// at N=1), then every partition's record, then every plan. Answers partition 1's revision.
///
/// # Errors
///
/// As [`bootstrap`], for any partition or `routes/0`.
pub(crate) async fn form(
    store: &Arc<dyn ConfigStore>,
    now: Tick,
    boot: BootId,
    partitions: u32,
    send: impl Fn(NodeId, Msg) -> Result<(), NodeStopped>,
) -> Result<Revision, BootstrapError> {
    if partitions > 1 {
        let key = ControlKey::Route(RangeId(0)).encode();
        let value = RoutesRoot {
            partition_count: partitions,
        }
        .encode();
        let revision = create_or_adopt(store, &key, &value).await?;
        tracing::info!(key = %key, revision = revision.0, partitions, "bootstrap_routes_root");
    }
    let all: Vec<PartitionId> = (1..=partitions).map(PartitionId).collect();
    let written = write_records(store, &crate::db::ALL_NODES, &all).await?;
    for (partition, revision) in &written {
        let members = members_for(*partition, boot);
        start_recovery(now, boot, *partition, *revision, members, &send)?;
    }
    Ok(written[0].1)
}

/// Create the generation-0 record of each of `partitions`, placed over `nodes`, or adopt one an
/// earlier attempt wrote (issue #79): the half of [`form`] before any plan goes out. Answers
/// each one's revision. `nodes` is the declared node list, or [`crate::db::ALL_NODES`] for a
/// database no declaration made (#80 S1b).
///
/// # Errors
///
/// As [`bootstrap`].
pub(crate) async fn write_records(
    store: &Arc<dyn ConfigStore>,
    nodes: &[NodeId],
    partitions: &[PartitionId],
) -> Result<Vec<(PartitionId, Revision)>, BootstrapError> {
    let mut written = Vec::with_capacity(partitions.len());
    for partition in partitions.iter().copied() {
        let key = ControlKey::Partition(partition).encode();
        written.push((
            partition,
            create_or_adopt(store, &key, &initial_record(nodes, partition).encode()).await?,
        ));
    }
    Ok(written)
}

/// The generation-0 record [`write_records`] creates for `partition`, its owner the one
/// [`owner_in`] places over `nodes` (#80 S1b). Layout 2, as before S1b: the record names its
/// owner and no members, so over nodes 1 to 3 its bytes are what they always were.
fn initial_record(nodes: &[NodeId], partition: PartitionId) -> PartitionRecord {
    PartitionRecord {
        partition,
        owner: owner_in(nodes, partition),
        generation: Generation(0),
        owner_epoch: OwnerEpoch(0),
        config_version: CONFIG_VERSION,
        lifecycle: PartitionLifecycle::Serving,
    }
}

/// Create `key` with `value`, or adopt it when an earlier attempt's create landed (F-004).
async fn create_or_adopt(
    store: &Arc<dyn ConfigStore>,
    key: &str,
    value: &Bytes,
) -> Result<Revision, BootstrapError> {
    let created = store
        .put(PutRequest {
            key: Bytes::from(key.to_owned()),
            value: value.clone(),
            expected_mod_revision: Some(0),
            dedup: None,
        })
        .await;
    match created {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            let revision = Revision(response.revision);
            tracing::info!(key = %key, revision = revision.0, "bootstrap_partition_record");
            Ok(revision)
        }
        Ok(_) | Err(ConfigError::DeadlineExceededUnknownOutcome) => adopt(store, key, value).await,
        Err(error) => Err(BootstrapError::Control(error.to_string())),
    }
}

/// Send `partition`'s bootstrap plan, pinning `members`, and fence to the member they pin
/// Primary: the placement's owner (#80 S1b).
fn start_recovery(
    now: Tick,
    boot: BootId,
    partition: PartitionId,
    revision: Revision,
    members: Vec<Member>,
    send: &impl Fn(NodeId, Msg) -> Result<(), NodeStopped>,
) -> Result<(), BootstrapError> {
    let owner = members
        .iter()
        .find(|member| member.role == ReplicaRole::Primary)
        .map(|member| member.node)
        .ok_or_else(|| {
            BootstrapError::Control(format!(
                "partition {}'s bootstrap plan pins no primary",
                partition.0
            ))
        })?;
    let lineage = Lineage {
        partition,
        generation: Generation(0),
        owner_epoch: OwnerEpoch(0),
    };
    let anchor = LineageAnchor {
        lineage,
        base_seq: Seq::ZERO,
        base_digest: Digest::ROOT,
    };
    let plan = recovery_plan(
        anchor,
        CONFIG_VERSION,
        members,
        boot,
        GrantId(1),
        AuthorityGeneration(1),
        PriorRecord::fresh(),
    );
    let fence = FencingProof {
        partition,
        prior_generation: Generation(0),
        prior_owner_epoch: OwnerEpoch(0),
        prior_grant_id: GrantId(1),
        prior_boot_id: boot,
        revocation: Revocation::DurableDrain {
            ack_revision: revision,
        },
        control_revision: revision,
        decision_tick: now,
    };
    for event in [
        RecoveryEvent::Plan(Box::new(plan)),
        RecoveryEvent::FenceProven(Box::new(fence)),
    ] {
        send(
            owner,
            Msg::Inject {
                partition,
                correlation: BOOTSTRAP,
                kind: EventKind::Kernel(KernelEvent::Recovery(event)),
            },
        )
        .map_err(|_| BootstrapError::NodeStopped)?;
    }
    tracing::info!(
        partition = partition.0,
        owner = owner.0,
        "bootstrap_recovery_started"
    );
    Ok(())
}

/// Read `key` back after a create that was not reported applied, and return its revision when
/// it is `value` and has not been written since it was created (F-004).
async fn adopt(
    store: &Arc<dyn ConfigStore>,
    key: &str,
    value: &Bytes,
) -> Result<Revision, BootstrapError> {
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.to_owned()),
        })
        .await
        .map_err(|error| BootstrapError::Control(error.to_string()))?;
    let Some(found) = read.record else {
        return Err(BootstrapError::Control(format!(
            "{key} is absent after a create that was not reported applied"
        )));
    };
    if found.value != *value || found.create_revision != found.mod_revision {
        return Err(BootstrapError::AlreadyExists {
            key: key.to_owned(),
            current: found.mod_revision,
        });
    }
    tracing::info!(key = %key, revision = found.mod_revision, "bootstrap_partition_record_adopted");
    Ok(Revision(found.mod_revision))
}

/// The `operations/` key of the formation record (#80 S1a, ADR-rdb-0022): tag `0x00E0` in bits
/// 48..64, apart from the promote tag and F1's, as [`PROMOTE_OPERATION_BASE`] is.
pub const FORMATION_OPERATION: OperationId = OperationId(0x00E0 << 48);

/// The version byte of the formation record: layout 1 is the instance, the partition count as
/// a little-endian u32, then one little-endian u32 per declared node.
const FORMATION_LAYOUT: u8 = 1;

/// The `operations/` key of the declaration's reservation (PR #138 review, ADR-rdb-0022): the
/// formation record's tag block, one past it. [`declare`] creates it before any other write, so
/// of two declarations of different counts exactly one goes on to write.
pub const RESERVATION_OPERATION: OperationId = OperationId(FORMATION_OPERATION.0 + 1);

/// The version byte an S1a build wrote its reservation with: the partition count as a
/// little-endian u32, then one little-endian u32 per declared node. Read, never written, so a
/// refusal can name such a reservation (#80 S1b, ADR-rdb-0023).
const RESERVATION_LAYOUT_NODES_ONLY: u8 = 1;

/// The version byte of the reservation (#80 S1b, ADR-rdb-0023): layout 2 is the partition
/// count as a little-endian u32, the node count as one byte, then per node its id as a
/// little-endian u32 and its address and machine, each as one length byte and that many
/// bytes. So two declarations that differ in one address reserve different bytes, and only one
/// goes on.
const RESERVATION_LAYOUT: u8 = 2;

/// What a declaration asks for (#80 S1a): the database's nodes and its partition count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declaration {
    /// The data nodes: 1 to K, K from 3 to [`crate::db::MAX_NODES`] ([`node_set_refusal`]),
    /// each with where it listens and its machine (#80 S1b, [`Declaration::records`]).
    pub nodes: Vec<NodeDecl>,
    /// The partition count, fixed from then on.
    pub partitions: u32,
}

/// One declared node (#80 S1b): its id, the address it listens on (`host:port`, as its
/// `--listen` and every `--peers` name it), and the machine it runs on. `None` takes the
/// address's host as [`parse_addr`] parses it as the machine: `[0:0:0:0:0:0:0:1]:7100` gives
/// `::1`, and `DB-A.example.:7100` gives `db-a.example`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDecl {
    /// The node.
    pub node: NodeId,
    /// Where it listens.
    pub addr: String,
    /// Its machine label; `None` is the host of [`Self::addr`] as [`parse_addr`] parses it,
    /// not as written. Lowercased, then held to [`machine_refusal`]: 1 to 63 bytes of
    /// `[a-z0-9._:-]`, so no whitespace, control byte or other character is ever stored.
    pub machine: Option<String>,
}

/// The reservation a declaration created, as [`Declaration::decode_reservation`] reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    /// The reserved partition count.
    pub partitions: u32,
    /// The reserved nodes, in order.
    pub nodes: Vec<NodeId>,
    /// Each node's record, from layout 2; `None` for a layout-1 reservation an S1a build wrote.
    pub records: Option<Vec<NodeRecord>>,
}

/// The formation record a declaration wrote: the database's identity, its partition count and
/// its nodes. Written once and never changed (ADR-rdb-0022).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Formation {
    /// The rDB instance every node's copy binds.
    pub instance: [u8; INSTANCE_LEN],
    /// The partition count.
    pub partitions: u32,
    /// The declared nodes, in order.
    pub nodes: Vec<NodeId>,
}

impl Formation {
    /// Layout 1 ([`FORMATION_LAYOUT`]).
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(1 + INSTANCE_LEN + 4 + 4 * self.nodes.len());
        out.push(FORMATION_LAYOUT);
        out.extend_from_slice(&self.instance);
        out.extend_from_slice(&self.partitions.to_le_bytes());
        for node in &self.nodes {
            out.extend_from_slice(&node.0.to_le_bytes());
        }
        Bytes::from(out)
    }

    /// `None` for a body that is not layout 1.
    #[must_use]
    pub fn decode(body: &[u8]) -> Option<Self> {
        let [FORMATION_LAYOUT, rest @ ..] = body else {
            return None;
        };
        let (instance, rest) = rest.split_at_checked(INSTANCE_LEN)?;
        let (partitions, rest) = rest.split_at_checked(4)?;
        let (nodes, tail) = rest.as_chunks::<4>();
        if !tail.is_empty() {
            return None;
        }
        let nodes: Vec<NodeId> = nodes
            .iter()
            .map(|node| NodeId(u32::from_le_bytes(*node)))
            .collect();
        // #80 S1b: placement runs over this list, so a list no declaration could write is not
        // a formation record this build reads.
        if node_set_refusal(&nodes).is_some() {
            return None;
        }
        Some(Self {
            instance: instance.try_into().ok()?,
            partitions: u32::from_le_bytes(partitions.try_into().ok()?),
            nodes,
        })
    }
}

impl Declaration {
    /// The declared node ids, in order.
    #[must_use]
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|decl| decl.node).collect()
    }

    /// The `nodes/{id}` records this declaration writes (#80 S1b), or why it is refused: the node
    /// set ([`node_set_refusal`]), then each node in turn: its address (one [`parse_addr`]
    /// takes: a port of 1 to 65535, and a dialable IP or a whitelisted host name; at most 255
    /// bytes; not an earlier node's), then its machine label ([`machine_refusal`], after
    /// lowercasing; unset, the address's host as [`parse_addr`] parses it). Last, across all
    /// nodes, [`machine_clash`].
    ///
    /// # Errors
    ///
    /// The refusal's text.
    pub fn records(&self) -> Result<Vec<NodeRecord>, String> {
        if let Some(why) = node_set_refusal(&self.node_ids()) {
            return Err(why);
        }
        let mut records: Vec<NodeRecord> = Vec::with_capacity(self.nodes.len());
        for decl in &self.nodes {
            let n = decl.node.0;
            let host = parse_addr(&decl.addr).map_err(|why| format!("node {n}: {why}"))?;
            if decl.addr.len() > MAX_ADDR_LEN {
                return Err(format!(
                    "node {n} address {:?} is longer than {MAX_ADDR_LEN} bytes",
                    decl.addr
                ));
            }
            if let Some(other) = records.iter().find(|record| record.addr == decl.addr) {
                return Err(format!(
                    "nodes {} and {n} both have address {}: each node listens on its own",
                    other.node.0, decl.addr
                ));
            }
            let machine = decl
                .machine
                .clone()
                .unwrap_or_else(|| host.to_string())
                .to_ascii_lowercase();
            if let Some(why) = machine_refusal(&machine) {
                return Err(format!(
                    "node {n}: {why}{}",
                    if decl.machine.is_none() {
                        " (taken from its address's host; name one with --machines)"
                    } else {
                        ""
                    }
                ));
            }
            records.push(NodeRecord {
                node: decl.node,
                addr: decl.addr.clone(),
                machine,
            });
        }
        if let Some(why) = machine_clash(&records) {
            return Err(why);
        }
        Ok(records)
    }

    /// The reservation's body, layout 2 ([`RESERVATION_LAYOUT`]), over `records` as
    /// [`Self::records`] made them.
    #[must_use]
    pub fn encode_reservation(&self, records: &[NodeRecord]) -> Bytes {
        let mut out = Vec::with_capacity(1 + 4 + 1 + records.len() * (4 + 2 + 32));
        out.push(RESERVATION_LAYOUT);
        out.extend_from_slice(&self.partitions.to_le_bytes());
        // At most `MAX_NODES` records, checked by `records`.
        out.push(u8::try_from(records.len()).unwrap_or(u8::MAX));
        for record in records {
            // The record's own layout without its version byte: id, address, machine.
            out.extend_from_slice(&record.encode()[1..]);
        }
        Bytes::from(out)
    }

    /// The reservation `body` holds: layout 2, or layout 1 from an S1a build (no records).
    /// `None` for anything else.
    #[must_use]
    pub fn decode_reservation(body: &[u8]) -> Option<Reservation> {
        match body {
            [RESERVATION_LAYOUT_NODES_ONLY, rest @ ..] => {
                let (partitions, rest) = rest.split_at_checked(4)?;
                let (nodes, tail) = rest.as_chunks::<4>();
                if !tail.is_empty() {
                    return None;
                }
                Some(Reservation {
                    partitions: u32::from_le_bytes(partitions.try_into().ok()?),
                    nodes: nodes
                        .iter()
                        .map(|node| NodeId(u32::from_le_bytes(*node)))
                        .collect(),
                    records: None,
                })
            }
            [RESERVATION_LAYOUT, rest @ ..] => {
                let (partitions, rest) = rest.split_at_checked(4)?;
                let (&count, mut rest) = rest.split_first()?;
                let mut records = Vec::with_capacity(usize::from(count));
                for _ in 0..count {
                    // Each entry is a node record's layout without its version byte: an id,
                    // then two length-prefixed fields.
                    let addr_len = usize::from(*rest.get(4)?);
                    let machine_len = usize::from(*rest.get(4 + 1 + addr_len)?);
                    let len = 4 + 1 + addr_len + 1 + machine_len;
                    let mut one = vec![1];
                    one.extend_from_slice(rest.get(..len)?);
                    records.push(NodeRecord::decode(&one)?);
                    rest = &rest[len..];
                }
                if !rest.is_empty() {
                    return None;
                }
                Some(Reservation {
                    partitions: u32::from_le_bytes(partitions.try_into().ok()?),
                    nodes: records.iter().map(|record| record.node).collect(),
                    records: Some(records),
                })
            }
            _ => None,
        }
    }
}

/// Why `records` put two nodes on one machine (#80 S1b, ruling 1), or `None`: two nodes whose
/// machine labels are equal, or whose addresses have the same host, whatever their labels say.
/// So `127.0.0.1` and `127.0.0.2` are two hosts, and two ports of one address are one. Labels
/// are compared as written, lowercase ([`Declaration::records`] lowercases them). Hosts are
/// compared as [`host_of`] parses them: an IP by value, so `[::1]` and `[0:0:0:0:0:0:0:1]` are
/// one host, and `[::ffff:127.0.0.1]` is `127.0.0.1`; a name lowercase and less one trailing
/// dot. A name and an IP of one machine are not caught; only the labels can say so.
#[must_use]
pub fn machine_clash(records: &[NodeRecord]) -> Option<String> {
    for (at, a) in records.iter().enumerate() {
        for b in &records[at + 1..] {
            let host_a = host_of(&a.addr);
            if host_a.is_some() && host_a == host_of(&b.addr) {
                return Some(format!(
                    "nodes {} ({}) and {} ({}) are on one machine: their addresses have the same \
                     host, and a database runs one data node per machine",
                    a.node.0, a.addr, b.node.0, b.addr
                ));
            }
            if a.machine == b.machine {
                return Some(format!(
                    "nodes {} and {} are both on machine {:?}: a database runs one data node per \
                     machine",
                    a.node.0, b.node.0, a.machine
                ));
            }
        }
    }
    None
}

/// Why the control link may not be plaintext for a database whose nodes listen at `addrs`
/// (#80 S2 ruling 1, ADR-rdb-0024), or `None`: `security` is `Insecure` and some node's address
/// has a host that is not a loopback IP. A host name counts as another machine, `localhost`
/// included: only an IP says it is this one. So a one-machine rig on 127.0.0.N (or `[::1]`)
/// runs without certificates, and anything else needs mutual TLS to rEtcd. The text names the
/// first such node and its address, and how to give the link TLS. The caller says what was
/// left undone.
#[must_use]
pub fn control_tls_refusal<'a>(
    addrs: impl IntoIterator<Item = (NodeId, &'a str)>,
    security: config_core::TransportSecurity,
) -> Option<String> {
    if security != config_core::TransportSecurity::Insecure {
        return None;
    }
    let (node, addr) = addrs
        .into_iter()
        .find(|(_, addr)| !matches!(host_of(addr), Some(Host::Ip(ip)) if ip.is_loopback()))?;
    Some(format!(
        "node {}'s address {addr} is on another machine and the control store link is not TLS: \
         a database whose nodes are not all on loopback IPs talks to rEtcd over mutual TLS; \
         pass --control-tls DIR (ca.pem, client.pem, client.key), or embed with a \
         config_client::GrpcClient under TlsMode::MutualTls",
        node.0
    ))
}

/// How a node list prints in a refusal: `[1@127.0.0.1:7100/127.0.0.1, ...]`.
fn records_in_words(records: &[NodeRecord]) -> String {
    let each: Vec<String> = records
        .iter()
        .map(|record| format!("{}@{}/{}", record.node.0, record.addr, record.machine))
        .collect();
    format!("[{}]", each.join(", "))
}

/// What a store declared by an S1a build lacks, and what to do (#80 S1b ruling 4, ADR-rdb-0023):
/// its formation record names the nodes, and no `nodes/` record says where each listens.
pub const S1A_STORE: &str = "the store was declared by a build before #80 S1b: it holds a \
                             formation record and no nodes/ records, so no node's address can \
                             be checked; declare the database again, with --peers, on a new \
                             empty control store";

/// How [`declare`] ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Declared {
    /// This call wrote the formation record.
    Created(Formation),
    /// The same declaration was already there; nothing was written.
    Already(Formation),
}

/// Why [`declare`] wrote no formation record.
#[derive(Debug, thiserror::Error)]
pub enum DeclareError {
    /// The declaration is not one this build makes, or differs from the one already there.
    #[error("{0}")]
    Refused(String),
    /// A control record was in the way, or the store failed a write.
    #[error(transparent)]
    Records(#[from] BootstrapError),
    /// The store failed a read: `step` names it (`get <key>`, `list <prefix>`). The error stays
    /// typed, so an open can say what to do about an `Unavailable` store.
    #[error("{step}: {error}")]
    Store {
        /// The read that failed.
        step: String,
        /// How it failed.
        #[source]
        error: ConfigError,
    },
    /// A control record a declaration reads (the formation record, its reservation,
    /// `routes/0` or a partition record) does not decode.
    #[error("{key} does not decode as a record this build reads")]
    Undecodable {
        /// Its key.
        key: String,
    },
}

/// Why `nodes` is not a node set a declaration takes (#80 S1b), or `None`: nodes 1 to K with no
/// gap and none twice, K from 3 to [`crate::db::MAX_NODES`]. Ids are never reused, so a
/// database declared with K nodes may add only `MAX_NODES - K` over its life (ruling C3).
#[must_use]
pub fn node_set_refusal(nodes: &[NodeId]) -> Option<String> {
    let max = crate::db::MAX_NODES;
    let named: Vec<u32> = nodes.iter().map(|node| node.0).collect();
    let k = u32::try_from(nodes.len()).unwrap_or(u32::MAX);
    if !(3..=max).contains(&k) {
        return Some(format!(
            "nodes {named:?}: a database is declared with 3 to {max} nodes"
        ));
    }
    if named.iter().copied().ne(1..=k) {
        return Some(format!(
            "nodes {named:?}: a declaration names nodes 1 to K, each once, in order, with no gap"
        ));
    }
    None
}

/// The formation record, `None` when no declaration wrote one (#80 S1a).
///
/// # Errors
///
/// [`DeclareError::Store`] when the store fails, [`DeclareError::Undecodable`] for a record this
/// build does not read.
pub async fn read_formation(store: &dyn ConfigStore) -> Result<Option<Formation>, DeclareError> {
    let key = ControlKey::Operation(FORMATION_OPERATION).encode();
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| DeclareError::Store {
            step: format!("get {key}"),
            error,
        })?;
    read.record
        .map(|found| Formation::decode(&found.value).ok_or(DeclareError::Undecodable { key }))
        .transpose()
}

/// Every live `nodes/{id}` record, by node (#80 S1b, ADR-rdb-0023); empty when none was
/// written. #80 S4: a removed record (layout 3) is not live and is left out, not undecodable,
/// so one removed node does not fail the list for every reader; [`read_removed_nodes`] names
/// it.
///
/// # Errors
///
/// [`DeclareError::Store`] when the store fails or truncates the list,
/// [`DeclareError::Undecodable`] for a record this build does not read, a key that is not
/// `nodes/` and a decimal id, or a body whose node is not its key's.
pub async fn read_nodes(
    store: &dyn ConfigStore,
) -> Result<BTreeMap<NodeId, NodeRecord>, DeclareError> {
    read_nodes_at(store).await.map(|(records, _)| records)
}

/// [`read_nodes`], with the revision the list read at (#80 S4: the watcher's
/// `members_revision`, and where its watch starts).
///
/// # Errors
///
/// As [`read_nodes`].
pub async fn read_nodes_at(
    store: &dyn ConfigStore,
) -> Result<(BTreeMap<NodeId, NodeRecord>, u64), DeclareError> {
    let prefix = ControlPrefix::Nodes.encode();
    let listed = store
        .list(ListRequest {
            prefix: Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .map_err(|error| DeclareError::Store {
            step: format!("list {prefix}"),
            error,
        })?;
    if listed.truncated {
        return Err(DeclareError::Store {
            step: format!("list {prefix}"),
            error: ConfigError::Unavailable {
                reason: format!("truncated at revision {}", listed.read_revision),
            },
        });
    }
    let mut records = BTreeMap::new();
    for found in &listed.records {
        let key = String::from_utf8_lossy(&found.key).into_owned();
        if removed_at_key(&found.value, &key).is_some() {
            continue;
        }
        let record = NodeRecord::decode(&found.value)
            .filter(|record| ControlKey::Node(record.node).encode() == key)
            .ok_or_else(|| DeclareError::Undecodable { key: key.clone() })?;
        records.insert(record.node, record);
    }
    Ok((records, listed.read_revision))
}

/// The node of `body` when it is a removed record (layout 3, #80 S4) for `key`'s node.
fn removed_at_key(body: &[u8], key: &str) -> Option<NodeId> {
    NodeRecord::decode_removed(body)
        .map(|(record, _)| record.node)
        .filter(|node| ControlKey::Node(*node).encode() == key)
}

/// The nodes whose `nodes/` record is removed (layout 3, #80 S4); their ids are never reused.
/// [`read_nodes`] leaves them out. Other records are not read here.
///
/// # Errors
///
/// [`DeclareError::Store`] when the store fails or truncates the list.
pub async fn read_removed_nodes(store: &dyn ConfigStore) -> Result<BTreeSet<NodeId>, DeclareError> {
    let prefix = ControlPrefix::Nodes.encode();
    let listed = store
        .list(ListRequest {
            prefix: Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .map_err(|error| DeclareError::Store {
            step: format!("list {prefix}"),
            error,
        })?;
    if listed.truncated {
        return Err(DeclareError::Store {
            step: format!("list {prefix}"),
            error: ConfigError::Unavailable {
                reason: format!("truncated at revision {}", listed.read_revision),
            },
        });
    }
    Ok(listed
        .records
        .iter()
        .filter_map(|found| removed_at_key(&found.value, &String::from_utf8_lossy(&found.key)))
        .collect())
}

/// Why node `node` may not start with `listen` and `peers` on a database declared as
/// `formation` with node records `records` (#80 S1b rulings 2 and 4, ADR-rdb-0023), or `None`.
/// Pure: `rdb_node` calls it before it writes its boot or binds, and a declared `Db::open` calls
/// it with `DbConfig::addresses` before it creates `nodes/` or writes anything.
///
/// - No formation record: `None`; an undeclared database has no node records.
/// - `node` neither in the formation record nor in a `nodes/` record: refused. A node added
///   at runtime (#80 S4, `admin::add_node`) is in the records only, and is declared.
/// - No `nodes/` record at all: an S1a store, refused with [`S1A_STORE`].
/// - `node`'s record absent: refused.
/// - `listen` must be its record's address, or the same port on the wildcard host (`0.0.0.0`,
///   `::`).
/// - Every `peers` entry, this node's included, must be its node's record's address, and every
///   recorded node with a **lower** id than `node` must have one: `node` dials those. A node
///   with a higher id dials `node`, so it may be left out (#80 S4, ADR-rdb-0025): a node added
///   after this one's `--peers` was written does not stop it starting.
///
/// Each refusal that compares a value with a record names the record (`nodes/N`) and both
/// values.
#[must_use]
pub fn node_start_refusal(
    node: NodeId,
    listen: &str,
    peers: &BTreeMap<NodeId, String>,
    formation: Option<&Formation>,
    records: &BTreeMap<NodeId, NodeRecord>,
) -> Option<String> {
    const NOTHING: &str = "nothing was started";
    let formation = formation?;
    // #80 S4: the formation record's nodes and every node added since, which has a record.
    let declared = || {
        let mut all: Vec<u32> = formation
            .nodes
            .iter()
            .chain(records.keys())
            .map(|n| n.0)
            .collect();
        all.sort_unstable();
        all.dedup();
        all
    };
    if !formation.nodes.contains(&node) && !records.contains_key(&node) {
        return Some(format!(
            "node {} is not declared: the database declares nodes {:?}; {NOTHING}",
            node.0,
            declared()
        ));
    }
    if records.is_empty() {
        return Some(format!("{S1A_STORE}; {NOTHING}"));
    }
    let key = |n: NodeId| ControlKey::Node(n).encode();
    let Some(own) = records.get(&node) else {
        return Some(format!(
            "{} is absent, so where node {} listens is not declared; {NOTHING}",
            key(node),
            node.0
        ));
    };
    let wildcard_same_port = || {
        let (host, port) = listen.rsplit_once(':')?;
        let (_, own_port) = own.addr.rsplit_once(':')?;
        let wildcard = matches!(host, "0.0.0.0" | "[::]" | "::");
        (wildcard && port == own_port).then_some(())
    };
    if listen != own.addr && wildcard_same_port().is_none() {
        return Some(format!(
            "listen address {listen} disagrees with {}, which records {}: a node listens where \
             its record says; {NOTHING}",
            key(node),
            own.addr
        ));
    }
    for (peer, addr) in peers {
        let Some(record) = records.get(peer) else {
            return Some(format!(
                "peer address {}={addr} names node {}, which has no {}: the database declares \
                 nodes {:?}; {NOTHING}",
                peer.0,
                peer.0,
                key(*peer),
                declared()
            ));
        };
        if *addr != record.addr {
            return Some(format!(
                "peer address {}={addr} disagrees with {}, which records {}; {NOTHING}",
                peer.0,
                key(*peer),
                record.addr
            ));
        }
    }
    for declared_node in declared().into_iter().map(NodeId) {
        if declared_node < node && !peers.contains_key(&declared_node) {
            let addr = records
                .get(&declared_node)
                .map_or("no address", |record| record.addr.as_str());
            return Some(format!(
                "no peer address for node {}, which {} records at {addr}: node {} dials every \
                 node with a lower id; {NOTHING}",
                declared_node.0,
                key(declared_node),
                node.0
            ));
        }
    }
    None
}

/// Declare a database (#80 S1a, ADR-rdb-0022): first the reservation under
/// [`RESERVATION_OPERATION`] ([`reserve`]), then `routes/0` at more than one partition, every
/// partition's generation-0 record ([`write_records`], owner as [`owner_in`] places it), then
/// **last** the formation record under [`FORMATION_OPERATION`], naming a new instance. No copy is
/// written: each node process forms the partitions it owns from its own empty folder.
///
/// Write-once and idempotent: the same declaration again answers [`Declared::Already`] and
/// writes nothing; a different one is refused, naming what is there. A declaration that stopped
/// before its formation record is finished by running it again: the records it wrote, its
/// reservation included, are adopted.
///
/// # Errors
///
/// [`DeclareError`]. A database formed some other way (`--form`, `lr up`) holds a rooted
/// `partitions/1`, and another declaration holds its reservation: each refuses this one with
/// nothing written, [`DeclareError::Refused`].
pub async fn declare(
    store: &Arc<dyn ConfigStore>,
    declaration: &Declaration,
) -> Result<Declared, DeclareError> {
    let records = declaration
        .records()
        .map_err(|why| DeclareError::Refused(format!("{why}; nothing was written")))?;
    let nodes = declaration.node_ids();
    let max = rdb_core::partition_map::MAX_PARTITIONS;
    if !(1..=max).contains(&declaration.partitions) {
        return Err(DeclareError::Refused(format!(
            "partitions {} is out of range: a database has 1 to {max} partitions; nothing was \
             written",
            declaration.partitions
        )));
    }
    // #80 S2 (ADR-rdb-0024): after the address and machine checks, so their refusals keep
    // their order, and before anything is read or written.
    if let Some(why) = control_tls_refusal(
        records
            .iter()
            .map(|record| (record.node, record.addr.as_str())),
        store.capabilities().transport_security,
    ) {
        tracing::warn!(%why, "declare_refused_plaintext_control");
        return Err(DeclareError::Refused(format!("{why}; nothing was written")));
    }
    // #80 S1b: `Already` only when the node records are this declaration's too, so a run that
    // changed an address is refused, and an S1a store (no records) is refused, not adopted.
    let same = |found: Formation, held: BTreeMap<NodeId, NodeRecord>| {
        let held: Vec<NodeRecord> = held.into_values().collect();
        if held.is_empty() {
            return Err(DeclareError::Refused(format!(
                "{S1A_STORE}; nothing was written"
            )));
        }
        if found.partitions == declaration.partitions && found.nodes == nodes && held == records {
            tracing::info!(
                instance_hex = %instance_hex(&found.instance),
                partitions = found.partitions,
                "declare_already"
            );
            Ok(Declared::Already(found))
        } else {
            Err(DeclareError::Refused(format!(
                "the database is already declared with {} partitions and nodes {}: a \
                 declaration is written once; nothing was written",
                found.partitions,
                records_in_words(&held)
            )))
        }
    };
    if let Some(found) = read_formation(&**store).await? {
        return same(found, read_nodes(&**store).await?);
    }
    let partitions = declaration.partitions;
    declare_preflight(&**store, &records, partitions).await?;
    reserve(store, declaration, &records).await?;
    if partitions > 1 {
        let key = ControlKey::Route(RangeId(0)).encode();
        let value = RoutesRoot {
            partition_count: partitions,
        }
        .encode();
        create_or_adopt(store, &key, &value).await?;
    }
    // #80 S1b: every node's record, before any partition record, so a store that holds a
    // partition record holds every node's address.
    for record in &records {
        let key = ControlKey::Node(record.node).encode();
        create_or_adopt(store, &key, &record.encode()).await?;
    }
    let all: Vec<PartitionId> = (1..=partitions).map(PartitionId).collect();
    write_records(store, &nodes, &all).await?;
    let formation = Formation {
        instance: *uuid::Uuid::new_v4().as_bytes(),
        partitions,
        nodes: nodes.clone(),
    };
    // PR #138 review r5: the declaration, not an open, creates the instance's counter, at no
    // opens yet. So a declared open never writes it from absent: an absent counter is refused
    // on a first open, a retry and a restart alike, and a manual delete can never hand N = 1
    // out twice. It goes before the formation record, so every formation record names a
    // counter; one written by a run that stopped before its formation record names an
    // instance no formation record will name, and nothing reads it.
    let counter = instance_key(&formation.instance);
    let revision = create_or_adopt(store, &counter, &encode_count(NO_OPENS)).await?;
    tracing::info!(
        key = %counter,
        revision = revision.0,
        "declare_counter_created"
    );
    let key = ControlKey::Operation(FORMATION_OPERATION).encode();
    let created = store
        .put(PutRequest {
            key: Bytes::from(key.clone()),
            value: formation.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        })
        .await;
    match created {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            tracing::info!(
                instance_hex = %instance_hex(&formation.instance),
                partitions,
                revision = response.revision,
                "declare_formation_written"
            );
            Ok(Declared::Created(formation))
        }
        // Another declaration's record landed first, or this one's reply was lost: read it.
        Ok(_) | Err(ConfigError::DeadlineExceededUnknownOutcome) => {
            match read_formation(&**store).await? {
                Some(found) if found == formation => Ok(Declared::Created(found)),
                Some(found) => same(found, read_nodes(&**store).await?),
                None => Err(BootstrapError::Control(format!(
                    "{key} is absent after a create that was not reported applied"
                ))
                .into()),
            }
        }
        Err(error) => Err(BootstrapError::Control(format!("put {key}: {error}")).into()),
    }
}

/// [`declare`]'s first write (PR #138 review): create the reservation under
/// [`RESERVATION_OPERATION`], or adopt it when this same declaration made it earlier. The
/// preflight is a read, so two declarations of different counts can both pass it; this one
/// create-only write is where exactly one of them goes on. The other is refused here, before
/// any write of its own.
///
/// # Errors
///
/// [`DeclareError::Refused`] naming the reserved count, [`DeclareError::Records`] or
/// [`DeclareError::Store`] when the store fails, [`DeclareError::Undecodable`] for a
/// reservation this build does not read.
async fn reserve(
    store: &Arc<dyn ConfigStore>,
    declaration: &Declaration,
    records: &[NodeRecord],
) -> Result<(), DeclareError> {
    let key = ControlKey::Operation(RESERVATION_OPERATION).encode();
    match create_or_adopt(store, &key, &declaration.encode_reservation(records)).await {
        Ok(revision) => {
            tracing::info!(
                key = %key,
                revision = revision.0,
                partitions = declaration.partitions,
                "declare_reserved"
            );
            Ok(())
        }
        Err(BootstrapError::AlreadyExists { .. }) => {
            let read = store
                .get(GetRequest {
                    key: Bytes::from(key.clone()),
                })
                .await
                .map_err(|error| DeclareError::Store {
                    step: format!("get {key}"),
                    error,
                })?;
            let held = read
                .record
                .and_then(|found| Declaration::decode_reservation(&found.value))
                .ok_or_else(|| DeclareError::Undecodable { key: key.clone() })?;
            tracing::info!(
                key = %key,
                reserved = held.partitions,
                asked = declaration.partitions,
                "declare_reservation_refused"
            );
            let Some(held_records) = held.records else {
                return Err(s1a_reservation_refused(&key, &held));
            };
            Err(DeclareError::Refused(format!(
                "{key} reserves the store for a declaration of {} partitions and nodes {}, and \
                 this declaration asks for {} partitions and nodes {}: only that declaration \
                 may go on; nothing was written",
                held.partitions,
                records_in_words(&held_records),
                declaration.partitions,
                records_in_words(records)
            )))
        }
        Err(error) => Err(error.into()),
    }
}

/// The refusal of a store an S1a build reserved (#80 S1b ruling 4): its reservation, layout 1,
/// names no node addresses, so the database is declared again on a new store.
fn s1a_reservation_refused(key: &str, held: &Reservation) -> DeclareError {
    DeclareError::Refused(format!(
        "{key} reserves the store for a declaration of {} partitions and nodes {:?} by a build \
         before #80 S1b, which wrote no node addresses: declare the database again, with \
         --peers, on a new empty control store; nothing was written",
        held.partitions,
        held.nodes.iter().map(|n| n.0).collect::<Vec<_>>(),
    ))
}

/// Before [`declare`] writes anything (PR #138 review): what is already in the store must be a
/// prefix of what this declaration writes, which it then adopts. It reads, in order: the
/// reservation, refused when an S1a build wrote it (#80 S1b); every `nodes/` record, whose
/// declared fields, decoded, must equal this declaration's for its node (node, address and
/// machine; a node's own app address, layout 2, is ignored on purpose, #83 B2, so a formed
/// database whose nodes registered one is still adopted); `routes/0`, which must be at this
/// count, if any; and the partition records, which must be untouched generation-0 records of
/// partitions 1 to `partitions` only. So a database formed some other way (a rooted
/// `partitions/1`), or an interrupted declaration of another count (its `routes/0`, or a record
/// past this count), refuses it with nothing written. This is a read: two declarations of different
/// counts **at once** can both pass it, and [`reserve`], the first write, is what lets only one of
/// them go on.
///
/// # Errors
///
/// [`DeclareError::Refused`] naming the record in the way, [`DeclareError::Store`] when the
/// store fails, [`DeclareError::Undecodable`] for a record this build does not read.
async fn declare_preflight(
    store: &dyn ConfigStore,
    records: &[NodeRecord],
    partitions: u32,
) -> Result<(), DeclareError> {
    let refused = |what: String| {
        DeclareError::Refused(format!(
            "{what}: the store holds part of another database or of another declaration, which \
             this declaration cannot adopt; nothing was written"
        ))
    };
    // #80 S1b ruling 4, PR #144 review: an S1a build's reservation first, so a store it left
    // half-written is refused with what to do, not by whichever of its records differs. A
    // layout-2 reservation is [`reserve`]'s to judge.
    let key = ControlKey::Operation(RESERVATION_OPERATION).encode();
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| DeclareError::Store {
            step: format!("get {key}"),
            error,
        })?;
    if let Some(found) = read.record {
        let held = Declaration::decode_reservation(&found.value)
            .ok_or_else(|| DeclareError::Undecodable { key: key.clone() })?;
        if held.records.is_none() {
            tracing::info!(key = %key, reserved = held.partitions, "declare_reservation_refused");
            return Err(s1a_reservation_refused(&key, &held));
        }
    }
    // #80 S1b: every `nodes/` record already there must hold the declared fields this
    // declaration writes. `read_nodes` decodes them, so this compares decoded fields, not
    // bytes: an app address a node wrote into its own record (layout 2, #83 B2) is ignored.
    // Read before `partitions/`, which stays the preflight's last read.
    for (node, held) in read_nodes(store).await? {
        let key = ControlKey::Node(node).encode();
        match records.iter().find(|record| record.node == node) {
            Some(record) if *record == held => {}
            Some(record) => {
                return Err(refused(format!(
                    "{key} records {}@{}/{} and this declaration says {}@{}/{}",
                    held.node.0,
                    held.addr,
                    held.machine,
                    record.node.0,
                    record.addr,
                    record.machine
                )));
            }
            None => {
                return Err(refused(format!(
                    "{key} exists and this declaration names nodes 1 to {}",
                    records.len()
                )));
            }
        }
    }
    let nodes: Vec<NodeId> = records.iter().map(|record| record.node).collect();
    let nodes = nodes.as_slice();
    let key = ControlKey::Route(RangeId(0)).encode();
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| DeclareError::Store {
            step: format!("get {key}"),
            error,
        })?;
    if let Some(found) = read.record {
        let stored = RoutesRoot::decode(&found.value)
            .ok_or_else(|| DeclareError::Undecodable { key: key.clone() })?
            .partition_count;
        if stored != partitions {
            return Err(refused(format!(
                "{key} says {stored} partitions and this declaration asks for {partitions}"
            )));
        }
    }
    let prefix = ControlPrefix::Partitions.encode();
    let listed = store
        .list(ListRequest {
            prefix: Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .map_err(|error| DeclareError::Store {
            step: format!("list {prefix}"),
            error,
        })?;
    if listed.truncated {
        return Err(DeclareError::Store {
            step: format!("list {prefix}"),
            error: ConfigError::Unavailable {
                reason: format!("truncated at revision {}", listed.read_revision),
            },
        });
    }
    for found in &listed.records {
        let key = String::from_utf8_lossy(&found.key).into_owned();
        let record = PartitionRecord::decode(&found.value)
            .ok_or_else(|| DeclareError::Undecodable { key: key.clone() })?;
        let partition = record.partition;
        if partition.0 > partitions {
            return Err(refused(format!(
                "{key} exists and this declaration asks for {partitions} partitions"
            )));
        }
        if found.value != initial_record(nodes, partition).encode()
            || found.create_revision != found.mod_revision
        {
            return Err(refused(format!(
                "{key} is not the untouched generation-0 record a declaration writes (revision \
                 {})",
                found.mod_revision
            )));
        }
    }
    Ok(())
}

/// The most opens one data directory may have (arch R3b): `N <= 2^23`, so a boot and a
/// request id derived from N never wrap, and every id a `Db` numbers its own writes with
/// (`(N - 1) << 40` and up) stays below 2^63, out of the range an app's ids take
/// (`app_request_id`, ruling A-R195).
pub const MAX_OPENS: u64 = 1 << 23;

/// The version byte of an open counter's value. Layout 2 (ruling A-R184): N, then the newest
/// generation of partition 1 a host marked activated, each a little-endian u64. A build from
/// before A-R184 reads layout 2 as `Unreadable`, never as a count.
const COUNTER_LAYOUT: u8 = 2;

/// Layout 1, written before A-R184: N alone. Read with the activated generation unknown.
const COUNTER_LAYOUT_N_ONLY: u8 = 1;

/// The activated generation a layout-1 counter leaves unknown: no generation is ever rolled
/// forward on it, and the next activation replaces it.
pub const ACTIVATED_UNKNOWN: Generation = Generation(u64::MAX);

/// The counter [`declare`] creates (PR #138 review r5): no opens yet, nothing activated. The
/// first open's CAS makes it N = 1.
const NO_OPENS: Count = Count {
    n: 0,
    activated: Generation(0),
};

/// An open counter's value (arch R3b, rulings A-R160 and A-R184).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count {
    /// How many opens were counted: N.
    pub n: u64,
    /// The newest generation of partition 1 a host marked activated before its first acquire
    /// under it ([`activate`]): `Generation(0)` before any, [`ACTIVATED_UNKNOWN`] on a layout-1
    /// value.
    pub activated: Generation,
}

impl Count {
    /// Whether `generation` may have admitted a write: always, unless the marker proves that no
    /// host ever activated it.
    #[must_use]
    pub fn may_have_admitted(self, generation: Generation) -> bool {
        self.activated == ACTIVATED_UNKNOWN || self.activated >= generation
    }
}

/// How long [`count_open`] keeps reading and CASing before it gives up.
pub(crate) const COUNT_WITHIN: Duration = Duration::from_secs(10);

/// An instance id as 32 lowercase hex digits: how logs and keys name it.
#[must_use]
pub fn instance_hex(instance: &[u8; INSTANCE_LEN]) -> String {
    instance.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The key of `instance`'s open counter (M10, arch R3b, ruling A-R160).
#[must_use]
pub fn instance_key(instance: &[u8; INSTANCE_LEN]) -> String {
    format!("rdb/instances/{}", instance_hex(instance))
}

fn encode_count(count: Count) -> Bytes {
    let mut out = Vec::with_capacity(17);
    out.push(COUNTER_LAYOUT);
    out.extend_from_slice(&count.n.to_le_bytes());
    out.extend_from_slice(&count.activated.0.to_le_bytes());
    Bytes::from(out)
}

fn decode_count(body: &[u8]) -> Option<Count> {
    match body {
        [COUNTER_LAYOUT, rest @ ..] if rest.len() == 16 => Some(Count {
            n: u64::from_le_bytes(rest[..8].try_into().ok()?),
            activated: Generation(u64::from_le_bytes(rest[8..].try_into().ok()?)),
        }),
        [COUNTER_LAYOUT_N_ONLY, rest @ ..] => Some(Count {
            n: u64::from_le_bytes(rest.try_into().ok()?),
            activated: ACTIVATED_UNKNOWN,
        }),
        _ => None,
    }
}

/// Why [`count_open`] did not count an open.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CounterError {
    /// The copies name an instance whose counter the store does not hold: the store is not the
    /// one this directory was opened against.
    #[error("{key} is absent: this directory was not opened against this control store")]
    Absent {
        /// The counter's key.
        key: String,
    },
    /// The directory has been opened [`MAX_OPENS`] times.
    #[error("{}", crate::db::error::counter_exhausted_text(.key, MAX_OPENS))]
    Exhausted {
        /// The counter's key.
        key: String,
    },
    /// The counter's value is not one this build writes.
    #[error("{}", crate::db::error::counter_unreadable_text(.key, *.revision))]
    Unreadable {
        /// The counter's key.
        key: String,
        /// Where it was read.
        revision: u64,
    },
    /// A roll-forward's count found the counter moved from the revision its decision read
    /// ([`count_open_at`]): another open counted in between, so the decision no longer holds.
    #[error("{key} moved from revision {read} that the roll-forward decision read")]
    Moved {
        /// The counter's key.
        key: String,
        /// The revision the decision read.
        read: u64,
    },
    /// The store refused, or did not settle the count within [`COUNT_WITHIN`].
    #[error("control store: {0}")]
    Control(String),
}

/// Count one more open of `instance` and return its number N (arch R3b): N = 1 by a create-only
/// write when `fresh` finds no counter, otherwise N + 1 by a CAS at the revision just read. Once
/// any read has found the counter, or this open's create has conflicted with another's, `fresh`
/// no longer applies: a counter gone on a later read is [`CounterError::Absent`], never created
/// again. A declared database's counter is created by [`declare`], and its opens count with
/// `fresh` false. A
/// conflict or a lost reply reads again and CASes again, so a lost reply can waste a number but
/// never hand one out twice. A transient store error waits 50 ms and tries again; all of it is
/// bounded by `within`. Logged as `open_counted{instance_hex, n, revision}` and each retry as
/// `open_count_retry{instance_hex, error}`.
///
/// # Errors
///
/// [`CounterError`].
pub async fn count_open(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
    fresh: bool,
    within: Duration,
) -> Result<u64, CounterError> {
    let key = instance_key(instance);
    let hex = instance_hex(instance);
    let deadline = tokio::time::Instant::now() + within;
    let mut last = String::from("nothing answered");
    // Whether this open may still create the counter: only until a read finds one. A counter
    // that was seen and is then gone was deleted, and a new N = 1 would overlap the request
    // range of the open that holds 1 (PR #138 review r4).
    let mut may_create = fresh;
    while tokio::time::Instant::now() < deadline {
        let read = match store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
        {
            Ok(read) => read,
            Err(error) if crate::control::transient(&error) => {
                last = error.to_string();
                tracing::warn!(instance_hex = %hex, error = %error, "open_count_retry");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(error) => return Err(CounterError::Control(format!("get {key}: {error}"))),
        };
        if read.record.is_some() {
            may_create = false;
        }
        let (next, expected) = match read.record {
            None if may_create => (
                Count {
                    n: 1,
                    activated: Generation(0),
                },
                0,
            ),
            None => return Err(CounterError::Absent { key }),
            Some(found) => {
                let count = decode_count(&found.value).ok_or_else(|| CounterError::Unreadable {
                    key: key.clone(),
                    revision: found.mod_revision,
                })?;
                if count.n >= MAX_OPENS {
                    return Err(CounterError::Exhausted { key });
                }
                (
                    Count {
                        n: count.n + 1,
                        ..count
                    },
                    found.mod_revision,
                )
            }
        };
        let written = store
            .put(PutRequest {
                key: Bytes::from(key.clone()),
                value: encode_count(next),
                expected_mod_revision: Some(expected),
                dedup: None,
            })
            .await;
        match written {
            Ok(response) if response.outcome == MutationOutcome::Applied => {
                tracing::info!(
                    instance_hex = %hex,
                    n = next.n,
                    activated = next.activated.0,
                    revision = response.revision,
                    "open_counted"
                );
                return Ok(next.n);
            }
            // A create that conflicts proves another open created the counter (PR #138 review
            // r5): this open may no longer create it, so a counter gone on the next read was
            // deleted. A lost reply proves nothing, and the next read finds this open's own
            // create if it landed.
            Ok(_) => {
                may_create = false;
                last = format!("CAS at {expected} was not applied");
            }
            Err(error @ ConfigError::Conflict { .. }) => {
                may_create = false;
                last = error.to_string();
            }
            Err(error)
                if matches!(error, ConfigError::DeadlineExceededUnknownOutcome)
                    || crate::control::transient(&error) =>
            {
                last = error.to_string();
            }
            Err(error) => return Err(CounterError::Control(format!("put {key}: {error}"))),
        }
        tracing::warn!(instance_hex = %hex, error = %last, "open_count_retry");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(CounterError::Control(format!(
        "{key} was not counted within {} ms; last: {last}",
        within.as_millis()
    )))
}

/// `instance`'s open counter and the revision it was read at, for a reopen's roll-forward
/// decision (ruling A-R184). One read, no retry, as the reopen's read of `partitions/1`.
///
/// # Errors
///
/// [`CounterError::Absent`], [`CounterError::Unreadable`], or [`CounterError::Control`] when the
/// read fails.
pub async fn read_count(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
) -> Result<(Count, u64), CounterError> {
    let key = instance_key(instance);
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| CounterError::Control(format!("get {key}: {error}")))?;
    let found = read
        .record
        .ok_or_else(|| CounterError::Absent { key: key.clone() })?;
    let count = decode_count(&found.value).ok_or(CounterError::Unreadable {
        key,
        revision: found.mod_revision,
    })?;
    Ok((count, found.mod_revision))
}

/// Count one more open of `instance` on a roll-forward (ruling A-R184, k199): a CAS from `read`
/// at exactly the `revision` the decision read it at, never from a newer value. A conflict means
/// another open counted since, so the decision no longer holds: [`CounterError::Moved`]. A lost
/// reply reads again: the counter still at `revision` sends the same CAS again, this CAS's own
/// bytes there count as applied, and anything else is [`CounterError::Moved`]. Bounded by
/// `within`. Logged as `open_counted{instance_hex, n, activated, revision, rolled_forward}`.
///
/// # Errors
///
/// [`CounterError::Moved`], [`CounterError::Exhausted`], or [`CounterError::Control`] when the
/// store refuses or does not settle within `within`.
pub async fn count_open_at(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
    read: Count,
    revision: u64,
    within: Duration,
) -> Result<u64, CounterError> {
    let key = instance_key(instance);
    let hex = instance_hex(instance);
    if read.n >= MAX_OPENS {
        return Err(CounterError::Exhausted { key });
    }
    let next = Count {
        n: read.n + 1,
        ..read
    };
    let counted = |at: u64| {
        tracing::info!(
            instance_hex = %hex,
            n = next.n,
            activated = next.activated.0,
            revision = at,
            rolled_forward = true,
            "open_counted"
        );
        Ok(next.n)
    };
    let deadline = tokio::time::Instant::now() + within;
    let mut last = String::from("nothing answered");
    let mut send = true;
    while tokio::time::Instant::now() < deadline {
        if send {
            let written = store
                .put(PutRequest {
                    key: Bytes::from(key.clone()),
                    value: encode_count(next),
                    expected_mod_revision: Some(revision),
                    dedup: None,
                })
                .await;
            match written {
                Ok(response) if response.outcome == MutationOutcome::Applied => {
                    return counted(response.revision)
                }
                Ok(_) | Err(ConfigError::Conflict { .. }) => {
                    return Err(CounterError::Moved {
                        key,
                        read: revision,
                    })
                }
                Err(error)
                    if matches!(error, ConfigError::DeadlineExceededUnknownOutcome)
                        || crate::control::transient(&error) =>
                {
                    last = error.to_string();
                }
                Err(error) => return Err(CounterError::Control(format!("put {key}: {error}"))),
            }
        }
        tracing::warn!(instance_hex = %hex, error = %last, "open_count_retry");
        tokio::time::sleep(Duration::from_millis(50)).await;
        match store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
        {
            Ok(read) => match read.record {
                Some(found) if found.mod_revision == revision => send = true,
                Some(found) if decode_count(&found.value) == Some(next) => {
                    return counted(found.mod_revision)
                }
                _ => {
                    return Err(CounterError::Moved {
                        key,
                        read: revision,
                    })
                }
            },
            Err(error) if crate::control::transient(&error) => {
                last = error.to_string();
                send = false;
            }
            Err(error) => return Err(CounterError::Control(format!("get {key}: {error}"))),
        }
    }
    Err(CounterError::Control(format!(
        "{key} was not counted within {} ms; last: {last}",
        within.as_millis()
    )))
}

/// Mark `generation` of partition 1 activated in `instance`'s open counter (ruling A-R184,
/// critic-rollfwd F2). A host calls it before its first acquire under `generation` and acquires
/// only once it returns, so a generation the counter does not name never admitted a write. It
/// reads the counter and CASes `activated = generation`, keeping N, at the revision read. A
/// conflict, a lost reply or a store error reads again, so it returns only once the counter names
/// `generation` or newer, and it never gives up on its own: the node's acquire waits on it, and
/// a node that stops abandons it. Logged as `open_activated{instance_hex, n, generation,
/// revision}`, each retry as `open_activate_retry{instance_hex, generation, error}`.
///
/// # Errors
///
/// [`CounterError::Absent`] or [`CounterError::Unreadable`]: no read again changes them.
pub async fn activate(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
    generation: Generation,
) -> Result<Count, CounterError> {
    let key = instance_key(instance);
    let hex = instance_hex(instance);
    let activated = |count: Count, revision: u64| {
        tracing::info!(
            instance_hex = %hex,
            n = count.n,
            generation = generation.0,
            activated = count.activated.0,
            revision,
            "open_activated"
        );
        Ok(count)
    };
    let mut wait = Duration::from_millis(50);
    loop {
        let error = match store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
        {
            Ok(read) => {
                let Some(found) = read.record else {
                    return Err(CounterError::Absent { key });
                };
                let count = decode_count(&found.value).ok_or_else(|| CounterError::Unreadable {
                    key: key.clone(),
                    revision: found.mod_revision,
                })?;
                if count.activated != ACTIVATED_UNKNOWN && count.activated >= generation {
                    return activated(count, found.mod_revision);
                }
                let next = Count {
                    activated: generation,
                    ..count
                };
                let written = store
                    .put(PutRequest {
                        key: Bytes::from(key.clone()),
                        value: encode_count(next),
                        expected_mod_revision: Some(found.mod_revision),
                        dedup: None,
                    })
                    .await;
                match written {
                    Ok(response) if response.outcome == MutationOutcome::Applied => {
                        return activated(next, response.revision)
                    }
                    Ok(_) => format!("CAS at {} was not applied", found.mod_revision),
                    Err(error) => error.to_string(),
                }
            }
            Err(error) => error.to_string(),
        };
        tracing::warn!(
            instance_hex = %hex,
            generation = generation.0,
            error = %error,
            "open_activate_retry"
        );
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(Duration::from_secs(1));
    }
}

/// The version byte of a partition's activation mark.
const ACTIVATED_LAYOUT: u8 = 1;

/// The key of `partition`'s activation mark (issue #79). Partition 1's lives in the open counter
/// ([`Count::activated`]); a later partition's is its own key.
#[must_use]
pub fn activated_key(instance: &[u8; INSTANCE_LEN], partition: PartitionId) -> String {
    format!("{}/activated/{}", instance_key(instance), partition.0)
}

fn encode_activated(generation: Generation) -> Bytes {
    let mut out = Vec::with_capacity(9);
    out.push(ACTIVATED_LAYOUT);
    out.extend_from_slice(&generation.0.to_le_bytes());
    Bytes::from(out)
}

fn decode_activated(body: &[u8]) -> Option<Generation> {
    match body {
        [ACTIVATED_LAYOUT, rest @ ..] => {
            Some(Generation(u64::from_le_bytes(rest.try_into().ok()?)))
        }
        _ => None,
    }
}

/// The newest generation of `partition` (above 1) a host marked activated, and the revision of
/// the mark: `Generation(0)` at revision 0 when no host has marked one.
///
/// # Errors
///
/// [`CounterError::Unreadable`], or [`CounterError::Control`] when the read fails.
pub async fn read_activated(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
    partition: PartitionId,
) -> Result<(Generation, u64), CounterError> {
    let key = activated_key(instance, partition);
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| CounterError::Control(format!("get {key}: {error}")))?;
    let Some(found) = read.record else {
        return Ok((Generation(0), 0));
    };
    let generation = decode_activated(&found.value).ok_or(CounterError::Unreadable {
        key,
        revision: found.mod_revision,
    })?;
    Ok((generation, found.mod_revision))
}

/// A roll-forward of `partition` (above 1) claims its decision (ruling A-R184, issue #79): a CAS
/// of the mark at exactly the `revision` the decision read, with the value it read. Partition 1
/// gets this from [`count_open_at`]; a later partition's mark is its own key, so a host that
/// marks the generation after the decision read must lose to this CAS or win against it.
///
/// # Errors
///
/// [`CounterError::Moved`] when the mark moved, or [`CounterError::Control`] when the store
/// refuses or does not answer: nothing was linked, so opening again decides again.
pub async fn claim_roll(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
    partition: PartitionId,
    revision: u64,
    activated: Generation,
) -> Result<(), CounterError> {
    let key = activated_key(instance, partition);
    let written = store
        .put(PutRequest {
            key: Bytes::from(key.clone()),
            value: encode_activated(activated),
            expected_mod_revision: Some(revision),
            dedup: None,
        })
        .await;
    match written {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            tracing::info!(key = %key, revision = response.revision, "open_roll_claimed");
            Ok(())
        }
        Ok(_) | Err(ConfigError::Conflict { .. }) => Err(CounterError::Moved {
            key,
            read: revision,
        }),
        Err(error) => Err(CounterError::Control(format!("put {key}: {error}"))),
    }
}

/// [`activate`] for `partition`: partition 1 in the open counter, a later one in its own mark
/// (issue #79). Retries as [`activate`] does, and logs the same events with `partition`.
///
/// # Errors
///
/// [`CounterError::Absent`] or [`CounterError::Unreadable`]: no read again changes them.
pub async fn activate_in(
    store: &dyn ConfigStore,
    instance: &[u8; INSTANCE_LEN],
    partition: PartitionId,
    generation: Generation,
) -> Result<(), CounterError> {
    if partition == PARTITION {
        return activate(store, instance, generation).await.map(|_| ());
    }
    let key = activated_key(instance, partition);
    let mut wait = Duration::from_millis(50);
    loop {
        let error = match store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
        {
            Ok(read) => {
                let (held, revision) = match &read.record {
                    None => (Generation(0), 0),
                    Some(found) => (
                        decode_activated(&found.value).ok_or_else(|| CounterError::Unreadable {
                            key: key.clone(),
                            revision: found.mod_revision,
                        })?,
                        found.mod_revision,
                    ),
                };
                if read.record.is_some() && held >= generation {
                    return Ok(());
                }
                let written = store
                    .put(PutRequest {
                        key: Bytes::from(key.clone()),
                        value: encode_activated(generation),
                        expected_mod_revision: Some(revision),
                        dedup: None,
                    })
                    .await;
                match written {
                    Ok(response) if response.outcome == MutationOutcome::Applied => {
                        tracing::info!(
                            partition = partition.0,
                            generation = generation.0,
                            revision = response.revision,
                            "open_activated"
                        );
                        return Ok(());
                    }
                    Ok(_) => format!("CAS at {revision} was not applied"),
                    Err(error) => error.to_string(),
                }
            }
            Err(error) => error.to_string(),
        };
        tracing::warn!(
            partition = partition.0,
            generation = generation.0,
            error = %error,
            "open_activate_retry"
        );
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(Duration::from_secs(1));
    }
}

/// What a reopen's heal proved and handed node 1 (M10, arch R3a, ruling A-R163).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Healed {
    /// How node 1's old grant was found gone: deleted by this heal (case A), or absent (case B).
    pub cleared: GrantCleared,
    /// The store revision `partitions/1` was then read at: never below [`Self::cleared`]'s.
    pub read_revision: Revision,
    /// The fence's `control_revision`: `partitions/1`'s `mod_revision` from that read.
    pub control_revision: Revision,
}

/// How a reopen's heal found node 1's old grant gone (M10, arch R3a).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantCleared {
    /// Case A: the heal deleted it, at this revision ([`Revocation::ExpiryProven`]).
    Deleted(Revision),
    /// Case B: a read at this store revision found it absent ([`Revocation::NoGrantHeld`]).
    Absent(Revision),
}

impl GrantCleared {
    /// The revision `partitions/1` must be read at or after.
    #[must_use]
    pub const fn revision(self) -> Revision {
        match self {
            Self::Deleted(revision) | Self::Absent(revision) => revision,
        }
    }
}

/// Why a reopen's heal did not hand node 1 a fence.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HealError {
    /// A grant was still not clearable when the heal's time ran out.
    #[error(
        "the last run's grant for node {node} (grants/{node}) was still held after {secs} \
         seconds of trying: {last}",
        secs = seconds_in_words(*.within_ms)
    )]
    NotCleared {
        /// The node.
        node: u32,
        /// The heal's bound.
        within_ms: u128,
        /// Why the last answer kept the grant, and the next step, in words (A-R191b).
        last: String,
    },
    /// The grant is frozen for a takeover that has not finished. Only a promote settles it, so
    /// the heal stops at its first look instead of waiting out its time (A-R266 follow-up).
    #[error("the last run's grant for node {node} (grants/{node}) is frozen: {step}")]
    Frozen {
        /// The node.
        node: u32,
        /// The next step, in words.
        step: String,
    },
    /// The `partitions/1` read was served before the grant delete it must follow.
    #[error("partitions/1 was read at revision {read}, before the grant delete at {deleted}")]
    ReadBeforeDelete {
        /// The read's store revision.
        read: u64,
        /// The delete's revision.
        deleted: u64,
    },
    /// The `partitions/1` read was served before the read that found `grants/1` absent.
    #[error(
        "partitions/1 was read at revision {read}, before grants/1 was read absent at {absent}"
    )]
    ReadBeforeAbsence {
        /// The read's store revision.
        read: u64,
        /// The absent read's store revision.
        absent: u64,
    },
    /// `partitions/1` is not a layout-3 record.
    #[error("partitions/1: {0}")]
    Record(String),
    /// The store refused.
    #[error("control store: {0}")]
    Control(String),
    /// The owner's thread is gone.
    #[error("node {} stopped before the heal reached it", .node.0)]
    NodeStopped {
        /// The owner the heal was handing the plan to.
        node: NodeId,
    },
    /// No handshake from these nodes within the heal's time, so the plan cannot pin their boots.
    #[error(
        "no link to node {who} came up within {secs} seconds, so the heal cannot pin its \
         boot: start node {who}, then open again",
        who = names_in_words(.nodes),
        secs = seconds_in_words(*.within_ms)
    )]
    PeersUnknown {
        /// The nodes with no boot yet.
        nodes: Vec<u32>,
        /// The heal's bound.
        within_ms: u128,
    },
}

/// What a reopen's heal runs with: it runs as a task, after `Db::open` has returned.
pub(crate) struct HealCtx {
    pub(crate) store: Arc<dyn ConfigStore>,
    /// The boot this open started every node at.
    pub(crate) boot: BootId,
    pub(crate) budgets: Budgets,
    pub(crate) clock: HostClock,
    /// The owner's mailbox.
    pub(crate) owner: std::sync::mpsc::Sender<Msg>,
    /// The node `partitions/1` named owner when the open read it, and that this process runs
    /// (arch-s2 B1, B2). The heal clears its grant alone and plans it primary; after a promote
    /// it is not node 1, and node 1's frozen grant stays as the promote left it.
    pub(crate) owner_node: NodeId,
    /// The partitions that node owns, all healed by this one heal (issue #79): a heal each would
    /// meet the grant the first partition's recovery had already acquired again.
    pub(crate) partitions: Vec<PartitionId>,
    /// How many partitions the database has: what a frozen grant's next step says depends on it.
    pub(crate) partition_count: u32,
    /// The nodes partitions are placed over (#80 S1b): the formation record's, or nodes 1 to 3
    /// for a database no declaration made. A partition still to form pins [`placed`] over them.
    pub(crate) nodes: Arc<[NodeId]>,
    /// The partitions of that node still to form, each with its record's revision (design 7.2):
    /// their plans go out after the clear, so a bootstrap F1 cannot acquire before it.
    pub(crate) bootstrap: Vec<(PartitionId, Revision)>,
    /// How long clearing the old grants may take.
    pub(crate) within: Duration,
    /// The boot each member runs at: this open's for a node this process runs, the one its
    /// last handshake reported for another ([`crate::transport::Links::boot_of`]); `None`
    /// until that node's link has come up (arch-s2 B1).
    pub(crate) member_boot: Arc<dyn Fn(NodeId) -> Option<BootId> + Send + Sync>,
}

/// The correlation a heal's two events carry.
const HEAL: CorrelationId = CorrelationId(2);

/// Heal partition 1 after a reopen (M10, arch R3a, rulings A-R159, A-R163): clear the old grant
/// of the owner `partitions/1` names, and no other (arch-s2 B1); only after it is known gone,
/// read `partitions/1` and check it still names that owner (B2); then hand the owner's F1 a
/// `Plan` from the record's root at config version + 1 with this open's boots, the owner
/// primary, and a takeover authorization whose `control_revision` is the record's `mod_revision`.
/// The same proof `bootstrap` and the simulator's `reinspect_now` already use: a CAS at that
/// revision lands only if nothing wrote `partitions/1` after the read.
///
/// Node 1's grant is gone one of two ways:
/// - **Case A**, deleted by this heal under `clear_verdict`'s guards:
///   [`Revocation::ExpiryProven`] from the record it judged, `prior_grant_id` that record's
///   grant. A record holding `GrantId(0)` is refused before the delete: A1 never issues 0.
/// - **Case B**, read absent (an earlier heal deleted it and its process died before an
///   acquire): [`Revocation::NoGrantHeld`] at that read's revision, `prior_grant_id`
///   `GrantId(0)` ("none held").
///
/// Either way `prior_boot_id` is the record's `owner_boot`, the owner the F1 commit pinned,
/// never 0 (R3a; R5 row 8).
///
/// Logged as `heal_takeover_authorized{variant, control_revision, prior_boot, prior_grant,
/// grant_delete_revision | absent_revision, read_revision}`.
///
/// # Errors
///
/// [`HealError`]. Nothing is sent to node 1 unless every step held.
pub(crate) async fn heal(ctx: HealCtx) -> Result<Healed, HealError> {
    let deadline = tokio::time::Instant::now() + ctx.within;
    // M11 T6: the members are the ones `partitions/1` pins, not nodes 1 to 3. A move may have
    // put a copy on node 4; re-pinning node 3 would recover from a copy the move left behind.
    let pinned = pinned_members(&ctx).await?;
    // A partition still to form pins its formed members (#80 S1a): in a node process they run
    // in other processes, at the boots their handshakes report, not at this open's.
    let forming: Vec<Vec<RecordMember>> = ctx
        .bootstrap
        .iter()
        .map(|(partition, _)| {
            placed(&ctx.nodes, *partition, BOOT)
                .iter()
                .map(RecordMember::of)
                .collect()
        })
        .collect();
    // Every member's boot first (arch-s2 B1): the plan pins them, and waiting here leaves the
    // fence's reads and decision tick as fresh as they were. One union over the partitions.
    let union = member_union(&[pinned.as_slice(), forming.as_slice()].concat());
    let boots = member_boots(&ctx, &union, deadline).await?;
    // The owner's grant alone (arch-s2 B1): another node's is either absent, or frozen by the
    // promote that moved the partition off it, which only that promote may settle. Once, for
    // every partition this node owns (issue #79).
    let gone = clear_until(&ctx, ctx.owner_node, deadline).await?;
    // Only now, after the owner's delete was acknowledged or its absence read (A-R163).
    let (read_revision, records) = heal_records(&ctx).await?;
    let cleared = match &gone {
        Gone::Deleted(revision, _) => GrantCleared::Deleted(*revision),
        Gone::Absent(revision) => GrantCleared::Absent(*revision),
    };
    if Revision(read_revision) < cleared.revision() {
        return Err(match cleared {
            GrantCleared::Deleted(deleted) => HealError::ReadBeforeDelete {
                read: read_revision,
                deleted: deleted.0,
            },
            GrantCleared::Absent(absent) => HealError::ReadBeforeAbsence {
                read: read_revision,
                absent: absent.0,
            },
        });
    }
    let read = HealRead {
        read_revision,
        cleared,
        now: ctx.clock.now(),
    };
    // Every record is judged before any plan goes out, so a heal that fails here has sent
    // nothing and `heal_retrying`'s premise holds (issue #79, design 7.1).
    let mut planned = Vec::with_capacity(records.len());
    for ((partition, found), was) in ctx.partitions.iter().copied().zip(&records).zip(&pinned) {
        planned.push(plan_heal(
            &ctx, partition, found, was, &boots, &gone, &read,
        )?);
    }
    // An owner with only partitions to form has no record to report on.
    let healed = planned.first().map_or(
        Healed {
            cleared,
            read_revision: Revision(read_revision),
            control_revision: Revision(read_revision),
        },
        |plan| plan.0,
    );
    for (_, partition, events) in planned {
        for event in events {
            ctx.owner
                .send(Msg::Inject {
                    partition,
                    correlation: HEAL,
                    kind: EventKind::Kernel(KernelEvent::Recovery(event)),
                })
                .map_err(|_| HealError::NodeStopped {
                    node: ctx.owner_node,
                })?;
        }
    }
    for ((partition, revision), was) in ctx.bootstrap.iter().copied().zip(&forming) {
        let send = |_, msg| ctx.owner.send(msg).map_err(|_| NodeStopped(ctx.owner_node));
        let members = members_at_boots(was, &boots);
        start_recovery(
            ctx.clock.now(),
            ctx.boot,
            partition,
            revision,
            members,
            &send,
        )
        .map_err(|_| HealError::NodeStopped {
            node: ctx.owner_node,
        })?;
    }
    Ok(healed)
}

/// What one heal's reads settled for every partition it plans.
struct HealRead {
    read_revision: u64,
    cleared: GrantCleared,
    now: Tick,
}

/// One partition of a heal: its record judged, and its plan and fence built, nothing sent.
fn plan_heal(
    ctx: &HealCtx,
    partition: PartitionId,
    found: &config_core::Record,
    was: &[RecordMember],
    boots: &[Member],
    gone: &Gone,
    read: &HealRead,
) -> Result<(Healed, PartitionId, [RecoveryEvent; 2]), HealError> {
    let name = partition.0;
    let (Some(record), Some(root), Some(prior)) = (
        PartitionRecord::decode(&found.value),
        PartitionRoot::decode(&found.value),
        prior_of(&found.value),
    ) else {
        return Err(HealError::Record(format!(
            "the record of partition {name} at revision {} carries no root (layout 3)",
            found.mod_revision
        )));
    };
    // T6: the plan pins the members read before the boots were awaited; a record that changed
    // them since is not the one the plan describes.
    if pinned_of(partition, &found.value)?.is_some_and(|now| now != was) {
        return Err(HealError::Record(format!(
            "the members partitions/{name} pins changed at revision {} while this open waited for their nodes: open again",
            found.mod_revision
        )));
    }
    // B2: the grant cleared above must be the owner's this record names.
    if record.owner != ctx.owner_node {
        return Err(HealError::Record(format!(
            "the record of partition {name} at revision {} names owner node {}, but this open \
             found node {}: open again",
            found.mod_revision, record.owner.0, ctx.owner_node.0
        )));
    }
    let healed = Healed {
        cleared: read.cleared,
        read_revision: Revision(read.read_revision),
        control_revision: Revision(found.mod_revision),
    };
    // R3a, both cases: the prior boot is the owner the F1 commit pinned (R5 row 8: never 0).
    if root.owner_boot == BootId(0) {
        return Err(HealError::Record(format!(
            "the record of partition {name} at revision {} names owner boot 0",
            found.mod_revision
        )));
    }
    let prior_boot = root.owner_boot;
    let now = read.now;
    // The plan's view: case A carries the old grant, as before; case B has none, so it takes
    // bootstrap's. A1 replaces the view at its first acquire either way.
    let (prior_grant, view_grant, view_generation, revocation) = match gone {
        Gone::Deleted(_, held) => {
            let sample = ctx.clock.sample(now, &ctx.budgets);
            let revocation = Revocation::ExpiryProven {
                frozen_expiry_utc_ms: held.expiry_utc_ms,
                authority_utc_ms: i64::try_from(sample.estimate.0).unwrap_or(i64::MAX),
                authority_tick: now,
                epsilon_ms: u32::try_from(ctx.budgets.clock_error_millis).unwrap_or(u32::MAX),
                delta_ms: u32::try_from(ctx.budgets.dispatch_margin_millis).unwrap_or(u32::MAX),
            };
            (
                held.grant,
                held.grant,
                held.authority_generation,
                revocation,
            )
        }
        Gone::Absent(absent_revision) => {
            let revocation = Revocation::NoGrantHeld {
                absent_revision: *absent_revision,
            };
            (GrantId(0), GrantId(1), AuthorityGeneration(1), revocation)
        }
    };
    let lineage = Lineage {
        partition,
        generation: record.generation,
        owner_epoch: record.owner_epoch,
    };
    let anchor = LineageAnchor {
        lineage,
        base_seq: root.base_seq,
        base_digest: root.base_digest,
    };
    let version = ConfigVersion(record.config_version.0 + 1);
    let plan = recovery_plan(
        anchor,
        version,
        members_at_boots(was, boots),
        ctx.boot,
        view_grant,
        view_generation,
        prior,
    );
    let fence = FencingProof {
        partition,
        prior_generation: record.generation,
        prior_owner_epoch: record.owner_epoch,
        prior_grant_id: prior_grant,
        prior_boot_id: prior_boot,
        revocation,
        control_revision: healed.control_revision,
        decision_tick: now,
    };
    match read.cleared {
        GrantCleared::Deleted(deleted) => tracing::info!(
            partition = name,
            variant = "ExpiryProven",
            control_revision = healed.control_revision.0,
            prior_boot = prior_boot.0,
            prior_grant = prior_grant.0,
            grant_delete_revision = deleted.0,
            read_revision = read.read_revision,
            "heal_takeover_authorized"
        ),
        GrantCleared::Absent(absent) => tracing::info!(
            partition = name,
            variant = "NoGrantHeld",
            control_revision = healed.control_revision.0,
            prior_boot = prior_boot.0,
            prior_grant = prior_grant.0,
            absent_revision = absent.0,
            read_revision = read.read_revision,
            "heal_takeover_authorized"
        ),
    }
    Ok((
        healed,
        partition,
        [
            RecoveryEvent::Plan(Box::new(plan)),
            RecoveryEvent::FenceProven(Box::new(fence)),
        ],
    ))
}

/// Every node `pinned` names once, in first-seen order. A node that is a `Shadow` in one
/// partition and a regular copy in another counts as regular: the heal must wait for it.
fn member_union(pinned: &[Vec<RecordMember>]) -> Vec<RecordMember> {
    let mut union: Vec<RecordMember> = Vec::new();
    for member in pinned.iter().flatten() {
        match union.iter_mut().find(|seen| seen.node == member.node) {
            Some(seen) if seen.role == ReplicaRole::Shadow => *seen = *member,
            Some(_) => {}
            None => union.push(*member),
        }
    }
    union
}

/// `was`, in record order, each at the boot `boots` names for its node; a member `boots` left
/// out (a shadow whose node is unknown) stays out.
fn members_at_boots(was: &[RecordMember], boots: &[Member]) -> Vec<Member> {
    was.iter()
        .filter_map(|member| {
            let boot = boots.iter().find(|known| known.node == member.node)?.boot;
            Some(member.at(boot))
        })
        .collect()
}

/// The records of `ctx.partitions`, in that order, and the store revision they were read at. One
/// read for all of them: a `Get` for partition 1 alone, otherwise one `list` of `partitions/`
/// (issue #79). A failed or truncated list is never read as a complete one.
async fn heal_records(ctx: &HealCtx) -> Result<(u64, Vec<config_core::Record>), HealError> {
    if ctx.partitions == [PARTITION] {
        let read = ctx
            .store
            .get(GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            })
            .await
            .map_err(|error| HealError::Control(format!("get partitions/1: {error}")))?;
        let found = read
            .record
            .ok_or_else(|| HealError::Record("absent".to_owned()))?;
        return Ok((read.read_revision, vec![found]));
    }
    let prefix = ControlPrefix::Partitions.encode();
    let listed = ctx
        .store
        .list(ListRequest {
            prefix: Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .map_err(|error| HealError::Control(format!("list {prefix}: {error}")))?;
    if listed.truncated {
        return Err(HealError::Control(format!(
            "list {prefix} came back truncated at revision {}",
            listed.read_revision
        )));
    }
    let mut records = Vec::with_capacity(ctx.partitions.len());
    for partition in &ctx.partitions {
        let key = ControlKey::Partition(*partition).encode();
        let found = listed
            .records
            .iter()
            .find(|record| record.key.as_ref() == key.as_bytes())
            .cloned()
            .ok_or_else(|| HealError::Record(format!("{key} is absent")))?;
        records.push(found);
    }
    Ok((listed.read_revision, records))
}

/// The first wait between two heal attempts that found a peer's boot unknown.
const HEAL_RETRY_FIRST: Duration = Duration::from_millis(500);
/// The longest wait between two such attempts.
const HEAL_RETRY_MAX: Duration = Duration::from_secs(5);

/// [`heal`], again and again while it ends [`HealError::PeersUnknown`], until it ends any other
/// way; the open's shutdown aborts the task that runs it (tester-s2 `lr up` race, ruling
/// A-R271). On a slow host a peer's process can start after the first window has passed, and
/// one attempt left the owner dead until it was opened again. Waiting for boots is the heal's
/// first step and has done nothing yet, so trying again is safe. Any other error ends the heal
/// as before. Each miss logs `heal_retry{attempt, missing, error, next_ms}`; the wait doubles
/// from 0.5 s to 5 s.
pub(crate) async fn heal_retrying(ctx: HealCtx) -> Result<Healed, HealError> {
    let mut backoff = HEAL_RETRY_FIRST;
    let mut attempt: u64 = 0;
    loop {
        attempt += 1;
        let again = HealCtx {
            store: Arc::clone(&ctx.store),
            owner: ctx.owner.clone(),
            member_boot: Arc::clone(&ctx.member_boot),
            partitions: ctx.partitions.clone(),
            bootstrap: ctx.bootstrap.clone(),
            nodes: Arc::clone(&ctx.nodes),
            ..ctx
        };
        match heal(again).await {
            Err(HealError::PeersUnknown { nodes, within_ms }) => {
                let missing = nodes.clone();
                let error = HealError::PeersUnknown { nodes, within_ms };
                tracing::warn!(
                    attempt,
                    missing = ?missing,
                    %error,
                    next_ms = backoff.as_millis(),
                    "heal_retry"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(HEAL_RETRY_MAX);
            }
            ended => return ended,
        }
    }
}

/// The members `partitions/1` pins (M11 T6): a layout-4 record's own, in record order. A layout
/// 2 or 3 record predates members in the record (bug bash #64), when every database was nodes
/// 1 to 3, so it gets [`members`] with this open's owner primary, as every heal did before.
async fn pinned_members(ctx: &HealCtx) -> Result<Vec<Vec<RecordMember>>, HealError> {
    let (_, records) = heal_records(ctx).await?;
    let mut all = Vec::with_capacity(records.len());
    for (partition, found) in ctx.partitions.iter().zip(&records) {
        let pinned = pinned_of(*partition, &found.value)?;
        let fixed = pinned.is_none();
        let pinned = pinned.unwrap_or_else(|| {
            members()
                .iter()
                .map(|member| RecordMember {
                    role: if member.node == ctx.owner_node {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::RegularSecondary
                    },
                    ..RecordMember::of(member)
                })
                .collect()
        });
        tracing::info!(
            partition = partition.0,
            source = if fixed { "rf3_before_layout_4" } else { "record" },
            members = ?pinned.iter().map(|m| (m.node.0, m.copy.0)).collect::<Vec<_>>(),
            "heal_members"
        );
        all.push(pinned);
    }
    Ok(all)
}

/// The members a record body pins: `None` for a layout 2 or 3 body, an error for a layout-4
/// body that pins none.
fn pinned_of(partition: PartitionId, body: &[u8]) -> Result<Option<Vec<RecordMember>>, HealError> {
    match RecordMember::decode_all(body) {
        Some(members) if members.is_empty() => Err(HealError::Record(format!(
            "partitions/{} pins no members",
            partition.0
        ))),
        found => Ok(found),
    }
}

/// `pinned`, each at the boot `ctx.member_boot` names for its node, once every regular one is
/// known. Polls until `deadline`: a peer's boot arrives with its handshake. A `Shadow` whose
/// node is unknown is left out at once, never waited for (A-R451, critic A3): the plan commits
/// without it, and a re-run of its move starts it again.
async fn member_boots(
    ctx: &HealCtx,
    pinned: &[RecordMember],
    deadline: tokio::time::Instant,
) -> Result<Vec<Member>, HealError> {
    loop {
        let found: Vec<(RecordMember, Option<BootId>)> = pinned
            .iter()
            .map(|member| (*member, (ctx.member_boot)(member.node)))
            .collect();
        let late = tokio::time::Instant::now() >= deadline;
        let missing: Vec<u32> = found
            .iter()
            // Critic A3: a shadow serves no write, so the heal never waits on one that is
            // down; it is left out at once. Copying from a shadow that answers is still fine.
            .filter(|(member, boot)| boot.is_none() && member.role != ReplicaRole::Shadow)
            .map(|(member, _)| member.node.0)
            .collect();
        if missing.is_empty() {
            for (member, _) in found.iter().filter(|(_, boot)| boot.is_none()) {
                tracing::warn!(
                    node = member.node.0,
                    copy = member.copy.0,
                    "heal_shadow_left_out"
                );
            }
            let members: Vec<Member> = found
                .into_iter()
                .filter_map(|(member, boot)| Some(member.at(boot?)))
                .collect();
            tracing::info!(
                boots = ?members.iter().map(|m| (m.node.0, m.boot.0)).collect::<Vec<_>>(),
                "heal_member_boots"
            );
            return Ok(members);
        }
        if late {
            tracing::warn!(missing = ?missing, "heal_peers_unknown");
            return Err(HealError::PeersUnknown {
                nodes: missing,
                within_ms: ctx.within.as_millis(),
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Node numbers in words: `2`, `2 and 3`, `1, 2 and 3`.
fn names_in_words(nodes: &[u32]) -> String {
    match nodes {
        [] => String::new(),
        [one] => one.to_string(),
        [rest @ .., last] => format!(
            "{} and {last}",
            rest.iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// [`PromoteError::Superseded`]'s text: the other run's owner when it is not this run's `to`,
/// else the nodes it dropped, else that `partitions/1` changed (A-R448 F-001, F-002).
fn superseded_text(partition: u32, owner: Option<u32>, dropped: &[u32], generation: u64) -> String {
    match owner {
        Some(owner) => format!(
            "another promote committed instead: node {owner} is the owner, generation \
             {generation}; run status before promoting again"
        ),
        None if dropped.is_empty() => format!(
            "partitions/{partition} changed again after a promote (generation {generation}); run status \
             before promoting again"
        ),
        None => format!(
            "another promote committed instead: node {} dropped, generation {generation}; run \
             status before promoting again",
            names_in_words(dropped)
        ),
    }
}

/// How [`clear_until`] found a node's grant gone.
enum Gone {
    /// Deleted at this revision; the record it judged.
    Deleted(Revision, GrantRecord),
    /// Read absent at this store revision.
    Absent(Revision),
}

/// Run [`judge`] for `node` until it answers `Cleared` or `Absent`, or `deadline` passes. A
/// guard that still holds is waited out until about when it says it will stop holding.
async fn clear_until(
    ctx: &HealCtx,
    node: NodeId,
    deadline: tokio::time::Instant,
) -> Result<Gone, HealError> {
    let mut last = format!("the control store did not answer; {STORE_STEP}");
    while tokio::time::Instant::now() < deadline {
        match judge(&*ctx.store, node, ctx.boot, &ctx.budgets, ctx.clock).await {
            Ok((Clearance::Cleared(revision), Some(record), _)) => {
                return Ok(Gone::Deleted(revision, record));
            }
            Ok((Clearance::Absent, None, read)) => return Ok(Gone::Absent(read)),
            // Waiting never clears a frozen grant: only the promote that froze it, run again.
            Ok((
                clearance @ Clearance::Refused {
                    refusal: ClearRefusal::Frozen,
                    ..
                },
                ..,
            )) => {
                tracing::warn!(node = node.0, "heal_grant_frozen");
                return Err(HealError::Frozen {
                    node: node.0,
                    step: if ctx.partition_count > 1 {
                        frozen_step(node, &ctx.partitions)
                    } else {
                        kept_in_words(clearance, node, ctx.clock.now())
                    },
                });
            }
            Ok((clearance, ..)) => last = kept_in_words(clearance, node, ctx.clock.now()),
            // A lost delete reply is read again next time round, as `clear_grant` advises.
            Err(error)
                if error.retry == RetryRule::BoundedJitter
                    || error.kind == ErrorKind::UnknownOutcome =>
            {
                last = format!("the control store answered: {}; {STORE_STEP}", error.detail);
            }
            Err(error) => return Err(HealError::Control(error.detail)),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(HealError::NotCleared {
        node: node.0,
        within_ms: ctx.within.as_millis(),
        last,
    })
}

/// The next step for a frozen grant above one partition (issue #79 S3): each partition the
/// node still owns is moved by its own promote; a node that owns none opens with no heal.
fn frozen_step(node: NodeId, partitions: &[PartitionId]) -> String {
    let names: Vec<u32> = partitions.iter().map(|partition| partition.0).collect();
    format!(
        "a takeover from node {} is unfinished and it still owns partition {}; move each with \
         `admin promote <the node you chose> --partition <p>`, then open again",
        node.0,
        names_in_words(&names)
    )
}

/// The next step when another process may still hold the grant (A-R191b, tester w3a03).
const OTHER_OPEN_STEP: &str = "another open may hold this folder's data, for example a live \
                               copy of this folder: shut it down, then open again";

/// The next step when the control store did not settle an answer.
const STORE_STEP: &str = "open again once the control store answers";

/// Why `clearance` kept `grants/{node}` in place, with the next step, in words for
/// [`HealError::NotCleared`] (A-R191b). `now` is the heal's clock, so a guard's until-time reads
/// as a time of day and a distance from now.
fn kept_in_words(clearance: Clearance, node: NodeId, now: Tick) -> String {
    match clearance {
        Clearance::Refused {
            refusal: ClearRefusal::NotProvenExpired,
            until_utc_ms,
        } => {
            let left = i128::from(until_utc_ms) - i128::from(now.0);
            let when = if left > 0 {
                format!(
                    "about {} seconds from now",
                    seconds_in_words(left.unsigned_abs())
                )
            } else {
                "which has just passed".to_owned()
            };
            format!(
                "it may still be in use until {} UTC ({when}), and later if its holder keeps \
                 renewing it; {OTHER_OPEN_STEP}",
                clock_in_words(until_utc_ms)
            )
        }
        Clearance::Refused {
            refusal: ClearRefusal::Frozen,
            ..
        } => format!(
            "a takeover from node {} is unfinished; run `admin promote <the node you chose>` \
             again",
            node.0
        ),
        Clearance::Refused {
            refusal: ClearRefusal::PartitionInTransfer(partition),
            ..
        } => format!(
            "partition {} names node {} as its owner and is not serving yet: let that move \
             finish, then open again",
            partition.0, node.0
        ),
        Clearance::Current => format!(
            "it names the boot this open runs node {} at, so another open of this folder's data \
             may be running now, for example a live copy of this folder: shut it down, then open \
             again",
            node.0
        ),
        Clearance::Conflict => format!(
            "it changed while it was being cleared, so something still renews it; \
             {OTHER_OPEN_STEP}"
        ),
        Clearance::Absent | Clearance::Cleared(_) => {
            format!(
                "the control store gave an answer that does not fit ({clearance:?}); {STORE_STEP}"
            )
        }
    }
}

/// Milliseconds as seconds with at most one decimal: 13200 is "13.2", 30000 is "30".
fn seconds_in_words(ms: u128) -> String {
    let tenths = ms / 100;
    if tenths.is_multiple_of(10) {
        (tenths / 10).to_string()
    } else {
        format!("{}.{}", tenths / 10, tenths % 10)
    }
}

/// UTC milliseconds as a time of day, `HH:MM:SS`.
fn clock_in_words(utc_ms: i64) -> String {
    let secs = utc_ms.div_euclid(1000).rem_euclid(86_400);
    format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
}

/// What [`clear_grant`] did with `grants/{node}`. The simulator's service answers the same way
/// (`rdb_sim`'s `grant_service::Clearance`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clearance {
    /// `grants/{node}` holds nothing: there is nothing to clear.
    Absent,
    /// The record names the node's current boot: it is the running process's grant, however
    /// expired it reads. Nothing was written.
    Current,
    /// The record was deleted at this revision.
    Cleared(Revision),
    /// A guard held the record in place. Nothing was written.
    Refused {
        /// Which guard.
        refusal: ClearRefusal,
        /// `E + epsilon + delta` in UTC milliseconds: once control time is past it, guard 2
        /// holds. It moves if the old process still renews.
        until_utc_ms: i64,
    },
    /// All three guards held, but the record moved between the read and the delete. Nothing was
    /// deleted; run it again.
    Conflict,
}

/// Remove `grants/{node}` if a restarted node's old process left it and it may safely go (M9
/// S2b). The operator runs it, again until it answers [`Clearance::Cleared`] or
/// [`Clearance::Absent`]; a polling service is M10.
///
/// Reads the record, then the whole `partitions/` family, then asks
/// [`rdb_core::authority::grant::clear_verdict`] with the boot `db` runs `node` at now, never
/// the boot the record names, and the `Db`'s own clock. Only a [`ClearVerdict::Clear`] writes:
/// a delete at the record's own `mod_revision`, so a record renewed or replaced since the read
/// is left alone ([`Clearance::Conflict`]).
///
/// The `partitions/` list is read before the verdict because the verdict's read is synchronous
/// and `store` is not (critic A2). So with lists failing this is an error even where it would
/// answer [`Clearance::Current`].
///
/// Guard 3 and the delete are two calls: rEtcd has no compare across keys, and guard 3 can go
/// false between them, because a promote (the operator's or the opt-in planner's) writes
/// `Fencing` to `partitions/{p}`. The race is closed on the promote side, not here (spec §7.3,
/// "The clearing service does not race a fence"): every `Fencing` write is followed by a
/// `freeze` attempt on this same grant key. A freeze that fails for another reason than
/// [`PromoteError::OwnerAlive`] can return before it writes anything.
/// - A delete after the freeze cannot apply: the freeze moved the grant's revision, so this
///   delete's compare fails, and guard 1 refuses a frozen record on any later pass.
/// - A delete between the `Fencing` write and the freeze: when the promote judged the owner
///   dead, the freeze finds the record moved and refuses with [`PromoteError::OwnerAlive`], and
///   the promote undoes its `Fencing` write (or rereads and reports, when the undo is unproven);
///   otherwise the freeze creates the key frozen, so the transfer goes on against a frozen
///   record, never a vanished one.
///
/// So the clearer need not be the planner, and rEtcd needs no multi-key compare.
///
/// Logged as `grant_clear{node, boot, record_boot, revision, expiry, now, until, verdict}`, and a
/// failure as `grant_clear_failed{node, error}`.
///
/// # Errors
///
/// `INVALID_ARGUMENT` for a node `db` does not run, or a `Db` built without budgets (a crate
/// test's). `UNAVAILABLE` when a read fails, the `partitions/` list comes back truncated, or a
/// record does not decode: this call deletes nothing then, and `no_mutation` says so. Its retry rule is
/// `BoundedJitter` only for a store that was unavailable, timed out or not the leader, and
/// `NotWired` otherwise: repeating a refused or unreadable call changes nothing (F-012).
/// `UNKNOWN_OUTCOME` when the delete's reply is lost; running it again reads where it stands.
pub async fn clear_grant(
    db: &Db,
    store: &dyn ConfigStore,
    node: NodeId,
) -> Result<Clearance, ApiError> {
    let answer = judge_and_clear(db, store, node).await;
    if let Err(error) = &answer {
        tracing::warn!(node = node.0, %error, "grant_clear_failed");
    }
    answer
}

async fn judge_and_clear(
    db: &Db,
    store: &dyn ConfigStore,
    node: NodeId,
) -> Result<Clearance, ApiError> {
    let boot = db.boot_of(node).ok_or_else(|| db.no_such_node(node))?;
    let budgets = db.budgets().ok_or_else(|| {
        ApiError::invalid("this Db was built without budgets, so it cannot judge a grant")
    })?;
    judge(store, node, boot, &budgets, db.clock())
        .await
        .map(|(clearance, ..)| clearance)
}

/// [`clear_grant`] for `node` running at `boot`, judged with `budgets` on `clock`, without a
/// `Db`: the reopen's heal runs it from a task (M10, arch R3c). Also returns the record it
/// judged, when there was one, and the store revision of the read that found it or its absence.
async fn judge(
    store: &dyn ConfigStore,
    node: NodeId,
    boot: BootId,
    budgets: &Budgets,
    clock: HostClock,
) -> Result<(Clearance, Option<GrantRecord>, Revision), ApiError> {
    let budgets = *budgets;
    let key = ControlKey::Grant(node).encode();
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| store_failed(&format!("get {key}"), &error, true))?;
    let read_revision = Revision(read.read_revision);
    let Some(found) = read.record else {
        tracing::info!(
            node = node.0,
            boot = boot.0,
            read_revision = read_revision.0,
            verdict = ?Clearance::Absent,
            "grant_clear"
        );
        return Ok((Clearance::Absent, None, read_revision));
    };
    let record = GrantRecord::decode(&found.value).ok_or_else(|| {
        unreadable(format!(
            "{key} at revision {} is not a grant record",
            found.mod_revision
        ))
    })?;
    // R3a; M10 arch R5 row 9: A1's one minting site counts from 1, so a record holding 0 was
    // not written by A1. Refused before anything is deleted; a heal's proof carrying it would
    // read as `NoGrantHeld`'s "none held". A frozen grant 0 is a promote's (PR #41: it froze an
    // owner that held no grant), so it goes on to the `Frozen` verdict, which never clears it
    // and says to finish the takeover (A-R265).
    if record.grant == GrantId(0) && !record.frozen {
        tracing::warn!(
            node = node.0,
            revision = found.mod_revision,
            "grant_clear_grant_0_refused"
        );
        return Err(unreadable(format!(
            "{key} at revision {} holds grant 0, which no grant issuer writes",
            found.mod_revision
        )));
    }
    let partitions = list_partitions(store).await?;
    let now = clock.now();
    let sample = clock.sample(now, &budgets);
    let verdict = clear_verdict(node, &record, boot, sample, now, &budgets, || {
        Ok::<_, std::convert::Infallible>(partitions)
    })
    .unwrap_or_else(|never| match never {});
    let margin = budgets
        .clock_error_millis
        .saturating_add(budgets.dispatch_margin_millis);
    let until_utc_ms = record
        .expiry_utc_ms
        .saturating_add(i64::try_from(margin).unwrap_or(i64::MAX));
    let clearance = match verdict {
        ClearVerdict::Current => Clearance::Current,
        ClearVerdict::Refused(refusal) => Clearance::Refused {
            refusal,
            until_utc_ms,
        },
        ClearVerdict::Clear => delete(store, &key, found.mod_revision).await?,
    };
    tracing::info!(
        node = node.0,
        boot = boot.0,
        record_boot = record.boot.0,
        revision = found.mod_revision,
        expiry = record.expiry_utc_ms,
        now = now.0,
        until = until_utc_ms,
        verdict = ?clearance,
        "grant_clear"
    );
    Ok((clearance, Some(record), read_revision))
}

/// The whole `partitions/` family, or an error: a failed or truncated list is never read as a
/// complete one, since a missing record could hide a partition in transfer.
async fn list_partitions(store: &dyn ConfigStore) -> Result<Vec<PartitionRecord>, ApiError> {
    let prefix = ControlPrefix::Partitions.encode();
    let listed = store
        .list(ListRequest {
            prefix: Bytes::from_static(prefix.as_bytes()),
            max_items: 0,
            max_bytes: 0,
        })
        .await
        .map_err(|error| store_failed(&format!("list {prefix}"), &error, true))?;
    if listed.truncated {
        return Err(unreadable(format!(
            "list {prefix} came back truncated at revision {}",
            listed.read_revision
        )));
    }
    listed
        .records
        .iter()
        .map(|found| {
            PartitionRecord::decode(&found.value).ok_or_else(|| {
                unreadable(format!(
                    "{} at revision {} is not a partition record",
                    String::from_utf8_lossy(&found.key),
                    found.mod_revision
                ))
            })
        })
        .collect()
}

/// Delete `grants/{node}` only while it is still at `revision` (#81 X2: the planner's clear
/// of a stale frozen grant, judged by `clear_frozen_verdict`). `Cleared` at the delete's
/// revision, or `Conflict` when the record moved or went first.
///
/// # Errors
///
/// The store's failure, as [`clear_grant`]'s delete reports it.
pub(crate) async fn delete_grant_at(
    store: &dyn ConfigStore,
    node: NodeId,
    revision: u64,
) -> Result<Clearance, ApiError> {
    delete(store, &ControlKey::Grant(node).encode(), revision).await
}

/// Delete `key` only while it is still at `revision`.
async fn delete(store: &dyn ConfigStore, key: &str, revision: u64) -> Result<Clearance, ApiError> {
    let answer = store
        .delete(DeleteRequest {
            key: Bytes::from(key.to_owned()),
            expected_mod_revision: Some(revision),
            dedup: None,
        })
        .await;
    match answer {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            Ok(Clearance::Cleared(Revision(response.revision)))
        }
        Ok(_) | Err(ConfigError::Conflict { .. } | ConfigError::NotFound) => {
            Ok(Clearance::Conflict)
        }
        Err(ConfigError::DeadlineExceededUnknownOutcome) => Err(ApiError::new(
            ErrorKind::UnknownOutcome,
            format!("delete {key} at revision {revision}: the reply was lost"),
        )),
        // Refused before it was proposed: the record is as it was. Any other failure may have
        // reached the store.
        Err(error) => Err(store_failed(
            &format!("delete {key} at revision {revision}"),
            &error,
            matches!(
                error,
                ConfigError::Unavailable { .. } | ConfigError::NotLeader { .. }
            ),
        )),
    }
}

/// `call` failed in the control store, told briefly: the store's own words go to the log
/// only (`grant_clear_store_error{call, error}`), since an `Unavailable` names the whole
/// membership with its ports. A read that timed out says so: the store's wording, "mutation
/// outcome is unknown", is about a write. Never a lost delete reply, which
/// [`delete`] answers itself. Only a transient error ([`crate::control::transient`]) is worth
/// a jittered retry; any other says the call itself is refused, and repeating it changes
/// nothing (review F-012). `no_mutation` is the caller's: true for a read, false for a delete
/// that may have reached the store.
fn store_failed(call: &str, error: &ConfigError, no_mutation: bool) -> ApiError {
    tracing::warn!(call, %error, "grant_clear_store_error");
    let short = match error {
        ConfigError::DeadlineExceededUnknownOutcome => "timed out".to_owned(),
        ConfigError::Unavailable { .. } => {
            "unavailable now (the store's reason is in the log)".to_owned()
        }
        other => other.to_string(),
    };
    let retry = if crate::control::transient(error) {
        RetryRule::BoundedJitter
    } else {
        RetryRule::NotWired
    };
    unavailable(format!("{call}: {short}"), retry, no_mutation)
}

/// A record this build cannot read, or a list cut short: before anything is written, and the
/// same on every retry, so `NotWired` (review F-012).
fn unreadable(detail: String) -> ApiError {
    unavailable(detail, RetryRule::NotWired, true)
}

fn unavailable(detail: String, retry: RetryRule, no_mutation: bool) -> ApiError {
    let tail = if no_mutation {
        "nothing deleted"
    } else {
        "the delete may have applied; run it again to read where it stands"
    };
    ApiError {
        kind: ErrorKind::Unavailable,
        retry,
        no_mutation,
        detail: format!("control store: {detail}; {tail}"),
        retry_after: None,
    }
}

/// What an admin promote did (arch-s2 "Promote", ruling A-R256).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Promoted {
    /// The partition that moved.
    pub partition: PartitionId,
    /// The node that owns it now.
    pub owner: NodeId,
    /// The node that owned it before the promote.
    pub prior_owner: NodeId,
    /// The member the new configuration leaves out.
    pub dropped: NodeId,
    /// The generation the new owner committed.
    pub generation: Generation,
    /// The prior owner's grant, now frozen: the one it held, or `GrantId(0)` ("none held")
    /// when it held none and the promote created a frozen record in its place (PR #41).
    pub frozen_grant: GrantId,
    /// `partitions/1`'s `mod_revision` when step 7 first read it at the new generation, never
    /// the `Fencing` revision of a run that did not commit (A-R448). That is the commit's
    /// revision, or a later write at the same generation if one landed before step 7 looked.
    pub control_revision: Revision,
}

/// Why an admin promote did not answer Ok: it was refused, it stopped, or another promote
/// committed instead (`Superseded`, where `to` may own partition 1 by the other run's commit).
/// The first four change nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PromoteError {
    /// The node is a `Shadow` member: it never counts toward an ACK, so it cannot own.
    #[error("node {node} is catching up (shadow); promote node {instead} instead")]
    Shadow {
        /// The node asked for.
        node: u32,
        /// A regular member to name instead.
        instead: u32,
    },
    /// The node holds no copy of the partition.
    #[error("node {node} holds no copy of partition {partition}; name a node that does")]
    NotAMember {
        /// The node named.
        node: u32,
        /// The partition.
        partition: u32,
    },
    /// The node owns the partition already.
    #[error("node {node} already owns partition {partition}; nothing was changed")]
    AlreadyOwner {
        /// The node named.
        node: u32,
        /// The partition.
        partition: u32,
    },
    /// The node to drop is the node to promote.
    #[error(
        "the node to drop is the node to promote; drop another member, or name none to drop the \
         owner"
    )]
    DropIsTarget,
    /// No handshake from these nodes within the promote's time, so the plan cannot pin their
    /// boots. Nothing was written.
    #[error(
        "no link to node {who} came up within {secs} seconds, so the promote cannot pin its \
         boot: start node {who}, then promote again",
        who = names_in_words(.nodes),
        secs = seconds_in_words(*.within_ms)
    )]
    PeersUnknown {
        /// The nodes with no boot yet.
        nodes: Vec<u32>,
        /// The promote's bound.
        within_ms: u128,
    },
    /// `to` answered holding no configuration: it has landed none since its restart, so its
    /// members' roles are unknown and a shadow could be pinned regular. Nothing was written,
    /// and running it again will not help until a configuration lands (A-R415 C-1, A-R416).
    #[error(
        "node {node} holds no configuration since its restart, so the promote cannot tell which \
         members are still catching up; nothing was changed. A promote needs a node that has \
         landed a configuration since it started: start the node that owned partition {partition}, or \
         promote a node that has not restarted since that node stopped"
    )]
    Unconfigured {
        /// The node asked for.
        node: u32,
        /// The partition.
        partition: u32,
    },
    /// `to`'s configuration is not the one `partitions/1` names, so its roles may be out of
    /// date (A-R415 C-2). Nothing was written.
    #[error(
        "node {node} holds configuration version {held}, but partitions/{partition} is at version \
         {current}, so its members' roles may be out of date; nothing was changed. Wait until \
         node {node} has landed version {current}, then promote again"
    )]
    StaleMembers {
        /// The node asked for.
        node: u32,
        /// The version it holds.
        held: u64,
        /// `partitions/1`'s version.
        current: u64,
        /// The partition.
        partition: u32,
    },
    /// The control store refused, or the promote ran out of time after it wrote: the detail
    /// says which, and the step.
    #[error("control store: {0}")]
    Control(String),
    /// `partitions/1` or the owner's grant record is not one this build reads.
    #[error("{0}")]
    Record(String),
    /// Another promote committed instead of this one (A-R448): it fenced over this run after
    /// this run had sent its plan, and F1 committed its plan. A promote did happen.
    #[error("{}", superseded_text(*.partition, *.owner, .dropped, *.generation))]
    Superseded {
        /// The partition.
        partition: u32,
        /// The node that owns partition 1 by that commit, when it is not the node this run
        /// promoted; `None` when it is.
        owner: Option<u32>,
        /// The nodes of this run's record that the committed configuration leaves out. Empty
        /// when it leaves none out (a rejoin added one back) or pins no members.
        dropped: Vec<u32>,
        /// The generation it committed.
        generation: u64,
    },
    /// The new owner's node could not be sent the plan.
    #[error(
        "node {node} could not be sent the new plan; partition {partition} takes no writes until \
         a promote finishes: check that node {node} runs, then promote again"
    )]
    Unreachable {
        /// The node.
        node: u32,
        /// The partition.
        partition: u32,
    },
    /// The node to promote holds a frozen grant, from a takeover away from it. Nothing clears
    /// one, so it could never admit writes: the partition would be stranded. Nothing was
    /// changed. Bringing a frozen node back to lead is not built (#61 critic Y3).
    #[error(
        "node {node}'s grant is frozen by an earlier takeover, so it cannot lead partition \
         {partition}; nothing was changed: {step}",
        step = crate::db::error::PROMOTE_FROZEN_STEP
    )]
    TargetFrozen {
        /// The node asked for.
        node: u32,
        /// The partition.
        partition: u32,
    },
    /// Above one partition, the owner is alive: it answered a look without a host fault, or
    /// its grant is still being renewed. An owner that answers with a host fault is not alive
    /// here (issue #122). A promote freezes the whole node's grant and moves one partition, so
    /// it would fence the node's other partitions (issue #79 S3). Nothing was changed. A live
    /// owner's promote is #61 slice S7.
    #[error(
        "node {node} is alive and owns partition {partition} ({evidence}): a promote of one \
         partition from a live node is not built, because it would stop every partition node \
         {node} owns; nothing was changed: {step}",
        step = crate::db::error::PROMOTE_ALIVE_STEP
    )]
    OwnerAlive {
        /// The owner.
        node: u32,
        /// The partition asked for.
        partition: u32,
        /// What shows it is alive.
        evidence: String,
    },
    /// The partition asked for is not one this database has.
    #[error(
        "this database has no partition {partition}: it has partitions 1 to {partitions}; \
         nothing was changed"
    )]
    NoSuchPartition {
        /// The partition asked for.
        partition: u32,
        /// How many this database has.
        partitions: u32,
    },
    /// This `Db` has no control store to run the promote against.
    #[error(
        "this Db was opened without a control store, so it cannot run admin commands such as \
         promote: open it with Db::open, which connects it to one"
    )]
    NoControlStore,
    /// This process's clock bound is lost (`clock_bound_lost`): its guard cannot show the clock
    /// is within epsilon of its peers', so no expiry it reads is a proof. Refused before
    /// anything is written, so no owner is fenced or frozen on a proof that cannot finish
    /// (PR #103 review B2).
    #[error(
        "this process's clock is not shown to agree with its peers (clock_bound_lost), so it \
         cannot prove the owner's grant expired; nothing was changed: wait until its clock \
         agrees with its peers again, then promote again"
    )]
    ClockUnbounded,
    /// The promote gave no answer in time, and may have written.
    #[error(
        "the promote of node {node} did not answer in time; it may have written: promote again"
    )]
    Unanswered {
        /// The node asked to take the partition.
        node: u32,
    },
}

impl PromoteError {
    /// Whether the promote stopped before it wrote anything.
    #[must_use]
    pub const fn no_mutation(&self) -> bool {
        matches!(
            self,
            Self::Shadow { .. }
                | Self::NotAMember { .. }
                | Self::AlreadyOwner { .. }
                | Self::DropIsTarget
                | Self::PeersUnknown { .. }
                | Self::Unconfigured { .. }
                | Self::StaleMembers { .. }
                | Self::OwnerAlive { .. }
                | Self::TargetFrozen { .. }
                | Self::NoSuchPartition { .. }
                | Self::NoControlStore
                | Self::ClockUnbounded
        )
    }
}

impl From<PromoteError> for ApiError {
    /// A-R256: a refused argument is `INVALID_ARGUMENT`; anything else `UNAVAILABLE`. Each of
    /// those texts but `Record`'s, `Unconfigured`'s and `Superseded`'s ends "promote again", and promoting again
    /// picks up where this one stopped, so its retry rule is `BoundedJitter`: the step says to
    /// try again and agrees with the text. A record this build cannot read, or a `to` with no configuration since its
    /// restart, or another promote's commit (`Superseded`), is `NotWired`: retrying will not help.
    fn from(error: PromoteError) -> Self {
        let no_mutation = error.no_mutation();
        let retry = match &error {
            PromoteError::Shadow { .. }
            | PromoteError::NotAMember { .. }
            | PromoteError::AlreadyOwner { .. }
            | PromoteError::DropIsTarget
            | PromoteError::NoSuchPartition { .. } => return Self::invalid(error.to_string()),
            PromoteError::Record(_)
            | PromoteError::Unconfigured { .. }
            | PromoteError::OwnerAlive { .. }
            | PromoteError::TargetFrozen { .. }
            | PromoteError::Superseded { .. }
            | PromoteError::NoControlStore
            | PromoteError::Unanswered { .. } => RetryRule::NotWired,
            PromoteError::PeersUnknown { .. }
            | PromoteError::StaleMembers { .. }
            | PromoteError::Unreachable { .. }
            | PromoteError::ClockUnbounded
            | PromoteError::Control(_) => RetryRule::BoundedJitter,
        };
        Self {
            kind: ErrorKind::Unavailable,
            retry,
            no_mutation,
            detail: error.to_string(),
            retry_after: None,
        }
    }
}

/// What an admin promote runs with.
pub(crate) struct PromoteCtx {
    pub(crate) store: Arc<dyn ConfigStore>,
    pub(crate) budgets: Budgets,
    pub(crate) clock: HostClock,
    /// The partition to move.
    pub(crate) partition: PartitionId,
    /// Refuse an owner that is alive ([`PromoteError::OwnerAlive`]): true above one partition,
    /// where the freeze would stop the node's other partitions (issue #79 S3).
    pub(crate) owner_must_be_dead: bool,
    /// How long the owner gets to answer a look before it counts as not answering.
    pub(crate) look_within: Duration,
    /// The node to make the owner.
    pub(crate) to: NodeId,
    /// The member to leave out; `None` leaves out the prior owner.
    pub(crate) drop: Option<NodeId>,
    /// How long the whole promote may take, the prior grant's expiry and the commit included.
    pub(crate) within: Duration,
    /// The members partition 1 is pinned to now, with their roles. `Db::promote` reads them from
    /// `to`'s newest configuration, never from [`members`], which holds no shadow. Used only
    /// while `partitions/1` is layout 3: a layout-4 record pins its own (bug bash #64, A-R426).
    pub(crate) members: Vec<Member>,
    /// The version of the configuration `members` comes from. `promote` refuses
    /// [`PromoteError::StaleMembers`] when it is not `partitions/1`'s (A-R415 C-2).
    pub(crate) members_version: ConfigVersion,
    /// The boot each member runs at, as for a heal ([`HealCtx::member_boot`]).
    pub(crate) member_boot: Arc<dyn Fn(NodeId) -> Option<BootId> + Send + Sync>,
    /// Deliver a message to a node's host.
    pub(crate) send: Arc<dyn Fn(NodeId, Msg) -> Result<(), NodeStopped> + Send + Sync>,
}

/// The members a promote of `to` plans with, their roles, and the version of the configuration
/// they come from: `to`'s own newest configuration, from its status `looked` (`None`: it did
/// not answer within `within`). Never [`members`], which holds no shadow, so a shadow `to` is
/// refused and a kept shadow stays one (review-s2 F-004, A-R412). `promote` refuses the set
/// when its version is not `partitions/1`'s (A-R415 C-2). Refused before anything is written.
///
/// # Errors
///
/// - [`PromoteError::PeersUnknown`] naming `to` when it did not answer.
/// - [`PromoteError::Unconfigured`] when it answered holding no configuration: it has landed
///   none since its restart (A-R415 C-1, A-R416).
pub(crate) fn promote_members(
    partition: PartitionId,
    to: NodeId,
    looked: Option<crate::host::NodeStatus>,
    within: Duration,
) -> Result<(Vec<Member>, ConfigVersion), PromoteError> {
    let error = match looked {
        // `!pinned.is_empty()` is unreachable today (the host fills `pinned` and
        // `config_version` from one committed config, which always has members); kept so a
        // later split refuses here instead of pinning nobody. Defensive, no row.
        // Only the configuration is read. The target's clock bound is checked where it commits
        // (`bind_routed_fence`, row 13d), and a qualifying copy says nothing about its members:
        // neither is a reason to call it unconfigured (PR #103 review question).
        Some(crate::host::NodeStatus {
            pinned,
            config_version: Some(version),
            ..
        }) if !pinned.is_empty() => return Ok((pinned, version)),
        Some(_) => PromoteError::Unconfigured {
            node: to.0,
            partition: partition.0,
        },
        None => PromoteError::PeersUnknown {
            nodes: vec![to.0],
            within_ms: within.as_millis(),
        },
    };
    // Debug, not a warning: a layout-4 `partitions/1` may still plan the promote, and then the
    // promote succeeds. `promote_looked` warns when it refuses on this look (bug bash #64).
    tracing::debug!(to = to.0, %error, "promote_members_unknown");
    Err(error)
}

/// The correlation a promote's two events carry.
const PROMOTE: CorrelationId = CorrelationId(4);

/// [`promote_looked`] with no refused look: the entry the tests use.
#[cfg(test)]
pub(crate) async fn promote(ctx: PromoteCtx) -> Result<Promoted, PromoteError> {
    promote_looked(ctx, None).await
}

/// Make `ctx.to` the owner of `ctx.partition` while its owner is dead or cut off (arch-s2
/// "Promote", A-R256). In order:
///
/// 1. Read `partitions/1`. Plan with the members it pins when it is layout 4, else with
///    `ctx.members` (bug bash #64, ruling A-R426). Refuse a `to` or `drop` that is not a member,
///    a `Shadow` `to`, the owner as `to`, and `drop == to`, before anything is written.
/// 2. Pin the boots of the two members kept: every member but `drop` (default: the prior
///    owner). A promote always plans two members at a new generation.
/// 3. CAS `partitions/1` to `Fencing` at the revision read, as layout 4 with the members of
///    step 1 (A-R426 M1), so a promote run again after a full restart finds them. Always: a
///    promote that stopped may have written it already, and the write still gives this run a
///    revision `R` of its own (A-R447).
/// 4. Freeze `grants/{prior}`, or create it frozen when it is absent (PR #41): no renewal can
///    move its expiry `E` after this.
/// 5. Wait until control time is past `E + epsilon + delta` ([`expiry_proven`]): from then
///    the prior owner admits nowhere.
/// 6. Read `partitions/1` again, at a store revision not below the freeze, and refuse unless it
///    is still at `R`. Store the plan, create-only, at [`promote_operation`]`(R)`
///    ([`store_plan`]), then hand `to` the plan and a fence whose revocation is that
///    `ExpiryProven` and whose `control_revision` is `R`. F1's commit is a CAS there. The node
///    commits only the stored plan, never the one sent (A-R447, issue #70); the plan is sent
///    too only so a node logs it arriving.
/// 7. Wait until `partitions/1` names `to` as owner at a newer generation, pinning the
///    members of this run's plan (A-R448). Another set there, or another owner at a newer
///    generation, is another promote's commit: [`PromoteError::Superseded`], at once. A
///    record that pins none (layout 3) is taken as today and logged
///    `promote_outcome_unproven`. The answer names the commit's revision.
///
/// Running it again after it stopped picks up from the record and grant it left. Logged as
/// `promote_*` lines.
///
/// [`expiry_proven`]: rdb_core::authority::clock::expiry_proven
///
/// # Errors
///
/// [`PromoteError`].
///
/// `refused` is the caller's refused look at `to` ([`promote_members`]). It is the answer when
/// `partitions/1` pins no members (layout 3), or when `to` did not answer at all
/// ([`PromoteError::PeersUnknown`]). Otherwise `ctx.members` is unused and the record's members
/// are planned with (bug bash #64, ruling A-R431).
pub(crate) async fn promote_looked(
    ctx: PromoteCtx,
    refused: Option<PromoteError>,
) -> Result<Promoted, PromoteError> {
    let deadline = tokio::time::Instant::now() + ctx.within;
    let partition = ctx.partition.0;
    let (record, root, stored, mod_revision, _, prior_record) =
        read_partition(&*ctx.store, ctx.partition).await?;
    let (planned, planned_version) = match (&stored, refused) {
        // `to` did not answer at all: it is refused before anything is written whatever the
        // record pins, so no owner is fenced toward a node that is not there (A-R412).
        (_, Some(refused @ PromoteError::PeersUnknown { .. })) | (None, Some(refused)) => {
            tracing::warn!(
                to = ctx.to.0,
                error = %refused,
                no_mutation = refused.no_mutation(),
                "promote_members_unknown"
            );
            return Err(refused);
        }
        (Some(stored), _) => (
            stored
                .iter()
                .map(|member| member.at(UNLINKED_BOOT))
                .collect::<Vec<_>>(),
            record.config_version,
        ),
        (None, None) => (ctx.members.clone(), ctx.members_version),
    };
    tracing::info!(
        from = if stored.is_some() { "record" } else { "live" },
        members = ?planned.iter().map(|m| (m.copy.0, m.node.0, m.role)).collect::<Vec<_>>(),
        "promote_members_from"
    );
    let prior = record.owner;
    let dropped = ctx.drop.unwrap_or(prior);
    let member = |node: NodeId| planned.iter().find(|member| member.node == node);
    let target = member(ctx.to).ok_or(PromoteError::NotAMember {
        node: ctx.to.0,
        partition,
    })?;
    if target.role == ReplicaRole::Shadow {
        let instead = planned
            .iter()
            .find(|member| member.role != ReplicaRole::Shadow && member.node != prior)
            .map_or(prior, |member| member.node);
        return Err(PromoteError::Shadow {
            node: ctx.to.0,
            instead: instead.0,
        });
    }
    if ctx.to == prior {
        return Err(PromoteError::AlreadyOwner {
            node: ctx.to.0,
            partition,
        });
    }
    if planned_version != record.config_version {
        return Err(PromoteError::StaleMembers {
            node: ctx.to.0,
            held: planned_version.0,
            current: record.config_version.0,
            partition,
        });
    }
    if dropped == ctx.to {
        return Err(PromoteError::DropIsTarget);
    }
    member(dropped).ok_or(PromoteError::NotAMember {
        node: dropped.0,
        partition,
    })?;
    tracing::info!(
        to = ctx.to.0,
        prior = prior.0,
        dropped = dropped.0,
        generation = record.generation.0,
        lifecycle = ?record.lifecycle,
        "promote_started"
    );
    refuse_frozen_target(&ctx).await?;
    // What the Fencing CAS writes and step 6 expects: every member, the dropped one included,
    // in its role before this promote (A-R426 M1).
    let written: Vec<RecordMember> = planned.iter().map(RecordMember::of).collect();
    let members = kept_boots(&ctx, &planned, dropped, deadline).await?;
    tracing::info!(
        members = ?members.iter().map(|m| (m.node.0, m.boot.0, m.role)).collect::<Vec<_>>(),
        "promote_members_kept"
    );

    // Every expiry below is proved on this process's clock. With no bound it proves nothing,
    // so refuse before the Fencing write and the freeze, not after them (PR #103 review B2).
    refuse_unbounded(&ctx)?;

    // Above one partition the freeze below is the whole node's, so only a dead owner goes on
    // (issue #79 S3). The grant revision read here is what step 4 must still find.
    let dead_at = if ctx.owner_must_be_dead {
        Some(owner_dead(&ctx, prior, deadline).await?)
    } else {
        None
    };

    // 3. Fencing, written by this run even when a stopped run already wrote it (A-R447): the
    // identical bytes still move `mod_revision`, so each run fences at a revision of its own,
    // and its plan at `operations/` is keyed by it. Written as layout 4, so a `Fencing` record
    // that pins no members (a layout-3 store) gains them here.
    let fencing = PartitionRecord {
        lifecycle: PartitionLifecycle::Fencing,
        ..record
    };
    let fenced_at = cas(
        &*ctx.store,
        &ControlKey::Partition(ctx.partition).encode(),
        fencing.encode_with_members(
            &root,
            &written,
            Some(&prior_record.note),
            Some(&prior_record.ledger),
        ),
        mod_revision,
    )
    .await?;
    tracing::info!(revision = fenced_at.0, "promote_fencing");

    // 4. Freeze the prior owner's grant.
    let (frozen, frozen_at) = match freeze(&ctx, prior, root.owner_boot, dead_at, deadline).await {
        // The owner came back between the look and the Fencing write: nothing is frozen, so
        // the record goes back to what it was and the refusal is true (issue #79 S3).
        Err(alive @ PromoteError::OwnerAlive { .. }) => {
            let undone = cas(
                &*ctx.store,
                &ControlKey::Partition(ctx.partition).encode(),
                record.encode_with_members(
                    &root,
                    &written,
                    Some(&prior_record.note),
                    Some(&prior_record.ledger),
                ),
                fenced_at.0,
            )
            .await;
            if let Err(undo) = undone {
                return Err(undo_unproven(&ctx, &record, fenced_at, undo, alive).await);
            }
            tracing::warn!(owner = prior.0, partition, "promote_fencing_undone");
            return Err(alive);
        }
        frozen => frozen?,
    };

    // 5. Its expiry, proven past.
    let (revocation, decided) = loop {
        let now = ctx.clock.now();
        let mut view = ClockView::new();
        view.accept(ctx.clock.sample(now, &ctx.budgets));
        if let Some(revocation) = expiry_proven(&view, frozen.expiry_utc_ms, now, &ctx.budgets) {
            break (revocation, now);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(PromoteError::Control(format!(
                "the expiry of grants/{} was not proven past within {} seconds; partition {partition} \
                 takes no writes until a promote finishes: promote again",
                prior.0,
                seconds_in_words(ctx.within.as_millis())
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    tracing::info!(
        expiry = frozen.expiry_utc_ms,
        now = decided.0,
        "promote_expiry_proven"
    );
    // A member pinned unlinked whose handshake came during the wait is pinned at its boot after
    // all, so F1 can count it (A-R401 F1). One that is still unlinked stays at `UNLINKED_BOOT`.
    let members: Vec<Member> = members
        .into_iter()
        .map(|member| match (ctx.member_boot)(member.node) {
            Some(boot) if member.boot == UNLINKED_BOOT => {
                tracing::info!(
                    node = member.node.0,
                    boot = boot.0,
                    "promote_member_linked_late"
                );
                Member { boot, ..member }
            }
            _ => member,
        })
        .collect();

    // 6. The record again, after the freeze, and the plan.
    let (fenced, fenced_root, fenced_members, control_revision, read_revision, _) =
        read_partition(&*ctx.store, ctx.partition).await?;
    if read_revision < frozen_at.0 {
        return Err(PromoteError::Control(format!(
            "partitions/{partition} was read at revision {read_revision}, before the freeze at {}; promote \
             again",
            frozen_at.0
        )));
    }
    let expected = PartitionRecord {
        lifecycle: PartitionLifecycle::Fencing,
        ..record
    };
    if fenced != expected || fenced_root != root || fenced_members.as_ref() != Some(&written) {
        return Err(PromoteError::Control(format!(
            "partitions/{partition} changed during the promote (owner {}, generation {}, {:?}); promote \
             again",
            fenced.owner.0, fenced.generation.0, fenced.lifecycle
        )));
    }
    if control_revision != fenced_at.0 {
        return Err(PromoteError::Control(format!(
            "partitions/{partition} was fenced again at revision {control_revision} after this promote \
             fenced it at {}: another promote is running; promote again",
            fenced_at.0
        )));
    }
    let lineage = Lineage {
        partition: ctx.partition,
        generation: record.generation,
        owner_epoch: record.owner_epoch,
    };
    let anchor = LineageAnchor {
        lineage,
        base_seq: root.base_seq,
        base_digest: root.base_digest,
    };
    let to_boot = members
        .iter()
        .find(|member| member.node == ctx.to)
        .map_or(BOOT, |member| member.boot);
    tracing::info!(
        members = ?members.iter().map(|m| (m.node.0, m.boot.0, m.role)).collect::<Vec<_>>(),
        config_version = record.config_version.0 + 1,
        control_revision,
        "promote_plan"
    );
    let plan = recovery_plan(
        anchor,
        ConfigVersion(record.config_version.0 + 1),
        members,
        to_boot,
        GrantId(1),
        AuthorityGeneration(1),
        prior_record,
    );
    store_plan(&*ctx.store, ctx.partition, fenced_at, &plan).await?;
    // What F1 pins in `partitions/1` when it commits this plan (A-R448): step 7 answers Ok only
    // for a commit that carries it.
    let own: Vec<NodeId> = sorted_nodes(&RecordMember::pinned(&plan.config));
    let fence = FencingProof {
        partition: ctx.partition,
        prior_generation: record.generation,
        prior_owner_epoch: record.owner_epoch,
        prior_grant_id: frozen.grant,
        prior_boot_id: root.owner_boot,
        revocation,
        control_revision: Revision(control_revision),
        decision_tick: decided,
    };
    // A-R265/A-R266: as `Routed`, the one kernel-event message that crosses the wire, so a
    // promote run from another process reaches `to` (an `Inject` never leaves its process). Both
    // go the same way, in this order, as in one process.
    for event in [
        RecoveryEvent::Plan(Box::new(plan)),
        RecoveryEvent::FenceProven(Box::new(fence)),
    ] {
        (ctx.send)(
            ctx.to,
            Msg::Routed {
                partition: ctx.partition,
                correlation: PROMOTE,
                kind: KernelEvent::Recovery(event),
            },
        )
        .map_err(|_| PromoteError::Unreachable {
            node: ctx.to.0,
            partition,
        })?;
    }

    // 7. The commit, and that it is this run's (A-R448): another promote that fenced over this
    // one after it sent may be the one F1 committed.
    loop {
        let (now, _, pinned, committed_at, _, _) =
            read_partition(&*ctx.store, ctx.partition).await?;
        let committed = pinned.as_deref().map(sorted_nodes);
        let left_out = |committed: &[NodeId]| -> Vec<u32> {
            written
                .iter()
                .map(|member| member.node.0)
                .filter(|node| !committed.contains(&NodeId(*node)))
                .collect()
        };
        // Another run committed another owner: this run's plan can no longer commit, so say so
        // now rather than wait out the deadline and advise promoting again (A-R448 F-002).
        if now.generation > record.generation && now.owner != ctx.to {
            tracing::warn!(
                owner = now.owner.0,
                to = ctx.to.0,
                generation = now.generation.0,
                revision = committed_at,
                "promote_superseded"
            );
            return Err(PromoteError::Superseded {
                partition,
                owner: Some(now.owner.0),
                dropped: committed.as_deref().map(left_out).unwrap_or_default(),
                generation: now.generation.0,
            });
        }
        if now.owner == ctx.to && now.generation > record.generation {
            match committed {
                Some(committed) if committed != own => {
                    let dropped = left_out(&committed);
                    tracing::warn!(
                        owner = ctx.to.0,
                        generation = now.generation.0,
                        revision = committed_at,
                        committed = ?committed.iter().map(|node| node.0).collect::<Vec<_>>(),
                        own = ?own.iter().map(|node| node.0).collect::<Vec<_>>(),
                        "promote_superseded"
                    );
                    return Err(PromoteError::Superseded {
                        partition,
                        owner: None,
                        dropped,
                        generation: now.generation.0,
                    });
                }
                Some(_) => {}
                None => tracing::warn!(
                    owner = ctx.to.0,
                    generation = now.generation.0,
                    "promote_outcome_unproven"
                ),
            }
            tracing::info!(
                owner = ctx.to.0,
                generation = now.generation.0,
                revision = committed_at,
                "promote_committed"
            );
            return Ok(Promoted {
                partition: ctx.partition,
                owner: ctx.to,
                prior_owner: prior,
                dropped,
                generation: now.generation,
                frozen_grant: frozen.grant,
                control_revision: Revision(committed_at),
            });
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(PromoteError::Control(format!(
                "node {} did not commit partition {partition} within {} seconds (partitions/{partition} names owner \
                 {} at generation {}); partition {partition} takes no writes until a promote finishes. If \
                 node {} refused the fence, its log has a routed_fence_refused line saying why: \
                 promote again",
                ctx.to.0,
                seconds_in_words(ctx.within.as_millis()),
                now.owner.0,
                now.generation.0,
                ctx.to.0
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Spec §7.3: the undo of this promote's `Fencing` write failed or did not answer, after the
/// freeze found the owner alive (PR #119 review). What the record holds is read, never
/// assumed, and the answer is what was found:
///
/// * still `Fencing` (or `FencingDrained`): a later promote resumes from it (A-R447). Refused
///   as [`PromoteError::Control`], saying so; logged `promote_fencing_undo_failed`. Nothing is
///   claimed about writes: the owner came back, and its grant may still admit them.
/// * back to `before`, the record this promote fenced: the undo applied and only its answer
///   was lost, so the `alive` refusal stands; logged `promote_fencing_undone`.
/// * the reread fails: whether the partition is still fenced is not known. Refused as
///   [`PromoteError::Control`] carrying both errors, saying a later promote reads it again;
///   logged `promote_fencing_undo_unknown`.
/// * any other record (a newer owner or generation committed meanwhile): the undo was
///   overtaken. Refused as [`PromoteError::Control`] naming that record, with nothing left to
///   resume; logged `promote_fencing_undo_overtaken`.
async fn undo_unproven(
    ctx: &PromoteCtx,
    before: &PartitionRecord,
    fenced_at: Revision,
    undo: PromoteError,
    alive: PromoteError,
) -> PromoteError {
    let partition = ctx.partition.0;
    let owner = before.owner.0;
    match read_partition(&*ctx.store, ctx.partition).await {
        Err(read) => {
            tracing::warn!(
                owner,
                partition,
                fenced_at = fenced_at.0,
                %undo,
                %read,
                "promote_fencing_undo_unknown"
            );
            PromoteError::Control(format!(
                "partitions/{partition} was fenced at revision {} and node {owner} came back; \
                 undoing the fence failed ({undo}) and partitions/{partition} could not be read \
                 again ({read}), so whether it is still fenced is not known: promote again, \
                 which reads it again",
                fenced_at.0
            ))
        }
        Ok((now, _, _, revision, _, _)) if now.lifecycle != PartitionLifecycle::Serving => {
            tracing::warn!(
                owner,
                partition,
                fenced_at = fenced_at.0,
                revision,
                lifecycle = ?now.lifecycle,
                ours = revision == fenced_at.0,
                %undo,
                "promote_fencing_undo_failed"
            );
            PromoteError::Control(format!(
                "partitions/{partition} is still {:?} at revision {revision}: node {owner} came \
                 back, and undoing this promote's fence failed ({undo}). A later promote resumes \
                 from it: promote again",
                now.lifecycle
            ))
        }
        Ok((now, ..)) if now == *before => {
            tracing::warn!(
                owner,
                partition,
                %undo,
                answer_lost = true,
                "promote_fencing_undone"
            );
            alive
        }
        Ok((now, _, _, revision, _, _)) => {
            tracing::warn!(
                owner,
                partition,
                revision,
                now_owner = now.owner.0,
                generation = now.generation.0,
                %undo,
                "promote_fencing_undo_overtaken"
            );
            PromoteError::Control(format!(
                "partitions/{partition} changed while this promote undid its fence ({undo}): it \
                 now names owner {} at generation {}, {:?}, revision {revision}; this promote has \
                 nothing left to resume",
                now.owner.0, now.generation.0, now.lifecycle
            ))
        }
    }
}

/// `partitions/{partition}` as a rooted record, the members it pins (`None` for layout 3), its
/// `mod_revision`, the read's store revision and what a recovery superseding it needs.
pub(crate) async fn read_partition(
    store: &dyn ConfigStore,
    partition: PartitionId,
) -> Result<
    (
        PartitionRecord,
        PartitionRoot,
        Option<Vec<RecordMember>>,
        u64,
        u64,
        PriorRecord,
    ),
    PromoteError,
> {
    let read = store
        .get(GetRequest {
            key: Bytes::from(ControlKey::Partition(partition).encode()),
        })
        .await
        .map_err(|error| {
            PromoteError::Control(format!(
                "get partitions/{}: {error}; promote again",
                partition.0
            ))
        })?;
    let found = read
        .record
        .ok_or_else(|| PromoteError::Record(format!("partitions/{} is absent", partition.0)))?;
    match (
        PartitionRecord::decode(&found.value),
        PartitionRoot::decode(&found.value),
        prior_of(&found.value),
    ) {
        (Some(record), Some(root), Some(prior)) => Ok((
            record,
            root,
            RecordMember::decode_all(&found.value),
            found.mod_revision,
            read.read_revision,
            prior,
        )),
        _ => Err(PromoteError::Record(format!(
            "partitions/{} at revision {} carries no root (layout 3 or 4)",
            partition.0, found.mod_revision
        ))),
    }
}

/// Write `value` at `key` if it is still at `revision`; the revision it was written at.
async fn cas(
    store: &dyn ConfigStore,
    key: &str,
    value: Bytes,
    revision: u64,
) -> Result<Revision, PromoteError> {
    let answer = store
        .put(PutRequest {
            key: Bytes::from(key.to_owned()),
            value,
            expected_mod_revision: Some(revision),
            dedup: None,
        })
        .await;
    match answer {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            Ok(Revision(response.revision))
        }
        Ok(_) | Err(ConfigError::Conflict { .. }) => Err(PromoteError::Control(format!(
            "{key} changed since it was read at revision {revision}; promote again"
        ))),
        Err(error) => Err(PromoteError::Control(format!(
            "put {key} at revision {revision}: {error}; promote again"
        ))),
    }
}

/// The first [`OperationId`] a promote's stored plan takes: the promote tag in bits 48..64, as
/// [`rdb_core::recovery::RECOVERY_CONTROL_REQUEST_BASE`] tags F1's requests, so
/// `operations/` stays free below it for other operations (A-R447).
pub(crate) const PROMOTE_OPERATION_BASE: u64 = 0x00B2 << 48;

/// The first byte of a stored promote plan: the layout of the postcard bytes after it. A build
/// that reads another refuses it (`plan_unreadable`) instead of guessing at its fields.
///
/// `2` (PR #74 F-001, ruling A-R477) appends [`RecoveryPlan::must_hold`]. `3` (#80 S5n) appends
/// [`RecoveryPlan::prior`] after it. `4` (issue #105) appends the prior record's ledger inside
/// `prior`, then [`RecoveryPlan::release`]. postcard is not self-describing, so a field cannot be
/// defaulted in: this build reads `1` as [`StoredPlanV1`], `2` as [`StoredPlanV2`] and `3` as
/// [`StoredPlanV3`], the fields they lack set by [`StoredPlanV1::with`], and an older build
/// refuses a newer plan by this byte, naming both versions (`plan version 2, this build reads 1`).
pub(crate) const PROMOTE_PLAN_VERSION: u8 = 4;

/// The stored plan before `must_hold` (version `1`): every field of [`RecoveryPlan`] before it,
/// in order.
#[derive(serde::Deserialize)]
struct StoredPlanV1 {
    anchor: LineageAnchor,
    config: PartitionConfig,
    candidates: Vec<Candidate>,
    rebuild_required: std::collections::BTreeSet<CopyId>,
    authority_view: AuthorityView,
    retention_millis: u64,
}

impl StoredPlanV1 {
    /// This plan as today's. The build that stored it wrote no note and named no prior members
    /// in its plan, so `prior` says "not known": F1 judges against the plan's own members and
    /// warns, as it does for a layout 3 record.
    fn with(
        self,
        must_hold: std::collections::BTreeSet<CopyId>,
        prior: Option<StoredPriorV3>,
    ) -> RecoveryPlan {
        let prior = prior.map_or_else(
            || PriorRecord {
                members: None,
                note: CommittedWithout::default(),
                ledger: rdb_core::contracts::recovery::TailLedger::unknown(),
            },
            |prior| PriorRecord {
                members: prior.members,
                note: prior.note,
                ledger: rdb_core::contracts::recovery::TailLedger::unknown(),
            },
        );
        RecoveryPlan {
            anchor: self.anchor,
            config: self.config,
            candidates: self.candidates,
            rebuild_required: self.rebuild_required,
            authority_view: self.authority_view,
            retention_millis: self.retention_millis,
            must_hold,
            prior,
            release: rdb_core::contracts::recovery::TailRelease::default(),
        }
    }
}

/// The stored plan before `prior` (version `2`): version `1` and then `must_hold`. Nesting
/// changes nothing on the wire, since postcard writes a struct as its fields in order.
#[derive(serde::Deserialize)]
struct StoredPlanV2 {
    v1: StoredPlanV1,
    must_hold: std::collections::BTreeSet<CopyId>,
}

/// [`PriorRecord`] as version `3` stored it: before its ledger (issue #105).
#[derive(serde::Deserialize)]
struct StoredPriorV3 {
    members: Option<Vec<rdb_core::contracts::recovery::PriorMember>>,
    note: CommittedWithout,
}

/// The stored plan before the ledger and `release` (version `3`): version `2`, then `prior` as
/// it was. Its ledger reads as unknown, which over-holds and never under-holds: the plan was
/// stored by a build that wrote none.
#[derive(serde::Deserialize)]
struct StoredPlanV3 {
    v2: StoredPlanV2,
    prior: StoredPriorV3,
}

/// The `operations/` key of the plan the promote that fenced `partitions/1` at `revision` stored
/// (A-R447): `operations/{PROMOTE_OPERATION_BASE + revision}`.
///
/// # Errors
///
/// A text, when `revision` does not fit below the tag.
pub(crate) fn promote_operation(revision: Revision) -> Result<ControlKey, String> {
    if revision.0 >= 1 << 48 {
        return Err(format!(
            "revision {} is past the promote operation range",
            revision.0
        ));
    }
    Ok(ControlKey::Operation(OperationId(
        PROMOTE_OPERATION_BASE + revision.0,
    )))
}

/// A promote plan as stored: [`PROMOTE_PLAN_VERSION`], then the plan in postcard, the codec the
/// links already use.
pub(crate) fn encode_promote_plan(plan: &RecoveryPlan) -> Bytes {
    let mut bytes = vec![PROMOTE_PLAN_VERSION];
    bytes.extend(postcard::to_stdvec(plan).expect("a recovery plan serialises"));
    Bytes::from(bytes)
}

/// The inverse of [`encode_promote_plan`], exactly: another version byte, or bytes left over
/// after the plan, is refused.
///
/// # Errors
///
/// A text saying which.
pub(crate) fn decode_promote_plan(bytes: &[u8]) -> Result<RecoveryPlan, String> {
    let Some((&version, body)) = bytes.split_first() else {
        return Err("the record is empty".to_owned());
    };
    let (plan, rest) = match version {
        PROMOTE_PLAN_VERSION => {
            postcard::take_from_bytes::<RecoveryPlan>(body).map_err(|error| error.to_string())?
        }
        1 => {
            let (old, rest) = postcard::take_from_bytes::<StoredPlanV1>(body)
                .map_err(|error| error.to_string())?;
            (old.with(std::collections::BTreeSet::new(), None), rest)
        }
        2 => {
            let (old, rest) = postcard::take_from_bytes::<StoredPlanV2>(body)
                .map_err(|error| error.to_string())?;
            (old.v1.with(old.must_hold, None), rest)
        }
        3 => {
            let (old, rest) = postcard::take_from_bytes::<StoredPlanV3>(body)
                .map_err(|error| error.to_string())?;
            (old.v2.v1.with(old.v2.must_hold, Some(old.prior)), rest)
        }
        _ => {
            return Err(format!(
                "plan version {version}, this build reads 1, 2, 3 and {PROMOTE_PLAN_VERSION}"
            ))
        }
    };
    if !rest.is_empty() {
        return Err(format!("{} bytes after the plan", rest.len()));
    }
    Ok(plan)
}

/// Promote step 6 (A-R447): create `plan` at [`promote_operation`]`(fenced_at)`. When the create
/// is not reported applied (it conflicts, or its reply is lost), read it back and go on only when
/// it holds exactly these bytes. Anything else is refused: promote again, which fences at a new
/// revision. The node that commits reads its plan from here.
async fn store_plan(
    store: &dyn ConfigStore,
    partition: PartitionId,
    fenced_at: Revision,
    plan: &RecoveryPlan,
) -> Result<(), PromoteError> {
    let key = promote_operation(fenced_at)
        .map_err(|error| PromoteError::Control(format!("{error}; promote again")))?
        .encode();
    let value = encode_promote_plan(plan);
    let created = store
        .put(PutRequest {
            key: Bytes::from(key.clone()),
            value: value.clone(),
            expected_mod_revision: Some(0),
            dedup: None,
        })
        .await;
    match created {
        Ok(response) if response.outcome == MutationOutcome::Applied => {
            tracing::info!(key = %key, revision = response.revision, "promote_plan_stored");
            return Ok(());
        }
        Ok(_) | Err(ConfigError::Conflict { .. } | ConfigError::DeadlineExceededUnknownOutcome) => {
        }
        Err(error @ ConfigError::PermissionDenied { .. }) => {
            return Err(PromoteError::Control(format!(
                "create {key}: {error}; partition {} is fencing and takes no writes until a \
                 promote finishes. A promote needs rEtcd write access to operations/ as well as \
                 partitions/ and grants/: grant it, then promote again",
                partition.0
            )))
        }
        Err(error) => {
            return Err(PromoteError::Control(format!(
                "could not store the plan at {key}: {error}; promote again"
            )))
        }
    }
    let read = store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| {
            PromoteError::Control(format!(
                "get {key} after a create that was not reported applied: {error}; promote again"
            ))
        })?;
    match read.record {
        Some(found) if found.value == value => {
            tracing::info!(key = %key, revision = found.mod_revision, "promote_plan_adopted");
            Ok(())
        }
        Some(found) => Err(PromoteError::Control(format!(
            "{key} holds another plan (revision {}); promote again",
            found.mod_revision
        ))),
        None => Err(PromoteError::Control(format!(
            "{key} is absent after a create that was not reported applied; promote again"
        ))),
    }
}

/// The nodes `members` names, in order.
fn sorted_nodes(members: &[RecordMember]) -> Vec<NodeId> {
    let mut nodes: Vec<NodeId> = members.iter().map(|member| member.node).collect();
    nodes.sort_unstable();
    nodes
}

/// The boot a promote pins a kept member at when no link to it came up (D4, A-R398): `0`,
/// which no process runs, since [`crate::db::next_boot`] starts at 1.
///
/// What it does, by F1's rules (critic-d4 Q1, A-R401 F1):
/// - A dead member never answers F1's inventory query, so `recovery::select` counts only the
///   live copies, and a lone survivor commits `ReadOnly` (`mode_for(1)`): no write is admitted.
///   The pin plays no part in the mode.
/// - A live member pinned here that does answer is counted, but its durability proof carries
///   its real boot, which the barrier never matches to `0`: the barrier ends incomplete and
///   nothing commits. The promote then fails "did not commit"; promoting again pins its real
///   boot. `promote` re-reads the boot after the expiry wait, so a handshake that comes late
///   is taken in that same run.
/// - When the member starts again, it is re-planned at the boot its link reports, by one of two
///   paths. Usually the owner sees `peer_booted` (its route still held the member's old boot)
///   and `rejoin_if_stale` revokes and re-plans, since the live boot is newer than `0`; seen on
///   the A-R401 F2 walk, `ReadOnly` gen 3 to `DegradedRf2` gen 4. When no `PeerBooted` comes (the
///   route never held a boot), the owner's `repair_duty` of the `ReadOnly` commit is the
///   fallback: it probes the member as a pinned regular secondary and the drain re-pins it.
pub(crate) const UNLINKED_BOOT: BootId = BootId(0);

/// The members kept, each at its boot; `to` becomes the primary and a `Shadow` stays one. Polls
/// until `deadline`: a peer's boot arrives with its handshake. `to`'s boot is required. Another
/// member with no handshake after a quarter of the promote's time is dead or cut off: it is
/// pinned at [`UNLINKED_BOOT`] and logged `promote_member_unlinked`, rather than refusing the
/// promote that S2-5's lone survivor needs (D4).
async fn kept_boots(
    ctx: &PromoteCtx,
    planned: &[Member],
    dropped: NodeId,
    deadline: tokio::time::Instant,
) -> Result<Vec<Member>, PromoteError> {
    let kept: Vec<Member> = planned
        .iter()
        .filter(|member| member.node != dropped)
        .map(|member| Member {
            role: if member.node == ctx.to {
                ReplicaRole::Primary
            } else if member.role == ReplicaRole::Shadow {
                ReplicaRole::Shadow
            } else {
                ReplicaRole::RegularSecondary
            },
            ..*member
        })
        .collect();
    let unlinked_after = tokio::time::Instant::now() + ctx.within / 4;
    loop {
        let missing: Vec<u32> = kept
            .iter()
            .filter(|member| (ctx.member_boot)(member.node).is_none())
            .map(|member| member.node.0)
            .collect();
        let target_known = !missing.contains(&ctx.to.0);
        if missing.is_empty() || (target_known && tokio::time::Instant::now() >= unlinked_after) {
            if !missing.is_empty() {
                tracing::warn!(
                    unlinked = ?missing,
                    boot = UNLINKED_BOOT.0,
                    "promote_member_unlinked"
                );
            }
            return Ok(kept
                .into_iter()
                .map(|member| Member {
                    boot: (ctx.member_boot)(member.node).unwrap_or(UNLINKED_BOOT),
                    ..member
                })
                .collect());
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(missing = ?missing, "promote_peers_unknown");
            return Err(PromoteError::PeersUnknown {
                nodes: missing,
                within_ms: ctx.within.as_millis(),
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// What `prior` answers to a look at `ctx.partition` within `ctx.look_within`; `None` when the
/// look cannot be sent or no answer comes in time.
async fn owner_answers(ctx: &PromoteCtx, prior: NodeId) -> Option<NodeStatus> {
    let (reply, answer) = std::sync::mpsc::channel();
    let asked = (ctx.send)(
        prior,
        Msg::Inspect {
            partition: ctx.partition,
            reply,
        },
    );
    if asked.is_err() {
        return None;
    }
    let within = ctx.look_within;
    tokio::task::spawn_blocking(move || answer.recv_timeout(within).ok())
        .await
        .ok()
        .flatten()
}

/// Refuse a promote run on a clock with no bound ([`PromoteError::ClockUnbounded`]), before it
/// writes anything. Logged `promote_clock_unbounded`.
fn refuse_unbounded(ctx: &PromoteCtx) -> Result<(), PromoteError> {
    if ctx
        .clock
        .sample(ctx.clock.now(), &ctx.budgets)
        .bound_established
    {
        return Ok(());
    }
    tracing::warn!(
        to = ctx.to.0,
        partition = ctx.partition.0,
        "promote_clock_unbounded"
    );
    Err(PromoteError::ClockUnbounded)
}

/// Refuse `ctx.to` when its grant is frozen: a promote there would commit, and the new owner
/// could never admit (#61 critic Y3).
async fn refuse_frozen_target(ctx: &PromoteCtx) -> Result<(), PromoteError> {
    let key = ControlKey::Grant(ctx.to).encode();
    let read = ctx
        .store
        .get(GetRequest {
            key: Bytes::from(key.clone()),
        })
        .await
        .map_err(|error| PromoteError::Control(format!("get {key}: {error}; promote again")))?;
    let frozen = match &read.record {
        Some(found) => {
            GrantRecord::decode(&found.value)
                .ok_or_else(|| {
                    PromoteError::Record(format!(
                        "{key} at revision {} is not a grant record",
                        found.mod_revision
                    ))
                })?
                .frozen
        }
        None => false,
    };
    if frozen {
        tracing::warn!(
            to = ctx.to.0,
            partition = ctx.partition.0,
            "promote_target_frozen"
        );
        return Err(PromoteError::TargetFrozen {
            node: ctx.to.0,
            partition: ctx.partition.0,
        });
    }
    Ok(())
}

/// Show that `prior`, which owns `ctx.partition` above one partition, is dead: it does not
/// answer a look, or answers it with a host fault, and its grant is not being renewed and has
/// run out. A just-killed owner is waited out, not refused. Returns the `mod_revision` of the
/// grant record judged, 0 for none.
///
/// The freeze is the node's, so this keeps it to nodes with nothing left to stop (issue #79
/// S3). A host fault is the whole host's and lasts the boot, so a faulted owner has nothing left
/// to stop either (issue #122): it goes on to the grant checks as a silent one does.
///
/// # Errors
///
/// [`PromoteError::OwnerAlive`] when it answers without a fault or its expiry moves;
/// [`PromoteError::Control`] when the store does not answer or the wait runs out. Nothing has
/// been written either way.
async fn owner_dead(
    ctx: &PromoteCtx,
    prior: NodeId,
    deadline: tokio::time::Instant,
) -> Result<u64, PromoteError> {
    let alive = |evidence: &str| PromoteError::OwnerAlive {
        node: prior.0,
        partition: ctx.partition.0,
        evidence: evidence.to_owned(),
    };
    let key = ControlKey::Grant(prior).encode();
    let mut first_expiry = None;
    let mut looked = false;
    loop {
        let read = ctx
            .store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
            .map_err(|error| PromoteError::Control(format!("get {key}: {error}; promote again")))?;
        let found = match read.record {
            Some(found) => {
                let held = GrantRecord::decode(&found.value).ok_or_else(|| {
                    PromoteError::Record(format!(
                        "{key} at revision {} is not a grant record",
                        found.mod_revision
                    ))
                })?;
                Some((found.mod_revision, held))
            }
            None => None,
        };
        // A frozen grant never comes back, so a node that still answers has nothing left to
        // lose: it serves nothing, and its partitions must be able to leave it.
        if let Some((revision, held)) = found.filter(|(_, held)| held.frozen) {
            tracing::info!(
                owner = prior.0,
                grant = held.grant.0,
                "promote_owner_frozen"
            );
            return Ok(revision);
        }
        if !looked {
            looked = true;
            match owner_answers(ctx, prior).await {
                Some(NodeStatus { fault: None, .. }) => {
                    tracing::warn!(owner = prior.0, "promote_owner_answered");
                    return Err(alive("it answered a look"));
                }
                Some(NodeStatus {
                    fault: Some(fault), ..
                }) => {
                    tracing::warn!(owner = prior.0, %fault, "promote_owner_faulted");
                }
                None => {}
            }
        }
        let Some((revision, held)) = found else {
            tracing::info!(owner = prior.0, "promote_owner_no_grant");
            return Ok(0);
        };
        if *first_expiry.get_or_insert(held.expiry_utc_ms) != held.expiry_utc_ms {
            tracing::warn!(
                owner = prior.0,
                grant = held.grant.0,
                "promote_owner_renewing"
            );
            return Err(alive("its grant is still being renewed"));
        }
        // The guarded sample, never `control_time`: a clock with no bound proves no expiry,
        // so a fast caller cannot read a live owner's grant as run out (PR #103 review B2).
        let now = ctx.clock.now();
        let mut view = ClockView::new();
        view.accept(ctx.clock.sample(now, &ctx.budgets));
        if expiry_proven(&view, held.expiry_utc_ms, now, &ctx.budgets).is_some() {
            tracing::info!(
                owner = prior.0,
                expiry = held.expiry_utc_ms,
                "promote_owner_dead"
            );
            return Ok(revision);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(PromoteError::Control(format!(
                "the expiry of grants/{} was not proven past within {} seconds; nothing was \
                 changed: promote again",
                prior.0,
                seconds_in_words(ctx.within.as_millis())
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Freeze `grants/{prior}`: CAS it `frozen` at the revision read, or create it frozen when it is
/// absent, with `GrantId(0)` ("none held"), the prior owner's boot and control time now as `E`.
/// A record already frozen is kept. Returns the frozen record and the revision of the write or
/// of the read that found it frozen.
///
/// `dead_at`, above one partition, is the grant revision [`owner_dead`] judged. A record that
/// is not frozen and is no longer at it was written by an owner that came back:
/// [`PromoteError::OwnerAlive`], with nothing frozen.
async fn freeze(
    ctx: &PromoteCtx,
    prior: NodeId,
    prior_boot: BootId,
    dead_at: Option<u64>,
    deadline: tokio::time::Instant,
) -> Result<(GrantRecord, Revision), PromoteError> {
    let key = ControlKey::Grant(prior).encode();
    let returned = || PromoteError::OwnerAlive {
        node: prior.0,
        partition: ctx.partition.0,
        evidence: "it came back during the promote".to_owned(),
    };
    loop {
        let read = ctx
            .store
            .get(GetRequest {
                key: Bytes::from(key.clone()),
            })
            .await
            .map_err(|error| PromoteError::Control(format!("get {key}: {error}; promote again")))?;
        let (frozen, expected, created) = match &read.record {
            Some(found) => {
                let held = GrantRecord::decode(&found.value).ok_or_else(|| {
                    PromoteError::Record(format!(
                        "{key} at revision {} is not a grant record",
                        found.mod_revision
                    ))
                })?;
                if held.frozen {
                    tracing::info!(
                        grant = held.grant.0,
                        revision = found.mod_revision,
                        "promote_frozen_already"
                    );
                    return Ok((held, Revision(read.read_revision)));
                }
                if dead_at.is_some_and(|judged| judged != found.mod_revision) {
                    return Err(returned());
                }
                (
                    GrantRecord {
                        frozen: true,
                        ..held
                    },
                    found.mod_revision,
                    false,
                )
            }
            None => {
                if dead_at.is_some_and(|judged| judged != 0) {
                    return Err(returned());
                }
                let now = ctx.clock.now();
                let sample = ctx.clock.sample(now, &ctx.budgets);
                (
                    GrantRecord {
                        grant: GrantId(0),
                        node: prior,
                        boot: prior_boot,
                        authority_generation: AuthorityGeneration::default(),
                        expiry_utc_ms: i64::try_from(sample.estimate.0).unwrap_or(i64::MAX),
                        frozen: true,
                    },
                    0,
                    true,
                )
            }
        };
        match cas(&*ctx.store, &key, frozen.encode(), expected).await {
            Ok(revision) => {
                tracing::info!(
                    grant = frozen.grant.0,
                    created,
                    revision = revision.0,
                    expiry = frozen.expiry_utc_ms,
                    "promote_frozen"
                );
                return Ok((frozen, revision));
            }
            // A renewal landed between the read and the freeze: read again.
            Err(error) if tokio::time::Instant::now() < deadline => {
                tracing::info!(%error, "promote_freeze_retry");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

pub mod membership;
pub(crate) mod rebuild;

pub use membership::{add_node, watch_members, AddNodeError};

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::test_store::{Script, Scripted};

    fn record(generation: u64) -> PartitionRecord {
        PartitionRecord {
            partition: PARTITION,
            owner: OWNER,
            generation: Generation(generation),
            owner_epoch: OwnerEpoch(0),
            config_version: CONFIG_VERSION,
            lifecycle: PartitionLifecycle::Serving,
        }
    }

    fn put(store: &Arc<dyn ConfigStore>, rt: &tokio::runtime::Runtime, value: Bytes, cas: u64) {
        let response = rt
            .block_on(store.put(PutRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
                value,
                expected_mod_revision: Some(cas),
                dedup: None,
            }))
            .expect("put");
        assert_eq!(response.outcome, MutationOutcome::Applied);
    }

    /// A `Db` whose owner restarted (boot 2, no thread) with `grants/1` left by its boot-1
    /// process, long expired and not frozen: every guard but the store's would clear it.
    /// Returns the revision the record was written at.
    fn restarted_owner(
        store: &dyn ConfigStore,
        rt: &tokio::runtime::Runtime,
    ) -> (Db, std::sync::mpsc::Receiver<Msg>, u64) {
        let (mut owner, mailbox) = crate::host::NodeHandle::unanswered(OWNER);
        owner.boot = rdb_core::contracts::ids::BootId(BOOT.0 + 1);
        let held: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let budgets = rdb_core::contracts::event::Budgets::SPEC_DEFAULTS;
        let db = Db::by_hand(
            vec![owner],
            held,
            rt.handle(),
            crate::db::Timeouts::default(),
            Some((std::path::PathBuf::new(), budgets)),
        );
        let old = GrantRecord {
            grant: GrantId(1),
            node: OWNER,
            boot: BOOT,
            authority_generation: AuthorityGeneration(0),
            expiry_utc_ms: 0,
            frozen: false,
        };
        let written = rt
            .block_on(store.put(PutRequest {
                key: Bytes::from(ControlKey::Grant(OWNER).encode()),
                value: old.encode(),
                expected_mod_revision: Some(0),
                dedup: None,
            }))
            .expect("put grants/1");
        (db, mailbox, written.revision)
    }

    fn grant_revision(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime) -> Option<u64> {
        rt.block_on(store.get(GetRequest {
            key: Bytes::from(ControlKey::Grant(OWNER).encode()),
        }))
        .expect("get grants/1")
        .record
        .map(|found| found.mod_revision)
    }

    /// Message 6, round-3 mutant M2: the delete names the revision `clear_grant` judged. A write
    /// to `grants/1` between that read and the delete (here, while the `partitions/` list is
    /// held) makes the delete lose: `Conflict`, and the moved record stays. The control row:
    /// the same record with nothing in between clears.
    /// Unit (~20 ms): `MemStore` behind the scripted list.
    #[test]
    fn a_grant_that_moves_after_it_was_judged_is_not_deleted() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let store = Scripted::new(Script {
            hold_lists: 1,
            ..Script::default()
        });
        let (db, _mailbox, written) = restarted_owner(&store, &rt);
        let moved = rt.block_on(async {
            let move_it = async {
                store.list_held.notified().await;
                let key = Bytes::from(ControlKey::Grant(OWNER).encode());
                let current = store.mem().get(GetRequest { key: key.clone() }).await;
                let value = current.expect("get").record.expect("grants/1").value;
                let moved = store
                    .mem()
                    .put(PutRequest {
                        key,
                        value,
                        expected_mod_revision: Some(written),
                        dedup: None,
                    })
                    .await
                    .expect("move grants/1");
                store.release_lists();
                moved.revision
            };
            let (clearance, moved) = tokio::join!(clear_grant(&db, &store, OWNER), move_it);
            assert_eq!(clearance, Ok(Clearance::Conflict));
            moved
        });
        assert_eq!(
            grant_revision(&store, &rt),
            Some(moved),
            "the moved record stays"
        );

        let store = config_testkit::MemStore::new();
        let (db, _mailbox, written) = restarted_owner(&store, &rt);
        assert!(
            matches!(rt.block_on(clear_grant(&db, &store, OWNER)), Ok(Clearance::Cleared(at)) if at.0 > written)
        );
        assert_eq!(grant_revision(&store, &rt), None);
    }

    /// Message 6, round-3 mutant M3: a `partitions/` list that fails or comes back truncated is
    /// never read as complete, since a missing record could hide a partition in transfer. Each
    /// is `UNAVAILABLE` ending "nothing deleted", and `grants/1` stays.
    /// Unit (~20 ms).
    #[test]
    fn a_failed_or_truncated_partition_list_deletes_nothing() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let failing = Scripted::new(Script {
            fail_lists: 1,
            ..Script::default()
        });
        let short = config_testkit::MemStore::with_limits(config_core::Limits {
            max_list_items: 1,
            ..config_core::Limits::DEFAULT
        });
        for id in [1, 2] {
            let mut serving = record(1);
            serving.partition = PartitionId(id);
            put_at(
                &short,
                &rt,
                ControlKey::Partition(PartitionId(id)),
                serving.encode(),
            );
        }
        let stores: [(&str, &dyn ConfigStore); 2] = [("failed", &failing), ("truncated", &short)];
        for (case, store) in stores {
            let (db, _mailbox, written) = restarted_owner(store, &rt);
            let error = rt.block_on(clear_grant(&db, store, OWNER)).expect_err(case);
            assert_eq!(error.kind, ErrorKind::Unavailable, "{case}: {error:?}");
            assert!(
                error.detail.ends_with("; nothing deleted"),
                "{case}: {error:?}"
            );
            assert_eq!(grant_revision(store, &rt), Some(written), "{case}");
        }
    }

    /// Msg 8, F-012: a refusal before the delete says nothing was mutated, and only a transient
    /// store error says a jittered retry helps. A permission error and a `grants/1` that does
    /// not decode stay the same however often they are retried, so they answer `NotWired`; an
    /// unavailable store answers `BoundedJitter`. Each leaves `grants/1` as it was.
    /// Unit (~20 ms).
    #[test]
    fn a_clear_refused_before_the_delete_says_no_mutation_and_retries_only_transient_errors() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        for (case, error, retry) in [
            (
                "permission",
                ConfigError::PermissionDenied {
                    detail: "no grant on grants/".to_owned(),
                },
                RetryRule::NotWired,
            ),
            (
                "unavailable",
                ConfigError::Unavailable {
                    reason: "down".to_owned(),
                },
                RetryRule::BoundedJitter,
            ),
        ] {
            let store = config_testkit::MemStore::new();
            let (db, _mailbox, written) = restarted_owner(&store, &rt);
            store.failing_with(error);
            let refused = rt
                .block_on(clear_grant(&db, &store, OWNER))
                .expect_err(case);
            store.stop_failing();
            assert_eq!(
                (refused.kind, refused.retry, refused.no_mutation),
                (ErrorKind::Unavailable, retry, true),
                "{case}: {refused:?}"
            );
            assert!(
                refused.detail.ends_with("nothing deleted"),
                "{case}: {refused:?}"
            );
            assert_eq!(grant_revision(&store, &rt), Some(written), "{case}");
        }

        let store = config_testkit::MemStore::new();
        let (db, _mailbox, _) = restarted_owner(&store, &rt);
        let key = Bytes::from(ControlKey::Grant(OWNER).encode());
        let found = rt
            .block_on(store.get(GetRequest { key: key.clone() }))
            .expect("get")
            .record
            .expect("grants/1");
        let garbled = rt
            .block_on(store.put(PutRequest {
                key,
                value: Bytes::from_static(b"not a grant record"),
                expected_mod_revision: Some(found.mod_revision),
                dedup: None,
            }))
            .expect("put")
            .revision;
        let refused = rt
            .block_on(clear_grant(&db, &store, OWNER))
            .expect_err("does not decode");
        assert_eq!(
            (refused.kind, refused.retry, refused.no_mutation),
            (ErrorKind::Unavailable, RetryRule::NotWired, true),
            "{refused:?}"
        );
        assert_eq!(grant_revision(&store, &rt), Some(garbled));
    }

    /// Review F-024 (mutant U4): a delete the store refused before proposing it, `Unavailable`
    /// or `NotLeader`, deleted nothing, and says so; any other delete error may have reached
    /// the store, so it says `no_mutation=false` and to run it again to see. Only the transient
    /// ones say a jittered retry helps. Unit (~20 ms): the scripted store fails the delete only.
    #[test]
    fn a_delete_the_store_refused_before_proposing_says_nothing_deleted_and_no_other_does() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        for (case, error, retry, no_mutation) in [
            (
                "unavailable",
                ConfigError::Unavailable {
                    reason: "down".to_owned(),
                },
                RetryRule::BoundedJitter,
                true,
            ),
            (
                "not leader",
                ConfigError::NotLeader { hint: None },
                RetryRule::BoundedJitter,
                true,
            ),
            (
                "permission",
                ConfigError::PermissionDenied {
                    detail: "no delete on grants/".to_owned(),
                },
                RetryRule::NotWired,
                false,
            ),
        ] {
            let store = Scripted::new(Script {
                fail_delete_with: Some(error),
                ..Script::default()
            });
            let (db, _mailbox, written) = restarted_owner(&store, &rt);
            let refused = rt
                .block_on(clear_grant(&db, &store, OWNER))
                .expect_err(case);
            assert_eq!(
                (refused.kind, refused.retry, refused.no_mutation),
                (ErrorKind::Unavailable, retry, no_mutation),
                "{case}: {refused:?}"
            );
            assert!(
                refused.detail.contains("delete grants/1"),
                "{case}: {refused:?}"
            );
            let tail = if no_mutation {
                "nothing deleted"
            } else {
                "the delete may have applied; run it again to read where it stands"
            };
            assert!(refused.detail.ends_with(tail), "{case}: {refused:?}");
            assert_eq!(grant_revision(&store, &rt), Some(written), "{case}");
        }
    }

    fn put_at(
        store: &dyn ConfigStore,
        rt: &tokio::runtime::Runtime,
        key: ControlKey,
        value: Bytes,
    ) {
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(key.encode()),
            value,
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put");
    }

    /// F-004 (b): only an untouched record of this bootstrap's own is adopted. A record that
    /// has been written since it was created, or one at a later generation (what F1's commit
    /// leaves), is a partition with a history: the bootstrap is refused with the record's
    /// revision and sends node 1 nothing, so it never starts a second generation-0 recovery.
    #[test]
    fn a_second_bootstrap_over_a_changed_record_is_refused_and_sends_nothing() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let sent = std::sync::Mutex::new(0u32);
        let send = |_: Msg| -> Result<(), NodeStopped> {
            *sent.lock().expect("count") += 1;
            Ok(())
        };

        // Rewritten with the same bytes: `mod_revision` moved past `create_revision`.
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let first = rt
            .block_on(bootstrap(&store, Tick(0), send))
            .expect("first bootstrap");
        assert_eq!(*sent.lock().expect("count"), 2, "Plan and FenceProven");
        put(&store, &rt, record(0).encode(), first.0);
        match rt.block_on(bootstrap(&store, Tick(0), send)) {
            Err(BootstrapError::AlreadyExists { current, .. }) => assert!(current > first.0),
            other => panic!("over a rewritten record: {other:?}"),
        }
        assert_eq!(*sent.lock().expect("count"), 2, "the refusal sent nothing");

        // Created once, untouched since, but at generation 1.
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        put(&store, &rt, record(1).encode(), 0);
        match rt.block_on(bootstrap(&store, Tick(0), send)) {
            Err(BootstrapError::AlreadyExists { .. }) => {}
            other => panic!("over a generation-1 record: {other:?}"),
        }
        assert_eq!(*sent.lock().expect("count"), 2, "the refusal sent nothing");
    }

    /// F-004: the create applies but its reply is lost. The bootstrap reads the record back,
    /// finds its own, untouched, and carries on at that record's revision. When the read-back
    /// fails as well, the bootstrap fails with `Control` and sends nothing; the next open
    /// adopts the record (the `Db` row covers that retry). (c) When the create never applied,
    /// the read-back finds nothing: `Control` again, nothing sent, and the next open creates it.
    #[test]
    fn a_bootstrap_whose_create_reply_was_lost_adopts_the_record_it_wrote() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let sent = std::sync::Mutex::new(0u32);
        let send = |_: Msg| -> Result<(), NodeStopped> {
            *sent.lock().expect("count") += 1;
            Ok(())
        };

        let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            lose_put_replies: 1,
            ..Script::default()
        }));
        let adopted = rt
            .block_on(bootstrap(&store, Tick(0), send))
            .expect("the lost create is adopted");
        let key = Bytes::from(ControlKey::Partition(PARTITION).encode());
        let found = rt
            .block_on(store.get(GetRequest { key }))
            .expect("get")
            .record
            .expect("partitions/1");
        assert_eq!(
            (adopted.0, found.value),
            (found.mod_revision, record(0).encode())
        );
        assert_eq!(*sent.lock().expect("count"), 2, "Plan and FenceProven");

        let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            lose_put_replies: 1,
            fail_gets: 1,
            ..Script::default()
        }));
        match rt.block_on(bootstrap(&store, Tick(0), send)) {
            Err(BootstrapError::Control(_)) => {}
            other => panic!("the read-back failed: {other:?}"),
        }
        assert_eq!(*sent.lock().expect("count"), 2, "the failure sent nothing");

        // (c) The reply is lost and the create never applied: the read-back finds no record.
        // That is not a history to refuse, and not a record to adopt (critic C-1).
        let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            drop_puts: 1,
            ..Script::default()
        }));
        match rt.block_on(bootstrap(&store, Tick(0), send)) {
            Err(BootstrapError::Control(_)) => {}
            other => panic!("(c) the create never applied: {other:?}"),
        }
        assert_eq!(
            *sent.lock().expect("count"),
            2,
            "(c) the failure sent nothing"
        );
    }

    // ---- case B: the heal with no grant held (M10 arch R3a, rulings A-R159, A-R163, A-R171) ----

    /// The boot the reopen in these rows runs at: open 2's first (arch R3b).
    const OPEN_2: BootId = BootId((1 << 16) + 1);

    /// `partitions/1` as a layout-3 record at generation 3, owner epoch 3, whose owner boot is
    /// `owner_boot`. Returns the revision it was written at.
    fn rooted(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime, owner_boot: BootId) -> u64 {
        let record = PartitionRecord {
            generation: Generation(3),
            owner_epoch: OwnerEpoch(3),
            ..record(3)
        };
        let root = PartitionRoot {
            mode: rdb_core::authority::partition::RecordMode::Active,
            owner_boot,
            base_seq: Seq(7),
            base_digest: Digest::ROOT,
            predecessor_generation: Generation(2),
            predecessor_cutoff: Seq(7),
            instance: [7; INSTANCE_LEN],
            run: Revision(1),
        };
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            value: record.encode_with(&root),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put partitions/1")
        .revision
    }

    /// Run one heal over `store` and return its answer and every message node 1 received.
    fn heal_once(
        store: Arc<dyn ConfigStore>,
        rt: &tokio::runtime::Runtime,
    ) -> (Result<Healed, HealError>, Vec<Msg>) {
        heal_once_within(store, rt, Duration::from_millis(500))
    }

    /// [`heal_once`] with the heal's time set to `within`.
    fn heal_once_within(
        store: Arc<dyn ConfigStore>,
        rt: &tokio::runtime::Runtime,
        within: Duration,
    ) -> (Result<Healed, HealError>, Vec<Msg>) {
        heal_over(store, rt, within, vec![PARTITION])
    }

    /// [`heal_once_within`] over `partitions`.
    fn heal_over(
        store: Arc<dyn ConfigStore>,
        rt: &tokio::runtime::Runtime,
        within: Duration,
        partitions: Vec<PartitionId>,
    ) -> (Result<Healed, HealError>, Vec<Msg>) {
        heal_forming(store, rt, within, partitions, Vec::new())
    }

    /// [`heal_over`] with `bootstrap`, the partitions this owner still has to form.
    fn heal_forming(
        store: Arc<dyn ConfigStore>,
        rt: &tokio::runtime::Runtime,
        within: Duration,
        partitions: Vec<PartitionId>,
        bootstrap: Vec<(PartitionId, Revision)>,
    ) -> (Result<Healed, HealError>, Vec<Msg>) {
        let (owner, mailbox) = std::sync::mpsc::channel();
        let ctx = HealCtx {
            partition_count: 1,
            nodes: crate::db::ALL_NODES.into(),
            store,
            boot: OPEN_2,
            budgets: Budgets::SPEC_DEFAULTS,
            clock: HostClock::start(),
            owner,
            within,
            member_boot: Arc::new(|_| Some(OPEN_2)),
            owner_node: OWNER,
            partitions,
            bootstrap,
        };
        let healed = rt.block_on(heal(ctx));
        (healed, mailbox.try_iter().collect())
    }

    /// Issue #79: a copy of `partitions/1` as `partition`, owned by `owner`.
    fn rooted_copy(
        store: &dyn ConfigStore,
        rt: &tokio::runtime::Runtime,
        partition: PartitionId,
        owner: NodeId,
    ) {
        let value = rt
            .block_on(store.get(GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("get partitions/1")
            .record
            .expect("partitions/1")
            .value;
        let root = PartitionRoot::decode(&value).expect("a root");
        let record = PartitionRecord {
            partition,
            owner,
            ..PartitionRecord::decode(&value).expect("a record")
        };
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(ControlKey::Partition(partition).encode()),
            value: record.encode_with(&root),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put the copy");
    }

    /// Issue #79, the single-heal rule: a node has one grant, so a second heal for the same owner
    /// meets the grant the first heal's partition already took, this open's own boot, and is
    /// refused `NotCleared`. One heal over both partitions clears it once and plans both. Cut:
    /// `clear_until` accepts a grant naming this open's boot. Unit (~300 ms): the heal's bound.
    #[test]
    fn a_second_heal_for_one_owner_meets_the_grant_the_first_one_led_to() {
        let rt = runtime();
        let four = PartitionId(4);
        let within = Duration::from_millis(300);
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        rooted_copy(&*store, &rt, four, OWNER);

        let (first, sent) = heal_over(Arc::clone(&store), &rt, within, vec![PARTITION]);
        first.expect("the first partition's heal");
        assert_eq!(sent.len(), 2, "a Plan and a proof");

        // What that partition's F1 does next: its node takes the grant, at this open's boot.
        let grants = Bytes::from(ControlKey::Grant(OWNER).encode());
        let taken = GrantRecord {
            grant: GrantId(1),
            node: OWNER,
            boot: OPEN_2,
            authority_generation: AuthorityGeneration(0),
            expiry_utc_ms: 0,
            frozen: false,
        };
        rt.block_on(store.put(PutRequest {
            key: grants.clone(),
            value: taken.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("the grant is taken");

        let (second, sent) = heal_over(Arc::clone(&store), &rt, within, vec![four]);
        assert!(
            matches!(second, Err(HealError::NotCleared { node: 1, .. })),
            "{second:?}"
        );
        assert!(sent.is_empty(), "{} messages sent", sent.len());

        rt.block_on(store.delete(DeleteRequest {
            key: grants,
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("the grant is gone");
        let (one, sent) = heal_over(store, &rt, within, vec![PARTITION, four]);
        one.expect("one heal over both");
        assert_eq!(sent.len(), 4, "a Plan and a proof each");
    }

    /// PR #102 review: a truncated `partitions/` list is never read as complete, even when every
    /// requested partition came back. Only the heal's first list read, the one `pinned_members`
    /// makes before the grant is looked at, comes back truncated; every later list is whole. The
    /// heal stops there: a `Control` error, nothing sent, and the grant and partition records
    /// unchanged. Cut: the `truncated` check in `heal_records`, and the later reads go on, the
    /// grant is deleted and the heal plans both partitions. Unit: `MemStore` underneath.
    #[test]
    fn a_truncated_partition_list_fails_the_heal_before_the_grant_is_read() {
        let rt = runtime();
        let short = Arc::new(FirstListShort {
            inner: config_testkit::MemStore::new(),
            lists: std::sync::atomic::AtomicU32::new(0),
        });
        let store: Arc<dyn ConfigStore> = short.clone();
        rooted(&*store, &rt, BootId(5));
        rooted_copy(&*store, &rt, PartitionId(2), OWNER);
        let granted = old_grant(&*store, &rt, GrantId(4));
        let read_all = || {
            [
                ControlKey::Grant(OWNER),
                ControlKey::Partition(PARTITION),
                ControlKey::Partition(PartitionId(2)),
            ]
            .map(|key| {
                let record = rt
                    .block_on(store.get(GetRequest {
                        key: Bytes::from(key.encode()),
                    }))
                    .expect("get")
                    .record
                    .expect("the record is there");
                (record.mod_revision, record.value)
            })
        };
        let before = read_all();
        let (answer, sent) = heal_over(
            Arc::clone(&store),
            &rt,
            Duration::from_millis(500),
            vec![PARTITION, PartitionId(2)],
        );
        let detail = match answer {
            Err(HealError::Control(detail)) => detail,
            other => panic!("a truncated list is a control error: {other:?}"),
        };
        assert!(detail.contains("truncated"), "{detail}");
        assert_eq!(
            short.lists.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the heal stopped at its first list"
        );
        assert!(sent.is_empty(), "{} messages sent", sent.len());
        assert_eq!(
            read_all(),
            before,
            "the grant and the records are unchanged"
        );
        assert_eq!(before[0].0, granted);
    }

    /// A control store whose first `list` answers every record but says it is truncated; every
    /// later list is whole. Everything else is the `MemStore` underneath.
    struct FirstListShort {
        inner: config_testkit::MemStore,
        lists: std::sync::atomic::AtomicU32,
    }

    impl ConfigStore for FirstListShort {
        fn get<'a, 'b>(&'a self, request: GetRequest) -> Reply<'b, config_core::GetResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.get(request)
        }

        fn list<'a, 'b>(&'a self, request: ListRequest) -> Reply<'b, config_core::ListResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            let first = self
                .lists
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                == 0;
            let listed = self.inner.list(request);
            Box::pin(async move {
                let mut listed = listed.await?;
                listed.truncated |= first;
                Ok(listed)
            })
        }

        fn put<'a, 'b>(&'a self, request: PutRequest) -> Reply<'b, config_core::MutationResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.put(request)
        }

        fn delete<'a, 'b>(
            &'a self,
            request: DeleteRequest,
        ) -> Reply<'b, config_core::MutationResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.delete(request)
        }

        fn capabilities(&self) -> config_core::Capabilities {
            self.inner.capabilities()
        }

        fn watch<'a, 'b>(
            &'a self,
            request: config_core::WatchRequest,
        ) -> Reply<'b, config_core::WatchStream>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.watch(request)
        }
    }

    /// Issue #79: one heal covers every partition its owner holds. Each gets a plan and a proof;
    /// a later record that is wrong or missing refuses the whole heal before any is sent. Cut:
    /// `heal` sends each partition's plan as it validates it. Unit: `MemStore`.
    #[test]
    fn a_heal_over_two_partitions_validates_both_before_it_sends_anything() {
        let rt = runtime();
        let two = PartitionId(2);
        let within = Duration::from_millis(500);

        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        rooted_copy(&*store, &rt, two, OWNER);
        let (healed, sent) = heal_over(Arc::clone(&store), &rt, within, vec![PARTITION, two]);
        healed.expect("both heal");
        assert_eq!(sent.len(), 4, "a Plan and a proof each");

        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        rooted_copy(&*store, &rt, two, NodeId(2));
        let (healed, sent) = heal_over(Arc::clone(&store), &rt, within, vec![PARTITION, two]);
        match healed {
            Err(HealError::Record(text)) => assert!(text.contains("partition 2"), "{text}"),
            other => panic!("{other:?}"),
        }
        assert!(sent.is_empty(), "{} messages sent", sent.len());

        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let (healed, sent) = heal_over(Arc::clone(&store), &rt, within, vec![PARTITION, two]);
        assert!(healed.is_err(), "a partition with no record");
        assert!(sent.is_empty(), "{} messages sent", sent.len());
    }

    /// #80 S1b (risk: K = 3 bytes or members move): over nodes 1 to 3 the ring places every
    /// partition a database may have exactly as before S1b. `placed` is `members_for`,
    /// `owner_in` is `initial_owner`, and each generation-0 record is byte for byte the record
    /// the pre-S1b `initial_record` wrote (its owner `initial_owner`), so a store declared or
    /// formed before S1b still passes `declare_preflight` and adopts. Two records are pinned as
    /// literal layout-2 bytes: partition 5 over nodes 1 to 3 (owner 2) and over nodes 1 to 5
    /// (owner 5). Unit.
    /// Cut: `placed` numbers copies from 0 in ring order instead of `node - 1`; partition 2's
    /// members differ, red.
    #[test]
    fn s1b_placement_over_nodes_one_to_three_is_unchanged_and_its_records_keep_their_bytes() {
        let three = crate::db::ALL_NODES;
        for p in 1..=rdb_core::partition_map::MAX_PARTITIONS {
            let partition = PartitionId(p);
            assert_eq!(
                placed(&three, partition, BOOT),
                members_for(partition, BOOT)
            );
            assert_eq!(owner_in(&three, partition), initial_owner(partition));
            let before = PartitionRecord {
                partition,
                owner: initial_owner(partition),
                generation: Generation(0),
                owner_epoch: OwnerEpoch(0),
                config_version: CONFIG_VERSION,
                lifecycle: PartitionLifecycle::Serving,
            };
            assert_eq!(
                initial_record(&three, partition).encode(),
                before.encode(),
                "partition {p}"
            );
        }
        let five: Vec<NodeId> = (1..=5).map(NodeId).collect();
        let layout_2 = |owner: u8| {
            let mut bytes = vec![2, 5, 0, 0, 0, owner, 0, 0, 0];
            bytes.extend_from_slice(&[0; 8]); // generation 0
            bytes.extend_from_slice(&[0; 8]); // owner epoch 0
            bytes.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0]); // config version 1
            bytes.push(0); // Serving
            bytes
        };
        assert_eq!(
            initial_record(&three, PartitionId(5)).encode().as_ref(),
            layout_2(2)
        );
        assert_eq!(
            initial_record(&five, PartitionId(5)).encode().as_ref(),
            layout_2(5)
        );
    }

    /// PR #138 review: the formation record and the declaration's reservation are written once
    /// and read by every later build, so their keys and layout-1 bytes are pinned as literals, as
    /// are the key and bytes of the counter a declaration creates (r5):
    /// changing `encode` and `decode` together cannot pass while old stores stop opening. A wrong
    /// version byte, a count cut short and a trailing byte each decode as nothing. Unit.
    /// #80 S1b moved the reservation to layout 2 (pinned by
    /// `s1b_reservation_layout_2_bytes_are_pinned`): layout 1 is now only read, so this row
    /// decodes its literal, as an S1a build wrote it, and no longer encodes it.
    #[test]
    fn s1a_formation_and_reservation_layout_1_bytes_are_pinned() {
        assert_eq!(
            ControlKey::Operation(FORMATION_OPERATION).encode(),
            "operations/63050394783186944"
        );
        assert_eq!(
            ControlKey::Operation(RESERVATION_OPERATION).encode(),
            "operations/63050394783186945"
        );
        let nodes = vec![NodeId(1), NodeId(2), NodeId(3)];
        let formation = Formation {
            instance: [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                0x0f, 0x10,
            ],
            partitions: 8,
            nodes: nodes.clone(),
        };
        let formation_bytes: [u8; 33] = [
            1, // layout
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10, // instance
            8, 0, 0, 0, // partitions
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // nodes
        ];
        assert_eq!(formation.encode().as_ref(), formation_bytes);
        assert_eq!(Formation::decode(&formation_bytes), Some(formation));
        let reservation_bytes: [u8; 17] = [
            1, // layout
            8, 0, 0, 0, // partitions
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, // nodes
        ];
        assert_eq!(
            Declaration::decode_reservation(&reservation_bytes),
            Some(Reservation {
                partitions: 8,
                nodes,
                records: None
            })
        );
        // PR #138 review r5: the counter `declare` creates, under the formation's instance, is
        // counter layout 2 at N = 0, nothing activated; the first open's CAS reads it as such.
        assert_eq!(
            instance_key(
                &formation_bytes[1..=INSTANCE_LEN]
                    .try_into()
                    .expect("16 bytes")
            ),
            "rdb/instances/0102030405060708090a0b0c0d0e0f10"
        );
        let no_opens: [u8; 17] = [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(encode_count(NO_OPENS).as_ref(), no_opens);
        assert_eq!(decode_count(&no_opens), Some(NO_OPENS));

        let malformed = |good: &[u8], count_at: usize| {
            let mut wrong_version = good.to_vec();
            wrong_version[0] = 2;
            let mut trailing = good.to_vec();
            trailing.push(0);
            [
                ("a wrong version", wrong_version),
                ("a count cut short", good[..count_at + 2].to_vec()),
                ("a trailing byte", trailing),
                ("an empty body", Vec::new()),
            ]
        };
        for (what, body) in malformed(&formation_bytes, 1 + INSTANCE_LEN) {
            assert_eq!(Formation::decode(&body), None, "formation: {what}");
        }
        // #80 S1b, PR #144 review: placement runs over the node list, so a list no declaration
        // writes does not decode; 3 to 8 nodes numbered 1 to K do.
        let with_nodes = |nodes: &[u32]| {
            let mut body = formation_bytes[..=INSTANCE_LEN + 4].to_vec();
            for node in nodes {
                body.extend_from_slice(&node.to_le_bytes());
            }
            body
        };
        for (what, nodes) in [
            ("no node", &[][..]),
            ("two nodes", &[1, 2][..]),
            ("nine nodes", &[1, 2, 3, 4, 5, 6, 7, 8, 9][..]),
            ("a gap", &[1, 2, 4][..]),
            ("a node twice", &[1, 2, 2][..]),
            ("out of order", &[1, 3, 2][..]),
            ("from node 0", &[0, 1, 2][..]),
            ("not from node 1", &[2, 3, 4][..]),
        ] {
            assert_eq!(
                Formation::decode(&with_nodes(nodes)),
                None,
                "formation: {what}"
            );
        }
        for k in 3..=8u32 {
            let nodes: Vec<u32> = (1..=k).collect();
            assert_eq!(
                Formation::decode(&with_nodes(&nodes)).map(|found| found.nodes.len()),
                Some(k as usize),
                "formation: {k} nodes"
            );
        }
        for (what, body) in malformed(&reservation_bytes, 1) {
            assert_eq!(
                Declaration::decode_reservation(&body),
                None,
                "reservation: {what}"
            );
        }
    }

    /// #80 S1b row 10 (rulings 2 and 4, ADR-rdb-0023 Decision 5): the node-start check's
    /// table. An undeclared database and a start that matches its records pass, as does
    /// `--listen` on the wildcard host at the recorded port. Each refusal names the record and
    /// both values: an undeclared node, an S1a store (no records), a missing own record, a
    /// `--listen` on another port, a `--peers` entry that disagrees (this node's own included),
    /// a peer that is not declared, and a declared peer left out. Unit.
    /// Cut: skip the `--peers` loop; the disagreeing peer passes, red.
    #[test]
    fn s1b_node_start_refusal_names_the_record_that_disagrees() {
        let nodes: Vec<NodeId> = (1..=3).map(NodeId).collect();
        let formation = Formation {
            instance: [7; INSTANCE_LEN],
            partitions: 8,
            nodes: nodes.clone(),
        };
        let records: BTreeMap<NodeId, NodeRecord> = nodes
            .iter()
            .map(|node| {
                let record = NodeRecord {
                    node: *node,
                    addr: format!("127.0.0.{}:7100", node.0),
                    machine: format!("127.0.0.{}", node.0),
                };
                (*node, record)
            })
            .collect();
        let peers = |entries: &[(u32, &str)]| -> BTreeMap<NodeId, String> {
            entries
                .iter()
                .map(|(n, addr)| (NodeId(*n), (*addr).to_owned()))
                .collect()
        };
        let good = peers(&[
            (1, "127.0.0.1:7100"),
            (2, "127.0.0.2:7100"),
            (3, "127.0.0.3:7100"),
        ]);
        let three = NodeId(3);
        assert_eq!(node_start_refusal(three, "x", &good, None, &records), None);
        assert_eq!(
            node_start_refusal(three, "127.0.0.3:7100", &good, Some(&formation), &records),
            None
        );
        assert_eq!(
            node_start_refusal(three, "0.0.0.0:7100", &good, Some(&formation), &records),
            None
        );
        let others = peers(&[(1, "127.0.0.1:7100"), (2, "127.0.0.2:7100")]);
        assert_eq!(
            node_start_refusal(three, "127.0.0.3:7100", &others, Some(&formation), &records),
            None,
            "--peers need not name this node"
        );
        let mut no_three = records.clone();
        no_three.remove(&three);
        for (what, node, listen, given, held, names) in [
            (
                "an undeclared node",
                NodeId(4),
                "127.0.0.4:7100",
                good.clone(),
                records.clone(),
                vec!["node 4 is not declared", "[1, 2, 3]"],
            ),
            (
                "an S1a store",
                three,
                "127.0.0.3:7100",
                good.clone(),
                BTreeMap::new(),
                vec![S1A_STORE],
            ),
            (
                "its own record missing",
                three,
                "127.0.0.3:7100",
                good.clone(),
                no_three,
                vec!["nodes/3 is absent"],
            ),
            (
                "--listen on another port",
                three,
                "127.0.0.3:7101",
                good.clone(),
                records.clone(),
                vec!["listen address 127.0.0.3:7101", "nodes/3", "127.0.0.3:7100"],
            ),
            (
                "--listen on the wildcard at another port",
                three,
                "0.0.0.0:7101",
                good.clone(),
                records.clone(),
                vec!["listen address 0.0.0.0:7101", "nodes/3", "127.0.0.3:7100"],
            ),
            (
                "a peer elsewhere",
                three,
                "127.0.0.3:7100",
                peers(&[
                    (1, "127.0.0.1:7100"),
                    (2, "127.0.0.2:7101"),
                    (3, "127.0.0.3:7100"),
                ]),
                records.clone(),
                vec![
                    "peer address 2=127.0.0.2:7101",
                    "nodes/2",
                    "records 127.0.0.2:7100",
                ],
            ),
            (
                "this node's own entry elsewhere",
                three,
                "127.0.0.3:7100",
                peers(&[
                    (1, "127.0.0.1:7100"),
                    (2, "127.0.0.2:7100"),
                    (3, "127.0.0.9:7100"),
                ]),
                records.clone(),
                vec![
                    "peer address 3=127.0.0.9:7100",
                    "nodes/3",
                    "records 127.0.0.3:7100",
                ],
            ),
            (
                "an undeclared peer",
                three,
                "127.0.0.3:7100",
                peers(&[
                    (1, "127.0.0.1:7100"),
                    (2, "127.0.0.2:7100"),
                    (4, "127.0.0.4:7100"),
                ]),
                records.clone(),
                vec!["peer address 4=127.0.0.4:7100", "nodes/4", "[1, 2, 3]"],
            ),
            (
                "a declared peer left out",
                three,
                "127.0.0.3:7100",
                peers(&[(1, "127.0.0.1:7100")]),
                records.clone(),
                vec!["no peer address for node 2", "nodes/2", "127.0.0.2:7100"],
            ),
        ] {
            let refused = node_start_refusal(node, listen, &given, Some(&formation), &held);
            let Some(text) = refused else {
                panic!("{what}: refused");
            };
            for name in names {
                assert!(text.contains(name), "{what}: {name:?} in {text}");
            }
            assert!(text.ends_with("nothing was started"), "{what}: {text}");
        }
    }

    /// #80 S1b (ADR-rdb-0023 Decision 2): the reservation's layout 2 carries every node's address
    /// and machine, so it is pinned as a literal too: one node with a machine label and two taking
    /// their host, one of them an IPv6 host in long form, whose stored label is the parsed `::3`
    /// (PR #144 review r2), so a change to the default label is red. It decodes back, and a body
    /// cut short, a trailing byte, a count past the entries and a bad machine byte each decode as
    /// nothing. Unit.
    /// Cut: `Declaration::records` takes the host as written, less its brackets
    /// (`0:0:0:0:0:0:0:3`); red at the pinned bytes.
    #[test]
    fn s1b_reservation_layout_2_bytes_are_pinned() {
        let declaration = Declaration {
            nodes: vec![
                NodeDecl {
                    node: NodeId(1),
                    addr: "10.0.0.1:7100".to_owned(),
                    machine: Some("M1".to_owned()),
                },
                NodeDecl {
                    node: NodeId(2),
                    addr: "10.0.0.2:7100".to_owned(),
                    machine: None,
                },
                NodeDecl {
                    node: NodeId(3),
                    addr: "[0:0:0:0:0:0:0:3]:7100".to_owned(),
                    machine: None,
                },
            ],
            partitions: 8,
        };
        let records = declaration
            .records()
            .expect("a declaration this build takes");
        let mut pinned = vec![2, 8, 0, 0, 0, 3];
        for (n, addr, machine) in [
            (1u8, &b"10.0.0.1:7100"[..], &b"m1"[..]),
            (2, b"10.0.0.2:7100", b"10.0.0.2"),
            (3, b"[0:0:0:0:0:0:0:3]:7100", b"::3"),
        ] {
            pinned.extend_from_slice(&[n, 0, 0, 0]);
            pinned.push(u8::try_from(addr.len()).expect("short"));
            pinned.extend_from_slice(addr);
            pinned.push(u8::try_from(machine.len()).expect("short"));
            pinned.extend_from_slice(machine);
        }
        assert_eq!(declaration.encode_reservation(&records).as_ref(), pinned);
        assert_eq!(
            Declaration::decode_reservation(&pinned),
            Some(Reservation {
                partitions: 8,
                nodes: vec![NodeId(1), NodeId(2), NodeId(3)],
                records: Some(records),
            })
        );
        let mut trailing = pinned.clone();
        trailing.push(0);
        let mut count_past = pinned.clone();
        count_past[5] = 4;
        let mut bad_machine = pinned.clone();
        // Header 6 bytes, then node 1: id 4, address length 1, address 13, machine length 1.
        bad_machine[6 + 4 + 1 + 13 + 1] = b'M';
        for (what, body) in [
            ("a body cut short", pinned[..pinned.len() - 1].to_vec()),
            ("a trailing byte", trailing),
            ("a count past the entries", count_past),
            ("a bad machine byte", bad_machine),
        ] {
            assert_eq!(Declaration::decode_reservation(&body), None, "{what}");
        }
    }

    /// #80 S1b, PR #144 review r3: a default machine label is the host as `parse_addr` parses
    /// it, not as written: an upper-case name with a trailing dot (`DB-A.example.`) gives
    /// `db-a.example`, a long-form IPv6 host gives `::3`, and an IPv4-mapped host in upper case
    /// gives its IPv4 address. Each record keeps its address as written. Unit.
    /// Cut: `Declaration::records` takes the host as written, lowercased and less its brackets;
    /// red at `db-a.example.`.
    #[test]
    fn s1b_default_labels_are_the_parsed_host() {
        let declaration = Declaration {
            nodes: [
                "DB-A.example.:7100",
                "[0:0:0:0:0:0:0:3]:7100",
                "[::FFFF:127.0.0.2]:7100",
            ]
            .iter()
            .zip(1..)
            .map(|(addr, n)| NodeDecl {
                node: NodeId(n),
                addr: (*addr).to_owned(),
                machine: None,
            })
            .collect(),
            partitions: 8,
        };
        let records = declaration
            .records()
            .expect("a declaration this build takes");
        let got: Vec<(&str, &str)> = records
            .iter()
            .map(|record| (record.addr.as_str(), record.machine.as_str()))
            .collect();
        assert_eq!(
            got,
            [
                ("DB-A.example.:7100", "db-a.example"),
                ("[0:0:0:0:0:0:0:3]:7100", "::3"),
                ("[::FFFF:127.0.0.2]:7100", "127.0.0.2"),
            ]
        );
    }

    /// Design 7.2 (Critic R2): a partition still to form joins its owner's heal, after the
    /// rooted partitions' plans, and nothing goes out until the grant is cleared. An owner with
    /// only partitions to form heals too. Cut: bootstrap plans sent before the clear.
    #[test]
    fn a_heal_sends_a_forming_partitions_plan_only_after_the_clear() {
        let rt = runtime();
        let two = PartitionId(2);
        let within = Duration::from_millis(300);
        let forming = vec![(two, Revision(4))];
        let order = |sent: &[Msg]| -> Vec<u32> {
            sent.iter()
                .map(|msg| match msg {
                    Msg::Inject { partition, .. } => partition.0,
                    other => panic!("{other:?}"),
                })
                .collect()
        };

        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let (healed, sent) = heal_forming(
            Arc::clone(&store),
            &rt,
            within,
            vec![PARTITION],
            forming.clone(),
        );
        healed.expect("the rooted partition heals");
        assert_eq!(
            order(&sent),
            [1, 1, 2, 2],
            "rooted first, a Plan and a proof each"
        );

        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let (healed, sent) =
            heal_forming(Arc::clone(&store), &rt, within, Vec::new(), forming.clone());
        healed.expect("nothing to clear, nothing rooted");
        assert_eq!(order(&sent), [2, 2], "the plans of the partition to form");

        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let taken = GrantRecord {
            grant: GrantId(1),
            node: OWNER,
            boot: OPEN_2,
            authority_generation: AuthorityGeneration(0),
            expiry_utc_ms: 0,
            frozen: false,
        };
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(ControlKey::Grant(OWNER).encode()),
            value: taken.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("the grant is taken");
        let (healed, sent) =
            heal_forming(Arc::clone(&store), &rt, within, vec![PARTITION], forming);
        assert!(
            matches!(healed, Err(HealError::NotCleared { .. })),
            "{healed:?}"
        );
        assert!(sent.is_empty(), "{} messages before the clear", sent.len());
    }

    /// PR #138 review (#80 S1a): a heal with only partitions to form waits for every member of
    /// them too, not only the rooted partitions' members. With node 3 never linked, it ends
    /// `PeersUnknown` naming node 3 and sends nothing. With three distinct boots, each plan
    /// pins every member at its own boot. Unit (~150 ms): `MemStore`, a 100 ms budget.
    /// Cut: the heal waits on `member_union(&pinned)` alone; the first heal sends plans, red.
    #[test]
    fn s1a_a_bootstrap_only_heal_waits_for_every_member_and_pins_its_boot() {
        let rt = runtime();
        let two = PartitionId(2);
        let heal_with = |member_boot: Arc<dyn Fn(NodeId) -> Option<BootId> + Send + Sync>| {
            let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
            let (owner, mailbox) = std::sync::mpsc::channel();
            let ctx = HealCtx {
                partition_count: 2,
                nodes: crate::db::ALL_NODES.into(),
                store,
                boot: OPEN_2,
                budgets: Budgets::SPEC_DEFAULTS,
                clock: HostClock::start(),
                owner,
                within: Duration::from_millis(100),
                member_boot,
                owner_node: OWNER,
                partitions: Vec::new(),
                bootstrap: vec![(two, Revision(4))],
            };
            let healed = rt.block_on(heal(ctx));
            (healed, mailbox.try_iter().collect::<Vec<Msg>>())
        };

        let (refused, sent) = heal_with(Arc::new(|node: NodeId| {
            (node != NodeId(3)).then_some(BootId(10 + u64::from(node.0)))
        }));
        assert!(
            matches!(&refused, Err(HealError::PeersUnknown { nodes, .. }) if nodes == &[3]),
            "{refused:?}"
        );
        assert!(sent.is_empty(), "{} messages sent", sent.len());

        let boot_of = |node: NodeId| BootId(10 + u64::from(node.0));
        let (healed, sent) = heal_with(Arc::new(move |node| Some(boot_of(node))));
        healed.expect("every member answered");
        let pinned: Vec<Vec<(NodeId, BootId)>> = sent
            .iter()
            .filter_map(|msg| match msg {
                Msg::Inject {
                    kind: EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(plan))),
                    ..
                } => Some(
                    plan.config
                        .members
                        .iter()
                        .map(|member| (member.node, member.boot))
                        .collect(),
                ),
                _ => None,
            })
            .collect();
        let want: Vec<(NodeId, BootId)> = members_for(two, BOOT)
            .iter()
            .map(|member| (member.node, boot_of(member.node)))
            .collect();
        assert_eq!(want.len(), 3, "{want:?}");
        assert_eq!(pinned, [want], "one plan, every member at its own boot");
    }

    /// The `FencingProof` a heal sent node 1, if it sent one.
    fn fence_sent(sent: &[Msg]) -> Option<FencingProof> {
        sent.iter().find_map(|msg| match msg {
            Msg::Inject {
                kind: EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(fence))),
                ..
            } => Some((**fence).clone()),
            _ => None,
        })
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime")
    }

    /// Case B (R3a; R5 rows 6 and 8): every `grants/{n}` absent and a layout-3 `partitions/1`.
    /// The heal hands node 1 a `Plan`, then a `NoGrantHeld` takeover authorization whose
    /// `absent_revision` is the read that found `grants/1` absent, whose `control_revision` is
    /// `partitions/1`'s `mod_revision` read at or after it (A-R163), with `prior_grant_id` 0
    /// ("none held") and `prior_boot_id` the record's `owner_boot`. Unit (~10 ms): `MemStore`.
    #[test]
    fn a_reopen_with_no_grant_held_hands_node_1_a_no_grant_held_takeover() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let written = rooted(&*store, &rt, BootId(5));
        // A later write elsewhere, so the absent read's revision is past the record's.
        rt.block_on(store.put(PutRequest {
            key: Bytes::from_static(b"unrelated"),
            value: Bytes::from_static(b"x"),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("put an unrelated key");
        let (healed, sent) = heal_once(Arc::clone(&store), &rt);
        let healed = healed.expect("case B heals");
        assert_eq!(sent.len(), 2, "a Plan, then the proof");
        assert!(matches!(
            &sent[0],
            Msg::Inject {
                kind: EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(_))),
                ..
            }
        ));
        let fence = fence_sent(&sent).expect("a takeover authorization");
        let Revocation::NoGrantHeld { absent_revision } = fence.revocation else {
            panic!("expected NoGrantHeld: {fence:?}");
        };
        assert!(absent_revision.0 > written, "{fence:?}");
        assert_eq!(
            fence.control_revision,
            Revision(written),
            "partitions/1's mod_revision"
        );
        assert_eq!(fence.prior_grant_id, GrantId(0));
        assert_eq!(fence.prior_boot_id, BootId(5));
        assert_eq!(fence.prior_generation, Generation(3));
        assert_eq!(fence.prior_owner_epoch, OwnerEpoch(3));
        assert_eq!(healed.control_revision, fence.control_revision);
        assert!(healed.read_revision >= absent_revision, "{healed:?}");
    }

    /// R5 row 8 for case B: a record whose `owner_boot` is 0 cannot name the prior process, so
    /// the heal refuses it and sends nothing. Unit (~10 ms).
    #[test]
    fn a_no_grant_heal_over_owner_boot_0_is_refused_and_sends_nothing() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(0));
        let (healed, sent) = heal_once(store, &rt);
        assert!(matches!(healed, Err(HealError::Record(_))), "{healed:?}");
        assert!(sent.is_empty(), "{} messages sent", sent.len());
    }

    /// Case B over a layout-2 `partitions/1`: no root, so no anchor and no owner boot. Refused,
    /// and nothing sent. Unit (~10 ms).
    #[test]
    fn a_no_grant_heal_over_a_record_with_no_root_is_refused_and_sends_nothing() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        put(&store, &rt, record(3).encode(), 0);
        let (healed, sent) = heal_once(store, &rt);
        assert!(matches!(healed, Err(HealError::Record(_))), "{healed:?}");
        assert!(sent.is_empty(), "{} messages sent", sent.len());
    }

    /// When [`old_grant`]'s record expired: long past, and not 0, so a proof that carried a
    /// constant 0 in place of the record's expiry would not match it.
    const OLD_EXPIRY_UTC_MS: i64 = 1_000;

    /// `grants/1` as a dead open's boot-1 process left it holding `grant`: long expired and not
    /// frozen, so case A's guards clear it. Returns the revision it was written at.
    fn old_grant(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime, grant: GrantId) -> u64 {
        let old = GrantRecord {
            grant,
            node: OWNER,
            boot: BOOT,
            authority_generation: AuthorityGeneration(0),
            expiry_utc_ms: OLD_EXPIRY_UTC_MS,
            frozen: false,
        };
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(ControlKey::Grant(OWNER).encode()),
            value: old.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put grants/1")
        .revision
    }

    /// Case A (R3a, A-R175): the heal deletes a dead open's expired grant and hands node 1 an
    /// `ExpiryProven` takeover. `prior_grant_id` is the record's grant; `prior_boot_id` is
    /// `partitions/1`'s `owner_boot`, which the F1 commit pinned, not the grant's own boot. The
    /// proof's frozen expiry is the deleted record's expiry (A-R207, tester cut r6d).
    /// Unit (~10 ms): `MemStore`.
    #[test]
    fn a_reopen_over_an_expired_grant_hands_node_1_an_expiry_proven_takeover_naming_the_owner_boot()
    {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let written = rooted(&*store, &rt, BootId(5));
        let granted = old_grant(&*store, &rt, GrantId(4));
        let (healed, sent) = heal_once(Arc::clone(&store), &rt);
        let healed = healed.expect("case A heals");
        assert!(
            matches!(healed.cleared, GrantCleared::Deleted(deleted) if deleted.0 > granted),
            "{healed:?}"
        );
        let fence = fence_sent(&sent).expect("a FenceProven was sent");
        let Revocation::ExpiryProven {
            frozen_expiry_utc_ms,
            ..
        } = fence.revocation
        else {
            panic!("expected ExpiryProven: {fence:?}");
        };
        assert_eq!(
            frozen_expiry_utc_ms, OLD_EXPIRY_UTC_MS,
            "the deleted record's expiry: {fence:?}"
        );
        assert_eq!(fence.prior_grant_id, GrantId(4));
        assert_eq!(
            fence.prior_boot_id,
            BootId(5),
            "partitions/1's owner_boot, not the grant's boot {}",
            BOOT.0
        );
        assert_eq!(fence.control_revision, Revision(written));
        assert_eq!(grant_revision(&*store, &rt), None, "the old grant is gone");
    }

    /// Case A (R3a; R5 row 9; A-R175): A1 never issues `GrantId(0)`, and a proof carrying it
    /// would read as case B's "none held". A record holding it is refused before anything is
    /// deleted: the heal fails naming it, `grants/1` stays at its revision, and node 1 is sent
    /// nothing. Unit (~10 ms): `MemStore`.
    #[test]
    fn a_heal_over_a_grant_record_holding_grant_0_is_refused_and_deletes_nothing() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let written = old_grant(&*store, &rt, GrantId(0));
        let (healed, sent) = heal_once(Arc::clone(&store), &rt);
        match &healed {
            Err(HealError::Control(detail)) => assert!(detail.contains("grant 0"), "{detail}"),
            other => panic!("expected a refusal naming grant 0: {other:?}"),
        }
        assert_eq!(
            grant_revision(&*store, &rt),
            Some(written),
            "nothing deleted"
        );
        assert!(sent.is_empty(), "{} messages sent", sent.len());
    }

    /// Case A over a `grants/1` that does not decode (A-R207, tester cut r10a, heal side): the
    /// record is there but unreadable, which is not "no grant held". The heal refuses it naming
    /// the record, `grants/1` stays, and node 1 is sent no `Plan` or anything else.
    /// Unit (~10 ms): `MemStore`.
    #[test]
    fn a_heal_over_a_grant_record_that_does_not_decode_is_refused_and_sends_nothing() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        put_at(
            &*store,
            &rt,
            ControlKey::Grant(OWNER),
            Bytes::from_static(b"not a grant record"),
        );
        let written = grant_revision(&*store, &rt).expect("grants/1 written");
        let (healed, sent) = heal_once(Arc::clone(&store), &rt);
        match &healed {
            Err(HealError::Control(detail)) => {
                assert!(detail.contains("is not a grant record"), "{detail}");
            }
            other => panic!("expected a refusal naming the unreadable grant: {other:?}"),
        }
        assert_eq!(
            grant_revision(&*store, &rt),
            Some(written),
            "nothing deleted"
        );
        assert!(sent.is_empty(), "{} messages sent", sent.len());
    }

    /// A `partitions/` family holding one record this build cannot decode is not a complete
    /// list, since that record could be a partition in transfer (A-R207, tester cut r10c). With
    /// an undecodable `partitions/2` beside an expired `grants/1`, `clear_grant` answers
    /// unreadable (`NotWired`, nothing deleted) and the heal refuses and sends nothing; in both,
    /// `grants/1` stays at its revision. Unit (~20 ms): `MemStore`.
    #[test]
    fn an_undecodable_partitions_record_makes_clear_and_heal_delete_nothing() {
        let rt = runtime();
        let damaged = || Bytes::from_static(b"not a partition record");

        let store = config_testkit::MemStore::new();
        let (db, _mailbox, written) = restarted_owner(&store, &rt);
        put_at(
            &store,
            &rt,
            ControlKey::Partition(PartitionId(2)),
            damaged(),
        );
        let refused = rt
            .block_on(clear_grant(&db, &store, OWNER))
            .expect_err("an unreadable partitions/2 is refused");
        assert_eq!(
            (refused.kind, refused.retry, refused.no_mutation),
            (ErrorKind::Unavailable, RetryRule::NotWired, true),
            "{refused:?}"
        );
        assert!(
            refused.detail.contains("partitions/2")
                && refused.detail.contains("is not a partition record")
                && refused.detail.ends_with("nothing deleted"),
            "{refused:?}"
        );
        assert_eq!(
            grant_revision(&store, &rt),
            Some(written),
            "clear: grants/1"
        );

        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let written = old_grant(&*store, &rt, GrantId(4));
        put_at(
            &*store,
            &rt,
            ControlKey::Partition(PartitionId(2)),
            damaged(),
        );
        let (healed, sent) = heal_once(Arc::clone(&store), &rt);
        match &healed {
            Err(HealError::Control(detail)) => {
                assert!(detail.contains("is not a partition record"), "{detail}");
            }
            other => panic!("expected a refusal naming the unreadable partition: {other:?}"),
        }
        assert_eq!(
            grant_revision(&*store, &rt),
            Some(written),
            "heal: grants/1"
        );
        assert!(sent.is_empty(), "heal: {} messages sent", sent.len());
    }

    /// A control store whose `partitions/1` reads answer from the snapshot [`Self::freeze`]
    /// took: a lagging read, older than anything written after it. Everything else is the
    /// `MemStore` underneath. Written out by hand because `async-trait` is not a dependency of
    /// this crate.
    struct StalePartition {
        inner: config_testkit::MemStore,
        stale: std::sync::Mutex<Option<config_core::GetResponse>>,
    }

    type Reply<'a, T> =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, ConfigError>> + Send + 'a>>;

    impl StalePartition {
        fn new() -> Self {
            Self {
                inner: config_testkit::MemStore::new(),
                stale: std::sync::Mutex::new(None),
            }
        }

        fn freeze(&self, rt: &tokio::runtime::Runtime) {
            let read = rt
                .block_on(self.inner.get(GetRequest {
                    key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
                }))
                .expect("get partitions/1");
            *self.stale.lock().expect("stale") = Some(read);
        }
    }

    impl ConfigStore for StalePartition {
        fn get<'a, 'b>(&'a self, request: GetRequest) -> Reply<'b, config_core::GetResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            let stale = self.stale.lock().expect("stale").clone();
            match stale {
                Some(read)
                    if request.key == ControlKey::Partition(PARTITION).encode().as_bytes() =>
                {
                    Box::pin(std::future::ready(Ok(read)))
                }
                _ => self.inner.get(request),
            }
        }

        fn list<'a, 'b>(&'a self, request: ListRequest) -> Reply<'b, config_core::ListResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.list(request)
        }

        fn put<'a, 'b>(&'a self, request: PutRequest) -> Reply<'b, config_core::MutationResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.put(request)
        }

        fn delete<'a, 'b>(
            &'a self,
            request: DeleteRequest,
        ) -> Reply<'b, config_core::MutationResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.delete(request)
        }

        fn capabilities(&self) -> config_core::Capabilities {
            self.inner.capabilities()
        }

        fn watch<'a, 'b>(
            &'a self,
            request: config_core::WatchRequest,
        ) -> Reply<'b, config_core::WatchStream>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.watch(request)
        }
    }

    /// A store whose `grants/1` moves under a promote: at every read of it (`on_get`, an owner
    /// still renewing), or when `partitions/1` is written `Fencing` (`on_fencing`, an owner that
    /// came back after the look).
    struct MovingGrant {
        inner: config_testkit::MemStore,
        on_get: bool,
        on_fencing: bool,
        /// Delete `grants/1` right after the `Fencing` write, as a clearing service
        /// (`clear_grant`, the reopen's heal) whose guard-3 read came first would.
        clear_on_fencing: bool,
        /// What befalls the promote's undo of its `Fencing` write: a put on the partition that
        /// is not `Fencing`.
        undo: UndoFault,
        /// Replace `grants/1` right after the `Fencing` write with a live, unfrozen grant
        /// ([`LIVE_GRANT`]): the owner came back and renewed under a new grant id.
        replace_on_fencing: bool,
        /// Set once the undo has been tried.
        undo_tried: AtomicBool,
        /// [`UndoFault::Overtaken`]: the bytes and `mod_revision` of the record it committed.
        overtook: std::sync::Mutex<Option<(Bytes, u64)>>,
    }

    /// The grant [`MovingGrant::replace_on_fencing`] writes: a new grant id, a minute to run.
    const LIVE_GRANT: GrantId = GrantId(9);

    /// [`MovingGrant::undo`].
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum UndoFault {
        /// The undo applies and answers.
        Answers,
        /// The undo applies and its answer is lost (`DeadlineExceededUnknownOutcome`).
        ReplyLost,
        /// The undo answers `DeadlineExceededUnknownOutcome` without applying.
        Dropped,
        /// As [`UndoFault::Dropped`], and every read of `partitions/1` after it fails.
        DroppedUnreadable,
        /// Just before the undo, another promote commits partition 1 `Serving` under node 3 at
        /// the next generation; the undo's CAS then conflicts with it.
        Overtaken,
    }

    impl MovingGrant {
        /// [`UndoFault::Overtaken`]: commit, unconditionally, the record `undo` would restore
        /// with node 3 as its `Serving` owner at the next generation, and keep its bytes and
        /// `mod_revision`.
        async fn overtake(&self, undo: &Bytes) {
            let before = PartitionRecord::decode(undo).expect("a record");
            let root = PartitionRoot::decode(undo).expect("a root");
            let newer = PartitionRecord {
                owner: NodeId(3),
                generation: Generation(before.generation.0 + 1),
                lifecycle: PartitionLifecycle::Serving,
                ..before
            };
            let value = newer.encode_with(&root);
            let committed = self
                .inner
                .put(PutRequest {
                    key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
                    value: value.clone(),
                    expected_mod_revision: None,
                    dedup: None,
                })
                .await
                .expect("overtake partitions/1");
            assert_eq!(committed.outcome, MutationOutcome::Applied);
            *self.overtook.lock().expect("overtook") = Some((value, committed.revision));
        }

        async fn renew(&self) {
            let key = Bytes::from(ControlKey::Grant(OWNER).encode());
            let found = self
                .inner
                .get(GetRequest { key: key.clone() })
                .await
                .expect("get")
                .record
                .expect("a grant");
            let held = GrantRecord::decode(&found.value).expect("a grant");
            let renewed = GrantRecord {
                expiry_utc_ms: held.expiry_utc_ms + 500,
                ..held
            };
            self.inner
                .put(PutRequest {
                    key,
                    value: renewed.encode(),
                    expected_mod_revision: Some(found.mod_revision),
                    dedup: None,
                })
                .await
                .expect("renew");
        }
    }

    impl ConfigStore for MovingGrant {
        fn get<'a, 'b>(&'a self, request: GetRequest) -> Reply<'b, config_core::GetResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            Box::pin(async move {
                if self.on_get && request.key == ControlKey::Grant(OWNER).encode().as_bytes() {
                    self.renew().await;
                }
                if self.undo == UndoFault::DroppedUnreadable
                    && self.undo_tried.load(Ordering::SeqCst)
                    && request.key == ControlKey::Partition(PARTITION).encode().as_bytes()
                {
                    return Err(ConfigError::Unavailable {
                        reason: "admin fixture: partitions/1 unreadable".to_owned(),
                    });
                }
                self.inner.get(request).await
            })
        }

        fn list<'a, 'b>(&'a self, request: ListRequest) -> Reply<'b, config_core::ListResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.list(request)
        }

        fn put<'a, 'b>(&'a self, request: PutRequest) -> Reply<'b, config_core::MutationResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            Box::pin(async move {
                let on_partition =
                    request.key == ControlKey::Partition(PARTITION).encode().as_bytes();
                let lifecycle = PartitionRecord::decode(&request.value).map(|r| r.lifecycle);
                let fencing = on_partition && lifecycle == Some(PartitionLifecycle::Fencing);
                let undo = on_partition && !fencing && lifecycle.is_some();
                if undo {
                    self.undo_tried.store(true, Ordering::SeqCst);
                }
                if undo && matches!(self.undo, UndoFault::Dropped | UndoFault::DroppedUnreadable) {
                    return Err(ConfigError::DeadlineExceededUnknownOutcome);
                }
                if undo && self.undo == UndoFault::Overtaken {
                    self.overtake(&request.value).await;
                }
                let answer = self.inner.put(request).await;
                if undo && self.undo == UndoFault::ReplyLost {
                    answer.expect("the undo applies");
                    return Err(ConfigError::DeadlineExceededUnknownOutcome);
                }
                if fencing && self.on_fencing {
                    self.renew().await;
                }
                if fencing && self.replace_on_fencing {
                    let live = GrantRecord {
                        grant: LIVE_GRANT,
                        node: OWNER,
                        boot: BOOT,
                        authority_generation: AuthorityGeneration(0),
                        expiry_utc_ms: expiry_ahead(60_000),
                        frozen: false,
                    };
                    let replaced = self
                        .inner
                        .put(PutRequest {
                            key: Bytes::from(ControlKey::Grant(OWNER).encode()),
                            value: live.encode(),
                            expected_mod_revision: None,
                            dedup: None,
                        })
                        .await
                        .expect("replace grants/1");
                    assert_eq!(replaced.outcome, MutationOutcome::Applied);
                }
                if fencing && self.clear_on_fencing {
                    let cleared = self
                        .inner
                        .delete(DeleteRequest {
                            key: Bytes::from(ControlKey::Grant(OWNER).encode()),
                            expected_mod_revision: None,
                            dedup: None,
                        })
                        .await
                        .expect("clear grants/1");
                    assert_eq!(cleared.outcome, MutationOutcome::Applied);
                }
                answer
            })
        }

        fn delete<'a, 'b>(
            &'a self,
            request: DeleteRequest,
        ) -> Reply<'b, config_core::MutationResponse>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.delete(request)
        }

        fn capabilities(&self) -> config_core::Capabilities {
            self.inner.capabilities()
        }

        fn watch<'a, 'b>(
            &'a self,
            request: config_core::WatchRequest,
        ) -> Reply<'b, config_core::WatchStream>
        where
            'a: 'b,
            Self: 'b,
        {
            self.inner.watch(request)
        }
    }

    /// A promote above one partition over `store`, with node 1, the owner, unreachable.
    fn promote_above_one(
        store: Arc<dyn ConfigStore>,
        rt: &tokio::runtime::Runtime,
    ) -> PromoteError {
        let ctx = PromoteCtx {
            partition: PARTITION,
            owner_must_be_dead: true,
            look_within: Duration::from_millis(1),
            store,
            budgets: Budgets::SPEC_DEFAULTS,
            clock: HostClock::start(),
            to: NodeId(2),
            drop: None,
            within: Duration::from_secs(1),
            members: members(),
            members_version: CONFIG_VERSION,
            member_boot: all_known(),
            send: Arc::new(|_, _| Err(NodeStopped(OWNER))),
        };
        rt.block_on(promote(ctx)).expect_err("refused")
    }

    /// What a host-faulted node 1 answers a look with (issue #122).
    fn faulted_status() -> NodeStatus {
        NodeStatus {
            node: OWNER,
            boot: BOOT,
            fault: Some("control adapter: admin fixture".to_owned()),
            holds: None,
            authority: "none".to_owned(),
            fence: None,
            fence_polling: false,
            replace_stopped: false,
            recovery: None,
            recovered: None,
            role: "primary",
            protection: None,
            admits: None,
            published: None,
            in_flight: None,
            waits_on: None,
            stalled: None,
            reload_pending: None,
            lengths: None,
            pinned: members(),
            config_version: Some(CONFIG_VERSION),
            qualifying_copy: None,
            clock_bound_lost: false,
            committed_without: rdb_core::contracts::recovery::NoteState::Unknown,
            tail_refused: None,
        }
    }

    /// A promote above one partition over `store` within `within`, with node 1, the owner,
    /// answering every look with [`faulted_status`] and every other node unreachable. Also
    /// returns how many looks node 1 answered.
    fn promote_above_one_faulted(
        store: Arc<dyn ConfigStore>,
        rt: &tokio::runtime::Runtime,
        within: Duration,
    ) -> (PromoteError, usize) {
        let looks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&looks);
        let ctx = PromoteCtx {
            partition: PARTITION,
            owner_must_be_dead: true,
            look_within: Duration::from_secs(1),
            store,
            budgets: Budgets::SPEC_DEFAULTS,
            clock: HostClock::start(),
            to: NodeId(2),
            drop: None,
            within,
            members: members(),
            members_version: CONFIG_VERSION,
            member_boot: all_known(),
            send: Arc::new(move |node, msg| match (node, msg) {
                (OWNER, Msg::Inspect { reply, .. }) => {
                    counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    reply.send(faulted_status()).expect("the look waits");
                    Ok(())
                }
                (node, _) => Err(NodeStopped(node)),
            }),
        };
        let refused = rt.block_on(promote(ctx)).expect_err("refused");
        (refused, looks.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// `grants/1`'s expiry `ahead_ms` past this host's control time now.
    fn expiry_ahead(ahead_ms: i64) -> i64 {
        i64::try_from(
            HostClock::control_time(HostClock::start().now(), &Budgets::SPEC_DEFAULTS)
                .estimate
                .0,
        )
        .expect("in range")
            + ahead_ms
    }

    fn unfrozen_and_serving(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime) {
        let grant = rt
            .block_on(store.get(GetRequest {
                key: Bytes::from(ControlKey::Grant(OWNER).encode()),
            }))
            .expect("get")
            .record
            .expect("a grant");
        assert!(!GrantRecord::decode(&grant.value).expect("a grant").frozen);
        let (record, ..) = rt.block_on(read_partition(store, PARTITION)).expect("read");
        assert_eq!(record.lifecycle, PartitionLifecycle::Serving, "{record:?}");
    }

    /// An owner with no link whose grant is still being renewed is alive: refused, nothing
    /// written (issue #79 S3). Unit (~60 ms): `MemStore`.
    #[test]
    fn above_one_partition_an_owner_still_renewing_is_refused() {
        let rt = runtime();
        let store = Arc::new(MovingGrant {
            inner: config_testkit::MemStore::new(),
            on_get: true,
            on_fencing: false,
            clear_on_fencing: false,
            undo: UndoFault::Answers,
            replace_on_fencing: false,
            undo_tried: AtomicBool::new(false),
            overtook: std::sync::Mutex::new(None),
        });
        rooted(&store.inner, &rt, BootId(5));
        old_grant(&store.inner, &rt, GrantId(4));
        let expiry = i64::try_from(
            HostClock::control_time(HostClock::start().now(), &Budgets::SPEC_DEFAULTS)
                .estimate
                .0,
        )
        .expect("in range")
            + 60_000;
        put_grant_expiry(&store.inner, &rt, expiry);
        let refused = promote_above_one(Arc::clone(&store) as Arc<dyn ConfigStore>, &rt);
        assert!(
            matches!(&refused, PromoteError::OwnerAlive { evidence, .. } if evidence.contains("renewed")),
            "{refused:?}"
        );
        let told = ApiError::from(refused).explain();
        assert!(
            told.ends_with("stop that node first, then promote again")
                && !told.contains("retrying will not help"),
            "{told}"
        );
        unfrozen_and_serving(&store.inner, &rt);
    }

    /// Issue #122 review: a host-faulted owner whose grant has not run out is still waited out,
    /// not promoted away from. Node 1 answers the look with a host fault, its 60 s grant stands
    /// still, and the promote has 100 ms: it runs out of time on the grant check, and
    /// `grants/1` and `partitions/1` are left as they were. Cuts: `owner_dead`'s fault arm
    /// returns the grant's revision at once, skipping the grant checks: the freeze writes
    /// `grants/1`, red. The fault arm refuses as a live owner: `OwnerAlive`, red.
    /// Unit (~150 ms): `MemStore`, waits out the 100 ms.
    #[test]
    fn above_one_partition_a_faulted_owner_with_an_unexpired_grant_is_refused() {
        let rt = runtime();
        let store = config_testkit::MemStore::new();
        rooted(&store, &rt, BootId(5));
        old_grant(&store, &rt, GrantId(4));
        put_grant_expiry(&store, &rt, expiry_ahead(60_000));
        let before = grant_revision(&store, &rt);
        let store: Arc<dyn ConfigStore> = Arc::new(store);
        let (refused, looks) =
            promote_above_one_faulted(Arc::clone(&store), &rt, Duration::from_millis(100));
        assert_eq!(looks, 1, "node 1 was looked at once");
        assert!(
            matches!(&refused, PromoteError::Control(detail) if detail.contains("was not proven past")),
            "{refused:?}"
        );
        assert_eq!(grant_revision(&*store, &rt), before, "grants/1 not written");
        unfrozen_and_serving(&*store, &rt);
    }

    /// Issue #122 review: a host-faulted owner whose grant is still being renewed is refused as
    /// alive, the same as a silent one ([`above_one_partition_an_owner_still_renewing_is_refused`]),
    /// with nothing written. Here the look is answered, so the fault arm runs. Cuts: `owner_dead`'s
    /// fault arm returns the grant's revision at once: the freeze refuses "it came back during the
    /// promote" instead, red. The fault arm refuses "it answered a look": red.
    /// Unit (~60 ms): `MemStore`.
    #[test]
    fn above_one_partition_a_faulted_owner_still_renewing_is_refused() {
        let rt = runtime();
        let store = Arc::new(MovingGrant {
            inner: config_testkit::MemStore::new(),
            on_get: true,
            on_fencing: false,
            clear_on_fencing: false,
            undo: UndoFault::Answers,
            replace_on_fencing: false,
            undo_tried: AtomicBool::new(false),
            overtook: std::sync::Mutex::new(None),
        });
        rooted(&store.inner, &rt, BootId(5));
        old_grant(&store.inner, &rt, GrantId(4));
        put_grant_expiry(&store.inner, &rt, expiry_ahead(60_000));
        let (refused, looks) = promote_above_one_faulted(
            Arc::clone(&store) as Arc<dyn ConfigStore>,
            &rt,
            Duration::from_secs(1),
        );
        assert_eq!(looks, 1, "node 1 was looked at once");
        assert!(
            matches!(&refused, PromoteError::OwnerAlive { evidence, .. } if evidence == "its grant is still being renewed"),
            "{refused:?}"
        );
        unfrozen_and_serving(&store.inner, &rt);
    }

    /// An owner that comes back after the look and before the freeze leaves nothing behind: the
    /// `Fencing` write is undone and the promote refused (issue #79 S3). Unit (~20 ms): `MemStore`.
    #[test]
    fn above_one_partition_an_owner_back_after_the_look_undoes_the_fencing() {
        let rt = runtime();
        let store = Arc::new(MovingGrant {
            inner: config_testkit::MemStore::new(),
            on_get: false,
            on_fencing: true,
            clear_on_fencing: false,
            undo: UndoFault::Answers,
            replace_on_fencing: false,
            undo_tried: AtomicBool::new(false),
            overtook: std::sync::Mutex::new(None),
        });
        rooted(&store.inner, &rt, BootId(5));
        old_grant(&store.inner, &rt, GrantId(4));
        let refused = promote_above_one(Arc::clone(&store) as Arc<dyn ConfigStore>, &rt);
        assert!(
            matches!(&refused, PromoteError::OwnerAlive { evidence, .. } if evidence.contains("came back")),
            "{refused:?}"
        );
        unfrozen_and_serving(&store.inner, &rt);
    }

    /// PR #119 review question (clear-grant against a fence, spec §7.3): a clearing service
    /// that read every partition `Serving` deletes `grants/1` between the promote's `Fencing`
    /// write and its freeze. The freeze finds the key absent where `owner_dead` judged a record,
    /// so the promote is refused `OwnerAlive`, the `Fencing` write is undone, and nothing is
    /// created frozen: no transfer goes on against a grant that vanished. Unit (~20 ms): `MemStore`.
    /// Cut: the absent-record arm of `freeze` ignores `dead_at`; a frozen record is created and
    /// the promote goes on, red.
    #[test]
    fn above_one_partition_a_grant_cleared_after_the_fencing_undoes_it() {
        let rt = runtime();
        let store = Arc::new(MovingGrant {
            inner: config_testkit::MemStore::new(),
            on_get: false,
            on_fencing: false,
            clear_on_fencing: true,
            undo: UndoFault::Answers,
            replace_on_fencing: false,
            undo_tried: AtomicBool::new(false),
            overtook: std::sync::Mutex::new(None),
        });
        rooted(&store.inner, &rt, BootId(5));
        old_grant(&store.inner, &rt, GrantId(4));
        let refused = promote_above_one(Arc::clone(&store) as Arc<dyn ConfigStore>, &rt);
        assert!(
            matches!(&refused, PromoteError::OwnerAlive { evidence, .. } if evidence.contains("came back")),
            "{refused:?}"
        );
        let grant = rt
            .block_on(store.inner.get(GetRequest {
                key: Bytes::from(ControlKey::Grant(OWNER).encode()),
            }))
            .expect("get")
            .record;
        assert!(grant.is_none(), "nothing created frozen: {grant:?}");
        let (record, ..) = rt
            .block_on(read_partition(&store.inner, PARTITION))
            .expect("read");
        assert_eq!(record.lifecycle, PartitionLifecycle::Serving, "{record:?}");
    }

    /// The grant-cleared-after-`Fencing` promote above one partition, its undo faulted.
    fn promote_with_undo(
        rt: &tokio::runtime::Runtime,
        undo: UndoFault,
    ) -> (Arc<MovingGrant>, PromoteError) {
        let store = Arc::new(MovingGrant {
            inner: config_testkit::MemStore::new(),
            on_get: false,
            on_fencing: false,
            clear_on_fencing: true,
            undo,
            replace_on_fencing: false,
            undo_tried: AtomicBool::new(false),
            overtook: std::sync::Mutex::new(None),
        });
        rooted(&store.inner, rt, BootId(5));
        old_grant(&store.inner, rt, GrantId(4));
        let refused = promote_above_one(Arc::clone(&store) as Arc<dyn ConfigStore>, rt);
        (store, refused)
    }

    fn undo_lines(method: &str, message: &str) -> Vec<serde_json::Value> {
        config_testkit::logs::lines_for_current_test(module_path!(), method)
            .into_iter()
            .filter(|line| line["@m"] == message)
            .collect()
    }

    /// PR #119 review (spec §7.3, a failed or unknown undo): the owner came back after the
    /// `Fencing` write, and the undo applies but its answer is lost. The promote reads the
    /// record again instead of assuming it is still `Fencing`: it finds the record it fenced,
    /// back to `Serving`, so the `OwnerAlive` refusal stands and nothing is left to resume.
    /// Unit (~20 ms): `MemStore`.
    /// Cut: return the undo's error without the reread (the PR's earlier answer); the promote
    /// says "promote again" over a partition that is serving, red.
    #[config_log::retcd_test]
    fn above_one_partition_an_undo_whose_answer_is_lost_is_read_again() {
        const METHOD: &str = "above_one_partition_an_undo_whose_answer_is_lost_is_read_again";
        let rt = runtime();
        let (store, refused) = promote_with_undo(&rt, UndoFault::ReplyLost);
        assert!(
            matches!(&refused, PromoteError::OwnerAlive { evidence, .. } if evidence.contains("came back")),
            "{refused:?}"
        );
        let (record, ..) = rt
            .block_on(read_partition(&store.inner, PARTITION))
            .expect("read");
        assert_eq!(record.lifecycle, PartitionLifecycle::Serving, "{record:?}");
        let undone = undo_lines(METHOD, "promote_fencing_undone");
        assert!(
            undone.len() == 1 && undone[0]["answer_lost"] == true,
            "{undone:?}"
        );
        assert!(undo_lines(METHOD, "promote_fencing_undo_failed").is_empty());
    }

    /// PR #119 review (spec §7.3, a failed or unknown undo): the same, but the undo does not
    /// apply. Read again, the record is still `Fencing` at this promote's own revision, so the
    /// refusal says the partition is still fenced and a promote must resume it: only then, and
    /// never on an assumption, is a later promote told to go on from `Fencing`.
    /// Unit (~20 ms): `MemStore`.
    /// Cut: take the reread's `Fencing` as undone (the refusal stays `OwnerAlive`); red.
    #[config_log::retcd_test]
    fn above_one_partition_an_undo_that_did_not_apply_is_read_still_fencing() {
        const METHOD: &str = "above_one_partition_an_undo_that_did_not_apply_is_read_still_fencing";
        let rt = runtime();
        let (store, refused) = promote_with_undo(&rt, UndoFault::Dropped);
        let (record, _, _, revision, _, _) = rt
            .block_on(read_partition(&store.inner, PARTITION))
            .expect("read");
        assert_eq!(record.lifecycle, PartitionLifecycle::Fencing, "{record:?}");
        assert!(
            matches!(&refused, PromoteError::Control(why)
                if why.contains(&format!("is still Fencing at revision {revision}"))
                    && why.ends_with("promote again")),
            "{refused:?}"
        );
        let failed = undo_lines(METHOD, "promote_fencing_undo_failed");
        assert!(
            failed.len() == 1 && failed[0]["ours"] == true && failed[0]["revision"] == revision,
            "{failed:?}"
        );
        assert!(undo_lines(METHOD, "promote_fencing_undone").is_empty());
    }

    /// The four outcome logs of [`undo_unproven`] and the plain undo, each by count, so a row can
    /// say exactly which one it wrote.
    fn undo_outcomes(method: &str) -> [usize; 4] {
        [
            "promote_fencing_undone",
            "promote_fencing_undo_failed",
            "promote_fencing_undo_unknown",
            "promote_fencing_undo_overtaken",
        ]
        .map(|message| undo_lines(method, message).len())
    }

    /// PR #119 review round 3 (spec §7.3): the undo does not answer and the reread of
    /// `partitions/1` fails too. Nothing is known about the fence, so the refusal says that,
    /// carries both errors, and tells the operator to promote again, which reads it again. It
    /// claims neither an undone record nor a still-fenced one. Unit (~20 ms): `MemStore`.
    /// Cut: take a failed reread as still fenced (the `Err` arm returns the still-`Fencing`
    /// refusal and logs `promote_fencing_undo_failed`); red.
    #[config_log::retcd_test]
    fn undo_and_reread_failure_reports_unknown() {
        const METHOD: &str = "undo_and_reread_failure_reports_unknown";
        let rt = runtime();
        let (store, refused) = promote_with_undo(&rt, UndoFault::DroppedUnreadable);
        let PromoteError::Control(why) = &refused else {
            panic!("{refused:?}");
        };
        assert!(
            why.contains("whether it is still fenced is not known"),
            "{why}"
        );
        assert!(
            why.contains(&ConfigError::DeadlineExceededUnknownOutcome.to_string()),
            "the undo's error: {why}"
        );
        assert!(
            why.contains("admin fixture: partitions/1 unreadable"),
            "the reread's error: {why}"
        );
        assert!(!why.contains("nothing left to resume"), "{why}");
        assert_eq!(
            undo_outcomes(METHOD),
            [0, 0, 1, 0],
            "only the unknown outcome"
        );
        let (record, ..) = rt
            .block_on(read_partition(&store.inner, PARTITION))
            .expect("read");
        assert_eq!(record.lifecycle, PartitionLifecycle::Fencing, "{record:?}");
    }

    /// PR #119 review round 3 (spec §7.3): another promote commits partition 1 `Serving` under
    /// node 3 at the next generation just before this promote's undo, so the undo's CAS
    /// conflicts. The reread finds that record: the refusal names its owner, generation and
    /// revision and says nothing is left to resume, the record is left byte for byte as it was
    /// committed, and the only undo outcome logged is `promote_fencing_undo_overtaken`.
    /// Unit (~20 ms): `MemStore`.
    /// Cut: take any `Serving` reread as undone (`now == *before` becomes a lifecycle test); the
    /// refusal stays `OwnerAlive`, red.
    #[config_log::retcd_test]
    fn serving_commit_overtakes_undo() {
        const METHOD: &str = "serving_commit_overtakes_undo";
        let rt = runtime();
        let (store, refused) = promote_with_undo(&rt, UndoFault::Overtaken);
        let (bytes, revision) = store
            .overtook
            .lock()
            .expect("overtook")
            .clone()
            .expect("the newer record was committed");
        let PromoteError::Control(why) = &refused else {
            panic!("{refused:?}");
        };
        assert!(
            why.contains(&format!(
                "now names owner 3 at generation 4, Serving, revision {revision}"
            )) && why.ends_with("nothing left to resume"),
            "{why}"
        );
        let found = rt
            .block_on(store.inner.get(GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("get")
            .record
            .expect("partitions/1");
        assert_eq!(found.value, bytes, "the newer record's bytes are unchanged");
        assert_eq!(found.mod_revision, revision, "and so is its revision");
        assert_eq!(
            undo_outcomes(METHOD),
            [0, 0, 0, 1],
            "only the overtaken outcome"
        );
    }

    /// PR #119 review round 3 (spec §7.3): the owner comes back under a new, live grant right
    /// after the `Fencing` write, and the undo does not apply. The partition is read still
    /// `Fencing`, the owner's grant stays live and unfrozen, and the refusal says a promote
    /// resumes it without claiming the partition takes no writes: that grant may still admit
    /// them. Unit (~20 ms): `MemStore`.
    /// Cut: put the "takes no writes until a promote finishes" sentence back in the
    /// still-`Fencing` refusal; red.
    #[config_log::retcd_test]
    fn live_grant_dropped_undo_reports_control_state() {
        const METHOD: &str = "live_grant_dropped_undo_reports_control_state";
        let rt = runtime();
        let store = Arc::new(MovingGrant {
            inner: config_testkit::MemStore::new(),
            on_get: false,
            on_fencing: false,
            clear_on_fencing: false,
            undo: UndoFault::Dropped,
            replace_on_fencing: true,
            undo_tried: AtomicBool::new(false),
            overtook: std::sync::Mutex::new(None),
        });
        rooted(&store.inner, &rt, BootId(5));
        old_grant(&store.inner, &rt, GrantId(4));
        let refused = promote_above_one(Arc::clone(&store) as Arc<dyn ConfigStore>, &rt);
        let (record, _, _, revision, _, _) = rt
            .block_on(read_partition(&store.inner, PARTITION))
            .expect("read");
        assert_eq!(record.lifecycle, PartitionLifecycle::Fencing, "{record:?}");
        let PromoteError::Control(why) = &refused else {
            panic!("{refused:?}");
        };
        assert!(
            why.contains(&format!("is still Fencing at revision {revision}"))
                && why.ends_with("promote again"),
            "{why}"
        );
        assert!(!why.contains("no writes"), "no promise about writes: {why}");
        let grant = rt
            .block_on(store.inner.get(GetRequest {
                key: Bytes::from(ControlKey::Grant(OWNER).encode()),
            }))
            .expect("get")
            .record
            .expect("grants/1");
        let grant = GrantRecord::decode(&grant.value).expect("a grant");
        assert_eq!(grant.grant, LIVE_GRANT, "{grant:?}");
        assert!(!grant.frozen, "unfrozen: {grant:?}");
        assert!(grant.expiry_utc_ms > expiry_ahead(0), "live: {grant:?}");
        assert_eq!(
            undo_outcomes(METHOD),
            [0, 1, 0, 0],
            "only the still-fenced outcome"
        );
    }

    /// PR #103 review question: a target whose clock bound is lost, or that reports a qualifying
    /// copy, still holds a configuration, so its members are planned with and it is not called
    /// `Unconfigured`. A lost target bound refuses later, where the target commits (row 13d).
    /// Cut: match `clock_bound_lost: false` or `qualifying_copy: None` again, red.
    #[test]
    fn a_target_with_a_lost_clock_bound_or_a_qualifying_copy_is_still_configured() {
        let configured = |qualifying_copy, clock_bound_lost| crate::host::NodeStatus {
            node: NodeId(2),
            boot: OPEN_2,
            fault: None,
            holds: None,
            authority: "none".to_owned(),
            fence: None,
            fence_polling: false,
            replace_stopped: false,
            recovery: None,
            recovered: None,
            role: "secondary",
            protection: None,
            admits: None,
            published: None,
            in_flight: None,
            waits_on: None,
            stalled: None,
            reload_pending: None,
            lengths: None,
            pinned: members(),
            config_version: Some(CONFIG_VERSION),
            qualifying_copy,
            clock_bound_lost,
            committed_without: rdb_core::contracts::recovery::NoteState::Unknown,
            tail_refused: None,
        };
        for (qualifying_copy, clock_bound_lost) in
            [(None, true), (Some(true), false), (Some(false), true)]
        {
            let planned = promote_members(
                PARTITION,
                NodeId(2),
                Some(configured(qualifying_copy, clock_bound_lost)),
                Duration::from_secs(1),
            );
            assert_eq!(
                planned.ok(),
                Some((members(), CONFIG_VERSION)),
                "qualifying_copy={qualifying_copy:?} clock_bound_lost={clock_bound_lost}"
            );
        }
    }

    /// A clock `ahead_millis` fast whose guard has no bound: attached in a cluster of three, with
    /// no Agree heard.
    fn lost_bound_clock(ahead_millis: i64) -> HostClock {
        let clock = HostClock::guarded(ahead_millis);
        clock.guard().expect("a guarded clock").attach(3);
        assert!(
            !clock
                .sample(clock.now(), &Budgets::SPEC_DEFAULTS)
                .bound_established,
            "the bound is lost"
        );
        clock
    }

    /// PR #103 review (B2): a promote run on a clock with no bound cannot prove an expiry, so it
    /// must not fence the partition or freeze the owner's grant first, then time out with the
    /// freeze standing. Here the caller is a minute fast, the owner is unreachable and its grant
    /// is three seconds from expiry on an honest clock. Above one partition (the unguarded time
    /// `owner_dead` used would read the grant as expired and go on to freeze a live owner) and at
    /// one, the promote is refused `ClockUnbounded` with `partitions/1` and `grants/1` untouched.
    /// Cut: no bound check before the writes, red on the moved revision.
    /// Unit (~20 ms): `MemStore`.
    #[test]
    fn b2_a_promote_on_a_clock_with_no_bound_fences_and_freezes_nothing() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        old_grant(&*store, &rt, GrantId(4));
        let honest = i64::try_from(HostClock::start().now().0).expect("a tick");
        put_grant_expiry(&*store, &rt, honest + 3_000);
        let before = promote_marks(&*store, &rt);
        for owner_must_be_dead in [true, false] {
            let ctx = PromoteCtx {
                partition: PARTITION,
                owner_must_be_dead,
                look_within: Duration::from_millis(1),
                store: Arc::clone(&store),
                budgets: Budgets::SPEC_DEFAULTS,
                clock: lost_bound_clock(60_000),
                to: NodeId(2),
                drop: None,
                within: Duration::from_secs(1),
                members: members(),
                members_version: CONFIG_VERSION,
                member_boot: all_known(),
                send: Arc::new(|_, _| Err(NodeStopped(OWNER))),
            };
            let refused = rt.block_on(promote(ctx)).expect_err("refused");
            assert!(
                matches!(refused, PromoteError::ClockUnbounded),
                "owner_must_be_dead={owner_must_be_dead}: {refused:?}"
            );
            assert!(refused.no_mutation(), "{refused:?}");
            assert_eq!(
                promote_marks(&*store, &rt),
                before,
                "owner_must_be_dead={owner_must_be_dead}: nothing written"
            );
            unfrozen_and_serving(&*store, &rt);
        }
    }

    /// A target's `grants/` record that does not decode says nothing about whether it is frozen,
    /// so it is refused as the other grant reads refuse it, not read as "not frozen". Nothing is
    /// written and nothing sent. Cut: an undecodable record reads as not frozen, and the promote
    /// goes on. Unit (~20 ms): `MemStore`.
    #[test]
    fn a_promote_onto_a_node_with_an_undecodable_grant_is_refused() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        old_grant(&*store, &rt, GrantId(4));
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(ControlKey::Grant(NodeId(2)).encode()),
            value: Bytes::from_static(b"not a grant"),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put grants/2");
        let (answer, sent) = promote_once(
            &store,
            &rt,
            NodeId(2),
            None,
            members(),
            all_known(),
            Duration::from_secs(1),
        );
        let refused = answer.expect_err("the target's grant is unreadable");
        assert!(matches!(refused, PromoteError::Record(_)), "{refused:?}");
        assert!(
            refused.to_string().contains("is not a grant record"),
            "{refused}"
        );
        assert!(sent.is_empty(), "nothing sent: {}", sent.len());
        unfrozen_and_serving(&*store, &rt);
    }

    /// A promote onto a node whose grant is frozen would commit a partition no one can serve:
    /// nothing clears that record, so its new owner never admits. Refused by name, before
    /// anything is written (#61 critic Y3). Unit (~20 ms): `MemStore`.
    #[test]
    fn a_promote_onto_a_node_with_a_frozen_grant_is_refused_by_name() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        old_grant(&*store, &rt, GrantId(4));
        let frozen = GrantRecord {
            grant: GrantId(9),
            node: NodeId(2),
            boot: BootId(7),
            authority_generation: AuthorityGeneration(0),
            expiry_utc_ms: 1,
            frozen: true,
        };
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(ControlKey::Grant(NodeId(2)).encode()),
            value: frozen.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put grants/2");
        let (answer, sent) = promote_once(
            &store,
            &rt,
            NodeId(2),
            None,
            members(),
            all_known(),
            Duration::from_secs(1),
        );
        let refused = answer.expect_err("node 2 cannot serve");
        assert!(
            matches!(refused, PromoteError::TargetFrozen { node: 2, .. }),
            "{refused:?}"
        );
        assert!(refused.no_mutation(), "{refused:?}");
        assert!(sent.is_empty(), "nothing sent: {}", sent.len());
        let told = ApiError::from(refused).explain();
        assert!(
            told.ends_with("promote another node") && !told.contains("retrying will not help"),
            "{told}"
        );
        let (record, ..) = rt
            .block_on(read_partition(&*store, PARTITION))
            .expect("read");
        assert_eq!(record.lifecycle, PartitionLifecycle::Serving, "{record:?}");
    }

    fn put_grant_expiry(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime, expiry_utc_ms: i64) {
        let key = Bytes::from(ControlKey::Grant(OWNER).encode());
        let found = rt
            .block_on(store.get(GetRequest { key: key.clone() }))
            .expect("get")
            .record
            .expect("a grant");
        let held = GrantRecord::decode(&found.value).expect("a grant");
        rt.block_on(
            store.put(PutRequest {
                key,
                value: GrantRecord {
                    expiry_utc_ms,
                    ..held
                }
                .encode(),
                expected_mod_revision: Some(found.mod_revision),
                dedup: None,
            }),
        )
        .expect("put");
    }

    /// A-R163's read-before guards (A-R175): a `partitions/1` read served below the revision of
    /// the grant delete (case A) or of the read that found the grant absent (case B) is a
    /// lagging answer, so the heal refuses it and sends node 1 nothing. Unit (~20 ms):
    /// `MemStore` behind [`StalePartition`].
    #[test]
    fn a_partitions_read_older_than_the_grant_is_gone_is_refused_and_sends_nothing() {
        let rt = runtime();

        let store = Arc::new(StalePartition::new());
        rooted(&store.inner, &rt, BootId(5));
        store.freeze(&rt);
        old_grant(&store.inner, &rt, GrantId(4));
        let (healed, sent) = heal_once(Arc::clone(&store) as Arc<dyn ConfigStore>, &rt);
        assert!(
            matches!(healed, Err(HealError::ReadBeforeDelete { read, deleted }) if read < deleted),
            "case A: {healed:?}"
        );
        assert!(sent.is_empty(), "case A: {} messages sent", sent.len());

        let store = Arc::new(StalePartition::new());
        rooted(&store.inner, &rt, BootId(5));
        store.freeze(&rt);
        rt.block_on(store.inner.put(PutRequest {
            key: Bytes::from_static(b"unrelated"),
            value: Bytes::from_static(b"x"),
            expected_mod_revision: None,
            dedup: None,
        }))
        .expect("put unrelated");
        let (healed, sent) = heal_once(Arc::clone(&store) as Arc<dyn ConfigStore>, &rt);
        assert!(
            matches!(healed, Err(HealError::ReadBeforeAbsence { read, absent }) if read < absent),
            "case B: {healed:?}"
        );
        assert!(sent.is_empty(), "case B: {} messages sent", sent.len());
    }

    /// arch-s2 B1: the owner opens alone (`local=[1]`) and no link to node 2 or 3 comes up. The
    /// heal cannot pin their boots, so it ends `PeersUnknown` naming both and saying to start
    /// them. It waits for them first, so a heal that cannot pin them sends node 1 nothing. Unit
    /// (~150 ms): `MemStore`, a 100 ms budget.
    #[test]
    fn m10_s2_a_heal_with_no_link_to_nodes_2_and_3_names_them_and_sends_nothing() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let (owner, mailbox) = std::sync::mpsc::channel();
        let ctx = HealCtx {
            partition_count: 1,
            nodes: crate::db::ALL_NODES.into(),
            store: Arc::clone(&store),
            boot: OPEN_2,
            budgets: Budgets::SPEC_DEFAULTS,
            clock: HostClock::start(),
            owner,
            within: Duration::from_millis(100),
            member_boot: Arc::new(|node: NodeId| (node == OWNER).then_some(OPEN_2)),
            owner_node: OWNER,
            partitions: vec![PARTITION],
            bootstrap: Vec::new(),
        };
        let refused = rt
            .block_on(heal(ctx))
            .expect_err("nodes 2 and 3 have no link");
        assert!(
            matches!(&refused, HealError::PeersUnknown { nodes, .. } if nodes == &[2, 3]),
            "{refused:?}"
        );
        let text = refused.to_string();
        assert!(
            text.contains("start node 2 and 3, then open again"),
            "the text names both nodes and the next step: {text}"
        );
        assert_eq!(mailbox.try_iter().count(), 0, "node 1 was sent nothing");
    }

    // ---- admin promote (arch-s2 "Promote", A-R256) ----

    /// Messages a promote sent, by node.
    type Sent = Arc<std::sync::Mutex<Vec<(NodeId, Msg)>>>;

    /// A promote of `to` over `store`, with nodes 1 to 3 at `OPEN_2` unless `boots` says
    /// otherwise, and the rf3 `members`. Returns its answer and what it sent.
    fn promote_once(
        store: &Arc<dyn ConfigStore>,
        rt: &tokio::runtime::Runtime,
        to: NodeId,
        drop: Option<NodeId>,
        members: Vec<Member>,
        boots: Arc<dyn Fn(NodeId) -> Option<BootId> + Send + Sync>,
        within: Duration,
    ) -> (Result<Promoted, PromoteError>, Vec<(NodeId, Msg)>) {
        let sent: Sent = Arc::default();
        let log = Arc::clone(&sent);
        let ctx = PromoteCtx {
            partition: PARTITION,
            owner_must_be_dead: false,
            look_within: Duration::from_secs(1),
            store: Arc::clone(store),
            budgets: Budgets::SPEC_DEFAULTS,
            clock: HostClock::start(),
            to,
            drop,
            within,
            members,
            members_version: CONFIG_VERSION,
            member_boot: boots,
            send: Arc::new(move |node, msg| {
                log.lock().expect("log").push((node, msg));
                Ok(())
            }),
        };
        let answer = rt.block_on(promote(ctx));
        let sent = std::mem::take(&mut *sent.lock().expect("sent"));
        (answer, sent)
    }

    fn all_known() -> Arc<dyn Fn(NodeId) -> Option<BootId> + Send + Sync> {
        Arc::new(|_| Some(OPEN_2))
    }

    /// `partitions/1`'s mod revision and `grants/1`, as read now.
    fn promote_marks(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime) -> (u64, bool) {
        let get = |key: String| {
            rt.block_on(store.get(GetRequest {
                key: Bytes::from(key),
            }))
            .expect("get")
            .record
        };
        let partition = get(ControlKey::Partition(PARTITION).encode()).expect("partitions/1");
        (
            partition.mod_revision,
            get(ControlKey::Grant(OWNER).encode()).is_some(),
        )
    }

    /// A-R256: a promote that names a node it cannot make the owner is refused before it writes
    /// or sends anything, and each text says what to name instead: a node that holds no copy,
    /// a shadow (R2-M3: name a regular member), the owner itself, `drop` equal to `to`, and a
    /// member whose boot no link has reported. Cut: `Fencing` is written before the checks;
    /// `partitions/1`'s revision moves. Unit (~150 ms): `MemStore`.
    #[test]
    fn m10_s2_a_promote_refused_by_its_arguments_writes_and_sends_nothing() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let before = promote_marks(&*store, &rt);
        let shadowed: Vec<Member> = members()
            .into_iter()
            .map(|member| Member {
                role: if member.node == NodeId(3) {
                    ReplicaRole::Shadow
                } else {
                    member.role
                },
                ..member
            })
            .collect();
        let only_1: Arc<dyn Fn(NodeId) -> Option<BootId> + Send + Sync> =
            Arc::new(|node: NodeId| (node == OWNER).then_some(OPEN_2));
        let cases = [
            (
                NodeId(4),
                None,
                members(),
                all_known(),
                "node 4 holds no copy of partition 1; name a node that does",
            ),
            (
                NodeId(3),
                None,
                shadowed,
                all_known(),
                "node 3 is catching up (shadow); promote node 2 instead",
            ),
            (
                OWNER,
                None,
                members(),
                all_known(),
                "node 1 already owns partition 1; nothing was changed",
            ),
            (
                NodeId(2),
                Some(NodeId(2)),
                members(),
                all_known(),
                "the node to drop is the node to promote; drop another member, or name none to \
                 drop the owner",
            ),
            (
                NodeId(2),
                Some(NodeId(5)),
                members(),
                all_known(),
                "node 5 holds no copy of partition 1; name a node that does",
            ),
            (
                NodeId(2),
                None,
                members(),
                only_1,
                "no link to node 2 and 3 came up within 0.1 seconds, so the promote cannot pin \
                 its boot: start node 2 and 3, then promote again",
            ),
        ];
        for (to, drop, members, boots, text) in cases {
            let (answer, sent) = promote_once(
                &store,
                &rt,
                to,
                drop,
                members,
                boots,
                Duration::from_millis(100),
            );
            let refused = answer.expect_err("refused");
            assert_eq!(refused.to_string(), text);
            assert!(refused.no_mutation(), "{refused:?}");
            assert!(sent.is_empty(), "{refused:?}: {} messages sent", sent.len());
            assert_eq!(promote_marks(&*store, &rt), before, "{refused:?} wrote");
        }
    }

    /// A-R256, PR #41: node 1 is dead and holds no grant. The promote writes `partitions/1`
    /// `Fencing`, creates `grants/1` frozen with `GrantId(0)` ("none held"), waits
    /// `E + epsilon + delta` out, and hands node 2 the plan (nodes 2 and 3, node 2 primary, config
    /// version + 1) and an `ExpiryProven` fence whose `control_revision` is the `Fencing`
    /// record's. Nothing commits here, so it ends saying to promote again; doing so keeps the
    /// grant it left and fences again at a revision of its own (A-R447), and each run stores the
    /// plan it sent at `operations/` under its own revision. Cut: the created grant is not frozen.
    /// Unit (~1 s): `MemStore`.
    #[test]
    fn m10_s2_a_promote_of_an_owner_with_no_grant_freezes_a_new_one_and_fences_on_it() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let fenced_on = |sent: Vec<(NodeId, Msg)>| -> (FencingProof, RecoveryPlan) {
            let (nodes, msgs): (Vec<NodeId>, Vec<Msg>) = sent.into_iter().unzip();
            assert!(
                nodes.iter().all(|node| *node == NodeId(2)),
                "sent to node 2 only"
            );
            // A-R266: both as `Routed`, Plan first, so they cross the wire in one-process order.
            let events: Vec<&RecoveryEvent> = msgs
                .iter()
                .map(|msg| match msg {
                    Msg::Routed {
                        kind: KernelEvent::Recovery(event),
                        correlation: PROMOTE,
                        ..
                    } => event,
                    other => panic!("a promote sends Routed recovery events only: {other:?}"),
                })
                .collect();
            match events.as_slice() {
                [RecoveryEvent::Plan(plan), RecoveryEvent::FenceProven(fence)] => {
                    ((**fence).clone(), (**plan).clone())
                }
                other => panic!("Plan then FenceProven, once each: {other:?}"),
            }
        };
        let within = Duration::from_millis(600);
        let (answer, sent) =
            promote_once(&store, &rt, NodeId(2), None, members(), all_known(), within);
        let stopped = answer.expect_err("nothing commits here");
        assert!(
            stopped
                .to_string()
                .contains("node 2 did not commit partition 1")
                && stopped
                    .to_string()
                    .contains("node 2 refused the fence, its log has a routed_fence_refused line")
                && stopped.to_string().ends_with("promote again"),
            "{stopped}"
        );
        assert!(!stopped.no_mutation());

        let read = |key: String| {
            rt.block_on(store.get(GetRequest {
                key: Bytes::from(key),
            }))
            .expect("get")
            .record
            .expect("present")
        };
        let partition = read(ControlKey::Partition(PARTITION).encode());
        let record = PartitionRecord::decode(&partition.value).expect("record");
        assert_eq!(
            (record.owner, record.lifecycle),
            (OWNER, PartitionLifecycle::Fencing)
        );
        let grant =
            GrantRecord::decode(&read(ControlKey::Grant(OWNER).encode()).value).expect("grant");
        assert_eq!(
            (grant.grant, grant.node, grant.boot, grant.frozen),
            (GrantId(0), OWNER, BootId(5), true)
        );

        let (fence, plan) = fenced_on(sent);
        assert_eq!(
            (
                fence.prior_generation,
                fence.prior_owner_epoch,
                fence.prior_grant_id,
                fence.prior_boot_id,
                fence.control_revision,
            ),
            (
                Generation(3),
                OwnerEpoch(3),
                GrantId(0),
                BootId(5),
                Revision(partition.mod_revision)
            )
        );
        let Revocation::ExpiryProven {
            frozen_expiry_utc_ms,
            authority_utc_ms,
            epsilon_ms,
            delta_ms,
            ..
        } = fence.revocation
        else {
            panic!("not ExpiryProven: {:?}", fence.revocation);
        };
        assert_eq!(frozen_expiry_utc_ms, grant.expiry_utc_ms);
        assert!(
            authority_utc_ms > frozen_expiry_utc_ms + i64::from(epsilon_ms) + i64::from(delta_ms)
        );
        assert_eq!(
            plan.config.config_version,
            ConfigVersion(CONFIG_VERSION.0 + 1)
        );
        assert_eq!(
            plan.config
                .members
                .iter()
                .map(|member| (member.node, member.role))
                .collect::<Vec<_>>(),
            [
                (NodeId(2), ReplicaRole::Primary),
                (NodeId(3), ReplicaRole::RegularSecondary)
            ]
        );

        let (again, resent) =
            promote_once(&store, &rt, NodeId(2), None, members(), all_known(), within);
        again.expect_err("still nothing commits");
        let (refenced, replanned) = fenced_on(resent);
        // A-R447: the second run fences again, so it has a revision of its own, and keeps the
        // grant the first froze.
        assert!(
            refenced.control_revision > fence.control_revision
                && refenced.prior_grant_id == GrantId(0),
            "the second run fences at its own revision on the grant the first left: {refenced:?}"
        );
        assert_eq!(
            (
                stored_plan_at(&*store, &rt, fence.control_revision),
                stored_plan_at(&*store, &rt, refenced.control_revision)
            ),
            (Some(plan), Some(replanned)),
            "each run stored the plan it sent, at its own revision"
        );
    }

    /// A-R265 (W7's walk `lr kill 1; lr promote 2; lr restart 1`): a promote that wrote, then
    /// could not reach node 2, leaves `partitions/1` fencing and `grants/1` frozen. A reopen's
    /// heal of node 1 cannot clear that grant, and its text says what does, in words: the
    /// takeover is unfinished, run the promote again. The re-run then sends node 2 both events.
    /// Both ways a promote freezes: node 1 held grant 4 (the walk), or held none and the promote
    /// created grant 0 frozen (PR #41), which the heal once refused as "no grant issuer writes".
    /// Waiting never clears a frozen grant, so the heal answers at its first look, well inside
    /// its 10 s, not when its time runs out.
    /// Cuts: the text loses its step, red; a frozen grant 0 refused as unwritten again, red; the
    /// heal waits out its time on a frozen grant, as before, red.
    /// Unit (~2 s): `MemStore`, two promotes of 600 ms per case and a heal that answers at once.
    #[test]
    fn m10_s2_a_heal_after_a_promote_that_could_not_reach_its_node_says_to_promote_again() {
        const HEAL_WITHIN: Duration = Duration::from_secs(10);
        let rt = runtime();
        for held in [Some(GrantId(4)), None] {
            let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
            rooted(&*store, &rt, BootId(5));
            if let Some(grant) = held {
                old_grant(&*store, &rt, grant);
            }
            let ctx = PromoteCtx {
                partition: PARTITION,
                owner_must_be_dead: false,
                look_within: Duration::from_secs(1),
                store: Arc::clone(&store),
                budgets: Budgets::SPEC_DEFAULTS,
                clock: HostClock::start(),
                to: NodeId(2),
                drop: None,
                within: Duration::from_millis(600),
                members: members(),
                members_version: CONFIG_VERSION,
                member_boot: all_known(),
                send: Arc::new(|node, _| Err(NodeStopped(node))),
            };
            let refused = rt
                .block_on(promote(ctx))
                .expect_err("node 2 is unreachable");
            assert!(
                matches!(refused, PromoteError::Unreachable { node: 2, .. }),
                "{held:?}: {refused:?}"
            );
            assert!(!refused.no_mutation(), "it wrote Fencing and the freeze");

            let started = std::time::Instant::now();
            let (healed, sent) = heal_once_within(Arc::clone(&store), &rt, HEAL_WITHIN);
            let took = started.elapsed();
            let stuck = healed.expect_err("grants/1 stays frozen");
            assert_eq!(
                stuck.to_string(),
                "the last run's grant for node 1 (grants/1) is frozen: a takeover from node 1 is \
                 unfinished; run `admin promote <the node you chose>` again",
                "{held:?}"
            );
            let at_once = Duration::from_secs(1) * config_testkit::poll::deadline_scale();
            assert!(
                took < at_once,
                "{held:?}: the heal answered after {took:?}, not at its first look (its time is \
                 {HEAL_WITHIN:?})"
            );
            assert!(sent.is_empty(), "the heal sent node 1 nothing");

            let (again, resent) = promote_once(
                &store,
                &rt,
                NodeId(2),
                None,
                members(),
                all_known(),
                Duration::from_millis(600),
            );
            again.expect_err("nothing commits here");
            let resent: Vec<_> = resent
                .iter()
                .map(|(node, msg)| match msg {
                    Msg::Routed {
                        kind: KernelEvent::Recovery(RecoveryEvent::Plan(_)),
                        ..
                    } => (node.0, "plan"),
                    Msg::Routed {
                        kind: KernelEvent::Recovery(RecoveryEvent::FenceProven(_)),
                        ..
                    } => (node.0, "fence"),
                    other => panic!("not a routed plan or fence: {other:?}"),
                })
                .collect();
            assert_eq!(
                resent,
                [(2, "plan"), (2, "fence")],
                "{held:?}: the re-run sends node 2 its plan and fence"
            );
        }
    }

    /// A-R260: after a promote the owner is node 2, so a heal whose owner's thread is gone
    /// names node 2, not node 1. Cut: the text names `OWNER`. Unit (~10 ms): `MemStore`, a
    /// mailbox whose receiver is dropped.
    #[test]
    fn m10_s2_a_heal_whose_owner_stopped_names_that_owner() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        let written = rooted(&*store, &rt, BootId(5));
        let found = rt
            .block_on(store.get(GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("get")
            .record
            .expect("partitions/1");
        let root = PartitionRoot::decode(&found.value).expect("root");
        let record = PartitionRecord {
            owner: NodeId(2),
            ..PartitionRecord::decode(&found.value).expect("record")
        };
        put(&store, &rt, record.encode_with(&root), written);
        let (owner, mailbox) = std::sync::mpsc::channel();
        drop(mailbox);
        let ctx = HealCtx {
            partition_count: 1,
            nodes: crate::db::ALL_NODES.into(),
            store: Arc::clone(&store),
            boot: OPEN_2,
            budgets: Budgets::SPEC_DEFAULTS,
            clock: HostClock::start(),
            owner,
            within: Duration::from_millis(500),
            member_boot: Arc::new(|_| Some(OPEN_2)),
            owner_node: NodeId(2),
            partitions: vec![PARTITION],
            bootstrap: Vec::new(),
        };
        let stopped = rt.block_on(heal(ctx)).expect_err("node 2's thread is gone");
        assert_eq!(
            stopped.to_string(),
            "node 2 stopped before the heal reached it",
            "{stopped:?}"
        );
    }

    /// D4 (A-R398, arch-s2 S2-5 non-owner survivor): nodes 1 and 2 are dead, so only node 3's
    /// boot is known. `promote 3` still plans two members, node 3 primary and node 2 pinned at
    /// [`UNLINKED_BOOT`], which no process runs, so F1 counts only node 3 and commits
    /// `ReadOnly` (`mode_for(1)`). It used to wait out its whole time for node 2's handshake and
    /// refuse "start node 2", so a survivor whose peers are both dead could never serve reads.
    /// The target's own boot is still required: the row above, which names it, stays refused.
    /// Cut: a member with no handshake is refused `PeersUnknown` again, red.
    /// Unit (~1 s): `MemStore`, a 600 ms promote.
    #[test]
    fn m10_s2_d4_a_promote_with_its_other_member_dead_plans_it_unlinked_and_sends_the_plan() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let (answer, sent) = promote_once(
            &store,
            &rt,
            NodeId(3),
            None,
            members(),
            Arc::new(|node: NodeId| (node == NodeId(3)).then_some(OPEN_2)),
            Duration::from_millis(600),
        );
        let stopped = answer.expect_err("nothing commits here");
        assert!(
            stopped
                .to_string()
                .contains("node 3 did not commit partition 1"),
            "{stopped}"
        );
        let plans: Vec<&RecoveryPlan> = sent
            .iter()
            .filter_map(|(node, msg)| match msg {
                Msg::Routed {
                    kind: KernelEvent::Recovery(RecoveryEvent::Plan(plan)),
                    ..
                } if *node == NodeId(3) => Some(&**plan),
                _ => None,
            })
            .collect();
        let [plan] = plans.as_slice() else {
            panic!("one plan, to node 3: {sent:?}");
        };
        assert_eq!(
            plan.config
                .members
                .iter()
                .map(|member| (member.node, member.role, member.boot))
                .collect::<Vec<_>>(),
            [
                (NodeId(2), ReplicaRole::RegularSecondary, UNLINKED_BOOT),
                (NodeId(3), ReplicaRole::Primary, OPEN_2)
            ]
        );
    }

    /// A-R401 F1 (critic-d4 Q1): node 2's handshake comes after the promote pinned it unlinked
    /// but before the grant's expiry is proven. The plan sent then pins node 2 at its real
    /// boot, so F1 can count it, instead of at [`UNLINKED_BOOT`], which a live copy's proof
    /// never matches. Cut: no re-read after the expiry wait, red (node 2 at boot 0).
    /// Unit (~1 s): `MemStore`, a 600 ms promote; node 2 links at 250 ms, after the 150 ms
    /// unlinked bound.
    #[test]
    fn m10_s2_f1_a_member_that_links_during_the_expiry_wait_is_planned_at_its_boot() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let started = std::time::Instant::now();
        let (_, sent) = promote_once(
            &store,
            &rt,
            NodeId(3),
            None,
            members(),
            Arc::new(move |node: NodeId| {
                (node == NodeId(3) || started.elapsed() >= Duration::from_millis(250))
                    .then_some(OPEN_2)
            }),
            Duration::from_millis(600),
        );
        let boots: Vec<(NodeId, BootId)> = sent
            .iter()
            .find_map(|(_, msg)| match msg {
                Msg::Routed {
                    kind: KernelEvent::Recovery(RecoveryEvent::Plan(plan)),
                    ..
                } => Some(
                    plan.config
                        .members
                        .iter()
                        .map(|member| (member.node, member.boot))
                        .collect(),
                ),
                _ => None,
            })
            .expect("a plan was sent");
        assert_eq!(boots, [(NodeId(2), OPEN_2), (NodeId(3), OPEN_2)]);
    }

    /// A-R262: an `UNAVAILABLE` promote refusal says to promote again, and its next step agrees:
    /// try again, never "retrying will not help". The whole text an app shows, for a promote
    /// with no link to nodes 2 and 3. Cut: the rule is `NotWired`, and the step contradicts the
    /// text. Unit (~150 ms): `MemStore`, a 100 ms budget.
    #[test]
    fn m10_s2_a_promote_refused_unavailable_says_to_try_again_in_its_next_step_too() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        let (answer, sent) = promote_once(
            &store,
            &rt,
            NodeId(2),
            None,
            members(),
            Arc::new(|node: NodeId| (node == OWNER).then_some(OPEN_2)),
            Duration::from_millis(100),
        );
        assert!(sent.is_empty());
        let refused = ApiError::from(answer.expect_err("no link to nodes 2 and 3"));
        assert_eq!(
            refused.to_string(),
            "UNAVAILABLE (no link to node 2 and 3 came up within 0.1 seconds, so the promote \
             cannot pin its boot: start node 2 and 3, then promote again; next: the database is \
             not serving right now: wait a few seconds and try again; if it keeps refusing, \
             check that its nodes and its rEtcd are running (on a local run, \
             `scripts/local-rdb.sh status`))"
        );
    }

    // ---- A-R447: the plan a promote stores (issue #70) ----

    /// The plan stored for the promote that fenced at `revision`, read now.
    fn stored_plan_at(
        store: &dyn ConfigStore,
        rt: &tokio::runtime::Runtime,
        revision: Revision,
    ) -> Option<RecoveryPlan> {
        let key = promote_operation(revision).expect("in range").encode();
        rt.block_on(store.get(GetRequest {
            key: Bytes::from(key),
        }))
        .expect("get")
        .record
        .map(|found| decode_promote_plan(&found.value).expect("a stored plan"))
    }

    /// The fence and plan a promote sent node 2, Plan first.
    fn sent_pair(sent: &[(NodeId, Msg)]) -> (FencingProof, RecoveryPlan) {
        match sent {
            [(
                NodeId(2),
                Msg::Routed {
                    kind: KernelEvent::Recovery(RecoveryEvent::Plan(plan)),
                    ..
                },
            ), (
                NodeId(2),
                Msg::Routed {
                    kind: KernelEvent::Recovery(RecoveryEvent::FenceProven(fence)),
                    ..
                },
            )] => ((**fence).clone(), (**plan).clone()),
            other => panic!("Plan then FenceProven to node 2: {other:?}"),
        }
    }

    /// A-R447 step 6 (critic-63 A4): the create of the stored plan applies but its reply is
    /// lost. The promote reads it back, finds exactly its own plan, and carries on: node 2 is
    /// sent the plan and fence, and the record at the fence's revision holds the plan sent.
    /// Cut: a create not reported applied is refused, red (nothing sent).
    /// Unit (~1 s): `Scripted` over `MemStore`, a 600 ms promote that nothing commits.
    #[test]
    fn m10_s2_r447_a_promote_whose_plan_create_reply_was_lost_adopts_its_own_plan() {
        let rt = runtime();
        let store: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            lose_put_replies: 1,
            only_prefix: Some(b"operations/".to_vec()),
            ..Script::default()
        }));
        rooted(&*store, &rt, BootId(5));
        let within = Duration::from_millis(600);
        let (answer, sent) =
            promote_once(&store, &rt, NodeId(2), None, members(), all_known(), within);
        let stopped = answer.expect_err("nothing commits here");
        assert!(
            stopped.to_string().contains("node 2 did not commit"),
            "{stopped}"
        );
        let (fence, plan) = sent_pair(&sent);
        assert_eq!(
            stored_plan_at(&*store, &rt, fence.control_revision),
            Some(plan)
        );
    }

    /// A-R447 step 6: a stored plan the promote cannot stand behind is refused, and node 2 is
    /// sent nothing. (a) The create never applied and the read-back finds nothing. (b) The key
    /// already holds another plan. (c) The promoter may not write `operations/` (critic-63 A4):
    /// partition 1 is already fencing and frozen, so the text says so and names the access.
    /// (d) Any other create error, such as `Unavailable`, says only to promote again.
    /// Cuts: absent on read-back carries on, red; another plan adopted, red; the text loses
    /// `operations/`, red; every create error given the access text, red (d).
    /// Unit (~3 s): four 600 ms promotes over `Scripted`/`MemStore`.
    #[test]
    fn m10_s2_r447_a_plan_that_did_not_land_or_is_not_this_runs_refuses_and_sends_nothing() {
        let rt = runtime();
        let within = Duration::from_millis(600);
        let run = |store: Arc<dyn ConfigStore>| {
            let (answer, sent) =
                promote_once(&store, &rt, NodeId(2), None, members(), all_known(), within);
            assert!(sent.is_empty(), "sent {sent:?}");
            let refused = answer.expect_err("refused").to_string();
            assert!(refused.ends_with("promote again"), "{refused}");
            refused
        };

        let dropped: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            drop_puts: 1,
            only_prefix: Some(b"operations/".to_vec()),
            ..Script::default()
        }));
        rooted(&*dropped, &rt, BootId(5));
        let refused = run(dropped);
        assert!(
            refused.contains("is absent after a create that was not reported applied"),
            "(a) {refused}"
        );

        // (b) `rooted` wrote revision 1 and this put writes 2, so the Fencing CAS lands at 3.
        let taken: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*taken, &rt, BootId(5));
        let other = encode_promote_plan(&recovery_plan(
            LineageAnchor {
                lineage: Lineage {
                    partition: PARTITION,
                    generation: Generation(3),
                    owner_epoch: OwnerEpoch(3),
                },
                base_seq: Seq(7),
                base_digest: Digest::ROOT,
            },
            ConfigVersion(CONFIG_VERSION.0 + 1),
            members(),
            OPEN_2,
            GrantId(1),
            AuthorityGeneration(1),
            PriorRecord::fresh(),
        ));
        let written = rt
            .block_on(taken.put(PutRequest {
                key: Bytes::from(promote_operation(Revision(3)).expect("in range").encode()),
                value: other,
                expected_mod_revision: Some(0),
                dedup: None,
            }))
            .expect("put the other plan");
        assert_eq!(written.revision, 2);
        let refused = run(taken);
        assert!(refused.contains("holds another plan"), "(b) {refused}");

        let denied: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            fail_put_with: Some(ConfigError::PermissionDenied {
                detail: "no grant for operations/".to_owned(),
            }),
            only_prefix: Some(b"operations/".to_vec()),
            ..Script::default()
        }));
        rooted(&*denied, &rt, BootId(5));
        let refused = run(denied);
        assert!(
            refused.contains("partition 1 is fencing and takes no writes")
                && refused.contains("rEtcd write access to operations/"),
            "(c) {refused}"
        );

        // (d) Any other create error is not an access problem: the text says only to promote
        // again (review-63b A1).
        let unavailable: Arc<dyn ConfigStore> = Arc::new(Scripted::new(Script {
            fail_put_with: Some(ConfigError::Unavailable {
                reason: "no quorum".to_owned(),
            }),
            only_prefix: Some(b"operations/".to_vec()),
            ..Script::default()
        }));
        rooted(&*unavailable, &rt, BootId(5));
        let refused = run(unavailable);
        assert!(
            refused.contains("could not store the plan at operations/")
                && refused.contains("promote again")
                && !refused.contains("write access"),
            "(d) {refused}"
        );
    }

    /// #80 S5n row 32: a promote stopped after its `Fencing` write and before its plan leaves
    /// the note on the record, and (issue #105) the ledger. Cut: the `Fencing` CAS passes `None`:
    /// layout 4, red.
    /// Unit (<500 ms): the freeze after it is refused.
    #[test]
    fn s5n_32_a_promote_stopped_after_its_fencing_write_keeps_the_note() {
        use rdb_core::authority::partition::committed_without_of;
        use rdb_core::contracts::recovery::WithoutEntry;
        let rt = runtime();
        let scripted = Arc::new(Scripted::new(Script {
            pass_puts: 1,
            drop_puts: u32::MAX,
            ..Script::default()
        }));
        let mem = scripted.mem();
        rooted(mem, &rt, BootId(5));
        let found = rt
            .block_on(mem.get(GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("get")
            .record
            .expect("partitions/1");
        let note = CommittedWithout::default().with(WithoutEntry {
            generation: Generation(3),
            cutoff: Seq(7),
            copies: vec![(CopyId(0), NodeId(1))],
        });
        let head = PartitionRecord::decode(&found.value).expect("a record");
        let root = PartitionRoot::decode(&found.value).expect("a root");
        let pinned: Vec<RecordMember> = members().iter().map(RecordMember::of).collect();
        // Issue #105: the stored record carries a ledger, which the Fencing write keeps.
        let ledger = rdb_core::contracts::recovery::TailLedger {
            last_writable: Some(Generation(3)),
            last_writable_regulars: vec![NodeId(1), NodeId(2)],
            held: vec![rdb_core::contracts::recovery::HeldTail {
                node: NodeId(3),
                head: Seq(9),
            }],
        };
        let noted = head.encode_with_members(&root, &pinned, Some(&note), Some(&ledger));
        rt.block_on(mem.put(PutRequest {
            key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            value: noted,
            expected_mod_revision: Some(found.mod_revision),
            dedup: None,
        }))
        .expect("the noted record");
        put_grant(mem, &rt, expired_grant());
        let store: Arc<dyn ConfigStore> = Arc::clone(&scripted) as Arc<dyn ConfigStore>;

        let (promoted, sent) = promote_once(
            &store,
            &rt,
            NodeId(2),
            None,
            members(),
            all_known(),
            crate::host::test_patience(Duration::from_secs(1)),
        );

        assert!(promoted.is_err(), "{promoted:?}");
        assert!(sent.is_empty(), "no plan went out: {sent:?}");
        let after = rt
            .block_on(mem.get(GetRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
            }))
            .expect("get")
            .record
            .expect("partitions/1");
        assert_eq!(
            PartitionRecord::decode(&after.value).map(|r| r.lifecycle),
            Some(PartitionLifecycle::Fencing),
            "the Fencing write landed"
        );
        assert_eq!(
            after.value[0], 6,
            "issue #105: the Fencing write keeps the ledger too"
        );
        assert_eq!(committed_without_of(&after.value), Some(note));
        assert_eq!(
            rdb_core::authority::partition::ledger_of(&after.value),
            Some(ledger),
            "issue #105: L, its regulars and the mark, unchanged"
        );
    }

    /// A-R447 step 6: another promote fences `partitions/1` again between this run's `Fencing`
    /// write and its step 6. The record's bytes are the same, but its revision is not this run's,
    /// so this run refuses and sends nothing: the newer run owns the fence.
    /// The order is built, not timed (Revobot r4): every put after this run's `Fencing` CAS
    /// parks at the store's gate, so its freeze (step 4) waits there while the row CASes over
    /// `partitions/1` underneath, then goes on. Node 1's grant is long expired, so no clock
    /// decides anything.
    /// Cut: the revision check removed: this run stores and sends its plan at the other run's
    /// revision, red.
    /// Unit (<100 ms): `Scripted::gated_with`.
    #[test]
    fn m10_s2_r447_a_promote_fenced_over_by_another_run_refuses_and_sends_nothing() {
        let rt = runtime();
        let gated = Arc::new(Scripted::gated_with(Script {
            pass_gate: 1,
            ..Script::default()
        }));
        let mem = gated.mem();
        let rooted_at = rooted(mem, &rt, BootId(5));
        put_grant(mem, &rt, expired_grant());
        let store: Arc<dyn ConfigStore> = Arc::clone(&gated) as Arc<dyn ConfigStore>;
        let promoter = std::thread::spawn(move || {
            let rt = runtime();
            promote_once(
                &store,
                &rt,
                NodeId(2),
                None,
                members(),
                all_known(),
                crate::host::test_patience(Duration::from_secs(2)),
            )
        });
        rt.block_on(async {
            tokio::time::timeout(
                crate::host::test_patience(Duration::from_secs(2)),
                gated.put_arrived.notified(),
            )
            .await
        })
        .expect("this run's freeze parks at the gate");
        let found = partition_now(mem, &rt);
        assert!(
            found.mod_revision > rooted_at
                && PartitionRecord::decode(&found.value)
                    .is_some_and(|record| record.lifecycle == PartitionLifecycle::Fencing),
            "this run's own Fencing write is in: revision {}",
            found.mod_revision
        );
        let over = rt
            .block_on(mem.put(PutRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
                value: found.value,
                expected_mod_revision: Some(found.mod_revision),
                dedup: None,
            }))
            .expect("the other run's CAS");
        assert_eq!(over.outcome, MutationOutcome::Applied);
        gated.rescript(Script {
            pass_gate: u32::MAX,
            ..Script::default()
        });
        gated.release();
        let (answer, sent) = promoter.join().expect("the promote");
        let refused = answer.expect_err("fenced over").to_string();
        assert!(
            refused.contains("another promote is running; promote again"),
            "{refused}"
        );
        assert!(sent.is_empty(), "sent {sent:?}");
    }

    /// Node 1's grant, never frozen and long expired: a promote freezes it and proves its expiry
    /// at once.
    fn expired_grant() -> GrantRecord {
        GrantRecord {
            grant: GrantId(4),
            node: OWNER,
            boot: BootId(5),
            authority_generation: AuthorityGeneration(0),
            expiry_utc_ms: i64::try_from(HostClock::start().now().0).expect("fits") - 60_000,
            frozen: false,
        }
    }

    fn put_grant(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime, grant: GrantRecord) {
        rt.block_on(store.put(PutRequest {
            key: Bytes::from(ControlKey::Grant(OWNER).encode()),
            value: grant.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put grants/1");
    }

    /// A promote's answer and what it sent, as [`promote_once`] returns them.
    type Ran = (Result<Promoted, PromoteError>, Vec<(NodeId, Msg)>);

    /// One promote on its own thread. The receiver hears once it has sent its fence.
    fn promote_started(
        store: &Arc<dyn ConfigStore>,
        to: NodeId,
        drop: Option<NodeId>,
    ) -> (std::thread::JoinHandle<Ran>, std::sync::mpsc::Receiver<()>) {
        let (fenced, heard) = std::sync::mpsc::channel();
        let fenced = std::sync::Mutex::new(fenced);
        let sent: Sent = Arc::default();
        let log = Arc::clone(&sent);
        let ctx = PromoteCtx {
            partition: PARTITION,
            owner_must_be_dead: false,
            look_within: Duration::from_secs(1),
            store: Arc::clone(store),
            budgets: Budgets::SPEC_DEFAULTS,
            clock: HostClock::start(),
            to,
            drop,
            within: crate::host::test_patience(Duration::from_secs(3)),
            members: members(),
            members_version: CONFIG_VERSION,
            member_boot: all_known(),
            send: Arc::new(move |node, msg| {
                let fence = matches!(
                    msg,
                    Msg::Routed {
                        kind: KernelEvent::Recovery(RecoveryEvent::FenceProven(_)),
                        ..
                    }
                );
                log.lock().expect("log").push((node, msg));
                if fence {
                    fenced
                        .lock()
                        .expect("fenced")
                        .send(())
                        .expect("the row listens");
                }
                Ok(())
            }),
        };
        // The row's span, so a `#[retcd_test]` row reads the run's lines as its own.
        let span = tracing::Span::current();
        let run = std::thread::spawn(move || {
            let answer = span.in_scope(|| runtime().block_on(promote(ctx)));
            let sent = std::mem::take(&mut *sent.lock().expect("sent"));
            (answer, sent)
        });
        (run, heard)
    }

    /// What F1 writes when it commits a promote to `owner`: `partitions/1` owned by `owner` at
    /// the next generation, pinning the members of the plan stored at `revision`, as layout 4,
    /// by a CAS at `revision` (`recovery::commit::Cas`). The commit's revision, or `None` when
    /// the CAS did not apply. `layout_3` writes it pinning no members, as a build before A-R426
    /// would.
    fn commit_at(
        store: &dyn ConfigStore,
        rt: &tokio::runtime::Runtime,
        revision: Revision,
        owner: NodeId,
        layout_3: bool,
    ) -> Option<u64> {
        let plan = stored_plan_at(store, rt, revision).expect("the fence's stored plan");
        let found = partition_now(store, rt);
        let root = PartitionRoot::decode(&found.value).expect("root");
        let record = PartitionRecord::decode(&found.value).expect("record");
        let committed = PartitionRecord {
            owner,
            generation: Generation(record.generation.0 + 1),
            owner_epoch: OwnerEpoch(record.owner_epoch.0 + 1),
            lifecycle: PartitionLifecycle::Serving,
            ..record
        };
        let response = rt
            .block_on(store.put(PutRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
                value: if layout_3 {
                    committed.encode_with(&root)
                } else {
                    committed.encode_with_members(
                        &root,
                        &RecordMember::pinned(&plan.config),
                        None,
                        None,
                    )
                },
                expected_mod_revision: Some(revision.0),
                dedup: None,
            }))
            .expect("put");
        (response.outcome == MutationOutcome::Applied).then_some(response.revision)
    }

    /// Two promotes overlap, each `(to, drop)`, the earlier sent first. The later fences over it
    /// after the earlier has sent, and F1 commits the later fence: the earlier's commit CAS
    /// fails. Answers both runs' answers, earlier first, and the commit's revision.
    fn overlapped_after_send(
        rt: &tokio::runtime::Runtime,
        (earlier_to, earlier_drop): (NodeId, Option<NodeId>),
        (later_to, later_drop): (NodeId, Option<NodeId>),
    ) -> (
        Result<Promoted, PromoteError>,
        Result<Promoted, PromoteError>,
        u64,
    ) {
        let patience = crate::host::test_patience(Duration::from_secs(2));
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, rt, BootId(5));
        put_grant(&*store, rt, expired_grant());
        let (earlier, earlier_fenced) = promote_started(&store, earlier_to, earlier_drop);
        earlier_fenced
            .recv_timeout(patience)
            .expect("the earlier run sends its fence");
        let earlier_at = partition_now(&*store, rt).mod_revision;
        let (later, later_fenced) = promote_started(&store, later_to, later_drop);
        later_fenced
            .recv_timeout(patience)
            .expect("the later run sends its fence");
        let later_at = partition_now(&*store, rt).mod_revision;
        assert!(later_at > earlier_at, "the later run fenced over");
        assert_eq!(
            commit_at(&*store, rt, Revision(earlier_at), later_to, false),
            None,
            "the earlier fence is stale"
        );
        let committed = commit_at(&*store, rt, Revision(later_at), later_to, false)
            .expect("F1 commits the later");
        let (earlier, _) = earlier.join().expect("the earlier run");
        let (later, _) = later.join().expect("the later run");
        (earlier, later, committed)
    }

    /// Revobot r4, ruling A-R448 (step 7): two promotes to node 2 overlap. Each answers only for
    /// the commit it sees, and only Ok when that commit pins its own plan's members.
    /// (1) The later run (drop 3) fences over the earlier (drop 1) before the earlier reaches
    /// step 6: the earlier is refused "another promote is running" and sends nothing; the later
    /// answers Ok, dropped 3, at the commit's revision.
    /// (2) The later run fences over after the earlier has sent, and F1 commits the later one.
    /// The earlier answers "another promote committed instead", naming node 3, and does not
    /// claim no mutation. The later answers Ok, dropped 3.
    /// (3) As (2), both dropping node 1: the commit carries both runs' members, so both answer
    /// Ok, each with the commit's generation and revision, never its own fence's.
    /// (4) A commit pinning no members (layout 3) is taken as before, Ok, and logged
    /// `promote_outcome_unproven`.
    /// (5) A commit to node 2 pinning every member of the record (a rejoin re-added one) is not
    /// this run's: "partitions/1 changed again", never "node  dropped", never Ok (F-001).
    /// (6) The later run promotes node 3 and commits: the earlier (to 2) answers at once,
    /// "node 3 is the owner", before its step 7 deadline (F-002).
    /// Cuts: the member comparison dropped, (2) red; the empty-drop text, (5) red; the
    /// other-owner branch, (6) red.
    /// Unit (<1 s): `MemStore`, `Scripted::gated_with`, a stand-in for F1's commit CAS.
    #[config_log::retcd_test]
    fn m10_s2_r448_overlapping_promotes_each_answer_only_for_their_own_commit() {
        const METHOD: &str =
            "m10_s2_r448_overlapping_promotes_each_answer_only_for_their_own_commit";
        let rt = runtime();
        let patience = crate::host::test_patience(Duration::from_secs(2));

        // (1)
        let gated = Arc::new(Scripted::gated_with(Script {
            pass_gate: 1,
            ..Script::default()
        }));
        let mem = gated.mem();
        rooted(mem, &rt, BootId(5));
        put_grant(mem, &rt, expired_grant());
        let store: Arc<dyn ConfigStore> = Arc::clone(&gated) as Arc<dyn ConfigStore>;
        let (earlier, _) = promote_started(&store, NodeId(2), None);
        rt.block_on(async { tokio::time::timeout(patience, gated.put_arrived.notified()).await })
            .expect("the earlier run's freeze parks at the gate");
        let earlier_at = partition_now(mem, &rt).mod_revision;
        gated.rescript(Script {
            pass_gate: u32::MAX,
            ..Script::default()
        });
        let (later, later_fenced) = promote_started(&store, NodeId(2), Some(NodeId(3)));
        later_fenced
            .recv_timeout(patience)
            .expect("the later run sends its fence");
        gated.release();
        let (refused, sent) = earlier.join().expect("the earlier run");
        let refused = refused.expect_err("fenced over").to_string();
        assert!(
            refused.contains("another promote is running"),
            "(1) {refused}"
        );
        assert!(sent.is_empty(), "(1) the earlier run sent {sent:?}");
        let later_at = partition_now(mem, &rt).mod_revision;
        assert!(later_at > earlier_at, "(1) the later run fenced over");
        let committed =
            commit_at(mem, &rt, Revision(later_at), NodeId(2), false).expect("(1) F1 commits");
        let (answer, _) = later.join().expect("the later run");
        let done = answer.expect("(1) the later run commits");
        assert_eq!(
            (done.control_revision, done.dropped, done.generation),
            (Revision(committed), NodeId(3), Generation(4)),
            "(1) the later run answers for its own commit"
        );

        // (2)
        let (earlier, later, committed) =
            overlapped_after_send(&rt, (NodeId(2), None), (NodeId(2), Some(NodeId(3))));
        let superseded = earlier.expect_err("(2) the earlier run's plan did not commit");
        assert_eq!(
            superseded,
            PromoteError::Superseded {
                partition: 1,
                owner: None,
                dropped: vec![3],
                generation: 4
            },
            "(2) {superseded}"
        );
        assert_eq!(
            superseded.to_string(),
            "another promote committed instead: node 3 dropped, generation 4; run status before \
             promoting again",
            "(2)"
        );
        assert!(!superseded.no_mutation(), "(2) a promote did happen");
        let answered = ApiError::from(superseded.clone());
        assert_eq!(
            (answered.retry, answered.no_mutation),
            (RetryRule::NotWired, false),
            "(2) promoting again blindly will not help: {answered:?}"
        );
        let done = later.expect("(2) the later run commits");
        assert_eq!(
            (done.control_revision, done.dropped, done.generation),
            (Revision(committed), NodeId(3), Generation(4)),
            "(2) the later run answers for its own commit"
        );

        // (3)
        let (earlier, later, committed) =
            overlapped_after_send(&rt, (NodeId(2), None), (NodeId(2), None));
        for (run, answer) in [("earlier", earlier), ("later", later)] {
            let done = answer.unwrap_or_else(|error| panic!("(3) the {run} run: {error}"));
            assert_eq!(
                (done.control_revision, done.dropped, done.generation),
                (Revision(committed), NodeId(1), Generation(4)),
                "(3) the {run} run names the commit"
            );
        }

        // (4) A commit that pins no members (layout 3) cannot be told apart: taken as today, and
        // logged `promote_outcome_unproven`.
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        put_grant(&*store, &rt, expired_grant());
        let (run, fenced) = promote_started(&store, NodeId(2), Some(NodeId(3)));
        fenced
            .recv_timeout(patience)
            .expect("(4) the run sends its fence");
        let fenced_at = partition_now(&*store, &rt).mod_revision;
        let committed =
            commit_at(&*store, &rt, Revision(fenced_at), NodeId(2), true).expect("(4) F1 commits");
        let done = run.join().expect("the run").0.expect("(4) taken as today");
        assert_eq!(
            (done.control_revision, done.dropped, done.generation),
            (Revision(committed), NodeId(3), Generation(4)),
            "(4)"
        );
        let unproven: Vec<_> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
            .into_iter()
            .filter(|line| line["@m"] == "promote_outcome_unproven")
            .collect();
        assert_eq!(unproven.len(), 1, "(4) {unproven:#?}");
        assert_eq!(
            (
                &unproven[0]["@l"],
                &unproven[0]["owner"],
                &unproven[0]["generation"]
            ),
            (
                &serde_json::json!("Warning"),
                &serde_json::json!(2),
                &serde_json::json!(4)
            ),
            "(4) {}",
            unproven[0]
        );

        // (5) The commit pins the run's owner and every member of its record: a rejoin added the
        // dropped node back. Not this run's plan, and no node is missing to name.
        let store: Arc<dyn ConfigStore> = Arc::new(config_testkit::MemStore::new());
        rooted(&*store, &rt, BootId(5));
        put_grant(&*store, &rt, expired_grant());
        let (run, fenced) = promote_started(&store, NodeId(2), None);
        fenced
            .recv_timeout(patience)
            .expect("(5) the run sends its fence");
        let (record, root, pinned, fenced_at, _, _) = rt
            .block_on(read_partition(&*store, PARTITION))
            .expect("(5) partitions/1");
        let readded = PartitionRecord {
            owner: NodeId(2),
            generation: Generation(record.generation.0 + 1),
            owner_epoch: OwnerEpoch(record.owner_epoch.0 + 1),
            lifecycle: PartitionLifecycle::Serving,
            ..record
        };
        let put = rt
            .block_on(store.put(PutRequest {
                key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
                value: readded.encode_with_members(
                    &root,
                    &pinned.expect("(5) layout 4"),
                    None,
                    None,
                ),
                expected_mod_revision: Some(fenced_at),
                dedup: None,
            }))
            .expect("put");
        assert_eq!(put.outcome, MutationOutcome::Applied, "(5)");
        let changed = run.join().expect("the run").0.expect_err("(5) never Ok");
        assert_eq!(
            changed,
            PromoteError::Superseded {
                partition: 1,
                owner: None,
                dropped: vec![],
                generation: 4
            },
            "(5) {changed}"
        );
        assert_eq!(
            changed.to_string(),
            "partitions/1 changed again after a promote (generation 4); run status before \
             promoting again",
            "(5)"
        );

        // (6) The later run promotes node 3 over the earlier's promote of node 2, and F1 commits
        // it. The earlier answers at once, naming node 3, rather than waiting out its deadline
        // and advising "promote again".
        let within = crate::host::test_patience(Duration::from_secs(3));
        let started = std::time::Instant::now();
        let (earlier, later, committed) =
            overlapped_after_send(&rt, (NodeId(2), None), (NodeId(3), None));
        let took = started.elapsed();
        let superseded = earlier.expect_err("(6) node 3 owns, not node 2");
        assert_eq!(
            superseded,
            PromoteError::Superseded {
                partition: 1,
                owner: Some(3),
                dropped: vec![1],
                generation: 4
            },
            "(6) {superseded}"
        );
        assert_eq!(
            superseded.to_string(),
            "another promote committed instead: node 3 is the owner, generation 4; run status \
             before promoting again",
            "(6)"
        );
        assert!(
            took < within,
            "(6) answered after {took:?}, not before step 7's deadline of {within:?}"
        );
        let done = later.expect("(6) the later run commits");
        assert_eq!(
            (done.owner, done.control_revision, done.generation),
            (NodeId(3), Revision(committed), Generation(4)),
            "(6) the later run answers for its own commit"
        );
    }

    fn partition_now(store: &dyn ConfigStore, rt: &tokio::runtime::Runtime) -> config_core::Record {
        rt.block_on(store.get(GetRequest {
            key: Bytes::from(ControlKey::Partition(PARTITION).encode()),
        }))
        .expect("get")
        .record
        .expect("partitions/1")
    }

    /// Critic A3: a heal waited the whole heal deadline for a pinned shadow that was down
    /// before it left it out, so a reopen with a dead move target stalled every write for that
    /// long. Now a shadow with no boot is left out at once; regular members are still waited
    /// for. Paused tokio time: the wait is read off the virtual clock, so nothing sleeps.
    /// Cut: shadows waited for until the deadline again, red (10 s waited).
    #[test]
    fn m11_a3_a_heal_does_not_wait_on_a_down_shadow() {
        use rdb_core::contracts::membership::CopyId;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime");
        let (owner, _mailbox) = std::sync::mpsc::channel();
        let within = Duration::from_secs(10);
        let ctx = HealCtx {
            partition_count: 1,
            nodes: crate::db::ALL_NODES.into(),
            store: Arc::new(config_testkit::MemStore::new()),
            boot: OPEN_2,
            budgets: Budgets::SPEC_DEFAULTS,
            clock: HostClock::start(),
            owner,
            within,
            member_boot: Arc::new(|node| (node != NodeId(4)).then_some(OPEN_2)),
            owner_node: OWNER,
            partitions: vec![PARTITION],
            bootstrap: Vec::new(),
        };
        let pinned = [
            (1, ReplicaRole::Primary),
            (2, ReplicaRole::RegularSecondary),
            (3, ReplicaRole::RegularSecondary),
            (4, ReplicaRole::Shadow),
        ]
        .map(|(node, role)| RecordMember {
            copy: CopyId(node),
            node: NodeId(u32::from(node)),
            role,
        });
        let (members, waited) = rt.block_on(async {
            let start = tokio::time::Instant::now();
            let members = member_boots(&ctx, &pinned, start + within).await;
            (members, start.elapsed())
        });
        let nodes: Vec<u32> = members
            .expect("the regular members are known")
            .iter()
            .map(|member| member.node.0)
            .collect();
        assert_eq!(nodes, vec![1, 2, 3], "the down shadow is left out");
        assert!(
            waited < Duration::from_millis(100),
            "the heal waited {waited:?} for a down shadow"
        );
    }
}
