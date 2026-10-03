//! Topology: nodes, partitions, core sets, regular copies and shadows, all in one process.
//!
//! Arbitrary topology is test infrastructure, not permission to weaken the safety policy
//! (spike §1). A configuration with fewer than three regular copies is *allowed to be built* so
//! that failure states can be exercised — and must reject writes, not quietly accept them.
//!
//! # State
//!
//! The configuration types, [`ClusterConfig::validate`], [`Cluster::new`], [`Cluster::stop`],
//! [`Cluster::start`] and, since 2026-10-02, [`Cluster::suspend`] are real: a suspension queues
//! [`rdb_core::contracts::event::NodeLifecycle::Resumed`] through the scheduler, ahead of
//! whatever the node had queued in its window.

use std::collections::BTreeMap;

use rdb_core::contracts::event::{Event, EventKind, NodeLifecycle};
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, EventId, NodeId, PartitionId, ReplicaRole,
};
use rdb_core::contracts::membership::PartitionConfig;
use rdb_core::contracts::time::Tick;

use crate::error::SimError;
use crate::sim::scheduler::Scheduler;

/// One simulated machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeSpec {
    /// Its identity.
    pub node: NodeId,
    /// Its current process lifetime.
    pub boot: BootId,
    /// Its failure domain. Two copies in one domain do not satisfy "different machines"
    /// (spec §9.1), and a scenario may deliberately build a configuration that violates it.
    pub failure_domain: u16,
    /// How many core sets it runs. One core set owns one data engine (spec §4.1).
    pub core_sets: u8,
}

/// One partition's placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpec {
    /// The partition.
    pub partition: PartitionId,
    /// Who holds which copy, pinned by a configuration version.
    pub config: PartitionConfig,
}

/// A whole scenario topology.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterConfig {
    /// The machines, in ascending [`NodeId`] order.
    pub nodes: Vec<NodeSpec>,
    /// The partitions, in ascending [`PartitionId`] order.
    pub partitions: Vec<PartitionSpec>,
    /// The configuration version every partition starts at.
    pub initial_config_version: ConfigVersion,
}

impl ClusterConfig {
    /// Check the topology is internally consistent before anything runs.
    ///
    /// Rejects nodes out of order or duplicated, partitions out of order or duplicated, a spec
    /// whose config names another partition, a config [`PartitionConfig::validate`] refuses,
    /// members out of slot order or with a duplicate slot, a member naming an absent node, and
    /// more than one primary. It does **not** reject an under-replicated partition: that is a
    /// legal scenario whose correct behaviour is to refuse writes.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming the offending field.
    pub fn validate(&self) -> Result<(), SimError> {
        if !self
            .nodes
            .windows(2)
            .all(|pair| pair[0].node < pair[1].node)
        {
            return Err(SimError::Config { field: "nodes" });
        }
        if !self
            .partitions
            .windows(2)
            .all(|pair| pair[0].partition < pair[1].partition)
        {
            return Err(SimError::Config {
                field: "partitions",
            });
        }
        for spec in &self.partitions {
            if spec.config.partition != spec.partition {
                return Err(SimError::Config { field: "partition" });
            }
            if spec.config.validate().is_err() {
                return Err(SimError::Config {
                    field: "min_regular_acks",
                });
            }
            let members = &spec.config.members;
            if !members.windows(2).all(|pair| pair[0].copy < pair[1].copy) {
                return Err(SimError::Config { field: "members" });
            }
            if members
                .iter()
                .any(|member| !self.nodes.iter().any(|node| node.node == member.node))
            {
                return Err(SimError::Config { field: "member" });
            }
            if members
                .iter()
                .filter(|member| member.role == ReplicaRole::Primary)
                .count()
                > 1
            {
                return Err(SimError::Config { field: "primary" });
            }
        }
        Ok(())
    }

    /// Every node holding a copy of `partition` in `role`, in slot order. Empty for an unknown
    /// partition.
    #[must_use]
    pub fn copies(&self, partition: PartitionId, role: ReplicaRole) -> Vec<NodeId> {
        self.partitions
            .iter()
            .filter(|spec| spec.partition == partition)
            .flat_map(|spec| spec.config.members.iter())
            .filter(|member| member.role == role)
            .map(|member| member.node)
            .collect()
    }
}

/// The running topology.
///
/// Holds its state and is not `Copy` (finding K-F-29).
#[derive(Debug)]
pub struct Cluster {
    config: ClusterConfig,
    /// Each node's current boot.
    boots: BTreeMap<NodeId, BootId>,
    /// Stopped nodes, and whether the stop was a host crash.
    stopped: BTreeMap<NodeId, bool>,
    /// Each suspended node's resume tick, kept after it passes, so a second suspension inside
    /// the window is refused.
    suspended: BTreeMap<NodeId, Tick>,
    /// The next boot id to hand out: above every boot the configuration named.
    next_boot: BootId,
}

impl Cluster {
    /// Build a cluster from a configuration, validating it first.
    ///
    /// # Errors
    ///
    /// Whatever [`ClusterConfig::validate`] returns.
    pub fn new(config: ClusterConfig) -> Result<Self, SimError> {
        config.validate()?;
        let boots: BTreeMap<NodeId, BootId> = config
            .nodes
            .iter()
            .map(|node| (node.node, node.boot))
            .collect();
        let next_boot = BootId(boots.values().map(|boot| boot.0).max().unwrap_or(0) + 1);
        Ok(Self {
            config,
            boots,
            stopped: BTreeMap::new(),
            suspended: BTreeMap::new(),
            next_boot,
        })
    }

    /// The configuration this cluster was built from.
    #[must_use]
    pub const fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// The node's current boot, or `None` for a node the configuration does not name.
    #[must_use]
    pub fn boot(&self, node: NodeId) -> Option<BootId> {
        self.boots.get(&node).copied()
    }

    /// Whether the node is stopped, and if so whether by a host crash.
    #[must_use]
    pub fn stopped(&self, node: NodeId) -> Option<bool> {
        self.stopped.get(&node).copied()
    }

    /// Stop a node's process. Buffered-but-unflushed state may survive a process stop; a host
    /// stop discards it (spike §6). Which is which is
    /// [`crate::storage::crash_image::CrashImage::of`]'s job; this records the fact.
    /// A stop also ends any suspension: the restarted process was never paused.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `node` for an unknown node, or `stopped` for one already
    /// stopped.
    pub fn stop(&mut self, node: NodeId, host_crash: bool) -> Result<(), SimError> {
        if !self.boots.contains_key(&node) {
            return Err(SimError::Config { field: "node" });
        }
        if self.stopped.contains_key(&node) {
            return Err(SimError::Config { field: "stopped" });
        }
        self.stopped.insert(node, host_crash);
        // A suspension belongs to the process that stopped. The `Resumed` it queued stays under
        // that process's boot, which a run drops as stale; the restarted process is not suspended.
        self.suspended.remove(&node);
        Ok(())
    }

    /// Start a stopped node under a fresh [`BootId`], strictly above every boot seen so far.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `node` for an unknown node, or `stopped` for one that is not
    /// stopped.
    pub fn start(&mut self, node: NodeId) -> Result<BootId, SimError> {
        if !self.boots.contains_key(&node) {
            return Err(SimError::Config { field: "node" });
        }
        if self.stopped.remove(&node).is_none() {
            return Err(SimError::Config { field: "stopped" });
        }
        let boot = self.next_boot;
        self.next_boot = BootId(boot.0 + 1);
        self.boots.insert(node, boot);
        Ok(boot)
    }

    /// Suspend a node for `millis` of logical time, then resume it.
    ///
    /// Delivers [`NodeLifecycle::Resumed`] on resumption. That event is the only way a kernel
    /// module can learn it was stopped — it is not allowed to read a clock and notice a jump — and
    /// team kernel-a's monotonic admission rule depends on it.
    ///
    /// The resume is `scheduler.now() + millis`. The `Resumed { suspended_millis: millis }` event
    /// is queued there, under the node's current boot, node-scoped (partition and correlation
    /// zero), and its id is returned. Every event already queued for the node at or before the
    /// resume is what the paused process would have handled in its window: each is taken out
    /// ([`Scheduler::take_for`]) and queued again at the resume, in its old order and after the
    /// `Resumed`, so the node hears of its suspension before anything that waited through it.
    /// The node keeps its boot and is not stopped: a suspension is not a restart.
    ///
    /// **What this does not do.** The run loop does not consult the cluster — in a run, process
    /// liveness is [`crate::harness::dispatch::Dispatcher`]'s, as it is for [`Self::stop`] and
    /// [`Self::start`] — so an event queued for the node *after* this call, inside the window, is
    /// not held. Nothing schedules one between this call and the next pop unless the caller does.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `node` for an unknown node, `stopped` for a stopped one,
    /// `suspended` for one whose last suspension has not resumed yet at `scheduler.now()`, and
    /// `millis` for a zero-length suspension, which is no suspension. Each changes nothing. And
    /// whatever [`Scheduler::schedule`] returns.
    pub fn suspend(
        &mut self,
        node: NodeId,
        millis: u64,
        scheduler: &mut Scheduler,
    ) -> Result<EventId, SimError> {
        let Some(&boot) = self.boots.get(&node) else {
            return Err(SimError::Config { field: "node" });
        };
        if self.stopped.contains_key(&node) {
            return Err(SimError::Config { field: "stopped" });
        }
        if self
            .suspended
            .get(&node)
            .is_some_and(|resume| *resume > scheduler.now())
        {
            return Err(SimError::Config { field: "suspended" });
        }
        if millis == 0 {
            return Err(SimError::Config { field: "millis" });
        }
        let resume = scheduler.now().plus_millis(millis);
        let held = scheduler.take_for(node, resume);
        let id = scheduler.next_event_id();
        scheduler.schedule(Event {
            id,
            at: resume,
            node,
            boot,
            partition: PartitionId(0),
            correlation: CorrelationId(0),
            kind: EventKind::Node(NodeLifecycle::Resumed {
                suspended_millis: millis,
            }),
        })?;
        for event in held {
            let id = scheduler.next_event_id();
            scheduler.schedule(Event {
                id,
                at: resume,
                ..event
            })?;
        }
        self.suspended.insert(node, resume);
        Ok(id)
    }
}
