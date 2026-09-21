//! Topology: nodes, partitions, core sets, regular copies and shadows, all in one process.
//!
//! Arbitrary topology is test infrastructure, not permission to weaken the safety policy
//! (spike §1). A configuration with fewer than three regular copies is *allowed to be built* so
//! that failure states can be exercised — and must reject writes, not quietly accept them.
//!
//! # State
//!
//! The configuration types, [`ClusterConfig::validate`], [`Cluster::new`], [`Cluster::stop`]
//! and [`Cluster::start`] are real. [`Cluster::suspend`] is owed: it needs the scheduler to
//! deliver [`rdb_core::contracts::event::NodeLifecycle::Resumed`], and says so.

use std::collections::BTreeMap;

use rdb_core::contracts::ids::{BootId, ConfigVersion, NodeId, PartitionId, ReplicaRole};
use rdb_core::contracts::membership::PartitionConfig;

use crate::error::SimError;

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
    /// Delivers [`rdb_core::contracts::event::NodeLifecycle::Resumed`] on resumption. That event
    /// is the only way a kernel module can learn it was stopped — it is not allowed to read a
    /// clock and notice a jump — and team kernel-a's monotonic admission rule depends on it.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 wires the resume event into the scheduler.
    pub fn suspend(&mut self, _node: NodeId, _millis: u64) -> Result<(), SimError> {
        Err(SimError::unavailable("sim::cluster::Cluster::suspend"))
    }
}
