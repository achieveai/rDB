//! Topology: nodes, partitions, core sets, regular copies and shadows, all in one process.
//!
//! Arbitrary topology is test infrastructure, not permission to weaken the safety policy
//! (spike §1). A configuration with fewer than three regular copies is *allowed to be built* so
//! that failure states can be exercised — and must reject writes, not quietly accept them.
//!
//! # Seed state
//!
//! The configuration types are real; stepping the cluster is package H1.

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
    /// Rejects duplicate node ids, a partition whose members name an absent node, duplicate
    /// copy slots, and more than one primary. It does **not** reject an under-replicated
    /// partition: that is a legal scenario whose correct behaviour is to refuse writes.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming the offending field. [`SimError::Unavailable`] until package
    /// H1 lands the checks.
    pub const fn validate(&self) -> Result<(), SimError> {
        Err(SimError::unavailable(
            "sim::cluster::ClusterConfig::validate",
        ))
    }

    /// Every node holding a copy of `partition` in `role`.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the topology.
    pub const fn copies(
        &self,
        _partition: PartitionId,
        _role: ReplicaRole,
    ) -> Result<Vec<NodeId>, SimError> {
        Err(SimError::unavailable("sim::cluster::ClusterConfig::copies"))
    }
}

/// The running topology.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Cluster;

impl Cluster {
    /// Build a cluster from a validated configuration.
    ///
    /// # Errors
    ///
    /// Whatever [`ClusterConfig::validate`] returns. [`SimError::Unavailable`] until package H1
    /// lands the topology.
    pub fn new(_config: ClusterConfig) -> Result<Self, SimError> {
        Err(SimError::unavailable("sim::cluster::Cluster::new"))
    }

    /// Stop a node's process. Buffered-but-unflushed state may survive a process stop; a host
    /// stop discards it (spike §6).
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the topology.
    pub const fn stop(&mut self, _node: NodeId, _host_crash: bool) -> Result<(), SimError> {
        Err(SimError::unavailable("sim::cluster::Cluster::stop"))
    }

    /// Start a stopped node under a fresh [`BootId`].
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the topology.
    pub const fn start(&mut self, _node: NodeId) -> Result<BootId, SimError> {
        Err(SimError::unavailable("sim::cluster::Cluster::start"))
    }

    /// Suspend a node for `millis` of logical time, then resume it.
    ///
    /// Delivers [`rdb_core::contracts::event::NodeLifecycle::Resumed`] on resumption. That event
    /// is the only way a kernel module can learn it was stopped — it is not allowed to read a
    /// clock and notice a jump — and team kernel-a's monotonic admission rule depends on it.
    ///
    /// # Errors
    ///
    /// [`SimError::Unavailable`] until package H1 lands the topology.
    pub const fn suspend(&mut self, _node: NodeId, _millis: u64) -> Result<(), SimError> {
        Err(SimError::unavailable("sim::cluster::Cluster::suspend"))
    }
}
