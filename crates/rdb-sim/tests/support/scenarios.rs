//! Scenario generators and the campaign runner. **Owned by team verification, packages G1 and
//! Q1.**
//!
//! Registered here by team foundation so the seam exists on day one. Empty on purpose.
//!
//! A generator emits the operation list — [`rdb_sim::sim::network::NetworkOp`],
//! [`rdb_sim::storage::StorageOp`] and the client and control operations — that the harness
//! replays. Topology breadth, including how many partitions a scenario runs, is decided here:
//! [`rdb_sim::sim::cluster::ClusterConfig`] holds a `Vec<PartitionSpec>` and fixes nothing.
