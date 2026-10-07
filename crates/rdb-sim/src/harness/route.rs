//! Kernel-to-kernel routing, re-exported from [`rdb_core::route`].
//!
//! The table moved to `rdb-core` in M9 so the simulator and the real host in `rdb-api` share one
//! routing table. Everything here is the same item under its old path.

pub use rdb_core::route::*;
