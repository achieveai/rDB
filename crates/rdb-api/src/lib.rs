//! rDB embedded API and host (M9).
//!
//! An embedder calls [`Db`] and gets a published, generation-stamped result from a real host:
//! the six M7 kernel modules run on wall-clock time, real RocksDB and real rEtcd records.
//!
//! # Layout (M9 architecture §3)
//!
//! * [`host`] — one thread per rDB node. It solely owns that node's `RocksEngine`, the six
//!   modules and the adopted triple, and runs the duties no kernel emits (§4).
//! * [`transport`] — [`transport::Links`], the in-process peer links, with a per-link hold.
//! * [`control`] — [`control::ControlAdapter`], `ControlEffect` onto a `config_core::ConfigStore`.
//! * [`admin`] — bootstrap of partition 1 through F1. It never injects `Recovered`.
//! * [`db`] — [`Db`]: identities, deadlines, compiles and the §5.4 error mapping.
//!
//! All M9 nodes run in one process (Decision 1).
#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod admin;
pub mod clock;
pub mod control;
pub mod db;
pub mod host;
pub mod transport;

#[cfg(test)]
mod test_store;

pub use db::{
    ApiError, Db, DbConfig, GetOk, OpenError, PutError, PutOk, Timeouts, TxnPut, MAX_TXN_DEADLINE,
};
