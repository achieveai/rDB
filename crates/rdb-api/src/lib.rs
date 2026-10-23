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
//!
//! # Values
//!
//! Every value an app writes is built from `rdb_api` paths alone, with no `rdb-value`
//! dependency (S4 A-8):
//!
//! ```
//! use rdb_api::{Decimal, Int, Timestamp, Value};
//!
//! let price = Decimal::new(-2, Int::new(1999).expect("an int")).expect("normalised");
//! let at = Timestamp::new(1_700_000_000, 5).expect("nanos in range");
//! let values = [Value::Decimal(price), Value::Timestamp(at)];
//! assert!(matches!(values[0], Value::Decimal(d) if d.exponent() == -2));
//! assert!(matches!(values[1], Value::Timestamp(t) if t == at));
//! ```
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
    app_request_id, ApiError, Db, DbConfig, GetOk, OpenError, PutError, PutOk, ReadOk, Timeouts,
    TxnPut, MAX_KEY, MAX_OBJECT_ID, MAX_OPS, MAX_PAGE, MAX_TXN_DEADLINE,
};
pub use host::{Change, ListEdit, Look, Seen};
// The rdb-core types `Db`'s calls take and answer with, so an app depends on rdb-api alone
// (A-R190): request ids, generations, the status answer and the error kinds.
pub use rdb_core::contracts::errors::{ErrorKind, RdbError, RetryRule};
pub use rdb_core::contracts::ids::{
    AffinityId, BootId, Generation, NodeId, OwnerEpoch, PartitionId, RequestId, RequestIdentity,
    Seq,
};
pub use rdb_core::contracts::txn::{Durability, Outcome, TxnRequest, TxnResult, TxnStatus};
// The value types an app builds a change from, by name only (S4 A-8): no `rdb_value` internals.
pub use rdb_value::blob::{Blob, Manifest, Upload};
pub use rdb_value::collection::{Collection, CollectionKind, ElemOp, Member, Members};
pub use rdb_value::delta::{Delta, Op};
pub use rdb_value::list::{Item, Items, List, Token};
pub use rdb_value::path::Path;
pub use rdb_value::value::{Decimal, Float, Int, Map, MapKey, Timestamp, Value};
pub use rdb_value::{Document, Expected};
