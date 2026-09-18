//! The stable, transport-independent client contract (spec §6.1).
//!
//! [`ConfigStore`] is the single client-facing interface. `DirectClient` (embedded) and
//! `GrpcClient` (remote) both implement it with identical semantics, and the conformance
//! suite runs against `Arc<dyn ConfigStore>` so neither implementation can quietly need an
//! extra method to pass.
//!
//! A direct client does **not** bypass consensus, authorization, leader confirmation, CAS, or
//! revision allocation. "Embedded" describes where the code runs, not which rules apply.
//!
//! `Watch` is deliberately absent. It arrives only in M4, after its journal, replay,
//! compaction, failover, and resource-isolation gates pass (spec §6.1, §11).

use async_trait::async_trait;

use crate::capabilities::Capabilities;
use crate::error::ConfigError;
use crate::types::{
    DeleteRequest, GetRequest, GetResponse, ListRequest, ListResponse, MutationResponse, PutRequest,
};

/// Read and mutate replicated configuration.
///
/// # Identity
///
/// No method takes a principal. The authenticated principal is bound to the implementation
/// when it is constructed — from the mTLS transport identity for a remote client, or from the
/// non-forgeable scoped handle an embedder passes to `ConfigNode::direct_client` — and
/// request fields never carry identity (spec §6.2, ADR-0012).
///
/// # Outcomes versus errors
///
/// A `CONFLICT` or `NOT_FOUND` mutation outcome is an `Ok(MutationResponse)`, not an `Err`
/// (spec §7.3). The `Err` cases are the [`ConfigError`] set: the request did not get a
/// decision, or the caller may not have one.
///
/// # Unknown outcomes
///
/// [`ConfigError::DeadlineExceededUnknownOutcome`] means the mutation may still commit.
/// An implementation must never replay the mutation on the caller's behalf, and a caller
/// must resolve the uncertainty by reading the key and issuing a CAS against the observed
/// `mod_revision` (ADR-0015).
#[async_trait]
pub trait ConfigStore: Send + Sync {
    /// Read one key at a leader-linearizable point.
    ///
    /// An absent key is `Ok` with `record: None`, not an error.
    async fn get(&self, request: GetRequest) -> Result<GetResponse, ConfigError>;

    /// Scan one prefix, bounded by the server's caps.
    ///
    /// A `truncated` response is not silently paginated: the caller narrows its prefix. There
    /// is no continuation token in the first release (spec §10.2).
    async fn list(&self, request: ListRequest) -> Result<ListResponse, ConfigError>;

    /// Write one key, optionally guarded by a compare-and-swap.
    ///
    /// A successful same-value `Put` is still a state-changing mutation: it allocates a
    /// revision and bumps `mod_revision` (spec §7.3).
    async fn put(&self, request: PutRequest) -> Result<MutationResponse, ConfigError>;

    /// Remove one key, optionally guarded by a compare-and-swap.
    ///
    /// `expected_mod_revision == Some(0)` is invalid and returns
    /// [`ConfigError::InvalidArgument`] without entering the log.
    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, ConfigError>;

    /// What this store actually guarantees (ADR-0016).
    ///
    /// Synchronous and cheap: it is a snapshot of static configuration, and a caller checking
    /// whether durability is real should not have to await a round trip to find out.
    fn capabilities(&self) -> Capabilities;
}
