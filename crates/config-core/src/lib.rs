//! rEtcd core types (M0): stable requests/responses, typed errors, the `ConfigStore` trait,
//! the deterministic KV state machine, the versioned command envelope, identity, authorization
//! contracts, and capability reporting. No async runtime, no network, no I/O (ADR-0004).
//!
//! # What lives here, and why it is small
//!
//! Dependencies point inward to this crate, so anything it names becomes part of every other
//! crate's vocabulary. It therefore holds semantics and nothing else: no consensus, no
//! storage, no transport, no ambient inputs. That is what lets the M0 gate — determinism,
//! CAS, revision allocation — be proved by plain synchronous tests with no harness.
//!
//! # The M0 surface
//!
//! * [`Command`] — the only payload that enters a Raft log entry, with a canonical versioned
//!   encoding ([`Command::encode`]).
//! * [`KvState`] — the deterministic state machine. [`KvState::apply`] is synchronous, total,
//!   and infallible, and [`KvState::state_hash`] is the oracle two replicas compare.
//! * [`validate_put`] / [`validate_delete`] / [`validate_list`] — one validator, used by both
//!   the API edge and apply, so a crafted log entry is rejected identically everywhere.
//! * [`ConfigStore`] — the client contract `DirectClient` and `GrpcClient` both implement.
//! * [`ConfigError`] with [`ConfigError::kind`] — semantic errors plus their transport class,
//!   named without a transport dependency.
//! * [`Principal`] / [`Authorizer`] and [`Capabilities`] — the authorization hook and the
//!   honest self-report a node publishes.
//!
//! # Determinism rules this crate must keep
//!
//! Apply must not consult a wall clock, an entropy source, ambient process state, external services,
//! platform-dependent normalization, or unordered iteration (spec §7.4). This is checked
//! mechanically by a source scan and a dependency assertion in `tests/m0_purity.rs`, not by
//! review.
#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod authz;
pub mod capabilities;
pub mod command;
pub mod error;
pub mod hint;
pub mod identity;
pub mod limits;
pub mod policy;
pub mod state;
pub mod store;
pub mod types;
pub mod validate;

pub use authz::{
    audit, Action, AllowAll, AllowlistPolicy, Authorizer, Decision, Grant, Principal,
    PrincipalKind, StaticAllowlist,
};
pub use capabilities::{
    Authz, Capabilities, Dedup, Durability, Pagination, TransportSecurity, WatchResumption,
};
pub use command::{
    principal_hash, Command, CommandResponse, DecodeError, DedupKey, DedupStamp, MutationEvent,
    MutationEventKind, COMMAND_ENVELOPE_VERSION, COMMAND_MAGIC, OP_COMPACT, OP_DELETE, OP_PUT,
    OP_RETIRE_NODE,
};
pub use error::{
    ConfigError, LeaderHint, PageTokenExpiredReason, StatusClass, REASON_POLICY_CHANGED,
    REASON_POLICY_CONVERGING, REASON_PREFIX_MISMATCH, REASON_TOKEN_PRINCIPAL,
    UNAVAILABLE_FEATURE_NOT_ACTIVATED,
};
pub use hint::{GossipObservationSource, Liveness, NoGossip, ObservedPeerHint};
pub use identity::{
    ClusterId, ClusterIdentity, IdentityMismatch, NodeId, RecoveryEpoch, RestoredFrom,
};
pub use limits::{DedupLimits, Limits, WatchLimits, WatchRetention, LIST_RECORD_OVERHEAD_BYTES};
pub use policy::{
    changed_prefixes, evaluate_converging, verify_policy, Adoption, PolicyDocument, PolicyRejected,
    PolicySignature, PolicyState, SignedPolicy, SignedPolicyAuthorizer, VerifyingKey,
    REASON_NO_VALID_POLICY,
};
pub use state::{
    dedup_index_key_from_storage, dedup_storage_key, ApplyEffects, DedupIndexKey, DedupRecord,
    KvState,
};
pub use store::{
    bind_hash, open_token, seal_token, token_fingerprint, ConfigStore, ListPage, PageRequest,
    PageToken, WatchItem, WatchRequest, WatchStream, PAGE_TOKEN_VERSION,
};
pub use types::{
    DeleteRequest, GetRequest, GetResponse, ListRequest, ListResponse, MutationOutcome,
    MutationResponse, PutRequest, Record,
};
pub use validate::{validate_command, validate_delete, validate_get, validate_list, validate_put};
