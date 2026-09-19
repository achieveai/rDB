//! rEtcd gRPC transport: two planes, one status table (ADR-0010, spec §6.2, §15.1).
//!
//! # Two planes, never one
//!
//! A node listens twice. The **client plane** ([`serve_client_plane`]) serves
//! `ConfigService` to applications and derives a [`config_core::Principal`] from the client
//! certificate. The **peer plane** ([`serve_peer_plane`]) carries OpenRaft RPCs between
//! cluster members and derives a *node* identity from the peer certificate. They have
//! separate listeners and separate TLS profiles because they have different trust
//! populations: an application certificate must never be able to speak Raft.
//!
//! # What this crate is allowed to decide
//!
//! Nothing semantic. Validation, authorization, consensus, and revision allocation happen
//! below it; this crate moves bytes and maps errors. The mapping itself lives in one place
//! ([`error`]) and is asserted in both directions, because a transport that quietly reclassed
//! an error would turn a "safe to retry" answer into a duplicate write (ADR-0015). For the
//! same reason every status a plane emits is marked with [`HEADER_OUTCOME`]: an error status
//! *without* that marker was minted by the transport and says nothing about whether the
//! request was applied.
//!
//! # Wire encoding
//!
//! Protobuf is compiled at build time by `protox` — a pure-Rust compiler — so no `protoc`
//! binary is needed on any developer or CI machine (ADR-0017). OpenRaft payloads travel as
//! opaque postcard bytes inside [`pb::PeerEnvelope`] — a binary serde format that carries a
//! byte string as itself rather than expanding it, so a megabyte value stays a megabyte on the
//! wire; the envelope's identity fields are checked before the payload is decoded (ADR-0011).
//!
//! # Example: serving a store over the client plane
//!
//! ```no_run
//! use std::sync::Arc;
//! use config_core::{ClusterId, ConfigStore, Limits, Principal};
//! use config_grpc::{serve_client_plane, ClientBackend, TlsMode};
//!
//! struct OneStore(Arc<dyn ConfigStore>);
//! impl ClientBackend for OneStore {
//!     fn store_for(&self, _principal: Principal) -> Arc<dyn ConfigStore> {
//!         self.0.clone()
//!     }
//! }
//!
//! # async fn run(store: Arc<dyn ConfigStore>, cluster_id: ClusterId)
//! # -> Result<(), Box<dyn std::error::Error>> {
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
//! let handle = serve_client_plane(
//!     Arc::new(OneStore(store)),
//!     listener,
//!     TlsMode::Insecure,
//!     cluster_id,
//!     Limits::DEFAULT,
//! )?;
//! println!("serving on {}", handle.local_addr());
//! handle.shutdown().await?;
//! # Ok(()) }
//! ```
#![deny(missing_docs)]
#![forbid(unsafe_code)]
// `tonic::Status` is a large error type and it is the error type of every generated service
// method, so the lint fires on code whose signature this crate does not choose.
#![allow(clippy::result_large_err)]

/// Generated Protobuf types and service stubs for package `retcd.v1`.
///
/// Regenerated on every build from `proto/retcd/v1/*.proto`; field numbers there are
/// normative and must never be reused (spec §6.2).
pub mod pb {
    #![allow(missing_docs)]
    #![allow(clippy::doc_overindented_list_items)]
    tonic::include_proto!("retcd.v1");
}

mod convert;

pub mod client_plane;
pub mod error;
pub mod limits;
pub mod peer_plane;
pub mod server;
pub mod tls;
pub mod transport;

pub use client_plane::{serve_client_plane, ClientBackend};
pub use error::{
    code_for, error_from_status, is_server_rejection, leader_hint, mark_rejected,
    status_from_error, GrpcError, HEADER_CONFLICT_EXISTS, HEADER_CONFLICT_MOD_REVISION,
    HEADER_LEADER_ENDPOINT, HEADER_LEADER_NODE_ID, HEADER_OUTCOME, OUTCOME_REJECTED,
};
pub use limits::{
    client_plane_message_limit, peer_plane_message_limit, MESSAGE_FRAMING_SLACK_BYTES,
};
pub use peer_plane::{serve_peer_plane, status_from_reject, PeerIdentity};
pub use server::ServerHandle;
pub use tls::{peer_server_domain, CertIdentity, MtlsConfig, TlsMode};
pub use transport::GrpcPeerTransport;
