//! The one place a semantic error becomes a transport status, and back (spec §6.2, ADR-0010).
//!
//! `config-core` classifies every [`ConfigError`] with [`ConfigError::kind`]; this module is
//! the only code in the tree that turns that classification into a [`tonic::Status`]. Keeping
//! both directions here means the server table and the client's inverse table cannot drift:
//! [`status_from_error`] and [`error_from_status`] are round-trip tested against every
//! variant.
//!
//! # What never goes on the wire
//!
//! Status messages carry the error's operator-facing `Display` only. `ConfigError` is
//! specified never to put a key or value in those fields, so a status message never leaks
//! request content. Structured detail travels as metadata instead, which also keeps the
//! message text non-load-bearing.

use config_core::{ConfigError, LeaderHint, NodeId, StatusClass};
use tonic::{Code, Status};

/// Metadata key carrying the leader's node id on a `FAILED_PRECONDITION` (ADR-0010).
pub const HEADER_LEADER_NODE_ID: &str = "retcd-leader-node-id";
/// Metadata key carrying the leader's client-plane endpoint on a `FAILED_PRECONDITION`.
pub const HEADER_LEADER_ENDPOINT: &str = "retcd-leader-endpoint";
/// Metadata key carrying `exists` for a [`ConfigError::Conflict`].
///
/// Additive to ADR-0010: `NotLeader` and `Conflict` share `FAILED_PRECONDITION`, so without a
/// structured marker a client could not tell them apart. It carries no key and no value.
pub const HEADER_CONFLICT_EXISTS: &str = "retcd-conflict-exists";
/// Metadata key carrying `current_mod_revision` for a [`ConfigError::Conflict`].
pub const HEADER_CONFLICT_MOD_REVISION: &str = "retcd-conflict-mod-revision";

/// Failures of the transport itself, as opposed to a semantic [`ConfigError`].
#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    /// A listener could not be bound or queried.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// tonic refused the server or channel configuration (usually TLS material).
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// A TLS profile could not be built from the supplied PEM material.
    #[error("tls configuration error: {0}")]
    Tls(String),
    /// An endpoint string was not a usable authority.
    #[error("invalid endpoint {endpoint:?}: {detail}")]
    InvalidEndpoint {
        /// The rejected endpoint.
        endpoint: String,
        /// Why it was rejected.
        detail: String,
    },
}

/// The gRPC code a [`StatusClass`] maps to (spec §6.2 normative table).
pub const fn code_for(class: StatusClass) -> Code {
    match class {
        StatusClass::InvalidArgument => Code::InvalidArgument,
        StatusClass::ResourceExhausted => Code::ResourceExhausted,
        StatusClass::Unavailable => Code::Unavailable,
        StatusClass::FailedPrecondition => Code::FailedPrecondition,
        StatusClass::NotFound => Code::NotFound,
        StatusClass::DeadlineExceeded => Code::DeadlineExceeded,
        StatusClass::PermissionDenied => Code::PermissionDenied,
        StatusClass::Unauthenticated => Code::Unauthenticated,
        StatusClass::Internal => Code::Internal,
    }
}

/// Map a semantic error onto the wire.
///
/// `NotLeader` with a validated hint additionally carries `retcd-leader-node-id` and
/// `retcd-leader-endpoint`; `Conflict` carries its two fields as metadata so the client can
/// reconstruct the variant rather than parsing prose.
pub fn status_from_error(err: &ConfigError) -> Status {
    let mut status = Status::new(code_for(err.kind()), err.to_string());
    match err {
        ConfigError::NotLeader { hint: Some(hint) } => {
            insert_ascii(
                &mut status,
                HEADER_LEADER_NODE_ID,
                hint.node_id.0.to_string(),
            );
            insert_ascii(&mut status, HEADER_LEADER_ENDPOINT, hint.endpoint.clone());
        }
        ConfigError::Conflict {
            exists,
            current_mod_revision,
        } => {
            insert_ascii(&mut status, HEADER_CONFLICT_EXISTS, exists.to_string());
            insert_ascii(
                &mut status,
                HEADER_CONFLICT_MOD_REVISION,
                current_mod_revision.to_string(),
            );
        }
        _ => {}
    }
    status
}

fn insert_ascii(status: &mut Status, key: &'static str, value: String) {
    if let Ok(value) = value.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>() {
        status.metadata_mut().insert(key, value);
    }
}

fn meta_str<'a>(status: &'a Status, key: &str) -> Option<&'a str> {
    status.metadata().get(key).and_then(|v| v.to_str().ok())
}

/// The inverse of [`status_from_error`], used by `config-client`.
///
/// `DEADLINE_EXCEEDED` becomes [`ConfigError::DeadlineExceededUnknownOutcome`], which is the
/// mutation reading (ADR-0015). A read path that produced the deadline itself should override
/// this with [`ConfigError::Unavailable`], because a read has no outcome to be unsure about.
pub fn error_from_status(status: &Status) -> ConfigError {
    let detail = status.message().to_string();
    match status.code() {
        Code::InvalidArgument => ConfigError::InvalidArgument { detail },
        Code::ResourceExhausted => ConfigError::ResourceExhausted { detail },
        Code::NotFound => ConfigError::NotFound,
        Code::DeadlineExceeded => ConfigError::DeadlineExceededUnknownOutcome,
        Code::PermissionDenied => ConfigError::PermissionDenied { detail },
        Code::Unauthenticated => ConfigError::Unauthenticated { detail },
        Code::Internal => ConfigError::FatalStorage { detail },
        Code::FailedPrecondition => failed_precondition(status),
        // Everything else — including a transport-level `UNAVAILABLE`, a cancelled call, or a
        // reset stream — is a rejection the caller may safely resubmit.
        _ => ConfigError::Unavailable { reason: detail },
    }
}

fn failed_precondition(status: &Status) -> ConfigError {
    if let Some(rev) = meta_str(status, HEADER_CONFLICT_MOD_REVISION).and_then(|v| v.parse().ok()) {
        return ConfigError::Conflict {
            exists: meta_str(status, HEADER_CONFLICT_EXISTS) == Some("true"),
            current_mod_revision: rev,
        };
    }
    ConfigError::NotLeader {
        hint: leader_hint(status),
    }
}

/// Read the leader hint a server attached to a `FAILED_PRECONDITION`, if any.
pub fn leader_hint(status: &Status) -> Option<LeaderHint> {
    let node_id: u64 = meta_str(status, HEADER_LEADER_NODE_ID)?.parse().ok()?;
    let endpoint = meta_str(status, HEADER_LEADER_ENDPOINT)?.to_string();
    (!endpoint.is_empty()).then_some(LeaderHint {
        node_id: NodeId(node_id),
        endpoint,
    })
}
