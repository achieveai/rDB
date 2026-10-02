//! Version gating: what this build understands, and the one function that refuses what it does
//! not.
//!
//! The rule the spike names twice (§4 "unknown mandatory versions fail before apply",
//! validation-plan V12) is not "reject unknown versions somewhere". It is **refuse before any
//! decode of the body**. A decoder that parses the payload and then checks the version has
//! already trusted bytes written by a newer protocol. So every artifact in this crate has a
//! fixed-width header that can be read on its own, and [`check_mandatory`] runs between reading
//! that header and touching anything after it.

use serde::{Deserialize, Serialize};

use crate::contracts::errors::RdbError;

/// Version of the client API contract (`TxnRequest.api_version`, spec §5.1).
pub const API_VERSION: u16 = 1;

/// Version of the replication envelope (`protocol_version`, spec §6.1).
pub const ENVELOPE_VERSION: u16 = 1;

/// Version of a control record body stored in rEtcd (spec §7.1).
pub const CONTROL_RECORD_VERSION: u16 = 1;

/// Version of the canonical trace format (spike §4, trace seam).
///
/// A trace is replayable only within one schema version *and* one generator version; the seed
/// alone is never sufficient. See [`crate::contracts::trace`].
pub const TRACE_SCHEMA_VERSION: u16 = 1;

/// Which versioned artifact a version number came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum VersionedArtifact {
    /// A client request or result (spec §5.1).
    Api,
    /// A replication envelope (spec §6.1).
    Envelope,
    /// A control record read from or written to rEtcd (spec §7.1).
    ControlRecord,
    /// A recorded trace (spike §4).
    Trace,
}

impl VersionedArtifact {
    /// The version range this build accepts for the artifact, inclusive.
    ///
    /// One version each today. The pair exists so that a later milestone can widen the range
    /// without changing any call site, which is what "unknown *mandatory* version" means: a
    /// version inside the range may carry fields this build ignores; a version outside it may
    /// not be decoded at all.
    #[must_use]
    pub const fn supported(self) -> (u16, u16) {
        match self {
            Self::Api => (API_VERSION, API_VERSION),
            Self::Envelope => (ENVELOPE_VERSION, ENVELOPE_VERSION),
            Self::ControlRecord => (CONTROL_RECORD_VERSION, CONTROL_RECORD_VERSION),
            Self::Trace => (TRACE_SCHEMA_VERSION, TRACE_SCHEMA_VERSION),
        }
    }
}

/// Refuse an artifact whose mandatory version this build does not support.
///
/// Call this with the version read from the artifact's fixed header, *before* decoding its body.
///
/// # Errors
///
/// [`RdbError::IncompatibleVersion`] when `found` falls outside
/// [`VersionedArtifact::supported`].
pub const fn check_mandatory(artifact: VersionedArtifact, found: u16) -> Result<(), RdbError> {
    let (min, max) = artifact.supported();
    if found < min || found > max {
        return Err(RdbError::IncompatibleVersion {
            artifact,
            found,
            min,
            max,
        });
    }
    Ok(())
}
