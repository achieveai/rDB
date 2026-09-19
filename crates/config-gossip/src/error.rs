//! Typed errors for the gossip adapter.
//!
//! Underlying `memberlist` errors are rendered to strings so that no `memberlist` type leaks
//! through this crate's public API (ADR-0004).

/// A gossip operation failed.
///
/// Per ADR-0003 only *startup* problems are errors. Once a node is running, join and probe
/// failures are logged at `warn` and never surface here — gossip must not be able to fail
/// the node.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum GossipError {
    /// The encoded hint exceeds the gossip metadata budget.
    ///
    /// Returned *before* the node is started; `memberlist` itself panics on oversized
    /// metadata, so the check happens here instead.
    #[error(
        "advertised hint encodes to {size} bytes, over the {limit}-byte gossip metadata limit"
    )]
    HintTooLarge {
        /// Encoded size in bytes.
        size: usize,
        /// Hard limit ([`crate::MAX_HINT_BYTES`]).
        limit: usize,
    },

    /// The hint could not be serialized at all.
    #[error("failed to encode advertised hint: {0}")]
    HintEncode(String),

    /// The configuration is not usable (for example, an unrepresentable gossip label).
    #[error("invalid gossip configuration: {0}")]
    Config(String),

    /// The `memberlist` node could not be created or bound.
    #[error("failed to start gossip node: {0}")]
    Start(String),

    /// Re-advertising the local hint failed.
    #[error("failed to re-advertise local hint: {0}")]
    Advertise(String),
}

/// A peer advertised metadata this node could not decode.
///
/// Always advisory: the peer is skipped and logged at `warn`; it is never fatal.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HintDecodeError {
    /// The metadata was empty, or its body did not parse as a hint.
    #[error("malformed gossip metadata ({len} bytes): {message}")]
    Malformed {
        /// Length of the metadata that failed to decode.
        len: usize,
        /// Decoder message.
        message: String,
    },

    /// The metadata's leading byte is not [`crate::HINT_WIRE_VERSION`].
    ///
    /// Emitted for a peer running a build whose hint layout this one cannot interpret.
    /// Trailing bytes alone never produce this — only a different version byte does.
    #[error("unsupported gossip metadata wire version {0}")]
    UnsupportedVersion(u8),
}
