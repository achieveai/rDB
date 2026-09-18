//! Compact encoding of [`ObservedPeerHint`] into gossip node metadata.
//!
//! `memberlist` caps node metadata at 512 bytes and **panics** if a delegate returns more, so
//! the budget is enforced here and reported as [`GossipError::HintTooLarge`] before a node is
//! ever started.
//!
//! # Wire format
//!
//! ```text
//! byte 0        : HINT_WIRE_VERSION (currently 1)
//! bytes 1..     : postcard encoding of ObservedPeerHint
//! bytes ..end   : ignored by the decoder (forward compatibility slack)
//! ```
//!
//! The body is [`postcard`] over the `serde` representation of [`ObservedPeerHint`]: varint
//! integers and length-prefixed strings, no field names. A realistic hint encodes to roughly
//! 60–90 bytes including the version byte, leaving ample headroom under
//! [`MAX_HINT_BYTES`].
//!
//! Decoding is deliberately **lenient about trailing bytes and strict about the version**:
//! a future build may append fields after the postcard body, and this build must ignore them
//! rather than reject the peer; but a body written under a different version byte is not a
//! format this build can interpret, so it is refused with
//! [`HintDecodeError::UnsupportedVersion`]. Both outcomes are advisory — the peer is skipped
//! and logged, never fatal.

use config_core::hint::ObservedPeerHint;

use crate::error::{GossipError, HintDecodeError};

/// Version byte prefixed to every encoded hint.
///
/// Bumped only when the postcard body's field layout changes incompatibly. Appending trailing
/// bytes does **not** need a bump: [`decode_hint`] ignores anything after the body.
pub const HINT_WIRE_VERSION: u8 = 1;

/// Maximum size of encoded gossip metadata, in bytes, **including the version byte**.
///
/// Mirrors `memberlist`'s `META_MAX_SIZE`. Kept as a local constant so the limit is part of
/// this crate's contract rather than a leaked dependency detail.
pub const MAX_HINT_BYTES: usize = 512;

/// Encode a hint for advertisement, enforcing [`MAX_HINT_BYTES`].
///
/// The result is [`HINT_WIRE_VERSION`] followed by the postcard body.
///
/// # Errors
///
/// [`GossipError::HintTooLarge`] if the encoding exceeds the metadata budget, or
/// [`GossipError::HintEncode`] if serialization itself fails.
pub fn encode_hint(hint: &ObservedPeerHint) -> Result<Vec<u8>, GossipError> {
    let body = postcard::to_stdvec(hint).map_err(|e| GossipError::HintEncode(e.to_string()))?;
    let size = body.len() + 1;
    if size > MAX_HINT_BYTES {
        return Err(GossipError::HintTooLarge {
            size,
            limit: MAX_HINT_BYTES,
        });
    }
    let mut bytes = Vec::with_capacity(size);
    bytes.push(HINT_WIRE_VERSION);
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

/// Decode metadata advertised by a peer.
///
/// Trailing bytes after the hint body are ignored, so a peer running a newer build that
/// appends fields is still understood. A different version byte is not.
///
/// The decoded `liveness` field is whatever the peer claimed; callers overwrite it with the
/// *local* failure-detector observation, which is the only liveness rEtcd trusts.
///
/// # Errors
///
/// [`HintDecodeError::UnsupportedVersion`] if the leading byte is not [`HINT_WIRE_VERSION`],
/// or [`HintDecodeError::Malformed`] if the metadata is empty or the body does not parse.
/// Callers log and skip the peer; they must never panic or fail on this.
pub fn decode_hint(bytes: &[u8]) -> Result<ObservedPeerHint, HintDecodeError> {
    let (&version, body) = bytes.split_first().ok_or(HintDecodeError::Malformed {
        len: 0,
        message: "empty gossip metadata: no wire version byte".to_string(),
    })?;
    if version != HINT_WIRE_VERSION {
        return Err(HintDecodeError::UnsupportedVersion(version));
    }
    // `take_from_bytes` rather than `from_bytes`: trailing slack is forward compatibility,
    // not corruption.
    postcard::take_from_bytes::<ObservedPeerHint>(body)
        .map(|(hint, _rest)| hint)
        .map_err(|e| HintDecodeError::Malformed {
            len: bytes.len(),
            message: e.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use config_core::hint::Liveness;
    use config_core::identity::{ClusterId, NodeId, RecoveryEpoch};

    use super::*;

    fn sample() -> ObservedPeerHint {
        ObservedPeerHint {
            cluster_id: ClusterId::from_bytes([0xab; 16]),
            recovery_epoch: RecoveryEpoch(7),
            node_id: NodeId(2),
            peer_endpoint: "node-2.retcd.invalid".into(),
            client_endpoint: Some("node-2-client.retcd.invalid".into()),
            software_version: "0.1.0".into(),
            protocol_version: 1,
            zone: Some("rack-a".into()),
            liveness: Liveness::Alive,
        }
    }

    #[test]
    fn round_trip_is_compact_and_versioned() {
        let hint = sample();
        let bytes = encode_hint(&hint).expect("encode");
        assert_eq!(bytes[0], HINT_WIRE_VERSION);
        assert!(bytes.len() < 128, "unexpectedly large: {}", bytes.len());
        assert_eq!(decode_hint(&bytes).expect("decode"), hint);
    }

    #[test]
    fn oversized_hint_is_rejected() {
        let mut hint = sample();
        hint.software_version = "v".repeat(MAX_HINT_BYTES + 64);
        match encode_hint(&hint) {
            Err(GossipError::HintTooLarge { size, limit }) => {
                assert!(size > limit);
                assert_eq!(limit, MAX_HINT_BYTES);
            }
            other => panic!("expected HintTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn garbage_does_not_panic() {
        assert!(matches!(
            decode_hint(&[]),
            Err(HintDecodeError::Malformed { len: 0, .. })
        ));
        assert!(decode_hint(&[HINT_WIRE_VERSION; 8]).is_err());
    }

    #[test]
    fn version_byte_is_enforced() {
        let mut bytes = encode_hint(&sample()).expect("encode");
        bytes[0] = HINT_WIRE_VERSION + 1;
        assert_eq!(
            decode_hint(&bytes),
            Err(HintDecodeError::UnsupportedVersion(HINT_WIRE_VERSION + 1))
        );
    }
}
