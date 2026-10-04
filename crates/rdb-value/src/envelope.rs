//! The object envelope: a 40-byte header, then the canonical payload (ADR-rdb-0012 decision 7).
//!
//! | Offset | Size | Field | v1 |
//! |---|---|---|---|
//! | 0 | 1 | `envelope_format` | `0x01` |
//! | 1 | 1 | `kind` | [`Kind`] |
//! | 2 | 1 | `codec_version` | `0x01` = `rdb-cbor-document` v1 |
//! | 3 | 1 | `digest_alg` | `0x01` = SHA-256 of bytes 0..8, then the payload |
//! | 4 | 4 | `payload_len` | u32 big-endian; equals the remaining bytes |
//! | 8 | 32 | `digest` | |
//! | 40 | n | payload | |
//!
//! The digest covers the 8 header bytes before it, then the payload (ADR-rdb-0012 §9 as amended
//! by ruling L-R186s), so a flipped `kind` or `codec_version` is damage, not another type.
//!
//! Fail closed: every unknown byte, a length mismatch or a digest mismatch is a named
//! [`EnvelopeError`]. Nothing is guessed. The record's key and version are not in here: they are
//! the storage record's (decision 8).

use bytes::Bytes;
use sha2::{Digest as _, Sha256};

/// The header length.
pub const HEADER_LEN: usize = 40;
/// The largest envelope, header included: 1 MiB (decision 5).
pub const MAX_ENVELOPE: usize = 1 << 20;
/// The largest payload: what is left of [`MAX_ENVELOPE`] after the header.
pub const MAX_PAYLOAD: usize = MAX_ENVELOPE - HEADER_LEN;
/// The size limit in words, for error messages: the limit people think in, then the exact one.
pub const LIMIT_TEXT: &str = "1 MiB envelope (1,048,536-byte payload)";
// `LIMIT_TEXT` spells the numbers out; this keeps it honest if the constants ever move.
const _: () = assert!(MAX_ENVELOPE == 1_048_576 && MAX_PAYLOAD == 1_048_536);

/// `envelope_format` v1.
pub const ENVELOPE_FORMAT_V1: u8 = 0x01;
/// `codec_version` v1: `rdb-cbor-document` v1.
pub const CODEC_DOCUMENT_V1: u8 = 0x01;
/// `digest_alg` `0x01`: SHA-256 over header bytes 0..8, then the payload.
pub const DIGEST_SHA256: u8 = 0x01;

/// The envelope's `kind` table. Its own table, not the object sub-key discriminator
/// ([`crate::keys::Sub`]; ADR-rdb-0012 decision 13). `0x00` is invalid; other values are reserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A document, encoded with the `rdb-cbor-document` codec.
    Document,
    /// The root record of a map (ADR-rdb-0013 decision 7).
    Map,
    /// The root record of a set (ADR-rdb-0013 decision 7).
    Set,
}

impl Kind {
    /// The header byte.
    #[must_use]
    pub const fn byte(self) -> u8 {
        match self {
            Self::Document => 0x01,
            Self::Map => 0x02,
            Self::Set => 0x03,
        }
    }

    /// The kind a header byte names, or `None` for `0x00` and every reserved value.
    #[must_use]
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x01 => Some(Self::Document),
            0x02 => Some(Self::Map),
            0x03 => Some(Self::Set),
            _ => None,
        }
    }
}

/// Why stored bytes are not a well-formed v1 envelope.
///
/// Two different reasons, both refused (ADR-rdb-0012 §7, §12):
/// - **written by a newer build**: [`Self::UnknownFormat`], [`Self::UnknownKind`] for a reserved
///   value, [`Self::UnknownCodec`] and [`Self::UnknownDigest`]. The record may be valid; this build
///   cannot read it. Mixed-version windows are supported, so an older node can meet one. It must
///   never be offered for deletion as damage;
/// - **damage**: everything else, and [`Self::UnknownKind`]`(0)`, which no build writes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    /// Shorter than the header.
    #[error("envelope of {len} bytes is shorter than the {HEADER_LEN}-byte header")]
    Truncated {
        /// The length found.
        len: usize,
    },
    /// Longer than [`MAX_ENVELOPE`].
    #[error("envelope of {len} bytes is over the {LIMIT_TEXT} limit")]
    TooLarge {
        /// The length found.
        len: usize,
    },
    /// `envelope_format` is not one this build reads: written by a newer build.
    #[error("unknown envelope format {0:#04x}")]
    UnknownFormat(u8),
    /// `kind` is `0x00` (damage: never written) or reserved (written by a newer build).
    #[error("unknown envelope kind {0:#04x}")]
    UnknownKind(u8),
    /// `codec_version` is not one this build reads: written by a newer build.
    #[error("unknown codec version {0:#04x}")]
    UnknownCodec(u8),
    /// `digest_alg` is not one this build reads: written by a newer build.
    #[error("unknown digest algorithm {0:#04x}")]
    UnknownDigest(u8),
    /// `payload_len` disagrees with the bytes that follow the header.
    #[error("payload_len says {declared} bytes, {actual} follow the header")]
    LengthMismatch {
        /// `payload_len`.
        declared: u32,
        /// The bytes after the header.
        actual: usize,
    },
    /// The payload does not hash to the stored digest.
    #[error("payload does not hash to the stored digest")]
    DigestMismatch,
}

/// [`seal`] was handed a payload over [`MAX_PAYLOAD`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("payload of {len} bytes is over the {LIMIT_TEXT} limit")]
pub struct OversizedPayload {
    /// The payload length.
    pub len: usize,
}

/// A checked envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Opened<'a> {
    /// What the payload is.
    pub kind: Kind,
    /// The stored digest (header bytes 0..8, then the payload), already checked.
    pub digest: [u8; 32],
    /// The canonical payload. Not decoded yet.
    pub payload: &'a [u8],
}

/// The bytes the digest covers before the payload: header bytes 0..8.
const DIGESTED_HEAD: usize = 8;

/// SHA-256 over `head` (header bytes 0..8), then `payload`.
#[must_use]
pub fn digest(head: &[u8; DIGESTED_HEAD], payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(head);
    hasher.update(payload);
    hasher.finalize().into()
}

/// The envelope for `payload`: header, then the payload as given.
///
/// # Errors
/// [`OversizedPayload`] when `payload` is over [`MAX_PAYLOAD`].
pub fn seal(kind: Kind, payload: &[u8]) -> Result<Bytes, OversizedPayload> {
    let len = u32::try_from(payload.len())
        .ok()
        .filter(|_| payload.len() <= MAX_PAYLOAD)
        .ok_or(OversizedPayload { len: payload.len() })?;
    let len = len.to_be_bytes();
    let head = [
        ENVELOPE_FORMAT_V1,
        kind.byte(),
        CODEC_DOCUMENT_V1,
        DIGEST_SHA256,
        len[0],
        len[1],
        len[2],
        len[3],
    ];
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&head);
    out.extend_from_slice(&digest(&head, payload));
    out.extend_from_slice(payload);
    Ok(Bytes::from(out))
}

/// Check every header field and the digest. The order is ruling L-R186s with critic K1:
///
/// 1. `envelope_format`, then `digest_alg`: they say how the digest is checked, so an unknown
///    one is a newer build's record and is refused before any hashing;
/// 2. `payload_len`;
/// 3. the digest: a mismatch is damage;
/// 4. `kind`, then `codec_version`. A newer build's record hashes correctly, so an unknown
///    value here is that build's, never damage (ADR-rdb-0012 §12). A flipped byte has already
///    failed step 3.
///
/// # Errors
/// The first [`EnvelopeError`] found.
pub fn open(bytes: &[u8]) -> Result<Opened<'_>, EnvelopeError> {
    if bytes.len() < HEADER_LEN {
        return Err(EnvelopeError::Truncated { len: bytes.len() });
    }
    if bytes.len() > MAX_ENVELOPE {
        return Err(EnvelopeError::TooLarge { len: bytes.len() });
    }
    let (header, payload) = bytes.split_at(HEADER_LEN);
    if header[0] != ENVELOPE_FORMAT_V1 {
        return Err(EnvelopeError::UnknownFormat(header[0]));
    }
    if header[3] != DIGEST_SHA256 {
        return Err(EnvelopeError::UnknownDigest(header[3]));
    }
    let declared = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
    if usize::try_from(declared).ok() != Some(payload.len()) {
        return Err(EnvelopeError::LengthMismatch {
            declared,
            actual: payload.len(),
        });
    }
    let (head, rest) = header.split_at(DIGESTED_HEAD);
    let head: &[u8; DIGESTED_HEAD] = head.try_into().expect("split at DIGESTED_HEAD");
    let stored: [u8; 32] = rest.try_into().expect("HEADER_LEN is DIGESTED_HEAD + 32");
    if digest(head, payload) != stored {
        return Err(EnvelopeError::DigestMismatch);
    }
    let kind = Kind::from_byte(header[1]).ok_or(EnvelopeError::UnknownKind(header[1]))?;
    if header[2] != CODEC_DOCUMENT_V1 {
        return Err(EnvelopeError::UnknownCodec(header[2]));
    }
    Ok(Opened {
        kind,
        digest: stored,
        payload,
    })
}
