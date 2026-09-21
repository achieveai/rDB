//! Content digests: the value the lineage, the envelope and the trace are all compared by.
//!
//! Three unrelated things in this system are "a hash of some bytes" — a transaction record, a
//! request payload, a trace checkpoint — and they must never collide. Every digest is therefore
//! domain separated and every part is length prefixed, so no concatenation of parts in one
//! domain can be confused with a different split in another.
//!
//! SHA-256, not BLAKE3: the workspace already pins `sha2`, it is pure Rust with no build script,
//! and the digest is a contract value that must not change silently for speed (rdb ADR-0002).

use core::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Prefix on every digest computed by this crate, so a digest of rDB bytes can never equal a
/// bare SHA-256 of the same bytes produced somewhere else.
pub const DIGEST_MAGIC: &[u8; 4] = b"RDBH";

/// What a digest is *about*. The tag is mixed in before any content, so the same bytes hashed
/// in two domains give two different digests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum Domain {
    /// One applied transaction record: the `record_digest` of the replication envelope
    /// (spec §6.1).
    Record = 1,
    /// The client-supplied request payload. Two requests with one identity and different
    /// digests are `REQUEST_ID_REUSE` (spec §5.3).
    Request = 2,
    /// The chain value linking sequence `n` to `n-1`: the `prev_digest` ancestry recovery
    /// validates (spec §8.1).
    Lineage = 3,
    /// A scenario configuration, recorded in the trace header so a replay can prove it ran the
    /// same configuration (spike §4, trace seam).
    Config = 4,
    /// An oracle checkpoint over client-visible state (spike §4, trace seam).
    Checkpoint = 5,
}

/// A 32-byte content digest.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Digest(
    /// The raw digest bytes.
    pub [u8; 32],
);

impl Digest {
    /// The digest of a lineage root's predecessor: the value `prev_digest` carries at
    /// [`crate::contracts::ids::Seq::ZERO`], where there is no predecessor to chain to.
    pub const ROOT: Self = Self([0u8; 32]);

    /// Domain-separated, length-prefixed digest of `parts`.
    ///
    /// Total and deterministic: no clock, no allocation of unordered collections, no iteration
    /// over a map. Same domain and same parts give the same 32 bytes on every platform.
    #[must_use]
    pub fn of(domain: Domain, parts: &[&[u8]]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(DIGEST_MAGIC);
        hasher.update([domain as u8]);
        for part in parts {
            hasher.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
            hasher.update(part);
        }
        Self(hasher.finalize().into())
    }

    /// Lowercase hex, for known-answer vectors and log fields.
    ///
    /// Written by hand rather than pulling `hex` into the dependency set of a crate every other
    /// crate depends on.
    #[must_use]
    pub fn to_hex(self) -> String {
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
            out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
        }
        out
    }
}

impl fmt::Debug for Digest {
    /// Prints the first eight hex characters. Full digests make a failing assertion unreadable;
    /// [`Digest::to_hex`] is there when the whole value is the point.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest({}..)", &self.to_hex()[..8])
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}
