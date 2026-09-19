//! gRPC codec caps derived from [`Limits`] (ADR-0010, fix-round note 2026-09-18).
//!
//! tonic refuses to decode a message larger than 4 MiB unless it is told otherwise, and that
//! default is *below* what this system legally produces on both planes. Rather than pick two
//! magic numbers, both caps are computed here from the same [`Limits`] the API edge and the
//! state machine already enforce, so raising a cap in `config-core` cannot leave the transport
//! behind.
//!
//! Both functions are deliberately generous in one direction only: a cap that is too large
//! costs a buffer bound nobody reaches, while a cap that is too small costs a wedged
//! replication stream or an `Unavailable` where a truncated answer was owed.

use config_core::Limits;
use config_engine::MAX_PAYLOAD_ENTRIES;

/// Headroom added to every derived cap for gRPC/Protobuf framing.
///
/// A message is never *exactly* its accounted payload: the `List` byte budget counts
/// `key + value + 16` per record and ignores field tags, length prefixes and the response's own
/// scalar fields, and a peer envelope adds its identity header plus postcard's own varint
/// framing on top of the payload. One formula, used by both planes, rather than a per-plane
/// fudge factor.
pub const MESSAGE_FRAMING_SLACK_BYTES: usize = 1024 * 1024;

/// The largest client-plane message either side must be able to move, in bytes.
///
/// The binding case is a `List` reply: the server fills it up to
/// [`Limits::max_list_bytes`] before it sets `truncated`, so a client whose codec cap is lower
/// gets a transport error instead of the truncated page the protocol promises it (spec §10.2).
/// Mutation requests are far smaller and are covered by the same number.
pub fn client_plane_message_limit(limits: &Limits) -> usize {
    saturating_usize(limits.max_list_bytes).saturating_add(MESSAGE_FRAMING_SLACK_BYTES)
}

/// The largest peer-plane message either side must be able to move, in bytes.
///
/// One `AppendEntries` carries up to [`MAX_PAYLOAD_ENTRIES`] log entries, each holding a command
/// of at most [`Limits::max_request_bytes`]. The batch travels as postcard
/// (`PAYLOAD_ENCODING_POSTCARD`), which writes a byte string as a length varint followed by the
/// bytes themselves — so there is no expansion factor to multiply by, only a few bytes of
/// framing per field that [`MESSAGE_FRAMING_SLACK_BYTES`] swallows many times over. A cap below
/// this value turns a legal write into `OUT_OF_RANGE`, which OpenRaft classifies as retryable
/// and replays forever.
pub fn peer_plane_message_limit(limits: &Limits) -> usize {
    limits
        .max_request_bytes
        .saturating_mul(saturating_usize(MAX_PAYLOAD_ENTRIES))
        .saturating_add(MESSAGE_FRAMING_SLACK_BYTES)
}

/// `u64` → `usize` without wrapping on a 32-bit target.
fn saturating_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two caps must cover what the limits they are derived from allow, with framing on
    /// top. Asserted as inequalities rather than as literal byte counts: the point is the
    /// relationship, and pinning the arithmetic would only restate the implementation.
    #[test]
    fn caps_cover_the_payloads_their_limits_permit() {
        let limits = Limits::DEFAULT;

        assert!(
            client_plane_message_limit(&limits) > limits.max_list_bytes as usize,
            "a full List page must fit with framing to spare"
        );
        assert!(
            peer_plane_message_limit(&limits)
                > limits.max_request_bytes * MAX_PAYLOAD_ENTRIES as usize,
            "a full AppendEntries batch must fit with framing to spare"
        );
        assert!(
            peer_plane_message_limit(&limits) > 4 * 1024 * 1024,
            "the whole point is to be above tonic's 4 MiB default"
        );
    }

    /// Raising a limit must never lower the cap derived from it: an operator who widens
    /// `config-core` and redeploys must not discover that the transport got tighter.
    #[test]
    fn caps_are_monotone_in_their_limits() {
        let base = Limits::DEFAULT;
        let wider = Limits {
            max_request_bytes: base.max_request_bytes * 2,
            max_list_bytes: base.max_list_bytes * 2,
            ..base
        };

        assert!(client_plane_message_limit(&wider) > client_plane_message_limit(&base));
        assert!(peer_plane_message_limit(&wider) > peer_plane_message_limit(&base));

        let narrower = Limits {
            max_request_bytes: base.max_request_bytes / 2,
            max_list_bytes: base.max_list_bytes / 2,
            ..base
        };
        assert!(client_plane_message_limit(&narrower) < client_plane_message_limit(&base));
        assert!(peer_plane_message_limit(&narrower) < peer_plane_message_limit(&base));
    }

    /// An absurd limit must saturate rather than wrap: a wrapped cap would be *smaller* than
    /// the default and would silently reintroduce the defect this module exists to fix.
    #[test]
    fn an_absurd_limit_saturates_instead_of_wrapping() {
        let absurd = Limits {
            max_request_bytes: usize::MAX,
            max_list_bytes: u64::MAX,
            ..Limits::DEFAULT
        };

        assert_eq!(client_plane_message_limit(&absurd), usize::MAX);
        assert_eq!(peer_plane_message_limit(&absurd), usize::MAX);
    }
}
