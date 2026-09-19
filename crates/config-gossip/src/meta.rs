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
//! bytes ..      : postcard encoding of HintExtras (optional; ADR-0030)
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
use config_core::SchemaTriple;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{GossipError, HintDecodeError};

/// Fields appended after the [`ObservedPeerHint`] body, in the slack the decoder already
/// ignores (ADR-0030 as-built).
///
/// Appended rather than added to [`ObservedPeerHint`] for two reasons. The hint is a
/// `config-core` type shared with the engine, and everything in it is a fact the engine *acts*
/// on, while everything here is advisory and must never reach a decision — the schema gate in
/// particular reads the peer plane and only the peer plane (ADR-0003 §19.9, M6-102). And the
/// hint's layout is pinned by a golden vector, so growing it is a wire-format change while
/// appending is exactly the forward compatibility this module already documents.
///
/// **Field order is the wire format.** postcard writes fields positionally with no names, so
/// a new field goes on the *end* and an existing one is never reordered or removed. The order,
/// and who owns each slot, is:
///
/// | field | name | ADR |
/// |---|---|---|
/// | 0 | `schema` | ADR-0030, mixed-version gating |
/// | 1 | `accepted_gossip_keys` | ADR-0028, gossip key rotation |
/// | 2 | `policy_version` | ADR-0027, signed policy convergence |
///
/// A peer that wrote fewer fields than this build knows about is understood, not rejected:
/// [`decode_hint_extras`] reads the trailer one field at a time and treats a short tail as
/// "not advertised". Appending a slot is therefore safe in both directions, which is the only
/// reason one may be added without bumping [`HINT_WIRE_VERSION`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HintExtras {
    /// What the advertising node can read and write. Advisory: an operator's view of a
    /// rolling upgrade, never an input to the gate.
    pub schema: Option<SchemaTriple>,
    /// Which gossip keys the advertising node will accept, by fingerprint. Advisory: read by
    /// the `gossip_remove_key` refusal (ADR-0028, M6-59) so an operator is told *which* peer
    /// still needs the key they are about to drop. Never an input to liveness or membership.
    pub accepted_gossip_keys: Option<AcceptedGossipKeys>,
    /// The signed policy document version the advertising node currently holds, if it holds
    /// one. Advisory, and deliberately so: it only ever *ends* a narrowing that is already
    /// fail-closed, so a forged advertisement can shorten the convergence window but can never
    /// grant access the document does not (ADR-0027 §15.3, OQ-56, M6-23). A node that has no
    /// valid document advertises `None`, which every reader must treat as lagging.
    pub policy_version: Option<u64>,
}

/// A gossip key's fingerprint as it appears on the wire and in logs: the leading 8 bytes of
/// the SHA-256 of the raw key material.
///
/// Truncated deliberately. A fingerprint exists so an operator, a log line and ADR-0028's
/// removal refusal can *name* a key; it must never be enough to recover one. Eight bytes
/// distinguish the handful of keys a cluster holds at once with room to spare (ADR-0028,
/// "secret hygiene": fingerprints only, never key bytes).
pub type GossipKeyFingerprint = [u8; 8];

/// How many accepted-key fingerprints a node advertises.
///
/// A rotation holds the outgoing key, the incoming key and the primary at the same time, so
/// four leaves slack while keeping the trailer a tiny fixed fraction of [`MAX_HINT_BYTES`].
pub const MAX_ADVERTISED_GOSSIP_KEYS: usize = 4;

/// The fingerprints of the gossip keys a node will accept, as that node advertises them.
///
/// Fixed capacity, so the type stays `Copy` and the encoding stays bounded. A node holding
/// more than [`MAX_ADVERTISED_GOSSIP_KEYS`] keys advertises the first ones and drops the rest.
/// That truncation is safe in the only direction that matters: it can make a peer look like it
/// accepts *fewer* keys, never like it accepts one it does not, and it can never turn a
/// multi-key peer into an apparently single-key one — so it can only make
/// [`AcceptedGossipKeys::is_sole`] answer `false`, biasing the removal refusal towards allowing
/// a removal the peer survives rather than towards a surprise exile.
#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedGossipKeys {
    /// Occupied slots come first; `None` is padding. A fixed-length array encodes positionally
    /// in postcard, so this is a stable four-slot trailer, not a length-prefixed list.
    slots: [Option<GossipKeyFingerprint>; MAX_ADVERTISED_GOSSIP_KEYS],
}

impl AcceptedGossipKeys {
    /// Advertise `fingerprints`, keeping at most [`MAX_ADVERTISED_GOSSIP_KEYS`] of them.
    #[must_use]
    pub fn new(fingerprints: impl IntoIterator<Item = GossipKeyFingerprint>) -> Self {
        let mut slots = [None; MAX_ADVERTISED_GOSSIP_KEYS];
        for (slot, fingerprint) in slots.iter_mut().zip(fingerprints) {
            *slot = Some(fingerprint);
        }
        Self { slots }
    }

    /// The advertised fingerprints, in advertisement order.
    pub fn iter(&self) -> impl Iterator<Item = GossipKeyFingerprint> + '_ {
        self.slots.iter().flatten().copied()
    }

    /// How many fingerprints were advertised.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.iter().flatten().count()
    }

    /// Whether the peer advertised no keys at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `fingerprint` is the **only** key this peer advertised.
    ///
    /// The single fact ADR-0028's `gossip_remove_key` refusal turns on: removing a key that is
    /// some known peer's only key exiles that peer from the mesh, so the removal is refused by
    /// default and the peer is named.
    #[must_use]
    pub fn is_sole(&self, fingerprint: GossipKeyFingerprint) -> bool {
        let mut advertised = self.iter();
        advertised.next() == Some(fingerprint) && advertised.next().is_none()
    }

    /// Whether `fingerprint` is the key this peer is **signing** with — its advertised slot 0.
    ///
    /// The other half of the removal refusal, and the half that covers the window an operator
    /// actually lands in (ruling M6-R21): between the `add` sweep and the `use` sweep every
    /// peer advertises both keys, so [`Self::is_sole`] is false everywhere while every peer is
    /// still signing with the old one. Removing it there makes this node deaf to all of them.
    ///
    /// Reads slot 0 because `publish_keyring` advertises the keyring in keyring order and the
    /// primary is first there; [`Self::is_sole`] is kept alongside it in the refusal because
    /// it is order-insensitive and so still holds if that ordering ever does not.
    #[must_use]
    pub fn is_primary(&self, fingerprint: GossipKeyFingerprint) -> bool {
        self.iter().next() == Some(fingerprint)
    }
}

/// Lower-case hex, the form a fingerprint takes in every log line and typed refusal.
#[must_use]
pub fn fingerprint_hex(fingerprint: GossipKeyFingerprint) -> String {
    use std::fmt::Write as _;
    fingerprint
        .iter()
        .fold(String::with_capacity(16), |mut out, byte| {
            // Writing into a `String` cannot fail; discarding the `Result` keeps this
            // infallible for callers that only want something to print.
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// The advertised fingerprint of an AES-256 gossip key: the first eight bytes of its SHA-256.
///
/// A fingerprint, never the key. These values travel in the gossip trailer, in log lines and in
/// the admin plane's reply, so they have to be safe to publish: eight bytes of a
/// preimage-resistant digest let an operator watching a rotation tell one key from another
/// without telling anyone who reads them anything about the key itself (ADR-0028).
///
/// Eight bytes and not four: a rotation compares fingerprints across every node of a cluster,
/// and a collision there would mean reporting a key as accepted where it is not.
pub fn gossip_key_fingerprint(key: &[u8]) -> GossipKeyFingerprint {
    let digest = Sha256::digest(key);
    let mut out = [0u8; 8];
    out.copy_from_slice(&digest[..8]);
    out
}

/// Hex, not raw bytes: these lines are read by an operator mid-rotation.
impl std::fmt::Debug for AcceptedGossipKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        for fingerprint in self.iter() {
            list.entry(&fingerprint_hex(fingerprint));
        }
        list.finish()
    }
}

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
    encode_hint_with_extras(hint, None)
}

/// Encode a hint with an optional [`HintExtras`] trailer.
///
/// `None` produces bytes identical to [`encode_hint`], which is what keeps a node that has
/// nothing extra to say byte-compatible with every build that came before it.
///
/// # Errors
///
/// As [`encode_hint`].
pub fn encode_hint_with_extras(
    hint: &ObservedPeerHint,
    extras: Option<&HintExtras>,
) -> Result<Vec<u8>, GossipError> {
    let mut body = postcard::to_stdvec(hint).map_err(|e| GossipError::HintEncode(e.to_string()))?;
    if let Some(extras) = extras {
        let trailer =
            postcard::to_stdvec(extras).map_err(|e| GossipError::HintEncode(e.to_string()))?;
        body.extend_from_slice(&trailer);
    }
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

/// Read the [`HintExtras`] trailer a peer appended, if it appended one.
///
/// `None` covers every way a peer can decline to say anything: an older build that wrote no
/// trailer, a version byte this build does not speak, and a trailer that does not parse. All
/// three are the same fact — *this peer told us nothing* — and a caller that treated any of
/// them as a default value would be inventing an advertisement (M6-111).
#[must_use]
pub fn decode_hint_extras(bytes: &[u8]) -> Option<HintExtras> {
    let (&version, body) = bytes.split_first()?;
    if version != HINT_WIRE_VERSION {
        return None;
    }
    let (_hint, rest) = postcard::take_from_bytes::<ObservedPeerHint>(body).ok()?;
    if rest.is_empty() {
        return None;
    }
    // Read the trailer **one field at a time** rather than as a whole `HintExtras`. postcard is
    // positional, so a peer built before a slot was appended writes a shorter trailer, and
    // deserializing the whole struct would call that truncation malformed and discard the
    // fields the peer *did* advertise. Each slot is therefore optional by position: present
    // means the peer advertised it, absent means the peer predates the slot. Every slot
    // appended after this one follows the same shape — see [`HintExtras`] for the field table.
    let (schema, rest) = postcard::take_from_bytes::<Option<SchemaTriple>>(rest).ok()?;
    let mut extras = HintExtras {
        schema,
        ..HintExtras::default()
    };
    if let Ok((accepted, rest)) = postcard::take_from_bytes::<Option<AcceptedGossipKeys>>(rest) {
        extras.accepted_gossip_keys = accepted;
        // Nested rather than sequential, and that nesting is the rule: field 2 begins where
        // field 1 ended, so a trailer that stopped before field 1 has no position at which
        // field 2 could start. Reading it from the same `rest` would decode field 1's bytes
        // as field 2 and invent a version the peer never advertised.
        if let Ok((policy_version, _rest)) = postcard::take_from_bytes::<Option<u64>>(rest) {
            extras.policy_version = policy_version;
        }
    }
    Some(extras)
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

    /// A fingerprint that is recognisable in a failure message.
    fn fp(n: u8) -> GossipKeyFingerprint {
        [n; 8]
    }

    /// The trailer exactly as a build that knew only field 0 wrote it: the hint body followed
    /// by that one field and nothing else. postcard writes a struct as its fields back to back
    /// with no framing, so serializing the lone field *is* the old encoder's output.
    fn trailer_from_a_schema_only_build(
        hint: &ObservedPeerHint,
        schema: Option<config_core::SchemaTriple>,
    ) -> Vec<u8> {
        let mut bytes = vec![HINT_WIRE_VERSION];
        bytes.extend_from_slice(&postcard::to_stdvec(hint).expect("hint"));
        bytes.extend_from_slice(&postcard::to_stdvec(&schema).expect("schema field"));
        bytes
    }

    /// The trailer is invisible to a build that does not know about it, and readable by one
    /// that does. Both halves matter: the first is why no version bump is needed, the second
    /// is why the field is on the wire at all.
    #[test]
    fn extras_ride_in_the_trailing_slack() {
        let hint = sample();
        let extras = HintExtras {
            schema: Some(config_core::CURRENT_SCHEMA),
            accepted_gossip_keys: None,
            policy_version: None,
        };
        let bytes = encode_hint_with_extras(&hint, Some(&extras)).expect("encode");

        assert_eq!(decode_hint(&bytes).expect("decode"), hint);
        assert_eq!(decode_hint_extras(&bytes), Some(extras));

        // A hint from a build that predates the trailer says nothing, rather than claiming a
        // default triple it never advertised.
        assert_eq!(
            decode_hint_extras(&encode_hint(&hint).expect("encode")),
            None
        );
        assert_eq!(decode_hint_extras(&[]), None);
        assert_eq!(decode_hint_extras(&[HINT_WIRE_VERSION + 1, 0, 0]), None);
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

    /// Field 1 round-trips beside field 0, and the pair still fits the metadata budget.
    #[test]
    fn accepted_gossip_keys_ride_beside_the_schema() {
        let hint = sample();
        let extras = HintExtras {
            schema: Some(config_core::CURRENT_SCHEMA),
            accepted_gossip_keys: Some(AcceptedGossipKeys::new([fp(1), fp(2)])),
            policy_version: None,
        };
        let bytes = encode_hint_with_extras(&hint, Some(&extras)).expect("encode");

        assert!(
            bytes.len() <= MAX_HINT_BYTES,
            "the two-field trailer must stay inside the gossip budget: {} bytes",
            bytes.len()
        );
        assert_eq!(decode_hint(&bytes).expect("decode"), hint);
        assert_eq!(decode_hint_extras(&bytes), Some(extras));
    }

    /// The backward half of the append rule, and the reason [`decode_hint_extras`] reads the
    /// trailer field by field: a peer built before field 1 existed writes a shorter trailer,
    /// and this build must still read the schema that peer *did* advertise. Decoding the
    /// trailer as a whole struct would call that truncation malformed and silently drop the
    /// mixed-version gate's only input (ADR-0030, M6-85).
    #[test]
    fn a_peer_that_predates_field_one_is_still_understood() {
        let hint = sample();
        let bytes = trailer_from_a_schema_only_build(&hint, Some(config_core::CURRENT_SCHEMA));

        let extras = decode_hint_extras(&bytes).expect("a short trailer is a trailer");
        assert_eq!(
            extras.schema,
            Some(config_core::CURRENT_SCHEMA),
            "the schema an older peer advertised was dropped by the append"
        );
        assert_eq!(
            extras.accepted_gossip_keys, None,
            "a slot the peer never wrote must read as 'not advertised', never as a default"
        );
        assert_eq!(
            extras.policy_version, None,
            "and neither may a slot two appends newer than that peer"
        );
    }

    /// The same backward half one slot further out, and the reason field 2 is read *inside*
    /// field 1's arm: a peer built before `policy_version` existed writes fields 0 and 1 only,
    /// and this build must read both of those and claim nothing about the third. A
    /// `policy_version` invented here would be worse than none — it is what ends the
    /// converging narrowing, so a fabricated one would let a newly granted prefix take effect
    /// while a voter still denies it (ADR-0027 §15.3, M6-22).
    #[test]
    fn a_peer_that_predates_field_two_is_still_understood() {
        let hint = sample();
        let keys = AcceptedGossipKeys::new([fp(3)]);
        let mut bytes = trailer_from_a_schema_only_build(&hint, Some(config_core::CURRENT_SCHEMA));
        bytes.extend_from_slice(&postcard::to_stdvec(&Some(keys)).expect("field 1"));

        let extras = decode_hint_extras(&bytes).expect("a short trailer is a trailer");
        assert_eq!(extras.schema, Some(config_core::CURRENT_SCHEMA));
        assert_eq!(
            extras.accepted_gossip_keys,
            Some(keys),
            "the slot that peer did write must survive the append after it"
        );
        assert_eq!(
            extras.policy_version, None,
            "a version the peer never advertised must read as 'not advertised'"
        );
    }

    /// The forward half: a peer running a *newer* build appends a slot this build does not
    /// know, and the fields this build does know are unaffected.
    #[test]
    fn a_peer_from_a_later_build_is_read_up_to_the_slots_we_know() {
        let hint = sample();
        let extras = HintExtras {
            schema: Some(config_core::CURRENT_SCHEMA),
            accepted_gossip_keys: Some(AcceptedGossipKeys::new([fp(9)])),
            policy_version: Some(42),
        };
        let mut bytes = encode_hint_with_extras(&hint, Some(&extras)).expect("encode");
        // Stand in for a slot appended after field 2, whoever appends it next.
        bytes.extend_from_slice(&postcard::to_stdvec(&Some(7u32)).expect("future slot"));

        assert_eq!(decode_hint(&bytes).expect("decode"), hint);
        assert_eq!(decode_hint_extras(&bytes), Some(extras));
    }

    /// `is_sole` is the whole of ADR-0028's removal refusal (M6-59), so it is asserted on its
    /// own rather than through a rotation: it must answer `true` only when the named key is the
    /// peer's single advertised key.
    #[test]
    fn is_sole_answers_the_removal_refusal_question() {
        let only_one = AcceptedGossipKeys::new([fp(1)]);
        assert!(only_one.is_sole(fp(1)), "the peer's single key");
        assert!(!only_one.is_sole(fp(2)), "a key this peer never advertised");

        let overlapping = AcceptedGossipKeys::new([fp(1), fp(2)]);
        assert!(
            !overlapping.is_sole(fp(1)),
            "a peer mid-rotation still has the other key, so removal is survivable"
        );

        assert!(
            !AcceptedGossipKeys::default().is_sole(fp(1)),
            "a peer that advertised nothing must never be read as needing a key"
        );
    }

    /// Over-capacity advertisement truncates rather than failing, and truncation can only make
    /// a peer look like it accepts fewer keys — never like a single-key peer it is not.
    #[test]
    fn advertising_more_keys_than_the_budget_truncates_safely() {
        let keys: Vec<_> = (0..(MAX_ADVERTISED_GOSSIP_KEYS as u8 + 3))
            .map(fp)
            .collect();
        let advertised = AcceptedGossipKeys::new(keys.clone());

        assert_eq!(advertised.len(), MAX_ADVERTISED_GOSSIP_KEYS);
        assert!(!advertised.is_empty());
        assert_eq!(
            advertised.iter().collect::<Vec<_>>(),
            keys[..MAX_ADVERTISED_GOSSIP_KEYS],
            "the kept keys are the leading ones, in advertisement order"
        );
        for key in &keys {
            assert!(
                !advertised.is_sole(*key),
                "truncation must never manufacture a single-key peer"
            );
        }
    }

    /// Fingerprints are printed as hex, and nothing but fingerprints is ever printed
    /// (ADR-0028 secret hygiene).
    #[test]
    fn fingerprints_print_as_hex() {
        assert_eq!(
            fingerprint_hex([0x0a, 0xff, 0, 0, 0, 0, 0, 0]),
            "0aff000000000000"
        );
        let rendered = format!("{:?}", AcceptedGossipKeys::new([fp(0xab)]));
        assert_eq!(rendered, "[\"abababababababab\"]");
    }
}
