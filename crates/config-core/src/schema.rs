//! The schema triple every node advertises, and the propose-time gate it feeds (ADR-0030).
//!
//! A mixed-version cluster is the normal state of a rolling upgrade, and the dangerous part of
//! it is not the upgrade — it is the moment a new leader proposes a command an old voter
//! cannot decode. postcard is not self-describing (`command` module docs; ADR-0008 note of
//! 2026-09-18), so a committed entry an applying voter cannot read is unrecoverable: it cannot
//! be skipped without diverging and it cannot be decoded at all. The gate therefore has to
//! stop the command at **propose** time, on the leader, before it enters the log (ADR-0030
//! A7). By apply time it is already too late for every voter in the cluster.
//!
//! # The triple
//!
//! [`SchemaTriple`] names the three things that can move independently:
//!
//! | component | what a change to it breaks |
//! |---|---|
//! | `format_version` | the on-disk layout (`config-storage`'s marker) |
//! | `command_schema` | the set of [`Command`] variants and fields a build can decode |
//! | `proto_rev` | the gRPC surface |
//!
//! It is advertised on three planes, and only one of them is authoritative:
//!
//! * the **peer plane** `AppendEntries` header — authoritative, because it is the same channel
//!   the command itself would travel on, and a voter that answers it is a voter the leader has
//!   actually heard from;
//! * **gossip meta** — advisory only (ADR-0003, §19.9). A gossip-derived level would let two
//!   leaders compute different answers and unlock a feature on one of them (M6-102);
//! * **health / `--capabilities`** — for operators, never read back by the cluster.
//!
//! # Why ordering is by `command_schema` first
//!
//! [`SchemaTriple`] orders by [`SchemaTriple::gate_key`] — `command_schema`, then
//! `format_version`, then `proto_rev` — rather than by declaration order, because
//! `command_schema` is the only component the gate consults and therefore the only one whose
//! ordering can refuse or admit a proposal. The three move together on a real build, so the
//! tie-breakers exist to make the order total, not because a build is expected to straddle.
//!
//! The minimum over a set of voters is always **one of the observed triples**, never a
//! field-wise blend: a blend could describe a build that does not exist and claim support no
//! voter actually has.

use serde::{Deserialize, Serialize};

use crate::command::{Command, DecodeError};

/// The `command_schema` of the M0–M3 envelope: `Put` and `Delete`, no dedup group.
pub const COMMAND_SCHEMA_V1: u16 = 1;

/// The `command_schema` of the M4/M5 envelope: adds `Compact`, `RetireNode` and the dedup
/// group (`command` module docs, ADR-0019, ADR-0025).
pub const COMMAND_SCHEMA_V2: u16 = 2;

/// The feature name a `Compact` proposal is gated behind.
///
/// These strings reach an operator's alert rules through `feature_gated{feature = …}`, so they
/// are constants for the same reason the [`crate::error`] reason strings are: a typo in either
/// the emitter or the rule silently stops matching.
pub const FEATURE_COMPACT: &str = "compact";

/// The feature name a `RetireNode` proposal is gated behind.
pub const FEATURE_RETIRE_NODE: &str = "retire_node";

/// The feature name a dedup-bearing `Put` or `Delete` is gated behind.
pub const FEATURE_DEDUP: &str = "dedup";

/// What one build can read and write, on all three axes that can move independently.
///
/// `Copy` because it is three integers that are passed around constantly — through the peer
/// plane, the health payload, gossip meta and the gate — and a borrow at every one of those
/// call sites would buy nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaTriple {
    /// The on-disk format this build reads and writes (`config_storage::FORMAT_VERSION`).
    pub format_version: u32,
    /// The [`Command`] envelope generation this build can decode.
    pub command_schema: u16,
    /// The gRPC surface revision this build serves.
    pub proto_rev: u32,
}

/// What this build is.
///
/// `format_version` is 3 because M5 added the dedup column family; `command_schema` is 2
/// because M4/M5 added `Compact`, `RetireNode` and the dedup group to the envelope.
///
/// `proto_rev` stays 1 through M6, deliberately (finding F-017). M6 did grow the gRPC
/// surface — the admin service, the RBAC fields, revision-pinned pagination — but every one
/// of those additions is a new protobuf field or a new service, which a peer built before it
/// still parses and simply never calls. `proto_rev` names a revision of the surface that an
/// older peer could *fail* on, and M6 produced none. Bumping it here and not in
/// [`COMPAT_SCHEMA_1`] would be worse than leaving it: `--compat-schema 1` pins the envelope
/// and the store ceiling, it does not remove a service from the gRPC surface, so the pinned
/// node would then advertise a protocol revision it is in fact serving past. The axis
/// consequently gates nothing today, which is an accurate report rather than a gap.
/// [`SchemaTriple::gate_key`] uses it only as a last tie-breaker, to make the order total
/// (module docs). It is still carried on the peer wire and printed by
/// [`Display`](std::fmt::Display).
pub const CURRENT_SCHEMA: SchemaTriple = SchemaTriple {
    format_version: 3,
    command_schema: COMMAND_SCHEMA_V2,
    proto_rev: 1,
};

/// What a node started with `--compat-schema 1` advertises and enforces.
///
/// Not a second encoder: there is no v1 command encoder in this build (ADR-0030 as-built).
/// A compat-1 node *advertises* 1, never proposes a command that needs 2, refuses to decode
/// one that does, and refuses to open a store newer than `format_version` — which is every
/// externally visible behaviour of a genuine v1 binary, and the only way §6 of the M6 test
/// plan is runnable in CI at all.
pub const COMPAT_SCHEMA_1: SchemaTriple = SchemaTriple {
    format_version: 1,
    command_schema: COMMAND_SCHEMA_V1,
    proto_rev: 1,
};

impl SchemaTriple {
    /// The total order used to pick the oldest voter in a cluster.
    ///
    /// See the module docs for why `command_schema` leads.
    #[must_use]
    pub fn gate_key(&self) -> (u16, u32, u32) {
        (self.command_schema, self.format_version, self.proto_rev)
    }

    /// Whether this build can carry `cmd`.
    #[must_use]
    pub fn admits(&self, cmd: &Command) -> bool {
        refuse_command(self.command_schema, cmd).is_none()
    }

    /// Decode a command envelope, refusing one this schema could not have produced.
    ///
    /// The refusal is the point (M6-99). A build pinned to schema 1 that decoded a v2-only
    /// variant on a best-effort basis would produce a *plausible but wrong* value, which is
    /// strictly worse than an error: the wrong value applies cleanly and diverges silently,
    /// while the error stops the node.
    ///
    /// The check is on the decoded shape rather than on the envelope's version byte because
    /// version 2 carries both generations of `Put` — the dedup group is what makes a
    /// particular `Put` a schema-2 command, not the header.
    pub fn decode_command(&self, buf: &[u8]) -> Result<Command, SchemaError> {
        let cmd = Command::decode(buf)?;
        match refuse_command(self.command_schema, &cmd) {
            Some(err) => Err(err),
            None => Ok(cmd),
        }
    }
}

/// The refusal a build pinned to `command_schema` owes `cmd`, or `None` when it may carry it.
///
/// The one expression of "may this generation carry this command"; [`SchemaTriple::admits`]
/// and [`SchemaTriple::decode_command`] are both thin wrappers over it, and so is the apply
/// path's fence (`config-storage`'s `rocks.rs`, ADR-0030 finding F-015). Three separate
/// spellings of the comparison is how the fence and the gate come to disagree about what a
/// pinned node accepts, which is the failure ADR-0030 is entirely about.
///
/// It takes the `u16` rather than a whole [`SchemaTriple`] because its newest caller holds
/// only that one axis: `RocksOptions::command_schema` is the pin the storage layer is given,
/// and assembling a triple around it to ask this question would be exactly the field-wise
/// blend the module docs forbid — a value describing a build that does not exist.
///
/// It takes a decoded [`Command`] rather than bytes because the apply path never sees the
/// envelope: a Raft entry stores `postcard(Entry<TypeConfig>)` and its payload reaches apply
/// through `Command`'s serde derive (`command` module docs), so by then the only question
/// left to ask is about the shape.
#[must_use]
pub fn refuse_command(command_schema: u16, cmd: &Command) -> Option<SchemaError> {
    match command_gate(cmd) {
        Some(gate) if gate.command_schema > command_schema => Some(SchemaError::CommandTooNew {
            feature: gate.feature,
            required: gate.command_schema,
            supported: command_schema,
        }),
        _ => None,
    }
}

impl Ord for SchemaTriple {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.gate_key().cmp(&other.gate_key())
    }
}

impl PartialOrd for SchemaTriple {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for SchemaTriple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.format_version, self.command_schema, self.proto_rev
        )
    }
}

/// What a command needs the cluster to have activated before it may be proposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateRequirement {
    /// The operator-facing feature name, for `feature_gated{feature = …}` and the refusal.
    pub feature: &'static str,
    /// The lowest `command_schema` that can decode this command.
    pub command_schema: u16,
}

/// The gate a command must pass, or `None` when every schema generation can carry it.
///
/// The one place the "which commands are new?" question is answered. A `Put` or `Delete` is
/// gated by its *dedup group* rather than by being a `Put` — the M0 shape of both still
/// decodes under schema 1, and gating it would stop the cluster writing anything at all during
/// a rolling upgrade.
#[must_use]
pub fn command_gate(cmd: &Command) -> Option<GateRequirement> {
    let feature = match cmd {
        Command::Compact { .. } => FEATURE_COMPACT,
        Command::RetireNode { .. } => FEATURE_RETIRE_NODE,
        Command::Put { dedup, .. } | Command::Delete { dedup, .. } if dedup.is_some() => {
            FEATURE_DEDUP
        }
        Command::Put { .. } | Command::Delete { .. } => return None,
    };
    Some(GateRequirement {
        feature,
        command_schema: COMMAND_SCHEMA_V2,
    })
}

/// Why a schema could not accept a command envelope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    /// The envelope decoded, but into a variant this schema generation does not have.
    #[error("command needs {feature} at command_schema {required}; this build is at {supported}")]
    CommandTooNew {
        /// The gated feature the command carries.
        feature: &'static str,
        /// The `command_schema` the command needs.
        required: u16,
        /// The `command_schema` this build has.
        supported: u16,
    },
    /// The envelope did not decode at all.
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{DedupKey, DedupStamp};
    use bytes::Bytes;

    fn put(dedup: Option<DedupStamp>) -> Command {
        Command::Put {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            expected_mod_revision: None,
            dedup,
        }
    }

    fn stamp() -> DedupStamp {
        DedupStamp {
            principal_hash: [7u8; 32],
            key: DedupKey {
                client_id: [9u8; 16],
                request_id: 3,
            },
        }
    }

    #[test]
    fn a_plain_put_is_not_gated_but_a_dedup_bearing_one_is() {
        assert_eq!(command_gate(&put(None)), None);
        let gate = command_gate(&put(Some(stamp()))).expect("dedup is gated");
        assert_eq!(gate.feature, FEATURE_DEDUP);
        assert_eq!(gate.command_schema, COMMAND_SCHEMA_V2);
    }

    #[test]
    fn the_minimum_of_two_triples_is_one_of_them() {
        // Not a field-wise blend: the answer must describe a build that exists.
        let older = COMPAT_SCHEMA_1;
        let newer = CURRENT_SCHEMA;
        assert_eq!(older.min(newer), older);
        assert_eq!(newer.min(older), older);
        assert!(older < newer);
    }

    #[test]
    fn ordering_is_by_command_schema_before_format_version() {
        // A build with the newer store but the older envelope is still the older build for
        // gating purposes, because only `command_schema` can refuse a proposal.
        let big_store_old_commands = SchemaTriple {
            format_version: 9,
            command_schema: COMMAND_SCHEMA_V1,
            proto_rev: 1,
        };
        assert!(big_store_old_commands < CURRENT_SCHEMA);
    }

    #[test]
    fn a_v1_schema_refuses_to_decode_a_v2_only_command() {
        let bytes = Command::Compact {
            up_to_revision: 4,
            dedup_trim_below: None,
        }
        .encode();
        let err = COMPAT_SCHEMA_1
            .decode_command(&bytes)
            .expect_err("compact needs schema 2");
        assert_eq!(
            err,
            SchemaError::CommandTooNew {
                feature: FEATURE_COMPACT,
                required: COMMAND_SCHEMA_V2,
                supported: COMMAND_SCHEMA_V1,
            }
        );
        // The same bytes are fine for the build that produced them.
        assert!(CURRENT_SCHEMA.decode_command(&bytes).is_ok());
    }

    #[test]
    fn a_v1_schema_still_decodes_a_plain_put() {
        let bytes = put(None).encode();
        assert_eq!(
            COMPAT_SCHEMA_1.decode_command(&bytes).expect("ungated"),
            put(None)
        );
    }
}
