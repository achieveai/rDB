//! The `grants/{node}` record: its shape, its CAS body, and the freeze/revoke classification A1
//! runs over a linearizable read of it (team kernel-a `design.md` §5).
//!
//! # Scope: the node-side consumer plus the CAS shape, and nothing else
//!
//! Team kernel-a `design.md` §6 puts the grant **service** — the planner-side writer of
//! `grants/{node}` and its freeze loop — at M8/M9, and keeps for A1 "the node-side consumer plus
//! the CAS shape". [`GrantRecord`] is that shape. The planner's behaviour is modelled as scenario
//! events, which is what makes the races testable without it.
//!
//! # The byte layout is A1's M7 spelling, not a canonical codec
//!
//! [`crate::contracts::control::ReadOutcome::Found`] carries opaque
//! [`bytes::Bytes`], so *something* has to say what those bytes are. No canonical encoder exists
//! in this workspace — [`crate::contracts::errors::Capability::Codec`] is package C0's and is not
//! wired — and `rdb-core` has no serialisation format among its dependencies, only `serde`'s
//! traits. So [`GrantRecord::encode`] and [`GrantRecord::decode`] are a fixed-width
//! little-endian layout with a version byte, written here because this is the only module on
//! either side of it.
//!
//! **It is deliberately boring and deliberately replaceable.** When C0's canonical codec lands,
//! these two functions go and nothing else in A1 moves: [`classify`] takes a decoded
//! [`GrantRecord`] and never sees a byte, so the rule A1 is judged on does not depend on the
//! spelling. A fixture can call [`classify`] directly for the same reason.

use bytes::Bytes;

use crate::contracts::authority::DenyReason;
use crate::contracts::ids::{AuthorityGeneration, BootId, GrantId, NodeId};

/// The version byte every encoded record starts with. A decoder that meets a different one
/// answers `None` rather than reading the remaining bytes as fields.
const LAYOUT_VERSION: u8 = 1;

/// Version byte, `GrantId`, `NodeId`, `BootId`, `AuthorityGeneration`, `expiry_utc_ms`, `frozen`.
const ENCODED_LEN: usize = 1 + 8 + 4 + 8 + 8 + 8 + 1;

/// One `grants/{node}` record, as spec §7.1 describes it and as §7.3's steps read it.
///
/// Every field is here because a `design.md` §2.4 row compares it: the four identity fields
/// decide *whose* grant this is, `frozen` is the planner's revocation step 1, and
/// `expiry_utc_ms` is the `E` the whole bounded-clock rule is written against.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GrantRecord {
    /// Which grant this is. A new grant id is the only thing that gets a node out of
    /// [`crate::authority::AuthorityState::Fenced`].
    pub grant: GrantId,
    /// The node the grant was issued to.
    pub node: NodeId,
    /// The process lifetime it was issued to. A restart gets a new one, so a grant issued to an
    /// earlier boot can never be honoured by the new process (spec §7.2).
    pub boot: BootId,
    /// The cluster authority generation the grant was issued under. The planner bumps it on
    /// every fence, so a holder that sees a different one has had its right re-issued under it.
    pub authority_generation: AuthorityGeneration,
    /// `E`, exactly as the committed record says. Never advanced locally.
    pub expiry_utc_ms: i64,
    /// The planner froze this exact revision (spec §7.3 step 1).
    pub frozen: bool,
}

impl GrantRecord {
    /// This record as the body of a [`crate::contracts::control::ControlEffect::Cas`].
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(ENCODED_LEN);
        out.push(LAYOUT_VERSION);
        out.extend_from_slice(&self.grant.0.to_le_bytes());
        out.extend_from_slice(&self.node.0.to_le_bytes());
        out.extend_from_slice(&self.boot.0.to_le_bytes());
        out.extend_from_slice(&self.authority_generation.0.to_le_bytes());
        out.extend_from_slice(&self.expiry_utc_ms.to_le_bytes());
        out.push(u8::from(self.frozen));
        Bytes::from(out)
    }

    /// Read a record back, or `None` when the bytes are not one.
    ///
    /// `None` rather than an error: a body A1 cannot read is not a grant it holds, and every
    /// caller here already has a "this is not our grant" path. Nothing is guessed from a partial
    /// read.
    #[must_use]
    pub fn decode(body: &[u8]) -> Option<Self> {
        if body.len() != ENCODED_LEN || body[0] != LAYOUT_VERSION {
            return None;
        }
        let u64_at = |offset: usize| -> u64 {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(&body[offset..offset + 8]);
            u64::from_le_bytes(buf)
        };
        let mut node = [0_u8; 4];
        node.copy_from_slice(&body[9..13]);
        Some(Self {
            grant: GrantId(u64_at(1)),
            node: NodeId(u32::from_le_bytes(node)),
            boot: BootId(u64_at(13)),
            authority_generation: AuthorityGeneration(u64_at(21)),
            expiry_utc_ms: i64::from_le_bytes({
                let mut buf = [0_u8; 8];
                buf.copy_from_slice(&body[29..37]);
                buf
            }),
            frozen: body[37] != 0,
        })
    }
}

/// The identity a held grant compares a read-back record against.
///
/// A value rather than a borrow of `Held`, so [`classify`] is callable from a fixture that has
/// no kernel (team kernel-a `KA-1`) and so the comparison cannot accidentally read a field of
/// `Held` that the rule does not name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeldIdentity {
    /// The grant id this node believes it holds.
    pub grant: GrantId,
    /// Its own process lifetime.
    pub boot: BootId,
    /// The authority generation the grant was adopted under.
    pub authority_generation: AuthorityGeneration,
}

/// Why a read-back of `grants/{node}` ends the grant, or `None` when it does not.
///
/// The four fencing rows of team kernel-a `design.md` §2.4's steady-state table, in the table's
/// own order. Order is part of the contract (property 5, finding K-A-43): the first matching row
/// fires and no later row is consulted, so the same trace always names the same reason.
///
/// `None` is **not** "healthy and adopted". It means only that no fence fires; whether the
/// record is worth adopting is the read-back row, and that row is not this function's business.
#[must_use]
pub fn classify(record: &GrantRecord, held: HeldIdentity) -> Option<DenyReason> {
    // Row 1. Freeze wins without a special case elsewhere: the planner's freeze CAS bumps the
    // revision, so a renewal built on the pre-freeze revision loses and re-reads into here.
    if record.frozen {
        return Some(DenyReason::Frozen);
    }
    // Row 2. A different boot is a different process wearing this node's name.
    if record.boot != held.boot {
        return Some(DenyReason::BootMismatch);
    }
    // Row 2 again, the other half: the record names a grant that is not the one we hold, so ours
    // was replaced.
    if record.grant != held.grant {
        return Some(DenyReason::Revoked);
    }
    // Row 3. The cluster re-issued authority under us.
    if record.authority_generation != held.authority_generation {
        return Some(DenyReason::AuthorityGenerationChanged);
    }
    None
}
