//! Issue #87 (ADR-rdb-0020, option A): the change feed's cursor, pages and resume rule.
//!
//! A consumer reads a partition's committed `History` in `seq` order, from the owner or from
//! any regular copy, and keeps a [`FeedCursor`] after each record. Every call checks the cursor
//! against the serving copy's own `Link` chain, which is how a consumer learns of a cut
//! ([`FeedPage::Rewound`]). Pure: no IO and no engine. [`crate::host`] reads the records and
//! hands them here.

use bytes::Bytes;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::ReplicationEnvelope;
use rdb_core::contracts::ids::{AffinityId, Generation, RequestIdentity, Seq, TenantId};
use rdb_core::contracts::storage::Namespace;

/// The most records one page returns.
pub const FEED_PAGE_RECORDS: u32 = 256;
/// The most encoded record bytes one page returns. A page always holds at least one record, so
/// a larger record still advances.
pub const FEED_PAGE_BYTES: usize = 4 << 20;
/// The most records one call examines. Above [`FEED_PAGE_RECORDS`] it binds only a call that
/// skips records: those with no write under the caller's tenant.
pub const FEED_SCAN_RECORDS: u32 = 1_024;
/// The most encoded record bytes one call examines, returned or skipped. Counted after each
/// read, so a call can pass it by one record; it always examines at least one. Twice
/// [`FEED_PAGE_BYTES`], so it binds only a call that skips records.
pub const FEED_SCAN_BYTES: usize = 2 * FEED_PAGE_BYTES;

/// The position after one record: its header's stamp (the generation that wrote it, not the
/// one that served it), its seq and its chained record digest. Equal digest at equal seq means
/// equal prefix, so a cursor matches in every lineage that kept its record and in none that cut
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedCursor {
    /// The stamp in the record's header.
    pub generation: Generation,
    /// The record's seq.
    pub seq: Seq,
    /// The record's `record_digest`.
    pub digest: Digest,
}

impl FeedCursor {
    /// Before the first record.
    pub const START: Self = Self {
        generation: Generation(0),
        seq: Seq(0),
        digest: Digest::ROOT,
    };

    /// The cursor after `envelope`.
    #[must_use]
    pub const fn after(envelope: &ReplicationEnvelope) -> Self {
        Self {
            generation: envelope.header.generation,
            seq: envelope.header.seq,
            digest: envelope.record_digest,
        }
    }
}

/// One `User` write of a record: the after-image, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedWrite {
    /// The affinity group, parsed from the key.
    pub group: AffinityId,
    /// The object id, parsed from the key (unescaped).
    pub object: Bytes,
    /// The stored key.
    pub key: Bytes,
    /// The stored value; `None` is a delete.
    pub value: Option<Bytes>,
}

/// One committed transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedRecord {
    /// The cursor after it.
    pub at: FeedCursor,
    /// The request that wrote it.
    pub request: RequestIdentity,
    /// Its `User` writes, in batch order.
    pub writes: Vec<FeedWrite>,
}

/// A copy's answer to a feed call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedPage {
    /// Whole records after the cursor, in seq order, and the cursor to call with next. `more`
    /// is set when a page cap stopped it below what this copy may serve: call again at once. An
    /// empty page with `more` unset means nothing new.
    Records {
        /// The records.
        records: Vec<FeedRecord>,
        /// The cursor after the last record examined.
        next: FeedCursor,
        /// Whether a cap stopped the page early.
        more: bool,
    },
    /// The cursor is ahead of what this copy may serve, or names a generation it has not
    /// landed. Keep the cursor; wait, or ask another copy.
    Behind {
        /// The highest seq this copy may serve.
        head: Seq,
    },
    /// This copy does not hold the record the cursor needs: ask another copy, or rebuild from
    /// the app's own source.
    Trimmed {
        /// The lowest seq this copy holds.
        floor: Seq,
    },
    /// The cursor's record was cut. Retract what was applied above `to.seq` and resume from
    /// `to`. A consumer that ignores this keeps data rDB lost.
    Rewound {
        /// The last record the cut kept.
        to: FeedCursor,
    },
}

/// One level of a copy's chain, newest first: `[(Gc, None), (G1, e1), (G2, e2), ...]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    /// The generation.
    pub generation: Generation,
    /// The cutoff at which the level above left this one (its `Link.base`); `None` for the
    /// newest level.
    pub exit: Option<Seq>,
}

/// What [`resume`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resume {
    /// The cursor holds here: serve the records after it.
    Serve(FeedCursor),
    /// Answer this page instead.
    Answer(FeedPage),
    /// The record at the cursor's seq is on the cursor's lineage here, with another digest:
    /// divergence (spec §8.1), `CORRUPT_HISTORY`.
    Diverged {
        /// The seq.
        seq: Seq,
        /// The digest this copy holds there.
        held: Digest,
    },
}

/// Check `cursor` against a copy's `chain` (design-87 §2). `cap` is the highest seq the copy
/// may serve, `floor` the lowest it holds, and `lookup(seq)` the stamp and digest of the record
/// at `seq` read through the chain from its newest level. A cursor whose exact stamp and digest
/// this copy serves at its seq is kept, whichever level holds it; only then do the chain's
/// exits judge it.
///
/// # Errors
///
/// Whatever `lookup` fails with.
pub fn resume<E>(
    cursor: FeedCursor,
    chain: &[Level],
    cap: Seq,
    floor: Seq,
    mut lookup: impl FnMut(Seq) -> Result<Option<(Generation, Digest)>, E>,
) -> Result<Resume, E> {
    let start = || {
        Resume::Answer(if floor.0 > 1 {
            FeedPage::Trimmed { floor }
        } else {
            FeedPage::Rewound {
                to: FeedCursor::START,
            }
        })
    };
    if cursor.seq.0 == 0 {
        return Ok(if floor.0 > 1 {
            Resume::Answer(FeedPage::Trimmed { floor })
        } else {
            Resume::Serve(FeedCursor::START)
        });
    }
    let newest = chain
        .first()
        .map_or(Generation(0), |level| level.generation);
    let at = chain
        .iter()
        .position(|level| level.generation == cursor.generation);
    // A newer stamp: this copy is stale, and its chain cannot judge the cursor (M1).
    if at.is_none() && cursor.generation > newest {
        return Ok(Resume::Answer(FeedPage::Behind { head: cap }));
    }
    // The record at the cursor's seq as this copy serves it, read once.
    let held = if cursor.seq <= cap {
        Some(lookup(cursor.seq)?)
    } else {
        None
    };
    // Equal stamp and digest at equal seq is equal prefix wherever the record sits: a tail
    // adopted under #105 keeps its stamp on a level of another generation, and a copy that
    // linked below a kept record catches it up with its old stamp onto its newest level, above
    // that stamp's exit (PR #170 review r1). Only a record this copy may serve counts.
    if held == Some(Some((cursor.generation, cursor.digest))) {
        return Ok(Resume::Serve(cursor));
    }
    // A fork this copy cannot locate: rewinding further than needed costs only replay (M2).
    let Some(at) = at else {
        return Ok(start());
    };
    // The effective exit: the lowest cutoff on the path from the stamp's level up to the
    // newest. A level above can cut lower than the stamp's own exit (R1).
    let exit = chain[1..=at].iter().filter_map(|level| level.exit).min();
    if let Some(exit) = exit.filter(|exit| cursor.seq > *exit) {
        if exit.0 == 0 {
            return Ok(Resume::Answer(FeedPage::Rewound {
                to: FeedCursor::START,
            }));
        }
        if exit < floor {
            return Ok(Resume::Answer(FeedPage::Trimmed { floor }));
        }
        return Ok(Resume::Answer(match lookup(exit)? {
            Some((stamp, digest)) => FeedPage::Rewound {
                to: FeedCursor {
                    generation: stamp,
                    seq: exit,
                    digest,
                },
            },
            None => FeedPage::Trimmed { floor },
        }));
    }
    let Some(held) = held else {
        return Ok(Resume::Answer(FeedPage::Behind { head: cap }));
    };
    Ok(match held {
        None => Resume::Answer(FeedPage::Trimmed { floor }),
        Some((_, held)) if held != cursor.digest => Resume::Diverged {
            seq: cursor.seq,
            held,
        },
        Some(_) => Resume::Serve(cursor),
    })
}

/// One record as a page reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scanned {
    /// The cursor after it.
    pub at: FeedCursor,
    /// Its encoded length.
    pub bytes: usize,
    /// What the page returns of it; `None` skips it (no write under the caller's tenant).
    pub record: Option<FeedRecord>,
}

/// The records after `after` up to `cap`, read one seq at a time by `read`, stopped by `max`,
/// [`FEED_PAGE_RECORDS`], [`FEED_PAGE_BYTES`], [`FEED_SCAN_RECORDS`] and [`FEED_SCAN_BYTES`].
/// Whole records only, and at least one when any is due. `next` is after the last record
/// examined, so skipped records still move it, and `more` is set whenever a cap stopped the
/// call below `cap`.
///
/// # Errors
///
/// Whatever `read` fails with.
pub fn page<E>(
    after: FeedCursor,
    cap: Seq,
    max: u32,
    mut read: impl FnMut(Seq) -> Result<Scanned, E>,
) -> Result<FeedPage, E> {
    let returned = max.min(FEED_PAGE_RECORDS) as usize;
    let (mut records, mut bytes, mut scanned, mut next) = (Vec::new(), 0_usize, 0_u32, after);
    let mut examined = 0_usize;
    while next.seq < cap
        && scanned < FEED_SCAN_RECORDS
        && examined < FEED_SCAN_BYTES
        && records.len() < returned
    {
        let found = read(Seq(next.seq.0 + 1))?;
        examined = examined.saturating_add(found.bytes);
        if let Some(record) = found.record {
            if !records.is_empty() && bytes.saturating_add(found.bytes) > FEED_PAGE_BYTES {
                break;
            }
            bytes = bytes.saturating_add(found.bytes);
            records.push(record);
        }
        next = found.at;
        scanned += 1;
    }
    Ok(FeedPage::Records {
        records,
        next,
        more: next.seq < cap,
    })
}

/// `envelope` as `tenant`'s feed record: its `User` writes under `tenant`, with the group and
/// object id parsed from each key. `None` when it has none: a page skips it (Q3, the tenant
/// comes from the caller's identity).
///
/// # Errors
///
/// A `User` key that does not parse, naming the write's index.
pub fn record_of(
    envelope: &ReplicationEnvelope,
    tenant: TenantId,
) -> Result<Option<FeedRecord>, String> {
    let mut writes = Vec::new();
    for (index, write) in envelope.mutations.iter().enumerate() {
        if write.ns != Namespace::User {
            continue;
        }
        let parsed = rdb_value::keys::parse(&write.key).map_err(|error| {
            format!(
                "write {index} of record {}: the key does not parse: {error}",
                envelope.header.seq.0
            )
        })?;
        if parsed.tenant != tenant {
            continue;
        }
        writes.push(FeedWrite {
            group: parsed.affinity,
            object: Bytes::from(parsed.object_id),
            key: write.key.clone(),
            value: write.value.clone(),
        });
    }
    if writes.is_empty() {
        return Ok(None);
    }
    Ok(Some(FeedRecord {
        at: FeedCursor::after(envelope),
        request: envelope.request_identity,
        writes,
    }))
}

#[cfg(test)]
mod tests;
