//! The deterministic in-memory KV state machine (M0, spec §7.4, ADR-0005, ADR-0006).
//!
//! [`KvState`] is pure and synchronous. It reads no clock, draws no entropy, consults no
//! ambient process state, performs no I/O, and iterates nothing unordered. Two replicas fed
//! the same command sequence therefore reach byte-identical state — which is the whole point,
//! since a replicated state machine that diverges is worse than one that fails.
//!
//! Everything node-local lives elsewhere: `last_applied`, membership, and storage identity
//! belong to `config-storage`, and are deliberately excluded from [`KvState::state_hash`] so
//! two nodes with the same applied prefix hash identically.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::command::{Command, CommandResponse, DedupStamp, MutationEvent};
use crate::identity::NodeId;
use crate::limits::Limits;
use crate::types::{GetResponse, ListRequest, ListResponse, MutationResponse, Record};
use crate::validate::{validate_command, validate_list};

/// Maximum key bytes rendered into the `key_hex` log field, giving 64 hex characters
/// (ADR-0013 redaction rule).
const KEY_HEX_MAX_BYTES: usize = 32;

/// The retained outcome of one deduplicated mutation (M5, ADR-0025).
///
/// Stored under `(principal_hash, client_id, request_id)` and replayed verbatim when that
/// triple is resubmitted within the window. The **whole** [`MutationResponse`] is kept, not
/// just the outcome and revision: a `Conflict` hit must replay the `exists` and
/// `current_mod_revision` the original submission observed, or the resubmission would answer a
/// CAS question against state the client never saw.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DedupRecord {
    /// The exact client-visible response the original submission produced.
    pub response: MutationResponse,
    /// `cluster_revision` at the moment this record was written.
    ///
    /// The trim watermark [`Command::Compact`]'s `dedup_trim_below` is compared against. It is
    /// a revision rather than a timestamp on purpose: `apply` reads no clock (spec §7.4), and
    /// a monotonic counter it already has is enough to order records by age.
    pub applied_revision: u64,
}

impl DedupRecord {
    /// The retained outcome.
    pub fn outcome(&self) -> crate::types::MutationOutcome {
        self.response.outcome
    }

    /// The revision the retained response reported.
    pub fn revision(&self) -> u64 {
        self.response.revision
    }
}

/// The replicated index key of a [`DedupRecord`]: `(principal_hash, client_id, request_id)`.
///
/// Ordered exactly as the `dedup` column family's byte key
/// `principal_hash(32) || client_id(16) || request_id(8, big-endian)` orders, so an in-memory
/// range over one `(principal, client_id)` pair and a RocksDB seek over the same prefix walk
/// the same records in the same order (ADR-0025).
pub type DedupIndexKey = ([u8; 32], [u8; 16], u64);

/// The index key a stamp addresses.
pub(crate) fn dedup_index_key(stamp: &DedupStamp) -> DedupIndexKey {
    (
        stamp.principal_hash,
        stamp.key.client_id,
        stamp.key.request_id,
    )
}

/// The 56-byte `dedup` column-family key for an index key (M5, ADR-0025).
///
/// `request_id` is big-endian so RocksDB's lexical order *is* submission order within one
/// `(principal, client_id)` pair — the same convention the `events` family uses for revision
/// order — which is what makes the window a seek rather than a scan.
pub fn dedup_storage_key((principal_hash, client_id, request_id): &DedupIndexKey) -> [u8; 56] {
    let mut out = [0u8; 56];
    out[..32].copy_from_slice(principal_hash);
    out[32..48].copy_from_slice(client_id);
    out[48..].copy_from_slice(&request_id.to_be_bytes());
    out
}

/// Parse a `dedup` column-family key back into its index key.
pub fn dedup_index_key_from_storage(raw: &[u8]) -> Option<DedupIndexKey> {
    if raw.len() != 56 {
        return None;
    }
    let mut principal_hash = [0u8; 32];
    principal_hash.copy_from_slice(&raw[..32]);
    let mut client_id = [0u8; 16];
    client_id.copy_from_slice(&raw[32..48]);
    let mut request_id = [0u8; 8];
    request_id.copy_from_slice(&raw[48..]);
    Some((principal_hash, client_id, u64::from_be_bytes(request_id)))
}

/// What a deduplication lookup found.
enum DedupLookup {
    /// This exact `(principal, client_id, request_id)` was applied before.
    Hit(DedupRecord),
    /// Not seen, and above every retained id for the pair.
    Miss,
    /// Not seen, but at or below an id the pair still retains — a client that reused or
    /// reordered ids, which is a client bug and is told so rather than silently absorbed.
    NotMonotonic {
        /// The highest retained id for this `(principal, client_id)` pair.
        floor: u64,
    },
}

/// Durable side effects one [`KvState::apply_with_effects`] call produced, for the storage
/// layer to mirror into the same synced batch (M5).
///
/// Every field is a *consequence* of a deterministic decision the state machine already made,
/// never an input to one: two voters fed the same command produce the same effects, which is
/// what lets storage write them without re-deriving anything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyEffects {
    /// A deduplication record to write, with the index key it is stored under.
    pub dedup_inserted: Option<(DedupIndexKey, DedupRecord)>,
    /// Deduplication records to delete — window evictions and `Compact` trims together.
    pub dedup_removed: Vec<DedupIndexKey>,
    /// Records evicted because their `(principal, client_id)` window was full.
    /// `retcd_dedup_evictions_total{reason="window"}`.
    pub dedup_window_evictions: u64,
    /// Records dropped by a `Compact` trim.
    /// `retcd_dedup_evictions_total{reason="trim"}`.
    pub dedup_trim_evictions: u64,
    /// Mutations that applied but stored no record because the global cap was already
    /// reached (OQ-49).
    pub dedup_cap_refusals: u64,
    /// A node id newly added to the retired set, if this command added one.
    pub retired_node: Option<NodeId>,
    /// The new value of [`KvState::max_applied_command_schema`], if this command raised it
    /// (M6, ADR-0030 ruling M6-R15).
    pub max_command_schema: Option<u16>,
}

impl ApplyEffects {
    /// Whether any dedup or retirement side effect needs writing.
    ///
    /// Deliberately does **not** consider [`ApplyEffects::max_command_schema`]: that field is
    /// not a side effect of the command's *meaning* but a record that this node decoded the
    /// envelope at all (M6-R15), and the storage layer writes it from its own `Option` rather
    /// than behind this test. Folding it in here would also make a dedup-bearing command look
    /// non-empty on a store with deduplication switched off, which is exactly the distinction
    /// this predicate exists to draw (M5-108).
    pub fn is_empty(&self) -> bool {
        self.dedup_inserted.is_none()
            && self.dedup_removed.is_empty()
            && self.retired_node.is_none()
    }
}

/// The replicated key/value state.
///
/// Keys are ordered by unsigned bytewise lexical order via [`BTreeMap`], which is both the
/// order `List` must return and the order [`KvState::state_hash`] folds records in. Using an
/// ordered map is not an optimization here — an unordered one would make the hash
/// non-deterministic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvState {
    records: BTreeMap<Bytes, Record>,
    cluster_revision: u64,
    /// Retained-history watermark (M4, ADR-0019): every revision at or below it has had its
    /// journal event dropped. `0` means nothing has ever been compacted.
    ///
    /// Deliberately **not** part of [`KvState::state_hash`] (lead ruling R1 of 2026-09-18,
    /// ADR-0019 / ADR-0021 notes): the v1 -> v2 migration stamps it from each node's *local*
    /// `cluster_revision` at upgrade time, so during a rolling upgrade three correct voters
    /// legitimately hold three different watermarks. Folding it into the divergence oracle
    /// would report that as corruption. Journal equality is asserted separately, by
    /// `StateReader::journal_hash(from_exclusive)` over a common lower bound.
    compact_revision: u64,
    /// Retained deduplication records, keyed by `(principal_hash, client_id, request_id)`
    /// (M5, ADR-0025). Mirrored by the storage layer into the `dedup` column family inside
    /// the same synced batch as the KV change, and rebuilt from it on open.
    ///
    /// An ordered map, not a hash map, for the same reason [`KvState::records`] is: a range
    /// over one `(principal, client_id)` prefix is how the window is evaluated, and unordered
    /// iteration inside `apply` would be non-deterministic (spec §7.4).,
    dedup: BTreeMap<DedupIndexKey, DedupRecord>,
    /// Node ids that have been removed and may never rejoin (M5, ADR-0023, M5-R4).
    retired_nodes: BTreeSet<NodeId>,
    /// The highest `command_schema` this state machine has ever applied (M6, ADR-0030 ruling
    /// M6-R15).
    ///
    /// Durable, monotonic proof that this node can decode that generation of the envelope —
    /// it applied one. The schema gate reads it so that an unreachable voter delays only the
    /// *first* activation of a feature and never re-gates a cluster that is already using it;
    /// without it, one node going down after a failover turns into a write outage
    /// (regression found on `m4_88`).
    max_applied_command_schema: u16,
    /// Deduplication lookups that returned a retained outcome, since this state was built
    /// (`retcd_dedup_hits_total`), with the two eviction counters beside it
    /// (`retcd_dedup_evictions_total{reason}`, ADR-0026).
    ///
    /// Deterministic over an applied prefix - the same command sequence produces the same
    /// three numbers on every voter - but **not** part of [`KvState::state_hash`] and not
    /// carried in a snapshot: a node that installs a snapshot or restarts starts them at
    /// zero, exactly as a process counter should. They live here rather than in the storage
    /// layer because the decisions they count are made here, and both stores would otherwise
    /// have to re-derive them from [`ApplyEffects`] identically.
    dedup_hits: u64,
    /// Records dropped because a `(principal, client_id)` window was full.
    dedup_window_evictions: u64,
    /// Records dropped by a `Compact` command carrying `dedup_trim_below`.
    dedup_trim_evictions: u64,
    /// Submissions whose outcome was **not** retained because the global `max_records` cap was
    /// already reached (OQ-49, review finding C5B-04).
    ///
    /// Counted separately from the two eviction counters because it is not an eviction: no
    /// record was dropped, one was never written. It is also the only one of the three that
    /// changes what a *client* may do — the mutation applied, but a resubmission will apply a
    /// second time — so an operator reading eviction counters alone would see the cap's
    /// pressure and miss its consequence.
    dedup_cap_refusals: u64,
    /// Compactions this state has applied that advanced the watermark
    /// (`retcd_compactions_total`, ADR-0019). Counted here so every voter counts the ones it
    /// actually applied, not the ones its leader proposed.
    compactions: u64,
    limits: Limits,
}

impl Default for KvState {
    fn default() -> Self {
        Self::new()
    }
}

impl KvState {
    /// An empty state machine at `cluster_revision = 0` with the spec §7.1 caps.
    pub fn new() -> Self {
        Self::with_limits(Limits::DEFAULT)
    }

    /// An empty state machine with explicit caps.
    ///
    /// Apply-time validation is part of the replicated state machine, so **every voter must
    /// be configured with identical limits**; otherwise a borderline command could be applied
    /// on one node and rejected on another. Tests use this to shrink caps to a few bytes
    /// instead of building megabyte payloads.
    pub fn with_limits(limits: Limits) -> Self {
        Self {
            records: BTreeMap::new(),
            cluster_revision: 0,
            compact_revision: 0,
            dedup: BTreeMap::new(),
            retired_nodes: BTreeSet::new(),
            max_applied_command_schema: crate::COMMAND_SCHEMA_V1,
            dedup_hits: 0,
            dedup_window_evictions: 0,
            dedup_trim_evictions: 0,
            dedup_cap_refusals: 0,
            compactions: 0,
            limits,
        }
    }

    /// Rebuild a state machine from a restored snapshot or a storage scan.
    ///
    /// The caller is responsible for the pair being consistent: `revision` must be the
    /// `cluster_revision` those records were captured at. This is the seam `config-storage`
    /// uses on restart, and the seam a test uses to prove that a restored machine and a
    /// replayed one hash identically.
    ///
    /// `limits` must be the same caps the machine ran with before the restart; apply-time
    /// validation is replicated state, so restoring under different caps diverges the voter.
    pub fn from_parts(limits: Limits, revision: u64, records: BTreeMap<Bytes, Record>) -> Self {
        Self {
            records,
            cluster_revision: revision,
            compact_revision: 0,
            dedup: BTreeMap::new(),
            retired_nodes: BTreeSet::new(),
            max_applied_command_schema: crate::COMMAND_SCHEMA_V1,
            dedup_hits: 0,
            dedup_window_evictions: 0,
            dedup_trim_evictions: 0,
            dedup_cap_refusals: 0,
            compactions: 0,
            limits,
        }
    }

    /// The caps this machine validates against.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// The public revision: `0` for an empty store, `+1` for each state-changing mutation.
    ///
    /// This is **not** the Raft log index. Blank, membership, rejected, and conflicting
    /// entries occupy log indexes without allocating a revision (ADR-0005).
    pub fn cluster_revision(&self) -> u64 {
        self.cluster_revision
    }

    /// The retained-history watermark (M4, ADR-0019).
    ///
    /// `0` means nothing has ever been compacted and *every* revision from 1 upwards is still
    /// resumable. A resume cursor `R` is refused only when `compact_revision > 0 && R <=
    /// compact_revision` (OQ-27), which is why the zero case must not be special-cased away.
    pub fn compact_revision(&self) -> u64 {
        self.compact_revision
    }

    /// Restore the watermark read back from storage on open, or stamped by the v1 -> v2
    /// migration (ADR-0021).
    ///
    /// Monotonic, exactly like the replicated apply path: a value below the current watermark
    /// is ignored, so a stale metadata read can never resurrect history the store no longer
    /// holds. This is a **local** restore seam, not a replicated one; `Command::Compact` is the
    /// only way a running cluster advances the watermark.
    pub fn restore_compact_revision(&mut self, revision: u64) {
        self.compact_revision = self.compact_revision.max(revision);
    }

    /// Restore the deduplication records read back from the `dedup` column family on open, or
    /// installed from a snapshot (M5, ADR-0025).
    ///
    /// A **local** restore seam, like [`KvState::restore_compact_revision`]: it replaces the
    /// in-memory mirror wholesale rather than merging, because the column family — not this
    /// map — is the durable copy, and a merge would let a stale mirror resurrect a record the
    /// store no longer holds.
    pub fn restore_dedup(&mut self, records: BTreeMap<DedupIndexKey, DedupRecord>) {
        self.dedup = records;
    }

    /// Restore the retired-node set read back from `state_meta/retired_nodes` on open (M5,
    /// ADR-0023).
    ///
    /// The union of what is stored and what is already known, because retirement is
    /// permanent: forgetting an id would un-fence a node, and there is no command that
    /// un-retires one.
    pub fn restore_retired_nodes(&mut self, nodes: impl IntoIterator<Item = NodeId>) {
        self.retired_nodes.extend(nodes);
    }

    /// Restore the durable activation watermark read back from `state_meta/max_command_schema`
    /// on open, or carried by an installed snapshot (M6, ADR-0030 M6-R15).
    ///
    /// Unioned by `max`, like [`KvState::restore_retired_nodes`] and for the same reason: the
    /// fact recorded is "this state has already carried that generation", and nothing can make
    /// that untrue afterwards. Lowering it would re-gate a cluster that is already activated.
    pub fn restore_max_applied_command_schema(&mut self, schema: u16) {
        self.max_applied_command_schema = self.max_applied_command_schema.max(schema);
    }

    /// The highest `command_schema` ever applied here (M6, ADR-0030 M6-R15).
    pub fn max_applied_command_schema(&self) -> u16 {
        self.max_applied_command_schema
    }

    /// Node ids that have been removed from the cluster and may never rejoin (M5, ADR-0023).
    ///
    /// Replicated state, not a leader-local list: the peer plane consults it to refuse a
    /// retired id with `identity_retired`, and that refusal has to hold on every node
    /// (TA-51, spec §21 M5 "stale identities cannot rejoin").
    pub fn retired_nodes(&self) -> &BTreeSet<NodeId> {
        &self.retired_nodes
    }

    /// Whether `node_id` has been retired.
    pub fn is_retired(&self, node_id: NodeId) -> bool {
        self.retired_nodes.contains(&node_id)
    }

    /// Retained deduplication records, in `(principal_hash, client_id, request_id)` order.
    pub fn dedup_records(&self) -> impl Iterator<Item = (&DedupIndexKey, &DedupRecord)> {
        self.dedup.iter()
    }

    /// How many deduplication records are retained, against
    /// [`crate::DedupLimits::max_records`].
    pub fn dedup_len(&self) -> u64 {
        self.dedup.len() as u64
    }

    /// Deduplication hits served since this state was built (`retcd_dedup_hits_total`).
    pub fn dedup_hits(&self) -> u64 {
        self.dedup_hits
    }

    /// Records evicted by a full `(principal, client_id)` window since this state was built
    /// (`retcd_dedup_evictions_total{reason="window"}`).
    pub fn dedup_window_evictions(&self) -> u64 {
        self.dedup_window_evictions
    }

    /// Compactions applied here that advanced the watermark (`retcd_compactions_total`).
    pub fn compactions(&self) -> u64 {
        self.compactions
    }

    /// Records released by a `Compact` carrying `dedup_trim_below` since this state was built
    /// (`retcd_dedup_evictions_total{reason="trim"}`).
    ///
    /// Reported under `trim`, not `global_cap`, since review finding C5B-04. A trim is applied
    /// retention: the leader proposes `dedup_trim_below` on every compaction, cap pressure or
    /// none, so counting trims as cap evictions told an operator the cap was shedding their
    /// records on a cluster that had never reached it. The cap's own event is
    /// [`KvState::dedup_cap_refusals`].
    pub fn dedup_trim_evictions(&self) -> u64 {
        self.dedup_trim_evictions
    }

    /// Outcomes the global cap refused to retain since this state was built
    /// (`retcd_dedup_cap_refusals_total`). See the field.
    pub fn dedup_cap_refusals(&self) -> u64 {
        self.dedup_cap_refusals
    }

    /// Number of live records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the store holds no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Records in ascending unsigned bytewise key order.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Record)> {
        self.records.iter()
    }

    /// Look up one key.
    pub fn get(&self, key: &[u8]) -> Option<&Record> {
        self.records.get(key)
    }

    /// Look up one key and pair it with the revision the read observed.
    ///
    /// An absent key yields `record: None` with the revision still populated, so a caller can
    /// tell "absent as of revision N" from "the read did not happen" (spec §7.3).
    pub fn get_response(&self, key: &[u8]) -> GetResponse {
        GetResponse {
            record: self.get(key).cloned(),
            read_revision: self.cluster_revision,
        }
    }

    /// Bounded prefix scan in ascending unsigned bytewise key order (spec §10.2).
    ///
    /// This is read-only: it allocates no revision and changes no state, so
    /// [`KvState::state_hash`] is identical before and after any number of calls.
    ///
    /// The request is clamped through [`validate_list`] first. Records are accumulated until
    /// `max_items` or `max_bytes` would be exceeded, at which point `truncated` is set and
    /// the scan stops. One special case guarantees forward progress: if the *first* matching
    /// record alone exceeds `max_bytes`, it is returned anyway with `truncated = true`,
    /// because a zero-record truncated response tells the caller nothing it can act on.
    ///
    /// A prefix that fails validation yields an empty, untruncated response at the current
    /// revision; the API edge rejects such a request before ever reaching here.
    pub fn list(&self, req: &ListRequest) -> ListResponse {
        let Ok(req) = validate_list(req, &self.limits) else {
            return ListResponse {
                records: Vec::new(),
                read_revision: self.cluster_revision,
                truncated: false,
            };
        };

        let prefix = req.prefix.clone();
        let mut records: Vec<Record> = Vec::new();
        let mut used: u64 = 0;
        let mut truncated = false;

        // Start at the prefix and walk forward, stopping at the first key that no longer
        // carries it. Walking rather than computing an exclusive upper bound sidesteps the
        // all-0xFF prefix overflow entirely.
        for (key, record) in self.records.range(prefix.clone()..) {
            if !key.starts_with(&prefix) {
                break;
            }
            if records.len() as u64 >= u64::from(req.max_items) {
                truncated = true;
                break;
            }
            let cost = Limits::list_record_cost(key.len(), record.value.len());
            if records.is_empty() {
                records.push(record.clone());
                used = cost;
                if cost > req.max_bytes {
                    truncated = true;
                    break;
                }
            } else if used + cost > req.max_bytes {
                truncated = true;
                break;
            } else {
                used += cost;
                records.push(record.clone());
            }
        }

        ListResponse {
            records,
            read_revision: self.cluster_revision,
            truncated,
        }
    }

    /// Apply one replicated command.
    ///
    /// Total and infallible: every input, including a crafted or over-cap one, produces a
    /// [`CommandResponse`]. It never returns `Result` and never panics, because a state
    /// machine that can fail to answer for a committed entry stalls apply on that node and
    /// diverges it from its peers.
    ///
    /// Revision allocation is inside this function, so it happens exactly once per
    /// state-changing mutation in committed apply order (ADR-0005). Rejected, conflicting,
    /// and not-found commands allocate nothing.
    pub fn apply(&mut self, cmd: &Command) -> CommandResponse {
        self.apply_with_effects(cmd, &mut ApplyEffects::default())
    }

    /// Apply one replicated command with `principal_hash` bound to its deduplication key
    /// (M5, ADR-0025 "Principal binding").
    ///
    /// The hash **replaces** whatever the command carried, so a command that claimed another
    /// principal's `client_id` namespace is evaluated under the caller's own namespace and is
    /// a miss there, never a hit returning someone else's outcome (test plan M5-101).
    ///
    /// This is the leader's bind seam, expressed against the state machine so it can be
    /// asserted without a transport. The replicated path is [`KvState::apply`]: by the time an
    /// entry is in the log its stamp is already bound, and re-deriving the principal on a
    /// follower — which has no session for that entry — is impossible, which is exactly why
    /// the bound hash travels in the envelope.
    pub fn apply_with_principal(
        &mut self,
        cmd: &Command,
        principal_hash: [u8; 32],
    ) -> CommandResponse {
        let bound = cmd.clone().bind_principal(principal_hash);
        self.apply(&bound)
    }

    /// Apply one replicated command, reporting the durable side effects the storage layer must
    /// mirror into the `dedup` column family and `state_meta/retired_nodes` (M5).
    ///
    /// `effects` is an out-parameter rather than part of [`CommandResponse`] because the
    /// response is OpenRaft's `R` type: it is replicated to a caller and must describe the
    /// *client-visible* outcome, not the storage layer's bookkeeping.
    pub fn apply_with_effects(
        &mut self,
        cmd: &Command,
        effects: &mut ApplyEffects,
    ) -> CommandResponse {
        // Invariant: `validate_list` can only fail on `prefix > max_key_bytes`, which matches
        // no legal key, so an empty page is the honest answer rather than an error.
        if let Err(err) = validate_command(cmd, &self.limits) {
            let reason = err.to_string();
            tracing::warn!(
                op = cmd.op_name(),
                key_hex = %key_hex(cmd.key()),
                outcome = "rejected",
                revision = self.cluster_revision,
                expected = ?cmd.expected_mod_revision(),
                reason = %reason,
                "rejected replicated command at apply time"
            );
            return CommandResponse::Rejected { reason };
        }
        // Recorded before the command is evaluated, and for every command that gets this far,
        // because the fact being recorded is that this node *decoded* the entry — which a
        // rejected or deduplicated command proves just as well as an applied one (M6-R15).
        if let Some(gate) = crate::schema::command_gate(cmd) {
            if gate.command_schema > self.max_applied_command_schema {
                self.max_applied_command_schema = gate.command_schema;
                effects.max_command_schema = Some(gate.command_schema);
            }
        }
        // Handled before the revision-exhaustion guard: neither maintenance command allocates
        // a revision, so a machine that has exhausted the revision space must still be able to
        // shed history and to fence a removed node out.
        if let Command::Compact {
            up_to_revision,
            dedup_trim_below,
        } = cmd
        {
            return self.apply_compact(*up_to_revision, *dedup_trim_below, effects);
        }
        if let Command::RetireNode { node_id } = cmd {
            return self.apply_retire_node(*node_id, effects);
        }

        // ADR-0025: the deduplication lookup happens **before** the command is evaluated
        // against `kv`, so a hit allocates nothing, writes no journal event, and cannot
        // observe state that moved since the original submission.
        let stamp = cmd.dedup().filter(|_| self.limits.dedup.enabled);
        if let Some(stamp) = stamp {
            match self.dedup_lookup(&stamp) {
                DedupLookup::Hit(record) => {
                    self.dedup_hits += 1;
                    tracing::debug!(
                        op = cmd.op_name(),
                        key_hex = %key_hex(cmd.key()),
                        outcome = "dedup_hit",
                        revision = record.revision(),
                        client_id_hex = %hex16(&stamp.key.client_id),
                        request_id = stamp.key.request_id,
                        original_revision = record.revision(),
                        "dedup_hit"
                    );
                    // The flag describes *this* submission, so it is set on the way out
                    // rather than stored: the retained record holds the original response,
                    // with `dedup_hit: false`, exactly as the first caller received it.
                    let mut response = record.response;
                    response.dedup_hit = true;
                    // A hit is proof a record exists, so a further resubmission inside the
                    // window will be recognized too (C5B-05).
                    response.dedup_recorded = true;
                    return CommandResponse::Mutation {
                        response,
                        event: None,
                        dedup_hit: true,
                        dedup_recorded: true,
                    };
                }
                DedupLookup::NotMonotonic { floor } => {
                    let reason = format!(
                        "request_id_not_monotonic: request_id {} is not above the \
                                 retained floor {floor} for this (principal, client_id)",
                        stamp.key.request_id
                    );
                    tracing::debug!(
                        op = cmd.op_name(),
                        key_hex = %key_hex(cmd.key()),
                        outcome = "rejected",
                        revision = self.cluster_revision,
                        client_id_hex = %hex16(&stamp.key.client_id),
                        request_id = stamp.key.request_id,
                        floor,
                        reason = %reason,
                        "dedup_rejected"
                    );
                    return CommandResponse::Rejected { reason };
                }
                DedupLookup::Miss => {}
            }
        }
        // Reachable only through a corrupted persisted revision, never by 2^64 mutations.
        // Wrapping to 0 in release builds would silently diverge voters, so refuse instead.
        if self.cluster_revision == u64::MAX {
            let reason = "cluster_revision exhausted".to_string();
            tracing::error!(
                op = cmd.op_name(),
                key_hex = %key_hex(cmd.key()),
                outcome = "rejected",
                revision = self.cluster_revision,
                reason = %reason,
                "rejected replicated command: revision space exhausted"
            );
            return CommandResponse::Rejected { reason };
        }

        let response = match cmd {
            Command::Put {
                key,
                value,
                expected_mod_revision,
                ..
            } => self.apply_put(key, value, *expected_mod_revision),
            Command::Delete {
                key,
                expected_mod_revision,
                ..
            } => self.apply_delete(key, *expected_mod_revision),
            // Unreachable: both are handled above, before the revision-exhaustion guard.
            Command::Compact {
                up_to_revision,
                dedup_trim_below,
            } => self.apply_compact(*up_to_revision, *dedup_trim_below, effects),
            Command::RetireNode { node_id } => self.apply_retire_node(*node_id, effects),
        };

        match (stamp, response) {
            (
                Some(stamp),
                CommandResponse::Mutation {
                    response, event, ..
                },
            ) => {
                let recorded = self.dedup_store(&stamp, &response, effects);
                // Mirrored onto the client-visible response, not just the internal one: the
                // caller is the party that has to decide whether resubmitting is safe, and
                // `recorded` is false whenever the global cap refused the record (C5B-05).
                let mut response = response;
                response.dedup_recorded = recorded;
                CommandResponse::Mutation {
                    response,
                    event,
                    dedup_hit: false,
                    dedup_recorded: recorded,
                }
            }
            (_, response) => response,
        }
    }

    /// Whether `(principal, client_id, request_id)` is a hit, a miss, or a client bug.
    fn dedup_lookup(&self, stamp: &DedupStamp) -> DedupLookup {
        let key = dedup_index_key(stamp);
        if let Some(record) = self.dedup.get(&key) {
            return DedupLookup::Hit(record.clone());
        }
        // The comparison is against this pair's **oldest retained** id, not its newest (review
        // finding C5B-07, ADR-0025 note of 2026-09-19).
        //
        // A ceiling comparison -- "a request_id must exceed every id still retained" -- is
        // safe but unusable: it forbids gaps, and any client with more than one request in
        // flight produces gaps. Ids are minted in order and arrive out of order, so a client
        // that mints 100, 101, 102 and whose 102 lands first would have 100 and 101 refused as
        // non-monotonic. Concurrency is not an exotic case here; it is what a shared client is
        // for, so that rule would have restricted deduplication to serial callers.
        //
        // A floor comparison is exactly as safe, because eviction is strictly oldest-first:
        // what a pair retains is always the highest `window_requests` ids it ever applied. Any
        // id above the oldest retained one that is *not* retained was therefore never applied,
        // and admitting it cannot duplicate anything. At or below that floor the id may have
        // been evicted, its outcome is unknowable, and it still fails closed (M5-99).
        //
        // The floor only exists once the window is **full**, which is the half that makes the
        // rule usable rather than merely safe. Below capacity this pair has never evicted
        // anything, so *every* id it does not retain is an id it never applied -- including the
        // ones that arrive below an id already stored. A client with n requests in flight and a
        // window of at least n therefore never has a legitimate request refused, which is the
        // whole point: deduplication is for the caller that has several writes outstanding.
        //
        // The one gap in the proof is the global cap: an id that applied while the cap refused
        // its record is above the floor and not retained, so a resubmission applies twice. That
        // is the documented OQ-49 downgrade and the caller is told about it by name, through
        // `dedup_recorded = false` on the original response (C5B-05).
        let mut pair = self.dedup_pair_range(stamp);
        let Some((&(_, _, oldest), _)) = pair.next() else {
            return DedupLookup::Miss;
        };
        let retained = pair.count() + 1;
        if retained < self.limits.dedup.window_requests as usize || stamp.key.request_id > oldest {
            return DedupLookup::Miss;
        }
        DedupLookup::NotMonotonic { floor: oldest }
    }

    /// Every retained record for one `(principal, client_id)` pair, ascending by request id.
    fn dedup_pair_range(
        &self,
        stamp: &DedupStamp,
    ) -> impl DoubleEndedIterator<Item = (&DedupIndexKey, &DedupRecord)> {
        let low = (stamp.principal_hash, stamp.key.client_id, 0u64);
        let high = (stamp.principal_hash, stamp.key.client_id, u64::MAX);
        self.dedup.range(low..=high)
    }

    /// Retain `response` under `stamp`, evicting this pair's oldest id once the window is
    /// full. Returns whether a record was actually stored (OQ-49).
    fn dedup_store(
        &mut self,
        stamp: &DedupStamp,
        response: &MutationResponse,
        effects: &mut ApplyEffects,
    ) -> bool {
        // Fail closed on a *new* key rather than evicting somebody else's record: turning one
        // client's load into another client's duplicate application is the one outcome the
        // per-pair window exists to prevent (OQ-49). The mutation has already applied; only
        // the promise that a resubmission will be recognized is withheld.
        if self.dedup.len() as u64 >= self.limits.dedup.max_records {
            effects.dedup_cap_refusals += 1;
            self.dedup_cap_refusals += 1;
            tracing::warn!(
                client_id_hex = %hex16(&stamp.key.client_id),
                request_id = stamp.key.request_id,
                records = self.dedup.len() as u64,
                max_records = self.limits.dedup.max_records,
                reason = "global_cap",
                "dedup_not_recorded"
            );
            return false;
        }

        let key = dedup_index_key(stamp);
        let record = DedupRecord {
            response: response.clone(),
            applied_revision: self.cluster_revision,
        };
        self.dedup.insert(key, record.clone());
        effects.dedup_inserted = Some((key, record));

        let window = u64::from(self.limits.dedup.window_requests);
        while self.dedup_pair_range(stamp).count() as u64 > window {
            let Some((&oldest, _)) = self.dedup_pair_range(stamp).next() else {
                break;
            };
            self.dedup.remove(&oldest);
            effects.dedup_removed.push(oldest);
            effects.dedup_window_evictions += 1;
            self.dedup_window_evictions += 1;
        }
        tracing::debug!(
            client_id_hex = %hex16(&stamp.key.client_id),
            request_id = stamp.key.request_id,
            revision = response.revision,
            "dedup_stored"
        );
        true
    }

    /// Apply a [`Command::RetireNode`] (M5, ADR-0023).
    ///
    /// Idempotent and monotonic: there is no command that un-retires an id, because "may never
    /// rejoin" has to survive every later membership change and every node that was
    /// partitioned while the removal happened.
    fn apply_retire_node(
        &mut self,
        node_id: NodeId,
        effects: &mut ApplyEffects,
    ) -> CommandResponse {
        if self.retired_nodes.insert(node_id) {
            effects.retired_node = Some(node_id);
            tracing::info!(
                op = "retire_node",
                target = node_id.0,
                outcome = "applied",
                "node_retired"
            );
        } else {
            tracing::debug!(
                op = "retire_node",
                target = node_id.0,
                outcome = "noop",
                "node already retired"
            );
        }
        CommandResponse::Retired { node_id }
    }

    /// Apply a [`Command::Compact`] (M4, ADR-0019).
    ///
    /// Two rules, both of which exist so that a resume cursor's validity is a stable,
    /// deterministic function of the applied log:
    ///
    /// * **clamped** to `cluster_revision` — a watermark above applied state would report a
    ///   revision that has not happened yet as already compacted (test plan M4-26);
    /// * **monotonic** — a watermark at or below the current one is a no-op that still answers
    ///   [`CommandResponse::Compacted`] with the unchanged watermark (M4-25, lead ruling R1).
    ///   It is a no-op rather than an error because a re-proposed or hand-crafted `Compact`
    ///   must apply identically on every voter, and "error here, no-op there" is divergence.
    ///
    /// Records are untouched: compaction sheds *history*, never state. The journal deletion
    /// itself is the storage layer's half of the same applied batch (ADR-0019).
    fn apply_compact(
        &mut self,
        up_to_revision: u64,
        dedup_trim_below: Option<u64>,
        effects: &mut ApplyEffects,
    ) -> CommandResponse {
        // Unconditional, and before the monotonic short-circuit below: the journal watermark
        // and the dedup watermark are independent bounds (ADR-0025), so a re-proposed
        // `Compact` that does not advance history must still be able to advance the trim.
        if let Some(watermark) = dedup_trim_below {
            self.trim_dedup(watermark, effects);
        }
        let clamped = up_to_revision.min(self.cluster_revision);
        if clamped < up_to_revision {
            tracing::info!(
                op = "compact",
                requested = up_to_revision,
                up_to = clamped,
                revision = self.cluster_revision,
                "compaction_clamped"
            );
        }
        if clamped <= self.compact_revision {
            tracing::debug!(
                op = "compact",
                up_to = clamped,
                compact_revision = self.compact_revision,
                outcome = "noop",
                "compaction watermark did not advance"
            );
            return CommandResponse::Compacted {
                compact_revision: self.compact_revision,
            };
        }
        self.compact_revision = clamped;
        self.compactions += 1;
        tracing::debug!(
            op = "compact",
            up_to = clamped,
            compact_revision = self.compact_revision,
            revision = self.cluster_revision,
            outcome = "applied",
            "compaction_applied"
        );
        CommandResponse::Compacted {
            compact_revision: self.compact_revision,
        }
    }

    /// Drop every deduplication record whose `applied_revision` is strictly below
    /// `watermark` (M5, ADR-0025 "Window and monotonic rule"; test plan M5-100).
    ///
    /// Deterministic by construction: a predicate over a value already in the record,
    /// evaluated in one ordered walk, so every voter deletes exactly the same set.
    ///
    /// A trim that leaves a pair holding at least one record leaves that pair's monotonic
    /// ceiling intact, because the ceiling is read from whatever remains and the oldest ids
    /// go first. A trim that removes a pair's **last** record drops the ceiling with it: the
    /// pair is then unknown, and its next request id is evaluated as a first id rather than
    /// against a floor. That is the honest consequence of a bounded index - retention is what
    /// the window promise is made of, and past the watermark there is nothing left to compare
    /// against (ADR-0025 "bounded, not universal exactly-once").
    fn trim_dedup(&mut self, watermark: u64, effects: &mut ApplyEffects) {
        let doomed: Vec<DedupIndexKey> = self
            .dedup
            .iter()
            .filter(|(_, record)| record.applied_revision < watermark)
            .map(|(key, _)| *key)
            .collect();
        if doomed.is_empty() {
            return;
        }
        for key in &doomed {
            self.dedup.remove(key);
        }
        effects.dedup_trim_evictions += doomed.len() as u64;
        self.dedup_trim_evictions += doomed.len() as u64;
        tracing::debug!(
            op = "compact",
            dedup_trim_below = watermark,
            trimmed = doomed.len() as u64,
            retained = self.dedup.len() as u64,
            "dedup_trimmed"
        );
        effects.dedup_removed.extend(doomed);
    }

    fn apply_put(&mut self, key: &Bytes, value: &Bytes, expected: Option<u64>) -> CommandResponse {
        let current = self.records.get(key);
        // ADR-0006 rows 1-6, evaluated against the state immediately preceding this command.
        let conflict = match (expected, current) {
            (None, _) => None,
            (Some(0), None) => None,
            (Some(0), Some(rec)) => Some((true, rec.mod_revision)),
            (Some(n), Some(rec)) if rec.mod_revision == n => None,
            (Some(_), Some(rec)) => Some((true, rec.mod_revision)),
            // A conditional Put against an absent key conflicts with `exists = false`; it is
            // not a NotFound, because the caller asked to replace a specific revision.
            (Some(_), None) => Some((false, 0)),
        };

        if let Some((exists, current_mod_revision)) = conflict {
            return self.reject(
                "put",
                key,
                expected,
                MutationResponse::conflict(self.cluster_revision, exists, current_mod_revision),
            );
        }

        let revision = self.cluster_revision + 1;
        // `create_revision` survives updates but resets after a delete, because it names the
        // revision at which the key came into existence *this* time (ADR-0005).
        let create_revision = current.map_or(revision, |rec| rec.create_revision);
        let record = Record {
            key: key.clone(),
            value: value.clone(),
            create_revision,
            mod_revision: revision,
        };
        let event = MutationEvent::put(revision, &record);
        self.records.insert(key.clone(), record);
        self.cluster_revision = revision;

        tracing::debug!(
            op = "put",
            key_hex = %key_hex(key),
            outcome = "applied",
            revision,
            expected = ?expected,
            "applied put"
        );
        CommandResponse::Mutation {
            response: MutationResponse::applied_put(revision),
            event: Some(event),
            dedup_hit: false,
            dedup_recorded: false,
        }
    }

    fn apply_delete(&mut self, key: &Bytes, expected: Option<u64>) -> CommandResponse {
        // ADR-0006 rows 7-12. An absent key is NOT_FOUND whether or not a positive expected
        // revision was supplied — the easy-to-get-wrong row, and the one that makes
        // "exactly one of N competing deletes applies" come out right.
        let Some(current) = self.records.get(key) else {
            return self.reject(
                "delete",
                key,
                expected,
                MutationResponse::not_found(self.cluster_revision),
            );
        };

        if let Some(n) = expected {
            if current.mod_revision != n {
                let current_mod_revision = current.mod_revision;
                return self.reject(
                    "delete",
                    key,
                    expected,
                    MutationResponse::conflict(self.cluster_revision, true, current_mod_revision),
                );
            }
        }

        let revision = self.cluster_revision + 1;
        self.records.remove(key);
        self.cluster_revision = revision;

        tracing::debug!(
            op = "delete",
            key_hex = %key_hex(key),
            outcome = "applied",
            revision,
            expected = ?expected,
            "applied delete"
        );
        CommandResponse::Mutation {
            response: MutationResponse::applied_delete(revision),
            event: Some(MutationEvent::delete(revision, key.clone())),
            dedup_hit: false,
            dedup_recorded: false,
        }
    }

    /// Log and wrap a non-applying outcome. Allocates nothing and mutates nothing.
    fn reject(
        &self,
        op: &'static str,
        key: &Bytes,
        expected: Option<u64>,
        response: MutationResponse,
    ) -> CommandResponse {
        tracing::debug!(
            op,
            key_hex = %key_hex(key),
            outcome = ?response.outcome,
            revision = response.revision,
            expected = ?expected,
            current_mod_revision = response.current_mod_revision,
            "mutation did not change state"
        );
        CommandResponse::Mutation {
            response,
            event: None,
            dedup_hit: false,
            dedup_recorded: false,
        }
    }

    /// Stable 32-byte digest of the replicated state (TA-2, ADR-0007 Clarifications).
    ///
    /// SHA-256 over, in this exact order:
    ///
    /// 1. `cluster_revision` as `u64` LE;
    /// 2. the record count as `u64` LE;
    /// 3. each record in ascending key order as
    ///    `key len u32 LE | key | value len u32 LE | value | create_revision u64 LE |
    ///    mod_revision u64 LE`.
    ///
    /// Every variable-length field is length-prefixed so no two distinct states can collide
    /// by concatenation (`{"ab" -> ""}` and `{"a" -> "b"}` must not hash alike). Node-local
    /// data — `last_applied`, membership, [`Limits`] — is excluded on purpose, so two nodes
    /// that applied the same prefix hash identically even though their log metadata differs.
    ///
    /// This ships unconditionally rather than behind a test feature: M2 compares nodes with
    /// it after replay, and a `cfg`-gated hash would be a different code path from the one
    /// under test.
    pub fn state_hash(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.cluster_revision.to_le_bytes());
        hasher.update((self.records.len() as u64).to_le_bytes());
        for (key, record) in &self.records {
            hasher.update((key.len() as u32).to_le_bytes());
            hasher.update(key);
            hasher.update((record.value.len() as u32).to_le_bytes());
            hasher.update(&record.value);
            hasher.update(record.create_revision.to_le_bytes());
            hasher.update(record.mod_revision.to_le_bytes());
        }
        hasher.finalize().into()
    }
}

/// Render a key as lowercase hex, truncated to [`KEY_HEX_MAX_BYTES`] so a log line can never
/// carry a full large key (ADR-0013). Values are never rendered at all.
/// Render a 16-byte client id as lowercase hex. A `client_id` is caller-chosen opaque bytes,
/// never a key or a value, so it is logged in full (ADR-0025 verification rows, Q-24).
pub(crate) fn hex16(client_id: &[u8; 16]) -> String {
    key_hex(client_id)
}

pub(crate) fn key_hex(key: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(KEY_HEX_MAX_BYTES * 2);
    for byte in key.iter().take(KEY_HEX_MAX_BYTES) {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[config_log::retcd_test]
    fn key_hex_is_lowercase_and_capped() {
        assert_eq!(key_hex(b"\x00\xff\x0a"), "00ff0a");
        assert_eq!(key_hex(&[0xabu8; 64]).len(), 64);
    }
}
