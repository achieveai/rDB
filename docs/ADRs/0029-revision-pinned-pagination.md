# ADR-0029: Revision-pinned pagination

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §10.2, §16, §19.12, §21 M6

## Context

ADR-0016 has always reported `Pagination::Unsupported`: `List` truncates and a caller narrows its
own prefix to work around a large result set, with no continuation token and no guarantee that two
calls observe one consistent snapshot. §10.2 and D6.3 require a real continuation mechanism for M6
— a token that lets a caller walk a large keyspace across many RPCs while seeing exactly the state
as of one revision, without holding a transaction open or blocking compaction. This ADR is the
owning decision for D6.3 and for the lead ruling that reconciles the token payload contradiction
raised in test-plan-m6 §15 (M6-R1).

## Decision

### Pinning: a bounded LRU of snapshot handles on the leader

- The leader keeps a bounded LRU of RocksDB snapshot handles (`list.max_pinned_snapshots`, default
  64) keyed by `(revision, policy_version)` — two callers continuing the same revision under the
  same policy version share one pinned snapshot rather than each holding a duplicate. Each entry
  carries a TTL (`list.ttl_seconds`, default 60 s) measured against the injectable clock; a pin
  that outlives its TTL, or that is evicted because the LRU is full, is simply dropped — RocksDB
  snapshots are cheap to release and expensive to hold indefinitely, so the bound protects the
  storage engine, not the client.
- The ephemeral (in-memory) store achieves the same isolation by cloning the relevant `BTreeMap`
  range behind an `Arc` at pin time — parity with the RocksDB-backed path is a listed test row
  (M6-65..68 exercise both backends identically), not an assumption.
- A pin is process-local. It does not survive a restart and does not replicate — a continuation
  token issued by a node that then restarts or loses leadership simply expires
  (`PageTokenExpired{reason="node"}`) on the next page request, and the caller is expected to
  re-`List` from scratch. This is consistent with revisions themselves being a Raft log position:
  a pin is a local cache of committed state, never a source of truth.
- A pinned snapshot **never blocks Raft apply or the replicated compaction path** (§19.12): holding
  N snapshots pins the RocksDB compaction horizon at the oldest pinned revision, exactly as
  ADR-0022's manual-snapshot pinning already does, and is bounded by the same LRU/TTL limits so a
  slow or abandoned pagination walker cannot pin storage growth unboundedly.

### The token: reconciled to the full binding per M6-R1

- The original D6.3 draft under-specified the token payload — test-plan-m6 §15 flagged this as
  contradiction #12, since §10.2's own list of what a continuation token must bind is broader than
  what D6.3 described. **M6-R1 resolves this in favor of the fuller §10.2 binding**, and this ADR
  is written to that resolution, not to the original draft:

  ```text
  PageToken {
      token_version: u8,        // = 1
      prefix_hash:   [u8; 32],  // sha256 of the List call's prefix argument
      principal_hash:[u8; 32],  // sha256 of the authenticated principal that requested the page
      revision:      u64,
      last_key:      Vec<u8>,
      policy_version: Option<u64>,
      issued_ms:     u64,       // injectable clock, not wall time read ad hoc
      node_id:       NodeId,    // the node whose LRU holds the pinned snapshot
  }
  ```

  serialized with the versioned command envelope's own postcard convention (ADR-0007) and
  authenticated with an appended `HMAC-SHA256(list.token_key, bytes)` — the server never trusts an
  unauthenticated token field, including `revision` and `last_key`, which a naive design might
  otherwise treat as self-verifying because they are checked against the pin table anyway.
  `token_key` is process/cluster configuration, not derived from TLS or gossip material, and is
  redacted with the same fingerprint-only discipline as ADR-0028's key material.

### Distinct failure modes: expired vs. mismatched are not the same error

- **`InvalidArgument{prefix_mismatch}`** (or `PermissionDenied{token_principal}` for the principal
  case) when a continuation call's prefix or authenticated principal does not match the token's
  bound `prefix_hash`/`principal_hash`. This is the specific resolution the test-plan review asked
  for (OQ-62): a caller who mutates the prefix argument between pages, or whose credentials
  changed, made a **caller error**, not a transient server-side expiry, and the two must be
  distinguishable so a client's retry logic does not loop on a token that will never succeed.
- **`PageTokenExpired`** is reserved for the five transient causes, each surfaced with a
  `reason` field so an operator (and a test assertion) can tell them apart: `mac` (HMAC
  verification failed — corrupt or forged token), `expired` (past TTL), `evicted` (LRU pressure
  dropped the pin before TTL), `node` (issuing node unreachable, restarted, or no longer leader),
  and `policy_version` (the active policy changed since the token was issued; see ADR-0027). A
  client's correct response to `PageTokenExpired` is to re-`List` from the start; its correct
  response to `InvalidArgument`/`PermissionDenied` is to fix the call, not retry it.
- The mismatch check runs **before any key is read** — a forged or mutated token cannot be used to
  probe whether a key exists under a prefix the caller is not authorized to see, since the
  authorization-relevant check (prefix and principal binding) is the first thing evaluated.

### Behavior without a token, and capability reporting

- A `List` call without a continuation token is unchanged from M3: no pin is created, no HMAC
  overhead, no LRU entry — existing non-paginating callers pay nothing for this feature's
  existence.
- `Pagination` (ADR-0016) replaces `Unsupported` with `RevisionPinned { max_pinned: u32, ttl_ms:
  u64 }`, reporting the effective configured bounds directly rather than a bare boolean — this is a
  breaking change to an existing public enum, the same class as `WatchResumption::Retained` at M4
  and `Authz::SignedPolicy` in ADR-0027; recorded once, here and in the dated note appended to
  ADR-0016, rather than three times.
- The token's `last_key` field is, by construction, a key name the requesting client has already
  observed (it is the last key of the prior page) — it is not a new information disclosure to that
  client. It is nonetheless treated as sensitive data at rest for whatever system stores or logs
  the token on the client's behalf (a load balancer access log, a client-side cache), and the
  operator runbook accompanying this feature says so explicitly, since a key name can itself be
  sensitive depending on deployment.

## Consequences

- The bounded LRU means heavy concurrent pagination against the same node can evict a slow
  walker's pin before it finishes, surfaced as a typed, expected
  `PageTokenExpired{reason="evicted"}` rather than silent data loss — accepted as the cost of
  bounding storage-engine pinning, and documented as an operator-visible metric
  (`retcd_pagination_pins_active`, `retcd_pagination_evictions_total`) so a real workload that
  needs a larger bound can be tuned via `list.max_pinned_snapshots` rather than discovered as a
  mystery failure.
- A pin being strictly process-local means pagination does not survive a leadership change; a
  caller mid-walk during a failover must restart its `List` from the beginning. This trades
  simplicity (no replicated pin state, no additional Raft traffic) for a worse experience during
  the specific window of a leadership change, judged acceptable since failovers are rare relative
  to pagination walks.
- Splitting `InvalidArgument`/`PermissionDenied` from `PageTokenExpired` adds two more error
  variants a client must handle correctly; this is intentional friction in exchange for retry logic
  that cannot loop forever on a caller-side bug.

## Verification

- M6 rows for: LRU pin creation/reuse/eviction and RocksDB/ephemeral parity (M6-65..68); TTL
  expiry against the injectable clock (M6-69..70); token HMAC verification, tamper detection, and
  version-field rejection of an unknown `token_version` (M6-71..74); prefix/principal mismatch as
  `InvalidArgument`/`PermissionDenied` distinct from `PageTokenExpired`, evaluated before any key
  read (M6-75..78); each of the five `PageTokenExpired` reasons individually (M6-79..82); no-token
  `List` byte-identical to M3 and capability reporting (M6-83..84).
- Test plan: `docs/testing/test-plan-m6.md` §5 (M6-65..M6-84); E2E-44.

## Notes

## Implementation note (2026-09-19, ruling M6-R7)

`PageTokenExpired.reason` has six values, not five: `token_version` was added alongside `mac`,
`ttl`, `evicted`, `node` and `policy_version`. A token whose leading version byte is not the
current `PageToken::token_version` is refused *before* the HMAC is checked, so a client that
survives a token-format change gets a named reason instead of a misleading `mac`. Test plan
row M6-78 asserts it. `ListPage` also carries `truncated: bool`, and the request type is
`PageRequest` rather than a second `ListRequest`; neither changes the wire contract above.
