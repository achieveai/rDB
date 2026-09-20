# dev-dedup (M5 wave 2) — research, decisions, ledger

Authority read: ADR-0025, ADR-0026, ADR-0023/0007/0015/0019/0022, architecture-m4-m6 D5.4/D5.5 +
amendments + M5-R1..R11, m5-interfaces.md, test-plan-m5 §6/§7 (M5-95..M5-126, E2E-35/E2E-38),
OQ-48/OQ-49/OQ-51. Landed code inspected: config-core (command.rs/state.rs/limits.rs/
capabilities.rs/error.rs), config-storage (rocks.rs open+apply+export, snapshot.rs, reader.rs),
config-engine (metrics.rs = observable-state types, direct.rs, node.rs), config-server
(health.rs, config.rs), config-grpc (convert.rs, client_plane.rs), config-client.

## Contradiction C-D1 (ADR-0025 vs replicated determinism) — resolved, escalated in handoff

ADR-0025: "`principal` is bound by the **leader** ... at propose time — never read from the
message itself" and "`DedupKey` is exactly 24 bytes when present".
m5-interfaces: "The PRINCIPAL is not in the envelope: the leader binds it when it evaluates the
command (`apply_with_principal`)".

A follower applies the *same* replicated entry and must reach the *same* dedup decision. It has no
authenticated session for that entry. So the bound principal MUST travel inside the log entry, or
apply is non-deterministic and the cluster diverges — the exact failure the envelope exists to
prevent. "Never from the wire" is satisfied by the leader **overwriting** the field at propose time
from the authenticated session, never copying a client-supplied value.

Resolution (implemented):
* `DedupKey { client_id: [u8;16], request_id: u64 }` — exactly 24 bytes, the *client-facing* type
  (proto message, `ConfigStore` surface). ADR wording preserved.
* `DedupStamp { principal_hash: [u8;32], key: DedupKey }` — the leader-bound form that rides the
  envelope. `Put`/`Delete` carry `dedup: Option<DedupStamp>`.
* `KvState::apply(cmd)` uses the stamp already in the command (follower/replay path).
* `KvState::apply_with_principal(cmd, principal_hash)` re-stamps then applies — the leader-side
  bind seam and the M5-101 oracle.
Deviation from m5-interfaces' literal `dedup: Option<DedupKey>`; reported to the lead.

## Other decisions

* **D-D2 `DedupRecord`** = `{ response: MutationResponse, applied_revision: u64 }` with
  `outcome()`/`revision()` accessors. ADR-0025 names `{outcome, revision, applied_revision}`;
  m5-interfaces names `{outcome, revision, conflict}`. Storing the whole `MutationResponse` is the
  superset of both and is what makes "a hit returns the stored response" exact (a `Conflict` hit
  must replay `exists`/`current_mod_revision` too). `applied_revision` is load-bearing: it is the
  watermark `Compact{dedup_trim_below}` trims against.
* **D-D3 dedup state lives in `KvState`** (`BTreeMap<DedupIndexKey, DedupRecord>`), mirrored to CF
  `dedup`. `KvState` already holds every record in memory, so this is the existing shape, and the
  window/monotonic rule must be decided inside pure `apply`.
* **D-D4 `KvState::state_hash` is NOT extended.** It stays records-only, so every M0–M4 golden hash
  holds. Journal/dedup equality is asserted by the testkit's own digest (TA-50, tester-m4's file).
* **D-D5 FORMAT_VERSION 2 -> 3** with a v2 -> v3 migration that creates CF `dedup`
  (`create_missing_column_families` already does the creation; `CfLayout::LegacyV2` is what makes
  the pre-open probe refuse a v3 directory someone deleted `dedup` out of). ADR-0025 asked for the
  bump explicitly.
* **D-D6 `Dedup::Unsupported` is kept** (not renamed to `Dedup::None` as test-plan M5-105 words it):
  it is referenced by 10 files owned by other developers. `Dedup::Bounded { window_requests }` is
  added alongside, per ADR-0025 and the ownership note.
* **D-D7 OQ-49 fail-closed at the global cap** => `CommandResponse::Mutation` carries
  `dedup_recorded: bool` next to `dedup_hit: bool`. At `max_records` the mutation applies normally,
  stores no record, and says so.
* **D-D8 `[metrics]`/`[dedup]` config sections** land in one hunk at the end of
  `config-server/src/config.rs` inside the first hour; config.rs then belongs to dev-admin.

## Ledger

| # | Task | State |
|---|---|---|
| 1 | config-core command.rs / state.rs / limits.rs / capabilities.rs / error.rs | DONE |
| 2 | config-storage retired_nodes + dedup CF + trim + reader | DONE |
| 3 | gate `cargo test -p config-core -p config-storage`; message lead | DONE |
| 4 | config-engine with_dedup + principal binding + metrics facade | DONE |
| 5 | config-client / proto / config-grpc dedup fields | DONE |
| 6 | config-server `/metrics`, `[metrics]`, `[dedup]` | DONE |
| 7 | runbooks + alerts.md + test-plan row names | DONE |
| 8 | acceptance gates + mutation checks | DONE |

---

## Addendum (2026-09-18): metrics facade, `/metrics`, runbooks — what actually landed

### Decisions taken during implementation

* **D-D9 The exporter is hand-rolled, not the `metrics` crate.** ADR-0026's Decision names
  `metrics` + `metrics-exporter-prometheus`. What landed renders Prometheus text directly from a
  `MetricsReport` gathered by `ConfigNode::metrics_report()`. Reason: nearly every series is
  already a value rEtcd holds (`RaftMetrics`, `WatchStats`, `DedupStats`, `KvState` counters,
  `StorageMetrics`); a facade recorder would have meant a second, write-only copy of each that can
  drift from the state machine's own number. Recorded as an ADR-0026 implementation note.
* **D-D10 Counters live in `KvState`, surfaced through *defaulted* `StateReader` methods.** This
  is what let the whole metrics surface land without touching the frozen `rocks.rs`, and it means
  the Rocks and ephemeral stores report identical numbers by construction.
* **D-D11 Unfilled series are omitted, never zeroed.** `retcd_rocks_disk_free_bytes`,
  `retcd_cert_expiry_seconds`, `retcd_backup_age_seconds` need a syscall, X.509 parsing and the
  backup command's bookkeeping respectively. `docs/runbooks/alerts.md` has a "cannot fire yet"
  section naming the substitute monitoring for each.
* **D-D12 `/metrics` shares the health listener**, gated by `[metrics] enabled` (default true). A
  disabled endpoint **404s** rather than answering emptily, so a scraper distinguishes "not
  exported here" from "exported and idle". `health::serve` gained a fourth parameter rather than a
  second entry point (a `pub` wrapper in a binary crate is dead code).

### Defect found and fixed

`run::capabilities_without_opening` pinned `dedup: Dedup::Unsupported` unconditionally, so
`--capabilities` disagreed with the started node as soon as `[dedup]` was enabled — which would
have broken E2E-02's "the two reports agree" invariant. Now reads `cfg.dedup`. Covered by
`m5_105_capability_report_follows_the_dedup_section`.

### Edits made inside dev-admin's `crates/config-server/src/run.rs` (4 hunks, all one-liners)

1. `fn limits`: `limits.dedup = cfg.dedup;`
2. beside `node_cfg.limits.watch`: `node_cfg.limits.dedup = cfg.dedup;`
3. `capabilities_without_opening`: the dedup arm above
4. the `health::serve` call site: fourth argument `cfg.metrics_enabled`

### Open escalations to the lead

| # | Item | Why it needs a ruling |
|---|---|---|
| E1 | Test plan §7.2 uses a **different metric vocabulary** from ADR-0026's table and from the exporter (`retcd_raft_is_leader` vs `retcd_raft_leader`, `retcd_rocks_memory_bytes{kind}` vs `retcd_rocks_mem_bytes`, `retcd_snapshot_size_bytes` vs `retcd_snapshot_bytes`, `retcd_watch_lag_revisions`, `retcd_vote_sync_latency_seconds`, `retcd_storage_sync_errors_total`, `retcd_raft_voters/learners/joint_membership`, `retcd_raft_quorum_ack_ms`, `retcd_raft_running_state`, …). M5-110/111/113/114 cannot be written against both. Not written; flagged in the test file's module doc |
| E2 | ADR-0026 mandates the `metrics` crate facade; D-D9 deviates. ADR note appended, lead sign-off needed |
| E3 | TA-50 requires `state_hash` to cover `events` + `dedup`; lead ruling R1 / D-D4 scoped it to records only |
| E4 | M5-105 words the off state as `Dedup::None`; code keeps `Dedup::Unsupported` (D-D6). Test asserts `Unsupported` |
| E5 | Patch note for the frozen `rocks.rs`: cumulative snapshot-build duration (currently a last-build gauge), per-CF RocksDB properties (`cf` is pinned to `"all"`), an open-files gauge, and a write-stall counter. All four are ADR-0026 rows that cannot be exported without it |
| E6 | The 4 one-line hunks in dev-admin's `run.rs`, above |

### Evidence

* `cargo test -p config-server --test m5_observability` — 8/8 pass, including the full dedup wire
  path (proto → convert → leader bind → apply → `dedup` CF → exported counters) and the
  no-key/no-value label scan.
* A 150-line `/metrics` sample from a formed single voter is at
  `scratchpad/metrics-sample.txt`.

---

## Addendum 3 — ruling M5-R17 items 1–9 closed (2026-09-18)

### Encoding (item 1)

Variable-width dedup group landed. `PUT_OVERHEAD` 25, `DELETE_OVERHEAD` 21,
`COMPACT_BASE_LEN` 16, `RETIRE_NODE_LEN` 15. `NonCanonicalDedup`/`NonCanonicalTrim` retained but
unreachable by construction; `m5_74` now asserts truncation and `TrailingBytes` instead.
ADR-0025 carries the dated ruling note. `m4_12` passes unmodified — the evidence the reading is
right.

### Observability rows (item 3)

M5-110/111/113/114 written in `crates/config-server/tests/m5_observability.rs` against
**ADR-0026 spellings**. E1 is closed by the ruling.

* `m5_110_required_metric_names_are_present` — 3-node daemon cluster, mixed workload. Parses
  ADR-0026's table out of the ADR file and asserts a **set equality** between the declared
  families and `(table − NOT_EXPORTED) ∪ EXTRA_EXPORTED`, plus type and label agreement.
  `NOT_EXPORTED` (10) and `EXTRA_EXPORTED` (8) mirror ADR-0026's implementation note items 2/3,
  so exporting a documented-absent series now fails the row until the note is corrected too.
* `m5_111_metric_values_change_under_load` — single voter, 200 writes + 3 dedup hits + 1 authz
  denial; asserts deltas and that no counter/histogram sample falls between scrapes.
  **Gotcha:** `retcd_proposal_latency_seconds_count` must be matched with `op="put"` — the
  `OpLatencies` snapshot emits every known op, so the first `_count` sample is `delete` and
  reads zero all run.
* `m5_113_metric_cardinality_is_bounded` — baseline after 1 key + 1 watch, then 200 keys + 20
  streams; sample count must be **identical** and under a checked-in ceiling of 300.
* `m5_114_derived_raft_metrics_match_their_sources` — leader `peer_lag` vs each follower's own
  `/health`, plus applied index / cluster revision / leader / denial counter per node against
  that node's health payload. Followers must export no `peer_lag` sample at all.

Plan §7.2 → ADR-0026 renames (the lead is amending the plan):
`retcd_raft_is_leader`→`retcd_raft_leader`, `retcd_raft_state`→`retcd_raft_role`,
`retcd_rocks_memory_bytes`→`retcd_rocks_mem_bytes`,
`retcd_rocks_compaction_pending_bytes`→`retcd_rocks_compaction_pending`,
`retcd_rocks_write_stall_seconds_total`→`retcd_rocks_write_stalls_total`,
`retcd_snapshot_size_bytes`→`retcd_snapshot_bytes`,
`retcd_watch_lag_revisions`→`retcd_watch_lag`,
`retcd_gossip_reachable_peers`→`retcd_gossip_reachable`,
`retcd_gossip_endpoint_mismatch`→`retcd_gossip_endpoint_mismatch_total`,
`retcd_authn_failures_total{reason}`→`retcd_authn_rejected_total{plane}`,
`retcd_authz_denied_total{decision}`→ same name, label `plane`.
Plan-only names with no ADR row and no exporter series: `retcd_raft_voters`, `_learners`,
`_joint_membership`, `_membership_log_index`, `_snapshot_index`, `_peer_matched_index`,
`_quorum_ack_ms`, `_unpurged`, `_unsnapshotted`, `_running_state`,
`retcd_vote_sync_latency_seconds`, `retcd_log_sync_latency_seconds`,
`retcd_storage_sync_errors_total`, `retcd_rocks_corruption_total`,
`retcd_snapshot_failures_total`, `retcd_gossip_probe_latency_seconds`,
`retcd_gossip_queue_depth`, `retcd_gossip_drops_total`,
`retcd_backup_verifications_total`, `retcd_restore_drill_age_seconds`.

### The six reconstructed M0 config-core test files

Compared against `HEAD` (read-only `git show`): **every file's `fn m0_*` name set is identical
to HEAD**, and the only line differences are M4/M5-motivated (v1→v2 goldens, the 24→25 Put
overhead, import lists). Nothing was lost.

| File | HEAD | now | tests |
|---|---|---|---|
| `m0_cas_table.rs` | 443 | 445 | 15 = 15 |
| `m0_command.rs` | 338 | 355 | 12 = 12 |
| `m0_contracts.rs` | 467 | 479 | 3 = 3 |
| `m0_limits.rs` | 189 | 195 | 8 = 8 |
| `m0_purity.rs` | 254 | 259 | 4 = 4 |
| `m0_replay.rs` | 318 | 320 | 10 = 10 |

### Formatting

`cargo fmt -- --check` is clean across the repo except `crates/config-testkit/src/evidence.rs`
and `crates/config-testkit/tests/m4_watch_cluster.rs`. Both are re-scoped to me but a **tester
agent is actively writing config-testkit right now**, so they are left alone rather than
reformatted under a concurrent writer.

---

## Addendum 4 — the critic-m5b fix round (2026-09-19)

### Blockers

**C5B-01 — live install lost the dedup index and the retired set.** Closed earlier this session.
`InstalledState` gained `dedup: BTreeMap<DedupIndexKey, DedupRecord>`; a streaming arm decodes
`CF_DEDUP`; the live install calls `kv.restore_dedup(...)` and re-reads `state_meta/retired_nodes`
through the same `read_meta` helper `load_state` uses. The sharp half was the retired set: a
snapshot deliberately excludes `state_meta` (`NON_DATA_CFS`) and the install does not clear it, so
the bug was never "the snapshot lacked it" — it was "the install threw away state the node still
had on disk", which an unrelated restart then silently healed. Row
`m5_103_install_restores_the_dedup_index_and_retired_set`.

**C5B-02 — `dedup_trim_below` never proposed.** Delivered as a patch, node.rs untouched:
`$SCRATCH/patch/c5b02-node-dedup-trim.patch`, `git apply --check` clean. `up_to + 1` rather than
`oldest_within_max_age`, because under `max_age = 0` the latter evaluates to the newest applied
revision and would trim records the instant they were written.

**C5B-03 / ruling M5-R19 — undrained legacy log.** Done.
- `StorageOpenError::UpgradeRequiresDrainedLog { format, log_entries, path }` (rocks.rs:305).
- `count_log_entries` / `probe_log_entries` / `refuse_if_undrained` (rocks.rs:1540/1562/1576).
- Two call sites: the **read-only** one in the legacy-layout prologue (rocks.rs:862) and a
  post-open backstop (rocks.rs:902).
- The read-only placement is the part that took a second pass. My first version checked after
  `open_db`, and the test caught it: `create_missing_column_families` had already created
  `dedup`, so the refused directory could no longer be opened by the old build — the refusal
  would have stranded the data it exists to protect. Checking before the writable open is the
  whole fix; the backstop only covers a current CF set with a legacy marker.
- Applied to any `FormatAction::Migrate`, not just v2: a v1 log is undecodable for strictly more
  reasons, so exempting it would be an accident.
- Row `m5_127_v2_directory_with_an_undrained_log_is_refused_by_name`, which hand-builds a genuine
  M4 entry (mirror types `M4Command`/`M4Payload`/`M4Entry`, real `LogId` and `Membership` since
  those did not move) and asserts it does **not** decode under the current `Entry<TypeConfig>`.
  Mutation evidence for the premise: with the refusal disabled the open fails with
  `Corrupt { what: "raft_log entry 1", detail: "Hit the end of buffer, expected more data" }`.
- ADR-0021 note 4; test plan §6.1 rows M5-127/M5-128; the pre-existing migration row's comment
  corrected (it never covered a non-empty log and read as though it did).

### Materials

**C5B-04 — truthful eviction vs cap-refusal counters.** The exporter fed
`retcd_dedup_evictions_total{reason="global_cap"}` from the *trim* counter, so every compaction
looked like cap pressure on a cluster that had never reached the cap; and the cap's real event —
a refusal to record — was exported nowhere. Now `reason ∈ {window, trim}` plus a new
`retcd_dedup_cap_refusals_total`. `KvState.dedup_cap_refusals` + accessor, `DedupStats.cap_refusals`
(and `cap_evictions` renamed `trim_evictions`), exporter, ADR-0026 table + note, alerts.md rows.

**C5B-05 — `dedup_recorded` on the wire.** `MutationResponse` gained the field (config-core
types.rs), proto field 6, both convert directions, set in `state.rs` on both the hit path and the
store path. Deliberately set *after* `dedup_store`, so the retained record does not bake in a flag
about one specific submission — same reasoning as `dedup_hit`.

**C5B-06 — validate.rs measures the real encoded command.** Already correct after M5-R17
(`From<&PutRequest>` stamps a zero principal hash, so the 56-byte group is measured); what was
missing was the gate. Row `m5_129_request_size_is_measured_on_the_stamped_command`. Mutation
evidence: setting `dedup: None` in the `From` impl fails the row on "attaching a dedup key must
not buy 56 bytes of headroom".

**C5B-07 — concurrent ids / in-window gap admission.** The monotonic rule compared against the
retained **ceiling**, which forbids gaps; `GrpcClient` mints from one `fetch_add` and is `Clone`,
so any concurrent caller produces gaps and had its second-landing request refused.

The rule now has two halves, and I only had one of them right on the first attempt.

1. **Compare against the oldest retained id, not the newest.** Safe because eviction is strictly
   oldest-first, so what a pair retains is always the highest `window_requests` ids it ever
   applied: "not retained AND above the floor" proves the id was never applied.
2. **The floor only exists once the pair's window is full.** This is the half I missed, and
   `m5_131_out_of_order_ids_are_admitted_inside_the_window` failed on it immediately —
   `id 100 gave Rejected { reason: "request_id_not_monotonic: request_id 100 is not above the
   retained floor 102 ..." }`. With only 102 stored, the floor *was* 102, so the very first
   out-of-order pair was still refused and the fix bought nothing. A window below capacity has
   evicted nothing, so every id it does not retain is an id it never applied. Sizing guidance
   follows directly and is now in ADR-0025 and the runbook: `window_requests` must be at least
   the client's maximum concurrency.

Worth recording that the row caught my own fix rather than the original defect. I had written the
test from the finding's description before implementing, which is the only reason it had the
teeth to do that.

The second half of M5-131 was then sharpened so it separates floor from ceiling *behaviourally*
instead of by error text: land 300 into the full window so it holds a genuine gap
(`{201..=207, 300}`), then submit 250. It is above the floor and unretained, so it was never
applied and must apply; a ceiling rule sees `250 <= 300` and refuses a request nobody has sent.
Mutation evidence: `pair.next()` -> `pair.next_back()` fails the row.

**Client half.** No code change was needed. `DedupSession::mint` already uses a single
`fetch_add`, so concurrent clones mint distinct ids, and both retry paths reuse the *same* `wire`
(and therefore the same `request_id`) rather than minting a second one. What was wrong was only
the server's admission rule and the missing sizing guidance, both now fixed. Rows M5-104/M5-106
are testkit-owned and unchanged by this.

**C5B-08 — dedup runbook.** `docs/runbooks/dedup.md`, new. Cap refusals as the correctness alert,
window evictions as the capacity one, the client-visible refusal table, recovery after a snapshot
install, and the M5-R19 drain-then-upgrade procedure. alerts.md's two dedup rows repointed at it
(they pointed at watch-overload.md) and rewritten against the corrected counters.

**C5B-15 — `authz_denied` plane label.** Reassigned to me mid-round when dev-admin handed off
metrics.rs. Cannot be applied: the admin plane refuses non-admins in the transport
(`admin_plane.rs`, allowlist) and never touches a counter, so
`retcd_authz_denied_total{plane="admin"}` reads zero while the label is declared. The fix needs a
`record_authz_denial` default method on `AdminBackend`, a forward in run.rs, and an
`authz_denied_admin` counter on `ConfigNode` — and `NodeMetrics` is a struct literal in node.rs,
so even the metrics.rs half will not compile alone. Delivered whole as
`$SCRATCH/patch/c5b15-authz-denied-plane.patch` (4 files, `git apply --check` clean, **not**
compile-verified because node.rs is not mine).

### Files touched outside the granted list

Flagged because the grant did not name them and each was forced by an assigned finding:
`crates/config-core/src/types.rs` (C5B-05 — the coordinator's "PutResponse/DeleteResponse" is this
type), `crates/config-storage/src/reader.rs` (`DedupStats`, C5B-04),
`crates/config-engine/src/metrics.rs` (C5B-04 — granted mid-round),
`docs/ADRs/0026-metrics-and-runbooks.md` and `docs/runbooks/alerts.md` (C5B-04's rename makes the
old alert rows name a series that no longer exists).

### Traps worth keeping

- **node.rs is CRLF; everything else I touched is LF.** A `\Q...\E` anchor built from an LF
  heredoc silently matches zero times. The patch generator normalizes on read and restores the
  file's own ending on write.
- Generate a patch for a file you do not own by mirroring it into the scratchpad and
  `diff -u --label a/... --label b/...` — never by editing the tree and reverting, which races
  whichever agent owns it.
- `cat -A` and `sed` in Git Bash can hide `\r`; `perl ... unpack("C*", ...)` is the honest probe.

### Mutation log (coordinator process rule, adopted 2026-09-19 01:10)

Every entry below is a deliberate temporary edit under `crates/*/src` made to prove a test has
teeth, then reverted. All windows are **closed**; the rule arrived after these ran, so the
timestamps are the runs' own, recorded retroactively and marked as such.

| # | File:line | Mutation | Target row | Observed | State |
|---|---|---|---|---|---|
| 1 | `crates/config-storage/src/rocks.rs` (the `refuse_if_undrained` call sites) | refusal removed | `m5_127_v2_directory_with_an_undrained_log_is_refused_by_name` | open fails `Corrupt { what: "raft_log entry 1", detail: "Hit the end of buffer, expected more data" }` — the ruling's premise, proved not assumed | MUTATION CLOSED (retroactive entry) |
| 2 | `crates/config-storage/src/rocks.rs` (same) | refusal moved back after `open_db` | same row | the refused directory came back with a `dedup` CF — the refusal had stranded it. This one was not a check, it was my first implementation, and the row caught it | MUTATION CLOSED (retroactive entry) |
| 3 | `crates/config-core/src/command.rs` (`From<&PutRequest>`) | `dedup: None` | `m5_129_request_size_is_measured_on_the_stamped_command` | fails on "attaching a dedup key must not buy 56 bytes of headroom" | MUTATION CLOSED (retroactive entry) |
| 4 | `crates/config-core/src/state.rs:700` | `pair.next()` -> `pair.next_back()` (floor -> ceiling) | `m5_131_out_of_order_ids_are_admitted_inside_the_window` | FAILED at `m5_core.rs:736`, the floor-naming assertion | MUTATION CLOSED 2026-09-19 ~01:02 |
| 5 | `crates/config-core/src/state.rs:736` | `self.dedup_cap_refusals += 1` -> `self.dedup_window_evictions += 1` | `m5_100_global_cap_refuses_new_records_and_compact_trims` | FAILED `left: 0, right: 1` at `m5_core.rs:373` | MUTATION CLOSED 2026-09-19 ~01:05 |
| 6 | `crates/config-core/src/state.rs:654` | `response.dedup_recorded = recorded` -> `= true` | `m5_100_...` | **first run passed** — see below | MUTATION CLOSED 2026-09-19 ~01:06 |
| 7 | `crates/config-core/src/state.rs:654` | same, after adding coverage | `m5_100_...` | FAILED "least of all on the response the client reads and resubmits on" at `m5_core.rs:379` | MUTATION CLOSED 2026-09-19 ~01:12 |

**Entry 6 is the one worth keeping.** The mutation passed, which meant the test was asserting the
wrong copy of the flag. `CommandResponse::Mutation` carries `dedup_recorded` in the envelope *and*
inside the nested `MutationResponse`; only the nested one crosses the wire and is what a client
resubmits on, and nothing asserted it. M5-100 now checks both copies on the recorded path and on
the cap-refused path, and entry 7 is the same mutation failing against the repaired row.

Process note for anyone repeating this: `perl -i -pe` is line-based, so a restore pattern that
spans two lines silently does not apply and leaves the mutation in the tree. Two of the restores
above needed `if $. == <line>` instead. Always print the line back after restoring.

**Sweep at handoff (2026-09-19 01:2x).** `grep -rni mutat crates/*/src` returns 328 lines. Every
one is domain vocabulary, not a leftover: `mutation` (97), `MutationResponse` (61),
`MutationOutcome` (52), `MutationEvent` (45), `MutationEventKind` (21), `Mutation` (20),
`mutate`/`mutations`/`is_mutation`/`mutated`/`to_mutation_result`/`mutating`/`mutate_inner`/
`refused_mutations`/`mutates` (the rest). The single uppercase `MUTATION` match is
`MUTATION_OUTCOME_UNSPECIFIED` in a `config-grpc/src/convert.rs` doc comment. Filtering those
328 lines for `todo|xxx|hack|revert|temporar|disabled|fixme` returns nothing.

The three mutated lines were confirmed back at their intended values by printing them after
restore; `rustfmt` then shifted the cap-refusal line from 736 to 734.
