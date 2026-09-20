# critic-m6 — independent review of M6 (uncommitted work on top of `e54c6ef`)

Reviewer: critic-m6 (read-only). Date: 2026-09-19. Branch `feature/m4-m6`.
Baseline: `git diff e54c6ef` (80 files, +9103/-1535) plus 23 untracked paths.
Excluded by assignment: `crates/config-server/tests/e2e_daemon.rs` (tester-m6d in flight).

## Verdict

**PASS_WITH_RISKS — conditional on BLOCKER-1 being closed or explicitly accepted by the lead.**

Counts: **1 BLOCKER, 3 MATERIAL, 5 ADVISORY.**

The M6 body of work is unusually careful. Compile-before-swap in the TLS rotator, the
closed `AuthnRejectReason` label set, the version-bound policy signature envelope, and the
M6-R20 drain predicate are all correctly built and correctly argued. The one blocker is a
seam between two correct pieces: the `--compat-schema` ceiling reuses `format_version 1`
as its marker, and the pre-existing **v1 migration** path treats that marker as proof of a
pre-journal history.

---

## BLOCKER-1 — the documented rolling upgrade silently sets `compact_revision` to the node's full revision

**Criterion.** ADR-0030 / M6-95 / E2E-42: a node started with `--compat-schema 1` must be
restartable without the flag and come back as an ordinary node. Spec §10 / OQ-27: a watch
resume cursor `R` is refused only when `compact_revision > 0 && R <= compact_revision`.

**Location.** `crates/config-storage/src/rocks.rs:1025-1043`.

**Evidence.**

```rust
// rocks.rs:1025
if matches!(format_action, FormatAction::Migrate { from } if from == FORMAT_VERSION_V1) {
    // Ruling R1: the watermark is stamped from *this node's* `cluster_revision` ...
    let cluster_revision: u64 = read_meta(&db, CF_STATE_META, KEY_CLUSTER_REVISION)...;
    open_batch.put_cf(state_meta, KEY_COMPACT_REVISION, encoded);   // rocks.rs:1043
}
```

Chain, every link read from the as-built source:

1. `cli.schema()` returns `COMPAT_SCHEMA_1` for `--compat-schema 1`
   (`crates/config-server/src/cli.rs`), whose `format_version` is **1**
   (`crates/config-core/src/schema.rs`, `COMPAT_SCHEMA_1`).
2. `run.rs:958` sets `max_format_version: cli.schema().format_version` → ceiling 1.
3. A fresh directory takes `FormatAction::Stamp` (`rocks.rs:1588`) and is stamped with
   `options.max_format_version`, i.e. **marker 1**, over the *current* column-family set and
   *current-grammar* data (`rocks.rs:1016-1023`). This is exactly what the M6-R20 comment
   at `rocks.rs:1620-1632` says happens, and what `m6_98c` and `m6_r20_a` exercise.
4. Restarting without the flag gives ceiling 3, so `check_format_version` falls to
   `FORMAT_VERSION_V1 | FORMAT_VERSION_V2 => Migrate { from: 1 }` (`rocks.rs:1584`).
5. `rocks.rs:1025` then fires and overwrites `state_meta/compact_revision` with
   `cluster_revision` — for a directory that has a full journal and has compacted nothing.

**Consequence.** After the upgrade step of the documented rolling upgrade, the upgraded node
reports `compact_revision == cluster_revision`. `KvState::restore_compact_revision` unions by
`max` (`config-core/src/state.rs:321`), so the value is **not recoverable**. Downstream:
`watch.rs:633` `fetch_max`es the hub floor to it and `watch.rs:1248` uses it as the resume
floor, so every resume cursor at or below the node's current revision is refused as
`compacted`; `node.rs:678` / `node.rs:2241` refuse historical reads the same way. The node's
peers still report `compact_revision == 0`, so the cluster's answer to "can I resume from R?"
now depends on which node the client reaches. No divergence panic, because ruling R1 keeps
`compact_revision` out of `state_hash` — which is precisely why this is silent.

**False-positive check.** Three things I tried to falsify it with, none of which hold:
- *Is the clause guarded by the CF layout?* No. The layout arm at `rocks.rs:901-903` only
  decides the pre-open probe; the stamp at 1025 keys on `from` alone.
- *Is `cluster_revision` zero in practice?* Only for a directory that never served a write.
  `m6_r20_a` itself drives it to 3 and asserts `cluster_revision == 3` after the migration.
- *Does an existing row catch it?* No. `m6_98c` migrates an **empty** directory (watermark
  stamped as 0, indistinguishable from correct). `m6_r20_a`
  (`crates/config-storage/tests/m6_compat_open.rs:155-193`) migrates a directory with three
  applied entries and asserts `cluster_revision`, but never `compact_revision`. Adding
  `assert_eq!(upgraded.reader().compact_revision(), 0)` to `m6_r20_a` is, I believe, a
  one-line reproduction; I did not run it (see Coverage gaps).

**Closure condition.** `m6_r20_a` (or a new row beside it) asserts that a directory written by
a pinned build and then migrated reports `compact_revision == 0`, and passes. The natural fix
is to gate the stamp on the directory actually being a legacy-v1 *layout* — e.g. only stamp when
the pre-open probe saw `CfLayout::LegacyV1`, or when `CF_EVENTS` was absent — rather than on
`from == FORMAT_VERSION_V1`. Ruling R1's rationale ("the pre-v2 history has no journal") is
a statement about the layout, not about the marker; M6-R20 already retired that same
marker-as-proxy reasoning one function away, and this is the second site that still uses it.

**If not fixed:** an explicit lead acceptance recorded in ADR-0021 Note 5 and in the
credential/upgrade runbook, saying that a compat-1 node loses watch resumability and
historical reads across its upgrade, and that clients must re-list rather than resume.

---

## MATERIAL-1 — the gossip-key removal refusal does not cover the peer that is still *signing* with the key

**Criterion.** ADR-0028: `remove_key` "is refused by default if the target key is still the
only key a known peer accepts"; the operator-facing promise (runbook
`docs/runbooks/credential-rotation.md:184`) is "`remove` too early is refused".

**Location.** `crates/config-gossip/src/node.rs:570-618`;
`crates/config-gossip/src/meta.rs:141-146`.

**Evidence.**

```rust
// node.rs:611
async fn peers_holding_only(&self, fingerprint: GossipKeyFingerprint) -> usize {
    ... .filter(|keys| keys.is_sole(fingerprint)).count()
}
// meta.rs:141
pub fn is_sole(&self, fingerprint: GossipKeyFingerprint) -> bool {
    let mut advertised = self.iter();
    advertised.next() == Some(fingerprint) && advertised.next().is_none()
}
```

The guard fires only for a peer advertising **exactly one** key. The dangerous state of a
rotation is the one in between: after the `add` sweep and before the `use` sweep, every peer
advertises `{K_old, K_new}` while still *signing* with `K_old` (its primary). `is_sole` is
false for those peers, the removal is allowed, and this node goes deaf to every peer that has
not yet been promoted. That is the exact failure the refusal exists to prevent, and it is the
mis-sequencing an operator is most likely to make (`add` and `remove` both look node-local).

The information needed to catch it is already on the wire. `GossipKeyring::read`
(`node.rs:669-680`) puts the primary first, `publish_keyring` advertises `state.accepted` in
that order, and `AcceptedGossipKeys` preserves order (`meta.rs:110-116`). A check of
"`fingerprint` is some peer's advertised **slot 0**" is one comparison away.

**False-positive check.** The code matches ADR-0028:92 and the runbook as written, so this is
a design gap rather than a code/doc mismatch — I checked both before raising it. It is still
a gap: `docs/runbooks/credential-rotation.md:184` sells the refusal as the guard rail, and it
is not one for the window that matters.

**Closure condition.** Either `peers_holding_only` also counts peers whose advertised slot 0
is `fingerprint` (with a row proving `remove` is refused mid-`use`-sweep), or ADR-0028 and
the runbook's `remove` step state in one sentence that the refusal catches only single-key
peers and that the `use` sweep must be confirmed complete first.

---

## MATERIAL-2 — the new self-owned TLS accept loop has no handshake timeout and no in-flight cap

**Criterion.** Spec §19 / ADR-0028: a listener's unauthenticated path must be bounded. The
module's own claim, `crates/config-grpc/src/server.rs:168-172`: "A client that completes TCP
and then sends nothing would otherwise hold the whole listener for as long as it liked — an
unauthenticated denial of service that needs one socket."

**Location.** `crates/config-grpc/src/server.rs:179-232` (`spawn_handshakes`), specifically
the spawn at 208 and `acceptor.accept(io).await` at 210.

**Evidence.** `accept` is awaited with no `tokio::time::timeout`, and a task plus a socket is
spawned per accepted connection with no semaphore. `HANDSHAKE_BUFFER = 32` (line 51) bounds
only *completed* handshakes waiting for tonic; nothing bounds handshakes in progress.
`grep -n "timeout" crates/config-grpc/src/server.rs` returns nothing.

**Consequence.** N TCP connections that complete the handshake's TCP stage and then stall
produce N permanently-parked tasks and N held descriptors, from an unauthenticated client.
The spawn fixed head-of-line blocking on `accept`; it converted the stall into a leak rather
than removing it, while the comment reads as though the problem is solved.

**False-positive check.** This is not a regression in kind: tonic's own `ServerTlsConfig`
acceptor, which this replaces, also awaits the handshake without a default timeout, so
pre-M6 exposure was comparable. Raising it as MATERIAL rather than BLOCKER on that basis.
What is new is that rEtcd now owns the loop, so the bound is now rEtcd's to set, and the
module comment now asserts the property.

**Closure condition.** A bounded handshake (`tokio::time::timeout` around `acceptor.accept`,
counted as `AuthnRejectReason::HandshakeFailed` on expiry) and/or a `Semaphore` cap on
in-flight handshakes, with a row driving stalled connections; **or** the comment at 168-172
amended to say the spawn removes the head-of-line stall only, with the residual exposure
recorded in ADR-0028.

---

## MATERIAL-3 — the testkit's duplicated `gossip_rotation_error` has drifted from the daemon's, while claiming it has not

**Criterion.** FOCUS: duplicate code; comment accuracy versus as-built.

**Location.** `crates/config-testkit/src/rotation.rs:96-118` vs
`crates/config-server/src/run.rs:556-582`.

**Evidence.**

```
crates/config-server/src/run.rs:579:      reason: format!("gossip_advertise_failed: {other}"),
crates/config-testkit/src/rotation.rs:115: reason: format!("gossip_key_advertise_failed: {other}"),
```

The testkit copy's doc comment (`rotation.rs:98-105`) states it "Mirrors `config-server`'s
`gossip_rotation_error` **exactly**, including the two greppable prefixes a caller branches
on ... a harness whose refusal read differently would make M6-59 assert a string production
never produces." Two of the three prefixes match; the third does not. The duplication was
accepted because `config-server` is bin-only (ADR-0028 as-built), and the first thing the
duplicate did was diverge — which is the hazard the comment was written to deny.

**False-positive check.** Only the advertise-failure arm differs, and no M6 row I read
asserts that prefix, so nothing currently fails. `parse_gossip_key`'s restatement
(`rotation.rs:79-90`) is byte-equivalent in behaviour to `config::parse_gossip_key` — I
checked that one separately and it is fine.

**Closure condition.** The two strings match (either spelling), or the comment stops
claiming "exactly" and names the arm that intentionally differs.

---

## ADVISORY findings

**A1 — `TlsRotator::try_reload` publishes `served` before it swaps the planes.**
`crates/config-grpc/src/rotation.rs:224-258`. `*served = found` happens at 224-231; the
per-plane `replace` and `peer_dial.reload` run at 236-252 and can each return `Err`. If one
ever failed mid-loop, `served` would already equal `found`, so the *next* reload computes
`changed == false` and never retries the un-swapped planes — a permanently split credential
set, silently, contradicting the "refusal is node-wide by construction" comment at 191-195.
Unreachable today: `Credentials::compile` is deterministic on the same bytes and already
succeeded at 218, and `GrpcPeerTransport::reload`'s only fallible step is that same compile.
Closure: swap first and publish `served` only after every plane has taken the new material,
or state in the comment that the ordering relies on `compile` determinism.

**A2 — stale doc comment on the expiry-latch test.** `crates/config-grpc/src/rotation.rs:633`
says "Asserted through the latch rather than by capturing the log line ... a subscriber
assertion would additionally be testing `tracing`'s dispatch." The test at 637 does exactly
the opposite — it installs `WarnCounter`, a `tracing_subscriber::Layer`, and counts emitted
events. The **code is right** (the `WarnCounter` doc at 596-601 gives the correct reason);
only this comment is wrong, and it is wrong in the direction the FOCUS list warns about.
Closure: delete or invert the paragraph at 633-636.

**A3 — `AuthnRejectReason::UntrustedServerCa` doc names the wrong side.**
`crates/config-engine/src/metrics.rs`, `UntrustedServerCa` variant: "Recorded on the dialling
side, where the alert rather than the verification failure is what is observed." As built it
is recorded by the **listener**: `classify_handshake_failure`
(`crates/config-grpc/src/server.rs`) maps `AlertReceived(UnknownCA)` to it, and
`source.record_rejection` runs on the accept path. The *meaning* is right (the peer rejected
our server certificate); the sentence about where it is counted is not. Closure: reword.

**A4 — "primary first" is an unverified memberlist claim, restated twice.**
`crates/config-gossip/src/node.rs:671` and `crates/config-grpc/src/admin_plane.rs:211` both
assert the accepted list is primary-first; the source is `keyring.keys()` ordering
(`node.rs:676`), which ruling M6-R5 recorded as an UNVERIFIED memberlist 0.8.5 capability.
Nothing today depends on the order (`is_sole` is order-insensitive for a one-element set), but
the MATERIAL-1 fix would. Closure: a unit row asserting `keyring.keys().next() ==
keyring.primary_key()` after `use_key`, or drop the claim.

**A5 — the policy signature payload carries no domain-separation tag.**
`crates/config-core/src/policy.rs:188`, `signature_payload` = `hash ‖ version_le`, 40 bytes
with no context string. Low risk in practice: `trust_keys` are policy-specific and
`verify_strict` is used correctly. Worth one constant prefix if an operator might ever reuse
an ed25519 key across rEtcd surfaces. Closure: prefix a fixed ASCII tag, or a one-line note in
ADR-0027 that trust keys must not be shared.

---

## Ratings on the already-logged gaps (not re-raised)

| Gap | Rating | Note |
|---|---|---|
| M6-33 `policy_version_ref` always `None` | ADVISORY | Page tokens still bind `policy_version`; the ref is reporting only. |
| M6-35 no `restore_policy_mismatch` line | ADVISORY | Operator loses one diagnostic, no behaviour change. |
| Continuation page on a follower returns `Node` without a leader hint | MATERIAL | Client-visible; a retry loop cannot self-steer. Worth a row, not a gate. |
| Drain predicate clause (a) could replicate a lucky decode to a lagging follower | ADVISORY (pre-existing) | Clause (b) `index <= last_applied` covers the case that reaches this node's own state machine, which is what M6-R20 claims. |
| `m6_rotation` port/timing sensitivity on Windows | ADVISORY | Harness property; does not affect the shipped path. |

## What I checked and found clean

- `scan_log_for_upgrade` / `refuse_if_undrained` (`rocks.rs:1596-1730`): the two clauses match
  the M6-R20 ruling text; unreadable keys count as blocking rather than being skipped; the
  scan is read-only and runs only on a legacy marker; `first_blocking_index` and `reason` reach
  the log. `m6_r20_a`/`m6_r20_b` assert the pass and the fail halves and assert the refusal is
  read-only.
- `max_applied_command_schema` (M6-R15): written in the same synced batch as its command
  (`rocks.rs:2590-2605`), captured under the same lock as the other header fields
  (`capture_view`), and unioned by `max` on install (`apply_snapshot_records`) — the union is
  in the same `last` batch as `retired_nodes`. Consistent with the ruling.
- `CredentialSource` (`credentials.rs`): compile-before-swap, generation bumped inside the
  write lock, `current_with_generation` reads both under one guard, poisoned locks recovered
  rather than fatal, ALPN `h2` asserted rather than assumed, per-reason counters indexed not
  mapped. No secret material in `Debug` for `TlsRotator` or in any log line or error I found.
- `AuthnRejectReason` closed label set, `ALL` zero-publication, `index()` ↔ `ALL` consistency.
- `PolicyLoader::reload`/`attempt` (`config-server/src/policy.rs:93-185`): fail-closed means
  "does not adopt", break-glass is surfaced on the `policy_loaded` line and counted separately,
  `policy_rejected` carries the stable reason, and `on_policy_change` is correctly suppressed
  for the first load / identical hash / a rollback about to be refused.
- `verify_policy` (`config-core/src/policy.rs:304-345`): `verify_strict`, version bound *into*
  the signed payload, envelope version byte checked first, version-binding checked before hash
  so a relabelled signature reports the relabelling.
- `rotate_gossip_key` admin path: step parsed before `dispatch` so a refused caller is still
  audited under the right `op`; `key_hex` never logged, echoed or audited; `Unspecified`
  refused rather than defaulted; per-step audit tokens (`gossip_key_add|use|remove`).
- `spawn_tls_poller`: first tick consumed, `MissedTickBehavior::Delay`, reload on the blocking
  pool, a failed poll counted and the loop continued, shutdown biased ahead of the tick.
- Evidence JSON: `dirty: true`, `profile: "debug"`, `scale_factor: 0.333`, `full_scale: false`
  are all recorded honestly — consistent with the "features-local" ruling. `m6_evidence.rs:1613`
  actively forbids the strings "production ready", "production-ready" and "rpo objective met".
  A repo-wide grep for production/enterprise claims found no hit outside that guard.

## Coverage gaps (what I did not do)

- **Ran no cargo build or test.** 191 GB free, so disk was not the constraint; a cold RocksDB
  build did not fit the 40-minute budget alongside the reading. Every finding above is from
  source reading, and each names the file:line to re-check. BLOCKER-1 in particular deserves
  the one-line assertion in `m6_r20_a` before it is acted on.
- Not reviewed in depth: `crates/config-testkit/tests/m6_rotation.rs` (24 rows — I read the
  harness, not the rows), `m6_evidence.rs`, `m6_policy_daemon.rs`, `m6_rbac.rs`,
  `m6_pagination_e2e.rs`, `config-core/src/state.rs` RBAC internals, `health.rs`,
  `scripts/local-cluster.ps1`, `docs/quickstart-local.md`, `docs/runbooks/dedup.md`,
  `docs/testing/test-plan-m6.md` row-by-row.
- `e2e_daemon.rs` excluded by assignment (tester-m6d in flight).

---

# Delta review — 2026-09-19 (second pass)

Read-only, deltas only. Every disposition below was checked against the as-built source, not
against the change description. **Revised verdict: PASS_WITH_RISKS, 0 BLOCKER, 0 MATERIAL,
2 new ADVISORY.** The gate is clear.

| Finding | Disposition |
|---|---|
| BLOCKER-1 compact_revision stamp | **CLOSED** |
| MATERIAL-1 gossip removal guard | **CLOSED** |
| MATERIAL-2 unbounded handshakes | **CLOSED** (residuals accepted) |
| MATERIAL-3 testkit prefix drift | **CLOSED** |
| A1 `served` published early | **CLOSED** |
| A2 stale latch-test comment | **CLOSED** |
| A3 `UntrustedServerCa` side | **CLOSED** |
| A4 "primary first" unverified | **CLOSED** |
| A5 no domain-separation tag | **CLOSED** (accepted + dated note) |
| — | **NEW: N1, N2 (both ADVISORY)** |

## BLOCKER-1 — closed, and the lead's question answered

`rocks.rs:1029-1060` now reads
`Migrate{from:1} && (layout == CfLayout::LegacyV1 || journal_is_empty(&db))`, with
`journal_is_empty` at `rocks.rs:1773-1776`. The comment above it states the M6-R22 reasoning
correctly and names the M4-14/M4-18 retry shape the empty-journal branch exists for. The
red→green and the marker-only mutation (E2E-42 failing with
`compact_revision=10 cluster_revision=10`, `m6_r20_a` failing `3 ≠ 0`) are the right pair of
proofs: the first shows the row catches the bug, the second shows the row catches it *for the
right reason*. Sustained as closed.

**Q: is "populated journal ⇒ history resumable" sound for a `--compat-schema 1` directory
whose journal has been retention-trimmed?** Yes. I tried to build the counter-shape and could
not; three independent arguments block it.

1. **A pinned directory cannot have been retention-trimmed at all.** Retention trims by
   proposing `Compact` (`watch.rs:1751` `retention_target` returns a watermark that becomes a
   `Compact` command), and `Compact` is `COMMAND_SCHEMA_V2`-gated (`schema.rs`
   `command_gate`). While any voter is pinned the gate is shut, and the pinned node would
   refuse to decode a `Compact` that somehow arrived (`SchemaTriple::decode_command`, M6-99).
   E2E-42's own preamble makes exactly this argument for its first two restarts.
2. **The reverse shape is unreachable.** A *current* directory that had been trimmed cannot
   then be restarted pinned: the pre-open probe refuses marker 3 against ceiling 1
   (`rocks.rs:876-893`, asserted by `m6_98`). So no trimmed history can ever be behind a
   marker-1 directory.
3. **Trimming and the watermark move together by construction.** The resume floor *is*
   `compact_revision` (`watch.rs:1248`, `1273-1276`), and the only thing that deletes journal
   records is the `Compact` that raises it. There is no "journal trimmed, watermark stale"
   state for the new predicate to be wrong about — a populated journal's existing watermark is
   the truth, and keeping it is right.

**Where the empty-journal branch could in principle over-claim, and why it does not.** It
raises the watermark to `cluster_revision` whenever `events` is empty. A snapshot install does
*not* produce that state: snapshots carry `CF_EVENTS` records and `apply_snapshot_records`
rebuilds `JournalStats` from them (`rocks.rs:3598-3607`). The only remaining empty-journal
shapes are a genuine v1 layout (no journal has ever existed) and a directory that has applied
nothing (`cluster_revision == 0`, so the stamp is a no-op). Both are correct.

One residual worth a sentence, not a change: the branch is a **raise**, and
`restore_compact_revision` unions by `max` (`config-core/src/state.rs:321`), so it is
irreversible. Its safety rests entirely on the reachability argument above, which lives
nowhere in the tree. Recording those three bullets beside `journal_is_empty` or in ADR-0021
note 6 is cheap insurance against a future change quietly making an empty `events` family
reachable with `cluster_revision > 0`.

## MATERIAL-1 — closed

`meta.rs:157-159` `is_primary` (slot 0), `node.rs:619-627` `peers_still_needing` =
`is_sole || is_primary`. Error wording (`error.rs:58-68`) now names both clauses and points at
the `use` sweep. `m6_r21_removing_a_key_a_peer_still_signs_with_is_refused`
(`tests/gossip.rs:926`) drives the real two-node window — both nodes hold both keys, only node
1 promotes, node 2's trailer is confirmed at `len() == 2` (so `is_sole` is false and only the
new clause can refuse), and the removal is refused. That is the exact shape I raised.

## MATERIAL-2 — closed, residuals accepted

`tls.rs:85` `handshake_timeout`, `DEFAULT_HANDSHAKE_TIMEOUT = 10s` (`tls.rs:94`);
`server.rs:208` `Semaphore::new(MAX_INFLIGHT_HANDSHAKES)` with the permit **taken before
`accept`** (`server.rs:214-224`) and **moved into the handshake task** (`server.rs:246`
`let _permit = permit;`), so it is held for exactly the slot's lifetime — the two ways this
fix is usually got wrong are both avoided. Expiry is counted as `HandshakeFailed`
(`server.rs:276-292`), and the `pin!` + `timeout(.., handshake.as_mut())` shape at 254-256
deliberately drops the socket *after* the count, which is what makes `m6_45` assertable
without a sleep. The rewritten comment no longer overclaims.

Residuals, all accepted as ADVISORY, none blocking: the rotator rebuilds `MtlsConfig` from
`TlsFiles` so the timeout is not operator-configurable today (it is carried through
`read_material`'s `template.clone()`, so it does not regress across a rotation); at the cap
the loop parks in `acquire_owned` and arrivals queue in the kernel backlog, which is the right
trade; a stalled tonic consumer can occupy all 256 slots through `tx.send().await`, still
bounded. No daemon-level row — the unit row is proportionate.

## MATERIAL-3, A1–A5 — closed

- **M3:** both copies now emit `gossip_advertise_failed:` (`run.rs:579`,
  `testkit/rotation.rs:115`); grep shows no other spelling.
- **A1:** `rotation.rs:224-262` holds the `served` guard across every swap and publishes at
  262, with the reasoning stated. I checked lock ordering for the deadlock this could have
  introduced: the guard now wraps `planes.lock()`, and no other path (`register`,
  `expiry_seconds`, `authn_rejections`, `metrics`) takes `planes` before `served`. Clean.
- **A2:** `rotation.rs:634-637` now says the assertion counts emitted events through
  `WarnCounter`, and gives the right reason.
- **A3:** `metrics.rs:100-103` now says "Recorded on the listening side", with the alert
  mechanism spelled out.
- **A4:** `node.rs:679-683` keeps the claim but marks it UNVERIFIED per M6-R5, cites `m6_57` as
  the evidence, and says the removal refusal reads slot 0 so the claim is load-bearing.
  (Nit, not a finding: `admin_plane.rs:211` still states "primary first" bare.)
- **A5:** ADR-0027:28 carries the dated as-built note; payload unchanged, as agreed.

## NEW findings

**N1 (ADVISORY) — `peers_still_needing` counts the calling node itself.**
`crates/config-gossip/src/node.rs:619-627` iterates `member_meta()`, which is documented at
`node.rs` as returning what every member advertises, **"this node included"**. A node that has
not yet promoted has its own slot 0 == `K_old`, so `is_primary` matches *itself*. Removing
`K_old` before promoting therefore now returns
`GossipKeyStillNeeded { peers: n+1 }` — "still needed by N peer(s)" counting a non-peer —
where it previously fell through to `memberlist`'s own primary refusal (`gossip_keyring_refused:`).
No unsafe outcome: with `--force` it still hits `keyring.remove`, which refuses the primary.
But the operator-visible error *class* changed for a common mistake, and the count is off by
one. False-positive check: `m6_r21` does not catch this because node 1 promotes first, so it
is correctly not counted there. Closure: skip this node's own meta in
`peers_still_needing` (or subtract it), with the count asserted in a row where the caller has
not promoted.

**N2 (ADVISORY) — E2E-42's client-visible resume assertion can silently skip on the restart
that matters most.** `e2e_daemon.rs:3582-3613`. The `assert_ne!(compact_revision,
cluster_revision)` signature check *is* as race-proof as its comment claims — I verified the
arithmetic: the row sets `max_revisions: Some(20)` and leaves `max_bytes`/`max_age` at their
zero defaults (`e2e_daemon.rs:3630-3634`), and `retention_target` (`watch.rs:1767-1771`) only
fires when `count > max_revisions` and then targets `newest - max_revisions`, so a legitimate
compaction lands ~20 revisions below `cluster_revision` and can never produce equality.
Sustained as correct.

The *strong* half is the problem: the revision-0 watch resume runs only
`if health.compact_revision == 0`. On the third restart every voter is current, so the schema
gate opens, `check_interval_secs: Some(1)` fires, and with ~30 revisions written the retention
tick trims almost immediately — so the branch is skipped and the row silently degrades to the
signature check alone, exactly on the node with the most history behind it. The comment
concedes this ("true in practice for the third too"). It is not a flake — it never fails
spuriously — it is a coverage hole that opens quietly, which is worse to discover later.
Closure: make the resume unconditional by resuming from the lowest revision the node still
claims to serve (`start_after_revision: health.compact_revision`, which must succeed for any
honest watermark) instead of skipping when the watermark has moved off zero.

## Delta-review coverage

Read: `rocks.rs` open/migration path + `journal_is_empty`, `watch.rs` retention and resume
floor, `schema.rs` gate, `meta.rs` `is_primary`, `node.rs` `peers_still_needing`/`member_meta`,
`error.rs`, `tests/gossip.rs` `m6_r21`, `server.rs` accept loop, `tls.rs` timeout,
`rotation.rs` `try_reload` + tests, `metrics.rs`, testkit `rotation.rs`, ADR-0027 note,
`e2e_daemon.rs` `e42_*` helpers and the E2E-42 body. Still not run: no cargo in either pass —
the red/green and mutation evidence for BLOCKER-1 is the lead's, not independently reproduced.
Not read: E2E-41/43/45, `m6_45` row body, ADR-0021 note 6 / ADR-0030 M6-R22 note text.
