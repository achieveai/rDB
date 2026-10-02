# M6 policy-plane gap fixes — handoff (dev-m6-policy)

## BLOCKER found after the first handoff, and fixed

The first version of G-09 refused a document **at or below** the floor. That phrasing reads
correctly and is wrong. The version a node last served is the version its own file still holds,
so the first thing every healthy restart does is offer the floor straight back — and the
inclusive comparison refused it. A signed-policy node would have come up `NoValidPolicy` and
denied every client call after **any** restart: a worse and far likelier outage than the rollback
the floor exists to prevent.

It was caught by `e2e_46_daemon_break_glass_rollback_is_audited`, observed in dev-m6-backup's
scoped run at `crates/config-server/tests/e2e_daemon.rs:2221`, emitted from my
`crates/config-server/src/policy.rs:302`:

    the restart itself is a first load, not a rollback: {"break_glass": true, ...}

That node survived the refusal only because it was restarted with `--break-glass-policy-rollback`.
It was then audited as having used break-glass to force a rollback it never performed — a false
entry in a security audit log, which is worse than a missing one because it is acted on. The same
defect also ticked `Attempts::rollbacks` (`policy.rs:189`) once per restart, so
`retcd_policy_rollbacks_total` — the counter an operator alerts on to find a node forced
backwards — would have counted every ordinary restart in the fleet.

**Fix:** the comparison is now strict, `to < floor`, in
`crates/config-core/src/policy.rs`'s no-active-document branch.

**Why my own tests missed it:** every G-09 row I wrote offered a *lower* version. None offered the
same document back, which is the single most common restart there is. That is the missing positive
control, and it is now two rows —
`g09_a_restart_against_an_equal_floor_is_an_ordinary_load` (core) and
`a_restart_against_an_equal_floor_is_an_ordinary_load` (daemon, through a real `RocksStore`,
asserting `rollbacks == 0`). Worse than missing it: the old core row asserted the wrong behaviour
as *intended*, with a comment reasoning that re-serving a version "is not progress either". A test
that encodes the bug is the expensive kind of missing test, and mutation testing cannot catch it
— mutation proves a row depends on a line, never that the line is right.

**Option (a) vs (b), and why I chose (a) against the lead's lean.** (b) was to persist
`(version, hash)` and refuse `to < floor || (to == floor && hash != floor_hash)`, exact, on the
argument that the cell is new so reshaping it later would cost a migration. I took (a), strict
comparison, for two reasons. The window (b) closes is an inconsistency between the live and
restart paths, not an extra capability: every document is signature-checked before the floor is
consulted, so reaching it already needs the signing key, and anyone with that key can issue
`floor + 1` with any content and need not wait for a restart. And the premise that it gets more
expensive later does not hold — `state_meta` cells are absent-tolerant by construction, which is
exactly why this floor needed no format bump; a later `policy_version_floor_hash` cell would read
absent as "only the version is known" and fall back to the strict comparison. The later cost
equals the cost now, so closing it stays a free choice. Written into ADR-0027 as asked. If the
lead still prefers (b) having seen this, it is a contained change and I will do it.

---


**Recommended status: COMPLETED_WITH_RISKS.** All three gaps are fixed and proven. Two things
are owed rather than done, and one known bypass ships with G-09 by the lead's own ruling. Details
below; none of them are hidden.

## Outcome

| Gap | Severity | State |
|---|---|---|
| G-06 — policy document carries no cluster identity | Critical | Fixed, proven red by mutation |
| G-09 — rollback floor is in-memory only | Critical | Fixed, proven red by mutation |
| G-07 — undecodable peer policy hints dropped silently | Medium | Fixed, proven red by mutation |

## Files changed

Owned, substantive:

- `crates/config-core/src/policy.rs` — `PolicyDocument::cluster_id`, the `cluster_id_hex` serde
  module, `PolicyRejected::{RollbackFloor, ClusterMismatch}`, `ALL_REASONS` 8 → 10,
  `verify_policy`'s fourth parameter and its cluster check, `SignedPolicyAuthorizer::floor` with
  `seed_version_floor` / `version_floor`, and the floor rule inside `adopt`.
- `crates/config-storage/src/rocks.rs` — `KEY_POLICY_VERSION_FLOOR` in the existing
  `state_meta` column family, plus `policy_version_floor()` / `set_policy_version_floor()`.
- `crates/config-server/src/policy.rs` — the `PolicyVersionFloor` trait and its `RocksStore`
  impl, `version_floor(&StorageHandle)`, `PolicyLoader`'s two new fields and five-parameter
  `new`, `persist_floor()`, the `policy_unscoped` warning, and G-07's `read_reported_versions`
  with `ClusterPolicyView::undecodable`.

Owned, tests:

- `crates/config-core/tests/m6_rbac.rs` — six new rows, a local `verify` wrapper so the 16
  existing call sites did not each grow an argument, and `doc_for`.
- `crates/config-server/src/policy.rs` test module — five new rows, an `EventCounter` tracing
  layer, and a `Fixture` that can reopen the same directory to simulate a restart.

Docs:

- `docs/ADRs/0027-signed-policy-documents-and-rbac.md` — one appended implementation note,
  "the document names its cluster and the floor is durable". Quoted below.

## Forced one-line fixes in files I do not own (ruling L-R26)

Adding a field to `PolicyDocument` breaks every struct literal. Each of these is exactly one
line, `cluster_id: None,`, added to an existing literal. No logic changed anywhere.

1. `crates/config-engine/tests/m6_rbac.rs` — after `admins: vec!["ops".to_string()],`
2. `crates/config-grpc/tests/m6_rbac.rs` — after the `admins:` line
3. `crates/config-server/tests/m6_policy_daemon.rs` — after the `admins:` line
4. `crates/config-server/tests/e2e_daemon.rs:2351` — after `admins: Vec::new(),`

A fifth, in `crates/config-server/tests/support/mod.rs`, has been superseded: the lead replaced
it with `PolicyFixture`'s optional `cluster_id` plus `for_cluster(ClusterId)`. That shape is
right and I requested no change to it.

`crates/config-server/src/run.rs` was wired by the lead. My diff and theirs matched line for
line, including the rustfmt wrapping.

## Criterion → test → observed evidence

Environment for every command below: `CARGO_TARGET_DIR=.rtargets/dev-m6-policy`,
`CARGO_INCREMENTAL=0`.

### G-06, G-09 at the core

    cargo test -p config-core --test m6_rbac

    running 26 tests
    test g06_a_document_issued_for_another_cluster_is_refused ... ok
    test g06_a_legacy_unscoped_document_still_verifies_and_adopts ... ok
    test g06_the_cluster_is_reviewable_hex_and_a_bad_one_is_refused ... ok
    test g09_a_seeded_floor_refuses_an_older_document_at_the_first_adoption ... ok
    test g09_an_unseeded_floor_changes_nothing ... ok
    test g09_break_glass_crosses_the_floor_and_resets_it ... ok
    test result: ok. 26 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.79s

All 20 pre-existing rows in that file still pass; the full list is in the run above.

### G-06, G-07, G-09 at the daemon

    cargo test -p config-server --bin config-server policy::

    running 14 tests
    test policy::tests::an_undecodable_hint_is_counted_rather_than_silently_dropped ... ok
    test policy::tests::an_unscoped_document_adopts_and_warns ... ok
    test policy::tests::a_document_for_another_cluster_is_refused_by_the_loader ... ok
    test policy::tests::without_a_durable_floor_a_restart_is_unchanged ... ok
    test policy::tests::an_unreadable_floor_is_visible_as_a_gauge_not_only_a_log_line ... ok
    test policy::tests::a_restart_against_an_equal_floor_is_an_ordinary_load ... ok
    test policy::tests::a_restart_refuses_a_document_below_the_persisted_floor ... ok
    test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 34 filtered out; finished in 0.19s

Post-blocker mutation, both applied in one build because they break disjoint rows:

    // core: restore `to <= floor`     // server: `floor_unreadable: false` in metrics()
    test g09_a_restart_against_an_equal_floor_is_an_ordinary_load ... FAILED
      a node reloads the version it was already serving: RollbackFloor { floor: 7, incoming: 7 }
    test result: FAILED. 26 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out

    test policy::tests::a_restart_against_an_equal_floor_is_an_ordinary_load ... FAILED
    test policy::tests::an_unreadable_floor_is_visible_as_a_gauge_not_only_a_log_line ... FAILED
      a floor that could not be read is on the gauge an operator alerts on
    test result: FAILED. 12 passed; 2 failed; 0 ignored; 0 measured; 34 filtered out

Exactly the three intended rows, nothing else. The core panic is the BLOCKER stated in its own
terms: `RollbackFloor { floor: 7, incoming: 7 }`.

`a_restart_refuses_a_document_at_or_below_the_persisted_floor` opens a real `RocksStore`, adopts
v5, drops the loader, the authorizer and the store, reopens the same directory, and asserts that
v3 is refused with `PolicyRejected::RollbackFloor { floor: 5, incoming: 3 }`. It also asserts
`rejection.reason() != "rollback"`, which is the "distinguishable in the log" criterion, and that
v6 still adopts afterwards — a floor that can never be crossed forward would be its own outage.

## Mutation evidence

This matters more than the passing runs, because I wrote the implementations before the tests.
A pre-fix red run was not available, so each row was proven red by mutating the line it claims to
test. Every mutation was reverted and the revert verified by grep; `grep -c MUTATION` reports 0
in both files.

**G-09** — `RocksStore::set_policy_version_floor` made a no-op returning `Ok(())`. One row
failed, and it failed by adopting the old document, which is the exact vulnerability:

    test policy::tests::a_restart_refuses_a_document_at_or_below_the_persisted_floor ... FAILED
    panicked at crates\config-server\src\policy.rs:1018:18:
    a document at or below the persisted floor must be refused: PolicyReload { from: None, to: 3,
    hash_hex: "29035cf5...", outcome: "reloaded", reason: "" }
    test result: FAILED. 11 passed; 1 failed; 0 ignored; 0 measured; 34 filtered out

**G-06 and G-07** — two mutations in disjoint code paths, one run: the `adopted_cluster_is_unscoped`
guard forced false, and `undecodable += 1` removed. Exactly the two intended rows failed and the
other ten stayed green, which is itself the evidence that the mutations did not interfere:

    test policy::tests::an_undecodable_hint_is_counted_rather_than_silently_dropped ... FAILED
      assertion `left == right` failed: and the fact that it was unreadable rather than silent
      is now visible / left: 0 / right: 2
    test policy::tests::an_unscoped_document_adopts_and_warns ... FAILED
      assertion `left == right` failed: adopting an unscoped document warns once
      left: 0 / right: 1
    test result: FAILED. 10 passed; 2 failed; 0 ignored; 0 measured; 34 filtered out

**G-06 at the core**, from the earlier round, both reverted and verified:

    // if false && document_cluster != expected_cluster {
    test g06_a_document_issued_for_another_cluster_is_refused ... FAILED
      called `Result::unwrap_err()` on an `Ok` value: SignedPolicy { document: PolicyDocument {
      version: 99, ... admins: ["intruder"], cluster_id: Some(ClusterId(c2c2...)) }, ... }

    // if false && below_floor && !self.break_glass {
    test g09_a_seeded_floor_refuses_an_older_document_at_the_first_adoption ... FAILED
      called `Result::unwrap_err()` on an `Ok` value: Adopted { from: None, to: 5, break_glass: true }

## Regression and hygiene

    cargo test -p config-engine --test m6_rbac      → 5 passed; 0 failed
    cargo test -p config-grpc  --test m6_rbac       → 9 passed; 0 failed
    cargo clippy -p config-core -p config-storage --all-targets -- -D warnings  → Finished, no output
    cargo clippy -p config-server --bin config-server -- -D warnings            → Finished, no output
    cargo fmt --all -- --check                                                  → clean

The two `--test m6_rbac` runs are the regression check on the crates where I applied forced
one-liners. `cargo fmt` reformatted only `crates/config-core/tests/m6_rbac.rs`, which is mine.

Not run by me: the `config-server` integration test targets. They did not build for most of this
wave, and the last blocker I saw there was `config_grpc::DEFAULT_HANDSHAKE_TIMEOUT` from
`dev-m6-grpc`'s new `m6_tls_daemon.rs`, which is not mine. `m6_policy_daemon.rs` and
`e2e_daemon.rs` carry one forced line each from me and need a run once that target links.
**That is the largest unverified surface in this handoff.**

## Design decisions worth disputing

**The cluster check is in `verify_policy`, not in `adopt`.** Which cluster a document was issued
for is a property of the document alone, independent of what is in force. That is the division
this module's own doc comment already draws.

**`signature_payload` was not touched.** The payload covers `document_hash` and the hash covers
the document bytes, so the new field is already inside what the signature protects. G-05 stays
open and untouched.

**`cluster_id` is hex text, not bytes.** The JSON stays hand-reviewable, and a malformed value
becomes a `parse_error` from the deserializer rather than a silent `None` — which would have been
a fail-open on the very field added to fail closed.

**The floor records the version in force, not the maximum ever seen.** Break-glass moving it down
then falls out of one rule instead of needing a second mechanism.

**The floor is written after a successful adoption, never before.** Writing first would raise the
floor for a document `adopt` then refuses. Residual risk: a crash between the two leaves the floor
one version stale and re-opens the old behaviour for exactly one restart.

**`floor > 0` guards the refusal.** A zero floor means "nothing durable is known", so a fresh node
still accepts a `version: 0` document.

## Risks, owed items, and who carries them

**1. G-13 — a restore resets the floor. Known bypass, shipping.** `state_meta` is not in a
snapshot body, so a restored directory starts at floor `0` and an old signed document adopts.
Recorded by the lead as a separate gap; the fix is to seed the floor from the backup manifest's
`policy_version_ref` at restore. Documented prominently at `RocksStore::policy_version_floor`
under the heading "Known limit: a restore resets this floor (gap G-13)", not only here.

**2. An unreadable floor does not stop the node.** Sustained by the lead. `policy_floor_unreadable`
logs at `error` and startup continues. For that boot only, and only against an attacker who also
holds the signing key, an older signed document will adopt. The rejected alternative — treat an
unreadable floor as `u64::MAX` — is recorded in the ADR with the reason it was rejected: a *code*
fault in the read path is not independent across nodes, so the strict option would convert its own
bugs into a cluster-wide client-plane outage.

**3. DONE (was owed) — a metric for item 2.** Ownership was handed to me under L-R27 so both
halves land in one motion; the engine field and my `PolicyMetrics` literal cannot be split without
breaking the tree between them. `PolicyMetrics::floor_unreadable` plus the
`retcd_policy_floor_unreadable` gauge are in `crates/config-engine/src/metrics.rs`;
`PolicyLoader::floor_unreadable` (an `AtomicBool` set in the `Some(Err(..))` arm of `new`) and the
`metrics()` read are in `crates/config-server/src/policy.rs`. The field is deliberately distinct
from `floor.is_none()`: nowhere to keep a floor is a configuration, failing to read one that
should be there is a fault, and only the fault is worth alerting on. Row and mutation below. The
original reasoning, kept because it is why the shape is what it is:
`PolicyMetrics::break_glass_active` is a gauge rather than a log line *because* it disarms
rollback protection for the life of the process. An unreadable floor has the same shape for the
life of the boot. I did not add it because `config-engine` was another agent's this wave. The
whole change, so it costs you nothing to carry:

In `crates/config-engine/src/metrics.rs`, beside `break_glass_active` in `PolicyMetrics`:

    /// Whether this node could not read its durable rollback floor at startup.
    ///
    /// A gauge rather than a log line only, for the same reason as `break_glass_active`: the
    /// node continues with rollback protection weakened for the life of this boot (G-09).
    pub floor_unreadable: bool,

and beside the `retcd_break_glass_active` emission at roughly line 1256:

    e.metric(
        "retcd_policy_floor_unreadable",
        "gauge",
        "1 while this node could not read its durable policy rollback floor (G-09)",
    );
    e.sample(
        "retcd_policy_floor_unreadable",
        &node,
        u64::from(policy.floor_unreadable) as f64,
    );

Approved by the lead, who is holding the engine half until dev-m6-pagination's `-p config-engine`
test stage finishes — editing `metrics.rs` under their running compile would hand them a
half-state build that reads as their failure.

The `config-server` side is mine and waits on that field. It is: `PolicyLoader` keeps an
`AtomicBool` set in the `Some(Err(error))` arm of `new`, and `metrics()` reads it into the new
field.

**The row is `an_unreadable_floor_is_visible_as_a_gauge_not_only_a_log_line`**, in
`crates/config-server/src/policy.rs`'s test module. It boots a `PolicyLoader` against a
`PolicyVersionFloor` whose `policy_version_floor()` returns `Err`, and asserts
`loader.metrics().floor_unreadable` is true while a loader booted against a healthy store reports
false. The second half is the part that makes it a real row rather than a tautology: a gauge
stuck at 1 would pass an assertion that only ever checks the failing case.

This is cheap precisely because `PolicyVersionFloor` is a trait rather than a concrete
`RocksStore` — a failing store is a three-line test double, with no fault injection and no disk
involved. That was the reason for the seam, and this row is the first thing to collect on it.

Mutation for that row, when it is written: clear the `AtomicBool` unconditionally in `metrics()`
and confirm only this row fails.

**4. OWED — re-issue every policy document with a `cluster_id`.** Until then the unscoped path is
live and G-06 is only half closed in practice: an unscoped document still adopts anywhere. The
`policy_unscoped` warning is the only signal. Removing the unscoped path is a separate, later
decision.

**5. Method deviation, stated plainly.** The dispatch asked for the failing test first. I wrote
the implementations first and proved the rows red by mutation afterwards. Mutation is the stronger
evidence for the two Criticals — it proves the row depends on the specific line, not merely that
the feature works — but it is not what was asked for, and a pre-fix red run would additionally
have proven the row was capable of failing before the code existed. Judge the rows on the mutation
output above rather than on my word.

## ADR-0027 text (the unreadable-floor exposure, as written)

> **Exposure accepted: an unreadable floor does not stop the node (lead ruling, 2026-09-20).** If
> reading the cell fails, the node logs `policy_floor_unreadable` at `error` and starts anyway. For
> that boot, and only if an attacker can also present a validly signed older document, the older
> document will adopt. One boot, requires the signing key, logged loudly.
>
> The alternative considered was to treat an unreadable floor as `u64::MAX`: refuse every document
> that boot, so the node starts unready and denies client calls while still replicating. That fails
> closed on authorization without failing closed on consensus, and it is a one-line change in
> `PolicyLoader::new`. It was not taken for two reasons. Operationally it produces a very confusing
> failure — a node refusing its current, valid, correctly-signed policy because of an unrelated disk
> read. More seriously, the failure mode is not independent across nodes: a disk fault is, but a
> *code* fault in the read path — a decode change, a new format, a bug — is not, and the strict
> option would then refuse every document on every node at once and take the whole cluster's client
> plane down, triggered by a read that has nothing to do with consensus. A security control that
> converts its own bugs into a cluster-wide outage is a bad trade for closing a one-boot window that
> already requires the signing key.
>
> This is a judgement call held loosely. It is recorded at this length so the next person revisits
> it on the argument rather than rediscovering it.

The note also covers the G-06 cluster binding, the G-09 durable floor and its `state_meta`
placement, the write-after-adopt ordering, the `Ephemeral` case, G-13, and the owed metric.

## Git

No git state was changed. Nothing committed, added, stashed or checked out.
