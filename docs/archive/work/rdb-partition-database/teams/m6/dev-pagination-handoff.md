# dev-pagination handoff — G-04 and M6-81

**Recommended status: COMPLETED.**

Both gaps are closed on the working tree. No commits, no git state changes.

---

## Outcome

| gap | state |
|---|---|
| **G-04** — a follower's continuation refusal carries no leader hint | **closed**, product change + wire change + test |
| **M6-81** — no row holds a pin across a compaction | **closed**, test only (no product fix needed) |

G-04 needed an escalation first: it cannot be done inside `config-engine`. The lead ruled
option (a) widened and gave me the whole vertical.

---

## Files changed

| file | what |
|---|---|
| `crates/config-core/src/error.rs` | `PageTokenExpired` gains `hint: Option<LeaderHint>`; `Display` mirrors `NotLeader`'s; `page_token_expired()` defaults it to `None` |
| `crates/config-engine/src/pagination.rs` | `open()` takes `&ConfigNode`; the `node` refusal routes through a new `reject_to_leader` |
| `crates/config-grpc/src/error.rs` | emits the two leader trailers on `PageTokenExpired`, reads them back, + 2 unit tests |
| `crates/config-engine/tests/m6_pagination.rs` | the two new rows, + one forced `..` |
| `crates/config-server/tests/m6_pagination_e2e.rs` | one forced `..` |

**`crates/config-engine/src/node.rs` is NOT changed.** See "what I got wrong" below.

### Forced compiler-determined fixes, all of them

Adding a field to a struct variant makes every existing struct pattern non-exhaustive (E0027).
Every one I applied:

- `crates/config-engine/tests/m6_pagination.rs`, `assert_expired`:
  `ConfigError::PageTokenExpired { reason }` -> `{ reason, .. }`
- `crates/config-server/tests/m6_pagination_e2e.rs:334`: added `..` after
  `reason: PageTokenExpiredReason::PolicyVersion`

Nothing else. `crates/config-server/tests/e2e_daemon.rs:1753` already carried `..` when I got
there — the lead applied it, as agreed. No construction site broke, because
`ConfigError::page_token_expired(reason)` is the single constructor and absorbs the default.

---

## Criterion -> test -> evidence

### 1. A continuation arriving at a follower is still refused, and the refusal carries a usable leader hint, content proved

**Row:** `g_04_a_followers_page_token_refusal_carries_the_leader_hint`
(`crates/config-engine/tests/m6_pagination.rs`)

Three-node cluster. The leader's paginator mints a real token for a real pin; the continuation
is handed to the **follower's** paginator. The refusal is asserted to still be
`PageTokenExpired{reason: node}` via the existing `assert_expired`, and then the hint is
asserted **equal to the `LeaderHint` that same follower produces on its own `NotLeader` path**
— obtained by attempting a write against the follower. So the assertion is node id *and*
client endpoint, against the node's own validated value, not against something the test
computed. `hint.is_some()` alone would have passed against a hint naming the wrong node.

The row also proves the withholding rule: a token from `NodeId(7)` presented to the **leader**
gets `reason: node` and `hint: None`, because the only leader that node knows is itself and a
self-redirect would send the caller into the same refusal.

**Red first.** With the fix suppressed (`.filter(|_| false)` on the hint lookup):

```
running 1 test
test g_04_a_followers_page_token_refusal_carries_the_leader_hint ... FAILED

---- g_04_a_followers_page_token_refusal_carries_the_leader_hint stdout ----
thread 'g_04...' panicked at crates\config-engine\tests\m6_pagination.rs:497:18:
expected a leader hint on the refusal, got page token expired: node

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 19 filtered out
```

**Wire half.** `crates/config-grpc/src/error.rs`:
`a_page_token_refusal_carries_its_leader_hint_both_ways` asserts the two trailers are the same
`retcd-leader-node-id` / `retcd-leader-endpoint` `NotLeader` uses, with the right values, and
that `error_from_status` rebuilds it as `PageTokenExpired` and **not** as `NotLeader` — the
ordering hazard the change introduced, since `failed_precondition` reads `reason` before the
hint headers.

### 2. The existing `reason: "node"` behaviour is unchanged (additive)

**Rows:** `m6_70_token_from_another_leader_is_rejected`, `m6_83_pins_do_not_survive_a_restart`,
plus `a_hintless_page_token_refusal_is_unchanged_on_the_wire` (config-grpc).

Both landed rows pass untouched. They are single-node fixtures, so the known leader *is* the
refusing node, the hint is withheld by the rule above, and the refusal is byte-for-byte what it
was. The grpc row proves a hintless refusal emits no leader trailers and round-trips to exactly
`ConfigError::page_token_expired(reason)`.

Also checked: `rg "page token expired"` across `crates/` and `docs/` outside
`config-core/src/error.rs` returns nothing, so no test or document depends on the `Display`
string I extended. The hintless rendering is unchanged anyway (empty suffix).

### 3. A pin survives a compaction that would otherwise have reclaimed the pinned revision

**Row:** `m6_81_a_pin_survives_a_compaction_that_reclaimed_its_revision`

#### How I know the compaction would otherwise have reclaimed it

This is the part the row exists for, and the naive version of it proves nothing. On this
fixture the store is `EphemeralStore`, whose `pin()` (`config-storage/src/ephemeral.rs:854`)
**clones the record map** into a `MapPin` (`config-storage/src/reader.rs:104`). A pin is
therefore structurally independent of live state. A row that merely ran a `Compact` next to a
pin would pass against a completely broken pin, because the M4 `Compact` trims the **journal**
and raises `compact_revision` — it does not touch records.

So the row makes the compaction bite, and proves it two ways before asserting survival:

1. It deletes `/p/0050..0099` — half the prefix, all of it keys the walk has **not yet
   reached** — so the pinned content and the live content genuinely differ.
2. It compacts to the revision of the last delete and asserts `floor > pinned_revision`, so the
   compaction reached *past* the pin rather than up to some earlier point.
3. It asserts the reclamation as a **refusal, not a number**: a watcher asking to replay the
   walk's own revision (`start_after_revision: pinned_revision - 1`) is refused
   `RevisionCompacted { minimum_available_revision }` with `minimum_available_revision >
   pinned_revision`. That is the storage rule at `config-storage/src/reader.rs:214-219`,
   proven from both sides by `m4_53_compacted_cursor_boundary_and_recovery`. History at the
   pinned revision is gone.
4. The counterfactual is asserted in-test: a live `List` of `/p/` now returns 50 records, not
   100.

Only then does it resume the walk, and require all 100 original keys at the pinned revision —
i.e. 50 keys the cluster no longer has, read out of a revision the journal no longer retains.

The row also carries the test plan's own half (test-plan-m6 line 782): the replicated `Compact`
applies while a pin is held, asserted as `propose_compact` succeeding.

#### How I established the test would fail if the pin did not work

There is no product fix here, so this is the whole discipline. I broke the pin in
`Paginator::next_page` — swapped `self.pins.lookup(...)` for `pin_current(...)`, so a
continuation reads live state instead of the pinned view — and ran the row. Two probes, because
the revision assertion fires before the content one:

```
running 2 tests
test m6_81_a_pinned_snapshot_does_not_block_raft_apply ... FAILED
test m6_81_a_pin_survives_a_compaction_that_reclaimed_its_revision ... FAILED

---- m6_81_a_pin_survives_a_compaction_that_reclaimed_its_revision stdout ----
panicked at crates\config-engine\tests\m6_pagination.rs:897:9:
assertion `left == right` failed: every page still reports the pinned revision
  left: 150
 right: 100
```

Then, with the pin still broken, I temporarily relaxed the revision assertion so execution
reached the key-set assertion:

```
running 1 test
test m6_81_a_pin_survives_a_compaction_that_reclaimed_its_revision ... FAILED

panicked at crates\config-engine\tests\m6_pagination.rs:906:5:
assertion `left == right` failed: the whole prefix as it was at the pinned revision,
including the deleted half
  left:  [/p/0000 .. /p/0049]   (50 keys)
  right: [/p/0000 .. /p/0099]   (100 keys)
```

Exactly the reclaimed half is missing. Both probes reverted; `git diff` on
`crates/config-engine/src/pagination.rs` shows no trace of either.

Worth stating plainly: steps 1-3 above (the compaction assertions) **still passed** under the
mutation, because they are about the store, not the pin. That is correct — they are the
precondition, and the survival assertions are the claim.

---

## Exact commands and real output

All runs on `CARGO_TARGET_DIR=.rtargets/dev-m6-pagination`, `CARGO_INCREMENTAL=0`, and all
**re-run against the current tree** after the lead landed the config-server seam hunks.

### fmt

```
$ ./scripts/gate.sh fmt
gate: target=.rtargets/dev-m6-pagination scale=3 logs=.../20260920-233810-130374
== fmt
gate: fmt OK
gate.sh fmt exit=0
```

### lint

```
$ ./scripts/gate.sh lint
gate: target=.rtargets/dev-m6-pagination scale=3 logs=.../20260920-233817-130381
== clippy
    Checking config-core v0.1.0 (...)
    Checking config-storage v0.1.0 (...)
    Checking config-engine v0.1.0 (...)
    Checking config-grpc v0.1.0 (...)
    Checking config-client v0.1.0 (...)
    Checking config-server v0.1.0 (...)
    Checking config-testkit v0.1.0 (...)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 44.52s
gate: lint OK
gate.sh lint exit=0
```

Zero warnings, whole workspace, `-D warnings`.

### test

**Read this before trusting any gate evidence dated before 2026-09-20 23:53.** My first
`gate.sh test -p config-engine` ran while the lead's scoping fix to `scripts/gate.sh` was
landing. The old script built `scope=(--workspace)` and cargo ignores a `-p` beside
`--workspace`, so that run executed the **entire workspace** under the name "config-engine" and
exited 101 on four targets that are not mine. I reported the list rather than the text, because
my own `| tail -60` had truncated the failures — my mistake, not the script's.

The run below is the re-run on the fixed script. 13 targets, all `config-engine`, and the only
doc-test set is `config_engine` — that is how I know it is actually scoped this time.

```
$ ./scripts/gate.sh test -p config-engine
gate.sh test exit=0

$ grep -cE "^     Running|^   Doc-tests" /tmp/ce-gate.log
13
$ grep -E "^   Doc-tests" /tmp/ce-gate.log
   Doc-tests config_engine
$ grep -cE "FAILED|panicked" /tmp/ce-gate.log
0
```

The `m6_pagination` target in full:

```
     Running tests\m6_pagination.rs (...\m6_pagination-77c9b0df7af45543.exe)

running 20 tests
test m6_83_pins_do_not_survive_a_restart ... ok
test m6_79_rotating_the_token_key_invalidates_outstanding_tokens ... ok
test m6_73_capability_reports_revision_pinned_pagination ... ok
test m6_71_policy_version_change_rejects_the_token ... ok
test m6_80_max_items_and_max_bytes_are_both_honoured_and_capped ... ok
test m6_70_token_from_another_leader_is_rejected ... ok
test m6_67_tampered_token_mac_is_rejected ... ok
test m6_84_list_without_a_token_keeps_exact_m3_semantics ... ok
test m6_82_pins_are_released_when_a_walk_is_abandoned ... ok
test m6_65_page_token_round_trip_returns_every_key_once ... ok
test m6_68_expired_token_is_rejected_by_ttl ... ok
test m6_77_token_is_bound_to_the_principal ... ok
test m6_72_a_backend_that_cannot_pin_refuses_instead_of_drifting ... ok
test m6_78_token_version_is_explicit_and_unknown_versions_are_rejected ... ok
test m6_69_lru_eviction_rejects_the_oldest_token ... ok
test m6_76_token_is_bound_to_the_requested_prefix ... ok
test m6_81_a_pin_survives_a_compaction_that_reclaimed_its_revision ... ok
test m6_81_a_pinned_snapshot_does_not_block_raft_apply ... ok
test m6_66_pages_are_consistent_at_one_revision_under_concurrent_writes ... ok
test g_04_a_followers_page_token_refusal_carries_the_leader_hint ... ok

test result: ok. 20 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.98s
```

Both new rows green; `m6_70` and `m6_83` — the two rows criterion 2 is about — green and
unmodified.

### config-grpc wire unit tests

```
$ cargo test -p config-grpc --lib error::
running 4 tests
test error::tests::an_ordinary_unavailable_reason_stays_out_of_the_trailer ... ok
test error::tests::the_schema_gate_refusal_carries_its_reason_trailer ... ok
test error::tests::a_hintless_page_token_refusal_is_unchanged_on_the_wire ... ok
test error::tests::a_page_token_refusal_carries_its_leader_hint_both_ways ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 26 filtered out; finished in 0.00s
```

### e2e_44, run alone on the host

`e2e_daemon` was one of the four targets the void unscoped run reported, and it is the only one
of the four that touches my change (the `reason: node` row at :1753). So I did not wave it off.
Run on its own, under the gate environment at `RETCD_TEST_DEADLINE_SCALE=3`:

```
$ ./scripts/gate.sh test -p config-server --test e2e_daemon e2e_44
gate: target=.rtargets/dev-m6-pagination scale=3 logs=.../20260921-000020-131238
== test
    Finished `test` profile [unoptimized + debuginfo] target(s) in 56.74s
     Running tests\e2e_daemon.rs (...\e2e_daemon-3fdf1bfa185b1801.exe)

running 1 test
test e2e_44_daemon_pagination_across_a_leader_failover ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out; finished in 2.46s

gate: test OK
exit=0
```

**Verdict: not mine.** The row passes on its own. Per AGENTS.md, `e2e_daemon`, `m2_crash` and
`m1_observability` are the capacity-sensitive targets that fail under host load without being
broken, and the void run had three cargo invocations sharing this host.

What I have **not** established, and am not claiming: that the other three targets
(`m6_policy_daemon`, `m1_observability`, `m2_crash`) are healthy. I did not run them — the lead
is running the full set alone on the host afterwards. All I can say about them is that none
touches pagination or `PageTokenExpired`, which is an argument from the diff, not evidence.

---

## What I got wrong, stated because the lead caught it before I did

My escalation said the engine half was "make `leader_hint` `pub(crate)` at `node.rs:1691`". That
was wrong. `ConfigNode::leader_hint()` is **already `pub`** at `node.rs:638` and delegates to
the private `NodeInner::leader_hint` at `:1691` — I had read the inner one and assumed it was
the only one. I added a second `ConfigNode::leader_hint`, which is E0592, and the workspace went
red. The lead flagged it while my own build was still compiling. Reverted immediately; `node.rs`
is unmodified and `git diff --stat` confirms it.

The useful consequence: `config-engine` needed **no** node.rs change at all. The public accessor
the paginator wanted already existed.

---

## Deviations

1. **`page_token_expired_with_hint` was written, then removed.** I first added a paired
   constructor in `config-core`. It had one caller, so it was public API for nothing. The one
   site that sets a hint now builds the variant literally, which is what `NotLeader` already
   does at `node.rs:2031`. Smaller `config-core` diff.
2. **The withholding rule is mine, not the triage's.** The triage said "attach the leader
   hint". I attach it only when `hint.node_id != self.node_id`. Without that, a single-node
   cluster and a leader refusing an old leader's token would both hand out a redirect to
   themselves, and the two landed rows would have changed — which criterion 2 forbids. Flagging
   it as a judgement call rather than a given.
3. **The grpc wire tests went in `src/error.rs`'s inline `mod tests`**, not
   `config-grpc/tests/m6_pagination.rs`. The lead granted me `src/error.rs`; the integration
   test file was not named, and the round trip being tested is `status_from_error` /
   `error_from_status` symmetry, which is a unit of that module.

---

## Risks

1. ~~`config-server/tests/e2e_daemon.rs`'s doc comment is stale.~~ **Withdrawn.** I raised this
   before re-reading the file. The lead had already updated both the doc comment (it now names
   G-04 explicitly and says why the row deliberately does not bind the hint) and the `..` at
   the match. Nothing needed.
2. **A client that branches on `NotLeader` to decide "follow the hint" will not follow this
   one** until it also reads hints off `PageTokenExpired`. That is inherent in keeping the
   refusal additive and is the documented trade: the client is no worse off than before, it
   just does not get the saving. `config-client` was not in my scope and is unchanged.
3. **No e2e row drives the hint through a real daemon.** The engine row uses two paginators
   against a real 3-node cluster, and the grpc row proves the wire encoding, but nothing joins
   them end to end. `config-server/tests/e2e_daemon.rs:1611-1758` is the natural home and it is
   the lead's file. Low value — the two halves that could drift are each pinned — but it is a
   real gap and I am naming it rather than letting it pass as covered.
4. **The G-04 row asserts against live leadership.** It captures `cluster.leader()` and then
   compares against a `NotLeader` hint taken a moment later. A leadership change between the
   two would flake. The cluster is quiet, with no fault injection, and the m4/m6 suites rely on
   the same stability throughout, so I judged this acceptable rather than adding a retry.

---

## Assumptions

- `ConfigNode::leader_hint()` is the right source: it is validated from committed membership
  plus `current_leader` and never from gossip (`node.rs:1676-1694`, ADR-0003/ADR-0009). Reusing
  it rather than deriving a second hint is why there is nothing new to keep validated.
- A `MapPin` is a genuine map clone, so the M6-81 row's isolation is real and not an artifact of
  the ephemeral backend being a no-op. Read at `config-storage/src/reader.rs:104-146`.

---

## Spotted and deliberately not fixed

- **M6-82** (pins release only by TTL, never on disconnect) — deferred by the user. Untouched.
- **M6-72** (ephemeral/rocks pagination parity unowned; the id is squatted by three `NoPin`
  refusal rows) — deferred by the user. Untouched.
- Nothing else new. Everything I found matched the triage.

## Breakage that is not mine

Reported per the lead's standing rule, not repaired:

- `crates/config-server/tests/m6_tls_daemon.rs` was red mid-run on
  `config_grpc::DEFAULT_HANDSHAKE_TIMEOUT` and `NodeOptions::tls_handshake_timeout_ms` (G-01's
  half-landed work). Green by the time of the runs above.
- `config-server/src/run.rs:1118` `PolicyLoader::new` arity and `policy.rs:1196`
  `ClusterPolicyView::undecodable` (dev-m6-policy's G-06/G-09). Both resolved by the lead's
  seam hunks before my final runs.
