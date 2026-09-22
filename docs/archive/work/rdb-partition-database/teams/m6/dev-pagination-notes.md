# dev-pagination — research notes (G-04, M6-81)

Scope: `crates/config-engine/src/pagination.rs`, `crates/config-engine/tests/m6_pagination.rs`.

## G-04 — leader hint on a follower's continuation refusal

### What the code does today

`Paginator::open` (`crates/config-engine/src/pagination.rs:503-507`):

```rust
if token.node_id != self.node_id || token.issued_ms < self.started_ms {
    return Err(self.reject(raw_token, PageTokenExpiredReason::Node));
}
```

`reject` (`pagination.rs:516-527`) counts the miss, logs by fingerprint, and returns
`ConfigError::page_token_expired(reason)`.

`ConfigError::PageTokenExpired` (`crates/config-core/src/error.rs:295-298`) has **one** field,
`reason`. There is nowhere to put a hint.

### The existing convention (checked, not invented)

- `LeaderHint { node_id, endpoint }` — `config-core/src/error.rs:138-146`. Constructed only
  from committed membership plus mTLS identity.
- `ConfigError::NotLeader { hint: Option<LeaderHint> }` — `error.rs:198-203`.
- Wire: `config-grpc/src/error.rs:143-152` inserts `retcd-leader-node-id` and
  `retcd-leader-endpoint`, but **only** on the `NotLeader { hint: Some(_) }` arm.
- Reading it back: `config-grpc/src/error.rs:296-324` `leader_hint(status)`, which already
  drops a partial hint loudly. That helper is reusable as-is.
- Engine side: `ConfigNode::leader_hint()` already exists at
  `crates/config-engine/src/node.rs:1691-1694` (private), built on `hint_for` at `:1682-1689`
  which returns `None` when the leader has no committed client endpoint. `Paginator::next_page`
  already holds `&ConfigNode`, so the engine has everything it needs.

So the convention is clear and matching it is cheap **on the engine side**. The cost is the
error shape.

### Why this cannot stay inside config-engine

Adding `hint: Option<LeaderHint>` to `PageTokenExpired` makes every existing struct pattern
non-exhaustive (E0027). Sites found by
`rg PageTokenExpired crates/ --include=*.rs`:

| file:line | mine? |
|---|---|
| `crates/config-grpc/src/error.rs:163` (match arm) | no — dev-m6-grpc |
| `crates/config-grpc/src/error.rs:283` (construction from a status) | no — dev-m6-grpc |
| `crates/config-server/tests/e2e_daemon.rs:1750` | no |
| `crates/config-server/tests/m6_pagination_e2e.rs:334` | no |
| `crates/config-engine/tests/m6_pagination.rs:923` | yes |

And for the hint to reach a real client the trailers must be emitted for `PageTokenExpired`
too, which is `config-grpc/src/error.rs` `status_from_error`.

Construction sites do **not** break: `ConfigError::page_token_expired(reason)`
(`config-core/src/error.rs:367`) is the single helper and can default the hint to `None`.

**Escalated to lead on 2026-09-20.** Options offered: (a) I take config-core/error.rs +
config-engine, dev-m6-grpc takes config-grpc + the two config-server test files; (b) hand all
of G-04 to the config-grpc owner; (c) relax acceptance criterion 2 and return
`NotLeader { hint }` from the engine — **not recommended**, it folds one of ADR-0029's five
closed page-token reasons into `NotLeader` and breaks the two landed `reason = "node"` rows.

**Ruling: (a), widened.** I own the whole vertical: `config-core/src/error.rs`,
`config-grpc/src/error.rs`, `config-engine/src/pagination.rs`, `config-engine/src/node.rs`,
`config-engine/tests/m6_pagination.rs`, `config-server/tests/m6_pagination_e2e.rs`.
`config-server/tests/e2e_daemon.rs` stayed with the lead.

### One thing I got wrong and corrected

My escalation said the engine half needed `leader_hint` made `pub(crate)` at `node.rs:1691`.
Wrong: `ConfigNode::leader_hint()` is **already `pub`** at `node.rs:638` and delegates to the
private `NodeInner::leader_hint` at `:1691`. I had read the inner one. I briefly added a second
`ConfigNode::leader_hint`, which is E0592 and stopped the build; the lead saw the red before I
did. Reverted — `node.rs` is now **unmodified**.

### The rule the hint follows

`node.leader_hint().filter(|h| h.node_id != self.node_id)`. A node never points a caller back
at itself: a leaderless node has nothing to offer, and a leader refusing a token minted by some
*earlier* leader would otherwise hand out a redirect into the same refusal. This is also why
the two landed single-node rows (M6-70, M6-83) are untouched — on a one-node cluster the known
leader *is* this node, so the hint is withheld and the refusal is byte-for-byte what it was.

## M6-81 — a pin across a compaction

### What the landed row does and does not do

`m6_81_a_pinned_snapshot_does_not_block_raft_apply`
(`crates/config-engine/tests/m6_pagination.rs:735-790`): four pins, a 50-write burst, asserts
apply advanced, every write readable, table within its cap, and the four walks still report
their pinned revision. No compaction anywhere.

test-plan-m6 line 782 additionally wants the M4 journal `Compact` to apply and M5 log purge to
happen while pins are held.

### What a pin actually is here, and what a compaction actually reclaims

This matters, because the naive test proves nothing.

- The fixture's store is `EphemeralStore`. Its `pin()`
  (`crates/config-storage/src/ephemeral.rs:854-871`) **clones the record map** under the state
  lock and wraps it in `MapPin` (`config-storage/src/reader.rs:104-146`). A pin is therefore
  structurally independent of live state: later puts and deletes cannot touch it.
- The M4 `Compact` command trims the **journal** (retained `MutationEvent`s) and raises
  `compact_revision`. It does not delete records.

So a compaction on its own reclaims nothing the pin holds records for, and "run a Compact next
to a pin" would pass on a completely broken pin. The reclamation that is real and observable is
the **history at the pinned revision**: after `propose_compact(up_to)` with `up_to >= R`, any
other consumer asking to replay from `R` is refused `RevisionCompacted` with
`minimum_available_revision = compact_revision + 1 > R`. The rule is at
`config-storage/src/reader.rs:214-219` and is proven from both sides by
`m4_53_compacted_cursor_boundary_and_recovery` (`crates/config-engine/tests/m4_watch.rs:335`).

### Test design

1. Page one of a walk over `/p/` → pin at revision `R`, token held.
2. Delete `/p/0050..0099` — 50 of the 100 keys the walk has not yet reached. Those records
   leave live state, so the pinned content and the live content now differ.
3. `propose_compact(&principal(), up_to)` with `up_to` the revision of the last delete, which
   is `> R`. This is also the test-plan half: the replicated `Compact` applies while a pin is
   held.
4. **Prove the compaction would have reclaimed R**: `compact_revision() > R`, and a watcher
   asking to replay the walk's own revision (`start_after_revision: R - 1`) is refused
   `RevisionCompacted { minimum_available_revision }` with `minimum_available_revision > R`.
5. **Prove the pin survived it**: resume from the token, walk to exhaustion, get all 100
   original keys, every page still reporting revision `R`.
6. **The counterfactual, in-test**: a live `List` of `/p/` now returns 50 records. An unpinned
   walk could not have returned the other 50.

Helpers: `ConfigNode::propose_compact` (`node.rs:1063`), `ConfigNode::compact_revision`
(`node.rs:1022`), `Cluster::delete` (`tests/common/mod.rs:414`), `ConfigNode::watch`.
`WatchRequest` is `config_core::WatchRequest { prefix, start_after_revision,
progress_interval }`.

### Mutation proof (the row has no product fix, so this is the discipline)

Temporarily break the pin in `Paginator::next_page` — swap `self.pins.lookup(...)` for
`self.pins.pin_current(...)` so a continuation reads live state instead of the pinned view —
run the row, observe the failure, revert. Result recorded in the handoff.

## Spotted, deliberately not fixed

Nothing beyond the triage. M6-82 (pins release only by TTL) and M6-72 (ephemeral/rocks parity)
left alone per the assignment.
