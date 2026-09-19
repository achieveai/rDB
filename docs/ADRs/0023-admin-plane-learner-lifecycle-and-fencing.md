# ADR-0023: Admin plane, learner lifecycle, and fencing

**Status:** Accepted
**Date:** 2026-09-18
**Spec:** §13.2, §18.2 (audit), §19.1, §19.8, §19.9, §20 (Operations), §21 M5

## Context

Spec §13.1 fixed M0–M3 to one explicit, static three-voter genesis with no resize and no voter
replacement (ADR-0011). §13.2 lifts that for M5: authenticated learner addition, catch-up,
promotion, removal, and fencing, using a new Node ID, fresh storage, and a new certificate for any
replacement — never `ChangeMembers::SetNodes` (spec §13.2's own warning, confirmed by the research
note's split-brain example, §3.5). This ADR is the admin surface that drives that sequence and the
fencing that keeps a removed or replaced node from rejoining.

## Decision

### `AdminService` on the client plane

- New proto `proto/retcd/v1/admin.proto`, `AdminService`, served on the **client-plane** listener
  (mTLS, ADR-0010) — not a third listener. RPCs: `GetMembership`, `AddLearner{node_id, peer_endpoint,
  client_endpoint, cluster_id}`, `PromoteVoter{node_id}`, `RemoveMember{node_id}`, `TriggerSnapshot`,
  `Backup{dest_dir}` (ADR-0024), `ReloadPolicy` and `ReloadTls` (both reserved, M6, ADR-0027/0028 —
  proto tags allocated, RPC bodies refuse `UNIMPLEMENTED` until then, following ADR-0010's existing
  pattern for reserved tags).
- Authorization: the static allowlist (ADR-0012) gains a top-level `admins = ["<principal>"]` list
  in the same TOML policy file. Only a principal named there may call any `AdminService` method;
  every other authenticated principal gets `PermissionDenied` before the handler runs, matching the
  existing pre-handler rejection logging rule (ADR-0010). `admins` is orthogonal to `[[grant]]`
  entries — an admin principal needs no separate KV grant to administer the cluster, and a KV grant
  confers no admin rights.
- Every call is audited: `admin_op { op, principal, target, outcome }` (ADR-0013's structured
  logging, no key/value bytes — there are none here — but `target` may name a node id, which is not
  key material and is not redacted).

### Sequence enforced server-side (spec §13.2, research §3)

`AddLearner` does not return until the RPC's own work — proposing the learner — is committed; it
does **not** wait for catch-up, because catch-up cannot be proven from `add_learner`'s return value
at all (see below). The full server-enforced sequence:

1. `AddLearner{node_id, endpoint, cluster_id}` → validates `cluster_id` matches this cluster, then
   calls `raft.add_learner(node_id, node, blocking=false)`. `blocking=false` is deliberate: research
   §3.1 shows `add_learner(.., blocking=true)`'s wait result is logged and then **discarded**
   (`impl_raft_blocking_write.rs:154-168` — trap T7), so a blocking call would buy nothing but a
   longer RPC. `retain` is hard-coded `true` inside openraft's `add_learner` regardless of what
   rEtcd passes (research §3.1) — not a rEtcd choice, a fact about the API.
2. The caller polls `GetMembership` (which also reports replication lag) until the learner has
   caught up. Catch-up is proven **only** by the leader's own metrics, never by `add_learner`'s
   return value: `RaftMetrics.replication[node_id].index >= leader.last_log_index -
   membership.promote_max_lag` (config, default `100`; research §3.3's exact predicate, mirrored
   observably since `Raft::wait()` has no "is this learner caught up" convenience). `replication` is
   `Some` only on a leader (research §3.3), which is consistent with admin RPCs only being served by
   the leader (see Leadership, below).
3. `PromoteVoter{node_id}` is refused with `FailedPrecondition{lag_exceeded}` unless the lag check
   above currently holds; when it holds, the server calls `change_membership(AddVoterIds({node_id}),
   retain=true)`.
4. `RemoveMember{node_id}` runs three committed steps (ruling M5-R5, 2026-09-18, supersedes the
   earlier single-step text): `change_membership(RemoveVoters({node_id}), retain=true)` demotes the
   node to a learner so the joint config never loses a replicating member mid-flight; then
   `change_membership(RemoveNodes({node_id}), retain=false)` drops the node entry entirely (research
   §3.2's `retain` semantics); then, once that commits, the leader proposes the replicated command
   `RetireNode { node_id }` (envelope v2 `op = 4`, reserved by ADR-0019's note). A crash between any
   two steps is a test row; the operator re-issues `RemoveMember` and each step is idempotent.
   `RetireNode` allocates no public revision and produces no event, the same class of command as
   `Compact` (§19 invariant 3).
5. `TriggerSnapshot` calls `raft.trigger_snapshot()` (ADR-0022); `Backup` is ADR-0024.

`change_membership` is two sequential round trips inside openraft (joint, then uniform; research
§3.1, trap T8) — this ADR does not add a third rEtcd-level phase, it relies on openraft's two and
adds crash recovery for a crash between them (see Joint-config crash recovery, below).

### Fencing

- `state_meta/retired_nodes: BTreeSet<NodeId>`, replicated via `RetireNode` in the same state batch
  discipline as every other apply-time write (ADR-0008). Once committed, it is durable and identical
  on every voter.
- **Peer plane:** a peer RPC whose envelope `from_node_id` names a retired id is refused before any
  payload is decoded, logged `identity_retired` (mirrors ADR-0010's existing pre-handler rejection
  pattern for identity failures).
- **`AddLearner`:** refuses a `node_id` that appears in `retired_nodes` with the same
  `identity_retired` reason — a retired id can never be reused, closing the loop a naive "just
  re-add it" recovery would otherwise open.
- New members must present a **fresh** `NodeId`, a **fresh** data directory, and a **new**
  certificate whose SAN carries the new node id (`peer_server_domain`, ADR-0010's M3 note). ADR-0011
  already refuses a directory whose stored identity mismatches its node's config, so directory reuse
  under a stale identity is already closed; fencing here closes the complementary path (a *new*
  directory presenting a *retired* id).
- Certificate fencing proper — revocation, CRL, or short-lived reissue that stops a still-valid old
  certificate from dialing in — is explicitly **not** this ADR; it is CA-level and belongs to M6
  rotation (ADR-0028). `retired_nodes` fences the Raft/admin-plane identity; it does not revoke a
  certificate.

### Joining node: no `--join`, learner-role manifest

- `config-server --join` is **not** added. A fresh node starts with an empty data directory and a
  manifest whose `[[nodes]]` entry for itself carries `role = "learner"` (a new manifest field,
  additive — existing `role = "voter"` entries are unaffected and default when absent, preserving
  ADR-0011's M0–M3 manifests unchanged). It waits idle, serving `Unavailable` (ADR-0011's existing
  behavior for an unformed node), until an operator calls `AddLearner` against the leader.
- This preserves ADR-0011's "no self-forming" rule exactly: a learner-role manifest tells the node
  what it *may become*, not what it *is* — the only thing that changes cluster membership is a
  committed Raft entry (§19 invariant 1), never a config file read locally.
- The joining node's manifest carries the cluster id, its own node id, and gossip seeds only — no
  voter list, because it is not forming anything.

### Interrupted transitions

Every phase of add → catch-up → promote → remove → retire is a candidate crash point. Rows exist
for the leader crashing:

- after `AddLearner` commits but before catch-up is observed — the new leader (if any) re-serves
  `GetMembership`; the learner is already a committed member, catch-up proceeds unaffected by the
  leadership change.
- after joint config commits but before uniform config commits (`change_membership`'s own
  documented failure mode, research §3.1: *"If it loses leadership or crashed before committing the
  second uniform config log, the cluster is left in the joint config"*) — see Joint-config crash
  recovery.
- after `change_membership(RemoveVoters)`'s uniform config commits but before `RetireNode` commits —
  the removed node is no longer a voter (safe: research §3.4 quotes the dynamic-membership doc
  verbatim, *"the node to remove can be safely terminated"* once the uniform config commits) but is
  not yet fenced; the new leader (or the same leader, once it notices) re-proposes `RetireNode` for
  the same `node_id` — idempotent, since `retired_nodes` is a set.
- after `RetireNode` commits — steady state, nothing to recover.

Each row asserts recovery to a **consistent, committed** membership; none asserts recovery to a
*specific* membership, because which phase completed before the crash is exactly what determines
the correct end state, and openraft's own recovery (below) determines that from committed log
content, not from rEtcd retrying blindly.

### Joint-config crash recovery (research §3.6, trap T8)

Nothing membership-specific is persisted beyond the membership log entries themselves plus whatever
`apply()` stores for `applied_state()`. On restart, `StorageHelper::get_membership()` reconstructs
committed and effective membership by scanning the log backwards for at most the last two
membership entries (research §3.6, citing `helper.rs:243-305`) — **rEtcd stores nothing extra for
this**; it is openraft's own reconstruction, and rEtcd's obligation is only what research §3.6
already states for ADR-0022: `apply` must store the membership from `RaftEntry::get_membership()`
and that membership must survive purge (a store that drops membership on install/apply loses the
cluster — covered by ADR-0022's install-phase-3 batch, which writes `membership` together with
`last_applied`).

Detection: any admin RPC handler, and the admin-plane's own periodic self-check, reads
`metrics.membership_config.membership().get_joint_config().len() > 1` (research §3.6's exact
detection expression). When true, the leader (whichever node currently holds leadership — it may
not be the node that initiated the original transition) **re-issues the same `change_membership`
call** with the same target voter set that the joint config already encodes — idempotent by
openraft's own rule (`change_membership` returns early if the membership it would propose is
already effectively uniform, research §3.1). No new rEtcd state records "a transition was in
progress"; the joint config itself, visible through committed membership, is the record.

### `SetNodes` is never used

Endpoint changes for an existing node id are not supported by this ADR. Per spec §13.2 and research
§3.5's split-brain example (`ChangeMembers::SetNodes` can let two disjoint quorums each believe they
hold the updated address, electing two leaders in the same term-space), any endpoint change is
remove-then-re-add-as-a-fresh-learner, going through the full sequence above, never a direct
address update. This is enforced at the API boundary: `AdminService` has no RPC that accepts a new
endpoint for an existing node id.

### Leadership

Every `AdminService` RPC that mutates membership requires this node to be the current leader
(`ensure_linearizable()`, ADR-0009); a non-leader returns `NotLeader { validated_hint }` before any
admin work, the same pattern `Watch` registration uses (ADR-0020 step 3). `GetMembership` is a read
and is served leader-linearizable for the same reason `Get`/`List` are (ADR-0009) — a stale
membership view would make the catch-up polling loop in step 2 unreliable.

## Consequences

- `promote_max_lag`'s default of `100` is a starting operational value, not a proven capacity claim
  (consistent with ADR-0020's stance on its own admission defaults) — it may need tuning once real
  catch-up timing evidence exists (M6, ADR-0031).
- Because `add_learner`'s wait result is unusable (trap T7), every learner-addition tool built on
  top of `AdminService` (a future CLI helper, an operator script) must poll `GetMembership` itself;
  this ADR does not hide that behind a blocking RPC, because doing so would either lie about
  catch-up (mirroring openraft's own discarded-result bug) or reinvent a bounded wait with its own
  timeout semantics inside the RPC, which belongs in the caller.
- `RetireNode` sharing `Compact`'s "no revision, no event" class means neither shows up in a client's
  watch stream or revision history — both are cluster-administration events, not data events, and
  this ADR does not change that boundary.
- No admin RPC exists to reconfigure `admins` itself at runtime in M5; the allowlist file is the only
  way to change who may administer the cluster, consistent with ADR-0012's existing "missing or
  unparsable policy → node is unready" fail-closed posture. Signed, reloadable RBAC (including admin
  grants) is ADR-0027 (M6).

## Verification

- M5 rows for: `AddLearner` refuses a retired node id; a retired node's peer RPCs are rejected with
  `identity_retired` before payload decode; `PromoteVoter` refused with `lag_exceeded` below the
  configured threshold and succeeds once the replication-map predicate holds; `RemoveMember` followed
  by `RetireNode` leaves the node unable to rejoin under its old id even with a fresh directory;
  crash injection at each of the four interrupted-transition phases above, each followed by a new
  leader (or the same leader after restart) reaching a consistent committed membership; joint-config
  detection and re-issue recovers a crash between joint and uniform commits (trap T8); an
  unauthorized (non-`admins`) principal is refused `PermissionDenied` on every `AdminService` method
  before the handler runs; every admin call produces exactly one `admin_op` audit line.
- Learner catch-up against a leader that has already purged (ADR-0022, research A11) is covered
  jointly with ADR-0022's Verification section: the leader must first purge (tiny
  `logs_since_last`/`logs_to_keep`), then a lagging learner must be caught up via snapshot install,
  not `AppendEntries` alone.
- Test plan: `docs/testing/test-plan-m5.md`, M5 rows for admin plane, learner lifecycle, and fencing
  (row IDs assigned when that plan is written).

## Notes


### 2026-09-18 — implementation notes (dev-admin, M5)

Six points the implementation had to settle. Each one either supersedes text above or records a
decision an operator can observe.

**1. The removal sequence is the ruling's, not this document's.** The body still describes
`RemoveVoters(retain = false)` and an `AddLearner { .., cluster_id }` shape. Ruling M5-R5
supersedes both. What shipped is three replicated steps in this order and no other:
`RemoveVoters({id})` with `retain = true` → `RemoveNodes({id})` → `Command::RetireNode { id }`.
`retain = true` is not a preference: openraft's `RemoveNodes` refuses an id that is still a
voter with `LearnerNotFound`, so the id has to survive step 1 as a learner for step 2 to be able
to remove it at all.

**2. A re-issued `RemoveMember` finishes an interrupted sequence instead of refusing it.**
`RemoveMember` against an id that is *out of membership and not retired* falls through to step 3
and fences it. That state is exactly what a crash between steps 2 and 3 leaves behind — the node
is out of the configuration and its identity is still live — and it is the one intermediate state
that is actually dangerous. Answering `NotAMember` there made the recovery this ADR itself
prescribes ("re-issue `RemoveMember`") permanently impossible.

The cost is that removing an id which was never a member now fences it rather than reporting a
typo. That is the right trade: once step 2 commits there is no evidence left that distinguishes
the two cases, and fencing an id nobody used is harmless and idempotent, whereas leaving a
removed node unfenced is not.

**3. `admin_op` is emitted by the admin plane only.** The engine emits its own line for the same
operations, but named `admin_op_local`. The requirement here is exactly one `admin_op` record per
operation *naming the principal*, and only the transport knows the principal — an engine-level
line called `admin_op` produced a second, principal-less record for every gRPC call, so a search
for "who removed node 3" returned two hits and one of them could not answer the question.
`admin_op_local` exists so that an embedder driving `ConfigNode` directly still leaves a trail;
it carries no principal by construction.

**4. Insecure mode's principal is `dev`, and the allowlist still applies to it.** An insecure
listener has no certificate to derive a name from, so every caller is `Principal::development()`,
named `dev`. That is a development affordance, not a hole: `dev` must still appear verbatim in
`[authz] admins` to reach a single method, so an insecure node with the default (empty) allowlist
serves no admin RPC at all. An operator who wants the admin plane on an insecure node has to
write `admins = ["dev"]` and thereby say so out loud.

**5. The refusal outcome is `rejected`, not `denied`.** `admin_op` refusals carry
`outcome = "rejected"` and a stable `reason` (`not_an_admin`, `unauthenticated`, `not_leader`,
`node_retired`, `learner_lagging`, …). `rejected` is the outcome vocabulary ADR-0013 already uses
across the client and peer planes; the cause travels in `reason`, where it can be greppable
without inventing a second outcome word for one plane.

**6. `InstallSnapshot` is served from M5 on.** ADR-0008's statement that the peer plane never
serves it is stale: learner catch-up from a leader that has already purged depends on it.

Verified by `config-engine/tests/m5_membership.rs` (7 rows), `config-grpc/tests/admin_plane.rs`
(M5-50/51/52) and `config-grpc/tests/peer_plane.rs`.

**7. `role = "learner"` in a bootstrap manifest is a statement of intent, and a refusal to
form.** A manifest entry may now carry `role = "voter"` (the default, and what an absent key
means) or `role = "learner"`. A learner-role entry never creates a learner: only a committed
Raft entry changes membership, so the node it names waits idle until an operator calls
`AddLearner` against the leader. What the role *does* do is refuse two things that would
otherwise silently produce a cluster disagreeing with the document that created it:

* `--form` on a node the manifest calls a learner exits 2 with `reason = "learner_cannot_form"`.
  It is refused in `run`, immediately after the manifest verifies and **before either plane
  binds**, so such a node never answers an RPC on its way out.
* `restore` with a manifest that gives the node being minted the learner role exits 2 with
  `reason = "manifest_rejected"`. A restored node forms the new cluster, so it has to be a voter
  in the manifest that mints it.

A manifest whose entries are *all* learners is refused outright ("manifest names no voters"),
because it creates nothing. Learner entries are excluded from the formation plan's voter set,
so a mixed document forms exactly the voters it names.

Verified by `config-server/tests/m5_learner_e2e.rs` (M5-86, M5-87) and
`config-server/tests/m5_admin.rs::m5_86b_restore_refuses_a_manifest_that_makes_this_node_a_learner`.

### 2026-09-19 — retired set travels in the snapshot header (finding C5B-18, ruling M5-R21)

**8. The fence converges through snapshots.** "Once committed, it is durable and identical on
every voter" in Fencing above was true of `RetireNode` and false of the only other way a node
acquires state: `InstallSnapshot`. `state_meta` is not in the snapshot body (ADR-0022), so a node
that was down while the cluster retired an id, and was then caught up by an install rather than
by `AppendEntries`, never learned the retirement — and no later event would teach it, because
the entry it missed may have been purged. Its peer plane and its `AddLearner` both re-admitted
the fenced identity, silently and for the lifetime of that node.

`SnapshotHeader` now carries `retired_nodes`, and install **unions** it into the receiver's set
(ADR-0022's note of the same date has the mechanism and the format reasoning). Union, not
replacement: the fence only ever widens, which is the only direction consistent with "a retired
id can never be reused". Certificate fencing is still out of scope here and still M6/ADR-0028 —
what this closes is the Raft/admin-plane identity fence lapsing on a node that learned its
membership from a file. Row: `m5_133_retired_set_converges_through_snapshot_install`.
