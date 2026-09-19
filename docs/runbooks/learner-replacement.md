# Runbook: replacing a voter (learner lifecycle)

**Applies to:** a voter that is permanently gone, or one whose endpoint must change.
**Does not apply to:** a cluster that has lost quorum — see [quorum-loss-recovery.md](quorum-loss-recovery.md).
**Reference:** ADR-0023, spec §13.2.

## When to use this

| Situation | Action |
|---|---|
| One voter of three is permanently lost; the other two still elect a leader | This runbook |
| A voter's address must change | This runbook — remove it, then add the new address as a *fresh* node id |
| Two voters of three are lost; no leader can be elected | [quorum-loss-recovery.md](quorum-loss-recovery.md) |
| A voter is merely slow or restarting | Nothing. Watch `retcd_raft_peer_lag` and wait |

There is no endpoint-update RPC, by design: `ChangeMembers::SetNodes` can let two disjoint
quorums each believe they hold the updated address (ADR-0023, "`SetNodes` is never used"). An
address change is always remove-then-add-as-a-new-id.

## Preconditions

- You can reach the **leader**'s client plane. Every mutating `AdminService` RPC is
  leader-only; a follower answers `NotLeader` with a validated hint naming the leader.
- Your client certificate's principal is listed in `[authz] admins`.
- The replacement host has: a **fresh** node id, a **fresh, empty** data directory, and a
  certificate whose SAN carries that new node id. Reusing a retired id is refused
  (`identity_retired`); reusing a directory bound to another identity is refused by ADR-0011.

## Steps

### 1. Record the starting state

```
GetMembership{}
```

Keep the response. It names the voters, the learners, and — on the leader — per-peer
replication progress. Everything below is checked against it.

### 2. Start the replacement node

Give it a bootstrap manifest whose own `[[nodes]]` entry carries `role = "learner"`, the
cluster id, its own node id, and gossip seeds. Do **not** pass `--form`. There is no `--join`
flag. The node starts, binds nothing that serves traffic, and answers `Unavailable` until an
operator adds it — a config file never changes membership, only a committed Raft entry does.

### 3. Add it as a learner

```
AddLearner{node_id, endpoint, cluster_id}
```

Returns once the learner entry is **committed**, not once the learner has caught up. Those are
different facts and the RPC only promises the first.

### 4. Poll for catch-up

```
GetMembership{}    # repeat
```

Catch-up holds when, for the new node,

```
replication[node_id].index >= leader.last_log_index - promote_max_lag
```

`promote_max_lag` is `[membership] promote_max_lag`, default 100. `replication` is populated
only on a leader; if it is empty you are talking to a follower.

On a scraped cluster the same fact is `retcd_raft_peer_lag{peer_id="<new id>"} <=
promote_max_lag`. A lag that stops falling means the learner is not receiving — check its logs
for `identity_retired`, a cluster id mismatch, or a TLS failure, not this runbook.

### 5. Promote

```
PromoteVoter{node_id}
```

Refused with `FailedPrecondition{lag_exceeded}` if the check in step 4 does not hold *at that
moment*. That is the intended interlock: promoting a behind learner shrinks the effective
quorum margin. Re-poll and retry.

### 6. Remove the dead voter

```
RemoveMember{node_id}
```

Three committed steps run server-side: demote to learner, drop the node entry, then replicate
`RetireNode{node_id}`. A crash between any two is safe — **re-issue the same call**, each step
is idempotent.

### 7. Confirm

```
GetMembership{}
```

Expect: the new node is a voter, the old id appears in neither voters nor learners, and
`retcd_raft_peer_lag` has a series for every remaining peer and none for the removed one.

## Stuck joint configuration

`change_membership` is two round trips inside openraft (joint, then uniform). A leader crash
between them leaves the cluster in a joint configuration: it still serves traffic, but the next
membership change will not proceed.

**Detect:** `metrics.membership_config.membership().get_joint_config().len() > 1`. Admin RPC
handlers and the admin plane's periodic self-check read exactly this.

**Fix:** re-issue the same `change_membership` — the same target voter set the joint config
already encodes. It is idempotent; openraft returns early when the membership it would propose
is already effectively uniform. The current leader may not be the node that started the
transition; that does not matter, the joint config itself is the record.

**Do not** try to "clear" a joint config by hand, remove a member to force it, or edit any
data directory. There is no rEtcd-side state to repair.

## What this runbook does not do

- It does not revoke the removed node's certificate. `retired_nodes` fences the Raft and admin
  plane identity only. Certificate revocation is M6 (ADR-0028). Until then, decommission the
  old host's key material by hand.
- It does not recover a cluster that cannot elect a leader. Nothing here works without a
  leader.
