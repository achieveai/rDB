# Runbook: quorum loss

**Reference:** spec §14, §19.11, ADR-0023, ADR-0024.

## First: decide which problem you have

Quorum loss is expensive to recover from and cheap to misdiagnose. Answer this before doing
anything:

**Can the surviving nodes still elect a leader?**

Check any survivor's `/health`, or `retcd_raft_leader` across the cluster. Exactly one node
reporting `1` means you have a leader.

| Survivors form a quorum? | This is | Go to |
|---|---|---|
| Yes — a leader exists | A lost *member*, not lost quorum | [learner-replacement.md](learner-replacement.md) |
| Yes, but no leader for less than about one election timeout | An election in progress | Wait |
| No — majority of voters permanently gone | Quorum loss | Below |
| No, but the nodes are only *unreachable* | A network partition | Fix the network. Do **not** restore |

A three-voter cluster tolerates one loss. Losing one voter is the learner-replacement runbook,
not this one. Restoring from backup when a learner replacement would have worked throws away
every write since the last backup, for nothing.

**A partition is not a loss.** If the nodes are alive but cannot see each other, restoring
creates a second writable authority for as long as it takes you to notice — the one outcome
spec §19.11 exists to prevent. Confirm the hosts and their disks are genuinely gone before
continuing.

## If it is genuinely quorum loss

There is no way to recover the existing cluster in place. rEtcd has no "force new cluster" flag
and will not gain one: an unsafe membership override is exactly the mechanism that produces two
writable authorities. Recovery is a restore from backup into a **new** cluster identity.

1. Confirm irrecoverability: the majority of voters have lost their data directories, or their
   hosts are gone and not coming back.
2. Declare recovery mode and block client traffic (spec §14 step 1).
3. Stop and fence any survivor (step 2). Do **not** use `RemoveMember` — the old cluster is
   being superseded, not reshaped. Keep the survivors' data directories; do not delete them
   until the restore is validated.
4. Follow [backup-restore.md](backup-restore.md) from its step 3 onward.

### If a survivor is more current than your newest backup

A stopped survivor's data directory can itself be backed up:

```
config-server backup --data-dir <survivor dir> --out <staging> --name from-survivor
```

The node must be stopped. Verify the artifact, compare its manifest `revision` against your
newest scheduled backup, and restore from whichever is higher. This is usually the difference
between losing an hour of writes and losing none.

### Data loss is expected

Everything committed after the chosen backup's `revision` is gone. Record the number: spec §14
step 10 asks for it, and clients will ask what they lost. Restore sets
`compact_revision = revision`, so watches cannot replay across the gap — clients must relist.

## After recovery

- Every client needs the new endpoints and new credentials. The old certificates will not work
  against the new cluster id, which is intentional.
- Watch `retcd_raft_leader`, `retcd_raft_peer_lag` and `retcd_cluster_revision` until the new
  cluster is forming and replicating normally.
- Do not bring old hosts back "just in case." Their cluster id no longer matches, so they
  cannot join, but a half-running old cluster still answering a stale DNS record is a real
  incident on its own.
