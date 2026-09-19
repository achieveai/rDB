# Runbook: break-glass policy rollback

**Reference:** ADR-0027 (OQ-57), ADR-0023, spec §15.3, §18.2.

Rolling a signed policy document **backwards** is refused by default. This runbook is the only
way to do it, and it is deliberately awkward: it needs a process restart, it is process-scoped
once taken, and every rollback it permits is audited.

Read `policy-rotation.md` first. Rolling *forward* to a re-issued document with a higher version
number is an ordinary rotation and does not need any of this.

## When this applies

A document that is already active is bad — it grants too little, or it grants too much — and the
correct content is an **older** version you still hold, signed and unmodified.

Prefer the ordinary path whenever you can: re-issue the old content under a **new, higher**
version number, sign it, and deploy it as a normal rotation. That needs no flag, no restart and no
break-glass audit trail. Use this runbook only when you cannot sign a new document right now —
typically because the signing key is unavailable and the old artifact is all you have.

## Why it is refused by default

Version monotonicity is what makes a signed document replay-proof. Without it, an attacker who can
place files on a node can re-install any historical document — including one from before a
principal was removed — and the signature is still perfectly valid, because it always was. The
version check, not the signature, is what makes yesterday's grants stay in yesterday.

## The procedure

1. **Record the decision.** The rollback is audited by the node; who authorized it is not. Write
   down the incident, the version you are leaving, the version you are returning to, and why a
   forward re-issue was not possible.

2. **Confirm the target artifact.** Both files, unmodified, signed by a key still listed in
   `[authz] trust_keys`. A rollback to a document that fails verification is refused for the
   verification reason and never reaches the version check.

3. **Restart the node with the flag:**

   ```
   config-server --config /etc/retcd/node.toml --break-glass-policy-rollback
   ```

   One node at a time, checking readiness between each. The flag is per process; nodes without it
   keep refusing rollbacks, which is the desired state for every node you are not repairing.

4. **Place the older document and reload:**

   ```
   retcdctl admin reload-policy --endpoint node1:8443
   ```

5. **Verify** on each node:

   ```
   curl -s localhost:9000/health | jq '{policy_version, policy_state}'
   ```

   and that the audit line is present:

   ```
   policy_loaded{version=<old>, previous_version=<new>, break_glass=true, source="rpc"}
   ```

6. **Disarm.** Restart each node **without** the flag as soon as the incident is closed. Until you
   do, `retcd_break_glass_active` reads 1 on that node and it will accept any further rollback
   without a second decision.

## The flag is process-scoped, not one-shot

Once set, it permits **every** rollback that process performs, not just the first (OQ-57). Both
semantics are defensible; this is the one implemented, and it is why step 6 is part of the
procedure rather than a suggestion. A one-shot flag that silently re-arms on the next restart
would be worse: an operator would believe the window had closed when it had not.

## Alerting

```
retcd_break_glass_active == 1        # a node still running with the flag
retcd_policy_rollbacks_total         # any increase, on any node
```

Alert on both. The first should be true only during an incident; the second should be flat
forever. A rollback on a node whose gauge reads 0 is impossible by construction, so a lone
increment of the counter means a node was restarted with the flag and nobody said so.

## After the incident

- Re-issue the intended content as a **new, higher** version and roll it forward normally, so the
  cluster is not sitting on a version number it has already used.
- Confirm every node is back above the rolled-back version and converged
  (`retcd_policy_converged_version == retcd_policy_version` everywhere).
- Confirm `retcd_break_glass_active` is 0 on every node.
