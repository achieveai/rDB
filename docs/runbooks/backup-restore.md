# Runbook: backup and restore

**Reference:** ADR-0024, ADR-0021, spec §14, §12.2.

## The artifact

A backup is three files sharing a `<name>` stem:

| File | Contents | Encrypted? |
|---|---|---|
| `<name>.snap` | ADR-0022 snapshot format: header, length-prefixed records, sha256 trailer | Optionally, AES-256-GCM |
| `<name>.manifest.json` | cluster id, recovery epoch, node id, revision, last applied, membership, record counts, sha256 of the **plaintext** `.snap`, creation time | Never |
| `<name>.manifest.sig` | Ed25519 signature over the exact manifest bytes | Never |

The manifest stays in the clear on purpose: it has to be readable to decide whether a restore
is even worth attempting, and its signature — not secrecy — is what protects it. The `sha256`
is over plaintext, so `verify-backup` can confirm the artifact matches its manifest without the
decryption key.

## Taking a backup

```
config-server backup \
  --data-dir /var/lib/retcd \
  --out /var/backups/retcd \
  --name nightly-2026-09-18 \
  --signing-key /etc/retcd/backup-signing.key \
  --encryption-key /etc/retcd/backup.key
```

- The data directory must **not** be in use by a running node. Stop the node, or back up a
  replica that is already stopped.
- `--name` defaults to `backup-<unix-millis>`.
- The keys default to `[backup] signing_key_file` / `encryption_key_file` in the config file;
  the flags override them.
- The `.snap` is built **fresh** from a consistent view, not copied from whatever Raft snapshot
  happens to be on disk. A backup runs on its own schedule, independent of snapshot policy.

Watch `retcd_backup_age_seconds` to confirm the schedule is running.

> As of M5 nothing populates `retcd_backup_age_seconds` — the daemon leaves it unset and the
> series is omitted rather than exported as zero. Until it is wired, alert on backup job exit
> status instead. See [alerts.md](alerts.md).

## Verifying a backup (do this daily)

```
config-server verify-backup \
  --from /var/backups/retcd \
  --name nightly-2026-09-18 \
  --trust-key /etc/retcd/backup-verify.pub \
  --encryption-key /etc/retcd/backup.key
```

Checks, in order: manifest signature against `--trust-key`, then `.snap` sha256 against the
manifest, then — only if `--encryption-key` is given — that the file decrypts and the plaintext
still hashes to the manifest value.

`--trust-key` is **required**. There is no implicit trust store; an unverified backup is not a
backup.

Exit is non-zero with a stable reason on failure: `signature_invalid`, `checksum_mismatch`,
`decrypt_failed`. Script against those strings, not against prose.

An unverified backup discovered during a real outage is worse than no backup, because you will
spend the outage discovering it. Run this unattended, nightly.

## Restoring

Restoring always mints a **new cluster identity**. This is not a convenience; it is what makes
"two writable authorities cannot exist" (spec §19.11) true structurally. The peer plane already
refuses any RPC whose cluster id does not match, so once the identity is guaranteed to differ,
the old cluster and the restored one physically cannot exchange a log entry — with no
restore-specific code anywhere in the engine.

### Step-by-step (spec §14)

**1. Declare recovery mode. Block client traffic.**
Operational. Restore itself runs offline and opens no listener. Announce it; §14 step 10 will
ask who ran it.

**2. Stop and fence the old members.**
Operational. Do *not* use `RemoveMember`/`retired_nodes` here — the old cluster is not losing a
node, it is being superseded.

**3. Select and verify the backup.**
`verify-backup`, above. Pick the newest artifact that passes, not the newest artifact.

**4. Mint the new identity.**
A new cluster id (32 lowercase hex), a recovery epoch strictly greater than the source's, fresh
node ids, fresh data directories, fresh credentials, and a **new** ADR-0011 bootstrap manifest
for the new cluster. The refusal matrix enforces the id and epoch rules; the rest is yours.

**5. Restore.**

```
config-server restore \
  --from /var/backups/retcd \
  --name nightly-2026-09-18 \
  --data-dir /var/lib/retcd-new \
  --cluster-id <32 hex> \
  --recovery-epoch <n> \
  --node-id 1 \
  --manifest /etc/retcd/new-bootstrap.toml \
  --trust-key /etc/retcd/backup-verify.pub \
  --encryption-key /etc/retcd/backup.key
```

`--manifest-sig` and `--manifest-key` default to `<manifest>.sig` and `<manifest>.pub`; name
them only if they live elsewhere. `--data-dir` must not exist, or must be empty.

Repeat per node, with that node's `--node-id` and directory.

**6. Validate.**
`verify-backup` covered checksum and counts; restore re-checks record counts against the
manifest as it writes. Then run your own conformance smoke test — read back keys you know the
values of. "Sample hashes" in §14 is deployment-specific and is not automated here.

**7. Cut over DNS/endpoints, audited.**
Operational.

**8. Revoke the old identities before accepting writes.**
Operationally, "do not put the old cluster's certificates back into service." Automated
revocation is M6 (ADR-0028). Missing this step is structurally harmless — the new cluster id
binding already rejects the old nodes — but do it anyway.

**9. Tell clients to discard page tokens and relist before restarting watches.**
Enforced at the storage layer: restore sets `compact_revision = revision`, so a watch resuming
at or below it gets `RevisionCompacted` rather than a partial replay. Restore does not promise
watch continuity.

**10. Record RPO, RTO, revision, operator, reviewer.**

## What the restored store looks like

- `kv`, `events` and `dedup` are populated from the `.snap`.
- `cluster_revision` is **preserved** — clients see revision numbers continue, even though the
  cluster identity changed.
- `compact_revision = revision`. No retained history.
- `membership` is the **new** manifest's voter set, not the backed-up one. Formation then
  proceeds normally with `--form` against the new manifest, against an already-populated store.
- `/health` reports `restored_from: { cluster_id, recovery_epoch, revision }` — the **source**
  identity, kept for audit, for the life of that directory. Nothing in the engine ever compares
  it against a live peer; doing so would reintroduce the coupling the new identity exists to
  break.

## RPO and RTO

Spec §12.2's 60-minute figures are **provisional and not measured here**. Any number rEtcd
publishes is a timestamped dev-host measurement (`docs/evidence/`, M6), never a production
claim. Measure your own, on your own hardware, with your own data size.
