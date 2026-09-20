# Runbook: rotating the signed policy document

**Reference:** ADR-0027, ADR-0026, spec §15.3, §19.9, §19.12.

Applies to a cluster running `authz.mode = "signed"`. Under the default `authz.mode = "static"`
nothing in this runbook exists: there is no document, no signature and no poller, and the M3
`[[grant]]` allowlist is still the whole policy.

## The artifact

Two files that travel together:

| File | Contents |
|---|---|
| `policy.json` | the document: `version`, `issued_unix_ms`, `grants`, `admins` |
| `policy.json.sig` | the detached envelope: `envelope_version`, `key_name`, `version`, `hash`, `signature` |

The signature is over `sha256(document bytes) || version.to_le_bytes()`, so a signature cannot be
moved onto a different document *or* relabelled with a different version. `policy.json.sig` is the
default path; `[authz] signature_file` overrides it.

```toml
[authz]
mode = "signed"
policy_file = "/etc/retcd/policy.json"
# signature_file defaults to "<policy_file>.sig"
poll_interval_secs = 10

[[authz.trust_keys]]
name = "ops"
public_key = "<64 hex characters of an ed25519 public key>"
```

`[authz] admins` is **ignored** under signed mode and the node says so once at startup: the admin
set is the document's `admins` list and nothing else. Otherwise file-write access to the TOML
would buy admin without touching the signed artifact (M6-40).

## Rotating

1. Author the new document with `version` **strictly greater** than the active one. Confirm the
   current version first — every node publishes it:

   ```
   curl -s localhost:9000/health | jq '{policy_version, policy_state, ready}'
   ```

2. Sign it and place both files. Write them by rename, not in place: a poll tick that lands in the
   middle of a two-chunk write reads a half-written document, and although that is refused
   (`hash_mismatch`), the refusal is noise an atomic rename avoids.

3. Either wait one `poll_interval_secs`, or reload immediately on each node:

   ```
   retcdctl admin reload-policy --endpoint node1:8443
   ```

   `ReloadPolicy` is admin-only, decided against the **currently active** document — so an
   operator removing their own admin grant still gets to install the document that removes it,
   and is locked out only from the next rotation (OQ-58).

4. Watch the rollout:

   ```
   retcd_policy_version                  # per node, should reach the new version everywhere
   retcd_policy_converged_version        # lags until every voter has reported
   retcd_policy_reload_failures_total    # by reason; any increase is step 5
   ```

## What a rotation does to live traffic

Between the first node adopting version N+1 and the last voter reporting it, every node that has
N+1 evaluates **changed prefixes** against `allowed(N) ∩ allowed(N+1)` and unchanged prefixes
against N+1 alone (§15.3). Practically:

- a **removed** grant takes effect immediately, on the node that saw it;
- an **added** grant does not take effect until convergence completes — a request under it is
  refused with the typed reason `policy_converging`, which is on the wire as the `retcd-reason`
  trailer, not as an ordinary prose denial;
- an unchanged prefix behaves identically throughout.

Watches on a changed prefix are **terminated** with `PERMISSION_DENIED` and
`retcd-reason: policy_changed` before any event under the new version is enqueued for them.
Clients re-open; the termination is expected, not an incident. Watches on unchanged prefixes are
not disturbed — if a rotation terminates every stream in the cluster, that is a defect, not the
design.

Outstanding list page tokens issued under the old version are refused with `PageTokenExpired`
(`reason = "policy_version"`); clients restart the walk.

## Convergence that does not complete

`retcd_policy_converged_version` staying below `retcd_policy_version` means some voter has not
reported the new document. An unknown version counts as lagging — never as "probably fine" — so
the intersection stays in force and newly granted access stays refused.

1. Find the node: compare `/health`'s `policy_version` across all voters.
2. The usual cause is that the files did not reach that node, or reached it in one piece only.
   Its log names the reason: `policy_rejected{reason, source}`.
3. Fix the files there. No restart is needed; the next poll adopts and convergence completes.

The cluster keeps serving throughout: unchanged prefixes are unaffected and writes keep applying
(§19.12 — a policy rotation must never block indefinitely).

## Refusal reasons

Each is a closed-set token on `policy_rejected` and on
`retcd_policy_reload_failures_total{reason}`. The active document is **retained** in every case:
"fails closed" means *does not adopt*, not *forgets what it had*.

| Reason | Meaning | Fix |
|---|---|---|
| `policy_file_missing` | the document is absent or unreadable | deliver it; the next poll recovers |
| `signature_file_missing` | the envelope is absent or unreadable | deliver it; usually a half-finished deploy |
| `signature_invalid` | the envelope is malformed, or the signature does not verify | re-sign; check the file was not truncated |
| `untrusted_signer` | a valid signature by a key not in `[authz] trust_keys` | add the key (see below) or sign with a trusted one |
| `version_binding` | the envelope's version does not match the document's | the signature file belongs to a different document |
| `hash_mismatch` | the document was edited after signing | re-sign the exact bytes being deployed |
| `rollback` | the document's version is at or below the active one | bump the version; a genuine rollback is the break-glass runbook |
| `parse_error` | the document did not parse | fix the JSON |

## Rotating the signing key

`[authz] trust_keys` is a set, so the key rotates without a flag day:

1. Add the new key beside the old one and restart each node (trust keys are read at config load).
2. Sign the next document with the new key. It is accepted everywhere.
3. Remove the old key on the following restart. A document signed by it is then
   `untrusted_signer`.

Never remove the old key in the same step that starts signing with the new one: a node that has
not restarted yet would refuse every document until it did.

## A node with no valid policy

The node stays up and its **peer plane is unaffected** — it votes, replicates, applies and may even
be the leader. It is unready for client and admin traffic, and answers both with `UNAVAILABLE`
rather than `PERMISSION_DENIED`: it is declining traffic, not making an authorization decision, and
the difference is what lets a client retry elsewhere. `/health` reports
`policy_state = {"state": "no_valid_policy", "reason": ...}` and `ready = false`.

Recovery is to deliver a valid document. No restart, no re-election, no membership change.
