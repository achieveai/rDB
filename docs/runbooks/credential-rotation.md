# Runbook: rotating TLS certificates and gossip keys

**Reference:** ADR-0028, ADR-0026, ADR-0011, spec §15.1, §18.2, §19.10, §20.

Applies to a cluster running `tls.mode = "mutual"`. Under `tls.mode = "insecure"` none of this
exists: there are no certificates to rotate, `ReloadTls` answers `UNAVAILABLE`, and
`retcd_cert_expiry_seconds` is not exported at all.

Nothing here restarts a node. A rotation that requires a restart is a rolling restart with extra
steps, and it is the thing this machinery exists to avoid.

## What rotates together

One node has **one** identity, held in three places:

| Holder | What it does with the material |
|---|---|
| client plane listener | presents the leaf to applications, verifies theirs against the CA bundle |
| peer plane listener | presents the leaf to peers, verifies theirs |
| peer dialler | presents the leaf when this node *opens* a connection to a peer |

All three are replaced by one operation. Rotating only the listeners leaves a node that serves a
new certificate and still dials with the old one — which, after the old CA is dropped, is a node
that can be called but cannot call out.

```toml
[tls]
mode = "mutual"
ca = "/etc/retcd/ca.pem"
cert = "/etc/retcd/node.pem"
key = "/etc/retcd/node.key"
watch_files_secs = 30
```

`watch_files_secs` is how often the node re-reads those three paths. Zero is refused: "never poll"
is spelled by rotating on demand, not by disabling the timer.

## Rotating a certificate

The whole procedure is: widen the trust, move the leaves, narrow the trust. Each step is safe to
stop at.

1. **Confirm what is deployed.** Every node reports the fingerprint and expiry of what it is
   actually serving, not what it read at boot:

   ```
   curl -s localhost:9000/metrics | grep retcd_cert_expiry_seconds
   ```

2. **Widen the CA bundle.** Concatenate CA-new onto `ca.pem` on **every** node and let it take
   effect. Both roots are now trusted; nothing has changed about who holds what.

   Do not go further until every node has picked this up. `retcd_tls_reloads_total` incrementing
   on each node is the confirmation.

3. **Issue and install the new leaves**, one node at a time. A peer certificate must keep its
   node's `retcd://` SAN and its `peer_server_domain(cluster_id, node_id)` — a leaf minted for a
   different node id is refused with the same typed identity error it has always been, rotation
   or not (§19.10).

   Write by rename, not in place. A poll that lands mid-write reads a truncated PEM; that is
   refused and retried a tick later, but an atomic rename avoids the noise entirely.

4. **Narrow the CA bundle.** Remove CA-old from every node and reload. This is the destructive
   step: any client or peer still holding a CA-old leaf is refused from this moment.

## Reloading on demand

```
retcdctl admin reload-tls --endpoint node1:8443
```

Admin-only, and audited as `reload_tls`. It carries no payload — the node re-reads the paths its
own configuration names, so there is no way for the call to point a node at different files.

The reply is one row per plane: `plane`, `outcome` (`reloaded` or `unchanged`), `generation`,
`cert_fingerprint`, `cert_expiry_unix`. `unchanged` means the bytes on disk are the bytes already
being served — it is a successful call, not a failure.

Waiting one `watch_files_secs` instead does exactly the same thing. The RPC only removes the wait.

## What a reload does to live traffic

Nothing, deliberately.

- **Established connections keep the material they handshook under.** TLS does not renegotiate
  mid-connection, so a client connected before the rotation keeps talking under the old
  certificate until it reconnects. Only *new* handshakes see the new material.
- **In-flight requests and watch streams are not interrupted.** A watch keeps delivering in
  revision order with no gap across a reload.
- **Pooled peer channels are dropped.** This is the one visible effect: a pooled outbound
  connection would otherwise carry withdrawn credentials for as long as it stayed up, so the
  dialler's cache is invalidated and the next RPC redials. Replication continues; no election is
  triggered by this.
- **Nothing replicated changes.** Rotation is a transport concern. No `Command` is proposed,
  membership is untouched, and `cluster_id`, `recovery_epoch` and `retired_nodes` are the same on
  both sides of it.

## A reload that fails

A refusal is a refusal to *change*. Nothing is swapped until the whole set has read and compiled
into a serveable profile, so a node that refuses a reload is still serving exactly what it served
before and is still `ready`.

| `reason` | What happened |
|---|---|
| `tls_file_unreadable` | one of `tls.ca`, `tls.cert`, `tls.key` could not be read |
| `tls_material_unusable` | the set read, and does not compile: a key that does not match its certificate, a malformed PEM, a chain that does not reach the configured CA |

Both are logged as `tls_reload_failed{source, plane="all", reason, recovery, detail}` and counted
by `retcd_tls_reload_failures_total{reason}`. Grep for the literal `plane="all"`; it is the stable
label for a rotation refusal and does not name a specific plane.

`plane="all"` does **not** promise that nothing changed. Both reasons above are raised by the
pre-checks, before any plane is touched, and those are node-wide. But `try_reload` swaps the planes
in a loop, so a failure raised *inside* that loop returns after an earlier plane has already taken
the new material. The window is narrow — the same bytes compiled successfully moments earlier — and
it is self-correcting rather than sticky: the node's record of what it is serving is only written
once every plane took the material, so the next reload retries all of them. That is what the
`recovery` field says. If a refusal ever surprises you, re-run the reload rather than assuming the
node is untouched.

The poller keeps polling after a failure. A half-written file is a transient condition, and
stopping the poller would turn it into a permanent refusal to rotate.

## Rotating with one voter unavailable

This works, and it is the reason for the widen/move/narrow ordering.

While a voter is down, complete steps 2 and 3 on the nodes that are up. The down node rejoins on
its **old** leaf, because the bundle still trusts CA-old, and catches up normally. Issue it a new
leaf once it is back, then do step 4.

If you narrow the bundle (step 4) while the node is still down, it will be refused when it
returns: its peer connections fail with the typed identity error and `reason="untrusted_peer_ca"`,
it never appears as a healthy peer in gossip, and it receives no log entries. The cluster keeps
quorum and keeps serving throughout — this is fencing working, not an outage — but the only way
out is to issue that node a leaf from CA-new.

This is also how you fence a node deliberately: removing a CA revokes every certificate under it
at once, which is blunter than revoking one identity and is the tool that exists.

## Expiry warnings

`retcd_cert_expiry_seconds{plane}` is the seconds remaining on the leaf each plane is serving. It
goes negative once the certificate has expired rather than clamping at zero, because "-2 days" is
a fact an operator can act on and a clamped zero reads as "expires today" forever.

A `cert_expiring{plane, days_remaining}` line is logged at `warn` the first time a plane's
certificate crosses 30 days remaining, and then **not again** for that plane until a rotation
moves it back above the threshold. One line per crossing rather than one per scrape: a line
repeated every scrape is a line every log pipeline learns to drop.

Alert on the gauge, not on the log line. `alerts.md` carries the two rows (`< 14d` warning,
`< 48h` critical).

## Rotating the gossip key

Gossip is encrypted with a symmetric key, and a node holds a **keyring**: one primary, which
outgoing messages are signed with, plus any number of additional keys it will accept on receive.

```toml
[gossip]
secret_key_hex = "<64 hex characters>"
accepted_key_hex = ["<64 hex characters>"]
```

`accepted_key_hex` without `secret_key_hex` is refused — there is no keyring to add to.

Three stages, in this order, **completed on every node before the next one starts**:

1. `add` K2 everywhere. Every node now accepts K2; every node still signs with K1.
2. `use` K2 everywhere. Every node now signs with K2, and still accepts K1.
3. `remove` K1 everywhere. The rotation is complete.

```
retcdctl admin rotate-gossip-key --op add --key-hex <...> --endpoint node1:8443
```

Admin-only, audited per stage as `gossip_key_add` / `gossip_key_use` / `gossip_key_remove`. The
key is never logged, never echoed in an error and never audited — the reply is fingerprints, and
fingerprints are what you follow a rotation by. Each stage logs
`gossip_key_rotated{stage, key_fingerprint}` and re-advertises the node's accepted set to its
peers.

### If you get the order wrong

**`use` before every peer has `add`.** Peers that lack K2 cannot decrypt that node's messages and
may mark it suspect. Gossip is advisory (§19.9), so this is an observability incident, not a
consensus one: the cluster keeps its leader, keeps its membership and loses no committed data.
It recovers by itself as soon as the lagging peers add K2.

**`remove` too early is refused.** A node will not remove its own primary, and will not remove a
key that a peer still needs: one it advertises as the only key it accepts, or — as built,
2026-09-19, ruling M6-R21 — one it still advertises as its primary, which is the state every
peer is in between the `add` sweep and the `use` sweep. The error names the count of peers that
still need it:

```
gossip_key_still_needed: gossip key <fingerprint> is still needed by 1 peer(s)
```

`--force` overrides this. It is the right call when the peers in question are known-dead and will
never return; it is the wrong call at any other time, because the peers naming that key will stop
being able to read anything this node sends.

### With a node unreachable

Run `add` then `use` on the reachable nodes. The isolated node keeps gossiping on K1, which the
others still accept, so it is reachable again the moment the partition heals. Complete `add` and
`use` on it, then `remove` K1 everywhere.

The accepted-list overlap is the whole point: it is what makes the return survivable.

## Secret hygiene

No rotation path logs a private key, a certificate body or a gossip key. What is logged is
fingerprints, counts, generations and `notAfter` timestamps. If you find PEM text or 32 bytes of
hex in a log, that is a defect — `docs/testing/test-plan-m6.md` M6-120 and M6-121 exist to catch
it, and a report is more useful than a workaround.
