# ADR-0003: `memberlist =0.8.5` gossip is advisory only

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §3.2, §5, §21 M1

## Context

Operators want liveness/endpoint observations from day zero, but gossip must never confer
authority or become a Raft liveness dependency.

## Decision

- Use `memberlist = "=0.8.5"` (al8n) with only the Tokio runtime, TCP/UDP net transport,
  encryption, CRC32 checksums and metrics features; no QUIC. The exact feature list is pinned
  once, in the root `Cargo.toml` under `[workspace.dependencies]`:
  `default-features = false, features = ["tokio", "tcp", "encryption", "crc32", "metrics"]`.
  Member crates inherit it with `memberlist = { workspace = true }`.
- `config-gossip` exposes one trait, `GossipObservationSource`, returning `Vec<ObservedPeerHint>`
  (cluster_id, node_id, candidate peer/client endpoints, versions, zone, liveness). No
  `memberlist` types cross the crate boundary.
- Advertised metadata is limited to the §5.2 list. Never configuration values, policy,
  secrets, votes or membership decisions.
- The engine consumes hints only as candidate endpoints that must pass mTLS identity plus
  cluster/node id binding (ADR-0011) before Raft transport uses them. A `dead` observation
  produces telemetry only.
- Gossip runs on its own port and key. It is optional: Raft must form and progress with gossip
  disabled, partitioned, or poisoned.
- Documented: not wire compatible with HashiCorp Go memberlist.

## Consequences

- Static seed configuration for Raft peers remains mandatory.
- Gossip failures are alerts, not outages.

## Verification

- M1 tests: cluster with gossip disabled behaves identically; poisoned hint (wrong node id or
  cluster id) is rejected and logged; gossip cannot alter membership (`membership_config`
  before == after).

## Notes (2026-09-18)

- `memberlist 0.8.5` is MPL-2.0 licensed; it is consumed unmodified as a crate dependency
  (file-level copyleft only). Recorded here for the license inventory.
- The engine consumes gossip through `config_core::GossipObservationSource` (sync `peers()`
  snapshot). Hints are validated by the pure `validate_hint(&hint, &CommittedMembership,
  &ClusterIdentity) -> HintVerdict` function so poisoning tests need no network.

## Notes (2026-09-18, review round 1)

### Deviation from §5.2: `suspect` is not observable in `memberlist 0.8.5`

Spec §5.2 lists local `alive`, `suspect` and `dead` observations as advertisable. The adapter
reports only `Alive` and `Dead`, with this mapping:

| `memberlist` state | `ObservedPeerHint.liveness` |
|---|---|
| `Alive` | `Alive` |
| `Suspect` | **`Alive`** |
| `Dead` | `Dead` |
| `Left` | `Dead` |

Why: `NodeState::state()` — the value reachable from `members()`, `by_id()` and the event
delegate — is fixed when a node is first declared alive and is never updated;
`dead_node`/`suspect_node` mutate the private `LocalNodeState.state` instead
(`memberlist-core-0.8.5/src/state.rs:38-44`). The only honest public signal is therefore
membership in `online_members()`, which filters on `!dead_or_left()` (`src/api.rs:116-127`),
and `dead_or_left()` is exactly `state == Dead || state == Left` (`src/state.rs:68-70`). So
`members()` minus `online_members()` yields `{Dead, Left}` and nothing else.

**Residual risk, stated plainly:** a peer that has started missing probes is advertised as
*healthy* for the whole suspicion window (roughly `suspicion_mult × log(N+1) × probe_interval`).
This is the *optimistic* direction, not the conservative one — an earlier version of this
note claimed the opposite and was wrong. It is acceptable only because the signal is advisory
by construction: a hint is a candidate endpoint that must still pass mTLS identity and
cluster/node-id binding (ADR-0011), and a `Dead` observation is telemetry that never removes a
voter. Nothing in rEtcd may read `Alive` here as evidence of health, and no Raft or membership
decision may depend on this field. Revisit if a future `memberlist` exposes per-node state.

### No overlapping-key gossip rotation in 0.8.5

Overlapping-key rotation (add new key → roll → promote → drop old) is **not achievable without
a restart**. `SecretKeys` is a fixed 3-slot inline vec
(`memberlist-proto-0.3.4/src/encryption.rs:440`) and is only settable through
`Options::with_secret_keys` at construction; `Memberlist::keyring()` hands out an immutable
`Option<&Keyring>` and there is no public setter on a running node. Gossip key rotation is
therefore a restart-with-new-key operation for now, i.e. a brief gossip outage — which is
tolerable precisely because gossip is advisory and Raft does not depend on it. Recorded so
nobody designs an online-rotation runbook against an API that does not exist.

### `peer_endpoint` is never synthesised

An empty advertised `peer_endpoint` is reported empty. The adapter deliberately does **not**
substitute the peer's gossip address: that is a different plane, on a different port, with a
different key, and manufacturing a plausible-looking endpoint would turn a hint the engine
must reject into one it might accept.

### Gossip metadata carries a wire-format version

Advertised metadata is a 1-byte `HINT_WIRE_VERSION` (currently 1) followed by the postcard
body, within the same 512-byte `MAX_HINT_BYTES` total. Decoding ignores trailing bytes (so a
newer build may append fields) but refuses an unknown version byte with
`HintDecodeError::UnsupportedVersion`. Both outcomes skip the peer and log; neither is fatal.

### Note (2026-09-18, A10): a hint carries the recovery epoch, and wrong-epoch hints are rejected

ADR-0011 makes a node's identity the pair `(ClusterId, RecoveryEpoch)`. `ObservedPeerHint`
originally carried only `cluster_id`, so `validate_hint` could not tell a live peer from one
that was fenced off during an unsafe recovery: the fenced peer keeps the cluster id, keeps its
node id, and keeps advertising the pre-recovery endpoint, and every field a validator could
look at matches. `ObservedPeerHint` now carries `recovery_epoch`, and `validate_hint` rejects a
mismatch with reason `"recovery_epoch mismatch"` (`REASON_EPOCH_MISMATCH`), logged as
`gossip_hint_rejected` like every other rejection.

Ordering: the epoch check runs after `cluster_mismatch` and **before** `self_claim`. A peer
stranded on the old epoch may well advertise a node id that has since been reassigned —
possibly our own — and "wrong epoch" is the truthful diagnosis of that, not "impersonation".

`HINT_WIRE_VERSION` stays at **1**. The field is inserted into the postcard body, which is not
a backward-compatible change, but rEtcd has not shipped: there is no v1 encoder in existence to
be incompatible with, and a `2` would be a version number nothing ever spoke. The format is
actually guarded by the golden byte vector in `config-gossip/tests/gossip.rs`
(`m1_gossip_09_hint_wire_format_golden_bytes`), which failed on this change and was updated
deliberately. The epoch is a postcard varint, so the 512-byte `MAX_HINT_BYTES` budget is
unaffected even at `u32::MAX` — asserted by `a10_recovery_epoch_round_trips_over_the_gossip_wire`.

### Note (2026-09-19, M6 gate): an ephemeral gossip port is bound up to eight times

`memberlist-net 0.8.5` resolves a port-`0` bind by binding TCP first (ten tries) and then
binding UDP on **the same port** with no retry. A port that was free for TCP can already be
held for UDP by any other process, and on a busy host that happens: the M6 gate run lost
E2E-43 twice to a node 0 whose ready line carried no gossip address, because
`GossipNode::start` had failed and the daemon, per this ADR, continued without gossip.

`GossipNode::start` now re-runs the whole bind up to `EPHEMERAL_BIND_ATTEMPTS` (8) times when
`bind_addr` asks for port `0` and the failure is `GossipError::Start`; each retry is logged at
debug as `gossip_ephemeral_bind_retry`. A fixed port is never retried: a taken fixed port is
the operator's configuration, not the host's luck. The daemon's degrade-to-no-gossip behaviour
is unchanged; only the odds of hitting it for a reason the operator cannot see have dropped.
