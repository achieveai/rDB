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
