# ADR-0003: `memberlist =0.8.5` gossip is advisory only

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §3.2, §5, §21 M1

## Context

Operators want liveness/endpoint observations from day zero, but gossip must never confer
authority or become a Raft liveness dependency.

## Decision

- Use `memberlist = "=0.8.5"` (al8n) with only the Tokio runtime, TCP/UDP net transport,
  encryption and metrics features; no QUIC. Exact feature list lives in
  `crates/config-gossip/Cargo.toml`.
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
