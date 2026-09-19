# ADR-0011: Cluster/Node identity binding and static formation

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §4.2, §4.3, §13.1, §19.10

## Decision

- `ClusterId` = 16 random bytes shown as hex; `RecoveryEpoch: u32`; `NodeId: u64` (1..=3 in
  the first release, never reused). Together they form `ClusterIdentity`.
- Every data directory stores identity on first open (`RocksStore`, `state_meta/identity`) or in
  memory (`EphemeralStore`). Any later open with a different identity → `IdentityMismatch`
  error before Raft starts.
- Bootstrap manifest (`manifest.toml` + `manifest.sig`): cluster id, epoch, version, expiry,
  `[[nodes]] {id, peer_endpoint, client_endpoint, cert_subject}`, gossip seeds and gossip
  protocol version, signer key id. Signed with Ed25519 (`ed25519-dalek`) over the file bytes.
  `config-server` refuses an invalid or expired signature.
- Formation is an explicit call `ConfigNode::form_cluster(FormationPlan)` invoked by the
  operator harness (`config-server --form` once, on one node) or the test harness. It calls
  `Raft::initialize` with all three voters. It succeeds only if the local store is fresh and the
  identity matches the manifest. An empty node without this call stays idle and serves
  `Unavailable`.
- Peer certificate binding: peer cert SAN URI `retcd://<cluster_id>/node/<node_id>` must match
  the `from_node_id` and target expectations of every peer RPC; mismatch → reject + audit log.

## Consequences

- A permanently lost voter requires a rebuild in this release (documented prominently in
  README).

## Verification

- M1: empty node never self-forms (no leader after 10 election timeouts).
- M2: identity mismatch prevents startup.
- M3: wrong cluster/node/destination identity rejected.
