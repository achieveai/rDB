# config-gossip — Arch Critic review, correction round 1 (2026-09-18, dev-gossip)

## Verified facts (read from vendored source, not docs.rs)

### B1 — liveness mapping (the old rustdoc was wrong)
- `memberlist-core-0.8.5/src/api.rs:116-127` — `online_members()` =
  `nodes.iter().filter(|n| !n.dead_or_left())`.
- `memberlist-core-0.8.5/src/state.rs:68-70` —
  `fn dead_or_left(&self) -> bool { self.state == State::Dead || self.state == State::Left }`.
- `memberlist-core-0.8.5/src/api.rs:97-107` — `members()` = every entry in `nodes`.
- => `members() - online_members()` == {Dead, Left}. **Suspect stays in `online_members()`**
  and is therefore reported `Alive`, NOT `Dead`.
- The previous doc ("Suspect and Left both surface as Dead ... can only understate
  confidence") was false and over-stated safety. Residual risk: a suspected peer is
  advertised healthy for the whole suspicion window.
- `Left` genuinely does surface as `Dead` (it is in `dead_or_left`).
- Root cause of not using `NodeState::state()`: `LocalNodeState` (state.rs:38-44) holds
  `server: Arc<NodeState>` plus a private `state: State`; `dead_node`/`suspect_node` mutate
  the private field, so the `Arc<NodeState>` handed out by `members()` keeps its first value.

### B2 — drop leak
- `memberlist-core-0.8.5/src/base.rs:284-291` — `Drop for MemberlistCore` closes `shutdown_tx`.
- `memberlist-net-0.8.5/src/lib.rs:504-512` — `Drop for NetTransport` closes its `shutdown_tx`,
  which is what the listener tasks select on.
- So the sockets DO close when the last `Memberlist` clone drops. The leak was ours: the
  refresher task owns an `inner.clone()` and loops forever, so dropping `GossipNode` without
  `shutdown()` never drops the last clone. Fix = `Drop for GossipNode` that notifies `stop`
  and `abort()`s the JoinHandle.

### M4 — wire versioning
- `postcard 1.1.3` has `postcard::take_from_bytes<'a, T>(&'a [u8]) -> Result<(T, &'a [u8])>`
  (`de/mod.rs:63`) — lenient decode ignoring trailing bytes.
- memberlist meta cap is 512 (`MAX_HINT_BYTES`); the version byte counts toward it.

### A5 — key rotation
- `SecretKeys` is `[SecretKey; 3]` (`memberlist-proto-0.3.4/src/encryption.rs:440`), settable
  only through `Options::with_secret_keys` at construction. `Memberlist::keyring()` returns
  `Option<&Keyring>` (immutable). No public setter after start => no overlapping-key rotation
  without a restart in 0.8.5.

## Test-id mapping decision (§6 rule 13)
§4.2 rows M1-28..M1-36 are cluster/engine gates; only M1-30 (wrong cluster_id poisoning) has a
gossip-crate half. Tests that evidence no row use the `m1_gossip_NN_` prefix, as authorized.

## Outcome (round 1 complete, all checks green)

### B2 deviation — the ruled fix was insufficient, verified empirically
The ruling said `Drop` = notify stop + `abort()` the worker. Measured: **still leaks.**
Cause found after the ruling was written: memberlist is self-referential. `stream_listener`
(`network/stream.rs:28`), `packet_listener` (`network/packet/listener.rs:49`) and
`packet_handler` each do `let this = self.clone()` — a strong `Memberlist` clone — and exit
only when `shutdown_tx` closes. `shutdown_tx` lives in the `MemberlistCore` those very tasks
keep alive, so `Drop for MemberlistCore` (`base.rs:284`) is unreachable in practice.
=> `Drop for GossipNode` must also spawn `Memberlist::shutdown()` on the current runtime.

Evidence (temporary env gates, since reverted):
- `RETCD_NO_DROP=1` (no Drop at all): FAIL, port bound after 10 s.
- `RETCD_ABORT_ONLY=1` (notify + abort, no spawn): FAIL, port bound after 10 s.
- Full fix: PASS.

### Golden wire bytes (v1), 49 bytes
`01` + 16×`5a` (ClusterId) + `02` (NodeId varint) + `14` + "node-2.retcd.invalid"
+ `00` (client_endpoint None) + `05` + "0.1.0" + `01` (protocol_version) + `00` (zone None)
+ `00` (Liveness::Alive). Hand-derived and confirmed by the test on first run.

### Test id mapping used
`m1_30_mismatched_cluster_hint_is_still_reported`, `m1_30_wrong_cluster_label_never_becomes_a_peer`
(both = gossip-side half of §4.2 M1-30); everything else `m1_gossip_01..10_` because no §4.2
row covers this crate's own contract.
