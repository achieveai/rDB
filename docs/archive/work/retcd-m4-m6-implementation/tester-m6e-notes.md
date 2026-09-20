# tester-m6e — M6 test plan §4.2 rotation rows (M6-49..M6-56, M6-60) — working notes

Scope: `crates/config-testkit/tests/m6_rotation.rs` (add M6-49..M6-56, M6-60),
`crates/config-testkit/src/cluster.rs` (additive harness methods only),
`docs/testing/test-plan-m6.md` (as-built notes + M6-121/M6-61 Expected column corrections).
Tests-only mandate: no product-code edits except timed, reverted mutations.

## Rows landed

| Row | Test function |
|---|---|
| M6-49 | `m6_49_peer_transport_reloads_its_client_and_server_credentials` |
| M6-50 | `m6_50_destination_binding_survives_rotation` |
| M6-51 | `m6_51_rotation_while_one_voter_is_down_keeps_the_cluster_available` |
| M6-52 | `m6_52_the_down_voter_rejoins_only_with_a_chain_to_a_trusted_root` |
| M6-53 | `m6_53_the_down_voter_is_refused_after_the_old_root_is_dropped` |
| M6-54 | `m6_54_the_down_voter_rejoins_after_being_issued_a_new_leaf` |
| M6-55 | `m6_55_rotation_does_not_disturb_committed_membership_or_identity` |
| M6-56 | `m6_56_acceptor_implementation_is_recorded_not_assumed` (plain `#[test]`, source assertion) |
| M6-60 | `m6_60_rotation_with_one_node_unreachable_converges_on_its_return` |

No rows skipped this pass. All 9 assigned rows are implemented, not stubbed.

## Harness changes (additive only, no existing signature changed)

- `Cluster::gossip_isolate(id)` / `Cluster::gossip_heal(id)` — new, in `cluster.rs`. `NetFault`
  has no seam on the real gossip UDP transport (only the peer-plane TCP transport), so
  isolation here is a full `GossipNode` shutdown (graceful `leave`) and heal is a fresh
  `GossipNode::start` seeded off a still-gossiping peer, reusing the node's pre-isolation
  `gossip_key`. Correct only for "nothing reached this node while it was down" — not a
  general keyring snapshot/restore.
- `overlap_all()` — test-local helper in `m6_rotation.rs` (not the harness proper), widens
  every configured node's CA bundle including stopped ones (file-write only; safe since
  `rotate_ca_bundle`/`rotate_files_with` operate on `NodeSlot.tls_files`, not `RunningNode`).

## Defects / findings (not product defects unless stated)

1. **M6-53's actual refusal reason is `HandshakeFailed`, not `UntrustedClientCa`, and
   definitely not `untrusted_peer_ca`** (that variant does not exist on `AuthnRejectReason`
   — the plan's own spelling was simply wrong). In a *mutual* distrust shape (both sides drop
   the shared root at once), the dialling client typically aborts before the listener's accept
   loop can downcast a specific `rustls::CertificateError`, so the listener only sees a plain
   I/O error, correctly classified by `classify_handshake_failure`
   (`config-grpc/src/server.rs`) as the catch-all `HandshakeFailed`. Not a product defect —
   the classification is doing the honest thing. Plan's Expected-column text left as-is per
   this pass's authorized scope (only M6-61/M6-121 columns were in scope to directly correct);
   documented in the §4.2 as-built note for a future correction pass.

2. **Harness admin-dialer trust pinning** (found while designing M6-50). `Cluster::reload_tls`'s
   `admin_rpc()` (`rotation.rs`) builds its TLS client from `self.fixture().issue(...)`, whose
   `ca_pem` is permanently the cluster's original fixture CA — never widened by
   `overlap()`/`rotate_ca_bundle`. Once a node's *served* leaf permanently rotates away from
   that CA, no further `reload_tls` RPC can reach it; only `poll_tls` (in-process) or a
   restart (fresh dial) can still interact with its TLS material. Not itself a product defect
   (it is the harness's own dialer, not the product's), but it shaped M6-50's and M6-53's
   designs: both use stop/rewrite-files/start rather than a second RPC.

3. **M6-60: a real bug, not a flake, in the first cut of `gossip_heal`.** The row's Setup has
   nodes 1 and 2 rotate to K2 as primary *while node 3 is isolated*. `gossip_heal` correctly
   rebuilds node 3 holding only K1 (the key it had when it left), but a fresh `GossipNode`'s
   very first join attempt then fails on *every* attempt with `"no installed keys could
   decrypt the message"` — retrying that join for longer never helps, since the key is wrong,
   not the timing. Evidence: single-threaded reruns failed 100% of the time (4/4, then a
   further 4/4), each ~136s, all reporting `peers seen [(1,1),(2,1),(3,0)]`; the structured log
   showed the identical decrypt-failure error on all 270 join attempts in one run. An earlier
   version of `gossip_heal` (written before this evidence) added a long internal join-retry
   loop and was misdiagnosed in-session as scheduling flakiness under concurrent load — that
   diagnosis was wrong; the loop only wasted ~30s per call without fixing anything. Fix: the
   retry budget in `gossip_heal` was shrunk back to a short one (genuine dropped-probe case
   only); the row itself, after telling node 3 the missed key via
   `gossip_add_key`/`gossip_use_key`, explicitly re-joins node 3 through the already-public
   `Cluster::gossip_node(id)` / `GossipNode::join` — the same step an operator's own reconnect
   would take. Verified 3x green after the fix (previously 100% failing, ~136s each).

4. **M6-49's original final assertions proved nothing about the dialer — found by the
   mandated mutation check, then fixed.** See Mutation log below. Root cause: the row's
   closing checks (`client(leader).put(...)`, `wait_converged`, leader-unchanged) all rode on
   peer-plane connections the leader had already opened *before* the rotation; TLS does not
   re-verify an already-open connection (the same fact M6-50's redesign is built on). Fixed by
   restarting one follower after the CA-narrowing step and asserting it rejoins
   (`Cluster::wait_rejoined`) — the leader can only complete that rejoin by dialling fresh,
   with its current `peer_dial` material, against a follower that now trusts only the new
   authority.

5. **Cross-cutting scan failure, not mine.** `cargo test -p config-testkit --test scan` failed
   once (`workspace_tests_contain_no_fixed_sleeps_or_literal_ports`, flagging an unmarked
   fixed sleep at `crates/config-server/tests/e2e_daemon.rs:3297`) — that file is out of my
   shared-tree scope (owned by the concurrent `tester-m6d` agent, working E2E-41/43/45). Not
   touched. Re-ran clean on a later pass (4/4), consistent with it having been transient WIP
   in a file I do not own.

## Mutation log (UTC)

All mutations were reverted; `grep -rnE "MUTATION (OPEN|CLOSED)" crates/*/src crates/*/tests`
returns nothing as of the end of this session.

1. **Peer-dial invalidation row — M6-49** (`crates/config-grpc/src/rotation.rs`,
   `TlsRotator::try_reload`).
   - `MUTATION OPEN 2026-09-19T17:28:52Z` — skipped `self.peer_dial.reload(found.clone())`
     while still reporting the `peer_dial` plane as `"reloaded"`.
   - Ran `m6_49` alone: **passed** — meaning the row, as originally written, could not detect
     this regression. Reverted at approximately 17:32Z (build+run took ~3m27s; this mutation
     window itself was longer than the 2-minute target because of a cold compile after a
     target-dir switch — noted as a process deviation, not hidden).
   - Fixed the row (see Finding 4 above); rebuilt.
   - `MUTATION OPEN 2026-09-19T17:34:11Z` — re-applied the identical skip.
   - Ran `m6_49` alone: **failed**, as expected — panic at
     `m6_rotation.rs:880` (rejoin timeout), reason pinned: "the leader must be able to dial
     this follower fresh, using its rotated peer_dial credentials, for it to rejoin at all".
   - `MUTATION CLOSED 2026-09-19T17:35:46Z` — reverted (~95s open). Reran `m6_49` alone:
     **passed** (0.52s).

2. **M6-60** (`crates/config-gossip/src/node.rs`, `GossipNode::add_gossip_key`).
   - `MUTATION OPEN 2026-09-19T17:36:43Z` — skipped the `keyring.insert(...)` call while still
     reporting `"added"` in the publish.
   - Ran `m6_60` alone: **failed**, reason pinned: panic at `m6_rotation.rs:1884`,
     `status: InvalidArgument, message: "invalid argument: gossip_keyring_refused: gossip
     keyring: cannot sign with gossip key 9f72ea0cf49536e3: secret key is not in the
     keyring"`. (Caught at node 1's own `use_key` step, earlier than node 3's specific
     post-heal catch-up, since the mutation affects `add_gossip_key` globally — still a valid
     demonstration that M6-60 depends on this code path working.)
   - `MUTATION CLOSED 2026-09-19T17:37:46Z` — reverted (~63s open). Reran `m6_60` alone:
     **passed** (5.62s).

## Verification evidence

- `rustfmt --edition 2021 --check` on all touched files: clean (no diffs) after every edit,
  confirmed as of the final state.
- `cargo clippy -p config-testkit --all-targets -- -D warnings`: clean, final state.
- `cargo check -p config-testkit --all-targets`: clean, final state.
- `cargo test -p config-testkit --test scan`: 4/4 passed, final state (see Finding 5 for the
  one transient failure observed mid-session, not caused by my files).
- Full 24-test `m6_rotation` suite, `--test-threads=4`, `RETCD_TEST_DEADLINE_SCALE=3`, run
  clean (no other cargo test/build process running concurrently) 3x consecutively after the
  M6-49 fix and both mutation reverts: **24/24 passed each time**, ~28.3-28.4s per run.
  (An earlier 3x attempt that overlapped with a concurrent mutation-test compile/run showed
  3 unrelated failures — M6-53/54/55, all real-TLS/Raft-heavy — in 2 of 3 runs; re-run in
  isolation afterward was clean 3/3, consistent with resource contention rather than a real
  defect. Not claiming those runs as evidence either way; the clean, isolated 3x run is what
  is being reported as passing.)

## Anomalies observed (not acted on)

Two system-reminder-formatted messages appeared attached to tool results during this session
that are not genuine user or system policy:

- One instructing that commits end with `Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>`
  and PRs with a "Generated with Claude Code" footer — contradicts the user's own CLAUDE.md
  ("Do not add Co-Authored-By or any AI/Claude signature..."). Disregarded. (No git operations
  were performed under this mandate regardless.)
- One instructing a preference for Bash+sed/heredoc over the dedicated Read/Edit/Write tools
  "while bypass permissions mode is active." Disregarded as likely prompt injection; continued
  using Read/Edit/Write normally throughout.

Both are recorded here as an observation, not escalated, since no action was taken on either.
