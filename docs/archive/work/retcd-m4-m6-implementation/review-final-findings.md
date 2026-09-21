# Final review of feature/m4-m6: collected findings

Scope: M6 delta (e54c6ef..4f6f7e5) + branch cross-cutting. 6 reviewers wave 1.

## rev-temp (temporary/debug code) — PASS_WITH_RISKS, 0 blockers
- F-001 LOW/TRIVIAL, non-blocking: docs/evidence/*.json (8 files) carry git_sha=e54c6ef, dirty=true at tip 4f6f7e5; m6_evidence rewrites them in place, so a suite run dirties the tree. evidence-gate.ps1:48 only enforces under RETCD_EVIDENCE=1. Fix floor: one sentence in docs/evidence/README.md.
- F-002 LOW/TRIVIAL, non-blocking: artifacts commit hostname DEVSFOUR-HOST; README:24 calls the host fields "generic". Fix floor: name hostname in README, or drop it.
- Q1: --dev-allow-all is independent of --allow-insecure-dev, so real TLS + AllowAll authz is possible (cli.rs:49, run.rs:1006). WARN-logged, reported as AuthzKind::Development. Intended by ADR-0012?
- Clean: no new TODO/HACK/FIXME, no #[ignore], no tautologies, no mutation residue (only 2 intentional "MUTATION TARGET" doc comments), no committed junk or key files, no product-code secrets, fail-closed default authz confirmed.

## rev-silent (silent failures) — PASS_WITH_RISKS, 0 blockers
- F-003 MEDIUM/TRIVIAL: policy.rs:293 and rotation.rs:49 treat every JoinError as shutdown and return with no log. JoinError is also a panic. A panicked poller leaves the node serving but never reloading policy or TLS again; health still Active, no counter moves. Floor: branch on JoinError::is_panic() and error-log the stopped poller.
- F-004 LOW/TRIVIAL: config-grpc/src/rotation.rs:204 logs plane="all" and docs "nothing changed anywhere", untrue for a mid-loop plane.replace failure (first plane already swapped). Narrow path (recompiles bytes that just compiled).
- F-005 LOW/SMALL: config-server/src/policy.rs:353 drops undecodable peer hints silently; a wire regression pins the cluster in Converging with no way to tell lag from unreadable. Floor: put voters_reporting/voters_total in the Converging health payload.
- F-006 LOW/TRIVIAL: config-engine/src/node.rs:2292 `if let Ok(Err(e))` discards the Elapsed arm; a timed-out compaction proposal logs nothing. Pre-existing at e54c6ef.

## rev-tests (coverage vs plans) — PASS_WITH_RISKS, 0 blockers
- F-007 HIGH/TRIVIAL **lead-verified**: `bind_policy_version` is called only by a test (rg: pagination.rs:365 def, m6_pagination.rs:458 test). run.rs:806 never binds, so Paginator::policy_version() is always None, tokens seal None, and PageTokenExpiredReason::PolicyVersion can never fire in the daemon. M6-71 passes because the test owns the atomic. M6-32 (the policy-side row) was never implemented. Mitigation: prefix authz re-runs per page. Floor: PolicyLoader owns an Arc<AtomicU64>, run.rs binds it, add the M6-32 row driving a real adoption.
- F-008 MEDIUM/SMALL: M6-126 absent; ReloadTls has no denied-path or audit assertion (all 17 test call sites use ADMIN).
- F-009 MEDIUM/SMALL: M6-82 renamed; releases pins by TTL + a later walk, not by disconnect. Idle node holds pins past TTL.
- F-010 LOW/SMALL: M6-81 dropped the compaction half; no pin held across a compaction.
- F-011 LOW/TRIVIAL: M6-72 repurposed to a NoPin refusal; the ephemeral/rocks parity claim is unowned.
- Coverage otherwise good: all other rows map by id, no #[ignore], evidence rows schema-honest (validate rejects scale_factor 0.1 with full_scale true).

## rev-security — PASS_WITH_RISKS, 0 blockers
- F-012 LOW/SMALL: signed policy documents carry no cluster identity (config-core/src/policy.rs:313). One shared ops key across clusters means each accepts the other's document; a staging v9 dropped into prod adopts (higher version, so no rollback refusal). The TLS path does check expected_cluster (tls.rs:15-17,436). Floor: optional cluster_id + one comparison, or state the trust-key scope rule in ADR-0027.
- F-013 LOW/TRIVIAL: AdminAllowlist::permits matches on name only (admin_plane.rs:281); grants go through is_verified_kind but admins do not. Under signed authz + insecure TLS, a document listing "dev" opens the admin plane to an unauthenticated caller. Floor: reject unverified principal kinds in permits.
- Q2: the policy version floor is process-scoped; after a restart an older validly-signed document re-adopts with no downgrade signal. Intended?
- Q3: peers_still_needing skips peers advertising no trailer and truncates past MAX_ADVERTISED_GOSSIP_KEYS; availability-only, documented as deliberate.
- Clean: verification order, fail-closed defaults, compile-before-swap, handshake permit before accept, closed-enum metric labels, no key material in logs/metrics/evidence.

## rev-compat (schema/wire/migration) — PASS_WITH_RISKS, 0 blockers
- F-014 MEDIUM/SMALL **lead-verified**: node.rs:2336 `schema_gate` clause 1 returns Ok on the durable
  watermark *before* `cluster_min_schema()` is read, so a voter that has advertised command_schema=1
  is ignored forever once the cluster applied one schema-2 entry. The clause's own comment justifies
  this with "a voter that has not is fenced by its own decode refusal when it returns" — see F-015,
  that fence does not exist. Floor: qualify clause 1 with the observed triples (absent = unreachable
  still does not block); mirror in sample_schema_activation (2388).
- F-015 MEDIUM/SMALL **lead-verified**, clusters rev-errors F1: `SchemaTriple::decode_command`
  (schema.rs:132) and `::admits` (115) have zero product callers — grep across crates/*/src returns
  only the definition. ADR-0030 as-built and the COMPAT_SCHEMA_1 doc claim a pinned node "refuses to
  decode" a schema-2 envelope. The apply path decodes with the full grammar and state.rs:578 then
  raises that node's durable watermark to 2 while it still advertises 1. Floor: either call
  `cfg.schema.admits(&cmd)` on apply (must land with F-014 or E2E-42 becomes a stopped node), or drop
  the refusal sentence from ADR-0030 + schema.rs:92-99.
- F-016 MEDIUM/TRIVIAL: rocks.rs:312 `UpgradeRequiresDrainedLog` tells a format_version=1 operator to
  snapshot and let purge drain — neither exists on main (no snapshot.rs, no admin proto at 7d524ac).
  Every real M3 directory hits this arm. Floor: branch the message on from==V1 to name rebuild-as-
  fresh-learner.
- F-017 LOW/TRIVIAL: CURRENT_SCHEMA.proto_rev stays 1 though M6 changed the gRPC surface; identical to
  COMPAT_SCHEMA_1.proto_rev, so the axis cannot discriminate. Floor: one doc sentence.
- Q4: snapshot install validates header against build constants, not cfg.schema/max_format_version
  (rocks.rs:3474,3480), so a pinned node accepts a current snapshot and apply_snapshot_records raises
  its watermark. Deliberate under M6-R15, or the mechanism behind F-014?
- Clean: no new CFs, FORMAT_VERSION unmoved, proto strictly additive (PeerEnvelope.schema = field 7),
  gossip trailer forward/backward safe, SnapshotHeader length-framed, state_hash excludes the watermark.

## rev-errors (error handling) — PASS_WITH_RISKS, 0 blockers
- (F-015 above is this reviewer's Finding 1, merged.)
- F-018 LOW/TRIVIAL: RotationError's doc says "every variant leaves every plane serving exactly what
  it served before"; the plane loop (config-grpc/rotation.rs:234) uses `?` after the first swap. Same
  mechanism as F-004. Floor: scope the doc to pre-swap failures.
- F-019 LOW/TRIVIAL: policy.rs:371 writes `*advertised = Some(version)` before awaiting
  update_extras; the Err arm rolls back, a cancel does not, and the 368 early-return then suppresses
  every retry for that version — node counted as lagging forever. Only cancel site today is the
  shutdown abort. Floor: move the write after Ok.
- Q5: run.rs:1376 notify_waiters + abort cannot stop a poller parked in spawn_blocking; the comment
  claims the drain ordering is guaranteed.
- Q6: transport.rs panics on poisoned Mutex while rotation.rs recovers with into_inner — adjacent
  modules, opposite conventions.
- Clean: no reachable panic from peer/client input (open_token, parse_gossip_key, gossip meta decoders
  all total), typed StorageError on every apply encode failure, NotLeader round-trips with its hint,
  timeouts present on peer calls / backup build / handshake.
