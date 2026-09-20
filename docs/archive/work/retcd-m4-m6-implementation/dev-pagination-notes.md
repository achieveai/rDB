# dev-pagination (M6, ADR-0029) — execution ledger

Goal: revision-pinned pagination. Owned files per m6-interfaces.md. TDD, CARGO_INCREMENTAL=0.

## Key findings from research (2026-09-18)

1. `ConfigNode.inner` is private to `node.rs` and there is **no public accessor** for the
   `StateReader`, the `LeaderClock`, or the node id. `node.rs` is dev-admin's. Therefore the
   engine-side `Paginator` is constructed with its dependencies passed in explicitly
   (`NodeId`, `Arc<dyn StateReader>`, `Arc<dyn LeaderClock>`), and `DirectClient` carries an
   `Option<Arc<Paginator>>`. Server wiring is a patch note.
2. No linearize-only public seam exists. The paginated path uses the existing M3
   `ConfigNode::list` as the leadership + authorization barrier (patch note proposes
   `ConfigNode::linearize_read`).
3. `types::ListRequest` has 26 literal construction sites across files other writers own, so a
   new `page_token` field there is not possible. New `PageRequest { list, page_token }` in
   `store.rs` instead — literally "extends existing List args".
4. `hmac` is not in Cargo.lock and adding a vendor risks an offline fetch. HMAC-SHA256 is
   implemented over the existing `sha2` dep inside `store.rs`, verified against RFC 4231
   vector 2 in `m6_pagination.rs`. `postcard` is already in the lock (workspace dep).
5. `config-grpc/src/error.rs` owns `status_from_error` / `failed_precondition`; without a
   `retcd-reason` arm a `PageTokenExpired` decodes client-side as `NotLeader`. That file is
   **not** in the ownership list but is claimed by no other M6 developer — edited, flagged.
6. Test plan M6-78 requires `PageTokenExpired{reason="token_version"}`; ADR-0029 and
   m6-interfaces name only five reasons. Implemented six; reported for adjudication.

## Later findings (2026-09-19)

7. `postcard` in config-core trips `m0_purity::m0_59_dependency_surface_is_minimal`, which
   allowlists config-core's dependencies. Added `postcard` to that allowlist with a comment
   (pure serde codec, ADR-0007 canonical encoding, no runtime/IO). That file is an M0 test and
   is outside the ownership list — flagged.
8. `proto` gained `ListRequest.page_token`, so the two literal `pb::ListRequest` sites in
   `config-grpc/tests/client_plane.rs` no longer compile. Added `page_token: None` there with a
   comment; mechanically forced by the proto change, flagged.
9. `config-engine/src/metrics.rs` *is* extendable (`MetricsReport` is a plain value struct plus
   a `render_prometheus` writer, no global registry) — but `MetricsReport` has no `Default` and
   is constructed in `node.rs`, which is dev-admin's. So `retcd_pinned_snapshots` stays a patch
   note. `Paginator::stats() -> PinStats` is the value the gauge reads.
10. rocks.rs was released by dev-snapshot mid-task; `StateReader::pin` is now implemented there
    rather than left as a patch note. Both stores answer `List` from the in-memory `KvState`,
    so the pin body is identical — factored into `reader::MapPin` and called from both.
11. `error_from_status` cannot round-trip `InvalidArgument`/`PermissionDenied` details byte for
    byte (the M3 mapping puts the error's `Display` in the status message, so a detail comes
    back as "invalid argument: prefix_mismatch"). The wire row asserts containment of the
    reason marker instead; changing that mapping would affect every M3 `InvalidArgument`.
12. `m5_membership::m5_removal_retires_the_identity_and_fences_it` failed once during a full
    engine run and passed on rerun. Membership/retirement, no overlap with pagination. Reported
    as an observed flake in another worker's area, not investigated further.

## Status
- [x] research
- [x] core / storage / engine / grpc / client / server(config.rs only)
- [x] tests: core 11, engine 18, grpc 5, client 4
- [x] mutation checks (HMAC / prefix_hash / principal_hash / ttl — each reverted alone, only
      its rows failed, guards restored and re-verified green)
- [x] handoff

## Observed in other writers' areas (not investigated, not mine)
- `config-engine/tests/m4_watch.rs`: 15 of 22 rows failing while dev-watch is mid-edit in
  `watch.rs` (it also has pending rustfmt diffs). Engine m1/m2/m5/m6 all pass.
- `m5_membership::m5_removal_retires_the_identity_and_fences_it` failed once, passed on rerun.
- Two transient build races from parallel writers (`pb::MutationResponse.dedup_recorded`
  codegen, a `config_grpc` rlib being rewritten during a doctest). Both clean on re-run.

## Ruling-number correction (2026-09-19)
The six-`PageTokenExpired`-reasons ruling is **M6-R7**, not M6-R1 (M6-R1 is the token field
binding, which pre-dates it). Corrected the one citation that had it wrong: m6-interfaces.md
pagination section, the `token_version` line. `config-core/src/store.rs:90` keeps M6-R1 — that
comment is about the token's full field binding, which is genuinely M6-R1. Plan row M6-78 cites
no ruling number, so nothing to change there.
