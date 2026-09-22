# M6 gap triage

**Date:** 2026-09-20
**Branch read:** `feature/rdb-m7` at `7684412`. The rEtcd crates are unchanged since `84b2e2d`
(comments only); the last behaviour change was `61df7bd`.
**Method:** every "still open" claim below was checked against the code, not against the
document that records it. File and line references are what I opened.

**Counts:** 19 gaps recorded at M6. **18 still open, 1 already closed.** Plus 4 deliberately
unowned items in the out-of-scope section at the end.

**Id scheme:** where a source names a test-plan row id, that id is used. Everything else is
assigned `G-01`..`G-12` by me; those ids are new and exist only in this document.

---

## 1. Table

Severity = how bad if this ships. Effort = the smallest real fix, not the ideal one.
S = under a day. M = days. L = weeks.

| id | one line | state | severity | effort |
|---|---|---|---|---|
| G-06 | A signed policy document carries no cluster identity; one shared ops key means cluster A adopts cluster B's document, rollback refusal bypassed | **open** | **Critical** | S |
| G-09 | The policy version floor is in-memory only, so a restart re-adopts an older validly-signed document with no downgrade signal | **open** | **Critical** | M |
| G-05 | The policy signature payload has no domain-separation tag (`hash ‖ version_le`, 40 bytes), so a trust key must never be reused for any other signing surface | **open** | High | L (flag day) |
| G-07 | `config-server/src/policy.rs` drops undecodable peer policy hints silently; a wire regression pins the cluster in `Converging` with no way to tell lag from unreadable | **open** | Medium | S |
| M6-33 | `backup.rs` hardcodes `policy_version_ref: None`; the signed manifest never references the active policy version | **open** | Medium | S |
| G-11 | `config-grpc/src/transport.rs` panics on a poisoned mutex while the adjacent `rotation.rs` recovers with `into_inner` | **open** | Medium | S |
| G-08 | `TlsRotator::try_reload` returns on the first plane failure, leaving earlier planes already swapped | **open** | Medium | M |
| M6-126 | The six-op admin audit assembly row has no owner; only the `ReloadTls` share landed | **open** | Medium | M |
| G-12 | No daemon-level row for the schema fence; it is proven at the storage layer instead | **open** | Medium | M |
| M6-82 | Pins release by TTL plus a later sweep, never on disconnect; an idle node holds pins past the TTL | **open** | Medium | M |
| M6-81 | The row dropped its compaction half; no row holds a pin across a compaction | **open** | Medium | S |
| G-10 | Shutdown `abort`s the TLS and policy pollers rather than joining them; a `spawn_blocking` poller can still complete a swap during the drain | **open** | Low | M |
| M6-72 | Repurposed to a `NoPin` refusal, so the ephemeral/rocks pagination parity claim is unowned | **open** | Low | M |
| G-01 | `handshake_timeout` is a start-up constant, not exposed in `[tls]`; no daemon-level row drives the bound | **open** | Low | S |
| G-02 | At the in-flight handshake cap the accept loop waits on the semaphore, so excess connections queue in the kernel backlog rather than being refused | **open** | Low | M |
| G-04 | A continuation page hitting a follower returns `PageTokenExpired{reason="node"}` with no leader hint | **open** | Low | S |
| M6-35 | `restore_policy_mismatch` is emitted nowhere; a restore against a different policy version logs no divergence line | **open** | Low | S |
| G-03 | The drain predicate's clause 1 is a decode check, not a semantic one; a positional decode can succeed into a different command | **open** (by design) | Low | L |
| M6-32 | Page tokens sealed `policy_version: None` because the daemon never bound the cell | **CLOSED** | — | — |

---

## 2. Detail

### G-06 — a signed policy document carries no cluster identity — CRITICAL, open, effort S

**Product defect.** `PolicyDocument` (`crates/config-core/src/policy.rs:81-101`) has exactly four
fields: `version`, `issued_unix_ms`, `grants`, `admins`. No cluster identity. The signature
payload is built by `signature_payload()` (`policy.rs:188-193`) as forty bytes, `hash ‖
version_le`, and `verify_policy()` (`policy.rs:313-345`) checks signer name, signature, body
parse, version binding and hash — and nothing about which cluster the document is for.

So with one operations key trusted by two clusters, each cluster's document verifies on the
other. `SignedPolicyAuthorizer::adopt` (`policy.rs:610-616`) refuses only
`incoming.version <= active.version`, so a *higher* version from the wrong cluster adopts
cleanly, replacing the grant set and the admin set with a foreign one. Nothing logs a cluster
mismatch, because nothing knows one happened.

The contrast the lead flagged is real. The transport path does check cluster:
`crates/config-grpc/src/tls.rs:402,436` compares a client SAN's `cluster_id` against the
listener's `expected_cluster`, and `config-grpc/src/transport.rs:283-286` checks the answering
peer's cluster. The authorization plane is the one surface that does not.

**Source:** ADR-0031 lines 210-213 (final branch review). ADR-0027 lines 22-33.
**Confirmed open by:** `policy.rs:81-101` (no field), `policy.rs:188-193` (40-byte payload),
`policy.rs:610-616` (version-only refusal), and `rg expected_cluster crates/` returning hits
only in `config-grpc` and `config-storage`.

**Touches:** `config-core` (`policy.rs`: one optional `cluster_id` field, one comparison in
`verify_policy` or at the adopt seam, one new `PolicyRejected` reason string), `config-server`
(pass the node's `ClusterId` into the loader), plus the policy-signing tooling and one test row.
Effort S because `ClusterId` already exists and is already plumbed to the daemon.

**Why S and not M:** an `Option<String>` field with `#[serde(default)]` keeps every existing
signed document valid. `None` means "unscoped, legacy" and can warn; `Some` must match. No flag
day. The alternative the ADR offers — "state the trust-key scope rule in ADR-0027" — is a
documentation-only close and would leave the defect in place.

---

### G-09 — the policy version floor is process-scoped — CRITICAL, open, effort M

**Product defect.** The rollback refusal reads the *in-memory* active document:
`crates/config-core/src/policy.rs:609-616`, `let from = active.signed.document.version;
let is_rollback = incoming.document.version <= from;`. When there is no active document —
which is every process start — `adopt` returns `Adoption::Adopted { from: None, .. }`
unconditionally (`policy.rs:596-602`). Nothing persists the highest version ever adopted:
`rg "policy_version|policy_floor" crates/config-storage/src/` returns nothing.

Consequence: an older, *validly signed* document — one from a backup, a git history, an
operator's home directory — re-adopts at the next restart with no refusal and no downgrade
signal. Grants and admins revert to the older set, and the audit trail shows an ordinary
adoption. The signing key is not needed; only the old file is.

**I rank this alongside G-06, not below it.** G-06 needs a shared key across two clusters, which
is an operator mistake that may never occur. G-09 needs a restart, which is routine, and a file
an attacker may already possess. **This is the entry I think is most likely to be
underestimated**, because the ADR's one-line wording ("the policy version floor is
process-scoped") reads like a scoping note rather than a bypass of the security control the
rollback refusal exists to be.

**Source:** ADR-0031 line 224.
**Confirmed open by:** `policy.rs:596-616`, and the absence of any durable key.

**Touches:** `config-core` (`policy.rs`: seed the floor from a durable value rather than from
the active document), `config-storage` (one `state_meta` key, or a sidecar the loader owns),
`config-server` (`policy.rs`/`run.rs`: read it at startup, write it on adopt). Effort M because
it needs a durable-state decision — a `state_meta` key makes the floor Raft-adjacent and raises
the question of what a restored-from-backup node does — plus the break-glass interaction, which
must clear the durable floor and not merely the in-memory one.

---

### G-05 — no domain-separation tag in the signature payload — High, open, effort L

**Product defect, mitigated by documentation.** `signature_payload()`
(`crates/config-core/src/policy.rs:188-193`) returns exactly `hash ‖ version_le`. There is no
context string, no tag byte, nothing that says "this is a rEtcd policy document". A trust key
reused for any other rEtcd or operator signing surface whose payload could collide with a
32-byte hash plus a little-endian u64 is a cross-protocol forgery risk.

ADR-0027 (lines 28-33) states the constraint plainly — the key MUST NOT be reused — and says
why it was not retrofitted: adding a tag is a flag day for every signed document in every
deployment.

**Source:** ADR-0027 lines 28-33; ADR-0031 line 170.
**Confirmed open by:** `policy.rs:188-193`.

**Touches:** `config-core` (`policy.rs`), the signing tooling, and every deployed document.
Effort L as written. There is a cheap partial: version the envelope so a tagged payload can be
introduced alongside the untagged one and the flag day becomes a rollout rather than a cutover.
That partial is S-to-M and is worth pricing separately if this is funded.

---

### G-07 — undecodable peer policy hints are dropped silently — Medium, open, effort S

**Product defect.** `crates/config-server/src/policy.rs:392-395`:

```rust
let Ok(hint) = config_gossip::decode_hint(&meta) else {
    continue;
};
```

A peer whose hint this build cannot decode is treated identically to a peer that has not
reported. It counts as lagging. Convergence never completes, the cluster sits in `Converging`
forever, and no line anywhere distinguishes "peer is behind" from "peer's wire format is
unreadable". A wire regression therefore presents as an indefinite convergence stall with no
diagnostic.

The `voters_reporting` / `voters_total` numbers exist and are already computed
(`policy.rs:291-292`) — but only on the `policy_converged` `info` line, which is emitted *after*
convergence succeeds. They are exactly the numbers an operator needs while it is failing, and
they are not in the health payload: `crates/config-server/src/health.rs:137-143` fills
`policy_state` and `policy_version` and nothing else.

**Source:** ADR-0031 lines 214-216.
**Confirmed open by:** `config-server/src/policy.rs:392-395`, `policy.rs:285-295`,
`config-server/src/health.rs:137-143`.

**Touches:** `config-server` (`policy.rs` + `health.rs`): add `voters_reporting`/`voters_total`
to the `Converging` health payload, and count undecodable metas separately. Effort S — the view
already carries both numbers.

---

### M6-33 — the backup manifest never references the policy version — Medium, open, effort S

**Product defect, not a missing test.** `crates/config-server/src/backup.rs:96` declares
`pub policy_version_ref: Option<u64>` and line 328 hardcodes `policy_version_ref: None` at the
artifact-finish seam. Nothing populates it from live policy state, even though the field's own
doc comment reserves it for M6 policy versioning. There is no M6-33 test row anywhere in
`crates/`.

The consequence is a signed backup manifest that cannot tell a restoring operator which policy
document was in force when the backup was taken. ADR-0027 (line 122) and the test plan both
describe the field as present.

**Source:** test-plan-m6.md line 504; ADR-0031 line 166.
**Confirmed open by:** `backup.rs:96,328`; `rg policy_version_ref crates/` returns those two
lines plus one comment in `config-server/tests/m6_policy_daemon.rs:27`.

**Touches:** `config-server` (`backup.rs` + whatever hands it the loader's active version, which
`run.rs` already holds), plus the M6-33 row. Effort S.

---

### G-11 — `transport.rs` panics on a poisoned mutex — Medium, open, effort S

**Product defect (inconsistency).** `crates/config-grpc/src/transport.rs` uses
`.expect("dial credentials poisoned")` at lines 167 and 200, and `.expect("channel cache
poisoned")` at 184 and 210. The adjacent `crates/config-grpc/src/rotation.rs` recovers from the
same condition at lines 201-205, 249 and 255 with `unwrap_or_else(|e| e.into_inner())`.

Neither is reachable from attacker-controlled input. The problem is that the two modules
disagree about what a poisoned lock *means*: in one it is fatal, in the other it is a recovered
condition. If a panic elsewhere ever poisons a shared lock, the transport turns a local failure
into a node outage while the rotator would have carried on.

**Source:** ADR-0031 lines 229-231.
**Confirmed open by:** `transport.rs:167,184,200,210` vs `rotation.rs:201-205,249,255`.

**Touches:** `config-grpc` (`transport.rs`, four call sites). Effort S. The decision — which
convention is correct — is the work; the edit is four lines.

---

### G-08 — `TlsRotator::try_reload` leaves planes split on a mid-loop failure — Medium, open, effort M

**Product defect, documented and accepted.** `crates/config-grpc/src/rotation.rs:251-262`: the
loop calls `plane.replace(found.clone()).map_err(...)?`. The `?` returns on the first plane that
refuses, after earlier planes have already taken the new material. The node is briefly serving
two generations across its planes.

This is genuinely self-correcting: `served` is written only after every plane succeeded (the
guard is held from line 249 through the loop), so the next reload recomputes `changed == true`
and retries the whole set. The `RotationError` doc (lines 74-80), the `tls_reload_failed` line
(lines 211-226, which now says `recovery = "the next reload retries every plane"`) and
`docs/runbooks/credential-rotation.md` all say so.

**Source:** ADR-0031 lines 217-222.
**Confirmed open by:** `rotation.rs:249-268`.

**Touches:** `config-grpc` (`rotation.rs`): a two-phase swap — stage every plane, then commit —
or a rollback on failure. Effort M because the plane trait's `replace` is the commit point and
would need splitting. **Counter-argument worth keeping:** the current behaviour may be the right
one. A rollback path is new code on the credential path, and the failure it guards against is
already self-healing. Fund this only if the split window is shown to matter.

---

### M6-126 — the six-op admin audit assembly row — Medium, open (missing test row), effort M

**Missing test row, no product defect known.** Only the `ReloadTls` share exists:
`m6_126_reload_tls_is_denied_for_a_non_admin_and_audited` in
`crates/config-grpc/tests/admin_plane.rs:369`. The row as specified covers six operations in one
run — `ReloadPolicy`, `ReloadTls`, gossip add/use/remove, break-glass rollback — and each op's
`admin_op` line shape is asserted by its own feature's tests, so the uncovered claim is that all
six agree on one vocabulary in one process.

The row has no owner because it spans three workstreams' test files.

**Source:** test-plan-m6.md line 878; ADR-0031 lines 203-205.
**Confirmed open by:** `rg m6_126 crates/` returning only `admin_plane.rs:369,394`.

**Touches:** a new cross-workstream integration test, most naturally in `config-testkit` or
`config-server/tests`. Effort M — it needs a fixture that has a policy file, TLS files, a gossip
mesh and break-glass all live at once.

---

### G-12 — no daemon-level row for the schema fence — Medium, open (missing test row), effort M

**Ownership/tooling gap.** The fence itself is real and wired: `config_core::refuse_command` is
called from the apply path at `crates/config-storage/src/rocks.rs:2645` and the snapshot-install
path takes `options.command_schema` at `rocks.rs:1163,1223`. It is proven against a real
`RocksStore` state machine.

What does not exist is a row that drives it through a running daemon. The blocker is mechanical:
it needs `config-engine`'s `testing` feature, and `crates/config-testkit/Cargo.toml:12` declares
`config-engine = { workspace = true }` with no `features` key. The back door the row would use
(`ConfigNode::propose_skipping_the_schema_gate`, per test-plan-m6 line 831) lives behind that
feature and the daemon never enables it.

**Source:** ADR-0031 lines 232-234.
**Confirmed open by:** `config-testkit/Cargo.toml:12`; `rocks.rs:2645` for the fence itself.

**Touches:** `config-testkit` (`Cargo.toml` + a new row). Effort M, and the real cost is the
decision, not the row: enabling a `testing` feature in the shared testkit manifest turns the
back door on for every test crate that depends on it. That may be unacceptable, in which case
the storage-layer proof is the right answer and this should be closed as
**"won't fix, documented"** rather than funded.

---

### M6-82 — pins release by TTL, never on disconnect — Medium, open (missing test row + behaviour), effort M

**Missing test row over a real behaviour limit.** The landed row is
`m6_82_pins_are_released_when_a_walk_is_abandoned`
(`crates/config-engine/tests/m6_pagination.rs:796-820`). It advances the injected clock past the
TTL and then drives one more walk to trigger a sweep. Its own doc comment says so: "an abandoned
walk's pin is released by the TTL rather than leaking."

The plan's row (test-plan-m6 line 783) requires release "by disconnect detection at the
earliest", and requires the underlying RocksDB snapshot handles to be observably dropped. Neither
is covered. The behaviour behind it is the gap: with no disconnect detection and a sweep that
only runs on the next table operation, an *idle* node holds every abandoned pin past its TTL,
pinning SST files. That is §19.12's disk-exhaustion path.

**Source:** ADR-0031 lines 206-208.
**Confirmed open by:** `m6_pagination.rs:794-820` (TTL + sweep-on-next-walk only).

**Touches:** `config-engine` (`pagination.rs`: a timer-driven sweep, or a stream-drop hook) plus
`config-grpc` for the disconnect signal, plus the row. Effort M. A timer-driven sweep alone
closes the idle-node half and is S.

---

### M6-81 — no row holds a pin across a compaction — Medium, open (missing test row), effort S

**Missing test row.** `m6_81_a_pinned_snapshot_does_not_block_raft_apply`
(`crates/config-engine/tests/m6_pagination.rs:735-790`) holds four pins over a write burst and
asserts apply progress and the pin cap. The plan's row (test-plan-m6 line 782) additionally
requires that the M4 journal `Compact` command still applies and that M5 log purge still happens
while pins are held. The landed row's name dropped `or_compaction` and so did its body.

**Source:** ADR-0031 line 207; test-plan-m6 line 782.
**Confirmed open by:** `m6_pagination.rs:735-790`.

**Touches:** `config-engine/tests/m6_pagination.rs` only — the fixture already exists and M6-81
already drives a write burst. Effort S. This is the cheapest of the pagination three.

---

### G-10 — shutdown aborts the pollers rather than joining them — Low, open, effort M

**Product defect, believed harmless.** `crates/config-server/src/run.rs:1380, 1389, 1396` each
call `task.abort()`. A task parked inside `spawn_blocking` cannot be stopped by `abort`, so a
poller's closure can still complete a credential swap during the drain.

The ADR's own assessment is that this is harmless: the listeners are stopping and open sessions
keep the handshake material they handshook under. The defect is that the surrounding comment
claims an ordering `abort` does not provide.

**Source:** ADR-0031 lines 225-228.
**Confirmed open by:** `run.rs:1374-1396`.

**Touches:** `config-server` (`run.rs`): signal via the existing `Notify` and then `await` the
`JoinHandle` with a bounded timeout. Effort M because shutdown ordering is load-bearing —
run.rs:1374-1383 explains why the policy poller must stop before the journal gate is taken, and a
join introduces a wait where there was none.

---

### M6-72 — the ephemeral/rocks pagination parity claim is unowned — Low, open (missing test row), effort M

**Missing test row.** The id was reused. Three `m6_72_*` tests exist and all three are
`NoPin`-refusal rows: `config-client/tests/m6_pagination.rs:294`,
`config-engine/tests/m6_pagination.rs:868`, `config-grpc/tests/m6_pagination.rs:331`. The
`NoPin` backend is defined at `config-engine/tests/m6_pagination.rs:933`.

The row as planned (test-plan-m6 line 773) runs M6-65, M6-66, M6-68 and M6-69 against
`StorageKind::Ephemeral` and asserts identical observable behaviour. Nothing does that. D6.3's
ephemeral clause is therefore unproven.

**Source:** ADR-0031 line 208; test-plan-m6 line 773.
**Confirmed open by:** `rg m6_72 crates/` returning three refusal rows and no parity row.

**Touches:** `config-engine/tests/m6_pagination.rs` — parameterize the existing fixture over
`StorageKind`. Effort M, mostly fixture work. The id collision should be resolved first; the
refusal rows have squatted on M6-72 in three crates.

---

### G-01 — `handshake_timeout` is not configurable — Low, open, effort S

**Ownership gap, not a defect.** The bound works. `MtlsConfig::handshake_timeout`
(`crates/config-grpc/src/tls.rs:85,105,114`) defaults to `DEFAULT_HANDSHAKE_TIMEOUT` and is
enforced at `config-grpc/src/server.rs:256`. It also survives rotation: `read_material`
(`rotation.rs:389-402`) clones `self.template` and overwrites only the three PEM byte fields, so
a non-default timeout is preserved — the ADR's worry that it "must be threaded through
`read_material`" is already satisfied by the template clone.

What is missing is the `[tls]` config key. `crates/config-server/src/config.rs:666-720` reads
`tls.mode`, `tls.ca`, `tls.cert`, `tls.key`, `tls.allow_common_name_principals` and
`tls.watch_files_secs`. No handshake key. An operator cannot change the 10 s default.

The second half — "no daemon-level row drives it" — is also still true. The only row is
`config-grpc/tests/mtls.rs:622`, at the listener level.

**Source:** ADR-0031 lines 159-163.
**Confirmed open by:** `config-server/src/config.rs:666-720` (no key), `rg handshake_timeout
crates/` returning only `config-grpc` and its own test.

**Touches:** `config-server` (`config.rs`: one `Option<u64>` field, one default, one validation
that it is greater than zero, matching the `watch_files_secs` pattern at line 699), plus one
daemon row. Effort S.

---

### G-02 — the in-flight cap queues rather than refuses — Low, open, effort M

**Design choice, recorded as a gap.** `crates/config-grpc/src/server.rs:208-221`: the permit is
acquired *before* `listener.accept()`, deliberately. The comment says so: "Taken before the
accept, so a listener at its cap leaves arrivals in the kernel backlog rather than accepting
sockets it has nowhere to put."

Consequence: at `MAX_INFLIGHT_HANDSHAKES` a client sees a TCP-level stall, not a typed refusal,
and nothing is counted. The behaviour is bounded and safe — the node does not consume descriptors
it cannot service — but the failure is invisible to both the client and the metrics.

**Source:** ADR-0031 line 164.
**Confirmed open by:** `config-grpc/src/server.rs:205-230`.

**Touches:** `config-grpc` (`server.rs`): accept first, then `try_acquire`, then close with a
counted rejection. Effort M, and it is a genuine trade: the current shape is the one that cannot
be made to exhaust descriptors. A cheaper partial is to count and log the saturation without
changing the accept order — that is S and gets the observability without the trade.

---

### G-04 — a follower's continuation page carries no leader hint — Low, open, effort S

**Product defect.** `crates/config-engine/src/pagination.rs:506` returns
`PageTokenExpiredReason::Node` when the pin cannot exist on this node.
`ConfigError::PageTokenExpired` carries only the reason (`config-core/src/error.rs:295-298`), so
the client is told "restart the walk" but not where. The `NotLeader` path has a hint mechanism
(`config-grpc/src/error.rs:135-145,291-315`, `retcd-leader-node-id`); the page-token path does
not use it.

Documented client recovery (restart the walk) still works, which is why this is Low: a restarted
walk goes through the ordinary `NotLeader` path and gets a hint there. The cost is one extra
round trip per misdirected continuation.

**Source:** ADR-0031 lines 167-168.
**Confirmed open by:** `pagination.rs:506`, `config-core/src/error.rs:73-95,295-298`.

**Touches:** `config-grpc` (`error.rs`: attach the leader-hint trailers to
`PageTokenExpired{Node}` as well), or `config-engine` to return `NotLeader` when a leader is
known. Effort S. Worth pairing with G-01 and M6-35 as one cleanup batch.

---

### M6-35 — `restore_policy_mismatch` is emitted nowhere — Low, open, effort S

**Product defect.** `rg restore_policy_mismatch crates/` matches only a comment in
`crates/config-server/tests/m6_policy_daemon.rs:31`. The string does not appear in any `src/`
file. ADR-0027 line 122 and test-plan-m6 line 506 both describe the line as emitted at `warn`
when a restore's manifest version diverges from the active policy version.

Nothing blocks — that is correct and deliberate. The loss is one operator diagnostic. The
critic-m6 report graded it ADVISORY for the same reason (`docs/archive/work/
retcd-m4-m6-implementation/critic-m6-report.md:255`).

**Note:** M6-35 depends on M6-33. The line needs `manifest_version`, which is the field M6-33
leaves as `None`. Fund them together or M6-35 has nothing to compare.

**Source:** test-plan-m6.md line 506; ADR-0031 line 166.
**Confirmed open by:** the grep above.

**Touches:** `config-server` (`backup.rs` restore path), plus the M6-35 row. Effort S once M6-33
lands.

---

### G-03 — the drain predicate's clause 1 is a decode check — Low, open by design, effort L

**Accepted design residual, not a defect.** The predicate as amended by ruling M6-R20 has two
clauses, and the code implements both: `scan_log_for_upgrade`
(`crates/config-storage/src/rocks.rs:1712-1752`) checks that each retained entry decodes as
`Entry<TypeConfig>` (`BLOCKED_UNDECODABLE`) *and* that its index is at or below
`state_meta/last_applied` (`BLOCKED_UNAPPLIED`).

The gap the ADR records is that clause 1 alone is not a semantic check — postcard is positional,
so a decode can succeed into a *different* command. ADR-0021 (lines 316-324) and the code's own
doc comment (`rocks.rs:1700-1708`) both say this explicitly, and clause 2 is the deliberate
blast-radius bound that covers it: an entry at or below `last_applied` has already had its
effect and will never be applied again, so a lucky decode cannot reach the state machine.

**I do not recommend funding this as a defect.** Closing it properly means a tagged or versioned
command encoding — an L-sized format change with its own migration. It is listed so it is not
quietly dropped, and so a reader does not mistake the ADR's candour for an unfixed bug.

**Source:** ADR-0031 line 165; ADR-0021 lines 300-345.
**Confirmed present-as-designed by:** `rocks.rs:1712-1752`.

**Touches:** `config-core` (`command.rs` encoding) and `config-storage`. Effort L.

---

### M6-32 — CLOSED

**Was:** `Paginator::bind_policy_version` had exactly one caller and it was a test, so every page
token sealed `policy_version: None` and `PageTokenExpiredReason::PolicyVersion` could never fire
in a running daemon.

**Closed by:** `crates/config-server/src/run.rs:819` —
`paginator.bind_policy_version(loader.policy_version_cell());`. The loader owns the cell
(`config-server/src/policy.rs:259`) and publishes it at its adopt point. The row exists:
`m6_32_a_policy_adoption_invalidates_an_outstanding_page_token` at
`crates/config-server/tests/m6_pagination_e2e.rs:270`.

**Why this entry is here:** test-plan-m6 line 498 still records M6-32 as "**not implemented this
pass**" with a recommendation to land it elsewhere. The document and the code disagree, and the
code is right. Anyone triaging from the test plan alone would fund work that is already done.
The same applies to the schema fence (`SchemaTriple::decode_command` / `::admits`), which now has
a real product caller at `config-storage/src/rocks.rs:2645` via `config_core::refuse_command` —
see G-12, where what remains is only the daemon-level row.

---

## 3. Out of scope — the four deliberately unowned items

ADR-0031 lines 82-96 rule these as **operator or target-hardware responsibility**. Each needs
host-level tooling this project does not have and did not scope. They are listed so they are
visible, and deliberately **not priced as ordinary work**. Treat them as reopened only on the
user's say-so; the honest estimate for each is "a tooling project, not a task."

| id | what is not covered | what it would need |
|---|---|---|
| U-1 | VM pause / freeze simulation | A hypervisor pause primitive and a harness that can drive it. No owner in M0..M6. |
| U-2 | Power loss with a device that lies about `fsync` | A `dm-flakey`-style or fsync-lying block layer under the test harness. No owner in M0..M6. |
| U-3 | Long-running compaction under sustained load | A multi-hour sustained-load rig. §19.12's compaction clause is covered *behaviourally* by M5-23 and M6-81; the fault injection is not (test-plan-m6 line 1168). |
| U-4 | No certificate revocation checking (no CRL, no OCSP) | Revocation infrastructure. ADR-0028 lines 60-68 and 128-131 record this as a deliberate, documented limitation: CA rotation is add/use/remove and is **not** revocation. A compromised single leaf in a large fleet has no fast remedy. |

U-4 is the one of the four with a security consequence rather than a coverage consequence, and
it is the one most likely to be raised by an evaluating operator. ADR-0031 line 94 already
requires it to be listed in `docs/evidence/README.md` alongside the three fault classes so a
reader finds one list rather than two ADRs.

---

## 4. What I could not settle from reading

Nothing in section 2 is an unknown — every open/closed call above is backed by a file and line I
opened, or by a grep whose result I quote. Two calls are judgements rather than observations, and
I flag them as such:

- **G-08's severity.** Whether the split-plane window matters in practice depends on how long a
  plane's `replace` can take between the first and last plane. I did not measure it and could
  not without running the workspace. What would settle it: an instrumented `try_reload` under a
  forced mid-loop failure, timing the window. My Medium rating assumes it is short, which is what
  the doc comment claims.
- **G-12's disposition.** Whether the daemon-level schema-fence row is worth the cost of enabling
  `config-engine`'s `testing` feature across the testkit is a decision, not a finding. What would
  settle it: a count of which test crates would inherit the back door. I did not enumerate them.

I did not run cargo, per the assignment. Every claim here is from source reading and git history.

---

## 5. Lead recommendation — what to fund

Added by the lead after checking the triage, 2026-09-20. This is the part that needs a yes.

**One thing I checked and got wrong, stated because it changed the answer.** I expected G-05 and
G-06 to collide: both are "the signature does not cover enough", so I assumed both rewrite
`signature_payload` and must ship together or produce two incompatible formats. They do not.
`document_hash` covers the document bytes and the payload covers the hash
(`policy.rs:188-193`, `:328`), so a new `cluster_id` **field** is already inside what the
signature protects. G-06 needs no payload change and no flag day, exactly as the triage priced
it. G-05 cannot use the same trick: a field inside a policy document says nothing about a key
that signs some *other* surface, and domain separation only works as a prefix on the signed
bytes. So they are independent, and the L rating on G-05 is about deployment compatibility, not
code size — `signature_payload` returns a fixed `[u8; 40]` with one product caller and five test
signers.

### Fund now, before M7 resumes

| id | why now |
|---|---|
| G-06 | Critical, effort S, no flag day. An optional field and one comparison. |
| G-09 | Critical, effort M. A restart is not an attack, and today a restart clears the floor. |
| G-07, M6-33, M6-35, G-11, M6-81, G-01, G-04 | Each under a day, each already diagnosed to a line. M6-33 and M6-35 must go together or M6-35 has nothing to compare. |

That is two Criticals and seven small ones. Everything here is a fix, not a redesign.

### Defer, with the reason written down

- **G-05.** Real, and the flag day is real. The interim control is the key-scope rule already in
  ADR-0027: the policy trust key signs policy documents and nothing else. I want that rule
  restated as binding rather than advisory, and the **trigger** named: the first time anyone
  proposes a second signing surface for that key, G-05 stops being deferrable. Deferring a
  cryptographic hygiene item with no trigger is how it never gets done.
- **G-08, G-10, M6-82, M6-126, G-12, M6-72.** M effort, no security consequence. A later batch.
- **G-03.** Do not fund. I agree with the triage: clause 2 is the real bound, and closing clause
  1 properly means a tagged command encoding and its migration.

### Cheap and worth doing while we are here — withdrawn, it is already done

**I wrote this section without checking, and it was wrong.** I recommended adding U-4 (no
revocation checking) to `docs/evidence/README.md`. It is already there: item 4 in the
out-of-scope list, beside the three fault classes, exactly as ADR-0031 line 94 requires. The
triage said the ADR "already requires it to be listed" and I read a requirement as an open
action without opening the file.

That is the same failure this triage opens by warning about — funding finished work by reading
the document that records the requirement instead of the artifact that satisfies it — and I did
it in the section recommending what to fund. Nothing to do here. Recorded rather than deleted,
because the lesson is the point.

### Two ratings I am taking on trust

The triage says so itself and I did not re-derive either: **G-08's severity** assumes the
split-plane window is short, unmeasured; **G-12's disposition** turns on how many test crates
would inherit `config-engine`'s `testing` feature, uncounted. Neither is in the fund-now list, so
neither blocks this decision.

---

## 6. G-13 — found while fixing G-09, needs your call

**Added by the lead, 2026-09-20, after the work started. This is new; it was not in the triage
you approved.**

Three developers escalated independently and the answer only appears when the three are read
together. None of them could see it alone, and neither could I at triage time.

**The finding.** G-09's durable policy-version floor goes in RocksDB `state_meta`, following the
`max_command_schema` precedent — a good choice, because it needs no format bump and no
migration, which is what keeps G-09 at effort M. But `state_meta` is excluded from the snapshot
body (`snapshot.rs:78`, `NON_DATA_CFS`). So a directory restored from a backup **starts at floor
zero**.

That is a rollback path that survives the G-09 fix. Restore an old backup, the floor resets, and
an old but validly signed policy document adopts cleanly — the exact scenario G-09 exists to
refuse, reached by a different door.

**Why it is not simply "G-09 is not closed".** A restore is an operator action against the data
directory, not a restart. Whoever can restore a backup can also replace the whole directory, so
the threat model is weaker than G-09's "a plain restart is enough". But it is the same class,
and G-09 shipping with a documented hole in it is worth a decision rather than a footnote.

**The close is cheap and the material already exists.** M6-33 — which you funded, and which is
being written right now — puts `policy_version_ref` into the signed backup manifest. Restore
already writes node-local provenance into a fresh store. Seeding `state_meta/policy_version_floor`
from the manifest's recorded version during restore closes G-13 and makes M6-35's mismatch
warning reachable at daemon startup with no operator input, which is better than the
`--active-policy-version` flag M6-35 is otherwise getting.

**What I have done and not done.** Not built. I told the policy developer to document the limit
in the code where the floor is read, not just in a handoff, and I told the backup developer that
M6-33 is its enabling half so the connection is not lost. I did not widen the approved scope on
my own.

**My recommendation:** fund it, effort S now that the two halves it needs are both landing in
this same wave. It is materially cheaper today than it will be in a month, and the alternative is
shipping a Critical fix with a known bypass.

### The question

Fund the two Criticals plus the seven small ones, defer the rest as above? If you would rather
keep M6 closed and push all of it into an M6.1 after M7, say so — the two Criticals are the only
items I would argue about, and G-09 is the one I would argue hardest for, because a plain restart
is enough to trigger it.
