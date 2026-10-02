# Critic: adversarial read of the ADR-0027 G-13 amendment

**Date:** 2026-09-22. **Role:** Critic, team foundation. **One pass.**
**Target:** `.claude/scratchpad/conversation_memories/rdb-partition-database/adr-0027-g13-amendment.md`
against `docs/ADRs/0027-signed-policy-documents-and-rbac.md` G-13 (`:477-482`) and G-06 (`:407-427`).

**Verdict: PASS_WITH_RISKS.**

The Shape E decision stands and I do not touch it. The *implementation* the amendment specifies
has five MATERIAL defects. The most serious are **C-5** and **C-8**, both on item 2: as written it
cannot deliver its own stated purpose, it cites a rationale that excludes it, and the distinction
it buys stops at the reader while the gate it describes keeps the sentinel.

**Excluded by the lead's mid-pass note, and not counted as findings of mine:** the wrong third
citation in the "Charter stated three times" row (`backup.rs:105` states the gap, not the charter
— I read `backup.rs:103-107` independently and agree), and item 3's under-sizing (the lead's, with
one hazard added at C-2). Neither colours the verdict.

**Source discipline.** Working tree unless stated. `crates/config-storage/src/rocks.rs` is read
**at HEAD** (`git show HEAD:...`) wherever it is load-bearing, because the architect's
`policy_version_floor_cell` accessor is uncommitted; I flag each place. `g13_scratch_repro.rs`
is untracked scratch and I treat it as an observation log, never as evidence of shipped behaviour.

---

## Attack 1 — is `cluster_id_reused` unconditional? **No. Finding C-1.**

**How I enumerated.** `grep -rn "restore_into_fresh_store\|cluster_id_reused" --include=*.rs .`
over the whole workspace, then read every hit; plus `grep -rn "Restore\b\|restore("` over
`crates/config-server/src/`, `grep -rn "restore" --include=*.proto .`, and the full
`cli.rs::Command` enum. That is the complete set of ways a restored data directory is produced.

**The enumeration:**

| Path | Passes `cluster_id_reused`? |
|---|---|
| `config-server` CLI `Restore` → `main.rs:189` → `backup::restore` (`backup.rs:854`) → `restore_into_fresh_store` | **Yes**, `backup.rs:880-889` |
| `config_storage::restore_into_fresh_store` called directly (public, re-exported at `config-storage/src/lib.rs:39`) | **No** |
| Admin RPC restore | **Does not exist.** No `restore` in any `.proto`; `cli.rs` has only `Backup`, `VerifyBackup`, `Restore` |
| Break-glass / force flag on restore | **Does not exist.** No such flag in `cli.rs:161-215` |

The second row is not hypothetical. `restore_into_fresh_store` compares nothing between
`new_identity` and `restored_from`; it writes `state_meta/identity = new_identity` unconditionally
(`config-storage/src/snapshot.rs:1279-1283`). Its own doc comment says so in as many words
(`snapshot.rs:1146-1149`):

> "The refusals that make that safe — a new cluster id, an advanced epoch, a verified signature —
> are enforced by the CLI before this is called; what this function enforces is the part the CLI
> cannot, namely that the destination is genuinely empty at the moment it is written."

The only thing it enforces is emptiness, format version and command schema
(`snapshot.rs:1170-1196`).

**And a landed green test already does a same-lineage restore.**
`crates/config-testkit/tests/m6_compat_cluster.rs:600-630`:

```rust
let identity = cluster.identity(donor);
config_storage::restore_into_fresh_store(
    &restored, &identity, &snap,
    &config_core::RestoredFrom { cluster_id: identity.cluster_id, .. },
)
```

`new_identity.cluster_id == restored_from.cluster_id`, same epoch, same node id. The resulting
directory is then **opened and accepted** by a current build at `:627-629`. So a restored
directory in the source's own lineage exists in this repository today, green.

### Does this invalidate the amendment? No — but the wording must change.

I applied my own evidence burden here rather than stopping at the headline. The chartered fix is
"seed the floor from **the backup manifest's** `policy_version_ref`". `restore_into_fresh_store`
never receives a manifest — it takes `(data_dir, new_identity, snapshot, restored_from)`. The
manifest is a `config-server/src/backup.rs` concept and exists only on the CLI path, which is
exactly the path that carries the refusal. So at the one site where the chartered fix could be
written, the cluster id does always differ, and **the decision survives.** I am not asking to
reopen it.

What does not survive is the sentence the amendment proposes to put into an Accepted security ADR:

> "A restore **must mint a new cluster identity**"

That is a CLI-layer check stated as a system invariant. It is the milestone's dominant defect —
a sufficient local check presented as a global claim — reproduced inside the amendment that was
written to correct an instance of it. It has **already propagated**: `docs/progress/src/risks.json:16`
carries "A restore must mint a new cluster identity" verbatim on the live dashboard.

---

## Attack 2 — does anything depend on G-13 as chartered? **One finding, and the lead's list of three was right but not the whole picture.**

Searched `docs/` (excluding `archive`), all `crates/**/*.rs`, `*.json`, `*.toml`, and the four
M7 test plans, for `G-13`, `policy_version_ref`, `policy_version_floor`, `rollback_floor`,
"seed the floor".

**The three statements of the charter, confirmed:** ADR `:477-482`;
`config-storage/src/rocks.rs` (HEAD `:1321-1323`, working tree `:1316-1324`) — "Closing it needs
the restore to seed this cell from the backup manifest's `policy_version_ref`";
`config-server/src/backup.rs:105` (the "until the policy version floor is durable (gap G-09)"
clause). The amendment's §1/§4 cover the first and second; §2 item 3 covers the third.

**What the lead's list missed — none of it is a dependency on the chartered behaviour, but two
are artefacts that will be left stale:**

- `docs/progress/src/risks.json:16` — carries the overclaim from C-1. **Not in the amendment's scope statement.**
- `docs/ADRs/0031-evidence-and-known-gaps.md:281` — "The remaining work is tracked with the
  ADR-0027 G-13 amendment, not here, and those two doc comments are stale **until it lands**."
  ADR-0031 has already been written forward against this amendment's item 3. If item 3 is cut or
  reshaped, ADR-0031 becomes false. This is a *forward* dependency, the opposite direction from
  what attack 2 asked for, and it is load-bearing.
- `docs/testing/test-plan-m6.md:504` (M6-33 row) — still records `policy_version_ref` as
  hardcoded `None` with "there is nothing to test until the field is wired". False since `096bbfa`
  for the admin plane (`m6_backup_policy.rs:324`). Same stale-gap-list class the amendment names,
  in a fourth artefact the amendment does not list.
- `crates/config-server/tests/m6_policy_daemon.rs:27` — the amendment already scopes this. Correct.

**No test row, plan section or ADR asserts the chartered seeding behaviour.** Retiring it breaks
nothing that executes. `config-core/src/policy.rs:376` (`ALL_REASONS`) pins `"rollback_floor"` as
a closed-set metric label — item 1 must add fields beside the token, never change it; the
amendment's wording ("reports the numbers it already holds") is compatible, but does not say so.

---

## Attack 3 — the offline backup path. **The lead kept the semantic question for me. Two findings: C-3 and C-4. The sizing is his, not mine.**

### The claim chain, link by link

| Link | Verdict |
|---|---|
| Both comments condition the `null` on G-09 | **True.** `backup.rs:103-107` "until the policy version floor is durable (gap G-09)"; `backup.rs:280-284` "The durable policy version floor (gap G-09) is what would let this path answer honestly" |
| G-09 closed in `096bbfa` | **True.** `KEY_POLICY_VERSION_FLOOR` at `config-storage/src/rocks.rs:197` (HEAD) |
| `export_snapshot` reads four `state_meta` cells off its read-only handle | **Understated — it reads seven.** `snapshot.rs:938-960`: `identity`, `last_applied`, `membership`, `cluster_revision`, `compact_revision`, `retired_nodes`, `max_command_schema`. `offline_keys` (`:863-871`) lists all seven. Reading one more is indeed a constant and a call. Matches the lead's own correction. |
| "One constant, one `offline_meta` call" is the whole cost | **False** — the lead's finding, see C-2 |
| `policy_version_floor` is an honest value for `policy_version_ref` | **No. Finding C-3**, and it fails in the population the offline path exists for |

### C-2 — **withdrawn as a finding of mine; the lead raised it first.** Kept as confirmation, with one thing added.

The lead's mid-pass note already carries this: item 3's "one constant plus one `offline_meta`
call" prices the read and ignores the carry, and the right shape is a separate
`snapshot::offline_policy_version_floor()` putting the value in the **manifest**, where
`policy_version_ref` already lives. I reached the same place independently and it is not mine to
re-spend. My work below stands only as corroboration, plus **one point the note does not make and
which strengthens its conclusion.**

**Added: appending to the header is formally *allowed*, which is what makes it dangerous.**
`snapshot.rs:209-221` does not forbid the carry — read to the end of the condition:

> "a field may be appended at the end, never inserted or reordered. `SnapshotHeader::format_version`
> **cannot gate its own decode** — it lives inside the struct being decoded — so a header written
> before a field was appended does not report an unsupported format, **it runs out of bytes and
> is refused as `Malformed`** ... it is not a compatibility story, and there is none to keep,
> because snapshots and `FORMAT_VERSION = 3` both first exist in M5 — no earlier build ever wrote
> one of these files."

So a developer reading only "a field may be appended at the end" gets a sanctioned route. But the
"no compatibility to keep" clause was true in M5 and is **not true of a backup artifact**: a
`.snap` sitting in an operator's backup directory, taken with today's build, becomes `Malformed`
to tomorrow's — not `UnsupportedFormat`, which is the refusal an operator could act on. The
manifest route avoids this entirely. That is a second, stronger reason for the lead's shape, and
it belongs in item 3 beside the sizing.

**The corroboration**, briefly: the read-only handle is opened inside `export_snapshot`
(`snapshot.rs:930-936`) and dropped when it returns; `finish_artifact(&header, ...)` at
`backup.rs:286` receives only the header; `offline_meta` is private (`snapshot.rs:874`), so a new
public accessor is needed either way. Confirmed the lead's other correction too: `export_snapshot`
reads **seven** `state_meta` cells, not four — `identity`, `last_applied`, `membership`,
`cluster_revision`, `compact_revision`, `retired_nodes`, `max_command_schema`
(`snapshot.rs:938-960`, `offline_keys` `:863-871`). The draft's "four" understates it; the
argument is unaffected.

### The mechanics, for the record (superseded by the lead's shape; kept so the route is costed)

`backup_offline` (`backup.rs:278-290`):

```rust
let header = config_storage::snapshot::export_snapshot(data_dir, &plaintext)?;
...
let finished = finish_artifact(&header, &plaintext, out_dir, &name, keys, None);
```

The read-only `rocksdb::DB` is opened inside `export_snapshot` (`snapshot.rs:930-936`) and dropped
when it returns. `finish_artifact` receives `&header` and nothing else. So the floor must travel
one of three ways, and the amendment costs none of them:

1. **A new field on `SnapshotHeader`.** That is the **on-disk `.snap` format** —
   `postcard::to_stdvec(header)` at `snapshot.rs:602`, and `restore_into_fresh_store` refuses
   `header.format_version != crate::FORMAT_VERSION` (`snapshot.rs:1180-1185`). Postcard is not
   self-describing, so a new field is a wire change. This is a format bump on the artifact
   format ADR-0021 fixes — not "a constant and a call".
2. **Change `export_snapshot`'s return type.** A `pub` signature with four external call sites
   (`backup.rs:279`, `m5_backup_fencing_cluster.rs:~205`, `m6_compat_cluster.rs:~602`,
   `m6_evidence.rs:~570`).
3. **A second read-only open in `backup.rs`.** `offline_meta` is private (`fn offline_meta`,
   `snapshot.rs:874`), so this needs a new public accessor in `config-storage` *and* a second
   `DB::open_cf_for_read_only` on the same directory.

Any of the three is fine engineering. None is what the amendment says it is. An ADR amendment
that records a cost basis this wrong will be cited later as the authority for the cheap version.

### Finding C-3: the floor is a dishonest breadcrumb in exactly the case the offline path exists for

The amendment concedes the concepts differ and argues they coincide "for a cleanly stopped node".
I attacked that and it fails on a case **ADR-0027 itself already documents**, fifty lines above
G-13 (`:441-442`):

> "The residual risk is a crash between adoption and write, which leaves the floor one version
> stale and re-opens the old behaviour for exactly one restart."

So: node adopts v413, crashes before `persist_floor` (`config-server/src/policy.rs:344-357`, which
is explicitly non-fatal on failure and logs `policy_floor_not_persisted`). The directory now holds
data authorized under v413 and a floor reading 412. Under item 3 the offline manifest records
`policy_version_ref: 412`.

The offline backup path is the *disaster-recovery* path — it exists to read a directory off a
host whose node did not stop cleanly. "They coincide for a cleanly stopped node" therefore
excludes the population the path is for. And the harm is not hypothetical; `backup.rs:280-284`
names it:

> "Recording a version it cannot observe would be worse than recording none — the field is read
> by an operator mid-recovery, and **a wrong breadcrumb is followed**."

`policy_floor_not_persisted` also gives a second, non-crash route to the same divergence: a disk
error on the floor write is logged and the node keeps serving.

**On `set_policy_version_floor` replacing rather than maximising** (HEAD `rocks.rs:1339-1341`) —
I ran this down and it is **not** a worse proxy, and I withdraw the suspicion. The full doc
comment:

> "The value replaces rather than maximises, and that is deliberate — see
> `SignedPolicyAuthorizer`'s `floor` field. A break-glass rollback has to be able to move this
> down, or the next restart refuses the document the operator deliberately installed."

Replacement is what makes the floor track *last-in-force* rather than *highest-ever*, and
last-in-force is the nearer of the two concepts to `policy_version_ref`. The amendment's reading
is correct. The divergence is the crash window, not the replace semantics.

### Finding C-4: item 3 silently feeds an already-landed comparison

`backup.rs:995-999` computes `policy_divergence(manifest.policy_version_ref, req.active_policy_version)`,
emitted by `main.rs:228-236` as `restore_policy_mismatch{manifest_version, active_version, level:"warn"}`.

Today an offline-CLI backup writes `null`, so a restore of it is **silent**. After item 3 it
writes the floor, so a restore of an offline backup will start emitting `restore_policy_mismatch`
comparing *that node's last-in-force floor* against the operator's `--active-policy-version` — a
comparison that, by the amendment's own central argument, spans two lineages and is meaningless.

**False-positive check:** I checked whether this turns the two green M6-35 rows red. **It does
not.** `PolicyBackup::build` (`m6_backup_policy.rs:150-215`) takes its backup through the
**admin-plane `Backup` RPC**, which already fills `policy_version_ref` from live state
(`m6_33` asserts `Some(POLICY_VERSION)` at `:330-336`). Item 3 touches only `backup_offline`. So
this is an unstated behaviour change, not a broken row.

It is not necessarily wrong — a warn that prints both numbers is evidence, which is the shape
the amendment wants. But an amendment whose thesis is "cross-lineage comparison is meaningless"
must say out loud that it is about to start one.

---

## Attack 4 — the `/health` ruling. **The reachability ruling is correct. The wiring beside it is not. Finding C-5.**

**Is `/health` reachable under `AuthzKind::NoValidPolicy`? Yes, and the ruling is right.**
`config-server/src/health.rs:123-152` is a bare TCP handler: parse the request line, match
`GET /health`, serialize, respond. There is no authorizer call, no principal, no policy check
anywhere in `handle`. The subcommand alternative is correctly deferred. I found no code that
gates it.

**Two qualifications the amendment does not carry:**

- `--health-listen` is **optional** — `pub health_listen: Option<SocketAddr>` (`config.rs:503-504`),
  and `/metrics` rides the same listener (`:544`). A deployment without it has **no** surface for
  the floor in either state. "The surface that still answers in the state actually reproduced" is
  true of the reproduction, which had to enable that listener to be observed at all
  (`g13_scratch_repro.rs:422` polls health). ADVISORY.
- No information-exposure concern: loopback is **enforced**, not conventional —
  `config.rs:673-678` refuses a non-loopback address, with `a_non_loopback_health_address_is_refused`
  at `:1366-1370` behind it. I raised this as a candidate and am withdrawing it.

### Finding C-5 (the one I would block on): item 2's wiring cannot deliver item 2's purpose

Item 2 says:

> "Its operator surface is **one new field on `/health`**, beside `policy_state` and
> `policy_version`, **filled from the same authorizer read that already fills those two**
> (`config-server/src/health.rs:141-144`, and the anti-tearing rationale above it applies
> unchanged)."

Both halves are false, and they fail in the direction that destroys item 2's stated purpose
("distinguishes absent from zero").

**The authorizer read does not produce the floor.** `health.rs:141-143` calls
`loader.state_and_version()` → `config-core/src/policy.rs:924-938`, which reads `self.active` and
the last rejection. It never touches the floor. The floor lives in two other places:

- `SignedPolicyAuthorizer::version_floor()` (`config-core/src/policy.rs:720-722`) — an
  `AtomicU64`, seeded at boot by `seed_version_floor(recorded)` (`:712-714`) where `recorded`
  came from `policy_version_floor()`'s `.unwrap_or_default()`. **It is a `u64`. Absent and zero
  are already collapsed before it is stored.** Routing item 2 through the authorizer publishes
  exactly the value item 2 exists to replace.
- `RocksStore::policy_version_floor_cell() -> Result<Option<u64>, String>` — **working tree only**;
  at HEAD `rocks.rs:1325` has only the `.unwrap_or_default()` accessor. Reachable from the loader,
  which holds `floor: Option<Arc<dyn PolicyVersionFloor>>` (`config-server/src/policy.rs:155`),
  but only as a **store read per `/health` request**.

**So the anti-tearing rationale does not apply unchanged — it applies in reverse.** The rationale
at `health.rs:137-141` is precisely that both fields come from *one* read of *one* shared
`Arc<SignedPolicyAuthorizer>`: "this is the same fact, read once", pairing that with a later read
is "the torn payload M6-20 catches". A per-request RocksDB read is a second source at a second
instant — the shape that rationale exists to forbid. The amendment cites it as cover for the one
case it excludes.

Also: `HealthPayload` is `config_engine` (`config-engine/src/metrics.rs:430-438`), so "one new
field on `/health`" is a field on a cross-crate public struct, not a local edit.

### Finding C-8: the sentinel collapse the lead asked me to hunt — there is a third site, and it is the enforcement path

The lead named two: `RocksStore::policy_version_floor()`'s `.unwrap_or_default()` (which
`policy_version_floor_cell()` fixes) and `SignedPolicyAuthorizer::floor: AtomicU64`, whose own
doc comment is explicit (`config-core/src/policy.rs:674-676`):

> "Zero means 'nothing durable is known', not 'version zero was served': a fresh node and a node
> whose storage cannot keep a floor both sit here, and both accept their first document exactly
> as they did before."

**I swept for others** — `git show HEAD:...rocks.rs | grep -n "unwrap_or_default()\|unwrap_or(0)"`
over every `state_meta` accessor, plus the policy metrics block. Most are not collapses and I
discount them: `cluster_revision` / `compact_revision` (HEAD `rocks.rs:1106`, `:1926-1931`)
genuinely start at 0, so absent and zero mean the same thing; `max_command_schema`
(`snapshot.rs:958-960`) reasons its default explicitly — "absent on a store that has only ever
applied schema-1 commands ... the default is the fact rather than a guess". The `/metrics` block
is scrupulous in the other direction: `retcd_policy_version` is omitted rather than exported as 0,
with the reason stated (`config-engine/src/metrics.rs:1216-1226`).

**The third site is the gate itself.** `config-core/src/policy.rs:767-769`:

```rust
let floor = self.floor.load(Ordering::Relaxed);
let to = incoming.document.version;
let below_floor = floor > 0 && to < floor;
```

`floor > 0` **is** the sentinel, load-bearing, on the security control. So the collapse is not one
accessor that `_cell` repairs — it is a chain:

1. cell → `policy_version_floor()` `.unwrap_or_default()` (HEAD `rocks.rs:1326-1329`)
2. → `PolicyLoader::new` → `authorizer.seed_version_floor(recorded)` (`config-server/src/policy.rs:159`)
3. → `AtomicU64` → `floor > 0` at the gate

`policy_version_floor_cell()` cuts into that chain **only at step 1, and only for a new reader**.
Steps 2 and 3 keep the collapse for the life of the process, by design, because the gate needs a
"nothing known" value and `u64` is what it has.

**Consequence for item 2, and it is the opposite of reassuring.** Publishing a true `Option<u64>`
on `/health` while the gate reads the collapsed `u64` creates a surface that can disagree with the
control it describes. Today they agree, because nothing writes a durable 0. But `persist_floor`
(`config-server/src/policy.rs:344-357`) writes `authorizer.version_floor()` straight back to the
cell, so the round trip absent → 0 → durable 0 is one adoption away from being reachable if a
version-0 document ever verifies. `/health` would then read `Some(0)` — "a floor of zero is
recorded" — while the gate reads `floor > 0 == false` and enforces nothing. An operator told the
floor is set, on a node enforcing no floor, is worse off than one told nothing.

This is the finding I would attach to item 2 alongside C-5: the amendment should state that
`_cell` is a **reader-side** distinction only, that the gate keeps the sentinel deliberately, and
either rule out a durable 0 or say what `/health` shows when the two disagree.

---

## Attack 5 — multi-node. **The silence is correctly out of scope. The *framing* of it is the defect. Finding C-6.**

**Argued both ways, with evidence.**

*For "defect":* the floor is written per node by that node's own loader after its own adoption
(`config-server/src/policy.rs:344-357`), from documents each node polls off its own filesystem
(`spawn_poller`, `:466-493`). Policy documents do not travel through Raft — `grep -rn "policy"
crates/config-core/src/state.rs` returns nothing. So divergent floors are guaranteed, not merely
possible: any node down when a document is issued holds a stale floor indefinitely. Under item 3,
two offline backups taken from two nodes of one cluster at one instant carry **different**
`policy_version_ref` values, for a field whose doc comment reads as a statement about the cluster
("the signed policy document that was in force when the backup was taken", `backup.rs:96`).

*For "out of scope", which is where I land:* nothing in the amendment enforces on any of this.
`policy_version` on `/health` is already node-local with exactly the same property, and ADR-0027
already establishes out-of-band per-node distribution. Adding a node-local floor beside a
node-local version changes no invariant. Multi-node is genuinely orthogonal to Shape E.

**Finding C-6 is the framing.** The amendment closes:

> "**Still unverified:** every run was single-voter. Whether a multi-node cluster behaves the same
> when only some nodes carry a floor is untested."

That reads as an open empirical question awaiting a row. It is not. The code answers it: floors
are per-node by construction, divergence is designed, and there is already a convergence surface
for the *version* — `ClusterPolicyVersions` / `observe_convergence`, wired into the poller at
`policy.rs:468-483` — with no counterpart for the floor. Leaving it as "untested" invites M7 to
fund a row for a settled question, which is the same stale-gap-list failure the amendment's own
L-R87 note was written to stop. It should be restated as a design property plus the one real
asymmetry (version converges, floor does not).

---

## Findings

| # | Criterion | Location | Evidence | Consequence | Severity | False-positive check | Closure |
|---|---|---|---|---|---|---|---|
| **C-1** | An ADR must not state a layer-local check as a system invariant | Amendment §1 proposed text; propagated to `docs/progress/src/risks.json:16` | `snapshot.rs:1146-1149` assigns the refusal to the CLI; `restore_into_fresh_store` compares nothing (`:1279-1283`); `m6_compat_cluster.rs:600-630` performs a same-lineage restore and opens it green | ADR-0027 records a false invariant on a security control; a future in-process or RPC restore inherits no refusal and no warning | **MATERIAL** | Traced whether it breaks the decision — it does not: the chartered fix needs a manifest, which only the CLI path has, so the refusal does cover the fix's only possible site | Reword to "the `restore` subcommand refuses a reused cluster id (`backup.rs:880`, `m5_admin.rs:763`); `restore_into_fresh_store` enforces only that the destination is empty". Correct `risks.json:16`. |
| **C-2** | *Withdrawn as mine — the lead raised the sizing first.* One addition only: appending the floor to `SnapshotHeader` is **permitted** by `snapshot.rs:209-221`, and that is the hazard | Amendment §2 item 3 | `format_version` "cannot gate its own decode", so a `.snap` taken by today's build decodes as `Malformed`, not `UnsupportedFormat`, under a build with the field appended | A developer following the sanctioned "append at the end" route silently invalidates every backup artifact already on disk | **ADVISORY** (the sizing itself is the lead's finding, not mine) | Read the whole passage including the "no compatibility story to keep" clause — true of M5 snapshots, **not** of stored backup artifacts | Item 3 names the manifest route and states this as the reason, not only the sizing |
| **C-8** | A new reader-side distinction must not be able to contradict the control it reports | Amendment §2 item 2; `config-core/src/policy.rs:767-769` | `below_floor = floor > 0 && to < floor` — the gate **is** the sentinel; the collapse survives `_cell` through `seed_version_floor` (`config-server/src/policy.rs:159`) for the process lifetime; `persist_floor` (`:347`) writes the in-memory value back | `/health` could report `Some(0)` on a node whose gate enforces nothing; an operator told the floor is set is worse off than one told nothing | **MATERIAL** | Swept every `state_meta` `unwrap_or` at HEAD and discounted `cluster_revision`, `compact_revision`, `max_command_schema` — 0 is a true value or the default is reasoned; `/metrics` omits rather than zeroes (`config-engine/src/metrics.rs:1216-1226`) | Item 2 states `_cell` is reader-side only, that the gate keeps the sentinel deliberately, and either rules out a durable 0 or says what `/health` shows when they disagree |
| **C-3** | An ADR must not assert a semantic equivalence its own text contradicts | Amendment §2 item 3, "They coincide for a cleanly stopped node" | ADR-0027 `:441-442` (crash between adoption and floor write); `policy.rs:344-357` (`policy_floor_not_persisted`, non-fatal); harm named at `backup.rs:280-284` | Offline backup of a crashed node records a floor one version stale as "the version in force"; the field's own comment says a wrong breadcrumb is followed | **MATERIAL** | Checked `set_policy_version_floor`'s replace-not-maximise semantics as an alternative cause and **withdrew** it — replacement makes the floor *closer* to the ref, not further (HEAD `rocks.rs:1339-1341`) | Item 3's doc comment must name the crash window explicitly, not "a cleanly stopped node" |
| **C-4** | A change that feeds an existing comparison must say so | Amendment §2 item 3 vs `backup.rs:995-999`, `main.rs:228-236` | Offline `null` → silence today; floor → `restore_policy_mismatch{manifest_version, active_version}` after | Restores of offline backups start warning on a cross-lineage comparison the amendment calls meaningless | **MATERIAL** | Verified the M6-35 rows use the **admin-plane** backup (`m6_backup_policy.rs:150-215`), so no green row turns red | One sentence in item 3 stating the new emission and why a warn is consistent with evidence-not-gate |
| **C-5** | A specified surface must be able to carry the value it is specified for | Amendment §2 item 2 | `state_and_version` never reads the floor (`config-core/src/policy.rs:924-938`); `version_floor()` is `AtomicU64` with absent already folded (`:712-722`); `health.rs:137-141`'s rationale is explicitly *one* read of *one* `Arc` | Either `/health` publishes the collapsed `u64` (item 2 buys nothing) or it takes a second per-request store read (the tearing M6-20 exists to catch) — and the amendment cites that rationale as cover | **MATERIAL**, and the one I would hold the amendment for | Confirmed the plumbing exists (`policy.rs:155` holds `floor`), so this is a specification error, not infeasibility | Item 2 must say which read, and either extend `state_and_version` to return the floor from the loader's `floor` handle under one lock, or state the tearing exposure and why it is accepted |
| **C-6** | An open question must not be recorded as untested when the code settles it | Amendment, "Still unverified" | Floor written per node (`policy.rs:344-357`); documents polled per node (`:466-493`); no policy in `config-core/src/state.rs`; version convergence exists (`ClusterPolicyVersions`, `:468-483`), floor convergence does not | Invites M7 to fund a row for a settled question — the failure class L-R87 was written to stop | **ADVISORY** | Argued the "defect" side too: nothing enforces on the floor and `policy_version` is already node-local, so multi-node is genuinely orthogonal to Shape E | Restate as designed per-node behaviour plus the version-converges / floor-does-not asymmetry |
| **C-7** | The gap-list correction must cover every stale artefact | Amendment's §2 scope statement | `docs/testing/test-plan-m6.md:504` still says `policy_version_ref` is hardcoded `None` with "nothing to test until the field is wired" — false since `096bbfa` on the admin plane | A fourth instance of the exact failure the amendment is correcting is left in place | **ADVISORY** | Confirmed against `m6_backup_policy.rs:324-336`, which is green | Add it to the amendment's scope beside `risks.json` and `m6_policy_daemon.rs:30-32` |

**What holds, confirmed rather than assumed:** the G-06 / G-13 contradiction is real and correctly
stated (`:407-415` vs `:477-482`). `/health` is reachable under deny-all. The poller repair route
is real and unconditional (`policy.rs:479-493` reloads regardless of authz state). The two doc
comments the amendment preserves are correct. `ALL_REASONS` pins the token but not the fields.
Nothing in the repository asserts the chartered seeding behaviour.

---

## What I could not check

- **The 2026-09-22 reproduction.** I did not run anything (no cargo, one pass). Everything I say
  about the deny-all state is read from source, except where I cite `g13_scratch_repro.rs`, which
  is untracked scratch and which I have treated as an observation log only.
- **Whether `096bbfa` contains what is claimed.** I read the working tree and HEAD, not
  `git show --name-only 096bbfa`.
- **`m5_admin.rs:763` green.** I read the assertion, not a run.
- **Whether adding a `SnapshotHeader` field needs a `FORMAT_VERSION` bump in practice.** I
  established postcard + format gate; I did not trace how `retired_nodes` and
  `max_applied_command_schema` were landed, so C-2's route 1 may be cheaper than I state. The
  finding does not depend on it — routes 2 and 3 are not free either.
- **`m6_evidence.rs` / `m5_backup_fencing_cluster.rs` restore call sites** were read for the
  cluster id only, not in full.

---

## The single most likely way this amendment is still wrong

**Item 2 gets built from the amendment's own sentence, and publishes the wrong number.**

C-5 is the finding that survives review by looking correct. "Filled from the same authorizer read
that already fills those two" is a precise-sounding instruction that a developer can follow
literally, and following it literally yields `authorizer.version_floor()` — an `AtomicU64` in which
absent and zero were already merged before it was stored. The field lands on `/health`, the row
goes green, the amendment is satisfied, and the distinction item 2 exists to create does not
exist. Nothing downstream would catch it, because nothing enforces on the value: it is evidence,
and evidence that is quietly wrong is the failure mode `backup.rs:280-284` already named — a wrong
breadcrumb is followed.

It is also the amendment's own pattern one more time. The claim was assembled by reading
`health.rs:141-144`, seeing two policy fields filled from one call, and concluding a third would
come from the same place — reading the passage up to its condition and stopping at the conclusion.
The condition is in the two lines above it: *one* read of *one* `Arc<SignedPolicyAuthorizer>`,
because M6-20. The floor is not on that `Arc`.

The lead's mid-pass note already forbids the authorizer route, which closes the crude version of
this. **C-8 is the version that survives that instruction**: route it from
`policy_version_floor_cell()` exactly as instructed, and `/health` still ends up describing a
control that reads a different value, because `seed_version_floor` re-collapses the cell at boot
and `floor > 0` at `config-core/src/policy.rs:769` is the gate. The correct wiring produces a
true `Option<u64>` on the surface and leaves the sentinel on the control, and the amendment does
not say which of the two an operator is reading.
