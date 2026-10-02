# Amendment to ADR-0027: G-13 is evidence, not a gate

**Status:** **APPLIED**, 2026-09-22. The prose changes are landed in
`docs/ADRs/0027-signed-policy-documents-and-rbac.md` (G-13 entry replaced, delivered-scope item
added, one sentence added to G-06) and in the three code doc comments
(`config-storage/src/rocks.rs`, `config-server/src/backup.rs` ×2). Critic verdict
PASS_WITH_RISKS with five MATERIAL findings, all applied above before landing.

**Not applied — the three delivered-scope changes are code, and unscheduled.** Items 1, 2 and 3
describe behaviour nobody has built. They are diagnostics on M6 code and the serial team is on the
M7 foundation. The ADR now states them as the delivered scope, which is a commitment, not a claim
that they exist. Do not read the ADR as describing shipped behaviour for those three.

**Decision:** yours, 2026-09-22, Shape E.
**Touches:** one Accepted ADR on a security control. Two doc comments stay exactly as they are
(`backup.rs:96-100`, `cli.rs:202-210` — they were right). Three others are corrected, because
they cite gaps that have since closed (`backup.rs:103-107`, `:280-284`, `rocks.rs:1322-1324`).

---

## Why this amendment exists

ADR-0027 charters G-13 at `:477-482`: *"the fix being to seed the floor from the backup
manifest's `policy_version_ref` at restore."*

That fix cannot be built. Not "is hard to build" — cannot, because the one path it could live on
**enforces** the condition that makes it meaningless.

**The enforcement, scoped correctly.** `crates/config-server/src/backup.rs:880-889` refuses a
restore that reuses the backup's cluster id:

> `cluster_id_reused` — "`--cluster-id {} is the cluster this backup was taken from; a restore
> must mint a new one, because reusing it is what leaves two writable authorities for one
> logical service`"

Green test: `crates/config-server/tests/m5_admin.rs:763`.

**That refusal is in the CLI, not in the storage layer, and an earlier draft of this amendment
stated it as a system invariant.** Corrected 2026-09-22 after a critic enumerated every restore
entry point. `config_storage::restore_into_fresh_store` is public and re-exported
(`config-storage/src/lib.rs:39`); it compares nothing between `new_identity` and `restored_from`,
and its own doc comment says so (`snapshot.rs:1146-1149`): the refusals "are enforced by the CLI
before this is called". What it enforces is emptiness, format version and command schema. A
**same-lineage restore already runs green**: `m6_compat_cluster.rs:604` passes
`restored_from.cluster_id == new_identity.cluster_id` and opens the directory at `:627-629`.
There is no admin RPC restore and no force flag; those two are the whole set.

**The decision survives, on a narrower and stronger argument.** The chartered fix reads the
**backup manifest**. The library path has no manifest — it takes a snapshot file and a
`RestoredFrom` struct. So the only site where the fix could be built is the CLI path, which is
precisely the path that carries the refusal. The claim the amendment needs is not "every restore
mints a new identity" but "every restore *that has a manifest to read* mints a new identity",
and that one holds by enforcement with a green test behind it.

On the CLI path, then, the manifest's `policy_version_ref` is **always** from a foreign lineage.
The floor it would seed and the version it would be compared against come from two independent
numbering systems. Every such comparison is meaningless; the ones that pass, pass by accident.

**Why this correction is recorded rather than quietly folded in.** A sufficient local check
presented as a global claim is this milestone's dominant defect class, ruled four times. This
amendment exists to correct one instance of it and reproduced it in its own first argument.
Recorded as L-R91.

**The self-contradiction.** Fifty lines earlier, the same ADR introduces **G-06** (`:407-427`):
a document names its issuing cluster, and `verify_policy` refuses one naming a different
cluster — because *"one ops key trusted by two clusters was enough for a higher-versioned
document issued for cluster B to adopt cleanly on cluster A."*

G-06 exists to stop cluster A's version numbers carrying authority into cluster B. **G-13 asks
for exactly that.** Nothing in ADR-0027 reconciles them.

**The code already said so, twice.** Both comments shipped and both are right:

- `crates/config-server/src/backup.rs:96-100` — *"Nothing checks it at restore, because the
  independently supplied policy may legitimately be older, newer or unrelated; it exists so a
  recovering operator can tell which document the data was authorized under."*
- `crates/config-server/src/cli.rs:202-210` — *"the manifest's reference is a breadcrumb for a
  human and not a validation input... Silence here is the absence of a check, not a statement
  that the versions agree."*

**Neither is retired.** They were correct. The charter was not.

**What the chartered fix would have done.** Operator restores, mints a new cluster (enforced),
issues a new signed document in a new lineage — naturally version 1. Floor seeded to the old
cluster's, say 412. `below_floor = floor > 0 && to < floor` (`crates/config-core/src/policy.rs:767-772`)
rejects it. Boot lands on `AuthzKind::NoValidPolicy` (`crates/config-server/src/run.rs:1136-1138`);
`authorize` returns `REASON_NO_VALID_POLICY` (`crates/config-core/src/policy.rs:960-963`) — deny
everything.

**Corrected 2026-09-22, and this correction is mine.** An earlier draft of this amendment said
there was **no in-band repair route**. That is false, and it was disproved by running the
scenario rather than reading it. A developer stood up a real daemon on a real restored directory
under a second cluster identity, reproduced the deny-all state, and then **repaired it in band**:
writing a document at version 413 onto the live deny-all node is adopted by the policy poller
within one tick — no restart, no break-glass flag, no admin RPC. `PolicyLoader::spawn_poller`
(`crates/config-server/src/policy.rs:466-493`) re-reads the files every `authz.poll_interval` and
calls `reload("poll")` regardless of the node's authz state, and
`m6_27_policy_arrival_restores_readiness_without_restart`
(`crates/config-server/tests/m6_rbac.rs:124`) already proves that shape for a node holding no
document. **The poller is the primary reload route and it is never closed.** The admin RPC is the
expedited path, not the only one.

**The defect is real and it is a different defect: the operator is not a prisoner, but they are
not told which number to issue.**

- `PolicyRejected::RollbackFloor` **carries both numbers** — `floor` and `incoming`
  (`crates/config-core/src/policy.rs:314-319`) — and the emitted reason is the bare token
  `"rollback_floor"` (`:358`). The `policy_rejected` line and `/health` both carry
  `reason: "rollback_floor"` and nothing else. Neither 412 nor 1 reaches the operator.
- The one message the operator *does* get names `[authz] admins` — a config key that
  `crates/config-server/src/run.rs:849` states is **not consulted at all** under signed mode. So
  the sole actionable-looking error points somewhere that cannot help.

Not a brick. A maze with the exit unmarked, and a sign pointing at the wrong wall.

---

## The amendment

### 1. Replace the G-13 paragraph at `:477-482`

**Current text:**

> **Known limit, carried rather than closed: a restore resets the floor (G-13).** `state_meta` is
> not carried in a snapshot body, so a directory restored from a backup starts at floor `0` and an
> old signed document adopts. G-09 ships with that bypass. It is recorded as a separate gap, with
> the fix being to seed the floor from the backup manifest's `policy_version_ref` at restore, and it
> is called out prominently at `RocksStore::policy_version_floor` so it is read by anyone relying on
> the floor as a security control rather than only by anyone reading this ADR.

**Proposed replacement:**

> **Known limit, carried deliberately: a restore resets the floor (G-13).** `state_meta` is not
> carried in a snapshot body, so a directory restored from a backup starts at floor `0` and an old
> signed document adopts. G-09 ships with that bypass. It is called out at
> `RocksStore::policy_version_floor` so it is read by anyone relying on the floor as a security
> control rather than only by anyone reading this ADR.
>
> **Amended 2026-09-22: the fix this entry originally named cannot be built, and is withdrawn.**
> It said to seed the floor from the backup manifest's `policy_version_ref` at restore. The
> manifest exists only on the CLI restore path, and **that path refuses a reused cluster id** —
> `cluster_id_reused` (`config-server/src/backup.rs:880`, proven by `m5_admin.rs:763`). (The
> library entry point `config_storage::restore_into_fresh_store` carries no such refusal and no
> manifest either, so it is not a site where this fix could be built.) The reference a restore
> could read is therefore always from a foreign lineage, whose version numbering is independent
> of the restored cluster's. Seeding it compares two unrelated integers: a legitimate new-lineage
> document, naturally at version 1, is refused as a rollback, and the node boots with no valid
> policy and denies every request. It is recoverable — the policy poller adopts a
> higher-numbered document without a restart — but the refusal reports only the token
> `rollback_floor`, never the two numbers it holds, so the operator is not told which version
> would be accepted; and the one actionable-looking error names `[authz] admins`, which signed
> mode does not consult.
>
> **This is the same thing G-06 above exists to prevent.** G-06 refuses a document issued for
> another cluster precisely because one cluster's version numbers must not carry authority into
> another. A floor seeded across the same boundary is that defect wearing the other hat.
>
> **`policy_version_ref` is evidence, not a gate**, which is what `backup.rs`'s and `cli.rs`'s
> doc comments on the field have always said. G-13 is therefore closed as a **design decision**
> rather than left open as a gap awaiting the withdrawn fix. What remains is stated as its own
> item below.
>
> **The residual exposure, stated plainly.** An operator who restores an old backup *and* supplies
> a correspondingly old signed document gets it adopted, with no refusal. That requires the signing
> key and a deliberate act, it is the same window that exists today, and nothing here widens it.
> Closing it needs a mechanism that does not depend on comparing version numbers across lineages;
> none is proposed here.

### 2. Add a new item beside it: what G-13 does deliver

> **Restore-time policy evidence (G-13, delivered scope).** Three changes, none a gate:
>
> 1. **The rollback refusal reports the numbers it already holds.** `PolicyRejected::RollbackFloor`
>    carries `floor` and `incoming` (`config-core/src/policy.rs:314-319`) and is emitted as the
>    bare token `"rollback_floor"` (`:358`), so neither number reaches the operator through the
>    `policy_rejected` line or `/health`. A refusal that will not say which version it would
>    accept is the whole of the difficulty this gap actually causes. Separately, the refusal an
>    operator meets first names `[authz] admins`, which `run.rs:849` states signed mode does not
>    consult.
>
>    **Constraint: the token itself is pinned and must not change.** `PolicyRejected::ALL_REASONS`
>    (`config-core/src/policy.rs:368-379`) is a fixed-length `[&'static str; 10]` holding
>    `"rollback_floor"`; it is the closed metric-label set. This item **adds fields beside the
>    token**, it never renames or splits it. Changing the string silently reshapes a metric's
>    cardinality and breaks any dashboard built on it.
> 2. **The durable floor gets a reading that distinguishes absent from zero.**
>    `RocksStore::policy_version_floor` is `.unwrap_or_default()`, so "never seeded" and "seeded
>    to 0" are one value, and G-13's founding premise was therefore unmeasurable for as long as
>    it has been written down. `policy_version_floor_cell() -> Result<Option<u64>, String>` makes
>    it checkable. Its operator surface is **one new field on `/health`**, beside `policy_state`
>    and `policy_version`. `/health` is chosen because it is the surface that still answers in the
>    state actually reproduced: a booted daemon in deny-all, with the admin plane refusing. (It is
>    served on the optional `--health-listen` socket, loopback-enforced at
>    `config-server/src/config.rs:673-678` with a test at `:1366`, so it adds no exposure; a node
>    configured without that listener has no surface for this in either state.)
>
>    **How it is wired, because the obvious wiring is wrong.** `state_and_version`
>    (`config-server/src/health.rs:141-144`) reads `policy_state` and `policy_version` from one
>    `Arc`, and **never touches the floor**. An earlier draft said the new field is "filled from
>    the same authorizer read" and cited the M6-20 anti-tearing rationale as cover; that rationale
>    is about one read of one `Arc`, and the floor is the one value it excludes. The field must be
>    read from `policy_version_floor_cell()` on the store.
>
>    **And that still leaves a sentinel on the control itself.** The gate is
>    `let below_floor = floor > 0 && to < floor;` (`config-core/src/policy.rs:767-769`). The
>    `floor > 0` *is* the absent-versus-zero sentinel, load-bearing on a security control, and it
>    sits at the end of a chain the new accessor does not cut: cell →
>    `.unwrap_or_default()` → `seed_version_floor` (`config-server/src/policy.rs:159`) →
>    `AtomicU64` → `floor > 0`. `persist_floor` (`:347`) writes the in-memory value back, so
>    absent → 0 → durable 0 is one adoption away from reachable. Wire `/health` from the cell and
>    a node can report `Some(0)` while its gate enforces nothing.
>
>    So the field must **say which of the two it is**: it reports the durable cell, and the
>    amendment states explicitly that the cell is evidence and the `AtomicU64` is the control.
>    Closing the sentinel on the gate is a separate change, out of scope here, and named as its
>    own follow-up rather than left implied.
>
>    **Not an offline `inspect-store` subcommand, for now.** One would fit — `Backup`,
>    `VerifyBackup` and `Restore` already form an offline family (`config-server/src/cli.rs:117`)
>    with a JSONL result convention, so it is cheap whenever it is wanted. It is not funded here
>    because no observation yet shows an operator stuck at a *stopped* directory. The
>    reproduction put them at a running one. If a hand-test finds otherwise, that is the evidence
>    that funds it.
> 3. **The offline backup path stops citing a gap that closed, and records the floor it can now
>    read.** Both comments that justify its `null` condition it on gap **G-09**
>    (`backup.rs:103-107`, "until the policy version floor is durable (gap G-09)"; `:280-284`,
>    "The durable policy version floor (gap G-09) is what would let this path answer honestly").
>    G-09 closed: the cell is durable at `config-storage/src/rocks.rs:197`. And the path can
>    reach it — `export_snapshot` already reads **seven** `state_meta` cells off its read-only
>    handle (`snapshot.rs:938-951`), through an `offline_keys` list (`:863-871`) that already
>    includes `max_command_schema`, the cell the floor was modelled on. `offline_meta` returns
>    `Option<T>`, so absent-versus-zero comes free here.
>
>    **Sized correctly: a separate `snapshot::offline_policy_version_floor()`, and the value
>    lands in the manifest.** An earlier draft said "one constant, one `offline_meta` call",
>    which priced the read and ignored the carry. It does **not** ride back on `SnapshotHeader`:
>    postcard is positional and not self-describing, so that declaration *is* the on-disk layout
>    (`snapshot.rs:210-221` — a field may be appended at the end, never inserted or reordered).
>    A standalone reader needs no format change at all, and the manifest is where
>    `policy_version_ref` already lives.
>
>    **What the number means, stated against the case that breaks it.** The floor and the ref are
>    not the same concept — "the version this node last had in force" against "the document in
>    force when this backup was taken". For a cleanly stopped node they coincide. **They do not
>    coincide for a node that crashed**, and ADR-0027 documents exactly that window at `:441-442`:
>    the floor is written after a successful adoption, so a crash between the two leaves it one
>    version stale. `policy_floor_not_persisted` (`config-core/src/policy.rs:344-357`, non-fatal)
>    is a second route to the same divergence. A crashed directory is the population the offline
>    path exists for, so this is not a corner — it is the main case. The doc comment says so in
>    those words; `backup.rs:280-284` already warns that "a wrong breadcrumb is followed", and
>    this field must not become one.
>
>    **Consequence to accept deliberately (C-4).** Populating the field on the offline path makes
>    offline restores start emitting `restore_policy_mismatch` (`backup.rs:995-999`) where they
>    are silent today — a cross-lineage comparison this amendment calls meaningless. False-positive
>    check: the landed M6-35 rows use the **admin-plane** backup, so no green row turns red. The
>    new line is a divergence warning against a number that is evidence, which is consistent with
>    the ruling; it is named here so it is a decision and not a surprise.
>
> *Optional, not required by this amendment:* let `--active-policy-version` also seed the floor.
> A number the operator chooses in the new lineage is the only number that means anything in it.

**Withdrawn from this amendment on 2026-09-22, before it landed** — and one of the two
withdrawals was itself withdrawn the same day. See the note below it.

- *"Restore reports divergence rather than discarding it."* Void. It already does.
  `main.rs:228-236` emits `restore_policy_mismatch{manifest_version, active_version, level:"warn"}`,
  with two landed rows: `m6_35_restore_records_a_policy_divergence_without_blocking` (`:367`) and
  `m6_35_restore_says_nothing_when_the_policy_versions_agree` (`:397`). The second asserts
  **silence when the versions agree** — so the proposed "report agreed / diverged / not-compared
  in words" would have turned a green row red.

**The gap list did not follow `096bbfa`, in five places, not two.** `ADR-0031:166` carried M6-33
and M6-35 as open and was corrected on 2026-09-22 with a dated note; `tests/m6_policy_daemon.rs`
was annotated the same day. A critic then found three more on 2026-09-22:

| Where | What is stale |
|---|---|
| `docs/progress/src/risks.json:16` | Carries the G-13 risk, including the "a restore **must** mint a new cluster identity" overclaim corrected above — it propagated out of this draft before the draft was checked |
| `docs/ADRs/0031-evidence-and-known-gaps.md:281` | Written *forward*, against item 3 landing, and repeats this draft's "four `state_meta` cells" (it is seven) |
| `docs/testing/test-plan-m6.md:504` | M6-33 still recorded as "not implemented this pass — genuine product gap"; false since `096bbfa`. The M6-35 row beside it repeats the `restore_policy_mismatch` grep claim |

**Correcting all of them is part of this amendment's scope**, because a stale gap list is what
produced a void item in a security ADR. The `risks.json` line is the sharpest of the five: it is
not a record that fell behind the code, it is an error this document manufactured and exported.

**And the correction was over-applied.** The offline-CLI item was withdrawn on the same day and
is now item 3 above. The reasoning that voided it — "the `None` is deliberate and documented
three times, so there is no value that path can honestly write" — read each doc comment up to
its condition and stopped at the conclusion. Both conditions name **G-09**, and G-09 closed in
`096bbfa`: the same commit that made the divergence item void disproved the withdrawal of the
offline item.

Worth stating because the failure is not the one this amendment was already tracking. A stale
gap list invites work that is done. This was the mirror image: a **retraction accepted on half a
citation**, agreed between two readers who had each read the same half. An overclaim gets caught
by the next reader; a retraction closes the file. A withdrawal carries the same evidence burden
as an assertion. Recorded as L-R87.

### 3. Reconcile G-06 and G-13 where they sit

Add one sentence to the **G-06** paragraph at `:407`, so a reader meets the principle before the
gap that used to contradict it:

> This principle is why G-13 below does not gate a restore on the backup's policy version: the
> same reasoning that refuses a foreign cluster's document refuses a foreign cluster's version
> number.

### 4. Update the third statement of the charter, in code

`crates/config-storage/src/rocks.rs:1322-1324` repeats the withdrawn fix:

> "Closing it needs the restore to seed this cell from the backup manifest's `policy_version_ref`,
> which is tracked separately and is not what this cell does today."

Replace the second clause so the comment stops pointing at a fix that no longer exists, and keep
the limit itself — it is still true and still worth reading before trusting the floor.

---

## What is **not** changing

- **Both doc comments stand.** `backup.rs:96-100` and `cli.rs:202-210` were right.
- **No behaviour change to the version gate.** `below_floor && !break_glass`
  (`config-core/src/policy.rs:767-772`) is untouched.
- **No change to G-09.** The durable floor still closes the plain-restart path, which is what it
  was scoped for and where the comparison has meaning.
- **`restore_into_fresh_store` keeps its signature.** Nothing in this amendment adds a parameter
  to it, so the four landed green tests that call it (`m5_snapshot.rs:1214`,
  `m5_backup_fencing_cluster.rs:218`, `m6_compat_cluster.rs:604`, `m6_evidence.rs:590`) are not
  touched.

## Evidence behind each claim

| Claim | Where I checked it |
|---|---|
| The **CLI** restore path refuses a reused identity | `config-server/src/backup.rs:880-889`; test `config-server/tests/m5_admin.rs:763` |
| The **library** path does not, and needs no manifest | `config-storage/src/snapshot.rs:1146-1149` (doc: "enforced by the CLI before this is called") and `:1163+` (signature takes a snapshot file, not a manifest); `lib.rs:39` re-export |
| A same-lineage restore is green today | `config-testkit/tests/m6_compat_cluster.rs:604` passes `restored_from.cluster_id == new_identity.cluster_id`; directory opens at `:627-629` |
| Those two are the whole entry-point set | No admin-plane restore RPC and no force flag: `cli.rs` `Command` enum `:117-161`, and a workspace sweep for `restore_into_fresh_store` |
| Documents are cluster-bound | `config-server/src/policy.rs:230-235` (`verify_policy(.., expected_cluster)`) |
| The gate is a bare integer compare | `config-core/src/policy.rs:767-772` |
| Boot lands on no-valid-policy | `config-server/src/run.rs:1136-1138` |
| Deny-all follows | `config-core/src/policy.rs:960-963` |
| Repair route **exists** (poller) | `config-server/src/policy.rs:466-493`; shape proven by `config-server/tests/m6_rbac.rs:124` |
| The refusal drops its own numbers | `config-core/src/policy.rs:314-319` (carries `floor`, `incoming`) vs `:358` (emits `"rollback_floor"`) |
| The actionable error points at an ignored key | `config-server/src/run.rs:849` |
| Floor after a real restore is `None` | measured on a restored directory via the new `policy_version_floor_cell` surface |
| Charter stated **twice**, not three times | ADR `:477-482`; `config-storage/src/rocks.rs:1322-1324`. An earlier draft added `config-server/src/backup.rs:105` as a third; that passage states the **gap**, not the fix, and the row was wrong |
| Charter contradicted twice | `config-server/src/backup.rs:96-100`; `config-server/src/cli.rs:202-210` |
| G-06's rationale | ADR `:407-427` |
| The offline `null` is conditional, not absolute | `config-server/src/backup.rs:103-107` and `:280-284` both condition it on gap **G-09** |
| G-09 has closed | `config-storage/src/rocks.rs:197` (`KEY_POLICY_VERSION_FLOOR`), `:1325`, `:1341`; commit `096bbfa` |
| The offline path can read `state_meta` | `config-storage/src/snapshot.rs:938-951` already reads **seven** cells; `offline_keys` `:863-871`; `offline_meta` `:874-896` returns `Option<T>` |
| The floor cannot ride back on the header | `config-storage/src/snapshot.rs:210-221` — postcard is positional, so the declaration *is* the on-disk layout |
| `/health` carries sibling policy fields but **not** the floor | `config-server/src/health.rs:141-144` — `state_and_version` reads `policy_state` and `policy_version` from one `Arc` and nothing else |
| `/health` is loopback-enforced, so a new field adds no exposure | `config-server/src/config.rs:673-678`, test `:1366` |
| The sentinel is on the gate, not only on the accessor | `config-core/src/policy.rs:767-769` `floor > 0 && to < floor`; chain via `config-server/src/policy.rs:159` (`seed_version_floor`) and `:347` (`persist_floor`) |
| `"rollback_floor"` is a pinned metric label | `config-core/src/policy.rs:368-379`, `ALL_REASONS: [&'static str; 10]` |
| Floor and ref diverge on the main offline case | ADR-0027 `:441-442` (crash between adoption and floor write); `config-core/src/policy.rs:344-357` (`policy_floor_not_persisted`, non-fatal) |
| Populating the offline field starts a new log line | `config-server/src/backup.rs:995-999`; M6-35's landed rows use the admin-plane backup, so none turn red |
| An offline subcommand family already exists | `config-server/src/cli.rs:117-161` (`Backup`, `VerifyBackup`, `Restore`) behind `run_offline` |

**Now observed, not inferred.** The scenario was run against a real daemon on a real restored
directory under a second cluster identity, built with the shipped CLI (`backup`, then `restore`
into a new cluster id at an advanced epoch, then `--form`). It reproduced: `authz_kind=no_valid_policy`,
`ready=false`, `policy_state={"reason":"rollback_floor"}`, client calls refused with
`REASON_NO_VALID_POLICY` as `Unavailable`, admin `ReloadPolicy` refused as `PermissionDenied`.
`v413` passes, by arithmetic accident.

**And the strongest claim in the earlier draft was refuted by that run.** I had asked for the
"no in-band repair route" claim to be attacked rather than confirmed; it was, and it fell. The
correction is folded in above. The decision itself is unaffected, because it never rested on
severity: it rests on `cluster_id_reused` making every comparison meaningless by enforcement, and
on G-06 already ruling out what G-13 asks for. Those two are what carry the amendment, and both
are code, not judgement.

**One measurement worth keeping.** The floor after a real restore reads `None` — G-13's founding
premise, measured for the first time rather than quoted from a doc comment, and a reading only
the new `Option<u64>` surface can produce. The existing accessor answers `0` for both "absent"
and "present and zero", which is why the premise had never been checked.

**Single-voter, and that is sufficient — corrected 2026-09-22.** Every run was single-voter. An
earlier draft closed by calling multi-node behaviour "untested", which invites M7 to fund a row
for it. It should not. The floor is **per-node by construction**: it is a `state_meta` cell and an
`AtomicU64` in each node's own authorizer, and policy never goes through Raft at all
(`config-core/src/state.rs` carries no policy). Nothing replicates a floor, so there is no
multi-node behaviour to discover — a second voter is a second copy of the single-voter case.
Framing a settled code question as an empirical one is the failure L-R87 exists to stop, and this
sentence was an instance of it.

**The real point in that neighbourhood, which is not a gap either.**
`ClusterPolicyVersions`/`observe_convergence` converges the policy *version* across nodes. Nothing
converges the *floor*, and nothing is supposed to. Worth one sentence in the ADR so the next
reader does not mistake the first for the second.

---

## Open after the critic's read (2026-09-22): one follow-up, deliberately not folded in

**The sentinel on the gate (C-8) is not closed by this amendment.** `policy_version_floor_cell()`
gives a true `Option<u64>` to a new reader; the control keeps reading an `AtomicU64` through
`floor > 0`. After item 2 lands, `/health` reports the cell and the gate enforces on the atomic,
and the amendment now says which is which — but they can disagree, and a node can show `Some(0)`
while its gate enforces nothing.

It is left open on purpose. Closing it changes a security control's behaviour, which is a
different kind of change from adding evidence, and this amendment's whole ruling is that G-13 is
evidence and not a gate. Folding a gate change into it would repeat the mistake the amendment is
correcting. Tracked as its own item with its own review.
