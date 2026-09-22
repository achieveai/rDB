# Verification developer handoff — O1 oracle, G1 grammar / generator / reducer

Date: 2026-09-20. Branch `feature/rdb-m7`. Built against foundation's C0 seed at `6893442`.
No git operations performed; the tree is left dirty for the lead to commit.

---

## 1. Outcome

**COMPLETED_WITH_RISKS.**

The O1 oracle and the G1 grammar, generator and reducer are built and green. 80 rows run across
three test binaries. Two things keep this off a clean COMPLETED:

1. Every campaign-class row is an explicit `Unavailable` naming I1 or a missing dependency. That
   is the charter's required behaviour, not a shortcut, but it means the *campaign* half of this
   package is unproven by anything except its inputs.
2. I found and fixed a real defect in my own INV-DEDUP checker (§6). It was caught by a row, but
   it had been written and reviewed in an earlier session without a fixture that would trip it.

---

## 2. Evidence

```
CARGO_TARGET_DIR=.rtargets/verification CARGO_INCREMENTAL=0 \
RETCD_TEST_LOG_DIR=.rtargets/verification/logs \
bash scripts/gate.sh test -p rdb-sim --test oracle --test scenarios --test campaign
```

Observed:

```
gate: target=.rtargets/verification scale=3 logs=.rtargets/verification/logs
     Running tests\campaign.rs
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.27s
     Running tests\oracle.rs
test result: ok. 55 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.27s
     Running tests\scenarios.rs
test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.33s
gate: test OK
```

`cargo fmt -p rdb-sim -- --check` — clean.
`cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings` — clean, no warnings.

**Workspace clippy is red, and not from my files.** `cargo clippy --workspace --all-targets`
fails in `crates/config-server/tests/m6_backup_policy.rs` (untracked, another team's in-progress
file): `no method named 'put' found for struct config_client::GrpcClient` plus a consequent
`type annotations needed`. I did not touch any `config-*` crate. Flagging it so it is not read
as an rDB regression — that is the same failure mode the standing rule was written for.

### Independence (charter hard rule 1)

```
grep -rhoE "rdb_core::[a-z_]+(::[a-z_]+)*" \
  crates/rdb-sim/tests/support/oracle.rs crates/rdb-sim/tests/support/oracle/ | sort -u
```

```
rdb_core::contracts::digest
rdb_core::contracts::ids
rdb_core::contracts::trace
```

```
grep -rnE "rdb_core::(authority|transaction|replication|publication|protection|recovery)" \
  crates/rdb-sim/tests/support/oracle.rs crates/rdb-sim/tests/support/oracle/
```
→ no matches.

Row **M7V-01** asserts this in-tree as an **allowlist**, not a blocklist, so a kernel module
nobody thought to name fails rather than passes. It also blocks the six by name, so widening the
allowlist cannot quietly admit one.

### Each checker has a trip and a near-miss

| Invariant | Trips it | Does not trip it |
|---|---|---|
| INV-ATOM | M7V-04, M7V-04b | M7V-05 |
| INV-PUB | M7V-06, 07, 07b, 08, 08b, 10, 11, 11b, 79 | M7V-09, M7V-10b, M7V-12 |
| INV-AUTH | M7V-13, M7V-14 | M7V-15 |
| INV-LIN | M7V-16, M7V-17, M7V-18 | M7V-16b, M7V-17 (quarantined), M7V-19 |
| INV-DEDUP | M7V-24, M7V-26 | M7V-24b, M7V-25, M7V-26b |
| INV-LOSS | M7V-27, 28, 28b, 29b, 29c | M7V-29 |
| INV-LIVE | M7V-30 | M7V-31 (both arms report `NotArmed`) |
| INV-ISO | M7V-32 | M7V-33 (`NotArmed`) |
| INV-VER | M7V-34 (both halves) | M7V-35 |
| INV-LAG | M7V-36, 37, 38, 39 | M7V-38b, M7V-39b, M7V-40, M7V-41 |

---

## 3. Files

All are mine and exclusively owned. **No edit was made to `tests/support/mod.rs`, and no
registration request is needed** — foundation already declares `pub mod oracle;` and
`pub mod scenarios;` there.

New test binaries:

- `crates/rdb-sim/tests/oracle.rs` (2545)
- `crates/rdb-sim/tests/scenarios.rs` (817)
- `crates/rdb-sim/tests/campaign.rs` (~500)
- `crates/rdb-sim/tests/campaign/{corpus,report,regressions}.rs`

Support layer (written across this and the prior session):

- `tests/support/oracle.rs` + `oracle/{model,checks}.rs` + `oracle/checks/{atomicity,authority,
  dedup,lag,lineage,liveness,loss,publication,version}.rs`
- `tests/support/scenarios.rs` + `scenarios/{builder,coverage,gen,grammar,mutate,reduce}.rs`

Empty directories created for the campaign to fill: `tests/fixtures/{scenarios,regressions}/`.

---

## 4. Row coverage

### Implemented and asserting

**oracle.rs (55 rows):** M7V-01, 02, 03, 03(b), 04, 04b, 05, 06, 07, 07b, 08(a), 08(b), 09, 10,
10b, 11, 11b, 12, 13, 14, 15, 16, 16b, 17, 18, 19, 24, 24b, 25, 26, 26b, 27, 28, 28b, 29, 29c,
30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 66, 67, 68, 71, 79, 81, 85.

**scenarios.rs (19 rows):** M7V-42 (+42b, 42c), 43, 43b, 44, 45, 46 (scenario half), 48 (loop
half), 49, 50 (pairing half), 59, 83, 84.

**campaign.rs (6 rows):** M7V-56, 57, 74 (dependency check only), 77 (document half), 82
(clauses a and b).

Rows suffixed `b`/`c` are sub-cases the plan's expectation column requires but does not number.
I gave them their own `fn` so a failure names the clause rather than the row.

### Stubbed with an explicit `Unavailable`, never a pass

M7V-20, 21, 47, 50 (replay half), 51, 86 — all `Unavailable{Capability(I1)}`.
M7V-82's trace-start clause — `Unavailable{Capability(I1)}`.
M7V-74 — unavailable on a missing dev-dependency (§7).
The campaign class as a whole (M7V-52..77) — one reported `Unavailable{Capability(I1)}`.

**These stubs fail when the capability lands.** Each reads
`rdb_sim::harness::environment_capabilities()` and asserts the package still reports
`Unavailable`. The day I1 flips to `Wired`, every one of them goes red and demands to be written.
A stub that hardcoded `Unavailable` would have gone on reporting it forever.

### Not touched (held, per assignment)

M7V-22, M7V-46's header half, M7V-72, M7V-87, M7V-88's `op_skipped` clause, M7V-89's op list, and
the I1-dependent rows of plan §12 beyond the stubs above. **M7V-90 is not present**: retired by
F-R13, its id is not reused, and `quorum_rule_mismatch` appears nowhere in the tree.

**Also held, and previously undisclosed: item 2 of `docs/testing/m7-log-fields.md` — the eleven
tier-2 runner lines.** Verification owns them. They unblock **Q-34, Q-38, Q-39 and Q-40**, and
none of them is emitted today. Measured against an 86-file log root from this package's own run
(`RETCD_TEST_LOG_DIR`, DuckDB with `map_inference_threshold=-1`), the whole emitted vocabulary is
`capability` 249 / `test started` 83 / `test finished` 83. All eleven of `invariant_status`,
`violation`, `capability_seen`, `coverage_cell`, `coverage_shortfall`, `coverage_unavailable`,
`shrink_step`, `shrink_result`, `campaign_run`, `mutation_caught` and `trace_header` return
**zero lines**. Verification emits no log line of its own.

Two consequences the lead must weigh rather than read past:

- `invariant_status.seeds_armed` is the log-field contract's **own named defence against the
  vacuous pass** — "a `proven` row with `seeds_armed = 0` is the vacuous pass, and Q-34's second
  statement fails the run on it". It has no producer. `coverage_shortfall` is the same shape for
  Q-38: without it, "Q-38 reports a clean sheet on a run that covered nothing".
- Until then, a zero-row result on Q-34 or Q-38..Q-40 is **indistinguishable from a clean run**.
  Those four Q-rows are dark, not green.

**Blocked on item 1** — foundation's tier-1 `TraceEvent` serialiser, in flight at the time of
writing. Nine of the eleven are the campaign runner's and the runner is I1-dependent, so they
could not be emitted today under any plan. The remaining two or three (`invariant_status`,
`capability_seen`, `trace_header`) are derivable from `Oracle::judge`'s `Report`, which
`campaign.rs` already holds — they are a real option once item 1 gives them a line format to
write into. Correction round r2 was scoped to disclose this debt, not to build it; the debt is
stated here so the lead can schedule it rather than discover it.

---

## 5. Deviations — read this section

1. **M7V-01's allowlist is three modules, not two.** The plan says `rdb_core::contracts::{trace,
   ids}`. I needed `contracts::digest` as well: `TraceKind`'s own fields are typed `Digest`, and
   the judge cannot read a field without naming its type's module. The row asserts the three and
   separately blocks the six kernel modules by name.

2. **`TraceBuilder` was moved out of the oracle subtree** — from `support/oracle/builder.rs` to
   `support/scenarios/builder.rs`. It needs `contracts::event::Budgets` and `contracts::version`
   to fill a trace header, and leaving it under `oracle/` would have forced M7V-01's allowlist
   two modules wider for a file that *writes* what the judge reads. It is an input to the judge,
   not part of it.

3. **File naming: `oracle.rs` / `scenarios.rs`, not `oracle/mod.rs` / `scenarios/mod.rs`.**
   Foundation registers them by those names and Rust forbids a crate having both. M7V-85's plan
   text says "its definition in `support/oracle/mod.rs`"; the definition is in
   `support/oracle.rs`. No behaviour differs, but the plan's path is wrong for this tree.

4. **M7V-85 counts call sites, not tokens.** The plan says the token `without_rule(` occurs in
   exactly two places. A row that greps for that literal matches its own source, so I build the
   needle at run time as `format!(".{}(", "without_rule")` and assert **zero** call sites today
   (M7V-23 is held, so there is no permitted caller yet; when it lands this becomes exactly one).
   The definition is not a call.

5. **Source rows skip whole-line comments.** M7V-01, M7V-43, M7V-49, M7V-82(b), M7V-83 and
   M7V-84 strip `//`-leading lines before scanning. Without this, `support/oracle.rs`'s own doc
   comment — which *names* `rdb_core::{authority, transaction, …}` in order to forbid them —
   failed M7V-01. Prose imports nothing. Trailing comments after code are still scanned, which is
   over-strict in the safe direction.

6. **The grammar is a second layer, not a second copy of foundation's provider enums.**
   `support/scenarios/grammar.rs` declares its own `NetworkOp`, `StorageOp`, `ControlOp` etc.
   Foundation's landed `sim::network::NetworkOp{SetLink, PlanNext, ForgeAck, ForgeNext}`,
   `storage::StorageOp{Fail, Crash, FalseDurable, ShortFlush}` and `sim::control::ControlOp` have
   different shapes and **no serde derives**, and D4 requires the scenario to serialize as the
   fixture. A scenario says "partition these two sets"; a provider op says `SetLink`. The
   lowering belongs in the runner and is keyed by `gen.rs`'s producer table. See §7 for the
   contract request if convergence is wanted.

7. **M7V-27's expectation was wrong in my first draft, and the checker was right.** I had
   asserted `Unavailable{NotArmed}`; INV-LOSS actually fires `loss_without_recovery_root`, which
   is the stronger and correct answer. I changed the row to assert the violation. Flagging it
   because it is the one place I changed a row rather than a fixture, and it was in the direction
   of a stronger assertion, never a weaker one.

### Where my code mirrors a design section word for word

Asked for after a sibling team hit silent drift. Two places:

- `support/oracle/checks/publication.rs` module docs restate design §2.5's two environment
  groundings — peer role from config-versioned topology, `Durable` from a preceding
  `durability_advance{outcome=Synced}` — close to verbatim.
- `support/scenarios/reduce.rs` module docs restate design §4.4's acceptance-predicate sentence:
  the core tuple `(checker, rule, partition, role, event_kind)` is the whole predicate and
  `faults` is recorded but never compared.

Both are restatements of a rule the code implements, so if the design changes the prose and the
code drift together silently. Neither is enforced by a row. Worth a critic's eye.

**Conversely:** I built M7V-37 (the 250 ms / 5 s resume **hold**) even though design §2.6
excludes the 1 s / 2.1 s pause **entry** ladder. These are different properties from the same
spec row — the hold is a resume condition and is checkable from the trace, the entry ladder is
kernel-b's L1. `lag.rs` says so in its docs. If the lead disagrees, that row is the one to cut.

---

## 6. Findings

**F-V1 (fixed, mine): INV-DEDUP counted a secondary's replay as a second effect.**
`checks/dedup.rs`'s `duplicate_effect` clause fired on any `batch_apply` carrying a known
identity, regardless of `role`. A correctly replicated RF3 transaction produces one primary apply
and two secondary applies, so **every clean RF3 trace with an armed dedup checker was reported as
a duplicate execution**. It went unnoticed because the checker's own unit shape had never been
fed a trace with replicas acking. Fixed: the clause now returns early unless
`role == ReplicaRole::Primary`, and the module's rule table says "primary `batch_apply`".
Caught by M7V-02, 09, 40, 66, 81 simultaneously — which is exactly what the golden trace is for.

**F-V2 (open, foundation's): the trace recorder emits no `capability` block.**
`harness/trace.rs` has `begin`/`record`/`finish` and no capability emitter, and
`Dispatcher::capability_report()` returns `[CapabilityState; 6]` while a capability block needs
all ten `PackageId`s. M7V-82's clause "the `capability` events at trace start equal, one for one,
the report the dispatcher returns" therefore has no event stream to compare against. I report it
as `Unavailable{Capability(I1)}` rather than passing. The other two clauses (both directions on
the real modules, and the source check for literal `Wired` / a const table) do run.

**F-V3 (open, foundation's): `Dispatcher`'s module fields are concrete, not trait objects.**
M7V-82(a) asks for "a module stubbed to answer `Ok` from `step`" and one stubbed to answer
`Unavailable`. There is no injection point. I test the `Module::capability` contract directly in
both directions instead, plus the real dispatcher's report. If foundation wants the row as
written, the dispatcher needs a constructor taking `Box<dyn Module>`.

---

## 7. Contract requests for foundation

1. **`config-testkit` as a dev-dependency of `rdb-sim`.** Blocks M7V-74 entirely. The row must
   parse each `docs/evidence/rdb-*.json` through `config_testkit::evidence::read_evidence` and
   compare `disclaimer` against the shared `DISCLAIMER` const. Re-typing that string in this
   crate would be exactly the "second copy" the row exists to forbid, so I did not. The row is
   present and *fails* the moment `config-testkit` appears in the manifest, which is the
   reminder. Requested line, `crates/rdb-sim/Cargo.toml`, `[dev-dependencies]`:

   ```toml
   config-testkit = { workspace = true }
   ```

2. **`Serialize, Deserialize` on `sim::network::NetworkOp`, `storage::StorageOp` and
   `sim::control::ControlOp`** — *only if* the lead wants the grammar and the provider API to
   converge. I do **not** recommend it. The two layers have genuinely different jobs (§5.6) and
   forcing one shape would either make the scenario fixture carry transport details or make the
   provider API carry scenario abstractions. My recommendation is to keep two layers and put the
   lowering in I1's runner.

3. **The tier-1 `TraceEvent` serialiser — item 1 of `docs/testing/m7-log-fields.md`.** This is a
   dependency, not a courtesy ask. Verification owes item 2, the eleven tier-2 runner lines
   (Q-34, Q-38..Q-40), and cannot emit any of them until a tier-1 line format exists to write
   into. See §4's held list for the measurement. Foundation has this in flight; naming it here
   so the ordering is on the record.

4. **No other asks.** `tests/support/mod.rs` needs no change.

---

## 8. Risks and assumptions

- **The largest risk is that nothing has run a kernel.** Every oracle row judges a trace I wrote
  by hand. M7V-88 — "every fixture and authored case is realizable by the runner" — is exactly
  the row that would catch a checker tuned to a shape the runner can never produce, and it is
  I1-dependent and unwritten. Until it runs, treat "the checker fires" as proven and "the
  checker fires *on something the kernel can produce*" as unproven.
- **The golden trace (M7V-02) is the single point of failure for arming.** Every arming condition
  in the system is pinned there and nowhere else. That is deliberate — one place to look — but it
  means a mis-shaped golden trace could disarm several checkers at once while staying green. The
  per-checker `NotArmed` rows (M7V-31, M7V-33) are the partial defence.
- **MUT-2's oracle half rests on a rewrite, not a provider fault.** `mut2_count_forged_ack` takes
  the first *refused* acknowledgement, elevates its claimed role to `RegularSecondary`, drops the
  refusal and makes the publication count it. I changed it this session so the pre-mutation run
  is genuinely clean — it previously started from a trace INV-PUB already rejected, which would
  have made the mutation prove nothing. The row now asserts the unmutated run is clean first.
  Upgrade to H1's `ForgeAck` when it lands; the assertion does not change.
- **Assumption, reversible:** `AuthorityGate::Dispatch` is the apply gate. The landed enum has
  `Admission | Dispatch | Publication | Reply` and no `Apply`; `Dispatch` is documented as
  "immediately before the storage batch is dispatched". M7V-14(a) is written against it.
- **Assumption, reversible:** a refused acknowledgement from a Shadow carries
  `AckRejectReason::StaleConfig`. The landed enum has no `NotAMember`.
- `tests/fixtures/{scenarios,regressions}/` are empty. M7V-45 and M7V-46 would pass vacuously on
  an empty directory, so both also run over generated and authored scenarios built in-process.
  M7V-50's pairing half correctly reports zero pairs.

---

## 9. Standing rule compliance

The workspace compiles at this stopping point, and every module file exists alongside the
declaration that names it. `cargo clippy -p rdb-core -p rdb-sim --all-targets -- -D warnings` is
clean. The one red thing in the workspace is another team's untracked
`config-server/tests/m6_backup_policy.rs` (§2).
