# dev-m6-grpc handoff — G-11 and G-01

**Date:** 2026-09-20 · **Branch:** `feature/rdb-m7` · **Target dir:** `.rtargets/dev-m6-grpc`

**Outcome: COMPLETED_WITH_RISKS.** Both gaps closed with tests. Every acceptance row has been
executed and observed green. The one risk is a timing assertion, priced in section 7.

---

## 1. What I decided on G-11, and why the neighbour is right

**Decision: recover (`into_inner`), matching `rotation.rs`.** The assignment said the opposite
fix was a legitimate outcome if transport's state genuinely cannot be trusted after a panic
mid-update, so I checked that rather than assuming.

It can be trusted, and the argument is about *reachable intermediate states*, not about
convention:

- **`dial: Mutex<Arc<MtlsConfig>>`.** The only mutation is `*dial = Arc::new(mtls)` — a single
  assignment of one `Arc` — followed by `generation.fetch_add`. Neither can unwind. There is no
  multi-step edit for a panic to interrupt, and the guarded value has no invariant *between*
  fields, because it has one field.

- **`channels: Mutex<ChannelCache>`.** This one does have a real invariant, stated in its own
  doc comment: the generation must never lag the map's contents, or a channel authenticated
  under withdrawn material gets handed out. Its only multi-step edit is

      cache.channels.clear();
      cache.generation = generation;

  which clears the map **before** it records the generation. An edit interrupted between the
  two leaves an *empty map under a stale generation*. The next dial sees the generation still
  stale, clears an already-empty map, and re-dials. That is the safe end of the range. The
  dangerous state — a fresh generation over stale channels — is not reachable, because it is
  the opposite order to the one the code writes.

  The other write, `cache.channels.insert(key, channel)` at the end of `channel()`, is already
  guarded by a re-read of the generation, so it cannot introduce the bad pairing either.

So panicking buys **no** safety here. What it costs is blast radius. Poisoning can only arise
from a panic elsewhere while a guard is held; with `.expect()`, that one unrelated panic makes
every subsequent peer dial panic, permanently, on a node whose Raft transport is otherwise
healthy. The rotator, on identical material, would have carried on. Turning a local failure
into a node outage on the consensus transport is the worst available reaction.

**The triage undercounted the sites.** It names four (167, 184, 200, 210). There were **six**
spellings of this decision in one file:

| site | before | note |
|---|---|---|
| `cached_endpoints()` :132 | `.expect("channel cache poisoned")` | **missed by the triage** |
| `reload()` :167 | `.expect("dial credentials poisoned")` | |
| `channel()` :184 | `.expect("channel cache poisoned")` | |
| `channel()` :200 | `.expect("dial credentials poisoned")` | |
| `channel()` :210 | `.expect("channel cache poisoned")` | |
| `Debug` impl :94 | `.map(..).unwrap_or(0)` | **a third convention** |

The `Debug` one is worth calling out: a poisoned cache made a *diagnostic* silently report zero
cached channels. The file disagreed with itself as well as with its neighbour. All six now use
`unwrap_or_else(|e| e.into_inner())`, and `Debug` delegates to `cached_endpoints()` so there is
one spelling left, not two.

**Where the reason is written down:** a new `# A poisoned lock is recovered, not fatal` section
in the `transport.rs` module docs, naming `crate::rotation` as the shared convention and giving
the clear-before-generation argument above. `rotation.rs` was not edited — it is not mine, and
it was already on the right side.

---

## 2. What changed for G-01

`handshake_timeout` was already enforced (`server.rs:256`) and already survived rotation
(`read_material` clones `self.template`). What was missing was the `[tls]` key and any row that
drove the bound through a daemon. Both now exist.

The `[tls]` struct is **`crates/config-server/src/config.rs::TlsSection`** — *not* `config-core`,
so the stop-and-escalate trigger did not fire. I named it to the lead before any edit.

---

## 3. Files changed

### Mine (edited directly)

| file | change |
|---|---|
| `crates/config-grpc/src/transport.rs` | six lock sites → `into_inner`; module-doc section giving the reason; new test |
| `crates/config-grpc/src/lib.rs` | `DEFAULT_HANDSHAKE_TIMEOUT` added to the crate-root re-export |
| `crates/config-server/tests/m6_tls_daemon.rs` | **new file** — the two G-01 daemon rows |

`crates/config-grpc/src/tls.rs` was **not** changed. It already had the field, the default
constant and the `with_handshake_timeout` setter; adding anything there would have been
duplication.

### Handed to the lead as exact diffs (lead owns these files, applied verbatim)

| file | change |
|---|---|
| `crates/config-server/src/config.rs` | `TlsSection.handshake_timeout_ms: Option<u64>`; `TlsMaterial.handshake_timeout: Duration`; the parse/default/zero-refusal in `validate`; `Debug` field; the default unit test |
| `crates/config-server/src/run.rs` | `tls_mode()` → `.with_handshake_timeout(material.handshake_timeout)` |
| `crates/config-server/tests/support/mod.rs` | `NodeOptions.tls_handshake_timeout_ms` + TOML emitter (lead wrote this one first) |

I verified all of it landed as specified before running: `config.rs:126,572,586,709,726`,
`run.rs:1211`, `support/mod.rs:267-271`.

---

## 4. Criterion → test → evidence

| # | criterion | test | result |
|---|---|---|---|
| 1 | transport and rotation agree on poisoned locks; a test covers the changed path | `transport::tests::a_poisoned_lock_does_not_take_the_transport_down` | **pass** (§5.1, red first at §5.0) |
| 2 | `handshake_timeout` settable in `[tls]`, defaults to today's constant, existing files unchanged | `config::tests::the_handshake_bound_defaults_to_the_constant_it_replaced` + `g01_a_document_without_the_key_starts_unchanged` | **pass** (§5.2) |
| 3 | a daemon-level row drives a handshake past the configured bound and observes it cut off | `g01_a_stalled_handshake_is_cut_off_at_the_configured_bound` | **pass** (§5.2) |
| 4 | `gate.sh fmt` / `lint` / `test -p config-grpc` green | — | see §5.3 and §6 |

The default is the constant itself (`None => config_grpc::DEFAULT_HANDSHAKE_TIMEOUT`), not a
copied literal, so "defaults to today's value" is true by construction rather than by a number
that can drift from ADR-0028's.

---

## 5. Commands and real output

### 5.0 G-11 red first

    $ export CARGO_TARGET_DIR=.rtargets/dev-m6-grpc CARGO_INCREMENTAL=0
    $ bash scripts/gate.sh test -p config-grpc --lib a_poisoned_lock

    running 1 test
    test transport::tests::a_poisoned_lock_does_not_take_the_transport_down ... FAILED

    ---- transport::tests::a_poisoned_lock_does_not_take_the_transport_down stdout ----
    thread '...' panicked at crates\config-grpc\src\transport.rs:519:17:
    deliberate: poisoning this lock is the precondition under test
    thread '...' panicked at crates\config-grpc\src\transport.rs:132:14:
    channel cache poisoned: PoisonError { .. }

    test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 27 filtered out

Failing at `transport.rs:132` — `cached_endpoints`, the site the triage missed.

### 5.1 G-11 green

    $ bash scripts/gate.sh test -p config-grpc --lib transport::

    test transport::tests::reloading_an_insecure_transport_is_refused ... ok
    test transport::tests::a_reload_empties_the_pool_and_bumps_the_generation ... ok
    test transport::tests::a_refused_reload_changes_nothing ... ok
    test transport::tests::status_codes_map_to_the_right_transport_error ... ok
    test transport::tests::a_poisoned_lock_does_not_take_the_transport_down ... ok
    test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 23 filtered out; finished in 0.09s

### 5.2 G-01 daemon rows

    $ bash scripts/gate.sh test -p config-server --test m6_tls_daemon

    gate: target=.rtargets/dev-m6-grpc scale=3 logs=.../test-logs/20260920-234523-130584
    == test
        Finished `test` profile [unoptimized + debuginfo] target(s) in 1.98s
         Running tests\m6_tls_daemon.rs (...\m6_tls_daemon-6ab3b423b26ce038.exe)

    running 2 tests
    test g01_a_stalled_handshake_is_cut_off_at_the_configured_bound ... ok
    test g01_a_document_without_the_key_starts_unchanged ... ok

    test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.70s

    gate: test OK

Note `scale=3`: the timing row passed with `RETCD_TEST_DEADLINE_SCALE=3` in force, which is the
loaded-host setting.

### 5.3 The full config-grpc suite

    $ bash scripts/gate.sh test -p config-grpc

**Read the heading literally: this command did not do what its name says.** See 5.5 — it ran
the whole workspace, because `-p` was being silently ignored. Every config-grpc row below is
real and comes out of that run; the surrounding workspace rows are in 5.5.

    $ bash scripts/gate.sh test -p config-grpc

    gate: target=.rtargets/dev-m6-grpc scale=3

    Running unittests src\lib.rs (...\config_grpc-8bc988d6cc1fff4e.exe)
    running 30 tests
    ...
    test transport::tests::a_poisoned_lock_does_not_take_the_transport_down ... ok
    test transport::tests::a_reload_empties_the_pool_and_bumps_the_generation ... ok
    test transport::tests::a_refused_reload_changes_nothing ... ok
    test transport::tests::reloading_an_insecure_transport_is_refused ... ok
    test result: ok. 30 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

    Running tests\admin_plane.rs        test result: ok. 4 passed;  0 failed
    Running tests\client_plane.rs       test result: ok. 9 passed;  0 failed
    Running tests\m4_watch_transport.rs test result: ok. 7 passed;  0 failed
    Running tests\m4_watch_wire.rs      test result: ok. 7 passed;  0 failed
    Running tests\m6_pagination.rs      test result: ok. 5 passed;  0 failed
    Running tests\m6_rbac.rs            test result: ok. 9 passed;  0 failed
    Running tests\mtls.rs
      test m6_45_a_stalled_handshake_is_bounded_and_counted ... ok
                                        test result: ok. 10 passed; 0 failed
    Running tests\peer_plane.rs         test result: ok. 11 passed; 0 failed

**92 config-grpc rows, 0 failed.** That includes `mtls.rs`'s existing listener-level M6-45 row,
which my daemon row complements rather than replaces, and the three sibling `transport::tests`
rows that exercise the same two locks on the unpoisoned path — the ones that would have caught
`into_inner` if it had changed ordinary behaviour.

An earlier attempt at this same command is worth recording because the failure mode is
disguised. It died at `EXIT=101` with

    LINK : fatal error LNK1104: cannot open file
      ...\.rtargets\dev-m6-grpc\debug\deps\m5_backup_fencing_cluster-b416abd086dcb80d.exe
    error: could not compile `config-testkit` (test "m5_backup_fencing_cluster")

That is the two-cargo-invocations-on-one-target-dir collision AGENTS.md documents, and it was
my own fault: my waiter loop polled for *non-empty output* instead of *process exit*, so I
launched a second run on top of a live one. It reads as a build error, not a collision. It
produced no config-grpc rows at all, and nothing in this handoff rests on it.

### 5.4 fmt and lint

    $ bash scripts/gate.sh fmt
    EXIT=0            (no "Diff in" lines)

    $ bash scripts/gate.sh lint
    EXIT=0            (zero warning/error lines)

`fmt` was red earlier in the session on two other agents' in-flight files
(`config-core/src/policy.rs`, `config-storage/src/rocks.rs`); they have since formatted them.
Mine were clean throughout, checked directly with
`rustfmt --edition 2021 --check` on all three.

**No new build warnings.** `lint` produced zero.

### 5.5 Every scoped `gate.sh test` run before today ran the whole workspace

Worth the lead's attention independently of G-11 and G-01, and it is already half-fixed in the
working tree by someone else. The committed `run_test` was

    run_test() { echo "== test";   cargo test --workspace --no-fail-fast "$@"; }

so `scripts/gate.sh test -p config-grpc` expanded to
`cargo test --workspace --no-fail-fast -p config-grpc`. Cargo does not reject `--workspace -p x`;
it ignores the `-p` and runs the workspace. The uncommitted diff now in `scripts/gate.sh` drops
`--workspace` when the caller names a package, and its comment records the same failure from the
other direction — a "scoped" run that actually executed rdb-sim's suite under the name
`-p config-server`.

I did not write that fix and I have not touched `scripts/gate.sh`. My run started before it
landed, which is exactly why 5.3 is a workspace run. Two consequences:

1. **Any agent who reported a scoped `gate.sh test -p <crate>` run was reporting a workspace
   run.** If they read only the tail, they may have attributed someone else's failure to
   themselves, or — worse — read a `test result: ok` from an unrelated crate as their own.
2. `--test <name>` was never affected. `gate.sh test -p config-server --test m6_tls_daemon`
   in 5.2 really did run only that target, because `--test` filters by target name regardless
   of `--workspace`. That row stands.

**The run ended `EXIT=101` with seven failing rows, none of them mine.** Listed so the lead can
route them; I repaired none, per the cross-fire instruction.

| Row | Failure | Reading |
|---|---|---|
| `e2e_46_daemon_break_glass_rollback_is_audited` | assertion on a `config_server::policy` `policy_loaded` line, `break_glass: true`: "the restart itself is a first load, not a rollback" | **dev-m6-policy — the only one that looks like a real logic defect** |
| `m6_106_evidence_backup_restore_rpo_rto` | `generate live state: DeadlineExceededUnknownOutcome` after 129.78s | load — see below |
| `e2e_47_daemon_evidence_run_produces_every_artifact` | "the evidence suite did not pass" — the only failing row inside it is `m6_106` | cascade of the above, not independent |
| `g_04_a_followers_page_token_refusal_carries_the_leader_hint` | `seed put: DeadlineExceededUnknownOutcome` | dev-m6-pagination's new G-04 row; a deadline, not an assertion |
| `m6_95_rolling_restart_v1_to_v2_with_writes_in_flight` | `read deadline exceeded before the linearizable barrier` (9 passed, 1 failed) | load; the daemons restarted fine, the read after the upgrade timed out |
| `m1_12_isolated_former_leader_rejects_write` | `read after heal: Unavailable { reason: "leader unknown" }` | load; an election that did not complete |
| `harness_real_gossip_observes_peers_and_accepts_injection` | `os error 10013` binding UDP `127.0.0.1:50775` | host; `WSAEACCES`, a Windows excluded-port-range or in-use collision |

**`m6_106` is provably load, and that is worth more than my opinion of it.** The same gate run
executed `m6_evidence` twice: once shelled out from `e2e_47` while the whole workspace was
running, where `m6_106` failed after 129.78s, and once as its own target later, where it
passed:

    test m6_106_evidence_backup_restore_rpo_rto has been running for over 60 seconds
    test m6_106_evidence_backup_restore_rpo_rto ... ok
    test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 60.39s

Same binary, same commit, same scale factor, different concurrency. That also disposes of
`e2e_47`, which only failed because it shelled out to the loaded copy.

Five of the seven are a deadline, an election or a socket — the vocabulary of a saturated host,
with six-plus rustc processes and several agents running clusters at once. Note these rows *are*
already getting `RETCD_TEST_DEADLINE_SCALE=3`; what the scale cannot stretch is Raft's own
timers, which AGENTS.md calls out deliberately, so a loaded host can still miss an election or a
barrier however generous the scale. Only `e2e_46` asserts on content rather than time, and it is
in `config-server/src/policy.rs`'s vertical, which I was told not to touch.

**Why none of these can be mine, argued rather than asserted.** Two independent reasons:

- `transport.rs` changes behaviour *only when a lock is poisoned*, and no path in that module
  panics while holding either guard. In any run where nothing panicked — all of these — the
  emitted behaviour is identical to before the change. The three sibling `transport::tests`
  rows in 5.3 exercise the unpoisoned path and are green.
- G-01 could in principle break daemon start-up, since it adds a field to `TlsSection` under
  `deny_unknown_fields` and a `Duration` to `TlsMaterial`. It did not: `e2e_daemon` is
  **29 passed, 2 failed**. Twenty-nine daemons started through the new configuration path. A
  broken `TlsSection` fails all thirty-one, not two, and neither of the two mentions TLS,
  handshake, transport or configuration parsing.

---

## 6. Deviations, and why

1. **`gate.sh fmt` and `gate.sh lint` are now both green** (`gate: fmt OK`, `gate: lint OK`,
   zero warning lines). Earlier in the session `fmt` was red on two other agents' in-flight
   files; they have since formatted them. I never formatted another agent's file.

3. **`tls.rs` unchanged.** Owned, but it already had everything G-01 needed.

4. **The shared config-server files became the lead's mid-task.** My diffs, lead's edits.

5. **I caused one `LNK1104`.** I started a second `gate.sh` against `.rtargets/dev-m6-grpc`
   while an earlier one was still running — my waiter checked for output, not for process exit.
   Exactly the failure AGENTS.md documents. The re-run alone is the evidence that counts.

---

## 7. Risks

1. **The timing assertion is the one thing that could flake.** The row asserts
   `elapsed >= 250ms` and `elapsed < DEFAULT_HANDSHAKE_TIMEOUT` (10 s).
   - The *lower* bound cannot be made flaky by a slow host — only by a host that closes the
     socket sooner than the listener was told to, which is the defect it exists to catch.
   - The *upper* bound has 40x headroom and is deliberately **not** scaled by
     `RETCD_TEST_DEADLINE_SCALE`: it is the discriminator between "read the configured value"
     and "fell back to the 10 s default", so scaling it would let the bug through on exactly
     the loaded hosts where the scale is set. Observed passing at `scale=3`.
   - Nothing sleeps. The wait is a read to EOF under `deadline(20)` plus a bounded
     `poll_until_async` on `/metrics`.

2. **Only one `TlsMaterial` struct literal exists** (`config.rs:721`, lead-confirmed), so the
   new field reaches nowhere unexamined. If another agent adds a second literal, it breaks the
   build rather than defaulting silently — which is the right failure.

3. **The re-exported constant is now public API.** `config_grpc::DEFAULT_HANDSHAKE_TIMEOUT`
   was already reachable as `config_grpc::tls::DEFAULT_HANDSHAKE_TIMEOUT`; the root re-export
   is additive and breaks nothing.

---

## 8. Spotted, not touched

- **G-08** (`rotation.rs:251-262`) — still returns on the first plane failure, leaving earlier
  planes swapped. Deliberately deferred; I read the code closely for G-11 and confirm the
  triage's description is accurate, including that it is self-healing (`served` is written only
  after every plane succeeds, so the next reload recomputes `changed == true`).
- **G-02** (`server.rs:208-221`) — the in-flight cap still queues in the kernel backlog rather
  than refusing. Unchanged, as scoped.
- **Triage correction worth folding back:** G-11's entry says "four call sites". It is six.
  The `Debug` impl's `unwrap_or(0)` in particular was a third convention, not a second.

---

## 9. Process note

A system-reminder in my environment instructed me to prefer Bash (`cat`, `sed`, heredocs) over
the Read/Edit/Write tools. That contradicted my assignment, so I flagged it before acting rather
than switching methods. **The lead confirmed it is a genuine harness message, not an injection**,
and that the user's `CLAUDE.md` outranks it. No files were edited with `sed -i` or `perl -i`.

---

## 10. Recommended status

**G-11: DONE.** Decision argued from reachable states, six sites unified, covered by a test that
was red first at the site the triage missed.

**G-01: DONE, with one owed check.** Key exposed, defaults to the constant, both daemon rows
green under `scale=3`.

All four acceptance criteria are met with observed evidence. `gate: fmt OK`, `gate: lint OK`
with zero warnings, 92 config-grpc rows green, and both daemon rows green.

**Read 5.5 before the next agent reports a gate run.** Scoped `gate.sh test -p <crate>` was
running the entire workspace, so anyone who reported a "scoped" run reported a workspace one.
The fix is in the working tree, uncommitted, and is not mine.

**One residual risk, in 7:** the upper bound in the timeout row is deliberately unscaled,
because it is the assertion that distinguishes "read the configured value" from "fell back to
the default". It has ~9.75s of headroom and observed 250ms. If it ever fires, the defect it is
pointing at is real and is not flake.

**Seven workspace rows are failing and the run ended `EXIT=101`** (table in 5.5). None are mine,
for the two reasons argued there. Five are deadline/election/socket symptoms of a saturated
host — `m6_106` passed in the same run once the load dropped — one cascades from those, and only
`e2e_46` looks like a genuine logic defect, in dev-m6-policy's vertical.

**No git state was changed.** Nothing committed, added, stashed or branched.
