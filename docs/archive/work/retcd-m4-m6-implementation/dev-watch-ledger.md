# dev-watch execution ledger (M4 watch delivery + transport)

Contract: m4-interfaces.md incl. "Lead rulings" (binding).

## Decisions
- D1: No shim module. `ConfigError::{RevisionCompacted, ResourceExhausted{resumable}}` are enum
  changes in config-core that a shim cannot emulate, so a shim would not buy a compile anyway.
  Code is written against the contract and compiled once dev-journal lands. Satisfies
  "no shim survives" trivially.
- D2: `TrackedWatch` (last_delivered_revision / stream_id, TA-38) lives in
  `config_engine::watch` and is re-exported by `config-grpc` so `config-client` can name it
  without taking a config-engine dependency. `ConfigStore::watch` keeps the contract's plain
  `config_core::WatchStream` alias; the inherent `GrpcClient::watch` / `DirectClient::watch_tracked`
  return `TrackedWatch`.
- D3: The journal gate is a hand-rolled `std::sync::Mutex<bool> + Condvar` "manual lock" because
  compaction acquires it in `before_compact` and releases it in `after_compact` (two calls, no
  guard can be held across them). Registration enters it inside `spawn_blocking`, so the sync
  park never blocks the async runtime (ruling R3: sync critical section only).
- D4: Gate hooks support both a synchronous park (AfterRegister, inside the gate; and the
  compaction side, TA-30.4) and an async park (BeforeReplay, BeforeLiveDrain).
- D5: trailers per ruling R9 — `retcd-min-revision`, `retcd-resumable`. No `retcd-reason`
  (test-plan rows M4-61/M4-100/E2E-25 that name `retcd-reason` are superseded).
- D6: progress frames use tokio time (tests use `tokio::time::pause`), not TA-33's `LeaderClock`.
  `LeaderClock` is injected into the retention task only ("clocks never enter apply").

## Status
- see handoff.

## Round 2 (C4-05, C4-08..C4-11 + two coordinator additions) — done

| Finding | Change | Row / evidence |
| --- | --- | --- |
| C4-08 | `register_locked` subscribes before reading `H`; new `GateHook::BeforeHighWater`; live-drain filter comment now load-bearing | `c4_08_a_batch_between_subscribe_and_high_water_is_delivered_once`; mutation (read-before-subscribe) fails with "only 2 of 3 events arrived" |
| C4-09 | `ephemeral.rs` drops `CompactGuard` after `on_applied`; the three bracket comments now state the ordering as a contract stores owe the sink | config-storage suite green |
| C4-10 | c4_07 row restarts all three nodes, asserts the **hub** floor on each, refusal now unconditional; m4_26 gained a hub assertion | mutation (drop attach seeding) fails at "node 1's hub must know its floor" |
| C4-11 | `WatchHub::applied_revision()` and its field removed; ADR spawn_blocking bullet reconciled | `cargo clippy -p config-engine --all-targets -D warnings` clean |
| C4-05 | `WatchHub::open` is async, `register_locked` runs on `spawn_blocking`, admission taken before the hop; `node.rs` call site awaits | 22/22 m4_watch, 20/20 m4_watch_cluster |

Two defects found in passing and fixed: a leftover mutation `mpsc::channel(queue_events + 100_000)`
in `WatchHub::open` (the per-stream event cap was disabled), and `HookSlot::wait_arrived`
sampling its baseline at call time (lost wakeup when the arrival won the race) — now sampled in
`arm()`.

Anti-flake: both progress-interval sleeps carry `// testkit:allow-sleep` with a justification;
pausing the runtime clock would stop the ticker under test and the cluster's raft timers.
`scan` still fails on literal ports in `config-grpc/tests/admin_plane.rs` (not mine).
