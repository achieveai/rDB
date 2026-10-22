---
title: "A hashed examples/<name>-<hash>.exe is the example's test harness, not the program"
date: 2026-10-22
type: bug
component: rdb_dev, scripts/gate.sh, manual walks
tags: [cargo, example, binary, manual-test, stale-build, rdb_dev]
applies_when:
  - a brief or walk names a binary "built by the gate" under target/debug/examples
  - a manual walk claims a commit and runs a CLI it did not build itself
skip_when: [the walker built the CLI with `cargo build --example <name>` from an export of the claimed commit]
source: "rdb M9 close-out final walk at f1120c8 (teams/m9/m9close-tester.md, Retrospective)"
status: applied
revalidate_when: rdb_dev moves from examples/ to a [[bin]] target, or the gate starts building examples with `cargo build --examples`
---

## Problem
The lead's final-walk brief named `target/debug/examples/rdb_dev-<hash>.exe` as the CLI the full gate had just built at the PR tip. It is not the CLI. `cargo test` builds every example as a libtest binary, and that hashed file is what the gate log names (`Running unittests examples\rdb_dev.rs (...rdb_dev-<hash>.exe)`). Run it and it prints test-runner flags. The real CLI is the unhashed `examples/rdb_dev.exe` next to it, but that one was an hour older than the gate: nothing in the gate rewrites it.

## What didn't work
- Taking "newest file in examples/ whose name starts with rdb_dev" as the gate's build: the newest one is the harness.
- Falling back to the unhashed file beside it: it runs, but it is a pre-tip build. A walk on it would report a PASS under the tip's commit for code it never ran.

## Solution
Before a walk, run the binary with `--help` and check that it prints the program's usage. Build the CLI yourself from an export of the claimed commit, with your own target dir: `cargo build -p rdb-api --example rdb_dev`. Record the export's commit and the binary's md5 next to the transcripts.

## Why it works
Cargo writes `examples/<name>.exe` only for `cargo build --example` (or `run --example`). `cargo test` writes `examples/<name>-<hash>.exe`, the example compiled as a test crate. So a gate refreshes the harness and leaves the program stale. `--help` tells the two apart in a second, and building from the export ties the binary to a commit you can name.
