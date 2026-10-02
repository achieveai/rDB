#!/usr/bin/env bash
# Row M7F-42, charter I1 acceptance: "no live I/O in core tests" (rdb ADR-0002 decisions 3, 5
# and 7; foundation test plan FA-1 and FA-4).
#
# `rdb-core` is a pure fold. A kernel module is `step(&ctx, &event) -> Result<Vec<Effect>>` and
# nothing else: no clock, no randomness, no I/O, no async, no thread. That is not a style
# preference, it is what makes a recorded event stream a reproducer — replay the stream, get the
# trace back. One `SystemTime::now()` in a kernel module and a failing run cannot be re-run.
#
# The same goes for iteration order. A `HashMap` on any path a trace reaches makes two runs of
# one event log produce two traces, which is the single thing the whole simulator exists to
# detect. Ordered maps only.
#
# Three clauses, each with a reason it is spelled the way it is:
#
#   1. `crates/rdb-core/Cargo.toml`'s `[dependencies]` section is exactly the five ADR-rdb-0002
#      names. **That section alone.** `[dev-dependencies]` legitimately holds `config-log`,
#      `config-log-macros`, `hex` and `serde_json`, and a check that grepped the whole manifest
#      would fail on all four for no reason. ADR-rdb-0002 forbids `config-* -> rdb-*`, not the
#      reverse; the forbidden direction is the `deps` stage's job, not this one's.
#
#   2. No `Instant::now`, `SystemTime`, `rand`, `std::fs`, `std::net`, `std::thread` or `async`
#      under `crates/rdb-core/src`.
#
#   3. The ADR's own command: no `HashMap` under either crate's `src`.
#
# Clauses 2 and 3 filter whole-line comments, and that filter is load-bearing in both. Finding
# K-F-31: an ADR revision once claimed the unfiltered `HashMap` grep printed nothing, and it does
# not — three doc comments *forbid* the type by name. `contracts/time.rs` and `rdb-core/src/lib.rs`
# do the same for the clock and the runtime. A check that failed on a comment saying "never do
# this" would be un-runnable from its first day, and whoever silenced it would silence the real
# clause with it. A `use std::fs;` is not a comment and is still caught.
#
# What turns this stage red, which is the only reason it is worth running: adding `config-log`
# (or any `config-*`) to `rdb-core`'s `[dependencies]`; reaching for `SystemTime::now()` in a
# kernel module instead of taking the tick off `StepCtx`; opening a file anywhere but
# `rdb-sim`'s `harness::trace`, which is the one place I/O is allowed and is allowed by path;
# or a `HashMap` in either crate's `src`, which is how a trace stops being a trace.
#
# Usage:
#   scripts/purity-check.sh

set -euo pipefail

cd "$(dirname "$0")/.."

echo "== purity (rdb ADR-0002; row M7F-42)"

bad=0

# A whole-line comment: the line number's colon, optional indentation, then `//`. Matches `///`
# and `//!` too, which is what the doc comments that name these forbidden things are written as.
comment='^[^:]*:[0-9]+:[[:space:]]*//'

# ---------------------------------------------------------------------------------------------
# Clause 1 — the dependency set, read out of `[dependencies]` and nothing else
# ---------------------------------------------------------------------------------------------

manifest="crates/rdb-core/Cargo.toml"

if [ ! -f "$manifest" ]; then
  echo "purity: $manifest does not exist" >&2
  exit 1
fi

# From `[dependencies]` to the next section header. A `[dependencies.foo]` sub-table is a
# dependency too and is caught by the second pattern; without it a dependency could be added in
# a form this clause never looked at.
observed="$(
  awk '
    /^\[dependencies\.[A-Za-z0-9_-]+\]$/ {
      name = $0
      sub(/^\[dependencies\./, "", name)
      sub(/\]$/, "", name)
      print name
      next
    }
    /^\[/ { inside = ($0 == "[dependencies]") ; next }
    inside && /^[A-Za-z0-9_-]+[[:space:]]*=/ {
      name = $1
      sub(/[[:space:]]*=.*$/, "", name)
      print name
    }
  ' "$manifest" | sort | tr '\n' ' '
)"

expected="bytes serde sha2 thiserror tracing "

if [ "$observed" != "$expected" ]; then
  echo "purity: $manifest [dependencies] is not the ADR-rdb-0002 set." >&2
  echo "       expected: $expected" >&2
  echo "       observed: $observed" >&2
  echo "       (this clause reads [dependencies] alone; [dev-dependencies] is not its business)" >&2
  bad=1
fi

# Said separately, because this is the edge the ADR names and "the set differs" undersells it.
if printf '%s' "$observed" | grep -q 'config-'; then
  echo "purity: rdb-core has a config-* crate in [dependencies] (rdb ADR-0002 decision 3)" >&2
  bad=1
fi

# ---------------------------------------------------------------------------------------------
# Clause 2 — no clock, no randomness, no I/O, no thread, no async in the pure crate
# ---------------------------------------------------------------------------------------------

impure='Instant::now|SystemTime|\brand\b|std::fs|std::net|std::thread|\basync\b'

if hits="$(grep -rnE "$impure" crates/rdb-core/src | grep -vE "$comment")" && [ -n "$hits" ]; then
  echo "purity: crates/rdb-core/src is a pure fold and must reach no clock, no randomness," >&2
  echo "        no I/O, no thread and no async (rdb ADR-0002 decision 5; FA-1):" >&2
  echo "$hits" | sed 's/^/          /' >&2
  bad=1
fi

# ---------------------------------------------------------------------------------------------
# Clause 3 — the ADR's own command, with the comment filter K-F-31 says it needs
# ---------------------------------------------------------------------------------------------

if hits="$(grep -rn "HashMap" crates/rdb-core/src crates/rdb-sim/src | grep -vE "$comment")" \
  && [ -n "$hits" ]; then
  echo "purity: no HashMap on a path a trace reaches; iteration order is part of the trace" >&2
  echo "        (rdb ADR-0002 decision 7; FA-4):" >&2
  echo "$hits" | sed 's/^/          /' >&2
  bad=1
fi

# ---------------------------------------------------------------------------------------------
# Clause 2b — the simulator's one sanctioned I/O, allowed by path and by nothing else
# ---------------------------------------------------------------------------------------------

if stray="$(grep -rln "std::fs" crates/rdb-sim/src | grep -v '^crates/rdb-sim/src/harness/trace\.rs$')" \
  && [ -n "$stray" ]; then
  echo "purity: the only I/O in rdb-sim is harness::trace's trace writer. Also found in:" >&2
  echo "$stray" | sed 's/^/          /' >&2
  bad=1
fi

if [ "$bad" -ne 0 ]; then
  echo "purity: rdb-core is not a pure fold, or a trace path is unordered (rdb ADR-0002)" >&2
  exit 1
fi

echo "gate: purity OK"
