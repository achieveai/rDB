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
#   1. `rdb-core`'s normal dependencies are exactly the five ADR-rdb-0002 names. **Normal ones
#      alone.** Its `[dev-dependencies]` legitimately hold `config-log`, `config-log-macros`, `hex`
#      and `serde_json`, and a check that read the whole manifest would fail on all four for no
#      reason. ADR-rdb-0002 forbids `config-* -> rdb-*`, not the reverse; the forbidden direction
#      is the `deps` stage's job, not this one's. The set is read from `cargo metadata --no-deps`,
#      not from the manifest's text, so every way of writing a dependency counts: a dotted key
#      (`foo.workspace = true`), an indented line, a `[dependencies.foo]` sub-table and a
#      `[target.'cfg(..)'.dependencies]` table. A build dependency counts as normal here.
#
#   2. No `Instant::now`, `SystemTime`, `rand`, `std::fs`, `std::net`, `std::thread` or `async`
#      under `crates/rdb-core/src`.
#
#   3. The ADR's own command: no `HashMap` under either crate's `src`.
#
# `rdb-value` (ADR-rdb-0011, ADR-rdb-0012 Verification) is pure too, and gets the same three
# clauses: its normal dependencies are exactly `bytes`, `cbor4ii`, `rdb-core`, `sha2` and
# `thiserror`; its `src` reaches no clock, randomness, I/O, thread or async; and no `HashMap`,
# because map key order is part of a document's bytes.
#
#   1b. Unlike `rdb-core`, `rdb-value`'s dev-dependencies are this stage's business too. ADR-rdb-0011
#       keeps its tests free of storage and the server: no `config-*`, `rdb-sim`, `rdb-storage`,
#       `rocksdb` or `librocksdb-sys`, not even as a dev-dependency, so its tests build without the
#       native RocksDB link. The others (`serde_json`, `serde`, `hex`, `proptest`, `ciborium`) are
#       allowed.
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
# or a `HashMap` in either crate's `src`, which is how a trace stops being a trace. Also, for
# `rdb-value`: any normal dependency outside its five, in any manifest form, and a banned
# dev-dependency (clause 1b).
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
# Clause 1 — the dependency sets, as cargo resolves the manifests
# ---------------------------------------------------------------------------------------------

# Read once. `--no-deps` needs no registry and no network (the `deps` stage reads it the same way).
metadata="$(cargo metadata --format-version 1 --no-deps)"

# `deps_of <package> <normal|dev>`: the package names of that kind, sorted, space-separated with a
# trailing space. Build dependencies count as normal. Target-specific ones are included.
deps_of() {
  printf '%s' "$metadata" | perl -MJSON::PP -e '
    my ($package, $want) = @ARGV;
    local $/;
    my $meta = decode_json(<STDIN>);
    my ($p) = grep { $_->{name} eq $package } @{ $meta->{packages} };
    die "purity: cargo metadata has no package $package\n" unless $p;
    my %names;
    for my $dep (@{ $p->{dependencies} }) {
      my $kind = ($dep->{kind} // "normal") eq "dev" ? "dev" : "normal";
      $names{ $dep->{name} } = 1 if $kind eq $want;
    }
    print join("", map { "$_ " } sort keys %names);
  ' "$1" "$2"
}

# One line per pure crate: package, the ADR that names its set, and the exact sorted set.
while read -r package adr expected; do
  observed="$(deps_of "$package" normal)"
  if [ "$observed" != "$expected " ]; then
    echo "purity: $package's normal dependencies are not the $adr set." >&2
    echo "       expected: $expected " >&2
    echo "       observed: $observed" >&2
    echo "       (dev-dependencies are not in this set; for rdb-value see clause 1b)" >&2
    bad=1
  fi
  # Said separately, because this is the edge the ADR names and "the set differs" undersells it.
  if printf '%s' "$observed" | grep -q 'config-'; then
    echo "purity: $package has a config-* crate as a normal dependency (rdb ADR-0002 decision 3)" >&2
    bad=1
  fi
done <<'CRATES'
rdb-core ADR-rdb-0002 bytes serde sha2 thiserror tracing
rdb-value ADR-rdb-0012 bytes cbor4ii rdb-core sha2 thiserror
CRATES

# Clause 1b: rdb-value's tests stay free of storage and the server (ADR-rdb-0011).
banned_dev="$(deps_of rdb-value dev | tr ' ' '\n' \
  | grep -E '^(config-.*|rdb-sim|rdb-storage|rocksdb|librocksdb-sys)$' || true)"
if [ -n "$banned_dev" ]; then
  echo "purity: rdb-value has a dev-dependency ADR-rdb-0011 bans (no config-*, rdb-sim," >&2
  echo "        rdb-storage, rocksdb or librocksdb-sys, not even as dev-dependencies):" >&2
  echo "$banned_dev" | sed 's/^/          /' >&2
  bad=1
fi

# ---------------------------------------------------------------------------------------------
# Clause 2 — no clock, no randomness, no I/O, no thread, no async in the pure crates
# ---------------------------------------------------------------------------------------------

impure='Instant::now|SystemTime|\brand\b|std::fs|std::net|std::thread|\basync\b'

for src in crates/rdb-core/src crates/rdb-value/src; do
  if hits="$(grep -rnE "$impure" "$src" | grep -vE "$comment")" && [ -n "$hits" ]; then
    echo "purity: $src is pure and must reach no clock, no randomness, no I/O, no thread" >&2
    echo "        and no async (rdb ADR-0002 decision 5; FA-1; ADR-rdb-0011):" >&2
    echo "$hits" | sed 's/^/          /' >&2
    bad=1
  fi
done

# ---------------------------------------------------------------------------------------------
# Clause 3 — the ADR's own command, with the comment filter K-F-31 says it needs
# ---------------------------------------------------------------------------------------------

if hits="$(grep -rn "HashMap" crates/rdb-core/src crates/rdb-sim/src crates/rdb-value/src \
  | grep -vE "$comment")" && [ -n "$hits" ]; then
  echo "purity: no HashMap on a path a trace reaches, or in a document; iteration order is part" >&2
  echo "        of the trace and of a document's bytes (rdb ADR-0002 decision 7; FA-4; ADR-rdb-0012):" >&2
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
  echo "purity: rdb-core or rdb-value is not pure, or a trace path is unordered (rdb ADR-0002)" >&2
  exit 1
fi

echo "gate: purity OK"
