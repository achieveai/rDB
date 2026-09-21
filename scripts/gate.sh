#!/usr/bin/env bash
# The workspace gate: format, lint, test — under the environment the rows were accepted in.
#
# The point of this script is the environment, not the three cargo invocations. Every M4-M6
# acceptance run was made with RETCD_TEST_DEADLINE_SCALE=3, a private target directory and a
# fresh log root, but nothing in the repository set them, so `cargo test --workspace` on a
# loaded host ran the cluster rows at a third of the patience they were accepted with and
# failed on capacity rows that are not broken (m4_69). A knob every contributor has to know
# about from somewhere else is not a knob.
#
# RETCD_TEST_DEADLINE_SCALE stretches deadlines only. Raft timers keep their real values, so
# the rows still test the real thing; a deadline here is a bound on how long a poll may wait
# for an observed state, never a sleep. See crates/config-testkit/src/poll.rs.
#
# Kept in sync with scripts/gate.ps1.
#
# The deps stage is the one non-cargo check: `rdb-*` may depend on `config-*`, never the
# reverse (rdb ADR-0002). It reads `cargo metadata` and fails on any `config-*` package that
# names an `rdb-*` dependency of any kind, dev and build included, because a dev-dependency is
# how the reverse edge would arrive first.
#
# Usage:
#   scripts/gate.sh                            fmt + deps + drift + clippy + test
#   scripts/gate.sh fmt|deps|drift|lint|test   one stage
#   scripts/gate.sh test -p config-engine --test m4_watch    extra args go to cargo

set -euo pipefail

cd "$(dirname "$0")/.."

# A private target directory: two cargo invocations sharing one lock each other out, and a
# test run that blocks on a build lock burns its own deadlines waiting.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-.rtargets/gate}"
# Incremental artifacts are worthless for a full clean gate and cost disk on every run.
export CARGO_INCREMENTAL=0
export RETCD_TEST_DEADLINE_SCALE="${RETCD_TEST_DEADLINE_SCALE:-3}"
# One log root per invocation, inside the private target dir, so one gate run's logs never mix
# with another's. Separating the binaries *within* a run is already config-log's job: it writes
# each into a test_run_id subdirectory under this root.
export RETCD_TEST_LOG_DIR="${RETCD_TEST_LOG_DIR:-$PWD/$CARGO_TARGET_DIR/test-logs/$(date +%Y%m%d-%H%M%S)-$$}"

stage="${1:-all}"
[ $# -gt 0 ] && shift || true

echo "gate: target=$CARGO_TARGET_DIR scale=$RETCD_TEST_DEADLINE_SCALE logs=$RETCD_TEST_LOG_DIR"

run_fmt()  { echo "== fmt";    cargo fmt --all --check; }
# The second non-cargo check. Every M7 test plan says which contract commit it was written
# against; this fails when that commit is no longer the newest one to touch the contracts.
# All four teams held a stale basis at once on 2026-09-20, and a stale basis always over-holds:
# rows report Unavailable on types that have already landed. See scripts/drift-check.sh.
run_drift() { scripts/drift-check.sh; }
run_lint() { echo "== clippy"; cargo clippy --workspace --all-targets -- -D warnings; }
run_test() { echo "== test";   cargo test --workspace --no-fail-fast "$@"; }
# perl with JSON::PP ships with every Git for Windows and every Linux perl; no jq needed.
run_deps() {
  echo "== deps"
  cargo metadata --format-version 1 --no-deps | perl -MJSON::PP -e '
    local $/;
    my $meta = decode_json(<STDIN>);
    my $bad = 0;
    for my $pkg (@{ $meta->{packages} }) {
      next unless $pkg->{name} =~ /^config-/;
      for my $dep (@{ $pkg->{dependencies} }) {
        next unless $dep->{name} =~ /^rdb-/;
        my $kind = $dep->{kind} // "normal";
        print STDERR "deps: $pkg->{name} depends on $dep->{name} ($kind)\n";
        $bad = 1;
      }
    }
    print STDERR "deps: config-* must never depend on rdb-* (rdb ADR-0002)\n" if $bad;
    exit $bad;
  '
}

case "$stage" in
  fmt)   run_fmt ;;
  deps)  run_deps ;;
  drift) run_drift ;;
  lint)  run_lint ;;
  test)  run_test "$@" ;;
  all)   run_fmt && run_deps && run_drift && run_lint && run_test ;;
  *)     echo "unknown stage: $stage (expected fmt, deps, drift, lint, test or all)" >&2; exit 1 ;;
esac

echo "gate: $stage OK"
