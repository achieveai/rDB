#!/usr/bin/env sh
# M7 row census — counts the tree, never a plan's prose.
#
# Why this exists. On 2026-09-22 the foundation plan's section 17 was found wrong in three of the
# six files it broke down, in *both* directions: seams.rs held 7 where the prose said 10,
# storage.rs held 7 where it said 6, dispatch.rs held 9 where it said 6. The same pass found the
# progress dashboard reporting "4 of 193" for kernel-a against 6 functions on disk, and no count
# at all for verification's 79. Every one of those numbers was a hand-maintained copy of another
# hand-maintained copy. A copy chain has no bias, only drift, which is why the errors ran both
# ways (ledger L-R97, L-R98).
#
# An over-count hides owed work behind a number that says it is done. An under-count funds a row
# that already exists — which happened, and was caught only because a developer read the file
# before writing beside it, not by any check.
#
# What this reports and what it does NOT report.
#   Reports: for each scope, the row ids DECLARED by its plan, the ids that have at least one
#   test function on disk (LANDED), and the ids that have none (OWED).
#   Does not report: whether a landed row is any good. A function whose name starts with m7f_29_
#   makes M7F-29 "landed" here even if its body asserts nothing. Vacuity is a reviewer's job;
#   this script only ever answers "does a function with this id exist".
#
# One row may land as several functions (M7F-43 has two), so the function count and the row count
# are different numbers and are printed separately. Do not convert one into the other.
#
# Usage:  scripts/m7-census.sh [scope]
#   scope: foundation | kernel-a | kernel-b | verification  (default: all)
# Exit 0 always unless --strict is passed, in which case a plan declaring an id whose function is
# absent is still exit 0 (owed work is not a failure) but an id on disk that NO plan declares is
# exit 1 — that is a row nobody planned, and it is how a typo'd prefix hides.

set -eu

cd "$(dirname "$0")/.."

TEST_DIRS="crates/rdb-core/tests crates/rdb-sim/tests"
STRICT=0
WANT="${1:-all}"
[ "${1:-}" = "--strict" ] && { STRICT=1; WANT=all; }
[ "${2:-}" = "--strict" ] && STRICT=1

scope_prefix() {
  case "$1" in
    foundation)   echo "m7f" ;;
    kernel-a)     echo "m7a" ;;
    kernel-b)     echo "m7b" ;;
    verification) echo "m7v" ;;
    *) echo "unknown scope: $1" >&2; exit 2 ;;
  esac
}

scope_plan() {
  echo "docs/testing/test-plan-m7-$1.md"
}

# Rows that legitimately have no `fn <prefix>_NN_...` on disk, with the reason and where it is
# recorded. Without this list the census reports five false "owed" for foundation, and a census
# that cries wolf is one nobody reads — which is how the last one drifted.
#
#   M7F-27, M7F-28  `script` class, not cargo tests: they are the two deps gate stages.
#                   Foundation plan section 17 counts them separately for exactly this reason.
#   M7F-42          `script` class, same as the two above. The foundation plan's section 5 row
#                   declares it `script`, and it landed 2026-09-22 as scripts/purity-check.sh,
#                   wired as the `purity` stage in scripts/gate.sh (line 56) and gate.ps1. There
#                   is no m7f_42_ function to find and there is not meant to be one.
#   M7F-39, M7F-48, M7F-49  landed under names kept verbatim rather than renamed to the id
#                   prefix. Foundation plan section 16 records all three as known exceptions
#                   ("met with three exceptions"), and section 18 Q-2 gives the reason a landed
#                   test is not renamed.
#
# Add to this list only with a citation. An id parked here stops being counted as owed, so an
# entry without a reason is a way to make owed work disappear.
EXEMPT="M7F-27 M7F-28 M7F-39 M7F-42 M7F-48 M7F-49"

is_exempt() {
  for e in $EXEMPT; do [ "$e" = "$1" ] && return 0; done
  return 1
}

# Ids that DO have a function carrying their prefix, where that function asserts a different
# row's claim. The name is the only thing the census can read, so without this list a
# miscredited id is indistinguishable from a landed one — and it is worse than a plain miss,
# because a miss is owed work and this reads as finished work.
#
# An id here is removed from `landed` and counted as `owed`, and printed on its own line. That
# is the conservative direction: it never invents coverage, it only stops claiming it.
#
# Measured 2026-09-22 by the kernel-a manual tester, each verified by the lead against
# docs/testing/test-plan-m7-kernel-a.md and crates/rdb-sim/tests/authority.rs:
#
#   M7A-28  Plan row 366 requires termination `RevisionCompacted`. Both on-disk functions
#           (authority.rs:412, :524) send `WatchTermination::ResourceExhaustedResumable`, which
#           is plan row 367's input — M7A-29's. M7A-28's own claim is asserted nowhere.
#   M7A-33  Plan row 371 is resync equivalence over `served`, `revoked_epochs` and
#           `partitions_revision`. `git grep 'revoked_epochs\|partitions_revision' -- crates/`
#           is EMPTY: two of the three fields exist nowhere in the workspace, so the row is not
#           merely untested, it is unwritable today. Both on-disk functions (:355, :467) send
#           `ResourceExhaustedFatal` and assert back-off and the cap, which is plan row 369 —
#           M7A-31's subject.
#
# M7A-31 is deliberately NOT listed as landed in its place. Its plan row also requires effects
# `[Fact(AdmissionRefused), Timer(backoff_n)]`, so the two functions assert part of M7A-31 and not
# its effect shape. Crediting it here would be the same error in the other direction.
#
# 2026-09-22: half this rationale has expired, and which half depends on the tree you read.
# `AdmissionRefused` IS an `AuthorityIgnoreReason` variant in the working tree
# (contracts/authority.rs:261, uncommitted with CB-7) and is NOT one at HEAD. The `Timer(backoff_n)`
# clause was never checked and still holds. Re-derive this entry against the plan row when CB-7
# commits. Do not drop it because one clause dissolved under somebody else's uncommitted work.
#
# Remove an id from this list only by making its claim true on disk, never to settle a count.
#
# 2026-09-22, A1 phase 2 — M7A-33 REMOVED, and this is the one legitimate way to do it.
#
# Both miscredited functions are renamed onto `m7a_31_*` in `crates/rdb-sim/tests/authority.rs`
# and now assert M7A-31's actual claim. That was not possible until today: `AuthorityTimer::
# WatchBackoff` returned `unavailable("...today the re-arm is immediate")`, so "bounded backoff,
# non-decreasing, backoff_20 == cap" had no subject and what the functions asserted was the stub.
# The timer is built, the under-cap arm emits
# `[Ignored(Authority(AdmissionRefused)), Timer(Arm WatchBackoff)]`, and the at-cap arm latches
# with `Fact(WatchAdmissionExhausted)` and no re-arm. So the claim became true on disk; the id was
# not removed to settle a count. M7A-31 is now genuinely landed and M7A-33 has zero functions,
# which reports it as `owed` — correct, because it was never written.
#
# Both clauses of the old rationale are now closed rather than dissolved:
#   - the `AdmissionRefused` wrapper: the row is written against the CONTRACT's spelling,
#     `AuthorityIgnoreReason::AdmissionRefused`, not the plan's `Fact(AdmissionRefused)` and not
#     `design.md:1087`'s `WatchAdmissionRefused`, which exists in no contract at all;
#   - the `Timer(backoff_n)` clause, never checked before, is asserted directly.
#
# M7A-33 itself is now WRITABLE and owed, not blocked: `revoked_epochs` and `partitions_revision`
# are both on `AuthorityStateView` in the working tree. The old "git grep ... is EMPTY" note above
# is history. Nobody has written the row; that is a miss, not a blocker.
MISCREDITED="M7A-28"

is_miscredited() {
  for m in $MISCREDITED; do [ "$m" = "$1" ] && return 0; done
  return 1
}

# Row ids that have at least one test function on disk, e.g. m7f_43_foo -> M7F-43
landed_ids() {
  grep -rho "^fn ${1}_[0-9][0-9]*" $TEST_DIRS 2>/dev/null \
    | sed "s/^fn ${1}_/$(echo "$1" | tr a-z A-Z)-/" \
    | sort -u
}

# Row ids the plan declares. Matches the id token wherever it appears; a plan that mentions an id
# only in prose still counts it as declared, which is deliberate — an id worth naming is an id
# somebody owes.
declared_ids() {
  [ -f "$2" ] || { echo "no plan file: $2" >&2; return 0; }
  grep -o "$(echo "$1" | tr a-z A-Z)-[0-9][0-9]*" "$2" 2>/dev/null | sort -u
}

report_scope() {
  scope="$1"
  pfx="$(scope_prefix "$scope")"
  plan="$(scope_plan "$scope")"
  upfx="$(echo "$pfx" | tr a-z A-Z)"

  fn_count=$(grep -rhc "^fn ${pfx}_" $TEST_DIRS 2>/dev/null | awk '{s+=$1} END {print s+0}')

  landed_ids "$pfx" > /tmp/.census_landed_raw.$$ || true
  declared_ids "$pfx" "$plan" > /tmp/.census_declared.$$ || true

  # A miscredited id has a function carrying its prefix but not its claim. Drop it from landed
  # so it falls through into owed below, and keep it to print on its own line.
  : > /tmp/.census_landed.$$
  : > /tmp/.census_miscredited.$$
  while IFS= read -r id; do
    [ -z "$id" ] && continue
    if is_miscredited "$id"; then echo "$id" >> /tmp/.census_miscredited.$$
    else echo "$id" >> /tmp/.census_landed.$$; fi
  done < /tmp/.census_landed_raw.$$
  n_miscredited=$(wc -l < /tmp/.census_miscredited.$$ | tr -d ' ')

  n_landed=$(wc -l < /tmp/.census_landed.$$ | tr -d ' ')
  n_declared=$(wc -l < /tmp/.census_declared.$$ | tr -d ' ')
  comm -13 /tmp/.census_landed.$$ /tmp/.census_declared.$$ > /tmp/.census_owed_raw.$$ || true
  comm -23 /tmp/.census_landed.$$ /tmp/.census_declared.$$ > /tmp/.census_unplanned.$$ || true

  : > /tmp/.census_owed.$$
  : > /tmp/.census_exempt.$$
  while IFS= read -r id; do
    [ -z "$id" ] && continue
    if is_exempt "$id"; then echo "$id" >> /tmp/.census_exempt.$$
    else echo "$id" >> /tmp/.census_owed.$$; fi
  done < /tmp/.census_owed_raw.$$

  n_owed=$(wc -l < /tmp/.census_owed.$$ | tr -d ' ')
  n_exempt=$(wc -l < /tmp/.census_exempt.$$ | tr -d ' ')
  n_unplanned=$(wc -l < /tmp/.census_unplanned.$$ | tr -d ' ')

  printf '\n== %s (%s_) ==\n' "$scope" "$pfx"
  printf '  test functions on disk : %s\n' "$fn_count"
  printf '  row ids declared       : %s   (%s)\n' "$n_declared" "$plan"
  printf '  row ids landed         : %s\n' "$n_landed"
  printf '  row ids owed           : %s\n' "$n_owed"
  if [ "$n_exempt" -gt 0 ]; then
    printf '  row ids exempt         : %s   (no id-prefixed function by design; see EXEMPT) '"" "$n_exempt"
    tr '\n' ' ' < /tmp/.census_exempt.$$
    printf '\n'
  fi
  if [ "$n_miscredited" -gt 0 ]; then
    printf '  row ids MISCREDITED    : %s   (a function carries the id, not the claim; counted as owed) ' "$n_miscredited"
    tr '\n' ' ' < /tmp/.census_miscredited.$$
    printf '\n'
  fi

  if [ "$n_owed" -gt 0 ]; then
    printf '  owed: '
    tr '\n' ' ' < /tmp/.census_owed.$$
    printf '\n'
  fi

  if [ "$n_unplanned" -gt 0 ]; then
    printf '  ** %s id(s) on disk that %s does not declare:\n     ' "$n_unplanned" "$plan"
    tr '\n' ' ' < /tmp/.census_unplanned.$$
    printf '\n'
    printf '     A function whose id no plan names is either a typo in the prefix or a row\n'
    printf '     somebody wrote without planning it. Both are worth a look.\n'
    UNPLANNED_SEEN=1
  fi

  printf '  per file:\n'
  for f in $(ls $TEST_DIRS 2>/dev/null >/dev/null; find $TEST_DIRS -name '*.rs' 2>/dev/null | sort); do
    n=$(grep -c "^fn ${pfx}_" "$f" 2>/dev/null || true)
    [ -z "$n" ] && n=0
    [ "$n" -gt 0 ] && printf '    %4s  %s\n' "$n" "$f"
  done

  rm -f /tmp/.census_landed.$$ /tmp/.census_declared.$$ /tmp/.census_owed.$$ \
        /tmp/.census_owed_raw.$$ /tmp/.census_exempt.$$ /tmp/.census_unplanned.$$
}

UNPLANNED_SEEN=0

printf 'M7 census — counted from the tree at %s\n' "$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown commit')"
printf 'Functions and rows are different numbers. Do not convert one into the other.\n'

if [ "$WANT" = "all" ]; then
  for s in foundation kernel-a kernel-b verification; do report_scope "$s"; done
  total=$(grep -rhc "^fn m7[fabv]_" $TEST_DIRS 2>/dev/null | awk '{s+=$1} END {print s+0}')
  printf '\n== total ==\n  M7 test functions on disk: %s\n' "$total"
else
  report_scope "$WANT"
fi

printf '\nThis says a function with that id exists. It does not say the function asserts anything.\n'

if [ "$STRICT" = "1" ] && [ "$UNPLANNED_SEEN" = "1" ]; then
  printf '\nFAIL (--strict): a row id on disk is declared by no plan.\n'
  exit 1
fi
exit 0
