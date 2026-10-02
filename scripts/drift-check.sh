#!/usr/bin/env bash
# Every M7 test plan carries a section 15 drift table: "here is the contract surface this plan
# was written against, and here is where it differs". The table names a basis commit. Nothing
# re-checked that commit, and by 2026-09-20 all four teams had a stale one at the same time.
#
# The failure is quiet and it always over-holds. Kernel-b's plan named a commit that does not
# contain contracts/authority.rs at all; the file landed two commits later. On that stale basis
# the plan held a row Unavailable on a type that had already shipped, and recorded the reject
# ladder as five variants when sixteen had landed. Verification held two contract asks open that
# were satisfied in the same round that asked for them. Kernel-a held thirteen rows on
# "not in rdb-core today" for a module that was in rdb-core.
#
# A drift table is only as fresh as its last re-read, and "remember to re-read it" is a
# convention. This makes it a red build instead.
#
# The rule: a plan's declared basis must be the newest commit that touched the contract surface.
# If the surface has moved since, the plan was written against something that no longer exists
# and its author has to look again.
#
# Declare the basis in the plan with a marker on its own line, anywhere in the file:
#
#     <!-- drift-basis: 6893442 -->
#
# It is an HTML comment, so it renders as nothing. Short or full hashes both work.
#
# Usage:
#   scripts/drift-check.sh            check every plan
#   scripts/drift-check.sh <file>...  check named plans

set -euo pipefail

cd "$(dirname "$0")/.."

# The surface a plan is written against. Contract types are what the rows name; the kernel
# modules under src/ are what the rows test, and those move constantly by design.
surface="crates/rdb-core/src/contracts"

if [ ! -d "$surface" ]; then
  echo "drift: $surface does not exist; nothing to check against" >&2
  exit 1
fi

if [ $# -gt 0 ]; then
  plans=("$@")
else
  # A plan with no rows to hold has no drift table to be stale.
  plans=(docs/testing/test-plan-m7-*.md)
fi

current="$(git log -1 --format=%H -- "$surface")"
if [ -z "$current" ]; then
  echo "drift: no commit has touched $surface" >&2
  exit 1
fi

echo "== drift (contract surface at ${current:0:7})"

bad=0

for plan in "${plans[@]}"; do
  name="$(basename "$plan")"

  if [ ! -f "$plan" ]; then
    echo "drift: $name: no such file" >&2
    bad=1
    continue
  fi

  # A marker is a whole line and nothing else. Matching the bare substring instead counted
  # foundation's section 15.1 twice on 2026-09-20: the plan quotes the grep command it used to
  # verify its own marker, and the transcript of that command contains the string. Showing your
  # work is the behaviour this check wants, so the check reads only the declared format.
  marker_re='^[[:space:]]*<!--[[:space:]]*drift-basis:[[:space:]]*[0-9a-fA-F]\{7,40\}[[:space:]]*-->[[:space:]]*$'

  # One marker per plan. Two would mean two answers to one question.
  count="$(grep -c "$marker_re" "$plan" || true)"

  if [ "$count" -eq 0 ]; then
    echo "drift: $name: no basis marker." >&2
    echo "       Add one line naming the commit the plan's section 15 was written against:" >&2
    echo "       <!-- drift-basis: ${current:0:7} -->" >&2
    if grep -q 'drift-basis:' "$plan"; then
      echo "       ('drift-basis:' does appear in this file, but not as a line of its own." >&2
      echo "        A marker is the whole line; anything else is prose about a marker.)" >&2
    fi
    bad=1
    continue
  fi

  if [ "$count" -gt 1 ]; then
    echo "drift: $name: $count basis markers; a plan has one basis" >&2
    bad=1
    continue
  fi

  basis="$(grep "$marker_re" "$plan" | grep -o '[0-9a-fA-F]\{7,40\}' | head -1)"

  if ! git cat-file -e "$basis^{commit}" 2>/dev/null; then
    echo "drift: $name: basis $basis is not a commit in this repository" >&2
    bad=1
    continue
  fi

  basis_full="$(git rev-parse "$basis^{commit}")"

  # The failure that started this: a basis that predates the surface entirely. Worth its own
  # message, because "your basis is old" undersells it.
  if [ -z "$(git ls-tree -r --name-only "$basis_full" -- "$surface")" ]; then
    echo "drift: $name: basis ${basis:0:7} does not contain $surface at all." >&2
    echo "       The plan was written against a commit where the contracts did not exist." >&2
    echo "       Current: ${current:0:7}" >&2
    bad=1
    continue
  fi

  if [ "$basis_full" = "$current" ]; then
    echo "drift: $name OK (${basis:0:7})"
    continue
  fi

  # Is the surface actually different, or did the basis just name an older commit that happens
  # to carry identical files? Only a real difference is worth failing on.
  changed="$(git diff --name-only "$basis_full" "$current" -- "$surface")"

  if [ -z "$changed" ]; then
    echo "drift: $name OK (${basis:0:7}; older than ${current:0:7}, surface identical)"
    continue
  fi

  echo "drift: $name: basis ${basis:0:7} is stale. The contract surface moved at ${current:0:7}." >&2
  echo "       Re-read section 15 against these files, then update the marker:" >&2
  echo "$changed" | sed 's/^/         /' >&2
  bad=1
done

if [ "$bad" -ne 0 ]; then
  echo "drift: at least one plan is written against a contract surface that has moved" >&2
  exit 1
fi

echo "gate: drift OK"
