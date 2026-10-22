---
title: "A repair and a read must share one rule for what counts as damaged"
date: 2026-10-22
type: bug
component: rdb-value repair / collection reads
tags: [damage, repair, admin-clear, corrupt-history, count-check, read-path]
applies_when:
  - a repair, clear or fsck proves damage before it deletes or rewrites
  - the repair can stop part way and resume in a later call
  - a read path and a repair path each decide "is this object healthy"
skip_when: [the repair finishes in one atomic step, so no half-repaired state is ever readable]
source: "rEtcd M9 S6, tester-s6 Walk 2 finding W2-A, fixed in c3fd2b3 (ruling A-R142)"
status: applied
revalidate_when: a new record kind or a new damage check lands in the clear's proof but not in reads
---

## Problem
The admin clear proves a map is damaged before deleting it. One proof was "the root counts
300 elements but fewer are stored". Reads never checked that. After the first clear call
deleted 254 clean entries, the app deleted the one bad entry. A full `members` read then
listed 46 entries under a root saying 300, with no error. The half-cleared map looked
healthy, and the next clear call deleted the app's new writes.

## What didn't work
- Testing the clear and the read separately: each was right by its own rule.
- Hand walks of a single clear call: the gap only opens between two calls, with an app
  write in between.

## Solution
One `check_count` helper, used by both the clear's proof and every full read (`members`
from start to end, scanning limit+1). A mismatch answers CORRUPT_HISTORY on both paths.
A page that starts with `--after` cannot see the whole object and says so in the docs.

## Why it works
"Damaged" is now one definition. Anything the clear may delete, a read refuses to serve as
healthy, so no state between clear calls reads as a good object. The same split is the
open S7 item: a newer build's map entry reads CORRUPT_HISTORY but the clear answers
INCOMPATIBLE_VERSION. Check new damage rules against both paths.
