---
title: Walk an acceptance recipe red before adopting it
date: 2026-10-23
type: process
component: rdb-api, rdb_dev
tags: [acceptance, walk, recipe, manual-test, red-first, dedup, hand-repro, pre-fix-binary]
applies_when:
  - a critic, architect or lead writes a command recipe that is to prove a fix
  - the recipe has not been run against the unfixed tree
  - a hand-built fault (WAL cut, held link, paused sync) is the evidence for a fix
skip_when: [the recipe was already seen red by the manual tester on the current tree]
source: rdb M9 S3, s3-tester.md w5 and the retrospective; rulings A-R108/A-R108a; rdb M10 S1 A-R215 power-loss reopen (tester retro-s1.md)
status: applied
revalidate_when: rdb_dev compiles conditions differently (Host::compile refusals change)
---

## Problem
Two acceptance recipes for the kernel dedup-aging fix (the critic's 5f and the architect's 5f-k) were adopted by careful readers without being run. Both used a conditional put on a missing key. `Host::compile` refuses that before anything is sent (`request=-`), so no dedup entry ever existed, the next put succeeded, and both recipes were green on the unfixed tree. They would have "proved" the fix without touching it.

## What didn't work
- Reading the kernel path and reasoning that the entry "sits at next_seq": correct about the kernel, wrong about whether the request reaches it.
- Deriving the second recipe from the first: it inherited the same compile-time refusal.

## Solution
No recipe becomes the acceptance walk until the manual tester has run it against the current tree and seen it red for the reason the fix addresses. The tester found 5f-r (fill the cap with applied writes, let them age, resend the originals so the kernel itself answers the condition failure) and it was red before the fix and green after.

For a hand-built fault, keep each walked commit's binaries and run the same script on the pre-fix build; it counts only when it fails there and passes on the fix. M10's A-R215 walk (node 1's WAL cut back before a write) was never ready on f5b3306 and ready in ~2.3 s on the fix, which proved the cut reached the fault. A PASS line plus one confirming log line does not: the cut can land too late, or the data can already be flushed.

## Why it works
An acceptance recipe is a claim about the whole path, not about the kernel. Only running it exercises every refusal between the command and the kernel. "Red for the right reason" is the same bar a regression test has to meet, and it costs one walk. Keeping the binaries makes the red check free, with no rebuild.
