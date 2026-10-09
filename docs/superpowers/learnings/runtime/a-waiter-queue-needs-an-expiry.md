---
title: "A per-key waiter queue needs an expiry for the waiter nobody will answer"
date: 2026-10-08
type: bug
component: rdb-api host
tags: [queue, waiter, liveness, expiry, review]
applies_when:
  - Adding a queue so that a second call under one key waits behind the first instead of displacing it
  - A waiter is released only by a reply from another module or a remote party
skip_when: [The reply is guaranteed by the same step that installs the waiter]
source: "rEtcd M9 S0 review, F-002 fix (b71961c) then F-014 (05f4e40, 9a2b372, e585443)"
status: applied
revalidate_when: P1 gains a reply guarantee for every write, or the host stops keying waiters by identity
---

## Problem
F-002 was a displacement bug: a second call under the same request identity took the first waiter's slot. The fix (b71961c) queued later calls behind the live waiter and released the queue when that waiter's reply arrived. But P1 can hold a write's reply back for good, for example after a deny at the reply check, a fence, or a recovery. Then the head waiter never cleared, and every later call under that identity waited forever. That included the status call the error contract tells the caller to make. The re-review caught it as a regression from the commit before the fix.

## What didn't work
- Release on reply only. That assumes every waiter is answered, which the other module does not promise.
- Queuing every call kind under the identity. Status calls then waited behind the very write they were asking about.
- Testing the expiry without anything queued behind it. The release step it runs was unprotected, so deleting it left every row green (F-017).

## Solution
- Every waiter that depends on another party's reply gets an expiry: its deadline plus a margin. It answers UNKNOWN_OUTCOME, then releases the queue.
- The expiry carries the waiter's event id and fires only for that waiter, so it can never touch a successor.
- Read-only calls about the same key, such as status, are never queued behind it.
- Each step has a row and a mutant: no expiry, no release, no event-id guard, status queued again.

## Why it works
A queue turns "one waiter may be lost" into "every later waiter is lost". So the queue's liveness rests on its head always ending. The only end the host controls is its own timer. The event-id guard keeps that timer from becoming a new displacement bug.
