# @retcd/client

Node.js client for a rEtcd cluster. Leader following and failover are built in.

## Install and connect

    cd clients/node && npm install      # only @grpc/grpc-js and @grpc/proto-loader
    import { RetcdClient } from '@retcd/client';   // or './src/index.mjs'
    const c = await RetcdClient.connect({ endpoints: ['127.0.0.1:17302', '127.0.0.1:17312', '127.0.0.1:17322'] });
    c.close();                          // when done

Endpoints are the CLIENT ports. Local cluster: `base + (n-1)*10 + 2` (base 17300: 17302, 17312, 17322).
The package carries `proto/` (`npm pack` copies it in). Inside the repo it falls back to the repo's `proto/`.
Override with `protoPath` or env `RETCD_PROTO`.
Options: `timeoutMs` (one attempt, 10000), `failoverMs` (hunt for a leader, 20000).
A dead node costs about 0.4 s: each call gives a node 0.4 s to connect, then tries the next
(3 s each once every node has missed). Nothing is sent to a node that never connected, so this is safe for writes.

## Use it

    const { revision } = await c.put('app/mode', 'blue');      // string or Buffer, up to 1 MiB
    const rec = await c.get('app/mode');                       // {key, value: Buffer, createRevision, modRevision} or null
    await c.put('app/mode', 'green', { ifRevision: revision }); // compare-and-set. 0 = only if new
    await c.delete('app/mode');                                // true, or false if it was not there
    for await (const r of c.list('app/')) {}                   // prefix, paged for you
    for await (const r of c.list('docs/**/*.md')) {}           // glob: * one level, ** any depth, ? one char, [a-z]
    await c.listDirs('docs');                                  // files + {dir:'docs/sub/', count}
    const ac = new AbortController();
    for await (const e of c.watch('app/', { signal: ac.signal })) console.log(e.type, e.key, e.revision);
    await c.putFile('./logo.png', 'files/logo.png');           // + meta/files/logo.png {size, sha256, stored_at_rev}
    await c.getFile('files/logo.png', './copy.png');           // checks sha256
    await c.health();                                          // [{endpoint, ok, ready, role, leader, revision}]

- `watch` starts "from now", or pass `fromRevision`. It reconnects by itself and resumes after the last
  revision it saw, so nothing repeats or is skipped. `break` or `ac.abort()` ends it.
- List then watch with no gap: `const it = c.list('app/'); for await (...) {}; c.watch('app/', { fromRevision: it.readRevision })`.
- Globs are matched in the client. The server scans only the text before the first `* ? [`.

## Errors

All extend `RetcdError` (has `.code`).

| Error | Meaning |
|---|---|
| `CasConflictError` | `ifRevision` was stale. `.currentRevision` to retry with, `.exists` |
| `NotFoundError` | getFile of a missing key (`get` returns null, `delete` returns false) |
| `TooLargeError` | key over 1 KiB or value over 1 MiB. Not sent |
| `UnknownOutcomeError` | a write timed out or the link died after sending. Maybe applied. **Never retried for you.** `get` the key to see |
| `UnavailableError` | no node served it in time. For a write: nothing was written |
| `CompactedError` | watch history is gone. `.minRevision`. List again, then watch |

Also `IntegrityError` (getFile sha256 mismatch). Safe automatic retries: "not the leader", "no leader yet", refused connection, and reads.
A List page refused as "minted by another node" is resent there with the same cursor. Other cursor refusals throw `code: 'PAGE_TOKEN'`.

## Presence (no leases, so heartbeats)

    import { startHeartbeat, watchPresence } from '@retcd/client';
    const hb = startHeartbeat(c, 'api', { intervalMs: 2000 });   // writes presence/api = {name,pid,seq,sent_at}
    const mon = watchPresence(c, { intervalMs: 2000, missedBeats: 3 });
    mon.on('change', ({ name, state, lastSeen }) => console.log(name, state)); // up | late | down | back
    await hb.stop({ remove: true });                             // remove: monitors see "down" at once

The monitor uses its own clock: `late` after 1.5 intervals, `down` after `missedBeats`. Use the same interval on both sides.
Also `for await (const ev of mon)`. Call `mon.stop()` and `hb.stop()` or the process will not exit.

## Limits

Value 1 MiB, key 1 KiB, list page 1000 items or 8 MiB (the client pages on), 100 watch streams per principal.

## What this is not

- No leases or TTLs. No multi-key transactions (putFile is two writes).
- Reads go to the leader only. No follower reads.
- Values up to 1 MiB. Dev cluster is plaintext: no TLS or auth in this client yet.

Try it: `RETCD_ENDPOINTS=... node examples/quickstart.mjs`, `node examples/presence.mjs`.
Tests: `npm test` (no cluster). With `RETCD_ENDPOINTS=...` it also runs `test/live.test.mjs`.
Types: `npm run typecheck` compiles `examples/types-check.ts` against `src/index.d.ts`.

## Changes

- 0.1.0, unreleased: `delete` resolves `true` if it deleted the key and `false` if the key was missing.
  It used to throw `NotFoundError` and resolve `{ revision }`. Same as C# `DeleteAsync`.
