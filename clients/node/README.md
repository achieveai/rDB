# @retcd/client

Node.js client for a rEtcd cluster. Leader following and failover are built in.

## Install and connect

    cd clients/node && npm install      # only @grpc/grpc-js and @grpc/proto-loader
    import { RetcdClient } from '@retcd/client';   // or './src/index.mjs'
    const c = await RetcdClient.connect({ endpoints: ['127.0.0.1:17302', '127.0.0.1:17312', '127.0.0.1:17322'] });
    c.close();                          // when done

Endpoints are the CLIENT ports. Local cluster: `base + (n-1)*10 + 2` (base 17300: 17302, 17312, 17322).
Give `host:port` or `http://host:port`. The client has no TLS yet, so `https://` (or any other scheme) is
refused when the client is made (`code: 'INVALID_ARGUMENT'`), never sent in plaintext.
The package carries `proto/` (`npm pack` copies it in). Inside the repo it falls back to the repo's `proto/`.
Override with `protoPath` or env `RETCD_PROTO`.
Options: `timeoutMs` (one attempt, 10000), `failoverMs` (the whole call, retries included, 20000).
No attempt or pause runs past `failoverMs`: each attempt's deadline is the smaller of `timeoutMs` and the time left,
and a pause that would not fit ends the call at once.
A dead node costs about 0.4 s: each call gives a node 0.4 s to connect, then tries the next
(3 s each once every node has missed). Nothing is sent to a node that never connected, so this is safe for writes.

## Use it

    const { revision } = await c.put('app/mode', 'blue');      // string or Buffer, up to 1 MiB
    const rec = await c.get('app/mode');                       // {key, value: Buffer, createRevision, modRevision} or null
    await c.put('app/mode', 'green', { ifRevision: revision }); // compare-and-set. 0 = only if new
    await c.delete('app/mode');                                // true, or false if it was not there
    for await (const r of c.list('app/')) {}                   // prefix, paged for you
    for await (const r of c.list('docs/**/*.md')) {}           // glob: * one level, ** any depth, ? one char, [a-z]
    await c.listDirs('docs');                                  // files + {dir:'docs/sub/', count}; max 64 MiB
    const ac = new AbortController();
    for await (const e of c.watch('app/', { signal: ac.signal })) console.log(e.type, e.key, e.revision);
    await c.putFile('./logo.png', 'files/logo.png');           // + meta/files/logo.png {size, sha256, stored_at_rev}
    await c.getFile('files/logo.png', './copy.png');           // checks sha256
    await c.health();                                          // [{endpoint, ok, ready, role, leader, revision}]

- `watch` starts "from now", or pass `fromRevision`. It reconnects by itself and resumes after the last
  revision it saw, so nothing repeats or is skipped. `break` or `ac.abort()` ends it.
- List then watch with no gap: `const it = c.list('app/'); for await (...) {}; c.watch('app/', { fromRevision: it.readRevision })`.
- Globs are matched in the client. The server scans only the text before the first `* ? [`.
- `listDirs` holds its whole result, so it throws `ResultTooLargeError` (`code: 'RESULT_TOO_LARGE'`) once the keys and values it keeps pass
  `LIMITS.maxListDirsBytes` (64 MiB). Bytes, not records, because values dominate. Stream a bigger folder with `list`.

## Keys are bytes

The server stores keys as bytes. `get`, `put` and `delete` take a string (sent as UTF-8) or a Buffer (sent as is).
Every record and watch event has `key` (the UTF-8 text, for display and globs) and `keyBytes` (a Buffer, the exact key).
`key` is lossy for keys that are not valid UTF-8: `[0x80]` and `[0x81]` both read as `'�'`.
**`keyBytes` is the form that round-trips**: pass it back to `get`/`put`/`delete` to address that record.
`list(buffer)` lists by an exact byte prefix (never a glob).

## Errors

All extend `RetcdError` (has `.code`).

| Error | Meaning |
|---|---|
| `CasConflictError` | `ifRevision` was stale. `.currentRevision` to retry with, `.exists` |
| `NotFoundError` | getFile of a missing key (`get` returns null, `delete` returns false) |
| `TooLargeError` | key over 1 KiB or value over 1 MiB. Not sent |
| `ResultTooLargeError` | `listDirs` result past 64 MiB. A read; nothing was written. `.size`, `.limit`. Use `list` |
| `UnknownOutcomeError` | a write timed out or the link died after sending. Maybe applied. **Never retried for you.** `get` the key to see |
| `UnavailableError` | no node served it in time. For a write: nothing was written |
| `CompactedError` | watch history is gone. `.minRevision`. List again, then watch |

Also `IntegrityError` (getFile sha256 mismatch). Safe automatic retries: "not the leader", "no leader yet", a node the
connect step could not reach (nothing was sent), and reads. A write whose error came from the call itself is never resent,
whatever its text says: `UnknownOutcomeError`. So is a write that hit its deadline, even one the server stamped.
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

- 0.1.0, unreleased: an `https://` endpoint, or any scheme but `http://`, now throws `INVALID_ARGUMENT`. It used to be
  stripped and the client connected in plaintext.
- 0.1.0, unreleased: a write is resent to another node only when the connect step failed. A refusal reported by the call
  itself, and a server-stamped write `DEADLINE_EXCEEDED`, are now `UnknownOutcomeError`. Both used to be `UnavailableError`
  "Nothing was written", and the refusal was resent.
- 0.1.0, unreleased: `failoverMs` bounds the whole call. An attempt used to get a fresh `timeoutMs` even near the end of it,
  and a pause after a failure could end up to 0.3 s past the budget. A pause that would not fit now ends the call at once.
- 0.1.0, unreleased: records and watch events carry `keyBytes`; `list` takes a Buffer prefix. `listDirs` stops at 64 MiB
  with `ResultTooLargeError`.

- 0.1.0, unreleased: `delete` resolves `true` if it deleted the key and `false` if the key was missing.
  It used to throw `NotFoundError` and resolve `{ revision }`. Same as C# `DeleteAsync`.
