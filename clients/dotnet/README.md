# Retcd.Client (C#)

Async .NET 8 client for a rEtcd config store. Talks gRPC. Follows the leader for you.

## Add it
```
dotnet pack src/Retcd.Client -o ./nupkgs
dotnet add package Retcd.Client --source ./nupkgs      # or: dotnet add reference src/Retcd.Client
```

## Connect
```csharp
await using var c = RetcdClient.Create(new RetcdClientOptions {
    Endpoints = new[] { "127.0.0.1:17302", "127.0.0.1:17312", "127.0.0.1:17322" }, Timeout = TimeSpan.FromSeconds(10) });
```
A dead node costs about 0.4 s: each call gives a node 0.4 s to connect, then tries the next
(3 s each once every node has missed). Nothing is sent to a node that never connected, so this is safe for writes.

## Use it
```csharp
var rev = await c.PutAsync("app/mode", "blue");                // returns the new revision
var rec = await c.GetAsync("app/mode");                        // null if missing; rec.ValueAsString()
await c.PutAsync("app/mode", "green", ifRevision: rec!.ModRevision);   // compare-and-set; 0 = create only
bool gone = await c.DeleteAsync("app/mode");                   // false if it was not there
await foreach (var r in c.ListAsync("app/**/*.json")) { }      // prefix or glob, pages fetched lazily
var d = await c.ListDirsAsync("app/*");                        // d.Files and d.Dirs (Path, KeyCount); max 64 MiB
await foreach (var e in c.WatchAsync("app/")) { }              // Put/Delete events, survives failover
await c.PutFileAsync("a.bin", "files/a.bin");                  // sha256 saved at meta/files/a.bin
await c.GetFileAsync("files/a.bin", "out.bin");                // checks sha256 before it keeps the file
foreach (var n in await c.HealthAsync()) Console.WriteLine($"{n.Endpoint} {n.Role}");
```
Every call takes a `CancellationToken`. Globs: `*` one folder level, `**` any depth, `?`, `[a-z]`, `[!a-z]`.
Watch from the past: `WatchAsync("app/", fromRevision: 42)`. Each event carries its revision.
`ListDirsAsync` holds its whole result, so it throws `ResultTooLargeException` once the keys and values it keeps pass
`RetcdClient.MaxListDirsBytes` (64 MiB). Bytes, not records, because values dominate. Stream a bigger folder with `ListAsync`.

## Keys are bytes
The server stores keys as bytes. `GetAsync`, `PutAsync` and `DeleteAsync` take a `string` (sent as UTF-8) or a
`ReadOnlyMemory<byte>` (sent as is). `ListAsync(ReadOnlyMemory<byte>)` lists by an exact byte prefix (never a pattern).
Every `RetcdRecord` and `RetcdEvent` has `Key` (UTF-8 text, for display and patterns) and `KeyBytes` (the exact key).
`Key` is lossy for keys that are not valid UTF-8: `[0x80]` and `[0x81]` both read as U+FFFD.
**`KeyBytes` is the form that round-trips**: pass it to the byte overloads to address that record.
The two never disagree: `rec with { Key = "x" }` also sets `KeyBytes`, and `with { KeyBytes = ... }` also sets `Key`.

## Errors
- `CasConflictException` - ifRevision did not match (`CurrentRevision`, `Exists`). Nothing written.
- `ValueTooLargeException` - value over 1 MiB or key over 1024 bytes. Nothing sent.
- `ResultTooLargeException` - a `ListDirsAsync` result past 64 MiB (`Size`, `Limit`). A read; nothing written. Use `ListAsync`.
- `UnknownOutcomeException` - a write was sent and no answer came. **Never retried.** Read before you retry.
- `RetcdUnavailableException` - no node reachable. Nothing was applied.
- `RevisionCompactedException`, `RetcdNotFoundException`, `ChecksumMismatchException` - as named.
- All derive from `RetcdException`. Cancel gives `OperationCanceledException`.

Retries happen only when nothing was applied: "not leader", "no leader yet", and a node the connect step could not
reach (nothing was sent). A write whose error came from the call itself is never resent, even if it reads like a refused
connection: `UnknownOutcomeException`.
A read the transport dropped (node died mid-call) is also resent: a read changes nothing. A write is not.
They stay inside `Timeout`: no attempt or pause runs past it. A List page refused as "minted by another node" is resent there with the same cursor.

## Presence (no leases)
```csharp
await using var hb = RetcdPresence.StartHeartbeat(c, "web-1", TimeSpan.FromSeconds(2));   // writes presence/web-1
await foreach (var ch in RetcdPresence.WatchAsync(c, TimeSpan.FromSeconds(2)))            // same interval
    Console.WriteLine($"{ch.Name} {ch.State}");                                           // Up Late Down Back
```
Beat value: `{"name","pid","seq","sent_at"}`. The monitor uses its own clock. Late at 1.5 beats, Down at 3.
Disposing a heartbeat deletes its key, so the monitor says Down at once.

## Sample and tests
```
dotnet run --project samples/Retcd.Sample -- help           # put get ls watch presence-* bench ...
dotnet test                                                 # offline tests only; live ones are skipped
RETCD_ENDPOINTS=127.0.0.1:17602,127.0.0.1:17612,127.0.0.1:17622 dotnet test
```
Also set `RETCD_CLUSTER_DIR=C:/path/to/cluster` and `CARGO_TARGET_DIR` to run the stop-the-leader test.
It stops and restarts one node of that cluster. Point it only at a cluster you own.

## Limits
Key 1024 bytes. Value 1 MiB. List page 1000. 100 watches per principal per node.

## Changes
- 0.1.0, unreleased: a write is resent only when the client's own connect step failed. A connect error reported by the
  call itself is now `UnknownOutcomeException`; it used to be resent, or reported as "Nothing was sent".
- 0.1.0, unreleased: `RetcdRecord.KeyBytes` and `RetcdEvent.KeyBytes`, byte-key overloads of Get/Put/Delete and a
  byte-prefix `ListAsync`. `ListDirsAsync` stops at 64 MiB with `ResultTooLargeException`.
- 0.1.0, unreleased, **source break**: a bare `null` or `default` key no longer compiles. `GetAsync(null)`,
  `GetAsync(default)`, `ListAsync(null)`, `DeleteAsync(null)` and `PutAsync(null, bytes)` are now ambiguous
  (CS0121) between the `string` and `ReadOnlyMemory<byte>` overloads. None of them ever worked: they failed at run time.
  A typed `string` variable still binds as before.
- 0.1.0, unreleased: the string-key `GetAsync`, `PutAsync` and `DeleteAsync` throw a bad key (empty, null, over
  1024 bytes) at once, from the call itself. They used to return a faulted `Task`. `await` sees no difference; code
  that keeps the `Task` and awaits it later gets the exception earlier.
- 0.1.0, unreleased: a dead first endpoint no longer fails the call. Before, the client waited the whole
  `Timeout` on it and threw `RetcdUnavailableException`.
- 0.1.0, unreleased: "no leader yet" (UNAVAILABLE stamped by the server, no reason) is waited out within
  `Timeout`, for reads and writes. Before, it threw `RetcdUnavailableException` at once.
- 0.1.0, unreleased: a retry pause that would run past `Timeout` is cut short and the call gives up at `Timeout`.
  Before, it could return up to 200 ms late.

## What this is not
- No leases or TTL. Presence is heartbeats plus a watcher.
- No multi-key transactions. One key at a time, optional compare-and-set.
- Reads go to the leader. No follower reads.
- Not for big blobs. Values max 1 MiB.
- Plain HTTP/2 only. No TLS options or client certificates in this client yet. An `https://` endpoint keeps its
  scheme; it is never quietly turned into plaintext.
- A node will not finish stopping while a watch is open on it. The client reconnects, so this is rarely visible.
