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
List a live node first. A dead first node costs ~2 s once per process.

## Use it
```csharp
var rev = await c.PutAsync("app/mode", "blue");                // returns the new revision
var rec = await c.GetAsync("app/mode");                        // null if missing; rec.ValueAsString()
await c.PutAsync("app/mode", "green", ifRevision: rec!.ModRevision);   // compare-and-set; 0 = create only
bool gone = await c.DeleteAsync("app/mode");                   // false if it was not there
await foreach (var r in c.ListAsync("app/**/*.json")) { }      // prefix or glob, pages fetched lazily
var d = await c.ListDirsAsync("app/*");                        // d.Files and d.Dirs (Path, KeyCount)
await foreach (var e in c.WatchAsync("app/")) { }              // Put/Delete events, survives failover
await c.PutFileAsync("a.bin", "files/a.bin");                  // sha256 saved at meta/files/a.bin
await c.GetFileAsync("files/a.bin", "out.bin");                // checks sha256 before it keeps the file
foreach (var n in await c.HealthAsync()) Console.WriteLine($"{n.Endpoint} {n.Role}");
```
Every call takes a `CancellationToken`. Globs: `*` one folder level, `**` any depth, `?`, `[a-z]`, `[!a-z]`.
Watch from the past: `WatchAsync("app/", fromRevision: 42)`. Each event carries its revision.

## Errors
- `CasConflictException` - ifRevision did not match (`CurrentRevision`, `Exists`). Nothing written.
- `ValueTooLargeException` - value over 1 MiB or key over 1024 bytes. Nothing sent.
- `UnknownOutcomeException` - a write was sent and no answer came. **Never retried.** Read before you retry.
- `RetcdUnavailableException` - no node reachable. Nothing was applied.
- `RevisionCompactedException`, `RetcdNotFoundException`, `ChecksumMismatchException` - as named.
- All derive from `RetcdException`. Cancel gives `OperationCanceledException`.

Retries happen only when nothing was applied: "not leader" and "connection refused".

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

## What this is not
- No leases or TTL. Presence is heartbeats plus a watcher.
- No multi-key transactions. One key at a time, optional compare-and-set.
- Reads go to the leader. No follower reads.
- Not for big blobs. Values max 1 MiB.
- Plain HTTP/2 only. No TLS or client certificates in this client yet.
- A node will not finish stopping while a watch is open on it. The client reconnects, so this is rarely visible.
