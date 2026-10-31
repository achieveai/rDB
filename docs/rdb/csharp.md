# rDB from C# (slice 1)

A C# app talks to rDB through `rdb-server`, a small gateway. The gateway holds one `rdb_api::Db`
as an app and serves gRPC on loopback. `Rdb.Client` (in `clients/dotnet`) calls it.

```text
C# app -- Rdb.Client -- gRPC, loopback, plaintext --> rdb-server -- mTLS --> rdb_node 1..3
```

## Run it on one machine

```sh
scripts/rdb-server-dev.sh up          # lr up (3 node processes) + the gateway; prints RDB_ENDPOINT
cd clients/dotnet
dotnet run --project samples/Rdb.Sample -- ready
dotnet run --project samples/Rdb.Sample -- put greeting hello
dotnet run --project samples/Rdb.Sample -- get greeting
dotnet run --project samples/Rdb.Sample -- walk
scripts/rdb-server-dev.sh down        # the gateway, then lr down
scripts/rdb-server-dev.sh clean
```

Defaults: lr on base port 19100, the gateway on `127.0.0.1:19190`, runs under
`C:/rdb_test_data/lr-cs`. `--base-port`, `--listen` and `--root` change them. `kill` kills the
gateway as a crash; `gateway` starts it again on the same run; `resume` runs `lr up --resume` then
the gateway. The gateway's output is `<run>/out/rdb-server.out`, its log `<run>/logs/rdb-server.jsonl`
(one `rdb_server_call` line per call: method, kind, ms, request id; never keys or values).

## The API

```csharp
using var db = RdbClient.Create(new RdbClientOptions { Endpoint = "http://127.0.0.1:19190" });
var ready = await db.ReadyAsync();                 // ready.Ready, ready.Detail
var w = await db.PutAsync("greeting", "hello");    // w.Version, w.RequestId, w.Generation
var r = await db.GetAsync("greeting");             // null when absent; r.ValueAsString()
var t = await db.TxnAsync(new[] { new RdbTxnPut("a", "90"), new RdbTxnPut("b", "10", ifVersion: 7) });
var s = await db.StatusAsync(t.RequestId, t.Generation);   // Yes, NotKnown, ReadTheObject
```

Values are bytes. Pick your own format (UTF-8 JSON is fine). The gateway never parses them.

## Errors

The server's decision rides in the gRPC rich status (`grpc-status-details-bin`, a
`google.rpc.Status` holding one `rdb.v1.RdbError`). The client turns it into:

- `RdbConditionFailedException`: a conditional write was refused. Nothing was written.
- `RdbUnknownOutcomeException`: the write may have applied. It is never sent again for you.
  With `RequestId` set, call `StatusAsync(RequestId, Generation)`. Without one, the answer was lost
  on the way: read the object.
- `RdbException`: anything else. `Kind`, `KindName`, `Retry`, `NoMutation`, `RequestId`,
  `Generation`, `RetryAfter`, `FromServer`. `Message` ends with the next step.

`FromServer = false` with `Kind = Unavailable` means nothing was sent: the client could not connect
within `Timeout`. A write while the gateway is down is "not sent", never "unknown".

## Retries

- The client sends a call again only when nothing was applied: it could not connect, or the server
  refused it with `NoMutation` and a retry rule of RefreshRoute, RetryAfterRecovery or
  BoundedJitter. A read retries on the rule alone, and after a lost transport.
- Back-off is jittered, at least the server's `RetryAfter`, and bounded by time (`Timeout`,
  default 30 s), never by count.
- Each attempt waits the server's worst case: about 12 s for a put, the deadline plus about 11 s
  for a transaction. A retry starts only when a whole attempt still fits in `Timeout`. A `Timeout`
  shorter than one attempt still makes one attempt, cut at `Timeout`; a write cut that way is
  `RdbUnknownOutcomeException` with no request id.
- Cancelling a write gives `OperationCanceledException`. The write may have applied.

## Limits and notes

- Keys up to 1,024 bytes, up to 254 puts per transaction, a transaction deadline up to 30 s. The
  server checks them (`INVALID_ARGUMENT`), exactly as the embedded API does.
- **No Delete yet** (slice 2). Workaround: write an empty value (or your format's tombstone) and
  treat it as absent.
- **One gateway is one client.** Every app on one gateway shares its client id and its 8 slots for
  unsettled writes: under an outage the ninth unknown write evicts the oldest, whose status then
  says ReadTheObject. Ask `StatusAsync` soon after an unknown outcome.
- Ask status about your **latest** write. Each later write tells the server you have the earlier
  answers, so it may drop them: a status of an older write can say ReadTheObject.
- Loopback only: `--listen` refuses `0.0.0.0`, `[::]`, `[::ffff:127.0.0.1]`, LAN addresses and host
  names. TLS for apps comes in slice 3.
