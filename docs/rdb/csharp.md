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
scripts/rdb-server-dev.sh down        # stop the gateway gracefully, then lr down
scripts/rdb-server-dev.sh clean
```

Defaults: lr on base port 19100, the gateway on `127.0.0.1:19190`, runs under
`C:/rdb_test_data/lr-cs`. `--base-port`, `--listen` and `--root` change them. `kill` kills the
gateway as a crash; `gateway` starts it again on the same run; `resume` runs `lr up --resume` then
the gateway. The gateway's output is `<run>/out/rdb-server.out`, its log `<run>/logs/rdb-server.jsonl`
(never keys or values). Each call writes one `rdb_server_call` line: `method`, `kind` (`OK`, the
error kind, or the gRPC code), `ms`, `request_id`, `no_mutation` (on errors), and `caller_gone`
(true when the client had given up before the call ended: that is the line an operator settles an
unknown write from). Each write that landed also writes `rdb_server_write`: `method`, `request_id`,
`generation`, `version`. A gateway past 64 running calls refuses more with `OVERLOADED`
(`no_mutation`, retried by the client). On Ctrl-C it waits up to 45 s for running calls to end and
write their lines.

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

`FromServer = false` with `Kind = Unavailable` on a **write** means nothing was sent: the client
could not connect within `Timeout`. A write while the gateway is down is "not sent", never
"unknown". On a **read** it can also mean the read was sent and not answered (the message says "no
answer"); a read changes nothing, so call it again.

A refusal with `NoMutation = true` wrote nothing and needs no `StatusAsync`, whether or not it
carries a `RequestId`. Some carry one (`LEASE_EXPIRED`); some never do (`CONDITION_FAILED`,
`INVALID_ARGUMENT`). `StatusAsync` is for an unknown outcome only.

Errors the client makes itself (`FromServer = false`) carry a retry class too: `BoundedJitter`
for "not sent" and for a read with no answer (call again later), `Reconcile` for a write whose
answer was lost with no request id (read the object, then decide).

## Retries

- The client sends a call again only when nothing was applied: it could not connect, or the server
  refused it with `NoMutation` and a retry rule of RefreshRoute, RetryAfterRecovery or
  BoundedJitter. A read retries on the rule alone, and after a lost transport.
- Back-off is jittered, at least the server's `RetryAfter`, and bounded by time (`Timeout`,
  default 45 s), never by count.
- Each attempt waits the server's worst case plus 2 s: 12 s for a put, 4 s for a read or status,
  and for a transaction 5 s + its deadline + 1 s + 2 s (13 s at the default 5 s deadline, 38 s at
  the largest, 30 s). A send starts only when a whole attempt still fits in `Timeout`, the first
  one included. A `Timeout` shorter than one attempt is refused before anything is sent:
  `RdbException` with `Kind = InvalidArgument`, `NoMutation = true`, `FromServer = false`.
- Cancelling a call (its `CancellationToken`) gives a bare `OperationCanceledException`, with no
  request id. For a write, it may have applied. Settle it by **reading the object**; never send
  it again blindly, or it may apply twice.
- One client shares one connection. A call that fails in transport makes later calls connect
  afresh; calls already running on the old connection run on to their own result.

## Limits and notes

- Keys up to 1,024 bytes, up to 255 puts per transaction, a transaction deadline up to 30 s.
- One write's record must fit 1 MiB (1,048,576 bytes): 265 bytes of framing, then per put 10
  bytes plus its key plus its value, and 1 byte per `ifVersion`. So one put's largest value is
  1,048,301 bytes minus the key's length (1,048,298 for a 3-byte key); a transaction's puts share
  the 1 MiB. The server checks them (`INVALID_ARGUMENT`), exactly as the embedded API does.
- **No Delete yet** (slice 2). Workaround: write an empty value (or your format's tombstone) and
  treat it as absent.
- A version is the partition's, not the object's: every write in the partition raises it, so an
  object's version can jump by more than one between two of your writes. Compare versions for
  equality (`ifVersion`), never count on them.
- **One gateway is one client.** Every app on one gateway shares its client id and its 8 slots for
  unsettled writes: under an outage the ninth unknown write evicts the oldest, whose status then
  says ReadTheObject. Ask `StatusAsync` soon after an unknown outcome.
- Ask status about your **latest** write. Each later write tells the server you have the earlier
  answers, so it may drop them: a status of an older write can say ReadTheObject.
- Loopback only: `--listen` refuses `0.0.0.0`, `[::]`, `[::ffff:127.0.0.1]`, LAN addresses and host
  names. TLS for apps comes in slice 3.
- **Known limit: memory is not bounded.** The gateway decodes a request of up to 254 MiB
  (`MAX_DECODE_BYTES`) before any check, and it does not cap connections, so many large requests
  at once can exhaust its memory. The 64-call limit bounds running calls, not decoding.
- **Local trust.** Until slice 3 the gateway checks no caller: any process on this machine can call
  it and acts as the gateway's app, with its client id and its rights. And while the gateway is
  down, any local process that takes its port receives your calls, values included. Run it only on
  a machine whose local users and processes you trust.
