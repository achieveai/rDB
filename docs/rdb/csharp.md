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
(`no_mutation`, retried by the client). A stop (Ctrl-C, or `down`) ends within 45 s of being
asked, however long a call has left: it waits that long in all for running calls to end and write
their lines, then cuts every open connection (`rdb_server_stop_cut`) and logs the calls still
running (`rdb_server_drain_incomplete`), whose lines may be lost. A cut call fails in transport,
so the client reports an unknown outcome: a write already handed to the store may still land.

## The API

```csharp
using var db = RdbClient.Create(new RdbClientOptions { Endpoint = "http://127.0.0.1:19190" });
var ready = await db.ReadyAsync();                 // ready.Ready, ready.Detail
var w = await db.PutAsync("greeting", "hello");    // w.Version, w.RequestId, w.Generation
var r = await db.GetAsync("greeting");             // r.Found false when absent; r.ValueAsString()
var t = await db.TxnAsync(new[] { new RdbTxnPut("a", "90"), new RdbTxnPut("b", "10", ifVersion: 7) });
var s = await db.StatusAsync(t.RequestId, t.Generation);   // Yes, NotKnown, ReadTheObject
```

Values are bytes. Pick your own format (UTF-8 JSON is fine). The gateway never parses them.

`GetAsync` always returns a record. `Found = false` means the key is absent. `RecoveryMode = true`
means the partition recovered from one copy after a loss and serves reads only: the answer may
lack writes acknowledged just before the loss, so it is not authoritative. An absent key may have
been written, and a value may be older than the last one acknowledged.

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
carries a `RequestId`. A refusal carries one whenever the write was sent before it was refused,
whatever its kind: `LEASE_EXPIRED`, and also `CONDITION_FAILED` or `INVALID_ARGUMENT` (for example
a transaction with two puts to the same new key). A write refused before it was sent, such as one
with a key over 1,024 bytes, carries none. `StatusAsync` is for an unknown outcome only.

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
  the largest, 30 s). A read or status attempt is 2 x the server read timeout (1 s by default)
  plus 2 s. A send starts only when a whole attempt still fits in what is left of `Timeout`
  **after connecting**, the first one included.
- **Minimum `Timeout`: one attempt plus 1 s to connect.** That is 13 s for a put, 5 s for a read
  or status, and 14 s for a transaction at the default 5 s deadline. A `Timeout` shorter than one
  attempt is refused before anything is sent: `RdbException` with `Kind = InvalidArgument`,
  `NoMutation = true`, `FromServer = false`. A `Timeout` that covers the attempt but not the
  connect ends "not sent" (`NoMutation = true`): the time left after connecting was too short.
- A `Timeout` near the minimum buys one attempt. During an outage a burst of writes may end well
  before its `Timeout` as `UNAVAILABLE` or `OVERLOADED`, with nothing applied. Give a larger
  `Timeout` to retry through an outage.
- Cancelling a call (its `CancellationToken`) gives an `OperationCanceledException`, with no
  request id; when the transport had already failed the call, that error is its `InnerException`.
  For a write, it may have applied. Settle it by **reading the object**; never send
  it again blindly, or it may apply twice.
- One client shares its connections. Past the gateway's 64 streams on one connection it opens
  another rather than queue the call in the client. A call that fails in transport makes later
  calls connect afresh; calls already running on the old connection run on to their own result.

## Limits and notes

- Keys up to 1,024 bytes, up to 255 puts per transaction, a transaction deadline from 1 ms up to
  30 s. `TxnAsync` refuses a deadline under 1 ms (`ArgumentOutOfRangeException`) before sending.
- An oversized write, a put or transaction over the gateway's decode limit (267,719,656 bytes), is
  refused by the client before sending: `RdbException` with `Kind = InvalidArgument`,
  `NoMutation = true`. The gateway would refuse it with no `RdbError`, which would read as an
  unknown outcome. Reads are not checked.
- One write's whole record must fit 1 MiB (1,048,576 bytes), framing included. One put's
  largest value is **1,048,240 bytes minus the key's length, minus 1 per 0x00 byte in the key**,
  for a new key; 1 byte more for an existing key; the same with or without `ifVersion` (a 3-byte
  key with no 0x00 byte: 1,048,237 new, 1,048,238 existing). A transaction's puts share the 1 MiB.
  The server checks them (`INVALID_ARGUMENT`), exactly as the embedded API does.
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
- **Known limit: memory is not bounded.** The gateway decodes a request of up to 267,719,656 bytes
  (about 255 MiB, `MAX_DECODE_BYTES`) before any check. The 64-call limit bounds running calls,
  not decoding: on one connection, 64 streams can each decode up to 267,719,656 bytes before the
  64-call cap applies, about 16 GiB, and connections are not capped. So many large requests at
  once can exhaust its memory. Tracked in #75.
- **Known limit: an unsent write can read as unknown.** If an extra connection fails to open while
  more than 64 calls are in flight, a write still waiting for it may end unknown though it was
  never sent. Treat unknown as "maybe applied", as always: ask `StatusAsync` or read the object.
- **Local trust.** Until slice 3 the gateway checks no caller: any process on this machine can call
  it and acts as the gateway's app, with its client id and its rights. And while the gateway is
  down, any local process that takes its port receives your calls, values included. Run it only on
  a machine whose local users and processes you trust.
