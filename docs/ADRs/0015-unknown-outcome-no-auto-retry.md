# ADR-0015: Unknown mutation outcome and no automatic replay

**Status:** Accepted  
**Date:** 2026-09-17  
**Spec:** §8.1, §16, §21 M3

## Decision

- A mutation whose deadline expires (or whose connection drops) after submission returns
  `ConfigError::DeadlineExceededUnknownOutcome`. The client library **never** re-sends the
  mutation on its own.
- The only automatic retries in `GrpcClient` are: following a `NotLeader` hint (the request was
  rejected before entering the log) and reconnecting on `Unavailable` returned before
  submission. Both are bounded.
- Recovery recipe (documented in the client rustdoc): `get(key)` and compare
  `mod_revision`/value, then issue a CAS with the observed revision.
- Deadlines: every request carries a client deadline (default 5 s); the server honors gRPC
  deadlines and returns `DEADLINE_EXCEEDED` if commit does not complete in time. The entry may
  still commit afterwards.

## Verification

- M3 test: harness drops the response of a committed Put; client observes
  `DeadlineExceededUnknownOutcome`; a subsequent Get shows exactly one revision allocated and
  the server-side apply count is one.

## Notes (2026-09-18, M1 delivery)

### `retcd-outcome: rejected` marks a status the server chose to send

The rule above is only enforceable if the client can tell a *decision* from a *disconnection*,
and gRPC gives it no way to: a node that says "I am not formed yet" and a connection that died
mid-request both arrive as `UNAVAILABLE`. Reading the code alone, a dropped connection after a
mutation was written to the socket looked like a rejection, and the client reported an error the
caller was told was safe to resubmit — the precise failure this ADR exists to prevent.

The marker closes it. Every `Status` either plane derives from a `ConfigError` carries the ASCII
metadata `retcd-outcome: rejected`, stamped in one place (`config_grpc::mark_rejected`, applied
by `status_from_error` and on the pre-handler rejection paths). A status is therefore
attributable to a server that decided something; a status without the marker was produced by the
transport, by a proxy, or by the client stack itself.

The client (`config_client::classify`) reads it as follows.

| status | mutation (`put`/`delete`) | read (`get`/`list`) |
| --- | --- | --- |
| marked `rejected` | mapped per ADR-0010 §6.2 | mapped per ADR-0010 §6.2 |
| `DEADLINE_EXCEEDED` | `DeadlineExceededUnknownOutcome` | `Unavailable` |
| unmarked, any other code | `DeadlineExceededUnknownOutcome` | `Unavailable` |

The asymmetry is the point: a read has no outcome to be uncertain about, so an unmarked failure
there is plainly `Unavailable` and freely retryable. A mutation that reached the socket may have
committed, so an unmarked failure is unknown and never replayed.

Consequences accepted:

- A mutation that failed *before* leaving the client (no route, TLS handshake refused) is also
  reported as unknown. Safety is preferred to precision; the caller's recovery recipe (read back,
  then CAS) is correct either way.
- A middlebox that strips unknown response metadata downgrades every rejection to "unknown".
  That is the safe direction, and rEtcd does not support terminating proxies on either plane.
- The marker is additive response metadata, like the leader hint (ADR-0010). It carries no key or
  value bytes.

### The request deadline is one budget for the whole operation

`GrpcClientOptions::request_deadline` bounds the operation the caller asked for, not each hop of
a leader-hint chase. Each attempt is given the remaining budget, both as the gRPC deadline the
server sees (`grpc-timeout`) and as a local timeout, so a server is never left working on a call
whose answer nobody will read. When the budget is spent before a follow, the last error is
returned rather than a fresh attempt. Previously each hop got a full deadline, so a chase of
three hints could take four times the budget the caller set.

## Note (2026-09-18, M3): a failure to connect is not an unknown outcome

The table above turns on *who produced the error*, and it silently answered a question it was
never asked: what about a request that was never produced at all? A `put` whose TLS handshake
was refused, or whose endpoint was not listening, arrived at `classify` as an unmarked status
and was reported as `DeadlineExceededUnknownOutcome`. The first version of this ADR accepted
that under "safety is preferred to precision". In M3 it stopped being free. §4.2's negative
rows — a client pointed at an untrusted certificate, a stopped node, an impostor at a hinted
endpoint (M3-54, M3-18, M3-64) — are all *demonstrably* pre-submission, and reporting them as
unknown sent a caller off to run the read-back-then-CAS recovery recipe for a mutation that
provably never left the process. A recovery procedure that runs when nothing happened teaches
operators to ignore it.

The fix is to make the phase observable rather than to guess at it. `config-client` builds each
channel with an explicit `Endpoint::connect().await` at first use instead of `connect_lazy()`,
bounded by whatever is left of `request_deadline`. The rule is now:

| where the failure happened | mutation | read |
| --- | --- | --- |
| connect phase (no route, refused connection, refused or rejected TLS handshake) | `Unavailable` | `Unavailable` |
| after a connected channel accepted the request, unmarked | `DeadlineExceededUnknownOutcome` | `Unavailable` |
| marked `retcd-outcome: rejected` | per ADR-0010 §6.2 | per ADR-0010 §6.2 |

Consequences accepted:

- **Connect failures are retried; nothing else new is.** Nothing was submitted, so a retry
  cannot duplicate anything. Reconnects are bounded by `max_hint_follows` and, like hint
  follows, spend the one request budget. They are counted in `ClientStats::reconnects` rather
  than in `sends`, which keeps `sends` meaning "requests actually put on the wire" — the
  counter a caller reasons with when it asks whether a mutation could have been applied — while
  still making "reconnect bounded" assertable (M3-64). The no-replay rule for a *submitted*
  mutation is untouched.
- **A channel that was connected and whose peer then died is still ambiguous.** tonic
  re-establishes a broken connection inside the request path, and the resulting status is
  indistinguishable from a mid-flight drop. Such a mutation stays `DeadlineExceededUnknownOutcome`
  — the safe direction. The client drops the cached channel whenever a transport-minted status
  appears, so the *next* operation re-runs the connect phase and gets the honest answer; the
  ambiguity is bounded to the one operation that raced the failure.
- **Construction still does not dial.** `GrpcClient::connect` validates endpoint syntax and the
  TLS profile and returns; a cluster that is still starting is not a configuration error.

## Note (2026-09-18, fix round): a successful `connect()` is not an accepted connection

The M3 note above moved the connect phase into the open, and then trusted the wrong signal for
its end. `Endpoint::connect().await` returning `Ok` was taken to mean "the channel is
established". Under TLS 1.3 it does not. The client sends its `Finished` and considers the
connection open before the server has validated the client certificate, so a server that
*refuses* that certificate — expired, wrong CA, revoked — still lets `connect()` succeed and
sends its alert afterwards. The refusal then surfaces on the first RPC as an unmarked
`CANCELLED`, which the table above reads as a post-submission failure: a `put` was reported
`DeadlineExceededUnknownOutcome` although no request had reached any store. That is the same
bug the M3 note was written to fix, arriving through a door the note left open, and on the
failure an operator actually meets — a certificate that expired overnight.

The phase is therefore ended by an observation rather than by an API return. Under
`TlsMode::MutualTls`, a freshly connected channel is probed before it counts as established,
and the probe asks the smallest question that settles the matter: **did this server's gRPC
layer answer us at all?** It cannot answer before it has accepted the handshake, so any answer
ends the connect phase.

It asks by calling a method that does not exist, `/retcd.v1.ConfigService/ConnectProbe`.
tonic's router answers an unknown path with `UNIMPLEMENTED` from the routing layer, which is
what makes this choice the right one: the call stops short of every handler. No principal is
derived, no `Authorizer` is consulted, no audit line is written, and — the load-bearing part —
no `ConfigStore` is touched.

- `UNIMPLEMENTED`, or absurdly a response, establishes the channel.
- A status the transport minted while the server's alert was arriving, or silence until the
  budget runs out, means the channel is unusable: `ConfigError::Unavailable`, never cached, and
  the operation's existing bounded reconnect loop decides what happens next inside the one
  request budget. The refused client certificate lands here, which is the point of the whole
  exercise.

`ClientStats::sends` is untouched either way: the probe is not the caller's request. The cost
is one round trip per **fresh** channel, never per request, and it carries no trace headers —
it is not part of the caller's operation, and stamping it with that `request_id` would put a
synthetic call into the log query for every `put` that had to dial. The mutation table is
unchanged; what changed is which failures reach its unmarked row.

### Why not probe with a real method

The first implementation sent a `Get` with an empty key, on the reasoning that every rEtcd
server refuses it deterministically with a marked `INVALID_ARGUMENT` — `config_core::validate_get`
runs at the API edge ahead of authorization, so the answer needs no grant and writes no audit
line. That is all true of a *correct* server, and it was verified in the engine before being
chosen. It was still the wrong probe, and two harness rows proved it inside this fix round.

- **It makes a transport decision depend on a backend's behaviour.** A `ConfigStore` that does
  not validate ahead of itself sees the probe as an ordinary request. `config-testkit`'s M3-54
  counts calls on the store behind a bare client plane to prove a mutation reached the node it
  was hinted to; the probe showed up there as a phantom `Get` and the count read 2. The store
  is not wrong — it is a double, sitting where an embedder's store may equally sit.
- **It invites the probe to judge the answer.** Requiring the marked `INVALID_ARGUMENT`
  specifically rejected a follower that answers every `Get` with `NotLeader` (M3-54's honest
  half), and reporting any other marked status verbatim turned that follower's hint into the
  caller's result — an answer to a request nobody made. Accepting *any* marked status in turn
  swallowed the one verdict worth surfacing.

Calling a nonexistent path removes the whole question. It reaches no store to disturb, and the
only answer it can get is the router's.

Note what the probe deliberately no longer decides. A client certificate minted for another
cluster now *passes* it, because the cluster check lives in the service handler the probe never
reaches (M3-17). The caller's real request then returns a marked `UNAUTHENTICATED` on an
established channel — accurate, precise, and exactly what `classify` exists to map. Telling a
connection apart from a rejection is the probe's job; pre-judging requests is not.

Consequences accepted:

- **The probe depends on tonic's routing behaviour**, not on rEtcd's. If a future server
  answered unknown methods with something other than `UNIMPLEMENTED` — a catch-all handler, a
  gateway in front of the plane — a fresh channel would read as unusable and the client would
  fail closed with `Unavailable`. Failing closed is the safe direction, and rEtcd does not
  support terminating proxies on either plane (ADR-0010).
- **Insecure mode is not probed.** There is no client certificate to be refused, and the
  failures that remain there are already visible at `connect()`.
- **A server that starts refusing us mid-life is still ambiguous** for the one operation that
  races the change, exactly as the M3 note describes. The probe bounds the window to a fresh
  channel; it does not remove it.

Verified by `config-client`'s `m3_client_65` row: a client holding an expired certificate,
against a server whose CA it trusts, gets `Unavailable` from a `put`, with `sends == 0`, no
call reaching the store, bounded reconnects, and a `client connect attempt failed` line.

## Note (2026-09-18, fix round): a fatal Raft core on the write path is an unknown outcome

`config-engine` mapped every non-storage `Fatal` to `Unavailable`, on both the read and the
write path. openraft returns `Fatal::Stopped`/`Fatal::Panicked` through the `client_write` reply
channel, which the core only drops *after* the proposal was enqueued, so the entry may already
be committed — and `Unavailable` tells the caller it was rejected before entering the log. The
write path now answers `DeadlineExceededUnknownOutcome`; the read path keeps `Unavailable`,
because `ensure_linearizable` submits nothing. `Fatal::StorageError` stays `FatalStorage` on both.
