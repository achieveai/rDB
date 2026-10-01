// Pure tests: gRPC failure -> typed error, and which failures the retry loop may resend.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import grpc from '@grpc/grpc-js';
import {
  CasConflictError,
  CompactedError,
  NotFoundError,
  RetcdError,
  TooLargeError,
  UnavailableError,
  UnknownOutcomeError,
  classify,
  leaderHint,
  mapError,
} from '../src/errors.mjs';

// A gRPC error as grpc-js hands it to a callback.
function grpcErr(code, details, meta = {}) {
  const md = new grpc.Metadata();
  for (const [k, v] of Object.entries(meta)) md.set(k, String(v));
  return Object.assign(new Error(`${code} ${details}`), { code, details, metadata: md });
}
const S = grpc.status;
const REFUSED = 'No connection established. Last error: Error: connect ECONNREFUSED 127.0.0.1:1';

test('a conflict trailer becomes CasConflictError with the revision to retry with', () => {
  const e = mapError(grpcErr(S.FAILED_PRECONDITION, 'conflict', { 'retcd-outcome': 'rejected', 'retcd-conflict-mod-revision': 7, 'retcd-conflict-exists': 'true' }), { write: true });
  assert.ok(e instanceof CasConflictError && e instanceof RetcdError);
  assert.equal(e.currentRevision, 7);
  assert.equal(e.exists, true);
  const gone = mapError(grpcErr(S.FAILED_PRECONDITION, 'conflict', { 'retcd-conflict-mod-revision': 0, 'retcd-conflict-exists': 'false' }));
  assert.equal(gone.exists, false);
  assert.equal(gone.currentRevision, 0);
});

test('a write that timed out is UnknownOutcome; a read that timed out is just Unavailable', () => {
  const deadline = grpcErr(S.DEADLINE_EXCEEDED, 'Deadline exceeded after 10s');
  assert.ok(mapError(deadline, { write: true }) instanceof UnknownOutcomeError);
  const read = mapError(deadline, { write: false });
  assert.ok(read instanceof UnavailableError);
  assert.ok(!(read instanceof UnknownOutcomeError));
});

test('a connection that died after sending is unknown for a write', () => {
  const e = mapError(grpcErr(S.UNAVAILABLE, 'Connection dropped'), { write: true });
  assert.ok(e instanceof UnknownOutcomeError);
  assert.equal(classify(grpcErr(S.UNAVAILABLE, 'Connection dropped')), 'transport');
});

test('a refused connection is safe: nothing was sent', () => {
  const err = grpcErr(S.UNAVAILABLE, REFUSED);
  assert.equal(classify(err), 'refused');
  const e = mapError(err, { write: true, endpoint: '127.0.0.1:1' });
  assert.ok(e instanceof UnavailableError);
  assert.match(e.message, /Nothing was written/);
});

test('a server rejection (retcd-outcome) is resendable, with or without a leader hint', () => {
  const hinted = grpcErr(S.FAILED_PRECONDITION, 'not leader', { 'retcd-outcome': 'rejected', 'retcd-leader-endpoint': '127.0.0.1:2', 'retcd-leader-node-id': 2 });
  assert.equal(classify(hinted), 'not-leader');
  assert.equal(leaderHint(hinted), '127.0.0.1:2');
  const noLeader = grpcErr(S.UNAVAILABLE, 'read deadline exceeded before the linearizable barrier', { 'retcd-outcome': 'rejected' });
  assert.equal(classify(noLeader), 'not-leader');
  assert.ok(mapError(noLeader, { write: true }) instanceof UnavailableError); // gave up waiting, but nothing applied
  assert.equal(leaderHint(noLeader), undefined);
});

test('a page-token refusal is not mistaken for "not the leader", even with a hint', () => {
  const err = grpcErr(S.FAILED_PRECONDITION, 'expired', { 'retcd-outcome': 'rejected', 'retcd-reason': 'node', 'retcd-leader-endpoint': '127.0.0.1:3' });
  assert.equal(classify(err), 'server');
  const e = mapError(err);
  assert.equal(e.code, 'PAGE_TOKEN');
  assert.equal(e.reason, 'node');
});

test('size limits, compaction, bad arguments and not-found', () => {
  assert.ok(mapError(grpcErr(S.RESOURCE_EXHAUSTED, 'value too large', { 'retcd-outcome': 'rejected' }), { write: true }) instanceof TooLargeError);
  const c = mapError(grpcErr(S.OUT_OF_RANGE, 'compacted', { 'retcd-outcome': 'rejected', 'retcd-min-revision': 42 }));
  assert.ok(c instanceof CompactedError);
  assert.equal(c.minRevision, 42);
  assert.equal(mapError(grpcErr(S.INVALID_ARGUMENT, 'key too long')).code, 'INVALID_ARGUMENT');
  assert.ok(mapError(grpcErr(S.NOT_FOUND, 'nope')) instanceof NotFoundError);
  assert.equal(mapError(grpcErr(S.PERMISSION_DENIED, 'no')).code, 'PERMISSION_DENIED');
});

test('mapError keeps the gRPC code and cause, and leaves typed errors alone', () => {
  const raw = grpcErr(S.UNAVAILABLE, REFUSED);
  const e = mapError(raw);
  assert.equal(e.grpcCode, S.UNAVAILABLE);
  assert.equal(e.cause, raw);
  assert.equal(mapError(e), e);
});
