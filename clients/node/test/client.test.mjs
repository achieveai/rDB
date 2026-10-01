// Client logic with a fake transport: leader following, retry rules, limits, paging, watch resume.
// No cluster needed. (The real thing is test/live.test.mjs.)
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { test } from 'node:test';
import grpc from '@grpc/grpc-js';
import { CasConflictError, NotFoundError, RetcdClient, TooLargeError, UnavailableError, UnknownOutcomeError } from '../src/index.mjs';

const S = grpc.status;
const A = '10.0.0.1:1';
const B = '10.0.0.2:1';
const C = '10.0.0.3:1';
const REFUSED = 'No connection established. Last error: Error: connect ECONNREFUSED';

function grpcErr(code, details, meta = {}) {
  const md = new grpc.Metadata();
  for (const [k, v] of Object.entries(meta)) md.set(k, String(v));
  return Object.assign(new Error(`${code} ${details}`), { code, details, metadata: md });
}
const notLeader = (hint) => grpcErr(S.FAILED_PRECONDITION, 'not leader', { 'retcd-outcome': 'rejected', ...(hint ? { 'retcd-leader-endpoint': hint, 'retcd-leader-node-id': 2 } : {}) });
const refused = () => grpcErr(S.UNAVAILABLE, REFUSED);
const timeout = () => grpcErr(S.DEADLINE_EXCEEDED, 'Deadline exceeded');

// A client whose transport is `script(addr, method, req)`: return a response or throw a gRPC error.
function fake(script, opts = {}) {
  const client = new RetcdClient({ endpoints: [A, B, C], failoverMs: 3000, ...opts });
  const attempts = [];
  client._once = async (addr, method, req) => {
    attempts.push(`${addr} ${method}`);
    return script(addr, method, req, attempts.length);
  };
  return { client, attempts };
}
const rec = (key, value, create = 1, mod = 1) => ({ key: Buffer.from(key), value: Buffer.from(value), create_revision: String(create), mod_revision: String(mod) });

test('a follower names the leader: follow it, and stick to it', async () => {
  const { client, attempts } = fake((addr, m) => {
    if (addr === A) throw notLeader(B);
    return { outcome: 'APPLIED', revision: '5' };
  });
  assert.deepEqual(await client.put('k', 'v'), { revision: 5 });
  assert.deepEqual(attempts, [`${A} Put`, `${B} Put`]);
  assert.equal(client.endpoint, B);
  await client.put('k', 'v2');
  assert.equal(attempts.at(-1), `${B} Put`, 'next call goes straight to the leader');
});

test('a leader we were never given is added to the node list', async () => {
  const { client } = fake((addr) => {
    if (addr === A) throw notLeader('10.9.9.9:1');
    return { outcome: 'APPLIED', revision: '1' };
  });
  await client.put('k', 'v');
  assert.equal(client.endpoint, '10.9.9.9:1');
});

test('a refused connection moves on to the next node, for a write too', async () => {
  const { client, attempts } = fake((addr) => {
    if (addr === A) throw refused();
    return { outcome: 'APPLIED', revision: '2' };
  });
  assert.deepEqual(await client.put('k', 'v'), { revision: 2 });
  assert.deepEqual(attempts, [`${A} Put`, `${B} Put`]);
});

test('no leader yet (server rejected it): wait and try again', async () => {
  let n = 0;
  const { client, attempts } = fake(() => {
    if (++n < 3) throw grpcErr(S.UNAVAILABLE, 'no leader', { 'retcd-outcome': 'rejected' });
    return { outcome: 'APPLIED', revision: '3' };
  });
  assert.deepEqual(await client.put('k', 'v'), { revision: 3 });
  assert.equal(attempts.length, 3);
});

test('a write that timed out is NEVER retried: UnknownOutcomeError after one attempt', async () => {
  const { client, attempts } = fake(() => {
    throw timeout();
  });
  await assert.rejects(client.put('k', 'v'), UnknownOutcomeError);
  await assert.rejects(client.delete('k'), UnknownOutcomeError);
  assert.equal(attempts.length, 2, 'one attempt per call, no resend');
});

test('a write whose connection dropped after sending is also unknown, and not resent', async () => {
  const { client, attempts } = fake(() => {
    throw grpcErr(S.UNAVAILABLE, 'Connection dropped');
  });
  await assert.rejects(client.put('k', 'v'), UnknownOutcomeError);
  assert.equal(attempts.length, 1);
});

test('a read that timed out is retried on another node', async () => {
  const { client, attempts } = fake((addr) => {
    if (addr === A) throw timeout();
    return { record: rec('k', 'v', 3, 4), read_revision: '9' };
  });
  const got = await client.get('k');
  assert.deepEqual(attempts, [`${A} Get`, `${B} Get`]);
  assert.equal(got.value.toString(), 'v');
  assert.equal(got.createRevision, 3);
  assert.equal(got.modRevision, 4);
});

test('when no node answers in time: UnavailableError (and a write says nothing was written)', async () => {
  const { client } = fake(() => {
    throw refused();
  }, { failoverMs: 400 });
  await assert.rejects(client.put('k', 'v'), (e) => e instanceof UnavailableError && /Nothing was written/.test(e.message));
  await assert.rejects(client.get('k'), UnavailableError);
});

test('get returns null for a missing key', async () => {
  const { client } = fake(() => ({ record: null, read_revision: '1' }));
  assert.equal(await client.get('nope'), null);
});

test('CAS: a conflict in an OK reply and a conflict trailer both give CasConflictError', async () => {
  const a = fake(() => ({ outcome: 'CONFLICT', current_mod_revision: '8', exists: true }));
  await assert.rejects(a.client.put('k', 'v', { ifRevision: 3 }), (e) => e instanceof CasConflictError && e.currentRevision === 8 && e.exists === true);
  const b = fake(() => {
    throw grpcErr(S.FAILED_PRECONDITION, 'conflict', { 'retcd-outcome': 'rejected', 'retcd-conflict-mod-revision': '0', 'retcd-conflict-exists': 'false' });
  });
  await assert.rejects(b.client.delete('k', { ifRevision: 3 }), (e) => e instanceof CasConflictError && e.exists === false);
  assert.equal(b.attempts.length, 1, 'a conflict is final, not retried');
});

test('ifRevision goes out as expected_mod_revision; 0 is sent, not dropped', async () => {
  let seen;
  const { client } = fake((a, m, req) => {
    seen = req;
    return { outcome: 'APPLIED', revision: '1' };
  });
  await client.put('k', 'v', { ifRevision: 0 });
  assert.equal(seen.expected_mod_revision, '0');
  await client.put('k', 'v');
  assert.equal('expected_mod_revision' in seen, false);
  await assert.rejects(client.put('k', 'v', { ifRevision: -1 }), RangeError);
});

test('delete of a missing key throws NotFoundError', async () => {
  const { client } = fake(() => ({ outcome: 'NOT_FOUND' }));
  await assert.rejects(client.delete('k'), NotFoundError);
});

test('limits are checked before anything is sent', async () => {
  const { client, attempts } = fake(() => ({ outcome: 'APPLIED', revision: '1' }));
  await assert.rejects(client.put('k'.repeat(1025), 'v'), TooLargeError);
  await assert.rejects(client.put('k', Buffer.alloc(1024 * 1024 + 1)), TooLargeError);
  await assert.rejects(client.put('', 'v'), { code: 'INVALID_ARGUMENT' });
  await assert.rejects(client.put('k', 42), TypeError);
  assert.equal(attempts.length, 0);
  await client.put('k'.repeat(1024), Buffer.alloc(1024 * 1024)); // exactly at the limits: allowed
  assert.equal(attempts.length, 1);
});

test('list walks every page with the cursor, filters globs, and exposes readRevision', async () => {
  const pages = [
    { records: [rec('d/a.md', '1'), rec('d/a.txt', '2')], read_revision: '50', truncated: false, next_page_token: Buffer.from('T1') },
    { records: [rec('d/x/b.md', '3'), rec('d/c.md', '4')], read_revision: '50', truncated: false, next_page_token: Buffer.alloc(0) },
  ];
  const reqs = [];
  const { client } = fake((a, m, req) => {
    reqs.push(req);
    return pages[reqs.length - 1];
  });
  const it = client.list('d/*.md', { pageSize: 2 });
  assert.equal(it.readRevision, undefined);
  const keys = [];
  for await (const r of it) keys.push(r.key);
  assert.deepEqual(keys, ['d/a.md', 'd/c.md']); // d/x/b.md is deeper than *
  assert.equal(it.readRevision, 50);
  assert.equal(reqs[0].prefix.toString(), 'd/'); // only the literal part goes to the server
  assert.equal(reqs[0].max_items, 2);
  assert.equal(reqs[0].page_token.length, 0, 'first call starts a pinned walk with an empty token');
  assert.equal(reqs[1].page_token.toString(), 'T1');
});

test('list refuses to end silently when the server truncated without a cursor', async () => {
  const { client } = fake(() => ({ records: [rec('a', '1')], read_revision: '1', truncated: true, next_page_token: Buffer.alloc(0) }));
  await assert.rejects(async () => {
    for await (const r of client.list('a')) void r;
  }, { code: 'LIST_TRUNCATED' });
});

test('listDirs gives files plus {dir, count} for deeper folders', async () => {
  const { client } = fake(() => ({
    records: [rec('docs/a.md', '1'), rec('docs/sub/b.md', '2'), rec('docs/sub/c.md', '3'), rec('docs/z/d/e.md', '4')],
    read_revision: '1',
    truncated: false,
  }));
  const view = await client.listDirs('docs');
  assert.deepEqual(
    view.map((e) => (e.dir ? { dir: e.dir, count: e.count } : e.key)),
    ['docs/a.md', { dir: 'docs/sub/', count: 2 }, { dir: 'docs/z/', count: 1 }],
  );
});

// ---- watch ---------------------------------------------------------------------------------
const putMsg = (rev, key, value = 'v') => ({ body: 'event', event: { revision: String(rev), key: Buffer.from(key), change: 'put', put: rec(key, value, rev, rev) } });
const delMsg = (rev, key) => ({ body: 'event', event: { revision: String(rev), key: Buffer.from(key), change: 'delete', delete: {} } });
const progMsg = (rev) => ({ body: 'progress', progress: { revision: String(rev) } });

// A fake Watch stream: emits `items`, then ends (or fails with `error`). Records the request.
function stream(items, error) {
  const s = Readable.from(
    (async function* gen() {
      for (const i of items) yield i;
      if (error) throw error;
    })(),
    { objectMode: true },
  );
  s.cancel = () => s.destroy(grpcErr(S.CANCELLED, 'Cancelled on client'));
  return s;
}

test('watch resumes after the last revision it saw, across a dropped connection and a leader change', async () => {
  const opened = [];
  const plan = [
    () => stream([putMsg(5, 'w/a'), putMsg(6, 'w/b')], grpcErr(S.UNAVAILABLE, 'Connection dropped')),
    () => stream([putMsg(7, 'w/c')], notLeader(C)), // a follower: go to the leader it names
    () => stream([delMsg(8, 'w/c'), putMsg(9, 'w/d')]), // then stays open
  ];
  const { client } = fake(() => ({ read_revision: '4' }));
  client._svc = (addr) => ({
    Watch: (req) => {
      opened.push({ addr, after: req.start_after_revision, prefix: req.prefix.toString() });
      return plan[opened.length - 1]();
    },
  });
  const ac = new AbortController();
  const got = [];
  for await (const ev of client.watch('w/', { fromRevision: 4, signal: ac.signal })) {
    got.push(`${ev.type}:${ev.key}@${ev.revision}`);
    if (ev.revision === 9) ac.abort();
  }
  assert.deepEqual(got, ['put:w/a@5', 'put:w/b@6', 'put:w/c@7', 'delete:w/c@8', 'put:w/d@9']);
  assert.deepEqual(opened.map((o) => o.after), ['4', '6', '7'], 'each reconnect resumes after the last revision, so nothing repeats or is skipped');
  assert.equal(opened[2].addr, C, 'followed the leader hint');
  assert.equal(opened[0].prefix, 'w/');
});

test('watch with a glob drops non-matching events but still advances the cursor; progress is optional', async () => {
  const opened = [];
  const plan = [
    () => stream([putMsg(5, 'w/a.md'), putMsg(6, 'w/skip.txt'), progMsg(7)], grpcErr(S.UNAVAILABLE, 'Connection dropped')),
    () => stream([putMsg(8, 'w/b.md')]),
  ];
  const { client } = fake(() => ({ read_revision: '4' }));
  client._svc = () => ({
    Watch: (req) => {
      opened.push(req.start_after_revision);
      return plan[opened.length - 1]();
    },
  });
  const got = [];
  for await (const ev of client.watch('w/*.md', { fromRevision: 4, progress: true })) {
    got.push(`${ev.type}@${ev.revision}`);
    if (ev.revision === 8) break; // breaking out of the loop closes the watch
  }
  assert.deepEqual(got, ['put@5', 'progress@7', 'put@8']);
  assert.deepEqual(opened, ['4', '7']);
});

test('watch: a typed refusal (history compacted) is thrown, not retried', async () => {
  const { client } = fake(() => ({ read_revision: '4' }));
  let opens = 0;
  client._svc = () => ({
    Watch: () => {
      opens++;
      return stream([], grpcErr(S.OUT_OF_RANGE, 'compacted', { 'retcd-outcome': 'rejected', 'retcd-min-revision': 100 }));
    },
  });
  await assert.rejects(async () => {
    for await (const ev of client.watch('w/', { fromRevision: 1 })) void ev;
  }, { code: 'COMPACTED', minRevision: 100 });
  assert.equal(opens, 1);
});

test('watch: the 100-streams cap is a clear error, a resumable queue overflow reconnects', async () => {
  const { client } = fake(() => ({ read_revision: '4' }));
  client._svc = () => ({ Watch: () => stream([], grpcErr(S.RESOURCE_EXHAUSTED, 'too many watches', { 'retcd-outcome': 'rejected', 'retcd-resumable': 'false' })) });
  await assert.rejects(async () => {
    for await (const ev of client.watch('w/', { fromRevision: 1 })) void ev;
  }, { code: 'WATCH_LIMIT' });

  let n = 0;
  client._svc = () => ({
    Watch: () => (++n === 1 ? stream([], grpcErr(S.RESOURCE_EXHAUSTED, 'lagging', { 'retcd-outcome': 'rejected', 'retcd-resumable': 'true' })) : stream([putMsg(2, 'w/a')])),
  });
  for await (const ev of client.watch('w/', { fromRevision: 1 })) {
    assert.equal(ev.revision, 2);
    break;
  }
  assert.equal(n, 2);
});

test('watch gives up with UnavailableError when it cannot reconnect for giveUpAfterMs', async () => {
  const { client } = fake(() => ({ read_revision: '4' }));
  client._svc = () => ({ Watch: () => stream([], refused()) });
  await assert.rejects(async () => {
    for await (const ev of client.watch('w/', { fromRevision: 1, giveUpAfterMs: 300 })) void ev;
  }, UnavailableError);
});

test('close() ends a running watch and later calls fail clearly', async () => {
  const { client } = fake(() => ({ read_revision: '4' }));
  client._svc = () => ({ Watch: () => new Readable({ objectMode: true, read() {} }) });
  const s = client._svc();
  let cancelled = 0;
  client._svc = () => ({
    Watch: () => {
      const r = new Readable({ objectMode: true, read() {} });
      r.cancel = () => {
        cancelled++;
        r.destroy(grpcErr(S.CANCELLED, 'Cancelled'));
      };
      return r;
    },
  });
  void s;
  const done = (async () => {
    for await (const ev of client.watch('w/', { fromRevision: 1 })) void ev;
  })();
  await new Promise((r) => setTimeout(r, 50));
  client.close();
  await done; // ends quietly
  assert.ok(cancelled >= 1);
  await assert.rejects(client.get('k'), { code: 'CLOSED' });
});

test('connect() fails loudly when nothing is listening', async () => {
  await assert.rejects(RetcdClient.connect({ endpoints: ['127.0.0.1:1'], failoverMs: 700 }), UnavailableError);
});

test('connect() needs endpoints, and a missing proto file says how to fix it', () => {
  assert.throws(() => new RetcdClient({ endpoints: [] }), { code: 'INVALID_ARGUMENT' });
  assert.throws(() => new RetcdClient({ endpoints: [A], protoPath: '/no/such/config.proto' }), { code: 'PROTO_NOT_FOUND', message: /RETCD_PROTO/ });
});
