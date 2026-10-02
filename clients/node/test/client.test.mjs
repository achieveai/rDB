// Client logic with a fake transport: leader following, retry rules, limits, paging, watch resume.
// No cluster needed. (The real thing is test/live.test.mjs.)
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { test } from 'node:test';
import grpc from '@grpc/grpc-js';
import {
  CasConflictError, LIMITS, ResultTooLargeError, RetcdClient, RetcdError, TooLargeError, UnavailableError, UnknownOutcomeError,
} from '../src/index.mjs';

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
  client._ready = async () => {}; // every node connects; watch() asks for this before it opens a stream
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
    if (addr === A) throw notConnected(addr); // only the connect step's stamp makes a write safe to resend
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
  const { client } = fake((addr) => {
    throw notConnected(addr);
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

// The connect step and the send step stubbed separately: `ready(addr, ms)` and `send(addr, method)`.
function staged({ ready, send }, opts = {}) {
  const client = new RetcdClient({ endpoints: [A, B, C], failoverMs: 3000, ...opts });
  const log = [];
  client._ready = async (addr, ms) => {
    log.push(`connect ${addr} ${ms}`);
    return ready(addr, ms);
  };
  client._send = async (addr, method) => {
    log.push(`send ${addr} ${method}`);
    return send(addr, method);
  };
  return { client, log };
}
const notConnected = (addr) => Object.assign(grpcErr(S.UNAVAILABLE, `No connection established to ${addr}: not connected within 400 ms`), { connectFailed: true });

test('a dead first node costs one quick connect window, and a write goes to the next node unsent', async () => {
  const { client, log } = staged({
    ready: (addr) => {
      if (addr === A) throw notConnected(addr);
    },
    send: () => ({ outcome: 'APPLIED', revision: '4' }),
  });
  assert.deepEqual(await client.put('k', 'v'), { revision: 4 });
  assert.deepEqual(log, [`connect ${A} 400`, `connect ${B} 400`, `send ${B} Put`], 'nothing is sent to a node that never connected');
  assert.equal(client.endpoint, B);
});

test('once every node misses the quick window, the call waits longer for each', async () => {
  // failoverMs above the windows: a connect window is also clipped to what is left of the call's budget.
  const { client, log } = staged({
    ready: (addr, ms) => {
      if (ms < 3000) throw notConnected(addr);
    },
    send: () => ({ record: null, read_revision: '1' }),
  }, { failoverMs: 20_000 });
  assert.equal(await client.get('k'), null);
  assert.deepEqual(log, [`connect ${A} 400`, `connect ${B} 400`, `connect ${C} 400`, `connect ${A} 3000`, `send ${A} Get`]);
});

test('a write that was sent and then timed out is not resent, even after a quick connect', async () => {
  const { client, log } = staged({ ready: () => {}, send: () => Promise.reject(timeout()) });
  await assert.rejects(client.put('k', 'v'), UnknownOutcomeError);
  assert.deepEqual(log, [`connect ${A} 400`, `send ${A} Put`]);
});

test('delete resolves true when it deleted the key and false when the key was missing', async () => {
  const gone = fake(() => ({ outcome: 'APPLIED', revision: '7' }));
  assert.equal(await gone.client.delete('k'), true);
  const missing = fake(() => ({ outcome: 'NOT_FOUND' }));
  assert.equal(await missing.client.delete('k'), false);
  assert.equal(missing.attempts.length, 1, 'a missing key is an answer, not a retry');
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

const cursorRefused = (reason, hint) =>
  grpcErr(S.FAILED_PRECONDITION, `page token refused: ${reason}`, { 'retcd-outcome': 'rejected', 'retcd-reason': reason, ...(hint ? { 'retcd-leader-endpoint': hint } : {}) });

test('list: a cursor refused as minted by another node is resent, same token, to the node the refusal names', async () => {
  const sent = [];
  const { client, attempts } = fake((addr, m, req) => {
    sent.push(`${addr} ${req.page_token.toString() || '(start)'}`);
    if (req.page_token.length === 0) return { records: [rec('p/a', '1')], read_revision: '9', truncated: false, next_page_token: Buffer.from('T1') };
    if (addr === A) throw cursorRefused('node', B);
    return { records: [rec('p/b', '2')], read_revision: '9', truncated: false, next_page_token: Buffer.alloc(0) };
  });
  const keys = [];
  for await (const r of client.list('p/')) keys.push(r.key);
  assert.deepEqual(keys, ['p/a', 'p/b'], 'the walk finishes, nothing repeated or lost');
  assert.deepEqual(sent, [`${A} (start)`, `${A} T1`, `${B} T1`], 'page 2 goes to B with the same cursor');
  assert.equal(attempts.length, 3);
  assert.equal(client.endpoint, B);
});

test('list: any other cursor refusal is final, hint or not, and so is "node" with no hint', async () => {
  for (const [reason, hint] of [['expired', B], ['evicted', B], ['node', undefined]]) {
    const { client, attempts } = fake((addr, m, req) => {
      if (req.page_token.length === 0) return { records: [rec('p/a', '1')], read_revision: '9', truncated: false, next_page_token: Buffer.from('T1') };
      throw cursorRefused(reason, hint);
    });
    await assert.rejects(async () => {
      for await (const r of client.list('p/')) void r;
    }, { code: 'PAGE_TOKEN', reason }, `${reason} with hint ${hint}`);
    assert.equal(attempts.length, 2, `${reason}: not resent`);
  }
});

test('a "node" cursor refusal outside List is not followed', async () => {
  const { client, attempts } = fake(() => {
    throw cursorRefused('node', B);
  });
  await assert.rejects(client.get('k'), { code: 'PAGE_TOKEN' });
  assert.equal(attempts.length, 1);
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

test('watch with fromRevision as the first call: a dead first node costs one quick window, and no stream is opened to it', async () => {
  const client = new RetcdClient({ endpoints: [A, B, C] });
  const log = [];
  client._ready = async (addr, ms) => {
    log.push(`connect ${addr} ${ms}`);
    if (addr === A) throw notConnected(addr);
  };
  client._svc = (addr) => ({
    Watch: (req) => {
      log.push(`watch ${addr} after ${req.start_after_revision}`);
      return stream([putMsg(8, 'w/a')]);
    },
  });
  for await (const ev of client.watch('w/', { fromRevision: 7 })) {
    assert.equal(ev.revision, 8);
    break;
  }
  assert.deepEqual(log, [`connect ${A} 400`, `connect ${B} 400`, `watch ${B} after 7`]);
  client.close();
});

test('watch: once every node misses the quick window it waits longer, and an abort ends a connect wait at once', async () => {
  const client = new RetcdClient({ endpoints: [A, B] });
  const windows = [];
  const ac = new AbortController();
  client._ready = (addr, ms) => {
    windows.push(ms);
    if (ms < 3000) return Promise.reject(notConnected(addr));
    return new Promise(() => {}); // the slow window never answers
  };
  client._svc = () => ({ Watch: () => assert.fail('no stream before a connection') });
  const started = Date.now();
  const done = (async () => {
    for await (const ev of client.watch('w/', { fromRevision: 1, signal: ac.signal })) void ev;
  })();
  await new Promise((r) => setTimeout(r, 50));
  ac.abort();
  await done; // ends quietly
  assert.deepEqual(windows, [400, 400, 3000]);
  assert.ok(Date.now() - started < 1000, 'the abort did not wait out the 3 s window');
  client.close();
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

// ---- PR #1 review rows -----------------------------------------------------------------------

test('R1-F001: refusal text from the send step is not proof nothing was sent: one send, UnknownOutcomeError', async () => {
  const { client, log } = staged({ ready: () => {}, send: () => Promise.reject(refused()) });
  await assert.rejects(client.put('k', 'v'), (e) => e instanceof UnknownOutcomeError && !/Nothing was written/.test(e.message));
  assert.deepEqual(log, [`connect ${A} 400`, `send ${A} Put`], 'exactly one send, no failover');
  // A real connect failure (stamped by _ready) still fails over, for a write too.
  const real = staged({
    ready: (addr) => {
      if (addr === A) throw notConnected(addr);
    },
    send: () => ({ outcome: 'APPLIED', revision: '6' }),
  });
  assert.deepEqual(await real.client.put('k', 'v'), { revision: 6 });
  assert.deepEqual(real.log, [`connect ${A} 400`, `connect ${B} 400`, `send ${B} Put`]);
  // A read with the same refusal text still moves on.
  const read = staged({ ready: () => {}, send: (addr) => (addr === A ? Promise.reject(refused()) : { record: null, read_revision: '1' }) });
  assert.equal(await read.client.get('k'), null);
  assert.deepEqual(read.log, [`connect ${A} 400`, `send ${A} Get`, `connect ${B} 400`, `send ${B} Get`]);
});

test('R1-F004: an https:// (or any non-http) endpoint is refused at construction, before any channel', async () => {
  const realInsecure = grpc.credentials.createInsecure;
  let insecure = 0;
  grpc.credentials.createInsecure = (...a) => {
    insecure++;
    return realInsecure(...a);
  };
  try {
    for (const ep of ['https://127.0.0.1:1', 'HTTPS://127.0.0.1:1', 'grpcs://127.0.0.1:1', 'dns:///127.0.0.1:1']) {
      assert.throws(() => new RetcdClient({ endpoints: [ep] }), { code: 'INVALID_ARGUMENT', message: /not supported/ }, ep);
      await assert.rejects(RetcdClient.connect({ endpoints: [A, ep], failoverMs: 300 }), { code: 'INVALID_ARGUMENT' }, ep);
    }
    assert.equal(insecure, 0, 'no plaintext channel was opened');
  } finally {
    grpc.credentials.createInsecure = realInsecure;
  }
  // No scheme and http:// are both plaintext by name, and still accepted.
  const ok = new RetcdClient({ endpoints: ['http://10.0.0.9:1/', ' 10.0.0.8:1 '] });
  assert.equal(ok.endpoint, '10.0.0.9:1');
});

test('R2-F004: a server-stamped write DEADLINE_EXCEEDED is an unknown outcome, never "Nothing was written", never resent', async () => {
  const { client, attempts } = fake(() => {
    throw grpcErr(S.DEADLINE_EXCEEDED, 'write timed out after submission', { 'retcd-outcome': 'rejected' });
  });
  await assert.rejects(client.put('k', 'v'), (e) => e instanceof UnknownOutcomeError && !/Nothing was written/.test(e.message));
  await assert.rejects(client.delete('k'), UnknownOutcomeError);
  assert.equal(attempts.length, 2, 'one attempt per call');
});

// A node that answers `first` once (if given), then holds every call until the gRPC deadline the client set.
function stallingNode(client, first) {
  const deadlines = [];
  let calls = 0;
  client._ready = async () => {};
  client._svc = () =>
    new Proxy(
      {},
      {
        get: () => (req, opts, cb) => {
          deadlines.push(Number(opts.deadline) - Date.now());
          if (calls++ === 0 && first) return setImmediate(() => cb(first()));
          setTimeout(() => cb(grpcErr(S.DEADLINE_EXCEEDED, 'Deadline exceeded')), Math.max(0, Number(opts.deadline) - Date.now()));
        },
      },
    );
  return deadlines;
}

test('R2-F005: a stalled attempt cannot outlive failoverMs, even after an earlier retryable failure', async () => {
  const noLeader = () => grpcErr(S.UNAVAILABLE, 'no leader', { 'retcd-outcome': 'rejected' });
  for (const [what, call, Err] of [
    ['write', (c) => c.put('k', 'v'), UnknownOutcomeError],
    ['read', (c) => c.get('k'), UnavailableError],
  ]) {
    const client = new RetcdClient({ endpoints: [A, B], timeoutMs: 10_000, failoverMs: 600 });
    const deadlines = stallingNode(client, noLeader);
    const started = Date.now();
    await assert.rejects(call(client), Err, what);
    const took = Date.now() - started;
    assert.ok(took < 1200, `${what}: took ${took} ms against a 600 ms budget`);
    assert.ok(deadlines.length >= 2, `${what}: retried after the first refusal`);
    assert.ok(deadlines.every((d) => d <= 600), `${what}: every attempt deadline fits the budget: ${deadlines}`);
  }
});

// An in-memory node keyed by exact bytes: Get, Put, Delete, List (paged).
function memoryNode() {
  const kv = new Map(); // hex(key) -> record
  let rev = 0;
  const sorted = () => [...kv.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([, r]) => r);
  return {
    kv,
    script: (addr, method, req) => {
      const hex = req.key?.toString('hex');
      if (method === 'Get') return { record: kv.get(hex) ?? null, read_revision: String(rev) };
      if (method === 'Put') {
        rev++;
        const old = kv.get(hex);
        kv.set(hex, { key: Buffer.from(req.key), value: Buffer.from(req.value), create_revision: old?.create_revision ?? String(rev), mod_revision: String(rev) });
        return { outcome: 'APPLIED', revision: String(rev) };
      }
      if (method === 'Delete') {
        if (!kv.delete(hex)) return { outcome: 'NOT_FOUND' };
        return { outcome: 'APPLIED', revision: String(++rev) };
      }
      if (method === 'List') {
        const after = req.page_token.length ? req.page_token.toString('hex') : '';
        const all = sorted().filter((r) => r.key.subarray(0, req.prefix.length).equals(req.prefix) && r.key.toString('hex') > after);
        const page = all.slice(0, req.max_items);
        const more = all.length > page.length;
        return { records: page, read_revision: String(rev), truncated: false, next_page_token: more ? page.at(-1).key : Buffer.alloc(0) };
      }
      throw new Error(`unexpected ${method}`);
    },
  };
}

test('R2-F006: two non-UTF-8 keys stay distinct: keyBytes round-trips into get, put, delete and list', async () => {
  const node = memoryNode();
  const { client } = fake(node.script);
  await client.put(Buffer.from([0x80]), 'eighty');
  await client.put(Buffer.from([0x81]), 'eighty-one');
  const under80 = [];
  for await (const r of client.list(Buffer.from([0x80]))) under80.push(r);
  assert.deepEqual(under80.map((r) => [...r.keyBytes]), [[0x80]], 'a byte prefix lists only its own keys');
  const all = [];
  for await (const r of client.list('')) all.push(r);
  assert.equal(all.length, 2);
  assert.equal(all[0].key, all[1].key, 'the string form is lossy: both are U+FFFD');
  assert.deepEqual(all.map((r) => [...r.keyBytes]), [[0x80], [0x81]], 'keyBytes are exact and distinct');
  for (const r of all) assert.ok(Buffer.isBuffer(r.keyBytes));
  // Each record is addressed again by its returned bytes.
  assert.equal((await client.get(all[0].keyBytes)).value.toString(), 'eighty');
  assert.equal((await client.get(all[1].keyBytes)).value.toString(), 'eighty-one');
  assert.deepEqual([...(await client.get(all[1].keyBytes)).keyBytes], [0x81]);
  await client.put(all[0].keyBytes, 'changed', { ifRevision: all[0].modRevision });
  assert.equal(await client.delete(all[1].keyBytes), true);
  assert.deepEqual([...node.kv.keys()], ['80'], 'the delete hit 0x81 only, and nothing landed at EF BF BD');
  assert.equal(node.kv.get('80').value.toString(), 'changed');
});

test('R2-F006: watch events carry keyBytes', async () => {
  const { client } = fake(() => ({ read_revision: '4' }));
  client._svc = () => ({ Watch: () => stream([putMsg(5, 'w/a'), { body: 'event', event: { revision: '6', key: Buffer.from([0x81]), change: 'delete', delete: {} } }]) });
  const got = [];
  for await (const ev of client.watch('', { fromRevision: 4 })) {
    got.push(ev);
    if (got.length === 2) break;
  }
  assert.deepEqual(got.map((e) => [...e.keyBytes]), [[...Buffer.from('w/a')], [0x81]]);
});

test('R2-F014: listDirs stops at its byte limit across pages and points at list(); a small folder is unchanged', async () => {
  const MiB = Buffer.alloc(1024 * 1024); // one buffer shared by every record: the fake itself stays small
  let pages = 0;
  const { client } = fake((a, m, req) => {
    if (++pages > 20) throw new Error('walked far past the limit');
    const start = req.page_token.length ? Number(req.page_token.toString()) : 0;
    const records = Array.from({ length: 10 }, (_, i) => ({ key: Buffer.from(`big/f${String(start + i).padStart(4, '0')}`), value: MiB, create_revision: '1', mod_revision: '1' }));
    return { records, read_revision: '1', truncated: false, next_page_token: Buffer.from(String(start + 10)) }; // never ends by itself
  });
  await assert.rejects(client.listDirs('big', { pageSize: 10 }), (e) => e.code === 'RESULT_TOO_LARGE' && e.limit === LIMITS.maxListDirsBytes && /list\(\)/.test(e.message));
  assert.equal(pages, 7, 'stopped on the page that crossed 64 MiB, not after the walk');
  assert.equal(LIMITS.maxListDirsBytes, 64 * 1024 * 1024);
});

// ---- Round 1: pauses inside the budget, and the rows the tester's surviving mutants showed were missing ----

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const noLeaderYet = () => grpcErr(S.UNAVAILABLE, 'no leader', { 'retcd-outcome': 'rejected' });

test('R2-F005: a pause that would end past failoverMs ends the call at once; it never returns late', async () => {
  // No leader yet: the 300 ms pause does not fit a 100 ms budget.
  const one = new RetcdClient({ endpoints: [A], timeoutMs: 10_000, failoverMs: 100 });
  let sends = 0;
  one._ready = async () => {};
  one._send = async () => {
    sends++;
    throw noLeaderYet();
  };
  let started = Date.now();
  await assert.rejects(one.put('k', 'v'), UnavailableError);
  let took = Date.now() - started;
  assert.equal(sends, 1, 'no attempt after the budget');
  assert.ok(took < 100, `no-leader pause: took ${took} ms against a 100 ms budget`);
  // A hint loop: the third hop's 200 ms pause does not fit either.
  const loop = new RetcdClient({ endpoints: [A, B], timeoutMs: 10_000, failoverMs: 100 });
  const sentTo = [];
  loop._ready = async () => {};
  loop._send = async (addr) => {
    sentTo.push(addr);
    throw notLeader(addr === A ? B : A);
  };
  started = Date.now();
  await assert.rejects(loop.put('k', 'v'), UnavailableError);
  took = Date.now() - started;
  assert.deepEqual(sentTo, [A, B, A], 'two free hops, then the paused one is never made');
  assert.ok(took < 100, `hint-loop pause: took ${took} ms against a 100 ms budget`);
});

test('R2-F005: no attempt starts after failoverMs, even when a pause wakes late', async () => {
  const client = new RetcdClient({ endpoints: [A], timeoutMs: 10_000, failoverMs: 400 });
  let sends = 0;
  client._ready = async () => {};
  client._send = async () => {
    sends++;
    throw noLeaderYet();
  };
  const started = Date.now();
  // The 300 ms pause fits the 400 ms budget, but the event loop is held from 100 ms to 500 ms, so it wakes late.
  setTimeout(() => {
    while (Date.now() - started < 500);
  }, 100);
  await assert.rejects(client.put('k', 'v'), UnavailableError);
  assert.equal(sends, 1, 'the late wake finds the budget spent and makes no second attempt');
});

test('R2-F005: the slow connect window is cut to the time left in the budget', async () => {
  const client = new RetcdClient({ endpoints: [A], timeoutMs: 10_000, failoverMs: 700 });
  const windows = [];
  client._ready = async (addr, ms) => {
    windows.push(ms);
    await sleep(Math.max(0, ms));
    throw notConnected(addr);
  };
  client._send = async () => {
    throw new Error('never sent');
  };
  const started = Date.now();
  await assert.rejects(client.put('k', 'v'), UnavailableError);
  const took = Date.now() - started;
  assert.equal(windows[0], 400, 'the quick window first');
  assert.ok(windows.length === 2 && windows[1] <= 300, `then what is left of 700 ms, not 3000: ${windows}`);
  assert.ok(took < 1000, `took ${took} ms against a 700 ms budget`);
});

test('R2-F014: the listDirs limit counts key, value and folder-path bytes, to the byte', async () => {
  const L = LIMITS.maxListDirsBytes;
  const page = (...records) => fake(() => ({ records, read_revision: '1', truncated: false, next_page_token: Buffer.alloc(0) })).client;
  const file = (key, size) => ({ key: Buffer.from(key), value: Buffer.alloc(size), create_revision: '1', mod_revision: '1' });
  const tooLarge = (e) => e instanceof ResultTooLargeError && e instanceof RetcdError && e.code === 'RESULT_TOO_LARGE';
  // 'd/k' is 3 bytes, so a value of L - 3 is exactly the limit.
  assert.equal((await page(file('d/k', L - 3)).listDirs('d')).length, 1, 'exactly the limit is kept');
  await assert.rejects(page(file('d/k', L - 2)).listDirs('d'), tooLarge, 'one byte over: the key bytes count');
  // 'd/sub/z' is not a file of 'd/*', but it adds the folder row 'd/sub/' (6 bytes), which tips it over.
  await assert.rejects(page(file('d/k', L - 3), file('d/sub/z', 0)).listDirs('d'), tooLarge, 'folder paths count');
});

test('A-R2-1: ResultTooLargeError is a runtime export of the package entry point', async () => {
  // By package name, so this goes through package.json "exports" as a caller's import does.
  const entry = await import('@retcd/client');
  assert.equal(typeof entry.ResultTooLargeError, 'function', 'exported at run time, not only in index.d.ts');
  assert.equal(entry.ResultTooLargeError, ResultTooLargeError);
  assert.ok(new entry.ResultTooLargeError('x', { size: 2, limit: 1 }) instanceof entry.RetcdError);
});
