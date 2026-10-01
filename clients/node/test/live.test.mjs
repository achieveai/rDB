// Integration tests against a real cluster. They run only when RETCD_ENDPOINTS is set:
//   RETCD_ENDPOINTS=127.0.0.1:17502,127.0.0.1:17512,127.0.0.1:17522 npm test
// The node-crash test also needs RETCD_CLUSTER_DIR (the --dir of local-cluster.sh) and
// RETCD_SERVER_BIN (path to config-server[.exe]); without them that one test is skipped.
// Every test writes under livetest/<run id>/ and removes what it wrote.
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, test } from 'node:test';
import { CasConflictError, NotFoundError, RetcdClient, TooLargeError, UnknownOutcomeError, startHeartbeat, watchPresence } from '../src/index.mjs';
import { classify } from '../src/errors.mjs';
import { killNode, nodeForEndpoint, startNode, waitFor } from '../test-support/node-ctl.mjs';

const ENDPOINTS = (process.env.RETCD_ENDPOINTS ?? '').split(',').map((s) => s.trim()).filter(Boolean);
const CLUSTER_DIR = process.env.RETCD_CLUSTER_DIR;
const SERVER_BIN = process.env.RETCD_SERVER_BIN;
const RUN = randomBytes(4).toString('hex');
const P = (name) => `livetest/${RUN}/${name}/`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const collect = async (it) => {
  const out = [];
  for await (const x of it) out.push(x);
  return out;
};

// The whole suite may run longer than npm test's 30 s per-test limit; each test inside keeps that limit.
describe('live cluster', { skip: ENDPOINTS.length ? false : 'RETCD_ENDPOINTS is not set', timeout: 600_000 }, () => {
  /** @type {RetcdClient} */
  let c;
  const written = new Set();
  const put = async (key, value, opts) => {
    written.add(key);
    return c.put(key, value, opts);
  };

  before(async () => {
    c = await RetcdClient.connect({ endpoints: ENDPOINTS });
  });

  after(async () => {
    // Clean up everything this run wrote (best effort), then close.
    const left = await collect(c.list(`livetest/${RUN}/`)).catch(() => []);
    for (const r of left) written.add(r.key);
    for (const k of written) await c.delete(k).catch(() => {});
    await c.delete('files/live-test.bin').catch(() => {});
    c.close();
  });

  test('put then get: value, revisions; get of a missing key is null', async () => {
    const key = `${P('basic')}k`;
    const { revision } = await put(key, 'hello');
    assert.ok(Number.isInteger(revision) && revision > 0);
    const got = await c.get(key);
    assert.equal(got.key, key);
    assert.equal(got.value.toString(), 'hello');
    assert.equal(got.modRevision, revision);
    assert.equal(got.createRevision, revision);
    const second = await put(key, Buffer.from([0, 1, 2, 255]));
    const again = await c.get(key);
    assert.deepEqual([...again.value], [0, 1, 2, 255], 'binary values survive');
    assert.equal(again.createRevision, revision, 'create revision does not move');
    assert.equal(again.modRevision, second.revision);
    assert.equal(await c.get(`${P('basic')}missing`), null);
  });

  test('compare-and-set: match, stale, must-not-exist, delete guard, delete of a missing key', async () => {
    const key = `${P('cas')}k`;
    const first = await put(key, '1', { ifRevision: 0 }); // 0 = only if it does not exist
    await assert.rejects(put(key, 'x', { ifRevision: 0 }), (e) => e instanceof CasConflictError && e.exists === true && e.currentRevision === first.revision);
    const second = await put(key, '2', { ifRevision: first.revision });
    assert.ok(second.revision > first.revision);
    await assert.rejects(put(key, '3', { ifRevision: first.revision }), (e) => e instanceof CasConflictError && e.currentRevision === second.revision);
    assert.equal((await c.get(key)).value.toString(), '2', 'a refused write changed nothing');
    await assert.rejects(c.delete(key, { ifRevision: first.revision }), CasConflictError);
    assert.equal(await c.delete(key, { ifRevision: second.revision }), true);
    assert.equal(await c.get(key), null);
    assert.equal(await c.delete(key), false, 'a missing key resolves false, matching C# DeleteAsync');
    await assert.rejects(put(key, 'x', { ifRevision: 5 }), (e) => e instanceof CasConflictError && e.exists === false);
  });

  test('size limits: refused before sending, and the server refuses too', async () => {
    await assert.rejects(put('k'.repeat(1025), 'v'), TooLargeError);
    await assert.rejects(put(`${P('big')}k`, Buffer.alloc(1024 * 1024 + 1)), TooLargeError);
    // bypass the client-side check to prove the server's refusal maps to the same error
    await assert.rejects(c._call('Put', { key: Buffer.from(`${P('big')}raw`), value: Buffer.alloc(1024 * 1024 + 1) }, { write: true }), TooLargeError);
    await put(`${P('big')}max`, Buffer.alloc(1024 * 1024, 7)); // exactly 1 MiB is fine
    assert.equal((await c.get(`${P('big')}max`)).value.length, 1024 * 1024);
  });

  test('a write that times out is UnknownOutcomeError and is never sent again', async () => {
    const impatient = new RetcdClient({ endpoints: ENDPOINTS, timeoutMs: 1 });
    // Record real sends, not attempts: a connect that fails sent nothing. A follower may also
    // answer first with not-leader, which wrote nothing either. So the rule is not "one send";
    // it is "the timed-out send is the last one, and only refusals came before it".
    const sends = [];
    const real = impatient._send.bind(impatient);
    impatient._send = async (...args) => {
      try {
        const res = await real(...args);
        sends.push('ok');
        return res;
      } catch (err) {
        sends.push(classify(err));
        throw err;
      }
    };
    written.add(`${P('unknown')}k`);
    await assert.rejects(impatient.put(`${P('unknown')}k`, 'v'), UnknownOutcomeError);
    assert.equal(sends.at(-1), 'transport', `the last send timed out (sends: ${sends})`);
    assert.ok(sends.slice(0, -1).every((k) => k === 'not-leader'), `only refusals came before it (sends: ${sends})`);
    impatient.close();
  });

  test('list: pages through everything in key order, exactly once', async () => {
    const base = P('paging');
    const keys = Array.from({ length: 25 }, (_, i) => `${base}k${String(i).padStart(2, '0')}`);
    await Promise.all(keys.map((k) => put(k, k.slice(-3))));
    let pages = 0;
    const real = c._call.bind(c);
    c._call = (m, ...rest) => (m === 'List' ? (pages++, real(m, ...rest)) : real(m, ...rest));
    const it = c.list(base, { pageSize: 4 });
    const got = await collect(it);
    c._call = real;
    assert.deepEqual(got.map((r) => r.key), keys);
    assert.deepEqual(got.map((r) => r.value.toString()), keys.map((k) => k.slice(-3)));
    assert.equal(pages, 7, '25 keys at 4 per page');
    assert.ok(it.readRevision > 0);
    assert.deepEqual(await collect(c.list(`${base}nothing`)), []);
  });

  // 1030 keys plus ~9 MiB of values: 13 s on an idle host against a debug server, over 30 s when loaded.
  test('list: more keys than the server cap of 1000 per page; and a page cut by the 8 MiB byte cap continues', { timeout: 120_000 }, async () => {
    const base = P('many');
    const keys = Array.from({ length: 1030 }, (_, i) => `${base}${String(i).padStart(5, '0')}`);
    for (let i = 0; i < keys.length; i += 40) await Promise.all(keys.slice(i, i + 40).map((k) => put(k, 'x')));
    const got = await collect(c.list(base, { pageSize: 5000 })); // the server clamps to 1000
    assert.equal(got.length, 1030);
    assert.deepEqual(got.map((r) => r.key), keys);
    for (const k of keys) await c.delete(k); // free the space before the big-value part

    const bigBase = P('bytes');
    const mib = Buffer.alloc(1024 * 1024, 1);
    for (let i = 0; i < 10; i++) await put(`${bigBase}${i}`, mib);
    const bigs = await collect(c.list(bigBase));
    assert.equal(bigs.length, 10, '10 MiB does not fit one 8 MiB page, but nothing is lost');
  });

  test('glob: * ** ? [..] against real keys', async () => {
    const base = P('glob');
    const names = ['a.md', 'b.txt', 'x/c.md', 'x/y/d.md', 'x/y/e.txt', 'k1', 'k2', 'k9'];
    for (const n of names) await put(base + n, n);
    const ls = async (pattern) => (await collect(c.list(base + pattern))).map((r) => r.key.slice(base.length));
    assert.deepEqual(await ls('*.md'), ['a.md']);
    assert.deepEqual(await ls('x/*'), ['x/c.md']);
    assert.deepEqual(await ls('**/*.md'), ['a.md', 'x/c.md', 'x/y/d.md']);
    assert.deepEqual(await ls('x/**'), ['x/c.md', 'x/y/d.md', 'x/y/e.txt']);
    assert.deepEqual(await ls('k?'), ['k1', 'k2', 'k9']);
    assert.deepEqual(await ls('k[1-2]'), ['k1', 'k2']);
    assert.deepEqual(await ls('k[!1-2]'), ['k9']);
  });

  test('listDirs: files at this level plus {dir, count} for folders below', async () => {
    const base = P('dirs');
    for (const n of ['top.md', 'sub/a.md', 'sub/b.md', 'sub/deep/c.md', 'other/z.md']) await put(base + n, 'v');
    const view = await c.listDirs(base.slice(0, -1)); // no trailing slash needed
    const names = view.map((e) => (e.dir ? `${e.dir.slice(base.length)} (${e.count})` : e.key.slice(base.length)));
    assert.deepEqual([...names].sort(), ['other/ (1)', 'sub/ (3)', 'top.md']);
    assert.deepEqual(
      (await c.listDirs(`${base}sub/`)).map((e) => (e.dir ? `${e.dir.slice(base.length)} (${e.count})` : e.key.slice(base.length))).sort(),
      ['sub/a.md', 'sub/b.md', 'sub/deep/ (1)'],
    );
  });

  test('watch: sees puts and deletes in order; fromRevision replays; glob filters; abort ends it', async () => {
    const base = P('watch');
    const ac = new AbortController();
    const seen = [];
    const done = (async () => {
      for await (const ev of c.watch(`${base}*.md`, { signal: ac.signal })) seen.push(ev);
    })();
    await sleep(300); // let the stream open
    const a = await put(`${base}a.md`, 'one');
    await put(`${base}skip.txt`, 'not matched');
    const b = await put(`${base}a.md`, 'two');
    await c.delete(`${base}a.md`);
    await waitFor(() => seen.length >= 3, 10_000, 'three watch events');
    ac.abort();
    await done; // ends without throwing
    assert.deepEqual(seen.map((e) => [e.type, e.key.slice(base.length), e.value?.toString() ?? null]), [
      ['put', 'a.md', 'one'],
      ['put', 'a.md', 'two'],
      ['delete', 'a.md', null],
    ]);
    assert.equal(seen[0].revision, a.revision);
    assert.equal(seen[1].revision, b.revision);
    assert.ok(seen[2].revision > b.revision);

    // replay from an older revision, no live writes needed
    const replay = [];
    const ac2 = new AbortController();
    for await (const ev of c.watch(base, { fromRevision: a.revision - 1, signal: ac2.signal })) {
      replay.push(`${ev.type}:${ev.key.slice(base.length)}`);
      if (replay.length === 4) ac2.abort();
    }
    assert.deepEqual(replay, ['put:a.md', 'put:skip.txt', 'put:a.md', 'delete:a.md']);
  });

  test('list-then-watch: readRevision leaves no gap', async () => {
    const base = P('gap');
    await put(`${base}1`, 'a');
    const it = c.list(base);
    await collect(it);
    await put(`${base}2`, 'b'); // written after the list, before the watch
    const ac = new AbortController();
    const got = [];
    for await (const ev of c.watch(base, { fromRevision: it.readRevision, signal: ac.signal })) {
      got.push(ev.key.slice(base.length));
      ac.abort();
    }
    assert.deepEqual(got, ['2']);
  });

  test('putFile / getFile: meta record, sha256 check, refuses to overwrite, catches tampering', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'retcd-live-'));
    try {
      const src = path.join(dir, 'in.bin');
      const data = Buffer.from(Array.from({ length: 5000 }, (_, i) => i % 251));
      fs.writeFileSync(src, data);
      const key = `${P('file')}live-test.bin`;
      written.add(key);
      written.add(`meta/${key}`);
      const info = await c.putFile(src, key);
      assert.equal(info.size, 5000);
      assert.match(info.sha256, /^[0-9a-f]{64}$/);
      const meta = JSON.parse((await c.get(`meta/${key}`)).value.toString());
      assert.deepEqual(Object.keys(meta), ['size', 'sha256', 'stored_at_rev'], 'same meta format as kv.mjs');
      assert.equal(meta.sha256, info.sha256);
      assert.equal(meta.stored_at_rev, info.revision);

      const out = path.join(dir, 'out.bin');
      const res = await c.getFile(key, out);
      assert.equal(res.verified, true);
      assert.ok(fs.readFileSync(out).equals(data));
      await assert.rejects(c.getFile(key, out), { code: 'EXISTS' });
      await c.getFile(key, out, { overwrite: true });

      await put(key, 'tampered'); // bytes no longer match the meta record
      await assert.rejects(c.getFile(key, path.join(dir, 'bad.bin')), { code: 'INTEGRITY' });
      assert.equal(fs.existsSync(path.join(dir, 'bad.bin')), false, 'nothing written on a failed check');
      await assert.rejects(c.getFile(`${P('file')}nope`, path.join(dir, 'n.bin')), NotFoundError);
      await assert.rejects(c.putFile(path.join(dir, 'nope.bin')), { code: 'ENOENT' });
      fs.writeFileSync(path.join(dir, 'big.bin'), Buffer.alloc(1024 * 1024 + 1));
      await assert.rejects(c.putFile(path.join(dir, 'big.bin')), TooLargeError);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  test('a dead first endpoint costs well under a second, for a read and for a write', async () => {
    // Nothing listens on the first endpoint. A write is safe to send elsewhere: the dead node never got it.
    const srv = (await import('node:net')).createServer();
    await new Promise((r) => srv.listen(0, '127.0.0.1', r));
    const dead = `127.0.0.1:${srv.address().port}`;
    await new Promise((r) => srv.close(r));
    const endpoints = [dead, ...ENDPOINTS];
    let t0 = performance.now();
    const reader = await RetcdClient.connect({ endpoints });
    try {
      assert.equal(await reader.get(`${P('dead-first')}none`), null);
      assert.ok(performance.now() - t0 < 1500, `first read took ${Math.round(performance.now() - t0)} ms`);
    } finally {
      reader.close();
    }
    const writer = new RetcdClient({ endpoints });
    try {
      t0 = performance.now();
      await writer.put(`${P('dead-first')}k`, 'v');
      written.add(`${P('dead-first')}k`);
      assert.ok(performance.now() - t0 < 1500, `first write took ${Math.round(performance.now() - t0)} ms`);
      assert.equal((await c.get(`${P('dead-first')}k`)).value.toString(), 'v');
    } finally {
      writer.close();
    }
  });

  test('a watch with fromRevision as the first call skips an unreachable first endpoint in well under a second', async () => {
    // 10.255.255.1 is not routed here, so a connect neither succeeds nor is refused: it just hangs.
    // Without the quick connect window the watch waited for the transport's own connect timeout.
    const key = `${P('watch-first')}k`;
    const from = (await c.put(key, 'v')).revision - 1;
    written.add(key);
    const w = new RetcdClient({ endpoints: ['10.255.255.1:1', ...ENDPOINTS] });
    try {
      const t0 = performance.now();
      for await (const ev of w.watch(P('watch-first'), { fromRevision: from })) {
        assert.equal(ev.key, key);
        break;
      }
      const took = performance.now() - t0;
      assert.ok(took < 1500, `first event took ${Math.round(took)} ms`);
    } finally {
      w.close();
    }
  });

  test('health(): one entry per node, one leader; a dead address shows ok:false', async () => {
    const h = await c.health();
    assert.equal(h.length, ENDPOINTS.length);
    assert.ok(h.every((n) => n.ok && n.ready === true), JSON.stringify(h.map((n) => [n.endpoint, n.ok, n.ready, n.error])));
    assert.equal(h.filter((n) => n.role === 'leader').length, 1);
    assert.ok(Number.isInteger(h[0].revision));
    const probe = new RetcdClient({ endpoints: ['127.0.0.1:1'] });
    const [dead] = await probe.health();
    assert.equal(dead.ok, false);
    probe.close();
  });

  test('presence: heartbeat format in the store; monitor sees up, down when it stops, back when it returns', async () => {
    const name = `live-${RUN}`;
    const key = `presence/${name}`;
    written.add(key);
    // LATE fires after 1.5 intervals. At 200 ms, one put delayed by 100 ms on a loaded host broke
    // "steady beats stay quiet"; 500 ms leaves 250 ms.
    const intervalMs = 500;
    const mon = watchPresence(c, { intervalMs, missedBeats: 3 });
    const events = [];
    mon.on('change', (e) => e.name === name && events.push(e.state));
    await mon.ready;
    const states = () => events.join(',');

    let hb = startHeartbeat(c, name, { intervalMs });
    await waitFor(() => events.includes('up'), 5000, 'up');
    await sleep(2 * intervalMs);
    const stored = JSON.parse((await c.get(key)).value.toString());
    assert.deepEqual(Object.keys(stored), ['name', 'pid', 'seq', 'sent_at']);
    assert.equal(stored.name, name);
    assert.equal(stored.pid, process.pid);
    assert.ok(stored.seq >= 2);
    assert.ok(Math.abs(Date.parse(stored.sent_at) - Date.now()) < 5000);
    assert.equal(states(), 'up', 'steady beats stay quiet');

    await hb.stop(); // silence, no goodbye
    await waitFor(() => events.includes('down'), 8000, `down (saw ${states()})`);
    assert.deepEqual(events.filter((s) => s !== 'late'), ['up', 'down']);

    hb = startHeartbeat(c, name, { intervalMs });
    await waitFor(() => events.includes('back'), 5000, 'back');
    await hb.stop({ remove: true });
    await waitFor(() => events.at(-1) === 'down', 5000, 'down after remove');
    assert.equal(await c.get(key), null);
    mon.stop();
  });

  test('watch resumes and writes follow the leader when the leader node is killed', {
    skip: CLUSTER_DIR && SERVER_BIN ? false : 'set RETCD_CLUSTER_DIR and RETCD_SERVER_BIN to run',
    timeout: 240_000,
  }, async () => {
    const base = P('failover');
    const leaderBefore = (await c.health()).find((n) => n.role === 'leader').endpoint;
    const victim = nodeForEndpoint(CLUSTER_DIR, leaderBefore);
    const ac = new AbortController();
    const seen = [];
    const done = (async () => {
      for await (const ev of c.watch(base, { signal: ac.signal })) seen.push(ev.key.slice(base.length));
    })();
    await sleep(300);
    for (const k of ['a1', 'a2', 'a3']) await put(base + k, 'x');
    await waitFor(() => seen.length === 3, 10_000, 'first three events');

    await killNode(CLUSTER_DIR, victim); // a crash, not a clean stop
    try {
      // The cluster elects a new leader; writes keep working through the same client object.
      // A slow election can time out a write (UnknownOutcome); a write is never resent for us, so
      // the test checks the key and writes again, which is what a caller would do.
      for (const k of ['b1', 'b2', 'b3']) {
        for (let attempt = 1; ; attempt++) {
          try {
            await put(base + k, 'y');
            break;
          } catch (err) {
            if (!(err instanceof UnknownOutcomeError) || attempt >= 4) throw err;
          }
        }
      }
      assert.notEqual(c.endpoint, leaderBefore, 'the client moved to a different node');
      await waitFor(() => seen.length >= 6, 30_000, `watch to resume (saw ${seen.join(',')})`);
      ac.abort();
      await done;
      const uniq = [...new Set(seen)];
      assert.deepEqual(uniq, ['a1', 'a2', 'a3', 'b1', 'b2', 'b3'].filter((k) => uniq.includes(k)), 'in order');
      for (const k of ['a1', 'a2', 'a3', 'b1', 'b2', 'b3']) assert.equal(seen.filter((s) => s === k).length >= 1, true, `${k} was delivered`);
      // b-keys written by an UnknownOutcome retry may legitimately appear twice; a-keys never.
      for (const k of ['a1', 'a2', 'a3']) assert.equal(seen.filter((s) => s === k).length, 1, `${k} delivered once`);
    } finally {
      ac.abort();
      await startNode(CLUSTER_DIR, victim, SERVER_BIN);
      await waitFor(async () => (await c.health()).every((n) => n.ok && n.ready), 90_000, 'all nodes ready again');
    }
  });
});
