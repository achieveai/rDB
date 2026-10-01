// Presence logic with a fake client (no cluster): heartbeat format, and the up/late/down/back states.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { startHeartbeat, watchPresence } from '../src/presence.mjs';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

test('heartbeat writes presence/<name> = {name, pid, seq, sent_at}, exactly', async () => {
  const puts = [];
  const client = { put: async (k, v) => puts.push([k, v]), delete: async () => {} };
  const hb = startHeartbeat(client, 'api', { intervalMs: 30 });
  await sleep(110);
  await hb.stop();
  assert.ok(puts.length >= 3, `got ${puts.length} beats`);
  const [key, value] = puts[0];
  assert.equal(key, 'presence/api');
  const obj = JSON.parse(value);
  assert.deepEqual(Object.keys(obj), ['name', 'pid', 'seq', 'sent_at'], 'same field names and order as the other libraries');
  assert.equal(obj.name, 'api');
  assert.equal(obj.pid, process.pid);
  assert.equal(obj.seq, 1);
  assert.equal(new Date(obj.sent_at).toISOString(), obj.sent_at, 'sent_at is an ISO time');
  assert.deepEqual(puts.map(([, v]) => JSON.parse(v).seq), puts.map((_, i) => i + 1), 'seq counts up by one');
  const n = puts.length;
  await sleep(80);
  assert.equal(puts.length, n, 'stop() really stops');
});

test('heartbeat: a failed beat is reported and the next one still goes out; stop({remove}) deletes the key', async () => {
  let calls = 0;
  const errors = [];
  const deleted = [];
  const client = {
    put: async () => {
      if (++calls === 1) throw new Error('boom');
    },
    delete: async (k) => deleted.push(k),
  };
  const hb = startHeartbeat(client, 'x', { intervalMs: 30, onError: (e) => errors.push(e.message) });
  await sleep(100);
  await hb.stop({ remove: true });
  assert.deepEqual(errors, ['boom']);
  assert.ok(calls >= 3);
  assert.deepEqual(deleted, ['presence/x']);
});

test('heartbeat rejects a bad name or interval', () => {
  const client = { put: async () => {} };
  assert.throws(() => startHeartbeat(client, ''), TypeError);
  assert.throws(() => startHeartbeat(client, 'a', { intervalMs: 0 }), RangeError);
});

// A fake client for the monitor: list() returns what is "stored", watch() is fed by hand.
function fakeStore(initial = {}) {
  const feeds = [];
  const client = {
    list(prefix) {
      assert.equal(prefix, 'presence/');
      const it = (async function* gen() {
        for (const [name, sentAt] of Object.entries(initial)) {
          yield { key: `presence/${name}`, value: Buffer.from(JSON.stringify({ name, pid: 1, seq: 1, sent_at: sentAt })) };
        }
      })();
      Object.defineProperty(it, 'readRevision', { get: () => 77 });
      return it;
    },
    async *watch(prefix, { fromRevision, signal }) {
      assert.equal(prefix, 'presence/');
      assert.equal(fromRevision, 77, 'watch starts at the list revision: no gap');
      const queue = [];
      let wake;
      feeds.push((ev) => {
        queue.push(ev);
        wake?.();
      });
      while (!signal.aborted) {
        if (queue.length) yield queue.shift();
        else await new Promise((r) => ((wake = r), signal.addEventListener('abort', r, { once: true }), setTimeout(r, 20)));
      }
    },
  };
  const beat = (name) => feeds[0]({ type: 'put', key: `presence/${name}`, value: Buffer.from('{}'), revision: 1 });
  const remove = (name) => feeds[0]({ type: 'delete', key: `presence/${name}`, value: null, revision: 2 });
  return { client, beat, remove };
}

test('monitor: up, then late, then down when beats stop, then back when they return', async () => {
  const { client, beat } = fakeStore();
  const mon = watchPresence(client, { intervalMs: 100, missedBeats: 3 }); // late at 150 ms, down at 350 ms
  const seen = [];
  mon.on('change', (e) => seen.push(`${e.name}:${e.state}`));
  await mon.ready;
  await sleep(30);
  beat('svc');
  await sleep(60);
  assert.deepEqual(seen, ['svc:up']);
  await sleep(150); // ~210 ms since the beat
  assert.deepEqual(seen, ['svc:up', 'svc:late']);
  await sleep(250); // ~460 ms
  assert.deepEqual(seen, ['svc:up', 'svc:late', 'svc:down']);
  beat('svc');
  await sleep(40);
  assert.deepEqual(seen, ['svc:up', 'svc:late', 'svc:down', 'svc:back']);
  beat('svc'); // a second beat while healthy says nothing
  await sleep(40);
  assert.equal(seen.length, 4);
  assert.deepEqual(mon.snapshot().map((m) => `${m.name}:${m.state}`), ['svc:up']);
  mon.stop();
});

test('monitor: a beat keeps a name up indefinitely', async () => {
  const { client, beat } = fakeStore();
  const mon = watchPresence(client, { intervalMs: 100, missedBeats: 3 });
  const seen = [];
  mon.on('change', (e) => seen.push(`${e.name}:${e.state}`));
  await mon.ready;
  await sleep(30);
  for (let i = 0; i < 8; i++) {
    beat('steady');
    await sleep(80);
  }
  assert.deepEqual(seen, ['steady:up']);
  mon.stop();
});

test('monitor: a deleted key is down at once; names already stored are judged from sent_at', async () => {
  const now = Date.now();
  const { client, remove } = fakeStore({
    fresh: new Date(now - 10).toISOString(),
    stale: new Date(now - 60_000).toISOString(),
    gone: new Date(now - 10).toISOString(),
  });
  const mon = watchPresence(client, { intervalMs: 100, missedBeats: 3 });
  const seen = [];
  mon.on('change', (e) => seen.push(`${e.name}:${e.state}`));
  await mon.ready;
  assert.deepEqual([...seen].sort(), ['fresh:up', 'gone:up', 'stale:down']);
  await sleep(30);
  remove('gone');
  await sleep(40);
  assert.ok(seen.includes('gone:down'));
  mon.stop();
});

test('monitor: usable with for-await, replays current state first, ends on stop()', async () => {
  const { client, beat } = fakeStore({ old: new Date().toISOString() });
  const mon = watchPresence(client, { intervalMs: 100, missedBeats: 3 });
  await mon.ready;
  const got = [];
  const done = (async () => {
    for await (const ev of mon) got.push(`${ev.name}:${ev.state}`);
  })();
  await sleep(30);
  beat('new');
  await sleep(60);
  mon.stop();
  await done;
  assert.deepEqual(got.slice(0, 2), ['old:up', 'new:up']);
});

test('monitor: a failing first list surfaces on ready and as error', async () => {
  const client = {
    list() {
      return (async function* boom() {
        throw new Error('no cluster');
      })();
    },
    watch() {
      throw new Error('unreachable');
    },
  };
  const mon = watchPresence(client, { intervalMs: 100 });
  const errors = [];
  mon.on('error', (e) => errors.push(e.message));
  const ended = new Promise((resolve) => mon.once('end', resolve));
  await assert.rejects(mon.ready, /no cluster/);
  await ended;
  assert.deepEqual(errors, ['no cluster']);
});
