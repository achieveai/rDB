// Presence logic with a fake client (no cluster): heartbeat format, and the up/late/down/back states.
//
// Time is fake (node:test mock timers for Date, setTimeout and setInterval), so these tests give the
// same answer on an idle host and on one with every core busy. They used to sleep on the real clock
// and assert after the sleep; under load a sleep overran, an assertion failed before mon.stop(), and
// the monitor's interval kept the test process alive for ever.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { startHeartbeat, watchPresence } from '../src/presence.mjs';

const START = Date.parse('2026-01-01T00:00:00Z');

// Let promises and async generators run. setImmediate is not mocked, so this is real "later".
const flush = async () => {
  for (let i = 0; i < 10; i++) await new Promise((r) => setImmediate(r));
};

// Fake time for this test only. advance(ms) moves the clock in 10 ms steps and lets the code
// react after each step, the way it would between real timer callbacks.
function fakeTime(t) {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout', 'setInterval'], now: START });
  return async (ms) => {
    for (let left = ms; left > 0; left -= 10) {
      t.mock.timers.tick(Math.min(10, left));
      await flush();
    }
  };
}

test('heartbeat writes presence/<name> = {name, pid, seq, sent_at}, exactly', async (t) => {
  const advance = fakeTime(t);
  const puts = [];
  const client = { put: async (k, v) => puts.push([k, v]), delete: async () => {} };
  const hb = startHeartbeat(client, 'api', { intervalMs: 30 });
  t.after(() => hb.stop());
  await flush();
  await advance(100); // beats at 0, 30, 60, 90
  await hb.stop();
  assert.equal(puts.length, 4);
  const [key, value] = puts[0];
  assert.equal(key, 'presence/api');
  const obj = JSON.parse(value);
  assert.deepEqual(Object.keys(obj), ['name', 'pid', 'seq', 'sent_at'], 'same field names and order as the other libraries');
  assert.equal(obj.name, 'api');
  assert.equal(obj.pid, process.pid);
  assert.equal(obj.seq, 1);
  assert.equal(obj.sent_at, new Date(START).toISOString(), 'sent_at is an ISO time');
  assert.deepEqual(puts.map(([, v]) => JSON.parse(v).seq), [1, 2, 3, 4], 'seq counts up by one');
  await advance(80);
  assert.equal(puts.length, 4, 'stop() really stops');
});

test('heartbeat: a failed beat is reported and the next one still goes out; stop({remove}) deletes the key', async (t) => {
  const advance = fakeTime(t);
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
  t.after(() => hb.stop());
  await flush();
  await advance(70); // beats at 0 (fails), 30, 60
  await hb.stop({ remove: true });
  assert.deepEqual(errors, ['boom']);
  assert.equal(calls, 3);
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
        else await new Promise((r) => ((wake = r), signal.addEventListener('abort', r, { once: true })));
      }
    },
  };
  const beat = (name) => feeds[0]({ type: 'put', key: `presence/${name}`, value: Buffer.from('{}'), revision: 1 });
  const remove = (name) => feeds[0]({ type: 'delete', key: `presence/${name}`, value: null, revision: 2 });
  return { client, beat, remove };
}

// Start a monitor that is always stopped when the test ends, pass or fail.
async function startMonitor(t, client, opts) {
  const mon = watchPresence(client, opts);
  t.after(() => mon.stop());
  const seen = [];
  mon.on('change', (e) => seen.push(`${e.name}:${e.state}`));
  await mon.ready;
  await flush(); // the watch is open
  return { mon, seen };
}

test('monitor: up, then late, then down when beats stop, then back when they return', async (t) => {
  const advance = fakeTime(t);
  const { client, beat } = fakeStore();
  const { mon, seen } = await startMonitor(t, client, { intervalMs: 100, missedBeats: 3 }); // late at 150 ms, down at 350 ms
  beat('svc');
  await flush();
  assert.deepEqual(seen, ['svc:up']);
  await advance(140);
  assert.deepEqual(seen, ['svc:up'], 'not late before 1.5 intervals');
  await advance(10); // 150 ms since the beat
  assert.deepEqual(seen, ['svc:up', 'svc:late']);
  await advance(190); // 340 ms
  assert.deepEqual(seen, ['svc:up', 'svc:late'], 'not down before 3.5 intervals');
  await advance(10); // 350 ms
  assert.deepEqual(seen, ['svc:up', 'svc:late', 'svc:down']);
  beat('svc');
  await flush();
  assert.deepEqual(seen, ['svc:up', 'svc:late', 'svc:down', 'svc:back']);
  beat('svc'); // a second beat while healthy says nothing
  await flush();
  assert.equal(seen.length, 4);
  assert.deepEqual(mon.snapshot().map((m) => `${m.name}:${m.state}`), ['svc:up']);
});

test('monitor: a beat keeps a name up indefinitely', async (t) => {
  const advance = fakeTime(t);
  const { client, beat } = fakeStore();
  const { seen } = await startMonitor(t, client, { intervalMs: 100, missedBeats: 3 });
  for (let i = 0; i < 8; i++) {
    beat('steady');
    await flush();
    await advance(140); // just short of late, every time
  }
  assert.deepEqual(seen, ['steady:up']);
});

test('monitor: a deleted key is down at once; names already stored are judged from sent_at', async (t) => {
  fakeTime(t);
  const { client, remove } = fakeStore({
    fresh: new Date(START - 10).toISOString(),
    stale: new Date(START - 60_000).toISOString(),
    gone: new Date(START - 10).toISOString(),
  });
  const { seen } = await startMonitor(t, client, { intervalMs: 100, missedBeats: 3 });
  assert.deepEqual([...seen].sort(), ['fresh:up', 'gone:up', 'stale:down']);
  remove('gone');
  await flush();
  assert.ok(seen.includes('gone:down'));
});

test('monitor: usable with for-await, replays current state first, ends on stop()', async (t) => {
  fakeTime(t);
  const { client, beat } = fakeStore({ old: new Date(START).toISOString() });
  const { mon } = await startMonitor(t, client, { intervalMs: 100, missedBeats: 3 });
  const got = [];
  const done = (async () => {
    for await (const ev of mon) got.push(`${ev.name}:${ev.state}`);
  })();
  await flush();
  beat('new');
  await flush();
  mon.stop();
  await done;
  assert.deepEqual(got, ['old:up', 'new:up']);
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
