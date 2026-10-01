// Presence without leases. rEtcd keys never expire, so "is X alive" is a heartbeat:
//   writer:  every intervalMs, put  presence/<name> = {"name","pid","seq","sent_at"}
//   monitor: watch presence/ and judge each name by when ITS OWN clock last saw a beat.
// The key and value format is shared with other libraries (C#, presence.mjs). Do not change it.
import { EventEmitter, on } from 'node:events';

export const PRESENCE_PREFIX = 'presence/';

/**
 * Start writing heartbeats for `name`. The first beat is sent at once.
 * @param {{put: Function, delete: Function}} client  a RetcdClient
 * @param {string} name
 * @param {{intervalMs?: number, onError?: (err: Error) => void}} [opts]
 *   onError: called when a beat fails (default: one line on stderr per outage).
 * @returns {{stop: (opts?: {remove?: boolean}) => Promise<void>, seq: number, key: string}}
 *   stop() ends the loop; with {remove: true} it also deletes the key, so monitors see "down" at once.
 *   The loop keeps the process alive until you call stop().
 */
export function startHeartbeat(client, name, { intervalMs = 2000, onError } = {}) {
  if (typeof name !== 'string' || name === '') throw new TypeError('presence name must be a non-empty string');
  if (!(intervalMs > 0)) throw new RangeError('intervalMs must be > 0');
  const key = `${PRESENCE_PREFIX}${name}`;
  let seq = 0;
  let stopped = false;
  let failing = false;
  let timer;
  let wake;
  const report = onError ?? ((err) => process.stderr.write(`[retcd heartbeat ${name}] ${err.message}\n`));

  const beat = async () => {
    seq++;
    const value = JSON.stringify({ name, pid: process.pid, seq, sent_at: new Date().toISOString() });
    try {
      await client.put(key, value);
      failing = false;
    } catch (err) {
      if (onError || !failing) report(err);
      failing = true;
    }
  };

  const loop = (async () => {
    while (!stopped) {
      const t0 = Date.now();
      await beat();
      if (stopped) break;
      await new Promise((resolve) => {
        wake = resolve;
        timer = setTimeout(resolve, Math.max(0, intervalMs - (Date.now() - t0)));
      });
    }
  })();

  return {
    key,
    get seq() {
      return seq;
    },
    async stop({ remove = false } = {}) {
      stopped = true;
      clearTimeout(timer);
      wake?.();
      await loop;
      if (remove) {
        try {
          await client.delete(key);
        } catch (err) {
          if (err.code !== 'NOT_FOUND') throw err;
        }
      }
    },
  };
}

/**
 * Watch every heartbeat and report each name as it changes state:
 *   up    first time seen
 *   late  no beat for 1.5 intervals
 *   down  no beat for `missedBeats` intervals (+ half), or its key was deleted
 *   back  a beat arrived after late/down
 * Judged with this process's clock, so machines need not agree on the time. Names already in the
 * store when the monitor starts are judged once from their `sent_at` (clocks should roughly agree
 * for that first look; a stale key shows up as `down`).
 *
 * Use it as an EventEmitter (`on('change', ev)`; also 'ready', 'error') or with
 * `for await (const ev of monitor)`, which first replays every member's current state.
 * Each ev is `{name, state, lastSeen: Date}`. Call `stop()` when done.
 *
 * @param {object} client  a RetcdClient
 * @param {{intervalMs?: number, missedBeats?: number, giveUpAfterMs?: number}} [opts]
 *   intervalMs must match the writers' interval.
 * @returns {PresenceMonitor}
 */
export function watchPresence(client, opts) {
  return new PresenceMonitor(client, opts);
}

export class PresenceMonitor extends EventEmitter {
  constructor(client, { intervalMs = 2000, missedBeats = 3, giveUpAfterMs } = {}) {
    super();
    if (!(intervalMs > 0)) throw new RangeError('intervalMs must be > 0');
    if (!(missedBeats >= 1)) throw new RangeError('missedBeats must be >= 1');
    this._client = client;
    this._lateAfter = intervalMs * 1.5;
    this._downAfter = intervalMs * missedBeats + intervalMs / 2;
    this._giveUpAfterMs = giveUpAfterMs;
    this._ac = new AbortController();
    this._members = new Map(); // name -> {state, lastSeen (ms)}
    this._tickMs = Math.max(50, Math.min(500, intervalMs / 4));
    /** Resolves once the starting members are known (or rejects if the first list fails). */
    this.ready = new Promise((resolve, reject) => {
      this._resolveReady = resolve;
      this._rejectReady = reject;
    });
    this.ready.catch(() => {}); // the failure is also reported by 'error' / the iterator
    this._run();
  }

  /** Current members: [{name, state, lastSeen: Date}] */
  snapshot() {
    return [...this._members].map(([name, m]) => ({ name, state: m.state, lastSeen: new Date(m.lastSeen) }));
  }

  /** Stop watching. Ends any `for await` loop. */
  stop() {
    this._ac.abort();
  }

  // `back` is an announcement, not a resting state: after it the member is simply `up`.
  _set(name, m, state) {
    m.state = state === 'back' ? 'up' : state;
    this.emit('change', { name, state, lastSeen: new Date(m.lastSeen) });
  }

  _seed(rec) {
    const name = rec.key.slice(PRESENCE_PREFIX.length);
    const now = Date.now();
    let sent = now;
    try {
      const t = Date.parse(JSON.parse(rec.value.toString('utf8')).sent_at);
      if (Number.isFinite(t)) sent = Math.min(t, now);
    } catch {
      /* not a heartbeat we understand: treat as just seen */
    }
    const age = now - sent;
    const m = { lastSeen: sent };
    this._members.set(name, m);
    this._set(name, m, age >= this._downAfter ? 'down' : age >= this._lateAfter ? 'late' : 'up');
  }

  _beat(name) {
    const now = Date.now();
    const m = this._members.get(name);
    if (!m) {
      const fresh = { lastSeen: now };
      this._members.set(name, fresh);
      this._set(name, fresh, 'up');
      return;
    }
    m.lastSeen = now;
    if (m.state !== 'up') this._set(name, m, 'back');
  }

  _tick() {
    const now = Date.now();
    for (const [name, m] of this._members) {
      if (m.state === 'down') continue;
      const age = now - m.lastSeen;
      if (age >= this._downAfter) this._set(name, m, 'down');
      else if (age >= this._lateAfter && m.state !== 'late') this._set(name, m, 'late');
    }
  }

  async _run() {
    let timer;
    try {
      const walk = this._client.list(PRESENCE_PREFIX);
      for await (const rec of walk) this._seed(rec);
      const from = walk.readRevision;
      this._resolveReady(this.snapshot());
      this.emit('ready', this.snapshot());
      timer = setInterval(() => this._tick(), this._tickMs);
      const watchOpts = { fromRevision: from, signal: this._ac.signal };
      if (this._giveUpAfterMs !== undefined) watchOpts.giveUpAfterMs = this._giveUpAfterMs;
      for await (const ev of this._client.watch(PRESENCE_PREFIX, watchOpts)) {
        const name = ev.key.slice(PRESENCE_PREFIX.length);
        if (ev.type === 'put') this._beat(name);
        else {
          const m = this._members.get(name);
          if (m && m.state !== 'down') this._set(name, m, 'down');
        }
      }
    } catch (err) {
      this.error = err;
      this._rejectReady(err);
      if (this.listenerCount('error') > 0) this.emit('error', err);
    } finally {
      clearInterval(timer);
      this._ac.abort();
      this.emit('end');
    }
  }

  async *[Symbol.asyncIterator]() {
    const live = on(this, 'change', { signal: this._ac.signal, close: ['end'] });
    for (const m of this.snapshot()) yield m;
    try {
      for await (const [ev] of live) yield ev;
    } catch (err) {
      if (err.name !== 'AbortError') throw err;
    }
    if (this.error) throw this.error;
  }
}
