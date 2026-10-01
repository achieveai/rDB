// @retcd/client: a small Node.js client for a rEtcd cluster. See ../README.md.
//
// Leader following, retry rules and glob matching are the ones proven in
// samples/retcd-playground/kv.mjs:
//   - a follower answers "not the leader" (nothing applied) -> go to the leader it names.
//   - a refused connection (nothing sent) -> try the next node.
//   - a write that timed out, or whose connection died after sending, is NEVER retried:
//     UnknownOutcomeError. Reads are retried on another node.
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import {
  CasConflictError,
  CompactedError,
  IntegrityError,
  NotFoundError,
  RetcdError,
  TooLargeError,
  UnavailableError,
  UnknownOutcomeError,
  classify,
  leaderHint,
  mapError,
  metaOf,
} from './errors.mjs';
import { globToRegExp, hasGlob, literalPrefix } from './glob.mjs';
import { startHeartbeat, watchPresence } from './presence.mjs';

export {
  CasConflictError,
  CompactedError,
  IntegrityError,
  NotFoundError,
  NotFoundError as NotFound,
  RetcdError,
  TooLargeError,
  UnavailableError,
  UnknownOutcomeError,
  globToRegExp,
  hasGlob,
  literalPrefix,
  startHeartbeat,
  watchPresence,
};

/** Server limits (crates/config-core/src/limits.rs). */
export const LIMITS = Object.freeze({ maxKeyBytes: 1024, maxValueBytes: 1024 * 1024, maxListItems: 1000 });

const DEFAULT_PROTO = fileURLToPath(new URL('../../../proto/retcd/v1/config.proto', import.meta.url));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sha256 = (buf) => createHash('sha256').update(buf).digest('hex');

// One loaded service definition per proto file.
const serviceCache = new Map();
function loadService(protoPath) {
  const file = path.resolve(protoPath ?? process.env.RETCD_PROTO ?? DEFAULT_PROTO);
  let svc = serviceCache.get(file);
  if (!svc) {
    if (!fs.existsSync(file)) {
      throw new RetcdError(
        `proto file not found: ${file}. Pass { protoPath } to RetcdClient.connect, or set RETCD_PROTO, to the repo's proto/retcd/v1/config.proto.`,
        { code: 'PROTO_NOT_FOUND' },
      );
    }
    const def = protoLoader.loadSync(file, { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
    svc = grpc.loadPackageDefinition(def).retcd.v1.ConfigService;
    serviceCache.set(file, svc);
  }
  return svc;
}

const keyBuf = (key) => {
  const buf = typeof key === 'string' ? Buffer.from(key, 'utf8') : Buffer.from(key ?? []);
  if (buf.length === 0) throw new RetcdError('key is empty', { code: 'INVALID_ARGUMENT' });
  if (buf.length > LIMITS.maxKeyBytes) throw new TooLargeError(`key is ${buf.length} bytes; the limit is ${LIMITS.maxKeyBytes}. Not sent.`);
  return buf;
};

const valueBuf = (value) => {
  let buf;
  if (typeof value === 'string') buf = Buffer.from(value, 'utf8');
  else if (value instanceof Uint8Array) buf = Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  else throw new TypeError('value must be a string, Buffer or Uint8Array');
  if (buf.length > LIMITS.maxValueBytes) throw new TooLargeError(`value is ${buf.length} bytes; the limit is ${LIMITS.maxValueBytes} (1 MiB). Not sent.`);
  return buf;
};

const revString = (n, what) => {
  if (!/^\d+$/.test(String(n))) throw new RangeError(`${what} must be a whole number >= 0, got ${String(n)}`);
  return String(n);
};

const toRecord = (r) => ({
  key: r.key.toString('utf8'),
  value: r.value,
  createRevision: Number(r.create_revision),
  modRevision: Number(r.mod_revision),
});

const normalizeEndpoint = (e) => String(e).trim().replace(/^[a-z]+:\/\//i, '').replace(/\/+$/, '');

/**
 * @typedef {{key: string, value: Buffer, createRevision: number, modRevision: number}} Record
 * @typedef {{type: 'put'|'delete', key: string, value: Buffer|null, revision: number}} WatchEvent
 */
export class RetcdClient {
  /**
   * Connect. Finds the leader with one read, so a dead cluster fails here.
   * @param {object} opts
   * @param {string[]} opts.endpoints  client addresses, e.g. ['127.0.0.1:17302', ...]
   * @param {number} [opts.timeoutMs=10000]   deadline for one attempt
   * @param {number} [opts.failoverMs=20000]  how long one call may hunt for a leader / live node
   * @param {string} [opts.protoPath]  config.proto; default: the repo's proto/ (or env RETCD_PROTO)
   * @param {string[]} [opts.healthEndpoints]  health host:port per endpoint; default client port + 2
   * @param {boolean} [opts.probe=true]  set false to connect lazily
   */
  static async connect(opts = {}) {
    const client = new RetcdClient(opts);
    if (opts.probe !== false) await client.revision();
    return client;
  }

  constructor({ endpoints, timeoutMs = 10_000, failoverMs = 20_000, protoPath, healthEndpoints } = {}) {
    const list = (Array.isArray(endpoints) ? endpoints : String(endpoints ?? '').split(',')).map(normalizeEndpoint).filter(Boolean);
    if (list.length === 0) throw new RetcdError("endpoints is empty. Pass e.g. { endpoints: ['127.0.0.1:17302'] }", { code: 'INVALID_ARGUMENT' });
    this._service = loadService(protoPath);
    this._endpoints = [...new Set(list)];
    this._configured = [...this._endpoints];
    this._healthEndpoints = healthEndpoints;
    this._timeoutMs = timeoutMs;
    this._failoverMs = failoverMs;
    this._addr = this._endpoints[0];
    this._clients = new Map();
    this._watches = new Set();
    this._closed = false;
  }

  /** Address of the node the next call goes to (the leader, once found). */
  get endpoint() {
    return this._addr;
  }

  // ---- plumbing -----------------------------------------------------------------------
  _svc(addr) {
    let c = this._clients.get(addr);
    if (!c) {
      c = new this._service(addr, grpc.credentials.createInsecure(), {
        'grpc.max_receive_message_length': 16 * 1024 * 1024,
        'grpc.max_send_message_length': 16 * 1024 * 1024,
      });
      this._clients.set(addr, c);
    }
    return c;
  }

  _assertOpen() {
    if (this._closed) throw new RetcdError('client is closed', { code: 'CLOSED' });
  }

  _use(addr) {
    if (!this._endpoints.includes(addr)) this._endpoints.push(addr); // a leader we were told about
    this._addr = addr;
  }

  _rotate() {
    const i = this._endpoints.indexOf(this._addr);
    this._addr = this._endpoints[(i + 1) % this._endpoints.length];
  }

  _once(addr, method, req) {
    return new Promise((resolve, reject) => {
      this._svc(addr)[method](req, { deadline: Date.now() + this._timeoutMs }, (err, res) => (err ? reject(err) : resolve(res)));
    });
  }

  // One logical call with leader following. See the rules at the top of this file.
  async _call(method, req, { write = false } = {}) {
    const giveUpAt = Date.now() + this._failoverMs;
    let hops = 0;
    for (;;) {
      this._assertOpen();
      const addr = this._addr;
      try {
        return await this._once(addr, method, req);
      } catch (err) {
        const kind = classify(err);
        const readRetry = !write && kind === 'transport' && (err.code === grpc.status.UNAVAILABLE || err.code === grpc.status.DEADLINE_EXCEEDED);
        if (!(kind === 'not-leader' || kind === 'refused' || readRetry) || Date.now() >= giveUpAt) {
          throw mapError(err, { write, endpoint: addr });
        }
        const hint = kind === 'not-leader' ? leaderHint(err) : undefined;
        if (hint && hint !== addr) {
          this._use(hint);
          if (++hops > 2) await sleep(200); // a hint loop must not spin
        } else {
          await sleep(kind === 'not-leader' ? 300 : 150); // election in progress, or node down
          if (!hint || kind !== 'not-leader') this._rotate();
        }
      }
    }
  }

  // ---- single keys --------------------------------------------------------------------

  /** Current cluster revision (a read; also finds the leader). */
  async revision() {
    const r = await this._call('Get', { key: Buffer.from('retcd-client/revision-probe') });
    return Number(r.read_revision);
  }

  /** @returns {Promise<Record|null>} null if the key does not exist */
  async get(key) {
    const r = await this._call('Get', { key: keyBuf(key) });
    return r.record ? toRecord(r.record) : null;
  }

  /**
   * Store a value. With `ifRevision`, only if the key is still at that modRevision
   * (0 = only if it does not exist). Throws CasConflictError otherwise.
   * @returns {Promise<{revision: number}>}
   */
  async put(key, value, { ifRevision } = {}) {
    const req = { key: keyBuf(key), value: valueBuf(value) };
    if (ifRevision !== undefined) req.expected_mod_revision = revString(ifRevision, 'ifRevision');
    const r = await this._call('Put', req, { write: true });
    return this._applied(r, 'put');
  }

  /** Delete a key. Throws NotFoundError if it does not exist; CasConflictError if `ifRevision` is stale. */
  async delete(key, { ifRevision } = {}) {
    const req = { key: keyBuf(key) };
    if (ifRevision !== undefined) req.expected_mod_revision = revString(ifRevision, 'ifRevision');
    const r = await this._call('Delete', req, { write: true });
    return this._applied(r, 'delete');
  }

  // A conflict can come back as an OK response or as a typed error (mapError handles that one).
  _applied(r, what) {
    if (r.outcome === 'APPLIED') return { revision: Number(r.revision) };
    if (r.outcome === 'CONFLICT') {
      const cur = Number(r.current_mod_revision);
      throw new CasConflictError(r.exists ? `key is at revision ${cur}, not the one you expected` : 'key does not exist right now', {
        currentRevision: cur,
        exists: r.exists,
      });
    }
    if (r.outcome === 'NOT_FOUND') throw new NotFoundError(`${what}: key does not exist`);
    throw new RetcdError(`${what}: unexpected outcome ${r.outcome}`, { code: 'UNEXPECTED' });
  }

  // ---- listing ------------------------------------------------------------------------

  /**
   * Every record whose key matches a prefix or a glob, in key order, paged and pinned to one
   * revision. The returned iterator also has `.readRevision` (set after the first page):
   * pass it to watch() as `fromRevision` to list-then-watch with no gap.
   * @param {string} [pattern]  plain prefix, or a glob ('docs/*.md', 'a/**')
   * @param {{pageSize?: number}} [opts]  server clamps pageSize to 1000
   * @returns {AsyncGenerator<Record> & {readRevision: number|undefined}}
   */
  list(pattern = '', { pageSize = 500 } = {}) {
    if (!Number.isInteger(pageSize) || pageSize < 1) throw new RangeError('pageSize must be a whole number >= 1');
    const self = this;
    const state = { readRevision: undefined };
    const gen = (async function* walk() {
      const prefix = Buffer.from(literalPrefix(pattern), 'utf8');
      const re = hasGlob(pattern) ? globToRegExp(pattern) : null;
      let token = Buffer.alloc(0); // present but empty: start a pinned walk
      for (;;) {
        const r = await self._call('List', { prefix, max_items: pageSize, page_token: token });
        state.readRevision ??= Number(r.read_revision);
        for (const rec of r.records) {
          const out = toRecord(rec);
          if (!re || re.test(out.key)) yield out;
        }
        if (r.next_page_token && r.next_page_token.length > 0) {
          token = r.next_page_token;
          continue;
        }
        if (r.truncated) throw new RetcdError('the server cut the list short (size cap) and gave no cursor to continue', { code: 'LIST_TRUNCATED' });
        return;
      }
    })();
    Object.defineProperty(gen, 'readRevision', { get: () => state.readRevision });
    return gen;
  }

  /**
   * One-level folder view. Keys that match are files; a key deeper than the pattern adds one
   * `{dir, count}` entry for the folder it sits in. 'docs' and 'docs/' mean 'docs/*'.
   * @returns {Promise<Array<Record | {dir: string, count: number}>>}
   */
  async listDirs(pattern = '', { pageSize = 500 } = {}) {
    const glob = hasGlob(pattern) ? pattern : `${pattern}${pattern === '' || pattern.endsWith('/') ? '' : '/'}*`;
    const re = globToRegExp(glob);
    const wantDirs = !glob.includes('**');
    const entries = []; // server (sorted) order; a dir entry sits where its first key was
    const dirs = new Map();
    // Walk the plain prefix (not the glob) so keys deeper than the pattern can add folders.
    for await (const rec of this.list(literalPrefix(glob), { pageSize })) {
      if (re.test(rec.key)) entries.push(rec);
      if (!wantDirs) continue;
      for (let i = rec.key.indexOf('/'); i !== -1; i = rec.key.indexOf('/', i + 1)) {
        const dir = rec.key.slice(0, i);
        if (!re.test(dir)) continue;
        let d = dirs.get(dir);
        if (!d) {
          d = { dir: `${dir}/`, count: 0 };
          dirs.set(dir, d);
          entries.push(d);
        }
        d.count++;
      }
    }
    return entries;
  }

  // ---- watch --------------------------------------------------------------------------

  /**
   * Stream changes under a prefix or glob. Reconnects by itself (leader change, node stopped)
   * and resumes after the last revision it saw, so no event is repeated or skipped.
   * Ends quietly when `signal` aborts or you `break`.
   * @param {string} [pattern]
   * @param {{fromRevision?: number, signal?: AbortSignal, progress?: boolean, giveUpAfterMs?: number}} [opts]
   *   fromRevision: events after this revision; default = from now. progress: also yield
   *   {type:'progress', revision}. giveUpAfterMs: throw UnavailableError after this long without a
   *   connection (default 60000; 0 = never).
   * @returns {AsyncGenerator<WatchEvent>}
   */
  async *watch(pattern = '', { fromRevision, signal, progress = false, giveUpAfterMs = 60_000 } = {}) {
    const prefix = Buffer.from(literalPrefix(pattern), 'utf8');
    const re = hasGlob(pattern) ? globToRegExp(pattern) : null;
    let last = fromRevision === undefined ? await this.revision() : Number(revString(fromRevision, 'fromRevision'));
    let failingSince = 0;
    let fails = 0;
    while (!signal?.aborted && !this._closed) {
      const addr = this._addr;
      const call = this._svc(addr).Watch({ prefix, start_after_revision: String(last) });
      this._watches.add(call);
      const onAbort = () => call.cancel();
      signal?.addEventListener('abort', onAbort, { once: true });
      try {
        for await (const m of call) {
          failingSince = 0;
          fails = 0;
          if (m.body === 'event') {
            const ev = m.event;
            last = Number(ev.revision);
            const key = ev.key.toString('utf8');
            if (re && !re.test(key)) continue;
            if (ev.change === 'put') yield { type: 'put', key, value: ev.put.value, revision: last };
            else yield { type: 'delete', key, value: null, revision: last };
          } else if (m.body === 'progress') {
            last = Number(m.progress.revision); // quiet heartbeat: no key data
            if (progress) yield { type: 'progress', key: '', value: null, revision: last };
          }
        }
        // stream ended cleanly (server shutting down): reconnect after a short pause
        await sleep(200);
      } catch (err) {
        if (signal?.aborted || this._closed) return;
        const kind = classify(err);
        const resumable = err.code === grpc.status.RESOURCE_EXHAUSTED && metaOf(err, 'retcd-resumable') === 'true';
        if (err.code === grpc.status.RESOURCE_EXHAUSTED && !resumable) {
          throw new RetcdError(`watch refused: ${err.details ?? err.message} (limit is 100 watch streams per principal)`, { code: 'WATCH_LIMIT', grpcCode: err.code, cause: err });
        }
        // A typed refusal (compacted, bad argument, denied) will not get better by retrying.
        if (!resumable && kind === 'server') throw mapError(err, { endpoint: addr });
        const hint = kind === 'not-leader' ? leaderHint(err) : undefined;
        failingSince ||= Date.now();
        if (giveUpAfterMs && Date.now() - failingSince > giveUpAfterMs) {
          throw new UnavailableError(`watch could not reconnect for ${giveUpAfterMs} ms; last seen revision ${last}`, { cause: err });
        }
        if (hint && hint !== addr) this._use(hint);
        else this._rotate();
        fails++;
        await sleep(hint && fails === 1 ? 0 : Math.min(2000, 100 * 2 ** fails));
      } finally {
        signal?.removeEventListener('abort', onAbort);
        this._watches.delete(call);
        call.cancel();
      }
    }
  }

  // ---- files --------------------------------------------------------------------------

  /**
   * Store a file (<= 1 MiB) under `key` (default files/<name>) plus a meta record
   * `meta/<key>` = {size, sha256, stored_at_rev}. Two writes, not atomic.
   * @returns {Promise<{key: string, size: number, sha256: string, revision: number, metaRevision: number}>}
   */
  async putFile(filePath, key) {
    const st = await fs.promises.stat(filePath);
    if (!st.isFile()) throw new RetcdError(`${filePath} is not a file`, { code: 'INVALID_ARGUMENT' });
    if (st.size > LIMITS.maxValueBytes) throw new TooLargeError(`${filePath} is ${st.size} bytes; the limit is ${LIMITS.maxValueBytes} (1 MiB). Not sent.`);
    const data = await fs.promises.readFile(filePath);
    const k = key ?? `files/${path.basename(filePath)}`;
    const put = await this.put(k, data);
    const meta = { size: data.length, sha256: sha256(data), stored_at_rev: put.revision };
    const m = await this.put(`meta/${k}`, JSON.stringify(meta));
    return { key: k, size: meta.size, sha256: meta.sha256, revision: put.revision, metaRevision: m.revision };
  }

  /**
   * Fetch a file and write it to `outPath`, checking sha256 + size against `meta/<key>` when
   * that exists. Refuses to overwrite unless `{overwrite: true}`. Throws NotFoundError / IntegrityError.
   * @returns {Promise<{size: number, sha256: string, revision: number, verified: boolean}>}
   */
  async getFile(key, outPath, { overwrite = false } = {}) {
    if (!overwrite && fs.existsSync(outPath)) throw new RetcdError(`${outPath} already exists. Pass { overwrite: true } to replace it.`, { code: 'EXISTS' });
    const rec = await this.get(key);
    if (!rec) throw new NotFoundError(`not found: ${key}`);
    const digest = sha256(rec.value);
    const metaRec = await this.get(`meta/${key}`);
    if (metaRec) {
      let meta;
      try {
        meta = JSON.parse(metaRec.value.toString('utf8'));
      } catch {
        throw new IntegrityError(`meta record for ${key} is not valid JSON. File not written.`);
      }
      if (meta.sha256 !== digest || meta.size !== rec.value.length) {
        throw new IntegrityError(`CHECK FAILED: stored ${rec.value.length} bytes / sha256 ${digest}, meta says ${meta.size} bytes / ${meta.sha256}. File not written.`);
      }
    }
    await fs.promises.writeFile(outPath, rec.value);
    return { size: rec.value.length, sha256: digest, revision: rec.modRevision, verified: Boolean(metaRec) };
  }

  // ---- health, close ------------------------------------------------------------------

  /**
   * Ask every configured node's /health. Never throws for a down node: it comes back `ok: false`.
   * Health port defaults to client port + 2 (the local-cluster.sh layout); override with
   * `healthEndpoints` in connect().
   * @returns {Promise<Array<{endpoint: string, ok: boolean, ready?: boolean, role?: string, leader?: number|null, term?: number, revision?: number, error?: string, raw?: object}>>}
   */
  async health() {
    return Promise.all(
      this._configured.map(async (endpoint, i) => {
        const hp = this._healthEndpoints?.[i] ?? healthOf(endpoint);
        try {
          const res = await fetch(`http://${hp}/health`, { signal: AbortSignal.timeout(1500) });
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const h = await res.json();
          return { endpoint, ok: true, ready: h.ready, role: h.role, leader: h.current_leader ?? null, term: h.term, revision: h.cluster_revision, raw: h };
        } catch (err) {
          return { endpoint, ok: false, error: err.cause?.code ?? err.message };
        }
      }),
    );
  }

  /** Close connections and end every open watch. Safe to call twice. */
  close() {
    this._closed = true;
    for (const call of this._watches) call.cancel();
    for (const c of this._clients.values()) c.close();
    this._clients.clear();
  }
}

function healthOf(endpoint) {
  const i = endpoint.lastIndexOf(':');
  return `${endpoint.slice(0, i)}:${Number(endpoint.slice(i + 1)) + 2}`;
}
