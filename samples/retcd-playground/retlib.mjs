// Shared helpers for load.mjs, fanout.mjs and presence.mjs (not a CLI). kv.mjs uses only the
// port helpers. Plain gRPC against a plaintext dev cluster. Nothing imports kv.mjs: it runs
// main() on import.
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PROTO = path.resolve(HERE, '..', '..', 'proto', 'retcd', 'v1', 'config.proto');

export const MAX_VALUE_BYTES = 1024 * 1024;
export const STATUS_NAME = Object.fromEntries(Object.entries(grpc.status).map(([k, v]) => [v, k]));
export { grpc };

const pkgDef = protoLoader.loadSync(PROTO, { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
export const ConfigService = grpc.loadPackageDefinition(pkgDef).retcd.v1.ConfigService;

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const now = () => performance.now();
export const b = (s) => Buffer.from(s, 'utf8');

// ---- arguments ---------------------------------------------------------------------------
// parseArgs(argv, {value: [...], bool: [...]}) -> {pos, flags}. Unknown flags are an error.
export function parseArgs(argv, spec) {
  const value = new Set(spec.value ?? []);
  const bool = new Set(spec.bool ?? []);
  const pos = [];
  const flags = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) {
      pos.push(a);
      continue;
    }
    const name = a.slice(2);
    if (bool.has(name)) flags[name] = true;
    else if (value.has(name)) {
      if (i + 1 >= argv.length) die(`${a} needs a value`);
      flags[name] = argv[++i];
    } else die(`unknown option ${a}`);
  }
  return { pos, flags };
}

export function die(msg) {
  process.stderr.write(`error: ${msg}\n`);
  process.exit(2);
}

export function num(flags, name, dflt) {
  if (flags[name] === undefined) return dflt;
  const v = Number(flags[name]);
  if (!Number.isFinite(v) || v < 0) die(`--${name} must be a number, got "${flags[name]}"`);
  return v;
}

// ---- cluster addresses -------------------------------------------------------------------
// Port formula (scripts/local-cluster.sh): base + (n-1)*10 + 2 is node n's client port.
// Defaults differ on purpose: load tools use the load cluster (17400), kv.mjs the
// local-cluster.sh default (17300).
export const DEFAULT_BASE_PORT = 17400;
export const clientPort = (base, n) => base + (n - 1) * 10 + 2;
export const healthPort = (base, n) => base + (n - 1) * 10 + 4;

// --base-port P wins, then env RETCD_BASE_PORT, then dflt. Throws on a value that is not a port.
export function basePortFrom(flagValue, dflt) {
  const fromEnv = flagValue === undefined;
  const raw = fromEnv ? process.env.RETCD_BASE_PORT : flagValue;
  if (raw === undefined || raw === '') return dflt;
  const v = Number(raw);
  if (!/^\d+$/.test(raw) || v < 1 || v > 65000) {
    throw new Error(`${fromEnv ? 'RETCD_BASE_PORT' : '--base-port'} must be a port number, got "${raw}"`);
  }
  return v;
}

const basePortOrDie = (flags) => {
  try {
    return basePortFrom(flags['base-port'], DEFAULT_BASE_PORT);
  } catch (err) {
    return die(err.message);
  }
};

export const COMMON_FLAGS = ['base-port', 'nodes', 'node', 'addr'];

// Node list from flags: --addr H:P pins one address; else --nodes N (default 3) at --base-port.
export function nodeList(flags) {
  if (flags.addr) return [flags.addr];
  const base = basePortOrDie(flags);
  const count = num(flags, 'nodes', 3);
  const list = [];
  for (let n = 1; n <= count; n++) list.push(`127.0.0.1:${clientPort(base, n)}`);
  if (flags.node) {
    const first = list.indexOf(`127.0.0.1:${clientPort(base, Number(flags.node))}`);
    if (first > 0) list.push(...list.splice(0, first)); // start at --node N, rotate the rest after it
  }
  return list;
}

export async function health(flags, n) {
  const base = basePortOrDie(flags);
  try {
    const r = await fetch(`http://127.0.0.1:${healthPort(base, n)}/health`, { signal: AbortSignal.timeout(1500) });
    return await r.json();
  } catch {
    return null;
  }
}

// ---- Conn: one logical client with its own TCP connection(s) -----------------------------
const metaOf = (err, key) => err.metadata?.get(key)?.[0];
const REFUSED = /ECONNREFUSED|No connection established|Connection refused|connect/i;

export class Conn {
  // nodes: list of host:port. Starts at nodes[0]. Each Conn has its own subchannel pool, so
  // N Conns are N TCP connections (grpc-js would otherwise share one per address).
  constructor(nodes, { name = '' } = {}) {
    this.nodes = nodes;
    this.name = name;
    this.addr = nodes[0];
    this.clients = new Map();
    this.stats = { redirects: 0, rotations: 0, retries: 0 };
    this.onEvent = null; // (text) => void, for reconnect logging
  }

  client(addr = this.addr) {
    let c = this.clients.get(addr);
    if (!c) {
      c = new ConfigService(addr, grpc.credentials.createInsecure(), {
        'grpc.max_receive_message_length': 16 * 1024 * 1024,
        'grpc.max_send_message_length': 16 * 1024 * 1024,
        'grpc.use_local_subchannel_pool': 1,
        'grpc.keepalive_time_ms': 0,
      });
      this.clients.set(addr, c);
    }
    return c;
  }

  use(addr) {
    if (addr !== this.addr) this.onEvent?.(`switch node ${this.addr} -> ${addr}`);
    this.addr = addr;
  }

  rotate() {
    const i = this.nodes.indexOf(this.addr);
    this.stats.rotations++;
    this.use(this.nodes[(i + 1) % this.nodes.length]);
  }

  close() {
    for (const c of this.clients.values()) c.close();
    this.clients.clear();
  }

  once(method, req, deadlineAt) {
    return new Promise((resolve, reject) => {
      this.client()[method](req, { deadline: deadlineAt }, (err, res) => (err ? reject(err) : resolve(res)));
    });
  }

  // One logical call. Follows "not the leader" hints (nothing was applied). A refused connect
  // also means nothing was sent: rotate to the next node. Any other UNAVAILABLE/DEADLINE is
  // retried only for reads (idempotent); for a write it is returned as-is (outcome unknown).
  // Throws an Error with .code (number) and .codeName.
  async call(method, req, { timeoutMs = 10_000, idempotent = false } = {}) {
    const deadlineAt = Date.now() + timeoutMs;
    let hops = 0;
    for (;;) {
      try {
        return await this.once(method, req, deadlineAt);
      } catch (err) {
        err.codeName = STATUS_NAME[err.code] ?? String(err.code);
        if (Date.now() >= deadlineAt - 5) throw err;
        const detail = err.details ?? '';
        if (err.code === grpc.status.FAILED_PRECONDITION && !metaOf(err, 'retcd-conflict-mod-revision')) {
          const hint = metaOf(err, 'retcd-leader-endpoint');
          if (hint && hint !== this.addr) {
            this.stats.redirects++;
            if (!this.nodes.includes(hint)) this.nodes.push(hint);
            this.use(hint);
            if (++hops > 2) await sleep(200);
          } else {
            await sleep(200); // election in progress
            if (!hint) this.rotate();
          }
          continue;
        }
        if (err.code === grpc.status.UNAVAILABLE && REFUSED.test(detail)) {
          this.stats.retries++;
          this.rotate();
          await sleep(100);
          continue;
        }
        if (idempotent && (err.code === grpc.status.UNAVAILABLE || err.code === grpc.status.DEADLINE_EXCEEDED)) {
          this.stats.retries++;
          this.rotate();
          await sleep(100);
          continue;
        }
        throw err;
      }
    }
  }

  // Put returns {outcome, revision}. A CAS conflict is returned as outcome CONFLICT.
  async put(key, value, opts) {
    const r = await this.call('Put', { key: typeof key === 'string' ? b(key) : key, value }, opts);
    return r;
  }
  async get(key, opts) {
    return this.call('Get', { key: typeof key === 'string' ? b(key) : key }, { idempotent: true, ...opts });
  }
  async del(key, opts) {
    return this.call('Delete', { key: typeof key === 'string' ? b(key) : key }, opts);
  }

  // All records under prefix, pinned pagination. Returns {records, readRevision}.
  async listAll(prefix, pageSize = 500) {
    const records = [];
    let token = Buffer.alloc(0);
    let readRevision = '0';
    for (;;) {
      const r = await this.call('List', { prefix: b(prefix), max_items: pageSize, page_token: token }, { idempotent: true });
      readRevision = r.read_revision;
      records.push(...r.records);
      if (r.next_page_token && r.next_page_token.length > 0) token = r.next_page_token;
      else break;
    }
    return { records, readRevision };
  }

  // Find the leader (a Get follows the hint) and pin to it. Returns the address.
  async findLeader(timeoutMs = 20_000) {
    const r = await this.call('Get', { key: b('retlib/none') }, { timeoutMs, idempotent: true });
    return { addr: this.addr, readRevision: r.read_revision };
  }
}

// ---- stats -------------------------------------------------------------------------------
export function pct(sortedAsc, p) {
  if (sortedAsc.length === 0) return NaN;
  return sortedAsc[Math.min(sortedAsc.length - 1, Math.max(0, Math.ceil(p * sortedAsc.length) - 1))];
}

export function summarize(values) {
  const s = Float64Array.from(values).sort();
  return { n: s.length, p50: pct(s, 0.5), p95: pct(s, 0.95), p99: pct(s, 0.99), max: s.length ? s[s.length - 1] : NaN };
}

export const f1 = (x) => (Number.isFinite(x) ? x.toFixed(1) : '-');
export const stamp = () => new Date().toISOString().slice(11, 23);
