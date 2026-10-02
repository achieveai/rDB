#!/usr/bin/env node
// kv: a tiny playground CLI for a local rEtcd cluster (plaintext gRPC, dev cluster only).
// Run `node kv.mjs help` for the commands. See README.md.
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import { basePortFrom, clientPort as clientPortAt, healthPort as healthPortAt } from './retlib.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '..', '..');
const PROTO = path.join(REPO_ROOT, 'proto', 'retcd', 'v1', 'config.proto');

// Limits::DEFAULT in crates/config-core/src/limits.rs: value 1 MiB, key 1 KiB.
const MAX_VALUE_BYTES = 1024 * 1024;
const MAX_KEY_BYTES = 1024;

// local-cluster.sh's default. --base-port P or env RETCD_BASE_PORT changes it (set in main).
const KV_DEFAULT_BASE_PORT = 17300;
let basePort = KV_DEFAULT_BASE_PORT;
const clientPort = (n) => clientPortAt(basePort, n);
const healthPort = (n) => healthPortAt(basePort, n);
const upHint = () =>
  `cluster not running? run: ./scripts/local-cluster.sh up --dir /c/rdb_test_data/local-cluster${
    basePort === KV_DEFAULT_BASE_PORT ? '' : ` --base-port ${basePort}`
  }`;

const STATUS_NAME = Object.fromEntries(Object.entries(grpc.status).map(([k, v]) => [v, k]));

class CliError extends Error {
  constructor(message, exitCode = 1) {
    super(message);
    this.exitCode = exitCode;
  }
}
const fail = (msg, code) => {
  throw new CliError(msg, code);
};
const out = (s = '') => process.stdout.write(`${s}\n`);
const note = (s) => process.stderr.write(`${s}\n`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------------------
// Arguments: `--name value` for the flags below, everything else is positional.
// ---------------------------------------------------------------------------------------
const VALUE_FLAGS = new Set(['node', 'addr', 'base-port', 'if-rev', 'limit', 'from', 'size', 'nodes']);
const BOOL_FLAGS = new Set(['force', 'help']);

function parseArgs(argv) {
  const pos = [];
  const flags = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) {
      pos.push(a);
      continue;
    }
    const name = a.slice(2);
    if (BOOL_FLAGS.has(name)) flags[name] = true;
    else if (VALUE_FLAGS.has(name)) {
      if (i + 1 >= argv.length) fail(`${a} needs a value`);
      flags[name] = argv[++i];
    } else fail(`unknown option ${a}. try: kv help`);
  }
  return { pos, flags };
}

function uintFlag(flags, name, dflt) {
  if (flags[name] === undefined) return dflt;
  if (!/^\d+$/.test(flags[name])) fail(`--${name} must be a whole number, got "${flags[name]}"`);
  return flags[name];
}

// ---------------------------------------------------------------------------------------
// gRPC plumbing
// ---------------------------------------------------------------------------------------
const pkgDef = protoLoader.loadSync(PROTO, {
  keepCase: true,
  longs: String, // uint64 stays exact
  enums: String,
  defaults: true,
  oneofs: true,
});
const ConfigService = grpc.loadPackageDefinition(pkgDef).retcd.v1.ConfigService;

function tcpReachable(addr, ms = 500) {
  const [host, port] = addr.split(':');
  return new Promise((resolve) => {
    const s = net.connect({ host, port: Number(port) });
    const done = (ok) => {
      s.destroy();
      resolve(ok);
    };
    s.setTimeout(ms, () => done(false));
    s.once('connect', () => done(true));
    s.once('error', () => done(false));
  });
}

// --addr wins, then --node N. With neither, use node 1, or the first node that answers.
async function chooseAddr(flags) {
  if (flags.addr) return { addr: flags.addr, explicit: true };
  if (flags.node) {
    if (!/^[1-9]\d*$/.test(flags.node)) fail(`--node must be 1, 2, 3..., got "${flags.node}"`);
    return { addr: `127.0.0.1:${clientPort(Number(flags.node))}`, explicit: true };
  }
  for (const n of [1, 2, 3]) {
    const addr = `127.0.0.1:${clientPort(n)}`;
    if (await tcpReachable(addr)) {
      if (n > 1) note(`(node 1 is not answering, using node ${n})`);
      return { addr, explicit: false };
    }
  }
  return fail(`cannot reach any node on 127.0.0.1:${[1, 2, 3].map(clientPort).join('/')}. ${upHint()}`);
}

// First of nodes 1-3 (other than `skip`) that accepts a TCP connection.
async function firstReachable(skip) {
  for (const n of [1, 2, 3]) {
    const addr = `127.0.0.1:${clientPort(n)}`;
    if (addr !== skip && (await tcpReachable(addr))) return addr;
  }
  return null;
}

const metaOf = (err, key) => err.metadata?.get(key)?.[0];

function describe(err, addr) {
  const detail = err.details || err.message || String(err);
  switch (err.code) {
    case grpc.status.UNAVAILABLE:
      if (/ECONNREFUSED|No connection established|Connection refused|connect/i.test(detail)) {
        return `cannot reach ${addr}. ${upHint()}`;
      }
      return `unavailable (${addr}): ${detail}`;
    case grpc.status.DEADLINE_EXCEEDED:
      return `timed out talking to ${addr}. For a write, the result is unknown: check with kv get.`;
    case grpc.status.RESOURCE_EXHAUSTED:
      return `server refused (too big or over a limit): ${detail}`;
    case grpc.status.INVALID_ARGUMENT:
      return `bad request: ${detail}`;
    case grpc.status.OUT_OF_RANGE: {
      const min = metaOf(err, 'retcd-min-revision');
      return `that revision was compacted away${min ? `; oldest available is ${min}` : ''}`;
    }
    default:
      return `${STATUS_NAME[err.code] ?? 'error'}: ${detail}`;
  }
}

class Session {
  constructor(addr, roaming = false) {
    this.roaming = roaming; // true when no --node/--addr was given
    this.viaHint = false;
    this.use(addr);
  }

  use(addr) {
    this.client?.close();
    this.addr = addr;
    this.client = new ConfigService(addr, grpc.credentials.createInsecure(), {
      'grpc.max_receive_message_length': 16 * 1024 * 1024,
      'grpc.max_send_message_length': 16 * 1024 * 1024,
    });
  }

  close() {
    this.client?.close();
  }

  once(method, req) {
    return new Promise((resolve, reject) => {
      this.client[method](req, { deadline: Date.now() + 10_000 }, (err, res) =>
        err ? reject(err) : resolve(res),
      );
    });
  }

  // One call. A follower answers "not the leader" (nothing applied), so it is safe to follow
  // the leader hint, or to wait briefly while an election finishes, and send again. A refused
  // connection means nothing was sent either, so we may move to another node (unless you
  // pinned a node with --node/--addr and did not get here through a leader hint).
  async call(method, req) {
    const giveUpAt = Date.now() + 20_000;
    let hops = 0;
    for (;;) {
      try {
        return await this.once(method, req);
      } catch (err) {
        if (metaOf(err, 'retcd-conflict-mod-revision')) throw wrap(err, this.addr);
        const refused =
          err.code === grpc.status.UNAVAILABLE && /ECONNREFUSED|No connection established|Connection refused/i.test(err.details ?? '');
        if (refused && (this.roaming || this.viaHint) && Date.now() < giveUpAt) {
          note(`(${this.addr} is down. looking for another node...)`);
          await sleep(700);
          const next = await firstReachable(this.addr);
          if (!next) throw wrap(err, this.addr);
          this.use(next);
          this.viaHint = false;
          continue;
        }
        if (err.code !== grpc.status.FAILED_PRECONDITION) throw wrap(err, this.addr);
        const hint = metaOf(err, 'retcd-leader-endpoint');
        if (hint && hint !== this.addr && Date.now() < giveUpAt) {
          note(`(not the leader. using leader at ${hint})`);
          if (++hops > 2) await sleep(400); // a hint loop must not spin
          this.use(hint);
          this.viaHint = true;
          continue;
        }
        if (Date.now() < giveUpAt && (!hint || hint === this.addr)) {
          note('(no leader yet, waiting...)');
          await sleep(700);
          continue;
        }
        throw wrap(err, this.addr);
      }
    }
  }
}

function wrap(err, addr) {
  if (err instanceof CliError) return err;
  const e = new CliError(describe(err, addr));
  e.grpc = err;
  return e;
}

// ---------------------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------------------
const b = (s) => Buffer.from(s, 'utf8');
const q = (buf) => `"${Buffer.from(buf).toString('utf8')}"`;
const hex = (buf, n = 32) =>
  Buffer.from(buf).subarray(0, n).toString('hex') + (buf.length > n ? '...' : '');

function asText(buf) {
  try {
    const s = new TextDecoder('utf-8', { fatal: true }).decode(buf);
    // eslint-disable-next-line no-control-regex
    return /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(s) ? null : s;
  } catch {
    return null;
  }
}

function checkKey(key) {
  if (!key) fail('key is empty');
  if (b(key).length > MAX_KEY_BYTES) fail(`key is ${b(key).length} bytes; the server limit is ${MAX_KEY_BYTES}`);
}

function checkValueSize(n) {
  if (n > MAX_VALUE_BYTES) {
    fail(`value is ${n} bytes. The server limit is ${MAX_VALUE_BYTES} bytes (1 MiB). Not sent.`);
  }
}

// put/delete. A conflict can arrive as an OK response or as a typed error; both land here.
async function mutate(session, method, req) {
  try {
    const r = await session.call(method, req);
    return { outcome: r.outcome, revision: r.revision, current: r.current_mod_revision, exists: r.exists };
  } catch (err) {
    const cur = err.grpc && metaOf(err.grpc, 'retcd-conflict-mod-revision');
    if (cur !== undefined) {
      return { outcome: 'CONFLICT', current: cur, exists: metaOf(err.grpc, 'retcd-conflict-exists') === 'true' };
    }
    throw err;
  }
}

function printConflict(what, ifRev, r) {
  out(`REFUSED: ${what} is not at revision ${ifRev}.`);
  out(r.exists === false ? '         It does not exist right now.' : `         Its current revision is ${r.current}.`);
  out(`         Retry with: --if-rev ${r.current}`);
  process.exitCode = 2;
}

// ---------------------------------------------------------------------------------------
// Patterns (ls, watch). The server only knows prefixes, so the literal part of a pattern
// goes to the server and the rest is matched here.
//   *  within one segment (never crosses /)     **  any depth, zero included
//   ?  one char except /                        [abc] [a-z] [!abc]  one char from a set
// ---------------------------------------------------------------------------------------
const GLOB_CHARS = /[*?[]/;
const hasGlob = (p) => GLOB_CHARS.test(p);
const literalPrefix = (p) => (hasGlob(p) ? p.slice(0, p.search(GLOB_CHARS)) : p);

function globToRegExp(pattern) {
  let re = '';
  for (let i = 0; i < pattern.length; i++) {
    const c = pattern[i];
    if (c === '*') {
      if (pattern[i + 1] === '*') {
        while (pattern[i + 1] === '*') i++;
        if (pattern[i + 1] === '/') {
          i++;
          re += '(?:[\\s\\S]*/)?'; // "**/" also matches no directory at all
        } else re += '[\\s\\S]*';
      } else re += '[^/]*';
    } else if (c === '?') re += '[^/]';
    else if (c === '[') {
      const close = pattern.indexOf(']', i + 2); // "[]" and "[!]" are not sets: a literal [
      if (close === -1) re += '\\[';
      else {
        let set = pattern.slice(i + 1, close);
        const neg = set[0] === '!' || set[0] === '^';
        if (neg) set = set.slice(1);
        re += `(?!/)[${neg ? '^' : ''}${set.replace(/[\\[\]^]/g, '\\$&')}]`;
        i = close;
      }
    } else re += c.replace(/[.+^${}()|\\\]/]/g, '\\$&');
  }
  return new RegExp(`^${re}$`);
}

// ---------------------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------------------
async function cmdPut(s, pos, flags) {
  if (pos.length !== 2) fail('usage: kv put <key> <value> [--if-rev N]');
  const [key, value] = pos;
  checkKey(key);
  checkValueSize(b(value).length);
  const req = { key: b(key), value: b(value) };
  if (flags['if-rev'] !== undefined) req.expected_mod_revision = uintFlag(flags, 'if-rev');
  const r = await mutate(s, 'Put', req);
  if (r.outcome === 'CONFLICT') return printConflict(q(key), flags['if-rev'], r);
  if (r.outcome !== 'APPLIED') fail(`unexpected outcome ${r.outcome}`);
  out(`OK: stored ${q(key)}`);
  out(`    revision ${r.revision}`);
}

async function cmdGet(s, pos) {
  if (pos.length !== 1) fail('usage: kv get <key>');
  const [key] = pos;
  checkKey(key);
  const r = await s.call('Get', { key: b(key) });
  if (!r.record) {
    out(`not found: ${q(key)}   (cluster at revision ${r.read_revision})`);
    process.exitCode = 1;
    return;
  }
  const { value } = r.record;
  const text = asText(value);
  if (text === null) {
    out(`${key} = <binary, ${value.length} bytes>`);
    out(`    hex: ${hex(value)}`);
  } else if (text.includes('\n') || text.length > 2000) {
    out(`${key} =`);
    out(text.length > 2000 ? `${text.slice(0, 2000)}\n... (${value.length} bytes in all)` : text);
  } else {
    out(`${key} = ${text}`);
  }
  out(`    created at revision  ${r.record.create_revision}`);
  out(`    modified at revision ${r.record.mod_revision}`);
  out(`    size ${value.length} bytes`);
}

// Walks every page of List under `prefix`. onRecord sees each record. Returns the facts.
async function walkList(s, prefix, limit, onRecord) {
  let pages = 0;
  let scanned = 0;
  let token = Buffer.alloc(0); // present but empty: start a pinned walk
  let readRev = '0';
  for (;;) {
    const r = await s.call('List', { prefix: b(prefix), max_items: limit, page_token: token });
    pages++;
    readRev = r.read_revision;
    for (const rec of r.records) {
      scanned++;
      onRecord(rec);
    }
    if (r.next_page_token && r.next_page_token.length > 0) {
      token = r.next_page_token;
      continue;
    }
    if (r.truncated) note('(list was cut short by a server size cap)');
    break;
  }
  return { pages, scanned, readRev };
}

function printTable(entries) {
  const w = Math.max(3, ...entries.map((e) => e.name.length));
  out(`${'KEY'.padEnd(w)}  ${'SIZE'.padStart(8)}  ${'MOD_REV'.padStart(8)}`);
  for (const e of entries) out(`${e.name.padEnd(w)}  ${e.size.padStart(8)}  ${e.rev.padStart(8)}`);
  out();
}

const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;

// Pattern listing. Keys that match are rows. With no "**", a key that sits deeper than the
// pattern but under a directory the pattern matches adds one DIR row for that directory.
async function lsPattern(s, pattern, limit) {
  const prefix = literalPrefix(pattern);
  if (!prefix) note('scanning all keys');
  const re = globToRegExp(pattern);
  const wantDirs = !pattern.includes('**');
  const entries = []; // server (sorted) order; a DIR row sits where its first key was
  const dirs = new Map();
  let keyRows = 0;
  const { scanned, readRev } = await walkList(s, prefix, limit, (rec) => {
    const key = rec.key.toString('utf8');
    if (re.test(key)) {
      keyRows++;
      entries.push({ name: key, size: String(rec.value.length), rev: String(rec.mod_revision) });
    }
    if (!wantDirs) return;
    for (let i = key.indexOf('/'); i !== -1; i = key.indexOf('/', i + 1)) {
      const dir = key.slice(0, i);
      if (!re.test(dir)) continue;
      let d = dirs.get(dir);
      if (!d) {
        d = { name: `${dir}/`, size: 'DIR', rev: '', keys: 0 };
        dirs.set(dir, d);
        entries.push(d);
      }
      d.keys++;
    }
  });
  for (const d of dirs.values()) d.rev = plural(d.keys, 'key');
  const what = `${plural(keyRows, 'key')}${dirs.size ? ` + ${plural(dirs.size, 'dir')}` : ''}`;
  const where = `scanned ${plural(scanned, 'key')}${prefix ? ` under '${prefix}'` : ''}, cluster at revision ${readRev}`;
  if (entries.length > 0) printTable(entries);
  out(`${what} matched '${pattern}' (${where})`);
}

async function cmdLs(s, pos, flags) {
  if (pos.length > 1) fail("usage: kv ls [prefix or 'pattern'] [--limit N]");
  const prefix = pos[0] ?? '';
  const limit = Number(uintFlag(flags, 'limit', '100'));
  if (limit < 1) fail('--limit must be at least 1');
  if (hasGlob(prefix)) return lsPattern(s, prefix, limit);
  const rows = [];
  const { pages, readRev } = await walkList(s, prefix, limit, (rec) => rows.push(rec));
  if (rows.length === 0) {
    out(`no keys${prefix ? ` under ${q(prefix)}` : ''}   (cluster at revision ${readRev})`);
    return;
  }
  printTable(rows.map((x) => ({ name: x.key.toString('utf8'), size: String(x.value.length), rev: String(x.mod_revision) })));
  out(`${rows.length} key${rows.length === 1 ? '' : 's'} in ${pages} page${pages === 1 ? '' : 's'} of up to ${limit}   (cluster at revision ${readRev})`);
}

async function cmdRm(s, pos, flags) {
  if (pos.length !== 1) fail('usage: kv rm <key> [--if-rev N]');
  const [key] = pos;
  checkKey(key);
  const req = { key: b(key) };
  if (flags['if-rev'] !== undefined) req.expected_mod_revision = uintFlag(flags, 'if-rev');
  const r = await mutate(s, 'Delete', req);
  if (r.outcome === 'CONFLICT') return printConflict(q(key), flags['if-rev'], r);
  if (r.outcome === 'NOT_FOUND') {
    out(`not found: ${q(key)}   (nothing removed)`);
    process.exitCode = 1;
    return;
  }
  if (r.outcome !== 'APPLIED') fail(`unexpected outcome ${r.outcome}`);
  out(`OK: removed ${q(key)}`);
  out(`    revision ${r.revision}`);
}

// After this many failed reconnects in a row, a watch pinned with --node/--addr gives up on
// that node and looks for the leader on the cluster's nodes (from the base port).
const WATCH_PIN_TRIES = 3;

// Move the session to the leader: start at a node other than `avoid` that answers, then let a
// read follow the "not the leader" hint. Returns false if no node answers yet.
async function findLeader(s, avoid) {
  const start = (await firstReachable(avoid)) ?? (await firstReachable(null));
  if (!start) return false;
  if (start !== s.addr) s.use(start);
  s.roaming = true;
  try {
    await s.call('Get', { key: b('kv/none') });
    return true;
  } catch {
    return false; // election still running; the caller tries again
  }
}

// Watch reconnects by itself (leader change, node stopped) and resumes after the last
// revision it saw, so you can stop a node and keep watching. Pinned with --node/--addr, it
// retries that node WATCH_PIN_TRIES times, then follows the leader like an unpinned watch.
async function cmdWatch(s, pos, flags, choice) {
  if (pos.length > 1) fail("usage: kv watch [prefix or 'pattern'] [--from REV]");
  const arg = pos[0] ?? '';
  const prefix = literalPrefix(arg);
  const re = hasGlob(arg) ? globToRegExp(arg) : null; // events that do not match are dropped here
  let last;
  if (flags.from !== undefined) last = uintFlag(flags, 'from');
  else last = (await s.call('Get', { key: b('kv/none') })).read_revision; // "from now"
  out(`watching ${arg ? q(arg) : 'everything'} after revision ${last}. Ctrl-C to stop.`);
  out();

  let call = null;
  let stopping = false;
  process.once('SIGINT', () => {
    stopping = true;
    call?.cancel();
  });

  let pinned = choice.explicit;
  let failures = 0; // reconnects in a row with no message received
  while (!stopping) {
    const done = await new Promise((resolve) => {
      call = s.client.Watch({ prefix: b(prefix), start_after_revision: last });
      call.on('data', (m) => {
        failures = 0;
        if (m.body === 'event') {
          const ev = m.event;
          if (BigInt(ev.revision) <= BigInt(last)) note(`(warning: rev ${ev.revision} is not after ${last}: a duplicate or out of order)`);
          last = ev.revision;
          if (re && !re.test(ev.key.toString('utf8'))) return;
          if (ev.change === 'put') out(`rev ${ev.revision}  PUT  ${ev.key.toString('utf8')}  (${ev.put.value.length} bytes)`);
          else out(`rev ${ev.revision}  DEL  ${ev.key.toString('utf8')}`);
        } else if (m.body === 'progress') {
          last = m.progress.revision; // quiet heartbeat: no key data
        }
      });
      call.on('error', (err) => resolve({ err }));
      call.on('end', () => resolve({ ended: true }));
    });
    if (stopping) break;
    const err = done.err;
    if (err && err.code === grpc.status.CANCELLED) break;
    const retryable =
      done.ended ||
      err.code === grpc.status.UNAVAILABLE ||
      (err.code === grpc.status.FAILED_PRECONDITION && /leader/i.test(err.details ?? ''));
    if (!retryable) fail(describe(err, s.addr));
    failures++;
    const hint = err && metaOf(err, 'retcd-leader-endpoint');
    const why = err ? `${STATUS_NAME[err.code] ?? err.code} ${err.details ?? ''}`.trim() : 'stream ended';
    note(`(watch on ${s.addr} lost: ${why}. reconnecting after revision ${last}...)`);
    await sleep(1000);
    if (hint) {
      s.use(hint);
      continue;
    }
    if (pinned && failures < WATCH_PIN_TRIES) continue;
    if (pinned) {
      note(`(${s.addr} failed ${failures} times. looking for the leader on the other nodes...)`);
      pinned = false;
    }
    const failed = s.addr;
    if (await findLeader(s, failed)) {
      if (s.addr !== failed) note(`(resuming the watch on ${s.addr})`);
    }
  }
  out();
  out(`stopped. last revision seen: ${last}`);
}

const sha256 = (buf) => createHash('sha256').update(buf).digest('hex');

async function cmdPutFile(s, pos) {
  if (pos.length < 1 || pos.length > 2) fail('usage: kv putfile <path> [key]');
  const [file, keyArg] = pos;
  let st;
  try {
    st = fs.statSync(file);
  } catch (err) {
    fail(`cannot read ${file}: ${err.code ?? err.message}`);
  }
  if (!st.isFile()) fail(`${file} is not a file`);
  if (st.size > MAX_VALUE_BYTES) {
    fail(`${file} is ${st.size} bytes. The server limit is ${MAX_VALUE_BYTES} bytes (1 MiB) per value. Not sent.`);
  }
  const data = fs.readFileSync(file);
  const key = keyArg ?? `files/${path.basename(file)}`;
  const metaKey = `meta/${key}`;
  checkKey(key);
  checkKey(metaKey);
  const r = await mutate(s, 'Put', { key: b(key), value: data });
  if (r.outcome !== 'APPLIED') fail(`unexpected outcome ${r.outcome}`);
  // A hand-made "index": one small JSON record next to the file. Not a database feature,
  // and not atomic with the file put (no multi-key transactions).
  const meta = { size: data.length, sha256: sha256(data), stored_at_rev: Number(r.revision) };
  const m = await mutate(s, 'Put', { key: b(metaKey), value: b(JSON.stringify(meta)) });
  if (m.outcome !== 'APPLIED') fail(`file stored, but meta record failed: ${m.outcome}`);
  out(`OK: stored ${q(key)}`);
  out(`    ${data.length} bytes, revision ${r.revision}`);
  out(`    sha256 ${meta.sha256}`);
  out(`    meta record ${q(metaKey)} at revision ${m.revision}`);
}

async function cmdGetFile(s, pos, flags) {
  if (pos.length !== 2) fail('usage: kv getfile <key> <outpath> [--force]');
  const [key, outPath] = pos;
  checkKey(key);
  if (fs.existsSync(outPath) && !flags.force) fail(`${outPath} already exists. Use --force to overwrite.`);
  const r = await s.call('Get', { key: b(key) });
  if (!r.record) fail(`not found: ${q(key)}`);
  const data = r.record.value;
  const digest = sha256(data);
  const mr = await s.call('Get', { key: b(`meta/${key}`) });
  let verdict;
  if (mr.record) {
    let meta;
    try {
      meta = JSON.parse(mr.record.value.toString('utf8'));
    } catch {
      fail(`meta record for ${q(key)} is not valid JSON. File not written.`);
    }
    if (meta.sha256 !== digest || meta.size !== data.length) {
      fail(`CHECK FAILED: stored ${data.length} bytes / sha256 ${digest}, meta says ${meta.size} bytes / ${meta.sha256}. File not written.`);
    }
    verdict = 'sha256 matches the meta record';
  } else {
    verdict = 'no meta record, so sha256 was not checked';
  }
  fs.writeFileSync(outPath, data);
  out(`OK: wrote ${outPath}`);
  out(`    ${data.length} bytes, revision ${r.record.mod_revision}`);
  out(`    sha256 ${digest}`);
  out(`    ${verdict}`);
}

async function cmdStatus(flags) {
  const count = Number(uintFlag(flags, 'nodes', '3'));
  const rows = [];
  for (let n = 1; n <= count; n++) {
    const url = `http://127.0.0.1:${healthPort(n)}/health`;
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(1500) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      rows.push({ n, h: await res.json() });
    } catch {
      rows.push({ n, h: null });
    }
  }
  out(`${'NODE'.padEnd(5)}${'STATE'.padEnd(11)}${'ROLE'.padEnd(11)}${'LEADER'.padEnd(8)}${'REV'.padEnd(8)}HASH`);
  for (const { n, h } of rows) {
    if (!h) {
      out(`${String(n).padEnd(5)}${'DOWN'.padEnd(11)}-`);
      continue;
    }
    const state = h.ready ? 'ready' : 'not ready';
    const leader = h.current_leader == null ? '-' : String(h.current_leader);
    out(`${String(n).padEnd(5)}${state.padEnd(11)}${String(h.role).padEnd(11)}${leader.padEnd(8)}${String(h.cluster_revision).padEnd(8)}${String(h.state_hash_hex).slice(0, 12)}`);
  }
  const up = rows.filter((r) => r.h);
  const ready = up.filter((r) => r.h.ready);
  const leaders = new Set(ready.map((r) => r.h.current_leader).filter((l) => l != null));
  const hashes = new Set(ready.map((r) => r.h.state_hash_hex));
  out();
  out(`${ready.length} of ${count} ready. leader: ${leaders.size === 1 ? `node ${[...leaders][0]}` : leaders.size === 0 ? 'none yet' : 'nodes disagree'}.`);
  if (ready.length > 1) out(`same data on all ready nodes: ${hashes.size === 1 ? 'yes' : 'not yet (a follower may lag for a moment)'}`);
  if (up.length === 0) out(upHint());
  if (ready.length === 0) process.exitCode = 1;
}

async function cmdBench(s, pos, flags) {
  const n = Number(pos[0] ?? '100');
  const size = Number(uintFlag(flags, 'size', '64'));
  if (!Number.isInteger(n) || n < 1) fail('usage: kv bench [n] [--size bytes]   (n is a whole number >= 1)');
  checkValueSize(size);
  const value = Buffer.alloc(size, 0x78);
  const ms = [];
  out(`bench: ${n} sequential puts of ${size} bytes, one client, keys bench/000001...`);
  const t0 = performance.now();
  for (let i = 1; i <= n; i++) {
    const t = performance.now();
    try {
      await s.call('Put', { key: b(`bench/${String(i).padStart(6, '0')}`), value });
    } catch (err) {
      fail(`${err.message} (stopped after ${ms.length} of ${n} puts)`);
    }
    ms.push(performance.now() - t);
  }
  const total = performance.now() - t0;
  const sorted = [...ms].sort((x, y) => x - y);
  const pct = (p) => sorted[Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1)];
  out();
  out(`  total  ${(total / 1000).toFixed(2)} s`);
  out(`  rate   ${(n / (total / 1000)).toFixed(0)} ops/sec`);
  out(`  p50    ${pct(0.5).toFixed(1)} ms`);
  out(`  p99    ${pct(0.99).toFixed(1)} ms`);
  out(`  max    ${sorted[sorted.length - 1].toFixed(1)} ms`);
  out();
  out('one client, one put at a time, each waits for a majority to commit.');
  out('dev cluster, debug build: for a feel, not a capacity test.');
  out('the keys stay under bench/ (kv ls bench/).');
}

const HELP = `kv: play with a local rEtcd cluster

  kv put <key> <value> [--if-rev N]   store a value (--if-rev = only if unchanged)
  kv get <key>                        read a value and its revisions
  kv ls [prefix|pattern] [--limit N]  list keys (N = page size, default 100)
  kv rm <key> [--if-rev N]            delete a key
  kv watch [prefix|pattern] [--from REV]  stream changes until Ctrl-C
  kv putfile <path> [key]             store a file (max 1 MiB) + a meta record
  kv getfile <key> <outpath>          fetch a file, check sha256 (--force to overwrite)
  kv status                           health of each node (--nodes N, default 3)
  kv bench [n] [--size bytes]         n sequential puts, prints ops/sec, p50, p99

  --node N       talk to node N (1 -> base+2, 2 -> base+12, ...)
  --addr H:P     talk to this client address
  default        node 1, or the first of nodes 1-3 that answers
  --base-port P  where the cluster is (or env RETCD_BASE_PORT). default 17300,
                 same as local-cluster.sh. Node n's client port is P + (n-1)*10 + 2.

  patterns (ls, watch): * one level, ** any depth, ? one char, [a-z] a set
    kv ls 'docs/*'  keys + one DIR row per sub-folder     kv ls 'docs/**/*.md'  every depth
    QUOTE THE PATTERN in Git Bash/PowerShell, or the shell expands it against local files.
    No * ? [ in the argument = plain prefix. Matching is done here; the server lists prefixes only.
`;

async function main() {
  const { pos, flags } = parseArgs(process.argv.slice(2));
  try {
    basePort = basePortFrom(flags['base-port'], KV_DEFAULT_BASE_PORT);
  } catch (err) {
    fail(err.message, 2);
  }
  const cmd = pos.shift();
  if (!cmd || cmd === 'help' || cmd === '-h' || flags.help) {
    out(HELP);
    return;
  }
  if (cmd === 'status') return cmdStatus(flags);
  const cmds = { put: cmdPut, get: cmdGet, ls: cmdLs, rm: cmdRm, watch: cmdWatch, putfile: cmdPutFile, getfile: cmdGetFile, bench: cmdBench };
  if (!cmds[cmd]) fail(`unknown command "${cmd}". try: kv help`);
  const choice = await chooseAddr(flags);
  const session = new Session(choice.addr, !choice.explicit);
  try {
    await cmds[cmd](session, pos, flags, choice);
  } finally {
    session.close();
  }
}

main().catch((err) => {
  const e = err instanceof CliError ? err : new CliError(err.stack ?? String(err));
  note(`error: ${e.message}`);
  process.exitCode = e.exitCode;
});
