#!/usr/bin/env node
// load.mjs: hammer a local rEtcd cluster with reads and writes. See LOAD-TESTING.txt.
//
//   node load.mjs write  --clients 16 --duration 20
//   node load.mjs read   --clients 16 --duration 20
//   node load.mjs mixed  --clients 16 --duration 20 --read-pct 90
//   node load.mjs big    --clients 4  --duration 20 --size 524288
//   node load.mjs ramp   --workload mixed --max-clients 64 --p99-ms 200
//
// Closed loop: each client has its own TCP connection and sends the next request when the
// last one is answered. Every acked write is remembered; at the end each acked key is read
// back and checked (a lost or wrong acked write is reported, and must be 0).
// One node process tops out near 3-4k ops/s, so big runs fork helper processes (--procs).
import { fork } from 'node:child_process';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import {
  COMMON_FLAGS, MAX_VALUE_BYTES, Conn, die, f1, nodeList, now, num, parseArgs, sleep, summarize,
} from './retlib.mjs';

const HELP = `usage: node load.mjs <write|read|mixed|big|ramp> [flags]

  --clients N      parallel clients, each its own connection (default 16; big: 4)
  --duration S     seconds to measure (default 10)
  --size BYTES     value size (default 256; big: 262144; max ${MAX_VALUE_BYTES})
  --keys N         keyspace size (default 1000; big: 40)
  --prefix P       key prefix (default load/)
  --read-pct N     mixed: percent reads (default 90)
  --node N | --addr H:P | --base-port P   where the cluster is (default base port 17400, 3 nodes)
  --op-timeout S   per-operation deadline incl. retries (default 10)
  --procs P        client processes (default auto: 1 per 8 clients, max 6). One node process
                   tops out near 3-4k ops/s, so more clients need more processes.
  --out FILE       write a JSON result
  --no-verify      skip the read-back of every acked write
  --no-seed        read/mixed: do not pre-write the keyspace
  --quiet          no per-second lines
  ramp only:  --workload write|read|mixed (default mixed)  --max-clients N (default 64)
              --p99-ms N (default 250)  --step S (seconds per step, default 8)
`;

const SPEC = {
  value: [...COMMON_FLAGS, 'clients', 'duration', 'size', 'keys', 'prefix', 'read-pct', 'out', 'op-timeout', 'workload', 'max-clients', 'p99-ms', 'step', 'procs', 'run-id'],
  bool: ['verify', 'no-verify', 'no-seed', 'quiet', 'help'],
};

const { pos, flags } = parseArgs(process.argv.slice(2), SPEC);
const IS_CHILD = typeof process.send === 'function' && process.env.LOAD_CHILD === '1';
const mode = pos[0];
if (!IS_CHILD && (!mode || flags.help || !['write', 'read', 'mixed', 'big', 'ramp'].includes(mode))) {
  process.stdout.write(HELP);
  process.exit(mode && !flags.help ? 2 : 0);
}

const nodes = nodeList(flags);
const RUN_ID = flags['run-id'] ?? Date.now().toString(36);
const prefix = flags.prefix ?? 'load/';
const keyOf = (i) => `${prefix}${String(i).padStart(6, '0')}`;
const opTimeoutMs = num(flags, 'op-timeout', 10) * 1000;

function makeValue(size, header) {
  const buf = Buffer.allocUnsafe(size);
  buf.fill(0x78);
  buf.write(header.slice(0, size), 0, 'latin1');
  return buf;
}

// ---- one measured run, in this process ---------------------------------------------------
// acked: Map key -> {rev, header, size}. barrier: awaited after warm-up (children wait for "go").
async function runOnce(cfg, acked, conns, { clientOffset = 0, barrier = null, onSecond = null, raw = false } = {}) {
  const { workload, clients, duration, size, keys, readPct, quiet } = cfg;
  while (conns.length < clients) conns.push(new Conn(nodes.slice(), { name: `c${conns.length}` }));
  // Warm up: connect and find the leader outside the measurement.
  await Promise.all(conns.slice(0, clients).map((c) => c.findLeader().catch(() => {})));
  if (barrier) await barrier();

  const lat = { write: [], read: [] };
  const errors = {};
  const timeline = []; // per second: {ops, err}
  let ops = 0;
  let errs = 0;
  const startAt = now();
  const stopAt = startAt + duration * 1000;
  const cpu0 = process.cpuUsage();
  let seq = 0;
  let lastOk = startAt;
  let gapMs = 0;
  let gapStart = 0;

  const bump = (isErr) => {
    const tn = now();
    const sec = Math.floor((tn - startAt) / 1000);
    const t = (timeline[sec] ??= { ops: 0, err: 0 });
    if (isErr) t.err++;
    else {
      t.ops++;
      if (tn - lastOk > gapMs) {
        gapMs = tn - lastOk;
        gapStart = lastOk - startAt;
      }
      lastOk = tn;
    }
  };

  let printed = 0;
  const emit = (sec) => {
    const t = timeline[sec] ?? { ops: 0, err: 0 };
    if (onSecond) onSecond(sec, t.ops, t.err);
    else process.stdout.write(`  t=${String(sec + 1).padStart(3)}s  ops/s=${String(t.ops).padStart(6)}  errors=${t.err}\n`);
  };
  const ticker = quiet && !onSecond
    ? null
    : setInterval(() => {
        const done = Math.floor((now() - startAt) / 1000);
        for (; printed < done; printed++) emit(printed);
      }, 250);

  async function client(idx) {
    const conn = conns[idx];
    while (now() < stopAt) {
      const isRead = workload === 'read' || (workload === 'mixed' && Math.random() * 100 < readPct);
      const key = keyOf(Math.floor(Math.random() * keys));
      const t0 = now();
      try {
        if (isRead) {
          const r = await conn.get(key, { timeoutMs: opTimeoutMs });
          if (!r.record) throw Object.assign(new Error('missing'), { codeName: 'MISSING_KEY' });
          lat.read.push(now() - t0);
        } else {
          const header = `${RUN_ID}.${clientOffset + idx}.${seq++}|`;
          const r = await conn.put(key, makeValue(size, header), { timeoutMs: opTimeoutMs });
          if (r.outcome !== 'APPLIED') throw Object.assign(new Error(r.outcome), { codeName: `OUTCOME_${r.outcome}` });
          lat.write.push(now() - t0);
          const rev = Number(r.revision);
          const prev = acked.get(key);
          if (!prev || rev > prev.rev) acked.set(key, { rev, header, size });
        }
        ops++;
        bump(false);
      } catch (e) {
        const name = e.codeName ?? 'ERROR';
        errors[name] = (errors[name] ?? 0) + 1;
        errs++;
        bump(true);
        if (errs <= 3) process.stderr.write(`  (first errors) ${name}: ${e.details ?? e.message}\n`);
        await sleep(20); // do not spin on a dead cluster
      }
    }
  }

  await Promise.all(Array.from({ length: clients }, (_, i) => client(i)));
  if (ticker) {
    clearInterval(ticker);
    for (; printed < Math.ceil(duration); printed++) emit(printed);
  }
  const elapsed = (now() - startAt) / 1000;
  const cpu = process.cpuUsage(cpu0);
  if (now() - lastOk > gapMs) {
    gapMs = now() - lastOk;
    gapStart = lastOk - startAt;
  }
  return {
    ops, errors, errs, elapsed, gapMs, gapStartSec: gapStart / 1000,
    redirects: conns.slice(0, clients).reduce((a, c) => a + c.stats.redirects, 0),
    cpuPct: ((cpu.user + cpu.system) / 1000 / (elapsed * 1000)) * 100,
    timelineOps: timeline.map((t) => t?.ops ?? 0),
    timelineErr: timeline.map((t) => t?.err ?? 0),
    writeLat: raw ? Float64Array.from(lat.write) : lat.write,
    readLat: raw ? Float64Array.from(lat.read) : lat.read,
  };
}

// ---- turn raw parts into the result record -----------------------------------------------
function buildResult(cfg, parts, cpuPct) {
  const { workload, clients, size, keys } = cfg;
  const ops = parts.reduce((a, p) => a + p.ops, 0);
  const errs = parts.reduce((a, p) => a + p.errs, 0);
  const elapsed = Math.max(...parts.map((p) => p.elapsed));
  const errors = {};
  for (const p of parts) for (const [k, v] of Object.entries(p.errors)) errors[k] = (errors[k] ?? 0) + v;
  const cat = (key) => parts.flatMap((p) => Array.from(p[key]));
  const wl = cat('writeLat');
  const rl = cat('readLat');
  const len = Math.max(...parts.map((p) => p.timelineOps.length));
  const timeline = Array.from({ length: len }, (_, i) => parts.reduce((a, p) => a + (p.timelineOps[i] ?? 0), 0));
  return {
    workload, clients, size, keys, duration: Number(elapsed.toFixed(2)), procs: parts.length,
    ops, errors, errorTotal: errs,
    errorRatePct: ops + errs ? (100 * errs) / (ops + errs) : 0,
    opsPerSec: ops / elapsed,
    mbPerSec: ((wl.length + rl.length) * size) / elapsed / 1e6,
    latencyMs: summarize([...wl, ...rl]),
    writeLatencyMs: summarize(wl),
    readLatencyMs: summarize(rl),
    longestSilenceMs: Math.max(...parts.map((p) => p.gapMs)),
    longestSilenceAtSec: parts.reduce((a, p) => (p.gapMs >= a.g ? { g: p.gapMs, at: p.gapStartSec } : a), { g: -1, at: 0 }).at,
    clientCpuCores: cpuPct / 100,
    clientCpuPctPerProc: cpuPct / parts.length,
    redirects: parts.reduce((a, p) => a + p.redirects, 0),
    timeline,
  };
}

// ---- run across P processes ---------------------------------------------------------------
function autoProcs(clients) {
  if (flags.procs !== undefined) return Math.max(1, Math.min(num(flags, 'procs', 1), clients));
  return Math.max(1, Math.min(6, Math.ceil(clients / 8)));
}

const mergeAcked = (acked, entries) => {
  for (const [k, a] of entries) {
    const prev = acked.get(k);
    if (!prev || a.rev > prev.rev) acked.set(k, a);
  }
};

async function run(cfg, acked, conns) {
  const procs = autoProcs(cfg.clients);
  if (procs <= 1) {
    const part = await runOnce(cfg, acked, conns);
    return buildResult(cfg, [part], part.cpuPct);
  }
  // Fork children; each owns a slice of the clients. Barrier so all start together.
  const per = Array.from({ length: procs }, (_, i) => Math.floor(cfg.clients / procs) + (i < cfg.clients % procs ? 1 : 0));
  let offset = 0;
  const kids = per.map((n, i) => {
    const cp = fork(fileURLToPath(import.meta.url), [...process.argv.slice(2), '--run-id', RUN_ID], {
      serialization: 'advanced',
      env: { ...process.env, LOAD_CHILD: '1' },
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
    });
    const kid = { cp, n, offset, i, ready: null, done: null };
    kid.ready = new Promise((res) => (kid.resolveReady = res));
    kid.done = new Promise((res, rej) => {
      kid.resolveDone = res;
      cp.on('exit', (c) => rej(new Error(`load child ${i} exited with ${c} before reporting`)));
    });
    offset += n;
    return kid;
  });
  const tick = new Map(); // sec -> {ops, err, n}
  let printedSec = 0;
  for (const k of kids) {
    k.cp.on('message', (m) => {
      if (m.type === 'ready') k.resolveReady();
      else if (m.type === 'tick') {
        const t = tick.get(m.sec) ?? { ops: 0, err: 0, n: 0 };
        t.ops += m.ops;
        t.err += m.err;
        t.n++;
        tick.set(m.sec, t);
        while (!cfg.quiet && tick.get(printedSec)?.n === procs) {
          const x = tick.get(printedSec);
          process.stdout.write(`  t=${String(printedSec + 1).padStart(3)}s  ops/s=${String(x.ops).padStart(6)}  errors=${x.err}\n`);
          printedSec++;
        }
      } else if (m.type === 'result') k.resolveDone(m);
    });
    k.cp.send({ type: 'start', cfg: { ...cfg, clients: k.n, quiet: false }, clientOffset: k.offset });
  }
  await Promise.all(kids.map((k) => k.ready));
  kids.forEach((k) => k.cp.send({ type: 'go' }));
  const results = await Promise.all(kids.map((k) => k.done));
  kids.forEach((k) => {
    k.cp.removeAllListeners('exit');
    k.cp.kill();
  });
  for (const r of results) mergeAcked(acked, r.acked);
  return buildResult(cfg, results.map((r) => r.part), results.reduce((a, r) => a + r.part.cpuPct, 0));
}

// Child side: wait for 'start', warm up, wait for 'go', run, send the raw result.
if (IS_CHILD) {
  process.on('message', async (m) => {
    if (m.type !== 'start') return;
    const acked = new Map();
    const conns = [];
    const part = await runOnce(m.cfg, acked, conns, {
      clientOffset: m.clientOffset,
      raw: true,
      barrier: () =>
        new Promise((go) => {
          process.send({ type: 'ready' });
          process.on('message', (x) => x.type === 'go' && go());
        }),
      onSecond: (sec, o, e) => process.send({ type: 'tick', sec, ops: o, err: e }),
    });
    conns.forEach((c) => c.close());
    process.send({ type: 'result', part, acked: [...acked] });
  });
}

async function seedKeys(keys, size, acked, conns) {
  process.stdout.write(`seeding ${keys} keys (${size} bytes each)...\n`);
  const conn = conns[0] ?? (conns[0] = new Conn(nodes.slice()));
  await conn.findLeader();
  const workers = Math.min(32, keys);
  let next = 0;
  await Promise.all(
    Array.from({ length: workers }, async (_, w) => {
      const c = w === 0 ? conn : (conns[w] ??= new Conn(nodes.slice()));
      for (;;) {
        const k = next++;
        if (k >= keys) return;
        const header = `${RUN_ID}.seed.${k}|`;
        const r = await c.put(keyOf(k), makeValue(size, header), { timeoutMs: 30_000 });
        acked.set(keyOf(k), { rev: Number(r.revision), header, size });
      }
    }),
  );
}

// ---- verify ------------------------------------------------------------------------------
async function verify(acked) {
  const conn = new Conn(nodes.slice());
  await conn.findLeader();
  const entries = [...acked.entries()];
  let lost = 0;
  let mismatched = 0;
  let newer = 0;
  const detail = [];
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(16, entries.length) }, async () => {
      const c = new Conn(conn.nodes.slice());
      c.use(conn.addr);
      for (;;) {
        const i = next++;
        if (i >= entries.length) return;
        const [key, a] = entries[i];
        const r = await c.get(key, { timeoutMs: 30_000 });
        if (!r.record) {
          lost++;
          if (detail.length < 5) detail.push(`LOST ${key}: acked at rev ${a.rev}, key missing`);
          continue;
        }
        const rev = Number(r.record.mod_revision);
        if (rev < a.rev) {
          lost++;
          if (detail.length < 5) detail.push(`LOST ${key}: acked at rev ${a.rev}, store has rev ${rev}`);
        } else if (rev === a.rev) {
          if (Buffer.compare(r.record.value, makeValue(a.size, a.header)) !== 0) {
            mismatched++;
            if (detail.length < 5) detail.push(`MISMATCH ${key} at rev ${rev}: ${r.record.value.length} bytes, header ${r.record.value.subarray(0, 24).toString('latin1')}`);
          }
        } else newer++; // a later write won (a timed-out write that applied, or a race on the ack path): fine
      }
      c.close();
    }),
  );
  conn.close();
  return { checked: entries.length, lost, mismatched, newerThanAcked: newer, detail };
}

// ---- printing ----------------------------------------------------------------------------
function printRun(r) {
  const L = r.latencyMs;
  const errTxt = Object.entries(r.errors).map(([k, v]) => `${k}=${v}`).join(' ') || 'none';
  process.stdout.write(
    `\n${r.workload}: ${r.clients} clients in ${r.procs} process(es), ${r.size} B values, ${r.duration}s\n` +
      `  ops/sec   ${r.opsPerSec.toFixed(0)}   (${r.ops} ok, ${r.errorTotal} errors, ${r.errorRatePct.toFixed(2)}%)\n` +
      `  latency   p50 ${f1(L.p50)}  p95 ${f1(L.p95)}  p99 ${f1(L.p99)}  max ${f1(L.max)} ms\n` +
      (r.workload === 'mixed'
        ? `  writes    p50 ${f1(r.writeLatencyMs.p50)}  p99 ${f1(r.writeLatencyMs.p99)} ms   reads  p50 ${f1(r.readLatencyMs.p50)}  p99 ${f1(r.readLatencyMs.p99)} ms\n`
        : '') +
      `  data      ${r.mbPerSec.toFixed(1)} MB/s of values\n` +
      `  errors    ${errTxt}\n` +
      `  longest silence (no op succeeded)  ${f1(r.longestSilenceMs)} ms, starting at t=${r.longestSilenceAtSec.toFixed(1)}s\n` +
      `  client    ${r.clientCpuCores.toFixed(1)} cores used by load processes (${r.clientCpuPctPerProc.toFixed(0)}% each)${r.clientCpuPctPerProc > 90 ? '  <- CLIENT-BOUND: server could do more, add --procs' : ''}\n`,
  );
}

function printVerify(v) {
  process.stdout.write(
    `\nverify: re-read ${v.checked} acked keys: LOST ${v.lost}, MISMATCHED ${v.mismatched}, newer-than-acked ${v.newerThanAcked}\n` +
      (v.lost + v.mismatched === 0 ? '  OK: no acked write was lost.\n' : `  FAIL:\n    ${v.detail.join('\n    ')}\n`),
  );
}

// ---- main --------------------------------------------------------------------------------
async function main() {
  const big = mode === 'big';
  const base = {
    clients: num(flags, 'clients', big ? 4 : 16),
    duration: num(flags, 'duration', 10),
    size: num(flags, 'size', big ? 262144 : 256),
    keys: num(flags, 'keys', big ? 40 : 1000),
    readPct: num(flags, 'read-pct', 90),
    quiet: !!flags.quiet,
  };
  if (base.size < 16 || base.size > MAX_VALUE_BYTES) die(`--size must be 16..${MAX_VALUE_BYTES}`);
  if (base.clients < 1 || base.keys < 1) die('--clients and --keys must be >= 1');

  const acked = new Map();
  const conns = [];
  const result = { tool: 'load.mjs', mode, startedAt: new Date().toISOString(), nodes, runId: RUN_ID, flags };
  process.stdout.write(`load.mjs ${mode}  nodes ${nodes.join(',')}  run ${RUN_ID}\n`);

  if (mode === 'ramp') {
    const workload = flags.workload ?? 'mixed';
    if (!['write', 'read', 'mixed'].includes(workload)) die('--workload must be write, read or mixed');
    const maxClients = num(flags, 'max-clients', 64);
    const p99Limit = num(flags, 'p99-ms', 250);
    const step = num(flags, 'step', 8);
    if (workload !== 'write' && !flags['no-seed']) await seedKeys(base.keys, base.size, acked, conns);
    const steps = [];
    for (let c = 1; c <= maxClients; c *= 2) {
      const r = await run({ ...base, workload, clients: c, duration: step, quiet: true }, acked, conns);
      const bad = r.errorRatePct > 1 ? `errors ${r.errorRatePct.toFixed(1)}% > 1%` : r.latencyMs.p99 > p99Limit ? `p99 ${f1(r.latencyMs.p99)} ms > ${p99Limit} ms` : null;
      r.stopReason = bad;
      steps.push(r);
      process.stdout.write(
        `  clients ${String(c).padStart(4)} (${r.procs} proc)  ops/s ${r.opsPerSec.toFixed(0).padStart(6)}  p50 ${f1(r.latencyMs.p50).padStart(7)}  p99 ${f1(r.latencyMs.p99).padStart(7)} ms  err ${r.errorRatePct.toFixed(2)}%  client-cores ${r.clientCpuCores.toFixed(1)}${bad ? `   STOP: ${bad}` : ''}\n`,
      );
      if (bad) break;
    }
    const good = steps.filter((s) => !s.stopReason);
    const peak = good.reduce((a, s) => (s.opsPerSec > (a?.opsPerSec ?? -1) ? s : a), null);
    const knee = peak ? good.find((s) => s.opsPerSec >= 0.9 * peak.opsPerSec) : null;
    const last = steps[steps.length - 1];
    process.stdout.write(
      `\nramp (${workload}, ${base.size} B): ` +
        (peak ? `peak ${peak.opsPerSec.toFixed(0)} ops/s at ${peak.clients} clients; KNEE ${knee.clients} clients (${knee.opsPerSec.toFixed(0)} ops/s, p99 ${f1(knee.latencyMs.p99)} ms)\n` : 'no step passed\n') +
        (last.stopReason
          ? `  stopped at ${last.clients} clients: ${last.stopReason}\n`
          : steps.length > 1 && last.opsPerSec < 1.05 * steps[steps.length - 2].opsPerSec
            ? `  reached --max-clients ${maxClients}: throughput stopped growing (the cluster is the limit), no error or p99 limit hit\n`
            : `  reached --max-clients ${maxClients} and throughput was still growing (raise --max-clients and --procs)\n`),
    );
    result.steps = steps;
    result.peak = peak && { clients: peak.clients, opsPerSec: peak.opsPerSec };
    result.knee = knee && { clients: knee.clients, opsPerSec: knee.opsPerSec, p99Ms: knee.latencyMs.p99 };
    if (workload !== 'read' && !flags['no-verify']) {
      result.verify = await verify(acked);
      printVerify(result.verify);
    }
  } else {
    const workload = big ? 'write' : mode;
    if (workload !== 'write' && !flags['no-seed']) await seedKeys(base.keys, base.size, acked, conns);
    const r = await run({ ...base, workload }, acked, conns);
    r.workload = big ? 'big (write)' : mode;
    printRun(r);
    result.run = r;
    if (acked.size > 0 && !flags['no-verify']) {
      result.verify = await verify(acked);
      printVerify(result.verify);
    }
  }

  conns.forEach((c) => c.close());
  if (flags.out) {
    fs.writeFileSync(flags.out, JSON.stringify(result, null, 2));
    process.stdout.write(`wrote ${flags.out}\n`);
  }
  if (result.verify && result.verify.lost + result.verify.mismatched > 0) process.exitCode = 3;
}

if (!IS_CHILD) {
  main().catch((e) => {
    process.stderr.write(`error: ${e.codeName ?? ''} ${e.details ?? e.stack ?? e}\n`);
    process.exitCode = 1;
  });
}
