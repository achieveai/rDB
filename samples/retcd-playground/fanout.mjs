#!/usr/bin/env node
// fanout.mjs: many watchers on one prefix + a few writers. Checks every watcher saw every
// acked write (no missing, no duplicates, no reordering) and measures write-to-event lag.
//
//   node fanout.mjs --watchers 20 --writers 2 --rate 200 --duration 10
//   node fanout.mjs --watchers 120 --writers 1 --rate 20 --duration 8 --readmit   # past the cap
//
// Writers and watchers live in this one process, so the send timestamp and the receive
// timestamp come from the same clock (no clock skew). Each value is JSON {w, s, t}:
// writer number, that writer's sequence, send time in ms.
import fs from 'node:fs';
import { COMMON_FLAGS, Conn, STATUS_NAME, b, die, f1, grpc, nodeList, now, num, parseArgs, sleep, summarize } from './retlib.mjs';

const SPEC = {
  value: [...COMMON_FLAGS, 'watchers', 'writers', 'rate', 'duration', 'size', 'prefix', 'keys', 'drain', 'out', 'progress-ms'],
  bool: ['readmit', 'detail', 'help'],
};
const { flags } = parseArgs(process.argv.slice(2), SPEC);
if (flags.help) {
  process.stdout.write(`usage: node fanout.mjs [--watchers 20] [--writers 2] [--rate 200] [--duration 10]
  --rate R        total writes per second across all writers (paced, one in flight per writer)
  --size BYTES    value size (default 128)   --keys N  keys per writer, rotated (default 100)
  --prefix P      default fan/<runid>/       --drain S  wait this long for stragglers (default 5)
  --readmit       after the run, close 5 watchers and try 5 new ones (does the cap free up?)
  --detail        print one line for every watcher
  --out FILE      JSON result      --node N | --addr H:P | --base-port P
`);
  process.exit(0);
}

const nW = num(flags, 'watchers', 20);
const nWriters = num(flags, 'writers', 2);
const rate = num(flags, 'rate', 200);
const duration = num(flags, 'duration', 10);
const size = Math.max(64, num(flags, 'size', 128));
const keysPer = Math.max(1, num(flags, 'keys', 100));
const drainS = num(flags, 'drain', 5);
const progressMs = num(flags, 'progress-ms', 500);
const RUN_ID = Date.now().toString(36);
const prefix = flags.prefix ?? `fan/${RUN_ID}/`;
if (nWriters < 1 || rate <= 0) die('--writers >= 1 and --rate > 0');

const metaOf = (err, key) => err.metadata?.get(key)?.[0];

async function main() {
  const seedConn = new Conn(nodeList(flags));
  const { addr: leader, readRevision } = await seedConn.findLeader();
  seedConn.close();
  const R0 = readRevision;
  process.stdout.write(`fanout: ${nW} watchers, ${nWriters} writers, ${rate} writes/s total, ${duration}s, leader ${leader}, prefix ${prefix}\n`);

  // ---- watchers ----
  const watchers = [];
  function openWatcher(id) {
    const conn = new Conn([leader]);
    const w = {
      id, conn, call: null, state: 'connecting', openedAtMs: now(), firstMsgMs: null,
      error: null, errorAtMs: null, events: 0, dups: 0, ooo: 0, seqOoo: 0, lastRev: 0,
      lastSeq: new Map(), seen: new Map(), lags: [],
    };
    const call = conn.client().Watch({ prefix: b(prefix), start_after_revision: R0, progress_interval_ms: progressMs });
    w.call = call;
    call.on('data', (m) => {
      const t = now();
      if (w.firstMsgMs === null) {
        w.firstMsgMs = t;
        w.state = 'open';
      }
      if (m.body !== 'event') return;
      const ev = m.event;
      const rev = Number(ev.revision);
      if (rev <= w.lastRev) w.ooo++;
      else w.lastRev = rev;
      if (ev.change !== 'put') return;
      let v;
      try {
        v = JSON.parse(ev.put.value.toString('utf8'));
      } catch {
        return;
      }
      w.events++;
      w.lags.push(t - v.t);
      let seen = w.seen.get(v.w);
      if (!seen) w.seen.set(v.w, (seen = new Set()));
      if (seen.has(v.s)) w.dups++;
      seen.add(v.s);
      const last = w.lastSeq.get(v.w) ?? -1;
      if (v.s < last) w.seqOoo++;
      else w.lastSeq.set(v.w, v.s);
    });
    call.on('error', (err) => {
      if (w.state === 'closing') return;
      w.state = 'failed';
      w.errorAtMs = now();
      w.error = {
        code: STATUS_NAME[err.code] ?? err.code,
        details: err.details ?? err.message,
        resumable: metaOf(err, 'retcd-resumable'),
        minRevision: metaOf(err, 'retcd-min-revision'),
        afterMs: w.errorAtMs - w.openedAtMs,
        eventsBefore: w.events,
      };
    });
    call.on('end', () => {
      if (w.state === 'open') w.state = 'ended';
    });
    watchers.push(w);
    return w;
  }
  for (let i = 0; i < nW; i++) openWatcher(i);
  // Wait until every stream either answered (progress arrives within --progress-ms) or failed.
  const t0 = now();
  while (now() - t0 < 5000 && watchers.some((w) => w.state === 'connecting')) await sleep(50);
  const openNow = watchers.filter((w) => w.state === 'open').length;
  const failedNow = watchers.filter((w) => w.state === 'failed').length;
  process.stdout.write(`  streams: ${openNow} open, ${failedNow} refused, ${nW - openNow - failedNow} silent after 5 s\n`);

  // ---- writers ----
  const acked = Array.from({ length: nWriters }, () => new Set());
  const writeLat = [];
  let writeErrors = {};
  let sent = 0;
  const startAt = now();
  const stopAt = startAt + duration * 1000;
  const interval = (1000 * nWriters) / rate;
  const cpu0 = process.cpuUsage();
  async function writer(w) {
    const conn = new Conn([leader]);
    let s = 0;
    let next = startAt + (w * interval) / nWriters;
    while (now() < stopAt) {
      const wait = next - now();
      if (wait > 1) await sleep(wait);
      next += interval;
      const seq = s++;
      const t = now();
      const body = JSON.stringify({ w, s: seq, t });
      const value = Buffer.from(body.padEnd(size, ' '));
      try {
        const r = await conn.put(`${prefix}w${w}/k${seq % keysPer}`, value, { timeoutMs: 10_000 });
        if (r.outcome !== 'APPLIED') throw Object.assign(new Error(r.outcome), { codeName: r.outcome });
        acked[w].add(seq);
        writeLat.push(now() - t);
        sent++;
      } catch (e) {
        const k = e.codeName ?? 'ERROR';
        writeErrors[k] = (writeErrors[k] ?? 0) + 1;
      }
    }
    conn.close();
  }
  await Promise.all(Array.from({ length: nWriters }, (_, w) => writer(w)));
  const elapsed = (now() - startAt) / 1000;
  const cpu = process.cpuUsage(cpu0);

  // ---- drain: every open watcher should reach every acked write ----
  const total = acked.reduce((a, s) => a + s.size, 0);
  const missingOf = (w) => {
    let m = 0;
    for (let wi = 0; wi < nWriters; wi++) {
      const seen = w.seen.get(wi);
      for (const s of acked[wi]) if (!seen || !seen.has(s)) m++;
    }
    return m;
  };
  const drainStart = now();
  while (now() - drainStart < drainS * 1000 && watchers.some((w) => w.state === 'open' && missingOf(w) > 0)) await sleep(100);

  // ---- report ----
  const live = watchers.filter((w) => w.state === 'open' || w.state === 'ended' || (w.state === 'failed' && w.events > 0));
  const rows = watchers.map((w) => {
    const l = summarize(w.lags);
    return { id: w.id, state: w.state, events: w.events, missing: w.state === 'failed' && w.events === 0 ? null : missingOf(w), dups: w.dups, ooo: w.ooo, seqOoo: w.seqOoo, lagP50: l.p50, lagP99: l.p99, lagMax: l.max, error: w.error };
  });
  const healthy = rows.filter((r) => r.state === 'open');
  process.stdout.write(`\nwrites: ${total} acked in ${elapsed.toFixed(1)}s = ${(total / elapsed).toFixed(0)}/s (asked ${rate}/s), errors ${JSON.stringify(writeErrors)}\n`);
  const wl = summarize(writeLat);
  process.stdout.write(`  write ack latency  p50 ${f1(wl.p50)}  p99 ${f1(wl.p99)}  max ${f1(wl.max)} ms\n`);
  process.stdout.write(`\nwatchers: ${healthy.length} healthy of ${nW}\n`);
  const show = flags.detail || rows.length <= 12 ? rows : [];
  if (show.length) {
    process.stdout.write(`  ${'id'.padStart(3)} ${'state'.padEnd(7)} ${'events'.padStart(7)} ${'missing'.padStart(7)} ${'dups'.padStart(5)} ${'ooo'.padStart(4)} ${'lag p50'.padStart(8)} ${'p99'.padStart(7)} ${'max'.padStart(7)}\n`);
    for (const r of show) {
      process.stdout.write(`  ${String(r.id).padStart(3)} ${r.state.padEnd(7)} ${String(r.events).padStart(7)} ${String(r.missing ?? '-').padStart(7)} ${String(r.dups).padStart(5)} ${String(r.ooo + r.seqOoo).padStart(4)} ${f1(r.lagP50).padStart(8)} ${f1(r.lagP99).padStart(7)} ${f1(r.lagMax).padStart(7)}\n`);
    }
  }
  const allLags = healthy.flatMap((r) => watchers[r.id].lags);
  const agg = summarize(allLags);
  const sum = (k) => healthy.reduce((a, r) => a + r[k], 0);
  process.stdout.write(
    `  all healthy watchers: ${allLags.length} events delivered (expected ${total * healthy.length})\n` +
      `    missing ${sum('missing')}   duplicates ${sum('dups')}   out-of-order ${sum('ooo') + sum('seqOoo')}\n` +
      `    delivery lag  p50 ${f1(agg.p50)}  p95 ${f1(agg.p95)}  p99 ${f1(agg.p99)}  max ${f1(agg.max)} ms\n` +
      `    worst watcher p99 ${f1(Math.max(...healthy.map((r) => r.lagP99), 0))} ms\n` +
      `  client: node process CPU ${(((cpu.user + cpu.system) / 1000 / (elapsed * 1000)) * 100).toFixed(0)}% of one core\n`,
  );

  // ---- what happened to refused streams ----
  const failed = rows.filter((r) => r.state === 'failed');
  const groups = new Map();
  for (const r of failed) {
    const k = `${r.error.code} | ${r.error.details} | resumable=${r.error.resumable ?? '-'}`;
    const g = groups.get(k) ?? { n: 0, minMs: Infinity, maxMs: 0, ids: [] };
    g.n++;
    g.minMs = Math.min(g.minMs, r.error.afterMs);
    g.maxMs = Math.max(g.maxMs, r.error.afterMs);
    g.ids.push(r.id);
    groups.set(k, g);
  }
  if (failed.length) {
    process.stdout.write(`\nstreams that did not stay open: ${failed.length}\n`);
    for (const [k, g] of groups) {
      process.stdout.write(`  ${g.n}x  ${k}\n      after ${f1(g.minMs)}..${f1(g.maxMs)} ms; watcher ids ${g.ids.length > 8 ? `${g.ids.slice(0, 4).join(',')}..${g.ids.slice(-2).join(',')}` : g.ids.join(',')}\n`);
    }
    const diedLate = failed.filter((r) => r.events > 0).length;
    process.stdout.write(`  refused before any event: ${failed.length - diedLate}; killed after receiving events: ${diedLate}; streams that stayed healthy: ${healthy.length}
`);
  }

  // ---- does the cap free up? ----
  let readmit = null;
  if (flags.readmit) {
    const toClose = healthy.slice(0, 5);
    for (const r of toClose) {
      watchers[r.id].state = 'closing';
      watchers[r.id].call.cancel();
    }
    await sleep(1000);
    const extra = Array.from({ length: 5 }, (_, i) => openWatcher(nW + i));
    const t1 = now();
    while (now() - t1 < 4000 && extra.some((w) => w.state === 'connecting')) await sleep(50);
    readmit = { closed: toClose.length, tried: extra.length, admitted: extra.filter((w) => w.state === 'open').length, refused: extra.filter((w) => w.state === 'failed').length };
    process.stdout.write(`\nreadmit: closed ${readmit.closed} open streams, then opened ${readmit.tried} new: ${readmit.admitted} admitted, ${readmit.refused} refused\n`);
    extra.forEach((w) => {
      w.state = 'closing';
      w.call.cancel();
    });
  }

  watchers.forEach((w) => {
    w.state = 'closing';
    w.call.cancel();
    w.conn.close();
  });
  if (flags.out) {
    fs.writeFileSync(flags.out, JSON.stringify({ tool: 'fanout.mjs', startedAt: new Date().toISOString(), flags, leader, acked: total, writeErrors, writeLatencyMs: wl, healthy: healthy.length, refused: failed.length, groups: [...groups].map(([k, g]) => ({ k, n: g.n, ids: g.ids })), delivery: { events: allLags.length, expected: total * healthy.length, missing: sum('missing'), duplicates: sum('dups'), outOfOrder: sum('ooo') + sum('seqOoo'), lagMs: agg }, readmit, rows: rows.map((r) => ({ ...r })) }, null, 2));
    process.stdout.write(`wrote ${flags.out}\n`);
  }
  if (sum('missing') + sum('dups') + sum('ooo') + sum('seqOoo') > 0) process.exitCode = 3;
  void grpc;
}

main().catch((e) => {
  process.stderr.write(`error: ${e.codeName ?? ''} ${e.details ?? e.stack ?? e}\n`);
  process.exitCode = 1;
});
