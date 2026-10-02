#!/usr/bin/env node
// presence.mjs: find dead or cut-off clients THROUGH rEtcd. rEtcd has no leases or TTLs, so
// every client writes a heartbeat key and a monitor watches the keys.
//
//   node presence.mjs client alice [--interval 2]      one client (Ctrl-C = graceful leave)
//   node presence.mjs monitor [--interval 2 --miss 3]  prints UP / LATE / DOWN / BACK / LEFT
//   node presence.mjs swarm 50 [--interval 2]          50 clients in one process
//
// Key: presence/<name>  Value: JSON {"name","pid","seq","sent_at"} (sent_at = ISO-8601 UTC).
// A DOWN verdict means "no heartbeat for --miss intervals", measured on the monitor's clock
// from when each heartbeat ARRIVED at the monitor.
import { COMMON_FLAGS, Conn, STATUS_NAME, b, die, grpc, nodeList, now, num, parseArgs, sleep, stamp } from './retlib.mjs';

const SPEC = {
  value: [...COMMON_FLAGS, 'interval', 'miss', 'prefix-name', 'stagger', 'stop', 'stop-after', 'table'],
  bool: ['no-delete', 'quiet', 'help'],
};
const { pos, flags } = parseArgs(process.argv.slice(2), SPEC);
const cmd = pos[0];
if (!cmd || flags.help || !['client', 'monitor', 'swarm', 'clean'].includes(cmd)) {
  process.stdout.write(`usage:
  node presence.mjs client <name> [--interval 2] [--no-delete]
  node presence.mjs monitor [--interval 2] [--miss 3] [--table all|bad]
  node presence.mjs swarm <count> [--interval 2] [--prefix-name sw] [--stop N --stop-after S]
  node presence.mjs clean                   delete every presence/ key (heartbeat keys never expire)
  common: --node N | --addr H:P | --base-port P (default 17400, 3 nodes)
  --interval  heartbeat period in seconds (client side) / expected period (monitor side)
  --miss      beats late before DOWN (default 3)
  swarm --stop N --stop-after S: after S seconds, silently stop N clients (a simulated crash)
`);
  process.exit(cmd && !flags.help ? 2 : 0);
}

const PREFIX = 'presence/';
const interval = num(flags, 'interval', 2);
const log = (s) => process.stdout.write(`${stamp()}  ${s}\n`);
// A node switch can repeat many times a second while an election runs: log at most one a second.
const throttled = (fn) => {
  let last = 0;
  return (t) => {
    if (now() - last > 1000) {
      last = now();
      fn(t);
    }
  };
};

// ---- client ------------------------------------------------------------------------------
function startClient(name, { nodes, quiet = false, onLog = log }) {
  const conn = new Conn(nodes.slice(), { name });
  conn.onEvent = throttled((t) => onLog(`${name}: reconnect: ${t}`));
  const st = { name, running: true, beats: 0, failed: 0, failStreak: 0, seq: 0, stopped: false };
  let wake = null;
  const loop = (async () => {
    let next = now();
    while (st.running) {
      const seq = ++st.seq;
      const value = Buffer.from(JSON.stringify({ name, pid: process.pid, seq, sent_at: new Date().toISOString() }));
      try {
        const r = await conn.put(PREFIX + name, value, { timeoutMs: Math.max(3000, interval * 1500) });
        if (r.outcome !== 'APPLIED') throw Object.assign(new Error(r.outcome), { codeName: r.outcome });
        if (st.failStreak > 0) onLog(`${name}: heartbeats OK again after ${st.failStreak} failed (node ${conn.addr})`);
        st.failStreak = 0;
        st.beats++;
        if (!quiet) onLog(`${name}: beat seq=${seq} rev=${r.revision} via ${conn.addr}`);
      } catch (e) {
        st.failed++;
        if (st.failStreak++ === 0 || st.failStreak % 5 === 0) onLog(`${name}: heartbeat FAILED seq=${seq} (${e.codeName ?? 'ERROR'}: ${e.details ?? e.message}); streak ${st.failStreak}`);
        conn.rotate();
      }
      next += interval * 1000;
      const wait = next - now();
      if (wait < -interval * 1000) next = now(); // fell far behind: do not burst
      if (st.running && wait > 0) await new Promise((r) => { wake = r; setTimeout(r, wait); });
    }
  })();
  st.stop = async ({ graceful }) => {
    st.running = false;
    wake?.();
    await loop;
    if (graceful) {
      try {
        await conn.del(PREFIX + name, { timeoutMs: 3000 });
        onLog(`${name}: left (key deleted)`);
      } catch (e) {
        onLog(`${name}: could not delete key (${e.codeName}); monitor will mark it DOWN`);
      }
    }
    conn.close();
  };
  return st;
}

// ---- monitor -----------------------------------------------------------------------------
async function monitor() {
  const miss = num(flags, 'miss', 3);
  const nodes = nodeList(flags);
  const conn = new Conn(nodes.slice(), { name: 'monitor' });
  conn.onEvent = throttled((t) => log(`MONITOR: ${t}`));
  const names = new Map(); // name -> {lastSeen(ms), seq, pid, state, shownMissed}
  let lastRev = 0;
  let watching = false;
  const nowMs = () => Date.now();
  const age = (m) => (nowMs() - m.lastSeen) / 1000;
  const parse = (buf) => {
    try {
      return JSON.parse(buf.toString('utf8'));
    } catch {
      return {};
    }
  };

  async function list() {
    const { records, readRevision } = await conn.listAll(PREFIX);
    lastRev = Number(readRevision);
    const seenNow = new Set();
    for (const rec of records) {
      const name = rec.key.toString('utf8').slice(PREFIX.length);
      const v = parse(rec.value);
      seenNow.add(name);
      const a = Number.isFinite(Date.parse(v.sent_at)) ? Math.max(0, nowMs() - Date.parse(v.sent_at)) : 0;
      const m = names.get(name);
      if (m) {
        m.lastSeen = Math.max(m.lastSeen, nowMs() - a);
        m.seq = v.seq;
        continue;
      }
      const entry = { lastSeen: nowMs() - a, seq: v.seq, pid: v.pid, state: 'UP', shownMissed: 0 };
      names.set(name, entry);
      const missed = missedOf(entry);
      if (missed >= miss) {
        entry.state = 'DOWN';
        log(`DOWN ${name} (last seen ${age(entry).toFixed(1)}s ago)  [found at start]`);
      } else if (missed >= 1) {
        entry.state = 'LATE';
        entry.shownMissed = missed;
        log(`LATE ${name} (missed ${missed})  [found at start]`);
      } else log(`UP ${name}  [found at start]`);
    }
    return seenNow;
  }

  function missedOf(m) {
    return Math.max(0, Math.floor((nowMs() - m.lastSeen - interval * 500) / (interval * 1000)));
  }

  function onPut(key, rec) {
    const name = key.slice(PREFIX.length);
    const v = parse(rec.value);
    const m = names.get(name);
    if (!m) {
      names.set(name, { lastSeen: nowMs(), seq: v.seq, pid: v.pid, state: 'UP', shownMissed: 0 });
      log(`UP ${name}  (pid ${v.pid}, seq ${v.seq})`);
      return;
    }
    const was = m.state;
    const gap = age(m);
    m.lastSeen = nowMs();
    m.seq = v.seq;
    m.pid = v.pid;
    m.state = 'UP';
    m.shownMissed = 0;
    if (was === 'DOWN') log(`BACK ${name} (was DOWN, silent ${gap.toFixed(1)}s)`);
    else if (was === 'LATE') log(`BACK ${name} (was LATE, silent ${gap.toFixed(1)}s)`);
  }

  // One watch connection. Resolves with the reason it ended.
  function watchOnce() {
    return new Promise((resolve) => {
      const call = conn.client().Watch({ prefix: b(PREFIX), start_after_revision: lastRev, progress_interval_ms: 1000 });
      let lastMsg = now();
      const stall = setInterval(() => {
        if (now() - lastMsg > 5000) {
          clearInterval(stall);
          call.cancel();
          resolve({ why: 'no message for 5 s (stalled)' });
        }
      }, 1000);
      call.on('data', (m) => {
        lastMsg = now();
        if (!watching) {
          watching = true;
          log(`MONITOR: watching ${PREFIX} from revision ${lastRev} via ${conn.addr}`);
        }
        if (m.body === 'progress') {
          lastRev = Math.max(lastRev, Number(m.progress.revision));
          return;
        }
        if (m.body !== 'event') return;
        const ev = m.event;
        lastRev = Math.max(lastRev, Number(ev.revision));
        const key = ev.key.toString('utf8');
        if (ev.change === 'put') onPut(key, ev.put.value === undefined ? { value: Buffer.alloc(0) } : { value: ev.put.value });
        else {
          const name = key.slice(PREFIX.length);
          if (names.delete(name)) log(`LEFT ${name} (key deleted: graceful exit)`);
        }
      });
      call.on('error', (err) => {
        clearInterval(stall);
        resolve({ err, why: `${STATUS_NAME[err.code] ?? err.code}: ${err.details ?? err.message}` });
      });
      call.on('end', () => {
        clearInterval(stall);
        resolve({ why: 'stream ended' });
      });
    });
  }

  // Detector tick.
  setInterval(() => {
    if (!watching) return; // blind: no verdicts while the watch is down
    for (const [name, m] of names) {
      const missed = missedOf(m);
      if (missed >= miss) {
        if (m.state !== 'DOWN') {
          m.state = 'DOWN';
          log(`DOWN ${name} (last seen ${age(m).toFixed(1)}s ago)`);
        }
      } else if (missed >= 1 && missed > m.shownMissed) {
        m.state = 'LATE';
        m.shownMissed = missed;
        log(`LATE ${name} (missed ${missed})`);
      }
    }
  }, 200);

  // Status table.
  setInterval(() => {
    const counts = { UP: 0, LATE: 0, DOWN: 0 };
    for (const m of names.values()) counts[m.state]++;
    const mode = flags.table ?? (names.size <= 20 ? 'all' : 'bad');
    log(`--- status: UP ${counts.UP}  LATE ${counts.LATE}  DOWN ${counts.DOWN}  (${names.size} known, rev ${lastRev}, node ${conn.addr}${watching ? '' : ', WATCH DOWN'}) ---`);
    for (const [name, m] of [...names].sort()) {
      if (mode === 'bad' && m.state === 'UP') continue;
      process.stdout.write(`    ${name.padEnd(16)} ${m.state.padEnd(5)} last seen ${age(m).toFixed(1).padStart(5)}s ago  seq ${m.seq}  pid ${m.pid}\n`);
    }
  }, 10_000);

  log(`MONITOR: nodes ${nodes.join(', ')}  interval ${interval}s  DOWN after ${miss} missed beats (~${((miss + 0.5) * interval).toFixed(1)}s silence)`);
  for (;;) {
    try {
      await conn.findLeader(15_000);
      await list();
      for (;;) {
        const r = await watchOnce();
        if (watching) log(`MONITOR: watch lost (${r.why}); reconnecting from revision ${lastRev}`);
        watching = false;
        await sleep(500);
        try {
          await conn.findLeader(15_000);
        } catch {
          conn.rotate();
        }
        if (r.err?.code === grpc.status.OUT_OF_RANGE) {
          log('MONITOR: revision compacted away; listing again');
          await list();
        }
        // Grace: we were blind, so nobody is late because of us.
        for (const m of names.values()) if (m.state !== 'DOWN') m.lastSeen = Math.max(m.lastSeen, nowMs());
      }
    } catch (e) {
      log(`MONITOR: cannot reach the cluster (${e.codeName ?? ''} ${e.details ?? e.message}); retrying`);
      watching = false;
      conn.rotate();
      await sleep(1000);
    }
  }
}

// ---- main --------------------------------------------------------------------------------
if (cmd === 'client') {
  const name = pos[1];
  if (!name || !/^[A-Za-z0-9._-]+$/.test(name)) die('usage: presence.mjs client <name>  (letters, digits, . _ -)');
  const c = startClient(name, { nodes: nodeList(flags), quiet: !!flags.quiet });
  log(`${name}: started, pid ${process.pid}, heartbeat every ${interval}s`);
  let leaving = false;
  const leave = async () => {
    if (leaving) return;
    leaving = true;
    await c.stop({ graceful: !flags['no-delete'] });
    process.exit(0);
  };
  process.on('SIGINT', leave);
  process.on('SIGTERM', leave);
} else if (cmd === 'monitor') {
  monitor();
} else if (cmd === 'clean') {
  const conn = new Conn(nodeList(flags));
  const { records } = await conn.listAll(PREFIX);
  for (const r of records) await conn.del(r.key);
  log(`deleted ${records.length} keys under ${PREFIX}`);
  conn.close();
} else {
  const count = Number(pos[1]);
  if (!Number.isInteger(count) || count < 1) die('usage: presence.mjs swarm <count>');
  const pfx = flags['prefix-name'] ?? 'sw';
  const nodes = nodeList(flags);
  const clients = [];
  const pad = String(count).length;
  const stagger = num(flags, 'stagger', (interval * 1000) / count);
  (async () => {
    for (let i = 1; i <= count; i++) {
      clients.push(startClient(`${pfx}-${String(i).padStart(pad, '0')}`, { nodes, quiet: true }));
      await sleep(stagger);
    }
    log(`swarm: ${count} clients started (names ${pfx}-${'1'.padStart(pad, '0')}..${pfx}-${String(count).padStart(pad, '0')}), pid ${process.pid}`);
  })();
  setInterval(() => {
    const ok = clients.reduce((a, c) => a + c.beats, 0);
    const bad = clients.reduce((a, c) => a + c.failed, 0);
    log(`swarm: ${clients.filter((c) => c.running).length} running, heartbeats ok ${ok}, failed ${bad}`);
  }, 10_000);
  if (flags.stop) {
    const n = Number(flags.stop);
    setTimeout(() => {
      const pick = [...clients].sort(() => Math.random() - 0.5).slice(0, n);
      for (const c of pick) {
        log(`STOPPED ${c.name} (silent stop, key left behind)`);
        c.stop({ graceful: false });
      }
    }, num(flags, 'stop-after', 30) * 1000);
  }
  let leaving = false;
  const leave = async () => {
    if (leaving) return;
    leaving = true;
    await Promise.all(clients.filter((c) => c.running).map((c) => c.stop({ graceful: !flags['no-delete'] })));
    process.exit(0);
  };
  process.on('SIGINT', leave);
  process.on('SIGTERM', leave);
}
