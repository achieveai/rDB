// Test helper: stop / start ONE node of a local-cluster.sh cluster (same stop file and flags as
// samples/retcd-playground/node.sh). Needs RETCD_CLUSTER_DIR and, to start, RETCD_SERVER_BIN.
// Liveness is judged by the node's client port, not by a pid (Git Bash pids are not Windows pids).
import { execFileSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function readEnv(dir, n) {
  const file = path.join(dir, `node-${n}`, 'node.env');
  if (!fs.existsSync(file)) throw new Error(`no node ${n} in ${dir}`);
  return Object.fromEntries(
    fs
      .readFileSync(file, 'utf8')
      .split(/\r?\n/)
      .filter((l) => l.includes('='))
      .map((l) => [l.slice(0, l.indexOf('=')), l.slice(l.indexOf('=') + 1).trim()]),
  );
}

export function portOpen(addr, ms = 400) {
  const i = addr.lastIndexOf(':');
  return new Promise((resolve) => {
    const s = net.connect({ host: addr.slice(0, i), port: Number(addr.slice(i + 1)) });
    const done = (ok) => {
      s.destroy();
      resolve(ok);
    };
    s.setTimeout(ms, () => done(false));
    s.once('connect', () => done(true));
    s.once('error', () => done(false));
  });
}

export async function waitFor(cond, ms, what) {
  const until = Date.now() + ms;
  for (;;) {
    if (await cond()) return;
    if (Date.now() > until) throw new Error(`timed out after ${ms} ms waiting for ${what}`);
    await sleep(150);
  }
}

/** Ask node n to stop (stop file) and wait until its client port refuses connections. */
export async function stopNode(dir, n) {
  const env = readEnv(dir, n);
  fs.writeFileSync(path.join(dir, `node-${n}`, 'stop'), '');
  await waitFor(async () => !(await portOpen(env.CLIENT)), 90_000, `node ${n} to stop`);
}

/**
 * Start node n again with the flags local-cluster.sh uses. The process is detached.
 * WARNING: local-cluster.sh finds a node by the pid in node-N/pid (a Git Bash pid). A node started
 * here has a different pid, so `local-cluster.sh down` will NOT stop it (it reports 0 stopped and
 * `clean` fails on the RocksDB LOCK). Use stopAllNodes(dir) first.
 */
export async function startNode(dir, n, bin) {
  const env = readEnv(dir, n);
  const nd = path.join(dir, `node-${n}`);
  fs.rmSync(path.join(nd, 'stop'), { force: true });
  const out = fs.openSync(path.join(nd, 'stdout.log'), 'a');
  const err = fs.openSync(path.join(nd, 'stderr.log'), 'a');
  const child = spawn(
    bin,
    ['--config', path.join(nd, 'config.toml'), '--log-dir', path.join(nd, 'logs'), '--shutdown-file', path.join(nd, 'stop'), '--health-listen', env.HEALTH, '--allow-insecure-dev', '--dev-allow-all'],
    { detached: true, stdio: ['ignore', out, err], windowsHide: true },
  );
  child.unref();
  await waitFor(() => portOpen(env.CLIENT), 30_000, `node ${n} to listen`);
}

// Windows pid of whatever listens on `addr`, checked to be THIS cluster's node n (its command
// line must name the node directory) so we never touch another agent's process.
function listenerPid(addr, mustContain) {
  const port = addr.slice(addr.lastIndexOf(':') + 1);
  const table = execFileSync('netstat', ['-ano', '-p', 'TCP'], { encoding: 'utf8' });
  const line = table.split(/\r?\n/).find((l) => /LISTENING/.test(l) && l.includes(`:${port} `));
  if (!line) throw new Error(`nothing listens on ${addr}`);
  const pid = Number(line.trim().split(/\s+/).pop());
  const cmd = execFileSync(
    'powershell',
    ['-NoProfile', '-Command', `(Get-CimInstance Win32_Process -Filter 'ProcessId=${pid}').CommandLine`],
    { encoding: 'utf8' },
  );
  const norm = (x) => x.split(String.fromCharCode(92)).join('/').toLowerCase();
  if (!norm(cmd).includes(norm(mustContain))) throw new Error(`pid ${pid} on ${addr} is not ${mustContain}: refusing to kill it`);
  return pid;
}

/** Crash node n (no graceful drain): kill its process, then wait for the port to close. */
export async function killNode(dir, n) {
  const env = readEnv(dir, n);
  const nd = path.join(dir, `node-${n}`);
  let pid;
  if (process.platform === 'win32') pid = listenerPid(env.CLIENT, nd);
  else pid = Number(fs.readFileSync(path.join(nd, 'pid'), 'utf8'));
  process.kill(pid);
  await waitFor(async () => !(await portOpen(env.CLIENT)), 15_000, `node ${n} to die`);
}

/** Which node number (1, 2, ...) listens on this client endpoint, according to the node.env files. */
export function nodeForEndpoint(dir, endpoint) {
  for (let n = 1; fs.existsSync(path.join(dir, `node-${n}`, 'node.env')); n++) {
    if (readEnv(dir, n).CLIENT === endpoint) return n;
  }
  throw new Error(`no node in ${dir} has client endpoint ${endpoint}`);
}

/** Write the stop file in every node directory (graceful stop, works for nodes started by any means). */
export function stopAllNodes(dir) {
  for (let n = 1; fs.existsSync(path.join(dir, `node-${n}`, 'node.env')); n++) fs.writeFileSync(path.join(dir, `node-${n}`, 'stop'), '');
}
