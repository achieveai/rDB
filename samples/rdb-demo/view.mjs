#!/usr/bin/env node
// Render the rDB demo trace as one self-contained HTML timeline.
//
//   node samples/rdb-demo/view.mjs <trace.json> <out.html>
//
// Input is the file written by `cargo run -p rdb-sim --example demo -- <trace.json>`.
// Plain Node 18+, ESM, no npm packages, no network. The page has no external scripts.
// Every row on the page is read from the JSON: trace rows come from `summary.timeline`, demo
// actions and probes from `summary.steps`, the capability table from `summary.capabilities`.

import { readFileSync, writeFileSync } from 'node:fs';

const [, , inPath, outPath] = process.argv;
if (!inPath || !outPath) {
  console.error('usage: node samples/rdb-demo/view.mjs <trace.json> <out.html>');
  process.exit(2);
}

let doc;
try {
  doc = JSON.parse(readFileSync(inPath, 'utf8'));
} catch (error) {
  console.error(`cannot read ${inPath}: ${error.message}`);
  process.exit(1);
}
const s = doc?.summary;
if (doc?.format !== 'rdb-demo/1' || !s || !Array.isArray(s.timeline) || !Array.isArray(s.steps)) {
  console.error(`${inPath} is not an rdb-demo/1 file (run the demo example to make one)`);
  process.exit(1);
}

const esc = (value) =>
  String(value).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);

// What each package is, in words. A package not listed here still renders, with no description.
const PACKAGES = {
  A1: 'authority: grants, epochs, the gates a request passes',
  T1: 'transactions: the write path',
  R1: 'replication: copies and acknowledgements',
  P1: 'publication: when a write counts as published',
  L1: 'protection: pauses writes when copies lag',
  F1: 'recovery: picks the new generation after a loss',
  H1: 'environment: scheduler, clock, network, control store',
  M1: 'environment: in-memory storage',
  I1: 'environment: harness and invariants',
};
const MODULE_LABEL = { client: 'client', scenario: 'scenario (demo action)', probe: 'probe (state read)', env: 'environment' };

const nodes = s.nodes.map(Number);
const colOf = new Map(nodes.map((n, i) => [n, i + 2])); // grid column 1 is the tick

// Items: trace rows and demo steps, merged by trace position. A step goes before the first row
// whose first event is at or after the number of events that existed when the step happened.
const items = [];
for (const row of s.timeline) {
  if (row.node === 0) continue; // the capability preamble: it has its own table
  items.push({ at: row.index, order: 1, type: 'trace', module: row.module, node: row.node, tick: row.tick, text: row.text, count: row.count, lastTick: row.last_tick, boot: row.boot });
}
for (const step of s.steps) {
  const crash = step.node !== null && /^crash\b/.test(step.text);
  const restart = step.node !== null && /^restart\b/.test(step.text);
  items.push({ at: step.after_events, order: 0, type: step.kind, module: step.kind, node: step.node, tick: step.tick, text: step.text, marker: crash ? 'crash' : restart ? 'restart' : null });
}
items.sort((a, b) => a.at - b.at || a.order - b.order);

const presentModules = [...new Set(items.map((i) => i.module))];
const rowsPerModule = {};
for (const row of s.timeline) if (row.node !== 0) rowsPerModule[row.module] = (rowsPerModule[row.module] ?? 0) + 1;

const laneHead = nodes.map((n) => `<div class="head" style="grid-column:${colOf.get(n)}">node ${n}</div>`).join('');

let body = '';
let gridRow = 1; // row 1 holds the lane headings; each item gets its own row
for (const item of items) {
  gridRow += 1;
  const tick = `<div class="tick" style="grid-column:1;grid-row:${gridRow}"><span class="sr">tick </span>t${esc(item.tick)}</div>`;
  const cls = `m-${esc(item.module)}`;
  if (item.node === null || !colOf.has(item.node)) {
    body += `${tick}<div class="cell band ${cls}" style="grid-column:2 / ${nodes.length + 2};grid-row:${gridRow}"><b>${esc(MODULE_LABEL[item.module] ?? item.module)}</b> ${esc(item.text)}</div>`;
    continue;
  }
  const extra = item.count > 1 ? ` <span class="rep">x${item.count}, last at t${esc(item.lastTick)}</span>` : '';
  const boot = item.boot !== undefined ? `<span class="boot">boot ${esc(item.boot)}</span>` : '';
  const mark = item.marker === 'crash' ? '<span class="mk crash" title="crash">&#10005;</span>' : item.marker === 'restart' ? '<span class="mk restart" title="restart">&#8635;</span>' : '';
  const kind = item.type === 'trace' ? `<b>${esc(item.module)}</b>` : `<b>${esc(MODULE_LABEL[item.module])}</b>`;
  body += `${tick}<div class="cell ${cls}${item.marker ? ' ' + item.marker : ''}" style="grid-column:${colOf.get(item.node)};grid-row:${gridRow}"><span class="sr">node ${esc(item.node)}: </span>${mark}${kind} ${esc(item.text)}${extra} ${boot}</div>`;
}

const legend = presentModules
  .map((m) => `<span class="chip m-${esc(m)}">${esc(MODULE_LABEL[m] ?? m)}${PACKAGES[m] ? ` <i>${esc(PACKAGES[m])}</i>` : ''}</span>`)
  .join('');

const capRows = s.capabilities
  .map((c) => {
    const n = rowsPerModule[c.package] ?? 0;
    const seen = n ? `${n} row${n === 1 ? '' : 's'} in this run` : '-';
    const odd = c.state === 'Unavailable' && n > 0 ? ' <span class="warn">declared Unavailable, but it produced rows</span>' : '';
    return `<tr><td>${esc(c.package)}</td><td><span class="st st-${esc(c.state)}">${esc(c.state)}</span></td><td>${esc(PACKAGES[c.package] ?? '')}</td><td>${seen}${odd}</td></tr>`;
  })
  .join('');

const plumbing = Object.entries(s.plumbing)
  .map(([k, v]) => `${esc(k)} ${v}`)
  .join(', ');

const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>rDB demo timeline</title>
<style>
:root {
  color-scheme: light dark;
  --bg: #ffffff; --fg: #1c1f24; --muted: #5b6470; --line: #d6dbe1; --panel: #f5f7fa;
  --A1: #1d69df; --P1: #8a4fd6; --F1: #c5420e; --R1: #177e4f; --L1: #966319; --T1: #c2255c;
  --H1: #6b7280; --M1: #0e7490; --client: #0f766e; --env: #6b7280; --scenario: #475569; --probe: #7c3aed;
  --crash: #c92a2a; --restart: #277e38; --wired: #277e38; --unavail: #966319;
  --mk-fg: #ffffff;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #12151a; --fg: #e6e9ee; --muted: #9aa4b2; --line: #2d343d; --panel: #1a1f26;
    --A1: #6ea8ff; --P1: #c4a1ff; --F1: #ff9a66; --R1: #5fd39b; --L1: #f0c36a; --T1: #ff7aa8;
    --H1: #9aa4b2; --M1: #5fd0e6; --client: #4fd1c5; --env: #9aa4b2; --scenario: #94a3b8; --probe: #b79cff;
    --crash: #ff6b6b; --restart: #69db7c; --wired: #69db7c; --unavail: #f0c36a;
    --mk-fg: #12151a;
  }
}
* { box-sizing: border-box; }
body { margin: 0; padding: 16px; background: var(--bg); color: var(--fg); font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width: 1200px; margin: 0 auto; }
h1 { font-size: 20px; margin: 0 0 4px; }
h2 { font-size: 15px; margin: 24px 0 8px; }
p, li { color: var(--muted); margin: 4px 0; }
.note { background: var(--panel); border: 1px solid var(--line); border-radius: 6px; padding: 8px 12px; color: var(--fg); }
.chips { display: flex; flex-wrap: wrap; gap: 6px; margin: 8px 0; }
.chip { border: 1px solid currentColor; border-left-width: 6px; border-radius: 4px; padding: 2px 8px; font-size: 12px; }
.chip i { font-style: normal; color: var(--muted); }
.lanes { display: grid; grid-template-columns: 64px repeat(${nodes.length}, minmax(200px, 1fr)); gap: 0 6px; overflow-x: auto; align-items: stretch; }
.head { grid-row: 1; position: sticky; top: 0; z-index: 1; background: var(--bg); border-bottom: 2px solid var(--line); font-weight: 600; padding: 6px 4px; text-align: center; }
.cell, .tick { position: relative; }
.sr { position: absolute; width: 1px; height: 1px; margin: -1px; padding: 0; overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; border: 0; }
.tick { color: var(--muted); font: 12px ui-monospace, Consolas, monospace; padding: 6px 4px 0 0; text-align: right; border-right: 1px solid var(--line); }
.cell { border-left: 5px solid var(--env); background: var(--panel); margin: 2px 0; padding: 3px 8px; border-radius: 0 4px 4px 0; overflow-wrap: anywhere; font-size: 13px; }
.cell b { font-size: 11px; letter-spacing: .03em; margin-right: 4px; }
.band { text-align: left; border-left-style: dashed; }
.m-scenario, .m-probe { border-left-style: dashed; }
.rep, .boot { color: var(--muted); font-size: 11px; }
.mk { display: inline-block; width: 18px; height: 18px; line-height: 18px; text-align: center; border-radius: 50%; color: var(--mk-fg); font-size: 12px; margin-right: 6px; vertical-align: middle; }
.mk.crash { background: var(--crash); }
.mk.restart { background: var(--restart); }
.cell.crash { outline: 2px solid var(--crash); }
.cell.restart { outline: 2px solid var(--restart); }
${['A1', 'P1', 'F1', 'R1', 'L1', 'T1', 'H1', 'M1', 'client', 'env', 'scenario', 'probe']
  .map((m) => `.m-${m} { border-color: var(--${m}); color: var(--fg); } .chip.m-${m} { color: var(--${m}); } .cell.m-${m} b { color: var(--${m}); }`)
  .join('\n')}
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; border-bottom: 1px solid var(--line); padding: 5px 8px; vertical-align: top; }
th { color: var(--muted); font-weight: 600; }
.st { font-weight: 600; }
.st-Wired { color: var(--wired); }
.st-Unavailable { color: var(--unavail); }
.warn { color: var(--unavail); font-size: 12px; }
.kv { display: grid; grid-template-columns: max-content 1fr; gap: 2px 14px; }
.kv dt { color: var(--muted); } .kv dd { margin: 0; }
@media (max-width: 640px) { body { padding: 12px 16px; } }
</style>
</head>
<body>
<main>
<h1>rDB demo: a primary crashes, a survivor takes over</h1>
<p>One deterministic simulator run, ${nodes.length} nodes, partition ${esc(s.partition)}. Ticks are the simulator's logical clock.</p>

<h2>Read this first</h2>
<div class="note">
  Solid bars are <b>trace events</b> the simulator recorded. Dashed bars are not trace events: <b>scenario</b> bars are things the demo did
  (crash, restart, seed a plan, call a modelled service, send a write) and <b>probe</b> bars read kernel state after the run.
  This shows the simulator's write path. T1 transactions are held Unavailable (V-R40), so this is not a client-transaction demo.
  ${esc(s.seed_note.charAt(0).toUpperCase() + s.seed_note.slice(1))}.
</div>

<h2>Legend</h2>
<div class="chips">${legend}</div>
<p><span class="mk crash">&#10005;</span> crash (scenario step) &nbsp; <span class="mk restart">&#8635;</span> restart (scenario step)</p>

<h2>Timeline</h2>
<div class="lanes">
<div class="head" style="grid-column:1">tick</div>${laneHead}
${body}
</div>

<h2>Capabilities</h2>
<p>Each line is what a module reports about itself at the start of the trace. Wired means its handler is registered and executes.</p>
<table>
<thead><tr><th>Package</th><th>Declared</th><th>What it is</th><th>Seen in this run</th></tr></thead>
<tbody>${capRows}</tbody>
</table>
<p>Declared Unavailable and seen in this run do not agree for some packages. Treat the declaration as the module's own claim, not as proof the logic is absent. It is reported here as found.</p>

<h2>Run</h2>
<dl class="kv">
<dt>case</dt><dd>${esc(s.case)}</dd>
<dt>provenance</dt><dd>${esc(JSON.stringify(s.provenance))}</dd>
<dt>trace events</dt><dd>${esc(s.trace_events)} recorded; ${s.timeline.filter((r) => r.node !== 0).length} rows shown above</dd>
<dt>left out as plumbing</dt><dd>${plumbing || 'none'}</dd>
</dl>
</main>
</body>
</html>
`;

try {
  writeFileSync(outPath, html);
} catch (error) {
  console.error(`cannot write ${outPath}: ${error.message}`);
  process.exit(1);
}
console.log(`wrote ${outPath} (${items.length} items, ${nodes.length} lanes, ${s.capabilities.length} capabilities)`);
