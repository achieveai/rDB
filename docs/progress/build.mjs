#!/usr/bin/env node
// docs/progress/build.mjs
//
// Builds docs/progress/index.html from the small data pieces in docs/progress/src.
// Plain Node ESM. Zero dependencies. See docs/progress/src/SCHEMA.md for the piece formats.
//
// CLI:
//   node build.mjs                validate, write index.html, advance meta/changes/log
//   node build.mjs --out <path>   validate, write the page to <path>
//   node build.mjs --check        validate only, print errors, exit 1 on any, write nothing
//   node build.mjs --touch        skip content validation, bump meta.updated, rewrite the page
//   node build.mjs --src <dir>    read pieces from <dir> instead of ./src (for testing)
//   node build.mjs --verify [f]   render f (default index.html) in headless Edge, count diagrams
//                                 and errors, write a script-free copy to .preview/index.html

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execSync, execFileSync } from 'node:child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '..', '..');
const DEFAULT_SRC_DIR = path.join(__dirname, 'src');
const VENDOR_MERMAID = path.join(__dirname, 'vendor', 'mermaid.min.js');
// docs/progress/config.json names the live work folder (ledger + refresh log) and the archive.
// A new session edits work_dir there; nothing else hard-codes a conversation folder.
const CONFIG = (() => {
  const p = path.join(__dirname, 'config.json');
  return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, 'utf8')) : {};
})();
const WORK_DIR = CONFIG.work_dir || '.scratchpad/work';
const LEDGER_PATH = `${WORK_DIR}/ledger.md`;
const REFRESH_LOG = path.join(REPO_ROOT, WORK_DIR, 'progress-refresh-log.md');

// ---------------------------------------------------------------------------
// Vocabularies (docs/progress/src/SCHEMA.md, "Shared vocabulary")
// ---------------------------------------------------------------------------

const CHIP_VALUES = new Set(['Not started', 'In progress', 'Blocked', 'Review', 'Gate passed', 'Done']);
const STAGE_VALUES = new Set(['design', 'develop', 'test', 'review', 'gate', 'committed', 'release']);
const PART_STATE_VALUES = new Set(['built', 'building', 'planned', 'gap']);
const MARK_VALUES = new Set(['proven', 'risk', 'open', 'failed']);
const AGENT_STATUS_VALUES = new Set(['running', 'blocked', 'finished']);
const AGENT_ROLE_VALUES = new Set(['Lead', 'Developers', 'Testers', 'Critics', 'Docs']);
const RISK_SEVERITY_VALUES = new Set(['low', 'medium', 'high']);
const CHANGE_KIND_VALUES = new Set(['gate', 'commit', 'dispatch', 'handoff', 'verdict', 'risk', 'ruling', 'decision', 'note']);
const PROSE_FIELDS = new Set(['text', 'summary', 'task', 'where', 'blocked', 'next', 'label', 'lead']);
const BUILT_LIKE = new Set(['built', 'gap']);
const STATE_META = {
  built: { glyph: '✓', word: 'built' },
  building: { glyph: '▶', word: 'building' },
  planned: { glyph: '○', word: 'planned' },
  gap: { glyph: '⚠', word: 'gap' },
};
const CHIP_SLUG = {
  'Not started': 'not-started',
  'In progress': 'in-progress',
  'Blocked': 'blocked',
  'Review': 'review',
  'Gate passed': 'gate-passed',
  'Done': 'done',
};
const STAGE_ORDER = ['design', 'develop', 'test', 'review', 'gate'];
const DIAGRAM_TITLE_FALLBACK = { 'system-map': 'System map' };

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const args = { out: null, check: false, touch: false, src: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--out') args.out = argv[++i];
    else if (a === '--check') args.check = true;
    else if (a === '--touch') args.touch = true;
    else if (a === '--src') args.src = argv[++i];
    else { console.error(`Unknown argument: ${a}`); process.exit(1); }
  }
  return args;
}

const EDGE_PATHS = [
  'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
  'C:/Program Files/Microsoft/Edge/Application/msedge.exe',
];

// Renders a built page the way a browser does and reports what Mermaid drew. The in-app
// browser shows local files as a static snapshot with no scripts and a ~900 KB cap, so the
// full page (Mermaid inlined) cannot be viewed there; the script-free .preview copy can.
function verify(file) {
  const target = path.resolve(process.cwd(), file || path.join(__dirname, 'index.html'));
  const edge = EDGE_PATHS.find((p) => fs.existsSync(p));
  if (!edge) { console.error('verify: Microsoft Edge not found; open the page in a browser instead.'); process.exit(1); }
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'progress-verify-'));
  let dom;
  try {
    dom = execFileSync(edge, ['--headless=new', '--disable-gpu', `--user-data-dir=${profile}`,
      '--virtual-time-budget=15000', '--dump-dom', pathToFileURL(target).href],
    { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, stdio: ['ignore', 'pipe', 'ignore'] });
  } finally {
    fs.rmSync(profile, { recursive: true, force: true });
  }
  const count = (re) => (dom.match(re) || []).length;
  const expected = count(/<pre class="mermaid"/g);
  const drawn = count(/aria-roledescription="flowchart-v2"/g);
  const errs = count(/aria-roledescription="error"/g);
  const preview = dom.replace(/<script\b[\s\S]*?<\/script>/gi, '');
  const previewPath = path.join(__dirname, '.preview', 'index.html');
  fs.mkdirSync(path.dirname(previewPath), { recursive: true });
  // The preview is local-only: keep it out of git via .git/info/exclude, not a committed ignore.
  const excl = path.join(REPO_ROOT, '.git', 'info', 'exclude');
  const exclLine = '/docs/progress/.preview/';
  if (fs.existsSync(path.dirname(excl)) && !(fs.existsSync(excl) && fs.readFileSync(excl, 'utf8').includes(exclLine))) {
    fs.appendFileSync(excl, `\n${exclLine}\n`);
  }
  fs.writeFileSync(previewPath, preview, 'utf8');
  console.log(`verify: ${drawn} of ${expected} diagrams drawn, ${errs} Mermaid error(s).`);
  console.log(`verify: static preview ${Math.round(preview.length / 1024)} KB at ${relPath(previewPath)}`);
  if (drawn !== expected || errs > 0) process.exit(1);
}

function main() {
  const argv = process.argv.slice(2);
  if (argv[0] === '--verify') { verify(argv[1]); return; }
  const args = parseArgs(argv);
  const srcDir = args.src ? path.resolve(process.cwd(), args.src) : DEFAULT_SRC_DIR;
  const errors = [];
  const pieces = loadPieces(srcDir, errors);

  if (!args.touch) {
    validateAll(pieces, errors);
  }

  if (errors.length > 0) {
    for (const e of errors) console.error(e);
    console.error(`\n${errors.length} error(s). Nothing written.`);
    process.exit(1);
  }

  if (args.check) {
    console.log('OK: all pieces in ' + relPath(srcDir) + ' are valid.');
    process.exit(0);
  }

  const canonical = path.join(path.dirname(DEFAULT_SRC_DIR), 'index.html');
  const outPath = args.out ? path.resolve(process.cwd(), args.out) : path.join(path.dirname(srcDir), 'index.html');

  // Publishing the live pieces to the real page *is* the refresh, however it was spelled.
  // Rebuilding an archived snapshot (`--src`) or writing a scratch copy elsewhere is not one,
  // and must not move the watermark.
  const refreshing = !args.src && outPath === canonical;

  // The new stamp goes into the page *before* it renders. Advancing afterwards left every page
  // carrying the previous run's timestamp, so a refresh that worked was indistinguishable from
  // one that never ran without opening meta.json. A report that cannot say when it was built is
  // failing at the one job a status page has.
  const pending = refreshing ? nextMeta(srcDir, pieces) : null;
  if (pending) pieces.meta = pending;

  const html = renderPage(pieces);
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, html, 'utf8');
  console.log(`Wrote ${relPath(outPath)}`);

  // Only after the page is on disk: a failed write must not consume changes it never showed.
  if (pending) commitMeta(srcDir, pending);
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

function relPath(p) {
  return path.relative(process.cwd(), p).split(path.sep).join('/');
}

function readJSONSafe(file, errors, fallback) {
  if (!fs.existsSync(file)) {
    errors.push(`${relPath(file)}: file not found`);
    return fallback;
  }
  const text = fs.readFileSync(file, 'utf8');
  try {
    return JSON.parse(text);
  } catch (e) {
    errors.push(`${relPath(file)}: invalid JSON: ${e.message}`);
    return fallback;
  }
}

function loadPieces(srcDir, errors) {
  const metaPath = path.join(srcDir, 'meta.json');
  const meta = readJSONSafe(metaPath, errors, { updated: null, ledger: { path: '', line: 0 }, git_head: '', part_states: {} });
  const now = readJSONSafe(path.join(srcDir, 'now.json'), errors, {});
  const milestones = readJSONSafe(path.join(srcDir, 'milestones.json'), errors, { milestones: [] });
  const work = readJSONSafe(path.join(srcDir, 'work.json'), errors, { agents: [] });
  const risks = readJSONSafe(path.join(srcDir, 'risks.json'), errors, { risks: [] });
  const parts = readJSONSafe(path.join(srcDir, 'parts.json'), errors, { parts: [] });

  const diagramsDir = path.join(srcDir, 'diagrams');
  const diagrams = [];
  if (fs.existsSync(diagramsDir)) {
    const files = fs.readdirSync(diagramsDir).filter((f) => f.endsWith('.mmd')).sort();
    for (const f of files) {
      diagrams.push({ name: f, src: fs.readFileSync(path.join(diagramsDir, f), 'utf8') });
    }
  } else {
    errors.push(`${relPath(diagramsDir)}: directory not found`);
  }

  const referencePath = path.join(srcDir, 'reference.html');
  let reference = '';
  if (fs.existsSync(referencePath)) {
    reference = fs.readFileSync(referencePath, 'utf8');
  } else {
    errors.push(`${relPath(referencePath)}: file not found`);
  }

  const changesPath = path.join(srcDir, 'changes.json');
  const changes = fs.existsSync(changesPath) ? readJSONSafe(changesPath, errors, null) : null;

  return { srcDir, meta, now, milestones, work, risks, parts, diagrams, reference, changes, changesPath };
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

function splitSentences(text) {
  const trimmed = text.trim();
  if (!trimmed) return [];
  return trimmed.split(/(?<=[.!?])\s+/).filter((s) => s.length > 0);
}

function wordCount(s) {
  return s.trim().split(/\s+/).filter(Boolean).length;
}

function hasBadDash(s) {
  if (s.includes('—')) return 'an em-dash';
  if (s.includes('–')) return 'an en-dash';
  if (/\s-\s/.test(s) || /^-\s/.test(s) || /\s-$/.test(s)) return 'a dash used as a dash';
  if (/--/.test(s)) return 'a double hyphen';
  return null;
}

function checkProse(errors, file, jsonPath, value) {
  if (typeof value !== 'string' || !value) return;
  const bad = hasBadDash(value);
  if (bad) errors.push(`${file}: ${jsonPath}: contains ${bad}: "${value}"`);
  for (const sentence of splitSentences(value)) {
    const wc = wordCount(sentence);
    if (wc > 20) errors.push(`${file}: ${jsonPath}: sentence has ${wc} words, max 20: "${sentence}"`);
  }
}

function walkProseFields(errors, file, node, jsonPath) {
  if (Array.isArray(node)) {
    node.forEach((item, i) => walkProseFields(errors, file, item, `${jsonPath}[${i}]`));
  } else if (node && typeof node === 'object') {
    for (const [k, v] of Object.entries(node)) {
      const p = `${jsonPath}.${k}`;
      if (PROSE_FIELDS.has(k) && typeof v === 'string') checkProse(errors, file, p, v);
      walkProseFields(errors, file, v, p);
    }
  }
}

function validateNow(now, errors) {
  for (const f of ['where', 'blocked', 'next']) {
    if (!now || typeof now[f] !== 'string' || !now[f].trim()) {
      errors.push(`src/now.json: $.${f}: required, non-empty string`);
    }
  }
}

function validateParts(partsDoc, errors) {
  const list = Array.isArray(partsDoc?.parts) ? partsDoc.parts : [];
  if (!Array.isArray(partsDoc?.parts)) errors.push('src/parts.json: $.parts: must be an array');
  const seen = new Set();
  list.forEach((part, i) => {
    const p = `$.parts[${i}]`;
    if (!part || !part.id) { errors.push(`src/parts.json: ${p}.id: required`); return; }
    if (seen.has(part.id)) errors.push(`src/parts.json: ${p}.id: duplicate id "${part.id}"`);
    seen.add(part.id);
    if (!PART_STATE_VALUES.has(part.state)) {
      errors.push(`src/parts.json: ${p}.state: "${part.state}" is not one of built|building|planned|gap`);
    }
    if ((part.state === 'built' || part.state === 'gap') && !(part.evidence && String(part.evidence).trim())) {
      errors.push(`src/parts.json: ${p}.evidence: "${part.state}" parts require non-empty evidence`);
    }
  });
}

function validateMilestones(doc, errors) {
  const list = Array.isArray(doc?.milestones) ? doc.milestones : [];
  const seen = new Set();
  list.forEach((m, i) => {
    const p = `$.milestones[${i}]`;
    if (!m || !m.id) { errors.push(`src/milestones.json: ${p}.id: required`); return; }
    if (seen.has(m.id)) errors.push(`src/milestones.json: ${p}.id: duplicate id "${m.id}"`);
    seen.add(m.id);
    if (!CHIP_VALUES.has(m.chip)) errors.push(`src/milestones.json: ${p}.chip: "${m.chip}" is not a known status chip`);
    if (!STAGE_VALUES.has(m.stage)) errors.push(`src/milestones.json: ${p}.stage: "${m.stage}" is not a known stage`);
    const status = Array.isArray(m.status) ? m.status : [];
    if (status.length > 3) errors.push(`src/milestones.json: ${p}.status: has ${status.length} entries, max 3`);
    const acc = Array.isArray(m.acceptance) ? m.acceptance : [];
    acc.forEach((a, j) => {
      const ap = `${p}.acceptance[${j}]`;
      if (!MARK_VALUES.has(a.mark)) errors.push(`src/milestones.json: ${ap}.mark: "${a.mark}" is not one of proven|risk|open|failed`);
      if ((a.mark === 'proven' || a.mark === 'risk') && !(a.evidence && String(a.evidence).trim())) {
        errors.push(`src/milestones.json: ${ap}.evidence: mark "${a.mark}" requires non-empty evidence`);
      }
    });
  });
}

function validateWork(doc, errors) {
  const list = Array.isArray(doc?.agents) ? doc.agents : [];
  const seen = new Set();
  list.forEach((a, i) => {
    const p = `$.agents[${i}]`;
    if (!a || !a.id) { errors.push(`src/work.json: ${p}.id: required`); return; }
    if (seen.has(a.id)) errors.push(`src/work.json: ${p}.id: duplicate id "${a.id}"`);
    seen.add(a.id);
    if (!AGENT_ROLE_VALUES.has(a.role)) errors.push(`src/work.json: ${p}.role: "${a.role}" is not one of Lead|Developers|Testers|Critics|Docs`);
    if (!AGENT_STATUS_VALUES.has(a.status)) errors.push(`src/work.json: ${p}.status: "${a.status}" is not one of running|blocked|finished`);
  });
}

function validateRisks(doc, errors) {
  const list = Array.isArray(doc?.risks) ? doc.risks : [];
  list.forEach((r, i) => {
    const p = `$.risks[${i}]`;
    if (!r || !(r.owner && String(r.owner).trim())) errors.push(`src/risks.json: ${p}.owner: required, non-empty owner`);
    if (r && r.severity !== undefined && !RISK_SEVERITY_VALUES.has(r.severity)) {
      errors.push(`src/risks.json: ${p}.severity: "${r.severity}" is not one of low|medium|high`);
    }
  });
}

function validateChanges(doc, errors) {
  const list = Array.isArray(doc?.items) ? doc.items : [];
  list.forEach((c, i) => {
    const p = `$.items[${i}]`;
    if (!CHANGE_KIND_VALUES.has(c.kind)) errors.push(`src/changes.json: ${p}.kind: "${c.kind}" is not a known change kind`);
  });
}

function extractTokens(src) {
  const re = /@\{([a-zA-Z0-9_-]+)\}/g;
  const out = [];
  let m;
  while ((m = re.exec(src)) !== null) out.push(m[1]);
  return out;
}

function validateDiagrams(diagrams, partsById, errors) {
  const usedAnywhere = new Set();
  for (const d of diagrams) {
    const { caption } = parseDiagramDirectives(d.src);
    if (!caption) errors.push(`src/diagrams/${d.name}: missing "%% caption:" line; say how to read the diagram in one sentence`);
    checkProse(errors, `src/diagrams/${d.name}`, 'caption', caption);
    const distinct = new Set(extractTokens(d.src));
    if (distinct.size > 16) {
      errors.push(`src/diagrams/${d.name}: contains ${distinct.size} distinct parts, max 16`);
    }
    // Mermaid drops a subgraph's `direction LR` when an arrow leaves it, which collapses
    // the rows. So a part-to-part arrow must stay inside one subgraph; link rows by
    // subgraph id instead (for example `WP1 --> WP2`).
    const groupOf = {};
    const stack = [];
    d.src.split('\n').forEach((line, i) => {
      const sg = line.match(/^\s*subgraph\s+([A-Za-z0-9_]+)/);
      if (sg) { stack.push(sg[1]); return; }
      if (/^\s*end\s*$/.test(line)) { stack.pop(); return; }
      if (/^\s*%%/.test(line)) return;
      if (!/(-->|-\.->|---)/.test(line)) {
        for (const id of extractTokens(line)) if (!(id in groupOf)) groupOf[id] = stack[stack.length - 1] || '';
        return;
      }
      const bare = line.replace(/@\{[^}]*\}/g, ' ').replace(/-\.->|-->|---/g, ' ').split(/\s+/);
      for (const word of bare) {
        if (partsById[word]) {
          errors.push(`src/diagrams/${d.name}: line ${i + 1} uses bare "${word}"; write @{${word}} so the arrow joins the part's box`);
        }
      }
      const ends = extractTokens(line);
      if (ends.length === 2 && ends[0] in groupOf && ends[1] in groupOf && groupOf[ends[0]] !== groupOf[ends[1]]) {
        errors.push(`src/diagrams/${d.name}: line ${i + 1} joins parts in different subgraphs; link the subgraphs instead, or the rows collapse`);
      }
    });
    const perGroup = {};
    for (const [id, g] of Object.entries(groupOf)) if (g) (perGroup[g] = perGroup[g] || []).push(id);
    for (const [g, ids] of Object.entries(perGroup)) {
      if (ids.length > 4) errors.push(`src/diagrams/${d.name}: subgraph ${g} has ${ids.length} parts, max 4 per row; split the row`);
    }
    for (const id of distinct) {
      usedAnywhere.add(id);
      if (!partsById[id]) {
        errors.push(`src/diagrams/${d.name}: @{${id}} has no matching entry in src/parts.json`);
      }
    }
  }
  for (const id of Object.keys(partsById)) {
    if (!usedAnywhere.has(id)) {
      errors.push(`src/parts.json: part "${id}" does not appear in any diagram`);
    }
  }
}

function indexParts(partsDoc) {
  const byId = {};
  for (const part of (partsDoc?.parts || [])) {
    if (part && part.id) byId[part.id] = part;
  }
  return byId;
}

// Cross-field consistency, rules 1-6 only. A seventh rule -- "a blocker-severity risk implies
// a blocked or at-risk milestone" -- has no vocabulary to stand on: risk `severity` is
// low|medium|high with no "blocker", and milestone `chip` has no "at-risk" value (SCHEMA.md).
const FINISHED_CHIPS = new Set(['Done', 'Gate passed']);
const BUILT_OK_STAGES = new Set(['test', 'review', 'gate']);

function validateCrossField(p, errors) {
  const milestonesById = {};
  for (const m of (p.milestones.milestones || [])) {
    if (m && m.id) milestonesById[m.id] = m;
  }

  // Rule 1: a finished chip (Done | Gate passed) requires a gate_commit and no acceptance
  // mark left `open` or `failed`. A `risk` mark is allowed and must stay allowed: a gate
  // that ran clean and a criterion that was met in full are different facts, and a milestone
  // can honestly ship with a named, evidenced scope limit (ADR-0031). Demanding `proven`
  // everywhere does not raise the bar, it just moves the pressure onto whoever fills the
  // marks in — the report then reads `proven` beside evidence that says otherwise.
  // Rule 2: a gate_commit implies the chip is a finished chip.
  for (const m of (p.milestones.milestones || [])) {
    if (!m || !m.id) continue;
    const loc = `src/milestones.json: milestone "${m.id}"`;
    const finished = FINISHED_CHIPS.has(m.chip);
    if (finished) {
      (Array.isArray(m.acceptance) ? m.acceptance : []).forEach((a, j) => {
        if (a.mark === 'open' || a.mark === 'failed') {
          errors.push(`${loc}.acceptance[${j}].mark: chip "${m.chip}" cannot carry an "${a.mark}" acceptance mark; prove it, or mark it "risk" with evidence naming the limit`);
        }
      });
      if (!m.gate_commit) {
        errors.push(`${loc}.gate_commit: chip "${m.chip}" requires gate_commit to be set`);
      }
    }
    if (m.gate_commit && !finished) {
      errors.push(`${loc}.chip: gate_commit is set but chip is "${m.chip}", expected "Done" or "Gate passed"`);
    }
  }

  // Rule 3: any milestone chip "Blocked" requires now.blocked to name the block.
  const anyBlocked = (p.milestones.milestones || []).some((m) => m.chip === 'Blocked');
  if (anyBlocked) {
    const blockedText = String(p.now.blocked || '').trim().toLowerCase();
    if (!blockedText || blockedText === 'nothing' || blockedText === 'nothing.') {
      errors.push('src/now.json: $.blocked: a milestone chip is "Blocked" but blocked is empty or "Nothing"');
    }
  }

  // Rule 4: now.where naming a milestone id requires that milestone to exist and to
  // not already be finished.
  const mentioned = new Set(String(p.now.where || '').match(/\bM[0-6]\b/g) || []);
  for (const id of mentioned) {
    const m = milestonesById[id];
    if (!m) {
      errors.push(`src/now.json: $.where: names milestone "${id}" which does not exist in src/milestones.json`);
    } else if (FINISHED_CHIPS.has(m.chip)) {
      errors.push(`src/now.json: $.where: names milestone "${id}" whose chip is "${m.chip}" (already finished)`);
    }
  }

  // Rule 5: a built part's milestone must be finished, or in stage test|review|gate.
  // A finished milestone may not own a planned part.
  for (const part of (p.parts.parts || [])) {
    if (!part || !part.id) continue;
    const m = milestonesById[part.milestone];
    if (!m) continue;
    const finished = FINISHED_CHIPS.has(m.chip);
    if (part.state === 'built' && !finished && !BUILT_OK_STAGES.has(m.stage)) {
      errors.push(`src/parts.json: part "${part.id}": state is "built" but milestone "${m.id}" is not finished and its stage is "${m.stage}", expected test|review|gate`);
    }
    if (part.state === 'planned' && finished) {
      errors.push(`src/parts.json: part "${part.id}": state is "planned" but milestone "${m.id}" is already finished ("${m.chip}")`);
    }
  }

  // Rule 6: a running or blocked agent's milestone must not already be finished.
  for (const a of (p.work.agents || [])) {
    if (!a || !a.id) continue;
    if (a.status === 'running' || a.status === 'blocked') {
      const m = milestonesById[a.milestone];
      if (m && FINISHED_CHIPS.has(m.chip)) {
        errors.push(`src/work.json: agent "${a.id}": status "${a.status}" but milestone "${a.milestone}" is already finished ("${m.chip}")`);
      }
    }
  }
}

function validateAll(p, errors) {
  validateNow(p.now, errors);
  validateParts(p.parts, errors);
  const partsById = indexParts(p.parts);
  validateMilestones(p.milestones, errors);
  validateWork(p.work, errors);
  validateRisks(p.risks, errors);
  if (p.changes) validateChanges(p.changes, errors);
  validateDiagrams(p.diagrams, partsById, errors);
  validateCrossField(p, errors);

  walkProseFields(errors, 'src/now.json', p.now, '$');
  walkProseFields(errors, 'src/milestones.json', p.milestones, '$');
  walkProseFields(errors, 'src/work.json', p.work, '$');
  walkProseFields(errors, 'src/risks.json', p.risks, '$');
  walkProseFields(errors, 'src/parts.json', p.parts, '$');
}

// ---------------------------------------------------------------------------
// Mermaid transform (docs/progress/src/SCHEMA.md, "src/diagrams/NN-name.mmd")
// ---------------------------------------------------------------------------

function safeNodeId(id) {
  return 'p_' + id.replace(/-/g, '_');
}

function escapeMermaidLabel(s) {
  return String(s).replace(/"/g, "'");
}

function parseDiagramDirectives(src) {
  const lines = src.split(/\r?\n/);
  let title = null;
  let open = 'collapsed';
  let milestones = [];
  let caption = '';
  const body = [];
  for (const line of lines) {
    const m = line.match(/^%%\s*(title|caption|open|milestones):\s*(.*)$/);
    if (m) {
      if (m[1] === 'title') title = m[2].trim();
      else if (m[1] === 'caption') caption = m[2].trim();
      else if (m[1] === 'open') open = m[2].trim();
      else if (m[1] === 'milestones') milestones = m[2].trim().split(/\s+/).filter(Boolean);
      continue;
    }
    body.push(line);
  }
  return { title, caption, open, milestones, body: body.join('\n') };
}

// Replaces every @{id} token: first occurrence becomes a full node definition
// (glyph, word, milestone tag, label, inline state/new classes); later
// occurrences become the bare safe node id. Arrows are then rewritten solid
// between two built-like parts, dashed otherwise. classDef lines are appended.
function transformMermaid(diagramSrc, partsById, oldPartStates) {
  const { title, caption, open, milestones, body: rawBody } = parseDiagramDirectives(diagramSrc);
  const tokenRe = /@\{([a-zA-Z0-9_-]+)\}/g;
  const firstSeen = new Set();
  const idBySafe = {};
  const changedSids = [];
  const distinctIds = new Set(extractTokens(rawBody));
  const counts = { built: 0, gap: 0, building: 0, planned: 0 };
  for (const id of distinctIds) {
    const part = partsById[id];
    if (part && part.state in counts) counts[part.state]++;
  }
  const builtCount = counts.built;

  let body = rawBody.replace(tokenRe, (whole, id) => {
    const part = partsById[id];
    const sid = safeNodeId(id);
    idBySafe[sid] = id;
    if (!part) return whole;
    if (!firstSeen.has(id)) {
      firstSeen.add(id);
      const meta = STATE_META[part.state] || STATE_META.planned;
      const changed = !(id in oldPartStates) || oldPartStates[id] !== part.state;
      const tagLine = `${meta.glyph} ${meta.word} · ${part.milestone}${changed ? ' new' : ''}`;
      const label = `<b>${escapeMermaidLabel(part.label)}</b><br/>${escapeMermaidLabel(tagLine)}`;
      if (changed) changedSids.push(sid);
      return `${sid}["${label}"]:::${part.state}`;
    }
    return sid;
  });

  // Rewrite arrows: solid between two built-like parts, dashed otherwise.
  body = body
    .split('\n')
    .map((line) => {
      const m = line.match(/^(\s*)([A-Za-z0-9_]+)(\s+)(?:-->|-\.->)(\|[^|]*\|)?(\s*)([A-Za-z0-9_]+)(\s*)$/);
      if (!m) return line;
      const [, indent, left, sp1, lbl, sp2, right, trail] = m;
      const leftId = idBySafe[left];
      const rightId = idBySafe[right];
      const leftPart = leftId ? partsById[leftId] : null;
      const rightPart = rightId ? partsById[rightId] : null;
      if (!leftPart || !rightPart) return line; // subgraph-to-subgraph links keep their author's arrow
      const solid = BUILT_LIKE.has(leftPart.state) && BUILT_LIKE.has(rightPart.state);
      const arrow = solid ? '-->' : '-.->';
      return `${indent}${left}${sp1}${arrow}${lbl || ''}${sp2 || ' '}${right}${trail}`;
    })
    .join('\n');

  const classDefs = [
    'classDef built fill:var(--green-soft),stroke:var(--green),stroke-width:2px,color:var(--text);',
    'classDef building fill:var(--amber-soft),stroke:var(--amber),stroke-width:3px,color:var(--text);',
    'classDef planned fill:transparent,stroke:var(--grey),stroke-width:2px,stroke-dasharray: 6 4,color:var(--text);',
    'classDef gap fill:var(--amber-soft),stroke:var(--amber),stroke-width:2px,color:var(--text);',
    'classDef changed stroke-width:4px;',
    ...(changedSids.length ? [`class ${changedSids.join(',')} changed;`] : []),
  ].join('\n');

  const accTitle = title || 'Diagram';
  const rollupBits = [`${counts.built} of ${distinctIds.size} built`];
  if (counts.gap) rollupBits.push(`${counts.gap} with ${counts.gap === 1 ? 'a gap' : 'gaps'}`);
  if (counts.building) rollupBits.push(`${counts.building} building`);
  if (counts.planned) rollupBits.push(`${counts.planned} planned`);
  const rollupText = rollupBits.join(', ');
  const allBuilt = counts.built === distinctIds.size;
  const acc = `accTitle: ${accTitle}\naccDescr: ${rollupText}`;

  const bodyLines = body.split('\n');
  const typeIdx = bodyLines.findIndex((l) => l.trim() !== '');
  bodyLines.splice(typeIdx + 1, 0, acc);
  const finalSrc = `${bodyLines.join('\n')}\n${classDefs}`;
  return { title, caption, open, milestones, src: finalSrc, built: builtCount, total: distinctIds.size, rollupText, allBuilt };
}

// ---------------------------------------------------------------------------
// Gate pipeline (flowchart LR) and swim lanes (HTML columns), computed from data
// ---------------------------------------------------------------------------

function buildGatePipeline(milestone) {
  const idx = STAGE_ORDER.indexOf(milestone.stage);
  const boxes = ['Design', 'Develop', 'Test', 'Review', 'Gate'].map((name, i) => {
    let glyph = '☐';
    let word = 'pending';
    let cls = 'pending';
    if (idx >= 0 && i < idx) { glyph = '☑'; word = 'done'; cls = 'done'; }
    else if (idx >= 0 && i === idx) {
      if (milestone.chip === 'Blocked') { glyph = '✕'; word = 'failed'; cls = 'failed'; }
      else { glyph = '▶'; word = 'active'; cls = 'active'; }
    }
    return { name, glyph, word, cls };
  });
  const nodeIds = ['gp_design', 'gp_develop', 'gp_test', 'gp_review', 'gp_gate'];
  const lines = ['flowchart LR'];
  boxes.forEach((b, i) => {
    lines.push(`  ${nodeIds[i]}["${b.glyph} ${b.name}<br/>${b.word}"]:::${b.cls}`);
  });
  for (let i = 0; i < nodeIds.length - 1; i++) {
    lines.push(`  ${nodeIds[i]} --> ${nodeIds[i + 1]}`);
  }
  lines.push('classDef done fill:var(--green-soft),stroke:var(--green),stroke-width:2px,color:var(--text);');
  lines.push('classDef active fill:var(--amber-soft),stroke:var(--amber),stroke-width:3px,color:var(--text);');
  lines.push('classDef pending fill:transparent,stroke:var(--grey),stroke-width:2px,stroke-dasharray: 6 4,color:var(--text);');
  lines.push('classDef failed fill:var(--red-soft),stroke:var(--red),stroke-width:2px,color:var(--text);');
  return lines.join('\n');
}

const ROLE_ORDER = ['Lead', 'Developers', 'Testers', 'Critics', 'Docs'];
const AGENT_STATUS_META = {
  running: { glyph: '▶', word: 'running', cls: 'running' },
  blocked: { glyph: '⚠', word: 'blocked', cls: 'blocked' },
  finished: { glyph: '✓', word: 'done', cls: 'finished' },
};

function hhmm(iso) {
  const m = /T(\d{2}:\d{2})/.exec(iso || '');
  return m ? m[1] : '';
}

// One column per role. Each column shows what that role is doing now (running or
// blocked) and its last two finished items, newest first. Empty columns say "Idle".
function buildSwimLanes(agents) {
  const lanes = ROLE_ORDER.map((role) => {
    const mine = agents.filter((a) => a.role === role);
    const live = mine.filter((a) => a.status !== 'finished');
    const done = mine.filter((a) => a.status === 'finished' && a.end)
      .sort((x, y) => String(y.end).localeCompare(String(x.end))).slice(0, 2);
    const cards = [...live, ...done].map((a) => {
      const st = AGENT_STATUS_META[a.status] || AGENT_STATUS_META.running;
      const when = a.status === 'finished' ? (hhmm(a.end) ? `finished ${hhmm(a.end)}` : '')
        : (hhmm(a.start) ? `since ${hhmm(a.start)}` : '');
      return `
          <li class="lane-card ${st.cls}"><span class="lane-st">${st.glyph} ${st.word}</span> ${esc(a.task || a.id)}<span class="lane-when">${esc(a.id)}${when ? ' · ' + when : ''}</span></li>`;
    }).join('');
    return `
      <div class="lane">
        <div class="lane-head">${esc(role)} <span class="lane-count">${live.length} active</span></div>
        <ul>${cards || '\n          <li class="lane-idle">Idle</li>'}
        </ul>
      </div>`;
  }).join('');
  return `
    <p class="diagram-lead">Who is doing what right now. One column per role. ▶ running, ⚠ blocked, ✓ just finished.</p>
    <div class="lanes">${lanes}
    </div>`;
}

// ---------------------------------------------------------------------------
// Page assembly
// ---------------------------------------------------------------------------

const PAGE_CSS = `
  :root {
    --bg: #f7f8fa;
    --panel: #ffffff;
    --text: #16181d;
    --muted: #4b5160;
    --border: #dde1e7;
    --accent: #1f6feb;
    --accent-soft: #e8f0fe;
    --green: #16794f;
    --green-soft: #e3f6ec;
    --amber: #8a5a00;
    --amber-soft: #fff2d9;
    --grey: #565c66;
    --grey-soft: #eceef1;
    --red: #b42318;
    --red-soft: #fdecea;
    --purple: #6934c9;
    --purple-soft: #f0e8fd;
    --shadow: 0 1px 2px rgba(20,20,30,0.06), 0 2px 8px rgba(20,20,30,0.04);
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0f1216;
      --panel: #171b21;
      --text: #f1f2f5;
      --muted: #b7bec9;
      --border: #2a2f38;
      --accent: #6fabff;
      --accent-soft: #16233b;
      --green: #57dba0;
      --green-soft: #10281d;
      --amber: #ffc266;
      --amber-soft: #2e2408;
      --grey: #b7bec9;
      --grey-soft: #232830;
      --red: #ff9b90;
      --red-soft: #331312;
      --purple: #bda2f7;
      --purple-soft: #241a36;
      --shadow: 0 1px 2px rgba(0,0,0,0.4), 0 2px 10px rgba(0,0,0,0.3);
    }
  }
  :root[data-theme="dark"] {
    --bg: #0f1216; --panel: #171b21; --text: #f1f2f5; --muted: #b7bec9; --border: #2a2f38;
    --accent: #6fabff; --accent-soft: #16233b; --green: #57dba0; --green-soft: #10281d;
    --amber: #ffc266; --amber-soft: #2e2408; --grey: #b7bec9; --grey-soft: #232830;
    --red: #ff9b90; --red-soft: #331312; --purple: #bda2f7; --purple-soft: #241a36;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, "Segoe UI", Inter, Roboto, Arial, sans-serif;
    font-size: 18px;
    line-height: 1.6;
  }
  .wrap { max-width: 1100px; margin: 0 auto; padding: 20px 16px 80px; }
  p, li { max-width: 70ch; }
  h1, h2, h3 { line-height: 1.3; }
  a { color: var(--accent); }

  .stale-banner {
    background: var(--red-soft); color: var(--red); border: 1px solid var(--red);
    border-radius: 10px; padding: 12px 16px; font-weight: 700; margin-bottom: 14px; font-size: 16px;
  }

  .now-box {
    background: var(--panel); border: 2px solid var(--accent); border-radius: 16px;
    padding: 20px 24px; box-shadow: var(--shadow); margin-bottom: 22px;
  }
  .now-box h1 { font-size: 22px; margin: 0 0 14px; font-weight: 800; }
  .now-line { display: flex; gap: 10px; padding: 6px 0; border-top: 1px solid var(--border); }
  .now-line:first-of-type { border-top: none; }
  .now-label { font-weight: 800; flex: 0 0 150px; }
  .now-value { color: var(--text); }
  .now-value .muted { color: var(--muted); }

  section { margin-bottom: 30px; }
  h2.section-title { font-size: 21px; font-weight: 800; margin: 0 0 12px 2px; }

  .legend-row { display: flex; flex-wrap: wrap; gap: 8px 16px; margin: 0 0 14px; font-size: 14px; color: var(--muted); }
  .legend-row .chip { font-size: 12px; }

  .strip-wrap { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 14px 18px; box-shadow: var(--shadow); margin-bottom: 22px; }
  .strip { display: flex; gap: 4px; }
  .strip-seg {
    flex: 1; text-align: center; padding: 10px 4px; border-radius: 8px; font-weight: 800; font-size: 15px;
    background: var(--grey-soft); color: var(--muted); border: 1px solid var(--border);
  }
  .strip-seg.pass { background: var(--green-soft); color: var(--green); border-color: var(--green); }
  .strip-seg.active { background: var(--amber-soft); color: var(--amber); border-color: var(--amber); }
  .strip-caption { color: var(--muted); font-size: 14px; margin-top: 10px; }

  .sys-block { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 16px 18px; box-shadow: var(--shadow); margin-bottom: 14px; }
  .sys-block.sys-fixed { border: 2px solid var(--accent); }
  .sys-block > summary { cursor: pointer; font-weight: 800; font-size: 16.5px; }
  .sys-block > summary .sys-asof { color: var(--muted); font-size: 13px; font-weight: 400; margin-left: 8px; }
  .sys-fixed-title { font-weight: 800; font-size: 16.5px; }
  .sys-fixed-title .sys-asof { color: var(--muted); font-size: 13px; font-weight: 400; margin-left: 8px; }
  .mm-legend { display: flex; flex-wrap: wrap; gap: 8px 18px; margin: 10px 0 14px; font-size: 14px; color: var(--muted); }
  .mm-legend .mm-lg-item { display: inline-flex; align-items: center; gap: 6px; }
  .mm-legend .mm-sw { display: inline-block; width: 16px; height: 16px; border-radius: 3px; flex: none; }
  .mm-legend .mm-sw.built { background: var(--green-soft); border: 2px solid var(--green); }
  .mm-legend .mm-sw.building { background: var(--amber-soft); border: 3px solid var(--amber); }
  .mm-legend .mm-sw.planned { background: transparent; border: 2px dashed var(--grey); }
  .mm-legend .mm-sw.gap { background: var(--amber-soft); border: 2px solid var(--amber); }
  .sys-caption { color: var(--muted); font-size: 14px; margin-top: 10px; }
  .diagram-panel .mermaid { overflow-x: auto; }
  .diagram-panel .mermaid svg { max-width: 100%; height: auto; }

  .board { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
  .card {
    background: var(--panel); border: 1px solid var(--border); border-radius: 14px;
    padding: 18px; box-shadow: var(--shadow); display: flex; flex-direction: column; gap: 8px;
  }
  .card-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
  .card-title { font-size: 18px; font-weight: 800; }
  .chip { font-size: 13px; font-weight: 700; padding: 4px 10px; border-radius: 999px; white-space: nowrap; }
  .chip.st-not-started { background: var(--grey-soft); color: var(--grey); }
  .chip.st-in-progress { background: var(--amber-soft); color: var(--amber); }
  .chip.st-blocked { background: var(--red-soft); color: var(--red); }
  .chip.st-review { background: var(--purple-soft); color: var(--purple); }
  .chip.st-gate-passed { background: var(--green-soft); color: var(--green); }
  .chip.st-done { background: var(--green-soft); color: var(--green); border: 1px solid var(--green); }

  .card ul.acc { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 8px; }
  .card ul.acc li { font-size: 16px; }
  .card ul.acc li .mark { font-weight: 800; margin-right: 6px; }
  .card ul.acc li .mark.yes { color: var(--green); }
  .card ul.acc li .mark.warn { color: var(--amber); }
  .card ul.acc li .mark.no { color: var(--grey); }
  .card ul.acc li .evi { display: block; color: var(--muted); font-size: 14px; margin: 2px 0 0 22px; }

  .card ul.status { list-style: disc; margin: 4px 0 0; padding-left: 20px; display: flex; flex-direction: column; gap: 6px; font-size: 17px; }

  .card .tests { font-size: 14px; color: var(--muted); border-top: 1px dashed var(--border); padding-top: 8px; margin-top: 2px; }

  details.history { margin-top: 4px; }
  details.history summary { cursor: pointer; color: var(--accent); font-weight: 700; font-size: 14px; }
  details.history ul { margin: 8px 0 0; padding-left: 20px; font-size: 14.5px; color: var(--muted); display: flex; flex-direction: column; gap: 5px; }

  .pipeline-wrap { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 16px 18px; box-shadow: var(--shadow); margin: 8px 0 18px; }
  .pipeline-note { color: var(--muted); font-size: 14.5px; margin-top: 10px; }

  ul.active-list { list-style: none; margin: 0 0 14px; padding: 0; display: flex; flex-direction: column; gap: 10px; }
  ul.active-list li { background: var(--panel); border: 1px solid var(--border); border-left: 4px solid var(--amber); border-radius: 10px; padding: 12px 16px; font-size: 16px; box-shadow: var(--shadow); }
  ul.active-list li .who { font-weight: 800; }
  ul.active-list li .since { color: var(--muted); font-size: 14px; display: block; margin-top: 2px; }

  details.finished summary { cursor: pointer; font-weight: 700; color: var(--accent); font-size: 15px; padding: 4px 0; }
  details.finished ul { list-style: none; margin: 10px 0 0; padding: 0; display: flex; flex-direction: column; gap: 6px; }
  details.finished li { font-size: 14.5px; color: var(--muted); border-bottom: 1px solid var(--border); padding-bottom: 6px; }
  details.finished li .fn { font-weight: 700; color: var(--text); }

  ul.risks { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 10px; }
  ul.risks li { background: var(--panel); border: 1px solid var(--border); border-left: 5px solid var(--amber); border-radius: 10px; padding: 12px 16px; font-size: 16px; box-shadow: var(--shadow); }
  ul.risks li .owner { display: block; color: var(--muted); font-size: 14px; margin-top: 4px; }

  details.ref-block { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 14px 18px; box-shadow: var(--shadow); margin-bottom: 12px; }
  details.ref-block summary { cursor: pointer; font-weight: 800; font-size: 16.5px; }
  details.ref-block .ref-body { margin-top: 14px; }
  .diagram-panel { overflow-x: auto; }
  .diagram-caption { color: var(--muted); font-size: 14px; margin-top: 8px; }
  .diagram-lead { margin: 6px 0 10px; font-size: 16px; }
  .lanes { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 10px; margin-bottom: 14px; }
  .lane { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 10px; }
  .lane-head { font-weight: 800; font-size: 16px; margin-bottom: 8px; }
  .lane-count { color: var(--muted); font-weight: 400; font-size: 14px; }
  .lane ul { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 8px; }
  .lane-card { border: 1px solid var(--border); border-left: 4px solid var(--grey); border-radius: 8px; padding: 8px 10px; font-size: 15px; }
  .lane-card.running { border-left-color: var(--amber); }
  .lane-card.blocked { border-left-color: var(--red); }
  .lane-card.finished { border-left-color: var(--green); }
  .lane-st { font-weight: 800; display: block; }
  .lane-when { color: var(--muted); font-size: 13px; display: block; margin-top: 2px; }
  .lane-idle { color: var(--muted); font-size: 15px; }
  @media (max-width: 800px) { .lanes { grid-template-columns: 1fr; } }
  svg text { font-family: -apple-system, "Segoe UI", Inter, Roboto, Arial, sans-serif; }
  pre.codeblock { background: var(--grey-soft); border: 1px solid var(--border); border-radius: 10px; padding: 12px 14px; overflow-x: auto; font-size: 13px; line-height: 1.5; }
  pre.codeblock code { font-family: "Cascadia Code", Consolas, "SFMono-Regular", Menlo, monospace; color: var(--text); }
  .ta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 10px; }
  .ta-item { background: var(--grey-soft); border: 1px solid var(--border); border-radius: 10px; padding: 10px 14px; font-size: 14.5px; }
  .ta-item .ta-id { color: var(--accent); font-weight: 800; margin-right: 6px; }
  table.decisions { width: 100%; border-collapse: collapse; font-size: 14.5px; }
  table.decisions th, table.decisions td { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
  table.decisions th { color: var(--muted); font-size: 12.5px; text-transform: uppercase; }
  code { font-family: "Cascadia Code", Consolas, "SFMono-Regular", Menlo, monospace; background: var(--grey-soft); padding: 1px 5px; border-radius: 4px; font-size: 0.9em; }

  footer { text-align: center; color: var(--muted); font-size: 13px; margin-top: 36px; }
`;

function esc(s) {
  return String(s === undefined || s === null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function renderNowBox(now, meta) {
  return `
  <header class="now-box">
    <h1>rEtcd — where things stand</h1>
    <div class="now-line"><span class="now-label">Where we are:</span><span class="now-value">${esc(now.where)}</span></div>
    <div class="now-line"><span class="now-label">Blocked on:</span><span class="now-value">${esc(now.blocked)}</span></div>
    <div class="now-line"><span class="now-label">Next:</span><span class="now-value">${esc(now.next)}</span></div>
    <div class="now-line"><span class="now-label">Updated:</span><span class="now-value" id="updated-line">—</span></div>
  </header>`;
}

function renderStrip(milestones) {
  const total = milestones.length;
  const passed = milestones.filter((m) => m.chip === 'Done' || m.chip === 'Gate passed').length;
  const pct = total ? Math.round((passed / total) * 100) : 0;
  const segs = milestones.map((m) => {
    let cls = '';
    let glyph = '';
    if (m.chip === 'Done' || m.chip === 'Gate passed') { cls = ' pass'; glyph = ' ✓'; }
    else if (m.chip === 'In progress') { cls = ' active'; glyph = ' ▶'; }
    return `      <div class="strip-seg${cls}">${esc(m.id)}${glyph}</div>`;
  }).join('\n');
  return `
  <div class="strip-wrap">
    <div class="strip" aria-label="Milestone strip ${esc(milestones.map((m) => m.id).join(' through '))}">
${segs}
    </div>
    <div class="strip-caption">${passed} of ${total} milestones passed their gate. ${pct}%.</div>
  </div>`;
}

function renderSystemPicture(diagrams, partsById, oldPartStates, activeMilestoneId) {
  const legend = `
    <div class="mm-legend">
      <span>Part states:</span>
      <span class="mm-lg-item"><span class="mm-sw built"></span>✓ built — evidence: gate commit or named green tests</span>
      <span class="mm-lg-item"><span class="mm-sw building"></span>▶ building — dispatched, tests landing, no gate yet</span>
      <span class="mm-lg-item"><span class="mm-sw planned"></span>○ planned — designed, no code yet</span>
      <span class="mm-lg-item"><span class="mm-sw gap"></span>⚠ gap — built, with a logged known gap</span>
    </div>`;

  const blocks = diagrams.map((d, i) => {
    const t = transformMermaid(d.src, partsById, oldPartStates);
    const id = `diagram-${i}`;
    const isAlways = t.open === 'always';
    // Anything not fully built stays open: a collapsed "all good" view must not hide a gap.
    const isActive = !t.allBuilt || (t.open === 'active' && t.milestones.includes(activeMilestoneId));
    const cap = t.caption ? `
        <p class="diagram-lead">${esc(t.caption)}</p>` : '';
    const panel = `
        <div class="diagram-panel">
          <pre class="mermaid" id="${id}">${esc(t.src)}</pre>
        </div>`;
    const body = cap + panel;
    if (isAlways) {
      return `
    <div class="sys-block sys-fixed">
      <div class="sys-fixed-title">${esc(t.title)}<span class="sys-asof"> — as of <span class="as-of">—</span></span></div>${body}
    </div>`;
    }
    return `
    <details class="sys-block"${isActive ? ' open' : ''}>
      <summary>${esc(t.title)} <span class="sys-asof">as of <span class="as-of">—</span></span> — <span data-role="rollup">${esc(t.rollupText)}</span></summary>${body}
    </details>`;
  }).join('\n');

  return `
  <section id="section-system-picture">
    <h2 class="section-title">System picture</h2>${legend}${blocks}
  </section>`;
}

function markSymbol(mark) {
  if (mark === 'proven') return { cls: 'yes', glyph: '☑' };
  if (mark === 'risk') return { cls: 'warn', glyph: '⚠' };
  if (mark === 'failed') return { cls: 'warn', glyph: '✕' };
  return { cls: 'no', glyph: '☐' };
}

function renderMilestoneCard(m) {
  const acc = (m.acceptance || []).map((a) => {
    const sym = markSymbol(a.mark);
    const evi = a.evidence ? `<span class="evi">Evidence: ${esc(a.evidence)}</span>` : '';
    return `          <li><span class="mark ${sym.cls}">${sym.glyph}</span>${esc(a.text)}${evi}</li>`;
  }).join('\n');
  const status = (m.status || []).map((s) => {
    return `          <li><strong>${esc(s.lead)}</strong> ${esc(s.text)}${s.evidence ? ` <span class="evi">Evidence: ${esc(s.evidence)}</span>` : ''}</li>`;
  }).join('\n');
  const history = (m.history || []).map((h) => `            <li>${esc(h)}</li>`).join('\n');
  const chipCls = CHIP_SLUG[m.chip] || 'not-started';
  return `
      <div class="card">
        <div class="card-head"><span class="card-title">${esc(m.id)} — ${esc(m.title)}</span><span class="chip st-${chipCls}">${esc(m.chip)}</span></div>
        <ul class="acc">
${acc}
        </ul>
        <ul class="status">
${status}
        </ul>
        <div class="tests">Tests: ${esc(m.tests)}</div>
        <details class="history"><summary>History (${(m.history || []).length})</summary>
          <ul>
${history}
          </ul>
        </details>
      </div>`;
}

function renderMilestones(milestones, activeMilestone) {
  const legend = `
    <div class="legend-row">
      <span>Legend:</span>
      <span class="chip st-not-started">Not started</span>
      <span class="chip st-in-progress">In progress</span>
      <span class="chip st-blocked">Blocked</span>
      <span class="chip st-review">Review</span>
      <span class="chip st-gate-passed">Gate passed</span>
      <span class="chip st-done">Done</span>
    </div>`;

  let pipeline = '';
  if (activeMilestone) {
    const mmd = buildGatePipeline(activeMilestone);
    pipeline = `
    <div class="pipeline-wrap">
      <strong>${esc(activeMilestone.id)} gate pipeline</strong> <span style="color:var(--muted);font-size:14px;">— as of <span class="as-of">—</span></span>
      <div class="diagram-panel" style="margin-top:10px;">
        <pre class="mermaid" id="diagram-gate-pipeline">${esc(buildGatePipeline(activeMilestone))}</pre>
      </div>
    </div>`;
  }

  const cards = milestones.map(renderMilestoneCard).join('\n');
  return `
  <section id="section-milestones">
    <h2 class="section-title">Milestones</h2>${legend}${pipeline}
    <div class="board">
${cards}
    </div>
  </section>`;
}

function renderActiveWork(agents, activeMilestoneId) {
  const finished = agents.filter((a) => a.status === 'finished');

  const lanesBlock = buildSwimLanes(agents);

  const finishedItems = finished.map((a) => `        <li><span class="fn">${esc(a.id)}</span>: ${esc(a.task)}</li>`).join('\n');

  return `
  <section id="section-active">
    <h2 class="section-title">Active work</h2>${lanesBlock}
    <details class="finished"><summary>Finished (${finished.length})</summary>
      <ul>
${finishedItems}
      </ul>
    </details>
  </section>`;
}

function renderRisks(risks) {
  const items = risks.map((r) => {
    return `      <li><strong>${esc(r.text)}</strong><span class="owner">Owner: ${esc(r.owner)}.${r.closing ? ` ${esc(r.closing)}` : ''}</span></li>`;
  }).join('\n');
  return `
  <section id="section-risks">
    <h2 class="section-title">Risks and open decisions</h2>
    <ul class="risks">
${items}
    </ul>
  </section>`;
}

function renderReference(referenceHtml) {
  return `
  <section id="section-reference">
    <h2 class="section-title">Reference</h2>
${referenceHtml}
  </section>`;
}

function pickActiveMilestone(milestones) {
  // "active" = not yet committed/released; if several, the one closest to its gate wins.
  const candidates = milestones.filter((m) => m.stage !== 'committed' && m.stage !== 'release');
  if (candidates.length === 0) return null;
  candidates.sort((a, b) => STAGE_ORDER.indexOf(b.stage) - STAGE_ORDER.indexOf(a.stage));
  return candidates[0];
}

function renderPage(p) {
  const partsById = indexParts(p.parts);
  const oldPartStates = p.meta.part_states || {};
  const activeMilestone = pickActiveMilestone(p.milestones.milestones || []);
  const activeMilestoneId = activeMilestone ? activeMilestone.id : null;
  const mermaidJs = fs.readFileSync(VENDOR_MERMAID, 'utf8');

  const body = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>rEtcd — Build Progress</title>
<style>${PAGE_CSS}</style>
</head>
<body>
<div class="wrap">

  <div id="stale-banner" class="stale-banner" hidden></div>
${renderNowBox(p.now, p.meta)}
${renderStrip(p.milestones.milestones || [])}
${renderSystemPicture(p.diagrams, partsById, oldPartStates, activeMilestoneId)}
${renderMilestones(p.milestones.milestones || [], activeMilestone)}
${renderActiveWork(p.work.agents || [], activeMilestoneId)}
${renderRisks(p.risks.risks || [])}
${renderReference(p.reference)}

  <footer>rEtcd progress report. Source of truth: the M4-M6 ledger. Regenerated after each milestone gate or critic verdict.</footer>
</div>

<script>
(function () {
  var UPDATED_ISO = ${JSON.stringify(p.meta.updated)};

  function fmtLocal(iso) {
    try {
      var d = new Date(iso);
      return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
    } catch (e) { return iso; }
  }
  function ageMinutes(iso) {
    return Math.round((Date.now() - new Date(iso).getTime()) / 60000);
  }
  function relAge(mins) {
    if (mins < 1) return 'just now';
    if (mins < 60) return mins + ' min ago';
    var hrs = Math.round(mins / 60);
    return hrs + ' h ago';
  }

  function refresh() {
    var mins = ageMinutes(UPDATED_ISO);
    var abs = fmtLocal(UPDATED_ISO);
    var updatedLine = document.getElementById('updated-line');
    if (updatedLine) updatedLine.textContent = abs + ' (' + relAge(mins) + ')';

    var asOfEls = document.querySelectorAll('.as-of');
    for (var i = 0; i < asOfEls.length; i++) { asOfEls[i].textContent = relAge(mins); }

    var banner = document.getElementById('stale-banner');
    if (banner) {
      if (mins > 60) {
        var hrs = Math.round(mins / 60);
        banner.textContent = 'This report is ' + hrs + ' hours old. Numbers below may have moved.';
        banner.hidden = false;
      } else {
        banner.hidden = true;
      }
    }
  }

  refresh();
  setInterval(refresh, 60000);
})();
</script>

<script>
${mermaidJs}
</script>
<script>
  (function () {
    // Mermaid parses classDef and theme colours itself and rejects var(--x).
    // Swap each token for the page's current value (light or dark), then render.
    var css = getComputedStyle(document.documentElement);
    function tok(name) { return css.getPropertyValue(name).trim(); }
    document.querySelectorAll('pre.mermaid').forEach(function (pre) {
      pre.textContent = pre.textContent.replace(/var\\((--[\\w-]+)\\)/g, function (m, n) { return tok(n) || m; });
    });
    mermaid.initialize({
      startOnLoad: false,
      securityLevel: 'loose',
      theme: 'base',
      themeVariables: {
        background: tok('--panel'),
        primaryColor: tok('--panel'),
        primaryBorderColor: tok('--grey'),
        primaryTextColor: tok('--text'),
        lineColor: tok('--muted'),
        clusterBkg: tok('--grey-soft'),
        clusterBorder: tok('--border'),
        titleColor: tok('--text'),
        textColor: tok('--text'),
        fontFamily: '-apple-system, "Segoe UI", Inter, Roboto, Arial, sans-serif',
        fontSize: '16px'
      }
    });
    // Mermaid has no pattern fills. Add the diagonal hatch for "building" parts after
    // render, so that state keeps a fill cue on top of its glyph and word.
    mermaid.run().then(function () {
      var ns = 'http://www.w3.org/2000/svg';
      function el(name, attrs) {
        var e = document.createElementNS(ns, name);
        for (var k in attrs) e.setAttribute(k, attrs[k]);
        return e;
      }
      document.querySelectorAll('pre.mermaid svg').forEach(function (svg, i) {
        var nodes = svg.querySelectorAll('g.node.building rect.label-container');
        if (!nodes.length) return;
        var id = 'hatch-' + i;
        var pattern = el('pattern', { id: id, patternUnits: 'userSpaceOnUse', width: 10, height: 10, patternTransform: 'rotate(45)' });
        pattern.appendChild(el('rect', { width: 10, height: 10, fill: tok('--amber-soft') }));
        pattern.appendChild(el('line', { x1: 0, y1: 0, x2: 0, y2: 10, stroke: tok('--amber'), 'stroke-width': 3, 'stroke-opacity': 0.35 }));
        var defs = svg.querySelector('defs') || svg.insertBefore(el('defs', {}), svg.firstChild);
        defs.appendChild(pattern);
        nodes.forEach(function (r) { r.style.setProperty('fill', 'url(#' + id + ')', 'important'); });
      });
    });
  })();
</script>
</body>
</html>
`;
  return body;
}

// ---------------------------------------------------------------------------
// meta.json / changes.json advance + refresh log (full build mode only)
// ---------------------------------------------------------------------------

function currentGitHead() {
  try {
    return execSync('git rev-parse --short HEAD', { cwd: REPO_ROOT }).toString().trim();
  } catch (e) {
    return '';
  }
}

function currentLedgerLine(ledgerPathFromMeta) {
  const p = ledgerPathFromMeta ? path.join(REPO_ROOT, ledgerPathFromMeta) : null;
  if (!p || !fs.existsSync(p)) return 0;
  const text = fs.readFileSync(p, 'utf8');
  return text.split(/\r?\n/).length;
}

/// The stamp this build will carry, computed without writing anything.
function nextMeta(srcDir, p) {
  const now = new Date().toISOString();
  // A new work_dir means a new ledger nobody has read yet: line 0 makes scout read all of it.
  const ledgerPath = LEDGER_PATH;
  const ledgerLine = p.meta.ledger?.path === ledgerPath ? currentLedgerLine(ledgerPath) : 0;
  const newPartStates = {};
  for (const part of (p.parts.parts || [])) {
    if (part && part.id) newPartStates[part.id] = part.state;
  }
  const newMeta = {
    updated: now,
    ledger: { path: ledgerPath, line: ledgerLine },
    git_head: currentGitHead(),
    part_states: newPartStates,
  };
  return newMeta;
}

/// Record the refresh the page just published: stamp, consumed changes, log line.
function commitMeta(srcDir, newMeta) {
  fs.writeFileSync(path.join(srcDir, 'meta.json'), JSON.stringify(newMeta, null, 2) + '\n', 'utf8');

  const changesPath = path.join(srcDir, 'changes.json');
  if (fs.existsSync(changesPath)) {
    fs.renameSync(changesPath, path.join(srcDir, 'changes.last.json'));
  }

  fs.mkdirSync(path.dirname(REFRESH_LOG), { recursive: true });
  const logLine = `- ${newMeta.updated} refresh: ledger.line=${newMeta.ledger.line} git_head=${newMeta.git_head} (build.mjs)\n`;
  fs.appendFileSync(REFRESH_LOG, logLine, 'utf8');
}

main();

export { transformMermaid, buildGatePipeline, buildSwimLanes, checkProse, splitSentences, wordCount, hasBadDash };
