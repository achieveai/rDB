#!/usr/bin/env node
// docs/progress/archive.mjs
//
// Files the current progress-report pieces and the live work notes into the archive, then
// regenerates <archive>/INDEX.md. Plain Node ESM, zero dependencies, zero LLM tokens: any agent
// (Claude Code, Codex, Copilot, Hermes) or a human can run it. See AGENTS.md, "Archive".
//
// CLI:
//   node docs/progress/archive.mjs                   archive now; milestone read from changes
//   node docs/progress/archive.mjs --milestone M6    archive now under that milestone
//   node docs/progress/archive.mjs --dry-run         print what would be copied, write nothing
//   node docs/progress/archive.mjs --work <dir>      file only that older work folder's notes (no snapshot)
//
// Where: docs/progress/config.json "archive" (repo preference). Without one, the archive goes
// to local scratch .scratchpad/archive/, which is added to .git/info/exclude (never committed).

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '..', '..');
const SRC_DIR = path.join(__dirname, 'src');
const CONFIG_PATH = path.join(__dirname, 'config.json');
const FALLBACK_ARCHIVE = '.scratchpad/archive';

const TEXT_EXT = new Set(['.md', '.json', '.txt']);
const MAX_FILE_BYTES = 512 * 1024;
// Build caches and raw test logs are big and have no lasting value.
const SKIP_DIR = /target|logs|node_modules|^\./i;
const SECRET_PATTERNS = [
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/,
  /\bgh[pousr]_[A-Za-z0-9]{36,}\b/,
  /\bsk-[A-Za-z0-9_-]{20,}\b/,
  /\bAKIA[0-9A-Z]{16}\b/,
  /\bxox[abprs]-[A-Za-z0-9-]{10,}\b/,
];

function git(...args) {
  try { return execFileSync('git', args, { cwd: REPO_ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim(); }
  catch { return ''; }
}

function parseArgs(argv) {
  const args = { milestone: null, dryRun: false, work: null };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--milestone') args.milestone = argv[++i];
    else if (argv[i] === '--dry-run') args.dryRun = true;
    else if (argv[i] === '--work') args.work = argv[++i];
    else { console.error(`Unknown argument: ${argv[i]}`); process.exit(1); }
  }
  return args;
}

// The newest gate or commit item the scout recorded names the milestone being archived.
function milestoneFromChanges() {
  for (const name of ['changes.json', 'changes.last.json']) {
    const p = path.join(SRC_DIR, name);
    if (!fs.existsSync(p)) continue;
    const items = (JSON.parse(fs.readFileSync(p, 'utf8')).items || [])
      .filter((it) => (it.kind === 'gate' || it.kind === 'commit') && it.milestone);
    if (items.length) return items[items.length - 1].milestone;
  }
  return 'adhoc';
}

function walk(dir, filter, out = []) {
  if (!fs.existsSync(dir)) return out;
  for (const ent of fs.readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const full = path.join(dir, ent.name);
    if (ent.isDirectory()) { if (!SKIP_DIR.test(ent.name)) walk(full, filter, out); }
    else if (filter(full)) out.push(full);
  }
  return out;
}

function isNote(file) {
  return TEXT_EXT.has(path.extname(file).toLowerCase()) && fs.statSync(file).size <= MAX_FILE_BYTES;
}

function findSecrets(files) {
  const hits = [];
  for (const f of files) {
    fs.readFileSync(f, 'utf8').split(/\r?\n/).forEach((line, i) => {
      if (SECRET_PATTERNS.some((re) => re.test(line))) hits.push(`${path.relative(REPO_ROOT, f)}:${i + 1}`);
    });
  }
  return hits;
}

function ensureLocalExclude(rel) {
  const excl = path.join(REPO_ROOT, '.git', 'info', 'exclude');
  const line = `/${rel.split('/')[0]}/`;
  const text = fs.existsSync(excl) ? fs.readFileSync(excl, 'utf8') : '';
  if (!text.split(/\r?\n/).includes(line)) {
    fs.mkdirSync(path.dirname(excl), { recursive: true });
    fs.appendFileSync(excl, `${text.endsWith('\n') || !text ? '' : '\n'}${line}\n`);
    console.log(`archive: added ${line} to .git/info/exclude (local only)`);
  }
}

function firstHeading(file) {
  if (path.extname(file) === '.json') return 'JSON data';
  const lines = fs.readFileSync(file, 'utf8').split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const h = lines.find((l) => l.startsWith('#')) || lines.find((l) => !l.startsWith('---')) || '';
  return h.replace(/^#+\s*/, '').replace(/\|/g, '/').slice(0, 100);
}

function copyAll(files, fromDir, toDir, dryRun) {
  for (const f of files) {
    const dest = path.join(toDir, path.relative(fromDir, f));
    if (dryRun) { console.log(`  ${path.relative(REPO_ROOT, dest)}`); continue; }
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.copyFileSync(f, dest);
  }
}

function writeIndex(root) {
  const rel = (p) => path.relative(root, p).split(path.sep).join('/');
  const snaps = fs.existsSync(path.join(root, 'progress'))
    ? fs.readdirSync(path.join(root, 'progress')).sort() : [];
  const notes = walk(path.join(root, 'work'), (f) => TEXT_EXT.has(path.extname(f)));
  const oldPages = git('log', '--diff-filter=AM', '--date=short', '--format=%h|%ad|%s', '--', 'docs/progress/index.html')
    .split('\n').filter(Boolean).map((l) => l.split('|'));
  const out = [
    '# Archive index',
    '',
    'History, not current truth. Generated by `node docs/progress/archive.mjs`; do not edit.',
    'Search: `rg <term> docs/archive` or `git grep <term>`. Default `rg` skips this folder (`.ignore`).',
    '',
    '## Report snapshots',
    '',
    'Rebuild one: `node docs/progress/build.mjs --src <folder> --out old.html`.',
    '',
    '| Folder (date-milestone-commit) |',
    '|---|',
    ...snaps.map((s) => `| progress/${s} |`),
    '',
    '## Work notes',
    '',
    '| File | First heading |',
    '|---|---|',
    ...notes.map((f) => `| ${rel(f)} | ${firstHeading(f)} |`),
    '',
    '## Older report pages in git history',
    '',
    'View one: `git show <commit>:docs/progress/index.html > old.html`.',
    '',
    '| Commit | Date | Subject |',
    '|---|---|---|',
    ...oldPages.map(([h, d, s]) => `| ${h} | ${d} | ${(s || '').replace(/\|/g, '/')} |`),
    '',
  ];
  fs.writeFileSync(path.join(root, 'INDEX.md'), out.join('\n'), 'utf8');
  return { snaps: snaps.length, notes: notes.length, oldPages: oldPages.length };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = fs.existsSync(CONFIG_PATH) ? JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')) : {};
  const archiveRel = config.archive || FALLBACK_ARCHIVE;
  const root = path.join(REPO_ROOT, archiveRel);
  const workRel = args.work || config.work_dir;
  const workDir = workRel ? path.resolve(REPO_ROOT, workRel) : null;

  const milestone = args.milestone || milestoneFromChanges();
  const sha = git('rev-parse', '--short', 'HEAD') || 'nogit';
  const d = new Date();
  const date = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  const snapDir = path.join(root, 'progress', `${date}-${milestone}-${sha}`);

  const pieces = args.work ? [] : walk(SRC_DIR, () => true);
  const notes = workDir ? walk(workDir, isNote) : [];
  const secrets = findSecrets([...pieces, ...notes]);
  if (secrets.length) {
    console.error('archive: refused, secret-like text found. Remove it, then re-run:');
    for (const s of secrets) console.error(`  ${s}`);
    process.exit(1);
  }

  if (!config.archive && !args.dryRun) ensureLocalExclude(FALLBACK_ARCHIVE);
  if (args.dryRun) console.log(`archive (dry run) -> ${archiveRel}`);
  if (pieces.length) copyAll(pieces, SRC_DIR, snapDir, args.dryRun);
  if (workDir) copyAll(notes, workDir, path.join(root, 'work', path.basename(workDir)), args.dryRun);
  if (args.dryRun) return;

  const n = writeIndex(root);
  console.log(`archive: ${pieces.length} report pieces -> ${path.relative(REPO_ROOT, snapDir).split(path.sep).join('/')}`);
  console.log(`archive: ${notes.length} work notes; INDEX lists ${n.snaps} snapshots, ${n.notes} notes, ${n.oldPages} older pages`);
}

main();
