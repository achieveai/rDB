// npm pack hooks. `prepack` copies the repo's proto/retcd/v1/*.proto into ./proto so the tarball
// carries them; `postpack` (--clean) removes the copy so the working tree never holds a stale one.
// Inside the repo the client falls back to ../../proto by itself (src/index.mjs).
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const pkg = fileURLToPath(new URL('..', import.meta.url));
const dest = path.join(pkg, 'proto');

if (process.argv.includes('--clean')) {
  fs.rmSync(dest, { recursive: true, force: true });
  process.exit(0);
}

const from = path.resolve(pkg, '..', '..', 'proto', 'retcd', 'v1');
const protos = fs.existsSync(from) ? fs.readdirSync(from).filter((f) => f.endsWith('.proto')) : [];
if (!protos.includes('config.proto')) {
  console.error(`bundle-proto: ${path.join(from, 'config.proto')} not found. Pack from inside the rEtcd repo.`);
  process.exit(1);
}
const to = path.join(dest, 'retcd', 'v1');
fs.rmSync(dest, { recursive: true, force: true });
fs.mkdirSync(to, { recursive: true });
for (const f of protos) fs.copyFileSync(path.join(from, f), path.join(to, f));
console.error(`bundle-proto: copied ${protos.join(', ')} into ${to}`);
