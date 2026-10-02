// put / get / compare-and-set / list / watch in 30 lines.
//   RETCD_ENDPOINTS=127.0.0.1:17302,127.0.0.1:17312,127.0.0.1:17322 node examples/quickstart.mjs
import { CasConflictError, RetcdClient } from '../src/index.mjs';

const endpoints = (process.env.RETCD_ENDPOINTS ?? '127.0.0.1:17302,127.0.0.1:17312,127.0.0.1:17322').split(',');
const client = await RetcdClient.connect({ endpoints });

// watch first, so we see our own writes (from "now")
const stop = new AbortController();
const seen = (async () => {
  for await (const ev of client.watch('demo/', { signal: stop.signal })) console.log('watch:', ev.type, ev.key, ev.value?.toString());
})();

const { revision } = await client.put('demo/counter', '1');
console.log('stored at revision', revision);
console.log('get:', (await client.get('demo/counter')).value.toString());

await client.put('demo/counter', '2', { ifRevision: revision }); // ok: still at that revision
try {
  await client.put('demo/counter', '3', { ifRevision: revision }); // stale
} catch (err) {
  if (!(err instanceof CasConflictError)) throw err;
  console.log(`refused: key is at revision ${err.currentRevision}`);
}

await client.put('demo/docs/a.md', 'hello');
await client.put('demo/docs/sub/b.md', 'world');
for await (const rec of client.list('demo/**/*.md')) console.log('list:', rec.key);
console.log('dirs:', await client.listDirs('demo/docs'));

await client.delete('demo/counter');
await client.delete('demo/docs/a.md');
await client.delete('demo/docs/sub/b.md');
await new Promise((r) => setTimeout(r, 300)); // let the watch print the last events
stop.abort();
await seen;
client.close();
