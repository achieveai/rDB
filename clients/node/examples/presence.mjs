// Heartbeat + monitor demo. Three "services" beat; one dies; the monitor notices; it comes back.
//   RETCD_ENDPOINTS=127.0.0.1:17302,... node examples/presence.mjs
// It uses a 500 ms interval so the whole story takes about 10 seconds.
import { RetcdClient, startHeartbeat, watchPresence } from '../src/index.mjs';

const endpoints = (process.env.RETCD_ENDPOINTS ?? '127.0.0.1:17302,127.0.0.1:17312,127.0.0.1:17322').split(',');
const intervalMs = 500;
const client = await RetcdClient.connect({ endpoints });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const monitor = watchPresence(client, { intervalMs, missedBeats: 3 });
monitor.on('change', (ev) => console.log(`${new Date().toISOString().slice(11, 23)}  ${ev.name.padEnd(8)} ${ev.state}`));
await monitor.ready;

const beats = {
  api: startHeartbeat(client, 'api', { intervalMs }),
  worker: startHeartbeat(client, 'worker', { intervalMs }),
  cron: startHeartbeat(client, 'cron', { intervalMs }),
};
await sleep(2000);

console.log('--- worker stops beating (no goodbye)');
await beats.worker.stop();
await sleep(3500);

console.log('--- cron says goodbye (key removed: instant down)');
await beats.cron.stop({ remove: true });
await sleep(1000);

console.log('--- worker restarts');
beats.worker = startHeartbeat(client, 'worker', { intervalMs });
await sleep(1500);

await beats.api.stop({ remove: true });
await beats.worker.stop({ remove: true });
monitor.stop();
client.close();
