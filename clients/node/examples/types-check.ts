// Compile-only check of src/index.d.ts: `npm run typecheck` (tsc --noEmit). Never run.
// It imports the package by name, so the "exports" map and its "types" condition are checked too.
// Each `@ts-expect-error` line must fail to compile; if the types loosen, tsc reports it as unused.
import {
  CasConflictError,
  CompactedError,
  LIMITS,
  NotFound,
  NotFoundError,
  RetcdClient,
  RetcdError,
  ResultTooLargeError,
  UnknownOutcomeError,
  globToRegExp,
  hasGlob,
  literalPrefix,
  startHeartbeat,
  watchPresence,
  type DirEntry,
  type NodeHealth,
  type PresenceEvent,
  type RetcdRecord,
  type WatchEvent,
} from '@retcd/client';

export async function typesCheck(): Promise<void> {
  const c: RetcdClient = await RetcdClient.connect({ endpoints: ['127.0.0.1:17302'], timeoutMs: 500, protoPath: 'x.proto', probe: false });
  const endpoint: string = c.endpoint;
  const rev: number = await c.revision();

  const put: { revision: number } = await c.put('k', 'v', { ifRevision: 0 });
  await c.put(Buffer.from('k'), new Uint8Array([1, 2]), { ifRevision: '3' });
  const rec: RetcdRecord | null = await c.get('k');
  const value: Buffer | undefined = rec?.value;
  // keyBytes is the exact key; it round-trips into get/put/delete, and list takes a byte prefix.
  const keyBytes: Buffer | undefined = rec?.keyBytes;
  if (keyBytes) await c.get(keyBytes);
  for await (const r of c.list(Buffer.from([0x80]))) void r.keyBytes;
  const dirsLimit: number = LIMITS.maxListDirsBytes;
  void dirsLimit;
  try {
    await c.listDirs('big');
  } catch (err) {
    if (err instanceof ResultTooLargeError) {
      const kept: number = err.size;
      const cap: number = err.limit;
      void kept;
      void cap;
    }
  }

  // delete resolves a boolean: true deleted, false missing.
  const deleted: boolean = await c.delete('k', { ifRevision: put.revision });
  // @ts-expect-error delete no longer resolves {revision}
  const old: { revision: number } = await c.delete('k');

  const it = c.list('docs/**/*.md', { pageSize: 10 });
  for await (const r of it) {
    const key: string = r.key;
    void key;
  }
  const readRevision: number | undefined = it.readRevision;
  const dirs: Array<RetcdRecord | DirEntry> = await c.listDirs('docs');

  const ac = new AbortController();
  for await (const e of c.watch('app/', { fromRevision: readRevision ?? 0, signal: ac.signal, progress: true, giveUpAfterMs: 0 })) {
    const ev: WatchEvent = e;
    const kind: 'put' | 'delete' | 'progress' = ev.type;
    const evKey: Buffer = ev.keyBytes;
    void kind;
    void evKey;
  }

  const file = await c.putFile('./a.bin', 'files/a.bin');
  const sha: string = file.sha256;
  const got = await c.getFile('files/a.bin', './b.bin', { overwrite: true });
  const verified: boolean = got.verified;
  const health: NodeHealth[] = await c.health();
  c.close();

  const maxValue: number = LIMITS.maxValueBytes;
  const re: RegExp = globToRegExp('a/*');
  const globby: boolean = hasGlob('a/*');
  const prefix: string = literalPrefix('a/*');

  try {
    await c.put('k', 'v');
  } catch (err) {
    if (err instanceof CasConflictError) {
      const cur: number = err.currentRevision;
      const exists: boolean = err.exists;
      void cur;
      void exists;
    } else if (err instanceof CompactedError) {
      const min: number | undefined = err.minRevision;
      void min;
    } else if (err instanceof UnknownOutcomeError || err instanceof NotFoundError || err instanceof NotFound) {
      const code: string = err.code;
      void code;
    } else if (err instanceof RetcdError) {
      const grpcCode: number | undefined = err.grpcCode;
      void grpcCode;
    }
  }

  const hb = startHeartbeat(c, 'api', { intervalMs: 2000, onError: (e: Error) => void e });
  const seq: number = hb.seq;
  await hb.stop({ remove: true });
  const mon = watchPresence(c, { intervalMs: 2000, missedBeats: 3 });
  mon.on('change', (ev: PresenceEvent) => {
    const state: 'up' | 'late' | 'down' | 'back' = ev.state;
    void state;
  });
  const members: PresenceEvent[] = await mon.ready;
  for await (const ev of mon) void ev.lastSeen.toISOString();
  mon.stop();

  // @ts-expect-error endpoints is required
  await RetcdClient.connect({});
  // @ts-expect-error values are strings or bytes, not numbers
  await c.put('k', 42);

  void [endpoint, rev, value, deleted, old, dirs, sha, verified, health, maxValue, re, globby, prefix, seq, members];
}
