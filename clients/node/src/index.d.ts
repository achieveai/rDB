/// <reference types="node" />
import { EventEmitter } from 'node:events';

export interface RetcdRecord {
  /** The key as UTF-8 text. Lossy for keys that are not valid UTF-8: two such keys can render the same. */
  key: string;
  /** The exact key. Pass it back to get/put/delete to address this record. */
  keyBytes: Buffer;
  value: Buffer;
  createRevision: number;
  modRevision: number;
}

export interface WatchEvent {
  type: 'put' | 'delete' | 'progress';
  /** UTF-8 rendering of the key ('' for progress). */
  key: string;
  /** The exact key (empty for progress). */
  keyBytes: Buffer;
  /** null for delete and progress */
  value: Buffer | null;
  revision: number;
}

export interface ConnectOptions {
  /** Client addresses, e.g. ['127.0.0.1:17302', '127.0.0.1:17312']. */
  endpoints: string[];
  /** Deadline for one attempt. Default 10000. */
  timeoutMs?: number;
  /** How long one call may hunt for a leader or a live node. Default 20000. */
  failoverMs?: number;
  /** Path to proto/retcd/v1/config.proto. Default: env RETCD_PROTO, else the copy in the package, else the repo's proto/. */
  protoPath?: string;
  /** Health host:port per endpoint. Default: client port + 2. */
  healthEndpoints?: string[];
  /** false = do not touch the cluster until the first call. Default true. */
  probe?: boolean;
}

export interface NodeHealth {
  endpoint: string;
  ok: boolean;
  ready?: boolean;
  role?: string;
  leader?: number | null;
  term?: number;
  revision?: number;
  error?: string;
  raw?: Record<string, unknown>;
}

export type DirEntry = { dir: string; count: number };

export class RetcdClient {
  static connect(opts: ConnectOptions): Promise<RetcdClient>;
  constructor(opts: ConnectOptions);
  /** Address the next call goes to (the leader, once found). */
  readonly endpoint: string;
  revision(): Promise<number>;
  get(key: string | Buffer): Promise<RetcdRecord | null>;
  /** ifRevision: only if the key is still at that modRevision (0 = only if it does not exist). */
  put(key: string | Buffer, value: string | Uint8Array, opts?: { ifRevision?: number | string | bigint }): Promise<{ revision: number }>;
  /** true if it was deleted, false if it did not exist. CasConflictError if ifRevision is stale. */
  delete(key: string | Buffer, opts?: { ifRevision?: number | string | bigint }): Promise<boolean>;
  /** A string is a prefix or a glob. A Buffer is an exact byte prefix, never a glob. */
  list(pattern?: string | Buffer, opts?: { pageSize?: number }): AsyncGenerator<RetcdRecord, void, undefined> & { readonly readRevision: number | undefined };
  /** Held in memory: ResultTooLargeError past LIMITS.maxListDirsBytes of keys and values. Use list() to stream a big folder. */
  listDirs(pattern?: string, opts?: { pageSize?: number }): Promise<Array<RetcdRecord | DirEntry>>;
  watch(
    pattern?: string,
    opts?: { fromRevision?: number | string; signal?: AbortSignal; progress?: boolean; giveUpAfterMs?: number },
  ): AsyncGenerator<WatchEvent, void, undefined>;
  putFile(filePath: string, key?: string): Promise<{ key: string; size: number; sha256: string; revision: number; metaRevision: number }>;
  getFile(key: string, outPath: string, opts?: { overwrite?: boolean }): Promise<{ size: number; sha256: string; revision: number; verified: boolean }>;
  health(): Promise<NodeHealth[]>;
  close(): void;
}

export const LIMITS: Readonly<{ maxKeyBytes: number; maxValueBytes: number; maxListItems: number; maxListDirsBytes: number }>;

// ---- errors ----
export class RetcdError extends Error {
  code: string;
  grpcCode?: number;
  cause?: unknown;
}
export class NotFoundError extends RetcdError {}
export { NotFoundError as NotFound };
export class CasConflictError extends RetcdError {
  /** The key's current modRevision: retry with this. */
  currentRevision: number;
  /** false if the key does not exist right now. */
  exists: boolean;
}
/** Key over 1 KiB or value over 1 MiB: not sent, or the server refused it. */
export class TooLargeError extends RetcdError {}
/** A read result is bigger than the client holds in memory (listDirs). Nothing was written. code 'RESULT_TOO_LARGE'. */
export class ResultTooLargeError extends RetcdError {
  /** Bytes kept when the client stopped. */
  size: number;
  /** The cap, LIMITS.maxListDirsBytes. */
  limit: number;
}
/** A write timed out or died after sending: it may or may not have been applied. Never auto-retried. */
export class UnknownOutcomeError extends RetcdError {}
export class UnavailableError extends RetcdError {}
export class CompactedError extends RetcdError {
  minRevision?: number;
}
export class IntegrityError extends RetcdError {}

// ---- patterns ----
export function globToRegExp(pattern: string): RegExp;
export function hasGlob(text: string): boolean;
export function literalPrefix(text: string): string;

// ---- presence ----
export interface PresenceEvent {
  name: string;
  state: 'up' | 'late' | 'down' | 'back';
  lastSeen: Date;
}
export interface Heartbeat {
  readonly key: string;
  readonly seq: number;
  stop(opts?: { remove?: boolean }): Promise<void>;
}
export function startHeartbeat(
  client: Pick<RetcdClient, 'put' | 'delete'>,
  name: string,
  opts?: { intervalMs?: number; onError?: (err: Error) => void },
): Heartbeat;

export class PresenceMonitor extends EventEmitter implements AsyncIterable<PresenceEvent> {
  /** Resolves with the starting members. */
  readonly ready: Promise<Array<PresenceEvent>>;
  error?: Error;
  snapshot(): PresenceEvent[];
  stop(): void;
  on(event: 'change', listener: (ev: PresenceEvent) => void): this;
  on(event: 'ready', listener: (members: PresenceEvent[]) => void): this;
  on(event: 'error', listener: (err: Error) => void): this;
  on(event: 'end', listener: () => void): this;
  [Symbol.asyncIterator](): AsyncIterator<PresenceEvent>;
}
export function watchPresence(
  client: Pick<RetcdClient, 'list' | 'watch'>,
  opts?: { intervalMs?: number; missedBeats?: number; giveUpAfterMs?: number },
): PresenceMonitor;
