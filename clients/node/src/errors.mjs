// Typed errors, and the one place a gRPC failure becomes one of them.
import grpc from '@grpc/grpc-js';

/** Base class. `code` is a short stable string; `grpcCode` is the gRPC status when there was one. */
export class RetcdError extends Error {
  constructor(message, { code = 'ERROR', grpcCode, cause } = {}) {
    super(message, cause ? { cause } : undefined);
    this.name = this.constructor.name;
    this.code = code;
    if (grpcCode !== undefined) this.grpcCode = grpcCode;
  }
}

/** The key does not exist (delete, getFile). `get` returns null instead of throwing. */
export class NotFoundError extends RetcdError {
  constructor(message, opts) {
    super(message, { code: 'NOT_FOUND', ...opts });
  }
}
export { NotFoundError as NotFound };

/** `ifRevision` did not match. `currentRevision` is what to retry with; `exists` is false if the key is gone. */
export class CasConflictError extends RetcdError {
  constructor(message, { currentRevision, exists, ...opts } = {}) {
    super(message, { code: 'CAS_CONFLICT', ...opts });
    this.currentRevision = currentRevision;
    this.exists = exists;
  }
}

/** Key over 1 KiB or value over 1 MiB (or the server refused a size cap). */
export class TooLargeError extends RetcdError {
  constructor(message, opts) {
    super(message, { code: 'TOO_LARGE', ...opts });
  }
}

/**
 * A read result is bigger than this client holds in memory (listDirs past LIMITS.maxListDirsBytes). Raised by the
 * client partway through the walk; nothing was written. `size` is the bytes kept when it stopped, `limit` the cap.
 */
export class ResultTooLargeError extends RetcdError {
  constructor(message, { size, limit, ...opts } = {}) {
    super(message, { code: 'RESULT_TOO_LARGE', ...opts });
    this.size = size;
    this.limit = limit;
  }
}

/** A write timed out or the connection died after it was sent. It may or may not have been applied. Never auto-retried. */
export class UnknownOutcomeError extends RetcdError {
  constructor(message, opts) {
    super(message, { code: 'UNKNOWN_OUTCOME', ...opts });
  }
}

/** No node could serve the call in time. For a write, nothing was applied. */
export class UnavailableError extends RetcdError {
  constructor(message, opts) {
    super(message, { code: 'UNAVAILABLE', ...opts });
  }
}

/** A watch asked for history the server already compacted. `minRevision` is the oldest it still has. */
export class CompactedError extends RetcdError {
  constructor(message, { minRevision, ...opts } = {}) {
    super(message, { code: 'COMPACTED', ...opts });
    this.minRevision = minRevision;
  }
}

/** getFile: the bytes do not match the sha256/size in the meta record. */
export class IntegrityError extends RetcdError {
  constructor(message, opts) {
    super(message, { code: 'INTEGRITY', ...opts });
  }
}

// ---------------------------------------------------------------------------------------
// gRPC -> typed. Rules come from crates/config-grpc/src/error.rs (ADR-0015):
//   - a status the server minted carries `retcd-outcome: rejected`: it never entered the log.
//   - a status without it came from the transport. For a write that means "unknown",
//     EXCEPT a failed connect step (`connectFailed`): nothing was ever sent.
//   - a write DEADLINE_EXCEEDED is unknown, stamped or not.
// ---------------------------------------------------------------------------------------
const STATUS_NAME = Object.fromEntries(Object.entries(grpc.status).map(([k, v]) => [v, k]));
const REFUSED = /ECONNREFUSED|No connection established|Connection refused|EHOSTUNREACH|ENETUNREACH|ENOTFOUND|EAI_AGAIN/i;

/** First value of a metadata key on a gRPC error, or undefined. */
export const metaOf = (err, key) => {
  const v = err?.metadata?.get?.(key);
  return v && v.length ? String(v[0]) : undefined;
};

/**
 * What a failed attempt means for the retry loop:
 * 'conflict' | 'not-leader' (server rejected it, safe to resend) | 'refused' (never connected) |
 * 'server' (typed refusal) | 'transport' (timeout, reset: outcome unknown for a write).
 */
export function classify(err) {
  if (metaOf(err, 'retcd-conflict-mod-revision') !== undefined) return 'conflict';
  const server = metaOf(err, 'retcd-outcome') === 'rejected';
  const typedReason = metaOf(err, 'retcd-reason') !== undefined;
  if (server && !typedReason && (err.code === grpc.status.FAILED_PRECONDITION || err.code === grpc.status.UNAVAILABLE)) {
    return 'not-leader'; // rejected before the log: always safe to resend
  }
  if (err.code === grpc.status.UNAVAILABLE && REFUSED.test(err.details ?? err.message ?? '')) return 'refused';
  return server ? 'server' : 'transport';
}

/** Leader endpoint a follower pointed us at, if any. */
export const leaderHint = (err) => metaOf(err, 'retcd-leader-endpoint');

/**
 * A List cursor minted by another node. The refusal names a better node; resend the same page
 * request there, with the same token. Any other cursor refusal (expired, evicted, ...) is final.
 */
export const isForeignPageToken = (err) =>
  err?.code === grpc.status.FAILED_PRECONDITION && metaOf(err, 'retcd-reason') === 'node' && leaderHint(err) !== undefined;

/**
 * Turn a gRPC error into a RetcdError.
 * @param {object} err   gRPC ServiceError ({code, details, metadata})
 * @param {{write?: boolean, endpoint?: string}} [ctx]
 */
export function mapError(err, { write = false, endpoint = '' } = {}) {
  if (err instanceof RetcdError) return err;
  const detail = err.details || err.message || String(err);
  const where = endpoint ? ` (${endpoint})` : '';
  const base = { grpcCode: err.code, cause: err };
  const kind = classify(err);
  if (kind === 'conflict') {
    const cur = Number(metaOf(err, 'retcd-conflict-mod-revision'));
    const exists = metaOf(err, 'retcd-conflict-exists') === 'true';
    return new CasConflictError(
      exists ? `key is at revision ${cur}, not the one you expected` : 'key does not exist right now',
      { currentRevision: cur, exists, ...base },
    );
  }
  switch (err.code) {
    case grpc.status.NOT_FOUND:
      return new NotFoundError(`not found${where}`, base);
    case grpc.status.RESOURCE_EXHAUSTED:
      return new TooLargeError(`server refused: over a size or capacity limit: ${detail}`, base);
    case grpc.status.OUT_OF_RANGE: {
      const min = metaOf(err, 'retcd-min-revision');
      return new CompactedError(`that revision was compacted away${min ? `; oldest available is ${min}` : ''}`, {
        minRevision: min === undefined ? undefined : Number(min),
        ...base,
      });
    }
    case grpc.status.INVALID_ARGUMENT:
      return new RetcdError(`bad request: ${detail}`, { code: 'INVALID_ARGUMENT', ...base });
    case grpc.status.PERMISSION_DENIED:
    case grpc.status.UNAUTHENTICATED:
      return new RetcdError(`${STATUS_NAME[err.code]}: ${detail}`, { code: STATUS_NAME[err.code], ...base });
    default:
  }
  if (err.code === grpc.status.FAILED_PRECONDITION && metaOf(err, 'retcd-reason') !== undefined) {
    const reason = metaOf(err, 'retcd-reason');
    const e = new RetcdError(`list cursor refused (${reason}): start the list again`, { code: 'PAGE_TOKEN', ...base });
    e.reason = reason;
    return e;
  }
  // Timeouts, dropped connections, "no leader": what it means depends on read or write.
  // For a write, only the connect step's own stamp (`connectFailed`, set before anything is sent) proves
  // nothing was sent; refusal text in an error from the send step does not. A write deadline is unknown
  // even when the server stamped it: the server can time out after handing the write to Raft.
  const server = metaOf(err, 'retcd-outcome') === 'rejected';
  const name = STATUS_NAME[err.code] ?? err.code;
  if (write && err.connectFailed !== true && (!server || err.code === grpc.status.DEADLINE_EXCEEDED)) {
    return new UnknownOutcomeError(
      `the write may or may not have been applied${where}: ${name}: ${detail}. Read the key to find out; do not blindly retry.`,
      base,
    );
  }
  const nothing = write ? ' Nothing was written.' : '';
  return new UnavailableError(`cannot reach a working node${where}: ${name}: ${detail}.${nothing}`, base);
}
