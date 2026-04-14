/**
 * Messages exchanged between the syncer (main thread) and a pool worker
 * thread that runs IVM for one or more client groups.
 *
 * Design rules (v3):
 * - Every message carries an explicit `requestId` field. There is NO
 *   module-level correlation state in the pool thread; handlers receive the
 *   id as part of the message and pass it through their closures.
 * - `advance` uses a streaming response: `advanceBegin` (exactly once), then
 *   zero or more `advanceChangeBatch`, then `advanceComplete` (exactly once).
 *   All three carry the same `requestId`.
 * - Fire-and-forget messages (`destroyClientGroup`, `removeQuery`, `shutdown`)
 *   still carry a `requestId` for log correlation even though the pool
 *   thread does not send a response.
 */

import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {LoadedPermissions} from '../auth/load-permissions.ts';
import type {RowChange} from '../services/view-syncer/pipeline-driver.ts';

// ---------------------------------------------------------------------------
// Syncer  →  pool thread  (requests)
// ---------------------------------------------------------------------------

export type InitMsg = {
  type: 'init';
  requestId: number;
  clientGroupID: string;
  clientSchema: ClientSchema;
};

export type AddQueryMsg = {
  type: 'addQuery';
  requestId: number;
  clientGroupID: string;
  queryID: string;
  transformationHash: string;
  ast: AST;
};

export type RemoveQueryMsg = {
  type: 'removeQuery';
  requestId: number;
  clientGroupID: string;
  queryID: string;
};

export type AdvanceMsg = {
  type: 'advance';
  requestId: number;
  clientGroupID: string;
};

export type GetRowMsg = {
  type: 'getRow';
  requestId: number;
  clientGroupID: string;
  table: string;
  rowKey: Record<string, unknown>;
};

export type ResetMsg = {
  type: 'reset';
  requestId: number;
  clientGroupID: string;
  clientSchema: ClientSchema;
};

export type AdvanceWithoutDiffMsg = {
  type: 'advanceWithoutDiff';
  requestId: number;
  clientGroupID: string;
};

export type DestroyClientGroupMsg = {
  type: 'destroyClientGroup';
  requestId: number;
  clientGroupID: string;
  generation: number;
};

export type ShutdownMsg = {
  type: 'shutdown';
  requestId: number;
};

export type PoolWorkerMsg =
  | InitMsg
  | AddQueryMsg
  | RemoveQueryMsg
  | AdvanceMsg
  | GetRowMsg
  | ResetMsg
  | AdvanceWithoutDiffMsg
  | DestroyClientGroupMsg
  | ShutdownMsg;

// ---------------------------------------------------------------------------
// Pool thread  →  syncer  (responses)
// ---------------------------------------------------------------------------

/**
 * Snapshot of the PipelineDriver's cacheable state. Sent in `initResult` and
 * `advanceComplete` so the syncer-side proxy can answer `initialized()`,
 * `currentVersion()`, `currentPermissions()`, `replicaVersion`,
 * `totalHydrationTimeMs()` and `queries()` without a round-trip.
 */
export type DriverState = {
  readonly version: string;
  readonly replicaVersion: string;
  readonly permissions: LoadedPermissions | null;
  readonly totalHydrationTimeMs: number;
  /** queryID → transformationHash — the minimum the syncer needs. */
  readonly queries: Record<string, string>;
};

export type InitResult = {
  type: 'initResult';
  requestId: number;
  clientGroupID: string;
  state: DriverState;
  generation: number;
};

/** Start of a streaming addQuery response. */
export type AddQueryBeginResult = {
  type: 'addQueryBegin';
  requestId: number;
  clientGroupID: string;
  queryID: string;
};

/** A batch of hydration row changes (shared memory). */
export type AddQueryBatchResult = {
  type: 'addQueryBatch';
  requestId: number;
  clientGroupID: string;
  /** SharedArrayBuffer containing JSON-encoded RowChange[]. */
  changesBuf: SharedArrayBuffer;
  /** Byte length of the encoded data within the buffer. */
  changesLen: number;
};

/** End of a streaming addQuery response. */
export type AddQueryCompleteResult = {
  type: 'addQueryComplete';
  requestId: number;
  clientGroupID: string;
  queryID: string;
  hydrationTimeMs: number;
  totalRows: number;
  batchCount: number;
  state: DriverState;
};

export type RemoveQueryResult = {
  type: 'removeQueryResult';
  requestId: number;
  clientGroupID: string;
  queryID: string;
  state: DriverState;
};

/** Start of a streaming advance response. */
export type AdvanceBeginResult = {
  type: 'advanceBegin';
  requestId: number;
  clientGroupID: string;
  version: string;
  numChanges: number;
  snapshotMs: number;
  /**
   * Diagnostics — pool thread's own clock, relative durations (ms).
   *
   * These let the syncer compute the queue-wait and IPC components of the
   * round-trip without having to reconcile the pool worker's
   * `performance.now()` origin (which is different from the syncer's).
   */
  /** Duration inside the pool thread from message receive to this post. */
  poolToBeginMs: number;
  /**
   * Pool-thread idle gap since the previous advance on the same thread
   * ended. A value close to 0 while this advance's {@link queueWaitMs} is
   * high directly implicates port-queue backlog (the previous advance
   * kept the thread busy while this one waited in the port queue).
   */
  gapSincePrevAdvanceMs: number;
  /** The `clientGroupID` of the previous advance on this pool thread. */
  prevAdvanceCgID: string | undefined;
  /** How long the previous advance on this pool thread took, ms. */
  prevAdvanceDurationMs: number | undefined;
  /** 0-indexed pool thread this message was handled on. */
  poolThreadIdx: number;
};

/** A batch of row changes from an in-progress advance stream (shared memory). */
export type AdvanceChangeBatchResult = {
  type: 'advanceChangeBatch';
  requestId: number;
  clientGroupID: string;
  /** SharedArrayBuffer containing JSON-encoded RowChange[]. */
  changesBuf: SharedArrayBuffer;
  /** Byte length of the encoded data within the buffer. */
  changesLen: number;
};

/**
 * Single-message advance for small advances (< BATCH_SIZE rows).
 * Combines begin + batch + complete into one postMessage call.
 */
export type AdvanceSingleResult = {
  type: 'advanceSingle';
  requestId: number;
  clientGroupID: string;
  // begin fields
  version: string;
  numChanges: number;
  snapshotMs: number;
  poolToBeginMs: number;
  gapSincePrevAdvanceMs: number;
  prevAdvanceCgID: string | undefined;
  prevAdvanceDurationMs: number | undefined;
  poolThreadIdx: number;
  // batch (shared memory, may be empty)
  changesBuf: SharedArrayBuffer | undefined;
  changesLen: number;
  // complete fields
  didReset: boolean;
  iterateMs: number;
  totalRows: number;
  state: DriverState;
  poolToCompleteMs: number;
  batchCount: number;
};

/** End of a streaming advance response. Carries updated driver state. */
export type AdvanceCompleteResult = {
  type: 'advanceComplete';
  requestId: number;
  clientGroupID: string;
  didReset: boolean;
  iterateMs: number;
  totalRows: number;
  state: DriverState;
  /**
   * Diagnostics — pool thread's own clock, relative durations (ms).
   */
  /** Duration inside the pool thread from message receive to this post. */
  poolToCompleteMs: number;
  /** Number of `advanceChangeBatch` messages sent during this advance. */
  batchCount: number;
};

export type GetRowResult = {
  type: 'getRowResult';
  requestId: number;
  clientGroupID: string;
  row: Row | undefined;
};

export type ResetResult = {
  type: 'resetResult';
  requestId: number;
  clientGroupID: string;
  state: DriverState;
};

export type AdvanceWithoutDiffResult = {
  type: 'advanceWithoutDiffResult';
  requestId: number;
  clientGroupID: string;
  version: string;
  state: DriverState;
};

/** Fire-and-forget destroy confirmation (not awaited by syncer, used for logs). */
export type DestroyClientGroupResult = {
  type: 'destroyClientGroupResult';
  requestId: number;
  clientGroupID: string;
};

export type ErrorResult = {
  type: 'error';
  requestId: number;
  clientGroupID: string | undefined;
  message: string;
  name: string;
  stack: string | undefined;
  /** Whether this error was a ResetPipelinesSignal from the stock driver. */
  isResetSignal: boolean;
};

export type PoolWorkerResult =
  | InitResult
  | AddQueryBeginResult
  | AddQueryBatchResult
  | AddQueryCompleteResult
  | RemoveQueryResult
  | AdvanceBeginResult
  | AdvanceChangeBatchResult
  | AdvanceCompleteResult
  | AdvanceSingleResult
  | GetRowResult
  | ResetResult
  | AdvanceWithoutDiffResult
  | DestroyClientGroupResult
  | ErrorResult;

// ---------------------------------------------------------------------------
// Zero-copy batch helpers
// ---------------------------------------------------------------------------

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Encode RowChange[] into a SharedArrayBuffer. Returns {buf, len}. */
export function encodeBatch(changes: RowChange[]): {
  buf: SharedArrayBuffer;
  len: number;
} {
  const encoded = encoder.encode(JSON.stringify(changes));
  const buf = new SharedArrayBuffer(encoded.byteLength);
  new Uint8Array(buf).set(encoded);
  return {buf, len: encoded.byteLength};
}

/** Decode a SharedArrayBuffer back to RowChange[]. */
export function decodeBatch(buf: SharedArrayBuffer, len: number): RowChange[] {
  return JSON.parse(decoder.decode(new Uint8Array(buf, 0, len))) as RowChange[];
}
