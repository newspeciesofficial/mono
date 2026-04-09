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

export type AddQueryResult = {
  type: 'addQueryResult';
  requestId: number;
  clientGroupID: string;
  queryID: string;
  /**
   * The full hydration changes, collected on the pool thread before the
   * response is posted. Hydrations are short enough that streaming is not
   * required; collecting them into one message simplifies the proxy.
   * `'yield'` markers are NOT included — the pool thread drops them.
   */
  changes: RowChange[];
  hydrationTimeMs: number;
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
};

/** A batch of row changes from an in-progress advance stream. */
export type AdvanceChangeBatchResult = {
  type: 'advanceChangeBatch';
  requestId: number;
  clientGroupID: string;
  changes: RowChange[];
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
  | AddQueryResult
  | RemoveQueryResult
  | AdvanceBeginResult
  | AdvanceChangeBatchResult
  | AdvanceCompleteResult
  | GetRowResult
  | ResetResult
  | AdvanceWithoutDiffResult
  | DestroyClientGroupResult
  | ErrorResult;
