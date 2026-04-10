/**
 * Pool worker thread entry point.
 *
 * Runs stock `PipelineDriver` instances for one or more client groups on a
 * dedicated worker thread. The syncer main thread talks to this worker over a
 * `MessagePort` (provided via `workerData.port`, not the shared `parentPort`),
 * one channel per pool thread — see `PoolThreadManager`.
 *
 * Design rules (v3):
 * - **Stock `PipelineDriver` is imported and used as-is.** We do not subclass,
 *   adapter-wrap, or patch it. All IVM semantics live in the original class.
 * - **No module-level `currentRequestId`.** Each handler captures `requestId`
 *   from the incoming message and threads it through its `send()` calls.
 *   Adding `await` inside a handler later cannot introduce cross-message
 *   bleed-through because nothing is shared via module state.
 * - **`'yield'` markers are dropped.** The pool thread never calls
 *   `setImmediate` to give time to other CGs — SQLite work is strictly
 *   sequential on this thread. Target density is 1 CG per pool thread so
 *   there is no other CG to yield to.
 * - **Advance is streamed** as `advanceBegin` → `advanceChangeBatch`* →
 *   `advanceComplete` so the syncer can start processing batches while IVM
 *   keeps running here.
 * - **`destroyClientGroup` uses a generation counter** to tolerate stale
 *   messages that arrive after the client reconnected with a new
 *   ViewSyncer. A newer `init` bumps the generation; stale destroys with an
 *   older generation are ignored.
 */

import {randomUUID} from 'node:crypto';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {MessagePort, workerData} from 'node:worker_threads';
import {assert} from '../../../shared/src/asserts.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {DatabaseStorage} from '../../../zqlite/src/database-storage.ts';
import type {LogConfig} from '../config/zero-config.ts';
import {
  PipelineDriver,
  type RowChange,
  type Timer,
} from '../services/view-syncer/pipeline-driver.ts';
import {
  ResetPipelinesSignal,
  Snapshotter,
} from '../services/view-syncer/snapshotter.ts';
import {createLogContext} from './logging.ts';
import {InspectorDelegate} from './inspector-delegate.ts';
import type {RowKey} from '../types/row-key.ts';
import type {ShardID} from '../types/shards.ts';
import type {
  DriverState,
  PoolWorkerMsg,
  PoolWorkerResult,
} from '../workers/pool-protocol.ts';

export type PoolThreadWorkerData = {
  /** One syncer-created `MessagePort` per pool thread — transferred. */
  port: MessagePort;
  /** Index within the parent syncer's pool (for logging). */
  poolThreadIdx: number;
  replicaFile: string;
  shardID: ShardID;
  logConfig: LogConfig;
  yieldThresholdMs: number;
  enableQueryPlanner: boolean | undefined;
};

const BATCH_SIZE = 20;

const {
  port,
  poolThreadIdx,
  replicaFile,
  shardID,
  logConfig,
  yieldThresholdMs,
  enableQueryPlanner,
} = workerData as PoolThreadWorkerData;

assert(
  port instanceof MessagePort,
  'pool-thread worker must receive a MessagePort via workerData.port',
);

const lc = createLogContext(
  {log: logConfig},
  {worker: 'pool-thread'},
).withContext('poolThreadIdx', String(poolThreadIdx));

const operatorStorage = DatabaseStorage.create(
  lc,
  path.join(tmpdir(), `pool-thread-${poolThreadIdx}-${randomUUID()}`),
);
const inspectorDelegate = new InspectorDelegate(undefined);

type ClientGroupState = {
  readonly driver: PipelineDriver;
  clientSchema: ClientSchema;
  queryCount: number;
  generation: number;
};

const clientGroups = new Map<string, ClientGroupState>();

// Queueing diagnostics — one-shot telemetry to pin down the IPC gap.
//
// These track the previous advance that ran on this pool thread so the next
// advance can compute `gapSincePrevAdvanceMs`. If the gap is near zero and
// the syncer-observed `postToBeginMs` is high, the new advance was waiting
// in the MessageChannel port queue while the previous one ran.
let prevAdvanceCgID: string | undefined = undefined;
let prevAdvanceDurationMs: number | undefined = undefined;
let prevAdvanceEndTime = 0; // performance.now() when the previous advance finished

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function send(msg: PoolWorkerResult): void {
  port.postMessage(msg);
}

function createTimer(): Timer {
  const start = performance.now();
  return {
    // Pool thread never resets laps — `elapsedLap` and `totalElapsed` are
    // equivalent here. The stock driver's `#shouldYield` will return true
    // quickly as a result, but the pool-thread consumer skips `'yield'`
    // markers anyway.
    elapsedLap: () => performance.now() - start,
    totalElapsed: () => performance.now() - start,
  };
}

function snapshotState(state: ClientGroupState): DriverState {
  const {driver} = state;
  const queries: Record<string, string> = {};
  for (const [qid, info] of driver.queries().entries()) {
    queries[qid] = info.transformationHash;
  }
  return {
    version: driver.currentVersion(),
    replicaVersion: driver.replicaVersion,
    permissions: driver.currentPermissions(),
    totalHydrationTimeMs: driver.totalHydrationTimeMs(),
    queries,
  };
}

function requireState(clientGroupID: string): ClientGroupState {
  const state = clientGroups.get(clientGroupID);
  assert(
    state !== undefined,
    `pool-thread ${poolThreadIdx}: no PipelineDriver for clientGroup=${clientGroupID}`,
  );
  return state;
}

function getOrCreateClientGroup(
  clientGroupID: string,
  clientSchema: ClientSchema,
): ClientGroupState {
  const existing = clientGroups.get(clientGroupID);
  if (existing) {
    lc.info?.(
      `replacing PipelineDriver for clientGroup=${clientGroupID} ` +
        `(previous generation=${existing.generation})`,
    );
    existing.driver.destroy();
  }

  const snapshotter = new Snapshotter(lc, replicaFile, shardID);
  const driver = new PipelineDriver(
    lc.withContext('clientGroupID', clientGroupID),
    logConfig,
    snapshotter,
    shardID,
    operatorStorage.createClientGroupStorage(clientGroupID),
    clientGroupID,
    inspectorDelegate,
    () => yieldThresholdMs,
    enableQueryPlanner,
  );
  driver.init(clientSchema);

  const state: ClientGroupState = {
    driver,
    clientSchema,
    queryCount: 0,
    generation: (existing?.generation ?? 0) + 1,
  };
  clientGroups.set(clientGroupID, state);

  lc.info?.(
    `created PipelineDriver for clientGroup=${clientGroupID} ` +
      `generation=${state.generation} totalClientGroups=${clientGroups.size}`,
  );

  return state;
}

function sendError(
  requestId: number,
  clientGroupID: string | undefined,
  error: unknown,
): void {
  const err = error instanceof Error ? error : new Error(String(error));
  lc.error?.(
    `pool-thread ${poolThreadIdx}: error on cg=${clientGroupID ?? '?'}: ` +
      `${err.message}`,
  );
  send({
    type: 'error',
    requestId,
    clientGroupID,
    message: err.message,
    name: err.name,
    stack: err.stack,
    isResetSignal: err instanceof ResetPipelinesSignal,
  });
}

// ---------------------------------------------------------------------------
// Per-message handlers — each takes `requestId` explicitly
// ---------------------------------------------------------------------------

function handleInit(
  requestId: number,
  clientGroupID: string,
  clientSchema: ClientSchema,
): void {
  const state = getOrCreateClientGroup(clientGroupID, clientSchema);
  send({
    type: 'initResult',
    requestId,
    clientGroupID,
    state: snapshotState(state),
    generation: state.generation,
  });
}

function handleAddQuery(
  requestId: number,
  clientGroupID: string,
  queryID: string,
  transformationHash: string,
  ast: AST,
): void {
  const state = requireState(clientGroupID);
  const timer = createTimer();

  send({
    type: 'addQueryBegin',
    requestId,
    clientGroupID,
    queryID,
  });

  let batch: RowChange[] = [];
  let totalRows = 0;
  let batchCount = 0;
  for (const change of state.driver.addQuery(
    transformationHash,
    queryID,
    ast,
    timer,
  )) {
    if (change === 'yield') {
      continue;
    }
    batch.push(change);
    if (batch.length >= BATCH_SIZE) {
      totalRows += batch.length;
      batchCount++;
      send({
        type: 'addQueryBatch',
        requestId,
        clientGroupID,
        changes: batch,
      });
      batch = [];
    }
  }
  if (batch.length > 0) {
    totalRows += batch.length;
    batchCount++;
    send({
      type: 'addQueryBatch',
      requestId,
      clientGroupID,
      changes: batch,
    });
  }

  state.queryCount++;
  const hydrationTimeMs = timer.totalElapsed();
  lc.info?.(
    `hydrated query=${queryID} clientGroup=${clientGroupID} ` +
      `rows=${totalRows} batches=${batchCount} ` +
      `hydrationMs=${hydrationTimeMs.toFixed(1)} ` +
      `queriesInGroup=${state.queryCount}`,
  );
  send({
    type: 'addQueryComplete',
    requestId,
    clientGroupID,
    queryID,
    hydrationTimeMs,
    totalRows,
    batchCount,
    state: snapshotState(state),
  });
}

function handleRemoveQuery(
  requestId: number,
  clientGroupID: string,
  queryID: string,
): void {
  const state = clientGroups.get(clientGroupID);
  if (state) {
    state.driver.removeQuery(queryID);
    state.queryCount = Math.max(0, state.queryCount - 1);
    send({
      type: 'removeQueryResult',
      requestId,
      clientGroupID,
      queryID,
      state: snapshotState(state),
    });
  } else {
    // Stale removeQuery after the CG was destroyed — reply with a
    // degenerate state (all fields empty) rather than asserting. The
    // syncer-side proxy will ignore the response if it is no longer
    // awaiting it.
    send({
      type: 'removeQueryResult',
      requestId,
      clientGroupID,
      queryID,
      state: {
        version: '',
        replicaVersion: '',
        permissions: null,
        totalHydrationTimeMs: 0,
        queries: {},
      },
    });
  }
}

function handleAdvance(requestId: number, clientGroupID: string): void {
  // Measured first thing: how long was this thread idle since the previous
  // advance finished? A near-zero gap combined with a high syncer-observed
  // `postToBeginMs` means this advance was queued on the port while the
  // previous one ran.
  const tEntry = performance.now();
  const gapSincePrevAdvanceMs =
    prevAdvanceEndTime > 0 ? tEntry - prevAdvanceEndTime : 0;
  const capturedPrevCg = prevAdvanceCgID;
  const capturedPrevDur = prevAdvanceDurationMs;

  const state = requireState(clientGroupID);
  const timer = createTimer();
  const {version, numChanges, snapshotMs, changes} =
    state.driver.advance(timer);

  const tBeforeBegin = performance.now();
  const poolToBeginMs = tBeforeBegin - tEntry;

  send({
    type: 'advanceBegin',
    requestId,
    clientGroupID,
    version,
    numChanges,
    snapshotMs,
    poolToBeginMs,
    gapSincePrevAdvanceMs,
    prevAdvanceCgID: capturedPrevCg,
    prevAdvanceDurationMs: capturedPrevDur,
    poolThreadIdx,
  });

  let batch: RowChange[] = [];
  let totalRows = 0;
  let batchCount = 0;
  let didReset = false;

  const flush = () => {
    if (batch.length > 0) {
      totalRows += batch.length;
      batchCount++;
      send({
        type: 'advanceChangeBatch',
        requestId,
        clientGroupID,
        changes: batch,
      });
      batch = [];
    }
  };

  try {
    for (const change of changes) {
      if (change === 'yield') {
        continue;
      }
      batch.push(change);
      if (batch.length >= BATCH_SIZE) {
        totalRows += batch.length;
        batchCount++;
        send({
          type: 'advanceChangeBatch',
          requestId,
          clientGroupID,
          changes: batch,
        });
        batch = [];
      }
    }
    flush();
  } catch (e) {
    if (!(e instanceof ResetPipelinesSignal)) {
      throw e;
    }
    didReset = true;
    lc.info?.(`advance reset for clientGroup=${clientGroupID}: ${e.message}`);
    // Snapshot the live query set before reset — `reset()` clears the map.
    const queryInfos: {
      queryID: string;
      transformationHash: string;
      ast: AST;
    }[] = [];
    for (const [qid, info] of state.driver.queries().entries()) {
      queryInfos.push({
        queryID: qid,
        transformationHash: info.transformationHash,
        ast: info.transformedAst,
      });
    }
    state.driver.reset(state.clientSchema);
    state.driver.advanceWithoutDiff();
    // Re-hydrate the same queries in the same order and stream their rows
    // as `advanceChangeBatch` messages — the syncer treats them as part of
    // the same advance from its perspective.
    for (const info of queryInfos) {
      for (const change of state.driver.addQuery(
        info.transformationHash,
        info.queryID,
        info.ast,
        timer,
      )) {
        if (change === 'yield') {
          continue;
        }
        batch.push(change);
        if (batch.length >= BATCH_SIZE) {
          totalRows += batch.length;
          batchCount++;
          send({
            type: 'advanceChangeBatch',
            requestId,
            clientGroupID,
            changes: batch,
          });
          batch = [];
        }
      }
      flush();
    }
  }

  const tEnd = performance.now();
  const iterateMs = tEnd - tEntry - snapshotMs;
  const poolToCompleteMs = tEnd - tEntry;
  // The pool-thread log line already fires once per advance — we just add
  // the new fields to it so there are no extra log calls in the hot path.
  lc.info?.(
    `advanced clientGroup=${clientGroupID} to=${version} ` +
      `changes=${numChanges} rows=${totalRows} didReset=${didReset} ` +
      `snapshotMs=${snapshotMs.toFixed(2)} iterateMs=${iterateMs.toFixed(1)} ` +
      `poolToBeginMs=${poolToBeginMs.toFixed(2)} ` +
      `poolToCompleteMs=${poolToCompleteMs.toFixed(1)} ` +
      `batchCount=${batchCount} ` +
      `gapSincePrevAdvanceMs=${gapSincePrevAdvanceMs.toFixed(1)} ` +
      `prevCg=${capturedPrevCg ?? '-'} ` +
      `prevDurMs=${capturedPrevDur !== undefined ? capturedPrevDur.toFixed(1) : '-'}`,
  );
  send({
    type: 'advanceComplete',
    requestId,
    clientGroupID,
    didReset,
    iterateMs,
    totalRows,
    state: snapshotState(state),
    poolToCompleteMs,
    batchCount,
  });

  // Update the rolling "prev advance" state for the next handleAdvance
  // invocation on this pool thread.
  prevAdvanceCgID = clientGroupID;
  prevAdvanceDurationMs = poolToCompleteMs;
  prevAdvanceEndTime = tEnd;
}

function handleGetRow(
  requestId: number,
  clientGroupID: string,
  table: string,
  rowKey: Record<string, unknown>,
): void {
  const state = clientGroups.get(clientGroupID);
  let row: Row | undefined;
  if (state) {
    row = state.driver.getRow(table, rowKey as RowKey);
  }
  send({type: 'getRowResult', requestId, clientGroupID, row});
}

function handleReset(
  requestId: number,
  clientGroupID: string,
  clientSchema: ClientSchema,
): void {
  const state = requireState(clientGroupID);
  state.driver.reset(clientSchema);
  state.clientSchema = clientSchema;
  state.queryCount = 0;
  send({
    type: 'resetResult',
    requestId,
    clientGroupID,
    state: snapshotState(state),
  });
}

function handleAdvanceWithoutDiff(
  requestId: number,
  clientGroupID: string,
): void {
  const state = requireState(clientGroupID);
  const version = state.driver.advanceWithoutDiff();
  send({
    type: 'advanceWithoutDiffResult',
    requestId,
    clientGroupID,
    version,
    state: snapshotState(state),
  });
}

function handleDestroyClientGroup(
  requestId: number,
  clientGroupID: string,
  generation: number,
): void {
  const state = clientGroups.get(clientGroupID);
  if (state && state.generation === generation) {
    lc.info?.(
      `destroying clientGroup=${clientGroupID} generation=${generation} ` +
        `queries=${state.queryCount}`,
    );
    state.driver.destroy();
    clientGroups.delete(clientGroupID);
  } else if (state) {
    lc.info?.(
      `ignoring stale destroyClientGroup=${clientGroupID} ` +
        `msgGen=${generation} currentGen=${state.generation}`,
    );
  }
  send({type: 'destroyClientGroupResult', requestId, clientGroupID});
}

function handleShutdown(): void {
  lc.info?.(
    `shutting down pool thread ${poolThreadIdx} with ` +
      `${clientGroups.size} client groups`,
  );
  for (const state of clientGroups.values()) {
    try {
      state.driver.destroy();
    } catch (e) {
      lc.error?.(`error destroying driver on shutdown: ${String(e)}`);
    }
  }
  clientGroups.clear();
  try {
    operatorStorage.close();
  } catch (e) {
    lc.error?.(`error closing operator storage: ${String(e)}`);
  }
  port.close();
  // Cleanly exit the worker thread. `process.exit(0)` is fine here — the
  // syncer's PoolThreadManager is expecting the exit and does not restart.
  process.exit(0);
}

// ---------------------------------------------------------------------------
// Message loop
// ---------------------------------------------------------------------------

port.on('message', (msg: PoolWorkerMsg) => {
  const {requestId} = msg;
  const cgID = 'clientGroupID' in msg ? msg.clientGroupID : undefined;
  try {
    switch (msg.type) {
      case 'init':
        handleInit(requestId, msg.clientGroupID, msg.clientSchema);
        return;
      case 'addQuery':
        handleAddQuery(
          requestId,
          msg.clientGroupID,
          msg.queryID,
          msg.transformationHash,
          msg.ast,
        );
        return;
      case 'removeQuery':
        handleRemoveQuery(requestId, msg.clientGroupID, msg.queryID);
        return;
      case 'advance':
        handleAdvance(requestId, msg.clientGroupID);
        return;
      case 'getRow':
        handleGetRow(requestId, msg.clientGroupID, msg.table, msg.rowKey);
        return;
      case 'reset':
        handleReset(requestId, msg.clientGroupID, msg.clientSchema);
        return;
      case 'advanceWithoutDiff':
        handleAdvanceWithoutDiff(requestId, msg.clientGroupID);
        return;
      case 'destroyClientGroup':
        handleDestroyClientGroup(requestId, msg.clientGroupID, msg.generation);
        return;
      case 'shutdown':
        handleShutdown();
        return;
      default: {
        const exhaustive: never = msg;
        throw new Error(
          `pool-thread ${poolThreadIdx}: unknown message type ` +
            `${JSON.stringify(exhaustive)}`,
        );
      }
    }
  } catch (e) {
    sendError(requestId, cgID, e);
  }
});

lc.info?.(
  `pool-thread ${poolThreadIdx} started. replica=${replicaFile} ` +
    `yieldThresholdMs=${yieldThresholdMs}`,
);
