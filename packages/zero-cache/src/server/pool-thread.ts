import {randomUUID} from 'crypto';
import {tmpdir} from 'os';
import path from 'path';
import {parentPort, workerData} from 'node:worker_threads';
import {assert} from '../../../shared/src/asserts.ts';
import {createLogContext} from '../../../shared/src/logging.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {InspectorDelegate} from './inspector-delegate.ts';
import {
  PipelineDriver,
  type RowChange,
  type Timer,
} from '../services/view-syncer/pipeline-driver.ts';
import {Snapshotter} from '../services/view-syncer/snapshotter.ts';
import {DatabaseStorage} from '../../../zqlite/src/database-storage.ts';
import type {LogConfig} from '../config/zero-config.ts';
import type {RowKey} from '../types/row-key.ts';
import type {ShardID} from '../types/shards.ts';
import type {PoolWorkerMsg, PoolWorkerResult} from '../workers/pool-protocol.ts';

export type PoolThreadWorkerData = {
  replicaFile: string;
  shardID: ShardID;
  logConfig: LogConfig;
  yieldThresholdMs: number;
  enableQueryPlanner: boolean | undefined;
};

const port = parentPort;
assert(port !== null, 'pool-thread must be run as a worker thread');

const {replicaFile, shardID, logConfig, yieldThresholdMs, enableQueryPlanner} =
  workerData as PoolThreadWorkerData;

const lc = createLogContext({log: logConfig}, {worker: 'pool-thread'});
const tmpDir = tmpdir();
const operatorStorage = DatabaseStorage.create(
  lc,
  path.join(tmpDir, `pool-thread-${randomUUID()}`),
);
const inspectorDelegate = new InspectorDelegate(undefined);

// One PipelineDriver per client group — SAME as the old code.
// Multiple queries on the same client group share one Snapshotter + TableSources.
type ClientGroupState = {
  driver: PipelineDriver;
  clientSchema: ClientSchema;
  queryCount: number;
  generation: number;
};
const clientGroups = new Map<string, ClientGroupState>();

// Current request ID — echoed back in responses for correlation.
let currentRequestId: string | undefined;

function send(msg: PoolWorkerResult) {
  port!.postMessage({...msg, requestId: currentRequestId});
}

function createTimer(): Timer {
  const start = performance.now();
  let lapStart = start;
  return {
    elapsedLap: () => performance.now() - lapStart,
    totalElapsed: () => performance.now() - start,
  };
}

function collectChanges(changes: Iterable<RowChange | 'yield'>): RowChange[] {
  const result: RowChange[] = [];
  for (const change of changes) {
    if (change === 'yield') {
      continue;
    }
    result.push(change);
  }
  return result;
}

function getOrCreateClientGroup(
  clientGroupID: string,
  clientSchema: ClientSchema,
): ClientGroupState {
  const existing = clientGroups.get(clientGroupID);
  if (existing) {
    // Destroy old PipelineDriver and create fresh.
    // This handles the case where old destroyQuery messages are still
    // in the queue — the old PipelineDriver is gone, those messages
    // will be no-ops because queryCount on the new state starts at 0.
    lc.info?.(
      `pool-thread replacing PipelineDriver for clientGroup=${clientGroupID}`,
    );
    existing.driver.destroy();
  }

  // SAME constructor as syncer.ts:174-188
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

  const generation = (existing?.generation ?? 0) + 1;
  const state: ClientGroupState = {driver, clientSchema, queryCount: 0, generation};
  clientGroups.set(clientGroupID, state);

  lc.info?.(
    `pool-thread created PipelineDriver for clientGroup=${clientGroupID} ` +
      `totalClientGroups=${clientGroups.size}`,
  );

  return state;
}

lc.info?.(`pool-thread started. replica=${replicaFile}`);

port.on('message', (msg: PoolWorkerMsg & {requestId?: string}) => {
  currentRequestId = msg.requestId;
  try {
    switch (msg.type) {
      case 'init': {
        const {clientGroupID, clientSchema} = msg;
        const state = getOrCreateClientGroup(clientGroupID, clientSchema);
        send({
          type: 'initResult',
          version: state.driver.currentVersion(),
          replicaVersion: state.driver.replicaVersion,
          permissions: state.driver.currentPermissions(),
          generation: state.generation,
        });
        break;
      }

      case 'hydrate': {
        const {clientGroupID, queryID, transformationHash, ast} = msg;
        const state = clientGroups.get(clientGroupID);
        assert(
          state !== undefined,
          `Must send init for clientGroup=${clientGroupID} before hydrate`,
        );

        const tH0 = performance.now();
        const timer = createTimer();
        const changes = collectChanges(
          state.driver.addQuery(transformationHash, queryID, ast, timer),
        );
        const tH1 = performance.now();

        state.queryCount++;

        lc.info?.(
          `pool-thread hydrated query=${queryID} ` +
            `addQueryMs=${(tH1 - tH0).toFixed(2)} ` +
            `rows=${changes.length} ` +
            `queriesInGroup=${state.queryCount} ` +
            `clientGroup=${clientGroupID}`,
        );

        send({
          type: 'hydrationResult',
          queryID,
          changes,
          hydrationTimeMs: timer.totalElapsed(),
        });
        break;
      }

      case 'advance': {
        const {clientGroupID} = msg;
        const state = clientGroups.get(clientGroupID);
        assert(
          state !== undefined,
          `No PipelineDriver for clientGroup=${clientGroupID}`,
        );

        // SAME advance + error recovery as ViewSyncer's run loop.
        // advanceWithRecovery handles ResetPipelinesSignal internally.
        const result = state.driver.advanceWithRecovery(
          state.clientSchema,
          createTimer(),
        );

        lc.info?.(
          `pool-thread advanced clientGroup=${clientGroupID} ` +
            `queries=${state.queryCount} to=${result.version} ` +
            `changes=${result.numChanges} rows=${result.changes.length} ` +
            `didReset=${result.didReset} ` +
            `snapshotMs=${result.metrics.snapshotMs.toFixed(2)} ` +
            `diffReadMs=${result.metrics.diffReadMs.toFixed(2)} ` +
            `ivmPushMs=${result.metrics.ivmPushMs.toFixed(2)} ` +
            `collectMs=${result.metrics.collectMs.toFixed(2)} ` +
            `totalMs=${result.metrics.totalMs.toFixed(1)}`,
        );

        send({
          type: 'advanceResult',
          version: result.version,
          replicaVersion: state.driver.replicaVersion,
          numChanges: result.numChanges,
          changes: result.changes,
          didReset: result.didReset,
          metrics: result.metrics,
        });
        break;
      }

      case 'destroyQuery': {
        const {clientGroupID, queryID} = msg;
        const state = clientGroups.get(clientGroupID);
        if (state) {
          state.driver.removeQuery(queryID);
          state.queryCount = Math.max(0, state.queryCount - 1);
          // Don't auto-destroy PipelineDriver when queryCount reaches 0.
          // Old destroyQuery messages from a previous ViewSyncer can arrive
          // after a new init has created a fresh PipelineDriver. The next
          // init will destroy and replace it cleanly.
        }
        send({type: 'destroyQueryResult', queryID});
        break;
      }

      case 'reset': {
        const {clientGroupID, clientSchema} = msg;
        const state = clientGroups.get(clientGroupID);
        if (state) {
          state.driver.reset(clientSchema);
          state.clientSchema = clientSchema;
        }
        send({type: 'resetResult'});
        break;
      }

      case 'destroyClientGroup': {
        const {clientGroupID, generation} = msg;
        const state = clientGroups.get(clientGroupID);
        // Only destroy if the generation matches. A newer init increments
        // the generation — stale destroyClientGroup from old ViewSyncer
        // has an older generation and is ignored.
        if (state && state.generation === generation) {
          lc.info?.(
            `pool-thread destroying clientGroup=${clientGroupID} ` +
              `generation=${generation} queries=${state.queryCount}`,
          );
          state.driver.destroy();
          clientGroups.delete(clientGroupID);
        } else if (state) {
          lc.info?.(
            `pool-thread ignoring stale destroyClientGroup=${clientGroupID} ` +
              `msgGen=${generation} currentGen=${state.generation}`,
          );
        }
        break;
      }

      case 'getRow': {
        const {clientGroupID, table, rowKey} = msg;
        const state = clientGroups.get(clientGroupID);
        let row: Row | undefined;
        if (state) {
          row = state.driver.getRow(table, rowKey as RowKey);
        }
        send({type: 'getRowResult', row});
        break;
      }

      case 'shutdown': {
        lc.info?.(
          `pool-thread shutting down with ${clientGroups.size} client groups`,
        );
        for (const [, state] of clientGroups) {
          state.driver.destroy();
        }
        clientGroups.clear();
        operatorStorage.close();
        process.exit(0);
        break;
      }
    }
  } catch (e) {
    const error = e instanceof Error ? e : new Error(String(e));
    lc.error?.(`pool-thread error handling ${msg.type}: ${error.message}`);
    send({
      type: 'error',
      message: error.message,
      name: error.name,
    });
  }
});
