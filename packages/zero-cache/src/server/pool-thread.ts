import {randomUUID} from 'crypto';
import {tmpdir} from 'os';
import path from 'path';
import {parentPort, workerData} from 'node:worker_threads';
import {assert} from '../../../shared/src/asserts.ts';
import {createLogContext} from '../../../shared/src/logging.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';
import {InspectorDelegate} from './inspector-delegate.ts';
import {
  PipelineDriver,
  type RowChange,
  type Timer,
} from '../services/view-syncer/pipeline-driver.ts';
import {Snapshotter} from '../services/view-syncer/snapshotter.ts';
import {DatabaseStorage} from '../../../zqlite/src/database-storage.ts';
import type {LogConfig} from '../config/zero-config.ts';
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

// One PipelineDriver per client group — same as the old code.
// Multiple queries on the same client group share one Snapshotter + TableSources.
type ClientGroupState = {
  driver: PipelineDriver;
  snapshotter: Snapshotter;
  clientSchema: ClientSchema;
  queryCount: number;
};
const clientGroups = new Map<string, ClientGroupState>();

function send(msg: PoolWorkerResult) {
  port!.postMessage(msg);
}

function createTimer(): Timer {
  const start = performance.now();
  let lapStart = start;
  return {
    elapsedLap: () => {
      const now = performance.now();
      return now - lapStart;
    },
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
  let state = clientGroups.get(clientGroupID);
  if (state) {
    return state;
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

  state = {driver, snapshotter, clientSchema, queryCount: 0};
  clientGroups.set(clientGroupID, state);

  lc.info?.(
    `pool-thread created PipelineDriver for clientGroup=${clientGroupID} ` +
      `totalClientGroups=${clientGroups.size}`,
  );

  return state;
}

lc.info?.(`pool-thread started. replica=${replicaFile}`);

port.on('message', (msg: PoolWorkerMsg) => {
  try {
    switch (msg.type) {
      case 'init': {
        // Pre-create the PipelineDriver for this client group
        const {clientGroupID, clientSchema} = msg;
        getOrCreateClientGroup(clientGroupID, clientSchema);
        send({type: 'initResult', version: '', replicaVersion: ''});
        break;
      }

      case 'hydrate': {
        const {clientGroupID, queryID, transformationHash, ast} = msg;

        // Get or create the shared PipelineDriver for this client group
        const state = clientGroups.get(clientGroupID);
        assert(
          state !== undefined,
          `Must send init for clientGroup=${clientGroupID} before hydrate`,
        );

        lc.info?.(
          `pool-thread hydrating query=${queryID} table=${ast.table} ` +
            `clientGroup=${clientGroupID}`,
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
            `version=${state.driver.currentVersion()} ` +
            `queriesInGroup=${state.queryCount} ` +
            `clientGroup=${clientGroupID}`,
        );

        send({
          type: 'hydrationResult',
          queryID,
          changes,
          hydrationTimeMs: timer.totalElapsed(),
          version: state.driver.currentVersion(),
          replicaVersion: state.driver.replicaVersion,
        });
        break;
      }

      case 'advance': {
        const {clientGroupID, targetVersion} = msg;
        const state = clientGroups.get(clientGroupID);
        assert(
          state !== undefined,
          `No PipelineDriver for clientGroup=${clientGroupID}`,
        );

        const start = performance.now();

        // One advance for ALL queries in this client group.
        // One Snapshotter leapfrog, one ChangeLog scan, shared TableSources.
        const result = state.driver.advanceWithRecovery(
          state.clientSchema,
          createTimer(),
          targetVersion,
        );

        const elapsed = performance.now() - start;
        lc.info?.(
          `pool-thread advanced clientGroup=${clientGroupID} ` +
            `queries=${state.queryCount} to=${result.version} ` +
            `changes=${result.numChanges} rows=${result.changes.length} ` +
            `didReset=${result.didReset} ` +
            `snapshotMs=${result.metrics.snapshotMs.toFixed(2)} ` +
            `diffReadMs=${result.metrics.diffReadMs.toFixed(2)} ` +
            `ivmPushMs=${result.metrics.ivmPushMs.toFixed(2)} ` +
            `collectMs=${result.metrics.collectMs.toFixed(2)} ` +
            `totalMs=${elapsed.toFixed(1)}`,
        );

        send({
          type: 'advanceResult',
          version: result.version,
          numChanges: result.numChanges,
          changes: result.changes,
        });
        break;
      }

      case 'destroyQuery': {
        const {clientGroupID, queryID} = msg;
        const state = clientGroups.get(clientGroupID);
        if (state) {
          lc.info?.(
            `pool-thread destroying query=${queryID} clientGroup=${clientGroupID}`,
          );
          state.driver.removeQuery(queryID);
          state.queryCount--;

          // If no more queries, destroy the PipelineDriver
          if (state.queryCount <= 0) {
            lc.info?.(
              `pool-thread destroying PipelineDriver for clientGroup=${clientGroupID}`,
            );
            state.driver.destroy();
            clientGroups.delete(clientGroupID);
          }
        }
        send({type: 'destroyQueryResult', queryID});
        break;
      }

      case 'reset': {
        const {clientGroupID, clientSchema} = msg;
        const state = clientGroups.get(clientGroupID);
        if (state) {
          lc.info?.(
            `pool-thread reset clientGroup=${clientGroupID} ` +
              `queries=${state.queryCount}`,
          );
          state.driver.reset(clientSchema);
          state.clientSchema = clientSchema;
        }
        send({type: 'resetResult', version: '', replicaVersion: ''});
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
