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
import {
  ResetPipelinesSignal,
  Snapshotter,
} from '../services/view-syncer/snapshotter.ts';
import {DatabaseStorage} from '../../../zqlite/src/database-storage.ts';
import type {LogConfig} from '../config/zero-config.ts';
import type {ShardID} from '../types/shards.ts';
import type {
  PoolWorkerMsg,
  PoolWorkerResult,
} from '../workers/pool-protocol.ts';

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

// Each query gets its own PipelineDriver + Snapshotter.
const drivers = new Map<
  string,
  {driver: PipelineDriver; snapshotter: Snapshotter}
>();

let clientSchema: ClientSchema | null = null;

function send(msg: PoolWorkerResult) {
  port!.postMessage(msg);
}

// Simple timer for hydration/advance.
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
      // Pool worker runs on its own core -- no need to yield for IO.
      continue;
    }
    result.push(change);
  }
  return result;
}

lc.info?.(`pool-thread started. replica=${replicaFile}`);

port.on('message', (msg: PoolWorkerMsg) => {
  try {
    switch (msg.type) {
      case 'init': {
        lc.info?.('pool-thread received init');
        clientSchema = msg.clientSchema;
        send({type: 'initResult', version: '', replicaVersion: ''});
        break;
      }

      case 'hydrate': {
        assert(clientSchema !== null, 'Must send init before hydrate');
        const {queryID, transformationHash, ast} = msg;
        lc.info?.(`pool-thread hydrating query=${queryID} table=${ast.table}`);

        // Create a PipelineDriver + Snapshotter for this query.
        const snapshotter = new Snapshotter(lc, replicaFile, shardID);
        const driver = new PipelineDriver(
          lc.withContext('queryID', queryID),
          logConfig,
          snapshotter,
          shardID,
          operatorStorage.createClientGroupStorage(queryID),
          queryID,
          inspectorDelegate,
          () => yieldThresholdMs,
          enableQueryPlanner,
        );

        // Initialize and hydrate.
        driver.init(clientSchema);
        const timer = createTimer();
        const changes = collectChanges(
          driver.addQuery(transformationHash, queryID, ast, timer),
        );

        drivers.set(queryID, {driver, snapshotter});

        lc.info?.(
          `pool-thread hydrated query=${queryID} rows=${changes.length} ` +
            `time=${timer.totalElapsed().toFixed(1)}ms version=${driver.currentVersion()}`,
        );

        send({
          type: 'hydrationResult',
          queryID,
          changes,
          hydrationTimeMs: timer.totalElapsed(),
          version: driver.currentVersion(),
          replicaVersion: driver.replicaVersion,
        });
        break;
      }

      case 'advance': {
        const {targetVersion} = msg;
        const allChanges: RowChange[] = [];
        let version = targetVersion;
        let totalNumChanges = 0;
        const start = performance.now();

        for (const [queryID, {driver}] of drivers) {
          try {
            const result = driver.advance(createTimer(), targetVersion);
            version = result.version;
            totalNumChanges += result.numChanges;
            allChanges.push(...collectChanges(result.changes));
          } catch (e) {
            if (e instanceof ResetPipelinesSignal) {
              lc.info?.(
                `pool-thread ResetPipelinesSignal for query=${queryID}: ${e.message}`,
              );
              throw e;
            }
            throw e;
          }
        }

        const elapsed = performance.now() - start;
        lc.debug?.(
          `pool-thread advanced ${drivers.size} queries to=${version} ` +
            `changes=${totalNumChanges} resultRows=${allChanges.length} ` +
            `time=${elapsed.toFixed(1)}ms`,
        );

        send({
          type: 'advanceResult',
          version,
          numChanges: totalNumChanges,
          changes: allChanges,
        });
        break;
      }

      case 'destroyQuery': {
        const {queryID} = msg;
        lc.info?.(`pool-thread destroying query=${queryID}`);
        const entry = drivers.get(queryID);
        if (entry) {
          entry.driver.destroy();
          drivers.delete(queryID);
        }
        send({type: 'destroyQueryResult', queryID});
        break;
      }

      case 'reset': {
        lc.info?.(`pool-thread reset with ${drivers.size} queries`);
        clientSchema = msg.clientSchema;
        for (const [, {driver}] of drivers) {
          driver.reset(clientSchema);
        }
        // Return empty version -- syncer reads version from its own connection.
        send({type: 'resetResult', version: '', replicaVersion: ''});
        break;
      }

      case 'shutdown': {
        lc.info?.(`pool-thread shutting down with ${drivers.size} queries`);
        for (const [, {driver}] of drivers) {
          driver.destroy();
        }
        drivers.clear();
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
