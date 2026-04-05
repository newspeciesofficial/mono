import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {Worker} from 'node:worker_threads';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {
  type LoadedPermissions,
  reloadPermissionsIfChanged,
} from '../../auth/load-permissions.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {RowKey} from '../../types/row-key.ts';
import type {ShardID} from '../../types/shards.ts';
import {ResetPipelinesSignal} from './snapshotter.ts';
import {getSubscriptionState} from '../replicator/schema/replication-state.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {
  AdvanceResult,
  HydrationResult,
  PoolWorkerMsg,
  PoolWorkerResult,
} from '../../workers/pool-protocol.ts';
import type {
  AdvanceWithRecoveryResult,
  PipelineDriverInterface,
  RowChange,
  Timer,
} from './pipeline-driver.ts';

type PoolThread = {
  worker: Worker;
  load: number;
};

type PendingResponse = {
  resolve: (result: PoolWorkerResult) => void;
  reject: (error: Error) => void;
};

/**
 * Implements PipelineDriverInterface by delegating IVM work to pool worker
 * threads. The syncer's ViewSyncer holds this instead of a local PipelineDriver.
 *
 * Each query is assigned to a pool thread. Hydration and advance happen on
 * the thread. Results come back as RowChange[] via structured clone over
 * MessagePort. CVR, pokes, and WebSocket handling stay in the syncer.
 */
export class RemotePipelineDriver implements PipelineDriverInterface {
  readonly #lc: LogContext;
  readonly #threads: PoolThread[];
  readonly #queryAssignments = new Map<string, PoolThread>();
  readonly #queryInfos = new Map<
    string,
    {transformedAst: AST; transformationHash: string}
  >();
  readonly #hydrationTimes = new Map<string, number>();

  // Single SQLite read connection in the syncer for getRow + version reads.
  readonly #replicaDb: Database;
  readonly #stmtRunner: StatementRunner;
  readonly #shardID: ShardID;
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();

  #initialized = false;
  #replicaVersion: string | null = null;
  #permissions: LoadedPermissions | null = null;
  #clientSchema: ClientSchema | null = null;
  readonly #initializedThreads = new Set<Worker>();

  // Pending response tracking per thread.
  readonly #pendingResponses = new Map<Worker, PendingResponse[]>();

  constructor(
    lc: LogContext,
    threads: Worker[],
    replicaFile: string,
    shardID: ShardID,
  ) {
    this.#lc = lc.withContext('component', 'remote-pipeline-driver');
    this.#threads = threads.map(worker => ({worker, load: 0}));
    this.#replicaDb = new Database(lc, replicaFile);
    this.#stmtRunner = new StatementRunner(this.#replicaDb);
    this.#shardID = shardID;

    // Set up message handlers for each thread.
    for (const thread of this.#threads) {
      this.#pendingResponses.set(thread.worker, []);
      thread.worker.on('message', (msg: PoolWorkerResult) => {
        const queue = this.#pendingResponses.get(thread.worker)!;
        const pending = queue.shift();
        if (pending) {
          if (msg.type === 'error') {
            this.#lc.warn?.(
              `RemotePipelineDriver received error from pool thread: ` +
                `name=${msg.name} message=${msg.message}`,
            );
            const error =
              msg.name === 'ResetPipelinesSignal'
                ? new ResetPipelinesSignal(msg.message)
                : Object.assign(new Error(msg.message), {name: msg.name});
            pending.reject(error);
          } else {
            pending.resolve(msg);
          }
        }
      });
      thread.worker.on('error', (err: Error) => {
        this.#lc.error?.(`pool thread error: ${err.message}`);
        const queue = this.#pendingResponses.get(thread.worker)!;
        for (const pending of queue) {
          pending.reject(err);
        }
        queue.length = 0;
      });
    }

    this.#lc.info?.(
      `RemotePipelineDriver created with ${threads.length} pool threads`,
    );
  }

  #sendAndWait(
    thread: PoolThread,
    msg: PoolWorkerMsg,
  ): Promise<PoolWorkerResult> {
    const {promise, resolve, reject} = resolver<PoolWorkerResult>();
    this.#pendingResponses.get(thread.worker)!.push({resolve, reject});
    thread.worker.postMessage(msg);
    return promise;
  }

  #leastLoadedThread(): PoolThread {
    let best = this.#threads[0];
    for (const thread of this.#threads) {
      if (thread.load < best.load) {
        best = thread;
      }
    }
    return best;
  }

  // --- PipelineDriverInterface ---

  get replicaVersion(): string {
    if (this.#replicaVersion === null) {
      throw new Error('Not yet initialized');
    }
    return this.#replicaVersion;
  }

  initialized(): boolean {
    return this.#initialized;
  }

  init(clientSchema: ClientSchema): void {
    this.#clientSchema = clientSchema;
    this.#refreshSpecs();
    this.#initialized = true;
    this.#lc.info?.('RemotePipelineDriver initialized');
  }

  async reset(clientSchema: ClientSchema): Promise<void> {
    this.#clientSchema = clientSchema;

    // Destroy all queries on all threads.
    const promises: Promise<unknown>[] = [];
    for (const [queryID, thread] of this.#queryAssignments) {
      promises.push(this.#sendAndWait(thread, {type: 'destroyQuery', queryID}));
      thread.load--;
    }
    await Promise.all(promises);

    this.#queryAssignments.clear();
    this.#queryInfos.clear();
    this.#hydrationTimes.clear();
    this.#initializedThreads.clear();
    this.#refreshSpecs();
    this.#lc.info?.('RemotePipelineDriver reset');
  }

  advanceWithoutDiff(): string {
    this.#refreshSpecs();
    const version = this.currentVersion();
    this.#lc.debug?.(`advanceWithoutDiff to ${version}`);
    return version;
  }

  currentVersion(): string {
    const row = this.#replicaDb
      .prepare('SELECT "stateVersion" FROM "_zero.replicationState"')
      .get() as {stateVersion: string} | undefined;
    return row?.stateVersion ?? '00';
  }

  currentPermissions(): LoadedPermissions | null {
    const res = reloadPermissionsIfChanged(
      this.#lc,
      this.#stmtRunner,
      this.#shardID.appID,
      this.#permissions,
    );
    if (res.changed) {
      this.#permissions = res.permissions;
    }
    return this.#permissions;
  }

  totalHydrationTimeMs(): number {
    let total = 0;
    for (const time of this.#hydrationTimes.values()) {
      total += time;
    }
    return total;
  }

  queries(): ReadonlyMap<
    string,
    {transformedAst: AST; transformationHash: string}
  > {
    return this.#queryInfos;
  }

  async addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    _timer: Timer,
  ): Promise<Iterable<RowChange | 'yield'>> {
    // Remove existing if re-adding.
    if (this.#queryAssignments.has(queryID)) {
      this.removeQuery(queryID);
    }

    const thread = this.#leastLoadedThread();
    this.#queryAssignments.set(queryID, thread);
    thread.load++;

    this.#lc.info?.(
      `RemotePipelineDriver hydrating query=${queryID} table=${query.table} ` +
        `on thread load=${thread.load}`,
    );

    // Send init if this thread hasn't been initialized yet.
    if (!this.#initializedThreads.has(thread.worker)) {
      await this.#sendAndWait(thread, {
        type: 'init',
        clientSchema: this.#clientSchema!,
      });
      this.#initializedThreads.add(thread.worker);
    }

    const result = (await this.#sendAndWait(thread, {
      type: 'hydrate',
      queryID,
      transformationHash,
      ast: query,
    })) as HydrationResult;

    this.#queryInfos.set(queryID, {
      transformedAst: query,
      transformationHash,
    });
    this.#hydrationTimes.set(queryID, result.hydrationTimeMs);

    this.#lc.info?.(
      `RemotePipelineDriver hydrated query=${queryID} ` +
        `rows=${result.changes.length} ` +
        `time=${result.hydrationTimeMs.toFixed(1)}ms`,
    );

    return result.changes;
  }

  removeQuery(queryID: string): void {
    const thread = this.#queryAssignments.get(queryID);
    if (thread) {
      this.#lc.info?.(`RemotePipelineDriver removing query=${queryID}`);
      // Fire and forget.
      thread.worker.postMessage({
        type: 'destroyQuery',
        queryID,
      } satisfies PoolWorkerMsg);
      thread.load--;
      this.#queryAssignments.delete(queryID);
      this.#queryInfos.delete(queryID);
      this.#hydrationTimes.delete(queryID);
    }
  }

  getRow(table: string, pk: RowKey): Row | undefined {
    const spec = this.#tableSpecs.get(table);
    if (!spec) {
      return undefined;
    }
    const cols = Object.keys(spec.zqlSpec.columns);
    const keyCols = Object.keys(pk);
    const sql = `SELECT ${cols.map(c => `"${c}"`).join(',')} FROM "${table}" WHERE ${keyCols.map(c => `"${c}"=?`).join(' AND ')}`;
    return this.#replicaDb.prepare(sql).get(...Object.values(pk)) as
      | Row
      | undefined;
  }

  async advance(_timer: Timer): Promise<{
    version: string;
    numChanges: number;
    changes: Iterable<RowChange | 'yield'>;
  }> {
    this.#refreshSpecs();
    const targetVersion = this.currentVersion();

    if (this.#queryAssignments.size === 0) {
      return {version: targetVersion, numChanges: 0, changes: []};
    }

    this.#lc.debug?.(
      `RemotePipelineDriver advancing ${this.#queryAssignments.size} queries ` +
        `to version=${targetVersion}`,
    );

    const start = performance.now();

    // Fan out advance to all threads that have queries.
    const threadsWithQueries = new Set<PoolThread>();
    for (const thread of this.#queryAssignments.values()) {
      threadsWithQueries.add(thread);
    }

    let results: PoolWorkerResult[];
    try {
      results = await Promise.all(
        [...threadsWithQueries].map(thread =>
          this.#sendAndWait(thread, {
            type: 'advance',
            targetVersion,
          }),
        ),
      );
    } catch (e) {
      const elapsed = performance.now() - start;
      const error = e instanceof Error ? e : new Error(String(e));
      this.#lc.error?.(
        `RemotePipelineDriver advance FAILED after ${elapsed.toFixed(1)}ms: ` +
          `${error.name}: ${error.message} ` +
          `(${this.#queryAssignments.size} queries on ${threadsWithQueries.size} threads)`,
      );
      throw e;
    }

    // Merge results.
    const allChanges: RowChange[] = [];
    let totalNumChanges = 0;
    for (const result of results) {
      const advResult = result as AdvanceResult;
      totalNumChanges += advResult.numChanges;
      allChanges.push(...advResult.changes);
    }

    const elapsed = performance.now() - start;
    this.#lc.info?.(
      `RemotePipelineDriver advanced ${threadsWithQueries.size} threads ` +
        `to=${targetVersion} changes=${totalNumChanges} ` +
        `resultRows=${allChanges.length} time=${elapsed.toFixed(1)}ms`,
    );

    return {
      version: targetVersion,
      numChanges: totalNumChanges,
      changes: allChanges,
    };
  }

  async advanceWithRecovery(
    _clientSchema: ClientSchema,
    timer: Timer,
    _upperBound?: string | undefined,
  ): Promise<AdvanceWithRecoveryResult> {
    // Pool threads handle ResetPipelinesSignal internally via
    // PipelineDriver.advanceWithRecovery(). The syncer never sees the error.
    const result = await this.advance(timer);
    const changes: RowChange[] = [];
    for (const change of result.changes) {
      if (change !== 'yield') {
        changes.push(change);
      }
    }
    return {
      version: result.version,
      numChanges: result.numChanges,
      changes,
      didReset: false,
      metrics: {snapshotMs: 0, collectMs: 0, diffReadMs: 0, ivmPushMs: 0, totalMs: 0},
    };
  }

  destroy(): void {
    this.#lc.info?.('RemotePipelineDriver destroying');
    for (const [queryID, thread] of this.#queryAssignments) {
      thread.worker.postMessage({
        type: 'destroyQuery',
        queryID,
      } satisfies PoolWorkerMsg);
      thread.load--;
    }
    this.#queryAssignments.clear();
    this.#queryInfos.clear();
    this.#hydrationTimes.clear();
    this.#replicaDb.close();
  }

  #refreshSpecs(): void {
    computeZqlSpecs(
      this.#lc,
      this.#replicaDb,
      {includeBackfillingColumns: false},
      this.#tableSpecs,
    );
    const {replicaVersion} = getSubscriptionState(this.#stmtRunner);
    this.#replicaVersion = replicaVersion;
  }
}
