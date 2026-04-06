import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {Worker} from 'node:worker_threads';
import {assert} from '../../../../shared/src/asserts.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {LoadedPermissions} from '../../auth/load-permissions.ts';
import type {RowKey} from '../../types/row-key.ts';
import type {InitResult} from '../../workers/pool-protocol.ts';
import type {
  AdvanceResult,
  GetRowResult,
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
  requestId: string;
  resolve: (result: PoolWorkerResult) => void;
  reject: (error: Error) => void;
};

/**
 * Implements PipelineDriverInterface by delegating to a pool worker thread.
 * One RemotePipelineDriver per client group. All queries for the client group
 * go to the SAME pool thread, which runs the EXACT same PipelineDriver code.
 *
 * No syncer-side SQLite connection. No version reading. The pool thread
 * owns the Snapshotter and advances to head on its own.
 */
export class RemotePipelineDriver implements PipelineDriverInterface {
  readonly #lc: LogContext;
  readonly #threads: PoolThread[];
  readonly #clientGroupID: string;
  #assignedThread: PoolThread | null = null;
  #threadInitialized = false;
  #initialized = false;
  #lastVersion = '00';
  #lastReplicaVersion = '';
  #lastPermissions: LoadedPermissions | null = null;

  readonly #queryInfos = new Map<
    string,
    {transformedAst: AST; transformationHash: string}
  >();
  readonly #hydrationTimes = new Map<string, number>();

  readonly #pendingResponses = new Map<Worker, PendingResponse[]>();

  constructor(
    lc: LogContext,
    threads: Worker[],
    clientGroupID: string,
  ) {
    this.#lc = lc.withContext('component', 'remote-pipeline-driver');
    this.#threads = threads.map(worker => ({worker, load: 0}));
    this.#clientGroupID = clientGroupID;

    for (const thread of this.#threads) {
      this.#pendingResponses.set(thread.worker, []);
      thread.worker.on(
        'message',
        (msg: PoolWorkerResult & {requestId?: string}) => {
          const queue = this.#pendingResponses.get(thread.worker)!;
          const idx = queue.findIndex(p => p.requestId === msg.requestId);
          if (idx >= 0) {
            const pending = queue.splice(idx, 1)[0];
            if (msg.type === 'error') {
              pending.reject(
                Object.assign(new Error(msg.message), {name: msg.name}),
              );
            } else {
              pending.resolve(msg);
            }
          }
        },
      );
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
      `RemotePipelineDriver created for clientGroup=${clientGroupID} ` +
        `with ${threads.length} pool threads`,
    );
  }

  static #nextRequestId = 0;

  #sendAndWait(
    thread: PoolThread,
    msg: PoolWorkerMsg,
  ): Promise<PoolWorkerResult> {
    const requestId = String(RemotePipelineDriver.#nextRequestId++);
    const {promise, resolve, reject} = resolver<PoolWorkerResult>();
    this.#pendingResponses.get(thread.worker)!.push({requestId, resolve, reject});
    thread.worker.postMessage({...msg, requestId});
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
    return this.#lastReplicaVersion;
  }

  initialized(): boolean {
    return this.#initialized;
  }

  async init(clientSchema: ClientSchema): Promise<void> {
    if (!this.#assignedThread) {
      this.#assignedThread = this.#leastLoadedThread();
      this.#assignedThread.load++;
    }

    if (!this.#threadInitialized) {
      const initResult = (await this.#sendAndWait(this.#assignedThread, {
        type: 'init',
        clientGroupID: this.#clientGroupID,
        clientSchema,
      })) as InitResult;
      this.#lastVersion = initResult.version;
      this.#lastReplicaVersion = initResult.replicaVersion;
      this.#lastPermissions = initResult.permissions;
      this.#threadInitialized = true;
    }

    this.#initialized = true;
    this.#lc.info?.('RemotePipelineDriver initialized');
  }

  async reset(clientSchema: ClientSchema): Promise<void> {
    if (this.#assignedThread && this.#threadInitialized) {
      await this.#sendAndWait(this.#assignedThread, {
        type: 'reset',
        clientGroupID: this.#clientGroupID,
        clientSchema,
      });
    }

    this.#queryInfos.clear();
    this.#hydrationTimes.clear();
    this.#lc.info?.('RemotePipelineDriver reset');
  }

  async advanceWithoutDiff(): Promise<string> {
    // Pool thread's PipelineDriver handles this internally during advance.
    // For now, return the last known version.
    return this.#lastVersion;
  }

  currentVersion(): string {
    return this.#lastVersion;
  }

  currentPermissions(): LoadedPermissions | null {
    return this.#lastPermissions;
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
    assert(this.#assignedThread !== null, 'Must call init before addQuery');
    const thread = this.#assignedThread;

    this.#lc.info?.(
      `RemotePipelineDriver hydrating query=${queryID} table=${query.table} ` +
        `clientGroup=${this.#clientGroupID}`,
    );

    const result = (await this.#sendAndWait(thread, {
      type: 'hydrate',
      clientGroupID: this.#clientGroupID,
      queryID,
      transformationHash,
      ast: query,
    }));

    if (result.type !== 'hydrationResult') {
      this.#lc.error?.(
        `RemotePipelineDriver addQuery got unexpected response type=${result.type} ` +
          `for query=${queryID} clientGroup=${this.#clientGroupID} ` +
          `response=${JSON.stringify(result).substring(0, 200)}`,
      );
      throw new Error(
        `Expected hydrationResult, got ${result.type} for query=${queryID}`,
      );
    }
    const hydrationResult = result as HydrationResult;

    this.#queryInfos.set(queryID, {transformedAst: query, transformationHash});
    this.#hydrationTimes.set(queryID, hydrationResult.hydrationTimeMs);

    this.#lc.info?.(
      `RemotePipelineDriver hydrated query=${queryID} ` +
        `rows=${hydrationResult.changes.length} ` +
        `time=${hydrationResult.hydrationTimeMs.toFixed(1)}ms`,
    );

    return hydrationResult.changes;
  }

  removeQuery(queryID: string): void {
    if (this.#assignedThread) {
      this.#assignedThread.worker.postMessage({
        type: 'destroyQuery',
        clientGroupID: this.#clientGroupID,
        queryID,
      } satisfies PoolWorkerMsg);
      this.#queryInfos.delete(queryID);
      this.#hydrationTimes.delete(queryID);
    }
  }

  async getRow(
    table: string,
    pk: RowKey,
  ): Promise<Row | undefined> {
    if (!this.#assignedThread) {
      return undefined;
    }
    const result = (await this.#sendAndWait(this.#assignedThread, {
      type: 'getRow',
      clientGroupID: this.#clientGroupID,
      table,
      rowKey: pk as Record<string, unknown>,
    })) as GetRowResult;
    return result.row;
  }

  async advance(
    _timer: Timer,
  ): Promise<{
    version: string;
    numChanges: number;
    changes: Iterable<RowChange | 'yield'>;
  }> {
    if (!this.#assignedThread || this.#queryInfos.size === 0) {
      return {version: this.#lastVersion, numChanges: 0, changes: []};
    }

    const start = performance.now();

    const result = (await this.#sendAndWait(this.#assignedThread, {
      type: 'advance',
      clientGroupID: this.#clientGroupID,
    })) as AdvanceResult;

    this.#lastVersion = result.version;
    this.#lastReplicaVersion = result.replicaVersion;
    const elapsed = performance.now() - start;

    this.#lc.info?.(
      `RemotePipelineDriver advanced clientGroup=${this.#clientGroupID} ` +
        `to=${result.version} changes=${result.numChanges} ` +
        `rows=${result.changes.length} time=${elapsed.toFixed(1)}ms`,
    );

    return {
      version: result.version,
      numChanges: result.numChanges,
      changes: result.changes,
    };
  }

  async advanceWithRecovery(
    _clientSchema: ClientSchema,
    _timer: Timer,
  ): Promise<AdvanceWithRecoveryResult> {
    // Pool thread handles ResetPipelinesSignal internally via
    // PipelineDriver.advanceWithRecovery(). Syncer never sees the error.
    if (!this.#assignedThread || this.#queryInfos.size === 0) {
      return {
        version: this.#lastVersion,
        numChanges: 0,
        changes: [],
        didReset: false,
        metrics: {snapshotMs: 0, collectMs: 0, diffReadMs: 0, ivmPushMs: 0, totalMs: 0},
      };
    }

    const result = (await this.#sendAndWait(this.#assignedThread, {
      type: 'advance',
      clientGroupID: this.#clientGroupID,
    })) as AdvanceResult;

    this.#lastVersion = result.version;
    this.#lastReplicaVersion = result.replicaVersion;

    return {
      version: result.version,
      numChanges: result.numChanges,
      changes: result.changes,
      didReset: result.didReset,
      metrics: result.metrics,
    };
  }

  destroy(): void {
    this.#lc.info?.('RemotePipelineDriver destroying');
    if (this.#assignedThread) {
      for (const queryID of this.#queryInfos.keys()) {
        this.#assignedThread.worker.postMessage({
          type: 'destroyQuery',
          clientGroupID: this.#clientGroupID,
          queryID,
        } satisfies PoolWorkerMsg);
      }
      this.#assignedThread.load--;
      this.#assignedThread = null;
    }
    this.#queryInfos.clear();
    this.#hydrationTimes.clear();
    this.#threadInitialized = false;
    this.#initialized = false;
  }
}
