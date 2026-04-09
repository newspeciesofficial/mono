/**
 * Syncer-side proxy for a pool worker thread-hosted `PipelineDriver`.
 *
 * One instance per ViewSyncer (per client group). Messages to the pool
 * thread are posted via a `PoolThreadHandle` provided by `PoolThreadManager`,
 * which owns the actual `MessagePort` and dispatches incoming responses to
 * the right driver by `clientGroupID`.
 *
 * Design rules (v3):
 * - Stock `PipelineDriver` is NOT modified. This proxy does not implement a
 *   shared interface with it — ViewSyncer stores `#pipelines` as a union type
 *   and relies on TypeScript's handling of `void | Promise<void>` /
 *   `Iterable | AsyncIterable` so that `await` and `for await` work on both.
 * - Every async method allocates a fresh `requestId` captured in the
 *   resolver closure. There is no module-level state.
 * - Sync accessors (`initialized()`, `currentVersion()`, `replicaVersion`,
 *   `currentPermissions()`, `totalHydrationTimeMs()`, `queries()`) read from
 *   a cached `DriverState` updated by every response that changes it.
 * - Advance is streamed via an async iterable fed by a push-pull buffer.
 *   The pool thread posts `advanceBegin` (resolves the outer `await`),
 *   then 0+ `advanceChangeBatch`, then `advanceComplete`. An `error`
 *   message at any point aborts the stream.
 * - `destroy()` unregisters from the manager and fails any pending requests
 *   so awaiters don't hang. The manager owns the single port listener, so
 *   there are no per-driver listeners to leak (v2's 303ed7411 bug cannot
 *   recur).
 */

import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {LoadedPermissions} from '../../auth/load-permissions.ts';
import type {RowKey} from '../../types/row-key.ts';
import type {
  DriverState,
  PoolWorkerMsg,
  PoolWorkerResult,
} from '../../workers/pool-protocol.ts';
import type {RowChange, Timer} from './pipeline-driver.ts';

/**
 * Minimal manager-facing view of a pool thread. `PoolThreadManager`
 * implements this; the driver only needs to post messages and announce its
 * destruction.
 */
export interface PoolThreadHandle {
  readonly poolThreadIdx: number;
  postMessage(msg: PoolWorkerMsg): void;
  unregister(clientGroupID: string): void;
}

type PendingResponse = {
  readonly resolve: (msg: PoolWorkerResult) => void;
  readonly reject: (err: Error) => void;
};

type StreamWaiter = {
  readonly resolve: (msg: StreamPullResult) => void;
  readonly reject: (err: Error) => void;
};

type StreamPullResult =
  | {done: false; batch: readonly RowChange[]}
  | {done: true};

/**
 * Push-pull buffer for a streaming `advance` response. Messages posted by
 * the pool thread are pushed here; the async iterable returned by
 * `advance()` pulls from it.
 */
class AdvanceStream {
  readonly #buffered: Array<{type: 'batch'; batch: readonly RowChange[]}> = [];
  #begin: {
    version: string;
    numChanges: number;
    snapshotMs: number;
  } | null = null;
  #beginWaiter: {
    resolve: (v: {
      version: string;
      numChanges: number;
      snapshotMs: number;
    }) => void;
    reject: (err: Error) => void;
  } | null = null;
  #done = false;
  #error: Error | null = null;
  #waiter: StreamWaiter | null = null;

  awaitBegin(): Promise<{
    version: string;
    numChanges: number;
    snapshotMs: number;
  }> {
    if (this.#error) return Promise.reject(this.#error);
    if (this.#begin) return Promise.resolve(this.#begin);
    return new Promise((resolve, reject) => {
      this.#beginWaiter = {resolve, reject};
    });
  }

  onBegin(version: string, numChanges: number, snapshotMs: number): void {
    this.#begin = {version, numChanges, snapshotMs};
    if (this.#beginWaiter) {
      const w = this.#beginWaiter;
      this.#beginWaiter = null;
      w.resolve(this.#begin);
    }
  }

  onBatch(changes: readonly RowChange[]): void {
    if (this.#waiter) {
      const w = this.#waiter;
      this.#waiter = null;
      w.resolve({done: false, batch: changes});
      return;
    }
    this.#buffered.push({type: 'batch', batch: changes});
  }

  onComplete(): void {
    this.#done = true;
    if (this.#waiter) {
      const w = this.#waiter;
      this.#waiter = null;
      w.resolve({done: true});
    }
  }

  onError(err: Error): void {
    this.#error = err;
    if (this.#beginWaiter) {
      const w = this.#beginWaiter;
      this.#beginWaiter = null;
      w.reject(err);
    }
    if (this.#waiter) {
      const w = this.#waiter;
      this.#waiter = null;
      w.reject(err);
    }
  }

  pull(): Promise<StreamPullResult> {
    if (this.#error) return Promise.reject(this.#error);
    if (this.#buffered.length > 0) {
      const next = this.#buffered.shift()!;
      return Promise.resolve({done: false, batch: next.batch});
    }
    if (this.#done) return Promise.resolve({done: true});
    return new Promise((resolve, reject) => {
      this.#waiter = {resolve, reject};
    });
  }
}

/**
 * Minimal query info surfaced by `queries()`. Only what ViewSyncer actually
 * reads (see view-syncer.ts lines 1560, 1671): the transformation hash is
 * used to decide whether a query needs re-hydration.
 */
export type RemoteQueryInfo = {
  readonly transformationHash: string;
};

export class RemotePipelineDriver {
  readonly #handle: PoolThreadHandle;
  readonly #clientGroupID: string;
  readonly #lc: LogContext;

  #nextRequestId = 1;
  readonly #pendingResponses = new Map<number, PendingResponse>();
  readonly #streams = new Map<number, AdvanceStream>();

  // Cached state from the most recent init/addQuery/removeQuery/advance/reset
  // response. `null` until `init()` resolves.
  #state: DriverState | null = null;
  #generation = 0;
  #destroyed = false;

  constructor(lc: LogContext, handle: PoolThreadHandle, clientGroupID: string) {
    this.#lc = lc.withContext('component', 'remote-pipeline-driver');
    this.#handle = handle;
    this.#clientGroupID = clientGroupID;
  }

  get clientGroupID(): string {
    return this.#clientGroupID;
  }

  get poolThreadIdx(): number {
    return this.#handle.poolThreadIdx;
  }

  // -------------------------------------------------------------------------
  // Cached sync accessors — no round-trip
  // -------------------------------------------------------------------------

  initialized(): boolean {
    return this.#state !== null;
  }

  get replicaVersion(): string {
    assert(this.#state, 'RemotePipelineDriver: not yet initialized');
    return this.#state.replicaVersion;
  }

  currentVersion(): string {
    assert(this.#state, 'RemotePipelineDriver: not yet initialized');
    return this.#state.version;
  }

  currentPermissions(): LoadedPermissions | null {
    assert(this.#state, 'RemotePipelineDriver: not yet initialized');
    return this.#state.permissions;
  }

  totalHydrationTimeMs(): number {
    return this.#state?.totalHydrationTimeMs ?? 0;
  }

  /**
   * Returns an empty list in pool mode. The stock driver exposes per-query
   * hydration times for the "top contributors" diagnostic log on reset; in
   * pool mode we do not ship this detail back in every state snapshot (it
   * would bloat every advance response). Pool thread logs already capture
   * per-query hydration time at the source.
   */
  hydrationBudgetBreakdown(): {id: string; table: string; ms: number}[] {
    return [];
  }

  queries(): ReadonlyMap<string, RemoteQueryInfo> {
    const map = new Map<string, RemoteQueryInfo>();
    if (this.#state) {
      for (const [qid, hash] of Object.entries(this.#state.queries)) {
        map.set(qid, {transformationHash: hash});
      }
    }
    return map;
  }

  // -------------------------------------------------------------------------
  // Async methods — round-trip to pool thread
  // -------------------------------------------------------------------------

  async init(clientSchema: ClientSchema): Promise<void> {
    const result = await this.#sendAndWait({
      type: 'init',
      requestId: 0, // assigned in #sendAndWait
      clientGroupID: this.#clientGroupID,
      clientSchema,
    });
    assert(
      result.type === 'initResult',
      `init: expected initResult, got ${result.type}`,
    );
    this.#state = result.state;
    this.#generation = result.generation;
  }

  async reset(clientSchema: ClientSchema): Promise<void> {
    const result = await this.#sendAndWait({
      type: 'reset',
      requestId: 0,
      clientGroupID: this.#clientGroupID,
      clientSchema,
    });
    assert(
      result.type === 'resetResult',
      `reset: expected resetResult, got ${result.type}`,
    );
    this.#state = result.state;
  }

  async advanceWithoutDiff(): Promise<string> {
    const result = await this.#sendAndWait({
      type: 'advanceWithoutDiff',
      requestId: 0,
      clientGroupID: this.#clientGroupID,
    });
    assert(
      result.type === 'advanceWithoutDiffResult',
      `advanceWithoutDiff: expected advanceWithoutDiffResult, got ${result.type}`,
    );
    this.#state = result.state;
    return result.version;
  }

  async addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    _timer: Timer,
  ): Promise<Iterable<RowChange | 'yield'>> {
    const result = await this.#sendAndWait({
      type: 'addQuery',
      requestId: 0,
      clientGroupID: this.#clientGroupID,
      queryID,
      transformationHash,
      ast: query,
    });
    assert(
      result.type === 'addQueryResult',
      `addQuery: expected addQueryResult, got ${result.type}`,
    );
    this.#state = result.state;
    return result.changes;
  }

  removeQuery(queryID: string): void {
    // Fire-and-forget from ViewSyncer's perspective, but we still send a
    // requestId for log correlation. We do not await — the pool thread
    // applies the removal synchronously on arrival.
    if (this.#destroyed) return;
    const requestId = this.#allocRequestId();
    this.#postMessage({
      type: 'removeQuery',
      requestId,
      clientGroupID: this.#clientGroupID,
      queryID,
    });
    // The matching `removeQueryResult` will arrive later. Register a
    // pending responder that just updates the cached state so subsequent
    // `queries()` calls see the removal. Resolution/rejection is discarded
    // by the caller (ViewSyncer does not await this).
    this.#pendingResponses.set(requestId, {
      resolve: (msg: PoolWorkerResult) => {
        if (msg.type === 'removeQueryResult') {
          this.#state = msg.state;
        }
      },
      reject: () => {
        /* ignored — destroy will clean up */
      },
    });
  }

  async getRow(table: string, pk: RowKey): Promise<Row | undefined> {
    const result = await this.#sendAndWait({
      type: 'getRow',
      requestId: 0,
      clientGroupID: this.#clientGroupID,
      table,
      rowKey: pk as Record<string, unknown>,
    });
    assert(
      result.type === 'getRowResult',
      `getRow: expected getRowResult, got ${result.type}`,
    );
    return result.row;
  }

  async advance(_timer: Timer): Promise<{
    version: string;
    numChanges: number;
    snapshotMs: number;
    changes: AsyncIterable<RowChange | 'yield'>;
  }> {
    assert(!this.#destroyed, 'advance: driver destroyed');
    const requestId = this.#allocRequestId();
    const stream = new AdvanceStream();
    this.#streams.set(requestId, stream);
    this.#postMessage({
      type: 'advance',
      requestId,
      clientGroupID: this.#clientGroupID,
    });
    let begin;
    try {
      begin = await stream.awaitBegin();
    } catch (e) {
      this.#streams.delete(requestId);
      throw e;
    }
    return {
      version: begin.version,
      numChanges: begin.numChanges,
      snapshotMs: begin.snapshotMs,
      changes: this.#streamAdvanceChanges(requestId, stream),
    };
  }

  async *#streamAdvanceChanges(
    requestId: number,
    stream: AdvanceStream,
  ): AsyncIterable<RowChange | 'yield'> {
    try {
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const next = await stream.pull();
        if (next.done) return;
        for (const change of next.batch) {
          yield change;
        }
      }
    } finally {
      this.#streams.delete(requestId);
    }
  }

  destroy(): void {
    if (this.#destroyed) return;
    this.#destroyed = true;
    // Post a destroyClientGroup message so the pool thread tears down its
    // PipelineDriver. Include the generation so stale destroys from an
    // earlier instance are ignored.
    const requestId = this.#allocRequestId();
    try {
      this.#postMessage({
        type: 'destroyClientGroup',
        requestId,
        clientGroupID: this.#clientGroupID,
        generation: this.#generation,
      });
    } catch (e) {
      this.#lc.warn?.(`error posting destroyClientGroup: ${String(e)}`);
    }
    // Fail any still-pending requests so their awaiters don't hang.
    this.#failAllPending(new Error('RemotePipelineDriver destroyed'));
    // Unregister from the manager so messages no longer route here.
    try {
      this.#handle.unregister(this.#clientGroupID);
    } catch (e) {
      this.#lc.warn?.(`error unregistering from pool manager: ${String(e)}`);
    }
  }

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  #allocRequestId(): number {
    return this.#nextRequestId++;
  }

  #postMessage(msg: PoolWorkerMsg): void {
    this.#handle.postMessage(msg);
  }

  #sendAndWait(msg: PoolWorkerMsg): Promise<PoolWorkerResult> {
    assert(!this.#destroyed, `${msg.type}: driver destroyed`);
    const requestId = this.#allocRequestId();
    // Clone the message with the assigned requestId. Callers pass `0`
    // as a placeholder.
    const outgoing = {...msg, requestId} as PoolWorkerMsg;
    return new Promise<PoolWorkerResult>((resolve, reject) => {
      this.#pendingResponses.set(requestId, {resolve, reject});
      try {
        this.#postMessage(outgoing);
      } catch (e) {
        this.#pendingResponses.delete(requestId);
        reject(
          e instanceof Error
            ? e
            : new Error(`postMessage failed: ${String(e)}`),
        );
      }
    });
  }

  /**
   * Called by `PoolThreadManager` when a response for this driver's
   * `clientGroupID` arrives on the pool thread's port. Do not call from
   * driver code.
   */
  handleMessage(msg: PoolWorkerResult): void {
    // Streaming advance responses are routed to the stream buffer first.
    const stream = this.#streams.get(msg.requestId);
    if (stream) {
      switch (msg.type) {
        case 'advanceBegin':
          stream.onBegin(msg.version, msg.numChanges, msg.snapshotMs);
          return;
        case 'advanceChangeBatch':
          stream.onBatch(msg.changes);
          return;
        case 'advanceComplete':
          this.#state = msg.state;
          stream.onComplete();
          return;
        case 'error':
          stream.onError(this.#toError(msg));
          return;
        default:
          // Fall through — not a stream message; treat as a one-shot response
          // that happens to share the requestId (shouldn't happen in practice).
          break;
      }
    }

    const pending = this.#pendingResponses.get(msg.requestId);
    if (!pending) {
      this.#lc.warn?.(
        `orphan response type=${msg.type} requestId=${msg.requestId}`,
      );
      return;
    }
    this.#pendingResponses.delete(msg.requestId);
    if (msg.type === 'error') {
      pending.reject(this.#toError(msg));
    } else {
      pending.resolve(msg);
    }
  }

  #toError(msg: Extract<PoolWorkerResult, {type: 'error'}>): Error {
    const err = new Error(msg.message);
    err.name = msg.name;
    if (msg.stack) {
      err.stack = msg.stack;
    }
    return err;
  }

  /**
   * Called by `PoolThreadManager` when the pool thread crashes. Fails every
   * pending request so that awaiters observe the error promptly. The driver
   * is NOT marked destroyed — the manager will restart the pool thread and
   * the next operation will trigger a fresh `init`.
   */
  onPoolThreadCrash(err: Error): void {
    this.#state = null;
    this.#failAllPending(err);
  }

  #failAllPending(err: Error): void {
    for (const [, pending] of this.#pendingResponses) {
      pending.reject(err);
    }
    this.#pendingResponses.clear();
    for (const [, stream] of this.#streams) {
      stream.onError(err);
    }
    this.#streams.clear();
  }
}
