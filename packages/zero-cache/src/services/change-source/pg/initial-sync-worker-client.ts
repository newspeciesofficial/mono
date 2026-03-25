import {resolver, type Resolver} from '@rocicorp/resolver';
import {Worker} from 'node:worker_threads';
import type {JSONObject} from '../../../../../shared/src/bigint-json.ts';
import type {LogConfig} from '../../../../../shared/src/logging.ts';
import type {IndexSpec, PublishedTableSpec} from '../../../db/specs.ts';
import {INITIAL_SYNC_WORKER_URL} from '../../../server/worker-urls.ts';
import type {
  FinalizeTableResult,
  InitSyncWriter,
  InitTableColumn,
  WriteChunkResult,
} from './initial-sync.ts';
import type {
  InitSyncError,
  InitSyncMethod,
  InitSyncResponse,
  InitSyncResultMap,
} from './initial-sync-worker.ts';

/**
 * Main-thread client that implements {@link InitSyncWriter} by delegating
 * all SQLite operations to a worker thread via `postMessage`.
 *
 * Uses request-ID multiplexed RPC to support concurrent table copies.
 */
export class InitSyncWorkerClient implements InitSyncWriter {
  readonly #worker: Worker;
  readonly #pending = new Map<number, Resolver<unknown, Error>>();
  #nextId = 0;
  #terminated = false;
  #fatalError: Error | undefined;

  constructor() {
    this.#worker = new Worker(INITIAL_SYNC_WORKER_URL);

    this.#worker.on('message', (msg: InitSyncResponse | InitSyncError) => {
      if ('initSyncError' in msg) {
        const error =
          msg.initSyncError instanceof Error
            ? msg.initSyncError
            : new Error(String(msg.initSyncError));
        this.#fatalError = error;
        this.#rejectAll(error);
        return;
      }
      const r = this.#pending.get(msg.id);
      if (!r) return;
      this.#pending.delete(msg.id);
      if (msg.error !== undefined) {
        r.reject(
          msg.error instanceof Error ? msg.error : new Error(String(msg.error)),
        );
      } else {
        r.resolve(msg.result);
      }
    });

    this.#worker.on('error', (err: Error) => {
      this.#fatalError = err;
      this.#rejectAll(err);
    });

    this.#worker.on('exit', (code: number) => {
      this.#terminated = true;
      if (code !== 0) {
        const err = new Error(`Initial sync worker exited with code ${code}`);
        this.#fatalError = err;
        this.#rejectAll(err);
      }
    });
  }

  #rejectAll(err: Error) {
    for (const r of this.#pending.values()) {
      r.reject(err);
    }
    this.#pending.clear();
  }

  #call<M extends InitSyncMethod>(
    method: M,
    args: unknown[],
  ): Promise<InitSyncResultMap[M]> {
    if (this.#fatalError) {
      return Promise.reject(this.#fatalError);
    }
    const id = this.#nextId++;
    const r = resolver<InitSyncResultMap[M]>();
    this.#pending.set(id, r as Resolver<unknown, Error>);
    this.#worker.postMessage({id, method, args});
    return r.promise;
  }

  init(
    dbPath: string,
    debugName: string,
    upstreamURI: string,
    logConfig: LogConfig,
  ): Promise<void> {
    return this.#call('init', [dbPath, debugName, upstreamURI, logConfig]);
  }

  createTables(
    tables: PublishedTableSpec[],
    initialVersion: string,
  ): Promise<void> {
    return this.#call('createTables', [tables, initialVersion]);
  }

  initReplicationState(
    publications: string[],
    watermark: string,
    context: JSONObject,
  ): Promise<void> {
    return this.#call('initReplicationState', [
      publications,
      watermark,
      context,
    ]);
  }

  initTable(tableName: string, columns: InitTableColumn[]): Promise<void> {
    return this.#call('initTable', [tableName, columns]);
  }

  writeChunk(tableName: string, chunk: Buffer): Promise<WriteChunkResult> {
    return this.#call('writeChunk', [tableName, chunk]);
  }

  finalizeTable(tableName: string): Promise<FinalizeTableResult> {
    return this.#call('finalizeTable', [tableName]);
  }

  createIndexes(indexes: IndexSpec[]): Promise<void> {
    return this.#call('createIndexes', [indexes]);
  }

  done(): Promise<void> {
    return this.#call('done', []);
  }

  abort(): void {
    if (!this.#terminated) {
      // Fire-and-forget — no pending resolver for abort.
      this.#worker.postMessage({
        id: this.#nextId++,
        method: 'abort',
        args: [],
      });
    }
  }

  async terminate(): Promise<void> {
    if (!this.#terminated) {
      await this.#worker.terminate();
    }
  }
}
