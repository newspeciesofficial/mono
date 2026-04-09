/**
 * Lifecycle and message routing for the pool worker threads owned by one
 * syncer process.
 *
 * Responsibilities:
 * - Spawn N worker threads at startup, each given its own `MessageChannel`
 *   (Decision D2 in docs/pool-threads-v3-architecture.md). The worker
 *   receives one end via `workerData.port`; the syncer keeps the other.
 * - Own the single `message` / `messageerror` listener on each syncer-side
 *   port. Dispatch incoming responses to the right `RemotePipelineDriver`
 *   by `clientGroupID`.
 * - Expose `PoolThreadHandle` instances to `RemotePipelineDriver`s so they
 *   can post messages without knowing about workers/ports.
 * - On worker exit/error: log, restart at the same index with a fresh
 *   `MessageChannel`, and notify every driver registered on that index via
 *   `onPoolThreadCrash()` so their pending requests fail cleanly. Drivers
 *   remain assigned to the restarted thread (via `PoolThreadMapper`) and
 *   will rebuild their IVM state on the next operation.
 * - On shutdown: post `shutdown` to every worker and terminate after a
 *   grace period if it does not exit.
 */

import type {LogContext} from '@rocicorp/logger';
import {MessageChannel, Worker, type MessagePort} from 'node:worker_threads';
import {assert} from '../../../../shared/src/asserts.ts';
import type {LogConfig} from '../../config/zero-config.ts';
import type {ShardID} from '../../types/shards.ts';
import type {
  PoolWorkerMsg,
  PoolWorkerResult,
} from '../../workers/pool-protocol.ts';
import type {
  PoolThreadHandle,
  RemotePipelineDriver,
} from './remote-pipeline-driver.ts';

export type PoolThreadManagerOptions = {
  readonly lc: LogContext;
  readonly numPoolThreads: number;
  readonly workerURL: URL | string;
  readonly replicaFile: string;
  readonly shardID: ShardID;
  readonly logConfig: LogConfig;
  readonly yieldThresholdMs: number;
  readonly enableQueryPlanner: boolean | undefined;
};

type ThreadEntry = {
  idx: number;
  worker: Worker;
  /** Syncer-side end of the MessageChannel. */
  port: MessagePort;
  /** clientGroupID → registered driver on this thread. */
  drivers: Map<string, RemotePipelineDriver>;
};

export class PoolThreadManager {
  readonly #lc: LogContext;
  readonly #options: PoolThreadManagerOptions;
  readonly #threads: ThreadEntry[] = [];
  #shuttingDown = false;

  constructor(options: PoolThreadManagerOptions) {
    assert(
      options.numPoolThreads > 0,
      `PoolThreadManager requires numPoolThreads > 0 (got ${options.numPoolThreads})`,
    );
    this.#lc = options.lc.withContext('component', 'pool-thread-manager');
    this.#options = options;
    for (let i = 0; i < options.numPoolThreads; i++) {
      this.#threads.push(this.#spawnThread(i));
    }
    this.#lc.info?.(
      `spawned ${options.numPoolThreads} pool worker threads for IVM`,
    );
  }

  get numPoolThreads(): number {
    return this.#threads.length;
  }

  /**
   * Returns a handle that a `RemotePipelineDriver` uses to post messages
   * to its assigned pool thread. The driver must call
   * `registerDriver(idx, cgID, driver)` before expecting to receive any
   * responses — done via `handle.register` below.
   */
  getHandle(poolThreadIdx: number): PoolThreadHandle {
    assert(
      poolThreadIdx >= 0 && poolThreadIdx < this.#threads.length,
      `poolThreadIdx out of range: ${poolThreadIdx}`,
    );
    const manager = this;
    return {
      get poolThreadIdx() {
        return poolThreadIdx;
      },
      postMessage(msg: PoolWorkerMsg) {
        manager.#postMessage(poolThreadIdx, msg);
      },
      unregister(clientGroupID: string) {
        manager.unregisterDriver(poolThreadIdx, clientGroupID);
      },
    };
  }

  /**
   * Register a driver as the recipient for responses carrying its
   * `clientGroupID` on the given thread. Call this once, as part of the
   * driver's construction.
   */
  registerDriver(
    poolThreadIdx: number,
    clientGroupID: string,
    driver: RemotePipelineDriver,
  ): void {
    const thread = this.#threads[poolThreadIdx];
    assert(thread, `no pool thread at index ${poolThreadIdx}`);
    const existing = thread.drivers.get(clientGroupID);
    if (existing && existing !== driver) {
      // The previous driver for this CG (e.g. from a TTL-expired ViewSyncer
      // that has not yet been destroyed) is superseded. Its pending
      // requests will fail with a destroy error when its own destroy()
      // runs; we simply swap the map entry so new responses route to the
      // new driver.
      this.#lc.info?.(
        `replacing driver for cg=${clientGroupID} on thread=${poolThreadIdx}`,
      );
    }
    thread.drivers.set(clientGroupID, driver);
  }

  unregisterDriver(poolThreadIdx: number, clientGroupID: string): void {
    const thread = this.#threads[poolThreadIdx];
    if (!thread) return;
    thread.drivers.delete(clientGroupID);
  }

  /**
   * Graceful shutdown: post `shutdown` to every worker and wait for them
   * to exit. Called from the syncer's `runUntilKilled` lifecycle.
   */
  async shutdown(): Promise<void> {
    if (this.#shuttingDown) return;
    this.#shuttingDown = true;
    this.#lc.info?.(`shutting down ${this.#threads.length} pool threads`);
    const exits = this.#threads.map(t => {
      try {
        t.port.postMessage({type: 'shutdown', requestId: 0});
      } catch (e) {
        this.#lc.warn?.(
          `error posting shutdown to thread ${t.idx}: ${String(e)}`,
        );
      }
      return new Promise<void>(resolve => {
        const onExit = () => resolve();
        t.worker.once('exit', onExit);
        // Safety net: if the worker does not exit in time, terminate.
        setTimeout(() => {
          t.worker.off('exit', onExit);
          t.worker
            .terminate()
            .catch(e =>
              this.#lc.warn?.(
                `terminate error on thread ${t.idx}: ${String(e)}`,
              ),
            )
            .finally(() => resolve());
        }, 5000).unref();
      });
    });
    await Promise.all(exits);
  }

  // --------------------------------------------------------------------
  // Internals
  // --------------------------------------------------------------------

  #postMessage(poolThreadIdx: number, msg: PoolWorkerMsg): void {
    const thread = this.#threads[poolThreadIdx];
    assert(thread, `no pool thread at index ${poolThreadIdx}`);
    thread.port.postMessage(msg);
  }

  #spawnThread(idx: number): ThreadEntry {
    const channel = new MessageChannel();
    const syncerPort = channel.port1;
    const workerPort = channel.port2;

    const worker = new Worker(this.#options.workerURL, {
      workerData: {
        port: workerPort,
        poolThreadIdx: idx,
        replicaFile: this.#options.replicaFile,
        shardID: this.#options.shardID,
        logConfig: this.#options.logConfig,
        yieldThresholdMs: this.#options.yieldThresholdMs,
        enableQueryPlanner: this.#options.enableQueryPlanner,
      },
      transferList: [workerPort],
    });

    const entry: ThreadEntry = {
      idx,
      worker,
      port: syncerPort,
      drivers: new Map(),
    };

    syncerPort.on('message', (msg: PoolWorkerResult) =>
      this.#dispatch(entry, msg),
    );
    syncerPort.on('messageerror', err =>
      this.#handlePortError(
        entry,
        err instanceof Error ? err : new Error(String(err)),
      ),
    );
    worker.on('error', err => this.#handleWorkerError(entry, err));
    worker.on('exit', code => this.#handleWorkerExit(entry, code));

    this.#lc.info?.(`pool thread ${idx} spawned (threadId=${worker.threadId})`);
    return entry;
  }

  #dispatch(entry: ThreadEntry, msg: PoolWorkerResult): void {
    const cgID = msg.clientGroupID;
    const driver = cgID ? entry.drivers.get(cgID) : undefined;
    if (!driver) {
      // Late response for a CG that has been unregistered — e.g. a
      // destroyClientGroupResult arriving after the driver cleaned up.
      // This is expected and safe to ignore.
      if (msg.type !== 'destroyClientGroupResult') {
        this.#lc.debug?.(
          `dropping orphan response type=${msg.type} cg=${cgID} ` +
            `thread=${entry.idx}`,
        );
      }
      return;
    }
    driver.handleMessage(msg);
  }

  #handlePortError(entry: ThreadEntry, err: Error): void {
    this.#lc.error?.(
      `messageerror on pool thread ${entry.idx}: ${err.message}`,
    );
    this.#failDrivers(entry, err);
  }

  #handleWorkerError(entry: ThreadEntry, err: Error): void {
    this.#lc.error?.(`error event on pool thread ${entry.idx}: ${err.message}`);
    this.#failDrivers(entry, err);
  }

  #handleWorkerExit(entry: ThreadEntry, code: number): void {
    if (this.#shuttingDown) {
      this.#lc.info?.(
        `pool thread ${entry.idx} exited code=${code} during shutdown`,
      );
      return;
    }
    this.#lc.error?.(
      `pool thread ${entry.idx} exited unexpectedly code=${code}, restarting`,
    );
    const err = new Error(
      `pool thread ${entry.idx} exited unexpectedly (code=${code})`,
    );
    // Notify registered drivers of the crash. The drivers stay registered
    // on this thread index (via PoolThreadMapper) so their next operation
    // triggers a fresh init on the restarted worker.
    this.#failDrivers(entry, err);
    // Close the dead port — listeners are removed automatically when the
    // channel is garbage collected after close.
    try {
      entry.port.close();
    } catch (e) {
      this.#lc.warn?.(
        `error closing port after crash on thread ${entry.idx}: ${String(e)}`,
      );
    }
    // Replace the thread entry in place at the same index.
    const fresh = this.#spawnThread(entry.idx);
    // Carry forward the driver map — every driver assigned here needs to
    // receive messages on the new port too. Their cached #state has been
    // cleared by onPoolThreadCrash; they will call init() again on next
    // use.
    fresh.drivers = entry.drivers;
    this.#threads[entry.idx] = fresh;
  }

  #failDrivers(entry: ThreadEntry, err: Error): void {
    for (const driver of entry.drivers.values()) {
      try {
        driver.onPoolThreadCrash(err);
      } catch (e) {
        this.#lc.warn?.(`error notifying driver of crash: ${String(e)}`);
      }
    }
  }
}
