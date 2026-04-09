/**
 * Assigns client groups to pool worker threads with sticky-per-CG semantics.
 *
 * Design rules:
 * - Once a CG is assigned to pool thread M, it stays on M for its entire
 *   lifetime. Every advance, addQuery, getRow, reset goes to the same
 *   thread. Re-hydrating a CG on a different thread would waste the
 *   IVM state (operator graph, hydrated row caches, open SQLite
 *   connections).
 * - Assignment is idempotent: calling `assign(cg)` twice returns the same
 *   index.
 * - New CGs are placed on the least-loaded thread (fewest currently-assigned
 *   CGs). Ties broken by lowest thread index.
 * - No disk persistence. On syncer restart, all pool threads are fresh
 *   workers with empty state, so preserving the mapping across restarts is
 *   pointless — affected CGs rehydrate when they next connect.
 * - On pool thread crash the syncer restarts the worker at the same index,
 *   so the mapping entries for that index remain valid (they refer to an
 *   empty driver that the next op will re-hydrate). `release()` is only
 *   called when a ViewSyncer is destroyed (TTL expiry or shutdown).
 */
export class PoolThreadMapper {
  readonly #assignments = new Map<string, number>();
  readonly #loadPerThread: number[];

  constructor(numPoolThreads: number) {
    if (numPoolThreads <= 0) {
      throw new Error(
        `PoolThreadMapper: numPoolThreads must be > 0 (got ${numPoolThreads})`,
      );
    }
    this.#loadPerThread = new Array(numPoolThreads).fill(0);
  }

  get numPoolThreads(): number {
    return this.#loadPerThread.length;
  }

  /**
   * Returns the pool thread index for `clientGroupID`. Assigns a thread on
   * the first call; subsequent calls return the same index.
   */
  assign(clientGroupID: string): number {
    const existing = this.#assignments.get(clientGroupID);
    if (existing !== undefined) {
      return existing;
    }
    let minIdx = 0;
    for (let i = 1; i < this.#loadPerThread.length; i++) {
      if (this.#loadPerThread[i] < this.#loadPerThread[minIdx]) {
        minIdx = i;
      }
    }
    this.#assignments.set(clientGroupID, minIdx);
    this.#loadPerThread[minIdx]++;
    return minIdx;
  }

  /**
   * Returns the pool thread index previously assigned to `clientGroupID`,
   * or `undefined` if the CG has no assignment.
   */
  get(clientGroupID: string): number | undefined {
    return this.#assignments.get(clientGroupID);
  }

  /**
   * Removes the assignment for `clientGroupID` and decrements the thread's
   * load counter. Returns the released index, or `undefined` if there was
   * no assignment.
   *
   * Call this when the ViewSyncer for this CG is destroyed (TTL expiry or
   * syncer shutdown), so a later `assign` for a new CG can consider this
   * thread for load-balancing.
   */
  release(clientGroupID: string): number | undefined {
    const idx = this.#assignments.get(clientGroupID);
    if (idx === undefined) {
      return undefined;
    }
    this.#assignments.delete(clientGroupID);
    this.#loadPerThread[idx]--;
    return idx;
  }

  /**
   * Returns a snapshot of current load per thread. Used for logging and
   * diagnostics.
   */
  loadSnapshot(): readonly number[] {
    return [...this.#loadPerThread];
  }

  /**
   * Returns the list of client groups currently assigned to a given pool
   * thread. Used for crash recovery: when pool thread M crashes, the caller
   * iterates this list to mark every affected CG as "needs re-init".
   */
  clientGroupsOn(poolThreadIdx: number): string[] {
    const result: string[] = [];
    for (const [cg, idx] of this.#assignments) {
      if (idx === poolThreadIdx) {
        result.push(cg);
      }
    }
    return result;
  }
}
