/**
 * Rust-backed `PipelineDriver` implementation.
 *
 * Mirrors the public surface of {@link PipelineDriver} but delegates the
 * IVM-touching methods (init, addQuery, advance, …) to the Rust port via
 * shadow-ffi. Methods that exist for TS-side concerns only (auth-permission
 * reload) stay in TS.
 *
 * ## When to use
 *
 * This is gated by `ZERO_USE_RUST_IVM=1`. The TS path remains the default
 * until the Rust path has been validated under real load.
 *
 * ## Architecture
 *
 * - Rust owns its own `Snapshotter` + `TableSource` connections to the
 *   replica file. WAL2 makes the parallel TS-side `Snapshotter` (kept for
 *   `currentPermissions`) safe.
 * - All `addQuery` / `advance` calls translate to one synchronous napi call
 *   each. Returned `Vec<RowChange>` is wrapped in a generator so callers
 *   that `for await … of` continue to work without modification.
 * - The TS yield markers (`'yield'`) used for cooperative scheduling are
 *   not emitted from the Rust path. Rust executes synchronously to
 *   completion. ViewSyncer's `for await` loops simply observe no yields.
 * - The `Timer` arg is accepted for signature parity but unused — Rust
 *   doesn't time-slice yet.
 *
 * ## Out of scope (TODO before declaring parity)
 *
 * - Live companion-pipeline reactivity for scalar subqueries
 *   (`#resolveScalarSubqueries` is invoked once at addQuery; subsequent
 *   updates that change a subquery's value won't trigger the
 *   `ResetPipelinesSignal` that the TS impl raises).
 * - `MeasurePushOperator` instrumentation (TS observability only).
 * - `inspectorDelegate` query-update hooks.
 */

import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {ClientGroupStorage} from '../../../../zqlite/src/database-storage.ts';
import {
  reloadPermissionsIfChanged,
  type LoadedPermissions,
} from '../../auth/load-permissions.ts';
import type {LogConfig, ZeroConfig} from '../../config/zero-config.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {loadShadowNative, type ShadowNative} from '../../shadow/native.ts';
import type {RowKey} from '../../types/row-key.ts';
import type {ShardID} from '../../types/shards.ts';
import type {RowChange, Timer} from './pipeline-driver.ts';
import type {Snapshotter} from './snapshotter.ts';

type QueryInfo = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
};

type PipelineMeta = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
  hydrationTimeMs: number;
};

/**
 * Same constructor shape as `PipelineDriver`. The arg list is preserved
 * verbatim so `syncer.ts` can choose between this and the TS implementation
 * with a one-line ternary.
 */
export class RustPipelineDriver {
  readonly #lc: LogContext;
  readonly #snapshotter: Snapshotter;
  readonly #shardID: ShardID;
  readonly #storage: ClientGroupStorage;
  readonly #config: ZeroConfig | undefined;
  readonly #native: ShadowNative;
  readonly #handle: ReturnType<ShadowNative['pipeline_driver_create']>;
  readonly #pipelines = new Map<string, PipelineMeta>();
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();

  #permissions: LoadedPermissions | null = null;
  #replicaVersion: string | null = null;
  #destroyed = false;

  constructor(
    lc: LogContext,
    _logConfig: LogConfig,
    snapshotter: Snapshotter,
    shardID: ShardID,
    storage: ClientGroupStorage,
    clientGroupID: string,
    _inspectorDelegate: InspectorDelegate,
    yieldThresholdMs: () => number,
    enablePlanner?: boolean | undefined,
    config?: ZeroConfig | undefined,
  ) {
    this.#lc = lc.withContext('clientGroupID', clientGroupID);
    this.#snapshotter = snapshotter;
    this.#shardID = shardID;
    this.#storage = storage;
    this.#config = config;
    this.#native = loadShadowNative();

    // Rust owns its own Snapshotter inside the handle. Pull the replica
    // path off the TS Snapshotter so both sides open the same file.
    const replicaPath = snapshotter.dbFile;
    this.#handle = this.#native.pipeline_driver_create(
      clientGroupID,
      replicaPath,
      shardID.appID,
      shardID.shardNum,
      yieldThresholdMs(),
      enablePlanner ?? false,
    );
  }

  init(clientSchema: ClientSchema): void {
    assert(!this.initialized(), 'Already initialized');
    // TS Snapshotter init for currentPermissions / version reads.
    this.#snapshotter.init();
    this.#refreshTableSpecs();
    const {tableSpecs, fullTables} = this.#deriveRustInitArgs();
    this.#native.pipeline_driver_init(
      this.#handle,
      // Rust expects the strict ClientSchema shape that
      // {tables: Record<string, {columns, primaryKey}>} satisfies. The TS
      // `ClientSchema` type from zero-protocol matches this shape exactly.
      clientSchema as unknown,
      tableSpecs,
      fullTables,
    );
    this.#replicaVersion = this.#native.pipeline_driver_replica_version(
      this.#handle,
    );
  }

  initialized(): boolean {
    return this.#native.pipeline_driver_initialized(this.#handle);
  }

  reset(clientSchema: ClientSchema): void {
    this.#refreshTableSpecs();
    const {tableSpecs, fullTables} = this.#deriveRustInitArgs();
    this.#native.pipeline_driver_reset(
      this.#handle,
      clientSchema as unknown,
      tableSpecs,
      fullTables,
    );
    this.#pipelines.clear();
    this.#replicaVersion = this.#native.pipeline_driver_replica_version(
      this.#handle,
    );
  }

  get replicaVersion(): string {
    return must(this.#replicaVersion, 'Not yet initialized');
  }

  currentVersion(): string {
    return this.#native.pipeline_driver_current_version(this.#handle);
  }

  /**
   * Reads the current effective permissions. This is a TS concern (auth
   * system integration) — Rust stays out of it. Uses the TS-owned
   * Snapshotter so the WAL2 read snapshot is consistent with what the
   * other PipelineDriver methods see.
   */
  currentPermissions(): LoadedPermissions | null {
    assert(this.initialized(), 'Not yet initialized');
    const res = reloadPermissionsIfChanged(
      this.#lc,
      this.#snapshotter.current().db,
      this.#shardID.appID,
      this.#permissions,
      this.#config,
    );
    if (res.changed) {
      this.#permissions = res.permissions;
    }
    return this.#permissions;
  }

  advanceWithoutDiff(): string {
    return this.#native.pipeline_driver_advance_without_diff(this.#handle);
  }

  /**
   * Hydrates a query. Returns an iterable of `RowChange`. The TS-side
   * `'yield'` cooperative-scheduling marker is not emitted — Rust runs
   * synchronously to completion in this thread. ViewSyncer's loops keep
   * working because the `for … of` falls through cleanly with no yields.
   */
  *addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    _timer: Timer,
  ): Iterable<RowChange | 'yield'> {
    assert(this.initialized(), 'Not yet initialized');
    const t0 = performance.now();
    const changes = this.#native.pipeline_driver_add_query(
      this.#handle,
      transformationHash,
      queryID,
      query,
      {elapsedLapMs: 0, totalElapsedMs: 0},
    );
    const ms = performance.now() - t0;
    this.#pipelines.set(queryID, {
      transformedAst: query,
      transformationHash,
      hydrationTimeMs: ms,
    });
    for (const c of changes) {
      // Skip the {type: 'yield'} marker — TS callers don't need it on
      // the Rust path (Rust ran synchronously).
      if (c.type === 'yield') continue;
      yield c as RowChange;
    }
  }

  /**
   * Batched parallel hydration. Each query is hydrated on its own rayon
   * worker in Rust; chunks stream back via `onChunk` as soon as each
   * worker finishes. The returned promise resolves once every worker
   * is done with one status per query (input order preserved).
   *
   * Use this in place of looping `addQuery()` per query when caller
   * has multiple queries to hydrate at once (e.g. ViewSyncer's
   * `#hydrateUnchangedQueries` batch).
   */
  addQueries(
    batch: ReadonlyArray<{
      transformationHash: string;
      queryID: string;
      ast: AST;
    }>,
    onChunk: (chunk: {
      queryID: string;
      rows: Iterable<RowChange | 'yield'>;
    }) => void,
  ): Array<
    | {queryID: string; ok: true; hydrationTimeMs: number}
    | {queryID: string; ok: false; error: string}
  > {
    assert(this.initialized(), 'Not yet initialized');
    const reqs = batch.map(b => ({
      transformationHash: b.transformationHash,
      queryID: b.queryID,
      ast: b.ast,
    }));
    const results = this.#native.pipeline_driver_add_queries_parallel(
      this.#handle,
      reqs,
      (err, chunk) => {
        if (err) {
          this.#lc.error?.('add_queries_parallel chunk callback error', err);
          return;
        }
        // Strip yield markers — the Rust path runs synchronously per
        // worker so cooperative-scheduling yields aren't meaningful here.
        const rows = chunk.rows.filter(r => r.type !== 'yield') as RowChange[];
        onChunk({queryID: chunk.queryID, rows});
      },
    );
    // Update local pipeline metadata for any successful queries so
    // queries() / totalHydrationTimeMs() reflect the new batch.
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.ok) {
        this.#pipelines.set(r.queryID, {
          transformedAst: batch[i].ast,
          transformationHash: batch[i].transformationHash,
          hydrationTimeMs: r.hydrationTimeMs,
        });
      }
    }
    return results;
  }

  removeQuery(queryID: string): void {
    if (!this.#pipelines.has(queryID)) return;
    this.#native.pipeline_driver_remove_query(this.#handle, queryID);
    this.#pipelines.delete(queryID);
  }

  getRow(table: string, pk: RowKey): Row | undefined {
    const v = this.#native.pipeline_driver_get_row(this.#handle, table, pk);
    if (v == null) return undefined;
    return v as Row;
  }

  advance(_timer: Timer): {
    version: string;
    numChanges: number;
    snapshotMs: number;
    changes: Iterable<RowChange | 'yield'>;
  } {
    const t0 = performance.now();
    const res = this.#native.pipeline_driver_advance(this.#handle, {
      elapsedLapMs: 0,
      totalElapsedMs: 0,
    });
    const snapshotMs = performance.now() - t0;
    return {
      version: res.version,
      numChanges: res.numChanges,
      snapshotMs,
      changes: this.#stripYields(res.changes),
    };
  }

  *#stripYields(arr: readonly {type: string}[]): Iterable<RowChange | 'yield'> {
    for (const c of arr) {
      if (c.type === 'yield') continue;
      yield c as RowChange;
    }
  }

  queries(): ReadonlyMap<string, QueryInfo> {
    return this.#pipelines;
  }

  totalHydrationTimeMs(): number {
    let total = 0;
    for (const p of this.#pipelines.values()) total += p.hydrationTimeMs;
    return total;
  }

  hydrationBudgetBreakdown(): {id: string; table: string; ms: number}[] {
    const out = this.#native.pipeline_driver_hydration_budget_breakdown(
      this.#handle,
    );
    return out as {id: string; table: string; ms: number}[];
  }

  destroy(): void {
    if (this.#destroyed) return;
    this.#destroyed = true;
    try {
      this.#native.pipeline_driver_destroy(this.#handle);
    } finally {
      this.#storage.destroy();
      this.#snapshotter.destroy();
    }
  }

  #refreshTableSpecs(): void {
    const {db} = this.#snapshotter.current();
    const fullTables = new Map<string, LiteTableSpec>();
    computeZqlSpecs(
      this.#lc,
      db.db,
      {includeBackfillingColumns: false},
      this.#tableSpecs,
      fullTables,
    );
  }

  #deriveRustInitArgs(): {
    tableSpecs: Record<string, unknown>;
    fullTables: Record<string, unknown>;
  } {
    const tableSpecs: Record<string, unknown> = {};
    for (const [name, spec] of this.#tableSpecs.entries()) {
      tableSpecs[name] = {
        allPotentialPrimaryKeys: spec.tableSpec.allPotentialPrimaryKeys ?? [
          spec.tableSpec.primaryKey,
        ],
        zqlSpec: spec.zqlSpec,
      };
    }

    // Mirror what computeZqlSpecs put in fullTables — Rust expects
    // {table: {columns: <colSpec map>}} for each table.
    const fullTables: Record<string, unknown> = {};
    const fullTablesMap = new Map<string, LiteTableSpec>();
    computeZqlSpecs(
      this.#lc,
      this.#snapshotter.current().db.db,
      {includeBackfillingColumns: false},
      new Map(),
      fullTablesMap,
    );
    for (const [name, spec] of fullTablesMap.entries()) {
      fullTables[name] = {columns: spec.columns};
    }
    return {tableSpecs, fullTables};
  }
}
