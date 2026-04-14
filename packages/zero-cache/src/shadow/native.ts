/**
 * Loader for the `zero-cache-shadow-ffi` native module.
 *
 * Lives behind a lazy import so absent / mis-built artifacts produce a
 * single clear error message at first use rather than crashing the worker
 * at module load. The framework is opt-in (env-gated by callers); when the
 * binding is missing, shadow comparisons are silently skipped — production
 * keeps running.
 */

import {createRequire} from 'node:module';

const require_ = createRequire(import.meta.url);

export type ShadowNative = {
  /**
   * Pure-function shadows. Match the corresponding TS export exactly in
   * argument count, types and thrown-error messages.
   */
  lsn_to_big_int(lsn: string): bigint;
  lsn_from_big_int(val: bigint): string;

  /** Shadow of `packages/zql/src/ivm/data.ts:compareValues`. Throws on type mismatch (matches TS). */
  ivm_data_compare_values(a: unknown, b: unknown): number;
  /** Variant that accepts `null` to denote `undefined`. */
  ivm_data_compare_values_opt(
    a: unknown | null | undefined,
    b: unknown | null | undefined,
  ): number;
  /** Shadow of `valuesEqual`. */
  ivm_data_values_equal(
    a: unknown | null | undefined,
    b: unknown | null | undefined,
  ): boolean;
  /** Shadow of `normalizeUndefined` — returns `null` for both `null` and `undefined`. */
  ivm_data_normalize_undefined(v: unknown | null | undefined): unknown;
  /**
   * Shadow of `makeComparator(order, reverse)(a, b)` — eagerly evaluated.
   * `order` matches TS `Ordering` shape: `[[field, 'asc'|'desc'], …]`.
   */
  ivm_data_compare_rows(
    order: Array<[string, 'asc' | 'desc']>,
    reverse: boolean,
    a: Record<string, unknown>,
    b: Record<string, unknown>,
  ): number;

  /** Shadow of `ivm/constraint.ts:constraintMatchesRow`. */
  ivm_constraint_matches_row(
    constraint: Record<string, unknown>,
    row: Record<string, unknown>,
  ): boolean;
  /** Shadow of `ivm/constraint.ts:constraintsAreCompatible`. */
  ivm_constraints_are_compatible(
    left: Record<string, unknown>,
    right: Record<string, unknown>,
  ): boolean;
  /** Shadow of `ivm/constraint.ts:constraintMatchesPrimaryKey`. */
  ivm_constraint_matches_primary_key(
    constraint: Record<string, unknown>,
    primary: string[],
  ): boolean;
  /** Shadow of `ivm/constraint.ts:keyMatchesPrimaryKey`. */
  ivm_key_matches_primary_key(key: string[], primary: string[]): boolean;
  /** Shadow of `ivm/constraint.ts:pullSimpleAndComponents`. */
  ivm_pull_simple_and_components(condition: unknown): unknown;
  /** Shadow of `ivm/constraint.ts:primaryKeyConstraintFromFilters`. */
  ivm_primary_key_constraint_from_filters(
    condition: unknown | null | undefined,
    primary: string[],
  ): unknown | null;

  // ─── Stateful: PipelineDriver (Layer 11) ───────────────────────────
  //
  // The `External<…>` handles are opaque to TS. Treat them as branded
  // tokens that only round-trip through these functions.

  /** Shadow of the TS `PipelineDriver` constructor. */
  pipeline_driver_create(
    clientGroupId: string,
    replicaPath: string,
    appId: string,
    shardNum: number,
    defaultYieldEveryMs: number,
    enablePlanner: boolean,
  ): PipelineDriverHandle;
  /** Shadow of `PipelineDriver.init(clientSchema)` + the table/spec context the Rust port needs. */
  pipeline_driver_init(
    driver: PipelineDriverHandle,
    clientSchema: unknown,
    tableSpecs: unknown,
    fullTables: unknown,
  ): void;
  /** Shadow of `PipelineDriver.initialized()`. */
  pipeline_driver_initialized(driver: PipelineDriverHandle): boolean;
  /** Shadow of `PipelineDriver.replicaVersion`. */
  pipeline_driver_replica_version(driver: PipelineDriverHandle): string;
  /** Shadow of `PipelineDriver.currentVersion()`. */
  pipeline_driver_current_version(driver: PipelineDriverHandle): string;
  /** Shadow of `PipelineDriver.reset(clientSchema)`. */
  pipeline_driver_reset(
    driver: PipelineDriverHandle,
    clientSchema: unknown,
    tableSpecs: unknown,
    fullTables: unknown,
  ): void;
  /** Shadow of `PipelineDriver.destroy()`. */
  pipeline_driver_destroy(driver: PipelineDriverHandle): void;
  /**
   * Shadow of `PipelineDriver.addQuery(transformationHash, queryID, ast,
   * timer)` — returns an eagerly-collected `Array<RowChange | {type: 'yield'}>`.
   */
  pipeline_driver_add_query(
    driver: PipelineDriverHandle,
    transformationHash: string,
    queryId: string,
    ast: unknown,
    timer?: {elapsedLapMs?: number; totalElapsedMs?: number} | null,
  ): ShadowRowChange[];
  /** Shadow of `PipelineDriver.removeQuery(queryId)`. */
  pipeline_driver_remove_query(
    driver: PipelineDriverHandle,
    queryId: string,
  ): void;
  /** Shadow of `PipelineDriver.advance(timer)`. */
  pipeline_driver_advance(
    driver: PipelineDriverHandle,
    timer?: {elapsedLapMs?: number; totalElapsedMs?: number} | null,
  ): {version: string; numChanges: number; changes: ShadowRowChange[]};
  /** Shadow of `PipelineDriver.advanceWithoutDiff()`. */
  pipeline_driver_advance_without_diff(driver: PipelineDriverHandle): string;
  /** Shadow of `PipelineDriver.queries()` — collapsed to an array for wire diffs. */
  pipeline_driver_queries(driver: PipelineDriverHandle): Array<{
    queryId: string;
    table: string;
    transformationHash: string;
  }>;
  /** Shadow of `PipelineDriver.totalHydrationTimeMs()`. */
  pipeline_driver_total_hydration_time_ms(driver: PipelineDriverHandle): number;
  /** Shadow of `PipelineDriver.hydrationBudgetBreakdown()`. */
  pipeline_driver_hydration_budget_breakdown(
    driver: PipelineDriverHandle,
  ): Array<{id: string; table: string; ms: number}>;
  /**
   * Shadow of `PipelineDriver.getRow(table, pk)`. Returns the row as an
   * object, or `null` if no such row exists.
   */
  pipeline_driver_get_row(
    driver: PipelineDriverHandle,
    table: string,
    pk: Record<string, unknown>,
  ): Record<string, unknown> | null;
  /**
   * Register an externally-constructed `TableSource` on the driver. The
   * Rust `PipelineDriver` does not build its own `TableSource` (the
   * snapshotter owns the Connection); callers supply the source via this
   * seam.
   */
  pipeline_driver_register_table_source(
    driver: PipelineDriverHandle,
    tableName: string,
    source: TableSourceHandle,
  ): void;

  /**
   * Shadow factory: open a Rust-side `rusqlite::Connection` on
   * `replicaPath` and wrap it in a `TableSource`. TS keeps its own
   * `better-sqlite3` handle on the same file; WAL handles concurrency.
   */
  table_source_create(
    replicaPath: string,
    tableName: string,
    primaryKey: string[],
    columns: Record<string, {type: string; optional?: boolean}>,
  ): TableSourceHandle;
  /**
   * Shadow factory variant backed by an in-memory SQLite — for diff
   * tests that don't want a replica file on disk. Caller supplies the
   * DDL + any seed inserts.
   */
  table_source_create_in_memory(
    tableName: string,
    primaryKey: string[],
    columns: Record<string, {type: string; optional?: boolean}>,
    createTableSql: string,
    seedSql: string[],
  ): TableSourceHandle;
};

/** Opaque handle to a Rust-side `PipelineDriver`. */
export type PipelineDriverHandle = {readonly __brand: 'PipelineDriverHandle'};

/** Opaque handle to a Rust-side `TableSource`. */
export type TableSourceHandle = {readonly __brand: 'TableSourceHandle'};

/** Wire shape for a single row change coming out of the Rust shadow. */
export type ShadowRowChange =
  | {
      type: 'add';
      queryID: string;
      table: string;
      rowKey: Record<string, unknown>;
      row: Record<string, unknown>;
    }
  | {
      type: 'remove';
      queryID: string;
      table: string;
      rowKey: Record<string, unknown>;
      row: null;
    }
  | {
      type: 'edit';
      queryID: string;
      table: string;
      rowKey: Record<string, unknown>;
      row: Record<string, unknown>;
    }
  | {type: 'yield'};

let cached: ShadowNative | undefined | null;

/**
 * Returns the loaded native module, or `undefined` if unavailable. Failure
 * to load is logged once; subsequent calls return `undefined` without
 * re-attempting.
 */
export function tryLoadShadowNative(): ShadowNative | undefined {
  if (cached !== undefined) return cached ?? undefined;
  // Candidate require paths, tried in order:
  //   1. Bundled location inside the published @rocicorp/zero package
  //      (`out/zero-cache-shadow-ffi/index.js`). This is what the Docker
  //      image / tarball consumers hit.
  //   2. In-repo dev path, relative to this source file.
  const candidates = [
    // Bundled: out/zero-cache-shadow-ffi/index.js lives next to the built
    // zero-cache output under `out/`. From this file (when compiled to
    // `out/zero-cache/src/shadow/native.js`) the relative path is
    // `../../../zero-cache-shadow-ffi/index.js`. From the TS source itself
    // there is no corresponding file — the bundled path is only valid
    // post-build.
    '../../../zero-cache-shadow-ffi/index.js',
    // Dev / monorepo path, relative to the TS source tree.
    '../../../zero-cache-rs/crates/shadow-ffi/index.js',
  ];
  const errors: string[] = [];
  for (const p of candidates) {
    try {
      cached = require_(p) as ShadowNative;
      return cached;
    } catch (e) {
      errors.push(`${p}: ${(e as Error).message}`);
    }
  }
  // eslint-disable-next-line no-console
  console.warn(
    `[shadow] native module not loaded; shadow comparisons disabled. ` +
      `Tried:\n  ${errors.join('\n  ')}`,
  );
  cached = null;
  return undefined;
}

/**
 * Returns the loaded native module or throws. Use only in tests / harness
 * code that *must* exercise the shadow path.
 */
export function loadShadowNative(): ShadowNative {
  const n = tryLoadShadowNative();
  if (!n) {
    throw new Error(
      '[shadow] native module unavailable — build it via `napi build` in packages/zero-cache-rs/crates/shadow-ffi',
    );
  }
  return n;
}
