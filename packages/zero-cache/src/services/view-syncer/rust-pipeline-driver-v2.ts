/**
 * Rust-backed `PipelineDriver` implementation, wired to the
 * `view_syncer_v2::PipelineV2` Rust driver via the `pipeline_v2_*` napi
 * exports. No shadow-diff; production path.
 *
 * Gated by `ZERO_USE_RUST_IVM_V2=1`. Mirrors the public surface of
 * {@link PipelineDriver} so `syncer.ts` can swap between TS and v2 with
 * a one-line ternary.
 *
 * ## Architecture
 *
 * - Rust owns its own SQLite connections via `ChannelSource` (one
 *   worker thread per table, spawned on first `add_query` touching it).
 * - Table schemas (columns, primary keys, sort) are registered once at
 *   `init()` time — Rust needs them to build `SourceSchema` for each
 *   source without reading `PRAGMA table_info` separately.
 * - `Change` on advance is translated from TS's `SourceChange` shape
 *   (`{type, row, oldRow?}`) in this wrapper.
 * - Permissions / currentPermissions / scalar-subquery resolution are
 *   TS-side concerns that remain here (identical to `RustPipelineDriver`).
 */

import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import type {
  AST,
  CorrelatedSubquery,
  LiteralValue,
} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {buildPipeline} from '../../../../zql/src/builder/builder.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import {skipYields, type Input, type Storage} from '../../../../zql/src/ivm/operator.ts';
import type {SourceInput} from '../../../../zql/src/ivm/source.ts';
import {
  resolveSimpleScalarSubqueries,
  type CompanionSubquery,
} from '../../../../zqlite/src/resolve-scalar-subqueries.ts';
import type {ClientGroupStorage} from '../../../../zqlite/src/database-storage.ts';
import {
  TableSource,
  fromSQLiteTypes,
} from '../../../../zqlite/src/table-source.ts';
import {
  reloadPermissionsIfChanged,
  type LoadedPermissions,
} from '../../auth/load-permissions.ts';
import type {LogConfig, ZeroConfig} from '../../config/zero-config.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {
  loadShadowNative,
  type PipelineV2Handle,
  type ShadowNative,
  type V2RowChange,
} from '../../shadow/native.ts';
import type {RowKey} from '../../types/row-key.ts';
import type {ShardID} from '../../types/shards.ts';
import {deepEqual, type JSONValue} from '../../../../shared/src/json.ts';
import {type RowChange, type Timer} from './pipeline-driver.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteTableSpec} from '../../db/specs.ts';
import {getSubscriptionState} from '../replicator/schema/replication-state.ts';
import {checkClientSchema} from './client-schema.ts';
import type {RustSnapshotter} from './rust-snapshotter.ts';

/**
 * Mirror of `pipeline-driver.ts`'s private `QueryInfo` shape — kept
 * here as a structural duplicate so we don't widen TS-side exports
 * just to share a type. {@link RustPipelineDriverV2.queries} returns
 * a Map satisfying this shape, which is a structural subset of TS's
 * `QueryInfo`, so callers typed against either driver agree.
 */
type QueryInfo = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
};

/**
 * Inlined copy of TS `pipeline-driver.ts`'s private `getRowKey`.
 * Duplicated here (not exported from there) so that pipeline-driver.ts
 * stays parity-pure: only logs added by this branch, no widened exports.
 */
function getRowKey(cols: PrimaryKey, row: Row): RowKey {
  return Object.fromEntries(cols.map(col => [col, must(row[col])]));
}

/**
 * Inlined copy of `mustGetPrimaryKey` from pipeline-driver.ts —
 * same behaviour and message format. Duplicated for the same reason
 * as `getRowKey`: no widened TS-side exports.
 */
function mustGetPrimaryKeyForTable(
  primaryKeys: Map<string, PrimaryKey> | null,
  table: string,
): PrimaryKey {
  const pKeys = must(primaryKeys, 'primaryKey map must be non-null');
  return must(
    pKeys.get(table),
    `table '${table}' is not one of: ${[...pKeys.keys()].sort()}. ` +
      `Check the spelling and ensure that the table has a primary key.`,
  );
}

/**
 * Inlined copy of TS `PipelineDriver.#initAndResetCommon`'s body
 * (pipeline-driver.ts L253-L281). Duplicated here so pipeline-driver.ts
 * stays parity-pure (logs only). Mutates `tableSpecs`, `allTableNames`,
 * and (if non-null) the passed `primaryKeys`. Returns the populated
 * `primaryKeys` map and the `replicaVersion`.
 */
function initAndResetCommon(
  lc: LogContext,
  snapshotter: RustSnapshotter,
  shardID: ShardID,
  clientSchema: ClientSchema,
  tableSpecs: Map<string, LiteAndZqlSpec>,
  allTableNames: Set<string>,
  primaryKeys: Map<string, PrimaryKey> | null,
): {primaryKeys: Map<string, PrimaryKey>; replicaVersion: string} {
  const {db} = snapshotter.current();
  const fullTables = new Map<string, LiteTableSpec>();
  computeZqlSpecs(
    lc,
    db.db,
    {includeBackfillingColumns: false},
    tableSpecs,
    fullTables,
  );
  checkClientSchema(shardID, clientSchema, tableSpecs, fullTables);
  allTableNames.clear();
  for (const table of fullTables.keys()) {
    allTableNames.add(table);
  }
  const pk = primaryKeys ?? new Map<string, PrimaryKey>();
  pk.clear();
  for (const [table, spec] of tableSpecs.entries()) {
    pk.set(table, spec.tableSpec.primaryKey);
  }
  // Mirror TS `buildPrimaryKeys(clientSchema, pk)` — overlay
  // client-schema PKs on top of replica PKs (client wins).
  for (const [tableName, {primaryKey}] of Object.entries(
    clientSchema.tables,
  )) {
    pk.set(tableName, primaryKey as unknown as PrimaryKey);
  }
  const {replicaVersion} = getSubscriptionState(db);
  return {primaryKeys: pk, replicaVersion};
}

/**
 * v2's runtime value stored in `#pipelines`. `hydrationTimeMs` is kept
 * here (rather than a separate map) so the `totalHydrationTimeMs` and
 * `hydrationBudgetBreakdown` projections can read it directly, matching
 * TS `PipelineDriver` which also keeps it alongside the AST in its
 * `PipelineInfo`. `QueryInfo` (the declared return type of `queries()`)
 * is a structural subset, so callers typed through the driver API see
 * parity — only reaching into the Map with `any` would expose the
 * extra field.
 */
type PipelineMeta = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
  hydrationTimeMs: number;
};

/**
 * Same constructor shape as `PipelineDriver` / `RustPipelineDriver` — so
 * `syncer.ts` picks between them with a ternary.
 */
export class RustPipelineDriverV2 {
  readonly #lc: LogContext;
  readonly #snapshotter: RustSnapshotter;
  readonly #shardID: ShardID;
  readonly #storage: ClientGroupStorage;
  readonly #config: ZeroConfig | undefined;
  readonly #native: ShadowNative;
  readonly #handle: PipelineV2Handle;
  readonly #pipelines = new Map<string, PipelineMeta>();
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();
  readonly #allTableNames = new Set<string>();
  readonly #yieldThresholdMs: () => number;
  /**
   * TS-side `TableSource`s used ONLY by `#resolveScalarSubqueries` at
   * `addQuery` time — the scalar resolver needs to actually execute a
   * subquery via `buildPipeline`, which expects TS-side `Source`
   * instances. Cleared on `reset` and `destroy`. NOT used for hydration
   * or runtime reads — those go through Rust's `SqliteSource`.
   */
  readonly #subquerySources = new Map<string, TableSource>();
  readonly #logConfig: LogConfig;

  #primaryKeys: Map<string, PrimaryKey> | null = null;
  #permissions: LoadedPermissions | null = null;
  #replicaVersion: string | null = null;

  constructor(
    lc: LogContext,
    logConfig: LogConfig,
    snapshotter: RustSnapshotter,
    shardID: ShardID,
    storage: ClientGroupStorage,
    clientGroupID: string,
    _inspectorDelegate: InspectorDelegate,
    yieldThresholdMs: () => number,
    _enablePlanner?: boolean | undefined,
    config?: ZeroConfig | undefined,
  ) {
    this.#lc = lc.withContext('clientGroupID', clientGroupID);
    this.#logConfig = logConfig;
    this.#yieldThresholdMs = yieldThresholdMs;
    this.#snapshotter = snapshotter;
    this.#shardID = shardID;
    this.#storage = storage;
    this.#config = config;
    this.#native = loadShadowNative();

    const replicaPath = snapshotter.dbFile;
    this.#handle = this.#native.pipeline_v2_create(replicaPath);
  }

  init(clientSchema: ClientSchema): void {
    assert(!this.#snapshotter.initialized(), 'Already initialized');
    this.#snapshotter.init();
    this.#initAndResetCommon(clientSchema);
    this.#syncRustState();
    this.#native.pipeline_v2_init(
      this.#handle,
      must(this.#replicaVersion),
    );
    // Re-pin Rust's read snapshot at the current DB head. The
    // SnapshotReader pinned eagerly at `pipeline_v2_create` time; the
    // snapshotter's BEGIN CONCURRENT happens later, so we re-pin now
    // to close the window where Rust might see a slightly older commit
    // than the TS-side snapshotter.
    this.#native.pipeline_v2_refresh_snapshot(this.#handle);
    this.#logInit();
  }

  /**
   * Push the post-`#initAndResetCommon` table metadata into Rust so that
   * the Rust source factory can build `SqliteSource` instances on demand.
   * Called from both `init` and `reset` — any time `#tableSpecs` /
   * `#primaryKeys` have been recomputed.
   */
  #syncRustState(): void {
    const primaryKeys = must(this.#primaryKeys);
    const tablesJson: Record<
      string,
      {
        columns: string[];
        primaryKey: string[];
        sort?: Array<[string, 'asc' | 'desc']>;
        columnTypes?: Record<string, string>;
      }
    > = {};
    for (const [name, spec] of this.#tableSpecs.entries()) {
      const cols = Object.keys(spec.tableSpec.columns);
      const pk = must(primaryKeys.get(name));
      // Pass per-column Zero-schema types so Rust's `SqliteSource` can
      // coerce at the source boundary — INTEGER 0/1 → Bool, TEXT → JSON
      // for `json` columns — matching TS `fromSQLiteType`. Without this
      // every Number-vs-Bool comparison downstream relies on ad-hoc
      // patches in the comparators.
      const columnTypes: Record<string, string> = {};
      for (const [col, schemaValue] of Object.entries(spec.zqlSpec)) {
        columnTypes[col] = schemaValue.type;
      }
      tablesJson[name] = {
        columns: cols,
        primaryKey: [...pk],
        columnTypes,
      };
    }
    this.#native.pipeline_v2_register_tables(this.#handle, tablesJson);
  }

  #logInit(): void {
    // Parity log — same key/values emitted by TS PipelineDriver.init so
    // side-by-side diffing tools can confirm both drivers agree on the
    // post-init world. Keep field order stable.
    this.#lc.info?.('[ivm:init]', {
      driver: 'v2',
      replicaVersion: this.#replicaVersion,
      syncedTables: this.#tableSpecs.size,
      allTables: this.#allTableNames.size,
      primaryKeys: this.#primaryKeys?.size ?? 0,
    });
  }

  #initAndResetCommon(clientSchema: ClientSchema): void {
    // Shared with TS `PipelineDriver.#initAndResetCommon`. Single source
    // of truth for the validate + register + version-read sequence; see
    // `initAndResetCommon` in pipeline-driver.ts.
    const {primaryKeys, replicaVersion} = initAndResetCommon(
      this.#lc,
      this.#snapshotter,
      this.#shardID,
      clientSchema,
      this.#tableSpecs,
      this.#allTableNames,
      this.#primaryKeys,
    );
    this.#primaryKeys = primaryKeys;
    this.#replicaVersion = replicaVersion;
  }

  initialized(): boolean {
    return this.#snapshotter.initialized();
  }

  reset(clientSchema: ClientSchema): void {
    // Matches TS `PipelineDriver.reset` step-by-step:
    //   1. Tear down every live pipeline (JS-side #pipelines entries
    //      PLUS any Rust-side chains / in-flight hydrations).
    //   2. Clear JS maps.
    //   3. Re-run the shared `initAndResetCommon` sequence — recomputes
    //      #tableSpecs, #allTableNames, #primaryKeys, #replicaVersion
    //      from the CURRENT snapshotter state (does NOT re-init the
    //      snapshotter, matching TS).
    //   4. Re-register updated table metadata with Rust + refresh
    //      Rust's replica_version.
    const droppedPipelines = this.#pipelines.size;
    this.#native.pipeline_v2_reset(this.#handle);
    this.#pipelines.clear();
    // Matches TS `PipelineDriver.reset` clearing `#tables`: drop cached
    // scalar-subquery TableSources so they're rebuilt against the
    // (potentially-changed) schema on the next addQuery.
    this.#subquerySources.clear();
    this.#initAndResetCommon(clientSchema);
    this.#syncRustState();
    this.#native.pipeline_v2_init(
      this.#handle,
      must(this.#replicaVersion),
    );
    // Re-pin Rust's read snapshot post-reset so subsequent reads align
    // with the refreshed TS-side state.
    this.#native.pipeline_v2_refresh_snapshot(this.#handle);
    this.#lc.info?.('[ivm:reset]', {
      driver: 'v2',
      droppedPipelines,
      syncedTables: this.#tableSpecs.size,
      allTables: this.#allTableNames.size,
      primaryKeys: this.#primaryKeys?.size ?? 0,
      replicaVersion: this.#replicaVersion,
    });
  }

  get replicaVersion(): string {
    return must(this.#replicaVersion, 'Not yet initialized');
  }

  currentVersion(): string {
    assert(this.initialized(), 'Not yet initialized');
    return this.#snapshotter.current().version;
  }

  currentPermissions(): LoadedPermissions | null {
    // Matches TS `PipelineDriver.currentPermissions` line-for-line.
    // `snapshotter.current().db` under RustSnapshotter is the TS-side
    // schema-stable connection (permissions table reads don't need a
    // version-pinned snapshot — permissions change is handled by
    // `ResetPipelinesSignal` during advance iteration).
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
      this.#lc.debug?.(
        'Reloaded permissions',
        JSON.stringify(this.#permissions),
      );
    }
    return this.#permissions;
  }

  advanceWithoutDiff(): string {
    // Matches TS `PipelineDriver.advanceWithoutDiff` — advance the
    // snapshotter to the current DB head *without* iterating the diff,
    // then tell Rust about the new version so its own `current_version`
    // tracking stays in sync. Used when the replica has advanced but
    // there are no IVM-relevant changes to push through pipelines.
    const {version} = this.#snapshotter.advanceWithoutDiff().curr;
    this.#native.pipeline_v2_advance_without_diff(this.#handle, version);
    // Re-pin Rust's read snapshot at the new DB head so subsequent
    // reads (getRow during catchup, follow-up hydrations) agree with
    // the driver's tracked version. Mirrors TS's
    // `table.setDB(curr.db.db)` loop that runs at the same point.
    this.#native.pipeline_v2_refresh_snapshot(this.#handle);
    return version;
  }

  *addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
  ): Iterable<RowChange | 'yield'> {
    assert(
      this.initialized(),
      'Pipeline driver must be initialized before adding queries',
    );
    this.removeQuery(queryID);

    // Pre-resolve scalar subqueries (TS-side, mirrors TS PipelineDriver
    // `#resolveScalarSubqueries`). Rust v2's AST builder doesn't
    // synthesize the `zsubq_*` companion-pipeline relationships TS
    // would otherwise materialise; instead we collapse them to literal
    // conditions here, leaving the AST in a shape Rust's
    // `ast_to_chain_spec` can fully consume.
    const {ast: resolvedAst, companionRows} =
      this.#resolveScalarSubqueries(query);
    // Streaming hydration: Rust runs on a worker thread, chunks of up
    // to 100 rows arrive via next_chunk. Each chunk is yielded
    // row-by-row so ViewSyncer sees rows as soon as each batch lands,
    // matching TS's generator semantics observationally.
    this.#native.pipeline_v2_add_query_start(
      this.#handle,
      transformationHash,
      queryID,
      resolvedAst as unknown,
    );

    let rows = 0;
    let completed = false;
    // `null` if Rust never delivered a genuine Final event (e.g. the
    // caller removed the query mid-stream, in which case next_chunk
    // returns isFinal=true with hydrationTimeMs=null).
    let rustHydrationTimeMs: number | null = null;
    // Parent rows emitted by Rust — collected so we can run the
    // `related[]` tree expansion TS-side after Rust finishes. TS-native
    // PipelineDriver does this inline via Join transformers; v2's AST
    // builder doesn't yet translate `related[]` into Rust Joins, so we
    // mirror the tree-walk here. For scope + correctness the approach is
    // "single TS pipeline per related entry, one `fetch({constraint})`
    // per parent row", matching what TableSource's indexed lookup gives.
    const parentRows: Row[] = [];
    try {
      while (true) {
        const chunk = this.#native.pipeline_v2_next_chunk(
          this.#handle,
          queryID,
        );
        for (const c of chunk.rows) {
          rows++;
          const rc = this.#toRowChange(c);
          if (rc.type !== 'remove' && rc.row) {
            parentRows.push(rc.row);
          }
          yield rc;
        }
        if (chunk.isFinal) {
          rustHydrationTimeMs = chunk.hydrationTimeMs;
          break;
        }
      }
      // Emit related[] tree rows for each parent — skipped when the AST
      // has no `related` array, so queries without hierarchical joins
      // pay zero cost.
      if (resolvedAst.related && resolvedAst.related.length > 0) {
        for (const parentRow of parentRows) {
          for (const rc of this.#emitRelatedForRow(
            queryID,
            parentRow,
            resolvedAst.related,
          )) {
            rows++;
            yield rc;
          }
        }
      }
      completed = true;
    } finally {
      if (completed && rustHydrationTimeMs !== null) {
        // Genuine completion — emit companion rows (scalar-subquery
        // resolved values that need to round-trip to the client) before
        // recording metadata, then emit parity log.
        // Scalar-subquery companions are emitted once each (TS
        // `#companionRows` semantics — one resolved row per subquery).
        // EXISTS companions are emitted per-match — TS native's
        // `FlippedJoin` + `#streamNodes` walk emits the subquery row
        // for every parent that matched, without deduping. No dedupe
        // here either so row-count parity holds on queries like p19.
        for (const {table, row} of companionRows) {
          const primaryKey = mustGetPrimaryKeyForTable(
            this.#primaryKeys,
            table,
          );
          rows++;
          yield {
            type: 'add',
            queryID,
            table,
            rowKey: getRowKey(primaryKey, row as Row) as Row,
            row: this.#coerceRow(table, row as Row),
          } satisfies RowChange;
        }
        const hydrationTimeMs = timer.totalElapsed();
        this.#pipelines.set(queryID, {
          transformedAst: resolvedAst,
          transformationHash,
          hydrationTimeMs,
        });
        this.#lc.info?.('[ivm:addQuery]', {
          driver: 'v2',
          queryID,
          transformationHash,
          hydrationTimeMs,
          rows,
        });
      } else {
        // Either the generator was abandoned mid-hydration, OR the query
        // was externally removed during iteration (Rust signaled this by
        // returning hydrationTimeMs=null on the terminal chunk). In
        // either case, do NOT write `#pipelines.set` — that would
        // resurrect an entry the caller just asked us to remove. Also
        // best-effort clean up the Rust worker if still alive.
        try {
          this.#native.pipeline_v2_remove_query(this.#handle, queryID);
        } catch {
          // best-effort cleanup; worker may already be gone.
        }
      }
    }
  }

  removeQuery(queryID: string): void {
    // Order matches TS PipelineDriver.removeQuery: delete from the JS
    // map first, then tear down the backing resource. Under a partial
    // failure of the destroy step, the caller at least sees a consistent
    // "not in #pipelines" view.
    //
    // Note: we forward to Rust unconditionally (not gated on
    // `#pipelines.has`) because a query may be mid-hydration — in that
    // state Rust has in-flight state but `#pipelines` does not yet have
    // an entry. Rust's remove_query is idempotent across all three of
    // its maps (chains / infos / in_flight).
    const existed = this.#pipelines.has(queryID);
    this.#pipelines.delete(queryID);
    this.#native.pipeline_v2_remove_query(this.#handle, queryID);
    this.#lc.info?.('[ivm:removeQuery]', {driver: 'v2', queryID, existed});
  }

  getRow(table: string, pk: RowKey): Row | undefined {
    // Delegates to Rust, which does an indexed
    // `SELECT cols FROM "table" WHERE pk_col=? AND ... LIMIT 1`
    // through the per-table `SqliteSource`. Matches TS
    // `TableSource.getRow` behaviorally (PK-indexed single-row lookup)
    // without introducing a second SQLite connection stack in JS.
    assert(this.initialized(), 'Not yet initialized');
    const v = this.#native.pipeline_v2_get_row(
      this.#handle,
      table,
      pk as Record<string, unknown>,
    );
    if (v == null) return undefined;
    return v as Row;
  }

  advance(timer: Timer): {
    version: string;
    numChanges: number;
    snapshotMs: number;
    changes: Iterable<RowChange | 'yield'>;
  } {
    const t0 = performance.now();
    const diff = this.#snapshotter.advance(
      this.#tableSpecs,
      this.#allTableNames,
    );
    const snapshotMs = performance.now() - t0;
    const nextVersion = diff.curr.version;
    // Parity log — same key/values emitted by TS PipelineDriver.advance
    // so both drivers can be diffed line-by-line. `prevVersion` reads
    // the snapshot before this advance call; `version` is the new head.
    this.#lc.info?.('[ivm:advance]', {
      driver: 'v2',
      prevVersion: diff.prev.version,
      version: nextVersion,
      numChanges: diff.changes,
      replicaVersion: this.#replicaVersion,
    });
    // Bind outside the generator so it sees `this`.
    const self = this;
    const changes = (function* () {
      let yieldedAny = false;
      try {
        for (const c of diff) {
          // Between-change yield check — matches TS
          // `shouldAdvanceYieldMaybeAbortAdvance` (pipeline-driver.ts:848):
          // yield 'yield' only when the caller's Timer has exceeded its
          // slice budget (configured via `yieldThresholdMs`), not on
          // every elapsed-millisecond. Earlier `> 0` was over-yielding
          // on every change and defeating the time-budget batching.
          if (timer.elapsedLap() > self.#yieldThresholdMs()) {
            yield 'yield';
            yieldedAny = true;
          }
          // Skip unsynced tables: Rust's chains map by table name, so
          // a change for a non-synced table produces no emissions. TS
          // skips explicitly via `#tables.get(table)`; v2 achieves the
          // same by not having a chain for the table, but we skip
          // here too to save the napi round-trip.
          if (!self.#allTableNames.has(c.table)) continue;

          const primaryKey = mustGetPrimaryKeyForTable(
            self.#primaryKeys,
            c.table,
          );
          let editOldRow: Record<string, unknown> | undefined = undefined;
          // Per-prev loop — PK-compare each prevValue against nextValue.
          // Exactly one prev can match (the edit target); the rest are
          // unique-constraint conflicts that must be removed.
          // Matches TS `#advance` lines 718-736.
          for (const prev of c.prevValues) {
            if (
              c.nextValue &&
              deepEqual(
                getRowKey(primaryKey, prev as Row) as JSONValue,
                getRowKey(primaryKey, c.nextValue as Row) as JSONValue,
              )
            ) {
              editOldRow = prev as Record<string, unknown>;
            } else {
              const out = self.#native.pipeline_v2_advance(
                self.#handle,
                c.table,
                {
                  type: 'remove',
                  row: prev as Record<string, unknown>,
                },
              );
              for (const rc of out) yield self.#toRowChange(rc);
            }
          }
          if (c.nextValue) {
            if (editOldRow) {
              const out = self.#native.pipeline_v2_advance(
                self.#handle,
                c.table,
                {
                  type: 'edit',
                  row: c.nextValue as Record<string, unknown>,
                  oldRow: editOldRow,
                },
              );
              for (const rc of out) yield self.#toRowChange(rc);
            } else {
              const out = self.#native.pipeline_v2_advance(
                self.#handle,
                c.table,
                {
                  type: 'add',
                  row: c.nextValue as Record<string, unknown>,
                },
              );
              for (const rc of out) yield self.#toRowChange(rc);
            }
          }
        }
        // Bump Rust's version tracking and re-pin its snapshot to the
        // new curr — matches TS's `table.setDB(curr.db.db)` loop that
        // runs AFTER diff iteration, so during the loop refetches saw
        // the prev snapshot (correct for transforming prev→curr).
        self.#native.pipeline_v2_advance_without_diff(
          self.#handle,
          nextVersion,
        );
        self.#native.pipeline_v2_refresh_snapshot(self.#handle);
        self.#lc.debug?.(`Advanced to ${nextVersion}`);
      } finally {
        // Suppress unused-var lint when no yield ever fired.
        void yieldedAny;
      }
    })();
    return {
      version: nextVersion,
      numChanges: diff.changes,
      snapshotMs,
      changes,
    };
  }

  queries(): ReadonlyMap<string, QueryInfo> {
    return this.#pipelines;
  }

  totalHydrationTimeMs(): number {
    // Matches TS `PipelineDriver.totalHydrationTimeMs` — sums the
    // `hydrationTimeMs` field of each live pipeline. Stays TS-side
    // because `#pipelines` is the authoritative source and uses
    // `timer.totalElapsed()` (time-slice aware), not Rust wall clock.
    let total = 0;
    for (const pipeline of this.#pipelines.values()) {
      total += pipeline.hydrationTimeMs;
    }
    return total;
  }

  hydrationBudgetBreakdown(): {id: string; table: string; ms: number}[] {
    // Matches TS `PipelineDriver.hydrationBudgetBreakdown` exactly —
    // projection over `#pipelines`, descending by ms. Kept TS-side
    // for the same reason as `totalHydrationTimeMs`: `#pipelines` holds
    // the timer-measured ms (matches what TS reports), and Rust's
    // `infos` map doesn't sort.
    return [...this.#pipelines.entries()]
      .map(([id, p]) => ({
        id,
        table: p.transformedAst.table,
        ms: p.hydrationTimeMs,
      }))
      .sort((a, b) => b.ms - a.ms);
  }

  destroy(): void {
    // Order matches TS `PipelineDriver.destroy`:
    //   this.#storage.destroy();
    //   this.#snapshotter.destroy();
    // Failures propagate (no try/catch) — if `#storage.destroy` throws,
    // `#snapshotter.destroy` doesn't run, same as TS.
    //
    // No idempotency guard — double-destroy crashes on the second call
    // inside `#storage.destroy`, same as TS. If the caller needs
    // idempotency they can check state themselves.
    //
    // Trailing v2-only step: tear down the Rust handle. Failures here
    // also propagate.
    this.#subquerySources.clear();
    this.#storage.destroy();
    this.#snapshotter.destroy();
    this.#native.pipeline_v2_destroy(this.#handle);
    this.#lc.info?.('[ivm:destroy]', {driver: 'v2'});
  }

  // ─── Scalar-subquery resolution (TS-side, mirrors PipelineDriver) ──

  /**
   * Mirrors TS `PipelineDriver.#resolveScalarSubqueries`. Walks the AST
   * and, for every `correlatedSubquery` condition flagged as `scalar`,
   * runs a one-shot pipeline against the schema-stable TS connection
   * to compute the literal value, then folds it into the WHERE clause.
   * Companion-pipeline reactivity (TS's live re-resolution on data
   * change) is intentionally NOT implemented — v2 resolves once at
   * `addQuery` time. `companionRows` carries the resolved subquery
   * rows out so the caller can include them in the initial poke (the
   * client needs them locally for its own subquery evaluation).
   */
  #resolveScalarSubqueries(ast: AST): {
    ast: AST;
    companionRows: {table: string; row: Row}[];
    companions: CompanionSubquery[];
  } {
    const companionRows: {table: string; row: Row}[] = [];
    const executor = (
      subqueryAST: AST,
      childField: string,
    ): LiteralValue | null | undefined => {
      const input = buildPipeline(
        subqueryAST,
        {
          getSource: name => this.#getSubquerySource(name),
          createStorage: () => this.#createStorage(),
          decorateSourceInput: (i: SourceInput): Input => i,
          decorateInput: i => i,
          addEdge() {},
          decorateFilterInput: i => i,
        },
        'scalar-subquery',
      );
      let node: Node | undefined;
      for (const n of skipYields(input.fetch({}))) {
        node ??= n;
      }
      if (!node) return undefined;
      companionRows.push({table: subqueryAST.table, row: node.row as Row});
      return (node.row[childField] as LiteralValue) ?? null;
    };
    const {ast: resolved, companions} = resolveSimpleScalarSubqueries(
      ast,
      this.#tableSpecs,
      executor,
    );
    return {ast: resolved, companionRows, companions};
  }

  #getSubquerySource(tableName: string): TableSource {
    let source = this.#subquerySources.get(tableName);
    if (source) return source;
    const tableSpec = must(
      this.#tableSpecs.get(tableName),
      `unknown table ${tableName} for scalar subquery`,
    );
    const primaryKey = must(
      this.#primaryKeys?.get(tableName),
      `no primary key for ${tableName}`,
    );
    const {db} = this.#snapshotter.current();
    source = new TableSource(
      this.#lc,
      this.#logConfig,
      db.db,
      tableName,
      tableSpec.zqlSpec,
      primaryKey,
      () => false, // no per-row yield signalling for one-shot resolution
    );
    this.#subquerySources.set(tableName, source);
    return source;
  }

  #createStorage(): Storage {
    return this.#storage.createStorage();
  }

  /**
   * Emit `related[]` child rows for a parent row, matching TS native
   * PipelineDriver's tree-walk during hydration.
   *
   * Why this lives TS-side rather than in Rust: the v2 AST-to-ChainSpec
   * translation doesn't yet lower `ast.related[]` to Rust `JoinSpec`
   * entries (Rust supports a single Join, but zbugs queries need N).
   * Running the child expansion here via `buildPipeline` reuses TS's
   * Join operators, which are already battle-tested for arbitrary
   * related-tree shapes including junction edges (`hidden: true`).
   *
   * For each entry in `relateds` we strip the entry's own nested
   * `related[]`, build one TS pipeline, then `fetch({constraint})` with
   * the parent's correlation values. Each returned `Node` is emitted as
   * a RowChange (unless `hidden`, mirroring TS `#streamNodes`), then we
   * recurse on `cs.subquery.related` with the child row as the new
   * parent. This re-fetch-per-level strategy keeps the traversal simple
   * and relies on TableSource's indexed `fetch({constraint})` path for
   * speed.
   */
  *#emitRelatedForRow(
    queryID: string,
    parentRow: Row,
    relateds: readonly CorrelatedSubquery[],
  ): Iterable<RowChange> {
    for (const cs of relateds) {
      // Skip permission-system subqueries — parity with TS `Streamer`.
      if (cs.system === 'permissions') continue;

      const childAst = cs.subquery;
      const childTable = childAst.table;

      // Strip nested `related[]` before handing the AST to
      // `buildPipeline` — we walk the hierarchy ourselves so we don't
      // double-emit rows (once via Join-generated node.relationships
      // and once via our own recursion).
      //
      // Encode the correlation as an extra AND'd WHERE clause rather
      // than `fetch({constraint})`. The latter fails with "Constraint
      // should match partition key" when the subquery carries a Take
      // operator partitioned by different columns (e.g. `.one()` over
      // `viewState` partitioned by `userID` vs. our correlation on
      // `issueID`). Injecting the correlation into WHERE keeps Take's
      // partition-key contract intact — it takes 1 per partition, and
      // our extra filter narrows the scan within those partitions.
      const correlationConds = cs.correlation.childField.map((cField, i) => {
        const pField = cs.correlation.parentField[i];
        const value = parentRow[pField] as LiteralValue;
        return {
          type: 'simple' as const,
          op: '=' as const,
          left: {type: 'column' as const, name: cField},
          right: {type: 'literal' as const, value},
        };
      });
      const extraWhere =
        correlationConds.length === 1
          ? correlationConds[0]
          : {type: 'and' as const, conditions: correlationConds};
      const combinedWhere = childAst.where
        ? {type: 'and' as const, conditions: [extraWhere, childAst.where]}
        : extraWhere;
      const filteredChildAst: AST = {
        ...childAst,
        where: combinedWhere,
        related: undefined,
      };

      const input = buildPipeline(
        filteredChildAst,
        {
          getSource: name => this.#getSubquerySource(name),
          createStorage: () => this.#createStorage(),
          decorateSourceInput: (i: SourceInput): Input => i,
          decorateInput: i => i,
          addEdge() {},
          decorateFilterInput: i => i,
        },
        'related',
      );

      for (const node of skipYields(input.fetch({}))) {
        const childRow = this.#coerceRow(childTable, node.row as Row);
        // TS `#streamNodes` emits every node regardless of `hidden` —
        // junction edges (e.g. `issueLabel` in `issue.labels`) still
        // ship to the client so Replicache can reconstruct the join
        // locally. Matching that behavior here keeps the CVR row counts
        // aligned with TS-native (otherwise 603 junction rows per
        // zbugs All Issues query go missing).
        const primaryKey = mustGetPrimaryKeyForTable(
          this.#primaryKeys,
          childTable,
        );
        yield {
          type: 'add',
          queryID,
          table: childTable,
          rowKey: getRowKey(primaryKey, childRow) as Row,
          row: childRow,
        };
        if (childAst.related && childAst.related.length > 0) {
          yield* this.#emitRelatedForRow(queryID, childRow, childAst.related);
        }
      }
    }
  }

  /**
   * Coerce SQLite-typed column values (e.g. `open: 1`) to their Zero
   * schema types (`open: true`) before handing the row to the CVR /
   * client. Without this, the client's local ZQL runtime — which
   * compares by JS value identity — drops rows whose bool/bigint
   * columns arrive as integers. Parity with the TS-native path, which
   * routes every reader through `fromSQLiteTypes`.
   */
  #coerceRow(table: string, row: Row): Row {
    const spec = this.#tableSpecs.get(table);
    if (!spec) return row;
    return fromSQLiteTypes(spec.zqlSpec, row, table) as Row;
  }

  #toRowChange(c: V2RowChange): RowChange {
    const row = c.type === 'remove' ? undefined : this.#coerceRow(c.table, c.row as Row);
    switch (c.type) {
      case 'add':
        return {
          type: 'add',
          queryID: c.queryID,
          table: c.table,
          rowKey: c.rowKey as Row,
          row: row as Row,
        };
      case 'remove':
        return {
          type: 'remove',
          queryID: c.queryID,
          table: c.table,
          rowKey: c.rowKey as Row,
          row: undefined,
        };
      case 'edit':
        return {
          type: 'edit',
          queryID: c.queryID,
          table: c.table,
          rowKey: c.rowKey as Row,
          row: row as Row,
        };
    }
  }
}
