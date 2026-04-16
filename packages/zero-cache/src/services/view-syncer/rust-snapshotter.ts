/**
 * Rust-backed Snapshotter — same public interface as the TS
 * {@link Snapshotter}, but the pinned SQLite connection(s) live in
 * Rust behind a napi handle. Both the snapshotter's internal reads
 * (changelog iteration, per-row lookups during diff) and any Rust-side
 * IVM reads (hydration, getRow, refetch) that go through the same
 * PipelineV2 driver end up on the same `BEGIN CONCURRENT` connection
 * — eliminating the cross-connection race the TS-native path had.
 *
 * Used by `RustPipelineDriverV2`. TS `PipelineDriver` continues to
 * use the native-TS `Snapshotter`.
 */

import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {stringify, type JSONValue} from '../../../../shared/src/bigint-json.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {fromSQLiteTypes} from '../../../../zqlite/src/table-source.ts';
import type {
  LiteAndZqlSpec,
  LiteTableSpecWithKeysAndVersion,
} from '../../db/specs.ts';
import {StatementRunner} from '../../db/statements.ts';
import {
  normalizedKeyOrder,
  type RowKey,
  type RowValue,
} from '../../types/row-key.ts';
import type {AppID} from '../../types/shards.ts';
import {id} from '../../types/sql.ts';
import {
  RESET_OP,
  SET_OP,
  TRUNCATE_OP,
} from '../replicator/schema/change-log.ts';
import {ZERO_VERSION_COLUMN_NAME as ROW_VERSION} from '../replicator/schema/replication-state.ts';
import {
  loadShadowNative,
  type ShadowNative,
  type SnapshotterHandle,
} from '../../shadow/native.ts';
import {
  InvalidDiffError,
  ResetPipelinesSignal,
  type Change,
  type SnapshotDiff,
} from './snapshotter.ts';

/**
 * Same constructor signature as the native-TS {@link Snapshotter} so
 * `syncer.ts`-level code can instantiate it identically.
 */
export class RustSnapshotter {
  readonly #lc: LogContext;
  readonly #dbFile: string;
  readonly #appID: string;
  readonly #native: ShadowNative;
  readonly #handle: SnapshotterHandle;

  /**
   * TS-side connection kept open *only* for `current().db` consumers
   * that need schema-stable reads (e.g. `computeZqlSpecs`,
   * `reloadPermissionsIfChanged`). Not pinned — schema doesn't depend
   * on snapshot version. All version-sensitive reads (changelog +
   * per-row lookups during diff) route through the Rust handle.
   */
  #tsSchemaDb: StatementRunner | undefined;

  constructor(
    lc: LogContext,
    dbFile: string,
    {appID}: AppID,
    _pageCacheSizeKib?: number | undefined,
  ) {
    this.#lc = lc;
    this.#dbFile = dbFile;
    this.#appID = appID;
    this.#native = loadShadowNative();
    this.#handle = this.#native.snapshotter_create(dbFile);
  }

  get dbFile(): string {
    return this.#dbFile;
  }

  init(): this {
    assert(!this.initialized(), 'Already initialized');
    const version = this.#native.snapshotter_init(this.#handle);
    this.#lc.debug?.(`Initial snapshot at version ${version}`);
    return this;
  }

  initialized(): boolean {
    return this.#native.snapshotter_initialized(this.#handle);
  }

  current(): RustSnapshot {
    assert(this.initialized(), 'Snapshotter has not been initialized');
    const version = this.#native.snapshotter_current_version(this.#handle);
    assert(version !== null, 'current_version returned null post-init');
    return new RustSnapshot(this, version, 'curr');
  }

  advance(
    syncableTables: Map<string, LiteAndZqlSpec>,
    allTableNames: Set<string>,
  ): SnapshotDiff {
    const {prev, curr} = this.advanceWithoutDiff();
    return new RustDiff(this, this.#appID, syncableTables, allTableNames, prev, curr);
  }

  advanceWithoutDiff(): {prev: RustSnapshot; curr: RustSnapshot} {
    assert(this.initialized(), 'Snapshotter has not been initialized');
    const {prevVersion, currVersion} = this.#native.snapshotter_advance(
      this.#handle,
    );
    return {
      prev: new RustSnapshot(this, prevVersion, 'prev'),
      curr: new RustSnapshot(this, currVersion, 'curr'),
    };
  }

  destroy() {
    this.#native.snapshotter_destroy(this.#handle);
    // better-sqlite3 Database doesn't expose a type-level `close()` but
    // its underlying db does.
    try {
      this.#tsSchemaDb?.db.close();
    } catch {
      // best-effort
    }
    this.#lc.debug?.('closed database connections');
  }

  // ─── Internal hooks used by RustSnapshot / RustDiff ────────────────

  /** Lazy TS-side connection for schema-stable reads only. */
  schemaDb(): StatementRunner {
    if (!this.#tsSchemaDb) {
      const conn = new Database(this.#lc, this.#dbFile);
      this.#tsSchemaDb = new StatementRunner(conn);
    }
    return this.#tsSchemaDb;
  }

  _native(): ShadowNative {
    return this.#native;
  }

  _handle(): SnapshotterHandle {
    return this.#handle;
  }

  numChangesSince(prevVersion: string): number {
    return Number(
      this.#native.snapshotter_num_changes_since(this.#handle, prevVersion),
    );
  }

  /** Start streaming the changelog. One iteration per handle at a time. */
  changelogStart(prevVersion: string): void {
    this.#native.snapshotter_changelog_start(this.#handle, prevVersion);
  }

  changelogNextChunk(): {
    rows: Array<Record<string, unknown>>;
    isFinal: boolean;
  } {
    return this.#native.snapshotter_changelog_next_chunk(this.#handle);
  }

  changelogCleanup(): void {
    this.#native.snapshotter_changelog_cleanup(this.#handle);
  }

  readInCurr(
    sql: string,
    params: Array<unknown>,
    columns: string[],
  ): Array<Record<string, unknown>> {
    return this.#native.snapshotter_read_in_curr(
      this.#handle,
      sql,
      params,
      columns,
    );
  }

  readInPrev(
    sql: string,
    params: Array<unknown>,
    columns: string[],
  ): Array<Record<string, unknown>> {
    return this.#native.snapshotter_read_in_prev(
      this.#handle,
      sql,
      params,
      columns,
    );
  }
}

/**
 * Snapshot view — exposes `db` (for schema-stable TS reads like
 * `computeZqlSpecs` / `reloadPermissionsIfChanged`) and `version`
 * (pinned replica version, from Rust). `db` is NOT snapshot-pinned —
 * it's a plain open connection used only for schema lookups; runtime
 * row reads go through the Rust-pinned connection via
 * `getRow` / `getRows` below.
 */
export class RustSnapshot {
  readonly #owner: RustSnapshotter;
  readonly version: string;
  readonly #which: 'prev' | 'curr';

  constructor(
    owner: RustSnapshotter,
    version: string,
    which: 'prev' | 'curr',
  ) {
    this.#owner = owner;
    this.version = version;
    this.#which = which;
  }

  /** For schema-stable TS reads only. See class docstring. */
  get db(): StatementRunner {
    return this.#owner.schemaDb();
  }

  /** Matches TS `Snapshot.getRow`, runs on Rust's pinned connection. */
  getRow(
    table: LiteTableSpecWithKeysAndVersion,
    rowKey: JSONValue,
  ): Record<string, unknown> | undefined {
    const key = normalizedKeyOrder(rowKey as RowKey);
    const conds = Object.keys(key).map(c => `${id(c)}=?`);
    const cols = Object.keys(table.columns);
    const sql = `SELECT ${cols.map(c => id(c)).join(',')} FROM ${id(
      table.name,
    )} WHERE ${conds.join(' AND ')} LIMIT 1`;
    const params = Object.values(key);
    const rows =
      this.#which === 'curr'
        ? this.#owner.readInCurr(sql, params, cols)
        : this.#owner.readInPrev(sql, params, cols);
    return rows[0];
  }

  /** Matches TS `Snapshot.getRows`, runs on Rust's pinned connection. */
  getRows(
    table: LiteTableSpecWithKeysAndVersion,
    keys: ReadonlyArray<ReadonlyArray<string>>,
    row: RowValue,
  ): Array<Record<string, unknown>> {
    const validKeys = keys.filter(key =>
      key.every(column => row[column] !== null && row[column] !== undefined),
    );
    if (validKeys.length === 0) return [];
    const conds = validKeys.map(key => key.map(c => `${id(c)}=?`));
    const cols = Object.keys(table.columns);
    const sql = `SELECT ${cols.map(c => id(c)).join(',')} FROM ${id(
      table.name,
    )} WHERE ${conds.map(cond => cond.join(' AND ')).join(' OR ')}`;
    const params = validKeys.flatMap(key => key.map(column => row[column]));
    return this.#which === 'curr'
      ? this.#owner.readInCurr(sql, params as unknown[], cols)
      : this.#owner.readInPrev(sql, params as unknown[], cols);
  }
}

// ─── Diff iteration ────────────────────────────────────────────────────

/** Raw changelog entry from Rust — matches `_zero.changeLog2` columns. */
type ChangelogEntry = {
  stateVersion: string;
  table: string;
  rowKey: string; // JSON-encoded
  op: string;
};

/**
 * Implements `SnapshotDiff` over Rust-backed snapshots. Mirrors the
 * TS-native `Diff` class's iteration logic line-by-line, but each read
 * (changelog + per-row lookups) routes through napi onto the Rust
 * pinned connections.
 */
class RustDiff implements SnapshotDiff {
  readonly #owner: RustSnapshotter;
  readonly #permissionsTable: string;
  readonly #syncableTables: Map<string, LiteAndZqlSpec>;
  readonly #allTableNames: Set<string>;
  readonly prev: RustSnapshot;
  readonly curr: RustSnapshot;
  readonly changes: number;

  constructor(
    owner: RustSnapshotter,
    appID: string,
    syncableTables: Map<string, LiteAndZqlSpec>,
    allTableNames: Set<string>,
    prev: RustSnapshot,
    curr: RustSnapshot,
  ) {
    this.#owner = owner;
    this.#permissionsTable = `${appID}.permissions`;
    this.#syncableTables = syncableTables;
    this.#allTableNames = allTableNames;
    this.prev = prev;
    this.curr = curr;
    this.changes = owner.numChangesSince(prev.version);
  }

  [Symbol.iterator](): Iterator<Change> {
    this.#owner.changelogStart(this.prev.version);
    let buffer: ChangelogEntry[] = [];
    let done = false;

    const refill = (): boolean => {
      const chunk = this.#owner.changelogNextChunk();
      buffer = chunk.rows.map(r => ({
        stateVersion: String(r.stateVersion),
        table: String(r.table),
        rowKey: String(r.rowKey),
        op: String(r.op),
      }));
      if (chunk.isFinal) done = true;
      return buffer.length > 0;
    };

    const cleanup = () => {
      try {
        this.#owner.changelogCleanup();
      } catch {
        // best-effort
      }
    };

    return {
      next: () => {
        try {
          for (;;) {
            if (buffer.length === 0) {
              if (done) {
                cleanup();
                return {value: undefined, done: true};
              }
              if (!refill()) {
                cleanup();
                return {value: undefined, done: true};
              }
            }
            const {stateVersion, table, rowKey: rowKeyStr, op} = buffer.shift()!;
            if (op === RESET_OP) {
              throw new ResetPipelinesSignal(
                `schema for table ${table} has changed`,
              );
            }
            if (op === TRUNCATE_OP) {
              throw new ResetPipelinesSignal(
                `table ${table} has been truncated`,
              );
            }
            const specs = this.#syncableTables.get(table);
            if (!specs) {
              if (this.#allTableNames.has(table)) continue;
              throw new Error(`change for unknown table ${table}`);
            }
            const {tableSpec, zqlSpec} = specs;

            // Parse rowKey JSON (TS-native Snapshotter parses via valita;
            // here we trust the Rust-provided string).
            const rowKey = JSON.parse(rowKeyStr) as JSONValue;
            assert(rowKey !== null, 'rowKey must be present for row changes');

            const nextValue =
              op === SET_OP ? this.curr.getRow(tableSpec, rowKey) : undefined;

            let prevValues: Array<Record<string, unknown>>;
            if (nextValue) {
              prevValues = this.prev.getRows(
                tableSpec,
                tableSpec.uniqueKeys,
                nextValue as RowValue,
              );
            } else {
              const prevValue = this.prev.getRow(tableSpec, rowKey);
              prevValues = prevValue ? [prevValue] : [];
            }
            if (op === SET_OP && nextValue === undefined) {
              throw new Error(
                `Missing value for ${table} ${stringify(rowKey)}`,
              );
            }

            const nextValueOrNull = nextValue ?? null;

            // Sanity checks matching TS `Diff.checkThatDiffIsValid`.
            if (stateVersion > this.curr.version) {
              throw new InvalidDiffError(
                `Diff is no longer valid. curr db has advanced past ${this.curr.version}`,
              );
            }
            if (
              prevValues.findIndex(
                p => (String(p[ROW_VERSION] ?? '~')) > this.prev.version,
              ) !== -1
            ) {
              throw new InvalidDiffError(
                `Diff is no longer valid. prev db has advanced past ${this.prev.version}.`,
              );
            }
            if (
              op === SET_OP &&
              nextValueOrNull &&
              nextValueOrNull[ROW_VERSION] !== stateVersion
            ) {
              throw new InvalidDiffError(
                'Diff is no longer valid. curr db has advanced.',
              );
            }

            if (prevValues.length === 0 && nextValueOrNull === null) {
              // No-op change (e.g., delete of a row that doesn't exist).
              continue;
            }

            if (
              table === this.#permissionsTable &&
              nextValueOrNull &&
              prevValues.find(
                p =>
                  (p as Record<string, unknown>).permissions !==
                  (nextValueOrNull as Record<string, unknown>).permissions,
              )
            ) {
              const differ = prevValues.find(
                p =>
                  (p as Record<string, unknown>).permissions !==
                  (nextValueOrNull as Record<string, unknown>).permissions,
              )!;
              throw new ResetPipelinesSignal(
                `Permissions have changed ${
                  (differ as Record<string, unknown>).hash
                } => ${(nextValueOrNull as Record<string, unknown>).hash}`,
              );
            }

            return {
              value: {
                table,
                prevValues: prevValues.map(p =>
                  fromSQLiteTypes(
                    zqlSpec,
                    p as unknown as Row,
                    table,
                  ) as Readonly<Row>,
                ),
                nextValue: nextValueOrNull
                  ? (fromSQLiteTypes(
                      zqlSpec,
                      nextValueOrNull as unknown as Row,
                      table,
                    ) as Readonly<Row>)
                  : null,
                rowKey: rowKey as RowKey,
              } satisfies Change,
              done: false,
            };
          }
        } catch (e) {
          cleanup();
          throw e;
        }
      },
      return: value => {
        cleanup();
        return {value, done: true};
      },
    };
  }
}
