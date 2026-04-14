/**
 * Native-SQLite differential harness for the Rust port of
 * `services/view-syncer/pipeline-driver.ts`.
 *
 * # Architecture
 *
 * Both TS and Rust open their own connections to the same WAL2-mode replica
 * file on disk. TS runs its `PipelineDriver` (hydrate / advance / …) and
 * Rust runs its `pipeline_driver_*` FFI-exposed equivalent on the same file.
 * WAL2 makes concurrent readers safe. Each side uses its own in-process
 * storage DB for IVM memos — the storage is implementation-internal and
 * doesn't need to match.
 *
 * # Diff policy
 *
 * Only the `RowChange` stream is diffed. SQL text, query plans, storage
 * layout, row order within a query — none of those are compared. Rows are
 * sorted by `(queryID, table, rowKey)` before `diff()` so IVM's
 * implementation-order doesn't cause false positives.
 *
 * # Why this replaces the record-and-replay harness
 *
 * The old harness captured every SQLite call TS made and made Rust consume
 * from those queues. That coupling was fragile (handle IDs, pragma
 * ordering, SQL-text byte diffs). Native SQLite in Rust lets both sides be
 * independent clients of the same file — the only coupling left is that
 * their IVM outputs must agree.
 */

import type {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../zqlite/src/database-storage.ts';
import type {Database as DB} from '../../../zqlite/src/db.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {computeZqlSpecs} from '../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../db/specs.ts';
import {listTables} from '../db/lite-tables.ts';
import {InspectorDelegate} from '../server/inspector-delegate.ts';
import {DbFile} from '../test/lite.ts';
import {upstreamSchema, type ShardID} from '../types/shards.ts';
import {populateFromExistingTables} from '../services/replicator/schema/column-metadata.ts';
import {initReplicationState} from '../services/replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../services/replicator/test-utils.ts';
import {PipelineDriver, type Timer} from '../services/view-syncer/pipeline-driver.ts';
import {Snapshotter} from '../services/view-syncer/snapshotter.ts';
import {diff} from './diff.ts';
import {loadShadowNative, type ShadowNative, type ShadowRowChange} from './native.ts';

function tryNative(): ShadowNative | undefined {
  try {
    return loadShadowNative();
  } catch {
    return undefined;
  }
}

const native = tryNative();

const NO_TIMER: Timer = {
  elapsedLap: () => 0,
  totalElapsed: () => 0,
};

type Fixture = {
  dbFile: DbFile;
  db: DB;
  lc: LogContext;
  pipelines: PipelineDriver;
  replicator: FakeReplicator;
  shardID: ShardID;
  clientSchema: ReturnType<typeof createSchema>;
};

function createFixture(testName: string): Fixture {
  const lc = createSilentLogContext();
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;

  const dbFile = new DbFile(testName);
  dbFile.connect(lc).pragma('journal_mode = wal2');

  const storage = new Database(lc, ':memory:');
  storage.prepare(CREATE_STORAGE_TABLE).run();

  const pipelines = new PipelineDriver(
    lc,
    testLogConfig,
    new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
    shardID,
    new DatabaseStorage(storage).createClientGroupStorage('cg-shadow'),
    testName,
    new InspectorDelegate(undefined),
    () => 200,
  );

  const db = dbFile.connect(lc);
  initReplicationState(db, ['zero_data'], '123');
  db.exec(/*sql*/ `
    CREATE TABLE "${mutationsTableName}" (
      "clientGroupID"  TEXT,
      "clientID"       TEXT,
      "mutationID"     INTEGER,
      "result"         TEXT,
      _0_version       TEXT NOT NULL,
      PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
    );
    CREATE TABLE users (
      id TEXT PRIMARY KEY,
      name TEXT,
      age INTEGER,
      _0_version TEXT NOT NULL
    );
    INSERT INTO users (id, name, age, _0_version) VALUES ('u1', 'alice', 30, '123');
    INSERT INTO users (id, name, age, _0_version) VALUES ('u2', 'bob', 25, '123');
    INSERT INTO users (id, name, age, _0_version) VALUES ('u3', 'carol', 40, '123');
  `);

  populateFromExistingTables(db, listTables(db, false));

  const replicator = fakeReplicator(lc, db);

  const users = table('users')
    .columns({
      id: string(),
      name: string(),
      age: number(),
    })
    .primaryKey('id');

  const clientSchema = createSchema({tables: [users]});

  return {dbFile, db, lc, pipelines, replicator, shardID, clientSchema};
}

function teardownFixture(f: Fixture) {
  f.pipelines.destroy();
  f.dbFile.delete();
}

/**
 * Build the `tableSpecs` / `fullTables` args that Rust's `pipeline_driver_init`
 * expects by running the same `computeZqlSpecs` the TS driver uses, so the
 * two sides agree on column order and types.
 */
function deriveRustInitArgs(f: Fixture) {
  const tableSpecsMap = new Map<string, LiteAndZqlSpec>();
  const fullTablesMap = new Map<string, LiteTableSpec>();
  computeZqlSpecs(
    f.lc,
    f.db,
    {includeBackfillingColumns: false},
    tableSpecsMap,
    fullTablesMap,
  );

  const tableSpecs: Record<string, unknown> = {};
  for (const [name, spec] of tableSpecsMap.entries()) {
    tableSpecs[name] = {
      allPotentialPrimaryKeys: spec.tableSpec.allPotentialPrimaryKeys ?? [
        spec.tableSpec.primaryKey,
      ],
      zqlSpec: spec.zqlSpec,
    };
  }

  const fullTables: Record<string, unknown> = {};
  for (const [name, spec] of fullTablesMap.entries()) {
    fullTables[name] = {columns: spec.columns};
  }

  // Shape matches Rust's ClientSchema deserializer — one entry per table
  // the test cares about.
  const clientSchema = {
    tables: {
      users: {
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
          age: {type: 'number'},
        },
        primaryKey: ['id'],
      },
    },
  };

  return {clientSchema, tableSpecs, fullTables};
}

/**
 * Canonical key for sorting RowChanges so the diff doesn't flag
 * implementation-order differences between TS and Rust IVM.
 */
function rowChangeKey(c: ShadowRowChange | {type: string; queryID?: string; table?: string; rowKey?: unknown}): string {
  if (c.type === 'yield') return 'zzz_yield';
  const qid = (c as {queryID: string}).queryID;
  const tbl = (c as {table: string}).table;
  const rk = JSON.stringify((c as {rowKey: unknown}).rowKey);
  return `${qid}\x00${tbl}\x00${rk}\x00${c.type}`;
}

function sortChanges(arr: ReadonlyArray<unknown>): unknown[] {
  return [...arr].sort((a, b) =>
    rowChangeKey(a as never).localeCompare(rowChangeKey(b as never)),
  );
}

const it = native ? test : test.skip;

describe('view-syncer/pipeline-driver native differential', () => {
  let fixture: Fixture | undefined;

  beforeEach(() => {
    if (!native) return;
    fixture = createFixture('pd-shadow-scenario');
  });

  afterEach(() => {
    if (fixture) {
      teardownFixture(fixture);
      fixture = undefined;
    }
  });

  /**
   * Hydrate scenario: both sides open the replica, init + addQuery, compare
   * emitted row changes.
   */
  async function runHydrate(name: string, ast: AST) {
    if (!native || !fixture) throw new Error('fixture not initialised');
    const f = fixture;

    f.pipelines.init(f.clientSchema as never);
    const tsRows = [...f.pipelines.addQuery('hash1', 'queryID1', ast, NO_TIMER)];

    const rs = native.pipeline_driver_create(
      'cg-shadow',
      f.dbFile.path,
      f.shardID.appID,
      f.shardID.shardNum,
      1_000,
      false,
    );
    try {
      const args = deriveRustInitArgs(f);
      native.pipeline_driver_init(rs, args.clientSchema, args.tableSpecs, args.fullTables);
      const rsRows = native.pipeline_driver_add_query(
        rs,
        'hash1',
        'queryID1',
        ast,
        {elapsedLapMs: 0, totalElapsedMs: 0},
      );

      const d = diff(name, sortChanges(tsRows), sortChanges(rsRows));
      return {d, tsRows, rsRows};
    } finally {
      native.pipeline_driver_destroy(rs);
    }
  }

  /**
   * Advance scenario: both sides hydrate the same query, the replicator
   * writes a mutation on the shared file, both advance() and compare.
   */
  async function runAdvance(
    name: string,
    ast: AST,
    mutate: (f: Fixture) => void,
  ) {
    if (!native || !fixture) throw new Error('fixture not initialised');
    const f = fixture;

    f.pipelines.init(f.clientSchema as never);
    [...f.pipelines.addQuery('hash1', 'queryID1', ast, NO_TIMER)];

    const rs = native.pipeline_driver_create(
      'cg-shadow',
      f.dbFile.path,
      f.shardID.appID,
      f.shardID.shardNum,
      1_000,
      false,
    );
    try {
      const args = deriveRustInitArgs(f);
      native.pipeline_driver_init(rs, args.clientSchema, args.tableSpecs, args.fullTables);
      native.pipeline_driver_add_query(
        rs,
        'hash1',
        'queryID1',
        ast,
        {elapsedLapMs: 0, totalElapsedMs: 0},
      );

      // The replicator write is observed by both drivers via their own
      // Snapshotter advancing on the next `advance()` call. WAL2 makes the
      // concurrent view safe.
      mutate(f);

      const tsAdvance = f.pipelines.advance(NO_TIMER);
      const tsChanges = [...tsAdvance.changes];

      const rsAdvance = native.pipeline_driver_advance(rs, {
        elapsedLapMs: 0,
        totalElapsedMs: 0,
      });

      const d = diff(
        name,
        sortChanges(tsChanges),
        sortChanges(rsAdvance.changes),
      );
      return {d, tsAdvance, rsAdvance};
    } finally {
      native.pipeline_driver_destroy(rs);
    }
  }

  // ─── Hydration scenarios ──────────────────────────────────────────────

  it('hydrate_simple_select', async () => {
    const ast: AST = {table: 'users', orderBy: [['id', 'asc']]};
    const {d} = await runHydrate('hydrate_simple_select', ast);
    expect(d.ok).toBe(true);
  });

  it('hydrate_with_where', async () => {
    const ast: AST = {
      table: 'users',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'age'},
        op: '>',
        right: {type: 'literal', value: 25},
      },
      orderBy: [['id', 'asc']],
    };
    const {d} = await runHydrate('hydrate_with_where', ast);
    expect(d.ok).toBe(true);
  });

  it('hydrate_with_order_by_and_limit', async () => {
    const ast: AST = {
      table: 'users',
      orderBy: [
        ['age', 'desc'],
        ['id', 'asc'],
      ],
      limit: 2,
    };
    const {d} = await runHydrate('hydrate_with_order_by_and_limit', ast);
    expect(d.ok).toBe(true);
  });

  // ─── Advance scenarios ────────────────────────────────────────────────

  const advanceMessages = new ReplicationMessages({users: 'id'});

  it('advance_insert', async () => {
    const ast: AST = {table: 'users', orderBy: [['id', 'asc']]};
    const {d} = await runAdvance('advance_insert', ast, f => {
      f.replicator.processTransaction(
        '134',
        advanceMessages.insert('users', {id: 'u4', name: 'dave', age: 50}),
      );
    });
    expect(d.ok).toBe(true);
  });

  it('advance_delete', async () => {
    const ast: AST = {table: 'users', orderBy: [['id', 'asc']]};
    const {d} = await runAdvance('advance_delete', ast, f => {
      f.replicator.processTransaction(
        '134',
        advanceMessages.delete('users', {id: 'u2'}),
      );
    });
    expect(d.ok).toBe(true);
  });

  it('advance_update', async () => {
    const ast: AST = {table: 'users', orderBy: [['id', 'asc']]};
    const {d} = await runAdvance('advance_update', ast, f => {
      f.replicator.processTransaction(
        '134',
        advanceMessages.update('users', {id: 'u1', age: 99}),
      );
    });
    expect(d.ok).toBe(true);
  });

  // ─── Lifecycle scenarios ──────────────────────────────────────────────

  it('remove_query_cleans_up', async () => {
    if (!native || !fixture) return;
    const f = fixture;
    const ast: AST = {table: 'users', orderBy: [['id', 'asc']]};

    f.pipelines.init(f.clientSchema as never);
    [...f.pipelines.addQuery('hash1', 'queryID1', ast, NO_TIMER)];
    f.pipelines.removeQuery('queryID1');

    const rs = native.pipeline_driver_create(
      'cg-shadow',
      f.dbFile.path,
      f.shardID.appID,
      f.shardID.shardNum,
      1_000,
      false,
    );
    try {
      const args = deriveRustInitArgs(f);
      native.pipeline_driver_init(rs, args.clientSchema, args.tableSpecs, args.fullTables);
      native.pipeline_driver_add_query(
        rs,
        'hash1',
        'queryID1',
        ast,
        {elapsedLapMs: 0, totalElapsedMs: 0},
      );
      native.pipeline_driver_remove_query(rs, 'queryID1');

      expect(f.pipelines.queries().size).toBe(0);
      expect(native.pipeline_driver_queries(rs).length).toBe(0);
    } finally {
      native.pipeline_driver_destroy(rs);
    }
  });

  it('two_queries_isolation', async () => {
    if (!native || !fixture) return;
    const f = fixture;
    const astYoung: AST = {
      table: 'users',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'age'},
        op: '<',
        right: {type: 'literal', value: 30},
      },
      orderBy: [['id', 'asc']],
    };
    const astOld: AST = {
      table: 'users',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'age'},
        op: '>=',
        right: {type: 'literal', value: 30},
      },
      orderBy: [['id', 'asc']],
    };

    f.pipelines.init(f.clientSchema as never);
    const tsYoung = [...f.pipelines.addQuery('hy', 'q_young', astYoung, NO_TIMER)];
    const tsOld = [...f.pipelines.addQuery('ho', 'q_old', astOld, NO_TIMER)];

    const rs = native.pipeline_driver_create(
      'cg-shadow',
      f.dbFile.path,
      f.shardID.appID,
      f.shardID.shardNum,
      1_000,
      false,
    );
    try {
      const args = deriveRustInitArgs(f);
      native.pipeline_driver_init(rs, args.clientSchema, args.tableSpecs, args.fullTables);
      const rsYoung = native.pipeline_driver_add_query(
        rs,
        'hy',
        'q_young',
        astYoung,
        {elapsedLapMs: 0, totalElapsedMs: 0},
      );
      const rsOld = native.pipeline_driver_add_query(
        rs,
        'ho',
        'q_old',
        astOld,
        {elapsedLapMs: 0, totalElapsedMs: 0},
      );

      const d1 = diff('two_queries_isolation/young', sortChanges(tsYoung), sortChanges(rsYoung));
      const d2 = diff('two_queries_isolation/old', sortChanges(tsOld), sortChanges(rsOld));
      expect(d1.ok).toBe(true);
      expect(d2.ok).toBe(true);
    } finally {
      native.pipeline_driver_destroy(rs);
    }
  });
});
