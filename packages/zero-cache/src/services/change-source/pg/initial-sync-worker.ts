import type {LogContext} from '@rocicorp/logger';
import {parentPort} from 'node:worker_threads';
import type {JSONObject} from '../../../../../shared/src/bigint-json.ts';
import type {LogConfig} from '../../../../../shared/src/logging.ts';
import {must} from '../../../../../shared/src/must.ts';
import type {Database} from '../../../../../zqlite/src/db.ts';
import {
  createLiteIndexStatement,
  createLiteTableStatement,
} from '../../../db/create.ts';
import type {Migration} from '../../../db/migration-lite.ts';
import {runSchemaMigrations} from '../../../db/migration-lite.ts';
import {TsvParser} from '../../../db/pg-copy.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteIndex,
} from '../../../db/pg-to-lite.ts';
import {getTypeParsers, type TypeParsers} from '../../../db/pg-type-parser.ts';
import type {IndexSpec, PublishedTableSpec} from '../../../db/specs.ts';
import {
  JSON_STRINGIFIED,
  liteValue,
  type LiteValueType,
} from '../../../types/lite.ts';
import {liteTableName} from '../../../types/names.ts';
import {pgClient, type PostgresValueType} from '../../../types/pg.ts';
import {id} from '../../../types/sql.ts';
import {createLogContext} from '../../../server/logging.ts';
import {ColumnMetadataStore} from '../../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../../replicator/schema/replication-state.ts';
import {schemaVersionMigrationMap} from '../common/replica-schema.ts';
import {
  INSERT_BATCH_SIZE,
  type FinalizeTableResult,
  type InitTableColumn,
  type WriteChunkResult,
} from './initial-sync.ts';

if (!parentPort) {
  throw new Error('initial-sync-worker must be run as a worker thread');
}

const port = parentPort;

// ── Wire protocol ────────────────────────────────────────────────────

export type InitSyncMethod = keyof InitSyncArgsMap;

export type InitSyncArgsMap = {
  init: [
    dbPath: string,
    debugName: string,
    upstreamURI: string,
    logConfig: LogConfig,
  ];
  createTables: [tables: PublishedTableSpec[], initialVersion: string];
  initReplicationState: [
    publications: string[],
    watermark: string,
    context: JSONObject,
  ];
  initTable: [tableName: string, columns: InitTableColumn[]];
  writeChunk: [tableName: string, chunk: Buffer];
  finalizeTable: [tableName: string];
  createIndexes: [indexes: IndexSpec[]];
  done: [];
  abort: [];
};

export type InitSyncResultMap = {
  init: void;
  createTables: void;
  initReplicationState: void;
  initTable: void;
  writeChunk: WriteChunkResult;
  finalizeTable: FinalizeTableResult;
  createIndexes: void;
  done: void;
  abort: void;
};

export type InitSyncRequest<M extends InitSyncMethod = InitSyncMethod> = {
  id: number;
  method: M;
  args: InitSyncArgsMap[M];
};

export type InitSyncResponse<M extends InitSyncMethod = InitSyncMethod> =
  | {id: number; method: M; result: InitSyncResultMap[M]; error?: undefined}
  | {id: number; method: M; error: unknown; result?: undefined};

export type InitSyncError = {initSyncError: Error};

// ── Message queue for async command processing ───────────────────────

const messageQueue: InitSyncRequest[] = [];
let messageResolve: ((msg: InitSyncRequest) => void) | null = null;

function nextMessage(): Promise<InitSyncRequest> {
  if (messageQueue.length > 0) {
    return Promise.resolve(must(messageQueue.shift()));
  }
  return new Promise(resolve => {
    messageResolve = resolve;
  });
}

// ── Per-table copy state ─────────────────────────────────────────────

const MB = 1024 * 1024;
const MAX_BUFFERED_ROWS = 10_000;
const BUFFERED_SIZE_THRESHOLD = 8 * MB;

type TableCopyState = {
  tsvParser: TsvParser;
  parsers: ((val: string) => LiteValueType)[];
  insertStmt: ReturnType<Database['prepare']>;
  insertBatchStmt: ReturnType<Database['prepare']>;
  pendingValues: LiteValueType[];
  pendingRows: number;
  pendingSize: number;
  valuesPerRow: number;
  valuesPerBatch: number;
  totalRows: number;
  flushTime: number;
  col: number;
};

// ── Worker entry point ───────────────────────────────────────────────

port.on('message', (msg: InitSyncRequest) => {
  if (msg.method === 'init') {
    void runInit(msg as InitSyncRequest<'init'>);
  } else if (messageResolve) {
    messageResolve(msg);
    messageResolve = null;
  } else {
    messageQueue.push(msg);
  }
});

async function runInit(initMsg: InitSyncRequest<'init'>) {
  const [dbPath, debugName, upstreamURI, logConfig] = initMsg.args;
  const lc = createLogContext(
    {log: logConfig},
    {worker: 'initial-sync-worker'},
  );

  let pgParsers: TypeParsers | undefined;

  try {
    // Fetch type parsers from a short-lived PG connection.
    const typeFetcher = pgClient(lc, upstreamURI);
    pgParsers = await getTypeParsers(typeFetcher, {returnJsonAsString: true});
    await typeFetcher.end();
  } catch (e) {
    port.postMessage({
      id: initMsg.id,
      method: 'init',
      error: e instanceof Error ? e : new Error(String(e)),
    } satisfies InitSyncResponse<'init'>);
    return;
  }

  const tables = new Map<string, TableCopyState>();

  const setupMigration: Migration = {
    migrateSchema: async (_log, tx) => {
      // Signal that we're ready to receive commands.
      port.postMessage({
        id: initMsg.id,
        method: 'init',
        result: undefined,
      } satisfies InitSyncResponse<'init'>);

      // Process commands from the main thread within the migration transaction.
      await processCommands(lc, tx, must(pgParsers), tables);
    },
    minSafeVersion: 1,
  };

  try {
    await runSchemaMigrations(
      lc,
      debugName,
      dbPath,
      setupMigration,
      schemaVersionMigrationMap,
    );
  } catch (e) {
    port.postMessage({
      initSyncError: e instanceof Error ? e : new Error(String(e)),
    } satisfies InitSyncError);
  }
}

class AbortSignal extends Error {
  readonly name = 'AbortSignal';
}

async function processCommands(
  lc: LogContext,
  tx: Database,
  pgParsers: TypeParsers,
  tables: Map<string, TableCopyState>,
) {
  for (;;) {
    const msg = await nextMessage();
    try {
      const result = handleCommand(lc, tx, pgParsers, tables, msg);
      if (msg.method === 'done') {
        port.postMessage({
          id: msg.id,
          method: msg.method,
          result: undefined,
        } satisfies InitSyncResponse);
        return; // Return from migrateSchema → migration framework commits
      }
      if (msg.method === 'abort') {
        throw new AbortSignal();
      }
      port.postMessage({
        id: msg.id,
        method: msg.method,
        result,
      } as InitSyncResponse);
    } catch (e) {
      if (e instanceof AbortSignal) {
        throw e; // Propagate to trigger rollback
      }
      port.postMessage({
        id: msg.id,
        method: msg.method,
        error: e instanceof Error ? e : new Error(String(e)),
      } as InitSyncResponse);
    }
  }
}

function handleCommand(
  lc: LogContext,
  tx: Database,
  pgParsers: TypeParsers,
  tables: Map<string, TableCopyState>,
  msg: InitSyncRequest,
): unknown {
  switch (msg.method) {
    case 'createTables': {
      const [pubTables, initialVersion] =
        msg.args as InitSyncArgsMap['createTables'];
      const columnMetadata = must(ColumnMetadataStore.getInstance(tx));
      for (const t of pubTables) {
        tx.exec(createLiteTableStatement(mapPostgresToLite(t, initialVersion)));
        const tableName = liteTableName(t);
        for (const [colName, colSpec] of Object.entries(t.columns)) {
          columnMetadata.insert(tableName, colName, colSpec);
        }
      }
      return undefined;
    }

    case 'initReplicationState': {
      const [publications, watermark, context] =
        msg.args as InitSyncArgsMap['initReplicationState'];
      initReplicationState(tx, publications, watermark, context);
      return undefined;
    }

    case 'initTable': {
      const [tableName, columns] = msg.args as InitSyncArgsMap['initTable'];
      const columnNames = columns.map(c => c.name);
      const insertColumnList = columnNames.map(c => id(c)).join(',');
      const valuesSql =
        columnNames.length > 0
          ? `(${'?,'.repeat(columnNames.length - 1)}?)`
          : '()';
      const insertSql = /*sql*/ `
        INSERT INTO "${tableName}" (${insertColumnList}) VALUES ${valuesSql}`;
      const valuesPerRow = columns.length;

      const parsers = columns.map(c => {
        const pgParse = pgParsers.getTypeParser(c.typeOID);
        return (val: string) =>
          liteValue(
            pgParse(val) as PostgresValueType,
            c.dataType,
            JSON_STRINGIFIED,
          );
      });

      tables.set(tableName, {
        tsvParser: new TsvParser(),
        parsers,
        insertStmt: tx.prepare(insertSql),
        insertBatchStmt: tx.prepare(
          insertSql + `,${valuesSql}`.repeat(INSERT_BATCH_SIZE - 1),
        ),
        pendingValues: Array.from({length: MAX_BUFFERED_ROWS * valuesPerRow}),
        pendingRows: 0,
        pendingSize: 0,
        valuesPerRow,
        valuesPerBatch: valuesPerRow * INSERT_BATCH_SIZE,
        totalRows: 0,
        flushTime: 0,
        col: 0,
      });
      return undefined;
    }

    case 'writeChunk': {
      const [tableName, chunk] = msg.args as InitSyncArgsMap['writeChunk'];
      const state = must(tables.get(tableName));
      const rowsBefore = state.totalRows;

      for (const text of state.tsvParser.parse(chunk)) {
        state.pendingSize += text === null ? 4 : text.length;
        state.pendingValues[
          state.pendingRows * state.valuesPerRow + state.col
        ] = text === null ? null : state.parsers[state.col](text);

        if (++state.col === state.parsers.length) {
          state.col = 0;
          if (
            ++state.pendingRows >= MAX_BUFFERED_ROWS - state.valuesPerRow ||
            state.pendingSize >= BUFFERED_SIZE_THRESHOLD
          ) {
            flush(lc, tableName, state);
          }
        }
      }

      return {
        rowsFlushed: state.totalRows - rowsBefore,
      } satisfies WriteChunkResult;
    }

    case 'finalizeTable': {
      const [tableName] = msg.args as InitSyncArgsMap['finalizeTable'];
      const state = must(tables.get(tableName));
      if (state.pendingRows > 0) {
        flush(lc, tableName, state);
      }
      const result: FinalizeTableResult = {
        rows: state.totalRows,
        flushTime: state.flushTime,
      };
      tables.delete(tableName);
      return result;
    }

    case 'createIndexes': {
      const [indexes] = msg.args as InitSyncArgsMap['createIndexes'];
      for (const index of indexes) {
        tx.exec(createLiteIndexStatement(mapPostgresToLiteIndex(index)));
      }
      return undefined;
    }

    case 'done':
    case 'abort':
      return undefined;

    default:
      throw new Error(`Unknown method: ${msg.method}`);
  }
}

function flush(lc: LogContext, tableName: string, state: TableCopyState) {
  const start = performance.now();
  const flushedRows = state.pendingRows;
  const flushedSize = state.pendingSize;

  let l = 0;
  for (
    ;
    state.pendingRows > INSERT_BATCH_SIZE;
    state.pendingRows -= INSERT_BATCH_SIZE
  ) {
    state.insertBatchStmt.run(
      state.pendingValues.slice(l, (l += state.valuesPerBatch)),
    );
  }
  for (; state.pendingRows > 0; state.pendingRows--) {
    state.insertStmt.run(
      state.pendingValues.slice(l, (l += state.valuesPerRow)),
    );
  }
  for (let i = 0; i < flushedRows; i++) {
    state.pendingValues[i] = undefined as unknown as LiteValueType;
  }
  state.pendingSize = 0;
  state.totalRows += flushedRows;

  const elapsed = performance.now() - start;
  state.flushTime += elapsed;
  lc.debug?.(
    `flushed ${flushedRows} ${tableName} rows (${flushedSize} bytes) in ${elapsed.toFixed(3)} ms`,
  );
}
