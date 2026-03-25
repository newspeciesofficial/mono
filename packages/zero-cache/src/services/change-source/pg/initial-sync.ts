import {
  PG_CONFIGURATION_LIMIT_EXCEEDED,
  PG_INSUFFICIENT_PRIVILEGE,
} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {platform} from 'node:os';
import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import postgres from 'postgres';
import type {JSONObject} from '../../../../../shared/src/bigint-json.ts';
import {must} from '../../../../../shared/src/must.ts';
import {equals} from '../../../../../shared/src/set-utils.ts';
import type {DownloadStatus} from '../../../../../zero-events/src/status.ts';
import type {Database} from '../../../../../zqlite/src/db.ts';
import {
  createLiteIndexStatement,
  createLiteTableStatement,
} from '../../../db/create.ts';
import * as Mode from '../../../db/mode-enum.ts';
import {TsvParser} from '../../../db/pg-copy.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteIndex,
} from '../../../db/pg-to-lite.ts';
import {getTypeParsers, type TypeParsers} from '../../../db/pg-type-parser.ts';
import {runTx} from '../../../db/run-transaction.ts';
import type {IndexSpec, PublishedTableSpec} from '../../../db/specs.ts';
import {importSnapshot, TransactionPool} from '../../../db/transaction-pool.ts';
import {
  JSON_STRINGIFIED,
  liteValue,
  type LiteValueType,
} from '../../../types/lite.ts';
import {liteTableName} from '../../../types/names.ts';
import {
  pgClient,
  type PostgresDB,
  type PostgresTransaction,
  type PostgresValueType,
} from '../../../types/pg.ts';
import {CpuProfiler} from '../../../types/profiler.ts';
import type {ShardConfig} from '../../../types/shards.ts';
import {ALLOWED_APP_ID_CHARACTERS} from '../../../types/shards.ts';
import {id} from '../../../types/sql.ts';
import {ReplicationStatusPublisher} from '../../replicator/replication-status.ts';
import {ColumnMetadataStore} from '../../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../../replicator/schema/replication-state.ts';
import {toStateVersionString} from './lsn.ts';
import {ensureShardSchema} from './schema/init.ts';
import {getPublicationInfo} from './schema/published.ts';
import {
  addReplica,
  dropShard,
  getInternalShardConfig,
  newReplicationSlot,
  replicationSlotExpression,
  validatePublications,
} from './schema/shard.ts';

export type InitialSyncOptions = {
  tableCopyWorkers: number;
  profileCopy?: boolean | undefined;
};

/** Server context to store with the initial sync metadata for debugging. */
export type ServerContext = JSONObject;

// ── InitSyncWriter interface ──────────────────────────────────────────

export type InitTableColumn = {
  name: string;
  typeOID: number;
  dataType: string;
};

export type WriteChunkResult = {rowsFlushed: number};
export type FinalizeTableResult = {rows: number; flushTime: number};

/**
 * Abstraction over the SQLite write operations during initial sync.
 * Implementations:
 * - {@link DirectInitSyncWriter}: writes to a local Database (in-process)
 * - Worker-based impl (see initial-sync-worker-client.ts): delegates to a worker thread
 */
export interface InitSyncWriter {
  createTables(
    tables: PublishedTableSpec[],
    initialVersion: string,
  ): void | Promise<void>;
  initReplicationState(
    publications: string[],
    watermark: string,
    context: JSONObject,
  ): void | Promise<void>;
  initTable(
    tableName: string,
    columns: InitTableColumn[],
  ): void | Promise<void>;
  writeChunk(
    tableName: string,
    chunk: Buffer,
  ): Promise<WriteChunkResult> | WriteChunkResult;
  finalizeTable(
    tableName: string,
  ): Promise<FinalizeTableResult> | FinalizeTableResult;
  createIndexes(indexes: IndexSpec[]): void | Promise<void>;
}

/**
 * Runs initial sync: PG setup (replication slot, schema validation) and
 * data copy from PostgreSQL to SQLite.
 *
 * Accepts either a `Database` (direct, in-process writes) or an
 * `InitSyncWriter` (worker-based writes). When a `Database` is passed,
 * a {@link DirectInitSyncWriter} is created internally.
 */
export async function initialSync(
  lc: LogContext,
  shard: ShardConfig,
  txOrWriter: Database | InitSyncWriter,
  upstreamURI: string,
  syncOptions: InitialSyncOptions,
  context: ServerContext,
) {
  if (!ALLOWED_APP_ID_CHARACTERS.test(shard.appID)) {
    throw new Error(
      'The App ID may only consist of lower-case letters, numbers, and the underscore character',
    );
  }
  // Distinguish between Database (direct mode) and InitSyncWriter (worker mode).
  const directDb =
    'writeChunk' in txOrWriter ? undefined : (txOrWriter as Database);

  const {tableCopyWorkers, profileCopy} = syncOptions;
  const copyProfiler = profileCopy ? await CpuProfiler.connect() : null;
  const sql = pgClient(lc, upstreamURI);
  const replicationSession = pgClient(lc, upstreamURI, {
    ['fetch_types']: false, // Necessary for the streaming protocol
    connection: {replication: 'database'}, // https://www.postgresql.org/docs/current/protocol-replication.html
  });
  const slotName = newReplicationSlot(shard);
  const statusPublisher = new ReplicationStatusPublisher(directDb).publish(
    lc,
    'Initializing',
  );
  try {
    await checkUpstreamConfig(sql);

    const {publications} = await ensurePublishedTables(lc, sql, shard);
    lc.info?.(`Upstream is setup with publications [${publications}]`);

    const {database, host} = sql.options;
    lc.info?.(`opening replication session to ${database}@${host}`);

    let slot: ReplicationSlot;
    for (let first = true; ; first = false) {
      try {
        slot = await createReplicationSlot(lc, replicationSession, slotName);
        break;
      } catch (e) {
        if (first && e instanceof postgres.PostgresError) {
          if (e.code === PG_INSUFFICIENT_PRIVILEGE) {
            // Some Postgres variants (e.g. Google Cloud SQL) require that
            // the user have the REPLICATION role in order to create a slot.
            // Note that this must be done by the upstreamDB connection, and
            // does not work in the replicationSession itself.
            await sql`ALTER ROLE current_user WITH REPLICATION`;
            lc.info?.(`Added the REPLICATION role to database user`);
            continue;
          }
          if (e.code === PG_CONFIGURATION_LIMIT_EXCEEDED) {
            const slotExpression = replicationSlotExpression(shard);

            const dropped = await sql<{slot: string}[]>`
              SELECT slot_name as slot, pg_drop_replication_slot(slot_name) 
                FROM pg_replication_slots
                WHERE slot_name LIKE ${slotExpression} AND NOT active`;
            if (dropped.length) {
              lc.warn?.(
                `Dropped inactive replication slots: ${dropped.map(({slot}) => slot)}`,
                e,
              );
              continue;
            }
            lc.error?.(`Unable to drop replication slots`, e);
          }
        }
        throw e;
      }
    }
    const {snapshot_name: snapshot, consistent_point: lsn} = slot;
    const initialVersion = toStateVersionString(lsn);

    // Run up to MAX_WORKERS to copy of tables at the replication slot's snapshot.
    const start = performance.now();
    // Retrieve the published schema at the consistent_point.
    const published = await runTx(
      sql,
      async tx => {
        await tx.unsafe(/* sql*/ `SET TRANSACTION SNAPSHOT '${snapshot}'`);
        return getPublicationInfo(tx, publications);
      },
      {mode: Mode.READONLY},
    );
    // Note: If this throws, initial-sync is aborted.
    validatePublications(lc, published);

    // Now that tables have been validated, kick off the copiers.
    const {tables, indexes} = published;
    const numTables = tables.length;
    if (platform() === 'win32' && tableCopyWorkers < numTables) {
      lc.warn?.(
        `Increasing the number of copy workers from ${tableCopyWorkers} to ` +
          `${numTables} to work around a Node/Postgres connection bug`,
      );
    }
    const numWorkers =
      platform() === 'win32'
        ? numTables
        : Math.min(tableCopyWorkers, numTables);

    const copyPool = pgClient(lc, upstreamURI, {
      max: numWorkers,
      connection: {['application_name']: 'initial-sync-copy-worker'},
      ['max_lifetime']: 120 * 60, // set a long (2h) limit for COPY streaming
    });
    const copiers = startTableCopyWorkers(
      lc,
      copyPool,
      snapshot,
      numWorkers,
      numTables,
    );
    try {
      const writer = directDb
        ? new DirectInitSyncWriter(directDb, copyPool, lc)
        : (txOrWriter as InitSyncWriter);
      writer.initReplicationState(publications, initialVersion, context);
      writer.createTables(tables, initialVersion);
      const downloads = await Promise.all(
        tables.map(spec =>
          copiers.processReadTask((db, lc) =>
            getInitialDownloadState(lc, db, spec),
          ),
        ),
      );
      statusPublisher.publish(
        lc,
        'Initializing',
        `Copying ${numTables} upstream tables at version ${initialVersion}`,
        5000,
        () => ({downloadStatus: downloads.map(({status}) => status)}),
      );

      void copyProfiler?.start();
      const rowCounts = await Promise.all(
        downloads.map(table =>
          copiers.processReadTask((db, lc) => copy(lc, table, db, writer)),
        ),
      );
      void copyProfiler?.stopAndDispose(lc, 'initial-copy');
      copiers.setDone();

      const total = rowCounts.reduce(
        (acc, curr) => ({
          rows: acc.rows + curr.rows,
          flushTime: acc.flushTime + curr.flushTime,
        }),
        {rows: 0, flushTime: 0},
      );

      statusPublisher.publish(
        lc,
        'Indexing',
        `Creating ${indexes.length} indexes`,
        5000,
      );
      const indexStart = performance.now();
      writer.createIndexes(indexes);
      const index = performance.now() - indexStart;
      lc.info?.(`Created indexes (${index.toFixed(3)} ms)`);

      await addReplica(
        sql,
        shard,
        slotName,
        initialVersion,
        published,
        context,
      );

      const elapsed = performance.now() - start;
      lc.info?.(
        `Synced ${total.rows.toLocaleString()} rows of ${numTables} tables in ${publications} up to ${lsn} ` +
          `(flush: ${total.flushTime.toFixed(3)}, index: ${index.toFixed(3)}, total: ${elapsed.toFixed(3)} ms)`,
      );
    } finally {
      // All meaningful errors are handled at the processReadTask() call site.
      void copyPool.end().catch(e => lc.warn?.(`Error closing copyPool`, e));
    }
  } catch (e) {
    // If initial-sync did not succeed, make a best effort to drop the
    // orphaned replication slot to avoid running out of slots in
    // pathological cases that result in repeated failures.
    lc.warn?.(`dropping replication slot ${slotName}`, e);
    await sql`
      SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
        WHERE slot_name = ${slotName};
    `.catch(e => lc.warn?.(`Unable to drop replication slot ${slotName}`, e));
    await statusPublisher.publishAndThrowError(lc, 'Initializing', e);
  } finally {
    statusPublisher.stop();
    await replicationSession.end();
    await sql.end();
  }
}

async function checkUpstreamConfig(sql: PostgresDB) {
  const {walLevel, version} = (
    await sql<{walLevel: string; version: number}[]>`
      SELECT current_setting('wal_level') as "walLevel", 
             current_setting('server_version_num') as "version";
  `
  )[0];

  if (walLevel !== 'logical') {
    throw new Error(
      `Postgres must be configured with "wal_level = logical" (currently: "${walLevel})`,
    );
  }
  if (version < 150000) {
    throw new Error(
      `Must be running Postgres 15 or higher (currently: "${version}")`,
    );
  }
}

async function ensurePublishedTables(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardConfig,
  validate = true,
): Promise<{publications: string[]}> {
  const {database, host} = sql.options;
  lc.info?.(`Ensuring upstream PUBLICATION on ${database}@${host}`);

  await ensureShardSchema(lc, sql, shard);
  const {publications} = await getInternalShardConfig(sql, shard);

  if (validate) {
    let valid = false;
    const nonInternalPublications = publications.filter(
      p => !p.startsWith('_'),
    );
    const exists = await sql`
      SELECT pubname FROM pg_publication WHERE pubname IN ${sql(publications)}
      `.values();
    if (exists.length !== publications.length) {
      lc.warn?.(
        `some configured publications [${publications}] are missing: ` +
          `[${exists.flat()}]. resyncing`,
      );
    } else if (
      !equals(new Set(shard.publications), new Set(nonInternalPublications))
    ) {
      lc.warn?.(
        `requested publications [${shard.publications}] differ from previous` +
          `publications [${nonInternalPublications}]. resyncing`,
      );
    } else {
      valid = true;
    }
    if (!valid) {
      await sql.unsafe(dropShard(shard.appID, shard.shardNum));
      return ensurePublishedTables(lc, sql, shard, false);
    }
  }
  return {publications};
}

function startTableCopyWorkers(
  lc: LogContext,
  db: PostgresDB,
  snapshot: string,
  numWorkers: number,
  numTables: number,
): TransactionPool {
  const {init} = importSnapshot(snapshot);
  const tableCopiers = new TransactionPool(
    lc,
    Mode.READONLY,
    init,
    undefined,
    numWorkers,
  );
  tableCopiers.run(db);

  lc.info?.(`Started ${numWorkers} workers to copy ${numTables} tables`);

  if (parseInt(process.versions.node) < 22) {
    lc.warn?.(
      `\n\n\n` +
        `Older versions of Node have a bug that results in an unresponsive\n` +
        `Postgres connection after running certain combinations of COPY commands.\n` +
        `If initial sync hangs, run zero-cache with Node v22+. This has the additional\n` +
        `benefit of being consistent with the Node version run in the production container image.` +
        `\n\n\n`,
    );
  }
  return tableCopiers;
}

// Row returned by `CREATE_REPLICATION_SLOT`
type ReplicationSlot = {
  slot_name: string;
  consistent_point: string;
  snapshot_name: string;
  output_plugin: string;
};

// Note: The replication connection does not support the extended query protocol,
//       so all commands must be sent using sql.unsafe(). This is technically safe
//       because all placeholder values are under our control (i.e. "slotName").
export async function createReplicationSlot(
  lc: LogContext,
  session: postgres.Sql,
  slotName: string,
): Promise<ReplicationSlot> {
  const slot = (
    await session.unsafe<ReplicationSlot[]>(
      /*sql*/ `CREATE_REPLICATION_SLOT "${slotName}" LOGICAL pgoutput`,
    )
  )[0];
  lc.info?.(`Created replication slot ${slotName}`, slot);
  return slot;
}

function createLiteTables(
  tx: Database,
  tables: PublishedTableSpec[],
  initialVersion: string,
) {
  // TODO: Figure out how to reuse the ChangeProcessor here to avoid
  //       duplicating the ColumnMetadata logic.
  const columnMetadata = must(ColumnMetadataStore.getInstance(tx));
  for (const t of tables) {
    tx.exec(createLiteTableStatement(mapPostgresToLite(t, initialVersion)));
    const tableName = liteTableName(t);
    for (const [colName, colSpec] of Object.entries(t.columns)) {
      columnMetadata.insert(tableName, colName, colSpec);
    }
  }
}

function createLiteIndices(tx: Database, indices: IndexSpec[]) {
  for (const index of indices) {
    tx.exec(createLiteIndexStatement(mapPostgresToLiteIndex(index)));
  }
}

// ── DirectInitSyncWriter (in-process implementation) ──────────────────

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

/**
 * In-process implementation of {@link InitSyncWriter} that writes directly
 * to a local SQLite {@link Database}. Used by tests and the direct
 * (non-worker) code path.
 */
export class DirectInitSyncWriter implements InitSyncWriter {
  readonly #db: Database;
  readonly #dbClient: PostgresDB;
  readonly #lc: LogContext;
  #pgParsers: TypeParsers | undefined;
  readonly #tables = new Map<string, TableCopyState>();

  constructor(db: Database, dbClient: PostgresDB, lc: LogContext) {
    this.#db = db;
    this.#dbClient = dbClient;
    this.#lc = lc;
  }

  createTables(tables: PublishedTableSpec[], initialVersion: string) {
    createLiteTables(this.#db, tables, initialVersion);
  }

  initReplicationState(
    publications: string[],
    watermark: string,
    context: JSONObject,
  ) {
    initReplicationState(this.#db, publications, watermark, context);
  }

  async initTable(tableName: string, columns: InitTableColumn[]) {
    if (!this.#pgParsers) {
      this.#pgParsers = await getTypeParsers(this.#dbClient, {
        returnJsonAsString: true,
      });
    }

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
      const pgParse = must(this.#pgParsers).getTypeParser(c.typeOID);
      return (val: string) =>
        liteValue(
          pgParse(val) as PostgresValueType,
          c.dataType,
          JSON_STRINGIFIED,
        );
    });

    this.#tables.set(tableName, {
      tsvParser: new TsvParser(),
      parsers,
      insertStmt: this.#db.prepare(insertSql),
      insertBatchStmt: this.#db.prepare(
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
  }

  writeChunk(tableName: string, chunk: Buffer): WriteChunkResult {
    const state = must(this.#tables.get(tableName));
    const rowsBefore = state.totalRows;

    for (const text of state.tsvParser.parse(chunk)) {
      state.pendingSize += text === null ? 4 : text.length;
      state.pendingValues[state.pendingRows * state.valuesPerRow + state.col] =
        text === null ? null : state.parsers[state.col](text);

      if (++state.col === state.parsers.length) {
        state.col = 0;
        if (
          ++state.pendingRows >= MAX_BUFFERED_ROWS - state.valuesPerRow ||
          state.pendingSize >= BUFFERED_SIZE_THRESHOLD
        ) {
          this.#flush(tableName, state);
        }
      }
    }

    return {rowsFlushed: state.totalRows - rowsBefore};
  }

  finalizeTable(tableName: string): FinalizeTableResult {
    const state = must(this.#tables.get(tableName));
    if (state.pendingRows > 0) {
      this.#flush(tableName, state);
    }
    const result = {rows: state.totalRows, flushTime: state.flushTime};
    this.#tables.delete(tableName);
    return result;
  }

  createIndexes(indexes: IndexSpec[]) {
    createLiteIndices(this.#db, indexes);
  }

  #flush(tableName: string, state: TableCopyState) {
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
    this.#lc.debug?.(
      `flushed ${flushedRows} ${tableName} rows (${flushedSize} bytes) in ${elapsed.toFixed(3)} ms`,
    );
  }
}

// Verified empirically that batches of 50 seem to be the sweet spot,
// similar to the report in https://sqlite.org/forum/forumpost/8878a512d3652655
//
// Exported for testing.
export const INSERT_BATCH_SIZE = 50;

const MB = 1024 * 1024;
const MAX_BUFFERED_ROWS = 10_000;
const BUFFERED_SIZE_THRESHOLD = 8 * MB;

export type DownloadStatements = {
  select: string;
  getTotalRows: string;
  getTotalBytes: string;
};

export function makeDownloadStatements(
  table: PublishedTableSpec,
  cols: string[],
): DownloadStatements {
  const filterConditions = Object.values(table.publications)
    .map(({rowFilter}) => rowFilter)
    .filter(f => !!f); // remove nulls
  const where =
    filterConditions.length === 0
      ? ''
      : /*sql*/ `WHERE ${filterConditions.join(' OR ')}`;
  const fromTable = /*sql*/ `FROM ${id(table.schema)}.${id(table.name)} ${where}`;
  const totalBytes = `(${cols.map(col => `SUM(COALESCE(pg_column_size(${id(col)}), 0))`).join(' + ')})`;
  const stmts = {
    select: /*sql*/ `SELECT ${cols.map(id).join(',')} ${fromTable}`,
    getTotalRows: /*sql*/ `SELECT COUNT(*) AS "totalRows" ${fromTable}`,
    getTotalBytes: /*sql*/ `SELECT ${totalBytes} AS "totalBytes" ${fromTable}`,
  };
  return stmts;
}

type DownloadState = {
  spec: PublishedTableSpec;
  status: DownloadStatus;
};

async function getInitialDownloadState(
  lc: LogContext,
  sql: PostgresDB,
  spec: PublishedTableSpec,
): Promise<DownloadState> {
  const start = performance.now();
  const table = liteTableName(spec);
  const columns = Object.keys(spec.columns);
  const stmts = makeDownloadStatements(spec, columns);
  const rowsResult = sql
    .unsafe<{totalRows: bigint}[]>(stmts.getTotalRows)
    .execute();
  const bytesResult = sql
    .unsafe<{totalBytes: bigint}[]>(stmts.getTotalBytes)
    .execute();

  const state: DownloadState = {
    spec,
    status: {
      table,
      columns,
      rows: 0,
      totalRows: Number((await rowsResult)[0].totalRows),
      totalBytes: Number((await bytesResult)[0].totalBytes),
    },
  };
  const elapsed = (performance.now() - start).toFixed(3);
  lc.info?.(`Computed initial download state for ${table} (${elapsed} ms)`, {
    state: state.status,
  });
  return state;
}

async function copy(
  lc: LogContext,
  {spec: table, status}: DownloadState,
  from: PostgresTransaction,
  writer: InitSyncWriter,
) {
  const start = performance.now();
  const tableName = liteTableName(table);
  const orderedColumns = Object.entries(table.columns);
  const columnNames = orderedColumns.map(([c]) => c);

  const columns: InitTableColumn[] = orderedColumns.map(([name, spec]) => ({
    name,
    typeOID: spec.typeOID,
    dataType: spec.dataType,
  }));

  await writer.initTable(tableName, columns);

  const {select} = makeDownloadStatements(table, columnNames);
  lc.info?.(`Starting copy stream of ${tableName}:`, select);

  let copyResult: FinalizeTableResult | undefined;

  await pipeline(
    await from.unsafe(`COPY (${select}) TO STDOUT`).readable(),
    new Writable({
      highWaterMark: BUFFERED_SIZE_THRESHOLD,

      write(
        chunk: Buffer,
        _encoding: string,
        callback: (error?: Error) => void,
      ) {
        Promise.resolve(writer.writeChunk(tableName, chunk))
          .then(({rowsFlushed}) => {
            status.rows += rowsFlushed;
            callback();
          })
          .catch(e => callback(e instanceof Error ? e : new Error(String(e))));
      },

      final(callback: (error?: Error) => void) {
        Promise.resolve(writer.finalizeTable(tableName))
          .then(result => {
            copyResult = result;
            status.rows = result.rows;
            callback();
          })
          .catch(e => callback(e instanceof Error ? e : new Error(String(e))));
      },
    }),
  );

  const result = must(copyResult);
  const elapsed = performance.now() - start;
  lc.info?.(
    `Finished copying ${result.rows} rows into ${tableName} ` +
      `(flush: ${result.flushTime.toFixed(3)} ms) (total: ${elapsed.toFixed(3)} ms) `,
  );
  return result;
}
