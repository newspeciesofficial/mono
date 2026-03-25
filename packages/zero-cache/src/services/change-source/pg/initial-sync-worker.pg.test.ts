/* oxlint-disable no-console */
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from '@testcontainers/postgresql';
import postgres from 'postgres';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {assert} from '../../../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {listTables} from '../../../db/lite-tables.ts';
import {DbFile} from '../../../test/lite.ts';
import {postgresTypeConfig} from '../../../types/pg.ts';
import {initReplica, initReplicaWithWorker} from '../common/replica-schema.ts';
import {initialSync} from './initial-sync.ts';
import {dropShard} from './schema/shard.ts';

const APP_ID = 'benchsync';
const SHARD_NUM = 0;
const TEST_CONTEXT = {benchmark: true};

const ROW_COUNTS = {
  wide_table: 100_000,
  narrow_table: 200_000,
  medium_table: 100_000,
};

const TOTAL_ROWS = Object.values(ROW_COUNTS).reduce((a, b) => a + b, 0);

describe('initial-sync worker benchmark', {timeout: 300_000}, () => {
  let container: StartedPostgreSqlContainer;
  let connectionURI: string;

  beforeAll(async () => {
    console.log('Starting PG container with NET_ADMIN...');
    container = await new PostgreSqlContainer('postgres:17')
      .withAddedCapabilities('NET_ADMIN')
      .withCommand([
        'postgres',
        '-c',
        'wal_level=logical',
        '-c',
        'max_replication_slots=100',
        '-c',
        'max_wal_senders=100',
      ])
      .start();
    connectionURI = container.getConnectionUri();

    // Install iproute2 for tc netem
    const install = await container.exec([
      'sh',
      '-c',
      'apt-get update -qq && apt-get install -y -qq iproute2 > /dev/null 2>&1',
    ]);
    assert(
      install.exitCode === 0,
      `iproute2 install failed (exit ${install.exitCode}): ${install.stderr}`,
    );
    console.log('PG container ready with tc netem support');
  }, 60_000);

  afterAll(async () => {
    await container?.stop();
  });

  for (const latencyMs of [0, 1, 2]) {
    test(`benchmark: latency=${latencyMs}ms`, async () => {
      const lc = createSilentLogContext();

      // ── Setup: create database and insert test data (no latency) ────
      const dbName = `bench_${latencyMs}ms`;
      const admin = postgres(connectionURI, postgresTypeConfig());
      await admin`DROP DATABASE IF EXISTS ${admin(dbName)}`;
      await admin`CREATE DATABASE ${admin(dbName)}`;
      await admin.end();

      const {
        host,
        port,
        user: username,
        password: pass,
      } = container.getConnectionUri()
        ? (() => {
            const url = new URL(connectionURI);
            return {
              host: url.hostname,
              port: Number(url.port),
              user: url.username,
              password: url.password,
            };
          })()
        : {host: '', port: 0, user: '', password: ''};

      const upstream = postgres({
        host,
        port,
        username,
        password: pass,
        database: dbName,
        ...postgresTypeConfig(),
      });

      const uri = `postgres://${username}:${pass}@${host}:${port}/${dbName}`;

      await upstream.unsafe(`
        CREATE TABLE wide_table (
          id int4 PRIMARY KEY,
          label text NOT NULL,
          amount float8 NOT NULL,
          flag bool NOT NULL,
          ts timestamp NOT NULL,
          meta jsonb NOT NULL,
          bin bytea NOT NULL,
          tag varchar(100) NOT NULL
        );
        CREATE TABLE narrow_table (
          id int4 PRIMARY KEY,
          value text NOT NULL
        );
        CREATE TABLE medium_table (
          id int4 PRIMARY KEY,
          name text NOT NULL,
          amount float8 NOT NULL,
          data jsonb NOT NULL
        );
      `);

      console.log(
        `[${latencyMs}ms] Inserting ${TOTAL_ROWS.toLocaleString()} rows...`,
      );
      const insertStart = performance.now();

      await upstream.unsafe(`
        INSERT INTO wide_table SELECT
          i, 'text_' || i, i * 1.5, i % 2 = 0,
          '2025-01-01'::timestamp + (i || ' seconds')::interval,
          json_build_object('key', i, 'nested', json_build_object('a', i % 100)),
          decode(lpad(to_hex(i), 8, '0'), 'hex'),
          'tag_' || (i % 1000)
        FROM generate_series(1, ${ROW_COUNTS.wide_table}) AS i
      `);
      await upstream.unsafe(`
        INSERT INTO narrow_table SELECT i, 'value_' || i
        FROM generate_series(1, ${ROW_COUNTS.narrow_table}) AS i
      `);
      await upstream.unsafe(`
        INSERT INTO medium_table SELECT
          i, 'name_' || i, i * 2.5,
          json_build_object('id', i, 'tags', json_build_array(i % 10, i % 20))
        FROM generate_series(1, ${ROW_COUNTS.medium_table}) AS i
      `);

      console.log(
        `[${latencyMs}ms] Inserted in ${(performance.now() - insertStart).toFixed(0)} ms`,
      );

      // ── Apply latency AFTER data insertion ──────────────────────
      if (latencyMs > 0) {
        const tc = await container.exec([
          'tc',
          'qdisc',
          'replace',
          'dev',
          'eth0',
          'root',
          'netem',
          'delay',
          `${latencyMs}ms`,
        ]);
        assert(
          tc.exitCode === 0,
          `tc netem failed (exit ${tc.exitCode}): ${tc.stderr}`,
        );
        console.log(`[${latencyMs}ms] Applied ${latencyMs}ms network latency`);
      }

      const shard = {appID: APP_ID, shardNum: SHARD_NUM, publications: []};
      const syncOptions = {tableCopyWorkers: 3};

      try {
        // ── Direct path ─────────────────────────────────────────────
        const directFile = new DbFile(`bench_direct_${latencyMs}`);
        const directStart = performance.now();

        await initReplica(lc, 'bench-direct', directFile.path, (log, tx) =>
          initialSync(log, shard, tx, uri, syncOptions, TEST_CONTEXT),
        );

        const directElapsed = performance.now() - directStart;

        const directDb = new Database(lc, directFile.path);
        for (const [table, expected] of Object.entries(ROW_COUNTS)) {
          const [{count}] = directDb
            .prepare(`SELECT COUNT(*) as count FROM "${table}"`)
            .all<{count: number}>();
          expect(count).toBe(expected);
        }
        const directTableCount = listTables(directDb, false, false).length;
        directDb.close();
        directFile.delete();

        // ── Cleanup between runs ────────────────────────────────────
        // Drop replication slots for this database
        for (let i = 0; i < 10; i++) {
          const slots = await upstream<{slotName: string; active: boolean}[]>`
            SELECT slot_name as "slotName", active
            FROM pg_replication_slots WHERE database = ${dbName}`;
          if (slots.length === 0) break;
          for (const s of slots) {
            if (!s.active) {
              await upstream`SELECT pg_drop_replication_slot(${s.slotName})`.catch(
                () => {},
              );
            }
          }
        }
        await upstream.unsafe(dropShard(APP_ID, SHARD_NUM));

        // ── Worker path ─────────────────────────────────────────────
        const workerFile = new DbFile(`bench_worker_${latencyMs}`);
        const workerStart = performance.now();

        await initReplicaWithWorker(
          lc,
          'bench-worker',
          workerFile.path,
          uri,
          (log, writer) =>
            initialSync(log, shard, writer, uri, syncOptions, TEST_CONTEXT),
          {level: 'info', format: 'text'},
        );

        const workerElapsed = performance.now() - workerStart;

        const workerDb = new Database(lc, workerFile.path);
        for (const [table, expected] of Object.entries(ROW_COUNTS)) {
          const [{count}] = workerDb
            .prepare(`SELECT COUNT(*) as count FROM "${table}"`)
            .all<{count: number}>();
          expect(count).toBe(expected);
        }
        const workerTableCount = listTables(workerDb, false, false).length;
        workerDb.close();
        workerFile.delete();

        expect(directTableCount).toBe(workerTableCount);

        // ── Results ─────────────────────────────────────────────────
        const speedup = directElapsed / workerElapsed;
        console.log('');
        console.log(
          `┌─ Latency: ${String(latencyMs).padEnd(3)}ms ──────────────────────────────┐`,
        );
        console.log(
          `│   Direct: ${directElapsed.toFixed(0).padStart(8)} ms                     │`,
        );
        console.log(
          `│   Worker: ${workerElapsed.toFixed(0).padStart(8)} ms                     │`,
        );
        console.log(
          `│   Speedup: ${speedup.toFixed(2).padStart(7)}x                      │`,
        );
        console.log(`└───────────────────────────────────────────────┘`);
        console.log('');
      } finally {
        // ── Remove latency ──────────────────────────────────────────
        if (latencyMs > 0) {
          await container
            .exec(['tc', 'qdisc', 'del', 'dev', 'eth0', 'root'])
            .catch(() => {});
        }
        await upstream.end();
      }
    });
  }
});
