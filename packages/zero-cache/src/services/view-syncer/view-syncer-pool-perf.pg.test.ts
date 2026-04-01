/**
 * Performance comparison: local PipelineDriver vs RemotePipelineDriver with pool threads.
 *
 * Measures wall time for:
 * 1. Hydration of N queries
 * 2. Advance after a data change
 *
 * Run with:
 *   DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock \
 *   npm --workspace=zero-cache run test -- --config vitest.config.pg-16.ts view-syncer-pool-perf
 */
import {afterEach, expect, vi} from 'vitest';
import type {Queue} from '../../../../shared/src/queue.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import type {Worker as WorkerThread} from 'node:worker_threads';
import {type PgTest, test} from '../../test/db.ts';
import type {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import type {FakeReplicator} from '../replicator/test-utils.ts';
import {
  messages,
  nextPoke,
  permissionsAll,
  setup,
} from './view-syncer-test-util.ts';
import type {ViewSyncerService} from './view-syncer.ts';
import {type SyncContext} from './view-syncer.ts';

afterEach(() => {
  vi.restoreAllMocks();
});

const SYNC_CONTEXT: SyncContext = {
  clientID: 'foo',
  profileID: 'p0000g00000003203',
  wsID: 'ws1',
  baseCookie: null,
  protocolVersion: PROTOCOL_VERSION,
  httpCookie: undefined,
  origin: undefined,
  userID: 'u1',
};

// Generate N distinct query ASTs. Each query selects ALL issues (no filter)
// so every query has significant IVM work.
function generateQueries(n: number): UpQueriesPatch {
  const queries: UpQueriesPatch = [];
  for (let i = 0; i < n; i++) {
    const ast: AST = {
      table: 'issues',
      orderBy: [['id', 'asc']],
    };
    queries.push({op: 'put', hash: `perf-q${i}`, ast});
  }
  return queries;
}

// Seed a large number of issues so IVM has real work to do.
function seedData(replicator: FakeReplicator, numIssues: number) {
  const BATCH = 500;
  for (let batch = 0; batch < numIssues; batch += BATCH) {
    const ops = [];
    const end = Math.min(batch + BATCH, numIssues);
    for (let i = batch + 1; i <= end; i++) {
      ops.push(
        messages.insert('issues', {
          id: String(i),
          owner: `u${i % 50}`,
          title: `Issue ${i} - ${'x'.repeat(100)}`,
        }),
      );
    }
    replicator.processTransaction(
      String(batch / BATCH + 1).padStart(2, '0'),
      ...ops,
    );
  }
}

let replicaDbFile: DbFile;
let cvrDB: PostgresDB;
let upstreamDb: PostgresDB;
let stateChanges: Subscription<ReplicaState>;
let vs: ViewSyncerService;
let viewSyncerDone: Promise<void>;
let replicator: FakeReplicator;
let poolThreads: WorkerThread[];
let connect: (
  ctx: SyncContext,
  desiredQueriesPatch: UpQueriesPatch,
  clientSchema?: ClientSchema | null,
) => Queue<Downstream>;

const NUM_QUERIES = 30;
const NUM_ISSUES = 2000;

// ---- LOCAL (baseline) ----

test.describe('perf: local PipelineDriver (baseline)', () => {
  test.beforeEach(async ({testDBs}: PgTest) => {
    ({
      replicaDbFile,
      cvrDB,
      upstreamDb,
      stateChanges,
      vs,
      viewSyncerDone,
      replicator,
      connect,
      poolThreads,
    } = await setup(
      testDBs,
      'perf_local',
      permissionsAll,
      undefined,
      false, // local
    ));

    return async () => {
      await vs.stop();
      await viewSyncerDone;
      await testDBs.drop(cvrDB, upstreamDb);
      replicaDbFile.delete();
    };
  });

  test(`hydrate + advance ${NUM_QUERIES} queries (LOCAL)`, async () => {
    seedData(replicator, NUM_ISSUES);
    stateChanges.push({state: 'version-ready'});

    const queries = generateQueries(NUM_QUERIES);

    // Measure hydration
    const hydrationStart = performance.now();
    const queue = connect(SYNC_CONTEXT, queries);
    await nextPoke(queue);
    const hydrationMs = performance.now() - hydrationStart;

    // Mutate multiple rows and measure advance
    replicator.processTransaction(
      'zz',
      messages.update('issues', {id: '1', owner: 'u1', title: 'Updated 1'}),
      messages.update('issues', {id: '2', owner: 'u2', title: 'Updated 2'}),
      messages.update('issues', {id: '3', owner: 'u3', title: 'Updated 3'}),
      messages.update('issues', {id: '10', owner: 'u10', title: 'Updated 10'}),
      messages.update('issues', {id: '50', owner: 'u0', title: 'Updated 50'}),
    );
    stateChanges.push({state: 'version-ready'});

    const advanceStart = performance.now();
    await nextPoke(queue);
    const advanceMs = performance.now() - advanceStart;

    process.stderr.write(
      `\n  *** LOCAL (${NUM_QUERIES}q x ${NUM_ISSUES} rows): Hydration=${hydrationMs.toFixed(1)}ms Advance=${advanceMs.toFixed(1)}ms ***\n`,
    );

    expect(hydrationMs).toBeGreaterThan(0);
    expect(advanceMs).toBeGreaterThan(0);
  });
});

// ---- POOL THREADS ----

test.describe('perf: RemotePipelineDriver (pool threads)', () => {
  test.beforeEach(async ({testDBs}: PgTest) => {
    ({
      replicaDbFile,
      cvrDB,
      upstreamDb,
      stateChanges,
      vs,
      viewSyncerDone,
      replicator,
      connect,
      poolThreads,
    } = await setup(
      testDBs,
      'perf_pool',
      permissionsAll,
      undefined,
      true, // pool threads
    ));

    return async () => {
      await vs.stop();
      await viewSyncerDone;
      for (const t of poolThreads) {
        await t.terminate();
      }
      await testDBs.drop(cvrDB, upstreamDb);
      replicaDbFile.delete();
    };
  });

  test(`hydrate + advance ${NUM_QUERIES} queries (POOL)`, async () => {
    seedData(replicator, NUM_ISSUES);
    stateChanges.push({state: 'version-ready'});

    const queries = generateQueries(NUM_QUERIES);

    // Measure hydration
    const hydrationStart = performance.now();
    const queue = connect(SYNC_CONTEXT, queries);
    await nextPoke(queue);
    const hydrationMs = performance.now() - hydrationStart;

    // Mutate multiple rows and measure advance
    replicator.processTransaction(
      'zz',
      messages.update('issues', {id: '1', owner: 'u1', title: 'Updated 1'}),
      messages.update('issues', {id: '2', owner: 'u2', title: 'Updated 2'}),
      messages.update('issues', {id: '3', owner: 'u3', title: 'Updated 3'}),
      messages.update('issues', {id: '10', owner: 'u10', title: 'Updated 10'}),
      messages.update('issues', {id: '50', owner: 'u0', title: 'Updated 50'}),
    );
    stateChanges.push({state: 'version-ready'});

    const advanceStart = performance.now();
    await nextPoke(queue);
    const advanceMs = performance.now() - advanceStart;

    process.stderr.write(
      `\n  *** POOL (${NUM_QUERIES}q x ${NUM_ISSUES} rows): Hydration=${hydrationMs.toFixed(1)}ms Advance=${advanceMs.toFixed(1)}ms ***\n`,
    );

    expect(hydrationMs).toBeGreaterThan(0);
    expect(advanceMs).toBeGreaterThan(0);
  });
});
