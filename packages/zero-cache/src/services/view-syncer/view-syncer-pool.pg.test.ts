/**
 * Runs a subset of view-syncer tests with RemotePipelineDriver + pool worker
 * threads. This validates the pool thread communication path end-to-end.
 */
import {afterEach, expect, vi} from 'vitest';
import type {Queue} from '../../../../shared/src/queue.ts';
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
  ISSUES_QUERY,
  messages,
  nextPoke,
  permissionsAll,
  setup,
  USERS_QUERY,
} from './view-syncer-test-util.ts';
import type {ViewSyncerService} from './view-syncer.ts';
import {type SyncContext} from './view-syncer.ts';

afterEach(() => {
  vi.restoreAllMocks();
});

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
    'view_syncer_pool_test',
    permissionsAll,
    undefined,
    true, // usePoolThreads
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

test('pool-threads: hydration and initial poke', async () => {
  replicator.processTransaction(
    '01',
    messages.insert('issues', {id: 'i1', owner: 'u1', title: 'Bug 1'}),
    messages.insert('issues', {id: 'i2', owner: 'u2', title: 'Bug 2'}),
  );
  stateChanges.push({state: 'version-ready'});

  const queue = connect(SYNC_CONTEXT, [
    {op: 'put', hash: 'h1', ast: ISSUES_QUERY},
  ]);

  const poke = await nextPoke(queue);
  // nextPoke returns an array of messages. Verify we got poke data.
  expect(poke.length).toBeGreaterThan(0);
});

test('pool-threads: advance produces delta', async () => {
  replicator.processTransaction(
    '01',
    messages.insert('issues', {id: 'i1', owner: 'u1', title: 'Bug 1'}),
  );
  stateChanges.push({state: 'version-ready'});

  const queue = connect(SYNC_CONTEXT, [
    {op: 'put', hash: 'h1', ast: ISSUES_QUERY},
  ]);
  await nextPoke(queue); // consume hydration poke

  // Now make a change and advance.
  replicator.processTransaction(
    '02',
    messages.update('issues', {
      id: 'i1',
      owner: 'u1',
      title: 'Bug 1 (edited)',
    }),
  );
  stateChanges.push({state: 'version-ready'});

  const poke = await nextPoke(queue);
  expect(poke.length).toBeGreaterThan(0);
});

test('pool-threads: multiple queries across threads', async () => {
  replicator.processTransaction(
    '01',
    messages.insert('issues', {id: 'i1', owner: 'u1', title: 'Bug'}),
    messages.insert('users', {id: 'u1', name: 'Alice'}),
  );
  stateChanges.push({state: 'version-ready'});

  const queue = connect(SYNC_CONTEXT, [
    {op: 'put', hash: 'h1', ast: ISSUES_QUERY},
    {op: 'put', hash: 'h2', ast: USERS_QUERY},
  ]);

  const poke = await nextPoke(queue);
  expect(poke.length).toBeGreaterThan(0);
});
