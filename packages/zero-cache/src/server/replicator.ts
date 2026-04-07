import {existsSync} from 'node:fs';
import {pid} from 'node:process';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import * as v from '../../../shared/src/valita.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {initEventSink} from '../observability/events.ts';
import {ChangeStreamerHttpClient} from '../services/change-streamer/change-streamer-http.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {
  ReplicatorService,
  type ReplicatorMode,
} from '../services/replicator/replicator.ts';
import {ThreadWriteWorkerClient} from '../services/replicator/write-worker-client.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardConfig} from '../types/shards.ts';
import {
  getPragmaConfig,
  replicaFileModeSchema,
  setUpMessageHandlers,
  setupReplica,
} from '../workers/replicator.ts';
import {createLogContext} from './logging.ts';

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  assert(args.length > 0, `replicator mode not specified`);
  const fileMode = v.parse(args[0], replicaFileModeSchema);
  const shardIndex =
    args.length > 1 && args[1].startsWith('--shard-index=')
      ? Number(args[1].split('=')[1])
      : undefined;
  const remainingArgs =
    shardIndex !== undefined ? args.slice(2) : args.slice(1);

  const config = getNormalizedZeroConfig({env, argv: remainingArgs});
  const mode: ReplicatorMode = fileMode === 'backup' ? 'backup' : 'serving';
  const shardSuffix = shardIndex !== undefined ? `-${shardIndex}` : '';
  const workerName = `${mode}-replicator${shardSuffix}`;
  const lc = createLogContext(config, {worker: workerName});
  initEventSink(lc, config);

  // Override the base replica file with shard suffix (NOT mode suffix).
  // setupReplica handles mode-specific naming (e.g. serving-copy).
  const baseFile =
    shardIndex !== undefined && shardIndex > 0
      ? `${config.replica.file}-shard-${shardIndex}`
      : config.replica.file;

  // For shard replicas (index > 0), the shard file may not exist yet.
  // Copy from the base replica (created by the change-streamer's initial sync)
  // so that upgradeReplica has a valid starting point.
  // We must checkpoint the source WAL first so that VACUUM INTO captures
  // all committed data (including _zero.replicationState metadata).
  if (shardIndex !== undefined && shardIndex > 0 && !existsSync(baseFile)) {
    const sourceFile = config.replica.file;
    lc.info?.(
      `shard file ${baseFile} does not exist, copying from ${sourceFile}`,
    );
    const source = new Database(lc, sourceFile);
    source.pragma('wal_checkpoint(TRUNCATE)');
    source.prepare('VACUUM INTO ?').run(baseFile);
    source.close();
    lc.info?.(`created shard file ${baseFile}`);
  }

  const replicaOptions = {...config.replica, file: baseFile};
  const replica = await setupReplica(lc, fileMode, replicaOptions);

  // Create the write worker for async SQLite writes.
  const dbPath = replica.name;
  const pragmas = getPragmaConfig(fileMode);
  const workerClient = new ThreadWriteWorkerClient();
  await workerClient.init(dbPath, mode, pragmas, config.log);

  const runningLocalChangeStreamer =
    config.changeStreamer.mode === 'dedicated' && !config.changeStreamer.uri;
  const shard = getShardConfig(config);
  const {
    taskID,
    change,
    changeStreamer: {
      port,
      uri: changeStreamerURI = runningLocalChangeStreamer
        ? `http://localhost:${port}/`
        : undefined,
    },
  } = config;
  const changeStreamer = new ChangeStreamerHttpClient(
    lc,
    shard,
    change.db,
    changeStreamerURI,
  );

  const replicator = new ReplicatorService(
    lc,
    taskID,
    `${workerName}-${pid}`,
    mode,
    changeStreamer,
    replica,
    workerClient,
    runningLocalChangeStreamer, // publish ReplicationStatusEvents
  );

  setUpMessageHandlers(lc, replicator, parent);

  const running = runUntilKilled(lc, parent, replicator);

  // Signal readiness once the first ReplicaVersionReady notification is received.
  for await (const _ of replicator.subscribe()) {
    parent.send(['ready', {ready: true}]);
    break;
  }

  return running;
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
