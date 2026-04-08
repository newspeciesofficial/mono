import {pid} from 'node:process';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import * as v from '../../../shared/src/valita.ts';
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

  // Each shard gets its own replica file. Each shard replicator is fully
  // independent — it does its own initial sync from the change-streamer,
  // just like shard 0. No copying between shards.
  const baseFile =
    shardIndex !== undefined && shardIndex > 0
      ? `${config.replica.file}-shard-${shardIndex}`
      : config.replica.file;

  const replicaOptions = {...config.replica, file: baseFile};
  const isNewShard = shardIndex !== undefined && shardIndex > 0;
  const replica = await setupReplica(lc, fileMode, replicaOptions, isNewShard);

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
