import type {LogContext} from '@rocicorp/logger';
import {createServer} from 'node:net';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type * as pg from '../../types/pg.ts';
import {type ShardID} from '../../types/shards.ts';

vi.mock('../../types/pg.ts', async () => {
  const actual = await vi.importActual<typeof pg>('../../types/pg.ts');
  return {
    ...actual,
    pgClient: vi.fn(() => ({})),
  };
});

import {ChangeStreamerHttpClient} from './change-streamer-http.ts';
import {PROTOCOL_VERSION} from './change-streamer.ts';

const SHARD_ID = {
  appID: 'foo',
  shardNum: 123,
} satisfies ShardID;

const SNAPSHOT_PATH = `/replication/v${PROTOCOL_VERSION}/snapshot`;
const CHANGES_PATH = `/replication/v${PROTOCOL_VERSION}/changes`;

describe('change-streamer/http client', () => {
  let lc: LogContext;

  beforeEach(() => {
    lc = createSilentLogContext();
  });

  async function getClosedPort(): Promise<number> {
    const server = createServer();
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', () => resolve());
    });

    const address = server.address();
    if (!address || typeof address === 'string') {
      throw new Error('expected TCP address');
    }

    await new Promise<void>((resolve, reject) => {
      server.close(err => (err ? reject(err) : resolve()));
    });

    return address.port;
  }

  test.each([
    [
      'snapshot',
      SNAPSHOT_PATH,
      (client: ChangeStreamerHttpClient) => client.reserveSnapshot('foo-task'),
    ],
    [
      'changes',
      CHANGES_PATH,
      (client: ChangeStreamerHttpClient) =>
        client.subscribe({
          protocolVersion: PROTOCOL_VERSION,
          taskID: 'foo-task',
          id: 'foo',
          mode: 'serving',
          replicaVersion: 'abc',
          watermark: '123',
          initial: true,
        }),
    ],
  ])('wraps initial %s connection failures', async (_name, path, connect) => {
    const port = await getClosedPort();
    const client = new ChangeStreamerHttpClient(
      lc,
      SHARD_ID,
      'postgres://unused',
      `http://127.0.0.1:${port}`,
    );

    await expect(connect(client)).rejects.toMatchObject({
      message:
        `Unable to connect to change-streamer at ` +
        `http://127.0.0.1:${port}${path}; it may still be starting, ` +
        `restarting, or restoring from backup`,
      cause: expect.objectContaining({
        code: 'ECONNREFUSED',
      }),
    });
  });
});
