import '../../../packages/shared/src/dotenv.ts';

import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid';
import * as fs from 'node:fs';
import {pipeline, Writable} from 'node:stream';
import WebSocket, {createWebSocketStream} from 'ws';
import {parseOptions} from '../../../packages/shared/src/options.ts';
import * as v from '../../../packages/shared/src/valita.ts';
import {
  encodeSecProtocols,
  initConnectionMessageSchema,
} from '../../../packages/zero-protocol/src/connect.ts';
import {downstreamSchema} from '../../../packages/zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../packages/zero-protocol/src/protocol-version.ts';
import initConnectionJSON from './init-connection.json' with {type: 'json'};

const options = {
  viewSyncers: {type: v.array(v.string())},

  numConnections: {type: v.number().default(1)},

  millisBetweenMessages: {type: v.number().optional()},

  // Absolute path to a JSON file containing an initConnection message,
  // captured from a real client. Overrides the bundled zbugs example so the
  // simulator can target other schemas (e.g. xyne-spaces-login).
  initConnectionFile: {type: v.string().optional()},

  // Path prefix appended to the view syncer URL before the `/sync/` suffix.
  // Empty string = hit `/sync/...` directly (zbugs). For xyne the frontend
  // connects via `/zero/sync/...`, so pass `/zero`.
  pathPrefix: {type: v.string().default('')},

  // Extra `key=value` query parameters appended to the connect URL. Repeat
  // the flag for multiple (e.g. `--extra-query-params userID=foo
  // --extra-query-params profileID=bar`). Required by xyne's auth layer
  // which expects `userID` and `profileID` in the URL.
  extraQueryParams: {type: v.array(v.string()).optional()},

  // When true, open the WebSocket with a Sec-WebSocket-Protocol header that
  // carries only the auth token (no initConnection), and send the
  // initConnection frame *after* the socket opens. The real client falls
  // back to this when the encoded header would exceed `maxHeaderLength`.
  // Required for xyne — its captured initConnection is ~69KB and exceeds
  // typical header size limits.
  postHandshakeInitConnection: {type: v.boolean().default(false)},

  // Auth token (JWT) — encoded into the Sec-WebSocket-Protocol header via
  // encodeSecProtocols, same as the real client. Used by clients that pass
  // an explicit auth function (e.g. zbugs). xyne-spaces-login uses cookies
  // instead; leave this unset and pass `--cookie` / `ZERO_COOKIE` instead.
  authToken: {type: v.string().optional()},

  // Cookie header value to attach to the WebSocket HTTP upgrade request.
  // xyne-spaces-login relies on the browser sending session cookies via
  // the upgrade request, and its zero-cache validates them server-side.
  // Example: `google_access_token=eyJhbGciOi...; user_session_id=cmnr...`.
  cookie: {type: v.string().optional()},
};

function run() {
  const lc = new LogContext('debug', {}, consoleLogSink);
  const {
    viewSyncers,
    numConnections,
    millisBetweenMessages,
    initConnectionFile,
    pathPrefix,
    extraQueryParams,
    postHandshakeInitConnection,
    authToken,
    cookie,
  } = parseOptions(options, {envNamePrefix: 'ZERO_'});

  const rawInitConnection: unknown = initConnectionFile
    ? JSON.parse(fs.readFileSync(initConnectionFile, 'utf8'))
    : initConnectionJSON;
  lc.info?.(
    `using init-connection from ${
      initConnectionFile ?? 'bundled zbugs example'
    }`,
  );
  const initConnectionMessage = v.parse(
    rawInitConnection,
    initConnectionMessageSchema,
  );

  const extraParams = (extraQueryParams ?? []).map(pair => {
    const eq = pair.indexOf('=');
    if (eq < 0) {
      throw new Error(
        `--extra-query-params expects key=value, got ${JSON.stringify(pair)}`,
      );
    }
    return [pair.slice(0, eq), pair.slice(eq + 1)] as const;
  });

  let pokesReceived = 0;
  let msgsReceived = 0;
  let connectionsOpen = 0;
  const clients: WebSocket[] = [];
  for (const vs of viewSyncers) {
    for (let i = 0; i < numConnections; i++) {
      const params = new URLSearchParams({
        clientGroupID: nanoid(10),
        clientID: nanoid(10),
        baseCookie: '',
        ts: String(performance.now()),
        lmid: '1',
      });
      for (const [k, v2] of extraParams) {
        params.set(k, v2);
      }
      const url = `${vs}${pathPrefix}/sync/v${PROTOCOL_VERSION}/connect?${params.toString()}`;
      const secProtocol = postHandshakeInitConnection
        ? encodeSecProtocols(undefined, authToken)
        : encodeSecProtocols(
            ['initConnection', initConnectionMessage[1]],
            authToken,
          );
      const wsOptions: WebSocket.ClientOptions = {};
      if (cookie) {
        wsOptions.headers = {Cookie: cookie};
      }
      const ws = new WebSocket(url, secProtocol, wsOptions);
      if (postHandshakeInitConnection) {
        ws.on('open', () => {
          connectionsOpen++;
          ws.send(JSON.stringify(initConnectionMessage));
        });
      } else {
        ws.on('open', () => {
          connectionsOpen++;
        });
      }
      lc.debug?.(`connecting to ${url}`);
      const stream = createWebSocketStream(ws);
      stream.on('error', err => lc.error?.(err));
      stream.on('close', () => lc.debug?.(`connection to ${url} closed`));
      pipeline(
        stream,
        new Writable({
          write: (data, _encoding, callback) => {
            try {
              const message = v.parse(
                JSON.parse(data.toString()),
                downstreamSchema,
              );
              const type = message[0];
              msgsReceived++;
              switch (type) {
                case 'error':
                  lc.error?.(message);
                  break;
                case 'pokeEnd':
                  pokesReceived++;
                  break;
              }
              if (millisBetweenMessages === undefined) {
                callback();
              } else {
                setTimeout(callback, millisBetweenMessages);
              }
            } catch (err) {
              callback(err instanceof Error ? err : new Error(String(err)));
            }
          },
        }),
        () => {},
      );
      clients.push(ws);
    }
  }

  lc.info?.('');
  function logStatus() {
    process.stdout.write(
      `\rOPEN:\t${connectionsOpen}/${clients.length}\tPOKES:\t${pokesReceived}\tMESSAGES:\t${msgsReceived}`,
    );
  }
  const statusUpdater = setInterval(logStatus, 1000);

  process.on('SIGINT', () => {
    clients.forEach(ws => ws.close());
    clearInterval(statusUpdater);
  });
}

run();
