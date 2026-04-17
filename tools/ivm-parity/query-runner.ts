/**
 * Sends an `initConnection` carrying one or more test ASTs to a
 * running zero-cache, counts the rows the server returns in the
 * subsequent poke stream, and prints a per-query summary.
 *
 * Run against the ivm-parity zero-cache:
 *   tsx query-runner.ts \
 *     --view-syncer ws://localhost:4858 \
 *     --queries queries/01-flip-attachments.json \
 *     --queries queries/02-conversation-messages.json
 *
 * Not a general-purpose client — no auth, no permissions, no CVR
 * reconciliation. Just a row-count probe for parity testing between
 * `ZERO_USE_RUST_IVM_V2=1` and TS-native zero-cache runs.
 */
import * as fs from 'node:fs';
import * as path from 'node:path';
import WebSocket from 'ws';

type Args = {
  viewSyncer: string;
  queries: string[];
  pathPrefix: string;
  extraParams: Array<[string, string]>;
};

function parseArgs(): Args {
  const argv = process.argv.slice(2);
  const queries: string[] = [];
  const extraParams: Array<[string, string]> = [];
  let viewSyncer = 'ws://localhost:4858';
  let pathPrefix = '';
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--view-syncer') viewSyncer = argv[++i];
    else if (a === '--queries') queries.push(argv[++i]);
    else if (a === '--path-prefix') pathPrefix = argv[++i];
    else if (a === '--param') {
      const [k, v] = argv[++i].split('=');
      extraParams.push([k, v]);
    } else if (a === '--help') {
      console.log(
        '--view-syncer <ws-url> --queries <json-path>... [--path-prefix <p>] [--param key=value ...]',
      );
      process.exit(0);
    }
  }
  if (queries.length === 0) {
    throw new Error('at least one --queries <path> required');
  }
  return {viewSyncer, queries, pathPrefix, extraParams};
}

type InitConn = [
  'initConnection',
  {desiredQueriesPatch: Array<{op: 'put'; hash: string; ast: unknown}>},
];

function loadInit(filePath: string): InitConn {
  const raw = JSON.parse(fs.readFileSync(filePath, 'utf8')) as unknown;
  if (
    !Array.isArray(raw) ||
    raw.length !== 2 ||
    raw[0] !== 'initConnection' ||
    typeof raw[1] !== 'object'
  ) {
    throw new Error(`${filePath}: not a valid initConnection message`);
  }
  return raw as InitConn;
}

function mergeInits(inits: InitConn[]): InitConn {
  const patch: InitConn[1]['desiredQueriesPatch'] = [];
  for (const init of inits) {
    for (const entry of init[1].desiredQueriesPatch) {
      patch.push(entry);
    }
  }
  return ['initConnection', {desiredQueriesPatch: patch}];
}

function buildUrl(base: string, prefix: string, extra: Array<[string, string]>) {
  // Zero client appends `/sync/v{N}/connect` to the base.
  // v21 matches the current protocol; bump if zero-cache rejects.
  const u = new URL(base.replace(/\/$/, '') + prefix + '/sync/v21/connect');
  // Minimal required query params. Any auth that zero-cache checks
  // must be pre-configured on the server side (the ivm-parity setup
  // disables auth in permissions).
  u.searchParams.set('clientGroupID', 'ivm-parity-runner-' + Date.now());
  u.searchParams.set('clientID', 'ivm-parity-runner-cid-' + Date.now());
  u.searchParams.set('schemaVersion', '1');
  u.searchParams.set('baseCookie', '');
  u.searchParams.set('ts', String(Date.now()));
  u.searchParams.set('lmid', '0');
  for (const [k, v] of extra) u.searchParams.set(k, v);
  return u.toString();
}

type Counter = {[hash: string]: {[table: string]: number}};

function tabulate(frame: unknown, counters: Counter) {
  if (!Array.isArray(frame)) return;
  const tag = frame[0];
  if (tag !== 'pokeStart' && tag !== 'pokePart' && tag !== 'pokeEnd') return;
  if (tag !== 'pokePart') return;
  const body = frame[1] as {rowsPatch?: Array<Record<string, unknown>>};
  if (!body || !Array.isArray(body.rowsPatch)) return;
  for (const row of body.rowsPatch) {
    const queryHash = (row['queryHash'] as string) ?? 'unknown';
    const table =
      (row['tableName'] as string) ?? (row['table'] as string) ?? 'unknown';
    counters[queryHash] = counters[queryHash] ?? {};
    counters[queryHash][table] = (counters[queryHash][table] ?? 0) + 1;
  }
}

async function main() {
  const args = parseArgs();
  const inits = args.queries.map(p => loadInit(path.resolve(p)));
  const merged = mergeInits(inits);
  const hashes = merged[1].desiredQueriesPatch.map(e => e.hash);
  console.log(
    `[ivm-parity] sending initConnection with ${hashes.length} queries:`,
    hashes,
  );

  const url = buildUrl(args.viewSyncer, args.pathPrefix, args.extraParams);
  console.log(`[ivm-parity] connecting ${url}`);

  const counters: Counter = {};
  let pokeSeen = 0;

  const ws = new WebSocket(url);
  const done = new Promise<void>((resolve, reject) => {
    ws.on('open', () => {
      console.log('[ivm-parity] open; sending initConnection');
      ws.send(JSON.stringify(merged));
    });
    ws.on('message', data => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(String(data));
      } catch {
        return;
      }
      if (Array.isArray(parsed) && parsed[0] === 'pokeEnd') {
        pokeSeen++;
      }
      tabulate(parsed, counters);
    });
    ws.on('close', () => resolve());
    ws.on('error', err => reject(err));
  });

  // Give the server a generous hydration window.
  const timeout = Number(process.env.PARITY_TIMEOUT_MS ?? 8000);
  setTimeout(() => ws.close(), timeout);

  try {
    await done;
  } catch (e) {
    console.error('[ivm-parity] ws error', e);
  }

  console.log(`[ivm-parity] pokeEnds seen: ${pokeSeen}`);
  for (const hash of hashes) {
    const tables = counters[hash] ?? {};
    const total = Object.values(tables).reduce((a, b) => a + b, 0);
    console.log(
      `[ivm-parity] query ${hash}: total=${total}`,
      JSON.stringify(tables),
    );
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
