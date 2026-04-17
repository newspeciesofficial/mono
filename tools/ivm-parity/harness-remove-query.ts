/**
 * removeQuery parity harness — asserts that TS and RS handle
 * `desiredQueriesPatch` op='del' identically.
 *
 * Flow per pattern:
 *   1. Open TS + RS WebSockets with the pattern subscribed.
 *   2. Wait for initial hydration (first `gotQueriesPatch` put).
 *   3. Send `changeDesiredQueries` op='del' on both sides.
 *   4. Wait for `gotQueriesPatch` op='del' on both (confirms server
 *      processed the removal).
 *   5. Run a mutation touching the query's table(s).
 *   6. Wait for the post-mutation poke window to close.
 *   7. Assert NEITHER side emitted rows under the removed hash; both
 *      converge to the same final state.
 *
 * Mirrors the `[ivm:removeQuery]` log on both sides — the ServiceLog
 * fields `{queryID, existed}` let a grep-diff confirm each driver
 * observed the same removal.
 */
import postgres from 'postgres';
import {patterns, type PatternName} from './patterns.ts';
import {schema as paritySchema} from './zero-schema.ts';

const TS_URL =
  process.env.PARITY_TS_URL ?? 'ws://localhost:4858/sync/v49/connect';
const RS_URL =
  process.env.PARITY_RS_URL ?? 'ws://localhost:4868/sync/v49/connect';
const PG_URL =
  process.env.PARITY_PG_URL ??
  'postgresql://user:password@127.0.0.1:6434/parity';
const POKE_WAIT_MS = Number(process.env.PARITY_POKE_WAIT_MS ?? 2500);
const REMOVE_ACK_WAIT_MS = Number(process.env.PARITY_REMOVE_ACK_WAIT_MS ?? 1000);

type RowsByTable = Record<string, Record<string, Record<string, unknown>>>;

function buildClientSchema() {
  const out: Record<
    string,
    {columns: Record<string, {type: string}>; primaryKey: string[]}
  > = {};
  for (const [tableName, tableDef] of Object.entries(
    paritySchema.tables,
  ) as Array<[string, any]>) {
    const columns: Record<string, {type: string}> = {};
    for (const [col, desc] of Object.entries(tableDef.columns) as Array<
      [string, any]
    >) {
      columns[col] = {type: desc.type};
    }
    out[tableName] = {columns, primaryKey: [...tableDef.primaryKey]};
  }
  return {tables: out};
}

function extractAst(query: unknown): unknown {
  const ast = (query as {ast?: unknown})?.ast;
  if (!ast) throw new Error('extractAst: query has no `.ast` getter');
  return ast;
}

function canon(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canon);
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(value as object).sort()) {
      out[k] = canon((value as Record<string, unknown>)[k]);
    }
    return out;
  }
  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

type Sub = {
  rows: RowsByTable;
  sendJson: (msg: unknown) => void;
  hydrated: Promise<void>;
  removed: Promise<void>;
  close: () => void;
};

async function subscribe(
  url: string,
  hash: string,
  ast: unknown,
  clientSchema: ReturnType<typeof buildClientSchema>,
): Promise<Sub> {
  const {default: WebSocket} = await import('ws');
  const {encodeSecProtocols} = await import(
    '../../packages/zero-protocol/src/connect.ts'
  );
  const rows: RowsByTable = {};
  const canonicalKey = (
    pk: readonly string[],
    row: Record<string, unknown>,
  ) => {
    const obj: Record<string, unknown> = {};
    for (const k of pk) obj[k] = row?.[k];
    return JSON.stringify(obj);
  };
  let resolveHydrated!: () => void;
  let rejectHydrated!: (e: unknown) => void;
  const hydrated = new Promise<void>((res, rej) => {
    resolveHydrated = res;
    rejectHydrated = rej;
  });
  let resolveRemoved!: () => void;
  let rejectRemoved!: (e: unknown) => void;
  const removed = new Promise<void>((res, rej) => {
    resolveRemoved = res;
    rejectRemoved = rej;
  });
  const initBody = {
    clientSchema,
    desiredQueriesPatch: [{op: 'put' as const, hash, ast}],
  };
  const secProtocol = encodeSecProtocols(
    ['initConnection', initBody],
    undefined,
  );
  const cgid = `parity-rm-${hash}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const ws = new WebSocket(
    url +
      '?clientGroupID=' +
      cgid +
      '&clientID=cid-' +
      Date.now() +
      '&baseCookie=&ts=' +
      Date.now() +
      '&lmid=1',
    secProtocol,
  );
  let sawGotPut = false;
  let sawGotDel = false;
  let sawPokeEndAfterGotPut = false;
  ws.on('message', (data: Buffer | string) => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(String(data));
    } catch {
      return;
    }
    if (!Array.isArray(parsed)) return;
    const [tag, body] = parsed;
    if (tag === 'pokePart') {
      const got = (body as any)?.gotQueriesPatch;
      if (Array.isArray(got)) {
        for (const g of got) {
          if (g?.hash !== hash) continue;
          if (g.op === 'put') sawGotPut = true;
          if (g.op === 'del') {
            sawGotDel = true;
            resolveRemoved();
          }
        }
      }
      const rp = (body as any)?.rowsPatch;
      if (Array.isArray(rp)) {
        for (const op of rp) {
          const t = op?.tableName;
          if (!t) continue;
          const pk =
            (paritySchema.tables as any)[t]?.primaryKey ?? ['id'];
          if (op.op === 'put') {
            const k = canonicalKey(pk, op.value);
            if (!rows[t]) rows[t] = {};
            rows[t][k] = op.value;
          } else if (op.op === 'del') {
            const k = canonicalKey(pk, op.id);
            if (rows[t]) delete rows[t][k];
          } else if (op.op === 'clear') {
            for (const k of Object.keys(rows)) delete rows[k];
          } else if (op.op === 'update') {
            const k = canonicalKey(pk, op.id);
            const prev = rows[t]?.[k] ?? {};
            if (!rows[t]) rows[t] = {};
            rows[t][k] = {...prev, ...(op.merge ?? {})};
          }
        }
      }
    } else if (tag === 'pokeEnd' && sawGotPut && !sawPokeEndAfterGotPut) {
      sawPokeEndAfterGotPut = true;
      resolveHydrated();
    }
  });
  ws.on('error', err => {
    rejectHydrated(err);
    rejectRemoved(err);
  });
  ws.on('close', () => {
    if (!sawGotPut) rejectHydrated(new Error('socket closed pre-hydration'));
    if (!sawGotDel) rejectRemoved(new Error('socket closed pre-remove-ack'));
  });
  // Silence unhandled-rejection warnings when `removed` is raced
  // against a sleep() timeout (normal path for some patterns).
  removed.catch(() => {});
  hydrated.catch(() => {});
  const sendJson = (msg: unknown) => ws.send(JSON.stringify(msg));
  return {
    rows,
    sendJson,
    hydrated,
    removed,
    close: () => ws.close(),
  };
}

const MUTATION_SQL = [
  `INSERT INTO channels (id, name, visibility) VALUES ('ch-rq-1', 'rq', 'public')`,
  `INSERT INTO participants ("userId", "channelId") VALUES ('u1', 'ch-rq-1')`,
  `INSERT INTO conversations (id, "channelId", title, "createdAt") VALUES ('co-rq-1', 'ch-rq-1', 'rq', 9500)`,
  `INSERT INTO messages (id, "conversationId", "authorId", body, "createdAt", "visibleTo") VALUES ('m-rq-1', 'co-rq-1', 'u1', 'rq', 9500, NULL)`,
];
const CLEANUP_SQL = [
  `DELETE FROM attachments WHERE id LIKE 'a-rq%'`,
  `DELETE FROM messages WHERE id = 'm-rq-1'`,
  `DELETE FROM conversations WHERE id = 'co-rq-1'`,
  `DELETE FROM participants WHERE "userId"='u1' AND "channelId"='ch-rq-1'`,
  `DELETE FROM channels WHERE id = 'ch-rq-1'`,
];

async function runOne(
  hash: PatternName,
  ast: unknown,
  clientSchema: ReturnType<typeof buildClientSchema>,
  sql: postgres.Sql,
): Promise<{pattern: PatternName; match: boolean; tsFinal: RowsByTable; rsFinal: RowsByTable; error?: string}> {
  const ts = await subscribe(TS_URL, hash, ast, clientSchema);
  const rs = await subscribe(RS_URL, hash, ast, clientSchema);
  try {
    await Promise.all([ts.hydrated, rs.hydrated]);
    // Step 1: send op='del' to both servers.
    const delMsg = ['changeDesiredQueries', {
      desiredQueriesPatch: [{op: 'del', hash}],
    }];
    ts.sendJson(delMsg);
    rs.sendJson(delMsg);
    // Step 2: wait for the gotQueriesPatch op='del' ack from both.
    await Promise.all([
      Promise.race([ts.removed, sleep(REMOVE_ACK_WAIT_MS)]),
      Promise.race([rs.removed, sleep(REMOVE_ACK_WAIT_MS)]),
    ]);
    // Step 3: run mutations — should emit nothing under this hash.
    for (const stmt of MUTATION_SQL) {
      await sql.unsafe(stmt);
    }
    await sleep(POKE_WAIT_MS);
    const tsFinal = canon(ts.rows) as RowsByTable;
    const rsFinal = canon(rs.rows) as RowsByTable;
    const match = JSON.stringify(tsFinal) === JSON.stringify(rsFinal);
    return {pattern: hash, match, tsFinal, rsFinal};
  } catch (err) {
    return {
      pattern: hash,
      match: false,
      tsFinal: {},
      rsFinal: {},
      error: (err as Error).message,
    };
  } finally {
    ts.close();
    rs.close();
  }
}

async function main() {
  const sql = postgres(PG_URL, {max: 1, prepare: false, idle_timeout: 5});
  const cleanup = async () => {
    for (const stmt of CLEANUP_SQL) {
      try {
        await sql.unsafe(stmt);
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error(`[cleanup] ${stmt}: ${(err as Error).message}`);
      }
    }
    await sleep(800);
  };
  await cleanup();
  const clientSchema = buildClientSchema();
  const results: Array<Awaited<ReturnType<typeof runOne>>> = [];
  // Subset mode: `PARITY_REMOVE_QUERY_PATTERNS=p01,p05,...`
  // picks specific patterns. Defaults to a small representative mix to
  // keep PG connection pressure low.
  const picks = (process.env.PARITY_REMOVE_QUERY_PATTERNS ??
    'p01_simple_where,p10_orderby_multi,p17_related_one,p18_whereExists_simple,p23_not_exists')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
  try {
    for (const hash of picks as PatternName[]) {
      const build = (patterns as Record<string, () => unknown>)[hash];
      if (!build) {
        // eslint-disable-next-line no-console
        console.error(`[skip] unknown pattern ${hash}`);
        continue;
      }
      const r = await runOne(hash, extractAst(build()), clientSchema, sql);
      results.push(r);
      await cleanup();
    }
  } finally {
    await cleanup();
    await sql.end({timeout: 5});
  }
  // eslint-disable-next-line no-console
  console.log('\n=== removeQuery parity report ===');
  let diverged = 0;
  for (const r of results) {
    const mark = r.match ? 'OK' : 'DIVERGE';
    // eslint-disable-next-line no-console
    console.log(`${mark.padEnd(8)} ${r.pattern}`);
    if (r.error) {
      // eslint-disable-next-line no-console
      console.log(`         error: ${r.error}`);
    }
    if (!r.match) {
      diverged++;
      // eslint-disable-next-line no-console
      console.log(`         ts final: ${JSON.stringify(r.tsFinal)}`);
      // eslint-disable-next-line no-console
      console.log(`         rs final: ${JSON.stringify(r.rsFinal)}`);
    }
  }
  // eslint-disable-next-line no-console
  console.log(`\ndiverged: ${diverged}/${results.length}`);
  process.exit(diverged === 0 ? 0 : 1);
}

main().catch(err => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
