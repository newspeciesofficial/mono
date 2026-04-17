/**
 * Advancement parity harness — extends the hydration harness in
 * `harness.ts` to also test the IVM `push()` path through every
 * operator. For each pattern:
 *   1. Open ONE WebSocket on TS (4858) and ONE on RS (4868).
 *   2. Wait for both to receive `gotQueriesPatch` (initial hydration
 *      done — same gate `harness.ts` uses).
 *   3. Snapshot each side's initial row state per table.
 *   4. Run a deterministic mutation block via direct PG against the
 *      shared upstream database. Both zero-cache instances replicate
 *      from the same PG, so both observe identical change events.
 *   5. Wait for the advancement pokes to flow through (replicator →
 *      snapshotter advance → IVM push() → CVR diff → pokeStart/Part/End).
 *   6. Snapshot each side's FINAL row state.
 *   7. Compute deltas (final - initial) and diff TS-delta vs RS-delta.
 *   8. In `finally`: run restore SQL so the seed remains untouched.
 *
 * Why per-pattern instead of all-22-at-once: easier debugging when a
 * specific pattern's push path diverges. Slower (44 connections total)
 * but parity bugs surface with a single pattern's name attached.
 *
 * Why direct PG mutations rather than zero-client mutators: the
 * advancement path under test is replicator → IVM advance → CVR diff
 * → poke. That path is identical whether the change originated from a
 * client mutation or a direct PG write. Direct PG is one library + one
 * SQL statement; client mutators would require mutator definitions, a
 * push processor, and a custom-mutators worker. Same code path tested
 * with a tenth of the scaffolding.
 */
import postgres from 'postgres';
import {patterns, type PatternName} from './patterns.ts';
import {schema as paritySchema} from './zero-schema.ts';

const TS_URL =
  process.env.PARITY_TS_URL ?? 'ws://localhost:4858/sync/v49/connect';
const RS_URL =
  process.env.PARITY_RS_URL ?? 'ws://localhost:4868/sync/v49/connect';
// ivm-parity uses its own database (`parity`) on the local PG instance
// — separate from zbugs's `postgres` database to avoid table-name
// collisions. Both zero-cache instances connect to the same DB so they
// see the same logical replication stream.
const PG_URL =
  process.env.PARITY_PG_URL ??
  'postgresql://user:password@127.0.0.1:6434/parity';
const POKE_WAIT_MS = Number(process.env.PARITY_POKE_WAIT_MS ?? 2500);

/**
 * Mutation block. Every step is `{label, run, undo}` so the test can
 * unwind in `finally` even if a step throws halfway through. All test
 * IDs are suffixed `-test-1` so they're easy to grep in logs and so a
 * stray row left after a crashed run is trivially cleanable.
 *
 * Coverage map (operator → mutation that exercises it):
 *  - Filter::push         insert/update/delete on messages
 *  - Take::push           messages with .one() (p17) — insert flips winner
 *  - Exists::push         insert participants — flips p18 / p23 EXISTS
 *  - FlippedJoin::push    update channel visibility — flips p20 / p21
 *  - Join::push           insert attachments / conversations / messages
 *                         appearing under related[] in p13-p17
 *  - UnionFanIn::push     p21's OR-of-EXISTS — exercised by visibility flip
 *  - Edit propagation     update messages.body — Edit (no PK change)
 */
type Step = {label: string; run: string; undo: string};
const MUTATIONS: Step[] = [
  {
    label: 'insert channel ch-test-1 (public)',
    run: `INSERT INTO channels (id, name, visibility) VALUES ('ch-test-1', 'test-channel', 'public')`,
    undo: `DELETE FROM channels WHERE id = 'ch-test-1'`,
  },
  {
    label: 'add u1 as participant of ch-test-1 (flips Exists for p18)',
    run: `INSERT INTO participants ("userId", "channelId") VALUES ('u1', 'ch-test-1')`,
    undo: `DELETE FROM participants WHERE "userId" = 'u1' AND "channelId" = 'ch-test-1'`,
  },
  {
    label: 'insert conversation co-test-1 in ch-test-1',
    run: `INSERT INTO conversations (id, "channelId", title, "createdAt") VALUES ('co-test-1', 'ch-test-1', 'test conv', 9000)`,
    undo: `DELETE FROM conversations WHERE id = 'co-test-1'`,
  },
  {
    label: 'insert message m-test-1 in co-test-1',
    run: `INSERT INTO messages (id, "conversationId", "authorId", body, "createdAt", "visibleTo") VALUES ('m-test-1', 'co-test-1', 'u1', 'hello-test', 9100, NULL)`,
    undo: `DELETE FROM messages WHERE id = 'm-test-1'`,
  },
  {
    label: 'insert attachment a-test-1 on m-test-1 (Join push)',
    run: `INSERT INTO attachments (id, "messageId", "conversationId", filename, "createdAt") VALUES ('a-test-1', 'm-test-1', 'co-test-1', 'test.png', 9100)`,
    undo: `DELETE FROM attachments WHERE id = 'a-test-1'`,
  },
  {
    label: 'update m-test-1 body (Edit propagation)',
    run: `UPDATE messages SET body = 'edited-test' WHERE id = 'm-test-1'`,
    // Reverting the body to its original value would re-emit an Edit
    // through every subscribed query — the cleanup below already
    // deletes m-test-1 entirely, so the body update vanishes with it.
    // Leaving `undo` empty for this step keeps the cleanup minimal.
    undo: ``,
  },
  {
    label: 'flip ch-test-1 visibility to private (FlippedJoin push for p21)',
    run: `UPDATE channels SET visibility = 'private' WHERE id = 'ch-test-1'`,
    undo: ``, // ch-test-1 is deleted in cleanup; this update vanishes with it
  },
];

type RowsByTable = Record<string, Record<string, Record<string, unknown>>>;

function buildClientSchema(): {
  tables: Record<
    string,
    {columns: Record<string, {type: string}>; primaryKey: string[]}
  >;
} {
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

/**
 * One open WebSocket plus a live `RowsByTable` mirror, kept in sync
 * with every pokePart frame. Caller closes via `close()`.
 */
type Subscription = {
  rows: RowsByTable;
  initialHydrated: Promise<void>;
  close: () => void;
};

async function subscribe(
  url: string,
  hash: string,
  ast: unknown,
  clientSchema: ReturnType<typeof buildClientSchema>,
): Promise<Subscription> {
  const {default: WebSocket} = await import('ws');
  const {encodeSecProtocols} = await import(
    '../../packages/zero-protocol/src/connect.ts'
  );
  const rows: RowsByTable = {};
  const remember = (
    table: string,
    key: string,
    value: Record<string, unknown>,
  ) => {
    if (!rows[table]) rows[table] = {};
    rows[table][key] = value;
  };
  const forget = (table: string, key: string) => {
    if (rows[table]) delete rows[table][key];
  };
  const canonicalKey = (
    pk: readonly string[],
    row: Record<string, unknown>,
  ) => {
    const obj: Record<string, unknown> = {};
    for (const k of pk) obj[k] = row?.[k];
    return JSON.stringify(obj);
  };
  const initBody = {
    clientSchema,
    desiredQueriesPatch: [{op: 'put' as const, hash, ast}],
  };
  const secProtocol = encodeSecProtocols(
    ['initConnection', initBody],
    undefined,
  );
  const cgid = `ivm-parity-adv-${hash}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
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
  let resolveHydrated: () => void;
  let rejectHydrated: (e: unknown) => void;
  const initialHydrated = new Promise<void>((res, rej) => {
    resolveHydrated = res;
    rejectHydrated = rej;
  });
  let sawGotPatch = false;
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
          if (g?.hash === hash && g?.op === 'put') sawGotPatch = true;
        }
      }
      const rowsPatch = (body as any)?.rowsPatch;
      if (Array.isArray(rowsPatch)) {
        for (const op of rowsPatch) {
          const t = op?.tableName;
          if (!t) continue;
          const pk =
            (paritySchema.tables as any)[t]?.primaryKey ?? ['id'];
          if (op.op === 'put') {
            remember(t, canonicalKey(pk, op.value), op.value);
          } else if (op.op === 'del') {
            forget(t, canonicalKey(pk, op.id));
          } else if (op.op === 'clear') {
            for (const k of Object.keys(rows)) delete rows[k];
          } else if (op.op === 'update') {
            const k = canonicalKey(pk, op.id);
            const prev = rows[t]?.[k] ?? {};
            remember(t, k, {...prev, ...(op.merge ?? {})});
          }
        }
      }
    } else if (tag === 'pokeEnd' && sawGotPatch) {
      // First pokeEnd after gotQueriesPatch = initial hydration done.
      // Don't close — we want to keep observing post-mutation pokes.
      resolveHydrated();
    }
  });
  ws.on('error', err => rejectHydrated(err));
  ws.on('close', () =>
    rejectHydrated(new Error('socket closed before initial hydration')),
  );
  return {
    rows,
    initialHydrated,
    close: () => ws.close(),
  };
}

/**
 * JSON.stringify is sensitive to key order. RS-side row objects come
 * out of Rust HashMap iteration order; TS-side rows come out of
 * insertion order matching the column definition. The wire-level
 * row contents are identical — only the encoded ordering differs.
 * Normalize by sorting keys before stringifying so equality is
 * order-insensitive without losing field-level fidelity.
 */
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
function snapshot(rows: RowsByTable): RowsByTable {
  return canon(rows) as RowsByTable;
}

/**
 * Compute `final - initial` per table — the rows added/removed/modified
 * during the observation window. Output shape:
 *   { table: { added: [...], removed: [...], modified: [{key, ts, rs}] } }
 * where each entry is keyed by canonical PK string.
 */
type Delta = {
  added: Record<string, Record<string, unknown>>;
  removed: Record<string, Record<string, unknown>>;
  modified: Record<
    string,
    {before: Record<string, unknown>; after: Record<string, unknown>}
  >;
};
function computeDelta(
  initial: RowsByTable,
  final: RowsByTable,
): Record<string, Delta> {
  const out: Record<string, Delta> = {};
  const tables = new Set([...Object.keys(initial), ...Object.keys(final)]);
  for (const table of tables) {
    const before = initial[table] ?? {};
    const after = final[table] ?? {};
    const delta: Delta = {added: {}, removed: {}, modified: {}};
    const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
    for (const key of keys) {
      const a = before[key];
      const b = after[key];
      if (a === undefined && b !== undefined) delta.added[key] = b;
      else if (a !== undefined && b === undefined) delta.removed[key] = a;
      else if (
        a !== undefined &&
        b !== undefined &&
        JSON.stringify(a) !== JSON.stringify(b)
      ) {
        delta.modified[key] = {before: a, after: b};
      }
    }
    if (
      Object.keys(delta.added).length > 0 ||
      Object.keys(delta.removed).length > 0 ||
      Object.keys(delta.modified).length > 0
    ) {
      out[table] = delta;
    }
  }
  return out;
}

function deltasEqual(
  a: Record<string, Delta>,
  b: Record<string, Delta>,
): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

async function runOnePatternAdvance(
  hash: PatternName,
  ast: unknown,
  clientSchema: ReturnType<typeof buildClientSchema>,
  sql: postgres.Sql,
): Promise<{
  pattern: PatternName;
  match: boolean;
  tsDelta: Record<string, Delta>;
  rsDelta: Record<string, Delta>;
  error?: string;
}> {
  const ts = await subscribe(TS_URL, hash, ast, clientSchema);
  const rs = await subscribe(RS_URL, hash, ast, clientSchema);
  try {
    await Promise.all([ts.initialHydrated, rs.initialHydrated]);
    const tsInitial = snapshot(ts.rows);
    const rsInitial = snapshot(rs.rows);
    // Run mutations (run-only here — undo happens in the outer
    // `finally` regardless of test outcome).
    for (const step of MUTATIONS) {
      await sql.unsafe(step.run);
    }
    // Wait for the advancement pokes to flow through both caches.
    await sleep(POKE_WAIT_MS);
    const tsFinal = snapshot(ts.rows);
    const rsFinal = snapshot(rs.rows);
    const tsDelta = computeDelta(tsInitial, tsFinal);
    const rsDelta = computeDelta(rsInitial, rsFinal);
    return {
      pattern: hash,
      match: deltasEqual(tsDelta, rsDelta),
      tsDelta,
      rsDelta,
    };
  } catch (err) {
    return {
      pattern: hash,
      match: false,
      tsDelta: {},
      rsDelta: {},
      error: (err as Error).message,
    };
  } finally {
    ts.close();
    rs.close();
  }
}

async function main() {
  const sql = postgres(PG_URL, {max: 4, prepare: false});
  // Wipe any leftover test rows from a prior crashed run before we
  // begin — does NOT touch seed data because the IDs are explicit.
  const cleanup = async () => {
    for (const step of [...MUTATIONS].reverse()) {
      if (step.undo) {
        try {
          await sql.unsafe(step.undo);
        } catch (err) {
          // eslint-disable-next-line no-console
          console.error(`[cleanup] ${step.label}: ${(err as Error).message}`);
        }
      }
    }
    // Defensive: explicit deletes by ID so even if MUTATIONS array
    // changes, no test row survives across runs.
    await sql`DELETE FROM attachments WHERE id = 'a-test-1'`;
    await sql`DELETE FROM messages WHERE id = 'm-test-1'`;
    await sql`DELETE FROM conversations WHERE id = 'co-test-1'`;
    await sql`DELETE FROM participants WHERE "userId" = 'u1' AND "channelId" = 'ch-test-1'`;
    await sql`DELETE FROM channels WHERE id = 'ch-test-1'`;
    // Give the change-streamer → replicator pipeline time to
    // propagate the DELETEs into both SQLite replicas. Without this
    // wait, the NEXT pattern's initial snapshot may still see
    // residual test rows in one replica but not the other (PG
    // replication slots advance independently), producing phantom
    // divergences in pattern-initial state. 800ms empirically suffices
    // on localhost; override via PARITY_CLEANUP_WAIT_MS.
    const waitMs = Number(process.env.PARITY_CLEANUP_WAIT_MS ?? 800);
    await sleep(waitMs);
  };
  await cleanup();

  const clientSchema = buildClientSchema();
  const results: Array<Awaited<ReturnType<typeof runOnePatternAdvance>>> = [];
  try {
    for (const [hash, build] of Object.entries(patterns) as Array<
      [PatternName, () => unknown]
    >) {
      const r = await runOnePatternAdvance(
        hash,
        extractAst(build()),
        clientSchema,
        sql,
      );
      results.push(r);
      // Restore between patterns so each pattern starts from the seed
      // baseline. Otherwise pattern N+1's "initial" includes pattern N's
      // mutation residue.
      await cleanup();
    }
  } finally {
    await cleanup();
    await sql.end({timeout: 5});
  }

  // eslint-disable-next-line no-console
  console.log('\n=== advancement parity report ===');
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
      console.log(`         ts delta: ${JSON.stringify(r.tsDelta)}`);
      // eslint-disable-next-line no-console
      console.log(`         rs delta: ${JSON.stringify(r.rsDelta)}`);
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
