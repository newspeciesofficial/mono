/**
 * Advancement coverage harness — pairs `harness-coverage.ts` (hydration
 * sweep) with the push-path observation logic from `harness-advance.ts`.
 *
 * For each AST in ast_corpus.json:
 *   1. Subscribe on TS (4858) and RS (4868), wait for hydration.
 *   2. Snapshot per-table row mirror produced by initial pokes.
 *   3. After the whole batch is hydrated, run a SHARED mutation block
 *      (insert/update across the schema) once against PG.
 *   4. Wait for advancement pokes to flow into both caches.
 *   5. Snapshot final state, compute (final - initial) delta per AST.
 *   6. Diff TS-delta vs RS-delta — any divergence is a push-path bug.
 *   7. Close all sockets, run cleanup SQL, advance to next batch.
 *
 * Why batched:
 *   - One mutation block per batch instead of per AST → 987 / BATCH_SIZE
 *     mutation cycles instead of 987.
 *   - All ASTs in a batch observe the same advancement event, so deltas
 *     are directly comparable per AST.
 *   - Cleanup happens once per batch, not 987 times.
 *
 * Tuning:
 *   - BATCH_SIZE       — ASTs per batch. ~30 keeps total sockets ≤ 60
 *                        per batch, well within local cache limits.
 *   - POKE_WAIT_MS     — time after mutation block before final snapshot.
 *   - HYDRATE_TIMEOUT  — per-AST hydration deadline.
 */
import {writeFileSync, readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import postgres from 'postgres';
import {schema as paritySchema} from './zero-schema.ts';
import {renderAst} from './ast-render.ts';
import type {AST} from '../../packages/zero-protocol/src/ast.ts';

const TS_URL = process.env.PARITY_TS_URL ?? 'ws://localhost:4858/sync/v49/connect';
const RS_URL = process.env.PARITY_RS_URL ?? 'ws://localhost:4868/sync/v49/connect';
const PG_URL =
  process.env.PARITY_PG_URL ??
  'postgresql://user:password@127.0.0.1:6434/parity';
const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? 30);
const POKE_WAIT_MS = Number(process.env.POKE_WAIT_MS ?? 3000);
const HYDRATE_TIMEOUT_MS = Number(process.env.HYDRATE_TIMEOUT_MS ?? 20_000);
const CORPUS_LIMIT = Number(process.env.CORPUS_LIMIT ?? 0);
const CLEANUP_WAIT_MS = Number(process.env.CLEANUP_WAIT_MS ?? 1000);

const here = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Mutation block — same set of operations as harness-advance.ts so the
// coverage map is identical: Filter / Take / Exists / Join / FlippedJoin /
// UnionFanIn / Edit propagation all get exercised.
// ---------------------------------------------------------------------------
type Step = {label: string; run: string; undo: string};
const MUTATIONS: Step[] = [
  {
    label: 'insert channel ch-test-1 (public)',
    run: `INSERT INTO channels (id, name, visibility) VALUES ('ch-test-1', 'test-channel', 'public')`,
    undo: `DELETE FROM channels WHERE id = 'ch-test-1'`,
  },
  {
    label: 'add u1 as participant of ch-test-1',
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
    label: 'insert attachment a-test-1 on m-test-1',
    run: `INSERT INTO attachments (id, "messageId", "conversationId", filename, "createdAt") VALUES ('a-test-1', 'm-test-1', 'co-test-1', 'test.png', 9100)`,
    undo: `DELETE FROM attachments WHERE id = 'a-test-1'`,
  },
  {
    label: 'update m-test-1 body (Edit propagation)',
    run: `UPDATE messages SET body = 'edited-test' WHERE id = 'm-test-1'`,
    undo: ``,
  },
  {
    label: 'flip ch-test-1 visibility to private (FlippedJoin push)',
    run: `UPDATE channels SET visibility = 'private' WHERE id = 'ch-test-1'`,
    undo: ``,
  },
];

// ---------------------------------------------------------------------------
// Types.
// ---------------------------------------------------------------------------
type CorpusEntry = {id: string; ast: unknown; kind: string};
type RowsByTable = Record<string, Record<string, Record<string, unknown>>>;
type Delta = {
  added: Record<string, Record<string, unknown>>;
  removed: Record<string, Record<string, unknown>>;
  modified: Record<string, {before: Record<string, unknown>; after: Record<string, unknown>}>;
};

type Outcome =
  | {status: 'ok'; tsHydrateHash: string; rsHydrateHash: string; tsAdvHash: string; rsAdvHash: string}
  | {status: 'hydrate-diverge'; tsHash: string; rsHash: string; diff: string}
  | {status: 'advance-diverge'; tsDelta: Record<string, Delta>; rsDelta: Record<string, Delta>; diff: string}
  | {status: 'error'; side?: 'ts' | 'rs' | 'both'; phase: 'hydrate' | 'wait' | 'mutate'; message: string};

type Result = {id: string; ast: unknown; outcome: Outcome};

// ---------------------------------------------------------------------------
// Subscription — keeps socket open after hydration so we can observe
// advancement pokes. Caller closes via close().
// ---------------------------------------------------------------------------
type Sub = {
  rows: RowsByTable;
  initialHydrated: Promise<void>;
  close: () => void;
};

function buildClientSchema(): {
  tables: Record<string, {columns: Record<string, {type: string}>; primaryKey: string[]}>;
} {
  const out: Record<string, {columns: Record<string, {type: string}>; primaryKey: string[]}> = {};
  for (const [tableName, tableDef] of Object.entries(paritySchema.tables) as Array<[string, any]>) {
    const columns: Record<string, {type: string}> = {};
    for (const [col, desc] of Object.entries(tableDef.columns) as Array<[string, any]>) {
      columns[col] = {type: desc.type};
    }
    out[tableName] = {columns, primaryKey: [...tableDef.primaryKey]};
  }
  return {tables: out};
}

async function subscribe(
  url: string,
  hash: string,
  ast: unknown,
  clientSchema: ReturnType<typeof buildClientSchema>,
): Promise<Sub> {
  const {default: WebSocket} = await import('ws');
  const {encodeSecProtocols} = await import('../../packages/zero-protocol/src/connect.ts');
  const rows: RowsByTable = {};
  const remember = (table: string, key: string, value: Record<string, unknown>) => {
    if (!rows[table]) rows[table] = {};
    rows[table][key] = value;
  };
  const forget = (table: string, key: string) => {
    if (rows[table]) delete rows[table][key];
  };
  const canonicalKey = (pk: readonly string[], row: Record<string, unknown>) => {
    const obj: Record<string, unknown> = {};
    for (const k of pk) obj[k] = row?.[k];
    return JSON.stringify(obj);
  };
  const initBody = {
    clientSchema,
    desiredQueriesPatch: [{op: 'put' as const, hash, ast}],
  };
  const secProtocol = encodeSecProtocols(['initConnection', initBody], undefined);
  const cgid = `ivm-parity-advcov-${hash}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const ws = new WebSocket(
    url +
      '?clientGroupID=' + cgid +
      '&clientID=cid-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8) +
      '&baseCookie=&ts=' + Date.now() + '&lmid=1',
    secProtocol,
  );
  let resolveHydrated: () => void;
  let rejectHydrated: (e: unknown) => void;
  const initialHydrated = new Promise<void>((res, rej) => {
    resolveHydrated = res;
    rejectHydrated = rej;
  });
  let sawGotPatch = false;
  let resolved = false;
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
        for (const g of got) if (g?.hash === hash && g?.op === 'put') sawGotPatch = true;
      }
      const rowsPatch = (body as any)?.rowsPatch;
      if (Array.isArray(rowsPatch)) {
        for (const op of rowsPatch) {
          const t = op?.tableName;
          if (!t) continue;
          const pk = (paritySchema.tables as any)[t]?.primaryKey ?? ['id'];
          if (op.op === 'put') remember(t, canonicalKey(pk, op.value), op.value);
          else if (op.op === 'del') forget(t, canonicalKey(pk, op.id));
          else if (op.op === 'clear') for (const k of Object.keys(rows)) delete rows[k];
          else if (op.op === 'update') {
            const k = canonicalKey(pk, op.id);
            const prev = rows[t]?.[k] ?? {};
            remember(t, k, {...prev, ...(op.merge ?? {})});
          }
        }
      }
    } else if (tag === 'pokeEnd' && sawGotPatch && !resolved) {
      resolved = true;
      resolveHydrated();
    }
  });
  ws.on('error', err => {
    if (!resolved) rejectHydrated(err);
  });
  ws.on('close', () => {
    if (!resolved) rejectHydrated(new Error('socket closed before hydration'));
  });
  return {rows, initialHydrated, close: () => { try { ws.close(); } catch {} }};
}

// ---------------------------------------------------------------------------
// Snapshot / canonicalize / diff.
// ---------------------------------------------------------------------------
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

function hashRows(rows: RowsByTable): string {
  return createHash('sha256').update(JSON.stringify(canon(rows))).digest('hex').slice(0, 16);
}

function computeDelta(initial: RowsByTable, final: RowsByTable): Record<string, Delta> {
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
      else if (a !== undefined && b !== undefined && JSON.stringify(a) !== JSON.stringify(b)) {
        delta.modified[key] = {before: a, after: b};
      }
    }
    if (Object.keys(delta.added).length > 0 || Object.keys(delta.removed).length > 0 || Object.keys(delta.modified).length > 0) {
      out[table] = delta;
    }
  }
  return out;
}

function deltasEqual(a: Record<string, Delta>, b: Record<string, Delta>): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function summarizeDeltaDiff(ts: Record<string, Delta>, rs: Record<string, Delta>): string {
  const tables = new Set([...Object.keys(ts), ...Object.keys(rs)]);
  const lines: string[] = [];
  for (const t of [...tables].sort()) {
    const a = ts[t];
    const b = rs[t];
    const tsCount = a ? Object.keys(a.added).length + Object.keys(a.removed).length + Object.keys(a.modified).length : 0;
    const rsCount = b ? Object.keys(b.added).length + Object.keys(b.removed).length + Object.keys(b.modified).length : 0;
    if (tsCount !== rsCount || (a && b && JSON.stringify(a) !== JSON.stringify(b))) {
      lines.push(`  ${t}: ts=${tsCount} change(s)  rs=${rsCount} change(s)`);
      if (a) {
        if (Object.keys(a.added).length) lines.push(`    ts added: ${Object.keys(a.added).slice(0, 3).join(', ')}`);
        if (Object.keys(a.removed).length) lines.push(`    ts removed: ${Object.keys(a.removed).slice(0, 3).join(', ')}`);
        if (Object.keys(a.modified).length) lines.push(`    ts modified: ${Object.keys(a.modified).slice(0, 3).join(', ')}`);
      }
      if (b) {
        if (Object.keys(b.added).length) lines.push(`    rs added: ${Object.keys(b.added).slice(0, 3).join(', ')}`);
        if (Object.keys(b.removed).length) lines.push(`    rs removed: ${Object.keys(b.removed).slice(0, 3).join(', ')}`);
        if (Object.keys(b.modified).length) lines.push(`    rs modified: ${Object.keys(b.modified).slice(0, 3).join(', ')}`);
      }
    }
  }
  return lines.join('\n');
}

function summarizeRowsDiff(ts: RowsByTable, rs: RowsByTable): string {
  const tables = new Set([...Object.keys(ts), ...Object.keys(rs)]);
  const lines: string[] = [];
  for (const t of [...tables].sort()) {
    const a = ts[t] ?? {};
    const b = rs[t] ?? {};
    if (Object.keys(a).length !== Object.keys(b).length) {
      lines.push(`  ${t}: ts=${Object.keys(a).length} rs=${Object.keys(b).length}`);
    }
  }
  return lines.join('\n');
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// Cleanup — restore seed-baseline state.
// ---------------------------------------------------------------------------
async function cleanupDb(sql: postgres.Sql): Promise<void> {
  for (const step of [...MUTATIONS].reverse()) {
    if (step.undo) {
      try { await sql.unsafe(step.undo); } catch {}
    }
  }
  // Defensive explicit deletes by ID — survive across MUTATIONS edits.
  await sql`DELETE FROM attachments WHERE id = 'a-test-1'`;
  await sql`DELETE FROM messages WHERE id = 'm-test-1'`;
  await sql`DELETE FROM conversations WHERE id = 'co-test-1'`;
  await sql`DELETE FROM participants WHERE "userId" = 'u1' AND "channelId" = 'ch-test-1'`;
  await sql`DELETE FROM channels WHERE id = 'ch-test-1'`;
  await sleep(CLEANUP_WAIT_MS);
}

// ---------------------------------------------------------------------------
// One batch: subscribe → hydrate → snapshot → mutate → wait → snapshot →
// diff. Returns one Result per AST in the batch.
// ---------------------------------------------------------------------------
async function runBatch(
  batch: CorpusEntry[],
  clientSchema: ReturnType<typeof buildClientSchema>,
  sql: postgres.Sql,
): Promise<Result[]> {
  const subs: Array<{entry: CorpusEntry; ts: Sub | null; rs: Sub | null; err?: string}> = [];

  // Open all subscriptions concurrently.
  await Promise.all(
    batch.map(async entry => {
      let ts: Sub | null = null;
      let rs: Sub | null = null;
      let err: string | undefined;
      try {
        ts = await subscribe(TS_URL, entry.id, entry.ast, clientSchema);
      } catch (e) {
        err = `ts-subscribe: ${(e as Error).message}`;
      }
      try {
        rs = await subscribe(RS_URL, entry.id, entry.ast, clientSchema);
      } catch (e) {
        err = (err ? err + ' | ' : '') + `rs-subscribe: ${(e as Error).message}`;
      }
      subs.push({entry, ts, rs, err});
    }),
  );

  // Wait for hydration (with timeout per AST).
  const hydrationOutcomes: Map<string, {tsHash?: string; rsHash?: string; tsRows?: RowsByTable; rsRows?: RowsByTable; err?: string}> = new Map();
  await Promise.all(
    subs.map(async s => {
      if (s.err || !s.ts || !s.rs) {
        hydrationOutcomes.set(s.entry.id, {err: s.err ?? 'no-sub'});
        return;
      }
      try {
        const timeout = new Promise<never>((_, rej) =>
          setTimeout(() => rej(new Error(`hydrate timeout ${HYDRATE_TIMEOUT_MS}ms`)), HYDRATE_TIMEOUT_MS),
        );
        await Promise.race([Promise.all([s.ts.initialHydrated, s.rs.initialHydrated]), timeout]);
        hydrationOutcomes.set(s.entry.id, {
          tsRows: snapshot(s.ts.rows),
          rsRows: snapshot(s.rs.rows),
          tsHash: hashRows(s.ts.rows),
          rsHash: hashRows(s.rs.rows),
        });
      } catch (e) {
        hydrationOutcomes.set(s.entry.id, {err: `hydrate: ${(e as Error).message}`});
      }
    }),
  );

  // Apply mutation block ONCE for the whole batch.
  let mutationErr: string | undefined;
  try {
    for (const step of MUTATIONS) await sql.unsafe(step.run);
  } catch (e) {
    mutationErr = `mutate: ${(e as Error).message}`;
  }

  // Wait for advancement to settle.
  await sleep(POKE_WAIT_MS);

  // Snapshot final per-AST.
  const results: Result[] = [];
  for (const s of subs) {
    const hyd = hydrationOutcomes.get(s.entry.id)!;
    if (hyd.err || mutationErr) {
      results.push({
        id: s.entry.id,
        ast: s.entry.ast,
        outcome: {
          status: 'error',
          phase: hyd.err?.startsWith('hydrate') ? 'hydrate' : 'mutate',
          message: hyd.err ?? mutationErr ?? 'unknown',
        },
      });
      try { s.ts?.close(); s.rs?.close(); } catch {}
      continue;
    }
    if (hyd.tsHash !== hyd.rsHash) {
      results.push({
        id: s.entry.id,
        ast: s.entry.ast,
        outcome: {
          status: 'hydrate-diverge',
          tsHash: hyd.tsHash!,
          rsHash: hyd.rsHash!,
          diff: summarizeRowsDiff(hyd.tsRows!, hyd.rsRows!),
        },
      });
      try { s.ts?.close(); s.rs?.close(); } catch {}
      continue;
    }
    const tsFinal = snapshot(s.ts!.rows);
    const rsFinal = snapshot(s.rs!.rows);
    const tsDelta = computeDelta(hyd.tsRows!, tsFinal);
    const rsDelta = computeDelta(hyd.rsRows!, rsFinal);
    if (deltasEqual(tsDelta, rsDelta)) {
      results.push({
        id: s.entry.id,
        ast: s.entry.ast,
        outcome: {
          status: 'ok',
          tsHydrateHash: hyd.tsHash!,
          rsHydrateHash: hyd.rsHash!,
          tsAdvHash: hashRows(tsFinal),
          rsAdvHash: hashRows(rsFinal),
        },
      });
    } else {
      results.push({
        id: s.entry.id,
        ast: s.entry.ast,
        outcome: {
          status: 'advance-diverge',
          tsDelta,
          rsDelta,
          diff: summarizeDeltaDiff(tsDelta, rsDelta),
        },
      });
    }
    try { s.ts?.close(); s.rs?.close(); } catch {}
  }

  // Cleanup PG so next batch starts from seed baseline.
  await cleanupDb(sql);

  return results;
}

// ---------------------------------------------------------------------------
// Entry.
// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  const corpusPath = join(here, 'ast_corpus.json');
  const corpus: CorpusEntry[] = JSON.parse(readFileSync(corpusPath, 'utf8'));
  const slice = CORPUS_LIMIT > 0 ? corpus.slice(0, CORPUS_LIMIT) : corpus;
  console.error(`[advance-coverage] running ${slice.length} ASTs through hydrate+mutate cycle, BATCH_SIZE=${BATCH_SIZE}, POKE_WAIT_MS=${POKE_WAIT_MS}`);

  const sql = postgres(PG_URL, {max: 4, prepare: false});
  await cleanupDb(sql); // start clean
  const clientSchema = buildClientSchema();
  const start = Date.now();
  const allResults: Result[] = [];

  try {
    const numBatches = Math.ceil(slice.length / BATCH_SIZE);
    for (let i = 0; i < numBatches; i++) {
      const batch = slice.slice(i * BATCH_SIZE, (i + 1) * BATCH_SIZE);
      const batchResults = await runBatch(batch, clientSchema, sql);
      allResults.push(...batchResults);
      const elapsed = ((Date.now() - start) / 1000).toFixed(1);
      const okSoFar = allResults.filter(r => r.outcome.status === 'ok').length;
      const advDiv = allResults.filter(r => r.outcome.status === 'advance-diverge').length;
      const hydDiv = allResults.filter(r => r.outcome.status === 'hydrate-diverge').length;
      const errs = allResults.filter(r => r.outcome.status === 'error').length;
      console.error(`[advance-coverage] batch ${i + 1}/${numBatches} done in ${elapsed}s — ok=${okSoFar} adv-diverge=${advDiv} hyd-diverge=${hydDiv} err=${errs}`);
    }
  } finally {
    await cleanupDb(sql);
    await sql.end({timeout: 5});
  }

  const ok = allResults.filter(r => r.outcome.status === 'ok');
  const hydDiv = allResults.filter(r => r.outcome.status === 'hydrate-diverge');
  const advDiv = allResults.filter(r => r.outcome.status === 'advance-diverge');
  const errs = allResults.filter(r => r.outcome.status === 'error');

  // Persist full report.
  writeFileSync(join(here, 'advance_coverage_run.json'), JSON.stringify({
    schema: 'ts-rs-advance-coverage-v1',
    ts: TS_URL, rs: RS_URL,
    elapsedMs: Date.now() - start,
    summary: {total: allResults.length, ok: ok.length, hydrateDiverge: hydDiv.length, advanceDiverge: advDiv.length, error: errs.length},
    mutations: MUTATIONS.map(m => m.label),
    results: allResults,
  }, null, 2));

  // Markdown summary.
  const md: string[] = [];
  md.push('# IVM TS/RS Advancement Parity — advance_coverage_run');
  md.push('');
  md.push(`- Total: **${allResults.length}**`);
  md.push(`- OK (hydrate + advance match): **${ok.length}**`);
  md.push(`- Hydrate diverge: **${hydDiv.length}**`);
  md.push(`- Advance diverge: **${advDiv.length}**`);
  md.push(`- Error: **${errs.length}**`);
  md.push(`- Elapsed: ${((Date.now() - start) / 1000).toFixed(1)}s`);
  md.push('');
  md.push('### Mutation block');
  for (const m of MUTATIONS) md.push(`- ${m.label}`);
  md.push('');

  if (advDiv.length > 0) {
    md.push('## Advance-only divergences (hydrate matched, push diverged)');
    md.push('');
    for (const r of advDiv) {
      const o = r.outcome as Extract<Outcome, {status: 'advance-diverge'}>;
      md.push(`### ${r.id}`);
      md.push('```ts');
      try { md.push(renderAst(r.ast as AST)); } catch { md.push('// renderer failed'); }
      md.push('```');
      md.push('```');
      md.push(o.diff || '(no per-table count diff — values differ)');
      md.push('```');
      md.push('');
    }
  }
  if (hydDiv.length > 0) {
    md.push('## Hydrate divergences');
    md.push('');
    for (const r of hydDiv.slice(0, 30)) {
      const o = r.outcome as Extract<Outcome, {status: 'hydrate-diverge'}>;
      md.push(`### ${r.id}`);
      md.push('```ts');
      try { md.push(renderAst(r.ast as AST)); } catch { md.push('// renderer failed'); }
      md.push('```');
      md.push('```');
      md.push(o.diff || '(hash diff)');
      md.push('```');
      md.push('');
    }
    if (hydDiv.length > 30) md.push(`… and ${hydDiv.length - 30} more`);
  }
  if (errs.length > 0) {
    md.push('## Errors');
    md.push('');
    for (const r of errs.slice(0, 30)) {
      const o = r.outcome as Extract<Outcome, {status: 'error'}>;
      md.push(`- ${r.id} [${o.phase}]: ${o.message}`);
    }
  }

  writeFileSync(join(here, 'advance_parity_gaps.md'), md.join('\n'));

  console.error(`\n[advance-coverage] done. ok=${ok.length} adv-diverge=${advDiv.length} hyd-diverge=${hydDiv.length} err=${errs.length}`);
  console.error(`[advance-coverage] wrote advance_coverage_run.json and advance_parity_gaps.md`);
  process.exit(advDiv.length === 0 && hydDiv.length === 0 && errs.length === 0 ? 0 : 1);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
