/**
 * Coverage-driven parity harness.
 *
 * Reads `ast_corpus.json` (Phase 1 fuzz output, optionally extended by
 * Phase 2 seed extraction). For each AST:
 *   1. Open ONE WebSocket on TS (4858) and ONE on RS (4868).
 *   2. Wait for `gotQueriesPatch` on both — initial hydration done.
 *   3. Snapshot the per-table row mirror produced by pokePart events.
 *   4. Hash the snapshot canonically and diff TS vs RS.
 *   5. Close both sockets.
 *
 * Runs `MAX_PARALLEL` ASTs concurrently (default 8) so the full sweep of
 * ~1000 ASTs finishes in minutes rather than hours.
 *
 * Outputs:
 *   - `coverage_run.json` — full per-AST result
 *   - `parity_gaps.md`   — categorized human-readable summary
 *   - exit code 0 iff every AST hydrates to the same payload on both sides
 */
import {writeFileSync, readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {schema as paritySchema} from './zero-schema.ts';
import {renderAst} from './ast-render.ts';
import type {AST} from '../../packages/zero-protocol/src/ast.ts';

const TS_URL = process.env.PARITY_TS_URL ?? 'ws://localhost:4858/sync/v49/connect';
const RS_URL = process.env.PARITY_RS_URL ?? 'ws://localhost:4868/sync/v49/connect';
const MAX_PARALLEL = Number(process.env.MAX_PARALLEL ?? 8);
const HYDRATION_TIMEOUT_MS = Number(process.env.HYDRATION_TIMEOUT_MS ?? 15_000);
const CORPUS_LIMIT = Number(process.env.CORPUS_LIMIT ?? 0); // 0 = no limit

const here = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Types.
// ---------------------------------------------------------------------------
type CorpusEntry = {id: string; ast: unknown; kind: string};
type RowsByTable = Record<string, Record<string, Record<string, unknown>>>;

type Outcome =
  | {status: 'ok'; tsHash: string; rsHash: string; rowCounts: {ts: number; rs: number}}
  | {status: 'diverge'; tsHash: string; rsHash: string; rowCounts: {ts: number; rs: number}; diff: string}
  | {status: 'error'; side?: 'ts' | 'rs' | 'both'; message: string};

type Result = {id: string; ast: unknown; outcome: Outcome};

// ---------------------------------------------------------------------------
// Client schema (mirrors zero-schema, used by initConnection).
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Subscription — mirrors harness-advance.ts but auto-closes on hydrate.
// ---------------------------------------------------------------------------
async function subscribeAndHydrate(
  url: string,
  hash: string,
  ast: unknown,
  clientSchema: ReturnType<typeof buildClientSchema>,
): Promise<RowsByTable> {
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
  const cgid = `ivm-parity-cov-${hash}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const ws = new WebSocket(
    url +
      '?clientGroupID=' + cgid +
      '&clientID=cid-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8) +
      '&baseCookie=&ts=' + Date.now() + '&lmid=1',
    secProtocol,
  );

  return new Promise<RowsByTable>((resolve, reject) => {
    let sawGotPatch = false;
    let settled = false;
    const finish = (cb: () => void) => {
      if (settled) return;
      settled = true;
      try {
        ws.close();
      } catch {}
      cb();
    };
    const timer = setTimeout(() => {
      finish(() => reject(new Error(`hydration timeout after ${HYDRATION_TIMEOUT_MS}ms`)));
    }, HYDRATION_TIMEOUT_MS);

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
      } else if (tag === 'pokeEnd' && sawGotPatch) {
        clearTimeout(timer);
        finish(() => resolve(rows));
      } else if (tag === 'error') {
        clearTimeout(timer);
        const msg = typeof body === 'string' ? body : JSON.stringify(body);
        finish(() => reject(new Error(`server error: ${msg}`)));
      }
    });
    ws.on('error', err => {
      clearTimeout(timer);
      finish(() => reject(err));
    });
    ws.on('close', () => {
      clearTimeout(timer);
      if (!sawGotPatch) finish(() => reject(new Error('socket closed before hydration')));
    });
  });
}

// ---------------------------------------------------------------------------
// Canonicalize + hash + diff.
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

function hashRows(rows: RowsByTable): string {
  const c = canon(rows);
  return createHash('sha256').update(JSON.stringify(c)).digest('hex').slice(0, 16);
}

function rowCount(rows: RowsByTable): number {
  let n = 0;
  for (const t of Object.values(rows)) n += Object.keys(t).length;
  return n;
}

function summarizeDiff(ts: RowsByTable, rs: RowsByTable): string {
  const tables = new Set([...Object.keys(ts), ...Object.keys(rs)]);
  const lines: string[] = [];
  for (const table of [...tables].sort()) {
    const a = ts[table] ?? {};
    const b = rs[table] ?? {};
    const ak = Object.keys(a).sort();
    const bk = Object.keys(b).sort();
    if (ak.length !== bk.length) {
      lines.push(`  ${table}: ts=${ak.length} rs=${bk.length}`);
      const onlyTs = ak.filter(k => !(k in b));
      const onlyRs = bk.filter(k => !(k in a));
      if (onlyTs.length > 0) lines.push(`    only-ts: ${onlyTs.slice(0, 5).join(', ')}${onlyTs.length > 5 ? '…' : ''}`);
      if (onlyRs.length > 0) lines.push(`    only-rs: ${onlyRs.slice(0, 5).join(', ')}${onlyRs.length > 5 ? '…' : ''}`);
    } else {
      const differing: string[] = [];
      for (const k of ak) {
        if (JSON.stringify(canon(a[k])) !== JSON.stringify(canon(b[k]))) differing.push(k);
      }
      if (differing.length > 0) {
        lines.push(`  ${table}: ${differing.length} row(s) differ at value-level`);
        lines.push(`    sample: ${differing[0]}`);
      }
    }
  }
  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Concurrency limiter (no external dep).
// ---------------------------------------------------------------------------
function pLimit(max: number) {
  let active = 0;
  const queue: Array<() => void> = [];
  const next = () => {
    if (active >= max) return;
    const job = queue.shift();
    if (!job) return;
    active++;
    job();
  };
  return <T>(fn: () => Promise<T>): Promise<T> =>
    new Promise<T>((resolve, reject) => {
      const run = () => {
        fn()
          .then(resolve, reject)
          .finally(() => {
            active--;
            next();
          });
      };
      queue.push(run);
      next();
    });
}

// ---------------------------------------------------------------------------
// Per-AST runner.
// ---------------------------------------------------------------------------
async function runOne(
  entry: CorpusEntry,
  clientSchema: ReturnType<typeof buildClientSchema>,
): Promise<Result> {
  const hash = entry.id;
  const ast = entry.ast;
  let tsRows: RowsByTable | undefined;
  let rsRows: RowsByTable | undefined;
  let tsErr: Error | undefined;
  let rsErr: Error | undefined;

  await Promise.allSettled([
    subscribeAndHydrate(TS_URL, hash, ast, clientSchema).then(r => (tsRows = r), e => (tsErr = e)),
    subscribeAndHydrate(RS_URL, hash, ast, clientSchema).then(r => (rsRows = r), e => (rsErr = e)),
  ]);

  if (tsErr || rsErr) {
    const side: 'ts' | 'rs' | 'both' = tsErr && rsErr ? 'both' : tsErr ? 'ts' : 'rs';
    return {
      id: entry.id,
      ast,
      outcome: {
        status: 'error',
        side,
        message: [tsErr?.message, rsErr?.message].filter(Boolean).join(' | '),
      },
    };
  }
  const ts = tsRows!;
  const rs = rsRows!;
  const tsHash = hashRows(ts);
  const rsHash = hashRows(rs);
  const rowCounts = {ts: rowCount(ts), rs: rowCount(rs)};
  if (tsHash === rsHash) {
    return {id: entry.id, ast, outcome: {status: 'ok', tsHash, rsHash, rowCounts}};
  }
  return {
    id: entry.id,
    ast,
    outcome: {
      status: 'diverge',
      tsHash,
      rsHash,
      rowCounts,
      diff: summarizeDiff(ts, rs),
    },
  };
}

// ---------------------------------------------------------------------------
// Entry.
// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  const corpusPath = join(here, 'ast_corpus.json');
  const corpus: CorpusEntry[] = JSON.parse(readFileSync(corpusPath, 'utf8'));
  const slice = CORPUS_LIMIT > 0 ? corpus.slice(0, CORPUS_LIMIT) : corpus;
  console.error(`[harness-coverage] running ${slice.length} ASTs against TS=${TS_URL} and RS=${RS_URL}, parallelism=${MAX_PARALLEL}`);

  const clientSchema = buildClientSchema();
  const limit = pLimit(MAX_PARALLEL);
  const start = Date.now();
  let done = 0;

  const results: Result[] = await Promise.all(
    slice.map(entry =>
      limit(async () => {
        const r = await runOne(entry, clientSchema);
        done++;
        if (done % 25 === 0 || done === slice.length) {
          const pct = ((done / slice.length) * 100).toFixed(1);
          const elapsed = ((Date.now() - start) / 1000).toFixed(1);
          console.error(`[harness-coverage] ${done}/${slice.length} (${pct}%) in ${elapsed}s`);
        }
        return r;
      }),
    ),
  );

  // Categorize.
  const ok = results.filter(r => r.outcome.status === 'ok');
  const diverge = results.filter(r => r.outcome.status === 'diverge');
  const errored = results.filter(r => r.outcome.status === 'error');

  // Persist full report.
  writeFileSync(join(here, 'coverage_run.json'), JSON.stringify({
    schema: 'ts-rs-coverage-v1',
    ts: TS_URL,
    rs: RS_URL,
    elapsedMs: Date.now() - start,
    summary: {total: results.length, ok: ok.length, diverge: diverge.length, error: errored.length},
    results,
  }, null, 2));

  // Markdown summary for humans.
  const md: string[] = [];
  md.push(`# IVM TS/RS Parity — coverage_run`);
  md.push('');
  md.push(`- Total: **${results.length}**`);
  md.push(`- OK: **${ok.length}**`);
  md.push(`- Diverge: **${diverge.length}**`);
  md.push(`- Error: **${errored.length}**`);
  md.push(`- Elapsed: ${((Date.now() - start) / 1000).toFixed(1)}s`);
  md.push('');
  if (diverge.length > 0) {
    md.push('## Divergences');
    md.push('');
    for (const r of diverge) {
      const o = r.outcome as Extract<Outcome, {status: 'diverge'}>;
      md.push(`### ${r.id} (ts=${o.rowCounts.ts}, rs=${o.rowCounts.rs})`);
      md.push('```ts');
      try {
        md.push(renderAst(r.ast as AST));
      } catch {
        md.push('// (renderer failed; see raw AST below)');
      }
      md.push('```');
      md.push('<details><summary>raw AST</summary>');
      md.push('');
      md.push('```json');
      md.push(JSON.stringify(r.ast, null, 2));
      md.push('```');
      md.push('');
      md.push('</details>');
      md.push('');
      md.push('```');
      md.push(o.diff || '(no per-row diff — hash differs but row sets equal cardinality)');
      md.push('```');
      md.push('');
    }
  }
  if (errored.length > 0) {
    md.push('## Errors');
    md.push('');
    for (const r of errored.slice(0, 30)) {
      const o = r.outcome as Extract<Outcome, {status: 'error'}>;
      md.push(`- ${r.id} [${o.side}]: ${o.message}`);
    }
    if (errored.length > 30) md.push(`- … and ${errored.length - 30} more`);
  }
  writeFileSync(join(here, 'parity_gaps.md'), md.join('\n'));

  console.error(`\n[harness-coverage] done. ok=${ok.length} diverge=${diverge.length} error=${errored.length}`);
  console.error(`[harness-coverage] wrote coverage_run.json and parity_gaps.md`);
  process.exit(diverge.length === 0 && errored.length === 0 ? 0 : 1);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
