/**
 * Parity harness: runs every pattern in `patterns.ts` against two
 * zero-cache instances — one on port 4858 (TS IVM) and one on 4868
 * (Rust v2 IVM) — and prints a table showing where counts diverge.
 *
 * Usage:
 *   tsx harness.ts
 *
 * Assumes both zero-caches are already running (see run.sh) and are
 * both replicating from the same Postgres database seeded by
 * `seed.sql`. This harness doesn't manage the caches — it only
 * measures them, so you can keep them long-running and iterate.
 */
import {patterns, type PatternName} from './patterns.ts';
import {schema as paritySchema} from './zero-schema.ts';

type CountsByTable = Record<string, number>;
/**
 * Per-table set of rows keyed by canonical PK string. Each value is the
 * full row object as received in the `put` op's `value` field. Used for
 * field-by-field diffs, not just count parity.
 */
type RowsByTable = Record<string, Record<string, Record<string, unknown>>>;
type RunResult = {
  pattern: PatternName;
  totalRows: number;
  byTable: CountsByTable;
  rowsByTable: RowsByTable;
  error?: string;
};

const TS_URL =
  process.env.PARITY_TS_URL ?? 'ws://localhost:4858/sync/v49/connect';
const RS_URL =
  process.env.PARITY_RS_URL ?? 'ws://localhost:4868/sync/v49/connect';

/**
 * Extract the wire-format AST from a ZQL builder expression. Every
 * `QueryImpl` instance exposes its frozen AST via a `.ast` getter
 * (see `packages/zql/src/query/query-impl.ts`).
 */
function extractAst(query: unknown): unknown {
  const ast = (query as {ast?: unknown})?.ast;
  if (!ast) {
    throw new Error('extractAst: query object has no `.ast` getter');
  }
  return ast;
}

/**
 * Build the `clientSchema` payload. zero-cache requires this on the
 * first connect of a new client group so it can validate that the
 * client's view matches the replica's columns. Computed once per
 * harness run since it doesn't depend on the query.
 */
function buildClientSchema(): {
  tables: Record<
    string,
    {columns: Record<string, {type: string}>; primaryKey: string[]}
  >;
} {
  const clientTables: Record<
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
    clientTables[tableName] = {
      columns,
      primaryKey: [...tableDef.primaryKey],
    };
  }
  return {tables: clientTables};
}

/**
 * Open ONE WebSocket per pattern and count `put` rows received.
 *
 * Why one socket per pattern instead of one socket with all patterns:
 * `rowsPatch` ops in pokePart frames contain only `{op, tableName,
 * value/id}` — there is no per-query attribution on the wire (the row
 * may satisfy multiple queries). Issuing each query in its own client
 * group makes the row count for that connection unambiguously the
 * row count for that one query. We use a fresh `clientGroupID` so
 * each query gets a clean hydration with no shared CVR state.
 */
async function runOnePattern(
  url: string,
  hash: string,
  ast: unknown,
  clientSchema: ReturnType<typeof buildClientSchema>,
): Promise<{byTable: CountsByTable; rowsByTable: RowsByTable}> {
  const {default: WebSocket} = await import('ws');
  const {encodeSecProtocols} = await import(
    '../../packages/zero-protocol/src/connect.ts'
  );
  const byTable: CountsByTable = {};
  const rowsByTable: RowsByTable = {};
  // Track rows by primary-key per table so `del` ops decrement
  // correctly and `put`/`update` are idempotent — this matches what
  // a real Zero client does when applying a poke. We keep the FULL
  // row object (not just the PK) so we can diff every column value
  // between TS and RS, not just count rows.
  const remember = (table: string, key: string, value: Record<string, unknown>) => {
    if (!rowsByTable[table]) rowsByTable[table] = {};
    rowsByTable[table][key] = value;
  };
  const forget = (table: string, key: string) => {
    if (rowsByTable[table]) delete rowsByTable[table][key];
  };
  const canonicalKey = (pk: readonly string[], row: Record<string, unknown>) => {
    const obj: Record<string, unknown> = {};
    for (const k of pk) obj[k] = row?.[k];
    return JSON.stringify(obj);
  };
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error(`harness: timeout for ${hash}`)),
      Number(process.env.PARITY_TIMEOUT_MS ?? 10_000),
    );
    const initBody = {
      clientSchema,
      desiredQueriesPatch: [{op: 'put', hash, ast}],
    };
    const secProtocol = encodeSecProtocols(
      ['initConnection', initBody],
      undefined,
    );
    const cgid = `ivm-parity-${hash}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
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
    // The server emits TWO poke cycles on the initial connect:
    //   1. acks the `desiredQueriesPatch` (no rows).
    //   2. delivers the hydration rows AND a `gotQueriesPatch`
    //      naming each query that's now fully sync'd.
    // We close after seeing the pokeEnd that follows a `gotQueriesPatch`
    // for our pattern's hash — that's when hydration is complete.
    let pokeEnded = false;
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
      if (process.env.PARITY_DEBUG === '1') {
        // eslint-disable-next-line no-console
        console.log(
          `[debug ${hash}] tag=${tag} body=${JSON.stringify(body).slice(0, 200)}`,
        );
      }
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
              for (const k of Object.keys(rowsByTable)) delete rowsByTable[k];
            } else if (op.op === 'update') {
              // Merge into existing row, mirroring Zero's client semantics.
              const k = canonicalKey(pk, op.id);
              const prev = rowsByTable[t]?.[k] ?? {};
              remember(t, k, {...prev, ...(op.merge ?? {})});
            }
          }
        }
      } else if (tag === 'pokeEnd' && sawGotPatch) {
        pokeEnded = true;
        ws.close();
      }
    });
    // Safety net if no pokeEnd ever arrives.
    setTimeout(() => {
      if (!pokeEnded) ws.close();
    }, 5_000);
    ws.on('close', () => {
      clearTimeout(timeout);
      for (const [table, rows] of Object.entries(rowsByTable)) {
        byTable[table] = Object.keys(rows).length;
      }
      resolve();
    });
    ws.on('error', err => {
      clearTimeout(timeout);
      reject(err);
    });
  });
  return {byTable, rowsByTable};
}

async function runAll(url: string, label: string): Promise<RunResult[]> {
  const clientSchema = buildClientSchema();
  const results: RunResult[] = [];
  for (const [hash, build] of Object.entries(patterns) as Array<
    [PatternName, () => unknown]
  >) {
    try {
      const {byTable, rowsByTable} = await runOnePattern(
        url,
        hash,
        extractAst(build()),
        clientSchema,
      );
      const totalRows = Object.values(byTable).reduce((a, b) => a + b, 0);
      results.push({pattern: hash, totalRows, byTable, rowsByTable});
    } catch (err) {
      results.push({
        pattern: hash,
        totalRows: 0,
        byTable: {},
        rowsByTable: {},
        error: (err as Error).message,
      });
    }
  }
  // eslint-disable-next-line no-console
  console.log(`[${label}] received ${results.length} pattern results`);
  return results;
}

/**
 * Field-level diff between TS and RS row sets for one pattern.
 * Returns null when fully equal, otherwise a structured description
 * of the first few differences (capped to keep logs readable).
 */
type ValueDiff = {
  missingInRs: Array<{table: string; key: string; row: Record<string, unknown>}>;
  missingInTs: Array<{table: string; key: string; row: Record<string, unknown>}>;
  fieldDiffs: Array<{
    table: string;
    key: string;
    field: string;
    ts: unknown;
    rs: unknown;
  }>;
};
function diffRows(ts: RowsByTable, rs: RowsByTable): ValueDiff | null {
  const out: ValueDiff = {missingInRs: [], missingInTs: [], fieldDiffs: []};
  const tables = new Set([...Object.keys(ts), ...Object.keys(rs)]);
  for (const table of tables) {
    const tsRows = ts[table] ?? {};
    const rsRows = rs[table] ?? {};
    const keys = new Set([...Object.keys(tsRows), ...Object.keys(rsRows)]);
    for (const key of keys) {
      const a = tsRows[key];
      const b = rsRows[key];
      if (a === undefined) {
        out.missingInTs.push({table, key, row: b});
        continue;
      }
      if (b === undefined) {
        out.missingInRs.push({table, key, row: a});
        continue;
      }
      const fieldNames = new Set([...Object.keys(a), ...Object.keys(b)]);
      for (const f of fieldNames) {
        if (JSON.stringify(a[f]) !== JSON.stringify(b[f])) {
          out.fieldDiffs.push({table, key, field: f, ts: a[f], rs: b[f]});
        }
      }
    }
  }
  if (
    out.missingInRs.length === 0 &&
    out.missingInTs.length === 0 &&
    out.fieldDiffs.length === 0
  ) {
    return null;
  }
  return out;
}

function diff(
  ts: RunResult[],
  rs: RunResult[],
): Array<{
  pattern: PatternName;
  tsTotal: number;
  rsTotal: number;
  countMatch: boolean;
  valueDiff: ValueDiff | null;
  tsByTable: CountsByTable;
  rsByTable: CountsByTable;
}> {
  const tsByPattern = new Map(ts.map(r => [r.pattern, r]));
  const rsByPattern = new Map(rs.map(r => [r.pattern, r]));
  const out = [];
  for (const pattern of Object.keys(patterns) as PatternName[]) {
    const t = tsByPattern.get(pattern);
    const r = rsByPattern.get(pattern);
    const tsTotal = t?.totalRows ?? 0;
    const rsTotal = r?.totalRows ?? 0;
    out.push({
      pattern,
      tsTotal,
      rsTotal,
      countMatch: tsTotal === rsTotal,
      valueDiff: diffRows(t?.rowsByTable ?? {}, r?.rowsByTable ?? {}),
      tsByTable: t?.byTable ?? {},
      rsByTable: r?.byTable ?? {},
    });
  }
  return out;
}

async function main() {
  const ts = await runAll(TS_URL, 'TS');
  const rs = await runAll(RS_URL, 'RS');
  const cmp = diff(ts, rs);
  const mismatch = cmp.filter(c => !c.countMatch || c.valueDiff !== null);
  // eslint-disable-next-line no-console
  console.log('\n=== parity report ===');
  for (const c of cmp) {
    const ok = c.countMatch && c.valueDiff === null;
    const mark = ok ? 'OK' : c.countMatch ? 'VALUE-DIVERGE' : 'COUNT-DIVERGE';
    // eslint-disable-next-line no-console
    console.log(
      `${mark.padEnd(14)} ${c.pattern.padEnd(32)} ts=${String(c.tsTotal).padStart(5)} rs=${String(c.rsTotal).padStart(5)}`,
    );
    if (!c.countMatch) {
      // eslint-disable-next-line no-console
      console.log(`           ts by-table: ${JSON.stringify(c.tsByTable)}`);
      // eslint-disable-next-line no-console
      console.log(`           rs by-table: ${JSON.stringify(c.rsByTable)}`);
    }
    if (c.valueDiff !== null) {
      const v = c.valueDiff;
      // eslint-disable-next-line no-console
      console.log(
        `           value diff: missingInRs=${v.missingInRs.length} missingInTs=${v.missingInTs.length} fieldDiffs=${v.fieldDiffs.length}`,
      );
      const sample = (label: string, items: unknown[]) => {
        for (const item of items.slice(0, 3)) {
          // eslint-disable-next-line no-console
          console.log(`             ${label}: ${JSON.stringify(item)}`);
        }
        if (items.length > 3) {
          // eslint-disable-next-line no-console
          console.log(`             ${label}: ... +${items.length - 3} more`);
        }
      };
      sample('missing-in-rs', v.missingInRs);
      sample('missing-in-ts', v.missingInTs);
      sample('field-diff', v.fieldDiffs);
    }
  }
  // eslint-disable-next-line no-console
  console.log(`\ndiverged: ${mismatch.length}/${cmp.length}`);
  process.exit(mismatch.length === 0 ? 0 : 1);
}

main().catch(err => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
