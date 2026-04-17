/**
 * Phase 1 of the coverage-driven IVM parity harness.
 *
 * Bounded BFS enumeration over the ZQL `AST` / `Condition` ADT against any
 * `@rocicorp/zero` schema (loaded at runtime from `./zero-schema.ts` by
 * default — point `SCHEMA_FILE` env at a different module to fuzz a
 * different surface). Produces a deterministic deduplicated corpus of
 * ASTs that the harness ships to both TS and RS zero-cache to drive
 * branch coverage and surface parity gaps.
 *
 * Designed to be reusable: schema is data, not code. Drop in a new
 * zero-schema.ts and the corpus regenerates automatically — the only
 * coupling is to the AST type definition in zero-protocol.
 *
 * Hard caps prevent recursion blow-up:
 *   MAX_WHERE_DEPTH       — and/or/csq nesting under `where`
 *   MAX_BRANCHES_PER_NODE — children of an And/Or
 *   MAX_RELATED_DEPTH     — nesting under `related[]`
 *   MAX_RELATED_PER_NODE  — siblings inside one `related[]`
 *   MAX_RELATIONSHIP_HOPS — total relationship traversals on any path
 *   MAX_NODES             — global per-AST node budget (BFS escape)
 *
 * No DB writes, no TS edits, no agents — pure code.
 */
import {writeFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {
  type AST,
  type Condition,
  type Conjunction,
  type CorrelatedSubquery,
  type CorrelatedSubqueryCondition,
  type Disjunction,
  type LiteralValue,
  type Ordering,
  type SimpleCondition,
  type SimpleOperator,
  normalizeAST,
} from '../../packages/zero-protocol/src/ast.ts';
import {renderAst} from './ast-render.ts';

// ---------------------------------------------------------------------------
// Caps. Tunable from env when iterating.
// ---------------------------------------------------------------------------
const MAX_WHERE_DEPTH = num('MAX_WHERE_DEPTH', 3);
const MAX_BRANCHES_PER_NODE = num('MAX_BRANCHES_PER_NODE', 2);
const MAX_RELATED_DEPTH = num('MAX_RELATED_DEPTH', 2);
const MAX_RELATED_PER_NODE = num('MAX_RELATED_PER_NODE', 2);
const MAX_RELATIONSHIP_HOPS = num('MAX_RELATIONSHIP_HOPS', 3);
const MAX_NODES = num('MAX_NODES', 8);

// Per-stratum sample caps. Each stratum gets a deterministic prefix slice;
// no Math.random — same caps + same schema always produce the same corpus.
// SIMPLES_PER_TABLE is sized to hold every (col, op) primary entry — bump
// proportionally if you add many columns to the schema.
const SIMPLES_PER_TABLE = num('SIMPLES_PER_TABLE', 96);
const ANDS_PER_TABLE = num('ANDS_PER_TABLE', 12);
const ORS_PER_TABLE = num('ORS_PER_TABLE', 12);
const DEEP_PER_TABLE = num('DEEP_PER_TABLE', 16);
const MAX_RELATED_PER_TABLE = num('MAX_RELATED_PER_TABLE', 6);
const JOINT_QUERIES_PER_TABLE = num('JOINT_QUERIES_PER_TABLE', 8);

// Sentinel literal values per type. Kept small (1 matching, 1 non-matching)
// — code-path coverage cares about op×type, not value cardinality.
const STRING_LITERALS = ['ch-pub-1', '__none__'];
const NUMBER_LITERALS = [1000, 9999];
const LIKE_PATTERNS = ['%standup%', 'no-match-%'];

function num(key: string, def: number): number {
  const v = process.env[key];
  if (!v) return def;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`bad ${key}=${v}`);
  return n;
}

// ---------------------------------------------------------------------------
// Internal schema model (data-only, decoupled from @rocicorp/zero internals).
// ---------------------------------------------------------------------------
type ColType = 'string' | 'number' | 'string?' | 'number?' | 'boolean' | 'unknown';
interface ColumnDef {
  name: string;
  type: ColType;
}
interface RelationshipDef {
  name: string;
  childTable: string;
  parentField: readonly string[];
  childField: readonly string[];
  cardinality: 'one' | 'many';
}
interface TableDef {
  name: string;
  pk: readonly string[];
  columns: readonly ColumnDef[];
  relationships: readonly RelationshipDef[];
}

// ---------------------------------------------------------------------------
// Schema loader: adapt any @rocicorp/zero schema export into our TableDef.
// One-hop relationships only (the standard zero schema shape). Junction-edge
// (`hidden:true`) relationships flatten to two hops automatically because the
// AST treats them as nested `related[]` entries.
// ---------------------------------------------------------------------------
function adaptSchema(zeroSchema: {
  tables: Record<string, {name: string; columns: Record<string, {type: string; optional: boolean}>; primaryKey: readonly string[]}>;
  relationships: Record<string, Record<string, readonly {sourceField: readonly string[]; destField: readonly string[]; destSchema: string; cardinality: 'one' | 'many'}[]>>;
}): Record<string, TableDef> {
  const out: Record<string, TableDef> = {};
  for (const [tableName, table] of Object.entries(zeroSchema.tables)) {
    const columns: ColumnDef[] = Object.entries(table.columns).map(([name, c]) => ({
      name,
      type: zeroTypeToInternal(c.type, c.optional),
    }));
    const rels = zeroSchema.relationships[tableName] ?? {};
    const relationships: RelationshipDef[] = [];
    for (const [relName, hops] of Object.entries(rels)) {
      // Junction edges produce a hops.length === 2 chain. The AST sees a
      // nested CorrelatedSubquery in either case; we surface only the first
      // hop here and let the recursive related[] generator descend.
      const first = hops[0];
      if (!first) continue;
      relationships.push({
        name: relName,
        childTable: first.destSchema,
        parentField: first.sourceField,
        childField: first.destField,
        cardinality: first.cardinality,
      });
    }
    out[tableName] = {
      name: table.name,
      pk: table.primaryKey,
      columns,
      relationships,
    };
  }
  return out;
}

function zeroTypeToInternal(type: string, optional: boolean): ColType {
  const opt = optional ? '?' : '';
  if (type === 'string') return (optional ? 'string?' : 'string') as ColType;
  if (type === 'number') return (optional ? 'number?' : 'number') as ColType;
  if (type === 'boolean') return 'boolean';
  return 'unknown';
}

// ---------------------------------------------------------------------------
// Operator coverage: legal ops per column type, sentinel values per op.
// ---------------------------------------------------------------------------
const EQ_OPS: SimpleOperator[] = ['=', '!='];
const ORD_OPS: SimpleOperator[] = ['<', '>', '<=', '>='];
const LIKE_OPS: SimpleOperator[] = ['LIKE', 'NOT LIKE', 'ILIKE', 'NOT ILIKE'];
const IN_OPS: SimpleOperator[] = ['IN', 'NOT IN'];
const NULL_OPS: SimpleOperator[] = ['IS', 'IS NOT'];

function opsForColumn(col: ColumnDef): SimpleOperator[] {
  const isString = col.type === 'string' || col.type === 'string?';
  const isNumber = col.type === 'number' || col.type === 'number?';
  const isOptional = col.type === 'string?' || col.type === 'number?';
  // NULL_OPS placed FIRST when applicable so the round-robin emits IS / IS NOT
  // before deeper variants — these are rare ops and should always survive caps.
  const ops: SimpleOperator[] = [];
  if (isOptional) ops.push(...NULL_OPS);
  ops.push(...EQ_OPS, ...IN_OPS);
  if (isString) ops.push(...LIKE_OPS, ...ORD_OPS);
  if (isNumber) ops.push(...ORD_OPS);
  return ops;
}

function valuesForOp(col: ColumnDef, op: SimpleOperator): LiteralValue[] {
  if (op === 'IS' || op === 'IS NOT') return [null];
  if (op === 'IN' || op === 'NOT IN') {
    if (col.type === 'number' || col.type === 'number?') {
      return [[], [NUMBER_LITERALS[0]], [NUMBER_LITERALS[0], NUMBER_LITERALS[1]]];
    }
    return [[], [STRING_LITERALS[0]], [STRING_LITERALS[0], STRING_LITERALS[1]]];
  }
  if (op === 'LIKE' || op === 'NOT LIKE' || op === 'ILIKE' || op === 'NOT ILIKE') {
    return LIKE_PATTERNS;
  }
  if (col.type === 'number' || col.type === 'number?') return NUMBER_LITERALS;
  return STRING_LITERALS;
}

// ---------------------------------------------------------------------------
// Generators. Deterministic, bounded.
// ---------------------------------------------------------------------------
type GenCtx = {
  /** parent tables already entered through relationships, for cycle detection. */
  readonly ancestorTables: ReadonlySet<string>;
  /** total relationship hops taken on the active path. */
  readonly hops: number;
};

function freshCtx(table: string): GenCtx {
  return {ancestorTables: new Set([table]), hops: 0};
}

let aliasCounter = 0;
function nextAlias(rel: string): string {
  return `zsubq_${rel}_${aliasCounter++}`;
}

function cKey(arr: readonly string[]): readonly [string, ...string[]] {
  if (arr.length === 0) throw new Error('empty compound key');
  return [arr[0], ...arr.slice(1)] as readonly [string, ...string[]];
}

function simpleConditions(table: TableDef): SimpleCondition[] {
  // Two passes — primary = one entry per (col, op) using the first value
  // strategy; secondary = remaining value strategies. Within each pass we
  // round-robin across columns so a prefix slice keeps every (col, op)
  // represented (including rare NULL_OPS on nullable cols).
  const perCol = table.columns.map(col => {
    const primary: SimpleCondition[] = [];
    const secondary: SimpleCondition[] = [];
    for (const op of opsForColumn(col)) {
      const values = valuesForOp(col, op);
      values.forEach((value, i) => {
        const cond: SimpleCondition = {
          type: 'simple',
          op,
          left: {type: 'column', name: col.name},
          right: {type: 'literal', value},
        };
        if (i === 0) primary.push(cond);
        else secondary.push(cond);
      });
    }
    return {primary, secondary};
  });

  // Primary first. callers using SIMPLES_PER_TABLE should pick a value ≥
  // sum(primary lengths) to keep every (col, op) — kept open for tuning.
  return [...roundRobin(perCol.map(c => c.primary)), ...roundRobin(perCol.map(c => c.secondary))];
}

/** Interleave N arrays element-by-element until all are exhausted. */
function roundRobin<T>(lists: T[][]): T[] {
  const out: T[] = [];
  let idx = 0;
  while (true) {
    let any = false;
    for (const l of lists) {
      if (idx < l.length) {
        out.push(l[idx]);
        any = true;
      }
    }
    if (!any) return out;
    idx++;
  }
}

function csqLeafConditions(table: TableDef, ctx: GenCtx): CorrelatedSubqueryCondition[] {
  if (ctx.hops >= MAX_RELATIONSHIP_HOPS) return [];
  const out: CorrelatedSubqueryCondition[] = [];
  for (const rel of table.relationships) {
    if (ctx.ancestorTables.has(rel.childTable)) continue;
    for (const op of ['EXISTS', 'NOT EXISTS'] as const) {
      // Cross flip × scalar — the planner cares about all four combos
      // (or rejects some, e.g. NOT EXISTS + flip:true; we drop those).
      for (const flip of [undefined, true, false] as const) {
        for (const scalar of [undefined, true] as const) {
          if (op === 'NOT EXISTS' && flip === true) continue;
          out.push({
            type: 'correlatedSubquery',
            related: {
              correlation: {parentField: cKey(rel.parentField), childField: cKey(rel.childField)},
              subquery: {table: rel.childTable, alias: nextAlias(rel.name)},
            },
            op,
            flip,
            scalar,
          });
        }
      }
    }
  }
  return out;
}

/**
 * For each csq leaf, also produce a variant whose subquery has its own
 * one-leaf where — exercises decoration + nested-where push paths
 * (matches user-reported pain shapes p18/p19/p29 etc.).
 */
function csqWithSubWhere(table: TableDef, ctx: GenCtx): CorrelatedSubqueryCondition[] {
  if (ctx.hops + 1 >= MAX_RELATIONSHIP_HOPS) return [];
  const out: CorrelatedSubqueryCondition[] = [];
  for (const rel of table.relationships) {
    if (ctx.ancestorTables.has(rel.childTable)) continue;
    const childTable = SCHEMA[rel.childTable];
    if (!childTable) continue;
    const childLeaves = simpleConditions(childTable);
    if (childLeaves.length === 0) continue;
    // Pick a few diverse leaves: first '=' and first 'ILIKE' if present.
    const picks: SimpleCondition[] = [];
    const eq = childLeaves.find(l => l.op === '=');
    const il = childLeaves.find(l => l.op === 'ILIKE');
    if (eq) picks.push(eq);
    if (il) picks.push(il);
    if (picks.length === 0) picks.push(childLeaves[0]);

    for (const subWhere of picks) {
      for (const op of ['EXISTS', 'NOT EXISTS'] as const) {
        for (const flip of [undefined, true] as const) {
          if (op === 'NOT EXISTS' && flip === true) continue;
          out.push({
            type: 'correlatedSubquery',
            related: {
              correlation: {parentField: cKey(rel.parentField), childField: cKey(rel.childField)},
              subquery: {table: rel.childTable, alias: nextAlias(rel.name), where: subWhere},
            },
            op,
            flip,
          });
        }
      }
    }
  }
  return out;
}

/**
 * Stratified where-shape generator. Returns a deduped flat list whose
 * categories are guaranteed represented (vs naive pickPairs which lets
 * one stratum starve the others through prefix slicing).
 */
function whereShapes(table: TableDef, ctx: GenCtx): Condition[] {
  const simples = simpleConditions(table);
  const csqs = csqLeafConditions(table, ctx);
  const csqsDeep = csqWithSubWhere(table, ctx);
  const csqAll = [...csqs, ...csqsDeep];

  // Combinator leaf pool: small to keep depth-2 finite. Deliberately mix
  // simples and csqs so AND/OR cover both kinds.
  const leafPool: Condition[] = [
    ...simples.slice(0, 4),
    ...csqs.slice(0, 6),
  ];

  // Depth-1 AND / OR over leafPool.
  const ands: Conjunction[] = [];
  const ors: Disjunction[] = [];
  for (let i = 0; i < leafPool.length; i++) {
    for (let j = i + 1; j < leafPool.length; j++) {
      if (MAX_BRANCHES_PER_NODE >= 2) {
        ands.push({type: 'and', conditions: [leafPool[i], leafPool[j]]});
        ors.push({type: 'or', conditions: [leafPool[i], leafPool[j]]});
      }
    }
  }

  // Depth-2: stratified across category so a prefix slice doesn't drop
  // p36-style OR-of-AND. Categories are emitted round-robin so any cap
  // hits each kind.
  const deep: Condition[] = [];
  if (MAX_WHERE_DEPTH >= 2) {
    const orPool = ors.slice(0, 4);
    const andPool = ands.slice(0, 4);
    const leafShortlist = [...simples.slice(0, 2), ...csqs.slice(0, 2)];
    const cat_andOrLeaf: Condition[] = [];
    const cat_orOrLeaf: Condition[] = [];
    const cat_orAndLeaf: Condition[] = []; // p36 shape
    const cat_andAndLeaf: Condition[] = [];
    for (const o of orPool) {
      for (const leaf of leafShortlist) {
        cat_andOrLeaf.push({type: 'and', conditions: [o, leaf]});
        cat_orOrLeaf.push({type: 'or', conditions: [o, leaf]});
      }
    }
    for (const a of andPool) {
      for (const leaf of leafShortlist) {
        cat_orAndLeaf.push({type: 'or', conditions: [a, leaf]});
        cat_andAndLeaf.push({type: 'and', conditions: [a, leaf]});
      }
    }
    deep.push(...roundRobin([cat_andOrLeaf, cat_orOrLeaf, cat_orAndLeaf, cat_andAndLeaf]));
    // Pure-CSQ OR (p32 shape) at the front so dedup never drops it.
    if (csqs.length >= 2) {
      deep.unshift({type: 'or', conditions: [csqs[0], csqs[1]]});
      deep.unshift({type: 'and', conditions: [csqs[0], csqs[1]]});
    }
  }

  // Depth-3: AND/OR of two depth-2 nodes — capped, the heavy hitter.
  if (MAX_WHERE_DEPTH >= 3 && deep.length >= 2) {
    const a = deep[0];
    const b = deep[1];
    deep.push({type: 'and', conditions: [a, b]});
    deep.push({type: 'or', conditions: [a, b]});
  }

  // Stratified slice — each category contributes a known share.
  // Simples take ALL primary entries (already (col,op)-unique) plus a slice
  // of secondary so rare ops like IS/IS NOT always survive.
  const out: Condition[] = [
    ...simples.slice(0, SIMPLES_PER_TABLE),
    ...csqAll, // keep all CSQs — they're high value, naturally bounded
    ...ands.slice(0, ANDS_PER_TABLE),
    ...ors.slice(0, ORS_PER_TABLE),
    ...deep.slice(0, DEEP_PER_TABLE),
  ];
  return out.filter(c => conditionNodes(c) <= MAX_NODES - 1);
}

function conditionNodes(c: Condition): number {
  if (c.type === 'simple') return 1;
  if (c.type === 'correlatedSubquery') {
    return 1 + (c.related.subquery.where ? conditionNodes(c.related.subquery.where) : 0);
  }
  let n = 1;
  for (const child of c.conditions) n += conditionNodes(child);
  return n;
}

function astNodes(ast: AST): number {
  let n = 1;
  if (ast.where) n += conditionNodes(ast.where);
  if (ast.related) {
    for (const r of ast.related) n += astNodes(r.subquery);
  }
  return n;
}

/**
 * `related[]` shapes up to MAX_RELATED_DEPTH. Each entry is a candidate
 * value for `AST.related`.
 */
function relatedShapes(table: TableDef, ctx: GenCtx): readonly CorrelatedSubquery[][] {
  if (ctx.hops >= MAX_RELATIONSHIP_HOPS) return [[]];
  const rels = table.relationships.filter(r => !ctx.ancestorTables.has(r.childTable));
  if (rels.length === 0) return [[]];

  const out: CorrelatedSubquery[][] = [[]];

  for (const rel of rels) {
    out.push([buildCsq(rel, undefined)]);
  }
  if (MAX_RELATED_PER_NODE >= 2 && rels.length >= 2) {
    out.push([buildCsq(rels[0], undefined), buildCsq(rels[1], undefined)]);
  }
  if (MAX_RELATED_DEPTH >= 2) {
    for (const rel of rels) {
      const childCtx: GenCtx = {
        ancestorTables: new Set([...ctx.ancestorTables, rel.childTable]),
        hops: ctx.hops + 1,
      };
      const childTable = SCHEMA[rel.childTable];
      if (!childTable) continue;
      const grand = childTable.relationships.find(r => !childCtx.ancestorTables.has(r.childTable));
      if (!grand) continue;
      out.push([buildCsq(rel, [buildCsq(grand, undefined)])]);
    }
  }
  return out.slice(0, MAX_RELATED_PER_TABLE);
}

function buildCsq(rel: RelationshipDef, related: readonly CorrelatedSubquery[] | undefined): CorrelatedSubquery {
  return {
    correlation: {parentField: cKey(rel.parentField), childField: cKey(rel.childField)},
    subquery: {table: rel.childTable, alias: nextAlias(rel.name), related},
  };
}

// ---------------------------------------------------------------------------
// Top-level options: orderBy + limit + start. Each branch is a distinct
// path in Take/Skip/orderBy code, so each is enumerated exactly once and
// applied as a *facet* — no cartesian product across the table.
// ---------------------------------------------------------------------------
type TopOption = {limit?: number; orderBy?: Ordering; start?: AST['start']};

function topOptionsForTable(table: TableDef): TopOption[] {
  const out: TopOption[] = [{}];
  const pkCol = table.pk[0];
  const otherCol = table.columns.find(c => !table.pk.includes(c.name))?.name;
  const orderBy1: Ordering = [[pkCol, 'asc']];
  const orderBy2: Ordering | undefined = otherCol
    ? [[otherCol, 'desc'], [pkCol, 'desc']]
    : undefined;
  out.push({limit: 1});
  out.push({limit: 5, orderBy: orderBy1});
  if (orderBy2) {
    out.push({limit: 3, orderBy: orderBy2});
    out.push({limit: 5, orderBy: orderBy2, start: {row: sampleRow(table, orderBy2), exclusive: false}});
    out.push({limit: 5, orderBy: orderBy2, start: {row: sampleRow(table, orderBy2), exclusive: true}});
  }
  return out;
}

const DEFAULT_TOP: TopOption = {};

function sampleRow(table: TableDef, ordering: Ordering): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  for (const [col] of ordering) {
    const def = table.columns.find(c => c.name === col);
    row[col] = def?.type === 'number' || def?.type === 'number?' ? NUMBER_LITERALS[1] : STRING_LITERALS[0];
  }
  return row;
}

// ---------------------------------------------------------------------------
// Per-table generator. Three facets (no cartesian explosion):
//   1. WHERE coverage   — every where-shape × DEFAULT_TOP (no related[])
//   2. RELATED coverage — every related-shape × DEFAULT_TOP (no where)
//   3. TOP coverage     — every TopOption × bare query
//   4. JOINT facet      — small cross of where × related × non-default top
//                          to exercise interactions (Filter+Take, etc.)
// Each facet contributes once per shape, keeping the corpus ~1 query per
// distinct code path rather than the naive product.
// ---------------------------------------------------------------------------
function generateForTable(table: TableDef): AST[] {
  const ctx = freshCtx(table.name);
  const wheres = whereShapes(table, ctx);
  const relateds = relatedShapes(table, ctx).slice(0, MAX_RELATED_PER_TABLE);
  const tops = topOptionsForTable(table);

  const out: AST[] = [];

  // 1. WHERE coverage: each where shape gets exactly one query.
  out.push({table: table.name});
  for (const w of wheres) {
    out.push({table: table.name, where: w});
  }

  // 2. RELATED coverage: each related shape gets exactly one query.
  for (const r of relateds) {
    if (r.length === 0) continue; // bare query already in WHERE coverage
    out.push({table: table.name, related: r});
  }

  // 3. TOP coverage: each top option gets one bare query.
  for (const t of tops) {
    if (!t.limit && !t.orderBy && !t.start) continue;
    if (t.start && !t.orderBy) continue;
    out.push({
      table: table.name,
      limit: t.limit,
      orderBy: t.orderBy,
      start: t.start,
    });
  }

  // 4. JOINT facet: take the first few interesting wheres × first related ×
  // a non-default top. Exercises Filter→Take, Filter→Join, etc.
  const interestingWheres = wheres.filter(
    w => w.type === 'or' || w.type === 'and' || w.type === 'correlatedSubquery',
  ).slice(0, JOINT_QUERIES_PER_TABLE);
  const firstRel = relateds.find(r => r.length > 0);
  const richTop = tops.find(t => t.orderBy && t.limit);
  if (firstRel && richTop) {
    for (const w of interestingWheres) {
      out.push({
        table: table.name,
        where: w,
        related: firstRel,
        limit: richTop.limit,
        orderBy: richTop.orderBy,
        start: richTop.start,
      });
    }
  }

  // Cap node budget defensively.
  return out.filter(ast => astNodes(ast) <= MAX_NODES);
}

// ---------------------------------------------------------------------------
// Dedup via canonical-form hash. Aliases are stripped; structurally
// identical ASTs collapse. Falls back to raw stringify if normalize throws.
// ---------------------------------------------------------------------------
function dedup(asts: AST[]): AST[] {
  const seen = new Set<string>();
  const out: AST[] = [];
  for (const ast of asts) {
    let key: string;
    try {
      key = canonicalKey(normalizeAST(ast));
    } catch {
      key = canonicalKey(ast);
    }
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(ast);
  }
  return out;
}

function canonicalKey(ast: unknown): string {
  return JSON.stringify(stripAliases(ast), sortKeys);
}

function stripAliases(node: unknown): unknown {
  if (Array.isArray(node)) return node.map(stripAliases);
  if (node && typeof node === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(node)) {
      if (k === 'alias') continue;
      out[k] = stripAliases(v);
    }
    return out;
  }
  return node;
}

function sortKeys(_k: string, v: unknown): unknown {
  if (v && typeof v === 'object' && !Array.isArray(v)) {
    return Object.fromEntries(
      Object.entries(v as Record<string, unknown>).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
    );
  }
  return v;
}

// ---------------------------------------------------------------------------
// Entry. Loads schema → adapts → enumerates → dedups → writes.
// ---------------------------------------------------------------------------
type CorpusEntry = {id: string; ast: AST; kind: 'fuzz'};

let SCHEMA: Record<string, TableDef> = {};

async function main(): Promise<void> {
  const start = Date.now();

  const schemaModule = process.env.SCHEMA_FILE ?? './zero-schema.ts';
  const here = dirname(fileURLToPath(import.meta.url));
  const mod = await import(join(here, schemaModule));
  if (!mod.schema) throw new Error(`schema export missing in ${schemaModule}`);
  SCHEMA = adaptSchema(mod.schema);

  const all: AST[] = [];
  for (const table of Object.values(SCHEMA)) {
    all.push(...generateForTable(table));
  }
  const deduped = dedup(all);

  for (const ast of deduped) {
    const n = astNodes(ast);
    if (n > MAX_NODES) {
      throw new Error(`AST exceeds MAX_NODES=${MAX_NODES} (got ${n}): ${JSON.stringify(ast)}`);
    }
  }

  const corpus: CorpusEntry[] = deduped.map((ast, i) => ({
    id: `fuzz_${i.toString().padStart(5, '0')}`,
    ast,
    kind: 'fuzz' as const,
  }));

  const outPath = join(here, 'ast_corpus.json');
  writeFileSync(outPath, JSON.stringify(corpus, null, 2));

  // Sidecar: human/LLM-readable ZQL builder rendering of every entry.
  // Use this to grok what each fuzz id actually queries, paste into a
  // REPL, or include in PR descriptions.
  const zqlOutPath = join(here, 'ast_corpus.zql.txt');
  const zqlBlocks = corpus.map(e => {
    let body: string;
    try {
      body = renderAst(e.ast);
    } catch (err) {
      body = `// renderer failed: ${(err as Error).message}\n// raw AST: ${JSON.stringify(e.ast)}`;
    }
    return `// ===== ${e.id} =====\n${body};`;
  });
  writeFileSync(zqlOutPath, zqlBlocks.join('\n\n') + '\n');

  const elapsed = Date.now() - start;
  const summary = {
    schemaModule,
    tables: Object.keys(SCHEMA),
    total: corpus.length,
    elapsedMs: elapsed,
    perTable: Object.fromEntries(
      Object.keys(SCHEMA).map(t => [t, corpus.filter(c => c.ast.table === t).length]),
    ),
    byShape: {
      noWhere: corpus.filter(c => !c.ast.where).length,
      simpleWhere: corpus.filter(c => c.ast.where?.type === 'simple').length,
      andWhere: corpus.filter(c => c.ast.where?.type === 'and').length,
      orWhere: corpus.filter(c => c.ast.where?.type === 'or').length,
      csqWhere: corpus.filter(c => c.ast.where?.type === 'correlatedSubquery').length,
      withRelated: corpus.filter(c => c.ast.related).length,
      withLimit: corpus.filter(c => c.ast.limit !== undefined).length,
      withStart: corpus.filter(c => c.ast.start).length,
      flipTrue: corpus.filter(c => JSON.stringify(c.ast).includes('"flip":true')).length,
      flipFalse: corpus.filter(c => JSON.stringify(c.ast).includes('"flip":false')).length,
      scalarTrue: corpus.filter(c => JSON.stringify(c.ast).includes('"scalar":true')).length,
      notExists: corpus.filter(c => JSON.stringify(c.ast).includes('"NOT EXISTS"')).length,
    },
    caps: {
      MAX_WHERE_DEPTH,
      MAX_BRANCHES_PER_NODE,
      MAX_RELATED_DEPTH,
      MAX_RELATED_PER_NODE,
      MAX_RELATIONSHIP_HOPS,
      MAX_NODES,
    },
  };
  console.error(JSON.stringify(summary, null, 2));

  if (elapsed > 60_000) {
    throw new Error(`fuzzer exceeded 60s budget (${elapsed}ms)`);
  }
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
