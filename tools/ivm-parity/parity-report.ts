/**
 * Combine the outputs of the parity sweep into one human-readable report
 * (`parity_report.md`). Reads:
 *   - `coverage_run.json`         — hydration sweep results
 *   - `advance_coverage_run.json` — push sweep results
 *   - `ast_corpus.json`           — corpus that was tested
 *
 * Output: a single markdown file consumable by humans, LLMs, and CI:
 *   - top-level summary (counts, % parity, elapsed)
 *   - bug catalog grouped by shape category
 *   - sample rendered ZQL per category
 *   - links to the raw JSON for deep triage
 *
 * Idempotent — re-running with the same inputs produces the same output.
 * Run after either harness completes; missing inputs are tolerated and
 * reflected in the report's "what was measured" section.
 */
import {readFileSync, writeFileSync, existsSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {renderAst} from './ast-render.ts';
import type {AST} from '../../packages/zero-protocol/src/ast.ts';

const here = dirname(fileURLToPath(import.meta.url));

type CorpusEntry = {id: string; ast: AST; kind: string};
type CovOutcome =
  | {status: 'ok'}
  | {status: 'diverge'; rowCounts: {ts: number; rs: number}; diff: string}
  | {status: 'error'; message: string; side?: string};
type CovResult = {id: string; ast: AST; outcome: CovOutcome};

type AdvOutcome =
  | {status: 'ok'}
  | {status: 'hydrate-diverge'; diff: string}
  | {status: 'advance-diverge'; diff: string}
  | {status: 'error'; phase: string; message: string};
type AdvResult = {id: string; ast: AST; outcome: AdvOutcome};

function tryRead<T>(path: string): T | undefined {
  if (!existsSync(path)) return undefined;
  try {
    return JSON.parse(readFileSync(path, 'utf8')) as T;
  } catch (e) {
    console.error(`failed to parse ${path}: ${(e as Error).message}`);
    return undefined;
  }
}

function shapeKey(ast: AST): string {
  const w = ast.where;
  if (!w) {
    if (ast.related) return 'related-only';
    if (ast.start) return 'start-cursor';
    if (ast.limit !== undefined) return 'limit-only';
    return 'bare';
  }
  if (w.type === 'simple') return `simple ${w.op}`;
  if (w.type === 'correlatedSubquery') {
    const flags: string[] = [];
    if (w.flip) flags.push('flip');
    if (w.scalar) flags.push('scalar');
    return `${w.op}${flags.length ? ' ' + flags.join('+') : ''}`;
  }
  const childKinds = w.conditions.map(c => c.type).join(',');
  return `${w.type}(${childKinds})`;
}

function safeRender(ast: AST): string {
  try {
    return renderAst(ast);
  } catch (e) {
    return `// renderer failed: ${(e as Error).message}`;
  }
}

function main(): void {
  const corpusPath = join(here, 'ast_corpus.json');
  const covPath = join(here, 'coverage_run.json');
  const advPath = join(here, 'advance_coverage_run.json');

  const corpus = tryRead<CorpusEntry[]>(corpusPath) ?? [];
  const cov = tryRead<{summary: any; results: CovResult[]; elapsedMs?: number}>(covPath);
  const adv = tryRead<{summary: any; results: AdvResult[]; elapsedMs?: number; mutations?: string[]}>(advPath);

  const lines: string[] = [];
  lines.push('# IVM TS/RS Parity Report');
  lines.push('');
  lines.push(`*Generated: ${new Date().toISOString()}*`);
  lines.push('');

  lines.push('## What was measured');
  lines.push('');
  lines.push(`- AST corpus: \`${corpusPath}\` — ${corpus.length} entries`);
  if (cov) {
    lines.push(`- Hydration sweep: \`${covPath}\` — ${cov.results.length} ASTs, ${((cov.elapsedMs ?? 0) / 1000).toFixed(1)}s`);
  } else {
    lines.push(`- Hydration sweep: NOT RUN (\`coverage_run.json\` missing — run \`npx tsx harness-coverage.ts\`)`);
  }
  if (adv) {
    lines.push(`- Advancement sweep: \`${advPath}\` — ${adv.results.length} ASTs, ${((adv.elapsedMs ?? 0) / 1000).toFixed(1)}s`);
    if (adv.mutations) {
      lines.push(`  - Mutation block: ${adv.mutations.length} steps`);
      for (const m of adv.mutations) lines.push(`    - ${m}`);
    }
  } else {
    lines.push(`- Advancement sweep: NOT RUN (\`advance_coverage_run.json\` missing — run \`npx tsx harness-advance-coverage.ts\`)`);
  }
  lines.push('');

  // ---- Top-line summary ----
  lines.push('## Summary');
  lines.push('');
  lines.push('| Metric | Hydration | Advancement |');
  lines.push('|---|---|---|');
  if (cov && adv) {
    const cs = cov.summary;
    const as_ = adv.summary;
    const cParity = cs.total > 0 ? ((cs.ok / cs.total) * 100).toFixed(1) : '–';
    const aParity = as_.total > 0 ? ((as_.ok / as_.total) * 100).toFixed(1) : '–';
    lines.push(`| Total ASTs | ${cs.total} | ${as_.total} |`);
    lines.push(`| OK | ${cs.ok} (${cParity}%) | ${as_.ok} (${aParity}%) |`);
    lines.push(`| Diverge | ${cs.diverge} | hyd=${as_.hydrateDiverge}, adv=${as_.advanceDiverge} |`);
    lines.push(`| Error | ${cs.error} | ${as_.error} |`);
  } else if (cov) {
    const cs = cov.summary;
    lines.push(`| Total ASTs | ${cs.total} | – |`);
    lines.push(`| OK | ${cs.ok} | – |`);
    lines.push(`| Diverge | ${cs.diverge} | – |`);
    lines.push(`| Error | ${cs.error} | – |`);
  } else if (adv) {
    const as_ = adv.summary;
    lines.push(`| Total ASTs | – | ${as_.total} |`);
    lines.push(`| OK | – | ${as_.ok} |`);
    lines.push(`| Diverge | – | hyd=${as_.hydrateDiverge}, adv=${as_.advanceDiverge} |`);
    lines.push(`| Error | – | ${as_.error} |`);
  } else {
    lines.push('| – | (no data) | (no data) |');
  }
  lines.push('');

  // ---- Bug catalog: hydration ----
  if (cov) {
    const divs = cov.results.filter(r => r.outcome.status === 'diverge');
    if (divs.length > 0) {
      lines.push('## Hydration divergences');
      lines.push('');
      lines.push(`${divs.length} AST(s) hydrate to different result sets between TS and RS.`);
      lines.push('');
      const grouped = new Map<string, CovResult[]>();
      for (const r of divs) {
        const k = shapeKey(r.ast);
        if (!grouped.has(k)) grouped.set(k, []);
        grouped.get(k)!.push(r);
      }
      lines.push('### By shape');
      lines.push('');
      lines.push('| Shape | Count | Example IDs |');
      lines.push('|---|---|---|');
      for (const [k, list] of [...grouped.entries()].sort((a, b) => b[1].length - a[1].length)) {
        const ids = list.slice(0, 3).map(r => r.id).join(', ');
        const more = list.length > 3 ? `, … +${list.length - 3}` : '';
        lines.push(`| \`${k}\` | ${list.length} | ${ids}${more} |`);
      }
      lines.push('');
      lines.push('### Sample (one per shape)');
      lines.push('');
      for (const [k, list] of [...grouped.entries()].sort((a, b) => b[1].length - a[1].length)) {
        const sample = list[0];
        const o = sample.outcome as Extract<CovOutcome, {status: 'diverge'}>;
        lines.push(`#### \`${k}\` — ${sample.id} (ts=${o.rowCounts.ts}, rs=${o.rowCounts.rs})`);
        lines.push('```ts');
        lines.push(safeRender(sample.ast));
        lines.push('```');
        lines.push('```');
        lines.push(o.diff);
        lines.push('```');
        lines.push('');
      }
    } else {
      lines.push('## Hydration divergences');
      lines.push('');
      lines.push('**None.** TS and RS hydrate every corpus AST to identical row sets.');
      lines.push('');
    }
  }

  // ---- Bug catalog: advancement ----
  if (adv) {
    const advDivs = adv.results.filter(r => r.outcome.status === 'advance-diverge');
    const hydDivs = adv.results.filter(r => r.outcome.status === 'hydrate-diverge');

    if (advDivs.length > 0) {
      lines.push('## Advancement divergences (push-path bugs)');
      lines.push('');
      lines.push(`${advDivs.length} AST(s) hydrate identically but produce different row deltas after the mutation block.`);
      lines.push('');
      const grouped = new Map<string, AdvResult[]>();
      for (const r of advDivs) {
        const k = shapeKey(r.ast);
        if (!grouped.has(k)) grouped.set(k, []);
        grouped.get(k)!.push(r);
      }
      lines.push('### By shape');
      lines.push('');
      lines.push('| Shape | Count | Example IDs |');
      lines.push('|---|---|---|');
      for (const [k, list] of [...grouped.entries()].sort((a, b) => b[1].length - a[1].length)) {
        const ids = list.slice(0, 3).map(r => r.id).join(', ');
        const more = list.length > 3 ? `, … +${list.length - 3}` : '';
        lines.push(`| \`${k}\` | ${list.length} | ${ids}${more} |`);
      }
      lines.push('');
      lines.push('### Sample (one per shape)');
      lines.push('');
      for (const [k, list] of [...grouped.entries()].sort((a, b) => b[1].length - a[1].length)) {
        const sample = list[0];
        const o = sample.outcome as Extract<AdvOutcome, {status: 'advance-diverge'}>;
        lines.push(`#### \`${k}\` — ${sample.id}`);
        lines.push('```ts');
        lines.push(safeRender(sample.ast));
        lines.push('```');
        lines.push('```');
        lines.push(o.diff);
        lines.push('```');
        lines.push('');
      }
    } else {
      lines.push('## Advancement divergences');
      lines.push('');
      lines.push('**None.** TS and RS produce identical row deltas after every mutation.');
      lines.push('');
    }

    if (hydDivs.length > 0) {
      lines.push(`## Hydrate divergences (caught by advancement sweep)`);
      lines.push('');
      lines.push(`${hydDivs.length} AST(s) hydrate to different sets — these are the same bugs as the hydration sweep but observed in the advancement run too.`);
      lines.push('');
    }
  }

  lines.push('## How to reproduce');
  lines.push('');
  lines.push('```bash');
  lines.push('cd tools/ivm-parity');
  lines.push('npx tsx ast-fuzz.ts                       # regenerate corpus');
  lines.push('npx tsx harness-coverage.ts               # hydration sweep (~5s)');
  lines.push('npx tsx harness-advance-coverage.ts       # advancement sweep (~2 min)');
  lines.push('npx tsx parity-report.ts                  # combine into parity_report.md');
  lines.push('```');
  lines.push('');
  lines.push('Or invoke the `ivm-parity-sweep` skill end-to-end.');
  lines.push('');

  const outPath = join(here, 'parity_report.md');
  writeFileSync(outPath, lines.join('\n'));
  console.error(`[parity-report] wrote ${outPath}`);
}

main();
