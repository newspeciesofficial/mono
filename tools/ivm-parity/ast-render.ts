/**
 * AST → human-readable ZQL builder string.
 *
 * Reverse of the fuzzer: takes a raw AST (the wire format Zero uses) and
 * renders it as something you'd actually type into a REPL or paste into
 * a query file. Used to write `ast_corpus.zql.txt` alongside the JSON
 * corpus and to include builder-style snippets in parity_gaps.md so a
 * human or LLM reading the divergence report can grok the query without
 * decoding raw AST.
 *
 * Best-effort: relationship names are recovered from CSQ aliases (which
 * the fuzzer stamps as `zsubq_<relName>_<counter>`). If an AST came from
 * outside the fuzzer with no alias, we fall back to the destination
 * table name.
 */
import type {
  AST,
  Condition,
  CorrelatedSubquery,
  CorrelatedSubqueryCondition,
  Disjunction,
  Conjunction,
  LiteralValue,
  Ordering,
  SimpleCondition,
  ValuePosition,
} from '../../packages/zero-protocol/src/ast.ts';

const INDENT = '  ';

export function renderAst(ast: AST): string {
  const lines: string[] = [`zql.${ast.table}`];
  emitChain(ast, 1, lines);
  return lines.join('\n');
}

function emitChain(ast: AST, depth: number, lines: string[]): void {
  if (ast.where) {
    const w = ast.where;
    if (w.type === 'simple') {
      lines.push(`${pad(depth)}.where(${renderSimpleArgs(w)})`);
    } else if (w.type === 'correlatedSubquery' && w.op === 'EXISTS' && !w.flip && !w.scalar) {
      lines.push(`${pad(depth)}.whereExists('${relNameFromCsq(w.related)}'${maybeSubqCallback(w.related, depth)})`);
    } else if (w.type === 'correlatedSubquery' && w.op === 'NOT EXISTS') {
      const inner = `not(exists('${relNameFromCsq(w.related)}'${maybeSubqCallback(w.related, depth)}))`;
      lines.push(`${pad(depth)}.where(({not, exists}) => ${inner})`);
    } else {
      // Inline expression-builder lambda for OR/AND/CSQ-with-flip/scalar.
      const helpers = collectHelpers(w);
      const expr = renderCondition(w, depth + 1);
      lines.push(`${pad(depth)}.where(({${helpers.join(', ')}}) =>`);
      lines.push(`${pad(depth + 1)}${expr},`);
      lines.push(`${pad(depth)})`);
    }
  }
  if (ast.related) {
    for (const r of ast.related) {
      const rel = relNameFromCsq(r);
      const sub = r.subquery;
      if (!hasInterestingSub(sub)) {
        lines.push(`${pad(depth)}.related('${rel}')`);
      } else {
        lines.push(`${pad(depth)}.related('${rel}', q =>`);
        lines.push(`${pad(depth + 1)}q`);
        emitChain(sub, depth + 2, lines);
        lines.push(`${pad(depth + 1)},`);
        lines.push(`${pad(depth)})`);
      }
    }
  }
  if (ast.orderBy) {
    for (const [col, dir] of ast.orderBy) {
      lines.push(`${pad(depth)}.orderBy('${col}', '${dir}')`);
    }
  }
  if (ast.start) {
    lines.push(`${pad(depth)}.start(${JSON.stringify(ast.start.row)}, {inclusive: ${!ast.start.exclusive}})`);
  }
  if (ast.limit !== undefined) {
    if (ast.limit === 1) {
      lines.push(`${pad(depth)}.one()`);
    } else {
      lines.push(`${pad(depth)}.limit(${ast.limit})`);
    }
  }
}

function hasInterestingSub(ast: AST): boolean {
  return Boolean(ast.where || ast.related || ast.orderBy || ast.limit !== undefined || ast.start);
}

function maybeSubqCallback(csq: CorrelatedSubquery, depth: number): string {
  if (!hasInterestingSub(csq.subquery)) return '';
  const buf: string[] = [];
  buf.push(`, q =>`);
  buf.push(`${pad(depth + 1)}q`);
  emitChain(csq.subquery, depth + 2, buf);
  buf.push(`${pad(depth + 1)}`);
  return buf.join('\n');
}

function renderCondition(c: Condition, depth: number): string {
  if (c.type === 'simple') {
    return `cmp(${renderSimpleArgs(c)})`;
  }
  if (c.type === 'correlatedSubquery') {
    const opts: string[] = [];
    if (c.flip !== undefined) opts.push(`flip: ${c.flip}`);
    if (c.scalar !== undefined) opts.push(`scalar: ${c.scalar}`);
    const optsStr = opts.length > 0 ? `, {${opts.join(', ')}}` : '';
    const sub = c.related.subquery;
    const cb = hasInterestingSub(sub) ? `, q => q.where(({cmp, or, and, exists, not}) => ${renderCondition(sub.where!, depth + 1)})` : '';
    const expr = `exists('${relNameFromCsq(c.related)}'${cb}${optsStr})`;
    return c.op === 'NOT EXISTS' ? `not(${expr})` : expr;
  }
  // and/or
  const kw = c.type === 'and' ? 'and' : 'or';
  const inner = c.conditions.map(child => renderCondition(child, depth + 1));
  if (inner.length === 0) return `${kw}()`;
  if (inner.every(i => !i.includes('\n')) && inner.join(', ').length < 80) {
    return `${kw}(${inner.join(', ')})`;
  }
  return `${kw}(\n${inner.map(i => pad(depth + 1) + i).join(',\n')},\n${pad(depth)})`;
}

function renderSimpleArgs(c: SimpleCondition): string {
  const left = renderValuePos(c.left);
  const right = renderValuePos(c.right);
  // Default-op shorthand `.where(col, value)` only when op is `=`
  // and left is a column name.
  if (c.op === '=' && c.left.type === 'column' && c.right.type === 'literal') {
    return `${left}, ${right}`;
  }
  return `${left}, '${c.op}', ${right}`;
}

function renderValuePos(v: ValuePosition): string {
  if (v.type === 'column') return `'${v.name}'`;
  if (v.type === 'literal') return renderLiteral(v.value);
  return `/* static:${v.anchor}.${Array.isArray(v.field) ? v.field.join('.') : v.field} */`;
}

function renderLiteral(v: LiteralValue): string {
  if (v === null) return 'null';
  if (Array.isArray(v)) return JSON.stringify(v);
  return JSON.stringify(v);
}

function relNameFromCsq(csq: CorrelatedSubquery): string {
  const alias = csq.subquery.alias;
  if (alias && alias.startsWith('zsubq_')) {
    // strip prefix + trailing `_<digits>` counter
    return alias.replace(/^zsubq_/, '').replace(/_\d+$/, '');
  }
  return csq.subquery.table;
}

function collectHelpers(c: Condition): string[] {
  const set = new Set<string>();
  walk(c, set);
  return [...set].sort();
}

function walk(c: Condition, set: Set<string>): void {
  if (c.type === 'simple') {
    set.add('cmp');
    return;
  }
  if (c.type === 'correlatedSubquery') {
    set.add('exists');
    if (c.op === 'NOT EXISTS') set.add('not');
    if (c.related.subquery.where) walk(c.related.subquery.where, set);
    return;
  }
  set.add(c.type === 'and' ? 'and' : 'or');
  for (const child of c.conditions) walk(child, set);
}

function pad(depth: number): string {
  return INDENT.repeat(depth);
}
