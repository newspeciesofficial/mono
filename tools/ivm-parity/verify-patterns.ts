import {patterns} from './patterns.ts';
const keys = Object.keys(patterns);
console.log('total patterns:', keys.length);
for (const k of keys) {
  try {
    const q = (patterns as Record<string, () => unknown>)[k]();
    const ast = (q as {ast: {table: string; where?: unknown; related?: unknown[]}})
      .ast;
    console.log(
      'OK',
      k,
      'table=' + ast.table,
      'hasWhere=' + !!ast.where,
      'related=' + (ast.related?.length ?? 0),
    );
  } catch (e) {
    console.log('FAIL', k, (e as Error).message);
  }
}
