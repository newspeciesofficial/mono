// oxlint-disable no-console
import {summary} from 'mitata';
import {expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import {mapAST, type AST, type Condition} from '../../zero-protocol/src/ast.ts';
import {type Format} from '../../zql/src/ivm/default-format.ts';
import {newQueryImpl} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {Database} from '../../zqlite/src/db.ts';
import {newQueryDelegate} from '../../zqlite/src/test/source-factory.ts';
import {builder, schema} from './schema.ts';
import {AccumulatorDebugger} from '../../zql/src/planner/planner-debug.ts';
import {planQuery} from '../../zql/src/planner/planner-builder.ts';
import {createSQLiteCostModel} from '../../zqlite/src/sqlite-cost-model.ts';
import {
  clientToServer,
  serverToClient,
} from '../../zero-schema/src/name-mapper.ts';

const dbPath = process.env.ZBUGS_REPLICA_PATH;

if (!dbPath) {
  console.error(
    'Cannot run zbugs.bench.ts without a path to the zbugs replica. Set env var: `ZBUGS_REPLICA_PATH`',
  );
} else {
  // Open the zbugs SQLite database
  const db = new Database(createSilentLogContext(), dbPath);
  const lc = createSilentLogContext();

  // Run ANALYZE to populate SQLite statistics for cost model
  console.log('analyze...');
  // db.exec('ANALYZE;');
  console.log('done analyze');

  // Get table specs using computeZqlSpecs
  const tableSpecs = new Map<string, LiteAndZqlSpec>();
  computeZqlSpecs(
    createSilentLogContext(),
    db,
    {includeBackfillingColumns: false},
    tableSpecs,
  );

  // Create SQLite cost model
  const costModel = createSQLiteCostModel(db, tableSpecs);
  const clientToServerMapper = clientToServer(schema.tables);
  const serverToClientMapper = serverToClient(schema.tables);

  // Create SQLite delegate
  const delegate = newQueryDelegate(lc, testLogConfig, db, schema);

  // Helper to set flip to false in all correlated subquery conditions
  function setFlipToFalse(condition: Condition): Condition {
    if (condition.type === 'correlatedSubquery') {
      return {
        ...condition,
        flip: false,
        related: {
          ...condition.related,
          subquery: setFlipToFalseInAST(condition.related.subquery),
        },
      };
    } else if (condition.type === 'and' || condition.type === 'or') {
      return {
        ...condition,
        conditions: condition.conditions.map(setFlipToFalse),
      };
    }
    return condition;
  }

  function setFlipToFalseInAST(ast: AST): AST {
    return {
      ...ast,
      where: ast.where ? setFlipToFalse(ast.where) : undefined,
      related: ast.related?.map(r => ({
        ...r,
        subquery: setFlipToFalseInAST(r.subquery),
      })),
    };
  }

  // Helper to create a query from an AST
  function createQuery(
    tableName: string,
    queryAST: AST,
    format: Format,
  ): AnyQuery {
    return newQueryImpl(
      schema,
      tableName as keyof typeof schema.tables & string,
      queryAST,
      format,
      'test',
    );
  }

  // Helper to benchmark planned vs unplanned
  async function benchmarkQuery<
    TTable extends keyof typeof schema.tables & string,
  >(_name: string, query: AnyQuery) {
    const unplannedAST = asQueryInternals(query).ast;
    const tableName = unplannedAST.table as TTable;
    const format = asQueryInternals(query).format;

    let mappedAST = mapAST(unplannedAST, clientToServerMapper);
    const resolved = resolveSimpleScalarSubqueries(mappedAST);
    mappedAST = resolved.ast;
    const mappedASTCopy = setFlipToFalseInAST(mappedAST);
    const dbg = new AccumulatorDebugger();
    const plannedServerAST = planQuery(mappedASTCopy, costModel, dbg);
    const plannedClientAST = mapAST(plannedServerAST, serverToClientMapper);
    const plannedQuery = createQuery(tableName, plannedClientAST, format);

    // const unplannedQuery = createQuery(tableName, unplannedAST, format);

    console.log('start...');
    db.exec('BEGIN');
    const start = performance.now();
    const data = await delegate.run(plannedQuery as AnyQuery);
    const end = performance.now();
    console.log('duration ', end - start);
    db.exec('ROLLBACK');
    console.log(data);

    summary(() => {
      // bench(`unplanned: ${name}`, async () => {
      //   await delegate.run(unplannedQuery as AnyQuery);
      // });
      // bench(`planned: ${name}`, async () => {
      //   await delegate.run(plannedQuery as AnyQuery);
      // });
    });
  }

  await benchmarkQuery(
    'full issue scan + join',
    builder.issue
      .whereExists('project', q => q.where('name', 'gatewaycore'), {
        scalar: true,
      })
      .limit(100),
  );

  // run all reads in an explicit tx
  // db.exec('BEGIN');
  // await run();
  // db.exec('ROLLBACK');
}

test('no-op', () => {
  expect(true).toBe(true);
});
