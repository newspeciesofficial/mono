/* oxlint-disable no-console */

import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';
import {createRandomYieldWrapper} from '../../../zql/src/ivm/test/random-yield-source.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {generateShrinkableQuery} from '../../../zql/src/query/test/query-gen.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../../zero-cache/src/db/lite-tables.ts';
import {
  type AST,
  type Condition,
  mapAST,
} from '../../../zero-protocol/src/ast.ts';
import {
  clientToServer,
  serverToClient,
} from '../../../zero-schema/src/name-mapper.ts';
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';
import {completeOrdering} from '../../../zql/src/query/complete-ordering.ts';
import {newQueryImpl} from '../../../zql/src/query/query-impl.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import '../helpers/comparePg.ts';
import {type bootstrap, checkPush, runAndCompare} from '../helpers/runner.ts';

const VITEST_TIMEOUT_MS = 60_000;

// Internal timeout for graceful handling (shorter than vitest timeout)
const TEST_TIMEOUT_MS = VITEST_TIMEOUT_MS / 2;

class FuzzTimeoutError extends Error {
  constructor(label: string, elapsedMs: number) {
    super(`Fuzz test "${label}" timed out after ${elapsedMs}ms`);
    this.name = 'FuzzTimeoutError';
  }
}

class PlannerMismatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PlannerMismatchError';
  }
}

function createCheckAbort(
  startTime: number,
  timeoutMs: number,
  label: string,
): () => void {
  return () => {
    const elapsed = Date.now() - startTime;
    if (elapsed > timeoutMs) {
      throw new FuzzTimeoutError(label, elapsed);
    }
  };
}

function stripFlips(ast: AST): AST {
  return {
    ...ast,
    where: ast.where ? stripFlipsCondition(ast.where) : undefined,
    related: ast.related?.map(r => ({
      ...r,
      subquery: stripFlips(r.subquery),
    })),
  };
}

function stripFlipsCondition(condition: Condition): Condition {
  if (condition.type === 'simple') {
    return condition;
  }
  if (condition.type === 'correlatedSubquery') {
    return {
      ...condition,
      flip: undefined,
      related: {
        ...condition.related,
        subquery: stripFlips(condition.related.subquery),
      },
    };
  }
  return {
    ...condition,
    conditions: condition.conditions.map(stripFlipsCondition),
  };
}

export function fuzzHydrationTests(
  schema: Schema,
  harness: Awaited<ReturnType<typeof bootstrap>>,
  reproSeed?: number | undefined,
) {
  // Set up planner infrastructure for testing planned queries
  harness.dbs.sqlite.exec('ANALYZE;');
  const tableSpecs = computeZqlSpecs(
    createSilentLogContext(),
    harness.dbs.sqlite,
    {includeBackfillingColumns: false},
  );
  const costModel = createSQLiteCostModel(harness.dbs.sqlite, tableSpecs);
  const c2sMapper = clientToServer(schema.tables);
  const s2cMapper = serverToClient(schema.tables);

  function planForFuzz(query: AnyQuery): AnyQuery {
    const qi = asQueryInternals(query);
    const ast = stripFlips(qi.ast);
    const completed = completeOrdering(
      ast,
      tableName => schema.tables[tableName].primaryKey,
    );
    const serverAst = mapAST(completed, c2sMapper);
    const plannedServerAst = planQuery(serverAst, costModel);
    const plannedClientAst = mapAST(plannedServerAst, s2cMapper);
    return newQueryImpl(
      schema,
      qi.ast.table,
      plannedClientAst,
      qi.format,
      'test',
    );
  }

  function createCase(seed?: number) {
    seed = seed ?? Date.now() ^ (Math.random() * 0x100000000);
    const randomizer = generateMersenne53Randomizer(seed);
    const rng = () => randomizer.next();
    const faker = new Faker({
      locale: en,
      randomizer,
    });
    return {
      seed,
      rng,
      query: generateShrinkableQuery(
        schema,
        Object.fromEntries(harness.dbs.raw),
        rng,
        faker,
        harness.delegates.pg.serverSchema,
      ),
    };
  }

  async function runCase({
    query,
    seed,
    rng,
  }: {
    query: [AnyQuery, AnyQuery[]];
    seed: number;
    rng: () => number;
  }) {
    const label = `fuzz-hydration ${seed}`;
    const startTime = Date.now();
    const checkAbort = createCheckAbort(startTime, TEST_TIMEOUT_MS, label);

    const sourceWrapper = createRandomYieldWrapper(rng, 0.3, checkAbort);

    try {
      await harness.transact(async delegates => {
        await runAndCompare(schema, delegates, query[0], undefined);
        await checkPush(schema, delegates, query[0], 10);

        // Also test with planner-decided join strategies
        const planned = planForFuzz(query[0]);
        try {
          await runAndCompare(schema, delegates, planned, undefined);
        } catch (plannerError) {
          throw new PlannerMismatchError(
            'Planner mismatch. Repro seed: ' +
              seed +
              '\n' +
              (plannerError instanceof Error
                ? plannerError.message
                : String(plannerError)),
          );
        }
      }, sourceWrapper);
    } catch (e) {
      if (e instanceof FuzzTimeoutError) {
        console.warn(`⚠️ ${e.message} - passing anyway`);
        return;
      }
      if (e instanceof PlannerMismatchError) {
        throw e;
      }

      const zql = await shrink(query[1], seed);
      if (seed === reproSeed) {
        throw e;
      }
      throw new Error('Mismatch. Repro seed: ' + seed + '\nshrunk zql: ' + zql);
    }
  }

  async function shrink(generations: AnyQuery[], seed: number) {
    console.log('Found failure at seed', seed);
    console.log('Shrinking', generations.length, 'generations');
    let low = 0;
    let high = generations.length;
    let lastFailure = -1;
    while (low < high) {
      const mid = low + ((high - low) >> 1);
      try {
        await harness.transact(async delegates => {
          await runAndCompare(schema, delegates, generations[mid], undefined);
          await checkPush(schema, delegates, generations[mid], 10);
        });
        low = mid + 1;
      } catch {
        lastFailure = mid;
        high = mid;
      }
    }
    if (lastFailure === -1) {
      throw new Error('no failure found');
    }
    const query = generations[lastFailure];
    const queryInternals = asQueryInternals(query);
    return formatOutput(
      queryInternals.ast.table + astToZQL(queryInternals.ast),
    );
  }

  // oxlint-disable-next-line expect-expect
  test.each(Array.from({length: 100}, () => createCase()))(
    'fuzz-hydration $seed',
    runCase,
    VITEST_TIMEOUT_MS,
  );

  test('sentinel', () => {
    expect(true).toBe(true);
  });

  if (reproSeed) {
    // oxlint-disable-next-line no-focused-tests
    test.only(
      'repro',
      async () => {
        const tc = createCase(reproSeed);
        const {query} = tc;
        console.log(
          'ZQL',
          await formatOutput(
            asQueryInternals(query[0]).ast.table +
              astToZQL(asQueryInternals(query[0]).ast),
          ),
        );
        await runCase(tc);
      },
      VITEST_TIMEOUT_MS,
    );
  }
}
