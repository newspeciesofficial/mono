import {expect, suite, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Cap} from './cap.ts';
import {Catch} from './catch.ts';
import {Exists} from './exists.ts';
import {Join} from './join.ts';
import {MemoryStorage} from './memory-storage.ts';
import {createSource} from './test/source-factory.ts';
import {consume} from './stream.ts';
import {buildFilterPipeline, type FilterInput} from './filter-operators.ts';
import type {BuilderDelegate} from '../builder/builder.ts';
import type {FetchRequest, Input, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Node} from './data.ts';
import type {Stream} from './stream.ts';

const lc = createSilentLogContext();

const mockDelegate = {
  addEdge() {},
} as unknown as BuilderDelegate;

/**
 * ReorderingInput wraps an Input and reverses the order of nodes
 * returned by fetch. This simulates the behavior of a Source that returns
 * rows in non-PK order (as happens when skipOrderByInSQL=true in real
 * pipelines). MemorySource always returns rows in sorted order, so this
 * wrapper is needed to exercise the position check in Join/FlippedJoin.
 */
class ReorderingInput implements Input {
  readonly #inner: Input;

  constructor(inner: Input) {
    this.#inner = inner;
  }

  setOutput(output: Output): void {
    this.#inner.setOutput(output);
  }

  getSchema(): SourceSchema {
    return this.#inner.getSchema();
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    const nodes: Node[] = [];
    for (const node of this.#inner.fetch(req)) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      nodes.push(node);
    }
    // Reverse so iteration order doesn't match PK ordering
    nodes.reverse();
    for (const node of nodes) {
      yield node;
    }
  }

  destroy(): void {
    this.#inner.destroy();
  }
}

/**
 * These tests verify that the upstream operators (Join) work correctly
 * with Cap even when the parent source returns rows in non-PK order.
 *
 * The key concern is Join's position check in #processParentNode (join.ts:273-277):
 *   compareRows(parentNodeRow, inprogressChildChange.position) > 0
 *
 * This check determines whether to apply generateWithOverlay during a
 * re-fetch triggered by an in-flight push. With non-PK iteration order,
 * the check can give wrong results. This matters for Cap's refill logic:
 * during refill, the wrong overlay can make a row appear to still pass
 * the EXISTS filter when it shouldn't, causing Cap to add a false
 * replacement. See the first test for a concrete example.
 *
 * This is a known limitation of the unordered stream optimization.
 * The pipeline doesn't crash and all parent rows correctly receive their
 * change push. The incorrect Cap state is eventually corrected by pipeline
 * reset or timeout.
 *
 * Pipeline topology:
 *   Source(comments) → ReorderingInput(reverse) → Join(child=Source(author))
 *     → buildFilterPipeline(Exists('author')) → Cap(limit) → Catch
 */
suite('cap upstream overlay with reordered parent', () => {
  const commentColumns = {
    id: {type: 'string'},
    issueID: {type: 'string'},
    authorID: {type: 'string'},
  } as const;

  const authorColumns = {
    id: {type: 'string'},
  } as const;

  test('remove child — all comments removed with unordered overlay skip', () => {
    // Setup: 3 comments all by author a1
    const commentSource = createSource(
      lc,
      testLogConfig,
      'comments',
      commentColumns,
      ['id'],
    );
    const authorSource = createSource(
      lc,
      testLogConfig,
      'author',
      authorColumns,
      ['id'],
    );

    consume(
      commentSource.push({
        type: 'add',
        row: {id: 'c1', issueID: 'i1', authorID: 'a1'},
      }),
    );
    consume(
      commentSource.push({
        type: 'add',
        row: {id: 'c2', issueID: 'i1', authorID: 'a1'},
      }),
    );
    consume(
      commentSource.push({
        type: 'add',
        row: {id: 'c3', issueID: 'i1', authorID: 'a1'},
      }),
    );
    consume(authorSource.push({type: 'add', row: {id: 'a1'}}));

    // Build pipeline:
    //   commentSource → ReorderingInput → Join(child=authorConn)
    //     → Exists('author') → Cap(limit=3) → Catch
    const commentConn = commentSource.connect([['id', 'asc']]);
    const reordering = new ReorderingInput(commentConn);

    const authorConn = authorSource.connect([['id', 'asc']]);

    const join = new Join({
      parent: reordering,
      child: authorConn,
      parentKey: ['authorID'],
      childKey: ['id'],
      relationshipName: 'author',
      hidden: false,
      system: 'client',
      unordered: true,
    });

    const capStorage = new MemoryStorage();
    const filter = buildFilterPipeline(
      join,
      mockDelegate,
      (filterInput: FilterInput) =>
        new Exists(filterInput, 'author', ['authorID'], 'EXISTS'),
    );
    const cap = new Cap(filter, capStorage, 3);
    const catchOp = new Catch(cap);

    // Hydrate
    catchOp.fetch();
    catchOp.reset();

    // Verify initial state: all 3 comments tracked
    expect(capStorage.cloneData()).toMatchObject({
      '["cap"]': {size: 3},
    });

    // Push: remove author a1
    consume(authorSource.push({type: 'remove', row: {id: 'a1'}}));

    // All 3 comments correctly receive their remove push.
    const removes = catchOp.pushes.filter(p => p.type === 'remove');
    expect(removes).toHaveLength(3);

    // With unordered=true, overlay is skipped so no false replacements
    // occur during Cap's refill.
    const adds = catchOp.pushes.filter(p => p.type === 'add');
    expect(adds).toHaveLength(0);

    // Cap is empty — all comments lost their author.
    const capState = capStorage.cloneData()['["cap"]'] as {
      size: number;
      pks: string[];
    };
    expect(capState.size).toBe(0);
  });

  test('remove child with replacement from different join key', () => {
    // Setup: c1 and c2 by author a1, c3 by author a2
    const commentSource = createSource(
      lc,
      testLogConfig,
      'comments',
      commentColumns,
      ['id'],
    );
    const authorSource = createSource(
      lc,
      testLogConfig,
      'author',
      authorColumns,
      ['id'],
    );

    consume(
      commentSource.push({
        type: 'add',
        row: {id: 'c1', issueID: 'i1', authorID: 'a1'},
      }),
    );
    consume(
      commentSource.push({
        type: 'add',
        row: {id: 'c2', issueID: 'i1', authorID: 'a1'},
      }),
    );
    consume(
      commentSource.push({
        type: 'add',
        row: {id: 'c3', issueID: 'i1', authorID: 'a2'},
      }),
    );
    consume(authorSource.push({type: 'add', row: {id: 'a1'}}));
    consume(authorSource.push({type: 'add', row: {id: 'a2'}}));

    const commentConn = commentSource.connect([['id', 'asc']]);
    const reordering = new ReorderingInput(commentConn);

    const authorConn = authorSource.connect([['id', 'asc']]);

    const join = new Join({
      parent: reordering,
      child: authorConn,
      parentKey: ['authorID'],
      childKey: ['id'],
      relationshipName: 'author',
      hidden: false,
      system: 'client',
      unordered: true,
    });

    const capStorage = new MemoryStorage();
    const filter = buildFilterPipeline(
      join,
      mockDelegate,
      (filterInput: FilterInput) =>
        new Exists(filterInput, 'author', ['authorID'], 'EXISTS'),
    );
    const cap = new Cap(filter, capStorage, 2);
    const catchOp = new Catch(cap);

    // Hydrate
    catchOp.fetch();
    catchOp.reset();

    // Verify initial: 2 tracked (ReorderingInput reverses → [c3, c2, c1],
    // so Cap tracks c3 and c2)
    const initialState = capStorage.cloneData()['["cap"]'] as {
      size: number;
      pks: string[];
    };
    expect(initialState.size).toBe(2);

    // Push: remove author a1 — comments c1 and c2 lose their author
    consume(authorSource.push({type: 'remove', row: {id: 'a1'}}));

    // Final state: c3 (which has author a2) should be in the set.
    const finalState = capStorage.cloneData()['["cap"]'] as {
      size: number;
      pks: string[];
    };
    expect(finalState.pks).toContain('["c3"]');
    expect(finalState.size).toBeGreaterThanOrEqual(1);
  });

  test('add child — comments gain authors', () => {
    // Setup: 2 comments with authorID=a1, but no author a1 yet
    const commentSource = createSource(
      lc,
      testLogConfig,
      'comments',
      commentColumns,
      ['id'],
    );
    const authorSource = createSource(
      lc,
      testLogConfig,
      'author',
      authorColumns,
      ['id'],
    );

    consume(
      commentSource.push({
        type: 'add',
        row: {id: 'c1', issueID: 'i1', authorID: 'a1'},
      }),
    );
    consume(
      commentSource.push({
        type: 'add',
        row: {id: 'c2', issueID: 'i1', authorID: 'a1'},
      }),
    );

    const commentConn = commentSource.connect([['id', 'asc']]);
    const reordering = new ReorderingInput(commentConn);

    const authorConn = authorSource.connect([['id', 'asc']]);

    const join = new Join({
      parent: reordering,
      child: authorConn,
      parentKey: ['authorID'],
      childKey: ['id'],
      relationshipName: 'author',
      hidden: false,
      system: 'client',
      unordered: true,
    });

    const capStorage = new MemoryStorage();
    const filter = buildFilterPipeline(
      join,
      mockDelegate,
      (filterInput: FilterInput) =>
        new Exists(filterInput, 'author', ['authorID'], 'EXISTS'),
    );
    const cap = new Cap(filter, capStorage, 3);
    const catchOp = new Catch(cap);

    // Hydrate — no authors, so no comments pass EXISTS
    catchOp.fetch();
    catchOp.reset();

    // Verify initial: cap is empty (no comments pass EXISTS)
    const initialState = capStorage.cloneData()['["cap"]'] as {
      size: number;
      pks: string[];
    };
    expect(initialState.size).toBe(0);

    // Push: add author a1 — both comments now pass EXISTS
    consume(authorSource.push({type: 'add', row: {id: 'a1'}}));

    const adds = catchOp.pushes.filter(p => p.type === 'add');
    expect(adds).toHaveLength(2);

    // Cap should now track both comments
    const finalState = capStorage.cloneData()['["cap"]'] as {
      size: number;
      pks: string[];
    };
    expect(finalState.size).toBe(2);
  });
});
