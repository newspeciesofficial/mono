import {expect, suite, test} from 'vitest';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import {Cap} from './cap.ts';
import type {Comparator, Node} from './data.ts';
import {generateWithOverlay, generateWithOverlayNoYield} from './join-utils.ts';
import {MemoryStorage} from './memory-storage.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';
import {createSource} from './test/source-factory.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';

const lc = createSilentLogContext();

function makeNode(row: Row): Node {
  return {row, relationships: {}};
}

const ascById: Comparator = (a, b) => (a.id as number) - (b.id as number);

function makeSchema(compareRows: Comparator): SourceSchema {
  return {
    tableName: 'test',
    columns: {id: {type: 'number'}, v: {type: 'string'}},
    primaryKey: ['id'],
    relationships: {},
    isHidden: false,
    system: 'client',
    compareRows,
    sort: [['id', 'asc']],
  };
}

function* streamFromRows(rows: Row[]): Stream<Node> {
  for (const row of rows) {
    yield makeNode(row);
  }
}

function collectRows(stream: Stream<Node | 'yield'>): Row[] {
  const result: Row[] = [];
  for (const node of stream) {
    if (node !== 'yield') {
      result.push(node.row);
    }
  }
  return result;
}

function rowSet(rows: Row[]): Set<string> {
  return new Set(rows.map(r => JSON.stringify(r)));
}

// Stream orderings for testing
const sorted = [{id: 1}, {id: 2}, {id: 3}];
const reversed = [{id: 3}, {id: 2}, {id: 1}];
const shuffled = [{id: 2}, {id: 3}, {id: 1}];

suite('equality-based operations are order-independent', () => {
  const schema = makeSchema(ascById);

  test('add overlay suppresses duplicate regardless of stream order', () => {
    const overlay: Change = {type: 'add', node: makeNode({id: 2})};
    const expected = rowSet([{id: 1}, {id: 3}]);

    for (const ordering of [sorted, reversed, shuffled]) {
      const result = collectRows(
        generateWithOverlay(streamFromRows(ordering), overlay, schema),
      );
      expect(rowSet(result)).toEqual(expected);
    }
  });

  test('child overlay replaces matching node regardless of stream order', () => {
    const childSchema = makeSchema(ascById);
    const schemaWithChild: SourceSchema = {
      ...schema,
      relationships: {r: childSchema},
    };

    const childChange: Change = {type: 'add', node: makeNode({id: 99})};
    const overlay: Change = {
      type: 'child',
      node: makeNode({id: 2}),
      child: {relationshipName: 'r', change: childChange},
    };

    for (const ordering of [sorted, reversed]) {
      const nodes: Node[] = [];
      for (const node of generateWithOverlay(
        streamFromRows(ordering),
        overlay,
        schemaWithChild,
      )) {
        if (node !== 'yield') {
          nodes.push(node);
        }
      }
      // All 3 rows present
      expect(rowSet(nodes.map(n => n.row))).toEqual(
        rowSet([{id: 1}, {id: 2}, {id: 3}]),
      );
      // The node with id:2 should have modified relationships
      const modified = nodes.find(n => n.row.id === 2);
      expect(modified).toBeDefined();
      expect(modified?.relationships).toHaveProperty('r');
    }
  });

  test('edit new-node suppression regardless of stream order', () => {
    const overlay: Change = {
      type: 'edit',
      node: makeNode({id: 2, v: 'new'}),
      oldNode: makeNode({id: 2, v: 'old'}),
    };
    const expected = rowSet([{id: 1}, {id: 2, v: 'old'}, {id: 3}]);

    const streamRows = [
      [{id: 1}, {id: 2, v: 'new'}, {id: 3}],
      [{id: 3}, {id: 2, v: 'new'}, {id: 1}],
    ];

    for (const ordering of streamRows) {
      const result = collectRows(
        generateWithOverlay(streamFromRows(ordering), overlay, schema),
      );
      expect(rowSet(result)).toEqual(expected);
    }
  });
});

suite('position-based operations — fallback guarantees', () => {
  const schema = makeSchema(ascById);

  test('remove overlay yields exactly once regardless of stream order', () => {
    const overlay: Change = {type: 'remove', node: makeNode({id: 2})};
    const expected = rowSet([{id: 1}, {id: 2}, {id: 3}]);

    // Stream does NOT contain id:2 (it was removed from source)
    const orderings = [
      [{id: 1}, {id: 3}],
      [{id: 3}, {id: 1}],
    ];

    for (const ordering of orderings) {
      const result = collectRows(
        generateWithOverlay(streamFromRows(ordering), overlay, schema),
      );
      expect(rowSet(result)).toEqual(expected);
      expect(result.length).toBe(3); // exactly once
    }
  });

  test('remove with empty stream', () => {
    const overlay: Change = {type: 'remove', node: makeNode({id: 2})};
    const result = collectRows(
      generateWithOverlay(streamFromRows([]), overlay, schema),
    );
    expect(result).toEqual([{id: 2}]);
  });

  test('remove where overlay node > all stream items (fallback fires)', () => {
    const overlay: Change = {type: 'remove', node: makeNode({id: 5})};
    const expected = rowSet([{id: 1}, {id: 2}, {id: 5}]);

    const result = collectRows(
      generateWithOverlay(streamFromRows([{id: 1}, {id: 2}]), overlay, schema),
    );
    expect(rowSet(result)).toEqual(expected);
  });

  test('edit old-node via fallback with reverse-ordered stream', () => {
    const overlay: Change = {
      type: 'edit',
      node: makeNode({id: 2, v: 'new'}),
      oldNode: makeNode({id: 2, v: 'old'}),
    };
    const expected = rowSet([{id: 1}, {id: 2, v: 'old'}, {id: 3}]);

    // Reverse order: compareRows will never see < 0 for old node during loop,
    // so it must use the fallback path
    const result = collectRows(
      generateWithOverlay(
        streamFromRows([{id: 3}, {id: 2, v: 'new'}, {id: 1}]),
        overlay,
        schema,
      ),
    );
    expect(rowSet(result)).toEqual(expected);
  });
});

suite('Cap schema passthrough', () => {
  test('Cap.getSchema() returns same compareRows as input', () => {
    const source = createSource(
      lc,
      testLogConfig,
      'test',
      {id: {type: 'string'}},
      ['id'],
    );
    const input = source.connect([['id', 'asc']]);
    const storage = new MemoryStorage();
    const cap = new Cap(input, storage, 5);

    const inputSchema = input.getSchema();
    const capSchema = cap.getSchema();

    // Reference equality — Cap passes through the exact same schema
    expect(capSchema.compareRows).toBe(inputSchema.compareRows);
    expect(capSchema.sort).toEqual(inputSchema.sort);
  });
});

suite('EXISTS correctness with unordered overlay', () => {
  const schema = makeSchema(ascById);

  test('remove overlay does not affect EXISTS outcome', () => {
    const overlay: Change = {type: 'remove', node: makeNode({id: 2})};

    const orderings = [
      [{id: 1}, {id: 3}],
      [{id: 3}, {id: 1}],
    ];

    for (const ordering of orderings) {
      const result = collectRows(
        generateWithOverlayNoYield(
          streamFromRows(ordering),
          overlay,
          schema,
        ) as Stream<Node>,
      );
      expect(result.length).toBe(3);
      expect(result.length > 0).toBe(true); // EXISTS = true
    }
  });

  test('add overlay suppresses but EXISTS still true', () => {
    const overlay: Change = {type: 'add', node: makeNode({id: 2})};

    const result = collectRows(
      generateWithOverlayNoYield(
        streamFromRows([{id: 1}, {id: 2}]),
        overlay,
        schema,
      ) as Stream<Node>,
    );
    expect(result.length).toBe(1);
    expect(result.length > 0).toBe(true); // EXISTS still true
  });
});
