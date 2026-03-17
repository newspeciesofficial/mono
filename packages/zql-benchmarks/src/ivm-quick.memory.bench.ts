/**
 * Quick validation benchmark - minimal data to ensure the bench setup works.
 */
import {bench, run, summary} from 'mitata';
import {expect, test} from 'vitest';
import type {Row} from '../../zero-protocol/src/data.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {schema, builder} from './schema.ts';

// Small dataset for a quick sanity check
const NUM_ISSUES = 50;

function makeSources() {
  const {tables} = schema;
  const sources: Record<string, MemorySource> = {};
  for (const [name, tableSchema] of Object.entries(tables)) {
    sources[name] = new MemorySource(
      tableSchema.name,
      tableSchema.columns,
      tableSchema.primaryKey,
    );
  }

  // Add a few users
  for (let i = 0; i < 10; i++) {
    for (const _ of sources['user'].push({
      type: 'add',
      row: {
        id: `user-${i}`,
        login: `user${i}`,
        name: `User ${i}`,
        avatar: '',
        role: 'user',
      },
    })) {
      /* consume */
    }
  }

  // Add a project
  for (const _ of sources['project'].push({
    type: 'add',
    row: {id: 'proj-0', name: 'Project Zero', lowerCaseName: 'project zero'},
  })) {
    /* consume */
  }

  // Add issues
  for (let i = 0; i < NUM_ISSUES; i++) {
    for (const _ of sources['issue'].push({
      type: 'add',
      row: {
        id: `issue-${i}`,
        shortID: i,
        title: `Issue ${i}`,
        open: i % 2 === 0,
        modified: Date.now() - i * 1000,
        created: Date.now() - i * 2000,
        projectID: 'proj-0',
        creatorID: `user-${i % 10}`,
        assigneeID: undefined,
        description: `Description ${i}`,
        visibility: 'public',
      },
    })) {
      /* consume */
    }
  }

  return sources;
}

const sources = makeSources();

summary(() => {
  bench('hydrate issues (50 rows)', async () => {
    const delegate = new QueryDelegateImpl({sources});
    await delegate.run(builder.issue);
  });
});

await run();

test('quick benchmark ran', () => {
  expect(true).toBe(true);
});
