/**
 * Phase 7 synthesis — append ASTs that TARGET the open behavioral
 * divergences listed in PARITY-DIVERGENCES.md. The sweep will verify
 * whether each divergence surfaces as a TS↔RS output mismatch.
 *
 * Pairs with the new DELETE / INSERT mutations added to the MUTATIONS
 * block in harness-advance-coverage.ts. Without those mutations, none
 * of these queries hit the push-path branches they're designed to test.
 */
import {readFileSync, writeFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const corpusPath = join(here, 'ast_corpus.json');
const corpus = JSON.parse(readFileSync(corpusPath, 'utf8'));

const additions: Array<{id: string; ast: unknown; kind: string}> = [];

// p7_take_remove_refetch_limit2 — BEH-5 (Take Remove missing refetch).
// TS take.ts:354-418 on Remove within/at bound fetches next row and
// pushes Remove + Add(newBound). RS take_t.rs:301-362 clears bound,
// emits only Remove. Query's window is {m-1, m-2}; deleting m-1
// should leave {m-2, m-10} on TS, {m-2} on RS.
additions.push({
  id: 'p7_take_remove_refetch_limit2',
  kind: 'p7',
  ast: {
    table: 'messages',
    orderBy: [['createdAt', 'asc']],
    limit: 2,
  },
});

// p7_take_remove_refetch_limit3 — same as above with limit=3.
additions.push({
  id: 'p7_take_remove_refetch_limit3',
  kind: 'p7',
  ast: {
    table: 'messages',
    orderBy: [['createdAt', 'asc']],
    limit: 3,
  },
});

// p7_take_remove_refetch_limit1 — edge case. Window is {m-1}; deleting
// m-1 should leave {m-2} on TS, {} on RS.
additions.push({
  id: 'p7_take_remove_refetch_limit1',
  kind: 'p7',
  ast: {
    table: 'messages',
    orderBy: [['createdAt', 'asc']],
    limit: 1,
  },
});

// p7_exists_remove_child_reinsert — BEH-18 (ExistsT Remove+size=0
// missing child re-insert). Seed + mutation block inserts ch-solo and
// u-solo as a lone participant, then later deletes that participant.
// TS exists.ts:189-209 emits Remove(ch-solo) with
// `relationships.participants = [u-solo-participant]`; RS emits
// Remove(ch-solo) with empty relationships — the child tombstone is
// lost. Observable as a missing `participants` del on RS.
additions.push({
  id: 'p7_exists_remove_child_reinsert',
  kind: 'p7',
  ast: {
    table: 'channels',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        correlation: {parentField: ['id'], childField: ['channelId']},
        subquery: {table: 'participants', alias: 'zsubq_participants'},
      },
    },
  },
});

// p7_join_processparent_related — BEH-1/2 (Join push_parent /
// push_child missing #processParentNode decoration). Query
// `.related('participants')` with a child INSERT; TS emits the parent
// (channel) with `relationships.participants = [the new participant]`;
// RS emits parent undecorated.
additions.push({
  id: 'p7_join_processparent_related',
  kind: 'p7',
  ast: {
    table: 'channels',
    related: [
      {
        correlation: {parentField: ['id'], childField: ['channelId']},
        subquery: {table: 'participants', alias: 'participants'},
      },
    ],
  },
});

// p7_and_with_flip_exists — BEH-19 (AND-with-flips stub). AST with
// `and(simpleCmp, whereExists.flip())`. TS `applyFilterWithFlips` AND
// case partitions on flip, applies non-flipped filter first, then
// recurses into flipped via FlippedJoin. RS returns empty or_branches
// and falls through to linear ExistsT chain.
additions.push({
  id: 'p7_and_with_flip_exists',
  kind: 'p7',
  ast: {
    table: 'channels',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'visibility'},
          right: {type: 'literal', value: 'public'},
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          flip: true, // explicitly flipped so TS takes applyFilterWithFlips path
          related: {
            correlation: {parentField: ['id'], childField: ['channelId']},
            subquery: {table: 'participants', alias: 'zsubq_participants'},
          },
        },
      ],
    },
  },
});

// p7_union_fanin_dedup — BEH-10 / BEH-12 (FanIn / UnionFanIn dedup vs
// accumulate+flush). Query OR where both branches match the same
// channel row via different branches. The insertion of a new
// participant fan-outs through each branch; TS accumulates and flushes
// once; RS first-wins drops the other branch's contribution.
additions.push({
  id: 'p7_union_fanin_dedup',
  kind: 'p7',
  ast: {
    table: 'channels',
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'visibility'},
          right: {type: 'literal', value: 'public'},
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            correlation: {parentField: ['id'], childField: ['channelId']},
            subquery: {table: 'participants', alias: 'zsubq_participants'},
          },
        },
      ],
    },
  },
});

// p7_flipped_join_child_push — BEH-14/15/16/17 (FlippedJoin unwired).
// Query `whereExists` on channels, planner-flipped. Child-side
// mutation (INSERT/DELETE participant) should propagate through
// FlippedJoin.push_child in TS; RS routes through ExistsT which
// may emit a different shape.
additions.push({
  id: 'p7_flipped_join_child_push',
  kind: 'p7',
  ast: {
    table: 'channels',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      flip: true,
      related: {
        correlation: {parentField: ['id'], childField: ['channelId']},
        subquery: {table: 'participants', alias: 'zsubq_participants'},
      },
    },
  },
});

corpus.push(...additions);
writeFileSync(corpusPath, JSON.stringify(corpus));
console.log(`[synth-p7] appended ${additions.length} entries; corpus now ${corpus.length}`);
