/**
 * Phase 6 synthesis — append targeted ASTs covering TS branches that the
 * pure fuzz corpus didn't exercise. Each pattern is a real query shape
 * that TS IVM naturally flows through a specific take/exists/union
 * branch; we emit them as `p6_*` entries so the coverage sweep picks
 * them up alongside the fuzz entries.
 *
 * The additions are APPEND-ONLY to `ast_corpus.json`. No mutation-block
 * change; the existing 7-step block (insert channel/participant/
 * conversation/message/attachment, update message body, flip channel
 * visibility) provides the mutations these ASTs need.
 */
import {readFileSync, writeFileSync} from 'node:fs';
import {join, dirname} from 'node:path';
import {fileURLToPath} from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const corpusPath = join(here, 'ast_corpus.json');
const corpus = JSON.parse(readFileSync(corpusPath, 'utf8'));

const additions: Array<{id: string; ast: unknown; kind: string}> = [];

// p6_take_edit_orderby_body — messages.orderBy('body').limit(2).
// Mutation block's `update m-test-1 body` shifts this row's position
// in the Take window, hitting take.ts:482/528/588/657 edit-transition
// branches depending on where in body-sort order m-test-1 lands before
// vs after the edit.
additions.push({
  id: 'p6_take_edit_orderby_body',
  kind: 'p6',
  ast: {
    table: 'messages',
    orderBy: [['body', 'asc']],
    limit: 2,
  },
});

// p6_take_limit1_orderby_body — same shape with limit=1. Specifically
// triggers take.ts:483 (push-edit-old-at-bound-new-before-bound-limit1).
additions.push({
  id: 'p6_take_limit1_orderby_body',
  kind: 'p6',
  ast: {
    table: 'messages',
    orderBy: [['body', 'asc']],
    limit: 1,
  },
});

// p6_take_limit1_orderby_body_desc — desc variant for symmetry.
additions.push({
  id: 'p6_take_limit1_orderby_body_desc',
  kind: 'p6',
  ast: {
    table: 'messages',
    orderBy: [['body', 'desc']],
    limit: 1,
  },
});

// p6_exists_size_gt1 — channels.whereExists('participants'); each
// public channel has ≥2 participants in the seed, so Add/Remove on
// participants during the mutation block hits exists.ts:170/205
// (size-gt1 paths).
additions.push({
  id: 'p6_exists_size_gt1',
  kind: 'p6',
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

// p6_or_overlap — OR where both branches match `ch-pub-1` (id='ch-pub-1'
// or visibility='public' both match that row). UnionFanIn's dedup at
// union-fan-in.ts:167 fires when the same row is produced by both
// branches.
additions.push({
  id: 'p6_or_overlap',
  kind: 'p6',
  ast: {
    table: 'channels',
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: 'ch-pub-1'},
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

corpus.push(...additions);
writeFileSync(corpusPath, JSON.stringify(corpus));
console.log(`[synth-p6] appended ${additions.length} entries; corpus now ${corpus.length}`);
