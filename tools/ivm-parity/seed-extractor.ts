/**
 * seed-extractor.ts — Appends hand-picked ZQL seed queries to ast_corpus.json.
 *
 * Strategy: Option B (hand-picked equivalents). The source-app queries.ts
 * uses 50+ tables; only channels/conversations/messages/participants/attachments/users
 * exist in ivm-parity. Rather than shim a TS-compiler walk, we translate the 20 most
 * structurally interesting shape archetypes directly into ivm-parity builder calls,
 * modeling each on a specific source-app or zbugs source line.
 *
 * Run: npx tsx seed-extractor.ts   (from tools/ivm-parity/)
 * Idempotent: de-dupes on `id` before writing.
 */

import {readFileSync, writeFileSync} from 'fs';
import {createBuilder} from '@rocicorp/zero';
import {schema} from './zero-schema.ts';

const zql = createBuilder(schema);

// ---------------------------------------------------------------------------
// Seed query definitions — each commented with the source-app/zbugs source being modelled.
// ---------------------------------------------------------------------------

const seeds: Array<{id: string; comment: string; query: () => {ast: unknown}}> =
  [
    // seed_01 — source: channelConversations (line 36)
    // conversations in a channel ordered by createdAt asc, with messages related and
    // messages filtered by visibleTo (IS NULL OR = user), then messages → attachments.
    {
      id: 'seed_01_channel_conversations_with_messages',
      comment:
        'source-app ref: channelConversations (line 36): conversations in channel, ordered asc, messages OR-visibleTo filter + attachments',
      query: () =>
        zql.conversations
          .where('channelId', 'ch-pub-1')
          .orderBy('createdAt', 'asc')
          .related('messages', m =>
            m
              .where(({cmp, or}) =>
                or(cmp('visibleTo', 'IS', null), cmp('visibleTo', '=', 'u1')),
              )
              .related('attachments'),
          ),
    },

    // seed_02 — source: conversationMessages (line 101)
    // messages for a conversation, OR-visibleTo filter, ordered asc, with attachments.
    {
      id: 'seed_02_conversation_messages_or_visible',
      comment:
        'source-app ref: conversationMessages (line 101): messages.where(conversationId).where(OR visibleTo IS NULL or =user).orderBy asc.related attachments',
      query: () =>
        zql.messages
          .where('conversationId', 'co-1')
          .where(({cmp, or}) =>
            or(cmp('visibleTo', 'IS', null), cmp('visibleTo', '=', 'u1')),
          )
          .orderBy('createdAt', 'asc')
          .related('attachments'),
    },

    // seed_03 — source: channelConversationsPaginated (line 1322)
    // Full bidirectional pagination: .orderBy × 2, .start(inclusive), .limit.
    {
      id: 'seed_03_conversations_full_pagination',
      comment:
        'source-app ref: channelConversationsPaginated (line 1370): .orderBy×2 .start(inclusive) .limit full pagination shape',
      query: () =>
        zql.conversations
          .where('channelId', 'ch-pub-1')
          .orderBy('createdAt', 'desc')
          .orderBy('id', 'desc')
          .start({id: 'co-2', createdAt: 2000}, {inclusive: true})
          .limit(3),
    },

    // seed_04 — source: userConversationsPaginated (line 616)
    // conversations with whereExists on channel (public) + .start(exclusive) + .limit.
    // (source-app uses participants on conversations; ivm-parity conversation → channel via FK)
    {
      id: 'seed_04_conversations_whereExists_channel_paginated',
      comment:
        'source-app ref: userConversationsPaginated (line 623): conversations.whereExists(channel).orderBy.start(exclusive).limit — paginated filtered conversations',
      query: () =>
        zql.conversations
          .whereExists('channel', ch => ch.where('visibility', '=', 'public'))
          .orderBy('createdAt', 'desc')
          .start({id: 'co-2', createdAt: 2000}, {inclusive: false})
          .limit(3),
    },

    // seed_05 — source: channelAndThreadMessages (line 1007)
    // messages.where('showInChannel')-equivalent: messages filtered via
    // whereExists('conversation', c => c.where('channelId', ...)), then related conversation.
    {
      id: 'seed_05_messages_whereExists_conversation_channel',
      comment:
        'source-app ref: channelAndThreadMessages (line 1012): messages.whereExists(conversation.where(channelId)).orderBy asc.related conversation',
      query: () =>
        zql.messages
          .whereExists('conversation', c => c.where('channelId', 'ch-pub-1'))
          .orderBy('createdAt', 'asc')
          .related('conversation'),
    },

    // seed_06 — source: channelLatestConversation (line 1538)
    // conversations in channel ordered desc, limit 1, .one() — latest row.
    {
      id: 'seed_06_channel_latest_conversation_one',
      comment:
        'source-app ref: channelLatestConversation (line 1541): conversations.where(channelId).orderBy desc.limit(1).one()',
      query: () =>
        zql.conversations
          .where('channelId', 'ch-pub-1')
          .orderBy('createdAt', 'desc')
          .orderBy('id', 'desc')
          .limit(1)
          .one(),
    },

    // seed_07 — source: dmChannelsLatestMessagesPaginated (line 1672)
    // related chain: channel → conversations ordered desc limit 1.
    // Modelled as channels.related('conversations', c => c.orderBy desc.limit(1)).
    {
      id: 'seed_07_channels_related_conversations_latest',
      comment:
        'source-app ref: dmChannelsLatestMessagesPaginated (line 1704): channel → conversations.orderBy(createdAt desc).limit(1) per-channel latest',
      query: () =>
        zql.channels.related('conversations', c =>
          c.orderBy('createdAt', 'desc').limit(1),
        ),
    },

    // seed_08 — source: conversationOfUserChannels (line 1713)
    // channels.whereExists(participants where userId=u1).related(conversations ordered desc limit 10)
    {
      id: 'seed_08_channels_whereExists_participants_related_conversations',
      comment:
        'source-app ref: conversationOfUserChannels (line 1713): channels.whereExists(participants.where userId).related(conversations.orderBy desc.limit 10)',
      query: () =>
        zql.channels
          .whereExists('participants', p => p.where('userId', 'u1'))
          .related('conversations', c => c.orderBy('createdAt', 'desc').limit(10)),
    },

    // seed_09 — source: getConversationAttachements (line 1549)
    // Exact port to ivm-parity:
    // attachments.where(exists('conversation', conv => conv.where('channelId', ...).where(OR flip-exists)))
    // Already have p21 for the full shape; this is the simplified "forward" branch.
    {
      id: 'seed_09_attachments_conv_flip_exists_single_branch',
      comment:
        'source-app ref: getConversationAttachements (line 1558): attachments filtered via exists(conv → OR(flip exists channel))',
      query: () =>
        zql.attachments.where(({exists}) =>
          exists('conversation', conv =>
            conv
              .where('channelId', 'ch-pub-1')
              .whereExists('channel', ch =>
                ch.where('visibility', '=', 'public'),
              ),
          ),
        ),
    },

    // seed_10 — source: searchChannelParticipants (line 637)
    // channel_participants.where(channelId).whereExists(user where name ILIKE '%q%')
    // → ivm-parity: participants.where(channelId).whereExists(user where name ILIKE '%a%')
    {
      id: 'seed_10_participants_whereExists_user_ilike',
      comment:
        'source-app ref: searchChannelParticipants (line 640): participants.where(channelId).whereExists(user.where name ILIKE)',
      query: () =>
        zql.participants
          .where('channelId', 'ch-pub-1')
          .whereExists('user', u => u.where('name', 'ILIKE', '%a%')),
    },

    // seed_11 — source: stagesByBoards (line 1114)
    // whereExists on a parent-side relationship: stages.whereExists('board', b => b.where('projectId', ...))
    // → ivm-parity: conversations.whereExists('channel', ch => ch.where('visibility', 'public'))
    {
      id: 'seed_11_conversations_whereExists_channel_visibility',
      comment:
        'source-app ref: stagesByBoards (line 1115): .whereExists(parent, p => p.where(foreignKey)) — exercises parent-side subquery',
      query: () =>
        zql.conversations.whereExists('channel', ch =>
          ch.where('visibility', '=', 'public'),
        ),
    },

    // seed_12 — source: userUnreadThreadActivities (line 889)
    // activities.where(isRead, false).whereExists('message', m => m.whereExists('conversation', c => c.where('replyCount', '>', 0)))
    // → ivm-parity: messages.whereExists('conversation', c => c.whereExists('channel', ch => ch.where('visibility', '=', 'public')))
    //   (already have p19 for the same shape but with different filter; this uses the 3-level chain variant)
    {
      id: 'seed_12_messages_whereExists_conv_whereExists_channel',
      comment:
        'source-app ref: userUnreadThreadActivities (line 893): triple-whereExists chain messages → conv → channel (3-level)',
      query: () =>
        zql.messages.whereExists('conversation', c =>
          c.whereExists('channel', ch => ch.where('visibility', '=', 'public')),
        ),
    },

    // seed_13 — source: channelDailyRecaps (line 2040)
    // spread OR via map: .where(or(...ids.map(id => cmp('channelId', id))))
    // → ivm-parity: messages with spread OR on conversationId
    {
      id: 'seed_13_messages_spread_or_conversationId',
      comment:
        'source-app ref: channelDailyRecaps (line 2044): .where(or(...ids.map(id => cmp(field, id)))) spread-OR pattern',
      query: () =>
        zql.messages.where(({or, cmp}) =>
          or(...['co-1', 'co-2', 'co-4'].map(id => cmp('conversationId', '=', id))),
        ),
    },

    // seed_14 — source: channelStatsByIds (line 602)
    // spread OR on a stats table → ivm-parity: conversations spread OR on channelId
    {
      id: 'seed_14_conversations_spread_or_channelId',
      comment:
        'source-app ref: channelStatsByIds (line 604): or(...ids.map(id => cmp(channelId, id))) applied to conversations',
      query: () =>
        zql.conversations.where(({or, cmp}) =>
          or(
            cmp('channelId', '=', 'ch-pub-1'),
            cmp('channelId', '=', 'ch-pub-2'),
          ),
        ),
    },

    // seed_15 — source: getPinnedMesseges (line 1596)
    // conversations.where(channelId).where(pinned, true) → boolean predicate
    // ivm-parity doesn't have 'pinned' but does have title; model as double-where on two scalar fields.
    {
      id: 'seed_15_conversations_double_scalar_where',
      comment:
        'source-app ref: getPinnedMessages (line 1599): conversations.where(channelId).where(scalarBool) — double scalar filter',
      query: () =>
        zql.conversations
          .where('channelId', 'ch-pub-1')
          .where('title', '!=', ''),
    },

    // seed_16 — zbugs: issuePreloadV2 (line 99)
    // issue.whereExists('project', ...).related('labels').related('viewState').orderBy.limit(1000)
    // → ivm-parity: conversations.whereExists('channel').related('messages').orderBy.limit(20)
    {
      id: 'seed_16_conversations_whereExists_channel_related_messages_limited',
      comment:
        'zbugs issuePreloadV2 (line 107): .whereExists(parent).related(rel1).related(rel2).orderBy.limit — preload shape',
      query: () =>
        zql.conversations
          .whereExists('channel', ch => ch.where('visibility', '=', 'public'))
          .related('messages', m => m.orderBy('createdAt', 'desc'))
          .related('attachments')
          .orderBy('createdAt', 'desc')
          .limit(20),
    },

    // seed_17 — zbugs: buildListQuery (line 424) — AND with optional cmp, textFilter OR-exists
    // or(cmp('title', 'ILIKE'), cmp('description', 'ILIKE'), exists('comments', q => q.where('body', 'ILIKE')))
    // → ivm-parity: or(cmp('body', 'ILIKE'), cmp('authorId', '='), exists('attachments', a => a.where('filename', 'ILIKE')))
    {
      id: 'seed_17_messages_or_cmp_and_exists_text_filter',
      comment:
        "zbugs buildListQuery (line 429): or(cmp ILIKE, cmp, exists(child.where ILIKE)) — text-filter + exists combo",
      query: () =>
        zql.messages.where(({or, cmp, exists}) =>
          or(
            cmp('body', 'ILIKE', '%standup%'),
            cmp('authorId', '=', 'u1'),
            exists('attachments', a => a.where('filename', 'ILIKE', '%.pdf')),
          ),
        ),
    },

    // seed_18 — zbugs: buildListQuery labels filter (line 437)
    // and(...labels.map(label => exists('issueLabels', q => q.whereExists('label', ...))))
    // → ivm-parity: and(exists('participants', where userId=u1), exists('participants', where userId=u2))
    // exercising AND-of-EXISTS on the same relationship with different filters.
    {
      id: 'seed_18_channels_and_of_exists_same_rel_diff_filter',
      comment:
        'zbugs buildListQuery labels (line 437): and(...labels.map(l => exists(rel, q => q.whereExists(child, ...)))) — AND-of-EXISTS array',
      query: () =>
        zql.channels.where(({and, exists}) =>
          and(
            exists('participants', p => p.where('userId', 'u1')),
            exists('participants', p => p.where('userId', 'u2')),
          ),
        ),
    },

    // seed_19 — zbugs: usersForProject filter=creators (line 153)
    // user.whereExists('createdIssues', i => i.whereExists('project', q => q.where(...)))
    // → ivm-parity: participants.whereExists(channel, ch => ch.whereExists(conversations, c => c.where(title)))
    // (users has no relationships in ivm-parity schema; use participants as the root instead)
    {
      id: 'seed_19_participants_whereExists_channel_whereExists_conversation',
      comment:
        'zbugs usersForProject filter=creators (line 153): root.whereExists(A, a => a.whereExists(B, b => b.where(field))) 2-level nested',
      query: () =>
        zql.participants.whereExists('channel', ch =>
          ch.whereExists('conversations', c => c.where('title', 'ILIKE', '%stand%')),
        ),
    },

    // seed_20 — source: userActivitiesPaginated (line 774) spread-OR on enum values
    // activities.where(or(...types.map(t => cmp('actorAction', '=', t))))
    // → ivm-parity: messages.where(or(...authorIds.map(id => cmp('authorId', '=', id))))
    {
      id: 'seed_20_messages_spread_or_authorId_enum_style',
      comment:
        'source-app ref: userActivitiesPaginated (line 785): .where(or(...types.map(t => cmp(field, =, t)))) spread-OR enum filter',
      query: () =>
        zql.messages.where(({or, cmp}) =>
          or(
            ...['u1', 'u2'].map(id => cmp('authorId', '=', id)),
          ),
        ),
    },

    // seed_21 — source: userCallHistory (line 726) NOT IN on multiple values
    // calls.where(helpers.cmp('status', 'NOT IN', [...]))
    // → ivm-parity: conversations.where(id, 'NOT IN', [...])  (complements p28)
    {
      id: 'seed_21_conversations_not_in',
      comment:
        'source-app ref: userCallHistory (line 733): .where(cmp(field, NOT IN, [val1, val2, ...])) on conversations',
      query: () =>
        zql.conversations.where('id', 'NOT IN', ['co-4', 'co-5']),
    },

    // seed_22 — source: channelLinks (line 982) complex AND-of-OR with exists inside OR
    // or(cmp(vis, DEFAULT), and(cmp(vis, PERSONAL), cmp(createdBy, user)), and(cmp(vis, PERSONAL), exists(sharedWith, sw => sw.where(userId, user))))
    // → ivm-parity: messages.where(or(cmp(visibleTo IS NULL), and(cmp(authorId, u1), cmp(conversationId, co-1)), and(cmp(authorId, u2), exists(attachments, a => a.where(filename ILIKE %.png)))))
    {
      id: 'seed_22_messages_or_cmp_and_cmp_and_exists',
      comment:
        'source-app ref: channelLinks (line 989): or(cmp, and(cmp,cmp), and(cmp, exists(rel))) — complex visibility OR with AND-exists branch',
      query: () =>
        zql.messages.where(({or, and, cmp, exists}) =>
          or(
            cmp('visibleTo', 'IS', null),
            and(cmp('authorId', '=', 'u1'), cmp('conversationId', '=', 'co-1')),
            and(
              cmp('authorId', '=', 'u2'),
              exists('attachments', a => a.where('filename', 'ILIKE', '%.png')),
            ),
          ),
        ),
    },

    // seed_23 — source: messageNudges (line 152) whereExists chain with OR+AND inside
    // → ivm-parity: messages.whereExists('conversation', c => c.whereExists('channel', ch => ch.where(or(cmp(vis public), and(cmp(vis private), exists(participants, p => p.where(userId)))))))
    {
      id: 'seed_23_messages_whereExists_conv_channel_or_and_exists',
      comment:
        'source-app ref: messageNudges (line 178): whereExists 3-level with OR(cmp, AND(cmp, exists(participants))) inside innermost',
      query: () =>
        zql.messages.whereExists('conversation', c =>
          c.whereExists('channel', ch =>
            ch.where(({or, and, cmp, exists}) =>
              or(
                cmp('visibility', '=', 'public'),
                and(
                  cmp('visibility', '=', 'private'),
                  exists('participants', p => p.where('userId', 'u1')),
                ),
              ),
            ),
          ),
        ),
    },

    // seed_24 — source: channelConversationsPaginated participants sub-filter (line 1353)
    // participantQuery.where(or(cmp(participationType, AUTHOR), cmp(userId, ctx.userID))).orderBy
    // → ivm-parity: participants.where(or(cmp(userId, u1), cmp(userId, u3))).orderBy
    {
      id: 'seed_24_participants_or_cmp_orderby',
      comment:
        'source-app ref: channelConversationsPaginated participants sub-filter (line 1355): participants.where(or(cmp, cmp)).orderBy',
      query: () =>
        zql.participants
          .where(({or, cmp}) =>
            or(cmp('userId', '=', 'u1'), cmp('userId', '=', 'u3')),
          )
          .orderBy('userId', 'asc'),
    },
  ];

// ---------------------------------------------------------------------------
// Main: read corpus, append new seeds (de-duped), write back.
// ---------------------------------------------------------------------------

type CorpusEntry = {
  id: string;
  ast: unknown;
  kind: string;
};

const corpusPath = new URL('./ast_corpus.json', import.meta.url).pathname;

const existing: CorpusEntry[] = JSON.parse(readFileSync(corpusPath, 'utf-8'));
const existingIds = new Set(existing.map(e => e.id));

let added = 0;
const newEntries: CorpusEntry[] = [];

for (const {id, comment, query} of seeds) {
  if (existingIds.has(id)) {
    console.log(`SKIP (already exists): ${id}`);
    continue;
  }
  try {
    const q = query();
    const ast = (q as {ast: unknown}).ast;
    newEntries.push({id, ast, kind: 'seed'});
    console.log(`ADD  ${id}  // ${comment}`);
    added++;
  } catch (e) {
    console.error(`FAIL ${id}: ${(e as Error).message}`);
  }
}

if (added > 0) {
  const updated = [...existing, ...newEntries];
  writeFileSync(corpusPath, JSON.stringify(updated, null, 2) + '\n', 'utf-8');
  console.log(`\nAppended ${added} seed entries to ast_corpus.json`);
} else {
  console.log('\nNo new entries to append (all already present).');
}
