/**
 * Query-pattern catalog for ivm-parity — one ZQL builder expression per
 * distinct pattern we found in source-app's
 * `dashboard/src/zero/queries.ts`. The harness extracts the AST from
 * each builder return value and ships it to zero-cache via
 * `client-simulator`'s initConnection path.
 *
 * Table names match `zero-schema.ts` (ivm-parity); literal values match
 * the seed in `seed.sql`. Each pattern comments its source-app counterpart
 * and the structural feature it exercises.
 */
import {createBuilder} from '@rocicorp/zero';
import {schema} from './zero-schema.ts';

const zql = createBuilder(schema);

/**
 * Patterns — each value is a ZQL query expression. The harness runs
 * `.ast` on each and sends that AST through initConnection. Dynamic
 * code paths (if/switch) from source-app queries are flattened to a single
 * representative branch since our goal is *shape* coverage, not full
 * behavioural coverage.
 */
export const patterns = {
  // 01 simple WHERE — source: channelStats(line 599).
  p01_simple_where: () => zql.channels.where('id', 'ch-pub-1'),

  // 02 .one() — source: getConversationById(225).
  p02_one_limit: () => zql.conversations.where('id', 'co-1').one(),

  // 03 chained .where() (implicit AND) — source:
  // conversationParticipantByConversationId(244).
  p03_multi_where_and: () =>
    zql.participants.where('userId', 'u1').where('channelId', 'ch-pub-1'),

  // 04 comparison operator — source: getConversationByTimestamp(269).
  p04_where_comparison: () =>
    zql.conversations
      .where('channelId', 'ch-pub-1')
      .where('createdAt', '<=', 2500)
      .orderBy('createdAt', 'desc')
      .orderBy('id', 'desc')
      .limit(5),

  // 05 dynamic OR — source: conversationMessages(101) visibility filter.
  p05_where_or: () =>
    zql.messages
      .where('conversationId', 'co-1')
      .where(({cmp, or}) =>
        or(cmp('visibleTo', 'IS', null), cmp('visibleTo', '=', 'u1')),
      )
      .orderBy('createdAt', 'asc')
      .orderBy('id', 'asc'),

  // 06 dynamic AND — source: ticketsSearch(1970) double-NOT-equal.
  p06_where_and: () =>
    zql.messages.where(({cmp, and}) =>
      and(cmp('authorId', '!=', 'u3'), cmp('authorId', '!=', 'u2')),
    ),

  // 07 IS NULL — source: conversationMessages(101) visibleTo branch.
  p07_where_is_null: () =>
    zql.messages.where('visibleTo', 'IS', null),

  // 08 IN array — source: subTicketsByIds(1997) / ticketsByIds(547).
  p08_where_in_array: () =>
    zql.messages.where('id', 'IN', ['m-1', 'm-3', 'm-5', 'm-7']),

  // 09 ILIKE — source: ticketsSearch(1970) title/externalId search.
  p09_where_ilike: () =>
    zql.messages.where('body', 'ILIKE', '%standup%'),

  // 10 multi-column orderBy — source: userActivitiesPaginated(774).
  p10_orderby_multi: () =>
    zql.messages.orderBy('createdAt', 'desc').orderBy('id', 'desc'),

  // 11 start cursor (inclusive) — source: getPinnedMesseges start arg.
  p11_start_cursor: () =>
    zql.messages
      .orderBy('createdAt', 'asc')
      .orderBy('id', 'asc')
      .start({id: 'm-5', createdAt: 3100}, {inclusive: true}),

  // 12 full pagination — source: userActivitiesPaginated(774).
  p12_start_limit_pagination: () =>
    zql.messages
      .orderBy('createdAt', 'desc')
      .orderBy('id', 'desc')
      .start({id: 'm-10', createdAt: 5200}, {inclusive: false})
      .limit(3),

  // 13 simple related — source: getMessageForActivityV2(878).
  p13_related_simple: () =>
    zql.messages.where('id', 'm-6').related('attachments'),

  // 14 related with filter — source: channelConversations(33) initialMessage
  // filter.
  p14_related_with_filter: () =>
    zql.conversations
      .where('channelId', 'ch-pub-1')
      .related('messages', m =>
        m.where(({cmp, or}) =>
          or(cmp('visibleTo', 'IS', null), cmp('visibleTo', '=', 'u1')),
        ),
      ),

  // 15 nested related — source: channelConversations(33)
  // initialMessage → reactions/attachments chain.
  p15_related_nested: () =>
    zql.channels.related('conversations', c =>
      c.related('messages', m =>
        m.orderBy('createdAt', 'asc').orderBy('id', 'asc'),
      ),
    ),

  // 16 multi-sibling related — source: userActivitiesPaginated(774)'s
  // `.related(message, m => m.related(conversation).related(reactions)...)`.
  p16_related_chain_multi: () =>
    zql.messages
      .where('id', 'm-6')
      .related('conversation')
      .related('author')
      .related('attachments'),

  // 17 related with .one() — source: channelLatestMessage(1648) latest-per-group.
  // Uses the `messages` relationship defined on `conversations`, narrowed
  // to the newest row (`.one()` produces an AST with `limit=1`).
  p17_related_one: () =>
    zql.conversations.related('messages', m =>
      m.orderBy('createdAt', 'desc').orderBy('id', 'desc').one(),
    ),

  // 18 simple whereExists — source: getAllChannelsUserStatus(680).
  p18_whereExists_simple: () =>
    zql.channels.whereExists('participants', p => p.where('userId', 'u1')),

  // 19 nested whereExists — source: ticketsForEmailChannels(448).
  p19_whereExists_nested: () =>
    zql.messages.whereExists('conversation', c =>
      c.whereExists('channel', ch => ch.where('visibility', '=', 'public')),
    ),

  // 20 single flip=true — source: getConversationAttachements(1549)
  // inner flip branch, simplified to one level.
  p20_exists_flip: () =>
    zql.attachments.where(({exists}) =>
      exists('conversation', conv => conv.where('channelId', 'ch-pub-1'), {
        flip: true,
      } as const),
    ),

  // 21 nested flip pair — full shape from
  // source_app.getConversationAttachements(1549). This is the target for
  // Rust FlippedJoin Chain integration (task #148).
  p21_exists_or_flip_pair: () =>
    zql.attachments.where(({exists}) =>
      exists('conversation', conv =>
        conv.where('channelId', 'ch-pub-1').where(({or, exists}) =>
          or(
            exists('channel', c => c.where('visibility', '=', 'public'), {
              flip: true,
            } as const),
            exists(
              'channel',
              c =>
                c.whereExists('participants', (v: any) =>
                  v.where('userId', 'u1').where('channelId', 'ch-pub-1'),
                ),
              {flip: true} as const,
            ),
          ),
        ),
      ),
    ),

  // 23 NOT EXISTS — synthetic, analog of `whereExists` with `.not()`.
  p23_not_exists: () =>
    zql.channels.where(({not, exists}) =>
      not(exists('participants', p => p.where('userId', 'u1'))),
    ),

  // 24 single flip with filtered child — targets FlippedJoin push
  // on an Add child that newly passes the child filter (must cause
  // the parent to appear for the first time).
  p24_flip_simple_filtered: () =>
    zql.messages.where(({exists}) =>
      exists(
        'conversation',
        c => c.where('channelId', 'ch-pub-1'),
        {flip: true} as const,
      ),
    ),

  // 25 flip where MULTIPLE parents correlate to one child — tests
  // that a single child mutation produces add/remove for every
  // correlated parent. In TS, FlippedJoin fetches the child first
  // and then walks parents keyed by childKey; an insert of one
  // qualifying child should surface *all* matching parents.
  p25_flip_multi_parent: () =>
    zql.attachments.where(({exists}) =>
      exists(
        'message',
        m => m.where('authorId', 'u1'),
        {flip: true} as const,
      ),
    ),

  // 26 nested flip where INNER is NOT flipped — exercises the
  // interaction between ExistsT (non-flip) inside a flipped outer
  // EXISTS. TS's FlippedJoin wraps the ExistsT's output; RS's
  // current fallback lowers both to ExistsT, so push ordering can
  // still diverge on grandchild filter flips.
  p26_flip_outer_exists_inner: () =>
    zql.attachments.where(({exists}) =>
      exists(
        'conversation',
        conv =>
          conv.whereExists('channel', ch =>
            ch.where('visibility', '=', 'public'),
          ),
        {flip: true} as const,
      ),
    ),

  // 27 IS NOT NULL — source: tickets.where(({cmp}) => cmp('ticketType',
  // 'IS NOT', null)) at line 101. Exercises the inverse of p07's IS
  // NULL. Seed has m-9 (visibleTo='u3') and m-10 (visibleTo='u1') →
  // expected 2 rows.
  p27_is_not_null: () =>
    zql.messages.where(({cmp}) => cmp('visibleTo', 'IS NOT', null)),

  // 28 NOT IN — source: userCallHistory at line 743 uses NOT IN to
  // exclude a set of call statuses. Seed has 10 messages; excluding
  // 2 leaves 8 rows.
  p28_not_in: () =>
    zql.messages.where('id', 'NOT IN', ['m-1', 'm-2']),

  // 29 whereExists 3 levels deep — source: getConversationAttachements
  // at line 1552 walks 3 levels (message → conversation → channel →
  // participants). Our chain: messages → conversation → channel →
  // participants where userId='u1'. Every seeded message's channel
  // has u1 as participant → expected all 10 messages.
  p29_whereExists_3levels: () =>
    zql.messages.whereExists('conversation', c =>
      c.whereExists('channel', ch =>
        ch.whereExists('participants', p => p.where('userId', 'u1')),
      ),
    ),

  // 30 whereExists with IN inside subquery — source: workflowsPaginated
  // at line 314 uses whereExists('ticket', t => t.where('id', 'IN',
  // [...])) for ticket-id batching. Channels whose participants
  // include u1 OR u2 — all 3 seeded channels (ch-pub-1, ch-pub-2,
  // ch-priv-1 all have at least one of u1/u2).
  p30_whereExists_with_in: () =>
    zql.channels.whereExists('participants', p =>
      p.where('userId', 'IN', ['u1', 'u2']),
    ),

  // 31 whereExists with ILIKE inside subquery — source:
  // searchChannelParticipants at line 1216 uses
  // whereExists('user', u => u.where('name', 'ILIKE', '%q%')).
  // Conversations whose messages contain 'standup' in body →
  // matches co-2 (m-3 body='standup notes') → expected 1 row.
  p31_whereExists_with_ilike: () =>
    zql.conversations.whereExists('messages', m =>
      m.where('body', 'ILIKE', '%standup%'),
    ),

  // 32 OR of non-flipped EXISTS — source: conversationMessages at
  // line 91 builds `or(exists('a'), exists('b'))` via helpers. We
  // use two *different* relationships on channels — participants
  // (for u1) OR conversations (titled 'welcome'). All 3 seeded
  // channels either have u1 as participant or have a 'welcome'
  // conversation → expected 3 rows.
  p32_or_of_plain_exists: () =>
    zql.channels.where(({or, exists}) =>
      or(
        exists('participants', p => p.where('userId', 'u1')),
        exists('conversations', c => c.where('title', 'welcome')),
      ),
    ),

  // 33 related() 3 levels deep — source: userActivities at line 772
  // drills activities → message → conversation → reactions, and the
  // V2 chain at line 1152 goes 3+ levels. In ivm-parity:
  // channels.related('conversations', c => c.related('messages',
  // m => m.related('attachments'))). Produces every channel with
  // its nested conversations → messages → attachments tree.
  p33_related_3levels: () =>
    zql.channels.related('conversations', c =>
      c.related('messages', m => m.related('attachments')),
    ),

  // 34 related() with orderBy + limit (NO `.one()`) — source:
  // ticketDetailsById at line 423 uses `.related('rcas', r =>
  // r.orderBy(...).limit(1))`. Distinct from p17 because `.one()`
  // wraps the result in a singleton; bare limit(1) leaves it as a
  // collection. Here we keep the semantics but use limit(2) so the
  // collection has non-trivial length.
  p34_related_orderby_limit: () =>
    zql.conversations.related('messages', m =>
      m.orderBy('createdAt', 'desc').limit(2),
    ),

  // 35 spread OR — source: channelStatsByIds at line 529 uses
  // `or(...ids.map(id => cmp('channelId', '=', id)))`. Same shape,
  // applied to messages: matches m-1 OR m-3 OR m-5 → expected 3.
  p35_spread_or: () =>
    zql.messages.where(({or, cmp}) =>
      or(
        ...['m-1', 'm-3', 'm-5'].map(id => cmp('id', '=', id)),
      ),
    ),

  // 36 AND-of-EXISTS inside OR — source: messageNudges at line 2043
  // composes `or(and(exists(a), exists(b)), cmp)`. Channels where
  // (has u1 AS participant AND has a 'welcome' conversation) OR
  // visibility='private'. ch-pub-1 has u1 AND welcome → match.
  // ch-pub-2 has u1 but no welcome → no AND. ch-priv-1: visibility
  // matches second branch. Expected 2 rows (ch-pub-1, ch-priv-1).
  p36_and_of_exists_in_or: () =>
    zql.channels.where(({or, and, exists, cmp}) =>
      or(
        and(
          exists('participants', p => p.where('userId', 'u1')),
          exists('conversations', c => c.where('title', 'welcome')),
        ),
        cmp('visibility', '=', 'private'),
      ),
    ),

  // 37 full pagination — source: channelConversationsPaginated at
  // line 1370 uses full `.orderBy × 2 .start(row, {inclusive:false})
  // .limit(N)`. All three atoms are covered piecewise in p10-p12,
  // but source-app applies them together frequently; worth a direct test.
  p37_full_pagination: () =>
    zql.messages
      .orderBy('createdAt', 'desc')
      .orderBy('id', 'desc')
      .start({id: 'm-7', createdAt: 4200}, {inclusive: false})
      .limit(3),

  // === Regression-prevention patterns — one per RS code-branch not
  // already covered by p01-p37. Each pattern targets a SPECIFIC
  // ivm_v2 branch so that a future change in one area immediately
  // surfaces if it breaks another. Labels refer to the branch map
  // produced by the `Explore` agent (see session notes). ===

  // 38 ExistsT cache hit path during fetch — `decorate_for_forward`
  // with the same `parent_join_key` reached twice in one hydration
  // (Branch E11). Achieved by `whereExists` on a relationship whose
  // parent-side join key equals the parent PK but multiple rows
  // share the same key projection: messages.whereExists('attachments',
  // ...) where many messages share a conversation that narrows down.
  // The ExistsT cache keys on parent_join_key values, so multiple
  // messages with the same message.id trigger the cached branch.
  p38_exists_cache_refetch: () =>
    zql.messages
      .whereExists('attachments', a => a.where('filename', 'ILIKE', '%.png'))
      .orderBy('createdAt', 'asc')
      .orderBy('id', 'asc')
      .limit(3),

  // 39 Join multi-parent — one child row correlates to multiple
  // parents (Branch J3 with N>1). conversations.related('messages')
  // where the SAME conversation has many messages; this exercises
  // JoinT's per-parent ChildChange emission during
  // `join.push_child`. Add/remove of a shared author would surface
  // any bug where only the first parent emits.
  p39_join_multi_parent_different_keys: () =>
    zql.conversations.related('messages', m =>
      m.orderBy('createdAt', 'asc').orderBy('id', 'asc'),
    ),

  // 40 Take refetch when filter might drop the refetch row (Branch
  // T4 + F3 interaction). `.where(flag).limit(N)` — the filter sits
  // before Take; an Add that beats the bound but fails the predicate
  // must not stall Take's refetch loop. Uses `messages` with
  // `visibleTo IS NULL` + `limit`, so a new message with
  // `visibleTo` set gets filtered out while Take asks for new bound.
  p40_take_refetch_filtered: () =>
    zql.messages
      .where('visibleTo', 'IS', null)
      .orderBy('createdAt', 'asc')
      .orderBy('id', 'asc')
      .limit(2),

  // 41 Skip + Edit split across cursor (Branch S7). A start cursor
  // mid-result-set plus a query that can observe row-sort-rank
  // changes. Testing requires an Edit that moves a row from "after
  // cursor" to "before cursor" — so we scope it to messages ordered
  // by createdAt with a cursor before m-5.
  p41_skip_edit_split_cursor: () =>
    zql.messages
      .orderBy('createdAt', 'asc')
      .orderBy('id', 'asc')
      .start({id: 'm-5', createdAt: 3100}, {inclusive: false})
      .limit(10),

  // 42 OR of nested-EXISTS branches — 3 levels deep (Branch O2 with
  // deeper decoration accumulation). Tests that union-all decoration
  // preserves nested relationship trees across branches without
  // dedup loss.
  p42_or_nested_exists_3level: () =>
    zql.channels.where(({or, exists}) =>
      or(
        exists('conversations', c =>
          c.whereExists('messages', m => m.where('body', 'ILIKE', '%standup%')),
        ),
        exists('participants', p => p.where('userId', 'u2')),
      ),
    ),

  // 43 NOT EXISTS advance-side parity (Branches E9 / E10). Parent
  // disappears when first matching child is added (E9), reappears
  // when last matching child is removed (E10). Exercises
  // `ExistsT::push_child` NOT EXISTS flip. Complements p23 which
  // only covers the fetch-side shape.
  p43_not_exists_with_advance: () =>
    zql.conversations.where(({not, exists}) =>
      not(exists('messages', m => m.where('body', 'ILIKE', '%blocked%'))),
    ),
};

export type PatternName = keyof typeof patterns;
