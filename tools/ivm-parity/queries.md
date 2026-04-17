# ZQL queries extracted from source-app

Source: `/Users/harsharanga/code/source-app/backend/src/zero/queries.ts` (2187 lines, 145 `defineQuery` calls).

The subset below covers the shapes our Rust v2 driver needs to exercise. Each query is shown as source ZQL then the approximate AST it lowers to after scalar-subquery resolution (what zero-cache actually sees). Tables are translated to our ivm-parity schema (conversations, messages, channels, participants, attachments, users).

---

## 1. Simple WHERE + orderBy + limit

source-app `conversationMessagesV2` (line 108):

```ts
zql.messages
  .where('conversationId', conversationId)
  .where(({ or, cmp }) => or(cmp('visibleTo', 'IS', null), cmp('visibleTo', '=', ctx.userID)))
  .orderBy('createdAt', 'asc')
  .related('attachments')
```

Features: `AND(col=literal, OR(col IS null, col=literal))`, `orderBy`, `related`.

## 2. Two-level nested WHERE EXISTS (no flip)

source-app `ticketsForEmailChannels` (line 335):

```ts
zql.tickets
  .whereExists('conversation', (conv) =>
    conv.whereExists('channel', (ch) => ch.where('type', ChannelType.EMAIL))
  )
```

In our schema: `conversations.whereExists('channel', c => c.where('visibility', '=', 'public'))`.

Features: nested EXISTS without flip.

## 3. `whereExists` with `{ scalar: true }`

source-app `supportTicketsFiltered` (line 348, similar to zbugs issueListV2). Already covered by zbugs parity work — scalar EXISTS gets pre-resolved to a literal WHERE clause TS-side before hitting Rust.

## 4. `exists(...)` inside WHERE with `{ flip: true }` — CRITICAL

source-app `getConversationAttachements` (line 1552). The filter:

```ts
query.where(({ exists }) =>
  exists('conversation', (conv) =>
    conv.where('channelId', channelId).where(({ or, exists }) =>
      or(
        exists('channel', (c) => c.where('visibility', '=', ChannelVisibility.PUBLIC), {
          flip: true,
        }),
        exists(
          'channel',
          (c) =>
            c.whereExists(
              'participants',
              (v) => v.where('userId', ctx.userID).where('channelId', channelId),
              { flip: true }
            ),
          { flip: true }
        )
      )
    )
  )
);
```

Features: OR of two flipped EXISTS, the second with a nested flipped `whereExists`. The shape our Rust v2 currently lowers to chained ExistsT (hydration-correct, push-path differs). Once Chain wires FlippedJoin (task #148), this is the test that will expose any push-path divergence.

## 5. `whereExists` with a filter and a parent-join-key that isn't the PK

source-app `browsableChannels` (line 499):

```ts
zql.channels.whereExists('participantsStatus', (p) => p.where('userId', ctx.userID))
```

In our schema: `channels.whereExists('participants', p => p.where('userId', 'u1'))`.

## 6. Deep `.related()` chain

source-app `channelConversations` (line 31): parent → related(initialMessage) → .related(reactions, attachments, nudgeCounts) — 3 levels deep with per-level filters.

## 7. `.one()` — Take(1) with explicit partition key

source-app `getConversationById` (line 128):

```ts
zql.conversations.where('id', id).one()
```

In our schema: same — trivial but validates Take(1).

---

## Mapping source-app tables → ivm-parity tables

| source-app              | ivm-parity        | notes                               |
|-------------------|------------------|-------------------------------------|
| `channels`        | `channels`       | same                                |
| `conversations`   | `conversations`  | same                                |
| `messages`        | `messages`       | same                                |
| `participants`    | `participants`   | join table (userId, channelId)      |
| `attachments`     | `attachments`    | same                                |
| `users`           | `users`          | same                                |
| `tickets`         | (dropped)        | not needed for flip path            |
| `message_attachments` | `attachments` | source-app uses underscore; we camelCase  |

The table names + column names (camelCase quoted in PG) are chosen so that most source-app query ASTs can transplant with only literal-value rewrites (e.g. swapping the source app's user IDs for our `u1`/`u2`).
