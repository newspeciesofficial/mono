# xyne query-pattern catalog

One AST JSON per distinct structural pattern found in
`xyne-spaces-login/dashboard/src/zero/queries.ts` (139 `defineQuery`
calls). The goal is to exercise every shape the Rust v2 driver must
handle — not every individual query — so a tester can drop any of
these into `client-simulator` / `query-runner.ts` and observe whether
the server emits the expected row shape.

Each JSON contains a single `initConnection` message with one
`desiredQueriesPatch` entry whose AST mirrors what the xyne helper
produces after `bindStaticParameters`.

Table/column names use the **xyne-lite** schema (see
`../../zero-schema.ts`). A few patterns retain xyne's original table
names where the xyne-lite rename wasn't a clean fit.

## Pattern index

| File | Xyne source | Exercises |
|---|---|---|
| `01-simple-where.json` | `channelStats` (line 599) | single `=` WHERE |
| `02-one-limit.json` | `getConversationById` (225) | `.one()` / `LIMIT 1` |
| `03-multi-where-and.json` | `conversationParticipantByConversationId` (244) | two chained `.where()` (implicit AND) |
| `04-where-comparison.json` | `getConversationByTimestamp` (269) | `<=` comparison |
| `05-where-or.json` | `conversationMessages` (101) | dynamic OR inside WHERE |
| `06-where-and.json` | `ticketsSearch` (1970) | dynamic AND inside WHERE |
| `07-where-is-null.json` | `channelConversations` (33) | `IS NULL` on nullable column |
| `08-where-in-array.json` | `subTicketsByIds` (1997) | `IN` with multi-value array |
| `09-where-ilike.json` | `ticketsSearch` (1970) | `ILIKE '%x%'` pattern |
| `10-orderby-multi.json` | `userActivitiesPaginated` (774) | two-column ORDER BY |
| `11-start-cursor.json` | `userActivitiesPaginated` (774) | `.start(row, {inclusive: false})` cursor |
| `12-start-limit-pagination.json` | `userActivitiesPaginated` (774) | cursor + ORDER BY + LIMIT |
| `13-related-simple.json` | `getMessageForActivityV2` (878) | one `related()` |
| `14-related-with-filter.json` | `channelConversations` (33) | related with nested WHERE |
| `15-related-nested.json` | `channelConversations` (33) | related → related (2 levels) |
| `16-related-chain-multi.json` | `userActivitiesPaginated` (774) | related across 4 sibling tables |
| `17-related-one.json` | `channelLatestMessage` (1648) | related with `.one()` partition |
| `18-whereExists-simple.json` | `getAllChannelsUserStatus` (680) | single EXISTS |
| `19-whereExists-nested.json` | `ticketsForEmailChannels` (448) | EXISTS inside EXISTS |
| `20-exists-flip.json` | xyne `getConversationAttachements` (1549) | single `flip: true` EXISTS |
| `21-exists-or-flip-pair.json` | `getConversationAttachements` (1549) | full nested flip triple — our Rust FlippedJoin target |
| `22-whereExists-scalar.json` | `supportTicketsFiltered` (460) | `scalar: true` — TS wrapper resolves to literal WHERE |
| `23-not-exists.json` | (synthetic) | `NOT EXISTS` operator |

## Running

```sh
# Against xyne-lite zero-cache (once the deploy script is in place):
tsx ../../query-runner.ts \
  --view-syncer ws://localhost:4858 \
  --queries patterns/20-exists-flip.json

# Or multiple at once:
for q in patterns/*.json; do
  tsx ../../query-runner.ts --view-syncer ws://localhost:4858 --queries "$q"
done
```
