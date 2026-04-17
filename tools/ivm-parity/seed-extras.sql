-- Additive seed extension. Layered on top of `seed.sql` for richer
-- coverage: edge-case timestamps, ILIKE-friendly text, multiple visibility
-- variants, deeper Take/Skip cursor surface area, NULL pile-ups in nullable
-- columns. Apply AFTER seed.sql:
--
--   psql $XYNE_LITE_PG_URL -f seed.sql
--   psql $XYNE_LITE_PG_URL -f seed-extras.sql
--
-- Strictly additive — never UPDATE or DELETE existing rows. IDs are
-- prefixed `x-` (extras) so they're easy to grep and never collide with
-- the base seed.

-- 2 more channels — `x-ch-empty` has zero conversations (matters for
-- NOT EXISTS branches); `x-ch-deep` has many conversations to exercise
-- Take/Skip / refetch.
INSERT INTO channels (id, name, visibility) VALUES
  ('x-ch-empty', 'empty-channel',           'public'),
  ('x-ch-deep',  'deep-history',            'public');

-- 2 more users — long names for ILIKE windowing.
INSERT INTO users (id, name) VALUES
  ('x-u-long', 'alice-anderson-engineering'),
  ('x-u-edge', 'edge-case-user');

-- u1/u2 join the deep-history channel; nobody joins the empty one.
INSERT INTO participants ("userId", "channelId") VALUES
  ('u1',       'x-ch-deep'),
  ('u2',       'x-ch-deep'),
  ('x-u-edge', 'x-ch-deep');

-- 5 conversations on x-ch-deep at varied timestamps so cursor + limit
-- branches see a real Take window. Titles include searchable text.
INSERT INTO conversations (id, "channelId", title, "createdAt") VALUES
  ('x-co-1', 'x-ch-deep',  'standup-monday',     6000),
  ('x-co-2', 'x-ch-deep',  'planning-session',   6500),
  ('x-co-3', 'x-ch-deep',  'standup-tuesday',    7000),
  ('x-co-4', 'x-ch-deep',  'retrospective',      7500),
  ('x-co-5', 'x-ch-deep',  'standup-wednesday',  8000);

-- 8 messages with varied visibility — gives nullable visibleTo column
-- both NULL and non-NULL ranges large enough that NOT IN [], IS NOT NULL,
-- and IN [...] discriminate a meaningful subset.
INSERT INTO messages (id, "conversationId", "authorId", body, "createdAt", "visibleTo") VALUES
  ('x-m-1',  'x-co-1', 'u1',       'standup notes for monday',     6100, NULL),
  ('x-m-2',  'x-co-1', 'u2',       'reviewed the plan',            6200, NULL),
  ('x-m-3',  'x-co-2', 'u1',       'standup planning kickoff',     6600, NULL),
  ('x-m-4',  'x-co-3', 'u1',       'standup notes for tuesday',    7100, 'u1'),
  ('x-m-5',  'x-co-3', 'x-u-edge', 'edge-case message body',       7150, NULL),
  ('x-m-6',  'x-co-4', 'u1',       'retro action items',           7600, NULL),
  ('x-m-7',  'x-co-5', 'u1',       'standup notes for wednesday',  8100, NULL),
  ('x-m-8',  'x-co-5', 'u2',       'private follow-up',            8200, 'x-u-edge');

-- 4 attachments — multiple per message tests Join multi-child paths.
INSERT INTO attachments (id, "messageId", "conversationId", filename, "createdAt") VALUES
  ('x-a-1', 'x-m-1', 'x-co-1', 'monday-standup.pdf',  6101),
  ('x-a-2', 'x-m-1', 'x-co-1', 'monday-charts.png',   6102),
  ('x-a-3', 'x-m-3', 'x-co-2', 'planning-doc.pdf',    6601),
  ('x-a-4', 'x-m-7', 'x-co-5', 'wednesday-recap.pdf', 8101);
