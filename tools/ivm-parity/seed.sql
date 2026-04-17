

-- ~30 rows of synthetic data, chosen so every test query has a
-- non-trivial result set (non-zero, non-trivial filtering).

-- 3 channels: 2 public, 1 private.
INSERT INTO channels (id, name, visibility) VALUES
  ('ch-pub-1',  'general',     'public'),
  ('ch-pub-2',  'random',      'public'),
  ('ch-priv-1', 'eng-private', 'private');

-- 3 users.
INSERT INTO users (id, name) VALUES
  ('u1', 'alice'),
  ('u2', 'bob'),
  ('u3', 'carol');

-- u1 is in both public channels + the private one.
-- u2 is in public channels only.
-- u3 is in the private channel only.
INSERT INTO participants ("userId", "channelId") VALUES
  ('u1', 'ch-pub-1'),
  ('u1', 'ch-pub-2'),
  ('u1', 'ch-priv-1'),
  ('u2', 'ch-pub-1'),
  ('u2', 'ch-pub-2'),
  ('u3', 'ch-priv-1');

-- 5 conversations spread across channels.
INSERT INTO conversations (id, "channelId", title, "createdAt") VALUES
  ('co-1', 'ch-pub-1',  'welcome',        1000),
  ('co-2', 'ch-pub-1',  'standup',        2000),
  ('co-3', 'ch-pub-2',  'watercooler',    3000),
  ('co-4', 'ch-priv-1', 'secret project', 4000),
  ('co-5', 'ch-priv-1', 'layoffs',        5000);

-- 10 messages, some with attachments, some visibility-restricted.
INSERT INTO messages (id, "conversationId", "authorId", body, "createdAt", "visibleTo") VALUES
  ('m-1',  'co-1', 'u1', 'hi team',        1100, NULL),
  ('m-2',  'co-1', 'u2', 'welcome',        1200, NULL),
  ('m-3',  'co-2', 'u1', 'standup notes',  2100, NULL),
  ('m-4',  'co-2', 'u2', 'blocked on db',  2200, NULL),
  ('m-5',  'co-3', 'u1', 'coffee anyone?', 3100, NULL),
  ('m-6',  'co-4', 'u1', 'design doc',     4100, NULL),
  ('m-7',  'co-4', 'u3', 'ship date',      4200, NULL),
  ('m-8',  'co-5', 'u1', 'announcement',   5100, NULL),
  -- visibility-restricted: only u3 can see.
  ('m-9',  'co-5', 'u1', 'priv msg u3',    5200, 'u3'),
  -- visibility-restricted: only u1 can see.
  ('m-10', 'co-1', 'u2', 'priv msg u1',    1300, 'u1');

-- 6 attachments across messages.
INSERT INTO attachments (id, "messageId", "conversationId", filename, "createdAt") VALUES
  ('a-1', 'm-1', 'co-1', 'welcome.png',    1101),
  ('a-2', 'm-3', 'co-2', 'standup.pdf',    2101),
  ('a-3', 'm-6', 'co-4', 'design-v1.pdf',  4101),
  ('a-4', 'm-6', 'co-4', 'design-v2.pdf',  4102),
  ('a-5', 'm-7', 'co-4', 'gantt.png',      4201),
  ('a-6', 'm-8', 'co-5', 'roadmap.pdf',    5101);
