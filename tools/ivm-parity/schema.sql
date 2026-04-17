-- Minimal xyne-like schema. Column names use camelCase (quoted) to
-- match what xyne's Zero schema expects so captured ASTs from xyne
-- can be transplanted with minimal rewriting.

-- Note: this file lives in its own database now; no schema namespacing.
-- schema defaulted to public


CREATE TABLE channels (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  visibility  TEXT NOT NULL  -- 'public' | 'private'
);

CREATE TABLE users (
  id    TEXT PRIMARY KEY,
  name  TEXT NOT NULL
);

CREATE TABLE participants (
  "userId"     TEXT NOT NULL REFERENCES users(id),
  "channelId"  TEXT NOT NULL REFERENCES channels(id),
  PRIMARY KEY ("userId", "channelId")
);

CREATE TABLE conversations (
  id           TEXT PRIMARY KEY,
  "channelId"  TEXT NOT NULL REFERENCES channels(id),
  title        TEXT NOT NULL,
  "createdAt"  BIGINT NOT NULL
);

CREATE TABLE messages (
  id               TEXT PRIMARY KEY,
  "conversationId" TEXT NOT NULL REFERENCES conversations(id),
  "authorId"       TEXT NOT NULL REFERENCES users(id),
  body             TEXT NOT NULL,
  "createdAt"      BIGINT NOT NULL,
  "visibleTo"      TEXT NULL
);

CREATE TABLE attachments (
  id               TEXT PRIMARY KEY,
  "messageId"      TEXT NOT NULL REFERENCES messages(id),
  "conversationId" TEXT NOT NULL REFERENCES conversations(id),
  filename         TEXT NOT NULL,
  "createdAt"      BIGINT NOT NULL
);

CREATE INDEX messages_conversation_idx ON messages("conversationId");
CREATE INDEX attachments_conversation_idx ON attachments("conversationId");
CREATE INDEX participants_channel_idx ON participants("channelId");
