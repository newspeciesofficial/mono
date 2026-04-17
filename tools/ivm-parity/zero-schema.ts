/**
 * Zero schema for the ivm-parity harness. Table + column names mirror
 * the source app's shared schema so ASTs transplanted from the source app's queries.ts
 * (after swapping literal IDs for our seed's IDs) run unchanged.
 */
import {
  definePermissions,
  createSchema,
  string,
  number,
  table,
  relationships,
  ANYONE_CAN_DO_ANYTHING,
} from '@rocicorp/zero';

const channel = table('channels')
  .columns({
    id: string(),
    name: string(),
    visibility: string(),
  })
  .primaryKey('id');

const user = table('users')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const participant = table('participants')
  .columns({
    userId: string(),
    channelId: string(),
  })
  .primaryKey('userId', 'channelId');

const conversation = table('conversations')
  .columns({
    id: string(),
    channelId: string(),
    title: string(),
    createdAt: number(),
  })
  .primaryKey('id');

const message = table('messages')
  .columns({
    id: string(),
    conversationId: string(),
    authorId: string(),
    body: string(),
    createdAt: number(),
    visibleTo: string().optional(),
  })
  .primaryKey('id');

const attachment = table('attachments')
  .columns({
    id: string(),
    messageId: string(),
    conversationId: string(),
    filename: string(),
    createdAt: number(),
  })
  .primaryKey('id');

// Relationships mirroring source: every correlated subquery our test
// queries need (attachments-on-conversation, channel-on-conversation,
// participants-on-channel) must be reachable through these.
const channelRelationships = relationships(channel, ({many}) => ({
  conversations: many({
    sourceField: ['id'],
    destField: ['channelId'],
    destSchema: conversation,
  }),
  participants: many({
    sourceField: ['id'],
    destField: ['channelId'],
    destSchema: participant,
  }),
}));

const conversationRelationships = relationships(conversation, ({one, many}) => ({
  channel: one({
    sourceField: ['channelId'],
    destField: ['id'],
    destSchema: channel,
  }),
  messages: many({
    sourceField: ['id'],
    destField: ['conversationId'],
    destSchema: message,
  }),
  attachments: many({
    sourceField: ['id'],
    destField: ['conversationId'],
    destSchema: attachment,
  }),
}));

const messageRelationships = relationships(message, ({one, many}) => ({
  conversation: one({
    sourceField: ['conversationId'],
    destField: ['id'],
    destSchema: conversation,
  }),
  author: one({
    sourceField: ['authorId'],
    destField: ['id'],
    destSchema: user,
  }),
  attachments: many({
    sourceField: ['id'],
    destField: ['messageId'],
    destSchema: attachment,
  }),
}));

const attachmentRelationships = relationships(attachment, ({one}) => ({
  message: one({
    sourceField: ['messageId'],
    destField: ['id'],
    destSchema: message,
  }),
  conversation: one({
    sourceField: ['conversationId'],
    destField: ['id'],
    destSchema: conversation,
  }),
}));

const participantRelationships = relationships(participant, ({one}) => ({
  channel: one({
    sourceField: ['channelId'],
    destField: ['id'],
    destSchema: channel,
  }),
  user: one({
    sourceField: ['userId'],
    destField: ['id'],
    destSchema: user,
  }),
}));

export const schema = createSchema({
  tables: [channel, user, participant, conversation, message, attachment],
  relationships: [
    channelRelationships,
    conversationRelationships,
    messageRelationships,
    attachmentRelationships,
    participantRelationships,
  ],
});

type AuthData = {
  sub: string;
};

// Permissive: anonymous can see / mutate everything. Zero's default
// when no `select` rule is declared is DENY (the query is rewritten
// as `WHERE 1=0`, collapsing to zero rows). We need explicit
// `ANYONE_CAN_DO_ANYTHING` for every table so the harness exercises
// the query pipelines, not the permission layer.
export const permissions = definePermissions<AuthData, typeof schema>(
  schema,
  () => ({
    channels: ANYONE_CAN_DO_ANYTHING,
    users: ANYONE_CAN_DO_ANYTHING,
    participants: ANYONE_CAN_DO_ANYTHING,
    conversations: ANYONE_CAN_DO_ANYTHING,
    messages: ANYONE_CAN_DO_ANYTHING,
    attachments: ANYONE_CAN_DO_ANYTHING,
  }),
);
