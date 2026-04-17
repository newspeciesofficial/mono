// Peek at the ivm-parity TS zero-cache's replica SQLite to verify
// the initial copy actually landed (zero-cache log shows "Finished
// copying 10 rows into messages", but queries return 0 so we want
// ground truth from the replica itself).
import {LogContext} from '@rocicorp/logger';
import {Database} from '../../packages/zqlite/src/db.ts';

const path = process.env.REPLICA_FILE ?? '/tmp/ivm-parity-ts.db';
const db = new Database(new LogContext('info'), path);
for (const t of ['channels', 'users', 'participants', 'conversations', 'messages', 'attachments']) {
  try {
    const row = db.prepare(`SELECT COUNT(*) AS c FROM "${t}"`).get() as {c: number};
    console.log(`${path} :: ${t} =`, row?.c);
  } catch (e) {
    console.log(`${path} :: ${t} = ERROR:`, (e as Error).message);
  }
}
