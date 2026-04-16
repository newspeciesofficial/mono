import {LogContext} from '@rocicorp/logger';
import {Database} from './src/db.ts';
const db = new Database(new LogContext('info'), '/Users/harsharanga/code/mono-v2/apps/zbugs/zero.db');
for (const limit of [100, 101, 110, 115]) {
  const ids = db.prepare(`SELECT id FROM issue WHERE open=1 AND visibility='public' AND projectID=(SELECT id FROM project WHERE lowerCaseName='zero') ORDER BY modified DESC, id DESC LIMIT ?`).all(limit).map((r: any) => r.id);
  const ph = ids.map(() => '?').join(',');
  const il = db.prepare(`SELECT COUNT(*) as c FROM issueLabel WHERE issueID IN (${ph})`).get(...ids);
  console.log(`limit=${limit}: issueLabels=`, il);
}
